/*
 * Copyright © 2016-2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.internal.app.namespace;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.kerberos.SecurityUtil;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.explore.client.ExploreFacade;
import co.cask.cdap.explore.service.ExploreException;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.sql.SQLException;

/**
 * Performs common namespace admin operations on storage providers (HBase, Filesystem, Hive, etc)
 */
abstract class AbstractStorageProviderNamespaceAdmin implements StorageProviderNamespaceAdmin {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractStorageProviderNamespaceAdmin.class);

  private final CConfiguration cConf;
  private final NamespacedLocationFactory namespacedLocationFactory;
  private final ExploreFacade exploreFacade;
  private final NamespaceQueryAdmin namespaceQueryAdmin;


  AbstractStorageProviderNamespaceAdmin(CConfiguration cConf,
                                        NamespacedLocationFactory namespacedLocationFactory,
                                        ExploreFacade exploreFacade,
                                        NamespaceQueryAdmin namespaceQueryAdmin) {
    this.cConf = cConf;
    this.namespacedLocationFactory = namespacedLocationFactory;
    this.exploreFacade = exploreFacade;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
  }

  /**
   * Create a namespace in the File System and Hive.
   *
   * @param namespaceMeta {@link NamespaceMeta} for the namespace to create
   * @throws IOException if there are errors while creating the namespace in the File System
   * @throws ExploreException if there are errors while creating the namespace in Hive
   * @throws SQLException if there are errors while creating the namespace in Hive
   */
  @Override
  public void create(NamespaceMeta namespaceMeta) throws IOException, ExploreException, SQLException {

    createLocation(namespaceMeta);

    if (cConf.getBoolean(Constants.Explore.EXPLORE_ENABLED)) {
      try {
        exploreFacade.createNamespace(namespaceMeta);
      } catch (ExploreException | SQLException e) {
        try {
          // if we failed to create a namespace in explore then delete the earlier created location for the namespace
          deleteLocation(namespaceMeta.getNamespaceId());
        } catch (Exception e2) {
          e.addSuppressed(e2);
        }
        throw e;
      }
    }
  }

  /**
   * Deletes the namespace directory on the FileSystem and Hive.
   *
   * @param namespaceId {@link NamespaceId} for the namespace to delete
   * @throws IOException if there are errors while deleting the namespace in the File System
   * @throws ExploreException if there are errors while deleting the namespace in Hive
   * @throws SQLException if there are errors while deleting the namespace in Hive
   */
  @Override
  public void delete(NamespaceId namespaceId) throws IOException, ExploreException, SQLException {

    deleteLocation(namespaceId);

    if (cConf.getBoolean(Constants.Explore.EXPLORE_ENABLED)) {
      exploreFacade.removeNamespace(namespaceId);
    }
  }

  private void deleteLocation(NamespaceId namespaceId) throws IOException {
    // TODO: CDAP-1581: Implement soft delete
    Location namespaceHome = namespacedLocationFactory.get(namespaceId.toId());
    try {
      if (hasCustomLocation(namespaceQueryAdmin.get(namespaceId))) {
        LOG.debug("Custom location mapping {} was found while deleting namespace {}. Deleting all data inside it but" +
                    "skipping namespace home directory delete.", namespaceHome, namespaceId);
        // delete everything inside the namespace home but not the namespace home as its user owned directory
        Locations.deleteContent(namespaceHome);
      } else {
        // a custom location was not provided for this namespace so cdap is responsible for managing the lifecycle of
        // the location hence delete it.
        if (namespaceHome.exists() && !namespaceHome.delete(true)) {
          throw new IOException(String.format("Error while deleting home directory '%s' for namespace '%s'",
                                              namespaceHome, namespaceId));
        }
      }
    } catch (Exception e) {
      throw new IOException(String.format("Error while deleting home directory %s for namespace %s ", namespaceHome,
                                          namespaceId), e);
    }
  }

  private void createLocation(NamespaceMeta namespaceMeta) throws IOException {
    boolean createdHome = false;
    Location namespaceHome;
    if (hasCustomLocation(namespaceMeta)) {
      namespaceHome = validateCustomLocation(namespaceMeta);
    } else {
      // no namespace custom location was provided one must be created by cdap
      namespaceHome = namespacedLocationFactory.get(namespaceMeta);
      if (namespaceHome.exists()) {
        throw new FileAlreadyExistsException(namespaceHome.toString());
      }
      // create namespace home dir
      if (!namespaceHome.mkdirs()) {
        throw new IOException(String.format("Error while creating home directory '%s' for namespace '%s'",
                                            namespaceHome, namespaceMeta.getNamespaceId()));
      }
      createdHome = true;
    }
    Location namespaceData = namespaceHome.append(Constants.Dataset.DEFAULT_DATA_DIR);
    String configuredGroupName = namespaceMeta.getConfig().getGroupName();
    boolean createdData = false;
    try {
      if (createdHome && SecurityUtil.isKerberosEnabled(cConf)) {
        // set the group id of the namespace home if configured, or the current user's primary group
        String groupName = configuredGroupName != null
          ? configuredGroupName : UserGroupInformation.getCurrentUser().getPrimaryGroupName();
        namespaceHome.setGroup(groupName);
      }
      // create the data directory with the default permissions; then add group privileges to rwx
      // so that all users in this group, when impersonated, can create subdirectories for their datasets
      if (!namespaceData.mkdirs()) {
        throw new IOException(String.format("Error while creating data directory '%s' for namespace '%s'",
                                            namespaceData, namespaceMeta.getNamespaceId()));
      }
      createdData = true;
      if (SecurityUtil.isKerberosEnabled(cConf)) {
        // the data dir should have the group from the namespace config if present, or the same group as the home dir
        String dataGroup = configuredGroupName != null ? configuredGroupName : namespaceHome.getGroup();
        namespaceData.setGroup(dataGroup);
        // set the permissions to rwx for group, if a group name was configured for the namespace
        if (configuredGroupName != null) {
          String permissions = namespaceData.getPermissions();
          namespaceData.setPermissions(permissions.substring(0, 3) + "rwx" + permissions.substring(6));
        }
      }
    } catch (Throwable t) {
      try {
        if (createdHome) {
          namespaceHome.delete(true);
        } else if (createdData) {
          namespaceData.delete(true);
        }
      } catch (Throwable t1) {
        LOG.warn("Error while cleaning up home directory '%s' for namespace '%s'",
                 namespaceHome, namespaceMeta.getNamespaceId());
        t.addSuppressed(t1);
      }
      Throwables.propagateIfInstanceOf(t, IOException.class);
      throw Throwables.propagate(t);
    }
  }

  private boolean hasCustomLocation(NamespaceMeta namespaceMeta) {
    return !Strings.isNullOrEmpty(namespaceMeta.getConfig().getRootDirectory());
  }

  private Location validateCustomLocation(NamespaceMeta namespaceMeta) throws IOException {
    // since this is a custom location we expect it to exist. Get the custom location for the namespace from
    // namespaceLocationFactory since the location needs to be aware of local/distributed fs.
    Location customNamespacedLocation = namespacedLocationFactory.get(namespaceMeta);
    if (!customNamespacedLocation.exists()) {
      throw new IOException(String.format(
        "The provided home directory '%s' for namespace '%s' does not exist. Please create it on filesystem " +
          "with sufficient privileges for the user %s and then try creating a namespace.",
        customNamespacedLocation.toString(), namespaceMeta.getNamespaceId(),
        namespaceMeta.getConfig().getPrincipal()));
    }
    if (!customNamespacedLocation.isDirectory()) {
      throw new IOException(String.format(
        "The provided home directory '%s' for namespace '%s' is not a directory. Please specify a directory for the " +
          "namespace with sufficient privileges for the user %s and then try creating a namespace.",
        customNamespacedLocation.toString(), namespaceMeta.getNamespaceId(),
        namespaceMeta.getConfig().getPrincipal()));
    }
    // we also expect it to empty since non-empty directories can lead to various inconsistencies CDAP-6743
    if (!customNamespacedLocation.list().isEmpty()) {
      throw new IOException(String.format(
        "The provided home directory '%s' for namespace '%s' is not empty. Please try creating the namespace " +
          "again with an empty directory mapping and sufficient privileges for the user %s.",
        customNamespacedLocation.toString(), namespaceMeta.getNamespaceId(),
        namespaceMeta.getConfig().getPrincipal()));
    }
    // if a group name was configured in the namespace meta, validate the home location's group and permissions
    if (namespaceMeta.getConfig().getGroupName() != null) {
      String groupName = customNamespacedLocation.getGroup();
      String permissions = customNamespacedLocation.getPermissions().substring(3, 6);
      if (!groupName.equals(namespaceMeta.getConfig().getGroupName())) {
        LOG.warn("The provided home directory '%s' for namespace '%s' has group '%s', which is different from " +
                   "the configured group '%s' of the namespace.", customNamespacedLocation.toString(),
                 namespaceMeta.getNamespaceId(), groupName, namespaceMeta.getConfig().getGroupName());
      }
      if (!"rwx".equals(permissions)) {
        LOG.warn("The provided home directory '%s' for namespace '%s' has group permissions of '%s'. It is " +
                   "recommended to set the group permissions to 'rwx'",
                 customNamespacedLocation.toString(), namespaceMeta.getNamespaceId(), permissions);
      }
    }
    return customNamespacedLocation;
  }
}
