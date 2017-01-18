/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.store;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.ConflictDetection;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.AlreadyExistsException;
import co.cask.cdap.common.kerberos.SecurityUtil;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.data2.transaction.TxCallable;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.inject.Inject;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;

import java.io.IOException;
import java.util.Collections;

/**
 * This class manages owner's principal information of CDAP entities.
 * <p>
 * Currently: Owner information is stored for the following entities:
 * <ul>
 * <li>{@link co.cask.cdap.api.data.stream.Stream}</li>
 * <li>{@link co.cask.cdap.api.dataset.Dataset}</li>
 * <li>{@link co.cask.cdap.api.app.Application}</li>
 * </ul>
 * </p>
 * <p>
 * It is the responsibility of the creator of the supported entities to add an entry in this store to store the
 * associated owner's principal. Note: An absence of an entry in this table for an {@link EntityId} does not
 * signifies that the entity does not exists. The owner information is only stored if an owner was provided during
 * creation time else the owner information is non-existent which signifies that the entity own is default CDAP owner.
 * </p>
 * <p>
 * <p>
 * The owner's principal must be in the of the following Kerberos name types:
 * <ul>
 * <li>KRB_NT_PRINCIPAL:  Just the name of the principal as in DCE, or for users. For example: alice@REALM</li>
 * <li>KRB_NT_SRV_HST:  Service with host name as instance(telnet, rcommands). For example alice/hostname@REALM</li>
 * </ul>
 * Refer to <a href=https://tools.ietf.org/html/rfc4120#section-7.5.8>Name Types</a> documentation for details on
 * Kerberos Name Types.
 * </p>
 */
public class OwnerStore {
  private static final String OWNER_PREFIX = "o";
  // currently, we only leverage one column of the table. However, not using KeyValueTable, so that being able to use
  // additional columns in the future is simple
  private static final byte[] COL = Bytes.toBytes("c");
  private static final DatasetId DATASET_ID = NamespaceId.SYSTEM.dataset("owner.meta");
  private static final DatasetProperties DATASET_PROPERTIES =
    DatasetProperties.builder().add(Table.PROPERTY_CONFLICT_LEVEL, ConflictDetection.COLUMN.name()).build();

  private final DatasetFramework datasetFramework;
  private final Transactional transactional;

  @Inject
  OwnerStore(DatasetFramework datasetFramework, TransactionSystemClient txClient) {
    this.datasetFramework = datasetFramework;
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(new SystemDatasetInstantiator(datasetFramework),
                                                                   txClient, DATASET_ID.getParent(),
                                                                   Collections.<String, String>emptyMap(), null, null)),
      RetryStrategies.retryOnConflict(20, 100)
    );
  }

  /**
   * Adds datasets and types to the given {@link DatasetFramework} used by {@link OwnerStore}
   *
   * @param framework framework to add types and datasets to
   */
  public static void setupDatasets(DatasetFramework framework) throws IOException, DatasetManagementException {
    framework.addInstance(Table.class.getName(), DATASET_ID, DATASET_PROPERTIES);
  }

  /**
   * Store the owner's principal for the given {@link EntityId}
   *
   * @param entityId The {@link EntityId} whose owner principal needs to be stored
   * @param principal the principal of the {@link EntityId} owner
   * @throws IOException if failed to get the store
   */
  public void add(final EntityId entityId, final String principal) throws IOException, AlreadyExistsException {
    if (exists(entityId)) {
      throw new AlreadyExistsException(String.format("Owner information already exists for entity '%s'.",
                                                     entityId));
    }
    // Before storing it make sure its a valid kerberos principal to fail early and hence failing the entity creation
    verifyPrincipal(principal);
    try {
      transactional.execute(new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          Table metaTable = getTable(context);
          metaTable.put(createRowKey(entityId), COL, Bytes.toBytes(principal));
        }
      });
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e, IOException.class);
    }
  }

  /**
   * Retrieves the owner information for the given {@link EntityId}
   *
   * @param entityId the {@link EntityId} whose owner principal information needs to be retrieved
   * @return {@link String} which is the principal of the {@link EntityId} owner
   * @throws IOException if failed to get the store
   */
  public String get(final EntityId entityId) throws IOException {
    try {
      return Transactions.execute(transactional, new TxCallable<String>() {
        @Override
        public String call(DatasetContext context) throws Exception {
          byte[] principalBytes = getTable(context).get(createRowKey(entityId), COL);
          return principalBytes == null ? null : Bytes.toString(principalBytes);
        }
      });
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e, IOException.class);
    }
  }

  /**
   * Checks if owner information exists or not
   *
   * @param entityId the {@link EntityId} for which the check needs to be performed
   * @return a boolean true: owner principal exists, false: no owner principal exists
   * @throws IOException if failed to get the store
   */
  public boolean exists(final EntityId entityId) throws IOException {
    try {
      return Transactions.execute(transactional, new TxCallable<Boolean>() {
        @Override
        public Boolean call(DatasetContext context) throws Exception {
          byte[] principalBytes = getTable(context).get(createRowKey(entityId), COL);
          return principalBytes != null;
        }
      });
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e, IOException.class);
    }
  }

  /**
   * Deletes the owner principal for the given {@link EntityId}
   *
   * @param entityId the entity whose owner principal needs to be deleted
   * @throws IOException if failed to get the owner store
   */
  public void delete(final EntityId entityId) throws IOException {
    try {
      transactional.execute(new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          getTable(context).delete(createRowKey(entityId));
        }
      });
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e, IOException.class);
    }
  }

  private Table getTable(DatasetContext context) throws IOException, DatasetManagementException {
    return DatasetsUtil.getOrCreateDataset(context, datasetFramework, DATASET_ID, Table.class.getName(),
                                           DATASET_PROPERTIES);
  }

  // creates rowkey for association entries
  private byte[] createRowKey(EntityId entityId) {
    return Bytes.toBytes(OWNER_PREFIX + ':' + entityId.toString());
  }

  private void verifyPrincipal(String principal) {
    // The below throws IllegalArgumentException if the given principal string is not in valid format
    SecurityUtil.parsePrincipal(principal);
  }
}
