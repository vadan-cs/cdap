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

package co.cask.cdap.security.impersonation;

import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.kerberos.OwnerAdmin;
import co.cask.cdap.common.kerberos.SecurityUtil;
import co.cask.cdap.proto.id.KerberosPrincipalId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * Default implementation of {@link Impersonator} that impersonate using {@link UGIProvider}.
 */
public class DefaultImpersonator implements Impersonator {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultImpersonator.class);

  private final CConfiguration cConf;
  private final OwnerAdmin ownerAdmin;
  private final boolean kerberosEnabled;
  private final UGIProvider ugiProvider;

  @Inject
  @VisibleForTesting
  public DefaultImpersonator(CConfiguration cConf, UGIProvider ugiProvider,
                             OwnerAdmin ownerAdmin) {
    this.cConf = cConf;
    this.ownerAdmin = ownerAdmin;
    this.ugiProvider = ugiProvider;
    this.kerberosEnabled = SecurityUtil.isKerberosEnabled(cConf);
  }

  @Override
  public <T> T doAs(NamespacedEntityId entityId, final Callable<T> callable) throws Exception {
    UserGroupInformation ugi = getUGI(entityId);
    return  ImpersonationUtils.doAs(ugi, callable);
  }

  @Override
  public UserGroupInformation getUGI(NamespacedEntityId entityId) throws IOException, NamespaceNotFoundException {
    // don't impersonate if kerberos isn't enabled OR if the operation is in the system namespace
    if (!kerberosEnabled || NamespaceId.SYSTEM.equals(entityId.getNamespaceId())) {
      return UserGroupInformation.getCurrentUser();
    }
    String principal = cConf.get(Constants.Security.CFG_CDAP_MASTER_KRB_PRINCIPAL);
    String keytabURI = cConf.get(Constants.Security.CFG_CDAP_MASTER_KRB_KEYTAB_PATH);
    try {
      KerberosPrincipalId principalId = ownerAdmin.getEffectiveOwner(entityId);
      // If an effective owner was not found then the operation will be performed as the configured master user
      if (principalId != null) {
        principal = principalId.getPrincipal();
        keytabURI = SecurityUtil.getKeytabURIforPrincipal(principal, cConf);
      }
      LOG.debug("Impersonating principal {} for entity {}, keytab path is {}", principal, entityId, keytabURI);
      return getUGI(new ImpersonationInfo(principal, keytabURI));
    } catch (Exception e) {
      Throwables.propagateIfInstanceOf(e, IOException.class);
      throw Throwables.propagate(e);
    }
  }

  private UserGroupInformation getUGI(ImpersonationInfo impersonationInfo) throws IOException {
    // no need to get a UGI if the current UGI is the one we're requesting; simply return it
    String configuredPrincipalShortName = new KerberosName(impersonationInfo.getPrincipal()).getShortName();
    if (UserGroupInformation.getCurrentUser().getShortUserName().equals(configuredPrincipalShortName)) {
      return UserGroupInformation.getCurrentUser();
    }
    return ugiProvider.getConfiguredUGI(impersonationInfo);
  }
}
