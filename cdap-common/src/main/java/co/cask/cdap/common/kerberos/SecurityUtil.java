/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

package co.cask.cdap.common.kerberos;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.id.KerberosPrincipalId;
import com.google.common.base.Preconditions;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.apache.twill.common.Threads;
import org.apache.zookeeper.client.ZooKeeperSaslClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginException;

/**
 * Utility functions for Kerberos.
 */
public final class SecurityUtil {

  private static final Logger LOG = LoggerFactory.getLogger(SecurityUtil.class);

  private SecurityUtil() { }

  /**
   * Enables Kerberos authentication based on configuration.
   *
   * @param cConf configuration object.
   */
  public static void enableKerberosLogin(CConfiguration cConf) throws IOException {
    if (System.getProperty(Constants.External.JavaSecurity.ENV_AUTH_LOGIN_CONFIG) != null) {
      LOG.warn("Environment variable '{}' was already set to {}. Not generating JAAS configuration.",
               Constants.External.JavaSecurity.ENV_AUTH_LOGIN_CONFIG,
               System.getProperty(Constants.External.JavaSecurity.ENV_AUTH_LOGIN_CONFIG));
      return;
    }

    if (!isKerberosEnabled(cConf)) {
      LOG.info("Kerberos login is not enabled. To enable Kerberos login, enable {} and configure {} and {}",
               Constants.Security.KERBEROS_ENABLED, Constants.Security.CFG_CDAP_MASTER_KRB_PRINCIPAL,
               Constants.Security.CFG_CDAP_MASTER_KRB_KEYTAB_PATH);
      return;
    }

    Preconditions.checkArgument(cConf.get(Constants.Security.CFG_CDAP_MASTER_KRB_PRINCIPAL) != null,
                                "Kerberos authentication is enabled, but " +
                                Constants.Security.CFG_CDAP_MASTER_KRB_PRINCIPAL + " is not configured");

    String principal = cConf.get(Constants.Security.CFG_CDAP_MASTER_KRB_PRINCIPAL);
    principal = expandPrincipal(principal);

    Preconditions.checkArgument(cConf.get(Constants.Security.CFG_CDAP_MASTER_KRB_KEYTAB_PATH) != null,
                                "Kerberos authentication is enabled, but " +
                                Constants.Security.CFG_CDAP_MASTER_KRB_KEYTAB_PATH + " is not configured");


    File keytabFile = new File(cConf.get(Constants.Security.CFG_CDAP_MASTER_KRB_KEYTAB_PATH));
    Preconditions.checkArgument(Files.isReadable(keytabFile.toPath()),
                                "Keytab file is not a readable file: %s", keytabFile);

    LOG.info("Using Kerberos principal {} and keytab {}", principal, keytabFile.getAbsolutePath());

    System.setProperty(Constants.External.Zookeeper.ENV_AUTH_PROVIDER_1,
                       "org.apache.zookeeper.server.auth.SASLAuthenticationProvider");
    System.setProperty(Constants.External.Zookeeper.ENV_ALLOW_SASL_FAILED_CLIENTS, "true");
    System.setProperty(ZooKeeperSaslClient.LOGIN_CONTEXT_NAME_KEY, "Client");

    final Map<String, String> properties = new HashMap<>();
    properties.put("doNotPrompt", "true");
    properties.put("useKeyTab", "true");
    properties.put("useTicketCache", "false");
    properties.put("principal", principal);
    properties.put("keyTab", keytabFile.getAbsolutePath());

    final AppConfigurationEntry configurationEntry = new AppConfigurationEntry(
      KerberosUtil.getKrb5LoginModuleName(), AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, properties);

    Configuration configuration = new Configuration() {
      @Override
      public AppConfigurationEntry[] getAppConfigurationEntry(String s) {
        return new AppConfigurationEntry[] { configurationEntry };
      }
    };

    // apply the configuration
    Configuration.setConfiguration(configuration);
  }

  /**
   * Expands _HOST in principal name with local hostname.
   *
   * @param principal Kerberos principal name
   * @return expanded principal name
   * @throws UnknownHostException if the local hostname could not be resolved into an address.
   */
  @Nullable
  public static String expandPrincipal(@Nullable String principal) throws UnknownHostException {
    if (principal == null) {
      return principal;
    }

    String localHostname = InetAddress.getLocalHost().getCanonicalHostName();
    return principal.replace("/_HOST@", "/" + localHostname + "@");
  }

  /**
   * @param cConf CConfiguration object.
   * @return true, if Kerberos is enabled.
   */
  public static boolean isKerberosEnabled(CConfiguration cConf) {
    return cConf.getBoolean(Constants.Security.KERBEROS_ENABLED,
                            cConf.getBoolean(Constants.Security.ENABLED));
  }

  public static void loginForMasterService(CConfiguration cConf) throws IOException, LoginException {
    String principal = cConf.get(Constants.Security.CFG_CDAP_MASTER_KRB_PRINCIPAL);
    String keytabPath = cConf.get(Constants.Security.CFG_CDAP_MASTER_KRB_KEYTAB_PATH);

    if (UserGroupInformation.isSecurityEnabled()) {
      Path keytabFile = Paths.get(keytabPath);
      Preconditions.checkArgument(Files.isReadable(keytabFile),
                                  "Keytab file is not a readable file: %s", keytabFile);
      String expandedPrincipal = expandPrincipal(principal);
      LOG.info("Logging in as: principal={}, keytab={}", principal, keytabPath);
      UserGroupInformation.loginUserFromKeytab(expandedPrincipal, keytabPath);

      long delaySec = cConf.getLong(Constants.Security.KERBEROS_KEYTAB_RELOGIN_INTERVAL);
      Executors.newSingleThreadScheduledExecutor(Threads.createDaemonThreadFactory("Kerberos keytab renewal"))
        .scheduleWithFixedDelay(new Runnable() {
          @Override
          public void run() {
            try {
              UserGroupInformation.getLoginUser().checkTGTAndReloginFromKeytab();
            } catch (IOException e) {
              LOG.error("Failed to relogin from keytab", e);
            }
          }
        }, delaySec, delaySec, TimeUnit.SECONDS);
    }
  }

  /**
   * Returns a {@link KerberosName} from the given {@link KerberosPrincipalId} if the given kerberos principal id
   * is valid. Refer to
   * <a href="https://web.mit.edu/kerberos/krb5-1.5/krb5-1.5.4/doc/krb5-user/What-is-a-Kerberos-Principal_003f.html">
   * Kerberos Principal</a> for details.
   *
   * @param principalId The {@link KerberosPrincipalId} from which {@link KerberosName} needs to be created
   * @return {@link KerberosName} for the given {@link KerberosPrincipalId}
   * @throws IllegalArgumentException if failed to create a {@link KerberosName} from the given
   * {@link KerberosPrincipalId}
   */
  public static KerberosName getKerberosName(KerberosPrincipalId principalId) {
    return new KerberosName(principalId.getPrincipal());
  }


  /**
   * Checks if the given {@link KerberosPrincipalId} is valid or not by calling
   * {@link #getKerberosName(KerberosPrincipalId)}. This is just a wrapper around
   * {@link #getKerberosName(KerberosPrincipalId)} to not return an object to the caller for simplicity.
   *
   * @param principalId {@link KerberosPrincipalId} which needs to be validated
   * @throws IllegalArgumentException if failed to create a {@link KerberosName} from the given
   * {@link KerberosPrincipalId}
   */
  public static void validateKerberosPrincipal(KerberosPrincipalId principalId) {
    getKerberosName(principalId);
  }

  public static String getKeytabURIforPrincipal(String principal, CConfiguration cConf) throws IOException {
    String confPath = cConf.get(Constants.Security.KEYTAB_PATH);
    String name = new KerberosName(principal).getShortName();
    return confPath.replace(Constants.USER_NAME_SPECIFIER, name);
  }
}
