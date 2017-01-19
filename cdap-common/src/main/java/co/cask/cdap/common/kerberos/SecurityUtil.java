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
import com.google.common.base.Preconditions;
import org.apache.hadoop.security.UserGroupInformation;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import javax.security.auth.kerberos.KerberosPrincipal;
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
   * A pattern to match kerberos principals
   */
  private static final Pattern KERBEROS_PRINCIPAL = Pattern.compile("([^/@]*)(/([^/@]*))?@([^/@]*)");

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
   * Helper to create a {@link KerberosPrincipal} from given principal string.
   * <p>
   * Supports two Kerberos name types:
   * <ul>
   * <li>KRB_NT_PRINCIPAL:  Just the name of the principal as in DCE, or for users. For example: alice@REALM</li>
   * <li>KRB_NT_SRV_HST:  Service with host name as instance(telnet, rcommands).
   * For example alice/hostname@REALM
   * </li>
   * </ul>
   * Refer to <a href=https://tools.ietf.org/html/rfc4120#section-7.5.8>Name Types</a> documentation for details on
   * Kerberos Name Types.
   * </p>
   *
   * @param principal the Kerberos principal string
   * @return {@link KerberosPrincipal} from the given principal
   * @throws IllegalArgumentException if a {@link KerberosPrincipal} cannot be created from the given principal string
   */
  public static KerberosPrincipal parsePrincipal(String principal) {
    Matcher match = KERBEROS_PRINCIPAL.matcher(principal);
    validatePrincipal(principal, match);
    String hostName = match.group(3);
    if (hostName == null) {
      return new KerberosPrincipal(principal);
    } else {
      return new KerberosPrincipal(principal, KerberosPrincipal.KRB_NT_SRV_HST);
    }
  }

  /**
   * Validates if a valid {@link KerberosPrincipal} can be created from the given principal string.
   * <p>
   * Supports two Kerberos name types:
   * <ul>
   * <li>KRB_NT_PRINCIPAL:  Just the name of the principal as in DCE, or for users. For example: alice@REALM</li>
   * <li>KRB_NT_SRV_HST:  Service with host name as instance(telnet, rcommands).
   * For example alice/hostname@REALM
   * </li>
   * </ul>
   * Refer to <a href=https://tools.ietf.org/html/rfc4120#section-7.5.8>Name Types</a> documentation for details on
   * Kerberos Name Types.
   * </p>
   *
   * @param principal the principal which needs to be validated
   * @throws IllegalArgumentException if the given principal is not valid
   */
  public static void validatePrincipal(String principal) {
    validatePrincipal(principal, KERBEROS_PRINCIPAL.matcher(principal));
  }

  private static void validatePrincipal(String principal, Matcher match) {
    if (!match.matches()) {
      throw new IllegalArgumentException(String.format("Malformed Kerberos Principal: %s. Note the supported " +
                                                         "Kerberos Name Types are KRB_NT_PRINCIPAL (ex: alice@REALM) " +
                                                         "and KRB_NT_SRV_HST (ex: alice/hostname@REALM)", principal));
    }
  }
}
