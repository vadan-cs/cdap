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

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.kerberos.SecurityUtil;
import co.cask.cdap.proto.NamespaceConfig;
import co.cask.cdap.proto.NamespaceMeta;

import java.io.IOException;
import java.util.Objects;

/**
 * Encapsulates information necessary to impersonate a user - principal and keytab path.
 */
public final class ImpersonationInfo {
  private final String principal;
  private final String keytabURI;

  /**
   * Creates {@link ImpersonationInfo} using the specified principal and keytab path.
   */
  public ImpersonationInfo(String principal, CConfiguration cConf) throws IOException {
    this.principal = principal;
    this.keytabURI = SecurityUtil.getKeytabURIforPrincipal(principal, cConf);
  }

  /**
   * Creates {@link ImpersonationInfo} using the specified principal and keytab path.
   */
  public ImpersonationInfo(String principal, String keytabURI) {
    this.principal = principal;
    this.keytabURI = keytabURI;
  }

  public String getPrincipal() {
    return principal;
  }

  public String getKeytabURI() {
    return keytabURI;
  }

  @Override
  public String toString() {
    return "ImpersonationInfo{" +
      "principal='" + principal + '\'' +
      ", keytabURI='" + keytabURI + '\'' +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ImpersonationInfo that = (ImpersonationInfo) o;
    return Objects.equals(principal, that.principal) && Objects.equals(keytabURI, that.keytabURI);
  }

  @Override
  public int hashCode() {
    return Objects.hash(principal, keytabURI);
  }
}
