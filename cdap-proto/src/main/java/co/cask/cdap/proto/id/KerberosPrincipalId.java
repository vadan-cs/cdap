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

package co.cask.cdap.proto.id;

import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.element.EntityType;

import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.security.auth.kerberos.KerberosPrincipal;

/**
 * <p>
 * Represents a Kerberos Principal and also extends {@link EntityId} to support granting
 * {@link co.cask.cdap.proto.security.Privilege} and other Authorization operations.
 * </p>
 * <p>
 * Note: This class should not be confused with {@link co.cask.cdap.proto.security.Principal} which represents a
 * user, group or role in CDAP to whom {@link co.cask.cdap.proto.security.Privilege} can be given.
 * Whereas this {@link KerberosPrincipalId} class represent a Kerberos principal on
 * which {@link co.cask.cdap.proto.security.Action} can be granted to {@link co.cask.cdap.proto.security.Principal} to
 * represent a {@link co.cask.cdap.proto.security.Privilege}.
 * </p>
 * <p>
 * For example, if a {@link co.cask.cdap.proto.security.Principal} has
 * {@link co.cask.cdap.proto.security.Action#READ} on a {@link KerberosPrincipalId} it signifies that the
 * {@link co.cask.cdap.proto.security.Principal} can READ (use) the {@link KerberosPrincipalId} to impersonate the user
 * of the {@link KerberosPrincipalId}.
 * </p>
 */
public class KerberosPrincipalId extends EntityId {

  // A pattern to match kerberos principals
  private static final Pattern KERBEROS_PRINCIPAL = Pattern.compile("([^/@]*)(/([^/@]*))?@([^/@]*)");

  private final String principal;
  private final KerberosPrincipal kerberosPrincipal;
  private transient Integer hashCode;

  public KerberosPrincipalId(String principal) {
    super(EntityType.KERBEROSPRINCIPAL);
    if (principal == null) {
      throw new NullPointerException("Principal cannot be null");
    }
    // Store the principal in the form given by the user as we have to present this back to the user later.
    // KerberosPrincipal.toString gives the principal back in GSS_KRB5_NT_PRINCIPAL_NAME form which might not be the
    // same as what user gave initially.
    this.principal = principal;
    this.kerberosPrincipal = getKerberosPrincipal(principal);
  }


  public String getPrincipalAsString() {
    return principal;
  }

  public KerberosPrincipal getKerberosPrincipal() {
    return kerberosPrincipal;
  }

  @Override
  public Id toId() {
    throw new UnsupportedOperationException(String.format("%s does not have old %s class",
                                                          KerberosPrincipalId.class.getName(), Id.class.getName()));
  }

  @Override
  protected Iterable<String> toIdParts() {
    return Collections.singletonList(principal);
  }

  @Override
  public String getEntityName() {
    return getPrincipalAsString();
  }

  @SuppressWarnings("unused")
  public static KerberosPrincipalId fromIdParts(Iterable<String> idString) {
    Iterator<String> iterator = idString.iterator();
    return new KerberosPrincipalId(nextAndEnd(iterator, "principal"));
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    KerberosPrincipalId other = (KerberosPrincipalId) o;
    return Objects.equals(principal, other.principal);
  }

  @Override
  public int hashCode() {
    Integer hashCode = this.hashCode;
    if (hashCode == null) {
      this.hashCode = hashCode = Objects.hash(super.hashCode(), principal);
    }
    return hashCode;
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
  private KerberosPrincipal getKerberosPrincipal(String principal) {
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
