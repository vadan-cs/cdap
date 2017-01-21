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

package co.cask.cdap.security;

import co.cask.cdap.common.AlreadyExistsException;
import co.cask.cdap.common.security.OwnerAdmin;
import co.cask.cdap.proto.id.KerberosPrincipalId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.store.OwnerStore;
import com.google.inject.Inject;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Default implementation of {@link OwnerAdmin};
 */
public class DefaultOwnerAdmin implements OwnerAdmin {

  private final OwnerStore ownerStore;

  @Inject
  public DefaultOwnerAdmin(OwnerStore ownerStore) {
    this.ownerStore = ownerStore;
  }

  @Override
  public void add(NamespacedEntityId entityId, KerberosPrincipalId kerberosPrincipalId)
    throws IOException, AlreadyExistsException {
    ownerStore.add(entityId, kerberosPrincipalId);
  }

  @Nullable
  @Override
  public KerberosPrincipalId getOwner(NamespacedEntityId entityId) throws IOException {
    return ownerStore.getOwner(entityId);
  }

  @Nullable
  @Override
  public KerberosPrincipalId getEffectiveOwner(NamespacedEntityId entityId) throws IOException {
    return ownerStore.getEffectiveOwner(entityId);
  }

  @Override
  public boolean exists(NamespacedEntityId entityId) throws IOException {
    return ownerStore.exists(entityId);
  }

  @Override
  public void delete(NamespacedEntityId entityId) throws IOException {
    ownerStore.delete(entityId);
  }
}
