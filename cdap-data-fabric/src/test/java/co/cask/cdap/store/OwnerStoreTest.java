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

import co.cask.cdap.common.AlreadyExistsException;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.namespace.guice.NamespaceClientRuntimeModule;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.data2.dataset2.DatasetFrameworkTestUtil;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.authorization.AuthorizationTestModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.runtime.TransactionInMemoryModule;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests for {@link OwnerStore}.
 */
public class OwnerStoreTest {

  @ClassRule
  public static DatasetFrameworkTestUtil dsFrameworkUtil = new DatasetFrameworkTestUtil();
  private static OwnerStore ownerStore;


  @BeforeClass
  public static void setup() throws IOException {
    Injector injector = Guice.createInjector(
      new ConfigModule(),
      new DataSetsModules().getInMemoryModules(),
      new LocationRuntimeModule().getInMemoryModules(),
      new TransactionInMemoryModule(),
      new SystemDatasetRuntimeModule().getInMemoryModules(),
      new NamespaceClientRuntimeModule().getInMemoryModules(),
      new AuthorizationTestModule(),
      new AuthorizationEnforcementModule().getInMemoryModules(),
      new AuthenticationContextModules().getMasterModule()
    );
    TransactionManager txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();
    ownerStore = injector.getInstance(OwnerStore.class);
  }

  @Test
  public void test() throws Exception {
    // create a stream without any owner
    StreamId streamId = NamespaceId.DEFAULT.stream("fooStream");

    // No owner info should exist for above stream
    Assert.assertNull(ownerStore.get(streamId));

    // delete behavior is idempotent, so won't throw NotFoundException
    ownerStore.delete(streamId);

    // Trying to add an invalid principal should fail
    try {
      ownerStore.add(streamId, "somePrincipal");
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }

    // Storing a valid principal should work
    ownerStore.add(streamId, "alice/somehost@SOMEKDC.NET");
    // Should be able to get the principal back
    Assert.assertEquals("alice/somehost@SOMEKDC.NET", ownerStore.get(streamId));

    // Should not be able to update the owner principal
    try {
      ownerStore.add(streamId, "bob@SOMEKDC.NET");
      Assert.fail();
    } catch (AlreadyExistsException e) {
      // expected
    }

    // delete the owner information
    ownerStore.delete(streamId);
    Assert.assertNull(ownerStore.get(streamId));
  }
}
