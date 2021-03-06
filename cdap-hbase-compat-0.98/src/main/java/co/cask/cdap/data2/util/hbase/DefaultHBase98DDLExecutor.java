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

package co.cask.cdap.data2.util.hbase;

import co.cask.cdap.spi.hbase.HBaseDDLExecutor;
import co.cask.cdap.spi.hbase.TableDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

/**
 * Implementation of the {@link HBaseDDLExecutor} for HBase 0.98
 */
public class DefaultHBase98DDLExecutor extends DefaultHBaseDDLExecutor {
  @Override
  public HTableDescriptor getHTableDescriptor(TableDescriptor descriptor) {
    return HBase98TableDescriptorUtil.getHTableDescriptor(descriptor);
  }

  @Override
  public TableDescriptor getTableDescriptor(HTableDescriptor descriptor) {
    return HBase98TableDescriptorUtil.getTableDescriptor(descriptor);
  }
}
