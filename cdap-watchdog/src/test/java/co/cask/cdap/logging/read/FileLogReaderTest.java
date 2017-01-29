/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.logging.read;

import co.cask.cdap.logging.write.LogLocation;
import co.cask.cdap.proto.id.NamespaceId;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.List;

public class FileLogReaderTest {
  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testFilterLogs() throws Exception {
    /*Location base = new LocalLocationFactory().create(tempFolder.newFolder().toURI());
    List<LogLocation> logLocationList = new ArrayList<>();
    for (long i = 10; i < 40; i += 5) {
      logLocationList.add(new LogLocation(LogLocation.VERSION_1, i, 0, base.append(String.valueOf(i)),
                                          NamespaceId.DEFAULT.getNamespace(), null));
    }
    // okay to pass metadata reader null, as we are just testing the methods which doesn't use metadata reader.
    FileLogReader fileLogReader = new FileLogReader(null);
    logLocationList = fileLogReader.sortFilesInRange(logLocationList);
    List<LogLocation> result = fileLogReader.filterFilesByStartTime(logLocationList, 32);
    Assert.assertEquals(2, result.size());
    Assert.assertEquals(30, result.get(0).getEventTimeMs());
    Assert.assertEquals(35, result.get(1).getEventTimeMs());

    result = fileLogReader.filterFilesByStartTime(logLocationList, 42);
    Assert.assertEquals(1, result.size());
    Assert.assertEquals(35, result.get(0).getEventTimeMs());



    logLocationList = new ArrayList<>();
    long timestamp = System.currentTimeMillis();

    logLocationList.add(new LogLocation(LogLocation.VERSION_1, 100, timestamp + 2, base.append(String.valueOf(100)),
                                        NamespaceId.DEFAULT.getNamespace(), null));
    logLocationList.add(new LogLocation(LogLocation.VERSION_1, 100, timestamp, base.append(String.valueOf(100)),
                                        NamespaceId.DEFAULT.getNamespace(), null));
    logLocationList.add(new LogLocation(LogLocation.VERSION_1, 100, timestamp + 1, base.append(String.valueOf(100)),
                                        NamespaceId.DEFAULT.getNamespace(), null));


    logLocationList = fileLogReader.sortFilesInRange(logLocationList);
    result = fileLogReader.filterFilesByStartTime(logLocationList, 100);
    Assert.assertEquals(3, result.size());
    Assert.assertEquals(timestamp, result.get(0).getFileCreationTimeMs());
    Assert.assertEquals(timestamp + 1, result.get(1).getFileCreationTimeMs());
    Assert.assertEquals(timestamp + 2, result.get(2).getFileCreationTimeMs());

    result = fileLogReader.filterFilesByStartTime(logLocationList, 98);
    Assert.assertEquals(3, result.size());*/
  }
}
