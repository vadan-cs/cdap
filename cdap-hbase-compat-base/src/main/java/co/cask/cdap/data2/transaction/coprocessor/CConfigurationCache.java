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

package co.cask.cdap.data2.transaction.coprocessor;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data2.transaction.queue.hbase.coprocessor.CConfigurationReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * {@link Thread} that refreshes {@link CConfiguration} periodically.
 */
public class CConfigurationCache {
  private static final Logger LOG = LoggerFactory.getLogger(CConfigurationCache.class);
  private static final String CCONF_UPDATE_PERIOD = "cdap.transaction.coprocessor.configuration.update.period.secs";
  private static final Long DEFAULT_CCONF_UPDATE_PERIOD = TimeUnit.MINUTES.toMillis(5);


  private final CConfigurationReader cConfReader;

  private Thread refreshThread;
  private CConfiguration cConf;
  private long cConfUpdatePeriodInMillis = DEFAULT_CCONF_UPDATE_PERIOD;
  private long lastUpdated;

  public CConfigurationCache(Configuration hConf, String sysConfigTablePrefix) {
    this.cConfReader = new CConfigurationReader(hConf, sysConfigTablePrefix);
    startRefreshThread();
  }

  @Nullable
  public CConfiguration getCConf() {
    return cConf;
  }

  public boolean isAlive() {
    return refreshThread.isAlive();
  }

  public void stop() {
    if (refreshThread != null) {
      refreshThread.interrupt();
    }
  }

  private void startRefreshThread() {
    refreshThread = new Thread("cdap-configuration-cache-refresh") {
      @Override
      public void run() {
        while (!isInterrupted()) {
          long now = System.currentTimeMillis();
          if (now > (lastUpdated + cConfUpdatePeriodInMillis)) {
            try {
              CConfiguration newCConf = cConfReader.read();
              if (newCConf != null) {
                cConf = newCConf;
                lastUpdated = now;
                cConfUpdatePeriodInMillis = cConf.getLong(CCONF_UPDATE_PERIOD, DEFAULT_CCONF_UPDATE_PERIOD);
              }
            } catch (TableNotFoundException ex) {
              LOG.warn("CConfiguration table not found : {}", ex.getMessage(), ex);
              break;
            } catch (IOException ex) {
              LOG.warn("Error updating cConf", ex);
            }
          }

          try {
            TimeUnit.SECONDS.sleep(1);
          } catch (InterruptedException ex) {
            interrupt();
            break;
          }
        }
        LOG.info("CConfiguration update thread terminated.");
      }
    };

    refreshThread.setDaemon(true);
    refreshThread.start();
  }
}
