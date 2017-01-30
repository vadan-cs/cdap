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

package co.cask.cdap.logging.appender.file;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.io.RootLocationFactory;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.logging.LoggingConfiguration;
import co.cask.cdap.logging.appender.LogAppender;
import co.cask.cdap.logging.appender.LogMessage;
import co.cask.cdap.logging.save.LogSaverTableUtil;
import co.cask.cdap.logging.serialize.LogSchema;
import co.cask.cdap.logging.serialize.LoggingEvent;
import co.cask.cdap.logging.write.AvroFileWriter;
import co.cask.cdap.logging.write.FileMetaDataManager;
import co.cask.cdap.logging.write.LogCleanup;
import co.cask.cdap.logging.write.LogFileWriter;
import co.cask.cdap.logging.write.LogWriteEvent;
import co.cask.cdap.logging.write.SimpleLogFileWriter;
import co.cask.cdap.security.impersonation.Impersonator;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.tephra.TransactionExecutorFactory;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Logback appender that writes log events to files.
 */
public class FileLogAppender extends LogAppender {
  private static final Logger LOG = LoggerFactory.getLogger(FileLogAppender.class);

  private static final String APPENDER_NAME = "FileLogAppender";

  private final CConfiguration cConf;
  private final LogSaverTableUtil tableUtil;
  private final TransactionExecutorFactory txExecutorFactory;
  private final NamespacedLocationFactory namespacedLocationFactory;
  private final RootLocationFactory rootLocationFactory;
  private final NamespaceQueryAdmin namespaceQueryAdmin;
  private final String logBaseDir;
  private final int syncIntervalBytes;
  private final long retentionDurationMs;
  private final long maxLogFileSizeBytes;
  private final long maxFileLifetimeMs;
  private final long checkpointIntervalMs;
  private final int logCleanupIntervalMins;
  private final ListeningScheduledExecutorService scheduledExecutor;
  private final Impersonator impersonator;

  private final AtomicBoolean stopped = new AtomicBoolean(false);

  private LogFileWriter<LogWriteEvent> logFileWriter;
  private Schema logSchema;

  @Inject
  public FileLogAppender(CConfiguration cConfig,
                         DatasetFramework dsFramework,
                         TransactionExecutorFactory txExecutorFactory,
                         RootLocationFactory rootLocationFactory, NamespaceQueryAdmin namespaceQueryAdmin,
                         NamespacedLocationFactory namespacedLocationFactory, Impersonator impersonator) {
    setName(APPENDER_NAME);
    this.cConf = cConfig;
    this.tableUtil = new LogSaverTableUtil(dsFramework, cConfig);
    this.txExecutorFactory = txExecutorFactory;
    this.namespacedLocationFactory = namespacedLocationFactory;
    this.rootLocationFactory = rootLocationFactory;
    this.impersonator = impersonator;
    this.namespaceQueryAdmin = namespaceQueryAdmin;

    this.logBaseDir = cConfig.get(LoggingConfiguration.LOG_BASE_DIR);
    Preconditions.checkNotNull(logBaseDir, "Log base dir cannot be null");

    // Sync interval should be around 10 times smaller than file size as we use sync points to navigate
    this.syncIntervalBytes = cConfig.getInt(LoggingConfiguration.LOG_FILE_SYNC_INTERVAL_BYTES, 2 * 1024 * 1024);
    Preconditions.checkArgument(this.syncIntervalBytes > 0,
                                "Log file sync interval is invalid: %s", this.syncIntervalBytes);

    long retentionDurationDays = cConfig.getLong(LoggingConfiguration.LOG_RETENTION_DURATION_DAYS, -1);
    Preconditions.checkArgument(retentionDurationDays > 0,
                                "Log file retention duration is invalid: %s", retentionDurationDays);
    this.retentionDurationMs = TimeUnit.MILLISECONDS.convert(retentionDurationDays, TimeUnit.DAYS);

    maxLogFileSizeBytes = cConfig.getLong(LoggingConfiguration.LOG_MAX_FILE_SIZE_BYTES, 20 * 1024 * 1024);
    Preconditions.checkArgument(maxLogFileSizeBytes > 0,
                                "Max log file size is invalid: %s", maxLogFileSizeBytes);

    maxFileLifetimeMs = cConfig.getLong(LoggingConfiguration.LOG_SAVER_MAX_FILE_LIFETIME,
                                        LoggingConfiguration.DEFAULT_LOG_SAVER_MAX_FILE_LIFETIME_MS);
    Preconditions.checkArgument(maxFileLifetimeMs > 0,
                                "Max file lifetime is invalid: %s", maxFileLifetimeMs);

    if (cConf.get(LoggingConfiguration.LOG_SAVER_INACTIVE_FILE_INTERVAL_MS) != null) {
      LOG.warn("Parameter '{}' is no longer supported. Instead, use '{}'.",
               LoggingConfiguration.LOG_SAVER_INACTIVE_FILE_INTERVAL_MS,
               LoggingConfiguration.LOG_SAVER_MAX_FILE_LIFETIME);
    }

    checkpointIntervalMs = cConfig.getLong(LoggingConfiguration.LOG_SAVER_CHECKPOINT_INTERVAL_MS,
                                                LoggingConfiguration.DEFAULT_LOG_SAVER_CHECKPOINT_INTERVAL_MS);
    Preconditions.checkArgument(checkpointIntervalMs > 0,
                                "Checkpoint interval is invalid: %s", checkpointIntervalMs);

    logCleanupIntervalMins = cConfig.getInt(LoggingConfiguration.LOG_CLEANUP_RUN_INTERVAL_MINS,
                                            LoggingConfiguration.DEFAULT_LOG_CLEANUP_RUN_INTERVAL_MINS);
    Preconditions.checkArgument(logCleanupIntervalMins > 0,
                                "Log cleanup run interval is invalid: %s", logCleanupIntervalMins);

    this.scheduledExecutor =
      MoreExecutors.listeningDecorator(Executors.newSingleThreadScheduledExecutor(
        Threads.createDaemonThreadFactory("file-log-appender")));
  }

  @Override
  public void start() {
    super.start();
    try {
      logSchema = LogSchema.LoggingEvent.SCHEMA;
      FileMetaDataManager fileMetaDataManager = new FileMetaDataManager(tableUtil, txExecutorFactory,
                                                                        rootLocationFactory, namespacedLocationFactory,
                                                                        cConf, impersonator);

      AvroFileWriter avroFileWriter = new AvroFileWriter(fileMetaDataManager, namespacedLocationFactory, logBaseDir,
                                                         logSchema, maxLogFileSizeBytes, syncIntervalBytes,
                                                         maxFileLifetimeMs, impersonator);
      logFileWriter = new SimpleLogFileWriter(avroFileWriter, checkpointIntervalMs);

      LogCleanup logCleanup = new LogCleanup(fileMetaDataManager, rootLocationFactory, namespaceQueryAdmin,
                                             namespacedLocationFactory, logBaseDir, retentionDurationMs, impersonator);
      scheduledExecutor.scheduleAtFixedRate(logCleanup, 10, logCleanupIntervalMins, TimeUnit.MINUTES);
    } catch (Exception e) {
      close();
      throw Throwables.propagate(e);
    }
  }

  @Override
  protected void append(LogMessage logMessage) {
    try {
      GenericRecord datum = LoggingEvent.encode(logSchema, logMessage.getLoggingEvent(),
                                                logMessage.getLoggingContext());
      logFileWriter.append(ImmutableList.of(new LogWriteEvent(datum, logMessage.getLoggingEvent(),
                                                              logMessage.getLoggingContext())));
    } catch (Throwable t) {
      LOG.error("Got exception while serializing log event {}.", logMessage.getLoggingEvent(), t);
    }
  }

  private void close() {
    try {
      if (logFileWriter != null) {
        logFileWriter.close();
      }
    } catch (IOException e) {
      LOG.error("Got exception while closing logFileWriter", e);
    }
  }

  @Override
  public void stop() {
    if (!stopped.compareAndSet(false, true)) {
      return;
    }

    scheduledExecutor.shutdownNow();
    try {
      scheduledExecutor.awaitTermination(5, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      LOG.debug("Interrupted while waiting for threads to terminate.");
      Thread.currentThread().interrupt();
    }
    close();
    super.stop();
  }
}
