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

package co.cask.cdap.logging.write;

import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.logging.context.LoggingContextHelper;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.impersonation.Impersonator;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Helper class that manages writing of KafkaLogEvent to Avro files. The events are written into appropriate files
 * based on the LoggingContext of the event. The files are also rotated based on size. This class is not thread-safe.
 */
public final class AvroFileWriter implements Closeable, Flushable {
  private static final Logger LOG = LoggerFactory.getLogger(AvroFileWriter.class);

  private final FileMetaDataManager fileMetaDataManager;
  private final NamespacedLocationFactory namespacedLocationFactory;
  private final String logBaseDir;
  private final Schema schema;
  private final int syncIntervalBytes;
  private final Map<String, AvroFile> fileMap;
  private final long maxFileSize;
  private final long maxFileLifetimeMs;
  private final Impersonator impersonator;

  private final AtomicBoolean closed = new AtomicBoolean(false);

  /**
   * Constructs an AvroFileWriter object.
   * @param fileMetaDataManager used to store file meta data.
   * @param namespacedLocationFactory the namespaced location factory
   * @param logBaseDir the basedirectory for logs as defined in configuration
   * @param schema schema of the Avro data to be written.
   * @param maxFileSize Avro files greater than maxFileSize will get rotated.
   * @param syncIntervalBytes the approximate number of uncompressed bytes to write in each block.
   * @param maxFileLifetimeMs files that are older than maxFileLifetimeMs will be closed.
   */
  public AvroFileWriter(FileMetaDataManager fileMetaDataManager, NamespacedLocationFactory namespacedLocationFactory,
                        String logBaseDir, Schema schema, long maxFileSize, int syncIntervalBytes,
                        long maxFileLifetimeMs, Impersonator impersonator) {
    this.fileMetaDataManager = fileMetaDataManager;
    this.namespacedLocationFactory = namespacedLocationFactory;
    this.logBaseDir = logBaseDir;
    this.schema = schema;
    this.syncIntervalBytes = syncIntervalBytes;
    this.fileMap = Maps.newHashMap();
    this.maxFileSize = maxFileSize;
    this.maxFileLifetimeMs = maxFileLifetimeMs;
    this.impersonator = impersonator;
  }

  /**
   * Appends a log event to an appropriate Avro file based on LoggingContext. If the log event does not contain
   * LoggingContext then the event will be dropped.
   * @param events Log event
   * @throws IOException
   */
  public void append(List<? extends LogWriteEvent> events) throws Exception {
    if (events.isEmpty()) {
      LOG.debug("Empty append list.");
      return;
    }

    LogWriteEvent event = events.get(0);
    LoggingContext loggingContext = event.getLoggingContext();

    if (LOG.isTraceEnabled()) {
      LOG.trace("Appending {} messages for logging context {}", events.size(),
                loggingContext.getLogPathFragment(logBaseDir));
    }

    long timestamp = event.getLogEvent().getTimeStamp();
    AvroFile avroFile = getAvroFile(loggingContext, timestamp);
    avroFile = rotateFile(avroFile, loggingContext, timestamp);

    for (LogWriteEvent e : events) {
      avroFile.append(e);
    }
    avroFile.flush();
  }

  @Override
  public void close() throws IOException {
    if (!closed.compareAndSet(false, true)) {
      return;
    }

    // First checkpoint state
    try {
      flush();
    } catch (Exception e) {
      LOG.error("Caught exception while checkpointing", e);
    }

    // Close all files
    LOG.debug("Closing all files");
    for (Map.Entry<String, AvroFile> entry : fileMap.entrySet()) {
      try {
        entry.getValue().close();
      } catch (Throwable e) {
        LOG.error("Caught exception while closing file {}", entry.getValue().getLocation(), e);
      }
    }
    fileMap.clear();
  }

  @Override
  public void flush() throws IOException {
    long currentTs = System.currentTimeMillis();

    for (Iterator<Map.Entry<String, AvroFile>> it = fileMap.entrySet().iterator(); it.hasNext();) {
      AvroFile avroFile = it.next().getValue();
      // TODO: refactor fileMap into a class so that all these checks (and creation of files) can move there CDAP-6475
      if (!avroFile.isOpen()) {
        // If the file is already closed (due to an exception),
        // then remove it from the map so that a new one gets created later
        it.remove();
      } else {
        avroFile.sync();

        // Close old files
        long timeSinceFileCreate = currentTs - avroFile.getCreateTime();
        if (timeSinceFileCreate > maxFileLifetimeMs) {
          avroFile.close();
          it.remove();
        }
      }
    }
  }

  private AvroFile getAvroFile(LoggingContext loggingContext, long timestamp) throws Exception {
    AvroFile avroFile = fileMap.get(loggingContext.getLogPathFragment(logBaseDir));

    // If the file is not open then set reference to null so that a new one gets created
    if (avroFile != null && !avroFile.isOpen()) {
      avroFile = null;
    }

    if (avroFile == null) {
      avroFile = createAvroFile(loggingContext, timestamp);
    }
    return avroFile;
  }

  private AvroFile createAvroFile(LoggingContext loggingContext, long timestamp) throws IOException {
    long currentTs = System.currentTimeMillis();
    Location location = createLocation(loggingContext, currentTs);
    LOG.info("Creating Avro file {}", location);
    AvroFile avroFile = new AvroFile(location);
    try {
      avroFile.open();
    } catch (IOException e) {
      closeAndDelete(avroFile);
      throw e;
    }
    try {
      fileMetaDataManager.writeMetaData(loggingContext, timestamp, location);
    } catch (Throwable e) {
      closeAndDelete(avroFile);
      throw new IOException(e);
    }
    fileMap.put(loggingContext.getLogPathFragment(logBaseDir), avroFile);
    return avroFile;
  }

  private Location createLocation(LoggingContext loggingContext, long timestamp)
    throws IOException {
    String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
    String fileName = String.format("%s.avro", timestamp);
    final NamespaceId namespaceId = LoggingContextHelper.getNamespaceId(loggingContext);
    Location namespaceLocation;
    try {
      namespaceLocation = impersonator.doAs(namespaceId, new Callable<Location>() {
        @Override
        public Location call() throws Exception {
          return namespacedLocationFactory.get(namespaceId.toId());
        }
      });
    } catch (IOException e) {
      throw e;
    } catch (Exception t) {
      Throwables.propagateIfPossible(t);

      // since the callables we execute only throw IOException (besides unchecked exceptions),
      // this should never happen
      LOG.warn("Unexpected exception while getting namespace location for namespace {}.", namespaceId, t);
      // the only checked exception that the Callables in this class is IOException, and we handle that in the previous
      // catch statement. So, no checked exceptions should be wrapped by the following statement. However, we need it
      // because ImpersonationUtils#doAs declares 'throws Exception', because it can throw other checked exceptions
      // in the general case
      throw Throwables.propagate(t);
    }
    // drop the namespaceid from path fragment since the namespaceLocation already points to the directory inside the
    // namespace
    String pathFragment = loggingContext.getLogPathFragment(logBaseDir).split(namespaceId.getNamespace())[1];
    return namespaceLocation.append(pathFragment).append(date).append(fileName);
  }

  private AvroFile rotateFile(AvroFile avroFile, LoggingContext loggingContext, long timestamp) throws Exception {
    if (!avroFile.isOpen()) {
      LOG.info("Rotating a closed file {}", avroFile.getLocation());
      return createAvroFile(loggingContext, timestamp);
    }
    if (avroFile.getPos() > maxFileSize) {
      LOG.info("Rotating file {}", avroFile.getLocation());
      avroFile.close();
      return createAvroFile(loggingContext, timestamp);
    }
    return avroFile;
  }

  private void closeAndDelete(AvroFile avroFile) {
    try {
      try {
        avroFile.close();
      } finally {
        if (avroFile.getLocation().exists()) {
          avroFile.getLocation().delete();
        }
      }
    } catch (IOException e) {
      LOG.error("Error while closing and deleting file {}", avroFile.getLocation(), e);
    }
  }

  /**
   * Represents an Avro file.
   *
   * Since there is no way to check the state of the underlying file on an exception,
   * all methods of this class assume that the file state is bad on any exception and close the file.
   */
  public class AvroFile implements Closeable {
    private final Location location;
    private FSDataOutputStream outputStream;
    private DataFileWriter<GenericRecord> dataFileWriter;
    private long createTime;
    private boolean isOpen = false;

    public AvroFile(Location location) {
      this.location = location;
    }

    /**
     * Opens the underlying file for writing.
     * If open throws an exception then underlying file may still need to be deleted.
     *
     * @throws IOException
     */
    void open() throws IOException {
      try {
        this.outputStream = new FSDataOutputStream(location.getOutputStream(), null);
        this.dataFileWriter = new DataFileWriter<>(new GenericDatumWriter<GenericRecord>(schema));
        this.dataFileWriter.create(schema, this.outputStream);
        this.dataFileWriter.setSyncInterval(syncIntervalBytes);
        this.createTime = System.currentTimeMillis();
        // Sync the file as soon as it is created, otherwise a zero length Avro file can get created on OOM
        sync();
      } catch (Exception e) {
        close();
        throw new IOException("Exception while creating file " + location, e);
      }
      this.isOpen = true;
    }

    public boolean isOpen() {
      return isOpen;
    }

    public Location getLocation() {
      return location;
    }

    public void append(LogWriteEvent event) throws IOException {
      try {
        dataFileWriter.append(event.getGenericRecord());
      } catch (Exception e) {
        close();
        throw new IOException("Exception while appending to file " + location, e);
      }
    }

    public long getPos() throws IOException {
      try {
        return outputStream.getPos();
      } catch (Exception e) {
        close();
        throw new IOException("Exception while getting position of file " + location, e);
      }
    }

    public long getCreateTime() {
      return createTime;
    }

    public void flush() throws IOException {
      try {
        dataFileWriter.flush();
        outputStream.hflush();
      } catch (Exception e) {
        close();
        throw new IOException("Exception while flushing file " + location, e);
      }
    }

    public void sync() throws IOException {
      try {
        dataFileWriter.flush();
        outputStream.hsync();
      } catch (Exception e) {
        close();
        throw new IOException("Exception while syncing file " + location, e);
      }
    }

    @Override
    public void close() throws IOException {
      if (!isOpen) {
        return;
      }

      LOG.trace("Closing file {}", location);
      isOpen = false;

      try {
        if (dataFileWriter != null) {
          dataFileWriter.close();
        }
      } finally {
        if (outputStream != null) {
          outputStream.close();
        }
      }
    }
  }
}
