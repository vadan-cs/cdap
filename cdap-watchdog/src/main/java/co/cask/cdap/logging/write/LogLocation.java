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

package co.cask.cdap.logging.write;

import ch.qos.logback.classic.spi.ILoggingEvent;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.io.SeekableInputStream;
import co.cask.cdap.logging.filter.Filter;
import co.cask.cdap.logging.read.LogEvent;
import co.cask.cdap.logging.read.LogOffset;
import co.cask.cdap.logging.serialize.LogSchema;
import co.cask.cdap.logging.serialize.LoggingEvent;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.impersonation.Impersonator;
import com.google.common.base.Throwables;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;

/**
 * LogLocation representing a log file and methods to read the file's contents.
 */
public class LogLocation {
  private static final Logger LOG = LoggerFactory.getLogger(LogLocation.class);

  // old version
  public static final String VERSION_0 = "V0";
  // new version
  public static final String VERSION_1 = "V1";
  private final String frameworkVersion;
  private final long eventTimeMs;
  private final long fileCreationTimeMs;
  private final Location location;
  private final NamespaceId namespaceId;
  private final Impersonator impersonator;

  public LogLocation(String frameworkVersion, long eventTimeMs, long fileCreationTimeMs, Location location,
                     String namespaceId, Impersonator impersonator) {
    this.frameworkVersion = frameworkVersion;
    this.eventTimeMs = eventTimeMs;
    this.fileCreationTimeMs = fileCreationTimeMs;
    this.location = location;
    this.namespaceId = new NamespaceId(namespaceId);
    this.impersonator = impersonator;
  }

  /**
   * get logging framework version
   * @return version string, currently V0 or V1
   */
  public String getFrameworkVersion() {
    return frameworkVersion;
  }

  /**
   * get start eventTimeMs for this logging file
   * @return eventTimeMs in long
   */
  public long getEventTimeMs() {
    return eventTimeMs;
  }

  /**
   * get location of log file
   * @return Location
   */
  public Location getLocation() {
    return location;
  }

  /**
   * get the timestamp associated with the file
   * @return
   */
  public long getFileCreationTimeMs() {
    return fileCreationTimeMs;
  }

  /**
   * Return closeable iterator of {@link LogEvent}
   * @param logFilter filter
   * @param fromTimeMs start timestamp in millis
   * @param toTimeMs end timestamp in millis
   * @param maxEvents max events to return
   * @return closeable iterator of log events
   */
  public CloseableIterator<LogEvent> readLog(Filter logFilter, long fromTimeMs, long toTimeMs, int maxEvents) {
    return new LogEventIterator(logFilter, fromTimeMs, toTimeMs, maxEvents);
  }

  private final class LogEventIterator implements CloseableIterator<LogEvent> {

    private final Filter logFilter;
    private final long fromTimeMs;
    private final long toTimeMs;
    private final long maxEvents;

    private DataFileReader<GenericRecord> dataFileReader;

    private ILoggingEvent loggingEvent;
    private GenericRecord datum;

    private int count = 0;
    private long prevTimestamp = -1;

    private LogEvent next;

    LogEventIterator(Filter logFilter, long fromTimeMs, long toTimeMs, long maxEvents) {
      this.logFilter = logFilter;
      this.fromTimeMs = fromTimeMs;
      this.toTimeMs = toTimeMs;
      this.maxEvents = maxEvents;

      try {
        dataFileReader = createReader();
        if (dataFileReader.hasNext()) {
          datum = dataFileReader.next();
          loggingEvent = LoggingEvent.decode(datum);
          long prevPrevSyncPos = 0;
          long prevSyncPos = 0;
          // Seek to time fromTimeMs
          while (loggingEvent.getTimeStamp() < fromTimeMs && dataFileReader.hasNext()) {
            // Seek to the next sync point
            long curPos = dataFileReader.tell();
            prevPrevSyncPos = prevSyncPos;
            prevSyncPos = dataFileReader.previousSync();
            LOG.trace("Syncing to pos {}", curPos);
            dataFileReader.sync(curPos);
            if (dataFileReader.hasNext()) {
              loggingEvent = LoggingEvent.decode(dataFileReader.next(datum));
            }
          }

          // We're now likely past the record with fromTimeMs, rewind to the previous sync point
          dataFileReader.sync(prevPrevSyncPos);
          LOG.trace("Final sync pos {}", prevPrevSyncPos);
        }

        // populate the first element
        computeNext();

      } catch (Exception e) {
        // we want to ignore invalid or missing log files
        LOG.error("Got exception while reading log file {}", location.getName(), e);
      }
    }

    // will compute the next LogEvent and set the field 'next', unless its already set
    private void computeNext() {
      try {
        // read events from file
        while (next == null && dataFileReader.hasNext()) {
          loggingEvent = LoggingEvent.decode(dataFileReader.next(datum));
          if (loggingEvent.getTimeStamp() >= fromTimeMs && logFilter.match(loggingEvent)) {
            ++count;
            if ((count > maxEvents || loggingEvent.getTimeStamp() >= toTimeMs)
              && loggingEvent.getTimeStamp() != prevTimestamp) {
              break;
            }
            next = new LogEvent(loggingEvent,
                                new LogOffset(LogOffset.INVALID_KAFKA_OFFSET, loggingEvent.getTimeStamp()));
          }
          prevTimestamp = loggingEvent.getTimeStamp();
        }
      } catch (Exception e) {
        // We want to ignore invalid or missing log files.
        // If the 'next' variable wasn't set by this method call, then the 'hasNext' method
        // will return false, and no more events will be read from this file.
        LOG.error("Got exception while reading log file {}", location.getName(), e);
      }
    }

    @Override
    public void close() {
      try {
        dataFileReader.close();
      } catch (IOException e) {
        LOG.error("Got exception while closing log file {}", location.getName(), e);
      }
    }

    @Override
    public boolean hasNext() {
      return next != null;
    }

    @Override
    public LogEvent next() {
      if (this.next == null) {
        throw new NoSuchElementException();
      }
      LogEvent toReturn = this.next;
      this.next = null;
      computeNext();
      return toReturn;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Remove not supported");
    }
  }

  private DataFileReader<GenericRecord> createReader() throws IOException {
    boolean shouldImpersonate = this.getFrameworkVersion().equals(VERSION_0);
    return new DataFileReader<>(new LocationSeekableInput(location, namespaceId, impersonator, shouldImpersonate),
                                new GenericDatumReader<GenericRecord>(LogSchema.LoggingEvent.SCHEMA));
  }

  /**
   * An implementation of Avro SeekableInput over Location.
   */
  private static final class LocationSeekableInput implements SeekableInput {

    private final SeekableInputStream is;
    private final long len;

    LocationSeekableInput(final Location location,
                          NamespaceId namespaceId, Impersonator impersonator,
                          boolean shouldImpersonate) throws IOException {
      try {
        if (shouldImpersonate) {
          this.is = impersonator.doAs(namespaceId, new Callable<SeekableInputStream>() {
            @Override
            public SeekableInputStream call() throws Exception {
              return Locations.newInputSupplier(location).getInput();
            }
          });
        } else {
          // impersonation is not required for V1 version.
          this.is = Locations.newInputSupplier(location).getInput();
        }
      } catch (IOException e) {
        throw e;
      } catch (Exception e) {
        // should not happen
        throw Throwables.propagate(e);
      }

      this.len = location.length();
    }

    @Override
    public void seek(long p) throws IOException {
      is.seek(p);
    }

    @Override
    public long tell() throws IOException {
      return is.getPos();
    }

    @Override
    public long length() throws IOException {
      return len;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      return is.read(b, off, len);
    }

    @Override
    public void close() throws IOException {
      is.close();
    }
  }
}
