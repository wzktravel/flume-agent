package com.firstshare.flume.source;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import com.firstshare.flume.service.FileHandlerRunnable;
import com.firstshare.flume.utils.FlumeUtil;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.FlumeException;
import org.apache.flume.client.avro.ReliableSpoolingFileEventReader;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.serialization.DecodeErrorPolicy;
import org.apache.flume.serialization.LineDeserializer;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.SpoolDirectorySourceConfigurationConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.*;
import static com.firstshare.flume.source.SpoolDirectoryHourlySourceConfigurationConstants.*;

/**
 * 基于SpoolDirectorySource,能每小时从logDir中将上一个小时的日志复制到spoolDir中
 * 避免在服务器中使用crontab等,减少维护成本
 * Created by wzk on 15/12/14.
 */
public class SpoolDirectoryHourlySource extends AbstractSource
    implements Configurable, EventDrivenSource {

  private static final Logger logger = LoggerFactory.getLogger(SpoolDirectoryHourlySource.class);

  // Delay used when polling for new files
  private static final int POLL_DELAY_MS = 500;
  // 每小时执行一次,将日志从logDir拷贝到spoolDirectory
  private static final int ROLL_DELAY_HOUR = 60 * 60 * 1000;
  // 每天执行一次
  private static final int ROLL_DELAY_DAY = 24 * 60 * 60 * 1000;

  /* Config options */
  private String completedSuffix;
  private String spoolDirectory;
  private boolean fileHeader;
  private String fileHeaderKey;
  private boolean basenameHeader;
  private String basenameHeaderKey;
  private int batchSize;
  private String ignorePattern;
  private String trackerDirPath;
  private String deserializerType;
  private Context deserializerContext;
  private String deletePolicy;
  private String inputCharset;
  private DecodeErrorPolicy decodeErrorPolicy;
  private String logDir;
  private String filePrefix;
  private String rollUnit;
  private int rollMinutes;
  private String fileCompressionMode;
  private int fileMaxHistory;
  private String dateFormat;

  private volatile boolean hasFatalError = false;

  private SourceCounter sourceCounter;
  ReliableSpoolingFileEventReader reader;
  private ScheduledExecutorService executor;
  private FileHandlerRunnable fileHandler;
  private ScheduledExecutorService fileHandleExcutor;
  private boolean backoff = true;
  private boolean hitChannelException = false;
  private int maxBackoff;
  private SpoolDirectorySourceConfigurationConstants.ConsumeOrder consumeOrder;

  @Override
  public synchronized void start() {
    logger.info("SpoolDirectorySource source starting with directory: {}", spoolDirectory);

    fileHandleExcutor = Executors.newSingleThreadScheduledExecutor();
    executor = Executors.newSingleThreadScheduledExecutor();

    File directory = new File(spoolDirectory);
    try {
      reader = new ReliableSpoolingFileEventReader.Builder()
          .spoolDirectory(directory)
          .completedSuffix(completedSuffix)
          .ignorePattern(ignorePattern)
          .trackerDirPath(trackerDirPath)
          .annotateFileName(fileHeader)
          .fileNameHeader(fileHeaderKey)
          .annotateBaseName(basenameHeader)
          .baseNameHeader(basenameHeaderKey)
          .deserializerType(deserializerType)
          .deserializerContext(deserializerContext)
          .deletePolicy(deletePolicy)
          .inputCharset(inputCharset)
          .decodeErrorPolicy(decodeErrorPolicy)
          .consumeOrder(consumeOrder)
          .build();
    } catch (IOException ioe) {
      throw new FlumeException("Error instantiating spooling event parser", ioe);
    }

    fileHandler = new FileHandlerRunnable(logDir, spoolDirectory, filePrefix, completedSuffix,
                                        fileCompressionMode, fileMaxHistory, dateFormat, rollUnit);
    if (StringUtils.equals(rollUnit, "day")) {
      long millisecondsToNextDay = FlumeUtil.getMilliSecondsToNextDay() + rollMinutes * 60 * 1000;
      fileHandleExcutor.scheduleAtFixedRate(fileHandler, millisecondsToNextDay, ROLL_DELAY_DAY, TimeUnit.MILLISECONDS);
    } else {
      long millisecondsToNextHour = FlumeUtil.getMilliSecondsToNextHour() + rollMinutes * 60 * 1000;
      fileHandleExcutor.scheduleAtFixedRate(fileHandler, millisecondsToNextHour, ROLL_DELAY_HOUR, TimeUnit.MILLISECONDS);
    }

    Runnable runner = new SpoolDirectoryRunnable(reader, sourceCounter);
    executor.scheduleWithFixedDelay(runner, 0, POLL_DELAY_MS, TimeUnit.MILLISECONDS);

    super.start();
    logger.debug("SpoolDirectoryHourlySource source started");
    sourceCounter.start();
  }

  @Override
  public synchronized void stop() {
    fileHandler.waitForAsynchronousJobToStop();
    executor.shutdown();
    fileHandleExcutor.shutdown();
    try {
      executor.awaitTermination(10L, TimeUnit.SECONDS);
      fileHandleExcutor.awaitTermination(10L, TimeUnit.SECONDS);
    } catch (InterruptedException ex) {
      logger.info("Interrupted while awaiting termination", ex);
    }
    executor.shutdownNow();
    fileHandleExcutor.shutdownNow();

    super.stop();
    sourceCounter.stop();
    logger.info("SpoolDir source {} stopped. Metrics: {}", getName(), sourceCounter);
  }

  @Override
  public String toString() {
    return "Spool Directory source " + getName() +
           ": { spoolDir: " + spoolDirectory + " }";
  }

  @Override
  public synchronized void configure(Context context) {
    logDir = context.getString(LOG_DIRECTORY);
    Preconditions.checkState(logDir != null,
                             "Configuration must specify a log directory");
    spoolDirectory = context.getString(SPOOL_DIRECTORY);
    Preconditions.checkState(spoolDirectory != null,
                             "Configuration must specify a spooling directory");

    completedSuffix = context.getString(SPOOLED_FILE_SUFFIX, DEFAULT_SPOOLED_FILE_SUFFIX);
    deletePolicy = context.getString(DELETE_POLICY, DEFAULT_DELETE_POLICY);
    fileHeader = context.getBoolean(FILENAME_HEADER, DEFAULT_FILE_HEADER);
    fileHeaderKey = context.getString(FILENAME_HEADER_KEY, DEFAULT_FILENAME_HEADER_KEY);
    basenameHeader = context.getBoolean(BASENAME_HEADER, DEFAULT_BASENAME_HEADER);
    basenameHeaderKey = context.getString(BASENAME_HEADER_KEY, DEFAULT_BASENAME_HEADER_KEY);
    batchSize = context.getInteger(BATCH_SIZE, DEFAULT_BATCH_SIZE);
    inputCharset = context.getString(INPUT_CHARSET, DEFAULT_INPUT_CHARSET);
    decodeErrorPolicy = DecodeErrorPolicy.valueOf(
        context.getString(DECODE_ERROR_POLICY, DEFAULT_DECODE_ERROR_POLICY)
            .toUpperCase());

    ignorePattern = context.getString(IGNORE_PAT, DEFAULT_IGNORE_PAT);
    trackerDirPath = context.getString(TRACKER_DIR, DEFAULT_TRACKER_DIR);

    deserializerType = context.getString(DESERIALIZER, DEFAULT_DESERIALIZER);
    deserializerContext = new Context(context.getSubProperties(DESERIALIZER + "."));

    consumeOrder = SpoolDirectorySourceConfigurationConstants.ConsumeOrder
        .valueOf(context.getString(CONSUME_ORDER, DEFAULT_CONSUME_ORDER.toString()).toUpperCase());

    // "Hack" to support backwards compatibility with previous generation of
    // spooling directory source, which did not support deserializers
    Integer bufferMaxLineLength = context.getInteger(BUFFER_MAX_LINE_LENGTH);
    if (bufferMaxLineLength != null && deserializerType != null &&
        deserializerType.equalsIgnoreCase(DEFAULT_DESERIALIZER)) {
      deserializerContext.put(LineDeserializer.MAXLINE_KEY, bufferMaxLineLength.toString());
    }

    maxBackoff = context.getInteger(MAX_BACKOFF, DEFAULT_MAX_BACKOFF);
    if (sourceCounter == null) {
      sourceCounter = new SourceCounter(getName());
    }

    filePrefix = context.getString(FILE_PREFIX, DEFAULT_FILE_PREFIX);
    rollUnit = context.getString(ROLL_UNIT, DEFAULT_ROLL_UNIT);
    rollMinutes = context.getInteger(ROLL_MINUTES, DEFAULT_ROOL_MINUTES);
    fileCompressionMode = context.getString(FILE_COMPRESSIONMODE, DEFAULT_FILE_COMPRESSIONMODE);
    fileMaxHistory = context.getInteger(FILE_MAXHISTORY, DEFAULT_FILE_MAXHISTORY);
    dateFormat = context.getString(FILE_DATEFORMAT, DEFAULT_FILE_DATEFORMAT);
  }

  @VisibleForTesting
  protected boolean hasFatalError() {
    return hasFatalError;
  }


  /**
   * The class always backs off, this exists only so that we can test without taking a really long
   * time.
   *
   * @param backoff - whether the source should backoff if the channel is full
   */
  @VisibleForTesting
  protected void setBackOff(boolean backoff) {
    this.backoff = backoff;
  }

  @VisibleForTesting
  protected boolean hitChannelException() {
    return hitChannelException;
  }

  @VisibleForTesting
  protected SourceCounter getSourceCounter() {
    return sourceCounter;
  }

  private class SpoolDirectoryRunnable implements Runnable {

    private ReliableSpoolingFileEventReader reader;
    private SourceCounter sourceCounter;

    public SpoolDirectoryRunnable(ReliableSpoolingFileEventReader reader,
                                  SourceCounter sourceCounter) {
      this.reader = reader;
      this.sourceCounter = sourceCounter;
    }

    @Override
    public void run() {
      int backoffInterval = 250;
      try {
        while (!Thread.interrupted()) {
          List<Event> events = reader.readEvents(batchSize);
          if (events.isEmpty()) {
            break;
          }
          sourceCounter.addToEventReceivedCount(events.size());
          sourceCounter.incrementAppendBatchReceivedCount();

          try {
            getChannelProcessor().processEventBatch(events);
            reader.commit();
          } catch (ChannelException ex) {
            logger.warn("The channel is full, and cannot write data now. The " +
                        "source will try again after " + String.valueOf(backoffInterval) +
                        " milliseconds");
            hitChannelException = true;
            if (backoff) {
              TimeUnit.MILLISECONDS.sleep(backoffInterval);
              backoffInterval = backoffInterval << 1;
              backoffInterval = backoffInterval >= maxBackoff ? maxBackoff :
                                backoffInterval;
            }
            continue;
          }
          backoffInterval = 250;
          sourceCounter.addToEventAcceptedCount(events.size());
          sourceCounter.incrementAppendBatchAcceptedCount();
        }
        logger.debug("Spooling Directory Source runner has shutdown.");
      } catch (Throwable t) {
        logger.error("FATAL: " + SpoolDirectoryHourlySource.this.toString() + ": " +
                     "Uncaught exception in SpoolDirectorySource thread. " +
                     "Restart or reconfigure Flume to continue processing.", t);
        hasFatalError = true;
        Throwables.propagate(t);
      }
    }
  }

}