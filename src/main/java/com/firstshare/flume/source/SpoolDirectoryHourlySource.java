package com.firstshare.flume.source;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.Files;

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
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.firstshare.flume.source.SpoolDirectoryHourlySourceConfigurationConstants.DEFAULT_FILE_PREFIX;
import static com.firstshare.flume.source.SpoolDirectoryHourlySourceConfigurationConstants.FILE_PREFIX;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.BASENAME_HEADER;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.BASENAME_HEADER_KEY;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.BATCH_SIZE;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.BUFFER_MAX_LINE_LENGTH;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.CONSUME_ORDER;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.DECODE_ERROR_POLICY;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.DEFAULT_BASENAME_HEADER;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.DEFAULT_BASENAME_HEADER_KEY;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.DEFAULT_BATCH_SIZE;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.DEFAULT_CONSUME_ORDER;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.DEFAULT_DECODE_ERROR_POLICY;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.DEFAULT_DELETE_POLICY;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.DEFAULT_DESERIALIZER;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.DEFAULT_FILENAME_HEADER_KEY;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.DEFAULT_FILE_HEADER;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.DEFAULT_IGNORE_PAT;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.DEFAULT_INPUT_CHARSET;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.DEFAULT_MAX_BACKOFF;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.DEFAULT_SPOOLED_FILE_SUFFIX;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.DEFAULT_TRACKER_DIR;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.DELETE_POLICY;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.DESERIALIZER;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.FILENAME_HEADER;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.FILENAME_HEADER_KEY;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.IGNORE_PAT;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.INPUT_CHARSET;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.MAX_BACKOFF;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.SPOOLED_FILE_SUFFIX;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.SPOOL_DIRECTORY;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.TRACKER_DIR;
import static com.firstshare.flume.source.SpoolDirectoryHourlySourceConfigurationConstants.LOG_DIRECTORY;
import static com.firstshare.flume.source.SpoolDirectoryHourlySourceConfigurationConstants.ROLL_MINUTES;
import static com.firstshare.flume.source.SpoolDirectoryHourlySourceConfigurationConstants.DEFAULT_ROOL_MINUTES;

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
  private static final int ROLL_DELAY_MS = 60 * 60 * 1000;

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
  private int rollMinutes;
  private volatile boolean hasFatalError = false;

  private SourceCounter sourceCounter;
  ReliableSpoolingFileEventReader reader;
  private ScheduledExecutorService executor;
  private ScheduledExecutorService copyAndRenameExcecutor;
  private boolean backoff = true;
  private boolean hitChannelException = false;
  private int maxBackoff;
  private SpoolDirectorySourceConfigurationConstants.ConsumeOrder consumeOrder;

  @Override
  public synchronized void start() {
    logger.info("SpoolDirectorySource source starting with directory: {}", spoolDirectory);

    copyAndRenameExcecutor = Executors.newSingleThreadScheduledExecutor();
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

    long millisecondsToNextHour = FlumeUtil.getMilliSecondsToNextHour() + rollMinutes * 60 * 1000;
    Runnable copyAndRenameRunner = new CopyAndRenameRunnable();
    copyAndRenameExcecutor.scheduleAtFixedRate(copyAndRenameRunner, millisecondsToNextHour,
                                               ROLL_DELAY_MS, TimeUnit.MILLISECONDS);

    Runnable runner = new SpoolDirectoryRunnable(reader, sourceCounter);
    executor.scheduleWithFixedDelay(runner, 0, POLL_DELAY_MS, TimeUnit.MILLISECONDS);

    super.start();
    logger.debug("SpoolDirectoryHourlySource source started");
    sourceCounter.start();
  }

  @Override
  public synchronized void stop() {
    executor.shutdown();
    copyAndRenameExcecutor.shutdown();
    try {
      executor.awaitTermination(10L, TimeUnit.SECONDS);
      copyAndRenameExcecutor.awaitTermination(10L, TimeUnit.SECONDS);
    } catch (InterruptedException ex) {
      logger.info("Interrupted while awaiting termination", ex);
    }
    executor.shutdownNow();
    copyAndRenameExcecutor.shutdownNow();

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

    completedSuffix = context.getString(SPOOLED_FILE_SUFFIX,
                                        DEFAULT_SPOOLED_FILE_SUFFIX);
    deletePolicy = context.getString(DELETE_POLICY, DEFAULT_DELETE_POLICY);
    fileHeader = context.getBoolean(FILENAME_HEADER,
                                    DEFAULT_FILE_HEADER);
    fileHeaderKey = context.getString(FILENAME_HEADER_KEY,
                                      DEFAULT_FILENAME_HEADER_KEY);
    basenameHeader = context.getBoolean(BASENAME_HEADER,
                                        DEFAULT_BASENAME_HEADER);
    basenameHeaderKey = context.getString(BASENAME_HEADER_KEY,
                                          DEFAULT_BASENAME_HEADER_KEY);
    batchSize = context.getInteger(BATCH_SIZE,
                                   DEFAULT_BATCH_SIZE);
    inputCharset = context.getString(INPUT_CHARSET, DEFAULT_INPUT_CHARSET);
    decodeErrorPolicy = DecodeErrorPolicy.valueOf(
        context.getString(DECODE_ERROR_POLICY, DEFAULT_DECODE_ERROR_POLICY)
            .toUpperCase());

    ignorePattern = context.getString(IGNORE_PAT, DEFAULT_IGNORE_PAT);
    trackerDirPath = context.getString(TRACKER_DIR, DEFAULT_TRACKER_DIR);

    deserializerType = context.getString(DESERIALIZER, DEFAULT_DESERIALIZER);
    deserializerContext = new Context(context.getSubProperties(DESERIALIZER +
                                                               "."));

    consumeOrder = SpoolDirectorySourceConfigurationConstants.ConsumeOrder
        .valueOf(context.getString(CONSUME_ORDER,
                                   DEFAULT_CONSUME_ORDER.toString()).toUpperCase());

    // "Hack" to support backwards compatibility with previous generation of
    // spooling directory source, which did not support deserializers
    Integer bufferMaxLineLength = context.getInteger(BUFFER_MAX_LINE_LENGTH);
    if (bufferMaxLineLength != null && deserializerType != null &&
        deserializerType.equalsIgnoreCase(DEFAULT_DESERIALIZER)) {
      deserializerContext.put(LineDeserializer.MAXLINE_KEY,
                              bufferMaxLineLength.toString());
    }

    maxBackoff = context.getInteger(MAX_BACKOFF, DEFAULT_MAX_BACKOFF);
    if (sourceCounter == null) {
      sourceCounter = new SourceCounter(getName());
    }

    filePrefix = context.getString(FILE_PREFIX, DEFAULT_FILE_PREFIX);
    rollMinutes = context.getInteger(ROLL_MINUTES, DEFAULT_ROOL_MINUTES);

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

  private class CopyAndRenameRunnable implements Runnable {

    @Override
    public void run() {
      File logPath = new File(logDir);
      String[] logs = new String[0];
      if (logPath.isDirectory()) {
        logs = logPath.list(new LogFilenameFilter());
      }
      if (logs == null || logs.length < 1) {
        logger.warn("no matched logs found in {}", logDir);
        return;
      }
      for (String from : logs) {
        String copyFrom = logDir + "/" + from;
        String copyTo = spoolDirectory + "/" + from + completedSuffix;
        String moveTo = spoolDirectory + "/" + from;
        File copySrcFile = new File(copyFrom);
        File copyDstFile = new File(copyTo);
        File moveDstFile = new File(moveTo);
        try {
          Files.copy(copySrcFile, copyDstFile);
          logger.info("copy file {} to {}.", from, copyTo);
        } catch (IOException e) {
          logger.error("connot copy file {} to {}.", from, copyTo);
        }
        try {
          Files.move(copyDstFile, moveDstFile);
          logger.info("move file {} to {}.", copyTo, moveTo);
        } catch (IOException e) {
          logger.error("connot move file {} to {}.", copyTo, moveTo);
        }
      }
    }
  }

  private class LogFilenameFilter implements FilenameFilter {

    /**
     * 匹配前缀, 不匹配后缀, 日期符合
     * @param dir
     * @param name
     * @return
     */
    @Override
    public boolean accept(File dir, String name) {
      if (StringUtils.isEmpty(name) || !StringUtils.startsWith(name, filePrefix)) {
        return false;
      }
      if (StringUtils.endsWith(name, completedSuffix)) {
        return false;
      }
      String lastHour = FlumeUtil.getLastHour();
      if (StringUtils.contains(name, lastHour)) {
        return true;
      }
      return false;
    }
  }

}