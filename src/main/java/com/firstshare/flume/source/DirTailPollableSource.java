package com.firstshare.flume.source;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.RandomAccessFile;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 使用轮询监测目录下最新的文件
 * 配置项：
 * 1. path: 监测的目录
 * 2. filePrefix: 文件前缀
 * 3. scanPeriod: 每多长时间监测一次，单位ms，默认1s
 * 4. debugThroughput: debug开关
 *
 * Created by wangzk on 2015/11/25.
 */
public class DirTailPollableSource extends AbstractSource implements Configurable, PollableSource {

  private static final Logger logger = LoggerFactory.getLogger(DirTailPollableSource.class);

  private String path;
  private String filePrefix;
  private long scanPeriod;
  private boolean debugThroughput;

  private Timer scannerTimer;
  private Timer throughputTimer;

  private Thread tailThread;
  private boolean run;

  private File lastModifiedFile;

  private long totalCount;
  private long throughput;

  private ChannelProcessor channelProcessor;

  private LinkedBlockingQueue<String> queue;

  @Override
  public void configure(Context context) {
    String path = context.getString("path", "/tmp");
    String filePrefix = context.getString("filePrefix", "");
    long scanPeriod = context.getLong("scanPeriod", 1000L);
    boolean debugThroughput = context.getBoolean("debugThroughput", false);

    this.path = path;
    this.filePrefix = filePrefix;
    this.scanPeriod = scanPeriod;
    this.debugThroughput = debugThroughput;

    queue = new LinkedBlockingQueue<String>();
  }

  @Override
  public void start() {

    logger.info("{} is starting..................", this.getClass().getSimpleName());

    channelProcessor = getChannelProcessor();

    scannerTimer = new Timer("scannerTimerThread", true);
    scannerTimer.scheduleAtFixedRate(new TimerTask() {

      public void run() {
        // new file
        File newLastModifiedFile = lastFileModified(path);
        // check for new file
        if (lastModifiedFile == null || !newLastModifiedFile.getPath()
            .equals(lastModifiedFile.getPath())) {
          // change
          lastModifiedFile = newLastModifiedFile;
          if (lastModifiedFile == null) {
            return;
          }
          logger.info("Detected new last modified file: {}", lastModifiedFile.getPath());

          run = false;
          if (tailThread != null) {
            tailThread.interrupt();
          }
          run = true;
          TailRunner tailRunner = new TailRunner();
          tailThread = new Thread(tailRunner);
          tailThread.setDaemon(true);
          tailThread.start();
        }
      }
    }, 0L, scanPeriod);

    if (debugThroughput) {
      throughputTimer = new Timer("throughputTimerThread", true);
      throughputTimer.scheduleAtFixedRate(new TimerTask() {
        long beforeTotalCount = 0;
        public void run() {
          throughput = totalCount - beforeTotalCount;
          logger.debug("totalCount= {}, throughput= {}", totalCount, throughput);
          beforeTotalCount = totalCount;
        }
      }, 0L, 1000);
    }

    logger.info("{} is started successfully.", this.getClass().getSimpleName());
  }

  @Override
  public void stop() {
    channelProcessor.close();
    run = false;
    if (tailThread != null) {
      tailThread.interrupt();
    }

    if (scannerTimer != null) {
      try {
        scannerTimer.cancel();
      } catch (Exception e) {

      } finally {
        scannerTimer = null;
      }
    }

    if (throughputTimer != null) {
      try {
        throughputTimer.cancel();
      } catch (Exception e) {

      } finally {
        throughputTimer = null;
      }
    }

    queue = null;
  }

  @Override
  public Status process() throws EventDeliveryException {
    Status status = Status.READY;
    channelProcessor = getChannelProcessor();
    try {
      String line = queue.take();
      Event e = EventBuilder.withBody(line.getBytes());
      channelProcessor.processEvent(e);
    } catch (Throwable t) {
      status = Status.BACKOFF;
      // re-throw all Errors
      if (t instanceof Error) {
        throw (Error) t;
      }
    }
    return status;
  }

  private class TailRunner implements Runnable {

    private RandomAccessFile randomFile;

    @Override
    public void run() {
      try {
        randomFile = new RandomAccessFile(lastModifiedFile, "r");
        randomFile.seek(randomFile.length());

        String line = null;
        while (run) {
          line = randomFile.readLine();
          if (line == null) {
            Thread.sleep(10);
            continue;
          }
          queue.offer(line);
          totalCount++;
        }
      } catch (Exception e) {
        Thread.currentThread().interrupt();
        logger.error("Error occurred: ", e);
      }
    }
  }

  /**
   * Find last modified file in the directory.
   *
   * @param dir directory
   * @return last modified file
   */
  private File lastFileModified(String dir) {
    File fl = new File(dir);
    File[] files = fl.listFiles(new FileFilter() {
      public boolean accept(File file) {
        if (file.getName().startsWith(filePrefix)) {
          return true;
        }
        return false;
      }
    });
    long lastMod = Long.MIN_VALUE;
    File choise = null;
    for (File file : files) {
      if (file.lastModified() > lastMod) {
        choise = file;
        lastMod = file.lastModified();
      }
    }
    return choise;
  }
}
