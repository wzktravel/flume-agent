package com.firstshare.flume.source;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;

import com.firstshare.flume.api.IFileListener;
import com.firstshare.flume.service.WatchServiceFilter;
import com.firstshare.flume.watcher.FileModifyWatcher;

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
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 使用WatchService监测目录下最新的文件
 * 配置项：
 * 1. path: 监测的目录
 * 2. filePrefix: 文件前缀
 * 每多长时间监测一次，单位ms，默认1s 4. debugThroughput: debug开关
 *
 * Created by wangzk on 2015/11/25.
 */
public class DirTailPollableSource2 extends AbstractSource implements Configurable, PollableSource {

  private static final Logger logger = LoggerFactory.getLogger(DirTailPollableSource2.class);
  private static final char SPLITER = '\u0001';

  private String path;
  private String filePrefix;
  private boolean debugThroughput;
  private String appName;

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
    this.path = context.getString("path", "/tmp");
    filePrefix = context.getString("filePrefix", "");
    this.debugThroughput = context.getBoolean("debugThroughput", false);
    this.appName = context.getString("appName", "");

    this.queue = new LinkedBlockingQueue<String>();
  }

  @Override
  public void start() {

    logger.info("{} is starting..................", this.getClass().getSimpleName());

    channelProcessor = getChannelProcessor();

    Path watchPath = Paths.get(path);
    FileModifyWatcher watcher = FileModifyWatcher.newInstance();
    watcher.watch(watchPath, new WatchServiceFilter(filePrefix), new IFileListener() {
      @Override
      public void changed(Path path) {
        File newLastModifiedFile = path.toFile();
        if (lastModifiedFile == null || !newLastModifiedFile.getPath()
            .equals(lastModifiedFile.getPath())) {
          lastModifiedFile = newLastModifiedFile;
          if (lastModifiedFile != null && lastModifiedFile.exists()) {
            logger.info("Thread {} detected new last modified file: {}",
                        Thread.currentThread().getName(), lastModifiedFile.getPath());
            restart();
          }
        }
      }
    });

    if (debugThroughput) {
      throughputTimer = new Timer("throughputTimerThread", true);
      throughputTimer.scheduleAtFixedRate(new TimerTask() {
        long beforeTotalCount = 0;

        public void run() {
          throughput = totalCount - beforeTotalCount;
          logger.info("totalCount= {}, throughput= {}", totalCount, throughput);
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

  private void restart() {
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

  @Override
  public Status process() throws EventDeliveryException {
    Status status = Status.READY;
    channelProcessor = getChannelProcessor();
    try {
      String line = queue.take();
      if (!Strings.isNullOrEmpty(appName)) {
        line += (SPLITER + appName);
      }
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
      } finally {
        if (randomFile != null) {
          try {
            randomFile.close();
          } catch (IOException e1) {
          }
        }
      }
    }
  }
}