package com.firstshare.flume.source;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;

import com.firstshare.flume.service.WatchServiceFilter;
import com.firstshare.flume.utils.IpUtils;
import com.firstshare.flume.watcher.FileModifyWatcher;

import org.apache.flume.ChannelException;
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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

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
  private static final char SPLITTER = '\u0001';

  private String path;
  private String filePrefix;
  private String appName;
  private String serverIp;

  private Timer throughputTimer;

  private Thread tailThread;
  private boolean run;

  private File lastModifiedFile;
  private int maxBackOff;

  private ChannelProcessor channelProcessor;

  private LinkedBlockingQueue<String> queue;

  @Override
  public void configure(Context context) {
    this.path = context.getString("path", "/tmp");
    filePrefix = context.getString("filePrefix", "");
    this.appName = context.getString("appName", "");
    this.maxBackOff = context.getInteger("maxBackOff", 4000);

    int queueSize = context.getInteger("queueSize", 10000);
    this.queue = new LinkedBlockingQueue<>(queueSize);
    this.serverIp = IpUtils.scanServerInnerIP();
  }

  @Override
  public void start() {

    logger.info("{} is starting..................", this.getClass().getSimpleName());

    channelProcessor = getChannelProcessor();

    Path watchPath = Paths.get(path);
    FileModifyWatcher watcher = FileModifyWatcher.newInstance();
    watcher.watch(watchPath, new WatchServiceFilter(filePrefix), listener -> {
      File newLastModifiedFile = listener.toFile();
      if (lastModifiedFile == null || !newLastModifiedFile.getPath()
          .equals(lastModifiedFile.getPath())) {
        lastModifiedFile = newLastModifiedFile;
        if (lastModifiedFile != null && lastModifiedFile.exists()) {
          logger.info("Thread {} detected new last modified file: {}",
                      Thread.currentThread().getName(), lastModifiedFile.getPath());
          restart();
        }
      }
    });

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
        logger.error("Cannot cancel timer. ", e);
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
    int backOffInterval = 250;
    try {
      channelProcessor = getChannelProcessor();
      String line = queue.take();
      if (!Strings.isNullOrEmpty(appName)) {
        line += (SPLITTER + appName);
      }
      String key = Joiner.on(SPLITTER).join(serverIp, appName);
      Event event = EventBuilder.withBody(line.getBytes());
      event.getHeaders().put("key", key);
      while (!Thread.interrupted()) {
        try {
          channelProcessor.processEvent(event);
        } catch (ChannelException ex) {
          logger.warn("The channel is full, and cannot write data now. The " +
                      "source will try again after " + String.valueOf(backOffInterval) +
                      " milliseconds");

          TimeUnit.MILLISECONDS.sleep(backOffInterval);
          backOffInterval = backOffInterval << 1;
          backOffInterval = backOffInterval >= maxBackOff ? maxBackOff :
                            backOffInterval;
          continue;
        }
        break;
      }
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
      int backOffInterval = 250;
      try {
        randomFile = new RandomAccessFile(lastModifiedFile, "r");
        randomFile.seek(randomFile.length());

        String line;
        while (run) {
          line = randomFile.readLine();
          if (line == null) {
            TimeUnit.MILLISECONDS.sleep(10);
            continue;
          }
          while (!Thread.interrupted()) {
            boolean offer = queue.offer(line);
            if (!offer) {
              logger.warn("The queue is full, and cannot write data now. Will try again after "
                          + String.valueOf(backOffInterval) + " milliseconds");

              TimeUnit.MILLISECONDS.sleep(backOffInterval);
              backOffInterval = backOffInterval << 1;
              backOffInterval = backOffInterval >= maxBackOff ? maxBackOff :
                                backOffInterval;
              continue;
            }
            backOffInterval = 250;
            break;
          }
        }
      } catch (Exception e) {
        Thread.currentThread().interrupt();
      } finally {
        if (randomFile != null) {
          try {
            randomFile.close();
          } catch (IOException e1) {
            logger.error("Cannot close RandomAccessFile {}", lastModifiedFile, e1);
          }
        }
      }
    }
  }
}