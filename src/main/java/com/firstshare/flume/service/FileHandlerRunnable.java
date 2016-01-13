package com.firstshare.flume.service;

import com.google.common.io.Files;

import com.firstshare.flume.utils.FileUtil;
import com.firstshare.flume.utils.FlumeUtil;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by wzk on 15/12/21.
 */
public class FileHandlerRunnable implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileHandlerRunnable.class);

  private Future<?> compressFuture;

  private String logDir;
  private String spoolDir;
  private String filePrefix;
  private String completedSuffix;
  private String fileCompresionMode;
  private int fileMaxHistory;
  private String dateFormat;

  public FileHandlerRunnable(String logDir, String spoolDir, String filePrefix, String completedSuffix,
                             String fileCompresionMode, int fileMaxHistory, String dateFormat) {
    this.logDir = logDir;
    this.spoolDir = spoolDir;
    this.filePrefix = filePrefix;
    this.completedSuffix = completedSuffix;
    this.fileCompresionMode = fileCompresionMode;
    this.fileMaxHistory = fileMaxHistory;
    this.dateFormat = dateFormat;
  }

  /**
   * 1. 得到需要处理的file,可能多个
   * 2. copy
   * 3. rename
   * 4. 异步压缩
   * 5. 删除之前文件
   */
  @Override
  public void run() {
    String lastHourWithDate = FlumeUtil.getLastHourWithDate(dateFormat);
    List<String> files = FileUtil.getFiles(logDir, filePrefix, completedSuffix, lastHourWithDate, false);
    if (files == null || files.size() < 1) {
      LOGGER.warn("No matched logs found in {}", logDir);
      return;
    }
    for (String file : files) {
      String src = logDir + "/" + file;
      String copyDst = spoolDir + "/" + file + completedSuffix;
      String moveDst = spoolDir + "/" + file;

      File srcFile = new File(src);
      File copyDstFile = new File(copyDst);
      File moveDstFile = new File(moveDst);

      // copy: 从logDir中拷贝file到spoolDir中,文件名要加上completedSuffix
      try {
        Files.copy(srcFile, copyDstFile);
        LOGGER.info("Copy file {} to {}.", src, copyDst);
      } catch (IOException e) {
        LOGGER.error("Connot copy file {} to {}.", src, copyDst);
      }

      // rename: 复制完成后,将file去掉completeSuffix进行重命名
      try {
        Files.move(copyDstFile, moveDstFile);
        LOGGER.info("Move file {} to {}.", copyDst, moveDst);
      } catch (IOException e) {
        LOGGER.error("Connot move file {} to {}.", copyDst, moveDst);
      }

      // 异步压缩
      if (needCompress()) {
        String innerEntryName = FileUtil.afterLastSlash(src);
        compressFuture = compressAsynchronously(src, src, innerEntryName, fileCompresionMode, 60);
      }
    }

    // 删除之前文件
    if (needDeletePastFile()) {
      deleteFiles(logDir, filePrefix, fileCompresionMode, fileMaxHistory);
    }

  }

  private void deleteFiles(String path, String prefix, String suffix, int maxHistory) {
    int dayBefore = maxHistory;
    while (true) {
      String filterDate = FlumeUtil.getDayBefore(dayBefore);
      List<String> files = FileUtil.getFiles(path, prefix, suffix, filterDate, true);
      // 一直上溯到没有日志的那天
      if (files == null || files.size() < 1) {
        break;
      }
      for (String file : files) {
        FileUtil.delete(new File(path + "/" + file));
      }
      LOGGER.info("delete files before {} days", dayBefore);
      dayBefore++;
    }
  }

  /**
   *
   * @param nameOfFile2Compress
   * @param nameOfCompressedFile
   * @param innerEntryName
   * @param mode
   * @return
   */
  private Future<?> compressAsynchronously(String nameOfFile2Compress, String nameOfCompressedFile,
                                           String innerEntryName, String mode) {
    CompressionMode compressionMode = this.determineCompressionMode(mode);
    Compressor compressor = new Compressor(compressionMode);
    ExecutorService executor = Executors.newScheduledThreadPool(1);
    Future<?> future = executor.submit(new CompressionRunnable(compressor, nameOfFile2Compress,
                                                           nameOfCompressedFile, innerEntryName));
    executor.shutdown();
    return future;
  }

  /**
   * 可以延迟一定时间后进行压缩
   * @param nameOfFile2Compress
   * @param nameOfCompressedFile
   * @param innerEntryName
   * @param delay
   * @return
   */
  private Future<?> compressAsynchronously(String nameOfFile2Compress, String nameOfCompressedFile,
                                          String innerEntryName, String mode, long delay) {
    CompressionMode compressionMode = this.determineCompressionMode(mode);
    Compressor compressor = new Compressor(compressionMode);

    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    Future<?> future = executor.schedule(new CompressionRunnable(compressor, nameOfFile2Compress,
                                   nameOfCompressedFile, innerEntryName), delay, TimeUnit.SECONDS);
    executor.shutdown();
    return future;
  }

  private CompressionMode determineCompressionMode(String fileCompressionMode) {
    CompressionMode compressionMode;
    if (StringUtils.equalsIgnoreCase(fileCompressionMode, "gz")) {
      compressionMode = CompressionMode.GZ;
    } else if (StringUtils.equalsIgnoreCase(fileCompressionMode, "zip")) {
      compressionMode = CompressionMode.ZIP;
    } else {
      compressionMode = CompressionMode.NONE;
    }
    return compressionMode;
  }

  private void waitForAsynchronousJobToStop() {
    if(compressFuture != null) {
      try {
        compressFuture.get(30, TimeUnit.SECONDS);
      } catch (TimeoutException e) {
        LOGGER.error("Timeout while waiting for compression job to finish", e);
      } catch (Exception e) {
        LOGGER.error("Unexpected exception while waiting for compression job to finish", e);
      }
    }
  }

  private boolean needCompress() {
    if (StringUtils.equals(fileCompresionMode, "zip") || StringUtils.equals(fileCompresionMode, "gz")) {
      return true;
    }
    return false;
  }

  private boolean needDeletePastFile() {
    if (fileMaxHistory > 0) {
      return true;
    }
    return false;
  }

}
