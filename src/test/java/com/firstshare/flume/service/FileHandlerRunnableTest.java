package com.firstshare.flume.service;

import com.google.common.base.Joiner;
import com.google.common.io.Files;

import com.firstshare.flume.utils.FileUtil;
import com.firstshare.flume.utils.FlumeUtil;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by wzk on 15/12/21.
 */
@Ignore
public class FileHandlerRunnableTest {

  public String logDir;
  public String spoolDir;
  public String filePrefix;
  public String completedSuffix;
  public String fileCompresionMode;
  public String dateFormat;

  FileHandlerRunnable fileHandler;

  @Before
  public void before() {
    logDir = "/tmp";
    spoolDir = "/tmp/flume";
    filePrefix = "test";
    completedSuffix = ".tmp";
    fileCompresionMode = "gz";
    dateFormat = "yyyyMMddHH";

    fileHandler = new FileHandlerRunnable(logDir, spoolDir, filePrefix, completedSuffix,
                                          fileCompresionMode, 1, dateFormat);
  }

  @Test
  public void test() throws InterruptedException {
    String file = "/tmp/test.log";
    fileHandler.compressAsynchronously(file, file, "test.log", "zip");
    Thread.sleep(10000L);
    fileHandler.waitForAsynchronousJobToStop();
  }

  @Test
  public void testDeleteFiles() throws Exception {

    File tmpDir = Files.createTempDir();
    try {
      for (int i = 0; i < 5; i++) {
        for (int j = 10; j < 24; j++) {
          String filename =
              Joiner.on(".").join(filePrefix, FlumeUtil.getDayBefore(i) + j, fileCompresionMode);
          File tmp = tmpDir.toPath().resolve(filename).toFile();
          Files.write("test".getBytes(), tmp);
        }
      }
      fileHandler.deleteFiles(tmpDir.getPath(), filePrefix, fileCompresionMode, 1);

      List<String>
          files =
          FileUtil.getFiles(tmpDir.getPath(), filePrefix, fileCompresionMode, "201512", true);
      for (String file : files) {
        System.out.println(file);
      }

    } finally {
      FileUtil.delete(tmpDir);
    }
  }
}