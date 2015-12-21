package com.firstshare.flume.watcher;

import com.google.common.io.Files;

import com.firstshare.flume.api.IFileListener;
import com.firstshare.flume.api.IWatchServiceFilter;
import com.firstshare.flume.service.WatchServiceFilter;

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

/**
 * Created by wzk on 15/12/17.
 */
@Ignore
public class FileModifyWatcherTest {

  private static final Logger LOG = LoggerFactory.getLogger(FileModifyWatcher.class);

  String filePrefix = "a";

  @Test
  public void testWatch() throws Exception {

    FileModifyWatcher watcher = FileModifyWatcher.newInstance();
    File tmpDir = Files.createTempDir();
    System.out.println(tmpDir);
    File aTxt = tmpDir.toPath().resolve("a.txt").toFile();

    try {
      watcher.watch(tmpDir.toPath(), new WatchServiceFilter(filePrefix), new IFileListener() {
        @Override
        public void changed(Path path) {
          System.out.println(path);
        }
      });
      //修改文件内容

      write("test".getBytes(), aTxt);
      File bTxt = tmpDir.toPath().resolve("b.txt").toFile();
      write("test".getBytes(), bTxt);

      write("test1".getBytes(), aTxt);
      write("test2".getBytes(), aTxt);
      write("test".getBytes(), bTxt);

      File aaTxt = tmpDir.toPath().resolve("aa.txt").toFile();
      write("test".getBytes(), aaTxt);

      Thread.sleep(10000L);

      delete(aaTxt);
      delete(bTxt);
    } finally {
      delete(aTxt);
      delete(tmpDir);
    }
  }

  private void write(byte[] bytes, File f) throws IOException {
    LOG.info("write {} bytes into {}", bytes.length, f);
    Files.write(bytes, f);
  }

  private void delete(File f) {
    if (f == null || !f.exists())
      return;
    LOG.info("delete {}", f);
    if (!f.delete()) {
      f.deleteOnExit();
    }
  }

}

