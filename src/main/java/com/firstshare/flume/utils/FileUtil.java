package com.firstshare.flume.utils;

import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.List;

/**
 * Created by wzk on 15/12/21.
 */
public class FileUtil {

  private static final Logger LOG = LoggerFactory.getLogger(FileUtil.class);

  public static URL fileToURL(File file) {
    try {
      return file.toURI().toURL();
    } catch (MalformedURLException e) {
      throw new RuntimeException("Unexpected exception on file [" + file + "]", e);
    }
  }

  /**
   * Creates the parent directories of a file. If parent directories not
   * specified in file's path, then nothing is done and this returns
   * gracefully.
   *
   * @param file file whose parent directories (if any) should be created
   * @return {@code true} if either no parents were specified, or if all
   * parent directories were created successfully; {@code false} otherwise
   */
  static public boolean createMissingParentDirectories(File file) {
    File parent = file.getParentFile();
    if (parent == null) {
      // Parent directory not specified, therefore it's a request to
      // create nothing. Done! ;)
      return true;
    }

    // File.mkdirs() creates the parent directories only if they don't
    // already exist; and it's okay if they do.
    parent.mkdirs();
    return parent.exists();
  }


  public String resourceAsString(ClassLoader classLoader, String resourceName) {
    URL url = classLoader.getResource(resourceName);
    if (url == null) {
      LOG.error("Failed to find resource [" + resourceName + "]");
      return null;
    }

    InputStreamReader isr = null;
    try {
      URLConnection urlConnection = url.openConnection();
      urlConnection.setUseCaches(false);
      isr = new InputStreamReader(urlConnection.getInputStream());
      char[] buf = new char[128];
      StringBuilder builder = new StringBuilder();
      int count = -1;
      while ((count = isr.read(buf, 0, buf.length)) != -1) {
        builder.append(buf, 0, count);
      }
      return builder.toString();
    } catch (IOException e) {
      LOG.error("Failed to open {}", resourceName, e);
    } finally {
      if (isr != null) {
        try {
          isr.close();
        } catch (IOException e) {
          // ignore
        }
      }
    }
    return null;
  }

  static final int BUF_SIZE = 32 * 1024;

  public void copy(String src, String destination) {
    BufferedInputStream bis = null;
    BufferedOutputStream bos = null;
    try {
      bis = new BufferedInputStream(new FileInputStream(src));
      bos = new BufferedOutputStream(new FileOutputStream(destination));
      byte[] inbuf = new byte[BUF_SIZE];
      int n;

      while ((n = bis.read(inbuf)) != -1) {
        bos.write(inbuf, 0, n);
      }

      bis.close();
      bis = null;
      bos.close();
      bos = null;
    } catch (IOException ioe) {
      String msg = "Failed to copy [" + src + "] to [" + destination + "]";
      LOG.error(msg, ioe);
    } finally {
      if (bis != null) {
        try {
          bis.close();
        } catch (IOException e) {
          // ignore
        }
      }
      if (bos != null) {
        try {
          bos.close();
        } catch (IOException e) {
          // ignore
        }
      }
    }
  }

  public static String afterLastSlash(String sregex) {
    int i = sregex.lastIndexOf('/');
    if (i == -1) {
      return sregex;
    } else {
      return sregex.substring(i + 1);
    }
  }


  public static void delete(File f) {
    if (f == null || !f.exists()) {
      return;
    }
    if (!f.delete()) {
      f.deleteOnExit();
    }
  }

  /**
   * 获取此目录下满足条件的文件
   * @param path
   * @return
   */
  public static List<String> getFiles(String path, String prefix, String suffix, String middle, boolean match) {
    File logPath = new File(path);
    String[] logs = new String[0];
    if (logPath.isDirectory()) {
      logs = logPath.list(new LogFilenameFilter(prefix, suffix, middle, match));
    }
    if (logs == null || logs.length < 1) {
      return null;
    }
    return Lists.asList(logs[0], logs);
  }
}
