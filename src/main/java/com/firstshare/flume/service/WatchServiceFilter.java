package com.firstshare.flume.service;

import com.firstshare.flume.api.IWatchServiceFilter;

import java.io.File;
import java.nio.file.Path;

/**
 * Created by wzk on 15/12/21.
 */
public class WatchServiceFilter implements IWatchServiceFilter {

  private String filePrefix;
  public WatchServiceFilter(String filePrefix) {
    this.filePrefix = filePrefix;
  }

  @Override
  public boolean filter(Path path) {
    if (path == null) {
      return true;
    }
    File file = path.toFile();
    if (file == null || ! file.isFile()) {
      return true;
    }
    if (file.getName().startsWith(filePrefix)) {
      return false;
    }
    return true;
  }
}


