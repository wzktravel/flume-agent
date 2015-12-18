package com.firstshare.flume.api;

import java.nio.file.Path;

/**
 * Created by wzk on 15/12/17.
 */
public interface IWatchServiceFilter {

  boolean filter(Path path);

}
