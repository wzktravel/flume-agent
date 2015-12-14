package com.firstshare.flume.source;

import org.apache.flume.source.SpoolDirectorySourceConfigurationConstants;

/**
 * Created by wzk on 15/12/14.
 */
public class SpoolDirectoryHourlySourceConfigurationConstants
    extends SpoolDirectorySourceConfigurationConstants {

  public static final String LOG_DIRECTORY = "logDir";

  public static final String FILE_PREFIX = "filePrefix";
  public static final String DEFAULT_FILE_PREFIX = "";

  // 过整点多长时间将日志拷贝到spoolDir
  public static final String ROLL_MINUTES = "rollMinutes";
  public static final int DEFAULT_ROOL_MINUTES = 1;

}
