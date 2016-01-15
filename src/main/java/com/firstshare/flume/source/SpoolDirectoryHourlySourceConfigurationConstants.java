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

  // 日志按天还是按小时进行上报
  public static final String ROLL_UNIT = "rollUnit";
  public static final String DEFAULT_ROLL_UNIT = "hour";

  // 过整点多长时间将日志拷贝到spoolDir
  public static final String ROLL_MINUTES = "rollMinutes";
  public static final int DEFAULT_ROOL_MINUTES = 1;

  // 压缩方式, gz或zip
  public static final String FILE_COMPRESSIONMODE = "fileCompressionMode";
  public static final String DEFAULT_FILE_COMPRESSIONMODE = "gz";

  // 日志存放最长时间,单位"天"
  public static final String FILE_MAXHISTORY = "fileMaxHistory";
  public static final int DEFAULT_FILE_MAXHISTORY = 5;

  // 日志文件名的dateformat
  public static final String FILE_DATEFORMAT = "fileDateFormat";
  public static final String DEFAULT_FILE_DATEFORMAT = "yyyyMMdd-HH";

}
