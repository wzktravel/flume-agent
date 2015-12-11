package com.firstshare.flume.utils;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by wzk on 15/12/8.
 */
public class FlumeUtil {

  private static final Logger log = LoggerFactory.getLogger(FlumeUtil.class);

  private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHH");
  private static final Pattern datePattern = Pattern.compile("\\d{4}\\d{2}\\d{2}\\d{2}");


  public static String getFileDate(String filename) {
    String date = "";
    Matcher m = datePattern.matcher(filename);
    if (m.find()) {
      date = m.group();
    }
    return date;
  }

  public static String getHostAddress() {
    String ip = "";
    try {
      InetAddress ia = InetAddress.getLocalHost();
      ip = ia.getHostAddress();
    } catch (UnknownHostException e) {
      log.error("cannot get local host: ", e);
    }
    return ip;
  }

  public static String getFormatTimeForNow() {
    return dateFormat.format(new Date());
  }

  public static String getFileName(String file) {
    return StringUtils.substringBeforeLast(file, ".log");
  }

}
