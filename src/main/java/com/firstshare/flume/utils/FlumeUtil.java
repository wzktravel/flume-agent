package com.firstshare.flume.utils;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by wzk on 15/12/8.
 */
public class FlumeUtil {

  private static final Logger log = LoggerFactory.getLogger(FlumeUtil.class);

  private static final SimpleDateFormat dateWithHourFormat = new SimpleDateFormat("yyyyMMddHH");
  private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
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
    return dateWithHourFormat.format(new Date());
  }

  public static String getFileName(String file) {
    return StringUtils.substringBeforeLast(file, ".log");
  }

  public static long getMilliSecondsToNextHour() {
    Calendar c = Calendar.getInstance();
    c.add(Calendar.HOUR, 1);
    c.set(Calendar.MINUTE, 0);
    c.set(Calendar.SECOND, 0);
    return c.getTimeInMillis() - System.currentTimeMillis();
  }

  public static long getMilliSecondsToNextDay() {
    Calendar c = Calendar.getInstance();
    c.add(Calendar.DATE, 1);
    c.set(Calendar.HOUR_OF_DAY, 0);
    c.set(Calendar.MINUTE, 0);
    c.set(Calendar.SECOND, 0);
    return c.getTimeInMillis() - System.currentTimeMillis();
  }

  public static String getLastHourWithDate() {
    return getLastHourWithDate(dateWithHourFormat);
  }

  public static String getLastHourWithDate(String dateFormat) {
    return getLastHourWithDate(new SimpleDateFormat(dateFormat));
  }

  public static String getLastHourWithDate(SimpleDateFormat format) {
    return getFormatTime(format, "hour", -1);
  }

  public static String getLastDayWithDate(String format) {
    return getLastDayWithDate(new SimpleDateFormat(format));
  }

  public static String getLastDayWithDate(SimpleDateFormat format) {
    return getFormatTime(format, "day", -1);
  }

  public static String getFormatTime(SimpleDateFormat format, String unit, int amount) {
    Calendar c = Calendar.getInstance();
    if (StringUtils.equals(unit, "day")) {
      c.add(Calendar.DATE, amount);
    } else if (StringUtils.equals(unit, "hour")) {
      c.add(Calendar.DATE, amount);
    }
    return format.format(c.getTime());
  }


  public static String getDayBefore(int day) {
    Calendar c = Calendar.getInstance();
    c.add(Calendar.DATE, 0 - day);
    return dateFormat.format(c.getTime());
  }

}
