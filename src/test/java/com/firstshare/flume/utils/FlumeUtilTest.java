package com.firstshare.flume.utils;

import org.junit.Ignore;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import static org.junit.Assert.*;

/**
 * Created by wzk on 15/12/8.
 */
@Ignore
public class FlumeUtilTest {

  @Test
  public void testGetFileDate() throws Exception {
    String filename = "www.fxk-2015-02-01-08.log";
    String fileDate = FlumeUtil.getFileDate(filename);
//    String year = StringUtils.substring(fileDate, 0, 4);
//    String month = StringUtils.substring(fileDate, 4, 6);
//    String day = StringUtils.substring(fileDate, 6, 8);
//    String hour = StringUtils.substring(fileDate, 8);
    System.out.println(fileDate);

    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH");

    Calendar fileDateCalendar = Calendar.getInstance();
    try {
      Date date = dateFormat.parse(fileDate);
      fileDateCalendar.setTime(date);
    } catch (ParseException e) {
//      log.error("cannot parse \"{}\" to DATE", fileDate);
    }
    String year = fileDateCalendar.get(Calendar.YEAR) + "";
    int monthIndex = fileDateCalendar.get(Calendar.MONTH) + 1;
    String month = monthIndex < 10 ? "0" + monthIndex : monthIndex + "";
    int dayIndex = fileDateCalendar.get(Calendar.DATE);
    String day = dayIndex < 10 ? "0" + dayIndex : dayIndex + "";
    int hourIndex = fileDateCalendar.get(Calendar.HOUR_OF_DAY);
    String hour = hourIndex < 10 ? "0" + hourIndex : hourIndex + "";

    System.out.println(year);
    System.out.println(month);
    System.out.println(day);
    System.out.println(hour);

//    assertEquals("2015101812", fileDate);
//    assertEquals("2015", year);
//    assertEquals("10", month);
//    assertEquals("18", day);
//    assertEquals("12", hour);
  }

  @Test
  public void testGetHostAddress() throws Exception {
    String ip = FlumeUtil.getHostAddress();
    System.out.println(ip);
  }

  @Test
  public void testGetFormatTimeForNow() throws Exception {

  }

  @Test
  public void testGetFileName() throws Exception {
    String file = "fs-app-center_web_2015120716.log";
    String fileName = FlumeUtil.getFileName(file);
    assertEquals(fileName, "fs-app-center_web_2015120716");
  }

  @Test
  public void testGetMilliSecondsToNextHour() throws Exception {
    long milliseconds = FlumeUtil.getMilliSecondsToNextHour();
    System.out.println(milliseconds);
  }

  @Test
  public void testCopyAndRename() throws Exception {

  }

  @Test
  public void testGetLastHour() throws Exception {
    String lastHour = FlumeUtil.getLastHourWithDate();
    System.out.println(lastHour);
  }

  @Test
  public void testGetDayBefore() throws Exception {
    String day = FlumeUtil.getDayBefore(7);
    System.out.println(day);
  }

  @Test
  public void testGetMilliSecondsToNextDay() throws Exception {
    long milliseconds = FlumeUtil.getMilliSecondsToNextDay();
    System.out.println(milliseconds);
    long minutes = milliseconds / 1000 / 60;
    System.out.println(minutes);
    System.out.println((double) minutes / 60);
  }

  @Test
  public void testGetLastHourWithDate() throws Exception {
    String aFormat = "yyyy-MM-dd-HH";
    String d = FlumeUtil.getLastHourWithDate(aFormat);
    System.out.println(d);
  }

}