package com.firstshare.flume.utils;

import org.apache.commons.lang.StringUtils;
import org.junit.Ignore;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;

import static org.junit.Assert.*;

/**
 * Created by wzk on 15/12/8.
 */
@Ignore
public class FlumeUtilTest {

  @Test
  public void testGetFileDate() throws Exception {
    String filename = "www.fxk-201510181205.log";
    String fileDate = FlumeUtil.getFileDate(filename);
    String year = StringUtils.substring(fileDate, 0, 4);
    String month = StringUtils.substring(fileDate, 4, 6);
    String day = StringUtils.substring(fileDate, 6, 8);
    String hour = StringUtils.substring(fileDate, 8);

    assertEquals("2015101812", fileDate);
    assertEquals("2015", year);
    assertEquals("10", month);
    assertEquals("18", day);
    assertEquals("12", hour);
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
}