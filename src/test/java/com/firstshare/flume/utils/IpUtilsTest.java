package com.firstshare.flume.utils;

import org.junit.Ignore;
import org.junit.Test;

/**
 * IpUtilsTest
 * Created by wangzk on 16/8/18.
 */
@Ignore
public class IpUtilsTest {

  @Test
  public void scanServerInnerIP() throws Exception {
    String ip = IpUtils.scanServerInnerIP();
    System.out.println(ip);
  }
}