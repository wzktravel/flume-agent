package com.firstshare.flume.utils;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.List;

/**
 * copy from config-core#ConfigHelper
 * Created by wangzk on 16/8/18.
 */
public class IpUtils {

  /**
   * 获取本机内网ip，ip会在第一次访问后缓存起来，并且不会再更新
   * 所以那个模式可能不适合你的机器，本类只是方便大多数人的使用，如果你的
   * 机器不能用该模式获得ip，请使用NetworkInterfaceEx类自行获取
   *
   * @return 返回服务器内部IP
   */
  public static String scanServerInnerIP() {
    try {
      Enumeration<NetworkInterface> e = NetworkInterface.getNetworkInterfaces();
      while (e.hasMoreElements()) {
        NetworkInterface ni = e.nextElement();
        Enumeration<InetAddress> en = ni.getInetAddresses();
        while (en.hasMoreElements()) {
          String ip = en.nextElement().getHostAddress();
          if (isInnerIP(ip)) {
            if (!ip.equals("127.0.0.1") && !ip.equals("0:0:0:0:0:0:0:1")) {
              return ip;
            }
          }
        }
      }
    } catch (Exception ignored) {
    }
    return null;
  }

  /**
   * <pre>
   * 判断一个IP是不是内网IP段的IP
   * 10.0.0.0 – 10.255.255.255
   * 172.16.0.0 – 172.31.255.255
   * 192.168.0.0 – 192.168.255.255
   * </pre>
   *
   * @param ip ip地址
   * @return 如果是内网返回true，否则返回false
   */
  public static boolean isInnerIP(String ip) {
    if (ip == null || ip.length() < 7) {
      return false;
    }

    if (ip.equals("127.0.0.1") || ip.equals("0:0:0:0:0:0:0:1")) {
      return true;
    }

    if (ip.startsWith("10.") || ip.startsWith("192.168.")) {
      return true;
    }

    if (ip.startsWith("172.")) {
      List<String> items = Lists.newArrayList(Splitter.on('.').split(ip));
      if (items.size() == 4) {
        int i = Integer.parseInt(items.get(1));
        if (i > 15 && i < 32) {
          return true;
        }
      }
    }
    return false;
  }

}
