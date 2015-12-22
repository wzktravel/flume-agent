package com.firstshare.flume.interceptor;

import com.google.common.base.Strings;

import com.firstshare.flume.utils.FlumeUtil;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * 对HDFS sink的interceptor, 在header中加入时间,文件名,ip.
 * Created by wangzk on 2015/12/08.
 */
public class HDFSInterceptor implements Interceptor {

  private static final Logger log = LoggerFactory.getLogger(HDFSInterceptor.class);

  private final boolean interceptorSwitch;
  private String hostAddress;

  public HDFSInterceptor(boolean interceptorSwitchInner) {
    this.interceptorSwitch = interceptorSwitchInner;
    hostAddress = FlumeUtil.getHostAddress();
  }

  @Override
  public void initialize() {
  }

  @Override
  public Event intercept(Event event) {
    if (!interceptorSwitch) {
      return event;
    }

    Map<String, String> headers = event.getHeaders();

    String fileName = FlumeUtil.getFileName(headers.get("file"));
    String fileDate = FlumeUtil.getFileDate(fileName);
    if (Strings.isNullOrEmpty(fileDate)) {
      fileDate = FlumeUtil.getFormatTimeForNow();
    }
    String year = StringUtils.substring(fileDate, 0, 4);
    String month = StringUtils.substring(fileDate, 4, 6);
    String day = StringUtils.substring(fileDate, 6, 8);
    String hour = StringUtils.substring(fileDate, 8);

    headers.put("filename", fileName);
    headers.put("host", hostAddress);
    headers.put("year", year);
    headers.put("month", month);
    headers.put("day", day);
    headers.put("hour", hour);

    event.setHeaders(headers);
    return event;
  }

  @Override
  public List<Event> intercept(List<Event> events) {
    List<Event> list = new LinkedList<Event>();
    for (Event event : events) {
      Event e = intercept(event);
      if (e != null) {
        list.add(e);
      }
    }
    return list;
  }

  @Override
  public void close() {

  }

  public static class Builder implements Interceptor.Builder {

    private boolean interceptorSwitchInner;

    @Override
    public Interceptor build() {
      return new HDFSInterceptor(interceptorSwitchInner);
    }

    @Override
    public void configure(Context context) {
      interceptorSwitchInner =
          context.getBoolean(Constants.HDFSINTERCEPTORSWITCH, Constants.HDFSINTERCEPTORSWITCH_DFLT);
    }
  }

  public static class Constants {
    public static String HDFSINTERCEPTORSWITCH = "hdfsinterceptor.switch";
    public static boolean HDFSINTERCEPTORSWITCH_DFLT = true;
  }

}