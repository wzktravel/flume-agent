package com.firstshare.flume.interceptor;

import com.google.common.base.Strings;

import com.firstshare.flume.utils.FlumeUtil;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
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
  private String fileDateFormat;

  public HDFSInterceptor(boolean interceptorSwitch, String fileDateFormat) {
    this.interceptorSwitch = interceptorSwitch;
    hostAddress = FlumeUtil.getHostAddress();
    this.fileDateFormat = fileDateFormat;
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

    SimpleDateFormat dateFormat = new SimpleDateFormat(fileDateFormat);
    Calendar fileDateCalendar = Calendar.getInstance();
    try {
      Date date = dateFormat.parse(fileDate);
      fileDateCalendar.setTime(date);
    } catch (ParseException e) {
      log.error("cannot parse \"{}\" to DATE", fileDate);
    }
    String year = fileDateCalendar.get(Calendar.YEAR) + "";
    int monthIndex = fileDateCalendar.get(Calendar.MONTH) + 1;
    String month = monthIndex < 10 ? "0" + monthIndex : monthIndex + "";
    int dayIndex = fileDateCalendar.get(Calendar.DATE);
    String day = dayIndex < 10 ? "0" + dayIndex : dayIndex + "";
    int hourIndex = fileDateCalendar.get(Calendar.HOUR_OF_DAY);
    String hour = hourIndex < 10 ? "0" + hourIndex : hourIndex + "";

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
    private String fileDateFormat;
    private String timeUnit;

    @Override
    public Interceptor build() {
      return new HDFSInterceptor(interceptorSwitchInner, fileDateFormat);
    }

    @Override
    public void configure(Context context) {
      interceptorSwitchInner =
          context.getBoolean(Constants.HDFSINTERCEPTORSWITCH, Constants.HDFSINTERCEPTORSWITCH_DFLT);
      fileDateFormat = context.getString(Constants.FILEDATEFORMAT, Constants.FILEDATEFORMAT_DEFAULT);
    }
  }

  public static class Constants {
    public static String HDFSINTERCEPTORSWITCH = "switch";
    public static boolean HDFSINTERCEPTORSWITCH_DFLT = true;

    public static String FILEDATEFORMAT = "fileDateFormat";
    public static String FILEDATEFORMAT_DEFAULT = "yyyyMMddHH";
  }

}