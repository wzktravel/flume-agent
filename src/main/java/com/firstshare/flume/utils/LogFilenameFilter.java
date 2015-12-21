package com.firstshare.flume.utils;

import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.FilenameFilter;

/**
 * Created by wzk on 15/12/21.
 */
public class LogFilenameFilter implements FilenameFilter {

  private String prefix;
  private String suffix;
  private String middle;
  private boolean match; //匹配后缀还是不能匹配后缀

  public LogFilenameFilter(String prefix, String suffix, String middle, boolean match) {
    this.prefix = prefix;
    this.suffix = suffix;
    this.middle = middle;
    this.match = match;
  }

  @Override
  public boolean accept(File dir, String name) {
    if (StringUtils.isEmpty(name) || !StringUtils.startsWith(name, prefix)) {
      return false;
    }

    if (!match && StringUtils.endsWith(name, suffix)) {
      return false;
    } else if (match && !StringUtils.endsWith(name, suffix)) {
      return false;
    }

    if (StringUtils.contains(name, middle)) {
      return true;
    }
    return false;
  }
}