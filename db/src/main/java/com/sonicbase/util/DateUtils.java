package com.sonicbase.util;

import org.apache.commons.lang.time.DateFormatUtils;

import java.text.ParseException;
import java.util.Date;
import java.util.TimeZone;

/**
 * User: lowryda
 * Date: 12/31/13
 * Time: 2:59 PM
 */
public final class DateUtils {


  public static String toString(final Date date) {
    return DateFormatUtils.format(date, "yyyy-MM-dd'T'HH:mm:ss.SSSZ", TimeZone.getTimeZone("UTC"));
  }

  public static String fromDate(final Date date) {
    return DateFormatUtils.format(date, "yyyy-MM-dd'T'HH:mm:ss.SSSZ", TimeZone.getTimeZone("UTC"));
  }

  public static Date fromString(final String iso8601string)
      throws ParseException {
    String[] patterns = new String[]{"yyyy-MM-dd'T'HH:mm:ss.SSSZ"};
    try {
      return org.apache.commons.lang.time.DateUtils.parseDate(iso8601string, patterns);
    }
    catch (Exception e) {
      patterns = new String[]{"yyyy-MM-dd'T'HH:mm:ss"};
      return org.apache.commons.lang.time.DateUtils.parseDate(iso8601string, patterns);
    }
  }
}
