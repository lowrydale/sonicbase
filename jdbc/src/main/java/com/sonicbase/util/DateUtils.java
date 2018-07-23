package com.sonicbase.util;

import com.sonicbase.query.DatabaseException;
import org.apache.commons.lang.time.DateFormatUtils;

import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

/**
 * User: lowryda
 * Date: 12/31/13
 * Time: 2:59 PM
 */
public final class DateUtils {


  private static final String TIME_STR_1 = "yyyy-MM-dd'T'HH_mm_ssZ";
  private static final String TIME_STR_2 = "yyyy-MM-dd HH:mm:ss.SSSZ GG";
  private static final String TIME_STR_3 = "yyyy-MM-dd HH:mm:ss.SSSZ";

  private DateUtils() {
  }

  public static String toString(final Date date) {
    return DateFormatUtils.format(date, TIME_STR_1, TimeZone.getTimeZone("UTC"));
  }

  public static String fromDate(final Date date) {
    return DateFormatUtils.format(date, TIME_STR_1, TimeZone.getTimeZone("UTC"));
  }

  public static Date fromString(final String formattedString)
      throws ParseException {
    String[] patterns = new String[]{"yyyy-MM-dd'T'HH:mm:ssZ", "yyyy-MM-dd'T'HH:mm:ss",
        "yyyy-MM-dd'T'HH:mm:ss.SSS", TIME_STR_1, "yyyy-MM-dd'T'HH_mm_ss"};
    return org.apache.commons.lang.time.DateUtils.parseDate(formattedString, patterns);
  }

  public static String toDbString(Calendar cal) {
    Calendar cal2 = Calendar.getInstance();
    cal2.set(-1, 11, 31, 17, 0, 1);

    SimpleDateFormat format1 = null;
    if (cal2.compareTo(cal) > 0) {
      format1 = new SimpleDateFormat(TIME_STR_2);
    }
    else {
      format1 = new SimpleDateFormat(TIME_STR_3);
    }
    TimeZone tz = TimeZone.getTimeZone("UTC");
    format1.setTimeZone(tz);
    return format1.format(cal.getTimeInMillis());
  }

  public static String toDbTimeString(Time time) {
    SimpleDateFormat format1 = new SimpleDateFormat("HH:mm:ss.SSSZ");
    TimeZone tz = TimeZone.getTimeZone("UTC");
    format1.setTimeZone(tz);
    return format1.format(time.getTime());
  }

  public static String toDbTimestampString(Timestamp timestamp) {
    Calendar cal = new GregorianCalendar();
    cal.set(-1, 11, 31, 17, 0, 1);
    Timestamp timestamp2 = new Timestamp(cal.getTimeInMillis());

    SimpleDateFormat format1 = null;
    if (timestamp2.compareTo(timestamp) > 0) {
      format1 = new SimpleDateFormat(TIME_STR_2);
    }
    else {
      format1 = new SimpleDateFormat(TIME_STR_3);
    }
    TimeZone tz = TimeZone.getTimeZone("UTC");
    format1.setTimeZone(tz);
    return format1.format(timestamp.getTime());
  }

  static String[] formatStrings = new String[]{
      TIME_STR_3,
      TIME_STR_2,
      "yyyy-MM-dd HH:mm:ss.SSS",
      "yyyy-MM-dd HH:mm:ss.SSS GG",
      "yyyy-MM-dd HH:mm:ssZ",
      "yyyy-MM-dd HH:mm:ssZ GG",
      "yyyy-MM-dd HH:mm:ss",
      "yyyy-MM-dd HH:mm:ss GG",
      "yyyy-MM-dd HH:mmZ",
      "yyyy-MM-dd HH:mmZ GG",
      "yyyy-MM-dd HH:mm",
      "yyyy-MM-dd HH:mm GG",
      "yyyy-MM-dd",
      "yyyy-MM-dd GG"
  };

  public static Calendar fromDbCalString(String formattedString) {
    Calendar cal = Calendar.getInstance();
    Exception lastException = null;
    for (String format : formatStrings) {
      try {
        DateFormat df1 = new SimpleDateFormat(format);
        cal.setTime(df1.parse(formattedString));
        return cal;
      }
      catch (Exception e) {
        lastException = e;
      }
    }
    throw new DatabaseException("Error parsing date", lastException);
  }

  static String[] formatTimeStrings = new String[]{
      "HH:mm:ss.SSSZ",
      "HH:mm:ss.SSS",
      "HH:mm:ssZ",
      "HH:mm:ss"
  };

  public static Calendar fromDbTimeString(String formattedString) {
    Calendar cal = Calendar.getInstance();
    Exception lastException = null;
    for (String format : formatTimeStrings) {
      try {
        DateFormat df1 = new SimpleDateFormat(format);
        cal.setTime(df1.parse(formattedString));
        return cal;
      }
      catch (Exception e) {
        lastException = e;
      }
    }
    throw new DatabaseException("Error parsing date", lastException);
  }

}
