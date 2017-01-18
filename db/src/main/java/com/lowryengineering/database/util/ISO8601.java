package com.lowryengineering.database.util;

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
public final class ISO8601 {


//    public static String to8601String(final Date date) {
//      SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
//      df.setTimeZone(TimeZone.getTimeZone("UTC"));
//      String formatted = df.format(date);
//      //formatted = formatted.replace("+0000", "000Z")
//      formatted = formatted.substring(0, 20) + "000Z";
//      return formatted; //return formatted.substring(0, 22) + ":" + formatted.substring(22);
//    }
//
//    /** Transform Calendar to ISO 8601 string. */
//    public static String fromCalendar(final Calendar calendar) {
//        Date date = calendar.getTime();
//        String formatted = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ")
//            .format(date);
//        return formatted.substring(0, 22) + ":" + formatted.substring(22);
//    }

//  public static String fromDate(final Date date) {
//      String formatted = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ")
//          .format(date);
//      return formatted.substring(0, 19) + ".000Z";
//  }

    /** Get current date and time formatted as ISO 8601 string. */
 //   public static String now() {
//        return fromCalendar(GregorianCalendar.getInstance());
//    }

    /** Transform ISO 8601 string to Calendar. */
//    public static Calendar toCalendar(final String iso8601string)
//            throws ParseException {
//        Calendar calendar = GregorianCalendar.getInstance();
//        String s = iso8601string.replace("Z", "+00:00");
//        try {
//            s = s.substring(0, 22) + s.substring(23);
//        } catch (IndexOutOfBoundsException e) {
//            throw new ParseException("Invalid length", 0);
//        }
//        Date date = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").parse(s);
//        calendar.setTime(date);
//        return calendar;
//    }

  public static Calendar from8601String(final String iso8601string)
          throws ParseException {
      Calendar calendar = GregorianCalendar.getInstance();
      String s = iso8601string.replace("0+00:00", "00");
      s = s.replace("Z", "");
//      try {
//          s = s.substring(0, 22) + s.substring(23);
//      } catch (IndexOutOfBoundsException e) {
//          throw new ParseException("Invalid length", 0);
//      }
      SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
      df.setTimeZone(TimeZone.getTimeZone("UTC"));
      Date date = df.parse(s);
      calendar.setTime(date);
      return calendar;
  }
}
