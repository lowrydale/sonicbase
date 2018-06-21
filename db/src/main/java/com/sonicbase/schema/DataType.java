package com.sonicbase.schema;

import com.sonicbase.common.ExcludeRename;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.util.DateUtils;
import org.apache.commons.io.IOUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

@ExcludeRename
public class DataType {


  public static interface Converter {
    public Object convert(Object value);
  }


  public static Converter getLongConverter() {
    return longConverter;
  }


  private static class LongConverter implements Converter {

    @Override
    public Object convert(Object value) {
      if (value == null) {
        return null;
      }
      if (value instanceof Long) {
        return value;
      }

      long ret = 0;
      if (value instanceof String) {
        ret = (long) Long.valueOf((String) value);
      }
      else if (value instanceof byte[]) {
        try {
          ret = (long) Long.valueOf(new String((byte[]) value, "utf-8"));
        }
        catch (UnsupportedEncodingException e) {
          throw new DatabaseException(e);
        }
      }
      else if (value instanceof Float) {
        ret = (long) (float) (Float) value;
      }
      else if (value instanceof Double) {
        ret = (long) (double) (Double) value;
      }
      else if (value instanceof Integer) {
        ret = (long) (int) (Integer) value;
      }
      else if (value instanceof Short) {
        ret = (long) (Short) value;
      }
      else if (value instanceof Byte) {
        ret = (long) (Byte) value;
      }
      else {
        throw new DatabaseException("Incompatible datatypes: lhs=long, rhs=" + value.getClass().getName());
      }
      return ret;
    }
  }

  private static Converter longConverter = new LongConverter();

  public static Converter getStringConverter() {
    return stringConverter;
  }

  private static Converter stringConverter = new Converter() {
    @Override
    public Object convert(Object value) {
      if (value == null) {
        return null;
      }
      if (value instanceof String) {
        return value;
      }
      if (value instanceof byte[]) {
        try {
          return new String((byte[])value, "utf-8");
        }
        catch (UnsupportedEncodingException e) {
          throw new DatabaseException(e);
        }
      }
      if (value instanceof Date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime((Date)value);
        return DateUtils.toDbString(cal);
      }
      if (value instanceof Time) {
        return DateUtils.toDbTimeString((Time)value);
      }
      if (value instanceof Timestamp) {
        return DateUtils.toDbTimestampString((Timestamp)value);
      }
      return String.valueOf(value);
    }
  };

  public static Converter getUtf8Converter() {
    return utf8Converter;
  }

  private static Converter utf8Converter = new Converter() {
    @Override
    public Object convert(Object value) {
      if (value == null) {
        return null;
      }
      if (value instanceof byte[]) {
        return value;
      }
      String ret = (String)stringConverter.convert(value);
      try {
        return ret.getBytes("utf-8");
      }
      catch (UnsupportedEncodingException e) {
        throw new DatabaseException(e);
      }
    }
  };

  public static Converter getByteArrayConverter() {
    return byteArrayConverter;
  }

  private static Converter byteArrayConverter = new Converter() {
    @Override
    public Object convert(Object value) {
      if (value == null) {
        return null;
      }
      if (value instanceof byte[]) {
        return value;
      }
      return null;
    }
  };

  public static Converter getBlobConverter() {
    return blobConverter;
  }

  private static class BlobConverter implements Converter {

    @Override
    public Object convert(Object value) {
      if (value == null) {
        return null;
      }
      if (value instanceof byte[]) {
        return value;
      }
      else if (value instanceof InputStream) {
        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        try {
          IOUtils.copy((InputStream) value, bytesOut);
          bytesOut.close();
        }
        catch (IOException e) {
          throw new DatabaseException(e);
        }
        return bytesOut.toByteArray();
      }
      return null;

    }
  }

  private static Converter blobConverter = new BlobConverter();

  public static Converter getDoubleConverter() {
    return doubleConverter;
  }

  private static class DoubleConverter implements Converter {

    @Override
    public Object convert(Object value) {
      if (value == null) {
        return null;
      }
      if (value instanceof Double) {
        return value;
      }

      double ret = 0;
      if (value instanceof String) {
        ret = Double.valueOf((String) value);
      }
      else if (value instanceof byte[]) {
        try {
          ret = Double.valueOf(new String((byte[]) value, "utf-8"));
        }
        catch (UnsupportedEncodingException e) {
          throw new DatabaseException(e);
        }
      }
      else if (value instanceof Float) {
        ret = (double) (float) (Float) value;
      }
      else if (value instanceof Integer) {
        ret = (double) (int) (Integer) value;
      }
      else if (value instanceof Long) {
        ret = (double) (long) (Long) value;
      }
      else if (value instanceof Short) {
        ret = (double) (short) (Short) value;
      }
      else if (value instanceof Byte) {
        ret = (double) (byte) (Byte) value;
      }
      else {
        throw new DatabaseException("Incompatible datatypes: lhs=String, rhs=" + value.getClass().getName());
      }
      return ret;

      }
  }

  private static Converter doubleConverter = new DoubleConverter();


  public static Converter getIntConverter() {
    return intConverter;
  }

  private static Converter intConverter = new Converter() {
    @Override
    public Object convert(Object value) {
      if (value == null) {
        return null;
      }
      if (value instanceof Integer) {
        return value;
      }
      long ret = (Long) longConverter.convert(value);
      return (int) ret;
    }
  };

  public static Converter getShortConverter() {
    return shortConverter;
  }

  private static Converter shortConverter = new Converter() {
    @Override
    public Object convert(Object value) {
      if (value == null) {
        return null;
      }
      if (value instanceof Short) {
        return value;
      }
      long ret = (Long) longConverter.convert(value);
      return (short) ret;
    }
  };

  public static Converter getBooleanConverter() {
    return booleanConverter;
  }

  private static Converter booleanConverter = new Converter() {
    @Override
    public Object convert(Object value) {
      if (value == null) {
        return null;
      }
      if (value instanceof Boolean) {
        return value;
      }
      if (value instanceof byte[]) {
        return new String((byte[])value).equalsIgnoreCase("true");
      }
      long ret = (Long) longConverter.convert(value);
      return ret == 1;
    }
  };

  public static Converter getByteConverter() {
    return byteConverter;
  }

  private static Converter byteConverter = new Converter() {
    @Override
    public Object convert(Object value) {
      if (value == null) {
        return null;
      }
      if (value instanceof Byte) {
        return value;
      }
      long ret = (Long) longConverter.convert(value);
      return (byte) ret;
    }
  };

  public static Converter getFloatConverter() {
    return floatConverter;
  }

  private static Converter floatConverter = new Converter() {
    @Override
    public Object convert(Object value) {
      if (value == null) {
        return null;
      }
      if (value instanceof Float) {
        return value;
      }
      double ret = (Double) doubleConverter.convert(value);
      return (float) ret;
    }
  };

  public static Converter getBigDecimalConverter() {
    return bigDecimalConverter;
  }

  private static class BigDecimalConverter implements Converter {

    @Override
    public Object convert(Object value) {
      BigDecimal ret = null;
      if (value == null) {
        return null;
      }
      if (value instanceof BigDecimal) {
        return value;
      }
      else if (value instanceof byte[]) {
        try {
          ret = BigDecimal.ONE.valueOf(Double.valueOf(new String((byte[]) value, "utf-8")));
        }
        catch (UnsupportedEncodingException e) {
          throw new DatabaseException(e);
        }
      }
      else if (value instanceof Float) {
        ret = new BigDecimal((float) (Float) value);
      }
      else if (value instanceof Double) {
        ret = new BigDecimal((double) (Double) value);
      }
      else if (value instanceof Integer) {
        ret = new BigDecimal((int) (Integer) value);
      }
      else if (value instanceof Long) {
        ret = new BigDecimal((long) (Long) value);
      }
      else if (value instanceof Short) {
        ret = new BigDecimal((short) (Short) value);
      }
      else if (value instanceof Byte) {
        ret = new BigDecimal((short) (byte) (Byte) value);
      }
      else {
        throw new DatabaseException("Incompatible datatypes: lhs=BigDecimal, rhs=" + value.getClass().getName());
      }
      return ret;
    }
  }

  private static Converter bigDecimalConverter = new BigDecimalConverter();

  public static Converter getDateConverter() {
    return dateConverter;
  }

  private static class DateConverter implements Converter {

    @Override
    public Object convert(Object value) {
      if (value == null) {
        return null;
      }
      if (value instanceof Date) {
        return value;
      }
      Date ret = null;
      if (value instanceof String) {
        Calendar cal = DateUtils.fromDbCalString((String)value);
        ret = new Date(cal.getTimeInMillis());
      }
      else if (value instanceof byte[]) {
        try {
          String str = new String((byte[]) value, "utf-8");
          Calendar cal = DateUtils.fromDbCalString(str);
          ret = new Date(cal.getTimeInMillis());
        }
        catch (UnsupportedEncodingException e) {
          throw new DatabaseException(e);
        }
      }
      else if (value instanceof Time) {
        ret = new Date(((Time) value).getTime());
      }
      else if (value instanceof Timestamp) {
        ret = new Date(((Timestamp) value).getTime());
      }
      else {
        throw new DatabaseException("Incompatible datatypes: lhs=Date, rhs=" + value.getClass().getName());
      }
      return ret;
    }
  }

  private static Converter dateConverter = new DateConverter();

  public static Converter getTimeConverter() {
    return timeConverter;
  }

  private static class TimeConverter implements Converter {

    @Override
    public Object convert(Object value) {
      if (value == null) {
        return null;
      }
      if (value instanceof Time) {
        return value;
      }
      Time ret = null;
      if (value instanceof String) {
        try {
          Calendar cal = DateUtils.fromDbTimeString((String)value);
          ret = new Time(cal.getTimeInMillis());
        }
        catch (Exception e) {
          throw new DatabaseException(e);
        }
      }
      else if (value instanceof byte[]) {
        try {
          String str = new String((byte[]) value, "utf-8");
          Calendar cal = DateUtils.fromDbTimeString(str);
          ret = new Time(cal.getTimeInMillis());
        }
        catch (Exception e) {
          throw new DatabaseException(e);
        }
      }
      else if (value instanceof Date) {
        ret = new Time(((Date) value).getTime());
      }
      else if (value instanceof Timestamp) {
        ret = new Time(((Timestamp) value).getTime());
      }
      else {
        throw new DatabaseException("Incompatible datatypes: lhs=Time, rhs=" + value.getClass().getName());
      }
      return ret;

    }
  }

  private static Converter timeConverter = new TimeConverter();

  public static Converter getTimestampConverter() {
    return timestampConverter;
  }

  private static class TimestampConverter implements Converter {

    @Override
    public Object convert(Object value) {
      if (value == null) {
        return null;
      }
      if (value instanceof Timestamp) {
        return value;
      }
      Timestamp ret = null;
      if (value instanceof String) {
        try {
          Calendar cal = DateUtils.fromDbCalString((String)value);
          ret = new Timestamp(cal.getTimeInMillis());
        }
        catch (Exception e) {
          throw new DatabaseException(e);
        }
      }
      else if (value instanceof byte[]) {
        try {
          String str = new String((byte[]) value, "utf-8");
          Calendar cal = DateUtils.fromDbCalString(str);
          ret = new Timestamp(cal.getTimeInMillis());
        }
        catch (Exception e) {
          throw new DatabaseException(e);
        }
      }
      else if (value instanceof Long) {
        ret = new Timestamp((Long)value);
      }
      else if (value instanceof Date) {
        ret = new Timestamp(((Date) value).getTime());
      }
      else if (value instanceof Time) {
        ret = new Timestamp(((Time) value).getTime());
      }
      else {
        throw new DatabaseException("Incompatible datatypes: lhs=Timestamp, rhs=" + value.getClass().getName());
      }
      return ret;
    }
  }

  private static Converter timestampConverter = new TimestampConverter();

  public static Comparator getBooleanComparator() {
    return booleanComparator;
  }

  private static Comparator booleanComparator = new Comparator() {
    @Override
    public int compare(Object o1, Object o2) {
      if (o1 == null && o2 == null) {
        return 0;
      }
      if (o1 == null) {
        return -1;
      }
      if (o2 == null) {
        return 1;
      }
      if (!(o1 instanceof Boolean)) {
        o1 = booleanConverter.convert(o1);
      }
      if (!(o2 instanceof Boolean)) {
        o2 = booleanConverter.convert(o2);
      }
      if (!(o1 instanceof Boolean) || !(o2 instanceof Boolean)) {
        throw new DatabaseException("Incompatible datatypes: lhs=" + o1.getClass().getName() + ", rhs=" + o2.getClass());
      }
      return ((Boolean) o1).compareTo((Boolean) o2);
    }
  };

  public static Comparator getLongComparator() {
    return longComparator;
  }

  private static class LongComparator implements Comparator {

    @Override
    public int compare(Object o1, Object o2) {
      if (o1 instanceof Long && o2 instanceof Long) {
        long l1 = (Long) o1;
        long l2 = (Long) o2;
        return l1 < l2 ? -1 : l1 > l2 ? 1 : 0;
      }
      Integer ret = compareNumerics(o1, o2);
      if (ret == null) {
        long lhs = (long) longConverter.convert(o1);
        long rhs = (long) longConverter.convert(o2);
        return Long.compare(lhs, rhs);
      }
      return ret;
    }
  }

  private static Integer compareNumerics(Object o1, Object o2) {
    if (o1 == null || o2 == null) {
      return 0;
    }
    if (o1 instanceof Double) {
      Double lhs = (Double) o1;
      if (o2 instanceof Double) {
        return lhs < (Double) o2 ? -1 : lhs > (Double) o2 ? 1 : 0;
      }
      if (o2 instanceof Float) {
        return lhs < (Float) o2 ? -1 : lhs > (Float) o2 ? 1 : 0;
      }
      if (o2 instanceof Short) {
        return lhs < (Short) o2 ? -1 : lhs > (Short) o2 ? 1 : 0;
      }
      if (o2 instanceof Byte) {
        return lhs < (Byte) o2 ? -1 : lhs > (Byte) o2 ? 1 : 0;
      }
      if (o2 instanceof Integer) {
        return lhs < (Integer) o2 ? -1 : lhs > (Integer) o2 ? 1 : 0;
      }
      if (o2 instanceof Long) {
        return lhs < (Long) o2 ? -1 : lhs > (Long) o2 ? 1 : 0;
      }
    }
    if (o1 instanceof Float) {
      Float lhs = (Float) o1;
      if (o2 instanceof Double) {
        return lhs < (Double) o2 ? -1 : lhs > (Double) o2 ? 1 : 0;
      }
      if (o2 instanceof Float) {
        return lhs < (Float) o2 ? -1 : lhs > (Float) o2 ? 1 : 0;
      }
      if (o2 instanceof Short) {
        return lhs < (Short) o2 ? -1 : lhs > (Short) o2 ? 1 : 0;
      }
      if (o2 instanceof Byte) {
        return lhs < (Byte) o2 ? -1 : lhs > (Byte) o2 ? 1 : 0;
      }
      if (o2 instanceof Long) {
        return lhs < (Long) o2 ? -1 : lhs > (Long) o2 ? 1 : 0;
      }
      if (o2 instanceof Integer) {
        return lhs < (Integer) o2 ? -1 : lhs > (Integer) o2 ? 1 : 0;
      }
    }
    if (o1 instanceof Short) {
      Short lhs = (Short) o1;
      if (o2 instanceof Double) {
        return lhs < (Double) o2 ? -1 : lhs > (Double) o2 ? 1 : 0;
      }
      if (o2 instanceof Float) {
        return lhs < (Float) o2 ? -1 : lhs > (Float) o2 ? 1 : 0;
      }
      if (o2 instanceof Short) {
        return lhs < (Short) o2 ? -1 : lhs > (Short) o2 ? 1 : 0;
      }
      if (o2 instanceof Byte) {
        return lhs < (Byte) o2 ? -1 : lhs > (Byte) o2 ? 1 : 0;
      }
      if (o2 instanceof Long) {
        return lhs < (Long) o2 ? -1 : lhs > (Long) o2 ? 1 : 0;
      }
      if (o2 instanceof Integer) {
        return lhs < (Integer) o2 ? -1 : lhs > (Integer) o2 ? 1 : 0;
      }
    }
    if (o1 instanceof Byte) {
      Byte lhs = (Byte) o1;
      if (o2 instanceof Double) {
        return lhs < (Double) o2 ? -1 : lhs > (Double) o2 ? 1 : 0;
      }
      if (o2 instanceof Float) {
        return lhs < (Float) o2 ? -1 : lhs > (Float) o2 ? 1 : 0;
      }
      if (o2 instanceof Short) {
        return lhs < (Short) o2 ? -1 : lhs > (Short) o2 ? 1 : 0;
      }
      if (o2 instanceof Byte) {
        return lhs < (Byte) o2 ? -1 : lhs > (Byte) o2 ? 1 : 0;
      }
      if (o2 instanceof Long) {
        return lhs < (Long) o2 ? -1 : lhs > (Long) o2 ? 1 : 0;
      }
      if (o2 instanceof Integer) {
        return lhs < (Integer) o2 ? -1 : lhs > (Integer) o2 ? 1 : 0;
      }
    }
    if (o1 instanceof Integer) {
      Integer lhs = (Integer) o1;
      if (o2 instanceof Double) {
        return lhs < (Double) o2 ? -1 : lhs > (Double) o2 ? 1 : 0;
      }
      if (o2 instanceof Float) {
        return lhs < (Float) o2 ? -1 : lhs > (Float) o2 ? 1 : 0;
      }
      if (o2 instanceof Short) {
        return lhs < (Short) o2 ? -1 : lhs > (Short) o2 ? 1 : 0;
      }
      if (o2 instanceof Byte) {
        return lhs < (Byte) o2 ? -1 : lhs > (Byte) o2 ? 1 : 0;
      }
      if (o2 instanceof Long) {
        return lhs < (Long) o2 ? -1 : lhs > (Long) o2 ? 1 : 0;
      }
      if (o2 instanceof Integer) {
        return lhs < (Integer) o2 ? -1 : lhs > (Integer) o2 ? 1 : 0;
      }
    }
    if (o1 instanceof Long) {
      Long lhs = (Long) o1;
      if (o2 instanceof Double) {
        return lhs < (Double) o2 ? -1 : lhs > (Double) o2 ? 1 : 0;
      }
      if (o2 instanceof Float) {
        return lhs < (Float) o2 ? -1 : lhs > (Float) o2 ? 1 : 0;
      }
      if (o2 instanceof Short) {
        return lhs < (Short) o2 ? -1 : lhs > (Short) o2 ? 1 : 0;
      }
      if (o2 instanceof Byte) {
        return lhs < (Byte) o2 ? -1 : lhs > (Byte) o2 ? 1 : 0;
      }
      if (o2 instanceof Long) {
        return lhs < (Long) o2 ? -1 : lhs > (Long) o2 ? 1 : 0;
      }
      if (o2 instanceof Integer) {
        return lhs < (Integer) o2 ? -1 : lhs > (Integer) o2 ? 1 : 0;
      }
    }
    return null;
  }

  private static Comparator longComparator = new LongComparator();

  public static Comparator getIntComparator() {
    return intComparator;
  }

  private static Comparator intComparator = new Comparator() {
    @Override
    public int compare(Object o1, Object o2) {
      if (o1 == null || o2 == null) {
        return 0;
      }
      Integer ret = compareNumerics(o1, o2);
      if (ret == null) {
        Integer lhs = (Integer) intConverter.convert(o1);
        Integer rhs = (Integer) intConverter.convert(o2);
        return lhs.compareTo(rhs);
      }
      return ret;
    }
  };

  public static Comparator getDoubleComparator() {
    return doubleComparator;
  }

  private static Comparator doubleComparator = new Comparator() {
    @Override
    public int compare(Object o1, Object o2) {
      if (o1 == null || o2 == null) {
        return 0;
      }
      Integer ret = compareNumerics(o1, o2);
      if (ret == null) {
        Double lhs = (Double) doubleConverter.convert(o1);
        Double rhs = (Double) doubleConverter.convert(o2);
        return lhs.compareTo(rhs);
      }
      return ret;
    }
  };

  public static Comparator getFloatComparator() {
    return floatComparator;
  }

  private static Comparator floatComparator = new Comparator() {
    @Override
    public int compare(Object o1, Object o2) {
      if (o1 == null || o2 == null) {
        return 0;
      }
      Integer ret = compareNumerics(o1, o2);
      if (ret == null) {
        Float lhs = (Float) floatConverter.convert(o1);
        Float rhs = (Float) floatConverter.convert(o2);
        return lhs.compareTo(rhs);
      }
      return ret;
    }
  };

  public static Comparator getStringComparator() {
    return stringComparator;
  }

  private static Comparator stringComparator = new Comparator() {
    @Override
    public int compare(Object o1, Object o2) {
      try {
        if (o1 == null || o2 == null) {
          return 0;
        }
        o1 = stringConverter.convert(o1);
        o2 = stringConverter.convert(o2);
        return ((String) o1).compareTo((String) o2);
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }
  };

  public static Comparator getUtf8Comparator() {
    return utf8Comparator;
  }

  private static class Utf8Comparator implements Comparator {

    @Override
    public int compare(Object o1, Object o2) {
      try {
        if (o1 == null || o2 == null) {
          return 0;
        }
        o1 = utf8Converter.convert(o1);
        o2 = utf8Converter.convert(o2);
        return (new String((byte[])o1, "utf-8")).compareTo(new String((byte[])o2, "utf-8"));
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }
  }

  private static Comparator utf8Comparator = new Utf8Comparator();

  public static Comparator getByteComparator() {
    return byteComparator;
  }

  private static Comparator byteComparator = new Comparator() {
    @Override
    public int compare(Object o1, Object o2) {
      if (o1 == null || o2 == null) {
        return 0;
      }
      Integer ret = compareNumerics(o1, o2);
      if (ret == null) {
        Byte lhs = (Byte) byteConverter.convert(o1);
        Byte rhs = (Byte) byteConverter.convert(o2);
        return lhs.compareTo(rhs);
      }
      return ret;
    }
  };

  public static Comparator getShortComparator() {
    return shortComparator;
  }

  private static Comparator shortComparator = new Comparator() {
    @Override
    public int compare(Object o1, Object o2) {
      if (o1 == null || o2 == null) {
        return 0;
      }
      Integer ret = compareNumerics(o1, o2);
      if (ret == null) {
        Short lhs = (Short) shortConverter.convert(o1);
        Short rhs = (Short) shortConverter.convert(o2);
        return lhs.compareTo(rhs);
      }
      return ret;
    }
  };

  public static Comparator getBigDecimalComparator() {
    return bigDecimalComparator;
  }

  private static Comparator bigDecimalComparator = new Comparator() {
    @Override
    public int compare(Object o1, Object o2) {
      if (o1 == null || o2 == null) {
        return 0;
      }
      BigDecimal lhs = (BigDecimal) bigDecimalConverter.convert(o1);
      BigDecimal rhs = (BigDecimal) bigDecimalConverter.convert(o2);
      return lhs.compareTo(rhs);
    }
  };

  public static Comparator getDateComparator() {
    return dateComparator;
  }

  private static Comparator dateComparator = new Comparator() {
    @Override
    public int compare(Object o1, Object o2) {
      if (o1 == null || o2 == null) {
        return 0;
      }
      Date lhs = (Date) dateConverter.convert(o1);
      Date rhs = (Date) dateConverter.convert(o2);
      return lhs.compareTo(rhs);
    }
  };

  public static Comparator getTimeComparator() {
    return timeComparator;
  }

  private static Comparator timeComparator = new Comparator() {
    @Override
    public int compare(Object o1, Object o2) {
      if (o1 == null || o2 == null) {
        return 0;
      }
      Time lhs = (Time) timeConverter.convert(o1);
      Time rhs = (Time) timeConverter.convert(o2);
      return lhs.compareTo(rhs);
    }
  };

  public static Comparator getTimestampComparator() {
    return timestampComparator;
  }

  private static Comparator timestampComparator = new Comparator() {
    @Override
    public int compare(Object o1, Object o2) {
      if (o1 == null || o2 == null) {
        return 0;
      }
      Timestamp lhs = (Timestamp) timestampConverter.convert(o1);
      Timestamp rhs = (Timestamp) timestampConverter.convert(o2);
      int ret = lhs.compareTo(rhs);
      return ret;
    }
  };

  public static Comparator getByteArrayComparator() {
    return byteArrayComparator;
  }

  private static class ByteArrayComparator implements Comparator {

    @Override
    public int compare(Object o1, Object o2) {
      if (o1 == null || o2 == null) {
        return 0;
      }
      if (!(o1 instanceof byte[]) || !(o2 instanceof byte[])) {
        throw new DatabaseException("Datatype mismatch - expecting byte[]: found=" + o1.getClass().getName() + ", found=" + o2.getClass().getName());
      }
      for (int i = 0; i < Math.min(((byte[]) o1).length, ((byte[]) o2).length); i++) {
        if (((byte[]) o1)[i] < ((byte[]) o2)[i]) {
          return -1;
        }
        if (((byte[]) o1)[i] > ((byte[]) o2)[i]) {
          return 1;
        }
      }
      return 0;
    }
  }

  private static Comparator byteArrayComparator = new ByteArrayComparator();

  public static Comparator getBlobComparator() {
    return blobComparator;
  }

  private static Comparator blobComparator = new Comparator() {
    @Override
    public int compare(Object o1, Object o2) {
      if (o1 == null || o2 == null) {
        return 0;
      }
      if (!(o1 instanceof byte[]) || !(o2 instanceof byte[])) {
        throw new DatabaseException("Datatype mismatch - expecting byte[]: found=" + o1.getClass().getName() + ", found=" + o2.getClass().getName());
      }
      byte[] lhs = (byte[]) o1;
      byte[] rhs = (byte[]) o2;

      for (int i = 0; i < Math.min(lhs.length, rhs.length); i++) {
        if (lhs[i] < rhs[i]) {
          return -1;
        }
        if (lhs[i] > rhs[i]) {
          return 1;
        }
      }
      return 0;
    }
  };

  public static interface Incrementer {
    public Object increment(Object value);
  }

  public static Incrementer getIntIncrementer() {
    return intIncrementer;
  }

  private static Incrementer intIncrementer = new Incrementer() {
    @Override
    public Object increment(Object value) {
      return ((Integer) value) + 1;
    }
  };

  public static Incrementer getLongIncrementer() {
    return longIncrementer;
  }

  private static Incrementer longIncrementer = new Incrementer() {
    @Override
    public Object increment(Object value) {
      return ((Long) value) + 1;
    }
  };

  public static Incrementer getBigDecimalIncrementer() {
    return bigDecimalIncrementer;
  }

  private static Incrementer bigDecimalIncrementer = new Incrementer() {
    @Override
    public Object increment(Object value) {
      return ((BigDecimal) value).add(new BigDecimal(1));
    }
  };



  static Map<Integer, DataType.Type> types = new HashMap<>();

  public enum Type {
    BIT(Types.BIT, booleanComparator, booleanConverter, null, null),
    TINYINT(Types.TINYINT, byteComparator, byteConverter, null, null),
    SMALLINT(Types.SMALLINT, shortComparator, shortConverter, null, null),
    INTEGER(Types.INTEGER, intComparator, intConverter, new Integer(0), intIncrementer),
    BIGINT(Types.BIGINT, longComparator, longConverter, new Long(0), longIncrementer),
    FLOAT(Types.FLOAT, doubleComparator, doubleConverter, null, null),
    REAL(Types.REAL, floatComparator, floatConverter, null, null),
    DOUBLE(Types.DOUBLE, doubleComparator, doubleConverter, null, null),
    NUMERIC(Types.NUMERIC, bigDecimalComparator, bigDecimalConverter, new BigDecimal(0), bigDecimalIncrementer),
    DECIMAL(Types.DECIMAL, bigDecimalComparator, bigDecimalConverter, new BigDecimal(0), bigDecimalIncrementer),
    CHAR(Types.CHAR, utf8Comparator, utf8Converter, null, null),
    VARCHAR(Types.VARCHAR, utf8Comparator, utf8Converter, null, null),
    LONGVARCHAR(Types.LONGVARCHAR, utf8Comparator, utf8Converter, null, null),
    DATE(Types.DATE, dateComparator, dateConverter, null, null),
    TIME(Types.TIME, timeComparator, timeConverter, null, null),
    TIMESTAMP(Types.TIMESTAMP, timestampComparator, timestampConverter, null, null),
    BINARY(Types.BINARY, blobComparator, blobConverter, null, null),
    VARBINARY(Types.VARBINARY, blobComparator, blobConverter, null, null),
    LONGVARBINARY(Types.LONGVARBINARY, blobComparator, blobConverter, null, null),
    NULL(Types.NULL, null, null, null, null),
    OTHER(Types.OTHER, null, null, null, null),
    JAVA_OBJECT(Types.JAVA_OBJECT, null, null, null, null),
    DISTINCT(Types.DISTINCT, null, null, null, null),
    STRUCT(Types.STRUCT, null, null, null, null),
    ARRAY(Types.ARRAY, null, null, null, null),
    BLOB(Types.BLOB, blobComparator, blobConverter, null, null),
    CLOB(Types.CLOB, utf8Comparator, utf8Converter, null, null),
    REF(Types.REF, null, null, null, null),
    DATALINK(Types.DATALINK, null, null, null, null),
    BOOLEAN(Types.BOOLEAN, booleanComparator, booleanConverter, null, null),
    ROWID(Types.ROWID, longComparator, longConverter, null, null),
    NCHAR(Types.NCHAR, utf8Comparator, utf8Converter, null, null),
    NVARCHAR(Types.NVARCHAR, utf8Comparator, utf8Converter, null, null),
    LONGNVARCHAR(Types.LONGNVARCHAR, utf8Comparator, utf8Converter, null, null),
    NCLOB(Types.NCLOB, utf8Comparator, utf8Converter, null, null),
    SQLXML(Types.SQLXML, null, null, null, null),
    //REF_CURSOR(Types.REF_CURSOR, null, null, null),
    //TIME_WITH_TIMEZONE(Types.TIME_WITH_TIMEZONE, null, null, null),
    //TIMESTAMP_WITH_TIMEZONE(Types.TIMESTAMP_WITH_TIMEZONE, null, null, null),
    PARAMETER(-999999999, null, null, null, null);
    ;
    private final int value;
    private final Comparator comparator;
    private final Incrementer incrementer;
    private final Converter converter;
    private Object initialValue;

    public static DataType.Type valueOf(int value) {
      return types.get(value);
    }

    Type(int value, Comparator comparator, Converter converter, Object initialValue, Incrementer incrementer) {
      this.value = value;
      types.put(value, this);
      this.comparator = comparator;
      this.converter = converter;
      this.initialValue = initialValue;
      this.incrementer = incrementer;
    }

    public int getValue() {
      return value;
    }

    public Incrementer getIncrementer() {
      return incrementer;
    }

    public Comparator getComparator() {
      return comparator;
    }

    public Converter getConverter() {
      return converter;
    }

    public static Comparator getComparatorForValue(Object lhsValue) {
      if (lhsValue instanceof String) {
        return stringComparator;
      }
      else if (lhsValue instanceof byte[]) {
        return utf8Comparator;
      }
      if (lhsValue instanceof Long) {
        return longComparator;
      }
      if (lhsValue instanceof Integer) {
        return intComparator;
      }
      if (lhsValue instanceof Short) {
        return shortComparator;
      }
      if (lhsValue instanceof Byte) {
        return byteComparator;
      }
      if (lhsValue instanceof Double) {
        return doubleComparator;
      }
      if (lhsValue instanceof Float) {
        return floatComparator;
      }
      if (lhsValue instanceof Date) {
        return dateComparator;
      }
      if (lhsValue instanceof Time) {
        return timeComparator;
      }
      if (lhsValue instanceof Timestamp) {
        return timestampComparator;
      }
      if (lhsValue instanceof Boolean) {
        return booleanComparator;
      }
      if (lhsValue instanceof BigDecimal) {
        return bigDecimalComparator;
      }
      if (lhsValue instanceof byte[]) {
        return byteArrayComparator;
      }
      return null;
    }

    public static int getTypeForValue(Object lhsValue) {
      if (lhsValue instanceof String) {
        return LONGVARCHAR.getValue();
      }
      else if (lhsValue instanceof byte[]) {
        return LONGVARCHAR.getValue();
      }
      if (lhsValue instanceof Long) {
        return BIGINT.getValue();
      }
      if (lhsValue instanceof Integer) {
        return INTEGER.getValue();
      }
      if (lhsValue instanceof Short) {
        return SMALLINT.getValue();
      }
      if (lhsValue instanceof Byte) {
        return TINYINT.getValue();
      }
      if (lhsValue instanceof Double) {
        return DOUBLE.getValue();
      }
      if (lhsValue instanceof Float) {
        return FLOAT.getValue();
      }
      if (lhsValue instanceof Date) {
        return DATE.getValue();
      }
      if (lhsValue instanceof Time) {
        return TIME.getValue();
      }
      if (lhsValue instanceof Timestamp) {
        return TIMESTAMP.getValue();
      }
      if (lhsValue instanceof Boolean) {
        return BIT.getValue();
      }
      if (lhsValue instanceof BigDecimal) {
        return DECIMAL.getValue();
      }
      return -1;
    }

    public Object getInitialValue() {
      return initialValue;
    }
  }
}

