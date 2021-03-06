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
@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class DataType {

  private static final String UTF_8_STR = "utf-8";

  public interface Converter {
    Object convert(Object value);
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
        try {
          ret = (long) Long.valueOf((String) value);
        }
        catch (NumberFormatException e) {
          ret = (long) (double)Double.valueOf((String) value);
        }
      }
      else if (value instanceof char[]) {
        try {
          ret = (long) Long.valueOf(new String((char[]) value));
        }
        catch (NumberFormatException e) {
          ret = (long) (double)Double.valueOf(new String((char[]) value));
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

  private static final Converter longConverter = new LongConverter();

  public static Converter getStringConverter() {
    return stringConverter;
  }

  private static final Converter stringConverter = value -> {
    if (value == null) {
      return null;
    }
    if (value instanceof String) {
      return value;
    }
    if (value instanceof char[]) {
      return new String((char[])value);
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
  };

  static Converter getCharArrayConverter() {
    return charArrayConverter;
  }

  private static final Converter charArrayConverter = value -> {
    if (value == null) {
      return null;
    }
    if (value instanceof byte[]) {
      try {
        String str = new String((byte[])value, "utf-8");
        char[] chars = new char[str.length()];
        str.getChars(0, str.length(), chars, 0);
        return chars;
      }
      catch (UnsupportedEncodingException e) {
        throw new DatabaseException(e);
      }
    }
    if (value instanceof char[]) {
      return value;
    }
    String ret = (String)stringConverter.convert(value);
    char[] chars = new char[ret.length()];
    ret.getChars(0, ret.length(), chars, 0);
    return chars;
  };

  public static Converter getByteArrayConverter() {
    return byteArrayConverter;
  }

  private static final Converter byteArrayConverter = value -> {
    if (value == null) {
      return null;
    }
    if (value instanceof byte[]) {
      return value;
    }
    return null;
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

  private static final Converter blobConverter = new BlobConverter();

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
      else if (value instanceof char[]) {
        ret = Double.valueOf(new String((char[]) value));
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

  private static final Converter doubleConverter = new DoubleConverter();


  public static Converter getIntConverter() {
    return intConverter;
  }

  private static final Converter intConverter = value -> {
    if (value == null) {
      return null;
    }
    if (value instanceof Integer) {
      return value;
    }
    long ret = (Long) longConverter.convert(value);
    return (int) ret;
  };

  public static Converter getShortConverter() {
    return shortConverter;
  }

  private static final Converter shortConverter = value -> {
    if (value == null) {
      return null;
    }
    if (value instanceof Short) {
      return value;
    }
    long ret = (Long) longConverter.convert(value);
    return (short) ret;
  };

  public static Converter getBooleanConverter() {
    return booleanConverter;
  }

  private static final Converter booleanConverter = value -> {
    if (value == null) {
      return null;
    }
    if (value instanceof Boolean) {
      return value;
    }
    if (value instanceof char[]) {
      return new String((char[])value).equalsIgnoreCase("true");
    }
    if (value instanceof String) {
      return ((String)value).equalsIgnoreCase("true");
    }
    long ret = (Long) longConverter.convert(value);
    return ret == 1;
  };

  public static Converter getByteConverter() {
    return byteConverter;
  }

  private static final Converter byteConverter = value -> {
    if (value == null) {
      return null;
    }
    if (value instanceof Byte) {
      return value;
    }
    long ret = (Long) longConverter.convert(value);
    return (byte) ret;
  };

  public static Converter getFloatConverter() {
    return floatConverter;
  }

  private static final Converter floatConverter = value -> {
    if (value == null) {
      return null;
    }
    if (value instanceof Float) {
      return value;
    }
    double ret = (Double) doubleConverter.convert(value);
    return (float) ret;
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
      else if (value instanceof char[]) {
        ret = BigDecimal.valueOf(Double.valueOf(new String((char[]) value)));
      }
      else if (value instanceof byte[]) {
        try {
          ret = BigDecimal.valueOf(Double.valueOf(new String((byte[]) value, UTF_8_STR)));
        }
        catch (UnsupportedEncodingException e) {
          throw new DatabaseException(e);
        }
      }
      else if (value instanceof String) {
        ret = BigDecimal.valueOf(Double.valueOf((String)value));
      }
      else if (value instanceof Float) {
        ret = BigDecimal.valueOf((float) (Float) value);
      }
      else if (value instanceof Double) {
        ret = BigDecimal.valueOf((double) (Double) value);
      }
      else if (value instanceof Integer) {
        ret = BigDecimal.valueOf((int) (Integer) value);
      }
      else if (value instanceof Long) {
        ret = BigDecimal.valueOf((long) (Long) value);
      }
      else if (value instanceof Short) {
        ret = BigDecimal.valueOf((short) (Short) value);
      }
      else if (value instanceof Byte) {
        ret = BigDecimal.valueOf((short) (byte) (Byte) value);
      }
      else {
        throw new DatabaseException("Incompatible datatypes: lhs=BigDecimal, rhs=" + value.getClass().getName());
      }
      return ret;
    }
  }

  private static final Converter bigDecimalConverter = new BigDecimalConverter();

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
      else if (value instanceof char[]) {
        String str = new String((char[]) value);
        Calendar cal = DateUtils.fromDbCalString(str);
        ret = new Date(cal.getTimeInMillis());
      }
      else if (value instanceof byte[]) {
        try {
          String str = new String((byte[]) value, UTF_8_STR);
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

  private static final Converter dateConverter = new DateConverter();

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
          String str = new String((byte[]) value, UTF_8_STR);
          Calendar cal = DateUtils.fromDbTimeString(str);
          ret = new Time(cal.getTimeInMillis());
        }
        catch (Exception e) {
          throw new DatabaseException(e);
        }
      }
      else if (value instanceof char[]) {
        try {
          String str = new String((char[]) value);
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

  private static final Converter timeConverter = new TimeConverter();

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
          String str = new String((byte[]) value, UTF_8_STR);
          Calendar cal = DateUtils.fromDbCalString(str);
          ret = new Timestamp(cal.getTimeInMillis());
        }
        catch (Exception e) {
          throw new DatabaseException(e);
        }
      }
      else if (value instanceof char[]) {
        try {
          String str = new String((char[]) value);
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
      else if (value instanceof Integer) {
        ret = new Timestamp((long)(int)value);
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

  private static final Converter timestampConverter = new TimestampConverter();

  static Comparator getBooleanComparator() {
    return booleanComparator;
  }

  private static final Comparator booleanComparator = (o1, o2) -> {
    if (o1 == null || o2 == null) {
      return 0;
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
        if (l1 < l2) {
          return -1;
        }
        else {
          return l1 > l2 ? 1 : 0;
        }
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
      Integer lhs = compareLhsDouble((Double) o1, o2);
      if (lhs != null) {
        return lhs;
      }
    }
    if (o1 instanceof Float) {
      Integer x = compareLhsDouble((double)(Float) o1, o2);
      if (x != null) {
        return x;
      }
    }
    if (o1 instanceof Short) {
      Integer x = compareLhsLong((long)(Short) o1, o2);
      if (x != null) {
        return x;
      }
    }
    if (o1 instanceof Byte) {
      Integer x = compareLhsLong((long)(Byte) o1, o2);
      if (x != null) {
        return x;
      }
    }
    if (o1 instanceof Integer) {
      Integer x = compareLhsLong((long)(Integer) o1, o2);
      if (x != null) {
        return x;
      }
    }
    if (o1 instanceof Long) {
      Integer x = compareLhsLong((Long) o1, o2);
      if (x != null) {
        return x;
      }
    }
    return null;
  }

  private static Integer compareLhsLong(Long o1, Object o2) {
    if (o2 instanceof Double) {
      if (o1 > (Double) o2) {
        return 1;
      }
      else if (o1 < (Double) o2) {
        return -1;
      }
      return 0;
    }
    if (o2 instanceof Float) {
      if (o1 > (Float) o2) {
        return 1;
      }
      else if (o1 < (Float) o2) {
        return -1;
      }
      return 0;
    }
    if (o2 instanceof Short) {
      if (o1 > (Short) o2) {
        return 1;
      }
      else if (o1 < (Short) o2) {
        return -1;
      }
      return 0;
    }
    if (o2 instanceof Byte) {
      if (o1 > (Byte) o2) {
        return 1;
      }
      else if (o1 < (Byte) o2) {
        return -1;
      }
      return 0;
    }
    if (o2 instanceof Long) {
      if (o1 > (Long) o2) {
        return 1;
      }
      else if (o1 < (Long) o2) {
        return -1;
      }
      return 0;
    }
    if (o2 instanceof Integer) {
      if (o1 > (Integer) o2) {
        return 1;
      }
      else if (o1 < (Integer) o2) {
        return -1;
      }
      return 0;
    }
    return null;
  }

  private static Integer compareLhsDouble(Double o1, Object o2) {
    if (o2 instanceof Double) {
      if (o1 > (Double) o2) {
        return 1;
      }
      else if (o1 < (Double) o2) {
        return -1;
      }
      return 0;
    }
    if (o2 instanceof Float) {
      if (o1 > (Float) o2) {
        return 1;
      }
      else if (o1 < (Float) o2) {
        return -1;
      }
      return 0;
    }
    if (o2 instanceof Short) {
      if (o1 > (Short) o2) {
        return 1;
      }
      else if (o1 < (Short) o2) {
        return -1;
      }
      return 0;
    }
    if (o2 instanceof Byte) {
      if (o1 > (Byte) o2) {
        return 1;
      }
      else if (o1 < (Byte) o2) {
        return -1;
      }
      return 0;
    }
    if (o2 instanceof Long) {
      if (o1 > (Long) o2) {
        return 1;
      }
      else if (o1 < (Long) o2) {
        return -1;
      }
      return 0;
    }
    if (o2 instanceof Integer) {
      if (o1 > (Integer) o2) {
        return 1;
      }
      else if (o1 < (Integer) o2) {
        return -1;
      }
      return 0;
    }
    return null;
  }

  private static final Comparator longComparator = new LongComparator();

  static Comparator getIntComparator() {
    return intComparator;
  }

  private static final Comparator intComparator = (o1, o2) -> {
    Integer ret = compareNumerics(o1, o2);
    if (ret == null) {
      Integer lhs = (Integer) intConverter.convert(o1);
      Integer rhs = (Integer) intConverter.convert(o2);
      return lhs.compareTo(rhs);
    }
    return ret;
  };

  public static Comparator getDoubleComparator() {
    return doubleComparator;
  }

  private static final Comparator doubleComparator = (o1, o2) -> {
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
  };

  static Comparator getFloatComparator() {
    return floatComparator;
  }

  private static final Comparator floatComparator = (o1, o2) -> {
    Integer ret = compareNumerics(o1, o2);
    if (ret == null) {
      Float lhs = (Float) floatConverter.convert(o1);
      Float rhs = (Float) floatConverter.convert(o2);
      return lhs.compareTo(rhs);
    }
    return ret;
  };

  static Comparator getStringComparator() {
    return stringComparator;
  }

  private static final Comparator stringComparator = (o1, o2) -> {
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
  };

  public static Comparator getCharArrayComparator() {
    return charArrayComparator;
  }

  private static class CharArrayComparator implements Comparator {

    @Override
    public int compare(Object o1, Object o2) {
      try {
        if (o1 == null ||  o2 == null) {
          return 0;
        }
        if (!(o1 instanceof char[])) {
          o1 = charArrayConverter.convert(o1);
        }
        if (!(o2 instanceof char[])) {
          o2 = charArrayConverter.convert(o2);
        }
        char[] o1Chars = (char[])o1;
        char[] o2Chars = (char[])o2;
        int len1 = o1Chars.length;
        int len2 = o2Chars.length;
        int lim = Math.min(len1, len2);
        char v1[] = o1Chars;
        char v2[] = o2Chars;

        int k = 0;
        while (k < lim) {
          char c1 = v1[k];
          char c2 = v2[k];
          if (c1 != c2) {
            return c1 - c2;
          }
          k++;
        }
        return len1 - len2;
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }
  }

  private static final Comparator charArrayComparator = new CharArrayComparator();

  static Comparator getByteComparator() {
    return byteComparator;
  }

  private static final Comparator byteComparator = (o1, o2) -> {
    Integer ret = compareNumerics(o1, o2);
    if (ret == null) {
      Byte lhs = (Byte) byteConverter.convert(o1);
      Byte rhs = (Byte) byteConverter.convert(o2);
      return lhs.compareTo(rhs);
    }
    return ret;
  };

  static Comparator getShortComparator() {
    return shortComparator;
  }

  private static final Comparator shortComparator = (o1, o2) -> {
    Integer ret = compareNumerics(o1, o2);
    if (ret == null) {
      Short lhs = (Short) shortConverter.convert(o1);
      Short rhs = (Short) shortConverter.convert(o2);
      return lhs.compareTo(rhs);
    }
    return ret;
  };

  public static Comparator getBigDecimalComparator() {
    return bigDecimalComparator;
  }

  private static final Comparator bigDecimalComparator = (o1, o2) -> {
    if (o1 == null || o2 == null) {
      return 0;
    }
    BigDecimal lhs = (BigDecimal) bigDecimalConverter.convert(o1);
    BigDecimal rhs = (BigDecimal) bigDecimalConverter.convert(o2);
    return lhs.compareTo(rhs);
  };

  public static Comparator getDateComparator() {
    return dateComparator;
  }

  private static final Comparator dateComparator = (o1, o2) -> {
    if (o1 == null || o2 == null) {
      return 0;
    }
    Date lhs = (Date) dateConverter.convert(o1);
    Date rhs = (Date) dateConverter.convert(o2);
    long l = lhs.getTime();
    long r = rhs.getTime();

    return Long.compare(l, r);
  };

  static Comparator getTimeComparator() {
    return timeComparator;
  }

  private static final Comparator timeComparator = (o1, o2) -> {
    if (o1 == null || o2 == null) {
      return 0;
    }
    Time lhs = (Time) timeConverter.convert(o1);
    Time rhs = (Time) timeConverter.convert(o2);
    long l = lhs.getTime();
    long r = rhs.getTime();
    return Long.compare(l, r);
  };

  static Comparator getTimestampComparator() {
    return timestampComparator;
  }

  private static final Comparator timestampComparator = (o1, o2) -> {
    if (o1 == null || o2 == null) {
      return 0;
    }
    Timestamp lhs = (Timestamp) timestampConverter.convert(o1);
    Timestamp rhs = (Timestamp) timestampConverter.convert(o2);
    return lhs.compareTo(rhs);
  };

  static Comparator getByteArrayComparator() {
    return byteArrayComparator;
  }

  private static class ByteArrayComparator implements Comparator {

    @Override
    public int compare(Object o1, Object o2) {
      if (o1 == null || o2 == null) {
        return 0;
      }
      if (!(o1 instanceof byte[]) || !(o2 instanceof byte[])) {
        throw new DatabaseException("Datatype mismatch - expecting byte[]: found=" + o1.getClass().getName() +
            ", found=" + o2.getClass().getName());
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

  private static final Comparator byteArrayComparator = new ByteArrayComparator();

  static Comparator getBlobComparator() {
    return blobComparator;
  }

  private static final Comparator blobComparator = (o1, o2) -> {
    if (o1 == null || o2 == null) {
      return 0;
    }
    if (!(o1 instanceof byte[]) || !(o2 instanceof byte[])) {
      throw new DatabaseException("Datatype mismatch - expecting byte[]: found=" + o1.getClass().getName() +
          ", found=" + o2.getClass().getName());
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
  };

  public interface Incrementer {
    Object increment(Object value);
  }

  public static Incrementer getIntIncrementer() {
    return intIncrementer;
  }

  private static final Incrementer intIncrementer = value -> ((Integer) value) + 1;

  public static Incrementer getLongIncrementer() {
    return longIncrementer;
  }

  private static final Incrementer longIncrementer = value -> ((Long) value) + 1;

  public static Incrementer getBigDecimalIncrementer() {
    return bigDecimalIncrementer;
  }

  private static final Incrementer bigDecimalIncrementer = value -> ((BigDecimal) value).add(new BigDecimal(1));



  static final Map<Integer, DataType.Type> types = new HashMap<>();

  public enum Type {
    BIT(Types.BIT, booleanComparator, booleanConverter, null, null),
    TINYINT(Types.TINYINT, byteComparator, byteConverter, null, null),
    SMALLINT(Types.SMALLINT, shortComparator, shortConverter, null, null),
    INTEGER(Types.INTEGER, intComparator, intConverter, 0, intIncrementer),
    BIGINT(Types.BIGINT, longComparator, longConverter, 0L, longIncrementer),
    FLOAT(Types.FLOAT, doubleComparator, doubleConverter, null, null),
    REAL(Types.REAL, floatComparator, floatConverter, null, null),
    DOUBLE(Types.DOUBLE, doubleComparator, doubleConverter, null, null),
    NUMERIC(Types.NUMERIC, bigDecimalComparator, bigDecimalConverter, new BigDecimal(0), bigDecimalIncrementer),
    DECIMAL(Types.DECIMAL, bigDecimalComparator, bigDecimalConverter, new BigDecimal(0), bigDecimalIncrementer),
    CHAR(Types.CHAR, charArrayComparator, charArrayConverter, null, null),
    VARCHAR(Types.VARCHAR, charArrayComparator, charArrayConverter, null, null),
    LONGVARCHAR(Types.LONGVARCHAR, charArrayComparator, charArrayConverter, null, null),
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
    CLOB(Types.CLOB, charArrayComparator, charArrayConverter, null, null),
    REF(Types.REF, null, null, null, null),
    DATALINK(Types.DATALINK, null, null, null, null),
    BOOLEAN(Types.BOOLEAN, booleanComparator, booleanConverter, null, null),
    ROWID(Types.ROWID, longComparator, longConverter, null, null),
    NCHAR(Types.NCHAR, charArrayComparator, charArrayConverter, null, null),
    NVARCHAR(Types.NVARCHAR, charArrayComparator, charArrayConverter, null, null),
    LONGNVARCHAR(Types.LONGNVARCHAR, charArrayComparator, charArrayConverter, null, null),
    NCLOB(Types.NCLOB, charArrayComparator, charArrayConverter, null, null),
    SQLXML(Types.SQLXML, null, null, null, null),
    //REF_CURSOR(Types.REF_CURSOR, null, null, null),
    //TIME_WITH_TIMEZONE(Types.TIME_WITH_TIMEZONE, null, null, null),
    //TIMESTAMP_WITH_TIMEZONE(Types.TIMESTAMP_WITH_TIMEZONE, null, null, null),
    PARAMETER(-999999999, null, null, null, null);

    private final int value;
    private final Comparator comparator;
    private final Incrementer incrementer;
    private final Converter converter;
    private final Object initialValue;

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
      else if (lhsValue instanceof char[]) {
        return charArrayComparator;
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
      return null;
    }

    public static int getTypeForValue(Object lhsValue) {
      if (lhsValue instanceof String || lhsValue instanceof char[]) {
        return LONGVARCHAR.getValue();
      }
      if (lhsValue instanceof byte[]) {
        return BLOB.getValue();
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
      return Integer.MIN_VALUE;
    }

    public Object getInitialValue() {
      return initialValue;
    }
  }
}

