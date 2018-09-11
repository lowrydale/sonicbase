package com.sonicbase.schema;

import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Comparator;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class DataTypeTest {

  @Test
  public void testLongConverter() throws UnsupportedEncodingException {
    DataType.Converter c = DataType.getLongConverter();

    assertNull(c.convert(null));
    assertEquals(c.convert(1L), 1L);
    assertEquals(c.convert("1"), 1L);
    assertEquals(c.convert("1".getBytes("utf-8")), 1L);
    assertEquals(c.convert(1.2f), 1L);
    assertEquals(c.convert(1.2d), 1L);
    assertEquals(c.convert(1), 1L);
    assertEquals(c.convert((short) 1), 1L);
    assertEquals(c.convert((byte) 1), 1L);
  }

  @Test
  public void testStringConverter() {
    DataType.Converter c = DataType.getStringConverter();

    assertNull(c.convert(null));
    assertEquals(c.convert("1"), "1");
    assertEquals(c.convert(new Date(2018, 2, 1)), "3918-03-01 07:00:00.000+0000");
    assertEquals(c.convert(new Time(12, 10, 1)), "19:10:01.000+0000");
    assertEquals(c.convert(new Timestamp(2018, 2, 1, 12, 10, 1, 0)), "3918-03-01 19:10:01.000+0000");
  }

  @Test
  public void testUtf8Converter() throws UnsupportedEncodingException {
    DataType.Converter c = DataType.getUtf8Converter();

    assertNull(c.convert(null));
    assertEquals(c.convert(new byte[]{1, 2, 3}), new byte[]{1, 2, 3});
    assertEquals(c.convert("1"), "1".getBytes("utf-8"));
    assertEquals(c.convert(new Date(2018, 2, 1)), "3918-03-01 07:00:00.000+0000".getBytes("utf-8"));
    assertEquals(c.convert(new Time(12, 10, 1)), "19:10:01.000+0000".getBytes("utf-8"));
    assertEquals(c.convert(new Timestamp(2018, 2, 1, 12, 10, 1, 0)), "3918-03-01 19:10:01.000+0000".getBytes("utf-8"));
  }

  @Test
  public void testByteArrayConverter() {
    DataType.Converter c = DataType.getByteArrayConverter();

    assertNull(c.convert(null));
    assertEquals(c.convert(new byte[]{1, 2, 3}), new byte[]{1, 2, 3});
  }

  @Test
  public void testBlobConverter() {
    DataType.Converter c = DataType.getBlobConverter();

    assertNull(c.convert(null));
    assertEquals(c.convert(new byte[]{1, 2, 3}), new byte[]{1, 2, 3});
    assertEquals(c.convert(new ByteArrayInputStream(new byte[]{1, 2, 3})), new byte[]{1, 2, 3});
  }

  @Test
  public void testDoubleConverter() throws UnsupportedEncodingException {
    DataType.Converter c = DataType.getDoubleConverter();

    assertNull(c.convert(null));
    assertEquals(c.convert(1L), 1d);
    assertEquals(c.convert("1"), 1d);
    assertEquals(c.convert("1".getBytes("utf-8")), 1d);
    assertEquals(c.convert(1.0f), 1.0d);
    assertEquals(c.convert(1.0d), 1.0d);
    assertEquals(c.convert(1), 1d);
    assertEquals(c.convert((short) 1), 1d);
    assertEquals(c.convert((byte) 1), 1d);
  }

  @Test
  public void testShortConverter() throws UnsupportedEncodingException {
    DataType.Converter c = DataType.getShortConverter();

    assertNull(c.convert(null));
    assertEquals(c.convert(1L), (short) 1);
    assertEquals(c.convert("1"), (short) 1);
    assertEquals(c.convert("1".getBytes("utf-8")), (short) 1);
    assertEquals(c.convert(1.0f), (short) 1);
    assertEquals(c.convert(1.0d), (short) 1);
    assertEquals(c.convert(1), (short) 1);
    assertEquals(c.convert((short) 1), (short) 1);
    assertEquals(c.convert((byte) 1), (short) 1);
  }

  @Test
  public void testIntConverter() throws UnsupportedEncodingException {
    DataType.Converter c = DataType.getIntConverter();

    assertNull(c.convert(null));
    assertEquals(c.convert(1L), 1);
    assertEquals(c.convert("1"), 1);
    assertEquals(c.convert("1".getBytes("utf-8")), 1);
    assertEquals(c.convert(1.2f), 1);
    assertEquals(c.convert(1.2d), 1);
    assertEquals(c.convert(1), 1);
    assertEquals(c.convert((short) 1), 1);
    assertEquals(c.convert((byte) 1), 1);
  }

  @Test
  public void testBooleanConverter() throws UnsupportedEncodingException {
    DataType.Converter c = DataType.getBooleanConverter();

    assertNull(c.convert(null));
    assertEquals(c.convert(1L), true);
    assertEquals(c.convert("true"), true);
    assertEquals(c.convert("true".getBytes("utf-8")), true);
    assertEquals(c.convert(1.0f), true);
    assertEquals(c.convert(1.0d), true);
    assertEquals(c.convert(1), true);
    assertEquals(c.convert((short) 1), true);
    assertEquals(c.convert((byte) 1), true);
  }

  @Test
  public void testByteConverter() throws UnsupportedEncodingException {
    DataType.Converter c = DataType.getByteConverter();

    assertNull(c.convert(null));
    assertEquals(c.convert(1L), (byte) 1);
    assertEquals(c.convert("1"), (byte) 1);
    assertEquals(c.convert("1".getBytes("utf-8")), (byte) 1);
    assertEquals(c.convert(1.0f), (byte) 1);
    assertEquals(c.convert(1.0d), (byte) 1);
    assertEquals(c.convert(1), (byte) 1);
    assertEquals(c.convert((short) 1), (byte) 1);
    assertEquals(c.convert((byte) 1), (byte) 1);
  }

  @Test
  public void testFloatConverter() throws UnsupportedEncodingException {
    DataType.Converter c = DataType.getFloatConverter();

    assertNull(c.convert(null));
    assertEquals(c.convert(1L), 1f);
    assertEquals(c.convert("1"), 1f);
    assertEquals(c.convert("1".getBytes("utf-8")), 1f);
    assertEquals(c.convert(1.0f), 1.0f);
    assertEquals(c.convert(1.0d), 1.0f);
    assertEquals(c.convert(1), 1f);
    assertEquals(c.convert((short) 1), 1f);
    assertEquals(c.convert((byte) 1), 1f);
  }

  @Test
  public void testBigDecimalConverter() throws UnsupportedEncodingException {
    DataType.Converter c = DataType.getBigDecimalConverter();

    assertNull(c.convert(null));
    assertEquals(c.convert(new BigDecimal(1)), new BigDecimal(1));
    assertEquals(c.convert(1L), new BigDecimal(1));
    assertEquals(c.convert("1"), BigDecimal.valueOf(1.0));
    assertEquals(c.convert("1".getBytes("utf-8")), BigDecimal.valueOf(1.0));
    assertEquals(c.convert(1.0f), BigDecimal.valueOf(1.0));
    assertEquals(c.convert(1.0d), BigDecimal.valueOf(1.0));
    assertEquals(c.convert(1), new BigDecimal(1));
    assertEquals(c.convert((short) 1), new BigDecimal(1));
    assertEquals(c.convert((byte) 1), new BigDecimal(1));
  }

  @Test
  public void testDateConverter() throws UnsupportedEncodingException {
    DataType.Converter c = DataType.getDateConverter();

    assertEquals(c.convert("3918-03-01 07:00:00.000+0000"), new Date(2018, 2, 1));
    assertEquals(c.convert("3918-03-01 07:00:00.000+0000".getBytes("utf-8")), new Date(2018, 2, 1));
    assertEquals(c.convert(new Date(2018, 2, 1)), new Date(2018, 2, 1));
//    assertEquals(c.convert(new Time(12, 10, 1)), new Date(3870, 2, 1));
    assertEquals((Date) c.convert(new Timestamp(2018, 2, 1, 0, 0, 0, 0)), new Date(2018, 2, 1));
  }

  @Test
  public void testTimeConverter() throws UnsupportedEncodingException {
    DataType.Converter c = DataType.getTimeConverter();

    assertEquals(c.convert("08:00:00.000+0000"), new Time(1, 0, 0));
    assertEquals(c.convert("08:00:00.000+0000".getBytes("utf-8")), new Time(1, 0, 0));
    //assertEquals(c.convert(new Date(2018, 2, 1)), new Time(0, 0, 0));
    assertEquals(c.convert(new Time(12, 10, 1)), new Time(12, 10, 1));
    //assertEquals(c.convert(new Timestamp(0, 0, 0, 7, 0, 0, 0)), new Time(7, 0, 0));
  }

  @Test
  public void testTimestampConverter() throws UnsupportedEncodingException {
    DataType.Converter c = DataType.getTimestampConverter();

    assertEquals(c.convert("3918-03-01 07:00:00.000+0000"), new Timestamp(2018, 2, 1, 0, 0, 0, 0));
    assertEquals(c.convert("3918-03-01 07:00:00.000+0000".getBytes("utf-8")), new Date(2018, 2, 1));
    assertEquals(c.convert(new Date(2018, 2, 1)), new Timestamp(2018, 2, 1, 0, 0, 0, 0));
//    assertEquals(c.convert(new Time(12, 10, 1)), new Date(3870, 2, 1));
    assertEquals(c.convert(new Timestamp(2018, 2, 1, 0, 0, 0, 0)), new Timestamp(2018, 2, 1, 0, 0, 0, 0));
  }

  @Test
  public void testBooleanComparator() {
    Comparator c = DataType.getBooleanComparator();

    assertEquals(c.compare(null, null), 0);
    assertEquals(c.compare(null, true), 0);
    assertEquals(c.compare(true, null), 0);
    assertEquals(c.compare("true", 1), 0);
  }

  @Test
  public void testLongComparator() {
    Comparator c = DataType.getLongComparator();

    assertEquals(c.compare(null, null), 0);
    assertEquals(c.compare(null, 1), 0);
    assertEquals(c.compare(1, null), 0);

    assertEquals(c.compare("1", "1"), 0);

    assertEquals(c.compare(1d, 1d), 0);
    assertEquals(c.compare(1d, 1f), 0);
    assertEquals(c.compare(1d, (short) 1), 0);
    assertEquals(c.compare(1d, (byte) 1), 0);
    assertEquals(c.compare(1d, 1), 0);
    assertEquals(c.compare(1d, 1L), 0);

    assertEquals(c.compare(1f, 1d), 0);
    assertEquals(c.compare(1f, 1f), 0);
    assertEquals(c.compare(1f, (short) 1), 0);
    assertEquals(c.compare(1f, (byte) 1), 0);
    assertEquals(c.compare(1f, 1), 0);
    assertEquals(c.compare(1f, 1L), 0);

    assertEquals(c.compare((short) 1, 1d), 0);
    assertEquals(c.compare((short) 1, 1f), 0);
    assertEquals(c.compare((short) 1, (short) 1), 0);
    assertEquals(c.compare((short) 1, (byte) 1), 0);
    assertEquals(c.compare((short) 1, 1), 0);
    assertEquals(c.compare((short) 1, 1L), 0);

    assertEquals(c.compare((byte) 1, 1d), 0);
    assertEquals(c.compare((byte) 1, 1f), 0);
    assertEquals(c.compare((byte) 1, (short) 1), 0);
    assertEquals(c.compare((byte) 1, (byte) 1), 0);
    assertEquals(c.compare((byte) 1, 1), 0);
    assertEquals(c.compare((byte) 1, 1L), 0);

    assertEquals(c.compare(1, 1d), 0);
    assertEquals(c.compare(1, 1f), 0);
    assertEquals(c.compare(1, (short) 1), 0);
    assertEquals(c.compare(1, (byte) 1), 0);
    assertEquals(c.compare(1, 1), 0);
    assertEquals(c.compare(1, 1L), 0);

    assertEquals(c.compare(1L, 1d), 0);
    assertEquals(c.compare(1L, 1f), 0);
    assertEquals(c.compare(1L, (short) 1), 0);
    assertEquals(c.compare(1L, (byte) 1), 0);
    assertEquals(c.compare(1L, 1), 0);
    assertEquals(c.compare(1L, 1L), 0);

  }

  @Test
  public void testIntComparator() {
    Comparator c = DataType.getIntComparator();

    assertEquals(c.compare(null, null), 0);
    assertEquals(c.compare(null, 1), 0);
    assertEquals(c.compare(1, null), 0);

    assertEquals(c.compare("1", "1"), 0);

  }

  @Test
  public void testDoubleComparator() {
    Comparator c = DataType.getDoubleComparator();

    assertEquals(c.compare(null, null), 0);
    assertEquals(c.compare(null, 1), 0);
    assertEquals(c.compare(1, null), 0);

    assertEquals(c.compare("1.0", "1.0"), 0);

  }

  @Test
  public void testFloatComparator() {
    Comparator c = DataType.getFloatComparator();

    assertEquals(c.compare(null, null), 0);
    assertEquals(c.compare(null, 1), 0);
    assertEquals(c.compare(1, null), 0);

    assertEquals(c.compare("1.0", "1.0"), 0);

  }

  @Test
  public void testStringComparator() {
    Comparator c = DataType.getStringComparator();

    assertEquals(c.compare(null, null), 0);
    assertEquals(c.compare(null, 1), 0);
    assertEquals(c.compare(1, null), 0);

    assertEquals(c.compare("1.0", "1.0"), 0);

  }


  @Test
  public void testUtf8Comparator() {
    Comparator c = DataType.getStringComparator();

    assertEquals(c.compare(null, null), 0);
    assertEquals(c.compare(null, 1), 0);
    assertEquals(c.compare(1, null), 0);

    assertEquals(c.compare("1.0", "1.0"), 0);

  }

  @Test
  public void testByteComparator() {
    Comparator c = DataType.getByteComparator();

    assertEquals(c.compare(null, null), 0);
    assertEquals(c.compare(null, 1), 0);
    assertEquals(c.compare(1, null), 0);

    assertEquals(c.compare("1.0", "1.0"), 0);

  }

  @Test
  public void testShortComparator() {
    Comparator c = DataType.getShortComparator();

    assertEquals(c.compare(null, null), 0);
    assertEquals(c.compare(null, 1), 0);
    assertEquals(c.compare(1, null), 0);

    assertEquals(c.compare("1.0", "1.0"), 0);

  }

  @Test
  public void testBigDecimalComparator() {
    Comparator c = DataType.getBigDecimalComparator();

    assertEquals(c.compare(null, null), 0);
    assertEquals(c.compare(null, 1), 0);
    assertEquals(c.compare(1, null), 0);

    assertEquals(c.compare("1.0", "1.0"), 0);

  }

  @Test
  public void testDateComparator() {
    Comparator c = DataType.getDateComparator();

    assertEquals(c.compare(null, null), 0);
    assertEquals(c.compare(null, 1), 0);
    assertEquals(c.compare(1, null), 0);

    assertEquals(c.compare("12-02-01", "12-02-01"), 0);

  }

  @Test
  public void testTimeComparator() {
    Comparator c = DataType.getTimeComparator();

    assertEquals(c.compare(null, null), 0);
    assertEquals(c.compare(null, 1), 0);
    assertEquals(c.compare(1, null), 0);

    assertEquals(c.compare("12:02:01", "12:02:01"), 0);

  }

  @Test
  public void testTimestampComparator() {
    Comparator c = DataType.getTimestampComparator();

    assertEquals(c.compare(null, null), 0);
    assertEquals(c.compare(null, 1), 0);
    assertEquals(c.compare(1, null), 0);

    assertEquals(c.compare("12-02-01 12:02:01", "12-02-01 12:02:01"), 0);

  }

  @Test
  public void testByteArrayComparator() {
    Comparator c = DataType.getByteArrayComparator();

    assertEquals(c.compare(null, null), 0);
    assertEquals(c.compare(null, 1), 0);
    assertEquals(c.compare(1, null), 0);

    assertEquals(c.compare(new byte[]{1,2,3}, new byte[]{1,2,3}), 0);

  }

  @Test
  public void testBlobComparator() {
    Comparator c = DataType.getBlobComparator();

    assertEquals(c.compare(null, null), 0);
    assertEquals(c.compare(null, 1), 0);
    assertEquals(c.compare(1, null), 0);

    assertEquals(c.compare(new byte[]{1,2,3}, new byte[]{1,2,3}), 0);

  }
}