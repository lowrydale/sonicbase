package com.sonicbase.jdbc;

import com.sonicbase.jdbcdriver.ParameterHandler;
import org.testng.annotations.Test;

import java.io.*;
import java.math.BigDecimal;
import java.sql.*;

import static org.testng.Assert.assertEquals;

public class ParameterHandlerTest {

  @Test
  public void test() throws IOException, SQLException {
    ParameterHandler ph = new ParameterHandler();

    ph.setNClob(1, new StringReader("123"));
    ph.setClob(2, new StringReader("123"));
    ph.setCharacterStream(3, new StringReader("123"));
    ph.setNCharacterStream(4, new StringReader("123"));
    ph.setBinaryStream(5, new ByteArrayInputStream("123".getBytes("utf-8")));
    ph.setAsciiStream(6, new ByteArrayInputStream("123".getBytes("utf-8")));
    NClob nc = new com.sonicbase.query.impl.NClob();
    nc.setString(0, "123");
    ph.setNClob(7, nc);
    ph.setNString(8,"123");
    ph.setTimestamp(9, new Timestamp(123));
    ph.setTime(10, new Time(123));
    ph.setDate(11, new Date(123));
    Clob cl = new com.sonicbase.query.impl.Clob();
    cl.setString(0, "123");
    ph.setClob(12, cl);
    Blob bl = new com.sonicbase.query.impl.Blob();
    bl.setBytes(0, "123".getBytes("utf-8"));
    ph.setBlob(13, bl);
    ph.setUnicodeStream(14, new ByteArrayInputStream("123".getBytes("utf-8")), 3);
    ph.setBytes(15, "123".getBytes("utf-8"));
    ph.setString(16, "123");
    ph.setBigDecimal(17, new BigDecimal(123));
    ph.setDouble(18, 123d);
    ph.setFloat(19, 123f);
    ph.setLong(20, 123L);
    ph.setInt(21,123);
    ph.setShort(22, (short)123);
    ph.setByte(23, (byte)123);
    ph.setBoolean(24, true);

    byte[] bytes = ph.serialize();

    ph = new ParameterHandler();
    ph.deserialize(bytes);

    assertEquals(ph.getValue(1), "123".getBytes("utf-8"));
    assertEquals(ph.getValue(2), "123".getBytes("utf-8"));
    assertEquals(ph.getValue(3), "123".getBytes("utf-8"));
    assertEquals(ph.getValue(4), "123".getBytes("utf-8"));
    assertEquals(ph.getValue(5), "123".getBytes("utf-8"));
    assertEquals(ph.getValue(6), "123".getBytes("utf-8"));
    assertEquals(ph.getValue(7), "123".getBytes("utf-8"));
    assertEquals(ph.getValue(8), "123".getBytes("utf-8"));
    assertEquals(ph.getValue(9), new Timestamp(123));
    assertEquals(ph.getValue(10), new Time(123));
    assertEquals(ph.getValue(11), new Date(123));
    assertEquals(ph.getValue(12), "123".getBytes("utf-8"));
    assertEquals(ph.getValue(13), "123".getBytes("utf-8"));
    assertEquals(ph.getValue(14), "123".getBytes("utf-8"));
    assertEquals(ph.getValue(15), "123".getBytes("utf-8"));
    assertEquals(ph.getValue(16), "123".getBytes("utf-8"));
    assertEquals(ph.getValue(17), new BigDecimal(123));
    assertEquals(ph.getValue(18), 123d);
    assertEquals(ph.getValue(19), 123f);
    assertEquals(ph.getValue(20), 123L);
    assertEquals(ph.getValue(21), 123);
    assertEquals(ph.getValue(22), (short)123);
    assertEquals(ph.getValue(23), (byte)123);
    assertEquals(ph.getValue(24), true);
//    ph.setClob(2, new StringReader("123"));
//    ph.setCharacterStream(3, new StringReader("123"));
//    ph.setNCharacterStream(4, new StringReader("123"));
//    ph.setBinaryStream(5, new ByteArrayInputStream("123".getBytes("utf-8")));
//    ph.setAsciiStream(6, new ByteArrayInputStream("123".getBytes("utf-8")));
//    NClob nc = new NClob();
//    nc.setString(0, "123");
//    ph.setNClob(7, nc);
//    ph.setNString(8,"123");
//    ph.setTimestamp(9, new Timestamp(123));
//    ph.setTime(10, new Time(123));
//    ph.setDate(11, new Date(123));
//    Clob cl = new Clob();
//    cl.setString(0, "123");
//    ph.setClob(12, cl);
//    Blob bl = new Blob();
//    bl.setBytes(0, "123".getBytes("utf-8"));
//    ph.setBlob(13, bl);
//    ph.setUnicodeStream(14, new ByteArrayInputStream("123".getBytes("utf-8")), 3);
//    ph.setBytes(15, "123".getBytes("utf-8"));
//    ph.setString(16, "123");
//    ph.setBigDecimal(17, new BigDecimal(123));
//    ph.setDouble(18, 123d);
//    ph.setFloat(19, 123f);
//    ph.setLong(20, 123L);
//    ph.setInt(21,123);
//    ph.setShort(22, (short)123);
//    ph.setByte(23, (byte)123);
//    ph.setBoolean(24, true);

  }
}
