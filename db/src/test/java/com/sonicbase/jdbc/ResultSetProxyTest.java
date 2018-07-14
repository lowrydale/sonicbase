/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.jdbc;

import com.sonicbase.jdbcdriver.ResultSetProxy;
import com.sonicbase.query.impl.ResultSetImpl;
import org.apache.commons.io.IOUtils;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class ResultSetProxyTest {

  @Test
  public void test() throws SQLException, IOException {
    ResultSetImpl rs = mock(ResultSetImpl.class);
    ResultSetProxy rsp = new ResultSetProxy(null, rs);

    //getNCharacterStream
    when(rs.getString(anyString())).thenReturn("value");
    assertEquals(IOUtils.toString(rsp.getNCharacterStream("field")), "value");
    when(rs.getString(anyString())).thenReturn(null);
    assertEquals(rsp.getNCharacterStream("field"), null);
    try {
      when(rs.getString(anyString())).thenThrow(new RuntimeException());
      rsp.getNCharacterStream("field");
      fail();
    }
    catch (SQLException e) {

    }

    when(rs.getString(anyInt())).thenReturn("value");
    assertEquals(IOUtils.toString(rsp.getNCharacterStream(1)), "value");
    when(rs.getString(anyInt())).thenReturn(null);
    assertEquals(rsp.getNCharacterStream(1), null);
    try {
      when(rs.getString(anyInt())).thenThrow(new RuntimeException());
      rsp.getNCharacterStream(1);
      fail();
    }
    catch (SQLException e) {

    }

    //getNString
    rs = mock(ResultSetImpl.class);
    rsp = new ResultSetProxy(null, rs);
    when(rs.getString(anyString())).thenReturn("value");
    assertEquals(rsp.getNString("field"), "value");
    when(rs.getString(anyString())).thenReturn(null);
    assertEquals(rsp.getNString("field"), null);
    try {
      when(rs.getString(anyString())).thenThrow(new RuntimeException());
      rsp.getNString("field");
      fail();
    }
    catch (SQLException e) {

    }

    when(rs.getString(anyInt())).thenReturn("value");
    assertEquals(rsp.getNString(1), "value");
    when(rs.getString(anyInt())).thenReturn(null);
    assertEquals(rsp.getNString(1), null);
    try {
      when(rs.getString(anyInt())).thenThrow(new RuntimeException());
      rsp.getNString(1);
      fail();
    }
    catch (SQLException e) {

    }

    //getNClob
    rs = mock(ResultSetImpl.class);
    rsp = new ResultSetProxy(null, rs);
    when(rs.getString(anyString())).thenReturn("value");
    assertEquals(IOUtils.toString(rsp.getNClob("field").getCharacterStream()), "value");
    when(rs.getString(anyString())).thenReturn(null);
    assertEquals(rsp.getNClob("field"), null);
    try {
      when(rs.getString(anyString())).thenThrow(new RuntimeException());
      rsp.getNClob("field");
      fail();
    }
    catch (SQLException e) {

    }

    when(rs.getString(anyInt())).thenReturn("value");
    assertEquals(IOUtils.toString(rsp.getNClob(1).getCharacterStream()), "value");
    when(rs.getString(anyInt())).thenReturn(null);
    assertEquals(rsp.getNClob(1), null);
    try {
      when(rs.getString(anyInt())).thenThrow(new RuntimeException());
      rsp.getNClob(1);
      fail();
    }
    catch (SQLException e) {

    }

    //getClob
    rs = mock(ResultSetImpl.class);
    rsp = new ResultSetProxy(null, rs);
    when(rs.getString(anyString())).thenReturn("value");
    assertEquals(IOUtils.toString(rsp.getClob("field").getCharacterStream()), "value");
    when(rs.getString(anyString())).thenReturn(null);
    assertEquals(rsp.getClob("field"), null);
    try {
      when(rs.getString(anyString())).thenThrow(new RuntimeException());
      rsp.getString("field");
      fail();
    }
    catch (SQLException e) {

    }

    when(rs.getString(anyInt())).thenReturn("value");
    assertEquals(IOUtils.toString(rsp.getClob(1).getCharacterStream()), "value");
    when(rs.getString(anyInt())).thenReturn(null);
    assertEquals(rsp.getClob(1), null);
    try {
      when(rs.getString(anyInt())).thenThrow(new RuntimeException());
      rsp.getString(1);
      fail();
    }
    catch (SQLException e) {

    }

    //getBlob
    rs = mock(ResultSetImpl.class);
    rsp = new ResultSetProxy(null, rs);
    when(rs.getBytes(anyString())).thenReturn(new byte[]{1,2,3});
    assertEquals(rsp.getBlob("field").getBytes(0,3), new byte[]{1,2,3});
    when(rs.getBytes(anyString())).thenReturn(null);
    assertEquals(rsp.getBlob("field"), null);
    try {
      when(rs.getBytes(anyString())).thenThrow(new RuntimeException());
      rsp.getBlob("field");
      fail();
    }
    catch (SQLException e) {

    }

    when(rs.getBytes(anyInt())).thenReturn(new byte[]{1,2,3});
    assertEquals(rsp.getBlob(1).getBytes(0, 3), new byte[]{1,2,3});
    when(rs.getBytes(anyInt())).thenReturn(null);
    assertEquals(rsp.getBlob(1), null);
    try {
      when(rs.getBytes(anyInt())).thenThrow(new RuntimeException());
      rsp.getBlob(1);
      fail();
    }
    catch (SQLException e) {

    }

    //getBigDecimal
    rs = mock(ResultSetImpl.class);
    rsp = new ResultSetProxy(null, rs);
    when(rs.getBigDecimal(anyString())).thenReturn(new BigDecimal(123));
    assertEquals(rsp.getBigDecimal("field"), new BigDecimal(123));
    when(rs.getBigDecimal(anyString())).thenReturn(null);
    assertEquals(rsp.getBigDecimal("field"), null);
    try {
      when(rs.getBigDecimal(anyString())).thenThrow(new RuntimeException());
      rsp.getBigDecimal("field");
      fail();
    }
    catch (SQLException e) {

    }

    when(rs.getBigDecimal(anyInt())).thenReturn(new BigDecimal(123));
    assertEquals(rsp.getBigDecimal(1), new BigDecimal(123));
    when(rs.getBigDecimal(anyInt())).thenReturn(null);
    assertEquals(rsp.getBigDecimal(1), null);
    try {
      when(rs.getBigDecimal(anyInt())).thenThrow(new RuntimeException());
      rsp.getBigDecimal(1);
      fail();
    }
    catch (SQLException e) {

    }

    //getCharacterStream
    rs = mock(ResultSetImpl.class);
    rsp = new ResultSetProxy(null, rs);
    when(rs.getCharacterStream(anyString())).thenReturn(new BufferedReader(new InputStreamReader(new ByteArrayInputStream("123".getBytes("utf-8")))));
    assertEquals(IOUtils.toString(rsp.getCharacterStream("field")), "123");
    when(rs.getCharacterStream(anyString())).thenReturn(null);
    assertEquals(rsp.getCharacterStream("field"), null);
    try {
      when(rs.getCharacterStream(anyString())).thenThrow(new RuntimeException());
      rsp.getCharacterStream("field");
      fail();
    }
    catch (SQLException e) {

    }

    when(rs.getCharacterStream(anyInt())).thenReturn(new BufferedReader(new InputStreamReader(new ByteArrayInputStream("123".getBytes("utf-8")))));
    assertEquals(IOUtils.toString(rsp.getCharacterStream(1)), "123");
    when(rs.getCharacterStream(anyInt())).thenReturn(null);
    assertEquals(rsp.getCharacterStream(1), null);
    try {
      when(rs.getCharacterStream(anyInt())).thenThrow(new RuntimeException());
      rsp.getCharacterStream(1);
      fail();
    }
    catch (SQLException e) {

    }

    //getBinaryStream
    rs = mock(ResultSetImpl.class);
    rsp = new ResultSetProxy(null, rs);
    when(rs.getBinaryStream(anyString())).thenReturn(new ByteArrayInputStream("123".getBytes("utf-8")));
    assertEquals(IOUtils.toString(rsp.getBinaryStream("field")), "123");
    when(rs.getBinaryStream(anyString())).thenReturn(null);
    assertEquals(rsp.getBinaryStream("field"), null);
    try {
      when(rs.getBinaryStream(anyString())).thenThrow(new RuntimeException());
      rsp.getBinaryStream("field");
      fail();
    }
    catch (SQLException e) {

    }

    when(rs.getBinaryStream(anyInt())).thenReturn(new ByteArrayInputStream("123".getBytes("utf-8")));
    assertEquals(IOUtils.toString(rsp.getBinaryStream(1)), "123");
    when(rs.getBinaryStream(anyInt())).thenReturn(null);
    assertEquals(rsp.getBinaryStream(1), null);
    try {
      when(rs.getBinaryStream(anyInt())).thenThrow(new RuntimeException());
      rsp.getBinaryStream(1);
      fail();
    }
    catch (SQLException e) {

    }

    //getUnicodeStream
    rs = mock(ResultSetImpl.class);
    rsp = new ResultSetProxy(null, rs);
    when(rs.getUnicodeStream(anyString())).thenReturn(new ByteArrayInputStream("123".getBytes("utf-8")));
    assertEquals(IOUtils.toString(rsp.getUnicodeStream("field")), "123");
    when(rs.getUnicodeStream(anyString())).thenReturn(null);
    assertEquals(rsp.getUnicodeStream("field"), null);
    try {
      when(rs.getUnicodeStream(anyString())).thenThrow(new RuntimeException());
      rsp.getUnicodeStream("field");
      fail();
    }
    catch (SQLException e) {

    }


//    when(rs.getUnicodeStream(anyInt())).thenReturn(new ByteArrayInputStream("123".getBytes("utf-8")));
//    assertEquals(IOUtils.toString(rsp.getUnicodeStream(1)), "123");
//    when(rs.getUnicodeStream(anyInt())).thenReturn(null);
//    assertEquals(rsp.getUnicodeStream(1), null);

    //getTimestamp
    rs = mock(ResultSetImpl.class);
    rsp = new ResultSetProxy(null, rs);
    when(rs.getTimestamp(anyString())).thenReturn(new Timestamp(123));
    assertEquals(rsp.getTimestamp("field"), new Timestamp(123));
    when(rs.getTimestamp(anyString())).thenReturn(null);
    assertEquals(rsp.getTimestamp("field"), null);
    try {
      when(rs.getTimestamp(anyString())).thenThrow(new RuntimeException());
      rsp.getTimestamp("field");
      fail();
    }
    catch (SQLException e) {

    }

    when(rs.getTimestamp(anyInt())).thenReturn(new Timestamp(123));
    assertEquals(rsp.getTimestamp(1), new Timestamp(123));
    when(rs.getTimestamp(anyInt())).thenReturn(null);
    assertEquals(rsp.getTimestamp( 1), null);
    try {
      when(rs.getTimestamp(anyInt())).thenThrow(new RuntimeException());
      rsp.getTimestamp(1);
      fail();
    }
    catch (SQLException e) {

    }

    //getTime
    rs = mock(ResultSetImpl.class);
    rsp = new ResultSetProxy(null, rs);
    when(rs.getTime(anyString())).thenReturn(new Time(123));
    assertEquals(rsp.getTime("field"), new Time(123));
    when(rs.getTime(anyString())).thenReturn(null);
    assertEquals(rsp.getTime("field"), null);
    try {
      when(rs.getTime(anyString())).thenThrow(new RuntimeException());
      rsp.getTime("field");
      fail();
    }
    catch (SQLException e) {

    }

    when(rs.getTime(anyInt())).thenReturn(new Time(123));
    assertEquals(rsp.getTime(1), new Time(123));
    when(rs.getTime(anyInt())).thenReturn(null);
    assertEquals(rsp.getTime( 1), null);
    try {
      when(rs.getTime(anyInt())).thenThrow(new RuntimeException());
      rsp.getTime(1);
      fail();
    }
    catch (SQLException e) {

    }

    //getDate
    rs = mock(ResultSetImpl.class);
    rsp = new ResultSetProxy(null, rs);
    when(rs.getDate(anyString())).thenReturn(new Date(123));
    assertEquals(rsp.getDate("field"), new Date(123));
    when(rs.getDate(anyString())).thenReturn(null);
    assertEquals(rsp.getDate("field"), null);
    try {
      when(rs.getDate(anyString())).thenThrow(new RuntimeException());
      rsp.getDate("field");
      fail();
    }
    catch (SQLException e) {

    }

    when(rs.getDate(anyInt())).thenReturn(new Date(123));
    assertEquals(rsp.getDate(1), new Date(123));
    when(rs.getDate(anyInt())).thenReturn(null);
    assertEquals(rsp.getDate( 1), null);
    try {
      when(rs.getDate(anyInt())).thenThrow(new RuntimeException());
      rsp.getDate(1);
      fail();
    }
    catch (SQLException e) {

    }

    //getBytes
    rs = mock(ResultSetImpl.class);
    rsp = new ResultSetProxy(null, rs);
    when(rs.getBytes(anyString())).thenReturn("123".getBytes("utf-8"));
    assertEquals(rsp.getBytes("field"), "123".getBytes("utf-8"));
    when(rs.getBytes(anyString())).thenReturn(null);
    assertEquals(rsp.getBytes("field"), null);
    try {
      when(rs.getBytes(anyString())).thenThrow(new RuntimeException());
      rsp.getBytes("field");
      fail();
    }
    catch (SQLException e) {

    }

    when(rs.getBytes(anyInt())).thenReturn("123".getBytes("utf-8"));
    assertEquals(rsp.getBytes(1), "123".getBytes("utf-8"));
    when(rs.getBytes(anyInt())).thenReturn(null);
    assertEquals(rsp.getBytes(1), null);
    try {
      when(rs.getBytes(anyInt())).thenThrow(new RuntimeException());
      rsp.getBytes(1);
      fail();
    }
    catch (SQLException e) {

    }

    //getBigDecimal
    rs = mock(ResultSetImpl.class);
    rsp = new ResultSetProxy(null, rs);
    when(rs.getBigDecimal(anyString(), anyInt())).thenReturn(new BigDecimal(123));
    assertEquals(rsp.getBigDecimal("field", 1), new BigDecimal(123));
    when(rs.getBigDecimal(anyString(), anyInt())).thenReturn(null);
    assertEquals(rsp.getBigDecimal("field", 1), null);
    try {
      when(rs.getBigDecimal(anyString())).thenThrow(new RuntimeException());
      rsp.getBigDecimal("field");
      fail();
    }
    catch (SQLException e) {

    }

    when(rs.getBigDecimal(anyInt(), anyInt())).thenReturn(new BigDecimal(123));
    assertEquals(rsp.getBigDecimal(1, 1), new BigDecimal(123));
    when(rs.getBigDecimal(anyInt(), anyInt())).thenReturn(null);
    assertEquals(rsp.getBigDecimal( 1, 1), null);
    try {
      when(rs.getBigDecimal(anyInt())).thenThrow(new RuntimeException());
      rsp.getBigDecimal(1);
      fail();
    }
    catch (SQLException e) {

    }

    //getDouble
    rs = mock(ResultSetImpl.class);
    rsp = new ResultSetProxy(null, rs);
    when(rs.getDouble(anyString())).thenReturn(123d);
    assertEquals(rsp.getDouble("field"), 123d);
    when(rs.getDouble(anyString())).thenReturn(null);
    assertEquals(rsp.getDouble("field"), 0d);
    try {
      when(rs.getDouble(anyString())).thenThrow(new RuntimeException());
      rsp.getDouble("field");
      fail();
    }
    catch (SQLException e) {

    }

    when(rs.getDouble(anyInt())).thenReturn(123d);
    assertEquals(rsp.getDouble(1), 123d);
    when(rs.getDouble(anyInt())).thenReturn(null);
    assertEquals(rsp.getDouble( 1), 0d);
    try {
      when(rs.getDouble(anyInt())).thenThrow(new RuntimeException());
      rsp.getDouble(1);
      fail();
    }
    catch (SQLException e) {

    }

    //getFloat
    rs = mock(ResultSetImpl.class);
    rsp = new ResultSetProxy(null, rs);
    when(rs.getFloat(anyString())).thenReturn(123f);
    assertEquals(rsp.getFloat("field"), 123f);
    when(rs.getFloat(anyString())).thenReturn(null);
    assertEquals(rsp.getFloat("field"), 0f);
    try {
      when(rs.getFloat(anyString())).thenThrow(new RuntimeException());
      rsp.getFloat("field");
      fail();
    }
    catch (SQLException e) {

    }

    when(rs.getFloat(anyInt())).thenReturn(123f);
    assertEquals(rsp.getFloat(1), 123f);
    when(rs.getFloat(anyInt())).thenReturn(null);
    assertEquals(rsp.getFloat( 1), 0f);
    try {
      when(rs.getFloat(anyInt())).thenThrow(new RuntimeException());
      rsp.getFloat(1);
      fail();
    }
    catch (SQLException e) {

    }

    //getLong
    rs = mock(ResultSetImpl.class);
    rsp = new ResultSetProxy(null, rs);
    when(rs.getLong(anyString())).thenReturn(123L);
    assertEquals(rsp.getLong("field"), 123L);
    when(rs.getLong(anyString())).thenReturn(null);
    assertEquals(rsp.getLong("field"), 0L);
    try {
      when(rs.getLong(anyString())).thenThrow(new RuntimeException());
      rsp.getLong("field");
      fail();
    }
    catch (SQLException e) {

    }

    when(rs.getLong(anyInt())).thenReturn(123L);
    assertEquals(rsp.getLong(1), 123L);
    when(rs.getLong(anyInt())).thenReturn(null);
    assertEquals(rsp.getLong( 1), 0L);
    try {
      when(rs.getLong(anyInt())).thenThrow(new RuntimeException());
      rsp.getLong(1);
      fail();
    }
    catch (SQLException e) {

    }

    //getInt
    rs = mock(ResultSetImpl.class);
    rsp = new ResultSetProxy(null, rs);
    when(rs.getInt(anyString())).thenReturn(123);
    assertEquals(rsp.getInt("field"), 123);
    when(rs.getInt(anyString())).thenReturn(null);
    assertEquals(rsp.getInt("field"), 0);
    try {
      when(rs.getInt(anyString())).thenThrow(new RuntimeException());
      rsp.getInt("field");
      fail();
    }
    catch (SQLException e) {

    }

    when(rs.getInt(anyInt())).thenReturn(123);
    assertEquals(rsp.getInt(1), 123);
    when(rs.getInt(anyInt())).thenReturn(null);
    assertEquals(rsp.getInt( 1), 0);
    try {
      when(rs.getInt(anyInt())).thenThrow(new RuntimeException());
      rsp.getInt(1);
      fail();
    }
    catch (SQLException e) {

    }

    //getShort
    rs = mock(ResultSetImpl.class);
    rsp = new ResultSetProxy(null, rs);
    when(rs.getShort(anyString())).thenReturn((short)123);
    assertEquals(rsp.getShort("field"), (short)123);
    when(rs.getShort(anyString())).thenReturn(null);
    assertEquals(rsp.getShort("field"), (short)0);
    try {
      when(rs.getShort(anyString())).thenThrow(new RuntimeException());
      rsp.getShort("field");
      fail();
    }
    catch (SQLException e) {

    }

    when(rs.getShort(anyInt())).thenReturn((short)123);
    assertEquals(rsp.getShort(1), (short)123);
    when(rs.getShort(anyInt())).thenReturn(null);
    assertEquals(rsp.getShort( 1), (short)0);
    try {
      when(rs.getShort(anyInt())).thenThrow(new RuntimeException());
      rsp.getShort(1);
      fail();
    }
    catch (SQLException e) {

    }

    //getByte
    rs = mock(ResultSetImpl.class);
    rsp = new ResultSetProxy(null, rs);
    when(rs.getByte(anyString())).thenReturn((byte)123);
    assertEquals(rsp.getByte("field"), (byte)123);
    when(rs.getByte(anyString())).thenReturn(null);
    assertEquals(rsp.getByte("field"), (byte)0);
    try {
      when(rs.getByte(anyString())).thenThrow(new RuntimeException());
      rsp.getByte("field");
      fail();
    }
    catch (SQLException e) {

    }

    when(rs.getByte(anyInt())).thenReturn((byte)123);
    assertEquals(rsp.getByte(1), (byte)123);
    when(rs.getByte(anyInt())).thenReturn(null);
    assertEquals(rsp.getByte( 1), (byte)0);
    try {
      when(rs.getBytes(anyInt())).thenThrow(new RuntimeException());
      rsp.getBytes(1);
      fail();
    }
    catch (SQLException e) {

    }

    //getBoolean
    rs = mock(ResultSetImpl.class);
    rsp = new ResultSetProxy(null, rs);
    when(rs.getBoolean(anyString())).thenReturn(true);
    assertEquals(rsp.getBoolean("field"), true);
    when(rs.getBoolean(anyString())).thenReturn(null);
    assertEquals(rsp.getBoolean("field"), false);
    try {
      when(rs.getBoolean(anyString())).thenThrow(new RuntimeException());
      rsp.getBoolean("field");
      fail();
    }
    catch (SQLException e) {

    }

    when(rs.getBoolean(anyInt())).thenReturn(true);
    assertEquals(rsp.getBoolean(1), true);
    when(rs.getBoolean(anyInt())).thenReturn(null);
    assertEquals(rsp.getBoolean( 1), false);
    try {
      when(rs.getBoolean(anyInt())).thenThrow(new RuntimeException());
      rsp.getBoolean(1);
      fail();
    }
    catch (SQLException e) {

    }

    //getString
    rs = mock(ResultSetImpl.class);
    rsp = new ResultSetProxy(null, rs);
    when(rs.getString(anyString())).thenReturn("123");
    assertEquals(rsp.getString("field"), "123");
    when(rs.getString(anyString())).thenReturn(null);
    assertEquals(rsp.getString("field"), null);
    try {
      when(rs.getString(anyString())).thenThrow(new RuntimeException());
      rsp.getString("field");
      fail();
    }
    catch (SQLException e) {

    }

    when(rs.getString(anyInt())).thenReturn("123");
    assertEquals(rsp.getString(1), "123");
    when(rs.getString(anyInt())).thenReturn(null);
    assertEquals(rsp.getString( 1), null);
    try {
      when(rs.getString(anyInt())).thenThrow(new RuntimeException());
      rsp.getString(1);
      fail();
    }
    catch (SQLException e) {

    }


  }
}
