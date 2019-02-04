package com.sonicbase.jdbc;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.jdbcdriver.StatementProxy;
import org.testng.annotations.Test;

import java.io.*;
import java.math.BigDecimal;
import java.sql.*;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

public class StatementProxyTest {

  @Test
  public void test() throws SQLException, UnsupportedEncodingException {
    ConnectionProxy cp = mock(ConnectionProxy.class);
    DatabaseClient client = mock(DatabaseClient.class);
    StatementProxy sp = new StatementProxy(cp, client, "select * from person");
    ParameterHandler parms = sp.getParms();

    sp.setNull(1, Types.BIGINT);
    assertNull(parms.getValue(1));

    sp.setBoolean(1, true);
    assertEquals(parms.getValue(1), true);

    sp.setByte(1, (byte)123);
    assertEquals(parms.getValue(1), (byte)123);

    sp.setShort(1, (short)123);
    assertEquals(parms.getValue(1), (short)123);

    sp.setInt(1, 123);
    assertEquals(parms.getValue(1), 123);

    sp.setLong(1, 123L);
    assertEquals(parms.getValue(1), 123L);

    sp.setFloat(1, 123f);
    assertEquals(parms.getValue(1), 123f);

    sp.setDouble(1, 123d);
    assertEquals(parms.getValue(1), 123d);

    sp.setBigDecimal(1, new BigDecimal(123));
    assertEquals(parms.getValue(1), new BigDecimal(123));

    sp.setString(1, "123");
    char[] chars = new char["123".length()];
    "123".getChars(0, "123".length(), chars, 0);
    assertEquals(parms.getValue(1), chars);

    sp.setBytes(1, "123".getBytes("utf-8"));
    assertEquals(parms.getValue(1), "123".getBytes("utf-8"));

    sp.setDate(1, new Date(123));
    assertEquals(parms.getValue(1), new Date(123));

    sp.setTime(1, new Time(123));
    assertEquals(parms.getValue(1), new Time(123));

    sp.setTimestamp(1, new Timestamp(123));
    assertEquals(parms.getValue(1), new Timestamp(123));

    sp.setAsciiStream(1, new ByteArrayInputStream("123".getBytes("utf-8")));
    assertEquals(new String((char[])parms.getValue(1)), "123");

    sp.setUnicodeStream(1, new ByteArrayInputStream("123".getBytes("utf-8")), 3);
    assertEquals(new String((char[])parms.getValue(1)), "123");

    sp.setBinaryStream(1, new ByteArrayInputStream("123".getBytes("utf-8")), 3);
    assertEquals(parms.getValue(1), "123".getBytes("utf-8"));

    chars = new char["123".length()];
    "123".getChars(0, "123".length(), chars, 0);

    sp.setCharacterStream(1, new StringReader(new String(chars)));
    assertEquals(new String((char[])parms.getValue(1)), "123");

    Blob blob = new com.sonicbase.query.impl.Blob("123".getBytes("utf-8"));
    sp.setBlob(1, blob);
    assertEquals(parms.getValue(1), "123".getBytes("utf-8"));

    chars = new char["123".length()];
    "123".getChars(0, "123".length(), chars, 0);

    Clob clob = new com.sonicbase.query.impl.Clob("123");
    sp.setClob(1, clob);
    assertEquals(new String((char[])parms.getValue(1)), "123");

    sp.setNString(1, "123");
    assertEquals(new String((char[])parms.getValue(1)), "123");

    chars = new char["123".length()];
    "123".getChars(0, "123".length(), chars, 0);

    sp.setNCharacterStream(1, new InputStreamReader(new ByteArrayInputStream("123".getBytes("utf-8"))));
    assertEquals(new String((char[])parms.getValue(1)), "123");

    NClob nlob = new com.sonicbase.query.impl.NClob("123");
    sp.setNClob(1, nlob);
    assertEquals(new String((char[])parms.getValue(1)), "123");

    chars = new char["123".length()];
    "123".getChars(0, "123".length(), chars, 0);

    sp.setClob(1, new StringReader(new String(chars)), 3);
    assertEquals(new String((char[])parms.getValue(1)), "123");

    chars = new char["123".length()];
    "123".getChars(0, "123".length(), chars, 0);

    sp.setClob(1, new StringReader(new String(chars)));
    assertEquals(new String((char[])parms.getValue(1)), "123");

    sp.setBlob(1, new ByteArrayInputStream("123".getBytes("utf-8")), 3);
    assertEquals(parms.getValue(1), "123".getBytes("utf-8"));

    sp.setBlob(1, new ByteArrayInputStream("123".getBytes("utf-8")));
    assertEquals(parms.getValue(1), "123".getBytes("utf-8"));

    chars = new char["123".length()];
    "123".getChars(0, "123".length(), chars, 0);

    sp.setNClob(1, new StringReader(new String(chars)), 3);
    assertEquals(new String((char[])parms.getValue(1)), "123");

    chars = new char["123".length()];
    "123".getChars(0, "123".length(), chars, 0);

    sp.setNClob(1, new StringReader(new String(chars)));
    assertEquals(new String((char[])parms.getValue(1)), "123");

    sp.setAsciiStream(1, new ByteArrayInputStream("123".getBytes("utf-8")), 3);
    assertEquals(new String((char[])parms.getValue(1)), "123");

    sp.setBinaryStream(1, new ByteArrayInputStream("123".getBytes("utf-8")));
    assertEquals(parms.getValue(1), "123".getBytes("utf-8"));

    chars = new char["123".length()];
    "123".getChars(0, "123".length(), chars, 0);

    sp.setNCharacterStream(1, new StringReader(new String(chars)), 3);
    assertEquals(new String((char[])parms.getValue(1)), "123");

    chars = new char["123".length()];
    "123".getChars(0, "123".length(), chars, 0);

    sp.setCharacterStream(1, new StringReader(new String(chars)), 3);
    assertEquals(new String((char[])parms.getValue(1)), "123");

    chars = new char["123".length()];
    "123".getChars(0, "123".length(), chars, 0);

    sp.setCharacterStream(1, new StringReader(new String(chars)), 3L);
    assertEquals(new String((char[])parms.getValue(1)), "123");

  }

  @Test
  public void testExceptions() throws SQLException, IOException {
    ConnectionProxy cp = mock(ConnectionProxy.class);
    DatabaseClient client = mock(DatabaseClient.class);
    ParameterHandler parms = mock(ParameterHandler.class);
    StatementProxy sp = new StatementProxy(cp, client, "select * from person");
    sp.setParms(parms);

    doThrow(new RuntimeException()).when(parms).setNull(anyInt(), anyByte());
    try {
      sp.setNull(1, Types.BIGINT);
      fail();
    }
    catch (SQLException e) {
    }

    doThrow(new RuntimeException()).when(parms).setBoolean(anyInt(), anyBoolean());
    try {
      sp.setBoolean(1, true);
      fail();
    }
    catch (SQLException e) {
    }

    doThrow(new RuntimeException()).when(parms).setByte(anyInt(), anyByte());
    try {
      sp.setByte(1, (byte)123);
      fail();
    }
    catch (SQLException e) {
    }

    doThrow(new RuntimeException()).when(parms).setShort(anyInt(), anyShort());
    try {
      sp.setShort(1, (short)123);
      fail();
    }
    catch (SQLException e) {
    }

    doThrow(new RuntimeException()).when(parms).setInt(anyInt(), anyInt());
    try {
      sp.setInt(1, (int)123);
      fail();
    }
    catch (SQLException e) {
    }

    doThrow(new RuntimeException()).when(parms).setLong(anyInt(), anyLong());
    try {
      sp.setLong(1, 123L);
      fail();
    }
    catch (SQLException e) {
    }

    doThrow(new RuntimeException()).when(parms).setFloat(anyInt(), anyFloat());
    try {
      sp.setFloat(1, 123f);
      fail();
    }
    catch (SQLException e) {
    }

    doThrow(new RuntimeException()).when(parms).setDouble(anyInt(), anyDouble());
    try {
      sp.setDouble(1, 123d);
      fail();
    }
    catch (SQLException e) {
    }

    doThrow(new RuntimeException()).when(parms).setBigDecimal(anyInt(), (BigDecimal) anyObject());
    try {
      sp.setBigDecimal(1, new BigDecimal(123));
      fail();
    }
    catch (SQLException e) {
    }

    doThrow(new RuntimeException()).when(parms).setString(anyInt(), (String) anyObject());
    try {
      sp.setString(1, "123");
      fail();
    }
    catch (SQLException e) {
    }

    doThrow(new RuntimeException()).when(parms).setBytes(anyInt(), (byte[]) anyObject());
    try {
      sp.setBytes(1, "123".getBytes("utf-8"));
      fail();
    }
    catch (SQLException e) {
    }

    doThrow(new RuntimeException()).when(parms).setDate(anyInt(), (Date) anyObject());
    try {
      sp.setDate(1, new Date(123));
      fail();
    }
    catch (SQLException e) {
    }

    doThrow(new RuntimeException()).when(parms).setTime(anyInt(), (Time) anyObject());
    try {
      sp.setTime(1, new Time(123));
      fail();
    }
    catch (SQLException e) {
    }

    doThrow(new RuntimeException()).when(parms).setTimestamp(anyInt(), (Timestamp) anyObject());
    try {
      sp.setTimestamp(1, new Timestamp(123));
      fail();
    }
    catch (SQLException e) {
    }

    doThrow(new RuntimeException()).when(parms).setAsciiStream(anyInt(), (InputStream) anyObject());
    try {
      sp.setAsciiStream(1, new ByteArrayInputStream("123".getBytes("utf-8")));
      fail();
    }
    catch (SQLException e) {
    }

    doThrow(new RuntimeException()).when(parms).setUnicodeStream(anyInt(), (InputStream) anyObject(), anyInt());
    try {
      sp.setUnicodeStream(1, new ByteArrayInputStream("123".getBytes("utf-8")), 3);
      fail();
    }
    catch (SQLException e) {
    }

    doThrow(new RuntimeException()).when(parms).setBinaryStream(anyInt(), (InputStream) anyObject(), anyInt());
    try {
      sp.setBinaryStream(1, new ByteArrayInputStream("123".getBytes("utf-8")), 3);
      fail();
    }
    catch (SQLException e) {
    }

    doThrow(new RuntimeException()).when(parms).setCharacterStream(anyInt(), (Reader) anyObject());
    try {
      sp.setCharacterStream(1, new InputStreamReader(new ByteArrayInputStream("123".getBytes("utf-8"))));
      fail();
    }
    catch (SQLException e) {
    }

    doThrow(new RuntimeException()).when(parms).setBlob(anyInt(), (Blob) anyObject());
    try {
      Blob blob = new com.sonicbase.query.impl.Blob("123".getBytes("utf-8"));
      sp.setBlob(1, blob);
      fail();
    }
    catch (SQLException e) {
    }

    doThrow(new RuntimeException()).when(parms).setClob(anyInt(), (Clob) anyObject());
    try {
      Clob clob = new com.sonicbase.query.impl.Clob("123");
      sp.setClob(1, clob);
      fail();
    }
    catch (SQLException e) {
    }

    doThrow(new RuntimeException()).when(parms).setNString(anyInt(), (String) anyObject());
    try {
      sp.setNString(1, "123");
      fail();
    }
    catch (SQLException e) {
    }

    doThrow(new RuntimeException()).when(parms).setNCharacterStream(anyInt(), (Reader) anyObject());
    try {
      sp.setNCharacterStream(1, new InputStreamReader(new ByteArrayInputStream("123".getBytes("utf-8"))));
      fail();
    }
    catch (SQLException e) {
    }

    doThrow(new RuntimeException()).when(parms).setNClob(anyInt(), (NClob) anyObject());
    try {
      NClob clob = new com.sonicbase.query.impl.NClob("123");
      sp.setNClob(1, clob);
      fail();
    }
    catch (SQLException e) {
    }

    doThrow(new RuntimeException()).when(parms).setClob(anyInt(), (Reader) anyObject());
    try {
      sp.setClob(1, new InputStreamReader(new ByteArrayInputStream("123".getBytes("utf-8"))));
      fail();
    }
    catch (SQLException e) {
    }

    doThrow(new RuntimeException()).when(parms).setClob(anyInt(), (Reader) anyObject(), anyInt());
    try {
      sp.setClob(1, new InputStreamReader(new ByteArrayInputStream("123".getBytes("utf-8"))), 3);
      fail();
    }
    catch (SQLException e) {
    }

    doThrow(new RuntimeException()).when(parms).setBlob(anyInt(), (InputStream) anyObject(), anyInt());
    try {
      sp.setBlob(1, new ByteArrayInputStream("123".getBytes("utf-8")), 3);
      fail();
    }
    catch (SQLException e) {
    }

    doThrow(new RuntimeException()).when(parms).setNClob(anyInt(), (Reader) anyObject(), anyInt());
    try {
      sp.setNClob(1, new InputStreamReader(new ByteArrayInputStream("123".getBytes("utf-8"))), 3);
      fail();
    }
    catch (SQLException e) {
    }

    doThrow(new RuntimeException()).when(parms).setNClob(anyInt(), (Reader) anyObject());
    try {
      sp.setNClob(1, new InputStreamReader(new ByteArrayInputStream("123".getBytes("utf-8"))));
      fail();
    }
    catch (SQLException e) {
    }

    doThrow(new RuntimeException()).when(parms).setAsciiStream(anyInt(), (InputStream) anyObject(), anyInt());
    try {
      sp.setAsciiStream(1, new ByteArrayInputStream("123".getBytes("utf-8")), 3);
      fail();
    }
    catch (SQLException e) {
    }

    doThrow(new RuntimeException()).when(parms).setBinaryStream(anyInt(), (InputStream) anyObject());
    try {
      sp.setBinaryStream(1, new ByteArrayInputStream("123".getBytes("utf-8")));
      fail();
    }
    catch (SQLException e) {
    }

    doThrow(new RuntimeException()).when(parms).setNCharacterStream(anyInt(), (Reader) anyObject(), anyInt());
    try {
      sp.setNCharacterStream(1, new InputStreamReader(new ByteArrayInputStream("123".getBytes("utf-8"))), 3);
      fail();
    }
    catch (SQLException e) {
    }

    doThrow(new RuntimeException()).when(parms).setCharacterStream(anyInt(), (Reader) anyObject(), anyInt());
    try {
      sp.setCharacterStream(1, new InputStreamReader(new ByteArrayInputStream("123".getBytes("utf-8"))), 3);
      fail();
    }
    catch (SQLException e) {
    }

  }
}
