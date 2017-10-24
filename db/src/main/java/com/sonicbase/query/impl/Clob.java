package com.sonicbase.query.impl;

import org.apache.commons.io.IOUtils;

import java.io.*;
import java.sql.SQLException;

/**
 * Responsible for
 */
public class Clob implements java.sql.Clob {
  private String data;

  public Clob() {

  }

  public Clob(String str) {
    this.data = str;
  }

  @Override
  public long length() throws SQLException {
    if (data == null) {
      return 0;
    }
    return data.length();
  }

  @Override
  public String getSubString(long pos, int length) throws SQLException {
    if (data == null || pos + length > data.length()) {
      throw new SQLException("out of bounds");
    }
    return data.substring((int)pos, length);
  }

  @Override
  public Reader getCharacterStream() throws SQLException {
    if (data == null) {
      throw new SQLException("null data");
    }
    return new StringReader(data);
  }

  @Override
  public InputStream getAsciiStream() throws SQLException {
    if (data == null) {
      throw new SQLException("null data");
    }
    try {
      return new ByteArrayInputStream(data.getBytes("utf-8"));
    }
    catch (UnsupportedEncodingException e) {
      throw new SQLException(e);
    }
  }

  @Override
  public long position(String searchstr, long start) throws SQLException {
    if (data == null) {
      return -1;
    }
    return data.indexOf(searchstr, (int)start);
  }

  @Override
  public long position(java.sql.Clob searchstr, long start) throws SQLException {
    if (data == null) {
      return -1;
    }
    try {
      String str = IOUtils.toString(searchstr.getCharacterStream());
      return data.indexOf(str, (int)start);
    }
    catch (IOException e) {
      throw new SQLException(e);
    }
  }

  @Override
  public int setString(long pos, String str) throws SQLException {
    return setString(pos, str, 0, str.length());
  }

  @Override
  public int setString(long pos, String str, int offset, int len) throws SQLException {
    if (data == null) {
      if (pos != 0) {
        throw new SQLException("null data");
      }
      data = str.substring(offset, len);
      return len;
    }
    String str0 = data.substring(0, (int)pos);
    str0 += str.substring(offset, len);
    if (data.length() > pos + len) {
      str0 += data.substring((int)pos + (int)len);
    }
    data = str0;
    return len;
  }

  @Override
  public OutputStream setAsciiStream(long pos) throws SQLException {
    throw new SQLException("not supported");
  }

  @Override
  public Writer setCharacterStream(long pos) throws SQLException {
    throw new SQLException("not supported");
  }

  @Override
  public void truncate(long len) throws SQLException {
    if (data == null) {
      return;
    }
    data = data.substring(0, (int)len);
  }

  @Override
  public void free() throws SQLException {
    data = null;
  }

  @Override
  public Reader getCharacterStream(long pos, long length) throws SQLException {
    if (data == null) {
      throw new SQLException("null data");
    }
    String ret = data.substring((int)pos, (int)length);
    return new StringReader(ret);
  }

  public String getString() {
    return data;
  }
}
