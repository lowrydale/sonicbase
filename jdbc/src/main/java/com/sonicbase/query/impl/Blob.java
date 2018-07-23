package com.sonicbase.query.impl;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;

/**
 * Responsible for
 */
public class Blob implements java.sql.Blob {
  public static final String OUT_OF_BOUNDS_STR = "out of bounds";
  public static final String NOT_SUPPORTED_STR = "not supported";
  private byte[] data;

  public Blob() {

  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="EI_EXPOSE_REP2", justification="copying the passed in data is too slow")
  @SuppressWarnings("PMD.ArrayIsStoredDirectly") //copying the passed in data is too slow
  public Blob(byte[] data) {
    this.data = data;
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="EI_EXPOSE_REP", justification="copying the returned data is too slow")
  public byte[] getData() {
    return data;
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="EI_EXPOSE_REP2", justification="copying the passed in data is too slow")
  @SuppressWarnings("PMD.ArrayIsStoredDirectly") //copying the passed in data is too slow
  public void setData(byte[] data) {
    this.data = data;
  }

  @Override
  public long length() throws SQLException {
    if (data == null) {
      return 0;
    }
    return data.length;
  }

  @Override
  public byte[] getBytes(long pos, int length) throws SQLException {
    if (data == null) {
      throw new SQLException(OUT_OF_BOUNDS_STR);
    }
    if (pos + length > data.length) {
      throw new SQLException(OUT_OF_BOUNDS_STR);
    }
    byte[] ret = new byte[length - (int)pos];
    System.arraycopy(data, (int)pos, ret, 0, length);
    return ret;
  }

  @Override
  public InputStream getBinaryStream() throws SQLException {
    if (data == null) {
      throw new SQLException("null blob");
    }
    return new ByteArrayInputStream(data);
  }

  @Override
  public long position(byte[] pattern, long start) throws SQLException {
    throw new SQLException(NOT_SUPPORTED_STR);
  }

  @Override
  public long position(java.sql.Blob pattern, long start) throws SQLException {
    throw new SQLException(NOT_SUPPORTED_STR);
  }

  @Override
  public int setBytes(long pos, byte[] bytes) throws SQLException {
    return setBytes(pos, bytes, 0, bytes.length);
  }

  @Override
  public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
    if (data == null) {
      if (pos == 0) {
        byte[] newBytes = new byte[len];
        System.arraycopy(bytes, offset, newBytes, 0, len);
        data = newBytes;
        return len;
      }
      else {
        throw new SQLException("null blob");
      }
    }
    if (pos + len > data.length) {
      byte[] newBytes = new byte[(int)pos + len];
      System.arraycopy(data, 0, newBytes, 0, data.length);
      System.arraycopy(bytes, offset, newBytes, (int)pos, len);
      return len;
    }
    System.arraycopy(bytes, offset, data, (int)pos, len);
    return len;
  }

  @Override
  public OutputStream setBinaryStream(long pos) throws SQLException {
    throw new SQLException(NOT_SUPPORTED_STR);
  }

  @Override
  public void truncate(long len) throws SQLException {
    if (data == null || len > data.length) {
      throw new SQLException(OUT_OF_BOUNDS_STR);
    }
    byte[] newBytes = new byte[(int)len];
    System.arraycopy(data, 0, newBytes, 0, (int)len);
    data = newBytes;
  }

  @Override
  public void free() throws SQLException {
    data = null;
  }

  @Override
  public InputStream getBinaryStream(long pos, long length) throws SQLException {
    return new ByteArrayInputStream(data, (int)pos, (int)length);
  }
}
