package com.sonicbase.query;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

public interface ResultSet {

  boolean next() throws DatabaseException;

  String getString(String columnLabel);

  Boolean getBoolean(String columnLabel);

  Byte getByte(String columnLabel);

  Short getShort(String columnLabel);

  Integer getInt(String columnLabel);

  Long getLong(String columnLabel);

  Float getFloat(String columnLabel);

  Double getDouble(String columnLabel);

  BigDecimal getBigDecimal(String columnLabel, int scale);

  byte[] getBytes(String columnLabel);

  Date getDate(String columnLabel);

  Time getTime(String columnLabel);

  Timestamp getTimestamp(String columnLabel);

  InputStream getUnicodeStream(String columnLabel);

  InputStream getBinaryStream(String columnLabel);

  Reader getCharacterStream(String columnLabel);

  BigDecimal getBigDecimal(String columnLabel);

  Integer getInt(int columnIndex);

  Long getLong(int columnIndex);

  BigDecimal getBigDecimal(int columnIndex);

  Timestamp getTimestamp(int columnIndex);

  Time getTime(int columnIndex);

  Date getDate(int columnIndex);

  byte[] getBytes(int columnIndex);

  BigDecimal getBigDecimal(int columnIndex, int scale);

  Double getDouble(int columnIndex);

  Float getFloat(int columnIndex);

  Short getShort(int columnIndex);

  Byte getByte(int columnIndex);

  Boolean getBoolean(int columnIndex);

  InputStream getBinaryStream(int columnIndex);

  void setIsCount();

  long getUniqueRecordCount();
}
