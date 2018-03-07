/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.procedure;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

public interface Record {

  String getDatabase();

  String getTableName();

  boolean isAdding();

  boolean isDeleting();

  int getViewVersion();

  String getString(String columnLabel);

  Long getLong(String columnLabel);

  boolean getBoolean(String columnLabel);

  byte getByte(String columnLabel);

  short getShort(String columnLabel);

  int getInt(String columnLabel);

  float getFloat(String columnLabel);

  double getDouble(String columnLabel);

  BigDecimal getBigDecimal(String columnLabel, int scale);

  byte[] getBytes(String columnLabel);

  java.sql.Date getDate(String columnLabel);

  java.sql.Time getTime(String columnLabel);

  java.sql.Timestamp getTimestamp(String columnLabel);

  java.io.InputStream getBinaryStream(String columnLabel);

  void setString(String columnLabel, String value);

  void setLong(String columnLabel, long value);

  void setBoolean(String columnLabel, boolean value);

  void setByte(String columnLabel, byte value);

  void setShort(String columnLabel, short value);

  void setInt(String columnLabel, int value);

  void setFloat(String columnLabel, float value);

  void setDouble(String columnLabel, double value);

  void setBigDecimal(String columnLabel, BigDecimal value);

  void setBytes(String columnLabel, byte[] value);

  void setDate(String columnLabel, Date value);

  void setTime(String columnLabel, Time value);

  void setTimestamp(String columnLabel, Timestamp value);

  void setBinaryStream(String columnLabel, InputStream value);
}

