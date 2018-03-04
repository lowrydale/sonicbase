/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.procedure;

import java.math.BigDecimal;

public interface Record {

  String getDatabase();

  String getTableName();

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
}
