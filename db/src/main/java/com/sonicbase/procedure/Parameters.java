/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.procedure;

import java.math.BigDecimal;

public interface Parameters {

  String getString(int offset);

  Long getLong(int offset);

  boolean getBoolean(int offset);

  byte getByte(int offset);

  short getShort(int offset);

  int getInt(int offset);

  float getFloat(int offset);

  double getDouble(int offset);

  BigDecimal getBigDecimal(int offset, int scale);

  java.sql.Date getDate(int offset);

  java.sql.Time getTime(int offset);

  java.sql.Timestamp getTimestamp(int offset);
}
