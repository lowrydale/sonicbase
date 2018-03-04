/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.procedure;

import com.sonicbase.schema.DataType;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

public class ParametersImpl implements Parameters {
  private Object[] parms;

  public ParametersImpl(Object[] parmsArray) {
    this.parms = parmsArray;
  }

  @Override
  public String getString(int offset) {
    return (String) DataType.getStringConverter().convert(parms[offset - 1]);
  }

  @Override
  public Long getLong(int offset) {
    return (Long) DataType.getLongConverter().convert(parms[offset - 1]);
  }

  @Override
  public boolean getBoolean(int offset) {
    return (Boolean) DataType.getBooleanConverter().convert(parms[offset - 1]);
  }

  @Override
  public byte getByte(int offset) {
    return (Byte) DataType.getByteConverter().convert(parms[offset - 1]);
  }

  @Override
  public short getShort(int offset) {
    return (Short) DataType.getShortConverter().convert(parms[offset - 1]);
  }

  @Override
  public int getInt(int offset) {
    return (Integer) DataType.getIntConverter().convert(parms[offset - 1]);
  }

  @Override
  public float getFloat(int offset) {
    return (Float) DataType.getFloatConverter().convert(parms[offset - 1]);
  }

  @Override
  public double getDouble(int offset) {
    return (Double) DataType.getDoubleConverter().convert(parms[offset - 1]);
  }

  @Override
  public BigDecimal getBigDecimal(int offset, int scale) {
    return (BigDecimal) DataType.getBigDecimalConverter().convert(parms[offset - 1]);
  }

  @Override
  public Date getDate(int offset) {
    return (Date) DataType.getDateConverter().convert(parms[offset - 1]);
  }

  @Override
  public Time getTime(int offset) {
    return (Time) DataType.getTimeConverter().convert(parms[offset - 1]);
  }

  @Override
  public Timestamp getTimestamp(int offset) {
    return (Timestamp) DataType.getTimestampConverter().convert(parms[offset - 1]);
  }
}
