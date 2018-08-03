package com.sonicbase.query.impl;

import com.sonicbase.schema.DataType;

import java.io.*;
import java.math.BigDecimal;

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class Counter {
  private String tableName;
  private String columnName;
  private int column;
  private Long longCount = null;
  private Double doubleCount = null;
  private Long minLong = Long.MAX_VALUE;
  private Long maxLong = Long.MIN_VALUE;
  private Double minDouble = Double.MAX_VALUE;
  private Double maxDouble = Double.MIN_VALUE;
  private long count;
  private DataType.Type dataType;

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  public void setColumn(int column) {
    this.column = column;
  }

  public int getColumnOffset() {
    return column;
  }

  public Long getLongCount() {
    return longCount;
  }

  public void add(Object[] fields) {
    if (column == 0) {
      count++;
      return;
    }

    if (fields[column] == null) {
      return;
    }

    switch (dataType) {
      case INTEGER:
        addLong((long)(int)((Integer)fields[column]));
        break;
      case BIGINT:
        addLong((Long)fields[column]);
        break;
      case SMALLINT:
        addLong((long)(short)((Short)fields[column]));
        break;
      case TINYINT:
        addLong((long)(byte)((Byte)fields[column]));
        break;
      case FLOAT:
        addDouble((double)(float)((Float)fields[column]));
        break;
      case DOUBLE:
        addDouble((Double)fields[column]);
        break;
      case NUMERIC:
        addDouble(((BigDecimal)fields[column]).doubleValue());
        break;
      case DECIMAL:
        addDouble(((BigDecimal)fields[column]).doubleValue());
        break;
      case REAL:
        addDouble((double)(float)((Float)fields[column]));
        break;
    }
  }

  public void addLong(Long toAdd) {
    if (longCount == null) {
      longCount = 0L;
    }
    longCount += toAdd;
    count++;

    minLong = Math.min(minLong, toAdd);
    maxLong = Math.max(maxLong, toAdd);
  }

  public Object getDoubleCount() {
    return doubleCount;
  }

  public void addDouble(Double toAdd) {
    if (doubleCount == null) {
      doubleCount = 0d;
    }
    doubleCount += toAdd;
    count++;

    minDouble = Math.min(minDouble, toAdd);
    maxDouble = Math.max(maxDouble, toAdd);
  }

  public String getColumnName() {
    return columnName;
  }

  public Long getMinLong() {
    return minLong;
  }

  public Double getMinDouble() {
    return minDouble;
  }

  public Long getMaxLong() {
    return maxLong;
  }

  public Double getMaxDouble() {
    return maxDouble;
  }

  public Double getAvgLong() {
    if (count == 0 || longCount == null) {
      return 0d;
    }
    return longCount / (double) count;
  }

  public Double getAvgDouble() {
    if (count == 0 || doubleCount == null) {
      return 0d;
    }
    return doubleCount / (double)count;
  }

  public void setDataType(DataType.Type dataType) {
    this.dataType = dataType;
  }

  public void setDestTypeToLong() {
    this.longCount = 0L;
  }

  public void setDestTypeToDouble() {
    this.doubleCount = 0d;
  }

  public boolean isDestTypeLong() {
    return longCount != null;
  }

  public boolean isDestTypeDouble() {
    return doubleCount != null;
  }

  public byte[] serialize() throws IOException {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytesOut);
    out.writeUTF(tableName);
    out.writeUTF(columnName);
    out.writeInt(column);
    if (longCount == null) {
      out.writeBoolean(false);
    }
    else {
      out.writeBoolean(true);
      out.writeLong(longCount);
    }
    if (doubleCount == null) {
      out.writeBoolean(false);
    }
    else {
      out.writeBoolean(true);
      out.writeDouble(doubleCount);
    }
    out.writeLong(minLong);
    out.writeLong(maxLong);
    out.writeDouble(minDouble);
    out.writeDouble(maxDouble);
    out.writeLong(count);
    out.writeInt(dataType.getValue());
    out.close();

    return bytesOut.toByteArray();
  }

  public void deserialize(byte[] bytes) throws IOException {
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
    deserialize(in);
  }

  public void deserialize(DataInputStream in) throws IOException {
    tableName = in.readUTF();
    columnName = in.readUTF();
    column = in.readInt();
    if (in.readBoolean()) {
      longCount = in.readLong();
    }
    if (in.readBoolean()) {
      doubleCount = in.readDouble();
    }
    minLong = in.readLong();
    maxLong = in.readLong();
    minDouble = in.readDouble();
    maxDouble = in.readDouble();
    count = in.readLong();
    dataType = DataType.Type.valueOf(in.readInt());
  }

  public DataType.Type getDataType() {
    return dataType;
  }

  public String getTableName() {
    return tableName;
  }

  public long getCount() {
    return count;
  }

  public void setMaxLong(Long maxLong) {
    this.maxLong = maxLong;
  }

  public void setMinLong(Long minLong) {
    this.minLong = minLong;
  }

  public void setMaxDouble(Double maxDouble) {
    this.maxDouble = maxDouble;
  }

  public void setMinDouble(double minDouble) {
    this.minDouble = minDouble;
  }

  public void setCount(Long count) {
    this.count = count;
  }
}
