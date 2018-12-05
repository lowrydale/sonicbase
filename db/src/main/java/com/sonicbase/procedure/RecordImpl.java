package com.sonicbase.procedure;

import com.sonicbase.common.ComArray;
import com.sonicbase.common.ComObject;
import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.DataType;
import com.sonicbase.schema.TableSchema;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class RecordImpl implements Record {
  private String dbName;
  private short serializationNumber;
  private DatabaseCommon common;
  private TableSchema tableSchema;
  private com.sonicbase.common.Record record;
  private Map<String, Object> fieldMap = new HashMap<>();
  private int viewVersion;
  private boolean isDeleting;
  private boolean isAdding;

  public RecordImpl(String dbName, DatabaseCommon common, short serializationNumber, String database,
                    TableSchema tableSchema, com.sonicbase.common.Record fields) {
    this.dbName = dbName;
    this.common = common;
    this.dbName = database;
    this.tableSchema = tableSchema;
    this.record = fields;
    this.serializationNumber = serializationNumber;
  }

  public RecordImpl() {
  }

  public RecordImpl(DatabaseCommon common, ComObject cobj) {
    byte[] bytes = cobj.getByteArray(ComObject.Tag.RECORD_BYTES);
    if (bytes != null) {
      dbName = cobj.getString(ComObject.Tag.DB_NAME);
      tableSchema = common.getTables(dbName).get(cobj.getString(ComObject.Tag.TABLE_NAME));
      record = new com.sonicbase.common.Record(dbName, common, bytes);
      this.common = common;
    }
    else {
      ComArray array = cobj.getArray(ComObject.Tag.FIELDS);
      for (int i = 0; i < array.getArray().size(); i++) {
        ComObject parm = (ComObject) array.getArray().get(i);
        String fieldName = parm.getString(ComObject.Tag.FIELD_NAME);
        com.sonicbase.common.ComObject.Map fields = parm.getMap();
        for (int j = 0; j < fields.getPos(); j++) {
          int tag = fields.getTags()[j];
          Object value = fields.getValues()[j];
          if (tag == ComObject.Tag.STRING_VALUE.tag) {
            this.fieldMap.put(fieldName, parm.getString(ComObject.Tag.STRING_VALUE));
          }
          else if (tag == ComObject.Tag.LONG_VALUE.tag) {
            fieldMap.put(fieldName, parm.getLong(ComObject.Tag.LONG_VALUE));
          }
          else if (tag == ComObject.Tag.INT_VALUE.tag) {
            fieldMap.put(fieldName, parm.getInt(ComObject.Tag.INT_VALUE));
          }
          else if (tag == ComObject.Tag.BOOLEAN_VALUE.tag) {
            fieldMap.put(fieldName, parm.getBoolean(ComObject.Tag.BOOLEAN_VALUE));
          }
          else if (tag == ComObject.Tag.TIME_VALUE.tag) {
            fieldMap.put(fieldName, parm.getTime(ComObject.Tag.TIME_VALUE));
          }
          else if (tag == ComObject.Tag.DATE_VALUE.tag) {
            fieldMap.put(fieldName, parm.getDate(ComObject.Tag.DATE_VALUE));
          }
          else if (tag == ComObject.Tag.TIMESTAMP_VALUE.tag) {
            fieldMap.put(fieldName, parm.getTimestamp(ComObject.Tag.TIMESTAMP_VALUE));
          }
          else if (tag == ComObject.Tag.FLOAT_VALUE.tag) {
            fieldMap.put(fieldName, parm.getFloat(ComObject.Tag.FLOAT_VALUE));
          }
          else if (tag == ComObject.Tag.DOUBLE_VALUE.tag) {
            fieldMap.put(fieldName, parm.getDouble(ComObject.Tag.DOUBLE_VALUE));
          }
          else if (tag == ComObject.Tag.BIG_DECIMAL_VALUE.tag) {
            fieldMap.put(fieldName, parm.getBigDecimal(ComObject.Tag.BIG_DECIMAL_VALUE));
          }
          else if (tag == ComObject.Tag.BYTE_VALUE.tag) {
            fieldMap.put(fieldName, parm.getByte(ComObject.Tag.BYTE_VALUE));
          }
          else if (tag == ComObject.Tag.SHORT_VALUE.tag) {
            fieldMap.put(fieldName, parm.getShort(ComObject.Tag.SHORT_VALUE));
          }
          else if (tag == ComObject.Tag.BYTE_ARRAY_VALUE.tag) {
            fieldMap.put(fieldName, parm.getByteArray(ComObject.Tag.BYTE_ARRAY_VALUE));
          }
        }
      }
    }
  }

  public ComObject serialize() {
    ComObject ret = new ComObject(3);
    if (tableSchema != null) {
      byte[] bytes = record.serialize(common, serializationNumber);
      ret.put(ComObject.Tag.RECORD_BYTES, bytes);
      ret.put(ComObject.Tag.TABLE_NAME, tableSchema.getName());
      ret.put(ComObject.Tag.DB_NAME, dbName);
    }
    else {
      ComArray array = ret.putArray(ComObject.Tag.FIELDS, ComObject.Type.OBJECT_TYPE, fieldMap.size());
      for (Map.Entry<String, Object> field : fieldMap.entrySet()) {
        ComObject parm = new ComObject(2);
        parm.put(ComObject.Tag.FIELD_NAME, field.getKey());
        Object value = field.getValue();
        if (value instanceof String) {
          parm.put(ComObject.Tag.STRING_VALUE, (String)value);
        }
        else if (value instanceof Long) {
          parm.put(ComObject.Tag.LONG_VALUE, (Long)value);
        }
        else if (value instanceof Integer) {
          parm.put(ComObject.Tag.INT_VALUE, (Integer)value);
        }
        else if (value instanceof Boolean) {
          parm.put(ComObject.Tag.BOOLEAN_VALUE, (Boolean)value);
        }
        else if (value instanceof Time) {
          parm.put(ComObject.Tag.TIME_VALUE, (Time)value);
        }
        else if (value instanceof Date) {
          parm.put(ComObject.Tag.DATE_VALUE, (Date)value);
        }
        else if (value instanceof Timestamp) {
          parm.put(ComObject.Tag.TIMESTAMP_VALUE, (Timestamp)value);
        }
        else if (value instanceof Float) {
          parm.put(ComObject.Tag.FLOAT_VALUE, (Float)value);
        }
        else if (value instanceof Double) {
          parm.put(ComObject.Tag.DOUBLE_VALUE, (Double)value);
        }
        else if (value instanceof BigDecimal) {
          parm.put(ComObject.Tag.BIG_DECIMAL_VALUE, (BigDecimal)value);
        }
        else if (value instanceof Byte) {
          parm.put(ComObject.Tag.BYTE_VALUE, (byte)value);
        }
        else if (value instanceof Short) {
          parm.put(ComObject.Tag.SHORT_VALUE, (short)value);
        }
        else if (value instanceof byte[]) {
          parm.put(ComObject.Tag.BYTE_ARRAY_VALUE, (byte[])value);
        }
        array.add(parm);
      }
    }
    return ret;
  }

  public String getDatabase() {
    return dbName;
  }

  public String getTableName() {
    return tableSchema.getName();
  }

  @Override
  public boolean isAdding() {
    return isAdding;
  }

  @Override
  public boolean isDeleting() {
    return isDeleting;
  }

  @Override
  public int getViewVersion() {
    return viewVersion;
  }

  public String getString(String columnLabel) {
    if (tableSchema == null) {
      return (String) DataType.getStringConverter().convert(fieldMap.get(columnLabel));
    }
    else {
      Object ret = record.getFields()[tableSchema.getFieldOffset(columnLabel)];
      return (String) DataType.getStringConverter().convert(ret);
    }
  }

  public Long getLong(String columnLabel) {
    if (tableSchema == null) {
      return (Long) DataType.getLongConverter().convert(fieldMap.get(columnLabel));
    }
    else {
      Object ret = record.getFields()[tableSchema.getFieldOffset(columnLabel)];
      return (Long) DataType.getLongConverter().convert(ret);
    }
  }

  public boolean getBoolean(String columnLabel) {
    if (tableSchema == null) {
      return (Boolean) DataType.getBooleanConverter().convert(fieldMap.get(columnLabel));
    }
    else {
      Object ret = record.getFields()[tableSchema.getFieldOffset(columnLabel)];
      return (Boolean) DataType.getBooleanConverter().convert(ret);
    }
  }

  public byte getByte(String columnLabel) {
    if (tableSchema == null) {
      return (Byte) DataType.getByteConverter().convert(fieldMap.get(columnLabel));
    }
    else {
      Object ret = record.getFields()[tableSchema.getFieldOffset(columnLabel)];
      return (Byte) DataType.getByteConverter().convert(ret);
    }
  }

  public short getShort(String columnLabel) {
    if (tableSchema == null) {
      return (Short) DataType.getShortConverter().convert(fieldMap.get(columnLabel));
    }
    else {
      Object ret = record.getFields()[tableSchema.getFieldOffset(columnLabel)];
      return (Short) DataType.getShortConverter().convert(ret);
    }
  }

  public int getInt(String columnLabel) {
    if (tableSchema == null) {
      return (Integer) DataType.getIntConverter().convert(fieldMap.get(columnLabel));
    }
    else {
      Object ret = record.getFields()[tableSchema.getFieldOffset(columnLabel)];
      return (Integer) DataType.getIntConverter().convert(ret);
    }
  }

  public float getFloat(String columnLabel) {
    if (tableSchema == null) {
      return (Float) DataType.getFloatConverter().convert(fieldMap.get(columnLabel));
    }
    else {
      Object ret = record.getFields()[tableSchema.getFieldOffset(columnLabel)];
      return (Float) DataType.getFloatConverter().convert(ret);
    }
  }

  public double getDouble(String columnLabel) {
    if (tableSchema == null) {
      return (Double) DataType.getDoubleConverter().convert(fieldMap.get(columnLabel));
    }
    else {
      Object ret = record.getFields()[tableSchema.getFieldOffset(columnLabel)];
      return (Double) DataType.getDoubleConverter().convert(ret);
    }
  }

  public BigDecimal getBigDecimal(String columnLabel, int scale) {
    if (tableSchema == null) {
      return (BigDecimal) DataType.getBigDecimalConverter().convert(fieldMap.get(columnLabel));
    }
    else {
      Object ret = record.getFields()[tableSchema.getFieldOffset(columnLabel)];
      return (BigDecimal) DataType.getBigDecimalConverter().convert(ret);
    }
  }

  public byte[] getBytes(String columnLabel) {
    if (tableSchema == null) {
      return (byte[]) DataType.getByteArrayConverter().convert(fieldMap.get(columnLabel));
    }
    else {
      Object ret = record.getFields()[tableSchema.getFieldOffset(columnLabel)];
      return (byte[]) DataType.getByteArrayConverter().convert(ret);
    }
  }

  public Date getDate(String columnLabel) {
    if (tableSchema == null) {
      return (Date) DataType.getDateConverter().convert(fieldMap.get(columnLabel));
    }
    else {
      Object ret = record.getFields()[tableSchema.getFieldOffset(columnLabel)];
      return (Date) DataType.getDateConverter().convert(ret);
    }
  }

  public Time getTime(String columnLabel) {
    if (tableSchema == null) {
      return (Time) DataType.getTimeConverter().convert(fieldMap.get(columnLabel));
    }
    else {
      Object ret = record.getFields()[tableSchema.getFieldOffset(columnLabel)];
      return (Time) DataType.getTimeConverter().convert(ret);
    }
  }

  public Timestamp getTimestamp(String columnLabel) {
    if (tableSchema == null) {
      return (Timestamp) DataType.getTimestampConverter().convert(fieldMap.get(columnLabel));
    }
    else {
      Object ret = record.getFields()[tableSchema.getFieldOffset(columnLabel)];
      return (Timestamp) DataType.getTimestampConverter().convert(ret);
    }
  }

  public InputStream getBinaryStream(String columnLabel) {
    if (tableSchema == null) {
      return new ByteArrayInputStream((byte[])fieldMap.get(columnLabel));
    }
    else {
      Object ret = record.getFields()[tableSchema.getFieldOffset(columnLabel)];
      byte[] bytes = (byte[]) DataType.getBlobConverter().convert(ret);
      return new ByteArrayInputStream(bytes);
    }
  }

  @Override
  public void setString(String columnLabel, String value) {
    try {
      if (tableSchema == null) {
        fieldMap.put(columnLabel, value);
      }
      else {
        record.getFields()[tableSchema.getFieldOffset(columnLabel)] = value.getBytes("utf-8");
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public void setLong(String columnLabel, long value) {
    if (tableSchema == null) {
      fieldMap.put(columnLabel, value);
    }
    else {
      record.getFields()[tableSchema.getFieldOffset(columnLabel)] = value;
    }
  }

  @Override
  public void setBoolean(String columnLabel, boolean value) {
    if (tableSchema == null) {
      fieldMap.put(columnLabel, value);
    }
    else {
      record.getFields()[tableSchema.getFieldOffset(columnLabel)] = value;
    }
  }

  @Override
  public void setByte(String columnLabel, byte value) {
    if (tableSchema == null) {
      fieldMap.put(columnLabel, value);
    }
    else {
      record.getFields()[tableSchema.getFieldOffset(columnLabel)] = value;
    }
  }

  @Override
  public void setShort(String columnLabel, short value) {
    if (tableSchema == null) {
      fieldMap.put(columnLabel, value);
    }
    else {
      record.getFields()[tableSchema.getFieldOffset(columnLabel)] = value;
    }
  }

  @Override
  public void setInt(String columnLabel, int value) {
    if (tableSchema == null) {
      fieldMap.put(columnLabel, value);
    }
    else {
      record.getFields()[tableSchema.getFieldOffset(columnLabel)] = value;
    }
  }

  @Override
  public void setFloat(String columnLabel, float value) {
    if (tableSchema == null) {
      fieldMap.put(columnLabel, value);
    }
    else {
      record.getFields()[tableSchema.getFieldOffset(columnLabel)] = value;
    }
  }

  @Override
  public void setDouble(String columnLabel, double value) {
    if (tableSchema == null) {
      fieldMap.put(columnLabel, value);
    }
    else {
      record.getFields()[tableSchema.getFieldOffset(columnLabel)] = value;
    }
  }

  @Override
  public void setBigDecimal(String columnLabel, BigDecimal value) {
    if (tableSchema == null) {
      fieldMap.put(columnLabel, value);
    }
    else {
      record.getFields()[tableSchema.getFieldOffset(columnLabel)] = value;
    }
  }

  @Override
  public void setBytes(String columnLabel, byte[] value) {
    if (tableSchema == null) {
      fieldMap.put(columnLabel, value);
    }
    else {
      record.getFields()[tableSchema.getFieldOffset(columnLabel)] = value;
    }
  }

  @Override
  public void setDate(String columnLabel, Date value) {
    if (tableSchema == null) {
      fieldMap.put(columnLabel, value);
    }
    else {
      record.getFields()[tableSchema.getFieldOffset(columnLabel)] = value;
    }
  }

  @Override
  public void setTime(String columnLabel, Time value) {
    if (tableSchema == null) {
      fieldMap.put(columnLabel, value);
    }
    else {
      record.getFields()[tableSchema.getFieldOffset(columnLabel)] = value;
    }
  }

  @Override
  public void setTimestamp(String columnLabel, Timestamp value) {
    if (tableSchema == null) {
      fieldMap.put(columnLabel, value);
    }
    else {
      record.getFields()[tableSchema.getFieldOffset(columnLabel)] = value;
    }
  }

  @Override
  public void setBinaryStream(String columnLabel, InputStream value) {
    if (tableSchema == null) {
      fieldMap.put(columnLabel, value);
    }
    else {
      record.getFields()[tableSchema.getFieldOffset(columnLabel)] = value;
    }
  }

  public void setRecord(com.sonicbase.common.Record record) {
    this.record = record;
  }

  public void setDatabase(String database) {
    this.dbName = database;
  }

  public void setTableSchema(TableSchema tableSchema) {
    this.tableSchema = tableSchema;
  }

  public void setCommon(DatabaseCommon common) {
    this.common = common;
  }

  public void setViewVersion(int viewVersion) {
    this.viewVersion = viewVersion;
  }

  public void setIsDeleting(boolean isDeleting) {
    this.isDeleting = isDeleting;
  }

  public void setIsAdding(boolean isAdding) {
    this.isAdding = isAdding;
  }
}
