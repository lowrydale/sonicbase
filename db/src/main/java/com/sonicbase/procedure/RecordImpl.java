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
    byte[] bytes = cobj.getByteArray(ComObject.Tag.recordBytes);
    if (bytes != null) {
      dbName = cobj.getString(ComObject.Tag.dbName);
      tableSchema = common.getTables(dbName).get(cobj.getString(ComObject.Tag.tableName));
      record = new com.sonicbase.common.Record(dbName, common, bytes);
      this.common = common;
    }
    else {
      ComArray array = cobj.getArray(ComObject.Tag.fields);
      for (int i = 0; i < array.getArray().size(); i++) {
        ComObject parm = (ComObject) array.getArray().get(i);
        String fieldName = parm.getString(ComObject.Tag.fieldName);
        Map<Integer, Object> fields = parm.getMap();
        for (Map.Entry<Integer, Object> field : fields.entrySet()) {
          if (field.getKey() == ComObject.Tag.stringValue.tag) {
            this.fieldMap.put(fieldName, parm.getString(ComObject.Tag.stringValue));
          }
          else if (field.getKey() == ComObject.Tag.longValue.tag) {
            fieldMap.put(fieldName, parm.getLong(ComObject.Tag.longValue));
          }
          else if (field.getKey() == ComObject.Tag.intValue.tag) {
            fieldMap.put(fieldName, parm.getInt(ComObject.Tag.intValue));
          }
          else if (field.getKey() == ComObject.Tag.booleanValue.tag) {
            fieldMap.put(fieldName, parm.getBoolean(ComObject.Tag.booleanValue));
          }
          else if (field.getKey() == ComObject.Tag.timeValue.tag) {
            fieldMap.put(fieldName, parm.getTime(ComObject.Tag.timeValue));
          }
          else if (field.getKey() == ComObject.Tag.dateValue.tag) {
            fieldMap.put(fieldName, parm.getDate(ComObject.Tag.dateValue));
          }
          else if (field.getKey() == ComObject.Tag.timestampValue.tag) {
            fieldMap.put(fieldName, parm.getTimestamp(ComObject.Tag.timestampValue));
          }
          else if (field.getKey() == ComObject.Tag.floatValue.tag) {
            fieldMap.put(fieldName, parm.getFloat(ComObject.Tag.floatValue));
          }
          else if (field.getKey() == ComObject.Tag.doubleValue.tag) {
            fieldMap.put(fieldName, parm.getDouble(ComObject.Tag.doubleValue));
          }
          else if (field.getKey() == ComObject.Tag.bigDecimalValue.tag) {
            fieldMap.put(fieldName, parm.getBigDecimal(ComObject.Tag.bigDecimalValue));
          }
          else if (field.getKey() == ComObject.Tag.byteValue.tag) {
            fieldMap.put(fieldName, parm.getByte(ComObject.Tag.byteValue));
          }
          else if (field.getKey() == ComObject.Tag.shortValue.tag) {
            fieldMap.put(fieldName, parm.getShort(ComObject.Tag.shortValue));
          }
          else if (field.getKey() == ComObject.Tag.byteArrayValue.tag) {
            fieldMap.put(fieldName, parm.getByteArray(ComObject.Tag.byteArrayValue));
          }
        }
      }
    }
  }

  public ComObject serialize() {
    ComObject ret = new ComObject();
    if (tableSchema != null) {
      byte[] bytes = record.serialize(common, serializationNumber);
      ret.put(ComObject.Tag.recordBytes, bytes);
      ret.put(ComObject.Tag.tableName, tableSchema.getName());
      ret.put(ComObject.Tag.dbName, dbName);
    }
    else {
      ComArray array = ret.putArray(ComObject.Tag.fields, ComObject.Type.objectType);
      for (Map.Entry<String, Object> field : fieldMap.entrySet()) {
        ComObject parm = new ComObject();
        parm.put(ComObject.Tag.fieldName, field.getKey());
        Object value = field.getValue();
        if (value instanceof String) {
          parm.put(ComObject.Tag.stringValue, (String)value);
        }
        else if (value instanceof Long) {
          parm.put(ComObject.Tag.longValue, (Long)value);
        }
        else if (value instanceof Integer) {
          parm.put(ComObject.Tag.intValue, (Integer)value);
        }
        else if (value instanceof Boolean) {
          parm.put(ComObject.Tag.booleanValue, (Boolean)value);
        }
        else if (value instanceof Time) {
          parm.put(ComObject.Tag.timeValue, (Time)value);
        }
        else if (value instanceof Date) {
          parm.put(ComObject.Tag.dateValue, (Date)value);
        }
        else if (value instanceof Timestamp) {
          parm.put(ComObject.Tag.timestampValue, (Timestamp)value);
        }
        else if (value instanceof Float) {
          parm.put(ComObject.Tag.floatValue, (Float)value);
        }
        else if (value instanceof Double) {
          parm.put(ComObject.Tag.doubleValue, (Double)value);
        }
        else if (value instanceof BigDecimal) {
          parm.put(ComObject.Tag.bigDecimalValue, (BigDecimal)value);
        }
        else if (value instanceof Byte) {
          parm.put(ComObject.Tag.byteValue, (byte)value);
        }
        else if (value instanceof Short) {
          parm.put(ComObject.Tag.shortValue, (short)value);
        }
        else if (value instanceof byte[]) {
          parm.put(ComObject.Tag.byteArrayValue, (byte[])value);
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

  public java.sql.Date getDate(String columnLabel) {
    if (tableSchema == null) {
      return (Date) DataType.getDateConverter().convert(fieldMap.get(columnLabel));
    }
    else {
      Object ret = record.getFields()[tableSchema.getFieldOffset(columnLabel)];
      return (Date) DataType.getDateConverter().convert(ret);
    }
  }

  public java.sql.Time getTime(String columnLabel) {
    if (tableSchema == null) {
      return (Time) DataType.getTimeConverter().convert(fieldMap.get(columnLabel));
    }
    else {
      Object ret = record.getFields()[tableSchema.getFieldOffset(columnLabel)];
      return (Time) DataType.getTimeConverter().convert(ret);
    }
  }

  public java.sql.Timestamp getTimestamp(String columnLabel) {
    if (tableSchema == null) {
      return (Timestamp) DataType.getTimestampConverter().convert(fieldMap.get(columnLabel));
    }
    else {
      Object ret = record.getFields()[tableSchema.getFieldOffset(columnLabel)];
      return (Timestamp) DataType.getTimestampConverter().convert(ret);
    }
  }

  public java.io.InputStream getBinaryStream(String columnLabel) {
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
