package com.lowryengineering.database.common;

import com.lowryengineering.database.query.DatabaseException;
import com.lowryengineering.database.schema.FieldSchema;
import com.lowryengineering.database.schema.TableSchema;
import com.lowryengineering.database.util.DataUtil;

import java.io.*;
import java.util.List;

/**
 * User: lowryda
 * Date: 9/30/14
 * Time: 5:22 PM
 */
public class Record {
  private TableSchema tableSchema;
  private long id;
  private Object[] fields;
  private long dbViewNumber;
  private long transId;
  private long dbViewFlags;

  public static long DB_VIEW_FLAG_DELETING = 0x1;
  public static long DB_VIEW_FLAG_ADDING = 0x2;

  public Record(TableSchema tableSchema) {
    this.tableSchema = tableSchema;
  }

  public Record(String dbName, DatabaseCommon common, byte[] bytes) {
    deserialize(dbName, common, bytes);
  }

  public void recoverFromSnapshot(String dbName, DatabaseCommon common, DataInputStream in, int serializationVersion) {
    try {
      DataUtil.ResultLength resultLength = new DataUtil.ResultLength();
      this.tableSchema = common.getTablesById(dbName).get((int) DataUtil.readVLong(in, resultLength));
      id = DataUtil.readVLong(in, resultLength);
      dbViewNumber = DataUtil.readVLong(in, resultLength);
      dbViewFlags = DataUtil.readVLong(in, resultLength);
      transId = DataUtil.readVLong(in, resultLength);
      fields = DatabaseCommon.deserializeFields(dbName, common, in, tableSchema, common.getSchemaVersion());
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public static long getTransId(byte[] bytes) {
    DataUtil.ResultLength resultLen = new DataUtil.ResultLength();
    int offset = 0;
    DataUtil.readVLong(bytes, offset, resultLen);
    offset += resultLen.getLength();
    DataUtil.readVLong(bytes, offset, resultLen);
    offset += resultLen.getLength();
    DataUtil.readVLong(bytes, offset, resultLen);
    offset += resultLen.getLength();
    DataUtil.readVLong(bytes, offset, resultLen);
    offset += resultLen.getLength();
    return DataUtil.readVLong(bytes, offset, resultLen);
  }

  public static long getDbViewNumber(byte[] bytes) {
    DataUtil.ResultLength resultLen = new DataUtil.ResultLength();
    int offset = 0;
    DataUtil.readVLong(bytes, offset, resultLen);
    offset += resultLen.getLength();
    DataUtil.readVLong(bytes, offset, resultLen);
    offset += resultLen.getLength();
    return DataUtil.readVLong(bytes, offset, resultLen);
  }

  public static long getDbViewFlags(byte[] bytes) {
    DataUtil.ResultLength resultLen = new DataUtil.ResultLength();
    int offset = 0;
    DataUtil.readVLong(bytes, offset, resultLen);
    offset += resultLen.getLength();
    DataUtil.readVLong(bytes, offset, resultLen);
    offset += resultLen.getLength();
    DataUtil.readVLong(bytes, offset, resultLen);
    offset += resultLen.getLength();
    return DataUtil.readVLong(bytes, offset, resultLen);
  }

  public long getDbViewFlags() {
    return dbViewFlags;
  }

  public void setDbViewFlags(long dbViewFlags) {
    this.dbViewFlags = dbViewFlags;
  }

  public long getId() {
    return id;
  }

  public long getDbViewNumber() {
    return dbViewNumber;
  }

  public void setDbViewNumber(long dbViewNumber) {
    this.dbViewNumber = dbViewNumber;
  }

  public long getTransId() {
    return transId;
  }

  public void setTransId(long transId) {
    this.transId = transId;
  }

  public void snapshot(DataOutputStream out, DatabaseCommon common) throws IOException {
    DataUtil.ResultLength resultLen = new DataUtil.ResultLength();
    DataUtil.writeVLong(out, tableSchema.getTableId(), resultLen);
    DataUtil.writeVLong(out, id, resultLen);
    DataUtil.writeVLong(out, dbViewNumber, resultLen);
    DataUtil.writeVLong(out, dbViewFlags, resultLen);
    DataUtil.writeVLong(out, transId, resultLen);
    DatabaseCommon.serializeFields(fields, out, tableSchema, common.getSchemaVersion());
  }

  public void setId(long id) {
    this.id = id;
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="EI_EXPOSE_REP2", justification="copying the passed in data is too slow")
  @SuppressWarnings("PMD.ArrayIsStoredDirectly") //copying the passed in data is too slow
  public void setFields(Object[] fields) {
    this.fields = fields;
  }

  public Object getField(String columnName) {
    List<FieldSchema> fieldSchema = tableSchema.getFields();
    for (int i = 0; i < fieldSchema.size(); i++) {
      if (columnName.toLowerCase().equals(fieldSchema.get(i).getName().toLowerCase())) {
        return fields[i];
      }
    }
    return null;
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="EI_EXPOSE_REP", justification="copying the returned data is too slow")
  public Object[] getFields() {
    return fields;
  }

  public TableSchema getTableSchema() {
    return tableSchema;
  }

  public byte[] serialize(DatabaseCommon common) {
    try {
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytesOut);
      snapshot(out, common);
      out.close();
      return bytesOut.toByteArray();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public void deserialize(String dbName, DatabaseCommon common, byte[] bytes) {
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
    recoverFromSnapshot(dbName, common, in, 0);
  }

  public void deserialize(String dbName, DatabaseCommon common, DataInputStream in) throws IOException {
    recoverFromSnapshot(dbName, common, in, 0);
  }

  public static long getTableId(byte[] record) {
    return DataUtil.readVLong(record, 0, new DataUtil.ResultLength());
  }

}
