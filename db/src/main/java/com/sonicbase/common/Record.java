package com.sonicbase.common;

import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.FieldSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.DataUtil;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

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
  private AtomicInteger serializedSchemaVersion = new AtomicInteger();

  public static long DB_VIEW_FLAG_DELETING = 0x1;
  public static long DB_VIEW_FLAG_ADDING = 0x2;

  public Record(TableSchema tableSchema) {
    this.tableSchema = tableSchema;
  }

  public Record(String dbName, DatabaseCommon common, byte[] bytes) {
    deserialize(dbName, common, bytes, null);
  }

  public Record(String dbName, DatabaseCommon common, byte[] bytes, Set<Integer> columns, boolean readHeader) {
    deserialize(dbName, common, bytes, columns, readHeader);
  }

  public static long readFlags(byte[] bytes) {
    DataUtil.ResultLength resultLength = new DataUtil.ResultLength();
    int byteOffset = 0;
    int headerLen = (int)DataUtil.readVLong(bytes, byteOffset, resultLength);
    byteOffset += resultLength.getLength();

    DataUtil.readVLong(bytes, byteOffset, resultLength);
    byteOffset += resultLength.getLength();
    DataUtil.readVLong(bytes, byteOffset, resultLength);
    byteOffset += resultLength.getLength();
    long dbViewFlags = DataUtil.readVLong(bytes, byteOffset, resultLength);

    return dbViewFlags;
  }

  public void recoverFromSnapshot(String dbName, DatabaseCommon common, byte[] bytes, int serializationVersion, Set<Integer> columns, boolean readHeader) {
    try {
      DataUtil.ResultLength resultLength = new DataUtil.ResultLength();
      int byteOffset = 0;
      int headerLen = (int)DataUtil.readVLong(bytes, byteOffset, resultLength);
      byteOffset += resultLength.getLength();
      if (!readHeader) {
        byteOffset += headerLen;
      }
      else {
        id = DataUtil.readVLong(bytes, byteOffset, resultLength);
        byteOffset += resultLength.getLength();
        dbViewNumber = DataUtil.readVLong(bytes, byteOffset, resultLength);
        byteOffset += resultLength.getLength();
        dbViewFlags = DataUtil.readVLong(bytes, byteOffset, resultLength);
        byteOffset += resultLength.getLength();
        transId = DataUtil.readVLong(bytes, byteOffset, resultLength);
        byteOffset += resultLength.getLength();
      }
      this.tableSchema = common.getTablesById(dbName).get((int) DataUtil.readVLong(bytes, byteOffset, resultLength));
      byteOffset += resultLength.getLength();

       int len = (int)DataUtil.readVLong(bytes, byteOffset, resultLength);
      byteOffset += resultLength.getLength();
      fields = DatabaseCommon.deserializeFields(dbName, common, bytes, byteOffset, tableSchema, common.getSchemaVersion(), columns, serializedSchemaVersion, true);
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
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream headerOut = new DataOutputStream(bytesOut);

    DataUtil.writeVLong(headerOut, id, resultLen);
    DataUtil.writeVLong(headerOut, dbViewNumber, resultLen);
    DataUtil.writeVLong(headerOut, dbViewFlags, resultLen);
    DataUtil.writeVLong(headerOut, transId, resultLen);
    headerOut.close();
    byte[] bytes = bytesOut.toByteArray();
    DataUtil.writeVLong(out, bytes.length);
    out.write(bytes);
    DataUtil.writeVLong(out, tableSchema.getTableId(), resultLen);
    DatabaseCommon.serializeFields(fields, out, tableSchema, common.getSchemaVersion(), true);
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

  public void deserialize(String dbName, DatabaseCommon common, byte[] bytes, Set<Integer> columns, boolean readHeader) {
    recoverFromSnapshot(dbName, common, bytes, 0, columns, readHeader);
  }

  public void deserialize(String dbName, DatabaseCommon common, byte[] bytes, Set<Integer> columns) {
    recoverFromSnapshot(dbName, common, bytes, 0, columns, true);
  }

  public static long getTableId(byte[] record) {
    return DataUtil.readVLong(record, 0, new DataUtil.ResultLength());
  }

  public int getSerializedSchemaVersion() {
    return serializedSchemaVersion.get();
  }
}
