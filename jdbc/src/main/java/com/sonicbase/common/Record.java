package com.sonicbase.common;

import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.Varint;

import java.io.*;
import java.util.Set;

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class Record {
  private TableSchema tableSchema;
  private Object[] fields;
  private int dbViewNumber;
  private long transId;
  private short dbViewFlags;
  private long sequence0;
  private long sequence1;
  private short sequence2;

  public static final short DB_VIEW_FLAG_DELETING = 0x1;
  public static final short DB_VIEW_FLAG_ADDING = 0x2;

  public Record(TableSchema tableSchema) {
    this.tableSchema = tableSchema;
  }

  public Record(String dbName, DatabaseCommon common, byte[] bytes) {
    deserialize(dbName, common, bytes, null);
  }

  public Record(String dbName, DatabaseCommon common, byte[] bytes, TableSchema tableSchema) {
    this.tableSchema = tableSchema;
    deserialize(dbName, common, bytes, null);
  }

  public Record(String dbName, DatabaseCommon common, byte[] bytes, Set<Integer> columns, boolean readHeader) {
    deserialize(dbName, common, bytes, columns, readHeader);
  }

  private void recoverFromSnapshot(String dbName, DatabaseCommon common, byte[] bytes, Set<Integer> columns, boolean readHeader) {
    try {
      DataInputStream sin = new DataInputStream(new ByteArrayInputStream(bytes, !readHeader ? 26 : 0, !readHeader ?
          bytes.length - 26 : bytes.length));
      if (readHeader) {
        sin.readShort(); // serializationVersion
        sequence0 = sin.readLong();
        sequence1 = sin.readLong();
        sequence2 = sin.readShort();

        dbViewNumber = sin.readInt();
        dbViewFlags = sin.readShort();
      }
      transId = Varint.readSignedVarLong(sin);
      int tableId = (int) Varint.readSignedVarLong(sin);
      if (tableSchema == null) {
        this.tableSchema = common.getTablesById(dbName).get(tableId);
      }

      Varint.readSignedVarLong(sin); //len
      fields = DatabaseCommon.deserializeFields(sin, tableSchema,
          common.getSchemaVersion(), columns);
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public static long getTransId(byte[] bytes) {
    try {
      int offset = 2 + 8 + 8 + 2 + 4 + 2;

      DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
      in.skipBytes(offset);
      return Varint.readSignedVarLong(in);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public static void setSequences(byte[] recordBytes, long sequence0, long sequence1, short sequence2) {
    DataUtils.longToBytes(sequence0, recordBytes, 2);
    DataUtils.longToBytes(sequence1, recordBytes, 2 + 8);
    DataUtils.shortToBytes(sequence2, recordBytes, 2 + 16);
  }

  public static void setDbViewFlags(byte[] bytes, short dbViewFlag) {
    DataUtils.shortToBytes(dbViewFlag, bytes, 2 + 8 + 8 + 2 + 4);
  }

  public static void setDbViewNumber(byte[] bytes, int schemaVersion) {
    DataUtils.intToBytes(schemaVersion, bytes, 2 + 8 + 8 + 2);
  }

  public static int getDbViewNumber(byte[] bytes) {
    int offset = 2 + 8 + 8 + 2; //serialization version + sequence numbers
    return DataUtils.bytesToInt(bytes, offset);
  }

  public static long getUpdateTime(byte[] bytes) {
    return getSequence0(bytes);
  }

  public static long getSequence1(byte[] bytes) {
    int offset = 2 + 8;
    return DataUtils.bytesToLong(bytes, offset);
  }

  private static long getSequence0(byte[] bytes) {
    int offset = 2;
    return DataUtils.bytesToLong(bytes, offset);
  }

  public static short getDbViewFlags(byte[] bytes) {
    int offset = 2 + 8 + 8 + 2 + 4; //serialization version + sequence numbers
    return DataUtils.bytesToShort(bytes, offset);
  }

  public short getDbViewFlags() {
    return dbViewFlags;
  }

  public void setDbViewFlags(short dbViewFlags) {
    this.dbViewFlags = dbViewFlags;
  }

  public long getDbViewNumber() {
    return dbViewNumber;
  }

  public void setDbViewNumber(int dbViewNumber) {
    this.dbViewNumber = dbViewNumber;
  }

  public long getTransId() {
    return transId;
  }

  public void setTransId(long transId) {
    this.transId = transId;
  }

  public void setSequence0(long value) {
    this.sequence0 = value;
  }

  public long getSequence0() {
    return sequence0;
  }

  public void setSequence1(long value) {
    this.sequence1 = value;
  }

  public long getSequence1() {
    return sequence1;
  }

  public void setSequence2(short value) {
    this.sequence2 = value;
  }

  public short getSequence2() {
    return sequence2;
  }

  public void snapshot(DataOutputStream out, DatabaseCommon common, short serializationVersion) throws IOException {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream headerOut = new DataOutputStream(bytesOut);

    headerOut.writeShort(serializationVersion);
    headerOut.writeLong(sequence0);
    headerOut.writeLong(sequence1);
    headerOut.writeShort(sequence2);

    headerOut.writeInt(dbViewNumber);
    headerOut.writeShort(dbViewFlags);
    Varint.writeSignedVarLong(transId, headerOut);

    headerOut.close();
    byte[] bytes = bytesOut.toByteArray();

    out.write(bytes);
    Varint.writeSignedVarLong(tableSchema.getTableId(), out);
    DatabaseCommon.serializeFields(fields, out, tableSchema, common.getSchemaVersion());
  }

  public void setFields(Object[] fields) {
    this.fields = fields;
  }

  public Object[] getFields() {
    return fields;
  }

  public TableSchema getTableSchema() {
    return tableSchema;
  }

  public byte[] serialize(DatabaseCommon common, short serializationVersion) {
    try {
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytesOut);
      snapshot(out, common, serializationVersion);
      out.close();
      return bytesOut.toByteArray();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public void deserialize(String dbName, DatabaseCommon common, byte[] bytes, Set<Integer> columns, boolean readHeader) {
    recoverFromSnapshot(dbName, common, bytes, columns, readHeader);
  }

  public void deserialize(String dbName, DatabaseCommon common, byte[] bytes, Set<Integer> columns) {
    recoverFromSnapshot(dbName, common, bytes, columns, true);
  }

}
