package com.sonicbase.common;

import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.FieldSchema;
import com.sonicbase.schema.TableSchema;
import org.apache.giraph.utils.Varint;

import java.io.*;
import java.util.List;
import java.util.Set;

/**
 * User: lowryda
 * Date: 9/30/14
 * Time: 5:22 PM
 */
public class Record {
  private TableSchema tableSchema;
  private Object[] fields;
  private int dbViewNumber;
  private long transId;
  private short dbViewFlags;
  private long sequence0;
  private long sequence1;
  private short sequence2;

  public static short DB_VIEW_FLAG_DELETING = 0x1;
  public static short DB_VIEW_FLAG_ADDING = 0x2;

  public Record(TableSchema tableSchema) {
    this.tableSchema = tableSchema;
  }

  public Record(String dbName, DatabaseCommon common, byte[] bytes) {
    deserialize(dbName, common, bytes, null);
  }

  public Record(String dbName, DatabaseCommon common, byte[] bytes, Set<Integer> columns, boolean readHeader) {
    deserialize(dbName, common, bytes, columns, readHeader);
  }

  public void recoverFromSnapshot(String dbName, DatabaseCommon common, byte[] bytes, Set<Integer> columns, boolean readHeader) {
    try {
      DataInputStream sin = new DataInputStream(new ByteArrayInputStream(bytes));
      short serializationVersion = sin.readShort();
      sequence0 = sin.readLong();
      sequence1 = sin.readLong();
      sequence2 = sin.readShort();

      dbViewNumber = sin.readInt();
      dbViewFlags = sin.readShort();
      transId = Varint.readSignedVarLong(sin);

      this.tableSchema = common.getTablesById(dbName).get((int) Varint.readSignedVarLong(sin));

      int len = (int)Varint.readSignedVarLong(sin);
      fields = DatabaseCommon.deserializeFields(dbName, common, sin, tableSchema,
          common.getSchemaVersion(), dbViewNumber, columns, true);
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
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytesOut);
    try {
      out.writeLong(sequence0);
      out.writeLong(sequence1);
      out.writeShort(sequence2);
      System.arraycopy(bytesOut.toByteArray(), 0, recordBytes, 2, 8 + 8 + 2);
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public static void setDbViewFlags(byte[] bytes, short dbViewFlag) {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytesOut);
    try {
      out.writeShort(dbViewFlag);
      System.arraycopy(bytesOut.toByteArray(), 0, bytes, 2 + 8 + 8 + 2 + 4, 2);
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public static void setDbViewNumber(byte[] bytes, int schemaVersion) {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytesOut);
    try {
      out.writeInt(schemaVersion);
      System.arraycopy(bytesOut.toByteArray(), 0, bytes,  2 + 8 + 8 + 2, 4);
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public static long getDbViewNumber(byte[] bytes) {
    int offset = 2 + 8 + 8 + 2; //serialization version + sequence numbers
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes, offset, 4));
    try {
      return in.readInt();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public static long getUpdateTime(byte[] bytes) {
    return getSequence0(bytes);
  }

  public static long getSequence1(byte[] bytes) {
    int offset = 2 + 8;
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes, offset, 8));
    try {
      return in.readLong();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public static long getSequence0(byte[] bytes) {
    int offset = 2;
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes, offset, 8));
    try {
      return in.readLong();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }


  public static long getDbViewFlags(byte[] bytes) {
    int offset = 2 + 8 + 8 + 2 + 4; //serialization version + sequence numbers
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes, offset, bytes.length - offset));
    try {
      return in.readShort();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public long getUpdateTime() {
    return sequence0;
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
    //Varint.writeSignedVarLong(out, bytes.length);
    out.write(bytes);
    Varint.writeSignedVarLong(tableSchema.getTableId(), out);
    DatabaseCommon.serializeFields(fields, out, tableSchema, common.getSchemaVersion(), true);
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
