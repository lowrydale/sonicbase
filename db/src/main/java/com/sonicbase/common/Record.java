package com.sonicbase.common;

import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.FieldSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.server.DatabaseServer;
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
  private long id;
  private Object[] fields;
  private int dbViewNumber;
  private long transId;
  private short dbViewFlags;
  private long sequence0;
  private long sequence1;
  private long sequence2;
  private long updateTime;

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

  public static long readFlags(byte[] bytes) {
    try {
      DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));

      Varint.readSignedVarLong(in);
      Varint.readSignedVarLong(in);

      return Varint.readSignedVarLong(in);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public void recoverFromSnapshot(String dbName, DatabaseCommon common, byte[] bytes, Set<Integer> columns, boolean readHeader) {
    try {
        DataInputStream sin = new DataInputStream(new ByteArrayInputStream(bytes));
        short serializationVersion = sin.readShort();
        sequence0 = sin.readLong();
        sequence1 = sin.readLong();
        sequence2 = sin.readLong();

        dbViewNumber = sin.readInt();
        dbViewFlags = sin.readShort();
        transId = sin.readLong();
        id = sin.readLong();

      if (serializationVersion >= DatabaseServer.SERIALIZATION_VERSION_23) {
        updateTime = sin.readLong();
      }

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
      int offset = 8 * 3; //sequence numbers

      DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
      in.skipBytes(offset);
      Varint.readSignedVarLong(in);
      Varint.readSignedVarLong(in);
      Varint.readSignedVarLong(in);
      return Varint.readSignedVarLong(in);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public static void setDbViewFlags(byte[] bytes, short dbViewFlag) {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytesOut);
    try {
      out.writeShort(dbViewFlag);
      System.arraycopy(bytesOut.toByteArray(), 0, bytes, 2 + 3 * 8 + 4, 2);
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
      System.arraycopy(bytesOut.toByteArray(), 0, bytes,  2 + 3 * 8, 4);
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public static long getDbViewNumber(byte[] bytes) {
    int offset = 2 + 8 * 3; //serialization version + sequence numbers
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes, offset, 4));
    try {
      return in.readInt();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public static long getUpdateTime(byte[] bytes) {
    int offset = 2 + 8 * 3 + 4 + 2 + 8 + 8; //serialization version + sequence numbers
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes, offset, bytes.length - offset));
    try {
      return in.readLong();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }

  }

  public static long getDbViewFlags(byte[] bytes) {
    int offset = 2 + 8 * 3; //serialization version + sequence numbers
    offset += 4;//viewNum
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes, offset, bytes.length - offset));
    try {
      return in.readShort();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public void setUpdateTime(long updateTime) {
    this.updateTime = updateTime;
  }

  public long getUpdateTime() {
    return updateTime;
  }

  public short getDbViewFlags() {
    return dbViewFlags;
  }

  public void setDbViewFlags(short dbViewFlags) {
    this.dbViewFlags = dbViewFlags;
  }

  public long getId() {
    return id;
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

  public void setSequence2(long value) {
    this.sequence2 = value;
  }

  public long getSequence2() {
    return sequence2;
  }

  public void snapshot(DataOutputStream out, DatabaseCommon common, short serializationVersion) throws IOException {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream headerOut = new DataOutputStream(bytesOut);

    headerOut.writeShort(serializationVersion);
    headerOut.writeLong(sequence0);
    headerOut.writeLong(sequence1);
    headerOut.writeLong(sequence2);

    headerOut.writeInt(dbViewNumber);
    headerOut.writeShort(dbViewFlags);
    headerOut.writeLong(transId);
    headerOut.writeLong(id);

    if (serializationVersion >= DatabaseServer.SERIALIZATION_VERSION_23) {
      headerOut.writeLong(updateTime);
    }

    headerOut.close();
    byte[] bytes = bytesOut.toByteArray();
    //Varint.writeSignedVarLong(out, bytes.length);
    out.write(bytes);
    Varint.writeSignedVarLong(tableSchema.getTableId(), out);
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
