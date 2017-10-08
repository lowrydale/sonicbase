package com.sonicbase.common;

import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.FieldSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.DataUtil;

import java.io.*;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * User: lowryda
 * Date: 9/30/14
 * Time: 5:22 PM
 */
public class KeyRecord {
  private long key;
  private int dbViewNumber;
  private short dbViewFlags;
  public static short DB_VIEW_FLAG_DELETING = 0x1;
  public static short DB_VIEW_FLAG_ADDING = 0x2;

  public KeyRecord() {

  }

  public KeyRecord(byte[] bytes) {
    deserialize(bytes);
  }

  public static long readFlags(byte[] bytes) {
    DataUtil.ResultLength resultLength = new DataUtil.ResultLength();
    int byteOffset = 0; //sequence numbers
    //int headerLen = (int)DataUtil.readVLong(bytes, byteOffset, resultLength);
    byteOffset += resultLength.getLength();

    DataUtil.readVLong(bytes, byteOffset, resultLength);
    byteOffset += resultLength.getLength();
    DataUtil.readVLong(bytes, byteOffset, resultLength);
    byteOffset += resultLength.getLength();
    long dbViewFlags = DataUtil.readVLong(bytes, byteOffset, resultLength);

    return dbViewFlags;
  }

  public void deserialize(byte[] bytes) {
    try {
      DataUtil.ResultLength resultLength = new DataUtil.ResultLength();
      int byteOffset = 0;
      byteOffset += resultLength.getLength();
      DataInputStream sin = new DataInputStream(new ByteArrayInputStream(bytes, byteOffset, 8 * 6 + 2));
      short serializationVersion = sin.readShort();
      byteOffset += 2;

      dbViewNumber = sin.readInt();
      dbViewFlags = sin.readShort();
      byteOffset += 4 + 2;

      key = DataUtil.readVLong(bytes, byteOffset, resultLength);
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public long getKey() {
    return key;
  }

  public static void setDbViewFlags(byte[] bytes, short dbViewFlag) {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytesOut);
    try {
      out.writeShort(dbViewFlag);
      System.arraycopy(bytesOut.toByteArray(), 0, bytes, 2 + 4, 2);
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
      System.arraycopy(bytesOut.toByteArray(), 0, bytes, 2, 4);
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public static long getDbViewNumber(byte[] bytes) {
    int offset = 2; //serialization version + sequence numbers
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes, offset, 4));
    try {
      return in.readInt();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public static long getDbViewFlags(byte[] bytes) {
    DataUtil.ResultLength resultLen = new DataUtil.ResultLength();
    int offset = 2; //serialization version
    offset += 4;  //viewNum
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes, offset, 2));
    try {
      return in.readShort();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public void serialize(DataOutputStream out, short serializationVersion) throws IOException {
    out.writeShort(serializationVersion);
    out.writeInt(dbViewNumber);
    out.writeShort(dbViewFlags);
    DataUtil.writeVLong(out, key);
  }


  public byte[] serialize(short serializationVersion) {
    try {
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytesOut);
      serialize(out, serializationVersion);
      out.close();
      return bytesOut.toByteArray();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public void setKey(long key) {
    this.key = key;
  }

  public void setDbViewNumber(int dbViewNumber) {
    this.dbViewNumber = dbViewNumber;
  }
}
