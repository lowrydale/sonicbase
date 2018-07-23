package com.sonicbase.common;

import com.sonicbase.query.DatabaseException;
import org.apache.giraph.utils.Varint;

import java.io.*;

/**
 * User: lowryda
 * Date: 9/30/14
 * Time: 5:22 PM
 */
public class KeyRecord {
  private byte[] primaryKey;
  private long sequence0;
  private long sequence1;
  private short sequence2;
  private int dbViewNumber;
  private short dbViewFlags;
  public static final short DB_VIEW_FLAG_DELETING = 0x1;
  public static final  short DB_VIEW_FLAG_ADDING = 0x2;

  public KeyRecord() {

  }

  public KeyRecord(byte[] bytes) {
    deserialize(bytes);
  }

  public void deserialize(byte[] bytes) {
    try {

      DataInputStream sin = new DataInputStream(new ByteArrayInputStream(bytes));
      sin.readShort(); // serializationVersion

      sequence0 = sin.readLong();
      sequence1 = sin.readLong();
      sequence2 = sin.readShort();
      dbViewNumber = sin.readInt();
      dbViewFlags = sin.readShort();

      int len = Varint.readSignedVarInt(sin);
      primaryKey = new byte[len];
      sin.readFully(primaryKey);
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public long getSequence0() {
    return sequence0;
  }

  public long getSequence1() {
    return sequence1;
  }

  public short getSequence2() {
    return sequence2;
  }

  public void setSequence0(long sequence0) {
    this.sequence0 = sequence0;
  }

  public void setSequence1(long sequence1) {
    this.sequence1 = sequence1;
  }

  public void setSequence2(short sequence2) {
    this.sequence2 = sequence2;
  }

  public byte[] getPrimaryKey() {
    return primaryKey;
  }

  public static void setSequence0(byte[] bytes, long sequence0) {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytesOut);
    try {
      out.writeLong(sequence0);
      System.arraycopy(bytesOut.toByteArray(), 0, bytes, 2 , 8);
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public static void setSequence1(byte[] bytes, long sequence1) {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytesOut);
    try {
      out.writeLong(sequence1);
      System.arraycopy(bytesOut.toByteArray(), 0, bytes, 2 + 8, 8);
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
      System.arraycopy(bytesOut.toByteArray(), 0, bytes, 2 + 8 + 8 + 2, 4);
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

  public static long getDbViewFlags(byte[] bytes) {
    int offset = 2; //serialization version
    offset += 8 + 8 + 2 + 4;  //viewNum
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes, offset, 2));
    try {
      return in.readShort();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
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

  public static byte[] getPrimaryKey(byte[] bytes) {
    int offset = 2; //serialization version
    offset += 8 + 8 + 2 + 4 + 2;  //viewNum
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes, offset, bytes.length - offset));
    try {
      int len = Varint.readSignedVarInt(in);
      byte[] ret = new byte[len];
      in.readFully(ret);
      return ret;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public void serialize(DataOutputStream out, short serializationVersion) throws IOException {
    out.writeShort(serializationVersion);
    out.writeLong(sequence0);
    out.writeLong(sequence1);
    out.writeShort(sequence2);
    out.writeInt(dbViewNumber);
    out.writeShort(dbViewFlags);
    Varint.writeSignedVarInt(primaryKey.length, out);
    out.write(primaryKey);
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

  public void setPrimaryKey(byte[] primaryKey) {
    this.primaryKey = primaryKey;
  }

  public void setDbViewNumber(int dbViewNumber) {
    this.dbViewNumber = dbViewNumber;
  }

}
