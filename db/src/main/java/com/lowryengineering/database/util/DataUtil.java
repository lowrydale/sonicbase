package com.lowryengineering.database.util;

/**
 * User: lowryda
 * Date: 12/26/13
 * Time: 8:27 AM
 */

import java.io.*;

public class DataUtil {


  /**
   * ResultLength.
   */
  public static final class ResultLength {

    private int length;

    public int getLength() {
      return length;
    }

    public void setLength(int length) {
      this.length = length;
    }

    public String toString() {
      return Integer.toString(length);
    }
  }

//  public static void writeVLong(long value, byte[] buffer, int sourceOffset, ResultLength resultLength) {
//    int localSourceOffset = sourceOffset;
//    long localValue = value;
//    try {
//      final int beginOffset = localSourceOffset;
//      while ((localValue & ~0x7F) != 0) {
//        buffer[localSourceOffset++] = (byte)((localValue & 0x7f) | 0x80);
//        localValue >>>= 7;
//      }
//      buffer[localSourceOffset++] = (byte)localValue;
//      resultLength.length = localSourceOffset - beginOffset;
//    }
//    catch (ArrayIndexOutOfBoundsException e) {
//      ArrayIndexOutOfBoundsException arrayEx = new ArrayIndexOutOfBoundsException("index: " + localSourceOffset + ", value: " + localValue);
//      arrayEx.setStackTrace(e.getStackTrace());
//      throw arrayEx;
//    }
//  }

  public static void writeVLong(DataOutputStream out, long value) throws IOException {
    long localValue = value;
    while ((localValue & ~0x7F) != 0) {
      out.write((byte)((localValue & 0x7f) | 0x80));
      localValue >>>= 7;
    }
    out.write((byte)localValue);
  }


  public static void writeVLong(DataOutput out, long value, ResultLength resultLength) throws IOException {
    long localValue = value;
    int count = 1;
    while ((localValue & ~0x7F) != 0) {
      out.write((byte)((localValue & 0x7f) | 0x80));
      localValue >>>= 7;
      count++;
    }
    out.write((byte)localValue);
    if (resultLength != null) {
      resultLength.length = count;
    }
  }


  /**
   * Reads a long stored in variable-length format.  Reads between one and
   * nine bytes.  Smaller values take fewer bytes.  Negative numbers are not
   * supported.
   */
  public static long readVLong(byte[] buffer, int sourceOffset, ResultLength resultLength) {
    final int beginOffset = sourceOffset;
    int localSourceOffset = sourceOffset;
    byte b = buffer[localSourceOffset++];
    long value = b & 0x7F;
    for (int shift = 7; (b & 0x80) != 0; shift += 7) {
      b = buffer[localSourceOffset++];
      value |= (b & 0x7FL) << shift;
    }
    if (resultLength != null) {
      resultLength.length = localSourceOffset - beginOffset;
    }
    return value;
  }

  /**
   * Reads a long stored in variable-length format.  Reads between one and
   * nine bytes.  Smaller values take fewer bytes.  Negative numbers are not
   * supported.
   */

  public static long readVLong(DataInput in) throws IOException {
    byte b = in.readByte();
    long value = b & 0x7F;
    for (int shift = 7; (b & 0x80) != 0; shift += 7) {
      b = in.readByte();
      value |= (b & 0x7FL) << shift;
    }
    return value;
  }

  public static long readVLong(DataInput in, ResultLength resultLength) throws IOException {

    byte b = in.readByte();
    int len = 1;
    long value = b & 0x7F;
    for (int shift = 7; (b & 0x80) != 0; shift += 7) {
      b = in.readByte();
      len++;
      value |= (b & 0x7FL) << shift;
    }
    if (resultLength != null) {
      resultLength.length = len;
    }
    return value;
  }

  /**
   * Writes a long value to the specified byte []
   *
   * @param buffer OutputWtream to write to value to
   * @param offset position in buffer to write value to
   * @param value  value to write to byte []
   */
//  public static void writeLong(byte[] buffer, int offset, long value) {
//    buffer[offset + 0] = (byte)(value >>> 56);
//    buffer[offset + 1] = (byte)(value >>> 48);
//    buffer[offset + 2] = (byte)(value >>> 40);
//    buffer[offset + 3] = (byte)(value >>> 32);
//    buffer[offset + 4] = (byte)(value >>> 24);
//    buffer[offset + 5] = (byte)(value >>> 16);
//    buffer[offset + 6] = (byte)(value >>> 8);
//    buffer[offset + 7] = (byte)(value >>> 0);
//  }

  /**
   * Writes a long value to the specified output stream
   *
   * @param out   OutputWtream to write to value to
   * @param value value to write to OutputStream
   */
//  public static void writeLong(OutputStream out, long value) throws IOException {
//    out.write((byte)(value >>> 56));
//    out.write((byte)(value >>> 48));
//    out.write((byte)(value >>> 40));
//    out.write((byte)(value >>> 32));
//    out.write((byte)(value >>> 24));
//    out.write((byte)(value >>> 16));
//    out.write((byte)(value >>> 8));
//    out.write((byte)(value >>> 0));
//  }

  /**
   * Writes a int value to the specified byte []
   *
   * @param buffer OutputWtream to write to value to
   * @param offset position in buffer to write value to
   * @param value  value to write to byte []
   */
//  public static void writeInt(byte[] buffer, int offset, int value) {
//    buffer[offset + 0] = (byte)(value >> 24);
//    buffer[offset + 1] = (byte)(value >> 16);
//    buffer[offset + 2] = (byte)(value >> 8);
//    buffer[offset + 3] = (byte)value;
//  }

  /**
   * Writes a short value to the specified byte []
   *
   * @param buffer OutputWtream to write to value to
   * @param offset position in buffer to write value to
   * @param value  value to write to byte []
   */
//  public static void writeShort(byte[] buffer, int offset, int value) {
//    buffer[offset + 0] = (byte)(value >> 8);
//    buffer[offset + 1] = (byte)value;
//  }

  /**
   * Writes an int value to the specified output stream
   *
   * @param out   OutputWtream to write to value to
   * @param value value to write to OutputStream
   */
//  public static void writeInt(OutputStream out, int value) throws IOException {
//    out.write(value >> 24);
//    out.write(value >> 16);
//    out.write(value >> 8);
//    out.write(value);
//  }

  /**
   * Writes a short value to the specified output stream
   *
   * @param out   OutputWtream to write to value to
   * @param value value to write to OutputStream
   */
//  public static void writeShort(OutputStream out, short value) throws IOException {
//    out.write((value >>> 8) & 0xFF);
//    out.write((value >>> 0) & 0xFF);
//  }

  /**
   * Writes a byte value to the specified output stream
   *
   * @param out   OutputWtream to write to value to
   * @param value value to write to OutputStream
   */
//  public static void writeByte(ByteArrayOutputStream out, int value) {
//    out.write(value);
//  }
//
//  public static long readLong(byte[] buffer, int offset) {
//    final long value =
//      (((long)buffer[offset + 0]) << 56) +
//        (((long)buffer[offset + 1] & 0xFF) << 48) +
//        (((long)buffer[offset + 2] & 0xFF) << 40) +
//        (((long)buffer[offset + 3] & 0xFF) << 32) +
//        (((long)buffer[offset + 4] & 0xFF) << 24) +
//        ((buffer[offset + 5] & 0xFF) << 16) +
//        ((buffer[offset + 6] & 0xFF) << 8) +
//        (buffer[offset + 7] & 0xFF);
//    return value;
//  }

//  public static long readLong(InputStream inStream) throws IOException {
//    final long value =
//      (((long)inStream.read() & 0xFF) << 56) +
//        (((long)inStream.read() & 0xFF) << 48) +
//        (((long)inStream.read() & 0xFF) << 40) +
//        (((long)inStream.read() & 0xFF) << 32) +
//        (((long)inStream.read() & 0xFF) << 24) +
//        ((inStream.read() & 0xFF) << 16) +
//        ((inStream.read() & 0xFF) << 8) +
//        (inStream.read() & 0xFF);
//    return value;
//  }


//  public static int readInt(byte[] buffer, int offset) {
//    final int value =
//      ((buffer[offset + 0] & 0xFF) << 24) +
//        ((buffer[offset + 1] & 0xFF) << 16) +
//        ((buffer[offset + 2] & 0xFF) << 8) +
//        (buffer[offset + 3] & 0xFF);
//    return value;
//  }

//  public static int readInt(InputStream inStream) throws IOException {
//    final int value =
//      ((inStream.read() & 0xFF) << 24) +
//        ((inStream.read() & 0xFF) << 16) +
//        ((inStream.read() & 0xFF) << 8) +
//        (inStream.read() & 0xFF);
//    return value;
//  }
//
//  public static short readShort(byte[] buffer, int offset) {
//    int highVal = (int)(buffer[offset]);
//    highVal = highVal & 0x000000ff;
//    highVal = highVal << 8;
//    int lowVal = (int)(buffer[offset + 1]);
//    lowVal = lowVal & 0x000000ff;
//    final int result = highVal + lowVal;
//    return (short)(result);
//
//    // return (short)((buffer[offset] << 8) + (buffer[offset + 1] << 0));
//  }

}
