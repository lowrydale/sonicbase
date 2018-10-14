package com.sonicbase.common;

import sun.misc.Unsafe;

import java.nio.ByteBuffer;

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class DataUtils {

  private DataUtils() {
  }

  public static int bytesToInt(byte[] bytes, int offset) {
    return bytes[offset] << 24 |
        (bytes[1 + offset] & 0xFF) << 16 |
        (bytes[2 + offset] & 0xFF) << 8 |
        (bytes[3 + offset] & 0xFF);
  }

  public static int addressToInt(long address, Unsafe unsafe) {
    return unsafe.getByte(address) << 24 |
        (unsafe.getByte(address + 1) & 0xFF) << 16 |
        (unsafe.getByte(address + 2) & 0xFF) << 8 |
        (unsafe.getByte(address + 3) & 0xFF);
  }

  public static long addressToLong(long address, Unsafe unsafe) {
    return unsafe.getByte(address) << 56 |
        (unsafe.getByte(address + 1) & 0xFF) << 48 |
        (unsafe.getByte(address + 2) & 0xFF) << 40 |
        (unsafe.getByte(address + 3) & 0xFF) << 32 |
        (unsafe.getByte(address + 4) & 0xFF) << 24 |
        (unsafe.getByte(address + 5) & 0xFF) << 16 |
        (unsafe.getByte(address + 6) & 0xFF) << 8 |
        (unsafe.getByte(address + 7) & 0xFF);
  }

  public static short bytesToShort(byte[] bytes, int offset) {
    return (short) (bytes[offset]<<8 | bytes[1 + offset] & 0xFF);
  }

  public static void shortToBytes(short value, byte[] array, int offset) {
    array[offset] = (byte)(0xff & (value >> 8));
    array[offset+1] = (byte)(0xff & value);
  }

  public static void intToBytes(int value, byte[] array, int offset) {
    array[offset]   = (byte)(0xff & (value >> 24));
    array[offset+1] = (byte)(0xff & (value >> 16));
    array[offset+2] = (byte)(0xff & (value >> 8));
    array[offset+3] = (byte)(0xff & value);
  }

  public static void intToAddress(int value, long address, Unsafe unsafe) {
    unsafe.putByte(address, (byte)(0xff & (value >> 24)));
    unsafe.putByte(address + 1, (byte)(0xff & (value >> 16)));
    unsafe.putByte(address + 2, (byte)(0xff & (value >> 8)));
    unsafe.putByte(address + 3, (byte)(0xff & value));
  }

  public static void longToAddress(long value, long address, Unsafe unsafe) {
    unsafe.putByte(address, (byte)(0xff & (value >> 56)));
    unsafe.putByte(address + 1, (byte)(0xff & (value >> 48)));
    unsafe.putByte(address + 2, (byte)(0xff & (value >> 40)));
    unsafe.putByte(address + 3, (byte)(0xff & (value >> 32)));
    unsafe.putByte(address + 4, (byte)(0xff & (value >> 24)));
    unsafe.putByte(address + 5, (byte)(0xff & (value >> 16)));
    unsafe.putByte(address + 6, (byte)(0xff & (value >> 8)));
    unsafe.putByte(address + 7, (byte)(0xff & value));
  }

  public static void longToBytes(long value, byte[] array, int offset) {
    array[offset]   = (byte)(0xff & (value >> 56));
    array[offset+1] = (byte)(0xff & (value >> 48));
    array[offset+2] = (byte)(0xff & (value >> 40));
    array[offset+3] = (byte)(0xff & (value >> 32));
    array[offset+4] = (byte)(0xff & (value >> 24));
    array[offset+5] = (byte)(0xff & (value >> 16));
    array[offset+6] = (byte)(0xff & (value >> 8));
    array[offset+7] = (byte)(0xff & value);
  }

  public static byte[] longToBytes(long l) {
    byte[] result = new byte[8];
    for (int i = 7; i >= 0; i--) {
      result[i] = (byte)(l & 0xFF);
      l >>= 8;
    }
    return result;
  }

  public static long bytesToLong(byte[] b, int offset) {
    long result = 0;
    for (int i = 0; i < 8; i++) {
      result <<= 8;
      result |= (b[i + offset] & 0xFF);
    }
    return result;
  }

  public static void bytesToAddress(byte[] bytes, int length, long address, Unsafe unsafe) {
    for (int i = 0; i < length; i++) {
      unsafe.putByte(address + i, bytes[i]);
    }

  }
}
