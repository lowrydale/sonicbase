/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.common;

import sun.misc.Unsafe;

public class DataUtils {

  public static int bytesToInt(byte[] bytes, int offset) {
    return bytes[0 + offset] << 24 |
        (bytes[1 + offset] & 0xFF) << 16 |
        (bytes[2 + offset] & 0xFF) << 8 |
        (bytes[3 + offset] & 0xFF);
  }

  public static int addressToInt(long address, Unsafe unsafe) {
    return unsafe.getByte(address + 0) << 24 |
        (unsafe.getByte(address + 1) & 0xFF) << 16 |
        (unsafe.getByte(address + 2) & 0xFF) << 8 |
        (unsafe.getByte(address + 3) & 0xFF);

  }

  public static short bytesToShort(byte[] bytes, int offset) {
    return (short) (bytes[0 + offset]<<8 | bytes[1 + offset] & 0xFF);
  }

  public static void intToBytes(int value, byte[] array, int offset) {
    array[offset]   = (byte)(0xff & (value >> 24));
    array[offset+1] = (byte)(0xff & (value >> 16));
    array[offset+2] = (byte)(0xff & (value >> 8));
    array[offset+3] = (byte)(0xff & value);
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
}
