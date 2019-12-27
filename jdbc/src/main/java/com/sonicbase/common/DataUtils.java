package com.sonicbase.common;

import sun.misc.Unsafe;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

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

  public static char bytesToChar(byte[] bytes, int offset) {
      int ch1 = bytes[offset++];
      int ch2 = bytes[offset++];
      return (char)((ch1 << 8) + (ch2 << 0));
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

  public final void charToBytes(char v, byte[] array, int offset) throws IOException {
    array[offset] = (byte)((v >>> 8) & 0xFF);
    array[offset+1] = (byte)((v >>> 0) & 0xFF);
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


  public static long readSignedVarLong(byte[] bytes, int[] offset) {
    long raw = readUnsignedVarLong(bytes, offset);
    long temp = (((raw << 63) >> 63) ^ raw) >> 1;
    return temp ^ (raw & (1L << 63));
  }

  public static long readUnsignedVarLong(byte[] bytes, int[] offset) {
    long tmp;
    // CHECKSTYLE: stop InnerAssignment
    if ((tmp = bytes[offset[0]++]) >= 0) {
      return tmp;
    }
    long result = tmp & 0x7f;
    if ((tmp = bytes[offset[0]++]) >= 0) {
      result |= tmp << 7;
    } else {
      result |= (tmp & 0x7f) << 7;
      if ((tmp = bytes[offset[0]++]) >= 0) {
        result |= tmp << 14;
      } else {
        result |= (tmp & 0x7f) << 14;
        if ((tmp = bytes[offset[0]++]) >= 0) {
          result |= tmp << 21;
        } else {
          result |= (tmp & 0x7f) << 21;
          if ((tmp = bytes[offset[0]++]) >= 0) {
            result |= tmp << 28;
          } else {
            result |= (tmp & 0x7f) << 28;
            if ((tmp = bytes[offset[0]++]) >= 0) {
              result |= tmp << 35;
            } else {
              result |= (tmp & 0x7f) << 35;
              if ((tmp = bytes[offset[0]++]) >= 0) {
                result |= tmp << 42;
              } else {
                result |= (tmp & 0x7f) << 42;
                if ((tmp = bytes[offset[0]++]) >= 0) {
                  result |= tmp << 49;
                } else {
                  result |= (tmp & 0x7f) << 49;
                  if ((tmp = bytes[offset[0]++]) >= 0) {
                    result |= tmp << 56;
                  } else {
                    result |= (tmp & 0x7f) << 56;
                    result |= ((long) bytes[offset[0]++]) << 63;
                  }
                }
              }
            }
          }
        }
      }
    }
    return result;
  }

  private static void writeVarLong(
      long value, byte[] bytes, int[] offset) throws IOException {
    while (true) {
      int bits = ((int) value) & 0x7f;
      value >>>= 7;
      if (value == 0) {
        bytes[offset[0]++] = (byte)bits;
        return;
      }
      bytes[offset[0]++] = (byte) (bits | 0x80);
    }
  }


  public static void writeSignedVarLong(long value, byte[] bytes, int[] offset) throws IOException {
    value = (value << 1);
    long tmp = (value >> 63);
    writeVarLong(value ^ tmp, bytes, offset);
  }

  public static int bytesToUnsignedShort(byte[] bytes, int[] offset) {
    int ch1 = bytes[offset[0]++];
    int ch2 = bytes[offset[0]++];
    return (ch1 << 8) + (ch2 << 0);
  }

  public static String bytesToUTF(byte[] bytes, int[] offset) throws UTFDataFormatException {
    int utflen = bytesToUnsignedShort(bytes, offset);
    byte[] bytearr = null;
    char[] chararr = null;

    bytearr = new byte[utflen];
    chararr = new char[utflen];

    int c, char2, char3;
    int count = 0;
    int chararr_count=0;

    System.arraycopy(bytes, offset[0], bytearr, 0, utflen);
    offset[0] += utflen;

    while (count < utflen) {
      c = (int) bytearr[count] & 0xff;
      if (c > 127) break;
      count++;
      chararr[chararr_count++]=(char)c;
    }

    while (count < utflen) {
      c = (int) bytearr[count] & 0xff;
      switch (c >> 4) {
        case 0: case 1: case 2: case 3: case 4: case 5: case 6: case 7:
          /* 0xxxxxxx*/
          count++;
          chararr[chararr_count++]=(char)c;
          break;
        case 12: case 13:
          /* 110x xxxx   10xx xxxx*/
          count += 2;
          if (count > utflen)
            throw new UTFDataFormatException(
                "malformed input: partial character at end");
          char2 = (int) bytearr[count-1];
          if ((char2 & 0xC0) != 0x80)
            throw new UTFDataFormatException(
                "malformed input around byte " + count);
          chararr[chararr_count++]=(char)(((c & 0x1F) << 6) |
              (char2 & 0x3F));
          break;
        case 14:
          /* 1110 xxxx  10xx xxxx  10xx xxxx */
          count += 3;
          if (count > utflen)
            throw new UTFDataFormatException(
                "malformed input: partial character at end");
          char2 = (int) bytearr[count-2];
          char3 = (int) bytearr[count-1];
          if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80))
            throw new UTFDataFormatException(
                "malformed input around byte " + (count-1));
          chararr[chararr_count++]=(char)(((c     & 0x0F) << 12) |
              ((char2 & 0x3F) << 6)  |
              ((char3 & 0x3F) << 0));
          break;
        default:
          /* 10xx xxxx,  1111 xxxx */
          throw new UTFDataFormatException(
              "malformed input around byte " + count);
      }
    }
    // The number of chars produced may be less than utflen
    return new String(chararr, 0, chararr_count);
  }
}
