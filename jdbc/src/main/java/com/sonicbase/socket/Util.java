package com.sonicbase.socket;

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class Util {

  private Util() {
  }

  public static void writeRawLittleEndian32(final int value, byte[] buffer) {
    buffer[0] = (byte)((value      ) & 0xFF);
    buffer[1] = (byte)((value >>  8) & 0xFF);
    buffer[2] = (byte)((value >> 16) & 0xFF);
    buffer[3] = (byte)((value >> 24) & 0xFF);
  }

  public static void writeRawLittleEndian64(final long value, byte[] buffer) {
    buffer[0] = (byte)((value      ) & 0xFF);
    buffer[1] = (byte)((value >>  8) & 0xFF);
    buffer[2] = (byte)((value >> 16) & 0xFF);
    buffer[3] = (byte)((value >> 24) & 0xFF);
    buffer[4] = (byte)((value >> 32) & 0xFF);
    buffer[5] = (byte)((value >> 40) & 0xFF);
    buffer[6] = (byte)((value >> 48) & 0xFF);
    buffer[7] = (byte)((value >> 56) & 0xFF);
  }

  public static int readRawLittleEndian32(byte[] buffer) {
    return readRawLittleEndian32(buffer, 0);
  }
  public static int readRawLittleEndian32(byte[] buffer, int offset) {
    return ((int)buffer[0 + offset] & 0xff)       |
           ((int)buffer[1 + offset] & 0xff) <<  8 |
           ((int)buffer[2 + offset] & 0xff) << 16 |
           ((int)buffer[3 + offset] & 0xff) << 24;
  }

  public static long readRawLittleEndian64(byte[] buffer) { return readRawLittleEndian64(buffer, 0); }
  public static long readRawLittleEndian64(byte[] buffer, int offset) {
    return ((long)buffer[0 + offset] & 0xff)       |
           ((long)buffer[1 + offset] & 0xff) <<  8 |
           ((long)buffer[2 + offset] & 0xff) << 16 |
           ((long)buffer[3 + offset] & 0xff) << 24 |
            ((long)buffer[4 + offset] & 0xff) << 32 |
            ((long)buffer[5 + offset] & 0xff) << 40 |
            ((long)buffer[6 + offset] & 0xff) << 48 |
            ((long)buffer[7 + offset] & 0xff) << 56;
  }
}
