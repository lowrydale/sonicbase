package com.lowryengineering.database.util;

/**
 * User: lowryda
 * Date: 9/30/14
 * Time: 7:38 PM
 */
/**
 * User: lowryda
 * Date: 3/28/14
 * Time: 9:41 AM
 */
public class SerializationHelper {

//  public static void writeRawLittleEndian32(final int value, byte[] buffer) throws IOException {
//    writeRawLittleEndian32(value, buffer, 0);
//  }
//
//  public static void writeRawLittleEndian32(final int value, byte[] buffer, int offset) throws IOException {
//    buffer[offset + 0] = (byte)((value      ) & 0xFF);
//    buffer[offset + 1] = (byte)((value >>  8) & 0xFF);
//    buffer[offset + 2] = (byte)((value >> 16) & 0xFF);
//    buffer[offset + 3] = (byte)((value >> 24) & 0xFF);
//  }
//
//  public static void writeRawLittleEndian64(final long value, byte[] buffer) throws IOException {
//    buffer[0] = (byte)((value      ) & 0xFF);
//    buffer[1] = (byte)((value >>  8) & 0xFF);
//    buffer[2] = (byte)((value >> 16) & 0xFF);
//    buffer[3] = (byte)((value >> 24) & 0xFF);
//    buffer[4] = (byte)((value >> 32) & 0xFF);
//    buffer[5] = (byte)((value >> 40) & 0xFF);
//    buffer[6] = (byte)((value >> 48) & 0xFF);
//    buffer[7] = (byte)((value >> 56) & 0xFF);
//  }
//
//  public static int readRawLittleEndian32(byte[] buffer) {
//    return readRawLittleEndian32(buffer, 0);
//  }
//
//  public static int readRawLittleEndian32(byte[] buffer, int offset) {
//    return (((int)buffer[0 + offset] & 0xff)      ) |
//           (((int)buffer[1 + offset] & 0xff) <<  8) |
//           (((int)buffer[2 + offset] & 0xff) << 16) |
//           (((int)buffer[3 + offset] & 0xff) << 24);
//  }
//
//  public static int readRawLittleEndian32(ByteBuf buff) {
//    final byte b1 = buff.readByte();
//    final byte b2 = buff.readByte();
//    final byte b3 = buff.readByte();
//    final byte b4 = buff.readByte();
//    return (((int)b1 & 0xff)      ) |
//           (((int)b2 & 0xff) <<  8) |
//           (((int)b3 & 0xff) << 16) |
//           (((int)b4 & 0xff) << 24);
//  }
//
//  public static long readRawLittleEndian64(byte[] buffer) {
//    return (((long)buffer[0] & 0xff)      ) |
//           (((long)buffer[1] & 0xff) <<  8) |
//           (((long)buffer[2] & 0xff) << 16) |
//           (((long)buffer[3] & 0xff) << 24) |
//            (((long)buffer[4] & 0xff) << 32) |
//            (((long)buffer[5] & 0xff) << 40) |
//            (((long)buffer[6] & 0xff) << 48) |
//            (((long)buffer[7] & 0xff) << 56);
//  }
}
