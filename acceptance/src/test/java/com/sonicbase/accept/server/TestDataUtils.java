package com.sonicbase.accept.server;

import org.testng.annotations.Test;

import java.io.*;

public class TestDataUtils {


  public static int bytesToInt(byte[] bytes, int offset) {
    return bytes[0 + offset] << 24 |
        (bytes[1 + offset] & 0xFF) << 16 |
        (bytes[2 + offset] & 0xFF) << 8 |
        (bytes[3 + offset] & 0xFF);
  }


  public static long bytesToLong(byte[] b, int offset) {
    long result = 0;
    for (int i = 0; i < 8; i++) {
      result <<= 8;
      result |= (b[i + offset] & 0xFF);
    }
    return result;
  }
  @Test
  public void test() throws IOException {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytesOut);

    for (int i = 0; i < 100_000; i++) {
      out.writeLong(999L);
      out.writeInt(999);
    }
    out.close();

    DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray()));
    long begin = System.currentTimeMillis();
    for (int i = 0; i < 100_000; i++) {
      in.readLong();
      in.readInt();
    }
    System.out.println("stream duration: " + (System.currentTimeMillis() - begin));

    byte[] bytes = bytesOut.toByteArray();
    begin = System.currentTimeMillis();
    for (int i = 0; i < 100_000; i++) {
      bytesToLong(bytes, 0);
      bytesToInt(bytes, 8);
    }
    System.out.println("direct duration: " + (System.currentTimeMillis() - begin));
  }

}
