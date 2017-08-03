package com.sonicbase.database;

import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.util.ISO8601;
import org.testng.annotations.Test;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.util.Date;

/**
 * Created by lowryda on 6/17/17.
 */
public class TestLite {

  @Test
  public void testSocket1() {
    long begin = System.currentTimeMillis();
    try {
      SocketChannel sock = SocketChannel.open();
      InetSocketAddress address = new InetSocketAddress("10.0.0.7", 9010);
      sock.connect(address);
    }
    catch (Exception e) {
      System.out.println("duration=" + (System.currentTimeMillis() - begin));
      e.printStackTrace();
    }
  }

  @Test
  public void testSocket2() {
    long begin = System.currentTimeMillis();
    try {
      Socket client = new Socket();
      client.setSoTimeout(20_000);
      client.connect(new InetSocketAddress("127.0.0.1", 9010), 5000);
      client.setKeepAlive(true);
    }
    catch (Exception e) {
      System.out.println("duration=" + (System.currentTimeMillis() - begin));
      e.printStackTrace();
    }

}
  @Test
  public void test() {
    System.out.println(ISO8601.to8601String(new Date(System.currentTimeMillis())));
  }

  @Test
  public void testSchema() throws InterruptedException, IOException {

    File schemaFile = new File(System.getProperty("user.home"), "tmp/schema.bin");
    try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(schemaFile)))) {
      DatabaseCommon common = new DatabaseCommon();
      common.deserializeSchema(in);

      Thread.sleep(200000);
    }

  }
}
