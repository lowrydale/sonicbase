package com.sonicbase.database;

import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.common.Record;
import com.sonicbase.index.Index;
import com.sonicbase.util.DateUtils;
import org.apache.giraph.utils.Varint;
import org.testng.annotations.Test;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.assertEquals;

/**
 * Created by lowryda on 6/17/17.
 */
public class TestLite {

  @Test
  public  void testDateFormat() throws ParseException {
    Date date = new Date(System.currentTimeMillis());
    String str = DateUtils.fromDate(date);
    System.out.println(str);
    date = DateUtils.fromString(str);
    System.out.println(date.getYear() + "-" + date.getMonth() + "-" + date.getDate() + ", " + date.getHours() + ":" +
      date.getMinutes() + ":" + date.getSeconds());
  }

  @Test
  public void testVarint() throws IOException {
    long len = Varint.sizeOfSignedVarLong(125);
    System.out.println(len);

  }
  @Test
  public void testDelete() throws InterruptedException {
    final ConcurrentSkipListMap<Long, Long> map = new ConcurrentSkipListMap<>();
    for (int i = 0; i < 1_000_000; i++) {
      map.put((long)i, (long)i);
    }
    final AtomicInteger offset = new AtomicInteger(999_999);
    Thread readThread =new Thread(new Runnable(){
      @Override
      public void run() {
        boolean first = true;
        int currOffset = offset.get();
        while (true) {
//          try {
//            Thread.sleep(1);
//          }
//          catch (InterruptedException e) {
//            break;
//          }
          if (map.get((long)offset.get()) != null) {
            System.out.println("Not missing: " + offset.get());
          }
          if (map.get((long)offset.get()) == null && first) {
            System.out.println("Missing: " + offset.get());
            first = false;
          }
          else {
          }
          if (currOffset != offset.get()) {
            currOffset = offset.get();
            first = true;
          }
        }
      }
    });
    readThread.start();
    Thread thread = new Thread(new Runnable(){
      @Override
      public void run() {
        while (true) {
          map.remove((long)offset.decrementAndGet());
          try {
            Thread.sleep(5);
          }
          catch (InterruptedException e) {
            break;
          }
          System.out.println("Changed offset: " + (offset.get() - 1));
          //offset.decrementAndGet();
        }

      }
    });
    thread.start();

    Thread.sleep(110000000L);
  }

  @Test
  public void testKeyHash() {
    for (int i = 0; i < 1_000_000; i++) {
      Object[] key1 = new Object[]{(long)i};
      Object[] key2 = new Object[]{(long)i};
      assertEquals(Index.hashCode(key1), Index.hashCode(key2));

      Object[] key3 = new Object[]{(long)i, ("hey there" + i).getBytes()};
      Object[] key4 = new Object[]{(long)i, ("hey there" + i).getBytes()};
      assertEquals(Index.hashCode(key3), Index.hashCode(key4));
    }
  }

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
    System.out.println(DateUtils.toString(new Date(System.currentTimeMillis())));
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

  @Test
  public void testRecord() {
    byte[] bytes = new byte[100];
    Record.setDbViewNumber(bytes, 100);

    assertEquals(Record.getDbViewNumber(bytes), 100);

    Record.setDbViewFlags(bytes, (short)1);

    assertEquals(Record.getDbViewFlags(bytes), 1);
  }
}
