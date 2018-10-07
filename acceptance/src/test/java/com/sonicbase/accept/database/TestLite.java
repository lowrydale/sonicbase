package com.sonicbase.accept.database;

import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.common.Record;
import com.sonicbase.index.Index;
import com.sonicbase.schema.DataType;
import com.sonicbase.util.DateUtils;
import org.apache.giraph.utils.Varint;
import org.testng.annotations.Test;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.assertEquals;

public class TestLite {

  @Test
  public void testFileDate() throws IOException {
    File file = new File(System.getProperty("user.hom"), "/tmp/test.txt");
    file.getParentFile().mkdirs();
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)));
    writer.write("test");
    writer.close();
    System.out.println(file.lastModified());
  }

  @Test
  public void testBoundaryTimestamp() {
    TimeZone tz = TimeZone.getTimeZone("UTC");
    SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS GG");
    format1.setTimeZone(tz);

    Calendar cal = Calendar.getInstance();
    cal.set(0, 11, 31, 17, 0, 0);
    Timestamp timestamp = new Timestamp(cal.getTimeInMillis());
    System.out.println(format1.format(timestamp.getTime()));
    System.out.println(DateUtils.toDbTimestampString(timestamp));
    System.out.println("");
    Calendar cal2 = Calendar.getInstance();
    cal2.set(-1, 11, 31, 17, 0, 0);
    Timestamp timestamp2 = new Timestamp(cal2.getTimeInMillis());
    System.out.println(format1.format(timestamp2.getTime()));
    System.out.println(DateUtils.toDbTimestampString(timestamp2));

    if (timestamp2.compareTo(timestamp) > 0) {
      System.out.println("A - greater");
    }
    else {
      System.out.println("A - less");
    }
    cal2.set(-2, 11, 30, 16, 59, 59);
    timestamp2 = new Timestamp(cal2.getTimeInMillis());
    System.out.println(format1.format(cal2.getTimeInMillis()));
    System.out.println(DateUtils.toDbTimestampString(timestamp2));

    if (timestamp2.compareTo(timestamp) < 0) {
      System.out.println("B - less");
    }
    else {
      System.out.println("B - greater");
    }
  }

  @Test
  public void testBoundary() {
    TimeZone tz = TimeZone.getTimeZone("UTC");
    SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS GG");
    format1.setTimeZone(tz);

    Calendar cal = Calendar.getInstance();
    cal.set(0, 11, 31, 17, 0, 0);
    System.out.println(format1.format(cal.getTimeInMillis()));
    System.out.println(DateUtils.toDbString(cal));
    System.out.println("");
    Calendar cal2 = Calendar.getInstance();
    cal2.set(-1, 11, 31, 17, 0, 0);
    System.out.println(format1.format(cal2.getTimeInMillis()));
    System.out.println(DateUtils.toDbString(cal2));

    if (cal2.compareTo(cal) > 0) {
      System.out.println("A - greater");
    }
    else {
      System.out.println("A - less");
    }
    cal2.set(-2, 11, 30, 16, 59, 59);
    System.out.println(format1.format(cal2.getTimeInMillis()));
    System.out.println(DateUtils.toDbString(cal2));

    if (cal2.compareTo(cal) < 0) {
      System.out.println("B - less");
    }
    else {
      System.out.println("B - greater");
    }
  }

  @Test
  public void testOldDate() throws ParseException {
    TimeZone tz = TimeZone.getTimeZone("UTC");
    SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS GG");
    format1.setTimeZone(tz);

    Calendar cal = Calendar.getInstance();
    cal.setTime(format1.parse("0056-12-11 12:03:33.700 AD"));
    System.out.println(cal.getTimeInMillis());
    System.out.println(format1.format(cal.getTimeInMillis()));
  }

  @Test
  public void testDateAgain() throws ParseException {
    Calendar cal = DateUtils.fromDbCalString("2009-12-1");

    java.sql.Date date = new java.sql.Date(new SimpleDateFormat("yyyy-mm-dd").parse("2009-09-09").getTime());
    System.out.println(date.getYear());
    java.sql.Date date2 = new java.sql.Date(new SimpleDateFormat("yyyy-mm-dd").parse("2008-09-09").getTime());
    System.out.println(date2.getYear());

    assertEquals(DataType.getDateComparator().compare(date, date2), 1);

    System.out.println(cal.get(Calendar.YEAR));

    System.out.println(DateUtils.toDbString(cal));
  }

  @Test
  public void testDate() throws ParseException {
    Calendar cal = Calendar.getInstance();
    cal.set(Calendar.HOUR_OF_DAY, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 123);

    TimeZone tz = TimeZone.getTimeZone("UTC");
    SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    format1.setTimeZone(tz);

    String formatted = format1.format(cal.getTime());
    System.out.println("formatted: " + formatted);

    cal = Calendar.getInstance();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.ENGLISH);
    sdf.setTimeZone(tz);
    cal.setTime(sdf.parse(formatted));

    formatted = format1.format(cal.getTime());
    System.out.println("parsed and formatted: " + formatted);

    cal = DateUtils.fromDbCalString("2017-11-27 12:30:10.112-0700 BC");

    System.out.println("fromDbCalString: " + cal.getTimeInMillis() + " " + DateUtils.toDbString(cal));

    cal = DateUtils.fromDbCalString("2017-11-27 12:30:10-0700");

    System.out.println("fromDbCalString: " + DateUtils.toDbString(cal));

    cal = DateUtils.fromDbCalString("2017-11-27 12:30:10+0000");

    System.out.println("fromDbCalString: " + DateUtils.toDbString(cal));

    cal = DateUtils.fromDbCalString("2017-11-27 12:30-0700");

    System.out.println("fromDbCalString: " + DateUtils.toDbString(cal));

    cal = DateUtils.fromDbCalString("2017-11-27 12:30");

    System.out.println("fromDbCalString: " + DateUtils.toDbString(cal));
  }

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
    Thread readThread =new Thread(() -> {
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
    });
    readThread.start();
    Thread thread = new Thread(() -> {
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
      client.connect(new InetSocketAddress("localhost", 9010), 5000);
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
