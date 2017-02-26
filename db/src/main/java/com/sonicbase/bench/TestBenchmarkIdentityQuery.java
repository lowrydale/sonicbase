package com.sonicbase.bench;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.sonicbase.util.JsonArray;
import com.sonicbase.util.JsonDict;
import com.sonicbase.util.StreamUtils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.testng.Assert.*;

public class TestBenchmarkIdentityQuery {

  private static final MetricRegistry METRICS = new MetricRegistry();

  public static final com.codahale.metrics.Timer LOOKUP_STATS = METRICS.timer("lookup");

  public static void main(String[] args) throws Exception {

    final long startId = Long.valueOf(args[0]);
    final String cluster = args[1];
    final String queryType = args[2];
    //   FileUtils.deleteDirectory(new File("/data/database"));

    System.out.println("Starting client");

    File file = new File(System.getProperty("user.dir"), "config/config-" + cluster + ".json");
    if (!file.exists()) {
      file = new File(System.getProperty("user.dir"), "db/src/main/resources/config/config-" + cluster + ".json");
      System.out.println("Loaded config resource dir");
    }
    else {
      System.out.println("Loaded config default dir");
    }
    String configStr = StreamUtils.inputStreamToString(new BufferedInputStream(new FileInputStream(file)));

    JsonDict dict = new JsonDict(configStr);
    JsonDict databaseDict = dict.getDict("database");
    JsonArray array = databaseDict.getArray("shards");
    JsonDict replica = array.getDict(0);
    JsonArray replicasArray = replica.getArray("replicas");
    String address = replicasArray.getDict(0).getString("publicAddress");
    System.out.println("Using address: address=" + address);

    Class.forName("com.sonicbase.jdbcdriver.Driver");

//       final java.sql.Connection conn = DriverManager.getConnection("jdbc:voltdb://localhost:21212");

//    final java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:mysql://localhost/test", "test", "test");
//    final java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:mysql://127.0.0.1:4306/test", "test", "test");

    //54.173.145.214
    final java.sql.Connection conn = DriverManager.getConnection("jdbc:sonicbase:" + address + ":9010/db", "user", "password");

    //test insert
    final AtomicLong totalSelectDuration = new AtomicLong();

    final AtomicLong selectErrorCount = new AtomicLong();
    final AtomicLong selectBegin = new AtomicLong(System.currentTimeMillis());
    final AtomicLong selectOffset = new AtomicLong();


    final AtomicInteger logMod = new AtomicInteger(10000);
    int threadCount = 256;
    if (queryType.equals("limitOffset") || queryType.equals("sort")) {
      threadCount = 4;
    }
    if (queryType.equals("equalNonIndex") || queryType.equals("orTableScan")) {
      threadCount = 4;
    }
    Thread[] threads = new Thread[threadCount];
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            outer:
            while (true) {
              try {
                long offset = startId;
                byte[] bytes = new byte[100];
                for (int i = 0; i < bytes.length; i++) {
                  bytes[i] = (byte) ThreadLocalRandom.current().nextInt(256);
                }
                while (true) {
                  long beginSelect = System.nanoTime();

                  if (queryType.equals("id")) {
                    PreparedStatement stmt = conn.prepareStatement("select id1  " +
                        "from persons where persons.id1=?");
                    int innerOffset = 0;
                    while (true) {
                      long begin = System.nanoTime();
                      //Timer.Context ctx = LOOKUP_STATS.time();
                      stmt.setLong(1, offset);
                      ResultSet rs = stmt.executeQuery();
                      boolean found = rs.next();
                      //ctx.stop();
                      if (!found) {
                        break;
                      }
                      totalSelectDuration.addAndGet(System.nanoTime() - begin);

                      if (selectOffset.incrementAndGet() % 10000 == 0) {
                        logProgress(selectOffset, selectErrorCount, selectBegin, totalSelectDuration);
                      }
                    }

                  }
                  else if (queryType.equals("twoFieldId")) {

                    boolean missing = false;
                    for (int i = 0; i < 5; i++) {
                      Timer.Context ctx = LOOKUP_STATS.time();
                      PreparedStatement stmt = conn.prepareStatement("select id1, id2  " +
                          "from persons where id1=? and id2=?");                                              //
                      stmt.setLong(1, offset);
                      stmt.setLong(2, 0);
                      ResultSet ret = stmt.executeQuery();
                      if (ret.next()) {
                        missing = false;
                        assertEquals(ret.getLong("id1"), offset);
                        assertEquals(ret.getLong("id2"), 0);
                        ctx.stop();
                        break;
                      }
                      else {
                        offset++;
                        missing = true;
                        ctx.stop();
                      }
                    }

                    if (missing) {
                      System.out.println("max=" + offset);
                      break;
                    }
                  }
                  else if (queryType.equals("max")) {
                    Timer.Context ctx = LOOKUP_STATS.time();
                    PreparedStatement stmt = conn.prepareStatement("select max(id1) as maxValue from persons");
                    ResultSet ret = stmt.executeQuery();

                    assertTrue(ret.next());
                    assertFalse(ret.next());
                    logMod.set(1);
                    ctx.stop();
                  }
                  else if (queryType.equals("maxTableScan")) {
                    Timer.Context ctx = LOOKUP_STATS.time();
                    PreparedStatement stmt = conn.prepareStatement("select max(id1) as maxValue from persons where id2 < 1");
                    ResultSet ret = stmt.executeQuery();

                    assertTrue(ret.next());
                    ctx.stop();
                  }
                  else if (queryType.equals("maxWhere")) {
                    Timer.Context ctx = LOOKUP_STATS.time();
                    PreparedStatement stmt = conn.prepareStatement("select max(id1) as maxValue from persons where id1 < 100000");
                    ResultSet ret = stmt.executeQuery();

                    assertTrue(ret.next());
                    assertFalse(ret.next());
                    ctx.stop();
                  }
                  else if (queryType.equals("sum")) {
                    Timer.Context ctx = LOOKUP_STATS.time();
                    PreparedStatement stmt = conn.prepareStatement("select sum(id1) as sumValue from persons");
                    ResultSet ret = stmt.executeQuery();

                    assertTrue(ret.next());
                    assertFalse(ret.next());
                    ctx.stop();
                  }
                  else if (queryType.equals("limit")) {
                    Timer.Context ctx = LOOKUP_STATS.time();
                    PreparedStatement stmt = conn.prepareStatement("select * from persons where id1 < ? and id1 > ? limit 3");
                    stmt.setLong(1, offset);
                    stmt.setLong(2, 2);
                    ResultSet ret = stmt.executeQuery();

                    assertTrue(ret.next());
                    ctx.stop();
                  }
                  else if (queryType.equals("limitOffset")) {
                    Timer.Context ctx = LOOKUP_STATS.time();
                    PreparedStatement stmt = conn.prepareStatement("select * from persons where id1 < ? and id1 > ? limit 3 offset 2");
                    stmt.setLong(1, offset);
                    stmt.setLong(2, offset / 2);
                    ResultSet ret = stmt.executeQuery();

                    ret.next();
                    ctx.stop();

                    int innerOffset = 0;
                    while (true) {
                      ctx = LOOKUP_STATS.time();
                      boolean found = ret.next();
                      ctx.stop();
                      if (!found) {
                        break;
                      }
                      if (++innerOffset % 10000 == 0) {
                        logProgress(selectOffset, selectErrorCount, selectBegin, totalSelectDuration);
                      }
                    }
                  }
                  else if (queryType.equals("sort")) {
                    Timer.Context ctx = LOOKUP_STATS.time();
                    PreparedStatement stmt = conn.prepareStatement("select * from persons order by id2 asc, id1 desc");
                    ResultSet ret = stmt.executeQuery();
                    ctx.stop();
                    while (ret.next()) {
                      ctx = LOOKUP_STATS.time();
                      logProgress(selectOffset, selectErrorCount, selectBegin, totalSelectDuration);
                      ctx.stop();
                    }
                  }
                  else if (queryType.equals("complex")) {
                    Timer.Context ctx = LOOKUP_STATS.time();
                    PreparedStatement stmt = conn.prepareStatement("select persons.id1  " +
                        "from persons where persons.id1>=100 AND id1 < " + offset + " AND ID2=0 OR id1> 6 AND ID1 < " + offset);                                              //
                    ResultSet ret = stmt.executeQuery();
                    assertTrue(ret.next());
                    ctx.stop();
                  }
                  else if (queryType.equals("or")) {
                    Timer.Context ctx = LOOKUP_STATS.time();
                    PreparedStatement stmt = conn.prepareStatement("select * from persons where id1>" + offset + " and id2=0 or id1<" +
                        (offset + 10000) + " and id2=1 order by id1 desc");
                    ResultSet ret = stmt.executeQuery();

                    assertTrue(ret.next());
                    ctx.stop();
                  }
                  else if (queryType.equals("mixed")) {
                    Timer.Context ctx = LOOKUP_STATS.time();
                    PreparedStatement stmt = conn.prepareStatement("select persons.id1  " +
                        "from persons where persons.id1>2 AND id1 < " + offset / 4 + " OR id1> 6 AND ID1 < " + offset * 0.75);                                              //
                    ResultSet ret = stmt.executeQuery();
                    assertTrue(ret.next());
                    ctx.stop();
                  }
                  else if (queryType.equals("equalNonIndex")) {
                    PreparedStatement stmt = conn.prepareStatement("select * from persons where id2=1");
                    ResultSet ret = stmt.executeQuery();
                    int innerOffset = 0;
                    while (true) {
                      Timer.Context ctx = LOOKUP_STATS.time();
                      boolean found = ret.next();
                      ctx.stop();
                      if (!found) {
                        break;
                      }
                      if (innerOffset++ % 10000 == 0) {
                        logProgress(selectOffset, selectErrorCount, selectBegin, totalSelectDuration);
                      }
                    }
                  }
                  else if (queryType.equals("in")) {
                    Timer.Context ctx = LOOKUP_STATS.time();
                    PreparedStatement stmt = conn.prepareStatement("select * from persons where id1 in (0, 1, 2, 3, 4)");
                    ResultSet ret = stmt.executeQuery();
                    assertTrue(ret.next());
                    ctx.stop();
                  }
                  else if (queryType.equals("secondaryIndex")) {
                    Timer.Context ctx = LOOKUP_STATS.time();
                    PreparedStatement stmt = conn.prepareStatement("select * from persons where socialSecurityNumber=?");
                    stmt.setString(1, "933-28-" + offset);
                    ResultSet ret = stmt.executeQuery();
                    assertTrue(ret.next());
                    ctx.stop();
                  }
                  else if (queryType.equals("orTableScan")) {
                    PreparedStatement stmt = conn.prepareStatement("select * from persons where id2=1 or id2=0");
                    ResultSet ret = stmt.executeQuery();
                    int innerOffset = 0;
                    while (true) {
                      Timer.Context ctx = LOOKUP_STATS.time();
                      boolean found = ret.next();
                      ctx.stop();
                      if (!found) {
                        break;
                      }
                      if (innerOffset++ % 10000 == 0) {
                        logProgress(selectOffset, selectErrorCount, selectBegin, totalSelectDuration);
                      }
                    }
                  }
                  else if (queryType.equals("orIndex")) {

                    Timer.Context ctx = LOOKUP_STATS.time();
                    PreparedStatement stmt = conn.prepareStatement("select * from persons where id1=0 OR id1=1 OR id1=2 OR id1=3 OR id1=4");
                    ResultSet ret = stmt.executeQuery();
                    ret.next();
                    ctx.stop();
                    while (true) {
                      ctx = LOOKUP_STATS.time();
                      boolean found = ret.next();
                      ctx.stop();
                      if (!found) {
                        break;
                      }
                    }
                  }

                  totalSelectDuration.addAndGet(System.nanoTime() - beginSelect);
                  if (selectOffset.incrementAndGet() % logMod.get() == 0) {
                    logProgress(selectOffset, selectErrorCount, selectBegin, totalSelectDuration);
                  }
                  offset++;
                }
              }
              catch (Exception e) {
                e.printStackTrace();
              }
            }
          }
          catch (Exception e) {
            e.printStackTrace();
          }
        }
      });
      threads[i].start();
    }
  }

  private static void logProgress(AtomicLong selectOffset, AtomicLong selectErrorCount, AtomicLong selectBegin, AtomicLong totalSelectDuration) {
    StringBuilder builder = new StringBuilder();
    builder.append("select: count=").append(selectOffset.get());
    Snapshot snapshot = LOOKUP_STATS.getSnapshot();
    builder.append(String.format(", rate=%.4f", selectOffset.get() / (double)(System.currentTimeMillis() - selectBegin.get())*1000f));//LOOKUP_STATS.getFiveMinuteRate()));
    builder.append(String.format(", avg=%.2f nanos", totalSelectDuration.get() / (double)selectOffset.get()));//snapshot.getMean()));
    //builder.append(String.format(", avg=%.4f", snapshot.getMean() / 1000000d));
    builder.append(String.format(", 99th=%.4f", snapshot.get99thPercentile() / 1000000d));
    builder.append(String.format(", max=%.4f", (double) snapshot.getMax() / 1000000d));
    builder.append(", errorCount=" + selectErrorCount.get());
    if (selectOffset.get() > 4000000) {
      selectOffset.set(0);
      selectBegin.set(System.currentTimeMillis());
      totalSelectDuration.set(0);
    }
    System.out.println(builder.toString());
  }
}
