package com.lowryengineering.database.bench;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.lowryengineering.database.util.JsonArray;
import com.lowryengineering.database.util.JsonDict;
import com.lowryengineering.database.util.StreamUtils;
import com.lowryengineering.research.socket.NettyServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestBenchmarkIdentityQuery {

  public static Logger logger = LoggerFactory.getLogger(TestBenchmarkIdentityQuery.class);


  private static final MetricRegistry METRICS = new MetricRegistry();

  public static final com.codahale.metrics.Timer LOOKUP_STATS = METRICS.timer("lookup");

  public static void main(String[] args) throws Exception {

    final long startId = Long.valueOf(args[0]);
    final String cluster = args[1];
    final String queryType = args[2];
    //   FileUtils.deleteDirectory(new File("/data/database"));

    final NettyServer[] dbServers = new NettyServer[4];
    for (int shard = 0; shard < dbServers.length; shard++) {
      dbServers[shard] = new NettyServer();
    }

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

    Class.forName("com.lowryengineering.database.jdbcdriver.DriverProxy");

//       final java.sql.Connection conn = DriverManager.getConnection("jdbc:voltdb://localhost:21212");

//    final java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:mysql://localhost/test", "test", "test");
//    final java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:mysql://127.0.0.1:4306/test", "test", "test");

    //54.173.145.214
    final java.sql.Connection conn = DriverManager.getConnection("jdbc:dbproxy:" + address + ":9010/db", "user", "password");

    //test insert
    final AtomicLong totalSelectDuration = new AtomicLong();

    final AtomicLong selectErrorCount = new AtomicLong();
    final AtomicLong selectBegin = new AtomicLong(System.currentTimeMillis());
    final AtomicLong selectOffset = new AtomicLong();


    Thread[] threads = new Thread[256];
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
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
                    Timer.Context ctx = LOOKUP_STATS.time();
                    PreparedStatement stmt = conn.prepareStatement("select id1  " +
                        "from persons where persons.id1=?");                                              //
                    stmt.setLong(1, offset);
                    ResultSet ret = stmt.executeQuery();

                    if (!ret.next()) {
                      System.out.println("max=" + offset);
                      break;
                    }

                    //                  ((ConnectionProxy)conn).getDatabaseClient().send(ThreadLocalRandom.current().nextInt(2), ThreadLocalRandom.current().nextInt(2), "DatabaseSever:echo:1", bytes, DatabaseClient.Replica.specified, 20000, new AtomicReference<String>());
                    ctx.stop();
                  }
                  else if (queryType.equals("twoFieldId")) {

                    Timer.Context ctx = LOOKUP_STATS.time();
                    PreparedStatement stmt = conn.prepareStatement("select id1, id2  " +
                        "from memberships where id1=? and id2=?");                                              //
                    stmt.setLong(1, offset);
                    stmt.setLong(2, 0);
                    ResultSet ret = stmt.executeQuery();

                    if (!ret.next()) {
                      System.out.println("max=" + offset);
                      break;
                    }
                    assertEquals(ret.getLong("id1"), offset);
                    assertEquals(ret.getLong("id2"), 0);
                    ctx.stop();
                  }
                  else if (queryType.equals("max")) {
                    Timer.Context ctx = LOOKUP_STATS.time();
                    PreparedStatement stmt = conn.prepareStatement("select max(id) as maxValue from persons");
                    ResultSet ret = stmt.executeQuery();

                    assertTrue(ret.next());
                    assertFalse(ret.next());
                    ctx.stop();
                  }
                  else if (queryType.equals("maxTableScan")) {
                    Timer.Context ctx = LOOKUP_STATS.time();
                    PreparedStatement stmt = conn.prepareStatement("select max(id) as maxValue from persons where id2 < 1");
                    ResultSet ret = stmt.executeQuery();

                    assertTrue(ret.next());
                    ctx.stop();
                  }
                  else if (queryType.equals("maxWhere")) {
                    Timer.Context ctx = LOOKUP_STATS.time();
                    PreparedStatement stmt = conn.prepareStatement("select max(id) as maxValue from persons where id < 100");
                    ResultSet ret = stmt.executeQuery();

                    assertTrue(ret.next());
                    assertFalse(ret.next());
                    ctx.stop();
                  }
                  else if (queryType.equals("sum")) {
                    Timer.Context ctx = LOOKUP_STATS.time();
                    PreparedStatement stmt = conn.prepareStatement("select sum(id) as sumValue from persons");
                    ResultSet ret = stmt.executeQuery();

                    assertTrue(ret.next());
                    assertFalse(ret.next());
                    ctx.stop();
                  }
                  else if (queryType.equals("limit")) {
                    Timer.Context ctx = LOOKUP_STATS.time();
                    PreparedStatement stmt = conn.prepareStatement("select * from persons where id < ? and id > ? limit 3");
                    stmt.setLong(1, offset);
                    stmt.setLong(2, 2);
                    ResultSet ret = stmt.executeQuery();

                    assertTrue(ret.next());
                    ctx.stop();
                  }
                  else if (queryType.equals("limitOffset")) {
                    Timer.Context ctx = LOOKUP_STATS.time();
                    PreparedStatement stmt = conn.prepareStatement("select * from persons where id < ? and id > ? limit 3 offset 2");
                    stmt.setLong(1, offset);
                    stmt.setLong(2, offset / 2);
                    ResultSet ret = stmt.executeQuery();

                    assertTrue(ret.next());
                    ctx.stop();
                  }
                  else if (queryType.equals("sort")) {
                    Timer.Context ctx = LOOKUP_STATS.time();
                    PreparedStatement stmt = conn.prepareStatement("select * from persons order by id2 asc, id desc");
                    ResultSet ret = stmt.executeQuery();

                    assertTrue(ret.next());
                    ctx.stop();
                  }
                  else if (queryType.equals("complex")) {
                    Timer.Context ctx = LOOKUP_STATS.time();
                    PreparedStatement stmt = conn.prepareStatement("select persons.id  " +
                          "from persons where persons.id>=100 AND id < " + offset + " AND ID2=0 OR id> 6 AND ID < " + offset);                                              //
                      ResultSet ret = stmt.executeQuery();
                    assertTrue(ret.next());
                    ctx.stop();
                  }
                  else if (queryType.equals("or")) {
                    Timer.Context ctx = LOOKUP_STATS.time();
                    PreparedStatement stmt = conn.prepareStatement("select * from persons where id>105 and id2=0 or id<105 and id2=1 order by id desc");
                     ResultSet ret = stmt.executeQuery();

                    assertTrue(ret.next());
                    ctx.stop();
                  }
                  else if (queryType.equals("mixed")) {
                    Timer.Context ctx = LOOKUP_STATS.time();
                    PreparedStatement stmt = conn.prepareStatement("select persons.id  " +
                        "from persons where persons.id>2 AND id < " + offset / 4 + " OR id> 6 AND ID < " + offset * 0.75);                                              //
                    ResultSet ret = stmt.executeQuery();
                    assertTrue(ret.next());
                    ctx.stop();
                  }
                  else if (queryType.equals("equalNonIndex")) {
                    Timer.Context ctx = LOOKUP_STATS.time();
                    PreparedStatement stmt = conn.prepareStatement("select * from persons where id2=1 order by id desc");
                    ResultSet ret = stmt.executeQuery();
                    assertTrue(ret.next());
                    ctx.stop();
                  }
                  else if (queryType.equals("in")) {
                    Timer.Context ctx = LOOKUP_STATS.time();
                    PreparedStatement stmt = conn.prepareStatement("select * from persons where id in (0, 1, 2, 3, 4)");
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
                    Timer.Context ctx = LOOKUP_STATS.time();
                    PreparedStatement stmt = conn.prepareStatement("select * from persons where id2=1 or id2=0 order by id2 asc, id desc");
                      ResultSet ret = stmt.executeQuery();
                      assertTrue(ret.next());
                    ctx.stop();
                  }
                  else if (queryType.equals("orIndex")) {
                    Timer.Context ctx = LOOKUP_STATS.time();
                    PreparedStatement stmt = conn.prepareStatement("select * from persons where id=0 OR id=1 OR id=2 OR id=3 OR id=4");
                    ResultSet ret = stmt.executeQuery();
                    assertTrue(ret.next());
                    ctx.stop();
                  }

                  totalSelectDuration.addAndGet(System.nanoTime() - beginSelect);
                  if (selectOffset.incrementAndGet() % 10000 == 0) {
                    StringBuilder builder = new StringBuilder();
                    builder.append("select: count=").append(selectOffset.get());
                    Snapshot snapshot = LOOKUP_STATS.getSnapshot();
                    builder.append(String.format(", rate=%.2f", LOOKUP_STATS.getFiveMinuteRate()));
                    builder.append(String.format(", avg=%.2f", snapshot.getMean() / 1000000d));
                    builder.append(String.format(", 99th=%.2f", snapshot.get99thPercentile() / 1000000d));
                    builder.append(String.format(", max=%.2f", (double) snapshot.getMax() / 1000000d));
                    builder.append(", errorCount=" + selectErrorCount.get());
                    if (selectOffset.get() > 4000000) {
                      selectOffset.set(0);
                      selectBegin.set(System.currentTimeMillis());
                      totalSelectDuration.set(0);
                    }
                    System.out.println(builder.toString());
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
}
