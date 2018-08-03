package com.sonicbase.bench;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.common.AssertUtils;
import com.sonicbase.query.DatabaseException;
import org.apache.commons.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.sonicbase.common.AssertUtils.assertFalse;
import static com.sonicbase.common.AssertUtils.assertTrue;


public class BenchmarkIdentityQuery {

  private static final MetricRegistry METRICS = new MetricRegistry();

  public static final com.codahale.metrics.Timer LOOKUP_STATS = METRICS.timer("lookup");
  private Thread mainThread;
  private boolean shutdown;
  final AtomicLong totalSelectDuration = new AtomicLong();
  final AtomicLong selectErrorCount = new AtomicLong();
  final AtomicLong selectBegin = new AtomicLong(System.currentTimeMillis());
  final AtomicLong selectOffset = new AtomicLong();

  private ConcurrentHashMap<Integer, Long> threadLiveliness = new ConcurrentHashMap<>();
  private int countDead = 0;
  private AtomicInteger activeThreads = new AtomicInteger();

  public void start(final String cluster, final int shardCount, final Integer shard, final long count, final String queryType) {
    shutdown = false;
    selectBegin.set(System.currentTimeMillis());
    doResetStats();
    mainThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          final AtomicInteger cycle = new AtomicInteger();
          final long startId = shard * count;

          System.out.println("Starting client");

          File file = new File(System.getProperty("user.dir"), "config/config-" + cluster + ".json");
          if (!file.exists()) {
            file = new File(System.getProperty("user.dir"), "db/src/main/resources/config/config-" + cluster + ".json");
            System.out.println("Loaded config resource dir");
          }
          else {
            System.out.println("Loaded config default dir");
          }
          String configStr = IOUtils.toString(new BufferedInputStream(new FileInputStream(file)), "utf-8");

          ObjectMapper mapper = new ObjectMapper();
          ObjectNode dict = (ObjectNode) mapper.readTree(configStr);
          ObjectNode databaseDict = dict;

          ArrayNode array = databaseDict.withArray("shards");
          ObjectNode replica = (ObjectNode) array.get(0);
          ArrayNode replicasArray = replica.withArray("replicas");
          String address = replicasArray.get(0).get("publicAddress").asText();
          if (databaseDict.get("clientIsPrivate").asBoolean()) {
            address = replicasArray.get(0).get("privateAddress").asText();
          }
          System.out.println("Using address: address=" + address);

          Class.forName("com.sonicbase.jdbcdriver.Driver");

          //       final java.sql.Connection conn = DriverManager.getConnection("jdbc:voltdb://localhost:21212");

          //    final java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:mysql://localhost/test", "test", "test");
          //    final java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:mysql://127.0.0.1:4306/test", "test", "test");

          //54.173.145.214
          final java.sql.Connection conn = DriverManager.getConnection("jdbc:sonicbase:" + address + ":9010/db", "user", "password");

          //test insert

          final AtomicInteger logMod = new AtomicInteger(10000);
          int threadCount = 32;
          if (queryType.equals("limitOffset") || queryType.equals("sort")) {
            threadCount = 4;
          }
          if (queryType.equals("equalNonIndex") || queryType.equals("orTableScan")) {
            threadCount = 4;
          }

          int countLookingForFirst = 0;
          final AtomicLong firstId = new AtomicLong(-1);
//          long offset = startId;
//          System.out.println("startId=" + startId);
//          while (firstId.get() == -1) {
//            PreparedStatement stmt = conn.prepareStatement("select id1  " +
//                "from persons where persons.id1=?");
//            stmt.setLong(1, offset);
//            ResultSet rs = stmt.executeQuery();
//            boolean found = rs.next();
//            if (found) {
//              firstId.set(rs.getLong("id1"));
//              break;
//            }
//            if (countLookingForFirst++ % 10_000 == 0) {
//              System.out.println("lookingForFirst: count=" + countLookingForFirst);
//            }
//            if (countLookingForFirst > 1_000_000) {
//              System.out.println("Aborting search for first");
//              return;
//            }
//            offset++;
//          }

          firstId.set(startId);

          Thread[] threads = new Thread[threadCount];
          for (int i = 0; i < threads.length; i++) {
            final int threadOffset = i;
            threads[i] = new Thread(new Runnable() {
              @Override
              public void run() {
                try {
                  activeThreads.incrementAndGet();
                  int countMissing = 0;
                  outer:
                  while (!shutdown) {
                    try {
                      long offset = firstId.get();
                      cycle.incrementAndGet();
                      byte[] bytes = new byte[100];
                      for (int i = 0; i < bytes.length; i++) {
                        bytes[i] = (byte) ThreadLocalRandom.current().nextInt(256);
                      }
                      long lastId = -1;
                      while (true) {
                        threadLiveliness.put(threadOffset, System.currentTimeMillis());
                        long beginSelect = System.nanoTime();

                        if (queryType.equals("id")) {
                          PreparedStatement stmt = conn.prepareStatement("select id1  " +
                              "from persons where persons.id1=?");
                          stmt.setLong(1, offset);
                          ResultSet rs = stmt.executeQuery();
                          boolean found = rs.next();
                          if (!found) {
                            System.out.println("lastId=" + lastId);
                            //if (countMissing++ > 100_000) {
                              break;
                            //}
                          }
                          else {
                            countMissing = 0;
                            lastId = rs.getLong("id1");
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
                              AssertUtils.assertEquals(ret.getLong("id1"), offset);
                              AssertUtils.assertEquals(ret.getLong("id2"), 0);
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
                finally {
                  activeThreads.decrementAndGet();
                }
              }

            });
            threads[i].start();
          }


          while (true) {
            int countDead = 0;
            for (Map.Entry<Integer, Long> entry : threadLiveliness.entrySet()) {
              if (System.currentTimeMillis() - entry.getValue() > 60 * 1000) {
                countDead++;
              }
            }
            BenchmarkIdentityQuery.this.countDead = countDead;
            Thread.sleep(1000);
          }

        }
        catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
    mainThread.start();
  }


  private void doResetStats() {
    totalSelectDuration.set(0);
    selectErrorCount.set(0);
    selectBegin.set(System.currentTimeMillis());
    selectOffset.set(0);
  }

  private static void logProgress(AtomicLong selectOffset, AtomicLong selectErrorCount, AtomicLong selectBegin, AtomicLong totalSelectDuration) {
    StringBuilder builder = new StringBuilder();
    builder.append("select: count=").append(selectOffset.get());
    Snapshot snapshot = LOOKUP_STATS.getSnapshot();
    builder.append(String.format(", rate=%.4f", selectOffset.get() / (double) (System.currentTimeMillis() - selectBegin.get()) * 1000f));//LOOKUP_STATS.getFiveMinuteRate()));
    builder.append(String.format(", avg=%.2f nanos", totalSelectDuration.get() / (double) selectOffset.get()));//snapshot.getMean()));
    //builder.append(String.format(", avg=%.4f", snapshot.getMean() / 1000000d));
    builder.append(String.format(", 99th=%.4f", snapshot.get99thPercentile() / 1000000d));
    builder.append(String.format(", max=%.4f", (double) snapshot.getMax() / 1000000d));
    builder.append(", errorCount=" + selectErrorCount.get());
    System.out.println(builder.toString());
  }

  public void stop() {
    shutdown = true;
    mainThread.interrupt();
  }

  public String stats() {
    ObjectNode dict = new ObjectNode(JsonNodeFactory.instance);
    dict.put("begin", selectBegin.get());
    dict.put("count", selectOffset.get());
    dict.put("errorCount", selectErrorCount.get());
    dict.put("totalDuration", totalSelectDuration.get());
    dict.put("countDead", countDead);
    dict.put("activeThreads", activeThreads.get());
    return dict.toString();
  }

  public void resetStats() {
    doResetStats();
  }
}
