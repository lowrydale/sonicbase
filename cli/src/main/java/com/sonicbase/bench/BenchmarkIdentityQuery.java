package com.sonicbase.bench;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.common.AssertUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  public static final Logger logger = LoggerFactory.getLogger(BenchmarkIdentityQuery.class);

  private static final MetricRegistry METRICS = new MetricRegistry();

  private static final com.codahale.metrics.Timer LOOKUP_STATS = METRICS.timer("lookup");
  public static final String LAST_ID_STR = "lastId=";
  private Thread mainThread;
  private boolean shutdown;
  final AtomicLong totalSelectDuration = new AtomicLong();
  final AtomicLong selectErrorCount = new AtomicLong();
  final AtomicLong selectBegin = new AtomicLong(System.currentTimeMillis());
  final AtomicLong selectOffset = new AtomicLong();

  private final ConcurrentHashMap<Integer, Long> threadLiveliness = new ConcurrentHashMap<>();
  private int countDead = 0;
  private final AtomicInteger activeThreads = new AtomicInteger();

  public void start(String address, final String cluster, final int shardCount, final Integer shard, final long count, final String queryType) {
    shutdown = false;
    selectBegin.set(System.currentTimeMillis());
    doResetStats();
    mainThread = new Thread(() -> {
      try {
        final AtomicInteger cycle = new AtomicInteger();
        final long startId = shard * count;

        logger.info("Starting client");

        File file = new File(System.getProperty("user.dir"), "config/config-" + cluster + ".yaml");
        if (!file.exists()) {
          file = new File(System.getProperty("user.dir"), "db/src/main/resources/config/config-" + cluster + ".yaml");
          logger.info("Loaded config resource dir");
        }
        else {
          logger.info("Loaded config default dir");
        }

        logger.info("Using address: address={}", address);

        Class.forName("com.sonicbase.jdbcdriver.Driver");

        final java.sql.Connection conn = DriverManager.getConnection("jdbc:sonicbase:" + address + ":9010/db", "user", "password");

        //test insert

        final AtomicInteger logMod = new AtomicInteger(10000);
        int threadCount = 32;
        if (queryType.equals("batch") || queryType.equals("cbatch")) {
          threadCount = 32;
        }
        if (queryType.equals("limitOffset") || queryType.equals("sort")) {
          threadCount = 4;
        }
        if (queryType.equals("equalNonIndex") || queryType.equals("orTableScan")) {
          threadCount = 4;
        }

        final int lessShardCount = shardCount;//(int) (shardCount * 0.75);

        final AtomicLong firstId = new AtomicLong(-1);

        firstId.set(startId);

        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threads.length; i++) {
          final int threadOffset = i;
          threads[i] = new Thread(() -> {
            try {
              long offset = firstId.get();
              final AtomicInteger currOffset = new AtomicInteger();
              activeThreads.incrementAndGet();
              while (!shutdown) {
                try {
                  cycle.incrementAndGet();
                  byte[] bytes = new byte[100];
                  for (int i1 = 0; i1 < bytes.length; i1++) {
                    bytes[i1] = (byte) ThreadLocalRandom.current().nextInt(256);
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
                        logger.info("lastId={}", lastId);
                        break;
                      }
                      else {
                        lastId = rs.getLong("id1");
                      }
                    }
                    else   if (queryType.equals("batch")) {
                      int batchSize = 1600;
                      int innerBatchSize = batchSize / lessShardCount;
                      long startOffset = offset;
                      StringBuilder builder = new StringBuilder("select id1 from persons where id1 in (");
                      for (int i1 = 0 ; i1 < innerBatchSize * lessShardCount; i1++) {
                        if (i1 == 0) {
                          builder.append("?");
                        }
                        else {
                          builder.append(", ?");
                        }
                      }
                      builder.append(")");
                      PreparedStatement stmt = conn.prepareStatement(builder.toString());

                      //spread the ids across all shards for fairness
//                      for (int i1 = 0; i1 < batchSize;) {
//                        for (int j = 0; j < shardCount; j++) {
//                          long shardedOffset = currOffset.get() + (j * count);
//                          stmt.setLong(i1 + 1, shardedOffset);
//                          i1++;
//                          if (i1 >= batchSize) {
//                            break;
//                          }
//                        }
//                        offset++;
//
//                      }
//

                      int parm = 0;
                      for (int j = 0; j < lessShardCount; j++) {
                        for (int k = 0; k < innerBatchSize; k++) {
                          long shardedOffset = (j * count) + currOffset.get() + k;
                          stmt.setLong(parm++ + 1, shardedOffset);
                        }
                      }
                      currOffset.addAndGet(innerBatchSize);
                      if (currOffset.get() > 100_000) {
                        currOffset.set(0);
                      }
//                      offset += shardCount * innerBatchSize;
//                      for (int i1 = 0; i1 < batchSize; ) {
//                        for (int j = 0; j < shardCount; j++) {
//                           long shardedOffset = offset + j * count;
//                           stmt.setLong(i1 + 1, shardedOffset);
//                           i1++;
//                           if (i1 >= batchSize) {
//                             break;
//                           }
//                        }
//                        offset++;
//
//                      }
                      ResultSet rs = stmt.executeQuery();
                      boolean found = rs.next();
                      if (!found) {
                        logger.info("lastId={}", lastId);
                        offset = firstId.get();
                        break;
                      }
                      else {
                        boolean hadSome = false;
                        for (long i1 = startOffset + 1; i1 < startOffset + batchSize; i1++) {
                          if (rs.next()) {
                            hadSome = true;
                            selectOffset.incrementAndGet();
                            lastId = rs.getLong("id1");
                          }
                        }
                        if (!hadSome) {
                          offset = firstId.get();
                        }
                      }
                    }
                    else if (queryType.equals("cbatch")) {
                      long startOffset = offset;
                      int batchSize = 1600;
                      StringBuilder builder = new StringBuilder("select personId from memberships where personId=? and personId2=0 ");
                      for (int i1 = 0; i1 < batchSize - 1; i1++) {
                        builder.append(" or personId=? and personId2=0 ");
                      }
                      PreparedStatement stmt = conn.prepareStatement(builder.toString());

                      //spread the ids across all shards for fairness
                      for (int i1 = 0; i1 < batchSize;) {
                        for (int j = 0; j < shardCount; j++) {
                          long shardedOffset = offset + j * count;
                          stmt.setLong(i1 + 1, shardedOffset);
                          i1++;
                          if (i1 >= batchSize) {
                            break;
                          }
                        }
                        offset++;

                      }
                      ResultSet rs = stmt.executeQuery();
                      boolean found = rs.next();
                      if (!found) {
                        logger.info("lastId={}", lastId);
                        offset = firstId.get();
                        break;
                      }
                      else {
                        boolean hadSome = false;
                        for (long i1 = startOffset + 1; i1 < startOffset + batchSize; i1++) {
                          if (rs.next()) {
                            hadSome = true;
                            selectOffset.incrementAndGet();
                            lastId = rs.getLong("id1");
                          }
                        }
                        if (!hadSome) {
                          offset = firstId.get();
                        }
                      }
                    }
                    else if (queryType.equals("twoFieldId")) {

                      boolean missing = false;
                      for (int i1 = 0; i1 < 5; i1++) {
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
                        logger.info("max={}", offset);
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
                  offset = firstId.get();
                }
                catch (Exception e) {
                  logger.error("Error", e);
                }
              }
            }
            catch (Exception e) {
              logger.error("Error", e);
            }
            finally {
              activeThreads.decrementAndGet();
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
        logger.error("Error", e);
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
    builder.append(String.format(", rate=%.4f", selectOffset.get() / (double) (System.currentTimeMillis() - selectBegin.get()) * 1000f));
    builder.append(String.format(", avg=%.2f nanos", totalSelectDuration.get() / (double) selectOffset.get()));
    builder.append(String.format(", 99th=%.4f", snapshot.get99thPercentile() / 1000000d));
    builder.append(String.format(", max=%.4f", (double) snapshot.getMax() / 1000000d));
    builder.append(", errorCount=").append(selectErrorCount.get());
    logger.info(builder.toString());
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
