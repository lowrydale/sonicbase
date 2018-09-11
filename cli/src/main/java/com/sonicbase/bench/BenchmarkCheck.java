package com.sonicbase.bench;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.query.DatabaseException;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class BenchmarkCheck {

  public static final Logger logger = LoggerFactory.getLogger(BenchmarkCheck.class);

  private static final MetricRegistry METRICS = new MetricRegistry();

  private static final com.codahale.metrics.Timer LOOKUP_STATS = METRICS.timer("lookup");
  private Thread mainThread;
  private boolean shutdown;
  final AtomicLong totalBegin = new AtomicLong(System.currentTimeMillis());
  final AtomicLong totalSelectDuration = new AtomicLong();
  final AtomicLong selectErrorCount = new AtomicLong();
  final AtomicLong selectBegin = new AtomicLong(System.currentTimeMillis());
  final AtomicLong selectOffset = new AtomicLong();
  final AtomicLong selectCount = new AtomicLong();
  final AtomicLong readCount = new AtomicLong();

  public void start(String address, final AtomicLong insertBegin, final AtomicLong insertHighest, final String cluster,
                    final int shardCount, final Integer shard, final long count) {
    shutdown = false;
    selectBegin.set(System.currentTimeMillis());
    doResetStats();
    mainThread = new Thread(() -> {
      try {
        final AtomicInteger cycle = new AtomicInteger();

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
        try {
          Thread[] threads = new Thread[1];
          for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
              while (!shutdown) {
                try {
                  PreparedStatement stmt = null;
                  ResultSet ret = null;
                  if (true) {
                    //primary index 2 expressions order desc
                    //700k

                    long actualStartId = insertBegin.get() + 1;
                    cycle.incrementAndGet();
                    long begin = System.nanoTime();
                    stmt = conn.prepareStatement("select persons.id1, id2  " +
                        "from persons where id1 >= ? order by id1 asc");                                              //
                    stmt.setLong(1, actualStartId);
                    long beginNano = System.nanoTime();
                    ret = stmt.executeQuery();
                    totalSelectDuration.addAndGet(System.nanoTime() - begin);

                    long highest = actualStartId;
                    while (true) {
                      begin = System.nanoTime();
                      if (!ret.next()) {
                        throw new DatabaseException("Not found: id=" + (highest + 1) + ", highestInsert=" + insertHighest.get());
                      }
                      if (highest % 1000 == 0) {
                        Thread.sleep(10);
                      }
                      long foundId = ret.getLong("id1");
                      if (foundId >= insertHighest.get()) {
                        break;
                      }
                      if (foundId != highest) {
                        throw new DatabaseException("Not found: expected=" + highest + ", found=" + foundId + ", highestInsert=" + insertHighest.get());
                      }
                      highest = foundId + 1;
                      totalSelectDuration.addAndGet(System.nanoTime() - begin);

                      if (readCount.incrementAndGet() % 100000 == 0) {
                        StringBuilder builder = new StringBuilder();
                        builder.append("count=").append(readCount.get());
                        Snapshot snapshot = LOOKUP_STATS.getSnapshot();
                        builder.append(String.format(", rate=%.2f", readCount.get() / (double) (System.currentTimeMillis() - totalBegin.get()) * 1000f));
                        builder.append(String.format(", avg=%.2f nanos", totalSelectDuration.get() / (double) readCount.get()));
                        builder.append(String.format(", 99th=%.2f nanos", snapshot.get99thPercentile()));
                        builder.append(String.format(", max=%d", snapshot.getMax()));
                        builder.append(", errorCount=").append(selectErrorCount.get());

                        logger.info(builder.toString());
                      }
                    }
                    totalSelectDuration.addAndGet(System.nanoTime() - beginNano);
                  }
                  selectCount.incrementAndGet();
                }
                catch (Exception e) {
                  selectErrorCount.incrementAndGet();
                  logger.error("Error", e);
                }
              }
            });
            threads[i].start();
          }
        }
        catch (Exception e) {
          logger.error("Error", e);
        }
        while (true) {
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
    totalBegin.set(System.currentTimeMillis());
    totalSelectDuration.set(0);
    selectErrorCount.set(0);
    selectBegin.set(System.currentTimeMillis());
    selectOffset.set(0);
    readCount.set(0);
  }

  public void stop() {
    shutdown = true;
    mainThread.interrupt();
  }

  public String stats() {
    ObjectNode dict = new ObjectNode(JsonNodeFactory.instance);
    dict.put("begin", selectBegin.get());
    dict.put("count", readCount.get());
    dict.put("errorCount", selectErrorCount.get());
    dict.put("totalDuration", totalSelectDuration.get());
    dict.put("countDead", 0);
    dict.put("activeThreads", 0);
    return dict.toString();
  }

  public void resetStats() {
    doResetStats();
  }
}
