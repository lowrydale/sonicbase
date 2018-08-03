package com.sonicbase.bench;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class BenchmarkDelete {

  public static Logger logger = LoggerFactory.getLogger(BenchmarkDelete.class);


  private static final MetricRegistry METRICS = new MetricRegistry();

  public static final Timer INSERT_STATS = METRICS.timer("insert");

  private Thread mainThread;
  private boolean shutdown;

  private AtomicInteger countInserted = new AtomicInteger();
  private AtomicLong insertErrorCount = new AtomicLong();
  private long begin;
  private AtomicLong totalDuration = new AtomicLong();
  private AtomicLong insertBegin;
  private AtomicLong insertHighest;

  public static void main(String[] args) {
    Thread[] threads = new Thread[4];
    final BenchmarkDelete insert = new BenchmarkDelete();
    for (int i = 0; i < 4; i++) {
      final int shard = i;
      threads[i] = new Thread(new Runnable() {
        @Override
        public void run() {
          insert.start(new AtomicLong(), new AtomicLong(), "1-local", 4, shard, 0, 1000000000, true);
        }
      });
      threads[i].start();
    }
  }

  private AtomicInteger activeThreads = new AtomicInteger();
  private ConcurrentHashMap<Integer, Long> threadLiveliness = new ConcurrentHashMap<>();
  private int countDead = 0;

  public void start(final AtomicLong insertBegin, AtomicLong insertHighest, final String cluster, final int shardCount, final int shard, final long offset,
                    final long count, final boolean simulate) {
    shutdown = false;
    doResetStats();
    this.insertBegin = insertBegin;
    this.insertHighest = insertHighest;
    begin = System.currentTimeMillis();
    mainThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          final ThreadPoolExecutor executor = new ThreadPoolExecutor(256, 256, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
          final ThreadPoolExecutor selectExecutor = new ThreadPoolExecutor(256, 256, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new RejectedExecutionHandler() {
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
              // This will block if the queue is full
              try {
                executor.getQueue().put(r);
              }
              catch (InterruptedException e) {
                System.err.println(e.getMessage());
              }

            }
          });

          final ComboPooledDataSource cpds;
          if (!simulate) {
            System.out.println("userDir=" + System.getProperty("user.dir"));
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

            //    final java.sql.Connection conn = DriverManager.getConnection("jdbc:voltdb://localhost:21212");

            //    final java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:mysql://localhost/test", "test", "test");
            //    final java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:mysql://127.0.0.1:4306/test", "test", "test");

            //conn = DriverManager.getConnection("jdbc:sonicbase:" + address + ":9010/db");

            cpds = new ComboPooledDataSource();
            cpds.setDriverClass("com.sonicbase.jdbcdriver.Driver"); //loads the jdbc driver
            cpds.setJdbcUrl("jdbc:sonicbase:" + address + ":9010/db");

            cpds.setMinPoolSize(5);
            cpds.setAcquireIncrement(1);
            cpds.setMaxPoolSize(20);
          }
          else {
            cpds = null;
          }

          final boolean batch = offset != 1;

          //test insert
          final AtomicLong countFinished = new AtomicLong();

          final AtomicInteger errorCountInARow = new AtomicInteger();
          final int batchSize = 100;
          while (!shutdown) {
            final long startId = offset + (shard * count);
            insertBegin.set(startId);
            List<Thread> threads = new ArrayList<>();
            final AtomicLong currOffset = new AtomicLong(startId);
            final int threadCount = (batch ? 8 : 256);
            for (int i = 0; i < threadCount; i++) {
              final int threadOffset = i;
              final AtomicLong lastLogged = new AtomicLong(System.currentTimeMillis());
              Thread insertThread = new Thread(new Runnable() {
                @Override
                public void run() {
                  try {
                    threadLiveliness.put(threadOffset, System.currentTimeMillis());
                    activeThreads.incrementAndGet();
                    while (!shutdown) {

                      long offset = 0;
                      synchronized (currOffset) {
                        offset = currOffset.addAndGet(2);
                      }
                      BenchmarkDelete.this.insertHighest.set(offset - (threadCount * batchSize * 2));
                      //                    if (offset >= startId + count) {//((cycleNum.get() - 1) * shardCount * count) + ((shard + 1) * count)) {
                      //                      break;
                      //                    }
                      try {

                        long thisDuration = 0;
                        //Thread.sleep(5000);
                        //conn.setAutoCommit(false);
                        for (int attempt = 0; attempt < 4; attempt++) {
                          Connection conn = cpds.getConnection();
                          try {
                            long currBegin = System.nanoTime();
                            PreparedStatement stmt = conn.prepareStatement("delete from persons where id=");
                            stmt.setLong(1, offset);
                            stmt.executeUpdate();

                            thisDuration += System.nanoTime() - currBegin;
                            break;
                          }
                          catch (Exception e) {
                            if (attempt == 3) {
                              throw e;
                            }
                            e.printStackTrace();
                          }
                          finally {
                            conn.close();
                          }
                        }

                        threadLiveliness.put(threadOffset, System.currentTimeMillis());
                        //conn.commit();
                        totalDuration.addAndGet(thisDuration);
                        countInserted.addAndGet(batchSize);
                        logProgress(offset, threadOffset, countInserted, lastLogged, begin, totalDuration, insertErrorCount);
                        errorCountInARow.set(0);
                      }
                      catch (Exception e) {
                        if (errorCountInARow.incrementAndGet() > 2000) {
                          System.out.println("Too many errors, aborting");
                          break;
                        }
                        insertErrorCount.incrementAndGet();
                        if (e.getMessage() != null && e.getMessage().contains("Unique constraint violated")) {
                          System.out.println("Unique constraint violation");
                        }
                        else {
                          System.out.println("Error inserting");
                          e.printStackTrace();
                        }
                      }
                      finally {
                        countFinished.incrementAndGet();
                        logProgress(offset, threadOffset, countInserted, lastLogged, begin, totalDuration, insertErrorCount);
                      }
                    }
                  }
                  finally {
                    activeThreads.decrementAndGet();
                  }
                }
              });
              insertThread.start();
              threads.add(insertThread);
            }

            while (true) {
              int countDead = 0;
              for (Map.Entry<Integer, Long> entry : threadLiveliness.entrySet()) {
                if (System.currentTimeMillis() - entry.getValue() > 4 * 60 * 1000) {
                  countDead++;
                }
              }
              BenchmarkDelete.this.countDead = countDead;
              Thread.sleep(1000);
            }
//            for (Thread thread : threads) {
//              thread.join();
//            }
          }

          selectExecutor.shutdownNow();
          executor.shutdownNow();
        }
        catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
    mainThread.start();
  }

  private void doResetStats() {
    countInserted.set(0);
    insertErrorCount.set(0);
    begin = System.currentTimeMillis();
    totalDuration.set(0);
  }

  private static void logProgress(long offset, int threadOffset, AtomicInteger countInserted, AtomicLong lastLogged, long begin, AtomicLong totalDuration, AtomicLong insertErrorCount) {
    if (threadOffset == 0) {
      if (System.currentTimeMillis() - lastLogged.get() > 2000) {
        lastLogged.set(System.currentTimeMillis());
        StringBuilder builder = new StringBuilder();
        builder.append("count=").append(countInserted.get());
        Snapshot snapshot = INSERT_STATS.getSnapshot();
        builder.append(String.format(", rate=%.2f", countInserted.get() / (double) (System.currentTimeMillis() - begin) * 1000f)); //INSERT_STATS.getFiveMinuteRate()));
        builder.append(String.format(", avg=%.2f", totalDuration.get() / (countInserted.get()) / 1000000d));//snapshot.getMean() / 1000000d));
        builder.append(String.format(", 99th=%.2f", snapshot.get99thPercentile() / 1000000d));
        builder.append(String.format(", max=%.2f", (double) snapshot.getMax() / 1000000d));
        builder.append(", errorCount=" + insertErrorCount.get());
        System.out.println(builder.toString());
      }
    }
  }

  private static ConcurrentHashMap<Long, Integer> addedRecords = new ConcurrentHashMap<>();

  public static byte[] checkAddedRecords(String command, byte[] body) {
    logger.info("begin checkAddedRecords");
    for (int i = 0; i < 1000000; i++) {
      if (addedRecords.get((long) i) == null) {
        logger.error("missing record: id=" + i + ", count=0");
      }
    }
    logger.info("finished checkAddedRecords");
    return null;
  }

  public void stop() {
    shutdown = true;
    mainThread.interrupt();
  }

  public String stats() {
    ObjectNode dict = new ObjectNode(JsonNodeFactory.instance);
    dict.put("begin", begin);
    dict.put("count", countInserted.get());
    dict.put("errorCount", insertErrorCount.get());
    dict.put("totalDuration", totalDuration.get());
    dict.put("activeThreads", activeThreads.get());
    dict.put("countDead", countDead);
    return dict.toString();
  }

  public void resetStats() {
    doResetStats();
  }
}
