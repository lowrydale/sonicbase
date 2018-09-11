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

  public static final Logger logger = LoggerFactory.getLogger(BenchmarkDelete.class);


  private static final MetricRegistry METRICS = new MetricRegistry();

  private static final Timer INSERT_STATS = METRICS.timer("insert");
  public static final String USER_DIR_STR = "user.dir";
  public static final String ERROR_STR = "Error";

  private Thread mainThread;
  private boolean shutdown;

  private final AtomicInteger countInserted = new AtomicInteger();
  private final AtomicLong insertErrorCount = new AtomicLong();
  private long begin;
  private final AtomicLong totalDuration = new AtomicLong();
  private AtomicLong insertHighest;

  private final AtomicInteger activeThreads = new AtomicInteger();
  private final ConcurrentHashMap<Integer, Long> threadLiveliness = new ConcurrentHashMap<>();
  private int countDead = 0;

  public void start(String address, final AtomicLong insertBegin, AtomicLong insertHighest, final String cluster, final int shardCount, final int shard, final long offset,
                    final long count, final boolean simulate) {
    shutdown = false;
    doResetStats();
    this.insertHighest = insertHighest;
    begin = System.currentTimeMillis();
    mainThread = new Thread(() -> {
      try {
        final ThreadPoolExecutor executor = new ThreadPoolExecutor(256, 256,
            10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
        final ThreadPoolExecutor selectExecutor = new ThreadPoolExecutor(256, 256,
            10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), (r, executor1) -> {
              // This will block if the queue is full
              try {
                executor1.getQueue().put(r);
              }
              catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error(ERROR_STR, e);
                return;
              }

            });

        final ComboPooledDataSource cpds;
        if (!simulate) {
          logger.info("userDir=" + System.getProperty(USER_DIR_STR));
          File file = new File(System.getProperty(USER_DIR_STR), "config/config-" + cluster + ".yaml");
          if (!file.exists()) {
            file = new File(System.getProperty(USER_DIR_STR), "db/src/main/resources/config/config-" + cluster + ".yaml");
            logger.info("Loaded config resource dir");
          }
          else {
            logger.info("Loaded config default dir");
          }

          logger.info("Using address: address=" + address);

          Class.forName("com.sonicbase.jdbcdriver.Driver");

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
            Thread insertThread = new Thread(() -> {
              try {
                threadLiveliness.put(threadOffset, System.currentTimeMillis());
                activeThreads.incrementAndGet();
                while (!shutdown) {

                  long offset1 = 0;
                  synchronized (currOffset) {
                    offset1 = currOffset.addAndGet(2);
                  }
                  BenchmarkDelete.this.insertHighest.set(offset1 - (threadCount * batchSize * 2));
                  try {

                    long thisDuration = 0;
                    for (int attempt = 0; attempt < 4; attempt++) {
                      Connection conn = cpds.getConnection();
                      try {
                        long currBegin = System.nanoTime();
                        PreparedStatement stmt = conn.prepareStatement("delete from persons where id=?");
                        stmt.setLong(1, offset1);
                        stmt.executeUpdate();

                        thisDuration += System.nanoTime() - currBegin;
                        break;
                      }
                      catch (Exception e) {
                        if (attempt == 3) {
                          throw e;
                        }
                        logger.error(ERROR_STR, e);
                      }
                      finally {
                        conn.close();
                      }
                    }

                    threadLiveliness.put(threadOffset, System.currentTimeMillis());
                    totalDuration.addAndGet(thisDuration);
                    countInserted.addAndGet(batchSize);
                    logProgress(threadOffset, countInserted, lastLogged, begin, totalDuration, insertErrorCount);
                    errorCountInARow.set(0);
                  }
                  catch (Exception e) {
                    if (errorCountInARow.incrementAndGet() > 2000) {
                      logger.error("Too many errors, aborting");
                      break;
                    }
                    insertErrorCount.incrementAndGet();
                    if (e.getMessage() != null && e.getMessage().contains("Unique constraint violated")) {
                      logger.error("Unique constraint violation");
                    }
                    else {
                      logger.error("Error inserting", e);
                    }
                  }
                  finally {
                    countFinished.incrementAndGet();
                    logProgress(threadOffset, countInserted, lastLogged, begin, totalDuration, insertErrorCount);
                  }
                }
              }
              finally {
                activeThreads.decrementAndGet();
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
        }

        selectExecutor.shutdownNow();
        executor.shutdownNow();
      }
      catch (Exception e) {
        logger.error(ERROR_STR, e);
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

  private static void logProgress(int threadOffset, AtomicInteger countInserted, AtomicLong lastLogged, long begin, AtomicLong totalDuration, AtomicLong insertErrorCount) {
    if (threadOffset == 0 && System.currentTimeMillis() - lastLogged.get() > 2000) {
      lastLogged.set(System.currentTimeMillis());
      StringBuilder builder = new StringBuilder();
      builder.append("count=").append(countInserted.get());
      Snapshot snapshot = INSERT_STATS.getSnapshot();
      builder.append(String.format(", rate=%.2f", countInserted.get() / (double) (System.currentTimeMillis() - begin) * 1000f));
      builder.append(String.format(", avg=%.2f", (double)totalDuration.get() / (countInserted.get()) / 1000000d));
      builder.append(String.format(", 99th=%.2f", snapshot.get99thPercentile() / 1000000d));
      builder.append(String.format(", max=%.2f", (double) snapshot.getMax() / 1000000d));
      builder.append(", errorCount=").append(insertErrorCount.get());
      logger.info(builder.toString());
    }
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
