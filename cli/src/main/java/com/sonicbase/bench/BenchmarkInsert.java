package com.sonicbase.bench;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.util.concurrent.RateLimiter;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.sonicbase.query.DatabaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class BenchmarkInsert {

  public static final Logger logger = LoggerFactory.getLogger(BenchmarkInsert.class);

  public static boolean STRING_KEY = false;

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

  public interface Converter {
    Object convert(Object value);
  }

  private static class LongConverter implements Converter {

    @Override
    public Object convert(Object value) {
      if (value instanceof Long) {
        return value;
      }

      long ret = 0;
      if (value instanceof String) {
        ret = (long) Long.valueOf((String) value);
      }
      else if (value instanceof char[]) {
        ret = (long) Long.valueOf(new String((char[]) value));
      }
      else if (value instanceof Float) {
        ret = (long) (float) (Float) value;
      }
      else if (value instanceof Double) {
        ret = (long) (double) (Double) value;
      }
      else if (value instanceof Integer) {
        ret = (long) (int) (Integer) value;
      }
      else if (value instanceof Short) {
        ret = (long) (Short) value;
      }
      else if (value instanceof Byte) {
        ret = (long) (Byte) value;
      }
      else {
        throw new DatabaseException("Incompatible datatypes: lhs=long, rhs=" + value.getClass().getName());
      }
      return ret;
    }
  }

  static final LongConverter longConverter = new LongConverter();

  private static class LongComparator implements Comparator {

    @Override
    public int compare(Object o1, Object o2) {
      if (o1 instanceof Long && o2 instanceof Long) {
        long l1 = (Long) o1;
        long l2 = (Long) o2;
        return l1 < l2 ? -1 : l1 > l2 ? 1 : 0;
      }
      if (o1 == null || o2 == null) {
        return 0;
      }
      Long lhs = (Long) longConverter.convert(o1);
      Long rhs = (Long) longConverter.convert(o2);
      return lhs.compareTo(rhs);
    }
  }

  final Comparator[] comparators = new Comparator[]{new LongComparator()};


  final Comparator<Object[]> comparator = (o1, o2) -> {
    for (int i = 0; i < Math.min(o1.length, o2.length); i++) {
      if (o1[i] == null || o2[i] == null) {
        continue;
      }
      int value = comparators[i].compare(o1[i], o2[i]);
      if (value < 0) {
        return -1;
      }
      if (value > 0) {
        return 1;
      }
    }
    return 0;
  };

  final Comparator[] memComparators = new Comparator[]{new LongComparator(), new LongComparator()};


  final Comparator<Object[]> memComparator = (o1, o2) -> {
    for (int i = 0; i < Math.min(o1.length, o2.length); i++) {
      if (o1[i] == null || o2[i] == null) {
        continue;
      }
      int value = memComparators[i].compare(o1[i], o2[i]);
      if (value < 0) {
        return -1;
      }
      if (value > 0) {
        return 1;
      }
    }
    return 0;
  };

  private final AtomicInteger activeThreads = new AtomicInteger();
  private final ConcurrentHashMap<Integer, Long> threadLiveliness = new ConcurrentHashMap<>();
  private int countDead = 0;

  public void start(String address, final AtomicLong insertBegin, final AtomicLong insertHighest, final String cluster,
                    final int shard, final long offset,
                    final long count, final boolean simulate) {
    shutdown = false;
    doResetStats();
    this.insertHighest = insertHighest;
    begin = System.currentTimeMillis();
    mainThread = new Thread(() -> {
      try {
        final ConcurrentSkipListMap<Object[], Object> persons = new ConcurrentSkipListMap<>(comparator);
        final ConcurrentSkipListMap<Object[], Object> memberships = new ConcurrentSkipListMap<>(memComparator);
        final ThreadPoolExecutor executor = new ThreadPoolExecutor(256, 256, 10000,
            TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
        final ThreadPoolExecutor selectExecutor = new ThreadPoolExecutor(256, 256, 10000,
            TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1000), (r, executor1) -> {
              // This will block if the queue is full
              try {
                executor1.getQueue().put(r);
              }
              catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error(ERROR_STR, e);
              }

            });

        final ComboPooledDataSource cpds;
        if (!simulate) {
          logger.info("Using address: address={}", address);

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

        RateLimiter limiter = RateLimiter.create(125_000);

        //test insert
        final AtomicLong countFinished = new AtomicLong();

        final AtomicInteger errorCountInARow = new AtomicInteger();
        final int batchSize = 500;
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
              Random rand = new Random(System.currentTimeMillis());
              try {
                threadLiveliness.put(threadOffset, System.currentTimeMillis());
                activeThreads.incrementAndGet();
                while (!shutdown) {

                  long offset1 = 0;
                  synchronized (currOffset) {
                    offset1 = currOffset.getAndAdd(batchSize);
                  }
                  BenchmarkInsert.this.insertHighest.set(offset1 - (threadCount * batchSize * 2));
                  try {

                    if (batch) {
                      long thisDuration = 0;
                      if (simulate) {
                        for (int i1 = 0; i1 < batchSize; i1++) {
                          if (null != persons.put(new Object[]{offset1 + i1}, new Object())) {
                            throw new DatabaseException("Unique constraint violation - persons: offset=" + offset1 + ", i=" + i1);
                          }
                        }
                        for (int i1 = 0; i1 < batchSize; i1++) {
                          for (int j = 0; j < 2; j++) {
                            if (null != memberships.put(new Object[]{offset1 + 1, (long) j}, new Object())) {
                              throw new DatabaseException("Unique constraint violation - memberships: offset=" + offset1 + ", i=" + i1 + ", j=" + j);
                            }
                          }
                        }
                      }
                      else {
                        if (STRING_KEY) {
                          try (Connection conn = cpds.getConnection()) {
                            try (PreparedStatement stmt = conn.prepareStatement("insert into strings (id1, id2) VALUES ( ?, ?)")) {
                              for (int i1 = 0; i1 < batchSize; i1++) {
                                long id = offset1 + i1;
//                                long id = Math.abs(rand.nextLong());
                                String idString = String.valueOf(id);
                                int len = idString.length();
                                for (int j = 0; j < 20 - len; j++) {
                                  idString = "0" + idString;
                                }
                                stmt.setString(1,  idString);
                                stmt.setLong(2, (id + 100));
                                long currBegin = System.nanoTime();
                                stmt.addBatch();
                                thisDuration += System.nanoTime() - currBegin;
                                //limiter.acquire();
                              }
                              long currBegin = System.nanoTime();
                              stmt.executeBatch();
                              thisDuration += System.nanoTime() - currBegin;

                              threadLiveliness.put(threadOffset, System.currentTimeMillis());
                              totalDuration.addAndGet(thisDuration);
                              countInserted.addAndGet(batchSize);
                              logProgress(threadOffset, countInserted, lastLogged, begin, totalDuration, insertErrorCount);
                            }
                          }
                        }
                        else if (true) {
                          try (Connection conn = cpds.getConnection()) {
                            //try (PreparedStatement stmt = conn.prepareStatement("insert into persons (id1, id2, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?)")) {
                            try (PreparedStatement stmt = conn.prepareStatement("insert into persons (id1, id2, restricted, gender) VALUES ( ?, ?, ?, ?)")) {
                              for (int i1 = 0; i1 < batchSize; i1++) {
                              /*
                               create database db
                               create table Employers (id VARCHAR(64), name VARCHAR(256))
                               create index employerId on Employers(id)
                               create table Persons (id1 BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id1))
                               create table Memberships (personId BIGINT, personId2 BIGINT, membershipName VARCHAR(20), resortId BIGINT, PRIMARY KEY (personId, personId2))
                               create table Resorts (resortId BIGINT, resortName VARCHAR(20), PRIMARY KEY (resortId))
                               */
                                long id = offset1 + i1;
//                                long id = Math.abs(rand.nextLong());
                                stmt.setLong(1,  id);
                                stmt.setLong(2, (id + 100) % 2);
//                                stmt.setString(3, "933-28-" + (offset1 + i1 + 1));
//                                stmt.setString(4, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
                                stmt.setBoolean(3, false);
                                stmt.setString(4, "m");
                                long currBegin = System.nanoTime();
                                stmt.addBatch();
                                thisDuration += System.nanoTime() - currBegin;
                                //limiter.acquire();
                              }
                              long currBegin = System.nanoTime();
                              stmt.executeBatch();
                              thisDuration += System.nanoTime() - currBegin;

                              threadLiveliness.put(threadOffset, System.currentTimeMillis());
                              totalDuration.addAndGet(thisDuration);
                              countInserted.addAndGet(batchSize);
                              logProgress(threadOffset, countInserted, lastLogged, begin, totalDuration, insertErrorCount);
                            }
                          }
                        }

                        if (false) {

                          for (int attempt = 0; attempt < 4; attempt++) {
                            Connection conn = cpds.getConnection();
                            try {
                              PreparedStatement stmt = conn.prepareStatement("insert into employers (id, name) VALUES (?, ?)");
                              for (int i1 = 0; i1 < batchSize; i1++) {
                                stmt.setString(1, String.valueOf(offset1));
                                stmt.setString(2, offset1 + ":" + offset1);
                                long currBegin = System.nanoTime();
                                stmt.addBatch();
                                thisDuration += System.nanoTime() - currBegin;
                              }
                              long currBegin = System.nanoTime();
                              stmt.executeBatch();
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
                        }

                        thisDuration = 0;
                        if (false) {
                          try (Connection conn = cpds.getConnection()) {
                            long currBegin = 0;
                            try (PreparedStatement stmt = conn.prepareStatement("insert into Memberships (personId, personId2, membershipName, resortId) VALUES (?, ?, ?, ?)")) {
                              for (int i1 = 0; i1 < batchSize; i1++) {
                                for (int j = 0; j < 2; j++) {
                                  long id1 = offset1 + i1;
                                  long id2 = j;
                                  stmt.setLong(1, id1);
                                  stmt.setLong(2, id2);
                                  stmt.setString(3, "membership-" + j);
                                  stmt.setLong(4, new long[]{1000, 2000}[j % 2]);
                                  currBegin = System.nanoTime();
                                  stmt.addBatch();
                                  thisDuration += System.nanoTime() - currBegin;
                                  //  limiter.acquire();
                                }
                              }
                              currBegin = System.nanoTime();
                              stmt.executeBatch();
                              thisDuration += System.nanoTime() - currBegin;
                              threadLiveliness.put(threadOffset, System.currentTimeMillis());

                              totalDuration.addAndGet(thisDuration);
                              countInserted.addAndGet(2 * batchSize);
                              logProgress(threadOffset, countInserted, lastLogged, begin, totalDuration, insertErrorCount);
                            }
                          }
                        }
                      }
                    }
                    else {
                      Connection conn = cpds.getConnection();
                      try {
                        for (int i1 = 0; i1 < batchSize; i1++) {
                          PreparedStatement stmt = conn.prepareStatement("insert into persons (id1, id2, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?)");
                          stmt.setLong(1, offset1 + i1);
                          stmt.setLong(2, (offset1 + i1 + 100) % 2);
                          stmt.setString(3, "933-28-" + (offset1 + i1 + 1));
                          stmt.setString(4, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
                          stmt.setBoolean(5, false);
                          stmt.setString(6, "m");
                          long currBegin = System.nanoTime();
                          if (stmt.executeUpdate() != 1) {
                            throw new DatabaseException("Failed to insert");
                          }
                          totalDuration.addAndGet(System.nanoTime() - currBegin);
                          countInserted.incrementAndGet();
                          logProgress(threadOffset, countInserted, lastLogged, begin, totalDuration, insertErrorCount);
                        }

                        if (true) {
                        for (int i1 = 0; i1 < batchSize; i1++) {
                          for (int j = 0; j < 2; j++) {
                            long id1 = offset1 + i1;
                            long id2 = j;
                            try {
                              PreparedStatement stmt = conn.prepareStatement("insert into Memberships (personId, personId2, membershipName, resortId) VALUES (?, ?, ?, ?)");
                              stmt.setLong(1, id1);
                              stmt.setLong(2, id2);
                              stmt.setString(3, "membership-" + j);
                              stmt.setLong(4, new long[]{1000, 2000}[j % 2]);
                              long currBegin = System.nanoTime();
                              totalDuration.addAndGet(System.nanoTime() - currBegin);
                              countInserted.incrementAndGet();
                            }
                            catch (Exception e) {
                              if (e.getMessage().contains("Unique constraint violated")) {
                                logger.error("Unique constraint violation - membership");
                              }
                              else {
                                logger.error("Error inserting membership: id=" + currOffset, e);
                              }
                            }
                          }
                        }
                          logProgress(threadOffset, countInserted, lastLogged, begin, totalDuration, insertErrorCount);
                        }
                      }
                      finally {
                        conn.close();
                      }
                    }

                    errorCountInARow.set(0);
                  }
                  catch (Exception e) {
                    if (errorCountInARow.incrementAndGet() > 2000) {
                      logger.info("Too many errors, aborting");
                      break;
                    }
                    insertErrorCount.incrementAndGet();
                    if (e.getMessage() != null && e.getMessage().contains("Unique constraint violated")) {
                      logger.info("Unique constraint violation");
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
            BenchmarkInsert.this.countDead = countDead;
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
      builder.append(String.format(", rate=%.2f", countInserted.get() / (double) (System.currentTimeMillis() - begin) * 1000f)); //INSERT_STATS.getFiveMinuteRate()));
      builder.append(String.format(", avg=%.2f", (double)totalDuration.get() / (countInserted.get()) / 1000000d));//snapshot.getMean() / 1000000d));
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
