package com.sonicbase.bench;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.sonicbase.query.DatabaseException;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class BenchmarkInsert {

  public static Logger logger = LoggerFactory.getLogger(BenchmarkInsert.class);


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
    final BenchmarkInsert insert = new BenchmarkInsert();
    for (int i = 0; i < 4; i++) {
      final int shard = i;
      threads[i] = new Thread(new Runnable() {
        @Override
        public void run() {
          insert.start(new AtomicLong(), new AtomicLong(), "1-local", 4, 0, 1000000000, true);
        }
      });
      threads[i].start();
    }
  }

  public static interface Converter {
    public Object convert(Object value);
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
      else if (value instanceof byte[]) {
        try {
          ret = (long) Long.valueOf(new String((byte[]) value, "utf-8"));
        }
        catch (UnsupportedEncodingException e) {
          throw new DatabaseException(e);
        }
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

  static LongConverter longConverter = new LongConverter();

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


  Comparator<Object[]> comparator = new Comparator<Object[]>() {
    @Override
    public int compare(Object[] o1, Object[] o2) {
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
    }
  };

  final Comparator[] memComparators = new Comparator[]{new LongComparator(), new LongComparator()};


  Comparator<Object[]> memComparator = new Comparator<Object[]>() {
    @Override
    public int compare(Object[] o1, Object[] o2) {
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
    }
  };

  private AtomicInteger activeThreads = new AtomicInteger();
  private ConcurrentHashMap<Integer, Long> threadLiveliness = new ConcurrentHashMap<>();
  private int countDead = 0;

  public void start(final AtomicLong insertBegin, final AtomicLong insertHighest, final String cluster,
                    final int shard, final long offset,
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
          final ConcurrentSkipListMap<Object[], Object> persons = new ConcurrentSkipListMap<>(comparator);
          final ConcurrentSkipListMap<Object[], Object> resorts = new ConcurrentSkipListMap<>(comparator);
          final ConcurrentSkipListMap<Object[], Object> memberships = new ConcurrentSkipListMap<>(memComparator);
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
          final int batchSize = 2000;
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
                        offset = currOffset.getAndAdd(batchSize);
                      }
                      BenchmarkInsert.this.insertHighest.set(offset - (threadCount * batchSize * 2));
                      //                    if (offset >= startId + count) {//((cycleNum.get() - 1) * shardCount * count) + ((shard + 1) * count)) {
                      //                      break;
                      //                    }
                      try {

                        if (batch) {
                          long thisDuration = 0;
                          //Thread.sleep(5000);
                          //conn.setAutoCommit(false);
                          if (simulate) {
                            for (int i = 0; i < batchSize; i++) {
                              if (null != persons.put(new Object[]{offset + i}, new Object())) {
                                throw new DatabaseException("Unique constraint violation - persons: offset=" + offset + ", i=" + i);
                              }
                            }
                            for (int i = 0; i < batchSize; i++) {
                              for (int j = 0; j < 2; j++) {
                                if (null != memberships.put(new Object[]{offset + 1, (long) j}, new Object())) ;
                                throw new DatabaseException("Unique constraint violation - memberships: offset=" + offset + ", i=" + i + ", j=" + j);
                              }
                            }
                          }
                          else {
                            if (true) {
                              for (int attempt = 0; attempt < 4; attempt++) {
                                Connection conn = cpds.getConnection();
                                try {
                                  PreparedStatement stmt = conn.prepareStatement("insert into persons (id1, id2, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?)");
                                  for (int i = 0; i < batchSize; i++) {
                                  /*
                                   create database db
                                   create table Employers (id VARCHAR(64), name VARCHAR(256))
                                   create index employerId on Employers(id)
                                   create table Persons (id1 BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id1))
                                   create table Memberships (personId BIGINT, personId2 BIGINT, membershipName VARCHAR(20), resortId BIGINT, PRIMARY KEY (personId, personId2))
                                   create table Resorts (resortId BIGINT, resortName VARCHAR(20), PRIMARY KEY (resortId))
                                   */
                                    stmt.setLong(1, offset + i);
                                    stmt.setLong(2, (offset + i + 100) % 2);
                                    stmt.setString(3, "933-28-" + (offset + i + 1));
                                    stmt.setString(4, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
                                    stmt.setBoolean(5, false);
                                    stmt.setString(6, "m");
                                    //Timer.Context ctx = INSERT_STATS.time();
                                    long currBegin = System.nanoTime();
                                    stmt.addBatch();
                                    thisDuration += System.nanoTime() - currBegin;

                                    //ctx.stop();
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
                            }

                            if (false) {

                              for (int attempt = 0; attempt < 4; attempt++) {
                                Connection conn = cpds.getConnection();
                                try {
                                  PreparedStatement stmt = conn.prepareStatement("insert into employers (id, name) VALUES (?, ?)");
                                  for (int i = 0; i < batchSize; i++) {
                                  /*
                                  start bulk import from persons(com.sonicbase.jdbcdriver.Driver, jdbc:sonicbase:10.0.0.135:9010/db) where id1 > 10
                                  start bulk import from memberships(com.sonicbase.jdbcdriver.Driver, jdbc:sonicbase:10.0.0.135:9010/db) where personid > 10

                                   create table Employers (id VARCHAR(64), name VARCHAR(256))
                                   create index employerId on Employers(id)
                                   create table Persons (id1 BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id1))
                                   create table Memberships (personId BIGINT, personId2 BIGINT, membershipName VARCHAR(20), resortId BIGINT, PRIMARY KEY (personId, personId2))
                                   create table Resorts (resortId BIGINT, resortName VARCHAR(20), PRIMARY KEY (resortId))
                                   */
                                    stmt.setString(1, String.valueOf(offset));
                                    stmt.setString(2, offset + ":" + offset);
                                    //Timer.Context ctx = INSERT_STATS.time();
                                    long currBegin = System.nanoTime();
                                    stmt.addBatch();
                                    thisDuration += System.nanoTime() - currBegin;

                                    //ctx.stop();
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
                            }

                            thisDuration = 0;
                            if (true) {
                              for (int attempt = 0; attempt < 4; attempt++) {
                                Connection conn = cpds.getConnection();
                                long currBegin = 0;
                                try {
                                  //conn.setAutoCommit(false);
                                  PreparedStatement stmt = conn.prepareStatement("insert into Memberships (personId, personId2, membershipName, resortId) VALUES (?, ?, ?, ?)");
                                  for (int i = 0; i < batchSize; i++) {
                                    for (int j = 0; j < 2; j++) {
                                      long id1 = offset + i;
                                      long id2 = j;
                                      stmt.setLong(1, id1);
                                      stmt.setLong(2, id2);
                                      stmt.setString(3, "membership-" + j);
                                      stmt.setLong(4, new long[]{1000, 2000}[j % 2]);
                                      currBegin = System.nanoTime();
                                      stmt.addBatch();
                                      thisDuration += System.nanoTime() - currBegin;
                                    }
                                  }
                                  currBegin = System.nanoTime();
                                  stmt.executeBatch();
                                  thisDuration += System.nanoTime() - currBegin;
                                  threadLiveliness.put(threadOffset, System.currentTimeMillis());
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
                            //                          stmt = conn.prepareStatement("insert into Employers (id, name) VALUES (?, ?)");
                            //                          for (int i = 0; i < batchSize; i++) {
                            //                            long id1 = offset + i;
                            //                            stmt.setString(1, String.valueOf(id1));
                            //                            stmt.setString(2, "name-" + String.valueOf(id1) + 100000);
                            //                            long currBegin = System.nanoTime();
                            //                            stmt.addBatch();
                            //                            thisDuration += System.nanoTime() - currBegin;
                            //                          }
                            //                          stmt.executeBatch();
                          totalDuration.addAndGet(thisDuration);
                          countInserted.addAndGet(2 * batchSize);
                          logProgress(offset, threadOffset, countInserted, lastLogged, begin, totalDuration, insertErrorCount);
                            }
                          //conn.commit();
                          }
                        }
                        else {
                          Connection conn = cpds.getConnection();
                          try {
                            for (int i = 0; i < batchSize; i++) {
                              // create table Persons (id1 BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id1))
                              // create table Memberships (personId BIGINT, personId2 BIGINT, membershipName VARCHAR(20), resortId BIGINT, PRIMARY KEY (personId, personId2))
                              // create table Resorts (resortId BIGINT, resortName VARCHAR(20), PRIMARY KEY (resortId))
                              PreparedStatement stmt = conn.prepareStatement("insert into persons (id1, id2, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?)");
                              stmt.setLong(1, offset + i);
                              stmt.setLong(2, (offset + i + 100) % 2);
                              stmt.setString(3, "933-28-" + (offset + i + 1));
                              stmt.setString(4, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
                              stmt.setBoolean(5, false);
                              stmt.setString(6, "m");
                              //Timer.Context ctx = INSERT_STATS.time();
                              long currBegin = System.nanoTime();
                              if (stmt.executeUpdate() != 1) {
                                throw new DatabaseException("Failed to insert");
                              }
                              totalDuration.addAndGet(System.nanoTime() - currBegin);
                              countInserted.incrementAndGet();
                              logProgress(offset, threadOffset, countInserted, lastLogged, begin, totalDuration, insertErrorCount);

                              //ctx.stop();
                            }

                            if (false) {
                            for (int i = 0; i < batchSize; i++) {
                              for (int j = 0; j < 2; j++) {
                                long id1 = offset + i;
                                long id2 = j;
                                try {
                                  PreparedStatement stmt = conn.prepareStatement("insert into Memberships (personId, personId2, membershipName, resortId) VALUES (?, ?, ?, ?)");
                                  stmt.setLong(1, id1);
                                  stmt.setLong(2, id2);
                                  stmt.setString(3, "membership-" + j);
                                  stmt.setLong(4, new long[]{1000, 2000}[j % 2]);
                                  long currBegin = System.nanoTime();
                                  //assertEquals(stmt.executeUpdate(), 1);
                                  totalDuration.addAndGet(System.nanoTime() - currBegin);
                                  countInserted.incrementAndGet();
                                  //ctx.stop();
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
                              logProgress(offset, threadOffset, countInserted, lastLogged, begin, totalDuration, insertErrorCount);
                            }
                          }
                          finally {
                            conn.close();
                          }
                        }

                        //                  stmt = conn.prepareStatement("select persons.id, id2  " +
                        //                      "from persons where id = " + currOffset + " order by id asc");                                              //
                        //                  ResultSet ret = stmt.executeQuery();
                        //                  if (ret.next()) {
                        //                    break;
                        //                  }
                        //break;
                        //System.out.println("retrying");
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
              BenchmarkInsert.this.countDead = countDead;
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
