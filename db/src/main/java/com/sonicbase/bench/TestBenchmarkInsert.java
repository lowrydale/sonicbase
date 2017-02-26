package com.sonicbase.bench;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.util.JsonArray;
import com.sonicbase.util.JsonDict;
import com.sonicbase.util.StreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.testng.Assert.assertEquals;

public class TestBenchmarkInsert {

  public static Logger logger = LoggerFactory.getLogger(TestBenchmarkInsert.class);


  private static final MetricRegistry METRICS = new MetricRegistry();

   public static final Timer INSERT_STATS = METRICS.timer("insert");

  public static void main(String[] args) throws Exception {

    final long startId = Long.valueOf(args[0]);
    final String cluster = args[1];

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
    final ThreadPoolExecutor executor = new ThreadPoolExecutor(256, 256, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
    final ThreadPoolExecutor selectExecutor = new ThreadPoolExecutor(256, 256, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new RejectedExecutionHandler() {
                        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                            // This will block if the queue is full
                            try {
                                executor.getQueue().put(r);
                            } catch (InterruptedException e) {
                                System.err.println(e.getMessage());
                            }

                        }});

    Class.forName("com.sonicbase.jdbcdriver.Driver");

//    final java.sql.Connection conn = DriverManager.getConnection("jdbc:voltdb://localhost:21212");

//    final java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:mysql://localhost/test", "test", "test");
//    final java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:mysql://127.0.0.1:4306/test", "test", "test");

    final java.sql.Connection conn = DriverManager.getConnection("jdbc:sonicbase:" + address + ":9010/db");

//    try {
//      PreparedStatement stmt = conn.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, id3 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(21000), restricted boolean, gender VARCHAR(8), PRIMARY KEY (id))");
//      stmt.executeUpdate();
//    }
//    catch (Exception e) {
//      logger.error("error", e);
//    }
//    try {
//      PreparedStatement stmt = conn.prepareStatement("create index socialSecurityNumber on persons(socialSecurityNumber)");
//      stmt.executeUpdate();
//    }
//    catch (Exception e) {
//      logger.error("error", e);
//    }
//    try {
//      PreparedStatement stmt = conn.prepareStatement("create index id2 on persons(id2)");
//      stmt.executeUpdate();
//    }
//    catch (Exception e) {
//      logger.error("error", e);
//    }

//
//    try {
//      PreparedStatement stmt = conn.prepareStatement("create table Memberships (personid BIGINT, personid2 BIGINT, membershipName VARCHAR(20), resortId BIGINT, PRIMARY KEY (personid, membershipName))");
//      stmt.executeUpdate();
//    }
//    catch (Exception e) {
//      logger.error("error", e);
//
//    }
//    try {
//      PreparedStatement stmt = conn.prepareStatement("create table Resorts (resortid BIGINT, resortName VARCHAR(20), PRIMARY KEY (resortid))");
//      stmt.executeUpdate();
//    }
//    catch (Exception e) {
//      logger.error("error", e);
//
//    }


    //test insert
    final AtomicLong countFinished = new AtomicLong();

//    PreparedStatement stmt = conn.prepareStatement("insert into Resorts (resortId, resortName) VALUES (?, ?)");
//    stmt.setLong(1, 1000);
//    stmt.setString(2, "resort-1000");
//    assertEquals(stmt.executeUpdate(), 1);
//
//    stmt = conn.prepareStatement("insert into Resorts (resortId, resortName) VALUES (?, ?)");
//    stmt.setLong(1, 2000);
//    stmt.setString(2, "resort-2000");
//    assertEquals(stmt.executeUpdate(), 1);

//    Thread thread = new Thread(new Runnable(){
//      @Override
//      public void run() {
//        while (true) {
//          try {
//            PreparedStatement stmt = conn.prepareStatement("select persons.id1, id2  " +
//                "from persons where id1 >= " + startId + " order by id asc");                                              //
//            ResultSet ret = stmt.executeQuery();
//
//            int count = 0;
//            for (int i = 0; ; i++) {
//
//              if (!ret.next()) {
//                break;
//              }
////              if (ret.getLong("id") != i) {
////                IndexSchema indexSchema = ((ConnectionProxy)conn).getDatabaseClient().getSchema("test").getTables().get("persons").getIndices().get("_1__primarykey");
////                Long upper = null;
////                if (indexSchema.getCurrPartitions() != null && indexSchema.getCurrPartitions()[0].getUpperKey() != null) {
////                  upper = (Long) indexSchema.getCurrPartitions()[0].getUpperKey()[0];
////                }
////                Long lastUpper = null;
////                if (indexSchema.getLastPartitions() != null && indexSchema.getLastPartitions()[0].getUpperKey() != null) {
////                  lastUpper = (Long) indexSchema.getLastPartitions()[0].getUpperKey()[0];
////                }
////                System.out.println("expected=" + i + ", found=" + ret.getLong("id") + ", currUpper=" + upper + ", lastUpper=" + lastUpper + ", schemaVersion=" +
////                    ((ConnectionProxy)conn).getDatabaseClient().getCommon().getSchemaVersion());
////                i = (int) ret.getLong("id");
////              }
//              //assertEquals(ret.getLong("id"), i);
//              count++;
//            }
//            System.out.println("Read count=" + count);
//          }
//          catch (Exception e) {
//            logger.error("Read error", e);
//          }
//        }
//      }
//    });
//    thread.start();

    final int batchSize = 1000;
    final AtomicInteger countInserted = new AtomicInteger();
    final AtomicLong insertErrorCount = new AtomicLong();
    final long begin = System.currentTimeMillis();
    final AtomicLong totalDuration = new AtomicLong();
    if (true) {
      final AtomicLong currOffset = new AtomicLong(startId);
      for (int i = 0; i < 256; i++) {
        final int threadOffset = i;
        final AtomicLong lastLogged = new AtomicLong(System.currentTimeMillis());
        Thread insertThread = new Thread(new Runnable() {
          @Override
          public void run() {
            while (true) {

              long offset = 0;
              synchronized (currOffset) {
                offset = currOffset.getAndAdd(batchSize);
              }
              try {
                //              PreparedStatement stmt = conn.prepareStatement("insert into Resorts (id, resortName) VALUES (?, ?)");
                //              stmt.setLong(1, currOffset);
                //              stmt.setString(2, "resort-" + currOffset);
                //              assertEquals(stmt.executeUpdate(), 1);
                //
                //              stmt = conn.prepareStatement("insert into Memberships (id, membershipName, resortId) VALUES (?, ?, ?)");
                //              stmt.setLong(1, currOffset);
                //              stmt.setString(2, "membership-" + 0);
                //              stmt.setLong(3, currOffset % 10);
                //              assertEquals(stmt.executeUpdate(), 1);

                //              for (int j = 0; j < 2; j++) {
                //                try {
                //                  PreparedStatement stmt = conn.prepareStatement("insert into Memberships (personId, personId2, membershipName, resortId) VALUES (?, ?, ?, ?)");
                //                  stmt.setLong(1, currOffset);
                //                  stmt.setLong(2, (currOffset + 100) % 3);
                //                  stmt.setString(3, "membership-" + j);
                //                  stmt.setLong(4, new long[]{1000, 2000}[j % 2]);
                //                  assertEquals(stmt.executeUpdate(), 1);
                //                }
                //                catch (Exception e) {
                //                  if (e.getMessage().contains("Unique constraint violated")) {
                //                    logger.error("Unique constraint violation - membership");
                //                  }
                //                  else {
                //                    logger.error("Error inserting membership: id=" + currOffset, e);
                //                  }
                //                }
                //              }

                //Thread.sleep(15);
                //while (true) {
                try {
                 // System.out.println("Inserting: id=" + offset);

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
                        assertEquals(stmt.executeUpdate(), 1);
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
                    logProgress(offset, threadOffset, countInserted, lastLogged, begin, totalDuration, insertErrorCount);
                  }

                  if (offset == 0) {
                    PreparedStatement stmt = conn.prepareStatement("insert into Resorts (resortId, resortName) VALUES (?, ?)");
                    stmt.setLong(1, 1000);
                    stmt.setString(2, "resort-1000");
                    assertEquals(stmt.executeUpdate(), 1);

                    stmt = conn.prepareStatement("insert into Resorts (resortId, resortName) VALUES (?, ?)");
                    stmt.setLong(1, 2000);
                    stmt.setString(2, "resort-2000");
                    assertEquals(stmt.executeUpdate(), 1);
                  }
                  //                  stmt = conn.prepareStatement("select persons.id, id2  " +
                  //                      "from persons where id = " + currOffset + " order by id asc");                                              //
                  //                  ResultSet ret = stmt.executeQuery();
                  //                  if (ret.next()) {
                  //                    break;
                  //                  }
                  //break;
                  //System.out.println("retrying");
                }
                catch (Exception e) {
                  if (e.getMessage().contains("Unique constraint violated")) {
                    System.out.println("Unique constraint violation - adding person");
                  }
                  else {
                    System.out.println("Error inserting person");
                    e.printStackTrace();
                  }
                }
                //}

              }
              catch (Exception e) {
                insertErrorCount.incrementAndGet();
                if (e.getMessage().contains("Unique constraint violated")) {
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
        });
        insertThread.start();
      }

      while (true) {
        Thread.sleep(1000);
      }

//      logger.info("checking test");
//      checkAddedRecords(null, null);
//
//      logger.info("checking client insert");
//      ((ConnectionProxy)conn).getDatabaseClient().checkAddedRecords(null, null);
    }

    selectExecutor.shutdownNow();
    executor.shutdownNow();
  }

  private static void logProgress(long offset, int threadOffset, AtomicInteger countInserted, AtomicLong lastLogged, long begin, AtomicLong totalDuration, AtomicLong insertErrorCount) {
    if (threadOffset == 0) {
      if (System.currentTimeMillis() - lastLogged.get() > 2000) {
        lastLogged.set(System.currentTimeMillis());
        StringBuilder builder = new StringBuilder();
        builder.append("count=").append(countInserted.get());
        Snapshot snapshot = INSERT_STATS.getSnapshot();
        builder.append(String.format(", rate=%.2f", countInserted.get() / (double)(System.currentTimeMillis() - begin) * 1000f)); //INSERT_STATS.getFiveMinuteRate()));
        builder.append(String.format(", avg=%.2f", totalDuration.get() / (countInserted.get()) / 1000000d));//snapshot.getMean() / 1000000d));
        builder.append(String.format(", 99th=%.2f", snapshot.get99thPercentile() / 1000000d));
        builder.append(String.format(", max=%.2f", (double)snapshot.getMax() / 1000000d));
        builder.append(", errorCount=" + insertErrorCount.get());
        System.out.println(builder.toString());
      }
    }
  }

  private static ConcurrentHashMap<Long, Integer> addedRecords = new ConcurrentHashMap<>();
  public static byte[] checkAddedRecords(String command, byte[] body) {
    logger.info("begin checkAddedRecords");
    for (int i = 0; i < 1000000; i++) {
      if (addedRecords.get((long)i) == null) {
        logger.error("missing record: id=" + i + ", count=0");
      }
    }
    logger.info("finished checkAddedRecords");
    return null;
  }


}
