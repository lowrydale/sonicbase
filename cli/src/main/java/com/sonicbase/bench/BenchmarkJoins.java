package com.sonicbase.bench;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class BenchmarkJoins {

  public static Logger logger = LoggerFactory.getLogger(BenchmarkJoins.class);
  private Thread mainThread;
  private boolean shutdown;

  final AtomicLong totalSelectDuration = new AtomicLong();
  final AtomicLong selectErrorCount = new AtomicLong();
  final AtomicLong selectBegin = new AtomicLong(System.currentTimeMillis());
  final AtomicLong selectOffset = new AtomicLong();
  final AtomicLong maxDuration = new AtomicLong();

  public void start(final String cluster, final int shardCount, final Integer shard, final long count, final String queryType) {
    shutdown = false;
    selectBegin.set(System.currentTimeMillis());
    doResetStats();
    mainThread = new Thread(new Runnable(){
      @Override
      public void run() {
        try {
          final AtomicInteger cycle = new AtomicInteger();
          final long startId = shard * count;

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

          //   FileUtils.deleteDirectory(new File("/data/database"));

          final ThreadPoolExecutor executor = new ThreadPoolExecutor(3, 3, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
          final ThreadPoolExecutor selectExecutor = new ThreadPoolExecutor(256, 256, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

          Class.forName("com.sonicbase.jdbcdriver.Driver");


//    final java.sql.Connection conn = DriverManager.getConnection("jdbc:voltdb://localhost:21212");

//    final java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:mysql://localhost/test", "test", "test");
//    final java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:mysql://127.0.0.1:4306/test", "test", "test");

          final java.sql.Connection conn = DriverManager.getConnection("jdbc:sonicbase:" + address + ":9010/db", "user", "password");

          //test insert
          int recordCount = 100000;
          final AtomicLong selectBegin = new AtomicLong(System.currentTimeMillis());
          final AtomicLong maxDuration = new AtomicLong();

          Thread[] threads = new Thread[1];
          for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(new Runnable() {
              @Override
              public void run() {
                while (!shutdown) {
                  try {
  //                    PreparedStatement stmt1 = conn.prepareStatement("select count(*) from persons");
  //
  //                    ResultSet ret1 = stmt1.executeQuery();
  //                    ret1.next();
                      long count = 60000000;//ret1.getLong(1);

                      long actualStartId = cycle.get() == 0 ? startId : 0;
                      cycle.incrementAndGet();
                      long countExpected = 0;
                      long beginSelect = System.nanoTime();
                      //memberships.membershipname, resorts.resortname
                      ResultSet ret = null;
                      PreparedStatement stmt = null;
                      if (queryType.equals("3wayInner")) {
                        stmt = conn.prepareStatement("select persons.id1, socialsecuritynumber, memberships.personId2  " +
                            "from persons inner join Memberships on persons.id1 = Memberships.personId1 inner join resorts on memberships.resortid = resorts.id" +
                            " where persons.id>" + actualStartId);

                        ret = stmt.executeQuery();
                        countExpected = count;
                      }
                      else if (queryType.equals("2wayInner")) {
                        //select persons.id1, persons.id2, persons.socialsecuritynumber, memberships.personId, memberships.personid2, memberships.membershipname from persons inner join Memberships on persons.id1 = Memberships.PersonId where persons.id1 > 0 order by persons.id1 desc
                        stmt = conn.prepareStatement(
                            "select persons.id1, persons.id2, persons.socialsecuritynumber, memberships.personId, memberships.personid2, memberships.membershipname from persons " +
                                " inner join Memberships on persons.id1 = Memberships.PersonId where persons.id1 > " + actualStartId + " order by persons.id1 asc");
                        ret = stmt.executeQuery();
                        countExpected = count;
                      }
                      else if (queryType.equals("2wayLeftOuter")) {
                        stmt = conn.prepareStatement(
                            "select persons.id1, persons.socialsecuritynumber, memberships.membershipname, memberships.personid from memberships " +
                                "left outer join Memberships on persons.id1 = Memberships.PersonId where memberships.personid>" + actualStartId + " order by memberships.personid desc");
                        ret = stmt.executeQuery();
                        countExpected = count;
                      }
                      else if (queryType.equals("2wayInner2Field")) {
                        stmt = conn.prepareStatement(
                            "select persons.id1, persons.socialsecuritynumber, memberships.membershipname from persons " +
                                " inner join Memberships on Memberships.PersonId = persons.id1 and memberships.personid > " + actualStartId + " where persons.id1 > 0 order by persons.id1 desc");
                        ret = stmt.executeQuery();
                        countExpected = count;
                      }
                      else if (queryType.equals("2wayRightOuter")) {
                        stmt = conn.prepareStatement(
                            "select persons.id1, persons.socialsecuritynumber, memberships.personId, memberships.membershipname from persons " +
                                "right outer join Memberships on persons.id1 = Memberships.PersonId where persons.id1>" + actualStartId + " order by persons.id1 desc");
                        ret = stmt.executeQuery();
                        countExpected = count;
                      }
                      totalSelectDuration.addAndGet(System.nanoTime() - beginSelect);

                      //System.out.println("Initial Query: duration=" + (System.nanoTime() - beginSelect));
                      while (true) {
                        //beginSelect = System.nanoTime();
                        if (selectOffset.incrementAndGet() % 100000 == 0) {
                          StringBuilder builder = new StringBuilder();
                          builder.append("select: count=").append(selectOffset.get()).append(", rate=");
                          builder.append(selectOffset.get() / (float) (System.currentTimeMillis() - selectBegin.get()) * 1000f).append("/sec");
                          builder.append(", avgDuration=" + (float) (totalSelectDuration.get() / (float) selectOffset.get() / 1000000f) + " millis, ");
                          builder.append(", maxDuration=" + maxDuration.get() / 1000000f + " millis, ");
                          builder.append("errorCount=" + selectErrorCount.get());
                          //                                    builder.append("0=" + ((ConnectionProxy)conn).getDatabaseClient().getPartitionSize("test", 0, 0, "persons", "_1__primarykey")).append(",");
                          //                                     builder.append("1=" + ((ConnectionProxy)conn).getDatabaseClient().getPartitionSize("test", 0, 1, "persons", "_1__primarykey")).append(",");
                          //                                     builder.append("0=" + ((ConnectionProxy)conn).getDatabaseClient().getPartitionSize("test", 1, 0, "persons", "_1__primarykey")).append(",");
                          //                                     builder.append("1=" + ((ConnectionProxy)conn).getDatabaseClient().getPartitionSize("test", 1, 1, "persons", "_1__primarykey")).append(",");

                          System.out.println(builder.toString());
                        }
                        long begin = System.nanoTime();
                        if (!ret.next()) {
                          ret.close();
                          stmt.close();
                          break;
                          //throw new Exception("Not found: at=" + i);
                        }
                        totalSelectDuration.addAndGet(System.nanoTime() - begin);
//                        if (System.nanoTime() - begin > 1_000_000) {
//                          System.out.println("Next Query: duration=" + (System.nanoTime() - begin));
//                        }
                      }
  //            long duration = System.nanoTime() - beginSelect;
  //            synchronized (maxDuration) {
  //              maxDuration.set(Math.max(maxDuration.get(), duration));
  //            }
  //            totalSelectDuration.addAndGet(duration);

                  }
                  catch (Exception e)

                  {
                    selectErrorCount.incrementAndGet();
                    e.printStackTrace();
                    //               e.printStackTrace();
                  }

                  finally

                  {
                    if (selectOffset.incrementAndGet() % 10000 == 0) {
                      StringBuilder builder = new StringBuilder();
                      builder.append("select: count=").append(selectOffset.get()).append(", rate=");
                      builder.append(selectOffset.get() / (float) (System.currentTimeMillis() - selectBegin.get()) * 1000f).append("/sec");
                      builder.append(", avgDuration=" + (totalSelectDuration.get() / selectOffset.get() / 1000000) + " millis, ");
                      builder.append("errorCount=" + selectErrorCount.get());
                      System.out.println(builder.toString());
                    }
                  }

                }
              }});
            threads[i].start();
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
    maxDuration.set(0);
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
    dict.put("countDead", 0);
    dict.put("activeThreads", 0);
    return dict.toString();
  }

  public void resetStats() {
    doResetStats();
  }
}
