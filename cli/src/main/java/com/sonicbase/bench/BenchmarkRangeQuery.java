package com.sonicbase.bench;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
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

public class BenchmarkRangeQuery {

  public static Logger logger = LoggerFactory.getLogger(BenchmarkRangeQuery.class);

  private static final MetricRegistry METRICS = new MetricRegistry();

  public static final com.codahale.metrics.Timer LOOKUP_STATS = METRICS.timer("lookup");
  private Thread mainThread;
  private boolean shutdown;
  final AtomicLong totalBegin = new AtomicLong(System.currentTimeMillis());
  final AtomicLong totalSelectDuration = new AtomicLong();
  final AtomicLong selectErrorCount = new AtomicLong();
  final AtomicLong selectBegin = new AtomicLong(System.currentTimeMillis());
  final AtomicLong selectOffset = new AtomicLong();
  final AtomicLong selectCount = new AtomicLong();
  final AtomicLong readCount = new AtomicLong();

  public void start(final String cluster, final int shardCount, final Integer shard, final long count) {
    shutdown = false;
    selectBegin.set(System.currentTimeMillis());
    doResetStats();
    mainThread = new Thread(new Runnable(){
      @Override
      public void run() {
        try {
          final AtomicInteger cycle = new AtomicInteger();
          final long startId = shard * count;
          final long countForShard = count;
          System.out.println("startId=" + startId + ", count=" + count + ", shard=" + shard);
          logger.info("startId=" + startId + ", count=" + count + ", shard=" + shard);

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


          final ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
          final ThreadPoolExecutor selectExecutor = new ThreadPoolExecutor(256, 256, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

          Class.forName("com.sonicbase.jdbcdriver.Driver");


//       final java.sql.Connection conn = DriverManager.getConnection("jdbc:voltdb://localhost:21212");

//    final java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:mysql://localhost/test", "test", "test");
//    final java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:mysql://127.0.0.1:4306/test", "test", "test");

          final java.sql.Connection conn = DriverManager.getConnection("jdbc:sonicbase:" + address + ":9010/db", "user", "password");

          //test insert
          try {
            Thread[] threads = new Thread[64];
            for (int i = 0; i < threads.length; i++) {
              threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                  while (!shutdown) {
                    try {
                      long beginSelect = System.nanoTime();
                      PreparedStatement stmt = null;
                      ResultSet ret = null;
                      if (true) {
                        //primary index 2 expressions order desc
                        //700k

                        long actualStartId = startId;//cycle.get() == 0 ? startId : 0;
                        cycle.incrementAndGet();
                        long begin = System.nanoTime();
                        stmt = conn.prepareStatement("select persons.id1  " +
                            "from persons where id1 >= " + actualStartId + " order by id1 asc");                                              //
                        long beginNano = System.nanoTime();
                        ret = stmt.executeQuery();
                        totalSelectDuration.addAndGet(System.nanoTime() - begin);

                        int count = 0;
                        while (true) {
                          begin = System.nanoTime();
                          //Timer.Context context = LOOKUP_STATS.time();
                          if (!ret.next()) {
                            break;
                          }
//                          if (ret.getLong("id1") > startId + 1000000) {
//                            break;
//                          }
                          totalSelectDuration.addAndGet(System.nanoTime() - begin);
                          //context.stop();

                          if (readCount.incrementAndGet() % 100000 == 0) {
                            StringBuilder builder = new StringBuilder();
                            builder.append("count=").append(readCount.get());
                            Snapshot snapshot = LOOKUP_STATS.getSnapshot();
                            builder.append(String.format(", rate=%.2f", readCount.get() / (double) (System.currentTimeMillis() - totalBegin.get()) * 1000f));//LOOKUP_STATS.getFiveMinuteRate()));
                            builder.append(String.format(", avg=%.2f nanos", totalSelectDuration.get() / (double) readCount.get()));//snapshot.getMean()));
                            builder.append(String.format(", 99th=%.2f nanos", snapshot.get99thPercentile()));
                            builder.append(String.format(", max=%d", snapshot.getMax()));
                            builder.append(", errorCount=" + selectErrorCount.get());

                            System.out.println(builder.toString());
                          }
                        }
                        totalSelectDuration.addAndGet(System.nanoTime() - beginNano);
                      }
                      else if (false) {
                        //primary index 2 expressions order desc
                        //700k
                        stmt = conn.prepareStatement("select persons.id, id2  " +
                            "from persons where id > 0 order by id2 desc");                                              //
                        ret = stmt.executeQuery();
                      }
                      else if (false) {
                        //primary index 2 expressions order desc
                        //700k
                        stmt = conn.prepareStatement("select persons.id  " +
                            "from persons where persons.id>100000 AND id < 2000000 order by id desc");                                              //
                        ret = stmt.executeQuery();
                      }
                      else if (false) {
                        //primary index 2 expressions
                        //700k
                        stmt = conn.prepareStatement("select persons.id  " +
                            "from persons where persons.id>100000 AND id < 120000");                                              //
                        ret = stmt.executeQuery();
                      }
                      else if (false) {
                        //primary index 'or' separating 'and's
                        //700k
                        stmt = conn.prepareStatement("select persons.id  " +
                            "from persons where persons.id>100000 AND id < 120000 OR id> 500000 AND ID < 600000");                                              //
                        ret = stmt.executeQuery();
                      }
                      else if (false) {
                        //370k
                        stmt = conn.prepareStatement("select persons.id  " +
                            "from persons where persons.id>100000 AND id < 120000 AND ID2>10 OR id> 500000 AND ID < 600000");                                              //
                        ret = stmt.executeQuery();
                      }
                      else if (false) {
                        stmt = conn.prepareStatement("select persons.id  " +
                            "from persons where persons.id>100000 AND id < 200000 AND socialsecuritynumber > '933-28-0'");                                              //
                        ret = stmt.executeQuery();
                      }
                      else if (false) {
                        //primary index overrides secondary index
                        //700k
                        stmt = conn.prepareStatement("select persons.id  " +
                            "from persons where socialsecuritynumber > '933-28-0' AND persons.id>100000 AND id < 200000");                                              //
                        ret = stmt.executeQuery();
                      }
                      else if (false) {
                        //primary and secondary index
                        //420k
                        stmt = conn.prepareStatement("select persons.id  " +
                            "from persons where socialsecuritynumber > '933-28-100000' AND socialsecuritynumber < '933-28-120000'  AND persons.id>100000 AND id < 200000");                                              //
                        ret = stmt.executeQuery();
                      }
                      else if (false) {
                        //secondary index overrides primary index
                        //270k
                        stmt = conn.prepareStatement("select persons.id  " +
                            "from persons where socialsecuritynumber > '933-28-100000' AND socialsecuritynumber < '933-28-120000'  AND persons.id>100000");                                              //
                        ret = stmt.executeQuery();
                      }
                      else if (false) {
                        //secondary index
                        //350k
                        stmt = conn.prepareStatement("select persons.id  " +
                            "from persons where socialsecuritynumber > '933-28-0'");                                              //
                        ret = stmt.executeQuery();
                      }
                      else if (false) {
                        //multiple secondary indices
                        //220k
                        stmt = conn.prepareStatement("select persons.id  " +
                            "from persons where socialsecuritynumber > '933-28-0' AND ID2>100000 AND ID2 < 1000000");                                              //
                        ret = stmt.executeQuery();
                      }
                      else if (false) {
                        //primary index overrides secondary index mixed order
                        //700k
                        stmt = conn.prepareStatement("select persons.id  " +
                            "from persons where persons.id>100000 AND socialsecuritynumber > '933-28-0' AND id < 200000");                                              //
                        ret = stmt.executeQuery();
                      }
                      else if (false) {
                        //primary index in operator
                        //100k
                        stmt = conn.prepareStatement("select * from persons where id not in (5, 6, 7, 8, 9, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109) order by id asc");
                        ret = stmt.executeQuery();
                      }
                      else if (false) {
                        //where all
                        //700k
                        stmt = conn.prepareStatement("select * from persons");
                        ret = stmt.executeQuery();
                      }
                      else if (false) {
                        //todo: needs to exhaust query before sorting
                        stmt = conn.prepareStatement("select * from persons order by id2 asc, id desc");
                        ret = stmt.executeQuery();
                      }
                      else if (false) {
                        stmt = conn.prepareStatement("select * from persons where restricted=1 order by id desc");
                        ret = stmt.executeQuery();
                      }
                      else if (false) {
                        //not equal table scan
                        //170k
                        stmt = conn.prepareStatement("select * from persons where id!=0 AND id!=1 AND id!=2 AND id!=3 AND id!=4 order by id asc");
                        ret = stmt.executeQuery();
                      }
                      else if (false) {
                        stmt = conn.prepareStatement("select * from persons where restricted>60 or restricted<50 order by restricted asc, id desc");
                        ret = stmt.executeQuery();
                      }
                      else if (false) {
                        stmt = conn.prepareStatement("select * from persons where id>restricted order by restricted asc, id desc");
                        ret = stmt.executeQuery();
                      }
                      selectCount.incrementAndGet();


                    }
                    catch (Exception e) {
                      e.printStackTrace();
                    }
                  }
                }

              });
              threads[i].start();
            }
          }
          catch (Exception e) {
            e.printStackTrace();
          }
          while (true) {
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
