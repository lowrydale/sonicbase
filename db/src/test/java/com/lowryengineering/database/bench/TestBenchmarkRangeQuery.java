package com.lowryengineering.database.bench;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.lowryengineering.database.client.DatabaseClient;
import com.lowryengineering.database.jdbcdriver.ConnectionProxy;
import com.lowryengineering.database.jdbcdriver.StatementProxy;
import com.lowryengineering.database.query.impl.ResultSetImpl;
import com.lowryengineering.database.util.JsonArray;
import com.lowryengineering.database.util.JsonDict;
import com.lowryengineering.database.util.StreamUtils;
import com.lowryengineering.research.socket.NettyServer;
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
import java.util.concurrent.atomic.AtomicLong;

import static org.testng.Assert.assertEquals;

public class TestBenchmarkRangeQuery {

  public static Logger logger = LoggerFactory.getLogger(TestBenchmarkRangeQuery.class);

  private static final MetricRegistry METRICS = new MetricRegistry();

  public static final com.codahale.metrics.Timer LOOKUP_STATS = METRICS.timer("lookup");

  public static void main(String[] args) throws Exception {
    final long startId = Long.valueOf(args[0]);
    final String cluster = args[1];

    //   FileUtils.deleteDirectory(new File("/data/database"));

    final NettyServer[] dbServers = new NettyServer[4];
    for (int shard = 0; shard < dbServers.length; shard++) {
      dbServers[shard] = new NettyServer();
    }


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



    final ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
    final ThreadPoolExecutor selectExecutor = new ThreadPoolExecutor(256, 256, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

    Class.forName("com.lowryengineering.database.jdbcdriver.DriverProxy");


//       final java.sql.Connection conn = DriverManager.getConnection("jdbc:voltdb://localhost:21212");

//    final java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:mysql://localhost/test", "test", "test");
//    final java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:mysql://127.0.0.1:4306/test", "test", "test");

    final java.sql.Connection conn = DriverManager.getConnection("jdbc:dbproxy:" + address + ":9010/db", "user", "password");

    //test insert
    final AtomicLong totalSelectDuration = new AtomicLong();
    final AtomicLong selectErrorCount = new AtomicLong();
    final AtomicLong selectBegin = new AtomicLong(System.currentTimeMillis());
    final AtomicLong selectOffset = new AtomicLong();
    final AtomicLong selectCount = new AtomicLong();
    final AtomicLong readCount = new AtomicLong();
        try {
          Thread[] threads = new Thread[8];
          for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(new Runnable() {
              @Override
              public void run() {
                while (true) {
                  try {
                    long beginSelect = System.nanoTime();
                    PreparedStatement stmt = null;
                    ResultSet ret = null;
                    if (false) {
                      ((ConnectionProxy)conn).getDatabaseClient().sendToAllShards(null, 0, "DatabaseServer:checkAddedRecords:1:test", null, DatabaseClient.Replica.all);
                      return;
                    }
                    else if (false) {
                      stmt = conn.prepareStatement("select persons.id, id2  " +
                                               "from persons where id = 143592 order by id asc");
                      ResultSetImpl rs = (ResultSetImpl) ((ConnectionProxy)conn).getDatabaseClient().executeQuery("test", null, "select persons.id, id2  " +
                                                "from persons where id = 158630 order by id asc", ((StatementProxy)stmt).getParms(), true);
                      rs.next();
                      System.out.println("hit: " + rs.getLong("id"));
//                      rs.next();
//                      System.out.println("hit: " + rs.getLong("id"));
                    }
                    else if (false) {
                      //primary index 2 expressions order desc
                      //700k
                      stmt = conn.prepareStatement("select persons.id, id2  " +
                          "from persons where id = 45573 order by id asc");                                              //
                      ret = stmt.executeQuery();

                      ret.next();
                      assertEquals(ret.getLong("id1"), 45573);
                    }
                    else if (true) {
                      //primary index 2 expressions order desc
                      //700k
                      stmt = conn.prepareStatement("select persons.id1, id2  " +
                          "from persons where id1 >= " + startId + " order by id1 asc");                                              //
                      long beginNano = System.nanoTime();
                      ret = stmt.executeQuery();

                      int count = 0;
                      while (true) {
                        Timer.Context context = LOOKUP_STATS.time();
                        if (!ret.next()) {
                          break;
                        }
                        context.stop();

                        if (readCount.incrementAndGet() % 100000 == 0) {
                          StringBuilder builder = new StringBuilder();
                          builder.append("count=").append(readCount.get());
                          Snapshot snapshot = LOOKUP_STATS.getSnapshot();
                          builder.append(String.format(", rate=%.2f", LOOKUP_STATS.getFiveMinuteRate()));
                          builder.append(String.format(", avg=%.2f nanos", snapshot.getMean()));
                          builder.append(String.format(", 99th=%.2f nanos", snapshot.get99thPercentile()));
                          builder.append(String.format(", max=%d", snapshot.getMax()));
                          builder.append(", errorCount=" + selectErrorCount.get());
                          if (selectCount.get() > 1000) {
                            selectOffset.set(0);
                            selectCount.set(0);
                            selectBegin.set(System.currentTimeMillis());
                            totalSelectDuration.set(0);
                          }
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
      catch(Exception e){
        e.printStackTrace();
      }
      while (true) {
        Thread.sleep(1000);
    }

}


}
