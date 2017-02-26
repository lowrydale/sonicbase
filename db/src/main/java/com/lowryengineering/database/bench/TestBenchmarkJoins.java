package com.lowryengineering.database.bench;

import com.lowryengineering.database.client.DatabaseClient;
import com.lowryengineering.database.jdbcdriver.ConnectionProxy;
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

public class TestBenchmarkJoins {

  public static Logger logger = LoggerFactory.getLogger(TestBenchmarkJoins.class);


  public static void main(String[] args) throws Exception {

    final long startId = Long.valueOf(args[0]);
    final String cluster = args[1];
    final String queryType = args[2];

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

    //   FileUtils.deleteDirectory(new File("/data/database"));

    final NettyServer[] dbServers = new NettyServer[4];
    for (int shard = 0; shard < dbServers.length; shard++) {
      dbServers[shard] = new NettyServer();
    }

    final ThreadPoolExecutor executor = new ThreadPoolExecutor(3, 3, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
    final ThreadPoolExecutor selectExecutor = new ThreadPoolExecutor(256, 256, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

    Class.forName("com.lowryengineering.database.jdbcdriver.DriverProxy");


//    final java.sql.Connection conn = DriverManager.getConnection("jdbc:voltdb://localhost:21212");

//    final java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:mysql://localhost/test", "test", "test");
//    final java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:mysql://127.0.0.1:4306/test", "test", "test");

    final java.sql.Connection conn = DriverManager.getConnection("jdbc:dbproxy:" + address + ":9010/db", "user", "password");

    DatabaseClient client = ((ConnectionProxy) conn).getDatabaseClient();
    client.setPageSize(30000);

    //test insert
    int recordCount = 100000;
    final AtomicLong totalSelectDuration = new AtomicLong();

    final AtomicLong selectErrorCount = new AtomicLong();
    final AtomicLong selectBegin = new AtomicLong(System.currentTimeMillis());
    final AtomicLong selectOffset = new AtomicLong();
    final AtomicLong maxDuration = new AtomicLong();

    while (true) {
      executor.submit(new Runnable(){
        @Override
        public void run() {
          try {
                  try {
//                    PreparedStatement stmt1 = conn.prepareStatement("select count(*) from persons");
//
//                    ResultSet ret1 = stmt1.executeQuery();
//                    ret1.next();
          long count = 60000000;//ret1.getLong(1);

          long countExpected = 0;
          long beginSelect = System.nanoTime();
          //memberships.membershipname, resorts.resortname
          ResultSet ret = null;
                    PreparedStatement stmt = null;
          if (queryType.equals("3wayInner")) {
            stmt = conn.prepareStatement("select persons.id1, socialsecuritynumber, memberships.personId2  " +
                "from persons inner join Memberships on persons.id1 = Memberships.personId1 inner join resorts on memberships.resortid = resorts.id" +
                " where persons.id<" + count);

            ret = stmt.executeQuery();
            countExpected = count;
          }
          else if (queryType.equals("2wayInner")) {
            stmt = conn.prepareStatement(
                "select persons.id1, persons.id2, persons.socialsecuritynumber, memberships.personId, memberships.personid2, memberships.membershipname from persons " +
                    " inner join Memberships on persons.id1 = Memberships.PersonId where persons.id1 < 65000000 order by persons.id1 desc");
            ret = stmt.executeQuery();
            countExpected = count;
          }
          else if (queryType.equals("2wayLeftOuter")) {
            stmt = conn.prepareStatement(
                "select persons.id1, persons.socialsecuritynumber, memberships.membershipname, memberships.personid from memberships " +
                    "left outer join Memberships on persons.id1 = Memberships.PersonId where memberships.personid<" + count + " order by memberships.personid desc");
            ret = stmt.executeQuery();
            countExpected = count;
          }
          else if (queryType.equals("2wayInner2Field")) {
            stmt = conn.prepareStatement(
                "select persons.id1, persons.socialsecuritynumber, memberships.membershipname from persons " +
                    " inner join Memberships on Memberships.PersonId = persons.id1 and memberships.personid < " + count + " where persons.id1 > 0 order by persons.id1 desc");
            ret = stmt.executeQuery();
            countExpected = count;
          }
          else if (queryType.equals("2wayRightOuter")) {
            stmt = conn.prepareStatement(
                "select persons.id1, persons.socialsecuritynumber, memberships.personId, memberships.membershipname from persons " +
                    "right outer join Memberships on persons.id1 = Memberships.PersonId where persons.id1<" + count + " order by persons.id1 desc");
            ret = stmt.executeQuery();
            countExpected = count;
          }
          totalSelectDuration.addAndGet(System.nanoTime() - beginSelect);

          for (int i = 0; i < countExpected; i++) {
            //beginSelect = System.nanoTime();
            if (!ret.next()) {
              ret.close();
              stmt.close();
              throw new Exception("Not found: at=" + i);
            }

//            long duration = System.nanoTime() - beginSelect;
//            synchronized (maxDuration) {
//              maxDuration.set(Math.max(maxDuration.get(), duration));
//            }
//            totalSelectDuration.addAndGet(duration);
            if (selectOffset.incrementAndGet() % 100000 == 0) {
              StringBuilder builder = new StringBuilder();
              builder.append("select: count=").append(selectOffset.get()).append(", rate=");
              builder.append(selectOffset.get() / (float) (System.currentTimeMillis() - selectBegin.get()) * 1000f).append("/sec");
              builder.append(", avgDuration=" + (float) (totalSelectDuration.get() / (float) selectOffset.get() / 1000000f) + " millis, ");
              builder.append(", maxDuration=" + maxDuration.get() / 1000000f + " millis, ");
              builder.append("errorCount=" + selectErrorCount.get());
              if (selectOffset.get() > 40000000) {
                selectOffset.set(0);
                selectBegin.set(System.currentTimeMillis());
                totalSelectDuration.set(0);
                maxDuration.set(0);
              }
//                                    builder.append("0=" + ((ConnectionProxy)conn).getDatabaseClient().getPartitionSize("test", 0, 0, "persons", "_1__primarykey")).append(",");
//                                     builder.append("1=" + ((ConnectionProxy)conn).getDatabaseClient().getPartitionSize("test", 0, 1, "persons", "_1__primarykey")).append(",");
//                                     builder.append("0=" + ((ConnectionProxy)conn).getDatabaseClient().getPartitionSize("test", 1, 0, "persons", "_1__primarykey")).append(",");
//                                     builder.append("1=" + ((ConnectionProxy)conn).getDatabaseClient().getPartitionSize("test", 1, 1, "persons", "_1__primarykey")).append(",");

              System.out.println(builder.toString());
            }
          }
        }
        catch (Exception e) {
          e.printStackTrace();
        }

      }

      catch (
          Exception e
          )

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
      });
    }



    //executor.shutdownNow();
  }


}
