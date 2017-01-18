package com.lowryengineering.database.bench;

import com.lowryengineering.database.jdbcdriver.ConnectionProxy;
import com.lowryengineering.database.util.JsonDict;
import com.lowryengineering.database.util.StreamUtils;
import com.lowryengineering.research.socket.NettyServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
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

    String configStr = StreamUtils.inputStreamToString(new BufferedInputStream(TestBenchmarkJoins.class.getResourceAsStream("/config/config-4-servers.json")));
    final JsonDict config = new JsonDict(configStr);

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

    final java.sql.Connection conn = DriverManager.getConnection("jdbc:dbproxy:localhost:9010", "user", "password");


    //test insert
    int recordCount = 100000;
    final AtomicLong totalSelectDuration = new AtomicLong();

    final AtomicLong selectErrorCount = new AtomicLong();
    final AtomicLong selectBegin = new AtomicLong(System.currentTimeMillis());
    final AtomicLong selectOffset = new AtomicLong();


    while (true) {
      executor.submit(new Runnable(){
        @Override
        public void run() {
          try {
                  try {
                    int countExpected = 0;
                    long beginSelect = System.nanoTime();
                    //memberships.membershipname, resorts.resortname
                    ResultSet ret = null;
                    if (false) {
                      PreparedStatement stmt = conn.prepareStatement("select persons.id, socialsecuritynumber, memberships.id  " +
                          "from persons inner join Memberships on persons.id = Memberships.id inner join resorts on memberships.resortid = resorts.id" +
                          " where persons.id<1000000");

                      ret = stmt.executeQuery();
                      countExpected = 500000;
                    }
                    else if (false) {
                      PreparedStatement stmt = conn.prepareStatement(
                           "select persons.id, persons.id2, persons.socialsecuritynumber, memberships.personId, memberships.personid2, memberships.membershipname from persons " +
                               " inner join Memberships on persons.id = Memberships.PersonId and memberships.personid2 = persons.id2  where persons.id > 0 order by persons.id desc");
                       ret = stmt.executeQuery();
                      countExpected = 100000;
                    }
                    else if (false) {
                      PreparedStatement stmt = conn.prepareStatement(
                          "select persons.id, persons.socialsecuritynumber, memberships.membershipname, memberships.personid from memberships " +
                              "left outer join Memberships on persons.id = Memberships.PersonId where memberships.personid<100000  order by memberships.personid desc");
                      ret = stmt.executeQuery();
                      countExpected = 190000;
                    }
                    else if (false) {
                      PreparedStatement stmt = conn.prepareStatement(
                           "select persons.id, persons.socialsecuritynumber, memberships.membershipname from persons " +
                               " inner join Memberships on Memberships.PersonId = persons.id and memberships.personid < 100000 where persons.id > 0 order by persons.id desc");
                       ret = stmt.executeQuery();
                      countExpected = 100000;
                    }
                    else {
                      PreparedStatement stmt = conn.prepareStatement(
                          "select persons.id, persons.socialsecuritynumber, memberships.personId, memberships.membershipname from persons " +
                              "right outer join Memberships on persons.id = Memberships.PersonId where persons.id<100000  order by persons.id desc");
                      ret = stmt.executeQuery();
                      countExpected = 100000;
                    }
                    totalSelectDuration.addAndGet(System.nanoTime() - beginSelect);

                    for (int i = 0; i < countExpected; i++) {
                      beginSelect = System.nanoTime();
                      if (!ret.next()) {
                        throw new Exception("Not found: at=" + i);
                      }

                      totalSelectDuration.addAndGet(System.nanoTime() - beginSelect);
                      if (selectOffset.incrementAndGet() % 100000 == 0) {
                        StringBuilder builder = new StringBuilder();
                        builder.append("select: count=").append(selectOffset.get()).append(", rate=");
                        builder.append(selectOffset.get() / (float) (System.currentTimeMillis() - selectBegin.get()) * 1000f).append("/sec");
                        builder.append(", avgDuration=" + (float) (totalSelectDuration.get() / (float) selectOffset.get() / 1000000f) + " millis, ");
                        builder.append("errorCount=" + selectErrorCount.get());
                        if (selectOffset.get() > 4000000) {
                          selectOffset.set(0);
                          selectBegin.set(System.currentTimeMillis());
                          totalSelectDuration.set(0);
                        }
                                    builder.append("0=" + ((ConnectionProxy)conn).getDatabaseClient().getPartitionSize("test", 0, 0, "persons", "_1__primarykey")).append(",");
                                     builder.append("1=" + ((ConnectionProxy)conn).getDatabaseClient().getPartitionSize("test", 0, 1, "persons", "_1__primarykey")).append(",");
                                     builder.append("0=" + ((ConnectionProxy)conn).getDatabaseClient().getPartitionSize("test", 1, 0, "persons", "_1__primarykey")).append(",");
                                     builder.append("1=" + ((ConnectionProxy)conn).getDatabaseClient().getPartitionSize("test", 1, 1, "persons", "_1__primarykey")).append(",");

                        logger.info(builder.toString());
                      }
                    }
                  }
                  catch (Exception e) {
                    logger.error("error", e);
                  }

              }

              catch (
                  Exception e
                  )

              {
                selectErrorCount.incrementAndGet();
                logger.error("Error searching: ", e);
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
                  logger.info(builder.toString());
                }
              }

        }
      });
    }


    //executor.shutdownNow();
  }


}
