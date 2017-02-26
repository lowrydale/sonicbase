package com.sonicbase.bench;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.jdbcdriver.QueryType;
import com.sonicbase.query.*;
import com.sonicbase.util.JsonDict;
import com.sonicbase.util.StreamUtils;
import com.sonicbase.research.socket.NettyServer;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.codehaus.plexus.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestBenchmark {

  public static Logger logger = LoggerFactory.getLogger(TestBenchmark.class);


  public static void main(String[] args) throws Exception {

    String configStr = StreamUtils.inputStreamToString(new BufferedInputStream(TestBenchmark.class.getResourceAsStream("/config/config-4-servers.json")));
    final JsonDict config = new JsonDict(configStr);

    FileUtils.deleteDirectory(new File("/data/database"));

    final NettyServer[] dbServers = new NettyServer[4];
    for (int shard = 0; shard < dbServers.length; shard++) {
      dbServers[shard] = new NettyServer();
    }

    final ThreadPoolExecutor executor = new ThreadPoolExecutor(32, 32, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
    final ThreadPoolExecutor selectExecutor = new ThreadPoolExecutor(256, 256, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

    executor.submit(new Callable() {
      @Override
      public Object call() throws Exception {
        try {
          dbServers[0].startServer(new String[]{"--port=" + String.valueOf(9010 + (50 * 0)), "--host=localhost",
              "--mport=" + String.valueOf(9010), "--mhost=localhost", "--role=master", "--shard=" + String.valueOf(0)}, "config/config-4-servers-large.json", true);
        }
        catch (Exception e) {
          e.printStackTrace();
        }
        return null;
      }
    });
    while (!dbServers[0].isRunning()) {
      Thread.sleep(1000);
    }

    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < dbServers.length; i++) {
      final int shard = i;
      futures.add(executor.submit(new Callable() {
        @Override
        public Object call() throws Exception {
          if (shard == 0) {
            return null;
          }
          try {
            dbServers[shard].startServer(new String[]{"--port=" + String.valueOf(9010 + (50 * shard)), "--host=localhost",
                "--mport=" + String.valueOf(9010), "--mhost=localhost",
                "--role=master", "--shard=" + String.valueOf(shard)}, "config/config-4-servers-large.json", true);
          }
          catch (Exception e) {
            e.printStackTrace();
          }
//          String role = "primaryMaster";
//          dbServers[shard] = new DatabaseServer();
//          dbServers[shard].setConfig(config, Integer.valueOf(shard));
//          dbServers[shard].setRole(role);
//          dbServers[shard].disableLogProcessor();
          return null;
        }
      }));
    }
//    for (Future future : futures) {
//      future.get();
//    }
    for (NettyServer server : dbServers) {
      while (!server.isRunning()) {
        Thread.sleep(1000);
      }
    }

    final DatabaseClient client = new DatabaseClient("localhost", 9010, true);

    ParameterHandler parms = new ParameterHandler();

    try {
      client.executeQuery("test", QueryType.update0,
          "create table Persons (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))", parms);
    }
    catch (Exception e) {

    }
    try {
      client.executeQuery("test", QueryType.update0,
          "create index socialSecurityNumber on persons(socialSecurityNumber)", parms);
    }
    catch (Exception e) {

    }
    //client.syncSchema();


    Class.forName("com.sonicbase.jdbcdriver.Driver");

    final java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9010", "user", "password");

    final Connection connection = new Connection("127.0.0.1:9010");

    //test insert
    int recordCount = 100000000;
    final AtomicLong offset = new AtomicLong();
    final AtomicLong totalDuration = new AtomicLong();
    final AtomicLong totalSelectDuration = new AtomicLong();

    final AtomicLong selectErrorCount = new AtomicLong();
    final long selectBegin = System.currentTimeMillis();
    final AtomicLong selectOffset = new AtomicLong();
    final AtomicLong highestInserteId = new AtomicLong();
    final LongArrayList idsInserted = new LongArrayList();
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        while (true) {
          selectExecutor.submit(new Runnable() {
            @Override
            public void run() {
              try {
                long id = 0;
                synchronized (idsInserted) {
                  id = idsInserted.get(ThreadLocalRandom.current().nextInt(0, idsInserted.size()));
                }

                long beginSelect = System.nanoTime();

                SelectStatement selectStatement = connection.createSelectStatement();
                selectStatement.setFromTable("persons");
                BinaryExpression binaryOp = null;
                if (false) {
                }
                else if (false) {
                  binaryOp = selectStatement.createBinaryExpression("id", BinaryExpression.Operator.equal, id);
                  selectStatement.addSelectColumn(null, null, "persons", "id", null);
                }
                else if (false) {
                  Expression leftExpression = selectStatement.createBinaryExpression("id", BinaryExpression.Operator.greater, Math.max(1, id-50));
                  Expression rightExpression = selectStatement.createBinaryExpression("id", BinaryExpression.Operator.less, id);
                  binaryOp = selectStatement.createBinaryExpression(leftExpression, BinaryExpression.Operator.and, rightExpression);

                  selectStatement.addSelectColumn(null, null, "persons", "id", null);
                }
                else if (true) {
                  try {
                    java.sql.PreparedStatement statement = conn.prepareStatement("select id from persons where id=?");
                    statement.setLong(1, id);
                    java.sql.ResultSet resultSet = statement.executeQuery();
                    assertTrue(resultSet.next());
                    assertEquals(resultSet.getLong("id"), id);
                    // assertNull(resultSet.getString("relatives"));
                  }
                  catch (Exception e) {
                    logger.error("Error reading record: id=" + id, e);
                  }
                }

                selectStatement.setWhereClause(binaryOp);

                try {
                  if (false) {
                    ResultSet results = (ResultSet) selectStatement.execute("test", null);
                    if (results == null) {
                      logger.error("record not found: id=" + id);
                      selectErrorCount.incrementAndGet();
                    }
                    else {
                      assertTrue(results.next(), "id=" + id);
                      //                    for (long i = Math.max(1, id - 49); i < id; i++) {
                      //                      assertEquals((long) results.getLong("id"), i);
                      //                      assertNull(results.getString("relatives"));
                      //                      break;
                      //                      //assertTrue(results.next(), "id=" + i);
                      //                    }
                      //assertFalse(results.next());
                    }
                    //dedup results
                  }
                }
                catch (Exception e) {
                  selectErrorCount.incrementAndGet();
                  logger.error("Error searching: id=" + id, e);
                  //               e.printStackTrace();
                }
                finally {
                  totalSelectDuration.addAndGet(System.nanoTime() - beginSelect);
                  if (selectOffset.incrementAndGet() % 10000 == 0) {
                    StringBuilder builder = new StringBuilder();
                    builder.append("select: count=").append(selectOffset.get()).append(", rate=");
                    builder.append(selectOffset.get() / (float) (System.currentTimeMillis() - selectBegin) * 1000f).append("/sec");
                    builder.append(", avgDuration=" + (totalSelectDuration.get() / selectOffset.get() / 1000000) + " millis, ");
                    builder.append("errorCount=" + selectErrorCount.get());
                    logger.info(builder.toString());
                  }
                }
              }
              catch (Exception e) {
                logger.error("Error processing request", e);
              }
            }
          });
        }
      }
    });
    thread.start();

    final AtomicLong insertErrorCount = new AtomicLong();
    final long begin = System.currentTimeMillis();
    for (; offset.get() < recordCount; ) {
      executor.submit(new Runnable() {
        @Override
        public void run() {
          long currOffset = offset.incrementAndGet();
          try {

            if (true) {
              InsertStatement statement = connection.createInsertStatement();
              statement.setTableName("persons");

              statement.addValue("id", currOffset);
              statement.addValue("socialsecuritynumber", "933-28-" + currOffset);
              statement.addValue("relatives", "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
              statement.addValue("restricted", false);
              statement.addValue("gender", "m");
              statement.execute("test", null);
              long currBegin = System.nanoTime();
             // client.doInsert(parms, insert);
              totalDuration.addAndGet(System.nanoTime() - currBegin);
              synchronized (idsInserted) {
                idsInserted.add(currOffset);
              }
            }

            if (false) {
              java.sql.PreparedStatement stmt = conn.prepareStatement("insert into persons (id, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?)");
              stmt.setLong(1, currOffset);
              stmt.setString(2, "933-28-" + currOffset);
              stmt.setString(3, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
              stmt.setBoolean(4, false);
              stmt.setString(5, "m");
              long currBegin = System.nanoTime();
              assertEquals(stmt.executeUpdate(), 1);
              totalDuration.addAndGet(System.nanoTime() - currBegin);
              synchronized (idsInserted) {
                idsInserted.add(currOffset);
              }
            }
          }
          catch (Exception e) {
            insertErrorCount.incrementAndGet();
            logger.error("Error inserting", e);
          }
          finally {
            if (currOffset % 10000 == 0) {
              StringBuilder builder = new StringBuilder();
              builder.append("count=").append(offset.get()).append(", rate=");
              builder.append(offset.get() / (float) (System.currentTimeMillis() - begin) * 1000f).append("/sec");
              builder.append(", avgDuration=" + (totalDuration.get() / offset.get() / 1000000) + " millis, errorCount=" +
                  insertErrorCount.get() + ", sizes=[");
              try {
                builder.append("0=" + client.getPartitionSize("test", 0, 0, "persons", "_1__primarykey")).append(",");
                builder.append("1=" + client.getPartitionSize("test", 0, 1, "persons", "_1__primarykey")).append(",");
                builder.append("0=" + client.getPartitionSize("test", 1, 0, "persons", "_1__primarykey")).append(",");
                builder.append("1=" + client.getPartitionSize("test", 1, 1, "persons", "_1__primarykey")).append(",");
                //              builder.append(client.getPartitionSize(2, "persons", "_primarykey")).append(",");
                //              builder.append(client.getPartitionSize(3, "persons", "_primarykey")).append("]");

                builder.append(", ssnSizes=[");
                builder.append("0=" + client.getPartitionSize("test", 0, 0, "persons", "_1_socialsecuritynumber")).append(",");
                builder.append("1=" + client.getPartitionSize("test", 0, 1, "persons", "_1_socialsecuritynumber")).append(",");
                builder.append("0=" + client.getPartitionSize("test", 1, 0, "persons", "_1_socialsecuritynumber")).append(",");
                builder.append("1=" + client.getPartitionSize("test", 1, 1, "persons", "_1_socialsecuritynumber")).append(",");
                //              builder.append(client.getPartitionSize(2, "persons", "socialsecuritynumber")).append(",");
                //              builder.append(client.getPartitionSize(3, "persons", "socialsecuritynumber")).append("]");
              }
              catch (Exception e) {
                logger.error("Error getting partition size", e);
              }
              logger.info(builder.toString());
            }
          }
        }
      });

    }

    executor.shutdownNow();
  }

}
