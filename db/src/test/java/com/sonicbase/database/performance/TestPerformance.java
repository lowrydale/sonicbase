/* © 2017 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.database.performance;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.Logger;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.research.socket.NettyServer;
import com.sonicbase.server.DatabaseServer;
import org.apache.commons.io.IOUtils;
import org.codehaus.plexus.util.FileUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestPerformance {

  private Connection conn;
  private DatabaseClient client;
  private List<Thread> serverThreads = new ArrayList<>();
  DatabaseServer[] dbServers;

  @AfterClass
  public void afterClass() {
    for (DatabaseServer server : dbServers) {
      server.shutdown();
    }
    for (Thread thread : serverThreads) {
      thread.interrupt();
    }
    Logger.queue.clear();

    int maxLen = 0;
    for (String name : results.keySet()) {
      maxLen = Math.max(name.length(), maxLen);
    }
    for (Map.Entry<String, Result> entry : results.entrySet()) {
      String name = entry.getKey();
      String prefix = "";
      for (int i = 0; i < maxLen - name.length(); i++) {
        prefix += " ";
      }
      System.out.println(prefix + name + ": " + entry.getValue().duration / 1_000_000 + " " +
          entry.getValue().duration / entry.getValue().count / 1_000_000D + " " +
          (double)entry.getValue().count / (double)entry.getValue().duration * 1_000_000D * 1000D);
    }

  }

  @BeforeClass
  public void beforeClass() throws IOException, InterruptedException, SQLException, ClassNotFoundException, ExecutionException {
    try {
      Logger.disable();
      String configStr = IOUtils.toString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-4-servers.json")), "utf-8");
      ObjectMapper mapper = new ObjectMapper();
      final ObjectNode config = (ObjectNode) mapper.readTree(configStr);

      FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db"));

      ArrayNode array = new ArrayNode(JsonNodeFactory.instance);
      array.add(DatabaseServer.FOUR_SERVER_LICENSE);
      config.put("licenseKeys", array);

      DatabaseClient.getServers().clear();

      dbServers = new DatabaseServer[4];

      String role = "primaryMaster";

      Logger.setReady(false);

      final CountDownLatch latch = new CountDownLatch(4);
      final NettyServer server0_0 = new NettyServer(128);
      Thread thread = new Thread(new Runnable(){
        @Override
        public void run() {
          server0_0.startServer(new String[]{"-port", String.valueOf(9010), "-host", "localhost",
              "-mport", String.valueOf(9010), "-mhost", "localhost", "-cluster", "4-servers", "-shard", String.valueOf(0)}, "db/src/main/resources/config/config-4-servers.json", true);
          latch.countDown();
        }
      });
      serverThreads.add(thread);
      thread.start();
      while (true) {
        Logger.setReady(false);
        if (server0_0.isRunning()) {
          break;
        }
        Thread.sleep(100);
      }

      final NettyServer server0_1 = new NettyServer(128);
      thread = new Thread(new Runnable(){
        @Override
        public void run() {
          server0_1.startServer(new String[]{"-port", String.valueOf(9060), "-host", "localhost",
              "-mport", String.valueOf(9060), "-mhost", "localhost", "-cluster", "4-servers", "-shard", String.valueOf(0)}, "db/src/main/resources/config/config-4-servers.json", true);
          latch.countDown();
        }
      });
      serverThreads.add(thread);
      thread.start();

      while (true) {
        Logger.setReady(false);
        if (server0_1.isRunning()) {
          break;
        }
        Thread.sleep(100);
      }

      final NettyServer server1_0 = new NettyServer(128);
      thread = new Thread(new Runnable(){
        @Override
        public void run() {
          server1_0.startServer(new String[]{"-port", String.valueOf(9110), "-host", "localhost",
              "-mport", String.valueOf(9110), "-mhost", "localhost", "-cluster", "4-servers", "-shard", String.valueOf(1)}, "db/src/main/resources/config/config-4-servers.json", true);
          latch.countDown();
        }
      });
      serverThreads.add(thread);
      thread.start();
      while (true) {
        Logger.setReady(false);
        if (server1_0.isRunning()) {
          break;
        }
        Thread.sleep(100);
      }

      final NettyServer server1_1 = new NettyServer(128);
      thread = new Thread(new Runnable(){
        @Override
        public void run() {
          server1_1.startServer(new String[]{"-port", String.valueOf(9160), "-host", "localhost",
              "-mport", String.valueOf(9160), "-mhost", "localhost", "-cluster", "4-servers", "-shard", String.valueOf(1)}, "db/src/main/resources/config/config-4-servers.json", true);
          latch.countDown();
        }
      });
      serverThreads.add(thread);
      thread.start();

      while (true) {
        Logger.setReady(false);
        if (server0_0.isRunning() && server0_1.isRunning() && server1_0.isRunning() && server1_1.isRunning()) {
          break;
        }
        Thread.sleep(100);
      }

      Thread.sleep(5_000);

      dbServers[0] = server0_0.getDatabaseServer();
      dbServers[1] = server0_1.getDatabaseServer();
      dbServers[2] = server1_0.getDatabaseServer();
      dbServers[3] = server1_1.getDatabaseServer();

      System.out.println("Started 4 servers");

//
//      //DatabaseClient client = new DatabaseClient("localhost", 9010, true);
//
      Class.forName("com.sonicbase.jdbcdriver.Driver");

      conn = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9010", "user", "password");

      ((ConnectionProxy) conn).getDatabaseClient().createDatabase("test");

      conn.close();

      conn = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9010/test", "user", "password");
      client = ((ConnectionProxy) conn).getDatabaseClient();
      client.syncSchema();

      Logger.setReady(false);

      //
      PreparedStatement stmt = conn.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
      stmt.executeUpdate();

      stmt = conn.prepareStatement("create index id2 on persons(id2)");
      stmt.executeUpdate();

      stmt = conn.prepareStatement("create table Employee (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20))");
      stmt.executeUpdate();

      stmt = conn.prepareStatement("create index socialsecuritynumber on employee(socialsecurityNumber)");
      stmt.executeUpdate();

      stmt = conn.prepareStatement("create table Residence (id BIGINT, id2 BIGINT, id3 BIGINT, address VARCHAR(20), PRIMARY KEY (id, id2, id3))");
      stmt.executeUpdate();


      //rebalance
      for (DatabaseServer server : dbServers) {
        server.shutdownRepartitioner();
      }

      List<Future> futures = new ArrayList<>();
      ThreadPoolExecutor executor = new ThreadPoolExecutor(8, 8, 10_000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
      int offset = 0;
      for (int i = 0; i < 5_000; i++) {
        final int currOffset = offset;
        offset += 100;
        futures.add(executor.submit(new Callable(){
          @Override
          public Object call() throws Exception {
            PreparedStatement stmt = conn.prepareStatement("insert into persons (id, id2, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?)");
            int offset = currOffset;
            for (int j = 0; j < 100; j++) {
              stmt.setLong(1, offset);
              stmt.setLong(2, offset + 1000);
              String leading = "";
              if (offset < 10) {
                leading = "00000";
              }
              else if (offset < 100) {
                leading = "0000";
              }
              else if (offset < 1000) {
                leading = "000";
              }
              else if (offset < 10000) {
                leading = "00";
              }
              else if (offset < 100_000) {
                leading = "0";
              }
              else if (offset < 1_000_000) {
                leading = "";
              }
              if (offset == 99_999) {
                System.out.println("here");
              }
              stmt.setString(3, leading + offset);
              stmt.setString(4, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
              stmt.setBoolean(5, false);
              stmt.setString(6, "m");
              stmt.addBatch();
              if (offset++ % 1000 == 0) {
                System.out.println("progress: count=" + offset);
              }
            }
            stmt.executeBatch();    return null;
          }
        }));

      }

      for (Future future : futures) {
        future.get();
      }

      offset = 0;
      for (int i = 0; i < 5_000; i++) {
        final int currOffset = offset;
        offset += 100;
        futures.add(executor.submit(new Callable(){
          @Override
          public Object call() throws Exception {
            int offset = currOffset;
            PreparedStatement stmt = conn.prepareStatement("insert into residence (id, id2, id3, address) VALUES (?, ?, ?, ?)");
            for (int j = 0; j < 100; j++) {
              stmt.setLong(1, offset);
              stmt.setLong(2, offset + 1000);
              stmt.setLong(3, offset + 2000);
              stmt.setString(4, "5078 West Black");
              if (offset++ % 1000 == 0) {
                System.out.println("progress: count=" + offset);
              }
              stmt.addBatch();
            }
            stmt.executeBatch();
            return null;
          }}));
      }

      for (Future future : futures) {
        future.get();
      }

      offset = 0;
      for (int i = 0; i < 5_000; i++) {
        final int currOffset = offset;
        offset += 100;
        futures.add(executor.submit(new Callable(){
          @Override
          public Object call() throws Exception {
            int offset = currOffset;
            PreparedStatement stmt = conn.prepareStatement("insert into employee (id, id2, socialSecurityNumber) VALUES (?, ?, ?)");
            for (int j = 0; j < 100; j++) {
              stmt.setLong(1, offset);
              String leading = "";
              if (offset < 10) {
                leading = "00000";
              }
              else if (offset < 100) {
                leading = "0000";
              }
              else if (offset < 1000) {
                leading = "000";
              }
              else if (offset < 10000) {
                leading = "00";
              }
              else if (offset < 100_000) {
                leading = "0";
              }
              else if (offset < 1_000_000) {
                leading = "";
              }
              stmt.setLong(2, offset + 1000);
              stmt.setString(3, leading + offset);
              if (offset++ % 1000 == 0) {
                System.out.println("progress: count=" + offset);
              }
              stmt.addBatch();
            }
            stmt.executeBatch();
            return null;
          }}));
      }
      for (Future future : futures) {
        future.get();
      }


//      long size = client.getPartitionSize("test", 0, "children", "_1_socialsecuritynumber");
//      assertEquals(size, 10);

      client.beginRebalance("test", "persons", "_1__primarykey");


      while (true) {
        if (client.isRepartitioningComplete("test")) {
          break;
        }
        Thread.sleep(200);
      }

      for (DatabaseServer server : dbServers) {
        server.shutdownRepartitioner();
      }
    }
    catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  class Result {
    private long duration;
    private int count;

    public Result(long duration, int count) {
      this.duration = duration;
      this.count = count;
    }
  }

  private Map<String, Result> results = new ConcurrentHashMap<>();

  private void registerResults(String testName, long duration, int count) {
    System.out.println(testName + ": " + duration / 1_000_000 + " " +
        duration / count / 1_000_000D + " " +
        count / duration / 1_000_000D * 1000);

    if (results.put(testName, new Result(duration, count)) != null) {
      throw new DatabaseException("non-unique test: name=" + testName);
    }
  }

  @Test
  public void test() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id=1000");
    long begin = System.nanoTime();
    int count = 0;
    for (int i = 0; i < 10_000; i++) {
      ResultSet rs = stmt.executeQuery();
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), 1_000);
      count++;
    }
    long end = System.nanoTime();
    registerResults("identity id=", end-begin, count);
    assertTrue((end - begin) < (3300 * 1_000_000L), String.valueOf(end-begin));
  }

  @Test
  public void testMath() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id5 from persons where id >= 0 and id = id2 - 1000 order by id asc");
    ResultSet ret = stmt.executeQuery();

    long begin = System.nanoTime();
    int count = 0;
    ResultSet rs = stmt.executeQuery();
    for (int i = 0; i < 500_000; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), i);
      count++;
    }
    long end = System.nanoTime();
    registerResults("math", end-begin, count);
    assertTrue((end - begin) < (5000 * 1_000_000L), String.valueOf(end-begin));
  }

  @Test
  public void testIdNoKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from employee where socialsecurityNumber='000009'");
    long begin = System.nanoTime();
    int count = 0;
    for (int i = 0; i < 10_000; i++) {
      ResultSet rs = stmt.executeQuery();
      assertTrue(rs.next());
      assertEquals(rs.getString("socialsecuritynumber"), "000009");
      count++;
    }
    long end = System.nanoTime();
    registerResults("identity secondary key id=", end-begin, count);
    assertTrue((end - begin) < (8000 * 1_000_000L), String.valueOf(end-begin));
  }

  @Test
  public void testRangeNoKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from employee where socialsecurityNumber>='001000'");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = 1_000; i < 500_000; i++) {
      assertTrue(rs.next());
      String lead = "0";
      if (i < 10_000) {
        lead = "00";
      }
      else if (i < 100_000) {
        lead = "0";
      }
      else if (i < 1_000_000) {
        lead = "";
      }
      assertEquals(rs.getString("socialsecuritynumber"), lead + i);
      count++;
    }
    assertFalse(rs.next());
    long end = System.nanoTime();
    registerResults("range secondary key", end-begin, count);
    assertTrue((end - begin) < (5500 * 1_000_000L), String.valueOf(end-begin));
  }

  @Test
  public void testRangeThreeKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from residence where id>1000 and id2>2000 and id3>3000");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = 1_001; i < 500_000; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), i);
      assertEquals(rs.getLong("id2"), i + 1000);
      assertEquals(rs.getLong("id3"), i + 2000);
      count++;
    }
    assertFalse(rs.next());
    long end = System.nanoTime();
    registerResults("range three key", end-begin, count);
    assertTrue((end - begin) < (3200 * 1_000_000L), String.valueOf(end-begin));
  }

  @Test
  public void testRangeThreeKeyBackwards() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from residence where id3>3000 and id2>2000 and id>1000");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = 1_001; i < 500_000; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), i);
      assertEquals(rs.getLong("id2"), i + 1000);
      assertEquals(rs.getLong("id3"), i + 2000);
      count++;
    }
    assertFalse(rs.next());
    long end = System.nanoTime();
    registerResults("range secondary key backwards", end-begin, count);
    assertTrue((end - begin) < (3200 * 1_000_000L), String.valueOf(end-begin));
  }

  @Test
  public void testRangeThreeKeyMixed() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from residence where id3<102000 and id2>4000 and id>1000");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = 3_001; i < 100_000; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), i);
      assertEquals(rs.getLong("id2"), i + 1000);
      assertEquals(rs.getLong("id3"), i + 2000);
      count++;
    }
    assertFalse(rs.next());
    long end = System.nanoTime();
    registerResults("range secondary key mixed", end-begin, count);
    assertTrue((end - begin) < (2500 * 1_000_000L), String.valueOf(end-begin));
  }

  @Test
  public void testRangeThreeKeySingle() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from residence where id>1000");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = 1_001; i < 500_000; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), i);
      assertEquals(rs.getLong("id2"), i + 1000);
      assertEquals(rs.getLong("id3"), i + 2000);
      count++;
    }
    assertFalse(rs.next());
    long end = System.nanoTime();
    registerResults("range secondary key single", end-begin, count);
    assertTrue((end - begin) < (3200 * 1_000_000L), String.valueOf(end-begin));
  }

  @Test
  public void testNoKeyTwoKeyGreaterEqual() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from employee where socialsecurityNumber>='001000' and socialsecurityNumber<'095000' order by socialsecuritynumber desc");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = 94_999; i >= 1_000; i--) {
      assertTrue(rs.next(), String.valueOf(i));
      String leading = "";
      if (i < 10_000) {
        leading = "00";
      }
      else if (i < 100_000) {
        leading = "0";
      }
      assertEquals(rs.getString("socialsecuritynumber"), leading + i);
      count++;
    }
    assertFalse(rs.next());
    long end = System.nanoTime();
    registerResults("range two key greater equal2", end-begin, count);
    assertTrue((end - begin) < (2000 * 1_000_000L), String.valueOf(end-begin));
  }

  @Test
  public void testNoKeyTwoKeyGreater() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from employee where socialsecurityNumber>'001000' and socialsecurityNumber<'095000' order by socialsecuritynumber desc");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = 94_999; i > 1_000; i--) {
      assertTrue(rs.next(), String.valueOf(i));
      String leading = "";
      if (i < 10_000) {
        leading = "00";
      }
      else if (i < 100_000) {
        leading = "0";
      }
      assertEquals(rs.getString("socialsecuritynumber"), leading + i);
      count++;
    }
    assertFalse(rs.next());
    long end = System.nanoTime();
    registerResults("range two key greater", end-begin, count);
    assertTrue((end - begin) < (1900 * 1_000_000L), String.valueOf(end-begin));
  }

  @Test
  public void testNoKeyTwoKeyGreaterLeftSided() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from employee where socialsecurityNumber>'001000' and (socialsecurityNumber>'003000' and socialsecurityNumber<'095000') order by socialsecuritynumber desc");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = 94_999; i > 3_000; i--) {
      assertTrue(rs.next(), String.valueOf(i));
      String leading = "";
      if (i < 10_000) {
        leading = "00";
      }
      else if (i < 100_000) {
        leading = "0";
      }
      assertEquals(rs.getString("socialsecuritynumber"), leading + i);
      count++;
    }
    assertFalse(rs.next());
    long end = System.nanoTime();
    registerResults("range two key greater left sided", end-begin, count);
    assertTrue((end - begin) < (2650 * 1_000_000L), String.valueOf(end-begin));
  }

  @Test
  public void notIn() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id < 100000 and id > 10 and id not in (5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19) order by id desc");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = 99_999; i > 19; i--) {
      assertTrue(rs.next(), String.valueOf(i));
      assertEquals(rs.getLong("id"), i);
      count++;
    }
    assertFalse(rs.next());
    long end = System.nanoTime();
    registerResults("not in", end-begin, count);
    assertTrue((end - begin) < (1500 * 1_000_000L), String.valueOf(end-begin));
  }

  @Test
  public void notInSecondary() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id2 < 101000 and id2 > 1010 and id2 not in (1005, 1006, 1007, 1008, 1009, 1010, 1011, 1012, 1013, 1014, 1015, 1016, 1017, 1018, 1019) order by id2 desc");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = 100999; i > 1019; i--) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id2"), i);
      count++;
    }
    assertFalse(rs.next());
    long end = System.nanoTime();
    registerResults("range not in secondary", end-begin, count);
    assertTrue((end - begin) < (2200 * 1_000_000L), String.valueOf(end-begin));
  }

  @Test
  public void notInTableScan() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where socialsecuritynumber not in ('000000') order by id2 asc");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = 1001; i < 501_000; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id2"), i);
      count++;
    }
    assertFalse(rs.next());
    long end = System.nanoTime();
    registerResults("range not in table scan", end-begin, count);
    assertTrue((end - begin) < (8500 * 1_000_000L), String.valueOf(end-begin));
  }

  @Test
  public void test2keyRange() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where (id < 92251) and (id > 1000)");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = 1001; i < 92251; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), i);
      count++;
    }
    assertFalse(rs.next());
    long end = System.nanoTime();
    registerResults("range two key", end-begin, count);
    assertTrue((end - begin) < (1200 * 1_000_000L), String.valueOf(end-begin));
  }

  @Test
  public void testSecondaryKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id2>1000");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = 1001; i < 501_000; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id2"), i);
      count++;
    }
    assertFalse(rs.next());
    long end = System.nanoTime();
    registerResults("range two key seocndyary key", end-begin, count);
    assertTrue((end - begin) < (4500 * 1_000_000L), String.valueOf(end-begin));
  }

  @Test
  public void testTableScan() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where socialsecuritynumber>'000000'");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = 1; i < 500_000; i++) {
      assertTrue(rs.next());
      String leading = "";
      if (i < 10) {
        leading = "00000";
      }
      else if (i < 100) {
        leading = "0000";
      }
      else if (i < 1000) {
        leading = "000";
      }
      else if (i < 10000) {
        leading = "00";
      }
      else if (i < 100_000) {
        leading = "0";
      }
      else if (i < 1_000_000) {
        leading = "";
      }
      assertEquals(rs.getString("socialsecuritynumber"), leading + i);
      count++;
    }
    assertFalse(rs.next());
    long end = System.nanoTime();
    registerResults("table scan", end-begin, count);
    assertTrue((end - begin) < (2500 * 1_000_000L), String.valueOf(end-begin));
  }

  @Test
  public void testTwoKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id>=1000 and id<=95000");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = 1000; i <= 95_000; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), i);
      count++;
    }
    assertFalse(rs.next());
    long end = System.nanoTime();
    registerResults("range two key2", end-begin, count);
    assertTrue((end - begin) < (1200 * 1_000_000L), String.valueOf(end-begin));
  }

  @Test
  public void testTwoKeyRightSided() throws SQLException {

    PreparedStatement stmt = conn.prepareStatement("select * from persons where (id >= 3501 and id < 94751) and (id > 1000)");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = 3501; i < 94_751; i++) {
      assertTrue(rs.next(), String.valueOf(i));
      assertEquals(rs.getLong("id"), i);
      count++;
    }
    assertFalse(rs.next());
    long end = System.nanoTime();
    registerResults("range two key right sided", end-begin, count);
    assertTrue((end - begin) < (1200 * 1_000_000L), String.valueOf(end-begin));
  }

  @Test
  public void testTwoKeyLeftSidedGreater() throws SQLException {

    PreparedStatement stmt = conn.prepareStatement("select * from persons where (id > 1000) and (id >= 3501 and id < 94751)");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = 3501; i < 94_751; i++) {
      assertTrue(rs.next(), String.valueOf(i));
      assertEquals(rs.getLong("id"), i);
      count++;
    }
    assertFalse(rs.next());
    long end = System.nanoTime();
    registerResults("range two key left sided greater", end-begin, count);
    assertTrue((end - begin) < (1300 * 1_000_000L), String.valueOf(end-begin));
  }

  @Test
  public void testTwoKeyGreater() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id>1000 and id<95000");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = 1001; i < 95_000; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), i);
      count++;
    }
    assertFalse(rs.next());
    long end = System.nanoTime();
    registerResults("range two key greater2", end-begin, count);
    assertTrue((end - begin) < (1600 * 1_000_000L), String.valueOf(end-begin));
  }

  @Test
  public void testTwoKeyGreaterBackwards() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id<95000 and id>1000");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = 1001; i < 95_000; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), i);
      count++;
    }
    assertFalse(rs.next());
    long end = System.nanoTime();
    registerResults("range two key greater backwards", end-begin, count);
    assertTrue((end - begin) < (1600 * 1_000_000L), String.valueOf(end-begin));
  }


  @Test
  public void testTwoKeyLeftSidedGreaterEqual() throws SQLException {

    PreparedStatement stmt = conn.prepareStatement("select * from persons where (id >= 1000) and (id >= 3501 and id < 94751)");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = 3501; i < 94_751; i++) {
      assertTrue(rs.next(), String.valueOf(i));
      assertEquals(rs.getLong("id"), i);
      count++;
    }
    assertFalse(rs.next());
    long end = System.nanoTime();
    registerResults("range two key left sided greater equal", end-begin, count);
    assertTrue((end - begin) < (800 * 1_000_000L), String.valueOf(end-begin));
  }

  @Test
  public void testCountTwoKeyGreaterEqual() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select count(*) from persons where id>=1000 and id<=5000");
    long begin = System.nanoTime();
    int count = 0;
    for (int i = 1000; i < 1020; i++) {
      ResultSet rs = stmt.executeQuery();
      assertTrue(rs.next());
      assertEquals(rs.getLong(1), 4001);
      count++;
    }
    long end = System.nanoTime();
    registerResults("range two key greater equal", end-begin, count);
    assertTrue((end - begin) < (12000 * 1_000_000L), String.valueOf(end-begin));
  }

  @Test
  public void testMaxWhere() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select max(id) as maxValue from persons where id<=25000");
    long begin = System.nanoTime();
    int count = 0;
    for (int i = 1000; i < 1020; i++) {
      ResultSet rs = stmt.executeQuery();
      assertTrue(rs.next());
      assertEquals(rs.getLong("maxValue"), 25_000);
      count++;
    }
    long end = System.nanoTime();
    registerResults("max where", end-begin, count);
    assertTrue((end - begin) < (2000 * 1_000_000L), String.valueOf(end-begin));
  }

  @Test
  public void testMax() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select max(id) as maxValue from persons");
    long begin = System.nanoTime();
    int count = 0;
    for (int i = 1000; i < 1020; i++) {
      ResultSet rs = stmt.executeQuery();
      assertTrue(rs.next());
      assertEquals(rs.getLong("maxValue"), 499_999);
      count++;
    }
    long end = System.nanoTime();
    registerResults("max", end-begin, count);
    assertTrue((end - begin) < (18000 * 1_000_000L), String.valueOf(end-begin));
  }

  @Test
  public void testSort() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id>=1000 order by id desc");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = 499_999; i >= 1000; i--) {
      assertTrue(rs.next());
      String leading = "";
      if (i < 10) {
        leading = "00000";
      }
      else if (i < 100) {
        leading = "0000";
      }
      else if (i < 1000) {
        leading = "000";
      }
      else if (i < 10000) {
        leading = "00";
      }
      else if (i < 100_000) {
        leading = "0";
      }
       else if (i < 1_000_000) {
        leading = "";
      }
      assertEquals(rs.getString("socialsecuritynumber"), leading + i);
      count++;
    }
    long end = System.nanoTime();
    registerResults("range sort", end-begin, count);
    assertTrue((end - begin) < (2300 * 1_000_000L), String.valueOf(end-begin));
  }

  @Test
  public void testSortDisk() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id>=1000 order by socialsecuritynumber desc");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = 499_999; i >= 1000; i--) {
      assertTrue(rs.next());
      String leading = "";
      if (i < 10) {
        leading = "00000";
      }
      else if (i < 100) {
        leading = "0000";
      }
      else if (i < 1000) {
        leading = "000";
      }
      else if (i < 10000) {
        leading = "00";
      }
      else if (i < 100_000) {
        leading = "0";
      }
      else if (i < 1_000_000) {
        leading = "";
      }
      assertEquals(rs.getString("socialsecuritynumber"), leading + i);
      count++;
    }
    long end = System.nanoTime();
    registerResults("range sort disk", end-begin, count);
    assertTrue((end - begin) < (12000 * 1_000_000L), String.valueOf(end-begin));
  }

  @Test
  public void testId2() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id2=2000");
    long begin = System.nanoTime();
    int count = 0;
    for (int i = 0; i < 10_000; i++) {
      ResultSet rs = stmt.executeQuery();
      assertTrue(rs.next());
      assertEquals(rs.getLong("id2"), 2_000);
      count++;
    }
    long end = System.nanoTime();
    registerResults("id2", end-begin, count);
    assertTrue((end - begin) < (9000 * 1_000_000L), String.valueOf(end-begin));
  }

  @Test
  public void testId2Range() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id2>2000");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = 2001; i < 501_000; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id2"), i);
      count++;
    }
    assertFalse(rs.next());
    long end = System.nanoTime();
    registerResults("range id2", end-begin, count);
    assertTrue((end - begin) < (6200 * 1_000_000L), String.valueOf(end-begin));
  }

  @Test
  public void testOtherExpression() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id=1000 and id2 < 10000 and id2 > 1000");
    long begin = System.nanoTime();
    int count = 0;
    for (int i = 0; i < 10_000; i++) {
      ResultSet rs = stmt.executeQuery();
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), 1_000);
      count++;
    }
    long end = System.nanoTime();
    registerResults("range other expression", end-begin, count);
    assertTrue((end - begin) < (3300 * 1_000_000L), String.valueOf(end-begin));
  }

  @Test
  public void testRange() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id>1000");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = 1001; i < 500_000; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), i);
      assertEquals(rs.getLong("id2"), i + 1000);
      count++;
    }
    assertFalse(rs.next());
    long end = System.nanoTime();
    registerResults("range", end-begin, count);
    assertTrue((end - begin) < (1800 * 1_000_000L), String.valueOf(end-begin));
  }

  @Test
  public void testRangeOtherExpression() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id>1000 and id2 < 500000 and id2 > 1000");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = 1001; i < 499_000; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), i);
      count++;
    }
    assertFalse(rs.next());
    long end = System.nanoTime();
    registerResults("range other expression2", end-begin, count);
    assertTrue((end - begin) < (2500 * 1_000_000L), String.valueOf(end-begin));
  }

  @Test
  public void testSecondary() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id2=2000");
    long begin = System.nanoTime();
    int count = 0;
    for (int i = 0; i < 10_000; i++) {
      ResultSet rs = stmt.executeQuery();
      assertTrue(rs.next());
      assertEquals(rs.getLong("id2"), 2_000);
      count++;
    }
    long end = System.nanoTime();
    registerResults("id secdonary", end-begin, count);
    assertTrue((end - begin) < (8000 * 1_000_000L), String.valueOf(end-begin));
  }
}
