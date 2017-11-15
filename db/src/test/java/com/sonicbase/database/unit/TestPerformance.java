/* © 2017 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.database.unit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.research.socket.NettyServer;
import com.sonicbase.server.DatabaseServer;
import org.apache.commons.io.IOUtils;
import org.codehaus.plexus.util.FileUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.concurrent.CountDownLatch;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestPerformance {

  private Connection conn;
  private DatabaseClient client;

  @BeforeClass
  public void beforeClass() throws IOException, InterruptedException, SQLException, ClassNotFoundException {
    try {
      String configStr = IOUtils.toString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-4-servers.json")), "utf-8");
      ObjectMapper mapper = new ObjectMapper();
      final ObjectNode config = (ObjectNode) mapper.readTree(configStr);

      FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db"));

      ArrayNode array = new ArrayNode(JsonNodeFactory.instance);
      array.add(DatabaseServer.FOUR_SERVER_LICENSE);
      config.put("licenseKeys", array);

      DatabaseClient.getServers().clear();

      final DatabaseServer[] dbServers = new DatabaseServer[4];

      String role = "primaryMaster";


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
      thread.start();
      while (true) {
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
      thread.start();

      while (true) {
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
      thread.start();
      while (true) {
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
      thread.start();

      while (true) {
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

      //
      PreparedStatement stmt = conn.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
      stmt.executeUpdate();

      stmt = conn.prepareStatement("create index id2 on persons(id2)");
      stmt.executeUpdate();

      //rebalance
      for (DatabaseServer server : dbServers) {
        server.shutdownRepartitioner();
      }

      for (int i = 0; i < 10_000; i++) {
        stmt = conn.prepareStatement("insert into persons (id, id2, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?)");
        stmt.setLong(1, i);
        stmt.setLong(2, i + 1000);
        stmt.setString(3, "933-28-" + i);
        stmt.setString(4, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
        stmt.setBoolean(5, false);
        stmt.setString(6, "m");
        assertEquals(stmt.executeUpdate(), 1);
        if (i % 1000 == 0) {
          System.out.println("progress: count=" + i);
        }
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

  @Test
  public void test() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id=1000");
    long begin = System.nanoTime();
    for (int i = 0; i < 10_000; i++) {
      ResultSet rs = stmt.executeQuery();
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), 1_000);
    }
    long end = System.nanoTime();
    System.out.println("duration=" + (end - begin) / 1_000_000 + ", latency=" + (end - begin) / 10_000D / 1_000_000);
    assertTrue((end - begin) < (3000 * 1_000_000L), String.valueOf(end-begin));
  }

  @Test
  public void testOtherExpression() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id=1000 and id2 < 10000 and id2 > 1000");
    long begin = System.nanoTime();
    for (int i = 0; i < 10_000; i++) {
      ResultSet rs = stmt.executeQuery();
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), 1_000);
    }
    long end = System.nanoTime();
    System.out.println("duration=" + (end - begin) / 1_000_000 + ", latency=" + (end - begin) / 10_000D / 1_000_000);
    assertTrue((end - begin) < (3000 * 1_000_000L), String.valueOf(end-begin));
  }

  @Test
  public void testRange() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id>1000");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    for (int i = 1001; i < 10_000; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), i);
      assertEquals(rs.getLong("id2"), i + 1000);
    }
    long end = System.nanoTime();
    System.out.println("duration=" + (end - begin) / 1_000_000 + ", latency=" + (end - begin) / 10_000D / 1_000_000);
    assertTrue((end - begin) < (300 * 1_000_000L), String.valueOf(end-begin));
  }

  @Test
  public void testRangeOtherExpression() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id>1000 and id2 < 10000 and id2 > 1000");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    for (int i = 1001; i < 9_000; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), i);
    }
    long end = System.nanoTime();
    System.out.println("duration=" + (end - begin) / 1_000_000 + ", latency=" + (end - begin) / 10_000D / 1_000_000);
    assertTrue((end - begin) < (300 * 1_000_000L), String.valueOf(end-begin));
  }

  @Test
  public void testSecondary() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id2=2000");
    long begin = System.nanoTime();
    for (int i = 0; i < 10_000; i++) {
      ResultSet rs = stmt.executeQuery();
      assertTrue(rs.next());
      assertEquals(rs.getLong("id2"), 2_000);
    }
    long end = System.nanoTime();
    System.out.println("duration=" + (end - begin) / 1_000_000 + ", latency=" + (end - begin) / 10_000D / 1_000_000);
    assertTrue((end - begin) < (6500 * 1_000_000L), String.valueOf(end-begin));
  }
}
