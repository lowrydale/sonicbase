/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.Logger;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.streams.LocalProducer;
import com.sonicbase.research.socket.NettyServer;
import org.apache.commons.io.IOUtils;
import org.codehaus.plexus.util.FileUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.concurrent.*;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestStreams {


  private Connection connA;
  private Connection connB;
  private DatabaseClient clientA;
  private DatabaseClient clientB;
  private DatabaseServer[] dbServers;

  @AfterClass
  public void afterClass() {
    for (DatabaseServer server : dbServers) {
      server.shutdown();
    }
    Logger.queue.clear();
  }

  @BeforeClass
  public void beforeClass() throws IOException, InterruptedException, SQLException, ClassNotFoundException {
    try {
      Logger.disable();

      String configStr = IOUtils.toString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-2-servers-a-producer.json")), "utf-8");
      ObjectMapper mapper = new ObjectMapper();
      final ObjectNode config = (ObjectNode) mapper.readTree(configStr);

      FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db"));

      ArrayNode array = new ArrayNode(JsonNodeFactory.instance);
      array.add(DatabaseServer.FOUR_SERVER_LICENSE);
      config.put("licenseKeys", array);

      DatabaseClient.getServers().clear();

      dbServers = new DatabaseServer[4];

      String role = "primaryMaster";


      final CountDownLatch latch = new CountDownLatch(4);
      final NettyServer serverA1 = new NettyServer(128);
      Thread thread = new Thread(new Runnable(){
        @Override
        public void run() {
          serverA1.startServer(new String[]{"-port", String.valueOf(9010), "-host", "localhost",
              "-mport", String.valueOf(9010), "-mhost", "localhost", "-cluster", "2-servers-a-producer", "-shard", String.valueOf(0)}, "db/src/main/resources/config/config-2-servers-a-producer.json", true);
          latch.countDown();
        }
      });
      thread.start();
      while (true) {
        if (serverA1.isRunning()) {
          break;
        }
        Thread.sleep(100);
      }

      final NettyServer serverA2 = new NettyServer(128);
      thread = new Thread(new Runnable(){
        @Override
        public void run() {
          serverA2.startServer(new String[]{"-port", String.valueOf(9060), "-host", "localhost",
              "-mport", String.valueOf(9060), "-mhost", "localhost", "-cluster", "2-servers-a-producer", "-shard", String.valueOf(1)}, "db/src/main/resources/config/config-2-servers-a-producer.json", true);
          latch.countDown();
        }
      });
      thread.start();

      while (true) {
        if (serverA2.isRunning()) {
          break;
        }
        Thread.sleep(100);
      }

      final NettyServer serverB1 = new NettyServer(128);
      thread = new Thread(new Runnable(){
        @Override
        public void run() {
          serverB1.startServer(new String[]{"-port", String.valueOf(9110), "-host", "localhost",
              "-mport", String.valueOf(9110), "-mhost", "localhost", "-cluster", "2-servers-b-consumer", "-shard", String.valueOf(0)}, "db/src/main/resources/config/config-2-servers-b-consumer.json", true);
          latch.countDown();
        }
      });
      thread.start();
      while (true) {
        if (serverB1.isRunning()) {
          break;
        }
        Thread.sleep(100);
      }

      final NettyServer serverB2 = new NettyServer(128);
      thread = new Thread(new Runnable(){
        @Override
        public void run() {
          serverB2.startServer(new String[]{"-port", String.valueOf(9160), "-host", "localhost",
              "-mport", String.valueOf(9160), "-mhost", "localhost", "-cluster", "2-servers-b-consumer", "-shard", String.valueOf(1)}, "db/src/main/resources/config/config-2-servers-b-consumer.json", true);
          latch.countDown();
        }
      });
      thread.start();

      while (true) {
        if (serverA1.isRunning() && serverA2.isRunning() && serverB1.isRunning() && serverB2.isRunning()) {
          break;
        }
        Thread.sleep(100);
      }

      serverB1.getDatabaseServer().getStreamManager().startStreaming(null);
      serverB2.getDatabaseServer().getStreamManager().startStreaming(null);

      dbServers[0] = serverA1.getDatabaseServer();
      dbServers[1] = serverA2.getDatabaseServer();
      dbServers[2] = serverB1.getDatabaseServer();
      dbServers[3] = serverB2.getDatabaseServer();

      System.out.println("Started 4 servers");

//
//      //DatabaseClient client = new DatabaseClient("localhost", 9010, true);
//
      Class.forName("com.sonicbase.jdbcdriver.Driver");

      connA = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9010", "user", "password");

      ((ConnectionProxy) connA).getDatabaseClient().createDatabase("test");

      connA.close();

      connA = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9010/test", "user", "password");
      clientA = ((ConnectionProxy)connA).getDatabaseClient();
      clientA.syncSchema();

      connB = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9110", "user", "password");

      ((ConnectionProxy) connB).getDatabaseClient().createDatabase("test");

      connB.close();

      connB = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9110/test", "user", "password");
      clientB = ((ConnectionProxy)connB).getDatabaseClient();
      clientB.syncSchema();

      Logger.setReady(false);
      //
      PreparedStatement stmt = connA.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
      stmt.executeUpdate();

      stmt = connA.prepareStatement("create table nokey (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20))");
      stmt.executeUpdate();

      stmt = connB.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
      stmt.executeUpdate();

      stmt = connB.prepareStatement("create table nokey (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20))");
      stmt.executeUpdate();

      LocalProducer.queue.clear();

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
  public void test() throws InterruptedException, SQLException {

    for (int i = 0; i < 10; i++) {
      PreparedStatement stmt = connA.prepareStatement("insert into persons (id, socialSecurityNumber, relatives, restricted, gender, id3) VALUES (?, ?, ?, ?, ?, ?)");
      stmt.setLong(1, i);
      stmt.setString(2, "933-28-" + i);
      stmt.setString(3, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
      stmt.setBoolean(4, false);
      stmt.setString(5, "m");
      stmt.setLong(6, i + 1000);
      assertEquals(stmt.executeUpdate(), 1);
    }

    Thread.sleep(10_000);

    PreparedStatement stmt = connB.prepareStatement("select * from persons");
    ResultSet ret = stmt.executeQuery();
    for (int i = 0; i < 10; i++) {
      assertTrue(ret.next(), String.valueOf(i));
      assertEquals(ret.getInt("id"), i);
    }
    assertFalse(ret.next());

    stmt = connA.prepareStatement("update persons set relatives='xxx' where id=0");
    stmt.executeUpdate();

    Thread.sleep(10_000);

    stmt = connB.prepareStatement("select * from persons");
    ret = stmt.executeQuery();
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 0);
    assertEquals(ret.getString("relatives"), "xxx");

    stmt = connA.prepareStatement("delete from persons where id=0");
    stmt.executeUpdate();

    Thread.sleep(10_000);

    stmt = connB.prepareStatement("select * from persons");
    ret = stmt.executeQuery();
    for (int i = 1; i < 10; i++) {
      assertTrue(ret.next(), String.valueOf(i));
      assertEquals(ret.getInt("id"), i);
    }
    assertFalse(ret.next());

  }

  @Test
  public void testNoKey() throws InterruptedException, SQLException {

    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 2; j++) {
        PreparedStatement stmt = connA.prepareStatement("insert into nokey (id, id2, socialSecurityNumber) VALUES (?, ?, ?)");
        stmt.setLong(1, i);
        stmt.setLong(2, 0);
        stmt.setString(3, "933-28-" + i + "-" + j);
        assertEquals(stmt.executeUpdate(), 1);
      }
    }

    Thread.sleep(10_000);

    PreparedStatement stmt = connB.prepareStatement("select * from nokey order by id, id2, socialsecuritynumber");
    ResultSet ret = stmt.executeQuery();
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 2; j++) {
        assertTrue(ret.next(), String.valueOf(i));
        assertEquals(ret.getInt("id"), i);
        assertEquals(ret.getInt("id2"), 0);
        assertEquals(ret.getString("socialsecuritynumber"), "933-28-" + i + "-" + j);
      }
    }
    assertFalse(ret.next());

    stmt = connA.prepareStatement("update nokey set socialsecuritynumber='xxx' where id=0 and id2=0 and socialsecuritynumber='933-28-0-1'");
    stmt.executeUpdate();

    Thread.sleep(10_000);

    stmt = connB.prepareStatement("select * from nokey order by id,id2,socialsecuritynumber ");
    ret = stmt.executeQuery();
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 0);
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getString("socialsecuritynumber"), "933-28-0-0");
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 0);
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getString("socialsecuritynumber"), "xxx");

    stmt = connA.prepareStatement("delete from nokey where id=0");
    stmt.executeUpdate();

    Thread.sleep(10_000);

    stmt = connB.prepareStatement("select * from nokey order by id,id2,socialsecuritynumber");
    ret = stmt.executeQuery();
    for (int i = 1; i < 10; i++) {
      for (int j = 0; j < 2; j++) {
        assertTrue(ret.next(), String.valueOf(i));
        assertEquals(ret.getInt("id"), i);
        assertEquals(ret.getInt("id2"), 0);
        assertEquals(ret.getString("socialsecuritynumber"), "933-28-" + i + "-" + j);
      }
    }
    assertFalse(ret.next());

  }
}
