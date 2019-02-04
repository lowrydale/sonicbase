package com.sonicbase.accept.server;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.Config;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.schema.Schema;
import com.sonicbase.server.DatabaseServer;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.testng.Assert.*;

public class TestSchema {

  private Connection conn;
  final List<Long> ids = new ArrayList<>();
  com.sonicbase.server.DatabaseServer[] dbServers;
  private DatabaseClient client;

  @AfterClass(alwaysRun = true)
  public void afterClass() throws SQLException {
    conn.close();
    for (com.sonicbase.server.DatabaseServer server : dbServers) {
      server.shutdown();
    }

    System.out.println("client refCount=" + DatabaseClient.clientRefCount.get() + ", sharedClients=" + DatabaseClient.sharedClients.size());
    for (DatabaseClient client : DatabaseClient.allClients) {
      System.out.println("Stack:\n" + client.getAllocatedStack());
    }

  }

  @BeforeClass
  public void beforeClass() throws Exception {


    String configStr = IOUtils.toString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-4-servers.yaml")), "utf-8");
    Config config = new Config(configStr);

    FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db-data"));

    //DatabaseServer.getAddressMap().clear();
    DatabaseClient.getServers().clear();

    dbServers = new com.sonicbase.server.DatabaseServer[4];
    ThreadPoolExecutor executor = new ThreadPoolExecutor(32, 32, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

    String role = "primaryMaster";

    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < dbServers.length; i++) {

      dbServers[i] = new com.sonicbase.server.DatabaseServer();
      dbServers[i].setConfig(config, "4-servers", "localhost", 9010 + (50 * i), true, new AtomicBoolean(true), new AtomicBoolean(true),null, false);
      dbServers[i].setRole(role);
    }
    for (Future future : futures) {
      future.get();
    }

    for (DatabaseServer server : dbServers) {
      server.shutdownRepartitioner();
    }

    Class.forName("com.sonicbase.jdbcdriver.Driver");

    conn = DriverManager.getConnection("jdbc:sonicbase:localhost:9010", "user", "password");

    ((ConnectionProxy)conn).getDatabaseClient().createDatabase("test");

    conn.close();

    conn = DriverManager.getConnection("jdbc:sonicbase:localhost:9010/test", "user", "password");



    client = ((ConnectionProxy)conn).getDatabaseClient();

    PreparedStatement stmt = conn.prepareStatement("create table Persons (id BIGINT, ssn VARCHAR(64), gender VARCHAR(8), PRIMARY KEY (id))");
          stmt.executeUpdate();


    for (int i = 0; i < 10; i++) {
      stmt = conn.prepareStatement("insert into persons (id, ssn, gender) VALUES (?, ?, ?)");
      stmt.setLong(1, i);
      stmt.setString(2, String.valueOf(i));
      stmt.setString(3, "m");
      assertEquals(stmt.executeUpdate(), 1);
      ids.add((long) i);
    }

    client.beginRebalance("test");

    while (true) {
      if (client.isRepartitioningComplete("test")) {
        break;
      }
      Thread.sleep(1000);
    }
    executor.shutdownNow();
  }

  @Test
  public void test() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("alter table persons add column id2 BIGINT");
    stmt.executeUpdate();

    Schema schema = client.getSchema("test");

    for (int i = 10; i < 20; i++) {
      stmt = conn.prepareStatement("insert into persons (id, id2, gender) VALUES (?, ?, ?)");
      stmt.setLong(1, i);
      stmt.setLong(2, i * 2);
      stmt.setString(3, "m");
      assertEquals(stmt.executeUpdate(), 1);
      ids.add((long) i);
    }

    stmt = conn.prepareStatement("select * from persons where id>=10");
    ResultSet rs = stmt.executeQuery();
    for (int i = 10; i < 20; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), i);
      assertEquals(rs.getLong("id2"), i * 2);
    }
    assertFalse(rs.next());

    stmt = conn.prepareStatement("select * from persons where id2 < 30");
    rs = stmt.executeQuery();
    for (int i = 10; i < 15; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id2"), i * 2);
    }
    assertFalse(rs.next());

    for (int i = 0; i < 20; i++) {
      stmt = conn.prepareStatement("update persons set id2=? where id=?");
      stmt.setLong(1, i * 2);
      stmt.setLong(2, i);
      assertEquals(stmt.executeUpdate(), 1);
      ids.add((long) i);
    }

    stmt = conn.prepareStatement("select * from persons where id2 < 40");
    rs = stmt.executeQuery();
    for (int i = 0; i < 20; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id2"), i * 2);
      assertEquals(rs.getLong("id"), i);
      assertEquals(rs.getString("gender"), "m");
    }
    assertFalse(rs.next());
  }

  @Test
  public void testDrop() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("alter table persons drop column ssn");
    stmt.executeUpdate();

    for (int i = 100; i < 110; i++) {
      stmt = conn.prepareStatement("insert into persons (id, ssn, gender) VALUES (?, ?, ?)");
      stmt.setLong(1, i);
      stmt.setString(2, String.valueOf(i));
      stmt.setString(3, "m");
      assertEquals(stmt.executeUpdate(), 1);
      ids.add((long) i);
    }

    stmt = conn.prepareStatement("select * from persons where id>=0 and id<10");
    ResultSet rs = stmt.executeQuery();
    for (int i = 0; i < 10; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getString("ssn"), null);
    }
    assertFalse(rs.next());

    stmt = conn.prepareStatement("alter table persons add column ssn VARCHAR(64)");
    stmt.executeUpdate();

    for (int i = 0; i < 10; i++) {
      stmt = conn.prepareStatement("update persons set ssn=? where id=?");
      stmt.setString(1, String.valueOf(i * 2));
      stmt.setLong(2, i);
      assertEquals(stmt.executeUpdate(), 1);
      ids.add((long) i);
    }

    stmt = conn.prepareStatement("select * from persons where id>=0 and id<10");
    rs = stmt.executeQuery();
    for (int i = 0; i < 10; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getString("ssn"), String.valueOf(i * 2));
    }
    assertFalse(rs.next());
  }

  @Test
  public void testDropOwnedByIndex() {
    try {
      PreparedStatement stmt = conn.prepareStatement("alter table persons drop column id");
      stmt.executeUpdate();
      fail();
    }
    catch (SQLException e) {
      //expected
    }
  }

  @Test
  public void testDropPrimaryKeyIndex() {
    try {
      PreparedStatement stmt = conn.prepareStatement("drop index persons._primarykey");
      stmt.executeUpdate();
      fail();
    }
    catch (SQLException e) {
      e.printStackTrace();
      //expected
    }
  }
}
