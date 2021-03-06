package com.sonicbase.accept.server;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.Config;
import com.sonicbase.jdbcdriver.ConnectionProxy;
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

public class TestInsertSelect {

  private Connection conn;
  private final int recordCount = 10;
  List<Long> ids = new ArrayList<>();
  com.sonicbase.server.DatabaseServer[] dbServers;

  @AfterClass(alwaysRun = true)
  public void afterClass() throws SQLException {
    conn.close();

    for (com.sonicbase.server.DatabaseServer server : dbServers) {
      server.shutdown();
    }

    System.out.println("client refCount=" + DatabaseClient.clientRefCount.get() + ", class=TestInsertSelect");
    for (DatabaseClient client : DatabaseClient.allClients) {
      System.out.println("Stack:\n" + client.getAllocatedStack());
    }

  }

  @BeforeClass
  public void beforeClass() throws Exception {
    System.setProperty("log4j.configuration", "test-log4j.xml");


    String configStr = IOUtils.toString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-4-servers.yaml")), "utf-8");
    Config config = new Config(configStr);

    FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db-data"));

    DatabaseClient.getServers().clear();

    dbServers = new com.sonicbase.server.DatabaseServer[4];
    ThreadPoolExecutor executor = new ThreadPoolExecutor(32, 32, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

    String role = "primaryMaster";

    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < dbServers.length; i++) {

      dbServers[i] = new com.sonicbase.server.DatabaseServer();
      Config.copyConfig("4-servers");
      dbServers[i].setConfig(config, "localhost", 9010 + (50 * i), true, new AtomicBoolean(true), new AtomicBoolean(true),null, false);
      dbServers[i].setRole(role);
    }
    for (Future future : futures) {
      future.get();
    }

    for (DatabaseServer server : dbServers) {
      server.shutdownRepartitioner();
    }

    Class.forName("com.sonicbase.jdbcdriver.Driver");
//    Class.forName("com.mysql.jdbc.Driver");

    boolean sonicbase = true;
    if (!sonicbase) {
      conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/db", "root", "pass");
    }
    else {
      conn = DriverManager.getConnection("jdbc:sonicbase:localhost:9010", "user", "password");

      ((ConnectionProxy) conn).getDatabaseClient().createDatabase("test");

      conn.close();

      conn = DriverManager.getConnection("jdbc:sonicbase:localhost:9010/test", "user", "password");
    }



    PreparedStatement stmt = conn.prepareStatement("drop table Persons");
    stmt.executeUpdate();
    stmt = conn.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, id3 BIGINT, id4 BIGINT, id5 BIGINT, num DOUBLE, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), timestamp TIMESTAMP, membershipname VARCHAR(128), PRIMARY KEY (id))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("drop table Memberships");
    stmt.executeUpdate();
    stmt = conn.prepareStatement("create table Memberships (id BIGINT, membershipName VARCHAR(20), resortId BIGINT, PRIMARY KEY (id, membershipName))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("drop table Resorts");
    stmt.executeUpdate();
    stmt = conn.prepareStatement("create table Resorts (resortId BIGINT, resortName VARCHAR(20), PRIMARY KEY (resortId))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("drop table Accounts");
    stmt.executeUpdate();
    stmt = conn.prepareStatement("create table Accounts (id BIGINT, membershipName VARCHAR(200), PRIMARY KEY (id))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("drop table Accounts1");
    stmt.executeUpdate();
    stmt = conn.prepareStatement("create table Accounts1 (id BIGINT, membershipName VARCHAR(200))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("drop table Accounts2");
    stmt.executeUpdate();
    stmt = conn.prepareStatement("create table Accounts2 (id BIGINT, membershipName VARCHAR(200))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("drop table Accounts3");
    stmt.executeUpdate();
    stmt = conn.prepareStatement("create table Accounts3 (id BIGINT, membershipName VARCHAR(200))");
    stmt.executeUpdate();

    try {
      stmt = conn.prepareStatement("drop index persons.socialSecurityNumber");
      stmt.executeUpdate();
    }
    catch (Exception e) {

    }
    stmt = conn.prepareStatement("create index socialSecurityNumber on persons(socialSecurityNumber)");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("drop table nokey");
    stmt.executeUpdate();
    stmt = conn.prepareStatement("create table nokey (id BIGINT, id2 BIGINT)");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("drop table nokeysecondaryindex");
    stmt.executeUpdate();
    stmt = conn.prepareStatement("create table nokeysecondaryindex (id BIGINT, id2 BIGINT)");
    stmt.executeUpdate();

    try {
      stmt = conn.prepareStatement("drop index nokeysecondaryindex.id");
      stmt.executeUpdate();
    }
    catch (Exception e) {

    }
    stmt = conn.prepareStatement("create index id on nokeysecondaryindex(id)");
    stmt.executeUpdate();

    //test upsert


    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into Accounts (id, membershipName) VALUES (?, ?)");
      stmt.setLong(1, i);
      stmt.setString(2, "membership-" + i);
      assertEquals(stmt.executeUpdate(), 1);
    }


    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into persons (id) VALUES (?)");
      stmt.setLong(1, i);
      assertEquals(stmt.executeUpdate(), 1);

      stmt = conn.prepareStatement("insert into memberships (id, membershipName) VALUES (?, ?)");
      stmt.setLong(1, i);
      stmt.setString(2, "membership-" + i);
      assertEquals(stmt.executeUpdate(), 1);
    }

    executor.shutdownNow();
  }

  @Test
  public void test() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("truncate table accounts1");
    stmt.execute();
    stmt = conn.prepareStatement("insert into accounts1 (id, membershipName) select id, membershipname from accounts where id < 8");
    stmt.execute();

    stmt = conn.prepareStatement("select * from accounts1");
    ResultSet rs = stmt.executeQuery();
    for (int i = 0; i < 8; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), i);
    }
    assertFalse(rs.next());
  }

  @Test
  public void testAlias() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("truncate table accounts1");
    stmt.execute();
    stmt = conn.prepareStatement("insert into accounts1 (id, membershipName) select id as myid, membershipname as myMem from accounts where id < 8");
    stmt.execute();

    stmt = conn.prepareStatement("select * from accounts1");
    ResultSet rs = stmt.executeQuery();
    for (int i = 0; i < 8; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), i);
    }
    assertFalse(rs.next());
  }

  @Test
  public void testJoins() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("truncate table accounts1");
    stmt.execute();
    stmt = conn.prepareStatement("insert into accounts1 (id, membershipName) " +
      "select persons.id, memberships.membershipname as membershipName from persons " +
        " inner join Memberships on persons.id = Memberships.id where persons.id<5");
    stmt.execute();

    stmt = conn.prepareStatement("select * from accounts1");
    ResultSet rs = stmt.executeQuery();
    for (int i = 0; i < 5; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), i);
      assertEquals(rs.getString("membershipName"), "membership-" + i);
    }
    assertFalse(rs.next());
  }
}
