package com.sonicbase.accept.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
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

public class TestSetOperations {

  private Connection conn;
  private int recordCount = 10;
  List<Long> ids = new ArrayList<>();
  com.sonicbase.server.DatabaseServer[] dbServers;

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

    String configStr = IOUtils.toString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-4-servers.json")), "utf-8");
    ObjectMapper mapper = new ObjectMapper();
    final ObjectNode config = (ObjectNode) mapper.readTree(configStr);

    FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db"));

    DatabaseClient.getServers().clear();

    dbServers = new com.sonicbase.server.DatabaseServer[4];
    ThreadPoolExecutor executor = new ThreadPoolExecutor(32, 32, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

    String role = "primaryMaster";

    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < dbServers.length; i++) {
      final int shard = i;

      dbServers[shard] = new com.sonicbase.server.DatabaseServer();
      dbServers[shard].setConfig(config, "4-servers", "localhost", 9010 + (50 * shard), true, new AtomicBoolean(true), new AtomicBoolean(true),null);
      dbServers[shard].setRole(role);
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
      conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/db", "root", "pass");
    }
    else {
      conn = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9000", "user", "password");

      ((ConnectionProxy) conn).getDatabaseClient().createDatabase("test");

      conn.close();

      conn = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9000/test", "user", "password");
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



    stmt = conn.prepareStatement("insert into Resorts (resortId, resortName) VALUES (?, ?)");
    stmt.setLong(1, 1000);
    stmt.setString(2, "resort-1000");
    assertEquals(stmt.executeUpdate(), 1);

    stmt = conn.prepareStatement("insert into Resorts (resortId, resortName) VALUES (?, ?)");
    stmt.setLong(1, 2000);
    stmt.setString(2, "resort-2000");
    assertEquals(stmt.executeUpdate(), 1);

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into Memberships (id, membershipName, resortId) VALUES (?, ?, ?)");
      stmt.setLong(1, i);
      stmt.setString(2, "membership-" + i);
      stmt.setLong(3, new long[]{1000, 2000}[i % 2]);
      assertEquals(stmt.executeUpdate(), 1);
    }

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into Accounts (id, membershipName) VALUES (?, ?)");
      stmt.setLong(1, i);
      stmt.setString(2, "membership-" + i);
      assertEquals(stmt.executeUpdate(), 1);
    }

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into Accounts1 (id, membershipName) VALUES (?, ?)");
      stmt.setLong(1, i % 3);
      stmt.setString(2, "membership-" + i % 3);
      assertEquals(stmt.executeUpdate(), 1);
    }
    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into Accounts2 (id, membershipName) VALUES (?, ?)");
      stmt.setLong(1, i % 3);
      stmt.setString(2, "membership-" + i % 3);
      assertEquals(stmt.executeUpdate(), 1);
    }
    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into Accounts3 (id, membershipName) VALUES (?, ?)");
      stmt.setLong(1, i % 3);
      stmt.setString(2, "membership-" + i % 3);
      assertEquals(stmt.executeUpdate(), 1);
    }

    for (int i = 0; i < 4; i++) {
      stmt = conn.prepareStatement("insert into persons (id, id2, id3, id4, id5, num, socialSecurityNumber, relatives, restricted, gender, timestamp, membershipname) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
      stmt.setLong(1, i);
      stmt.setLong(2, (i + 100) % 2);
      stmt.setLong(3, -1 * (i + 100));
      stmt.setLong(4, (i + 100) % 3);
      stmt.setDouble(5, 1);
      stmt.setDouble(6, i * 0.5);
      stmt.setString(7, "s" + i);
      stmt.setString(8, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
      stmt.setBoolean(9, false);
      stmt.setString(10, "m");
      Timestamp timestamp = new Timestamp(2000 + i - 1900, i % 12 - 1, i % 30, i % 24, i % 60, i % 60, 0);
      stmt.setTimestamp(11, timestamp);
      stmt.setString(12, "membership-" + i);
      assertEquals(stmt.executeUpdate(), 1);
      ids.add((long) i);
    }

    for (int i = 4; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into persons (id, id3, id4, num, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
      stmt.setLong(1, i);
      stmt.setLong(2, -1 * (i + 100));
      stmt.setLong(3, (i + 100) % 3);
      stmt.setDouble(4, i * 0.5);
      stmt.setString(5, "ssN-933-28-" + i);
      stmt.setString(6, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
      stmt.setBoolean(7, false);
      stmt.setString(8, "m");
      assertEquals(stmt.executeUpdate(), 1);
      ids.add((long) i);
    }

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into persons (id, id2, id3, id4, num, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
      stmt.setLong(1, i + 100);
      stmt.setLong(2, (i + 100) % 2);
      stmt.setLong(3, -1 * (i + 100));
      stmt.setLong(4, (i + 100) % 3);
      stmt.setDouble(5, (i + 100) * 0.5);
      stmt.setString(6, "ssN-933-28-" + (i % 4));
      stmt.setString(7, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
      stmt.setBoolean(8, false);
      stmt.setString(9, "m");
      int count = stmt.executeUpdate();
      assertEquals(count, 1);
      ids.add((long) (i + 100));
    }

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into nokey (id, id2) VALUES (?, ?)");
      stmt.setLong(1, i);
      stmt.setLong(2, i * 2);
      assertEquals(stmt.executeUpdate(), 1);

      stmt = conn.prepareStatement("insert into nokey (id, id2) VALUES (?, ?)");
      stmt.setLong(1, i);
      stmt.setLong(2, i * 2);
      assertEquals(stmt.executeUpdate(), 1);
    }

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into nokeysecondaryindex (id, id2) VALUES (?, ?)");
      stmt.setLong(1, i);
      stmt.setLong(2, i * 2);
      assertEquals(stmt.executeUpdate(), 1);
    }

//    Thread.sleep(30000);

//    assertEquals(client.getPartitionSize("test", 0, "persons", "_1__primarykey"), 9);
//    assertEquals(client.getPartitionSize("test", 1, "persons", "_1__primarykey"), 11);

//    assertEquals(client.getPartitionSize(2, "persons", "_1__primarykey"), 9);
//    assertEquals(client.getPartitionSize(3, "persons", "_1__primarykey"), 8);

    Thread.sleep(10000);

    executor.shutdownNow();
  }

  @Test
  public void testUnique() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("(select id, membershipname from persons where id > 1 and id < 4) union (select id,membershipname from memberships where id  > 1 and id < 4) order by id");
    ResultSet rs = stmt.executeQuery();
    assertTrue(rs.next());
    assertEquals(rs.getLong("id"), 2);
    assertEquals(rs.getString("membershipname"), "membership-2");
    assertTrue(rs.next());
    assertEquals(rs.getLong("id"), 3);
    assertEquals(rs.getString("membershipname"), "membership-3");
    assertFalse(rs.next());
  }

  @Test
  public void testOrdered() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("(select id, membershipname from persons where id > 3 and id < 6) union (select id,membershipname from memberships where id  < 3) order by id");
    ResultSet rs = stmt.executeQuery();
    assertTrue(rs.next());
    assertEquals(rs.getLong("id"), 0);
    assertEquals(rs.getString("membershipname"), "membership-0");
    assertTrue(rs.next());
    assertEquals(rs.getLong("id"), 1);
    assertEquals(rs.getString("membershipname"), "membership-1");
    assertTrue(rs.next());
    assertEquals(rs.getLong("id"), 2);
    assertEquals(rs.getString("membershipname"), "membership-2");
    assertTrue(rs.next());
    assertEquals(rs.getLong("id"), 4);
    assertNull(rs.getString("membershipname"));
    assertTrue(rs.next());
    assertEquals(rs.getLong("id"), 5);
    assertNull(rs.getString("membershipname"));
    assertFalse(rs.next());
  }
//
//  public void assertEquals(long l, long l2) {
//    System.out.println(l);
//  }
//
//  public void assertEquals(String s, String s2) {
//    System.out.println(s);
//  }

  @Test
  public void testUnOrderedAll() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, membershipname from persons where id > 3 and id < 6 union all select id,membershipname from memberships where id  < 3");
    ResultSet rs = stmt.executeQuery();
    assertTrue(rs.next());
    assertEquals(rs.getLong("id"), 4);
    assertNull(rs.getString("membershipname"));
    assertTrue(rs.next());
    assertEquals(rs.getLong("id"), 5);
    assertNull(rs.getString("membershipname"));
    assertTrue(rs.next());
    assertEquals(rs.getLong("id"), 0);
    assertEquals(rs.getString("membershipname"), "membership-0");
    assertTrue(rs.next());
    assertEquals(rs.getLong("id"), 1);
    assertEquals(rs.getString("membershipname"), "membership-1");
    assertTrue(rs.next());
    assertEquals(rs.getLong("id"), 2);
    assertEquals(rs.getString("membershipname"), "membership-2");
    assertFalse(rs.next());
  }

  @Test
  public void testIntersect() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, membershipname from persons where id < 3 intersect select id,membershipname from memberships where id  < 6");
    ResultSet rs = stmt.executeQuery();
    assertTrue(rs.next());
    assertEquals(rs.getLong("id"), 0);
    assertEquals(rs.getString("membershipname"), "membership-0");
    assertTrue(rs.next());
    assertEquals(rs.getLong("id"), 1);
    assertEquals(rs.getString("membershipname"), "membership-1");
    assertTrue(rs.next());
    assertEquals(rs.getLong("id"), 2);
    assertEquals(rs.getString("membershipname"), "membership-2");
    assertFalse(rs.next());
  }

  @Test
  public void testExcept() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, membershipname from persons where id < 6 except select id,membershipname from memberships where id >= 3 and id < 6");
    ResultSet rs = stmt.executeQuery();
    assertTrue(rs.next());
    assertEquals(rs.getLong("id"), 0);
    assertEquals(rs.getString("membershipname"), "membership-0");
    assertTrue(rs.next());
    assertEquals(rs.getLong("id"), 1);
    assertEquals(rs.getString("membershipname"), "membership-1");
    assertTrue(rs.next());
    assertEquals(rs.getLong("id"), 2);
    assertEquals(rs.getString("membershipname"), "membership-2");
    assertTrue(rs.next());
    assertEquals(rs.getLong("id"), 4);
    assertEquals(rs.getString("membershipname"), null);
    assertTrue(rs.next());
    assertEquals(rs.getLong("id"), 5);
    assertEquals(rs.getString("membershipname"), null);
    assertFalse(rs.next());
  }

  @Test
  public void test3Select() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, membershipname from persons where id < 3 union all select id,membershipname from memberships where id  < 3 union all select id,membershipname from accounts where id < 3");
    ResultSet rs = stmt.executeQuery();
    assertTrue(rs.next());
    assertEquals(rs.getLong("id"), 0);
    assertEquals(rs.getString("membershipname"), "membership-0");
    assertTrue(rs.next());
    assertEquals(rs.getLong("id"), 1);
    assertEquals(rs.getString("membershipname"), "membership-1");
    assertTrue(rs.next());
    assertEquals(rs.getLong("id"), 2);
    assertEquals(rs.getString("membershipname"), "membership-2");
    assertTrue(rs.next());
    assertEquals(rs.getLong("id"), 0);
    assertEquals(rs.getString("membershipname"), "membership-0");
    assertTrue(rs.next());
    assertEquals(rs.getLong("id"), 1);
    assertEquals(rs.getString("membershipname"), "membership-1");
    assertTrue(rs.next());
    assertEquals(rs.getLong("id"), 2);
    assertEquals(rs.getString("membershipname"), "membership-2");
    assertTrue(rs.next());
    assertEquals(rs.getLong("id"), 0);
    assertEquals(rs.getString("membershipname"), "membership-0");
    assertTrue(rs.next());
    assertEquals(rs.getLong("id"), 1);
    assertEquals(rs.getString("membershipname"), "membership-1");
    assertTrue(rs.next());
    assertEquals(rs.getLong("id"), 2);
    assertEquals(rs.getString("membershipname"), "membership-2");
    assertFalse(rs.next());
  }

  @Test
  public void test3UniqueSelect() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, membershipname from persons where id < 3 union select id,membershipname from memberships where id  < 3 union select id,membershipname from accounts where id < 3");
    ResultSet rs = stmt.executeQuery();
    assertTrue(rs.next());
    assertEquals(rs.getLong("id"), 0);
    assertEquals(rs.getString("membershipname"), "membership-0");
    assertTrue(rs.next());
    assertEquals(rs.getLong("id"), 1);
    assertEquals(rs.getString("membershipname"), "membership-1");
    assertTrue(rs.next());
    assertEquals(rs.getLong("id"), 2);
    assertEquals(rs.getString("membershipname"), "membership-2");
    assertFalse(rs.next());
  }

  @Test
  public void test3UniqueSelectRepeated() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, membershipname from accounts1 where id < 3 union select id,membershipname from accounts2 where id  < 3 union select id,membershipname from accounts3 where id < 3");
    ResultSet rs = stmt.executeQuery();
    assertTrue(rs.next());
    assertEquals(rs.getLong("id"), 0);
    assertEquals(rs.getString("membershipname"), "membership-0");
    assertTrue(rs.next());
    assertEquals(rs.getLong("id"), 1);
    assertEquals(rs.getString("membershipname"), "membership-1");
    assertTrue(rs.next());
    assertEquals(rs.getLong("id"), 2);
    assertEquals(rs.getString("membershipname"), "membership-2");
    assertFalse(rs.next());
  }

  @Test
  public void testUnionAll() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id from persons where id > 1 union all select id from memberships where id > 1");
    ResultSet rs = stmt.executeQuery();
    for (int i = 2; i < 10; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), i);
    }
    for (int i = 100; i < 110; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), i);
    }
    for (int i = 2; i < 10; i++) {
      try {
        assertTrue(rs.next());
        assertEquals(rs.getLong("id"), i, String.valueOf(i));
      }
      catch (Exception e) {
        System.out.println("i=" + i);
      }
    }
    assertFalse(rs.next());
  }
}
