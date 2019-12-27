package com.sonicbase.accept.server;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.Config;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.server.DatabaseServer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
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

import static com.sonicbase.client.InsertStatementHandler.BATCH_STATUS_FAILED;
import static com.sonicbase.client.InsertStatementHandler.BATCH_STATUS_SUCCCESS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestBatch {

  private Connection conn;
  private final int recordCount = 10;
  private DatabaseServer[] dbServers;
  private DatabaseClient client;

  @AfterClass(alwaysRun = true)
  public void afterClass() throws SQLException {
    conn.close();
    for (DatabaseServer server : dbServers) {
      server.shutdown();
    }
    System.out.println("client refCount=" + DatabaseClient.clientRefCount.get() + ", class=TestBatch");
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

    //DatabaseServer.getAddressMap().clear();
    DatabaseClient.getServers().clear();

    dbServers = new DatabaseServer[4];
    ThreadPoolExecutor executor = new ThreadPoolExecutor(32, 32, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

    String role = "primaryMaster";

    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < dbServers.length; i++) {

      dbServers[i] = new DatabaseServer();
      Config.copyConfig("4-servers");
      dbServers[i].setConfig(config,  "localhost", 9010 + (50 * i), true, new AtomicBoolean(true), new AtomicBoolean(true),null, false);
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

    client = ((ConnectionProxy)conn).getDatabaseClient();
    client.createDatabase("test");

    conn.close();

    conn = DriverManager.getConnection("jdbc:sonicbase:localhost:9010/test", "user", "password");

    client = ((ConnectionProxy)conn).getDatabaseClient();


    PreparedStatement stmt = conn.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, id3 BIGINT, id4 BIGINT, id5 BIGINT, num DOUBLE, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create table Memberships (personId BIGINT, membershipName VARCHAR(20), resortId BIGINT, PRIMARY KEY (personId, membershipName))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create table trikey(id1 BIGINT, id2 VARCHAR(20), id3 BIGINT, PRIMARY KEY (id1, id2, id3))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create table Resorts (resortId BIGINT, resortName VARCHAR(20), PRIMARY KEY (resortId))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create table LimitOffset (id1 BIGINT, id2 BIGINT, id3 BIGINT, PRIMARY KEY (id1))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create index socialSecurityNumber on persons(socialSecurityNumber)");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create table nokey (id BIGINT, id2 BIGINT)");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create table nokeysecondaryindex (id BIGINT, id2 BIGINT)");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create index id on nokeysecondaryindex(id)");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create index id3 on limitoffset(id3)");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create index id2 on persons(id2)");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create unique index id3 on persons(id3)");
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
      for (int j = 0; j < recordCount; j++) {
        stmt = conn.prepareStatement("insert into Memberships (personId, membershipName, resortId) VALUES (?, ?, ?)");
        stmt.setLong(1, i);
        stmt.setString(2, "membership-" + j);
        stmt.setLong(3, new long[]{1000, 2000}[j % 2]);
        assertEquals(stmt.executeUpdate(), 1);
      }
    }

    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 5; j++) {
        for (int k = 0; k < 5; k++) {
          stmt = conn.prepareStatement("insert into trikey (id1, id2, id3) VALUES (?, ?, ?)");
          stmt.setLong(1, i);
          stmt.setString(2, "id-" + j);
          stmt.setLong(3, k);
          assertEquals(stmt.executeUpdate(), 1);
        }
      }
    }

    for (int i = 0; i < 4; i++) {
      stmt = conn.prepareStatement("insert into persons (id, id2, id3, id4, id5, num, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
      stmt.setLong(1, i);
      stmt.setLong(2, (i + 100) % 2);
      stmt.setLong(3, i + 100);
      stmt.setLong(4, (i + 100) % 3);
      stmt.setDouble(5, 1);
      stmt.setDouble(6, i * 0.5);
      stmt.setString(7, "ssN-933-28-" + i);
      stmt.setString(8, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
      stmt.setBoolean(9, false);
      stmt.setString(10, "m");
      assertEquals(stmt.executeUpdate(), 1);
    }

    for (int i = 4; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into persons (id, id3, id4, num, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
      stmt.setLong(1, i);
      stmt.setLong(2, i + 100);
      stmt.setLong(3, (i + 100) % 3);
      stmt.setDouble(4, i * 0.5);
      stmt.setString(5, "ssN-933-28-" + i);
      stmt.setString(6, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
      stmt.setBoolean(7, false);
      stmt.setString(8, "m");
      assertEquals(stmt.executeUpdate(), 1);
    }

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into persons (id, id2, id3, id4, num, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
      stmt.setLong(1, i + 100);
      stmt.setLong(2, (i + 100) % 2);
      stmt.setLong(3, i + 100 + recordCount);
      stmt.setLong(4, (i + 100) % 3);
      stmt.setDouble(5, (i + 100) * 0.5);
      stmt.setString(6, "ssN-933-28-" + (i % 4));
      stmt.setString(7, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
      stmt.setBoolean(8, false);
      stmt.setString(9, "m");
      int count = stmt.executeUpdate();
      assertEquals(count, 1);
    }

    for (int i = 0; i < 1000; i++) {
      stmt = conn.prepareStatement("insert into LimitOffset (id1, id2, id3) VALUES (?, ?, ?)");
      stmt.setLong(1, i);
      stmt.setLong(2, i * 2);
      stmt.setLong(3, i);
      assertEquals(stmt.executeUpdate(), 1);
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

    client.beginRebalance("test");

    while (true) {
      if (client.isRepartitioningComplete("test")) {
        break;
      }
      Thread.sleep(1_000);
    }

    for (DatabaseServer server : dbServers) {
      server.shutdownRepartitioner();
    }

    Thread.sleep(5_000);

    executor.shutdownNow();
  }

  @Test
  public void testIn() throws SQLException {

    PreparedStatement stmt = conn.prepareStatement("select * from persons where id in (0, 1, 2, 3, 4, 109, 108, 107) order by id desc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 109);
    ret.next();
    assertEquals(ret.getLong("id"), 108);
    ret.next();
    assertEquals(ret.getLong("id"), 107);
    ret.next();
    assertEquals(ret.getLong("id"), 4);
    ret.next();
    assertEquals(ret.getLong("id"), 3);
    ret.next();
    assertEquals(ret.getLong("id"), 2);
    ret.next();
    assertEquals(ret.getLong("id"), 1);
    ret.next();
    assertEquals(ret.getLong("id"), 0);
    assertFalse(ret.next());
  }


  @Test
  public void testUniqueConstraintViolationOnSecondaryKey() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("create table batch1 (id BIGINT, id2 BIGINT, PRIMARY KEY (id))")) {
      stmt.executeUpdate();
      try (PreparedStatement idxtmt = conn.prepareStatement("create unique index id2 on batch1(id2)")) {
        idxtmt.executeUpdate();
      }

      try (PreparedStatement insertStmt = conn.prepareStatement("insert into batch1 (id, id2) VALUES (?, ?)")) {
        for (int i = 0; i < recordCount; i++) {
          insertStmt.setLong(1, i);
          insertStmt.setLong(2, i + 100);
          insertStmt.addBatch();
        }
        int[] batchRet = insertStmt.executeBatch();
        for (int i = 0; i < recordCount; i++) {
          assertEquals(batchRet[i], BATCH_STATUS_SUCCCESS);
        }
      }
      try (PreparedStatement insertStmt = conn.prepareStatement("insert into batch1 (id, id2) VALUES (?, ?)")) {
        for (int i = 0; i < recordCount; i++) {
          insertStmt.setLong(1, i + 100);
          insertStmt.setLong(2, i + 100);
          insertStmt.addBatch();
        }
        int[] batchRet = insertStmt.executeBatch();
        for (int i = 0; i < recordCount; i++) {
          assertEquals(batchRet[i], BATCH_STATUS_FAILED);
        }
      }
    }

    try (PreparedStatement stmt = conn.prepareStatement(
        "select * from batch1 where id >= 0" )) {
      try (ResultSet rs = stmt.executeQuery()) {
        for (int i = 0; i < recordCount; i++) {
          rs.next();
          assertEquals(rs.getLong("id"), i);
        }
        assertFalse(rs.next());
      }
    }

  }

  @Test
  public void singleKeyBatch() throws SQLException {

    try (PreparedStatement stmt = conn.prepareStatement(
        "explain select * from persons where id=0 or id=1 or id=109 or id=108 order by id desc")) {
      try (ResultSet rs = stmt.executeQuery()) {
        rs.next();
        String value = rs.getString(1);
        assertEquals(value, "Batch index lookup: table=persons, idx=_primarykey, keyCount=4");
      }
    }

    try (PreparedStatement stmt = conn.prepareStatement(
        "select * from persons where id=0 or id=1 or id=109 or id=108 order by id desc")) {
      try (ResultSet rs = stmt.executeQuery()) {
        rs.next();
        assertEquals(rs.getLong("id"), 109);
        rs.next();
        assertEquals(rs.getLong("id"), 108);
        rs.next();
        assertEquals(rs.getLong("id"), 1);
        rs.next();
        assertEquals(rs.getLong("id"), 0);
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void compositeBatch() throws SQLException {

    try (PreparedStatement stmt = conn.prepareStatement(
        "explain select * from memberships where personId=? and membershipname='membership-1' or " +
            "personid=? and membershipname='membership-2' or " +
            "personid=? and membershipname='membership-3' or " +
            "personid=? and membershipname='membership-4' or " +
            "personid=? and membershipname='membership-5'")) {
      stmt.setLong(1, 1);
      stmt.setLong(2, 2);
      stmt.setLong(3, 3);
      stmt.setLong(4, 4);
      stmt.setLong(5, 5);
      try (ResultSet rs = stmt.executeQuery()) {
        rs.next();
        String value = rs.getString(1);
        assertEquals(value, "Batch index lookup: table=memberships, idx=_primarykey, keyCount=5");
      }
    }

    try (PreparedStatement stmt = conn.prepareStatement(
        "select * from memberships where personId=? and membershipname='membership-1' or " +
            "personid=? and membershipname='membership-2' or " +
            "personid=? and membershipname='membership-3' or " +
            "personid=? and membershipname='membership-4' or " +
            "personid=? and membershipname='membership-5'")) {
      stmt.setLong(1, 1);
      stmt.setLong(2, 2);
      stmt.setLong(3, 3);
      stmt.setLong(4, 4);
      stmt.setLong(5, 5);
      try (ResultSet rs = stmt.executeQuery()) {
        rs.next();
        assertEquals(rs.getInt("personid"), 1);
        assertEquals(rs.getString("membershipname"), "membership-1");
        rs.next();
        assertEquals(rs.getInt("personid"), 2);
        assertEquals(rs.getString("membershipname"), "membership-2");
        rs.next();
        assertEquals(rs.getInt("personid"), 3);
        assertEquals(rs.getString("membershipname"), "membership-3");
        rs.next();
        assertEquals(rs.getInt("personid"), 4);
        assertEquals(rs.getString("membershipname"), "membership-4");
        rs.next();
        assertEquals(rs.getInt("personid"), 5);
        assertEquals(rs.getString("membershipname"), "membership-5");
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void compositeBatchTriKey() throws SQLException {

    try (PreparedStatement stmt = conn.prepareStatement(
        "explain select * from trikey where id1=0 and id2='id-3' and id3=2 or " +
            "?=id1 and id2='id-2' and id3=2 or " +
            "2=id1 and id2='id-1' and id3=2 or " +
            "3=id1 and id2='id-0' and id3=?")
    ) {
      stmt.setLong(1, 1);
      stmt.setLong(2, 2);
      try (ResultSet rs = stmt.executeQuery()) {
        rs.next();
        String value = rs.getString(1);
        assertEquals(value, "Batch index lookup: table=trikey, idx=_primarykey, keyCount=4");
      }
    }

    try (PreparedStatement stmt = conn.prepareStatement(
        "select * from trikey where id1=0 and id2='id-3' and id3=2 or " +
            "?=id1 and id2='id-2' and id3=2 or " +
            "2=id1 and id2='id-1' and id3=2 or " +
            "3=id1 and id2='id-0' and id3=?")
    ) {
      stmt.setLong(1, 1);
      stmt.setLong(2, 2);
      try (ResultSet rs = stmt.executeQuery()) {
        rs.next();
        assertEquals(rs.getInt("id1"), 0);
        assertEquals(rs.getString("id2"), "id-3");
        assertEquals(rs.getInt("id3"), 2);
        rs.next();
        assertEquals(rs.getInt("id1"), 1);
        assertEquals(rs.getString("id2"), "id-2");
        assertEquals(rs.getInt("id3"), 2);
        rs.next();
        assertEquals(rs.getInt("id1"), 2);
        assertEquals(rs.getString("id2"), "id-1");
        assertEquals(rs.getInt("id3"), 2);
        rs.next();
        assertEquals(rs.getInt("id1"), 3);
        assertEquals(rs.getString("id2"), "id-0");
        assertEquals(rs.getInt("id3"), 2);
        assertFalse(rs.next());
      }
    }
  }

  //@Test
  public void testBatchInsertNoKeySecondaryIndex() throws SQLException {

    try {
      client.syncSchema();
      conn.setAutoCommit(false);
      PreparedStatement stmt = conn.prepareStatement("insert into nokeysecondaryindex (id, id2) VALUES (?, ?)");
      for (int i = 0; i < 10; i++) {
        stmt.setLong(1, 200000 + i);
        stmt.setLong(2, 200000 * 2 + i);
        stmt.addBatch();
      }
      stmt.executeBatch();
      conn.commit();

      stmt = conn.prepareStatement("select * from nokeysecondaryindex where id>=200000");
      ResultSet resultSet = stmt.executeQuery();
      for (int i = 0; i < 10; i++) {
        assertTrue(resultSet.next());
      }
      assertFalse(resultSet.next());
    }
    finally {
      for (int i = 0; i < 10; i++) {
        PreparedStatement stmt = conn.prepareStatement("delete from nokeysecondaryindex where id=?");
        stmt.setLong(1, 200000 + i);
        stmt.executeUpdate();
      }
    }
  }

  @Test
  public void testBatchInsertNoKey() throws SQLException {

//    LocalConsumer consumer = new LocalConsumer();
//    while (true) {
//      List<Message> msgs = consumer.receive();
//      if (msgs == null || msgs.size() == 0) {
//        break;
//      }
//    }

    client.syncSchema();
    conn.setAutoCommit(false);
    PreparedStatement stmt = conn.prepareStatement("insert into nokey (id, id2) VALUES (?, ?)");
    for (int i = 0; i < 10; i++) {
      stmt.setLong(1, 200000 + i);
      stmt.setLong(2, 200000 * 2 + i);
      stmt.addBatch();
      stmt.setLong(1, 200000 + i);
      stmt.setLong(2, 200000 * 2 + i);
      stmt.addBatch();
    }
    stmt.executeBatch();
    conn.commit();

    try {
//      List<Message> msgs = consumer.receive();
//      assertEquals(msgs.size(), 10);
//      int offset = 0;
//      for (Message msg : msgs) {
//        String actual = msg.getBody();
//        ObjectMapper mapper = new ObjectMapper();
//        ObjectNode dict = (ObjectNode) mapper.readTree(actual);
//        assertEquals(dict.withArray("records").size(), 1);
//        JsonNode node = dict.withArray("records").get(0);
//        assertEquals(node.get("id").asLong(), 200000 + (offset / 2));
//        offset += 1;
//      }


      stmt = conn.prepareStatement("select * from nokey where id>=200000");
      ResultSet resultSet = stmt.executeQuery();
      for (int i = 0; i < 10; i++) {
        assertTrue(resultSet.next());
        assertEquals(resultSet.getLong("id"), 200000 + i);
        assertTrue(resultSet.next());
        assertEquals(resultSet.getLong("id"), 200000 + i);
      }
      assertFalse(resultSet.next());

      for (int i = 0; i < 10; i++) {
        stmt = conn.prepareStatement("delete from nokey where id=?");
        stmt.setLong(1, 200000 + i);
        stmt.executeUpdate();
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  @Test
  public void testBatchInsert() throws SQLException, InterruptedException {

    try {

//      LocalConsumer consumer = new LocalConsumer();
//      while (true) {
//        List<Message> msgs = consumer.receive();
//        if (msgs == null || msgs.size() == 0) {
//          break;
//        }
//      }

      client.syncSchema();
      conn.setAutoCommit(false);
      PreparedStatement stmt = conn.prepareStatement("insert into persons (id, id2, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?)");
      for (int i = 0; i < 10; i++) {
        stmt.setLong(1, 200000 + i);
        stmt.setLong(2, (100) % 2);
        stmt.setString(3, "ssn");
        stmt.setString(4, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
        stmt.setBoolean(5, false);
        stmt.setString(6, "m");
        stmt.addBatch();
      }
      stmt.executeBatch();
      conn.commit();

      try {
        stmt = conn.prepareStatement("update persons set id=1 where id=1");
        for (int i = 0; i < 10; i++) {
          stmt.addBatch();
        }
        stmt.executeBatch();
      }
      catch (Exception e) {
        System.out.println("pass");
      }

      try {
        stmt = conn.prepareStatement("delete from persons where id=1");
        for (int i = 0; i < 10; i++) {
          stmt.addBatch();
        }
        stmt.executeBatch();
      }
      catch (Exception e) {
        System.out.println("pass");
      }

      Thread.sleep(5000);

      stmt = conn.prepareStatement("select * from persons where id>=200000");
      ResultSet resultSet = stmt.executeQuery();
      for (int i = 0; i < 10; i++) {
        assertTrue(resultSet.next());
      }
      assertFalse(resultSet.next());
    }
    finally {
      for (int i = 0; i < 10; i++) {
        PreparedStatement stmt = conn.prepareStatement("delete from persons where id=?");
        stmt.setLong(1, 200000 + i);
        stmt.executeUpdate();
      }
    }
  }


}
