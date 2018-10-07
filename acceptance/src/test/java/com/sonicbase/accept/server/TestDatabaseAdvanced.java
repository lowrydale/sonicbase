package com.sonicbase.accept.server;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.*;
import com.sonicbase.index.Index;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.server.DatabaseServer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.UnsupportedEncodingException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.testng.Assert.*;

public class TestDatabaseAdvanced {

  private Connection conn;
  private final int recordCount = 10;
  final List<Long> ids = new ArrayList<>();
  DatabaseServer[] dbServers;

  @AfterClass(alwaysRun = true)
  public void afterClass() throws SQLException {
    conn.close();
    for (DatabaseServer server : dbServers) {
      server.shutdown();
    }
    System.out.println("client refCount=" + DatabaseClient.clientRefCount.get() + ", sharedClients=" + DatabaseClient.sharedClients.size() + ", class=TestDatabaseAdvanced");
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
      dbServers[i].setConfig(config, "4-servers", "localhost", 9010 + (50 * i), true, new AtomicBoolean(true), new AtomicBoolean(true),null);
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

    DatabaseClient client = ((ConnectionProxy)conn).getDatabaseClient();


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
      ids.add((long) i);
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
      ids.add((long) i);
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
      ids.add((long) (i + 100));
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
      Thread.sleep(1000);
    }

    for (DatabaseServer server : dbServers) {
      server.shutdownRepartitioner();
    }

    dbServers[3].enableSnapshot(false);

    File snapshotDir = new File(System.getProperty("user.home"), "db/snapshot/1/1");
    FileUtils.deleteDirectory(snapshotDir);

    ComObject cobj = new ComObject();
    client.send("DatabaseServer:disableServer", 1, 1, cobj, DatabaseClient.Replica.SPECIFIED);
    client.send("DatabaseServer:reloadServer", 1, 1, cobj, DatabaseClient.Replica.SPECIFIED);

    boolean complete = false;
    while (!complete) {
      byte[] bytes = client.send("DatabaseServer:isServerReloadFinished", 1, 1, cobj, DatabaseClient.Replica.SPECIFIED);
      ComObject retObj = new ComObject(bytes);
      complete = retObj.getBoolean(ComObject.Tag.IS_COMPLETE);
      Thread.sleep(200);
    }

    File tableDir = dbServers[2].getSnapshotManager().getTableSchemaDir("test", "persons");

    File[] tableFiles = tableDir.listFiles();
    DatabaseCommon.sortSchemaFiles(tableFiles);
    File tableFile = tableFiles[tableFiles.length - 1];
    int tableVersion = getVersionFromFile(tableFile.getName());
    tableFile.delete();


    File indexDir = dbServers[2].getSnapshotManager().getIndexSchemaDir("test", "persons", "_primarykey");

    File[] indexFiles = indexDir.listFiles();
    DatabaseCommon.sortSchemaFiles(indexFiles);
    File indexFile = indexFiles[indexFiles.length - 1];
    int indexVersion = getVersionFromFile(indexFile.getName());
    indexFile.delete();

    long begin = System.currentTimeMillis();
    dbServers[0].getSchemaManager().reconcileSchema();
    System.out.println("reconcile duration=" + (System.currentTimeMillis() - begin));

    File[] files = tableDir.listFiles();
    DatabaseCommon.sortSchemaFiles(files);

    String filename = files[files.length - 1].getName();
    assertTrue(getVersionFromFile(filename) >= tableVersion);

    files = indexDir.listFiles();
    DatabaseCommon.sortSchemaFiles(files);

    filename = files[files.length - 1].getName();
    assertTrue(getVersionFromFile(filename) >= indexVersion);


//    Thread.sleep(30000);

//    assertEquals(client.getPartitionSize("test", 0, "persons", "_1__primarykey"), 9);
//    assertEquals(client.getPartitionSize("test", 1, "persons", "_1__primarykey"), 11);

//    assertEquals(client.getPartitionSize(2, "persons", "_1__primarykey"), 9);
//    assertEquals(client.getPartitionSize(3, "persons", "_1__primarykey"), 8);

    Thread.sleep(10000);

    executor.shutdownNow();
  }

  private Integer getVersionFromFile(String filename) {
    int pos = filename.indexOf(".");
    int pos2 = filename.indexOf(".", pos + 1);
    return Integer.valueOf(filename.substring(pos + 1, pos2));
  }

  @Test
  public void testUpsertNotUnique() throws SQLException {
    try {
      PreparedStatement stmt = conn.prepareStatement("insert ignore into persons (id, id2) values (1000, 1000)");
      stmt.executeUpdate();
      stmt = conn.prepareStatement("insert ignore into persons (id, id2) values (?, 1001)");
      stmt.setLong(1, 1000);
      stmt.executeUpdate();

      stmt = conn.prepareStatement("select * from persons where  id2=1001");
      ResultSet rs = stmt.executeQuery();
      assertTrue(rs.next());
    }
    finally {
      PreparedStatement stmt = conn.prepareStatement("delete from persons where id=1000");
      stmt.executeUpdate();
      stmt = conn.prepareStatement("delete from persons where id2=1000");
      stmt.executeUpdate();
    }
  }

  @Test
  public void testNoKeyIndexLookupExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from nokey where  id=0")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Table scan: table=nokey nokey.id = 0\n");
      }
    }
  }

  @Test
  public void testNoKeyIndexLookup() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from nokey where  id=0");
    ResultSet rs = stmt.executeQuery();
    assertTrue(rs.next());
    assertEquals(rs.getLong("id"), 0);
    assertTrue(rs.next());
    assertEquals(rs.getLong("id"), 0);
    assertFalse(rs.next());
  }

  @Test
  public void testOrPaging() throws SQLException {

    int prevSize = ((ConnectionProxy)conn).getDatabaseClient().getPageSize();
    ((ConnectionProxy)conn).getDatabaseClient().setPageSize(1);
    try {
      try (PreparedStatement stmt = conn.prepareStatement(
          "explain select * from persons where id>=100 or id2=0")) {
        try (ResultSet rs = stmt.executeQuery()) {
          rs.next();
          String value = rs.getString(1);
        }
      }

      try (PreparedStatement stmt = conn.prepareStatement(
          "select * from persons where id>=100 or id2=0" )) {
        try (ResultSet rs = stmt.executeQuery()) {
          for (int i = 0; i < recordCount; i++) {
            rs.next();
             assertEquals(rs.getLong("id"), i + 100);
          }
          rs.next();
          assertEquals(rs.getLong("id"), 0);
          rs.next();
          assertEquals(rs.getLong("id"), 2);
          assertFalse(rs.next());
        }
      }
    }
    finally {
      ((ConnectionProxy)conn).getDatabaseClient().setPageSize(prevSize);
    }
  }

  @Test
  public void testOrPagingWithSort() throws SQLException {

    int prevSize = ((ConnectionProxy)conn).getDatabaseClient().getPageSize();
    ((ConnectionProxy)conn).getDatabaseClient().setPageSize(1);
    try {
      try (PreparedStatement stmt = conn.prepareStatement(
          "explain select * from persons where id>=100 or id2=0 order by id desc")) {
        try (ResultSet rs = stmt.executeQuery()) {
          rs.next();
          String value = rs.getString(1);
        }
      }

      try (PreparedStatement stmt = conn.prepareStatement(
          "select * from persons where id>=100 or id2=0 order by id desc")) {
        try (ResultSet rs = stmt.executeQuery()) {
          for (int i = recordCount - 1; i >= 0; i--) {
            rs.next();
            assertEquals(rs.getLong("id"), i + 100);
          }
          rs.next();
          assertEquals(rs.getLong("id"), 2);
          rs.next();
          assertEquals(rs.getLong("id"), 0);
          assertFalse(rs.next());
        }
      }
    }
    finally {
      ((ConnectionProxy)conn).getDatabaseClient().setPageSize(prevSize);
    }
  }

//  @Test
//  public void testUpdateNonUniqueConstraintViolation() throws SQLException {
//
//    int prevSize = ((ConnectionProxy) conn).getDatabaseClient().getPageSize();
//    ((ConnectionProxy) conn).getDatabaseClient().setPageSize(1);
//    try {
//      try (PreparedStatement stmt = conn.prepareStatement(
//          "update persons set id = id + 1 where id>=100 order by id desc")) {
//        stmt.executeUpdate();
//      }
//      try (PreparedStatement stmt = conn.prepareStatement(
//          "select * from persons where id>=100 order by id desc")) {
//        try (ResultSet rs = stmt.executeQuery()) {
//          for (int i = recordCount - 1; i >= 0; i--) {
//            rs.next();
//            assertEquals(rs.getLong("id"), i + 100 + 1);
//          }
//          assertFalse(rs.next());
//        }
//      }
//    }
//    finally {
//      ((ConnectionProxy)conn).getDatabaseClient().setPageSize(prevSize);
//    }
//  }

//  @Test
//  public void testUpdateUniqueConstraintViolation() throws SQLException {
//
//    int prevSize = ((ConnectionProxy) conn).getDatabaseClient().getPageSize();
//    ((ConnectionProxy) conn).getDatabaseClient().setPageSize(1);
//    try {
//      try (PreparedStatement stmt = conn.prepareStatement(
//          "select * from persons where id>=100 or id2=0 order by id desc")) {
//        try (ResultSet rs = stmt.executeQuery()) {
//          for (int i = recordCount - 1; i >= 0; i--) {
//            rs.next();
//            System.out.println(rs.getLong("id"));
//            assertEquals(rs.getLong("id"), i + 100);
//          }
//          rs.next();
//          assertEquals(rs.getLong("id"), 2);
//          rs.next();
//          assertEquals(rs.getLong("id"), 0);
//          assertFalse(rs.next());
//        }
//      }
//      try (PreparedStatement stmt = conn.prepareStatement(
//          "update persons set id = id + 1 where id>=100 or id2=0 order by id asc")) {
//        stmt.executeUpdate();
//      }
//      catch (Exception e) {
//        assertNotEquals(ExceptionUtils.indexOfThrowable(e, UniqueConstraintViolationException.class), -1);
//      }
//      try (PreparedStatement stmt = conn.prepareStatement(
//          "select * from persons where id>=100 or id2=0 order by id desc")) {
//        try (ResultSet rs = stmt.executeQuery()) {
//          for (int i = recordCount - 1; i >= 0; i--) {
//            rs.next();
//            System.out.println(rs.getLong("id"));
//            assertEquals(rs.getLong("id"), i + 100);
//          }
//          rs.next();
//          assertEquals(rs.getLong("id"), 2);
//          rs.next();
//          assertEquals(rs.getLong("id"), 0);
//          assertFalse(rs.next());
//        }
//      }
//    }
//    finally {
//      ((ConnectionProxy)conn).getDatabaseClient().setPageSize(prevSize);
//    }
//  }

  @Test
  public void testUpdateNull() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(
        "update persons set gender = null where id=2")) {
      stmt.executeUpdate();
    }

    try (PreparedStatement stmt = conn.prepareStatement(
        "select * from persons where gender = null")) {
      try (ResultSet rs = stmt.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(rs.getLong("id"), 2);
        assertFalse(rs.next());
      }
    }

    try (PreparedStatement stmt = conn.prepareStatement(
        "update persons set gender = null where id >= 0")) {
      stmt.executeUpdate();
    }

    try (PreparedStatement stmt = conn.prepareStatement(
        "update persons set gender = 'm' where id=2")) {
      stmt.executeUpdate();
    }

    try (PreparedStatement stmt = conn.prepareStatement(
        "select * from persons where gender != null")) {
      try (ResultSet rs = stmt.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(rs.getLong("id"), 2);
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void testUpdateNullOneSidedIndex() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(
        "update persons set gender = null")) {
      stmt.executeUpdate();
    }

    try (PreparedStatement stmt = conn.prepareStatement(
        "update persons set gender = 'm' where id=2")) {
      stmt.executeUpdate();
    }

    try (PreparedStatement stmt = conn.prepareStatement(
        "explain select * from persons where id < 5 and gender != null")) {
      try (ResultSet rs = stmt.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for relational op: table=persons, idx=_primarykey, persons.id < 5\n" +
            "single key index lookup\n" +
            " AND \n" +
            "Read record from index and evaluate: persons.gender != null\n");
      }
    }

    try (PreparedStatement stmt = conn.prepareStatement(
        "select * from persons where id < 5 and gender != null")) {
      try (ResultSet rs = stmt.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(rs.getLong("id"), 2);
        assertFalse(rs.next());
      }
    }

    try (PreparedStatement stmt = conn.prepareStatement(
        "explain select * from persons where id != null and gender = 'm'")) {
      try (ResultSet rs = stmt.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Table scan: table=persons persons.id != null and persons.gender = m\n");
      }
    }

    try (PreparedStatement stmt = conn.prepareStatement(
        "select * from persons where id != null and gender = 'm'")) {
      try (ResultSet rs = stmt.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(rs.getLong("id"), 2);
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void testDoubleTableScanExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from persons where id = null and gender = null")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Table scan: table=persons persons.id = null and persons.gender = null\n");
      }
    }
  }

  @Test
  public void testUpdateUniqueConstraintViolation() throws SQLException {

    int prevSize = ((ConnectionProxy) conn).getDatabaseClient().getPageSize();
    ((ConnectionProxy) conn).getDatabaseClient().setPageSize(1);
    try {
      try (PreparedStatement stmt = conn.prepareStatement(
          "select * from persons where id>=100 or id2=0 order by id desc")) {
        try (ResultSet rs = stmt.executeQuery()) {
          for (int i = recordCount - 1; i >= 0; i--) {
            rs.next();
            System.out.println(rs.getLong("id"));
            assertEquals(rs.getLong("id"), i + 100);
          }
          rs.next();
          assertEquals(rs.getLong("id"), 2);
          rs.next();
          assertEquals(rs.getLong("id"), 0);
          assertFalse(rs.next());
        }
      }
      try (PreparedStatement stmt = conn.prepareStatement(
          "update persons set id = id + 1 where id>=100 or id2=0 order by id asc")) {
        stmt.executeUpdate();
      }
      catch (Exception e) {
        assertNotEquals(ExceptionUtils.indexOfThrowable(e, UniqueConstraintViolationException.class), -1);
      }
      try (PreparedStatement stmt = conn.prepareStatement(
          "select * from persons where id>=100 or id2=0 order by id desc")) {
        try (ResultSet rs = stmt.executeQuery()) {
          for (int i = recordCount - 1; i >= 0; i--) {
            rs.next();
            System.out.println(rs.getLong("id"));
            assertEquals(rs.getLong("id"), i + 100);
          }
          rs.next();
          assertEquals(rs.getLong("id"), 2);
          rs.next();
          assertEquals(rs.getLong("id"), 0);
          assertFalse(rs.next());
        }
      }
    }
    finally {
      ((ConnectionProxy)conn).getDatabaseClient().setPageSize(prevSize);
    }
  }

  @Test
  public void testInsertUniqueConstraintViolationOnSecondaryIndex() throws SQLException {

        int prevSize = ((ConnectionProxy) conn).getDatabaseClient().getPageSize();
        ((ConnectionProxy) conn).getDatabaseClient().setPageSize(1);

    try (PreparedStatement stmt = conn.prepareStatement(
        "insert into persons (id, id3) values (1000, 100)")) {
      stmt.executeUpdate();
    }
    catch (Exception e) {
      assertNotEquals(ExceptionUtils.indexOfThrowable(e, UniqueConstraintViolationException.class), -1);
    }
    System.out.println("after");
    try (PreparedStatement stmt = conn.prepareStatement(
          "select * from persons where id>=100 order by id desc")) {
      try (ResultSet rs = stmt.executeQuery()) {
        for (int i = 100 + recordCount - 1; i >= 100; i--) {
          rs.next();
          assertEquals(rs.getLong("id"), i);
        }
        assertFalse(rs.next());
      }
    }
    finally {
      ((ConnectionProxy)conn).getDatabaseClient().setPageSize(prevSize);
    }
  }

  @Test
  public void testUpdateOrPagingWithSort() throws SQLException {

    int prevSize = ((ConnectionProxy)conn).getDatabaseClient().getPageSize();
    ((ConnectionProxy)conn).getDatabaseClient().setPageSize(1);
    try {
      try (PreparedStatement stmt = conn.prepareStatement(
          "update persons set id4 = id4 + 1 where id>=100 or id2=0 order by id desc")) {
        stmt.executeUpdate();
      }
      try (PreparedStatement stmt = conn.prepareStatement(
          "select * from persons where id>=100 or id2=1 order by id desc")) {
        try (ResultSet rs = stmt.executeQuery()) {
          rs.next();
          assertEquals(rs.getLong("id4"), 2);
          rs.next();
          assertEquals(rs.getLong("id4"), 1);
          rs.next();
          assertEquals(rs.getLong("id4"), 3);
          rs.next();
          assertEquals(rs.getLong("id4"), 2);
          rs.next();
          assertEquals(rs.getLong("id4"), 1);
          rs.next();
          assertEquals(rs.getLong("id4"), 3);
          rs.next();
          assertEquals(rs.getLong("id4"), 2);
          rs.next();
          assertEquals(rs.getLong("id4"), 1);
          rs.next();
          assertEquals(rs.getLong("id4"), 3);
          rs.next();
          assertEquals(rs.getLong("id4"), 2);
          rs.next();
          assertEquals(rs.getLong("id4"), 1);
          rs.next();
          assertEquals(rs.getLong("id4"), 2);
          assertFalse(rs.next());
        }
      }
    }
    finally {
      ((ConnectionProxy)conn).getDatabaseClient().setPageSize(prevSize);
    }
  }

  @Test
  public void testOffsetLimit() throws SQLException {

    try (PreparedStatement stmt = conn.prepareStatement("select * from limitoffset order by id1 asc limit 2 offset 2");
         ResultSet rs = stmt.executeQuery()) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id1"), 1);
      assertTrue(rs.next());
      assertEquals(rs.getLong("id1"), 2);
      assertFalse(rs.next());
    }

    try (PreparedStatement stmt = conn.prepareStatement("select * from limitoffset order by id1 desc limit 2 offset 2");
         ResultSet rs = stmt.executeQuery()) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id1"), 998);
      assertTrue(rs.next());
      assertEquals(rs.getLong("id1"), 997);
      assertFalse(rs.next());
    }

    try (PreparedStatement stmt = conn.prepareStatement("select * from limitoffset order by id3 asc limit 2 offset 2");
         ResultSet rs = stmt.executeQuery()) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id3"), 1);
      assertTrue(rs.next());
      assertEquals(rs.getLong("id3"), 2);
      assertFalse(rs.next());
    }

    try (PreparedStatement stmt = conn.prepareStatement("select * from limitoffset order by id1 asc limit 150 offset 100");
         ResultSet rs = stmt.executeQuery()) {
      for (int i = 99; i < 249; i++) {
        assertTrue(rs.next());
        assertEquals(rs.getLong("id1"), i);
      }
      assertFalse(rs.next());
    }

    try (PreparedStatement stmt = conn.prepareStatement("select * from limitoffset order by id1 desc limit 50 offset 150");
         ResultSet rs = stmt.executeQuery()) {
      for (int i = 850; i > 800; i--) {
        assertTrue(rs.next());
        assertEquals(rs.getLong("id1"), i);
      }
      assertFalse(rs.next());
    }

    try (PreparedStatement stmt = conn.prepareStatement("select * from limitoffset order by id3 asc limit 150 offset 100");
         ResultSet rs = stmt.executeQuery()) {
      for (int i = 99; i < 249; i++) {
        assertTrue(rs.next());
        assertEquals(rs.getLong("id1"), i);
      }
      assertFalse(rs.next());
    }


    int pageSize = ((ConnectionProxy)conn).getDatabaseClient().getPageSize();
    ((ConnectionProxy)conn).getDatabaseClient().setPageSize(1);
    try (PreparedStatement stmt = conn.prepareStatement("select * from limitoffset order by id2 asc limit 2 offset 2");
          ResultSet rs = stmt.executeQuery()) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id2"), 4);
      assertTrue(rs.next());
      assertEquals(rs.getLong("id2"), 6);
    }
    ((ConnectionProxy)conn).getDatabaseClient().setPageSize(pageSize);
  }

  @Test
  public void testSizeExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select count(*) from persons")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Count records, all shards: table=persons, column=null, expression=<all>\n");
      }
    }
  }

  @Test
  public void testSize() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("select count(*) from persons");
         ResultSet ret = stmt.executeQuery()) {
      assertEquals(ret.getInt(1), 2 * recordCount);
    }
  }

  @Test
  public void testMathLeftExpressionJustMathExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id, id5 from persons where id < 2 + 1 order by id asc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Table scan: table=persons persons.id < 2 + 1\n");
      }
    }
  }

  @Test
  public void testMathLeftExpressionJustMath() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("select id, id5 from persons where id < 2 + 1 order by id asc");
          ResultSet ret = stmt.executeQuery()) {

      assertTrue(ret.next());
      assertEquals(ret.getLong("id"), 0);
      assertEquals(ret.getLong("id5"), 1);
      assertTrue(ret.next());
      assertEquals(ret.getLong("id"), 1);
      assertEquals(ret.getLong("id5"), 1);
      assertTrue(ret.next());
      assertEquals(ret.getLong("id"), 2);
      assertEquals(ret.getLong("id5"), 1);
      assertFalse(ret.next());
    }
  }

  @Test (enabled=false)
  public void testMathLeftExpressionTwoColumns() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id5 from persons where 5 < id + id5 order by id asc");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 0);
    assertEquals(ret.getLong("id5"), 1);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 1);
    assertEquals(ret.getLong("id5"), 1);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 2);
    assertEquals(ret.getLong("id5"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testMathLeftExpressionTwoColumns2Explain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id, id5 from persons where id < id5 + id5 order by id asc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Table scan: table=persons persons.id < persons.id5 + persons.id5\n");
      }
    }
  }

  @Test
  public void testMathLeftExpressionTwoColumns2() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id5 from persons where id < id5 + id5 order by id asc");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 0);
    assertEquals(ret.getLong("id5"), 1);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 1);
    assertEquals(ret.getLong("id5"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testMathLeftExpressionGreaterEqualExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id, id5 from persons where id >= id5 + 1 and id > 1 order by id asc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for relational op: table=persons, idx=_primarykey, persons.id > 1\n" +
            "single key index lookup\n" +
            " AND \n" +
            "Read record from index and evaluate: persons.id >= persons.id5 + 1\n");
      }
    }
  }

  @Test
  public void testMathLeftExpressionGreaterEqual() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id5 from persons where id >= id5 + 1 and id > 1 order by id asc");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 2);
    assertEquals(ret.getLong("id5"), 1);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 3);
    assertEquals(ret.getLong("id5"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testMathLeftExpressionExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id, id5 from persons where id = id5 + 1 and id > 1 order by id asc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for relational op: table=persons, idx=_primarykey, persons.id > 1\n" +
            "single key index lookup\n" +
            " AND \n" +
            "Read record from index and evaluate: persons.id = persons.id5 + 1\n");
      }
    }
  }

  @Test
  public void testMathLeftExpression() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id5 from persons where id = id5 + 1 and id > 1 order by id asc");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 2);
    assertEquals(ret.getLong("id5"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testMathRightExpressionExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id, id5 from persons where id > 1 and id = id5 + 1 order by id asc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Table scan: table=persons persons.id > 1 and persons.id = persons.id5 + 1\n");
      }
    }
  }

  @Test
  public void testMathRightExpression() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id5 from persons where id > 1 and id = id5 + 1 order by id asc");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 2);
    assertEquals(ret.getLong("id5"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testMathExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id, id5 from persons where id = id5 + 1 order by id asc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Table scan: table=persons persons.id = persons.id5 + 1\n");
      }
    }
  }

  @Test
  public void testMath() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id5 from persons where id = id5 + 1 order by id asc");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 2);
    assertEquals(ret.getLong("id5"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testMathMultiplyExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id, id5 from persons where id = id5 * 2 order by id asc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Table scan: table=persons persons.id = persons.id5 * 2\n");
      }
    }
  }

  @Test
  public void testMathMultiply() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id5 from persons where id = id5 * 2 order by id asc");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 2);
    assertEquals(ret.getLong("id5"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testMathDivideExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id, id5 from persons where id = id5 / 2 order by id asc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Table scan: table=persons persons.id = persons.id5 / 2\n");
      }
    }
  }

  @Test
  public void testMathDivide() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id5 from persons where id = id5 / 2 order by id asc");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 0);
    assertEquals(ret.getLong("id5"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testMathMinusExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id, id5 from persons where id = id5 - 1 order by id asc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Table scan: table=persons persons.id = persons.id5 - 1\n");
      }
    }
  }

  @Test
  public void testMathMinus() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id5 from persons where id = id5 - 1 order by id asc");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 0);
    assertEquals(ret.getLong("id5"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testMathBitwiseAndExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id, id5 from persons where id = id5 & 1 order by id asc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Table scan: table=persons persons.id = persons.id5 & 1\n");
      }
    }
  }

  @Test
  public void testMathBitwiseAnd() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id5 from persons where id = id5 & 1 order by id asc");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 1);
    assertEquals(ret.getLong("id5"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testMathBitwiseOrExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id, id5 from persons where id = id5 | 2 order by id asc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Table scan: table=persons persons.id = persons.id5 | 2\n");
      }
    }
  }

  @Test
  public void testMathBitwiseOr() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id5 from persons where id = id5 | 2 order by id asc");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 3);
    assertEquals(ret.getLong("id5"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testMathBitwiseXOrExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id, id5 from persons where id = id5 ^ 1 order by id asc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Table scan: table=persons persons.id = persons.id5 ^ 1\n");
      }
    }
  }

  @Test
  public void testMathBitwiseXOr() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id5 from persons where id = id5 ^ 1 order by id asc");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 0);
    assertEquals(ret.getLong("id5"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testMathDoubleExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id, id5 from persons where id < id5 * 1.5 order by id asc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Table scan: table=persons persons.id < persons.id5 * 1.5\n");
      }
    }
  }

  @Test
  public void testMathDouble() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id5 from persons where id < id5 * 1.5 order by id asc");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 0);
    assertEquals(ret.getLong("id5"), 1);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 1);
    assertEquals(ret.getLong("id5"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testMathModuloExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id, id5 from persons where id = id5 % 1 order by id asc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Table scan: table=persons persons.id = persons.id5 % 1\n");
      }
    }
  }

  @Test
  public void testMathModulo() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id5 from persons where id = id5 % 1 order by id asc");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 0);
    assertEquals(ret.getLong("id5"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testIncompatibleTypesExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id, id5 from persons where id < 5.4 order by id asc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for relational op: table=persons, idx=_primarykey, persons.id < 5.4\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test
  public void testIncompatibleTypes() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id5 from persons where id < 5.4 order by id asc");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 0);
    assertEquals(ret.getLong("id5"), 1);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 1);
    assertEquals(ret.getLong("id5"), 1);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 2);
    assertEquals(ret.getLong("id5"), 1);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 3);
    assertEquals(ret.getLong("id5"), 1);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 4);
    assertEquals(ret.getLong("id5"), 0);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 5);
    assertEquals(ret.getLong("id5"), 0);
    assertFalse(ret.next());
  }

//  @Test
//  public void testJson() throws SQLException {
//    String dbName = "test";
//    String tableName = "persons";
//    final TableSchema tableSchema = ((ConnectionProxy)conn).getTables(dbName).get(tableName);
//    final List<FieldSchema> fields = tableSchema.getFields();
//
//    final StringBuilder fieldsStr = new StringBuilder();
//    final StringBuilder parmsStr = new StringBuilder();
//    boolean first = true;
//    for (FieldSchema field : fields) {
//      if (field.getName().equals("_sonicbase_id")) {
//        continue;
//      }
//      if (first) {
//        first = false;
//      }
//      else {
//        fieldsStr.append(",");
//        parmsStr.append(",");
//      }
//      fieldsStr.append(field.getName());
//      parmsStr.append("?");
//    }
//
//    PreparedStatement stmt = conn.prepareStatement("insert into " + tableName + " (" + fieldsStr.toString() +
//        ") VALUES (" + parmsStr.toString() + ")");
//
//
//    ObjectNode recordJson = new ObjectNode(JsonNodeFactory.instance);
//    recordJson.put("id", 1000000);
//    recordJson.put("socialSecurityNumber", "529-17-2010");
//    recordJson.put("relatives", "xxxyyyxxx");
//    recordJson.put("restricted", true);
//    recordJson.put("gender", "m");
//
//    Object[] record = StreamManager.getCurrRecordFromJson(recordJson, fields);
//    BulkImportManager.setFieldsInInsertStatement(stmt, 1, record, fields);
//
//    assertEquals(stmt.executeUpdate(), 1);
//
//    stmt = conn.prepareStatement("select * from persons where id = 1000000");
//    ResultSet ret = stmt.executeQuery();
//    assertTrue(ret.next());
//    assertEquals(ret.getLong("id"), 1000000);
//    assertEquals(ret.getString("socialsecuritynumber"), "529-17-2010");
//    assertEquals(ret.getString("relatives"), "xxxyyyxxx");
//    assertEquals(ret.getBoolean("restricted"), true);
//    assertEquals(ret.getString("gender"), "m");
//
//
//    stmt = conn.prepareStatement("delete from persons where id=1000000");
//    stmt.executeUpdate();
//  }
//
//

  @Test
  public void testAliasExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id as i from persons where id < 2 order by id asc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for relational op: table=persons, idx=_primarykey, persons.id < 2\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test
  public void testAlias() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id as i from persons where id < 2 order by id asc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("i"), 0);
    ret.next();
    assertEquals(ret.getLong("i"), 1);

    assertFalse(ret.next());

  }

  @Test
  public void testNotExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id as i from persons where not (id > 2) order by id asc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for not relational op: table=persons, idx=_primarykey, not (persons.id > 2)\n");
      }
    }
  }

  @Test
  public void testNot() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id as i from persons where not (id > 2) order by id asc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("i"), 0);
    ret.next();
    assertEquals(ret.getLong("i"), 1);
    ret.next();
    assertEquals(ret.getLong("i"), 2);
    assertFalse(ret.next());

  }

  @Test
  public void testParensExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id as i from persons where (id < 2) order by id asc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for relational op: table=persons, idx=_primarykey, persons.id < 2\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test
  public void testParens() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id as i from persons where (id < 2) order by id asc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("i"), 0);
    ret.next();
    assertEquals(ret.getLong("i"), 1);
    assertFalse(ret.next());

  }

  @Test
  public void testAlias2Explain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select persons.id as i from persons where id < 2 order by id asc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for relational op: table=persons, idx=_primarykey, persons.id < 2\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test
  public void testAlias2() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select persons.id as i from persons where id < 2 order by id asc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("i"), 0);
    ret.next();
    assertEquals(ret.getLong("i"), 1);
    assertFalse(ret.next());
  }


  @Test(enabled=false)
  public void testNow() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id as i, now() as now from persons where id < 2 order by id asc");
    ResultSet ret = stmt.executeQuery();

    assertTrue(false);
    ret.next();
    assertEquals(ret.getLong("i"), 0);
    ret.next();
    assertEquals(ret.getLong("i"), 1);

    assertFalse(ret.next());
  }

  @Test
  public void testLengthExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select socialsecuritynumber, length(socialsecuritynumber) as Length from persons where id = 0")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for relational op: table=persons, idx=_primarykey, persons.id = 0\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test
  public void testLength() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select socialsecuritynumber, length(socialsecuritynumber) as Length from persons where id = 0");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getString("socialsecuritynumber"), "ssN-933-28-0");
    assertEquals(ret.getLong("length"), 12);
    assertEquals(ret.getInt("length"), 12);
    assertFalse(ret.next());
  }

  @Test
  public void testDistinctExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select distinct id2 from persons where id > 102 order by id asc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for relational op: table=persons, idx=_primarykey, persons.id > 102\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test
  public void testDistinct() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select distinct id2 from persons where id > 102 order by id asc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 1);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertFalse(ret.next());

  }

  @Test
  public void testGroupByNoKeyExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id, id2 from nokey where id < 2 group by id2 order by id")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to server sort\n" +
            "Table scan: table=nokey nokey.id < 2\n");
      }
    }
  }

  @Test
  public void testGroupByNoKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id2 from nokey where id < 2 group by id2 order by id");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 0);
    ret.next();
    assertEquals(ret.getLong("id2"), 2);
    assertEquals(ret.getLong("id"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testGroupByExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id, id2 from persons group by id2 order by id")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for all records: table=persons, idx=id2\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test
  public void testGroupBy() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id2 from persons group by id2 order by id");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 108);
    ret.next();
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getLong("id"), 109);
    assertFalse(ret.next());
  }

  @Test
  public void testServerSortExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id, id2 from persons order by id2 ASC")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for all records: table=persons, idx=id2\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test
  public void testServerSort() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id2 from persons order by id2 ASC");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 0);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 2);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 100);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 102);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 104);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 106);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 108);
    ret.next();
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getLong("id"), 1);
    ret.next();
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getLong("id"), 3);
    ret.next();
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getLong("id"), 101);
    ret.next();
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getLong("id"), 103);
    ret.next();
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getLong("id"), 105);
    ret.next();
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getLong("id"), 107);
    ret.next();
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getLong("id"), 109);
    assertFalse(ret.next());
//    assertEquals(ret.getLong("id2"), 0);
//    assertTrue(ret.wasNull());
//    assertEquals(ret.getLong("id"), 4);
    //assertFalse(ret.next());

  }

  @Test
  public void testServerSortNoKeySecondaryIndexExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id, id2 from nokeysecondaryindex where id < 2 order by id2 ASC")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to server sort\n" +
            "Index lookup for relational op: table=nokeysecondaryindex, idx=id, nokeysecondaryindex.id < 2\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test
  public void testServerSortNoKeySecondaryIndex() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id2 from nokeysecondaryindex where id < 2 order by id2 ASC");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 0);
    ret.next();
    assertEquals(ret.getLong("id2"), 2);
    assertEquals(ret.getLong("id"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testServerSortNoKeyExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id, id2 from nokey where id < 2 order by id2 ASC")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to server sort\n" +
            "Table scan: table=nokey nokey.id < 2\n");
      }
    }
  }

  @Test
  public void testServerSortNoKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id2 from nokey where id < 2 order by id2 ASC");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 0);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 0);
    ret.next();
    assertEquals(ret.getLong("id2"), 2);
    assertEquals(ret.getLong("id"), 1);
    ret.next();
    assertEquals(ret.getLong("id2"), 2);
    assertEquals(ret.getLong("id"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testGroupByNestedNoKeySecondaryIndexExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from nokeysecondaryindex where id < 2 group by id2,id")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to server sort\n" +
            "Index lookup for relational op: table=nokeysecondaryindex, idx=id, nokeysecondaryindex.id < 2\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test
  public void testGroupByNestedNoKeySecondaryIndex() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from nokeysecondaryindex where id < 2 group by id2,id");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 0);
    ret.next();
    assertEquals(ret.getLong("id2"), 2);
    assertEquals(ret.getLong("id"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testGroupByNestedNoKeyExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from nokey where id < 2 group by id2,id")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to server sort\n" +
            "Table scan: table=nokey nokey.id < 2\n");
      }
    }
  }

  @Test
  public void testGroupByNestedNoKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from nokey where id < 2 group by id2,id");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 0);
    ret.next();
    assertEquals(ret.getLong("id2"), 2);
    assertEquals(ret.getLong("id"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testGroupByNestedExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from persons group by id2,id4")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for all records: table=persons, idx=id2\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test
  public void testGroupByNested() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons group by id2,id4");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id4"), 0);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id4"), 1);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id4"), 2);
    ret.next();
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getLong("id4"), 0);
    ret.next();
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getLong("id4"), 1);
    ret.next();
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getLong("id4"), 2);
    assertFalse(ret.next());
  }

  @Test
  public void testGroupByNestedMinNoKeySecondaryIndexExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select min(id) as minValue from nokeysecondaryindex where id < 2 group by id2,id")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to server sort\n" +
            "Table scan: table=nokeysecondaryindex nokeysecondaryindex.id < 2\n");
      }
    }
  }

  @Test
  public void testGroupByNestedMinNoKeySecondaryIndex() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select min(id) as minValue from nokeysecondaryindex where id < 2 group by id2,id");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 0);
    assertEquals(ret.getLong("minValue"), 0);
    ret.next();
    assertEquals(ret.getLong("id2"), 2);
    assertEquals(ret.getLong("id"), 1);
    assertEquals(ret.getLong("minValue"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testGroupByNestedMinNoKeyExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select min(id) as minValue from nokey where id < 2 group by id2,id")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to server sort\n" +
            "Table scan: table=nokey nokey.id < 2\n");
      }
    }
  }

  @Test
  public void testGroupByNestedMinNoKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select min(id) as minValue from nokey where id < 2 group by id2,id");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 0);
    assertEquals(ret.getLong("minValue"), 0);
    ret.next();
    assertEquals(ret.getLong("id2"), 2);
    assertEquals(ret.getLong("id"), 1);
    assertEquals(ret.getLong("minValue"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testGroupByNestedMinExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select min(id) as minValue from persons group by id2,id4")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for all records: table=persons, idx=_primarykey\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test
  public void testGroupByNestedMin() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select min(id) as minValue from persons group by id2,id4");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id4"), 0);
    assertEquals(ret.getLong("minValue"), 2);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id4"), 1);
    assertEquals(ret.getLong("minValue"), 0);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id4"), 2);
    assertEquals(ret.getLong("minValue"), 104);
    ret.next();
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getLong("id4"), 0);
    assertEquals(ret.getLong("minValue"), 105);
    ret.next();
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getLong("id4"), 1);
    assertEquals(ret.getLong("minValue"), 3);
    ret.next();
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getLong("id4"), 2);
    assertEquals(ret.getLong("minValue"), 1);

    assertFalse(ret.next());
  }

  @Test
  public void testGroupByMaxNoKeySecondaryIndexExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id2, max(id) as maxValue from nokeysecondaryindex where id < 2 group by id2")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to server sort\n" +
            "Table scan: table=nokeysecondaryindex nokeysecondaryindex.id < 2\n");
      }
    }
  }

  @Test
  public void testGroupByMaxNoKeySecondaryIndex() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id2, max(id) as maxValue from nokeysecondaryindex where id < 2 group by id2");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getInt("maxValue"), 0);
    ret.next();
    assertEquals(ret.getLong("id2"), 2);
    assertEquals(ret.getInt("maxValue"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testGroupByMaxNoKeyExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id2, max(id) as maxValue from nokey where id < 2 group by id2")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to server sort\n" +
            "Table scan: table=nokey nokey.id < 2\n");
      }
    }
  }

  @Test
  public void testGroupByMaxNoKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id2, max(id) as maxValue from nokey where id < 2 group by id2");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getInt("maxValue"), 0);
    ret.next();
    assertEquals(ret.getLong("id2"), 2);
    assertEquals(ret.getInt("maxValue"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testGroupByMaxExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id2, max(id) as maxValue from persons where id < 200 group by id2")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to server sort\n" +
            "Index lookup for relational op: table=persons, idx=_primarykey, persons.id < 200\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test
  public void testGroupByMax() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id2, max(id) as maxValue from persons where id < 200 group by id2");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getInt("maxValue"), 108);
//
//    ret.next();
//     assertEquals(ret.getLong("id2"), 0);
//     assertEquals(ret.getInt("maxValue"), 108);

    ret.next();
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getInt("maxValue"), 109);
    assertFalse(ret.next());

  }

  @Test
  public void testGroupBySumDoubleExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id2, sum(num) as sumValue from persons group by id2")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for all records: table=persons, idx=_primarykey\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test
  public void testGroupBySumDouble() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id2, sum(num) as sumValue from persons group by id2");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getDouble("sumValue"), 261.0D);

    ret.next();
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getDouble("sumValue"), 264.5D);
    assertFalse(ret.next());
  }

  @Test
  public void testGroupByMinNoKeySecondaryIndexExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id2, min(id) as minValue from nokeysecondaryindex where id < 2 group by id2")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to server sort\n" +
            "Table scan: table=nokeysecondaryindex nokeysecondaryindex.id < 2\n");
      }
    }
  }

  @Test
  public void testGroupByMinNoKeySecondaryIndex() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id2, min(id) as minValue from nokeysecondaryindex where id < 2 group by id2");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getInt("minValue"), 0);

    ret.next();
    assertEquals(ret.getLong("id2"), 2);
    assertEquals(ret.getLong("minValue"), 1);
    assertFalse(ret.next());

  }

  @Test
  public void testGroupByMinNoKeyExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id2, min(id) as minValue from nokey where id < 2 group by id2")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to server sort\n" +
            "Table scan: table=nokey nokey.id < 2\n");
      }
    }
  }

  @Test
  public void testGroupByMinNoKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id2, min(id) as minValue from nokey where id < 2 group by id2");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getInt("minValue"), 0);

    ret.next();
    assertEquals(ret.getLong("id2"), 2);
    assertEquals(ret.getLong("minValue"), 1);
    assertFalse(ret.next());

  }

  @Test
  public void testGroupByMinExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id2, min(id) as minValue from persons group by id2")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for all records: table=persons, idx=_primarykey\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test
  public void testGroupByMin() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id2, min(id) as minValue from persons group by id2");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getInt("minValue"), 0);

    ret.next();
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getLong("minValue"), 1);
    assertFalse(ret.next());

  }

  @Test
  public void testGroupByMinMaxSumAvgNoKeySecondaryIndexExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id2, min(id) as minValue, max(id) as maxValue, sum(id) as sumValue, avg(id) as avgValue from nokeysecondaryindex where id < 2 group by id2")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to server sort\n" +
            "Table scan: table=nokeysecondaryindex nokeysecondaryindex.id < 2\n");
      }
    }
  }

  @Test
  public void testGroupByMinMaxSumAvgNoKeySecondaryIndex() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id2, min(id) as minValue, max(id) as maxValue, sum(id) as sumValue, avg(id) as avgValue from nokeysecondaryindex where id < 2 group by id2");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getInt("minValue"), 0);
    assertEquals(ret.getInt("maxValue"), 0);
    assertEquals(ret.getInt("sumValue"), 0);
    assertEquals(ret.getInt("avgValue"), 0);

    ret.next();
    assertEquals(ret.getLong("id2"), 2);
    assertEquals(ret.getLong("minValue"), 1);
    assertEquals(ret.getLong("maxValue"), 1);
    assertEquals(ret.getLong("sumValue"), 1);
    assertEquals(ret.getLong("avgValue"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testGroupByMinMaxSumAvgNoKeyExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id2, min(id) as minValue, max(id) as maxValue, sum(id) as sumValue, avg(id) as avgValue from nokey where id < 2 group by id2")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to server sort\n" +
            "Table scan: table=nokey nokey.id < 2\n");
      }
    }
  }

  @Test
  public void testGroupByMinMaxSumAvgNoKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id2, min(id) as minValue, max(id) as maxValue, sum(id) as sumValue, avg(id) as avgValue from nokey where id < 2 group by id2");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getInt("minValue"), 0);
    assertEquals(ret.getInt("maxValue"), 0);
    assertEquals(ret.getInt("sumValue"), 0);
    assertEquals(ret.getInt("avgValue"), 0);

    ret.next();
    assertEquals(ret.getLong("id2"), 2);
    assertEquals(ret.getLong("minValue"), 1);
    assertEquals(ret.getLong("maxValue"), 1);
    assertEquals(ret.getLong("sumValue"), 2);
    assertEquals(ret.getLong("avgValue"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testGroupByMinMaxSumAvgExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id2, min(id) as minValue, max(id) as maxValue, sum(id) as sumValue, avg(id) as avgValue from persons group by id2")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for all records: table=persons, idx=_primarykey\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test
  public void testGroupByMinMaxSumAvg() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id2, min(id) as minValue, max(id) as maxValue, sum(id) as sumValue, avg(id) as avgValue from persons group by id2");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getInt("minValue"), 0);
    assertEquals(ret.getInt("maxValue"), 108);
    assertEquals(ret.getInt("sumValue"), 522);
    assertEquals(ret.getInt("avgValue"), 74);

    ret.next();
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getLong("minValue"), 1);
    assertEquals(ret.getLong("maxValue"), 109);
    assertEquals(ret.getLong("sumValue"), 529);
    assertEquals(ret.getLong("avgValue"), 75);
    assertFalse(ret.next());
  }

  @Test
  public void testGroupByMinTwoFieldsNoKeySecondaryIndexExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id2, min(id) as minValue, min(id2) as minId3Value from nokeysecondaryindex where id < 2 group by id2")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to server sort\n" +
            "Table scan: table=nokeysecondaryindex nokeysecondaryindex.id < 2\n");
      }
    }
  }

  @Test
    public void testGroupByMinTwoFieldsNoKeySecondaryIndex() throws SQLException {
      PreparedStatement stmt = conn.prepareStatement("select id2, min(id) as minValue, min(id2) as minId3Value from nokeysecondaryindex where id < 2 group by id2");
      ResultSet ret = stmt.executeQuery();

      ret.next();
      assertEquals(ret.getLong("id2"), 0);
      assertEquals(ret.getInt("minValue"), 0);
      assertEquals(ret.getInt("minId3Value"), 0);

      ret.next();
      assertEquals(ret.getLong("id2"), 2);
      assertEquals(ret.getLong("minValue"), 1);
      assertEquals(ret.getLong("minId3Value"), 2);
      assertFalse(ret.next());
    }

  @Test
  public void testGroupByMinTwoFieldsNoKeyExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id2, min(id) as minValue, min(id2) as minId3Value from nokey where id < 2 group by id2")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to server sort\n" +
            "Table scan: table=nokey nokey.id < 2\n");
      }
    }
  }

  @Test
  public void testGroupByMinTwoFieldsNoKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id2, min(id) as minValue, min(id2) as minId3Value from nokey where id < 2 group by id2");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getInt("minValue"), 0);
    assertEquals(ret.getInt("minId3Value"), 0);

    ret.next();
    assertEquals(ret.getLong("id2"), 2);
    assertEquals(ret.getLong("minValue"), 1);
    assertEquals(ret.getLong("minId3Value"), 2);
    assertFalse(ret.next());
  }

  @Test
  public void testGroupByMinTwoFieldsExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id2, min(id) as minValue, min(id3) as minId3Value from persons group by id2")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for all records: table=persons, idx=_primarykey\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test
  public void testGroupByMinTwoFields() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id2, min(id) as minValue, min(id3) as minId3Value from persons group by id2");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getInt("minValue"), 0);
    assertEquals(ret.getInt("minId3Value"), 100);

    ret.next();
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getLong("minValue"), 1);
    assertEquals(ret.getLong("minId3Value"), 101);
    assertFalse(ret.next());
  }

  @Test
  public void testGroupByMinWhereTablescanNoKeySecondaryIndexExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id2, min(id) as minValue from nokeysecondaryindex where id2 > 0 AND id < 2 group by id2")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to server sort\n" +
            "Table scan: table=nokeysecondaryindex nokeysecondaryindex.id < 2 and nokeysecondaryindex.id2 > 0\n");
      }
    }
  }

  @Test
  public void testGroupByMinWhereTablescanNoKeySecondaryIndex() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id2, min(id) as minValue from nokeysecondaryindex where id2 > 0 AND id < 2 group by id2");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 2);
    assertEquals(ret.getLong("minValue"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testGroupByMinWhereTablescanNoKeyExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id2, min(id) as minValue from nokey where id2 > 0 AND id < 2 group by id2")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to server sort\n" +
            "Table scan: table=nokey nokey.id2 > 0 and nokey.id < 2\n");
      }
    }
  }

  @Test
  public void testGroupByMinWhereTablescanNoKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id2, min(id) as minValue from nokey where id2 > 0 AND id < 2 group by id2");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 2);
    assertEquals(ret.getLong("minValue"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testGroupByMinWhereTablescanExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id2, min(id) as minValue from persons where id2 > 0 group by id2")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Table scan: table=persons persons.id2 > 0\n");
      }
    }
  }

  @Test
  public void testGroupByMinWhereTablescan() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id2, min(id) as minValue from persons where id2 > 0 group by id2");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getLong("minValue"), 1);
    assertFalse(ret.next());
  }


  @Test
  public void testGroupByMinWhereExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id2, min(id) as minValue from persons where id > 5 group by id2")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to server sort\n" +
            "Index lookup for relational op: table=persons, idx=_primarykey, persons.id > 5\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test
  public void testGroupByMinWhere() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id2, min(id) as minValue from persons where id > 5 group by id2");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getInt("minValue"), 100);

    ret.next();
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getLong("minValue"), 101);
    assertFalse(ret.next());

  }

  @Test
  public void testGroupCountNoKeySecondaryIndexExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id2, count(id) as countValue from nokeysecondaryindex where id < 2 group by id2")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to server sort\n" +
            "Table scan: table=nokeysecondaryindex nokeysecondaryindex.id < 2\n");
      }
    }
  }

  @Test
  public void testGroupCountNoKeySecondaryIndex() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id2, count(id) as countValue from nokeysecondaryindex where id < 2 group by id2");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("countValue"), 1);

    ret.next();
    assertEquals(ret.getLong("id2"), 2);
    assertEquals(ret.getLong("countValue"), 1);
    assertFalse(ret.next());

  }

  @Test
  public void testGroupCountNoKeyExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id2, count(id) as countValue from nokey where id < 2 group by id2")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to server sort\n" +
            "Table scan: table=nokey nokey.id < 2\n");
      }
    }
  }

  @Test
  public void testGroupCountNoKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id2, count(id) as countValue from nokey where id < 2 group by id2");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("countValue"), 2);

    ret.next();
    assertEquals(ret.getLong("id2"), 2);
    assertEquals(ret.getLong("countValue"), 2);
    assertFalse(ret.next());

  }

  @Test
  public void testGroupCountExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id2, count(id) as countValue from persons group by id2")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for all records: table=persons, idx=_primarykey\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test
  public void testGroupCount() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id2, count(id) as countValue from persons group by id2");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("countValue"), 7);

    ret.next();
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getLong("countValue"), 7);
    assertFalse(ret.next());

  }

  @Test
  public void testCountDistinctExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select count(distinct id2) as count from persons where id > 100 order by id asc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for relational op: table=persons, idx=_primarykey, persons.id > 100\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test
  public void testCountDistinct() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select count(distinct id2) as count from persons where id > 100 order by id asc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getInt("count"), 2);
    assertFalse(ret.next());

  }

  @Test
  public void testDistinctWhereExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select distinct id2 from persons where id >= 100 order by id asc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for relational op: table=persons, idx=_primarykey, persons.id >= 100\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test
  public void testDistinctWhere() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select distinct id2 from persons where id >= 100 order by id asc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getInt("id"), 100);
    assertEquals(ret.getInt("id2"), 0);
    ret.next();
    assertEquals(ret.getInt("id"), 101);
    assertEquals(ret.getInt("id2"), 1);
//    ret.next();
//    assertEquals(ret.getInt("id"), 102);
//    assertEquals(ret.getInt("id2"), 0);
//    ret.next();
//    assertEquals(ret.getInt("id"), 103);
//    assertEquals(ret.getInt("id2"), 1);
//    ret.next();
//    assertEquals(ret.getInt("id"), 104);
//    assertEquals(ret.getInt("id2"), 0);
//    ret.next();
//    assertEquals(ret.getInt("id"), 105);
//    assertEquals(ret.getInt("id2"), 1);
//    ret.next();
//    assertEquals(ret.getInt("id"), 106);
//    assertEquals(ret.getInt("id2"), 0);
//    ret.next();
//    assertEquals(ret.getInt("id"), 107);
//    assertEquals(ret.getInt("id2"), 1);
//    ret.next();
//    assertEquals(ret.getInt("id"), 108);
//    assertEquals(ret.getInt("id2"), 0);
//    ret.next();
//    assertEquals(ret.getInt("id"), 109);
//    assertEquals(ret.getInt("id2"), 1);
    assertFalse(ret.next());

  }

  @Test
  public void testDistinct2Explain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select distinct id2, socialsecuritynumber from persons where id > 102 order by id asc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for relational op: table=persons, idx=_primarykey, persons.id > 102\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test
  public void testDistinct2() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select distinct id2, socialsecuritynumber from persons where id > 102 order by id asc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getString("socialsecuritynumber"), "ssN-933-28-3");
    assertEquals(ret.getLong("id2"), 1);
    ret.next();
    assertEquals(ret.getString("socialsecuritynumber"), "ssN-933-28-0");
    assertEquals(ret.getLong("id2"), 0);
    ret.next();
    assertEquals(ret.getString("socialsecuritynumber"), "ssN-933-28-1");
    assertEquals(ret.getLong("id2"), 1);
    ret.next();
    assertEquals(ret.getString("socialsecuritynumber"), "ssN-933-28-2");
    assertEquals(ret.getLong("id2"), 0);
    ret.next();
    assertFalse(ret.next());

  }

  @Test
  public void testLowerExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id, socialsecuritynumber, lower(socialsecuritynumber) as lower from persons where id < 5")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for relational op: table=persons, idx=_primarykey, persons.id < 5\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test
  public void testLower() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, socialsecuritynumber, lower(socialsecuritynumber) as lower from persons where id < 5");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 0);
    assertEquals(ret.getString("lower"), "ssn-933-28-0");
    ret.next();
    assertEquals(ret.getLong("id"), 1);
    assertEquals(ret.getString("lower"), "ssn-933-28-1");
    ret.next();
    assertEquals(ret.getLong("id"), 2);
    assertEquals(ret.getString("lower"), "ssn-933-28-2");
    ret.next();
    assertEquals(ret.getLong("id"), 3);
    assertEquals(ret.getString("lower"), "ssn-933-28-3");
    ret.next();
    assertEquals(ret.getLong("id"), 4);
    assertEquals(ret.getString("lower"), "ssn-933-28-4");
    assertFalse(ret.next());
  }

  @Test
  public void testSubstringExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select substring(socialsecuritynumber, 11, 12) as str from persons where id < 5")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for relational op: table=persons, idx=_primarykey, persons.id < 5\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test
  public void testSubstring() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select substring(socialsecuritynumber, 11, 12) as str from persons where id < 5");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getString("str"), "0");
    ret.next();
    assertEquals(ret.getString("str"), "1");
    ret.next();
    assertEquals(ret.getString("str"), "2");
    ret.next();
    assertEquals(ret.getString("str"), "3");
    ret.next();
    assertEquals(ret.getString("str"), "4");
    assertFalse(ret.next());
  }

  @Test
  public void testSubstring2Explain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select substring(socialsecuritynumber, 10) as str from persons where id < 5")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for relational op: table=persons, idx=_primarykey, persons.id < 5\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test
  public void testSubstring2() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select substring(socialsecuritynumber, 10) as str from persons where id < 5");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getString("str"), "-0");
    ret.next();
    assertEquals(ret.getString("str"), "-1");
    ret.next();
    assertEquals(ret.getString("str"), "-2");
    ret.next();
    assertEquals(ret.getString("str"), "-3");
    ret.next();
    assertEquals(ret.getString("str"), "-4");
    assertFalse(ret.next());
  }

  @Test
  public void testUpperExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select socialsecuritynumber, upper(socialsecuritynumber) as upper from persons where id < 5")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for relational op: table=persons, idx=_primarykey, persons.id < 5\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test
  public void testUpper() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select socialsecuritynumber, upper(socialsecuritynumber) as upper from persons where id < 5");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 0);
    assertEquals(ret.getString("upper"), "SSN-933-28-0");
    ret.next();
    assertEquals(ret.getLong("id"), 1);
    assertEquals(ret.getString("upper"), "SSN-933-28-1");
    ret.next();
    assertEquals(ret.getLong("id"), 2);
    assertEquals(ret.getString("upper"), "SSN-933-28-2");
    ret.next();
    assertEquals(ret.getLong("id"), 3);
    assertEquals(ret.getString("upper"), "SSN-933-28-3");
    ret.next();
    assertEquals(ret.getLong("id"), 4);
    assertEquals(ret.getString("upper"), "SSN-933-28-4");
    assertFalse(ret.next());
  }

  @Test
  public void testBetweenExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from persons where id between 1 and 5 order by id asc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Two key index lookup: table=persons, idx=_primarykey, id <= 5 and id >= 1\n");
      }
    }
  }

  @Test
  public void testBetween() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id between 1 and 5 order by id asc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 1);
    ret.next();
    assertEquals(ret.getLong("id"), 2);
    ret.next();
    assertEquals(ret.getLong("id"), 3);
    ret.next();
    assertEquals(ret.getLong("id"), 4);
    ret.next();
    assertEquals(ret.getLong("id"), 5);
    assertFalse(ret.next());
  }

  @Test
  public void testNotBetweenExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from persons where id not between 1 and 5 and id < 10")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for relational op: table=persons, idx=_primarykey, persons.id < 10\n" +
            "single key index lookup\n" +
            " AND \n" +
            "Read record from index and evaluate: persons.id >= 1 and persons.id <= 5\n");
      }
    }
  }

  @Test
  public void testNotBetween() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id not between 1 and 5 and id < 10");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 0);
    ret.next();
    assertEquals(ret.getLong("id"), 6);
    ret.next();
    assertEquals(ret.getLong("id"), 7);
    ret.next();
    assertEquals(ret.getLong("id"), 8);
    ret.next();
    assertEquals(ret.getLong("id"), 9);
    assertFalse(ret.next());
  }

  @Test
  public void testNotBetweenInExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from persons where id between 1 and 5 AND id not in (1,2)")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Two key index lookup: table=persons, idx=_primarykey, id <= 5 and id >= 1\n" +
            " AND \n" +
            "Read record from index and evaluate: persons.id not in (1, 2)\n");
      }
    }
  }

  @Test
  public void testNotBetweenIn() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id between 1 and 5 AND id not in (1,2)");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 3);
    ret.next();
    assertEquals(ret.getLong("id"), 4);
    ret.next();
    assertEquals(ret.getLong("id"), 5);
    assertFalse(ret.next());
  }

  @Test(enabled=false)
  public void testUpsert() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("upsert into persons (id) values (?)");
    stmt.setLong(1, 1000000L);
    stmt.executeQuery();
  }

  @Test(enabled=false)
  public void testUnion() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id from persons union select id from memberships");
    stmt.executeUpdate();
  }

  @Test
  public void testCeilingExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id, num from persons where ceiling(num) < 2.0")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Table scan: table=persons ceiling(persons.num) < 2.0\n");
      }
    }
  }

  @Test
  public void testCeiling() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, num from persons where ceiling(num) < 2.0");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id"), 0);
    rs.next();
    assertEquals(rs.getLong("id"), 1);
    rs.next();
    assertEquals(rs.getLong("id"), 2);
    assertFalse(rs.next());
  }

  @Test
  public void testLikeExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id, socialsecuritynumber from persons where socialsecuritynumber like '%3'")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Table scan: table=persons persons.socialsecuritynumber like %3\n");
      }
    }
  }

  @Test
  public void testLike() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, socialsecuritynumber from persons where socialsecuritynumber like '%3'");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 3);
    assertEquals(ret.getString("socialsecuritynumber"), "ssN-933-28-3");
    ret.next();
    assertEquals(ret.getLong("id"), 103);
    assertEquals(ret.getString("socialsecuritynumber"), "ssN-933-28-3");
    ret.next();
    assertEquals(ret.getLong("id"), 107);
    assertEquals(ret.getString("socialsecuritynumber"), "ssN-933-28-3");
    assertFalse(ret.next());
  }

  @Test
  public void testLikeAndExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id, socialsecuritynumber from persons where socialsecuritynumber like '%3' and id < 5")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Table scan: table=persons persons.id < 5 and persons.socialsecuritynumber like %3\n" +
            " AND \n" +
            "Read record from index and evaluate: persons.id < 5\n");
      }
    }
  }

  @Test
   public void testLikeAnd() throws SQLException {
     PreparedStatement stmt = conn.prepareStatement("select id, socialsecuritynumber from persons where socialsecuritynumber like '%3' and id < 5");
     ResultSet ret = stmt.executeQuery();

     ret.next();
     assertEquals(ret.getLong("id"), 3);
     assertEquals(ret.getString("socialsecuritynumber"), "ssN-933-28-3");
     assertFalse(ret.next());
   }

  @Test
  public void testLikeORExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id, socialsecuritynumber from persons where socialsecuritynumber like '%3' or id = 5 order by id desc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to 'or' expression\n" +
            "Table scan: table=persons persons.socialsecuritynumber like %3 or persons.id = 5\n" +
            " OR \n" +
            "Index lookup for relational op: table=persons, idx=_primarykey, persons.id = 5\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test
   public void testLikeOR() throws SQLException {
     PreparedStatement stmt = conn.prepareStatement("select id, socialsecuritynumber from persons where socialsecuritynumber like '%3' or id = 5 order by id desc");
     ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 107);
    assertEquals(ret.getString("socialsecuritynumber"), "ssN-933-28-3");
    ret.next();
    assertEquals(ret.getLong("id"), 103);
    assertEquals(ret.getString("socialsecuritynumber"), "ssN-933-28-3");
    ret.next();
    assertEquals(ret.getLong("id"), 5);
    assertEquals(ret.getString("socialsecuritynumber"), "ssN-933-28-5");
     ret.next();
     assertEquals(ret.getLong("id"), 3);
     assertEquals(ret.getString("socialsecuritynumber"), "ssN-933-28-3");
     assertFalse(ret.next());
   }

  @Test
  public void testNotLikeExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id, sociasecuritynumber from persons where socialsecuritynumber not like '%3' and id < 5")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for relational op: table=persons, idx=_primarykey, persons.id < 5\n" +
            "single key index lookup\n" +
            " AND \n" +
            "Read record from index and evaluate: persons.socialsecuritynumber like %3\n");
      }
    }
  }

  @Test
  public void testNotLike() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, sociasecuritynumber from persons where socialsecuritynumber not like '%3' and id < 5");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 0);
    assertEquals(ret.getString("socialsecuritynumber"), "ssN-933-28-0");
    ret.next();
    assertEquals(ret.getLong("id"), 1);
    assertEquals(ret.getString("socialsecuritynumber"), "ssN-933-28-1");
    ret.next();
    assertEquals(ret.getLong("id"), 2);
    assertEquals(ret.getString("socialsecuritynumber"), "ssN-933-28-2");
    ret.next();
    assertEquals(ret.getLong("id"), 4);
    assertEquals(ret.getString("socialsecuritynumber"), "ssN-933-28-4");
    assertFalse(ret.next());
  }

  @Test
  public void testColumnEqualsColumnExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from persons where id = id2 order by id desc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Table scan: table=persons persons.id = persons.id2\n");
      }
    }
  }

  @Test
  public void testColumnEqualsColumn() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id = id2 order by id desc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 1);
    ret.next();
    assertEquals(ret.getLong("id"), 0);
    assertFalse(ret.next());
  }

  @Test
  public void testColumnEqualsColumnAndExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from persons where id = id2 and id < 1 order by id desc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Table scan: table=persons persons.id < 1 and persons.id = persons.id2\n");
      }
    }
  }

  @Test
  public void testColumnEqualsColumnAnd() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id = id2 and id < 1 order by id desc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 0);
    assertFalse(ret.next());
  }

  @Test
  public void testColumnEqualsColumnAndInExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from persons where id = id2 and id in (0, 1) order by id desc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Table scan: table=persons persons.id = persons.id2 and persons.id in (0, 1)\n" +
            " AND \n" +
            "Read record from index and evaluate: persons.id in (0, 1)\n");
      }
    }
  }

  @Test(invocationCount = 1)
  public void testColumnEqualsColumnAndIn() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id = id2 and id in (0, 1) order by id desc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    System.out.println(ret.getLong("id"));
    assertEquals(ret.getLong("id"), 1);
    ret.next();
    System.out.println(ret.getLong("id"));
    assertEquals(ret.getLong("id"), 0);
    assertFalse(ret.next());
  }

  @Test
  public void testColumnEqualsColumnAndNotInExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from persons where id = id2 and id not in (0) order by id desc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Table scan: table=persons persons.id = persons.id2 and persons.id not in (0)\n" +
            " AND \n" +
            "Read record from index and evaluate: persons.id not in (0)\n");
      }
    }
  }

  @Test
  public void testColumnEqualsColumnAndNotIn() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id = id2 and id not in (0) order by id desc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 1);
    assertFalse(ret.next());
  }


  @Test
  public void testCountExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select count(*) from persons")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Count records, all shards: table=persons, column=null, expression=<all>\n");
      }
    }
  }

  @Test
  public void testCount() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select count(*) from persons");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getInt(1), 20);
  }

  @Test
  public void testCountAsExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select count(*) As personCount from persons")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Count records, all shards: table=persons, column=null, expression=<all>\n");
      }
    }
  }

  @Test
  public void testCountAs() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select count(*) As personCount from persons");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getInt(1), 20);
  }

  @Test
  public void testCount2NoKeySecondaryIndexExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select count(id2) from nokeysecondaryindex")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Count records, all shards: table=nokeysecondaryindex, column=id2, expression=<all>\n");
      }
    }
  }

  @Test
  public void testCount2NoKeySecondaryIndex() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select count(id2) from nokeysecondaryindex");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getInt(1), 10);
  }

  @Test
  public void testCount2NoKeyExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select count(id2) from nokey")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Count records, all shards: table=nokey, column=id2, expression=<all>\n");
      }
    }
  }

  @Test
  public void testCount2NoKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select count(id2) from nokey");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getInt(1), 20);
  }

  @Test
  public void testCount2Explain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select count(id2) from persons")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Count records, all shards: table=persons, column=id2, expression=<all>\n");
      }
    }
  }

  @Test
  public void testCount2() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select count(id2) from persons");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getInt(1), 14);
  }

  @Test
  public void testCount3NoKeySecondaryIndexExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select count(*) from nokeysecondaryindex where id2 = 0")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Table scan: table=nokeysecondaryindex nokeysecondaryindex.id2 = 0\n");
      }
    }
  }

  @Test
  public void testCount3NoKeySecondaryIndex() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select count(*) from nokeysecondaryindex where id2 = 0");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong(1), 1);
  }

  @Test
  public void testCount3NoKeyExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select count(*) from nokey where id2 = 0")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Table scan: table=nokey nokey.id2 = 0\n");
      }
    }
  }

  @Test
  public void testCount3NoKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select count(*) from nokey where id2 = 0");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong(1), 2);
  }

  @Test
  public void testCount3Explain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select count(*) from persons where id2 = 0")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Table scan: table=persons persons.id2 = 0\n");
      }
    }
  }

  @Test
  public void testCount3() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select count(*) from persons where id2 = 0");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong(1), 7);
  }

  @Test
  public void testTruncateTable() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("create table tvs (id BIGINT, make VARCHAR(1024), model VARCHAR(1024), PRIMARY KEY (id))");
    stmt.executeUpdate();

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into tvs (id, make, model) VALUES (?, ?, ?)");
      stmt.setLong(1, i);
      stmt.setString(2, "make-" + i);
      stmt.setString(3, "model-" + i);
      assertEquals(stmt.executeUpdate(), 1);
    }

    stmt = conn.prepareStatement("select count(*) from tvs");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong(1), recordCount);

    stmt = conn.prepareStatement("truncate table tvs");
    boolean bool = stmt.execute();

    stmt = conn.prepareStatement("select count(*) from tvs");
    ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong(1), 0);
  }

  @Test
  public void testUpdateSecondary() throws SQLException, UnsupportedEncodingException {
    PreparedStatement stmt = conn.prepareStatement("create table secondary_update (id BIGINT, make VARCHAR(1024), model VARCHAR(1024))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create index make_model on secondary_update(make, model)");
    stmt.executeUpdate();

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into secondary_update (id, make, model) VALUES (?, ?, ?)");
      stmt.setLong(1, i);
      stmt.setString(2, "make-" + i);
      stmt.setString(3, "model-" + i);
      assertEquals(stmt.executeUpdate(), 1);
    }

    stmt = conn.prepareStatement("update secondary_update set make=?, model=? where make=? and model=?");
    stmt.setString(1, "my-make");
    stmt.setString(2, "my-model");
    stmt.setString(3, "make-0");
    stmt.setString(4, "model-0");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("select * from secondary_update where make=?");
    stmt.setString(1, "my-make");
    ResultSet rs = stmt.executeQuery();

    assertTrue(rs.next());
    assertEquals(rs.getString("make"), "my-make");
    assertEquals(rs.getString("model"), "my-model");
    assertFalse(rs.next());

    Index index = dbServers[0].getIndices().get("test").getIndices().get("secondary_update").get("make_model");
    Object value = index.get(new Object[]{"make-0".getBytes("utf-8"), "model-0".getBytes("utf-8")});
    assertEquals(value, null);

    stmt = conn.prepareStatement("select * from secondary_update where make=?");
    stmt.setString(1, "make-0");
    rs = stmt.executeQuery();

    assertFalse(rs.next());
  }

  @Test
  public void testDeleteSecondary() throws SQLException, UnsupportedEncodingException, EOFException {
    PreparedStatement stmt = conn.prepareStatement("create table secondary_delete (id BIGINT, make VARCHAR(1024), model VARCHAR(1024))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create index make_model on secondary_delete(make, model)");
    stmt.executeUpdate();

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into secondary_delete (id, make, model) VALUES (?, ?, ?)");
      stmt.setLong(1, i);
      stmt.setString(2, "make-" + i);
      stmt.setString(3, "model-" + i);
      assertEquals(stmt.executeUpdate(), 1);
    }

    Index index = dbServers[0].getIndices().get("test").getIndices().get("secondary_delete").get("make_model");
    Object value = index.get(new Object[]{"make-0".getBytes("utf-8"), "model-0".getBytes("utf-8")});
    byte[][] keys = dbServers[0].getAddressMap().fromUnsafeToKeys(value);

    index = dbServers[0].getIndices().get("test").getIndices().get("secondary_delete").get("_primarykey");
    TableSchema tableSchema = dbServers[0].getCommon().getTables("test").get("secondary_delete");
    KeyRecord keyRecord = new KeyRecord(keys[0]);
    Object[] primaryKey = DatabaseCommon.deserializeKey(tableSchema, keyRecord.getPrimaryKey());
    value = index.get(primaryKey);
    assertNotNull(value);

    stmt = conn.prepareStatement("delete from secondary_delete where make=? and model=?");
    stmt.setString(1, "make-0");
    stmt.setString(2, "model-0");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("select * from secondary_delete where make=?");
    stmt.setString(1, "make-0");
    ResultSet rs = stmt.executeQuery();

    assertFalse(rs.next());

    index = dbServers[0].getIndices().get("test").getIndices().get("secondary_delete").get("make_model");
    value = index.get(new Object[]{"make-0".getBytes("utf-8")});
    assertEquals(value, null);

    index = dbServers[0].getIndices().get("test").getIndices().get("secondary_delete").get("_primarykey");
    primaryKey = DatabaseCommon.deserializeKey(tableSchema, keyRecord.getPrimaryKey());
    value = index.get(primaryKey);
    assertNull(value);
  }

  @Test
  public void testTableScanExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from persons where restricted='false'")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Table scan: table=persons persons.restricted = false\n");
      }
    }
  }

  @Test
  public void testTableScan() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where restricted='false'");
    ResultSet ret = stmt.executeQuery();

    for (int i = 0; i < recordCount; i++) {
      assertTrue(ret.next());
    }
  }

  @Test
  public void testColumnsInKeyExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id from persons where id = 2 order by id desc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for relational op: table=persons, idx=_primarykey, persons.id = 2\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test
  public void testColumnsInKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id from persons where id = 2 order by id desc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 2);
    assertFalse(ret.next());
  }

  @Test
  public void testColumnsInKeyRangeExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id from persons where id < 5 order by id asc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for relational op: table=persons, idx=_primarykey, persons.id < 5\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test
    public void testColumnsInKeyRange() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id from persons where id < 5 order by id asc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 0);
    ret.next();
    assertEquals(ret.getLong("id"), 1);
    ret.next();
    assertEquals(ret.getLong("id"), 2);
    ret.next();
    assertEquals(ret.getLong("id"), 3);
    ret.next();
    assertEquals(ret.getLong("id"), 4);
    assertFalse(ret.next());
  }

  @Test
  public void testColumnsInKeyRangeTwoKeyExplain() throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id from persons where id < 5 and id > 1 order by id asc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Two key index lookup: table=persons, idx=_primarykey, id > 1 and id < 5\n");
      }
    }
  }

  @Test
  public void testColumnsInKeyRangeTwoKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id from persons where id < 5 and id > 1 order by id asc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 2);
    ret.next();
    assertEquals(ret.getLong("id"), 3);
    ret.next();
    assertEquals(ret.getLong("id"), 4);
    assertFalse(ret.next());
  }
}
