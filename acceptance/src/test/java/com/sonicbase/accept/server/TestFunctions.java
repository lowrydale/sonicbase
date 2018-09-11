package com.sonicbase.accept.server;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.Config;
import com.sonicbase.jdbcdriver.ConnectionProxy;
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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestFunctions {

  private Connection conn;
  private final int recordCount = 10;
  final List<Long> ids = new ArrayList<>();
  com.sonicbase.server.DatabaseServer[] dbServers;

  @AfterClass(alwaysRun = true)
  public void afterClass() throws SQLException {
    conn.close();

    for (com.sonicbase.server.DatabaseServer server : dbServers) {
      server.shutdown();
    }

    System.out.println("client refCount=" + DatabaseClient.clientRefCount.get() + ", sharedClients=" + DatabaseClient.sharedClients.size() + ", class=TestFunctions");
    for (DatabaseClient client : DatabaseClient.allClients) {
      System.out.println("Stack:\n" + client.getAllocatedStack());
    }
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    System.setProperty("log4j.configuration", "test-log4j.xml");


    String configStr = IOUtils.toString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-4-servers.yaml")), "utf-8");
    Config config = new Config(configStr);

    FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db"));

    DatabaseClient.getServers().clear();

    dbServers = new com.sonicbase.server.DatabaseServer[4];
    ThreadPoolExecutor executor = new ThreadPoolExecutor(32, 32, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

    String role = "primaryMaster";

    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < dbServers.length; i++) {

      dbServers[i] = new com.sonicbase.server.DatabaseServer();
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

    conn = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9000", "user", "password");

    ((ConnectionProxy)conn).getDatabaseClient().createDatabase("test");

    conn.close();

    conn = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9000/test", "user", "password");



    DatabaseClient client = ((ConnectionProxy)conn).getDatabaseClient();


    PreparedStatement stmt = conn.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, id3 BIGINT, id4 BIGINT, id5 BIGINT, num DOUBLE, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), timestamp TIMESTAMP, PRIMARY KEY (id))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create table Memberships (personId BIGINT, membershipName VARCHAR(20), resortId BIGINT, PRIMARY KEY (personId, membershipName))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create table Resorts (resortId BIGINT, resortName VARCHAR(20), PRIMARY KEY (resortId))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create index socialSecurityNumber on persons(socialSecurityNumber)");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create table nokey (id BIGINT, id2 BIGINT)");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create table nokeysecondaryindex (id BIGINT, id2 BIGINT)");
    stmt.executeUpdate();

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
      for (int j = 0; j < recordCount; j++) {
        stmt = conn.prepareStatement("insert into Memberships (personId, membershipName, resortId) VALUES (?, ?, ?)");
        stmt.setLong(1, i);
        stmt.setString(2, "membership-" + j);
        stmt.setLong(3, new long[]{1000, 2000}[j % 2]);
        assertEquals(stmt.executeUpdate(), 1);
      }
    }

    for (int i = 0; i < 4; i++) {
      stmt = conn.prepareStatement("insert into persons (id, id2, id3, id4, id5, num, socialSecurityNumber, relatives, restricted, gender, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
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
      assertEquals(stmt.executeUpdate(), 1);
      ids.add((long) i);
    }

    for (int i = 4; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into persons (id, id3, id4, num, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
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
  public void testCeiling() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where ceiling(num) = 2");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id"), 3);
    rs.next();
    assertEquals(rs.getLong("id"), 4);
    assertFalse(rs.next());
  }

  @Test
  public void testFloor() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where floor(num) = 1");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id"), 2);
    rs.next();
    assertEquals(rs.getLong("id"), 3);
    assertFalse(rs.next());
  }

  @Test
  public void testAbs() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where abs(id3) = 100");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id3"), -100);
    rs.next();
    assertEquals(rs.getLong("id3"), -100);
    assertFalse(rs.next());
  }

  @Test
  public void testAvg() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where avg(id, id2, id3) = -50 and id < 7");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id"), 4);
    rs.next();
    assertEquals(rs.getLong("id"), 5);
    rs.next();
    assertEquals(rs.getLong("id"), 6);
    assertFalse(rs.next());
  }

  @Test
  public void testMin() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where min(id, 5, id3) = -102");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id"), 2);
    rs.next();
    assertEquals(rs.getLong("id"), 102);
    assertFalse(rs.next());
  }

  @Test
  public void testMax() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where max(id, 5, id3) = 102");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id"), 102);
    assertFalse(rs.next());
  }

  @Test
  public void testSum() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where sum(id, 5) = 5");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id"), 0);
    assertFalse(rs.next());
  }

  @Test
  public void testStr() {

  }

  @Test
  public void testMaxTimestamp() {

  }

  @Test
  public void testMinTimestamp() {

  }

  @Test
  public void testBitShiftLeft() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where bit_shift_left(id, 2) = 400");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id"), 100);
    assertFalse(rs.next());
  }

  @Test
  public void testBitShiftRight() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where bit_shift_right(id, 2) = 1");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id"), 4);
    rs.next();
    assertEquals(rs.getLong("id"), 5);
    rs.next();
    assertEquals(rs.getLong("id"), 6);
    rs.next();
    assertEquals(rs.getLong("id"), 7);
    assertFalse(rs.next());
  }

  @Test
  public void testBitAnd() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where bit_and(id, id2) = 1 and id < 4");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id"), 1);
    rs.next();
    assertEquals(rs.getLong("id"), 3);
    assertFalse(rs.next());
  }

  @Test
  public void testBitNot() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where bit_not(id) = -4");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id"), 3);
    assertFalse(rs.next());
  }

  @Test
  public void testBitOr() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where bit_or(id, id2) = 3");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id"), 3);
    assertFalse(rs.next());
  }

  @Test
  public void testBitXOr() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where bit_xor(id, id2) = 2");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id"), 2);
    rs.next();
    assertEquals(rs.getLong("id"), 3);
    assertFalse(rs.next());
  }

  @Test
  public void testCoelesce() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where coalesce(id2, id) = 4");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id"), 4);
    assertFalse(rs.next());
  }

  @Test
  public void testCharLength() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id < 6 and char_length(socialSecurityNumber) = "  + ("ssN-933-28-".length() + 1));
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id"), 4);
    rs.next();
    assertEquals(rs.getLong("id"), 5);
    assertFalse(rs.next());
  }

  @Test
  public void testConcat() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where concat(socialSecurityNumber, str(id)) = 'ssN-933-28-44'");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id"), 4);
    assertFalse(rs.next());
  }

  @Test
  public void testNow() {

  }

  @Test
  public void testDateAdd() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where date_add(timestamp, 50000) = '2001-01-01 01:01:51'");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id"), 1);
    assertFalse(rs.next());
  }

  @Test
  public void testDay() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where day(timestamp) = 1");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id"), 1);
    assertFalse(rs.next());
  }

  @Test
  public void testDayOfWeek() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where day_of_week(timestamp) = 2");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id"), 1);
    rs.next();
    assertEquals(rs.getLong("id"), 3);
    assertFalse(rs.next());
  }

  @Test
  public void testDayOfYear() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where day_of_year(timestamp) = 1");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id"), 1);
    assertFalse(rs.next());
  }

  @Test
  public void testMinute() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where minute(timestamp) = 1");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id"), 1);
    assertFalse(rs.next());
  }

  @Test
  public void testMonth() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where month(timestamp) = 1");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id"), 2);
    assertFalse(rs.next());
  }

  @Test
  public void testSecond() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where second(timestamp) = 1");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id"), 1);
    assertFalse(rs.next());
  }

  @Test
  public void testHour() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where hour(timestamp) = 1");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id"), 1);
    assertFalse(rs.next());
  }

  @Test
  public void testWeekOfMonth() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where week_of_month(timestamp) = 1");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id"), 1);
    rs.next();
    assertEquals(rs.getLong("id"), 2);
    assertFalse(rs.next());
  }

  @Test
  public void testWeekOfYear() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where week_of_year(timestamp) = 1");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id"), 1);
    assertFalse(rs.next());
  }

  @Test
  public void testYear() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where year(timestamp) = 2001");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id"), 1);
    assertFalse(rs.next());
  }

  @Test
  public void testPower() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where power(id3 * -1, id) = 10404");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id"), 2);
    assertFalse(rs.next());
  }

  @Test
  public void testHex() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where hex(socialsecurityNumber) = '73734E2D3933332D32382D34'");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id"), 4);
    assertFalse(rs.next());
  }

  @Test
  public void testLog() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where log(id3 * -1) < 4.63 order by id3 asc");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id3"), -102);
    rs.next();
    assertEquals(rs.getLong("id3"), -102);
    rs.next();
    assertEquals(rs.getLong("id3"), -101);
    rs.next();
    assertEquals(rs.getLong("id3"), -101);
    rs.next();
    assertEquals(rs.getLong("id3"), -100);
    rs.next();
    assertEquals(rs.getLong("id3"), -100);
    assertFalse(rs.next());
  }

  @Test
  public void testLog10() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where log10(id3 * -1) < 2.009 order by id3 asc");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id3"), -102);
    rs.next();
    assertEquals(rs.getLong("id3"), -102);
    rs.next();
    assertEquals(rs.getLong("id3"), -101);
    rs.next();
    assertEquals(rs.getLong("id3"), -101);
    rs.next();
    assertEquals(rs.getLong("id3"), -100);
    rs.next();
    assertEquals(rs.getLong("id3"), -100);
    assertFalse(rs.next());
  }

  @Test
  public void testMod() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where mod(id, 100) = 1");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id"), 1);
    rs.next();
    assertEquals(rs.getLong("id"), 101);
    assertFalse(rs.next());
  }

  @Test
  public void testLower() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where lower(socialSecurityNumber) = 'ssn-933-28-0'");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id"), 100);
    rs.next();
    assertEquals(rs.getLong("id"), 104);
    rs.next();
    assertEquals(rs.getLong("id"), 108);
    assertFalse(rs.next());
  }

  @Test
  public void testUpper() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where upper(socialSecurityNumber) = 'SSN-933-28-0'");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id"), 100);
    rs.next();
    assertEquals(rs.getLong("id"), 104);
    rs.next();
    assertEquals(rs.getLong("id"), 108);
    assertFalse(rs.next());
  }

  @Test
  public void testPi() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id < pi()");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id"), 0);
    rs.next();
    assertEquals(rs.getLong("id"), 1);
    rs.next();
    assertEquals(rs.getLong("id"), 2);
    rs.next();
    assertEquals(rs.getLong("id"), 3);
    assertFalse(rs.next());
  }

  @Test
  public void testIndexOf() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where index_of(socialSecurityNumber, '28-0') = 8");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id"), 100);
    rs.next();
    assertEquals(rs.getLong("id"), 104);
    rs.next();
    assertEquals(rs.getLong("id"), 108);
    assertFalse(rs.next());
  }

  @Test
  public void testReplace() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where replace(lower(socialSecurityNumber), lower('SSN-933'), 'new_string') = 'new_string-28-0'");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id"), 100);
    rs.next();
    assertEquals(rs.getLong("id"), 104);
    rs.next();
    assertEquals(rs.getLong("id"), 108);
    assertFalse(rs.next());
  }

  @Test
  public void testRound() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where round(num) = 1");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id"), 1);
    rs.next();
    assertEquals(rs.getLong("id"), 2);
    assertFalse(rs.next());
  }

  @Test
  public void testSqrt() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where sqrt(id) = 2");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id"), 4);
    assertFalse(rs.next());
  }

  @Test
  public void testTan() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where tan(id) < -3.36 and id > 3 and id < 6");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id"), 5);
    assertFalse(rs.next());
  }

  @Test
  public void testCos() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where cos(id) < 0.28 and id > 3 and id < 6");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id"), 4);
    assertFalse(rs.next());
  }

  @Test
  public void testSin() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where sin(id) > -0.95 and id > 3 and id < 6");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id"), 4);
    assertFalse(rs.next());
  }

  @Test
  public void testCot() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where cot(id) > -0.3 and id > 3 and id < 6");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id"), 4);
    rs.next();
    assertEquals(rs.getLong("id"), 5);
    assertFalse(rs.next());
  }

  @Test
  public void testTrim() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where upper(trim(concat(socialsecuritynumber, '  '))) = 'SSN-933-28-4'");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id"), 4);
    assertFalse(rs.next());
  }

  @Test
  public void testRadians() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where radians(id) < 0.089 and id > 4");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id"), 5);
    assertFalse(rs.next());
  }

  @Test
  public void testIsNull() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where is_Null(id2) and id < 7");
    ResultSet rs = stmt.executeQuery();
    for (int i = 4; i < 7; i++) {
      rs.next();
      assertEquals(rs.getLong("id"), i);
    }
    assertFalse(rs.next());
  }

  @Test
  public void testNotNull() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where not(is_Null(id2)) and id < 7");
    ResultSet rs = stmt.executeQuery();
    for (int i = 0; i < 4; i++) {
      rs.next();
      assertEquals(rs.getLong("id"), i);
    }
    assertFalse(rs.next());
  }

  @Test
  public void testCustom() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where custom('com.sonicbase.accept.server.CustomFunctions', 'plus', id, id2) < 3");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id"), 0);
    rs.next();
    assertEquals(rs.getLong("id"), 1);
    rs.next();
    assertEquals(rs.getLong("id"), 2);
    assertFalse(rs.next());
  }

}
