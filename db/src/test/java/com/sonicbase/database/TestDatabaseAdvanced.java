package com.sonicbase.database;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.server.DatabaseServer;
import com.sonicbase.util.JsonArray;
import com.sonicbase.util.JsonDict;
import com.sonicbase.util.StreamUtils;
import org.codehaus.plexus.util.FileUtils;
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

/**
 * Responsible for
 */
public class TestDatabaseAdvanced {

  private Connection conn;
  private int recordCount = 10;
  List<Long> ids = new ArrayList<>();

  @BeforeClass
  public void beforeClass() throws Exception {
    String configStr = StreamUtils.inputStreamToString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-4-servers.json")));
    final JsonDict config = new JsonDict(configStr);

    JsonArray array = config.putArray("licenseKeys");
    array.add(DatabaseServer.FOUR_SERVER_LICENSE);

    FileUtils.deleteDirectory(new File("/data/database"));

    DatabaseServer.getServers().clear();

    final DatabaseServer[] dbServers = new DatabaseServer[4];
    ThreadPoolExecutor executor = new ThreadPoolExecutor(32, 32, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

    String role = "primaryMaster";

    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < dbServers.length; i++) {
      final int shard = i;

      dbServers[shard] = new DatabaseServer();
      dbServers[shard].setConfig(config, "4-servers", "localhost", 9010 + (50 * shard), true, new AtomicBoolean(true), null, true);
      dbServers[shard].setRole(role);
      dbServers[shard].disableLogProcessor();
      dbServers[shard].setMinSizeForRepartition(0);
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


    PreparedStatement stmt = conn.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, id3 BIGINT, id4 BIGINT, num DOUBLE, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create table Memberships (personId BIGINT, membershipName VARCHAR(20), resortId BIGINT, PRIMARY KEY (personId, membershipName))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create table Resorts (resortId BIGINT, resortName VARCHAR(20), PRIMARY KEY (resortId))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create index socialSecurityNumber on persons(socialSecurityNumber)");
    stmt.executeUpdate();

    //test insert

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
      stmt = conn.prepareStatement("insert into persons (id, id2, id3, id4, num, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
      stmt.setLong(1, i);
      stmt.setLong(2, (i + 100) % 2);
      stmt.setLong(3, i + 100);
      stmt.setLong(4, (i + 100) % 3);
      stmt.setDouble(5, i / 100d);
      stmt.setString(6, "ssN-933-28-" + i);
      stmt.setString(7, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
      stmt.setBoolean(8, false);
      stmt.setString(9, "m");
      assertEquals(stmt.executeUpdate(), 1);
      ids.add((long) i);
    }

    for (int i = 4; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into persons (id, id3, id4, num, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
      stmt.setLong(1, i);
      stmt.setLong(2, i + 100);
      stmt.setLong(3, (i + 100) % 3);
      stmt.setDouble(4, i / 100d);
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
      stmt.setLong(3, i + 100);
      stmt.setLong(4, (i + 100) % 3);
      stmt.setDouble(5, i / 100d);
      stmt.setString(6, "ssN-933-28-" + (i % 4));
      stmt.setString(7, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
      stmt.setBoolean(8, false);
      stmt.setString(9, "m");
      int count = stmt.executeUpdate();
      assertEquals(count, 1);
      ids.add((long) (i + 100));
    }

    client.beginRebalance("test", "persons", "_1__primarykey");

    while (true) {
      if (client.isRepartitioningComplete("test")) {
        break;
      }
      Thread.sleep(1000);
    }

//    Thread.sleep(30000);

//    assertEquals(client.getPartitionSize("test", 0, "persons", "_1__primarykey"), 9);
//    assertEquals(client.getPartitionSize("test", 1, "persons", "_1__primarykey"), 11);

//    assertEquals(client.getPartitionSize(2, "persons", "_1__primarykey"), 9);
//    assertEquals(client.getPartitionSize(3, "persons", "_1__primarykey"), 8);

    executor.shutdownNow();
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
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertTrue(ret.wasNull());
    assertEquals(ret.getLong("id"), 4);
    //assertFalse(ret.next());

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
  public void testGroupByMax() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id2, max(id) as maxValue from persons group by id2");
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
  public void testGroupBySumDouble() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id2, sum(num) as sumValue from persons group by id2");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getDouble("sumValue"), 0.22000000000000003d);

    ret.next();
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getDouble("sumValue"), 0.29000000000000004d);
    assertFalse(ret.next());
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
  public void testGroupByMinWhereTablescan() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id2, min(id) as minValue from persons where id2 > 0 group by id2");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getLong("minValue"), 1);
    assertFalse(ret.next());
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
  public void testCountDistinct() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select count(distinct id2) as count from persons where id > 100 order by id asc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getInt("count"), 2);
    assertFalse(ret.next());

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
   public void testLikeAnd() throws SQLException {
     PreparedStatement stmt = conn.prepareStatement("select id, socialsecuritynumber from persons where socialsecuritynumber like '%3' and id < 5");
     ResultSet ret = stmt.executeQuery();

     ret.next();
     assertEquals(ret.getLong("id"), 3);
     assertEquals(ret.getString("socialsecuritynumber"), "ssN-933-28-3");
     assertFalse(ret.next());
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
  public void testColumnEqualsColumnAnd() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id = id2 and id < 1 order by id desc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 0);
    assertFalse(ret.next());
  }

  @Test
  public void testColumnEqualsColumnAndIn() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id = id2 and id in (0, 1) order by id desc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 1);
    ret.next();
    assertEquals(ret.getLong("id"), 0);
    assertFalse(ret.next());
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
  public void testCount() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select count(*) from persons");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getInt(1), 20);
  }

  @Test
  public void testCountAs() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select count(*) As personCount from persons");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getInt(1), 20);
  }

  @Test
  public void testCount2() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select count(id2) from persons");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getInt(1), 14);
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

}
