package com.sonicbase.accept.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.ComObject;
import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.common.Logger;
import com.sonicbase.index.Index;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.server.DatabaseServer;
import com.sonicbase.streams.LocalProducer;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.testng.Assert.*;

public class TestJoins {

  private Connection conn;
  private int recordCount = 10;
  List<Long> ids = new ArrayList<>();
  DatabaseClient client;

  com.sonicbase.server.DatabaseServer[] dbServers;

  @AfterClass(alwaysRun = true)
  public void afterClass() throws SQLException {
    conn.close();
    for (com.sonicbase.server.DatabaseServer server : dbServers) {
      server.shutdown();
    }
    Logger.queue.clear();
    System.out.println("client refCount=" + DatabaseClient.clientRefCount.get() + ", sharedClients=" + DatabaseClient.sharedClients.size() + ", class=TestJoins");
    for (DatabaseClient client : DatabaseClient.allClients) {
      System.out.println("Stack:\n" + client.getAllocatedStack());
    }

  }

  @BeforeClass
  public void beforeClass() throws Exception {
    Logger.disable();

    String configStr = IOUtils.toString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-4-servers.json")), "utf-8");
    ObjectMapper mapper = new ObjectMapper();
    final ObjectNode config = (ObjectNode) mapper.readTree(configStr);

    FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db"));

    ArrayNode array = new ArrayNode(JsonNodeFactory.instance);
    array.add(com.sonicbase.server.DatabaseServer.FOUR_SERVER_LICENSE);
    config.put("licenseKeys", array);

    DatabaseClient.getServers().clear();

    dbServers = new com.sonicbase.server.DatabaseServer[4];
    ThreadPoolExecutor executor = new ThreadPoolExecutor(32, 32, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

    String role = "primaryMaster";

    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < dbServers.length; i++) {
      final int shard = i;
      dbServers[shard] = new com.sonicbase.server.DatabaseServer();
      dbServers[shard].setConfig(config, "4-servers", "localhost", 9010 + (50 * shard), true, new AtomicBoolean(true), new AtomicBoolean(true),null, true);
      dbServers[shard].overrideProLicense();
      dbServers[shard].setRole(role);
      dbServers[shard].disableLogProcessor();
      dbServers[shard].setMinSizeForRepartition(0);
    }
    for (Future future : futures) {
      future.get();
    }

    com.sonicbase.server.DatabaseServer.initDeathOverride(2, 2);
    com.sonicbase.server.DatabaseServer.deathOverride[0][0] = false;
    com.sonicbase.server.DatabaseServer.deathOverride[0][1] = false;
    com.sonicbase.server.DatabaseServer.deathOverride[1][0] = false;
    com.sonicbase.server.DatabaseServer.deathOverride[1][1] = false;


    for (com.sonicbase.server.DatabaseServer server : dbServers) {
      server.shutdownRepartitioner();
    }

    Thread.sleep(5000);

    Class.forName("com.sonicbase.jdbcdriver.Driver");

    conn = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9000", "user", "password");

    ((ConnectionProxy)conn).getDatabaseClient().createDatabase("test");

    conn.close();

    conn = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9000/test", "user", "password");

    client = ((ConnectionProxy)conn).getDatabaseClient();

    Logger.setReady(false);

    client.setPageSize(3);

    PreparedStatement stmt = conn.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create table Memberships (personId BIGINT, personId2 BIGINT, membershipName VARCHAR(20), resortId BIGINT, PRIMARY KEY (personId, membershipName))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create table Resorts (resortId BIGINT, resortName VARCHAR(20), PRIMARY KEY (resortId))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create index socialSecurityNumber on persons(socialSecurityNumber)");
    stmt.executeUpdate();

    //test upsert

    LocalProducer.queue.clear();


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
        stmt = conn.prepareStatement("insert into Memberships (personId, personId2, membershipName, resortId) VALUES (?, ?, ?, ?)");
        stmt.setLong(1, i);
        stmt.setLong(2, (i + 100) % 3);
        stmt.setString(3, "membership-" + j);
        stmt.setLong(4, new long[]{1000, 2000}[j % 2]);
        assertEquals(stmt.executeUpdate(), 1, "id=" + i);
      }
    }

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into persons (id, id2, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?)");
      stmt.setLong(1, i);
      stmt.setLong(2, (i + 100) % 2);
      stmt.setString(3, "933-28-" + i);
      stmt.setString(4, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
      stmt.setBoolean(5, false);
      stmt.setString(6, "m");
      assertEquals(stmt.executeUpdate(), 1);
      ids.add((long) i);
    }

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into persons (id, id2, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?)");
      stmt.setLong(1, i + 100);
      stmt.setLong(2, (i + 100) % 2);
      stmt.setString(3, "933-28-" + (i % 4));
      stmt.setString(4, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
      stmt.setBoolean(5, false);
      stmt.setString(6, "m");
      int count = stmt.executeUpdate();
      assertEquals(count, 1);
      ids.add((long) (i + 100));
    }

//    client.beginRebalance("test", "persons", "_1__primarykey");
//
//    while (true) {
//      if (client.isRepartitioningComplete("test")) {
//        break;
//      }
//      Thread.sleep(1000);
//    }
//
//    for (DatabaseServer server : dbServers) {
//      server.shutdownRepartitioner();
//    }
//
//    assertEquals(client.getPartitionSize("test", 0, "persons", "_1__primarykey"), 9);
//    assertEquals(client.getPartitionSize("test", 1, "persons", "_1__primarykey"), 11);

//    assertEquals(client.getPartitionSize(2, "persons", "_1__primarykey"), 9);
//    assertEquals(client.getPartitionSize(3, "persons", "_1__primarykey"), 8);

    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.dbName, "test");
    cobj.put(ComObject.Tag.schemaVersion, client.getCommon().getSchemaVersion());
    cobj.put(ComObject.Tag.method, "DeleteManager:forceDeletes");
    String command = "DatabaseServer:ComObject:forceDeletes:";
    //client.sendToAllShards(null, 0, command, cobj, DatabaseClient.Replica.all);

    executor.shutdownNow();
  }

  @Test
  public void testMemberships() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from memberships");
    ResultSet ret = stmt.executeQuery();
    assertTrue(ret.next());
    System.out.println(ret.getLong("personId"));
  }

  @Test
  public void testThreeWayJoin() throws SQLException {
    //fails
    PreparedStatement stmt = conn.prepareStatement("select id, socialsecuritynumber, memberships.personId, memberships.membershipname, resorts.resortname " +
        "from persons inner join Memberships on persons.id = Memberships.PersonId inner join resorts on memberships.resortid = resorts.resortId " +
        "where persons.id<2  order by persons.id desc, memberships.membershipname desc, resorts.resortname asc");

    ResultSet ret = stmt.executeQuery();

//    while (ret.next()) {
//     System.out.println("id=" + ret.getLong("id") + ", membname=" + ret.getString("membershipname") + ", resort=" + ret.getString("resortname"));
//   }
    for (int i = 1; i >= 0; i--) {
      for (int j = 9; j >= 0; j--) {
        ret.next();
        assertEquals(ret.getLong("id"), i);
        String value = ret.getString("membershipname");
        assertEquals(value, "membership-" + j);
        assertEquals(ret.getString("resortname"), j % 2 == 0 ? "resort-1000" : "resort-2000");
      }
    }
    assertFalse(ret.next());
   }

  @Test
  public void testFullOuterJoin() throws SQLException {
    //fails

    PreparedStatement stmt = conn.prepareStatement(
        "select persons.id, persons.socialsecuritynumber, memberships.personId, memberships.membershipname, memberships.personid from persons " +
            "full outer join Memberships on persons.id = Memberships.PersonId where persons.id<2  order by persons.id desc");
    ResultSet ret = stmt.executeQuery();

    for (int i = 109; i >= 100; i--) {
      ret.next();
      System.out.println(ret.getLong("id"));
      System.out.println(ret.getString("membershipname"));
      assertEquals(ret.getLong("id"), i);
      assertEquals(ret.getString("membershipname"), null);
    }

    for (int i = 9; i >= 0; i--) {
      for (int j = 0; j < 10; j++) {
        ret.next();
        assertEquals(ret.getLong("id"), i);
        assertEquals(ret.getLong("personid"), i, "id=" + i + ", memb=" + j);
        assertEquals(ret.getString("membershipname"), "membership-" + j, "id=" + i + ", memb=" + j);
      }
    }

  }

  @Test
  public void testRightOuterJoin() throws SQLException {
    //fails

    PreparedStatement stmt = conn.prepareStatement(
        "select persons.id, persons.socialsecuritynumber, memberships.personId, memberships.membershipname from persons " +
            "right outer join Memberships on persons.id = Memberships.PersonId where persons.id<2  order by persons.id desc");
    ResultSet ret = stmt.executeQuery();

    for (int i = 9; i >= 0; i--) {
      for (int j = 9; j >= 0; j--) {
        ret.next();
        assertEquals(ret.getLong("personid"), i);
        assertEquals(ret.getString("membershipname"), "membership-" + j);
        System.out.println(ret.getLong("personid"));
        System.out.println(ret.getString("membershipname"));
        if (i < 2) {
          assertEquals(ret.getLong("id"), i);
        }
      }
    }
    assertFalse(ret.next());
  }

  @Test
  public void testLeftOuterJoin() throws SQLException {
    //fails

    PreparedStatement stmt = conn.prepareStatement(
        "select persons.id, persons.socialsecuritynumber, memberships.membershipname, memberships.personid from persons " +
            "left outer join Memberships on persons.id = Memberships.PersonId where memberships.personid<2  order by memberships.personid desc");
    ResultSet ret = stmt.executeQuery();

//    while (ret.next()) {
//      System.out.println("id=" + ret.getLong("id") + ", personId=" + ret.getLong("personId") + ", membname=" + ret.getString("membershipname"));
//    }

    //todo: I don't think these results are right

    for (int i = 109; i >= 100; i--) {
      ret.next();
      assertEquals(ret.getLong("id"), i);
      assertEquals(ret.getLong("personid"), 0);
      assertTrue(ret.wasNull());
    }
    for (int i = 9; i >= 0; i--) {
      if (i < 2) {
        for (int j = 0; j < 10; j++) {
          ret.next();
          assertEquals(ret.getLong("id"), i);
          assertEquals(ret.getLong("personid"), i);
          assertEquals(ret.getString("membershipname"), "membership-" + j);
        }
      }
      else {
        ret.next();
        assertEquals(ret.getLong("id"), i);
      }
    }
    assertFalse(ret.next());
  }

  @Test
  public void testInnerJoin() throws SQLException {

    PreparedStatement stmt = conn.prepareStatement(
        "select persons.id, persons.socialsecuritynumber, memberships.membershipname from persons " +
            " inner join Memberships on persons.id = Memberships.PersonId where persons.id<5  order by persons.id desc");
    ResultSet ret = stmt.executeQuery();

    for (int i = 4; i >= 0; i--) {
      for (int j = 0; j < 10; j++) {
        ret.next();
        System.out.println("id=" + ret.getLong("id"));
        System.out.println("membershipName=" + ret.getString("membershipname"));
        assertEquals(ret.getLong("id"), i);
        assertEquals(ret.getString("membershipname"), "membership-" + j);
      }
    }
    assertFalse(ret.next());
  }

  @Test
  public void testInnerJoinString() throws SQLException, InterruptedException {

    PreparedStatement stmt = conn.prepareStatement("create table PersonsString (id VARCHAR, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create table MembershipsString (personId VARCHAR, personId2 BIGINT, membershipName VARCHAR(20), resortId BIGINT, PRIMARY KEY (personId, membershipName))");
    stmt.executeUpdate();

    for (int i = 0; i < recordCount; i++) {
      for (int j = 0; j < recordCount; j++) {
        stmt = conn.prepareStatement("insert into MembershipsString (personId, personId2, membershipName, resortId) VALUES (?, ?, ?, ?)");
        stmt.setString(1, String.valueOf(i));
        stmt.setLong(2, (i + 100) % 3);
        stmt.setString(3, "membership-" + j);
        stmt.setLong(4, new long[]{1000, 2000}[j % 2]);
        assertEquals(stmt.executeUpdate(), 1);
      }
    }

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into personsString (id, id2, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?)");
      stmt.setString(1, String.valueOf(i));
      stmt.setLong(2, (i + 100) % 2);
      stmt.setString(3, "933-28-" + i);
      stmt.setString(4, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
      stmt.setBoolean(5, false);
      stmt.setString(6, "m");
      assertEquals(stmt.executeUpdate(), 1);
      ids.add((long) i);
    }

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into personsString (id, id2, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?)");
      stmt.setString(1, String.valueOf(i + 100));
      stmt.setLong(2, (i + 100) % 2);
      stmt.setString(3, "933-28-" + (i % 4));
      stmt.setString(4, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
      stmt.setBoolean(5, false);
      stmt.setString(6, "m");
      int count = stmt.executeUpdate();
      assertEquals(count, 1);
      ids.add((long) (i + 100));
    }

    client.beginRebalance("test", "personsString", "_primarykey");

    while (true) {
      if (client.isRepartitioningComplete("test")) {
        break;
      }
      Thread.sleep(1000);
    }

    stmt = conn.prepareStatement(
            "select personsString.id, personsString.socialsecuritynumber, membershipsString.membershipname from personsString " +
                    " inner join MembershipsString on personsString.id = MembershipsString.PersonId where personsString.id<'5'  order by personsString.id desc");
    ResultSet ret = stmt.executeQuery();

    for (int i = 4; i >= 0; i--) {
      for (int j = 0; j < 10; j++) {
        ret.next();
        assertEquals(ret.getString("id"), String.valueOf(i));
        assertEquals(ret.getString("membershipname"), "membership-" + j);
      }
    }
    assertFalse(ret.next());
  }

  @Test
  public void testInnerJoinDouble() throws SQLException, InterruptedException {

    PreparedStatement stmt = conn.prepareStatement("create table PersonsDouble (id DOUBLE, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create table MembershipsDouble (personId DOUBLE, personId2 BIGINT, membershipName VARCHAR(20), resortId BIGINT, PRIMARY KEY (personId, membershipName))");
    stmt.executeUpdate();

    for (int i = 0; i < recordCount; i++) {
      for (int j = 0; j < recordCount; j++) {
        stmt = conn.prepareStatement("insert into MembershipsDouble (personId, personId2, membershipName, resortId) VALUES (?, ?, ?, ?)");
        stmt.setDouble(1, Double.valueOf(i));
        stmt.setLong(2, (i + 100) % 3);
        stmt.setString(3, "membership-" + j);
        stmt.setLong(4, new long[]{1000, 2000}[j % 2]);
        assertEquals(stmt.executeUpdate(), 1);
      }
    }

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into personsDouble (id, id2, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?)");
      stmt.setDouble(1, Double.valueOf(i));
      stmt.setLong(2, (i + 100) % 2);
      stmt.setString(3, "933-28-" + i);
      stmt.setString(4, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
      stmt.setBoolean(5, false);
      stmt.setString(6, "m");
      assertEquals(stmt.executeUpdate(), 1);
      ids.add((long) i);
    }

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into personsDouble (id, id2, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?)");
      stmt.setDouble(1, Double.valueOf(i + 100));
      stmt.setLong(2, (i + 100) % 2);
      stmt.setString(3, "933-28-" + (i % 4));
      stmt.setString(4, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
      stmt.setBoolean(5, false);
      stmt.setString(6, "m");
      int count = stmt.executeUpdate();
      assertEquals(count, 1);
      ids.add((long) (i + 100));
    }

    client.beginRebalance("test", "personsDouble", "_primarykey");

    while (true) {
      if (client.isRepartitioningComplete("test")) {
        break;
      }
      Thread.sleep(1000);
    }

//    client.beginRebalance("test", "membershipsdouble", "_2__primarykey");
//
//    while (true) {
//      if (client.isRepartitioningComplete("test")) {
//        break;
//      }
//      Thread.sleep(1000);
//    }

    com.sonicbase.server.DatabaseServer server = (com.sonicbase.server.DatabaseServer) DatabaseClient.getServers().get(0).get(0);
    Index index = server.getIndices().get("test").getIndices().get("membershipsdouble").get("_primarykey");

    Map.Entry<Object[], Object> entry = index.firstEntry();
    while (entry != null) {
      System.out.println(DatabaseCommon.keyToString(entry.getKey()));
      entry = index.higherEntry(entry.getKey());
    }

    System.out.println("memberships shard 1");
    server = (com.sonicbase.server.DatabaseServer) DatabaseClient.getServers().get(1).get(0);
    index = server.getIndices().get("test").getIndices().get("membershipsdouble").get("_primarykey");

    entry = index.firstEntry();
    while (entry != null) {
      System.out.println(DatabaseCommon.keyToString(entry.getKey()));
      entry = index.higherEntry(entry.getKey());
    }

    index = server.getIndices().get("test").getIndices().get("personsdouble").get("_primarykey");

    System.out.println("Persons");
    entry = index.firstEntry();
    while (entry != null) {
      System.out.println(DatabaseCommon.keyToString(entry.getKey()));
      entry = index.higherEntry(entry.getKey());
    }

    server = (DatabaseServer) DatabaseClient.getServers().get(1).get(0);
    index = server.getIndices().get("test").getIndices().get("persons").get("_primarykey");

    System.out.println("Persons - replica 1");
    entry = index.firstEntry();
    while (entry != null) {
      System.out.println(DatabaseCommon.keyToString(entry.getKey()));
      entry = index.higherEntry(entry.getKey());
    }

    stmt = conn.prepareStatement(
            "select personsDouble.id, personsDouble.socialsecuritynumber, membershipsDouble.membershipname from personsDouble " +
                    " inner join MembershipsDouble on personsDouble.id = MembershipsDouble.PersonId where personsDouble.id<'5'  order by personsDouble.id desc");
    ResultSet ret = stmt.executeQuery();

    for (int i = 4; i >= 0; i--) {
      for (int j = 0; j < 10; j++) {
        ret.next();
        assertEquals(ret.getDouble("id"), Double.valueOf(i));
        assertEquals(ret.getString("membershipname"), "membership-" + j);
        System.out.println(ret.getDouble("id"));
        System.out.println(ret.getString("membershipname"));
      }
    }
    assertFalse(ret.next());
  }

  @Test
  public void testInnerJoinNumeric() throws SQLException, InterruptedException {

    PreparedStatement stmt = conn.prepareStatement("create table PersonsNumeric (id NUMERIC, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create table MembershipsNumeric (personId NUMERIC, personId2 BIGINT, membershipName VARCHAR(20), resortId BIGINT, PRIMARY KEY (personId, membershipName))");
    stmt.executeUpdate();

    for (int i = 0; i < recordCount; i++) {
      for (int j = 0; j < recordCount; j++) {
        stmt = conn.prepareStatement("insert into MembershipsNumeric (personId, personId2, membershipName, resortId) VALUES (?, ?, ?, ?)");
        stmt.setBigDecimal(1, BigDecimal.valueOf(i));
        stmt.setLong(2, (i + 100) % 3);
        stmt.setString(3, "membership-" + j);
        stmt.setLong(4, new long[]{1000, 2000}[j % 2]);
        assertEquals(stmt.executeUpdate(), 1);
      }
    }

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into personsNumeric (id, id2, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?)");
      stmt.setBigDecimal(1, BigDecimal.valueOf(i));
      stmt.setLong(2, (i + 100) % 2);
      stmt.setString(3, "933-28-" + i);
      stmt.setString(4, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
      stmt.setBoolean(5, false);
      stmt.setString(6, "m");
      assertEquals(stmt.executeUpdate(), 1);
      ids.add((long) i);
    }

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into personsNumeric (id, id2, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?)");
      stmt.setBigDecimal(1, BigDecimal.valueOf(i + 100));
      stmt.setLong(2, (i + 100) % 2);
      stmt.setString(3, "933-28-" + (i % 4));
      stmt.setString(4, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
      stmt.setBoolean(5, false);
      stmt.setString(6, "m");
      int count = stmt.executeUpdate();
      assertEquals(count, 1);
      ids.add((long) (i + 100));
    }

    client.beginRebalance("test", "personsNumeric", "_primarykey");

    while (true) {
      if (client.isRepartitioningComplete("test")) {
        break;
      }
      Thread.sleep(1000);
    }

    stmt = conn.prepareStatement(
            "select personsNumeric.id, personsNumeric.socialsecuritynumber, membershipsNumeric.membershipname from personsNumeric " +
                    " inner join MembershipsNumeric on personsNumeric.id = MembershipsNumeric.PersonId where personsNumeric.id<'5'  order by personsNumeric.id desc");
    ResultSet ret = stmt.executeQuery();

    for (int i = 4; i >= 0; i--) {
      for (int j = 0; j < 10; j++) {
        ret.next();
        assertEquals(ret.getBigDecimal("id"), BigDecimal.valueOf(i));
        assertEquals(ret.getString("membershipname"), "membership-" + j);
      }
    }
    assertFalse(ret.next());
  }


  @Test
  public void testInnerJoinTimestamp() throws SQLException, InterruptedException {

    PreparedStatement stmt = conn.prepareStatement("create table PersonsTimestamp (id TIMESTAMP, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create table MembershipsTimestamp (personId TIMESTAMP, personId2 BIGINT, membershipName VARCHAR(20), resortId BIGINT, PRIMARY KEY (personId, membershipName))");
    stmt.executeUpdate();

    for (int i = 0; i < recordCount; i++) {
      for (int j = 0; j < recordCount; j++) {
        stmt = conn.prepareStatement("insert into MembershipsTimestamp (personId, personId2, membershipName, resortId) VALUES (?, ?, ?, ?)");
        stmt.setTimestamp(1, new Timestamp(i));
        stmt.setLong(2, (i + 100) % 3);
        stmt.setString(3, "membership-" + j);
        stmt.setLong(4, new long[]{1000, 2000}[j % 2]);
        assertEquals(stmt.executeUpdate(), 1);
      }
    }

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into personsTimestamp (id, id2, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?)");
      stmt.setTimestamp(1, new Timestamp(i));
      stmt.setLong(2, (i + 100) % 2);
      stmt.setString(3, "933-28-" + i);
      stmt.setString(4, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
      stmt.setBoolean(5, false);
      stmt.setString(6, "m");
      assertEquals(stmt.executeUpdate(), 1);
      ids.add((long) i);
    }

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into personsTimestamp (id, id2, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?)");
      stmt.setTimestamp(1, new Timestamp(i + 100));
      stmt.setLong(2, (i + 100) % 2);
      stmt.setString(3, "933-28-" + (i % 4));
      stmt.setString(4, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
      stmt.setBoolean(5, false);
      stmt.setString(6, "m");
      int count = stmt.executeUpdate();
      assertEquals(count, 1);
      ids.add((long) (i + 100));
    }

    client.beginRebalance("test", "personsTimestamp", "_primarykey");

    while (true) {
      if (client.isRepartitioningComplete("test")) {
        break;
      }
      Thread.sleep(1000);
    }

    stmt = conn.prepareStatement(
            "select personsTimestamp.id, personsTimestamp.socialsecuritynumber, membershipsTimestamp.membershipname from personsTimestamp " +
                    " inner join MembershipsTimestamp on personsTimestamp.id = MembershipsTimestamp.PersonId where personsTimestamp.id<5  order by personsTimestamp.id desc");
    ResultSet ret = stmt.executeQuery();

    for (int i = 4; i >= 0; i--) {
      for (int j = 0; j < 10; j++) {
        ret.next();
        assertEquals(ret.getTimestamp("id"), new Timestamp(i));
        assertEquals(ret.getString("membershipname"), "membership-" + j);
      }
    }
    assertFalse(ret.next());
  }

  @Test
   public void testInnerJoinMultiTableQuery() throws SQLException {

     PreparedStatement stmt = conn.prepareStatement(
         "select persons.id, persons.socialsecuritynumber, memberships.membershipname from persons " +
             " inner join Memberships on persons.id = Memberships.PersonId and memberships.personid < 2 where persons.id > 0 order by persons.id desc");
     ResultSet ret = stmt.executeQuery();

     for (int i = 1; i >= 1; i--) {
       for (int j = 0; j < 10; j++) {
         ret.next();
         assertEquals(ret.getLong("id"), i);
         assertEquals(ret.getString("membershipname"), "membership-" + j);
       }
     }

      assertFalse(ret.next());
   }

  @Test
   public void testInnerJoinMultiTableQueryChangeTableOrder() throws SQLException {

     PreparedStatement stmt = conn.prepareStatement(
         "select persons.id, persons.socialsecuritynumber, memberships.membershipname from persons " +
             " inner join Memberships on Memberships.PersonId = persons.id and memberships.personid < 2 where persons.id > 0 order by persons.id desc");
     ResultSet ret = stmt.executeQuery();

     for (int i = 1; i >= 1; i--) {
       for (int j = 0; j < 10; j++) {
         ret.next();
         assertEquals(ret.getLong("id"), i);
         assertEquals(ret.getString("membershipname"), "membership-" + j);
       }
     }

      assertFalse(ret.next());
   }

  @Test
     public void testInnerJoinMultiColumnJoinQuery() throws SQLException {

       PreparedStatement stmt = conn.prepareStatement(
           "select persons.id, persons.id2, persons.socialsecuritynumber, memberships.personId, memberships.personid2, memberships.membershipname from persons " +
               " inner join Memberships on persons.id = Memberships.PersonId and memberships.personid2 = persons.id2  where persons.id > 0 order by persons.id desc");
       ResultSet ret = stmt.executeQuery();

       for (int i = 9; i >= 0; i--) {
         for (int j = 0; j < 10; j++) {
           if ((i + 100) % 2 == (i + 100) % 3) {
             ret.next();
             System.out.println("id=" + ret.getLong("id") + ", id2=" + ret.getLong("id2") + ", personId=" + ret.getLong("personId") + ", personId2=" + ret.getLong("personID2"));
             assertEquals(ret.getLong("id"), i);
             assertEquals(ret.getString("membershipname"), "membership-" + j, "id=" + i + ", id2=" + ret.getLong("id2") + ", personId2=" + ret.getLong("personId2"));
           }
         }
       }

        assertFalse(ret.next());
     }
}
