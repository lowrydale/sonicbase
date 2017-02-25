package com.lowryengineering.database;

import com.lowryengineering.database.client.DatabaseClient;
import com.lowryengineering.database.jdbcdriver.ConnectionProxy;
import com.lowryengineering.database.server.DatabaseServer;
import com.lowryengineering.database.util.JsonArray;
import com.lowryengineering.database.util.JsonDict;
import com.lowryengineering.database.util.StreamUtils;
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

public class TestJoins {

  private Connection conn;
  private int recordCount = 10;
  List<Long> ids = new ArrayList<>();


  @BeforeClass
  public void beforeClass() throws Exception {
    String configStr = StreamUtils.inputStreamToString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-4-servers.json")));
    final JsonDict config = new JsonDict(configStr);

    JsonArray array = config.getDict("database").putArray("licenseKeys");
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
      dbServers[shard].setConfig(config, "4-servers", "localhost", 9010 + (50 * shard), true, new AtomicBoolean(true));
      dbServers[shard].setRole(role);
      dbServers[shard].disableLogProcessor();
    }
    for (Future future : futures) {
      future.get();
    }

    for (DatabaseServer server : dbServers) {
      server.disableRepartitioner();
    }

    Class.forName("com.lowryengineering.database.jdbcdriver.DriverProxy");

    conn = DriverManager.getConnection("jdbc:dbproxy:127.0.0.1:9000", "user", "password");

    ((ConnectionProxy)conn).getDatabaseClient().createDatabase("test");

    conn.close();

    conn = DriverManager.getConnection("jdbc:dbproxy:127.0.0.1:9000/test", "user", "password");

    DatabaseClient client = ((ConnectionProxy)conn).getDatabaseClient();

    client.setPageSize(3);

    PreparedStatement stmt = conn.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create table Memberships (personId BIGINT, personId2 BIGINT, membershipName VARCHAR(20), resortId BIGINT, PRIMARY KEY (personId, membershipName))");
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
        stmt = conn.prepareStatement("insert into Memberships (personId, personId2, membershipName, resortId) VALUES (?, ?, ?, ?)");
        stmt.setLong(1, i);
        stmt.setLong(2, (i + 100) % 3);
        stmt.setString(3, "membership-" + j);
        stmt.setLong(4, new long[]{1000, 2000}[j % 2]);
        assertEquals(stmt.executeUpdate(), 1);
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

    client.beginRebalance("test", "persons", "_1__primarykey");

    while (true) {
      if (client.isRepartitioningComplete("test")) {
        break;
      }
      Thread.sleep(1000);
    }

    assertEquals(client.getPartitionSize("test", 0, "persons", "_1__primarykey"), 11);
    assertEquals(client.getPartitionSize("test", 1, "persons", "_1__primarykey"), 9);

//    assertEquals(client.getPartitionSize(2, "persons", "_1__primarykey"), 9);
//    assertEquals(client.getPartitionSize(3, "persons", "_1__primarykey"), 8);

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
        assertEquals(ret.getLong("id"), i);
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
