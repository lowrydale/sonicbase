package com.sonicbase.bench;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.server.DatabaseServer;
import com.sonicbase.util.JsonDict;
import com.sonicbase.util.StreamUtils;
import org.codehaus.plexus.util.FileUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

/**
 * Responsible for
 */
public class TestDatabaseAdvancedToDo {

  private Connection conn;
  private int recordCount = 10;
  List<Long> ids = new ArrayList<>();

  @BeforeClass
  public void beforeClass() throws Exception {
    String configStr = StreamUtils.inputStreamToString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-4-servers.json")));
    final JsonDict config = new JsonDict(configStr);

    FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db"));

    DatabaseServer.getServers().clear();

    final DatabaseServer[] dbServers = new DatabaseServer[4];
    ThreadPoolExecutor executor = new ThreadPoolExecutor(32, 32, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

    String role = "primaryMaster";

    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < dbServers.length; i++) {
      final int shard = i;
      futures.add(executor.submit(new Callable() {
        @Override
        public Object call() throws Exception {
          String role = "primaryMaster";

          dbServers[shard] = new DatabaseServer();
          dbServers[shard].setConfig(config, "test", "localhost", 9010 + (50 * shard), true, new AtomicBoolean(true), null, true);
          dbServers[shard].setRole(role);
          dbServers[shard].disableLogProcessor();
          return null;
        }
      }));
    }
    for (Future future : futures) {
      future.get();
    }

    for (DatabaseServer server : dbServers) {
      server.shutdownRepartitioner();
    }

    DatabaseClient client = new DatabaseClient("localhost", 9010, -1, -1, true);

    Class.forName("com.sonicbase.jdbcdriver.Driver");

    conn = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9000", "user", "password");

    PreparedStatement stmt = conn.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
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
      stmt = conn.prepareStatement("insert into persons (id, id2, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?)");
      stmt.setLong(1, i);
      stmt.setLong(2, (i + 100) % 2);
      stmt.setString(3, "ssN-933-28-" + i);
      stmt.setString(4, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
      stmt.setBoolean(5, false);
      stmt.setString(6, "m");
      assertEquals(stmt.executeUpdate(), 1);
      ids.add((long) i);
    }

    for (int i = 4; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into persons (id, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?)");
      stmt.setLong(1, i);
      stmt.setString(2, "ssN-933-28-" + i);
      stmt.setString(3, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
      stmt.setBoolean(4, false);
      stmt.setString(5, "m");
      assertEquals(stmt.executeUpdate(), 1);
      ids.add((long) i);
    }

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into persons (id, id2, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?)");
      stmt.setLong(1, i + 100);
      stmt.setLong(2, (i + 100) % 2);
      stmt.setString(3, "ssN-933-28-" + (i % 4));
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

  @Test(enabled=false)
  public void testAlias3() throws SQLException {
//    PreparedStatement stmt = conn.prepareStatement("select p.id as i from persons as p where i < 2 order by i asc");
//    ResultSet ret = stmt.executeQuery();
//
//    ret.next();
//    assertEquals(ret.getLong("i"), 0);
//    ret.next();
//    assertEquals(ret.getLong("i"), 1);
    fail();
  }

  @Test(enabled=false)
  public void testAlterTable() throws SQLException {
//      PreparedStatement stmt = conn.prepareStatement("alter table x drop column y");
//      ResultSet ret = stmt.executeQuery();
//
//      ret.next();
//      assertEquals(ret.getLong("id"), 10000);
    fail();
  }

  @Test(enabled=false)
  public void testInsertFromSelect() throws SQLException {
//      PreparedStatement stmt = conn.prepareStatement("INSERT INTO db_name.dest_table (First,Last,Age) SELECT First,Last,Age from db_name.source_table");
//      ResultSet ret = stmt.executeQuery();
//
//      ret.next();
//      assertEquals(ret.getLong("id"), 10000);
    fail();
  }

  @Test(enabled=false)
  public void testCreateTableLike() throws SQLException {
//      PreparedStatement stmt = conn.prepareStatement("CREATE TABLE database_name.copy_name LIKE database_name.original_name");
//      ResultSet ret = stmt.executeQuery();
//
//      ret.next();
//      assertEquals(ret.getLong("id"), 10000);
    fail();
  }

  @Test(enabled=false)
  public void testCopyTable() throws SQLException {
//      PreparedStatement stmt = conn.prepareStatement("INSERT database_name.copy_name SELECT * FROM database_name.original_name");
//      ResultSet ret = stmt.executeQuery();
//
//      ret.next();
//      assertEquals(ret.getLong("id"), 10000);
    fail();
  }

  @Test(enabled=false)
  public void testChangeFieldDataType() throws SQLException {
//      PreparedStatement stmt = conn.prepareStatement("ALTER TABLE db_name.ziptable CHANGE latitude latitude double");
//      ResultSet ret = stmt.executeQuery();
//
//      ret.next();
//      assertEquals(ret.getLong("id"), 10000);
    fail();
  }

  @Test(enabled=false)
  public void testInsertColumn() throws SQLException {
//      PreparedStatement stmt = conn.prepareStatement("ALTER TABLE mybase.mytable ADD COLUMN Extra text AFTER Ordinary");
//      ResultSet ret = stmt.executeQuery();
//
//      ret.next();
//      assertEquals(ret.getLong("id"), 10000);
    fail();
  }
//
//  @Test(enabled=false)
//  public void testArithmetic() throws SQLException {
//     PreparedStatement stmt = conn.prepareStatement("select id, Discounts = ((OrderQty * UnitPrice) * UnitPriceDiscount) from persons where id > 5");
//     ResultSet ret = stmt.executeQuery();
//
//     ret.next();
//     assertEquals(ret.getLong("discounts"), 150);
//    fail();
//  }

  @Test(enabled=false)
  public void testDropIndex() throws SQLException {
//     PreparedStatement stmt = conn.prepareStatement("drop index developers.UniqueName");
//     ResultSet ret = stmt.executeQuery();
//
//     ret.next();
//     assertEquals(ret.getLong("discounts"), 150);
    fail();
  }

  @Test(enabled=false)
  public void testToDate() throws SQLException {
//      PreparedStatement stmt = conn.prepareStatement("select e1.\"Event_Name\", e1.\"Event_Date\", (sysdate) as \"CurDate\"\n" +
//          "  from \"events\" e1\n" +
//          "  where (e1.\"Event_Date\" < todate (\"11/19/2003 10:15:30 am\", \"MM/DD/YYYY hh:nn:ss ampm\"))\n");
//      ResultSet ret = stmt.executeQuery();
//
//      ret.next();
//      assertEquals(ret.getLong("discounts"), 150);
    fail();
  }

  @Test(enabled=false)
  public void testHaving() throws SQLException {
//      PreparedStatement stmt = conn.prepareStatement("select e1.\"VenueNo\", avg(e1.\"Ticket_price\") as \"avgTicket_price\"\n" +
//          "  from \"events\" e1\n" +
//          "  group by e1.\"VenueNo\"\n" +
//          "  having ((e1.\"Event_Date\" < now))");
//      ResultSet ret = stmt.executeQuery();
//
//      ret.next();
//      assertEquals(ret.getLong("discounts"), 150);
    fail();
  }

  @Test(enabled=false)
  public void testCase() throws SQLException {
//      PreparedStatement stmt = conn.prepareStatement("select e1.\"Event_Name\", \n" +
//          "  case e1.\"VenueNo\" \n" +
//          "  when 2 then \"Memorial Stadium\" \n" +
//          "  when 4 then \"Coliseum\" \n" +
//          "  else \"unknown venue: VenueNo=\" + e1.\"VenueNo\" \n" +
//          "  end as \"Venue\"\n" +
//          "  from \"events\" e1");
//      ResultSet ret = stmt.executeQuery();
//
//      ret.next();
//      assertEquals(ret.getLong("discounts"), 150);
    fail();
  }

  @Test(enabled=false)
  public void testDropTable() throws SQLException {
//       PreparedStatement stmt = conn.prepareStatement("drop table x");
//       ResultSet ret = stmt.executeQuery();
//
//       ret.next();
//       assertEquals(ret.getLong("id"), 10000);
    fail();
  }

  @Test(enabled=false)
  public void testUnion() throws SQLException {
//      PreparedStatement stmt = conn.prepareStatement("SELECT BusinessEntityID, JobTitle, HireDate, VacationHours, SickLeaveHours\n" +
//          "  FROM HumanResources.Employee AS e1\n" +
//          "  UNION\n" +
//          "  SELECT BusinessEntityID, JobTitle, HireDate, VacationHours, SickLeaveHours\n" +
//          "  FROM HumanResources.Employee AS e2");
//      ResultSet ret = stmt.executeQuery();
//
//      ret.next();
//      assertEquals(ret.getLong("id"), 10000);
    fail();
  }


}
