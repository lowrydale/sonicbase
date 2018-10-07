package com.sonicbase.accept.database;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.ComObject;
import com.sonicbase.common.Config;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.query.impl.ColumnImpl;
import com.sonicbase.schema.IndexSchema;
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
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.testng.Assert.*;

public class TestFailover {

  private Connection conn;
  private final int recordCount = 10;
  final List<Long> ids = new ArrayList<>();

  DatabaseClient client = null;
  final DatabaseServer[][] dbServers = new DatabaseServer[2][];

  @AfterClass
  public void afterClass() throws SQLException {
    conn.close();

    for (DatabaseServer[] servers : dbServers) {
      for (DatabaseServer server : servers) {
        server.shutdown();
      }
    }

  }

  @BeforeClass
  public void beforeClass() {
    try {
      String configStr = IOUtils.toString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-4-servers.yaml")), "utf-8");
      Config config = new Config(configStr);

      FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db-data"));

      DatabaseClient.getServers().clear();

      ThreadPoolExecutor executor = new ThreadPoolExecutor(32, 32, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

      String role = "primaryMaster";

      dbServers[0] = new DatabaseServer[2];
      dbServers[1] = new DatabaseServer[2];

      List<Future> futures = new ArrayList<>();
      for (int i = 0; i < 4; i++) {
        int shard = i / 2;
        int replica = i % 2;

        dbServers[shard][replica] = new DatabaseServer();
        dbServers[shard][replica].setConfig(config, "4-servers", "localhost", 9010 + (50 * i), true, new AtomicBoolean(true), new AtomicBoolean(true),null);
        dbServers[shard][replica].setRole(role);
        //          return null;
        //        }
        //      }));
      }
      for (Future future : futures) {
        future.get();
      }

      for (DatabaseServer[] server : dbServers) {
        for (DatabaseServer replica : server) {
          replica.shutdownRepartitioner();
        }
      }

      //DatabaseClient client = new DatabaseClient("localhost", 9010, true);

      Class.forName("com.sonicbase.jdbcdriver.Driver");

      conn = DriverManager.getConnection("jdbc:sonicbase:localhost:9010,localhost:9060", "user", "password");

      ((ConnectionProxy) conn).getDatabaseClient().createDatabase("test");

      conn.close();

      conn = DriverManager.getConnection("jdbc:sonicbase:localhost:9010,localhost:9060/test", "user", "password");

      client = ((ConnectionProxy) conn).getDatabaseClient();

      client.setPageSize(3);

      PreparedStatement stmt = conn.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
      stmt.executeUpdate();

      stmt = conn.prepareStatement("create table Memberships (personId BIGINT, membershipName VARCHAR(20), resortId BIGINT, PRIMARY KEY (personId, membershipName))");
      stmt.executeUpdate();

      stmt = conn.prepareStatement("create table Resorts (resortId BIGINT, resortName VARCHAR(20), PRIMARY KEY (resortId))");
      stmt.executeUpdate();


      //test insertWithRecord

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

      for (int i = 0; i < recordCount; i++) {
        stmt = conn.prepareStatement("insert into persons (id, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?)");
        stmt.setLong(1, i);
        stmt.setString(2, "933-28-" + i);
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
        stmt.setString(3, "933-28-" + (i % 4));
        stmt.setString(4, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
        stmt.setBoolean(5, false);
        stmt.setString(6, "m");
        int count = stmt.executeUpdate();
        assertEquals(count, 1);
        ids.add((long) (i + 100));
      }

      //create index ssn2 on persons(socialSecurityNumber)
      //    stmt = conn.prepareStatement("create index ssn on persons(socialSecurityNumber)");
      //    stmt.executeUpdate();

      while (true) {
        ComObject cobj = new ComObject();
        cobj.put(ComObject.Tag.METHOD, "DatabaseServer:areAllLongRunningCommandsComplete");
        byte[] bytes = ((ConnectionProxy) conn).getDatabaseClient().sendToMaster(cobj);
        ComObject retObj = new ComObject(bytes);
        if (retObj.getBoolean(ComObject.Tag.IS_COMPLETE)) {
          break;
        }
        Thread.sleep(1000);
      }

      IndexSchema indexSchema = null;
      for (Map.Entry<String, IndexSchema> entry : client.getCommon().getTables("test").get("persons").getIndices().entrySet()) {
        if (entry.getValue().getFields()[0].equalsIgnoreCase("socialsecuritynumber")) {
          indexSchema = entry.getValue();
        }
      }
      List<ColumnImpl> columns = new ArrayList<>();
      columns.add(new ColumnImpl(null, null, "persons", "socialsecuritynumber", null));

      client.beginRebalance("test");

      while (true) {
        if (client.isRepartitioningComplete("test")) {
          break;
        }
        Thread.sleep(1000);
      }

      assertEquals(client.getPartitionSize("test", 0, "persons", "_1__primarykey"), 9);
      assertEquals(client.getPartitionSize("test", 1, "persons", "_1__primarykey"), 11);

      executor.shutdownNow();
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testInsert() throws SQLException, InterruptedException {
    Thread.sleep(4000);

    DatabaseServer.initDeathOverride(2, 2);
    DatabaseServer.deathOverride[1][1] = true;
    try {
      Thread.sleep(20_000);

      client.syncSchema();
      assertEquals(client.getCommon().getServersConfig().getShards()[1].getMasterReplica(), 0);
      assertTrue(client.getCommon().getServersConfig().getShards()[1].getReplicas()[1].isDead());
      assertFalse(client.getCommon().getServersConfig().getShards()[1].getReplicas()[0].isDead());

      PreparedStatement stmt = conn.prepareStatement("insert into persons (id, id2) VALUES (?, ?)");
      stmt.setLong(1, 2000);
      stmt.setLong(2, (2000) % 2);
      stmt.executeUpdate();

      for (int i = 0; i < 10; i++) {
        stmt = conn.prepareStatement("select * from persons where id=?");
        stmt.setLong(1, 2000);
        ResultSet ret = stmt.executeQuery();
        assertTrue(ret.next());
        assertEquals(ret.getLong("id"), 2000);
      }

      assertTrue(dbServers[1][0].getLogManager().hasLogsForPeer(1));
    }
    finally {
      DatabaseServer.deathOverride[1][1] = false;
    }

    Thread.sleep(1000);

    assertFalse(dbServers[1][0].getLogManager().hasLogsForPeer(1));
    try {
      assertNotNull(dbServers[1][1].getIndices().get("test").getIndices().get("persons").get("_1__primarykey").get(new Object[]{2000L}));
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testSpecifiedServer() throws InterruptedException {
    Thread.sleep(4000);

    DatabaseServer.initDeathOverride(2, 2);
    DatabaseServer.deathOverride[1][1] = true;
    try {
      Thread.sleep(4000);

      client.syncSchema();
      assertEquals(client.getCommon().getServersConfig().getShards()[1].getMasterReplica(), 0);
      assertTrue(client.getCommon().getServersConfig().getShards()[1].getReplicas()[1].isDead());
      assertFalse(client.getCommon().getServersConfig().getShards()[1].getReplicas()[0].isDead());

      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.DB_NAME, "__none__");
      cobj.put(ComObject.Tag.SCHEMA_VERSION, client.getCommon().getSchemaVersion());
      cobj.put(ComObject.Tag.METHOD, "testWrite");
      client.send(null, 1, 1, cobj, DatabaseClient.Replica.SPECIFIED);

      assertTrue(dbServers[1][0].getLogManager().hasLogsForPeer(1));

      //assertEquals(dbServers[1][1].getTestWriteCallCount(), 0);
    }
    finally {
      DatabaseServer.deathOverride[1][1] = false;
    }

    Thread.sleep(4000);

    assertFalse(dbServers[1][0].getLogManager().hasLogsForPeer(1));

    //assertEquals(dbServers[1][1].getTestWriteCallCount(), 1);
  }

  @Test(enabled=false)
  public void testAllDead() throws InterruptedException {
    Thread.sleep(4000);

    DatabaseServer.initDeathOverride(2, 2);
    DatabaseServer.deathOverride[1][1] = true;
    DatabaseServer.deathOverride[1][0] = true;
    try {
      Thread.sleep(8000);

      client.syncSchema();
      assertEquals(client.getCommon().getServersConfig().getShards()[1].getMasterReplica(), 0);
      assertTrue(client.getCommon().getServersConfig().getShards()[1].getReplicas()[1].isDead());
      assertTrue(client.getCommon().getServersConfig().getShards()[1].getReplicas()[0].isDead());

      try {
        PreparedStatement stmt = conn.prepareStatement("insert into persons (id, id2) VALUES (?, ?)");
        stmt.setLong(1, 8000);
        stmt.setLong(2, (8000) % 2);
        stmt.executeUpdate();
        fail();
      }
      catch (Exception e) {
        //expected
      }
    }
    finally {
      DatabaseServer.deathOverride[1][1] = false;
      DatabaseServer.deathOverride[1][0] = false;
    }

    Thread.sleep(4000);

    assertFalse(dbServers[1][0].getLogManager().hasLogsForPeer(1));
    assertFalse(dbServers[1][1].getLogManager().hasLogsForPeer(0));

  }

  @Test
  public void testMasterDeath() throws SQLException, InterruptedException {
    Thread.sleep(4000);

    DatabaseServer.initDeathOverride(2, 2);
    DatabaseServer.deathOverride[1][0] = true;
    try {
      Thread.sleep(4000);

      client.syncSchema();
      assertEquals(client.getCommon().getServersConfig().getShards()[1].getMasterReplica(), 1);
      assertTrue(client.getCommon().getServersConfig().getShards()[1].getReplicas()[0].isDead());
      assertFalse(client.getCommon().getServersConfig().getShards()[1].getReplicas()[1].isDead());

      PreparedStatement stmt = conn.prepareStatement("insert into persons (id, id2) VALUES (?, ?)");
      stmt.setLong(1, 4000);
      stmt.setLong(2, (4000) % 2);
      stmt.executeUpdate();

      assertNotNull(dbServers[1][1].getIndices().get("test").getIndices().get("persons").get("_1__primarykey").get(new Object[]{4000L}));

      Thread.sleep(1000);

      assertTrue(dbServers[1][1].getLogManager().hasLogsForPeer(0));
    }
    finally {
      DatabaseServer.deathOverride[1][0] = false;
    }

    Thread.sleep(1000);

    assertFalse(dbServers[1][1].getLogManager().hasLogsForPeer(0));
    try {
      assertNotNull(dbServers[1][0].getIndices().get("test").getIndices().get("persons").get("_1__primarykey").get(new Object[]{4000L}));
    }
    catch (Exception e) {
      e.printStackTrace();
    }

  }

  @Test
  public void testMainMasterDeath() throws InterruptedException, SQLException {
    Thread.sleep(4000);
    DatabaseServer.initDeathOverride(2, 2);
    DatabaseServer.deathOverride[0][0] = true;
    try {
      Thread.sleep(4000);

      client.syncSchema();
      assertEquals(client.getCommon().getServersConfig().getShards()[0].getMasterReplica(), 1);
      assertTrue(client.getCommon().getServersConfig().getShards()[0].getReplicas()[0].isDead());
      assertFalse(client.getCommon().getServersConfig().getShards()[0].getReplicas()[1].isDead());

      Thread.sleep(2000);
      assertNull(dbServers[0][0].getDeathMonitorThreads());
      assertNotNull(dbServers[0][1].getDeathMonitorThreads());

      PreparedStatement stmt = conn.prepareStatement("insert into persons (id, id2) VALUES (?, ?)");
      stmt.setLong(1, -6000);
      stmt.setLong(2, (-6000) % 2);
      stmt.executeUpdate();

      Thread.sleep(2000);
      DatabaseServer.deathOverride[1][0] = true;
      Thread.sleep(2000);
      client.syncSchema();
      assertTrue(client.getCommon().getServersConfig().getShards()[1].getReplicas()[0].isDead());
      DatabaseServer.deathOverride[1][0] = false;

      assertTrue(dbServers[0][1].getLogManager().hasLogsForPeer(0));
    }
    finally {
      DatabaseServer.deathOverride[0][0] = false;
    }

    Thread.sleep(2000);

    assertFalse(dbServers[0][1].getLogManager().hasLogsForPeer(0));
    try {
      assertNotNull(dbServers[0][0].getIndices().get("test").getIndices().get("persons").get("_1__primarykey").get(new Object[]{-6000L}));
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }

}
