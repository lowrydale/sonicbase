package com.sonicbase.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.ComObject;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.query.impl.ColumnImpl;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.server.DatabaseServer;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

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


/**
 * Created by lowryda on 7/5/17.
 */
public class TestAWSBackup {

  private static Connection conn;
  private static int recordCount = 10;
  static List<Long> ids = new ArrayList<>();

  static DatabaseClient client = null;

  public static void main(String[] args) throws Exception {
    try {
      String configStr = IOUtils.toString(new BufferedInputStream(TestAWSBackup.class.getResourceAsStream("/config/config-4-servers.json")), "utf-8");
      ObjectMapper mapper = new ObjectMapper();
      final ObjectNode config = (ObjectNode) mapper.readTree(configStr);

      ArrayNode array = new ArrayNode(JsonNodeFactory.instance);
      array.add(DatabaseServer.FOUR_SERVER_LICENSE);
      config.put("licenseKeys", array);

      FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db"));

      DatabaseServer.getServers().clear();

      final DatabaseServer[] dbServers = new DatabaseServer[4];
      ThreadPoolExecutor executor = new ThreadPoolExecutor(32, 32, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

      String role = "primaryMaster";

      List<Future> futures = new ArrayList<>();
      for (int i = 0; i < dbServers.length; i++) {
        final int shard = i;
        //      futures.add(executor.submit(new Callable() {
        //        @Override
        //        public Object call() throws Exception {
        //          String role = "primaryMaster";

        dbServers[shard] = new DatabaseServer();
        dbServers[shard].setConfig(config, "4-servers", "localhost", 9010 + (50 * shard), true, new AtomicBoolean(true), null, true);
        dbServers[shard].setRole(role);
        dbServers[shard].disableLogProcessor();
        dbServers[shard].setMinSizeForRepartition(0);

        dbServers[shard].enableSnapshot(false);
        //          return null;
        //        }
        //      }));
      }
      for (Future future : futures) {
        future.get();
      }

      for (DatabaseServer server : dbServers) {
        server.shutdownRepartitioner();
      }

      //DatabaseClient client = new DatabaseClient("localhost", 9010, true);

      Class.forName("com.sonicbase.jdbcdriver.Driver");

      conn = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9000", "user", "password");

      ((ConnectionProxy) conn).getDatabaseClient().createDatabase("test");

      conn.close();

      conn = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9000/test", "user", "password");

      client = ((ConnectionProxy) conn).getDatabaseClient();

      client.setPageSize(3);

      PreparedStatement stmt = conn.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
      stmt.executeUpdate();

      stmt = conn.prepareStatement("create table Children (parent BIGINT, socialSecurityNumber VARCHAR(20), bio VARCHAR(256))");
      stmt.executeUpdate();

      stmt = conn.prepareStatement("create table Memberships (personId BIGINT, membershipName VARCHAR(20), resortId BIGINT, PRIMARY KEY (personId, membershipName))");
      stmt.executeUpdate();

      stmt = conn.prepareStatement("create table Resorts (resortId BIGINT, resortName VARCHAR(20), PRIMARY KEY (resortId))");
      stmt.executeUpdate();

      stmt = conn.prepareStatement("create table nokey (id BIGINT, id2 BIGINT)");
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

      for (int i = 0; i < recordCount; i++) {
        stmt = conn.prepareStatement("insert into children (parent, socialSecurityNumber, bio) VALUES (?, ?, ?)");
        stmt.setLong(1, i);
        stmt.setString(2, "933-28-" + i);
        stmt.setString(3, "xxxx yyyyyy zzzzzz xxxxx yyyy zzzzz xxxxxx yyyyyyy zzzzzzzz xxxxxxx yyyyyy");
        assertEquals(stmt.executeUpdate(), 1);
        ids.add((long) i);

        stmt = conn.prepareStatement("insert into children (parent, socialSecurityNumber, bio) VALUES (?, ?, ?)");
        stmt.setLong(1, i + 100);
        stmt.setString(2, "933-28-" + i);
        stmt.setString(3, "xxxx yyyyyy zzzzzz xxxxx yyyy zzzzz xxxxxx yyyyyyy zzzzzzzz xxxxxxx yyyyyy");
        assertEquals(stmt.executeUpdate(), 1);
        ids.add((long) i);
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

      stmt = conn.prepareStatement("create index socialSecurityNumber on persons(socialSecurityNumber)");
      stmt.executeUpdate();

      stmt = conn.prepareStatement("create index socialSecurityNumber on children(socialSecurityNumber)");
      stmt.executeUpdate();

      while (true) {
        ComObject cobj = new ComObject();
        cobj.put(ComObject.Tag.method, "areAllLongRunningCommandsComplete");

        byte[] bytes = ((ConnectionProxy) conn).getDatabaseClient().sendToMaster(cobj);
        if (new String(bytes).equals("true")) {
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

      long size = client.getPartitionSize("test", 0, "children", "_1_socialsecuritynumber");
      assertEquals(size, 10);

      client.beginRebalance("test", "persons", "_1__primarykey");


      while (true) {
        if (client.isRepartitioningComplete("test")) {
          break;
        }
        Thread.sleep(1000);
      }

      assertEquals(client.getPartitionSize("test", 0, "persons", "_1__primarykey"), 9);
      assertEquals(client.getPartitionSize("test", 1, "persons", "_1__primarykey"), 11);
      long count = client.getPartitionSize("test", 0, "children", "_1__primarykey");
      assertEquals(count, 9);
      count = client.getPartitionSize("test", 1, "children", "_1__primarykey");
      assertEquals(count, 11);
      count = client.getPartitionSize("test", 0, "children", "_1_socialsecuritynumber");
      assertEquals(count, 4);
      count = client.getPartitionSize("test", 1, "children", "_1_socialsecuritynumber");
      assertEquals(count, 6);

      dbServers[0].enableSnapshot(false);
      // dbServers[1].enableSnapshot(false);

      dbServers[0].runSnapshot();
//      dbServers[0].recoverFromSnapshot();
//      dbServers[0].getSnapshotManager().lockSnapshot("test");
//      dbServers[0].getSnapshotManager().unlockSnapshot(1);
//
//      long commandCount = dbServers[1].getCommandCount();
//      dbServers[1].purgeMemory();
//      dbServers[1].replayLogs();

      //    assertEquals(dbServers[1].getLogManager().getCountLogged(), commandCount);
      //    assertEquals(dbServers[1].getCommandCount(), commandCount * 2);

      //Thread.sleep(10000);

      ObjectNode backupConfig = (ObjectNode) mapper.readTree("{\n" +
          "    \"type\" : \"AWS\",\n" +
          "    \"bucket\": \"sonicbase-test-backup\",\n" +
          "    \"prefix\": \"backups\",\n" +
          "    \"period\": \"daily\",\n" +
          "    \"time\": \"23:00\",\n" +
          "    \"maxBackupCount\": 10\n" +
          "  }");

      for (DatabaseServer dbServer : dbServers) {
        dbServer.setBackupConfig(backupConfig);
      }

      System.out.println("Starting backup");
      client.startBackup();
      while (true) {
        Thread.sleep(1000);
        if (client.isBackupComplete()) {
          break;
        }
      }
      System.out.println("Finished backup");

      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.dbName, "__none__");
      cobj.put(ComObject.Tag.method, "getLastBackupDir");
      cobj.put(ComObject.Tag.schemaVersion, client.getCommon().getSchemaVersion());
      byte[] ret = client.send(null, 0, 0, cobj, DatabaseClient.Replica.master);
      ComObject retObj = new ComObject(ret);
      String dir = retObj.getString(ComObject.Tag.directory);


      File file = new File("/data/db-backup");
      File[] dirs = file.listFiles();

      System.out.println("Starting restore");
      client.startRestore(dir);
      while (true) {
        Thread.sleep(1000);
        if (client.isRestoreComplete()) {
          break;
        }
      }
      System.out.println("Finished restore");

      DatabaseServer.initDeathOverride(2, 2);
      DatabaseServer.deathOverride[0][0] = false;
      DatabaseServer.deathOverride[0][1] = false;
      DatabaseServer.deathOverride[1][0] = false;
      DatabaseServer.deathOverride[1][1] = false;
      Thread.sleep(20000);

      cobj = new ComObject();
      cobj.put(ComObject.Tag.dbName, "test");
      cobj.put(ComObject.Tag.schemaVersion, client.getCommon().getSchemaVersion());
      cobj.put(ComObject.Tag.method, "forceDeletes");
      client.sendToAllShards(null, 0, cobj, DatabaseClient.Replica.all);

      executor.shutdownNow();
    }
    catch (Exception e) {
      e.printStackTrace();
    }

    verifyData();
  }

  private static void assertEquals(int a, int b) throws Exception {
    if (a != b) {
      throw new Exception("Not equal, expected=" + b + ", actual=" + a);
    }
  }

  private static void assertEquals(long a, long b) throws Exception {
    if (a != b) {
      throw new Exception("Not equal, expected=" + b + ", actual=" + a);
    }
  }

  private static void verifyData() throws Exception {

    client.syncSchema();
    for (int i = 0; i < recordCount; i++) {
      PreparedStatement stmt = conn.prepareStatement("select * from persons where id=?");
      stmt.setLong(1, i);
      ResultSet rs = stmt.executeQuery();
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), i);

      stmt = conn.prepareStatement("select * from persons where id=?");
      stmt.setLong(1, i + 100);
      rs = stmt.executeQuery();
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), i + 100);
    }
    System.out.println("Passed - persons");


    for (int i = 0; i < recordCount; i++) {
      PreparedStatement stmt = conn.prepareStatement("select * from nokey where id=? and id2=?");
      stmt.setLong(1, i);
      stmt.setLong(2, i * 2);
      ResultSet rs = stmt.executeQuery();
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), i);
      assertEquals(rs.getLong("id2"), i * 2);
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), i);
      assertEquals(rs.getLong("id2"), i * 2);
    }
    System.out.println("Passed - nokey");

    for (int i = 0; i < recordCount; i++) {
      PreparedStatement stmt = conn.prepareStatement("select * from children where parent=?");
      stmt.setLong(1, i);
      ResultSet rs = stmt.executeQuery();
      assertTrue(rs.next());
      assertEquals(rs.getLong("parent"), i);

      stmt = conn.prepareStatement("select * from children where parent=?");
      stmt.setLong(1, i + 100);
      rs = stmt.executeQuery();
      assertTrue(rs.next());
      assertEquals(rs.getLong("parent"), i + 100);
    }
    System.out.println("Passed - children");

    System.out.println("Passed - all");
  }

  private static void assertTrue(boolean value) throws Exception {
    if (!value) {
      throw new Exception("Not true");
    }
  }

}
