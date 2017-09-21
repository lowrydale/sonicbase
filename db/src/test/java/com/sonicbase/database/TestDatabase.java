package com.sonicbase.database;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.ComObject;
import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.query.BinaryExpression;
import com.sonicbase.query.impl.ColumnImpl;
import com.sonicbase.query.impl.ExpressionImpl;
import com.sonicbase.query.impl.SelectContextImpl;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.server.DatabaseServer;
import com.sonicbase.util.JsonArray;
import com.sonicbase.util.JsonDict;
import com.sonicbase.util.StreamUtils;
import com.sun.jersey.core.util.Base64;
import org.apache.commons.codec.binary.Hex;
import org.codehaus.plexus.util.FileUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.crypto.*;
import javax.crypto.spec.SecretKeySpec;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.security.InvalidKeyException;
import java.security.Key;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.testng.Assert.*;

public class TestDatabase {

  private Connection conn;
  private int recordCount = 10;
  List<Long> ids = new ArrayList<>();

  DatabaseClient client = null;

  @BeforeClass
  public void beforeClass() throws Exception {
    try {
      String configStr = StreamUtils.inputStreamToString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-4-servers.json")));
      final JsonDict config = new JsonDict(configStr);

      JsonArray array = config.putArray("licenseKeys");
      array.add(DatabaseServer.FOUR_SERVER_LICENSE);

      FileUtils.deleteDirectory(new File("/data/database"));
      FileUtils.deleteDirectory(new File("/data/db-backup"));

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
        //          return null;
        //        }
        //      }));
      }
      for (Future future : futures) {
        future.get();
      }

      DatabaseServer.initDeathOverride(2, 2);
      DatabaseServer.deathOverride[0][0] = false;
      DatabaseServer.deathOverride[0][1] = false;
      DatabaseServer.deathOverride[1][0] = false;
      DatabaseServer.deathOverride[1][1] = false;

      dbServers[0].enableSnapshot(false);
      dbServers[1].enableSnapshot(false);
      dbServers[2].enableSnapshot(false);
      dbServers[3].enableSnapshot(false);


      Thread.sleep(5000);

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

      stmt = conn.prepareStatement("create table nokeysecondaryindex (id BIGINT, id2 BIGINT)");
      stmt.executeUpdate();

      stmt = conn.prepareStatement("create index id on nokeysecondaryindex(id)");
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

      for (int i = 0; i < recordCount; i++) {
        stmt = conn.prepareStatement("insert into nokeysecondaryindex (id, id2) VALUES (?, ?)");
        stmt.setLong(1, i);
        stmt.setLong(2, i * 2);
        assertEquals(stmt.executeUpdate(), 1);
      }

      stmt = conn.prepareStatement("create index socialSecurityNumber on persons(socialSecurityNumber)");
      stmt.executeUpdate();

      stmt = conn.prepareStatement("create index socialSecurityNumber on children(socialSecurityNumber)");
      stmt.executeUpdate();

      //create index ssn2 on persons(socialSecurityNumber)
      //    stmt = conn.prepareStatement("create index ssn on persons(socialSecurityNumber)");
      //    stmt.executeUpdate();

      while (true) {
        ComObject cobj = new ComObject();
        cobj.put(ComObject.Tag.method, "areAllLongRunningCommandsComplete");
        byte[] bytes = ((ConnectionProxy) conn).getDatabaseClient().sendToMaster("DatabaseServer:ComObject:areAllLongRunningCommandsComplete:1:test", cobj);
        ComObject retObj = new ComObject(bytes);
        if (retObj.getBoolean(ComObject.Tag.isComplete)) {
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
  /*
      AtomicReference<String> usedIndex = new AtomicReference<String>();
      ExpressionImpl.RecordCache recordCache = new ExpressionImpl.RecordCache();
      ParameterHandler parms = new ParameterHandler();
      SelectContextImpl ret = ExpressionImpl.lookupIds(
            "test", client.getCommon(), client, 0,
            1000, client.getCommon().getTables("test").get("persons"), indexSchema, false, BinaryExpression.Operator.equal,
            null,
            null,
            new Object[]{"933-28-0".getBytes()}, parms, null, null,
        new Object[]{"933-28-0".getBytes()},
            null,
            columns, "socialsecuritynumber", 0, recordCache,
            usedIndex, false, client.getCommon().getSchemaVersion(), null, null, false);

      assertEquals(ret.getCurrKeys().length, 4);
  */


  //rebalance
      for (DatabaseServer server : dbServers) {
        server.shutdownRepartitioner();
      }

//      long size = client.getPartitionSize("test", 0, "children", "_1_socialsecuritynumber");
//      assertEquals(size, 10);

      client.beginRebalance("test", "persons", "_1__primarykey");


      while (true) {
        if (client.isRepartitioningComplete("test")) {
          break;
        }
        Thread.sleep(1000);
      }

      //Thread.sleep(60000);

//      assertTrue(client.getPartitionSize("test", 0, "persons", "_1__primarykey") >= 8);
//      assertTrue(client.getPartitionSize("test", 1, "persons", "_1__primarykey") <= 12);
//      long count = client.getPartitionSize("test", 0, "children", "_1__primarykey");
//      assertEquals(count, 8);
//      count = client.getPartitionSize("test", 1, "children", "_1__primarykey");
//      assertEquals(count, 12);
//      count = client.getPartitionSize("test", 0, "children", "_1_socialsecuritynumber");
//      assertEquals(count, 3);
//      count = client.getPartitionSize("test", 1, "children", "_1_socialsecuritynumber");
//      assertEquals(count, 7);
//      count = client.getPartitionSize("test", 0, "nokey", "_1__primarykey");
//      assertEquals(count, 8);
//      count = client.getPartitionSize("test", 1, "nokey", "_1__primarykey");
//      assertEquals(count, 12);
//      count = client.getPartitionSize("test", 0, "nokeysecondaryindex", "_1__primarykey");
//      assertEquals(count, 3);
//      count = client.getPartitionSize("test", 1, "nokeysecondaryindex", "_1__primarykey");
//      assertEquals(count, 7);
//      count = client.getPartitionSize("test", 0, "nokeysecondaryindex", "_1_id");
//      assertEquals(count, 3);
//      count = client.getPartitionSize("test", 1, "nokeysecondaryindex", "_1_id");
//      assertEquals(count, 7);

     // dbServers[1].enableSnapshot(false);

//      dbServers[0].runSnapshot();
//      dbServers[0].recoverFromSnapshot();
//      dbServers[0].getSnapshotManager().lockSnapshot("test");
//      dbServers[0].getSnapshotManager().unlockSnapshot(1);
//
//      long commandCount = dbServers[1].getCommandCount();
//      dbServers[2].purgeMemory();
//      dbServers[2].recoverFromSnapshot();
//      dbServers[2].replayLogs();
//      dbServers[3].purgeMemory();
//      dbServers[3].recoverFromSnapshot();
//      dbServers[3].replayLogs();

      //    assertEquals(dbServers[1].getLogManager().getCountLogged(), commandCount);
      //    assertEquals(dbServers[1].getCommandCount(), commandCount * 2);

      //Thread.sleep(10000);

//      JsonDict backupConfig = new JsonDict("{\n" +
//          "    \"type\" : \"AWS\",\n" +
//          "    \"bucket\": \"sonicbase-test-backup\",\n" +
//          "    \"prefix\": \"backups\",\n" +
//          "    \"period\": \"daily\",\n" +
//          "    \"time\": \"23:00\",\n" +
//          "    \"maxBackupCount\": 10\n" +
//          "  }");
//
//      for (DatabaseServer dbServer : dbServers) {
//        dbServer.setBackupConfig(backupConfig);
//      }
//
//      client.startBackup();
//      while (true) {
//        Thread.sleep(1000);
//        if (client.isBackupComplete()) {
//          break;
//        }
//      }
//
//      ComObject cobj = new ComObject();
//      cobj.put(ComObject.Tag.dbName, "__none__");
//      cobj.put(ComObject.Tag.method, "getLastBackupDir");
//      cobj.put(ComObject.Tag.schemaVersion, client.getCommon().getSchemaVersion());
//      String command = "DatabaseServer:ComObject:getLastBackupDir:";
//      byte[] ret = client.send(null, 0, 0, command, cobj.serialize(), DatabaseClient.Replica.master);
//      ComObject retObj = new ComObject(ret);
//      String dir = retObj.getString(ComObject.Tag.directory);
//
//
//      File file = new File("/data/db-backup");
//      File[] dirs = file.listFiles();
//
//      client.startRestore(dir);
//      while (true) {
//        Thread.sleep(1000);
//        if (client.isRestoreComplete()) {
//          break;
//        }
//      }

      JsonDict backupConfig = new JsonDict("{\n" +
          "    \"type\" : \"fileSystem\",\n" +
          "    \"directory\": \"/data/db-backup\",\n" +
          "    \"period\": \"daily\",\n" +
          "    \"time\": \"23:00\",\n" +
          "    \"maxBackupCount\": 10,\n" +
          "    \"sharedDirectory\": true\n" +
          "  }");

      for (DatabaseServer dbServer : dbServers) {
        dbServer.setBackupConfig(backupConfig);
      }

      client.startBackup();
      while (true) {
        Thread.sleep(1000);
        if (client.isBackupComplete()) {
          break;
        }
      }

      Thread.sleep(5000);

      File file = new File("/data/db-backup");
      File[] dirs = file.listFiles();

      client.startRestore(dirs[0].getName());
      while (true) {
        Thread.sleep(1000);
        if (client.isRestoreComplete()) {
          break;
        }
      }

//      Thread.sleep(10000);

      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.dbName, "test");
      cobj.put(ComObject.Tag.schemaVersion, client.getCommon().getSchemaVersion());
      cobj.put(ComObject.Tag.method, "forceDeletes");
      String command = "DatabaseServer:ComObject:forceDeletes:";
      client.sendToAllShards(null, 0, command, cobj, DatabaseClient.Replica.all);

     // Thread.sleep(10000);
      executor.shutdownNow();
    }
    catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  public void testSecondaryIndexWithNoKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from children where socialsecuritynumber = '933-28-4'");
    ResultSet ret = stmt.executeQuery();
    assertTrue(ret.next());
    assertEquals(ret.getString("socialSecurityNumber"), "933-28-4");

    stmt = conn.prepareStatement("select * from children where socialsecuritynumber >= '933-28-0' order by socialsecuritynumber asc");
    ret = stmt.executeQuery();
    assertTrue(ret.next());
    assertEquals(ret.getString("socialSecurityNumber"), "933-28-0");
    assertTrue(ret.next());
    assertEquals(ret.getString("socialSecurityNumber"), "933-28-0");
    assertTrue(ret.next());
    assertEquals(ret.getString("socialSecurityNumber"), "933-28-1");
    assertTrue(ret.next());
    assertEquals(ret.getString("socialSecurityNumber"), "933-28-1");
    assertTrue(ret.next());
    assertEquals(ret.getString("socialSecurityNumber"), "933-28-2");
    assertTrue(ret.next());
    assertEquals(ret.getString("socialSecurityNumber"), "933-28-2");
    assertTrue(ret.next());
    assertEquals(ret.getString("socialSecurityNumber"), "933-28-3");
    assertTrue(ret.next());
    assertEquals(ret.getString("socialSecurityNumber"), "933-28-3");
    assertTrue(ret.next());
    assertEquals(ret.getString("socialSecurityNumber"), "933-28-4");
    assertTrue(ret.next());
    assertEquals(ret.getString("socialSecurityNumber"), "933-28-4");
    assertTrue(ret.next());
    assertEquals(ret.getString("socialSecurityNumber"), "933-28-5");
    assertTrue(ret.next());
    assertEquals(ret.getString("socialSecurityNumber"), "933-28-5");
    assertTrue(ret.next());
    assertEquals(ret.getString("socialSecurityNumber"), "933-28-6");
    assertTrue(ret.next());
    assertEquals(ret.getString("socialSecurityNumber"), "933-28-6");
    assertTrue(ret.next());
    assertEquals(ret.getString("socialSecurityNumber"), "933-28-7");
    assertTrue(ret.next());
    assertEquals(ret.getString("socialSecurityNumber"), "933-28-7");
    assertTrue(ret.next());
    assertEquals(ret.getString("socialSecurityNumber"), "933-28-8");
    assertTrue(ret.next());
    assertEquals(ret.getString("socialSecurityNumber"), "933-28-8");
    assertTrue(ret.next());
    assertEquals(ret.getString("socialSecurityNumber"), "933-28-9");
    assertTrue(ret.next());
    assertEquals(ret.getString("socialSecurityNumber"), "933-28-9");
    assertFalse(ret.next());

  }

  @Test
  public void testDebug() {
    ((ConnectionProxy) conn).getDatabaseClient().debugRecord("test", "persons", "_1__primarykey", "[100]");
  }

  @Test(enabled = false)
  public void testMulti() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("create table Persons2 (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
    stmt.executeUpdate();

    for (int i = 0; i < 100000; i++) {
      stmt = conn.prepareStatement("insert into persons2 (id, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?)");
      stmt.setLong(1, i);
      stmt.setString(2, "933-28-" + i);
      stmt.setString(3, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
      stmt.setBoolean(4, false);
      stmt.setString(5, "m");
      assertEquals(stmt.executeUpdate(), 1);
      ids.add((long) i);

      stmt = conn.prepareStatement("select * from persons2 where id=" + i + " order by id asc");
      ResultSet ret = stmt.executeQuery();

      ret.next();
      assertEquals(ret.getLong("id"), i);
    }
    System.out.println("complete");
  }

  @Test
  public void testIndex() {
    ConcurrentSkipListMap<Long, Object> map = new ConcurrentSkipListMap<>();
    map.put((long) 6, 6);
    map.put((long) 7, 7);
    Object obj = map.ceilingEntry((long) 5);
    if (obj == null) {
      System.out.println("bad");
    }
    System.out.println(obj);

    obj = map.floorEntry((long) 5);
    System.out.println(obj);
  }

  @Test
  public void testDropIndex() throws SQLException, InterruptedException {

    PreparedStatement stmt = conn.prepareStatement("drop index persons.socialSecurityNumber");
     stmt.executeUpdate();

    IndexSchema indexSchema = null;
        for (Map.Entry<String, IndexSchema> entry : client.getCommon().getTables("test").get("persons").getIndices().entrySet()) {
          if (entry.getValue().getFields()[0].equalsIgnoreCase("socialsecuritynumber")) {
            indexSchema = entry.getValue();
            if (indexSchema.getName().contains("socialsecuritynumber")) {
              fail();
            }
          }
        }



    stmt = conn.prepareStatement("create index socialSecurityNumber on persons(socialSecurityNumber)");
     stmt.executeUpdate();

     Thread.sleep(5000);

    for (Map.Entry<String, IndexSchema> entry : client.getCommon().getTables("test").get("persons").getIndices().entrySet()) {
      if (entry.getValue().getFields()[0].equalsIgnoreCase("socialsecuritynumber")) {
        indexSchema = entry.getValue();
      }
    }

    List<ColumnImpl> columns = new ArrayList<>();
     columns.add(new ColumnImpl(null, null, "persons", "socialsecuritynumber", null));

     AtomicReference<String> usedIndex = new AtomicReference<String>();
     ExpressionImpl.RecordCache recordCache = new ExpressionImpl.RecordCache();
     ParameterHandler parms = new ParameterHandler();
     SelectContextImpl ret = ExpressionImpl.lookupIds(
           "test", client.getCommon(), client, 0,
           1000, "persons", indexSchema.getName(), false, BinaryExpression.Operator.equal,
           null,
           null,
           new Object[]{"933-28-0".getBytes()}, parms, null, null,
       new Object[]{"933-28-0".getBytes()},
           null,
           columns, "socialsecuritynumber", 0, recordCache,
           usedIndex, false, client.getCommon().getSchemaVersion(), null, null,
         false, new AtomicLong(), null, null);

     assertEquals(ret.getCurrKeys().length, 4);

  }

  @Test
  public void testUniqueIndex() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("create table indexes (id BIGINT, id2 BIGINT)");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create unique index uniqueIndex on indexes(id, id2)");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create unique index uniqueIndex on indexes(id)");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("insert into indexes (id, id2) VALUES (?, ?)");
    stmt.setLong(1, 1);
    stmt.setLong(2, 1);
    assertEquals(stmt.executeUpdate(), 1);

    stmt = conn.prepareStatement("insert into indexes (id, id2) VALUES (?, ?)");
    stmt.setLong(1, 1);
    stmt.setLong(2, 2);
    try {
      assertEquals(stmt.executeUpdate(), 0);
    }
    catch (SQLException e) {
      //expected
    }

    stmt = conn.prepareStatement("update indexes set id2=? where id=?");
    stmt.setLong(1, 2);
    stmt.setLong(2, 1);
    stmt.executeUpdate();
  }

    @Test
  public void testDualKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("create table dualKey (id BIGINT, id2 BIGINT, PRIMARY KEY (id, id2))");
    stmt.executeUpdate();

    for (int i = 0; i < 2; i++) {
      stmt = conn.prepareStatement("insert into dualKey (id, id2) VALUES (?, ?)");
      stmt.setLong(1, i);
      stmt.setLong(2, i * 2);
      assertEquals(stmt.executeUpdate(), 1);

      stmt = conn.prepareStatement("insert into dualKey (id, id2) VALUES (?, ?)");
      stmt.setLong(1, i);
      stmt.setLong(2, i * 4 + 1);
      assertEquals(stmt.executeUpdate(), 1);
    }

    stmt = conn.prepareStatement("select id, id2 from dualKey where id >= 0");
    ResultSet ret = stmt.executeQuery();
    ret.next();
    assertEquals(ret.getLong("id"), 0);
    assertEquals(ret.getLong("id2"), 0);
    ret.next();
    assertEquals(ret.getLong("id"), 0);
    assertEquals(ret.getLong("id2"), 1);
    ret.next();
    assertEquals(ret.getLong("id"), 1);
    assertEquals(ret.getLong("id2"), 2);
    ret.next();
    assertEquals(ret.getLong("id"), 1);
    assertEquals(ret.getLong("id2"), 5);
    assertFalse(ret.next());
  }

  @Test
  public void testAddColumn() throws SQLException {

    PreparedStatement stmt = conn.prepareStatement("create table addColumn (id BIGINT, id2 BIGINT, PRIMARY KEY (id))");
    stmt.executeUpdate();

    for (int i = 0; i < 10; i++) {
      stmt = conn.prepareStatement("insert into addColumn (id, id2) VALUES (?, ?)");
      stmt.setLong(1, i);
      stmt.setLong(2, i * 2);
      assertEquals(stmt.executeUpdate(), 1);
    }

    stmt = conn.prepareStatement("alter table addColumn add column id3 BIGINT");
    stmt.executeUpdate();

    for (int i = 10; i < 20; i++) {
      stmt = conn.prepareStatement("insert into addColumn (id, id2, id3) VALUES (?, ?, ?)");
      stmt.setLong(1, i);
      stmt.setLong(2, i * 2);
      stmt.setLong(3, i * 3);
      assertEquals(stmt.executeUpdate(), 1);
    }

    stmt = conn.prepareStatement("select id, id2, id3 from addColumn where id >= 0");
    ResultSet ret = stmt.executeQuery();
    for (int i = 0; i < 20; i++) {
      ret.next();
      assertEquals(ret.getLong("id"), i);
      if (i >= 10) {
        assertEquals(ret.getLong("id3"), i * 3);
      }
    }

    stmt = conn.prepareStatement("alter table addColumn drop column id3");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("select id, id2, id3 from addColumn where id >= 0");
    ret = stmt.executeQuery();
    for (int i = 0; i < 20; i++) {
      ret.next();
      assertEquals(ret.getLong("id"), i);
      if (i >= 10) {
        assertEquals(ret.getLong("id3"), 0);
      }
    }

    assertFalse(ret.next());


  }

  @Test
  public void testSchema() throws Exception {
    DatabaseCommon common = new DatabaseCommon();
    common.addDatabase("test");
    TableSchema tableSchema = new TableSchema();
    tableSchema.setName("table1");
    List<String> primaryKey = new ArrayList<>();
    primaryKey.add("id");
    tableSchema.setPrimaryKey(primaryKey);
    common.addTable("test", "/data/database", tableSchema);
    common.saveSchema("/data/database");

    common.getTables("test").clear();
    common.loadSchema("/data/database");

    assertEquals(common.getTables("test").size(), 1);
  }

  @Test
  public void testLess() throws Exception {

    PreparedStatement stmt = conn.prepareStatement("select * from persons where id<106 and id>100 order by id2 asc, id desc");
    ResultSet ret = stmt.executeQuery();
    assertTrue(ret.next());
    assertEquals(ret.getInt("id2"), 0);
    assertEquals(ret.getInt("id"), 104);
    assertTrue(ret.next());
    assertEquals(ret.getInt("id2"), 0);
    assertEquals(ret.getInt("id"), 102);
    assertTrue(ret.next());
    assertEquals(ret.getInt("id2"), 1);
    assertEquals(ret.getInt("id"), 105);
    assertTrue(ret.next());
    assertEquals(ret.getInt("id2"), 1);
    assertEquals(ret.getInt("id"), 103);
    assertTrue(ret.next());
    assertEquals(ret.getInt("id2"), 1);
    assertEquals(ret.getInt("id"), 101);
  }

  @Test
  public void testLessNoKeySecondaryIndex() throws Exception {

    PreparedStatement stmt = conn.prepareStatement("select * from nokeysecondaryindex where id<10 and id>7 order by id2 asc, id desc");
    ResultSet ret = stmt.executeQuery();
    assertTrue(ret.next());
    assertEquals(ret.getInt("id2"), 16);
    assertEquals(ret.getInt("id"), 8);
    assertTrue(ret.next());
    assertEquals(ret.getInt("id2"), 18);
    assertEquals(ret.getInt("id"), 9);
    assertFalse(ret.next());
  }

  @Test
  public void testLessNoKey() throws Exception {

    PreparedStatement stmt = conn.prepareStatement("select * from nokey where id<10 and id>7 order by id2 asc, id desc");
    ResultSet ret = stmt.executeQuery();
    assertTrue(ret.next());
    assertEquals(ret.getInt("id2"), 16);
    assertEquals(ret.getInt("id"), 8);
    assertTrue(ret.next());
    assertEquals(ret.getInt("id2"), 16);
    assertEquals(ret.getInt("id"), 8);
    assertTrue(ret.next());
    assertEquals(ret.getInt("id2"), 18);
    assertEquals(ret.getInt("id"), 9);
    assertTrue(ret.next());
    assertEquals(ret.getInt("id2"), 18);
    assertEquals(ret.getInt("id"), 9);
    assertFalse(ret.next());
  }

  @Test
  public void testBasicsNoKeySecondaryIndex() throws Exception {

    //test select returns multiple records with an index using operator '<'
    PreparedStatement stmt = conn.prepareStatement("select id, id2 from nokeysecondaryIndex where id<5 order by id desc");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.isBeforeFirst());
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 4);
    assertEquals(ret.getLong("id2"), 8);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 3);
    assertEquals(ret.getLong("id2"), 6);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 2);
    assertEquals(ret.getLong("id2"), 4);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 1);
    assertEquals(ret.getLong("id2"), 2);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 0);
    assertEquals(ret.getLong("id2"), 0);
    assertFalse(ret.next());
  }

  @Test
  public void testBasicsNoKey() throws Exception {

    //test select returns multiple records with an index using operator '<'
    PreparedStatement stmt = conn.prepareStatement("select id, id2 from nokey where id<5 order by id desc");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.isBeforeFirst());
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 4);
    assertEquals(ret.getLong("id2"), 8);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 4);
    assertEquals(ret.getLong("id2"), 8);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 3);
    assertEquals(ret.getLong("id2"), 6);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 3);
    assertEquals(ret.getLong("id2"), 6);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 2);
    assertEquals(ret.getLong("id2"), 4);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 2);
    assertEquals(ret.getLong("id2"), 4);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 1);
    assertEquals(ret.getLong("id2"), 2);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 1);
    assertEquals(ret.getLong("id2"), 2);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 0);
    assertEquals(ret.getLong("id2"), 0);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 0);
    assertEquals(ret.getLong("id2"), 0);
    assertFalse(ret.next());
  }

  @Test
  public void testBasics() throws Exception {

    //test select returns multiple records with an index using operator '<'
    PreparedStatement stmt = conn.prepareStatement("select id, id2 from persons where id<5 order by id desc");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.isBeforeFirst());
    assertTrue(ret.next());
//    System.out.println(ret.getLong("id"));
//    System.out.println(ret.getLong("id2"));
    ret.getLong("id2");
    assertTrue(ret.wasNull());
    ret.getInt(2);
    assertTrue(ret.wasNull());
    ret.getLong("id2");
    assertTrue(ret.wasNull());
    ret.getLong(2);
    assertTrue(ret.wasNull());
    ret.getDouble("id2");
    assertTrue(ret.wasNull());
    ret.getDouble(2);
    assertTrue(ret.wasNull());
    ret.getFloat("id2");
    assertTrue(ret.wasNull());
    ret.getFloat(2);
    assertTrue(ret.wasNull());
    ret.getString("id2");
    assertTrue(ret.wasNull());
    ret.getString(2);
    assertTrue(ret.wasNull());
    ret.getBigDecimal("id2");
    assertTrue(ret.wasNull());
    ret.getBigDecimal(2);
    assertTrue(ret.wasNull());
    ret.getBinaryStream("id2");
    assertTrue(ret.wasNull());
    ret.getBinaryStream(2);
    assertTrue(ret.wasNull());
    ret.getBytes("id2");
    assertTrue(ret.wasNull());
    ret.getBytes(2);
    assertTrue(ret.wasNull());
    ret.getBoolean("id2");
    assertTrue(ret.wasNull());
    ret.getBoolean(2);
    assertTrue(ret.wasNull());
    ret.getByte("id2");
    assertTrue(ret.wasNull());
    ret.getByte(2);
    assertTrue(ret.wasNull());
    ret.getDate("id2");
    assertTrue(ret.wasNull());
    ret.getDate(2);
    assertTrue(ret.wasNull());
    ret.getCharacterStream("id2");
    assertTrue(ret.wasNull());
//    ret.getCharacterStream(2);
//    assertTrue(ret.wasNull());
    ret.getNClob("id2");
    assertTrue(ret.wasNull());
    ret.getNClob(2);
    assertTrue(ret.wasNull());
    ret.getNString("id2");
    assertTrue(ret.wasNull());
    ret.getNString(2);
    assertTrue(ret.wasNull());
    ret.getShort("id2");
    assertTrue(ret.wasNull());
    ret.getShort(2);
    assertTrue(ret.wasNull());
    ret.getTime("id2");
    assertTrue(ret.wasNull());
    ret.getTime(2);
    assertTrue(ret.wasNull());
    ret.getTimestamp("id2");
    assertTrue(ret.wasNull());
    ret.getTimestamp(2);
    assertTrue(ret.wasNull());
    assertEquals(ret.getRow(), 1);
    assertTrue(ret.isFirst());
    assertEquals(ret.getLong("id"), 4);
    ret.next();
    assertEquals(ret.getLong("id"), 3);
    ret.next();
    assertEquals(ret.getLong("id"), 2);
    ret.next();
    assertEquals(ret.getLong("id"), 1);
    ret.next();
    assertEquals(ret.getLong("id"), 0);
    assertTrue(ret.isLast());
    assertFalse(ret.next());
    assertTrue(ret.isAfterLast());

  }

  @Test
  public void testNotInNoKeySecondaryIndex() throws SQLException {
    //test select with not in expression
    PreparedStatement stmt = conn.prepareStatement("select * from nokeysecondaryIndex where id not in (3, 4, 5, 6, 7, 8, 9, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109) order by id asc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 0);
    assertEquals(ret.getLong("id2"), 0);
    ret.next();
    assertEquals(ret.getLong("id"), 1);
    assertEquals(ret.getLong("id2"), 2);
    ret.next();
    assertEquals(ret.getLong("id"), 2);
    assertEquals(ret.getLong("id2"), 4);
    assertFalse(ret.next());
  }

  @Test
  public void testNotInNoKey() throws SQLException {
    //test select with not in expression
    PreparedStatement stmt = conn.prepareStatement("select * from nokey where id not in (3, 4, 5, 6, 7, 8, 9, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109) order by id asc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 0);
    assertEquals(ret.getLong("id2"), 0);
    ret.next();
    assertEquals(ret.getLong("id"), 0);
    assertEquals(ret.getLong("id2"), 0);
    ret.next();
    assertEquals(ret.getLong("id"), 1);
    assertEquals(ret.getLong("id2"), 2);
    ret.next();
    assertEquals(ret.getLong("id"), 1);
    assertEquals(ret.getLong("id2"), 2);
    ret.next();
    assertEquals(ret.getLong("id"), 2);
    assertEquals(ret.getLong("id2"), 4);
    ret.next();
    assertEquals(ret.getLong("id"), 2);
    assertEquals(ret.getLong("id2"), 4);
    assertFalse(ret.next());
  }

  @Test
  public void testNotIn() throws SQLException {
    //test select with not in expression
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id not in (5, 6, 7, 8, 9, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109) order by id asc");
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
  public void testIdentityNoKeySecondaryIndex() throws SQLException {
    //test select with not in expression
    PreparedStatement stmt = conn.prepareStatement("select * from nokeysecondaryindex where id = 5");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 5);
    assertEquals(ret.getLong("id2"), 10);
    assertFalse(ret.next());
  }

  @Test
  public void testIdentityNoKey() throws SQLException {
    //test select with not in expression
    PreparedStatement stmt = conn.prepareStatement("select * from nokey where id = 5");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 5);
    assertEquals(ret.getLong("id2"), 10);
    ret.next();
    assertEquals(ret.getLong("id"), 5);
    assertEquals(ret.getLong("id2"), 10);
    assertFalse(ret.next());
  }

  @Test
  public void testIdentity() throws SQLException {
    //test select with not in expression
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id = 5");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 5);
    assertFalse(ret.next());

    stmt = conn.prepareStatement("select * from persons where id = 5");
    ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 5);
    assertFalse(ret.next());
  }

  @Test
  public void testNoKeySecondaryIndex() throws SQLException {
    //test select with not in expression
    PreparedStatement stmt = conn.prepareStatement("select * from nokeysecondaryIndex where id < 5 order by id asc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 0);
    assertEquals(ret.getLong("id2"), 0);
    ret.next();
    assertEquals(ret.getLong("id"), 1);
    assertEquals(ret.getLong("id2"), 2);
    ret.next();
    assertEquals(ret.getLong("id"), 2);
    assertEquals(ret.getLong("id2"), 4);
  }

  @Test
  public void testNoKey() throws SQLException {
    //test select with not in expression
    PreparedStatement stmt = conn.prepareStatement("select * from nokey where id < 5 order by id asc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 0);
    assertEquals(ret.getLong("id2"), 0);
    ret.next();
    assertEquals(ret.getLong("id"), 0);
    assertEquals(ret.getLong("id2"), 0);
    ret.next();
    assertEquals(ret.getLong("id"), 1);
    assertEquals(ret.getLong("id2"), 2);
    ret.next();
    assertEquals(ret.getLong("id"), 1);
    assertEquals(ret.getLong("id2"), 2);
    ret.next();
    assertEquals(ret.getLong("id"), 2);
    assertEquals(ret.getLong("id2"), 4);
  }

  @Test
  public void testNoKeySecondaryIndex2() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from nokeysecondaryindex where id <= 2 and id2 = 4 order by id asc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 2);
    assertEquals(ret.getLong("id2"), 4);
    assertFalse(ret.next());
  }

  @Test
  public void testNoKey2() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from nokey where id <= 2 and id2 = 4 order by id asc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 2);
    assertEquals(ret.getLong("id2"), 4);
    ret.next();
    assertEquals(ret.getLong("id"), 2);
    assertEquals(ret.getLong("id2"), 4);
    assertFalse(ret.next());
  }


  @Test
  public void testNotInAndNoKeySecondaryIndex() throws SQLException {
    //test select with not in expression
    PreparedStatement stmt = conn.prepareStatement("select * from nokeysecondaryindex where id < 4 and id > 1 and id not in (5, 6, 7, 8, 9, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109) order by id desc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 3);
    assertEquals(ret.getLong("id2"), 6);
    ret.next();
    assertEquals(ret.getLong("id"), 2);
    assertEquals(ret.getLong("id2"), 4);
    assertFalse(ret.next());
  }

  @Test
  public void testNotInAndNoKey() throws SQLException {
    //test select with not in expression
    PreparedStatement stmt = conn.prepareStatement("select * from nokey where id < 4 and id > 1 and id not in (5, 6, 7, 8, 9, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109) order by id desc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 3);
    assertEquals(ret.getLong("id2"), 6);
    ret.next();
    assertEquals(ret.getLong("id"), 3);
    assertEquals(ret.getLong("id2"), 6);
    ret.next();
    assertEquals(ret.getLong("id"), 2);
    assertEquals(ret.getLong("id2"), 4);
    ret.next();
    assertEquals(ret.getLong("id"), 2);
    assertEquals(ret.getLong("id2"), 4);
    assertFalse(ret.next());
  }

  @Test
  public void testNotInAnd() throws SQLException {
    //test select with not in expression
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id < 4 and id > 1 and id not in (5, 6, 7, 8, 9, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109) order by id desc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 3);
    ret.next();
    assertEquals(ret.getLong("id"), 2);
    assertFalse(ret.next());
  }

  @Test
  public void testAllNoKeySecondaryIndex() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from nokeysecondaryIndex");
    ResultSet ret = stmt.executeQuery();

    for (int i = 0; i < recordCount; i++) {
      ret.next();
      assertEquals(ret.getLong("id"), i);
      assertEquals(ret.getLong("id2"), i * 2);
    }
    assertFalse(ret.next());
  }

  @Test
  public void testAllNoKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from nokey");
    ResultSet ret = stmt.executeQuery();

    for (int i = 0; i < recordCount; i++) {
      ret.next();
      assertEquals(ret.getLong("id"), i);
      assertEquals(ret.getLong("id2"), i * 2);
      ret.next();
      assertEquals(ret.getLong("id"), i);
      assertEquals(ret.getLong("id2"), i * 2);
    }
    assertFalse(ret.next());
  }

  @Test
  public void testAll() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons");
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
    ret.next();
    assertEquals(ret.getLong("id"), 5);
    ret.next();
    assertEquals(ret.getLong("id"), 6);
    ret.next();
    assertEquals(ret.getLong("id"), 7);
    ret.next();
    assertEquals(ret.getLong("id"), 8);
    ret.next();
    assertEquals(ret.getLong("id"), 9);
    ret.next();
    assertEquals(ret.getLong("id"), 100);
    ret.next();
    assertEquals(ret.getLong("id"), 101);
    ret.next();
    assertEquals(ret.getLong("id"), 102);
    ret.next();
    assertEquals(ret.getLong("id"), 103);
    ret.next();
    assertEquals(ret.getLong("id"), 104);
    ret.next();
    assertEquals(ret.getLong("id"), 105);
    ret.next();
    assertEquals(ret.getLong("id"), 106);
    ret.next();
    assertEquals(ret.getLong("id"), 107);
    ret.next();
    assertEquals(ret.getLong("id"), 108);
    ret.next();
    assertEquals(ret.getLong("id"), 109);
    assertFalse(ret.next());
  }

  @Test
  public void testParametersNoKeySecondaryIndex() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from nokeysecondaryIndex where id < ? and id > ?");
    stmt.setLong(1, 5);
    stmt.setLong(2, 2);
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 3);
    assertEquals(ret.getLong("id2"), 6);
    ret.next();
    assertEquals(ret.getLong("id"), 4);
    assertEquals(ret.getLong("id2"), 8);
    assertFalse(ret.next());
  }

  @Test
  public void testParametersNoKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from nokey where id < ? and id > ?");
    stmt.setLong(1, 5);
    stmt.setLong(2, 2);
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 3);
    assertEquals(ret.getLong("id2"), 6);
    ret.next();
    assertEquals(ret.getLong("id"), 3);
    assertEquals(ret.getLong("id2"), 6);
    ret.next();
    assertEquals(ret.getLong("id"), 4);
    assertEquals(ret.getLong("id2"), 8);
    ret.next();
    assertEquals(ret.getLong("id"), 4);
    assertEquals(ret.getLong("id2"), 8);
    assertFalse(ret.next());
  }

  @Test
  public void testParameters() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id < ? and id > ?");
    stmt.setLong(1, 5);
    stmt.setLong(2, 2);
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 3);
    ret.next();
    assertEquals(ret.getLong("id"), 4);
    assertFalse(ret.next());
  }


  @Test
  public void test2FieldKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from memberships where personId=? and membershipName=?");
    stmt.setLong(1, 0);
    stmt.setString(2, "membership-0");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("personId"), 0);
    assertEquals(ret.getString("membershipName"), "membership-0");
    assertFalse(ret.next());
  }

  @Test
  public void testMaxNoKeySecondaryIndex() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select max(id) as maxValue from nokeysecondaryindex");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("maxValue"), 9);
    assertFalse(ret.next());
  }

  @Test
  public void testMaxNoKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select max(id) as maxValue from nokey");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("maxValue"), 9);
    assertFalse(ret.next());
  }

  @Test
  public void testMax() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select max(id) as maxValue from persons");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("maxValue"), 109);
    assertFalse(ret.next());
  }

  @Test
  public void testMinNoKeySecondaryIndex() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select min(id) as minValue from nokeysecondaryindex");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("minValue"), 0);
    assertFalse(ret.next());
  }

  @Test
  public void testMinNoKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select min(id) as minValue from nokey");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("minValue"), 0);
    assertFalse(ret.next());
  }

  @Test
  public void testMin() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select min(id) as minValue from persons");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("minValue"), 0);
    assertFalse(ret.next());
  }

  @Test
  public void testMaxTableScanNoKeySecondaryIndex() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select max(id) as maxValue from nokeysecondaryindex where id2 < 1");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("maxValue"), 0);
    assertFalse(ret.next());
  }

  @Test
  public void testMaxTableScan() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select max(id) as maxValue from persons where id2 < 1");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("maxValue"), 108);
    assertFalse(ret.next());
  }

  @Test
  public void testMaxWhereNoKeySecondaryIndex() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select max(id) as maxValue from nokeysecondaryindex where id < 4");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("maxValue"), 3);
    assertFalse(ret.next());
  }

  @Test
  public void testMaxWhereNoKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select max(id) as maxValue from nokey where id < 4");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("maxValue"), 3);
    assertFalse(ret.next());
  }

  @Test
  public void testMaxWhere() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select max(id) as maxValue from persons where id < 100");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("maxValue"), 9);
    assertFalse(ret.next());
  }

  @Test
  public void testSumNoKeySecondaryIndex() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select sum(id) as sumValue from nokeysecondaryindex");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("sumValue"), 45);
    assertFalse(ret.next());
  }

  @Test
  public void testSumNoKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select sum(id) as sumValue from nokey");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("sumValue"), 90);
    assertFalse(ret.next());
  }

  @Test
  public void testSum() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select sum(id) as sumValue from persons");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("sumValue"), 1090);
    assertFalse(ret.next());
  }

  @Test
  public void testLimitNoKeySecondaryIndex() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from nokeysecondaryindex where id < ? and id > ? limit 3");
    stmt.setLong(1, 9);
    stmt.setLong(2, 2);
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 3);
    assertEquals(ret.getLong("id2"), 6);
    ret.next();
    assertEquals(ret.getLong("id"), 4);
    assertEquals(ret.getLong("id2"), 8);
    ret.next();
    assertEquals(ret.getLong("id"), 5);
    assertEquals(ret.getLong("id2"), 10);
    assertFalse(ret.next());
  }

  @Test
  public void testLimitNoKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from nokey where id < ? and id > ? limit 3");
    stmt.setLong(1, 9);
    stmt.setLong(2, 2);
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 3);
    assertEquals(ret.getLong("id2"), 6);
    ret.next();
    assertEquals(ret.getLong("id"), 3);
    assertEquals(ret.getLong("id2"), 6);
    ret.next();
    assertEquals(ret.getLong("id"), 4);
    assertEquals(ret.getLong("id2"), 8);
    assertFalse(ret.next());
  }

  @Test
  public void testLimit() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id < ? and id > ? limit 3");
    stmt.setLong(1, 100);
    stmt.setLong(2, 2);
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
  public void testLimitOffsetNoKeySecondaryIndex() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from nokeysecondaryindex where id < ? and id > ? limit 3 offset 2");
    stmt.setLong(1, 9);
    stmt.setLong(2, 2);
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 4);
    assertEquals(ret.getLong("id2"), 8);
    ret.next();
    assertEquals(ret.getLong("id"), 5);
    assertEquals(ret.getLong("id2"), 10);
    ret.next();
    assertEquals(ret.getLong("id"), 6);
    assertEquals(ret.getLong("id2"), 12);
    assertFalse(ret.next());
  }

  @Test
  public void testLimitOffsetNoKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from nokey where id < ? and id > ? limit 3 offset 2");
    stmt.setLong(1, 9);
    stmt.setLong(2, 2);
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 3);
    assertEquals(ret.getLong("id2"), 6);
    ret.next();
    assertEquals(ret.getLong("id"), 4);
    assertEquals(ret.getLong("id2"), 8);
    ret.next();
    assertEquals(ret.getLong("id"), 4);
    assertEquals(ret.getLong("id2"), 8);
    assertFalse(ret.next());
  }

  @Test
  public void testLimitOffset() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id < ? and id > ? limit 3 offset 2");
    stmt.setLong(1, 100);
    stmt.setLong(2, 2);
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 4);
    ret.next();
    assertEquals(ret.getLong("id"), 5);
    ret.next();
    assertEquals(ret.getLong("id"), 6);
    assertFalse(ret.next());
  }

  @Test
  public void testLimitOffsetOneKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id > ? limit 3 offset 2");
    stmt.setLong(1, 2);
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 4);
    ret.next();
    assertEquals(ret.getLong("id"), 5);
    ret.next();
    assertEquals(ret.getLong("id"), 6);
    assertFalse(ret.next());
  }


  @Test
  public void testSort2NoKeySecondaryIndex() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id2 from nokeysecondaryindex order by id2 asc, id asc");
    ResultSet ret = stmt.executeQuery();

    for (int i = 0; i < recordCount; i++) {
      ret.next();
      assertEquals(ret.getLong("id2"), i * 2);
      assertEquals(ret.getLong("id"), i);
    }
  }

  @Test
  public void testSort2NoKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id2 from nokey order by id2 asc, id asc");
    ResultSet ret = stmt.executeQuery();

    for (int i = 0; i < recordCount; i++) {
      ret.next();
      assertEquals(ret.getLong("id2"), i * 2);
      assertEquals(ret.getLong("id"), i);
      ret.next();
      assertEquals(ret.getLong("id2"), i * 2);
      assertEquals(ret.getLong("id"), i);
    }

  }

  @Test
  public void testSort2() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id2 from persons order by id2 asc, id asc");
    ResultSet ret = stmt.executeQuery();

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
    assertEquals(ret.getLong("id"), 0);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertTrue(ret.wasNull());
    assertEquals(ret.getLong("id"), 1);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertTrue(ret.wasNull());
    assertEquals(ret.getLong("id"), 2);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertTrue(ret.wasNull());
    assertEquals(ret.getLong("id"), 3);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertTrue(ret.wasNull());
    assertEquals(ret.getLong("id"), 4);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertTrue(ret.wasNull());
    assertEquals(ret.getLong("id"), 5);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertTrue(ret.wasNull());
    assertEquals(ret.getLong("id"), 6);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertTrue(ret.wasNull());
    assertEquals(ret.getLong("id"), 7);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertTrue(ret.wasNull());
    assertEquals(ret.getLong("id"), 8);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertTrue(ret.wasNull());
    assertEquals(ret.getLong("id"), 9);
    assertFalse(ret.next());
  }


  @Test
  public void testAllSortAndNoKeySecondaryIndex() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from nokeysecondaryindex where id < 2 and id2 = 0 order by id2 asc, id desc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 0);
    assertFalse(ret.next());
  }

  @Test
  public void testAllSortAndNoKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from nokey where id < 2 and id2 = 0 order by id2 asc, id desc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 0);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 0);
    assertFalse(ret.next());
  }

  @Test
  public void testAllSortAnd() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id > 100 and id2 = 0 order by id2 asc, id desc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 108);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 106);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 104);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 102);
    assertFalse(ret.next());
  }


  @Test
  public void testComplexNoKeySecondaryIndex() throws SQLException {
    //fails

    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select nokeysecondaryindex.id  " +
        "from nokeysecondaryindex where nokeysecondaryindex.id>=1 AND id < 3 AND ID2=2 OR id> 2 AND ID < 4");                                              //
    ResultSet ret = stmt.executeQuery();


    ret.next();
    assertEquals(ret.getLong("id"), 1);
    ret.next();
    assertEquals(ret.getLong("id"), 3);

    assertFalse(ret.next());
  }

  @Test
  public void testComplexNoKey() throws SQLException {
    //fails

    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select nokey.id  " +
        "from nokey where nokey.id>=1 AND id < 3 AND ID2=2 OR id> 2 AND ID < 4");                                              //
    ResultSet ret = stmt.executeQuery();


    ret.next();
    assertEquals(ret.getLong("id"), 1);
    ret.next();
    assertEquals(ret.getLong("id"), 1);
    ret.next();
    assertEquals(ret.getLong("id"), 3);
    ret.next();
    assertEquals(ret.getLong("id"), 3);

    assertFalse(ret.next());
  }

  @Test
  public void testComplex() throws SQLException {
    //fails

    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select persons.id  " +
        "from persons where persons.id>=100 AND id < 105 AND ID2=0 OR id> 6 AND ID < 10");                                              //
    ResultSet ret = stmt.executeQuery();


    ret.next();
    assertEquals(ret.getLong("id"), 100);
    ret.next();
    assertEquals(ret.getLong("id"), 102);
    ret.next();
    assertEquals(ret.getLong("id"), 104);
    ret.next();
    assertEquals(ret.getLong("id"), 7);
    ret.next();
    assertEquals(ret.getLong("id"), 8);
    ret.next();
    assertEquals(ret.getLong("id"), 9);

    assertFalse(ret.next());
  }

  @Test
  public void testParensNoKeySecondaryIndex() throws SQLException {
    //fails

    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select nokeysecondaryindex.id  " +
        "from nokeysecondaryindex where nokeysecondaryindex.id<=5 AND (id < 2 OR id> 4)");                                              //
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 0);
    ret.next();
    assertEquals(ret.getLong("id"), 1);
    ret.next();
    assertEquals(ret.getLong("id"), 5);

    assertFalse(ret.next());
  }

  @Test
  public void testParensNoKey() throws SQLException {
    //fails

    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select nokey.id  " +
        "from nokey where nokey.id<=5 AND (id < 2 OR id> 4)");                                              //
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 0);
    ret.next();
    assertEquals(ret.getLong("id"), 0);
    ret.next();
    assertEquals(ret.getLong("id"), 1);
    ret.next();
    assertEquals(ret.getLong("id"), 1);
    ret.next();
    assertEquals(ret.getLong("id"), 5);
    ret.next();
    assertEquals(ret.getLong("id"), 5);

    assertFalse(ret.next());
  }

  @Test
  public void testParens() throws SQLException {
    //fails

    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select persons.id  " +
        "from persons where persons.id<=100 AND (id < 6 OR id> 8)");                                              //
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
    ret.next();
    assertEquals(ret.getLong("id"), 5);
    ret.next();
    assertEquals(ret.getLong("id"), 9);
    ret.next();
    assertEquals(ret.getLong("id"), 100);

    assertFalse(ret.next());
  }

  @Test
  public void testPrecedenceNoKeySecondaryIndex() throws SQLException {
    //fails

    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select nokeysecondaryindex.id  " +
        "from nokeysecondaryindex where nokeysecondaryindex.id<=7 AND id > 4 OR id> 8");                                              //
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 5);
    ret.next();
    assertEquals(ret.getLong("id"), 6);
    ret.next();
    assertEquals(ret.getLong("id"), 7);
    ret.next();
    assertEquals(ret.getLong("id"), 9);

    assertFalse(ret.next());
  }

  @Test
  public void testPrecedenceNoKey() throws SQLException {
    //fails

    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select nokey.id  " +
        "from nokey where nokey.id<=7 AND id > 4 OR id> 8");                                              //
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 5);
    ret.next();
    assertEquals(ret.getLong("id"), 5);
    ret.next();
    assertEquals(ret.getLong("id"), 6);
    ret.next();
    assertEquals(ret.getLong("id"), 6);
    ret.next();
    assertEquals(ret.getLong("id"), 7);
    ret.next();
    assertEquals(ret.getLong("id"), 7);
    ret.next();
    assertEquals(ret.getLong("id"), 9);
    ret.next();
    assertEquals(ret.getLong("id"), 9);

    assertFalse(ret.next());
  }

  @Test
  public void testPrecedence() throws SQLException {
    //fails

    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select persons.id  " +
        "from persons where persons.id<=100 AND id > 4 OR id> 103");                                              //
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 5);
    ret.next();
    assertEquals(ret.getLong("id"), 6);
    ret.next();
    assertEquals(ret.getLong("id"), 7);
    ret.next();
    assertEquals(ret.getLong("id"), 8);
    ret.next();
    assertEquals(ret.getLong("id"), 9);
    ret.next();
    assertEquals(ret.getLong("id"), 100);
    ret.next();
    assertEquals(ret.getLong("id"), 104);
    ret.next();
    assertEquals(ret.getLong("id"), 105);
    ret.next();
    assertEquals(ret.getLong("id"), 106);
    ret.next();
    assertEquals(ret.getLong("id"), 107);
    ret.next();
    assertEquals(ret.getLong("id"), 108);
    ret.next();
    assertEquals(ret.getLong("id"), 109);

    assertFalse(ret.next());
  }


  @Test
  public void testOverlapPrecedenceNoKeySecondaryIndex() throws SQLException {
    //fails

    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select nokeysecondaryindex.id  " +
        "from nokeysecondaryindex where nokeysecondaryindex.id<=8 AND id < 2 OR id> 8");                                              //
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 0);
    ret.next();
    assertEquals(ret.getLong("id"), 1);
    ret.next();
    assertEquals(ret.getLong("id"), 9);

    assertFalse(ret.next());
  }

  @Test
  public void testOverlapPrecedenceNoKey() throws SQLException {
    //fails

    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select nokey.id  " +
        "from nokey where nokey.id<=8 AND id < 2 OR id> 8");                                              //
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 0);
    ret.next();
    assertEquals(ret.getLong("id"), 0);
    ret.next();
    assertEquals(ret.getLong("id"), 1);
    ret.next();
    assertEquals(ret.getLong("id"), 1);
    ret.next();
    assertEquals(ret.getLong("id"), 9);
    ret.next();
    assertEquals(ret.getLong("id"), 9);

    assertFalse(ret.next());
  }

  @Test
  public void testOverlapPrecedence() throws SQLException {
    //fails

    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select persons.id  " +
        "from persons where persons.id<=100 AND id < 4 OR id> 103");                                              //
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
    assertEquals(ret.getLong("id"), 104);
    ret.next();
    assertEquals(ret.getLong("id"), 105);
    ret.next();
    assertEquals(ret.getLong("id"), 106);
    ret.next();
    assertEquals(ret.getLong("id"), 107);
    ret.next();
    assertEquals(ret.getLong("id"), 108);
    ret.next();
    assertEquals(ret.getLong("id"), 109);

    assertFalse(ret.next());
  }

  @Test
  public void testOverlapPrecedence2NoKeySecondaryIndex() throws SQLException {
    //fails

    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select nokeysecondaryindex.id  " +
        "from nokeysecondaryindex where nokeysecondaryindex.id<=7 AND id = 4 OR id> 8");                                              //
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 4);
    ret.next();
    assertEquals(ret.getLong("id"), 9);

    assertFalse(ret.next());
  }

  @Test
  public void testOverlapPrecedence2NoKey() throws SQLException {
    //fails

    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select nokey.id  " +
        "from nokey where nokey.id<=7 AND id = 4 OR id> 8");                                              //
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 4);
    ret.next();
    assertEquals(ret.getLong("id"), 4);
    ret.next();
    assertEquals(ret.getLong("id"), 9);
    ret.next();
    assertEquals(ret.getLong("id"), 9);
    assertFalse(ret.next());
  }

  @Test
  public void testOverlapPrecedence2() throws SQLException {
    //fails

    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select persons.id  " +
        "from persons where persons.id<=100 AND id = 4 OR id> 103");                                              //
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 4);
    ret.next();
    assertEquals(ret.getLong("id"), 104);
    ret.next();
    assertEquals(ret.getLong("id"), 105);
    ret.next();
    assertEquals(ret.getLong("id"), 106);
    ret.next();
    assertEquals(ret.getLong("id"), 107);
    ret.next();
    assertEquals(ret.getLong("id"), 108);
    ret.next();
    assertEquals(ret.getLong("id"), 109);

    assertFalse(ret.next());
  }

  @Test
  public void testAvgNoKeySecondaryIndex() throws SQLException {
    //fails

    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select avg(nokeysecondaryindex.id) as avgValue from nokeysecondaryindex");                                              //
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getDouble("avgValue"), 4.5d);
    assertFalse(ret.next());
  }

  @Test
  public void testAvgNoKey() throws SQLException {
    //fails

    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select avg(nokey.id) as avgValue from nokey");                                              //
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getDouble("avgValue"), 4.5d);
    assertFalse(ret.next());
  }

  @Test
  public void testAvg() throws SQLException {
    //fails

    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select avg(persons.id) as avgValue from persons");                                              //
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getDouble("avgValue"), 54.5d);
    assertFalse(ret.next());
  }

  @Test(enabled = false)
  public void testAvg2() throws SQLException {
    //fails

    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select persons.id from persons where id > (select avg(id) from persons)");                                              //
    ResultSet ret = stmt.executeQuery();

    assertTrue(false);
    ret.next();
    assertEquals(ret.getLong("id"), 100);
    ret.next();
    assertEquals(ret.getLong("id"), 102);
    ret.next();
    assertEquals(ret.getLong("id"), 104);
    ret.next();
    assertEquals(ret.getLong("id"), 7);
    ret.next();
    assertEquals(ret.getLong("id"), 8);
    ret.next();
    assertEquals(ret.getLong("id"), 9);

    assertFalse(ret.next());
  }

  @Test
  public void testOrNoKeySecondaryIndex() throws SQLException {
    //fails

    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select * from nokeysecondaryindex where id>8 and id2=18 or id<6 and id2=2 order by id desc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 9);
    ret.next();
    assertEquals(ret.getLong("id"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testOrNoKey() throws SQLException {
    //fails

    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select * from nokey where id>8 and id2=18 or id<6 and id2=2 order by id desc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 9);
    ret.next();
    assertEquals(ret.getLong("id"), 9);
    ret.next();
    assertEquals(ret.getLong("id"), 1);
    ret.next();
    assertEquals(ret.getLong("id"), 1);
    assertFalse(ret.next());
  }


  @Test
  public void testOr() throws SQLException {
    //fails

    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id>105 and id2=0 or id<105 and id2=1 order by id desc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 108);
    ret.next();
    assertEquals(ret.getLong("id"), 106);
    ret.next();
    assertEquals(ret.getLong("id"), 103);
    ret.next();
    assertEquals(ret.getLong("id"), 101);
    assertFalse(ret.next());
//    assertFalse(ret.next());

//    Set<Long> found = new HashSet<>();
//    while (true) {
//      if (!ret.next()) {
//        break;
//      }
//      found.add(ret.getLong("id"));
//    }
//    assertEquals(found.size(), 5);
//
//    assertTrue(found.contains(0L));
//    assertTrue(found.contains(1L));
//    assertTrue(found.contains(2L));
//    assertTrue(found.contains(3L));
//    assertTrue(found.contains(4L));
  }

  @Test(enabled=false)
  public void serverSort() throws SQLException {

    PreparedStatement stmt = conn.prepareStatement("select persons.id, socialsecuritynumber as s " +
        "from persons where persons.id>100 AND id < 105 order by socialsecuritynumber desc");                                              //
    ResultSet ret = stmt.executeQuery();
    try {
      ret.next();
      assertEquals(ret.getLong("id"), 103);
      assertEquals(ret.getString("socialsecuritynumber"), "933-28-3");
      ret.next();
      assertEquals(ret.getLong("id"), 102);
      assertEquals(ret.getString("socialsecuritynumber"), "933-28-2");
      ret.next();
      assertEquals(ret.getLong("id"), 101);
      assertEquals(ret.getString("socialsecuritynumber"), "933-28-1");
      ret.next();
      assertEquals(ret.getLong("id"), 104);
      assertEquals(ret.getString("socialsecuritynumber"), "933-28-0");
      assertFalse(ret.next());
    }
    finally {
      ret.close();
    }
  }

  @Test
  public void testMixed() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select persons.id, socialsecuritynumber as s " +
        "from persons where socialsecuritynumber > '933-28-6' AND persons.id>5 AND id < 10");                                              //
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 7);
    assertEquals(ret.getString("s"), "933-28-7");
    ret.next();
    assertEquals(ret.getLong("id"), 8);
    assertEquals(ret.getString("s"), "933-28-8");
    ret.next();
    assertEquals(ret.getLong("id"), 9);
    assertEquals(ret.getString("s"), "933-28-9");
    assertFalse(ret.next());
  }

  @Test
  public void testOrAnd() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select persons.id  " +
        "from persons where persons.id>2 AND id < 4 OR id> 6 AND ID < 8");                                              //
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 3);
    ret.next();
    assertEquals(ret.getLong("id"), 7);
    ret.next();
    assertFalse(ret.next());
  }

  @Test
  public void testEqualNonIndex() throws SQLException {
    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id2=1 order by id desc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 109);
    ret.next();
    assertEquals(ret.getLong("id"), 107);
    ret.next();
    assertEquals(ret.getLong("id"), 105);
    ret.next();
    assertEquals(ret.getLong("id"), 103);
    ret.next();
    assertEquals(ret.getLong("id"), 101);
    assertFalse(ret.next());
  }

  @Test
  public void testEqualIndex() throws SQLException {
    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id=1 order by id desc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testInNoKeySecondaryIndex() throws SQLException {

    //test select with in expression
    PreparedStatement stmt = conn.prepareStatement("select * from nokeysecondaryindex where id in (0, 1, 2)");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 0);
    ret.next();
    assertEquals(ret.getLong("id"), 1);
    ret.next();
    assertEquals(ret.getLong("id"), 2);
    assertFalse(ret.next());
  }

  @Test
  public void testInNoKey() throws SQLException {

    //test select with in expression
    PreparedStatement stmt = conn.prepareStatement("select * from nokey where id in (0, 1, 2)");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 0);
    ret.next();
    assertEquals(ret.getLong("id"), 0);
    ret.next();
    assertEquals(ret.getLong("id"), 1);
    ret.next();
    assertEquals(ret.getLong("id"), 1);
    ret.next();
    assertEquals(ret.getLong("id"), 2);
    ret.next();
    assertEquals(ret.getLong("id"), 2);
    assertFalse(ret.next());
  }

  @Test
  public void testIn() throws SQLException {

    //test select with in expression
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id in (0, 1, 2, 3, 4)");
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

  @Test(invocationCount = 1)
  public void testSecondaryIndex() throws SQLException {

    //test select on secondary index
    PreparedStatement stmt;
    ResultSet ret;
    for (int i = 0; i < recordCount; i++) {

      //test jdbc select
      stmt = conn.prepareStatement("select * from persons where socialSecurityNumber=? order by id");
      stmt.setString(1, "933-28-" + i);
      ret = stmt.executeQuery();
      System.out.println("checking: 993-28-" + i);
      assertTrue(ret.next());

      long retId = ret.getLong("id");
      String socialSecurityNumber = ret.getString("socialSecurityNumber");
      String retRelatives = ret.getString("relatives");
      boolean restricted = ret.getBoolean("restricted");
      String gender = ret.getString("gender");

      assertEquals(retId, i, "Returned id doesn't match: id=" + i + ", retId=" + retId);
      assertEquals(socialSecurityNumber, "933-28-" + i);
      assertNotNull(retRelatives);
      assertNotEquals(retRelatives.length(), 0);
      assertFalse(restricted);
      assertNotNull(gender);
      assertNotEquals(gender.length(), 0);
      assertEquals(gender.charAt(0), 'm');
    }
  }

  @Test
  public void testMultipleFields() throws SQLException {
    //test select on multiple fields
    PreparedStatement stmt;
    ResultSet ret;
    for (int i = 0; i < recordCount; i++) {

      //test jdbc select
      stmt = conn.prepareStatement("select * from persons where id=" + (i + 100) + " AND id2=" + ((i + 100) % 2));
      ret = stmt.executeQuery();

      assertTrue(ret.next());

      long retId = ret.getLong("id");
      long retId2 = ret.getLong("id2");
      String socialSecurityNumber = ret.getString("socialSecurityNumber");
      String retRelatives = ret.getString("relatives");
      boolean restricted = ret.getBoolean("restricted");
      String gender = ret.getString("gender");

      assertEquals(retId, i + 100, "Returned id doesn't match: id=" + i + ", retId=" + retId);
      assertEquals(retId2, (i + 100) % 2, "Returned id2 doesn't match: id2=" + (i + 100) % 2 + ", retId2=" + retId2);
      assertEquals(socialSecurityNumber, "933-28-" + (i % 4));
      assertNotNull(retRelatives);
      assertNotEquals(retRelatives.length(), 0);
      assertFalse(restricted);
      assertNotNull(gender);
      assertNotEquals(gender.length(), 0);
      assertEquals(gender.charAt(0), 'm');
    }
  }

  @Test
  public void testAndNoKeySecondaryIndex() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from nokeysecondaryindex where id!=0 AND id!=1 AND id!=2 AND id!=3 AND id!=4 order by id asc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 5);
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
  public void testAndNoKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from nokey where id!=0 AND id!=1 AND id!=2 AND id!=3 AND id!=4 order by id asc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 5);
    ret.next();
    assertEquals(ret.getLong("id"), 5);
    ret.next();
    assertEquals(ret.getLong("id"), 6);
    ret.next();
    assertEquals(ret.getLong("id"), 6);
    ret.next();
    assertEquals(ret.getLong("id"), 7);
    ret.next();
    assertEquals(ret.getLong("id"), 7);
    ret.next();
    assertEquals(ret.getLong("id"), 8);
    ret.next();
    assertEquals(ret.getLong("id"), 8);
    ret.next();
    assertEquals(ret.getLong("id"), 9);
    ret.next();
    assertEquals(ret.getLong("id"), 9);
    assertFalse(ret.next());
  }

  @Test
  public void testAnd() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id!=0 AND id!=1 AND id!=2 AND id!=3 AND id!=4 order by id asc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 5);
    ret.next();
    assertEquals(ret.getLong("id"), 6);
    ret.next();
    assertEquals(ret.getLong("id"), 7);
    ret.next();
    assertEquals(ret.getLong("id"), 8);
    ret.next();
    assertEquals(ret.getLong("id"), 9);
    ret.next();
    assertEquals(ret.getLong("id"), 100);
    ret.next();
    assertEquals(ret.getLong("id"), 101);
    ret.next();
    assertEquals(ret.getLong("id"), 102);
    ret.next();
    assertEquals(ret.getLong("id"), 103);
    ret.next();
    assertEquals(ret.getLong("id"), 104);
    ret.next();
    assertEquals(ret.getLong("id"), 105);
    ret.next();
    assertEquals(ret.getLong("id"), 106);
    ret.next();
    assertEquals(ret.getLong("id"), 107);
    ret.next();
    assertEquals(ret.getLong("id"), 108);
    ret.next();
    assertEquals(ret.getLong("id"), 109);
    assertFalse(ret.next());
  }

  @Test
  public void testOrTableScanNoKeySecondaryIndex() throws SQLException {
    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select * from nokeysecondaryindex where id2=2 or id2=0 order by id2 asc, id desc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 0);
    ret.next();
    assertEquals(ret.getLong("id"), 1);
    assertFalse(ret.next());

//    Set<Long> found = new HashSet<>();
//    for (int i = 101; i < 110; i += 2) {
//      ret.next();
//      found.add(ret.getLong("id"));
//    }
//    assertEquals(found.size(), 5);
//
//    assertTrue(found.contains(101L));
//    assertTrue(found.contains(103L));
//    assertTrue(found.contains(105L));
//    assertTrue(found.contains(107L));
//    assertTrue(found.contains(109L));
  }

  @Test
  public void testOrTableScan() throws SQLException {
    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id2=1 or id2=0 order by id2 asc, id desc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 108);
    ret.next();
    assertEquals(ret.getLong("id"), 106);
    ret.next();
    assertEquals(ret.getLong("id"), 104);
    ret.next();
    assertEquals(ret.getLong("id"), 102);
    ret.next();
    assertEquals(ret.getLong("id"), 100);
    ret.next();
    assertEquals(ret.getLong("id"), 109);
    ret.next();
    assertEquals(ret.getLong("id"), 107);
    ret.next();
    assertEquals(ret.getLong("id"), 105);
    ret.next();
    assertEquals(ret.getLong("id"), 103);
    ret.next();
    assertEquals(ret.getLong("id"), 101);
    assertFalse(ret.next());

//    Set<Long> found = new HashSet<>();
//    for (int i = 101; i < 110; i += 2) {
//      ret.next();
//      found.add(ret.getLong("id"));
//    }
//    assertEquals(found.size(), 5);
//
//    assertTrue(found.contains(101L));
//    assertTrue(found.contains(103L));
//    assertTrue(found.contains(105L));
//    assertTrue(found.contains(107L));
//    assertTrue(found.contains(109L));
  }

  @Test
  public void testOrIndexNoKeySecondaryIndex() throws SQLException {
    //test select returns multiple records with an index
    PreparedStatement stmt = conn.prepareStatement("select * from nokeysecondaryindex where id=0 OR id=1 OR id=2 OR id=3 OR id=4");
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
  public void testOrIndex() throws SQLException {
    //test select returns multiple records with an index
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id=0 OR id=1 OR id=2 OR id=3 OR id=4");
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
  public void testLessEqualNoKeySecondaryIndex() throws SQLException {

    //test select returns multiple records with an index using operator '<='
    PreparedStatement stmt = conn.prepareStatement("select * from nokeysecondaryindex where id<=3 order by id desc");
    ResultSet ret = stmt.executeQuery();

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
  public void testLessEqualNoKey() throws SQLException {

    //test select returns multiple records with an index using operator '<='
    PreparedStatement stmt = conn.prepareStatement("select * from nokey where id<=3 order by id desc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 3);
    ret.next();
    assertEquals(ret.getLong("id"), 3);
    ret.next();
    assertEquals(ret.getLong("id"), 2);
    ret.next();
    assertEquals(ret.getLong("id"), 2);
    ret.next();
    assertEquals(ret.getLong("id"), 1);
    ret.next();
    assertEquals(ret.getLong("id"), 1);
    ret.next();
    assertEquals(ret.getLong("id"), 0);
    ret.next();
    assertEquals(ret.getLong("id"), 0);
    assertFalse(ret.next());
  }

  @Test
  public void testLessEqual() throws SQLException {

    //test select returns multiple records with an index using operator '<='
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id<=5 order by id desc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 5);
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
  public void testLessEqualGreaterEqual() throws SQLException {

    //test select returns multiple records with an index using operator '<='
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id >= 1 and id<=5 order by id asc");
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
  public void testLessEqualGreaterEqualDesc() throws SQLException {

    //test select returns multiple records with an index using operator '<='
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id >= 1 and id<=5 order by id desc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 5);
    ret.next();
    assertEquals(ret.getLong("id"), 4);
    ret.next();
    assertEquals(ret.getLong("id"), 3);
    ret.next();
    assertEquals(ret.getLong("id"), 2);
    ret.next();
    assertEquals(ret.getLong("id"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testLessEqualAndGreaterEqualNoKeySecondaryIndex() throws SQLException {

    //test select returns multiple records with an index using operator '<='
    PreparedStatement stmt = conn.prepareStatement("select * from nokeysecondaryindex where id<=5 and id>=1 order by id desc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 5);
    ret.next();
    assertEquals(ret.getLong("id"), 4);
    ret.next();
    assertEquals(ret.getLong("id"), 3);
    ret.next();
     assertEquals(ret.getLong("id"), 2);
    ret.next();
    assertEquals(ret.getLong("id"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testLessEqualAndGreaterEqualNoKey() throws SQLException {

    //test select returns multiple records with an index using operator '<='
    PreparedStatement stmt = conn.prepareStatement("select * from nokey where id<=5 and id>=1 order by id desc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 5);
    ret.next();
    assertEquals(ret.getLong("id"), 5);
    ret.next();
    assertEquals(ret.getLong("id"), 4);
    ret.next();
    assertEquals(ret.getLong("id"), 4);
    ret.next();
    assertEquals(ret.getLong("id"), 3);
    ret.next();
    assertEquals(ret.getLong("id"), 3);
    ret.next();
    assertEquals(ret.getLong("id"), 2);
    ret.next();
    assertEquals(ret.getLong("id"), 2);
    ret.next();
    assertEquals(ret.getLong("id"), 1);
    ret.next();
    assertEquals(ret.getLong("id"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testLessEqualAndGreaterEqual() throws SQLException {

    //test select returns multiple records with an index using operator '<='
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id<=5 and id>=1 order by id desc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 5);
    ret.next();
    assertEquals(ret.getLong("id"), 4);
    ret.next();
    assertEquals(ret.getLong("id"), 3);
    ret.next();
    assertEquals(ret.getLong("id"), 2);
    ret.next();
    assertEquals(ret.getLong("id"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testGreaterNoKeySecondaryIndex() throws SQLException {
    //test select returns multiple records with an index using operator '>'
    PreparedStatement stmt = conn.prepareStatement("select * from nokeysecondaryindex where id>5");
    ResultSet ret = stmt.executeQuery();

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
  public void testGreaterNoKey() throws SQLException {
    //test select returns multiple records with an index using operator '>'
    PreparedStatement stmt = conn.prepareStatement("select * from nokey where id>5");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 6);
    ret.next();
    assertEquals(ret.getLong("id"), 6);
    ret.next();
    assertEquals(ret.getLong("id"), 7);
    ret.next();
    assertEquals(ret.getLong("id"), 7);
    ret.next();
    assertEquals(ret.getLong("id"), 8);
    ret.next();
    assertEquals(ret.getLong("id"), 8);
    ret.next();
    assertEquals(ret.getLong("id"), 9);
    ret.next();
    assertEquals(ret.getLong("id"), 9);
    assertFalse(ret.next());
  }

  @Test
  public void testGreater() throws SQLException {
    //test select returns multiple records with an index using operator '>'
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id>5");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 6);
    ret.next();
    assertEquals(ret.getLong("id"), 7);
    ret.next();
    assertEquals(ret.getLong("id"), 8);
    ret.next();
    assertEquals(ret.getLong("id"), 9);
    ret.next();
    assertEquals(ret.getLong("id"), 100);
    ret.next();
    assertEquals(ret.getLong("id"), 101);
    ret.next();
    assertEquals(ret.getLong("id"), 102);
    ret.next();
    assertEquals(ret.getLong("id"), 103);
    ret.next();
    assertEquals(ret.getLong("id"), 104);
    ret.next();
    assertEquals(ret.getLong("id"), 105);
    ret.next();
    assertEquals(ret.getLong("id"), 106);
    ret.next();
    assertEquals(ret.getLong("id"), 107);
    ret.next();
    assertEquals(ret.getLong("id"), 108);
    ret.next();
    assertEquals(ret.getLong("id"), 109);
    if (ret.next()) {
      long next = ret.getLong("id");
      System.out.println(next);
    }
    //assertFalse(ret.next());
  }

  @Test
  public void testGreaterEqualNoKeySecondaryIndex() throws SQLException {
    //test select returns multiple records with an index using operator '<='
    PreparedStatement stmt = conn.prepareStatement("select * from nokeysecondaryindex where id>=5");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 5);
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
  public void testGreaterEqualNoKey() throws SQLException {
    //test select returns multiple records with an index using operator '<='
    PreparedStatement stmt = conn.prepareStatement("select * from nokey where id>=5");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 5);
    ret.next();
    assertEquals(ret.getLong("id"), 5);
    ret.next();
    assertEquals(ret.getLong("id"), 6);
    ret.next();
    assertEquals(ret.getLong("id"), 6);
    ret.next();
    assertEquals(ret.getLong("id"), 7);
    ret.next();
    assertEquals(ret.getLong("id"), 7);
    ret.next();
    assertEquals(ret.getLong("id"), 8);
    ret.next();
    assertEquals(ret.getLong("id"), 8);
    ret.next();
    assertEquals(ret.getLong("id"), 9);
    ret.next();
    assertEquals(ret.getLong("id"), 9);
    assertFalse(ret.next());
  }

  @Test
  public void testGreaterEqual() throws SQLException {
    //test select returns multiple records with an index using operator '<='
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id>=5");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 5);
    ret.next();
    assertEquals(ret.getLong("id"), 6);
    ret.next();
    assertEquals(ret.getLong("id"), 7);
    ret.next();
    assertEquals(ret.getLong("id"), 8);
    ret.next();
    assertEquals(ret.getLong("id"), 9);
    ret.next();
    assertEquals(ret.getLong("id"), 100);
    ret.next();
    assertEquals(ret.getLong("id"), 101);
    ret.next();
    assertEquals(ret.getLong("id"), 102);
    ret.next();
    assertEquals(ret.getLong("id"), 103);
    ret.next();
    assertEquals(ret.getLong("id"), 104);
    ret.next();
    assertEquals(ret.getLong("id"), 105);
    ret.next();
    assertEquals(ret.getLong("id"), 106);
    ret.next();
    assertEquals(ret.getLong("id"), 107);
    ret.next();
    assertEquals(ret.getLong("id"), 108);
    ret.next();
    assertEquals(ret.getLong("id"), 109);
    assertFalse(ret.next());
  }

  @Test
  public void testEqual2() throws SQLException {
    //test select
    PreparedStatement stmt;
    ResultSet ret;
    for (int i = 0; i < recordCount; i++) {

      //test jdbc select
      stmt = conn.prepareStatement("select * from persons where id=" + i);
      ret = stmt.executeQuery();

      ret.next();
      long retId = ret.getLong("id");
      String socialSecurityNumber = ret.getString("socialSecurityNumber");
      String retRelatives = ret.getString("relatives");
      boolean restricted = ret.getBoolean("restricted");
      String gender = ret.getString("gender");

      assertEquals(retId, i, "Returned id doesn't match: id=" + i + ", retId=" + retId);
      assertEquals(socialSecurityNumber, "933-28-" + i);
      assertNotNull(retRelatives);
      assertNotEquals(retRelatives.length(), 0);
      assertFalse(restricted);
      assertNotNull(gender);
      assertNotEquals(gender.length(), 0);
      assertEquals(gender.charAt(0), 'm');

    }
  }

  @Test
  public void testEqual2NoKeySecondaryIndex() throws SQLException {
    //test select
    PreparedStatement stmt;
    ResultSet ret;
    for (int i = 0; i < recordCount; i++) {

      //test jdbc select
      stmt = conn.prepareStatement("select * from nokeysecondaryindex where id=" + i);
      ret = stmt.executeQuery();

      ret.next();
      long retId = ret.getLong("id");
      assertEquals(retId, i, "Returned id doesn't match: id=" + i + ", retId=" + retId);
      assertEquals(ret.getLong("id2"), 2 * i);
    }
  }

  @Test
  public void testEqual2NoKey() throws SQLException {
    //test select
    PreparedStatement stmt;
    ResultSet ret;
    for (int i = 0; i < recordCount; i++) {

      //test jdbc select
      stmt = conn.prepareStatement("select * from nokey where id=" + i);
      ret = stmt.executeQuery();

      ret.next();
      long retId = ret.getLong("id");
      assertEquals(retId, i, "Returned id doesn't match: id=" + i + ", retId=" + retId);
      assertEquals(ret.getLong("id2"), 2 * i);
    }
  }

  @Test
  public void testUpdateNoKeySecondaryIndex() throws SQLException {
    //fails

    PreparedStatement stmt = conn.prepareStatement("update nokeysecondaryindex set id = ?, id2=? where id=?");
    stmt.setLong(1, 1000);
    stmt.setLong(2, 2000);
    stmt.setLong(3, 0);
    stmt.executeUpdate();

    stmt = conn.prepareStatement("select * from nokeysecondaryindex where id=" + 0);
    ResultSet ret = stmt.executeQuery();
    assertFalse(ret.next());

    stmt = conn.prepareStatement("select * from nokeysecondaryindex where id=1000");
    ret = stmt.executeQuery();
    assertTrue(ret.next());
    assertFalse(ret.next());

    stmt = conn.prepareStatement("select * from nokeysecondaryindex where id2=2000");
    ret = stmt.executeQuery();
    assertTrue(ret.next());
    assertFalse(ret.next());

    stmt = conn.prepareStatement("update nokeysecondaryindex set id = ?, id2=? where id=?");
    stmt.setLong(1, 0);
    stmt.setLong(2, 0);
    stmt.setLong(3, 1000);
    stmt.executeUpdate();
  }

  @Test
  public void testUpdateNoKey() throws SQLException {
    //fails

    PreparedStatement stmt = conn.prepareStatement("update nokey set id = ?, id2=? where id=?");
    stmt.setLong(1, 1000);
    stmt.setLong(2, 2000);
    stmt.setLong(3, 0);
    stmt.executeUpdate();

    stmt = conn.prepareStatement("select * from nokey where id=" + 0);
    ResultSet ret = stmt.executeQuery();
    assertFalse(ret.next());

    stmt = conn.prepareStatement("select * from nokey where id=1000");
    ret = stmt.executeQuery();
    assertTrue(ret.next());
    assertTrue(ret.next());
    assertFalse(ret.next());

    stmt = conn.prepareStatement("select * from nokey where id2=2000");
    ret = stmt.executeQuery();
    assertTrue(ret.next());
    assertTrue(ret.next());
    assertFalse(ret.next());

    stmt = conn.prepareStatement("update nokey set id = ?, id2=? where id=?");
    stmt.setLong(1, 0);
    stmt.setLong(2, 0);
    stmt.setLong(3, 1000);
    stmt.executeUpdate();
  }

  @Test
  public void testUpdate() throws SQLException {
    //fails

    PreparedStatement stmt = conn.prepareStatement("update persons set id = ?, socialSecurityNumber=? where id=?");
    stmt.setLong(1, 1000);
    stmt.setString(2, "ssn");
    stmt.setLong(3, 0);
    stmt.executeUpdate();

    stmt = conn.prepareStatement("select * from persons where id=" + 0);
    ResultSet ret = stmt.executeQuery();
    assertFalse(ret.next());

    stmt = conn.prepareStatement("select * from persons where socialSecurityNumber='933-28-0'");
    ret = stmt.executeQuery();
    assertTrue(ret.next());
    assertTrue(ret.next());
    assertTrue(ret.next());
    assertFalse(ret.next());

    stmt = conn.prepareStatement("select * from persons where socialSecurityNumber='ssn'");
    ret = stmt.executeQuery();
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 1000);
    assertFalse(ret.next());

    stmt = conn.prepareStatement("select * from persons where id=" + 1000);
    ret = stmt.executeQuery();
    assertTrue(ret.next());

    stmt = conn.prepareStatement("select * from persons where socialSecurityNumber='ssn'");
    ret = stmt.executeQuery();
    assertTrue(ret.next());
  }

  @Test
  public void testUpdate2() throws SQLException, InterruptedException {

    PreparedStatement stmt = conn.prepareStatement("insert into persons (id, id2, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?)");
    stmt.setLong(1, 100000);
    stmt.setLong(2, (100) % 2);
    stmt.setString(3, "ssn");
    stmt.setString(4, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
    stmt.setBoolean(5, false);
    stmt.setString(6, "m");
    int count = stmt.executeUpdate();
    assertEquals(count, 1);

    stmt = conn.prepareStatement("update persons set id = ?, socialSecurityNumber=? where id=?");
    stmt.setLong(1, 101000);
    stmt.setString(2, "ssn2");
    stmt.setLong(3, 100000);
    count = stmt.executeUpdate();
    assertEquals(count, 1);

    stmt = conn.prepareStatement("select * from persons where id=101000");
    ResultSet resultSet = stmt.executeQuery();
    assertTrue(resultSet.next());

    stmt = conn.prepareStatement("select * from persons where id=100000");
    resultSet = stmt.executeQuery();
    assertFalse(resultSet.next());

    stmt = conn.prepareStatement("delete from persons where id=101000");
    stmt.executeUpdate();
  }

  @Test
  public void testInsert() throws SQLException, InterruptedException {

    PreparedStatement stmt = conn.prepareStatement("insert into persons (id, id2, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?)");
    stmt.setLong(1, 200000);
    stmt.setLong(2, (100) % 2);
    stmt.setString(3, "ssn");
    stmt.setString(4, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
    stmt.setBoolean(5, false);
    stmt.setString(6, "m");
    int count = stmt.executeUpdate();
    assertEquals(count, 1);

    try {
      stmt = conn.prepareStatement("insert into persons (id, id2, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?)");
      stmt.setLong(1, 200000);
      stmt.setLong(2, (100) % 2);
      stmt.setString(3, "ssn");
      stmt.setString(4, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
      stmt.setBoolean(5, false);
      stmt.setString(6, "m");
      count = stmt.executeUpdate();
      assertEquals(count, 1);
    }
    catch (Exception e) {
      //expected
    }

    stmt = conn.prepareStatement("select * from persons where id=200000");
    ResultSet resultSet = stmt.executeQuery();
    assertTrue(resultSet.next());
    assertFalse(resultSet.next());

    stmt = conn.prepareStatement("delete from persons where id=200000");
    stmt.executeUpdate();
  }


  @Test
  public void testBatchInsertNoKeySecondaryIndex() throws SQLException, InterruptedException {

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
  public void testBatchInsertNoKey() throws SQLException, InterruptedException {

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

    stmt = conn.prepareStatement("select * from nokey where id>=200000");
    ResultSet resultSet = stmt.executeQuery();
    for (int i = 0; i < 10; i++){
      assertTrue(resultSet.next());
      assertTrue(resultSet.next());
    }
    assertFalse(resultSet.next());

    for (int i = 0; i < 10; i++) {
      stmt = conn.prepareStatement("delete from nokey where id=?");
      stmt.setLong(1, 200000 + i);
      stmt.executeUpdate();
    }
  }

  @Test
  public void testBatchInsert() throws SQLException, InterruptedException {

    try {
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

  @Test
  public void testDelete() throws SQLException {


    for (int i = 2000; i < recordCount; i++) {
      PreparedStatement stmt = conn.prepareStatement("insert into persons (id, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?)");
      stmt.setLong(1, i);
      stmt.setString(2, "933-28-" + i);
      stmt.setString(3, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
      stmt.setBoolean(4, false);
      stmt.setString(5, "m");
      assertEquals(stmt.executeUpdate(), 1);
      ids.add((long) i);
    }

    //test remove
    PreparedStatement stmt;
    for (int i = 2000; i < recordCount; i++) {
      //if (i % 2 == 0) {
      //test jdbc remove

      stmt = conn.prepareStatement("delete from persons where id=?");
      stmt.setLong(1, i);
      assertEquals(1, stmt.executeUpdate());

    }


    for (int i = 2000; i < recordCount; i++) {
      stmt = conn.prepareStatement("select * from persons where id=" + i);
      ResultSet ret = stmt.executeQuery();
      assertFalse(ret.next());
    }

  }


  @Test(enabled = false)
  public void testPaging() throws Exception {
    String configStr = StreamUtils.inputStreamToString(new BufferedInputStream(new FileInputStream("config/config-4-servers-large.json")));
    final JsonDict config = new JsonDict(configStr);

    FileUtils.deleteDirectory(new File("/data/database"));

    DatabaseServer.getServers().clear();

    final DatabaseServer[] dbServers = new DatabaseServer[4];
    ThreadPoolExecutor executor = new ThreadPoolExecutor(32, 32, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
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


    DatabaseClient client = new DatabaseClient("localhost", 9010,-1, -1, true);

    Class.forName("com.sonicbase.jdbcdriver.Driver");

    Connection conn = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9000", "user", "password");

    PreparedStatement stmt = conn.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create index socialSecurityNumber on persons(socialSecurityNumber)");
    stmt.executeUpdate();

    List<Long> ids = new ArrayList<>();

    //test insert
    int recordCount = 2000;

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

    client.beginRebalance("test", "persons", "_primarykey");

    while (true) {
      if (client.isRepartitioningComplete("test")) {
        break;
      }
      Thread.sleep(1000);
    }

    System.out.println(client.getPartitionSize("test", 0, "persons", "_primarykey"));
    System.out.println(client.getPartitionSize("test", 1, "persons", "_primarykey"));
//    System.out.println(client.getPartitionSize(2, "persons", "_primarykey"));
//    System.out.println(client.getPartitionSize(3, "persons", "_primarykey"));


//    stmt = conn.prepareStatement("select * from persons where id = 0");
//    ResultSet ret = stmt.executeQuery();

    //test select returns multiple records with an index using operator '<='
    stmt = conn.prepareStatement("select * from persons where id>=0");
    ResultSet ret = stmt.executeQuery();

    for (int i = 0; i < recordCount; i++) {
      ret.next();
      try {
        assertEquals(ret.getLong("id"), i);
      }
      catch (Exception e) {
        System.out.println("currId=" + i);
        throw e;
      }
    }

    executor.shutdownNow();
  }

  @Test(enabled = false)
  public void testMultiValue() throws Exception {
    String configStr = StreamUtils.inputStreamToString(new BufferedInputStream(new FileInputStream("config/config-4-servers-large.json")));
    final JsonDict config = new JsonDict(configStr);

    FileUtils.deleteDirectory(new File("/data/database"));

    DatabaseServer.getServers().clear();

    final DatabaseServer[] dbServers = new DatabaseServer[4];
    ThreadPoolExecutor executor = new ThreadPoolExecutor(32, 32, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
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


    DatabaseClient client = new DatabaseClient("localhost", 9010, -1, -1, true);

    Class.forName("com.sonicbase.jdbcdriver.Driver");

    Connection conn = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9000", "user", "password");

    PreparedStatement stmt = conn.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create index socialSecurityNumber on persons(socialSecurityNumber)");
    stmt.executeUpdate();

    List<Long> ids = new ArrayList<>();

    //test insert
    int recordCount = 20000;

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into persons (id, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?)");
      stmt.setLong(1, i);
      stmt.setString(2, "933-28-" + (i % 4));
      stmt.setString(3, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
      stmt.setBoolean(4, false);
      stmt.setString(5, "m");
      assertEquals(stmt.executeUpdate(), 1);
      ids.add((long) i);
    }

    client.beginRebalance("test", "persons", "_primarykey");

    while (true) {
      if (client.isRepartitioningComplete("test")) {
        break;
      }
      Thread.sleep(1000);
    }

    System.out.println(client.getPartitionSize("test", 0, "persons", "_primarykey"));
    System.out.println(client.getPartitionSize("test", 1, "persons", "_primarykey"));
    //    System.out.println(client.getPartitionSize(2, "persons", "_primarykey"));
    //    System.out.println(client.getPartitionSize(3, "persons", "_primarykey"));

    stmt = conn.prepareStatement("select * from persons where socialsecuritynumber > '933-28-' order by id asc");
    ResultSet ret = stmt.executeQuery();

    for (int i = 0; i < recordCount; i++) {
      ret.next();
      try {
        assertEquals(ret.getLong("id"), i, "index=" + i);
      }
      catch (Exception e) {
        throw new Exception("index=" + i, e);
      }
    }

    executor.shutdownNow();
  }

  @Test(enabled = false)
  public void testDescending() throws Exception {
    String configStr = StreamUtils.inputStreamToString(new BufferedInputStream(new FileInputStream("config/config-4-servers-large.json")));
    final JsonDict config = new JsonDict(configStr);

    FileUtils.deleteDirectory(new File("/data/database"));

    DatabaseServer.getServers().clear();

    final DatabaseServer[] dbServers = new DatabaseServer[4];
    ThreadPoolExecutor executor = new ThreadPoolExecutor(32, 32, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
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


    DatabaseClient client = new DatabaseClient("localhost", 9010, -1, -1, true);

    Class.forName("com.sonicbase.jdbcdriver.Driver");

    Connection conn = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9000", "user", "password");

    PreparedStatement stmt = conn.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create index socialSecurityNumber on persons(socialSecurityNumber)");
    stmt.executeUpdate();

    List<Long> ids = new ArrayList<>();

    //test insert
    int recordCount = 20000;

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into persons (id, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?)");
      stmt.setLong(1, i);
      stmt.setString(2, "933-28-" + (i % 4));
      stmt.setString(3, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
      stmt.setBoolean(4, false);
      stmt.setString(5, "m");
      assertEquals(stmt.executeUpdate(), 1);
      ids.add((long) i);
    }

    client.beginRebalance("test", "persons", "_primarykey");

    while (true) {
      if (client.isRepartitioningComplete("test")) {
        break;
      }
      Thread.sleep(1000);
    }

    System.out.println(client.getPartitionSize("test", 0, "persons", "_primarykey"));
    System.out.println(client.getPartitionSize("test", 1, "persons", "_primarykey"));
    //    System.out.println(client.getPartitionSize(2, "persons", "_primarykey"));
    //    System.out.println(client.getPartitionSize(3, "persons", "_primarykey"));


//    stmt = conn.prepareStatement("select * from persons where id = 0");
//    ResultSet ret = stmt.executeQuery();

    stmt = conn.prepareStatement("select * from persons where id <= 19999 order by id asc");
    ResultSet ret = stmt.executeQuery();

    for (int i = 0; i < recordCount; i++) {
      ret.next();
      try {
        assertEquals(ret.getLong("id"), i, "index=" + i);
      }
      catch (Exception e) {
        throw new Exception("index=" + i, e);
      }
    }

    stmt = conn.prepareStatement("select * from persons where id < 20000 order by id desc");
    ret = stmt.executeQuery();

    for (int i = recordCount - 1; i >= 1; i--) {
      ret.next();
      try {
        assertEquals(ret.getLong("id"), i, "index=" + i);
      }
      catch (Exception e) {
        throw new Exception("index=" + i, e);
      }
    }


    stmt = conn.prepareStatement("select * from persons where id < 20000 order by id asc");
    ret = stmt.executeQuery();

    for (int i = 0; i < recordCount; i++) {
      ret.next();
      try {
        assertEquals(ret.getLong("id"), i, "index=" + i);
      }
      catch (Exception e) {
        throw new Exception("index=" + i, e);
      }
    }

    stmt = conn.prepareStatement("select * from persons where id > 0 order by id desc");
    ret = stmt.executeQuery();

    for (int i = recordCount - 1; i >= 1; i--) {
      ret.next();
      try {
        assertEquals(ret.getLong("id"), i, "index=" + i);
      }
      catch (Exception e) {
        throw new Exception("index=" + i, e);
      }
    }

    stmt = conn.prepareStatement("select * from persons where id > 0 order by id asc");
    ret = stmt.executeQuery();

    for (int i = 1; i < recordCount; i++) {
      ret.next();
      try {
        assertEquals(ret.getLong("id"), i, "index=" + i);
      }
      catch (Exception e) {
        throw new Exception("index=" + i, e);
      }
    }

    executor.shutdownNow();
  }


  @Test
  public void testTruncate() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("create table ToTruncate (id BIGINT, id2 BIGINT, PRIMARY KEY (id))");
    stmt.executeUpdate();

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into ToTruncate (id, id2) VALUES (?, ?)");
      stmt.setLong(1, i);
      stmt.setLong(2, i * 2);
      assertEquals(stmt.executeUpdate(), 1);
    }

    stmt = conn.prepareStatement("select * from ToTruncate where id > 0");
    ResultSet rs = stmt.executeQuery();
    assertTrue(rs.next());
    assertEquals(rs.getLong("id"), 1);

    PreparedStatement stmt2 = conn.prepareStatement("truncate table ToTruncate");
    assertTrue(stmt2.execute());

    rs = stmt.executeQuery();
    assertFalse(rs.next());
  }

  @Test
  public void testDeleteNoPrimaryKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("create table ToDeleteNoPrimarykey (id BIGINT, id2 BIGINT)");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("insert into ToDeleteNoPrimaryKey (id, id2) VALUES (?, ?)");
    stmt.setLong(1, 0);
    stmt.setLong(2, 0 * 2);
    assertEquals(stmt.executeUpdate(), 1);

    stmt = conn.prepareStatement("select * from ToDeleteNoPrimaryKey where id = 0");
    ResultSet rs = stmt.executeQuery();
    assertTrue(rs.next());
    assertFalse(rs.next());

    PreparedStatement stmt2 = conn.prepareStatement("delete from ToDeleteNoPrimaryKey where id=0");
    assertTrue(stmt2.execute());

    stmt = conn.prepareStatement("select * from ToDeleteNoPrimaryKey where id = 0");
    rs = stmt.executeQuery();
    assertFalse(rs.next());
  }

  @Test
  public void testDropTable() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("create table ToDrop (id BIGINT, id2 BIGINT, PRIMARY KEY (id))");
    stmt.executeUpdate();

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into ToDrop (id, id2) VALUES (?, ?)");
      stmt.setLong(1, i);
      stmt.setLong(2, i * 2);
      assertEquals(stmt.executeUpdate(), 1);
    }

    stmt = conn.prepareStatement("select * from ToDrop where id > 0");
    ResultSet rs = stmt.executeQuery();
    assertTrue(rs.next());
    assertEquals(rs.getLong("id"), 1);

    PreparedStatement stmt2 = conn.prepareStatement("drop table ToDrop");
    assertTrue(stmt2.execute());

    try {
      rs = stmt.executeQuery();
      fail();
    }
    catch (SQLException e) {
      //expected
    }

    try {
      stmt = conn.prepareStatement("insert into ToDrop (id, id2) VALUES (?, ?)");
      stmt.setLong(1, 0);
      stmt.setLong(2, 0 * 2);
      assertEquals(stmt.executeUpdate(), 0);
      fail();
    }
    catch (SQLException e) {
      //expected
    }
  }

  static String algorithm = "DESede";

  public static void xmain(String[] args) throws Exception {

    SecretKey symKey = KeyGenerator.getInstance(algorithm).generateKey();
    symKey = new SecretKeySpec(Base64.decode(DatabaseServer.LICENSE_KEY), algorithm);

    System.out.println("key=" + new String(Base64.encode(symKey.getEncoded()), "utf-8"));

    Cipher c = Cipher.getInstance(algorithm);

    byte[] encryptionBytes = encryptF("sonicbase:pro:4", symKey, c);

    System.out.println("encrypted: " + new String(new Hex().encode(encryptionBytes), "utf-8"));
    System.out.println("Decrypted: " + decryptF(encryptionBytes, symKey, c));

    symKey = new SecretKeySpec(Base64.decode(DatabaseServer.LICENSE_KEY), algorithm);
    System.out.println("Decrypted: " + decryptF(encryptionBytes, symKey, c));

  }

  private static byte[] encryptF(String input, Key pkey, Cipher c) throws InvalidKeyException, BadPaddingException,

      IllegalBlockSizeException {

    c.init(Cipher.ENCRYPT_MODE, pkey);

    byte[] inputBytes = input.getBytes();
    return c.doFinal(inputBytes);
  }

  private static String decryptF(byte[] encryptionBytes, Key pkey, Cipher c) throws InvalidKeyException,
      BadPaddingException, IllegalBlockSizeException {
    c.init(Cipher.DECRYPT_MODE, pkey);
    byte[] decrypt = c.doFinal(encryptionBytes);
    String decrypted = new String(decrypt);

    return decrypted;
  }


  @Test
   public void testExplain() throws SQLException {
     //client.setPageSize(100);
     PreparedStatement stmt = conn.prepareStatement("explain select * from persons where id < 100");
     ResultSet ret = stmt.executeQuery();

     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

    stmt = conn.prepareStatement(
         "select persons.id, persons.socialsecuritynumber, memberships.personId, memberships.membershipname from persons " +
             "right outer join Memberships on persons.id = Memberships.PersonId where persons.id<1000 order by persons.id desc");
     ret = stmt.executeQuery();
     while (ret.next()) {
       System.out.println(ret.getString(1));
     }

     stmt = conn.prepareStatement("explain select * from persons where id < 100 and id > 10");
     ret = stmt.executeQuery();

     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement("explain select * from persons where id < 100 or id > 10");
     ret = stmt.executeQuery();

     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement("explain select * from persons where id < 100 and id > 10 and id2 > 10");
     ret = stmt.executeQuery();

     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement("explain select * from persons where id < 100 and id > 10 and id2 > 10 and id2 < 1");
     ret = stmt.executeQuery();

     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement("explain select * from persons where id < 100 and id > 10 and id > 10 and id < 1");
     ret = stmt.executeQuery();

     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement("explain select * from persons where id2 < 100");
     ret = stmt.executeQuery();

     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement("explain select personId, membershipname from memberships where personid=1 and membershipname='name'");
     ret = stmt.executeQuery();

     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();


     stmt = conn.prepareStatement("explain select max(id) as maxValue from persons");
     ret = stmt.executeQuery();

     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement("explain select max(id) as maxValue from persons where id2 < 1");
     ret = stmt.executeQuery();

     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement("explain select max(id) as maxValue from persons where id < 100");
     ret = stmt.executeQuery();

     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement("explain select sum(id) as sumValue from persons");
     ret = stmt.executeQuery();

     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement("explain select * from persons where id < 100 and id > 200 limit 3");
     ret = stmt.executeQuery();

     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement("explain select * from persons where id < 100 and id > 200 limit 3 offset 2");
     ret = stmt.executeQuery();

     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement("explain select * from persons order by id2 asc, id desc");
     ret = stmt.executeQuery();

     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement("explain select persons.id  from persons where persons.id>=100 AND id < 100AND ID2=0 OR id> 6 AND ID < 100");
     ret = stmt.executeQuery();

     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement("explain select * from persons where id>105 and id2=0 or id<105 and id2=1 order by id desc");
     ret = stmt.executeQuery();

     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement("explain select persons.id from persons where persons.id>2 AND id < 100 OR id> 6 AND ID < 200");
     ret = stmt.executeQuery();

     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement("explain select * from persons where id2=1 order by id desc");
     ret = stmt.executeQuery();

     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement("explain select * from persons where id in (0, 1, 2, 3, 4)");
     ret = stmt.executeQuery();

     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement("explain select * from persons where socialSecurityNumber='555'");
     ret = stmt.executeQuery();

     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement("explain select * from persons where id2=1 or id2=0 order by id2 asc, id desc");
     ret = stmt.executeQuery();

     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement("explain select * from persons where id=0 OR id=1 OR id=2 OR id=3 OR id=4");
     ret = stmt.executeQuery();

     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();


     stmt = conn.prepareStatement("explain select persons.id, socialsecuritynumber, memberships.personid  " +
         "from persons inner join Memberships on persons.id = Memberships.personId inner join resorts on memberships.resortid = resorts.resortid" +
         " where persons.id<1000");
     ret = stmt.executeQuery();
     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement(
         "explain select persons.id, persons.id2, persons.socialsecuritynumber, memberships.personId, memberships.personid, memberships.membershipname from persons " +
             " inner join Memberships on persons.id = Memberships.PersonId and memberships.personid = persons.id2  where persons.id > 0 order by persons.id desc");
     ret = stmt.executeQuery();
     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement(
         "explain select persons.id, persons.socialsecuritynumber, memberships.membershipname, memberships.personid from memberships " +
             "left outer join persons on persons.id = Memberships.PersonId where memberships.personid<1000 order by memberships.personid desc");
     ret = stmt.executeQuery();
     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement(
         "explain select persons.id, persons.socialsecuritynumber, memberships.membershipname from persons " +
             " inner join Memberships on Memberships.PersonId = persons.id and memberships.personid < 1000 where persons.id > 0 order by persons.id desc");
     ret = stmt.executeQuery();
     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement(
         "select persons.id, persons.socialsecuritynumber, memberships.personId, memberships.membershipname from persons " +
             "right outer join Memberships on persons.id = Memberships.PersonId where persons.id<1000 order by persons.id desc");
     ret = stmt.executeQuery();
     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();
   }
}

