package com.sonicbase.accept.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.ComObject;
import com.sonicbase.common.Config;
import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.embedded.EmbeddedDatabase;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.query.BinaryExpression;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.query.impl.ColumnImpl;
import com.sonicbase.query.impl.ExpressionImpl;
import com.sonicbase.query.impl.IndexLookup;
import com.sonicbase.query.impl.SelectContextImpl;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.server.DatabaseServer;
import com.sonicbase.server.ProServer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.yaml.snakeyaml.Yaml;

import javax.crypto.*;
import java.io.BufferedInputStream;
import java.io.File;
import java.security.InvalidKeyException;
import java.security.Key;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.sonicbase.client.InsertStatementHandler.BATCH_STATUS_FAILED;
import static com.sonicbase.client.InsertStatementHandler.BATCH_STATUS_SUCCCESS;
import static org.testng.Assert.*;

public class TestDatabase {

  private Connection clusterConn;
  private final int recordCount = 10;
  final List<Long> ids = new ArrayList<>();

  DatabaseClient clusterClient = null;
  DatabaseServer[] dbServers;
  private EmbeddedDatabase embedded;
  private Connection embeddedConn;
  private DatabaseClient embeddedClient;

  @AfterClass(alwaysRun = true)
  public void afterClass() throws SQLException {
    clusterConn.close();
    embeddedConn.close();
    
    embedded.shutdown();

    for (DatabaseServer server : dbServers) {
      server.shutdown();
    }

    System.out.println("client refCount=" + DatabaseClient.getClientRefCount().get() + ", sharedClients=" + DatabaseClient.getSharedClients().size() + ", class=TestDatabase");
    for (DatabaseClient client : DatabaseClient.getAllClients()) {
      System.out.println("Stack:\n" + client.getAllocatedStack());
    }
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    try {
      System.setProperty("log4j.configuration", "test-log4j.xml");
//      System.setProperty("log4j.configuration", "/Users/lowryda/Dropbox/git/sonicbase/db/src/main/resources/log4j.xml");

      String configStr = IOUtils.toString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-4-servers.yaml")), "utf-8");
      Config config = new Config(configStr);

      FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db-data"));

      DatabaseClient.getServers().clear();

      dbServers = new DatabaseServer[4];
      ThreadPoolExecutor executor = new ThreadPoolExecutor(32, 32, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

      String role = "primaryMaster";

      List<Future> futures = new ArrayList<>();
      for (int i = 0; i < dbServers.length; i++) {
        //      futures.add(executor.submit(new Callable() {
        //        @Override
        //        public Object call(Connection conn, DatabaseClient client) throws Exception {
        //          String role = "primaryMaster";

        dbServers[i] = new DatabaseServer();
        dbServers[i].setConfig(config, "4-servers", "localhost", 9010 + (50 * i), true, new AtomicBoolean(true), new AtomicBoolean(true),null, false);
        dbServers[i].setRole(role);

//        if (shard == 0) {
//          Map<Integer, Object> map = new HashMap<>();
//          DatabaseClient.getServers2().put(0, map);
//          map.put(0, dbServers[shard]);
//        }
//        if (shard == 1) {
//          DatabaseClient.getServers2().get(0).put(1, dbServers[shard]);
//        }
//        if (shard == 2) {
//          Map<Integer, Object> map = new HashMap<>();
//          DatabaseClient.getServers2().put(1, map);
//          map.put(0, dbServers[shard]);
//        }
//        if (shard == 3) {
//          DatabaseClient.getServers2().get(1).put(1, dbServers[shard]);
//        }
        //          return null;
        //        }
        //      }));
      }
      for (Future future : futures) {
        future.get();
      }

      DatabaseServer.initDeathOverride(2, 2);
      DatabaseServer.getDeathOverride()[0][0] = false;
      DatabaseServer.getDeathOverride()[0][1] = false;
      DatabaseServer.getDeathOverride()[1][0] = false;
      DatabaseServer.getDeathOverride()[1][1] = false;

      dbServers[0].enableSnapshot(false);
      dbServers[1].enableSnapshot(false);
      dbServers[2].enableSnapshot(false);
      dbServers[3].enableSnapshot(false);


      Thread.sleep(5000);

      //DatabaseClient client = new DatabaseClient("localhost", 9010, true);

      Class.forName("com.sonicbase.jdbcdriver.Driver");

      clusterConn = DriverManager.getConnection("jdbc:sonicbase:localhost:9010", "user", "password");
      clusterClient = ((ConnectionProxy) clusterConn).getDatabaseClient();

      ((ConnectionProxy) clusterConn).getDatabaseClient().createDatabase("test");

      clusterConn.close();

      clusterConn = DriverManager.getConnection("jdbc:sonicbase:localhost:9010/test", "user", "password");

      clusterClient = ((ConnectionProxy) clusterConn).getDatabaseClient();
      clusterClient.setPageSize(3);

      embedded = new EmbeddedDatabase();
      embedded.enableDurability(System.getProperty("user.home") + "/db-data.embedded");
      embedded.setUseUnsafe(false);
      embedded.purge();
      embedded.start();
      embedded.createDatabaseIfNeeded("test");
      embeddedConn = embedded.getConnection("test");
      embeddedClient = ((ConnectionProxy)embeddedConn).getDatabaseClient();
      
      initDatabase(clusterConn);
      initDatabase(embeddedConn);
      
//      IndexSchema indexSchema = null;
//      for (Map.Entry<String, IndexSchema> entry : client.getCommon().getTables("test").get("persons").getIndices().entrySet()) {
//        if (entry.getValue().getFields()[0].equalsIgnoreCase("socialsecuritynumber")) {
//          indexSchema = entry.getValue();
//        }
//      }
//      List<ColumnImpl> columns = new ArrayList<>();
//      columns.add(new ColumnImpl(null, null, "persons", "socialsecuritynumber", null));

  //rebalance
      for (DatabaseServer server : dbServers) {
        server.shutdownRepartitioner();
      }

//      long size = client.getPartitionSize("test", 0, "children", "_1_socialsecuritynumber");
//      assertEquals(size, 10);

      clusterClient.beginRebalance("test");


      while (true) {
        if (clusterClient.isRepartitioningComplete("test")) {
          break;
        }
        Thread.sleep(1000);
      }

      for (DatabaseServer server : dbServers) {
        server.shutdownRepartitioner();
      }

      //Thread.sleep(60000);

      dbServers[0].runSnapshot();
      dbServers[1].runSnapshot();
      dbServers[2].runSnapshot();
      dbServers[3].runSnapshot();

      ObjectMapper mapper = new ObjectMapper();
      Map<String, Object> backupConfig = new Yaml().loadAs(
          "type: fileSystem\n" +
              "directory: $HOME/db/backup\n" +
              "period: daily\n" +
              "time: 23:00\n" +
              "maxBackupCount: 10\n" +
              "sharedDirectory: true\n", Map.class);

//          "    \"type\" : \"fileSystem\",\n" +
//          "    \"directory\": \"$HOME/db/backup\",\n" +
//          "    \"period\": \"daily\",\n" +
//          "    \"time\": \"23:00\",\n" +
//          "    \"maxBackupCount\": 10,\n" +
//          "    \"sharedDirectory\": true\n" +
//          "  }");

      for (DatabaseServer dbServer : dbServers) {
        ((ProServer)dbServer.getProServer()).getBackupManager().setBackupConfig(backupConfig);
      }

      clusterClient.syncSchema();

      for (Map.Entry<String, TableSchema> entry : clusterClient.getCommon().getTables("test").entrySet()) {
        for (Map.Entry<String, IndexSchema> indexEntry : entry.getValue().getIndices().entrySet()) {
          Object[] upperKey = indexEntry.getValue().getCurrPartitions()[0].getUpperKey();
          System.out.println("table=" + entry.getKey() + ", index=" + indexEntry.getKey() + ", upper=" + (upperKey == null ? null : DatabaseCommon.keyToString(upperKey)));
        }
      }

      File dir = new File(System.getProperty("user.home"), "/db/backup");
      FileUtils.deleteDirectory(dir);
      dir.mkdirs();

      clusterClient.startBackup();
      while (true) {
        Thread.sleep(1000);
        if (clusterClient.isBackupComplete()) {
          break;
        }
      }

      Thread.sleep(5000);

      File file = new File(System.getProperty("user.home"), "/db/backup");
      File[] dirs = file.listFiles();

      clusterClient.startRestore(dirs[0].getName());
      while (true) {
        Thread.sleep(1000);
        if (clusterClient.isRestoreComplete()) {
          break;
        }
      }
      dbServers[0].enableSnapshot(false);
      dbServers[1].enableSnapshot(false);
      dbServers[2].enableSnapshot(false);
      dbServers[3].enableSnapshot(false);

      clusterClient.syncSchema();
      for (Map.Entry<String, TableSchema> entry : clusterClient.getCommon().getTables("test").entrySet()) {
        for (Map.Entry<String, IndexSchema> indexEntry : entry.getValue().getIndices().entrySet()) {
          Object[] upperKey = indexEntry.getValue().getCurrPartitions()[0].getUpperKey();
          System.out.println("table=" + entry.getKey() + ", index=" + indexEntry.getKey() + ", upper=" + (upperKey == null ? null : DatabaseCommon.keyToString(upperKey)));
        }
      }

      for (DatabaseServer server : dbServers) {
        server.shutdownRepartitioner();
      }

      ComObject cobj = new ComObject(3);
      cobj.put(ComObject.Tag.DB_NAME, "test");
      cobj.put(ComObject.Tag.SCHEMA_VERSION, clusterClient.getCommon().getSchemaVersion());
      cobj.put(ComObject.Tag.METHOD, "DeleteManager:forceDeletes");
      clusterClient.sendToAllShards(null, 0, cobj, DatabaseClient.Replica.ALL);

        // Thread.sleep(10000);
      executor.shutdownNow();
    }
    catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  private void initDatabase(Connection conn) throws SQLException, InterruptedException {
    PreparedStatement stmt = conn.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, id3 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create table Children (parent BIGINT, socialSecurityNumber VARCHAR(20), bio VARCHAR(256))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create table Memberships (personId BIGINT, membershipName VARCHAR(20), resortId BIGINT, PRIMARY KEY (personId, membershipName))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create table Resorts (resortId BIGINT, resortName VARCHAR(20), PRIMARY KEY (resortId))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create table nokey (id BIGINT, id2 BIGINT)");
    stmt.executeUpdate();
//
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
      stmt = conn.prepareStatement("insert into persons (id, socialSecurityNumber, relatives, restricted, gender, id3) VALUES (?, ?, ?, ?, ?, ?)");
      stmt.setLong(1, i);
      stmt.setString(2, "933-28-" + i);
      stmt.setString(3, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
      stmt.setBoolean(4, false);
      stmt.setString(5, "m");
      stmt.setLong(6, i + 1000);
      assertEquals(stmt.executeUpdate(), 1);
      ids.add((long) i);
    }

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert ignore into persons (id, socialSecurityNumber, relatives, restricted, gender, id3) VALUES (?, ?, ?, ?, ?, ?)");
      stmt.setLong(1, i);
      stmt.setString(2, "933-28-" + i);
      stmt.setString(3, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
      stmt.setBoolean(4, false);
      stmt.setString(5, "m");
      stmt.setLong(6, i + 1000);
      assertEquals(stmt.executeUpdate(), 1);
      ids.add((long) i);
    }

    try {
      for (int i = 0; i < recordCount; i++) {
        stmt = conn.prepareStatement("insert into persons (id, socialSecurityNumber, relatives, restricted, gender, id3) VALUES (?, ?, ?, ?, ?, ?)");
        stmt.setLong(1, i);
        stmt.setString(2, "933-28-" + i);
        stmt.setString(3, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
        stmt.setBoolean(4, false);
        stmt.setString(5, "m");
        stmt.setLong(6, i + 1000);
        assertEquals(stmt.executeUpdate(), 1);
        ids.add((long) i);
      }
    }
    catch (Exception e) {
      //e.printStackTrace();
      assertTrue(ExceptionUtils.getStackTrace(e).contains("Unique constraint violated"));
    }

    stmt = conn.prepareStatement("insert ignore into persons (id, socialSecurityNumber, relatives, restricted, gender, id3) VALUES (?, ?, ?, ?, ?, ?)");
    for (int i = 0; i < recordCount; i++) {
      stmt.setLong(1, i);
      stmt.setString(2, "933-28-" + i);
      stmt.setString(3, "updated value");
      stmt.setBoolean(4, false);
      stmt.setString(5, "m");
      stmt.setLong(6, i + 1000);
      ids.add((long) i);
      stmt.addBatch();
    }
    int[] batchRet = stmt.executeBatch();
    for (int i = 0; i < recordCount; i++) {
      assertEquals(batchRet[i], BATCH_STATUS_SUCCCESS);
    }

    stmt = conn.prepareStatement("select * from persons where id=0");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getString("relatives"), "updated value");

    stmt = conn.prepareStatement("insert into persons (id, socialSecurityNumber, relatives, restricted, gender, id3) VALUES (?, ?, ?, ?, ?, ?)");
    for (int i = 0; i < recordCount; i++) {
      stmt.setLong(1, i);
      stmt.setString(2, "933-28-" + i);
      stmt.setString(3, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
      stmt.setBoolean(4, false);
      stmt.setString(5, "m");
      stmt.setLong(6, i + 1000);
      ids.add((long) i);
      stmt.addBatch();
    }
    batchRet = stmt.executeBatch();
    for (int i = 0; i < recordCount; i++) {
      assertEquals(batchRet[i], BATCH_STATUS_FAILED);
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
//
//      stmt = conn.prepareStatement("create index socialSecurityNumber on children(socialSecurityNumber)");
//      stmt.executeUpdate();

    //create index ssn2 on persons(socialSecurityNumber)
    //    stmt = conn.prepareStatement("create index ssn on persons(socialSecurityNumber)");
    //    stmt.executeUpdate();

    while (true) {
      ComObject cobj = new ComObject(1);
      cobj.put(ComObject.Tag.METHOD, "DatabaseServer:areAllLongRunningCommandsComplete");
      byte[] bytes = ((ConnectionProxy) conn).getDatabaseClient().sendToMaster(cobj);
      ComObject retObj = new ComObject(bytes);
      if (retObj.getBoolean(ComObject.Tag.IS_COMPLETE)) {
        break;
      }
      Thread.sleep(1000);
    }

  }

  @DataProvider
  public Object[][] connections() {
    return new Object[][]{
        {clusterConn, clusterClient},
        {embeddedConn, embeddedClient}
    };
  }

  @Test(dataProvider = "connections")
  public void testSecondaryIndexWithNoKeyExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(
        "explain select * from children where socialsecuritynumber = '933-28-4'")) {
      try (ResultSet rs = stmt.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Table scan: table=children children.socialsecuritynumber = 933-28-4\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testSecondaryIndexWithNoKey(Connection conn, DatabaseClient client) throws SQLException {

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

  @Test(dataProvider = "connections")
  public void testIndex(Connection conn, DatabaseClient client) {
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

  @Test(dataProvider = "connections")
  public void testDropIndex(Connection conn, DatabaseClient client) throws SQLException, InterruptedException {

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

    ExpressionImpl expressionImpl = new ExpressionImpl();
    expressionImpl.setDbName("test");
    expressionImpl.setTableName("persons");
    expressionImpl.setClient(client);
    expressionImpl.setReplica(0);
    expressionImpl.setForceSelectOnServer(false);
    expressionImpl.setParms(parms);
    expressionImpl.setColumns(columns);
    expressionImpl.setNextShard(-1);
    expressionImpl.setRecordCache(recordCache);
    expressionImpl.setViewVersion(client.getCommon().getSchemaVersion());
    expressionImpl.setCounters(null);
    expressionImpl.setGroupByContext(null);
    expressionImpl.setIsProbe(false);
    expressionImpl.setRestrictToThisServer(false);
    expressionImpl.setProcedureContext(null);


    IndexLookup indexLookup = new IndexLookup();
    indexLookup.setCount(1000);
    indexLookup.setIndexName(indexSchema.getName());
    indexLookup.setLeftOp(BinaryExpression.Operator.EQUAL);
    indexLookup.setLeftKey(new Object[]{"933-28-0".getBytes()});
    indexLookup.setLeftOriginalKey(new Object[]{"933-28-0".getBytes()});
    indexLookup.setColumnName("socialsecuritynumber");
    indexLookup.setSchemaRetryCount(0);
    indexLookup.setUsedIndex(usedIndex);
    indexLookup.setEvaluateExpression(false);

    SelectContextImpl ret = indexLookup.lookup(expressionImpl, expressionImpl);

    assertEquals(ret.getCurrKeys().length, 4);

  }

  @Test(dataProvider = "connections")
  public void testUniqueIndex(Connection conn, DatabaseClient client) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("create table indexes (id BIGINT, id2 BIGINT)");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create unique index uniqueIndex2 on indexes(id, id2)");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create unique index uniqueIndex1 on indexes(id)");
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

  @Test(dataProvider = "connections")
  public void testDualKey(Connection conn, DatabaseClient client) throws SQLException {
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

  @Test(dataProvider = "connections")
  public void testAddColumnExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id, id2, id3 from addColumn where id >= 0")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for relational op: table=addcolumn, idx=_primarykey, addcolumn.id >= 0\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testAddColumn(Connection conn, DatabaseClient client) throws SQLException {

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

  @Test(dataProvider = "connections")
  public void testSchema(Connection conn, DatabaseClient client) throws Exception {
//    DatabaseCommon common = new DatabaseCommon();
//    common.addDatabase("test");
//    TableSchema tableSchema = new TableSchema();
//    tableSchema.setName("table1");
//    List<String> primaryKey = new ArrayList<>();
//    primaryKey.add("id");
//    tableSchema.setPrimaryKey(primaryKey);
//    common.addTable(client, "test", DatabaseClient.getServers().get(0).get(0).getDataDir(), tableSchema);
//    common.saveSchema(client, DatabaseClient.getServers().get(0).get(0).getDataDir());
//
//    common.getTables("test").clear();
//    common.loadSchema(DatabaseClient.getServers().get(0).get(0).getDataDir());
//
//    assertEquals(common.getTables("test").size(), 1);
  }

  @Test(dataProvider = "connections")
  public void testMath(Connection conn, DatabaseClient client) throws Exception {

    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from persons where id3 = id + 1000")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Table scan: table=persons persons.id3 = persons.id + 1000\n");
      }
    }

    PreparedStatement stmt = conn.prepareStatement("select * from persons where id3 = id + 1000");
    ResultSet ret = stmt.executeQuery();
    assertTrue(ret.next());
    assertEquals(ret.getInt("id3"), 1000);
    assertEquals(ret.getInt("id"), 0);
  }

  @Test(dataProvider = "connections")
  public void testLessExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from persons where id<106 and id>100 order by id2 asc, id desc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to server sort\n" +
            "Two key index lookup: table=persons, idx=_primarykey, id > 100 and id < 106\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testLess(Connection conn, DatabaseClient client) throws Exception {
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

  @Test(dataProvider = "connections")
  public void testLessNoKeySecondaryIndexExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from nokeysecondaryindex where id<10 and id>7 order by id2 asc, id desc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to server sort\n" +
            "Two key index lookup: table=nokeysecondaryindex, idx=id, id > 7 and id < 10\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testLessNoKeySecondaryIndex(Connection conn, DatabaseClient client) throws Exception {

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

  @Test(dataProvider = "connections")
  public void testLessNoKeyExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from nokey where id<10 and id>7 order by id2 asc, id desc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to server sort\n" +
            "Table scan: table=nokey nokey.id < 10 and nokey.id > 7\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testLessNoKey(Connection conn, DatabaseClient client) throws Exception {
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

  @Test(dataProvider = "connections")
  public void testBasicsNoKeySecondaryIndexExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id, id2 from nokeysecondaryIndex where id<5 order by id desc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for relational op: table=nokeysecondaryindex, idx=id, nokeysecondaryindex.id < 5\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testBasicsNoKeySecondaryIndex(Connection conn, DatabaseClient client) throws Exception {
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

  @Test(dataProvider = "connections")
  public void testBasicsNoKeyExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id, id2 from nokey where id<5 order by id desc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to server sort\n" +
            "Table scan: table=nokey nokey.id < 5\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testBasicsNoKey(Connection conn, DatabaseClient client) throws Exception {
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

  @Test(dataProvider = "connections")
  public void testBasicsExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id, id2 from persons where id<5 order by id desc")) {
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

  @Test(dataProvider = "connections")
  public void testBasics(Connection conn, DatabaseClient client) throws Exception {

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

  @Test(dataProvider = "connections")
  public void testNotInNoKeySecondaryIndexExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from nokeysecondaryIndex where id not in (3, 4, 5, 6, 7, 8, 9, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109) order by id asc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Table scan: table=nokeysecondaryindex nokeysecondaryindex.id not in (3, 4, 5, 6, 7, 8, 9, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109)\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testNotInNoKeySecondaryIndex(Connection conn, DatabaseClient client) throws SQLException {
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

  @Test(dataProvider = "connections")
  public void testNotInNoKeyExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from nokey where id not in (3, 4, 5, 6, 7, 8, 9, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109) order by id asc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to server sort\n" +
            "Table scan: table=nokey nokey.id not in (3, 4, 5, 6, 7, 8, 9, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109)\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testNotInNoKey(Connection conn, DatabaseClient client) throws SQLException {
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

  @Test(dataProvider = "connections")
  public void testNotInExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from persons where id not in (5, 6, 7, 8, 9, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109) order by id asc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Table scan: table=persons persons.id not in (5, 6, 7, 8, 9, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109)\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testNotIn(Connection conn, DatabaseClient client) throws SQLException {
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

  @Test(dataProvider = "connections")
  public void testIdentityNoKeySecondaryIndexExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from nokeysecondaryindex where id = 5")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for relational op: table=nokeysecondaryindex, idx=id, nokeysecondaryindex.id = 5\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testIdentityNoKeySecondaryIndex(Connection conn, DatabaseClient client) throws SQLException {
    //test select with not in expression
    PreparedStatement stmt = conn.prepareStatement("select * from nokeysecondaryindex where id = 5");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 5);
    assertEquals(ret.getLong("id2"), 10);
    assertFalse(ret.next());
  }

  @Test(dataProvider = "connections")
  public void testIdentityNoKeyExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from nokey where id = 5")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Table scan: table=nokey nokey.id = 5\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testIdentityNoKey(Connection conn, DatabaseClient client) throws SQLException {

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

  @Test(dataProvider = "connections")
  public void testIdentityExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from persons where id = 5")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for relational op: table=persons, idx=_primarykey, persons.id = 5\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testIdentity(Connection conn, DatabaseClient client) throws SQLException {
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

  @Test(dataProvider = "connections")
  public void testNoKeySecondaryIndexExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from nokeysecondaryIndex where id < 5 order by id asc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for relational op: table=nokeysecondaryindex, idx=id, nokeysecondaryindex.id < 5\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testNoKeySecondaryIndex(Connection conn, DatabaseClient client) throws SQLException {
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

  @Test(dataProvider = "connections")
  public void testNoKeyExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from nokey where id < 5 order by id asc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to server sort\n" +
            "Table scan: table=nokey nokey.id < 5\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testNoKey(Connection conn, DatabaseClient client) throws SQLException {
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

  @Test(dataProvider = "connections")
  public void testNoKeySecondaryIndex2Explain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from nokeysecondaryindex where id <= 2 and id2 = 4 order by id asc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for relational op: table=nokeysecondaryindex, idx=id, nokeysecondaryindex.id <= 2\n" +
            "single key index lookup\n" +
            " AND \n" +
            "Read record from index and evaluate: nokeysecondaryindex.id2 = 4\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testNoKeySecondaryIndex2(Connection conn, DatabaseClient client) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from nokeysecondaryindex where id <= 2 and id2 = 4 order by id asc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 2);
    assertEquals(ret.getLong("id2"), 4);
    assertFalse(ret.next());
  }

  @Test(dataProvider = "connections")
  public void testNoKey2Explain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from nokey where id <= 2 and id2 = 4 order by id asc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to server sort\n" +
            "Table scan: table=nokey nokey.id <= 2 and nokey.id2 = 4\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testNoKey2(Connection conn, DatabaseClient client) throws SQLException {
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


  @Test(dataProvider = "connections")
  public void testNotInAndNoKeySecondaryIndexExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from nokeysecondaryindex where id < 4 and id > 1 and id not in (5, 6, 7, 8, 9, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109) order by id desc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Two key index lookup: table=nokeysecondaryindex, idx=id, id > 1 and id < 4\n" +
            " AND \n" +
            "Read record from index and evaluate: nokeysecondaryindex.id not in (5, 6, 7, 8, 9, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109)\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testNotInAndNoKeySecondaryIndex(Connection conn, DatabaseClient client) throws SQLException {
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

  @Test(dataProvider = "connections")
  public void testNotInAndNoKeyExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from nokey where id < 4 and id > 1 and id not in (5, 6, 7, 8, 9, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109) order by id desc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to server sort\n" +
            "Table scan: table=nokey nokey.id < 4 and nokey.id > 1 and nokey.id not in (5, 6, 7, 8, 9, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109)\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testNotInAndNoKey(Connection conn, DatabaseClient client) throws SQLException {
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

  @Test(dataProvider = "connections")
  public void testNotInAndExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from persons where id < 4 and id > 1 and id not in (5, 6, 7, 8, 9, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109) order by id desc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Two key index lookup: table=persons, idx=_primarykey, id > 1 and id < 4\n" +
            " AND \n" +
            "Read record from index and evaluate: persons.id not in (5, 6, 7, 8, 9, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109)\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testNotInAnd(Connection conn, DatabaseClient client) throws SQLException {
    //test select with not in expression
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id < 4 and id > 1 and id not in (5, 6, 7, 8, 9, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109) order by id desc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 3);
    ret.next();
    assertEquals(ret.getLong("id"), 2);
    assertFalse(ret.next());
  }

  @Test(dataProvider = "connections")
  public void testAllNoKeySecondaryIndex(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from nokeysecondaryIndex")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for all records: table=nokeysecondaryindex, idx=_primarykey\n" +
            "single key index lookup\n");
      }
    }
    PreparedStatement stmt = conn.prepareStatement("select * from nokeysecondaryIndex");
    ResultSet ret = stmt.executeQuery();

    for (int i = 0; i < recordCount; i++) {
      ret.next();
      assertEquals(ret.getLong("id"), i);
      assertEquals(ret.getLong("id2"), i * 2);
    }
    assertFalse(ret.next());
  }

  @Test(dataProvider = "connections")
  public void testAllNoKey(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from nokey")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for all records: table=nokey, idx=_primarykey\n" +
            "single key index lookup\n");
      }
    }
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

  @Test(dataProvider = "connections")
  public void testAllExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from persons")) {
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

  @Test(dataProvider = "connections")
  public void testAll(Connection conn, DatabaseClient client) throws SQLException {
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

  @Test(dataProvider = "connections")
  public void testParametersNoKeySecondaryIndexExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from nokeysecondaryIndex where id < ? and id > ?")) {
      stmt2.setLong(1, 5);
      stmt2.setLong(2, 2);
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Two key index lookup: table=nokeysecondaryindex, idx=id, id > 2 and id < 5\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testParametersNoKeySecondaryIndex(Connection conn, DatabaseClient client) throws SQLException {
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

  @Test(dataProvider = "connections")
  public void testParametersNoKeyExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from nokey where id < ? and id > ?")) {
      stmt2.setLong(1, 5);
      stmt2.setLong(2, 2);
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Table scan: table=nokey nokey.id < 5 and nokey.id > 2\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testParametersNoKey(Connection conn, DatabaseClient client) throws SQLException {
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

  @Test(dataProvider = "connections")
  public void testParametersExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from persons where id < ? and id > ?")) {
      stmt2.setLong(1, 5);
      stmt2.setLong(2, 2);
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Two key index lookup: table=persons, idx=_primarykey, id > 2 and id < 5\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testParameters(Connection conn, DatabaseClient client) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id < ? and id > ?");
    stmt.setLong(1, 5);
    stmt.setLong(2, 2);
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 3);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 4);
    assertFalse(ret.next());
  }


  @Test(dataProvider = "connections")
  public void test2FieldKeyExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from memberships where personId=? and membershipName=?")) {
      stmt2.setLong(1, 0);
      stmt2.setString(2, "membership-0");
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Merged composite key index lookup: table=memberships, idx=_primarykey, ,personid,membershipname=[,0,membership-0]\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void test2FieldKey(Connection conn, DatabaseClient client) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from memberships where personId=? and membershipName=?");
    stmt.setLong(1, 0);
    stmt.setString(2, "membership-0");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("personId"), 0);
    assertEquals(ret.getString("membershipName"), "membership-0");
    assertFalse(ret.next());
  }

  @Test(dataProvider = "connections")
  public void testMaxNoKeySecondaryIndexExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select max(id) as maxValue from nokeysecondaryindex")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Evaluate counters\n" +
            "Index lookup for all records: table=nokeysecondaryindex, idx=_primarykey\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testMaxNoKeySecondaryIndex(Connection conn, DatabaseClient client) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select max(id) as maxValue from nokeysecondaryindex");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("maxValue"), 9);
    assertFalse(ret.next());
  }

  @Test(dataProvider = "connections")
  public void testMaxNoKeyExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select max(id) as maxValue from nokey")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Evaluate counters\n" +
            "Index lookup for all records: table=nokey, idx=_primarykey\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testMaxNoKey(Connection conn, DatabaseClient client) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select max(id) as maxValue from nokey");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("maxValue"), 9);
    assertFalse(ret.next());
  }

  @Test(dataProvider = "connections")
  public void testMaxExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select max(id) as maxValue from persons")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Evaluate counters\n" +
            "Index lookup for all records: table=persons, idx=_primarykey\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testMax(Connection conn, DatabaseClient client) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select max(id) as maxValue from persons");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("maxValue"), 109);
    assertFalse(ret.next());
  }

  @Test(dataProvider = "connections")
  public void testMinNoKeySecondaryIndexExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select min(id) as minValue from nokeysecondaryindex")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Evaluate counters\n" +
            "Index lookup for all records: table=nokeysecondaryindex, idx=_primarykey\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testMinNoKeySecondaryIndex(Connection conn, DatabaseClient client) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select min(id) as minValue from nokeysecondaryindex");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("minValue"), 0);
    assertFalse(ret.next());
  }

  @Test(dataProvider = "connections")
  public void testMinNoKeyExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select min(id) as minValue from nokey")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Evaluate counters\n" +
            "Index lookup for all records: table=nokey, idx=_primarykey\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testMinNoKey(Connection conn, DatabaseClient client) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select min(id) as minValue from nokey");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("minValue"), 0);
    assertFalse(ret.next());
  }

  @Test(dataProvider = "connections")
  public void testMinExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select min(id) as minValue from persons")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Evaluate counters\n" +
            "Index lookup for all records: table=persons, idx=_primarykey\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testMin(Connection conn, DatabaseClient client) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select min(id) as minValue from persons");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("minValue"), 0);
    assertEquals(ret.getLong(1), 0);
    assertEquals(ret.getString("minValue"), "0");
    assertEquals(ret.getString(1), "0");
    assertEquals(ret.getInt("minValue"), 0);
    assertEquals(ret.getInt(1), 0);
    assertEquals(ret.getFloat("minValue"), 0f);
    assertEquals(ret.getFloat(1), 0f);
    assertEquals(ret.getDouble("minValue"), 0d);
    assertEquals(ret.getDouble(1), 0d);
    assertFalse(ret.next());
  }

  @Test(dataProvider = "connections")
  public void testMaxTableScanNoKeySecondaryIndexExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select max(id) as maxValue from nokeysecondaryindex where id2 < 1")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Evaluate counters\n" +
            "Table scan: table=nokeysecondaryindex nokeysecondaryindex.id2 < 1\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testMaxTableScanNoKeySecondaryIndex(Connection conn, DatabaseClient client) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select max(id) as maxValue from nokeysecondaryindex where id2 < 1");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("maxValue"), 0);
    assertFalse(ret.next());
  }

  @Test(dataProvider = "connections")
  public void testMaxTableScanExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select max(id) as maxValue from persons where id2 < 1")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Evaluate counters\n" +
            "Table scan: table=persons persons.id2 < 1\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testMaxTableScan(Connection conn, DatabaseClient client) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select max(id) as maxValue from persons where id2 < 1");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("maxValue"), 108);
    assertFalse(ret.next());
  }

  @Test(dataProvider = "connections")
  public void testMaxWhereNoKeySecondaryIndexExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select max(id) as maxValue from nokeysecondaryindex where id < 4")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Evaluate counters\n" +
            "Table scan: table=nokeysecondaryindex nokeysecondaryindex.id < 4\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testMaxWhereNoKeySecondaryIndex(Connection conn, DatabaseClient client) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select max(id) as maxValue from nokeysecondaryindex where id < 4");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("maxValue"), 3);
    assertFalse(ret.next());
  }

  @Test(dataProvider = "connections")
  public void testMaxWhereNoKeyExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select max(id) as maxValue from nokey where id < 4")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Evaluate counters\n" +
            "Table scan: table=nokey nokey.id < 4\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testMaxWhereNoKey(Connection conn, DatabaseClient client) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select max(id) as maxValue from nokey where id < 4");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("maxValue"), 3);
    assertFalse(ret.next());
  }

  @Test(dataProvider = "connections")
  public void testMaxWhereExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select max(id) as maxValue from persons where id < 100")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Evaluate counters\n" +
            "Index lookup for relational op: table=persons, idx=_primarykey, persons.id < 100\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testMaxWhere(Connection conn, DatabaseClient client) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select max(id) as maxValue from persons where id < 100");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("maxValue"), 9);
    assertFalse(ret.next());
  }

  @Test(dataProvider = "connections")
  public void testSumNoKeySecondaryIndexExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select sum(id) as sumValue from nokeysecondaryindex")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Evaluate counters\n" +
            "Index lookup for all records: table=nokeysecondaryindex, idx=_primarykey\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testSumNoKeySecondaryIndex(Connection conn, DatabaseClient client) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select sum(id) as sumValue from nokeysecondaryindex");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("sumValue"), 45);
    assertFalse(ret.next());
  }

  @Test(dataProvider = "connections")
  public void testSumNoKeyExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select sum(id) as sumValue from nokey")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Evaluate counters\n" +
            "Index lookup for all records: table=nokey, idx=_primarykey\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testSumNoKey(Connection conn, DatabaseClient client) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select sum(id) as sumValue from nokey");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("sumValue"), 90);
    assertFalse(ret.next());
  }

  @Test(dataProvider = "connections")
  public void testSumExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select sum(id) as sumValue from persons")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Evaluate counters\n" +
            "Index lookup for all records: table=persons, idx=_primarykey\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testSum(Connection conn, DatabaseClient client) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select sum(id) as sumValue from persons");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("sumValue"), 1090);
    assertFalse(ret.next());
  }

  @Test(dataProvider = "connections")
  public void testLimitNoKeySecondaryIndexExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from nokeysecondaryindex where id < ? and id > ? limit 3")) {
      stmt2.setLong(1, 9);
      stmt2.setLong(2, 2);
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Two key index lookup: table=nokeysecondaryindex, idx=id, id > 2 and id < 9\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testLimitNoKeySecondaryIndex(Connection conn, DatabaseClient client) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from nokeysecondaryindex where id < ? and id > ? limit 3");
    stmt.setLong(1, 9);
    stmt.setLong(2, 2);
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 3);
    assertEquals(ret.getLong("id2"), 6);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 4);
    assertEquals(ret.getLong("id2"), 8);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 5);
    assertEquals(ret.getLong("id2"), 10);
    assertFalse(ret.next());
  }

  @Test(dataProvider = "connections")
  public void testLimitNoKeyExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from nokey where id < ? and id > ? limit 3")) {
      stmt2.setLong(1, 9);
      stmt2.setLong(2, 2);
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Table scan: table=nokey nokey.id < 9 and nokey.id > 2\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testLimitNoKey(Connection conn, DatabaseClient client) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from nokey where id < ? and id > ? limit 3");
    stmt.setLong(1, 9);
    stmt.setLong(2, 2);
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 3);
    assertEquals(ret.getLong("id2"), 6);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 3);
    assertEquals(ret.getLong("id2"), 6);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 4);
    assertEquals(ret.getLong("id2"), 8);
    assertFalse(ret.next());
  }

  @Test(dataProvider = "connections")
  public void testLimitExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from persons where id < ? and id > ? limit 3")) {
      stmt2.setLong(1, 100);
      stmt2.setLong(2, 2);
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Two key index lookup: table=persons, idx=_primarykey, id > 2 and id < 100\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testLimit(Connection conn, DatabaseClient client) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id < ? and id > ? limit 3");
    stmt.setLong(1, 100);
    stmt.setLong(2, 2);
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 3);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 4);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 5);
    assertFalse(ret.next());
  }

  @Test(dataProvider = "connections")
  public void testLimitOffsetNoKeySecondaryIndexExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from nokeysecondaryindex where id < ? and id > ? limit 3 offset 2")) {
      stmt2.setLong(1, 9);
      stmt2.setLong(2, 2);
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Two key index lookup: table=nokeysecondaryindex, idx=id, id > 2 and id < 9\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testLimitOffsetNoKeySecondaryIndex(Connection conn, DatabaseClient client) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from nokeysecondaryindex where id < ? and id > ? limit 3 offset 2");
    stmt.setLong(1, 9);
    stmt.setLong(2, 2);
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 4);
    assertEquals(ret.getLong("id2"), 8);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 5);
    assertEquals(ret.getLong("id2"), 10);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 6);
    assertEquals(ret.getLong("id2"), 12);
    assertFalse(ret.next());
  }

  @Test(dataProvider = "connections")
  public void testLimitOffsetNoKeyExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from nokey where id < ? and id > ? limit 3 offset 2")) {
      stmt2.setLong(1, 9);
      stmt2.setLong(2, 2);
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Table scan: table=nokey nokey.id < 9 and nokey.id > 2\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testLimitOffsetNoKey(Connection conn, DatabaseClient client) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from nokey where id < ? and id > ? limit 3 offset 2");
    stmt.setLong(1, 9);
    stmt.setLong(2, 2);
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 3);
    assertEquals(ret.getLong("id2"), 6);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 4);
    assertEquals(ret.getLong("id2"), 8);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 4);
    assertEquals(ret.getLong("id2"), 8);
    assertFalse(ret.next());
  }

  @Test(dataProvider = "connections")
  public void testLimitOffsetExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from persons where id < ? and id > ? limit 3 offset 2")) {
      stmt2.setLong(1, 100);
      stmt2.setLong(2, 2);
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Two key index lookup: table=persons, idx=_primarykey, id > 2 and id < 100\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testLimitOffset(Connection conn, DatabaseClient client) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id < ? and id > ? limit 3 offset 2");
    stmt.setLong(1, 100);
    stmt.setLong(2, 2);
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 4);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 5);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 6);
    assertFalse(ret.next());
  }

  @Test(dataProvider = "connections")
  public void testLimitOffsetOneKeyExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from persons where id > ? limit 3 offset 2")) {
      stmt2.setLong(1, 2);
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for relational op: table=persons, idx=_primarykey, persons.id > 2\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testLimitOffsetOneKey(Connection conn, DatabaseClient client) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id > ? limit 3 offset 2");
    stmt.setLong(1, 2);
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 4);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 5);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 6);
    assertFalse(ret.next());
  }

  @Test(dataProvider = "connections")
  public void testSort2NoKeySecondaryIndexExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id, id2 from nokeysecondaryindex order by id2 asc, id asc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to server sort\n" +
            "Index lookup for all records: table=nokeysecondaryindex, idx=_primarykey\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testSort2NoKeySecondaryIndex(Connection conn, DatabaseClient client) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id2 from nokeysecondaryindex order by id2 asc, id asc");
    ResultSet ret = stmt.executeQuery();

    for (int i = 0; i < recordCount; i++) {
      assertTrue(ret.next());
      assertEquals(ret.getLong("id2"), i * 2);
      assertEquals(ret.getLong("id"), i);
    }
  }

  @Test(dataProvider = "connections")
  public void testSort2NoKeyExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id, id2 from nokey order by id2 asc, id asc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to server sort\n" +
            "Index lookup for all records: table=nokey, idx=_primarykey\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testSort2NoKey(Connection conn, DatabaseClient client) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id2 from nokey order by id2 asc, id asc");
    ResultSet ret = stmt.executeQuery();

    for (int i = 0; i < recordCount; i++) {
      assertTrue(ret.next());
      assertEquals(ret.getLong("id2"), i * 2);
      assertEquals(ret.getLong("id"), i);
      assertTrue(ret.next());
      assertEquals(ret.getLong("id2"), i * 2);
      assertEquals(ret.getLong("id"), i);
    }

  }

  @Test(dataProvider = "connections")
  public void testSort2Explain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select id, id2 from persons order by id2 asc, id asc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to server sort\n" +
            "Index lookup for all records: table=persons, idx=_primarykey\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testSort2(Connection conn, DatabaseClient client) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id2 from persons order by id2 asc, id asc");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 100);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 102);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 104);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 106);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 108);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getLong("id"), 101);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getLong("id"), 103);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getLong("id"), 105);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getLong("id"), 107);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getLong("id"), 109);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id2"), 0);
    assertTrue(ret.wasNull());
    assertEquals(ret.getLong("id"), 0);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id2"), 0);
    assertTrue(ret.wasNull());
    assertEquals(ret.getLong("id"), 1);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id2"), 0);
    assertTrue(ret.wasNull());
    assertEquals(ret.getLong("id"), 2);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id2"), 0);
    assertTrue(ret.wasNull());
    assertEquals(ret.getLong("id"), 3);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id2"), 0);
    assertTrue(ret.wasNull());
    assertEquals(ret.getLong("id"), 4);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id2"), 0);
    assertTrue(ret.wasNull());
    assertEquals(ret.getLong("id"), 5);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id2"), 0);
    assertTrue(ret.wasNull());
    assertEquals(ret.getLong("id"), 6);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id2"), 0);
    assertTrue(ret.wasNull());
    assertEquals(ret.getLong("id"), 7);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id2"), 0);
    assertTrue(ret.wasNull());
    assertEquals(ret.getLong("id"), 8);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id2"), 0);
    assertTrue(ret.wasNull());
    assertEquals(ret.getLong("id"), 9);
    assertFalse(ret.next());
  }

  @Test(dataProvider = "connections")
  public void testAllSortAndNoKeySecondaryIndexExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from nokeysecondaryindex where id < 2 and id2 = 0 order by id2 asc, id desc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to server sort\n" +
            "Index lookup for relational op: table=nokeysecondaryindex, idx=id, nokeysecondaryindex.id < 2\n" +
            "single key index lookup\n" +
            " AND \n" +
            "Read record from index and evaluate: nokeysecondaryindex.id2 = 0\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testAllSortAndNoKeySecondaryIndex(Connection conn, DatabaseClient client) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from nokeysecondaryindex where id < 2 and id2 = 0 order by id2 asc, id desc");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 0);
    assertFalse(ret.next());
  }

  @Test(dataProvider = "connections")
  public void testAllSortAndNoKeyExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from nokey where id < 2 and id2 = 0 order by id2 asc, id desc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to server sort\n" +
            "Table scan: table=nokey nokey.id < 2 and nokey.id2 = 0\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testAllSortAndNoKey(Connection conn, DatabaseClient client) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from nokey where id < 2 and id2 = 0 order by id2 asc, id desc");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 0);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 0);
    assertFalse(ret.next());
  }

  @Test(dataProvider = "connections")
  public void testAllSortAndExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from persons where id > 100 and id2 = 0 order by id2 asc, id desc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to server sort\n" +
            "Index lookup for relational op: table=persons, idx=_primarykey, persons.id > 100\n" +
            "single key index lookup\n" +
            " AND \n" +
            "Read record from index and evaluate: persons.id2 = 0\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testAllSortAnd(Connection conn, DatabaseClient client) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id > 100 and id2 = 0 order by id2 asc, id desc");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 108);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 106);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 104);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 102);
    assertFalse(ret.next());
  }

  @Test(dataProvider = "connections")
  public void testComplexNoKeySecondaryIndexExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select nokeysecondaryindex.id  " +
            "from nokeysecondaryindex where nokeysecondaryindex.id>=1 AND id < 3 AND ID2=2 OR id> 2 AND ID < 4")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to 'or' expression\n" +
            "Two key index lookup: table=nokeysecondaryindex, idx=id, id < 3 and id >= 1\n" +
            " AND \n" +
            "Read record from index and evaluate: nokeysecondaryindex.id2 = 2\n" +
            " OR \n" +
            "Two key index lookup: table=nokeysecondaryindex, idx=id, id < 4 and id > 2\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testComplexNoKeySecondaryIndex(Connection conn, DatabaseClient client) throws SQLException {
    //fails

    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select nokeysecondaryindex.id  " +
        "from nokeysecondaryindex where nokeysecondaryindex.id>=1 AND id < 3 AND ID2=2 OR id> 2 AND ID < 4");                                              //
    ResultSet ret = stmt.executeQuery();


    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 1);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 3);

    assertFalse(ret.next());
  }

  @Test(dataProvider = "connections")
  public void testComplexNoKeyExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select nokey.id  " +
            "from nokey where nokey.id>=1 AND id < 3 AND ID2=2 OR id> 2 AND ID < 4")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Table scan: table=nokey nokey.id >= 1 and nokey.id < 3 and nokey.id2 = 2 or nokey.id > 2 and nokey.id < 4\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testComplexNoKey(Connection conn, DatabaseClient client) throws SQLException {
    //fails

    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select nokey.id  " +
        "from nokey where nokey.id>=1 AND id < 3 AND ID2=2 OR id> 2 AND ID < 4");                                              //
    ResultSet ret = stmt.executeQuery();


    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 1);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 1);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 3);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 3);

    assertFalse(ret.next());
  }

  @Test(dataProvider = "connections")
  public void testComplexExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select persons.id  " +
            "from persons where persons.id>=100 AND id < 105 AND ID2=0 OR id> 6 AND ID < 10")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to 'or' expression\n" +
            "Two key index lookup: table=persons, idx=_primarykey, id < 105 and id >= 100\n" +
            " AND \n" +
            "Read record from index and evaluate: persons.id2 = 0\n" +
            " OR \n" +
            "Two key index lookup: table=persons, idx=_primarykey, id < 10 and id > 6\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testComplex(Connection conn, DatabaseClient client) throws SQLException {
    //fails

    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select persons.id  " +
        "from persons where persons.id>=100 AND id < 105 AND ID2=0 OR id> 6 AND ID < 10");                                              //
    ResultSet ret = stmt.executeQuery();


    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 100);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 102);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 104);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 7);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 8);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 9);

    assertFalse(ret.next());
  }

  @Test(dataProvider = "connections")
  public void testParensNoKeySecondaryIndexExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select nokeysecondaryindex.id  " +
            "from nokeysecondaryindex where nokeysecondaryindex.id<=5 AND (id < 2 OR id> 4)")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to 'or' expression\n" +
            "Index lookup for relational op: table=nokeysecondaryindex, idx=id, nokeysecondaryindex.id < 2\n" +
            "single key index lookup\n" +
            " OR \n" +
            "Index lookup for relational op: table=nokeysecondaryindex, idx=id, nokeysecondaryindex.id > 4\n" +
            "single key index lookup\n" +
            " AND \n" +
            "Read record from index and evaluate: nokeysecondaryindex.id <= 5\n" +
            " AND \n" +
            "Read record from index and evaluate: nokeysecondaryindex.id <= 5\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testParensNoKeySecondaryIndex(Connection conn, DatabaseClient client) throws SQLException {
    //fails

    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select nokeysecondaryindex.id  " +
        "from nokeysecondaryindex where nokeysecondaryindex.id<=5 AND (id < 2 OR id> 4)");                                              //
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 0);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 1);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 5);

    assertFalse(ret.next());
  }

  @Test(dataProvider = "connections")
  public void testParensNoKeyExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select nokey.id  " +
            "from nokey where nokey.id<=5 AND (id < 2 OR id> 4)")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Table scan: table=nokey nokey.id <= 5 and nokey.id < 2 or nokey.id > 4\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testParensNoKey(Connection conn, DatabaseClient client) throws SQLException {
    //fails

    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select nokey.id  " +
        "from nokey where nokey.id<=5 AND (id < 2 OR id> 4)");                                              //
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 0);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 0);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 1);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 1);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 5);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 5);

    assertFalse(ret.next());
  }

  @Test(dataProvider = "connections")
  public void testParensExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select persons.id  " +
            "from persons where persons.id<=100 AND (id < 6 OR id> 8)")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to 'or' expression\n" +
            "Index lookup for relational op: table=persons, idx=_primarykey, persons.id < 6\n" +
            "single key index lookup\n" +
            " OR \n" +
            "Index lookup for relational op: table=persons, idx=_primarykey, persons.id > 8\n" +
            "single key index lookup\n" +
            " AND \n" +
            "Read record from index and evaluate: persons.id <= 100\n" +
            " AND \n" +
            "Read record from index and evaluate: persons.id <= 100\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testParens(Connection conn, DatabaseClient client) throws SQLException {
    //fails

    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select persons.id  " +
        "from persons where persons.id<=100 AND (id < 6 OR id> 8)");                                              //
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 0);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 1);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 2);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 3);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 4);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 5);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 9);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 100);

    assertFalse(ret.next());
  }

  @Test(dataProvider = "connections")
  public void testPrecedenceNoKeySecondaryIndexExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select nokeysecondaryindex.id  " +
            "from nokeysecondaryindex where nokeysecondaryindex.id<=7 AND id > 4 OR id> 8")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to 'or' expression\n" +
            "Two key index lookup: table=nokeysecondaryindex, idx=id, id > 4 and id <= 7\n" +
            " OR \n" +
            "Index lookup for relational op: table=nokeysecondaryindex, idx=id, nokeysecondaryindex.id > 8\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testPrecedenceNoKeySecondaryIndex(Connection conn, DatabaseClient client) throws SQLException {
    //fails

    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select nokeysecondaryindex.id  " +
        "from nokeysecondaryindex where nokeysecondaryindex.id<=7 AND id > 4 OR id> 8");                                              //
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 5);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 6);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 7);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 9);

    assertFalse(ret.next());
  }

  @Test(dataProvider = "connections")
  public void testPrecedenceNoKeyExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select nokey.id  " +
            "from nokey where nokey.id<=7 AND id > 4 OR id> 8")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Table scan: table=nokey nokey.id <= 7 and nokey.id > 4 or nokey.id > 8\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testPrecedenceNoKey(Connection conn, DatabaseClient client) throws SQLException {
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

  @Test(dataProvider = "connections")
  public void testPrecedenceExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select persons.id  " +
            "from persons where persons.id<=100 AND id > 4 OR id> 103")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to 'or' expression\n" +
            "Two key index lookup: table=persons, idx=_primarykey, id > 4 and id <= 100\n" +
            " OR \n" +
            "Index lookup for relational op: table=persons, idx=_primarykey, persons.id > 103\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testPrecedence(Connection conn, DatabaseClient client) throws SQLException {
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

  @Test(dataProvider = "connections")
  public void testTwoKeyLessEqualExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select persons.id  " +
            "from persons where persons.id<=100 AND id > 4")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Two key index lookup: table=persons, idx=_primarykey, id > 4 and id <= 100\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testTwoKeyLessEqual(Connection conn, DatabaseClient client) throws SQLException {
    //fails

    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select persons.id  " +
        "from persons where persons.id<=100 AND id > 4");                                              //
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
    assertFalse(ret.next());
  }

  @Test(dataProvider = "connections")
  public void testOverlapPrecedenceNoKeySecondaryIndexExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select nokeysecondaryindex.id  " +
            "from nokeysecondaryindex where nokeysecondaryindex.id<=8 AND id < 2 OR id> 8")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to 'or' expression\n" +
            "Index lookup for relational op: table=nokeysecondaryindex, idx=id, nokeysecondaryindex.id < 2\n" +
            "single key index lookup\n" +
            " AND \n" +
            "Read record from index and evaluate: nokeysecondaryindex.id <= 8\n" +
            " OR \n" +
            "Index lookup for relational op: table=nokeysecondaryindex, idx=id, nokeysecondaryindex.id > 8\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testOverlapPrecedenceNoKeySecondaryIndex(Connection conn, DatabaseClient client) throws SQLException {
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

  @Test(dataProvider = "connections")
  public void testOverlapPrecedenceNoKeyExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select nokey.id  " +
            "from nokey where nokey.id<=8 AND id < 2 OR id> 8")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Table scan: table=nokey nokey.id <= 8 and nokey.id < 2 or nokey.id > 8\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testOverlapPrecedenceNoKey(Connection conn, DatabaseClient client) throws SQLException {
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

  @Test(dataProvider = "connections")
  public void testOverlapPrecedenceExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select persons.id  " +
            "from persons where persons.id<=100 AND id < 4 OR id> 103")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to 'or' expression\n" +
            "Index lookup for relational op: table=persons, idx=_primarykey, persons.id < 4\n" +
            "single key index lookup\n" +
            " AND \n" +
            "Read record from index and evaluate: persons.id <= 100\n" +
            " OR \n" +
            "Index lookup for relational op: table=persons, idx=_primarykey, persons.id > 103\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testOverlapPrecedence(Connection conn, DatabaseClient client) throws SQLException {
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

  @Test(dataProvider = "connections")
  public void testOverlapPrecedence2NoKeySecondaryIndexeExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select nokeysecondaryindex.id  " +
            "from nokeysecondaryindex where nokeysecondaryindex.id<=7 AND id = 4 OR id> 8")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to 'or' expression\n" +
            "Index lookup for relational op: table=nokeysecondaryindex, idx=id, nokeysecondaryindex.id = 4\n" +
            "single key index lookup\n" +
            " AND \n" +
            "Read record from index and evaluate: nokeysecondaryindex.id <= 7\n" +
            " OR \n" +
            "Index lookup for relational op: table=nokeysecondaryindex, idx=id, nokeysecondaryindex.id > 8\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testOverlapPrecedence2NoKeySecondaryIndex(Connection conn, DatabaseClient client) throws SQLException {
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

  @Test(dataProvider = "connections")
  public void testOverlapPrecedence2NoKeyExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select nokey.id  " +
            "from nokey where nokey.id<=7 AND id = 4 OR id> 8")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Table scan: table=nokey nokey.id <= 7 and nokey.id = 4 or nokey.id > 8\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testOverlapPrecedence2NoKey(Connection conn, DatabaseClient client) throws SQLException {
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

  @Test(dataProvider = "connections")
  public void testOverlapPrecedence2Explain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select persons.id  " +
            "from persons where persons.id<=100 AND id = 4 OR id> 103")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to 'or' expression\n" +
            "Index lookup for relational op: table=persons, idx=_primarykey, persons.id = 4\n" +
            "single key index lookup\n" +
            " AND \n" +
            "Read record from index and evaluate: persons.id <= 100\n" +
            " OR \n" +
            "Index lookup for relational op: table=persons, idx=_primarykey, persons.id > 103\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testOverlapPrecedence2(Connection conn, DatabaseClient client) throws SQLException {
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

  @Test(dataProvider = "connections")
  public void testAvgNoKeySecondaryIndexExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select avg(nokeysecondaryindex.id) as avgValue from nokeysecondaryindex")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Evaluate counters\n" +
            "Index lookup for all records: table=nokeysecondaryindex, idx=_primarykey\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testAvgNoKeySecondaryIndex(Connection conn, DatabaseClient client) throws SQLException {
    //fails

    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select avg(nokeysecondaryindex.id) as avgValue from nokeysecondaryindex");                                              //
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getDouble("avgValue"), 4.5d);
    assertFalse(ret.next());
  }

  @Test(dataProvider = "connections")
  public void testAvgNoKeyExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select avg(nokey.id) as avgValue from nokey")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Evaluate counters\n" +
            "Index lookup for all records: table=nokey, idx=_primarykey\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testAvgNoKey(Connection conn, DatabaseClient client) throws SQLException {
    //fails

    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select avg(nokey.id) as avgValue from nokey");                                              //
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getDouble("avgValue"), 4.5d);
    assertFalse(ret.next());
  }

  @Test(dataProvider = "connections")
  public void testAvgExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select avg(persons.id) as avgValue from persons")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Evaluate counters\n" +
            "Index lookup for all records: table=persons, idx=_primarykey\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testAvg(Connection conn, DatabaseClient client) throws SQLException {
    //fails

    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select avg(persons.id) as avgValue from persons");                                              //
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getDouble("avgValue"), 54.5d);
    assertFalse(ret.next());
  }

  @Test(enabled = false)
  public void testAvg2(Connection conn, DatabaseClient client) throws SQLException {
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

  @Test(dataProvider = "connections")
  public void testOrNoKeySecondaryIndexExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from nokeysecondaryindex where id>8 and id2=18 or id<6 and id2=2 order by id desc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to 'or' expression\n" +
            "Index lookup for relational op: table=nokeysecondaryindex, idx=id, nokeysecondaryindex.id > 8\n" +
            "single key index lookup\n" +
            " AND \n" +
            "Read record from index and evaluate: nokeysecondaryindex.id2 = 18\n" +
            " OR \n" +
            "Index lookup for relational op: table=nokeysecondaryindex, idx=id, nokeysecondaryindex.id < 6\n" +
            "single key index lookup\n" +
            " AND \n" +
            "Read record from index and evaluate: nokeysecondaryindex.id2 = 2\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testOrNoKeySecondaryIndex(Connection conn, DatabaseClient client) throws SQLException {
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

  @Test(dataProvider = "connections")
  public void testOrNoKeyExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from nokey where id>8 and id2=18 or id<6 and id2=2 order by id desc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to server sort\n" +
            "Table scan: table=nokey nokey.id > 8 and nokey.id2 = 18 or nokey.id < 6 and nokey.id2 = 2\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testOrNoKey(Connection conn, DatabaseClient client) throws SQLException {
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

  @Test(dataProvider = "connections")
  public void testOrExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from persons where id>105 and id2=0 or id<105 and id2=1 order by id desc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to 'or' expression\n" +
            "Index lookup for relational op: table=persons, idx=_primarykey, persons.id > 105\n" +
            "single key index lookup\n" +
            " AND \n" +
            "Read record from index and evaluate: persons.id2 = 0\n" +
            " OR \n" +
            "Index lookup for relational op: table=persons, idx=_primarykey, persons.id < 105\n" +
            "single key index lookup\n" +
            " AND \n" +
            "Read record from index and evaluate: persons.id2 = 1\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testOr(Connection conn, DatabaseClient client) throws SQLException {
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
  public void serverSort(Connection conn, DatabaseClient client) throws SQLException {

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

  @Test(dataProvider = "connections")
  public void testMixedExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select persons.id, socialsecuritynumber as s " +
            "from persons where socialsecuritynumber > '933-28-6' AND persons.id>5 AND id < 10")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for relational op: table=persons, idx=socialsecuritynumber, persons.socialsecuritynumber > 933-28-6\n" +
            "single key index lookup\n" +
            " AND \n" +
            "Read record from index and evaluate: persons.id < 10 and persons.id > 5\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testMixed(Connection conn, DatabaseClient client) throws SQLException {
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

  @Test(dataProvider = "connections")
  public void testOrAndExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select persons.id  " +
            "from persons where persons.id>2 AND id < 4 OR id> 6 AND ID < 8")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to 'or' expression\n" +
            "Two key index lookup: table=persons, idx=_primarykey, id < 4 and id > 2\n" +
            " OR \n" +
            "Two key index lookup: table=persons, idx=_primarykey, id < 8 and id > 6\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testOrAnd(Connection conn, DatabaseClient client) throws SQLException {
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

  @Test(dataProvider = "connections")
  public void testEqualNonIndexExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from persons where id2=1 order by id desc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to server sort\n" +
            "Table scan: table=persons persons.id2 = 1\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testEqualNonIndex(Connection conn, DatabaseClient client) throws SQLException {
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

  @Test(dataProvider = "connections")
  public void testEqualIndexExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from persons where id=1 order by id desc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for relational op: table=persons, idx=_primarykey, persons.id = 1\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testEqualIndex(Connection conn, DatabaseClient client) throws SQLException {
    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id=1 order by id desc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 1);
    assertFalse(ret.next());
  }

  @Test(dataProvider = "connections")
  public void testInNoKeySecondaryIndexExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from nokeysecondaryindex where id in (0, 1, 2)")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "In expression read each expression from index: table=nokeysecondaryindex, idx=id\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testInNoKeySecondaryIndex(Connection conn, DatabaseClient client) throws SQLException {

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

  @Test(dataProvider = "connections")
  public void testInNoKeyExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from nokey where id in (0, 1, 2)")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Table scan: table=nokey nokey.id in (0, 1, 2)\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testInNoKey(Connection conn, DatabaseClient client) throws SQLException {

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

  @Test(dataProvider = "connections")
  public void testSecondaryIndexExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from persons where socialSecurityNumber=? order by id")) {
      stmt2.setString(1, "933-28-" + 0);
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to server sort\n" +
            "Index lookup for relational op: table=persons, idx=socialsecuritynumber, persons.socialsecuritynumber = 933-28-0\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testSecondaryIndex(Connection conn, DatabaseClient client) throws SQLException {

    //test select on secondary index
    PreparedStatement stmt;
    ResultSet ret;
    for (int i = 0; i < recordCount; i++) {

      //test jdbc select
      stmt = conn.prepareStatement("select * from persons where socialSecurityNumber=? order by id");
      stmt.setString(1, "933-28-" + i);
      ret = stmt.executeQuery();
      System.out.println("checking: 933-28-" + i);
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

  @Test(dataProvider = "connections")
  public void testMultipleFieldsExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from persons where id=" + (0 + 100) + " AND id2=" + ((0 + 100) % 2))) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for relational op: table=persons, idx=_primarykey, persons.id = 100\n" +
            "single key index lookup\n" +
            " AND \n" +
            "Read record from index and evaluate: persons.id2 = 0\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testMultipleFields(Connection conn, DatabaseClient client) throws SQLException {
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

  @Test(dataProvider = "connections")
  public void testAndNoKeySecondaryIndexExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from nokeysecondaryindex where id!=0 AND id!=1 AND id!=2 AND id!=3 AND id!=4 order by id asc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Table scan: table=nokeysecondaryindex nokeysecondaryindex.id != 0 and nokeysecondaryindex.id != 1 and nokeysecondaryindex.id != 2 and nokeysecondaryindex.id != 3 and nokeysecondaryindex.id != 4\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testAndNoKeySecondaryIndex(Connection conn, DatabaseClient client) throws SQLException {
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

  @Test(dataProvider = "connections")
  public void testAndNoKeyExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from nokey where id!=0 AND id!=1 AND id!=2 AND id!=3 AND id!=4 order by id asc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to server sort\n" +
            "Table scan: table=nokey nokey.id != 0 and nokey.id != 1 and nokey.id != 2 and nokey.id != 3 and nokey.id != 4\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testAndNoKey(Connection conn, DatabaseClient client) throws SQLException {
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

  @Test(dataProvider = "connections")
  public void testAndExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from persons where id!=0 AND id!=1 AND id!=2 AND id!=3 AND id!=4 order by id asc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Table scan: table=persons persons.id != 0 and persons.id != 1 and persons.id != 2 and persons.id != 3 and persons.id != 4\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testAnd(Connection conn, DatabaseClient client) throws SQLException {
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

  @Test(dataProvider = "connections")
  public void testOrTableScanNoKeySecondaryIndexExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from nokeysecondaryindex where id2=2 or id2=0 order by id2 asc, id desc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to server sort\n" +
            "Table scan: table=nokeysecondaryindex nokeysecondaryindex.id2 = 2 or nokeysecondaryindex.id2 = 0\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testOrTableScanNoKeySecondaryIndex(Connection conn, DatabaseClient client) throws SQLException {
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

  @Test(dataProvider = "connections")
  public void testOrTableScanExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from persons where id2=1 or id2=0 order by id2 asc, id desc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to server sort\n" +
            "Table scan: table=persons persons.id2 = 1 or persons.id2 = 0\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testOrTableScan(Connection conn, DatabaseClient client) throws SQLException {
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

  @Test(dataProvider = "connections")
  public void testOrIndexNoKeySecondaryIndexExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from nokeysecondaryindex where id=0 OR id=1 OR id=2 OR id=3 OR id=4")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Batch index lookup: table=nokeysecondaryindex, idx=id, keyCount=5\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testOrIndexNoKeySecondaryIndex(Connection conn, DatabaseClient client) throws SQLException {
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

  @Test(dataProvider = "connections")
  public void testOrIndexExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from persons where id=0 OR id=1 OR id=2 OR id=3 OR id=4")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Batch index lookup: table=persons, idx=_primarykey, keyCount=5\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testOrIndex(Connection conn, DatabaseClient client) throws SQLException {
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

  @Test(dataProvider = "connections")
  public void testLessEqualNoKeySecondaryIndexExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from nokeysecondaryindex where id<=3 order by id desc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for relational op: table=nokeysecondaryindex, idx=id, nokeysecondaryindex.id <= 3\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testLessEqualNoKeySecondaryIndex(Connection conn, DatabaseClient client) throws SQLException {

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

  @Test(dataProvider = "connections")
  public void testLessEqualNoKeyExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from nokey where id<=3 order by id desc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to server sort\n" +
            "Table scan: table=nokey nokey.id <= 3\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testLessEqualNoKey(Connection conn, DatabaseClient client) throws SQLException {

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

  @Test(dataProvider = "connections")
  public void testLessEqualExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from persons where id<=5 order by id desc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for relational op: table=persons, idx=_primarykey, persons.id <= 5\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testLessEqual(Connection conn, DatabaseClient client) throws SQLException {

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

  @Test(dataProvider = "connections")
  public void testLessEqualGreaterEqualExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from persons where id >= 1 and id<=5 order by id asc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Two key index lookup: table=persons, idx=_primarykey, id <= 5 and id >= 1\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testLessEqualGreaterEqual(Connection conn, DatabaseClient client) throws SQLException {

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

  @Test(dataProvider = "connections")
  public void testLessEqualGreaterEqualDescExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from persons where id >= 1 and id<=5 order by id desc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Two key index lookup: table=persons, idx=_primarykey, id <= 5 and id >= 1\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testLessEqualGreaterEqualDesc(Connection conn, DatabaseClient client) throws SQLException {

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

  @Test(dataProvider = "connections")
  public void testLessEqualAndGreaterEqualNoKeySecondaryIndexExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from nokeysecondaryindex where id<=5 and id>=1 order by id desc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Two key index lookup: table=nokeysecondaryindex, idx=id, id >= 1 and id <= 5\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testLessEqualAndGreaterEqualNoKeySecondaryIndex(Connection conn, DatabaseClient client) throws SQLException {

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

  @Test(dataProvider = "connections")
  public void testLessEqualAndGreaterEqualNoKeyExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from nokey where id<=5 and id>=1 order by id desc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to server sort\n" +
            "Table scan: table=nokey nokey.id <= 5 and nokey.id >= 1\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testLessEqualAndGreaterEqualNoKey(Connection conn, DatabaseClient client) throws SQLException {

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

  @Test(dataProvider = "connections")
  public void testLessEqualAndGreaterEqualExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from persons where id<=5 and id>=1 order by id desc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Two key index lookup: table=persons, idx=_primarykey, id >= 1 and id <= 5\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testLessEqualAndGreaterEqual(Connection conn, DatabaseClient client) throws SQLException {

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

  @Test(dataProvider = "connections")
  public void testGreaterNoKeySecondaryIndexExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from nokeysecondaryindex where id>5")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for relational op: table=nokeysecondaryindex, idx=id, nokeysecondaryindex.id > 5\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testGreaterNoKeySecondaryIndex(Connection conn, DatabaseClient client) throws SQLException {
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

  @Test(dataProvider = "connections")
  public void testGreaterNoKeyExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from nokey where id>5 order by id asc")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Server select due to server sort\n" +
            "Table scan: table=nokey nokey.id > 5\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testGreaterNoKey(Connection conn, DatabaseClient client) throws SQLException {
    //test select returns multiple records with an index using operator '>'
    PreparedStatement stmt = conn.prepareStatement("select * from nokey where id>5 order by id asc");
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

  @Test(dataProvider = "connections")
  public void testGreaterExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from persons where id>5")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for relational op: table=persons, idx=_primarykey, persons.id > 5\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testGreater(Connection conn, DatabaseClient client) throws SQLException {
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

  @Test(dataProvider = "connections")
  public void testGreaterEqualNoKeySecondaryIndexExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from nokeysecondaryindex where id>=5")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for relational op: table=nokeysecondaryindex, idx=id, nokeysecondaryindex.id >= 5\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testGreaterEqualNoKeySecondaryIndex(Connection conn, DatabaseClient client) throws SQLException {
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

  @Test(dataProvider = "connections")
  public void testGreaterEqualNoKeyExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from nokey where id>=5")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Table scan: table=nokey nokey.id >= 5\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testGreaterEqualNoKey(Connection conn, DatabaseClient client) throws SQLException {
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

  @Test(dataProvider = "connections")
  public void testGreaterEqualExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from persons where id>=5")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for relational op: table=persons, idx=_primarykey, persons.id >= 5\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testGreaterEqual(Connection conn, DatabaseClient client) throws SQLException {
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

  @Test(dataProvider = "connections")
  public void testEqual2Explain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from persons where id=0")) {
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

  @Test(dataProvider = "connections")
  public void testEqual2(Connection conn, DatabaseClient client) throws SQLException {
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

  @Test(dataProvider = "connections")
  public void testEqual2NoKeySecondaryIndexExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from nokeysecondaryindex where id=0")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for relational op: table=nokeysecondaryindex, idx=id, nokeysecondaryindex.id = 0\n" +
            "single key index lookup\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testEqual2NoKeySecondaryIndex(Connection conn, DatabaseClient client) {
    //test select
    PreparedStatement stmt;
    ResultSet ret;
    for (int i = 0; i < recordCount; i++) {
      try {
        //test jdbc select
        stmt = conn.prepareStatement("select * from nokeysecondaryindex where id=" + i);
        ret = stmt.executeQuery();

        ret.next();
        long retId = ret.getLong("id");
        assertEquals(retId, i, "Returned id doesn't match: id=" + i + ", retId=" + retId);
        assertEquals(ret.getLong("id2"), 2 * i);
      }
      catch (Exception e) {
        fail("i=" + i);
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testEqual2NoKeyExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from nokey where id=0")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Table scan: table=nokey nokey.id = 0\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testEqual2NoKey(Connection conn, DatabaseClient client) throws SQLException {
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

  @Test(dataProvider = "connections")
  public void testUpdateNoKeySecondaryIndex(Connection conn, DatabaseClient client) throws SQLException {
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

  @Test(dataProvider = "connections")
  public void testUpdateNoKey(Connection conn, DatabaseClient client) throws SQLException {
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

  @Test(dataProvider = "connections")
  public void testUpdate(Connection conn, DatabaseClient client) throws SQLException {
    //fails

    PreparedStatement stmt = conn.prepareStatement("update persons set id = id + ?, socialSecurityNumber=? where id=?");
    stmt.setLong(1, 1000);
    stmt.setString(2, "ssn");
    stmt.setLong(3, 0);
    stmt.executeUpdate();

    stmt = conn.prepareStatement("select * from persons where id=" + 0);
    ResultSet ret = stmt.executeQuery();
    assertFalse(ret.next());

    stmt = conn.prepareStatement("select * from persons where socialSecurityNumber='ssn'");
    ret = stmt.executeQuery();
    assertTrue(ret.next());
    assertFalse(ret.next());
//    assertTrue(ret.next());
//    assertTrue(ret.next());
//    assertFalse(ret.next());

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

  @Test(dataProvider = "connections")
  public void testUpdate2(Connection conn, DatabaseClient client) throws SQLException {

    PreparedStatement stmt = conn.prepareStatement("insert into persons (id, id2, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?)");
    stmt.setLong(1, 100000);
    stmt.setLong(2, (100) % 2);
    stmt.setString(3, "ssn");
    stmt.setString(4, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
    stmt.setBoolean(5, false);
    stmt.setString(6, "m");
    int count = stmt.executeUpdate();
    assertEquals(count, 1);

    stmt = conn.prepareStatement("update persons set id = id + ?, socialSecurityNumber=? where id=?");
    stmt.setLong(1, 1000);
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

  @Test(dataProvider = "connections")
  public void testInsert(Connection conn, DatabaseClient client) throws SQLException {

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

  @Test(dataProvider = "connections")
  public void testDelete(Connection conn, DatabaseClient client) throws SQLException {


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

  @Test(dataProvider = "connections")
  public void testOverlappingExplain(Connection conn, DatabaseClient client) throws SQLException {
    try (PreparedStatement stmt2 = conn.prepareStatement(
        "explain select * from persons where id>4 and id < 10 and id > 2")) {
      try (ResultSet rs = stmt2.executeQuery()) {
        StringBuilder builder = new StringBuilder();
        while (rs.next()) {
          builder.append(rs.getString(1)).append("\n");
        }
        assertEquals(builder.toString(), "Index lookup for relational op: table=persons, idx=_primarykey, persons.id > 2\n" +
            "single key index lookup\n" +
            " AND \n" +
            "Read record from index and evaluate: persons.id > 4 and persons.id < 10\n");
      }
    }
  }

  @Test(dataProvider = "connections")
  public void testOverlapping(Connection conn, DatabaseClient client) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id>4 and id < 10 and id > 2");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id"), 5);
    rs.next();
    assertEquals(rs.getLong("id"), 6);
    rs.next();
    assertEquals(rs.getLong("id"), 7);
    rs.next();
    assertEquals(rs.getLong("id"), 8);
    rs.next();
    assertEquals(rs.getLong("id"), 9);
    assertFalse(rs.next());
  }

  @Test(dataProvider = "connections")
  public void testTruncate(Connection conn, DatabaseClient client) throws SQLException {
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

  @Test(dataProvider = "connections")
  public void testDeleteNoPrimaryKey(Connection conn, DatabaseClient client) throws SQLException {
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

//    LocalConsumer consumer = new LocalConsumer();
//    while (true) {
//      List<Message> msgs = consumer.receive();
//      if (msgs == null || msgs.size() == 0) {
//        break;
//      }
//    }
    PreparedStatement stmt2 = conn.prepareStatement("delete from ToDeleteNoPrimaryKey where id=0");
    assertEquals(stmt2.executeUpdate(), 1);

    try {
      stmt = conn.prepareStatement("select * from ToDeleteNoPrimaryKey where id = 0");
      rs = stmt.executeQuery();
      assertFalse(rs.next());
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  @Test(dataProvider = "connections")
  public void testDropTable(Connection conn, DatabaseClient client) throws SQLException {
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

  public static void xmain(String[] args) {

//    SecretKey symKey = KeyGenerator.getInstance(algorithm).generateKey();
//    symKey = new SecretKeySpec(Base64.getDecoder().decode(DatabaseServer.LICENSE_KEY), algorithm);
//
//    System.out.println("key=" + new String(Base64.getEncoder().encode(symKey.getEncoded()), "utf-8"));
//
//    Cipher c = Cipher.getInstance(algorithm);
//
//    byte[] encryptionBytes = encryptF("sonicbase:pro:4", symKey, c);
//
//    System.out.println("encrypted: " + new String(new Hex().encode(encryptionBytes), "utf-8"));
//    System.out.println("Decrypted: " + decryptF(encryptionBytes, symKey, c));

//    symKey = new SecretKeySpec(Base64.getDecoder().decode(DatabaseServer.LICENSE_KEY), algorithm);
//    System.out.println("Decrypted: " + decryptF(encryptionBytes, symKey, c));

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

    return new String(decrypt);
  }

}

