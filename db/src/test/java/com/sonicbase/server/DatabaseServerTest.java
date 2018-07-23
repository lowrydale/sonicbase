package com.sonicbase.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.*;
import com.sonicbase.index.Index;
import com.sonicbase.index.Indices;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.TestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.*;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

public class DatabaseServerTest {

  @BeforeMethod
  public void beforeMethod() throws IOException {
    FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "/db-data"));
  }

  @Test
  public void test() throws InterruptedException {

    com.sonicbase.server.DatabaseServer server = new DatabaseServer();
    AtomicBoolean isHealthy = new AtomicBoolean();
    DatabaseClient client = mock(DatabaseClient.class);
    server.setDatabaseClient(client);

    when(client.send(eq("DatabaseServer:healthCheck"), anyInt(), eq((long) 0), any(ComObject.class),
        eq(DatabaseClient.Replica.SPECIFIED))).thenAnswer(
        new Answer() {
          public Object answer(InvocationOnMock invocation) {
            Object[] args = invocation.getArguments();
            ComObject retObj = (ComObject) args[3];
            retObj.put(ComObject.Tag.STATUS, "{\"status\" : \"ok\"}");
            return retObj.serialize();
          }
        });


    server.checkHealthOfServer(0, 0, isHealthy);

    assertTrue(isHealthy.get());
  }


  @Test
  public void testSetConfig() throws Exception {

    final AtomicBoolean calledErrorLogger = new AtomicBoolean();
    String configStr = IOUtils.toString(DatabaseServerTest.class.getResourceAsStream("/config/config-1-local.json"), "utf-8");
    ObjectNode config = (ObjectNode) new ObjectMapper().readTree(configStr);
    com.sonicbase.server.DatabaseServer server = new DatabaseServer() {
      @Override
      public Logger getErrorLogger() {
        Logger mockLogger = mock(Logger.class);
        doAnswer((Answer<Object>) invocationOnMock -> {
          calledErrorLogger.set(true);
          return null;
        }).when(mockLogger).error(anyObject());
        return mockLogger;
      }
    };
    server.setConfig(config, "test", "localhost", 9010, true, new AtomicBoolean(), new AtomicBoolean(), "gc.log");
    DatabaseClient client = mock(DatabaseClient.class);
    server.setDatabaseClient(client);

    FileUtils.deleteDirectory(new File(server.getDataDir()));
    server.setRecovered(true);

    server.getCommon().addDatabase("test");
    File file = new File(server.getDataDir(), "snapshot/0/0/test");
    file.mkdirs();

    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.METHOD, "echo");
    cobj.put(ComObject.Tag.COUNT, 1);
    server.getMethodInvoker().invokeMethod(cobj.serialize(), 0, 0, false, true, new AtomicLong(), new AtomicLong());
    assertEquals(server.getMethodInvoker().getEchoCount(), 1);

    server.initDeathOverride(2, 2);
    boolean[][] override = server.getDeathOverride();
    assertEquals(override.length, 2);
    assertEquals(override[0].length, 2);

    TableSchema tableSchema = TestUtils.createTable();
    IndexSchema indexSchema = TestUtils.createIndexSchema(tableSchema);
    server.getCommon().getTables("test").put(tableSchema.getName(), tableSchema);

    server.getIndices().put("test", new Indices());
    Comparator[] comparators = tableSchema.getComparators(new String[]{"field1"});

    server.getIndices("test").addIndex(tableSchema, indexSchema.getName(), comparators);

    Index index = server.getIndex("test", tableSchema.getName(), indexSchema.getName());
    assertNotNull(index);

    server.removeIndex("test", tableSchema.getName(), indexSchema.getName());

    //autocreates
    assertNotNull(server.getIndex("test", tableSchema.getName(), indexSchema.getName()));

    server.removeIndex("test", tableSchema.getName(), indexSchema.getName());

    assertNull(server.getIndices("test").getIndices().get(tableSchema.getName()).get(indexSchema.getName()));

    cobj.put(ComObject.Tag.METHOD, "getDbNames");
    ComObject retObj = server.getDbNames(cobj, false);
    ComArray array = retObj.getArray(ComObject.Tag.DB_NAMES);
    Set<String> dbs = new HashSet<>();
    for (int i = 0; i < array.getArray().size(); i++) {
      dbs.add((String) array.getArray().get(i));
    }
    assertTrue(dbs.contains("test"));

    assertNotNull(server.getCommon());
    assertNotNull(server.getTransactionManager());
    assertNotNull(server.getUpdateManager());
    assertNotNull(server.getLogManager());
    assertNotNull(server.getSchemaManager());
    assertNotNull(server.getPartitionManager());
    assertNotNull(server.getDeleteManager());

    cobj.put(ComObject.Tag.IS_CLIENT, false);
    cobj.put(ComObject.Tag.HOST, "localhost");
    cobj.put(ComObject.Tag.MESSAGE, "message");
    cobj.put(ComObject.Tag.EXCEPTION, "exception");
    cobj.put(ComObject.Tag.METHOD, "logError");
    server.logError(cobj, false);

    assertTrue(calledErrorLogger.get());
    server.shutdown();
  }

  @Test
  public void testMaxRecordId() throws IOException {
    DatabaseServer server = new DatabaseServer();
    DatabaseClient client = mock(DatabaseClient.class);
    server.setDatabaseClient(client);
    server.setShard(1);
    server.setReplica(1);
    server.setReplicationFactor(2);
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.MAX_ID, 100L);
    server.setMaxRecordId(cobj, false);

    File file = new File(server.getDataDir(), "nextRecordId/1/0/nextRecorId.txt");
    String str = IOUtils.toString(new FileInputStream(file));
    assertEquals(str, "100");

    final AtomicBoolean calledPush = new AtomicBoolean();
    when(client.send(eq("DatabaseServer:setMaxRecordId"), anyInt(), anyInt(), anyObject(), eq(DatabaseClient.Replica.SPECIFIED), eq(true))).thenAnswer(
        invocationOnMock -> {
          Object[] args = invocationOnMock.getArguments();
          calledPush.set(true);
          assertEquals((long) ((ComObject) args[0]).getLong(ComObject.Tag.MAX_ID), 100L);
          return null;
        });
    server.pushMaxRecordId(cobj, false);
    assertTrue(calledPush.get());
  }

  @Test
  public void testGetIndexSchema() throws IOException {
    DatabaseServer server = new DatabaseServer();
    DatabaseClient client = mock(DatabaseClient.class);
    server.setDatabaseClient(client);
    TableSchema tableSchema = TestUtils.createTable();
    IndexSchema indexSchema = TestUtils.createIndexSchema(tableSchema);
    final AtomicInteger syncCalled = new AtomicInteger();

    doAnswer(invocationOnMock -> { if (syncCalled.incrementAndGet() == 1) {
      server.getCommon().getTables("test").put(tableSchema.getName(), tableSchema);
      tableSchema.getIndices().clear();
    }
    else if (syncCalled.get() == 2) {
      tableSchema.getIndices().put(indexSchema.getName(), indexSchema);
    }
    return null;}).when(client).syncSchema();

    IndexSchema ret = server.getIndexSchema("test", tableSchema.getName(), indexSchema.getName());
    assertEquals(ret.getName(), indexSchema.getName());
    assertEquals(syncCalled.get(), 2);
  }

  @Test
  public void testProcedurePrimary() throws IOException {
    String configStr = IOUtils.toString(DatabaseServerTest.class.getResourceAsStream("/config/config-1-local.json"), "utf-8");
    ObjectNode config = (ObjectNode) new ObjectMapper().readTree(configStr);
    DatabaseServer server = new DatabaseServer();
    server.setConfig(config, "test", "localhost", 9010, true, new AtomicBoolean(), new AtomicBoolean(), "gc.log");

    DatabaseClient client = mock(DatabaseClient.class);
    server.setDatabaseClient(client);

    final AtomicBoolean calledExecute = new AtomicBoolean();
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.SQL, "execute procedure 'com.sonicbase.server.MyStoredProcedure', 100000");
    cobj.put(ComObject.Tag.DB_NAME, "test");

    when(client.send(eq("DatabaseServer:executeProcedure"), anyInt(), anyInt(), anyObject(), eq(DatabaseClient.Replica.DEF))).
        thenAnswer((Answer) invocationOnMock -> {calledExecute.set(true); return null;});

    server.executeProcedurePrimary(cobj, false);

    assertTrue(calledExecute.get());
  }

  @Test
  public void testProcedure() {
    DatabaseClient client = mock(DatabaseClient.class);
    ConnectionProxy conn = mock(ConnectionProxy.class);
    when(conn.getDatabaseClient()).thenReturn(client);

    DatabaseServer server = new DatabaseServer() {
      public ConnectionProxy getConnectionForStoredProcedure(String dbName) throws ClassNotFoundException, SQLException {
        return conn;
      }
    };
    server.setDatabaseClient(client);
    when(client.getCommon()).thenReturn(server.getCommon());

    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.SQL, "execute procedure 'com.sonicbase.server.MyStoredProcedure', 100000");
    cobj.put(ComObject.Tag.DB_NAME, "test");
    cobj.put(ComObject.Tag.ID, 100L);
    server.executeProcedure(cobj, false);

    assertTrue(MyStoredProcedure.called);
  }

  @Test
  public void testAllocateHighestId() throws IOException {
    DatabaseServer server = new DatabaseServer();
    server.setDataDir(new File(System.getProperty("user.dir"), "db-data").getAbsolutePath());

    FileUtils.deleteDirectory(new File(server.getDataDir()));
    DatabaseClient client = mock(DatabaseClient.class);
    server.setDatabaseClient(client);

    ComObject cobj = new ComObject();
    ComObject retObj = server.allocateRecordIds(cobj, false);

    assertEquals((long)retObj.getLong(ComObject.Tag.NEXT_ID), 1L);
    assertEquals((long)retObj.getLong(ComObject.Tag.MAX_ID), 100000L);

    retObj = server.allocateRecordIds(cobj, false);

    assertEquals((long)retObj.getLong(ComObject.Tag.NEXT_ID), 100001L);
    assertEquals((long)retObj.getLong(ComObject.Tag.MAX_ID), 200000L);
  }

  @Test
  public void testReconfigure() throws IOException {
    final AtomicBoolean calledPush = new AtomicBoolean();
    String configStr = IOUtils.toString(DatabaseServerTest.class.getResourceAsStream("/config/config-1-local.json"), "utf-8");
    ObjectNode config = (ObjectNode) new ObjectMapper().readTree(configStr);
    DatabaseServer server = new DatabaseServer() {
      public void pushSchema() {
        calledPush.set(true);
      }
    };
    server.setConfig(config, "test", "localhost", 9010, true, new AtomicBoolean(), new AtomicBoolean(), "gc.log");

    ((ObjectNode)config.withArray("shards").get(0).withArray("replicas").get(0)).put("port", 50);

    File configFile = new File(System.getProperty("user.dir"), "config/config-test.json");
    configFile.getParentFile().mkdirs();
    try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(configFile)))) {
      writer.write(config.toString());
    }

    ComObject cobj = new ComObject();
    server.reconfigureCluster(cobj, false);

    assertEquals(server.getConfig().withArray("shards").get(0).withArray("replicas").get(0).get("port").asInt(), 50);
    assertTrue(calledPush.get());
  }

  @Test
  public void testSyncDbNames() throws IOException {
    final AtomicBoolean calledPush = new AtomicBoolean();
    String configStr = IOUtils.toString(DatabaseServerTest.class.getResourceAsStream("/config/config-1-local.json"), "utf-8");
    ObjectNode config = (ObjectNode) new ObjectMapper().readTree(configStr);
    DatabaseServer server = new DatabaseServer();
    server.setConfig(config, "test", "localhost", 9010, true, new AtomicBoolean(), new AtomicBoolean(), "gc.log");

    DatabaseClient client = mock(DatabaseClient.class);
    server.setDatabaseClient(client);

    FileUtils.deleteDirectory(new File(server.getDataDir()));

    when(client.send(eq("DatabaseServer:getDbNames"), anyInt(), anyInt(), anyObject(), eq(DatabaseClient.Replica.MASTER), eq(true))).
        thenAnswer((Answer) invocationOnMock -> {
          ComObject ret = new ComObject();
          ComArray array = ret.putArray(ComObject.Tag.DB_NAMES, ComObject.Type.STRING_TYPE);
          array.add("my-db"); return ret.serialize();});

    server.syncDbNames();

    File file = new File(server.getDataDir(), "snapshot/0/0/my-db");
    assertTrue(file.exists());

  }

  @Test
  public void testMarkReplicaDead() throws IOException {
    final AtomicBoolean calledPush = new AtomicBoolean();
    String configStr = IOUtils.toString(DatabaseServerTest.class.getResourceAsStream("/config/config-1-local.json"), "utf-8");
    ObjectNode config = (ObjectNode) new ObjectMapper().readTree(configStr);
    DatabaseServer server = new DatabaseServer();
    server.setConfig(config, "test", "localhost", 9010, true, new AtomicBoolean(), new AtomicBoolean(), "gc.log");

    DatabaseClient client = mock(DatabaseClient.class);
    server.setDatabaseClient(client);

    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.REPLICA, 0);
    server.markReplicaDead(cobj, false);

    ServersConfig.Shard[] shards = server.getCommon().getServersConfig().getShards();
    for (ServersConfig.Shard shard : shards) {
      assertTrue(shard.getReplicas()[0].isDead());
    }

    server.markReplicaAlive(cobj, false);
    for (ServersConfig.Shard shard : shards) {
      assertFalse(shard.getReplicas()[0].isDead());
    }
  }

  @Test
  public void testDeathMonitor() throws IOException, InterruptedException {

    final AtomicBoolean calledPush = new AtomicBoolean();
    String configStr = IOUtils.toString(DatabaseServerTest.class.getResourceAsStream("/config/config-1-local.json"), "utf-8");
    ObjectNode config = (ObjectNode) new ObjectMapper().readTree(configStr);
    DatabaseServer server = new DatabaseServer() {
      public void checkHealthOfServer(final int shard, final int replica, final AtomicBoolean isHealthy, final boolean ignoreDeath) throws InterruptedException {
        if (shard == 0 && replica == 0) {
          isHealthy.set(false);
        }
      }
    };
    server.setConfig(config, "test", "localhost", 9010, true, new AtomicBoolean(), new AtomicBoolean(), "gc.log");
    server.setShard(1);
    server.setReplica(1);
    DatabaseClient client = mock(DatabaseClient.class);
    server.setDatabaseClient(client);

    server.startDeathMonitor(5L);
    Thread.sleep(1_000);

    assertTrue(server.getCommon().getServersConfig().getShards()[0].getReplicas()[0].isDead());
  }

}