package com.sonicbase.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.common.*;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.procedure.StoredProcedureContextImpl;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.query.impl.ResultSetImpl;
import com.sonicbase.query.impl.SelectStatementImpl;
import com.sonicbase.schema.Schema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.socket.DatabaseSocketClient;
import com.sonicbase.util.ClientTestUtils;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.Offset;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.*;

public class DatabaseClientTest {
  private DatabaseCommon common;
  private ServersConfig serversConfig;

  @BeforeMethod
  public void beforeMethod() throws IOException {
    common = new DatabaseCommon();
    JsonNode node = new ObjectMapper().readTree(" { \"shards\" : [\n" +
        "    {\n" +
        "      \"replicas\": [\n" +
        "        {\n" +
        "          \"address\": \"localhost\",\n" +
        "          \"port\": 9010,\n" +
        "          \"httpPort\": 8080\n" +
        "        },\n" +
        "        {\n" +
        "          \"address\": \"localhost\",\n" +
        "          \"port\": 9020,\n" +
        "          \"httpPort\": 8080\n" +
        "        },\n" +
        "        {\n" +
        "          \"address\": \"localhost\",\n" +
        "          \"port\": 9030,\n" +
        "          \"httpPort\": 8080\n" +
        "        }\n" +
        "      ]\n" +
        "    },\n" +
        "    {\n" +
        "      \"replicas\": [\n" +
        "        {\n" +
        "          \"address\": \"localhost\",\n" +
        "          \"port\": 10010,\n" +
        "          \"httpPort\": 8080\n" +
        "        },\n" +
        "        {\n" +
        "          \"address\": \"localhost\",\n" +
        "          \"port\": 10020,\n" +
        "          \"httpPort\": 8080\n" +
        "        },\n" +
        "        {\n" +
        "          \"address\": \"localhost\",\n" +
        "          \"port\": 10030,\n" +
        "          \"httpPort\": 8080\n" +
        "        }\n" +
        "      ]\n" +
        "    }\n" +
        "  ]}\n");
    serversConfig = new ServersConfig((ArrayNode) ((ObjectNode)node).withArray("shards"), true, true);
    common.setServersConfig(serversConfig);
  }

  @Test
  public void testCreateDatabase() {

    DatabaseClient client = new DatabaseClient("localhost", 9010, 0, 0, false, common, null) {
      public ComObject sendToMaster(ComObject cobj) {
        assertEquals(cobj.getString(ComObject.Tag.DB_NAME), "test");
        assertEquals(cobj.getString(ComObject.Tag.MASTER_SLAVE), "master");
        assertEquals(cobj.getString(ComObject.Tag.METHOD), "SchemaManager:createDatabase");
        return null;
      }
      protected void syncConfig() {

      }
    };

    client.createDatabase("test");

    client.shutdown();
  }

  @Test
  public void testGetPartitionSize() {
    DatabaseClient client = new DatabaseClient("localhost", 9010, 0, 0, false, common, null) {
      public ComObject send(String verb, int shard, long partition, ComObject cobj, Replica replica) {

        cobj = new ComObject(1);
        ComArray array = cobj.putArray(ComObject.Tag.SIZES, ComObject.Type.OBJECT_TYPE, 1);
        ComObject size0Obj = new ComObject(3);
        size0Obj.put(ComObject.Tag.SHARD, 0);
        size0Obj.put(ComObject.Tag.SIZE, (long)1001);
        size0Obj.put(ComObject.Tag.RAW_SIZE, (long)1001);
        array.add(size0Obj);

        return cobj;
      }
      protected void syncConfig() {

      }
    };

    long size = client.getPartitionSize("test", 0, 0, "table1", "index1");
    assertEquals(size, 1001);
  }

  @Test
  public void testIsRepartitioningComplete() {
    DatabaseClient client = new DatabaseClient("localhost", 9010, 0, 0, false, common, null) {
      public ComObject sendToMaster(String verb, ComObject cobj) {

        if (verb.equals("PartitionManager:isRepartitioningComplete")) {
          ComObject ret = new ComObject(1);
          ret.put(ComObject.Tag.FINISHED, true);
          return ret;
        }
        return null;
      }
      protected void syncConfig() {

      }
    };

    boolean complete = client.isRepartitioningComplete("test");
    assertEquals(complete, true);
  }

  @Test
  public void testTransFlags() {
    final AtomicBoolean committing = new AtomicBoolean();
    final AtomicBoolean called = new AtomicBoolean();
    DatabaseClient client = new DatabaseClient("localhost", 9010, 0, 0, false, common, null) {
      public ComObject[] sendToAllShards(
          final String method,
          final long authUser, final ComObject body, final Replica replica) {
        if (committing.get()) {
          assertEquals(method, "UpdateManager:commit");
          called.set(true);
        }
        else {
          assertEquals(method, "UpdateManager:rollback");
          called.set(true);
        }
        return null;
      }
    };

    assertFalse(client.isExplicitTrans());
    assertFalse(client.isCommitting());
    assertEquals(client.getTransactionId(), 0L);

    client.setIsExplicitTrans(true);
    client.setIsCommitting(true);
    client.setTransactionId(1L);

    assertTrue(client.isExplicitTrans());
    assertTrue(client.isCommitting());
    assertEquals(client.getTransactionId(), 1L);

    called.set(false);
    committing.set(true);
    client.commit("test");
    assertTrue(called.get());

    client.setIsExplicitTrans(true);
    client.setIsCommitting(true);
    client.setTransactionId(1L);

    called.set(false);
    committing.set(false);
    client.rollback("test");
    assertTrue(called.get());

    client.shutdown();
  }

  @Test
  public void testAllocateId() {
    DatabaseClient client = new DatabaseClient("localhost", 9010, 0, 0, false, common, null) {
      public ComObject sendToMaster(String method, ComObject cobj) {
        ComObject retObj = new ComObject(2);
        retObj.put(ComObject.Tag.NEXT_ID, 1001L);
        retObj.put(ComObject.Tag.MAX_ID, 2000L);
        return retObj;
      }
      public ComObject send(String method,
                            int shard, long authUser, ComObject body, Replica replica) {

        ComObject retObj = new ComObject(1);
        try {
          TableSchema tableSchema = ClientTestUtils.createTable();
          DatabaseCommon common = new DatabaseCommon();
          common.getTables("test").put(tableSchema.getName(), tableSchema);
          common.getTablesById("test").put(tableSchema.getTableId(), tableSchema);
          byte[] bytes = common.serializeSchema((short)1000);
          retObj.put(ComObject.Tag.SCHEMA_BYTES, bytes);
          return retObj;
        }
        catch (IOException e) {
          throw new DatabaseException(e);
        }
      }
    };

    assertEquals(client.allocateId("test"), 1001L);
  }

  @Test
  public void testBackupRestoreComplete() {
    final DatabaseCommon common = new DatabaseCommon();

    ServersConfig config = new ServersConfig();
    config.setShards(new ServersConfig.Shard[]{
            new ServersConfig.Shard(new ServersConfig.Host[]{
                new ServersConfig.Host("localhost", 9010, true),
                new ServersConfig.Host("localhost", 9060, false)
            }),
            new ServersConfig.Shard(new ServersConfig.Host[]{
                new ServersConfig.Host("localhost", 10010, false),
                new ServersConfig.Host("localhost", 10060, true)
            })
        });
    common.setServersConfig(config);

    DatabaseClient client = new DatabaseClient("localhost", 9010, 0, 0, false, common, null) {
      public ComObject sendToMaster(String method, ComObject cobj) {
        ComObject retObj = new ComObject(2);
        retObj.put(ComObject.Tag.NEXT_ID, 1001L);
        retObj.put(ComObject.Tag.MAX_ID, 2000L);
        return retObj;
      }
      public ComObject send(String method,
                            int shard, long authUser, ComObject body, Replica replica) {

        if (method.equals("BackupManager:isEntireRestoreComplete")) {
          ComObject retObj = new ComObject(1);
          retObj.put(ComObject.Tag.IS_COMPLETE, true);
          return retObj;
        }
        if (method.equals("BackupManager:isEntireBackupComplete")) {
          ComObject retObj = new ComObject(1);
          retObj.put(ComObject.Tag.IS_COMPLETE, true);
          return retObj;
        }
        if (method.equals("DatabaseServer:getConfig")) {
          ComObject retObj = new ComObject(1);
          try {
            byte[] bytes = common.serializeConfig(SERIALIZATION_VERSION);
            retObj.put(ComObject.Tag.CONFIG_BYTES, bytes);
            return retObj;
          }
          catch (IOException e) {
            throw new DatabaseException(e);
          }
        }

        ComObject retObj = new ComObject(1);
        try {
          TableSchema tableSchema = ClientTestUtils.createTable();
          DatabaseCommon common = new DatabaseCommon();
          common.getTables("test").put(tableSchema.getName(), tableSchema);
          common.getTablesById("test").put(tableSchema.getTableId(), tableSchema);
          byte[] bytes = common.serializeSchema((short)1000);
          retObj.put(ComObject.Tag.SCHEMA_BYTES, bytes);
          return retObj;
        }
        catch (IOException e) {
          throw new DatabaseException(e);
        }
      }
    };

    assertTrue(client.isBackupComplete());
    assertTrue(client.isRestoreComplete());
    client.getConfig();
    assertTrue(client.getCommon().getServersConfig().getShards()[0].getReplicas()[0].isDead());
  }

  @Test
  public void testSyncSchema() {

    DatabaseClient client = new DatabaseClient("localhost", 9010, 0, 0, false, common, null) {
      public ComObject sendToMaster(ComObject body) {
        return null;
      }
      public ComObject send(String method,
                            int shard, long authUser, ComObject body, Replica replica) {

        ComObject retObj = new ComObject(1);
        try {
          TableSchema tableSchema = ClientTestUtils.createTable();
          DatabaseCommon common = new DatabaseCommon();
          common.getTables("test").put(tableSchema.getName(), tableSchema);
          common.getTablesById("test").put(tableSchema.getTableId(), tableSchema);
          byte[] bytes = common.serializeSchema((short)1000);
          retObj.put(ComObject.Tag.SCHEMA_BYTES, bytes);
          return retObj;
        }
        catch (IOException e) {
          throw new DatabaseException(e);
        }
      }
    };

    common.getServersConfig().getShards()[0].getReplicas()[0].setDead(true);
    common.getServersConfig().getShards()[0].getReplicas()[1].setDead(true);

    client.doSyncSchema();

    assertTrue(common.getTables("test").containsKey("table1"));
  }

  @Test
  public void testLimitOffset() {

    Limit limit = new Limit();
    limit.setRowCount(50);
    Offset offset = new Offset();
    offset.setOffset(1000);
    String removed = DatabaseClient.removeOffsetAndLimit("select * from persons limit 50 offset 1000", limit, offset);
    System.out.println(removed);
    assertEquals(removed, "select * from persons limit x offset x");
  }

  @Test
  public void testLimitOffset2() {

    Offset offset = new Offset();
    offset.setOffset(10);
    String removed = DatabaseClient.removeOffsetAndLimit("select * from persons offset 10", null, offset);
    System.out.println(removed);
    assertEquals(removed, "select * from persons offset x");
  }

  @Test
  public void testLimitOffset3() {

    Limit limit = new Limit();
    limit.setRowCount(50);
    String removed = DatabaseClient.removeOffsetAndLimit("select * from persons limit 50", limit, null);
    System.out.println(removed);
    assertEquals(removed, "select * from persons limit x");
  }

  @Test
  public void testExecuteQuery() throws SQLException {
    final AtomicBoolean handled = new AtomicBoolean();
    DatabaseClient client = new DatabaseClient("localhost", 9010, 0, 0, false, common, null) {
      public StatementHandler getHandler(Statement statement) {
        return new StatementHandler() {
          @Override
          public Object execute(String dbName, ParameterHandler parms, String sqlToUse, Statement statement, SelectStatementImpl.Explain explain, Long sequence0, Long sequence1, Short sequence2, boolean restrictToThisServer, StoredProcedureContextImpl procedureContext, int schemaRetryCount) {
            handled.set(true);
            return null;
          }
        };
      }

      public ComObject sendToMaster(ComObject body) {
        return null;
      }
      public void syncSchema() {
      }
    };
    DatabaseClient.setSharedClient(client);
    TableSchema tableSchema = ClientTestUtils.createTable();
    common.getTables("test").put(tableSchema.getName(), tableSchema);
    common.getTablesById("test").put(tableSchema.getTableId(), tableSchema);

    common.getDatabases().put("test", new Schema());
    client.executeQuery("test", "select * from table1 limit 5 offset 10", new ParameterHandler(), false, null, false);

    assertTrue(handled.get());
  }

  @Test
  public void testExecuteQueryExplain() throws SQLException {
    final AtomicBoolean handled = new AtomicBoolean();
    DatabaseClient client = new DatabaseClient("localhost", 9010, 0, 0, false, common, null) {
      public StatementHandler getHandler(Statement statement) {
        return new StatementHandler() {
          @Override
          public Object execute(String dbName, ParameterHandler parms, String sqlToUse, Statement statement, SelectStatementImpl.Explain explain, Long sequence0, Long sequence1, Short sequence2, boolean restrictToThisServer, StoredProcedureContextImpl procedureContext, int schemaRetryCount) {
            handled.set(true);
            ResultSetImpl ret = new ResultSetImpl(new String[]{"explain"});
            ret.setCurrPos(0);
            return ret;
          }
        };
      }

      public ComObject sendToMaster(ComObject body) {
        return null;
      }
      public void syncSchema() {
      }
    };
    DatabaseClient.setSharedClient(client);
    TableSchema tableSchema = ClientTestUtils.createTable();
    common.getTables("test").put(tableSchema.getName(), tableSchema);
    common.getTablesById("test").put(tableSchema.getTableId(), tableSchema);

    common.getDatabases().put("test", new Schema());
    ResultSetImpl ret = (ResultSetImpl) client.executeQuery("test", "explain select * from table1 limit 5 offset 10", new ParameterHandler(), false, null, false);

    assertEquals(ret.getString(1), "explain");
    assertTrue(handled.get());
  }

  @Test
  public void testSend() {
    final AtomicBoolean calledSync = new AtomicBoolean();
    final Set<String> called = new HashSet<>();
    ServersConfig.Shard[] shards = serversConfig.getShards();
    DatabaseClient client = new DatabaseClient("localhost", 9010, 0, 0, false, common, null) {
      public void syncSchema() {
        calledSync.set(true);
      }
      public byte[] doSendOnSocket(List<DatabaseSocketClient.Request> requests) {
        for(DatabaseSocketClient.Request request : requests) {
          called.add(request.getHostPort());
        }
        return null;
      }
    };

    DatabaseClient.Server[][] servers = new DatabaseClient.Server[shards.length][];
    for (int i = 0; i < servers.length; i++) {
      ServersConfig.Shard shard = shards[i];
      servers[i] = new DatabaseClient.Server[shard.getReplicas().length];
      for (int j = 0; j < servers[i].length; j++) {
        ServersConfig.Host replicaHost = shard.getReplicas()[j];

        servers[i][j] = new DatabaseClient.Server(replicaHost.getaddress(), replicaHost.getPort()) {
          public byte[] doSend(String batchKey, ComObject body) {
            called.add(replicaHost.getaddress() + ":" + replicaHost.getPort());
            return null;
          }

          public byte[] doSend(String batchKey, byte[] body) {
            called.add(replicaHost.getaddress() + ":" + replicaHost.getPort());
            return null;
          }
        };
      }
    }
    client.setServers(servers);

    called.clear();
    client.send("method", servers[0], 0, 0, new ComObject(1),
        DatabaseClient.Replica.SPECIFIED, true);

    assertEquals(called.iterator().next(), "localhost:9010");
    assertEquals(called.size(), 1);

    called.clear();
    client.send("method", servers[0], 0, 0, new ComObject(1),
        DatabaseClient.Replica.DEF, true);
    client.send("method", servers[0], 0, 0, new ComObject(1),
        DatabaseClient.Replica.DEF, true);
    client.send("method", servers[0], 0, 0, new ComObject(1),
        DatabaseClient.Replica.DEF, true);
    assertEquals(called.size(), 3);

    called.clear();
    client.send("method", servers[0], 0, 0, new ComObject(1),
        DatabaseClient.Replica.MASTER, true);

    assertEquals(called.iterator().next(), "localhost:9010");
    assertEquals(called.size(), 1);

    called.clear();
    client.send("method", servers[0], 0, 0, new ComObject(1),
        DatabaseClient.Replica.ALL, true);
    assertEquals(called.size(), 3);


    called.clear();
    client.send("UpdateManager:deleteRecord", servers[0], 0, 0, new ComObject(1),
        DatabaseClient.Replica.SPECIFIED, true);

    assertEquals(called.iterator().next(), "localhost:9010");
    assertEquals(called.size(), 1);


    servers[0][1].setDead(true);
    called.clear();
    client.send("UpdateManager:deleteRecord", servers[0], 0, 1, new ComObject(1),
        DatabaseClient.Replica.SPECIFIED, false);

    assertEquals(called.iterator().next(), "localhost:9010");
    assertEquals(called.size(), 1);

    servers[0][1].setDead(false);

    called.clear();
    client.send("UpdateManager:deleteRecord", servers[0], 0, 0, new ComObject(1),
        DatabaseClient.Replica.DEF, false);

    assertEquals(called.size(), 3);

    servers[0][0].setDead(true);

    called.clear();
    client.send("UpdateManager:deleteRecord", servers[0], 0, 0, new ComObject(1),
        DatabaseClient.Replica.DEF, false);

    assertTrue(called.contains("localhost:9020"));
    assertTrue(called.contains("localhost:9030"));
    assertEquals(called.size(), 2);

    servers[0][0].setDead(false);
    servers[0][1].setDead(true);

    called.clear();
    client.send("UpdateManager:deleteRecord", servers[0], 0, 0, new ComObject(1),
        DatabaseClient.Replica.DEF, false);

    assertTrue(called.contains("localhost:9010"));
    assertTrue(called.contains("localhost:9030"));
    assertEquals(called.size(), 2);
  }

  @Test
  public void testSchemaOutOfSync() {
    final AtomicBoolean calledSync = new AtomicBoolean();
    final Set<String> called = new HashSet<>();
    ServersConfig.Shard[] shards = serversConfig.getShards();
    DatabaseClient client = new DatabaseClient("localhost", 9010, 0, 0, false, common, null) {
      public void syncSchema() {
        calledSync.set(true);
      }
      public void syncSchema(Integer serverVersion) {
        calledSync.set(true);
      }
      public byte[] doSendOnSocket(List<DatabaseSocketClient.Request> requests) {
        for(DatabaseSocketClient.Request request : requests) {
          called.add(request.getHostPort());
        }
        return null;
      }
    };

    calledSync.set(false);
    common.setSchemaVersion(1);
    DatabaseException e = new DatabaseException(
        "msg", new DatabaseException("msg", new RuntimeException("SchemaOutOfSyncException currVer:10:")));
    try {
      client.handleSchemaOutOfSyncException(e);
      fail();
    }
    catch (SchemaOutOfSyncException e1) {

    }
    assertTrue(calledSync.get());


    calledSync.set(false);
    common.setSchemaVersion(1);
    e = new DatabaseException(
        "msg", new SchemaOutOfSyncException("SchemaOutOfSyncException currVer:10:"));
    try {
      client.handleSchemaOutOfSyncException(e);
      fail();
    }
    catch (SchemaOutOfSyncException e1) {

    }
    assertTrue(calledSync.get());


    calledSync.set(false);
    common.setSchemaVersion(1);
    e = new SchemaOutOfSyncException("SchemaOutOfSyncException currVer:10:");
    try {
      client.handleSchemaOutOfSyncException(e);
      fail();
    }
    catch (SchemaOutOfSyncException e1) {

    }
    assertTrue(calledSync.get());
  }

  @Test
  public void testSendToMaster() {
    AtomicBoolean calledGetSchema = new AtomicBoolean();
    final AtomicInteger callCount = new AtomicInteger();
    DatabaseClient client = new DatabaseClient("localhost", 9010, 0, 0, false, common, null) {

      public ComObject send(String method,
                            int shard, long authUser, ComObject body, Replica replica) {
        if (shard == 0 && authUser == 1) {
          calledGetSchema.set(true);
        }
        return null;
      }

      public ComObject send(
          String batchKey, Server[] replicas, int shard, long authUser,
          ComObject body, Replica replica) {
        if (callCount.incrementAndGet() == 1) {
          throw new DatabaseException();
        }
        return null;
      }
    };

    ComObject cobj = new ComObject(1);
    cobj.put(ComObject.Tag.METHOD, "DatabaseServer:heartbeat");
    client.sendToMaster(cobj);

    assertTrue(calledGetSchema.get());
  }

  @Test
  public void testSendToAllShards() {
    final AtomicInteger callCount = new AtomicInteger();
    DatabaseClient client = new DatabaseClient("localhost", 9010, 0, 0, false, common, null) {

      public ComObject send(String method,
                            int shard, long authUser, ComObject body, Replica replica, boolean ignoreDeath) {
        callCount.incrementAndGet();
        return null;
      }
    };

    ComObject cobj = new ComObject(1);
    cobj.put(ComObject.Tag.METHOD, "DatabaseServer:heartbeat");
    client.sendToAllShards(null, 0, cobj, DatabaseClient.Replica.SPECIFIED);

    assertEquals(callCount.get(), 2);
  }

  @Test
  public void testSyncConfig() {
    final AtomicInteger callCount = new AtomicInteger();
    DatabaseClient client = new DatabaseClient("localhost", 9010, 0, 0, false, common, null) {

      public ComObject send(String method,
                            int shard, long authUser, ComObject body, Replica replica) {
        if (method.equals("DatabaseServer:getConfig")) {
          callCount.incrementAndGet();
        }
        return null;
      }
    };

    client.syncConfig();

    assertEquals(callCount.get(), 2);
  }
}
