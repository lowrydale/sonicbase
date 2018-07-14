/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.common.ComObject;
import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.common.SchemaOutOfSyncException;
import com.sonicbase.common.ServersConfig;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.jdbcdriver.QueryType;
import com.sonicbase.procedure.StoredProcedureContextImpl;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.query.impl.SelectStatementImpl;
import com.sonicbase.schema.Schema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.server.IndexLookupTest;
import com.sonicbase.socket.DatabaseSocketClient;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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
        "          \"publicAddress\": \"localhost\",\n" +
        "          \"privateAddress\": \"localhost\",\n" +
        "          \"port\": 9010,\n" +
        "          \"httpPort\": 8080\n" +
        "        },\n" +
        "        {\n" +
        "          \"publicAddress\": \"localhost\",\n" +
        "          \"privateAddress\": \"localhost\",\n" +
        "          \"port\": 9020,\n" +
        "          \"httpPort\": 8080\n" +
        "        },\n" +
        "        {\n" +
        "          \"publicAddress\": \"localhost\",\n" +
        "          \"privateAddress\": \"localhost\",\n" +
        "          \"port\": 9030,\n" +
        "          \"httpPort\": 8080\n" +
        "        }\n" +
        "      ]\n" +
        "    },\n" +
        "    {\n" +
        "      \"replicas\": [\n" +
        "        {\n" +
        "          \"publicAddress\": \"localhost\",\n" +
        "          \"privateAddress\": \"localhost\",\n" +
        "          \"port\": 10010,\n" +
        "          \"httpPort\": 8080\n" +
        "        },\n" +
        "        {\n" +
        "          \"publicAddress\": \"localhost\",\n" +
        "          \"privateAddress\": \"localhost\",\n" +
        "          \"port\": 10020,\n" +
        "          \"httpPort\": 8080\n" +
        "        },\n" +
        "        {\n" +
        "          \"publicAddress\": \"localhost\",\n" +
        "          \"privateAddress\": \"localhost\",\n" +
        "          \"port\": 10030,\n" +
        "          \"httpPort\": 8080\n" +
        "        }\n" +
        "      ]\n" +
        "    }\n" +
        "  ]}\n");
    serversConfig = new ServersConfig("test", (ArrayNode) ((ObjectNode)node).withArray("shards"), 1, true, true);
    common.setServersConfig(serversConfig);
  }

  @Test
  public void testCreateDatabase() throws IOException {

    DatabaseClient client = new DatabaseClient("localhost", 9010, 0, 0, false, common, null) {
      public byte[] sendToMaster(ComObject cobj) {
        assertEquals(cobj.getString(ComObject.Tag.dbName), "test");
        assertEquals(cobj.getString(ComObject.Tag.masterSlave), "master");
        assertEquals(cobj.getString(ComObject.Tag.method), "SchemaManager:createDatabase");
        return null;
      }
      public String getCluster() {
        return "test";
      }
      protected void syncConfig() {

      }
    };

    client.createDatabase("test");

    client.shutdown();
  }

  @Test
  public void testTransFlags() {
    final AtomicBoolean committing = new AtomicBoolean();
    final AtomicBoolean called = new AtomicBoolean();
    DatabaseClient client = new DatabaseClient("localhost", 9010, 0, 0, false, common, null) {
      public byte[][] sendToAllShards(
          final String method,
          final long auth_user, final ComObject body, final Replica replica) {
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

      public String getCluster() {
        return "test";
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
    client.commit("test", null);
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
  public void testSyncSchema() {

    DatabaseClient client = new DatabaseClient("localhost", 9010, 0, 0, false, common, null) {
      public byte[] sendToMaster(ComObject body) {
        return null;
      }
      public byte[] send(String method,
                         int shard, long auth_user, ComObject body, Replica replica) {

        ComObject retObj = new ComObject();
        try {
          TableSchema tableSchema = IndexLookupTest.createTable();
          DatabaseCommon common = new DatabaseCommon();
          common.getTables("test").put(tableSchema.getName(), tableSchema);
          common.getTablesById("test").put(tableSchema.getTableId(), tableSchema);
          byte[] bytes = common.serializeSchema((short)1000);
          retObj.put(ComObject.Tag.schemaBytes, bytes);
          return retObj.serialize();
        }
        catch (IOException e) {
          throw new DatabaseException(e);
        }
      }

      public String getCluster() {
        return "test";
      }
    };

    common.getServersConfig().getShards()[0].getReplicas()[0].setDead(true);
    common.getServersConfig().getShards()[0].getReplicas()[1].setDead(true);

    client.doSyncSchema(1);

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
          public Object execute(String dbName, ParameterHandler parms, String sqlToUse, Statement statement, SelectStatementImpl.Explain explain, Long sequence0, Long sequence1, Short sequence2, boolean restrictToThisServer, StoredProcedureContextImpl procedureContext, int schemaRetryCount) throws SQLException {
            handled.set(true);
            return null;
          }
        };
      }

      public byte[] sendToMaster(ComObject body) {
        return null;
      }
      public String getCluster() {
        return "test";
      }
      public void syncSchema() {
      }
    };
    DatabaseClient.getSharedClients().put("test", client);
    TableSchema tableSchema = IndexLookupTest.createTable();
    common.getTables("test").put(tableSchema.getName(), tableSchema);
    common.getTablesById("test").put(tableSchema.getTableId(), tableSchema);

    common.getDatabases().put("test", new Schema());
    client.executeQuery("test", QueryType.query1, "select * from table1 limit 5 offset 10", new ParameterHandler(), false, null, false);

    assertTrue(handled.get());
  }

  @Test
  public void testSend() {
    final AtomicBoolean calledSync = new AtomicBoolean();
    final Set<String> called = new HashSet<>();
    ServersConfig.Shard[] shards = serversConfig.getShards();
    DatabaseClient client = new DatabaseClient("localhost", 9010, 0, 0, false, common, null) {
      public String getCluster() {
        return "test";
      }
      public void syncSchema() {
        calledSync.set(true);
      }
      public byte[] do_sendOnSocket(List<DatabaseSocketClient.Request> requests) {
        for(DatabaseSocketClient.Request request : requests) {
          called.add(request.hostPort);
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

        servers[i][j] = new DatabaseClient.Server(replicaHost.getPrivateAddress(), replicaHost.getPort()) {
          public byte[] do_send(String batchKey, ComObject body) {
            called.add(replicaHost.getPrivateAddress() + ":" + replicaHost.getPort());
            return null;
          }

          public byte[] do_send(String batchKey, byte[] body) {
            called.add(replicaHost.getPrivateAddress() + ":" + replicaHost.getPort());
            return null;
          }
        };
      }
    }
    client.setServers(servers);

    called.clear();
    client.send("method", servers[0], 0, 0, new ComObject(),
        DatabaseClient.Replica.specified, true);

    assertEquals(called.iterator().next(), "localhost:9010");
    assertEquals(called.size(), 1);

    called.clear();
    client.send("method", servers[0], 0, 0, new ComObject(),
        DatabaseClient.Replica.def, true);
    client.send("method", servers[0], 0, 0, new ComObject(),
        DatabaseClient.Replica.def, true);
    client.send("method", servers[0], 0, 0, new ComObject(),
        DatabaseClient.Replica.def, true);
    assertEquals(called.size(), 3);

    called.clear();
    client.send("method", servers[0], 0, 0, new ComObject(),
        DatabaseClient.Replica.master, true);

    assertEquals(called.iterator().next(), "localhost:9010");
    assertEquals(called.size(), 1);

    called.clear();
    client.send("method", servers[0], 0, 0, new ComObject(),
        DatabaseClient.Replica.all, true);
    assertEquals(called.size(), 3);


    called.clear();
    client.send("UpdateManager:deleteRecord", servers[0], 0, 0, new ComObject(),
        DatabaseClient.Replica.specified, true);

    assertEquals(called.iterator().next(), "localhost:9010");
    assertEquals(called.size(), 1);


    servers[0][1].setDead(true);
    called.clear();
    client.send("UpdateManager:deleteRecord", servers[0], 0, 1, new ComObject(),
        DatabaseClient.Replica.specified, false);

    assertEquals(called.iterator().next(), "localhost:9010");
    assertEquals(called.size(), 1);

    servers[0][1].setDead(false);

    called.clear();
    client.send("UpdateManager:deleteRecord", servers[0], 0, 0, new ComObject(),
        DatabaseClient.Replica.def, false);

    assertEquals(called.size(), 3);

    servers[0][0].setDead(true);

    called.clear();
    client.send("UpdateManager:deleteRecord", servers[0], 0, 0, new ComObject(),
        DatabaseClient.Replica.def, false);

    assertTrue(called.contains("localhost:9020"));
    assertTrue(called.contains("localhost:9030"));
    assertEquals(called.size(), 2);

    servers[0][0].setDead(false);
    servers[0][1].setDead(true);

    called.clear();
    client.send("UpdateManager:deleteRecord", servers[0], 0, 0, new ComObject(),
        DatabaseClient.Replica.def, false);

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
      public String getCluster() {
        return "test";
      }
      public void syncSchema() {
        calledSync.set(true);
      }
      public void syncSchema(Integer serverVersion) {
        calledSync.set(true);
      }
      public byte[] do_sendOnSocket(List<DatabaseSocketClient.Request> requests) {
        for(DatabaseSocketClient.Request request : requests) {
          called.add(request.hostPort);
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

      public byte[] send(String method,
                         int shard, long auth_user, ComObject body, Replica replica) {
        if (shard == 0 && auth_user == 1) {
          calledGetSchema.set(true);
        }
        return null;
      }

      public byte[] send(
          String batchKey, Server[] replicas, int shard, long auth_user,
          ComObject body, Replica replica) {
        if (callCount.incrementAndGet() == 1) {
          throw new DatabaseException();
        }
        return null;
      }

      public String getCluster() {
        return "test";
      }
    };

    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.method, "DatabaseServer:heartbeat");
    client.sendToMaster(cobj);

    assertTrue(calledGetSchema.get());
  }

  @Test
  public void testSendToAllShards() {
    final AtomicInteger callCount = new AtomicInteger();
    DatabaseClient client = new DatabaseClient("localhost", 9010, 0, 0, false, common, null) {

      public byte[] send(String method,
                         int shard, long auth_user, ComObject body, Replica replica, boolean ignoreDeath) {
        callCount.incrementAndGet();
        return null;
      }

      public String getCluster() {
        return "test";
      }
    };

    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.method, "DatabaseServer:heartbeat");
    client.sendToAllShards(null, 0, cobj, DatabaseClient.Replica.specified);

    assertEquals(callCount.get(), 2);
  }

  @Test
  public void testSyncConfig() {
    final AtomicInteger callCount = new AtomicInteger();
    DatabaseClient client = new DatabaseClient("localhost", 9010, 0, 0, false, common, null) {

      public byte[] send(String method,
                         int shard, long auth_user, ComObject body, Replica replica) {
        if (method.equals("DatabaseServer:getConfig")) {
          callCount.incrementAndGet();
        }
        return null;
      }

      public String getCluster() {
        return "test";
      }
    };

    client.syncConfig();

    assertEquals(callCount.get(), 1);
  }
}
