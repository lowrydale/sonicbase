/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.common.ComObject;
import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.common.ServersConfig;
import com.sonicbase.query.ResultSet;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.server.DatabaseServer;
import com.sonicbase.server.DatabaseServerTest;
import com.sonicbase.server.IndexLookupTest;
import com.sonicbase.server.PartitionManager;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class DescribeStatementHandlerTest {

  @Test
  public void test() throws InterruptedException, ExecutionException, IOException {
    DatabaseClient client = mock(DatabaseClient.class);
    DatabaseCommon common = new DatabaseCommon();
    when(client.getCommon()).thenReturn(common);
    TableSchema tableSchema = IndexLookupTest.createTable();
    IndexSchema indexSchema = IndexLookupTest.createIndexSchema(tableSchema);
    common.getTables("test").put(tableSchema.getName(), tableSchema);

    DescribeStatementHandler handler = new DescribeStatementHandler(client);
    ResultSet ret = handler.doDescribe("test", "describe table table1");
    StringBuilder builder = new StringBuilder();
    while (ret.next()) {
      String line = ret.getString(1);
      builder.append(line).append("\n");
    }

    String expected = "-----------------------------------\n" +
        "| Name    | Type          | Width |\n" +
        "-----------------------------------\n" +
        "| _id     | BIGINT        | 0     |\n" +
        "| field1  | BIGINT        | 0     |\n" +
        "| field2  | VARCHAR       | 0     |\n" +
        "| field3  | TIMESTAMP     | 0     |\n" +
        "| field4  | INTEGER       | 0     |\n" +
        "| field5  | SMALLINT      | 0     |\n" +
        "| field6  | TINYINT       | 0     |\n" +
        "| field7  | CHAR          | 0     |\n" +
        "| field8  | NCHAR         | 0     |\n" +
        "| field9  | FLOAT         | 0     |\n" +
        "| field10 | REAL          | 0     |\n" +
        "| field11 | DOUBLE        | 0     |\n" +
        "| field12 | BOOLEAN       | 0     |\n" +
        "| field13 | BIT           | 0     |\n" +
        "| field14 | VARCHAR       | 0     |\n" +
        "| field15 | CLOB          | 0     |\n" +
        "| field16 | NCLOB         | 0     |\n" +
        "| field17 | LONGVARCHAR   | 0     |\n" +
        "| field18 | NVARCHAR      | 0     |\n" +
        "| field19 | LONGNVARCHAR  | 0     |\n" +
        "| field20 | LONGVARBINARY | 0     |\n" +
        "| field21 | VARBINARY     | 0     |\n" +
        "| field22 | BLOB          | 0     |\n" +
        "| field23 | NUMERIC       | 0     |\n" +
        "| field24 | DECIMAL       | 0     |\n" +
        "| field25 | DATE          | 0     |\n" +
        "| field26 | TIME          | 0     |\n" +
        "| field27 | TIMESTAMP     | 0     |\n" +
        "-----------------------------------\n" +
        "Index=_primarykey\n" +
        "---------------------------\n" +
        "| Name   | Type   | Width |\n" +
        "---------------------------\n" +
        "| field1 | BIGINT | 0     |\n" +
        "---------------------------\n";

    assertEquals(builder.toString(), expected);
  }


  @Test
  public void testIndex() throws InterruptedException, ExecutionException, IOException {
    DatabaseClient client = mock(DatabaseClient.class);
    DatabaseCommon common = new DatabaseCommon();
    when(client.getCommon()).thenReturn(common);
    TableSchema tableSchema = IndexLookupTest.createTable();
    IndexSchema indexSchema = IndexLookupTest.createIndexSchema(tableSchema);
    common.getTables("test").put(tableSchema.getName(), tableSchema);

    DescribeStatementHandler handler = new DescribeStatementHandler(client);
    ResultSet ret = handler.doDescribe("test", "describe index table1._primarykey");
    StringBuilder builder = new StringBuilder();
    while (ret.next()) {
      String line = ret.getString(1);
      builder.append(line).append("\n");
    }

    String expected = "---------------------------\n" +
        "| Name   | Type   | Width |\n" +
        "---------------------------\n" +
        "| field1 | BIGINT | 0     |\n" +
        "---------------------------\n";

    assertEquals(builder.toString(), expected);
  }

  @Test
  public void testTables() throws InterruptedException, ExecutionException, IOException {
    DatabaseClient client = mock(DatabaseClient.class);
    DatabaseCommon common = new DatabaseCommon();
    when(client.getCommon()).thenReturn(common);
    TableSchema tableSchema = IndexLookupTest.createTable();
    IndexSchema indexSchema = IndexLookupTest.createIndexSchema(tableSchema);
    common.getTables("test").put(tableSchema.getName(), tableSchema);

    DescribeStatementHandler handler = new DescribeStatementHandler(client);
    ResultSet ret = handler.doDescribe("test", "describe tables");
    StringBuilder builder = new StringBuilder();
    while (ret.next()) {
      String line = ret.getString(1);
      builder.append(line).append("\n");
    }

    String expected = "table1\n";

    assertEquals(builder.toString(), expected);
  }

  @Test
  public void testShards() throws InterruptedException, ExecutionException, IOException {
    DatabaseClient client = mock(DatabaseClient.class);
    DatabaseCommon common = new DatabaseCommon();
    when(client.getCommon()).thenReturn(common);
    TableSchema tableSchema = IndexLookupTest.createTable();
    IndexSchema indexSchema = IndexLookupTest.createIndexSchema(tableSchema);
    common.getTables("test").put(tableSchema.getName(), tableSchema);

    DescribeStatementHandler handler = new DescribeStatementHandler(client);
    ResultSet ret = handler.doDescribe("test", "describe shards");
    StringBuilder builder = new StringBuilder();
    while (ret.next()) {
      String line = ret.getString(1);
      builder.append(line).append("\n");
    }

    String expected = "Table=table1, Index=_primarykey, shard=0, key=[null], lastKey=[null]\n";

    assertEquals(builder.toString(), expected);
  }

  @Test
  public void testRepartitioner() throws InterruptedException, ExecutionException, IOException {
    String configStr = IOUtils.toString(DatabaseServerTest.class.getResourceAsStream("/config/config-1-local.json"), "utf-8");
    ObjectNode config = (ObjectNode) new ObjectMapper().readTree(configStr);
    com.sonicbase.server.DatabaseServer server = new DatabaseServer() {
      @Override
      public Logger getErrorLogger() {
        Logger mockLogger = mock(Logger.class);
        doAnswer((Answer<Object>) invocationOnMock -> {
          return null;
        }).when(mockLogger).error(anyObject());
        return mockLogger;
      }
    };
    server.setConfig(config, "test", "localhost", 9010, true, new AtomicBoolean(), new AtomicBoolean(), "gc.log", true);
    DatabaseClient client = mock(DatabaseClient.class);
    server.setDatabaseClient(client);

    DatabaseCommon common = new DatabaseCommon();
    when(client.getCommon()).thenReturn(common);
    TableSchema tableSchema = IndexLookupTest.createTable();
    IndexSchema indexSchema = IndexLookupTest.createIndexSchema(tableSchema);
    common.getTables("test").put(tableSchema.getName(), tableSchema);

    when(client.sendToMaster(eq("PartitionManager:getRepartitionerState"), anyObject())).thenAnswer(
        (Answer) invocationOnMock -> {
          PartitionManager mgr = server.getPartitionManager();
          return mgr.getRepartitionerState(new ComObject(), false).serialize();
        });
    DescribeStatementHandler handler = new DescribeStatementHandler(client);
    ResultSet ret = handler.doDescribe("test", "describe repartitioner");
    StringBuilder builder = new StringBuilder();
    while (ret.next()) {
      String line = ret.getString(1);
      builder.append(line).append("\n");
    }

    String expected = "state=idle\n";

    assertEquals(builder.toString(), expected);
  }

  @Test
  public void testServerStats() throws InterruptedException, ExecutionException, IOException {
    String configStr = IOUtils.toString(DatabaseServerTest.class.getResourceAsStream("/config/config-1-local.json"), "utf-8");
    ObjectNode config = (ObjectNode) new ObjectMapper().readTree(configStr);
    com.sonicbase.server.DatabaseServer server = new DatabaseServer() {
      @Override
      public Logger getErrorLogger() {
        Logger mockLogger = mock(Logger.class);
        doAnswer((Answer<Object>) invocationOnMock -> {
          return null;
        }).when(mockLogger).error(anyObject());
        return mockLogger;
      }
    };
    server.setConfig(config, "test", "localhost", 9010, true, new AtomicBoolean(), new AtomicBoolean(), "gc.log", true);
    DatabaseClient client = mock(DatabaseClient.class);
    server.setDatabaseClient(client);

    DatabaseCommon common = new DatabaseCommon();
    when(client.getCommon()).thenReturn(common);
    TableSchema tableSchema = IndexLookupTest.createTable();
    IndexSchema indexSchema = IndexLookupTest.createIndexSchema(tableSchema);
    common.getTables("test").put(tableSchema.getName(), tableSchema);

    when(client.sendToMaster(eq("PartitionManager:getRepartitionerState"), anyObject())).thenAnswer(
        (Answer) invocationOnMock -> {
          PartitionManager mgr = server.getPartitionManager();
          return mgr.getRepartitionerState(new ComObject(), false).serialize();
        });
    DescribeStatementHandler handler = new DescribeStatementHandler(client);
    ResultSet ret = handler.doDescribe("test", "describe server stats");
    StringBuilder builder = new StringBuilder();
    while (ret.next()) {
      String line = ret.getString(1);
      builder.append(line).append("\n");
    }

    String expected = "";

    assertEquals(builder.toString(), expected);
  }

  @Test
  public void testServerHealth() throws InterruptedException, ExecutionException, IOException {
    String configStr = IOUtils.toString(DatabaseServerTest.class.getResourceAsStream("/config/config-1-local.json"), "utf-8");
    ObjectNode config = (ObjectNode) new ObjectMapper().readTree(configStr);
    com.sonicbase.server.DatabaseServer server = new DatabaseServer() {
      @Override
      public Logger getErrorLogger() {
        Logger mockLogger = mock(Logger.class);
        doAnswer((Answer<Object>) invocationOnMock -> {
          return null;
        }).when(mockLogger).error(anyObject());
        return mockLogger;
      }
    };
    server.setConfig(config, "test", "localhost", 9010, true, new AtomicBoolean(), new AtomicBoolean(), "gc.log", true);
    DatabaseClient client = mock(DatabaseClient.class);
    server.setDatabaseClient(client);

    JsonNode node = new ObjectMapper().readTree(" { \"shards\" : [\n" +
        "    {\n" +
        "      \"replicas\": [\n" +
        "        {\n" +
        "          \"publicAddress\": \"localhost\",\n" +
        "          \"privateAddress\": \"localhost\",\n" +
        "          \"port\": 9010,\n" +
        "          \"httpPort\": 8080\n" +
        "        }\n" +
        "      ]\n" +
        "    }\n" +
        "  ]}\n");
    ServersConfig serversConfig = new ServersConfig("test", (ArrayNode) ((ObjectNode)node).withArray("shards"), 1, true, true);
    //when(common.getServersConfig()).thenReturn(serversConfig);
    DatabaseCommon common = new DatabaseCommon();
    common.setServersConfig(serversConfig);

    when(client.getCommon()).thenReturn(common);
    TableSchema tableSchema = IndexLookupTest.createTable();
    IndexSchema indexSchema = IndexLookupTest.createIndexSchema(tableSchema);
    common.getTables("test").put(tableSchema.getName(), tableSchema);

    when(client.sendToMaster(eq("PartitionManager:getRepartitionerState"), anyObject())).thenAnswer(
        (Answer) invocationOnMock -> {
          PartitionManager mgr = server.getPartitionManager();
          return mgr.getRepartitionerState(new ComObject(), false).serialize();
        });
    DescribeStatementHandler handler = new DescribeStatementHandler(client);
    ResultSet ret = handler.doDescribe("test", "describe server health");
    StringBuilder builder = new StringBuilder();
    while (ret.next()) {
      String line = ret.getString("host") + " " +
          ret.getString("shard") + " " +
          ret.getString("replica") + " " +
          ret.getString("dead") + " " +
          ret.getString("master");
      builder.append(line).append("\n");
    }

    String expected = "localhost:9010 0 0 false true\n";

    assertEquals(builder.toString(), expected);
  }

  @Test
  public void testServerSchemaVersion() throws InterruptedException, ExecutionException, IOException {
    String configStr = IOUtils.toString(DatabaseServerTest.class.getResourceAsStream("/config/config-1-local.json"), "utf-8");
    ObjectNode config = (ObjectNode) new ObjectMapper().readTree(configStr);
    com.sonicbase.server.DatabaseServer server = new DatabaseServer() {
      @Override
      public Logger getErrorLogger() {
        Logger mockLogger = mock(Logger.class);
        doAnswer((Answer<Object>) invocationOnMock -> {
          return null;
        }).when(mockLogger).error(anyObject());
        return mockLogger;
      }
    };
    server.setConfig(config, "test", "localhost", 9010, true, new AtomicBoolean(), new AtomicBoolean(), "gc.log", true);
    DatabaseClient client = mock(DatabaseClient.class);
    server.setDatabaseClient(client);
    JsonNode node = new ObjectMapper().readTree(" { \"shards\" : [\n" +
        "    {\n" +
        "      \"replicas\": [\n" +
        "        {\n" +
        "          \"publicAddress\": \"localhost\",\n" +
        "          \"privateAddress\": \"localhost\",\n" +
        "          \"port\": 9010,\n" +
        "          \"httpPort\": 8080\n" +
        "        }\n" +
        "      ]\n" +
        "    }\n" +
        "  ]}\n");
    ServersConfig serversConfig = new ServersConfig("test", (ArrayNode) ((ObjectNode)node).withArray("shards"), 1, true, true);
    //when(common.getServersConfig()).thenReturn(serversConfig);
    DatabaseCommon common = new DatabaseCommon();
    common.setServersConfig(serversConfig);

    when(client.getCommon()).thenReturn(common);
    TableSchema tableSchema = IndexLookupTest.createTable();
    IndexSchema indexSchema = IndexLookupTest.createIndexSchema(tableSchema);
    common.getTables("test").put(tableSchema.getName(), tableSchema);

    when(client.send(eq("DatabaseServer:getSchema"), anyInt(), anyInt(), anyObject(), anyObject())).thenAnswer(
        (Answer) invocationOnMock -> {
          ComObject retObj = new ComObject();
          retObj.put(ComObject.Tag.schemaBytes, common.serializeSchema((short)100));
          return retObj.serialize();
        });
    DescribeStatementHandler handler = new DescribeStatementHandler(client);
    ResultSet ret = handler.doDescribe("test", "describe schema version");
    StringBuilder builder = new StringBuilder();
    while (ret.next()) {
      String line = ret.getString("host") + " " +
          ret.getString("shard") + " " +
          ret.getString("replica") + " " +
          ret.getString("version");
      builder.append(line).append("\n");
    }

    String expected = "localhost:9010 0 0 0\n";

    assertEquals(builder.toString(), expected);
  }

}
