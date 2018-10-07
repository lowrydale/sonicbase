package com.sonicbase.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.*;
import com.sonicbase.index.Indices;
import com.sonicbase.query.impl.CreateTableStatementImpl;
import com.sonicbase.schema.DataType;
import com.sonicbase.schema.FieldSchema;
import com.sonicbase.schema.TableSchema;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

public class SchemaManagerTest {

  @Test
  public void testCreateDatabase() {
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, "test");
    cobj.put(ComObject.Tag.SCHEMA_VERSION, 1000);
    cobj.put(ComObject.Tag.MASTER_SLAVE, "master");

    com.sonicbase.server.DatabaseServer server = mock(com.sonicbase.server.DatabaseServer.class);
    when(server.getDataDir()).thenReturn("/tmp");
    DatabaseCommon common = new DatabaseCommon();
    when(server.getCommon()).thenReturn(common);
    DatabaseClient client = mock(DatabaseClient.class);
    when(client.getCommon()).thenReturn(common);
    when(server.getClient()).thenReturn(client);
    when(server.getDatabaseClient()).thenReturn(client);

    when(client.send(   anyString(), anyInt(), anyInt(), (ComObject) anyObject(), eq(DatabaseClient.Replica.DEF))).thenReturn(
        null
    );

    SchemaManager schemaManager = new SchemaManager(server);

    ComObject ret = schemaManager.createDatabase(cobj, false);

    assertTrue(common.getDatabases().containsKey("test"));

    common.getDatabases().clear();

    cobj.put(ComObject.Tag.DB_NAME, "test");
    cobj.put(ComObject.Tag.SLAVE, true);
    cobj.put(ComObject.Tag.MASTER_SLAVE, "slave");
    cobj.put(ComObject.Tag.METHOD, "SchemaManager:createDatabaseSlave");

    when(server.getShard()).thenReturn(1);
    schemaManager.createDatabaseSlave(cobj, false);

    assertTrue(common.getDatabases().containsKey("test"));
  }

  @Test
  public void testCreateTableDropTable() throws IOException {
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, "test");
    cobj.put(ComObject.Tag.SCHEMA_VERSION, 1000);
    cobj.put(ComObject.Tag.MASTER_SLAVE, "master");

    com.sonicbase.server.DatabaseServer server = mock(com.sonicbase.server.DatabaseServer.class);
    when(server.getSnapshotManager()).thenReturn(mock(SnapshotManager.class));
    when(server.getDataDir()).thenReturn("/tmp");
    DatabaseCommon common = new DatabaseCommon();
    when(server.getCommon()).thenReturn(common);
    DatabaseClient client = mock(DatabaseClient.class);
    when(client.getCommon()).thenReturn(common);
    when(server.getClient()).thenReturn(client);
    when(server.getDatabaseClient()).thenReturn(client);
    Indices indices = new Indices();
    when(server.getIndices(anyString())).thenReturn(indices);

    when(client.send(   anyString(), anyInt(), anyInt(), (ComObject) anyObject(), eq(DatabaseClient.Replica.DEF))).thenReturn(
        null
    );

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
    ServersConfig serversConfig = new ServersConfig("test", (ArrayNode) ((ObjectNode)node).withArray("shards"), true, true);
    common.setServersConfig(serversConfig);

    createTable(cobj, server, common, client);

    byte[] schemaBytes = server.getCommon().serializeSchema(DatabaseClient.SERIALIZATION_VERSION);

    cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, "test");
    cobj.put(ComObject.Tag.SCHEMA_VERSION, 1000);
    cobj.put(ComObject.Tag.MASTER_SLAVE, "master");
    cobj.put(ComObject.Tag.TABLE_NAME, "table1");

    SchemaManager schemaManager = new SchemaManager(server);
    schemaManager.dropTable(cobj, false);

    assertFalse(common.getTables("test").containsKey("table1"));

    cobj = new ComObject();
    cobj.put(ComObject.Tag.SCHEMA_BYTES, schemaBytes);
    cobj.put(ComObject.Tag.TABLE_NAME, "table1");
    cobj.put(ComObject.Tag.DB_NAME, "test");
    cobj.put(ComObject.Tag.SCHEMA_VERSION, 1000);
    cobj.put(ComObject.Tag.MASTER_SLAVE, "slave");

    when(server.getShard()).thenReturn(1);
    schemaManager.createTableSlave(cobj, false);

    assertTrue(common.getTables("test").containsKey("table1"));

  }

  private void createTable(ComObject cobj, com.sonicbase.server.DatabaseServer server, DatabaseCommon common, DatabaseClient client) throws IOException {
    SchemaManager schemaManager = new SchemaManager(server);

    schemaManager.createDatabase(cobj, false);

    assertTrue(common.getDatabases().containsKey("test"));


    CreateTableStatementImpl createTableStatement = new CreateTableStatementImpl(client);
    createTableStatement.setTableName("table1");

    List<FieldSchema> fields = new ArrayList<>();
    FieldSchema fieldSchema = new FieldSchema();
    fieldSchema.setName("field1");
    fieldSchema.setType(DataType.Type.BIGINT);

    fields.add(fieldSchema);

    fieldSchema = new FieldSchema();
    fieldSchema.setName("field2");
    fieldSchema.setType(DataType.Type.VARCHAR);

    fields.add(fieldSchema);

    List<String> primaryKey = new ArrayList<String>();
    primaryKey.add("field1");

    createTableStatement.setFields(fields);
    createTableStatement.setPrimaryKey(primaryKey);

    cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, "test");
    cobj.put(ComObject.Tag.SCHEMA_VERSION, 1000);
    cobj.put(ComObject.Tag.CREATE_TABLE_STATEMENT, createTableStatement.serialize());
    cobj.put(ComObject.Tag.MASTER_SLAVE, "master");

    schemaManager.createTable(cobj, false);

    assertTrue(common.getTables("test").containsKey("table1"));

  }

  @Test
  public void testCreateIndexDropIndex() throws IOException {
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, "test");
    cobj.put(ComObject.Tag.SCHEMA_VERSION, 1000);
    cobj.put(ComObject.Tag.MASTER_SLAVE, "master");

    com.sonicbase.server.DatabaseServer server = mock(com.sonicbase.server.DatabaseServer.class);
    when(server.getUpdateManager()).thenReturn(mock(UpdateManager.class));
    when(server.getSnapshotManager()).thenReturn(mock(SnapshotManager.class));
    when(server.getDataDir()).thenReturn("/tmp");
    DatabaseCommon common = new DatabaseCommon();
    when(server.getCommon()).thenReturn(common);
    DatabaseClient client = mock(DatabaseClient.class);
    when(client.getCommon()).thenReturn(common);
    when(server.getClient()).thenReturn(client);
    when(server.getDatabaseClient()).thenReturn(client);
    Indices indices = new Indices();
    when(server.getIndices(anyString())).thenReturn(indices);

    when(client.send(   anyString(), anyInt(), anyInt(), (ComObject) anyObject(), eq(DatabaseClient.Replica.DEF))).thenReturn(
        null
    );

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
    ServersConfig serversConfig = new ServersConfig("test", (ArrayNode) ((ObjectNode)node).withArray("shards"), true, true);
    common.setServersConfig(serversConfig);

    createTable(cobj, server, common, client);

    cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, "test");
    cobj.put(ComObject.Tag.SCHEMA_VERSION, 1000);
    cobj.put(ComObject.Tag.MASTER_SLAVE, "master");
    cobj.put(ComObject.Tag.TABLE_NAME, "table1");
    cobj.put(ComObject.Tag.INDEX_NAME, "index1");
    cobj.put(ComObject.Tag.IS_UNIQUE, true);

    cobj.put(ComObject.Tag.FIELDS_STR, "field1");

    SchemaManager schemaManager = new SchemaManager(server);
    schemaManager.createIndex(cobj, false);

    byte[] schemaBytes = server.getCommon().serializeSchema(DatabaseClient.SERIALIZATION_VERSION);

    assertTrue(common.getTables("test").get("table1").getIndices().containsKey("index1"));

    cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, "test");
    cobj.put(ComObject.Tag.SCHEMA_VERSION, client.getCommon().getSchemaVersion());
    cobj.put(ComObject.Tag.TABLE_NAME, "table1");
    cobj.put(ComObject.Tag.INDEX_NAME, "index1");
    cobj.put(ComObject.Tag.MASTER_SLAVE, "master");

    schemaManager.dropIndex(cobj, false);

    assertFalse(common.getTables("test").get("table1").getIndices().containsKey("index1"));

    server.getCommon().setSchemaVersion(10);
    byte[] schemaBytes2 = server.getCommon().serializeSchema(DatabaseClient.SERIALIZATION_VERSION);
    server.getCommon().setSchemaVersion(1);

    cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, "test");
    cobj.put(ComObject.Tag.SCHEMA_VERSION, server.getCommon().getSchemaVersion());
    cobj.put(ComObject.Tag.SCHEMA_BYTES, schemaBytes);
    cobj.put(ComObject.Tag.TABLE_NAME, "table1");

    ComArray array = cobj.putArray(ComObject.Tag.INDICES, ComObject.Type.STRING_TYPE);
    array.add("index1");
    cobj.put(ComObject.Tag.MASTER_SLAVE, "slave");

    when(server.getShard()).thenReturn(1);
    schemaManager.createIndexSlave(cobj, false);

    assertTrue(common.getTables("test").get("table1").getIndices().containsKey("index1"));


    cobj = new ComObject();
    cobj.put(ComObject.Tag.SCHEMA_BYTES, schemaBytes2);
    cobj.put(ComObject.Tag.DB_NAME, "test");
    cobj.put(ComObject.Tag.SCHEMA_VERSION, server.getCommon().getSchemaVersion());
    cobj.put(ComObject.Tag.TABLE_NAME, "table1");

    array = cobj.putArray(ComObject.Tag.INDICES, ComObject.Type.STRING_TYPE);
    array.add("index1");
    cobj.put(ComObject.Tag.MASTER_SLAVE, "slave");

    schemaManager.dropIndexSlave(cobj, false);

    assertFalse(common.getTables("test").get("table1").getIndices().containsKey("index1"));
  }

  @Test
  public void testAddDropColumn() throws IOException {
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, "test");
    cobj.put(ComObject.Tag.SCHEMA_VERSION, 1000);
    cobj.put(ComObject.Tag.MASTER_SLAVE, "master");

    com.sonicbase.server.DatabaseServer server = mock(com.sonicbase.server.DatabaseServer.class);
    when(server.getSnapshotManager()).thenReturn(mock(SnapshotManager.class));
    when(server.getDataDir()).thenReturn("/tmp");
    DatabaseCommon common = new DatabaseCommon();
    when(server.getCommon()).thenReturn(common);
    DatabaseClient client = mock(DatabaseClient.class);
    when(client.getCommon()).thenReturn(common);
    when(server.getClient()).thenReturn(client);
    when(server.getDatabaseClient()).thenReturn(client);
    Indices indices = new Indices();
    when(server.getIndices(anyString())).thenReturn(indices);

    when(client.send(   anyString(), anyInt(), anyInt(), (ComObject) anyObject(), eq(DatabaseClient.Replica.DEF))).thenReturn(
        null
    );

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
    ServersConfig serversConfig = new ServersConfig("test", (ArrayNode) ((ObjectNode)node).withArray("shards"), true, true);
    common.setServersConfig(serversConfig);

    createTable(cobj, server, common, client);

    cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, "test");
    cobj.put(ComObject.Tag.SCHEMA_VERSION, 1000);
    cobj.put(ComObject.Tag.TABLE_NAME, "table1");
    cobj.put(ComObject.Tag.COLUMN_NAME, "field2");
    cobj.put(ComObject.Tag.MASTER_SLAVE, "master");

    SchemaManager schemaManager = new SchemaManager(server);
    schemaManager.dropColumn(cobj, false);

    TableSchema tableSchema = common.getTables("test").get("table1");
    assertEquals(tableSchema.getFields().size(), 2);
    assertEquals(tableSchema.getFields().get(0).getName(), "_sonicbase_id");
    assertEquals(tableSchema.getFields().get(1).getName(), "field1");

    byte[] schemaBytes = server.getCommon().serializeSchema(DatabaseClient.SERIALIZATION_VERSION);

    cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, "test");
    cobj.put(ComObject.Tag.SCHEMA_VERSION, client.getCommon().getSchemaVersion());
    cobj.put(ComObject.Tag.TABLE_NAME, "table1");
    cobj.put(ComObject.Tag.COLUMN_NAME, "field2");
    cobj.put(ComObject.Tag.DATA_TYPE, DataType.Type.BIGINT.name());
    cobj.put(ComObject.Tag.MASTER_SLAVE, "master");

    schemaManager.addColumn(cobj, false);

    tableSchema = common.getTables("test").get("table1");
    assertEquals(tableSchema.getFields().size(), 3);
    assertEquals(tableSchema.getFields().get(0).getName(), "_sonicbase_id");
    assertEquals(tableSchema.getFields().get(1).getName(), "field1");
    assertEquals(tableSchema.getFields().get(2).getName(), "field2");

    schemaBytes = server.getCommon().serializeSchema(DatabaseClient.SERIALIZATION_VERSION);

    cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, "test");
    cobj.put(ComObject.Tag.SCHEMA_VERSION, 1000);
    cobj.put(ComObject.Tag.TABLE_NAME, "table1");
    cobj.put(ComObject.Tag.COLUMN_NAME, "field2");
    cobj.put(ComObject.Tag.MASTER_SLAVE, "master");

    schemaManager = new SchemaManager(server);
    schemaManager.dropColumn(cobj, false);

    tableSchema = common.getTables("test").get("table1");
    assertEquals(tableSchema.getFields().size(), 2);
    assertEquals(tableSchema.getFields().get(0).getName(), "_sonicbase_id");
    assertEquals(tableSchema.getFields().get(1).getName(), "field1");

    byte[] schemaBytes2 = server.getCommon().serializeSchema(DatabaseClient.SERIALIZATION_VERSION);

    cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, "test");
    cobj.put(ComObject.Tag.SCHEMA_BYTES, schemaBytes);
    cobj.put(ComObject.Tag.SCHEMA_VERSION, 1000);
    cobj.put(ComObject.Tag.TABLE_NAME, "table1");
    cobj.put(ComObject.Tag.COLUMN_NAME, "field2");
    cobj.put(ComObject.Tag.DATA_TYPE, DataType.Type.BIGINT.name());
    cobj.put(ComObject.Tag.MASTER_SLAVE, "master");

    when(server.getShard()).thenReturn(1);
    schemaManager.addColumnSlave(cobj, false);

    tableSchema = common.getTables("test").get("table1");
    assertEquals(tableSchema.getFields().size(), 3);
    assertEquals(tableSchema.getFields().get(0).getName(), "_sonicbase_id");
    assertEquals(tableSchema.getFields().get(1).getName(), "field1");
    assertEquals(tableSchema.getFields().get(2).getName(), "field2");


    cobj.put(ComObject.Tag.COLUMN_NAME, "field2");
    cobj.put(ComObject.Tag.SCHEMA_BYTES, schemaBytes2);
    cobj.put(ComObject.Tag.TABLE_NAME, "table1");
    cobj.put(ComObject.Tag.DB_NAME, "test");
    cobj.put(ComObject.Tag.SCHEMA_VERSION, 1000);

    when(server.getShard()).thenReturn(1);
    schemaManager.dropColumnSlave(cobj, false);

    tableSchema = common.getTables("test").get("table1");
    assertEquals(tableSchema.getFields().size(), 2);
    assertEquals(tableSchema.getFields().get(0).getName(), "_sonicbase_id");
    assertEquals(tableSchema.getFields().get(1).getName(), "field1");

  }

  @Test
  public void testReconcileSchema() throws IOException {
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, "test");
    cobj.put(ComObject.Tag.SCHEMA_VERSION, 1000);
    cobj.put(ComObject.Tag.MASTER_SLAVE, "master");

    com.sonicbase.server.DatabaseServer server = mock(DatabaseServer.class);
    when(server.getSnapshotManager()).thenReturn(mock(SnapshotManager.class));
    when(server.getDataDir()).thenReturn("/tmp/database");

    FileUtils.deleteDirectory(new File("/tmp/database"));
    when(server.getShardCount()).thenReturn(1);
    when(server.getReplicationFactor()).thenReturn(2);
    when(server.isDurable()).thenReturn(true);

    SnapshotManager snapshotManager = new SnapshotManager(server);
    when(server.getSnapshotManager()).thenReturn(snapshotManager);
    final SchemaManager schemaManager = new SchemaManager(server);

    DatabaseCommon common = new DatabaseCommon();
    common.setIsDurable(true);
    when(server.getCommon()).thenReturn(common);
    DatabaseClient client = mock(DatabaseClient.class);
    when(client.getCommon()).thenReturn(common);
    when(server.getClient()).thenReturn(client);
    when(server.getDatabaseClient()).thenReturn(client);
    Indices indices = new Indices();
    when(server.getIndices(anyString())).thenReturn(indices);

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
        "          \"port\": 9010,\n" +
        "          \"httpPort\": 8080\n" +
        "        }\n" +
        "      ]\n" +
        "    }\n" +
        "  ]}\n");
    ServersConfig serversConfig = new ServersConfig("test", (ArrayNode) ((ObjectNode)node).withArray("shards"), true, true);
    common.setServersConfig(serversConfig);

    createTable(cobj, server, common, client);

    cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, "test");
    cobj.put(ComObject.Tag.SCHEMA_VERSION, 1000);
    cobj.put(ComObject.Tag.MASTER_SLAVE, "master");
    cobj.put(ComObject.Tag.TABLE_NAME, "table1");
    cobj.put(ComObject.Tag.INDEX_NAME, "index1");
    cobj.put(ComObject.Tag.IS_UNIQUE, true);

    cobj.put(ComObject.Tag.FIELDS_STR, "field1");

    when(client.send(eq("PartitionManager:getIndexCounts"), anyInt(), anyLong(), any(ComObject.class),
        eq(DatabaseClient.Replica.MASTER))).thenReturn(
        new ComObject().serialize()
    );

    schemaManager.createIndex(cobj, false);

    ComObject schemaVersions = schemaManager.readSchemaVersions();
    ComArray databases = schemaVersions.getArray(ComObject.Tag.DATABASES);
    ComObject database = (ComObject) databases.getArray().get(0);
    ComArray tables = database.getArray(ComObject.Tag.TABLES);
    ComObject table = (ComObject) tables.getArray().get(0);
    ComArray indexArray = table.getArray(ComObject.Tag.INDICES);
    ComObject index = (ComObject) indexArray.getArray().get(0);
    index.put(ComObject.Tag.SCHEMA_VERSION, 1000);

    byte[] bytes = schemaVersions.serialize();
    when(client.send(eq("SchemaManager:getSchemaVersions"), eq(0), eq((long)1), any(ComObject.class),
        eq(DatabaseClient.Replica.SPECIFIED))).thenReturn(
        bytes
    );

    when(client.sendToAllShards(eq("SchemaManager:updateTableSchema"), anyLong(), any(ComObject.class),
        eq(DatabaseClient.Replica.ALL))).thenAnswer(
        new Answer() {
          public Object answer(InvocationOnMock invocation) {
            Object[] args = invocation.getArguments();
            return schemaManager.updateTableSchema((ComObject)args[2], false).serialize();
          }
        });

    when(client.sendToAllShards(eq("SchemaManager:updateIndexSchema"), anyLong(), any(ComObject.class),
        eq(DatabaseClient.Replica.ALL))).thenAnswer(
        new Answer() {
          public Object answer(InvocationOnMock invocation) {
            Object[] args = invocation.getArguments();
            return schemaManager.updateIndexSchema((ComObject)args[2], false).serialize();
          }
        });

    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytesOut);
    TableSchema.serializeIndexSchema(out, common.getTables("test").get("table1"),
        common.getTables("test").get("table1").getIndices().get("index1"));
    byte[] indexSchema = bytesOut.toByteArray();
    ComObject schemaRet = new ComObject();
    schemaRet.put(ComObject.Tag.INDEX_SCHEMA, indexSchema);

    when(client.send(eq("SchemaManager:getIndexSchema"), anyInt(), anyLong(), any(ComObject.class),
      eq(DatabaseClient.Replica.SPECIFIED))).thenReturn(schemaRet.serialize());

    common.getTables("test").get("table1").getIndices().clear();

    schemaManager.reconcileSchema();

    assertTrue(common.getTables("test").get("table1").getIndices().containsKey("index1"));

  }
}
