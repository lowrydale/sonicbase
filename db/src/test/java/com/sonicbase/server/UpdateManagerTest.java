/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.client.InsertStatementHandler;
import com.sonicbase.common.ComObject;
import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.common.KeyRecord;
import com.sonicbase.common.ServersConfig;
import com.sonicbase.index.AddressMap;
import com.sonicbase.index.Index;
import com.sonicbase.index.Indices;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import org.apache.commons.io.IOUtils;
import org.testng.annotations.Test;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.sonicbase.client.DatabaseClient.SERIALIZATION_VERSION;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

public class UpdateManagerTest {

  @Test
  public void test() throws IOException {
    com.sonicbase.server.DatabaseServer server = mock(com.sonicbase.server.DatabaseServer.class);
    AddressMap addressMap = new AddressMap(server);
    when(server.getAddressMap()).thenReturn(addressMap);
    when(server.getBatchRepartCount()).thenReturn(new AtomicInteger(0));
    Map<Integer, TableSchema> tables = new HashMap<>();
    final TableSchema tableSchema = IndexLookupTest.createTable();
    IndexSchema indexSchema = IndexLookupTest.createIndexSchema(tableSchema);

    when(server.getIndexSchema(anyString(), anyString(), anyString())).thenReturn(indexSchema);
    when(server.getShardCount()).thenReturn(2);
    when(server.getReplicationFactor()).thenReturn(1);

    String configStr = IOUtils.toString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-4-servers.json")), "utf-8");
    ObjectMapper mapper = new ObjectMapper();
    final ObjectNode config = (ObjectNode) mapper.readTree(configStr);

    when(server.getConfig()).thenReturn(config);
    OSStatsManager statsManager = new OSStatsManager(server);
    when(server.getOSStatsManager()).thenReturn(statsManager);
    TransactionManager transManager = new TransactionManager(server);
    when(server.getTransactionManager()).thenReturn(transManager);

    DatabaseCommon common = IndexLookupTest.createCommon(tableSchema);
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
    common.setServersConfig(serversConfig);
    when(server.getCommon()).thenReturn(common);

    Indices indices = new Indices();
    indices.addIndex(tableSchema, indexSchema.getName(), indexSchema.getComparators());
    Index index = indices.getIndices().get(tableSchema.getName()).get(indexSchema.getName());
    when(server.getIndex(anyString(), anyString(), anyString())).thenReturn(index);

    Map<String, Indices> map = new HashMap<>();
    map.put("test", indices);
    when(server.getIndices()).thenReturn(map);
    when(server.getIndices(anyString())).thenReturn(map.get("test"));

    byte[][] records = IndexLookupTest.createRecords(common, tableSchema, 10);

    final List<Object[]> keys = IndexLookupTest.createKeys(10);

    UpdateManager updateManager = new UpdateManager(server);

    for (int j = 0; j < keys.size(); j++) {
      int tableId = common.getTables("test").get("table1").getTableId();
      int indexId = common.getTables("test").get("table1").getIndexes().get(indexSchema.getName()).getIndexId();

      InsertStatementHandler.KeyInfo keyInfo = new InsertStatementHandler.KeyInfo();
      keyInfo.setIndexSchema(indexSchema);
      keyInfo.setKey(keys.get(j));

      KeyRecord keyRecord = new KeyRecord();
      byte[] primaryKeyBytes = DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), keys.get(j));
      keyRecord.setPrimaryKey(primaryKeyBytes);
      keyRecord.setDbViewNumber(common.getSchemaVersion());

      ComObject cobj = InsertStatementHandler.serializeInsertKey(common, "test", 0, tableId,
          indexId, "table1", keyInfo, indexSchema.getName(),
          keys.get(j), keyRecord, false);

      byte[] keyRecordBytes = keyRecord.serialize(SERIALIZATION_VERSION);
      cobj.put(ComObject.Tag.keyRecordBytes, keyRecordBytes);

      cobj.put(ComObject.Tag.dbName, "test");
      cobj.put(ComObject.Tag.schemaVersion, 1000);
      cobj.put(ComObject.Tag.isExcpliciteTrans, false);
      cobj.put(ComObject.Tag.isCommitting, false);
      cobj.put(ComObject.Tag.transactionId, 0L);
      cobj.put(ComObject.Tag.sequence0, 1000L);
      cobj.put(ComObject.Tag.sequence1, 1000L);

      updateManager.insertIndexEntryByKey(cobj, false);
    }

    for (int j = 0; j < keys.size(); j++) {
      assertNotNull(index.get(keys.get(j)));
    }
  }

  @Test
  public void testInsertWithRecord() throws IOException {
    com.sonicbase.server.DatabaseServer server = mock(DatabaseServer.class);
    AddressMap addressMap = new AddressMap(server);
    when(server.getAddressMap()).thenReturn(addressMap);
    when(server.getBatchRepartCount()).thenReturn(new AtomicInteger(0));
    Map<Integer, TableSchema> tables = new HashMap<>();
    final TableSchema tableSchema = IndexLookupTest.createTable();
    IndexSchema indexSchema = IndexLookupTest.createIndexSchema(tableSchema);

    when(server.getIndexSchema(anyString(), anyString(), anyString())).thenReturn(indexSchema);
    when(server.getShardCount()).thenReturn(2);
    when(server.getReplicationFactor()).thenReturn(1);

    String configStr = IOUtils.toString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-4-servers.json")), "utf-8");
    ObjectMapper mapper = new ObjectMapper();
    final ObjectNode config = (ObjectNode) mapper.readTree(configStr);

    when(server.getConfig()).thenReturn(config);
    OSStatsManager statsManager = new OSStatsManager(server);
    when(server.getOSStatsManager()).thenReturn(statsManager);
    TransactionManager transManager = new TransactionManager(server);
    when(server.getTransactionManager()).thenReturn(transManager);

    DatabaseCommon common = IndexLookupTest.createCommon(tableSchema);
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
    common.setServersConfig(serversConfig);
    when(server.getCommon()).thenReturn(common);

    Indices indices = new Indices();
    indices.addIndex(tableSchema, indexSchema.getName(), indexSchema.getComparators());
    Index index = indices.getIndices().get(tableSchema.getName()).get(indexSchema.getName());
    when(server.getIndex(anyString(), anyString(), anyString())).thenReturn(index);

    Map<String, Indices> map = new HashMap<>();
    map.put("test", indices);
    when(server.getIndices()).thenReturn(map);
    when(server.getIndices(anyString())).thenReturn(map.get("test"));

    byte[][] records = IndexLookupTest.createRecords(common, tableSchema, 10);

    final List<Object[]> keys = IndexLookupTest.createKeys(10);

    UpdateManager updateManager = new UpdateManager(server);

    for (int j = 0; j < keys.size(); j++) {
      int tableId = common.getTables("test").get("table1").getTableId();
      int indexId = common.getTables("test").get("table1").getIndexes().get(indexSchema.getName()).getIndexId();

      InsertStatementHandler.KeyInfo keyInfo = new InsertStatementHandler.KeyInfo();
      keyInfo.setIndexSchema(indexSchema);
      keyInfo.setKey(keys.get(j));

      byte[] primaryKeyBytes = DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), keys.get(j));

      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.recordBytes, records[j]);
      cobj.put(ComObject.Tag.keyBytes, primaryKeyBytes);

      cobj.put(ComObject.Tag.tableId, tableId);
      cobj.put(ComObject.Tag.indexId, indexId);
      cobj.put(ComObject.Tag.originalOffset, j);
      cobj.put(ComObject.Tag.originalIgnore, false);
      cobj.put(ComObject.Tag.dbName, "test");
      cobj.put(ComObject.Tag.schemaVersion, 1000);
      cobj.put(ComObject.Tag.isExcpliciteTrans, false);
      cobj.put(ComObject.Tag.isCommitting, false);
      cobj.put(ComObject.Tag.transactionId, 0L);
      cobj.put(ComObject.Tag.sequence0, 1000L);
      cobj.put(ComObject.Tag.sequence1, 1000L);

      updateManager.insertIndexEntryByKeyWithRecord(cobj, false);
    }

    for (int j = 0; j < keys.size(); j++) {
      assertEquals(addressMap.fromUnsafeToRecords(index.get(keys.get(j)))[0], records[j]);
    }
    //test delete
    for (int j = 0; j < 4; j++) {
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.serializationVersion, DatabaseClient.SERIALIZATION_VERSION);
      cobj.put(ComObject.Tag.keyBytes, DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), keys.get(j)));
      cobj.put(ComObject.Tag.schemaVersion, 1000);
      cobj.put(ComObject.Tag.dbName, "test");
      cobj.put(ComObject.Tag.tableName, tableSchema.getName());
      cobj.put(ComObject.Tag.indexName, indexSchema.getName());
      cobj.put(ComObject.Tag.isExcpliciteTrans, false);
      cobj.put(ComObject.Tag.isCommitting, false);
      cobj.put(ComObject.Tag.transactionId, 0L);
      cobj.put(ComObject.Tag.sequence0, 1000L);
      cobj.put(ComObject.Tag.sequence1, 1000L);
      updateManager.deleteRecord(cobj, false);

      index.remove(keys.get(j));
    }
    for (int j = 0; j < keys.size(); j++) {
      if (j < 4) {
        assertNull(index.get(keys.get(j)));
      }
      else {
        assertEquals(addressMap.fromUnsafeToRecords(index.get(keys.get(j)))[0], records[j]);
      }
    }

    //test upsert
    for (int j = 0; j < keys.size(); j++) {
      int tableId = common.getTables("test").get("table1").getTableId();
      int indexId = common.getTables("test").get("table1").getIndexes().get(indexSchema.getName()).getIndexId();

      InsertStatementHandler.KeyInfo keyInfo = new InsertStatementHandler.KeyInfo();
      keyInfo.setIndexSchema(indexSchema);
      keyInfo.setKey(keys.get(j));

      byte[] primaryKeyBytes = DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), keys.get(j));

      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.recordBytes, records[j]);
      cobj.put(ComObject.Tag.keyBytes, primaryKeyBytes);

      cobj.put(ComObject.Tag.tableId, tableId);
      cobj.put(ComObject.Tag.indexId, indexId);
      cobj.put(ComObject.Tag.originalOffset, j);
      cobj.put(ComObject.Tag.originalIgnore, true);
      cobj.put(ComObject.Tag.dbName, "test");
      cobj.put(ComObject.Tag.schemaVersion, 1000);
      cobj.put(ComObject.Tag.isExcpliciteTrans, false);
      cobj.put(ComObject.Tag.isCommitting, false);
      cobj.put(ComObject.Tag.transactionId, 0L);
      cobj.put(ComObject.Tag.sequence0, 1000L);
      cobj.put(ComObject.Tag.sequence1, 1000L);

      updateManager.insertIndexEntryByKeyWithRecord(cobj, false);
    }

    for (int j = 0; j < keys.size(); j++) {
      assertEquals(addressMap.fromUnsafeToRecords(index.get(keys.get(j)))[0], records[j]);
    }

    for (int j = 0; j < 4; j++) {
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.dbName, "test");
      cobj.put(ComObject.Tag.schemaVersion, 1000);

      cobj.put(ComObject.Tag.tableName, tableSchema.getName());
      cobj.put(ComObject.Tag.indexName, indexSchema.getName());
      cobj.put(ComObject.Tag.primaryKeyIndexName, indexSchema.getName());
      cobj.put(ComObject.Tag.isExcpliciteTrans, false);
      cobj.put(ComObject.Tag.isCommitting, false);
      cobj.put(ComObject.Tag.transactionId, 0L);

      byte[] keyBytes = DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), keys.get(j));
      cobj.put(ComObject.Tag.keyBytes, keyBytes);
      cobj.put(ComObject.Tag.primaryKeyBytes, keyBytes);
      cobj.put(ComObject.Tag.sequence0, 1000L);
      cobj.put(ComObject.Tag.sequence1, 1000L);

      updateManager.deleteIndexEntryByKey(cobj, false);
    }

    for (int j = 0; j < keys.size(); j++) {
      if (j < 4) {
        assertNull(index.get(keys.get(j)));
      }
      else {
        assertEquals(addressMap.fromUnsafeToRecords(index.get(keys.get(j)))[0], records[j]);
      }
    }

    //test truncate
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.dbName, "test");
    cobj.put(ComObject.Tag.schemaVersion, 1000);
    cobj.put(ComObject.Tag.tableName, tableSchema.getName());
    cobj.put(ComObject.Tag.phase, "secondary");

    updateManager.truncateTable(cobj, false);

    cobj.put(ComObject.Tag.phase, "primary");

    updateManager.truncateTable(cobj, false);

    for (int j = 0; j < keys.size(); j++) {
      assertNull(index.get(keys.get(j)));
    }
  }
}
