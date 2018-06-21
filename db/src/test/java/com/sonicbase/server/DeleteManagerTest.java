/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.common.ComObject;
import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.common.Record;
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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class DeleteManagerTest {
  @Test
  public void testDeletes() throws IOException {
    DatabaseServer server = mock(DatabaseServer.class);
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

    int k = 0;
    for (Object[] key : keys) {
      Record.setDbViewFlags(records[k], Record.DB_VIEW_FLAG_DELETING);
      Object address = addressMap.toUnsafeFromRecords(new byte[][]{records[k]});
      index.put(key, address);
      k++;
    }

    DeleteManager deleteManager = new DeleteManager(server);

    ConcurrentLinkedQueue<DeleteManager.DeleteRequest> requests = new ConcurrentLinkedQueue<>();
    for (int i = 0; i < 4; i++) {
      DeleteManager.DeleteRequest request = new DeleteManager.DeleteRequest(keys.get(i));
      requests.add(request);
    }
    deleteManager.saveDeletesForRecords("test", tableSchema.getName(), indexSchema.getName(),
         1000, 1000, requests);

    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.dbName, "test");
    cobj.put(ComObject.Tag.schemaVersion, 1000);
    deleteManager.forceDeletes(cobj, false);

    for (int j = 0; j < keys.size(); j++) {
      if (j < 4) {
        assertNull(index.get(keys.get(j)));
      }
      else {
        assertEquals(addressMap.fromUnsafeToRecords(index.get(keys.get(j)))[0], records[j]);
      }
    }

  }
}
