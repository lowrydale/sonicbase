package com.sonicbase.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.common.*;
import com.sonicbase.index.AddressMap;
import com.sonicbase.index.Index;
import com.sonicbase.index.Indices;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.TestUtils;
import org.apache.commons.io.IOUtils;
import org.testng.annotations.Test;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.ArrayList;
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
    final TableSchema tableSchema = TestUtils.createTable();
    IndexSchema indexSchema = TestUtils.createIndexSchema(tableSchema);

    when(server.getIndexSchema(anyString(), anyString(), anyString())).thenReturn(indexSchema);
    when(server.getShardCount()).thenReturn(2);
    when(server.getReplicationFactor()).thenReturn(1);

    String configStr = IOUtils.toString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-4-servers.yaml")), "utf-8");
    Config config = new Config(configStr);

    when(server.getConfig()).thenReturn(config);
     TransactionManager transManager = new TransactionManager(server);
    when(server.getTransactionManager()).thenReturn(transManager);

    DatabaseCommon common = TestUtils.createCommon(tableSchema);
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

    byte[][] records = TestUtils.createRecords(common, tableSchema, 10);

    final List<Object[]> keys = TestUtils.createKeys(10);

    int k = 0;
    for (Object[] key : keys) {
      Record.setDbViewFlags(records[k], Record.DB_VIEW_FLAG_DELETING);
      Object address = addressMap.toUnsafeFromRecords(new byte[][]{records[k]});
      index.put(key, address);
      k++;
    }

    DeleteManager deleteManager = new DeleteManager(server);

    List<DeleteManager.DeleteRequest> requests = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      DeleteManager.DeleteRequest request = new DeleteManager.DeleteRequest(keys.get(i));
      requests.add(request);
    }
    deleteManager.saveDeletesForRecords("test", tableSchema.getName(), indexSchema.getName(),
         1000, 1000, requests);

    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, "test");
    cobj.put(ComObject.Tag.SCHEMA_VERSION, 1000);
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
