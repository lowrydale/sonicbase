/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.common.FileUtils;
import com.sonicbase.common.ServersConfig;
import com.sonicbase.index.AddressMap;
import com.sonicbase.index.Index;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.TestUtils;
import org.testng.annotations.Test;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class SnapshotManagerTest {

  @Test
  public void test() throws Exception {
    com.sonicbase.server.DatabaseServer server = mock(DatabaseServer.class);
    AddressMap addressMap = new AddressMap(server);
    when(server.getAddressMap()).thenReturn(addressMap);
    when(server.getBatchRepartCount()).thenReturn(new AtomicInteger(0));
    Map<Integer, TableSchema> tables = new HashMap<>();
    TableSchema tableSchema = TestUtils.createTable();
    IndexSchema indexSchema = TestUtils.createIndexSchema(tableSchema);

    when(server.getDataDir()).thenReturn("/tmp/database");
    FileUtils.deleteDirectory(new File("/tmp/database"));

    when(server.getIndexSchema(anyString(), anyString(), anyString())).thenReturn(indexSchema);

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
    ServersConfig serversConfig = new ServersConfig("test", (ArrayNode) ((ObjectNode)node).withArray("shards"), 1, true, true);
    common.setServersConfig(serversConfig);
    when(server.getCommon()).thenReturn(common);
    when(server.getConfig()).thenReturn((ObjectNode)node);
    when(server.getSchemaManager()).thenReturn(mock(SchemaManager.class));

    Index index = new Index(tableSchema, indexSchema.getName(), indexSchema.getComparators());
    when(server.getIndex(anyString(), anyString(), anyString())).thenReturn(index);


    byte[][] records = TestUtils.createRecords(common, tableSchema, 10);

    List<Object[]> keys = TestUtils.createKeys(10);

    int k = 0;
    for (Object[] key : keys) {
      Object address = addressMap.toUnsafeFromRecords(new byte[][]{records[k]});
      index.put(key, address);
      k++;
    }

    DatabaseClient client = mock(DatabaseClient.class);
    when(client.getCommon()).thenReturn(common);
    when(server.getClient()).thenReturn(client);

    SnapshotManager snapshotManager = new SnapshotManager(server);

    snapshotManager.runSnapshot("test");

    index.clear();

    snapshotManager.recoverFromSnapshot("test");

    for (int j = 0; j < keys.size(); j++) {
      assertTrue(index.get(keys.get(j)) != null);
    }
    assertEquals(index.size(), records.length);

  }
}
