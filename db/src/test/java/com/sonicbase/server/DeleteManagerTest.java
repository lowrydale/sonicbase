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
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.apache.commons.io.IOUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class DeleteManagerTest {

  @BeforeClass
  public void beforeClass() {
    System.setProperty("log4j.configuration", "test-log4j.xml");
  }

  @Test
  public void testDeletes() throws IOException, InterruptedException {
    FileUtils.deleteDirectory(new File(System.getProperty("user.dir"), "db-data"));

    DatabaseServer server = mock(DatabaseServer.class);
    AddressMap addressMap = new AddressMap(server);
    when(server.getAddressMap()).thenReturn(addressMap);
    when(server.getBatchRepartCount()).thenReturn(new AtomicInteger(0));
    when(server.getSchemaVersion()).thenReturn(100);
    when(server.getDataDir()).thenReturn(new File(System.getProperty("user.dir"), "db-data").toString());
    Map<String, DatabaseServer.SimpleStats> stats = DatabaseServer.initStats();
    when(server.getStats()).thenReturn(stats);


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

    byte[][] records = TestUtils.createRecords(common, tableSchema, 1000);

    final List<Object[]> keys = TestUtils.createKeys(1000);

    int k = 0;
    for (Object[] key : keys) {
      Record.setDbViewFlags(records[k], Record.DB_VIEW_FLAG_DELETING);
      Object address = addressMap.toUnsafeFromRecords(new byte[][]{records[k]});
      index.put(key, address);
      k++;
    }

    DeleteManager deleteManager = new DeleteManager(server);
    deleteManager.start();

    ThreadPoolExecutor executor = ThreadUtil.createExecutor(20, "test");
    try {
      LongSet deleted = new LongOpenHashSet();

      executor.submit(() -> saveDeletes(0, 4, tableSchema, indexSchema, keys, deleteManager, deleted));
      executor.submit(() -> saveDeletes(10, 100, tableSchema, indexSchema, keys, deleteManager, deleted));
      executor.submit(() -> saveDeletes(120, 240, tableSchema, indexSchema, keys, deleteManager, deleted));
      executor.submit(() -> saveDeletes(245, 255, tableSchema, indexSchema, keys, deleteManager, deleted));
      executor.submit(() -> saveDeletes(260, 300, tableSchema, indexSchema, keys, deleteManager, deleted));
      executor.submit(() -> saveDeletes(310, 500, tableSchema, indexSchema, keys, deleteManager, deleted));
      executor.submit(() -> saveDeletes(501, 900, tableSchema, indexSchema, keys, deleteManager, deleted));
      executor.submit(() -> saveDeletes(910, 975, tableSchema, indexSchema, keys, deleteManager, deleted));

      ComObject cobj = new ComObject(2);
      cobj.put(ComObject.Tag.DB_NAME, "test");
      cobj.put(ComObject.Tag.SCHEMA_VERSION, 1000);
      //deleteManager.forceDeletes(cobj, false);

      Thread.sleep(10_000);
      for (int j = 0; j < keys.size(); j++) {
        if (deleted.contains(j)) {
          assertNull(index.get(keys.get(j)), "key=" + j);
        }
        else {
          assertEquals(addressMap.fromUnsafeToRecords(index.get(keys.get(j)))[0], records[j]);
        }
      }
    }
    finally {
      executor.shutdownNow();
    }
  }

  private void saveDeletes(int begin, int end, TableSchema tableSchema, IndexSchema indexSchema, List<Object[]> keys, DeleteManager deleteManager, LongSet deleted) {
    try {
      List<DeleteManager.DeleteRequest> requests = new ArrayList<>();
      for (int i = begin; i < end; i++) {
        DeleteManager.DeleteRequest request = new DeleteManager.DeleteRequest(keys.get(i));
        requests.add(request);
        deleted.add(i);
      }
      deleteManager.saveDeletesForRecords("test", tableSchema.getName(), indexSchema.getName(),
          1000, 1000, requests, 0);
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }
}
