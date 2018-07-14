/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.ComArray;
import com.sonicbase.common.ComObject;
import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.common.ServersConfig;
import com.sonicbase.index.AddressMap;
import com.sonicbase.index.Index;
import com.sonicbase.index.Indices;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class PartitionManagerTest {

  @Test
  public void test() throws Exception {
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
    when(server.getSnapshotManager()).thenReturn(mock(SnapshotManager.class));

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

    int i = 0;
    for (Object[] key : keys) {
      Object address = addressMap.toUnsafeFromRecords(new byte[][]{records[i]});
      index.put(key, address);
      i++;
    }

    DatabaseClient client = mock(DatabaseClient.class);
    when(client.getCommon()).thenReturn(common);
    when(client.getShardCount()).thenReturn(2);
    when(server.getClient()).thenReturn(client);
    when(server.getDatabaseClient()).thenReturn(client);

    final PartitionManager partitionManager = new PartitionManager(server, common);

    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.size, (long)10);
    byte[] bytes0 = cobj.serialize();
    when(client.send(   eq("PartitionManager:getPartitionSize"), eq(0), eq((long)0), (ComObject) anyObject(), eq(DatabaseClient.Replica.master))).thenAnswer(
        new Answer() {
          public Object answer(InvocationOnMock invocation) {
            Object[] args = invocation.getArguments();
            return partitionManager.getPartitionSize((ComObject)args[3], false).serialize();
          }
        });

    cobj.put(ComObject.Tag.size, (long)0);
    byte[] bytes1 = cobj.serialize();
    when(client.send(   eq("PartitionManager:getPartitionSize"), eq(1), eq((long)0), (ComObject) anyObject(), eq(DatabaseClient.Replica.master))).thenReturn(
        bytes1
    );

    cobj = new ComObject();
    ComArray array = cobj.putArray(ComObject.Tag.keys, ComObject.Type.byteArrayType);
    array.add(DatabaseCommon.serializeKey(tableSchema, "_primarykey", keys.get(4)));
    bytes1 = cobj.serialize();
    when(client.send(   eq("PartitionManager:getKeyAtOffset"), eq(0), eq((long)0), (ComObject) anyObject(), eq(DatabaseClient.Replica.master))).thenAnswer(
        new Answer() {
          public Object answer(InvocationOnMock invocation) {
            Object[] args = invocation.getArguments();
            return partitionManager.getKeyAtOffset((ComObject)args[3], false).serialize();
          }
        });

//    cobj.put(ComObject.Tag.replica, 0);
//    byte[] bytes3 = cobj.serialize();
//    when(client.send(   eq("PartitionManager:rebalanceOrderedIndex"), eq(0), eq((long)0), (ComObject) anyObject(), eq(DatabaseClient.Replica.master))).thenReturn(
//        bytes3
//    );

    when(client.send(eq("PartitionManager:rebalanceOrderedIndex"), eq(0), eq((long)0), any(ComObject.class),
        eq(DatabaseClient.Replica.master))).thenAnswer(
        new Answer() {
          public Object answer(InvocationOnMock invocation) {
            Object[] args = invocation.getArguments();
            return partitionManager.doRebalanceOrderedIndex((ComObject)args[3], false);
          }
        });

    when(client.send(eq("PartitionManager:deleteMovedRecords"), anyInt(), eq((long)0), any(ComObject.class),
        eq(DatabaseClient.Replica.specified))).thenAnswer(
        new Answer() {
          public Object answer(InvocationOnMock invocation) {
            Object[] args = invocation.getArguments();
            ComObject cobj = (ComObject)args[3];
            cobj.put(ComObject.Tag.sequence0, 10000L);
            cobj.put(ComObject.Tag.sequence1, 10000L);

            return partitionManager.deleteMovedRecords(cobj, false);
          }
        });


    final AtomicReference<Exception> exception = new AtomicReference<>();
    final AtomicBoolean calledMoveIndexEntries = new AtomicBoolean();
    when(client.send(eq("PartitionManager:moveIndexEntries"), eq(1), eq((long)0), any(ComObject.class),
        eq(DatabaseClient.Replica.def))).thenAnswer(
        new Answer() {
          public Object answer(InvocationOnMock invocation) {
            try {
              calledMoveIndexEntries.set(true);
              Object[] args = invocation.getArguments();
              ComObject cobj = (ComObject) args[3];
              ComArray sentKeys = cobj.getArray(ComObject.Tag.keys);
              for (int i = 4; i < keys.size(); i++) {
                ComObject keyObj = (ComObject) sentKeys.getArray().get(i - 4);
                byte[] bytes = keyObj.getByteArray(ComObject.Tag.keyBytes);
                Object[] key = DatabaseCommon.deserializeKey(tableSchema, bytes);
                if (!key[0].equals(keys.get(i)[0])) {
                  exception.set(new Exception());
                }
              }
            }
            catch (Exception e) {
              exception.set(e);
            }
            return null;
          }
        });

    List<String> toRebalance = new ArrayList<>();
    toRebalance.add("table1 _primarykey");
    partitionManager.beginRebalance("test", toRebalance);

    if (exception.get() != null) {
      throw exception.get();
    }
    assertTrue(calledMoveIndexEntries.get());
  }


  @Test
  public void testShard2() throws Exception {
    DatabaseServer server = mock(DatabaseServer.class);
    AddressMap addressMap = new AddressMap(server);
    when(server.getAddressMap()).thenReturn(addressMap);
    when(server.getBatchRepartCount()).thenReturn(new AtomicInteger(0));
    Map<Integer, TableSchema> tables = new HashMap<>();
    final TableSchema tableSchema = IndexLookupTest.createTable();
    IndexSchema indexSchema = IndexLookupTest.createIndexSchema(tableSchema, 2);

    when(server.getIndexSchema(anyString(), anyString(), anyString())).thenReturn(indexSchema);
    when(server.getShardCount()).thenReturn(2);
    when(server.getReplicationFactor()).thenReturn(1);
    when(server.getShard()).thenReturn(1);

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

    int i = 0;
    for (Object[] key : keys) {
      Object address = addressMap.toUnsafeFromRecords(new byte[][]{records[i]});
      index.put(key, address);
      i++;
    }

    DatabaseClient client = mock(DatabaseClient.class);
    when(client.getCommon()).thenReturn(common);
    when(client.getShardCount()).thenReturn(2);
    when(server.getClient()).thenReturn(client);
    when(server.getDatabaseClient()).thenReturn(client);
    when(server.getSnapshotManager()).thenReturn(mock(SnapshotManager.class));

    final PartitionManager partitionManager = new PartitionManager(server, common);
    partitionManager.setBatchOverride(1);

    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.size, (long)10);
    byte[] bytes0 = cobj.serialize();
    when(client.send(   eq("PartitionManager:getPartitionSize"), eq(0), eq((long)0), (ComObject) anyObject(), eq(DatabaseClient.Replica.master))).thenAnswer(
        new Answer() {
          public Object answer(InvocationOnMock invocation) {
            Object[] args = invocation.getArguments();
            return partitionManager.getPartitionSize((ComObject)args[3], false).serialize();
          }
        });

    cobj.put(ComObject.Tag.size, (long)0);
    byte[] bytes1 = cobj.serialize();
    when(client.send(   eq("PartitionManager:getPartitionSize"), eq(1), eq((long)0), (ComObject) anyObject(), eq(DatabaseClient.Replica.master))).thenReturn(
        bytes1
    );

    cobj = new ComObject();
    ComArray array = cobj.putArray(ComObject.Tag.keys, ComObject.Type.byteArrayType);
    array.add(DatabaseCommon.serializeKey(tableSchema, "_primarykey", keys.get(4)));
    bytes1 = cobj.serialize();
    when(client.send(   eq("PartitionManager:getKeyAtOffset"), eq(0), eq((long)0), (ComObject) anyObject(), eq(DatabaseClient.Replica.master))).thenAnswer(
        new Answer() {
          public Object answer(InvocationOnMock invocation) {
            Object[] args = invocation.getArguments();
            return partitionManager.getKeyAtOffset((ComObject)args[3], false).serialize();
          }
        });

//    cobj.put(ComObject.Tag.replica, 0);
//    byte[] bytes3 = cobj.serialize();
//    when(client.send(   eq("PartitionManager:rebalanceOrderedIndex"), eq(0), eq((long)0), (ComObject) anyObject(), eq(DatabaseClient.Replica.master))).thenReturn(
//        bytes3
//    );

    when(client.send(eq("PartitionManager:rebalanceOrderedIndex"), eq(0), eq((long)0), any(ComObject.class),
        eq(DatabaseClient.Replica.master))).thenAnswer(
        new Answer() {
          public Object answer(InvocationOnMock invocation) {
            Object[] args = invocation.getArguments();
            return partitionManager.doRebalanceOrderedIndex((ComObject)args[3], false);
          }
        });

    when(client.send(eq("PartitionManager:deleteMovedRecords"), anyInt(), eq((long)0), any(ComObject.class),
        eq(DatabaseClient.Replica.specified))).thenAnswer(
        new Answer() {
          public Object answer(InvocationOnMock invocation) {
            Object[] args = invocation.getArguments();
            ComObject cobj = (ComObject)args[3];
            cobj.put(ComObject.Tag.sequence0, 10000L);
            cobj.put(ComObject.Tag.sequence1, 10000L);

            return partitionManager.deleteMovedRecords(cobj, false);
          }
        });


    final AtomicInteger callCount = new AtomicInteger();
    final AtomicReference<Exception> exception = new AtomicReference<>();
    final AtomicBoolean calledMoveIndexEntries = new AtomicBoolean();
    when(client.send(eq("PartitionManager:moveIndexEntries"), anyInt(), eq((long)0), any(ComObject.class),
        eq(DatabaseClient.Replica.def))).thenAnswer(
        new Answer() {
          public Object answer(InvocationOnMock invocation) {
            try {
              if (callCount.getAndIncrement() > 0) {
                return null;
              }
              calledMoveIndexEntries.set(true);
            }
            catch (Exception e) {
              exception.set(e);
            }
            return null;
          }
        });

    List<String> toRebalance = new ArrayList<>();
    toRebalance.add("table1 _primarykey");
    partitionManager.beginRebalance("test", toRebalance);

    if (exception.get() != null) {
      throw exception.get();
    }
    assertTrue(calledMoveIndexEntries.get());
  }

  @Test
  public void testGetIndexCounts() throws Exception {
    DatabaseServer server = mock(DatabaseServer.class);
    AddressMap addressMap = new AddressMap(server);
    when(server.getAddressMap()).thenReturn(addressMap);
    when(server.getBatchRepartCount()).thenReturn(new AtomicInteger(0));
    Map<Integer, TableSchema> tables = new HashMap<>();
    final TableSchema tableSchema = IndexLookupTest.createTable();
    IndexSchema indexSchema = IndexLookupTest.createIndexSchema(tableSchema, 2);

    when(server.getIndexSchema(anyString(), anyString(), anyString())).thenReturn(indexSchema);
    when(server.getShardCount()).thenReturn(2);
    when(server.getReplicationFactor()).thenReturn(1);
    when(server.getShard()).thenReturn(1);

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
    DatabaseClient client = mock(DatabaseClient.class);
    when(client.getCommon()).thenReturn(common);
    when(client.getShardCount()).thenReturn(2);
    when(server.getClient()).thenReturn(client);
    when(server.getDatabaseClient()).thenReturn(client);
    byte[][] records = IndexLookupTest.createRecords(common, tableSchema, 10);

    Indices indices = new Indices();
    indices.addIndex(tableSchema, indexSchema.getName(), indexSchema.getComparators());
    Index index = indices.getIndices().get(tableSchema.getName()).get(indexSchema.getName());
    when(server.getIndex(anyString(), anyString(), anyString())).thenReturn(index);

    Map<String, Indices> map = new HashMap<>();
    map.put("test", indices);
    when(server.getIndices()).thenReturn(map);
    when(server.getIndices(anyString())).thenReturn(map.get("test"));

    ThreadPoolExecutor executor = new ThreadPoolExecutor(5, 5, 10_000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
    try {
      when(client.getExecutor()).thenReturn(executor);
      final List<Object[]> keys = IndexLookupTest.createKeys(10);

      int i = 0;
      for (Object[] key : keys) {
        Object address = addressMap.toUnsafeFromRecords(new byte[][]{records[i]});
        index.put(key, address);
        i++;
      }

      final PartitionManager partitionManager = new PartitionManager(server, common);
      partitionManager.setBatchOverride(1);

      when(client.send(eq("PartitionManager:getIndexCounts"), anyInt(), anyInt(), any(ComObject.class),
          eq(DatabaseClient.Replica.master))).thenAnswer(
          new Answer() {
            public Object answer(InvocationOnMock invocation) {
              Object[] args = invocation.getArguments();
              ComObject cobj = (ComObject) args[3];
              cobj.put(ComObject.Tag.sequence0, 10000L);
              cobj.put(ComObject.Tag.sequence1, 10000L);

              return partitionManager.getIndexCounts(cobj, false).serialize();
            }
          });


      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.size, (long) 10);
      byte[] bytes0 = cobj.serialize();

      PartitionManager.GlobalIndexCounts counts = PartitionManager.getIndexCounts("test", client);
      ConcurrentHashMap<Integer, Long> count = counts.getTables().get("table1").getIndices().get("_primarykey").getCounts();
      assertEquals((long)count.get(0), 10);
      System.out.println("test");
      //    ComObject ret = partitionManager.getIndexCounts(cobj, false);
      //
      //    ComArray array = ret.getArray(ComObject.Tag.tables);
      //    ComObject tableObj = (ComObject) array.getArray().get(0);
      //    ComArray indexArray = tableObj.getArray(ComObject.Tag.indices);
      //    ComObject indexObj = (ComObject) indexArray.getArray().get(0);
      //    assertEquals(indexObj.getString(ComObject.Tag.indexName), "_primarykey");
      //    int count = (int)(long)indexObj.getLong(ComObject.Tag.size);
      //    assertEquals(count, 10);
    }
    finally {
      executor.shutdownNow();
    }
  }
}
