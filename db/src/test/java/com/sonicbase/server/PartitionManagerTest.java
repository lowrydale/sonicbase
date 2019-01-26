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
import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.PartitionUtils;
import com.sonicbase.util.TestUtils;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.EOFException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class PartitionManagerTest {

  private DatabaseServer server;

  @BeforeClass
  public void beforeClass() {
    System.setProperty("log4j.configuration", "test-log4j.xml");

    server = mock(DatabaseServer.class);

    Map<String, DatabaseServer.SimpleStats> stats = DatabaseServer.initStats();
    when(server.getStats()).thenReturn(stats);
  }

  @Test
  public void test() throws Exception {
    AddressMap addressMap = new AddressMap(server);
    when(server.getAddressMap()).thenReturn(addressMap);
    when(server.getBatchRepartCount()).thenReturn(new AtomicInteger(0));
    Map<Integer, TableSchema> tables = new HashMap<>();
    final TableSchema tableSchema = TestUtils.createTable();
    IndexSchema indexSchema = TestUtils.createIndexSchema(tableSchema, 2);

    when(server.getIndexSchema(anyString(), anyString(), anyString())).thenReturn(indexSchema);
    when(server.getShardCount()).thenReturn(2);
    when(server.getReplicationFactor()).thenReturn(1);
    when(server.getSnapshotManager()).thenReturn(mock(SnapshotManager.class));

    DatabaseCommon common = TestUtils.createCommon(tableSchema);
    common.setIsNotDurable(true);
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
    ServersConfig serversConfig = new ServersConfig("test", (ArrayNode) ((ObjectNode) node).withArray("shards"), true, true);
    //when(common.getServersConfig()).thenReturn(serversConfig);
    common.setServersConfig(serversConfig);
    when(server.getCommon()).thenReturn(common);
    when(server.useUnsafe()).thenReturn(true);

    Indices indices = new Indices();
    indices.addIndex(server.getPort(), new HashMap<Long, Boolean>(), tableSchema, indexSchema.getName(), indexSchema.getComparators());
    Index index = indices.getIndices().get(tableSchema.getName()).get(indexSchema.getName());
    when(server.getIndex(anyString(), anyString(), anyString())).thenReturn(index);

    Map<String, Indices> map = new HashMap<>();
    map.put("test", indices);
    when(server.getIndices()).thenReturn(map);
    when(server.getIndices(anyString())).thenReturn(map.get("test"));

    byte[][] records = TestUtils.createRecords(common, tableSchema, 10);

    final List<Object[]> keys = TestUtils.createKeys(10);

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
    when(server.isNotDurable()).thenReturn(true);

    final PartitionManager partitionManager = new PartitionManager(server, common);

    ComObject cobj = new ComObject(1);
    ComArray array = cobj.putArray(ComObject.Tag.SIZES, ComObject.Type.OBJECT_TYPE, 2);
    ComObject size0Obj = new ComObject(3);
    size0Obj.put(ComObject.Tag.SHARD, 0);
    size0Obj.put(ComObject.Tag.SIZE, (long) 10);
    size0Obj.put(ComObject.Tag.RAW_SIZE, (long) 10);
    array.add(size0Obj);

    ComObject size1Obj = new ComObject(3);
    size1Obj.put(ComObject.Tag.SHARD, 0);
    size1Obj.put(ComObject.Tag.SIZE, (long) 0);
    size1Obj.put(ComObject.Tag.RAW_SIZE, (long) 0);
    array.add(size1Obj);

    byte[] bytes0 = cobj.serialize();
    when(client.send(eq("PartitionManager:getPartitionSize"), anyInt(), eq((long) 0), (ComObject) anyObject(), eq(DatabaseClient.Replica.MASTER))).thenAnswer(
        (Answer) invocation -> {
//            Object[] args = invocation.getArguments();
//            return partitionManager.getPartitionSize((ComObject)args[3], false).serialize();
          return bytes0;
        });

    cobj = new ComObject(1);
    array = cobj.putArray(ComObject.Tag.SIZES, ComObject.Type.OBJECT_TYPE, 2);
    size0Obj = new ComObject(3);
    size0Obj.put(ComObject.Tag.SHARD, 0);
    size0Obj.put(ComObject.Tag.SIZE, (long) 0);
    size0Obj.put(ComObject.Tag.RAW_SIZE, (long) 0);
    array.add(size0Obj);

    size1Obj = new ComObject(3);
    size1Obj.put(ComObject.Tag.SHARD, 0);
    size1Obj.put(ComObject.Tag.SIZE, (long) 0);
    size1Obj.put(ComObject.Tag.RAW_SIZE, (long) 0);
    array.add(size1Obj);

    byte[] bytes1 = cobj.serialize();
    when(client.send(eq("PartitionManager:getPartitionSize"), eq(1), eq((long) 0), (ComObject) anyObject(), eq(DatabaseClient.Replica.MASTER))).thenReturn(
        bytes1
    );

    cobj = new ComObject(1);
    array = cobj.putArray(ComObject.Tag.KEYS, ComObject.Type.BYTE_ARRAY_TYPE, 1);
    array.add(DatabaseCommon.serializeKey(tableSchema, "_primarykey", keys.get(4)));
    bytes1 = cobj.serialize();
    when(client.send(eq("PartitionManager:getKeyAtOffset"), eq(0), eq((long) 0), (ComObject) anyObject(), eq(DatabaseClient.Replica.MASTER))).thenAnswer(
        (Answer) invocation -> {
          Object[] args = invocation.getArguments();
          return partitionManager.getKeyAtOffset((ComObject) args[3], false).serialize();
        });

//    cobj.put(ComObject.Tag.replica, 0);
//    byte[] bytes3 = cobj.serialize();
//    when(client.send(   eq("PartitionManager:rebalanceOrderedIndex"), eq(0), eq((long)0), (ComObject) anyObject(), eq(DatabaseClient.Replica.MASTER))).thenReturn(
//        bytes3
//    );

    when(client.send(eq("PartitionManager:isShardRepartitioningComplete"), anyInt(), anyLong(), any(ComObject.class),
        eq(DatabaseClient.Replica.SPECIFIED))).thenAnswer(
        (Answer) invocation -> {
          Object[] args = invocation.getArguments();
          return partitionManager.isShardRepartitioningComplete((ComObject) args[3], false).serialize();
        });


    when(client.send(eq("PartitionManager:rebalanceOrderedIndex"), eq(0), eq((long) 0), any(ComObject.class),
        eq(DatabaseClient.Replica.MASTER))).thenAnswer(
        (Answer) invocation -> {
          Object[] args = invocation.getArguments();
          return partitionManager.doRebalanceOrderedIndex((ComObject) args[3], false);
        });

    when(client.send(eq("PartitionManager:deleteMovedRecords"), anyInt(), eq((long) 0), any(ComObject.class),
        eq(DatabaseClient.Replica.SPECIFIED))).thenAnswer(
        (Answer) invocation -> {
          Object[] args = invocation.getArguments();
          ComObject cobj12 = (ComObject) args[3];
          cobj12.put(ComObject.Tag.SEQUENCE_0, 10000L);
          cobj12.put(ComObject.Tag.SEQUENCE_1, 10000L);

          return partitionManager.deleteMovedRecords(cobj12, false);
        });


    final AtomicReference<Exception> exception = new AtomicReference<>();
    final AtomicBoolean calledMoveIndexEntries = new AtomicBoolean();
    when(client.send(eq("PartitionManager:moveIndexEntries"), eq(1), eq((long) 0), any(ComObject.class),
        eq(DatabaseClient.Replica.DEF))).thenAnswer(
        (Answer) invocation -> {
          try {
            calledMoveIndexEntries.set(true);
            Object[] args = invocation.getArguments();
            ComObject cobj1 = (ComObject) args[3];
            ComArray sentKeys = cobj1.getArray(ComObject.Tag.KEYS);
            for (int i1 = 5; i1 < keys.size(); i1++) {
              ComObject keyObj = (ComObject) sentKeys.getArray().get(i1 - 5);
              byte[] bytes = keyObj.getByteArray(ComObject.Tag.KEY_BYTES);
              Object[] key = DatabaseCommon.deserializeKey(tableSchema, bytes);
              if (!key[0].equals(keys.get(i1)[0])) {
                exception.set(new Exception());
              }
            }
          }
          catch (Exception e) {
            exception.set(e);
          }
          return null;
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
    AddressMap addressMap = new AddressMap(server);
    when(server.getAddressMap()).thenReturn(addressMap);
    when(server.getBatchRepartCount()).thenReturn(new AtomicInteger(0));
    Map<Integer, TableSchema> tables = new HashMap<>();
    final TableSchema tableSchema = TestUtils.createTable();
    IndexSchema indexSchema = TestUtils.createIndexSchema(tableSchema, 2);

    when(server.getIndexSchema(anyString(), anyString(), anyString())).thenReturn(indexSchema);
    when(server.getShardCount()).thenReturn(2);
    when(server.getReplicationFactor()).thenReturn(1);
    when(server.getShard()).thenReturn(1);
    when(server.useUnsafe()).thenReturn(true);

    DatabaseCommon common = TestUtils.createCommon(tableSchema);
    common.setIsNotDurable(false);
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
    ServersConfig serversConfig = new ServersConfig("test", (ArrayNode) ((ObjectNode) node).withArray("shards"), true, true);
    //when(common.getServersConfig()).thenReturn(serversConfig);
    common.setServersConfig(serversConfig);
    when(server.getCommon()).thenReturn(common);

    Indices indices = new Indices();
    indices.addIndex(server.getPort(), new HashMap<Long, Boolean>(), tableSchema, indexSchema.getName(), indexSchema.getComparators());
    Index index = indices.getIndices().get(tableSchema.getName()).get(indexSchema.getName());
    when(server.getIndex(anyString(), anyString(), anyString())).thenReturn(index);

    Map<String, Indices> map = new HashMap<>();
    map.put("test", indices);
    when(server.getIndices()).thenReturn(map);
    when(server.getIndices(anyString())).thenReturn(map.get("test"));

    byte[][] records = TestUtils.createRecords(common, tableSchema, 10);

    final List<Object[]> keys = TestUtils.createKeys(10);

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

    ComObject cobj = new ComObject(1);
    ComArray array = cobj.putArray(ComObject.Tag.SIZES, ComObject.Type.OBJECT_TYPE, 2);
    ComObject size0Obj = new ComObject(3);
    size0Obj.put(ComObject.Tag.SHARD, 0);
    size0Obj.put(ComObject.Tag.SIZE, (long) 10);
    size0Obj.put(ComObject.Tag.RAW_SIZE, (long) 10);
    array.add(size0Obj);

    ComObject size1Obj = new ComObject(3);
    size1Obj.put(ComObject.Tag.SHARD, 0);
    size1Obj.put(ComObject.Tag.SIZE, (long) 0);
    size1Obj.put(ComObject.Tag.RAW_SIZE, (long) 0);
    array.add(size1Obj);

    byte[] bytes0 = cobj.serialize();
    when(client.send(eq("PartitionManager:getPartitionSize"), anyInt(), eq((long) 0), (ComObject) anyObject(), eq(DatabaseClient.Replica.MASTER))).thenAnswer(
        new Answer() {
          public Object answer(InvocationOnMock invocation) {
//            Object[] args = invocation.getArguments();
//            return partitionManager.getPartitionSize((ComObject)args[3], false).serialize();
            return bytes0;
          }
        });

    cobj = new ComObject(1);
    array = cobj.putArray(ComObject.Tag.SIZES, ComObject.Type.OBJECT_TYPE, 2);
    size0Obj = new ComObject(3);
    size0Obj.put(ComObject.Tag.SHARD, 0);
    size0Obj.put(ComObject.Tag.SIZE, (long) 0);
    size0Obj.put(ComObject.Tag.RAW_SIZE, (long) 0);
    array.add(size0Obj);

    size1Obj = new ComObject(3);
    size1Obj.put(ComObject.Tag.SHARD, 0);
    size1Obj.put(ComObject.Tag.SIZE, (long) 0);
    size1Obj.put(ComObject.Tag.RAW_SIZE, (long) 0);
    array.add(size1Obj);

    byte[] bytes1 = cobj.serialize();
    when(client.send(eq("PartitionManager:getPartitionSize"), eq(1), eq((long) 0), (ComObject) anyObject(), eq(DatabaseClient.Replica.MASTER))).thenReturn(
        bytes1
    );

    cobj = new ComObject(1);
    array = cobj.putArray(ComObject.Tag.KEYS, ComObject.Type.BYTE_ARRAY_TYPE, 1);
    array.add(DatabaseCommon.serializeKey(tableSchema, "_primarykey", keys.get(4)));
    bytes1 = cobj.serialize();
    when(client.send(eq("PartitionManager:getKeyAtOffset"), eq(0), eq((long) 0), (ComObject) anyObject(), eq(DatabaseClient.Replica.MASTER))).thenAnswer(
        new Answer() {
          public Object answer(InvocationOnMock invocation) {
            Object[] args = invocation.getArguments();
            return partitionManager.getKeyAtOffset((ComObject) args[3], false).serialize();
          }
        });

//    cobj.put(ComObject.Tag.replica, 0);
//    byte[] bytes3 = cobj.serialize();
//    when(client.send(   eq("PartitionManager:rebalanceOrderedIndex"), eq(0), eq((long)0), (ComObject) anyObject(), eq(DatabaseClient.Replica.MASTER))).thenReturn(
//        bytes3
//    );

    when(client.send(eq("PartitionManager:rebalanceOrderedIndex"), eq(0), eq((long) 0), any(ComObject.class),
        eq(DatabaseClient.Replica.MASTER))).thenAnswer(
        new Answer() {
          public Object answer(InvocationOnMock invocation) {
            Object[] args = invocation.getArguments();
            return partitionManager.doRebalanceOrderedIndex((ComObject) args[3], false);
          }
        });

    when(client.send(eq("PartitionManager:deleteMovedRecords"), anyInt(), eq((long) 0), any(ComObject.class),
        eq(DatabaseClient.Replica.SPECIFIED))).thenAnswer(
        new Answer() {
          public Object answer(InvocationOnMock invocation) {
            Object[] args = invocation.getArguments();
            ComObject cobj = (ComObject) args[3];
            cobj.put(ComObject.Tag.SEQUENCE_0, 10000L);
            cobj.put(ComObject.Tag.SEQUENCE_1, 10000L);

            return partitionManager.deleteMovedRecords(cobj, false);
          }
        });


    final AtomicInteger callCount = new AtomicInteger();
    final AtomicReference<Exception> exception = new AtomicReference<>();
    final AtomicBoolean calledMoveIndexEntries = new AtomicBoolean();
    when(client.send(eq("PartitionManager:moveIndexEntries"), anyInt(), eq((long) 0), any(ComObject.class),
        eq(DatabaseClient.Replica.DEF))).thenAnswer(
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
    AddressMap addressMap = new AddressMap(server);
    when(server.getAddressMap()).thenReturn(addressMap);
    when(server.getBatchRepartCount()).thenReturn(new AtomicInteger(0));
    Map<Integer, TableSchema> tables = new HashMap<>();
    final TableSchema tableSchema = TestUtils.createTable();
    IndexSchema indexSchema = TestUtils.createIndexSchema(tableSchema, 2);

    when(server.getIndexSchema(anyString(), anyString(), anyString())).thenReturn(indexSchema);
    when(server.getShardCount()).thenReturn(2);
    when(server.getReplicationFactor()).thenReturn(1);
    when(server.getShard()).thenReturn(1);
    when(server.useUnsafe()).thenReturn(true);

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
    ServersConfig serversConfig = new ServersConfig("test", (ArrayNode) ((ObjectNode) node).withArray("shards"), true, true);
    //when(common.getServersConfig()).thenReturn(serversConfig);
    common.setServersConfig(serversConfig);
    when(server.getCommon()).thenReturn(common);
    DatabaseClient client = mock(DatabaseClient.class);
    when(client.getCommon()).thenReturn(common);
    when(client.getShardCount()).thenReturn(2);
    when(server.getClient()).thenReturn(client);
    when(server.getDatabaseClient()).thenReturn(client);
    byte[][] records = TestUtils.createRecords(common, tableSchema, 10);

    Indices indices = new Indices();
    indices.addIndex(server.getPort(), new HashMap<Long, Boolean>(), tableSchema, indexSchema.getName(), indexSchema.getComparators());
    Index index = indices.getIndices().get(tableSchema.getName()).get(indexSchema.getName());
    when(server.getIndex(anyString(), anyString(), anyString())).thenReturn(index);

    Map<String, Indices> map = new HashMap<>();
    map.put("test", indices);
    when(server.getIndices()).thenReturn(map);
    when(server.getIndices(anyString())).thenReturn(map.get("test"));

    ThreadPoolExecutor executor = new ThreadPoolExecutor(5, 5, 10_000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
    try {
      when(client.getExecutor()).thenReturn(executor);
      final List<Object[]> keys = TestUtils.createKeys(10);

      int i = 0;
      for (Object[] key : keys) {
        Object address = addressMap.toUnsafeFromRecords(new byte[][]{records[i]});
        index.put(key, address);
        i++;
      }

      final PartitionManager partitionManager = new PartitionManager(server, common);
      partitionManager.setBatchOverride(1);

      when(client.send(eq("PartitionManager:getIndexCounts"), anyInt(), anyInt(), any(ComObject.class),
          eq(DatabaseClient.Replica.MASTER))).thenAnswer(
          new Answer() {
            public Object answer(InvocationOnMock invocation) {
              Object[] args = invocation.getArguments();
              ComObject cobj = (ComObject) args[3];
              cobj.put(ComObject.Tag.SEQUENCE_0, 10000L);
              cobj.put(ComObject.Tag.SEQUENCE_1, 10000L);

              return partitionManager.getIndexCounts(cobj, false).serialize();
            }
          });


      ComObject cobj = new ComObject(1);
      cobj.put(ComObject.Tag.SIZE, (long) 10);
      byte[] bytes0 = cobj.serialize();

      PartitionUtils.GlobalIndexCounts counts = PartitionUtils.getIndexCounts("test", client);
      Map<Integer, Long> count = counts.getTables().get("table1").getIndices().get("_primarykey").getCounts();
      assertEquals((long) count.get(0), 10);
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


  @Test
  public void testSeveralShards() throws Exception {
    int shardCount = 8;
    int countPerShard = 2_000;
    int totalRecordCount = shardCount * countPerShard;

    final TableSchema tableSchema = TestUtils.createTable();
    IndexSchema indexSchema = TestUtils.createIndexSchema(tableSchema, shardCount);
    final TableSchema stringTableSchema = TestUtils.createStringTable();
    IndexSchema stringIndexSchema = TestUtils.createStringIndexSchema(stringTableSchema, shardCount);


    DatabaseCommon common = TestUtils.createCommon(tableSchema);
    common.getTables("test").put("table2", stringTableSchema);
    common.getTablesById("test").put(stringTableSchema.getTableId(), stringTableSchema);

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
    ServersConfig serversConfig = new ServersConfig("test", (ArrayNode) ((ObjectNode) node).withArray("shards"), true, true);
    //when(common.getServersConfig()).thenReturn(serversConfig);
    common.setServersConfig(serversConfig);

    DatabaseClient client = mock(DatabaseClient.class);
    when(client.getCommon()).thenReturn(common);
    when(client.getShardCount()).thenReturn(shardCount);

    final List<Object[]> keys = new ArrayList<>();

    for (long i = 2306398204483007890L; i < 2306398204483007890L + totalRecordCount * 4; i++) {
      Object[] fieldArray = new Object[1];
      fieldArray[0] = (long) i;
      keys.add(fieldArray);
    }

    final List<Object[]> stringKeys = new ArrayList<>();
    for (long i = 0; i < totalRecordCount * 4; i++) {
      String idString = String.valueOf(i);
      for (int j = 0; j < 6 - String.valueOf(i).length(); j++) {
        idString = "0" + idString;
      }

      Object[] fieldArray = new Object[1];
      fieldArray[0] = idString.getBytes("utf-8");
      stringKeys.add(fieldArray);
    }

    byte[][] records = TestUtils.createRecords(common, tableSchema, totalRecordCount * 4, keys);
    byte[][] stringRecords = TestUtils.createStringRecords(common, stringTableSchema, totalRecordCount * 4, stringKeys);

    final PartitionManager[] partitionManagers = new PartitionManager[shardCount];
    DatabaseServer[] servers = new DatabaseServer[shardCount];

    List<Index> allIndices = new ArrayList<>();
    List<Index> allStringIndices = new ArrayList<>();
    AddressMap lastAddressMap = null;
    Index lastIndex = null;
    Index lastStringIndex = null;
    for (int shard = 0; shard < shardCount; shard++) {
      DatabaseServer server = mock(DatabaseServer.class);
      servers[shard] = server;

      Map<String, DatabaseServer.SimpleStats> stats = DatabaseServer.initStats();
      when(server.getStats()).thenReturn(stats);

      UpdateManager updateManager = new UpdateManager(server);
      DeleteManager deleteManager = mock(DeleteManager.class);
      when(server.getDeleteManager()).thenReturn(deleteManager);
      when(server.getUpdateManager()).thenReturn(updateManager);
      AddressMap addressMap = new AddressMap(server);
      when(server.getAddressMap()).thenReturn(addressMap);
      when(server.getBatchRepartCount()).thenReturn(new AtomicInteger(0));

      when(server.getIndexSchema(anyString(), eq("table1"), anyString())).thenReturn(indexSchema);
      when(server.getIndexSchema(anyString(), eq("table2"), anyString())).thenReturn(stringIndexSchema);
      when(server.getShard()).thenReturn(shard);
      when(server.getShardCount()).thenReturn(shardCount);
      when(server.getReplicationFactor()).thenReturn(1);
      when(server.getSnapshotManager()).thenReturn(mock(SnapshotManager.class));

      when(server.getCommon()).thenReturn(common);
      when(server.useUnsafe()).thenReturn(true);

      Indices indices = new Indices();
      indices.addIndex(server.getPort(), new HashMap<Long, Boolean>(), tableSchema, indexSchema.getName(), indexSchema.getComparators());
      indices.addIndex(server.getPort(), new HashMap<Long, Boolean>(), stringTableSchema, stringIndexSchema.getName(), stringIndexSchema.getComparators());
      Index index = indices.getIndices().get(tableSchema.getName()).get(indexSchema.getName());
      Index stringIndex = indices.getIndices().get(stringTableSchema.getName()).get(stringIndexSchema.getName());
      when(server.getIndex(anyString(), eq("table1"), anyString())).thenAnswer(new Answer() {
        @Override
        public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
          Object[] args = invocationOnMock.getArguments();
          if (!((String) args[1]).equalsIgnoreCase("table1")) {
            throw new DatabaseException("wrong index");
          }
          return index;
        }
      });
      allIndices.add(index);
      if (shard == shardCount - 1) {
        lastIndex = index;
        lastAddressMap = addressMap;
      }
      when(server.getIndex(anyString(), eq("table2"), anyString())).thenAnswer(new Answer() {
        @Override
        public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
          Object[] args = invocationOnMock.getArguments();
          if (!((String) args[1]).equalsIgnoreCase("table2")) {
            throw new DatabaseException("wrong index");
          }
          return stringIndex;
        }
      });
      allStringIndices.add(stringIndex);
      if (shard == shardCount - 1) {
        lastStringIndex = stringIndex;
        lastAddressMap = addressMap;
      }
      doAnswer(new Answer() {
        public Object answer(InvocationOnMock invocation) {
          Object[] args = invocation.getArguments();
          ConcurrentLinkedQueue<DeleteManager.DeleteRequest> keysToDeleteExpanded = (ConcurrentLinkedQueue<DeleteManager.DeleteRequest>) args[5];
          for (DeleteManager.DeleteRequest request : keysToDeleteExpanded) {
            index.remove(request.getKey());
          }
          return null;
        }
      }).when(deleteManager).saveDeletesForRecords(anyString(), eq("table1"), anyString(), anyLong(), anyLong(), any());
      doAnswer(new Answer() {
        public Object answer(InvocationOnMock invocation) {
          Object[] args = invocation.getArguments();
          ConcurrentLinkedQueue<DeleteManager.DeleteRequest> keysToDeleteExpanded = (ConcurrentLinkedQueue<DeleteManager.DeleteRequest>) args[5];
          for (DeleteManager.DeleteRequest request : keysToDeleteExpanded) {
            stringIndex.remove(request.getKey());
          }
          return null;
        }
      }).when(deleteManager).saveDeletesForRecords(anyString(), eq("table2"), anyString(), anyLong(), anyLong(), any());


      Map<String, Indices> map = new HashMap<>();
      map.put("test", indices);
      when(server.getIndices()).thenReturn(map);
      when(server.getIndices(anyString())).thenReturn(map.get("test"));

      if (shard == 0) {
        for (int j = 0; j < totalRecordCount; j++) {
          Object[] key = keys.get(j);
          Object address = addressMap.toUnsafeFromRecords(new byte[][]{records[j]});
          index.put(key, address);
        }
        for (int j = 0; j < totalRecordCount; j++) {
          Object[] key = stringKeys.get(j);
          Object address = addressMap.toUnsafeFromRecords(new byte[][]{stringRecords[j]});
          stringIndex.put(key, address);
        }
      }

      when(server.getClient()).thenReturn(client);
      when(server.getDatabaseClient()).thenReturn(client);

      partitionManagers[shard] = new PartitionManager(server, common);
    }

    ComObject cobj = new ComObject(1);
    //cobj.put(ComObject.Tag.SIZE, (long)10);
    byte[] bytes0 = cobj.serialize();
    when(client.send(eq("PartitionManager:getPartitionSize"), anyInt(), eq((long) 0), (ComObject) anyObject(), eq(DatabaseClient.Replica.MASTER))).thenAnswer(
        new Answer() {
          public Object answer(InvocationOnMock invocation) {
            Object[] args = invocation.getArguments();
            ComObject retObj = partitionManagers[(Integer) args[1]].getPartitionSize((ComObject) args[3], false);
            return retObj.serialize();
          }
        });

    //cobj.put(ComObject.Tag.SIZE, (long)0);
//    byte[] bytes1 = cobj.serialize();
//    when(client.send(   eq("PartitionManager:getPartitionSize"), eq(1), eq((long)0), (ComObject) anyObject(), eq(DatabaseClient.Replica.MASTER))).thenReturn(
//        bytes1
//    );

    cobj = new ComObject(1);
//    ComArray array = cobj.putArray(ComObject.Tag.KEYS, ComObject.Type.BYTE_ARRAY_TYPE);
//    array.add(DatabaseCommon.serializeKey(tableSchema, "_primarykey", keys.get(4)));
//    bytes1 = cobj.serialize();
    when(client.send(eq("PartitionManager:getKeyAtOffset"), anyInt(), eq((long) 0), (ComObject) anyObject(), eq(DatabaseClient.Replica.MASTER))).thenAnswer(
        new Answer() {
          public Object answer(InvocationOnMock invocation) {
            Object[] args = invocation.getArguments();
            ComObject retObj = partitionManagers[(Integer) args[1]].getKeyAtOffset((ComObject) args[3], false);
            ComArray array = retObj.getArray(ComObject.Tag.KEYS);
            if (array.getArray().size() != 0) {
              byte[] bytes = (byte[]) array.getArray().get(0);
              try {
                Object[] key = DatabaseCommon.deserializeKey(tableSchema, bytes);
                key = key;
              }
              catch (EOFException e) {
                throw new DatabaseException(e);
              }
            }
            return retObj.serialize();
          }
        });

//    cobj.put(ComObject.Tag.replica, 0);
//    byte[] bytes3 = cobj.serialize();
//    when(client.send(   eq("PartitionManager:rebalanceOrderedIndex"), eq(0), eq((long)0), (ComObject) anyObject(), eq(DatabaseClient.Replica.MASTER))).thenReturn(
//        bytes3
//    );

    when(client.send(eq("PartitionManager:rebalanceOrderedIndex"), anyInt(), eq((long) 0), any(ComObject.class),
        eq(DatabaseClient.Replica.MASTER))).thenAnswer(
        new Answer() {
          public Object answer(InvocationOnMock invocation) {
            Object[] args = invocation.getArguments();
            return partitionManagers[(Integer) args[1]].rebalanceOrderedIndex((ComObject) args[3], false).serialize();
          }
        });

    when(client.send(eq("PartitionManager:isShardRepartitioningComplete"), anyInt(), anyLong(), any(ComObject.class),
        eq(DatabaseClient.Replica.SPECIFIED))).thenAnswer(
        new Answer() {
          public Object answer(InvocationOnMock invocation) {
            Object[] args = invocation.getArguments();
            return partitionManagers[(Integer) args[1]].isShardRepartitioningComplete((ComObject) args[3], false).serialize();
          }
        });

    when(client.send(eq("PartitionManager:deleteMovedRecords"), anyInt(), eq((long) 0), any(ComObject.class),
        eq(DatabaseClient.Replica.SPECIFIED))).thenAnswer(
        new Answer() {
          public Object answer(InvocationOnMock invocation) throws EOFException {
            Object[] args = invocation.getArguments();
            ComObject cobj = (ComObject) args[3];
            cobj.put(ComObject.Tag.SEQUENCE_0, 10000L);
            cobj.put(ComObject.Tag.SEQUENCE_1, 10000L);

            ConcurrentLinkedQueue<DeleteManager.DeleteRequest> keysToDelete = new ConcurrentLinkedQueue<>();
            final ArrayList<DeleteManager.DeleteRequest> keysToDeleteExpanded = new ArrayList<>();
            final long sequence0 = cobj.getLong(ComObject.Tag.SEQUENCE_0);
            final long sequence1 = cobj.getLong(ComObject.Tag.SEQUENCE_1);
            String dbName = cobj.getString(ComObject.Tag.DB_NAME);
            String tableName = cobj.getString(ComObject.Tag.TABLE_NAME);
            String indexName = cobj.getString(ComObject.Tag.INDEX_NAME);
            TableSchema tableSchema = common.getTables(dbName).get(tableName);
            final IndexSchema indexSchema = tableSchema.getIndices().get(indexName);
            ComArray keys = cobj.getArray(ComObject.Tag.KEYS);

            partitionManagers[(Integer) args[1]].getKeysToDelete(keysToDelete, tableSchema, indexSchema, keys);

            final Index index = servers[(Integer) args[1]].getIndex(dbName, tableName, indexName);

            List<DeleteManager.DeleteRequest> batch = new ArrayList<>();
            AtomicInteger count = new AtomicInteger();
            for (DeleteManager.DeleteRequest request : keysToDelete) {
              batch.add(request);
              for (DeleteManager.DeleteRequest request1 : batch) {
                synchronized (index.getMutex(request1.getKey())) {
                  Object obj = index.remove(request1.getKey());
                  if (obj != null) {
                    servers[(Integer) args[1]].getAddressMap().delayedFreeUnsafeIds(obj);
                  }
                }
              }
            }
            return null;
            //return partitionManagers[(Integer)args[1]].deleteMovedRecords(cobj, false);
          }
        });


    final AtomicReference<Exception> exception = new AtomicReference<>();
    final AtomicBoolean calledMoveIndexEntries = new AtomicBoolean();
    when(client.send(eq("PartitionManager:moveIndexEntries"), anyInt(), eq((long) 0), any(ComObject.class),
        eq(DatabaseClient.Replica.DEF))).thenAnswer(
        new Answer() {
          public Object answer(InvocationOnMock invocation) {
            try {
              calledMoveIndexEntries.set(true);

              Object[] args = invocation.getArguments();
              ComObject cobj = (ComObject) args[3];
              cobj.put(ComObject.Tag.SEQUENCE_0, 10000L);
              cobj.put(ComObject.Tag.SEQUENCE_1, 10000L);
              return partitionManagers[(Integer) args[1]].moveIndexEntries(cobj, false).serialize();

//              Object[] args = invocation.getArguments();
//              ComObject cobj = (ComObject) args[3];
//              ComArray sentKeys = cobj.getArray(ComObject.Tag.KEYS);
//              for (int i = 4; i < keys.size(); i++) {
//                ComObject keyObj = (ComObject) sentKeys.getArray().get(i - 4);
//                byte[] bytes = keyObj.getByteArray(ComObject.Tag.KEY_BYTES);
//                Object[] key = DatabaseCommon.deserializeKey(tableSchema, bytes);
//                if (!key[0].equals(keys.get(i)[0])) {
//                  exception.set(new Exception());
//                }
//              }
            }
            catch (Exception e) {
              exception.set(e);
            }
            return null;
          }
        });


    for (int j = 0; j < shardCount; j++) {
      Index index = servers[j].getIndices().get("test").getIndices().get(tableSchema.getName()).get(indexSchema.getName());
      //assertEquals(index.size(), countPerShard, "shard=" + i);
      System.out.println("pass=pre, shard=" + j + ", count=" + index.size());
    }

    List<String> toRebalance = new ArrayList<>();
    toRebalance.add("table1 _primarykey");
    toRebalance.add("table2 _primarykey");

    int countAdded = 0;
    int countStringAdded = 0;
    for (int i = 0; i < 6; i++) {

      if (i == 2) {
        for (int j = totalRecordCount; j < totalRecordCount * 2; j++) {
          Object[] key = keys.get(j);
          Object address = lastAddressMap.toUnsafeFromRecords(new byte[][]{records[j]});
          lastIndex.put(key, address);
          lastIndex.addAndGetCount(1);
        }
        for (int j = 0; j < shardCount; j++) {
          Index index = servers[j].getIndices().get("test").getIndices().get(tableSchema.getName()).get(indexSchema.getName());
          countAdded += index.size();
        }

        for (int j = totalRecordCount; j < totalRecordCount * 2; j++) {
          Object[] key = stringKeys.get(j);
          Object address = lastAddressMap.toUnsafeFromRecords(new byte[][]{stringRecords[j]});
          lastStringIndex.put(key, address);
          lastStringIndex.addAndGetCount(1);
        }
        for (int j = 0; j < shardCount; j++) {
          Index index = servers[j].getIndices().get("test").getIndices().get(stringTableSchema.getName()).get(stringIndexSchema.getName());
          countStringAdded += index.size();
        }
      }

      if (i == 4) {
        for (int k = 0; k < shardCount; k++) {
          for (int j = 0; j < k * 10; j++) {
            Object[] key = keys.get(totalRecordCount + k * j);
            Object address = lastAddressMap.toUnsafeFromRecords(new byte[][]{records[totalRecordCount + k * j]});
            allIndices.get(k).put(key, address);
            allIndices.get(k).addAndGetCount(1);

            Object[] stringKey = stringKeys.get(totalRecordCount + k * j);
            Object stringAddress = lastAddressMap.toUnsafeFromRecords(new byte[][]{records[totalRecordCount + k * j]});
            allStringIndices.get(k).put(stringKey, stringAddress);
            allStringIndices.get(k).addAndGetCount(1);
          }
        }
      }

      partitionManagers[0].beginRebalance("test", toRebalance);

      while (true) {
        ComObject retObj = partitionManagers[0].isRepartitioningComplete(null, false);
        if (retObj.getBoolean(ComObject.Tag.FINISHED)) {
          break;
        }
        Thread.sleep(1000);
      }

      if (exception.get() != null) {
        throw exception.get();
      }

      for (int j = 0; j < shardCount; j++) {
        Index index = servers[j].getIndices().get("test").getIndices().get(tableSchema.getName()).get(indexSchema.getName());
        //assertEquals(index.size(), countPerShard, "shard=" + i);
        System.out.println("pass=" + i + ", shard=" + j + ", count=" + index.size());
        if (i == 3) {
//          System.out.println(index.size());
          assertTrue(index.size() > countPerShard * 2 - 162 && index.size() < countPerShard * 2 + 162);
        }
        else if (i == 5) {
//          System.out.println(index.size());
          assertTrue(index.size() > countPerShard * 2 - 50 && index.size() < countPerShard * 2 + 92);
        }
        else if (i == 1) {
//          System.out.println(index.size());
          assertTrue(index.size() > countPerShard - 32 && index.size() < countPerShard + 32);
        }

      }
      for (int j = 0; j < shardCount; j++) {
        Index index = servers[j].getIndices().get("test").getIndices().get(stringTableSchema.getName()).get(stringIndexSchema.getName());
        //assertEquals(index.size(), countPerShard, "shard=" + i);
        System.out.println("pass(string)=" + i + ", shard=" + j + ", count=" + index.size());
        if (i == 3) {
//          System.out.println(index.size());
          assertTrue(index.size() > countPerShard * 2 - 162 && index.size() < countPerShard * 2 + 162);
        }
        else if (i == 5) {
//          System.out.println(index.size());
          assertTrue(index.size() > countPerShard * 2 - 50 && index.size() < countPerShard * 2 + 92);
        }
        else if (i == 1) {
//          System.out.println(index.size());
          assertTrue(index.size() > countPerShard - 32 && index.size() < countPerShard + 32);
        }
      }

      partitionManagers[0].beginRebalance("test", toRebalance);

      while (true) {
        ComObject retObj = partitionManagers[0].isRepartitioningComplete(null, false);
        if (retObj.getBoolean(ComObject.Tag.FINISHED)) {
          break;
        }
        Thread.sleep(1000);
      }

      if (exception.get() != null) {
        throw exception.get();
      }

      final AtomicInteger count = new AtomicInteger();
      final AtomicInteger stringCount = new AtomicInteger();
      final AtomicLong lastKey = new AtomicLong();
      for (int j = 0; j < shardCount; j++) {
        Index index = servers[j].getIndices().get("test").getIndices().get(tableSchema.getName()).get(indexSchema.getName());

        index.visitTailMap(new Object[]{2306398204483007890L}, new Index.Visitor() {
          @Override
          public boolean visit(Object[] key, Object value) {
            if (lastKey.get() != 0 && (long) key[0] < lastKey.get()) {
              fail();
            }
            lastKey.set((long) key[0]);
            assertEquals((long) key[0], 2306398204483007890L + count.get());
            count.incrementAndGet();
            return true;
          }
        });
      }
      if (i > 3) {
        assertEquals(count.get(), countAdded);
      }

      AtomicReference<byte[]> lastStringKey = new AtomicReference<>();
      for (int j = 0; j < shardCount; j++) {
        Index stringIndex = servers[j].getIndices().get("test").getIndices().get(stringTableSchema.getName()).get(stringIndexSchema.getName());

        String idString = String.valueOf(0);
        for (int k = 0; k < 6 - String.valueOf(0).length(); k++) {
          idString = "0" + idString;
        }


        stringIndex.visitTailMap(new Object[]{idString.getBytes("utf-8")}, new Index.Visitor() {
          @Override
          public boolean visit(Object[] key, Object value) {

            try {
              if (lastStringKey.get() != null && (new String((byte[])key[0], "utf-8").compareTo(new String(lastStringKey.get())) < 0)) {
                fail();
              }
            }
            catch (UnsupportedEncodingException e) {
              e.printStackTrace();
            }
            lastStringKey.set((byte[])key[0]);
            String idString = String.valueOf(stringCount.get());
            for (int j = 0; j < 6 - String.valueOf(stringCount.get()).length(); j++) {
              idString = "0" + idString;
            }

            try {
              assertEquals(new String((byte[])key[0], "utf-8"), idString);
            }
            catch (UnsupportedEncodingException e) {
              e.printStackTrace();
            }
            stringCount.incrementAndGet();
            return true;
          }
        });
      }
      if (i > 3) {
        assertEquals(stringCount.get(), countStringAdded);
      }

      assertTrue(calledMoveIndexEntries.get());
    }

  }
}