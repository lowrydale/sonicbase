package com.sonicbase.server;

import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.client.InsertStatementHandler;
import com.sonicbase.client.SelectStatementHandler;
import com.sonicbase.common.*;
import com.sonicbase.index.AddressMap;
import com.sonicbase.index.Index;
import com.sonicbase.index.Indices;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.query.impl.AllRecordsExpressionImpl;
import com.sonicbase.query.impl.ExpressionImpl;
import com.sonicbase.query.impl.InsertStatementImpl;
import com.sonicbase.query.impl.SelectStatementImpl;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.streams.StreamManager;
import com.sonicbase.util.TestUtils;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import org.apache.commons.io.IOUtils;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.*;

import java.io.*;
import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.sonicbase.client.DatabaseClient.SERIALIZATION_VERSION;
import static com.sonicbase.client.InsertStatementHandler.BATCH_STATUS_FAILED;
import static com.sonicbase.client.InsertStatementHandler.BATCH_STATUS_SUCCCESS;
import static com.sonicbase.server.DatabaseServer.*;
import static com.sonicbase.server.MonitorManagerImpl.METRICS;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

public class UpdateManagerTest {
  private com.sonicbase.server.DatabaseServer server;
  private AddressMap addressMap;
  private TableSchema tableSchema;
  private IndexSchema indexSchema;
  private TransactionManager transManager;
  private DatabaseCommon common;
  private Index index;
  private byte[][] records;
  private UpdateManager updateManager;
  private List<Object[]> keys;
  private DatabaseClient client;
  private ThreadPoolExecutor executor;
  private IndexSchema stringIndexSchema;
  private Index stringIndex;
  private AtomicLong transId;

  @BeforeClass
  public void beforeClass() {
    executor = new ThreadPoolExecutor(5, 5, 10_000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
  }

  @AfterClass
  public void afterClass() {
    executor.shutdownNow();
  }

  @BeforeMethod
  public void beforeMethod() throws IOException {
    server = mock(com.sonicbase.server.DatabaseServer.class);
    addressMap = new AddressMap(server);
    when(server.getAddressMap()).thenReturn(addressMap);
    when(server.getBatchRepartCount()).thenReturn(new AtomicInteger(0));
    Map<Integer, TableSchema> tables = new HashMap<>();
    tableSchema = TestUtils.createTable();
    indexSchema = TestUtils.createIndexSchema(tableSchema);
    stringIndexSchema = TestUtils.createStringIndexSchema(tableSchema);

    when(server.getIndexSchema(anyString(), anyString(), anyString())).thenReturn(indexSchema);
    when(server.getShardCount()).thenReturn(2);
    when(server.getReplicationFactor()).thenReturn(1);

    String configStr = IOUtils.toString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-4-servers.yaml")), "utf-8");
    Config config = new Config(configStr);

    when(server.getConfig()).thenReturn(config);

    final Map<String, Timer> timers = new HashMap<>();

    timers.put(METRIC_SNAPSHOT_WRITE, METRICS.timer("snapshotWrite"));
    timers.put(METRIC_SNAPSHOT_RECOVER, METRICS.timer("snapshotRecover"));
    timers.put(METRIC_REPART_MOVE_ENTRY, METRICS.timer("repartMoveEntry"));
    timers.put(METRIC_REPART_PROCESS_ENTRY, METRICS.timer("repartProcessEntry"));
    timers.put(METRIC_REPART_DELETE_ENTRY, METRICS.timer("repartDeleteEntry"));
    timers.put(METRIC_READ, METRICS.timer("read"));
    timers.put(METRIC_INSERT, METRICS.timer("insert"));
    timers.put(METRIC_UPDATE, METRICS.timer("update"));
    timers.put(METRIC_DELETE, METRICS.timer("delete"));

    when(server.getTimers()).thenReturn(timers);

    when(server.getStreamManager()).thenReturn(mock(StreamManager.class));

    transManager = new TransactionManager(server);
    when(server.getTransactionManager()).thenReturn(transManager);

    common = TestUtils.createCommon(tableSchema);
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
    indices.addIndex(tableSchema, stringIndexSchema.getName(), stringIndexSchema.getComparators());
    index = indices.getIndices().get(tableSchema.getName()).get(indexSchema.getName());
    stringIndex = indices.getIndices().get(tableSchema.getName()).get(stringIndexSchema.getName());
    when(server.getIndex(anyString(), anyString(), eq(indexSchema.getName()))).thenReturn(index);
    when(server.getIndex(anyString(), anyString(), eq(stringIndexSchema.getName()))).thenReturn(stringIndex);

    Map<String, Indices> map = new HashMap<>();
    map.put("test", indices);
    when(server.getIndices()).thenReturn(map);
    when(server.getIndices(anyString())).thenReturn(map.get("test"));

    records = TestUtils.createRecords(common, tableSchema, 10);

    keys = TestUtils.createKeys(10);

    updateManager = new UpdateManager(server);
    updateManager.initStreamManager();

    client = mock(DatabaseClient.class);
    when(client.getCommon()).thenReturn(common);
    when(client.getShardCount()).thenReturn(1);
    when(client.getExecutor()).thenReturn(executor);
    transId = new AtomicLong();
    when(client.allocateId(anyString())).thenAnswer(
        (Answer) invocation -> transId.incrementAndGet());

    when(server.getClient()).thenReturn(client);
  }

  @Test
  public void test() {

    for (int j = 0; j < keys.size(); j++) {
      int tableId = common.getTables("test").get("table1").getTableId();
      int indexId = common.getTables("test").get("table1").getIndices().get(indexSchema.getName()).getIndexId();

      InsertStatementHandler.KeyInfo keyInfo = new InsertStatementHandler.KeyInfo();
      keyInfo.setIndexSchema(indexSchema);
      keyInfo.setKey(keys.get(j));

      KeyRecord keyRecord = new KeyRecord();
      byte[] primaryKeyBytes = DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), keys.get(j));
      keyRecord.setPrimaryKey(primaryKeyBytes);
      keyRecord.setDbViewNumber(common.getSchemaVersion());

      ComObject cobj = InsertStatementHandler.serializeInsertKey(client, common, 0, "test", 0, tableId,
          indexId, "table1", keyInfo, indexSchema.getName(),
          keys.get(j), null, keyRecord, false);

      byte[] keyRecordBytes = keyRecord.serialize(SERIALIZATION_VERSION);
      cobj.put(ComObject.Tag.KEY_RECORD_BYTES, keyRecordBytes);

      cobj.put(ComObject.Tag.DB_NAME, "test");
      cobj.put(ComObject.Tag.SCHEMA_VERSION, 1000);
      cobj.put(ComObject.Tag.IS_EXCPLICITE_TRANS, false);
      cobj.put(ComObject.Tag.IS_COMMITTING, false);
      cobj.put(ComObject.Tag.TRANSACTION_ID, 0L);
      cobj.put(ComObject.Tag.SEQUENCE_0, 1000L);
      cobj.put(ComObject.Tag.SEQUENCE_1, 1000L);

      updateManager.insertIndexEntryByKey(cobj, false);
    }

    for (int j = 0; j < keys.size(); j++) {
      assertNotNull(index.get(keys.get(j)));
    }
  }

  @Test
  public void testInsertWithRecord() {

    for (int j = 0; j < keys.size(); j++) {
      int tableId = common.getTables("test").get("table1").getTableId();
      int indexId = common.getTables("test").get("table1").getIndices().get(indexSchema.getName()).getIndexId();

      InsertStatementHandler.KeyInfo keyInfo = new InsertStatementHandler.KeyInfo();
      keyInfo.setIndexSchema(indexSchema);
      keyInfo.setKey(keys.get(j));

      byte[] primaryKeyBytes = DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), keys.get(j));

      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.RECORD_BYTES, records[j]);
      cobj.put(ComObject.Tag.KEY_BYTES, primaryKeyBytes);

      cobj.put(ComObject.Tag.TABLE_ID, tableId);
      cobj.put(ComObject.Tag.INDEX_ID, indexId);
      cobj.put(ComObject.Tag.ORIGINAL_OFFSET, j);
      cobj.put(ComObject.Tag.ORIGINAL_IGNORE, false);
      cobj.put(ComObject.Tag.DB_NAME, "test");
      cobj.put(ComObject.Tag.SCHEMA_VERSION, 1000);
      cobj.put(ComObject.Tag.IS_EXCPLICITE_TRANS, false);
      cobj.put(ComObject.Tag.IS_COMMITTING, false);
      cobj.put(ComObject.Tag.TRANSACTION_ID, 0L);
      cobj.put(ComObject.Tag.SEQUENCE_0, 1000L);
      cobj.put(ComObject.Tag.SEQUENCE_1, 1000L);

      updateManager.insertIndexEntryByKeyWithRecord(cobj, false);
    }

    for (int j = 0; j < keys.size(); j++) {
      assertEquals(addressMap.fromUnsafeToRecords(index.get(keys.get(j)))[0], records[j]);
    }
    //test delete
    for (int j = 0; j < 4; j++) {
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.SERIALIZATION_VERSION, DatabaseClient.SERIALIZATION_VERSION);
      cobj.put(ComObject.Tag.KEY_BYTES, DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), keys.get(j)));
      cobj.put(ComObject.Tag.SCHEMA_VERSION, 1000);
      cobj.put(ComObject.Tag.DB_NAME, "test");
      cobj.put(ComObject.Tag.TABLE_NAME, tableSchema.getName());
      cobj.put(ComObject.Tag.INDEX_NAME, indexSchema.getName());
      cobj.put(ComObject.Tag.IS_EXCPLICITE_TRANS, false);
      cobj.put(ComObject.Tag.IS_COMMITTING, false);
      cobj.put(ComObject.Tag.TRANSACTION_ID, 0L);
      cobj.put(ComObject.Tag.SEQUENCE_0, 1000L);
      cobj.put(ComObject.Tag.SEQUENCE_1, 1000L);
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
      int indexId = common.getTables("test").get("table1").getIndices().get(indexSchema.getName()).getIndexId();

      InsertStatementHandler.KeyInfo keyInfo = new InsertStatementHandler.KeyInfo();
      keyInfo.setIndexSchema(indexSchema);
      keyInfo.setKey(keys.get(j));

      byte[] primaryKeyBytes = DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), keys.get(j));

      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.RECORD_BYTES, records[j]);
      cobj.put(ComObject.Tag.KEY_BYTES, primaryKeyBytes);

      cobj.put(ComObject.Tag.TABLE_ID, tableId);
      cobj.put(ComObject.Tag.INDEX_ID, indexId);
      cobj.put(ComObject.Tag.ORIGINAL_OFFSET, j);
      cobj.put(ComObject.Tag.ORIGINAL_IGNORE, true);
      cobj.put(ComObject.Tag.DB_NAME, "test");
      cobj.put(ComObject.Tag.SCHEMA_VERSION, 1000);
      cobj.put(ComObject.Tag.IS_EXCPLICITE_TRANS, false);
      cobj.put(ComObject.Tag.IS_COMMITTING, false);
      cobj.put(ComObject.Tag.TRANSACTION_ID, 0L);
      cobj.put(ComObject.Tag.SEQUENCE_0, 1000L);
      cobj.put(ComObject.Tag.SEQUENCE_1, 1000L);

      updateManager.insertIndexEntryByKeyWithRecord(cobj, false);
    }

    for (int j = 0; j < keys.size(); j++) {
      assertEquals(addressMap.fromUnsafeToRecords(index.get(keys.get(j)))[0], records[j]);
    }

    for (int j = 0; j < 4; j++) {
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.DB_NAME, "test");
      cobj.put(ComObject.Tag.SCHEMA_VERSION, 1000);

      cobj.put(ComObject.Tag.TABLE_NAME, tableSchema.getName());
      cobj.put(ComObject.Tag.INDEX_NAME, indexSchema.getName());
      cobj.put(ComObject.Tag.PRIMARY_KEY_INDEX_NAME, indexSchema.getName());
      cobj.put(ComObject.Tag.IS_EXCPLICITE_TRANS, false);
      cobj.put(ComObject.Tag.IS_COMMITTING, false);
      cobj.put(ComObject.Tag.TRANSACTION_ID, 0L);

      byte[] keyBytes = DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), keys.get(j));
      cobj.put(ComObject.Tag.KEY_BYTES, keyBytes);
      cobj.put(ComObject.Tag.PRIMARY_KEY_BYTES, keyBytes);
      cobj.put(ComObject.Tag.SEQUENCE_0, 1000L);
      cobj.put(ComObject.Tag.SEQUENCE_1, 1000L);

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
    cobj.put(ComObject.Tag.DB_NAME, "test");
    cobj.put(ComObject.Tag.SCHEMA_VERSION, 1000);
    cobj.put(ComObject.Tag.TABLE_NAME, tableSchema.getName());
    cobj.put(ComObject.Tag.PHASE, "secondary");

    updateManager.truncateTable(cobj, false);

    cobj.put(ComObject.Tag.PHASE, "primary");

    updateManager.truncateTable(cobj, false);

    for (int j = 0; j < keys.size(); j++) {
      assertNull(index.get(keys.get(j)));
    }
  }

  @Test
  public void testBatchInsert() throws SQLException {

    InsertStatementHandler handler = new InsertStatementHandler(client);
    InsertStatementHandler.getBatch().set(new ArrayList<InsertStatementHandler.InsertRequest>());



    for (int i = 0; i < keys.size(); i++) {
      InsertStatementImpl insertStatement = new InsertStatementImpl(client);

      insertStatement.setTableName("table1");
      List<String> columns = new ArrayList<>();
      columns.add("field1");
      insertStatement.setColumns(columns);
      insertStatement.addValue("field1", keys.get(i)[0]);

      handler.doInsert("test", insertStatement, 0);
    }

    when(client.send(eq("UpdateManager:batchInsertIndexEntryByKeyWithRecord"), anyInt(), anyInt(), (ComObject)anyObject(), eq(DatabaseClient.Replica.DEF))).thenAnswer(
        new Answer() {
          public Object answer(InvocationOnMock invocation) {
            Object[] args = invocation.getArguments();
            ComObject cobj = (ComObject) args[3];
            cobj.put(ComObject.Tag.SEQUENCE_0, 1000L);
            cobj.put(ComObject.Tag.SEQUENCE_1, 1000L);
            ComObject ret = updateManager.batchInsertIndexEntryByKeyWithRecord(cobj, false);
            ret.put(ComObject.Tag.COUNT, 1);
            return ret.serialize();
          }
        });
    handler.executeBatch();

    for (int j = 0; j < 1; j++) {
      assertEquals(new Record("test", common, addressMap.fromUnsafeToRecords(index.get(keys.get(j)))[0]).getFields()[1],
          new Record("test", common, records[j]).getFields()[1]);
    }
  }

  @Test
  public void testBatchInsertByKey() throws SQLException, UnsupportedEncodingException, EOFException {

    InsertStatementHandler handler = new InsertStatementHandler(client);
    InsertStatementImpl insertStatement = new InsertStatementImpl(client);

    insertStatement.setTableName("table1");
    List<String> columns = new ArrayList<>();
    columns.add("field1");
    columns.add("field2");
    insertStatement.setColumns(columns);
    //for (int i = 0; i < keys.size(); i++) {
      insertStatement.addValue("field1", keys.get(0)[0]);
      insertStatement.addValue("field2", String.valueOf(keys.get(0)[0]).getBytes("utf-8"));
    //}

    InsertStatementHandler.getBatch().set(new ArrayList<>());

    handler.doInsert("test", insertStatement, 0);

    final AtomicLong sequence = new AtomicLong(1000);
    when(client.send(eq("UpdateManager:batchInsertIndexEntryByKeyWithRecord"), anyInt(), anyInt(), (ComObject)anyObject(), eq(DatabaseClient.Replica.DEF))).thenAnswer(
        new Answer() {
          public Object answer(InvocationOnMock invocation) {
            Object[] args = invocation.getArguments();
            ComObject cobj = (ComObject) args[3];
            cobj.put(ComObject.Tag.SEQUENCE_0, sequence.incrementAndGet());
            cobj.put(ComObject.Tag.SEQUENCE_1, sequence.incrementAndGet());
            ComObject ret = updateManager.batchInsertIndexEntryByKeyWithRecord(cobj, false);
            ret.put(ComObject.Tag.COUNT, 1);
            return ret.serialize();
          }
        });

    when(client.send(eq("UpdateManager:batchInsertIndexEntryByKey"), anyInt(), anyInt(), (ComObject)anyObject(), eq(DatabaseClient.Replica.DEF))).thenAnswer(
        new Answer() {
          public Object answer(InvocationOnMock invocation) {
            Object[] args = invocation.getArguments();
            ComObject cobj = (ComObject) args[3];
            cobj.put(ComObject.Tag.SEQUENCE_0, sequence.incrementAndGet());
            cobj.put(ComObject.Tag.SEQUENCE_1, sequence.incrementAndGet());
            ComObject ret = updateManager.batchInsertIndexEntryByKey(cobj, false);
            ret.put(ComObject.Tag.COUNT, 1);
            return ret.serialize();
          }
        });
    int[] ret = handler.executeBatch();
    assertEquals(ret[0], BATCH_STATUS_SUCCCESS);

    for (int j = 0; j < 1; j++) {
      assertEquals(new Record("test", common, addressMap.fromUnsafeToRecords(index.get(keys.get(j)))[0]).getFields()[1],
          new Record("test", common, records[j]).getFields()[1]);
    }

    for (int j = 0; j < 1; j++) {
      KeyRecord keyRecord = new KeyRecord(addressMap.fromUnsafeToRecords(stringIndex.get(new Object[]{String.valueOf(keys.get(j)[0]).getBytes("utf-8")}))[0]);

      assertEquals(new Record("test", common, addressMap.fromUnsafeToRecords(index.get(DatabaseCommon.deserializeKey(tableSchema, keyRecord.getPrimaryKey())))[0]).getFields()[1],
          new Record("test", common, records[j]).getFields()[1]);
    }

    InsertStatementHandler.getBatch().set(new ArrayList<>());

    handler.doInsert("test", insertStatement, 0);

    ret = handler.executeBatch();
    assertEquals(ret[0], BATCH_STATUS_FAILED);

  }

  @Test
  public void testPopulateIndex() throws SQLException, UnsupportedEncodingException, EOFException {

    InsertStatementHandler handler = new InsertStatementHandler(client);
    InsertStatementImpl insertStatement = new InsertStatementImpl(client);
    for (Map.Entry<String, IndexSchema> schema : tableSchema.getIndices().entrySet()) {
      if (schema.getValue().getName().equals("stringIndex")) {
        tableSchema.getIndices().remove(schema.getKey());
        break;
      }
    }
    for (Map.Entry<Integer, IndexSchema> schema : tableSchema.getIndexesById().entrySet()) {
      if (schema.getValue().getName().equals("stringIndex")) {
        tableSchema.getIndexesById().remove(schema.getKey());
        break;
      }
    }

    insertStatement.setTableName("table1");
    List<String> columns = new ArrayList<>();
    columns.add("field1");
    columns.add("field2");
    insertStatement.setColumns(columns);
    for (int i = 0; i < keys.size(); i++) {
      insertStatement.addValue("field1", keys.get(i)[0]);
      insertStatement.addValue("field2", String.valueOf(keys.get(i)[0]).getBytes("utf-8"));
    }

    InsertStatementHandler.getBatch().set(new ArrayList<InsertStatementHandler.InsertRequest>());

    handler.doInsert("test", insertStatement, 0);

    final AtomicLong sequence = new AtomicLong(1000);
    when(client.send(eq("UpdateManager:batchInsertIndexEntryByKeyWithRecord"), anyInt(), anyInt(), (ComObject)anyObject(), eq(DatabaseClient.Replica.DEF))).thenAnswer(
        new Answer() {
          public Object answer(InvocationOnMock invocation) {
            Object[] args = invocation.getArguments();
            ComObject cobj = (ComObject) args[3];
            cobj.put(ComObject.Tag.SEQUENCE_0, sequence.incrementAndGet());
            cobj.put(ComObject.Tag.SEQUENCE_1, sequence.incrementAndGet());
            ComObject ret = updateManager.batchInsertIndexEntryByKeyWithRecord(cobj, false);
            ret.put(ComObject.Tag.COUNT, 1);
            return ret.serialize();
          }
        });

    when(client.send(eq("UpdateManager:batchInsertIndexEntryByKey"), anyInt(), anyInt(), (ComObject)anyObject(), eq(DatabaseClient.Replica.DEF))).thenAnswer(
        new Answer() {
          public Object answer(InvocationOnMock invocation) {
            Object[] args = invocation.getArguments();
            ComObject cobj = (ComObject) args[3];
            cobj.put(ComObject.Tag.SEQUENCE_0, sequence.incrementAndGet());
            cobj.put(ComObject.Tag.SEQUENCE_1, sequence.incrementAndGet());
            ComObject ret = updateManager.batchInsertIndexEntryByKey(cobj, false);
            ret.put(ComObject.Tag.COUNT, 1);
            return ret.serialize();
          }
        });
    int[] ret = handler.executeBatch();
    assertEquals(ret[0], BATCH_STATUS_SUCCCESS);

    for (int j = 0; j < 1; j++) {
      assertEquals(new Record("test", common, addressMap.fromUnsafeToRecords(index.get(keys.get(j)))[0]).getFields()[1],
          new Record("test", common, records[j]).getFields()[1]);
    }

    assertEquals(stringIndex.size(), 0);

    TestUtils.createStringIndexSchema(tableSchema);

    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, "test");
    cobj.put(ComObject.Tag.TABLE_NAME, "table1");
    cobj.put(ComObject.Tag.INDEX_NAME, "stringIndex");


    when(client.send(eq("UpdateManager:insertIndexEntryByKey"), anyInt(), anyInt(), (ComObject)anyObject(), eq(DatabaseClient.Replica.DEF))).thenAnswer(
        new Answer() {
          public Object answer(InvocationOnMock invocation) {
            Object[] args = invocation.getArguments();
            ComObject cobj = (ComObject) args[3];
            cobj.put(ComObject.Tag.SEQUENCE_0, 1000L);
            cobj.put(ComObject.Tag.SEQUENCE_1, 1000L);
            updateManager.insertIndexEntryByKey(cobj, false);
            return null;
          }
        });

    updateManager.doPopulateIndex(cobj, false);

    for (int j = 0; j < 1; j++) {
      KeyRecord keyRecord = new KeyRecord(addressMap.fromUnsafeToRecords(stringIndex.get(new Object[]{String.valueOf(keys.get(j)[0]).getBytes("utf-8")}))[0]);

      assertEquals(new Record("test", common, addressMap.fromUnsafeToRecords(index.get(DatabaseCommon.deserializeKey(tableSchema, keyRecord.getPrimaryKey())))[0]).getFields()[1],
          new Record("test", common, records[j]).getFields()[1]);
    }
  }

  @Test
  public void testDeleteIndexEntry() throws SQLException {

    InsertStatementHandler handler = new InsertStatementHandler(client);
    InsertStatementImpl insertStatement = new InsertStatementImpl(client);

    insertStatement.setTableName("table1");
    List<String> columns = new ArrayList<>();
    columns.add("field1");
    columns.add("field2");
    insertStatement.setColumns(columns);
    for (int i = 0; i < keys.size(); i++) {
      insertStatement.addValue("field1", keys.get(i)[0]);
      Record record = new Record("test", common, records[0]);
      insertStatement.addValue("field2",
          record.getFields()[2]);
    }

    InsertStatementHandler.getBatch().set(new ArrayList<InsertStatementHandler.InsertRequest>());

    handler.doInsert("test", insertStatement, 0);

    final AtomicLong sequence = new AtomicLong(1000);
    when(client.send(eq("UpdateManager:batchInsertIndexEntryByKeyWithRecord"), anyInt(), anyInt(), (ComObject)anyObject(), eq(DatabaseClient.Replica.DEF))).thenAnswer(
        new Answer() {
          public Object answer(InvocationOnMock invocation) {
            Object[] args = invocation.getArguments();
            ComObject cobj = (ComObject) args[3];
            cobj.put(ComObject.Tag.SEQUENCE_0, sequence.incrementAndGet());
            cobj.put(ComObject.Tag.SEQUENCE_1, sequence.incrementAndGet());
            ComObject ret = updateManager.batchInsertIndexEntryByKeyWithRecord(cobj, false);
            ret.put(ComObject.Tag.COUNT, 1);
            return ret.serialize();
          }
        });

    when(client.send(eq("UpdateManager:batchInsertIndexEntryByKey"), anyInt(), anyInt(), (ComObject)anyObject(), eq(DatabaseClient.Replica.DEF))).thenAnswer(
        new Answer() {
          public Object answer(InvocationOnMock invocation) {
            Object[] args = invocation.getArguments();
            ComObject cobj = (ComObject) args[3];
            cobj.put(ComObject.Tag.SEQUENCE_0, sequence.incrementAndGet());
            cobj.put(ComObject.Tag.SEQUENCE_1, sequence.incrementAndGet());
            ComObject ret = updateManager.batchInsertIndexEntryByKey(cobj, false);
            ret.put(ComObject.Tag.COUNT, 1);
            return ret.serialize();
          }
        });
    int[] ret = handler.executeBatch();
    assertEquals(ret[0], BATCH_STATUS_SUCCCESS);

    for (int j = 0; j < 1; j++) {
      assertEquals(new Record("test", common, addressMap.fromUnsafeToRecords(index.get(keys.get(j)))[0]).getFields()[1],
          new Record("test", common, records[j]).getFields()[1]);
    }

    assertEquals(stringIndex.size(), 1);

    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, "test");
    cobj.put(ComObject.Tag.TABLE_NAME, "table1");
    cobj.put(ComObject.Tag.INDEX_NAME, "stringIndex");


    when(client.send(eq("UpdateManager:insertIndexEntryByKey"), anyInt(), anyInt(), (ComObject)anyObject(), eq(DatabaseClient.Replica.DEF))).thenAnswer(
        new Answer() {
          public Object answer(InvocationOnMock invocation) {
            Object[] args = invocation.getArguments();
            ComObject cobj = (ComObject) args[3];
            cobj.put(ComObject.Tag.SEQUENCE_0, 1000L);
            cobj.put(ComObject.Tag.SEQUENCE_1, 1000L);
            updateManager.insertIndexEntryByKey(cobj, false);
            return null;
          }
        });

    cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, "test");
    cobj.put(ComObject.Tag.SCHEMA_VERSION, 1000);
    cobj.put(ComObject.Tag.PRIMARY_KEY_BYTES, DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), keys.get(0)));
    cobj.put(ComObject.Tag.TABLE_NAME, "table1");
    cobj.put(ComObject.Tag.IS_EXCPLICITE_TRANS, false);
    cobj.put(ComObject.Tag.IS_COMMITTING, false);
    cobj.put(ComObject.Tag.TRANSACTION_ID, 0L);
    cobj.put(ComObject.Tag.SEQUENCE_0, 1000L);
    cobj.put(ComObject.Tag.SEQUENCE_1, 1000L);
    byte[] bytes = records[0];
    cobj.put(ComObject.Tag.RECORD_BYTES, bytes);
    updateManager.deleteIndexEntry(cobj, false);

    assertEquals(stringIndex.size(), 0);

  }


  @Test
  public void testUpdateRecord() throws SQLException, UnsupportedEncodingException {

    InsertStatementHandler handler = new InsertStatementHandler(client);
    InsertStatementImpl insertStatement = new InsertStatementImpl(client);

    insertStatement.setTableName("table1");
    List<String> columns = new ArrayList<>();
    columns.add("field1");
    columns.add("field2");
    insertStatement.setColumns(columns);
    for (int i = 0; i < keys.size(); i++) {
      insertStatement.addValue("field1", keys.get(i)[0]);
      Record record = new Record("test", common, records[0]);
      insertStatement.addValue("field2",
          record.getFields()[2]);
    }

    InsertStatementHandler.getBatch().set(new ArrayList<>());

    handler.doInsert("test", insertStatement, 0);

    final AtomicLong sequence = new AtomicLong(1000);
    when(client.send(eq("UpdateManager:batchInsertIndexEntryByKeyWithRecord"), anyInt(), anyInt(), (ComObject)anyObject(), eq(DatabaseClient.Replica.DEF))).thenAnswer(
        new Answer() {
          public Object answer(InvocationOnMock invocation) {
            Object[] args = invocation.getArguments();
            ComObject cobj = (ComObject) args[3];
            cobj.put(ComObject.Tag.SEQUENCE_0, sequence.incrementAndGet());
            cobj.put(ComObject.Tag.SEQUENCE_1, sequence.incrementAndGet());
            ComObject ret = updateManager.batchInsertIndexEntryByKeyWithRecord(cobj, false);
            ret.put(ComObject.Tag.COUNT, 1);
            return ret.serialize();
          }
        });

    when(client.send(eq("UpdateManager:batchInsertIndexEntryByKey"), anyInt(), anyInt(), (ComObject)anyObject(), eq(DatabaseClient.Replica.DEF))).thenAnswer(
        new Answer() {
          public Object answer(InvocationOnMock invocation) {
            Object[] args = invocation.getArguments();
            ComObject cobj = (ComObject) args[3];
            cobj.put(ComObject.Tag.SEQUENCE_0, sequence.incrementAndGet());
            cobj.put(ComObject.Tag.SEQUENCE_1, sequence.incrementAndGet());
            ComObject ret = updateManager.batchInsertIndexEntryByKey(cobj, false);
            ret.put(ComObject.Tag.COUNT, 1);
            return ret.serialize();
          }
        });
    int[] ret = handler.executeBatch();
    assertEquals(ret[0], BATCH_STATUS_SUCCCESS);

    for (int j = 0; j < 1; j++) {
      assertEquals(new Record("test", common, addressMap.fromUnsafeToRecords(index.get(keys.get(j)))[0]).getFields()[1],
          new Record("test", common, records[j]).getFields()[1]);
    }

    assertEquals(stringIndex.size(), 1);

    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, "test");
    cobj.put(ComObject.Tag.TABLE_NAME, "table1");
    cobj.put(ComObject.Tag.INDEX_NAME, "stringIndex");


    when(client.send(eq("UpdateManager:insertIndexEntryByKey"), anyInt(), anyInt(), (ComObject)anyObject(), eq(DatabaseClient.Replica.DEF))).thenAnswer(
        new Answer() {
          public Object answer(InvocationOnMock invocation) {
            Object[] args = invocation.getArguments();
            ComObject cobj = (ComObject) args[3];
            cobj.put(ComObject.Tag.SEQUENCE_0, 1000L);
            cobj.put(ComObject.Tag.SEQUENCE_1, 1000L);
            updateManager.insertIndexEntryByKey(cobj, false);
            return null;
          }
        });

    cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, "test");
    cobj.put(ComObject.Tag.SCHEMA_VERSION, 1000);
    cobj.put(ComObject.Tag.TABLE_NAME, "table1");
    cobj.put(ComObject.Tag.INDEX_NAME, indexSchema.getName());
    cobj.put(ComObject.Tag.IS_EXCPLICITE_TRANS, false);
    cobj.put(ComObject.Tag.IS_COMMITTING, false);
    cobj.put(ComObject.Tag.TRANSACTION_ID, 0L);
    Object[] newPrimaryKey = keys.get(0);
    cobj.put(ComObject.Tag.PRIMARY_KEY_BYTES, DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), newPrimaryKey));
    cobj.put(ComObject.Tag.PREV_KEY_BYTES, DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), newPrimaryKey));
    Record record = createNewRecord(common, tableSchema);
    cobj.put(ComObject.Tag.BYTES, record.serialize(client.getCommon(), SERIALIZATION_VERSION));
    cobj.put(ComObject.Tag.PREV_BYTES, record.serialize(client.getCommon(), SERIALIZATION_VERSION));
    cobj.put(ComObject.Tag.SEQUENCE_0, 1000L);
    cobj.put(ComObject.Tag.SEQUENCE_1, 1000L);
    AllRecordsExpressionImpl expression = new AllRecordsExpressionImpl();
    expression.setFromTable("table1");
    cobj.put(ComObject.Tag.LEGACY_EXPRESSION, ExpressionImpl.serializeExpression(expression));

    updateManager.updateRecord(cobj, false);

    for (int j = 0; j < 1; j++) {
      assertEquals(new Record("test", common, addressMap.fromUnsafeToRecords(index.get(keys.get(j)))[0]).getFields()[2],
          record.getFields()[2]);
    }
  }

  public static Record createNewRecord(DatabaseCommon common, TableSchema tableSchema) throws UnsupportedEncodingException {
    int i = 0;
    Object[] fieldArray = new Object[28];
    fieldArray[1] = 200L + (100 * i);
    fieldArray[2] = "changed".getBytes("utf-8");
    fieldArray[3] = new Timestamp(200 + (100 * i));
    fieldArray[4] = (int)(1200 + (100 * i));
    fieldArray[5] = (short)i;
    fieldArray[6] = (byte)i;
    fieldArray[7] = (i + "-value").getBytes("utf-8");
    fieldArray[8] = (i + "-value").getBytes("utf-8");
    fieldArray[9] = (double) i;
    fieldArray[10] = (float) i;
    fieldArray[11] = (double) i;
    fieldArray[12] = true;
    fieldArray[13] = true;
    fieldArray[14] = (i + "-value").getBytes("utf-8");
    fieldArray[15] = (i + "-value").getBytes("utf-8");
    fieldArray[16] = (i + "-value").getBytes("utf-8");
    fieldArray[17] = (i + "-value").getBytes("utf-8");
    fieldArray[18] = (i + "-value").getBytes("utf-8");
    fieldArray[19] = (i + "-value").getBytes("utf-8");
    fieldArray[20] = (i + "-value").getBytes("utf-8");
    fieldArray[21] = (i + "-value").getBytes("utf-8");
    fieldArray[22] = (i + "-value").getBytes("utf-8");
    fieldArray[23] = new BigDecimal(i);
    fieldArray[24] = new BigDecimal(i);
    fieldArray[25] = new Date(i);
    fieldArray[26] = new Time(i);
    fieldArray[27] = new Timestamp(i);

    Record record = new Record(tableSchema);
    record.setFields(fieldArray);
    return record;
  }

  @Test
  public void testTransactionsCommit() throws SQLException {

    int tableId = common.getTables("test").get("table1").getTableId();
    int indexId = common.getTables("test").get("table1").getIndices().get(indexSchema.getName()).getIndexId();

    InsertStatementHandler.KeyInfo keyInfo = new InsertStatementHandler.KeyInfo();
    keyInfo.setIndexSchema(indexSchema);
    keyInfo.setKey(keys.get(9));

    KeyRecord keyRecord = new KeyRecord();
    byte[] primaryKeyBytes = DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), keys.get(9));
    keyRecord.setPrimaryKey(primaryKeyBytes);
    keyRecord.setDbViewNumber(common.getSchemaVersion());

    ComObject cobj = InsertStatementHandler.serializeInsertKey(client, common, 0, "test", 0, tableId,
        indexId, "table1", keyInfo, indexSchema.getName(),
        keys.get(9), null, keyRecord, false);

    byte[] keyRecordBytes = keyRecord.serialize(SERIALIZATION_VERSION);
    cobj.put(ComObject.Tag.KEY_RECORD_BYTES, keyRecordBytes);

    cobj.put(ComObject.Tag.DB_NAME, "test");
    cobj.put(ComObject.Tag.SCHEMA_VERSION, 1000);
    cobj.put(ComObject.Tag.IS_EXCPLICITE_TRANS, true);
    cobj.put(ComObject.Tag.IS_COMMITTING, false);
    cobj.put(ComObject.Tag.TRANSACTION_ID, 1L);
    cobj.put(ComObject.Tag.SEQUENCE_0, 1000L);
    cobj.put(ComObject.Tag.SEQUENCE_1, 1000L);

    updateManager.insertIndexEntryByKey(cobj, false);

    assertNull(index.get(keys.get(9)));


    InsertStatementHandler handler = new InsertStatementHandler(client);
    InsertStatementImpl insertStatement = new InsertStatementImpl(client);

    insertStatement.setTableName("table1");
    List<String> columns = new ArrayList<>();
    columns.add("field1");
    columns.add("field2");
    insertStatement.setColumns(columns);
    for (int i = 0; i < keys.size(); i++) {
      insertStatement.addValue("field1", keys.get(i)[0]);
      Record record = new Record("test", common, records[0]);
      insertStatement.addValue("field2",
          record.getFields()[2]);
    }

    InsertStatementHandler.getBatch().set(new ArrayList<>());

    handler.doInsert("test", insertStatement, 0);

    final AtomicLong sequence = new AtomicLong(1000);
    when(client.send(eq("UpdateManager:batchInsertIndexEntryByKeyWithRecord"), anyInt(), anyInt(), (ComObject)anyObject(), eq(DatabaseClient.Replica.DEF))).thenAnswer(
        new Answer() {
          public Object answer(InvocationOnMock invocation) {
            Object[] args = invocation.getArguments();
            ComObject cobj = (ComObject) args[3];
            cobj.put(ComObject.Tag.IS_EXCPLICITE_TRANS, true);
            cobj.put(ComObject.Tag.IS_COMMITTING, false);
            cobj.put(ComObject.Tag.TRANSACTION_ID, 1L);
            cobj.put(ComObject.Tag.SEQUENCE_0, sequence.incrementAndGet());
            cobj.put(ComObject.Tag.SEQUENCE_1, sequence.incrementAndGet());
            ComObject ret = updateManager.batchInsertIndexEntryByKeyWithRecord(cobj, false);
            ret.put(ComObject.Tag.COUNT, 1);
            return ret.serialize();
          }
        });

    when(client.send(eq("UpdateManager:batchInsertIndexEntryByKey"), anyInt(), anyInt(), (ComObject)anyObject(), eq(DatabaseClient.Replica.DEF))).thenAnswer(
        new Answer() {
          public Object answer(InvocationOnMock invocation) {
            Object[] args = invocation.getArguments();
            ComObject cobj = (ComObject) args[3];
            cobj.put(ComObject.Tag.IS_EXCPLICITE_TRANS, true);
            cobj.put(ComObject.Tag.IS_COMMITTING, false);
            cobj.put(ComObject.Tag.TRANSACTION_ID, 1L);
            cobj.put(ComObject.Tag.SEQUENCE_0, sequence.incrementAndGet());
            cobj.put(ComObject.Tag.SEQUENCE_1, sequence.incrementAndGet());
            ComObject ret = updateManager.batchInsertIndexEntryByKey(cobj, false);
            ret.put(ComObject.Tag.COUNT, 1);
            return ret.serialize();
          }
        });
    int[] ret = handler.executeBatch();
    assertEquals(ret[0], BATCH_STATUS_SUCCCESS);

    for (int j = 0; j < 1; j++) {
      assertNull(index.get(keys.get(j)));
    }

    cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, "test");
    cobj.put(ComObject.Tag.SCHEMA_VERSION, 1000);
    cobj.put(ComObject.Tag.TRANSACTION_ID, 1L);
    cobj.put(ComObject.Tag.SEQUENCE_0, 1000L);
    cobj.put(ComObject.Tag.SEQUENCE_1, 1000L);

    updateManager.commit(cobj, false);

    assertNotNull(index.get(keys.get(9)));


    for (int j = 0; j < 1; j++) {
      assertEquals(new Record("test", common, addressMap.fromUnsafeToRecords(index.get(keys.get(j)))[0]).getFields()[1],
          new Record("test", common, records[j]).getFields()[1]);
    }

  }

  @Test
  public void testTransactionsRollback() {

    for (int j = 0; j < keys.size(); j++) {
      int tableId = common.getTables("test").get("table1").getTableId();
      int indexId = common.getTables("test").get("table1").getIndices().get(indexSchema.getName()).getIndexId();

      InsertStatementHandler.KeyInfo keyInfo = new InsertStatementHandler.KeyInfo();
      keyInfo.setIndexSchema(indexSchema);
      keyInfo.setKey(keys.get(j));

      KeyRecord keyRecord = new KeyRecord();
      byte[] primaryKeyBytes = DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), keys.get(j));
      keyRecord.setPrimaryKey(primaryKeyBytes);
      keyRecord.setDbViewNumber(common.getSchemaVersion());

      ComObject cobj = InsertStatementHandler.serializeInsertKey(client, common, 0, "test", 0, tableId,
          indexId, "table1", keyInfo, indexSchema.getName(),
          keys.get(j), null, keyRecord, false);

      byte[] keyRecordBytes = keyRecord.serialize(SERIALIZATION_VERSION);
      cobj.put(ComObject.Tag.KEY_RECORD_BYTES, keyRecordBytes);

      cobj.put(ComObject.Tag.DB_NAME, "test");
      cobj.put(ComObject.Tag.SCHEMA_VERSION, 1000);
      cobj.put(ComObject.Tag.IS_EXCPLICITE_TRANS, true);
      cobj.put(ComObject.Tag.IS_COMMITTING, false);
      cobj.put(ComObject.Tag.TRANSACTION_ID, 1L);
      cobj.put(ComObject.Tag.SEQUENCE_0, 1000L);
      cobj.put(ComObject.Tag.SEQUENCE_1, 1000L);

      updateManager.insertIndexEntryByKey(cobj, false);
    }

    for (int j = 0; j < keys.size(); j++) {
      assertNull(index.get(keys.get(j)));
    }

    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, "test");
    cobj.put(ComObject.Tag.SCHEMA_VERSION, 1000);
    cobj.put(ComObject.Tag.TRANSACTION_ID, 1L);
    cobj.put(ComObject.Tag.SEQUENCE_0, 1000L);
    cobj.put(ComObject.Tag.SEQUENCE_1, 1000L);

    updateManager.rollback(cobj, false);

    for (int j = 0; j < keys.size(); j++) {
      assertNull(index.get(keys.get(j)));
    }

    TransactionManager.Transaction trans = server.getTransactionManager().getTransaction(1L);
    assertNull(trans);
  }

  private ResultSet createResultSetMock() throws SQLException {
    ResultSet rs = mock(ResultSet.class);
    when(rs.getLong(eq("field1"))).thenReturn(200L);
    when(rs.getString(eq("field2"))).thenReturn("0-value");
    when(rs.getTimestamp(eq("field3"))).thenReturn(new Timestamp(200));
    when(rs.getInt(eq("field4"))).thenReturn(1200);
    return rs;
  }

  @Test
  public void testInsertWithSelect() throws JSQLParserException, UnsupportedEncodingException, SQLException {
    String sql = "INSERT INTO table1 (field1, field2, field3, field4, field4, field5, field6, field7, field8, field9, field10, field11, field12, field13, field14, field15, field16, field17, field18, field19, field20, field21, field22, field23, field24, field25, field26, field27)\n" +
        "SELECT field1, field2, field3, field4, field4, field5, field6, field7, field8, field9, field10, field11, field12, field13, field14, field15, field16, field17, field18, field19, field20, field21, field22, field23, field24, field25, field26, field27\n" +
        "FROM table1\n" +
        "WHERE field1 > 0";
    CCJSqlParserManager parser = new CCJSqlParserManager();
    Insert insert = (Insert) parser.parse(new StringReader(sql));

    when(client.send(anyString(), anyInt(), anyInt(), anyObject(), anyObject())).thenAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock) {
        ComObject retObj = new ComObject();
        ComArray array = retObj.putArray(ComObject.Tag.KEYS, ComObject.Type.BYTE_ARRAY_TYPE);
        array = retObj.putArray(ComObject.Tag.KEY_RECORDS, ComObject.Type.BYTE_ARRAY_TYPE);
        array = retObj.putArray(ComObject.Tag.RECORDS, ComObject.Type.BYTE_ARRAY_TYPE);

        for (int i = 0; i < records.length; i++) {
          byte[] bytes = records[i];
          array.add(bytes);
        }

        retObj.put(ComObject.Tag.CURR_OFFSET, records.length);
        retObj.put(ComObject.Tag.COUNT_RETURNED, records.length);

        return retObj.serialize();
      }
    });
    InsertStatementImpl insertStatement = new InsertStatementImpl(client);
    Select select = insert.getSelect();
    if (select != null) {
      SelectBody selectBody = select.getSelectBody();
      AtomicInteger currParmNum = new AtomicInteger();
      if (selectBody instanceof PlainSelect) {
        SelectStatementImpl selectStatement = SelectStatementHandler.parseSelectStatement(client, new ParameterHandler(),
            (PlainSelect) selectBody, currParmNum);
        insertStatement.setSelect(selectStatement);
      }
      else {
        throw new DatabaseException("Unsupported select type");
      }
    }

    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, "test");
    cobj.put(ComObject.Tag.SCHEMA_VERSION, client.getCommon().getSchemaVersion());
    cobj.put(ComObject.Tag.METHOD, "UpdateManager:insertWithSelect");

    SelectStatementImpl selectStatement = insertStatement.getSelect();
    if (select != null) {
      String[] tableNames = new String[]{selectStatement.getFromTable()};
      selectStatement.setTableNames(tableNames);
    }

    String columnsStr = "field1 field2 field3 field4 field4 field5 field6 field7 field8 field9 field10 field11 field12 field13 field14 field15 field16 field17 field18 field19 field20 field21 field22 field23 field24 field25 field26 field27";
    String[] columns = columnsStr.split(" ");
    List<String> columnsList = new ArrayList<>();
    columnsList.addAll(Arrays.asList(columns));
    insertStatement.setColumns(columnsList);
    insertStatement.serialize(cobj);

    ResultSet rs = createResultSetMock();
    PreparedStatement ps = mock(PreparedStatement.class);

    final AtomicBoolean calledSetLong = new AtomicBoolean();
    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock) {
        calledSetLong.set(true);
        return null;
      }
    }).when(ps).setLong(anyInt(), eq(200L));
    final Set<Integer> called = new HashSet<>();
    final AtomicInteger countReturned = new AtomicInteger();
    when(ps.executeQuery()).thenReturn(rs);
    when(rs.next()).thenAnswer(new Answer(){
      @Override
      public Object answer(InvocationOnMock invocationOnMock) {
        return countReturned.getAndIncrement() == 0;
      }
    });
    final Connection connection = mock(Connection.class);
    when(connection.prepareStatement(anyString())).thenReturn(ps);

    UpdateManager updateManager = new UpdateManager(server) {
      protected Connection getSonicBaseConnection(String dbName, String address, int port) {
        return connection;
      }
    };
    updateManager.insertWithSelect(cobj, false);
    assertTrue(calledSetLong.get());
  }
}
