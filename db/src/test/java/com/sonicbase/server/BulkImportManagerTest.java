package com.sonicbase.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.*;
import com.sonicbase.index.AddressMap;
import com.sonicbase.index.Index;
import com.sonicbase.index.Indices;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.TestUtils;
import org.apache.commons.io.IOUtils;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.*;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.sonicbase.schema.DataType.Type.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class BulkImportManagerTest {
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
  private TableSchema tableSchema2;

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
    stringIndexSchema = TestUtils.createStringIndexSchema(tableSchema, 1);

    when(server.getIndexSchema(anyString(), anyString(), anyString())).thenReturn(indexSchema);
    when(server.getShardCount()).thenReturn(2);
    when(server.getReplicationFactor()).thenReturn(1);

    String configStr = IOUtils.toString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-4-servers.yaml")), "utf-8");
    final Config config = new Config(configStr);

    when(server.getConfig()).thenReturn(config);
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
    indices.addIndex(server.getPort(), new HashMap<Long, Boolean>(), tableSchema, indexSchema.getName(), indexSchema.getComparators());
    indices.addIndex(server.getPort(), new HashMap<Long, Boolean>(), tableSchema, stringIndexSchema.getName(), stringIndexSchema.getComparators());
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

    client = mock(DatabaseClient.class);
    when(client.getCommon()).thenReturn(common);
    when(client.getShardCount()).thenReturn(1);
    when(client.getExecutor()).thenReturn(executor);
    transId = new AtomicLong();
    when(client.allocateId(anyString())).thenAnswer(
        new Answer() {
          public Object answer(InvocationOnMock invocation) {
            return transId.incrementAndGet();
          }
        });

    when(server.getClient()).thenReturn(client);
  }

  @Test
  public void testBulkImport() throws SQLException, UnsupportedEncodingException {

    ResultSet rs = createResultSetMock();

    BulkImportManager bim = new BulkImportManager(server);

    Object[] record = bim.getCurrRecordFromResultSet(rs, tableSchema.getFields());
    Object[] dbFields = getDbFields(record);

    assertEquals(record, dbFields);
  }

  private ResultSet createResultSetMock() throws SQLException, UnsupportedEncodingException {
    ResultSet rs = mock(ResultSet.class);
    when(rs.getLong(eq("field1"))).thenReturn(200L);
    when(rs.getString(eq("field2"))).thenReturn("0-value");
    when(rs.getTimestamp(eq("field3"))).thenReturn(new Timestamp(200));
    when(rs.getInt(eq("field4"))).thenReturn(1200);
    when(rs.getShort(eq("field5"))).thenReturn((short)0);
    when(rs.getByte(eq("field6"))).thenReturn((byte)0);
    when(rs.getString(eq("field7"))).thenReturn("0-value");
    when(rs.getString(eq("field8"))).thenReturn("0-value");
    when(rs.getDouble(eq("field9"))).thenReturn(0d);
    when(rs.getFloat(eq("field10"))).thenReturn(0f);
    when(rs.getDouble(eq("field11"))).thenReturn(0d);
    when(rs.getBoolean(eq("field12"))).thenReturn(true);
    when(rs.getBoolean(eq("field13"))).thenReturn(true);
    when(rs.getString(eq("field14"))).thenReturn("0-value");
    when(rs.getString(eq("field15"))).thenReturn("0-value");
    when(rs.getString(eq("field16"))).thenReturn("0-value");
    when(rs.getString(eq("field17"))).thenReturn("0-value");
    when(rs.getString(eq("field18"))).thenReturn("0-value");
    when(rs.getString(eq("field19"))).thenReturn("0-value");
    when(rs.getBytes(eq("field20"))).thenReturn("0-value".getBytes("utf-8"));
    when(rs.getBytes(eq("field21"))).thenReturn("0-value".getBytes("utf-8"));
    when(rs.getBytes(eq("field22"))).thenReturn("0-value".getBytes("utf-8"));
    when(rs.getBigDecimal(eq("field23"))).thenReturn(new BigDecimal(0));
    when(rs.getBigDecimal(eq("field24"))).thenReturn(new BigDecimal(0));
    when(rs.getDate(eq("field25"))).thenReturn(new Date(1900, 10, 1));
    when(rs.getTime(eq("field26"))).thenReturn(new Time(1, 0, 0));
    when(rs.getTimestamp(eq("field27"))).thenReturn(new Timestamp(0));
    return rs;
  }

  private Object[] getDbFields(Object[] record) throws UnsupportedEncodingException {
    Record dbRecord = new Record("test", common, records[0]);
    Object[] origDbFields = dbRecord.getFields();
    Object[] dbFields = new Object[record.length];
    dbFields[0] = 0L;
    dbFields[1] = origDbFields[1];
    dbFields[2] = new String((byte[]) origDbFields[2], "utf-8");
    dbFields[3] = origDbFields[3];
    dbFields[4] = origDbFields[4];
    dbFields[5] = origDbFields[5];
    dbFields[6] = origDbFields[6];
    dbFields[7] = new String((byte[]) origDbFields[7], "utf-8");
    dbFields[8] = new String((byte[]) origDbFields[8], "utf-8");
    dbFields[9] = origDbFields[9];
    dbFields[10] = origDbFields[10];
    dbFields[11] = origDbFields[11];
    dbFields[12] = origDbFields[12];
    dbFields[13] = origDbFields[13];
    dbFields[14] = new String((byte[]) origDbFields[14], "utf-8");
    dbFields[15] = new String((byte[]) origDbFields[15], "utf-8");
    dbFields[16] = new String((byte[]) origDbFields[16], "utf-8");
    dbFields[17] = new String((byte[]) origDbFields[17], "utf-8");
    dbFields[18] = new String((byte[]) origDbFields[18], "utf-8");
    dbFields[19] = new String((byte[]) origDbFields[19], "utf-8");
    dbFields[20] = origDbFields[20];
    dbFields[21] = origDbFields[21];
    dbFields[22] = origDbFields[22];
    dbFields[23] = origDbFields[23];
    dbFields[24] = origDbFields[24];
    dbFields[25] = origDbFields[25];
    dbFields[26] = origDbFields[26];
    dbFields[27] = origDbFields[27];
    return dbFields;
  }

  @Test
  public void testSetFieldsInInsertStatement() throws UnsupportedEncodingException, SQLException {
    PreparedStatement ps = mock(PreparedStatement.class);

    final Set<Integer> called = new HashSet<>();
    attachSetters(ps, called);


//    when(ps.setDate(eq("field25"))).thenReturn(new Date(1900, 10, 1));
//    when(ps.setTime(eq("field26"))).thenReturn(new Time(1, 0, 0));
//    when(ps.setTimestamp(eq("field27"))).thenReturn(new Timestamp(0));

    Record record = new Record("test", common, records[0]);
    Object[] dbFields = getDbFields(record.getFields());
    BulkImportManager.setFieldsInInsertStatement(ps, 1, dbFields, tableSchema.getFields());

    assertEquals(called.size(), 28);
  }

  private void attachSetters(PreparedStatement ps, final Set<Integer> called) throws SQLException {
    doAnswer(new Answer(){
      @Override
      public Object answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        assertEquals(args[1], 0L);
        called.add(1);
        return null;
      }
    }).when(ps).setLong(eq(1), anyLong());

    doAnswer(new Answer(){
      @Override
      public Object answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        assertEquals(args[1], 200L);
        called.add(2);
        return null;
      }
    }).when(ps).setLong(eq(2), anyLong());

    doAnswer(new Answer(){
      @Override
      public Object answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        assertEquals(args[1], "0-value");
        called.add(3);
        return null;
      }
    }).when(ps).setString(eq(3), (String) anyObject());

    doAnswer(new Answer(){
      @Override
      public Object answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        assertEquals(args[1], new Timestamp(200));
        called.add(4);
        return null;
      }
    }).when(ps).setTimestamp(eq(4), (Timestamp) anyObject());

    doAnswer(new Answer(){
      @Override
      public Object answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        assertEquals(args[1], 1200);
        called.add(5);
        return null;
      }
    }).when(ps).setInt(eq(5), anyInt());

    doAnswer(new Answer(){
      @Override
      public Object answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        assertEquals(args[1], (short)0);
        called.add(6);
        return null;
      }
    }).when(ps).setShort(eq(6), anyShort());

    doAnswer(new Answer(){
      @Override
      public Object answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        assertEquals(args[1], (byte)0);
        called.add(7);
        return null;
      }
    }).when(ps).setByte(eq(7), anyByte());

    doAnswer(new Answer(){
      @Override
      public Object answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        assertEquals(args[1],  "0-value");
        called.add(8);
        return null;
      }
    }).when(ps).setString(eq(8), anyString());

    doAnswer(new Answer(){
      @Override
      public Object answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        assertEquals(args[1],  "0-value");
        called.add(9);
        return null;
      }
    }).when(ps).setString(eq(9), anyString());

    doAnswer(new Answer(){
      @Override
      public Object answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        assertEquals(args[1], 0d);
        called.add(10);
        return null;
      }
    }).when(ps).setDouble(eq(10), anyDouble());

    doAnswer(new Answer(){
      @Override
      public Object answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        assertEquals(args[1], 0f);
        called.add(11);
        return null;
      }
    }).when(ps).setFloat(eq(11), anyFloat());

    doAnswer(new Answer(){
      @Override
      public Object answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        assertEquals(args[1], 0d);
        called.add(12);
        return null;
      }
    }).when(ps).setDouble(eq(12), anyDouble());

    doAnswer(new Answer(){
      @Override
      public Object answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        assertEquals(args[1], true);
        called.add(13);
        return null;
      }
    }).when(ps).setBoolean(eq(13), anyBoolean());

    doAnswer(new Answer(){
      @Override
      public Object answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        assertEquals(args[1], true);
        called.add(14);
        return null;
      }
    }).when(ps).setBoolean(eq(14), anyBoolean());

    doAnswer(new Answer(){
      @Override
      public Object answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        assertEquals(args[1],  "0-value");
        called.add(15);
        return null;
      }
    }).when(ps).setString(eq(15), anyString());

    doAnswer(new Answer(){
      @Override
      public Object answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        assertEquals(args[1],  "0-value");
        called.add(16);
        return null;
      }
    }).when(ps).setString(eq(16), anyString());

    doAnswer(new Answer(){
      @Override
      public Object answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        assertEquals(args[1],  "0-value");
        called.add(17);
        return null;
      }
    }).when(ps).setString(eq(17), anyString());

    doAnswer(new Answer(){
      @Override
      public Object answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        assertEquals(args[1],  "0-value");
        called.add(18);
        return null;
      }
    }).when(ps).setString(eq(18), anyString());

    doAnswer(new Answer(){
      @Override
      public Object answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        assertEquals(args[1],  "0-value");
        called.add(19);
        return null;
      }
    }).when(ps).setString(eq(19), anyString());

    doAnswer(new Answer(){
      @Override
      public Object answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        assertEquals(args[1],  "0-value");
        called.add(20);
        return null;
      }
    }).when(ps).setString(eq(20), anyString());

    doAnswer(new Answer(){
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        assertEquals(args[1],  "0-value".getBytes("utf-8"));
        called.add(21);
        return null;
      }
    }).when(ps).setBytes(eq(21), (byte[]) anyObject());

    doAnswer(new Answer(){
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        assertEquals(args[1],  "0-value".getBytes("utf-8"));
        called.add(22);
        return null;
      }
    }).when(ps).setBytes(eq(22), (byte[]) anyObject());

    doAnswer(new Answer(){
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        assertEquals(args[1],  "0-value".getBytes("utf-8"));
        called.add(23);
        return null;
      }
    }).when(ps).setBytes(eq(23), (byte[]) anyObject());

    doAnswer(new Answer(){
      @Override
      public Object answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        assertEquals(args[1],  new BigDecimal(0));
        called.add(24);
        return null;
      }
    }).when(ps).setBigDecimal(eq(24), (BigDecimal) anyObject());

    doAnswer(new Answer(){
      @Override
      public Object answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        assertEquals(args[1],  new BigDecimal(0));
        called.add(25);
        return null;
      }
    }).when(ps).setBigDecimal(eq(25), (BigDecimal) anyObject());

    doAnswer(new Answer(){
      @Override
      public Object answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        assertEquals(args[1],  new Date(1900, 10, 1));
        called.add(26);
        return null;
      }
    }).when(ps).setDate(eq(26), (Date) anyObject());

    doAnswer(new Answer(){
      @Override
      public Object answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        assertEquals(args[1],  new Time(1, 0, 0));
        called.add(27);
        return null;
      }
    }).when(ps).setTime(eq(27), (Time) anyObject());

    doAnswer(new Answer(){
      @Override
      public Object answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        assertEquals(args[1],  new Timestamp(0));
        called.add(28);
        return null;
      }
    }).when(ps).setTimestamp(eq(28), (Timestamp) anyObject());
  }


  @Test
  public void testSetFieldsInInsertStatementNulls() throws SQLException {
    PreparedStatement ps = mock(PreparedStatement.class);

    final Set<Integer> called = new HashSet<>();
    doAnswer(new Answer(){
      @Override
      public Object answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        called.add((Integer) args[0]);
        return null;
      }
    }).when(ps).setNull(anyInt(), anyInt());

    Object[] dbFields = new Object[28];
    BulkImportManager.setFieldsInInsertStatement(ps, 1, dbFields, tableSchema.getFields());

    assertEquals(called.size(), 28);
  }

  @Test
  public void testGetValueOfField() throws UnsupportedEncodingException, SQLException {
    ResultSet rs = createResultSetMock();

    BulkImportManager bim = new BulkImportManager(server);

    assertEquals(bim.getValueOfField(rs, "field1", BIGINT), 200L);
    assertEquals(bim.getValueOfField(rs, "field2", VARCHAR), "0-value".getBytes("utf-8"));
    assertEquals(bim.getValueOfField(rs, "field3", TIMESTAMP), new Timestamp(200));
    assertEquals(bim.getValueOfField(rs, "field4", INTEGER), 1200);
    assertEquals(bim.getValueOfField(rs, "field5", SMALLINT), (short)0);
    assertEquals(bim.getValueOfField(rs, "field6", TINYINT), (byte)0);
    assertEquals(bim.getValueOfField(rs, "field7", CHAR), "0-value".getBytes("utf-8"));
    assertEquals(bim.getValueOfField(rs, "field8", NCHAR), "0-value".getBytes("utf-8"));
    assertEquals(bim.getValueOfField(rs, "field9", FLOAT), 0d);
    assertEquals(bim.getValueOfField(rs, "field10", REAL), 0f);
    assertEquals(bim.getValueOfField(rs, "field11", DOUBLE), 0d);
    assertEquals(bim.getValueOfField(rs, "field12", BOOLEAN), true);
    assertEquals(bim.getValueOfField(rs, "field13", BIT), true);
    assertEquals(bim.getValueOfField(rs, "field14", VARCHAR), "0-value".getBytes("utf-8"));
    assertEquals(bim.getValueOfField(rs, "field15", CLOB), "0-value".getBytes("utf-8"));
    assertEquals(bim.getValueOfField(rs, "field16", NCLOB), "0-value".getBytes("utf-8"));
    assertEquals(bim.getValueOfField(rs, "field17", LONGVARCHAR), "0-value".getBytes("utf-8"));
    assertEquals(bim.getValueOfField(rs, "field18", NVARCHAR), "0-value".getBytes("utf-8"));
    assertEquals(bim.getValueOfField(rs, "field19", LONGNVARCHAR), "0-value".getBytes("utf-8"));
    assertEquals(bim.getValueOfField(rs, "field20", LONGVARBINARY), "0-value".getBytes("utf-8"));
    assertEquals(bim.getValueOfField(rs, "field21", VARBINARY), "0-value".getBytes("utf-8"));
    assertEquals(bim.getValueOfField(rs, "field22", BLOB), "0-value".getBytes("utf-8"));
    assertEquals(bim.getValueOfField(rs, "field23", NUMERIC), new BigDecimal(0));
    assertEquals(bim.getValueOfField(rs, "field24", DECIMAL), new BigDecimal(0));
    assertEquals(bim.getValueOfField(rs, "field25", DATE), new Date(1900, 10, 1));
    assertEquals(bim.getValueOfField(rs, "field26", TIME), new Time(1, 0, 0));
    assertEquals(bim.getValueOfField(rs, "field27", TIMESTAMP), new Timestamp(0));
  }

  @Test
  public void testBulkImportOnServer() throws IOException, SQLException, InterruptedException {
    ResultSet rs = createResultSetMock();
    PreparedStatement ps = mock(PreparedStatement.class);

    final Set<Integer> called = new HashSet<>();
    attachSetters(ps, called);
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
    BulkImportManager bim = new BulkImportManager(server) {
      protected Connection getConnection(String dbName, String address, int port) {
        return connection;
      }
      protected Connection getConnection(String connectString) {
        return connection;
      }

      protected Connection getConnection(String connectString, String username, String password) {
        return connection;
      }
    };

    when(server.getDbNames(anyString())).thenReturn(Collections.singletonList("test"));
    String configStr = IOUtils.toString(BulkImportManager.class.getResourceAsStream("/config/config-1-local.yaml"));
    Config config = new Config(configStr);
    when(server.getConfig()).thenReturn(config);

    ComObject cobj = new ComObject(5);
    ComArray keyArray = cobj.putArray(ComObject.Tag.KEYS, ComObject.Type.BYTE_ARRAY_TYPE, 1);
    cobj.put(ComObject.Tag.LOWER_KEY, DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), keys.get(0)));
    keyArray.add(DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), keys.get(0)));


    cobj.put(ComObject.Tag.EXPECTED_COUNT, 1L);
    cobj.put(ComObject.Tag.SHOULD_PROCESS, true);
    cobj.put(ComObject.Tag.DB_NAME, "test");
    cobj.put(ComObject.Tag.TABLE_NAME, "table1");

    ComObject ret = bim.startBulkImportOnServer(cobj, false);

    while (true) {
      ComObject progress = bim.getBulkImportProgressOnServer(cobj, false);
      ComArray array = progress.getArray(ComObject.Tag.STATUSES);
      progress = (ComObject) array.getArray().get(0);
      if (progress.getBoolean(ComObject.Tag.FINISHED)) {
        break;
      }
      String e = progress.getString(ComObject.Tag.EXCEPTION);
      if (e != null) {
        throw new DatabaseException(e);
      }
      Thread.sleep(500);
    }

    assertEquals(called.size(), 28);
  }

  @Test
  public void testBulkImportOnServerCoordinate() throws IOException, SQLException, InterruptedException {
    ResultSet rs = createResultSetMock();
    PreparedStatement ps = mock(PreparedStatement.class);

    final Set<Integer> called = new HashSet<>();
    attachSetters(ps, called);
    final AtomicInteger countReturned = new AtomicInteger();
    when(ps.executeQuery()).thenReturn(rs);
    when(rs.next()).thenAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock) {
        return countReturned.getAndIncrement() == 0;
      }
    });
    final Connection connection = mock(Connection.class);
    when(connection.prepareStatement(anyString())).thenReturn(ps);
    BulkImportManager bim = new BulkImportManager(server) {
      protected Connection getConnection(String dbName, String address, int port) {
        return connection;
      }

      protected Connection getConnection(String connectString) {
        return connection;
      }

      protected Connection getConnection(String connectString, String username, String password) {
        return connection;
      }
    };

    when(server.getDbNames(anyString())).thenReturn(Collections.singletonList("test"));
    String configStr = IOUtils.toString(BulkImportManager.class.getResourceAsStream("/config/config-1-local.yaml"));
    Config config = new Config(configStr);
    when(server.getConfig()).thenReturn(config);

    ComObject cobj = new ComObject(9);
    ComArray keyArray = cobj.putArray(ComObject.Tag.KEYS, ComObject.Type.BYTE_ARRAY_TYPE, 1);
    cobj.put(ComObject.Tag.LOWER_KEY, DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), keys.get(0)));
    keyArray.add(DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), keys.get(0)));

    cobj.put(ComObject.Tag.DB_NAME, "test");
    cobj.put(ComObject.Tag.TABLE_NAME, "table1");
    cobj.put(ComObject.Tag.SHOULD_PROCESS, true);
    cobj.put(ComObject.Tag.DRIVER_NAME, "com.sonicbase.jdbcdriver.Driver");
    cobj.put(ComObject.Tag.USER, "user");
    cobj.put(ComObject.Tag.PASSWORD, "password");
    cobj.put(ComObject.Tag.CONNECT_STRING, "jdbc:sonicbase:localhost:9010");

    ComObject ret = bim.coordinateBulkImportForTable(cobj, false);
//    while (bim.getCountCoordinating() == 0) {
//      Thread.sleep(5L);
//    }
    while (bim.getCountCoordinating() > 0) {
      Thread.sleep(5L);
    }
  }

  @Test
  public void testBulkImportOnServerCoordinateNoIndex() throws IOException, SQLException, InterruptedException {
    tableSchema2 = TestUtils.createTable2();
    common.getTables("test").put(tableSchema2.getName(), tableSchema2);

    ResultSet rs = createResultSetMock();
    PreparedStatement ps = mock(PreparedStatement.class);

    final Set<Integer> called = new HashSet<>();
    attachSetters(ps, called);
    final AtomicInteger countReturned = new AtomicInteger();
    when(ps.executeQuery()).thenReturn(rs);
    when(rs.next()).thenAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock) {
        return countReturned.getAndIncrement() == 0;
      }
    });
    final Connection connection = mock(Connection.class);
    when(connection.prepareStatement(anyString())).thenReturn(ps);
    BulkImportManager bim = new BulkImportManager(server) {
      protected Connection getConnection(String dbName, String address, int port) {
        return connection;
      }

      protected Connection getConnection(String connectString) {
        return connection;
      }

      protected Connection getConnection(String connectString, String username, String password) {
        return connection;
      }
    };

    when(server.getDbNames(anyString())).thenReturn(Collections.singletonList("test"));
    String configStr = IOUtils.toString(BulkImportManager.class.getResourceAsStream("/config/config-1-local.yaml"));
    Config config = new Config(configStr);
    when(server.getConfig()).thenReturn(config);

    ComObject cobj = new ComObject(10);
    ComArray keyArray = cobj.putArray(ComObject.Tag.KEYS, ComObject.Type.BYTE_ARRAY_TYPE, 1);
    cobj.put(ComObject.Tag.LOWER_KEY, DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), keys.get(0)));
    keyArray.add(DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), keys.get(0)));

    cobj.put(ComObject.Tag.DB_NAME, "test");
    cobj.put(ComObject.Tag.TABLE_NAME, "table2");
    cobj.put(ComObject.Tag.SHOULD_PROCESS, true);
    cobj.put(ComObject.Tag.DRIVER_NAME, "com.sonicbase.jdbcdriver.Driver");
    cobj.put(ComObject.Tag.USER, "user");
    cobj.put(ComObject.Tag.PASSWORD, "password");
    cobj.put(ComObject.Tag.EXPECTED_COUNT, 100L);
    cobj.put(ComObject.Tag.CONNECT_STRING, "jdbc:sonicbase:localhost:9010");

    ComObject ret = bim.coordinateBulkImportForTable(cobj, false);
//    while (bim.getCountCoordinating() == 0) {
//      Thread.sleep(5L);
//    }
    while (bim.getCountCoordinating() > 0) {
      Thread.sleep(5L);
    }
  }


  @Test
  public void testBulkImportStatus() throws IOException, SQLException, InterruptedException {
    ResultSet rs = createResultSetMock();
    PreparedStatement ps = mock(PreparedStatement.class);

    final Set<Integer> called = new HashSet<>();
    attachSetters(ps, called);
    final AtomicInteger countReturned = new AtomicInteger();
    when(ps.executeQuery()).thenReturn(rs);
    when(rs.next()).thenAnswer((Answer) invocationOnMock -> countReturned.getAndIncrement() == 0);
    final Connection connection = mock(Connection.class);
    when(connection.prepareStatement(anyString())).thenReturn(ps);
    BulkImportManager bim = new BulkImportManager(server) {
      protected Connection getConnection(String dbName, String address, int port) {
        return connection;
      }

      protected Connection getConnection(String connectString) {
        return connection;
      }

      protected Connection getConnection(String connectString, String username, String password) {
        return connection;
      }
    };

    when(server.getDbNames(anyString())).thenReturn(Collections.singletonList("test"));
    String configStr = IOUtils.toString(BulkImportManager.class.getResourceAsStream("/config/config-1-local.yaml"), "utf-8");
    Config config = new Config(configStr);
    when(server.getConfig()).thenReturn(config);

    ComObject cobj = new ComObject(10);
    ComArray keyArray = cobj.putArray(ComObject.Tag.KEYS, ComObject.Type.BYTE_ARRAY_TYPE, 1);
    cobj.put(ComObject.Tag.LOWER_KEY, DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), keys.get(0)));
    keyArray.add(DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), keys.get(0)));

    cobj.put(ComObject.Tag.DB_NAME, "test");
    cobj.put(ComObject.Tag.TABLE_NAME, "table1");
    cobj.put(ComObject.Tag.SHOULD_PROCESS, true);
    cobj.put(ComObject.Tag.DRIVER_NAME, "com.sonicbase.jdbcdriver.Driver");
    cobj.put(ComObject.Tag.USER, "user");
    cobj.put(ComObject.Tag.PASSWORD, "password");
    cobj.put(ComObject.Tag.CONNECT_STRING, "jdbc:sonicbase:localhost:9010");

    ComObject ret = bim.coordinateBulkImportForTable(cobj, false);
    ret = bim.getBulkImportProgress(cobj, false);

    ComArray array = ret.getArray(ComObject.Tag.PROGRESS_ARRAY);
    ComObject serverObj = (ComObject) array.getArray().get(0);
    assertEquals(serverObj.getString(ComObject.Tag.TABLE_NAME), "test:table1");
    assertEquals((long)serverObj.getLong(ComObject.Tag.EXPECTED_COUNT), 0);
    assertEquals((boolean)serverObj.getBoolean(ComObject.Tag.FINISHED), true);
    assertEquals((long)serverObj.getLong(ComObject.Tag.PRE_PROCESS_EXPECTED_COUNT), 0);
    assertEquals((long)serverObj.getLong(ComObject.Tag.PRE_PROCESS_COUNT_PROCESSED), 0);
    assertEquals((boolean)serverObj.getBoolean(ComObject.Tag.PRE_PROCESS_FINISHED), true);

  }

  @Test
  public void testBulkImportStart() throws IOException, SQLException, InterruptedException {
    ResultSet rs = createResultSetMock();
    PreparedStatement ps = mock(PreparedStatement.class);

    final Set<Integer> called = new HashSet<>();
    attachSetters(ps, called);
    final AtomicInteger countReturned = new AtomicInteger();
    when(ps.executeQuery()).thenReturn(rs);
    when(rs.next()).thenAnswer((Answer) invocationOnMock -> countReturned.getAndIncrement() == 0);
    final Connection connection = mock(Connection.class);
    when(connection.prepareStatement(anyString())).thenReturn(ps);
    BulkImportManager bim = new BulkImportManager(server) {
      protected Connection getConnection(String dbName, String address, int port) {
        return connection;
      }

      protected Connection getConnection(String connectString) {
        return connection;
      }

      protected Connection getConnection(String connectString, String username, String password) {
        return connection;
      }
    };

    when(server.getDbNames(anyString())).thenReturn(Collections.singletonList("test"));
    String configStr = IOUtils.toString(BulkImportManager.class.getResourceAsStream("/config/config-1-local.yaml"), "utf-8");
    Config config = new Config(configStr);
    when(server.getConfig()).thenReturn(config);

    ComObject cobj = new ComObject(9);
    ComArray keyArray = cobj.putArray(ComObject.Tag.KEYS, ComObject.Type.BYTE_ARRAY_TYPE, 1);
    cobj.put(ComObject.Tag.LOWER_KEY, DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), keys.get(0)));
    keyArray.add(DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), keys.get(0)));

    cobj.put(ComObject.Tag.DB_NAME, "test");
    cobj.put(ComObject.Tag.TABLE_NAME, "table1");
    cobj.put(ComObject.Tag.SHOULD_PROCESS, true);
    cobj.put(ComObject.Tag.DRIVER_NAME, "com.sonicbase.jdbcdriver.Driver");
    cobj.put(ComObject.Tag.USER, "user");
    cobj.put(ComObject.Tag.PASSWORD, "password");
    cobj.put(ComObject.Tag.CONNECT_STRING, "jdbc:sonicbase:localhost:9010");

    bim.startBulkImport(cobj, false);

    Thread.sleep(1_000);

    ComObject ret = bim.getBulkImportProgress(cobj, false);

    ComArray array = ret.getArray(ComObject.Tag.PROGRESS_ARRAY);
    ComObject serverObj = (ComObject) array.getArray().get(0);
    assertEquals(serverObj.getString(ComObject.Tag.TABLE_NAME), "test:table1");
    assertEquals((long)serverObj.getLong(ComObject.Tag.EXPECTED_COUNT), 0);
    assertEquals((boolean)serverObj.getBoolean(ComObject.Tag.FINISHED), true);
    assertEquals((long)serverObj.getLong(ComObject.Tag.PRE_PROCESS_EXPECTED_COUNT), 0);
    assertEquals((long)serverObj.getLong(ComObject.Tag.PRE_PROCESS_COUNT_PROCESSED), 0);
    assertEquals((boolean)serverObj.getBoolean(ComObject.Tag.PRE_PROCESS_FINISHED), true);

  }
}
