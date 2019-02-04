package com.sonicbase.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.client.SelectStatementHandler;
import com.sonicbase.common.*;
import com.sonicbase.index.AddressMap;
import com.sonicbase.index.Index;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.query.BinaryExpression;
import com.sonicbase.query.impl.*;
import com.sonicbase.schema.DataType;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.TestUtils;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import org.mockito.stubbing.Answer;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class ReadManagerTest {

  @BeforeClass
  public void beforeClass() {
    System.setProperty("log4j.configuration", "test-log4j.xml");
  }

  @Test
  public void testIndexLookupExpression() throws IOException {
    com.sonicbase.server.DatabaseServer server = mock(com.sonicbase.server.DatabaseServer.class);
    AddressMap addressMap = new AddressMap(server);
    when(server.getAddressMap()).thenReturn(addressMap);
    when(server.getBatchRepartCount()).thenReturn(new AtomicInteger(0));
    Map<Integer, TableSchema> tables = new HashMap<>();
    TableSchema tableSchema = TestUtils.createTable();
    IndexSchema indexSchema = TestUtils.createIndexSchema(tableSchema);

    when(server.getTableSchema(anyString(), anyString(), anyString())).thenReturn(tableSchema);
    when(server.getIndexSchema(anyString(), anyString(), anyString())).thenReturn(indexSchema);

    DatabaseCommon common = TestUtils.createCommon(tableSchema);

    when(server.getCommon()).thenReturn(common);

    Index index = new Index(9010, new HashMap<Long, Boolean>(), tableSchema, indexSchema.getName(), indexSchema.getComparators());
    when(server.getIndex(anyString(), anyString(), anyString())).thenReturn(index);


    byte[][] records = TestUtils.createRecords(common, tableSchema, 10);

    List<Object[]> keys = TestUtils.createKeys(10);

    int i = 0;
    for (Object[] key : keys) {
      Object address = addressMap.toUnsafeFromRecords(new byte[][]{records[i]});
      index.put(key, address);
      i++;
    }

    BinaryExpressionImpl expression = new BinaryExpressionImpl();
    ColumnImpl leftExpression = new ColumnImpl();
    leftExpression.setColumnName("field1");
    leftExpression.setTableName(tableSchema.getName());
    expression.setLeftExpression(leftExpression);
    ConstantImpl rightExpression = new ConstantImpl();
    rightExpression.setValue(300L);
    rightExpression.setSqlType(DataType.Type.BIGINT.getValue());
    expression.setRightExpression(rightExpression);
    expression.setOperator(BinaryExpression.Operator.EQUAL);


    OrderByExpressionImpl orderByExpression = new OrderByExpressionImpl();
    orderByExpression.setAscending(true);
    orderByExpression.setColumnName("field1");

    ComObject cobj = new ComObject(12);
    cobj.put(ComObject.Tag.TABLE_ID, tableSchema.getTableId());
    cobj.put(ComObject.Tag.LEGACY_EXPRESSION, ExpressionImpl.serializeExpression(expression));
    ComArray array = cobj.putArray(ComObject.Tag.ORDER_BY_EXPRESSIONS, ComObject.Type.BYTE_ARRAY_TYPE, 1);
    array.add(orderByExpression.serialize());

    cobj.put(ComObject.Tag.RIGHT_KEY, DatabaseCommon.serializeTypedKey(new Object[]{500L}));
    cobj.put(ComObject.Tag.RIGHT_OPERATOR, BinaryExpression.Operator.LESS.getId());


    cobj.put(ComObject.Tag.CURR_OFFSET,0L);
    cobj.put(ComObject.Tag.LIMIT_LONG, 500L);
    cobj.put(ComObject.Tag.OFFSET_LONG, 0L);

    List<ColumnImpl> columns = new ArrayList<>();
    columns.add(new ColumnImpl(null, null, tableSchema.getName(), "field1", null));

    ComArray columnArray = cobj.putArray(ComObject.Tag.COLUMN_OFFSETS, ComObject.Type.INT_TYPE, columns.size());
    ExpressionImpl.writeColumns(tableSchema, columns, columnArray);


    cobj.put(ComObject.Tag.IS_PROBE, false);
    cobj.put(ComObject.Tag.VIEW_VERSION, (long)1);
    cobj.put(ComObject.Tag.COUNT, 100);
    cobj.put(ComObject.Tag.DB_NAME, "test");
    cobj.put(ComObject.Tag.SCHEMA_VERSION, common.getSchemaVersion());

    ReadManager readManager = new ReadManager(server);

    ComObject retObj = readManager.indexLookupExpression(cobj, false);

    ComArray retArray = retObj.getArray(ComObject.Tag.RECORDS);
    assertEquals(retArray.getArray().get(0), records[1]);
    assertEquals(retArray.getArray().size(), 1);

  }

  @Test
  public void testIndexLookup() throws IOException {
    com.sonicbase.server.DatabaseServer server = mock(com.sonicbase.server.DatabaseServer.class);
    AddressMap addressMap = new AddressMap(server);
    when(server.getAddressMap()).thenReturn(addressMap);
    when(server.getBatchRepartCount()).thenReturn(new AtomicInteger(0));
    Map<Integer, TableSchema> tables = new HashMap<>();
    TableSchema tableSchema = TestUtils.createTable();
    IndexSchema indexSchema = TestUtils.createIndexSchema(tableSchema);

    when(server.getTableSchema(anyString(), anyString(), anyString())).thenReturn(tableSchema);
    when(server.getIndexSchema(anyString(), anyString(), anyString())).thenReturn(indexSchema);

    DatabaseCommon common = TestUtils.createCommon(tableSchema);

    when(server.getCommon()).thenReturn(common);

    Index index = new Index(9010, new HashMap<Long, Boolean>(), tableSchema, indexSchema.getName(), indexSchema.getComparators());
    when(server.getIndex(anyString(), anyString(), anyString())).thenReturn(index);


    byte[][] records = TestUtils.createRecords(common, tableSchema, 10);

    List<Object[]> keys = TestUtils.createKeys(10);

    int i = 0;
    for (Object[] key : keys) {
      Object address = addressMap.toUnsafeFromRecords(new byte[][]{records[i]});
      index.put(key, address);
      i++;
    }

    ComObject cobj = new ComObject(21);

    cobj.put(ComObject.Tag.DB_NAME, "test");
    cobj.put(ComObject.Tag.SCHEMA_VERSION, 1000);
    cobj.put(ComObject.Tag.COUNT, 100);

    cobj.put(ComObject.Tag.IS_EXCPLICITE_TRANS, false);
    cobj.put(ComObject.Tag.IS_COMMITTING, false);
    cobj.put(ComObject.Tag.VIEW_VERSION, (long)1);

    cobj.put(ComObject.Tag.IS_PROBE, false);

    cobj.put(ComObject.Tag.CURR_OFFSET, 0L);
    cobj.put(ComObject.Tag.COUNT_RETURNED, 0L);
    cobj.put(ComObject.Tag.LIMIT_LONG, 500L);
    cobj.put(ComObject.Tag.OFFSET_LONG, 0L);

    cobj.put(ComObject.Tag.TABLE_ID, tableSchema.getTableId());
    cobj.put(ComObject.Tag.INDEX_ID, indexSchema.getIndexId());
    cobj.put(ComObject.Tag.FORCE_SELECT_ON_SERVER, false);

    cobj.put(ComObject.Tag.EVALUATE_EXPRESSION, false);

    OrderByExpressionImpl orderByExpression = new OrderByExpressionImpl();
    orderByExpression.setAscending(true);
    orderByExpression.setColumnName("field1");
    ComArray array = cobj.putArray(ComObject.Tag.ORDER_BY_EXPRESSIONS, ComObject.Type.BYTE_ARRAY_TYPE, 1);
    byte[] bytes = orderByExpression.serialize();
    array.add(bytes);

    cobj.put(ComObject.Tag.ORIGINAL_LEFT_KEY, DatabaseCommon.serializeTypedKey(new Object[]{500L}));
    cobj.put(ComObject.Tag.LEFT_OPERATOR, BinaryExpression.Operator.GREATER.getId());
    cobj.put(ComObject.Tag.ORIGINAL_RIGHT_KEY, DatabaseCommon.serializeTypedKey(new Object[]{700L}));
    cobj.put(ComObject.Tag.RIGHT_OPERATOR, BinaryExpression.Operator.LESS.getId());


    List<ColumnImpl> columns = new ArrayList<>();
    columns.add(new ColumnImpl(null, null, tableSchema.getName(), "field1", null));
    ComArray columnArray = cobj.putArray(ComObject.Tag.COLUMN_OFFSETS, ComObject.Type.INT_TYPE, columns.size());
    ExpressionImpl.writeColumns(tableSchema, columns, columnArray);


    ReadManager readManager = new ReadManager(server);


    Map<String, DatabaseServer.SimpleStats> stats = DatabaseServer.initStats();
    when(server.getStats()).thenReturn(stats);

    ComObject retObj = readManager.indexLookup(cobj, false);

    ComArray retArray = retObj.getArray(ComObject.Tag.RECORDS);
    assertEquals(retArray.getArray().get(0), records[4]);
    assertEquals(retArray.getArray().size(), 1);
  }

  @Test
  public void testCountRecords() throws UnsupportedEncodingException {
    com.sonicbase.server.DatabaseServer server = mock(com.sonicbase.server.DatabaseServer.class);
    AddressMap addressMap = new AddressMap(server);
    when(server.getAddressMap()).thenReturn(addressMap);
    when(server.getBatchRepartCount()).thenReturn(new AtomicInteger(0));
    Map<Integer, TableSchema> tables = new HashMap<>();
    TableSchema tableSchema = TestUtils.createTable();
    IndexSchema indexSchema = TestUtils.createIndexSchema(tableSchema);

    when(server.getTableSchema(anyString(), anyString(), anyString())).thenReturn(tableSchema);
    when(server.getIndexSchema(anyString(), anyString(), anyString())).thenReturn(indexSchema);

    DatabaseCommon common = TestUtils.createCommon(tableSchema);

    when(server.getCommon()).thenReturn(common);

    Index index = new Index(9010, new HashMap<Long, Boolean>(), tableSchema, indexSchema.getName(), indexSchema.getComparators());
    when(server.getIndex(anyString(), anyString(), anyString())).thenReturn(index);


    byte[][] records = TestUtils.createRecords(common, tableSchema, 10);

    List<Object[]> keys = TestUtils.createKeys(10);

    int i = 0;
    for (Object[] key : keys) {
      Object address = addressMap.toUnsafeFromRecords(new byte[][]{records[i]});
      index.put(key, address);
      i++;
    }

    BinaryExpressionImpl expression = new BinaryExpressionImpl();
    ColumnImpl leftExpression = new ColumnImpl();
    leftExpression.setColumnName("field1");
    leftExpression.setTableName(tableSchema.getName());
    expression.setLeftExpression(leftExpression);
    ConstantImpl rightExpression = new ConstantImpl();
    rightExpression.setValue(300L);
    rightExpression.setSqlType(DataType.Type.BIGINT.getValue());
    expression.setRightExpression(rightExpression);
    expression.setOperator(BinaryExpression.Operator.EQUAL);


    ComObject cobj = new ComObject(7);
    cobj.put(ComObject.Tag.SERIALIZATION_VERSION, DatabaseClient.SERIALIZATION_VERSION);

    cobj.put(ComObject.Tag.LEGACY_EXPRESSION, ExpressionImpl.serializeExpression(expression));


    cobj.put(ComObject.Tag.COUNT_TABLE_NAME, tableSchema.getName());

    cobj.put(ComObject.Tag.COUNT_COLUMN, "field1");

    cobj.put(ComObject.Tag.DB_NAME, "test");
    cobj.put(ComObject.Tag.SCHEMA_VERSION, 1000);
    cobj.put(ComObject.Tag.TABLE_NAME, tableSchema.getName());

    ReadManager readManager = new ReadManager(server);

    ComObject retObj = readManager.countRecords(cobj, false);

    assertEquals((long)retObj.getLong(ComObject.Tag.COUNT_LONG), 1L);
  }

  @Test
  public void testBatchIndexLookup() throws IOException {
    com.sonicbase.server.DatabaseServer server = mock(com.sonicbase.server.DatabaseServer.class);
    AddressMap addressMap = new AddressMap(server);
    when(server.getAddressMap()).thenReturn(addressMap);
    when(server.getBatchRepartCount()).thenReturn(new AtomicInteger(0));
    Map<Integer, TableSchema> tables = new HashMap<>();
    TableSchema tableSchema = TestUtils.createTable();
    IndexSchema indexSchema = TestUtils.createIndexSchema(tableSchema);

    when(server.getTableSchema(anyString(), anyString(), anyString())).thenReturn(tableSchema);
    when(server.getIndexSchema(anyString(), anyString(), anyString())).thenReturn(indexSchema);

    DatabaseCommon common = TestUtils.createCommon(tableSchema);

    when(server.getCommon()).thenReturn(common);

    Index index = new Index(9010, new HashMap<Long, Boolean>(), tableSchema, indexSchema.getName(), indexSchema.getComparators());
    when(server.getIndex(anyString(), anyString(), anyString())).thenReturn(index);


    byte[][] records = TestUtils.createRecords(common, tableSchema, 10);

    List<Object[]> keys = TestUtils.createKeys(10);

    int i = 0;
    for (Object[] key : keys) {
      Object address = addressMap.toUnsafeFromRecords(new byte[][]{records[i]});
      index.put(key, address);
      i++;
    }
    ComObject cobj = new ComObject(9);
    cobj.put(ComObject.Tag.TABLE_NAME, tableSchema.getName());
    cobj.put(ComObject.Tag.INDEX_NAME, indexSchema.getName());
    cobj.put(ComObject.Tag.LEFT_OPERATOR, BinaryExpression.Operator.EQUAL.getId());

    List<ColumnImpl> columns = new ArrayList<>();
    columns.add(new ColumnImpl(null, null, tableSchema.getName(), "field1", null));
    ComArray columnArray = cobj.putArray(ComObject.Tag.COLUMN_OFFSETS, ComObject.Type.INT_TYPE, columns.size());
    ExpressionImpl.writeColumns(tableSchema, columns, columnArray);

    cobj.put(ComObject.Tag.SINGLE_VALUE, false);


    ComArray keyArray = cobj.putArray(ComObject.Tag.KEYS, ComObject.Type.OBJECT_TYPE, keys.size());
    int k = 0; ///(offset * srcValues.size() / threadCount);
    for (; k < keys.size(); k++) {
      ComObject key = new ComObject(2);
      keyArray.add(key);
      key.put(ComObject.Tag.OFFSET, k);
      key.put(ComObject.Tag.KEY_BYTES, DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), keys.get(k)));
    }

    cobj.put(ComObject.Tag.DB_NAME, "test");
    cobj.put(ComObject.Tag.SCHEMA_VERSION, 1000);
    cobj.put(ComObject.Tag.COUNT, 100);


    ReadManager readManager = new ReadManager(server);

    ComObject retObj = readManager.batchIndexLookup(cobj, false);
    ComArray retArray = retObj.getArray(ComObject.Tag.RET_KEYS);
    for (int j = 0; j < keys.size(); j++) {
      ComObject obj = (ComObject) retArray.getArray().get(j);
      ComArray recordArray = obj.getArray(ComObject.Tag.RECORDS);
      assertEquals((byte[])recordArray.getArray().get(0), records[j]);
    }
    assertEquals(retArray.getArray().size(), keys.size());
  }

  @Test
  public void testServerSelect() throws IOException {
    com.sonicbase.server.DatabaseServer server = mock(com.sonicbase.server.DatabaseServer.class);
    AddressMap addressMap = new AddressMap(server);
    when(server.getAddressMap()).thenReturn(addressMap);
    when(server.getBatchRepartCount()).thenReturn(new AtomicInteger(0));
    Map<Integer, TableSchema> tables = new HashMap<>();
    TableSchema tableSchema = TestUtils.createTable();
    IndexSchema indexSchema = TestUtils.createIndexSchema(tableSchema);

    when(server.getTableSchema(anyString(), anyString(), anyString())).thenReturn(tableSchema);
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
    ServersConfig serversConfig = new ServersConfig("test", (ArrayNode) ((ObjectNode)node).withArray("shards"), true, true);
    common.setServersConfig(serversConfig);
    when(server.getCommon()).thenReturn(common);

    Index index = new Index(9010, new HashMap<Long, Boolean>(), tableSchema, indexSchema.getName(), indexSchema.getComparators());
    when(server.getIndex(anyString(), anyString(), anyString())).thenReturn(index);


    byte[][] records = TestUtils.createRecords(common, tableSchema, 10);

    List<Object[]> keys = TestUtils.createKeys(10);

    int i = 0;
    for (Object[] key : keys) {
      Object address = addressMap.toUnsafeFromRecords(new byte[][]{records[i]});
      index.put(key, address);
      i++;
    }

    BinaryExpressionImpl expression = new BinaryExpressionImpl();
    ColumnImpl leftExpression = new ColumnImpl();
    leftExpression.setColumnName("field1");
    leftExpression.setTableName(tableSchema.getName());
    expression.setLeftExpression(leftExpression);
    ConstantImpl rightExpression = new ConstantImpl();
    rightExpression.setValue(300L);
    rightExpression.setSqlType(DataType.Type.BIGINT.getValue());
    expression.setRightExpression(rightExpression);
    expression.setOperator(BinaryExpression.Operator.EQUAL);

    DatabaseClient client = mock(DatabaseClient.class);
    when(client.getCommon()).thenReturn(common);


    ComObject retObj = new ComObject(3);

    ComArray array = retObj.putArray(ComObject.Tag.RECORDS, ComObject.Type.BYTE_ARRAY_TYPE, records.length);
    for (int j = 0; j < records.length; j++) {
      array.add(records[j]);
    }
    retObj.put(ComObject.Tag.CURR_OFFSET, (long)records.length);
    retObj.put(ComObject.Tag.COUNT_RETURNED, (long)records.length);

    when(client.send(   anyString(), anyInt(), anyInt(), (ComObject) anyObject(), eq(DatabaseClient.Replica.DEF))).thenReturn(
        retObj.serialize()
    );
    SelectStatementImpl select = new SelectStatementImpl(client);
    when(server.getDatabaseClient()).thenReturn(client);
    select.setExpression(expression);
    select.setFromTable(tableSchema.getName());
    select.setPageSize(100);
    select.setTableNames(new String[]{tableSchema.getName()});
    List<OrderByExpressionImpl> orderByExpressions = new ArrayList<>();
    OrderByExpressionImpl orderByExpression = new OrderByExpressionImpl();
    orderByExpression.setAscending(true);
    orderByExpression.setColumnName("field1");
    orderByExpressions.add(orderByExpression);

    select.setOrderByExpressions(orderByExpressions);
    ComObject cobj = new ComObject(6);
    cobj.put(ComObject.Tag.LEGACY_SELECT_STATEMENT, select.serialize());
    cobj.put(ComObject.Tag.SCHEMA_VERSION, 1000);
    cobj.put(ComObject.Tag.COUNT, 100);
    cobj.put(ComObject.Tag.DB_NAME, "test");
    cobj.put(ComObject.Tag.CURR_OFFSET, 0L);
    cobj.put(ComObject.Tag.COUNT_RETURNED, 0L);

    ReadManager readManager = new ReadManager(server);

    retObj = readManager.serverSelect(cobj,  false);
    ComArray retArray = retObj.getArray(ComObject.Tag.TABLE_RECORDS);
    for (int j = 0; j < records.length; j++) {
      ComArray recordArray = (ComArray) retArray.getArray().get(j);
      assertEquals(recordArray.getArray().get(0), records[j]);
    }
    assertEquals(retArray.getArray().size(), records.length);
  }

  @Test
  public void testSetServerSelect() throws JSQLParserException, IOException {

    com.sonicbase.server.DatabaseServer server = mock(DatabaseServer.class);
    AddressMap addressMap = new AddressMap(server);
    when(server.getAddressMap()).thenReturn(addressMap);
    when(server.getBatchRepartCount()).thenReturn(new AtomicInteger(0));
    Map<Integer, TableSchema> tables = new HashMap<>();
    TableSchema tableSchema = TestUtils.createTable();
    IndexSchema indexSchema = TestUtils.createIndexSchema(tableSchema);

    when(server.getTableSchema(anyString(), anyString(), anyString())).thenReturn(tableSchema);
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
    ServersConfig serversConfig = new ServersConfig("test", (ArrayNode) ((ObjectNode)node).withArray("shards"), true, true);
    common.setServersConfig(serversConfig);
    when(server.getCommon()).thenReturn(common);

    Index index = new Index(9010, new HashMap<Long, Boolean>(), tableSchema, indexSchema.getName(), indexSchema.getComparators());
    when(server.getIndex(anyString(), anyString(), anyString())).thenReturn(index);


    byte[][] records = TestUtils.createRecords(common, tableSchema, 2000);

    List<Object[]> keys = TestUtils.createKeys(2000);

    int k = 0;
    for (Object[] key : keys) {
      Object address = addressMap.toUnsafeFromRecords(new byte[][]{records[k]});
      index.put(key, address);
      k++;
    }

    DatabaseClient client = mock(DatabaseClient.class);
    when(client.getCommon()).thenReturn(common);
    when(server.getClient()).thenReturn(client);

    ComObject retObj = new ComObject(3);
    ComArray array = retObj.putArray(ComObject.Tag.RECORDS, ComObject.Type.BYTE_ARRAY_TYPE, records.length);
    for (int j = 0; j < records.length; j++) {
      array.add(records[j]);
    }
    retObj.put(ComObject.Tag.CURR_OFFSET, (long)records.length);
    retObj.put(ComObject.Tag.COUNT_RETURNED, (long)records.length);

    String sql = "select field1 from table1 where field1 < 700 intersect select field1 from table1 where field1 > 500";
    CCJSqlParserManager parser = new CCJSqlParserManager();
    Statement statement = parser.parse(new StringReader(sql));

    Select selectNode = (Select) statement;

    SelectBody selectBody = selectNode.getSelectBody();
    SetOperationList opList = (SetOperationList) selectBody;
    String[] tableNames = new String[opList.getSelects().size()];
    SelectStatementImpl[] statements = new SelectStatementImpl[opList.getSelects().size()];
    for (int i = 0; i < opList.getSelects().size(); i++) {
      SelectBody innerBody = opList.getSelects().get(i);
      SelectStatementImpl selectStatement = SelectStatementHandler.parseSelectStatement(client, new ParameterHandler(), (PlainSelect) innerBody, new AtomicInteger(0));
      tableNames[i] = selectStatement.getFromTable();
      statements[i] = selectStatement;
    }
    String[] operations = new String[opList.getOperations().size()];
    for (int i = 0; i < operations.length; i++) {
      operations[i] = opList.getOperations().get(i).toString();
    }
    List<OrderByElement> orderByElements = opList.getOrderByElements();
    OrderByExpressionImpl[] orderBy = null;
    if (orderByElements != null) {
      orderBy = new OrderByExpressionImpl[orderByElements.size()];
      for (int i = 0; i < orderBy.length; i++) {
        OrderByElement element = orderByElements.get(i);
        String tableName = ((Column) element.getExpression()).getTable().getName();
        String columnName = ((Column) element.getExpression()).getColumnName();
        orderBy[i] = new OrderByExpressionImpl(tableName == null ? null : tableName.toLowerCase(),
            columnName.toLowerCase(), element.isAsc());
      }
    }
    SelectStatementHandler.SetOperation setOperation = new SelectStatementHandler.SetOperation();
    setOperation.setSelectStatements(statements);
    setOperation.setOperations(operations);
    setOperation.setOrderBy(orderBy);

    ComObject cobj = new ComObject(10);
    array = cobj.putArray(ComObject.Tag.SELECT_STATEMENTS, ComObject.Type.BYTE_ARRAY_TYPE, setOperation.getSelectStatements().length);
    for (int i = 0; i < setOperation.getSelectStatements().length; i++) {
      setOperation.getSelectStatements()[i].setTableNames(new String[]{setOperation.getSelectStatements()[i].getFromTable()});
      array.add(setOperation.getSelectStatements()[i].serialize());
    }
    if (setOperation.getOrderBy() != null) {
      ComArray orderByArray = cobj.putArray(ComObject.Tag.ORDER_BY_EXPRESSIONS, ComObject.Type.BYTE_ARRAY_TYPE, setOperation.getOrderBy().length);
      for (int i = 0; i < setOperation.getOrderBy().length; i++) {
        orderByArray.add(setOperation.getOrderBy()[i].serialize());
      }
    }
    ComArray tablesArray = cobj.putArray(ComObject.Tag.TABLES, ComObject.Type.STRING_TYPE, tableNames.length);
    for (int i = 0; i < tableNames.length; i++) {
      tablesArray.add(tableNames[i]);
    }
    ComArray strArray = cobj.putArray(ComObject.Tag.OPERATIONS, ComObject.Type.STRING_TYPE, setOperation.getOperations().length);
    for (int i = 0; i < setOperation.getOperations().length; i++) {
      strArray.add(setOperation.getOperations()[i]);
    }
    cobj.put(ComObject.Tag.SCHEMA_VERSION, client.getCommon().getSchemaVersion());
    cobj.put(ComObject.Tag.COUNT, DatabaseClient.SELECT_PAGE_SIZE);
    cobj.put(ComObject.Tag.METHOD, "ReadManager:serverSetSelect");
    cobj.put(ComObject.Tag.DB_NAME, "test");
    cobj.put(ComObject.Tag.SERVER_SELECT_PAGE_NUMBER, setOperation.getServerSelectPageNumber());
    cobj.put(ComObject.Tag.RESULT_SET_ID, setOperation.getResultSetId());

    ReadManager readManager = new ReadManager(server);

    Map<String, DatabaseServer.SimpleStats> stats = DatabaseServer.initStats();
    when(server.getStats()).thenReturn(stats);

    when(client.send(   eq("ReadManager:indexLookup"), anyInt(), anyInt(), (ComObject) anyObject(), eq(DatabaseClient.Replica.DEF))).thenAnswer(
        (Answer) invocationOnMock -> {
          Object[] args = invocationOnMock.getArguments();
          //retObj.serialize()
          return readManager.indexLookup((ComObject)args[3], false).serialize();
        }
    );

    retObj = readManager.serverSetSelect(cobj, false);

    ComArray retArray = retObj.getArray(ComObject.Tag.TABLE_RECORDS);

    ComArray recordArray = (ComArray) retArray.getArray().get(0);
    Record retRecord = new Record("test", common, (byte[])recordArray.getArray().get(0));
    Record origRecord = new Record("test", common, records[4]);
    assertEquals(retRecord.getFields()[1], origRecord.getFields()[1]);
    assertEquals(retArray.getArray().size(), 1);

  }
}
