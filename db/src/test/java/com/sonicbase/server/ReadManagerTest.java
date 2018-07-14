/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.client.SelectStatementHandler;
import com.sonicbase.common.ComArray;
import com.sonicbase.common.ComObject;
import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.common.ServersConfig;
import com.sonicbase.index.AddressMap;
import com.sonicbase.index.Index;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.query.BinaryExpression;
import com.sonicbase.query.impl.*;
import com.sonicbase.schema.DataType;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
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

  @Test
  public void testIndexLookupExpression() throws IOException {
    com.sonicbase.server.DatabaseServer server = mock(com.sonicbase.server.DatabaseServer.class);
    AddressMap addressMap = new AddressMap(server);
    when(server.getAddressMap()).thenReturn(addressMap);
    when(server.getBatchRepartCount()).thenReturn(new AtomicInteger(0));
    Map<Integer, TableSchema> tables = new HashMap<>();
    TableSchema tableSchema = IndexLookupTest.createTable();
    IndexSchema indexSchema = IndexLookupTest.createIndexSchema(tableSchema);

    when(server.getIndexSchema(anyString(), anyString(), anyString())).thenReturn(indexSchema);

    DatabaseCommon common = IndexLookupTest.createCommon(tableSchema);

    when(server.getCommon()).thenReturn(common);

    Index index = new Index(tableSchema, indexSchema.getName(), indexSchema.getComparators());
    when(server.getIndex(anyString(), anyString(), anyString())).thenReturn(index);


    byte[][] records = IndexLookupTest.createRecords(common, tableSchema, 10);

    List<Object[]> keys = IndexLookupTest.createKeys(10);

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
    expression.setOperator(BinaryExpression.Operator.equal);


    OrderByExpressionImpl orderByExpression = new OrderByExpressionImpl();
    orderByExpression.setAscending(true);
    orderByExpression.setColumnName("field1");

    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.tableId, tableSchema.getTableId());
    cobj.put(ComObject.Tag.legacyExpression, ExpressionImpl.serializeExpression(expression));
    ComArray array = cobj.putArray(ComObject.Tag.orderByExpressions, ComObject.Type.byteArrayType);
    array.add(orderByExpression.serialize());

    cobj.put(ComObject.Tag.rightKey, DatabaseCommon.serializeTypedKey(new Object[]{500L}));
    cobj.put(ComObject.Tag.rightOperator, BinaryExpression.Operator.less.getId());


    cobj.put(ComObject.Tag.currOffset,0L);
    cobj.put(ComObject.Tag.limitLong, 500L);
    cobj.put(ComObject.Tag.offsetLong, 0L);

    List<ColumnImpl> columns = new ArrayList<>();
    columns.add(new ColumnImpl(null, null, tableSchema.getName(), "field1", null));

    ComArray columnArray = cobj.putArray(ComObject.Tag.columnOffsets, ComObject.Type.intType);
    ExpressionImpl.writeColumns(tableSchema, columns, columnArray);


    cobj.put(ComObject.Tag.isProbe, false);
    cobj.put(ComObject.Tag.viewVersion, (long)1);
    cobj.put(ComObject.Tag.count, 100);
    cobj.put(ComObject.Tag.dbName, "test");
    cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());

    ReadManager readManager = new ReadManager(server);

    ComObject retObj = readManager.indexLookupExpression(cobj, false);

    ComArray retArray = retObj.getArray(ComObject.Tag.records);
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
    TableSchema tableSchema = IndexLookupTest.createTable();
    IndexSchema indexSchema = IndexLookupTest.createIndexSchema(tableSchema);

    when(server.getIndexSchema(anyString(), anyString(), anyString())).thenReturn(indexSchema);

    DatabaseCommon common = IndexLookupTest.createCommon(tableSchema);

    when(server.getCommon()).thenReturn(common);

    Index index = new Index(tableSchema, indexSchema.getName(), indexSchema.getComparators());
    when(server.getIndex(anyString(), anyString(), anyString())).thenReturn(index);


    byte[][] records = IndexLookupTest.createRecords(common, tableSchema, 10);

    List<Object[]> keys = IndexLookupTest.createKeys(10);

    int i = 0;
    for (Object[] key : keys) {
      Object address = addressMap.toUnsafeFromRecords(new byte[][]{records[i]});
      index.put(key, address);
      i++;
    }

    ComObject cobj = new ComObject();

    cobj.put(ComObject.Tag.dbName, "test");
    cobj.put(ComObject.Tag.schemaVersion, 1000);
    cobj.put(ComObject.Tag.count, 100);

    cobj.put(ComObject.Tag.isExcpliciteTrans, false);
    cobj.put(ComObject.Tag.isCommitting, false);
    cobj.put(ComObject.Tag.viewVersion, (long)1);

    cobj.put(ComObject.Tag.isProbe, false);

    cobj.put(ComObject.Tag.currOffset, 0L);
    cobj.put(ComObject.Tag.countReturned, 0L);
    cobj.put(ComObject.Tag.limitLong, 500L);
    cobj.put(ComObject.Tag.offsetLong, 0L);

    cobj.put(ComObject.Tag.tableId, tableSchema.getTableId());
    cobj.put(ComObject.Tag.indexId, indexSchema.getIndexId());
    cobj.put(ComObject.Tag.forceSelectOnServer, false);

    cobj.put(ComObject.Tag.evaluateExpression, false);

    OrderByExpressionImpl orderByExpression = new OrderByExpressionImpl();
    orderByExpression.setAscending(true);
    orderByExpression.setColumnName("field1");
    ComArray array = cobj.putArray(ComObject.Tag.orderByExpressions, ComObject.Type.byteArrayType);
    byte[] bytes = orderByExpression.serialize();
    array.add(bytes);

    cobj.put(ComObject.Tag.originalLeftKey, DatabaseCommon.serializeTypedKey(new Object[]{500L}));
    cobj.put(ComObject.Tag.leftOperator, BinaryExpression.Operator.greater.getId());
    cobj.put(ComObject.Tag.originalRightKey, DatabaseCommon.serializeTypedKey(new Object[]{700L}));
    cobj.put(ComObject.Tag.rightOperator, BinaryExpression.Operator.less.getId());


    List<ColumnImpl> columns = new ArrayList<>();
    columns.add(new ColumnImpl(null, null, tableSchema.getName(), "field1", null));
    ComArray columnArray = cobj.putArray(ComObject.Tag.columnOffsets, ComObject.Type.intType);
    ExpressionImpl.writeColumns(tableSchema, columns, columnArray);

    ReadManager readManager = new ReadManager(server);

    ComObject retObj = readManager.indexLookup(cobj, false);

    ComArray retArray = retObj.getArray(ComObject.Tag.records);
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
    TableSchema tableSchema = IndexLookupTest.createTable();
    IndexSchema indexSchema = IndexLookupTest.createIndexSchema(tableSchema);

    when(server.getIndexSchema(anyString(), anyString(), anyString())).thenReturn(indexSchema);

    DatabaseCommon common = IndexLookupTest.createCommon(tableSchema);

    when(server.getCommon()).thenReturn(common);

    Index index = new Index(tableSchema, indexSchema.getName(), indexSchema.getComparators());
    when(server.getIndex(anyString(), anyString(), anyString())).thenReturn(index);


    byte[][] records = IndexLookupTest.createRecords(common, tableSchema, 10);

    List<Object[]> keys = IndexLookupTest.createKeys(10);

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
    expression.setOperator(BinaryExpression.Operator.equal);


    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.serializationVersion, DatabaseClient.SERIALIZATION_VERSION);

    cobj.put(ComObject.Tag.legacyExpression, ExpressionImpl.serializeExpression(expression));


    cobj.put(ComObject.Tag.countTableName, tableSchema.getName());

    cobj.put(ComObject.Tag.countColumn, "field1");

    cobj.put(ComObject.Tag.dbName, "test");
    cobj.put(ComObject.Tag.schemaVersion, 1000);
    cobj.put(ComObject.Tag.tableName, tableSchema.getName());

    ReadManager readManager = new ReadManager(server);

    ComObject retObj = readManager.countRecords(cobj, false);

    assertEquals((long)retObj.getLong(ComObject.Tag.countLong), 1L);
  }

  @Test
  public void testBatchIndexLookup() throws IOException {
    com.sonicbase.server.DatabaseServer server = mock(com.sonicbase.server.DatabaseServer.class);
    AddressMap addressMap = new AddressMap(server);
    when(server.getAddressMap()).thenReturn(addressMap);
    when(server.getBatchRepartCount()).thenReturn(new AtomicInteger(0));
    Map<Integer, TableSchema> tables = new HashMap<>();
    TableSchema tableSchema = IndexLookupTest.createTable();
    IndexSchema indexSchema = IndexLookupTest.createIndexSchema(tableSchema);

    when(server.getIndexSchema(anyString(), anyString(), anyString())).thenReturn(indexSchema);

    DatabaseCommon common = IndexLookupTest.createCommon(tableSchema);

    when(server.getCommon()).thenReturn(common);

    Index index = new Index(tableSchema, indexSchema.getName(), indexSchema.getComparators());
    when(server.getIndex(anyString(), anyString(), anyString())).thenReturn(index);


    byte[][] records = IndexLookupTest.createRecords(common, tableSchema, 10);

    List<Object[]> keys = IndexLookupTest.createKeys(10);

    int i = 0;
    for (Object[] key : keys) {
      Object address = addressMap.toUnsafeFromRecords(new byte[][]{records[i]});
      index.put(key, address);
      i++;
    }
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.tableName, tableSchema.getName());
    cobj.put(ComObject.Tag.indexName, indexSchema.getName());
    cobj.put(ComObject.Tag.leftOperator, BinaryExpression.Operator.equal.getId());

    List<ColumnImpl> columns = new ArrayList<>();
    columns.add(new ColumnImpl(null, null, tableSchema.getName(), "field1", null));
    ComArray columnArray = cobj.putArray(ComObject.Tag.columnOffsets, ComObject.Type.intType);
    ExpressionImpl.writeColumns(tableSchema, columns, columnArray);

    cobj.put(ComObject.Tag.singleValue, false);


    ComArray keyArray = cobj.putArray(ComObject.Tag.keys, ComObject.Type.objectType);
    int k = 0; ///(offset * srcValues.size() / threadCount);
    for (; k < keys.size(); k++) {
      ComObject key = new ComObject();
      keyArray.add(key);
      key.put(ComObject.Tag.offset, k);
      key.put(ComObject.Tag.keyBytes, DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), keys.get(k)));
    }

    cobj.put(ComObject.Tag.dbName, "test");
    cobj.put(ComObject.Tag.schemaVersion, 1000);
    cobj.put(ComObject.Tag.count, 100);


    ReadManager readManager = new ReadManager(server);

    ComObject retObj = readManager.batchIndexLookup(cobj, false);
    ComArray retArray = retObj.getArray(ComObject.Tag.retKeys);
    for (int j = 0; j < keys.size(); j++) {
      ComObject obj = (ComObject) retArray.getArray().get(j);
      ComArray recordArray = obj.getArray(ComObject.Tag.records);
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
    TableSchema tableSchema = IndexLookupTest.createTable();
    IndexSchema indexSchema = IndexLookupTest.createIndexSchema(tableSchema);

    when(server.getIndexSchema(anyString(), anyString(), anyString())).thenReturn(indexSchema);

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
    common.setServersConfig(serversConfig);
    when(server.getCommon()).thenReturn(common);

    Index index = new Index(tableSchema, indexSchema.getName(), indexSchema.getComparators());
    when(server.getIndex(anyString(), anyString(), anyString())).thenReturn(index);


    byte[][] records = IndexLookupTest.createRecords(common, tableSchema, 10);

    List<Object[]> keys = IndexLookupTest.createKeys(10);

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
    expression.setOperator(BinaryExpression.Operator.equal);

    DatabaseClient client = mock(DatabaseClient.class);
    when(client.getCommon()).thenReturn(common);


    ComObject retObj = new ComObject();

    ComArray array = retObj.putArray(ComObject.Tag.records, ComObject.Type.byteArrayType);
    for (int j = 0; j < records.length; j++) {
      array.add(records[j]);
    }
    retObj.put(ComObject.Tag.currOffset, (long)records.length);
    retObj.put(ComObject.Tag.countReturned, (long)records.length);

    when(client.send(   anyString(), anyInt(), anyInt(), (ComObject) anyObject(), eq(DatabaseClient.Replica.def))).thenReturn(
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
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.legacySelectStatement, select.serialize());
    cobj.put(ComObject.Tag.schemaVersion, 1000);
    cobj.put(ComObject.Tag.count, 100);
    cobj.put(ComObject.Tag.dbName, "test");
    cobj.put(ComObject.Tag.currOffset, 0L);
    cobj.put(ComObject.Tag.countReturned, 0L);

    ReadManager readManager = new ReadManager(server);

    retObj = readManager.serverSelect(cobj,  false);
    ComArray retArray = retObj.getArray(ComObject.Tag.tableRecords);
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
    TableSchema tableSchema = IndexLookupTest.createTable();
    IndexSchema indexSchema = IndexLookupTest.createIndexSchema(tableSchema);

    when(server.getIndexSchema(anyString(), anyString(), anyString())).thenReturn(indexSchema);

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
    common.setServersConfig(serversConfig);
    when(server.getCommon()).thenReturn(common);

    Index index = new Index(tableSchema, indexSchema.getName(), indexSchema.getComparators());
    when(server.getIndex(anyString(), anyString(), anyString())).thenReturn(index);


    byte[][] records = IndexLookupTest.createRecords(common, tableSchema, 2000);

    List<Object[]> keys = IndexLookupTest.createKeys(2000);

    int k = 0;
    for (Object[] key : keys) {
      Object address = addressMap.toUnsafeFromRecords(new byte[][]{records[k]});
      index.put(key, address);
      k++;
    }

    DatabaseClient client = mock(DatabaseClient.class);
    when(client.getCommon()).thenReturn(common);
    when(server.getClient()).thenReturn(client);

    ComObject retObj = new ComObject();
    ComArray array = retObj.putArray(ComObject.Tag.records, ComObject.Type.byteArrayType);
    for (int j = 0; j < records.length; j++) {
      array.add(records[j]);
    }
    retObj.put(ComObject.Tag.currOffset, (long)records.length);
    retObj.put(ComObject.Tag.countReturned, (long)records.length);

    when(client.send(   anyString(), anyInt(), anyInt(), (ComObject) anyObject(), eq(DatabaseClient.Replica.def))).thenReturn(
        retObj.serialize()
    );

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
    setOperation.selectStatements = statements;
    setOperation.operations = operations;
    setOperation.orderBy = orderBy;

    ComObject cobj = new ComObject();
    array = cobj.putArray(ComObject.Tag.selectStatements, ComObject.Type.byteArrayType);
    for (int i = 0; i < setOperation.selectStatements.length; i++) {
      setOperation.selectStatements[i].setTableNames(new String[]{setOperation.selectStatements[i].getFromTable()});
      array.add(setOperation.selectStatements[i].serialize());
    }
    if (setOperation.orderBy != null) {
      ComArray orderByArray = cobj.putArray(ComObject.Tag.orderByExpressions, ComObject.Type.byteArrayType);
      for (int i = 0; i < setOperation.orderBy.length; i++) {
        orderByArray.add(setOperation.orderBy[i].serialize());
      }
    }
    ComArray tablesArray = cobj.putArray(ComObject.Tag.tables, ComObject.Type.stringType);
    for (int i = 0; i < tableNames.length; i++) {
      tablesArray.add(tableNames[i]);
    }
    ComArray strArray = cobj.putArray(ComObject.Tag.operations, ComObject.Type.stringType);
    for (int i = 0; i < setOperation.operations.length; i++) {
      strArray.add(setOperation.operations[i]);
    }
    cobj.put(ComObject.Tag.schemaVersion, client.getCommon().getSchemaVersion());
    cobj.put(ComObject.Tag.count, DatabaseClient.SELECT_PAGE_SIZE);
    cobj.put(ComObject.Tag.method, "ReadManager:serverSetSelect");
    cobj.put(ComObject.Tag.dbName, "test");
    cobj.put(ComObject.Tag.serverSelectPageNumber, setOperation.serverSelectPageNumber);
    cobj.put(ComObject.Tag.resultSetId, setOperation.resultSetId);

    ReadManager readManager = new ReadManager(server);

    retObj = readManager.serverSetSelect(cobj, false);

    ComArray retArray = retObj.getArray(ComObject.Tag.tableRecords);
    for (int j = 0; j < 1000; j++) {
      ComArray recordArray = (ComArray) retArray.getArray().get(j);
      assertEquals(recordArray.getArray().get(0), records[j]);
    }
    assertEquals(retArray.getArray().size(), 1001);
  }
}
