/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.query.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.ComObject;
import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.common.Record;
import com.sonicbase.common.ServersConfig;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.procedure.StoredProcedureContextImpl;
import com.sonicbase.query.BinaryExpression;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.query.Expression;
import com.sonicbase.schema.DataType;
import com.sonicbase.schema.FieldSchema;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.TestUtils;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.Offset;
import org.apache.commons.io.IOUtils;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

public class ResultSetImplTest {
  TableSchema tableSchema;
  IndexSchema indexSchema;
  DatabaseClient client;
  DatabaseCommon common;
  ServersConfig serversConfig;
  byte[][] records;
  List<Object[]> keys;
  ExpressionImpl.NextReturn ids;
  BinaryExpressionImpl expression;
  ExpressionImpl.RecordCache recordCache;
  SelectStatementImpl statement;
  List<ColumnImpl> columns;
  Set<SelectStatementImpl.DistinctRecord> uniqueRecords;
  ExpressionImpl.CachedRecord[][] cachedRecords;

  @BeforeMethod
  public void beforeMethod() throws IOException {
    tableSchema = TestUtils.createTable();
    indexSchema = TestUtils.createIndexSchema(tableSchema);
    tableSchema.addIndex(indexSchema);

    client = mock(DatabaseClient.class);
    when(client.getShardCount()).thenReturn(1);

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
    serversConfig = new ServersConfig("test", (ArrayNode) ((ObjectNode)node).withArray("shards"), 1, true, true);
    //when(common.getServersConfig()).thenReturn(serversConfig);
    common.setServersConfig(serversConfig);
    when(client.getCommon()).thenReturn(common);


    records = TestUtils.createRecords(common, tableSchema, 10);

    keys = TestUtils.createKeys(10);

    uniqueRecords = new HashSet<>();
    Object[][][] keysForRet = new Object[records.length][][];
    for (int i = 0; i < keysForRet.length; i++) {
      keysForRet[i] = new Object[][]{keys.get(i)};
    }
    ids = new ExpressionImpl.NextReturn(new String[]{"table1"}, keysForRet);
    statement = new SelectStatementImpl(client);
    statement.setTableNames(new String[]{"table1"});
    statement.applyDistinct("test", new String[]{"table1"}, ids, uniqueRecords);
    statement.setFromTable("table1");

    cachedRecords = new ExpressionImpl.CachedRecord[records.length][];
    recordCache = new ExpressionImpl.RecordCache();
    for (int i = 0; i < records.length; i++) {
      byte[] bytes = records[i];
      ExpressionImpl.CachedRecord record = new ExpressionImpl.CachedRecord(new Record("test", common, bytes), bytes);
      cachedRecords[i] = new ExpressionImpl.CachedRecord[]{record};
      recordCache.put("table1", keys.get(i), record);
    }
    columns = new ArrayList<>();
    columns.add(new ColumnImpl(null, null, "table1", "field1", null));
    columns.add(new ColumnImpl(null, null, "table1", "field2", null));
    columns.add(new ColumnImpl(null, null, "table1", "field3", null));
    statement.addSelectColumn(null, null, "table1", "field1", null);
    statement.addSelectColumn(null, null, "table1", "field2", null);
    statement.addSelectColumn(null, null, "table1", "field3", null);

    expression = new BinaryExpressionImpl() {
      protected IndexLookup createIndexLookup() {
//        IndexLookup ret = mock(IndexLookup.class);
//
//        when(ret.lookup(any(ExpressionImpl.class), any(Expression.class))).thenAnswer(
//            new Answer() {
//              public Object answer(InvocationOnMock invocation) {
//                Object[] args = invocation.getArguments();
//                SelectContextImpl ret = new SelectContextImpl();
//
//                return ret;
//              }
//            });
        IndexLookup ret = new IndexLookup() {
          @Override
          public SelectContextImpl lookup(ExpressionImpl expression, Expression topLevelExpression) {
            assertEquals(getCount(), 1000);
            assertEquals(getIndexName(), "_primarykey");
            assertEquals(getLeftOp(), Operator.equal);
            assertEquals(getRightOp(), null);
            assertEquals(getLeftKey(), null);
            assertEquals(getRightKey(), null);
            assertEquals(getLeftOriginalKey()[0], 300L);
            //assertEquals(getRightOriginalKey()[0], 200L);
            assertFalse(getEvaluateExpression());

            Object[][][] retKeys = new Object[keys.size()][][];
            for (int i = 0; i < keys.size(); i++) {
              retKeys[i] = new Object[][]{keys.get(i)};
            }

            try {
              return new SelectContextImpl("table1", "_primary", Operator.lessEqual, 0, null,
                  retKeys, expression.getRecordCache(), 0, true);
            }
            catch (IOException e) {
              throw new DatabaseException(e);
            }
          }
        };
        return ret;
      }

      protected SelectContextImpl tableScan(String dbName, long viewVersion, DatabaseClient client, int count,
                                            TableSchema tableSchema,
                                            List<OrderByExpressionImpl> orderByExpressions, ParameterHandler parmss,
                                            List<ColumnImpl> columns, int nextShard, Object[] nextKey,
                                            RecordCache recordCache, Counter[] counters, GroupByContext groupByContext, AtomicLong currOffset,
                                            Limit limit, Offset offset, boolean isProbe,
                                            boolean isRestrictToThisServer, StoredProcedureContextImpl storedProcesudureContext) {
        SelectContextImpl ret = new SelectContextImpl();
        return ret;
      }

      protected NextReturn evaluateOneSidedIndex(
          final String[] tableNames, int count, ExpressionImpl leftExpression, ExpressionImpl rightExpression, String leftColumn, Operator leftOp,
          Object leftValue, String rightColumn, Operator rightOp, Object rightValue, SelectStatementImpl.Explain explain,
          AtomicLong currOffset, AtomicLong countReturned, Limit limit, Offset offset, boolean analyze, int schemaRetryCount) {
        NextReturn ret = new NextReturn();
        return ret;
      }
    };

    ColumnImpl leftExpression = new ColumnImpl();
    leftExpression.setColumnName("field1");
    leftExpression.setTableName(tableSchema.getName());
    expression.setLeftExpression(leftExpression);
    ConstantImpl rightExpression = new ConstantImpl();
    rightExpression.setValue(300L);
    rightExpression.setSqlType(DataType.Type.BIGINT.getValue());
    expression.setRightExpression(rightExpression);
    expression.setOperator(BinaryExpression.Operator.equal);
    expression.setClient(client);
    expression.setTableName("table1");

  }

  @Test
  public void test() throws Exception {



    statement.setExpression(expression);
    ResultSetImpl ret = new ResultSetImpl("test", null, client, statement, new ParameterHandler(), uniqueRecords,
        new SelectContextImpl(ids, false, new String[]{"table1"}, 0, null,
            statement, recordCache, false, null), null, columns,
        null, null, null, null, new AtomicLong(), new AtomicLong(0),
        null, null, false, null){
      public ExpressionImpl.CachedRecord[][] readRecords(ExpressionImpl.NextReturn nextReturn, int schemaRetryCount){
        return cachedRecords;
      }
    };

    ret.next();
    assertEquals(ret.getString("field1"), "200");
    assertEquals((long)ret.getLong("field1"), 200L);
    assertEquals((int)ret.getInt("field1"), 200);
    assertEquals((float)ret.getFloat("field1"), 200f);
    assertEquals((double)ret.getDouble("field1"), 200d);
    assertEquals((short)ret.getShort("field1"), 200);
    assertEquals(ret.getBigDecimal("field1"), new BigDecimal(200));
    assertEquals(ret.getTimestamp("field3"), new Timestamp(200));
    assertEquals(ret.getDate("field3").getTime(), 200);
    assertEquals(ret.getTime("field3").getTime(), 200);
    assertEquals(new String(ret.getBytes("field2")), "0-value");
    assertEquals(IOUtils.toString(ret.getBinaryStream("field2"), "utf-8"), "0-value");
    assertEquals(IOUtils.toString(ret.getCharacterStream("field2")), "0-value");
    assertEquals(IOUtils.toString(ret.getUnicodeStream("field2")), "0-value");
    assertEquals(IOUtils.toString(ret.getAsciiStream("field2")), "0-value");
    //assertEquals(ret.getBigDecimal("field1"), new BigDecimal(200));

    assertEquals(ret.getString(1), "200");
    assertEquals((long)ret.getLong(1), 200L);
    assertEquals((int)ret.getInt(1), 200);
    assertEquals((float)ret.getFloat(1), 200f);
    assertEquals((double)ret.getDouble(1), 200d);
    assertEquals((short)ret.getShort(1), (short)200);
    assertEquals(ret.getBigDecimal(1), new BigDecimal(200));
    assertEquals(ret.getTimestamp(3), new Timestamp(200));
    assertEquals(ret.getDate(3).getTime(), 200);
    assertEquals(ret.getTime(3).getTime(), 200);
    assertEquals(new String(ret.getBytes(2)), "0-value");
    assertEquals(IOUtils.toString(ret.getBinaryStream(2), "utf-8"), "0-value");
    assertEquals(IOUtils.toString(ret.getCharacterStream(2)), "0-value");

    //assertEquals(ret.getBigDecimal(0), new BigDecimal(200));

    ret.getMoreResults(0);

    assertEquals(ret.getString("field1"), "200");
    assertEquals((long)ret.getLong("field1"), 200L);
    assertEquals((int)ret.getInt("field1"), 200);
    assertEquals((float)ret.getFloat("field1"), 200f);
    assertEquals((double)ret.getDouble("field1"), 200d);
    assertEquals((short)ret.getShort("field1"), 200);

    assertFalse(ret.isLast());
    assertTrue(ret.isFirst());
    //assertFalse(ret.isAfterLast());
    assertFalse(ret.isBeforeFirst());

    ret.close();
  }

  @Test
  public void testAliases() throws Exception {


    columns.set(0, new ColumnImpl(null, null, "table1", "field1", "alias1"));

    Map<String, ColumnImpl> aliases = new HashMap<>();
    aliases.put("alias1", columns.get(0));
    statement.setAliases(aliases);
    statement.setExpression(expression);
    ResultSetImpl ret = new ResultSetImpl("test", null, client, statement, new ParameterHandler(), uniqueRecords,
        new SelectContextImpl(ids, false, new String[]{"table1"}, 0, null,
            statement, recordCache, false, null), null, columns,
        null, null, null, null, new AtomicLong(), new AtomicLong(0),
        null, null, false, null){
      public ExpressionImpl.CachedRecord[][] readRecords(ExpressionImpl.NextReturn nextReturn, int schemaRetryCount){
        return cachedRecords;
      }
    };

    ret.next();
    assertEquals(ret.getString("alias1"), "200");
    assertEquals((long)ret.getLong("alias1"), 200L);
    assertEquals((int)ret.getInt("alias1"), 200);
    assertEquals((float)ret.getFloat("alias1"), 200f);
    assertEquals((double)ret.getDouble("alias1"), 200d);
    assertEquals((short)ret.getShort("alias1"), 200);
    //assertEquals(ret.getBigDecimal("field1"), new BigDecimal(200));

  }

  @Test(enabled=false)
  public void testGroupBy() throws Exception {


    List<net.sf.jsqlparser.expression.Expression> groupByColumns = new ArrayList<>();
    Column column = new Column(new Table("table1"), "field2");
    groupByColumns.add(column);

    List<GroupByContext.FieldContext> fieldContexts = new ArrayList<>();
    for (int j = 0; j < groupByColumns.size(); j++) {
      GroupByContext.FieldContext fieldContext = new GroupByContext.FieldContext();

      column = (Column) groupByColumns.get(j);
      String tableName = column.getTable().getName();
      if (tableName == null) {
        tableName = "table1";
      }
      int fieldOffset = client.getCommon().getTables("test").get(tableName).getFieldOffset(column.getColumnName());
      FieldSchema fieldSchema = client.getCommon().getTables("test").get(tableName).getFields().get(fieldOffset);
      fieldContext.setFieldName(column.getColumnName());
      fieldContext.setDataType(fieldSchema.getType());
      fieldContext.setComparator(fieldSchema.getType().getComparator());
      fieldContext.setFieldOffset(fieldOffset);
      fieldContexts.add(fieldContext);
    }
    GroupByContext groupContext = null;//new GroupByContext(fieldContexts);
    //expression.setGroupByContext(groupContext);


    statement.setExpression(expression);
    ResultSetImpl ret = new ResultSetImpl("test", null, client, statement, new ParameterHandler(), uniqueRecords,
        new SelectContextImpl(ids, false, new String[]{"table1"}, 0, null,
            statement, recordCache, false, null), null, columns,
        null, null, null, null, new AtomicLong(), new AtomicLong(0),
        groupByColumns, groupContext, false, null){
      public ExpressionImpl.CachedRecord[][] readRecords(ExpressionImpl.NextReturn nextReturn, int schemaRetryCount){
        return cachedRecords;
      }
    };

    ret.next();
    assertEquals(ret.getString("field1"), "200");
    assertEquals(ret.getString("field2"), "0-value");

    ret.next();
    assertEquals(ret.getString("field1"), "300");
    assertEquals(ret.getString("field2"), "1-value");

    ret.next();
    assertEquals(ret.getString("field1"), "400");
    assertEquals(ret.getString("field2"), "0-value");

    ret.getMoreResults(0);

    ret.next();
    assertEquals(ret.getString("field1"), "200");
    assertEquals(ret.getString("field2"), "0-value");

    ret.next();
    assertEquals(ret.getString("field1"), "400");
    assertEquals(ret.getString("field2"), "0-value");

    ret.next();
    assertEquals(ret.getString("field1"), "600");
    assertEquals(ret.getString("field2"), "0-value");
  }

  @Test
  public void testCounters() throws Exception {


    Counter counter = new Counter();
    Counter[] countersList = new Counter[2];

    counter.setTableName("table1");
    counter.setColumnName("__all__");
    counter.setColumn(0);
    counter.setDataType(DataType.Type.BIGINT);
    counter.setDestTypeToLong();

    counter.setCount(100L);
    countersList[0] = counter;

    counter = new Counter();
    counter.setTableName("table1");
    counter.setColumnName("field1");
    counter.setColumn(0);
    counter.setDataType(DataType.Type.BIGINT);
    counter.setDestTypeToLong();

    counter.addLong(200L);
    counter.addLong(100L);
    countersList[1] = counter;

    when(client.send(eq("ReadManager:indexLookupExpression"), anyInt(), anyInt(), any(ComObject.class), eq(DatabaseClient.Replica.def))).thenAnswer(
        new Answer() {
          public Object answer(InvocationOnMock invocation) {
            return new ComObject().serialize();
          }});
    expression.setTopLevelExpression(expression);
    expression.setCounters(countersList);
    statement.setExpression(expression);
    statement.addSelectColumn("count", null, "table1", "field1", "count");
    ExpressionList parms = new ExpressionList();
    Column column = new Column(new Table("table1"), "field1");
    parms.setExpressions(Collections.singletonList((net.sf.jsqlparser.expression.Expression)column));
    statement.addSelectColumn("avg", parms, "table1", "field1", "avg");
    statement.addSelectColumn("min", parms, "table1", "field1", "min");
    statement.addSelectColumn("max", parms, "table1", "field1", "max");
    statement.addSelectColumn("sum", parms, "table1", "field1", "sum");

    ResultSetImpl ret = new ResultSetImpl("test", null, client, statement, new ParameterHandler(), uniqueRecords,
        new SelectContextImpl(ids, false, new String[]{"table1"}, 0, null,
            statement, recordCache, false, null), null, columns,
        null, countersList, null, null, new AtomicLong(), new AtomicLong(0),
        null, null, false, null){
      public ExpressionImpl.CachedRecord[][] readRecords(ExpressionImpl.NextReturn nextReturn, int schemaRetryCount){
        return cachedRecords;
      }
    };


    ret.getMoreResults(0);

    ret.next();
    assertEquals((long)ret.getLong("count"), 100L);
    assertEquals((long)ret.getLong("min"), 100L);
    assertEquals((long)ret.getLong("max"), 200L);
    assertEquals((long)ret.getLong("avg"), 150L);
    assertEquals((long)ret.getLong("sum"), 300L);

    assertEquals((int)ret.getInt("count"), 100);
    assertEquals((int)ret.getInt("min"), 100);
    assertEquals((int)ret.getInt("max"), 200);
    assertEquals((int)ret.getInt("avg"), 150);
    assertEquals((int)ret.getInt("sum"), 300);

    assertEquals((double)ret.getDouble("count"), 100d);
    assertEquals((double)ret.getDouble("min"), 100d);
    assertEquals((double)ret.getDouble("max"), 200d);
    assertEquals((double)ret.getDouble("avg"), 150d);
    assertEquals((double)ret.getDouble("sum"), 300d);

    assertEquals((float)ret.getFloat("count"), 100f);
    assertEquals((float)ret.getFloat("min"), 100f);
    assertEquals((float)ret.getFloat("max"), 200f);
    assertEquals((float)ret.getFloat("avg"), 150f);
    assertEquals((float)ret.getFloat("sum"), 300f);

  }
}
