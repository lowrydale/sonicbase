package com.sonicbase.query.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.common.Record;
import com.sonicbase.common.ServersConfig;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.procedure.StoredProcedureContextImpl;
import com.sonicbase.query.BinaryExpression;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.query.Expression;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.TestUtils;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.Offset;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static java.sql.Types.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

public class BInaryExpressionImplTest {
  TableSchema tableSchema;
  IndexSchema indexSchema;
  DatabaseClient client;
  List<Object[]> keys;
  DatabaseCommon common;
  byte[][] records;

  @BeforeMethod
  public void beforeMethod() throws IOException {
    tableSchema = TestUtils.createTable();
    indexSchema = TestUtils.createIndexSchema(tableSchema);

    client = mock(DatabaseClient.class);

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
    when(client.getCommon()).thenReturn(common);

    records = TestUtils.createRecords(common, tableSchema, 10);

    keys = TestUtils.createKeys(10);

  }
  @Test
  public void testAndExpression() throws IOException {


    BinaryExpressionImpl expression = new BinaryExpressionImpl() {
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
            assertEquals(getCount(), 100);
            assertEquals(getIndexName(), "_primarykey");
            assertEquals(getLeftOp(), Operator.LESS_EQUAL);
            assertEquals(getRightOp(), Operator.GREATER_EQUAL);
            assertEquals(getLeftKey(), null);
            assertEquals(getRightKey(), null);
            assertEquals(getLeftOriginalKey()[0], 500L);
            assertEquals(getRightOriginalKey()[0], 200L);
            assertFalse(getEvaluateExpression());

            Object[][][] retKeys = new Object[keys.size()][][];
            for (int i = 0; i < keys.size(); i++) {
              retKeys[i] = new Object[][]{keys.get(i)};
            }

            return new SelectContextImpl("table1", "_primary", Operator.LESS_EQUAL, 0, null,
                retKeys, expression.getRecordCache(), 0, true);

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


    BinaryExpressionImpl leftExp = new BinaryExpressionImpl();
    ColumnImpl column = new ColumnImpl();
    column.setTableName("table1");
    column.setColumnName("field1");
    leftExp.setLeftExpression(column);
    ConstantImpl constant = new ConstantImpl();
    constant.setSqlType(BIGINT);
    constant.setValue(200L);
    leftExp.setRightExpression(constant);
    leftExp.setOperator(BinaryExpression.Operator.GREATER_EQUAL);
    leftExp.setDbName("test");
    leftExp.setTableName("table1");
    leftExp.setClient(client);

    BinaryExpressionImpl rightExp = new BinaryExpressionImpl();
    column = new ColumnImpl();
    column.setTableName("table1");
    column.setColumnName("field1");
    rightExp.setLeftExpression(column);
    constant = new ConstantImpl();
    constant.setSqlType(BIGINT);
    constant.setValue(500L);
    rightExp.setRightExpression(constant);
    rightExp.setOperator(BinaryExpression.Operator.LESS_EQUAL);
    rightExp.setDbName("test");
    rightExp.setTableName("table1");
    rightExp.setClient(client);

    expression.setLeftExpression(leftExp);
    expression.setOperator(BinaryExpression.Operator.AND);
    expression.setRightExpression(rightExp);

    expression.setDbName("test");
    expression.setTableName("table1");
    expression.setClient(client);
    AtomicLong currOffset = new AtomicLong();
    AtomicLong countReturned = new AtomicLong();
    Limit limit = null;
    Offset offset = null;
    AtomicReference<String> usedIndex = new AtomicReference<>();
    SelectStatementImpl.Explain explain = null;
    ExpressionImpl.NextReturn ret = expression.evaluateAndExpression(null,100, usedIndex, explain, currOffset, countReturned,
        limit, offset, false, false, 0);

    for (int i = 0; i < ret.getKeys().length; i++) {
      assertEquals(ret.getKeys()[i][0][0], keys.get(i)[0]);
    }
    assertEquals(ret.getKeys().length, keys.size());
  }

  @Test
  public void testLike() {
    BinaryExpressionImpl expression = new BinaryExpressionImpl();
    assertTrue(expression.like("testing this string", "test%"));

    assertTrue(expression.like("testing this string", "t?sting%"));
  }

  @Test
  public void testEvaluateExpression() throws UnsupportedEncodingException {
    BinaryExpressionImpl leftExp = new BinaryExpressionImpl();
    ColumnImpl column = new ColumnImpl();
    column.setTableName("table1");
    column.setColumnName("field1");
    leftExp.setLeftExpression(column);
    ConstantImpl constant = new ConstantImpl();
    constant.setSqlType(BIGINT);
    constant.setValue(200L);
    leftExp.setRightExpression(constant);
    leftExp.setOperator(BinaryExpression.Operator.GREATER_EQUAL);
    leftExp.setDbName("test");
    leftExp.setTableName("table1");
    leftExp.setClient(client);

    BinaryExpressionImpl rightExp = new BinaryExpressionImpl();
    column = new ColumnImpl();
    column.setTableName("table1");
    column.setColumnName("field1");
    rightExp.setLeftExpression(column);
    constant = new ConstantImpl();
    constant.setSqlType(BIGINT);
    constant.setValue(500L);
    rightExp.setRightExpression(constant);
    rightExp.setOperator(BinaryExpression.Operator.LESS_EQUAL);
    rightExp.setDbName("test");
    rightExp.setTableName("table1");
    rightExp.setClient(client);

    BinaryExpressionImpl expression = new BinaryExpressionImpl();
    expression.setLeftExpression(leftExp);
    expression.setOperator(BinaryExpression.Operator.AND);
    expression.setRightExpression(rightExp);

    expression.setDbName("test");
    expression.setTableName("table1");
    expression.setClient(client);

    Record[] records = new Record[1];
    for (int i = 0; i < records.length; i++) {
      records[i] = new Record("test", common, this.records[i]);
    }
    assertTrue((Boolean) expression.evaluateSingleRecord(new TableSchema[]{tableSchema}, records, null));

    column.setColumnName("field1");
    rightExp.setNot(true);
    rightExp.setOperator(BinaryExpression.Operator.LESS_EQUAL);
    constant.setValue(500L);

    assertFalse((Boolean) expression.evaluateSingleRecord(new TableSchema[]{tableSchema}, records, null));

    column.setColumnName("field1");
    rightExp.setNot(true);
    rightExp.setOperator(BinaryExpression.Operator.GREATER_EQUAL);
    constant.setValue(100L);

    assertFalse((Boolean) expression.evaluateSingleRecord(new TableSchema[]{tableSchema}, records, null));

    column.setColumnName("field2");
    rightExp.setNot(false);
    rightExp.setOperator(BinaryExpression.Operator.LIKE);
    constant.setValue("%value%".getBytes("utf-8"));

    assertTrue((Boolean) expression.evaluateSingleRecord(new TableSchema[]{tableSchema}, records, null));

    column.setColumnName("field2");
    rightExp.setNot(true);
    rightExp.setOperator(BinaryExpression.Operator.LIKE);
    constant.setValue("%value%".getBytes("utf-8"));

    assertFalse((Boolean) expression.evaluateSingleRecord(new TableSchema[]{tableSchema}, records, null));


    column.setColumnName("field1");
    rightExp.setNot(false);
    rightExp.setOperator(BinaryExpression.Operator.NOT_EQUAL);
    constant.setValue(700L);

    assertTrue((Boolean) expression.evaluateSingleRecord(new TableSchema[]{tableSchema}, records, null));

    column.setColumnName("field1");
    rightExp.setNot(true);
    rightExp.setOperator(BinaryExpression.Operator.NOT_EQUAL);
    constant.setValue(700L);

    assertFalse((Boolean) expression.evaluateSingleRecord(new TableSchema[]{tableSchema}, records, null));
    column.setColumnName("field1");
    rightExp.setNot(false);
    rightExp.setOperator(BinaryExpression.Operator.LESS);
    constant.setValue(700L);

    assertTrue((Boolean) expression.evaluateSingleRecord(new TableSchema[]{tableSchema}, records, null));

    column.setColumnName("field1");
    rightExp.setNot(true);
    rightExp.setOperator(BinaryExpression.Operator.LESS);
    constant.setValue(700L);

    assertFalse((Boolean) expression.evaluateSingleRecord(new TableSchema[]{tableSchema}, records, null));

    column.setColumnName("field1");
    rightExp.setNot(false);
    rightExp.setOperator(BinaryExpression.Operator.GREATER);
    constant.setValue(0L);

    assertTrue((Boolean) expression.evaluateSingleRecord(new TableSchema[]{tableSchema}, records, null));

    column.setColumnName("field1");
    rightExp.setNot(true);
    rightExp.setOperator(BinaryExpression.Operator.GREATER);
    constant.setValue(0L);

    assertFalse((Boolean) expression.evaluateSingleRecord(new TableSchema[]{tableSchema}, records, null));
  }

  @Test
  public void testEvaluateExpressionMath() throws UnsupportedEncodingException {
    ColumnImpl column1 = new ColumnImpl();
    column1.setTableName("table1");
    column1.setColumnName("field1");

    BinaryExpressionImpl rightExp = new BinaryExpressionImpl();
    ColumnImpl column2 = new ColumnImpl();
    column2.setTableName("table1");
    column2.setColumnName("field4");
    rightExp.setLeftExpression(column2);
    ConstantImpl constant = new ConstantImpl();
    constant.setSqlType(BIGINT);
    constant.setValue(-1000L);
    rightExp.setRightExpression(constant);
    rightExp.setOperator(BinaryExpression.Operator.PLUS);
    rightExp.setDbName("test");
    rightExp.setTableName("table1");
    rightExp.setClient(client);

    BinaryExpressionImpl expression = new BinaryExpressionImpl();
    expression.setLeftExpression(column1);
    expression.setOperator(BinaryExpression.Operator.EQUAL);
    expression.setRightExpression(rightExp);

    expression.setDbName("test");
    expression.setTableName("table1");
    expression.setClient(client);

    Record[] records = new Record[1];
    for (int i = 0; i < records.length; i++) {
      records[i] = new Record("test", common, this.records[i]);
    }
    assertTrue((Boolean) expression.evaluateSingleRecord(new TableSchema[]{tableSchema}, records, null));

    column1.setColumnName("field4");

    column2.setColumnName("field1");
    constant.setValue(1000L);
    rightExp.setRightExpression(constant);
    rightExp.setOperator(BinaryExpression.Operator.PLUS);

    assertTrue((Boolean) expression.evaluateSingleRecord(new TableSchema[]{tableSchema}, records, null));


    column1.setColumnName("field4");

    column2.setColumnName("field1");
    constant.setValue(6L);
    rightExp.setRightExpression(constant);
    rightExp.setOperator(BinaryExpression.Operator.TIMES);

    assertTrue((Boolean) expression.evaluateSingleRecord(new TableSchema[]{tableSchema}, records, null));

    column1.setColumnName("field1");

    column2.setColumnName("field4");
    constant.setValue(6L);
    rightExp.setRightExpression(constant);
    rightExp.setOperator(BinaryExpression.Operator.DIVIDE);

    assertTrue((Boolean) expression.evaluateSingleRecord(new TableSchema[]{tableSchema}, records, null));
  }

  @Test
  public void testEvaluateExpressionMath2() throws UnsupportedEncodingException {
    ConstantImpl constant1 = new ConstantImpl();
    constant1.setSqlType(DOUBLE);

    BinaryExpressionImpl rightExp = new BinaryExpressionImpl();
    ConstantImpl constant2 = new ConstantImpl();
    ConstantImpl constant3 = new ConstantImpl();
    constant2.setSqlType(DOUBLE);
    constant3.setSqlType(DOUBLE);
    rightExp.setLeftExpression(constant2);
    rightExp.setRightExpression(constant3);
    rightExp.setDbName("test");
    rightExp.setTableName("table1");
    rightExp.setClient(client);

    BinaryExpressionImpl expression = new BinaryExpressionImpl();
    expression.setLeftExpression(constant1);
    expression.setOperator(BinaryExpression.Operator.EQUAL);
    expression.setRightExpression(rightExp);

    expression.setDbName("test");
    expression.setTableName("table1");
    expression.setClient(client);

    Record[] records = new Record[1];
    for (int i = 0; i < records.length; i++) {
      records[i] = new Record("test", common, this.records[i]);
    }

    constant1.setSqlType(BIGINT);
    constant2.setSqlType(BIGINT);
    constant3.setSqlType(BIGINT);
    constant1.setValue((long)(2 | 4));
    constant2.setValue(2L);
    constant3.setValue(4L);
    rightExp.setOperator(BinaryExpression.Operator.BITWISE_OR);
    assertTrue((Boolean) expression.evaluateSingleRecord(new TableSchema[]{tableSchema}, records, null));

    constant1.setValue((long)(2 & 3));
    constant2.setValue(2L);
    constant3.setValue(3L);
    rightExp.setOperator(BinaryExpression.Operator.BITWISE_AND);
    assertTrue((Boolean) expression.evaluateSingleRecord(new TableSchema[]{tableSchema}, records, null));

    constant1.setValue((long)(2 ^ 3));
    constant2.setValue(2L);
    constant3.setValue(3L);
    rightExp.setOperator(BinaryExpression.Operator.BITWISE_X_OR);
    assertTrue((Boolean) expression.evaluateSingleRecord(new TableSchema[]{tableSchema}, records, null));


    constant1.setSqlType(DECIMAL);
    constant2.setSqlType(DECIMAL);
    constant3.setSqlType(DECIMAL);
    constant1.setValue(new BigDecimal(4));
    constant2.setValue(new BigDecimal(2));
    constant3.setValue(new BigDecimal(2));
    rightExp.setOperator(BinaryExpression.Operator.PLUS);
    assertTrue((Boolean) expression.evaluateSingleRecord(new TableSchema[]{tableSchema}, records, null));

    constant1.setValue(new BigDecimal(4));
    constant2.setValue(new BigDecimal(6));
    constant3.setValue(new BigDecimal(2));
    rightExp.setOperator(BinaryExpression.Operator.MINUS);
    assertTrue((Boolean) expression.evaluateSingleRecord(new TableSchema[]{tableSchema}, records, null));

    constant1.setValue(new BigDecimal(8));
    constant2.setValue(new BigDecimal(2));
    constant3.setValue(new BigDecimal(4));
    rightExp.setOperator(BinaryExpression.Operator.TIMES);
    assertTrue((Boolean) expression.evaluateSingleRecord(new TableSchema[]{tableSchema}, records, null));

    constant1.setValue(new BigDecimal(2));
    constant2.setValue(new BigDecimal(8));
    constant3.setValue(new BigDecimal(4));
    rightExp.setOperator(BinaryExpression.Operator.DIVIDE);
    assertTrue((Boolean) expression.evaluateSingleRecord(new TableSchema[]{tableSchema}, records, null));

    constant1.setSqlType(DOUBLE);
    constant2.setSqlType(DOUBLE);
    constant3.setSqlType(DOUBLE);
    constant1.setValue(4d);
    constant2.setValue(2d);
    constant3.setValue(2d);
    rightExp.setOperator(BinaryExpression.Operator.PLUS);
    assertTrue((Boolean) expression.evaluateSingleRecord(new TableSchema[]{tableSchema}, records, null));

    constant1.setValue(4d);
    constant2.setValue(6d);
    constant3.setValue(2d);
    rightExp.setOperator(BinaryExpression.Operator.MINUS);
    assertTrue((Boolean) expression.evaluateSingleRecord(new TableSchema[]{tableSchema}, records, null));

    constant1.setValue(8d);
    constant2.setValue(2d);
    constant3.setValue(4d);
    rightExp.setOperator(BinaryExpression.Operator.TIMES);
    assertTrue((Boolean) expression.evaluateSingleRecord(new TableSchema[]{tableSchema}, records, null));

    constant1.setValue(2d);
    constant2.setValue(8d);
    constant3.setValue(4d);
    rightExp.setOperator(BinaryExpression.Operator.DIVIDE);
    assertTrue((Boolean) expression.evaluateSingleRecord(new TableSchema[]{tableSchema}, records, null));

  }

  @Test
  public void testIsIndexed() {
    BinaryExpressionImpl expression = new BinaryExpressionImpl();

    ColumnImpl column = new ColumnImpl();
    column.setTableName("table1");
    column.setColumnName("field1");
    expression.setLeftExpression(column);
    ConstantImpl constant = new ConstantImpl();
    constant.setSqlType(BIGINT);
    constant.setValue(500L);
    expression.setRightExpression(constant);
    expression.setOperator(BinaryExpression.Operator.LESS_EQUAL);
    expression.setDbName("test");
    expression.setTableName("table1");
    expression.setClient(client);

    AtomicBoolean isColumn = new AtomicBoolean();
    assertEquals(expression.isIndexed(column, isColumn), "field1");
    assertTrue(isColumn.get());

  }
}
