package com.sonicbase.server;

import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.common.Record;
import com.sonicbase.query.BinaryExpression;
import com.sonicbase.query.impl.BinaryExpressionImpl;
import com.sonicbase.query.impl.ColumnImpl;
import com.sonicbase.query.impl.ConstantImpl;
import com.sonicbase.query.impl.Counter;
import com.sonicbase.schema.DataType;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.TestUtils;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class IndexLookupTest {

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testProcessViewFlags_returnAll() {

    DatabaseServer server = mock(DatabaseServer.class);
    IndexLookupOneKey indexLookup = new IndexLookupOneKey(server);

    byte[][] bytes = new byte[10][];
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = new byte[100];
      Record.setDbViewNumber(bytes[i], 100);
      Record.setDbViewFlags(bytes[i], Record.DB_VIEW_FLAG_ADDING);
    }

    int viewVersion = 100;
    byte[][] ret = indexLookup.processViewFlags(viewVersion, bytes);
    assertEquals(ret.length, 10);
  }

  @Test
  public void testProcessViewFlags_blockViewVersion() {

    DatabaseServer server = mock(DatabaseServer.class);
    IndexLookupOneKey indexLookup = new IndexLookupOneKey(server);

    byte[][] bytes = new byte[10][];
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = new byte[100];
      if (i == 0) {
        Record.setDbViewNumber(bytes[i], 110);
        Record.setDbViewFlags(bytes[i], Record.DB_VIEW_FLAG_DELETING);
      }
      else {
        Record.setDbViewNumber(bytes[i], 100);
        Record.setDbViewFlags(bytes[i], Record.DB_VIEW_FLAG_ADDING);
      }
    }

    int viewVersion = 100;
    byte[][] ret = indexLookup.processViewFlags(viewVersion, bytes);
    assertEquals(ret.length, 9);
  }

  @Test
  public void testEvaluateCounters() throws UnsupportedEncodingException {
    DatabaseServer server = mock(DatabaseServer.class);
    IndexLookupOneKey indexLookup = new IndexLookupOneKey(server);
    indexLookup.setDbName("test");
    Map<Integer, TableSchema> tables = new HashMap<>();
    TableSchema tableSchema = TestUtils.createTable();

    DatabaseCommon common = TestUtils.createCommon(tableSchema);
    when(server.getCommon()).thenReturn(common);

    Set<Integer> columnOffsets = new HashSet<>();
    columnOffsets.add(1);

    Counter[] counters = new Counter[1];
    counters[0] = new Counter();
    counters[0].setColumn(1);
    counters[0].setTableName("table");
    counters[0].setDataType(DataType.Type.BIGINT);
    indexLookup.setCounters(counters);

    byte[][] records = TestUtils.createRecords(common, tableSchema, 1);

//    GroupByContext groupContext = new GroupByContext();
//    Counter counter = new Counter();
//    counter.setTableName("table");
//    counter.setColumnName("field1");
//    groupContext.addCounterTemplate(counter);

    byte[][] ret = indexLookup.evaluateCounters(columnOffsets, records);
    Record record = new Record("test", common, ret[0]);
    assertEquals(record.getFields()[1], 200L);
    //assertNull(record.getFields()[2]);
    assertEquals((long) counters[0].getMaxLong(), 200L);
    assertEquals((long) counters[0].getMinLong(), 200L);

//    Counter retCounter = groupContext.getCounterTemplates().values().iterator().next();
//    assertEquals((long)retCounter.getMaxLong(), 200L);
//    assertEquals((long)retCounter.getMinLong(), 200L);
  }

  @Test
  public void testHandleRecord() throws UnsupportedEncodingException {
    DatabaseServer server = mock(DatabaseServer.class);
    IndexLookupOneKey indexLookup = new IndexLookupOneKey(server);

    AtomicBoolean done = new AtomicBoolean();
    Object[] key = new Object[]{100L};

    Map<Integer, TableSchema> tables = new HashMap<>();
    TableSchema tableSchema = TestUtils.createTable();

    DatabaseCommon common = TestUtils.createCommon(tableSchema);

    byte[][] records = TestUtils.createRecords(common, tableSchema, 10);

    indexLookup.currOffset = new AtomicLong();
    indexLookup.countReturned = new AtomicLong();
    indexLookup.retRecords = new ArrayList<>();

    indexLookup.handleRecord(100, key, false,
      records, null, done);

    assertEquals(indexLookup.retRecords.get(0), records[0]);

  }

  @Test
  public void testHandleRecordEvaluateExpression() throws UnsupportedEncodingException {
    DatabaseServer server = mock(DatabaseServer.class);
    IndexLookupOneKey indexLookup = new IndexLookupOneKey(server);
    indexLookup.setDbName("test");
    AtomicBoolean done = new AtomicBoolean();

    Map<Integer, TableSchema> tables = new HashMap<>();
    TableSchema tableSchema = TestUtils.createTable();
    indexLookup.setTableSchema(tableSchema);

    DatabaseCommon common = TestUtils.createCommon(tableSchema);
    when(server.getCommon()).thenReturn(common);

    byte[][] records = TestUtils.createRecords(common, tableSchema, 10);

    indexLookup.currOffset = new AtomicLong();
    indexLookup.countReturned = new AtomicLong();
    indexLookup.retRecords = new ArrayList<>();


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

    indexLookup.setExpression(expression);
    indexLookup.handleRecordEvaluateExpression(records, done);

    assertEquals(indexLookup.retRecords.get(0), records[1]);
    assertEquals(indexLookup.retRecords.size(), 1);

  }


}