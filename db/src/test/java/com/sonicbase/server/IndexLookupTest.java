/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.server;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.common.Record;
import com.sonicbase.query.BinaryExpression;
import com.sonicbase.query.impl.BinaryExpressionImpl;
import com.sonicbase.query.impl.ColumnImpl;
import com.sonicbase.query.impl.ConstantImpl;
import com.sonicbase.query.impl.Counter;
import com.sonicbase.schema.DataType;
import com.sonicbase.schema.FieldSchema;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import org.jetbrains.annotations.NotNull;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.UnsupportedEncodingException;
import java.sql.Timestamp;
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
    TableSchema tableSchema = createTable();

    DatabaseCommon common = createCommon(tableSchema);
    when(server.getCommon()).thenReturn(common);

    Set<Integer> columnOffsets = new HashSet<>();
    columnOffsets.add(1);

    Counter[] counters = new Counter[1];
    counters[0] = new Counter();
    counters[0].setColumn(1);
    counters[0].setTableName("table");
    counters[0].setDataType(DataType.Type.BIGINT);
    indexLookup.setCounters(counters);

    byte[][] records = createRecords(common, tableSchema, 1);

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

  @NotNull
  public static DatabaseCommon createCommon(TableSchema tableSchema) {
    DatabaseCommon common = new DatabaseCommon();
    common.addDatabase("test");
    common.getTables("test").put("table1", tableSchema);
    common.getTablesById("test").put(tableSchema.getTableId(), tableSchema);
//    when(server.getCommon()).thenReturn(common);
//    when(common.getSchemaVersion()).thenReturn(10);
//    tables.put(tableSchema.getTableId(), tableSchema);
//    when(common.getTablesById(anyString())).thenReturn(tables);
//    Map<String, TableSchema> tablesByName = new HashMap<>();
//    tablesByName.put(tableSchema.getName(), tableSchema);
//    when(common.getTables(anyString())).thenReturn(tablesByName);
//    when(common.getTableSchema(anyString(), anyString(), anyString())).thenReturn(tableSchema);
//
//    Schema schema = new Schema();
//    schema.setTables(tablesByName);
//    schema.setTablesById(tables);
//
//    when(common.getSchema(anyString())).thenReturn(schema);
    return common;
  }

  @NotNull
  static DatabaseCommon createCommon(DatabaseServer server) {
    DatabaseCommon common = mock(DatabaseCommon.class);
    when(server.getCommon()).thenReturn(common);
    when(common.getSchemaVersion()).thenReturn(10);
    return common;
  }

  @Test
  public void testHandleRecord() throws UnsupportedEncodingException {
    DatabaseServer server = mock(DatabaseServer.class);
    IndexLookupOneKey indexLookup = new IndexLookupOneKey(server);

    AtomicBoolean done = new AtomicBoolean();
    Object[] key = new Object[]{100L};

    Map<Integer, TableSchema> tables = new HashMap<>();
    TableSchema tableSchema = createTable();

    DatabaseCommon common = createCommon(tableSchema);

    byte[][] records = createRecords(common, tableSchema, 10);

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
    TableSchema tableSchema = createTable();
    indexLookup.setTableSchema(tableSchema);

    DatabaseCommon common = createCommon(tableSchema);
    when(server.getCommon()).thenReturn(common);

    byte[][] records = createRecords(common, tableSchema, 10);

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
    expression.setOperator(BinaryExpression.Operator.equal);

    indexLookup.setExpression(expression);
    indexLookup.handleRecordEvaluateExpression(records, done);

    assertEquals(indexLookup.retRecords.get(0), records[1]);
    assertEquals(indexLookup.retRecords.size(), 1);

  }

  public static byte[][] createRecords(DatabaseCommon common, TableSchema tableSchema, int count) throws UnsupportedEncodingException {
    byte[][] records = new byte[count][];
    for (int i = 0; i < records.length; i++) {
      Object[] fieldArray = new Object[5];
      fieldArray[1] = 200L + (100 * i);
      fieldArray[2] = ((i % 2) + "-value").getBytes("utf-8");
      fieldArray[3] = new Timestamp(200 + (100 * i));
      fieldArray[4] = (int)(1200 + (100 * i));
      Record record = new Record(tableSchema);
      record.setFields(fieldArray);
      records[i] = record.serialize(common, DatabaseClient.SERIALIZATION_VERSION);
    }
    return records;
  }
  public static List<Object[]> createKeys(int count) {
    List<Object[]> ret = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      Object[] fieldArray = new Object[1];
      fieldArray[0] = 200L + (100 * i);
      ret.add(fieldArray);
    }
    return ret;
  }

  public static TableSchema createTable() {
    TableSchema tableSchema = new TableSchema();
    tableSchema.setName("table1");
    tableSchema.setTableId(100);
    List<FieldSchema> fields = new ArrayList<>();
    FieldSchema fSchema = new FieldSchema();
    fSchema.setName("_id");
    fSchema.setType(DataType.Type.BIGINT);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field1");
    fSchema.setType(DataType.Type.BIGINT);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field2");
    fSchema.setType(DataType.Type.VARCHAR);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field3");
    fSchema.setType(DataType.Type.TIMESTAMP);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field4");
    fSchema.setType(DataType.Type.INTEGER);
    fields.add(fSchema);
    tableSchema.setFields(fields);
    List<String> primaryKey = new ArrayList<>();
    primaryKey.add("field1");
    tableSchema.setPrimaryKey(primaryKey);
    return tableSchema;
  }

  public static IndexSchema createIndexSchema(TableSchema tableSchema) {
    IndexSchema indexSchema = new IndexSchema();
    indexSchema.setFields(new String[]{"field1"}, tableSchema);
    indexSchema.setIndexId(1);
    indexSchema.setIsPrimaryKey(true);
    indexSchema.setName("_primarykey");
    indexSchema.setComparators(tableSchema.getComparators(new String[]{"field1"}));

    TableSchema.Partition[] partitions = new TableSchema.Partition[1];
    partitions[0] = new TableSchema.Partition();
    partitions[0].setUnboundUpper(true);
    indexSchema.setCurrPartitions(partitions);
    tableSchema.addIndex(indexSchema);
    return indexSchema;
  }
}