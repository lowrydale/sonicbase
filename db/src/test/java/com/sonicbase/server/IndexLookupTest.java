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
import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Date;
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
      Object[] fieldArray = new Object[28];
      fieldArray[1] = 200L + (100 * i);
      fieldArray[2] = ((i % 2) + "-value").getBytes("utf-8");
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
      fieldArray[25] = new Date(1900 + i, 10, 1);
      fieldArray[26] = new Time(1, i, 0);
      fieldArray[27] = new Timestamp(i);

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

  public static List<Object[]> createKeysForSecondaryIndex(int count) throws UnsupportedEncodingException {
    List<Object[]> ret = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      Object[] fieldArray = new Object[27];
      fieldArray[0] = 200L + (100 * i);
      fieldArray[1] = ((i % 2) + "-value").getBytes("utf-8");
      fieldArray[2] = new Timestamp(200 + (100 * i));
      fieldArray[3] = (int)(1200 + (100 * i));
      fieldArray[4] = (short)i;
      fieldArray[5] = (byte)i;
      fieldArray[6] = (i + "-value").getBytes("utf-8");
      fieldArray[7] = (i + "-value").getBytes("utf-8");
      fieldArray[8] = (double) i;
      fieldArray[9] = (float) i;
      fieldArray[10] = (double) i;
      fieldArray[11] = true;
      fieldArray[12] = true;
      fieldArray[13] = (i + "-value").getBytes("utf-8");
      fieldArray[14] = (i + "-value").getBytes("utf-8");
      fieldArray[15] = (i + "-value").getBytes("utf-8");
      fieldArray[16] = (i + "-value").getBytes("utf-8");
      fieldArray[17] = (i + "-value").getBytes("utf-8");
      fieldArray[18] = (i + "-value").getBytes("utf-8");
      fieldArray[19] = (i + "-value").getBytes("utf-8");
      fieldArray[20] = (i + "-value").getBytes("utf-8");
      fieldArray[21] = (i + "-value").getBytes("utf-8");
      fieldArray[22] = new BigDecimal(i);
      fieldArray[23] = new BigDecimal(i);
      fieldArray[24] = new Date(i);
      fieldArray[25] = new Time(i);
      fieldArray[26] = new Timestamp(i);
      ret.add(fieldArray);
    }
    return ret;
  }

  public static List<Object[]> createKeysForStringIndex(int count) throws UnsupportedEncodingException {
    List<Object[]> ret = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      Object[] fieldArray = new Object[1];
      fieldArray[0] = (i  + "-value").getBytes("utf-8");
      ret.add(fieldArray);
    }
    return ret;
  }

  public static List<Object[]> createKeysForBigDecimalIndex(int count) throws UnsupportedEncodingException {
    List<Object[]> ret = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      Object[] fieldArray = new Object[1];
      fieldArray[0] = new BigDecimal(i);
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
    fSchema = new FieldSchema();
    fSchema.setName("field5");
    fSchema.setType(DataType.Type.SMALLINT);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field6");
    fSchema.setType(DataType.Type.TINYINT);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field7");
    fSchema.setType(DataType.Type.CHAR);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field8");
    fSchema.setType(DataType.Type.NCHAR);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field9");
    fSchema.setType(DataType.Type.FLOAT);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field10");
    fSchema.setType(DataType.Type.REAL);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field11");
    fSchema.setType(DataType.Type.DOUBLE);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field12");
    fSchema.setType(DataType.Type.BOOLEAN);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field13");
    fSchema.setType(DataType.Type.BIT);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field14");
    fSchema.setType(DataType.Type.VARCHAR);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field15");
    fSchema.setType(DataType.Type.CLOB);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field16");
    fSchema.setType(DataType.Type.NCLOB);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field17");
    fSchema.setType(DataType.Type.LONGVARCHAR);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field18");
    fSchema.setType(DataType.Type.NVARCHAR);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field19");
    fSchema.setType(DataType.Type.LONGNVARCHAR);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field20");
    fSchema.setType(DataType.Type.LONGVARBINARY);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field21");
    fSchema.setType(DataType.Type.VARBINARY);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field22");
    fSchema.setType(DataType.Type.BLOB);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field23");
    fSchema.setType(DataType.Type.NUMERIC);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field24");
    fSchema.setType(DataType.Type.DECIMAL);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field25");
    fSchema.setType(DataType.Type.DATE);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field26");
    fSchema.setType(DataType.Type.TIME);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field27");
    fSchema.setType(DataType.Type.TIMESTAMP);
    fields.add(fSchema);
    tableSchema.setFields(fields);
    List<String> primaryKey = new ArrayList<>();
    primaryKey.add("field1");
    tableSchema.setPrimaryKey(primaryKey);
    return tableSchema;
  }

  public static TableSchema createTable2() {
    TableSchema tableSchema = new TableSchema();
    tableSchema.setName("table2");
    tableSchema.setTableId(101);
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
    tableSchema.setFields(fields);
    List<String> primaryKey = new ArrayList<>();
    primaryKey.add("field1");
    tableSchema.setPrimaryKey(primaryKey);
    return tableSchema;
  }

  public static IndexSchema createIndexSchema(TableSchema tableSchema) {
    return createIndexSchema(tableSchema, 1);
  }

  public static IndexSchema createIndexSchema(TableSchema tableSchema, int partitionCount) {
    IndexSchema indexSchema = new IndexSchema();
    indexSchema.setFields(new String[]{"field1"}, tableSchema);
    indexSchema.setIndexId(1);
    indexSchema.setIsPrimaryKey(true);
    indexSchema.setName("_primarykey");
    indexSchema.setComparators(tableSchema.getComparators(new String[]{"field1"}));

    TableSchema.Partition[] partitions = new TableSchema.Partition[partitionCount];
    for (int i = 0; i < partitionCount; i++) {
      partitions[i] = new TableSchema.Partition();
      partitions[i].setUnboundUpper(true);
    }
    indexSchema.setCurrPartitions(partitions);
    tableSchema.addIndex(indexSchema);
    return indexSchema;
  }

  public static IndexSchema createIndex2Schema(TableSchema tableSchema) {
    IndexSchema indexSchema = new IndexSchema();
    indexSchema.setFields(new String[]{"field1"}, tableSchema);
    indexSchema.setIndexId(2);
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

  public static IndexSchema createSecondaryIndexSchema(TableSchema tableSchema) {
    IndexSchema indexSchema = new IndexSchema();
    indexSchema.setFields(new String[]{
        "field1",
    "field2",
    "field3",
    "field4",
    "field5",
    "field6",
    "field7",
    "field8",
    "field9",
    "field10",
    "field11",
    "field12",
    "field13",
    "field14",
    "field15",
    "field16",
    "field17",
    "field18",
    "field19",
    "field20",
    "field21",
    "field22",
    "field23",
    "field24",
    "field25",
    "field26",
    "field27"}, tableSchema);

    indexSchema.setIndexId(2);
    indexSchema.setIsPrimaryKey(false);
    indexSchema.setName("allTypes");
    indexSchema.setComparators(tableSchema.getComparators(
        new String[]{"field1",
            "field2",
            "field3",
            "field4",
            "field5",
            "field6",
            "field7",
            "field8",
            "field9",
            "field10",
            "field11",
            "field12",
            "field13",
            "field14",
            "field15",
            "field16",
            "field17",
            "field18",
            "field19",
            "field20",
            "field21",
            "field22",
            "field23",
            "field24",
            "field25",
            "field26",
            "field27"}));

    TableSchema.Partition[] partitions = new TableSchema.Partition[1];
    partitions[0] = new TableSchema.Partition();
    partitions[0].setUnboundUpper(true);
    indexSchema.setCurrPartitions(partitions);
    tableSchema.addIndex(indexSchema);
    return indexSchema;
  }
  public static IndexSchema createStringIndexSchema(TableSchema tableSchema) {
    IndexSchema indexSchema = new IndexSchema();
    indexSchema.setFields(new String[]{
        "field2"}, tableSchema);

    indexSchema.setIndexId(3);
    indexSchema.setIsPrimaryKey(false);
    indexSchema.setName("stringIndex");
    indexSchema.setComparators(tableSchema.getComparators(
        new String[]{
            "field2"}));

    TableSchema.Partition[] partitions = new TableSchema.Partition[1];
    partitions[0] = new TableSchema.Partition();
    partitions[0].setUnboundUpper(true);
    indexSchema.setCurrPartitions(partitions);
    tableSchema.addIndex(indexSchema);
    return indexSchema;
  }

  public static IndexSchema createBigDecimalIndexSchema(TableSchema tableSchema) {
    IndexSchema indexSchema = new IndexSchema();
    indexSchema.setFields(new String[]{
        "field23"}, tableSchema);

    indexSchema.setIndexId(3);
    indexSchema.setIsPrimaryKey(false);
    indexSchema.setName("bigDecimalIndex");
    indexSchema.setComparators(tableSchema.getComparators(
        new String[]{
            "field23"}));

    TableSchema.Partition[] partitions = new TableSchema.Partition[1];
    partitions[0] = new TableSchema.Partition();
    partitions[0].setUnboundUpper(true);
    indexSchema.setCurrPartitions(partitions);
    tableSchema.addIndex(indexSchema);
    return indexSchema;
  }
}