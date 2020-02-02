/* © 2020 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.index;

import com.sonicbase.schema.DataType;
import com.sonicbase.schema.FieldSchema;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.*;

import static org.testng.AssertJUnit.assertEquals;

public class NativeSkipListMapTest {

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
    tableSchema.setFields(fields);
    List<String> primaryKey = new ArrayList<>();
    primaryKey.add("field1");
    primaryKey.add("field2");
    tableSchema.setPrimaryKey(primaryKey);
    return tableSchema;
  }

  public static IndexSchema createIndexSchema(TableSchema tableSchema) {
    return createIndexSchema(tableSchema, 1);
  }

  public static IndexSchema createIndexSchema(TableSchema tableSchema, int partitionCount) {
    IndexSchema indexSchema = new IndexSchema();
    indexSchema.setFields(new String[]{"field1", "field2"}, tableSchema);
    indexSchema.setIndexId(1);
    indexSchema.setIsPrimaryKey(true);
    indexSchema.setName("_primarykey");
    indexSchema.setComparators(tableSchema.getComparators(new String[]{"field1", "field2"}));

    TableSchema.Partition[] partitions = new TableSchema.Partition[partitionCount];
    for (int i = 0; i < partitionCount; i++) {
      partitions[i] = new TableSchema.Partition();
      partitions[i].setUnboundUpper(true);
    }
    indexSchema.setCurrPartitions(partitions);
    tableSchema.addIndex(indexSchema);
    return indexSchema;
  }

  @Test
  public void test() {
    TableSchema tableSchema = createTable();
    createIndexSchema(tableSchema);

    Index index = new Index(9010, new HashMap<>(), tableSchema, "_primarykey",
        new Comparator[]{DataType.getLongComparator(), DataType.getCharArrayComparator()});
    NativeSkipListMapImpl map = new NativeSkipListMapImpl(8080, index);

    map.put(new Object[]{0L, getChars("0")}, 100L);
    map.put(new Object[]{0L, getChars("1")}, 101L);
    map.put(new Object[]{0L, getChars("2")}, 102L);
    map.put(new Object[]{0L, getChars("3")}, 103L);

    Object[][] keys = new Object[100][];
    long[] values = new long[100];
    map.headBlock(new Object[]{0L, getChars("0")}, 100, true, keys, values);

    //assertEquals(keys[0][2], "0");

    assertEquals(103L, map.remove(new Object[]{0L, getChars("3")}));
    assertEquals(102L, map.remove(new Object[]{0L, getChars("2")}));
    assertEquals(101L, map.get(new Object[]{0L, getChars("1")}));

    map.put(new Object[]{0L, getChars("4")}, 104L);
    map.put(new Object[]{0L, getChars("5")}, 105L);
  }

  private Object getChars(String s) {
    char[] dst = new char[s.length()];
    s.getChars(0, s.length(), dst, 0);
    return dst;
  }


  @Test
  public void testDuplicate() throws ExecutionException, InterruptedException {
    ThreadPoolExecutor executor = new ThreadPoolExecutor(32, 32, 10_000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(100), new ThreadPoolExecutor.CallerRunsPolicy());
    TableSchema tableSchema = createTable();
    createIndexSchema(tableSchema);

    Index index = new Index(9010, new HashMap<>(), tableSchema, "_primarykey",
        new Comparator[]{DataType.getLongComparator(), DataType.getCharArrayComparator()});
    NativeSkipListMapImpl map = new NativeSkipListMapImpl(8080, index);

    for (int i = 0; i < 10_000; i++) {
      map.put(new Object[]{(long)i, getChars("0")}, (long)i);
    }

    Object[][] keys = new Object[100][];
    long[] values = new long[100];
    int count = map.tailBlock(new Object[]{0L, getChars("0")}, 100, true, keys, values);

    assertEquals(100, count);
    assertEquals(keys[0][0], 0L);

    map.clear();

    for (int i = 0; i < 10_000; i++) {
      map.put(new Object[]{(long)i, getChars("0")}, (long)i);
    }

    keys = new Object[100][];
    values = new long[100];
    count = map.tailBlock(new Object[]{0L, getChars("0")}, 100, true, keys, values);

    assertEquals(100, count);
    assertEquals(keys[0][0], 0L);

    List<Future> futures = new ArrayList<>();
    for (int i = 10; i < 10_000; i++) {
      final int offset = i;
      futures.add(executor.submit(new Callable(){
        @Override
        public Object call() throws Exception {
          map.remove(new Object[]{(long)offset, getChars("0")});
          return null;
        }
      }));
    }

    for (Future future : futures) {
      future.get();
    }
    keys = new Object[100][];
    values = new long[100];
    count = map.tailBlock(new Object[]{0L, getChars("0")}, 100, true, keys, values);

    assertEquals(10, count);
    assertEquals(keys[0][0], 0L);

    for (int i = 0; i < 10_000; i++) {
      map.put(new Object[]{(long)i, getChars("0")}, (long)i);
    }

    keys = new Object[100][];
    values = new long[100];
    count = map.tailBlock(new Object[]{0L, getChars("0")}, 100, true, keys, values);

    assertEquals(100, count);
    assertEquals(keys[0][0], 0L);

    executor.shutdownNow();
  }
}

