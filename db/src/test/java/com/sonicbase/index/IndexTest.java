package com.sonicbase.index;

import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.TestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.testng.Assert.assertEquals;

public class IndexTest {

  @BeforeClass
  public void beforeClass() {
    System.setProperty("log4j.configuration", "test-log4j.xml");
  }

  @Test
  public void testDelete() {
    TableSchema tableSchema = TestUtils.createTable();
    IndexSchema indexSchema = TestUtils.createIndexSchema(tableSchema);
    List<Object[]> keys = TestUtils.createKeys(10_000);
    Index index = new Index(tableSchema, indexSchema.getName(), indexSchema.getComparators());

    for (int i = 0; i < keys.size(); i++) {
      index.put(keys.get(i), (long)i);
    }

    for (int i = keys.size() - 1; i >= 0; i--) {
      assertEquals(index.remove(keys.get(i)), (long)i);
    }
  }

  @Test
  public void testGetKeyAtOffset()  {
    TableSchema tableSchema = TestUtils.createTable();
    IndexSchema indexSchema = TestUtils.createIndexSchema(tableSchema);
    Index index = new Index(tableSchema, indexSchema.getName(), indexSchema.getComparators());

    int count = 0;
    for (int i = 0; i < 4; i++) {
      for (int j = 1; j < 100_000 + i; j++) {
        Object[] key = new Object[]{(long)(100_000 * i + j)};
        index.put(key, (long)100_000 * i + j);
        count++;
        //System.out.println(100_000 * i + j);
      }
    }

    System.out.println("loaded records");

    List<Long> offsets = new ArrayList<>();
    offsets.add(count / 4L);
    offsets.add(count / 4L * 2);
    offsets.add(count / 4L * 3);
    offsets.add(count / 4L * 4);
    List<Object[]> keys = index.getKeyAtOffset(offsets, index.firstEntry().getKey(), index.lastEntry().getKey());
    int offset = 0;
    for (Object[] key : keys) {
      if (offset == 0) {
        assertEquals((long) key[0], 100_001);
      }
      if (offset == 1) {
        assertEquals((long) key[0], 200_001);
      }
      if (offset == 2) {
        assertEquals((long) key[0], 300_001);
      }
      if (offset == 3) {
        assertEquals((long) key[0], 400_001);
      }
      offset++;
      System.out.println(DatabaseCommon.keyToString(key));
    }
  }

  @Test
  public void test() throws UnsupportedEncodingException {
    TableSchema tableSchema = TestUtils.createTable();
    IndexSchema indexSchema = TestUtils.createStringIndexSchema(tableSchema);
    List<Object[]> keys = TestUtils.createKeysForStringIndex(10);
    Index index = new Index(tableSchema, indexSchema.getName(), indexSchema.getComparators());

    index.put(keys.get(0), 100);
    assertEquals(index.get(keys.get(0)), 100);

    index.clear();
    assertEquals(index.size(), 0);
    index.put(keys.get(0), 100);
    index.remove(keys.get(0));
    assertEquals(index.size(), 0);

    for (int i = 0; i < keys.size(); i++) {
      index.put(keys.get(i), i);
    }

    assertEquals(index.ceilingEntry(keys.get(4)).getValue(), 4);
    assertEquals(index.equalsEntries(keys.get(4)).get(0).getValue(), 4);
    assertEquals(index.floorEntry(keys.get(4)).getValue(), 4);
    assertEquals(index.lowerEntry(keys.get(4)).getValue(), 3);
    assertEquals(index.higherEntry(keys.get(4)).getValue(), 5);

    final AtomicInteger count = new AtomicInteger(4);
    index.visitTailMap(keys.get(4), new Index.Visitor(){
      @Override
      public boolean visit(Object[] key, Object value) {
        assertEquals(value, count.get());
        count.incrementAndGet();
        return true;
      }
    });

    count.set(3);
    index.visitHeadMap(keys.get(4), new Index.Visitor(){
      @Override
      public boolean visit(Object[] key, Object value) {
        assertEquals(value, count.get());
        count.decrementAndGet();
        return true;
      }
    });

    assertEquals(index.lastEntry().getValue(), 9);
    assertEquals(index.firstEntry().getValue(), 0);
  }


  @Test
  public void testHeadMap() {
    TableSchema tableSchema = TestUtils.createTable();
    IndexSchema indexSchema = TestUtils.createIndexSchema(tableSchema);
    List<Object[]> keys = TestUtils.createKeys(10000);
    Index index = new Index(tableSchema, indexSchema.getName(), indexSchema.getComparators());

    for (int i = 0; i < keys.size(); i++) {
      index.put(keys.get(i), (long) i);
    }

    AtomicLong offset = new AtomicLong(keys.size() / 2);
    index.visitHeadMap(keys.get(keys.size() / 2 - 1), (k, v) -> {
      assertEquals((long)k[0], (long)offset.getAndDecrement() * 100, "value=" + v);
      return true;
    });
  }

  @Test
  public void getKeyAtOffset() {
    TableSchema tableSchema = TestUtils.createTable();
    IndexSchema indexSchema = TestUtils.createIndexSchema(tableSchema);
    List<Object[]> keys = TestUtils.createKeys(10000);
    Index index = new Index(tableSchema, indexSchema.getName(), indexSchema.getComparators());

    for (int i = 0; i < keys.size() / 2; i++) {
      index.put(keys.get(i), (long)i);
    }

    List<Long> offsets = new ArrayList<>();
    offsets.add(1000L);
    offsets.add(2000L);
    offsets.add(4000L);
    List<Object[]> retKeys = index.getKeyAtOffset(offsets, index.firstEntry().getKey(), index.lastEntry().getKey());
    assertEquals(retKeys.get(0)[0], 100100L);
    assertEquals(retKeys.get(1)[0], 200100L);
    assertEquals(retKeys.get(2)[0], 400100L);

    for (int i = keys.size() / 2; i < keys.size(); i++) {
      index.put(keys.get(i), (long)i);
    }

    offsets = new ArrayList<>();
    offsets.add(1000L);
    offsets.add(5000L);
    offsets.add(9000L);
    retKeys = index.getKeyAtOffset(offsets, index.firstEntry().getKey(), index.lastEntry().getKey());
    assertEquals(retKeys.get(0)[0], 100100L);
    assertEquals(retKeys.get(1)[0], 500100L);
    assertEquals(retKeys.get(2)[0], 900100L);

  }

  @Test
  public void testBigDecimal() throws UnsupportedEncodingException {
    TableSchema tableSchema = TestUtils.createTable();
    IndexSchema indexSchema = TestUtils.createBigDecimalIndexSchema(tableSchema);
    List<Object[]> keys = TestUtils.createKeysForBigDecimalIndex(10);
    Index index = new Index(tableSchema, indexSchema.getName(), indexSchema.getComparators());

    index.put(keys.get(0), 100);
    assertEquals(index.get(keys.get(0)), 100);

    index.clear();
    assertEquals(index.size(), 0);
    index.put(keys.get(0), 100);
    index.remove(keys.get(0));
    assertEquals(index.size(), 0);

    for (int i = 0; i < keys.size(); i++) {
      index.put(keys.get(i), i);
    }

    assertEquals(index.ceilingEntry(keys.get(4)).getValue(), 4);
    assertEquals(index.equalsEntries(keys.get(4)).get(0).getValue(), 4);
    assertEquals(index.floorEntry(keys.get(4)).getValue(), 4);
    assertEquals(index.lowerEntry(keys.get(4)).getValue(), 3);
    assertEquals(index.higherEntry(keys.get(4)).getValue(), 5);

    final AtomicInteger count = new AtomicInteger(4);
    index.visitTailMap(keys.get(4), new Index.Visitor(){
      @Override
      public boolean visit(Object[] key, Object value) {
        assertEquals(value, count.get());
        count.incrementAndGet();
        return true;
      }
    });

    count.set(3);
    index.visitHeadMap(keys.get(4), new Index.Visitor(){
      @Override
      public boolean visit(Object[] key, Object value) {
        assertEquals(value, count.get());
        count.decrementAndGet();
        return true;
      }
    });

    assertEquals(index.lastEntry().getValue(), 9);
    assertEquals(index.firstEntry().getValue(), 0);
  }
}
