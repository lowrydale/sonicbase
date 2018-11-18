package com.sonicbase.index;

import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.TestUtils;
import org.testng.annotations.Test;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.assertEquals;

public class IndexTest {

  @Test
  public void testBatchOrig() {
    ConcurrentHashMap<Long, Object> origMap = new ConcurrentHashMap<>();
    long offset = 0;
    while (true) {
      origMap.put(offset++, offset);
      if (offset % 1_000 == 0) {
        System.out.println("count=" + offset);
      }
    }
  }

  private final int batchSize = 100;

  private class Entry {
    long key;
    Object address;
  }
  private class InnerMap {
    private Entry[] keys = new Entry[batchSize];

  }

  @Test
  public void testBatchNew() {
    ConcurrentHashMap<Long, Object> newMap = new ConcurrentHashMap<>();
    long offset = 0;
    while (true) {
      Entry[] keys = new Entry[batchSize];
      for (long i = offset; i < offset + batchSize; i++) {
        Entry entry = new Entry();
        keys[(int) (i - offset)] = entry;
        entry.key = offset;
        entry.address = offset;

        if (i % 1_000 == 0) {
          System.out.println("count=" + i);
        }
      }
      newMap.put(offset, keys);
      offset += batchSize;
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
