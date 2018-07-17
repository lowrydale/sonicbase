/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.index;

import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.TestUtils;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.assertEquals;

public class IndexTest {

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
      public boolean visit(Object[] key, Object value) throws IOException {
        assertEquals(value, count.get());
        count.incrementAndGet();
        return true;
      }
    });

    count.set(3);
    index.visitHeadMap(keys.get(4), new Index.Visitor(){
      @Override
      public boolean visit(Object[] key, Object value) throws IOException {
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
      public boolean visit(Object[] key, Object value) throws IOException {
        assertEquals(value, count.get());
        count.incrementAndGet();
        return true;
      }
    });

    count.set(3);
    index.visitHeadMap(keys.get(4), new Index.Visitor(){
      @Override
      public boolean visit(Object[] key, Object value) throws IOException {
        assertEquals(value, count.get());
        count.decrementAndGet();
        return true;
      }
    });

    assertEquals(index.lastEntry().getValue(), 9);
    assertEquals(index.firstEntry().getValue(), 0);
  }
}
