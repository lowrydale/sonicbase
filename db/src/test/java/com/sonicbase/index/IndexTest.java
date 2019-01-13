package com.sonicbase.index;

import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.TestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.testng.Assert.*;

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
    Index index = new Index(9010, tableSchema, indexSchema.getName(), indexSchema.getComparators());

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
    Index index = new Index(9010, tableSchema, indexSchema.getName(), indexSchema.getComparators());

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
  public void testPartitioning() {
    List<List> keys = new ArrayList<>();
    for (int i = 0; i < 16; i++) {
      keys.add(new ArrayList<>());
    }
    Random rand = new Random(System.currentTimeMillis());
    for (int i = 0; i < 16 * 1000; i++) {
      long key = rand.nextLong();
      long hash1 = Math.abs(key);
      if (hash1 % 4 == 0L) {
        keys.get((int)(Math.abs(key ^ (key >>> 32)) % 16)).add(key);
      }
    }
    for (int i = 0; i < 16; i++) {
      System.out.println("part=" + i + ", count=" + keys.get(i).size());
    }
  }

  @Test
  public void testLargeLong() {
    TableSchema tableSchema = TestUtils.createTable();
    IndexSchema indexSchema = TestUtils.createIndexSchema(tableSchema);
    Index index = new Index(9010, tableSchema, indexSchema.getName(), indexSchema.getComparators());
    Set<Long> keys = new HashSet<>();
    Random rand = new Random(System.currentTimeMillis());
    for (long i = 0; i < 1500; i++) {
      long key = Math.abs(rand.nextLong());
      keys.add(key);
      index.put(new Object[]{key}, (long)100);
    }

    Set<Long> keysCopy = new HashSet<>(keys);
    {
      AtomicInteger count = new AtomicInteger();
      index.visitTailMap(new Object[]{0l}, new Index.Visitor() {
        @Override
        public boolean visit(Object[] key, Object value) {
          assertTrue(keys.remove(key[0]));
          count.incrementAndGet();
          return true;
        }
      });
      assertEquals(count.get(), 1500);
      assertEquals(keys.size(), 0);
    }

    List<Long> toRemove = new ArrayList<>();
    int count1 = 0;
    for (long key : keysCopy) {
      toRemove.add(key);
      if (count1++ >= 9) {
        break;
      }
    }

    for (long key : toRemove) {
      keysCopy.remove(key);
      index.remove(new Object[]{key});
    }

//    assertNull(index.get(new Object[]{2306398204483007890L + 100}));
//    assertNull(index.get(new Object[]{2306398204483007890L + 101}));
//    assertNull(index.get(new Object[]{2306398204483007890L + 100 + 9}));
//    assertNotNull(index.get(new Object[]{2306398204483007890L + 100 + 10}));

    {
      AtomicBoolean first = new AtomicBoolean(true);
      final AtomicInteger count = new AtomicInteger();
      final AtomicInteger actualCount = new AtomicInteger();
      index.visitTailMap(new Object[]{0l}, new Index.Visitor() {
        @Override
        public boolean visit(Object[] key, Object value) {
          int adjustment = 0;
          if (count.get() >= 100) {
            adjustment += 10;
          }
          if (first.get() && count.get() == 100) {
            //count.addAndGet(-10);
            first.set(false);
          }
          assertTrue(keysCopy.remove(key[0]));
//          assertEquals((long) key[0], 2306398204483007890L + (count.get() + adjustment));
          System.out.println(count + ", key=" + key[0]);
          count.incrementAndGet();
          actualCount.incrementAndGet();
          return true;
        }
      });
      assertEquals(actualCount.get(), 1490);
      assertEquals(keysCopy.size(), 0);
    }

  }

  @Test
  public void test() throws UnsupportedEncodingException {
    TableSchema tableSchema = TestUtils.createTable();
    IndexSchema indexSchema = TestUtils.createStringIndexSchema(tableSchema);
    List<Object[]> keys = TestUtils.createKeysForStringIndex(10);
    Index index = new Index(9010, tableSchema, indexSchema.getName(), indexSchema.getComparators());

    index.put(keys.get(0), (long)100);
    assertEquals(index.get(keys.get(0)), (long)100);

    index.clear();
    assertEquals(index.size(), (long)0);
    index.put(keys.get(0), (long)100);
    index.remove(keys.get(0));
    assertEquals(index.size(), (long)0);

    for (int i = 0; i < keys.size(); i++) {
      index.put(keys.get(i), (long)i);
    }

    assertEquals(index.ceilingEntry(keys.get(4)).getValue(), (long)4);
    assertEquals(index.equalsEntries(keys.get(4)).get(0).getValue(), (long)4);
    assertEquals(index.floorEntry(keys.get(4)).getValue(), (long)4);
    assertEquals(index.lowerEntry(keys.get(4)).getValue(), (long)3);
    assertEquals(index.higherEntry(keys.get(4)).getValue(), (long)5);

    final AtomicInteger count = new AtomicInteger(4);
    index.visitTailMap(keys.get(4), new Index.Visitor(){
      @Override
      public boolean visit(Object[] key, Object value) {
        assertEquals(value, (long)count.get());
        count.incrementAndGet();
        return true;
      }
    });

    count.set(3);
    index.visitHeadMap(keys.get(4), new Index.Visitor(){
      @Override
      public boolean visit(Object[] key, Object value) {
        assertEquals(value, (long)count.get());
        count.decrementAndGet();
        return true;
      }
    });

    assertEquals(index.lastEntry().getValue(), (long)9);
    assertEquals(index.firstEntry().getValue(), (long)0);
  }


  @Test
  public void testHeadMap() {
    TableSchema tableSchema = TestUtils.createTable();
    IndexSchema indexSchema = TestUtils.createIndexSchema(tableSchema);
    List<Object[]> keys = TestUtils.createKeys(10000);
    Index index = new Index(9010, tableSchema, indexSchema.getName(), indexSchema.getComparators());

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
  public void testTailMap() {
    TableSchema tableSchema = TestUtils.createTable();
    IndexSchema indexSchema = TestUtils.createIndexSchema(tableSchema);
    List<Object[]> keys = TestUtils.createKeys(10000);
    Index index = new Index(9010, tableSchema, indexSchema.getName(), indexSchema.getComparators());

    for (int i = 0; i < keys.size(); i++) {
      index.put(keys.get(i), (long) i);
    }

    AtomicLong offset = new AtomicLong(2);
    index.visitTailMap(keys.get(2), (k, v) -> {
      assertEquals((long)k[0], (long)(offset.getAndIncrement() + 2) * 100, "value=" + v);
      return true;
    });
  }


  @Test
  public void put() {
    TableSchema tableSchema = TestUtils.createTable();
    IndexSchema indexSchema = TestUtils.createIndexSchema(tableSchema);
    List<Object[]> keys = TestUtils.createKeys(10000);
    Index index = new Index(9010, tableSchema, indexSchema.getName(), indexSchema.getComparators());

    for (int i = 0; i < keys.size(); i++) {
      index.put(keys.get(i), (long) i);
    }

    final AtomicInteger count = new AtomicInteger();
    index.visitTailMap(new Object[]{(long)0}, new Index.Visitor(){
      @Override
      public boolean visit(Object[] key, Object value) {
        count.incrementAndGet();
        return true;
      }
    });

    assertEquals(count.get(), 10000);
  }

  @Test
  public void last() {
    TableSchema tableSchema = TestUtils.createTable();
    IndexSchema indexSchema = TestUtils.createIndexSchema(tableSchema);
    List<Object[]> keys = TestUtils.createKeys(10000);
    Index index = new Index(9010, tableSchema, indexSchema.getName(), indexSchema.getComparators());

    for (int i = 0; i < keys.size(); i++) {
      index.put(keys.get(i), (long) i);
    }

    assertEquals(index.lastEntry().getKey()[0], (long)1000100);
  }

  @Test
  public void getKeyAtOffset() {
    TableSchema tableSchema = TestUtils.createTable();
    IndexSchema indexSchema = TestUtils.createIndexSchema(tableSchema);
    List<Object[]> keys = TestUtils.createKeys(10000);
    Index index = new Index(190, tableSchema, indexSchema.getName(), indexSchema.getComparators());

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
    Index index = new Index(9010, tableSchema, indexSchema.getName(), indexSchema.getComparators());

    index.put(keys.get(0), (long)100);
    assertEquals(index.get(keys.get(0)), (long)100);

    index.clear();
    assertEquals(index.size(), (long)0);
    index.put(keys.get(0), (long)100);
    index.remove(keys.get(0));
    assertEquals(index.size(), (long)0);

    for (int i = 0; i < keys.size(); i++) {
      index.put(keys.get(i), (long)i);
    }

    assertEquals(index.ceilingEntry(keys.get(4)).getValue(), (long)4);
    assertEquals(index.equalsEntries(keys.get(4)).get(0).getValue(), (long)4);
    assertEquals(index.floorEntry(keys.get(4)).getValue(), (long)4);
    assertEquals(index.lowerEntry(keys.get(4)).getValue(), (long)3);
    assertEquals(index.higherEntry(keys.get(4)).getValue(), (long)5);

    final AtomicInteger count = new AtomicInteger(4);
    index.visitTailMap(keys.get(4), new Index.Visitor(){
      @Override
      public boolean visit(Object[] key, Object value) {
        assertEquals(value, (long)count.get());
        count.incrementAndGet();
        return true;
      }
    });

    count.set(3);
    index.visitHeadMap(keys.get(4), new Index.Visitor(){
      @Override
      public boolean visit(Object[] key, Object value) {
        assertEquals(value, (long)count.get());
        count.decrementAndGet();
        return true;
      }
    });

    assertEquals(index.lastEntry().getValue(), (long)9);
    assertEquals(index.firstEntry().getValue(), (long)0);
  }

  class SortedListNode {
    SortedListNode next;
    SortedListNode prev;
    long key;
    long value;
    int partition;
  };


  class SortedList {
    SortedListNode head = null;
    SortedListNode tail = null;
    boolean ascending = true;

    SortedList(boolean ascending) {
      this.ascending = ascending;
    }

    int size() {
      int ret = 0;
      SortedListNode curr = head;
      while (curr != null) {
        curr = curr.next;
        ret++;
      }
      return ret;
    }
    void push(SortedListNode node) {
      if (ascending) {
        pushAscending(node);
      }
      else {
        pushDescending(node);
      }
    }

    void pushAscending(SortedListNode node) {
      if (tail == null || head == null) {
        head = tail = node;
        return;
      }
//	 		printf("called push: depth=%d\n", size());
//	 		fflush(stdout);
      SortedListNode curr = tail;
      int compareCount = 0;
      while (curr != null) {
        compareCount++;
        int cmp = Long.compare(node.key, curr.key);
        if (cmp > 0) {
          SortedListNode next = curr.next;
          if (next != null) {
            node.next = next;
            next.prev = node;
          }
          if (curr == tail) {
            tail.next = node;
            node.prev = tail;
            tail = node;
          }
          curr.next = node;
          node.prev = curr;
//			    	if (compareCount > 1) {
//			    		printf("compareCount=%d", compareCount);
//			    		fflush(stdout);
//					}
          return;
        }
        else {
          curr = curr.prev;
        }
      }

//			    	if (compareCount > 1) {
//			    		printf("compareCount=%d", compareCount);
//			    		fflush(stdout);
//					}

        head.prev = node;
        node.next = head;
        head = node;
    }

    void pushDescending(SortedListNode node) {
      if (tail == null || head == null) {
        head = tail = node;
        return;
      }
//	 		printf("called push: depth=%d\n", size());
//	 		fflush(stdout);
      SortedListNode curr = head;

      int compareCount = 0;
      while (curr != null) {
        compareCount++;
        int cmp = Long.compare(node.key, curr.key);
        if (cmp < 0) {
          SortedListNode prev = curr.prev;
          if (prev != null) {
            node.prev = prev;
            prev.next = node;
          }
          if (curr == head) {
            head.prev = node;
            node.next = head;
            //tail = curr;
            head = node;
          }
          curr.prev = node;
          node.next = curr;
//			    	if (compareCount > 1) {
//			    		printf("compareCount=%d", compareCount);
//			    		fflush(stdout);
//					}
          return;
        }
        else {
            curr = curr.next;
        }
      }

//			    	if (compareCount > 1) {
//			    		printf("compareCount=%d", compareCount);
//			    		fflush(stdout);
//					}

        tail.next = node;
        node.prev = tail;
        tail = node;
    }

    SortedListNode pop() {
      if (ascending) {
        return popAscending();
      }
      return popDescending();
    }

    SortedListNode popAscending() {
      if (head == null) {
        return null;
      }

      SortedListNode ret = null;

      ret = head;

      SortedListNode next = head.next;

      if (next != null) {
        next.prev = null;
      }

      head = next;
      if (next == null) {
        head = tail = null;
      }

      return ret;
    }

    SortedListNode popDescending() {
      if (tail == null) {
        return null;
      }
      SortedListNode ret = tail;
      SortedListNode prev = tail.prev;

      if (prev != null) {
        prev.next = null;
      }
      tail = prev;
      if (prev == null) {
        head = tail = null;
      }

      return ret;
    }
  };



  @Test
  public void testSortedList() {

    SortedList list = new SortedList(true);
    SortedListNode node = new SortedListNode();
    node.key = 100;
    list.push(node);

    node = new SortedListNode();
    node.key = 200;
    list.push(node);

    node = new SortedListNode();
    node.key = 300;
    list.push(node);

    node = list.pop();
    assertEquals(node.key, 100);

    node = new SortedListNode();
    node.key = 75;
    list.push(node);

    node = new SortedListNode();
    node.key = 500;
    list.push(node);

    node = list.pop();
    assertEquals(node.key, 75);

    node = list.pop();
    assertEquals(node.key, 200);

    node = list.pop();
    assertEquals(node.key, 300);

    node = list.pop();
    assertEquals(node.key, 500);
  }

  @Test
  public void testSortedListDescending2() {

    SortedList list = new SortedList(false);
    SortedListNode node = new SortedListNode();
    node.key = 100;
    list.push(node);

    node = new SortedListNode();
    node.key = 200;
    list.push(node);

    node = new SortedListNode();
    node.key = 300;
    list.push(node);

    node = new SortedListNode();
    node.key = 75;
    list.push(node);

    node = new SortedListNode();
    node.key = 500;
    list.push(node);

    node = new SortedListNode();
    node.key = 50;
    list.push(node);

    node = new SortedListNode();
    node.key = 600;
    list.push(node);

    node = list.pop();
    assertEquals(node.key, 600);

    node = list.pop();
    assertEquals(node.key, 500);

    node = list.pop();
    assertEquals(node.key, 300);

    node = list.pop();
    assertEquals(node.key, 200);

    node = list.pop();
    assertEquals(node.key, 100);

    node = list.pop();
    assertEquals(node.key, 75);

    node = list.pop();
    assertEquals(node.key, 50);
  }


  @Test
  public void testSortedListDescending() {

    SortedList list = new SortedList(false);
    SortedListNode node = new SortedListNode();
    node.key = 500000;
    list.push(node);

    node = new SortedListNode();
    node.key = 499700;
    list.push(node);

    node = new SortedListNode();
    node.key = 499800;
    list.push(node);

    node = list.pop();
    assertEquals(node.key, 500000);

    node = list.pop();
    assertEquals(node.key, 499800);

    node = list.pop();
    assertEquals(node.key, 499700);

  }
}
