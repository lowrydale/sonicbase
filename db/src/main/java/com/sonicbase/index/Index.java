package com.sonicbase.index;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.DataType;
import com.sonicbase.schema.FieldSchema;
import com.sonicbase.schema.TableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class Index {
  private static final Logger logger = LoggerFactory.getLogger(Index.class);

  private Comparator[] comparators;
  private final Object[] mutexes = new Object[100_000];

  private final AtomicLong count = new AtomicLong();
  private IndexImpl impl;

  private final AtomicLong size = new AtomicLong();
  private Comparator<Object[]> comparator = null;

  private AtomicInteger countAdded = new AtomicInteger();
  private AtomicLong beginAdded = new AtomicLong();

  public Index() {

  }

  public Index(TableSchema tableSchema, String indexName, final Comparator[] comparators) {
    this.comparators = comparators;

    comparator = (o1, o2) -> getObjectArrayComparator(comparators, o1, o2);

    for (int i = 0; i < mutexes.length; i++) {
      mutexes[i] = new Object();
    }

    String[] fields = tableSchema.getIndices().get(indexName).getFields();
    if (fields.length == 1) {
      FieldSchema fieldSchema = tableSchema.getFields().get(tableSchema.getFieldOffset(fields[0]));
      if (fieldSchema.getType() == DataType.Type.BIGINT) {
//        impl = new NativePartitionedTreeImpl(this); //new LongIndexImpl(this); //
        impl = new LongIndexImpl(this); //
      }
      else if (fieldSchema.getType() == DataType.Type.VARCHAR) {
        impl = new StringIndexImpl(this);
      }
      else {
        impl = new ObjectIndexImpl(this, comparators);
      }
    }
    else {
      impl = new ObjectIndexImpl(this, comparators);
    }
  }

  public Comparator[] getComparators() {
    return comparators;
  }

  public Comparator<Object[]> getComparator() {
    return comparator;
  }

  int getObjectArrayComparator(Comparator[] comparators, Object[] o1, Object[] o2) {
    int keyLen = (o1.length <= o2.length) ? o1.length : o2.length;
    for (int i = 0; i < keyLen; i++) {
      int value = comparators[i].compare(o1[i], o2[i]);
      if (value != 0) {
        return value;
      }
    }
    return 0;
  }

  public static int hashCode(Object[] key) {
    int hash = 1;
    for (int i = 0; i < key.length; i++) {
      if (key[i] == null) {
        continue;
      }
      if (key[i] instanceof byte[]) {
        hash = 31 * hash + Arrays.hashCode((byte[]) key[i]);
      }
      else {
        hash = 31 * hash + key[i].hashCode();
      }
    }
    return Math.abs(hash);
  }

  public Object getMutex(Object[] key) {
    return mutexes[hashCode(key) % mutexes.length];
  }

  public void clear() {
    impl.clear();
    size.set(0);
    count.set(0);
  }

  public Object get(Object[] key) {
    return impl.get(key);
  }

  public Object put(Object[] key, Object id) {

    Object ret = impl.put(key, id);
    if (ret == null) {
      countAdded.incrementAndGet();
      if (System.currentTimeMillis() - beginAdded.get() > 60 * 1_000) {
        beginAdded.set(System.currentTimeMillis());
        countAdded.set(0);
      }
    }
    return ret;
  }

  public Object remove(Object[] key) {
    Object ret = impl.remove(key);
    if (ret != null) {
      countAdded.decrementAndGet();
      if (System.currentTimeMillis() - beginAdded.get() > 60 * 1_000) {
        beginAdded.set(System.currentTimeMillis());
        countAdded.set(0);
      }
    }
    return ret;
  }

  public long getCount() {
    return count.get();
  }

  public void addAndGetCount(int count) {
    this.count.addAndGet(count);
  }

  public void setCount(int count) {
    this.count.set(count);
  }

  AtomicLong getSizeObj() {
    return size;
  }

  public interface Visitor {
    boolean visit(Object[] key, Object value);
  }

  public static class MyEntry<T, V> implements Map.Entry<T, V> {
    private final T key;
    private V value;

    MyEntry(T key, V value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public T getKey() {
      return key;
    }

    @Override
    public V getValue() {
      return value;
    }

    @Override
    public V setValue(V value) {
      this.value = value;
      return value;
    }
  }

  public Map.Entry<Object[], Object> ceilingEntry(Object[] key) {
    return impl.ceilingEntry(key);
  }

  public List<Map.Entry<Object[], Object>> equalsEntries(Object[] key) {
    return impl.equalsEntries(key);
  }

  public Map.Entry<Object[], Object> floorEntry(Object[] key) {
    return impl.floorEntry(key);
  }

  public Map.Entry<Object[], Object> lowerEntry(Object[] key) {
    return impl.lowerEntry(key);
  }

  public Map.Entry<Object[], Object> higherEntry(Object[] key) {
    return impl.higherEntry(key);
  }

  public long getSize(final Object[] minKey, final Object[] maxKey) {
    final AtomicLong currOffset = new AtomicLong();

    final Object[] actualMin;
    if (minKey == null) {
      Map.Entry<Object[], Object> entry = firstEntry();
      if (entry == null) {
        return 0;
      }
      actualMin = entry.getKey();
    }
    else {
      actualMin = minKey;
    }

    visitTailMap(actualMin, (key, value) -> {
      if (maxKey != null && comparator.compare(key, maxKey) > 0) {
        return false;
      }
      currOffset.incrementAndGet();
      return true;
    });
    return currOffset.get();
  }

  public long anticipatedSize() {
    long size = size();

    long duration = System.currentTimeMillis() - beginAdded.get();

    double rate = countAdded.get() / duration * 1000d;

    size += (rate * 30);
    return size;
  }

  public long size() {
    return size.get();
  }

  public List<Object[]> getKeyAtOffset(final List<Long> offsets, final Object[] minKey, final Object[] maxKey) {
    long begin = System.currentTimeMillis();
    final AtomicLong countSkipped = new AtomicLong();
    final AtomicLong currOffset = new AtomicLong();
    final List<Object[]> ret = new ArrayList<>();
    if (firstEntry() != null) {
      Object[] floorKey = getFloorKey(minKey);
      final AtomicInteger curr = new AtomicInteger();
      doGetKeyAtOffset(offsets, maxKey, currOffset, ret, floorKey, curr);
    }

    for (int i = ret.size(); i < offsets.size(); i++) {
      if (lastEntry() != null) {
        ret.add(lastEntry().getKey());
      }
    }

    StringBuilder builder = new StringBuilder();
    for (Object[] key : ret) {
      builder.append(",").append(DatabaseCommon.keyToString(key));
    }
    logger.info("getKeyAtOffset - scanForKey: offsetInPartition={}, duration={}, startKey={}, endKey={}, foundKeys={}, countSkipped={}",
        currOffset.get(), (System.currentTimeMillis() - begin), firstEntry() == null ? null : DatabaseCommon.keyToString(firstEntry().getKey()),
        lastEntry() == null ? null : DatabaseCommon.keyToString(lastEntry().getKey()), builder.toString(), countSkipped.get());
    return ret;
  }

  private void doGetKeyAtOffset(List<Long> offsets, Object[] maxKey, AtomicLong currOffset, List<Object[]> ret,
                                Object[] floorKey, AtomicInteger curr) {
    visitTailMap(floorKey, (key, value) -> {
      if (maxKey != null && comparator.compare(key, maxKey) > 0) {
        ret.add(key);
        return false;
      }
      currOffset.incrementAndGet();
      if (currOffset.get() >= offsets.get(curr.get())) {
        ret.add(key);
        curr.incrementAndGet();
        return curr.get() != offsets.size();
      }
      return true;
    });
  }

  private Object[] getFloorKey(Object[] minKey) {
    Object[] floorKey;
    if (minKey != null) {
      Map.Entry<Object[], Object> entry = floorEntry(minKey);
      if (entry == null) {
        floorKey = firstEntry().getKey();
      }
      else {
        floorKey = entry.getKey();
      }
    }
    else {
      floorKey = firstEntry().getKey();
    }
    return floorKey;
  }

  public boolean visitTailMap(Object[] key, Index.Visitor visitor) {
    int blockSize = DatabaseClient.SELECT_PAGE_SIZE;
    Object[][] keys = new Object[blockSize][];
    Object[] values = new Object[blockSize];

//    Map.Entry<Object[], Object> last = impl.lastEntry();

    int countRet = impl.tailBlock(key, blockSize, true, keys, values);
    for (int i = 0; i < countRet; i++) {
      if (!visitor.visit(keys[i], values[i])) {
        return false;
      }
    }

//    long begin = System.currentTimeMillis();
//    int count = 0;
    while (countRet >= blockSize) {
      countRet = impl.tailBlock(keys[keys.length - 1], blockSize, false, keys, values);
      for (int i = 0; i < countRet; i++) {
//        if (count++ % 10_000 == 0) {
//          logger.info("progress: count={}, rate={}", count, (double)count / (System.currentTimeMillis() - begin) * 1000d);
//        }
        if (!visitor.visit(keys[i], values[i])) {
          return false;
        }
      }
//      if (countRet > 0 && (long)last.getKey()[0] < (long)keys[countRet - 1][0]) {
//        return true;
//      }
    }


//    ConcurrentNavigableMap<Object[], Object> map = objectSkipIndex.tailMap(key);
//    for (Map.Entry<Object[], Object> entry : map.entrySet()) {
//      if (!visitor.visit(entry.getKey(), entry.getValue())) {
//        return false;
//      }
//    }
    return true;


//    try {
//      return impl.visitTailMap(key, visitor);
//    }
//    catch (IOException e) {
//      throw new DatabaseException(e);
//    }
  }

  public boolean visitHeadMap(Object[] key, Index.Visitor visitor) {
    int blockSize = DatabaseClient.SELECT_PAGE_SIZE;
    Object[][] keys = new Object[blockSize][];
    Object[] values = new Object[blockSize];

//    Map.Entry<Object[], Object> first = impl.firstEntry();

    int countRet = impl.headBlock(key, blockSize, true, keys, values);
    for (int i = 0; i < countRet; i++) {
      if (!visitor.visit(keys[i], values[i])) {
        return false;
      }
    }

    while (countRet >= blockSize) {
      countRet = impl.headBlock(keys[keys.length - 1], blockSize, false, keys, values);
      for (int i = 0; i < countRet; i++) {
        if (!visitor.visit(keys[i], values[i])) {
          return false;
        }
      }
//      if (countRet > 0 && (long)first.getKey()[0] > (long)keys[countRet - 1][0]) {
//        return true;
//      }
    }
    return true;
//
//    try {
//      return impl.visitHeadMap(key, visitor);
//    }
//    catch (IOException e) {
//      throw new DatabaseException(e);
//    }
  }

  public Map.Entry<Object[], Object> lastEntry() {
    return impl.lastEntry();
  }

  public Map.Entry<Object[], Object> firstEntry() {
    return impl.firstEntry();
  }
}
