package com.sonicbase.index;

import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.DataType;
import com.sonicbase.schema.FieldSchema;
import com.sonicbase.schema.TableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class Index {
  private static final Logger logger = LoggerFactory.getLogger(Index.class);

  private final Comparator[] comparators;
  private final Object[] mutexes = new Object[100_000];

  private final AtomicLong count = new AtomicLong();
  private final IndexImpl impl;

  private final AtomicLong size = new AtomicLong();
  private Comparator<Object[]> comparator = null;

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
        impl = new LongIndexImpl(this);
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
    return impl.put(key, id);
  }

  public Object remove(Object[] key) {
    return impl.remove(key);
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
    boolean visit(Object[] key, Object value) throws IOException;
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

  public Iterable<Object> values() {
    return impl.values();
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
      ret.add(lastEntry().getKey());
    }

    StringBuilder builder = new StringBuilder();
    for (Object[] key : ret) {
      builder.append(",").append(DatabaseCommon.keyToString(key));
    }
    logger.info("getKeyAtOffset - scanForKey: offsetInPartition={}, duration={}, startKey={}, endKey={}, foundKeys={}, countSkipped={}",
        currOffset.get(), (System.currentTimeMillis() - begin), DatabaseCommon.keyToString(firstEntry().getKey()),
        DatabaseCommon.keyToString(lastEntry().getKey()), builder.toString(), countSkipped.get());
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
    try {
      return impl.visitTailMap(key, visitor);
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public boolean visitHeadMap(Object[] key, Index.Visitor visitor) {
    try {
      return impl.visitHeadMap(key, visitor);
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public Map.Entry<Object[], Object> lastEntry() {
    return impl.lastEntry();
  }

  public Map.Entry<Object[], Object> firstEntry() {
    return impl.firstEntry();
  }
}
