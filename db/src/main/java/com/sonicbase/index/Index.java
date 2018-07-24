package com.sonicbase.index;

import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.DataType;
import com.sonicbase.schema.FieldSchema;
import com.sonicbase.schema.TableSchema;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@SuppressWarnings("squid:S1168") // I prefer to return null instead of an empty array
public class Index {
  private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("com.sonicbase.logger");

  private boolean ordered = false;

  private final Comparator[] comparators;
  private Object[] mutexes = new Object[100_000];

  private AtomicLong count = new AtomicLong();

  public Comparator[] getComparators() {
    return comparators;
  }

  public boolean isOrdered() {
    return ordered;
  }

  private ConcurrentSkipListMap<Long, Object> longSkipIndex;
  private ConcurrentSkipListMap<byte[], Object> stringSkipIndex;
  private ConcurrentSkipListMap<Object[], Object> objectSkipIndex;

  private AtomicLong size = new AtomicLong();


  private static Comparator utf8Comparator = (o1, o2) -> {
    byte[] b1 = (byte[]) o1;
    byte[] b2 = (byte[]) o2;
    if (b1 == null && b2 == null) {
      return 0;
    }
    if (b1 == null) {
      return -1;
    }
    if (b2 == null) {
      return 1;
    }
    for (int i = 0; i < Math.min(b1.length, b2.length); i++) {
      if (b1[i] < b2[i]) {
        return -1;
      }
      if (b1[i] > b2[i]) {
        return 1;
      }
    }
    if (b1.length < b2.length) {
      return -1;
    }
    if (b1.length > b2.length) {
      return 1;
    }
    return 0;
  };
  Comparator<Object[]> comparator = null;

  public Index(TableSchema tableSchema, String indexName, final Comparator[] comparators) {
    this.comparators = comparators;

    comparator = (o1, o2) -> {
      for (int i = 0; i < Math.min(o1.length, o2.length); i++) {
        if (o1[i] == null || o2[i] == null) {
          continue;
        }
        int value = comparators[i].compare(o1[i], o2[i]);
        if (value < 0) {
          return -1;
        }
        if (value > 0) {
          return 1;
        }
      }
      return 0;
    };

    for (int i = 0; i < mutexes.length; i++) {
      mutexes[i] = new Object();
    }

    String[] fields = tableSchema.getIndices().get(indexName).getFields();
    if (fields.length == 1) {
      FieldSchema fieldSchema = tableSchema.getFields().get(tableSchema.getFieldOffset(fields[0]));
      if (fieldSchema.getType() == DataType.Type.BIGINT) {
        longSkipIndex = new ConcurrentSkipListMap<>((o1, o2) -> o1 < o2 ? -1 : o1 > o2 ? 1 : 0);
      }
      else if (fieldSchema.getType() == DataType.Type.VARCHAR) {
        stringSkipIndex = new ConcurrentSkipListMap<>(utf8Comparator);
      }
      else {
        objectSkipIndex = new ConcurrentSkipListMap<>((o1, o2) -> {
          for (int i = 0; i < Math.min(o1.length, o2.length); i++) {
            if (o1[i] == null || o2[i] == null) {
              continue;
            }
            int value = comparators[i].compare(o1[i], o2[i]);
            if (value < 0) {
              return -1;
            }
            if (value > 0) {
              return 1;
            }
          }
          return 0;
        });
      }
    }
    else {
      objectSkipIndex = new ConcurrentSkipListMap<>((o1, o2) -> {
        for (int i = 0; i < Math.min(o1.length, o2.length); i++) {
          if (o1[i] == null || o2[i] == null) {
            continue;
          }
          int value = comparators[i].compare(o1[i], o2[i]);
          if (value < 0) {
            return -1;
          }
          if (value > 0) {
            return 1;
          }
        }
        return 0;
      });
    }
  }

  public static int hashCode(Object[] key) {
    int hash = 1;
    for (int i = 0; i < key.length; i++) {
      if (key[i] == null) {
        continue;
      }
      if (key[i] instanceof byte[]) {
        hash = 31 * hash + Arrays.hashCode((byte[])key[i]);
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
    if (longSkipIndex != null) {
      longSkipIndex.clear();
    }
    if (stringSkipIndex != null) {
      stringSkipIndex.clear();
    }
    if (objectSkipIndex != null) {
      objectSkipIndex.clear();
    }
    size.set(0);
    count.set(0);
  }

  public Object get(Object[] key) {
    if (longSkipIndex != null) {
      return longSkipIndex.get((long) key[0]);
    }
    else if (stringSkipIndex != null) {
      try {
        return stringSkipIndex.get((byte[]) key[0]);
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }
    else if (objectSkipIndex != null) {
      return objectSkipIndex.get(key);
    }
    return null;
  }

  public Object put(Object[] key, Object id) {
    Object ret = null;
    if (longSkipIndex != null) {
      ret = longSkipIndex.put((Long) key[0], id);
    }
    else if (stringSkipIndex != null) {
      ret = stringSkipIndex.put((byte[]) key[0], id);
    }
    else if (objectSkipIndex != null) {
      ret = objectSkipIndex.put(key, id);
    }
    if (ret == null) {
      size.incrementAndGet();
    }
    return ret;
  }

  public Object remove(Object[] key) {
    Object ret = null;
    if (longSkipIndex != null) {
      ret = longSkipIndex.remove((Long) key[0]);
    }
    else if (stringSkipIndex != null) {
      ret = stringSkipIndex.remove((byte[]) key[0]);
    }
    else if (objectSkipIndex != null) {
      ret = objectSkipIndex.remove(key);
    }
    if (ret != null) {
      size.decrementAndGet();
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

  public interface Visitor {
    boolean visit(Object[] key, Object value) throws IOException;
  }

  public static class MyEntry<T, V> implements Map.Entry<T, V> {
    private T key;
    private V value;

    public MyEntry(T key, V value) {
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
    if (longSkipIndex != null) {
      Map.Entry<Long, Object> entry = longSkipIndex.ceilingEntry((Long) key[0]);
      if (entry == null) {
        return null;
      }
      return new MyEntry<>(new Object[]{entry.getKey()}, entry.getValue());
    }
    else if (stringSkipIndex != null) {
      Map.Entry<byte[], Object> entry = stringSkipIndex.ceilingEntry((byte[]) key[0]);
      if (entry == null) {
        return null;
      }
      return new MyEntry<>(new Object[]{entry.getKey()}, entry.getValue());
    }
    else if (objectSkipIndex != null) {
      Map.Entry<Object[], Object> entry = objectSkipIndex.ceilingEntry(key);
      if (entry == null) {
        return null;
      }
      return new MyEntry<>(entry.getKey(), entry.getValue());
    }
    return null;
  }

  public List<Map.Entry<Object[], Object>> equalsEntries(Object[] key) {

    List<Map.Entry<Object[], Object>> ret = new ArrayList<>();

    if (longSkipIndex != null) {
      synchronized (this) {
        Map.Entry<Object[], Object> entry = floorEntry(key);
        if (entry == null) {
          return null;
        }
        ret.add(entry);
        while (true) {
          entry = higherEntry(entry.getKey());
          if (entry == null) {
            break;
          }
          if ((long) entry.getKey()[0] != (long) key[0]) {
            return ret;
          }
          ret.add(entry);
        }
        return ret;
      }
    }
    else if (stringSkipIndex != null) {
      synchronized (this) {
        Map.Entry<Object[], Object> entry = floorEntry(key);
        if (entry == null) {
          return null;
        }
        ret.add(entry);
        while (true) {
          entry = higherEntry(entry.getKey());
          if (entry == null) {
            break;
          }
          if (stringSkipIndex.comparator().compare((byte[]) entry.getKey()[0], (byte[]) key[0]) != 0) {
            return ret;
          }
          ret.add(entry);
        }
        return ret;
      }
    }
    else if (objectSkipIndex != null) {
      synchronized (this) {
        if (objectSkipIndex.isEmpty()) {
          return null;
        }
        Object[] lastKey = key;

        Iterator<Map.Entry<Object[], Object>> iterator = objectSkipIndex.tailMap(lastKey).entrySet().iterator();
        if (iterator.hasNext()) {
          Map.Entry<Object[], Object> entry = iterator.next();
          if (entry != null) {
            lastKey = entry.getKey();
            while (true) {
              ConcurrentNavigableMap<Object[], Object> head = objectSkipIndex.headMap(lastKey);
              if (head.isEmpty()) {
                break;
              }
              Object[] curr = head.lastKey();
              if (objectSkipIndex.comparator().compare(curr, key) == 0) {
                lastKey = curr;
              } else {
                break;
              }
            }

            ConcurrentNavigableMap<Object[], Object> head = objectSkipIndex.tailMap(lastKey);
            Set<Map.Entry<Object[], Object>> entries = head.entrySet();
            for (Map.Entry<Object[], Object> currEntry : entries) {
              if (0 != objectSkipIndex.comparator().compare(currEntry.getKey(), key)) {
                break;
              }
              lastKey = currEntry.getKey();
              Object value = objectSkipIndex.get(lastKey);
              ret.add(new MyEntry<>(lastKey, value));
            }
          }
        }
        return ret;
      }
    }
    return null;
  }


  public Map.Entry<Object[], Object> floorEntry(Object[] key) {
    if (longSkipIndex != null) {
      Map.Entry<Long, Object> entry = longSkipIndex.floorEntry((Long) key[0]);
      if (entry == null) {
        return null;
      }
      return new MyEntry<>(new Object[]{entry.getKey()}, entry.getValue());
    }
    else if (stringSkipIndex != null) {
      Map.Entry<byte[], Object> entry = stringSkipIndex.floorEntry((byte[]) key[0]);
      if (entry == null) {
        return null;
      }
      return new MyEntry<>(new Object[]{entry.getKey()}, entry.getValue());
    }
    else if (objectSkipIndex != null) {
      Map.Entry<Object[], Object> entry = objectSkipIndex.floorEntry(key);
      if (entry == null) {
        return null;
      }
      return new MyEntry<>(entry.getKey(), entry.getValue());
    }
    return null;
  }

  public Map.Entry<Object[], Object> lowerEntry(Object[] key) {
    if (longSkipIndex != null) {
      Map.Entry<Long, Object> entry = longSkipIndex.lowerEntry((Long) key[0]);
      if (entry == null) {
        return null;
      }
      return new MyEntry<>(new Object[]{entry.getKey()}, entry.getValue());
    }
    else if (stringSkipIndex != null) {
      Map.Entry<byte[], Object> entry = stringSkipIndex.lowerEntry((byte[]) key[0]);
      if (entry == null) {
        return null;
      }
      return new MyEntry<>(new Object[]{entry.getKey()}, entry.getValue());
    }
    else if (objectSkipIndex != null) {
      Map.Entry<Object[], Object> entry = objectSkipIndex.lowerEntry(key);
      if (entry == null) {
        return null;
      }
      return new MyEntry<>(entry.getKey(), entry.getValue());
    }
    return null;
  }

  public Map.Entry<Object[], Object> higherEntry(Object[] key) {
    try {
      if (longSkipIndex != null) {
        Map.Entry<Long, Object> entry = longSkipIndex.higherEntry((Long) key[0]);
        if (entry == null) {
          return null;
        }
        return new MyEntry<>(new Object[]{entry.getKey()}, entry.getValue());
      }
      else if (stringSkipIndex != null) {
        Map.Entry<byte[], Object> entry = stringSkipIndex.higherEntry((byte[]) key[0]);
        if (entry == null) {
          return null;
        }
        return new MyEntry<>(new Object[]{entry.getKey()}, entry.getValue());
      }
      else if (objectSkipIndex != null) {
        Map.Entry<Object[], Object> entry = objectSkipIndex.higherEntry(key);
        if (entry == null) {
          return null;
        }
        return new MyEntry<>(entry.getKey(), entry.getValue());
      }
      return null;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public Iterable<? extends Object> values() {
    if (longSkipIndex != null) {
      return longSkipIndex.values();
    }
    else if (stringSkipIndex != null) {
      return stringSkipIndex.values();
    }
    else if (objectSkipIndex != null) {
      return objectSkipIndex.values();
    }
    return null;
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

    visitTailMap(actualMin, new Index.Visitor() {
      @Override
      public boolean visit(Object[] key, Object value) throws IOException {
        if (maxKey != null && comparator.compare(key, maxKey) > 0) {
          return false;
        }
        currOffset.incrementAndGet();
        return true;
      }
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
      Object[] floorKey = null;
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
      final AtomicInteger curr = new AtomicInteger();
      visitTailMap(floorKey, new Index.Visitor() {
        @Override
        public boolean visit(Object[] key, Object value) throws IOException {
          if (maxKey != null && comparator.compare(key, maxKey) > 0) {
            ret.add(key);
            return false;
          }
          currOffset.incrementAndGet();
          if (currOffset.get() >= offsets.get(curr.get())) {
            ret.add(key);
            curr.incrementAndGet();
            if (curr.get() == offsets.size()) {
              return false;
            }
          }
          return true;
        }
      });
    }

    for (int i = ret.size(); i < offsets.size(); i++) {
      ret.add(lastEntry().getKey());
    }

    StringBuilder builder = new StringBuilder();
    for (Object[] key : ret) {
      builder.append(",").append(DatabaseCommon.keyToString(key));
    }
    logger.info("getKeyAtOffset - scanForKey: " +
        ", offsetInPartition=" + currOffset.get() + ", duration=" + (System.currentTimeMillis() - begin) +
        ", startKey=" + DatabaseCommon.keyToString(firstEntry().getKey()) +
        ", endKey=" + DatabaseCommon.keyToString(lastEntry().getKey()) +
        ", foundKeys=" + builder.toString() +
        ", countSkipped=" + countSkipped.get());


    return ret;
  }

  public boolean visitTailMap(Object[] key, Index.Visitor visitor) {
    try {
      if (longSkipIndex != null) {
        ConcurrentNavigableMap<Long, Object> map = longSkipIndex.tailMap((long) key[0]);
        for (Map.Entry<Long, Object> entry : map.entrySet()) {
          if (!visitor.visit(new Object[]{entry.getKey()}, entry.getValue())) {
            return false;
          }
        }
      }
      else if (stringSkipIndex != null) {
        ConcurrentNavigableMap<byte[], Object> map = stringSkipIndex.tailMap((byte[]) key[0]);
        for (Map.Entry<byte[], Object> entry : map.entrySet()) {
          if (!visitor.visit(new Object[]{entry.getKey()}, entry.getValue())) {
            return false;
          }
        }
      }
      else if (objectSkipIndex != null) {
        ConcurrentNavigableMap<Object[], Object> map = objectSkipIndex.tailMap(key);
        for (Map.Entry<Object[], Object> entry : map.entrySet()) {
          if (!visitor.visit(entry.getKey(), entry.getValue())) {
            return false;
          }
        }
      }
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
    return true;
  }

  public boolean visitHeadMap(Object[] key, Index.Visitor visitor) {
    try {
      if (longSkipIndex != null) {
        ConcurrentNavigableMap<Long, Object> map = longSkipIndex.headMap((long) key[0]).descendingMap();
        for (Map.Entry<Long, Object> entry : map.entrySet()) {
          if (!visitor.visit(new Object[]{entry.getKey()}, entry.getValue())) {
            return false;
          }
        }
      }
      else if (stringSkipIndex != null) {
        ConcurrentNavigableMap<byte[], Object> map = stringSkipIndex.headMap((byte[]) key[0]).descendingMap();
        for (Map.Entry<byte[], Object> entry : map.entrySet()) {
          if (!visitor.visit(new Object[]{entry.getKey()}, entry.getValue())) {
            return false;
          }
        }
      }
      else if (objectSkipIndex != null) {
        ConcurrentNavigableMap<Object[], Object> map = objectSkipIndex.headMap(key).descendingMap();
        for (Map.Entry<Object[], Object> entry : map.entrySet()) {
          if (!visitor.visit(entry.getKey(), entry.getValue())) {
            return false;
          }
        }
      }
      return true;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public Map.Entry<Object[], Object> lastEntry() {
    if (longSkipIndex != null) {
      Map.Entry<Long, Object> entry = longSkipIndex.lastEntry();
      if (entry == null) {
        return null;
      }
      return new MyEntry<>(new Object[]{entry.getKey()}, entry.getValue());
    }
    else if (stringSkipIndex != null) {
      Map.Entry<byte[], Object> entry = stringSkipIndex.lastEntry();
      if (entry == null) {
        return null;
      }
      return new MyEntry<>(new Object[]{entry.getKey()}, entry.getValue());
    }
    else if (objectSkipIndex != null) {
      Map.Entry<Object[], Object> entry = objectSkipIndex.lastEntry();
      if (entry == null) {
        return null;
      }
      return new MyEntry<>(entry.getKey(), entry.getValue());
    }

    return null;
  }


  public Map.Entry<Object[], Object> firstEntry() {
    if (longSkipIndex != null) {
      Map.Entry<Long, Object> entry = longSkipIndex.firstEntry();
      if (entry == null) {
        return null;
      }
      return new MyEntry<>(new Object[]{entry.getKey()}, entry.getValue());
    }
    else if (stringSkipIndex != null) {
      Map.Entry<byte[], Object> entry = stringSkipIndex.firstEntry();
      if (entry == null) {
        return null;
      }
      return new MyEntry<>(new Object[]{entry.getKey()}, entry.getValue());
    }
    else if (objectSkipIndex != null) {
      Map.Entry<Object[], Object> entry = objectSkipIndex.firstEntry();
      if (entry == null) {
        return null;
      }
      return new MyEntry<>(entry.getKey(), entry.getValue());
    }
    return null;
  }
}
