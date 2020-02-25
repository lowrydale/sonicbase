package com.sonicbase.index;

import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class ObjectIndexImpl implements IndexImpl {
  private final Index index;

  private final ConcurrentSkipListMap<Object[], Object> objectSkipIndex;
  private final Comparator<Object[]> comparator;

  ObjectIndexImpl(Index index, Comparator[] comparators) {
    this.index = index;
    this.comparator = (o1, o2) -> {
      int keyLen = (o1.length <= o2.length) ? o1.length : o2.length;
      for (int i = 0; i < keyLen; i++) {
        int value = comparators[i].compare(o1[i], o2[i]);
        if (value != 0) {
          return value;
        }
      }
      return 0;
    };
    //don't make this a lambda
    objectSkipIndex = new ConcurrentSkipListMap<>(comparator);
  }

  public void clear() {
    objectSkipIndex.clear();
  }

  @Override
  public void delete() {

  }

  public Object get(Object[] key) {
    return objectSkipIndex.get(key);
  }

  @Override
  public void put(Object[][] key, Object[] value) {

  }

  public Object put(Object[] key, Object id) {
    Object ret = objectSkipIndex.put(key, id);
    if (ret == null) {
      index.getSizeObj().incrementAndGet();
    }
    return ret;
  }

  public Object remove(Object[] key) {
    Object ret = objectSkipIndex.remove(key);
    if (ret != null) {
      index.getSizeObj().decrementAndGet();
    }
    return ret;
  }

  public Map.Entry<Object[], Object> ceilingEntry(Object[] key) {
    Map.Entry<Object[], Object> entry = objectSkipIndex.ceilingEntry(key);
    if (entry == null) {
      return null;
    }
    return new Index.MyEntry<>(entry.getKey(), entry.getValue());
  }

  public List<Map.Entry<Object[], Object>> equalsEntries(Object[] key) {
    List<Map.Entry<Object[], Object>> ret = new ArrayList<>();

    synchronized (this) {
      if (objectSkipIndex.isEmpty()) {
        return null;
      }

      Iterator<Map.Entry<Object[], Object>> iterator = objectSkipIndex.tailMap(key).entrySet().iterator();
      if (iterator.hasNext()) {
        Map.Entry<Object[], Object> entry = iterator.next();
        if (entry != null) {
          doEqualsEntries(key, ret, entry);
        }
      }
      return ret;
    }
  }

  private void doEqualsEntries(Object[] key, List<Map.Entry<Object[], Object>> ret, Map.Entry<Object[], Object> entry) {
    Object[] lastKey;
    lastKey = entry.getKey();
    while (true) {
      ConcurrentNavigableMap<Object[], Object> head = objectSkipIndex.headMap(lastKey);
      if (head.isEmpty()) {
        break;
      }
      Object[] curr = head.lastKey();
      if (objectSkipIndex.comparator().compare(curr, key) == 0) {
        lastKey = curr;
      }
      else {
        break;
      }
    }

    ConcurrentNavigableMap<Object[], Object> head = objectSkipIndex.tailMap(lastKey);
    Set<Map.Entry<Object[], Object>> entries = head.entrySet();
    for (Map.Entry<Object[], Object> currEntry : entries) {
      if (0 != objectSkipIndex.comparator().compare(currEntry.getKey(), key)) {
        return;
      }
      lastKey = currEntry.getKey();
      Object value = objectSkipIndex.get(lastKey);
      ret.add(new Index.MyEntry<>(lastKey, value));
    }
  }

  public Map.Entry<Object[], Object> floorEntry(Object[] key) {
    Map.Entry<Object[], Object> entry = objectSkipIndex.floorEntry(key);
    if (entry == null) {
      return null;
    }
    return new Index.MyEntry<>(entry.getKey(), entry.getValue());
  }

  public Map.Entry<Object[], Object> lowerEntry(Object[] key) {
    Map.Entry<Object[], Object> entry = objectSkipIndex.lowerEntry(key);
    if (entry == null) {
      return null;
    }
    return new Index.MyEntry<>(entry.getKey(), entry.getValue());
  }

  public Map.Entry<Object[], Object> higherEntry(Object[] key) {
    Map.Entry<Object[], Object> entry = objectSkipIndex.higherEntry(key);
    if (entry == null) {
      return null;
    }
    return new Index.MyEntry<>(entry.getKey(), entry.getValue());
  }

  public int tailBlock(Object[] key, int count, boolean first, Object[][] keys, long[] values) {
    ConcurrentNavigableMap<Object[], Object> map = objectSkipIndex.tailMap(key);
    int offset = 0;
    for (Map.Entry<Object[], Object> entry : map.entrySet()) {
      if (offset == 0 && !first && comparator.compare(entry.getKey(), key) == 0) {
        continue;
      }

      keys[offset] = entry.getKey();
      values[offset] = (long)entry.getValue();
      if (offset++ >= count - 1) {
        break;
      }
    }
    return offset;
  }

  public int headBlock(Object[] key, int count, boolean first, Object[][] keys, long[] values) {
    ConcurrentNavigableMap<Object[], Object> map = objectSkipIndex.headMap(key).descendingMap();
    int offset = 0;
    for (Map.Entry<Object[], Object> entry : map.entrySet()) {
      if (offset == 0 && !first && comparator.compare(entry.getKey(), key) == 0) {
        continue;
      }
      keys[offset] = entry.getKey();
      values[offset] = (long)entry.getValue();
      if (offset++ >= count - 1) {
        break;
      }
    }
    return offset;
  }


  public Map.Entry<Object[], Object> lastEntry() {
    Map.Entry<Object[], Object> entry = objectSkipIndex.lastEntry();
    if (entry == null) {
      return null;
    }
    return new Index.MyEntry<>(entry.getKey(), entry.getValue());
  }

  public Map.Entry<Object[], Object> firstEntry() {
    Map.Entry<Object[], Object> entry = objectSkipIndex.firstEntry();
    if (entry == null) {
      return null;
    }
    return new Index.MyEntry<>(entry.getKey(), entry.getValue());
  }
}
