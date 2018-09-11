package com.sonicbase.index;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class ObjectIndexImpl implements IndexImpl {
  private final Index index;

  private final ConcurrentSkipListMap<Object[], Object> objectSkipIndex;

  ObjectIndexImpl(Index index, Comparator[] comparators) {
    this.index = index;
    //don't make this a lambda
    objectSkipIndex = new ConcurrentSkipListMap<>(new Comparator<Object[]>() {
      @Override
      public int compare(Object[] o1, Object[] o2) {
        int keyLen = (o1.length <= o2.length) ? o1.length : o2.length;
        for (int i = 0; i < keyLen; i++) {
          int value = comparators[i].compare(o1[i], o2[i]);
          if (value != 0) {
            return value;
          }
        }
        return 0;
      }
    });
  }

  public void clear() {
    objectSkipIndex.clear();
  }

  public Object get(Object[] key) {
    return objectSkipIndex.get(key);
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

  public Iterable<Object> values() {
    return objectSkipIndex.values();
  }


  public boolean visitTailMap(Object[] key, Index.Visitor visitor) throws IOException {
    ConcurrentNavigableMap<Object[], Object> map = objectSkipIndex.tailMap(key);
    for (Map.Entry<Object[], Object> entry : map.entrySet()) {
      if (!visitor.visit(entry.getKey(), entry.getValue())) {
        return false;
      }
    }
    return true;
  }

  public boolean visitHeadMap(Object[] key, Index.Visitor visitor) throws IOException {
    ConcurrentNavigableMap<Object[], Object> map = objectSkipIndex.headMap(key).descendingMap();
    for (Map.Entry<Object[], Object> entry : map.entrySet()) {
      if (!visitor.visit(entry.getKey(), entry.getValue())) {
        return false;
      }
    }
    return true;
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
