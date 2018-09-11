package com.sonicbase.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class LongIndexImpl implements IndexImpl {
  private final Index index;

  private final ConcurrentSkipListMap<Long, Object> longSkipIndex;

  public LongIndexImpl(Index index) {
    this.index = index;
    longSkipIndex = new ConcurrentSkipListMap<>((o1, o2) -> o1 < o2 ? -1 : o1 > o2 ? 1 : 0);
  }

  public void clear() {
    longSkipIndex.clear();
  }

  public Object get(Object[] key) {
    return longSkipIndex.get((long) key[0]);
  }

  public Object put(Object[] key, Object id) {
    Object ret = longSkipIndex.put((Long) key[0], id);
    if (ret == null) {
      index.getSizeObj().incrementAndGet();
    }
    return ret;
  }

  public Object remove(Object[] key) {
    Object ret = longSkipIndex.remove((Long) key[0]);
    if (ret != null) {
      index.getSizeObj().decrementAndGet();
    }
    return ret;
  }

  public Map.Entry<Object[], Object> ceilingEntry(Object[] key) {
    Map.Entry<Long, Object> entry = longSkipIndex.ceilingEntry((Long) key[0]);
    if (entry == null) {
      return null;
    }
    return new Index.MyEntry<>(new Object[]{entry.getKey()}, entry.getValue());
  }

  public List<Map.Entry<Object[], Object>> equalsEntries(Object[] key) {
    List<Map.Entry<Object[], Object>> ret = new ArrayList<>();

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

  public Map.Entry<Object[], Object> floorEntry(Object[] key) {
    Map.Entry<Long, Object> entry = longSkipIndex.floorEntry((Long) key[0]);
    if (entry == null) {
      return null;
    }
    return new Index.MyEntry<>(new Object[]{entry.getKey()}, entry.getValue());
  }

  public Map.Entry<Object[], Object> lowerEntry(Object[] key) {
    Map.Entry<Long, Object> entry = longSkipIndex.lowerEntry((Long) key[0]);
    if (entry == null) {
      return null;
    }
    return new Index.MyEntry<>(new Object[]{entry.getKey()}, entry.getValue());
  }

  public Map.Entry<Object[], Object> higherEntry(Object[] key) {
    Map.Entry<Long, Object> entry = longSkipIndex.higherEntry((Long) key[0]);
    if (entry == null) {
      return null;
    }
    return new Index.MyEntry<>(new Object[]{entry.getKey()}, entry.getValue());
  }

  public Iterable<Object> values() {
    return longSkipIndex.values();
  }


  public boolean visitTailMap(Object[] key, Index.Visitor visitor) throws IOException {
    ConcurrentNavigableMap<Long, Object> map = longSkipIndex.tailMap((long) key[0]);
    for (Map.Entry<Long, Object> entry : map.entrySet()) {
      if (!visitor.visit(new Object[]{entry.getKey()}, entry.getValue())) {
        return false;
      }
    }
    return true;
  }

  public boolean visitHeadMap(Object[] key, Index.Visitor visitor) throws IOException {
    ConcurrentNavigableMap<Long, Object> map = longSkipIndex.headMap((long) key[0]).descendingMap();
    for (Map.Entry<Long, Object> entry : map.entrySet()) {
      if (!visitor.visit(new Object[]{entry.getKey()}, entry.getValue())) {
        return false;
      }
    }
    return true;
  }

  public Map.Entry<Object[], Object> lastEntry() {
    Map.Entry<Long, Object> entry = longSkipIndex.lastEntry();
    if (entry == null) {
      return null;
    }
    return new Index.MyEntry<>(new Object[]{entry.getKey()}, entry.getValue());
  }


  public Map.Entry<Object[], Object> firstEntry() {
    Map.Entry<Long, Object> entry = longSkipIndex.firstEntry();
    if (entry == null) {
      return null;
    }
    return new Index.MyEntry<>(new Object[]{entry.getKey()}, entry.getValue());
  }
}
