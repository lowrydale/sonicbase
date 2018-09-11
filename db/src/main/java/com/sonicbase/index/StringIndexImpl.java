package com.sonicbase.index;

import org.apache.hadoop.io.WritableComparator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class StringIndexImpl implements IndexImpl {
  private final Index index;

  private final ConcurrentSkipListMap<byte[], Object> stringSkipIndex;

  StringIndexImpl(Index index) {
    this.index = index;
    //don't make this a lambda
    stringSkipIndex = new ConcurrentSkipListMap<>(new Comparator<byte[]>() {
      @Override
      public int compare(byte[] o1, byte[] o2) {
        return WritableComparator.compareBytes(o1, 0, o1.length, o2, 0, o2.length);
      }
    });
  }

  public void clear() {
    stringSkipIndex.clear();
  }

  public Object get(Object[] key) {
    return stringSkipIndex.get((byte[]) key[0]);
  }

  public Object put(Object[] key, Object id) {
    Object ret = stringSkipIndex.put((byte[]) key[0], id);
    if (ret == null) {
      index.getSizeObj().incrementAndGet();
    }
    return ret;
  }

  public Object remove(Object[] key) {
    Object ret = stringSkipIndex.remove((byte[]) key[0]);
    if (ret != null) {
      index.getSizeObj().decrementAndGet();
    }
    return ret;
  }

  public Map.Entry<Object[], Object> ceilingEntry(Object[] key) {
    Map.Entry<byte[], Object> entry = stringSkipIndex.ceilingEntry((byte[]) key[0]);
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
        if (stringSkipIndex.comparator().compare((byte[]) entry.getKey()[0], (byte[]) key[0]) != 0) {
          return ret;
        }
        ret.add(entry);
      }
      return ret;
    }
  }

  public Map.Entry<Object[], Object> floorEntry(Object[] key) {
    Map.Entry<byte[], Object> entry = stringSkipIndex.floorEntry((byte[]) key[0]);
    if (entry == null) {
      return null;
    }
    return new Index.MyEntry<>(new Object[]{entry.getKey()}, entry.getValue());
  }

  public Map.Entry<Object[], Object> lowerEntry(Object[] key) {
    Map.Entry<byte[], Object> entry = stringSkipIndex.lowerEntry((byte[]) key[0]);
    if (entry == null) {
      return null;
    }
    return new Index.MyEntry<>(new Object[]{entry.getKey()}, entry.getValue());
  }

  public Map.Entry<Object[], Object> higherEntry(Object[] key) {
    Map.Entry<byte[], Object> entry = stringSkipIndex.higherEntry((byte[]) key[0]);
    if (entry == null) {
      return null;
    }
    return new Index.MyEntry<>(new Object[]{entry.getKey()}, entry.getValue());
  }

  public Iterable<Object> values() {
    return stringSkipIndex.values();
  }


  public boolean visitTailMap(Object[] key, Index.Visitor visitor) throws IOException {
    ConcurrentNavigableMap<byte[], Object> map = stringSkipIndex.tailMap((byte[]) key[0]);
    for (Map.Entry<byte[], Object> entry : map.entrySet()) {
      if (!visitor.visit(new Object[]{entry.getKey()}, entry.getValue())) {
        return false;
      }
    }
    return true;
  }

  public boolean visitHeadMap(Object[] key, Index.Visitor visitor) throws IOException {
    ConcurrentNavigableMap<byte[], Object> map = stringSkipIndex.headMap((byte[]) key[0]).descendingMap();
    for (Map.Entry<byte[], Object> entry : map.entrySet()) {
      if (!visitor.visit(new Object[]{entry.getKey()}, entry.getValue())) {
        return false;
      }
    }
    return true;
  }

  public Map.Entry<Object[], Object> lastEntry() {
    Map.Entry<byte[], Object> entry = stringSkipIndex.lastEntry();
    if (entry == null) {
      return null;
    }
    return new Index.MyEntry<>(new Object[]{entry.getKey()}, entry.getValue());
  }


  public Map.Entry<Object[], Object> firstEntry() {
    Map.Entry<byte[], Object> entry = stringSkipIndex.firstEntry();
    if (entry == null) {
      return null;
    }
    return new Index.MyEntry<>(new Object[]{entry.getKey()}, entry.getValue());
  }
}
