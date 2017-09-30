package com.sonicbase.index;

import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.DataType;
import com.sonicbase.schema.FieldSchema;
import com.sonicbase.schema.TableSchema;
import it.unimi.dsi.fastutil.longs.Long2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectSortedMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectSortedMap;
import it.unimi.dsi.fastutil.objects.ObjectBidirectionalIterator;
import it.unimi.dsi.fastutil.objects.ObjectSortedSet;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class Index {
  private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("com.sonicbase.logger");

  private boolean ordered = false;

  private final Comparator[] comparators;
  private Object[] mutexes = new Object[100_000];

  private boolean fastUtil = false;

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "EI_EXPOSE_REP", justification = "copying the returned data is too slow")
  public Comparator[] getComparators() {
    return comparators;
  }

  public boolean isOrdered() {
    return ordered;
  }

  private Long2ObjectAVLTreeMap<Object> longIndex;
  private Object2ObjectAVLTreeMap<byte[], Object> stringIndex;
  private Object2ObjectAVLTreeMap<Object[], Object> objectIndex;

  private ConcurrentSkipListMap<Long, Object> longSkipIndex;
  private ConcurrentSkipListMap<byte[], Object> stringSkipIndex;
  private ConcurrentSkipListMap<Object[], Object> objectSkipIndex;

  private AtomicLong size = new AtomicLong();


  private static Comparator utf8Comparator = new Comparator() {
    @Override
    public int compare(Object o1, Object o2) {
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
    }
  };
  Comparator<Object[]> comparator = null;

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "EI_EXPOSE_REP2", justification = "copying the passed in data is too slow")
  @SuppressWarnings("PMD.ArrayIsStoredDirectly") //copying the passed in data is too slow
  public Index(TableSchema tableSchema, String indexName, final Comparator[] comparators) {
    this.comparators = comparators;

    comparator = new Comparator<Object[]>() {
      @Override
      public int compare(Object[] o1, Object[] o2) {
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
      }
    };

    for (int i = 0; i < mutexes.length; i++) {
      mutexes[i] = new Object();
    }

    String[] fields = tableSchema.getIndices().get(indexName).getFields();
    if (fields.length == 1) {
      FieldSchema fieldSchema = tableSchema.getFields().get(tableSchema.getFieldOffset(fields[0]));
      if (fieldSchema.getType() == DataType.Type.BIGINT) {
        if (fastUtil) {
          longIndex = new Long2ObjectAVLTreeMap<>();
        }
        else {
          longSkipIndex = new ConcurrentSkipListMap<>(new Comparator<Long>() {
            @Override
            public int compare(Long o1, Long o2) {

              return o1 < o2 ? -1 : o1 > o2 ? 1 : 0;
            }
          });
        }

      }
      else if (fieldSchema.getType() == DataType.Type.VARCHAR) {
        if (fastUtil) {
          stringIndex = new Object2ObjectAVLTreeMap<>(utf8Comparator);
        }
        else {
          stringSkipIndex = new ConcurrentSkipListMap<>(utf8Comparator);
        }
      }
      else {
        if (fastUtil) {
          objectIndex = new Object2ObjectAVLTreeMap<>(new Comparator<Object[]>() {
            @Override
            public int compare(Object[] o1, Object[] o2) {
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
            }
          });
        }
        else {
          objectSkipIndex = new ConcurrentSkipListMap<>(new Comparator<Object[]>() {
            @Override
            public int compare(Object[] o1, Object[] o2) {
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
            }
          });
        }
      }
    }
    else {
      if (fastUtil) {
        objectIndex = new Object2ObjectAVLTreeMap<>(new Comparator<Object[]>() {
          @Override
          public int compare(Object[] o1, Object[] o2) {
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
          }
        });
      }
      else {
        objectSkipIndex = new ConcurrentSkipListMap<>(new Comparator<Object[]>() {
          @Override
          public int compare(Object[] o1, Object[] o2) {
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
          }
        });
      }
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
    if (longIndex != null) {
      synchronized (this) {
        longIndex.clear();
      }
    }
    else if (stringIndex != null) {
      synchronized (this) {
        stringIndex.clear();
      }
    }
    else if (objectIndex != null) {
      synchronized (this) {
        objectIndex.clear();
      }
    }
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
  }

  public boolean iterate(Index.Visitor visitor) throws IOException {
    if (longIndex != null) {
      Map.Entry<Object[], Object> entry = firstEntry();
      while (entry != null) {
        visitor.visit(entry.getKey(), entry.getValue());
        entry = higherEntry(entry.getKey());
      }
    }
    else if (stringIndex != null) {
      Map.Entry<Object[], Object> entry = firstEntry();
      while (entry != null) {
        visitor.visit(entry.getKey(), entry.getValue());
        entry = higherEntry(entry.getKey());
      }
    }
    else if (objectIndex != null) {
      Map.Entry<Object[], Object> entry = firstEntry();
      while (entry != null) {
        visitor.visit(entry.getKey(), entry.getValue());
        entry = higherEntry(entry.getKey());
      }
    }
    else if (longSkipIndex != null) {
      for (Map.Entry<Long, Object> entry : longSkipIndex.entrySet()) {
        visitor.visit(new Object[]{entry.getKey()}, entry.getValue());
      }
    }
    else if (stringSkipIndex != null) {
      for (Map.Entry<byte[], Object> entry : stringSkipIndex.entrySet()) {
        visitor.visit(new Object[]{entry.getKey()}, entry.getValue());
      }
    }
    else if (objectSkipIndex != null) {
      for (Map.Entry<Object[], Object> entry : objectSkipIndex.entrySet()) {
        visitor.visit(entry.getKey(), entry.getValue());
      }
    }
    return true;
  }

  public Object get(Object[] key) {
    if (longIndex != null) {
      synchronized (this) {
        return longIndex.get(key[0]);
      }
    }
    else if (stringIndex != null) {
      synchronized (this) {
        return stringIndex.get((byte[]) key[0]/*((String)key[0]).getBytes("utf-8")*/);
      }
    }
    else if (objectIndex != null) {
      synchronized (this) {
        return objectIndex.get(key);
      }
    }

    if (longSkipIndex != null) {
      return longSkipIndex.get((long) key[0]);
    }
    else if (stringSkipIndex != null) {
      return stringSkipIndex.get((byte[]) key[0]);
    }
    else if (objectSkipIndex != null) {
      return objectSkipIndex.get(key);
    }
    return null;
  }

  public Object put(Object[] key, Object id) {
    Object ret = null;
    if (longIndex != null) {
      synchronized (this) {
        ret = longIndex.put((long) key[0], id);
      }
    }
    else if (stringIndex != null) {
      synchronized (this) {
        ret = stringIndex.put((byte[]) key[0]/*((String)key[0]).getBytes("utf-8")*/, id);
      }
    }
    else if (objectIndex != null) {
      synchronized (this) {
        ret = objectIndex.put(key, id);
      }
    }
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
    if (longIndex != null) {
      synchronized (this) {
        ret = longIndex.remove((long) key[0]);
      }
    }
    else if (stringIndex != null) {
      synchronized (this) {
        ret = stringIndex.remove((byte[]) key[0] /*((String)key[0]).getBytes("utf-8")*/);
      }
    }
    else if (objectIndex != null) {
      synchronized (this) {
        ret = objectIndex.remove(key);
      }
    }

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

  public Object unsafePutIfAbsent(Object[] key, Object id) {
    Object ret = null;
    if (longIndex != null) {
      synchronized (this) {
        ret = longIndex.putIfAbsent((long) key[0], id);
      }
    }
    else if (stringIndex != null) {
      synchronized (this) {
        ret = stringIndex.putIfAbsent((byte[]) key[0] /*((String) key[0]).getBytes("utf-8")*/, id);
      }
    }
    else if (objectIndex != null) {
      synchronized (this) {
        ret = objectIndex.putIfAbsent(key, id);
      }
    }
    if (longSkipIndex != null) {
      ret = longSkipIndex.putIfAbsent((long) key[0], id);
    }
    else if (objectSkipIndex != null) {
      ret = objectSkipIndex.putIfAbsent(key, id);
    }
    else if (stringSkipIndex != null) {
      ret = stringSkipIndex.putIfAbsent((byte[]) key[0], id);
    }
    if (ret == null) {
      size.incrementAndGet();
    }
    return ret;
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
    if (longIndex != null) {
      synchronized (this) {
        if (longIndex.isEmpty()) {
          return null;
        }
        Long2ObjectSortedMap tail = longIndex.tailMap((Long) key[0]);
        if (tail.isEmpty()) {
          return null;
        }
        long firstKey = tail.firstLongKey();
        Object value = longIndex.get(firstKey);
        return new MyEntry<>(new Object[]{firstKey}, value);
      }
    }
    else if (stringIndex != null) {
      synchronized (this) {
        if (stringIndex.isEmpty()) {
          return null;
        }
        Object2ObjectSortedMap tail = stringIndex.tailMap((byte[]) key[0]/*((String)key[0]).getBytes("utf-8")*/);
        if (tail.isEmpty()) {
          return null;
        }
        byte[] firstKey = (byte[]) tail.firstKey();
        Object value = stringIndex.get(firstKey);
        return new MyEntry<>(new Object[]{firstKey/*new String(firstKey, "utf-8")*/}, value);
      }
    }
    else if (objectIndex != null) {
      synchronized (this) {
        if (objectIndex.isEmpty()) {
          return null;
        }
        Object2ObjectSortedMap<Object[], Object> tail = objectIndex.tailMap(key);
        if (tail.isEmpty()) {
          return null;
        }
        Object[] firstKey = tail.firstKey();
        Object value = objectIndex.get(firstKey);
        return new MyEntry<>(firstKey, value);

        //        if (objectIndex.isEmpty()) {
        //          return null;
        //        }
        //        boolean haveKey = false;
        //        Object[] lastKey = key;
        //        while (true) {
        //          Object2LongSortedMap<Object[]> head = objectIndex.tailMap(lastKey);
        //          if (head.isEmpty()) {
        //            break;
        //          }
        //          Object[] curr = head.firstKey();
        //          if (objectIndex.comparator().compare(curr, key) != 0) {
        //            break;
        //          }
        //          lastKey = curr;
        //          haveKey = true;
        //        }
        //
        //        if (!haveKey) {
        //          Map.Entry<Object[], Long> entry = lowerEntry(lastKey);
        //          if (entry != null) {
        //            lastKey = entry.getKey();
        //            while (true) {
        //              Object2LongSortedMap<Object[]> head = objectIndex.tailMap(lastKey);
        //              if (head.isEmpty()) {
        //                break;
        //              }
        //              Object[] curr = head.firstKey();
        //              if (objectIndex.comparator().compare(curr, key) == 0) {
        //                lastKey = curr;
        //              }
        //              else {
        //                break;
        //              }
        //            }
        //          }
        //        }
        //        Long value = objectIndex.get(lastKey);
        //        return new MyEntry<>(lastKey, value);
      }
    }

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

    if (longIndex != null) {
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
    else if (stringIndex != null) {
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
          if (stringIndex.comparator().compare((byte[]) entry.getKey()[0], (byte[]) key[0]) != 0) {
            return ret;
          }
          ret.add(entry);
        }
        return ret;
      }
    }
    else if (objectIndex != null) {
      synchronized (this) {
        if (objectIndex.isEmpty()) {
          return null;
        }
        boolean haveKey = false;
        Object[] lastKey = key;
        if (!haveKey) {
          Iterator<Map.Entry<Object[], Object>> iterator = objectIndex.tailMap(lastKey).entrySet().iterator();
          if (iterator.hasNext()) {
            Map.Entry<Object[], Object> entry = iterator.next();//higherEntry(lastKey);
            if (entry != null) {
              lastKey = entry.getKey();
              while (true) {
                Object2ObjectSortedMap<Object[], Object> head = objectIndex.headMap(lastKey);
                if (head.isEmpty()) {
                  break;
                }
                Object[] curr = head.lastKey();
                if (objectIndex.comparator().compare(curr, key) == 0) {
                  lastKey = curr;
                } else {
                  break;
                }
              }

              Object2ObjectSortedMap<Object[], Object> head = objectIndex.tailMap(lastKey);
              ObjectSortedSet<Map.Entry<Object[], Object>> entries = head.entrySet();
              for (Map.Entry<Object[], Object> currEntry : entries) {
                if (0 != objectIndex.comparator().compare(currEntry.getKey(), key)) {
                  break;
                }
                lastKey = currEntry.getKey();
                Object value = objectIndex.get(lastKey);
                ret.add(new MyEntry<>(lastKey, value));
              }
            }
          }
          return ret;
        }
      }
    }
    else if (longSkipIndex != null) {
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
        boolean haveKey = false;
        Object[] lastKey = key;
        if (!haveKey) {
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
    }
    return null;
  }


  public Map.Entry<Object[], Object> floorEntry(Object[] key) {
    if (longIndex != null) {
      synchronized (this) {
        if (longIndex.isEmpty()) {
          return null;
        }
        Object value = longIndex.get((Long) key[0]);
        if (value != null) {
          return new MyEntry<>(key, value);
        }
        Long2ObjectSortedMap head = longIndex.headMap((Long) key[0]);
        if (head.isEmpty()) {
          return null;
        }
        long lastKey = head.lastLongKey();
        value = longIndex.get(lastKey);
        return new MyEntry<>(new Object[]{lastKey},  value);
      }
    }
    else if (stringIndex != null) {
      synchronized (this) {
        if (stringIndex.isEmpty()) {
          return null;
        }
        Object value = stringIndex.get((byte[]) key[0]/*((String)key[0]).getBytes("utf-8")*/);
        if (value != null) {
          return new MyEntry<>(key, value);
        }
        Object2ObjectSortedMap head = stringIndex.headMap((byte[]) key[0]/*((String)key[0]).getBytes("utf-8")*/);
        if (head.isEmpty()) {
          return null;
        }
        byte[] lastKey = (byte[]) head.lastKey();
        value = stringIndex.get(lastKey);
        return new MyEntry<>(new Object[]{(byte[]) lastKey/*new String(lastKey, "utf-8")*/}, value);
      }
    }
    else if (objectIndex != null) {
      synchronized (this) {
        if (objectIndex.isEmpty()) {
          return null;
        }
        boolean haveKey = false;
        Object[] lastKey = key;
        //        while (true) {
        //          Object2LongSortedMap<Object[]> head = objectIndex.headMap(lastKey);
        //          if (head.isEmpty()) {
        //            break;
        //          }
        //          Object[] curr = head.lastKey();
        //          if (objectIndex.comparator().compare(curr, key) != 0) {
        //            break;
        //          }
        //          lastKey = curr;
        //          haveKey = true;
        //        }

        if (!haveKey) {
          Map.Entry<Object[], Object> entry = objectIndex.tailMap(lastKey).entrySet().first();//higherEntry(lastKey);
          if (entry != null) {
            lastKey = entry.getKey();
            while (true) {
              Object2ObjectSortedMap<Object[], Object> head = objectIndex.headMap(lastKey);
              if (head.isEmpty()) {
                break;
              }
              Object[] curr = head.lastKey();
              if (objectIndex.comparator().compare(curr, key) == 0) {
                lastKey = curr;
              }
              else {
                break;
              }
            }
          }
        }
        Object value = objectIndex.get(lastKey);
        return new MyEntry<>(lastKey, value);
      }
    }

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

  public Map.Entry<Object[], Object>[] lowerEntries(Object[] key, Map.Entry<Object[], Object>[] ret) {
    if (longIndex != null) {
      synchronized (this) {
        Long2ObjectSortedMap head = longIndex.headMap((Long) key[0]);
        ObjectSortedSet<Map.Entry<Long, Object>> entries = head.entrySet();
        if (head.isEmpty()) {
          return null;
        }
        int offset = 0;
        ObjectBidirectionalIterator<Map.Entry<Long, Object>> iterator = entries.iterator(entries.last());
        while (iterator.hasPrevious()) {
          Map.Entry<Long, Object> entry = iterator.previous();
          Long lastKey = entry.getKey();
          ret[offset++] = new MyEntry<>(new Object[]{lastKey}, entry.getValue());
          if (offset >= ret.length) {
            break;
          }
        }
        return ret;
      }
    }
    else if (stringIndex != null) {
      synchronized (this) {
        Object2ObjectSortedMap head = stringIndex.headMap((byte[]) key[0]);
        ObjectSortedSet<Map.Entry<byte[], Object>> entries = head.entrySet();
        if (head.isEmpty()) {
          return null;
        }
        int offset = 0;
        ObjectBidirectionalIterator<Map.Entry<byte[], Object>> iterator = entries.iterator(entries.last());
        while (iterator.hasPrevious()) {
          Map.Entry<byte[], Object> entry = iterator.previous();
          byte[] lastKey = entry.getKey();
          ret[offset++] = new MyEntry<>(new Object[]{lastKey}, entry.getValue());
          if (offset >= ret.length) {
            break;
          }
        }
        return ret;
      }
    }
    else if (objectIndex != null) {
      synchronized (this) {
        Object2ObjectSortedMap head = objectIndex.headMap(key);
        ObjectSortedSet<Map.Entry<Object[], Object>> entries = head.entrySet();
        if (head.isEmpty()) {
          return null;
        }
        int offset = 0;
        ObjectBidirectionalIterator<Map.Entry<Object[], Object>> iterator = entries.iterator(entries.last());
        //Map.Entry<Object[], Long> entry = entries.last();
        //Object[] lastKey = entry.getKey();
        //ret[offset++] = new MyEntry<>(lastKey, entry.getValue());
        while (iterator.hasPrevious()) {
          Map.Entry<Object[], Object> entry = iterator.previous();
          Object[] lastKey = entry.getKey();
          ret[offset++] = new MyEntry<>(lastKey, entry.getValue());
          if (offset >= ret.length) {
            break;
          }
        }
        return ret;
      }
    }

    if (longSkipIndex != null) {
      for (int i = 0; i < ret.length; i++) {
        Map.Entry<Long, Object> entry = longSkipIndex.lowerEntry((Long) key[0]);
        if (entry == null) {
          return ret;
        }
        ret[i] = new MyEntry<>(new Object[]{entry.getKey()}, entry.getValue());
        key = new Object[]{entry.getKey()};
      }
      return ret;
    }
    else if (stringSkipIndex != null) {
      for (int i = 0; i < ret.length; i++) {
        Map.Entry<byte[], Object> entry = stringSkipIndex.lowerEntry((byte[]) key[0]);
        if (entry == null) {
          return ret;
        }
        ret[i] = new MyEntry<>(new Object[]{entry.getKey()}, entry.getValue());
        key = new Object[]{entry.getKey()};
      }
      return ret;
    }
    else if (objectSkipIndex != null) {
      for (int i = 0; i < ret.length; i++) {
        Map.Entry<Object[], Object> entry = objectSkipIndex.lowerEntry(key);
        if (entry == null) {
          return ret;
        }
        ret[i] = new MyEntry<>(entry.getKey(), entry.getValue());
        key = entry.getKey();
      }
      return ret;
    }
    return null;
  }

  public Map.Entry<Object[], Object> lowerEntry(Object[] key) {
    if (longIndex != null) {
      synchronized (this) {
        Long2ObjectSortedMap head = longIndex.headMap((Long) key[0]);
        if (head.isEmpty()) {
          return null;
        }
        long lastKey = head.lastLongKey();
        Object value = longIndex.get(lastKey);
        return new MyEntry<>(new Object[]{lastKey}, value);
      }
    }
    else if (stringIndex != null) {
      synchronized (this) {
        Object2ObjectSortedMap head = stringIndex.headMap((byte[]) key[0]/*((String)key[0]).getBytes("utf-8")*/);
        if (head.isEmpty()) {
          return null;
        }
        byte[] lastKey = (byte[]) head.lastKey();
        Object value = stringIndex.get(lastKey);
        return new MyEntry<>(new Object[]{(byte[]) lastKey/*new String(lastKey, "utf-8")*/}, value);
      }
    }
    else if (objectIndex != null) {
      synchronized (this) {
        Object2ObjectSortedMap<Object[], Object> head = objectIndex.headMap(key);
        if (head.isEmpty()) {
          return null;
        }
        Object[] lastKey = head.lastKey();
        Object value = objectIndex.get(lastKey);
        return new MyEntry<>(lastKey, value);
      }
    }

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

  public Map.Entry<Object[], Object>[] higherEntries(Object[] key, Map.Entry<Object[], Object>[] ret) {
    try {
      if (longIndex != null) {
        synchronized (this) {
          Long2ObjectSortedMap head = longIndex.tailMap((Long) key[0]);
          ObjectSortedSet<Map.Entry<Long, Object>> entries = head.entrySet();
          if (head.isEmpty()) {
            return null;
          }
          try {
            int offset = 0;
            Long lastKey = head.firstLongKey();
            //if (lastKey.equals(key[0])) {
            lastKey = null;
            Long currKey = null;
            for (Map.Entry<Long, Object> entry : entries) {
              if (currKey == null) {
                currKey = entry.getKey();
              }
              else {
                lastKey = entry.getKey();
                ret[offset++] = new MyEntry<>(new Object[]{lastKey}, entry.getValue());
                if (offset >= ret.length) {
                  break;
                }
              }
            }
            //}
            return ret;
          }
          catch (NoSuchElementException e) {
            return null;
          }
        }
      }
      else if (stringIndex != null) {
        synchronized (this) {
          Object2ObjectSortedMap head = stringIndex.tailMap((byte[]) key[0]);
          ObjectSortedSet<Map.Entry<byte[], Object>> entries = head.entrySet();
          if (head.isEmpty()) {
            return null;
          }
          try {
            int offset = 0;
            byte[] lastKey = (byte[]) head.firstKey();
            //if (lastKey.equals(key[0])) {
            lastKey = null;
            byte[] currKey = null;
            for (Map.Entry<byte[], Object> entry : entries) {
              if (currKey == null) {
                currKey = (byte[]) entry.getKey();
              }
              else {
                lastKey = entry.getKey();
                ret[offset++] = new MyEntry<>(new Object[]{lastKey}, entry.getValue());
                if (offset >= ret.length) {
                  break;
                }
              }
            }
            //}
            return ret;
          }
          catch (NoSuchElementException e) {
            return null;
          }
        }
      }
      else if (objectIndex != null) {
        synchronized (this) {
          Object2ObjectSortedMap head = objectIndex.tailMap(key);
          ObjectSortedSet<Map.Entry<Object[], Object>> entries = head.entrySet();
          if (head.isEmpty()) {
            return null;
          }
          try {
            int offset = 0;
            Object[] lastKey = (Object[]) head.firstKey();
            //if (lastKey.equals(key[0])) {
            lastKey = null;
            Object[] currKey = null;
            for (Map.Entry<Object[], Object> entry : entries) {
              if (currKey == null) {
                currKey = (Object[]) entry.getKey();
              }
              else {
                lastKey = entry.getKey();
                ret[offset++] = new MyEntry<>(lastKey, entry.getValue());
                if (offset >= ret.length) {
                  break;
                }
              }
            }
            //}
            return ret;
          }
          catch (NoSuchElementException e) {
            return null;
          }
        }
      }

      if (longSkipIndex != null) {
        for (int i = 0; i < ret.length; i++) {
          Map.Entry<Long, Object> entry = longSkipIndex.higherEntry((Long) key[0]);
          if (entry == null) {
            return ret;
          }
          ret[i] = new MyEntry<>(new Object[]{entry.getKey()}, entry.getValue());
          key = new Object[]{entry.getKey()};
        }
        return ret;
      }
      else if (stringSkipIndex != null) {
        for (int i = 0; i < ret.length; i++) {
          Map.Entry<byte[], Object> entry = stringSkipIndex.higherEntry((byte[]) key[0]);
          if (entry == null) {
            return ret;
          }
          ret[i] = new MyEntry<>(new Object[]{entry.getKey()}, entry.getValue());
          key = new Object[]{entry.getKey()};
        }
        return ret;
      }
      else if (objectSkipIndex != null) {
        for (int i = 0; i < ret.length; i++) {
          Map.Entry<Object[], Object> entry = objectSkipIndex.higherEntry(key);
          if (entry == null) {
            return ret;
          }
          ret[i] = new MyEntry<>(entry.getKey(), entry.getValue());
          key = entry.getKey();
        }
        return ret;
      }
      return null;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public Map.Entry<Object[], Object> higherEntry(Object[] key) {
    try {
      if (longIndex != null) {
        synchronized (this) {
          Long2ObjectSortedMap head = longIndex.tailMap((Long) key[0]);
          ObjectSortedSet<Map.Entry<Long, Object>> entries = head.entrySet();
          if (head.isEmpty()) {
            return null;
          }
          try {
            Object lastValue = null;
            Long lastKey = head.firstLongKey();
            if (lastKey.equals(key[0])) {
              lastKey = null;
              Long currKey = null;
              for (Map.Entry<Long, Object> entry : entries) {
                if (currKey == null) {
                  currKey = entry.getKey();
                }
                else {
                  lastKey = entry.getKey();
                  lastValue = entry.getValue();
                  break;
                }
              }
            }
            if (lastKey == null) {
              return null;
            }
            Object value = lastValue;
            if (value == null) {
              value = longIndex.get(lastKey);
            }
            return new MyEntry<>(new Object[]{lastKey}, value);
          }
          catch (NoSuchElementException e) {
            return null;
          }
        }
      }
      else if (stringIndex != null) {
        synchronized (this) {
          byte[] inputKey = (byte[]) key[0];//((String) key[0]).getBytes("utf-8");
          Object2ObjectSortedMap head = stringIndex.tailMap(inputKey);
          ObjectSortedSet<Map.Entry<byte[], Object>> entries = head.entrySet();
          if (head.isEmpty()) {
            return null;
          }
          try {
            byte[] lastKey = (byte[]) head.firstKey();
            if (Arrays.equals(lastKey, inputKey)) {
              lastKey = null;
              byte[] currKey = null;
              for (Map.Entry<byte[], Object> entry : entries) {
                if (currKey == null) {
                  currKey = entry.getKey();
                }
                else {
                  lastKey = entry.getKey();
                  break;
                }
              }
            }
            if (lastKey == null) {
              return null;
            }
            Object value = stringIndex.get(lastKey);
            return new MyEntry<>(new Object[]{(byte[]) lastKey/*new String(lastKey, "utf-8")*/}, value);
          }
          catch (NoSuchElementException e) {
            return null;
          }
        }
      }
      else if (objectIndex != null) {
        synchronized (this) {
          Object2ObjectSortedMap<Object[], Object> head = objectIndex.tailMap(key);
          ObjectSortedSet<Map.Entry<Object[], Object>> entries = head.entrySet();
          if (head.isEmpty()) {
            return null;
          }
          try {
            Object[] lastKey = head.firstKey();
            if (0 == objectIndex.comparator().compare(lastKey, key)) {
              lastKey = null;
              Object[] currKey = null;
              for (Map.Entry<Object[], Object> entry : entries) {
                if (currKey == null) {
                  currKey = entry.getKey();
                }
                else {
                  lastKey = entry.getKey();
                  break;
                }
              }
            }
            if (lastKey == null) {
              return null;
            }
            Object value = objectIndex.get(lastKey);
            return new MyEntry<>(lastKey, value);
          }
          catch (NoSuchElementException e) {
            return null;
          }
        }
      }

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
    if (longIndex != null) {
      synchronized (this) {
        return longIndex.values();
      }
    }
    else if (stringIndex != null) {
      synchronized (this) {
        return stringIndex.values();
      }
    }
    else if (objectIndex != null) {
      synchronized (this) {
        return objectIndex.values();
      }
    }

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
      if (longIndex != null) {

        boolean shouldSkip = false;
        while (true) {
          int count = 0;
          List<Repartitioner.MapEntry> list = new ArrayList<>();
          synchronized (this) {
            Long2ObjectSortedMap<Object> map = longIndex.tailMap((long) key[0]);
            if (map.isEmpty()) {
              return false;
            }
            for (Map.Entry<Long, Object> entry : map.entrySet()) {
              if (shouldSkip) {
                shouldSkip = false;
                continue;
              }
              key = new Object[]{entry.getKey()};
              list.add(new Repartitioner.MapEntry(key, entry.getValue()));
              if (count++ > 100) {
                break;
              }
            }
            shouldSkip = true;
          }

          if (list.isEmpty()) {
            return false;
          }
          for (Repartitioner.MapEntry entry : list) {
            if (!visitor.visit(entry.key, entry.value)) {
              return false;
            }
          }
        }
      }
      else if (stringIndex != null) {
        boolean shouldSkip = false;
        while (true) {
          int count = 0;
          List<Repartitioner.MapEntry> list = new ArrayList<>();
          synchronized (this) {
            Object2ObjectSortedMap<byte[], Object> map = stringIndex.tailMap((byte[]) key[0]);
            if (map.isEmpty()) {
              return false;
            }
            for (Map.Entry<byte[], Object> entry : map.entrySet()) {
              if (shouldSkip) {
                shouldSkip = false;
                continue;
              }
              key = new Object[]{entry.getKey()};
              list.add(new Repartitioner.MapEntry(key, entry.getValue()));
              if (count++ > 100) {
                break;
              }
            }
            shouldSkip = true;
          }

          if (list.isEmpty()) {
            return false;
          }
          for (Repartitioner.MapEntry entry : list) {
            if (!visitor.visit(entry.key, entry.value)) {
              return false;
            }
          }
        }
      }
      else if (objectIndex != null) {
        boolean shouldSkip = false;
        while (true) {
          int count = 0;
          List<Repartitioner.MapEntry> list = new ArrayList<>();
          synchronized (this) {
            Object2ObjectSortedMap<Object[], Object> map = objectIndex.tailMap(key);
            if (map.isEmpty()) {
              return false;
            }
            for (Map.Entry<Object[], Object> entry : map.entrySet()) {
              if (shouldSkip) {
                shouldSkip = false;
                continue;
              }
              key = entry.getKey();
              list.add(new Repartitioner.MapEntry(key, entry.getValue()));
              if (count++ > 100) {
                break;
              }
            }
            shouldSkip = true;
          }

          if (list.isEmpty()) {
            return false;
          }
          for (Repartitioner.MapEntry entry : list) {
            if (!visitor.visit(entry.key, entry.value)) {
              return false;
            }
          }
        }
      }
      else if (longSkipIndex != null) {
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
      if (longIndex != null) {
        while (true) {
          int count = 0;
          synchronized (this) {
            Long2ObjectSortedMap<Object> map = longIndex.headMap((long) key[0]);
            if (map.isEmpty()) {
              return true;
            }
            for (Map.Entry<Long, Object> entry : map.entrySet()) {
              key[0] = entry.getKey();
              if (!visitor.visit(key, entry.getValue())) {
                return false;
              }
              if (count++ > 1000) {
                break;
              }
            }
          }
        }
/*
        while (true) {
          int count = 0;
          List<Repartitioner.MapEntry> list = new ArrayList<>();
          synchronized (this) {
            Long2ObjectSortedMap<Object> map = longIndex.headMap((long) key[0]);
            if (map.isEmpty()) {
              return true;
            }
            for (Map.Entry<Long, Object> entry : map.entrySet()) {
              key = new Object[]{entry.getKey()};
              list.add(new Repartitioner.MapEntry(key, entry.getValue()));
              if (count++ > 100) {
                break;
              }
            }
          }

          if (list.isEmpty()) {
            return true;
          }
          for (Repartitioner.MapEntry entry : list) {
            if (!visitor.visit(entry.key, entry.value)) {
              return false;
            }
          }
        }
  */
      }
      else if (stringIndex != null) {
        while (true) {
          int count = 0;
          synchronized (this) {
            Object2ObjectSortedMap<byte[], Object> map = stringIndex.headMap((byte[]) key[0]);
            if (map.isEmpty()) {
              return true;
            }
            for (Map.Entry<byte[], Object> entry : map.entrySet()) {
              key[0] = entry.getKey();
              if (!visitor.visit(key, entry.getValue())) {
                return false;
              }
              if (count++ > 1000) {
                break;
              }
            }
          }
        }
      }
      else if (objectIndex != null) {
        while (true) {
          int count = 0;
          synchronized (this) {
            Object2ObjectSortedMap<Object[], Object> map = objectIndex.headMap(key);
            if (map.isEmpty()) {
              return true;
            }
            for (Map.Entry<Object[], Object> entry : map.entrySet()) {
              key[0] = entry.getKey();
              if (!visitor.visit(key, entry.getValue())) {
                return false;
              }
              if (count++ > 1000) {
                break;
              }
            }
          }
        }
      }
      else if (longSkipIndex != null) {
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
    if (longIndex != null) {
      synchronized (this) {
        if (longIndex.isEmpty()) {
          return null;
        }
        long lastKey = longIndex.lastLongKey();
        Object value = longIndex.get(lastKey);
        return new MyEntry<>(new Object[]{lastKey}, value);
      }
    }
    else if (stringIndex != null) {
      synchronized (this) {
        if (stringIndex.isEmpty()) {
          return null;
        }
        byte[] lastKey = stringIndex.lastKey();
        Object value = stringIndex.get(lastKey);
        return new MyEntry<>(new Object[]{(byte[]) lastKey/*new String(lastKey, "utf-8")*/}, value);
      }
    }
    else if (objectIndex != null) {
      synchronized (this) {
        if (objectIndex.isEmpty()) {
          return null;
        }
        Object[] lastKey = objectIndex.lastKey();
        Object value = objectIndex.get(lastKey);
        return new MyEntry<>(lastKey, value);
      }
    }

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
    if (longIndex != null) {
      synchronized (this) {
        if (longIndex.isEmpty()) {
          return null;
        }
        long firstKey = longIndex.firstLongKey();
        Object value = longIndex.get(firstKey);
        return new MyEntry<>(new Object[]{firstKey}, value);
      }
    }
    else if (stringIndex != null) {
      synchronized (this) {
        if (stringIndex.isEmpty()) {
          return null;
        }
        byte[] firstKey = (byte[]) stringIndex.firstKey();
        Object value = stringIndex.get(firstKey);
        return new MyEntry<>(new Object[]{(byte[]) firstKey/*new String(firstKey, "utf-8")*/}, value);
      }
    }
    else if (objectIndex != null) {
      synchronized (this) {
        if (objectIndex.isEmpty()) {
          return null;
        }
        Object[] firstKey = objectIndex.firstKey();
        Object value = objectIndex.get(firstKey);
        return new MyEntry<>(firstKey, value);
      }
    }

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
