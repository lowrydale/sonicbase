package com.lowryengineering.database.index;

import com.lowryengineering.database.query.DatabaseException;
import com.lowryengineering.database.schema.DataType;
import com.lowryengineering.database.schema.FieldSchema;
import com.lowryengineering.database.schema.TableSchema;
import it.unimi.dsi.fastutil.longs.Long2LongAVLTreeMap;
import it.unimi.dsi.fastutil.longs.Long2LongSortedMap;
import it.unimi.dsi.fastutil.objects.Object2LongAVLTreeMap;
import it.unimi.dsi.fastutil.objects.Object2LongSortedMap;
import it.unimi.dsi.fastutil.objects.ObjectSortedSet;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Responsible for
 */
public class Index {
  private boolean ordered = false;

  private final Comparator[] comparators;


  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="EI_EXPOSE_REP", justification="copying the returned data is too slow")
  public Comparator[] getComparators() {
    return comparators;
  }

  public boolean isOrdered() {
    return ordered;
  }

  private Long2LongAVLTreeMap longIndex;
  private Object2LongAVLTreeMap<byte[]> stringIndex;
  private Object2LongAVLTreeMap<Object[]> objectIndex;

  private ConcurrentSkipListMap<Long, Long> longSkipIndex;
  private ConcurrentSkipListMap<byte[], Long> stringSkipIndex;
  private ConcurrentSkipListMap<Object[], Long> objectSkipIndex;



  public void setLongIndex(Long2LongAVLTreeMap longIndex) {
    this.longIndex = longIndex;
  }

  public void setStringIndex(Object2LongAVLTreeMap stringIndex) {
    this.stringIndex = stringIndex;
  }

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

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="EI_EXPOSE_REP2", justification="copying the passed in data is too slow")
  @SuppressWarnings("PMD.ArrayIsStoredDirectly") //copying the passed in data is too slow
  public Index(TableSchema tableSchema, String indexName, final Comparator[] comparators) {
    this.comparators = comparators;
    String[] fields = tableSchema.getIndices().get(indexName).getFields();
    if (fields.length == 1) {
      FieldSchema fieldSchema = tableSchema.getFields().get(tableSchema.getFieldOffset(fields[0]));
      if (fieldSchema.getType() == DataType.Type.BIGINT) {
//        longIndex = new Long2LongAVLTreeMap();
        longSkipIndex = new ConcurrentSkipListMap<>(new Comparator<Long>(){
          @Override
          public int compare(Long o1, Long o2) {
            return o1 < o2 ? -1 : o1 > o2 ? 1 : 0;
          }
        }   );
      }
      else if (fieldSchema.getType() == DataType.Type.VARCHAR) {
//        stringIndex = new Object2LongAVLTreeMap<>(utf8Comparator);
        stringSkipIndex = new ConcurrentSkipListMap<>(utf8Comparator);
      }
      else {
        objectSkipIndex = new ConcurrentSkipListMap<>(new Comparator<Object[]>() {
                        @Override
                        public int compare(Object[] o1, Object[] o2) {
                          for (int i = 0; i < o1.length; i++) {
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
    else {
      objectSkipIndex = new ConcurrentSkipListMap<>(new Comparator<Object[]>() {
                @Override
                public int compare(Object[] o1, Object[] o2) {
                  for (int i = 0; i < o1.length; i++) {
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
//      objectIndex = new Object2LongAVLTreeMap<>(new Comparator<Object[]>() {
//          @Override
//          public int compare(Object[] o1, Object[] o2) {
//            for (int i = 0; i < o1.length; i++) {
//              if (o1[i] == null || o2[i] == null) {
//                continue;
//              }
//              int value = comparators[i].compare(o1[i], o2[i]);
//              if (value < 0) {
//                return -1;
//              }
//              if (value > 0) {
//                return 1;
//              }
//            }
//            return 0;
//          }
//        });
    }
  }

  public void clear() {
    synchronized (this) {
      if (longIndex != null) {
        longIndex.clear();
      }
      else if (stringIndex != null) {
        stringIndex.clear();
      }
      else if (objectIndex != null) {
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
  }

  public interface Visitor {
    void visit(Object[] key, long value) throws IOException;
  }

  public void iterate(Visitor visitor) throws IOException {
    if (longIndex != null) {
      for (Map.Entry<Long, Long> entry : longIndex.entrySet()) {
        visitor.visit(new Object[]{entry.getKey()}, entry.getValue());
      }
    }
    else if (longSkipIndex != null) {
      for (Map.Entry<Long, Long> entry : longSkipIndex.entrySet()) {
        visitor.visit(new Object[]{entry.getKey()}, entry.getValue());
      }
    }
    else if (objectSkipIndex != null) {
      for (Map.Entry<Object[], Long> entry : objectSkipIndex.entrySet()) {
        visitor.visit(entry.getKey(), entry.getValue());
      }
    }
    else if (stringIndex != null) {
      for (Map.Entry<byte[], Long> entry : stringIndex.entrySet()) {
        visitor.visit(new Object[]{entry.getKey()/*new String(entry.getKey(), "utf-8")*/}, entry.getValue());
      }
    }
    else if (objectIndex != null) {
      for (Map.Entry<Object[], Long> entry : objectIndex.entrySet()) {
        visitor.visit(entry.getKey(), entry.getValue());
      }
    }
  }

  public Long get(Object[] key) {
    synchronized (this) {
      if (longIndex != null) {
        return longIndex.get(key[0]);
      }
      else if (stringIndex != null) {
        return stringIndex.get((byte[])key[0]/*((String)key[0]).getBytes("utf-8")*/);
      }
      else if (objectIndex != null) {
        return objectIndex.get(key);
      }
    }
    if (longSkipIndex != null) {
      return longSkipIndex.get((long)key[0]);
    }
    else if (stringSkipIndex != null) {
      return stringSkipIndex.get((byte[])key[0]);
    }
    else if (objectSkipIndex != null) {
      return objectSkipIndex.get(key);
    }
    return null;
  }

  public Long put(Object[] key, long id) {
    synchronized (this) {
      if (longIndex != null) {
        return longIndex.put((long) key[0], id);
      }
      else if (stringIndex != null) {
        return stringIndex.put((byte[])key[0]/*((String)key[0]).getBytes("utf-8")*/, id);
      }
      else if (objectIndex != null) {
        return objectIndex.put(key, id);
      }
    }
    if (longSkipIndex != null) {
      return longSkipIndex.put((Long)key[0], id);
    }
    else if (stringSkipIndex != null) {
      return stringSkipIndex.put((byte[])key[0], id);
    }
    else if (objectSkipIndex != null) {
      return objectSkipIndex.put(key, id);
    }
    return null;
  }

  public Long remove(Object[] key) {
    synchronized (this) {
      if (longIndex != null) {
        return longIndex.remove((long) key[0]);
      }
      else if (stringIndex != null) {
        return stringIndex.remove((byte[])key[0] /*((String)key[0]).getBytes("utf-8")*/);
      }
      else if (objectIndex != null) {
        return objectIndex.remove(key);
      }
    }
    if (longSkipIndex != null) {
      return longSkipIndex.remove((Long)key[0]);
    }
    else if (stringSkipIndex != null) {
      return stringSkipIndex.remove((byte[])key[0]);
    }
    else if (objectSkipIndex != null) {
      return objectSkipIndex.remove(key);
    }
    return null;
  }

  public Long unsafePutIfAbsent(Object[] key, long id) {

    if (longIndex != null) {
      return longIndex.putIfAbsent((long) key[0], id);
    }
    else if (stringIndex != null) {
      return stringIndex.putIfAbsent((byte[])key[0] /*((String) key[0]).getBytes("utf-8")*/, id);
    }
    else if (longSkipIndex != null) {
      return longSkipIndex.putIfAbsent((long) key[0], id);
    }
    else if (objectSkipIndex != null) {
      return objectSkipIndex.putIfAbsent(key, id);
    }
    else if (stringSkipIndex != null) {
      return stringSkipIndex.putIfAbsent((byte[])key[0], id);
    }
    else if (objectIndex != null) {
      return objectIndex.putIfAbsent(key, id);
    }
    return null;
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

  public Map.Entry<Object[], Long> ceilingEntry(Object[] key) {
    synchronized (this) {
      if (longIndex != null) {
        if (longIndex.isEmpty()) {
          return null;
        }
        Long2LongSortedMap tail = longIndex.tailMap((Long) key[0]);
        if (tail.isEmpty()) {
          return null;
        }
        long firstKey = tail.firstLongKey();
        Object value = longIndex.get(firstKey);
        return new MyEntry<>(new Object[]{firstKey}, (long) value);
      }
      else if (stringIndex != null) {
        if (stringIndex.isEmpty()) {
          return null;
        }
        Object2LongSortedMap tail = stringIndex.tailMap((byte[])key[0]/*((String)key[0]).getBytes("utf-8")*/);
        if (tail.isEmpty()) {
          return null;
        }
        byte[] firstKey = (byte[]) tail.firstKey();
        Object value = stringIndex.get(firstKey);
        return new MyEntry<>(new Object[]{firstKey/*new String(firstKey, "utf-8")*/}, (long)value);
      }
      else if (objectIndex != null) {
        if (objectIndex.isEmpty()) {
          return null;
        }
        Object2LongSortedMap<Object[]> tail = objectIndex.tailMap(key);
        if (tail.isEmpty()) {
          return null;
        }
        Object[] firstKey = tail.firstKey();
        Long value = objectIndex.get(firstKey);
        return new MyEntry<>(firstKey, value);

      }
    }
    if (longSkipIndex != null) {
      Map.Entry<Long, Long> entry = longSkipIndex.ceilingEntry((Long)key[0]);
      if (entry == null) {
        return null;
      }
      return new MyEntry<>(new Object[]{entry.getKey()}, (long)entry.getValue());
    }
    else if (stringSkipIndex != null) {
      Map.Entry<byte[], Long> entry = stringSkipIndex.ceilingEntry((byte[])key[0]);
      if (entry == null) {
        return null;
      }
      return new MyEntry<>(new Object[]{entry.getKey()}, (long)entry.getValue());
    }
    else if (objectSkipIndex != null) {
      Map.Entry<Object[], Long> entry = objectSkipIndex.ceilingEntry(key);
      if (entry == null) {
        return null;
      }
      return new MyEntry<>(entry.getKey(), (long)entry.getValue());
    }
    return null;
  }

  public Map.Entry<Object[], Long> floorEntry(Object[] key) {
    synchronized (this) {
      if (longIndex != null) {
        if (longIndex.isEmpty()) {
          return null;
        }
        Object value = longIndex.get((Long) key[0]);
        if (value != null) {
          return new MyEntry<>(key, (long) value);
        }
        Long2LongSortedMap head = longIndex.headMap((Long) key[0]);
        if (head.isEmpty()) {
          return null;
        }
        long lastKey = head.lastLongKey();
        value = longIndex.get(lastKey);
        return new MyEntry<>(new Object[]{lastKey}, (long) value);
      }
      else if (stringIndex != null) {
        if (stringIndex.isEmpty()) {
          return null;
        }
        Object value = stringIndex.get((byte[])key[0]/*((String)key[0]).getBytes("utf-8")*/);
        if (value != null) {
          return new MyEntry<>(key, (long) value);
        }
        Object2LongSortedMap head = stringIndex.headMap((byte[])key[0]/*((String)key[0]).getBytes("utf-8")*/);
        if (head.isEmpty()) {
          return null;
        }
        byte[] lastKey = (byte[]) head.lastKey();
        value = stringIndex.get(lastKey);
        return new MyEntry<>(new Object[]{(byte[])lastKey/*new String(lastKey, "utf-8")*/}, (long) value);

      }
      else if (objectIndex != null) {
        if (objectIndex.isEmpty()) {
          return null;
        }
        Long value = objectIndex.get(key);
        if (value != null) {
          return new MyEntry<>(key, value);
        }
        Object2LongSortedMap<Object[]> head = objectIndex.headMap(key);
        if (head.isEmpty()) {
          return null;
        }
        Object[] lastKey = head.lastKey();
        value = objectIndex.get(lastKey);
        return new MyEntry<>(lastKey, value);

      }
    }
    if (longSkipIndex != null) {
      Map.Entry<Long, Long> entry = longSkipIndex.floorEntry((Long)key[0]);
      if (entry == null) {
        return null;
      }
      return new MyEntry<>(new Object[]{entry.getKey()}, (long)entry.getValue());
    }
    else if (stringSkipIndex != null) {
      Map.Entry<byte[], Long> entry = stringSkipIndex.floorEntry((byte[])key[0]);
      if (entry == null) {
        return null;
      }
      return new MyEntry<>(new Object[]{entry.getKey()}, (long)entry.getValue());
    }
    else if (objectSkipIndex != null) {
      Map.Entry<Object[], Long> entry = objectSkipIndex.floorEntry(key);
      if (entry == null) {
        return null;
      }
      return new MyEntry<>(entry.getKey(), (long)entry.getValue());
    }
    return null;
  }

  public Map.Entry<Object[], Long> lowerEntry(Object[] key) {
    synchronized (this) {
      if (longIndex != null) {
        Long2LongSortedMap head = longIndex.headMap((Long) key[0]);
        if (head.isEmpty()) {
          return null;
        }
        long lastKey = head.lastLongKey();
        Object value = longIndex.get(lastKey);
        return new MyEntry<>(new Object[]{lastKey}, (long) value);
      }
      else if (stringIndex != null) {
        Object2LongSortedMap head = stringIndex.headMap((byte[])key[0]/*((String)key[0]).getBytes("utf-8")*/);
        if (head.isEmpty()) {
          return null;
        }
        byte[] lastKey = (byte[]) head.lastKey();
        Object value = stringIndex.get(lastKey);
        return new MyEntry<>(new Object[]{(byte[])lastKey/*new String(lastKey, "utf-8")*/}, (long) value);
      }
      else if (objectIndex != null) {
        Object2LongSortedMap<Object[]> head = objectIndex.headMap(key);
        if (head.isEmpty()) {
          return null;
        }
        Object[] lastKey = head.lastKey();
        Long value = objectIndex.get(lastKey);
        return new MyEntry<>(lastKey, value);
      }
    }
    if (longSkipIndex != null) {
      Map.Entry<Long, Long> entry = longSkipIndex.lowerEntry((Long)key[0]);
      if (entry == null) {
        return null;
      }
      return new MyEntry<>(new Object[]{entry.getKey()}, (long)entry.getValue());
    }
    else if (stringSkipIndex != null) {
      Map.Entry<byte[], Long> entry = stringSkipIndex.lowerEntry((byte[])key[0]);
      if (entry == null) {
        return null;
      }
      return new MyEntry<>(new Object[]{entry.getKey()}, (long)entry.getValue());
    }
    else if (objectSkipIndex != null) {
      Map.Entry<Object[], Long> entry = objectSkipIndex.lowerEntry(key);
      if (entry == null) {
        return null;
      }
      return new MyEntry<>(entry.getKey(), (long)entry.getValue());
     }
    return null;
  }

  public Map.Entry<Object[], Long> higherEntry(Object[] key) {
    try {
      synchronized (this) {
        if (longIndex != null) {
          Long2LongSortedMap head = longIndex.tailMap((Long) key[0]);
          ObjectSortedSet<Map.Entry<Long, Long>> entries = head.entrySet();
          if (head.isEmpty()) {
            return null;
          }
          try {
            Long lastKey = head.firstLongKey();
            if (lastKey.equals(key[0])) {
              lastKey = null;
              Long currKey = null;
              for (Map.Entry<Long, Long> entry : entries) {
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
            Object value = longIndex.get(lastKey);
            return new MyEntry<>(new Object[]{lastKey}, (Long) value);
          }
          catch (NoSuchElementException e) {
            return null;
          }
        }
        else if (stringIndex != null) {
          byte[] inputKey = ((String) key[0]).getBytes("utf-8");
          Object2LongSortedMap head = stringIndex.tailMap(inputKey);
          ObjectSortedSet<Map.Entry<byte[], Long>> entries = head.entrySet();
          if (head.isEmpty()) {
            return null;
          }
          try {
            byte[] lastKey = (byte[]) head.firstKey();
            if (Arrays.equals(lastKey, inputKey)) {
              lastKey = null;
              byte[] currKey = null;
              for (Map.Entry<byte[], Long> entry : entries) {
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
            return new MyEntry<>(new Object[]{(byte[]) lastKey/*new String(lastKey, "utf-8")*/}, (long) value);
          }
          catch (NoSuchElementException e) {
            return null;
          }

        }
        else if (objectIndex != null) {
          Object2LongSortedMap<Object[]> head = objectIndex.tailMap(key);
          ObjectSortedSet<Map.Entry<Object[], Long>> entries = head.entrySet();
          if (head.isEmpty()) {
            return null;
          }
          try {
            Object[] lastKey = head.firstKey();
            if (0 == objectIndex.comparator().compare(lastKey, key)) {
              lastKey = null;
              Object[] currKey = null;
              for (Map.Entry<Object[], Long> entry : entries) {
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
            Long value = objectIndex.get(lastKey);
            return new MyEntry<>(lastKey, value);
          }
          catch (NoSuchElementException e) {
            return null;
          }
        }
      }
      if (longSkipIndex != null) {
        Map.Entry<Long, Long> entry = longSkipIndex.higherEntry((Long) key[0]);
        if (entry == null) {
          return null;
        }
        return new MyEntry<>(new Object[]{entry.getKey()}, (long) entry.getValue());
      }
      else if (stringSkipIndex != null) {
        Map.Entry<byte[], Long> entry = stringSkipIndex.higherEntry((byte[]) key[0]);
        if (entry == null) {
          return null;
        }
        return new MyEntry<>(new Object[]{entry.getKey()}, (long) entry.getValue());
      }
      else if (objectSkipIndex != null) {
        Map.Entry<Object[], Long> entry = objectSkipIndex.higherEntry(key);
        if (entry == null) {
          return null;
        }
        return new MyEntry<>(entry.getKey(), (long) entry.getValue());
      }
      return null;
    }
    catch (UnsupportedEncodingException e) {
      throw new DatabaseException(e);
    }
  }

  public Iterable<? extends Long> values() {
    synchronized (this) {
      if (longIndex != null) {
        return longIndex.values();
      }
      else if (stringIndex != null) {
        return stringIndex.values();
      }
      else if (objectIndex != null) {
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

  public long size() {
    synchronized (this) {
      if (longIndex != null) {
        return longIndex.size();
      }
      else if (stringIndex != null) {
        return stringIndex.size();
      }
      else if (objectIndex != null) {
        return objectIndex.size();
      }
    }
    if (longSkipIndex != null) {
      return longSkipIndex.size();
    }
    else if (stringSkipIndex != null) {
      return stringSkipIndex.size();
    }
    else if (objectSkipIndex != null) {
      return objectSkipIndex.size();
    }
    return 0;
  }

  public Map.Entry<Object[], Long> lastEntry() {
    synchronized (this) {
      if (longIndex != null) {
        if (longIndex.isEmpty()) {
          return null;
        }
        long lastKey = longIndex.lastLongKey();
        Long value = longIndex.get(lastKey);
        return new MyEntry<>(new Object[]{lastKey}, value);
      }
      else if (stringIndex != null) {
        if (stringIndex.isEmpty()) {
          return null;
        }
        byte[] lastKey = stringIndex.lastKey();
        Object value = stringIndex.get(lastKey);
        return new MyEntry<>( new Object[]{(byte[])lastKey/*new String(lastKey, "utf-8")*/}, (long) value);
      }
      else if (objectIndex != null) {
        if (objectIndex.isEmpty()) {
          return null;
        }
        Object[] lastKey = objectIndex.lastKey();
        Long value = objectIndex.get(lastKey);
        return new MyEntry<>(lastKey, value);
      }
    }
    if (longSkipIndex != null) {
      Map.Entry<Long, Long> entry = longSkipIndex.lastEntry();
      if (entry == null) {
        return null;
      }
      return new MyEntry<>(new Object[]{entry.getKey()}, entry.getValue());
    }
    else if (stringSkipIndex != null) {
      Map.Entry<byte[], Long> entry = stringSkipIndex.lastEntry();
      if (entry == null) {
        return null;
      }
      return new MyEntry<>(new Object[]{entry.getKey()}, (long)entry.getValue());
    }
    else if (objectSkipIndex != null) {
      Map.Entry<Object[], Long> entry = objectSkipIndex.lastEntry();
      if (entry == null) {
        return null;
      }
      return new MyEntry<>(entry.getKey(), (long)entry.getValue());
    }

    return null;
  }


  public Map.Entry<Object[], Long> firstEntry() {
    synchronized (this) {
      if (longIndex != null) {
        if (longIndex.isEmpty()) {
          return null;
        }
        long firstKey = longIndex.firstLongKey();
        Object value = longIndex.get(firstKey);
        return new MyEntry<>(new Object[]{firstKey}, (long) value);
      }
      else if (stringIndex != null) {
        if (stringIndex.isEmpty()) {
          return null;
        }
        byte[] firstKey = (byte[]) stringIndex.firstKey();
        Object value = stringIndex.get(firstKey);
        return new MyEntry<>(new Object[]{(byte[])firstKey/*new String(firstKey, "utf-8")*/}, (long) value);
      }
      else if (objectIndex != null) {
        if (objectIndex.isEmpty()) {
          return null;
        }
        Object[] firstKey = objectIndex.firstKey();
        Long value = objectIndex.get(firstKey);
        return new MyEntry<>(firstKey, value);
      }
    }
    if (longSkipIndex != null) {
      Map.Entry<Long, Long> entry = longSkipIndex.firstEntry();
      if (entry == null) {
        return null;
      }
      return new MyEntry<>(new Object[]{entry.getKey()}, (long)entry.getValue());
    }
    else if (stringSkipIndex != null) {
      Map.Entry<byte[], Long> entry = stringSkipIndex.firstEntry();
      if (entry == null) {
        return null;
      }
      return new MyEntry<>(new Object[]{entry.getKey()}, (long)entry.getValue());
    }
    else if (objectSkipIndex != null) {
      Map.Entry<Object[], Long> entry = objectSkipIndex.firstEntry();
      if (entry == null) {
        return null;
      }
      return new MyEntry<>(entry.getKey(), (long)entry.getValue());
    }
    return null;
  }
}
