/* © 2020 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.index;

import com.sonicbase.common.DataUtils;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.FieldSchema;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;

public class NativeSkipListMapImpl extends NativeSkipListMap implements IndexImpl {

  private final int port;
  private final Index index;
  private final long indexId;
  private final int[] dataTypes;

  public  NativeSkipListMapImpl(int port, Index index) {

    init(port);

    this.port = port;
    this.index = index;

    TableSchema tableSchema = index.getTableSchema();
    IndexSchema schema = index.getIndexSchema();
    String[] fields = schema.getFields();
    int[] dataTypes = new int[fields.length];
    for (int i = 0; i < fields.length; i++) {
      int offset = tableSchema.getFieldOffset(fields[i]);
      FieldSchema field = tableSchema.getFields().get(offset);
      dataTypes[i] = field.getType().getValue();
    }

    this.indexId = initIndexSkipListMap(dataTypes);
    this.dataTypes = dataTypes;
  }

  public static void init(int port) {
    NativePartitionedTreeImpl.init(port);
  }

  //public static ConcurrentSkipListSet<Long> added = new ConcurrentSkipListSet<>();;


  //public static NativeSkipListMapImpl map;

  @Override
  public Object put(Object[] key, Object value) {
    //map = this;
    //added.add((Long) key[0]);
    long ret = put(indexId, key, (long)value);
    if (ret == -1) {
      index.getSizeObj().incrementAndGet();
      return null;
    }
    return ret;
  }

  public void put(Object[][] keys,  long[] values, long[] retValues) {
    put(indexId, keys, values, retValues);
  }

  @Override
  public Object get(Object[] key) {
    long ret = get(indexId, key);
    if (-1 == ret) {
      return null;
    }
    return ret;
  }

  @Override
  public Object remove(Object[] key) {
    long ret = remove(indexId, key);
    if (ret != -1) {
      index.getSizeObj().decrementAndGet();
      return ret;
    }
    return null;
  }

  public void sortKeys(Object[][] keys, boolean ascend) {
    throw new DatabaseException("not supported");
  }

  private ConcurrentLinkedQueue<byte[]> bytePool = new ConcurrentLinkedQueue<>();

  @Override
  public int tailBlock(Object[] startKey, int count, boolean first, Object[][] keys, long[] values) {
    int retCount = 0;
    try {
      byte[] bytes = bytePool.poll();
      if (bytes == null) {
        bytes = new byte[218196];
      }

      while (!tailBlockArray(indexId, startKey, count, first, bytes, bytes.length)) {
        bytes = new byte[bytes.length * 2];
      }

      /*
      //int len = DataUtils.bytesToInt(bytes, 0);
      LzoDecompressor1x d = new LzoDecompressor1x();

      lzo_uintp retLen = new lzo_uintp();
      byte[] out = bytePool.poll();
      if (out == null) {
        out = new byte[8196];
      }
      d.decompress(bytes, 0, bytes.length, out, 0, retLen);

      if (bytePool.size() < 100) {
        bytePool.offer(out);
      }

      bytes = out;
*/
      int[] offset = new int[]{0};
      retCount = (int) DataUtils.bytesToLong(bytes, offset[0]);
      offset[0] += 8;
//
      for (int i = 0; i < retCount; i++) {
        keys[i] = NativePartitionedTreeImpl.deserializeKey(dataTypes, bytes, offset);
        values[i] = DataUtils.bytesToLong(bytes, offset[0]);
        offset[0] += 8;
      }

      if (bytePool.size() < 100) {
        bytePool.offer(bytes);
      }

      return retCount;
    }
    catch (Exception e) {
      throw new RuntimeException("retCount=" + retCount, e);
      //return 0;
    }
  }

  public int getResultsObjs(Object[] startKey, int count, boolean first, Object[][] keys, long[] values) {
    try {
      return getResultsObjects(indexId, startKey, count, first, keys, values);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public byte[] getResultsByteArray(Object[] startKey, int count, boolean first) {
    try {
      return getResultsBytes(indexId, startKey, count, first);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int headBlock(Object[] startKey, int count, boolean first, Object[][] keys, long[] values) {
    int retCount = 0;
    try {
      byte[] bytes = bytePool.poll();
      if (bytes == null) {
        bytes = new byte[218196];
      }

      while (!headBlockArray(indexId, startKey, count, first, bytes, bytes.length)) {
        bytes = new byte[bytes.length * 2];
      }

      /*
      //int len = DataUtils.bytesToInt(bytes, 0);
      LzoDecompressor1x d = new LzoDecompressor1x();

      lzo_uintp retLen = new lzo_uintp();
      byte[] out = bytePool.poll();
      if (out == null) {
        out = new byte[8196];
      }
      d.decompress(bytes, 0, bytes.length, out, 0, retLen);

      if (bytePool.size() < 100) {
        bytePool.offer(out);
      }

      bytes = out;
*/
      int[] offset = new int[]{0};
      retCount = (int) DataUtils.bytesToLong(bytes, offset[0]);
      offset[0] += 8;
//
      for (int i = 0; i < retCount; i++) {
        keys[i] = NativePartitionedTreeImpl.deserializeKey(dataTypes, bytes, offset);
        values[i] = DataUtils.bytesToLong(bytes, offset[0]);
        offset[0] += 8;
      }

      if (bytePool.size() < 100) {
        bytePool.offer(bytes);
      }

      return retCount;
    }
    catch (Exception e) {
      throw new RuntimeException("retCount=" + retCount, e);
      //return 0;
    }
  }

  @Override
  public Map.Entry<Object[], Object> higherEntry(Object[] key) {
    Object[][] keys = new Object[1][];
    long[] values = new long[1];
    boolean found = higherEntry(indexId, key, keys, values);
    if (!found) {
      return null;
    }
    return new Index.MyEntry(keys[0], values[0]);
  }

  @Override
  public Map.Entry<Object[], Object> lowerEntry(Object[] key) {
    Object[][] keys = new Object[1][];
    long[] values = new long[1];
    boolean found = lowerEntry(indexId, key, keys, values);
    if (!found) {
      return null;
    }
    return new Index.MyEntry(keys[0], values[0]);
  }

  @Override
  public Map.Entry<Object[], Object> floorEntry(Object[] key) {
    Object[][] keys = new Object[1][];
    long[] values = new long[1];

    boolean found = floorEntry(indexId, key, keys, values);
    if (!found) {
      return null;
    }
    return new Index.MyEntry(keys[0], values[0]);
  }

  @Override
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
        if (0 != index.getComparator().compare(entry.getKey(), key)) {
          return ret;
        }
        ret.add(entry);
      }
      return ret;
    }
  }


  @Override
  public Map.Entry<Object[], Object> ceilingEntry(Object[] key) {
    Object[][] keys = new Object[1][];
    long[] values = new long[1];

    boolean found = ceilingEntry(indexId, key, keys, values);
    if (!found) {
      return null;
    }
    return new Index.MyEntry(keys[0], values[0]);
  }

  @Override
  public Map.Entry<Object[], Object> lastEntry() {

    Object[][] keys = new Object[1][];
    long[] values = new long[1];

    boolean found = lastEntry2(indexId, keys, values);
    if (!found) {
      return null;
    }
    return new Index.MyEntry(keys[0], values[0]);
  }

  @Override
  public Map.Entry<Object[], Object> firstEntry() {

    Object[][] keys = new Object[1][];
    long[] values = new long[1];

    boolean found = firstEntry2(indexId, keys, values);
    if (!found) {
      return null;
    }
    return new Index.MyEntry(keys[0], values[0]);
  }

  @Override
  public void clear() {
    clear(indexId);
  }

  @Override
  public void delete() {

  }


}
