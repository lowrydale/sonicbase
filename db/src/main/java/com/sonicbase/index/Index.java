package com.sonicbase.index;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.ComObject;
import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.common.Record;
import com.sonicbase.common.ThreadUtil;
import com.sonicbase.schema.DataType;
import com.sonicbase.schema.FieldSchema;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.server.DatabaseServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.sonicbase.server.PartitionManager.DONT_RETURN_MISSING_KEY;
import static java.sql.Types.*;

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class Index {
  private static final Logger logger = LoggerFactory.getLogger(Index.class);

  private Map<Long, Boolean> isOpForRebalance;
  private TableSchema tableSchema;
  private IndexSchema indexSchema;

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

  public Index(int port, Map<Long, Boolean> isOpForRebalance, TableSchema tableSchema, String indexName, final Comparator[] comparators) {
    this.isOpForRebalance = isOpForRebalance;
    this.comparators = comparators;

    comparator = (o1, o2) -> getObjectArrayComparator(comparators, o1, o2);

    for (int i = 0; i < mutexes.length; i++) {
      mutexes[i] = new Object();
    }

    this.tableSchema = tableSchema;
    this.indexSchema = tableSchema.getIndices().get(indexName);
    String[] fields = indexSchema.getFields();
    if (fields.length == 1) {
      FieldSchema fieldSchema = tableSchema.getFields().get(tableSchema.getFieldOffset(fields[0]));
      switch (fieldSchema.getType().getValue()) {
        case BIGINT:
        case ROWID:
        case VARCHAR:
        case CHAR:
        case LONGVARCHAR:
        case NCHAR:
        case NVARCHAR:
        case LONGNVARCHAR:
        case NCLOB:
        case NUMERIC:
        case DECIMAL:
        case SMALLINT:
        case BIT:
        case BOOLEAN:
        case TINYINT:
        case INTEGER:
        case DOUBLE:
        case FLOAT:
        case REAL:
        case DATE:
        case TIME:
        case TIMESTAMP:
          impl = new NativeSkipListMapImpl(port, this);// new NativePartitionedTreeImpl(port, this); //new LongIndexImpl(this); //
//          impl = new NativePartitionedTreeImpl(port, this); //new LongIndexImpl(this); //
//          impl = new ObjectIndexImpl(this, comparators);
        break;
        default:
          impl = new ObjectIndexImpl(this, comparators);
          break;
      }
    }
    else {
      impl = new NativeSkipListMapImpl(port, this);// new NativePartitionedTreeImpl(port, this); //new LongIndexImpl(this); //
//      impl = new NativePartitionedTreeImpl(port, this); //new LongIndexImpl(this); //
//      impl = new ObjectIndexImpl(this, comparators);
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

  public boolean getIsOpForRebalance() {
    Boolean ret = isOpForRebalance.get(Thread.currentThread().getId());
    if (ret == null) {
      return false;
    }
    return ret;
  }

  public void setIsOpForRebalance(boolean rebalance) {
    isOpForRebalance.put(Thread.currentThread().getId(), rebalance);
  }

  public Object put(Object[] key, Object id) {

    Object ret = impl.put(key, id);
    if (ret == null) {
      if (!getIsOpForRebalance()) {
        countAdded.incrementAndGet();
      }
      if (System.currentTimeMillis() - beginAdded.get() > 10 * 1_000) {
        beginAdded.set(System.currentTimeMillis());
        countAdded.set(0);
      }
    }
    return ret;
  }

  public Object remove(Object[] key) {
    Object ret = impl.remove(key);
    if (ret != null) {
      if (!getIsOpForRebalance()) {
        countAdded.decrementAndGet();
      }
      if (System.currentTimeMillis() - beginAdded.get() > 10 * 1_000) {
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

  public void addSize(int count) {
    size.addAndGet(count);
  }

  public IndexImpl getImpl() {
    return impl;
  }

  public IndexSchema getIndexSchema() {
    return indexSchema;
  }

  public TableSchema getTableSchema() {
    return tableSchema;
  }

  public void delete() {
    impl.delete();
  }

  public interface Visitor {
    boolean visit(Object[] key, Object value);
  }

  public static class MyEntry<T, V> implements Map.Entry<T, V> {
    private final T key;
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

  public long anticipatedSize(long lastCycleDuration, Object[] minKey, Object[] maxKey) {

    long size = size();//getSize(minKey, maxKey);

    if (System.currentTimeMillis() - beginAdded.get() > 10 * 1_000) {
      beginAdded.set(System.currentTimeMillis());
      countAdded.set(0);
    }

    long duration = System.currentTimeMillis() - beginAdded.get();
    if (duration == 0) {
      return size();
    }

    double rate = countAdded.get() / duration * 1000d;
//    if (rate < 150_000) {
//      return size;
//    }
    //size += (rate * (lastCycleDuration / 1_000d));

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

    if (!DONT_RETURN_MISSING_KEY) {
      for (int i = ret.size(); i < offsets.size(); i++) {
        if (lastEntry() != null) {
          ret.add(lastEntry().getKey());
        }
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
    }, 8 * DatabaseClient.SELECT_PAGE_SIZE);
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
    return visitTailMap(key, visitor, blockSize);
  }

  public ComObject traverseIndex(DatabaseServer server) {
    int blockSize = DatabaseClient.SELECT_PAGE_SIZE;
    Object[][] keys = new Object[blockSize][];
    long[] values = new long[blockSize];
    Map.Entry<Object[], Object> firstEntry = firstEntry();
    Object[] currKey = firstEntry.getKey();
    int countRet = blockSize;
    boolean first = true;
    final AtomicInteger totalCount = new AtomicInteger();
    final AtomicInteger countDeleted = new AtomicInteger();
    final AtomicInteger countAdding = new AtomicInteger();
    final long begin = System.currentTimeMillis();
    AtomicReference<Object[]> highestKey = new AtomicReference<>();
    AtomicReference<Object[]> lowestKey = new AtomicReference<>();
    AtomicLong lastLogged = new AtomicLong(System.currentTimeMillis());
    visitTailMap(currKey, new Visitor(){
      @Override
      public boolean visit(Object[] key, Object value) {
        totalCount.incrementAndGet();
        boolean skip = false;
        byte[][] records = server.getAddressMap().fromUnsafeToRecords(value);
        for (int j = 0; j < records.length; j++) {
          byte[] bytes = records[j];
          if ((Record.getDbViewFlags(bytes) & Record.DB_VIEW_FLAG_DELETING) != 0) {
            skip = true;
            countDeleted.incrementAndGet();
          }
          if ((Record.getDbViewFlags(bytes) & Record.DB_VIEW_FLAG_ADDING) != 0) {
            countAdding.incrementAndGet();
          }
        }

        if (!skip) {
          if (lowestKey.get() == null) {
            lowestKey.set(key);
          }
          highestKey.set(key);
        }
        if (System.currentTimeMillis() - lastLogged.get() > 5_000) {
          lastLogged.set(System.currentTimeMillis());
          logger.info("Index traversal progress: count={}, rate={}", totalCount, (totalCount.get() / (System.currentTimeMillis() - begin)* 1000f));
        }

        return true;
      }
    }, blockSize);

    logger.info("Index traversal finished: count={}, rate={}, lowestKey={}, highestKey={}",
        totalCount, (totalCount.get() / (System.currentTimeMillis() - begin)* 1000f),
        DatabaseCommon.keyToString(lowestKey.get()), DatabaseCommon.keyToString(highestKey.get()));

    ComObject ret = new ComObject(3);
    ret.put(ComObject.Tag.COUNT, totalCount.get());
    ret.put(ComObject.Tag.DELETE_COUNT, (long)countDeleted.get());
    ret.put(ComObject.Tag.ADD_COUNT, (long)countAdding.get());
    return ret;
  }

  public boolean visitTailMap(Object[] key, Index.Visitor visitor, int blockSize) {
    Object[][] keys = new Object[blockSize][];
    long[] values = new long[blockSize];
    int countRet = impl.tailBlock(key, blockSize, true, keys, values);
    for (int i = 0; i < countRet; i++) {
      if (!visitor.visit(keys[i], values[i])) {
        return false;
      }
    }

    while (countRet >= blockSize) {
      countRet = impl.tailBlock(keys[countRet - 1], blockSize, false, keys, values);
      for (int i = 0; i < countRet; i++) {
        if (!visitor.visit(keys[i], values[i])) {
          return false;
        }
      }
    }

    return true;
  }

  public boolean visitHeadMap(Object[] key, Index.Visitor visitor) {
    int blockSize = DatabaseClient.SELECT_PAGE_SIZE;
    return visitHeadMap(key, visitor, blockSize, true);
  }

  public boolean visitHeadMap(Object[] key, Index.Visitor visitor, boolean first) {
    int blockSize = DatabaseClient.SELECT_PAGE_SIZE;
    return visitHeadMap(key, visitor, blockSize, first);
  }

  public boolean visitHeadMap(Object[] key, Index.Visitor visitor, int blockSize, boolean first) {
    Object[][] keys = new Object[blockSize][];
    long[] values = new long[blockSize];
    int countRet = impl.headBlock(key, blockSize, false, keys, values);
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
    }

    return true;
  }

  public Map.Entry<Object[], Object> lastEntry() {
    return impl.lastEntry();
  }

  public Map.Entry<Object[], Object> firstEntry() {
    return impl.firstEntry();
  }
}
