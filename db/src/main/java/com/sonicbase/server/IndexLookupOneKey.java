package com.sonicbase.server;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.index.Index;
import com.sonicbase.query.BinaryExpression;
import com.sonicbase.schema.TableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings({"squid:S1172", "squid:S1168", "squid:S00107"})
// all methods called from method invoker must have cobj and replayed command parms
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class IndexLookupOneKey extends IndexLookup {
  private static final Logger logger = LoggerFactory.getLogger(IndexLookupOneKey.class);

  public IndexLookupOneKey(DatabaseServer server) {
    super(server);
  }

  public Map.Entry<Object[], Object> lookup() {
    Map.Entry<Object[], Object> entry = null;

    final AtomicInteger countSkipped = new AtomicInteger();

    prepareNullKey();

    if (originalLeftKey != null && leftOperator.equals(BinaryExpression.Operator.EQUAL)) {
      handleLookupEquals();
      return null;
    }

    TableSchema.Partition[] partitions = indexSchema.getCurrPartitions();


    if ((ascending == null || ascending)) {
      entry = getStartingOneKeyAscending(entry);
    }
    else {
      entry = getStartingOneKeyDescending(entry);
    }
    if (entry != null) {
      entry = adjustStartingOneKey(entry);

      Object[][] keys = new Object[count][];
      Object[] values = new Object[count];

      if (entry == null) {
        return null;
      }
      keys[0] = entry.getKey();
      values[0] = entry.getValue();
      AtomicInteger entriesPos = new AtomicInteger(1);

      entry = processEntries(partitions, entry.getKey(), entry.getValue(), countSkipped, keys, values, entriesPos);
    }
    return entry;
  }


  private class ProcessEntries {
    private TableSchema.Partition[] partitions;
    Object[][] keys;
    Object[] values;
    AtomicInteger entriesPos;
    private final AtomicInteger countSkipped;
    private boolean shouldContinue;
    private boolean shouldReturn;
    private Object[] key;
    private Object value;
    private Object[] retKey;
    private Object retValue;

    ProcessEntries(TableSchema.Partition[] partitions, Object[] key, Object value, Object[][] keys, Object[] values, AtomicInteger entriesPos, AtomicInteger countSkipped) {
      this.partitions = partitions;
      this.key = key;
      this.value = value;
      this.keys = keys;
      this.entriesPos = entriesPos;
      this.values = values;
      this.countSkipped = countSkipped;
    }

    public Object[] getRetKey() {
      return retKey;
    }

    public Object getRetValue() {
      return retValue;
    }

    public ProcessEntries invoke() {
      for (int i = 0; i < entriesPos.get(); i++) {
        retKey = keys[i];
        retValue = values[i];

        CheckForEndOfTraversal checkForEndOfTraversal = new CheckForEndOfTraversal(partitions, retKey, retValue, keys[i], values[i]).invoke();
        retKey = checkForEndOfTraversal.getRetKey();
        retValue = checkForEndOfTraversal.getRetValue();
        if (checkForEndOfTraversal.shouldBreak()) {
          shouldReturn = true;
          return this;
        }

        if (handleExcludedKeys()) {
          shouldContinue = true;
          return this;
        }

        ProcessKey processKey = new ProcessKey(countSkipped, retKey, retValue, keys[i], values[i]).invoke();
        retKey = processKey.getKey();
        retValue = processKey.getValue();
        if (processKey.shouldReturn()) {
          shouldReturn = true;
          return this;
          
        }
      }
      return this;
    }

    private boolean handleExcludedKeys() {
      if (excludeKeys != null) {
        for (Object[] excludeKey : excludeKeys) {
          if (DatabaseCommon.compareKey(indexSchema.getComparators(), excludeKey, leftKey) == 0) {
            return true;
          }
        }
      }
      return false;
    }
  }

  private Map.Entry<Object[], Object> processEntries(TableSchema.Partition[] partitions, Object[] key, Object value, AtomicInteger countSkipped,
                                                     Object[][] keys, Object[] values, AtomicInteger entriesPos) {
    while (!(retRecords.size() >= count || retKeyRecords.size() >= count)) {
      ProcessEntries process = new ProcessEntries(partitions, key, value, keys, values, entriesPos, countSkipped).invoke();
      key = process.getRetKey();
      value = process.getRetValue();
      entriesPos.set(0);

      if (process.shouldContinue) {
        continue;
      }
      if (process.shouldReturn) {
        if (key == null) {
          return null;
        }
        Map.Entry<Object[], Object> entry = new Index.MyEntry<>(key, value);
        return entry;
      }
      if (retRecords.size() >= count || retKeyRecords.size() >= count) {
        if (key == null) {
          return null;
        }
        Map.Entry<Object[], Object> entry = new Index.MyEntry<>(key, value);
        return entry;
      }

      GetNextEntries next = new GetNextEntries(key, value, keys, values, entriesPos).invoke();
      if (next.shouldRet) {
        if (next.getRetKey() == null) {
          return null;
        }
        Map.Entry<Object[], Object> entry = new Index.MyEntry<>(next.getRetKey(), next.getRetValue());
        return entry;
      }
    }
    if (key == null) {
      return null;
    }
    Map.Entry<Object[], Object> entry = new Index.MyEntry<>(key, value);
    return entry;
  }

  private boolean handleProbe(AtomicInteger countSkipped, boolean shouldProcess) {
    if (isProbe) {
      if (countSkipped.incrementAndGet() < DatabaseClient.OPTIMIZED_RANGE_PAGE_SIZE) {
        shouldProcess = false;
      }
      else {
        countSkipped.set(0);
      }
    }
    return shouldProcess;
  }


  private void prepareNullKey() {
    if (originalLeftKey == null || originalLeftKey.length == 0) {
      originalLeftKey = null;
    }
    if (originalLeftKey != null) {
      boolean found = false;
      for (int i = 0; i < originalLeftKey.length; i++) {
        if (originalLeftKey[i] != null) {
          found = true;
        }
      }
      if (!found) {
        originalLeftKey = null;
      }
    }
  }

  private Map.Entry<Object[], Object> getStartingOneKeyDescending(Map.Entry<Object[], Object> entry) {
    if (leftKey == null) {
      entry = getStartingOneKeyDescendingNullLeftKey(entry);
    }
    else {
      entry = getStartingOneKeyDescendingNotNullLeftKey();
    }
    return entry;
  }

  private Map.Entry<Object[], Object> getStartingOneKeyDescendingNotNullLeftKey() {
    Map.Entry<Object[], Object> entry;
    entry = index.floorEntry(leftKey);
    if (entry == null) {
      entry = index.lastEntry();
    }

    if (leftOperator.equals(BinaryExpression.Operator.LESS) ||
        leftOperator.equals(BinaryExpression.Operator.LESS_EQUAL) ||
        leftOperator.equals(BinaryExpression.Operator.GREATER) ||
        leftOperator.equals(BinaryExpression.Operator.GREATER_EQUAL)) {
      boolean foundMatch = 0 == DatabaseCommon.compareKey(indexSchema.getComparators(), entry.getKey(), leftKey);
      if (foundMatch) {
        entry = index.lowerEntry((entry.getKey()));
      }
      else if (leftOperator.equals(BinaryExpression.Operator.LESS) ||
          leftOperator.equals(BinaryExpression.Operator.GREATER)) {
        foundMatch = originalLeftKey != null && 0 == DatabaseCommon.compareKey(indexSchema.getComparators(),
            entry.getKey(), originalLeftKey);
        if (foundMatch) {
          entry = index.lowerEntry((entry.getKey()));
        }
      }
    }
    return entry;
  }

  private Map.Entry<Object[], Object> getStartingOneKeyDescendingNullLeftKey(Map.Entry<Object[], Object> entry) {
    if (originalLeftKey == null) {
      entry = index.lastEntry();
    }
    else {
      if (ascending != null && !ascending &&
          (leftOperator.equals(BinaryExpression.Operator.LESS) ||
              leftOperator.equals(BinaryExpression.Operator.LESS_EQUAL))) {
        entry = index.floorEntry(originalLeftKey);
        if (entry == null) {
          entry = index.lastEntry();
        }
      }
      else if (ascending != null && !ascending &&
          (leftOperator.equals(BinaryExpression.Operator.GREATER) ||
              leftOperator.equals(BinaryExpression.Operator.GREATER_EQUAL)) &&
          entry == null) {
        entry = index.lastEntry();
      }
    }
    return entry;
  }

  private Map.Entry<Object[], Object> getStartingOneKeyAscending(Map.Entry<Object[], Object> entry) {
    if (leftKey == null) {
      if (originalLeftKey == null) {
        entry = index.firstEntry();
      }
      else if (leftOperator.equals(BinaryExpression.Operator.GREATER) ||
          leftOperator.equals(BinaryExpression.Operator.GREATER_EQUAL)) {
        entry = index.floorEntry(originalLeftKey);
        if (entry == null) {
          entry = index.firstEntry();
        }
      }
      else if (leftOperator.equals(BinaryExpression.Operator.LESS) ||
          leftOperator.equals(BinaryExpression.Operator.LESS_EQUAL)) {
        entry = index.firstEntry();
      }
    }
    else {
      entry = new Index.MyEntry<>(leftKey, 0L);
      if (entry == null) {
        entry = index.firstEntry();
      }
    }
    return processFoundEntry(entry);
  }

  private Map.Entry<Object[], Object> processFoundEntry(Map.Entry<Object[], Object> entry) {
    if (entry != null && (leftOperator.equals(BinaryExpression.Operator.LESS) ||
        leftOperator.equals(BinaryExpression.Operator.LESS_EQUAL) ||
        leftOperator.equals(BinaryExpression.Operator.GREATER) ||
        leftOperator.equals(BinaryExpression.Operator.GREATER_EQUAL))) {
      boolean foundMatch = leftKey != null && 0 == DatabaseCommon.compareKey(indexSchema.getComparators(),
          entry.getKey(), leftKey);
      if (foundMatch) {
        entry = index.higherEntry((entry.getKey()));
      }
      else if (leftOperator.equals(BinaryExpression.Operator.LESS) ||
          leftOperator.equals(BinaryExpression.Operator.GREATER)) {
        foundMatch = originalLeftKey != null && 0 == DatabaseCommon.compareKey(indexSchema.getComparators(),
            entry.getKey(), originalLeftKey);
        if (foundMatch) {
          entry = index.higherEntry((entry.getKey()));
        }
      }
    }
    return entry;
  }

  private Map.Entry<Object[], Object> adjustStartingOneKey(Map.Entry<Object[], Object> entry) {
    if ((ascending != null && !ascending)) {
      entry = adjustStartignOneKeyDescending(entry);
    }
    else {
      entry = adjustStartingOneKeyAscending(entry);
    }
    return entry;
  }

  private Map.Entry<Object[], Object> adjustStartingOneKeyAscending(Map.Entry<Object[], Object> entry) {
    if (leftKey != null && (leftOperator.equals(BinaryExpression.Operator.GREATER) ||
        leftOperator.equals(BinaryExpression.Operator.GREATER_EQUAL))) {
      entry = adjustStartingOneKeyAscendingLeftKeyNotNull(entry);
    }
    else if (leftOperator.equals(BinaryExpression.Operator.GREATER_EQUAL)) {
      while (entry != null && leftKey != null) {
        int compare = DatabaseCommon.compareKey(indexSchema.getComparators(), entry.getKey(), leftKey);
        if (compare < 0) {
          entry = index.higherEntry(entry.getKey());
        }
        else {
          break;
        }
      }
    }
    return entry;
  }

  private Map.Entry<Object[], Object> adjustStartingOneKeyAscendingLeftKeyNotNull(Map.Entry<Object[], Object> entry) {
    while (entry != null) {
      int compare = DatabaseCommon.compareKey(indexSchema.getComparators(), entry.getKey(), leftKey);
      if (compare <= 0) {
        entry = index.higherEntry(entry.getKey());
      }
      else {
        break;
      }
    }
    return entry;
  }

  private Map.Entry<Object[], Object> adjustStartignOneKeyDescending(Map.Entry<Object[], Object> entry) {
    if (leftKey != null && (leftOperator.equals(BinaryExpression.Operator.LESS) ||
        leftOperator.equals(BinaryExpression.Operator.LESS_EQUAL) ||
        leftOperator.equals(BinaryExpression.Operator.GREATER_EQUAL))) {
      boolean foundMatch = 0 == DatabaseCommon.compareKey(indexSchema.getComparators(), entry.getKey(), leftKey);
      if (foundMatch) {
        entry = index.lowerEntry((entry.getKey()));
      }
    }
    else if (leftOperator.equals(BinaryExpression.Operator.LESS) || leftOperator.equals(BinaryExpression.Operator.GREATER)) {
      boolean foundMatch = originalLeftKey != null &&
          0 == DatabaseCommon.compareKey(indexSchema.getComparators(), entry.getKey(), originalLeftKey);
      if (foundMatch) {
        entry = index.lowerEntry((entry.getKey()));
      }
    }
    return entry;
  }

  private void handleLookupEquals() {
    final AtomicInteger countSkipped = new AtomicInteger();

    if (originalLeftKey == null) {
      return;
    }

    boolean hasNull = false;
    for (Object part : originalLeftKey) {
      if (part == null) {
        hasNull = true;
        break;
      }
    }

    if (!hasNull && originalLeftKey.length == indexSchema.getFields().length &&
        (indexSchema.isPrimaryKey() || indexSchema.isUnique())) {
      processFullKeyForUniqueIndex(countSkipped);
    }
    else {
      processPartialKeyOrNonUniqueIndex(countSkipped);
    }
  }

  private void processPartialKeyOrNonUniqueIndex(AtomicInteger countSkipped) {
    List<Map.Entry<Object[], Object>> entries;
    Map.Entry<Object[], Object> entry;
    entries = index.equalsEntries(originalLeftKey);
    if (entries != null) {
      for (Map.Entry<Object[], Object> currEntry : entries) {
        entry = currEntry;
        if (processPartialKeyOrNonUniqueIndexExitEarly(entry)) {
          return;
        }
        byte[][] records = null;
        byte[][] currKeyRecords = null;
        boolean shouldProcess = true;

        shouldProcess = handleProbe(countSkipped, shouldProcess);

        if (doProcessPartialKeyOrNonUniqueIndex(entry, records, currKeyRecords, shouldProcess)) {
          return;
        }
      }
    }
  }

  private boolean processPartialKeyOrNonUniqueIndexExitEarly(Map.Entry<Object[], Object> entry) {
    if (DatabaseCommon.compareKey(indexSchema.getComparators(), originalLeftKey, entry.getKey()) != 0) {
      return true;
    }
    Object value = entry.getValue();
    if (value == null) {
      return true;
    }
    if (excludeKeys != null) {
      for (Object[] excludeKey : excludeKeys) {
        if (DatabaseCommon.compareKey(indexSchema.getComparators(), excludeKey, originalLeftKey) == 0) {
          return true;
        }
      }
    }
    return false;
  }

  private boolean doProcessPartialKeyOrNonUniqueIndex(Map.Entry<Object[], Object> entry, byte[][] records,
                                                      byte[][] currKeyRecords, boolean shouldProcess) {
    Object value;
    if (shouldProcess) {
      Object[] key = entry.getKey();
      synchronized (index.getMutex(key)) {
        value = index.get(key);
        if (value != null && !value.equals(0L)) {
          if (keys) {
            currKeyRecords = server.getAddressMap().fromUnsafeToKeys(value);
          }
          else {
            records = server.getAddressMap().fromUnsafeToRecords(value);
          }
        }
      }
      Object[] keyToUse = leftKey;
      if (keyToUse == null) {
        keyToUse = originalLeftKey;
      }
      if (value != null) {
        AtomicBoolean done = new AtomicBoolean();
        handleRecord(viewVersion, keyToUse, evaluateExpression, records, currKeyRecords, done);
        return done.get();
      }
    }
    return false;
  }

  private void processFullKeyForUniqueIndex(AtomicInteger countSkipped) {
    byte[][] records = null;
    byte[][] currKeyRecords = null;
    Object value = null;

    boolean shouldProcess = true;
    shouldProcess = handleProbe(countSkipped, shouldProcess);
    if (shouldProcess) {
      synchronized (index.getMutex(originalLeftKey)) {
        value = index.get(originalLeftKey);
        if (value != null && !value.equals(0L)) {
          if (keys) {
            currKeyRecords = server.getAddressMap().fromUnsafeToKeys(value);
          }
          else {
            records = server.getAddressMap().fromUnsafeToRecords(value);
          }
        }
      }
      if (value != null) {
        Object[] keyToUse = leftKey;
        if (keyToUse == null) {
          keyToUse = originalLeftKey;
        }

        AtomicBoolean done = new AtomicBoolean();
        handleRecord(viewVersion, keyToUse, evaluateExpression, records, currKeyRecords, done);
      }
    }
  }

  private class CheckForEndOfTraversal {
    private final Object[] currKey;
    private final Object currValue;
    private final TableSchema.Partition[] partitions;
    private Object[] key;
    private Object value;
    private boolean myResult;

    public CheckForEndOfTraversal(TableSchema.Partition[] partitions, Object[] key, Object value, Object[] currKey, Object currValue) {
      this.partitions = partitions;
      this.key = key;
      this.value = value;
      this.currKey = currKey;
      this.currValue = currValue;
    }

    boolean shouldBreak() {
      return myResult;
    }

    public Object[] getRetKey() {
      return key;
    }

    public Object getRetValue() {
      return value;
    }

    public CheckForEndOfTraversal invoke() {
      if (currKey == null) {
        key = null;
        value = null;
        myResult = true;
        return this;
      }

      if (originalLeftKey != null) {
        int compare = DatabaseCommon.compareKey(indexSchema.getComparators(), currKey, originalLeftKey);
        if (compare == 0 &&
            (leftOperator.equals(BinaryExpression.Operator.LESS) || leftOperator.equals(BinaryExpression.Operator.GREATER))) {
          key = null;
          value = null;
          myResult = true;
          return this;
        }

        if (keyOnDifferentServer()) {
          key = null;
          value = null;
          myResult = true;
          return this;
        }

        if (handleCompareIsGreater(compare)) {
          return this;
        }

        if (handleCompareIsLess(compare)) {
          return this;
        }
      }
      myResult = false;
      return this;
    }

    private boolean keyOnDifferentServer() {
      int shard = server.getShard();
      if (shard == 0) {
        Object[] upperKey = partitions[shard].getUpperKey();
        if (upperKey != null) {
          if (DatabaseCommon.compareKey(index.getComparators(), currKey, upperKey) > 0) {
            return true;
          }
        }
      }
      else {
        Object[] upperKey = partitions[shard].getUpperKey();
        if (upperKey != null) {
          if (DatabaseCommon.compareKey(index.getComparators(), currKey, upperKey) > 0) {
            return true;
          }
        }
        Object[] lowerKey = partitions[shard - 1].getUpperKey();
        if (lowerKey != null) {
          if (DatabaseCommon.compareKey(index.getComparators(), currKey, lowerKey) < 0) {
            return true;
          }
        }
      }
      return false;
    }

    private boolean handleCompareIsLess(int compare) {
      if (compare == -1 && (ascending != null && !ascending) && leftOperator.equals(BinaryExpression.Operator.GREATER_EQUAL)) {
        key = null;
        value = null;
        myResult = true;
        return true;
      }
      if (compare == -1 && (ascending != null && !ascending) && leftOperator.equals(BinaryExpression.Operator.GREATER)) {
        key = null;
        value = null;
        myResult = true;
        return true;
      }
      return false;
    }

    private boolean handleCompareIsGreater(int compare) {
      if (compare == 1 && (ascending == null || ascending) && leftOperator.equals(BinaryExpression.Operator.LESS_EQUAL)) {
        key = null;
        value = null;
        myResult = true;
        return true;
      }
      if (compare == 1 && (ascending == null || ascending) && leftOperator.equals(BinaryExpression.Operator.LESS)) {
        key = null;
        value = null;
        myResult = true;
        return true;
      }
      return false;
    }
  }

  private class ProcessKey {
    private final AtomicInteger countSkipped;
    private final Object[] currKey;
    private final Object currValue;
    private Object[] key;
    private Object value;
    private boolean myResult;

    ProcessKey(AtomicInteger countSkipped, Object[] key, Object value, Object[] currKey, Object currValue) {
      this.countSkipped = countSkipped;
      this.key = key;
      this.value = value;
      this.currKey = currKey;
      this.currValue = currValue;
    }

    boolean shouldReturn() {
      return myResult;
    }

    public Object[] getKey() {
      return key;
    }

    public Object getValue() {
      return value;
    }

    public ProcessKey invoke() {
      byte[][] currKeyRecords = null;
      byte[][] records = null;

      boolean shouldProcess = true;
      shouldProcess = handleProbe(countSkipped, shouldProcess);
      if (shouldProcess) {
        GetRecords getRecords = new GetRecords(currKeyRecords, records).invoke();
        currKeyRecords = getRecords.getCurrKeyRecords();
        records = getRecords.getRecords();
        Object[] keyToUse = currKey;
        if (keyToUse == null) {
          keyToUse = originalLeftKey;
        }
        if (currValue != null) {
          AtomicBoolean done = new AtomicBoolean();
          handleRecord(viewVersion, keyToUse, evaluateExpression, records, currKeyRecords, done);
          if (done.get()) {
            key = null;
            value = null;
            myResult = true;
            return this;
          }
        }
      }
      if (leftOperator.equals(BinaryExpression.Operator.EQUAL)) {
        key = null;
        value = null;
        myResult = true;
        return this;
      }

      myResult = false;
      return this;
    }

    private class GetRecords {
      private byte[][] currKeyRecords;
      private byte[][] records;

      GetRecords(byte[][] currKeyRecords, byte[]... records) {
        this.currKeyRecords = currKeyRecords;
        this.records = records;
      }

      byte[][] getCurrKeyRecords() {
        return currKeyRecords;
      }

      public byte[][] getRecords() {
        return records;
      }

      public GetRecords invoke() {
        Object unsafeAddress = currValue;
        if (keys) {
          if (unsafeAddress != null && !unsafeAddress.equals(0L)) {
            currKeyRecords = server.getAddressMap().fromUnsafeToKeys(unsafeAddress);
          }
        }
        else {
          if (unsafeAddress != null && !unsafeAddress.equals(0L)) {
            records = server.getAddressMap().fromUnsafeToRecords(unsafeAddress);
          }
        }
        return this;
      }
    }
  }

  public class MapEntry<K, V> implements Map.Entry<K, V> {
    private K key;
    private V value;

    public MapEntry() {
    }

    public K getKey() {
      return this.key;
    }

    public V getValue() {
      return this.value;
    }

    public V setValue(V value) {
      this.value = value;
      return this.value;
    }

    public K setKey(K key) {
      this.key = key;
      return this.key;
    }
  }


  private class GetNextEntries {
    private final AtomicInteger entriesPos;
    private final Object[] key;
    private final Object value;
    private Object[] retKey;
    private Object retValue;
    private final Object[][] keys;
    private final Object[] values;
    private boolean shouldRet;

    GetNextEntries(Object[] key, Object value, Object[][] keys, Object[] values, AtomicInteger entriesPos) {
      this.key = key;
      this.value = value;
      this.keys = keys;
      this.values = values;
      this.entriesPos = entriesPos;
    }

    public Object[] getRetKey() {
      return retKey;
    }

    public Object getRetValue() {
      return retValue;
    }

    public GetNextEntries invoke() {
      if (key == null) {
        shouldRet = true;
        retKey = null;
        retValue = null;
        return this;
      }
      final int diff = Math.max(retRecords.size(), retKeyRecords.size());
      if (count - diff <= 0) {
        shouldRet = true;
        retKey = key;
        retValue = value;
        return this;
      }

      visitMap(key, value, keys, values, entriesPos, diff);

      if (entriesPos.get() == 0) {
        shouldRet = true;
        retKey = null;
        retValue = null;
        return this;
      }
      shouldRet = false;
      retKey = key;
      retValue = value;
      return this;
    }

    private void visitMap(Object[] key, Object value, Object[][] keys, Object[] values, AtomicInteger entriesPos, final int diff) {
      if (ascending != null && !ascending) {
        final AtomicInteger countRead = new AtomicInteger();
        final AtomicBoolean first = new AtomicBoolean(true);
        index.visitHeadMap(key, (currKey, currValue) -> {
//          if (first.get()) {
//            first.set(false);
//            return true;
//          }
          first.set(false);
          keys[entriesPos.get()] = currKey;
          values[entriesPos.get()] = currValue;
          entriesPos.getAndIncrement();

          return countRead.incrementAndGet() < count - diff;
        }, first.get());
      }
      else {
        final AtomicInteger countRead = new AtomicInteger();
        final AtomicBoolean first = new AtomicBoolean(true);
        index.visitTailMap(key, (currKey, currValue) -> {
          if (first.get()) {
            first.set(false);
            return true;
          }
          keys[entriesPos.get()] = currKey;
          values[entriesPos.get()] = currValue;
          entriesPos.getAndIncrement();
          return countRead.incrementAndGet() < count - diff;
        }, count + 2);
      }
    }
  }
}
