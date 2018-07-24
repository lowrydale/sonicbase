package com.sonicbase.server;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.query.BinaryExpression;
import com.sonicbase.query.DatabaseException;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class IndexLookupTwoKeys extends IndexLookup {

  public IndexLookupTwoKeys(DatabaseServer server) {
    super(server);
  }

  @Override
  public Map.Entry<Object[], Object> lookup() {
    Map.Entry<Object[], Object> entry = null;
    try {
      final AtomicInteger countSkipped = new AtomicInteger();
      BinaryExpression.Operator greaterOp = leftOperator;
      Object[] greaterKey = leftKey;
      Object[] greaterOriginalKey = originalLeftKey;
      BinaryExpression.Operator lessOp = rightOperator;
      Object[] lessKey = leftKey;
      Object[] lessOriginalKey = originalRightKey;
      if (greaterOp == BinaryExpression.Operator.LESS ||
          greaterOp == BinaryExpression.Operator.LESS_EQUAL) {
        greaterOp = rightOperator;
        greaterKey = leftKey;
        greaterOriginalKey = originalRightKey;
        lessOp = leftOperator;
        lessKey = leftKey;
        lessOriginalKey = originalLeftKey;
      }

      boolean useGreater = false;
      GetStartingKey getStartingKey = new GetStartingKey(entry, greaterKey, greaterOriginalKey, lessKey, lessOriginalKey, useGreater).invoke();
      entry = getStartingKey.getEntry();
      greaterKey = getStartingKey.getGreaterKey();
      lessKey = getStartingKey.getLessKey();
      useGreater = getStartingKey.isUseGreater();

      if (entry != null) {
        AdjustStartingKey adjustStartingKey = new AdjustStartingKey(entry, greaterOp, greaterKey, greaterOriginalKey, lessOp, lessKey, lessOriginalKey, useGreater).invoke();
        entry = adjustStartingKey.getEntry();
        Object[] key = adjustStartingKey.getKey();

        entry = traverseIndex(entry, countSkipped, greaterOp, greaterOriginalKey, lessOp, lessOriginalKey, key);
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
    return entry;
  }

  private Map.Entry<Object[], Object> traverseIndex(Map.Entry<Object[], Object> entry, AtomicInteger countSkipped, BinaryExpression.Operator greaterOp, Object[] greaterOriginalKey, BinaryExpression.Operator lessOp, Object[] lessOriginalKey, Object[] key) {
    outer:
    while (entry != null) {
      if (retKeyRecords.size() >= count || retRecords.size() >= count) {
        break;
      }

      if (key != null) {
        if (excludeKeys != null) {
          for (Object[] excludeKey : excludeKeys) {
            if (DatabaseCommon.compareKey(indexSchema.getComparators(), excludeKey, key) == 0) {
              continue outer;
            }
          }
        }

        CheckIfRightIsDone checkIfRightIsDone = new CheckIfRightIsDone(entry, lessOp, lessOriginalKey).invoke();
        entry = checkIfRightIsDone.getEntry();
        if (checkIfRightIsDone.is()) {
          break;
        }
      }
      byte[][] currKeyRecords = null;
      byte[][] records = null;
      boolean shouldProcess = true;

      if (isProbe) {
        if (countSkipped.incrementAndGet() < DatabaseClient.OPTIMIZED_RANGE_PAGE_SIZE) {
          shouldProcess = false;
        }
        else {
          countSkipped.set(0);
        }
      }
      if (shouldProcess) {
        ProcessKey processKey = new ProcessKey(entry, currKeyRecords, records).invoke();
        entry = processKey.getEntry();
        if (processKey.shouldBreak()) {
          break outer;
        }
      }
      if (ascending != null && !ascending) {
        entry = index.lowerEntry(entry.getKey());
      }
      else {
        entry = index.higherEntry(entry.getKey());
      }
      if (entry != null) {
        if (entry.getKey() == null) {
          throw new DatabaseException("entry key is null");
        }
        if (lessOriginalKey == null) {
          throw new DatabaseException("original less key is null");
        }
        CheckForEndTraversal checkForEndTraversal = new CheckForEndTraversal(entry, greaterOp, greaterOriginalKey, lessOp, lessOriginalKey).invoke();
        entry = checkForEndTraversal.getEntry();
        if (checkForEndTraversal.shouldBreak()) {
          break;
        }
      }
    }
    return entry;
  }

  private class GetStartingKey {
    private Map.Entry<Object[], Object> entry;
    private Object[] greaterKey;
    private Object[] greaterOriginalKey;
    private Object[] lessKey;
    private Object[] lessOriginalKey;
    private boolean useGreater;

    public GetStartingKey(Map.Entry<Object[], Object> entry, Object[] greaterKey, Object[] greaterOriginalKey, Object[] lessKey, Object[] lessOriginalKey, boolean useGreater) {
      this.entry = entry;
      this.greaterKey = greaterKey;
      this.greaterOriginalKey = greaterOriginalKey;
      this.lessKey = lessKey;
      this.lessOriginalKey = lessOriginalKey;
      this.useGreater = useGreater;
    }

    public Map.Entry<Object[], Object> getEntry() {
      return entry;
    }

    public Object[] getGreaterKey() {
      return greaterKey;
    }

    public Object[] getLessKey() {
      return lessKey;
    }

    public boolean isUseGreater() {
      return useGreater;
    }

    public GetStartingKey invoke() {
      if (ascending == null || ascending) {
        useGreater = true;
        if (greaterKey != null) {
          entry = index.ceilingEntry(greaterKey);
          lessKey = originalLeftKey;
        }
        else {
          if (greaterOriginalKey == null) {
            entry = index.firstEntry();
          }
          else {
            entry = index.ceilingEntry(greaterOriginalKey);
          }
        }
        if (entry == null) {
          entry = index.firstEntry();
        }
      }
      else {
        if (lessKey != null) {
          entry = index.floorEntry(lessKey);
          greaterKey = originalRightKey;
        }
        else {
          if (lessOriginalKey == null) {
            entry = index.lastEntry();
          }
          else {
            entry = index.floorEntry(lessOriginalKey);
          }
        }
        if (entry == null) {
          entry = index.lastEntry();
        }
      }
      return this;
    }
  }

  private class AdjustStartingKey {
    private Map.Entry<Object[], Object> entry;
    private BinaryExpression.Operator greaterOp;
    private Object[] greaterKey;
    private Object[] greaterOriginalKey;
    private BinaryExpression.Operator lessOp;
    private Object[] lessKey;
    private Object[] lessOriginalKey;
    private boolean useGreater;
    private Object[] key;

    public AdjustStartingKey(Map.Entry<Object[], Object> entry, BinaryExpression.Operator greaterOp, Object[] greaterKey, Object[] greaterOriginalKey, BinaryExpression.Operator lessOp, Object[] lessKey, Object[] lessOriginalKey, boolean useGreater) {
      this.entry = entry;
      this.greaterOp = greaterOp;
      this.greaterKey = greaterKey;
      this.greaterOriginalKey = greaterOriginalKey;
      this.lessOp = lessOp;
      this.lessKey = lessKey;
      this.lessOriginalKey = lessOriginalKey;
      this.useGreater = useGreater;
    }

    public Map.Entry<Object[], Object> getEntry() {
      return entry;
    }

    public Object[] getKey() {
      return key;
    }

    public AdjustStartingKey invoke() {
      key = lessKey;
      key = greaterOriginalKey;
      rightKey = lessKey;

      if ((ascending != null && !ascending)) {
        adjustStartingKeyDescending();
      }
      else {
        adjustStartingKeyAscending();
      }
      if (entry != null && lessKey != null) {
        if (useGreater) {
          int compareValue = DatabaseCommon.compareKey(indexSchema.getComparators(), entry.getKey(), greaterOriginalKey);
          if ((0 == compareValue || -1 == compareValue) && greaterOp == BinaryExpression.Operator.GREATER) {
            entry = null;
          }
        }
        else {
          int compareValue = DatabaseCommon.compareKey(indexSchema.getComparators(), entry.getKey(), lessKey);
          if ((0 == compareValue || 1 == compareValue) && lessOp == BinaryExpression.Operator.LESS) {
            entry = null;
          }
          if (1 == compareValue) {
            entry = null;
          }
        }
      }
      return this;
    }

    private void adjustStartingKeyAscending() {
      if (greaterKey != null) {
        if (greaterOp.equals(BinaryExpression.Operator.LESS) ||
            greaterOp.equals(BinaryExpression.Operator.LESS_EQUAL) ||
            greaterOp.equals(BinaryExpression.Operator.GREATER) ||
            greaterOp.equals(BinaryExpression.Operator.GREATER_EQUAL)) {
          boolean foundMatch = key != null && 0 == DatabaseCommon.compareKey(indexSchema.getComparators(), entry.getKey(), greaterKey);
          if (foundMatch) {
            entry = index.higherEntry((entry.getKey()));
          }
        }
      }
      else if (greaterOriginalKey != null && greaterOp.equals(BinaryExpression.Operator.GREATER)) {
        boolean foundMatch = 0 == DatabaseCommon.compareKey(indexSchema.getComparators(), entry.getKey(), greaterOriginalKey);
        if (foundMatch) {
          entry = index.higherEntry((entry.getKey()));
        }
      }
    }

    private void adjustStartingKeyDescending() {
      if (lessKey != null && lessOriginalKey != lessKey) {
        if (lessOp.equals(BinaryExpression.Operator.LESS) ||
            lessOp.equals(BinaryExpression.Operator.LESS_EQUAL) ||
            lessOp.equals(BinaryExpression.Operator.GREATER) ||
            lessOp.equals(BinaryExpression.Operator.GREATER_EQUAL)) {
          boolean foundMatch = 0 == DatabaseCommon.compareKey(indexSchema.getComparators(), entry.getKey(), lessKey);
          if (foundMatch) {
            entry = index.lowerEntry((entry.getKey()));
          }
        }
      }
      else if (lessOriginalKey != null && lessOp.equals(BinaryExpression.Operator.LESS)) {
        boolean foundMatch = 0 == DatabaseCommon.compareKey(indexSchema.getComparators(), entry.getKey(), lessOriginalKey);
        if (foundMatch) {
          entry = index.lowerEntry((entry.getKey()));
        }
      }
    }
  }

  private class ProcessKey {
    private boolean myResult;
    private Map.Entry<Object[], Object> entry;
    private byte[][] currKeyRecords;
    private byte[][] records;

    public ProcessKey(Map.Entry<Object[], Object> entry, byte[][] currKeyRecords, byte[]... records) {
      this.entry = entry;
      this.currKeyRecords = currKeyRecords;
      this.records = records;
    }

    boolean shouldBreak() {
      return myResult;
    }

    public Map.Entry<Object[], Object> getEntry() {
      return entry;
    }

    public ProcessKey invoke() {
      if (entry.getValue() != null && !entry.getValue().equals(0L)) {
        if (keys) {
          currKeyRecords = server.getAddressMap().fromUnsafeToKeys(entry.getValue());
        }
        else {
          records = server.getAddressMap().fromUnsafeToRecords(entry.getValue());
        }
      }
      if (keys) {
        Object unsafeAddress = entry.getValue();
        currKeyRecords = server.getAddressMap().fromUnsafeToKeys(unsafeAddress);
      }

      if (entry.getValue() != null) {
        Object[] keyToUse = entry.getKey();
        if (keyToUse == null) {
          keyToUse = originalLeftKey;
        }

        AtomicBoolean done = new AtomicBoolean();
        handleRecord(viewVersion, keyToUse, evaluateExpression,
            records, currKeyRecords, done);
        if (done.get()) {
          entry = null;
          myResult = true;
          return this;
        }
      }
      myResult = false;
      return this;
    }
  }

  private class CheckForEndTraversal {
    private boolean myResult;
    private Map.Entry<Object[], Object> entry;
    private BinaryExpression.Operator greaterOp;
    private Object[] greaterOriginalKey;
    private BinaryExpression.Operator lessOp;
    private Object[] lessOriginalKey;

    public CheckForEndTraversal(Map.Entry<Object[], Object> entry, BinaryExpression.Operator greaterOp, Object[] greaterOriginalKey, BinaryExpression.Operator lessOp, Object... lessOriginalKey) {
      this.entry = entry;
      this.greaterOp = greaterOp;
      this.greaterOriginalKey = greaterOriginalKey;
      this.lessOp = lessOp;
      this.lessOriginalKey = lessOriginalKey;
    }

    boolean shouldBreak() {
      return myResult;
    }

    public Map.Entry<Object[], Object> getEntry() {
      return entry;
    }

    public CheckForEndTraversal invoke() {
      int compareValue = DatabaseCommon.compareKey(indexSchema.getComparators(), entry.getKey(), lessOriginalKey);
      if ((0 == compareValue || 1 == compareValue) && lessOp == BinaryExpression.Operator.LESS) {
        entry = null;
        myResult = true;
        return this;
      }
      if (1 == compareValue) {
        entry = null;
        myResult = true;
        return this;
      }
      compareValue = 1;
      if (greaterOriginalKey != null) {
        compareValue = DatabaseCommon.compareKey(indexSchema.getComparators(), entry.getKey(), greaterOriginalKey);
      }
      if (0 == compareValue && greaterOp == BinaryExpression.Operator.GREATER) {
        entry = null;
        myResult = true;
        return this;
      }
      if (-1 == compareValue) {
        entry = null;
        myResult = true;
        return this;
      }
      myResult = false;
      return this;
    }
  }

  private class CheckIfRightIsDone {
    private boolean myResult;
    private Map.Entry<Object[], Object> entry;
    private BinaryExpression.Operator lessOp;
    private Object[] lessOriginalKey;

    public CheckIfRightIsDone(Map.Entry<Object[], Object> entry, BinaryExpression.Operator lessOp, Object... lessOriginalKey) {
      this.entry = entry;
      this.lessOp = lessOp;
      this.lessOriginalKey = lessOriginalKey;
    }

    boolean is() {
      return myResult;
    }

    public Map.Entry<Object[], Object> getEntry() {
      return entry;
    }

    public CheckIfRightIsDone invoke() {
      boolean rightIsDone = false;
      int compareRight = 1;
      if (lessOriginalKey != null) {
        compareRight = DatabaseCommon.compareKey(indexSchema.getComparators(), entry.getKey(), lessOriginalKey);
      }
      if (lessOp.equals(BinaryExpression.Operator.LESS) && compareRight >= 0) {
        rightIsDone = true;
      }
      if (lessOp.equals(BinaryExpression.Operator.LESS_EQUAL) && compareRight > 0) {
        rightIsDone = true;
      }
      if (rightIsDone) {
        entry = null;
        myResult = true;
        return this;
      }
      myResult = false;
      return this;
    }
  }
}
