package com.sonicbase.server;

import com.amazonaws.transform.MapEntry;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.index.Index;
import com.sonicbase.query.BinaryExpression;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class IndexLookupOneKey extends IndexLookup {

  public IndexLookupOneKey(DatabaseServer server) {
    super(server);
  }

  public Map.Entry<Object[], Object> lookup() {
    Map.Entry<Object[], Object> entry = null;

    final AtomicInteger countSkipped = new AtomicInteger();

    prepareNullKey();

    if (originalLeftKey != null && leftOperator.equals(BinaryExpression.Operator.equal)) {
      handleLookupEquals();
      return null;
    }

    if ((ascending == null || ascending)) {
      entry = getStartingOneKeyAscending(entry);
    }
    else {
      entry = getStartingOneKeyDescending(entry);
    }
    if (entry != null) {
      entry = adjustStartingOneKey(entry);

      Map.Entry[] entries = new Map.Entry[]{entry};

      outer:
      while (true) {
        if (retRecords.size() >= count || retKeyRecords.size() >= count) {
          break;
        }
        for (Map.Entry<Object[], Object> currEntry : entries) {
          entry = currEntry;

          if (currEntry == null) {
            break outer;
          }
          CheckForEndOfTraversal checkForEndOfTraversal = new CheckForEndOfTraversal(entry, currEntry).invoke();
          entry = checkForEndOfTraversal.getEntry();
          if (checkForEndOfTraversal.shouldBreak()) {
            break outer;
          }

          if (excludeKeys != null) {
            for (Object[] excludeKey : excludeKeys) {
              if (server.getCommon().compareKey(indexSchema.getComparators(), excludeKey, leftKey) == 0) {
                continue outer;
              }
            }
          }
          byte[][] currKeyRecords = null;
          byte[][] records = null;

          boolean shouldProcess = true;
          shouldProcess = handleProbe(countSkipped, shouldProcess);
          if (shouldProcess) {
            ProcessKey processKey = new ProcessKey(entry, currEntry, currKeyRecords, records).invoke();
            if (processKey.shouldBreak()) {
              break outer;
            }
            entry = processKey.getEntry();
          }

          if (leftOperator.equals(BinaryExpression.Operator.equal)) {
            entry = null;
            break outer;
          }
        }
        if (entry == null) {
          break outer;
        }
        final int diff = Math.max(retRecords.size(), retKeyRecords.size());
        if (count - diff <= 0) {
          break outer;
        }

        entries = visitMap(entry, diff);

        if (entries == null || entries.length == 0) {
          entry = null;
          break outer;
        }
      }
    }
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

  @NotNull
  private Map.Entry[] visitMap(Map.Entry<Object[], Object> entry, final int diff) {
    Map.Entry[] entries;
    if (ascending != null && !ascending) {
      final AtomicInteger countRead = new AtomicInteger();
      final List<MapEntry<Object[], Object>> currEntries = new ArrayList<>();
      index.visitHeadMap(entry.getKey(), new Index.Visitor() {
        @Override
        public boolean visit(Object[] key, Object value) throws IOException {
          MapEntry<Object[], Object> curr = new MapEntry<>();
          curr.setKey(key);
          curr.setValue(value);
          currEntries.add(curr);
          if (countRead.incrementAndGet() >= count - diff) {
            return false;
          }
          return true;
        }
      });
      entries = currEntries.toArray(new Map.Entry[currEntries.size()]);
    }
    else {
      final AtomicInteger countRead = new AtomicInteger();
      final List<MapEntry<Object[], Object>> currEntries = new ArrayList<>();
      final AtomicBoolean first = new AtomicBoolean(true);
      index.visitTailMap(entry.getKey(), new Index.Visitor() {
        @Override
        public boolean visit(Object[] key, Object value) throws IOException {
          if (first.get()) {
            first.set(false);
            return true;
          }
          MapEntry<Object[], Object> curr = new MapEntry<>();
          curr.setKey(key);
          curr.setValue(value);
          currEntries.add(curr);
          if (countRead.incrementAndGet() >= count - diff) {
            return false;
          }
          return true;
        }
      });
      entries = currEntries.toArray(new Map.Entry[currEntries.size()]);
    }
    return entries;
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
      if (originalLeftKey == null) {
        entry = index.lastEntry();
      }
      else {
        if (ascending != null && !ascending && originalLeftKey != null &&
            (leftOperator.equals(BinaryExpression.Operator.less) || leftOperator.equals(BinaryExpression.Operator.lessEqual))) {
          entry = index.floorEntry(originalLeftKey);
          if (entry == null) {
            entry = index.lastEntry();
          }
        }
        else if (ascending != null && !ascending && originalLeftKey != null &&
            (leftOperator.equals(BinaryExpression.Operator.greater) || leftOperator.equals(BinaryExpression.Operator.greaterEqual))) {
          if (entry == null) {
            entry = index.lastEntry();
          }
        }
      }
    }
    else {
      entry = index.floorEntry(leftKey);
      if (entry == null) {
        entry = index.lastEntry();
      }

      if (leftOperator.equals(BinaryExpression.Operator.less) ||
          leftOperator.equals(BinaryExpression.Operator.lessEqual) ||
          leftOperator.equals(BinaryExpression.Operator.greater) ||
          leftOperator.equals(BinaryExpression.Operator.greaterEqual)) {
        boolean foundMatch = leftKey != null && 0 == server.getCommon().compareKey(indexSchema.getComparators(), entry.getKey(), leftKey);
        if (foundMatch) {
          //todo: match below
          entry = index.lowerEntry((entry.getKey()));
        }
        else if (leftOperator.equals(BinaryExpression.Operator.less) ||
            leftOperator.equals(BinaryExpression.Operator.greater)) {
          foundMatch = originalLeftKey != null && 0 == server.getCommon().compareKey(indexSchema.getComparators(), entry.getKey(), originalLeftKey);
          if (foundMatch) {
            //todo: match below
            entry = index.lowerEntry((entry.getKey()));
          }
        }
      }
    }
    return entry;
  }

  @Nullable
  private Map.Entry<Object[], Object> getStartingOneKeyAscending(Map.Entry<Object[], Object> entry) {
    if (leftKey == null) {
      if (originalLeftKey == null) {
        entry = index.firstEntry();
      }
      else if (leftOperator.equals(BinaryExpression.Operator.greater) || leftOperator.equals(BinaryExpression.Operator.greaterEqual)) {
        entry = index.floorEntry(originalLeftKey);
        if (entry == null) {
          entry = index.firstEntry();
        }
      }
      else if (leftOperator.equals(BinaryExpression.Operator.less) || leftOperator.equals(BinaryExpression.Operator.lessEqual)) {
        entry = index.firstEntry();
      }
    }
    else {
      entry = index.ceilingEntry(leftKey);
      if (entry == null) {
        entry = index.firstEntry();
      }
    }
    if (entry != null) {

      if (leftOperator.equals(BinaryExpression.Operator.less) ||
          leftOperator.equals(BinaryExpression.Operator.lessEqual) ||
          leftOperator.equals(BinaryExpression.Operator.greater) ||
          leftOperator.equals(BinaryExpression.Operator.greaterEqual)) {
        boolean foundMatch = leftKey != null && 0 == server.getCommon().compareKey(indexSchema.getComparators(), entry.getKey(), leftKey);
        if (foundMatch) {
          //todo: match below
          entry = index.higherEntry((entry.getKey()));
        }
        else if (leftOperator.equals(BinaryExpression.Operator.less) ||
            leftOperator.equals(BinaryExpression.Operator.greater)) {
          foundMatch = originalLeftKey != null && 0 == server.getCommon().compareKey(indexSchema.getComparators(), entry.getKey(), originalLeftKey);
          if (foundMatch) {
            //todo: match below
            entry = index.higherEntry((entry.getKey()));
          }
        }
      }

    }
    return entry;
  }

  @Nullable
  private Map.Entry<Object[], Object> adjustStartingOneKey(Map.Entry<Object[], Object> entry) {
    if ((ascending != null && !ascending)) {
      if (leftKey != null && (leftOperator.equals(BinaryExpression.Operator.less) ||
          leftOperator.equals(BinaryExpression.Operator.lessEqual) || leftOperator.equals(BinaryExpression.Operator.greaterEqual))) {
        boolean foundMatch = leftKey != null && 0 == server.getCommon().compareKey(indexSchema.getComparators(), entry.getKey(), leftKey);
        if (foundMatch) {
          //todo: match below
          entry = index.lowerEntry((entry.getKey()));
        }
      }
      else if (leftOperator.equals(BinaryExpression.Operator.less) || leftOperator.equals(BinaryExpression.Operator.greater)) {
        boolean foundMatch = originalLeftKey != null && 0 == server.getCommon().compareKey(indexSchema.getComparators(), entry.getKey(), originalLeftKey);
        if (foundMatch) {
          //todo: match below
          entry = index.lowerEntry((entry.getKey()));
        }
      }
    }
    else {
      if (leftKey != null && (leftOperator.equals(BinaryExpression.Operator.greater) ||
          leftOperator.equals(BinaryExpression.Operator.greaterEqual))) {
        while (entry != null && leftKey != null) {
          int compare = server.getCommon().compareKey(indexSchema.getComparators(), entry.getKey(), leftKey);
          if (compare <= 0) {
            entry = index.higherEntry(entry.getKey());
          }
          else {
            break;
          }
        }
      }
      else if (leftOperator.equals(BinaryExpression.Operator.greaterEqual)) {
        while (entry != null && leftKey != null) {
          int compare = server.getCommon().compareKey(indexSchema.getComparators(), entry.getKey(), leftKey);
          if (compare < 0) {
            entry = index.higherEntry(entry.getKey());
          }
          else {
            break;
          }
        }
      }
    }
    return entry;
  }

  private void handleLookupEquals() {
    Map.Entry<Object[], Object> entry = null;
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
    List<Map.Entry<Object[], Object>> entries = null;
    if (!hasNull && originalLeftKey.length == indexSchema.getFields().length &&
        (indexSchema.isPrimaryKey() || indexSchema.isUnique())) {
      byte[][] records = null;
      Object[][] currKeys = null;
      byte[][] currKeyRecords = null;
      Object value = null;

      boolean shouldProcess = true;
      shouldProcess = handleProbe(countSkipped, shouldProcess);
      if (shouldProcess) {
        value = index.get(originalLeftKey);
        if (value != null && !value.equals(0L)) {
          if (keys) {
            currKeyRecords = server.getAddressMap().fromUnsafeToKeys(value);
          }
          else {
            records = server.getAddressMap().fromUnsafeToRecords(value);
          }
        }
        if (value != null) {
          Object[] keyToUse = leftKey;
          if (keyToUse == null) {
            keyToUse = originalLeftKey;
          }

          AtomicBoolean done = new AtomicBoolean();
          handleRecord(viewVersion, keyToUse, evaluateExpression, records, currKeyRecords, done);
          if (done.get()) {
            return;
          }
        }
      }

    }
    else {
      entries = index.equalsEntries(originalLeftKey);
      if (entries != null) {
        for (Map.Entry<Object[], Object> currEntry : entries) {
          entry = currEntry;
          if (server.getCommon().compareKey(indexSchema.getComparators(), originalLeftKey, entry.getKey()) != 0) {
            break;
          }
          Object value = entry.getValue();
          if (value == null) {
            break;
          }
          if (excludeKeys != null) {
            for (Object[] excludeKey : excludeKeys) {
              if (server.getCommon().compareKey(indexSchema.getComparators(), excludeKey, originalLeftKey) == 0) {
                return;
              }
            }
          }
          byte[][] records = null;
          Object[][] currKeys = null;
          byte[][] currKeyRecords = null;

          boolean shouldProcess = true;

          shouldProcess = handleProbe(countSkipped, shouldProcess);

          if (shouldProcess) {
            value = entry.getValue();
            if (value != null && !value.equals(0L)) {
              if (keys) {
                currKeyRecords = server.getAddressMap().fromUnsafeToKeys(value);
              }
              else {
                records = server.getAddressMap().fromUnsafeToRecords(value);
              }
            }
            Object[] keyToUse = leftKey;
            if (keyToUse == null) {
              keyToUse = originalLeftKey;
            }
            if (value != null) {
              AtomicBoolean done = new AtomicBoolean();
              handleRecord(viewVersion, keyToUse, evaluateExpression, records, currKeyRecords, done);
              if (done.get()) {
                return;
              }
            }
          }
        }
      }
    }
  }

  private class CheckForEndOfTraversal {
    private boolean myResult;
    private Map.Entry<Object[], Object> entry;
    private Map.Entry<Object[], Object> currEntry;

    public CheckForEndOfTraversal(Map.Entry<Object[], Object> entry, Map.Entry<Object[], Object> currEntry) {
      this.entry = entry;
      this.currEntry = currEntry;
    }

    boolean shouldBreak() {
      return myResult;
    }

    public Map.Entry<Object[], Object> getEntry() {
      return entry;
    }

    public CheckForEndOfTraversal invoke() {
      if (originalLeftKey != null) {
        int compare = server.getCommon().compareKey(indexSchema.getComparators(), currEntry.getKey(), originalLeftKey);
        if (compare == 0 &&
            (leftOperator.equals(BinaryExpression.Operator.less) || leftOperator.equals(BinaryExpression.Operator.greater))) {
          entry = null;
          myResult = true;
          return this;
        }
        if (compare == 1 && (ascending == null || ascending) && leftOperator.equals(BinaryExpression.Operator.lessEqual)) {
          entry = null;
          myResult = true;
          return this;
        }
        if (compare == 1 && (ascending == null || ascending) && leftOperator.equals(BinaryExpression.Operator.less)) {
          entry = null;
          myResult = true;
          return this;
        }
        if (compare == -1 && (ascending != null && !ascending) && leftOperator.equals(BinaryExpression.Operator.greaterEqual)) {
          entry = null;
          myResult = true;
          return this;
        }
        if (compare == -1 && (ascending != null && !ascending) && leftOperator.equals(BinaryExpression.Operator.greater)) {
          entry = null;
          myResult = true;
          return this;
        }
      }
      myResult = false;
      return this;
    }
  }

  private class ProcessKey {
    private boolean myResult;
    private Map.Entry<Object[], Object> entry;
    private Map.Entry<Object[], Object> currEntry;
    private byte[][] currKeyRecords;
    private byte[][] records;

    public ProcessKey(Map.Entry<Object[], Object> entry, Map.Entry<Object[], Object> currEntry,
                      byte[][] currKeyRecords, byte[]... records) {
      this.entry = entry;
      this.currEntry = currEntry;
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
      if (keys) {
        Object unsafeAddress = currEntry.getValue();
        if (unsafeAddress != null && !unsafeAddress.equals(0L)) {
          currKeyRecords = server.getAddressMap().fromUnsafeToKeys(unsafeAddress);
        }
      }
      else {
        Object unsafeAddress = currEntry.getValue();
        if (unsafeAddress != null && !unsafeAddress.equals(0L)) {
          records = server.getAddressMap().fromUnsafeToRecords(unsafeAddress);
        }
      }
      Object[] keyToUse = currEntry.getKey();
      if (keyToUse == null) {
        keyToUse = originalLeftKey;
      }
      if (currEntry.getValue() != null) {
        AtomicBoolean done = new AtomicBoolean();
        handleRecord(viewVersion, keyToUse, evaluateExpression, records, currKeyRecords, done);
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
}
