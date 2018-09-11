package com.sonicbase.server;

import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.common.Record;
import com.sonicbase.procedure.RecordImpl;
import com.sonicbase.query.BinaryExpression;
import com.sonicbase.query.impl.ExpressionImpl;
import com.sonicbase.schema.TableSchema;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.sonicbase.client.DatabaseClient.OPTIMIZED_RANGE_PAGE_SIZE;

@SuppressWarnings({"squid:S1172", "squid:S1168", "squid:S00107"})
// all methods called from method invoker must have cobj and replayed command parms
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class IndexLookupWithExpression extends IndexLookup {
  public IndexLookupWithExpression(DatabaseServer server) {
    super(server);
  }

  @Override
  public Map.Entry<Object[], Object> lookup() {

    final AtomicInteger countSkipped = new AtomicInteger();

    Map.Entry<Object[], Object> entry = getFirstEntry();

    while (entry != null) {
      if (retRecords.size() >= count) {
        return entry;
      }

      CheckForRightIsDone checkForRightIsDone = new CheckForRightIsDone(entry).invoke();
      entry = checkForRightIsDone.getEntry();
      if (checkForRightIsDone.is()) {
        return entry;
      }

      ProcessRecord processRecord = new ProcessRecord(countSkipped, entry).invoke();
      entry = processRecord.getEntry();
      if (processRecord.is()) {
        return entry;
      }
      if (ascending == null || ascending) {
        entry = index.higherEntry((entry.getKey()));
      }
      else {
        entry = index.lowerEntry((entry.getKey()));
      }
    }
    return entry;
  }

  private Map.Entry<Object[], Object> getFirstEntry() {
    Map.Entry<Object[], Object> entry;
    if (ascending == null || ascending) {
      if (leftKey == null) {
        entry = index.firstEntry();
      }
      else {
        entry = index.floorEntry(leftKey);
      }
    }
    else {
      if (leftKey == null) {
        entry = index.lastEntry();
      }
      else {
        entry = index.ceilingEntry(leftKey);
      }
    }
    return entry;
  }


  private class ProcessWithExpression {
    private boolean myResult;
    private Map.Entry<Object[], Object> entry;
    private byte[][] records;

    ProcessWithExpression(Map.Entry<Object[], Object> entry, byte[]... records) {
      this.entry = entry;
      this.records = records;
    }

    boolean shouldBreak() {
      return myResult;
    }

    public Map.Entry<Object[], Object> getEntry() {
      return entry;
    }

    public ProcessWithExpression invoke() {
      for (byte[] bytes : records) {
        Record record = new Record(tableSchema);
        record.deserialize(dbName, server.getCommon(), bytes, null, true);
        boolean pass = (Boolean) ((ExpressionImpl) expression).evaluateSingleRecord(
            new TableSchema[]{tableSchema}, new Record[]{record}, parms);
        if (pass) {
          ProcessWithExpression process = handlePassingRecord(bytes, record, pass);
          if (process.shouldBreak()) {
            myResult = true;
            return this;
          }
        }
      }
      myResult = false;
      return this;
    }

    private ProcessWithExpression handlePassingRecord(byte[] bytes, Record record, boolean pass) {
      if (procedureContext != null) {
        if (procedureContext.getRecordEvaluator() == null) {
          pass = true;
        }
        else {
          RecordImpl procedureRecord = new RecordImpl();
          procedureRecord.setRecord(record);
          procedureRecord.setDatabase(dbName);
          procedureRecord.setTableSchema(tableSchema);
          procedureRecord.setCommon(server.getCommon());
          procedureRecord.setViewVersion((int) record.getDbViewNumber());
          procedureRecord.setIsDeleting((record.getDbViewFlags() & Record.DB_VIEW_FLAG_DELETING) != 0);
          procedureRecord.setIsAdding((record.getDbViewFlags() & Record.DB_VIEW_FLAG_ADDING) != 0);
          pass = procedureContext.getRecordEvaluator().evaluate(procedureContext, procedureRecord);
        }
      }
      if (pass) {
        ProcessWithExpression process = handlePassingRecord(bytes);
        if (process.shouldBreak()) {
          myResult = true;
          return this;
        }
      }
      myResult = false;
      return this;
    }

    private ProcessWithExpression handlePassingRecord(byte[] bytes) {
      byte[][] currRecords = new byte[][]{bytes};
      records = processViewFlags(viewVersion, records);
      if (records != null) {
        byte[][] currRet = evaluateCounters(columnOffsets, currRecords);
        if (counters == null) {
          for (byte[] currBytes : currRet) {
            if (doProcessRecord(currBytes)) {
              return this;
            }
          }
        }
      }
      return this;
    }

    private boolean doProcessRecord(byte[] currBytes) {
      boolean localDone = false;
      boolean include = true;
      long targetOffset = 1;
      currOffset.incrementAndGet();
      if (offset != null) {
        targetOffset = offset;
        if (currOffset.get() < offset) {
          include = false;
        }
      }
      if (include && limit != null && currOffset.get() >= targetOffset + limit) {
        include = false;
        localDone = true;
      }
      if (include) {
        retRecords.add(currBytes);
      }
      if (localDone) {
        entry = null;
        myResult = true;
        return true;
      }
      return false;
    }
  }

  private class ProcessWithoutExpression {
    private boolean myResult;
    private Map.Entry<Object[], Object> entry;
    private byte[][] records;

    ProcessWithoutExpression(Map.Entry<Object[], Object> entry, byte[]... records) {
      this.entry = entry;
      this.records = records;
    }

    boolean shoudBreak() {
      return myResult;
    }

    public Map.Entry<Object[], Object> getEntry() {
      return entry;
    }

    public ProcessWithoutExpression invoke() {
      records = processViewFlags(viewVersion, records);
      if (records != null) {
        byte[][] retRecordsArray = evaluateCounters(columnOffsets, records);
        if (counters == null) {
          for (byte[] currBytes : retRecordsArray) {
            if (doProcessRecord(currBytes)) {
              return this;
            }
          }
        }
      }
      myResult = false;
      return this;
    }

    private boolean doProcessRecord(byte[] currBytes) {
      boolean localDone = false;
      boolean include = true;
      long targetOffset = 1;
      currOffset.incrementAndGet();
      if (offset != null) {
        targetOffset = offset;
        if (currOffset.get() < offset) {
          include = false;
        }
      }
      if (include && limit != null && currOffset.get() >= targetOffset + limit) {
        include = false;
        localDone = true;
      }
      if (include) {
        retRecords.add(currBytes);
      }
      if (localDone) {
        entry = null;
        myResult = true;
        return true;
      }
      return false;
    }
  }

  private class ProcessRecord {
    private boolean myResult;
    private final AtomicInteger countSkipped;
    private Map.Entry<Object[], Object> entry;

    ProcessRecord(AtomicInteger countSkipped, Map.Entry<Object[], Object> entry) {
      this.countSkipped = countSkipped;
      this.entry = entry;
    }

    boolean is() {
      return myResult;
    }

    public Map.Entry<Object[], Object> getEntry() {
      return entry;
    }

    public ProcessRecord invoke() {
      boolean shouldProcess = handleProbe();
      if (shouldProcess) {
        byte[][] records = null;
        Object[] key = entry.getKey();
        synchronized (index.getMutex(key)) {
          Object value = index.get(key);
          if (value != null && !value.equals(0L)) {
            records = server.getAddressMap().fromUnsafeToRecords(value);
          }
        }
        if (expression != null && records != null) {
          ProcessWithExpression processWithExpression = new ProcessWithExpression(entry, records).invoke();
          entry = processWithExpression.getEntry();
          if (processWithExpression.shouldBreak()) {
            myResult = true;
            return this;
          }
        }
        else {
          ProcessWithoutExpression processWithoutExpression = new ProcessWithoutExpression(entry, records).invoke();
          entry = processWithoutExpression.getEntry();
          if (processWithoutExpression.shoudBreak()) {
            myResult = true;
            return this;
          }
        }
      }
      myResult = false;
      return this;
    }

    private boolean handleProbe() {
      boolean shouldProcess = true;
      if (isProbe) {
        if (countSkipped.incrementAndGet() < OPTIMIZED_RANGE_PAGE_SIZE) {
          shouldProcess = false;
        }
        else {
          countSkipped.set(0);
        }
      }
      return shouldProcess;
    }
  }

  private class CheckForRightIsDone {
    private boolean myResult;
    private Map.Entry<Object[], Object> entry;

    CheckForRightIsDone(Map.Entry<Object[], Object> entry) {
      this.entry = entry;
    }

    boolean is() {
      return myResult;
    }

    public Map.Entry<Object[], Object> getEntry() {
      return entry;
    }

    public CheckForRightIsDone invoke() {
      boolean rightIsDone = false;
      int compareRight = 1;
      if (rightOperator != null) {
        if (rightKey != null) {
          compareRight = DatabaseCommon.compareKey(indexSchema.getComparators(), entry.getKey(), rightKey);
        }
        if (rightOperator.equals(BinaryExpression.Operator.LESS) && compareRight >= 0) {
          rightIsDone = true;
        }
        if (rightOperator.equals(BinaryExpression.Operator.LESS_EQUAL) && compareRight > 0) {
          rightIsDone = true;
        }
        if (rightIsDone) {
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
