package com.sonicbase.server;

import com.sonicbase.common.KeyRecord;
import com.sonicbase.common.Record;
import com.sonicbase.index.Index;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.procedure.RecordImpl;
import com.sonicbase.procedure.StoredProcedureContextImpl;
import com.sonicbase.query.BinaryExpression;
import com.sonicbase.query.Expression;
import com.sonicbase.query.impl.Counter;
import com.sonicbase.query.impl.ExpressionImpl;
import com.sonicbase.query.impl.GroupByContext;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@SuppressWarnings({"squid:S1172", "squid:S1168", "squid:S00107"})
// all methods called from method invoker must have cobj and replayed command parms
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public abstract class IndexLookup {
  BinaryExpression.Operator rightOperator;
  protected int count;
  private boolean isExplicitTrans;
  private boolean isCommiting;
  private Long transactionId;
  long viewVersion;
  boolean isProbe;
  protected ParameterHandler parms;
  boolean evaluateExpression;
  protected Expression expression;
  protected String dbName;
  protected IndexSchema indexSchema;
  Object[] leftKey;
  Object[] originalLeftKey;
  BinaryExpression.Operator leftOperator;
  Object[] rightKey;
  Object[] originalRightKey;
  Set<Integer> columnOffsets;
  Counter[] counters;
  private GroupByContext groupContext;
  AtomicLong countReturned;
  protected Index index;
  Boolean ascending;
  List<byte[]> retKeyRecords;
  List<Object[]> retKeys;
  List<byte[]> retRecords;
  List<Object[]> excludeKeys;
  protected boolean keys;
  protected final DatabaseServer server;
  protected TableSchema tableSchema;
  protected AtomicLong currOffset;
  protected Long offset;
  protected Long limit;
  StoredProcedureContextImpl procedureContext;

  IndexLookup(DatabaseServer server) {
    this.server = server;
  }

  void setRightOperator(BinaryExpression.Operator rightOperator) {
    this.rightOperator = rightOperator;
  }

  public void setCount(Integer count) {
    this.count = count;
  }

  void setIsExplicitTrans(Boolean isExplicitTrans) {
    this.isExplicitTrans = isExplicitTrans;
  }

  void setIsCommiting(Boolean isCommiting) {
    this.isCommiting = isCommiting;
  }

  void setTransactionId(Long transactionId) {
    this.transactionId = transactionId;
  }

  void setViewVersion(Long viewVersion) {
    this.viewVersion = viewVersion;
  }

  void setIsProbe(Boolean isProbe) {
    this.isProbe = isProbe;
  }

  public void setParms(ParameterHandler parms) {
    this.parms = parms;
  }

  void setEvaluateExpression(Boolean evaluateExpression) {
    this.evaluateExpression = evaluateExpression;
  }

  public void setExpression(Expression expression) {
    this.expression = expression;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  public void setIndexSchema(IndexSchema indexSchema) {
    this.indexSchema = indexSchema;
  }

  void setLeftKey(Object[] leftKey) {
    this.leftKey = leftKey;
  }

  void setOriginalLeftKey(Object[] originalLeftKey) {
    this.originalLeftKey = originalLeftKey;
  }

  void setLeftOperator(BinaryExpression.Operator leftOperator) {
    this.leftOperator = leftOperator;
  }

  public void setTableSchema(TableSchema tableSchema) {
    this.tableSchema = tableSchema;
  }

  void setRightKey(Object[] rightKey) {
    this.rightKey = rightKey;
  }

  void setOriginalRightKey(Object[] originalRightKey) {
    this.originalRightKey = originalRightKey;
  }

  void setColumnOffsets(Set<Integer> columnOffsets) {
    this.columnOffsets = columnOffsets;
  }

  void setCounters(Counter[] counters) {
    this.counters = counters;
  }

  void setGroupContext(GroupByContext groupContext) {
    this.groupContext = groupContext;
  }

  void setCountReturned(AtomicLong countReturned) {
    this.countReturned = countReturned;
  }

  public void setIndex(Index index) {
    this.index = index;
  }

  void setAscending(Boolean ascending) {
    this.ascending = ascending;
  }

  public void setCurrOffset(AtomicLong currOffset) {
    this.currOffset = currOffset;
  }

  public void setOffset(Long offset) {
    this.offset = offset;
  }

  public void setLimit(Long limit) {
    this.limit = limit;
  }

  void setProcedureContext(StoredProcedureContextImpl procedureContext) {
    this.procedureContext = procedureContext;
  }

  boolean isExplicitTrans() {
    return isExplicitTrans;
  }

  boolean isCommitting() {
    return isCommiting;
  }

  Long getTransactionId() {
    return transactionId;
  }

  void setRetKeyRecords(List<byte[]> retKeyRecords) {
    this.retKeyRecords = retKeyRecords;
  }

  void setRetKeys(List<Object[]> retKeys) {
    this.retKeys = retKeys;
  }

  void setRetRecords(List<byte[]> retRecords) {
    this.retRecords = retRecords;
  }

  void setExcludeKeys(List<Object[]> excludeKeys) {
    this.excludeKeys = excludeKeys;
  }

  public void setKeys(boolean keys) {
    this.keys = keys;
  }

  public abstract Map.Entry<Object[], Object> lookup();

  byte[][] processViewFlags(long viewVersion, byte[][] records) {
    if (records != null) {
      byte[][] ret = new byte[records.length][];
      AtomicInteger retPos = new AtomicInteger();
      for (byte[] bytes : records) {
        processViewFlags(viewVersion, ret, retPos, bytes);
      }
      if (retPos.get() == 0) {
        records = null;
      }
      else {
        if (retPos.get() < records.length) {
          records = new byte[retPos.get()][];
          System.arraycopy(ret, 0, records, 0, retPos.get());
        }
        else {
          records = ret;
        }
      }
    }
    return records;
  }

  private boolean processViewFlags(long viewVersion, byte[][] ret, AtomicInteger retPos,  byte[] bytes) {
    long dbViewNum = Record.getDbViewNumber(bytes);
    long dbViewFlags = Record.getDbViewFlags(bytes);
    if ((dbViewNum <= viewVersion - 1) && (dbViewFlags & Record.DB_VIEW_FLAG_ADDING) != 0) {
      ret[retPos.getAndIncrement()] = bytes;
    }
    else if ((dbViewNum == viewVersion || dbViewNum == viewVersion - 1) && (dbViewFlags & Record.DB_VIEW_FLAG_DELETING) != 0) {
      ret[retPos.getAndIncrement()] = bytes;
    }
    else if ((dbViewFlags & Record.DB_VIEW_FLAG_DELETING) == 0) {
      ret[retPos.getAndIncrement()] = bytes;
    }
    else {
      return false;
    }
    return true;
  }

  private void count(Counter[] counters, Record record) {
    if (counters != null && record != null) {
      for (Counter counter : counters) {
        counter.add(record.getFields());
      }
    }
  }

  byte[][] evaluateCounters(Set<Integer> columnOffsets, byte[][] records) {
    if (columnOffsets == null || columnOffsets.isEmpty()) {
      columnOffsets = null;
    }
    byte[][] ret = new byte[records.length][];
    for (int i = 0; i < records.length; i++) {
      byte[] recordBytes = records[i];
      ret[i] = recordBytes;

      Record record = null;
      if (counters != null || groupContext != null) {
        record = new Record(dbName, server.getCommon(), recordBytes, columnOffsets, false);
      }
      if (groupContext != null) {
        doEvaluateCounters(record);
      }

      if (counters != null) {
        count(counters, record);
      }
    }
    return ret;
  }

  private void doEvaluateCounters(Record record) {
    List<GroupByContext.FieldContext> fieldContexts = groupContext.getFieldContexts();
    Object[] groupValues = new Object[fieldContexts.size()];
    boolean isNull = true;
    for (int j = 0; j < groupValues.length; j++) {
      groupValues[j] = record.getFields()[fieldContexts.get(j).getFieldOffset()];
      if (groupValues[j] != null) {
        isNull = false;
      }
    }
    if (!isNull) {
      Map<String, Map<Object[], GroupByContext.GroupCounter>> map = groupContext.getGroupCounters();
      if (map == null || map.size() == 0) {
        groupContext.addGroupContext(groupValues);
        map = groupContext.getGroupCounters();
      }
      for (Map<Object[], GroupByContext.GroupCounter> innerMap : map.values()) {
        GroupByContext.GroupCounter counter = innerMap.get(groupValues);
        if (counter == null) {
          groupContext.addGroupContext(groupValues);
          counter = innerMap.get(groupValues);
        }
        counter.getCounter().add(record.getFields());
      }
    }
  }

  void handleRecord(long viewVersion, Object[] key, boolean evaluateExpresion,
                    byte[][] records, byte[][] currKeyRecords,
                    AtomicBoolean done) {
    if (keys) {
      handleRecordForKeys(viewVersion, key, currKeyRecords, done);
    }
    else {
      handleRecordForRecords(viewVersion, evaluateExpresion, records, done);
    }
  }

  private void handleRecordForRecords(long viewVersion, boolean evaluateExpresion, byte[][] records, AtomicBoolean done) {
    records = processViewFlags(viewVersion, records);

    if (records != null) {
      if (parms != null && expression != null && evaluateExpresion) {
        handleRecordEvaluateExpression(records, done);
      }
      else {
        handleRecordWithoutEvaluatingExpression(records, done);
      }
    }
  }

  private void handleRecordWithoutEvaluatingExpression(byte[][] records, AtomicBoolean done) {
    byte[][] ret = evaluateCounters(columnOffsets, records);
    boolean pass = true;
    if (procedureContext != null && procedureContext.getRecordEvaluator() != null) {
      for (byte[] bytes : records) {
        Record record = new Record(tableSchema);
        record.deserialize(dbName, server.getCommon(), bytes, null, true);
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
    if (pass && counters == null) {
      handlePassingRecord(done, ret);
    }
    else if (pass) {
      if (limit != null && countReturned.get() >= limit) {
        done.set(true);
      }
    }
  }

  private void handlePassingRecord(AtomicBoolean done, byte[][] ret) {
    for (byte[] currBytes : ret) {
      done.set(false);
      boolean include = true;
      currOffset.incrementAndGet();
      if (offset != null && currOffset.get() < offset) {
        include = false;
      }
      if (include && limit != null && countReturned.get() >= limit) {
        include = false;
        done.set(true);
      }
      if (include) {
        countReturned.incrementAndGet();
        retRecords.add(currBytes);
      }
    }
  }

  void handleRecordEvaluateExpression(byte[][] records, AtomicBoolean done) {
    for (byte[] bytes : records) {
      Record record = new Record(tableSchema);
      record.deserialize(dbName, server.getCommon(), bytes, null, true);
      boolean pass = (Boolean) ((ExpressionImpl) expression).evaluateSingleRecord(new TableSchema[]{tableSchema},
          new Record[]{record}, parms);
      if (pass) {
        doHandleRecordEvaluateExpression(done, bytes, record, pass);
      }
    }
  }

  private void doHandleRecordEvaluateExpression(AtomicBoolean done, byte[] bytes, Record record, boolean pass) {
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
      byte[][] currRecords = new byte[][]{bytes};
      byte[][] ret = evaluateCounters(columnOffsets, currRecords);
      if (counters == null) {
        handlePassingRecord(done, ret);
      }
    }
  }

  private void handleRecordForKeys(long viewVersion, Object[] key, byte[][] currKeyRecords, AtomicBoolean done) {
    if (currKeyRecords != null) {
      for (byte[] currKeyRecord : currKeyRecords) {
        done.set(false);
        boolean include = true;
        currOffset.incrementAndGet();
        if (offset != null && currOffset.get() < offset) {
          include = false;
        }
        if (include && limit != null && countReturned.get() >= limit) {
          include = false;
          done.set(true);
        }
        if (include) {
          doHandleRecordForKeys(viewVersion, key, currKeyRecord);
        }
      }
    }
  }

  private void doHandleRecordForKeys(long viewVersion, Object[] key, byte[] currKeyRecord) {
    boolean passesFlags = false;
    long dbViewNum = KeyRecord.getDbViewNumber(currKeyRecord);
    long dbViewFlags = KeyRecord.getDbViewFlags(currKeyRecord);
    if ((dbViewNum <= viewVersion - 1) && (dbViewFlags & Record.DB_VIEW_FLAG_ADDING) != 0 ||
        (dbViewNum == viewVersion || dbViewNum == viewVersion - 1) && (dbViewFlags & Record.DB_VIEW_FLAG_DELETING) != 0 ||
        (dbViewFlags & Record.DB_VIEW_FLAG_DELETING) == 0) {
      passesFlags = true;
    }
    if (passesFlags) {
      retKeys.add(key);
      retKeyRecords.add(currKeyRecord);
      countReturned.incrementAndGet();
    }
    else {
      currOffset.decrementAndGet();
    }
  }
}
