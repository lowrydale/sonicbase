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
import com.sonicbase.query.impl.OrderByExpressionImpl;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public abstract class IndexLookup {
  private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("com.sonicbase.logger");

  protected BinaryExpression.Operator rightOperator;
  protected int count;
  private boolean isExplicitTrans;
  private boolean isCommiting;
  private Long transactionId;
  protected long viewVersion;
  protected boolean isProbe;
  protected boolean forceSelectOnServer;
  protected ParameterHandler parms;
  protected boolean evaluateExpression;
  protected Expression expression;
  protected String dbName;
  private String tableName;
  protected IndexSchema indexSchema;
  private String indexName;
  private List<OrderByExpressionImpl> orderByExpressions;
  protected Object[] leftKey;
  protected Object[] originalLeftKey;
  protected BinaryExpression.Operator leftOperator;
  protected Object[] rightKey;
  protected Object[] originalRightKey;
  protected Set<Integer> columnOffsets;
  protected Counter[] counters;
  protected GroupByContext groupContext;
  protected AtomicLong countReturned;
  protected Index index;
  protected Boolean ascending;
  protected int[] keyOffsets;
  protected List<byte[]> retKeyRecords;
  protected List<Object[]> retKeys;
  protected List<byte[]> retRecords;
  protected List<Object[]> excludeKeys;
  protected boolean keys;
  protected final DatabaseServer server;
  protected TableSchema tableSchema;
  protected short serializationVersion;
  protected AtomicLong currOffset;
  protected Long offset;
  protected Long limit;
  protected StoredProcedureContextImpl procedureContext;

  public IndexLookup(DatabaseServer server) {
    this.server = server;
  }

  public void setRightOperator(BinaryExpression.Operator rightOperator) {
    this.rightOperator = rightOperator;
  }

  public void setCount(Integer count) {
    this.count = count;
  }

  public void setIsExplicitTrans(Boolean isExplicitTrans) {
    this.isExplicitTrans = isExplicitTrans;
  }

  public void setIsCommiting(Boolean isCommiting) {
    this.isCommiting = isCommiting;
  }

  public void setTransactionId(Long transactionId) {
    this.transactionId = transactionId;
  }

  public void setViewVersion(Long viewVersion) {
    this.viewVersion = viewVersion;
  }

  public void setIsProbe(Boolean isProbe) {
    this.isProbe = isProbe;
  }

  public void setForceSelectOnServer(Boolean forceSelectOnServer) {
    this.forceSelectOnServer = forceSelectOnServer;
  }

  public void setParms(ParameterHandler parms) {
    this.parms = parms;
  }

  public void setEvaluateExpression(Boolean evaluateExpression) {
    this.evaluateExpression = evaluateExpression;
  }

  public void setExpression(Expression expression) {
    this.expression = expression;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public void setIndexSchema(IndexSchema indexSchema) {
    this.indexSchema = indexSchema;
  }

  public void setIndexName(String indexName) {
    this.indexName = indexName;
  }

  public void setOrderByExpressions(List<OrderByExpressionImpl> orderByExpressions) {
    this.orderByExpressions = orderByExpressions;
  }

  public void setLeftKey(Object[] leftKey) {
    this.leftKey = leftKey;
  }

  public void setOriginalLeftKey(Object[] originalLeftKey) {
    this.originalLeftKey = originalLeftKey;
  }

  public void setLeftOperator(BinaryExpression.Operator leftOperator) {
    this.leftOperator = leftOperator;
  }

  public void setTableSchema(TableSchema tableSchema) {
    this.tableSchema = tableSchema;
  }

  public void setRightKey(Object[] rightKey) {
    this.rightKey = rightKey;
  }

  public void setOriginalRightKey(Object[] originalRightKey) {
    this.originalRightKey = originalRightKey;
  }

  public void setColumnOffsets(Set<Integer> columnOffsets) {
    this.columnOffsets = columnOffsets;
  }

  public void setCounters(Counter[] counters) {
    this.counters = counters;
  }

  public void setGroupContext(GroupByContext groupContext) {
    this.groupContext = groupContext;
  }

  public void setCountReturned(AtomicLong countReturned) {
    this.countReturned = countReturned;
  }

  public void setIndex(Index index) {
    this.index = index;
  }

  public void setAscending(Boolean ascending) {
    this.ascending = ascending;
  }

  public void setSerializationVersion(short serializationVersion) {
    this.serializationVersion = serializationVersion;
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

  public void setProcedureContext(StoredProcedureContextImpl procedureContext) {
    this.procedureContext = procedureContext;
  }

  public boolean isExplicitTrans() {
    return isExplicitTrans;
  }

  public boolean isCommitting() {
    return isCommiting;
  }

  public Long getTransactionId() {
    return transactionId;
  }

  public void setKeyOffsets(int[] keyOffsets) {
    this.keyOffsets = keyOffsets;
  }

  public void setRetKeyRecords(List<byte[]> retKeyRecords) {
    this.retKeyRecords = retKeyRecords;
  }

  public void setRetKeys(List<Object[]> retKeys) {
    this.retKeys = retKeys;
  }

  public void setRetRecords(List<byte[]> retRecords) {
    this.retRecords = retRecords;
  }

  public void setExcludeKeys(List<Object[]> excludeKeys) {
    this.excludeKeys = excludeKeys;
  }

  public void setKeys(boolean keys) {
    this.keys = keys;
  }

  public abstract Map.Entry<Object[], Object> lookup();

  protected byte[][] processViewFlags(long viewVersion, byte[][] records) {
    List<byte[]> remaining = new ArrayList<>();
    if (records != null) {
      for (byte[] bytes : records) {
        processViewFlags(viewVersion, remaining, bytes);
      }
      if (remaining.size() == 0) {
        records = null;
      }
      else {
        records = remaining.toArray(new byte[remaining.size()][]);
      }
    }
    return records;
  }

  private boolean processViewFlags(long viewVersion, List<byte[]> remaining, byte[] bytes) {
    long dbViewNum = Record.getDbViewNumber(bytes);
    long dbViewFlags = Record.getDbViewFlags(bytes);
    if ((dbViewNum <= viewVersion - 1) && (dbViewFlags & Record.DB_VIEW_FLAG_ADDING) != 0) {
      remaining.add(bytes);
    }
    else if ((dbViewNum == viewVersion || dbViewNum == viewVersion - 1) && (dbViewFlags & Record.DB_VIEW_FLAG_DELETING) != 0) {
      remaining.add(bytes);
    }
    else if ((dbViewFlags & Record.DB_VIEW_FLAG_DELETING) == 0) {
      remaining.add(bytes);
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

  protected byte[][] evaluateCounters(Set<Integer> columnOffsets, byte[][] records) {
    if (columnOffsets == null || columnOffsets.size() == 0) {
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

      if (counters != null) {
        count(counters, record);
      }
    }
    return ret;
  }

  protected void handleRecord(long viewVersion, Object[] key,  boolean evaluateExpresion,
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
        byte[][] ret = evaluateCounters(columnOffsets, records);
        boolean pass = true;
        if (procedureContext != null) {
          if (procedureContext.getRecordEvaluator() == null) {
            pass = true;
          }
          else {
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
        }
        if (pass) {
          if (counters == null) {
            for (byte[] currBytes : ret) {
              done.set(false);
              boolean include = true;
              currOffset.incrementAndGet();
              if (offset != null) {
                if (currOffset.get() < offset) {
                  include = false;
                }
              }
              if (include) {
                if (limit != null) {
                  if (countReturned.get() >= limit) {
                    include = false;
                    done.set(true);
                  }
                }
              }
              if (include) {
                countReturned.incrementAndGet();
                retRecords.add(currBytes);
              }
            }
          }
        }
      }
    }
  }

  protected void handleRecordEvaluateExpression(byte[][] records, AtomicBoolean done) {
    for (byte[] bytes : records) {
      Record record = new Record(tableSchema);
      record.deserialize(dbName, server.getCommon(), bytes, null, true);
      boolean pass = (Boolean) ((ExpressionImpl) expression).evaluateSingleRecord(new TableSchema[]{tableSchema}, new Record[]{record}, parms);
      if (pass) {
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
            for (byte[] currBytes : ret) {
              done.set(false);
              boolean include = true;
              currOffset.incrementAndGet();
              if (offset != null) {
                if (currOffset.get() < offset) {
                  include = false;
                }
              }
              if (include) {
                if (limit != null) {
                  if (countReturned.get() >= limit) {
                    include = false;
                    done.set(true);
                  }
                }
              }
              if (include) {
                countReturned.incrementAndGet();
                retRecords.add(currBytes);
              }
            }
          }
        }
      }
    }
  }

  private void handleRecordForKeys(long viewVersion, Object[] key, byte[][] currKeyRecords, AtomicBoolean done) {
    if (currKeyRecords != null) {
      for (byte[] currKeyRecord : currKeyRecords) {
        done.set(false);
        boolean include = true;
        currOffset.incrementAndGet();
        if (offset != null) {
          if (currOffset.get() < offset) {
            include = false;
          }
        }
        if (include) {
          if (limit != null) {
            if (countReturned.get() >= limit) {
              include = false;
              done.set(true);
            }
          }
        }
        if (include) {
          boolean passesFlags = false;
          long dbViewNum = KeyRecord.getDbViewNumber(currKeyRecord);
          long dbViewFlags = KeyRecord.getDbViewFlags(currKeyRecord);
          if ((dbViewNum <= viewVersion - 1) && (dbViewFlags & Record.DB_VIEW_FLAG_ADDING) != 0) {
            passesFlags = true;
          }
          else if ((dbViewNum == viewVersion || dbViewNum == viewVersion - 1) && (dbViewFlags & Record.DB_VIEW_FLAG_DELETING) != 0) {
            passesFlags = true;
          }
          else if ((dbViewFlags & Record.DB_VIEW_FLAG_DELETING) == 0) {
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
    }
  }
}
