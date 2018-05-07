/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.server;

import com.sonicbase.common.Record;
import com.sonicbase.procedure.RecordImpl;
import com.sonicbase.query.BinaryExpression;
import com.sonicbase.query.impl.ExpressionImpl;
import com.sonicbase.schema.TableSchema;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.sonicbase.client.DatabaseClient.OPTIMIZED_RANGE_PAGE_SIZE;

public class IndexLookupWithExpression extends IndexLookup {
  public IndexLookupWithExpression(DatabaseServer server) {
    super(server);
  }

  @Override
  public Map.Entry<Object[], Object> lookup() {

    final AtomicInteger countSkipped = new AtomicInteger();

    Map.Entry<Object[], Object> entry = getFirstEntry();

    outer:
    while (entry != null) {
      if (retRecords.size() >= count) {
        break;
      }

      boolean rightIsDone = false;
      int compareRight = 1;
      if (rightOperator != null) {
        if (rightKey != null) {
          compareRight = server.getCommon().compareKey(indexSchema.getComparators(), entry.getKey(), rightKey);
        }
        if (rightOperator.equals(BinaryExpression.Operator.less) && compareRight >= 0) {
          rightIsDone = true;
        }
        if (rightOperator.equals(BinaryExpression.Operator.lessEqual) && compareRight > 0) {
          rightIsDone = true;
        }
        if (rightIsDone) {
          entry = null;
          break;
        }
      }

      boolean shouldProcess = true;
      if (isProbe) {
        if (countSkipped.incrementAndGet() < OPTIMIZED_RANGE_PAGE_SIZE) {
          shouldProcess = false;
        }
        else {
          countSkipped.set(0);
        }
      }
      if (shouldProcess) {

        boolean forceSelectOnServer = false;
        byte[][] records = null;
        if (entry.getValue() != null && !entry.getValue().equals(0L)) {
          records = server.getAddressMap().fromUnsafeToRecords(entry.getValue());
        }
        if (parms != null && expression != null && records != null) {
          ProcessWithExpression processWithExpression = new ProcessWithExpression(entry, forceSelectOnServer, records).invoke();
          entry = processWithExpression.getEntry();
          if (processWithExpression.shouldBreak()) {
            break outer;
          }
        }
        else {
          ProcessWithoutExpression processWithoutExpression = new ProcessWithoutExpression(entry, forceSelectOnServer, records).invoke();
          entry = processWithoutExpression.getEntry();
          if (processWithoutExpression.shoudBreak()) {
            break outer;
          }
        }
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
    private boolean forceSelectOnServer;
    private byte[][] records;

    public ProcessWithExpression(Map.Entry<Object[], Object> entry, boolean forceSelectOnServer, byte[]... records) {
      this.entry = entry;
      this.forceSelectOnServer = forceSelectOnServer;
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
            AtomicBoolean done = new AtomicBoolean();
            records = processViewFlags(dbName, tableSchema, indexSchema, index, viewVersion, entry.getKey(), records, done);
            if (records != null) {

              int[] keyOffsets = null;

              byte[][] currRet = applySelectToResultRecords(serializationVersion, dbName, columnOffsets, forceSelectOnServer, currRecords,
                  entry.getKey(), tableSchema, counters, groupContext, keyOffsets);
              if (counters == null) {
                for (byte[] currBytes : currRet) {
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
                  if (include) {
                    if (limit != null) {
                      if (currOffset.get() >= targetOffset + limit) {
                        include = false;
                        localDone = true;
                      }
                    }
                  }
                  if (include) {
                    retRecords.add(currBytes);
                  }
                  if (localDone) {
                    entry = null;
                    myResult = true;
                    return this;
                  }
                }
              }
            }
          }
        }
      }
      myResult = false;
      return this;
    }
  }

  private class ProcessWithoutExpression {
    private boolean myResult;
    private Map.Entry<Object[], Object> entry;
    private boolean forceSelectOnServer;
    private byte[][] records;

    public ProcessWithoutExpression(Map.Entry<Object[], Object> entry, boolean forceSelectOnServer, byte[]... records) {
      this.entry = entry;
      this.forceSelectOnServer = forceSelectOnServer;
      this.records = records;
    }

    boolean shoudBreak() {
      return myResult;
    }

    public Map.Entry<Object[], Object> getEntry() {
      return entry;
    }

    public ProcessWithoutExpression invoke() {
      AtomicBoolean done = new AtomicBoolean();
      records = processViewFlags(dbName, tableSchema, indexSchema, index, viewVersion, entry.getKey(), records, done);
      if (records != null) {
        int[] keyOffsets = null;

        byte[][] retRecordsArray = applySelectToResultRecords(serializationVersion, dbName, columnOffsets, forceSelectOnServer, records,
            entry.getKey(), tableSchema, counters, groupContext, keyOffsets);
        if (counters == null) {
          for (byte[] currBytes : retRecordsArray) {
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
            if (include) {
              if (limit != null) {
                if (currOffset.get() >= targetOffset + limit) {
                  include = false;
                  localDone = true;
                }
              }
            }
            if (include) {
              retRecords.add(currBytes);
            }
            if (localDone) {
              entry = null;
              myResult = true;
              return this;
            }
          }
        }
      }
      myResult = false;
      return this;
    }
  }
}
