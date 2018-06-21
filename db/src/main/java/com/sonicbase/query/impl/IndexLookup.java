package com.sonicbase.query.impl;

import com.codahale.metrics.Timer;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.*;
import com.sonicbase.procedure.StoredProcedureContextImpl;
import com.sonicbase.query.BinaryExpression;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.query.Expression;
import com.sonicbase.schema.DataType;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.server.DatabaseServer;
import com.sonicbase.server.PartitionManager;
import com.sonicbase.server.ReadManager;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.Offset;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class IndexLookup {

  private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("com.sonicbase.logger");
  private int count;
  private String indexName;
  private BinaryExpression.Operator leftOp;
  private BinaryExpression.Operator rightOp;
  private Object[] leftKey;
  private Object[] rightKey;
  private Object[] leftOriginalKey;
  private Object[] rightOriginalKey;
  private String columnName;
  private AtomicLong currOffset = new AtomicLong();
  private AtomicLong countReturned = new AtomicLong();
  private Limit limit;
  private Offset offset;
  private int schemaRetryCount;
  private AtomicReference<String> usedIndex;
  private boolean evaluateExpression;

  public int getCount() {
    return count;
  }

  public void setCount(int count) {
    this.count = count;
  }

  public void setIndexName(String indexName) {
    this.indexName = indexName;
  }

  public String getIndexName() {
    return indexName;
  }

  public void setLeftOp(BinaryExpression.Operator leftOp) {
    this.leftOp = leftOp;
  }

  public BinaryExpression.Operator getLeftOp() {
    return leftOp;
  }

  public void setRightOp(BinaryExpression.Operator rightOp) {
    this.rightOp = rightOp;
  }

  public BinaryExpression.Operator getRightOp() {
    return rightOp;
  }

  public void setLeftKey(Object[] leftKey) {
    this.leftKey = leftKey;
  }

  public Object[] getLeftKey() {
    return leftKey;
  }

  public void setRightKey(Object[] rightKey) {
    this.rightKey = rightKey;
  }

  public Object[] getRightKey() {
    return rightKey;
  }

  public void setLeftOriginalKey(Object[] leftOriginalKey) {
    this.leftOriginalKey = leftOriginalKey;
  }

  public Object[] getLeftOriginalKey() {
    return leftOriginalKey;
  }

  public void setRightOriginalKey(Object[] rightOriginalKey) {
    this.rightOriginalKey = rightOriginalKey;
  }

  public Object[] getRightOriginalKey() {
    return rightOriginalKey;
  }

  public void setColumnName(String column) {
    this.columnName = column;
  }

  public void setCurrOffset(AtomicLong currOffset) {
    this.currOffset = currOffset;
  }

  public void setCountReturned(AtomicLong countReturned) {
    this.countReturned = countReturned;
  }

  public void setLimit(Limit limit) {
    this.limit = limit;
  }

  public void setOffset(Offset offset) {
    this.offset = offset;
  }

  public void setSchemaRetryCount(int schemaRetryCount) {
    this.schemaRetryCount = schemaRetryCount;
  }

  public void setUsedIndex(AtomicReference<String> usedIndex) {
    this.usedIndex = usedIndex;
  }

  public AtomicReference<String> getUsedIndex() {
    return usedIndex;
  }

  public void setEvaluateExpression(boolean evaluateExpression) {
    this.evaluateExpression = evaluateExpression;
  }

  public boolean getEvaluateExpression() {
    return evaluateExpression;
  }

  public SelectContextImpl lookup(ExpressionImpl expression, Expression topLevelExpression) {

    DatabaseClient client = expression.getClient();
    DatabaseCommon common = client.getCommon();
    int viewVersion = expression.getViewVersion();

    Timer.Context ctx = DatabaseClient.INDEX_LOOKUP_STATS.time();
    while (true) {
      try {
        //todo: do we really want to change the view version?
        if (viewVersion == 0) {
          viewVersion = common.getSchemaVersion();
        }

        TableSchema tableSchema = common.getTables(expression.dbName).get(expression.getTableName());
        IndexSchema indexSchema = tableSchema.getIndexes().get(indexName);
        int lastShard = -1;
        boolean currPartitions = false;
        List<Integer> selectedShards = null;
        int currShardOffset = 0;
        KeyRecord[][] retKeyRecords = null;
        Object[][][] retKeys;
        Record[] recordRet = null;
        AtomicReference<Object[]> nextKey = new AtomicReference<>();
        int nextShard = expression.getNextShard();
        int localShard = expression.getNextShard();
        Object[] localLeftValue = leftKey;

        List<Object> leftValues = new ArrayList<>();
        leftValues.add(localLeftValue);

        List<Object> rightValues = new ArrayList<>();
        rightValues.add(rightKey);

        String[] fields = indexSchema.getFields();
        boolean shouldIndex = true;
        if (fields.length == 1 && !fields[0].equals(columnName)) {
          shouldIndex = false;
        }

        if (shouldIndex) {
          retKeys = null;
          retKeyRecords = null;

          String[] indexFields = indexSchema.getFields();
          int[] fieldOffsets = new int[indexFields.length];
          for (int k = 0; k < indexFields.length; k++) {
            fieldOffsets[k] = tableSchema.getFieldOffset(indexFields[k]);
          }
          Comparator[] comparators = indexSchema.getComparators();

          if (nextShard == -2) {
            return new SelectContextImpl();
          }

          currPartitions = false;
          SelectShard selectShard = new SelectShard(common, expression.getOrderByExpressions(), tableSchema, indexSchema,
              currPartitions, currShardOffset, nextShard, localShard, fieldOffsets).invoke();
          currPartitions = selectShard.isCurrPartitions();
          selectedShards = selectShard.getSelectedShards();
          nextShard = selectShard.getNextShard();
          localShard = selectShard.getLocalShard();

          usedIndex.set(indexSchema.getName());

          String[] cfields = tableSchema.getPrimaryKey();
          int[] keyOffsets = new int[cfields.length];
          for (int i = 0; i < keyOffsets.length; i++) {
            keyOffsets[i] = tableSchema.getFieldOffset(cfields[i]);
          }

          boolean keyContainsColumns = true;
          if (true || expression.getColumns() == null || expression.getColumns().size() == 0 || expression.getCounters() != null) {
            keyContainsColumns = false;
          }
          else {
            List<Integer> array = new ArrayList<>();
            for (ColumnImpl column : expression.getColumns()) {
              if (column.getTableName() == null || column.getTableName().equals(tableSchema.getName())) {
                Integer o = tableSchema.getFieldOffset(column.getColumnName());
                if (o == null) {
                  continue;
                }
                array.add(o);
              }
            }
            for (Integer columnOffset : array) {
              boolean cfound = false;
              for (int i = 0; i < keyOffsets.length; i++) {
                if (columnOffset == keyOffsets[i]) {
                  cfound = true;
                }
              }
              if (!cfound) {
                keyContainsColumns = false;
                break;
              }
            }
          }

          while (true) {
            lastShard = nextShard;
            boolean switchedShards = false;

            ComObject cobj = buildRequest(expression, topLevelExpression, localLeftValue);

            ComObject retObj = null;
            if (expression.isRestrictToThisServer()) {
              DatabaseServer server = (DatabaseServer) client.getDatabaseServer();
              ReadManager readManager = server.getMethodInvoker().getReadManager();
              retObj = readManager.indexLookup(cobj, expression.getProcedureContext());
            }
            else {
              byte[] lookupRet = client.send(null, localShard, 0, cobj, DatabaseClient.Replica.def);
              retObj = new ComObject(lookupRet);
            }

            ProcessResponse processResponse = new ProcessResponse(expression, client, tableSchema, selectedShards,
                retKeyRecords, retKeys, recordRet, nextKey, nextShard, localShard, localLeftValue, switchedShards, retObj).invoke();
            retKeyRecords = processResponse.getRetKeyRecords();
            retKeys = processResponse.getRetKeys();
            recordRet = processResponse.getRecordRet();
            nextShard = processResponse.getNextShard();
            localShard = processResponse.getLocalShard();
            localLeftValue = processResponse.getLocalLeftValue();
            if (processResponse.shouldBreak()) {
              break;
            }

            if (/*originalShard != -1 ||*/localShard == -1 || localShard == -2 ||
                (retKeys != null && retKeys.length >= count) || (recordRet != null && recordRet.length >= count)) {
              break;
            }
          }

          retKeys = loadRecordCache(expression.dbName, client, expression.isForceSelectOnServer(), expression.getColumns(),
              expression.getRecordCache(), viewVersion, expression.isRestrictToThisServer(),
              expression.getProcedureContext(), tableSchema, retKeyRecords, retKeys, recordRet, keyOffsets, keyContainsColumns);

          return new SelectContextImpl(tableSchema.getName(), indexSchema.getName(), leftOp, nextShard, nextKey.get(),
              retKeys, expression.getRecordCache(), lastShard, currPartitions);
        }
        return new SelectContextImpl();
      }
      catch (Exception e) {
        int index = ExceptionUtils.indexOfThrowable(e, SchemaOutOfSyncException.class);
        if (-1 != index) {
          continue;
        }
        throw new DatabaseException(e);
      }
      finally {
        ctx.stop();
      }
    }
  }

  @Nullable
  private Object[][][] loadRecordCache(String dbName, DatabaseClient client, boolean forceSelectOnServer, List<ColumnImpl> columns, ExpressionImpl.RecordCache recordCache, int viewVersion, boolean restrictToThisServer, StoredProcedureContextImpl procedureContext, TableSchema tableSchema, KeyRecord[][] retKeyRecords, Object[][][] retKeys, Record[] recordRet, int[] keyOffsets, boolean keyContainsColumns) throws EOFException {
    if (recordRet == null) {
      String[] indexColumns = null;
      for (Map.Entry<String, IndexSchema> entry : tableSchema.getIndices().entrySet()) {
        if (entry.getValue().isPrimaryKey()) {
          indexColumns = entry.getValue().getFields();
          break;
        }
      }
      if (retKeys != null || retKeyRecords != null) {
        if (keyContainsColumns) {
          if (retKeys != null) {
            for (int i = 0; i < retKeys.length; i++) {
              Object[][] key = retKeys[i];
              Record keyRecord = new Record(tableSchema);
              Object[] rfields = new Object[tableSchema.getFields().size()];
              keyRecord.setFields(rfields);
              for (int j = 0; j < keyOffsets.length; j++) {
                keyRecord.getFields()[keyOffsets[j]] = key[0][j];
              }

              recordCache.put(tableSchema.getName(), key[0], new ExpressionImpl.CachedRecord(keyRecord, null));
            }
          }
        }
        else {
          if (retKeyRecords != null) {
            List<ExpressionImpl.IdEntry> keysToRead = new ArrayList<>();
            for (int i = 0; i < retKeyRecords.length; i++) {
              KeyRecord[] id = retKeyRecords[i];

              Object[] key = DatabaseCommon.deserializeKey(tableSchema, id[0].getPrimaryKey());
              if (!recordCache.containsKey(tableSchema.getName(), key)) {
                keysToRead.add(new ExpressionImpl.IdEntry(i, key));
              }
            }
            ExpressionImpl.doReadRecords(dbName, client, count, forceSelectOnServer, tableSchema, keysToRead, indexColumns,
                columns, recordCache, viewVersion, restrictToThisServer, procedureContext, schemaRetryCount);
          }
        }
      }
    }
    else {
      String[] primaryKeyFields = null;
      for (Map.Entry<String, IndexSchema> entry : tableSchema.getIndices().entrySet()) {
        if (entry.getValue().isPrimaryKey()) {
          primaryKeyFields = entry.getValue().getFields();
          break;
        }
      }
      retKeys = new Object[recordRet.length][][];
      for (int i = 0; i < recordRet.length; i++) {
        Record record = recordRet[i];

        Object[] key = new Object[primaryKeyFields.length];
        for (int j = 0; j < primaryKeyFields.length; j++) {
          key[j] = record.getFields()[tableSchema.getFieldOffset(primaryKeyFields[j])];
        }

        if (retKeys[i] == null) {
          retKeys[i] = new Object[][]{key};
        }

        recordCache.put(tableSchema.getName(), key, new ExpressionImpl.CachedRecord(record, null));
      }
    }
    return retKeys;
  }

  @NotNull
  private ComObject buildRequest(ExpressionImpl expression, Expression topLevelExpression, Object[] localLeftValue) throws IOException {
    ComObject cobj = new ComObject();
    DatabaseClient client = expression.getClient();
    cobj.put(ComObject.Tag.dbName, expression.dbName);
    if (schemaRetryCount < 2) {
      cobj.put(ComObject.Tag.schemaVersion, client.getCommon().getSchemaVersion());
    }

    cobj.put(ComObject.Tag.count, count);

    cobj.put(ComObject.Tag.isExcpliciteTrans, client.isExplicitTrans());
    cobj.put(ComObject.Tag.isCommitting, client.isCommitting());
    cobj.put(ComObject.Tag.transactionId, client.getTransactionId());
    cobj.put(ComObject.Tag.viewVersion, (long)expression.getViewVersion());

    cobj.put(ComObject.Tag.isProbe, expression.isProbe());

    cobj.put(ComObject.Tag.currOffset, currOffset.get());
    cobj.put(ComObject.Tag.countReturned, countReturned.get());
    if (limit != null) {
      cobj.put(ComObject.Tag.limitLong, limit.getRowCount());
    }
    if (offset != null) {
      cobj.put(ComObject.Tag.offsetLong, offset.getOffset());
    }
    TableSchema tableSchema = client.getCommon().getTables(expression.dbName).get(expression.getTableName());
    IndexSchema indexSchema = tableSchema.getIndexes().get(indexName);

    cobj.put(ComObject.Tag.tableId, tableSchema.getTableId());
    cobj.put(ComObject.Tag.indexId, indexSchema.getIndexId());
    cobj.put(ComObject.Tag.forceSelectOnServer, expression.isForceSelectOnServer());

    if (expression.getParms() != null) {
      byte[] bytes = expression.getParms().serialize();
      cobj.put(ComObject.Tag.parms, bytes);
    }

    cobj.put(ComObject.Tag.evaluateExpression, evaluateExpression);
    if (topLevelExpression != null) {
      byte[] bytes = ExpressionImpl.serializeExpression((ExpressionImpl) topLevelExpression);
      cobj.put(ComObject.Tag.legacyExpression, bytes);
    }
    if (expression.getOrderByExpressions() != null) {
      ComArray array = cobj.putArray(ComObject.Tag.orderByExpressions, ComObject.Type.byteArrayType);
      for (int j = 0; j < expression.getOrderByExpressions().size(); j++) {
        OrderByExpressionImpl orderByExpression = expression.getOrderByExpressions().get(j);
        byte[] bytes = orderByExpression.serialize();
        array.add(bytes);
      }
    }

    if (localLeftValue != null) {
      cobj.put(ComObject.Tag.leftKey, DatabaseCommon.serializeTypedKey(localLeftValue));
    }
    if (leftOriginalKey != null) {
      cobj.put(ComObject.Tag.originalLeftKey, DatabaseCommon.serializeTypedKey(leftOriginalKey));
    }
    cobj.put(ComObject.Tag.leftOperator, leftOp.getId());

    if (rightOp != null) {
      if (rightKey != null) {
        cobj.put(ComObject.Tag.rightKey, DatabaseCommon.serializeTypedKey(rightKey));
      }

      if (rightOriginalKey != null) {
        cobj.put(ComObject.Tag.originalRightKey, DatabaseCommon.serializeTypedKey(rightOriginalKey));
      }

      //out.writeInt(rightOperator.getId());
      cobj.put(ComObject.Tag.rightOperator, rightOp.getId());
    }

    ComArray columnArray = cobj.putArray(ComObject.Tag.columnOffsets, ComObject.Type.intType);
    ExpressionImpl.writeColumns(tableSchema, expression.getColumns(), columnArray);

    if (expression.getCounters() != null) {
      ComArray array = cobj.putArray(ComObject.Tag.counters, ComObject.Type.byteArrayType);
      for (int i = 0; i < expression.getCounters().length; i++) {
        array.add(expression.getCounters()[i].serialize());
      }
    }

    if (expression.getGroupByContext() != null) {
      cobj.put(ComObject.Tag.legacyGroupContext, expression.getGroupByContext().serialize(client.getCommon()));
    }

    cobj.put(ComObject.Tag.schemaVersion, client.getCommon().getSchemaVersion());
    cobj.put(ComObject.Tag.dbName, expression.dbName);
    cobj.put(ComObject.Tag.method, "ReadManager:indexLookup");
    return cobj;
  }

  private class ProcessResponseKeys {
    private TableSchema tableSchema;
    private AtomicReference<Object[]> nextKey;
    private ComObject retObj;
    private Object[][][] currRetKeys;
    private KeyRecord[][] currRetKeyRecords;

    public ProcessResponseKeys(TableSchema tableSchema, AtomicReference<Object[]> nextKey, ComObject retObj) {
      this.tableSchema = tableSchema;
      this.nextKey = nextKey;
      this.retObj = retObj;
    }

    public Object[][][] getCurrRetKeys() {
      return currRetKeys;
    }

    public KeyRecord[][] getCurrRetKeyRecords() {
      return currRetKeyRecords;
    }

    public ProcessResponseKeys invoke() throws IOException {
      byte[] keyBytes;
      currRetKeys = null;
      ComArray keys = retObj.getArray(ComObject.Tag.keys);
      if (keys != null && keys.getArray().size() != 0) {
        currRetKeys = new Object[keys.getArray().size()][][];
        DataType.Type[] types = DatabaseCommon.deserializeKeyPrep(tableSchema, (byte[])keys.getArray().get(0));

        for (int k = 0; k < keys.getArray().size(); k++) {
          keyBytes = (byte[])keys.getArray().get(k);
          //Object[] key = DatabaseCommon.deserializeKey(tableSchema, keyBytes);
          Object[] key = DatabaseCommon.deserializeKey(tableSchema, types,  new DataInputStream(new ByteArrayInputStream(keyBytes)));
          currRetKeys[k] = new Object[][]{key};
        }
        if (/*nextKey == null && */currRetKeys.length != 0) {
          nextKey.set(currRetKeys[currRetKeys.length - 1][0]);
        }
      }

      currRetKeyRecords = null;
      ComArray keyRecords = retObj.getArray(ComObject.Tag.keyRecords);
      if (keyRecords != null && keyRecords.getArray().size() != 0) {
        currRetKeyRecords = new KeyRecord[keyRecords.getArray().size()][];
        currRetKeys = new Object[keyRecords.getArray().size()][][];

        for (int k = 0; k < keyRecords.getArray().size(); k++) {
          keyBytes = (byte[])keyRecords.getArray().get(k);
          KeyRecord keyRecord = new KeyRecord(keyBytes);
          currRetKeyRecords[k] = new KeyRecord[]{keyRecord};

          Object[] key = DatabaseCommon.deserializeKey(tableSchema, keyRecord.getPrimaryKey());
          currRetKeys[k] = new Object[][]{key};
        }
      }
      return this;
    }
  }

  private class GetNextShard {
    private boolean restrictToThisServer;
    private List<Integer> selectedShards;
    private AtomicReference<Object[]> nextKey;
    private int nextShard;
    private int localShard;
    private boolean switchedShards;

    public GetNextShard(boolean restrictToThisServer, List<Integer> selectedShards, AtomicReference<Object[]> nextKey, int nextShard, int localShard, boolean switchedShards) {
      this.restrictToThisServer = restrictToThisServer;
      this.selectedShards = selectedShards;
      this.nextKey = nextKey;
      this.nextShard = nextShard;
      this.localShard = localShard;
      this.switchedShards = switchedShards;
    }

    public int getNextShard() {
      return nextShard;
    }

    public int getLocalShard() {
      return localShard;
    }

    public boolean isSwitchedShards() {
      return switchedShards;
    }

    public GetNextShard invoke() {
      if (restrictToThisServer) {
        if (nextKey.get() == null) {
          localShard = nextShard = -2;
        }
      }
      else {
        for (int i = 0; i < selectedShards.size(); i++) {
          if (localShard == selectedShards.get(i)) {
            if (nextKey.get() == null && i >= selectedShards.size() - 1) {
              localShard = nextShard = -2;
              //System.out.println("nextKey == null && > shards");
              break;
            }
            else {
              if (nextKey.get() == null) {
                localShard = nextShard = selectedShards.get(i + 1);
                //                    nextKey = indexSchema.getCurrPartitions()[localShard - 1].getUpperKey();

                switchedShards = true;
                //System.out.println("nextKey == null, nextShard=" + localShard);
              }
              break;
            }
          }
        }
      }
      return this;
    }
  }

  private class SelectShard {
    private DatabaseCommon common;
    private List<OrderByExpressionImpl> orderByExpressions;
    private TableSchema tableSchema;
    private IndexSchema indexSchema;
    private boolean currPartitions;
    private int currShardOffset;
    private int nextShard;
    private int localShard;
    private int[] fieldOffsets;
    private List<Integer> selectedShards;

    public SelectShard(DatabaseCommon common, List<OrderByExpressionImpl> orderByExpressions, TableSchema tableSchema, IndexSchema indexSchema, boolean currPartitions, int currShardOffset, int nextShard, int localShard, int... fieldOffsets) {
      this.common = common;
      this.orderByExpressions = orderByExpressions;
      this.tableSchema = tableSchema;
      this.indexSchema = indexSchema;
      this.currPartitions = currPartitions;
      this.currShardOffset = currShardOffset;
      this.nextShard = nextShard;
      this.localShard = localShard;
      this.fieldOffsets = fieldOffsets;
    }

    public boolean isCurrPartitions() {
      return currPartitions;
    }

    public List<Integer> getSelectedShards() {
      return selectedShards;
    }

    public int getNextShard() {
      return nextShard;
    }

    public int getLocalShard() {
      return localShard;
    }

    public SelectShard invoke() {
      int lastShard;
      selectedShards = PartitionManager.findOrderedPartitionForRecord(false, true, fieldOffsets, common, tableSchema,
          indexSchema.getName(), orderByExpressions, leftOp, rightOp, leftOriginalKey, rightOriginalKey);
      if (selectedShards.size() == 0) {
        currPartitions = true;
        selectedShards = PartitionManager.findOrderedPartitionForRecord(true, false, fieldOffsets, common, tableSchema,
            indexSchema.getName(), orderByExpressions, leftOp, rightOp, leftOriginalKey, rightOriginalKey);
      }

      if (localShard == -1) {
        localShard = nextShard = selectedShards.get(currShardOffset);
        lastShard = localShard;
      }

      boolean found = false;
      for (int i = 0; i < selectedShards.size(); i++) {
        if (localShard == selectedShards.get(i)) {
          found = true;
        }
      }
      if (!found) {
        localShard = nextShard = selectedShards.get(currShardOffset);
        lastShard = localShard;
      }
      return this;
    }
  }

  private class ProcessResponse {
    private boolean myResult;
    private ExpressionImpl expression;
    private DatabaseClient client;
    private TableSchema tableSchema;
    private List<Integer> selectedShards;
    private KeyRecord[][] retKeyRecords;
    private Object[][][] retKeys;
    private Record[] recordRet;
    private AtomicReference<Object[]> nextKey;
    private int nextShard;
    private int localShard;
    private Object[] localLeftValue;
    private boolean switchedShards;
    private ComObject retObj;

    public ProcessResponse(ExpressionImpl expression, DatabaseClient client, TableSchema tableSchema, List<Integer> selectedShards, KeyRecord[][] retKeyRecords, Object[][][] retKeys, Record[] recordRet, AtomicReference<Object[]> nextKey, int nextShard, int localShard, Object[] localLeftValue, boolean switchedShards, ComObject retObj) {
      this.expression = expression;
      this.client = client;
      this.tableSchema = tableSchema;
      this.selectedShards = selectedShards;
      this.retKeyRecords = retKeyRecords;
      this.retKeys = retKeys;
      this.recordRet = recordRet;
      this.nextKey = nextKey;
      this.nextShard = nextShard;
      this.localShard = localShard;
      this.localLeftValue = localLeftValue;
      this.switchedShards = switchedShards;
      this.retObj = retObj;
    }

    boolean shouldBreak() {
      return myResult;
    }

    public KeyRecord[][] getRetKeyRecords() {
      return retKeyRecords;
    }

    public Object[][][] getRetKeys() {
      return retKeys;
    }

    public Record[] getRecordRet() {
      return recordRet;
    }

    public int getNextShard() {
      return nextShard;
    }

    public int getLocalShard() {
      return localShard;
    }

    public Object[] getLocalLeftValue() {
      return localLeftValue;
    }

    public ProcessResponse invoke() throws IOException {
      byte[] keyBytes = retObj.getByteArray(ComObject.Tag.keyBytes);
      if (keyBytes != null) {
        Object[] retKey = DatabaseCommon.deserializeKey(tableSchema, keyBytes);
        nextKey.set(retKey);
      }
      else {
        nextKey.set(null);
      }
      Long retOffset = retObj.getLong(ComObject.Tag.currOffset);
      if (retOffset != null) {
        currOffset.set(retOffset);
      }
      Long retCountReturned = retObj.getLong(ComObject.Tag.countReturned);
      if (retCountReturned != null) {
        countReturned.set(retCountReturned);
      }
      GetNextShard getNextShard = new GetNextShard(expression.isRestrictToThisServer(), selectedShards, nextKey, nextShard,
          localShard, switchedShards).invoke();
      nextShard = getNextShard.getNextShard();
      localShard = getNextShard.getLocalShard();
      switchedShards = getNextShard.isSwitchedShards();

      ProcessResponseKeys processResponseKeys = new ProcessResponseKeys(tableSchema, nextKey, retObj).invoke();
      Object[][][] currRetKeys = processResponseKeys.getCurrRetKeys();
      KeyRecord[][] currRetKeyRecords = processResponseKeys.getCurrRetKeyRecords();
      Record[] currRetRecords = processRetRecords(expression.dbName, client, tableSchema, nextKey, retObj);

      recordRet = ExpressionImpl.aggregateResults(recordRet, currRetRecords);
      retKeys = ExpressionImpl.aggregateResults(retKeys, currRetKeys);
      retKeyRecords = ExpressionImpl.aggregateResults(retKeyRecords, currRetKeyRecords);

      if (recordRet == null && retKeys == null && retKeyRecords == null) {
        nextKey.set(localLeftValue);
      }

      if (switchedShards && logger.isDebugEnabled()) {
        //long id = (Long) currRetRecords[currRetRecords.length - 1].getFields()[tableSchema.getFieldOffset("id")];
        logger.debug("Switched shards: id=" + (nextKey.get() == null ? "null" : (long)nextKey.get()[0]) +
            ", retLen=" + (recordRet == null ? 0 : recordRet.length) + ", count=" + count + ", nextShard=" + nextShard);
      }

      processRetCounters(expression.getCounters(), retObj);

      processRetGroupBy(expression.dbName, client, expression.getGroupByContext(), retObj);

      if (switchedShards && logger.isDebugEnabled()) {
        long id = currRetRecords.length == 0 ? -1 : (Long) currRetRecords[currRetRecords.length - 1].getFields()[tableSchema.getFieldOffset("id")];
        logger.debug("Switched shards: id=" + id +
            ", retLen=" + (recordRet == null ? 0 : recordRet.length) + ", count=" + count + ", nextShard=" + nextShard);
      }

      if (limit != null) {
        long tmpOffset = 1;
        if (offset != null) {
          tmpOffset = offset.getOffset();
        }
        if (currOffset.get() >= tmpOffset + limit.getRowCount() - 1) {
          nextShard = -2;
          nextKey.set(null);
          myResult = true;
          return this;
        }
      }

      localLeftValue = nextKey.get();
      myResult = false;
      return this;
    }

    @NotNull
    private Record[] processRetRecords(String dbName, DatabaseClient client, TableSchema tableSchema, AtomicReference<Object[]> nextKey, ComObject retObj) {
      ComArray records = retObj.getArray(ComObject.Tag.records);
      Record[] currRetRecords = new Record[records == null ? 0 : records.getArray().size()];
      if (currRetRecords.length > 0) {
        String[] primaryKeyFields = null;
        int[] primaryKeyOffsets = null;
        for (Map.Entry<String, IndexSchema> entry : tableSchema.getIndices().entrySet()) {
          if (entry.getValue().isPrimaryKey()) {
            primaryKeyFields = entry.getValue().getFields();
            primaryKeyOffsets = new int[primaryKeyFields.length];
            for (int i = 0; i < primaryKeyFields.length; i++) {
              primaryKeyOffsets[i] = tableSchema.getFieldOffset(primaryKeyFields[i]);
            }
            break;
          }
        }

        for (int k = 0; k < currRetRecords.length; k++) {
          byte[] recordBytes = (byte[])records.getArray().get(k);
          try {
            currRetRecords[k] = new Record(dbName, client.getCommon(), recordBytes, null, false);
          }
          catch (Exception e) {
            throw e;
          }
        }
        if (/*nextKey == null &&*/ currRetRecords.length != 0) {
          Object[] key = new Object[primaryKeyFields.length];
          for (int j = 0; j < primaryKeyFields.length; j++) {
            key[j] = currRetRecords[currRetRecords.length - 1].getFields()[primaryKeyOffsets[j]];
          }
          nextKey.set(key);
        }
      }
      return currRetRecords;
    }

    private void processRetCounters(Counter[] counters, ComObject retObj) throws IOException {
      Counter[] retCounters = null;
      ComArray countersArray = retObj.getArray(ComObject.Tag.counters);
      if (countersArray != null) {
        retCounters = new Counter[countersArray.getArray().size()];
        for (int i = 0; i < retCounters.length; i++) {
          retCounters[i] = new Counter();
          retCounters[i].deserialize((byte[])countersArray.getArray().get(i));
        }
        System.arraycopy(retCounters, 0, counters, 0, Math.min(counters.length, retCounters.length));
      }
    }

    private void processRetGroupBy(String dbName, DatabaseClient client, GroupByContext groupByContext, ComObject retObj) throws IOException {
      byte[] groupBytes = retObj.getByteArray(ComObject.Tag.legacyGroupContext);
      if (groupBytes != null) {
        groupByContext.deserialize(groupBytes, client.getCommon(), dbName);
      }
    }
  }
}
