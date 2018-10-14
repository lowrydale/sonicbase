package com.sonicbase.query.impl;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.ComObject;
import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.common.Record;
import com.sonicbase.common.SchemaOutOfSyncException;
import com.sonicbase.procedure.StoredProcedureContextImpl;
import com.sonicbase.query.BinaryExpression;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.query.DeleteStatement;
import com.sonicbase.query.Expression;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.PartitionUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class DeleteStatementImpl extends StatementImpl implements DeleteStatement {
  private final DatabaseClient client;
  private final ExpressionImpl.RecordCache recordCache;
  private String tableName;
  private ExpressionImpl expression;

  public DeleteStatementImpl(DatabaseClient client) {
    this.client = client;
    this.recordCache = new ExpressionImpl.RecordCache();
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName.toLowerCase();
  }


  public ExpressionImpl getExpression() {
    return expression;
  }

  public void setWhereClause(Expression expression) {
    this.expression = (ExpressionImpl) expression;
  }

  @Override
  public Object execute(String dbName, String sqlToUse, SelectStatementImpl.Explain explain, Long sequence0,
                        Long sequence1, Short sequence2,
                        boolean restrictToThisServer, StoredProcedureContextImpl procedureContext, int schemaRetryCount) {
    while (true) {
      try {
        expression.setViewVersion(client.getCommon().getSchemaVersion());
        expression.setTableName(tableName);
        expression.setClient(client);
        expression.setParms(getParms());
        expression.setTopLevelExpression(expression);
        expression.setRecordCache(recordCache);
        expression.setDbName(dbName);

        TableSchema tableSchema = client.getCommon().getSchema(dbName).getTables().get(tableName);

        Integer replica = expression.getReplica();
        if (replica == null) {
          int replicaCount = client.getCommon().getServersConfig().getShards()[0].getReplicas().length;
          replica = ThreadLocalRandom.current().nextInt(0, replicaCount);
          expression.setReplica(replica);
        }

        Random rand = new Random(System.currentTimeMillis());
        int countDeleted = 0;
        while (true) {
          DoDelete doDelete = new DoDelete(dbName, explain, sequence0, sequence1, sequence2, restrictToThisServer,
              procedureContext, schemaRetryCount, tableSchema, rand, countDeleted).invoke();
          countDeleted = doDelete.getCountDeleted();
          if (doDelete.is()) {
            return countDeleted;
          }
        }
      }
      catch (SchemaOutOfSyncException e) {
        try {
          Thread.sleep(200);
        }
        catch (InterruptedException e1) {
          Thread.currentThread().interrupt();
        }
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }

  }

  public void serialize(DataOutputStream out) {
    try {
      out.writeUTF(tableName);
      ExpressionImpl.serializeExpression(expression, out);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public void deserialize(DataInputStream in) {
    try {
      tableName = in.readUTF();
      expression = ExpressionImpl.deserializeExpression(in);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private class DoDelete {
    private boolean myResult;
    private final String dbName;
    private final SelectStatementImpl.Explain explain;
    private final Long sequence0;
    private final Long sequence1;
    private final Short sequence2;
    private final boolean restrictToThisServer;
    private final StoredProcedureContextImpl procedureContext;
    private final int schemaRetryCount;
    private final TableSchema tableSchema;
    private final Random rand;
    private int countDeleted;

    DoDelete(String dbName, SelectStatementImpl.Explain explain, Long sequence0, Long sequence1,
             Short sequence2, boolean restrictToThisServer, StoredProcedureContextImpl procedureContext,
             int schemaRetryCount, TableSchema tableSchema, Random rand, int countDeleted) {
      this.dbName = dbName;
      this.explain = explain;
      this.sequence0 = sequence0;
      this.sequence1 = sequence1;
      this.sequence2 = sequence2;
      this.restrictToThisServer = restrictToThisServer;
      this.procedureContext = procedureContext;
      this.schemaRetryCount = schemaRetryCount;
      this.tableSchema = tableSchema;
      this.rand = rand;
      this.countDeleted = countDeleted;
    }

    boolean is() {
      return myResult;
    }

    int getCountDeleted() {
      return countDeleted;
    }

    public DoDelete invoke() {
      SelectStatementImpl select = new SelectStatementImpl(client);
      select.setExpression(expression);

      AtomicBoolean didTableScan = new AtomicBoolean();
      ExpressionImpl.NextReturn ids = expression.next(select, DatabaseClient.SELECT_PAGE_SIZE, explain, new AtomicLong(), new AtomicLong(),
          null, null, schemaRetryCount, didTableScan);
      if (ids == null || ids.getIds() == null) {
        myResult = true;
        return this;
      }

      IndexSchema indexSchema = null;
      for (Map.Entry<String, IndexSchema> entry : tableSchema.getIndices().entrySet()) {
        if (entry.getValue().isPrimaryKey()) {
          indexSchema = entry.getValue();
        }
      }
      if (indexSchema == null) {
        throw new DatabaseException("primary index not found");
      }

      String[] indexFields = indexSchema.getFields();
      int[] fieldOffsets = new int[indexFields.length];
      for (int k = 0; k < indexFields.length; k++) {
        fieldOffsets[k] = tableSchema.getFieldOffset(indexFields[k]);
      }

      for (Object[][] entry : ids.getKeys()) {
        ExpressionImpl.CachedRecord cachedRecord = recordCache.get(tableName, entry[0]);
        Record record = cachedRecord == null ? null : cachedRecord.getRecord();
        if (record == null) {
          boolean forceSelectOnServer = false;
          record = ExpressionImpl.doReadRecord(dbName, client, forceSelectOnServer, recordCache, entry[0], tableName,
              null, null, null, client.getCommon().getSchemaVersion(),
              restrictToThisServer, procedureContext, schemaRetryCount);
        }
        doDeleteRecordAndIndexEntry(indexSchema, entry, record);
      }
      myResult = false;
      return this;
    }

    private void doDeleteRecordAndIndexEntry(IndexSchema indexSchema, Object[][] entry, Record record) {
      if (record != null) {
        List<Integer> selectedShards = PartitionUtils.findOrderedPartitionForRecord(true,
            false, tableSchema,
            indexSchema, null, BinaryExpression.Operator.EQUAL, null,
            entry[0], null);
        if (selectedShards.isEmpty()) {
          throw new DatabaseException("No shards selected for query");
        }

        doDeleteRecord(indexSchema, DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), entry[0]), selectedShards);

        doDeleteIndexEntry(DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), entry[0]), record);

        countDeleted++;
      }
    }

    private void doDeleteIndexEntry(byte[] primaryKeyBytes, Record record) {
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.DB_NAME, dbName);
      if (schemaRetryCount < 2) {
        cobj.put(ComObject.Tag.SCHEMA_VERSION, client.getCommon().getSchemaVersion());
      }
      cobj.put(ComObject.Tag.PRIMARY_KEY_BYTES, primaryKeyBytes);
      cobj.put(ComObject.Tag.TABLE_NAME, tableName);
      cobj.put(ComObject.Tag.IS_EXCPLICITE_TRANS, client.isExplicitTrans());
      cobj.put(ComObject.Tag.IS_COMMITTING, client.isCommitting());
      cobj.put(ComObject.Tag.TRANSACTION_ID, client.getTransactionId());
      byte[] bytes = record.serialize(client.getCommon(), DatabaseClient.SERIALIZATION_VERSION);
      cobj.put(ComObject.Tag.RECORD_BYTES, bytes);
      if (sequence0 != null && sequence1 != null && sequence2 != null) {
        cobj.put(ComObject.Tag.SEQUENCE_0_OVERRIDE, sequence0);
        cobj.put(ComObject.Tag.SEQUENCE_1_OVERRIDE, sequence1);
        cobj.put(ComObject.Tag.SEQUENCE_2_OVERRIDE, sequence2);
      }

      client.sendToAllShards("UpdateManager:deleteIndexEntry", rand.nextLong(), cobj, DatabaseClient.Replica.DEF);
    }

    private void doDeleteRecord(IndexSchema indexSchema, byte[] bytes, List<Integer> selectedShards) {
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.SERIALIZATION_VERSION, DatabaseClient.SERIALIZATION_VERSION);
      cobj.put(ComObject.Tag.KEY_BYTES, bytes);
      if (schemaRetryCount < 2) {
        cobj.put(ComObject.Tag.SCHEMA_VERSION, client.getCommon().getSchemaVersion());
      }
      cobj.put(ComObject.Tag.DB_NAME, dbName);
      cobj.put(ComObject.Tag.TABLE_NAME, tableName);
      cobj.put(ComObject.Tag.INDEX_NAME, indexSchema.getName());
      cobj.put(ComObject.Tag.IS_EXCPLICITE_TRANS, client.isExplicitTrans());
      cobj.put(ComObject.Tag.IS_COMMITTING, client.isCommitting());
      cobj.put(ComObject.Tag.TRANSACTION_ID, client.getTransactionId());
      if (sequence0 != null && sequence1 != null && sequence2 != null) {
        cobj.put(ComObject.Tag.SEQUENCE_0_OVERRIDE, sequence0);
        cobj.put(ComObject.Tag.SEQUENCE_1_OVERRIDE, sequence1);
        cobj.put(ComObject.Tag.SEQUENCE_2_OVERRIDE, sequence2);
      }
      client.send("UpdateManager:deleteRecord", selectedShards.get(0), rand.nextLong(), cobj, DatabaseClient.Replica.DEF);
    }
  }
}
