package com.sonicbase.query.impl;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.ComObject;
import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.common.Record;
import com.sonicbase.common.SchemaOutOfSyncException;
import com.sonicbase.index.Repartitioner;
import com.sonicbase.query.BinaryExpression;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.query.DeleteStatement;
import com.sonicbase.query.Expression;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.server.SnapshotManager;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

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
  public Object execute(String dbName, SelectStatementImpl.Explain explain) throws DatabaseException {
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
          ExpressionImpl.NextReturn ids = expression.next(explain, new AtomicLong(), null, null);
          if (ids == null || ids.getIds() == null) {
            return countDeleted;
          }

          IndexSchema indexSchema = null;
          for (Map.Entry<String, IndexSchema> entry : tableSchema.getIndices().entrySet()) {
            if (entry.getValue().isPrimaryKey()) {
              indexSchema = entry.getValue();
            }
          }

          String[] indexFields = indexSchema.getFields();
          int[] fieldOffsets = new int[indexFields.length];
          for (int k = 0; k < indexFields.length; k++) {
            fieldOffsets[k] = tableSchema.getFieldOffset(indexFields[k]);
          }

          for (Object[][] entry : ids.getKeys()) {
            ExpressionImpl.CachedRecord cachedRecord = recordCache.get(tableName, entry[0]);;
            Record record = cachedRecord == null ? null : cachedRecord.getRecord();
            if (record == null) {
              boolean forceSelectOnServer = false;
              record = expression.doReadRecord(dbName, client, forceSelectOnServer, recordCache, entry[0], tableName, null, null, null, client.getCommon().getSchemaVersion(), false);
            }
            if (record != null) {
              List<Integer> selectedShards = Repartitioner.findOrderedPartitionForRecord(true, false, fieldOffsets, client.getCommon(), tableSchema,
                  indexSchema.getName(), null, BinaryExpression.Operator.equal, null, entry[0], null);
              if (selectedShards.size() == 0) {
                throw new Exception("No shards selected for query");
              }

              ComObject cobj = new ComObject();
              cobj.put(ComObject.Tag.serializationVersion, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
              cobj.put(ComObject.Tag.keyBytes, DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), entry[0]));
              cobj.put(ComObject.Tag.schemaVersion, client.getCommon().getSchemaVersion());
              cobj.put(ComObject.Tag.dbName, dbName);
              cobj.put(ComObject.Tag.tableName, tableName);
              cobj.put(ComObject.Tag.indexName, indexSchema.getName());
              cobj.put(ComObject.Tag.isExcpliciteTrans, client.isExplicitTrans());
              cobj.put(ComObject.Tag.isCommitting, client.isCommitting());
              cobj.put(ComObject.Tag.transactionId, client.getTransactionId());
              cobj.put(ComObject.Tag.method, "deleteRecord");
              String command = "DatabaseServer:ComObject:deleteRecord:";
              client.send("DatabaseServer:deleteRecord", selectedShards.get(0), rand.nextLong(), command, cobj, DatabaseClient.Replica.def);

              command = "DatabaseServer:ComObject:deleteIndexEntry:";
              cobj = new ComObject();
              cobj.put(ComObject.Tag.dbName, dbName);
              cobj.put(ComObject.Tag.schemaVersion, client.getCommon().getSchemaVersion());
              cobj.put(ComObject.Tag.tableName, tableName);
              cobj.put(ComObject.Tag.isExcpliciteTrans, client.isExplicitTrans());
              cobj.put(ComObject.Tag.isCommitting, client.isCommitting());
              cobj.put(ComObject.Tag.transactionId, client.getTransactionId());
              cobj.put(ComObject.Tag.method, "deleteIndexEntry");
              byte[] bytes = record.serialize(client.getCommon(), SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
              cobj.put(ComObject.Tag.recordBytes, bytes);

              client.sendToAllShards(null, rand.nextLong(), command, cobj, DatabaseClient.Replica.def);
              countDeleted++;
            }
          }
        }
      }
      catch (SchemaOutOfSyncException e) {
        try {
          Thread.sleep(200);
        }
        catch (InterruptedException e1) {
        }
        continue;
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }

  }

  public void serialize(DataOutputStream out) throws Exception {
    out.writeUTF(tableName);
    ExpressionImpl.serializeExpression(expression, out);
  }

  public void deserialize(DataInputStream in) throws Exception {
    tableName = in.readUTF();
    expression = ExpressionImpl.deserializeExpression(in);
  }
}
