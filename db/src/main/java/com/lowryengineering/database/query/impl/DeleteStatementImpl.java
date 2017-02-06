package com.lowryengineering.database.query.impl;

import com.lowryengineering.database.client.DatabaseClient;
import com.lowryengineering.database.common.DatabaseCommon;
import com.lowryengineering.database.common.Record;
import com.lowryengineering.database.common.SchemaOutOfSyncException;
import com.lowryengineering.database.index.Repartitioner;
import com.lowryengineering.database.query.BinaryExpression;
import com.lowryengineering.database.query.DatabaseException;
import com.lowryengineering.database.query.DeleteStatement;
import com.lowryengineering.database.query.Expression;
import com.lowryengineering.database.schema.IndexSchema;
import com.lowryengineering.database.schema.TableSchema;
import com.lowryengineering.database.server.SnapshotManager;
import com.lowryengineering.database.util.DataUtil;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

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
          ExpressionImpl.NextReturn ids = expression.next(explain);
          if (ids == null) {
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

              ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
              DataOutputStream out = new DataOutputStream(bytesOut);

              out.write(DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), entry[0]));

              out.close();

              String command = "DatabaseServer:deleteRecord:1:" + client.getCommon().getSchemaVersion() + ":" + dbName + ":" + tableName + ":" + indexSchema.getName();
              client.send("DatabaseServer:deleteRecord", selectedShards.get(0), rand.nextLong(), command, bytesOut.toByteArray(), DatabaseClient.Replica.def);

              command = "DatabaseServer:deleteIndexEntry:1:" + client.getCommon().getSchemaVersion() + ":" + dbName + ":" + tableSchema.getName();
              bytesOut = new ByteArrayOutputStream();
              out = new DataOutputStream(bytesOut);
              DataUtil.writeVLong(out, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
              byte[] bytes = record.serialize(client.getCommon());
              out.writeInt(bytes.length);
              out.write(bytes);
              out.close();

              client.sendToAllShards("DatabaseServer:deleteIndexEntry", rand.nextLong(), command, bytesOut.toByteArray(), DatabaseClient.Replica.def);
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
