package com.sonicbase.query.impl;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.*;
import com.sonicbase.index.Repartitioner;
import com.sonicbase.query.BinaryExpression;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.query.Expression;
import com.sonicbase.query.UpdateStatement;
import com.sonicbase.schema.FieldSchema;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.server.SnapshotManager;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

public class UpdateStatementImpl extends StatementImpl implements UpdateStatement {
  private final DatabaseClient client;
  private final ExpressionImpl.RecordCache recordCache;
  private String tableName;
  private List<ExpressionImpl> setExpressions = new ArrayList<>();
  private ExpressionImpl whereClause;
  private List<ColumnImpl> columns = new ArrayList<>();

  public UpdateStatementImpl(DatabaseClient client) {
    this.client = client;
    this.recordCache = new ExpressionImpl.RecordCache();
  }

  public List<ColumnImpl> getColumns() {
    return columns;
  }

  public ExpressionImpl getWhereClause() {
    return whereClause;
  }

  public void setWhereClause(Expression whereClause) {
    this.whereClause = (ExpressionImpl) whereClause;
  }

  @Override
  public Object execute(String dbName, SelectStatementImpl.Explain explain) throws DatabaseException {

    while (true) {
      try {
        whereClause.setViewVersion(client.getCommon().getSchemaVersion());
        whereClause.setTableName(tableName);
        whereClause.setClient(client);
        whereClause.setParms(getParms());
        whereClause.setTopLevelExpression(getWhereClause());
        whereClause.setRecordCache(recordCache);
        whereClause.setDbName(dbName);

        Integer replica = whereClause.getReplica();
        if (replica == null) {
          int replicaCount = client.getCommon().getServersConfig().getShards()[0].getReplicas().length;
          replica = ThreadLocalRandom.current().nextInt(0, replicaCount);
          whereClause.setReplica(replica);
        }

        Random rand = new Random(System.currentTimeMillis());
        int countUpdated = 0;
        getWhereClause().reset();
        while (true) {

          ExpressionImpl.NextReturn ret = getWhereClause().next(explain, new AtomicLong(), null, null);
          if (ret == null || ret.getIds() == null) {
            return countUpdated;
          }
          //todo: use tablescan update if necessary

          TableSchema tableSchema = client.getCommon().getTables(dbName).get(tableName);
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

          for (Object[][] entry : ret.getKeys()) {

            ExpressionImpl.CachedRecord cachedRecord = recordCache.get(tableName, entry[0]);
            Record record = cachedRecord == null ? null : cachedRecord.getRecord();
            if (record == null) {
              boolean forceSelectOnServer = false;
              record = whereClause.doReadRecord(dbName, client, forceSelectOnServer, recordCache, entry[0], tableName,
                  null, null, null, client.getCommon().getSchemaVersion(), false);
            }

            Object[] newPrimaryKey = new Object[entry.length];

            if (record != null) {
              Object[] fields = record.getFields();
              List<String> columnNames = new ArrayList<>();
              List<Object> values = new ArrayList<>();
              List<FieldSchema> tableFields = tableSchema.getFields();
              for (int i = 0; i < fields.length; i++) {
                Object fieldValue = fields[i];
                if (fieldValue != null) {
                  columnNames.add(tableFields.get(i).getName().toLowerCase());
                  values.add(fieldValue);
                }
              }

              long id = 0;
              if (tableFields.get(0).getName().equals("_id")) {
                id = (long)record.getFields()[0];
              }
              List<DatabaseClient.KeyInfo> previousKeys = client.getKeys(client.getCommon(), tableSchema, columnNames, values, id);

              List<ColumnImpl> qColumns = getColumns();
              List<ExpressionImpl> setExpressions = getSetExpressions();
              Object[] newFields = record.getFields();
              for (int i = 0; i < qColumns.size(); i++) {
                String columnName = qColumns.get(i).getColumnName();
                Object value = null;
                ExpressionImpl setExpression = setExpressions.get(i);
                if (setExpression instanceof ConstantImpl) {
                  ConstantImpl cNode1 = (ConstantImpl) setExpression;
                  value = cNode1.getValue();
                  if (value instanceof String) {
                    value = ((String) value).getBytes("utf-8");
                  }
                }
                else if (setExpression instanceof ParameterImpl) {
                  ParameterImpl pNode = (ParameterImpl) setExpression;
                  int parmNum = pNode.getParmOffset();
                  value = getParms().getValue(parmNum + 1);
                  if (value instanceof String) {
                    value = ((String) value).getBytes("utf-8");
                  }
                }
                int offset = tableSchema.getFieldOffset(columnName);
                FieldSchema fieldSchema = tableFields.get(offset);
                if (fieldSchema.getWidth() != 0) {
                  switch(fieldSchema.getType()) {
                    case VARCHAR:
                    case NVARCHAR:
                    case LONGVARCHAR:
                    case LONGNVARCHAR:
                    case CLOB:
                    case NCLOB:
                      String str = new String((byte[])value, "utf-8");
                      if (str.length() > fieldSchema.getWidth()) {
                        throw new SQLException("value too long: field=" + fieldSchema.getName() + ", width=" + fieldSchema.getWidth());
                      }
                      break;
                    case VARBINARY:
                    case LONGVARBINARY:
                    case BLOB:
                      if (((byte[])value).length > fieldSchema.getWidth()) {
                        throw new SQLException("value too long: field=" + fieldSchema.getName() + ", width=" + fieldSchema.getWidth());
                      }
                      break;
                  }
                }

                newFields[offset] = value;
              }
              columnNames = new ArrayList<>();
              values = new ArrayList<>();
              tableFields = tableSchema.getFields();
              for (int i = 0; i < newFields.length; i++) {
                Object fieldValue = newFields[i];
                if (fieldValue != null) {
                  columnNames.add(tableFields.get(i).getName());
                  values.add(fieldValue);
                }
              }

              for (int i = 0; i < newPrimaryKey.length; i++) {
                newPrimaryKey[i] = record.getFields()[fieldOffsets[i]];
              }

              //update record
              List<Integer> selectedShards = Repartitioner.findOrderedPartitionForRecord(true, false, fieldOffsets, client.getCommon(), tableSchema,
                  indexSchema.getName(), null, BinaryExpression.Operator.equal, null, newPrimaryKey, null);
              if (selectedShards.size() == 0) {
                throw new Exception("No shards selected for query");
              }

              String command = "DatabaseServer:ComObject:updateRecord:";

              ComObject cobj = new ComObject();
              cobj.put(ComObject.Tag.dbName, dbName);
              cobj.put(ComObject.Tag.schemaVersion, client.getCommon().getSchemaVersion());
              cobj.put(ComObject.Tag.method, "updateRecord");
              cobj.put(ComObject.Tag.tableName, tableName);
              cobj.put(ComObject.Tag.indexName, indexSchema.getName());
              cobj.put(ComObject.Tag.isExcpliciteTrans, client.isExplicitTrans());
              cobj.put(ComObject.Tag.isCommitting, client.isCommitting());
              cobj.put(ComObject.Tag.transactionId, client.getTransactionId());
              cobj.put(ComObject.Tag.primaryKeyBytes, DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), newPrimaryKey));
              cobj.put(ComObject.Tag.bytes, record.serialize(client.getCommon(), SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION));

              client.send(null, selectedShards.get(0), rand.nextLong(), command, cobj, DatabaseClient.Replica.def);

              //update keys

              List<DatabaseClient.KeyInfo> newKeys = client.getKeys(client.getCommon(), tableSchema, columnNames, values, id);

              Map<String, ConcurrentSkipListMap<Object[], DatabaseClient.KeyInfo>> orderedKeyInfosPrevious = new HashMap<>();
              Map<String, ConcurrentSkipListMap<Object[], DatabaseClient.KeyInfo>> orderedKeyInfosNew = new HashMap<>();

              client.populateOrderedKeyInfo(orderedKeyInfosPrevious, previousKeys);
              client.populateOrderedKeyInfo(orderedKeyInfosNew, newKeys);

              for (Map.Entry<String, ConcurrentSkipListMap<Object[], DatabaseClient.KeyInfo>> previousEntry : orderedKeyInfosPrevious.entrySet()) {
                ConcurrentSkipListMap<Object[], DatabaseClient.KeyInfo> newMap = orderedKeyInfosNew.get(previousEntry.getKey());
                if (newMap == null) {
                  for (Map.Entry<Object[], DatabaseClient.KeyInfo> prevEntry : previousEntry.getValue().entrySet()) {
                    client.deleteKey(dbName, tableSchema.getName(), prevEntry.getValue(), indexSchema.getName(), entry[0]);
                  }
                }
                else {
                  for (Map.Entry<Object[], DatabaseClient.KeyInfo> prevEntry : previousEntry.getValue().entrySet()) {
                    if (!newMap.containsKey(prevEntry.getKey())) {
                      client.deleteKey(dbName, tableSchema.getName(), prevEntry.getValue(), indexSchema.getName(), entry[0]);
                    }
                  }
                }
              }

              for (Map.Entry<String, ConcurrentSkipListMap<Object[], DatabaseClient.KeyInfo>> newEntry : orderedKeyInfosNew.entrySet()) {
                ConcurrentSkipListMap<Object[], DatabaseClient.KeyInfo> prevMap = orderedKeyInfosPrevious.get(newEntry.getKey());
                if (prevMap == null) {
                  for (Map.Entry<Object[], DatabaseClient.KeyInfo> innerNewEntry : newEntry.getValue().entrySet()) {
                    KeyRecord keyRecord = new KeyRecord();
                    keyRecord.setKey((long)newPrimaryKey[0]);
                    keyRecord.setDbViewNumber(client.getCommon().getSchemaVersion());
                    client.insertKey(dbName, tableSchema.getName(), innerNewEntry.getValue(), indexSchema.getName(),
                        newPrimaryKey, keyRecord, -1, -1);
                  }
                }
                else {
                  for (Map.Entry<Object[], DatabaseClient.KeyInfo> innerNewEntry : newEntry.getValue().entrySet()) {
                    if (!prevMap.containsKey(innerNewEntry.getKey())) {
                      if (innerNewEntry.getValue().getIndexSchema().getKey().equals(indexSchema.getName())) {
                        continue;
                      }
                      KeyRecord keyRecord = new KeyRecord();
                      keyRecord.setKey((long)newPrimaryKey[0]);
                      keyRecord.setDbViewNumber(client.getCommon().getSchemaVersion());
                      client.insertKey(dbName, tableSchema.getName(), innerNewEntry.getValue(), indexSchema.getName(),
                          newPrimaryKey, keyRecord, -1, -1);
                    }
                  }
                }
              }
            }
            countUpdated++;
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

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName.toLowerCase();
  }

  public void addSetExpression(Expression expression) {
    setExpressions.add((ExpressionImpl) expression);
  }

  public List<ExpressionImpl> getSetExpressions() {
    return setExpressions;
  }

  public void addColumn(net.sf.jsqlparser.schema.Column column) {
    ColumnImpl newColumn = new ColumnImpl();
    String tableName = column.getTable().getName();
    if (tableName != null) {
      tableName = tableName.toLowerCase();
    }
    newColumn.setTableName(tableName);
    newColumn.setColumnName(column.getColumnName().toLowerCase());
    columns.add(newColumn);
  }

  public int getCurrParmNum() {
    int currParmNum = 0;
    for (ExpressionImpl expression : setExpressions) {
      if (expression instanceof ParameterImpl) {
        currParmNum++;
      }
    }
    return currParmNum;
  }
}
