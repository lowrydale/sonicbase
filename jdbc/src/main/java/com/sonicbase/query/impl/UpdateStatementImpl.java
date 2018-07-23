package com.sonicbase.query.impl;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.client.InsertStatementHandler;
import com.sonicbase.common.*;
import com.sonicbase.procedure.StoredProcedureContextImpl;
import com.sonicbase.query.BinaryExpression;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.query.Expression;
import com.sonicbase.query.UpdateStatement;
import com.sonicbase.schema.FieldSchema;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.PartitionUtils;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import static com.sonicbase.client.DatabaseClient.SERIALIZATION_VERSION;

public class UpdateStatementImpl extends StatementImpl implements UpdateStatement {
  public static final String UTF_8_STR = "utf-8";
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
  public Object execute(String dbName, String sqlToUse, SelectStatementImpl.Explain explain, Long sequence0, Long sequence1, Short sequence2,
                        boolean restrictToThisServer, StoredProcedureContextImpl procedureContext, int schemaRetryCount) {
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

          ExpressionImpl.NextReturn ret = getWhereClause().next(explain, new AtomicLong(), new AtomicLong(), null, null, schemaRetryCount);
          if (ret == null || ret.getIds() == null) {
            return countUpdated;
          }

          TableSchema tableSchema = client.getCommon().getTables(dbName).get(tableName);
          IndexSchema indexSchema = null;
          for (Map.Entry<String, IndexSchema> entry : tableSchema.getIndices().entrySet()) {
            if (entry.getValue().isPrimaryKey()) {
              indexSchema = entry.getValue();
            }
          }
          if (indexSchema == null) {
            throw new DatabaseException("primary index not found: table=" + tableName);
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
              record = ExpressionImpl.doReadRecord(dbName, client, forceSelectOnServer, recordCache, entry[0], tableName,
                  null, null, null, client.getCommon().getSchemaVersion(), restrictToThisServer, procedureContext, schemaRetryCount);
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
              if (tableFields.get(0).getName().equals("_sonicbase_id")) {
                id = (long)record.getFields()[0];
              }
              List<InsertStatementHandler.KeyInfo> previousKeys = InsertStatementHandler.getKeys(client.getCommon(),
                  tableSchema, columnNames, values, id);

              List<ColumnImpl> qColumns = getColumns();
              List<ExpressionImpl> localSetExpressions = getSetExpressions();
              Object[] newFields = record.getFields();
              for (int i = 0; i < qColumns.size(); i++) {
                String columnName = qColumns.get(i).getColumnName();
                Object value = null;
                ExpressionImpl setExpression = localSetExpressions.get(i);
                if (setExpression instanceof ConstantImpl) {
                  ConstantImpl cNode1 = (ConstantImpl) setExpression;
                  value = cNode1.getValue();
                  if (value instanceof String) {
                    value = ((String) value).getBytes(UTF_8_STR);
                  }
                }
                else if (setExpression instanceof ParameterImpl) {
                  ParameterImpl pNode = (ParameterImpl) setExpression;
                  int parmNum = pNode.getParmOffset();
                  value = getParms().getValue(parmNum + 1);
                  if (value instanceof String) {
                    value = ((String) value).getBytes(UTF_8_STR);
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
                      String str = new String((byte[])value, UTF_8_STR);
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
              List<Integer> selectedShards = PartitionUtils.findOrderedPartitionForRecord(true, false, tableSchema,
                  indexSchema.getName(), null, BinaryExpression.Operator.EQUAL, null, newPrimaryKey, null);
              if (selectedShards.isEmpty()) {
                throw new DatabaseException("No shards selected for query");
              }

              ComObject cobj = new ComObject();
              cobj.put(ComObject.Tag.DB_NAME, dbName);
              if (schemaRetryCount < 2) {
                cobj.put(ComObject.Tag.SCHEMA_VERSION, client.getCommon().getSchemaVersion());
              }
              cobj.put(ComObject.Tag.TABLE_NAME, tableName);
              cobj.put(ComObject.Tag.INDEX_NAME, indexSchema.getName());
              cobj.put(ComObject.Tag.IS_EXCPLICITE_TRANS, client.isExplicitTrans());
              cobj.put(ComObject.Tag.IS_COMMITTING, client.isCommitting());
              cobj.put(ComObject.Tag.TRANSACTION_ID, client.getTransactionId());
              cobj.put(ComObject.Tag.PRIMARY_KEY_BYTES, DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), newPrimaryKey));
              cobj.put(ComObject.Tag.BYTES, record.serialize(client.getCommon(), SERIALIZATION_VERSION));
              if (sequence0 != null && sequence1 != null && sequence2 != null) {
                cobj.put(ComObject.Tag.SEQUENCE_0_OVERRIDE, sequence0);
                cobj.put(ComObject.Tag.SEQUENCE_1_OVERRIDE, sequence1);
                cobj.put(ComObject.Tag.SEQUENCE_2_OVERRIDE, sequence2);
              }

              client.send("UpdateManager:updateRecord", selectedShards.get(0), rand.nextLong(), cobj, DatabaseClient.Replica.DEF);

              //update keys

              List<InsertStatementHandler.KeyInfo> newKeys = InsertStatementHandler.getKeys(client.getCommon(), tableSchema, columnNames, values, id);

              Map<String, ConcurrentSkipListMap<Object[], InsertStatementHandler.KeyInfo>> orderedKeyInfosPrevious = new HashMap<>();
              Map<String, ConcurrentSkipListMap<Object[], InsertStatementHandler.KeyInfo>> orderedKeyInfosNew = new HashMap<>();

              DatabaseClient.populateOrderedKeyInfo(orderedKeyInfosPrevious, previousKeys);
              DatabaseClient.populateOrderedKeyInfo(orderedKeyInfosNew, newKeys);

              for (Map.Entry<String, ConcurrentSkipListMap<Object[], InsertStatementHandler.KeyInfo>> previousEntry : orderedKeyInfosPrevious.entrySet()) {
                ConcurrentSkipListMap<Object[], InsertStatementHandler.KeyInfo> newMap = orderedKeyInfosNew.get(previousEntry.getKey());
                if (newMap == null) {
                  for (Map.Entry<Object[], InsertStatementHandler.KeyInfo> prevEntry : previousEntry.getValue().entrySet()) {
                    deleteKey(dbName, tableSchema.getName(), prevEntry.getValue(), indexSchema.getName(), entry[0], schemaRetryCount);
                  }
                }
                else {
                  for (Map.Entry<Object[], InsertStatementHandler.KeyInfo> prevEntry : previousEntry.getValue().entrySet()) {
                    if (!newMap.containsKey(prevEntry.getKey())) {
                      deleteKey(dbName, tableSchema.getName(), prevEntry.getValue(), indexSchema.getName(), entry[0], schemaRetryCount);
                    }
                  }
                }
              }

              for (Map.Entry<String, ConcurrentSkipListMap<Object[], InsertStatementHandler.KeyInfo>> newEntry : orderedKeyInfosNew.entrySet()) {
                ConcurrentSkipListMap<Object[], InsertStatementHandler.KeyInfo> prevMap = orderedKeyInfosPrevious.get(newEntry.getKey());
                if (prevMap == null) {
                  for (Map.Entry<Object[], InsertStatementHandler.KeyInfo> innerNewEntry : newEntry.getValue().entrySet()) {
                    KeyRecord keyRecord = new KeyRecord();
                    byte[] primaryKeyBytes = DatabaseCommon.serializeKey(tableSchema, innerNewEntry.getValue().getIndexSchema().getName(), newPrimaryKey);
                    keyRecord.setPrimaryKey(primaryKeyBytes);
                    keyRecord.setDbViewNumber(client.getCommon().getSchemaVersion());
                    InsertStatementHandler.insertKey(client, dbName, tableSchema.getName(), innerNewEntry.getValue(), indexSchema.getName(),
                        newPrimaryKey, keyRecord, false, schemaRetryCount);
                  }
                }
                else {
                  for (Map.Entry<Object[], InsertStatementHandler.KeyInfo> innerNewEntry : newEntry.getValue().entrySet()) {
                    if (!prevMap.containsKey(innerNewEntry.getKey())) {
                      if (innerNewEntry.getValue().getIndexSchema().getName().equals(indexSchema.getName())) {
                        continue;
                      }
                      KeyRecord keyRecord = new KeyRecord();
                      byte[] primaryKeyBytes = DatabaseCommon.serializeKey(tableSchema,
                          indexSchema.getName(), newPrimaryKey);
                      keyRecord.setPrimaryKey(primaryKeyBytes);
                      keyRecord.setDbViewNumber(client.getCommon().getSchemaVersion());
                      InsertStatementHandler.insertKey(client, dbName, tableSchema.getName(), innerNewEntry.getValue(), indexSchema.getName(),
                          newPrimaryKey, keyRecord, false, schemaRetryCount);
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
          Thread.currentThread().interrupt();
          throw new DatabaseException(e1);
        }
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }

  }

  public void deleteKey(String dbName, String tableName, InsertStatementHandler.KeyInfo keyInfo, String primaryKeyIndexName,
                        Object[] primaryKey, int schemaRetryCount) {
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, dbName);
    if (schemaRetryCount < 2) {
      cobj.put(ComObject.Tag.SCHEMA_VERSION, client.getCommon().getSchemaVersion());
    }
    cobj.put(ComObject.Tag.METHOD, "UpdateManager:deleteIndexEntryByKey");
    cobj.put(ComObject.Tag.TABLE_NAME, tableName);
    cobj.put(ComObject.Tag.INDEX_NAME, keyInfo.getIndexSchema().getName());
    cobj.put(ComObject.Tag.PRIMARY_KEY_INDEX_NAME, primaryKeyIndexName);
    cobj.put(ComObject.Tag.IS_EXCPLICITE_TRANS, client.isExplicitTrans());
    cobj.put(ComObject.Tag.IS_COMMITTING, client.isCommitting());
    cobj.put(ComObject.Tag.TRANSACTION_ID, client.getTransactionId());

    cobj.put(ComObject.Tag.SERIALIZATION_VERSION, SERIALIZATION_VERSION);
    cobj.put(ComObject.Tag.KEY_BYTES, DatabaseCommon.serializeKey(client.getCommon().getTables(dbName).get(tableName), keyInfo.getIndexSchema().getName(), keyInfo.getKey()));
    cobj.put(ComObject.Tag.PRIMARY_KEY_BYTES, DatabaseCommon.serializeKey(client.getCommon().getTables(dbName).get(tableName), primaryKeyIndexName, primaryKey));

    client.send("UpdateManager:deleteIndexEntryByKey", keyInfo.getShard(), 0, cobj, DatabaseClient.Replica.DEF);
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
    String localTableName = column.getTable().getName();
    if (localTableName != null) {
      localTableName = localTableName.toLowerCase();
    }
    newColumn.setTableName(localTableName);
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
