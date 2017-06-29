package com.sonicbase.server;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.*;
import com.sonicbase.index.Index;
import com.sonicbase.index.Repartitioner;
import com.sonicbase.query.BinaryExpression;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.FieldSchema;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.DataUtil;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.sonicbase.server.TransactionManager.*;
import static com.sonicbase.server.TransactionManager.OperationType.*;

/**
 * Responsible for
 */
public class UpdateManager {

  private Logger logger;


  private static final String CURR_VER_STR = "currVer:";
  private final DatabaseServer server;

  public UpdateManager(DatabaseServer databaseServer) {
    this.server = databaseServer;
    this.logger = new Logger(databaseServer.getDatabaseClient());
  }

  public byte[] deleteIndexEntry(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    String tableName = cobj.getString(ComObject.Tag.tableName);
    int schemaVersion = cobj.getInt(ComObject.Tag.schemaVersion);
    if (!replayedCommand && schemaVersion < server.getSchemaVersion()) {
      throw new SchemaOutOfSyncException(CURR_VER_STR + server.getCommon().getSchemaVersion() + ":");
    }

    TableSchema tableSchema = server.getCommon().getSchema(dbName).getTables().get(tableName);
    Record record = new Record(tableSchema);
    byte[] recordBytes = cobj.getByteArray(ComObject.Tag.recordBytes);
    long serializationVersion = cobj.getLong(ComObject.Tag.serializationVersion);
    record.deserialize(dbName, server.getCommon(), recordBytes, null);
    List<FieldSchema> fieldSchemas = tableSchema.getFields();

    for (Map.Entry<String, IndexSchema> indexSchema : tableSchema.getIndices().entrySet()) {
      String[] fields = indexSchema.getValue().getFields();
      boolean shouldIndex = true;
      for (int i = 0; i < fields.length; i++) {
        boolean found = false;
        for (int j = 0; j < fieldSchemas.size(); j++) {
          if (fields[i].equals(fieldSchemas.get(j).getName())) {
            if (record.getFields()[j] != null) {
              found = true;
              break;
            }
          }
        }
        if (!found) {
          shouldIndex = false;
          break;
        }
      }
      if (shouldIndex) {
        String[] indexFields = indexSchema.getValue().getFields();
        Object[] key = new Object[indexFields.length];
        for (int i = 0; i < key.length; i++) {
          for (int j = 0; j < fieldSchemas.size(); j++) {
            if (fieldSchemas.get(j).getName().equals(indexFields[i])) {
              key[i] = record.getFields()[j];
            }
          }
        }
        Index index = server.getIndices(dbName).getIndices().get(tableSchema.getName()).get(indexSchema.getKey());
        //synchronized (index.getMutex(key)) {
        Object obj = index.remove(key);
        if (obj == null) {
          continue;
        }
        server.freeUnsafeIds(obj);
        //}
      }
    }

    return null;
  }

  public byte[] populateIndex(ComObject cobj) {
    String command = "DatabaseServer:ComObject:doPopulateIndex:";
    cobj.put(ComObject.Tag.method, "doPopulateIndex");
    server.getLongRunningCommands().addCommand(server.getLongRunningCommands().createSingleCommand(command, cobj.serialize()));
    return null;
  }

  public byte[] doPopulateIndex(ComObject cobj) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    String tableName = cobj.getString(ComObject.Tag.tableName);
    String indexName = cobj.getString(ComObject.Tag.indexName);

    TableSchema tableSchema = server.getCommon().getTables(dbName).get(tableName);
    String primaryKeyIndexName = null;
    for (Map.Entry<String, IndexSchema> entry : tableSchema.getIndices().entrySet()) {
      if (entry.getValue().isPrimaryKey()) {
        primaryKeyIndexName = entry.getKey();
      }
    }

    Index primaryKeyIndex = server.getIndices().get(dbName).getIndices().get(tableName).get(primaryKeyIndexName);
    Map.Entry<Object[], Object> entry = primaryKeyIndex.firstEntry();
    while (entry != null) {
    //  server.getCommon().getSchemaReadLock(dbName).lock();
      try {
        synchronized (primaryKeyIndex.getMutex(entry.getKey())) {
          Object value = primaryKeyIndex.get(entry.getKey());
          byte[][] records = server.fromUnsafeToRecords(value);
          for (int i = 0; i < records.length; i++) {
            Record record = new Record(dbName, server.getCommon(), records[i]);
            Object[] fields = record.getFields();
            List<String> columnNames = new ArrayList<>();
            List<Object> values = new ArrayList<>();
            for (int j = 0; j < fields.length; j++) {
              values.add(fields[j]);
              columnNames.add(tableSchema.getFields().get(j).getName());
            }

            DatabaseClient.KeyInfo primaryKey = new DatabaseClient.KeyInfo();
            tableSchema = server.getCommon().getTables(dbName).get(tableName);

            long id = 0;
            if (tableSchema.getFields().get(0).getName().equals("_id")) {
              id = (long) record.getFields()[0];
            }
            List<DatabaseClient.KeyInfo> keys = server.getDatabaseClient().getKeys(tableSchema, columnNames, values, id);

            for (final DatabaseClient.KeyInfo keyInfo : keys) {
              if (keyInfo.getIndexSchema().getValue().isPrimaryKey()) {
                primaryKey.setKey(keyInfo.getKey());
                primaryKey.setIndexSchema(keyInfo.getIndexSchema());
                break;
              }
            }
            for (final DatabaseClient.KeyInfo keyInfo : keys) {
              if (keyInfo.getIndexSchema().getKey().equals(indexName)) {
                while (true) {
                  try {
                    server.getDatabaseClient().insertKey(dbName, tableName, keyInfo, primaryKeyIndexName, primaryKey.getKey());
                    break;
                  }
                  catch (SchemaOutOfSyncException e) {
                    continue;
                  }
                }
              }
            }
          }
          entry = primaryKeyIndex.higherEntry(entry.getKey());
        }
      }
      finally {
      //  server.getCommon().getSchemaReadLock(dbName).unlock();
      }

    }
    return null;
  }

  public byte[] deleteIndexEntryByKey(ComObject cobj, boolean replayedCommand) {
    AtomicBoolean isExplicitTrans = new AtomicBoolean();
    AtomicLong transactionId = new AtomicLong();
    byte[] ret = doDeleteIndexEntryByKey(cobj, replayedCommand, isExplicitTrans, transactionId, false);
    if (isExplicitTrans.get()) {
      Transaction trans = server.getTransactionManager().getTransaction(transactionId.get());
      String command = "DatabaseServer:ComObject:deleteIndexEntryByKey:";
      trans.addOperation(delete, command, cobj.serialize(), replayedCommand);
    }
    return ret;
  }

  public byte[] doDeleteIndexEntryByKey(ComObject cobj, boolean replayedCommand,
                                        AtomicBoolean isExplicitTransRet, AtomicLong transactionIdRet, boolean isCommitting) {
    try {
      String dbName = cobj.getString(ComObject.Tag.dbName);
      int schemaVersion = cobj.getInt(ComObject.Tag.schemaVersion);
      if (!replayedCommand && schemaVersion < server.getSchemaVersion()) {
        throw new SchemaOutOfSyncException(CURR_VER_STR + server.getCommon().getSchemaVersion() + ":");
      }
      String tableName = cobj.getString(ComObject.Tag.tableName);
      String indexName = cobj.getString(ComObject.Tag.indexName);
      String primaryKeyIndexName = cobj.getString(ComObject.Tag.primaryKeyIndexName);
      boolean isExplicitTrans = cobj.getBoolean(ComObject.Tag.isExcpliciteTrans);
      //boolean isCommitting = Boolean.valueOf(parts[9]);
      long transactionId = cobj.getLong(ComObject.Tag.transactionId);
      if (isExplicitTrans && isExplicitTransRet != null) {
        isExplicitTransRet.set(true);
        transactionIdRet.set(transactionId);
      }

      TableSchema tableSchema = server.getCommon().getTables(dbName).get(tableName);
      long serializationVersion = cobj.getLong(ComObject.Tag.serializationVersion);
      byte[] keyBytes = cobj.getByteArray(ComObject.Tag.keyBytes);
      byte[] primaryKeyBytes = cobj.getByteArray(ComObject.Tag.primaryKeyBytes);
      Object[] key = DatabaseCommon.deserializeKey(tableSchema, keyBytes);
      Object[] primaryKey = DatabaseCommon.deserializeKey(tableSchema, primaryKeyBytes);

      AtomicBoolean shouldExecute = new AtomicBoolean();
      AtomicBoolean shouldDeleteLock = new AtomicBoolean();

      server.getTransactionManager().preHandleTransaction(dbName, tableName, indexName, isExplicitTrans, isCommitting, transactionId, primaryKey, shouldExecute, shouldDeleteLock);

      if (shouldExecute.get()) {
        doRemoveIndexEntryByKey(dbName, tableSchema, primaryKeyIndexName, primaryKey, indexName, key);
      }

      if (shouldDeleteLock.get()) {
        server.getTransactionManager().deleteLock(dbName, tableName, indexName, transactionId, tableSchema, primaryKey);
      }
      return null;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public byte[] batchInsertIndexEntryByKey(ComObject cobj, boolean replayedCommand) {
    AtomicBoolean isExplicitTrans = new AtomicBoolean();
    AtomicLong transactionId = new AtomicLong();
    int count = 0;
    try {
      ComArray array = cobj.getArray(ComObject.Tag.insertObjects);
      for (int i = 0; i < array.getArray().size(); i++) {
        ComObject innerObj = (ComObject) array.getArray().get(i);
        doInsertIndexEntryByKey(innerObj, replayedCommand, isExplicitTrans, transactionId, false);
        count++;
      }
      if (isExplicitTrans.get()) {
        Transaction trans = server.getTransactionManager().getTransaction(transactionId.get());
        String command = "DatabaseServer:ComObject:batchInsertIndexEntryByKey:";
        trans.addOperation(batchInsert, command, cobj.serialize(), replayedCommand);
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }

    ComObject retObj = new ComObject();
    retObj.put(ComObject.Tag.count, count);
    return retObj.serialize();
  }

  public byte[] insertIndexEntryByKey(ComObject cobj, boolean replayedCommand) {
    AtomicBoolean isExplicitTrans = new AtomicBoolean();
    AtomicLong transactionId = new AtomicLong();
    try {
      byte[] ret = doInsertIndexEntryByKey(cobj, replayedCommand, isExplicitTrans, transactionId, false);
      if (isExplicitTrans.get()) {
        Transaction trans = server.getTransactionManager().getTransaction(transactionId.get());
        String command = "DatabaseServer:ComObject:insertIndexEntryByKey:";
        trans.addOperation(insert, command, cobj.serialize(), replayedCommand);
      }
      return ret;
    }
    catch (EOFException e) {
      throw new DatabaseException(e);
    }
  }

  public byte[] doInsertIndexEntryByKey(ComObject cobj, boolean replayedCommand,
                                        AtomicBoolean isExplicitTransRet, AtomicLong transactionIdRet,
                                        boolean isCommitting) throws EOFException {
    try {
      if (server.getAboveMemoryThreshold().get()) {
        throw new DatabaseException("Above max memory threshold. Further inserts are not allowed");
      }

      String dbName = cobj.getString(ComObject.Tag.dbName);
      int schemaVersion = cobj.getInt(ComObject.Tag.schemaVersion);
      if (!replayedCommand && schemaVersion < server.getSchemaVersion()) {
        throw new SchemaOutOfSyncException(CURR_VER_STR + server.getCommon().getSchemaVersion() + ":");
      }

      long serializationVersion = cobj.getLong(ComObject.Tag.serializationVersion);

      String tableName = cobj.getString(ComObject.Tag.tableName);
      String indexName = cobj.getString(ComObject.Tag.indexName);
      boolean isExplicitTrans = cobj.getBoolean(ComObject.Tag.isExcpliciteTrans);

      long transactionId = cobj.getLong(ComObject.Tag.transactionId);
      if (isExplicitTrans && isExplicitTransRet != null) {
        isExplicitTransRet.set(true);
        transactionIdRet.set(transactionId);
      }

      TableSchema tableSchema = server.getCommon().getTables(dbName).get(tableName);
      Object[] key = DatabaseCommon.deserializeKey(tableSchema, cobj.getByteArray(ComObject.Tag.keyBytes));
      byte[] primaryKeyBytes = cobj.getByteArray(ComObject.Tag.primaryKeyBytes);
      IndexSchema indexSchema = tableSchema.getIndexes().get(indexName);


      Index index = server.getIndices(dbName).getIndices().get(tableSchema.getName()).get(indexName);

      Object[] primaryKey = DatabaseCommon.deserializeKey(tableSchema, primaryKeyBytes);

      AtomicBoolean shouldExecute = new AtomicBoolean();
      AtomicBoolean shouldDeleteLock = new AtomicBoolean();

      server.getTransactionManager().preHandleTransaction(dbName, tableName, indexName, isExplicitTrans, isCommitting, transactionId, primaryKey, shouldExecute, shouldDeleteLock);

      if (shouldExecute.get()) {
        doInsertKey(key, primaryKeyBytes, tableName, index, indexSchema);
      }

      //    else {
      //      if (transactionId != 0) {
      //        Transaction trans = transactions.get(transactionId);
      //        Map<String, Index> indices = trans.indices.get(tableName);
      //        if (indices == null) {
      //          indices = new ConcurrentHashMap<>();
      //          trans.indices.put(tableName, indices);
      //        }
      //        index = indices.get(indexName);
      //        if (index == null) {
      //          Comparator[] comparators = tableSchema.getIndices().get(indexName).getComparators();
      //          index = new Index(tableSchema, indexName, comparators);
      //          indices.put(indexName, index);
      //        }
      //        long unsafe = toUnsafeFromKeys(new byte[][]{primaryKeyBytes});
      //        index.put(key, unsafe);
      //      }
      //    }


      if (shouldDeleteLock.get()) {
        server.getTransactionManager().deleteLock(dbName, tableName, indexName, transactionId, tableSchema, primaryKey);
      }

      if (!replayedCommand && schemaVersion < server.getSchemaVersion()) {
        throw new SchemaOutOfSyncException(CURR_VER_STR + server.getCommon().getSchemaVersion() + ":");
      }

      return null;
    }
    catch (EOFException e) {
      throw e;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  private AtomicLong insertCount = new AtomicLong();

  public byte[] batchInsertIndexEntryByKeyWithRecord(ComObject cobj, boolean replayedCommand) {
    int schemaVersion = cobj.getInt(ComObject.Tag.schemaVersion);
    if (!replayedCommand && schemaVersion < server.getSchemaVersion()) {
      throw new SchemaOutOfSyncException(CURR_VER_STR + server.getCommon().getSchemaVersion() + ":");
    }

    int count = 0;
    AtomicLong transactionId = new AtomicLong();
    AtomicBoolean isExplicitTrans = new AtomicBoolean();
    try {
      ComArray array = cobj.getArray(ComObject.Tag.insertObjects);
      for (int i = 0; i < array.getArray().size(); i++) {
         synchronized (server.getBatchRepartCount()) {
//          while (server.getBatchRepartCount().get() != 0) {
//            Thread.sleep(10);
//          }
          ComObject innerObj = (ComObject)array.getArray().get(i);
          doInsertIndexEntryByKeyWithRecord(innerObj, replayedCommand, transactionId, isExplicitTrans, false);
          count++;
          //if (insertCount.incrementAndGet() % 5000 == 0) {
          while (server.isThrottleInsert()) {
            Thread.sleep(100);
          }
          //}
        }
      }
      if (isExplicitTrans.get()) {
        Transaction trans = server.getTransactionManager().getTransaction(transactionId.get());
        String command = "DatabaseServer:ComObject:batchInsertIndexEntryByKeyWithRecord:";
        trans.addOperation(batchInsertWithRecord, command, cobj.serialize(), replayedCommand);
      }
    }
    catch (InterruptedException e) {
      //ignore
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
    ComObject retObj = new ComObject();
    retObj.put(ComObject.Tag.count, count);
    return retObj.serialize();
  }

  public byte[] insertIndexEntryByKeyWithRecord(ComObject cobj, boolean replayedCommand) {
    try {
      AtomicBoolean isExplicitTrans = new AtomicBoolean();
      AtomicLong transactionId = new AtomicLong();
      byte[] ret = doInsertIndexEntryByKeyWithRecord(cobj, replayedCommand, transactionId, isExplicitTrans, false);
      if (isExplicitTrans.get()) {
        Transaction trans = server.getTransactionManager().getTransaction(transactionId.get());
        String command = "DatabaseServer:ComObject:insertIndexEntryByKeyWithRecord:";
        trans.addOperation(insertWithRecord, command, cobj.serialize(), replayedCommand);
      }
      //if (insertCount.incrementAndGet() % 5000 == 0) {
      if (server.isThrottleInsert()) {
        Thread.sleep(1);
      }
      //}
      return ret;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public byte[] doInsertIndexEntryByKeyWithRecord(ComObject cobj,
                                                  boolean replayedCommand, AtomicLong transactionIdRet,
                                                  AtomicBoolean isExpliciteTransRet, boolean isCommitting) throws EOFException {
    try {
      if (server.getAboveMemoryThreshold().get()) {
        throw new DatabaseException("Above max memory threshold. Further inserts are not allowed");
      }

      String dbName = cobj.getString(ComObject.Tag.dbName);
      int schemaVersion = cobj.getInt(ComObject.Tag.schemaVersion);
      if (!replayedCommand && schemaVersion < server.getSchemaVersion()) {
        throw new SchemaOutOfSyncException(CURR_VER_STR + server.getCommon().getSchemaVersion() + ":");
      }

      String tableName = cobj.getString(ComObject.Tag.tableName);
      String indexName = cobj.getString(ComObject.Tag.indexName);
      long id = cobj.getLong(ComObject.Tag.id);
      boolean isExplicitTrans = cobj.getBoolean(ComObject.Tag.isExcpliciteTrans);
      long transactionId = cobj.getLong(ComObject.Tag.transactionId);
      if (isExplicitTrans && isExpliciteTransRet != null) {
        isExpliciteTransRet.set(true);
        transactionIdRet.set(transactionId);
      }

      TableSchema tableSchema = server.getCommon().getTables(dbName).get(tableName);
      byte[] recordBytes = cobj.getByteArray(ComObject.Tag.recordBytes);
      byte[] keyBytes = cobj.getByteArray(ComObject.Tag.keyBytes);
      Object[] primaryKey = DatabaseCommon.deserializeKey(tableSchema, keyBytes);

      AtomicBoolean shouldExecute = new AtomicBoolean();
      AtomicBoolean shouldDeleteLock = new AtomicBoolean();
      server.getTransactionManager().preHandleTransaction(dbName, tableName, indexName, isExplicitTrans, isCommitting, transactionId, primaryKey, shouldExecute, shouldDeleteLock);

      List<Integer> selectedShards = null;
      IndexSchema indexSchema = server.getCommon().getTables(dbName).get(tableName).getIndexes().get(indexName);
      Index index = server.getIndices(dbName).getIndices().get(tableSchema.getName()).get(indexName);
      boolean alreadyExisted = false;
      if (shouldExecute.get()) {

        String[] indexFields = indexSchema.getFields();
        int[] fieldOffsets = new int[indexFields.length];
        for (int i = 0; i < indexFields.length; i++) {
          fieldOffsets[i] = tableSchema.getFieldOffset(indexFields[i]);
        }
        selectedShards = Repartitioner.findOrderedPartitionForRecord(true, false, fieldOffsets, server.getCommon(), tableSchema,
            indexName, null, BinaryExpression.Operator.equal, null, primaryKey, null);

//        if (null != index.get(primaryKey)) {
//          alreadyExisted = true;
//        }
        doInsertKey(id, recordBytes, primaryKey, index, tableSchema.getName(), indexName);

        int selectedShard = selectedShards.get(0);
        if (indexSchema.getCurrPartitions()[selectedShard].getShardOwning() != server.getShard()) {
          server.getRepartitioner().deleteIndexEntry(tableName, indexName, primaryKey);
        }
      }
      else {
        if (transactionId != 0) {
          if (transactionId != 0) {
            Transaction trans = server.getTransactionManager().getTransaction(transactionId);
            List<Record> records = trans.getRecords().get(tableName);
            if (records == null) {
              records = new ArrayList<>();
              trans.getRecords().put(tableName, records);
            }
            Record record = new Record(dbName, server.getCommon(), recordBytes);
            records.add(record);
          }
        }

        //throw new DatabaseException("in trans");
//        if (transactionId != 0) {
//          TransactionManager.Transaction trans = server.getTransactionManager().getTransaction(transactionId);
//          List<Record> records = trans.getRecords().get(tableName);
//          if (records == null) {
//            records = new ArrayList<>();
//            trans.getRecords().put(tableName, records);
//          }
//          Record record = new Record(server.getCommon(), recordBytes);
//          records.add(record);
//        }
      }

      if (shouldDeleteLock.get()) {
        server.getTransactionManager().deleteLock(dbName, tableName, indexName, transactionId, tableSchema, primaryKey);
      }

      if (!replayedCommand && schemaVersion < server.getSchemaVersion()) {

        if (selectedShards != null) {
//          if (!alreadyExisted) {
//            synchronized (index) {
//              Long existingValue = index.remove(primaryKey);
//              if (existingValue != null) {
//                server.freeUnsafeIds(existingValue);
//              }
//            }
//          }

          if (indexSchema.getCurrPartitions()[selectedShards.get(0)].getShardOwning() != server.getShard()) {
            if (server.getRepartitioner().undeleteIndexEntry(dbName, tableName, indexName, primaryKey, recordBytes)) {
              doInsertKey(id, recordBytes, primaryKey, index, tableSchema.getName(), indexName);
            }
          }
        }
        throw new SchemaOutOfSyncException(CURR_VER_STR + server.getCommon().getSchemaVersion() + ":");
      }

      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.count, 1);
      return retObj.serialize();
    }
    catch (EOFException e) {
      throw e;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public byte[] rollback(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    int schemaVersion = cobj.getInt(ComObject.Tag.schemaVersion);
    if (!replayedCommand && schemaVersion < server.getSchemaVersion()) {
      throw new SchemaOutOfSyncException(CURR_VER_STR + server.getCommon().getSchemaVersion() + ":");
    }
    long transactionId = cobj.getLong(ComObject.Tag.transactionId);

    Transaction trans = server.getTransactionManager().getTransaction(transactionId);
    ConcurrentHashMap<String, ConcurrentSkipListMap<Object[], RecordLock>> tableLocks = server.getTransactionManager().getLocks(dbName);
    if (trans != null) {
      List<RecordLock> locks = trans.getLocks();
      for (RecordLock lock : locks) {
        String tableName = lock.getTableName();
        tableLocks.get(tableName).remove(lock.getPrimaryKey());
      }
      server.getTransactionManager().getTransactions().remove(transactionId);
    }
    return null;
  }

  public byte[] commit(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    int schemaVersion = cobj.getInt(ComObject.Tag.schemaVersion);
    if (!replayedCommand && schemaVersion < server.getSchemaVersion()) {
      throw new SchemaOutOfSyncException(CURR_VER_STR + server.getCommon().getSchemaVersion() + ":");
    }
    long transactionId = cobj.getLong(ComObject.Tag.transactionId);

    Transaction trans = server.getTransactionManager().getTransaction(transactionId);
    if (trans != null) {
      List<TransactionManager.Operation> ops = trans.getOperations();
      for (Operation op : ops) {
        byte[] opBody = op.getBody();
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(opBody));
        try {
          switch (op.getType()) {
            case insert:
              doInsertIndexEntryByKey(new ComObject(opBody), op.getReplayed(), null, null, true);
              break;
            case batchInsert:
              cobj = new ComObject(opBody);
              ComArray array = cobj.getArray(ComObject.Tag.insertObjects);
              for (int i = 0; i < array.getArray().size(); i++) {
                ComObject innerObj = (ComObject)array.getArray().get(i);
                doInsertIndexEntryByKey(innerObj, replayedCommand, null, null, true);
              }
              break;
            case insertWithRecord:
              doInsertIndexEntryByKeyWithRecord(new ComObject(opBody), op.getReplayed(), null, null, true);
              break;
            case batchInsertWithRecord:
              cobj = new ComObject(opBody);
              array = cobj.getArray(ComObject.Tag.insertObjects);
              for (int i = 0; i < array.getArray().size(); i++) {
                ComObject innerObj = (ComObject) array.getArray().get(i);
                doInsertIndexEntryByKeyWithRecord(innerObj, op.getReplayed(), null, null, true);
              }
              break;
            case update:
              doUpdateRecord(new ComObject(op.getBody()), op.getReplayed(), null, null, true);
              break;
            case delete:
              doDeleteIndexEntryByKey(new ComObject(op.getBody()), op.getReplayed(), null, null, true);
              break;
          }
        }
        catch (EOFException e) {
          //expected
        }
        catch (Exception e) {
          throw new DatabaseException(e);
        }
      }
      server.getTransactionManager().getTransactions().remove(transactionId);
    }
    return null;
  }

  public byte[] updateRecord(ComObject cobj, boolean replayedCommand) {

    AtomicBoolean isExplicitTrans = new AtomicBoolean();
    AtomicLong transactionId = new AtomicLong();
    byte[] ret = doUpdateRecord(cobj, replayedCommand, isExplicitTrans, transactionId, false);
    if (isExplicitTrans.get()) {
      Transaction trans = server.getTransactionManager().getTransaction(transactionId.get());
      String command = "DatabaseServer:ComObject:upateRecord:";
      trans.addOperation(update, command, cobj.serialize(), replayedCommand);
    }
    return ret;
  }

  public byte[] doUpdateRecord(ComObject cobj, boolean replayedCommand,
                               AtomicBoolean isExplicitTransRet, AtomicLong transactionIdRet, boolean isCommitting) {
    try {
      String dbName = cobj.getString(ComObject.Tag.dbName);
      int schemaVersion = cobj.getInt(ComObject.Tag.schemaVersion);
      if (!replayedCommand && schemaVersion < server.getSchemaVersion()) {
        throw new SchemaOutOfSyncException(CURR_VER_STR + server.getCommon().getSchemaVersion() + ":");
      }
      String tableName = cobj.getString(ComObject.Tag.tableName);
      String indexName = cobj.getString(ComObject.Tag.indexName);
      boolean isExplicitTrans = cobj.getBoolean(ComObject.Tag.isExcpliciteTrans);
      //boolean isCommitting = Boolean.valueOf(parts[9]);
      long transactionId = cobj.getLong(ComObject.Tag.transactionId);

      if (isExplicitTrans && isExplicitTransRet != null) {
        isExplicitTransRet.set(true);
        transactionIdRet.set(transactionId);
      }

      TableSchema tableSchema = server.getCommon().getTables(dbName).get(tableName);
      byte[] primaryKeyBytes = cobj.getByteArray(ComObject.Tag.primaryKeyBytes);
      Object[] primaryKey = DatabaseCommon.deserializeKey(tableSchema, primaryKeyBytes);
      byte[] bytes = cobj.getByteArray(ComObject.Tag.bytes);

      AtomicBoolean shouldExecute = new AtomicBoolean();
      AtomicBoolean shouldDeleteLock = new AtomicBoolean();

      server.getTransactionManager().preHandleTransaction(dbName, tableName, indexName, isExplicitTrans, isCommitting, transactionId, primaryKey, shouldExecute, shouldDeleteLock);

      if (shouldExecute.get()) {
        //because this is the primary key index we won't have more than one index entry for the key
        Index index = server.getIndices(dbName).getIndices().get(tableName).get(indexName);
        Object newValue = server.toUnsafeFromRecords(new byte[][]{bytes});
        synchronized (index.getMutex(primaryKey)) {
          Object value = index.get(primaryKey);
          index.put(primaryKey, newValue);
          if (value != null) {
            server.freeUnsafeIds(value);
          }
        }
      }
      else {
        if (transactionId != 0) {
          if (transactionId != 0) {
            Transaction trans = server.getTransactionManager().getTransaction(transactionId);
            List<Record> records = trans.getRecords().get(tableName);
            if (records == null) {
              records = new ArrayList<>();
              trans.getRecords().put(tableName, records);
            }
            Record record = new Record(dbName, server.getCommon(), bytes);
            records.add(record);
          }
        }
      }

      if (shouldDeleteLock.get()) {
        server.getTransactionManager().deleteLock(dbName, tableName, indexName, transactionId, tableSchema, primaryKey);
      }
      return null;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  private void doInsertKey(
      long id, byte[] recordBytes, Object[] key, Index index, String tableName, String indexName) throws IOException, DatabaseException {
    doActualInsertKeyWithRecord(recordBytes, key, index, tableName, indexName, false);
  }

  private void doInsertKey(Object[] key, byte[] primaryKeyBytes, String tableName, Index index, IndexSchema indexSchema) {
    //    ArrayBlockingQueue<Entry> existing = insertQueue.computeIfAbsent(index, k -> new ArrayBlockingQueue<>(1000));
    //    insertThreads.computeIfAbsent(index, k -> createThread(index));

    doActualInsertKey(key, primaryKeyBytes, tableName, index, indexSchema);

    //    Entry currEntry = new Entry(id, key);
    //    existing.put(currEntry);
    //    currEntry.latch.await();
  }

  public void doInsertKeys(
      List<Repartitioner.MoveRequest> moveRequests, Index index, String tableName, IndexSchema indexSchema) {
    //    ArrayBlockingQueue<Entry> existing = insertQueue.computeIfAbsent(index, k -> new ArrayBlockingQueue<>(1000));
    //    insertThreads.computeIfAbsent(index, k -> createThread(index));

    if (indexSchema.isPrimaryKey()) {
      for (Repartitioner.MoveRequest moveRequest : moveRequests) {
        byte[][] content = moveRequest.getContent();
        for (int i = 0; i < content.length; i++) {
          doActualInsertKeyWithRecord(content[i], moveRequest.getKey(), index, tableName, indexSchema.getName(), true);
        }
      }
    }
    else {
      for (Repartitioner.MoveRequest moveRequest : moveRequests) {
        byte[][] content = moveRequest.getContent();
        for (int i = 0; i < content.length; i++) {
          doActualInsertKey(moveRequest.getKey(), content[i], tableName, index, indexSchema);
        }
      }
    }

    //    Entry currEntry = new Entry(id, key);
    //    existing.put(currEntry);
    //    currEntry.latch.await();
  }

  /**
   * Caller must synchronized index
   */
  private void doActualInsertKey(Object[] key, byte[] primaryKeyBytes, String tableName, Index index, IndexSchema indexSchema) {
    int fieldCount = index.getComparators().length;
    if (fieldCount != key.length) {
      Object[] newKey = new Object[fieldCount];
      for (int i = 0; i < newKey.length; i++) {
        newKey[i] = key[i];
      }
      key = newKey;
    }
    Object existingValue = null;
    synchronized (index.getMutex(key)) {
      existingValue = index.get(key);
      if (existingValue != null) {
        byte[][] records = server.fromUnsafeToRecords(existingValue);
        boolean replaced = false;
        for (int i = 0; i < records.length; i++) {
          if (Arrays.equals(records[i], primaryKeyBytes)) {
            replaced = true;
            break;
          }
        }

        if (indexSchema.isUnique()) {
          throw new DatabaseException("Unique constraint violated: table=" + tableName + ", index=" + indexSchema.getName() + ", key=" + DatabaseCommon.keyToString(key));
        }
        if (!replaced) {
          byte[][] newRecords = new byte[records.length + 1][];
          System.arraycopy(records, 0, newRecords, 0, records.length);
          newRecords[newRecords.length - 1] = primaryKeyBytes;
          Object address = server.toUnsafeFromRecords(newRecords);
          server.freeUnsafeIds(existingValue);
          index.put(key, address);
        }
      }
    }
    if (existingValue == null) {
      index.put(key, server.toUnsafeFromKeys(new byte[][]{primaryKeyBytes}));
    }
  }

  /**
   * Caller must synchronized index
   */

  private void doActualInsertKeyWithRecord(
      byte[] recordBytes, Object[] key, Index index, String tableName, String indexName, boolean ignoreDuplicates) {
//    int fieldCount = index.getComparators().length;
//    if (fieldCount != key.length) {
//      Object[] newKey = new Object[fieldCount];
//      for (int i = 0; i < newKey.length; i++) {
//        newKey[i] = key[i];
//      }
//      key = newKey;
//    }
    if (recordBytes == null) {
      throw new DatabaseException("Invalid record, null");
    }

    //server.getRepartitioner().notifyAdded(key, tableName, indexName);


    if (true) {
      Object newUnsafeRecords = server.toUnsafeFromRecords(new byte[][]{recordBytes});
      synchronized (index.getMutex(key)) {
        Object existingValue = index.put(key, newUnsafeRecords);
        if (existingValue != null) {
          //synchronized (index) {
          boolean sameTrans = false;
          byte[][] bytes = server.fromUnsafeToRecords(existingValue);
          long transId = Record.getTransId(recordBytes);
          for (byte[] innerBytes : bytes) {
            if (Record.getTransId(innerBytes) == transId) {
              sameTrans = true;
              break;
            }
          }
          if (!ignoreDuplicates && existingValue != null && !sameTrans) {
            index.put(key, existingValue);
            server.freeUnsafeIds(newUnsafeRecords);
            throw new DatabaseException("Unique constraint violated: table=" + tableName + ", index=" + indexName + ", key=" + DatabaseCommon.keyToString(key));
          }

          server.freeUnsafeIds(existingValue);
        }
      }
    }
    else {
      Object newValue = server.toUnsafeFromRecords(new byte[][]{recordBytes});
      synchronized (index.getMutex(key)) {
        Object existingValue = index.get(key);
        boolean sameTrans = false;
        if (existingValue != null) {
          byte[][] bytes = server.fromUnsafeToRecords(existingValue);
          long transId = Record.getTransId(recordBytes);
          for (byte[] innerBytes : bytes) {
            if (Record.getTransId(innerBytes) == transId) {
              sameTrans = true;
              break;
            }
          }
        }
        if (!ignoreDuplicates && existingValue != null && !sameTrans) {
          server.freeUnsafeIds(newValue);
          throw new DatabaseException("Unique constraint violated: table=" + tableName + ", index=" + indexName + ", key=" + DatabaseCommon.keyToString(key));
        }
        //    if (existingValue == null) {
        index.put(key, newValue);
        if (existingValue != null) {
          server.freeUnsafeIds(existingValue);
        }
      }
    }


    //    if (existingValue == null) {
    //}
    //    }
    //    else {
    //      byte[][] records = fromUnsafeToRecords(existingValue);
    //      boolean replaced = false;
    //      for (int i = 0; i < records.length; i++) {
    //        if (Arrays.equals(records[i], primaryKeyBytes)) {
    //          replaced = true;
    //          break;
    //        }
    //      }
    //      if (!replaced) {
    //        //logger.info("Replacing: table=" + tableName + ", index=" + indexName + ", key=" + key[0]);
    //        byte[][] newRecords = new byte[records.length + 1][];
    //        System.arraycopy(records, 0, newRecords, 0, records.length);
    //        newRecords[newRecords.length - 1] = recordBytes;
    //        long address = toUnsafeFromRecords(newRecords);
    //        freeUnsafeIds(existingValue);
    //        index.put(key, address);
    //      }
    //    }
  }

  //  public void indexKey(TableSchema schema, String indexName, Object[] key, long id) throws IOException {
  //
  //    //todo: add synchronization
  //    Index index = indexes.getIndices().get(schema.getName()).get(indexName);
  //    synchronized (index) {
  //      Object existingValue = index.put(key, id);
  //
  //      if (existingValue != null) {
  //        if (existingValue instanceof Long) {
  //          long[] records = new long[2];
  //          records[0] = (Long) existingValue;
  //          records[1] = id;
  //          if (records[0] != records[1]) {
  //            index.put(key, records);
  //          }
  //        }
  //        else {
  //          Long[] existingRecords = (Long[]) existingValue;
  //          boolean replaced = false;
  //          for (int i = 0; i < existingRecords.length; i++) {
  //            if (existingRecords[i] == id) {
  //              replaced = true;
  //              break;
  //            }
  //          }
  //          if (!replaced) {
  //            long[] records = new long[existingRecords.length + 1];
  //
  //            System.arraycopy(existingRecords, 0, records, 0, existingRecords.length);
  //            records[records.length - 1] = id;
  //            index.put(key, records);
  //          }
  //        }
  //      }
  //    }
  //  }

  public byte[] deleteRecord(ComObject cobj, boolean replayedCommand) {
    try {
      String dbName = cobj.getString(ComObject.Tag.dbName);
      String tableName = cobj.getString(ComObject.Tag.tableName);
      String indexName = cobj.getString(ComObject.Tag.indexName);
      int schemaVersion = cobj.getInt(ComObject.Tag.schemaVersion);
      if (!replayedCommand && schemaVersion < server.getSchemaVersion()) {
        throw new SchemaOutOfSyncException(CURR_VER_STR + server.getCommon().getSchemaVersion() + ":");
      }

      TableSchema tableSchema = server.getCommon().getTables(dbName).get(tableName);
      byte[] keyBytes = cobj.getByteArray(ComObject.Tag.keyBytes);
      Object[] key = DatabaseCommon.deserializeKey(tableSchema, keyBytes);

      Index index = server.getIndices(dbName).getIndices().get(tableName).get(indexName);
      //synchronized (index.getMutex(key)) {
      Object value = index.remove(key);
      if (value != null) {
        server.freeUnsafeIds(value);
      }
      //}

      return null;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public byte[] truncateTable(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    int schemaVersion = cobj.getInt(ComObject.Tag.schemaVersion);
    if (!replayedCommand && schemaVersion < server.getSchemaVersion()) {
      throw new SchemaOutOfSyncException(CURR_VER_STR + server.getCommon().getSchemaVersion() + ":");
    }
    String table = cobj.getString(ComObject.Tag.tableName);
    String phase = cobj.getString(ComObject.Tag.phase);
    TableSchema tableSchema = server.getCommon().getTables(dbName).get(table);
    for (Map.Entry<String, IndexSchema> entry : tableSchema.getIndices().entrySet()) {
      Index index = server.getIndices(dbName).getIndices().get(table).get(entry.getKey());
      if (entry.getValue().isPrimaryKey()) {
        if (phase.equals("primary")) {
          Map.Entry<Object[], Object> indexEntry = index.firstEntry();
          do {
            if (indexEntry == null) {
              break;
            }
            //synchronized (indexEntry.getKey()) {
            Object value = index.remove(indexEntry.getKey());
            if (value != null) {
              server.freeUnsafeIds(value);
            }
            //}
            indexEntry = index.higherEntry(indexEntry.getKey());
          }
          while (true);
        }
      }
      else if (phase.equals("secondary")) {
        Map.Entry<Object[], Object> indexEntry = index.firstEntry();
        do {
          if (indexEntry == null) {
            break;
          }
          //synchronized (indexEntry.getKey()) {
          Object value = index.remove(indexEntry.getKey());
          if (value != null) {
            server.freeUnsafeIds(value);
          }
          //}
          indexEntry = index.higherEntry(indexEntry.getKey());
        }
        while (true);
      }
    }
    return null;
  }

//  public void removeRecordFromAllIndices(TableSchema schema, Record record) throws IOException {
//     Map<String, IndexSchema> tableIndexes = schema.getIndices();
//     for (Map.Entry<String, IndexSchema> entry : tableIndexes.entrySet()) {
//       String[] indexFields = entry.getValue().getFields();
//       Object[] indexEntries = new Object[indexFields.length];
//       boolean indexedAValue = false;
//       for (int i = 0; i < indexFields.length; i++) {
//         int offset = schema.getFieldOffset(indexFields[i]);
//         indexEntries[i] = record.getFields()[offset];
//         if (indexEntries[i] != null) {
//           indexedAValue = true;
//         }
//       }
// //      if (indexedAValue) {
// //
// //        doRemoveIndexEntryByKey(schema, record.getId(), indexName, indexEntries);
// //      }
//     }
//   }

  private void doRemoveIndexEntryByKey(
      String dbName, TableSchema schema, String primaryKeyIndexName, Object[] primaryKey, String indexName,
      Object[] key) {

    Comparator[] comparators = schema.getIndices().get(primaryKeyIndexName).getComparators();

    Index index = server.getIndices(dbName).getIndices().get(schema.getName()).get(indexName);
    synchronized (index.getMutex(key)) {
      Object value = index.get(key);
      if (value == null) {
        return;
      }
      else {
        byte[][] ids = server.fromUnsafeToKeys(value);
        if (ids.length == 1) {
          boolean mismatch = false;
          if (!indexName.equals(primaryKeyIndexName)) {
            try {
              Object[] lhsKey = DatabaseCommon.deserializeKey(schema, new DataInputStream(new ByteArrayInputStream(ids[0])));
              for (int i = 0; i < lhsKey.length; i++) {
                if (0 != comparators[i].compare(lhsKey[i], primaryKey[i])) {
                  mismatch = true;
                }
              }
            }
            catch (EOFException e) {
              throw new DatabaseException(e);
            }
          }
          if (!mismatch) {
            value = index.remove(key);
            if (value != null) {
              server.freeUnsafeIds(value);
            }
          }
        }
        else {
          byte[][] newValues = new byte[ids.length - 1][];
          int offset = 0;
          boolean found = false;
          for (byte[] currValue : ids) {
            boolean mismatch = false;
            try {
              Object[] lhsKey = DatabaseCommon.deserializeKey(schema, new DataInputStream(new ByteArrayInputStream(currValue)));
              for (int i = 0; i < lhsKey.length; i++) {
                if (0 != comparators[i].compare(lhsKey[i], primaryKey[i])) {
                  mismatch = true;
                }
              }
            }
            catch (EOFException e) {
              throw new DatabaseException(e);
            }

            if (mismatch) {
              newValues[offset++] = currValue;
            }
            else {
              found = true;
            }
          }
          if (found) {
            value = server.toUnsafeFromKeys(newValues);
            value = index.put(key, value);
            if (value != null) {
              server.freeUnsafeIds(value);
            }
          }
        }
      }
    }
  }
}
