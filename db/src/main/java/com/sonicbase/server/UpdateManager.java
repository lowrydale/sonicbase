package com.sonicbase.server;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.common.Logger;
import com.sonicbase.common.Record;
import com.sonicbase.common.SchemaOutOfSyncException;
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

  public byte[] deleteIndexEntry(String command, byte[] body, boolean replayedCommand) {
    try {
      String[] parts = command.split(":");
      String dbName = parts[4];
      String tableName = parts[5];
      int schemaVersion = Integer.valueOf(parts[3]);
      if (!replayedCommand && schemaVersion < server.getSchemaVersion()) {
        throw new SchemaOutOfSyncException(CURR_VER_STR + server.getCommon().getSchemaVersion() + ":");
      }

      TableSchema tableSchema = server.getCommon().getSchema(dbName).getTables().get(tableName);
      Record record = new Record(tableSchema);
      DataInputStream in = new DataInputStream(new ByteArrayInputStream(body));
      long serializationVersion = DataUtil.readVLong(in);
      int len = in.readInt();
      byte[] bytes = new byte[len];
      in.readFully(bytes);
      record.deserialize(dbName, server.getCommon(), bytes, null);
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
          synchronized (index.getMutex(key)) {
            Object obj = index.remove(key);
            if (obj == null) {
              continue;
            }
            server.freeUnsafeIds(obj);
          }
        }
      }

      return null;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public byte[] populateIndex(String command, byte[] body) {
    command = command.replace(":populateIndex:", ":doPopulateIndex:");
    server.getLongRunningCommands().addCommand(server.getLongRunningCommands().createSingleCommand(command, body));
    return null;
  }

  public byte[] doPopulateIndex(final String command, byte[] body) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    String tableName = parts[5];
    String indexName = parts[6];

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
              server.getDatabaseClient().insertKey(dbName, tableName, keyInfo, primaryKeyIndexName, primaryKey.getKey());
            }
          }
        }
        entry = primaryKeyIndex.higherEntry(entry.getKey());
      }
    }
    return null;
  }

  public byte[] deleteIndexEntryByKey(String command, byte[] body, boolean replayedCommand) {
    AtomicBoolean isExplicitTrans = new AtomicBoolean();
    AtomicLong transactionId = new AtomicLong();
    byte[] ret = doDeleteIndexEntryByKey(command, body, replayedCommand, isExplicitTrans, transactionId, false);
    if (isExplicitTrans.get()) {
      Transaction trans = server.getTransactionManager().getTransaction(transactionId.get());
      trans.addOperation(delete, command, body, replayedCommand);
    }
    return ret;
  }

  public byte[] doDeleteIndexEntryByKey(String command, byte[] body, boolean replayedCommand,
                                        AtomicBoolean isExplicitTransRet, AtomicLong transactionIdRet, boolean isCommitting) {
    try {
      String[] parts = command.split(":");
      String dbName = parts[4];
      int schemaVersion = Integer.valueOf(parts[3]);
      if (!replayedCommand && schemaVersion < server.getSchemaVersion()) {
        throw new SchemaOutOfSyncException(CURR_VER_STR + server.getCommon().getSchemaVersion() + ":");
      }
      String tableName = parts[5];
      String indexName = parts[6];
      String primaryKeyIndexName = parts[7];
      boolean isExplicitTrans = Boolean.valueOf(parts[8]);
      //boolean isCommitting = Boolean.valueOf(parts[9]);
      long transactionId = Long.valueOf(parts[10]);
      if (isExplicitTrans && isExplicitTransRet != null) {
        isExplicitTransRet.set(true);
        transactionIdRet.set(transactionId);
      }

      TableSchema tableSchema = server.getCommon().getTables(dbName).get(tableName);
      ByteArrayInputStream bytesIn = new ByteArrayInputStream(body);
      DataInputStream in = new DataInputStream(bytesIn);
      long serializationVersion = DataUtil.readVLong(in);
      Object[] key = DatabaseCommon.deserializeKey(tableSchema, in);
      Object[] primaryKey = DatabaseCommon.deserializeKey(tableSchema, in);

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

  public byte[] batchInsertIndexEntryByKey(String command, byte[] body, boolean replayedCommand) {
    ByteArrayInputStream bytesIn = new ByteArrayInputStream(body);
    DataInputStream in = new DataInputStream(bytesIn);
    AtomicBoolean isExplicitTrans = new AtomicBoolean();
    AtomicLong transactionId = new AtomicLong();
    int count = 0;
    try {
      while (true) {
        doInsertIndexEntryByKey(command, in, replayedCommand, isExplicitTrans, transactionId, false);
        count++;
      }
    }
    catch (EOFException e) {
      //expected
      if (isExplicitTrans.get()) {
        Transaction trans = server.getTransactionManager().getTransaction(transactionId.get());
        trans.addOperation(batchInsert, command, body, replayedCommand);
      }
    }
    try {
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytesOut);
      DataUtil.writeVLong(out, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
      out.writeInt(count);
      out.close();
      return bytesOut.toByteArray();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public byte[] insertIndexEntryByKey(String command, byte[] body, boolean replayedCommand) {
    ByteArrayInputStream bytesIn = new ByteArrayInputStream(body);
    DataInputStream in = new DataInputStream(bytesIn);
    AtomicBoolean isExplicitTrans = new AtomicBoolean();
    AtomicLong transactionId = new AtomicLong();
    try {
      byte[] ret = doInsertIndexEntryByKey(command, in, replayedCommand, isExplicitTrans, transactionId, false);
      if (isExplicitTrans.get()) {
        Transaction trans = server.getTransactionManager().getTransaction(transactionId.get());
        trans.addOperation(insert, command, body, replayedCommand);
      }
      return ret;
    }
    catch (EOFException e) {
      throw new DatabaseException(e);
    }
  }

  public byte[] doInsertIndexEntryByKey(String command, DataInputStream in, boolean replayedCommand,
                                        AtomicBoolean isExplicitTransRet, AtomicLong transactionIdRet,
                                        boolean isCommitting) throws EOFException {
    try {
      if (server.getAboveMemoryThreshold().get()) {
        throw new DatabaseException("Above max memory threshold. Further inserts are not allowed");
      }

      String[] parts = command.split(":");
      String dbName = parts[4];
      int schemaVersion = Integer.valueOf(parts[3]);
      if (!replayedCommand && schemaVersion < server.getSchemaVersion()) {
        throw new SchemaOutOfSyncException(CURR_VER_STR + server.getCommon().getSchemaVersion() + ":");
      }
      long serializationVersion = DataUtil.readVLong(in);

      String tableName = in.readUTF();
      String indexName = in.readUTF();
      boolean isExplicitTrans = in.readBoolean();
      /*boolean isCommitting =*/
      in.readBoolean();
      long transactionId = DataUtil.readVLong(in);
      if (isExplicitTrans && isExplicitTransRet != null) {
        isExplicitTransRet.set(true);
        transactionIdRet.set(transactionId);
      }

      TableSchema tableSchema = server.getCommon().getTables(dbName).get(tableName);
      int len = (int) DataUtil.readVLong(in);
      Object[] key = DatabaseCommon.deserializeKey(tableSchema, in);
      len = (int) DataUtil.readVLong(in);
      byte[] primaryKeyBytes = new byte[len];
      in.readFully(primaryKeyBytes);
      IndexSchema indexSchema = tableSchema.getIndexes().get(indexName);


      Index index = server.getIndices(dbName).getIndices().get(tableSchema.getName()).get(indexName);

      Object[] primaryKey = DatabaseCommon.deserializeKey(tableSchema, new DataInputStream(new ByteArrayInputStream(primaryKeyBytes)));

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

  public byte[] batchInsertIndexEntryByKeyWithRecord(String command, byte[] body, boolean replayedCommand) {
    ByteArrayInputStream bytesIn = new ByteArrayInputStream(body);
    DataInputStream in = new DataInputStream(bytesIn);
    int count = 0;
    AtomicLong transactionId = new AtomicLong();
    AtomicBoolean isExplicitTrans = new AtomicBoolean();
    try {
      while (true) {
        doInsertIndexEntryByKeyWithRecord(command, in, replayedCommand, transactionId, isExplicitTrans, false);
        count++;
      }
    }
    catch (EOFException e) {
      //expected
      if (isExplicitTrans.get()) {
        Transaction trans = server.getTransactionManager().getTransaction(transactionId.get());
        trans.addOperation(batchInsertWithRecord, command, body, replayedCommand);
      }
    }
    try {
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytesOut);
      DataUtil.writeVLong(out, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
      out.writeInt(count);
      out.close();
      return bytesOut.toByteArray();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public byte[] insertIndexEntryByKeyWithRecord(String command, byte[] body, boolean replayedCommand) {
    ByteArrayInputStream bytesIn = new ByteArrayInputStream(body);
    DataInputStream in = new DataInputStream(bytesIn);
    try {
      AtomicBoolean isExplicitTrans = new AtomicBoolean();
      AtomicLong transactionId = new AtomicLong();
      byte[] ret = doInsertIndexEntryByKeyWithRecord(command, in, replayedCommand, transactionId, isExplicitTrans, false);
      if (isExplicitTrans.get()) {
        Transaction trans = server.getTransactionManager().getTransaction(transactionId.get());
        trans.addOperation(insertWithRecord, command, body, replayedCommand);
      }
      return ret;
    }
    catch (EOFException e) {
      throw new DatabaseException(e);
    }
  }

  public byte[] doInsertIndexEntryByKeyWithRecord(String command, DataInputStream in,
                                                  boolean replayedCommand, AtomicLong transactionIdRet,
                                                  AtomicBoolean isExpliciteTransRet, boolean isCommitting) throws EOFException {
    try {
      if (server.getAboveMemoryThreshold().get()) {
        throw new DatabaseException("Above max memory threshold. Further inserts are not allowed");
      }

      String[] parts = command.split(":");
      String dbName = parts[4];
      int schemaVersion = Integer.valueOf(parts[3]);
      if (!replayedCommand && schemaVersion < server.getSchemaVersion()) {
        throw new SchemaOutOfSyncException(CURR_VER_STR + server.getCommon().getSchemaVersion() + ":");
      }
      long serializationVersion = DataUtil.readVLong(in);

      String tableName = in.readUTF();
      String indexName = in.readUTF();
      long id = DataUtil.readVLong(in);
      boolean isExplicitTrans = in.readBoolean();
      /*boolean isCommitting =*/
      in.readBoolean();
      long transactionId = DataUtil.readVLong(in);
      if (isExplicitTrans && isExpliciteTransRet != null) {
        isExpliciteTransRet.set(true);
        transactionIdRet.set(transactionId);
      }

      TableSchema tableSchema = server.getCommon().getTables(dbName).get(tableName);
      int len = in.readInt();
      byte[] recordBytes = new byte[len];
      in.readFully(recordBytes);
      Object[] primaryKey = DatabaseCommon.deserializeKey(tableSchema, in);

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

      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytesOut);
      DataUtil.writeVLong(out, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
      out.writeInt(1);
      out.close();
      return bytesOut.toByteArray();
    }
    catch (EOFException e) {
      throw e;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public byte[] rollback(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    int schemaVersion = Integer.valueOf(parts[3]);
    if (!replayedCommand && schemaVersion < server.getSchemaVersion()) {
      throw new SchemaOutOfSyncException(CURR_VER_STR + server.getCommon().getSchemaVersion() + ":");
    }
    long transactionId = Long.valueOf(parts[5]);

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

  public byte[] commit(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    int schemaVersion = Integer.valueOf(parts[3]);
    if (!replayedCommand && schemaVersion < server.getSchemaVersion()) {
      throw new SchemaOutOfSyncException(CURR_VER_STR + server.getCommon().getSchemaVersion() + ":");
    }
    long transactionId = Long.valueOf(parts[5]);

    Transaction trans = server.getTransactionManager().getTransaction(transactionId);
    if (trans != null) {
      List<TransactionManager.Operation> ops = trans.getOperations();
      for (Operation op : ops) {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(op.getBody()));
        try {
          switch (op.getType()) {
            case insert:
              doInsertIndexEntryByKey(op.getCommand(), in, op.getReplayed(), null, null, true);
              break;
            case batchInsert:
              while (true) {
                doInsertIndexEntryByKey(command, in, replayedCommand, null, null, true);
              }
              //break;
            case insertWithRecord:
              doInsertIndexEntryByKeyWithRecord(op.getCommand(), in, op.getReplayed(), null, null, true);
              break;
            case batchInsertWithRecord:
              while (true) {
                doInsertIndexEntryByKeyWithRecord(op.getCommand(), in, op.getReplayed(), null, null, true);
              }
              //break;
            case update:
              doUpdateRecord(op.getCommand(), op.getBody(), op.getReplayed(), null, null, true);
              break;
            case delete:
              doDeleteIndexEntryByKey(op.getCommand(), op.getBody(), op.getReplayed(), null, null, true);
              break;
          }
        }
        catch (EOFException e) {
          //expected
        }
      }
      server.getTransactionManager().getTransactions().remove(transactionId);
    }
    return null;
  }

  public byte[] updateRecord(String command, byte[] body, boolean replayedCommand) {

    AtomicBoolean isExplicitTrans = new AtomicBoolean();
    AtomicLong transactionId = new AtomicLong();
    byte[] ret = doUpdateRecord(command, body, replayedCommand, isExplicitTrans, transactionId, false);
    if (isExplicitTrans.get()) {
      Transaction trans = server.getTransactionManager().getTransaction(transactionId.get());
      trans.addOperation(update, command, body, replayedCommand);
    }
    return ret;
  }

  public byte[] doUpdateRecord(String command, byte[] body, boolean replayedCommand,
                               AtomicBoolean isExplicitTransRet, AtomicLong transactionIdRet, boolean isCommitting) {
    try {
      String[] parts = command.split(":");
      String dbName = parts[4];
      int schemaVersion = Integer.valueOf(parts[3]);
      if (!replayedCommand && schemaVersion < server.getSchemaVersion()) {
        throw new SchemaOutOfSyncException(CURR_VER_STR + server.getCommon().getSchemaVersion() + ":");
      }
      String tableName = parts[5];
      String indexName = parts[6];
      boolean isExplicitTrans = Boolean.valueOf(parts[7]);
      //boolean isCommitting = Boolean.valueOf(parts[8]);
      long transactionId = Long.valueOf(parts[9]);

      if (isExplicitTrans && isExplicitTransRet != null) {
        isExplicitTransRet.set(true);
        transactionIdRet.set(transactionId);
      }

      TableSchema tableSchema = server.getCommon().getTables(dbName).get(tableName);
      DataInputStream in = new DataInputStream(new ByteArrayInputStream(body));
      long serializationVersion = DataUtil.readVLong(in);
      Object[] primaryKey = DatabaseCommon.deserializeKey(tableSchema, in);
      int len = in.readInt();
      byte[] bytes = new byte[len];
      in.read(bytes);


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
          throw new DatabaseException("Unique constraint violated: table=" + tableName + ", index=" + indexSchema.getName() +  ", key=" + DatabaseCommon.keyToString(key));
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

    server.getRepartitioner().notifyAdded(key, tableName, indexName);


    if (true) {
      Object newUnsafeRecords = server.toUnsafeFromRecords(new byte[][]{recordBytes});
      synchronized (index.getMutex(key)) {
        Object existingValue = index.unsafePutIfAbsent(key, newUnsafeRecords);
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
            throw new DatabaseException("Unique constraint violated: table=" + tableName + ", index=" + indexName +  ", key=" + DatabaseCommon.keyToString(key));
          }

          index.put(key, newUnsafeRecords);

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
          throw new DatabaseException("Unique constraint violated: table=" + tableName + ", index=" + indexName +  ", key=" + DatabaseCommon.keyToString(key));
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

  public byte[] deleteRecord(String command, byte[] body, boolean replayedCommand) {
    try {
      String[] parts = command.split(":");
      String dbName = parts[4];
      String tableName = parts[5];
      String indexName = parts[6];
      int schemaVersion = Integer.valueOf(parts[3]);
      if (!replayedCommand && schemaVersion < server.getSchemaVersion()) {
        throw new SchemaOutOfSyncException(CURR_VER_STR + server.getCommon().getSchemaVersion() + ":");
      }

      DataInputStream in = new DataInputStream(new ByteArrayInputStream(body));
      long serializationVersion = DataUtil.readVLong(in);

      TableSchema tableSchema = server.getCommon().getTables(dbName).get(tableName);
      Object[] key = DatabaseCommon.deserializeKey(tableSchema, in);

      Index index = server.getIndices(dbName).getIndices().get(tableName).get(indexName);
      synchronized (index.getMutex(key)) {
        Object value = index.remove(key);
        if (value != null) {
          server.freeUnsafeIds(value);
        }
      }

      return null;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public byte[] truncateTable(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    int schemaVersion = Integer.valueOf(parts[3]);
    if (!replayedCommand && schemaVersion < server.getSchemaVersion()) {
      throw new SchemaOutOfSyncException(CURR_VER_STR + server.getCommon().getSchemaVersion() + ":");
    }
    String table = parts[5];
    String phase = parts[6];
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
            synchronized (indexEntry.getKey()) {
              Object value = index.remove(indexEntry.getKey());
              server.freeUnsafeIds(value);
            }
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
          synchronized (indexEntry.getKey()) {
            Object value = index.remove(indexEntry.getKey());
            server.freeUnsafeIds(value);
          }
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
            Object[] lhsKey = DatabaseCommon.deserializeKey(schema, new DataInputStream(new ByteArrayInputStream(ids[0])));
            for (int i = 0; i < lhsKey.length; i++) {
              if (0 != comparators[i].compare(lhsKey[i], primaryKey[i])) {
                mismatch = true;
              }
            }
          }
          if (!mismatch) {
            server.freeUnsafeIds(value);
            index.remove(key);
          }
        }
        else {
          byte[][] newValues = new byte[ids.length - 1][];
          int offset = 0;
          boolean found = false;
          for (byte[] currValue : ids) {
            boolean mismatch = false;
            Object[] lhsKey = DatabaseCommon.deserializeKey(schema, new DataInputStream(new ByteArrayInputStream(currValue)));
            for (int i = 0; i < lhsKey.length; i++) {
              if (0 != comparators[i].compare(lhsKey[i], primaryKey[i])) {
                mismatch = true;
              }
            }

            if (mismatch) {
              newValues[offset++] = currValue;
            }
            else {
              found = true;
            }
          }
          if (found) {
            server.freeUnsafeIds(value);
            value = server.toUnsafeFromKeys(newValues);
            index.put(key, value);
          }
        }
      }
    }
  }
}
