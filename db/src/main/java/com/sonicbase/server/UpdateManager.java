package com.sonicbase.server;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.client.InsertStatementHandler;
import com.sonicbase.common.*;
import com.sonicbase.index.Index;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.query.ResultSet;
import com.sonicbase.query.impl.ColumnImpl;
import com.sonicbase.query.impl.SelectStatementImpl;
import com.sonicbase.schema.DataType;
import com.sonicbase.schema.FieldSchema;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.sonicbase.server.TransactionManager.OperationType.*;

/**
 * Responsible for
 */
@SuppressWarnings("squid:S1172") // all methods called from method invoker must have cobj and replayed command parms
public class UpdateManager {

  public static final String ERROR_INSERTING_RECORD_STR = "Error inserting record";
  public static final String OUT_OF_ORDER_UPDATE_DETECTED_KEY_STR = "Out of order update detected: key=";
  private static Logger logger = LoggerFactory.getLogger(UpdateManager.class);

  private static final String CURR_VER_STR = "currVer:";
  private final com.sonicbase.server.DatabaseServer server;
  private StreamManagerProxy streamManager;
  private List<Object> producers = new ArrayList<>();
  private AtomicLong batchCount = new AtomicLong();
  private AtomicLong batchEntryCount = new AtomicLong();
  private AtomicLong lastBatchLogReset = new AtomicLong(System.currentTimeMillis());
  private AtomicLong batchDuration = new AtomicLong();

  private AtomicLong insertCount = new AtomicLong();
  private AtomicLong lastReset = new AtomicLong(System.currentTimeMillis());
  private ThreadLocal<Boolean> threadLocalIsBatchRequest = new ThreadLocal<>();


  public UpdateManager(DatabaseServer databaseServer) {
    this.server = databaseServer;
  }

  public void initStreamManager() {
    streamManager = new StreamManagerProxy(server.getProServer());
    try {
      streamManager.initPublisher();
    }
    catch (Exception e) {
      logger.error("Error initializing stream manager", e);
    }
  }

  @SchemaReadLock
  public ComObject deleteIndexEntry(ComObject cobj, boolean replayedCommand) {

    AtomicBoolean isExplicitTrans = new AtomicBoolean();
    AtomicLong transactionId = new AtomicLong();

    final long sequence0 = cobj.getLong(ComObject.Tag.SEQUENCE_0);
    final long sequence1 = cobj.getLong(ComObject.Tag.SEQUENCE_1);

    doDeleteIndexEntry(cobj, replayedCommand, sequence0, sequence1, isExplicitTrans, transactionId, false);

    if (isExplicitTrans.get()) {
      TransactionManager.Transaction trans = server.getTransactionManager().getTransaction(transactionId.get());
      String command = "UpdateManager:ComObject:deleteIndexEntryByKey:";
      trans.addOperation(DELETE_INDEX_ENTRY, command, cobj.serialize(), replayedCommand);
    }

    return null;
  }

  private void doDeleteIndexEntry(ComObject cobj, boolean replayedCommand, long sequence0, long sequence1,
                                  AtomicBoolean isExplicitTransRet,
                                  AtomicLong transactionIdRet, boolean isCommitting) {
    String dbName = cobj.getString(ComObject.Tag.DB_NAME);
    String tableName = cobj.getString(ComObject.Tag.TABLE_NAME);
    byte[] primaryKeyBytes = cobj.getByteArray(ComObject.Tag.PRIMARY_KEY_BYTES);
    Integer schemaVersion = cobj.getInt(ComObject.Tag.SCHEMA_VERSION);
    if (!replayedCommand && schemaVersion != null && schemaVersion < server.getSchemaVersion()) {
      throw new SchemaOutOfSyncException(CURR_VER_STR + server.getCommon().getSchemaVersion() + ":");
    }

    boolean isExplicitTrans = cobj.getBoolean(ComObject.Tag.IS_EXCPLICITE_TRANS);
    long transactionId = cobj.getLong(ComObject.Tag.TRANSACTION_ID);
    if (isExplicitTrans && isExplicitTransRet != null) {
      isExplicitTransRet.set(true);
      transactionIdRet.set(transactionId);
    }

    AtomicBoolean shouldExecute = new AtomicBoolean();
    AtomicBoolean shouldDeleteLock = new AtomicBoolean();

    TableSchema tableSchema = server.getCommon().getTableSchema(dbName, tableName, server.getDataDir());
    Record record = new Record(tableSchema);
    byte[] recordBytes = cobj.getByteArray(ComObject.Tag.RECORD_BYTES);
    record.deserialize(dbName, server.getCommon(), recordBytes, null);
    List<FieldSchema> fieldSchemas = tableSchema.getFields();

    for (Map.Entry<String, IndexSchema> indexSchema : tableSchema.getIndices().entrySet()) {
      String[] fields = indexSchema.getValue().getFields();
      boolean shouldIndex = true;
      for (int i = 0; i < fields.length; i++) {
        boolean found = false;
        for (int j = 0; j < fieldSchemas.size(); j++) {
          if (fields[i].equals(fieldSchemas.get(j).getName()) && record.getFields()[j] != null) {
            found = true;
            break;
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
              break;
            }
          }
        }
        Object[] primaryKey = null;
        try {
          primaryKey = DatabaseCommon.deserializeKey(tableSchema, primaryKeyBytes);
        }
        catch (Exception e) {
          throw new DatabaseException(e);
        }
        server.getTransactionManager().preHandleTransaction(dbName, tableName, indexSchema.getKey(), isExplicitTrans, isCommitting,
            transactionId, primaryKey, shouldExecute, shouldDeleteLock);

        if (shouldExecute.get()) {
          Index index = server.getIndex(dbName, tableSchema.getName(), indexSchema.getKey());
          byte[][] records = null;
          List<byte[]> deletedRecords = new ArrayList<>();
          synchronized (index.getMutex(key)) {
            Object value = index.get(key);
            if (value != null) {
              records = server.getAddressMap().fromUnsafeToKeys(value);
            }
            if (records != null) {
              byte[][] newRecords = new byte[records.length - 1][];
              boolean found = false;
              int offset = 0;

              int deletedOffset = -1;
              for (int i = 0; i < records.length; i++) {
                if (Arrays.equals(KeyRecord.getPrimaryKey(records[i]), primaryKeyBytes)) {
                  found = true;
                  deletedRecords.add(records[i]);
                  deletedOffset = i;
                }
              }

              if (deletedOffset != -1) {
                for (int i = 0; i < records.length; i++) {
                  if (i != deletedOffset) {
                    newRecords[offset++] = records[i];
                  }
                }
              }
              if (found) {
                if (newRecords.length == 0) {
                  Object obj = index.remove(key);
                  if (obj != null) {
                    server.getAddressMap().freeUnsafeIds(obj);
                    index.addAndGetCount(-1);
                  }
                }
                else {
                  index.put(key, server.getAddressMap().toUnsafeFromKeys(newRecords));
                  if (value != null) {
                    server.getAddressMap().freeUnsafeIds(value);
                  }
                }
              }
            }
          }
        }

        if (indexSchema.getValue().isPrimaryKey() && shouldDeleteLock.get()) {
          server.getTransactionManager().deleteLock(dbName, tableName, transactionId, tableSchema, key);
        }
      }
    }
  }

  public ComObject populateIndex(ComObject cobj, boolean replayedCommand) {
    cobj.put(ComObject.Tag.METHOD, "UpdateManager:doPopulateIndex");
    server.getLongRunningCommands().addCommand(
        server.getLongRunningCommands().createSingleCommand(cobj.serialize()));
    return null;
  }

  public ComObject doPopulateIndex(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.DB_NAME);
    String tableName = cobj.getString(ComObject.Tag.TABLE_NAME);
    String indexName = cobj.getString(ComObject.Tag.INDEX_NAME);

    TableSchema tableSchema = server.getCommon().getTableSchema(dbName, tableName, server.getDataDir());
    String primaryKeyIndexName = null;
    for (Map.Entry<String, IndexSchema> entry : tableSchema.getIndices().entrySet()) {
      if (entry.getValue().isPrimaryKey()) {
        primaryKeyIndexName = entry.getKey();
      }
    }

    Index primaryKeyIndex = server.getIndex(dbName, tableName, primaryKeyIndexName);
    Map.Entry<Object[], Object> entry = primaryKeyIndex.firstEntry();
    while (entry != null) {
      synchronized (primaryKeyIndex.getMutex(entry.getKey())) {
        Object value = primaryKeyIndex.get(entry.getKey());
        if (!value.equals(0L)) {
          byte[][] records = server.getAddressMap().fromUnsafeToRecords(value);
          for (int i = 0; i < records.length; i++) {
            Record record = new Record(dbName, server.getCommon(), records[i]);
            Object[] fields = record.getFields();
            List<String> columnNames = new ArrayList<>();
            List<Object> values = new ArrayList<>();
            for (int j = 0; j < fields.length; j++) {
              values.add(fields[j]);
              columnNames.add(tableSchema.getFields().get(j).getName());
            }

            InsertStatementHandler.KeyInfo primaryKey = new InsertStatementHandler.KeyInfo();
            tableSchema = server.getCommon().getTableSchema(dbName, tableName, server.getDataDir());

            long id = 0;
            if (tableSchema.getFields().get(0).getName().equals("_sonicbase_id")) {
              id = (long) record.getFields()[0];
            }
            List<InsertStatementHandler.KeyInfo> keys = InsertStatementHandler.getKeys(server.getCommon(), tableSchema, columnNames, values, id);

            for (final InsertStatementHandler.KeyInfo keyInfo : keys) {
              if (keyInfo.getIndexSchema().isPrimaryKey()) {
                primaryKey.setKey(keyInfo.getKey());
                primaryKey.setIndexSchema(keyInfo.getIndexSchema());
                break;
              }
            }
            for (final InsertStatementHandler.KeyInfo keyInfo : keys) {
              if (keyInfo.getIndexSchema().getName().equals(indexName)) {
                int schemaRetryCount = 0;
                while (true) {
                  try {
                    KeyRecord keyRecord = new KeyRecord();
                    byte[] primaryKeyBytes = DatabaseCommon.serializeKey(tableSchema,
                        primaryKeyIndexName, primaryKey.getKey());
                    keyRecord.setPrimaryKey(primaryKeyBytes);
                    keyRecord.setDbViewNumber(server.getCommon().getSchemaVersion());
                    InsertStatementHandler.insertKey(server.getClient(), dbName, tableName, keyInfo, primaryKeyIndexName,
                        primaryKey.getKey(), keyRecord, true, schemaRetryCount);
                    break;
                  }
                  catch (SchemaOutOfSyncException e) {
                    schemaRetryCount++;
                  }
                  catch (Exception e) {
                    throw new DatabaseException(e);
                  }
                }
              }
            }
          }
        }
        entry = primaryKeyIndex.higherEntry(entry.getKey());
      }
    }
    return null;
  }

  @SchemaReadLock
  public ComObject deleteIndexEntryByKey(ComObject cobj, boolean replayedCommand) {
    AtomicBoolean isExplicitTrans = new AtomicBoolean();
    AtomicLong transactionId = new AtomicLong();
    final long sequence0 = cobj.getLong(ComObject.Tag.SEQUENCE_0);
    final long sequence1 = cobj.getLong(ComObject.Tag.SEQUENCE_1);

    ComObject ret = doDeleteIndexEntryByKey(cobj, replayedCommand, sequence0, sequence1, isExplicitTrans, transactionId, false);
    if (isExplicitTrans.get()) {
      TransactionManager.Transaction trans = server.getTransactionManager().getTransaction(transactionId.get());
      String command = "UpdateManager:ComObject:deleteIndexEntryByKey:";
      trans.addOperation(DELETE_ENTRY_BY_KEY, command, cobj.serialize(), replayedCommand);
    }
    return ret;
  }

  public ComObject doDeleteIndexEntryByKey(ComObject cobj, boolean replayedCommand,
                                           long sequence0, long sequence1, AtomicBoolean isExplicitTransRet, AtomicLong transactionIdRet, boolean isCommitting) {
    try {
      String dbName = cobj.getString(ComObject.Tag.DB_NAME);
      Integer schemaVersion = cobj.getInt(ComObject.Tag.SCHEMA_VERSION);
      if (!replayedCommand && schemaVersion != null && schemaVersion < server.getSchemaVersion()) {
        throw new SchemaOutOfSyncException(CURR_VER_STR + server.getCommon().getSchemaVersion() + ":");
      }
      String tableName = cobj.getString(ComObject.Tag.TABLE_NAME);
      String indexName = cobj.getString(ComObject.Tag.INDEX_NAME);
      String primaryKeyIndexName = cobj.getString(ComObject.Tag.PRIMARY_KEY_INDEX_NAME);
      boolean isExplicitTrans = cobj.getBoolean(ComObject.Tag.IS_EXCPLICITE_TRANS);
      long transactionId = cobj.getLong(ComObject.Tag.TRANSACTION_ID);
      if (isExplicitTrans && isExplicitTransRet != null) {
        isExplicitTransRet.set(true);
        transactionIdRet.set(transactionId);
      }

      TableSchema tableSchema = server.getCommon().getTableSchema(dbName, tableName, server.getDataDir());
      byte[] keyBytes = cobj.getByteArray(ComObject.Tag.KEY_BYTES);
      byte[] primaryKeyBytes = cobj.getByteArray(ComObject.Tag.PRIMARY_KEY_BYTES);
      Object[] key = DatabaseCommon.deserializeKey(tableSchema, keyBytes);
      Object[] primaryKey = DatabaseCommon.deserializeKey(tableSchema, primaryKeyBytes);

      AtomicBoolean shouldExecute = new AtomicBoolean();
      AtomicBoolean shouldDeleteLock = new AtomicBoolean();

      server.getTransactionManager().preHandleTransaction(dbName, tableName, indexName, isExplicitTrans, isCommitting, transactionId, primaryKey, shouldExecute, shouldDeleteLock);

      if (shouldExecute.get()) {
        doRemoveIndexEntryByKey(dbName, tableSchema, primaryKeyIndexName, primaryKey, indexName, key, sequence0, sequence1);
      }

      if (shouldDeleteLock.get()) {
        server.getTransactionManager().deleteLock(dbName, tableName, transactionId, tableSchema, primaryKey);
      }
      return null;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  @SchemaReadLock
  public ComObject batchInsertIndexEntryByKey(ComObject cobj, boolean replayedCommand) {
    AtomicBoolean isExplicitTrans = new AtomicBoolean();
    AtomicLong transactionId = new AtomicLong();
    int count = 0;
    try {
      ComArray array = cobj.getArray(ComObject.Tag.INSERT_OBJECTS);
      for (int i = 0; i < array.getArray().size(); i++) {
        ComObject innerObj = (ComObject) array.getArray().get(i);

        throttle();
        doInsertIndexEntryByKey(cobj, innerObj, replayedCommand, isExplicitTrans, transactionId, false);
        count++;
      }
      if (isExplicitTrans.get()) {
        TransactionManager.Transaction trans = server.getTransactionManager().getTransaction(transactionId.get());
        String command = "UpdateManager:ComObject:batchInsertIndexEntryByKey:";
        trans.addOperation(BATCH_INSERT, command, cobj.serialize(), replayedCommand);
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }

    ComObject retObj = new ComObject();
    retObj.put(ComObject.Tag.COUNT, count);
    return retObj;
  }

  @SchemaReadLock
  public ComObject insertIndexEntryByKey(ComObject cobj, boolean replayedCommand) {

    Integer schemaVersion = cobj.getInt(ComObject.Tag.SCHEMA_VERSION);
    if (!replayedCommand && schemaVersion != null && schemaVersion < server.getSchemaVersion()) {
      throw new SchemaOutOfSyncException(CURR_VER_STR + server.getCommon().getSchemaVersion() + ":");
    }

    AtomicBoolean isExplicitTrans = new AtomicBoolean();
    AtomicLong transactionId = new AtomicLong();
    try {
      ComObject ret = doInsertIndexEntryByKey(cobj, cobj, replayedCommand, isExplicitTrans, transactionId, false);
      if (isExplicitTrans.get()) {
        TransactionManager.Transaction trans = server.getTransactionManager().getTransaction(transactionId.get());
        String command = "UpdateManager:ComObject:insertIndexEntryByKey:";
        trans.addOperation(TransactionManager.OperationType.INSERT, command, cobj.serialize(), replayedCommand);
      }
      return ret;
    }
    catch (EOFException e) {
      throw new DatabaseException(e);
    }
  }

  public ComObject doInsertIndexEntryByKey(ComObject outerCobj, ComObject cobj, boolean replayedCommand,
                                           AtomicBoolean isExplicitTransRet, AtomicLong transactionIdRet,
                                           boolean isCommitting) throws EOFException {
    try {
      String dbName = outerCobj.getString(ComObject.Tag.DB_NAME);
      Integer schemaVersion = outerCobj.getInt(ComObject.Tag.SCHEMA_VERSION);
      if (!replayedCommand && schemaVersion != null && schemaVersion < server.getSchemaVersion()) {
        throw new SchemaOutOfSyncException(CURR_VER_STR + server.getCommon().getSchemaVersion() + ":");
      }

      long sequence0 = outerCobj.getLong(ComObject.Tag.SEQUENCE_0);
      long sequence1 = outerCobj.getLong(ComObject.Tag.SEQUENCE_1);

      String tableName = server.getCommon().getTablesById(dbName).get(cobj.getInt(ComObject.Tag.TABLE_ID)).getName();
      String indexName = null;
      try {
        IndexSchema indexSchema = server.getCommon().getTablesById(dbName).get(cobj.getInt(ComObject.Tag.TABLE_ID)).getIndexesById().get(cobj.getInt(ComObject.Tag.INDEX_ID));
        indexName = indexSchema.getName();
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
      boolean isExplicitTrans = outerCobj.getBoolean(ComObject.Tag.IS_EXCPLICITE_TRANS);

      long transactionId = outerCobj.getLong(ComObject.Tag.TRANSACTION_ID);
      if (isExplicitTrans && isExplicitTransRet != null) {
        isExplicitTransRet.set(true);
        transactionIdRet.set(transactionId);
      }

      TableSchema tableSchema = server.getCommon().getTableSchema(dbName, tableName, server.getDataDir());
      Object[] key = DatabaseCommon.deserializeKey(tableSchema, cobj.getByteArray(ComObject.Tag.KEY_BYTES));
      byte[] keyRecordBytes = cobj.getByteArray(ComObject.Tag.KEY_RECORD_BYTES);
      IndexSchema indexSchema = tableSchema.getIndices().get(indexName);
      KeyRecord keyRecord = new KeyRecord(keyRecordBytes);

      Index index = server.getIndex(dbName, tableSchema.getName(), indexName);

      Object[] primaryKey = DatabaseCommon.deserializeKey(tableSchema, keyRecord.getPrimaryKey());

      AtomicBoolean shouldExecute = new AtomicBoolean();
      AtomicBoolean shouldDeleteLock = new AtomicBoolean();

      server.getTransactionManager().preHandleTransaction(dbName, tableName, indexName, isExplicitTrans, isCommitting, transactionId, primaryKey, shouldExecute, shouldDeleteLock);

      KeyRecord.setSequence0(keyRecordBytes, sequence0);
      KeyRecord.setSequence1(keyRecordBytes, sequence1);

      if (shouldExecute.get()) {
        doInsertKey(key, keyRecordBytes, tableName, index, indexSchema);
      }

      if (shouldDeleteLock.get()) {
        server.getTransactionManager().deleteLock(dbName, tableName, transactionId, tableSchema, primaryKey);
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

  public void startStreamsConsumerMasterMonitor() {
    streamManager.startStreamsConsumerMasterMonitor();
  }

  private static class InsertRequest {
    private final ComObject innerObj;
    private final long sequence0;
    private final long sequence1;
    private final short sequence2;
    private final boolean replayedCommand;
    private final boolean isCommitting;

    public InsertRequest(ComObject innerObj, long sequence0, long sequence1, short sequence2, boolean replayedCommand,
                         boolean isCommitting) {
      this.innerObj = innerObj;
      this.sequence0 = sequence0;
      this.sequence1 = sequence1;
      this.sequence2 = sequence2;
      this.replayedCommand = replayedCommand;
      this.isCommitting = isCommitting;
    }

  }

  @SchemaReadLock
  public ComObject batchInsertIndexEntryByKeyWithRecord(final ComObject cobj, final boolean replayedCommand) {
    Integer schemaVersion = cobj.getInt(ComObject.Tag.SCHEMA_VERSION);
    if (!replayedCommand && schemaVersion != null && schemaVersion < server.getSchemaVersion()) {
      throw new SchemaOutOfSyncException(CURR_VER_STR + server.getCommon().getSchemaVersion() + ":");
    }

    threadLocalIsBatchRequest.set(true);
    streamManager.initBatchInsert();

    final long sequence0 = cobj.getLong(ComObject.Tag.SEQUENCE_0);
    final long sequence1 = cobj.getLong(ComObject.Tag.SEQUENCE_1);

    final boolean isExplicitTrans = cobj.getBoolean(ComObject.Tag.IS_EXCPLICITE_TRANS);
    final long transactionId = cobj.getLong(ComObject.Tag.TRANSACTION_ID);
    int count = 0;

    final ComObject retObj = new ComObject();

    try {
      List<Future> futures = new ArrayList<>();
      final ComArray array = cobj.getArray(ComObject.Tag.INSERT_OBJECTS);

      batchEntryCount.addAndGet(array.getArray().size());
      if (batchCount.incrementAndGet() % 1000 == 0) {
        logger.info("batchInsert stats: batchSize=" + array.getArray().size() + ", avgBatchSize=" +
            (batchEntryCount.get() / batchCount.get()) +
            ", avgBatchDuration=" + ((double)batchDuration.get() / batchCount.get() / 1000000d));
        synchronized (lastBatchLogReset) {
          if (System.currentTimeMillis() - lastBatchLogReset.get() > 4 * 60 * 1000) {
            lastBatchLogReset.set(System.currentTimeMillis());
            batchCount.set(0);
            batchEntryCount.set(0);
            batchDuration.set(0);
          }
        }
      }

      final ComArray batchResponses = retObj.putArray(ComObject.Tag.BATCH_RESPONSES, ComObject.Type.OBJECT_TYPE);
      final long begin = System.nanoTime();
      List<InsertRequest> requests = new ArrayList<>();
      for (int i = 0; i < array.getArray().size(); i++) {
        final int offset = i;

        final ComObject innerObj = (ComObject) array.getArray().get(offset);

        short sequence2 = (short) offset;
        if (replayedCommand) {
          InsertRequest request = new InsertRequest(innerObj, sequence0, sequence1, sequence2, replayedCommand, false);
          doInsertIndexEntryByKeyWithRecord(cobj, request.innerObj, request.sequence0, request.sequence1, request.sequence2,
              request.replayedCommand, transactionId, isExplicitTrans, request.isCommitting, batchResponses);
        }
        else {

          throttle();

          try {
            doInsertIndexEntryByKeyWithRecord(cobj, innerObj, sequence0, sequence1, sequence2, replayedCommand,
                transactionId, isExplicitTrans, false, batchResponses);
          }
          catch (Exception e) {
            if (-1 != ExceptionUtils.indexOfThrowable(e, SchemaOutOfSyncException.class)) {
              throw new DatabaseException(e);
            }
            else {
              logger.error(ERROR_INSERTING_RECORD_STR, e);
            }
          }

        }
        count++;
      }

      for (InsertRequest request : requests) {
        try {
          doInsertIndexEntryByKeyWithRecord(cobj, request.innerObj, request.sequence0, request.sequence1, request.sequence2,
              request.replayedCommand, transactionId, isExplicitTrans, request.isCommitting, batchResponses);
        }
        catch (Exception e) {
          if (-1 != ExceptionUtils.indexOfThrowable(e, SchemaOutOfSyncException.class)) {
            throw new DatabaseException(e);
          }
          else {
            logger.error(ERROR_INSERTING_RECORD_STR, e);
          }
        }
      }

      for (Future future : futures) {
        try {
          future.get();
        }
        catch (Exception e) {
          if (-1 != ExceptionUtils.indexOfThrowable(e, SchemaOutOfSyncException.class)) {
            throw new DatabaseException(e);
          }
          else {
            logger.error(ERROR_INSERTING_RECORD_STR, e);
          }
        }
      }
      if (isExplicitTrans) {
        TransactionManager.Transaction trans = server.getTransactionManager().getTransaction(transactionId);
        String command = "UpdateManager:ComObject:batchInsertIndexEntryByKeyWithRecord:";
        trans.addOperation(BATCH_INSERT_WITH_RECORD, command, cobj.serialize(), replayedCommand);
      }

      streamManager.publishBatch(cobj);

      batchDuration.addAndGet(System.nanoTime() - begin);
    }
    catch (SchemaOutOfSyncException e) {
      throw e;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
    finally {
      streamManager.batchInsertFinish();
      threadLocalIsBatchRequest.set(false);
    }
    retObj.put(ComObject.Tag.COUNT, count);
    return retObj;
  }

  private void throttle() throws InterruptedException {
    insertCount.incrementAndGet();
    synchronized (insertCount) {
      if (System.currentTimeMillis() - lastReset.get() > 30_000) {
        lastReset.set(System.currentTimeMillis());
        insertCount.set(0);
      }
      while (insertCount.get() / (double) (System.currentTimeMillis() - lastReset.get()) * 1000d > 200_000) {
        Thread.sleep(20);
      }
    }
  }

  @SchemaReadLock
  public ComObject insertIndexEntryByKeyWithRecord(ComObject cobj, boolean replayedCommand) {
    try {
      Integer schemaVersion = cobj.getInt(ComObject.Tag.SCHEMA_VERSION);
      if (!replayedCommand && schemaVersion != null && schemaVersion < server.getSchemaVersion()) {
        throw new SchemaOutOfSyncException(CURR_VER_STR + server.getCommon().getSchemaVersion() + ":");
      }

      long sequence0 = cobj.getLong(ComObject.Tag.SEQUENCE_0);
      long sequence1 = cobj.getLong(ComObject.Tag.SEQUENCE_1);
      short sequence2 = 0;
      final boolean isExplicitTrans = cobj.getBoolean(ComObject.Tag.IS_EXCPLICITE_TRANS);
      final long transactionId = cobj.getLong(ComObject.Tag.TRANSACTION_ID);

      ComObject ret = doInsertIndexEntryByKeyWithRecord(cobj, cobj, sequence0, sequence1, sequence2, replayedCommand,
          transactionId, isExplicitTrans, false, null);
      if (isExplicitTrans) {
        TransactionManager.Transaction trans = server.getTransactionManager().getTransaction(transactionId);
        String command = "UpdateManager:ComObject:insertIndexEntryByKeyWithRecord:";
        trans.addOperation(INSERT_WITH_RECORD, command, cobj.serialize(), replayedCommand);
      }
      if (server.isThrottleInsert()) {
        Thread.sleep(1);
      }
      return ret;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public ComObject doInsertIndexEntryByKeyWithRecord(ComObject outerCobj, ComObject cobj, long sequence0, long sequence1,
                                                     short sequence2, boolean replayedCommand, long transactionId,
                                                     boolean isExpliciteTrans, boolean isCommitting, ComArray batchResponses) {
    int originalOffset = cobj.getInt(ComObject.Tag.ORIGINAL_OFFSET);

    try {
      String dbName = outerCobj.getString(ComObject.Tag.DB_NAME);
      Integer schemaVersion = outerCobj.getInt(ComObject.Tag.SCHEMA_VERSION);
      if (!replayedCommand && schemaVersion != null && schemaVersion < server.getSchemaVersion()) {
        throw new SchemaOutOfSyncException(CURR_VER_STR + server.getCommon().getSchemaVersion() + ":");
      }

      TableSchema tableSchema = server.getCommon().getTablesById(dbName).get(cobj.getInt(ComObject.Tag.TABLE_ID));
      IndexSchema indexSchema;
      try {
        indexSchema = tableSchema.getIndexesById().get(cobj.getInt(ComObject.Tag.INDEX_ID));
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
      String tableName = tableSchema.getName();
      String indexName = indexSchema.getName();

      byte[] recordBytes = cobj.getByteArray(ComObject.Tag.RECORD_BYTES);

      Record.setSequences(recordBytes, sequence0, sequence1, sequence2);

      byte[] keyBytes = cobj.getByteArray(ComObject.Tag.KEY_BYTES);
      Object[] primaryKey = DatabaseCommon.deserializeKey(tableSchema, keyBytes);

      Boolean ignore = cobj.getBoolean(ComObject.Tag.IGNORE);
      if (ignore == null) {
        ignore = false;
      }

      AtomicBoolean shouldExecute = new AtomicBoolean();
      AtomicBoolean shouldDeleteLock = new AtomicBoolean();
      server.getTransactionManager().preHandleTransaction(dbName, tableName, indexName, isExpliciteTrans, isCommitting, transactionId, primaryKey, shouldExecute, shouldDeleteLock);

      Index index = server.getIndex(dbName, tableName, indexName);
      if (shouldExecute.get()) {
        String[] indexFields = indexSchema.getFields();
        int[] fieldOffsets = new int[indexFields.length];
        for (int i = 0; i < indexFields.length; i++) {
          fieldOffsets[i] = tableSchema.getFieldOffset(indexFields[i]);
        }
        doInsertKey(outerCobj, dbName, recordBytes, primaryKey, index, tableSchema.getName(), indexName, ignore || replayedCommand);
      }
      else {
        if (transactionId != 0) {
          TransactionManager.Transaction trans = server.getTransactionManager().getTransaction(transactionId);
          List<Record> records = trans.getRecords().get(tableName);
          if (records == null) {
            records = new ArrayList<>();
            trans.getRecords().put(tableName, records);
          }
          Record record = new Record(dbName, server.getCommon(), recordBytes);
          records.add(record);
        }
      }

      if (shouldDeleteLock.get()) {
        server.getTransactionManager().deleteLock(dbName, tableName, transactionId, tableSchema, primaryKey);
      }

      if (batchResponses != null) {
        synchronized (batchResponses) {
          ComObject obj = new ComObject();
          obj.put(ComObject.Tag.ORIGINAL_OFFSET, originalOffset);
          obj.put(ComObject.Tag.INT_STATUS, InsertStatementHandler.BATCH_STATUS_SUCCCESS);
          batchResponses.add(obj);
        }
      }
      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.COUNT, 1);
      return retObj;
    }
    catch (Exception e) {
      if (-1 != ExceptionUtils.indexOfThrowable(e, UniqueConstraintViolationException.class)) {
        if (batchResponses != null) {
          synchronized (batchResponses) {
            ComObject obj = new ComObject();
            obj.put(ComObject.Tag.ORIGINAL_OFFSET, originalOffset);
            obj.put(ComObject.Tag.INT_STATUS, InsertStatementHandler.BATCH_STATUS_UNIQUE_CONSTRAINT_VIOLATION);
            batchResponses.add(obj);
          }
        }
      }
      else {
        if (batchResponses != null) {
          synchronized (batchResponses) {
            ComObject obj = new ComObject();
            obj.put(ComObject.Tag.ORIGINAL_OFFSET, originalOffset);
            obj.put(ComObject.Tag.INT_STATUS, InsertStatementHandler.BATCH_STATUS_FAILED);
            batchResponses.add(obj);
          }
        }
      }

      throw new DatabaseException(e);
    }
  }

  @SchemaReadLock
  public ComObject rollback(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.DB_NAME);
    Integer schemaVersion = cobj.getInt(ComObject.Tag.SCHEMA_VERSION);
    if (!replayedCommand && schemaVersion != null && schemaVersion < server.getSchemaVersion()) {
      throw new SchemaOutOfSyncException(CURR_VER_STR + server.getCommon().getSchemaVersion() + ":");
    }
    long transactionId = cobj.getLong(ComObject.Tag.TRANSACTION_ID);

    TransactionManager.Transaction trans = server.getTransactionManager().getTransaction(transactionId);
    Map<String, ConcurrentSkipListMap<Object[], TransactionManager.RecordLock>> tableLocks =
        server.getTransactionManager().getLocks(dbName);
    if (trans != null) {
      List<TransactionManager.RecordLock> locks = trans.getLocks();
      for (TransactionManager.RecordLock lock : locks) {
        String tableName = lock.getTableName();
        tableLocks.get(tableName).remove(lock.getPrimaryKey());
      }
      server.getTransactionManager().getTransactions().remove(transactionId);
    }
    return null;
  }

  @SchemaReadLock
  public ComObject commit(ComObject cobj, boolean replayedCommand) {
    long sequence0 = cobj.getLong(ComObject.Tag.SEQUENCE_0);
    long sequence1 = cobj.getLong(ComObject.Tag.SEQUENCE_1);

    Integer schemaVersion = cobj.getInt(ComObject.Tag.SCHEMA_VERSION);
    if (!replayedCommand && schemaVersion != null && schemaVersion < server.getSchemaVersion()) {
      throw new SchemaOutOfSyncException(CURR_VER_STR + server.getCommon().getSchemaVersion() + ":");
    }
    long transactionId = cobj.getLong(ComObject.Tag.TRANSACTION_ID);
    Boolean isExplicitTrans = cobj.getBoolean(ComObject.Tag.IS_EXCPLICITE_TRANS);
    if (isExplicitTrans == null) {
      isExplicitTrans = true;
    }

    TransactionManager.Transaction trans = server.getTransactionManager().getTransaction(transactionId);
    if (trans != null) {
      List<TransactionManager.Operation> ops = trans.getOperations();
      for (TransactionManager.Operation op : ops) {
        byte[] opBody = op.getBody();
        try {
          switch (op.getType()) {
            case INSERT:
              doInsertIndexEntryByKey(new ComObject(opBody), new ComObject(opBody), op.getReplayed(), null, null, true);
              break;
            case BATCH_INSERT:
              cobj = new ComObject(opBody);
              ComArray array = cobj.getArray(ComObject.Tag.INSERT_OBJECTS);
              for (int i = 0; i < array.getArray().size(); i++) {
                ComObject innerObj = (ComObject) array.getArray().get(i);
                doInsertIndexEntryByKey(cobj, innerObj, replayedCommand, null, null, true);
              }
              break;
            case INSERT_WITH_RECORD:
              doInsertIndexEntryByKeyWithRecord(cobj, new ComObject(opBody), sequence0, sequence1, (short) 0,
                  op.getReplayed(), transactionId, isExplicitTrans, true, null);
              break;
            case BATCH_INSERT_WITH_RECORD:
              threadLocalIsBatchRequest.set(true);
              streamManager.initBatchInsert();

              try {
                cobj = new ComObject(opBody);
                array = cobj.getArray(ComObject.Tag.INSERT_OBJECTS);
                for (int i = 0; i < array.getArray().size(); i++) {
                  ComObject innerObj = (ComObject) array.getArray().get(i);
                  doInsertIndexEntryByKeyWithRecord(cobj, innerObj, sequence0, sequence1, (short) i, op.getReplayed(),
                      transactionId, isExplicitTrans, true, null);
                }
                streamManager.publishBatch(cobj);
              }
              finally {
                threadLocalIsBatchRequest.set(false);
                streamManager.batchInsertFinish();
              }
              break;
            case UPDATE:
              doUpdateRecord(new ComObject(op.getBody()), op.getReplayed(), null, null, true);
              break;
            case DELETE_ENTRY_BY_KEY:
              doDeleteIndexEntryByKey(new ComObject(op.getBody()), op.getReplayed(), sequence0, sequence1, null, null, true);
              break;
            case DELETE_RECORD:
              doDeleteRecord(new ComObject(op.getBody()), op.getReplayed(), sequence0, sequence1,null, null, true);
              break;
            case DELETE_INDEX_ENTRY:
              doDeleteIndexEntry(new ComObject(op.getBody()), op.getReplayed(), sequence0, sequence1,null, null, true);
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

  @SchemaReadLock
  public ComObject updateRecord(ComObject cobj, boolean replayedCommand) {

    AtomicBoolean isExplicitTrans = new AtomicBoolean();
    AtomicLong transactionId = new AtomicLong();
    ComObject ret = doUpdateRecord(cobj, replayedCommand, isExplicitTrans, transactionId, false);
    if (isExplicitTrans.get()) {
      TransactionManager.Transaction trans = server.getTransactionManager().getTransaction(transactionId.get());
      String command = "UpdateManager:ComObject:updateRecord:";
      trans.addOperation(TransactionManager.OperationType.UPDATE, command, cobj.serialize(), replayedCommand);
    }
    return ret;
  }

  public ComObject doUpdateRecord(ComObject cobj, boolean replayedCommand,
                                  AtomicBoolean isExplicitTransRet, AtomicLong transactionIdRet, boolean isCommitting) {
    try {
      String dbName = cobj.getString(ComObject.Tag.DB_NAME);
      Integer schemaVersion = cobj.getInt(ComObject.Tag.SCHEMA_VERSION);
      if (!replayedCommand && schemaVersion != null && schemaVersion < server.getSchemaVersion()) {
        throw new SchemaOutOfSyncException(CURR_VER_STR + server.getCommon().getSchemaVersion() + ":");
      }
      String tableName = cobj.getString(ComObject.Tag.TABLE_NAME);
      String indexName = cobj.getString(ComObject.Tag.INDEX_NAME);
      boolean isExplicitTrans = cobj.getBoolean(ComObject.Tag.IS_EXCPLICITE_TRANS);
      long transactionId = cobj.getLong(ComObject.Tag.TRANSACTION_ID);

      if (isExplicitTrans && isExplicitTransRet != null) {
        isExplicitTransRet.set(true);
        transactionIdRet.set(transactionId);
      }

      TableSchema tableSchema = server.getCommon().getTableSchema(dbName, tableName, server.getDataDir());
      byte[] primaryKeyBytes = cobj.getByteArray(ComObject.Tag.PRIMARY_KEY_BYTES);
      Object[] primaryKey = DatabaseCommon.deserializeKey(tableSchema, primaryKeyBytes);
      byte[] bytes = cobj.getByteArray(ComObject.Tag.BYTES);

      AtomicBoolean shouldExecute = new AtomicBoolean();
      AtomicBoolean shouldDeleteLock = new AtomicBoolean();

      server.getTransactionManager().preHandleTransaction(dbName, tableName, indexName, isExplicitTrans, isCommitting, transactionId, primaryKey, shouldExecute, shouldDeleteLock);

      Record record = new Record(dbName, server.getCommon(), bytes);
      if (cobj.getLong(ComObject.Tag.SEQUENCE_0_OVERRIDE) != null) {
        long sequence0 = cobj.getLong(ComObject.Tag.SEQUENCE_0_OVERRIDE);
        long sequence1 = cobj.getLong(ComObject.Tag.SEQUENCE_1_OVERRIDE);
        short sequence2 = cobj.getShort(ComObject.Tag.SEQUENCE_2_OVERRIDE);

        if (sequence0 < record.getSequence0() && sequence1 < record.getSequence1() && sequence2 < record.getSequence2()) {
          throw new DatabaseException(OUT_OF_ORDER_UPDATE_DETECTED_KEY_STR + DatabaseCommon.keyToString(primaryKey));
        }
        record.setSequence0(sequence0);
        record.setSequence1(sequence1);
        record.setSequence2(sequence2);
      }
      else {
        long sequence0 = cobj.getLong(ComObject.Tag.SEQUENCE_0);
        long sequence1 = cobj.getLong(ComObject.Tag.SEQUENCE_1);

        if (sequence0 < record.getSequence0() && sequence1 < record.getSequence1()) {
          throw new DatabaseException(OUT_OF_ORDER_UPDATE_DETECTED_KEY_STR + DatabaseCommon.keyToString(primaryKey));
        }
        record.setSequence0(sequence0);
        record.setSequence1(sequence1);
        record.setSequence2((short) 0);
      }

      bytes = record.serialize(server.getCommon(), DatabaseClient.SERIALIZATION_VERSION);

      if (shouldExecute.get()) {
        //because this is the primary key index we won't have more than one index entry for the key
        Index index = server.getIndex(dbName, tableName, indexName);
        Object newValue = server.getAddressMap().toUnsafeFromRecords(new byte[][]{bytes});
        byte[] existingBytes = null;
        synchronized (index.getMutex(primaryKey)) {
          Object value = index.get(primaryKey);
          if (value != null) {
            byte[][] content = server.getAddressMap().fromUnsafeToRecords(value);
            existingBytes = content[0];
            if ((Record.getDbViewFlags(content[0]) & Record.DB_VIEW_FLAG_DELETING) != 0) {
              if ((Record.getDbViewFlags(bytes) & Record.DB_VIEW_FLAG_DELETING) == 0) {
                index.addAndGetCount(1);
              }
            }
            else {
              if ((Record.getDbViewFlags(bytes) & Record.DB_VIEW_FLAG_DELETING) != 0) {
                index.addAndGetCount(-1);
              }
            }
          }
          index.put(primaryKey, newValue);
          if (value != null) {
            server.getAddressMap().freeUnsafeIds(value);
          }
        }
        streamManager.publishInsertOrUpdate(cobj, dbName, tableName, bytes, existingBytes, UpdateType.UPDATE);
      }
      else {
        if (transactionId != 0) {
          TransactionManager.Transaction trans = server.getTransactionManager().getTransaction(transactionId);
          List<Record> records = trans.getRecords().computeIfAbsent(tableName, k -> new ArrayList<>());
          records.add(record);
        }
      }

      if (shouldDeleteLock.get()) {
        server.getTransactionManager().deleteLock(dbName, tableName, transactionId, tableSchema, primaryKey);
      }
      return null;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  private void doInsertKey(
      ComObject cobj, String dbName, byte[] recordBytes, Object[] key, Index index, String tableName, String indexName, boolean ignoreDuplicates) {
    doActualInsertKeyWithRecord(cobj, dbName, recordBytes, key, index, tableName, indexName, ignoreDuplicates, false);
  }

  private void doInsertKey(Object[] key, byte[] keyRecordBytes, String tableName, Index index, IndexSchema indexSchema) {
    doActualInsertKey(key, keyRecordBytes, tableName, index, indexSchema);
  }

  public void doInsertKeys(final ComObject cobj, final String dbName, List<PartitionManager.MoveRequest> moveRequests, final Index index,
                           final String tableName, final IndexSchema indexSchema, boolean replayedCommand,
                           final boolean movingRecord) {
    try {
      if (indexSchema.isPrimaryKey()) {
        if (replayedCommand) {
          List<Future> futures = new ArrayList<>();
          for (final PartitionManager.MoveRequest moveRequest : moveRequests) {
            futures.add(server.getExecutor().submit(new Callable() {
              @Override
              public Object call() throws Exception {
                byte[][] content = moveRequest.getContent();
                for (int i = 0; i < content.length; i++) {
                  doActualInsertKeyWithRecord(cobj, dbName, content[i], moveRequest.getKey(), index, tableName,
                      indexSchema.getName(), true, movingRecord);
                }
                return null;
              }
            }));
          }
          for (Future future : futures) {
            future.get();
          }
        }
        else {
          for (PartitionManager.MoveRequest moveRequest : moveRequests) {
            byte[][] content = moveRequest.getContent();
            for (int i = 0; i < content.length; i++) {
              doActualInsertKeyWithRecord(cobj, dbName, content[i], moveRequest.getKey(), index, tableName,
                  indexSchema.getName(), true, movingRecord);
            }
          }
        }
      }
      else {
        if (replayedCommand) {
          List<Future> futures = new ArrayList<>();
          for (final PartitionManager.MoveRequest moveRequest : moveRequests) {
            futures.add(server.getExecutor().submit(new Callable(){
              @Override
              public Object call() throws Exception {
                byte[][] content = moveRequest.getContent();
                for (int i = 0; i < content.length; i++) {
                  doActualInsertKey(moveRequest.getKey(), content[i], tableName, index, indexSchema);
                }
                return null;
              }
            }));
          }
          for (Future future : futures) {
            future.get();
          }
        }
        else {
          for (PartitionManager.MoveRequest moveRequest : moveRequests) {
            byte[][] content = moveRequest.getContent();
            for (int i = 0; i < content.length; i++) {
              doActualInsertKey(moveRequest.getKey(), content[i], tableName, index, indexSchema);
            }
          }
        }
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  /**
   * Caller must synchronized index
   */
  private void doActualInsertKey(Object[] key, byte[] keyRecordBytes, String tableName, Index index, IndexSchema indexSchema) {
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
        byte[][] records = server.getAddressMap().fromUnsafeToRecords(existingValue);
        boolean replaced = false;
        for (int i = 0; i < records.length; i++) {
          if (Arrays.equals(KeyRecord.getPrimaryKey(records[i]), KeyRecord.getPrimaryKey(keyRecordBytes))) {
            replaced = true;
            if (KeyRecord.getDbViewFlags(records[i]) == Record.DB_VIEW_FLAG_DELETING &&
                KeyRecord.getDbViewFlags(keyRecordBytes) != Record.DB_VIEW_FLAG_DELETING) {
              index.addAndGetCount(1);
            }
            else if (KeyRecord.getDbViewFlags(records[i]) != Record.DB_VIEW_FLAG_DELETING &&
                KeyRecord.getDbViewFlags(keyRecordBytes) == Record.DB_VIEW_FLAG_DELETING) {
              index.addAndGetCount(-1);
            }
            records[i] = keyRecordBytes;
            break;
          }
        }

        if (indexSchema.isUnique()) {
          throw new UniqueConstraintViolationException("Unique constraint violated: table=" + tableName + ", index=" + indexSchema.getName() + ", key=" + DatabaseCommon.keyToString(key));
        }
        if (replaced) {
          Object address = server.getAddressMap().toUnsafeFromRecords(records);
          index.put(key, address);
          server.getAddressMap().freeUnsafeIds(existingValue);
        }
        else {
          byte[][] newRecords = new byte[records.length + 1][];
          System.arraycopy(records, 0, newRecords, 0, records.length);
          newRecords[newRecords.length - 1] = keyRecordBytes;
          Object address = server.getAddressMap().toUnsafeFromRecords(newRecords);
          index.put(key, address);
          index.addAndGetCount(1);
          server.getAddressMap().freeUnsafeIds(existingValue);
        }
      }
      if (existingValue == null) {
        index.put(key, server.getAddressMap().toUnsafeFromKeys(new byte[][]{keyRecordBytes}));
      }
    }
  }

  /**
   * Caller must synchronized index
   */
  private void doActualInsertKeyWithRecord(ComObject cobj, String dbName, byte[] recordBytes, Object[] key, Index index,
                                           String tableName, String indexName, boolean ignoreDuplicates, boolean movingRecord) {
    if (recordBytes == null) {
      throw new DatabaseException("Invalid record, null");
    }

    try {
      Object newUnsafeRecords = server.getAddressMap().toUnsafeFromRecords(new byte[][]{recordBytes});
      synchronized (index.getMutex(key)) {
        Object existingValue = index.put(key, newUnsafeRecords);
        if (existingValue == null) {
          index.addAndGetCount(1);
        }
        else {
          boolean sameTrans = false;
          byte[][] bytes = server.getAddressMap().fromUnsafeToRecords(existingValue);
          long transId = Record.getTransId(recordBytes);
          boolean sameSequence = false;
          for (byte[] innerBytes : bytes) {
            if (Record.getTransId(innerBytes) == transId) {
              sameTrans = true;
              break;
            }
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(innerBytes));
            in.readShort(); //serializationVersion
            long sequence0 = in.readLong();
            long sequence1 = in.readLong();
            in = new DataInputStream(new ByteArrayInputStream(recordBytes));
            in.readShort(); //serializationVersion
            long rsequence0 = in.readLong();
            long rsequence1 = in.readLong();
            if (sequence0 == rsequence0 && sequence1 == rsequence1) {
              sameSequence = true;
              break;
            }
          }
          if (!ignoreDuplicates && existingValue != null && !sameTrans && !sameSequence) {
            index.put(key, existingValue);
            server.getAddressMap().freeUnsafeIds(newUnsafeRecords);
            throw new UniqueConstraintViolationException("Unique constraint violated: table=" + tableName + ", index=" + indexName + ", key=" + DatabaseCommon.keyToString(key));
          }
          if ((Record.getDbViewFlags(bytes[0]) & Record.DB_VIEW_FLAG_DELETING) != 0) {
            if ((Record.getDbViewFlags(recordBytes) & Record.DB_VIEW_FLAG_DELETING) == 0) {
              index.addAndGetCount(1);
            }
          }
          else if ((Record.getDbViewFlags(recordBytes) & Record.DB_VIEW_FLAG_DELETING) != 0) {
            index.addAndGetCount(-1);
          }
          server.getAddressMap().freeUnsafeIds(existingValue);
        }
      }
      if (!movingRecord) {
        if (threadLocalIsBatchRequest.get() != null && threadLocalIsBatchRequest.get()) {
          if (!dbName.equals("_sonicbase_sys") && !producers.isEmpty()) {
            streamManager.addToBatch(dbName, tableName, recordBytes, UpdateType.INSERT);
          }
        }
        else if (Record.DB_VIEW_FLAG_DELETING != Record.getDbViewFlags(recordBytes)) {
          streamManager.publishInsertOrUpdate(cobj, dbName, tableName, recordBytes, null, UpdateType.INSERT);
        }
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public enum UpdateType {
    INSERT,
    UPDATE,
    DELETE
  }


  @SchemaReadLock
  public ComObject deleteRecord(ComObject cobj, boolean replayedCommand) {
    try {
      AtomicBoolean isExplicitTrans = new AtomicBoolean();
      AtomicLong transactionId = new AtomicLong();
      final long sequence0 = cobj.getLong(ComObject.Tag.SEQUENCE_0);
      final long sequence1 = cobj.getLong(ComObject.Tag.SEQUENCE_1);

      doDeleteRecord(cobj, replayedCommand, sequence0, sequence1, isExplicitTrans, transactionId, false);

      if (isExplicitTrans.get()) {
        TransactionManager.Transaction trans = server.getTransactionManager().getTransaction(transactionId.get());
        String command = "UpdateManager:ComObject:deleteIndexEntryByKey:";
        trans.addOperation(DELETE_RECORD, command, cobj.serialize(), replayedCommand);
      }

      return null;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private void doDeleteRecord(ComObject cobj, boolean replayedCommand, long sequence0, long sequence1, AtomicBoolean isExplicitTransRet,
                              AtomicLong transactionIdRet, boolean isCommitting) throws EOFException {
    String dbName = cobj.getString(ComObject.Tag.DB_NAME);
    String tableName = cobj.getString(ComObject.Tag.TABLE_NAME);
    String indexName = cobj.getString(ComObject.Tag.INDEX_NAME);
    Integer schemaVersion = cobj.getInt(ComObject.Tag.SCHEMA_VERSION);
    if (!replayedCommand && schemaVersion != null && schemaVersion < server.getSchemaVersion()) {
      throw new SchemaOutOfSyncException(CURR_VER_STR + server.getCommon().getSchemaVersion() + ":");
    }

    boolean isExplicitTrans = cobj.getBoolean(ComObject.Tag.IS_EXCPLICITE_TRANS);
    long transactionId = cobj.getLong(ComObject.Tag.TRANSACTION_ID);
    if (isExplicitTrans && isExplicitTransRet != null) {
      isExplicitTransRet.set(true);
      transactionIdRet.set(transactionId);
    }

    AtomicBoolean shouldExecute = new AtomicBoolean();
    AtomicBoolean shouldDeleteLock = new AtomicBoolean();

    TableSchema tableSchema = server.getCommon().getTableSchema(dbName, tableName, server.getDataDir());
    byte[] keyBytes = cobj.getByteArray(ComObject.Tag.KEY_BYTES);
    Object[] key = DatabaseCommon.deserializeKey(tableSchema, keyBytes);

    server.getTransactionManager().preHandleTransaction(dbName, tableName, indexName, isExplicitTrans,
        isCommitting, transactionId, key, shouldExecute, shouldDeleteLock);

    if (shouldExecute.get()) {

      byte[][] bytes = null;
      Index index = server.getIndex(dbName, tableName, indexName);
      synchronized (index.getMutex(key)) {
        Object value = index.remove(key);
        if (value != null) {
          bytes = server.getAddressMap().fromUnsafeToRecords(value);
          server.getAddressMap().freeUnsafeIds(value);
          index.addAndGetCount(-1);
        }
      }

      if (bytes != null) {
        for (byte[] currBytes : bytes) {
          Record record = new Record(dbName, server.getCommon(), currBytes);
          if (cobj.getLong(ComObject.Tag.SEQUENCE_0_OVERRIDE) != null) {
            sequence0 = cobj.getLong(ComObject.Tag.SEQUENCE_0_OVERRIDE);
            sequence1 = cobj.getLong(ComObject.Tag.SEQUENCE_1_OVERRIDE);
            short sequence2 = cobj.getShort(ComObject.Tag.SEQUENCE_2_OVERRIDE);

            if (sequence0 < record.getSequence0() && sequence1 < record.getSequence1() && sequence2 < record.getSequence2()) {
              throw new DatabaseException(OUT_OF_ORDER_UPDATE_DETECTED_KEY_STR + DatabaseCommon.keyToString(key));
            }
          }
          else {
            if (sequence0 < record.getSequence0() && sequence1 < record.getSequence1()) {
              throw new DatabaseException(OUT_OF_ORDER_UPDATE_DETECTED_KEY_STR + DatabaseCommon.keyToString(key));
            }
          }
        }

        if (tableSchema.getIndices().get(indexName).isPrimaryKey()) {
          for (byte[] innerBytes : bytes) {
            streamManager.publishInsertOrUpdate(cobj, dbName, tableName, innerBytes, null, UpdateType.DELETE);
          }
        }
      }
    }

    if (shouldDeleteLock.get()) {
      server.getTransactionManager().deleteLock(dbName, tableName, transactionId, tableSchema, key);
    }
  }

  @SchemaReadLock
  public ComObject truncateTable(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.DB_NAME);
    Integer schemaVersion = cobj.getInt(ComObject.Tag.SCHEMA_VERSION);
    if (!replayedCommand && schemaVersion != null && schemaVersion < server.getSchemaVersion()) {
      throw new SchemaOutOfSyncException(CURR_VER_STR + server.getCommon().getSchemaVersion() + ":");
    }
    String table = cobj.getString(ComObject.Tag.TABLE_NAME);
    String phase = cobj.getString(ComObject.Tag.PHASE);
    TableSchema tableSchema = server.getCommon().getTableSchema(dbName, table, server.getDataDir());
    if (tableSchema != null) {
      for (Map.Entry<String, IndexSchema> entry : tableSchema.getIndices().entrySet()) {
        Index index = server.getIndex(dbName, table, entry.getKey());
        if (entry.getValue().isPrimaryKey()) {
          if (phase.equals("primary")) {
            Map.Entry<Object[], Object> indexEntry = index.firstEntry();
            do {
              if (indexEntry == null) {
                break;
              }
              synchronized (indexEntry.getKey()) {
                Object value = index.remove(indexEntry.getKey());
                if (value != null) {
                  server.getAddressMap().freeUnsafeIds(value);
                }
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
              if (value != null) {
                server.getAddressMap().freeUnsafeIds(value);
              }
            }
            indexEntry = index.higherEntry(indexEntry.getKey());
          }
          while (true);
        }
        index.setCount(0);
      }
    }

    return null;
  }

  private void doRemoveIndexEntryByKey(
      String dbName, TableSchema tableSchema, String primaryKeyIndexName, Object[] primaryKey, String indexName,
      Object[] key, long sequence0, long sequence1) {

    Comparator[] comparators = server.getIndex(dbName, tableSchema.getName(), primaryKeyIndexName).getComparators();

    Index index = server.getIndex(dbName, tableSchema.getName(), indexName);
    synchronized (index.getMutex(key)) {
      Object value = index.get(key);
      if (value != null) {
        byte[][] ids = server.getAddressMap().fromUnsafeToKeys(value);
        if (ids.length == 1) {
          boolean mismatch = false;
          if (!indexName.equals(primaryKeyIndexName)) {
            try {
              KeyRecord keyRecord = new KeyRecord(ids[0]);
              Object[] lhsKey = DatabaseCommon.deserializeKey(tableSchema, keyRecord.getPrimaryKey());
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
            if (indexName.equals(primaryKeyIndexName)) {
              if (Record.DB_VIEW_FLAG_DELETING != Record.getDbViewFlags(ids[0])) {
                index.addAndGetCount(-1);
              }
            }
            else {
              if (Record.DB_VIEW_FLAG_DELETING != KeyRecord.getDbViewFlags(ids[0])) {
                index.addAndGetCount(-1);
              }
            }
            value = index.remove(key);
            if (value != null) {
              server.getAddressMap().freeUnsafeIds(value);
            }
          }
        }
        else {
          byte[][] newValues = new byte[ids.length - 1][];
          int offset = 0;
          boolean found = false;
          byte[] foundBytes = null;
          for (byte[] currValue : ids) {
            boolean mismatch = false;
            KeyRecord keyRecord = new KeyRecord(currValue);
            try {
              Object[] lhsKey = DatabaseCommon.deserializeKey(tableSchema, keyRecord.getPrimaryKey());
              for (int i = 0; i < lhsKey.length; i++) {
                if (0 != comparators[i].compare(lhsKey[i], primaryKey[i])) {
                  mismatch = true;
                }
              }
            }
            catch (Exception e) {
              throw new DatabaseException(e);
            }

            if (mismatch) {
              newValues[offset++] = currValue;
            }
            else {
              found = true;
              foundBytes = currValue;
            }
          }
          if (found) {
            if (indexName.equals(primaryKeyIndexName)) {
              if (Record.DB_VIEW_FLAG_DELETING != Record.getDbViewFlags(foundBytes)) {
                index.addAndGetCount(-1);
              }
            }
            else {
              if (Record.DB_VIEW_FLAG_DELETING != KeyRecord.getDbViewFlags(foundBytes)) {
                index.addAndGetCount(-1);
              }
            }
            Object newValue = server.getAddressMap().toUnsafeFromKeys(newValues);
            index.put(key, newValue);
            if (value != null) {
              server.getAddressMap().freeUnsafeIds(value);
            }
          }
        }
      }
    }
  }

  @SchemaReadLock
  public ComObject insertWithSelect(ComObject cobj, boolean replayedCommand) {

    try {
      String dbName = cobj.getString(ComObject.Tag.DB_NAME);
      byte[] selectBytes = cobj.getByteArray(ComObject.Tag.SELECT);
      SelectStatementImpl selectStatement = new SelectStatementImpl(server.getClient());
      selectStatement.deserialize(selectBytes);
      selectStatement.setIsOnServer(true);
      selectStatement.setPageSize(30_000);

      String tableName = cobj.getString(ComObject.Tag.TABLE_NAME);
      boolean ignore = cobj.getBoolean(ComObject.Tag.IGNORE);
      ComArray columnsArray = cobj.getArray(ComObject.Tag.COLUMNS);

      String fromTable = selectStatement.getFromTable();
      List<ColumnImpl> srcColumns = selectStatement.getSelectColumns();

      TableSchema fromTableSchema = server.getCommon().getTableSchema(dbName, fromTable, server.getDataDir());
      DataType.Type[] columnTypes = new DataType.Type[srcColumns.size()];
      for (int i = 0; i < columnTypes.length; i++) {
        ColumnImpl srcColumn = srcColumns.get(i);
        DataType.Type type = fromTableSchema.getFields().get(fromTableSchema.getFieldOffset(srcColumn.getColumnName())).getType();
        columnTypes[i] = type;
      }

      ObjectNode dict = server.getConfig();
      ObjectNode databaseDict = dict;
      ArrayNode array = databaseDict.withArray("shards");
      ObjectNode replicaDict = (ObjectNode) array.get(0);
      ArrayNode replicasArray = replicaDict.withArray("replicas");
      final String address = databaseDict.get("clientIsPrivate") != null && databaseDict.get("clientIsPrivate").asBoolean() ?
          replicasArray.get(0).get("privateAddress").asText() :
          replicasArray.get(0).get("publicAddress").asText();
      final int port = replicasArray.get(0).get("port").asInt();

      Class.forName("com.sonicbase.jdbcdriver.Driver");

      try (final Connection conn = getSonicBaseConnection(dbName, address, port)) {
        StringBuilder destColumnsStr = new StringBuilder();
        StringBuilder destParmsStr = new StringBuilder();
        for (int i = 0; i < columnsArray.getArray().size(); i++) {
          if (i != 0) {
            destColumnsStr.append(",");
            destParmsStr.append(",");
          }
          destColumnsStr.append((String) columnsArray.getArray().get(i));
          destParmsStr.append("?");
        }

        ResultSet rs = (ResultSet) selectStatement.execute(dbName, null, null, null, null,
            null, false, null, 0);
        String sql = "insert " + (ignore ? "ignore" : "") + " into " + tableName + " (" + destColumnsStr + ") values (" + destParmsStr + ")";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
          int totalCountInserted = 0;
          while (true) {
            int batchSize = 0;
            for (int j = 0; j < 100 && rs.next(); j++) {
              for (int i = 0; i < columnsArray.getArray().size(); i++) {
                ColumnImpl srcColumn = srcColumns.get(i);
                String alias = srcColumn.getAlias();
                String columnName = srcColumn.getColumnName();
                if (alias != null && !"__alias__".equals(alias)) {
                  columnName = alias;
                }
                switch (columnTypes[i]) {
                  case BIGINT:
                    stmt.setLong(i + 1, rs.getLong(columnName));
                    break;
                  case INTEGER:
                    stmt.setInt(i + 1, rs.getInt(columnName));
                    break;
                  case BIT:
                    stmt.setBoolean(i + 1, rs.getBoolean(columnName));
                    break;
                  case TINYINT:
                    stmt.setByte(i + 1, rs.getByte(columnName));
                    break;
                  case SMALLINT:
                    stmt.setShort(i + 1, rs.getShort(columnName));
                    break;
                  case FLOAT:
                    stmt.setDouble(i + 1, rs.getDouble(columnName));
                    break;
                  case REAL:
                    stmt.setFloat(i + 1, rs.getFloat(columnName));
                    break;
                  case DOUBLE:
                    stmt.setDouble(i + 1, rs.getDouble(columnName));
                    break;
                  case NUMERIC:
                  case DECIMAL:
                    stmt.setBigDecimal(i + 1, rs.getBigDecimal(columnName));
                    break;
                  case CHAR:
                  case VARCHAR:
                  case CLOB:
                  case NCLOB:
                  case NCHAR:
                  case NVARCHAR:
                  case LONGNVARCHAR:
                  case LONGVARCHAR:
                    stmt.setString(1 + 1, rs.getString(columnName));
                    break;
                  case DATE:
                    stmt.setDate(i + 1, rs.getDate(columnName));
                    break;
                  case TIME:
                    stmt.setTime(i + 1, rs.getTime(columnName));
                    break;
                  case TIMESTAMP:
                    stmt.setTimestamp(i + 1, rs.getTimestamp(columnName));
                    break;
                  case BINARY:
                  case VARBINARY:
                  case LONGVARBINARY:
                  case BLOB:
                    stmt.setBytes(i + 1, rs.getBytes(columnName));
                    break;
                  case BOOLEAN:
                    stmt.setBoolean(i + 1, rs.getBoolean(columnName));
                    break;
                  case ROWID:
                    stmt.setLong(i + 1, rs.getLong(columnName));
                    break;
                  default:
                    throw new DatabaseException("Data type not supported: " + columnTypes[i].name());
                }
              }
              stmt.addBatch();
              batchSize++;
              totalCountInserted++;
            }
            if (batchSize == 0) {
              break;
            }
            stmt.executeBatch();
          }
          ComObject retObj = new ComObject();
          retObj.put(ComObject.Tag.COUNT, totalCountInserted);
          return retObj;
        }
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  protected Connection getSonicBaseConnection(String dbName, String address, int port) throws SQLException {
    return DriverManager.getConnection("jdbc:sonicbase:" + address + ":" + port + "/" + dbName);
  }
}
