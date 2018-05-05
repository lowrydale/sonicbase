package com.sonicbase.server;

import com.amazonaws.transform.MapEntry;
import com.codahale.metrics.MetricRegistry;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.*;
import com.sonicbase.index.Index;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.procedure.RecordImpl;
import com.sonicbase.procedure.StoredProcedureContextImpl;
import com.sonicbase.query.BinaryExpression;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.query.Expression;
import com.sonicbase.query.impl.*;
import com.sonicbase.schema.DataType;
import com.sonicbase.schema.FieldSchema;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.Offset;
import org.apache.giraph.utils.Varint;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.sonicbase.client.DatabaseClient.OPTIMIZED_RANGE_PAGE_SIZE;

/**
 * Responsible for
 */
public class ReadManager {

  private Logger logger;

  private final DatabaseServer server;
  private Thread diskReaper;
  private boolean shutdown;

  public ReadManager(DatabaseServer databaseServer) {

    this.server = databaseServer;
    this.logger = new Logger(null/*databaseServer.getDatabaseClient()*/);

    startDiskResultsReaper();
  }

  private void startDiskResultsReaper() {
    diskReaper = ThreadUtil.createThread(new Runnable(){
      @Override
      public void run() {
        while (!shutdown) {
          try {
            DiskBasedResultSet.deleteOldResultSets(server);
          }
          catch (Exception e) {
            logger.error("Error in disk results reaper thread", e);
          }
          try {
            Thread.sleep(100 * 1000);
          }
          catch (InterruptedException e) {
            break;
          }
        }
      }
    }, "SonicBase Disk Results Reaper Thread");
    diskReaper.start();
  }


  public ComObject countRecords(ComObject cobj) {
    if (server.getBatchRepartCount().get() != 0 && lookupCount.incrementAndGet() % 1000 == 0) {
      try {
        Thread.sleep(10);
      }
      catch (InterruptedException e) {
        throw new DatabaseException(e);
      }
    }

    String dbName = cobj.getString(ComObject.Tag.dbName);
    Integer schemaVersion = cobj.getInt(ComObject.Tag.schemaVersion);
    if (schemaVersion != null && schemaVersion < server.getSchemaVersion()) {
      throw new SchemaOutOfSyncException("currVer:" + server.getCommon().getSchemaVersion() + ":");
    }
    String fromTable = cobj.getString(ComObject.Tag.tableName);

    byte[] expressionBytes = cobj.getByteArray(ComObject.Tag.legacyExpression);
    Expression expression = null;
    if (expressionBytes != null) {
      expression = ExpressionImpl.deserializeExpression(expressionBytes);
    }
    byte[] parmsBytes = cobj.getByteArray(ComObject.Tag.parms);
    ParameterHandler parms = null;
    if (parmsBytes != null) {
      parms = new ParameterHandler();
      parms.deserialize(parmsBytes);
    }
    String countColumn = cobj.getString(ComObject.Tag.countColumn);

    long count = 0;
    String primaryKeyIndex = null;
    for (Map.Entry<String, IndexSchema> entry : server.getCommon().getTableSchema(dbName, fromTable, server.getDataDir()).getIndexes().entrySet()) {
      if (entry.getValue().isPrimaryKey()) {
        primaryKeyIndex = entry.getValue().getName();
        break;
      }
    }
    TableSchema tableSchema = server.getCommon().getTableSchema(dbName, fromTable, server.getDataDir());
    Index index = server.getIndex(dbName, fromTable, primaryKeyIndex);

    int countColumnOffset = 0;
    if (countColumn != null) {
      for (int i = 0; i < tableSchema.getFields().size(); i++) {
        FieldSchema field = tableSchema.getFields().get(i);
        if (field.getName().equals(countColumn)) {
          countColumnOffset = i;
          break;
        }
      }
    }

    if (countColumn == null && expression == null) {
      count = index.getCount();
    }
    else {
      Map.Entry<Object[], Object> entry = index.firstEntry();
      while (true) {
        if (entry == null) {
          break;
        }
        byte[][] records = null;
        //synchronized (index.getMutex(entry.getKey())) {
          //if (entry.getValue() instanceof Long) {
          //TODO: unsafe
            //entry.setValue(index.get(entry.getKey()));
          //}
          if (entry.getValue() != null && !entry.getValue().equals(0L)) {
            records = server.fromUnsafeToRecords(entry.getValue());
          }
        //}
        for (byte[] bytes : records) {
          if ((Record.getDbViewFlags(bytes) & Record.DB_VIEW_FLAG_DELETING) != 0) {
            continue;
          }
          Record record = new Record(tableSchema);
          record.deserialize(dbName, server.getCommon(), bytes, null, true);
          boolean pass = true;
          if (countColumn != null) {
            if (record.getFields()[countColumnOffset] == null) {
              pass = false;
            }
          }
          if (pass) {
            if (expression == null) {
              count++;
            }
            else {
              pass = (Boolean) ((ExpressionImpl) expression).evaluateSingleRecord(new TableSchema[]{tableSchema}, new Record[]{record}, parms);
              if (pass) {
                count++;
              }
            }
          }
        }
        entry = index.higherEntry(entry.getKey());
      }
    }


    ComObject retObj = new ComObject();
    retObj.put(ComObject.Tag.countLong, count);
    return retObj;
  }

  public ComObject batchIndexLookup(ComObject cobj) {
    try {
      if (server.getBatchRepartCount().get() != 0 && lookupCount.incrementAndGet() % 1000 == 0) {
        try {
          Thread.sleep(10);
        }
        catch (InterruptedException e) {
          throw new DatabaseException(e);
        }
      }

      String dbName = cobj.getString(ComObject.Tag.dbName);
      Integer schemaVersion = cobj.getInt(ComObject.Tag.schemaVersion);
      if (schemaVersion != null && schemaVersion < server.getSchemaVersion()) {
        throw new SchemaOutOfSyncException("currVer:" + server.getCommon().getSchemaVersion() + ":");
      }
      int count = cobj.getInt(ComObject.Tag.count);
      short serializationVersion = cobj.getShort(ComObject.Tag.serializationVersion);
      String tableName = cobj.getString(ComObject.Tag.tableName);
      String indexName = cobj.getString(ComObject.Tag.indexName);

      TableSchema tableSchema = server.getCommon().getTableSchema(dbName, tableName, server.getDataDir());
      IndexSchema indexSchema = server.getIndexSchema(dbName, tableSchema.getName(), indexName);
      AtomicInteger AtomicInteger = new AtomicInteger();

      Index index = server.getIndex(dbName, tableSchema.getName(), indexName);
      Boolean ascending = null;

      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.serializationVersion, DatabaseClient.SERIALIZATION_VERSION);

      int leftOperatorId = cobj.getInt(ComObject.Tag.leftOperator);
      BinaryExpression.Operator leftOperator = BinaryExpression.Operator.getOperator(leftOperatorId);

      ComArray cOffsets = cobj.getArray(ComObject.Tag.columnOffsets);
      Set<Integer> columnOffsets = new HashSet<>();
      for (Object obj : cOffsets.getArray()) {
        columnOffsets.add((Integer)obj);
      }

      boolean singleValue = cobj.getBoolean(ComObject.Tag.singleValue);

      IndexSchema primaryKeyIndexSchema = null;
      Index primaryKeyIndex = null;
      for (Map.Entry<String, IndexSchema> entry : tableSchema.getIndices().entrySet()) {
        if (entry.getValue().isPrimaryKey()) {
          primaryKeyIndexSchema = entry.getValue();
          primaryKeyIndex = server.getIndex(dbName, tableSchema.getName(), entry.getKey());
        }
      }

      //out.writeInt(SERIALIZATION_VERSION);

      ComArray keys = cobj.getArray(ComObject.Tag.keys);
      ComArray retKeysArray = retObj.putArray(ComObject.Tag.retKeys, ComObject.Type.objectType);
      for (Object keyObj : keys.getArray()) {
        ComObject key = (ComObject)keyObj;
        int offset = key.getInt(ComObject.Tag.offset);
        Object[] leftKey = null;
        if (singleValue) {
          leftKey = new Object[]{key.getLong(ComObject.Tag.longKey)};
        }
        else {
          byte[] keyBytes = key.getByteArray(ComObject.Tag.keyBytes);
          leftKey = DatabaseCommon.deserializeKey(tableSchema, keyBytes);
        }

        Counter[] counters = null;
        GroupByContext groupContext = null;

        List<byte[]> retKeyRecords = new ArrayList<>();
        List<Object[]> retKeys = new ArrayList<>();
        List<byte[]> retRecords = new ArrayList<>();

        boolean keyContainsColumns = false;
        int[] keyOffsets = null;

        boolean forceSelectOnServer = false;
        if (indexSchema.isPrimaryKey()) {
          doIndexLookupOneKey(cobj.getShort(ComObject.Tag.serializationVersion), dbName, count, tableSchema, indexSchema, null, false, null,
              columnOffsets, forceSelectOnServer, null, leftKey, leftKey, leftOperator, index, ascending,
              retKeyRecords, retKeys, retRecords, server.getCommon().getSchemaVersion(), false, counters, groupContext,
              new AtomicLong(), new AtomicLong(), null, null, keyOffsets, keyContainsColumns, false, null);
        }
        else {
          doIndexLookupOneKey(cobj.getShort(ComObject.Tag.serializationVersion), dbName, count, tableSchema, indexSchema, null, false,
              null, columnOffsets, forceSelectOnServer, null, leftKey, leftKey, leftOperator,
              index, ascending, retKeyRecords, retKeys, retRecords, server.getCommon().getSchemaVersion(), true, counters,
              groupContext, new AtomicLong(), new AtomicLong(), null, null, keyOffsets, keyContainsColumns, false, null);

//          if (indexSchema.isPrimaryKeyGroup()) {
//            if (((Long)leftKey[0]) == 5) {
//              System.out.println("Keys size: " + retKeys.size());
//            }
//            for (byte[] keyBytes : retKeys) {
//              Object[] key = DatabaseCommon.deserializeKey(tableSchema, new DataInputStream(new ByteArrayInputStream(keyBytes)));
//              doIndexLookupOneKey(count, tableSchema, primaryKeyIndexSchema, null, false, null, columnOffsets, null, key, BinaryExpression.Operator.equal, primaryKeyIndex, ascending, retRecords, server.getCommon().getSchemaVersion(), false);
//            }
//          }
//          retKeys.clear();
        }

        ComObject retEntry = new ComObject();
        retKeysArray.add(retEntry);
        retEntry.put(ComObject.Tag.offset, offset);
        retEntry.put(ComObject.Tag.keyCount, retKeyRecords.size());

        ComArray keysArray = retEntry.putArray(ComObject.Tag.keyRecords, ComObject.Type.byteArrayType);
        for (byte[] currKey : retKeyRecords) {
          keysArray.add(currKey);
        }
        keysArray = retEntry.putArray(ComObject.Tag.keys, ComObject.Type.byteArrayType);
        for (Object[] currKey : retKeys) {
          keysArray.add(DatabaseCommon.serializeKey(tableSchema, indexName, currKey));
        }
        ComArray retRecordsArray = retEntry.putArray(ComObject.Tag.records, ComObject.Type.byteArrayType);
        for (int j = 0; j < retRecords.size(); j++) {
          byte[] bytes = retRecords.get(j);
          retRecordsArray.add(bytes);
        }
      }
//
//      if (schemaVersion < server.getSchemaVersion()) {
//        throw new SchemaOutOfSyncException("currVer:" + server.getCommon().getSchemaVersion() + ":");
//      }

      return retObj;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private static final MetricRegistry METRICS = new MetricRegistry();

  public static final com.codahale.metrics.Timer INDEX_LOOKUP_STATS = METRICS.timer("indexLookup");
  public static final com.codahale.metrics.Timer BATCH_INDEX_LOOKUP_STATS = METRICS.timer("batchIndexLookup");

  public void shutdown() {
    try {
      shutdown = true;
      if (diskReaper != null) {
        diskReaper.interrupt();
        diskReaper.join();
      }
    }
    catch (InterruptedException e) {
      throw new DatabaseException(e);
    }
  }

  private AtomicInteger lookupCount = new AtomicInteger();

  public ComObject indexLookup(ComObject cobj) {
    return indexLookup(cobj, null);
  }

  public ComObject indexLookup(ComObject cobj, StoredProcedureContextImpl procedureContext) {
    //Timer.Context context = INDEX_LOOKUP_STATS.time();
    try {

      if (server.getBatchRepartCount().get() != 0 && lookupCount.incrementAndGet() % 1000 == 0) {
        try {
          Thread.sleep(10);
        }
        catch (InterruptedException e) {
          throw new DatabaseException(e);
        }
      }

      Integer schemaVersion = cobj.getInt(ComObject.Tag.schemaVersion);
      if (schemaVersion != null && schemaVersion < server.getSchemaVersion()) {
        throw new SchemaOutOfSyncException("currVer:" + server.getCommon().getSchemaVersion() + ":");
      }
//      else if (server.getSchemaVersion() != server.getDatabaseClient().getCommon().getSchemaVersion()) {
//        throw new DatabaseException("Schema version mismatch: server=" + server.getSchemaVersion() +
//            ", client=" + server.getDatabaseClient().getCommon().getSchemaVersion());
//      }
      else if (schemaVersion != null && schemaVersion > server.getSchemaVersion()) {
        if (server.getShard() != 0 || server.getReplica() != 0) {
          server.getDatabaseClient().syncSchema();
          schemaVersion = server.getSchemaVersion();
        }
        logger.error("Client schema is newer than server schema: client=" + schemaVersion + ", server=" + server.getSchemaVersion());
      }

      short serializationVersion = cobj.getShort(ComObject.Tag.serializationVersion);
      AtomicInteger AtomicInteger = new AtomicInteger();

      int count = cobj.getInt(ComObject.Tag.count);
      boolean isExplicitTrans = cobj.getBoolean(ComObject.Tag.isExcpliciteTrans);
      boolean isCommitting = cobj.getBoolean(ComObject.Tag.isCommitting);
      long transactionId = cobj.getLong(ComObject.Tag.transactionId);
      long viewVersion = cobj.getLong(ComObject.Tag.viewVersion);
      Boolean isProbe = cobj.getBoolean(ComObject.Tag.isProbe);
      if (isProbe == null) {
        isProbe = false;
      }

      int tableId = 0;
      int indexId = 0;
      boolean forceSelectOnServer = false;

      tableId = cobj.getInt(ComObject.Tag.tableId);
      indexId = cobj.getInt(ComObject.Tag.indexId);
      forceSelectOnServer = cobj.getBoolean(ComObject.Tag.forceSelectOnServer);

      ParameterHandler parms = null;
      byte[] parmBytes = cobj.getByteArray(ComObject.Tag.parms);
      if (parmBytes != null) {
        parms = new ParameterHandler();
        parms.deserialize(parmBytes);
      }
      boolean evaluateExpression;
      evaluateExpression = cobj.getBoolean(ComObject.Tag.evaluateExpression);

      Expression expression = null;

      byte[] expressionBytes = cobj.getByteArray(ComObject.Tag.legacyExpression);
      if (expressionBytes != null) {
        expression = ExpressionImpl.deserializeExpression(expressionBytes);
      }
      String dbName = cobj.getString(ComObject.Tag.dbName);
      String tableName = null;
      String indexName = null;
      TableSchema tableSchema = null;
      IndexSchema indexSchema = null;
      try {
        //  logger.info("indexLookup: tableid=" + tableId + ", tableCount=" + common.getTablesById().size() + ", tableNull=" + (common.getTablesById().get(tableId) == null));
        Map<Integer, TableSchema> tablesById = server.getCommon().getTablesById(dbName);
        if (tablesById == null) {
          logger.error("Error");
        }
        tableSchema = tablesById.get(tableId);
        if (tableSchema == null) {
          logger.error("Error");
        }
        tableName = tableSchema.getName();
        indexSchema = tableSchema.getIndexesById().get(indexId);
        indexName = indexSchema.getName();
      }
      catch (Exception e) {
        logger.info("indexLookup: tableName=" + tableName + ", tableid=" + tableId + ", tableByNameCount=" + server.getCommon().getTables(dbName).size() + ", tableCount=" + server.getCommon().getTablesById(dbName).size() +
            ", tableNull=" + (server.getCommon().getTablesById(dbName).get(tableId) == null) + ", indexName=" + indexName + ", indexId=" + indexId +
            ", indexNull=" /*+ (common.getTablesById().get(tableId).getIndexesById().get(indexId) == null) */);
        throw e;
      }
      List<OrderByExpressionImpl> orderByExpressions = null;
      orderByExpressions = new ArrayList<>();
      ComArray oarray = cobj.getArray(ComObject.Tag.orderByExpressions);
      if (oarray != null) {
        for (Object entry : oarray.getArray()) {
          OrderByExpressionImpl orderByExpression = new OrderByExpressionImpl();
          orderByExpression.deserialize((byte[]) entry);
          orderByExpressions.add(orderByExpression);
        }
      }

      byte[] leftBytes = cobj.getByteArray(ComObject.Tag.leftKey);
      Object[] leftKey = null;
      if (leftBytes != null) {
        leftKey = DatabaseCommon.deserializeTypedKey(leftBytes);
      }
      byte[] originalLeftBytes = cobj.getByteArray(ComObject.Tag.originalLeftKey);
      Object[] originalLeftKey = null;
      if (originalLeftBytes != null) {
        originalLeftKey = DatabaseCommon.deserializeTypedKey(originalLeftBytes);
      }
      //BinaryExpression.Operator leftOperator = BinaryExpression.Operator.getOperator(in.readInt());
      BinaryExpression.Operator leftOperator = BinaryExpression.Operator.getOperator(cobj.getInt(ComObject.Tag.leftOperator));

      BinaryExpression.Operator rightOperator = null;
      byte[] rightBytes = cobj.getByteArray(ComObject.Tag.rightKey);
      byte[] originalRightBytes = cobj.getByteArray(ComObject.Tag.originalRightKey);
      Object[] originalRightKey = null;
      Object[] rightKey = null;
      if (rightBytes != null) {
        rightKey = DatabaseCommon.deserializeTypedKey(rightBytes);
      }
      if (originalRightBytes != null) {
        originalRightKey = DatabaseCommon.deserializeTypedKey(originalRightBytes);
      }

      if (cobj.getInt(ComObject.Tag.rightOperator) != null) {
        rightOperator = BinaryExpression.Operator.getOperator(cobj.getInt(ComObject.Tag.rightOperator));
      }

      Set<Integer> columnOffsets = null;
      ComArray cOffsets = cobj.getArray(ComObject.Tag.columnOffsets);
      columnOffsets = new HashSet<>();
      for (Object obj : cOffsets.getArray()) {
        columnOffsets.add((Integer)obj);
      }

      Counter[] counters = null;
      ComArray counterArray = cobj.getArray(ComObject.Tag.counters);
      if (counterArray != null && counterArray.getArray().size() != 0) {
        counters = new Counter[counterArray.getArray().size()];
        for (int i = 0; i < counters.length; i++) {
          counters[i] = new Counter();
          counters[i].deserialize((byte[])counterArray.getArray().get(i));
        }
      }

      byte[] groupContextBytes = cobj.getByteArray(ComObject.Tag.legacyGroupContext);
      GroupByContext groupContext = null;
      if (groupContextBytes != null) {
        groupContext = new GroupByContext();
        groupContext.deserialize(groupContextBytes, server.getCommon(), dbName);
      }

      Long offset = cobj.getLong(ComObject.Tag.offsetLong);
      Long limit = cobj.getLong(ComObject.Tag.limitLong);
      AtomicLong currOffset = new AtomicLong(cobj.getLong(ComObject.Tag.currOffset));
      AtomicLong countReturned = new AtomicLong();
      if (cobj.getLong(ComObject.Tag.countReturned) != null) {
        countReturned.set(cobj.getLong(ComObject.Tag.countReturned));
      }

      Index index = server.getIndex(dbName, tableSchema.getName(), indexName);
      Map.Entry<Object[], Object> entry = null;

      Boolean ascending = null;
      if (orderByExpressions != null && orderByExpressions.size() != 0) {
        OrderByExpressionImpl orderByExpression = orderByExpressions.get(0);
        String columnName = orderByExpression.getColumnName();
        boolean isAscending = orderByExpression.isAscending();
        if (orderByExpression.getTableName() == null || !orderByExpression.getTableName().equals(tableSchema.getName()) ||
            columnName.equals(indexSchema.getFields()[0])) {
          ascending = isAscending;
        }
      }

      List<byte[]> retKeyRecords = new ArrayList<>();
      List<Object[]> retKeys = new ArrayList<>();
      List<byte[]> retRecords = new ArrayList<>();

      List<Object[]> excludeKeys = new ArrayList<>();

      String[] fields = tableSchema.getPrimaryKey();
      int[] keyOffsets = new int[fields.length];
      for (int i = 0; i < keyOffsets.length; i++) {
        keyOffsets[i] = tableSchema.getFieldOffset(fields[i]);
      }

      boolean keyContainsColumns = true;
      if (true || columnOffsets == null || columnOffsets.size() == 0 || counters != null) {
        keyContainsColumns = false;
      }
      else {
        for (Integer columnOffset : columnOffsets) {
          boolean found = false;
          for (int i = 0; i < keyOffsets.length; i++) {
            if (columnOffset == keyOffsets[i]) {
              found = true;
            }
          }
          if (!found) {
            keyContainsColumns = false;
            break;
          }
        }
      }

      if (isExplicitTrans && !isCommitting) {
        TransactionManager.Transaction trans = server.getTransactionManager().getTransaction(transactionId);
        if (trans != null) {
          List<Record> records = trans.getRecords().get(tableName);
          if (records != null) {
            for (Record record : records) {
              boolean pass = (Boolean) ((ExpressionImpl) expression).evaluateSingleRecord(new TableSchema[]{tableSchema}, new Record[]{record}, parms);
              if (pass) {
                Object[] excludeKey = new Object[keyOffsets.length];
                for (int i = 0; i < excludeKey.length; i++) {
                  excludeKey[i] = record.getFields()[keyOffsets[i]];
                }
                excludeKeys.add(excludeKey);
                retRecords.add(record.serialize(server.getCommon(), cobj.getShort(ComObject.Tag.serializationVersion)));
              }
            }
          }
        }
      }

      if (indexSchema.isPrimaryKey()) {
        if (rightOperator == null) {
          boolean keys = false;
          if (keyContainsColumns) {
            keys = true;
          }
          entry = doIndexLookupOneKey(cobj.getShort(ComObject.Tag.serializationVersion), dbName, count, tableSchema, indexSchema, parms, evaluateExpression, expression,
              columnOffsets, forceSelectOnServer, excludeKeys, originalLeftKey, leftKey, leftOperator, index,
              ascending, retKeyRecords, retKeys, retRecords, viewVersion, keys, counters, groupContext, currOffset, countReturned, limit,
              offset, keyOffsets, keyContainsColumns, isProbe, procedureContext);
        }
        else {
          entry = doIndexLookupTwoKeys(cobj.getShort(ComObject.Tag.serializationVersion), dbName, count, tableSchema, indexSchema, forceSelectOnServer, excludeKeys,
              originalLeftKey, leftKey, columnOffsets, originalRightKey, rightKey, leftOperator, rightOperator, parms,
              evaluateExpression, expression, index, ascending, retKeyRecords, retKeys, retRecords, viewVersion, false, counters,
              groupContext, currOffset, countReturned, limit, offset, keyOffsets, keyContainsColumns, isProbe, procedureContext);
        }
        //todo: support rightOperator
      }
      else {
        keyContainsColumns = false;
        if (rightOperator == null) {
          entry = doIndexLookupOneKey(cobj.getShort(ComObject.Tag.serializationVersion), dbName, count, tableSchema, indexSchema, parms, evaluateExpression, expression,
              columnOffsets, forceSelectOnServer, excludeKeys, originalLeftKey, leftKey, leftOperator, index, ascending,
              retKeyRecords, retKeys, retRecords, viewVersion, true, counters, groupContext, currOffset, countReturned, limit, offset,
              keyOffsets, keyContainsColumns, isProbe, procedureContext);
        }
        else {
          entry = doIndexLookupTwoKeys(cobj.getShort(ComObject.Tag.serializationVersion), dbName, count, tableSchema, indexSchema, forceSelectOnServer, excludeKeys,
              originalLeftKey, leftKey, columnOffsets, originalRightKey, rightKey, leftOperator, rightOperator, parms,
              evaluateExpression, expression, index, ascending, retKeyRecords, retKeys, retRecords, viewVersion, true,
              counters, groupContext, currOffset, countReturned, limit, offset, keyOffsets, keyContainsColumns, isProbe, procedureContext);
        }
      }

      ComObject retObj = new ComObject();
      if (entry != null) {
        retObj.put(ComObject.Tag.keyBytes, DatabaseCommon.serializeKey(tableSchema, indexName, entry.getKey()));
      }
      ComArray array = retObj.putArray(ComObject.Tag.keys, ComObject.Type.byteArrayType);
      for (Object[] key : retKeys) {
        array.add(DatabaseCommon.serializeKey(tableSchema, indexName, key));
      }
      array = retObj.putArray(ComObject.Tag.keyRecords, ComObject.Type.byteArrayType);
      for (byte[] key : retKeyRecords) {
        array.add(key);
      }
      array = retObj.putArray(ComObject.Tag.records, ComObject.Type.byteArrayType);
      for (int i = 0; i < retRecords.size(); i++) {
        byte[] bytes = retRecords.get(i);
        array.add(bytes);
      }

      if (counters != null) {
        array = retObj.putArray(ComObject.Tag.counters, ComObject.Type.byteArrayType);
        for (int i = 0; i < counters.length; i++) {
          array.add(counters[i].serialize());
        }
      }

      if (groupContext != null) {
        retObj.put(ComObject.Tag.legacyGroupContext, groupContext.serialize(server.getCommon()));
      }

      retObj.put(ComObject.Tag.currOffset, currOffset.get());
      retObj.put(ComObject.Tag.countReturned, countReturned.get());
//
//      if (schemaVersion < server.getSchemaVersion()) {
//        throw new SchemaOutOfSyncException("currVer:" + server.getCommon().getSchemaVersion() + ":");
//      }

      return retObj;
    }
    catch (IOException e) {
      e.printStackTrace();
      throw new DatabaseException(e);
    }
    finally {
      //context.stop();
    }
  }

  private Set<Integer> getSimpleColumnOffsets(DataInputStream in, String tableName, TableSchema tableSchema) throws IOException {
    int count = (int) Varint.readSignedVarLong(in);
    Set<Integer> columnOffsets = new HashSet<>();
    for (int i = 0; i < count; i++) {
      columnOffsets.add((int) Varint.readSignedVarLong(in));
    }
    return columnOffsets;
  }

  private Set<Integer> getColumnOffsets(
      DataInputStream in, String tableName,
      TableSchema tableSchema) throws IOException {
    Set<Integer> columnOffsets = new HashSet<>();
    int columnCount = (int) Varint.readSignedVarLong(in);
    for (int i = 0; i < columnCount; i++) {
      ColumnImpl column = new ColumnImpl();
      if (in.readBoolean()) {
        column.setTableName(in.readUTF());
      }
      column.setColumnName(in.readUTF());
      if (column.getTableName() == null || tableName.equals(column.getTableName())) {
        Integer offset = tableSchema.getFieldOffset(column.getColumnName());
        if (offset != null) {
          columnOffsets.add(offset);
        }
      }
    }
    if (columnOffsets.size() == tableSchema.getFields().size()) {
      columnOffsets.clear();
    }
    return columnOffsets;
  }

  public ComObject closeResultSet(ComObject cobj, boolean replayedCommand) {
    long resultSetId = cobj.getLong(ComObject.Tag.resultSetId);

    DiskBasedResultSet resultSet = new DiskBasedResultSet(server, resultSetId);
    resultSet.delete();

    return null;
  }

  public ComObject serverSelectDelete(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    long id = cobj.getLong(ComObject.Tag.id);

    DiskBasedResultSet resultSet = new DiskBasedResultSet(server, id);
    resultSet.delete();
    return null;
  }

  public ComObject serverSelect(ComObject cobj) {
    return serverSelect(cobj, false,null);
  }

  public ComObject serverSelect(ComObject cobj, boolean restrictToThisServer, StoredProcedureContextImpl procedureContext) {
    try {
      int schemaRetryCount = 0;
      if (server.getBatchRepartCount().get() != 0 && lookupCount.incrementAndGet() % 1000 == 0) {
        try {
          Thread.sleep(10);
        }
        catch (InterruptedException e) {
          throw new DatabaseException(e);
        }
      }

      String dbName = cobj.getString(ComObject.Tag.dbName);
      Integer schemaVersion = cobj.getInt(ComObject.Tag.schemaVersion);
      if (schemaVersion != null && schemaVersion < server.getSchemaVersion()) {
        throw new SchemaOutOfSyncException("currVer:" + server.getCommon().getSchemaVersion() + ":");
      }
      int count = cobj.getInt(ComObject.Tag.count);

      byte[] selectBytes = cobj.getByteArray(ComObject.Tag.legacySelectStatement);
      SelectStatementImpl select = new SelectStatementImpl(server.getDatabaseClient());
      select.deserialize(selectBytes, dbName);
      select.setIsOnServer(true);

      select.setServerSelectPageNumber(select.getServerSelectPageNumber() + 1);
      select.setServerSelectShardNumber(server.getShard());
      select.setServerSelectReplicaNumber(server.getReplica());

      Offset offset = select.getOffset();
      select.setOffset(null);
      Limit limit = select.getLimit();
      select.setLimit(null);
      DiskBasedResultSet diskResults = null;
      if (select.getServerSelectPageNumber() == 0) {
        //select.setPageSize(1000);
        ResultSetImpl resultSet = (ResultSetImpl) select.execute(dbName, null, null, null, null,
            null, restrictToThisServer, procedureContext, schemaRetryCount);

        ExpressionImpl.CachedRecord[][] results = resultSet.getReadRecordsAndSerializedRecords();
        if (results == null) {

        }
        else {
          int currCount = 0;
          for (ExpressionImpl.CachedRecord[] records : results) {
            for (ExpressionImpl.CachedRecord record : records) {
              currCount++;
            }
          }
          if (currCount < count) {
            // exhausted results

            ComObject retObj = new ComObject();
            select.setIsOnServer(false);
            retObj.put(ComObject.Tag.legacySelectStatement, select.serialize());

            long currOffset = 0;
            if (cobj.getLong(ComObject.Tag.currOffset) != null) {
              currOffset = cobj.getLong(ComObject.Tag.currOffset);
            }
            long countReturned = 0;
            if (cobj.getLong(ComObject.Tag.countReturned) != null) {
              countReturned = cobj.getLong(ComObject.Tag.countReturned);
            }
            ComArray tableArray = retObj.putArray(ComObject.Tag.tableRecords, ComObject.Type.arrayType);
            outer:
            for (ExpressionImpl.CachedRecord[] tableRecords : results) {
              ComArray recordArray = null;

              for (ExpressionImpl.CachedRecord record : tableRecords) {
                if (offset != null && currOffset < offset.getOffset()) {
                  currOffset++;
                  continue;
                }
                if (limit != null && countReturned >= limit.getRowCount()) {
                  break outer;
                }
                if (recordArray == null) {
                  recordArray = tableArray.addArray(ComObject.Tag.records, ComObject.Type.byteArrayType);
                }
                currOffset++;
                countReturned++;
                byte[] bytes = record.getSerializedRecord();
                if (bytes == null) {
                  bytes = record.getRecord().serialize(server.getCommon(), DatabaseClient.SERIALIZATION_VERSION);
                }
                recordArray.add(bytes);
              }
            }
            retObj.put(ComObject.Tag.currOffset, currOffset);
            retObj.put(ComObject.Tag.countReturned, countReturned);
            return retObj;
          }
        }

        diskResults = new DiskBasedResultSet(cobj.getShort(ComObject.Tag.serializationVersion), dbName, server, select.getOffset(), select.getLimit(),
            select.getTableNames(), new int[]{0}, new ResultSetImpl[]{resultSet}, select.getOrderByExpressions(), count, select, false);
      }
      else {
        diskResults = new DiskBasedResultSet(server, select, select.getTableNames(), select.getServerSelectResultSetId(), restrictToThisServer, procedureContext);
      }
      select.setServerSelectResultSetId(diskResults.getResultSetId());
      byte[][][] records = diskResults.nextPage(select.getServerSelectPageNumber());

      ComObject retObj = new ComObject();
      select.setIsOnServer(false);
      retObj.put(ComObject.Tag.legacySelectStatement, select.serialize());

      long currOffset = 0;
      if (cobj.getLong(ComObject.Tag.currOffset) != null) {
        currOffset = cobj.getLong(ComObject.Tag.currOffset);
      }
      long countReturned = 0;
      if (cobj.getLong(ComObject.Tag.countReturned) != null) {
        countReturned = cobj.getLong(ComObject.Tag.countReturned);
      }
      if (records != null) {
        ComArray tableArray = retObj.putArray(ComObject.Tag.tableRecords, ComObject.Type.arrayType);
        outer:
        for (byte[][] tableRecords : records) {
          ComArray recordArray = null;

          for (byte[] record : tableRecords) {
            if (offset != null && currOffset < offset.getOffset()) {
              currOffset++;
              continue;
            }
            if (limit != null && countReturned >= limit.getRowCount()) {
              break outer;
            }
            if (recordArray == null) {
              recordArray = tableArray.addArray(ComObject.Tag.records, ComObject.Type.byteArrayType);
            }
            currOffset++;
            countReturned++;
            recordArray.add(record);
          }
        }
      }
      retObj.put(ComObject.Tag.currOffset, currOffset);
      retObj.put(ComObject.Tag.countReturned, countReturned);

      return retObj;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public ComObject serverSetSelect(ComObject cobj) {
    return serverSetSelect(cobj, false, null);
  }

  public ComObject serverSetSelect(ComObject cobj, final boolean restrictToThisServer, final StoredProcedureContextImpl procedureContext) {
    try {
      final int schemaRetryCount = 0;
      if (server.getBatchRepartCount().get() != 0 && lookupCount.incrementAndGet() % 1000 == 0) {
        try {
          Thread.sleep(10);
        }
        catch (InterruptedException e) {
          throw new DatabaseException(e);
        }
      }

      final String dbName = cobj.getString(ComObject.Tag.dbName);
//      int schemaVersion = cobj.getInt(ComObject.Tag.schemaVersion);
//      if (schemaVersion < server.getSchemaVersion()) {
//        throw new SchemaOutOfSyncException("currVer:" + server.getCommon().getSchemaVersion() + ":");
//      }

      ComArray array = cobj.getArray(ComObject.Tag.selectStatements);
      final SelectStatementImpl[] selectStatements = new SelectStatementImpl[array.getArray().size()];
      for (int i = 0; i < array.getArray().size(); i++) {
        SelectStatementImpl stmt = new SelectStatementImpl(server.getClient());
        stmt.deserialize((byte[])array.getArray().get(i), dbName);
        selectStatements[i] = stmt;
      }
      ComArray tablesArray = cobj.getArray(ComObject.Tag.tables);
      String[] tableNames = new String[tablesArray.getArray().size()];
      TableSchema[] tableSchemas = new TableSchema[tableNames.length];
      for (int i = 0; i < array.getArray().size(); i++) {
        tableNames[i] = (String)tablesArray.getArray().get(i);
        tableSchemas[i] = server.getCommon().getTableSchema(dbName, tableNames[i], server.getDataDir());
      }

      List<OrderByExpressionImpl> orderByExpressions = new ArrayList<>();
      ComArray orderByArray = cobj.getArray(ComObject.Tag.orderByExpressions);
      if (orderByArray != null) {
        for (int i = 0; i < orderByArray.getArray().size(); i++) {
          OrderByExpressionImpl orderBy = new OrderByExpressionImpl();
          orderBy.deserialize((byte[]) orderByArray.getArray().get(i));
          orderByExpressions.add(orderBy);
        }
      }

      boolean notAll = false;
      ComArray operationsArray = cobj.getArray(ComObject.Tag.operations);
      String[] operations = new String[operationsArray.getArray().size()];
      for (int i = 0; i < operations.length; i++) {
        operations[i] = (String) operationsArray.getArray().get(i);
        if (!operations[i].toUpperCase().endsWith("ALL")) {
          notAll = true;
        }
      }

      long serverSelectPageNumber = cobj.getLong(ComObject.Tag.serverSelectPageNumber);
      long resultSetId = cobj.getLong(ComObject.Tag.resultSetId);

      int count = cobj.getInt(ComObject.Tag.count);


      DiskBasedResultSet diskResults = null;
      if (serverSelectPageNumber == 0) {
        ThreadPoolExecutor executor = ThreadUtil.createExecutor(selectStatements.length, "SonicBase ReadManager serverSetSelect Thread");
        final ResultSetImpl[] resultSets = new ResultSetImpl[selectStatements.length];
        try {
          List<Future> futures = new ArrayList<>();
          for (int i = 0; i < selectStatements.length; i++) {
            final int offset = i;
            futures.add(executor.submit(new Callable() {
              @Override
              public Object call() throws Exception {
                SelectStatementImpl stmt = selectStatements[offset];
                stmt.setPageSize(1000);
                resultSets[offset] = (ResultSetImpl) stmt.execute(dbName, null, null, null, null,
                    null, restrictToThisServer, procedureContext, schemaRetryCount);
                return null;
              }
            }));
          }
          for (Future future : futures) {
            future.get();
          }
        }
        finally {
          executor.shutdownNow();
        }

        if (notAll) {
          diskResults = buildUniqueResultSet(cobj.getShort(ComObject.Tag.serializationVersion),
              dbName, selectStatements, tableNames, resultSets, orderByExpressions, count, operations);
        }
        else {
          int[] offsets = new int[tableNames.length];
          for (int i = 0; i < offsets.length; i++) {
            offsets[i] = i;
          }
          diskResults = new DiskBasedResultSet(cobj.getShort(ComObject.Tag.serializationVersion), dbName, server, null, null,
              tableNames, offsets, resultSets, orderByExpressions, count, null, true);
        }
      }
      else {
        diskResults = new DiskBasedResultSet(server, null, tableNames, resultSetId, restrictToThisServer, procedureContext);
      }
      byte[][][] records = diskResults.nextPage((int)serverSelectPageNumber);

      ComObject retObj = new ComObject();

      retObj.put(ComObject.Tag.resultSetId, diskResults.getResultSetId());
      retObj.put(ComObject.Tag.serverSelectPageNumber, serverSelectPageNumber + 1);
      retObj.put(ComObject.Tag.shard, server.getShard());
      retObj.put(ComObject.Tag.replica, server.getReplica());

      if (records != null) {
        ComArray tableArray = retObj.putArray(ComObject.Tag.tableRecords, ComObject.Type.arrayType);
        for (byte[][] tableRecords : records) {
          ComArray recordArray = tableArray.addArray(ComObject.Tag.records, ComObject.Type.byteArrayType);

          for (int i = 0; i < tableRecords.length; i++) {
            byte[] record = tableRecords[i];
            if (record == null) {
              recordArray.add(new byte[]{});
            }
            else {
              recordArray.add(record);
            }
          }
        }
      }

      return retObj;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private DiskBasedResultSet buildUniqueResultSet(final Short serializationVersion, final String dbName,
                                                  final SelectStatementImpl[] selectStatements, String[] tableNames,
                                                  final ResultSetImpl[] resultSets,
                                                  List<OrderByExpressionImpl> orderByExpressions, final int count, String[] operations) {
    final List<OrderByExpressionImpl> orderByUnique = new ArrayList<>();
    for (ColumnImpl column : selectStatements[0].getSelectColumns()) {
      String columnName = column.getColumnName();
      OrderByExpressionImpl orderBy = new OrderByExpressionImpl();
      orderBy.setAscending(true);
      orderBy.setColumnName(columnName);
      orderByUnique.add(orderBy);
    }
    boolean inMemory = true;
    for (int i = 0; i < resultSets.length; i++) {
      ExpressionImpl.CachedRecord[][] records = resultSets[0].getReadRecordsAndSerializedRecords();
      if (records != null && records.length >= 1000) {
        inMemory = false;
        break;
      }
    }
    Object[] diskResultSets = new Object[resultSets.length];
    if (inMemory) {
      for (int i = 0; i < resultSets.length; i++) {
        diskResultSets[i] = resultSets[i];

        ResultSetImpl.sortResults(dbName, server.getCommon(), resultSets[i].getReadRecordsAndSerializedRecords(),
            resultSets[i].getTableNames(), orderByUnique);
      }
    }
    else {
      ThreadPoolExecutor executor = ThreadUtil.createExecutor(resultSets.length, "SonicBase ReadManager buildUniqueResultSet Thread");
      List<Future> futures = new ArrayList<>();
      try {
        for (int i = 0; i < resultSets.length; i++) {
          final int offset = i;
          futures.add(executor.submit(new Callable() {
            @Override
            public Object call() throws Exception {
              return new DiskBasedResultSet(serializationVersion, dbName, server, null, null,
                  new String[]{selectStatements[offset].getFromTable()},
                  new int[]{offset}, new ResultSetImpl[]{resultSets[offset]}, orderByUnique, 30_000, null, true);
            }
          }));
        }
        for (int i = 0; i < futures.size(); i++) {
          diskResultSets[i] = (DiskBasedResultSet) futures.get(i).get();
        }
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
      finally {
        executor.shutdownNow();
      }
    }

    Object ret = diskResultSets[0];
    for (int i = 1; i < resultSets.length; i++) {
      boolean unique = !operations[i-1].toUpperCase().endsWith("ALL");
      boolean intersect = operations[i-1].toUpperCase().startsWith("INTERSECT");
      boolean except = operations[i-1].toUpperCase().startsWith("EXCEPT");
      List<String> tables = new ArrayList<>();
      String[] localTableNames = ret instanceof ResultSetImpl ?
          ((ResultSetImpl)ret).getTableNames() : ((DiskBasedResultSet)ret).getTableNames();
      for (String tableName : localTableNames) {
        tables.add(tableName);
      }
      localTableNames = diskResultSets[i] instanceof ResultSetImpl ?
          ((ResultSetImpl)diskResultSets[i]).getTableNames() : ((DiskBasedResultSet)diskResultSets[i]).getTableNames();
      for (String tableName : localTableNames) {
        tables.add(tableName);
      }
      ret = new DiskBasedResultSet(serializationVersion, dbName, server,
          tables.toArray(new String[tables.size()]), new Object[]{ret, diskResultSets[i]},
          orderByExpressions, count, unique, intersect, except, selectStatements[0].getSelectColumns());
    }

    return (DiskBasedResultSet) ret;
  }

  public ComObject indexLookupExpression(ComObject cobj) {
    return indexLookupExpression(cobj, null);
  }

  public ComObject indexLookupExpression(ComObject cobj, StoredProcedureContextImpl procedureContext) {
    try {
      if (server.getBatchRepartCount().get() != 0 && lookupCount.incrementAndGet() % 1000 == 0) {
        try {
          Thread.sleep(10);
        }
        catch (InterruptedException e) {
          throw new DatabaseException(e);
        }
      }


      String dbName = cobj.getString(ComObject.Tag.dbName);
      Integer schemaVersion = cobj.getInt(ComObject.Tag.schemaVersion);
      if (schemaVersion != null && schemaVersion < server.getSchemaVersion()) {
        throw new SchemaOutOfSyncException("currVer:" + server.getCommon().getSchemaVersion() + ":");
      }
      int count = cobj.getInt(ComObject.Tag.count);

      AtomicLong currOffset = new AtomicLong(cobj.getLong(ComObject.Tag.currOffset));
      Long limit = cobj.getLong(ComObject.Tag.limitLong);
      Long offset = cobj.getLong(ComObject.Tag.offsetLong);

      int tableId = cobj.getInt(ComObject.Tag.tableId);
      byte[] parmBytes = cobj.getByteArray(ComObject.Tag.parms);
      ParameterHandler parms = null;
      if (parmBytes != null) {
        parms = new ParameterHandler();
        parms.deserialize(parmBytes);
      }
      byte[] expressionBytes = cobj.getByteArray(ComObject.Tag.legacyExpression);
      Expression expression = null;
      if (expressionBytes != null) {
        expression = ExpressionImpl.deserializeExpression(expressionBytes);
      }
      String tableName = null;
      String indexName = null;
      try {
        //  logger.info("indexLookup: tableid=" + tableId + ", tableCount=" + common.getTablesById().size() + ", tableNull=" + (common.getTablesById().get(tableId) == null));
        TableSchema tableSchema = server.getCommon().getTablesById(dbName).get(tableId);
        tableName = tableSchema.getName();
        for (Map.Entry<String, IndexSchema> entry : tableSchema.getIndices().entrySet()) {
          if (entry.getValue().isPrimaryKey()) {
            indexName = entry.getKey();
          }
        }
      }
      catch (Exception e) {
        logger.info("indexLookup: tableName=" + tableName + ", tableid=" + tableId + ", tableByNameCount=" + server.getCommon().getTables(dbName).size() + ", tableCount=" + server.getCommon().getTablesById(dbName).size() +
            ", tableNull=" + (server.getCommon().getTablesById(dbName).get(tableId) == null) + ", indexName=" + indexName + ", indexName=" + indexName +
            ", indexNull=" /*+ (common.getTablesById().get(tableId).getIndexesById().get(indexId) == null) */);
        throw e;
      }
      //int srcCount = in.readInt();
      ComArray orderByArray = cobj.getArray(ComObject.Tag.orderByExpressions);
      List<OrderByExpressionImpl> orderByExpressions = new ArrayList<>();
      if (orderByArray != null) {
        for (int i = 0; i < orderByArray.getArray().size(); i++) {
          OrderByExpressionImpl orderByExpression = new OrderByExpressionImpl();
          orderByExpression.deserialize((byte[])orderByArray.getArray().get(i));
          orderByExpressions.add(orderByExpression);
        }
      }

      long viewVersion = cobj.getLong(ComObject.Tag.viewVersion);

      TableSchema tableSchema = server.getCommon().getTableSchema(dbName, tableName, server.getDataDir());
      IndexSchema indexSchema = server.getIndexSchema(dbName, tableSchema.getName(), indexName);

      byte[] leftKeyBytes = cobj.getByteArray(ComObject.Tag.leftKey);
      Object[] leftKey = null;
      if (leftKeyBytes != null) {
        leftKey = DatabaseCommon.deserializeTypedKey(leftKeyBytes);
      }

      byte[] rightKeyBytes = cobj.getByteArray(ComObject.Tag.rightKey);
      Object[] rightKey = null;
      if (rightKeyBytes != null) {
        rightKey = DatabaseCommon.deserializeTypedKey(rightKeyBytes);
      }

      Integer rightOpValue = cobj.getInt(ComObject.Tag.rightOperator);
      BinaryExpression.Operator rightOperator = null;
      if (rightOpValue != null) {
        rightOperator = BinaryExpression.Operator.getOperator(rightOpValue);
      }

      ComArray cOffsets = cobj.getArray(ComObject.Tag.columnOffsets);
      Set<Integer> columnOffsets = new HashSet<>();
      for (Object obj : cOffsets.getArray()) {
        columnOffsets.add((Integer)obj);
      }

      ComArray countersArray = cobj.getArray(ComObject.Tag.counters);
      Counter[] counters = null;
      if (countersArray != null) {
        counters = new Counter[countersArray.getArray().size()];
        for (int i = 0; i < counters.length; i++) {
          counters[i] = new Counter();
          counters[i].deserialize((byte[])countersArray.getArray().get(i));
        }
      }

      byte[] groupBytes = cobj.getByteArray(ComObject.Tag.legacyGroupContext);
      GroupByContext groupByContext = null;
      if (groupBytes != null) {
        groupByContext = new GroupByContext();
        groupByContext.deserialize(groupBytes, server.getCommon(), dbName);
      }

      Boolean isProbe = cobj.getBoolean(ComObject.Tag.isProbe);
      if (isProbe == null) {
        isProbe = false;
      }

      Index index = server.getIndex(dbName, tableSchema.getName(), indexName);
      Map.Entry<Object[], Object> entry = null;

      Boolean ascending = null;
      if (orderByExpressions.size() != 0) {
        OrderByExpressionImpl orderByExpression = orderByExpressions.get(0);
        String columnName = orderByExpression.getColumnName();
        boolean isAscending = orderByExpression.isAscending();
        if (columnName.equals(indexSchema.getFields()[0])) {
          ascending = isAscending;
        }
      }

      List<byte[]> retRecords = new ArrayList<>();

      if (tableSchema.getIndexes().get(indexName).isPrimaryKey()) {
        entry = doIndexLookupWithRecordsExpression(cobj.getShort(ComObject.Tag.serializationVersion), dbName,
            viewVersion, count, tableSchema, indexSchema, columnOffsets, parms, expression, index, leftKey,
            rightKey, rightOperator,
            ascending, retRecords, counters, groupByContext, currOffset, limit, offset, isProbe, procedureContext);
      }
      else {
        //entry = doIndexLookupExpression(count, indexSchema, columnOffsets, index, leftKey, ascending, retKeys);
      }
      //}
      ComObject retObj = new ComObject();
      if (entry != null) {
        retObj.put(ComObject.Tag.keyBytes, DatabaseCommon.serializeKey(tableSchema, indexName, entry.getKey()));
      }

      ComArray records = retObj.putArray(ComObject.Tag.records, ComObject.Type.byteArrayType);
      for (byte[] record : retRecords) {
        records.add(record);
      }

      if (counters != null) {
        countersArray = retObj.putArray(ComObject.Tag.counters, ComObject.Type.byteArrayType);
        for (int i = 0; i < counters.length; i++) {
          countersArray.add(counters[i].serialize());
        }
      }

      if (groupByContext != null) {
        retObj.put(ComObject.Tag.legacyGroupContext, groupByContext.serialize(server.getCommon()));
      }

      retObj.put(ComObject.Tag.currOffset, currOffset.get());

//      if (schemaVersion < server.getSchemaVersion()) {
//        throw new SchemaOutOfSyncException("currVer:" + server.getCommon().getSchemaVersion() + ":");
//      }

      return retObj;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  private Map.Entry<Object[], Object> doIndexLookupWithRecordsExpression(
      short serializationVersion,
      String dbName, long viewVersion, int count, TableSchema tableSchema, IndexSchema indexSchema,
      Set<Integer> columnOffsets, ParameterHandler parms, Expression expression,
      Index index, Object[] leftKey, Object[] rightKey, BinaryExpression.Operator rightOperator,
      Boolean ascending, List<byte[]> ret, Counter[] counters, GroupByContext groupByContext,
      AtomicLong currOffset, Long limit, Long offset, boolean isProbe, StoredProcedureContextImpl procedureContext) {

    final AtomicInteger countSkipped = new AtomicInteger();

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
    outer:
    while (entry != null) {
      if (ret.size() >= count) {
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
        //synchronized (index.getMutex(entry.getKey())) {
        //if (entry.getValue() instanceof Long) {
        //TODO: unsafe
        // entry.setValue(index.get(entry.getKey()));
        //}
        if (entry.getValue() != null && !entry.getValue().equals(0L)) {
          records = server.fromUnsafeToRecords(entry.getValue());
        }
        //}
        if (parms != null && expression != null && records != null) {
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
                  com.sonicbase.procedure.RecordImpl procedureRecord = new RecordImpl();
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
                  boolean keyContainsColumns = false;

                  byte[][] currRet = applySelectToResultRecords(serializationVersion, dbName, columnOffsets, forceSelectOnServer, currRecords,
                      entry.getKey(), tableSchema, counters, groupByContext, keyOffsets, keyContainsColumns);
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
                        ret.add(currBytes);
                      }
                      if (localDone) {
                        entry = null;
                        break outer;
                      }
                    }
                  }
                }
              }
            }
          }
        }
        else {
          AtomicBoolean done = new AtomicBoolean();
          records = processViewFlags(dbName, tableSchema, indexSchema, index, viewVersion, entry.getKey(), records, done);
          if (records != null) {
            int[] keyOffsets = null;
            boolean keyContainsColumns = false;

            byte[][] retRecords = applySelectToResultRecords(serializationVersion, dbName, columnOffsets, forceSelectOnServer, records,
                entry.getKey(), tableSchema, counters, groupByContext, keyOffsets, keyContainsColumns);
            if (counters == null) {
              for (byte[] currBytes : retRecords) {
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
                  ret.add(currBytes);
                }
                if (localDone) {
                  entry = null;
                  break outer;
                }
              }
            }
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


  private Map.Entry<Object[], Object> doIndexLookupTwoKeys(
      short serializationVersion,
      String dbName,
      final int count,
      TableSchema tableSchema,
      IndexSchema indexSchema,
      boolean forceSelectOnServer, List<Object[]> excludeKeys,
      Object[] originalLeftKey,
      Object[] leftKey,
      Set<Integer> columnOffsets,
      Object[] originalRightKey,
      Object[] rightKey,
      BinaryExpression.Operator leftOperator,
      BinaryExpression.Operator rightOperator,
      ParameterHandler parms,
      boolean evaluateExpression,
      Expression expression,
      Index index,
      Boolean ascending,
      List<byte[]> retKeyRecords,
      List<Object[]> retKeys,
      List<byte[]> retRecords,
      long viewVersion, boolean keys,
      Counter[] counters,
      GroupByContext groupContext, AtomicLong currOffset, AtomicLong countReturned, Long limit, Long offset, int[] keyOffsets,
      boolean keyContainsColumns, boolean isProbe, StoredProcedureContextImpl procedureContext) {

    Map.Entry<Object[], Object> entry = null;
    try {
      final AtomicInteger countSkipped = new AtomicInteger();
      BinaryExpression.Operator greaterOp = leftOperator;
      Object[] greaterKey = leftKey;
      Object[] greaterOriginalKey = originalLeftKey;
      BinaryExpression.Operator lessOp = rightOperator;
      Object[] lessKey = leftKey;//originalRightKey;
      Object[] lessOriginalKey = originalRightKey;
      if (greaterOp == BinaryExpression.Operator.less ||
          greaterOp == BinaryExpression.Operator.lessEqual) {
        greaterOp = rightOperator;
        greaterKey = leftKey; //rightKey;
        greaterOriginalKey = originalRightKey;
        lessOp = leftOperator;
        lessKey = leftKey;//originalLeftKey;
        lessOriginalKey = originalLeftKey;
      }

      boolean useGreater = false;
      if (ascending == null || ascending) {
        useGreater = true;
        if (greaterKey != null) {
          entry = index.ceilingEntry(greaterKey);
          lessKey = originalLeftKey;
        }
        else {
          if (greaterOriginalKey == null) {
            entry = index.firstEntry();
          }
          else {
            entry = index.ceilingEntry(greaterOriginalKey);
          }
        }
        if (entry == null) {
          entry = index.firstEntry();
        }
      }
      else {
        if (ascending != null && !ascending) {
          if (lessKey != null) {
            entry = index.floorEntry(lessKey);
            greaterKey = originalRightKey;
          }
          else {
            if (lessOriginalKey == null) {
              entry = index.lastEntry();
            }
            else {
              entry = index.floorEntry(lessOriginalKey);
            }
          }
          if (entry == null) {
            entry = index.lastEntry();
          }
        }
      }
      if (entry != null) {
        Object[] key = lessKey;
        //      rightKey = greaterKey;
        //      if (ascending == null || ascending) {
        key = greaterOriginalKey;
        rightKey = lessKey;
        //}

        if ((ascending != null && !ascending)) {
          if (lessKey != null && lessOriginalKey != lessKey) {
            if (lessOp.equals(BinaryExpression.Operator.less) ||
                lessOp.equals(BinaryExpression.Operator.lessEqual) ||
                lessOp.equals(BinaryExpression.Operator.greater) ||
                lessOp.equals(BinaryExpression.Operator.greaterEqual)) {
              boolean foundMatch = 0 == server.getCommon().compareKey(indexSchema.getComparators(), entry.getKey(), lessKey);
              if (foundMatch) {
                entry = index.lowerEntry((entry.getKey()));
              }
            }
          }
          else if (lessOriginalKey != null) {
            if (lessOp.equals(BinaryExpression.Operator.less)) {
              boolean foundMatch = 0 == server.getCommon().compareKey(indexSchema.getComparators(), entry.getKey(), lessOriginalKey);
              if (foundMatch) {
                entry = index.lowerEntry((entry.getKey()));
              }
            }
          }
        }
        else {
          if (greaterKey != null) {
            if (greaterOp.equals(BinaryExpression.Operator.less) ||
                greaterOp.equals(BinaryExpression.Operator.lessEqual) ||
                greaterOp.equals(BinaryExpression.Operator.greater) ||
                greaterOp.equals(BinaryExpression.Operator.greaterEqual)) {
              boolean foundMatch = key != null && 0 == server.getCommon().compareKey(indexSchema.getComparators(), entry.getKey(), greaterKey);
              if (foundMatch) {
                entry = index.higherEntry((entry.getKey()));
              }
            }
          }
          else if (greaterOriginalKey != null) {
            if (greaterOp.equals(BinaryExpression.Operator.greater)) {
              boolean foundMatch = 0 == server.getCommon().compareKey(indexSchema.getComparators(), entry.getKey(), greaterOriginalKey);
              if (foundMatch) {
                entry = index.higherEntry((entry.getKey()));
              }
            }
          }
        }
        if (entry != null && lessKey != null) {
          if (useGreater) {
            int compareValue = server.getCommon().compareKey(indexSchema.getComparators(), entry.getKey(), greaterOriginalKey);
            if ((0 == compareValue || -1 == compareValue) && greaterOp == BinaryExpression.Operator.greater) {
              entry = null;
            }
//            if (1 == compareValue) {
//              entry = null;
//            }
          }
          else {
            int compareValue = server.getCommon().compareKey(indexSchema.getComparators(), entry.getKey(), lessKey);
            if ((0 == compareValue || 1 == compareValue) && lessOp == BinaryExpression.Operator.less) {
              entry = null;
            }
            if (1 == compareValue) {
              entry = null;
            }
          }
        }

        Map.Entry<Object[], Object>[] entries = new Map.Entry[]{entry};
        outer:
        while (entry != null) {
          if (retKeyRecords.size() >= count || retRecords.size() >= count) {
            break;
          }

          if (key != null) {
            if (excludeKeys != null) {
              for (Object[] excludeKey : excludeKeys) {
                if (server.getCommon().compareKey(indexSchema.getComparators(), excludeKey, key) == 0) {
                  continue outer;
                }
              }
            }

            boolean rightIsDone = false;
            int compareRight = 1;
            if (lessOriginalKey != null) {
              compareRight = server.getCommon().compareKey(indexSchema.getComparators(), entry.getKey(), lessOriginalKey);
            }
            if (lessOp.equals(BinaryExpression.Operator.less) && compareRight >= 0) {
              rightIsDone = true;
            }
            if (lessOp.equals(BinaryExpression.Operator.lessEqual) && compareRight > 0) {
              rightIsDone = true;
            }
            if (rightIsDone) {
              entry = null;
              break;
            }
          }
          Object[][] currKeys = null;
          byte[][] currKeyRecords = null;
          byte[][] records = null;

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

            //synchronized (index.getMutex(entry.getKey())) {
            //if (entry.getValue() instanceof Long) {
            //TODO: unsafe
            //entry.setValue(index.get(entry.getKey()));
            //}
            if (entry.getValue() != null && !entry.getValue().equals(0L)) {
              if (keys) {
                currKeyRecords = server.fromUnsafeToKeys(entry.getValue());
              }
              else {
                records = server.fromUnsafeToRecords(entry.getValue());
              }
            }
            //}
            if (keys) {
              if (keyContainsColumns) {
                ProcessKeyContainsColumns processKeyContainsColumns = new ProcessKeyContainsColumns(serializationVersion, dbName, tableSchema,
                    indexSchema, parms, evaluateExpression, expression, columnOffsets, forceSelectOnServer, index,
                    viewVersion, counters, groupContext, keyOffsets, keyContainsColumns, entry, entry, currKeyRecords,
                    records).invoke();
                //                    if (processKeyContainsColumns.is())
                //                      break outer;
                currKeys = processKeyContainsColumns.getCurrKeys();
                currKeyRecords = processKeyContainsColumns.getCurrKeyRecords();
                records = processKeyContainsColumns.getRecords();
                entry = processKeyContainsColumns.getEntry();
              }
              else {
                Object unsafeAddress = entry.getValue();
                currKeyRecords = server.fromUnsafeToKeys(unsafeAddress);
              }
            }
            else {
//              Object unsafeAddress = entry.getValue();//index.get(entry.getKey());
//              if (unsafeAddress != null && !unsafeAddress.equals(0L)) {
//                records = server.fromUnsafeToRecords(unsafeAddress);
//                while (records == null) {
//                  try {
//                    Thread.sleep(100);
//                    System.out.println("null records ************************************");
//                  }
//                  catch (InterruptedException e) {
//                    throw new DatabaseException(e);
//                  }
//                  entry.setValue(index.get(entry.getKey()));
//                  unsafeAddress = entry.getValue();//index.get(entry.getKey());
//                  if (unsafeAddress != null && !unsafeAddress.equals(0L)) {
//                    records = server.fromUnsafeToRecords(unsafeAddress);
//                  }
//                  else {
//                    break;
//                  }
//                }
//              }
            }

            if (entry.getValue() != null) {
              Object[] keyToUse = entry.getKey();//key;
              if (keyToUse == null) {
                keyToUse = originalLeftKey;
              }

              AtomicBoolean done = new AtomicBoolean();
              handleRecord(serializationVersion, dbName, tableSchema, indexSchema, viewVersion, index, keyToUse, parms, evaluateExpression,
                  expression, columnOffsets, forceSelectOnServer, retKeyRecords, retKeys, retRecords, keys, counters, groupContext,
                  records, currKeyRecords, currKeys, offset, currOffset, countReturned, limit, done, countSkipped, isProbe, procedureContext);
              if (done.get()) {
                entry = null;
                break outer;
              }


              //          for (byte[] currKey : currKeyRecords) {
              //            boolean localDone = false;
              //            boolean include = true;
              //            long targetOffset = 1;
              //            currOffset.incrementAndGet();
              //            if (offset != null) {
              //              targetOffset = offset;
              //              if (currOffset.get() < offset) {
              //                include = false;
              //              }
              //            }
              //            if (include) {
              //              if (limit != null) {
              //                if (currOffset.get() >= targetOffset + limit) {
              //                  include = false;
              //                  localDone = true;
              //                }
              //              }
              //            }
              //            if (include) {
              //              retKeyRecords.add(currKey);
              //            }
              //            if (localDone) {
              //              entry = null;
              //              break outer;
              //            }
              //          }
              //        }
              //        else {
              //          AtomicBoolean done = new AtomicBoolean();
              //          records = processViewFlags(dbName, tableSchema, indexSchema, index, viewVersion, entry.getKey(), records, done);
              //          if (records == null) {
              //            if (done.get()) {
              //              entry = null;
              //              break outer;
              //            }
              //          }
              //          else {
              //            if (parms != null && expression != null && evaluateExpression) {
              //              for (byte[] bytes : records) {
              //                Record record = new Record(tableSchema);
              //                record.deserialize(dbName, server.getCommon(), bytes, null, true);
              //                boolean pass = (Boolean) ((ExpressionImpl) expression).evaluateSingleRecord(new TableSchema[]{tableSchema}, new Record[]{record}, parms);
              //                if (pass) {
              //                  byte[][] currRecords = new byte[][]{bytes};
              //
              //                  byte[][] ret = applySelectToResultRecords(serializationVersion, dbName, columnOffsets, forceSelectOnServer, currRecords,
              //                      entry.getKey(), tableSchema, counters, groupContext, null, false);
              //                  if (counters == null) {
              //                    for (byte[] currBytes : ret) {
              //                      boolean localDone = false;
              //                      boolean include = true;
              //                      long targetOffset = 1;
              //                      currOffset.incrementAndGet();
              //                      if (offset != null) {
              //                        targetOffset = offset;
              //                        if (currOffset.get() < offset) {
              //                          include = false;
              //                        }
              //                      }
              //                      if (include) {
              //                        if (limit != null) {
              //                          if (currOffset.get() >= targetOffset + limit) {
              //                            include = false;
              //                            localDone = true;
              //                          }
              //                        }
              //                      }
              //                      if (include) {
              //                        retRecords.add(currBytes);
              //                      }
              //                      if (localDone) {
              //                        entry = null;
              //                        break outer;
              //                      }
              //                    }
              //                  }
              //                }
              //              }
              //            } else {
              //              if (records.length > 2) {
              //                logger.error("Records size: " + records.length);
              //              }
              //
              //              byte[][] ret = applySelectToResultRecords(serializationVersion, dbName, columnOffsets, forceSelectOnServer, records,
              //                  entry.getKey(), tableSchema, counters, groupContext, null, false);
              //              if (counters == null) {
              //                for (byte[] currBytes : ret) {
              //                  boolean localDone = false;
              //                  boolean include = true;
              //                  long targetOffset = 1;
              //                  currOffset.incrementAndGet();
              //                  if (offset != null) {
              //                    targetOffset = offset;
              //                    if (currOffset.get() < offset) {
              //                      include = false;
              //                    }
              //                  }
              //                  if (include) {
              //                    if (limit != null) {
              //                      if (currOffset.get() >= targetOffset + limit) {
              //                        include = false;
              //                        localDone = true;
              //                      }
              //                    }
              //                  }
              //                  if (include) {
              //                    retRecords.add(currBytes);
              //                  }
              //                  if (localDone) {
              //                    entry = null;
              //                    break outer;
              //                  }
              //                }
              //              }
              //            }
              //          }

            }
          }
          //        if (operator.equals(QueryEvaluator.BinaryRelationalOperator.Operator.equal)) {
          //          entry = null;
          //          break;
          //        }
          if (ascending != null && !ascending) {
            entry = index.lowerEntry(entry.getKey());
          }
          else {
            entry = index.higherEntry(entry.getKey());
          }
          if (entry != null) {
            if (entry.getKey() == null) {
              throw new DatabaseException("entry key is null");
            }
            if (lessOriginalKey == null) {
              throw new DatabaseException("original less key is null");
            }
            int compareValue = server.getCommon().compareKey(indexSchema.getComparators(), entry.getKey(), lessOriginalKey);
            if ((0 == compareValue || 1 == compareValue) && lessOp == BinaryExpression.Operator.less) {
              entry = null;
              break;
            }
            if (1 == compareValue) {
              entry = null;
              break;
            }
            compareValue = 1;
            if (greaterOriginalKey != null) {
              compareValue = server.getCommon().compareKey(indexSchema.getComparators(), entry.getKey(), greaterOriginalKey);
            }
            if (0 == compareValue && greaterOp == BinaryExpression.Operator.greater) {
              entry = null;
              break;
            }
            if (-1 == compareValue) {
              entry = null;
              break;
            }
          }

//          for (Map.Entry<Object[], Object> currEntry : entries) {
//            entry = currEntry;
//
//            if (currEntry == null) {
//              break outer;
//            }
//            if (entry != null) {
//              if (entry.getKey() == null) {
//                throw new DatabaseException("entry key is null");
//              }
//              if (lessOriginalKey == null) {
//                throw new DatabaseException("original less key is null");
//              }
//              int compareValue = server.getCommon().compareKey(indexSchema.getComparators(), entry.getKey(), lessOriginalKey);
//              if ((0 == compareValue || 1 == compareValue) && lessOp == BinaryExpression.Operator.less) {
//                entry = null;
//                break outer;
//              }
//              if (1 == compareValue) {
//                entry = null;
//                break outer;
//              }
//              compareValue = 1;
//              if (greaterOriginalKey != null) {
//                compareValue = server.getCommon().compareKey(indexSchema.getComparators(), entry.getKey(), greaterOriginalKey);
//              }
//              if (0 == compareValue && greaterOp == BinaryExpression.Operator.greater) {
//                entry = null;
//                break outer;
//              }
//              if (-1 == compareValue) {
//                entry = null;
//                break outer;
//              }
//            }
//
////          System.out.println("processing key: " + DatabaseCommon.keyToString(entry.getKey()) +
////              ", shard=" + server.getShard() + ", replica=" + server.getReplica() + ", viewver=" + server.getCommon().getSchemaVersion());
//            if (key != null) {
//              if (excludeKeys != null) {
//                for (Object[] excludeKey : excludeKeys) {
//                  if (server.getCommon().compareKey(indexSchema.getComparators(), excludeKey, key) == 0) {
//                    continue outer;
//                  }
//                }
//              }
//
//              boolean rightIsDone = false;
//              int compareRight = 1;
//              if (lessOriginalKey != null) {
//                compareRight = server.getCommon().compareKey(indexSchema.getComparators(), entry.getKey(), lessOriginalKey);
//              }
//              if (lessOp.equals(BinaryExpression.Operator.less) && compareRight >= 0) {
//                rightIsDone = true;
//              }
//              if (lessOp.equals(BinaryExpression.Operator.lessEqual) && compareRight > 0) {
//                rightIsDone = true;
//              }
//              if (rightIsDone) {
//                entry = null;
//                break;
//              }
//            }
//            Object[][] currKeys = null;
//            byte[][] currKeyRecords = null;
//            byte[][] records = null;
//
//            boolean shouldProcess = true;
//            if (isProbe) {
//              if (countSkipped.incrementAndGet() < OPTIMIZED_RANGE_PAGE_SIZE) {
//                shouldProcess = false;
//              }
//              else {
//                countSkipped.set(0);
//              }
//            }
//            if (shouldProcess) {
//
//              //synchronized (index.getMutex(entry.getKey())) {
//              //if (entry.getValue() instanceof Long) {
//              //TODO: unsafe
//              //entry.setValue(index.get(entry.getKey()));
//              //}
//              if (entry.getValue() != null && !entry.getValue().equals(0L)) {
//                if (keys) {
//                  currKeyRecords = server.fromUnsafeToKeys(entry.getValue());
//                }
//                else {
//                  records = server.fromUnsafeToRecords(entry.getValue());
//                }
//              }
//              //}
//              if (keys) {
//                if (keyContainsColumns) {
//                  ProcessKeyContainsColumns processKeyContainsColumns = new ProcessKeyContainsColumns(serializationVersion, dbName, tableSchema,
//                      indexSchema, parms, evaluateExpression, expression, columnOffsets, forceSelectOnServer, index,
//                      viewVersion, counters, groupContext, keyOffsets, keyContainsColumns, entry, entry, currKeyRecords,
//                      records).invoke();
//                  //                    if (processKeyContainsColumns.is())
//                  //                      break outer;
//                  currKeys = processKeyContainsColumns.getCurrKeys();
//                  currKeyRecords = processKeyContainsColumns.getCurrKeyRecords();
//                  records = processKeyContainsColumns.getRecords();
//                  entry = processKeyContainsColumns.getEntry();
//                }
//                else {
//                  Object unsafeAddress = entry.getValue();
//                  currKeyRecords = server.fromUnsafeToKeys(unsafeAddress);
//                }
//              }
//              else {
//                Object unsafeAddress = entry.getValue();//index.get(entry.getKey());
//                if (unsafeAddress != null && !unsafeAddress.equals(0L)) {
//                  records = server.fromUnsafeToRecords(unsafeAddress);
//                  while (records == null) {
//                    try {
//                      Thread.sleep(100);
//                      System.out.println("null records ************************************");
//                    }
//                    catch (InterruptedException e) {
//                      throw new DatabaseException(e);
//                    }
//                    entry.setValue(index.get(entry.getKey()));
//                    unsafeAddress = entry.getValue();//index.get(entry.getKey());
//                    if (unsafeAddress != null && !unsafeAddress.equals(0L)) {
//                      records = server.fromUnsafeToRecords(unsafeAddress);
//                    }
//                    else {
//                      break;
//                    }
//                  }
//                }
//              }
//
//              if (entry.getValue() != null) {
//                Object[] keyToUse = entry.getKey();//key;
//                if (keyToUse == null) {
//                  keyToUse = originalLeftKey;
//                }
//
//                AtomicBoolean done = new AtomicBoolean();
//                handleRecord(serializationVersion, dbName, tableSchema, indexSchema, viewVersion, index, keyToUse, parms, evaluateExpression,
//                    expression, columnOffsets, forceSelectOnServer, retKeyRecords, retKeys, retRecords, keys, counters, groupContext,
//                    records, currKeyRecords, currKeys, offset, currOffset, limit, done, countSkipped, isProbe, procedureContext);
//                if (done.get()) {
//                  entry = null;
//                  break outer;
//                }
//
//
//                //          for (byte[] currKey : currKeyRecords) {
//                //            boolean localDone = false;
//                //            boolean include = true;
//                //            long targetOffset = 1;
//                //            currOffset.incrementAndGet();
//                //            if (offset != null) {
//                //              targetOffset = offset;
//                //              if (currOffset.get() < offset) {
//                //                include = false;
//                //              }
//                //            }
//                //            if (include) {
//                //              if (limit != null) {
//                //                if (currOffset.get() >= targetOffset + limit) {
//                //                  include = false;
//                //                  localDone = true;
//                //                }
//                //              }
//                //            }
//                //            if (include) {
//                //              retKeyRecords.add(currKey);
//                //            }
//                //            if (localDone) {
//                //              entry = null;
//                //              break outer;
//                //            }
//                //          }
//                //        }
//                //        else {
//                //          AtomicBoolean done = new AtomicBoolean();
//                //          records = processViewFlags(dbName, tableSchema, indexSchema, index, viewVersion, entry.getKey(), records, done);
//                //          if (records == null) {
//                //            if (done.get()) {
//                //              entry = null;
//                //              break outer;
//                //            }
//                //          }
//                //          else {
//                //            if (parms != null && expression != null && evaluateExpression) {
//                //              for (byte[] bytes : records) {
//                //                Record record = new Record(tableSchema);
//                //                record.deserialize(dbName, server.getCommon(), bytes, null, true);
//                //                boolean pass = (Boolean) ((ExpressionImpl) expression).evaluateSingleRecord(new TableSchema[]{tableSchema}, new Record[]{record}, parms);
//                //                if (pass) {
//                //                  byte[][] currRecords = new byte[][]{bytes};
//                //
//                //                  byte[][] ret = applySelectToResultRecords(serializationVersion, dbName, columnOffsets, forceSelectOnServer, currRecords,
//                //                      entry.getKey(), tableSchema, counters, groupContext, null, false);
//                //                  if (counters == null) {
//                //                    for (byte[] currBytes : ret) {
//                //                      boolean localDone = false;
//                //                      boolean include = true;
//                //                      long targetOffset = 1;
//                //                      currOffset.incrementAndGet();
//                //                      if (offset != null) {
//                //                        targetOffset = offset;
//                //                        if (currOffset.get() < offset) {
//                //                          include = false;
//                //                        }
//                //                      }
//                //                      if (include) {
//                //                        if (limit != null) {
//                //                          if (currOffset.get() >= targetOffset + limit) {
//                //                            include = false;
//                //                            localDone = true;
//                //                          }
//                //                        }
//                //                      }
//                //                      if (include) {
//                //                        retRecords.add(currBytes);
//                //                      }
//                //                      if (localDone) {
//                //                        entry = null;
//                //                        break outer;
//                //                      }
//                //                    }
//                //                  }
//                //                }
//                //              }
//                //            } else {
//                //              if (records.length > 2) {
//                //                logger.error("Records size: " + records.length);
//                //              }
//                //
//                //              byte[][] ret = applySelectToResultRecords(serializationVersion, dbName, columnOffsets, forceSelectOnServer, records,
//                //                  entry.getKey(), tableSchema, counters, groupContext, null, false);
//                //              if (counters == null) {
//                //                for (byte[] currBytes : ret) {
//                //                  boolean localDone = false;
//                //                  boolean include = true;
//                //                  long targetOffset = 1;
//                //                  currOffset.incrementAndGet();
//                //                  if (offset != null) {
//                //                    targetOffset = offset;
//                //                    if (currOffset.get() < offset) {
//                //                      include = false;
//                //                    }
//                //                  }
//                //                  if (include) {
//                //                    if (limit != null) {
//                //                      if (currOffset.get() >= targetOffset + limit) {
//                //                        include = false;
//                //                        localDone = true;
//                //                      }
//                //                    }
//                //                  }
//                //                  if (include) {
//                //                    retRecords.add(currBytes);
//                //                  }
//                //                  if (localDone) {
//                //                    entry = null;
//                //                    break outer;
//                //                  }
//                //                }
//                //              }
//                //            }
//                //          }
//
//              }
//            }
//            //        if (operator.equals(QueryEvaluator.BinaryRelationalOperator.Operator.equal)) {
//            //          entry = null;
//            //          break;
//            //        }
//
//          }
//
//
//
//          if (entry == null) {
//            break outer;
//          }
//          final int diff = Math.max(retRecords.size(), retKeyRecords.size());
//          if (count - diff <= 0) {
//            break outer;
//          }
//          if (ascending != null && !ascending) {
//            if (true) {
//              final AtomicInteger countRead = new AtomicInteger();
//              final List<MapEntry<Object[], Object>> currEntries = new ArrayList<>();
//              index.visitHeadMap(entry.getKey(), new Index.Visitor() {
//                @Override
//                public boolean visit(Object[] key, Object value) throws IOException {
//                  MapEntry<Object[], Object> curr = new MapEntry<>();
//                  curr.setKey(key);
//                  curr.setValue(value);
//                  currEntries.add(curr);
//                  if (countRead.incrementAndGet() >= count - diff) {
//                    return false;
//                  }
//                  return true;
//                }
//              });
//              entries = currEntries.toArray(new Map.Entry[currEntries.size()]);
//            }
//            else {
//              entries = index.lowerEntries((entry.getKey()), entries);
//            }
//          }
//          else {
//            if (true) {
//              final AtomicInteger countRead = new AtomicInteger();
//              final MapEntry<Object[], Object>[] currEntries = new MapEntry[count - diff];
//              final AtomicBoolean first = new AtomicBoolean(true);
//              index.visitTailMap(entry.getKey(), new Index.Visitor() {
//                @Override
//                public boolean visit(Object[] key, Object value) throws IOException {
//                  if (first.get()) {
//                    first.set(false);
//                    return true;
//                  }
//                  MapEntry<Object[], Object> curr = new MapEntry<>();
//                  curr.setKey(key);
//                  curr.setValue(value);
//                  currEntries[countRead.get()] = curr;
//                  if (countRead.incrementAndGet() >= count - diff) {
//                    return false;
//                  }
//                  return true;
//                }
//              });
//              entries = currEntries;
//            }
//            else {
//              entries = index.higherEntries(entry.getKey(), entries);
//            }
//          }
//          if (entries == null || entries.length == 0) {
//            entry = null;
//            break outer;
//          }
//
//
//
//
//

        }
      }
    }
    catch (Exception e) {
      e.printStackTrace();
      throw new DatabaseException(e);
    }
    return entry;
  }

  private byte[][] applySelectToResultRecords(short serializationVersion, String dbName, Set<Integer> columnOffsets, boolean forceSelectOnServer,
                                              byte[][] records, Object[] key,
                                              TableSchema tableSchema, Counter[] counters, GroupByContext groupContext,
                                              int[] keyOffsets, boolean keyContainsColumns) {
    if (columnOffsets == null || columnOffsets.size() == 0) {
      columnOffsets = null;
    }
    byte[][] ret = new byte[records.length][];
    for (int i = 0; i < records.length; i++) {
      byte[] recordBytes = records[i];
      ret[i] = recordBytes;

      Record record = null;
      if (counters != null || groupContext != null) {
        record = new Record(dbName, server.getCommon(), recordBytes, columnOffsets, false);
      }
      if (groupContext != null) {

        List<GroupByContext.FieldContext> fieldContexts = groupContext.getFieldContexts();
        Object[] groupValues = new Object[fieldContexts.size()];
        boolean isNull = true;
        for (int j = 0; j < groupValues.length; j++) {
          groupValues[j] = record.getFields()[fieldContexts.get(j).getFieldOffset()];
          if (groupValues[j] != null) {
            isNull = false;
          }
        }
        if (!isNull) {
          Map<String, Map<Object[], GroupByContext.GroupCounter>> map = groupContext.getGroupCounters();
          if (map == null || map.size() == 0) {
            groupContext.addGroupContext(groupValues);
            map = groupContext.getGroupCounters();
          }
          for (Map<Object[], GroupByContext.GroupCounter> innerMap : map.values()) {
            GroupByContext.GroupCounter counter = innerMap.get(groupValues);
            if (counter == null) {
              groupContext.addGroupContext(groupValues);
              counter = innerMap.get(groupValues);
            }
            counter.getCounter().add(record.getFields());
          }
        }
      }

      if (counters != null) {
        count(counters, record);
      }

      if (false && keyContainsColumns) {
        Record keyRecord = new Record(tableSchema);
        Object[] fields = new Object[tableSchema.getFields().size()];
        keyRecord.setFields(fields);
        for (int j = 0; j < keyOffsets.length; j++) {
          keyRecord.getFields()[keyOffsets[j]] = key[j];
        }
        ret[i] = keyRecord.serialize(server.getCommon(), serializationVersion);
      }
    }
    return ret;
  }

  private Map.Entry<Object[], Object> doIndexLookupOneKey(
      short serializationVersion,
      String dbName,
      final int count,
      TableSchema tableSchema,
      IndexSchema indexSchema,
      ParameterHandler parms,
      boolean evaluateExpresion,
      Expression expression,
      Set<Integer> columnOffsets,
      boolean forceSelectOnServer, List<Object[]> excludeKeys,
      Object[] originalKey,
      Object[] key,
      BinaryExpression.Operator operator,
      Index index,
      Boolean ascending,
      List<byte[]> retKeyRecords,
      List<Object[]> retKeys,
      List<byte[]> retRecords,
      long viewVersion,
      boolean keys,
      Counter[] counters,
      GroupByContext groupContext, AtomicLong currOffset, AtomicLong countReturned, Long limit, Long offset, int[] keyOffsets,
      boolean keyContainsColumns, Boolean isProbe, StoredProcedureContextImpl procedureContext) {
    Map.Entry<Object[], Object> entry = null;

    final AtomicInteger countSkipped = new AtomicInteger();

      if (originalKey == null || originalKey.length == 0) {
      originalKey = null;
    }
    if (originalKey != null) {
      boolean found = false;
      for (int i = 0; i < originalKey.length; i++) {
        if (originalKey[i] != null) {
          found = true;
        }
      }
      if (!found) {
        originalKey = null;
      }
    }
    //count = 3;
    if (originalKey != null && operator.equals(BinaryExpression.Operator.equal)) {
      if (originalKey == null) {
        return null;
      }

      boolean hasNull = false;
      for (Object part : originalKey) {
        if (part == null) {
          hasNull = true;
          break;
        }
      }
      List<Map.Entry<Object[], Object>> entries = null;
      if (!hasNull && originalKey.length == indexSchema.getFields().length &&
          (indexSchema.isPrimaryKey() || indexSchema.isUnique())) {
        byte[][] records = null;
        Object[][] currKeys = null;
        byte[][] currKeyRecords = null;
        Object value = null;

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
          //synchronized (index.getMutex(originalKey)) {
            value = index.get(originalKey);
            if (value != null && !value.equals(0L)) {
              if (keys) {
                if (keyContainsColumns) {
                  records = server.fromUnsafeToRecords(value);

                  MapEntry<Object[], Object> currEntry = new MapEntry<>();
                  currEntry.setKey(originalKey);
                  currEntry.setValue(value);
                  ProcessKeyContainsColumns processKeyContainsColumns = new ProcessKeyContainsColumns(serializationVersion, dbName, tableSchema,
                      indexSchema, parms, evaluateExpresion, expression, columnOffsets, forceSelectOnServer, index,
                      viewVersion, counters, groupContext, keyOffsets, keyContainsColumns, currEntry, currEntry, currKeyRecords,
                      records).invoke();
                  currKeys = processKeyContainsColumns.getCurrKeys();
                  currKeyRecords = processKeyContainsColumns.getCurrKeyRecords();
                  records = processKeyContainsColumns.getRecords();
                }
                else {
                  currKeyRecords = server.fromUnsafeToKeys(value);
                }
              }
              else {
                records = server.fromUnsafeToRecords(value);
              }
            }
          //}
          if (value != null) {
//          if (records != null && records[0].length < 300 && !keys && (true ||operator == BinaryExpression.Operator.equal)  &&
//              (parms == null || expression == null || !evaluateExpresion) && groupContext == null && counters == null) {
//            for (int i = 0; i < records.length; i++) {
//              retRecords.add(new Record(dbName, server.getCommon(), records[i], columnOffsets , true));
//            }
//          }
//          else {
            Object[] keyToUse = key;
            if (keyToUse == null) {
              keyToUse = originalKey;
            }

            AtomicBoolean done = new AtomicBoolean();
            handleRecord(serializationVersion, dbName, tableSchema, indexSchema, viewVersion, index, keyToUse, parms,
                evaluateExpresion, expression,
                columnOffsets, forceSelectOnServer, retKeyRecords, retKeys, retRecords, keys, counters, groupContext, records,
                currKeyRecords, currKeys, offset, currOffset, countReturned, limit, done, countSkipped, isProbe, procedureContext);
            if (done.get()) {
              return null;
            }

            //}
          }
        }

      }
      else {
        entries = index.equalsEntries(originalKey);
        if (entries != null) {
          for (Map.Entry<Object[], Object> currEntry : entries) {
            entry = currEntry;
            if (server.getCommon().compareKey(indexSchema.getComparators(), originalKey, entry.getKey()) != 0) {
              break;
            }
            Object value = entry.getValue();
            if (value == null) {
              break;
            }
            if (excludeKeys != null) {
              for (Object[] excludeKey : excludeKeys) {
                if (server.getCommon().compareKey(indexSchema.getComparators(), excludeKey, originalKey) == 0) {
                  return null;
                }
              }
            }
            byte[][] records = null;
            Object[][] currKeys = null;
            byte[][] currKeyRecords = null;


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
              //synchronized (index.getMutex(entry.getKey())) {
                //if (value instanceof Long) {
                //TODO: unsafe
                value = entry.getValue(); //index.get(entry.getKey());
                //}
                if (value != null && !value.equals(0L)) {
                  if (keys) {
                    if (keyContainsColumns) {
                      records = server.fromUnsafeToRecords(value);

                      ProcessKeyContainsColumns processKeyContainsColumns = new ProcessKeyContainsColumns(serializationVersion, dbName, tableSchema,
                          indexSchema, parms, evaluateExpresion, expression, columnOffsets, forceSelectOnServer, index,
                          viewVersion, counters, groupContext, keyOffsets, keyContainsColumns, currEntry, currEntry, currKeyRecords,
                          records).invoke();
                      //                    if (processKeyContainsColumns.is())
                      //                      break outer;
                      currKeys = processKeyContainsColumns.getCurrKeys();
                      currKeyRecords = processKeyContainsColumns.getCurrKeyRecords();
                      records = processKeyContainsColumns.getRecords();
                      //entry = processKeyContainsColumns.getEntry();
                    }
                    else {
                      currKeyRecords = server.fromUnsafeToKeys(value);
                    }
                  }
                  else {
                    records = server.fromUnsafeToRecords(value);
                  }
                }
              //}
//            if (records != null && records[0].length < 300 && !keys && (true || operator == BinaryExpression.Operator.equal) &&
//                (parms == null || expression == null || !evaluateExpresion) && groupContext == null && counters == null) {
//              for (int i = 0; i < records.length; i++) {
//                retRecords.add(new Record(dbName, server.getCommon(), records[i], columnOffsets , true));
//              }
//            }
//            else {
              Object[] keyToUse = key;
              if (keyToUse == null) {
                keyToUse = originalKey;
              }
              if (value != null) {
                AtomicBoolean done = new AtomicBoolean();
                handleRecord(serializationVersion, dbName, tableSchema, indexSchema, viewVersion, index, keyToUse, parms, evaluateExpresion,
                    expression, columnOffsets, forceSelectOnServer, retKeyRecords, retKeys, retRecords, keys, counters,
                    groupContext, records, currKeyRecords, currKeys, offset, currOffset, countReturned, limit, done, countSkipped, isProbe, procedureContext);
                if (done.get()) {
                  entry = null;
                  return null;
                }
              }
            }
            //            }
          }
        }
      }
      entry = null;
    }
    else if ((ascending == null || ascending)) {
      if (key == null) {
        if (originalKey == null) {
          entry = index.firstEntry();
        }
        else if (operator.equals(BinaryExpression.Operator.greater) || operator.equals(BinaryExpression.Operator.greaterEqual)) {
          entry = index.floorEntry(originalKey);
          if (entry == null) {
            entry = index.firstEntry();
          }
        }
        else if (operator.equals(BinaryExpression.Operator.less) || operator.equals(BinaryExpression.Operator.lessEqual)) {
          entry = index.firstEntry();
        }
      }
      else {
        //entry = index.floorEntry(key);
        //        if (operator.equals(BinaryExpression.Operator.greater) ||
        //             operator.equals(BinaryExpression.Operator.greaterEqual)) {
        entry = index.ceilingEntry(key);
        if (entry == null) {
          entry = index.firstEntry();
        }
      }
      if (entry != null) {

        if (operator.equals(BinaryExpression.Operator.less) ||
            operator.equals(BinaryExpression.Operator.lessEqual) ||
            operator.equals(BinaryExpression.Operator.greater) ||
            operator.equals(BinaryExpression.Operator.greaterEqual)) {
          boolean foundMatch = key != null && 0 == server.getCommon().compareKey(indexSchema.getComparators(), entry.getKey(), key);
          if (foundMatch) {
            //todo: match below
            entry = index.higherEntry((entry.getKey()));
          }
          else if (operator.equals(BinaryExpression.Operator.less) ||
              operator.equals(BinaryExpression.Operator.greater)) {
            foundMatch = originalKey != null && 0 == server.getCommon().compareKey(indexSchema.getComparators(), entry.getKey(), originalKey);
            if (foundMatch) {
              //todo: match below
              entry = index.higherEntry((entry.getKey()));
            }
          }
        }

      }
    }
    else {
      //        if (key[0] == indexSchema.getCurrPartitions()[shard].getUpperKey()) {
      //
      //        }
      if (key == null) {
        if (originalKey == null) {
          entry = index.lastEntry();
        }
        else {
          if (ascending != null && !ascending && originalKey != null &&
              (operator.equals(BinaryExpression.Operator.less) || operator.equals(BinaryExpression.Operator.lessEqual))) {
            entry = index.floorEntry(originalKey);
            if (entry == null) {
              entry = index.lastEntry();
            }
          }
          else if (ascending != null && !ascending && originalKey != null &&
              (operator.equals(BinaryExpression.Operator.greater) || operator.equals(BinaryExpression.Operator.greaterEqual))) {
            //entry = index.ceilingEntry(originalKey);
            if (entry == null) {
              entry = index.lastEntry();
            }
          }
        }
      }
      else {
        //  if (ascending != null && !ascending &&
        //      (key == null || operator.equals(BinaryExpression.Operator.greater) ||
        //          operator.equals(BinaryExpression.Operator.greaterEqual))) {
        //    entry = index.lastEntry();
        //  }
        //  else {
        //    if (key == null) {
        //     entry = index.firstEntry();
        //   }
        //   else {
        entry = index.floorEntry(key);
        if (entry == null) {
          entry = index.lastEntry();
        }



        if (operator.equals(BinaryExpression.Operator.less) ||
            operator.equals(BinaryExpression.Operator.lessEqual) ||
            operator.equals(BinaryExpression.Operator.greater) ||
            operator.equals(BinaryExpression.Operator.greaterEqual)) {
          boolean foundMatch = key != null && 0 == server.getCommon().compareKey(indexSchema.getComparators(), entry.getKey(), key);
          if (foundMatch) {
            //todo: match below
            entry = index.lowerEntry((entry.getKey()));
          }
          else if (operator.equals(BinaryExpression.Operator.less) ||
              operator.equals(BinaryExpression.Operator.greater)) {
            foundMatch = originalKey != null && 0 == server.getCommon().compareKey(indexSchema.getComparators(), entry.getKey(), originalKey);
            if (foundMatch) {
              //todo: match below
              entry = index.lowerEntry((entry.getKey()));
            }
          }
        }

        //   }

        //}
      }
    }
    if (entry != null) {
      if ((ascending != null && !ascending)) {
        if (key != null && (operator.equals(BinaryExpression.Operator.less) ||
            operator.equals(BinaryExpression.Operator.lessEqual) || operator.equals(BinaryExpression.Operator.greaterEqual))) {
          boolean foundMatch = key != null && 0 == server.getCommon().compareKey(indexSchema.getComparators(), entry.getKey(), key);
          if (foundMatch) {
            //todo: match below
            entry = index.lowerEntry((entry.getKey()));
          }
        }
        else if (operator.equals(BinaryExpression.Operator.less) || operator.equals(BinaryExpression.Operator.greater)) {
          boolean foundMatch = originalKey != null && 0 == server.getCommon().compareKey(indexSchema.getComparators(), entry.getKey(), originalKey);
          if (foundMatch) {
            //todo: match below
            entry = index.lowerEntry((entry.getKey()));
          }
        }
      }
      else {
        if (key != null && (operator.equals(BinaryExpression.Operator.greater) ||
            operator.equals(BinaryExpression.Operator.greaterEqual))) {
          while (entry != null && key != null) {
            int compare = server.getCommon().compareKey(indexSchema.getComparators(), entry.getKey(), key);
            if (compare <= 0) {
              entry = index.higherEntry(entry.getKey());
            }
            else {
              break;
            }
          }
        }
        else if (operator.equals(BinaryExpression.Operator.greaterEqual)) {
          while (entry != null && key != null) {
            int compare = server.getCommon().compareKey(indexSchema.getComparators(), entry.getKey(), key);
            if (compare < 0) {
              entry = index.higherEntry(entry.getKey());
            }
            else {
              break;
            }
          }
        }
//        else if (operator.equals(BinaryExpression.Operator.less)) {
//          while (key != null) {
//            int compare = server.getCommon().compareKey(indexSchema.getComparators(), entry.getKey(), key);
//            if (foundMatch) {
//              entry = index.higherEntry((entry.getKey()));
//            }
//          }
//        }
      }

      Map.Entry[] entries = new Map.Entry[]{entry};

      outer:
      while (true) {
        if (retRecords.size() >= count || retKeyRecords.size() >= count) {
          break;
        }
        for (Map.Entry<Object[], Object> currEntry : entries) {
          entry = currEntry;

//          if (indexSchema.getLastPartitions() != null && indexSchema.getLastPartitions()[server.getShard()].getUpperKey() != null &&
//              DatabaseCommon.compareKey(indexSchema.getComparators(),
//                  indexSchema.getLastPartitions()[server.getShard()].getUpperKey(), entry.getKey()) == -1) {
//            entry = null;
//            break outer;
//          }

          if (currEntry == null) {
            break outer;
          }
          if (originalKey != null) {
             int compare = server.getCommon().compareKey(indexSchema.getComparators(), currEntry.getKey(), originalKey);
            if (compare == 0 &&
                (operator.equals(BinaryExpression.Operator.less) || operator.equals(BinaryExpression.Operator.greater))) {
              entry = null;
              break outer;
            }
            if (compare == 1 && (ascending == null || ascending) && operator.equals(BinaryExpression.Operator.lessEqual)) {
              entry = null;
              break outer;
            }
            if (compare == 1 && (ascending == null || ascending) && operator.equals(BinaryExpression.Operator.less)) {
              entry = null;
              break outer;
            }
            if (compare == -1 && (ascending != null && !ascending) && operator.equals(BinaryExpression.Operator.greaterEqual)) {
              entry = null;
              break outer;
            }
            if (compare == -1 && (ascending != null && !ascending) && operator.equals(BinaryExpression.Operator.greater)) {
              entry = null;
              break outer;
            }
          }

          if (excludeKeys != null) {
            for (Object[] excludeKey : excludeKeys) {
              if (server.getCommon().compareKey(indexSchema.getComparators(), excludeKey, key) == 0) {
                continue outer;
              }
            }
          }

          Object[][] currKeys = null;
          byte[][] currKeyRecords = null;
          byte[][] records = null;

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
            //synchronized (index.getMutex(currEntry.getKey())) {
              //if (currEntry.getValue() instanceof Long) {
              //TODO: unsafe
              //currEntry.setValue(index.get(currEntry.getKey()));
              //}
              if (keys) {
                Object unsafeAddress = currEntry.getValue();//index.get(entry.getKey());
                if (unsafeAddress != null && !unsafeAddress.equals(0L)) {
                  if (keyContainsColumns) {
                    records = server.fromUnsafeToRecords(currEntry.getValue());
                    ProcessKeyContainsColumns processKeyContainsColumns = new ProcessKeyContainsColumns(serializationVersion, dbName, tableSchema,
                        indexSchema, parms, evaluateExpresion, expression, columnOffsets, forceSelectOnServer, index,
                        viewVersion, counters, groupContext, keyOffsets, keyContainsColumns, entry, currEntry, currKeyRecords,
                        records).invoke();
                    if (processKeyContainsColumns.is())
                      break outer;
                    currKeys = processKeyContainsColumns.getCurrKeys();
                    currKeyRecords = processKeyContainsColumns.getCurrKeyRecords();
                    records = processKeyContainsColumns.getRecords();
                    entry = processKeyContainsColumns.getEntry();
                  }
                  else {
                    currKeyRecords = server.fromUnsafeToKeys(unsafeAddress);
                  }
                }
              }
              else {
                Object unsafeAddress = currEntry.getValue();//index.get(entry.getKey());
                if (unsafeAddress != null && !unsafeAddress.equals(0L)) {
                  records = server.fromUnsafeToRecords(unsafeAddress);
//                  while (records == null) {
//                    try {
//                      Thread.sleep(100);
//                      System.out.println("null records ************************************");
//                    }
//                    catch (InterruptedException e) {
//                      throw new DatabaseException(e);
//                    }
//                    currEntry.setValue(index.get(currEntry.getKey()));
//                    unsafeAddress = currEntry.getValue();//index.get(entry.getKey());
//                    if (unsafeAddress != null && !unsafeAddress.equals(0L)) {
//                      records = server.fromUnsafeToRecords(unsafeAddress);
//                    }
//                    else {
//                      break;
//                    }
//                  }
                }
              }
            //}
            Object[] keyToUse = currEntry.getKey(); //key
            if (keyToUse == null) {
              keyToUse = originalKey;
            }
            if (currEntry.getValue() != null) {
              AtomicBoolean done = new AtomicBoolean();
              handleRecord(serializationVersion, dbName, tableSchema, indexSchema, viewVersion, index, keyToUse, parms, evaluateExpresion,
                  expression, columnOffsets, forceSelectOnServer, retKeyRecords, retKeys, retRecords, keys, counters, groupContext,
                  records, currKeyRecords, currKeys, offset, currOffset, countReturned, limit, done, countSkipped, isProbe, procedureContext);
              if (done.get()) {
                entry = null;
                break outer;
              }
            }
          }

          //entry = null;
          if (operator.equals(BinaryExpression.Operator.equal)) {
            entry = null;
            break outer;
          }
        }
//        if ((ascending == null || ascending) &&
//            (operator.equals(BinaryExpression.Operator.less) ||
//                operator.equals(BinaryExpression.Operator.lessEqual))) {
//          entry = index.lowerEntry((entry.getKey()));
//        }
//        else {
        if (entry == null) {
          break outer;
        }
        final int diff = Math.max(retRecords.size(), retKeyRecords.size());
        if (count - diff <= 0) {
          break outer;
        }
        if (ascending != null && !ascending) {
          if (true) {
            final AtomicInteger countRead = new AtomicInteger();
            final List<MapEntry<Object[], Object>> currEntries = new ArrayList<>();
            index.visitHeadMap(entry.getKey(), new Index.Visitor() {
              @Override
              public boolean visit(Object[] key, Object value) throws IOException {
                MapEntry<Object[], Object> curr = new MapEntry<>();
                curr.setKey(key);
                curr.setValue(value);
                currEntries.add(curr);
                if (countRead.incrementAndGet() >= count - diff) {
                  return false;
                }
                return true;
              }
            });
            entries = currEntries.toArray(new Map.Entry[currEntries.size()]);
          }
          else {
            entries = index.lowerEntries((entry.getKey()), entries);
          }
        }
        else {
          if (true) {
            final AtomicInteger countRead = new AtomicInteger();
            final List<MapEntry<Object[], Object>> currEntries = new ArrayList<>();
            final AtomicBoolean first = new AtomicBoolean(true);
            index.visitTailMap(entry.getKey(), new Index.Visitor() {
              @Override
              public boolean visit(Object[] key, Object value) throws IOException {
                if (first.get()) {
                  first.set(false);
                  return true;
                }
                MapEntry<Object[], Object> curr = new MapEntry<>();
                curr.setKey(key);
                curr.setValue(value);
                currEntries.add(curr);
                if (countRead.incrementAndGet() >= count - diff) {
                  return false;
                }
                return true;
              }
            });
            entries = currEntries.toArray(new Map.Entry[currEntries.size()]);
          }
          else {
            entries = index.higherEntries(entry.getKey(), entries);
          }
        }
        if (entries == null || entries.length == 0) {
          entry = null;
          break outer;
        }
        //}
      }
    }
    return entry;
  }

  private byte[][] processViewFlags(String dbName, TableSchema tableSchema, IndexSchema indexSchema, Index index,
                                    long viewVersion, Object[] key, byte[][] records, AtomicBoolean done) {
    if (records == null) {
      //System.out.println("null records *******************");
    }
    else {
      if (indexSchema == null || server.getIndexSchema(dbName, tableSchema.getName(), indexSchema.getName()).getLastPartitions() != null) {
        List<byte[]> remaining = new ArrayList<>();
        for (byte[] bytes : records) {
          if (!processViewFlags(viewVersion, remaining, bytes)) {
            //done.set(true);
            return null;
          }
        }
        if (remaining.size() == 0) {
          records = null;
        }
        else {
          records = remaining.toArray(new byte[remaining.size()][]);
        }
      }
      else {
        List<byte[]> remaining = new ArrayList<>();
        if (records != null) {
          for (byte[] bytes : records) {
            if (!processViewFlags(viewVersion, remaining, bytes)) {
              //done.set(true);
              return null;
            }
//            else {
//              remaining.add(bytes);
//            }
//          else if ((dbViewFlags & Record.DB_VIEW_FLAG_DELETING) != 0) {
//            synchronized (index.getMutex(key)) {
//              Object unsafeAddress = index.remove(key);
//              if (unsafeAddress != null && !unsafeAddress.equals(0L)) {
//                server.freeUnsafeIds(unsafeAddress);
//              }
//            }
//          }
          }
          if (remaining.size() == 0) {
            records = null;
          }
          else {
            records = remaining.toArray(new byte[remaining.size()][]);
          }
        }
      }
    }
    return records;
  }

  private boolean processViewFlags(long viewVersion, List<byte[]> remaining, byte[] bytes) {
    long dbViewNum = Record.getDbViewNumber(bytes);
    long dbViewFlags = Record.getDbViewFlags(bytes);
//                    if (dbViewNum > viewVersion && (dbViewFlags & Record.DB_VIEW_FLAG_ADDING) != 0) {
//
//                    }
//                    else
//    remaining.add(bytes);
    if ((dbViewNum <= viewVersion - 1) && (dbViewFlags & Record.DB_VIEW_FLAG_ADDING) != 0) {
      remaining.add(bytes);
    }
    else if ((dbViewNum == viewVersion || dbViewNum == viewVersion - 1) && (dbViewFlags & Record.DB_VIEW_FLAG_DELETING) != 0) {
      remaining.add(bytes);
    }
    else if ((dbViewFlags & Record.DB_VIEW_FLAG_DELETING) == 0) {
      remaining.add(bytes);
    }
    else {
      return false;
    }
    return true;
  }

  private boolean handleRecord(short serializationVersion, String dbName, TableSchema tableSchema, IndexSchema indexSchema,
                               long viewVersion,
                               Index index, Object[] key, ParameterHandler parms, boolean evaluateExpresion,
                               Expression expression, Set<Integer> columnOffsets, boolean forceSelectOnServer,
                               List<byte[]> retKeyRecords, List<Object[]> retKeys, List<byte[]> retRecords, boolean keys,
                               Counter[] counters,
                               GroupByContext groupContext, byte[][] records, byte[][] currKeyRecords,
                               Object[][] currKeys, Long offset,
                               AtomicLong currOffset, AtomicLong countReturned, Long limit, AtomicBoolean done, AtomicInteger countSkipped,
                               boolean isProbe, StoredProcedureContextImpl procedureContext) {
    if (keys) {
      if (currKeyRecords != null) {

        for (byte[] currKeyRecord : currKeyRecords) {
          done.set(false);
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
              if (countReturned.get() >= limit) {
                include = false;
                done.set(true);
              }
            }
          }
          if (include) {
            boolean passesFlags = false;
            long dbViewNum = KeyRecord.getDbViewNumber(currKeyRecord);
            long dbViewFlags = KeyRecord.getDbViewFlags(currKeyRecord);
            if ((dbViewNum <= viewVersion - 1) && (dbViewFlags & Record.DB_VIEW_FLAG_ADDING) != 0) {
              passesFlags = true;
            }
            else if ((dbViewNum == viewVersion || dbViewNum == viewVersion - 1) && (dbViewFlags & Record.DB_VIEW_FLAG_DELETING) != 0) {
              passesFlags = true;
            }
            else if ((dbViewFlags & Record.DB_VIEW_FLAG_DELETING) == 0) {
              passesFlags = true;
            }
            if (passesFlags) {
              boolean shouldAdd = true;
//              if (isProbe) {
//                if (countSkipped.incrementAndGet() < OPTIMIZED_RANGE_PAGE_SIZE) {
//                  shouldAdd = false;
//                }
//                else {
//                  countSkipped.set(0);
//                }
//              }
              if (shouldAdd) {
                if (currKeys != null) {
                  for (Object[] currKey : currKeys) {
                    retKeys.add(currKey);
                  }
                }
                else {
                  retKeys.add(key);
                }
//                Object[] keyObj =null;
//                try {
//                  KeyRecord keyRecord = new KeyRecord(currKeyRecord);
//                  keyObj = DatabaseCommon.deserializeKey(tableSchema, keyRecord.getPrimaryKey());
//                }
//                catch (Exception e) {
//                  e.printStackTrace();
//                }
                retKeyRecords.add(currKeyRecord);
                countReturned.incrementAndGet();
                retKeyRecords.add(currKeyRecord);
              }
            }
            else {
              currOffset.decrementAndGet();
            }
          }
          if (done.get()) {
            return true;
          }
        }
      }
    }
    else {
      List<byte[]> remaining = new ArrayList<>();
      if (records == null) {
        //System.out.println("null records *******************");
      }
      else {
        for (byte[] bytes : records) {
          if (!processViewFlags(viewVersion, remaining, bytes)) {
            return false;
          }
        }
        if (remaining.size() == 0) {
          records = null;
        }
        else {
          records = remaining.toArray(new byte[remaining.size()][]);
        }
      }

      if (records != null) {
        if (parms != null && expression != null && evaluateExpresion) {
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
                  com.sonicbase.procedure.RecordImpl procedureRecord = new RecordImpl();
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
                int[] keyOffsets = null;
                boolean keyContainsColumns = false;

                byte[][] currRecords = new byte[][]{bytes};
                byte[][] ret = applySelectToResultRecords(serializationVersion, dbName, columnOffsets, forceSelectOnServer, currRecords,
                    null, tableSchema, counters, groupContext, keyOffsets, keyContainsColumns);
                if (counters == null) {
                  for (byte[] currBytes : ret) {
                    done.set(false);
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
                        if (countReturned.get() >= limit) {
                          include = false;
                          done.set(true);
                        }
                      }
                    }
                    if (include) {
                      countReturned.incrementAndGet();
                      retRecords.add(currBytes);
                    }
                    if (done.get()) {
                      return true;
                    }
                  }
                }
              }
            }
          }
        }
        else {
          int[] keyOffsets = null;
          boolean keyContainsColumns = false;

          byte[][] ret = applySelectToResultRecords(serializationVersion, dbName, columnOffsets, forceSelectOnServer, records, null,
              tableSchema, counters, groupContext, keyOffsets, keyContainsColumns);
          boolean pass = true;
          if (procedureContext != null) {
            if (procedureContext.getRecordEvaluator() == null) {
              pass = true;
            }
            else {
              for (byte[] bytes : records) {
                Record record = new Record(tableSchema);
                record.deserialize(dbName, server.getCommon(), bytes, null, true);
                com.sonicbase.procedure.RecordImpl procedureRecord = new RecordImpl();
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
          }
          if (pass) {
            if (counters == null) {
              for (byte[] currBytes : ret) {
                done.set(false);
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
                    if (countReturned.get() >= limit) {
                      include = false;
                      done.set(true);
                    }
                  }
                }
                if (include) {
                  countReturned.incrementAndGet();
                  retRecords.add(currBytes);
                }
                if (done.get()) {
                  return true;
                }
              }
            }
          }
        }
      }
    }
    return true;
  }

  private void count(Counter[] counters, Record record) {
    if (counters != null && record != null) {
      for (Counter counter : counters) {
        counter.add(record.getFields());
      }
    }
  }

  public ComObject evaluateCounterGetKeys(ComObject cobj) {

    String dbName = cobj.getString(ComObject.Tag.dbName);

    Counter counter = new Counter();
    try {
      byte[] counterBytes = cobj.getByteArray(ComObject.Tag.legacyCounter);
      counter.deserialize(counterBytes);

      boolean isPrimaryKey = false;
      String tableName = counter.getTableName();
      String columnName = counter.getColumnName();
      String indexName = null;
      TableSchema tableSchema = server.getCommon().getTableSchema(dbName, tableName, server.getDataDir());
      for (IndexSchema indexSchema : tableSchema.getIndexes().values()) {
        if (indexSchema.getFields()[0].equals(columnName)) {
          isPrimaryKey = indexSchema.isPrimaryKey();
          indexName = indexSchema.getName();
          //break;
        }
      }
      byte[] maxKey = null;
      byte[] minKey = null;
      Index index = server.getIndex(dbName, tableName, indexName);
      Map.Entry<Object[], Object> entry = index.lastEntry();
      if (entry != null) {
        byte[][] records = null;
        //synchronized (index.getMutex(entry.getKey())) {
          Object unsafeAddress = entry.getValue();
          //if (unsafeAddress instanceof Long) {
          //TODO: unsafe
           // unsafeAddress = index.get(entry.getKey());
          //}
          if (unsafeAddress != null && !unsafeAddress.equals(0L)) {
            records = server.fromUnsafeToRecords(unsafeAddress);
          }
        //}
        if (records != null) {
          if (isPrimaryKey) {
            maxKey = DatabaseCommon.serializeKey(tableSchema, indexName, entry.getKey());
          }
          else {
            KeyRecord keyRecord = new KeyRecord(records[0]);
            maxKey = keyRecord.getPrimaryKey();
          }
        }
      }
      entry = index.firstEntry();
      if (entry != null) {
        byte[][] records = null;
        //synchronized (index.getMutex(entry.getKey())) {
          Object unsafeAddress = entry.getValue();
          //if (unsafeAddress instanceof Long) {
          //TODO: unsafe
           // unsafeAddress = index.get(entry.getKey());
          //}
          if (unsafeAddress != null && !unsafeAddress.equals(0L)) {
            records = server.fromUnsafeToRecords(unsafeAddress);
          }
        //}
        if (records != null) {
          if (isPrimaryKey) {
            minKey = DatabaseCommon.serializeKey(tableSchema, indexName, entry.getKey());
          }
          else {
            KeyRecord keyRecord = new KeyRecord(records[0]);
            minKey = keyRecord.getPrimaryKey();
          }
        }
      }
      if (minKey == null || maxKey == null) {
        logger.error("minkey==null || maxkey==null");
      }
      ComObject retObj = new ComObject();
      if (minKey != null) {
        retObj.put(ComObject.Tag.minKey, minKey);
      }
      if (maxKey != null) {
        retObj.put(ComObject.Tag.maxKey, maxKey);
      }
      retObj.put(ComObject.Tag.legacyCounter, counter.serialize());
      return retObj;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }


  public ComObject evaluateCounterWithRecord(ComObject cobj) {

    String dbName = cobj.getString(ComObject.Tag.dbName);

    Counter counter = new Counter();
    try {
      byte[] minKeyBytes = cobj.getByteArray(ComObject.Tag.minKey);
      byte[] maxKeyBytes = cobj.getByteArray(ComObject.Tag.maxKey);
      byte[] counterBytes = cobj.getByteArray(ComObject.Tag.legacyCounter);
      counter.deserialize(counterBytes);

      String tableName = counter.getTableName();
      String columnName = counter.getColumnName();
      String indexName = null;
      TableSchema tableSchema = server.getCommon().getTableSchema(dbName, tableName, server.getDataDir());
      for (IndexSchema indexSchema : tableSchema.getIndexes().values()) {
        if (indexSchema.isPrimaryKey()) {
          indexName = indexSchema.getName();
        }
      }
      byte[] keyBytes = minKeyBytes;
      if (minKeyBytes == null) {
        keyBytes = maxKeyBytes;
      }
      Object[] key = DatabaseCommon.deserializeKey(tableSchema, keyBytes);

      Index index = server.getIndex(dbName, tableName, indexName);
      //synchronized (index.getMutex(key)) {
        Object unsafeAddress = index.get(key);
        if (unsafeAddress != null) {
          byte[][] records = null;
          if (unsafeAddress != null && !unsafeAddress.equals(0L)) {
            records = server.fromUnsafeToRecords(unsafeAddress);
          }
          if (records != null) {
            Record record = new Record(dbName, server.getCommon(), records[0]);
            Object value = record.getFields()[tableSchema.getFieldOffset(columnName)];
            if (minKeyBytes != null) {
              if (counter.isDestTypeLong()) {
                counter.setMinLong((Long) DataType.getLongConverter().convert(value));
              }
              else {
                counter.setMinDouble((Double) DataType.getDoubleConverter().convert(value));
              }
            }
            else {
              if (counter.isDestTypeLong()) {
                counter.setMaxLong((Long) DataType.getLongConverter().convert(value));
              }
              else {
                counter.setMaxDouble((Double) DataType.getDoubleConverter().convert(value));
              }
            }
          }
        }
      //}
      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.legacyCounter, counter.serialize());
      return retObj;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  private class ProcessKeyContainsColumns {
    private final short serializationVersion;
    private boolean myResult;
    private String dbName;
    private TableSchema tableSchema;
    private IndexSchema indexSchema;
    private ParameterHandler parms;
    private boolean evaluateExpresion;
    private Expression expression;
    private Set<Integer> columnOffsets;
    private boolean forceSelectOnServer;
    private Index index;
    private long viewVersion;
    private Counter[] counters;
    private GroupByContext groupContext;
    private int[] keyOffsets;
    private boolean keyContainsColumns;
    private Map.Entry<Object[], Object> entry;
    private Map.Entry<Object[], Object> currEntry;
    private byte[][] currKeyRecords;
    private Object[][] currKeys;
    private byte[][] records;

    public ProcessKeyContainsColumns(short serializationVersion, String dbName, TableSchema tableSchema, IndexSchema indexSchema,
                                     ParameterHandler parms, boolean evaluateExpresion, Expression expression,
                                     Set<Integer> columnOffsets, boolean forceSelectOnServer, Index index,
                                     long viewVersion, Counter[] counters, GroupByContext groupContext, int[] keyOffsets,
                                     boolean keyContainsColumns, Map.Entry<Object[], Object> entry,
                                     Map.Entry<Object[], Object> currEntry, byte[][] currKeys, byte[]... records) {
      this.serializationVersion = serializationVersion;
      this.dbName = dbName;
      this.tableSchema = tableSchema;
      this.indexSchema = indexSchema;
      this.parms = parms;
      this.evaluateExpresion = evaluateExpresion;
      this.expression = expression;
      this.columnOffsets = columnOffsets;
      this.forceSelectOnServer = forceSelectOnServer;
      this.index = index;
      this.viewVersion = viewVersion;
      this.counters = counters;
      this.groupContext = groupContext;
      this.keyOffsets = keyOffsets;
      this.keyContainsColumns = keyContainsColumns;
      this.entry = entry;
      this.currEntry = currEntry;
      this.currKeyRecords = currKeys;
      this.records = records;
    }

    boolean is() {
      return myResult;
    }

    public Map.Entry<Object[], Object> getEntry() {
      return entry;
    }

    public byte[][] getCurrKeyRecords() {
      return currKeyRecords;
    }

    public byte[][] getRecords() {
      return records;
    }

    public ProcessKeyContainsColumns invoke() {
      Object unsafeAddress;
      Object[] currKey = new Object[currEntry.getKey().length];
      for (int i = 0; i < currKey.length; i++) {
        currKey[i] = currEntry.getKey()[i];
      }

      unsafeAddress = currEntry.getValue();
      if (unsafeAddress != null && !unsafeAddress.equals(0L)) {
        records = server.fromUnsafeToRecords(unsafeAddress);
      }

      boolean shouldInclude = true;
      AtomicBoolean done = new AtomicBoolean();
      records = processViewFlags(dbName, tableSchema, indexSchema, index, viewVersion, currEntry.getKey(), records, done);
      if (records == null) {
        if (done.get()) {
          entry = null;
          myResult = true;
          return this;
        }
        shouldInclude = false;
      }
      else {
        if (parms != null && expression != null && evaluateExpresion) {
          for (byte[] bytes : records) {
            Record record = new Record(tableSchema);
            record.deserialize(dbName, server.getCommon(), bytes, null);
            boolean pass = (Boolean) ((ExpressionImpl) expression).evaluateSingleRecord(new TableSchema[]{tableSchema}, new Record[]{record}, parms);
            if (pass) {
              byte[][] currRecords = new byte[][]{bytes};
              byte[][] ret = applySelectToResultRecords(serializationVersion, dbName, columnOffsets, forceSelectOnServer, currRecords,
                  null, tableSchema, counters, groupContext, keyOffsets, keyContainsColumns);
              if (ret == null || ret.length == 0) {
                shouldInclude = false;
              }
            }
            else {
              shouldInclude = false;
            }
          }
        }
        else {
          if (records.length > 2) {
            logger.error("Records size: " + records.length);
          }
          if (records != null) {
            byte[][] ret = applySelectToResultRecords(serializationVersion, dbName, columnOffsets, forceSelectOnServer, records,
                entry.getKey(), tableSchema, counters, groupContext, keyOffsets, keyContainsColumns);
            if (ret == null || ret.length == 0) {
              shouldInclude = false;
            }
          }
        }
      }

      if (shouldInclude && records != null && records.length != 0) {
        currKeyRecords = new byte[1][];
        currKeyRecords = records; //currKeyRecords[0] = DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), currKey);
        currKeys = new Object[1][];
        currKeys[0] = currKey; //DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), currKey);
      }
      myResult = false;
      return this;
    }

    public Object[][] getCurrKeys() {
      return currKeys;
    }
  }
}
