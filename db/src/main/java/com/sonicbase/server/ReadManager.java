package com.sonicbase.server;

import com.amazonaws.transform.MapEntry;
import com.codahale.metrics.MetricRegistry;
import com.sonicbase.common.*;
import com.sonicbase.index.Index;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.query.BinaryExpression;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.query.Expression;
import com.sonicbase.query.impl.*;
import com.sonicbase.schema.DataType;
import com.sonicbase.schema.FieldSchema;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.DataUtil;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Responsible for
 */
public class ReadManager {

  private Logger logger;

  private final DatabaseServer server;
  private Thread preparedReaper;
  private Thread diskReaper;

  public ReadManager(DatabaseServer databaseServer) {

    this.server = databaseServer;
    this.logger = new Logger(databaseServer.getDatabaseClient());

    new java.util.Timer().scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        logger.info("IndexLookup stats: count=" + INDEX_LOOKUP_STATS.getCount() + ", rate=" + INDEX_LOOKUP_STATS.getFiveMinuteRate() +
            ", durationAvg=" + INDEX_LOOKUP_STATS.getSnapshot().getMean() / 1000000d +
            ", duration99.9=" + INDEX_LOOKUP_STATS.getSnapshot().get999thPercentile() / 1000000d);
        logger.info("BatchIndexLookup stats: count=" + BATCH_INDEX_LOOKUP_STATS.getCount() + ", rate=" + BATCH_INDEX_LOOKUP_STATS.getFiveMinuteRate() +
            ", durationAvg=" + BATCH_INDEX_LOOKUP_STATS.getSnapshot().getMean() / 1000000d +
            ", duration99.9=" + BATCH_INDEX_LOOKUP_STATS.getSnapshot().get999thPercentile() / 1000000d);
      }
    }, 20 * 1000, 20 * 1000);

    startPreparedReaper();

    startDiskResultsReaper();
  }

  private void startDiskResultsReaper() {
    diskReaper = new Thread(new Runnable(){
      @Override
      public void run() {
        while (true) {
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
    });
    diskReaper.start();
  }


  public static final int SELECT_PAGE_SIZE = 1000;

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
    long schemaVersion = cobj.getLong(ComObject.Tag.schemaVersion);
    if (schemaVersion < server.getSchemaVersion()) {
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
    for (Map.Entry<String, IndexSchema> entry : server.getCommon().getTables(dbName).get(fromTable).getIndexes().entrySet()) {
      if (entry.getValue().isPrimaryKey()) {
        primaryKeyIndex = entry.getValue().getName();
        break;
      }
    }
    TableSchema tableSchema = server.getCommon().getTables(dbName).get(fromTable);
    Index index = server.getIndices(dbName).getIndices().get(fromTable).get(primaryKeyIndex);

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
      count = index.size();
    }
    else {
      Map.Entry<Object[], Object> entry = index.firstEntry();
      while (true) {
        if (entry == null) {
          break;
        }
        byte[][] records = null;
        synchronized (index.getMutex(entry.getKey())) {
          //if (entry.getValue() instanceof Long) {
            entry.setValue(index.get(entry.getKey()));
          //}
          if (entry.getValue() != null && !entry.getValue().equals(0L)) {
            records = server.fromUnsafeToRecords(entry.getValue());
          }
        }
        for (byte[] bytes : records) {
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
      long schemaVersion = cobj.getLong(ComObject.Tag.schemaVersion);
      if (schemaVersion < server.getSchemaVersion()) {
        throw new SchemaOutOfSyncException("currVer:" + server.getCommon().getSchemaVersion() + ":");
      }
      int count = cobj.getInt(ComObject.Tag.count);
      long serializationVersion = cobj.getLong(ComObject.Tag.serializationVersion);
      String tableName = cobj.getString(ComObject.Tag.tableName);
      String indexName = cobj.getString(ComObject.Tag.indexName);

      TableSchema tableSchema = server.getCommon().getSchema(dbName).getTables().get(tableName);
      IndexSchema indexSchema = tableSchema.getIndices().get(indexName);
      DataUtil.ResultLength resultLength = new DataUtil.ResultLength();

      Index index = server.getIndices(dbName).getIndices().get(tableSchema.getName()).get(indexName);
      Boolean ascending = null;

      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.serializationVersion, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);

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
          primaryKeyIndex = server.getIndices(dbName).getIndices().get(tableSchema.getName()).get(entry.getKey());
        }
      }

      //out.writeInt(SNAPSHOT_SERIALIZATION_VERSION);

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

        List<byte[]> retKeys = new ArrayList<>();
        List<byte[]> retRecords = new ArrayList<>();

        boolean forceSelectOnServer = false;
        if (indexSchema.isPrimaryKey()) {
          doIndexLookupOneKey(dbName, count, tableSchema, indexSchema, null, false, null,
              columnOffsets, forceSelectOnServer, null, leftKey, leftKey, leftOperator, index, ascending,
              retKeys, retRecords, server.getCommon().getSchemaVersion(), false, counters, groupContext,
              new AtomicLong(), null, null);
        }
        else {
          doIndexLookupOneKey(dbName, count, tableSchema, indexSchema, null, false,
              null, columnOffsets, forceSelectOnServer, null, leftKey, leftKey, leftOperator,
              index, ascending, retKeys, retRecords, server.getCommon().getSchemaVersion(), true, counters,
              groupContext, new AtomicLong(), null, null);

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
        retEntry.put(ComObject.Tag.keyCount, retKeys.size());
        ComArray keysArray = retEntry.putArray(ComObject.Tag.keys, ComObject.Type.byteArrayType);
        for (byte[] currKey : retKeys) {
          keysArray.add(currKey);
        }
        ComArray retRecordsArray = retEntry.putArray(ComObject.Tag.records, ComObject.Type.byteArrayType);
        for (int j = 0; j < retRecords.size(); j++) {
          byte[] bytes = retRecords.get(j);
          retRecordsArray.add(bytes);
        }
      }

      if (schemaVersion < server.getSchemaVersion()) {
        throw new SchemaOutOfSyncException("currVer:" + server.getCommon().getSchemaVersion() + ":");
      }

      return retObj;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  private static final MetricRegistry METRICS = new MetricRegistry();

  public static final com.codahale.metrics.Timer INDEX_LOOKUP_STATS = METRICS.timer("indexLookup");
  public static final com.codahale.metrics.Timer BATCH_INDEX_LOOKUP_STATS = METRICS.timer("batchIndexLookup");

  public void expirePreparedStatement(long preparedId) {
    preparedIndexLookups.remove(preparedId);
  }

  public void startPreparedReaper() {
    preparedReaper = new Thread(new Runnable(){
      @Override
      public void run() {
        while (true) {
          try {
            for (Map.Entry<Long, PreparedIndexLookup> prepared : preparedIndexLookups.entrySet()) {
              if (prepared.getValue().lastTimeUsed != 0 &&
                      prepared.getValue().lastTimeUsed < System.currentTimeMillis() - 30 * 60 * 1000) {
                preparedIndexLookups.remove(prepared.getKey());
              }
            }
            Thread.sleep(10 * 1000);
          }
          catch (Exception e) {
            logger.error("Error in prepared reaper thread", e);
          }
        }
      }
    });
    preparedReaper.start();
  }
  class PreparedIndexLookup {

    public long lastTimeUsed;
    public int count;
    public int tableId;
    public int indexId;
    public boolean forceSelectOnServer;
    public boolean evaluateExpression;
    public Expression expression;
    public List<OrderByExpressionImpl> orderByExpressions;
    public Set<Integer> columnOffsets;
  }

  private ConcurrentHashMap<Long, PreparedIndexLookup> preparedIndexLookups = new ConcurrentHashMap<>();

  private AtomicInteger lookupCount = new AtomicInteger();
  public ComObject indexLookup(ComObject cobj) {
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

      long schemaVersion = cobj.getLong(ComObject.Tag.schemaVersion);
      if (schemaVersion < server.getSchemaVersion()) {
        throw new SchemaOutOfSyncException("currVer:" + server.getCommon().getSchemaVersion() + ":");
      }
      else if (schemaVersion > server.getSchemaVersion()) {
        if (server.getShard() != 0 || server.getReplica() != 0) {
          server.getDatabaseClient().syncSchema();
          schemaVersion = server.getSchemaVersion();
        }
      }

      long serializationVersion = cobj.getLong(ComObject.Tag.serializationVersion);
      DataUtil.ResultLength resultLength = new DataUtil.ResultLength();
      long preparedId = cobj.getLong(ComObject.Tag.preparedId);
      boolean isPrepared = cobj.getBoolean(ComObject.Tag.isPrepared);

      PreparedIndexLookup prepared = null;
      if (isPrepared) {
        prepared = preparedIndexLookups.get(preparedId);
        if (prepared == null) {
          throw new PreparedIndexLookupNotFoundException();
        }
      }
      else {
        prepared = new PreparedIndexLookup();
        preparedIndexLookups.put(preparedId, prepared);
      }
      prepared.lastTimeUsed = System.currentTimeMillis();
      int count = 0;
      if (isPrepared) {
        count = prepared.count;
      }
      else {
        prepared.count = count = cobj.getInt(ComObject.Tag.count);
      }
      boolean isExplicitTrans = cobj.getBoolean(ComObject.Tag.isExcpliciteTrans);
      boolean isCommitting = cobj.getBoolean(ComObject.Tag.isCommitting);
      long transactionId = cobj.getLong(ComObject.Tag.transactionId);
      long viewVersion = cobj.getLong(ComObject.Tag.viewVersion);

      int tableId = 0;
      int indexId = 0;
      boolean forceSelectOnServer = false;
      if (isPrepared) {
        tableId = prepared.tableId;
        indexId = prepared.indexId;
        forceSelectOnServer = prepared.forceSelectOnServer;
      }
      else {
        prepared.tableId = tableId = cobj.getInt(ComObject.Tag.tableId);
        prepared.indexId = indexId = cobj.getInt(ComObject.Tag.indexId);
        prepared.forceSelectOnServer = forceSelectOnServer = cobj.getBoolean(ComObject.Tag.forceSelectOnServer);
      }
      ParameterHandler parms = null;
      byte[] parmBytes = cobj.getByteArray(ComObject.Tag.parms);
      if (parmBytes != null) {
        parms = new ParameterHandler();
        parms.deserialize(parmBytes);
      }
      boolean evaluateExpression;
      if (isPrepared) {
        evaluateExpression = prepared.evaluateExpression;
      }
      else {
        prepared.evaluateExpression = evaluateExpression = cobj.getBoolean(ComObject.Tag.evaluateExpression);
      }
      Expression expression = null;
      if (isPrepared) {
        expression = prepared.expression;
      }
      else {
        byte[] expressionBytes = cobj.getByteArray(ComObject.Tag.legacyExpression);
        if (expressionBytes != null) {
          prepared.expression = expression = ExpressionImpl.deserializeExpression(expressionBytes);
        }
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
      if (isPrepared) {
        orderByExpressions = prepared.orderByExpressions;
      }
      else {
        prepared.orderByExpressions = orderByExpressions = new ArrayList<>();
        ComArray array = cobj.getArray(ComObject.Tag.orderByExpressions);
        if (array != null) {
          for (Object entry : array.getArray()) {
            OrderByExpressionImpl orderByExpression = new OrderByExpressionImpl();
            orderByExpression.deserialize((byte[]) entry);
            orderByExpressions.add(orderByExpression);
          }
        }
      }
      byte[] leftBytes = cobj.getByteArray(ComObject.Tag.leftKey);
      Object[] leftKey = null;
      if (leftBytes != null) {
        leftKey = DatabaseCommon.deserializeKey(tableSchema, leftBytes);
      }
      byte[] originalLeftBytes = cobj.getByteArray(ComObject.Tag.originalLeftKey);
      Object[] originalLeftKey = null;
      if (originalLeftBytes != null) {
        originalLeftKey = DatabaseCommon.deserializeKey(tableSchema, originalLeftBytes);
      }
      //BinaryExpression.Operator leftOperator = BinaryExpression.Operator.getOperator(in.readInt());
      BinaryExpression.Operator leftOperator = BinaryExpression.Operator.getOperator(cobj.getInt(ComObject.Tag.leftOperator));

      BinaryExpression.Operator rightOperator = null;
      byte[] rightBytes = cobj.getByteArray(ComObject.Tag.rightKey);
      byte[] originalRightBytes = cobj.getByteArray(ComObject.Tag.originalRightKey);
      Object[] originalRightKey = null;
      Object[] rightKey = null;
      if (rightBytes != null) {
        rightKey = DatabaseCommon.deserializeKey(tableSchema, rightBytes);
      }
      if (originalRightBytes != null) {
        originalRightKey = DatabaseCommon.deserializeKey(tableSchema, originalRightBytes);
      }

      if (cobj.getInt(ComObject.Tag.rightOperator) != null) {
        rightOperator = BinaryExpression.Operator.getOperator(cobj.getInt(ComObject.Tag.rightOperator));
      }

      Set<Integer> columnOffsets = null;
      if (isPrepared) {
        columnOffsets = prepared.columnOffsets;
      }
      else {
        ComArray cOffsets = cobj.getArray(ComObject.Tag.columnOffsets);
        prepared.columnOffsets = columnOffsets = new HashSet<>();
        for (Object obj : cOffsets.getArray()) {
          columnOffsets.add((Integer)obj);
        }
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

      Index index = server.getIndices(dbName).getIndices().get(tableSchema.getName()).get(indexName);
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

      List<byte[]> retKeys = new ArrayList<>();
      List<byte[]> retRecords = new ArrayList<>();

      List<Object[]> excludeKeys = new ArrayList<>();

      if (isExplicitTrans && !isCommitting) {
        String[] fields = tableSchema.getPrimaryKey();
        int[] keyOffsets = new int[fields.length];
        for (int i = 0; i < keyOffsets.length; i++) {
          keyOffsets[i] = tableSchema.getFieldOffset(fields[i]);
        }
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
                retRecords.add(record.serialize(server.getCommon()));
              }
            }
          }
        }
      }

      if (indexSchema.isPrimaryKey()) {
        if (rightOperator == null) {
          entry = doIndexLookupOneKey(dbName, count, tableSchema, indexSchema, parms, evaluateExpression, expression,
              columnOffsets, forceSelectOnServer, excludeKeys, originalLeftKey, leftKey, leftOperator, index,
              ascending, retKeys, retRecords, viewVersion, false, counters, groupContext, currOffset, limit, offset);
        }
        else {
          entry = doIndexLookupTwoKeys(dbName, count, tableSchema, indexSchema, forceSelectOnServer, excludeKeys,
              originalLeftKey, leftKey, columnOffsets, originalRightKey, rightKey, leftOperator, rightOperator, parms,
              evaluateExpression, expression, index, ascending, retKeys, retRecords, viewVersion, false, counters,
              groupContext, currOffset, limit, offset);
        }
        //todo: support rightOperator
      }
      else {
        if (rightOperator == null) {
          entry = doIndexLookupOneKey(dbName, count, tableSchema, indexSchema, parms, evaluateExpression, expression,
              columnOffsets, forceSelectOnServer, excludeKeys, originalLeftKey, leftKey, leftOperator, index, ascending,
              retKeys, retRecords, viewVersion, true, counters, groupContext, currOffset, limit, offset);
        }
        else {
          entry = doIndexLookupTwoKeys(dbName, count, tableSchema, indexSchema, forceSelectOnServer, excludeKeys,
              originalLeftKey, leftKey, columnOffsets, originalRightKey, rightKey, leftOperator, rightOperator, parms,
              evaluateExpression, expression, index, ascending, retKeys, retRecords, viewVersion, true, counters, groupContext, currOffset, limit, offset);
        }
      }

      ComObject retObj = new ComObject();
      if (entry != null) {
        retObj.put(ComObject.Tag.keyBytes, DatabaseCommon.serializeKey(tableSchema, indexName, entry.getKey()));
      }
      ComArray array = retObj.putArray(ComObject.Tag.keys, ComObject.Type.byteArrayType);
      for (byte[] key : retKeys) {
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

      if (schemaVersion < server.getSchemaVersion()) {
        throw new SchemaOutOfSyncException("currVer:" + server.getCommon().getSchemaVersion() + ":");
      }

      return retObj;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
    finally {
      //context.stop();
    }
  }

  private Set<Integer> getSimpleColumnOffsets(DataInputStream in, DataUtil.ResultLength resultLength, String tableName, TableSchema tableSchema) throws IOException {
    int count = (int) DataUtil.readVLong(in, resultLength);
    Set<Integer> columnOffsets = new HashSet<>();
    for (int i = 0; i < count; i++) {
      columnOffsets.add((int) DataUtil.readVLong(in, resultLength));
    }
    return columnOffsets;
  }

  private Set<Integer> getColumnOffsets(
      DataInputStream in, DataUtil.ResultLength resultLength, String tableName,
      TableSchema tableSchema) throws IOException {
    Set<Integer> columnOffsets = new HashSet<>();
    int columnCount = (int) DataUtil.readVLong(in, resultLength);
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
      long schemaVersion = cobj.getLong(ComObject.Tag.schemaVersion);
      if (schemaVersion < server.getSchemaVersion()) {
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

      DiskBasedResultSet diskResults = null;
      if (select.getServerSelectPageNumber() == 0) {
        select.setPageSize(500000);
        ResultSetImpl resultSet = (ResultSetImpl) select.execute(dbName, null);
        diskResults = new DiskBasedResultSet(dbName, server, select.getTableNames(), resultSet, count, select);
      }
      else {
        diskResults = new DiskBasedResultSet(server, select, select.getTableNames(), select.getServerSelectResultSetId());
      }
      select.setServerSelectResultSetId(diskResults.getResultSetId());
      byte[][][] records = diskResults.nextPage(select.getServerSelectPageNumber(), count);

      ComObject retObj = new ComObject();
      select.setIsOnServer(false);
      retObj.put(ComObject.Tag.legacySelectStatement, select.serialize());

      if (records != null) {
        ComArray tableArray = retObj.putArray(ComObject.Tag.tableRecords, ComObject.Type.arrayType);
        for (byte[][] tableRecords : records) {
          ComArray recordArray = tableArray.addArray(ComObject.Tag.records, ComObject.Type.byteArrayType);

          for (byte[] record : tableRecords) {
            recordArray.add(record);
          }
        }
      }

      return retObj;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public ComObject indexLookupExpression(ComObject cobj) {
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
      long schemaVersion = cobj.getLong(ComObject.Tag.schemaVersion);
      if (schemaVersion < server.getSchemaVersion()) {
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

      TableSchema tableSchema = server.getCommon().getSchema(dbName).getTables().get(tableName);
      IndexSchema indexSchema = tableSchema.getIndices().get(indexName);

      byte[] leftKeyBytes = cobj.getByteArray(ComObject.Tag.leftKey);
      Object[] leftKey = null;
      if (leftKeyBytes != null) {
        leftKey = DatabaseCommon.deserializeKey(tableSchema, leftKeyBytes);
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

      Index index = server.getIndices(dbName).getIndices().get(tableSchema.getName()).get(indexName);
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

      List<byte[]> retKeys = new ArrayList<>();
      List<byte[]> retRecords = new ArrayList<>();

      if (tableSchema.getIndexes().get(indexName).isPrimaryKey()) {
        entry = doIndexLookupWithRecordsExpression(dbName, viewVersion, count, tableSchema, indexSchema, columnOffsets, parms, expression, index, leftKey,
            ascending, retRecords, counters, groupByContext, currOffset, limit, offset);
      }
      else {
        //entry = doIndexLookupExpression(count, indexSchema, columnOffsets, index, leftKey, ascending, retKeys);
      }
      //}
      ComObject retObj = new ComObject();
      if (entry != null) {
        retObj.put(ComObject.Tag.keyBytes, DatabaseCommon.serializeKey(tableSchema, indexName, entry.getKey()));
      }

      ComArray keys = retObj.putArray(ComObject.Tag.keys, ComObject.Type.byteArrayType);
      for (byte[] key : retKeys) {
        keys.add(key);
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

      if (schemaVersion < server.getSchemaVersion()) {
        throw new SchemaOutOfSyncException("currVer:" + server.getCommon().getSchemaVersion() + ":");
      }

      return retObj;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  private Map.Entry<Object[], Object> doIndexLookupWithRecordsExpression(
      String dbName, long viewVersion, int count, TableSchema tableSchema, IndexSchema indexSchema, Set<Integer> columnOffsets, ParameterHandler parms,
      Expression expression,
      Index index, Object[] leftKey, Boolean ascending, List<byte[]> ret, Counter[] counters, GroupByContext groupByContext, AtomicLong currOffset, Long limit, Long offset) {

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
      boolean forceSelectOnServer = false;
      byte[][] records = null;
      synchronized (index.getMutex(entry.getKey())) {
        //if (entry.getValue() instanceof Long) {
          entry.setValue(index.get(entry.getKey()));
        //}
        if (entry.getValue() != null && !entry.getValue().equals(0L)) {
          records = server.fromUnsafeToRecords(entry.getValue());
        }
      }
      if (parms != null && expression != null && records != null) {
        for (byte[] bytes : records) {
          Record record = new Record(tableSchema);
          record.deserialize(dbName, server.getCommon(), bytes, null, true);
          boolean pass = (Boolean) ((ExpressionImpl) expression).evaluateSingleRecord(new TableSchema[]{tableSchema}, new Record[]{record}, parms);
          if (pass) {
            byte[][] currRecords = new byte[][]{bytes};
            AtomicBoolean done = new AtomicBoolean();
            records = processViewFlags(dbName, tableSchema, indexSchema, index, viewVersion, entry.getKey(), records, done);
            if (records != null) {

              byte[][] currRet = applySelectToResultRecords(dbName, columnOffsets, forceSelectOnServer, currRecords, entry.getKey(), tableSchema, counters, groupByContext);
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
      else {
        AtomicBoolean done = new AtomicBoolean();
        records = processViewFlags(dbName, tableSchema, indexSchema, index, viewVersion, entry.getKey(), records, done);
        if (records != null) {
          byte[][] retRecords = applySelectToResultRecords(dbName, columnOffsets, forceSelectOnServer, records, entry.getKey(), tableSchema, counters, groupByContext);
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
      String dbName,
      int count,
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
      List<byte[]> retKeys,
      List<byte[]> retRecords,
      long viewVersion, boolean keys,
      Counter[] counters,
      GroupByContext groupContext, AtomicLong currOffset, Long limit, Long offset) {

    BinaryExpression.Operator greaterOp = leftOperator;
    Object[] greaterKey = leftKey;
    Object[] greaterOriginalKey = originalLeftKey;
    BinaryExpression.Operator lessOp = rightOperator;
    Object[] lessKey = leftKey;//rightKey;
    Object[] lessOriginalKey = originalRightKey;
    if (greaterOp == BinaryExpression.Operator.less ||
        greaterOp == BinaryExpression.Operator.lessEqual) {
      greaterOp = rightOperator;
      greaterKey = leftKey;
      greaterOriginalKey = originalRightKey;
      lessOp = leftOperator;
      lessKey = leftKey;
      lessOriginalKey = originalLeftKey;
    }

    Map.Entry<Object[], Object> entry = null;
    if (ascending == null || ascending) {
      if (greaterKey != null) {
        entry = index.floorEntry(greaterKey);
        lessKey = originalLeftKey;
      }
      else {
        if (greaterOriginalKey == null) {
          entry = index.firstEntry();
        }
        else {
          entry = index.floorEntry(greaterOriginalKey);
        }
      }
      if (entry == null) {
        entry = index.firstEntry();
      }
    }
    else {
      if (ascending != null && !ascending) {
        if (lessKey != null) {
          entry = index.ceilingEntry(lessKey);
          greaterKey = originalRightKey;
        }
        else {
          if (lessOriginalKey == null) {
            entry = index.lastEntry();
          }
          else {
            entry = index.ceilingEntry(lessOriginalKey);
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
        if (lessKey != null) {
          if (lessOp.equals(BinaryExpression.Operator.less) || lessOp.equals(BinaryExpression.Operator.lessEqual)) {
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
          if (greaterOp.equals(BinaryExpression.Operator.greater) || greaterOp.equals(BinaryExpression.Operator.greaterEqual)) {
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
        int compareValue = server.getCommon().compareKey(indexSchema.getComparators(), entry.getKey(), lessKey);
        if ((0 == compareValue || 1 == compareValue) && lessOp == BinaryExpression.Operator.less) {
          entry = null;
        }
        if (1 == compareValue) {
          entry = null;
        }
      }

      outer:
      while (entry != null) {
        if (retKeys.size() >= count || retRecords.size() >= count) {
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
        byte[][] currKeys = null;
        byte[][] records = null;
        synchronized (index.getMutex(entry.getKey())) {
          //if (entry.getValue() instanceof Long) {
            entry.setValue(index.get(entry.getKey()));
          //}
          if (entry.getValue() != null && !entry.getValue().equals(0L)) {
            if (keys) {
              currKeys = server.fromUnsafeToKeys(entry.getValue());
            }
            else {
              records = server.fromUnsafeToRecords(entry.getValue());
            }
          }
        }
        if (keys) {
          for (byte[] currKey : currKeys) {
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
              retKeys.add(currKey);
            }
            if (localDone) {
              entry = null;
              break outer;
            }
          }
        }
        else {
          AtomicBoolean done = new AtomicBoolean();
          records = processViewFlags(dbName, tableSchema, indexSchema, index, viewVersion, entry.getKey(), records, done);
          if (records == null) {
            if (done.get()) {
              entry = null;
              break outer;
            }
          }
          else {
            if (parms != null && expression != null && evaluateExpression) {
              for (byte[] bytes : records) {
                Record record = new Record(tableSchema);
                record.deserialize(dbName, server.getCommon(), bytes, null, true);
                boolean pass = (Boolean) ((ExpressionImpl) expression).evaluateSingleRecord(new TableSchema[]{tableSchema}, new Record[]{record}, parms);
                if (pass) {
                  byte[][] currRecords = new byte[][]{bytes};
                  byte[][] ret = applySelectToResultRecords(dbName, columnOffsets, forceSelectOnServer, currRecords, entry.getKey(), tableSchema, counters, groupContext);
                  if (counters == null) {
                    for (byte[] currBytes : ret) {
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
                        retRecords.add(currBytes);
                      }
                      if (localDone) {
                        entry = null;
                        break outer;
                      }
                    }
                  }
                }
              }
            } else {
              if (records.length > 2) {
                logger.error("Records size: " + records.length);
              }

              byte[][] ret = applySelectToResultRecords(dbName, columnOffsets, forceSelectOnServer, records, entry.getKey(), tableSchema, counters, groupContext);
              if (counters == null) {
                for (byte[] currBytes : ret) {
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
                    retRecords.add(currBytes);
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
        //        if (operator.equals(QueryEvaluator.BinaryRelationalOperator.Operator.equal)) {
        //          entry = null;
        //          break;
        //        }
        if (ascending != null && !ascending) {
          entry = index.lowerEntry((entry.getKey()));
        }
        else {
          entry = index.higherEntry((entry.getKey()));
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
      }
    }
    return entry;
  }

  private byte[][] applySelectToResultRecords(String dbName, Set<Integer> columnOffsets, boolean forceSelectOnServer, byte[][] records, Object[] key,
                                              TableSchema tableSchema, Counter[] counters, GroupByContext groupContext) {
    if (columnOffsets == null || columnOffsets.size() == 0) {
      columnOffsets = null;
    }
    for (int i = 0; i < records.length; i++) {
      byte[] recordBytes = records[i];

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
    }
    return records;
  }

  private Map.Entry<Object[], Object> doIndexLookupOneKey(
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
      List<byte[]> retKeys,
      List<byte[]> retRecords,
      long viewVersion,
      boolean keys,
      Counter[] counters,
      GroupByContext groupContext, AtomicLong currOffset, Long limit, Long offset) {
    Map.Entry<Object[], Object> entry = null;

    //count = 3;
    if (operator.equals(BinaryExpression.Operator.equal)) {
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
      if (!hasNull && originalKey.length == indexSchema.getFields().length && (indexSchema.isPrimaryKey() || indexSchema.isUnique())) {
        byte[][] records = null;
        byte[][] currKeys = null;
        Object value = null;
        synchronized (index.getMutex(originalKey)) {
          value = index.get(originalKey);
          if (value != null && !value.equals(0L)) {
            if (keys) {
              currKeys = server.fromUnsafeToKeys(value);
            }
            else {
              records = server.fromUnsafeToRecords(value);
            }
          }
        }
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

          handleRecord(dbName, tableSchema, indexSchema, viewVersion, index, keyToUse, parms, evaluateExpresion, expression, columnOffsets, forceSelectOnServer, retKeys, retRecords, keys, counters, groupContext, records, currKeys);

          //}
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
            byte[][] currKeys = null;
            synchronized (index.getMutex(entry.getKey())) {
              //if (value instanceof Long) {
                value = index.get(entry.getKey());
              //}
              if (value != null && !value.equals(0L)) {
                if (keys) {
                  currKeys = server.fromUnsafeToKeys(value);
                }
                else {
                  records = server.fromUnsafeToRecords(value);
                }
              }
            }
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
              handleRecord(dbName, tableSchema, indexSchema, viewVersion, index, keyToUse, parms, evaluateExpresion, expression, columnOffsets, forceSelectOnServer, retKeys, retRecords, keys, counters, groupContext, records, currKeys);
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
        entry = index.floorEntry(key);
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
            entry = index.ceilingEntry(originalKey);
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
            entry = index.ceilingEntry(key);
            if (entry == null) {
              entry = index.lastEntry();
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
        if (retRecords.size() >= count || retKeys.size() >= count) {
          break;
        }

        for (Map.Entry<Object[], Object> currEntry : entries) {
          entry = currEntry;
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

          byte[][] currKeys = null;
          byte[][] records = null;
          synchronized (index.getMutex(currEntry.getKey())) {
            //if (currEntry.getValue() instanceof Long) {
              currEntry.setValue(index.get(currEntry.getKey()));
            //}
            if (keys) {
              Object unsafeAddress = currEntry.getValue();//index.get(entry.getKey());
              if (unsafeAddress != null && !unsafeAddress.equals(0L)) {
                currKeys = server.fromUnsafeToKeys(unsafeAddress);
              }
            }
            else {
              Object unsafeAddress = currEntry.getValue();//index.get(entry.getKey());
              if (unsafeAddress != null && !unsafeAddress.equals(0L)) {
                records = server.fromUnsafeToRecords(unsafeAddress);
                while (records == null) {
                  try {
                    Thread.sleep(100);
                    System.out.println("null records ************************************");
                  }
                  catch (InterruptedException e) {
                    throw new DatabaseException(e);
                  }
                  currEntry.setValue(index.get(currEntry.getKey()));
                  unsafeAddress = currEntry.getValue();//index.get(entry.getKey());
                  if (unsafeAddress != null && !unsafeAddress.equals(0L)) {
                    records = server.fromUnsafeToRecords(unsafeAddress);
                  }
                  else {
                    break;
                  }
                }
              }
            }
          }
          if (keys) {
            if (currKeys != null) {
              for (byte[] currKey : currKeys) {
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
                  retKeys.add(currKey);
                }
                if (localDone) {
                  entry = null;
                  break outer;
                }
              }
            }
          }
          else {
            AtomicBoolean done = new AtomicBoolean();
            records = processViewFlags(dbName, tableSchema, indexSchema, index, viewVersion, currEntry.getKey(), records, done);
            if (records == null) {
              if (done.get()) {
                entry = null;
                break outer;
              }
            }
            else {
              if (parms != null && expression != null && evaluateExpresion) {
                for (byte[] bytes : records) {
                  Record record = new Record(tableSchema);
                  record.deserialize(dbName, server.getCommon(), bytes, null);
                  boolean pass = (Boolean) ((ExpressionImpl) expression).evaluateSingleRecord(new TableSchema[]{tableSchema}, new Record[]{record}, parms);
                  if (pass) {
                    byte[][] currRecords = new byte[][]{bytes};
//                    if (currRecords != null && currRecords[0].length < 300 && (true || operator == BinaryExpression.Operator.equal) && groupContext == null && counters == null) {
//                      for (int i = 0; i < currRecords.length; i++) {
//                        retRecords.add(new Record(dbName, server.getCommon(), currRecords[i], columnOffsets , true));
//                      }
//                    }
//                    else {
                      byte[][] ret = applySelectToResultRecords(dbName, columnOffsets, forceSelectOnServer, currRecords, null, tableSchema, counters, groupContext);
                      if (counters == null) {
                        for (byte[] currBytes : ret) {
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
                            retRecords.add(currBytes);
                          }
                          if (localDone) {
                            entry = null;
                            break outer;
                          }
                        }
                      }
//                    }
                  }
                }
              }
              else {
                if (records.length > 2) {
                  logger.error("Records size: " + records.length);
                }
                if (records != null) {
//                  if (records[0].length < 300 && (true || operator == BinaryExpression.Operator.equal) && groupContext == null && counters == null) {
//                    for (int i = 0; i < records.length; i++) {
//                      retRecords.add(new Record(dbName, server.getCommon(), records[i], columnOffsets, true));
//                    }
//                  }
//                  else {
                    byte[][] ret = applySelectToResultRecords(dbName, columnOffsets, forceSelectOnServer, records, entry.getKey(), tableSchema, counters, groupContext);
                    if (counters == null) {
                      for (byte[] currBytes : ret) {
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
                          retRecords.add(currBytes);
                        }
                        if (localDone) {
                          entry = null;
                          break outer;
                        }
                      }
                    }
//                  }
                }
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
        final int diff = Math.max(retRecords.size(), retKeys.size());
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
      System.out.println("null records *******************");
    }
    else {
      if (indexSchema == null || server.getCommon().getTables(dbName).get(tableSchema.getName()).getIndices().get(indexSchema.getName()).getLastPartitions() != null) {
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

  private boolean handleRecord(String dbName, TableSchema tableSchema, IndexSchema indexSchema, long viewVersion,
                               Index index, Object[] key, ParameterHandler parms, boolean evaluateExpresion,
                               Expression expression, Set<Integer> columnOffsets, boolean forceSelectOnServer,
                               List<byte[]> retKeys, List<byte[]> retRecords, boolean keys, Counter[] counters,
                               GroupByContext groupContext, byte[][] records, byte[][] currKeys) {
    if (keys) {
      for (byte[] currKey : currKeys) {
        retKeys.add(currKey);
      }
    }
    else {
//      records = processViewFlags(dbName, tableSchema, indexSchema, index, viewVersion, key, records);

      List<byte[]> remaining = new ArrayList<>();
      if (records == null) {
        System.out.println("null records *******************");
      }
      else {
        for (byte[] bytes : records) {
          if (!processViewFlags(viewVersion, remaining, bytes)) {
            return false;
          }
//          else {
//            remaining.add(bytes);
//          }
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

      if (records != null) {
        if (parms != null && expression != null && evaluateExpresion) {
          for (byte[] bytes : records) {
            Record record = new Record(tableSchema);
            record.deserialize(dbName, server.getCommon(), bytes, null, true);
            boolean pass = (Boolean) ((ExpressionImpl) expression).evaluateSingleRecord(new TableSchema[]{tableSchema}, new Record[]{record}, parms);
            if (pass) {
              byte[][] currRecords = new byte[][]{bytes};
              byte[][] ret = applySelectToResultRecords(dbName, columnOffsets, forceSelectOnServer, currRecords, null, tableSchema, counters, groupContext);
              if (counters == null) {
                for (byte[] currBytes : ret) {
                  retRecords.add(currBytes);
                }
              }
            }
          }
        }
        else {
          byte[][] ret = applySelectToResultRecords(dbName, columnOffsets, forceSelectOnServer, records, null, tableSchema, counters, groupContext);
          if (counters == null) {
            for (byte[] currBytes : ret) {
              retRecords.add(currBytes);
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
      TableSchema tableSchema = server.getCommon().getTables(dbName).get(tableName);
      for (IndexSchema indexSchema : tableSchema.getIndexes().values()) {
        if (indexSchema.getFields()[0].equals(columnName)) {
          isPrimaryKey = indexSchema.isPrimaryKey();
          indexName = indexSchema.getName();
          //break;
        }
      }
      byte[] maxKey = null;
      byte[] minKey = null;
      Index index = server.getIndices().get(dbName).getIndices().get(tableName).get(indexName);
      Map.Entry<Object[], Object> entry = index.lastEntry();
      if (entry != null) {
        byte[][] records = null;
        synchronized (index.getMutex(entry.getKey())) {
          Object unsafeAddress = entry.getValue();
          //if (unsafeAddress instanceof Long) {
            unsafeAddress = index.get(entry.getKey());
          //}
          if (unsafeAddress != null && !unsafeAddress.equals(0L)) {
            records = server.fromUnsafeToRecords(unsafeAddress);
          }
        }
        if (records != null) {
          if (isPrimaryKey) {
            maxKey = DatabaseCommon.serializeKey(tableSchema, indexName, entry.getKey());
          }
          else {
            maxKey = records[0];
          }
        }
      }
      entry = index.firstEntry();
      if (entry != null) {
        byte[][] records = null;
        synchronized (index.getMutex(entry.getKey())) {
          Object unsafeAddress = entry.getValue();
          //if (unsafeAddress instanceof Long) {
            unsafeAddress = index.get(entry.getKey());
          //}
          if (unsafeAddress != null && !unsafeAddress.equals(0L)) {
            records = server.fromUnsafeToRecords(unsafeAddress);
          }
        }
        if (records != null) {
          if (isPrimaryKey) {
            minKey = DatabaseCommon.serializeKey(tableSchema, indexName, entry.getKey());
          }
          else {
            minKey = records[0];
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
      TableSchema tableSchema = server.getCommon().getTables(dbName).get(tableName);
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

      Index index = server.getIndices().get(dbName).getIndices().get(tableName).get(indexName);
      synchronized (index.getMutex(key)) {
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
      }
      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.legacyCounter, counter.serialize());
      return retObj;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }
}
