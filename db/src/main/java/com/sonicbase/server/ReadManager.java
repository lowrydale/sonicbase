package com.sonicbase.server;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.*;
import com.sonicbase.index.Index;
import com.sonicbase.jdbcdriver.ParameterHandler;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.sonicbase.server.DatabaseServer.METRIC_READ;

@SuppressWarnings({"squid:S1172", "squid:S1168", "squid:S00107"})
// all methods called from method invoker must have cobj and replayed command parms
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class ReadManager {

  private static final String CURR_VER_STR = "currVer:";
  private static final Logger logger = LoggerFactory.getLogger(ReadManager.class);


  private final com.sonicbase.server.DatabaseServer server;
  private Thread diskReaper;
  private boolean shutdown;
  private final AtomicInteger lookupCount = new AtomicInteger();


  ReadManager(DatabaseServer databaseServer) {

    this.server = databaseServer;

    startDiskResultsReaper();
  }

  private void startDiskResultsReaper() {
    diskReaper = ThreadUtil.createThread(() -> {
      while (!shutdown) {
        try {
          DiskBasedResultSet.deleteOldResultSets(server);
        }
        catch (Exception e) {
          logger.error("Error in disk results reaper thread", e);
        }
        try {
          Thread.sleep(100 * 1000L);
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }, "SonicBase Disk Results Reaper Thread");
    diskReaper.start();
  }


  @SchemaReadLock
  public ComObject countRecords(ComObject cobj, boolean replayedCommand) {
    if (server.getBatchRepartCount().get() != 0 && lookupCount.incrementAndGet() % 1000 == 0) {
      try {
        Thread.sleep(10);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new DatabaseException(e);
      }
    }

    String dbName = cobj.getString(ComObject.Tag.DB_NAME);
    Integer schemaVersion = cobj.getInt(ComObject.Tag.SCHEMA_VERSION);
    if (schemaVersion != null && schemaVersion < server.getSchemaVersion()) {
      throw new SchemaOutOfSyncException(CURR_VER_STR + server.getCommon().getSchemaVersion() + ":");
    }
    String fromTable = cobj.getString(ComObject.Tag.TABLE_NAME);

    byte[] expressionBytes = cobj.getByteArray(ComObject.Tag.LEGACY_EXPRESSION);
    Expression expression = null;
    if (expressionBytes != null) {
      expression = ExpressionImpl.deserializeExpression(expressionBytes);
    }
    byte[] parmsBytes = cobj.getByteArray(ComObject.Tag.PARMS);
    ParameterHandler parms = null;
    if (parmsBytes != null) {
      parms = new ParameterHandler();
      parms.deserialize(parmsBytes);
    }
    String countColumn = cobj.getString(ComObject.Tag.COUNT_COLUMN);

    long count = 0;
    String primaryKeyIndex = null;
    for (Map.Entry<String, IndexSchema> entry : server.getCommon().getTableSchema(dbName, fromTable,
        server.getDataDir()).getIndices().entrySet()) {
      if (entry.getValue().isPrimaryKey()) {
        primaryKeyIndex = entry.getValue().getName();
        break;
      }
    }
    TableSchema tableSchema = server.getCommon().getTableSchema(dbName, fromTable, server.getDataDir());
    Index index = server.getIndex(dbName, fromTable, primaryKeyIndex);

    int countColumnOffset = getCountColumnOffset(countColumn, tableSchema);

    if (countColumn == null && expression == null) {
      count = index.getCount();
    }
    else {
      count = doCount(dbName, expression, parms, countColumn, count, tableSchema, index, countColumnOffset);
    }


    ComObject retObj = new ComObject();
    retObj.put(ComObject.Tag.COUNT_LONG, count);
    return retObj;
  }

  private int getCountColumnOffset(String countColumn, TableSchema tableSchema) {
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
    return countColumnOffset;
  }

  private long doCount(String dbName, Expression expression, ParameterHandler parms, String countColumn, long count,
                       TableSchema tableSchema, Index index, int countColumnOffset) {
    Map.Entry<Object[], Object> entry = index.firstEntry();
    while (entry != null) {
      byte[][] records = null;
      Object[] key = entry.getKey();
      synchronized (index.getMutex(key)) {
        Object value = index.get(key);
        if (value != null && !value.equals(0L)) {
          records = server.getAddressMap().fromUnsafeToRecords(value);
        }
      }
      for (byte[] bytes : records) {
        count = doCountForRecord(dbName, expression, parms, countColumn, count, tableSchema, countColumnOffset, bytes);
      }
      entry = index.higherEntry(entry.getKey());
    }
    return count;
  }

  private long doCountForRecord(String dbName, Expression expression, ParameterHandler parms, String countColumn,
                                long count, TableSchema tableSchema, int countColumnOffset, byte[] bytes) {
    if ((Record.getDbViewFlags(bytes) & Record.DB_VIEW_FLAG_DELETING) != 0) {
      return count;
    }
    Record record = new Record(tableSchema);
    record.deserialize(dbName, server.getCommon(), bytes, null, true);
    boolean pass = true;
    if (countColumn != null && record.getFields()[countColumnOffset] == null) {
      pass = false;
    }
    if (pass) {
      if (expression == null) {
        count++;
      }
      else {
        pass = (Boolean) ((ExpressionImpl) expression).evaluateSingleRecord(new TableSchema[]{tableSchema},
            new Record[]{record}, parms);
        if (pass) {
          count++;
        }
      }
    }
    return count;
  }

  @SchemaReadLock
  public ComObject batchIndexLookup(ComObject cobj, boolean replayedCommand) {
    try {
      if (server.getBatchRepartCount().get() != 0 && lookupCount.incrementAndGet() % 1000 == 0) {
        ThreadUtil.sleep(10);
      }

      IndexLookup indexLookup = new IndexLookupOneKey(server);
      String dbName = cobj.getString(ComObject.Tag.DB_NAME);
      indexLookup.setDbName(dbName);
      Integer schemaVersion = cobj.getInt(ComObject.Tag.SCHEMA_VERSION);
      if (schemaVersion != null && schemaVersion < server.getSchemaVersion()) {
        throw new SchemaOutOfSyncException(CURR_VER_STR + server.getCommon().getSchemaVersion() + ":");
      }
      indexLookup.setCount(cobj.getInt(ComObject.Tag.COUNT));

      String tableName = cobj.getString(ComObject.Tag.TABLE_NAME);
      String indexName = cobj.getString(ComObject.Tag.INDEX_NAME);
      TableSchema tableSchema = server.getCommon().getTableSchema(dbName, tableName, server.getDataDir());
      IndexSchema indexSchema = server.getIndexSchema(dbName, tableSchema.getName(), indexName);
      indexLookup.setTableSchema(tableSchema);
      indexLookup.setIndexSchema(indexSchema);
      Index index = server.getIndex(dbName, tableSchema.getName(), indexName);
      indexLookup.setIndex(index);

      indexLookup.setCurrOffset(new AtomicLong());
      indexLookup.setCountReturned(new AtomicLong());

      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.SERIALIZATION_VERSION, DatabaseClient.SERIALIZATION_VERSION);

      int leftOperatorId = cobj.getInt(ComObject.Tag.LEFT_OPERATOR);
      BinaryExpression.Operator leftOperator = BinaryExpression.Operator.getOperator(leftOperatorId);
      indexLookup.setLeftOperator(leftOperator);

      ComArray cOffsets = cobj.getArray(ComObject.Tag.COLUMN_OFFSETS);
      Set<Integer> columnOffsets = new HashSet<>();
      for (Object obj : cOffsets.getArray()) {
        columnOffsets.add((Integer)obj);
      }
      indexLookup.setColumnOffsets(columnOffsets);

      boolean singleValue = cobj.getBoolean(ComObject.Tag.SINGLE_VALUE);

      ComArray keys = cobj.getArray(ComObject.Tag.KEYS);
      ComArray retKeysArray = retObj.putArray(ComObject.Tag.RET_KEYS, ComObject.Type.OBJECT_TYPE);
      for (Object keyObj : keys.getArray()) {
        batchIndexLookupForKey(indexLookup, indexName, tableSchema, indexSchema, singleValue, retKeysArray,
            (ComObject) keyObj);
      }
      return retObj;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private void batchIndexLookupForKey(IndexLookup indexLookup, String indexName, TableSchema tableSchema,
                                      IndexSchema indexSchema, boolean singleValue, ComArray retKeysArray,
                                      ComObject keyObj) throws EOFException {
    int offset = keyObj.getInt(ComObject.Tag.OFFSET);
    Object[] leftKey = null;
    if (singleValue) {
      leftKey = new Object[]{keyObj.getLong(ComObject.Tag.LONG_KEY)};
    }
    else {
      byte[] keyBytes = keyObj.getByteArray(ComObject.Tag.KEY_BYTES);
      leftKey = DatabaseCommon.deserializeKey(tableSchema, keyBytes);
    }
    indexLookup.setLeftKey(leftKey);
    indexLookup.setOriginalLeftKey(leftKey);

    List<byte[]> retKeyRecords = new ArrayList<>();
    List<Object[]> retKeys = new ArrayList<>();
    List<byte[]> retRecords = new ArrayList<>();

    indexLookup.setRetKeyRecords(retKeyRecords);
    indexLookup.setRetKeys(retKeys);
    indexLookup.setRetRecords(retRecords);

    boolean returnKeys = true;
    if (indexSchema.isPrimaryKey()) {
      returnKeys = false;
    }
    indexLookup.setKeys(returnKeys);
    indexLookup.lookup();

    ComObject retEntry = new ComObject();
    retKeysArray.add(retEntry);
    retEntry.put(ComObject.Tag.OFFSET, offset);
    retEntry.put(ComObject.Tag.KEY_COUNT, retKeyRecords.size());

    ComArray keysArray = retEntry.putArray(ComObject.Tag.KEY_RECORDS, ComObject.Type.BYTE_ARRAY_TYPE);
    for (byte[] currKey : retKeyRecords) {
      keysArray.add(currKey);
    }
    keysArray = retEntry.putArray(ComObject.Tag.KEYS, ComObject.Type.BYTE_ARRAY_TYPE);
    for (Object[] currKey : retKeys) {
      keysArray.add(DatabaseCommon.serializeKey(tableSchema, indexName, currKey));
    }
    ComArray retRecordsArray = retEntry.putArray(ComObject.Tag.RECORDS, ComObject.Type.BYTE_ARRAY_TYPE);
    if (retRecords.isEmpty()) {
      logger.error("Record not found: key=" + DatabaseCommon.keyToString(leftKey));
    }
    for (int j = 0; j < retRecords.size(); j++) {
      byte[] bytes = retRecords.get(j);
      retRecordsArray.add(bytes);
    }
  }

  public void shutdown() {
    try {
      shutdown = true;
      if (diskReaper != null) {
        diskReaper.interrupt();
        diskReaper.join();
      }
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new DatabaseException(e);
    }
  }

  @SchemaReadLock
  public ComObject indexLookup(ComObject cobj, boolean replayedCommand) {
    return indexLookup(cobj, null);
  }

  ComObject indexLookup(ComObject cobj, StoredProcedureContextImpl procedureContext) {
    try {
      Integer schemaVersion = cobj.getInt(ComObject.Tag.SCHEMA_VERSION);
      if (schemaVersion != null && schemaVersion < server.getSchemaVersion()) {
        throw new SchemaOutOfSyncException(CURR_VER_STR + server.getCommon().getSchemaVersion() + ":");
      }
      else if (schemaVersion != null && schemaVersion > server.getSchemaVersion()) {
        if (server.getShard() != 0 || server.getReplica() != 0) {
          server.getDatabaseClient().syncSchema();
          schemaVersion = server.getSchemaVersion();
        }
        else {
          logger.error("Client schema is newer than server schema: client={}, server={}", schemaVersion,
              server.getSchemaVersion());
        }
      }

      short serializationVersion = cobj.getShort(ComObject.Tag.SERIALIZATION_VERSION);

      IndexLookup indexLookup;
      if (cobj.getInt(ComObject.Tag.RIGHT_OPERATOR) != null) {
        indexLookup = new IndexLookupTwoKeys(server);
        indexLookup.setRightOperator(BinaryExpression.Operator.getOperator(cobj.getInt(ComObject.Tag.RIGHT_OPERATOR)));
      }
      else {
        indexLookup = new IndexLookupOneKey(server);
      }

      indexLookup.setProcedureContext(procedureContext);
      indexLookup.setCount(cobj.getInt(ComObject.Tag.COUNT));
      indexLookup.setIsExplicitTrans(cobj.getBoolean(ComObject.Tag.IS_EXCPLICITE_TRANS));
      indexLookup.setIsCommiting(cobj.getBoolean(ComObject.Tag.IS_COMMITTING));
      indexLookup.setTransactionId(cobj.getLong(ComObject.Tag.TRANSACTION_ID));
      indexLookup.setViewVersion(cobj.getLong(ComObject.Tag.VIEW_VERSION));
      Boolean isProbe = cobj.getBoolean(ComObject.Tag.IS_PROBE);
      if (isProbe == null) {
        isProbe = false;
      }
      indexLookup.setIsProbe(isProbe);

      int tableId = cobj.getInt(ComObject.Tag.TABLE_ID);
      int indexId = cobj.getInt(ComObject.Tag.INDEX_ID);

      ParameterHandler parms = setParms(cobj, indexLookup);

      indexLookup.setEvaluateExpression(cobj.getBoolean(ComObject.Tag.EVALUATE_EXPRESSION));

      Expression expression = setExpression(cobj, indexLookup);

      String dbName = cobj.getString(ComObject.Tag.DB_NAME);
      indexLookup.setDbName(dbName);
      String tableName = null;
      String indexName = null;
      TableSchema tableSchema;
      IndexSchema indexSchema;
      try {
        Map<Integer, TableSchema> tablesById = server.getCommon().getTablesById(dbName);
        tableSchema = tablesById.get(tableId);
        tableName = tableSchema.getName();
        indexLookup.setTableSchema(tableSchema);
        indexSchema = tableSchema.getIndexesById().get(indexId);
        indexLookup.setIndexSchema(indexSchema);
        indexName = indexSchema.getName();
      }
      catch (Exception e) {
        logger.info("indexLookup: tableName={}, tableId={}, tableByNameCount={}, tableCount={}, tableNull={}, indexName={}, indexId={}",
            tableName, tableId, server.getCommon().getTables(dbName).size(), server.getCommon().getTablesById(dbName).size(),
            (server.getCommon().getTablesById(dbName).get(tableId) == null), indexName, indexId);
        throw e;
      }
      List<OrderByExpressionImpl> orderByExpressions = null;
      orderByExpressions = new ArrayList<>();
      ComArray oarray = cobj.getArray(ComObject.Tag.ORDER_BY_EXPRESSIONS);
      getOrderByExpressions(orderByExpressions, oarray);

      setLeftKeyAndOperator(cobj, indexLookup);

      setRightKeyAndOperator(cobj, indexLookup);

      setColumnOffsets(cobj, indexLookup);

      Counter[] counters = getCounters(cobj, indexLookup);

      GroupByContext groupContext = setGroupByContext(cobj, indexLookup);

      SetOffsetLimit setOffsetLimit = new SetOffsetLimit(cobj, indexLookup).invoke();
      AtomicLong currOffset = setOffsetLimit.getCurrOffset();
      AtomicLong countReturned = setOffsetLimit.getCountReturned();

      Index index = server.getIndex(dbName, tableSchema.getName(), indexName);
      Map.Entry<Object[], Object> entry = null;
      indexLookup.setIndex(index);

      setAscending(indexLookup, tableSchema, indexSchema, orderByExpressions);

      List<byte[]> retKeyRecords = new ArrayList<>();
      indexLookup.setRetKeyRecords(retKeyRecords);
      List<Object[]> retKeys = new ArrayList<>();
      indexLookup.setRetKeys(retKeys);
      List<byte[]> retRecords = new ArrayList<>();
      indexLookup.setRetRecords(retRecords);

      List<Object[]> excludeKeys = new ArrayList<>();
      indexLookup.setExcludeKeys(excludeKeys);

      String[] fields = tableSchema.getPrimaryKey();
      int[] keyOffsets = new int[fields.length];
      for (int i = 0; i < keyOffsets.length; i++) {
        keyOffsets[i] = tableSchema.getFieldOffset(fields[i]);
      }

      handleTransaction(cobj, indexLookup, parms, (ExpressionImpl) expression, tableName, tableSchema, retRecords,
          excludeKeys, keyOffsets);

      if (indexSchema.isPrimaryKey()) {
        indexLookup.setKeys(false);
      }
      else {
        indexLookup.setKeys(true);
      }
      entry = indexLookup.lookup();

      return processResponse(indexName, tableSchema, counters, groupContext, currOffset, countReturned, entry,
          retKeyRecords, retKeys, retRecords);
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
    finally {
      server.getStats().get(METRIC_READ).getCount().incrementAndGet();
    }
  }

  private GroupByContext setGroupByContext(ComObject cobj, IndexLookup indexLookup) throws IOException {
    byte[] groupContextBytes = cobj.getByteArray(ComObject.Tag.LEGACY_GROUP_CONTEXT);
    GroupByContext groupContext = null;
    if (groupContextBytes != null) {
      groupContext = new GroupByContext();
      groupContext.deserialize(groupContextBytes, server.getCommon());
    }
    indexLookup.setGroupContext(groupContext);
    return groupContext;
  }

  private ComObject processResponse(String indexName, TableSchema tableSchema, Counter[] counters,
                                    GroupByContext groupContext, AtomicLong currOffset, AtomicLong countReturned,
                                    Map.Entry<Object[], Object> entry, List<byte[]> retKeyRecords,
                                    List<Object[]> retKeys, List<byte[]> retRecords) throws IOException {
    ComObject retObj = new ComObject();
    if (entry != null) {
      retObj.put(ComObject.Tag.KEY_BYTES, DatabaseCommon.serializeKey(tableSchema, indexName, entry.getKey()));
    }
    ComArray array = retObj.putArray(ComObject.Tag.KEYS, ComObject.Type.BYTE_ARRAY_TYPE);
    for (Object[] key : retKeys) {
      array.add(DatabaseCommon.serializeKey(tableSchema, indexName, key));
    }
    array = retObj.putArray(ComObject.Tag.KEY_RECORDS, ComObject.Type.BYTE_ARRAY_TYPE);
    for (byte[] key : retKeyRecords) {
      array.add(key);
    }
    array = retObj.putArray(ComObject.Tag.RECORDS, ComObject.Type.BYTE_ARRAY_TYPE);
    for (int i = 0; i < retRecords.size(); i++) {
      byte[] bytes = retRecords.get(i);
      array.add(bytes);
    }

    if (counters != null) {
      array = retObj.putArray(ComObject.Tag.COUNTERS, ComObject.Type.BYTE_ARRAY_TYPE);
      for (int i = 0; i < counters.length; i++) {
        array.add(counters[i].serialize());
      }
    }

    if (groupContext != null) {
      retObj.put(ComObject.Tag.LEGACY_GROUP_CONTEXT, groupContext.serialize(server.getCommon()));
    }

    retObj.put(ComObject.Tag.CURR_OFFSET, currOffset.get());
    retObj.put(ComObject.Tag.COUNT_RETURNED, countReturned.get());
    return retObj;
  }

  private void handleTransaction(ComObject cobj, IndexLookup indexLookup, ParameterHandler parms,
                                 ExpressionImpl expression, String tableName, TableSchema tableSchema,
                                 List<byte[]> retRecords, List<Object[]> excludeKeys, int[] keyOffsets) {
    if (indexLookup.isExplicitTrans() && !indexLookup.isCommitting()) {
      TransactionManager.Transaction trans = server.getTransactionManager().getTransaction(indexLookup.getTransactionId());
      if (trans != null) {
        List<Record> records = trans.getRecords().get(tableName);
        if (records != null) {
          for (Record record : records) {
            handleTransactionProcessRecord(cobj, parms, expression, tableSchema, retRecords, excludeKeys, keyOffsets, record);
          }
        }
      }
    }
  }

  private void handleTransactionProcessRecord(ComObject cobj, ParameterHandler parms, ExpressionImpl expression,
                                              TableSchema tableSchema, List<byte[]> retRecords, List<Object[]> excludeKeys,
                                              int[] keyOffsets, Record record) {
    boolean pass = (Boolean) expression.evaluateSingleRecord(new TableSchema[]{tableSchema}, new Record[]{record}, parms);
    if (pass) {
      Object[] excludeKey = new Object[keyOffsets.length];
      for (int i = 0; i < excludeKey.length; i++) {
        excludeKey[i] = record.getFields()[keyOffsets[i]];
      }
      excludeKeys.add(excludeKey);
      retRecords.add(record.serialize(server.getCommon(), cobj.getShort(ComObject.Tag.SERIALIZATION_VERSION)));
    }
  }

  private ParameterHandler setParms(ComObject cobj, IndexLookup indexLookup) {
    ParameterHandler parms = null;
    byte[] parmBytes = cobj.getByteArray(ComObject.Tag.PARMS);
    if (parmBytes != null) {
      parms = new ParameterHandler();
      parms.deserialize(parmBytes);
    }
    indexLookup.setParms(parms);
    return parms;
  }

  private Expression setExpression(ComObject cobj, IndexLookup indexLookup) {
    Expression expression = null;
    byte[] expressionBytes = cobj.getByteArray(ComObject.Tag.LEGACY_EXPRESSION);
    if (expressionBytes != null) {
      expression = ExpressionImpl.deserializeExpression(expressionBytes);
    }
    indexLookup.setExpression(expression);
    return expression;
  }

  private void setLeftKeyAndOperator(ComObject cobj, IndexLookup indexLookup) {
    byte[] leftBytes = cobj.getByteArray(ComObject.Tag.LEFT_KEY);
    Object[] leftKey = null;
    if (leftBytes != null) {
      leftKey = DatabaseCommon.deserializeTypedKey(leftBytes);
    }
    indexLookup.setLeftKey(leftKey);
    byte[] originalLeftBytes = cobj.getByteArray(ComObject.Tag.ORIGINAL_LEFT_KEY);
    Object[] originalLeftKey = null;
    if (originalLeftBytes != null) {
      originalLeftKey = DatabaseCommon.deserializeTypedKey(originalLeftBytes);
    }
    indexLookup.setOriginalLeftKey(originalLeftKey);
    indexLookup.setLeftOperator(BinaryExpression.Operator.getOperator(cobj.getInt(ComObject.Tag.LEFT_OPERATOR)));
  }

  private void setRightKeyAndOperator(ComObject cobj, IndexLookup indexLookup) {
    byte[] rightBytes = cobj.getByteArray(ComObject.Tag.RIGHT_KEY);
    byte[] originalRightBytes = cobj.getByteArray(ComObject.Tag.ORIGINAL_RIGHT_KEY);
    if (rightBytes != null) {
      indexLookup.setRightKey(DatabaseCommon.deserializeTypedKey(rightBytes));
    }
    if (originalRightBytes != null) {
      indexLookup.setOriginalRightKey(DatabaseCommon.deserializeTypedKey(originalRightBytes));
    }

    if (cobj.getInt(ComObject.Tag.RIGHT_OPERATOR) != null) {
      indexLookup.setRightOperator(BinaryExpression.Operator.getOperator(cobj.getInt(ComObject.Tag.RIGHT_OPERATOR)));
    }
  }

  private void setColumnOffsets(ComObject cobj, IndexLookup indexLookup) {
    Set<Integer> columnOffsets = null;
    ComArray cOffsets = cobj.getArray(ComObject.Tag.COLUMN_OFFSETS);
    columnOffsets = new HashSet<>();
    for (Object obj : cOffsets.getArray()) {
      columnOffsets.add((Integer)obj);
    }
    indexLookup.setColumnOffsets(columnOffsets);
  }

  private Counter[] getCounters(ComObject cobj, IndexLookup indexLookup) throws IOException {
    Counter[] counters = null;
    ComArray counterArray = cobj.getArray(ComObject.Tag.COUNTERS);
    if (counterArray != null && !counterArray.getArray().isEmpty()) {
      counters = new Counter[counterArray.getArray().size()];
      for (int i = 0; i < counters.length; i++) {
        counters[i] = new Counter();
        counters[i].deserialize((byte[])counterArray.getArray().get(i));
      }
    }
    indexLookup.setCounters(counters);
    return counters;
  }

  private void setAscending(IndexLookup indexLookup, TableSchema tableSchema, IndexSchema indexSchema,
                            List<OrderByExpressionImpl> orderByExpressions) {
    Boolean ascending = null;
    if (orderByExpressions != null && !orderByExpressions.isEmpty()) {
      OrderByExpressionImpl orderByExpression = orderByExpressions.get(0);
      String columnName = orderByExpression.getColumnName();
      boolean isAscending = orderByExpression.isAscending();
      if (orderByExpression.getTableName() == null || !orderByExpression.getTableName().equals(tableSchema.getName()) ||
          columnName.equals(indexSchema.getFields()[0])) {
        ascending = isAscending;
      }
    }
    indexLookup.setAscending(ascending);
  }

  private void getOrderByExpressions(List<OrderByExpressionImpl> orderByExpressions, ComArray oarray) throws IOException {
    if (oarray != null) {
      for (Object entry : oarray.getArray()) {
        OrderByExpressionImpl orderByExpression = new OrderByExpressionImpl();
        orderByExpression.deserialize((byte[]) entry);
        orderByExpressions.add(orderByExpression);
      }
    }
  }

  @SchemaReadLock
  public ComObject closeResultSet(ComObject cobj, boolean replayedCommand) {
    long resultSetId = cobj.getLong(ComObject.Tag.RESULT_SET_ID);

    DiskBasedResultSet resultSet = new DiskBasedResultSet(server, resultSetId);
    resultSet.delete();

    return null;
  }

  @SchemaReadLock
  public ComObject serverSelectDelete(ComObject cobj, boolean replayedCommand) {
    long id = cobj.getLong(ComObject.Tag.ID);

    DiskBasedResultSet resultSet = new DiskBasedResultSet(server, id);
    resultSet.delete();
    return null;
  }

  @SchemaReadLock
  public ComObject serverSelect(ComObject cobj, boolean replayedCommand) {
    return serverSelect(cobj,
        false,null);
  }

  ComObject serverSelect(ComObject cobj, boolean restrictToThisServer, StoredProcedureContextImpl procedureContext) {
    try {
      int schemaRetryCount = 0;
      if (server.getBatchRepartCount().get() != 0 && lookupCount.incrementAndGet() % 1000 == 0) {
        ThreadUtil.sleep(10);
      }

      SelectStatementImpl.Explain explain = null;
      Boolean shouldExplain = cobj.getBoolean(ComObject.Tag.SHOULD_EXPLAIN);
      if (shouldExplain != null && shouldExplain) {
        explain = new SelectStatementImpl.Explain();
      }

      String dbName = cobj.getString(ComObject.Tag.DB_NAME);
      Integer schemaVersion = cobj.getInt(ComObject.Tag.SCHEMA_VERSION);
      if (schemaVersion != null && schemaVersion < server.getSchemaVersion()) {
        throw new SchemaOutOfSyncException(CURR_VER_STR + server.getCommon().getSchemaVersion() + ":");
      }
      int count = cobj.getInt(ComObject.Tag.COUNT);

      byte[] selectBytes = cobj.getByteArray(ComObject.Tag.LEGACY_SELECT_STATEMENT);
      SelectStatementImpl select = new SelectStatementImpl(server.getDatabaseClient());
      select.deserialize(selectBytes);
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
        ResultSetImpl resultSet = (ResultSetImpl) select.execute(dbName, null, explain,
            null, null,
            null, restrictToThisServer, procedureContext, schemaRetryCount);

        ExpressionImpl.CachedRecord[][] results = resultSet.getReadRecordsAndSerializedRecords();
        if (results != null) {
          ComObject retObj = processServerSelectResults(cobj, count, select, offset, limit, results);
          if (retObj != null) {
            return retObj;
          }
        }

        diskResults = new DiskBasedResultSet(cobj.getShort(ComObject.Tag.SERIALIZATION_VERSION), dbName, server,
            select.getOffset(), select.getLimit(),
            select.getTableNames(), new int[]{0}, new ResultSetImpl[]{resultSet}, select.getOrderByExpressions(),
            count, select, false);
      }
      else {
        diskResults = new DiskBasedResultSet(server, select, select.getTableNames(), select.getServerSelectResultSetId(),
            restrictToThisServer, procedureContext);
      }
      select.setServerSelectResultSetId(diskResults.getResultSetId());
      byte[][][] records = diskResults.nextPage(select.getServerSelectPageNumber());

      return serverSelectProcessResponse(cobj, explain, select, offset, limit, records);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private ComObject serverSelectProcessResponse(ComObject cobj, SelectStatementImpl.Explain explain, SelectStatementImpl select, Offset offset, Limit limit,
                                                byte[][][] records) {
    ComObject retObj = new ComObject();
    select.setIsOnServer(false);
    retObj.put(ComObject.Tag.LEGACY_SELECT_STATEMENT, select.serialize());

    long currOffset = 0;
    if (cobj.getLong(ComObject.Tag.CURR_OFFSET) != null) {
      currOffset = cobj.getLong(ComObject.Tag.CURR_OFFSET);
    }
    long countReturned = 0;
    if (cobj.getLong(ComObject.Tag.COUNT_RETURNED) != null) {
      countReturned = cobj.getLong(ComObject.Tag.COUNT_RETURNED);
    }
    if (records != null) {
      ComArray tableArray = retObj.putArray(ComObject.Tag.TABLE_RECORDS, ComObject.Type.ARRAY_TYPE);
      ServerSelectProcessRecordsForResponse serverSelectProcessRecordsForResponse = new ServerSelectProcessRecordsForResponse(
          offset, limit, records, currOffset, countReturned, tableArray).invoke();
      currOffset = serverSelectProcessRecordsForResponse.getCurrOffset();
      countReturned = serverSelectProcessRecordsForResponse.getCountReturned();
    }
    if (explain != null) {
      retObj.put(ComObject.Tag.EXPLAIN, explain.getBuilder().toString());
    }
    retObj.put(ComObject.Tag.CURR_OFFSET, currOffset);
    retObj.put(ComObject.Tag.COUNT_RETURNED, countReturned);
    return retObj;
  }

  private ComObject processServerSelectResults(ComObject cobj, int count, SelectStatementImpl select, Offset offset,
                                               Limit limit, ExpressionImpl.CachedRecord[][] results) {
    int currCount = 0;
    for (ExpressionImpl.CachedRecord[] records : results) {
      currCount += records.length;
    }
    if (currCount < count) {
      // exhausted results

      ComObject retObj = new ComObject();
      select.setIsOnServer(false);
      retObj.put(ComObject.Tag.LEGACY_SELECT_STATEMENT, select.serialize());

      long currOffset = 0;
      if (cobj.getLong(ComObject.Tag.CURR_OFFSET) != null) {
        currOffset = cobj.getLong(ComObject.Tag.CURR_OFFSET);
      }
      long countReturned = 0;
      if (cobj.getLong(ComObject.Tag.COUNT_RETURNED) != null) {
        countReturned = cobj.getLong(ComObject.Tag.COUNT_RETURNED);
      }
      ComArray tableArray = retObj.putArray(ComObject.Tag.TABLE_RECORDS, ComObject.Type.ARRAY_TYPE);

      DoProcessServerSelectResults doProcessServerSelectResults = new DoProcessServerSelectResults(offset, limit,
          results, currOffset, countReturned, tableArray).invoke();
      currOffset = doProcessServerSelectResults.getCurrOffset();
      countReturned = doProcessServerSelectResults.getCountReturned();

      retObj.put(ComObject.Tag.CURR_OFFSET, currOffset);
      retObj.put(ComObject.Tag.COUNT_RETURNED, countReturned);
      return retObj;
    }
    return null;
  }

  @SchemaReadLock
  public ComObject serverSetSelect(ComObject cobj, boolean replayedCommand) {
    return serverSetSelect(cobj, false, null);
  }

  ComObject serverSetSelect(ComObject cobj, final boolean restrictToThisServer,
                            final StoredProcedureContextImpl procedureContext) {
    try {
      final int schemaRetryCount = 0;
      if (server.getBatchRepartCount().get() != 0 && lookupCount.incrementAndGet() % 1000 == 0) {
        ThreadUtil.sleep(10);
      }

      final String dbName = cobj.getString(ComObject.Tag.DB_NAME);
      ComArray array = cobj.getArray(ComObject.Tag.SELECT_STATEMENTS);
      final SelectStatementImpl[] selectStatements = new SelectStatementImpl[array.getArray().size()];
      for (int i = 0; i < array.getArray().size(); i++) {
        SelectStatementImpl stmt = new SelectStatementImpl(server.getClient());
        stmt.deserialize((byte[])array.getArray().get(i));
        selectStatements[i] = stmt;
      }
      ComArray tablesArray = cobj.getArray(ComObject.Tag.TABLES);
      String[] tableNames = new String[tablesArray.getArray().size()];
      TableSchema[] tableSchemas = new TableSchema[tableNames.length];
      for (int i = 0; i < array.getArray().size(); i++) {
        tableNames[i] = (String)tablesArray.getArray().get(i);
        tableSchemas[i] = server.getCommon().getTableSchema(dbName, tableNames[i], server.getDataDir());
      }

      List<OrderByExpressionImpl> orderByExpressions = new ArrayList<>();
      ComArray orderByArray = cobj.getArray(ComObject.Tag.ORDER_BY_EXPRESSIONS);
      if (orderByArray != null) {
        for (int i = 0; i < orderByArray.getArray().size(); i++) {
          OrderByExpressionImpl orderBy = new OrderByExpressionImpl();
          orderBy.deserialize((byte[]) orderByArray.getArray().get(i));
          orderByExpressions.add(orderBy);
        }
      }

      boolean notAll = false;
      ComArray operationsArray = cobj.getArray(ComObject.Tag.OPERATIONS);
      String[] operations = new String[operationsArray.getArray().size()];
      for (int i = 0; i < operations.length; i++) {
        operations[i] = (String) operationsArray.getArray().get(i);
        if (!operations[i].toUpperCase().endsWith("ALL")) {
          notAll = true;
        }
      }

      long serverSelectPageNumber = cobj.getLong(ComObject.Tag.SERVER_SELECT_PAGE_NUMBER);
      long resultSetId = cobj.getLong(ComObject.Tag.RESULT_SET_ID);

      int count = cobj.getInt(ComObject.Tag.COUNT);

      DiskBasedResultSet diskResults = null;
      if (serverSelectPageNumber == 0) {
        diskResults = doInitialServerSetSelect(cobj, restrictToThisServer, procedureContext, schemaRetryCount, dbName,
            selectStatements, tableNames, orderByExpressions, notAll, operations, count);
      }
      else {
        diskResults = new DiskBasedResultSet(server, null, tableNames, resultSetId, restrictToThisServer,
            procedureContext);
      }
      byte[][][] records = diskResults.nextPage((int)serverSelectPageNumber);

      return processServerSetSelectResults(serverSelectPageNumber, diskResults, records);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private ComObject processServerSetSelectResults(long serverSelectPageNumber, DiskBasedResultSet diskResults,
                                                  byte[][][] records) {
    ComObject retObj = new ComObject();

    retObj.put(ComObject.Tag.RESULT_SET_ID, diskResults.getResultSetId());
    retObj.put(ComObject.Tag.SERVER_SELECT_PAGE_NUMBER, serverSelectPageNumber + 1);
    retObj.put(ComObject.Tag.SHARD, server.getShard());
    retObj.put(ComObject.Tag.REPLICA, server.getReplica());

    if (records != null) {
      ComArray tableArray = retObj.putArray(ComObject.Tag.TABLE_RECORDS, ComObject.Type.ARRAY_TYPE);
      for (byte[][] tableRecords : records) {
        ComArray recordArray = tableArray.addArray(ComObject.Type.BYTE_ARRAY_TYPE);

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

  private DiskBasedResultSet doInitialServerSetSelect(ComObject cobj, final boolean restrictToThisServer,
                                                      final StoredProcedureContextImpl procedureContext,
                                                      final int schemaRetryCount, final String dbName,
                                                      final SelectStatementImpl[] selectStatements, String[] tableNames,
                                                      List<OrderByExpressionImpl> orderByExpressions, boolean notAll,
                                                      String[] operations, int count) throws InterruptedException,
      java.util.concurrent.ExecutionException {
    DiskBasedResultSet diskResults;
    ThreadPoolExecutor executor = ThreadUtil.createExecutor(selectStatements.length, "SonicBase ReadManager serverSetSelect Thread");
    final ResultSetImpl[] resultSets = new ResultSetImpl[selectStatements.length];
    try {
      List<Future> futures = new ArrayList<>();
      for (int i = 0; i < selectStatements.length; i++) {
        final int offset = i;
        futures.add(executor.submit((Callable) () -> {
          SelectStatementImpl stmt = selectStatements[offset];
          stmt.setPageSize(1000);
          resultSets[offset] = (ResultSetImpl) stmt.execute(dbName, null, null, null, null,
              null, restrictToThisServer, procedureContext, schemaRetryCount);
          return null;
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
      diskResults = buildUniqueResultSet(cobj.getShort(ComObject.Tag.SERIALIZATION_VERSION),
          dbName, selectStatements, tableNames, resultSets, orderByExpressions, count, operations);
    }
    else {
      int[] offsets = new int[tableNames.length];
      for (int i = 0; i < offsets.length; i++) {
        offsets[i] = i;
      }
      diskResults = new DiskBasedResultSet(cobj.getShort(ComObject.Tag.SERIALIZATION_VERSION), dbName, server,
          null, null, tableNames, offsets, resultSets, orderByExpressions, count, null, true);
    }
    return diskResults;
  }

  private DiskBasedResultSet buildUniqueResultSet(final Short serializationVersion, final String dbName,
                                                  final SelectStatementImpl[] selectStatements, String[] tableNames,
                                                  final ResultSetImpl[] resultSets,
                                                  List<OrderByExpressionImpl> orderByExpressions, final int count,
                                                  String[] operations) {
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
      buildUniqResultSets(serializationVersion, dbName, selectStatements, resultSets, orderByUnique, diskResultSets);
    }

    Object ret = diskResultSets[0];
    for (int i = 1; i < resultSets.length; i++) {
      ret = buildUniqueResultSetsForRemainingResultSets(serializationVersion, dbName, selectStatements[0],
          orderByExpressions, count, operations[i - 1], diskResultSets, ret, i);
    }

    return (DiskBasedResultSet) ret;
  }

  private Object buildUniqueResultSetsForRemainingResultSets(
      Short serializationVersion, String dbName, SelectStatementImpl selectStatement,
      List<OrderByExpressionImpl> orderByExpressions, int count, String operation, Object[] diskResultSets, Object ret, int i) {
    boolean unique = !operation.toUpperCase().endsWith("ALL");
    boolean intersect = operation.toUpperCase().startsWith("INTERSECT");
    boolean except = operation.toUpperCase().startsWith("EXCEPT");
    List<String> tables = new ArrayList<>();
    String[] localTableNames = ret instanceof ResultSetImpl ?
        ((ResultSetImpl)ret).getTableNames() : ((DiskBasedResultSet)ret).getTableNames();
    tables.addAll(Arrays.asList(localTableNames));
    localTableNames = diskResultSets[i] instanceof ResultSetImpl ?
        ((ResultSetImpl)diskResultSets[i]).getTableNames() : ((DiskBasedResultSet)diskResultSets[i]).getTableNames();
    tables.addAll(Arrays.asList(localTableNames));
    ret = new DiskBasedResultSet(serializationVersion, dbName, server,
        tables.toArray(new String[tables.size()]), new Object[]{ret, diskResultSets[i]},
        orderByExpressions, count, unique, intersect, except, selectStatement.getSelectColumns());
    return ret;
  }

  private void buildUniqResultSets(Short serializationVersion, String dbName, SelectStatementImpl[] selectStatements,
                                   ResultSetImpl[] resultSets, List<OrderByExpressionImpl> orderByUnique, Object[] diskResultSets) {
    ThreadPoolExecutor executor = ThreadUtil.createExecutor(resultSets.length,
        "SonicBase ReadManager buildUniqueResultSet Thread");
    List<Future> futures = new ArrayList<>();
    try {
      for (int i = 0; i < resultSets.length; i++) {
        final int offset = i;
        futures.add(executor.submit((Callable) () -> new DiskBasedResultSet(serializationVersion, dbName, server,
            null, null,
            new String[]{selectStatements[offset].getFromTable()},
            new int[]{offset}, new ResultSetImpl[]{resultSets[offset]}, orderByUnique, 30_000, null,
            true)));
      }
      for (int i = 0; i < futures.size(); i++) {
        diskResultSets[i] = futures.get(i).get();
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
    finally {
      executor.shutdownNow();
    }
  }

  @SchemaReadLock
  public ComObject indexLookupExpression(ComObject cobj, boolean replayedCommand) {
    return indexLookupExpression(cobj, null);
  }

  ComObject indexLookupExpression(ComObject cobj, StoredProcedureContextImpl procedureContext) {
    try {
      IndexLookupWithExpression indexLookup = new IndexLookupWithExpression(server);
      String dbName = cobj.getString(ComObject.Tag.DB_NAME);
      indexLookup.setDbName(dbName);
      Integer schemaVersion = cobj.getInt(ComObject.Tag.SCHEMA_VERSION);
      if (schemaVersion != null && schemaVersion < server.getSchemaVersion()) {
        throw new SchemaOutOfSyncException(CURR_VER_STR + server.getCommon().getSchemaVersion() + ":");
      }
      int count = cobj.getInt(ComObject.Tag.COUNT);
      indexLookup.setCount(count);
      AtomicLong currOffset = new AtomicLong(cobj.getLong(ComObject.Tag.CURR_OFFSET));
      Long limit = cobj.getLong(ComObject.Tag.LIMIT_LONG);
      Long offset = cobj.getLong(ComObject.Tag.OFFSET_LONG);
      indexLookup.setCurrOffset(currOffset);
      indexLookup.setLimit(limit);
      indexLookup.setOffset(offset);

      int tableId = cobj.getInt(ComObject.Tag.TABLE_ID);

      setParms(cobj, indexLookup);

      byte[] expressionBytes = cobj.getByteArray(ComObject.Tag.LEGACY_EXPRESSION);
      Expression expression = null;
      if (expressionBytes != null) {
        expression = ExpressionImpl.deserializeExpression(expressionBytes);
      }
      indexLookup.setExpression(expression);
      TableSchema tableSchema = server.getCommon().getTablesById(dbName).get(tableId);
      String tableName = tableSchema.getName();
      String indexName = getIndexName(dbName, tableId, tableName, tableSchema);

      List<OrderByExpressionImpl> orderByExpressions = setOrderByExpressions(cobj);

      long viewVersion = cobj.getLong(ComObject.Tag.VIEW_VERSION);
      indexLookup.setViewVersion(viewVersion);
      tableSchema = server.getCommon().getTableSchema(dbName, tableName, server.getDataDir());
      IndexSchema indexSchema = server.getIndexSchema(dbName, tableSchema.getName(), indexName);
      indexLookup.setTableSchema(tableSchema);
      indexLookup.setIndexSchema(indexSchema);

      setLeftKeyAndOperator(cobj, indexLookup);
      setRightKeyAndOperator(cobj, indexLookup);

      ComArray cOffsets = cobj.getArray(ComObject.Tag.COLUMN_OFFSETS);
      Set<Integer> columnOffsets = new HashSet<>();
      for (Object obj : cOffsets.getArray()) {
        columnOffsets.add((Integer)obj);
      }
      indexLookup.setColumnOffsets(columnOffsets);

      Counter[] counters = setCounters(cobj, indexLookup);

      GroupByContext groupByContext = setGroupByContext(cobj, indexLookup);

      Boolean isProbe = cobj.getBoolean(ComObject.Tag.IS_PROBE);
      if (isProbe == null) {
        isProbe = false;
      }
      indexLookup.setIsProbe(isProbe);
      Index index = server.getIndex(dbName, tableSchema.getName(), indexName);
      Map.Entry<Object[], Object> entry = null;
      indexLookup.setIndex(index);
      setAscending(indexLookup, orderByExpressions, indexSchema);

      List<byte[]> retRecords = new ArrayList<>();
      indexLookup.setRetRecords(retRecords);

      entry = indexLookup.lookup();

      return indexLookupExpressionProcessResponse(currOffset, indexName, tableSchema, counters, groupByContext, entry, retRecords);
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  private void setParms(ComObject cobj, IndexLookupWithExpression indexLookup) {
    byte[] parmBytes = cobj.getByteArray(ComObject.Tag.PARMS);
    ParameterHandler parms = null;
    if (parmBytes != null) {
      parms = new ParameterHandler();
      parms.deserialize(parmBytes);
    }
    indexLookup.setParms(parms);
  }

  private void setAscending(IndexLookupWithExpression indexLookup, List<OrderByExpressionImpl> orderByExpressions,
                            IndexSchema indexSchema) {
    Boolean ascending = null;
    if (!orderByExpressions.isEmpty()) {
      OrderByExpressionImpl orderByExpression = orderByExpressions.get(0);
      String columnName = orderByExpression.getColumnName();
      boolean isAscending = orderByExpression.isAscending();
      if (columnName.equals(indexSchema.getFields()[0])) {
        ascending = isAscending;
      }
    }
    indexLookup.setAscending(ascending);
  }

  private String getIndexName(String dbName, int tableId, String tableName, TableSchema tableSchema) {
    String indexName = null;
    try {
      for (Map.Entry<String, IndexSchema> entry : tableSchema.getIndices().entrySet()) {
        if (entry.getValue().isPrimaryKey()) {
          indexName = entry.getKey();
        }
      }
    }
    catch (Exception e) {
      logger.info("indexLookup: tableName=" + tableName + ", tableid=" + tableId + ", tableByNameCount=" +
          server.getCommon().getTables(dbName).size() + ", tableCount=" + server.getCommon().getTablesById(dbName).size() +
          ", tableNull=" + (server.getCommon().getTablesById(dbName).get(tableId) == null) + ", indexName=" +
          indexName + ", indexName=" + indexName +
          ", indexNull=" /*+ (common.getTablesById().get(tableId).getIndexesById().get(indexId) == null) */);
      throw e;
    }
    return indexName;
  }

  private List<OrderByExpressionImpl> setOrderByExpressions(ComObject cobj) throws IOException {
    ComArray orderByArray = cobj.getArray(ComObject.Tag.ORDER_BY_EXPRESSIONS);
    List<OrderByExpressionImpl> orderByExpressions = new ArrayList<>();
    if (orderByArray != null) {
      for (int i = 0; i < orderByArray.getArray().size(); i++) {
        OrderByExpressionImpl orderByExpression = new OrderByExpressionImpl();
        orderByExpression.deserialize((byte[])orderByArray.getArray().get(i));
        orderByExpressions.add(orderByExpression);
      }
    }
    return orderByExpressions;
  }

  private void setLeftKeyAndOperator(ComObject cobj, IndexLookupWithExpression indexLookup) {
    byte[] leftKeyBytes = cobj.getByteArray(ComObject.Tag.LEFT_KEY);
    Object[] leftKey = null;
    if (leftKeyBytes != null) {
      leftKey = DatabaseCommon.deserializeTypedKey(leftKeyBytes);
      indexLookup.setLeftKey(leftKey);
    }
  }

  private void setRightKeyAndOperator(ComObject cobj, IndexLookupWithExpression indexLookup) {
    byte[] rightKeyBytes = cobj.getByteArray(ComObject.Tag.RIGHT_KEY);
    Object[] rightKey = null;
    if (rightKeyBytes != null) {
      rightKey = DatabaseCommon.deserializeTypedKey(rightKeyBytes);
      indexLookup.setRightKey(rightKey);
    }

    Integer rightOpValue = cobj.getInt(ComObject.Tag.RIGHT_OPERATOR);
    BinaryExpression.Operator rightOperator = null;
    if (rightOpValue != null) {
      rightOperator = BinaryExpression.Operator.getOperator(rightOpValue);
      indexLookup.setRightOperator(rightOperator);
    }
  }

  private Counter[] setCounters(ComObject cobj, IndexLookupWithExpression indexLookup) throws IOException {
    ComArray countersArray = cobj.getArray(ComObject.Tag.COUNTERS);
    Counter[] counters = null;
    if (countersArray != null) {
      counters = new Counter[countersArray.getArray().size()];
      for (int i = 0; i < counters.length; i++) {
        counters[i] = new Counter();
        counters[i].deserialize((byte[])countersArray.getArray().get(i));
      }
    }
    indexLookup.setCounters(counters);
    return counters;
  }

  private ComObject indexLookupExpressionProcessResponse(
      AtomicLong currOffset, String indexName, TableSchema tableSchema, Counter[] counters, GroupByContext groupByContext,
      Map.Entry<Object[], Object> entry, List<byte[]> retRecords) throws IOException {
    ComArray countersArray;
    ComObject retObj = new ComObject();
    if (entry != null) {
      retObj.put(ComObject.Tag.KEY_BYTES, DatabaseCommon.serializeKey(tableSchema, indexName, entry.getKey()));
    }

    ComArray records = retObj.putArray(ComObject.Tag.RECORDS, ComObject.Type.BYTE_ARRAY_TYPE);
    for (byte[] record : retRecords) {
      records.add(record);
    }

    if (counters != null) {
      countersArray = retObj.putArray(ComObject.Tag.COUNTERS, ComObject.Type.BYTE_ARRAY_TYPE);
      for (int i = 0; i < counters.length; i++) {
        countersArray.add(counters[i].serialize());
      }
    }

    if (groupByContext != null) {
      retObj.put(ComObject.Tag.LEGACY_GROUP_CONTEXT, groupByContext.serialize(server.getCommon()));
    }

    retObj.put(ComObject.Tag.CURR_OFFSET, currOffset.get());

    return retObj;
  }


  @SchemaReadLock
  public ComObject evaluateCounterGetKeys(ComObject cobj, boolean replayedCommand) {

    String dbName = cobj.getString(ComObject.Tag.DB_NAME);

    Counter counter = new Counter();
    try {
      byte[] counterBytes = cobj.getByteArray(ComObject.Tag.LEGACY_COUNTER);
      counter.deserialize(counterBytes);

      boolean isPrimaryKey = false;
      String tableName = counter.getTableName();
      String columnName = counter.getColumnName();
      String indexName = null;
      TableSchema tableSchema = server.getCommon().getTableSchema(dbName, tableName, server.getDataDir());
      for (IndexSchema indexSchema : tableSchema.getIndices().values()) {
        if (indexSchema.getFields()[0].equals(columnName)) {
          isPrimaryKey = indexSchema.isPrimaryKey();
          indexName = indexSchema.getName();
        }
      }
      byte[] maxKey = null;
      byte[] minKey = null;
      Index index = server.getIndex(dbName, tableName, indexName);
      Map.Entry<Object[], Object> entry = index.lastEntry();

      maxKey = evaluateCounterProcessEntry(index, isPrimaryKey, indexName, tableSchema, maxKey, entry);
      minKey = evaluateCounterProcessFirstEntry(isPrimaryKey, indexName, tableSchema, minKey, index);

      if (minKey == null || maxKey == null) {
        logger.error("minkey==null || maxkey==null");
      }
      return evaluateCounterProcessResponse(counter, maxKey, minKey);
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  private byte[] evaluateCounterProcessEntry(Index index, boolean isPrimaryKey, String indexName, TableSchema tableSchema,
                                             byte[] maxKey, Map.Entry<Object[], Object> entry) {
    if (entry != null) {
      byte[][] records = null;
      Object[] key = entry.getKey();
      synchronized (index.getMutex(key)) {
        Object unsafeAddress = index.get(key);
        if (unsafeAddress != null && !unsafeAddress.equals(0L)) {
          records = server.getAddressMap().fromUnsafeToRecords(unsafeAddress);
        }
      }
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
    return maxKey;
  }

  private byte[] evaluateCounterProcessFirstEntry(boolean isPrimaryKey, String indexName, TableSchema tableSchema,
                                                  byte[] minKey, Index index) {
    Map.Entry<Object[], Object> entry = index.firstEntry();
    minKey = evaluateCounterProcessEntry(index, isPrimaryKey, indexName, tableSchema, minKey, entry);
    return minKey;
  }

  private ComObject evaluateCounterProcessResponse(Counter counter, byte[] maxKey, byte[] minKey) throws IOException {
    ComObject retObj = new ComObject();
    if (minKey != null) {
      retObj.put(ComObject.Tag.MIN_KEY, minKey);
    }
    if (maxKey != null) {
      retObj.put(ComObject.Tag.MAX_KEY, maxKey);
    }
    retObj.put(ComObject.Tag.LEGACY_COUNTER, counter.serialize());
    return retObj;
  }

  @SchemaReadLock
  public ComObject evaluateCounterWithRecord(ComObject cobj, boolean replayedCommand) {

    String dbName = cobj.getString(ComObject.Tag.DB_NAME);

    Counter counter = new Counter();
    try {
      byte[] minKeyBytes = cobj.getByteArray(ComObject.Tag.MIN_KEY);
      byte[] maxKeyBytes = cobj.getByteArray(ComObject.Tag.MAX_KEY);
      byte[] counterBytes = cobj.getByteArray(ComObject.Tag.LEGACY_COUNTER);
      counter.deserialize(counterBytes);

      String tableName = counter.getTableName();
      String columnName = counter.getColumnName();
      String indexName = null;
      TableSchema tableSchema = server.getCommon().getTableSchema(dbName, tableName, server.getDataDir());
      for (IndexSchema indexSchema : tableSchema.getIndices().values()) {
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
      byte[][] records = null;
      synchronized (index.getMutex(key)) {
        Object unsafeAddress = index.get(key);
        if (unsafeAddress != null) {
          if (!unsafeAddress.equals(0L)) {
            records = server.getAddressMap().fromUnsafeToRecords(unsafeAddress);
          }
        }
      }
      if (records != null) {
        doEvaluateCounter(new Record(dbName, server.getCommon(), records[0]), counter, minKeyBytes,
            tableSchema.getFieldOffset(columnName));
      }
      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.LEGACY_COUNTER, counter.serialize());
      return retObj;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  private void doEvaluateCounter(Record record1, Counter counter, byte[] minKeyBytes, Integer fieldOffset) {
    Object value = record1.getFields()[fieldOffset];
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


  private class SetOffsetLimit {
    private final ComObject cobj;
    private final IndexLookup indexLookup;
    private AtomicLong currOffset;
    private AtomicLong countReturned;

    SetOffsetLimit(ComObject cobj, IndexLookup indexLookup) {
      this.cobj = cobj;
      this.indexLookup = indexLookup;
    }

    public AtomicLong getCurrOffset() {
      return currOffset;
    }

    AtomicLong getCountReturned() {
      return countReturned;
    }

    public SetOffsetLimit invoke() {
      Long offset = cobj.getLong(ComObject.Tag.OFFSET_LONG);
      Long limit = cobj.getLong(ComObject.Tag.LIMIT_LONG);
      currOffset = new AtomicLong(cobj.getLong(ComObject.Tag.CURR_OFFSET));
      countReturned = new AtomicLong();
      if (cobj.getLong(ComObject.Tag.COUNT_RETURNED) != null) {
        countReturned.set(cobj.getLong(ComObject.Tag.COUNT_RETURNED));
      }
      indexLookup.setCurrOffset(currOffset);
      indexLookup.setCountReturned(countReturned);
      indexLookup.setOffset(offset);
      indexLookup.setLimit(limit);
      return this;
    }
  }

  private class ServerSelectProcessRecordsForResponse {
    private final Offset offset;
    private final Limit limit;
    private final byte[][][] records;
    private long currOffset;
    private long countReturned;
    private final ComArray tableArray;

    ServerSelectProcessRecordsForResponse(Offset offset, Limit limit, byte[][][] records, long currOffset,
                                          long countReturned, ComArray tableArray) {
      this.offset = offset;
      this.limit = limit;
      this.records = records;
      this.currOffset = currOffset;
      this.countReturned = countReturned;
      this.tableArray = tableArray;
    }

    public long getCurrOffset() {
      return currOffset;
    }

    long getCountReturned() {
      return countReturned;
    }

    public ServerSelectProcessRecordsForResponse invoke() {
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
            recordArray = tableArray.addArray(ComObject.Type.BYTE_ARRAY_TYPE);
          }
          currOffset++;
          countReturned++;
          recordArray.add(record);
        }
      }
      return this;
    }
  }

  private class DoProcessServerSelectResults {
    private final Offset offset;
    private final Limit limit;
    private final ExpressionImpl.CachedRecord[][] results;
    private long currOffset;
    private long countReturned;
    private final ComArray tableArray;

    DoProcessServerSelectResults(Offset offset, Limit limit, ExpressionImpl.CachedRecord[][] results,
                                 long currOffset, long countReturned, ComArray tableArray) {
      this.offset = offset;
      this.limit = limit;
      this.results = results;
      this.currOffset = currOffset;
      this.countReturned = countReturned;
      this.tableArray = tableArray;
    }

    public long getCurrOffset() {
      return currOffset;
    }

    long getCountReturned() {
      return countReturned;
    }

    public DoProcessServerSelectResults invoke() {
      for (ExpressionImpl.CachedRecord[] tableRecords : results) {
        if (doProcessServerSelectResultsProcessRecords(tableRecords)) {
          break;
        }
      }
      return this;
    }

    private boolean doProcessServerSelectResultsProcessRecords(ExpressionImpl.CachedRecord[] tableRecords) {
      ComArray recordArray = null;
      for (ExpressionImpl.CachedRecord record : tableRecords) {
        if (offset != null && currOffset < offset.getOffset()) {
          currOffset++;
          continue;
        }
        if (limit != null && countReturned >= limit.getRowCount()) {
          return true;
        }
        if (recordArray == null) {
          recordArray = tableArray.addArray(ComObject.Type.BYTE_ARRAY_TYPE);
        }
        currOffset++;
        countReturned++;
        byte[] bytes = record.getSerializedRecord();
        if (bytes == null) {
          bytes = record.getRecord().serialize(server.getCommon(), DatabaseClient.SERIALIZATION_VERSION);
        }
        recordArray.add(bytes);
      }
      return false;
    }
  }
}
