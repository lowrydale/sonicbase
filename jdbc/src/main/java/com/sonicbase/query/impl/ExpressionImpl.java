package com.sonicbase.query.impl;

import com.codahale.metrics.Timer;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.client.DatabaseServerProxy;
import com.sonicbase.common.*;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.procedure.StoredProcedureContextImpl;
import com.sonicbase.query.BinaryExpression;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.query.Expression;
import com.sonicbase.schema.DataType;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.PartitionUtils;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.Offset;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class ExpressionImpl implements Expression {

  private static Map<Integer, Type> typesById = new HashMap<>();
  private String tableName;
  private DatabaseClient client;
  private ParameterHandler parms;
  private Expression topLevelExpression;
  private List<OrderByExpressionImpl> orderByExpressions;
  private RecordCache recordCache;
  private int nextShard = -1;
  private Object[] nextKey;
  private List<ColumnImpl> columns;
  protected boolean debug;
  private Integer replica;
  private int viewVersion;
  private Counter[] counters;
  private GroupByContext groupByContext;
  protected String dbName;
  private boolean forceSelectOnServer;
  protected short serializationVersion = DatabaseClient.SERIALIZATION_VERSION;
  private int lastShard;
  private boolean isCurrPartitions;
  private boolean probe;
  private boolean restrictToThisServer;
  private StoredProcedureContextImpl procedureContext;

  public StoredProcedureContextImpl getProcedureContext() {
    return procedureContext;
  }

  public Counter[] getCounters() {
    return counters;
  }

  public boolean isRestrictToThisServer() {
    return restrictToThisServer;
  }

  public GroupByContext getGroupByContext() {
    return groupByContext;
  }

  public int getViewVersion() {
    return viewVersion;
  }

  public void setNextShard(int nextShard) {
    this.nextShard = nextShard;
  }

  public void setNextKey(Object[] nextKey) {
    this.nextKey = nextKey;
  }

  public RecordCache getRecordCache() {
    return recordCache;
  }

  public List<ColumnImpl> getColumns() {
    return columns;
  }

  public void setRecordCache(RecordCache recordCache) {
    this.recordCache = recordCache;
  }

  public Integer getReplica() {
    return replica;
  }

  public void setReplica(Integer replica) {
    this.replica = replica;
  }

  int getNextShard() {
    return nextShard;
  }

  Object[] getNextKey() {
    return nextKey;
  }

  public void setColumns(List<ColumnImpl> columns) {
    this.columns = columns;
  }

  public void reset() {
    nextShard = -1;
    nextKey = null;
  }

  public void setLastShard(int lastShard) {
    this.lastShard = lastShard;
  }

  public int getLastShard() {
    return lastShard;
  }

  public void setDebug(boolean debug) {
    this.debug = debug;
  }

  public void setViewVersion(int viewVersion) {
    this.viewVersion = viewVersion;
  }

  public void setCounters(Counter[] counters) {
    this.counters = counters;
  }

  public void setGroupByContext(GroupByContext groupByContext) {
    this.groupByContext = groupByContext;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  public void forceSelectOnServer(boolean forceSelectOnServer) {
    this.forceSelectOnServer = forceSelectOnServer;
  }

  public static void evaluateCounter(DatabaseCommon common, DatabaseClient client, String dbName,
                                     ExpressionImpl expression, IndexSchema indexSchema, Counter counter,
                                     SelectFunctionImpl function, boolean restrictToThisServer,
                                     StoredProcedureContextImpl procedureContext, int schemaRetryCount) throws IOException {
    if ((function.getName().equalsIgnoreCase("min") || function.getName().equalsIgnoreCase("max"))) {
      doLookupForMinOrMax(client, dbName, expression, indexSchema, counter, function, restrictToThisServer,
          procedureContext, schemaRetryCount);
    }
    else {
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.LEGACY_COUNTER, counter.serialize());
      cobj.put(ComObject.Tag.DB_NAME, dbName);
      cobj.put(ComObject.Tag.SCHEMA_VERSION, common.getSchemaVersion());

      Counter lastCounter = null;
      int shardCount = common.getServersConfig().getShardCount();

      for (int i = 0; i < shardCount; i++) {
        lastCounter = evaluateCounterGetKeys(common, client, dbName, counter, cobj, lastCounter, i);
      }
      counter.setMaxLong(lastCounter.getMaxLong());
      counter.setMinLong(lastCounter.getMinLong());
      counter.setMaxDouble(lastCounter.getMaxDouble());
      counter.setMinDouble(lastCounter.getMinDouble());
      counter.setCount(lastCounter.getCount());
    }
  }

  private static Counter evaluateCounterGetKeys(DatabaseCommon common, DatabaseClient client, String dbName,
                                                Counter counter, ComObject cobj, Counter lastCounter, int i) throws IOException {
    byte[] ret = client.send("ReadManager:evaluateCounterGetKeys", i, 0, cobj, DatabaseClient.Replica.DEF);
    ComObject retObj = new ComObject(ret);

    byte[] minKeyBytes = retObj.getByteArray(ComObject.Tag.MIN_KEY);
    byte[] maxKeyBytes = retObj.getByteArray(ComObject.Tag.MAX_KEY);

    Counter minCounter = null;
    if (minKeyBytes != null) {
      minCounter = getCounterValue(common, client, dbName, counter, minKeyBytes, false);
    }
    else {
      minCounter = new Counter();
    }
    Counter maxCounter = null;
    if (maxKeyBytes != null) {
      maxCounter = getCounterValue(common, client, dbName, counter, maxKeyBytes, false);
    }
    else {
      maxCounter = new Counter();
    }

    if (lastCounter != null) {
      setCounterValues(lastCounter, minCounter, maxCounter);
    }
    lastCounter = minCounter;
    return lastCounter;
  }

  private static void doLookupForMinOrMax(DatabaseClient client, String dbName, ExpressionImpl expression,
                                          IndexSchema indexSchema, Counter counter, SelectFunctionImpl function,
                                          boolean restrictToThisServer, StoredProcedureContextImpl procedureContext,
                                          int schemaRetryCount) {
    BinaryExpression.Operator op = BinaryExpression.Operator.GREATER;
    AtomicReference<String> usedIndex = new AtomicReference<>();
    Random rand = new Random(System.currentTimeMillis());
    int replica = rand.nextInt(client.getReplicaCount());

    List<OrderByExpressionImpl> orderBy = null;
    if (function.getName().equalsIgnoreCase("max")) {
      orderBy = new ArrayList<>();
      OrderByExpressionImpl ret = new OrderByExpressionImpl();
      ret.setTableName(counter.getTableName());
      ret.setColumnName(counter.getColumnName());
      ret.setAscending(false);
      orderBy.add(ret);
    }

    boolean found = false;
    Set<ColumnImpl> columns = new HashSet<>();
    expression.getColumns(columns);
    for (ColumnImpl column : columns) {
      if (column.getColumnName().equals(counter.getColumnName()) && column.getTableName().equals(counter.getTableName())) {
        found = true;
      }
    }
    if (!found) {
      ColumnImpl column = new ColumnImpl();
      column.setTableName(counter.getTableName());
      column.setColumnName(counter.getColumnName());
      columns.add(column);
    }
    List<ColumnImpl> columnList = new ArrayList<>();
    columnList.addAll(columns);

    ExpressionImpl expressionImpl = new ExpressionImpl();
    expressionImpl.setDbName(dbName);
    expressionImpl.setTableName(counter.getTableName());
    expressionImpl.setClient(client);
    expressionImpl.setReplica(replica);
    expressionImpl.setForceSelectOnServer(false);
    expressionImpl.setColumns(columnList);
    expressionImpl.setNextShard(-1);
    expressionImpl.setRecordCache(expression.getRecordCache());
    expressionImpl.setViewVersion(client.getCommon().getSchemaVersion());
    expressionImpl.setCounters(expression == null ? null : expression.getCounters());
    expressionImpl.setGroupByContext(expression == null ? null : expression.groupByContext);
    expressionImpl.setIsProbe(false);
    expressionImpl.setOrderByExpressions(orderBy);
    expressionImpl.setRestrictToThisServer(restrictToThisServer);
    expressionImpl.setProcedureContext(procedureContext);

    IndexLookup indexLookup = new IndexLookup();
    indexLookup.setCount(1);
    indexLookup.setIndexName(indexSchema.getName());
    indexLookup.setLeftOp(op);
    indexLookup.setColumnName(indexSchema.getFields()[0]);
    indexLookup.setSchemaRetryCount(schemaRetryCount);
    indexLookup.setUsedIndex(usedIndex);
    indexLookup.setEvaluateExpression(false);

    SelectContextImpl context = indexLookup.lookup(expressionImpl, expression);
    NextReturn ret = new NextReturn();
    ret.setTableNames(context.getTableNames());
    ret.setIds(context.getCurrKeys());

    if (context.getCurrKeys() != null) {
      CachedRecord record = expression.getRecordCache().get(counter.getTableName(), context.getCurrKeys()[0][0]);

      TableSchema tableSchema = client.getCommon().getTables(dbName).get(counter.getTableName());
      int offset = tableSchema.getFieldOffset(counter.getColumnName());
      if (function.getName().equalsIgnoreCase("max")) {
        counter.setMaxLong((Long) DataType.getLongConverter().convert(record.getRecord().getFields()[offset]));
        counter.setMaxDouble((Double) DataType.getDoubleConverter().convert(record.getRecord().getFields()[offset]));
      }
      else {
        counter.setMinLong((Long) DataType.getLongConverter().convert(record.getRecord().getFields()[offset]));
        counter.setMinDouble((Double) DataType.getDoubleConverter().convert(record.getRecord().getFields()[offset]));
      }
    }
  }

  private static void setCounterValues(Counter lastCounter, Counter minCounter, Counter maxCounter) {

    getMaxCounterValue(lastCounter, maxCounter);

    getMinCounterValue(lastCounter, minCounter);

    minCounter.setMaxLong(maxCounter.getMaxLong());
    minCounter.setMaxDouble(maxCounter.getMaxDouble());
  }

  private static void getMaxCounterValue(Counter lastCounter, Counter maxCounter) {
    Long maxLong = maxCounter.getMaxLong();
    Long lastMaxLong = lastCounter.getMaxLong();
    if (maxLong != null) {
      if (lastMaxLong != null) {
        maxCounter.setMaxLong(Math.max(maxLong, lastMaxLong));
      }
    }
    else {
      maxCounter.setMaxLong(lastMaxLong);
    }
    Double maxDouble = maxCounter.getMaxDouble();
    Double lastMaxDouble = lastCounter.getMaxDouble();
    if (maxDouble != null) {
      if (lastMaxDouble != null) {
        maxCounter.setMaxDouble(Math.max(maxDouble, lastMaxDouble));
      }
    }
    else {
      maxCounter.setMaxDouble(lastMaxDouble);
    }
  }

  private static void getMinCounterValue(Counter lastCounter, Counter minCounter) {
    Long minLong = minCounter.getMinLong();
    Long lastMinLong = lastCounter.getMinLong();
    if (minLong != null) {
      if (lastMinLong != null) {
        minCounter.setMinLong(Math.max(minLong, lastMinLong));
      }
    }
    else {
      minCounter.setMinLong(lastMinLong);
    }
    Double minDouble = minCounter.getMinDouble();
    Double lastMinDouble = lastCounter.getMinDouble();
    if (minDouble != null) {
      if (lastMinDouble != null) {
        minCounter.setMinDouble(Math.max(minDouble, lastMinDouble));
      }
    }
    else {
      minCounter.setMinDouble(lastMinDouble);
    }
  }

  private static Counter getCounterValue(DatabaseCommon common, DatabaseClient client, String dbName, Counter counter,
                                         byte[] keyBytes,
                                         boolean isMin) throws IOException {
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.LEGACY_COUNTER, counter.serialize());
    if (isMin) {
      cobj.put(ComObject.Tag.MIN_KEY, keyBytes);
    }
    else {
      cobj.put(ComObject.Tag.MAX_KEY, keyBytes);
    }
    cobj.put(ComObject.Tag.DB_NAME, dbName);
    cobj.put(ComObject.Tag.SCHEMA_VERSION, common.getSchemaVersion());
    cobj.put(ComObject.Tag.METHOD, "ReadManager:evaluateCounterWithRecord");

    String batchKey = "ReadManager:evaluateCounterWithRecord";

    TableSchema tableSchema = common.getTables(dbName).get(counter.getTableName());
    Object[] key = DatabaseCommon.deserializeKey(tableSchema, keyBytes);

    String indexName = null;
    IndexSchema indexSchema = null;
    for (Map.Entry<String, IndexSchema> entry : tableSchema.getIndices().entrySet()) {
      if (entry.getValue().isPrimaryKey()) {
        indexName = entry.getKey();
        indexSchema = entry.getValue();
      }
    }
    if (indexSchema == null) {
      throw new DatabaseException("primary index not found");
    }
    String[] indexFields = indexSchema.getFields();
    int[] fieldOffsets = new int[indexFields.length];
    for (int i = 0; i < indexFields.length; i++) {
      fieldOffsets[i] = tableSchema.getFieldOffset(indexFields[i]);
    }
    List<Integer> selectedShards = PartitionUtils.findOrderedPartitionForRecord(true,
        false, tableSchema,
        indexName, null, BinaryExpression.Operator.EQUAL, null, key, null);

    byte[] ret = client.send(batchKey, selectedShards.get(0), 0, cobj, DatabaseClient.Replica.DEF);
    ComObject retObj = new ComObject(ret);
    Counter retCounter = new Counter();
    byte[] counterBytes = retObj.getByteArray(ComObject.Tag.LEGACY_COUNTER);
    retCounter.deserialize(counterBytes);
    return retCounter;
  }

  public boolean isForceSelectOnServer() {
    return forceSelectOnServer;
  }

  public void getColumnsInExpression(List<ColumnImpl> columns) {
    //nothing to implement
  }

  public void setIsCurrPartitions(boolean isCurrPartitions) {
    this.isCurrPartitions = isCurrPartitions;
  }

  public boolean isCurrPartitions() {
    return isCurrPartitions;
  }

  public short getSerializationVersion() {
    return serializationVersion;
  }

  public void setProbe(boolean probe) {
    this.probe = probe;
  }

  public boolean isProbe() {
    return probe;
  }

  public void setRestrictToThisServer(boolean restrictToThisServer) {
    this.restrictToThisServer = restrictToThisServer;
  }

  public void setProcedureContext(StoredProcedureContextImpl procedureContext) {
    this.procedureContext = procedureContext;
  }

  public void setForceSelectOnServer(boolean forceSelectOnServer) {
    this.forceSelectOnServer = forceSelectOnServer;
  }

  public void setIsProbe(boolean isProbe) {
    this.probe = isProbe;
  }

  public enum Type {
    COLUMN(0),
    CONSTANT(1),
    PARAMETER(2),
    BINARY_OP(3),
    PARENTHESIS(4),
    IN_EXPRESSION(5),
    ALL_EXPRESSION(6),
    FUNCTION(7),
    SIGNED_EXPRESSION(8),
    BASE_EXPRESSION(9);

    private final int id;

    public int getId() {
      return id;
    }


    Type(int id) {
      this.id = id;
      typesById.put(id, this);
    }
  }

  public List<OrderByExpressionImpl> getOrderByExpressions() {
    return orderByExpressions;
  }

  public void setOrderByExpressions(List<OrderByExpressionImpl> orderByExpressions) {
    this.orderByExpressions = orderByExpressions;
  }

  public void getColumns(Set<ColumnImpl> columns) {
    //nothing to implement
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public void setClient(DatabaseClient client) {
    this.client = client;
  }

  public void setParms(ParameterHandler parms) {
    this.parms = parms;
  }

  public void setTopLevelExpression(Expression topLevelExpression) {
    this.topLevelExpression = topLevelExpression;
  }

  public Expression getTopLevelExpression() {
    return topLevelExpression;
  }

  public String getTableName() {
    return tableName;
  }

  public DatabaseClient getClient() {
    return client;
  }

  public ParameterHandler getParms() {
    return parms;
  }

  public void serialize(short serializationVersion, DataOutputStream out) {
    try {
      out.writeInt(nextShard);
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public Type getType() {
    return Type.BASE_EXPRESSION;
  }

  /**
   * ###############################
   * DON"T MODIFY THIS SERIALIZATION
   * ###############################
   */
  public void deserialize(short serializationVersion, DataInputStream in) {
    try {
      this.serializationVersion = serializationVersion;
      nextShard = in.readInt();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public Object evaluateSingleRecord(
      TableSchema[] tableSchemas, Record[] records, ParameterHandler parms) {
    return false;
  }


  public NextReturn next(SelectStatementImpl.Explain explain, AtomicLong currOffset, AtomicLong countReturned,
                         Limit limit, Offset offset, int schemaRetryCount) {
    return null;
  }

  public NextReturn next(int count, SelectStatementImpl.Explain explain, AtomicLong currOffset,
                         AtomicLong countReturned, Limit limit,
                                  Offset offset, boolean evaluateExpression, boolean analyze, int schemaRetryCount) {
    return null;
  }

  public boolean canUseIndex() {
    return false;
  }

  public boolean canSortWithIndex() {
    return false;
  }

  public void queryRewrite() {
    //nothing to implement
  }

  public ColumnImpl getPrimaryColumn() {
    return null;
  }

  public static byte[] serializeExpression(ExpressionImpl expression) {
    try {
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytesOut);
      serializeExpression(expression, out);
      out.close();
      return bytesOut.toByteArray();
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  /**
   * ###############################
   * DON"T MODIFY THIS SERIALIZATION
   * ###############################
   */
  public static void serializeExpression(ExpressionImpl expression, DataOutputStream out) {
    try {
      out.writeShort(expression.getSerializationVersion());
      out.writeInt(expression.getType().getId());
      expression.serialize(expression.getSerializationVersion(), out);
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public static ExpressionImpl deserializeExpression(byte[] bytes) {
    return deserializeExpression(new DataInputStream(new ByteArrayInputStream(bytes)));
  }

  /**
   * ###############################
   * DON"T MODIFY THIS SERIALIZATION
   * ###############################
   */
  public static ExpressionImpl deserializeExpression(DataInputStream in) {
    try {
      short serializationVersion = in.readShort();
      int type = in.readInt();
      switch (ExpressionImpl.typesById.get(type)) {
        case PARENTHESIS:
          ParenthesisImpl parens = new ParenthesisImpl();
          parens.deserialize(serializationVersion, in);
          return parens;
        case COLUMN:
          ColumnImpl column = new ColumnImpl();
          column.deserialize(serializationVersion, in);
          return column;
        case CONSTANT:
          ConstantImpl constant = new ConstantImpl();
          constant.deserialize(serializationVersion, in);
          return constant;
        case PARAMETER:
          ParameterImpl parameter = new ParameterImpl();
          parameter.deserialize(serializationVersion, in);
          return parameter;
        case IN_EXPRESSION:
          InExpressionImpl expression = new InExpressionImpl();
          expression.deserialize(serializationVersion, in);
          return expression;
        case BINARY_OP:
          BinaryExpressionImpl binaryOp = new BinaryExpressionImpl();
          binaryOp.deserialize(serializationVersion, in);
          return binaryOp;
        case ALL_EXPRESSION:
          AllRecordsExpressionImpl allExpression = new AllRecordsExpressionImpl();
          allExpression.deserialize(serializationVersion, in);
          return allExpression;
        case FUNCTION:
          FunctionImpl function = new FunctionImpl();
          function.deserialize(serializationVersion, in);
          return function;
      }
      return null;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }


  public static class CachedRecord {
    private Record record;
    private byte[] serializedRecord;

    public CachedRecord(Record record, byte[] serializedRecord) {
      this.record = record;
      this.serializedRecord = serializedRecord;
    }

    public Record getRecord() {
      return record;
    }

    public void setRecord(Record record) {
      this.record = record;
    }

    public byte[] getSerializedRecord() {
      return serializedRecord;
    }

    public void setSerializedRecord(byte[] serializedRecord) {
      this.serializedRecord = serializedRecord;
    }
  }

  public static class RecordCache {
    private Map<String, ConcurrentHashMap<Key, CachedRecord>> recordsForTable = new ConcurrentHashMap<>();

    public Map<String, ConcurrentHashMap<Key, CachedRecord>> getRecordsForTable() {
      return recordsForTable;
    }

    public void clear() {
      for (ConcurrentHashMap<Key, CachedRecord> records : recordsForTable.values()) {
        records.clear();
      }
    }

    static class Key {
      private int hashCode = 0;
      private Object[] key;

      public Key(Object[] key) {
        this.key = key;
        hashCode = 0;
        for (int i = 0; i < key.length; i++) {
          if (key[i] == null) {
            continue;
          }
          if (key[i] instanceof byte[]) {
            hashCode += Arrays.hashCode((byte[]) key[i]);
          }
          else if (key[i] instanceof Blob) {
            hashCode += Arrays.hashCode(((Blob) key[i]).getData());
          }
          else {
            hashCode += key[i].hashCode();
          }
        }
      }

      public int hashCode() {
        return hashCode;
      }

      public boolean equals(Object o) {
        if (!(o instanceof Key)) {
          return false;
        }
        return doCheckKeysEqual(o);
      }

      private boolean doCheckKeysEqual(Object o) {
        for (int i = 0; i < key.length; i++) {
          if (isEitherKeyNull(i, o)) {
            continue;
          }
          if (key[i] instanceof Long) {
            if (((Long) key[i]).equals(((Key) o).key[i])) {
              continue;
            }
            return false;
          }
          else if (key[i] instanceof byte[]) {
            if (Arrays.equals((byte[]) key[i], (byte[]) ((Key) o).key[i])) {
              continue;
            }
            return false;
          }
          else {
            if (key[i].equals(((Key) o).key[i])) {
              continue;
            }
            return false;
          }
        }
        return true;
      }

      private boolean isEitherKeyNull(int i, Object o) {
        return key[i] == null || ((Key) o).key[i] == null;
      }
    }


    public RecordCache() {

    }

    public boolean containsKey(String tableName, Object[] key) {
      ConcurrentHashMap<Key, CachedRecord> records = recordsForTable.get(tableName);
      if (records == null) {
        return false;
      }
      return records.containsKey(new Key(key));
    }

    public Map<Key, CachedRecord> getRecordsForTable(String tableName) {
      Map<Key, CachedRecord> records = recordsForTable.get(tableName);
      if (records == null) {
        synchronized (this) {
          records = recordsForTable.computeIfAbsent(tableName, k -> new ConcurrentHashMap<>());
        }
      }
      return records;
    }

    public CachedRecord get(String tableName, Object[] key) {
      Map<Key, CachedRecord> records = recordsForTable.get(tableName);
      if (records == null) {
        return null;
      }
      return records.get(new Key(key));
    }

    public void put(String tableName, Object[] key, CachedRecord record) {
      Map<Key, CachedRecord> records = recordsForTable.get(tableName);
      if (records == null) {
        synchronized (this) {
          records = recordsForTable.computeIfAbsent(tableName, k -> new ConcurrentHashMap<>());
        }
      }
      records.put(new Key(key), record);
    }
  }

  public static Map<Integer, Object[][]> readRecords(
      String dbName, final DatabaseClient client, int pageSize, boolean forceSelectOnServer, final TableSchema tableSchema,
      List<IdEntry> keysToRead, String[] columns, List<ColumnImpl> selectColumns, RecordCache recordCache,
      int viewVersion, boolean restrictToThisServer, StoredProcedureContextImpl procedureContext, int schemaRetryCount) {

    return doReadRecords(dbName, client, pageSize, forceSelectOnServer, tableSchema,
        keysToRead, columns, selectColumns, recordCache, viewVersion, restrictToThisServer, procedureContext, schemaRetryCount);
  }

  public static Map<Integer, Object[][]> doReadRecords(
      final String dbName, final DatabaseClient client, final int pageSize, final boolean forceSelectOnServer,
      final TableSchema tableSchema, List<IdEntry> keysToRead, String[] columns, final List<ColumnImpl> selectColumns,
      final RecordCache recordCache, final int viewVersion, final boolean restrictToThisServer,
      final StoredProcedureContextImpl procedureContext, final int schemaRetryCount) {
    try {
      final AtomicReference<Map.Entry<String, IndexSchema>> indexSchema = new AtomicReference<>();

      prepareToReadRecords(tableSchema, columns, indexSchema);

      Map<Integer, List<IdEntry>> partitionedValues = preparePartitionedValues(client, tableSchema, keysToRead, indexSchema);

      final HashMap<Integer, Object[][]> fullMap = new HashMap<>();
      List<Future> futures = new ArrayList<>();
      for (final Map.Entry<Integer, List<IdEntry>> entry : partitionedValues.entrySet()) {
        futures.add(client.getExecutor().submit((Callable) () -> {
          AtomicReference<String> usedIndex = new AtomicReference<>();
          return ExpressionImpl.batchLookupIds(dbName,
              client.getCommon(), client, forceSelectOnServer, pageSize, tableSchema,
              BinaryExpression.Operator.EQUAL, indexSchema.get(), selectColumns, entry.getValue(), entry.getKey(),
              usedIndex, recordCache, viewVersion, restrictToThisServer, procedureContext, schemaRetryCount);
        }));
      }

      String[] primaryKeyFields = null;
      for (Map.Entry<String, IndexSchema> entry : tableSchema.getIndices().entrySet()) {
        if (entry.getValue().isPrimaryKey()) {
          primaryKeyFields = entry.getValue().getFields();
          break;
        }
      }

      for (Future future : futures) {
        processResults(tableSchema, recordCache, fullMap, primaryKeyFields, future);
      }
      return fullMap;
    }
    catch (InterruptedException | ExecutionException e) {
      throw new DatabaseException(e);
    }
  }

  private static void prepareToReadRecords(TableSchema tableSchema, String[] columns,
                                           AtomicReference<Map.Entry<String, IndexSchema>> indexSchema) {
    int[] fieldOffsets;
    for (Map.Entry<String, IndexSchema> entry : tableSchema.getIndices().entrySet()) {
      String[] fields = entry.getValue().getFields();
      boolean shouldIndex = false;
      if (fields.length ==  columns.length) {
        shouldIndex = prepareToReadRecordsWhereFieldsLenEqualsColumnsLen(columns, fields, shouldIndex);
      }
      else {
        shouldIndex = prepareToReadRecordsWhereFieldsLenNotEqualsColumnsLen(columns, fields, shouldIndex);
      }
      if (shouldIndex) {
        indexSchema.set(entry);
        String[] indexFields = indexSchema.get().getValue().getFields();
        fieldOffsets = new int[indexFields.length];
        for (int l = 0; l < indexFields.length; l++) {
          fieldOffsets[l] = tableSchema.getFieldOffset(indexFields[l]);
        }
        break;
      }
    }
  }

  private static boolean prepareToReadRecordsWhereFieldsLenNotEqualsColumnsLen(String[] columns, String[] fields,
                                                                               boolean shouldIndex) {
    int columnsFound = 0;
    for (int i = 0; i < fields.length; i++) {
      boolean found = false;
      for (int j = 0; j < columns.length; j++) {
        if (fields[i].equals(columns[j])) {
          found = true;
        }
      }
      if (!found) {
        break;
      }
      else {
        columnsFound++;
      }
    }
    if (columnsFound >= 1) {
      shouldIndex = true;
    }
    return shouldIndex;
  }

  private static boolean prepareToReadRecordsWhereFieldsLenEqualsColumnsLen(String[] columns, String[] fields,
                                                                            boolean shouldIndex) {
    boolean foundAll = true;
    for (int i = 0; i < fields.length; i++) {
      boolean found = false;
      for (int j = 0; j < columns.length; j++) {
        if (fields[i].equals(columns[j])) {
          found = true;
          break;
        }
      }
      if (!found) {
        foundAll = false;
        break;
      }
    }
    if (foundAll) {
      shouldIndex = true;
    }
    return shouldIndex;
  }

  private static void processResults(TableSchema tableSchema, RecordCache recordCache,
                                     HashMap<Integer, Object[][]> fullMap, String[] primaryKeyFields,
                                     Future future) throws InterruptedException, ExecutionException {
    BatchLookupReturn curr = (BatchLookupReturn) future.get();
    if (curr.records.size() != 0) {
      for (Map.Entry<Integer, Record[]> entry : curr.records.entrySet()) {
        Object[][] keys = new Object[entry.getValue().length][];
        for (int i = 0; i < entry.getValue().length; i++) {
          Record record = entry.getValue()[i];

          Object[] key = new Object[primaryKeyFields.length];
          for (int j = 0; j < primaryKeyFields.length; j++) {
            key[j] = record.getFields()[tableSchema.getFieldOffset(primaryKeyFields[j])];
          }

          recordCache.put(tableSchema.getName(), key, new CachedRecord(record, null));

          keys[i] = key;
        }
        aggregateKeys(fullMap, entry.getKey(), keys);
      }
    }
    else {
      fullMap.putAll(curr.keys);
    }
  }

  private static Map<Integer, List<IdEntry>> preparePartitionedValues(DatabaseClient client, TableSchema tableSchema,
                                                                      List<IdEntry> keysToRead,
                                                                      AtomicReference<Map.Entry<String, IndexSchema>> indexSchema) {
    Map<Integer, List<IdEntry>> partitionedValues = new HashMap<>();
    for (int j = 0; j < client.getShardCount(); j++) {
      partitionedValues.put(j, new ArrayList<IdEntry>());
    }

    for (int i = 0; i < keysToRead.size(); i++) {
      List<Integer> selectedShards = PartitionUtils.findOrderedPartitionForRecord(
          true, false, tableSchema,
          indexSchema.get().getKey(), null, BinaryExpression.Operator.EQUAL,
          null, keysToRead.get(i).getValue(), null);
      if (selectedShards.isEmpty()) {
        throw new DatabaseException("No shards selected for query");
      }
      for (int selectedShard : selectedShards) {
        partitionedValues.get(selectedShard).add(new IdEntry(keysToRead.get(i).getOffset(), keysToRead.get(i).getValue()));
      }
    }
    return partitionedValues;
  }

  public static Record doReadRecord(
      String dbName, DatabaseClient client, boolean forceSelectOnServer, RecordCache recordCache, Object[] key,
      String tableName, List<ColumnImpl> columns, Expression expression,
      ParameterHandler parms, int viewVersion, boolean restrictToThisServer,
      StoredProcedureContextImpl procedureContext, int schemaRetryCount) {

    CachedRecord ret = recordCache.get(tableName, key);
    if (ret != null) {
      return ret.getRecord();
    }

    IndexSchema primaryKeyIndex = null;
    for (Map.Entry<String, IndexSchema> entry : client.getCommon().getTables(dbName).get(tableName).getIndices().entrySet()) {
      if (entry.getValue().isPrimaryKey()) {
        primaryKeyIndex = entry.getValue();
        break;
      }
    }
    if (primaryKeyIndex == null) {
      throw new DatabaseException("primary index not found: table=" + tableName);
    }
    int nextShard = -1;
    int replicaCount = client.getCommon().getServersConfig().getShards()[0].getReplicas().length;
    int replica = ThreadLocalRandom.current().nextInt(0, replicaCount);
    AtomicReference<String> usedIndex = new AtomicReference<>();

    ExpressionImpl expressionImpl = new ExpressionImpl();
    expressionImpl.setDbName(dbName);
    expressionImpl.setTableName(tableName);
    expressionImpl.setClient(client);
    expressionImpl.setReplica(replica);
    expressionImpl.setForceSelectOnServer(forceSelectOnServer);
    expressionImpl.setParms(parms);
    expressionImpl.setColumns(columns);
    expressionImpl.setNextShard(nextShard);
    expressionImpl.setRecordCache(recordCache);
    expressionImpl.setViewVersion(viewVersion);
    expressionImpl.setCounters(expression == null ? null : ((ExpressionImpl) expression).getCounters());
    expressionImpl.setGroupByContext(expression == null ? null : ((ExpressionImpl) expression).groupByContext);
    expressionImpl.setIsProbe(false);
    expressionImpl.setOrderByExpressions(null);
    expressionImpl.setRestrictToThisServer(restrictToThisServer);
    expressionImpl.setProcedureContext(procedureContext);

    IndexLookup indexLookup = new IndexLookup();
    indexLookup.setCount(1);
    indexLookup.setIndexName(primaryKeyIndex.getName());
    indexLookup.setLeftOp(BinaryExpression.Operator.EQUAL);
    indexLookup.setLeftKey(key);
    indexLookup.setLeftOriginalKey(key);
    indexLookup.setColumnName(primaryKeyIndex.getFields()[0]);
    indexLookup.setSchemaRetryCount(schemaRetryCount);
    indexLookup.setUsedIndex(usedIndex);
    indexLookup.setEvaluateExpression(false);

    SelectContextImpl context = indexLookup.lookup(expressionImpl, expression);
//
    Object[][][] currKeys = context.getCurrKeys();
    if (currKeys != null) {
      Object[] retKey = currKeys[0][0];
      if (retKey != null) {
        CachedRecord currRet = recordCache.get(tableName, retKey);
        if (currRet != null) {
          return currRet.getRecord();
        }
      }
    }
    return null;
  }

  public static Record doReadRecord(
      String dbName, DatabaseClient client, boolean forceSelectOnServer, ParameterHandler parms, Expression expression,
      RecordCache recordCache, Object[] key, String tableName, List<ColumnImpl> columns, int viewVersion,
      boolean restrictToThisServer, StoredProcedureContextImpl procedureContext, int schemaRetryCount) {

    IndexSchema primaryKeyIndex = null;
    for (Map.Entry<String, IndexSchema> entry : client.getCommon().getTables(dbName).get(tableName).getIndices().entrySet()) {
      if (entry.getValue().isPrimaryKey()) {
        primaryKeyIndex = entry.getValue();
        break;
      }
    }
    if (primaryKeyIndex == null) {
      throw new DatabaseException("primary index not found: table=" + tableName);
    }
    int nextShard = -1;
    int replicaCount = client.getCommon().getServersConfig().getShards()[0].getReplicas().length;
    int replica = ThreadLocalRandom.current().nextInt(0, replicaCount);
    AtomicReference<String> usedIndex = new AtomicReference<>();

    ExpressionImpl expressionImpl = new ExpressionImpl();
    expressionImpl.setDbName(dbName);
    expressionImpl.setTableName(tableName);
    expressionImpl.setClient(client);
    expressionImpl.setReplica(replica);
    expressionImpl.setForceSelectOnServer(forceSelectOnServer);
    expressionImpl.setParms(parms);
    expressionImpl.setColumns(columns);
    expressionImpl.setNextShard(nextShard);
    expressionImpl.setRecordCache(recordCache);
    expressionImpl.setViewVersion(viewVersion);
    expressionImpl.setCounters(expression == null ? null : ((ExpressionImpl) expression).getCounters());
    expressionImpl.setGroupByContext(expression == null ? null : ((ExpressionImpl) expression).groupByContext);
    expressionImpl.setIsProbe(false);
    expressionImpl.setOrderByExpressions(null);
    expressionImpl.setRestrictToThisServer(restrictToThisServer);
    expressionImpl.setProcedureContext(procedureContext);

    IndexLookup indexLookup = new IndexLookup();
    indexLookup.setCount(1);
    indexLookup.setIndexName(primaryKeyIndex.getName());
    indexLookup.setLeftOp(BinaryExpression.Operator.EQUAL);
    indexLookup.setLeftKey(key);
    indexLookup.setLeftOriginalKey(key);
    indexLookup.setColumnName(primaryKeyIndex.getFields()[0]);
    indexLookup.setSchemaRetryCount(schemaRetryCount);
    indexLookup.setUsedIndex(usedIndex);
    indexLookup.setEvaluateExpression(false);

    indexLookup.lookup(expressionImpl, expression);

    CachedRecord ret = recordCache.get(tableName, key);
    if (ret != null) {
      return ret.getRecord();
    }
    return null;
  }

  public static class NextReturn {
    private Object[][][] ids;
    private String[] tableNames;
    private Map<String, String[]> fields = new ConcurrentHashMap<>();

    public Object[][][] getIds() {
      return ids;
    }

    public void setFields(Map<String, String[]> fields) {
      this.fields = fields;
    }

    public NextReturn(String[] tableNames, Object[][][] ids) {
      this.tableNames = tableNames;
      this.ids = ids;
    }

    public NextReturn() {
    }

    public Object[][][] getKeys() {
      return ids;
    }

    public void setIds(Object[][][] ids) {
      this.ids = ids;
    }

    void setTableNames(String[] tableNames) {
      this.tableNames = tableNames;
    }

    void setFields(String tableName, String[] fields) {
      this.fields.put(tableName, fields);
    }

    String[] getTableNames() {
      return tableNames;
    }

    public Map<String, String[]> getFields() {
      return fields;
    }
  }

  public static class IdEntry {
    private int offset;
    private Object[] value;

    public IdEntry(int offset, Object[] value) {
      this.offset = offset;
      this.value = value;
    }

    public int getOffset() {
      return offset;
    }

    public Object[] getValue() {
      return value;
    }
  }

  private static class BatchLookupReturn {
    private Map<Integer, Object[][]> keys;
    private Map<Integer, Record[]> records;
  }

  private static BatchLookupReturn batchLookupIds(
      final String dbName, final DatabaseCommon common, final DatabaseClient client, final boolean forceSelectOnServer,
      final int count, final TableSchema tableSchema,
      final BinaryExpression.Operator operator,
      final Map.Entry<String, IndexSchema> indexSchema, final List<ColumnImpl> columns, final List<IdEntry> srcValues,
      final int shard,
      AtomicReference<String> usedIndex, final RecordCache recordCache, final int viewVersion, final boolean restrictToThisServer, 
      final StoredProcedureContextImpl procedureContext, final int schemaRetryCount) {

    Timer.Context ctx = DatabaseClient.BATCH_INDEX_LOOKUP_STATS.time();
    try {
      usedIndex.set(indexSchema.getKey());

      final int threadCount = 1;

      Future[] futures = new Future[threadCount];
      for (int i = 0; i < threadCount; i++) {
        futures[i] = client.getExecutor().submit(() -> {
          try {
            ComObject cobj = new ComObject();
            cobj.put(ComObject.Tag.SERIALIZATION_VERSION, DatabaseClient.SERIALIZATION_VERSION);
            cobj.put(ComObject.Tag.TABLE_NAME, tableSchema.getName());
            cobj.put(ComObject.Tag.INDEX_NAME, indexSchema.getKey());
            cobj.put(ComObject.Tag.LEFT_OPERATOR, operator.getId());

            ComArray columnArray = cobj.putArray(ComObject.Tag.COLUMN_OFFSETS, ComObject.Type.INT_TYPE);
            writeColumns(tableSchema, columns, columnArray);

            int subCount = srcValues.size();

            boolean writingLongs = markSingleValueOrNot(srcValues, cobj);

            ComArray keys = cobj.putArray(ComObject.Tag.KEYS, ComObject.Type.OBJECT_TYPE);

            putKeysInComObject(tableSchema, indexSchema, srcValues, subCount, writingLongs, keys);

            cobj.put(ComObject.Tag.DB_NAME, dbName);
            cobj.put(ComObject.Tag.SCHEMA_VERSION, common.getSchemaVersion());
            cobj.put(ComObject.Tag.COUNT, count);
            byte[] lookupRet = client.send("ReadManager:batchIndexLookup", shard, -1, cobj,
                DatabaseClient.Replica.DEF);

            return batchLookupIdsProcessResults(dbName, common, client, forceSelectOnServer, tableSchema, columns,
                recordCache, viewVersion, restrictToThisServer, procedureContext, schemaRetryCount, lookupRet);
          }
          catch (IOException e) {
            throw new DatabaseException(e);
          }
        });
      }

      Map<Integer, Object[][]> ret = new HashMap<>();
      Map<Integer, Record[]> retRecords = new HashMap<>();
      for (int i = 0; i < futures.length; i++) {
        BatchLookupReturn currRet = (BatchLookupReturn) futures[i].get();
        for (Map.Entry<Integer, Object[][]> entry : currRet.keys.entrySet()) {
          aggregateKeys(ret, entry.getKey(), entry.getValue());
        }
        for (Map.Entry<Integer, Record[]> entry : currRet.records.entrySet()) {
          aggregateRecords(retRecords, entry.getKey(), entry.getValue());
        }
      }
      BatchLookupReturn batchRet = new BatchLookupReturn();
      batchRet.keys = ret;
      batchRet.records = retRecords;
      return batchRet;
    }
    catch (ExecutionException | InterruptedException e) {
      throw new DatabaseException(e);
    }
    finally {
      ctx.stop();
    }
  }

  private static void putKeysInComObject(TableSchema tableSchema, Map.Entry<String, IndexSchema> indexSchema,
                                         List<IdEntry> srcValues, int subCount, boolean writingLongs, ComArray keys) {
    for (int k = 0; k < subCount; k++) {
      ComObject key = new ComObject();
      keys.add(key);

      IdEntry entry = srcValues.get(k);
      key.put(ComObject.Tag.OFFSET, entry.getOffset());
      if (writingLongs) {
        key.put(ComObject.Tag.LONG_KEY, (long) entry.getValue()[0]);
      }
      else {
        key.put(ComObject.Tag.KEY_BYTES, DatabaseCommon.serializeKey(tableSchema, indexSchema.getKey(), entry.getValue()));
      }
    }
  }

  private static boolean markSingleValueOrNot(List<IdEntry> srcValues, ComObject cobj) {
    boolean writingLongs = false;
    if (!srcValues.isEmpty() && srcValues.get(0).getValue().length == 1) {
      Object value = srcValues.get(0).getValue()[0];
      if (value instanceof Long) {
        cobj.put(ComObject.Tag.SINGLE_VALUE, true);
        writingLongs = true;
      }
      else {
        cobj.put(ComObject.Tag.SINGLE_VALUE, false);
        writingLongs = false;
      }
    }
    else {
      cobj.put(ComObject.Tag.SINGLE_VALUE, false);
    }
    return writingLongs;
  }

  private static BatchLookupReturn batchLookupIdsProcessResults(String dbName, DatabaseCommon common,
                                                                DatabaseClient client, boolean forceSelectOnServer,
                                                                TableSchema tableSchema, List<ColumnImpl> columns,
                                                                RecordCache recordCache, int viewVersion,
                                                                boolean restrictToThisServer,
                                                                StoredProcedureContextImpl procedureContext,
                                                                int schemaRetryCount, byte[] lookupRet) throws EOFException {
    ComObject retObj = new ComObject(lookupRet);
    Map<Integer, Object[][]> retKeys = new HashMap<>();
    Map<Integer, Record[]> retRecords = new HashMap<>();
    ComArray retKeysArray = retObj.getArray(ComObject.Tag.RET_KEYS);
    for (Object entryObj : retKeysArray.getArray()) {
      ComObject retEntryObj = (ComObject)entryObj;
      int offset1 = retEntryObj.getInt(ComObject.Tag.OFFSET);
      ComArray keysArray = retEntryObj.getArray(ComObject.Tag.KEYS);
      Object[][] ids = null;
      if (!keysArray.getArray().isEmpty()) {
        ids = new Object[keysArray.getArray().size()][];
        for (int j = 0; j < ids.length; j++) {
          byte[] keyBytes = (byte[])keysArray.getArray().get(j);
          ids[j] = DatabaseCommon.deserializeKey(tableSchema, keyBytes);
        }
        aggregateKeys(retKeys, offset1, ids);
      }

      aggregateRecordsFromBatchLookup(dbName, common, client, forceSelectOnServer, tableSchema, columns, recordCache,
          viewVersion, restrictToThisServer, procedureContext, schemaRetryCount, retRecords, retEntryObj, offset1, ids);
    }
    BatchLookupReturn batchReturn = new BatchLookupReturn();
    batchReturn.keys = retKeys;
    batchReturn.records = retRecords;
    return batchReturn;
  }

  private static void aggregateRecordsFromBatchLookup(String dbName, DatabaseCommon common, DatabaseClient client,
                                                      boolean forceSelectOnServer, TableSchema tableSchema,
                                                      List<ColumnImpl> columns, RecordCache recordCache, int viewVersion,
                                                      boolean restrictToThisServer, StoredProcedureContextImpl procedureContext,
                                                      int schemaRetryCount, Map<Integer, Record[]> retRecords,
                                                      ComObject retEntryObj, int offset1, Object[][] ids) {
    ComArray recordsArray = retEntryObj.getArray(ComObject.Tag.RECORDS);
    if (!recordsArray.getArray().isEmpty()) {
      Record[] records = new Record[recordsArray.getArray().size()];

      for (int l = 0; l < records.length; l++) {
        byte[] recordBytes = (byte[])recordsArray.getArray().get(l);
        records[l] = new Record(dbName, common, recordBytes);
      }

      aggregateRecords(retRecords, offset1, records);
    }
    else {
      if (ids != null) {
        Record[] records = new Record[ids.length];
        for (int j = 0; j < ids.length; j++) {
          Object[] key = ids[j];
          Record record = doReadRecord(dbName, client, forceSelectOnServer, recordCache, key,
              tableSchema.getName(), columns, null, null, viewVersion,
              restrictToThisServer, procedureContext, schemaRetryCount);
          records[j] = record;
        }
        aggregateRecords(retRecords, offset1, records);
      }
    }
  }

  private static void aggregateKeys(Map<Integer, Object[][]> retKeys, int offset, Object[][] ids) {
    Object[][] currRecords = retKeys.get(offset);
    if (currRecords == null) {
      retKeys.put(offset, ids);
    }
    else {
      currRecords = aggregateResults(currRecords, ids);
      retKeys.put(offset, currRecords);
    }
  }

  public static void aggregateRecords(Map<Integer, Record[]> retRecords, int offset, Record[] records) {
    Record[] currRecords = retRecords.get(offset);
    if (currRecords == null) {
      retRecords.put(offset, records);
    }
    else {
      currRecords = aggregateResults(currRecords, records);
      retRecords.put(offset, currRecords);
    }
  }

  public static void aggregateRecords(Map<Integer, byte[][]> retRecords, int offset, byte[][] records) {
    byte[][] currRecords = retRecords.get(offset);
    if (currRecords == null) {
      retRecords.put(offset, records);
    }
    else {
      currRecords = aggregateResults(currRecords, records);
      retRecords.put(offset, currRecords);
    }
  }

  public static void aggregateRecords(Map<Integer, byte[][]> retRecords, int offset, byte[] record) {
    byte[][] currRecords = retRecords.get(offset);
    if (currRecords == null) {
      retRecords.put(offset, new byte[][]{record});
    }
    else {
      currRecords = aggregateResults(currRecords, new byte[][]{record});
      retRecords.put(offset, currRecords);
    }
  }

  public static void writeColumns(
      TableSchema tableSchema, List<ColumnImpl> columns, ComArray array) {
    if (columns != null) {
      for (ColumnImpl column : columns) {
        if (column.getTableName() == null || column.getTableName().equals(tableSchema.getName())) {
          Integer offset = tableSchema.getFieldOffset(column.getColumnName());
          if (offset == null) {
            continue;
          }
          array.add(offset);
        }
      }
    }
  }

  static SelectContextImpl tableScan(
      String dbName, long viewVersion, DatabaseClient client, int count, TableSchema tableSchema,
      List<OrderByExpressionImpl> orderByExpressions,
      ExpressionImpl expression, ParameterHandler parms, List<ColumnImpl> columns, int shard, Object[] nextKey,
      RecordCache recordCache, Counter[] counters, GroupByContext groupByContext, AtomicLong currOffset,
      Limit limit, Offset offset, boolean isProbe, boolean restrictToThisServer, StoredProcedureContextImpl procedureContext) {
    while (true) {
      try {
        int localShard = shard;
        Object[] localNextKey = nextKey;
        DatabaseCommon common = client.getCommon();

        List<Integer> selectedShards = addAllShardsToSelectedShards(client);

        ExpressionImpl rightKeyExpression = getRightKeyExpression((ExpressionImpl) expression.getTopLevelExpression());

        IndexSchema indexSchema = null;
        for (Map.Entry<String, IndexSchema> entry : common.getTables(dbName).get(tableSchema.getName()).getIndices().entrySet()) {
          if (entry.getValue().isPrimaryKey()) {
            indexSchema = entry.getValue();
            break;
          }
        }

        int nextShard = localShard;

        if (nextShard == -2) {
          return new SelectContextImpl();
        }

        if (localShard == -1) {
          localShard = nextShard = selectedShards.get(0);
        }


        Object[][][] retKeys = null;
        byte[][] recordRet = null;

        return indexLookupForTableScan(dbName, viewVersion, client, count, tableSchema, orderByExpressions, expression,
            parms, columns, nextKey, recordCache, counters, groupByContext, currOffset, limit, offset, isProbe,
            restrictToThisServer, procedureContext, localShard, localNextKey, common, selectedShards, rightKeyExpression,
            indexSchema, nextShard, retKeys, recordRet);
      }
      catch (SchemaOutOfSyncException e) {
        // try again
      }
      catch (IOException e) {
        throw new DatabaseException(e);
      }
    }
  }

  private static List<Integer> addAllShardsToSelectedShards(DatabaseClient client) {
    List<Integer> selectedShards = new ArrayList<>();
    for (int i = 0; i < client.getShardCount(); i++) {
      selectedShards.add(i);
    }
    return selectedShards;
  }

  private static SelectContextImpl indexLookupForTableScan(String dbName, long viewVersion, DatabaseClient client,
                                                           int count, TableSchema tableSchema,
                                                           List<OrderByExpressionImpl> orderByExpressions,
                                                           ExpressionImpl expression, ParameterHandler parms,
                                                           List<ColumnImpl> columns, Object[] nextKey, RecordCache recordCache,
                                                           Counter[] counters, GroupByContext groupByContext, AtomicLong currOffset,
                                                           Limit limit, Offset offset, boolean isProbe,
                                                           boolean restrictToThisServer, StoredProcedureContextImpl procedureContext,
                                                           int localShard, Object[] localNextKey, DatabaseCommon common,
                                                           List<Integer> selectedShards, ExpressionImpl rightKeyExpression,
                                                           IndexSchema indexSchema, int nextShard, Object[][][] retKeys,
                                                           byte[][] recordRet) throws IOException {
    while (nextShard != -2 && (retKeys == null || retKeys.length < count)) {
      PrepareTableScanCall prepareTableScanCall = new PrepareTableScanCall(dbName, viewVersion, client, count,
          tableSchema, orderByExpressions, expression, parms, columns, counters, groupByContext, currOffset, limit,
          offset, isProbe, localNextKey, common, rightKeyExpression).invoke();
      columns = prepareTableScanCall.getColumns();
      ComObject cobj = prepareTableScanCall.getCobj();

      ComObject retObj;
      if (restrictToThisServer) {
        retObj = DatabaseServerProxy.indexLookupExpression(client.getDatabaseServer(), cobj, procedureContext);
      }
      else {
        byte[] lookupRet = client.send("ReadManager:indexLookupExpression", localShard, 0, cobj,
            DatabaseClient.Replica.DEF);
        retObj = new ComObject(lookupRet);
      }
      ProcessTableScanResults processTableScanResults = new ProcessTableScanResults(dbName, client, tableSchema, nextKey,
          recordCache, counters, groupByContext, currOffset, limit, offset, restrictToThisServer, localShard, common,
          selectedShards, nextShard, retKeys, recordRet, retObj).invoke();
      nextKey = processTableScanResults.getNextKey();
      localShard = processTableScanResults.getLocalShard();
      localNextKey = processTableScanResults.getLocalNextKey();
      nextShard = processTableScanResults.getNextShard();
      retKeys = processTableScanResults.getRetKeys();
      recordRet = processTableScanResults.getRecordRet();
    }

    return new SelectContextImpl(tableSchema.getName(),
        indexSchema.getName(), null, nextShard, localNextKey,
        retKeys, recordCache, -1, true);
  }

  private static ExpressionImpl getRightKeyExpression(ExpressionImpl expression) {
    if (expression instanceof BinaryExpressionImpl) {
      if (((BinaryExpressionImpl)expression).isRighKey()) {
        return expression;
      }
      ExpressionImpl ret = getRightKeyExpression(((BinaryExpressionImpl)expression).getLeftExpression());
      if (ret != null) {
        return ret;
      }
      ret = getRightKeyExpression(((BinaryExpressionImpl)expression).getRightExpression());
      if (ret != null) {
        return ret;
      }
    }
    return null;
  }

  static Object getValueFromExpression(
      ParameterHandler parms, ExpressionImpl rightExpression) {
    try {
      Object value = null;
      if (rightExpression instanceof ConstantImpl) {
        ConstantImpl cNode1 = (ConstantImpl) rightExpression;
        value = cNode1.getValue();
      }
      else if (rightExpression instanceof ParameterImpl) {
        ParameterImpl pNode = (ParameterImpl) rightExpression;
        String parmName = pNode.getParmName();
        if (parmName != null) {
         value = parms.getValue(parmName);
        }
        else {
          int parmNum = pNode.getParmOffset();
          value = parms.getValue(parmNum + 1);
        }
      }
      if (value instanceof String) {
        return ((String) value).getBytes("utf-8");
      }
      return value;
    }
    catch (UnsupportedEncodingException e) {
      throw new DatabaseException(e);
    }
  }

  static Object[] buildKey(List<Object> values, String[] indexFields) {
    Object[] key = new Object[indexFields.length];
    for (int i = 0; i < values.size(); i++) {
      key[i] = values.get(i);
    }
    return key;
  }

  public static Object[][][] aggregateResults(Object[][][] records1, Object[][][] records2) {
    if (records1 == null || records1.length == 0) {
      if (records2 == null || records2.length == 0) {
        return null;
      }
      return records2;
    }
    if (records2 == null || records2.length == 0) {
      return records1;
    }

    Object[][][] retArray = new Object[records1.length + records2.length][][];
    System.arraycopy(records1, 0, retArray, 0, records1.length);
    System.arraycopy(records2, 0, retArray, records1.length, records2.length);

    return retArray;
  }

  static KeyRecord[][] aggregateResults(KeyRecord[][] records1, KeyRecord[][] records2) {
    if (records1 == null || records1.length == 0) {
      if (records2 == null || records2.length == 0) {
        return null;
      }
      return records2;
    }
    if (records2 == null || records2.length == 0) {
      return records1;
    }

    KeyRecord[][] retArray = new KeyRecord[records1.length + records2.length][];
    System.arraycopy(records1, 0, retArray, 0, records1.length);
    System.arraycopy(records2, 0, retArray, records1.length, records2.length);

    return retArray;
  }
  static Object[][] aggregateResults(Object[][] records1, Object[][] records2) {

    if (records1 == null || records1.length == 0) {
      if (records2 == null || records2.length == 0) {
        return null;
      }
      return records2;
    }
    if (records2 == null || records2.length == 0) {
      return records1;
    }

    Object[][] retArray = new Object[records1.length + records2.length][];
    System.arraycopy(records1, 0, retArray, 0, records1.length);
    System.arraycopy(records2, 0, retArray, records1.length, records2.length);

    return retArray;
  }

  static byte[][] aggregateResults(byte[][] records1, byte[][] records2) {
    if (records1 == null || records1.length == 0) {
      if (records2 == null || records2.length == 0) {
        return null;
      }
      return records2;
    }
    if (records2 == null || records2.length == 0) {
      return records1;
    }

    byte[][] retArray = new byte[records1.length + records2.length][];
    System.arraycopy(records1, 0, retArray, 0, records1.length);
    System.arraycopy(records2, 0, retArray, records1.length, records2.length);
    return retArray;
  }

  static Record[] aggregateResults(Record[] records1, Record[] records2) {
    if (records1 == null || records1.length == 0) {
      if (records2 == null || records2.length == 0) {
        return null;
      }
      return records2;
    }
    if (records2 == null || records2.length == 0) {
      return records1;
    }

    Record[] retArray = new Record[records1.length + records2.length];
    System.arraycopy(records1, 0, retArray, 0, records1.length);
    System.arraycopy(records2, 0, retArray, records1.length, records2.length);
    return retArray;
  }

  private static class ProcessTableScanResults {
    private String dbName;
    private DatabaseClient client;
    private TableSchema tableSchema;
    private Object[] nextKey;
    private RecordCache recordCache;
    private Counter[] counters;
    private GroupByContext groupByContext;
    private AtomicLong currOffset;
    private Limit limit;
    private Offset offset;
    private boolean restrictToThisServer;
    private int localShard;
    private DatabaseCommon common;
    private List<Integer> selectedShards;
    private int nextShard;
    private Object[][][] retKeys;
    private byte[][] recordRet;
    private ComObject retObj;
    private Object[] localNextKey;

    public ProcessTableScanResults(String dbName, DatabaseClient client, TableSchema tableSchema, Object[] nextKey,
                                   RecordCache recordCache, Counter[] counters, GroupByContext groupByContext,
                                   AtomicLong currOffset, Limit limit, Offset offset, boolean restrictToThisServer,
                                   int localShard, DatabaseCommon common, List<Integer> selectedShards, int nextShard,
                                   Object[][][] retKeys, byte[][] recordRet, ComObject retObj) {
      this.dbName = dbName;
      this.client = client;
      this.tableSchema = tableSchema;
      this.nextKey = nextKey;
      this.recordCache = recordCache;
      this.counters = counters;
      this.groupByContext = groupByContext;
      this.currOffset = currOffset;
      this.limit = limit;
      this.offset = offset;
      this.restrictToThisServer = restrictToThisServer;
      this.localShard = localShard;
      this.common = common;
      this.selectedShards = selectedShards;
      this.nextShard = nextShard;
      this.retKeys = retKeys;
      this.recordRet = recordRet;
      this.retObj = retObj;
    }

    public Object[] getNextKey() {
      return nextKey;
    }

    public int getLocalShard() {
      return localShard;
    }

    public Object[] getLocalNextKey() {
      return localNextKey;
    }

    public int getNextShard() {
      return nextShard;
    }

    public Object[][][] getRetKeys() {
      return retKeys;
    }

    public byte[][] getRecordRet() {
      return recordRet;
    }

    public ProcessTableScanResults invoke() throws IOException {
      Long retOffset = retObj.getLong(ComObject.Tag.CURR_OFFSET);
      if (retOffset != null) {
        currOffset.set(retOffset);
      }

      localNextKey = null;
      byte[] keyBytes = retObj.getByteArray(ComObject.Tag.KEY_BYTES);
      if (keyBytes != null) {
        Object[] retKey = DatabaseCommon.deserializeKey(tableSchema, keyBytes);
        localNextKey = retKey;
      }

      getNextShardForTableScan();

      Object[][][] currRetKeys = getNextKeyForTableScan();

      ComArray recordArray = retObj.getArray(ComObject.Tag.RECORDS);
      int recordCount = recordArray == null ? 0 : recordArray.getArray().size();
      byte[][] currRetRecords = new byte[recordCount][];
      for (int k = 0; k < recordCount; k++) {
        byte[] recordBytes = (byte[]) recordArray.getArray().get(k);
        currRetRecords[k] = recordBytes;
      }

      processCountersForTableScanResults();

      byte[] groupBytes = retObj.getByteArray(ComObject.Tag.LEGACY_GROUP_CONTEXT);
      if (groupBytes != null) {
        groupByContext.deserialize(groupBytes, client.getCommon());
      }

      recordRet = aggregateResults(recordRet, currRetRecords);


      retKeys = aggregateResults(retKeys, currRetKeys);

      fillRecordCacheWithTableScanResults();

      if (limit != null) {
        long tmpOffset = 1;
        if (offset != null) {
          tmpOffset = offset.getOffset();
        }
        if (currOffset.get() >= tmpOffset + limit.getRowCount() - 1) {
          nextShard = -2;
          nextKey = null;
        }
      }
      return this;
    }

    private void getNextShardForTableScan() {
      if (localNextKey == null) {
        if (restrictToThisServer) {
          localShard = nextShard = -2;
        }
        else {
          if (nextShard == selectedShards.size() - 1) {
            localShard = nextShard = -2;
          }
          else {
            localShard = nextShard = selectedShards.get(nextShard + 1);
          }
        }
      }
    }

    private Object[][][] getNextKeyForTableScan() throws EOFException {
      byte[] keyBytes;
      Object[] tmpKey = null;
      ComArray keyArray = retObj.getArray(ComObject.Tag.KEYS);
      Object[][][] currRetKeys = null;
      if (keyArray != null) {
        currRetKeys = new Object[keyArray.getArray().size()][][];
        for (int k = 0; k < currRetKeys.length; k++) {
          keyBytes = (byte[]) keyArray.getArray().get(k);
          Object[] key = DatabaseCommon.deserializeKey(common.getTables(dbName).get(tableSchema.getName()), keyBytes);
          currRetKeys[k] = new Object[][]{key};
          tmpKey = key;
        }
      }

      if (localNextKey == null) {
        localNextKey = tmpKey;
      }
      return currRetKeys;
    }

    private void fillRecordCacheWithTableScanResults() {
      if (recordRet != null) {
        String[] primaryKeyFields = null;
        for (Map.Entry<String, IndexSchema> entry : tableSchema.getIndices().entrySet()) {
          if (entry.getValue().isPrimaryKey()) {
            primaryKeyFields = entry.getValue().getFields();
            break;
          }
        }
        retKeys = new Object[recordRet.length][][];
        for (int i = 0; i < recordRet.length; i++) {
          byte[] curr = recordRet[i];

          Record record = new Record(dbName, client.getCommon(), curr);
          Object[] key = new Object[primaryKeyFields.length];
          for (int j = 0; j < primaryKeyFields.length; j++) {
            key[j] = record.getFields()[tableSchema.getFieldOffset(primaryKeyFields[j])];
          }

          if (retKeys[i] == null) {
            retKeys[i] = new Object[][]{key};
          }

          recordCache.put(tableSchema.getName(), key, new CachedRecord(record, curr));
        }

      }
    }

    private void processCountersForTableScanResults() throws IOException {
      Counter[] retCounters = null;
      ComArray countersArray = retObj.getArray(ComObject.Tag.COUNTERS);
      if (countersArray != null) {
        retCounters = new Counter[countersArray.getArray().size()];
        for (int i = 0; i < countersArray.getArray().size(); i++) {
          retCounters[i] = new Counter();
          byte[] counterBytes = (byte[]) countersArray.getArray().get(i);
          retCounters[i].deserialize(counterBytes);
        }
        System.arraycopy(retCounters, 0, counters, 0, Math.min(counters.length, retCounters.length));
      }
    }
  }

  private static class PrepareTableScanCall {
    private String dbName;
    private long viewVersion;
    private DatabaseClient client;
    private int count;
    private TableSchema tableSchema;
    private List<OrderByExpressionImpl> orderByExpressions;
    private ExpressionImpl expression;
    private ParameterHandler parms;
    private List<ColumnImpl> columns;
    private Counter[] counters;
    private GroupByContext groupByContext;
    private AtomicLong currOffset;
    private Limit limit;
    private Offset offset;
    private boolean isProbe;
    private Object[] localNextKey;
    private DatabaseCommon common;
    private ExpressionImpl rightKeyExpression;
    private ComObject cobj;

    public PrepareTableScanCall(String dbName, long viewVersion, DatabaseClient client, int count,
                                TableSchema tableSchema, List<OrderByExpressionImpl> orderByExpressions,
                                ExpressionImpl expression, ParameterHandler parms, List<ColumnImpl> columns,
                                Counter[] counters, GroupByContext groupByContext, AtomicLong currOffset,
                                Limit limit, Offset offset, boolean isProbe, Object[] localNextKey, DatabaseCommon common,
                                ExpressionImpl rightKeyExpression) {
      this.dbName = dbName;
      this.viewVersion = viewVersion;
      this.client = client;
      this.count = count;
      this.tableSchema = tableSchema;
      this.orderByExpressions = orderByExpressions;
      this.expression = expression;
      this.parms = parms;
      this.columns = columns;
      this.counters = counters;
      this.groupByContext = groupByContext;
      this.currOffset = currOffset;
      this.limit = limit;
      this.offset = offset;
      this.isProbe = isProbe;
      this.localNextKey = localNextKey;
      this.common = common;
      this.rightKeyExpression = rightKeyExpression;
    }

    public List<ColumnImpl> getColumns() {
      return columns;
    }

    public ComObject getCobj() {
      return cobj;
    }

    public PrepareTableScanCall invoke() throws IOException {
      cobj = new ComObject();
      cobj.put(ComObject.Tag.TABLE_ID, tableSchema.getTableId());
      if (parms != null) {
        cobj.put(ComObject.Tag.PARMS, parms.serialize());
      }
      if (expression != null) {
        cobj.put(ComObject.Tag.LEGACY_EXPRESSION, ExpressionImpl.serializeExpression(
            (ExpressionImpl) expression.getTopLevelExpression()));
      }
      if (orderByExpressions != null) {
        ComArray array = cobj.putArray(ComObject.Tag.ORDER_BY_EXPRESSIONS, ComObject.Type.BYTE_ARRAY_TYPE);
        for (int j = 0; j < orderByExpressions.size(); j++) {
          OrderByExpressionImpl orderByExpression = orderByExpressions.get(j);
          array.add(orderByExpression.serialize());
        }
      }

      if (localNextKey != null) {
        cobj.put(ComObject.Tag.LEFT_KEY, DatabaseCommon.serializeTypedKey(localNextKey));
      }
      if (rightKeyExpression != null) {
        Object value = null;
        BinaryExpressionImpl binary = (BinaryExpressionImpl) rightKeyExpression;
        if (binary.getLeftExpression() instanceof ConstantImpl ||
            binary.getLeftExpression() instanceof ParameterImpl) {
          value = getValueFromExpression(binary.getParms(), binary.getLeftExpression());
        }
        else {
          value = getValueFromExpression(binary.getParms(), binary.getRightExpression());
        }
        cobj.put(ComObject.Tag.RIGHT_KEY, DatabaseCommon.serializeTypedKey(new Object[]{value}));

        cobj.put(ComObject.Tag.RIGHT_OPERATOR, ((BinaryExpressionImpl) rightKeyExpression).getOperator().getId());
      }

      cobj.put(ComObject.Tag.CURR_OFFSET, currOffset.get());
      if (limit != null) {
        cobj.put(ComObject.Tag.LIMIT_LONG, limit.getRowCount());
      }
      if (offset != null) {
        cobj.put(ComObject.Tag.OFFSET_LONG, offset.getOffset());
      }

      prepareColumnsForTableScan();

      ComArray columnArray = cobj.putArray(ComObject.Tag.COLUMN_OFFSETS, ComObject.Type.INT_TYPE);
      writeColumns(tableSchema, columns, columnArray);

      prepareCountersForTableScan();

      if (groupByContext != null) {
        cobj.put(ComObject.Tag.LEGACY_GROUP_CONTEXT, groupByContext.serialize(client.getCommon()));
      }
      cobj.put(ComObject.Tag.IS_PROBE, isProbe);
      cobj.put(ComObject.Tag.VIEW_VERSION, viewVersion);
      cobj.put(ComObject.Tag.COUNT, count);
      cobj.put(ComObject.Tag.DB_NAME, dbName);
      cobj.put(ComObject.Tag.SCHEMA_VERSION, common.getSchemaVersion());
      cobj.put(ComObject.Tag.METHOD, "ReadManager:indexLookupExpression");
      return this;
    }

    private void prepareCountersForTableScan() throws IOException {
      if (counters != null) {
        ComArray array = cobj.putArray(ComObject.Tag.COUNTERS, ComObject.Type.BYTE_ARRAY_TYPE);
        for (int i = 0; i < counters.length; i++) {
          array.add(counters[i].serialize());
        }
      }
    }

    private void prepareColumnsForTableScan() {
      if (columns == null) {
        columns = new ArrayList<>();
      }
      for (IndexSchema schema : tableSchema.getIndices().values()) {
        if (schema.isPrimaryKey()) {
          for (String field : schema.getFields()) {
            prepareColumForField(field);
          }
        }
      }
    }

    private void prepareColumForField(String field) {
      boolean found = false;
      for (ColumnImpl column : columns) {
        if (column.getColumnName().equalsIgnoreCase(field)) {
          found = true;
        }
      }
      if (!found) {
        columns.add(new ColumnImpl(null, null, tableSchema.getName(), field, null));
      }
    }
  }
}
