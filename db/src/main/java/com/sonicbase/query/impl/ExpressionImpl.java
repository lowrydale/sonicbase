package com.sonicbase.query.impl;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.*;
import com.sonicbase.index.Repartitioner;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.query.BinaryExpression;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.query.Expression;
import com.sonicbase.schema.DataType;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.server.PreparedIndexLookupNotFoundException;
import com.sonicbase.server.SnapshotManager;
import com.sonicbase.util.DataUtil;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.Offset;
import org.apache.commons.lang.exception.ExceptionUtils;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.singletonList;

public abstract class ExpressionImpl implements Expression {

  private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("com.sonicbase.logger");

  private static Map<Integer, Type> typesById = new HashMap<Integer, Type>();
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
  private long viewVersion;
  private int dbViewNum;
  private Counter[] counters;
  private Limit limit;
  private GroupByContext groupByContext;
  protected String dbName;
  private boolean forceSelectOnServer;
  protected long serializationVersion;
  private int lastShard;
  private boolean isCurrPartitions;

  public Counter[] getCounters() {
    return counters;
  }


  public GroupByContext getGroupByContext() {
    return groupByContext;
  }

  public long getViewVersion() {
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

  public void setViewVersion(long viewVersion) {
    this.viewVersion = viewVersion;
  }

  public void setCounters(Counter[] counters) {
    this.counters = counters;
  }

  public void setLimit(Limit limit) {
    this.limit = limit;
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

  public static void evaluateCounter(DatabaseCommon common, DatabaseClient client, String dbName, Counter counter) throws IOException {
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.legacyCounter, counter.serialize());
    cobj.put(ComObject.Tag.dbName, dbName);
    cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
    cobj.put(ComObject.Tag.method, "evaluateCounterGetKeys");
    String command = "DatabaseServer:ComObject:evaluateCounterGetKeys:";

    String batchKey = "DatabaseServer:evaluateCounterGetKeys";

    Counter lastCounter = null;
    int shardCount = common.getServersConfig().getShardCount();

    for (int i = 0; i < shardCount; i++) {
      byte[] ret = client.send(batchKey, i, 0, command, cobj, DatabaseClient.Replica.def);
      ComObject retObj = new ComObject(ret);

      byte[] minKeyBytes = retObj.getByteArray(ComObject.Tag.minKey);
      byte[] maxKeyBytes = retObj.getByteArray(ComObject.Tag.maxKey);

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
//        Long count = retCounter.getCount();
//        Long lastCount = lastCounter.getCount();
//        if (count != null) {
//          if (lastCount != null) {
//            retCounter.setCount(count + lastCount);
//          }
//        }
//        else {
//          retCounter.setCount(lastCount);
//        }
        minCounter.setMaxLong(maxCounter.getMaxLong());
        minCounter.setMaxDouble(maxCounter.getMaxDouble());
      }
      lastCounter = minCounter;
    }
    counter.setMaxLong(lastCounter.getMaxLong());
    counter.setMinLong(lastCounter.getMinLong());
    counter.setMaxDouble(lastCounter.getMaxDouble());
    counter.setMinDouble(lastCounter.getMinDouble());
    counter.setCount(lastCounter.getCount());
  }

  private static Counter getCounterValue(DatabaseCommon common, DatabaseClient client, String dbName, Counter counter, byte[] keyBytes,
                                         boolean isMin) throws IOException {
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.legacyCounter, counter.serialize());
    if (isMin) {
      cobj.put(ComObject.Tag.minKey, keyBytes);
    }
    else {
      cobj.put(ComObject.Tag.maxKey, keyBytes);
    }
    cobj.put(ComObject.Tag.dbName, dbName);
    cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
    cobj.put(ComObject.Tag.method, "evaluateCounterWithRecord");
    String command = "DatabaseServer:ComObject:evaluateCounterWithRecord:";

    String batchKey = "DatabaseServer:evaluateCounterWithRecord";

    TableSchema tableSchema = common.getTables(dbName).get(counter.getTableName());
    Object[] key = DatabaseCommon.deserializeKey(tableSchema, keyBytes);

    String indexName = null;
    IndexSchema indexSchema = null;
    for (Map.Entry<String, IndexSchema> entry : tableSchema.getIndexes().entrySet()) {
      if (entry.getValue().isPrimaryKey()) {
        indexName = entry.getKey();
        indexSchema = entry.getValue();
      }
    }
    String[] indexFields = indexSchema.getFields();
    int[] fieldOffsets = new int[indexFields.length];
    for (int i = 0; i < indexFields.length; i++) {
      fieldOffsets[i] = tableSchema.getFieldOffset(indexFields[i]);
    }
    List<Integer> selectedShards = Repartitioner.findOrderedPartitionForRecord(true, false, fieldOffsets, client.getCommon(), tableSchema,
        indexName, null, BinaryExpression.Operator.equal, null, key, null);

    byte[] ret = client.send(batchKey, selectedShards.get(0), 0, command, cobj, DatabaseClient.Replica.def);
    ComObject retObj = new ComObject(ret);
    Counter retCounter = new Counter();
    byte[] counterBytes = retObj.getByteArray(ComObject.Tag.legacyCounter);
    retCounter.deserialize(counterBytes);
    return retCounter;
  }

  public boolean isForceSelectOnServer() {
    return forceSelectOnServer;
  }

  public void getColumnsInExpression(List<ColumnImpl> columns) {
  }

  public void setIsCurrPartitions(boolean isCurrPartitions) {
    this.isCurrPartitions = isCurrPartitions;
  }

  public boolean isCurrPartitions() {
    return isCurrPartitions;
  }

  public static enum Type {
    column(0),
    constant(1),
    parameter(2),
    binaryOp(3),
    parenthesis(4),
    inExpression(5),
    allExpression(6);

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

  public abstract void getColumns(Set<ColumnImpl> columns);

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

  public void serialize(DataOutputStream out) {
    try {
      DataUtil.writeVLong(out, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
      out.writeInt(nextShard);
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  abstract public Type getType();

  /**
   * ###############################
   * DON"T MODIFY THIS SERIALIZATION
   * ###############################
   */
  public void deserialize(DataInputStream in) {
    try {
      this.serializationVersion = DataUtil.readVLong(in);
      nextShard = in.readInt();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  abstract public Object evaluateSingleRecord(
      TableSchema[] tableSchemas, Record[] records, ParameterHandler parms);

  abstract public NextReturn next(SelectStatementImpl.Explain explain, AtomicLong currOffset, Limit limit, Offset offset);

  public abstract NextReturn next(int count, SelectStatementImpl.Explain explain, AtomicLong currOffset, Limit limit, Offset offset);

  abstract public boolean canUseIndex();

  public abstract boolean canSortWithIndex();

  public abstract void queryRewrite();

  public abstract ColumnImpl getPrimaryColumn();

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
      out.writeInt(expression.getType().getId());
      expression.serialize(out);
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
      int type = in.readInt();
      switch (ExpressionImpl.typesById.get(type)) {
        case column:
          ColumnImpl column = new ColumnImpl();
          column.deserialize(in);
          return column;
        case constant:
          ConstantImpl constant = new ConstantImpl();
          constant.deserialize(in);
          return constant;
        case parameter:
          ParameterImpl parameter = new ParameterImpl();
          parameter.deserialize(in);
          return parameter;
        case inExpression:
          InExpressionImpl expression = new InExpressionImpl();
          expression.deserialize(in);
          return expression;
        case binaryOp:
          BinaryExpressionImpl binaryOp = new BinaryExpressionImpl();
          binaryOp.deserialize(in);
          return binaryOp;
        case allExpression:
          AllRecordsExpressionImpl allExpression = new AllRecordsExpressionImpl();
          allExpression.deserialize(in);
          return allExpression;
      }
      return null;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  static class RecordToRead {
    private int tableId;
    private long id;

    public RecordToRead(int tableId, long id) {
      this.tableId = tableId;
      this.id = id;
    }
  }

//  private static ThreadPoolExecutor readExecutor = new ThreadPoolExecutor(32, 32, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
//  public static Record[][] doReadRecords(
//      final DatabaseClient client, Object[][][] keys, String[] tableNames, final List<String> columns) throws Exception {
//
//    final int previousSchemaVersion = client.getCommon().getSchemaVersion();
//
//    final List<List<RecordToRead>> partitionedIds = new ArrayList<>();
//    for (int i = 0; i < client.getShardCount(); i++) {
//      partitionedIds.add(new ArrayList<RecordToRead>());
//    }
//    int[] tableIds = new int[tableNames.length];
//    for (int i = 0; i < tableNames.length; i++) {
//      tableIds[i] = client.getCommon().getTables().get(tableNames[i]).getTableId();
//    }
//
//    RecordIndexPartition[] recordPartitions = client.getCommon().getSchema().getRecordIndexPartitions();
//    for (Object[][] id : keys) {
//      for (int i = 0; i < id.length; i++) {
//        Object[] currId = id[i];
//        if (currId == null) {
//          continue;
//        }
//
//        selectedShards = Repartitioner.findOrderedPartitionForRecord(false, fieldOffsets, client.getCommon(), tableSchema,
//              indexSchema.getKey(), orderByExpressions, leftOperator, rightOperator, comparators, originalLeftValue, originalRightValue);
//
//        RecordIndexPartition recordPartition = recordPartitions[(int) (currId % recordPartitions.length)];
//        partitionedIds.get(recordPartition.getShardOwning()).add(new RecordToRead(tableIds[i], currId));
//      }
//    }
//
//    final ConcurrentHashMap<Long, Record> recordsRead = new ConcurrentHashMap<>();
//
//    List<Future> outerFutures = new ArrayList<>();
//    for (int i = 0; i < partitionedIds.size(); i++) {
//      final int partitionOffset = i;
//      final List<RecordToRead> records = partitionedIds.get(partitionOffset);
//      if (records.size() == 0) {
//        continue;
//      }
//      outerFutures.add(readExecutor.submit(new Callable() {
//        @Override
//        public Object call() throws Exception {
//          int threadCount = 128;
//          final ByteArrayOutputStream[] bytesOut = new ByteArrayOutputStream[threadCount];
//          DataOutputStream[] out = new DataOutputStream[threadCount];
//          List<Future> futures = new ArrayList<>();
//          DataUtil.ResultLength resultLength = new DataUtil.ResultLength();
//          int l = 0;
//          for (int j = 0; j < threadCount; j++) {
//            int count = (j == (threadCount - 1) ? records.size() - l : records.size() / threadCount);
//            if (count == 0) {
//              continue;
//            }
//            bytesOut[j] = new ByteArrayOutputStream();
//            out[j] = new DataOutputStream(bytesOut[j]);
//            final int threadOffset = j;
//           // List<RecordToRead> records = partitionedIds.get(partitionOffset);
//            out[j].writeInt(columns.size());
//            for (String column : columns) {
//              out[j].writeUTF(column);
//            }
//            out[j].writeInt(count);
//            int k = l;
//            for (; k < l + count; k++) {
//              RecordToRead record = records.get(k);
//              DataUtil.writeVLong(out[j], record.tableId, resultLength);
//              DataUtil.writeVLong(out[j], record.id, resultLength);
//            }
//            out[j].close();
//            l = k;
//
//            futures.add(client.getExecutor().submit(new Callable() {
//              @Override
//              public Object call() throws Exception {
//                String command = "DatabaseServer:readRecordsWithColumns:1:" + client.getCommon().getSchemaVersion();
//
//                AtomicReference<String> selectedHost = new AtomicReference<>();
//
//                byte[] recordRet = client.send(partitionOffset, ThreadLocalRandom.current().nextLong(), command, bytesOut[threadOffset].toByteArray(), DatabaseClient.Replica.def, 30000, selectedHost);
//                if (previousSchemaVersion < client.getCommon().getSchemaVersion()) {
//                  throw new SchemaOutOfSyncException();
//                }
//
//                Map<Long, Record> recordsRead = new HashMap<>();
//
//                if (recordRet != null) {
//                  DataInputStream in = new DataInputStream(new ByteArrayInputStream(recordRet));
//                  int count = in.readInt();
//                  for (int j = 0; j < count; j++) {
//                    if (in.readBoolean()) {
//                      int tableId = in.readInt();
//                      Record record = new Record(client.getCommon().getTablesById().get(tableId));
//                      byte[] bytes = new byte[in.readInt()];
//                      in.readFully(bytes);
//                      record.deserialize(client.getCommon(), bytes);
//                      recordsRead.put(record.getId(), record);
//                    }
//                  }
//                }
//                return recordsRead;
//              }
//              }));
//          }
//          for (Future future : futures) {
//            Map<Long, Record> currRead = (Map<Long, Record>) future.get();
//            recordsRead.putAll(currRead);
//          }
//          return null;
//        }
//      }));
//
//    }
//
//    for (Future future : outerFutures) {
//      future.get();
//    }
//
//    Record[][] retRecords = new Record[keys.length][];
//    for (int j = 0; j < keys.length; j++) {
//      retRecords[j] = new Record[tableNames.length];
//      for (int k = 0; k < keys[j].length; k++) {
//        if (keys[j][k] == -1) {
//          continue;
//        }
//        retRecords[j][k] = recordsRead.get(keys[j][k]);
//      }
//    }
//    return retRecords;
//  }
//


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
    private Map<String, Object2ObjectOpenHashMap<Key, CachedRecord>> recordsForTable = new Object2ObjectOpenHashMap<>();

    public Map<String, Object2ObjectOpenHashMap<Key, CachedRecord>> getRecordsForTable() {
      return recordsForTable;
    }

    public void clear() {
      for (Object2ObjectOpenHashMap<Key, CachedRecord> records : recordsForTable.values()) {
        records.clear();
      }
    }

    class Key {
      private int hashCode = 0;
      private Object[] key;

      @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="EI_EXPOSE_REP2", justification="copying the passed in data is too slow")
      @SuppressWarnings("PMD.ArrayIsStoredDirectly") //copying the passed in data is too slow
      public Key(String tableName, Object[] key) {
        this.key = key;
        hashCode = 0;
        for (int i = 0; i < key.length; i++) {
          if (key[i] == null) {
            continue;
          }
          if (key[i] instanceof byte[]) {
            hashCode += Arrays.hashCode((byte[])key[i]);
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
        for (int i = 0; i < key.length; i++) {
          if (key[i] == null || ((Key)o).key[i] == null) {
            continue;
          }
          if (key[i] instanceof Long) {
            if (((Long)key[i]).equals(((Key)o).key[i])) {
              continue;
            }
            return false;
          }
          else if (key[i] instanceof byte[]) {
            if (Arrays.equals((byte[])key[i], (byte[])((Key)o).key[i])) {
              continue;
            }
            return false;
          }
          else {
            if (key[i].equals(((Key)o).key[i])) {
              continue;
            }
            return false;
          }
        }
        return true;
      }
    }


    public RecordCache() {

    }

    public boolean containsKey(String tableName, Object[] key) {
      Object2ObjectOpenHashMap<Key, CachedRecord> records = recordsForTable.get(tableName);
      if (records == null) {
        return false;
      }
      return records.containsKey(new Key(tableName, key));
    }

    public CachedRecord get(String tableName, Object[] key) {
      Object2ObjectOpenHashMap<Key, CachedRecord> records = recordsForTable.get(tableName);
      if (records == null) {
        return null;
      }
      return records.get(new Key(tableName, key));
    }

    public void put(String tableName, Object[] key, CachedRecord record) {
      Object2ObjectOpenHashMap<Key, CachedRecord> records = null;
      synchronized (this) {
        records = recordsForTable.get(tableName);
        if (records == null) {
          recordsForTable.put(tableName, new Object2ObjectOpenHashMap<Key, CachedRecord>());
          records = recordsForTable.get(tableName);
        }
      }
      records.put(new Key(tableName, key), record);
    }
  }

  public static HashMap<Integer, Object[][]> readRecords(
      String dbName, final DatabaseClient client, int pageSize, boolean forceSelectOnServer, final TableSchema tableSchema,
      List<IdEntry> keysToRead, String[] columns, List<ColumnImpl> selectColumns, RecordCache recordCache, long viewVersion) {

    columns = new String[selectColumns.size()];
    for (int i = 0; i < selectColumns.size(); i++) {
      columns[i] = selectColumns.get(i).getColumnName();
    }

    HashMap<Integer, Object[][]> ret = doReadRecords(dbName, client, pageSize, forceSelectOnServer, tableSchema,
        keysToRead, columns, selectColumns, recordCache, viewVersion);

    return ret;
  }

  public static HashMap<Integer, Object[][]> doReadRecords(
      final String dbName, final DatabaseClient client, final int pageSize, final boolean forceSelectOnServer, final TableSchema tableSchema, List<IdEntry> keysToRead, String[] columns, final List<ColumnImpl> selectColumns,
      final RecordCache recordCache, final long viewVersion) {
    try {
      int[] fieldOffsets = null;
      Comparator[] comparators = null;
      final AtomicReference<Map.Entry<String, IndexSchema>> indexSchema = new AtomicReference<>();
      for (Map.Entry<String, IndexSchema> entry : tableSchema.getIndices().entrySet()) {
        String[] fields = entry.getValue().getFields();
        boolean shouldIndex = false;
        if (fields.length ==  columns.length) {
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
        }
        else {
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
        }
        if (shouldIndex) {
          indexSchema.set(entry);
          String[] indexFields = indexSchema.get().getValue().getFields();
          fieldOffsets = new int[indexFields.length];
          for (int l = 0; l < indexFields.length; l++) {
            fieldOffsets[l] = tableSchema.getFieldOffset(indexFields[l]);
          }
          comparators = indexSchema.get().getValue().getComparators();
          break;
        }
      }

      Map<Integer, List<IdEntry>> partitionedValues = new HashMap<>();
      for (int j = 0; j < client.getShardCount(); j++) {
        partitionedValues.put(j, new ArrayList<IdEntry>());
      }

      boolean synced = false;
      for (int i = 0; i < keysToRead.size(); i++) {
        List<Integer> selectedShards = Repartitioner.findOrderedPartitionForRecord(true, false, fieldOffsets, client.getCommon(), tableSchema,
            indexSchema.get().getKey(), null, BinaryExpression.Operator.equal, null, keysToRead.get(i).getValue(), null);
        if (selectedShards.size() == 0) {
          throw new DatabaseException("No shards selected for query");
        }
//        if (selectedShards.size() > 1) {
//          if (!synced) {
//            client.syncSchema();
//            synced = true;
//          }
//          throw new DatabaseException("Invalid state. Multiple shards");
//        }
        for (int selectedShard : selectedShards) {
          partitionedValues.get(selectedShard).add(new IdEntry(keysToRead.get(i).getOffset(), keysToRead.get(i).getValue()));
        }
      }


      final HashMap<Integer, Object[][]> fullMap = new HashMap<>();
      List<Future> futures = new ArrayList<>();
      for (final Map.Entry<Integer, List<IdEntry>> entry : partitionedValues.entrySet()) {
        futures.add(client.getExecutor().submit(new Callable() {
          @Override
          public Object call() {
            AtomicReference<String> usedIndex = new AtomicReference<>();
            Object rightRet = ExpressionImpl.batchLookupIds(dbName,
                client.getCommon(), client, forceSelectOnServer, pageSize, tableSchema,
                BinaryExpression.Operator.equal, indexSchema.get(), selectColumns, entry.getValue(), entry.getKey(),
                usedIndex, recordCache, viewVersion);
            return rightRet;
          }
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
            //          fullMap.put(entry.getKey(), keys);
          }
        }
        else {
          fullMap.putAll(curr.keys);
        }
      }
      return fullMap;
    }
    catch (InterruptedException | ExecutionException e) {
      throw new DatabaseException(e);
    }
  }

  public static Record doReadRecord(
      String dbName, DatabaseClient client, boolean forceSelectOnServer, RecordCache recordCache, Object[] key,
      String tableName, List<ColumnImpl> columns, Expression expression,
      ParameterHandler parms, long viewVersion, boolean debug) {
//    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
//    DataOutputStream out = new DataOutputStream(bytesOut);
//    out.writeUTF(tableName);
//    out.writeInt(selectColumns.size());
//    for (String column : selectColumns) {
//      out.writeUTF(column);
//    }
//    ExpressionImpl.serializeExpression((ExpressionImpl) expression, out);
//    parms.serialize(out);
//    out.close();

    CachedRecord ret = recordCache.get(tableName, key);
    if (ret != null) {
      return ret.getRecord();
    }

    TableSchema tableSchema = client.getCommon().getTables(dbName).get(tableName);
    IndexSchema primaryKeyIndex = null;
    for (Map.Entry<String, IndexSchema> entry : client.getCommon().getTables(dbName).get(tableName).getIndices().entrySet()) {
      if (entry.getValue().isPrimaryKey()) {
        primaryKeyIndex = entry.getValue();
        break;
      }
    }
    int nextShard = -1;
    int replicaCount = client.getCommon().getServersConfig().getShards()[0].getReplicas().length;
    int replica = ThreadLocalRandom.current().nextInt(0, replicaCount);
    AtomicReference<String> usedIndex = new AtomicReference<>();
    SelectContextImpl context = ExpressionImpl.lookupIds(dbName, client.getCommon(), client, replica, 1, tableSchema.getName(), primaryKeyIndex.getName(), forceSelectOnServer,
        BinaryExpression.Operator.equal, null, null, key, parms, expression, null, key, null,
        columns, primaryKeyIndex.getFields()[0], nextShard, recordCache, usedIndex, true, viewVersion, expression == null ? null : ((ExpressionImpl)expression).getCounters(),
        expression == null ? null : ((ExpressionImpl)expression).groupByContext, debug, new AtomicLong(), null, null);
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

//    String command = "DatabaseServer:selectRecord:1:" + client.getCommon().getSchemaVersion() + ":" + id;
//
//    RecordIndexPartition[] recordPartitions = client.getCommon().getSchema().getRecordIndexPartitions();
//    RecordIndexPartition recordPartition = recordPartitions[(int) (id % recordPartitions.length)];
//
//    AtomicReference<String> selectedHost = new AtomicReference<>();
//
//    Record currRecord = null;
//    byte[] recordRet = client.send(recordPartition.getShardOwning(), ThreadLocalRandom.current().nextLong(), command, bytesOut.toByteArray(), DatabaseClient.Replica.def, 30000, selectedHost);
//    if (recordRet != null) {
//      Record record = new Record(client.getCommon().getTables().get(tableName));
//      record.deserialize(client.getCommon(), recordRet);
//      currRecord = record;
//    }
//    if (currRecord != null) {
//      return currRecord;
//    }
//    return null;
  }

  public static Record doReadRecord(
      String dbName, DatabaseClient client, boolean forceSelectOnServer, ParameterHandler parms, Expression expression, RecordCache recordCache, Object[] key,
      String tableName,
      List<ColumnImpl> columns, long viewVersion, boolean debug) {


    TableSchema tableSchema = client.getCommon().getTables(dbName).get(tableName);
    IndexSchema primaryKeyIndex = null;
    for (Map.Entry<String, IndexSchema> entry : client.getCommon().getTables(dbName).get(tableName).getIndices().entrySet()) {
      if (entry.getValue().isPrimaryKey()) {
        primaryKeyIndex = entry.getValue();
        break;
      }
    }
    int nextShard = -1;
    int replicaCount = client.getCommon().getServersConfig().getShards()[0].getReplicas().length;
    int replica = ThreadLocalRandom.current().nextInt(0, replicaCount);
    AtomicReference<String> usedIndex = new AtomicReference<>();
    ExpressionImpl.lookupIds(dbName, client.getCommon(), client, replica, 1, tableSchema.getName(), primaryKeyIndex.getName(), forceSelectOnServer,
        BinaryExpression.Operator.equal, null, null, key, parms, expression, null, key, null,
        columns, primaryKeyIndex.getFields()[0], nextShard, recordCache, usedIndex, false, viewVersion,
        expression == null ? null : ((ExpressionImpl)expression).getCounters(),
        expression == null ? null : ((ExpressionImpl)expression).groupByContext, debug, new AtomicLong(), null, null);

    CachedRecord ret = recordCache.get(tableName, key);
    if (ret != null) {
      return ret.getRecord();
    }
    return null;

//    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
//    DataOutputStream out = new DataOutputStream(bytesOut);
//    out.writeUTF(tableName);
//    out.writeInt(selectColumns.size());
//    for (String column : selectColumns) {
//      out.writeUTF(column);
//    }
//    out.close();
//
//    String command = "DatabaseServer:readRecordWithColumns:1:" + client.getCommon().getSchemaVersion();
//
//    RecordIndexPartition[] recordPartitions = client.getCommon().getSchema().getRecordIndexPartitions();
//    RecordIndexPartition recordPartition = recordPartitions[(int) (id % recordPartitions.length)];
//
//    AtomicReference<String> selectedHost = new AtomicReference<>();
//
//    Record currRecord = null;
//    byte[] recordRet = client.send(recordPartition.getShardOwning(), ThreadLocalRandom.current().nextLong(), command, bytesOut.toByteArray(), DatabaseClient.Replica.def, 30000, selectedHost);
//    if (recordRet != null) {
//      Record record = new Record(client.getCommon().getTables().get(tableName));
//      record.deserialize(client.getCommon(), recordRet);
//      currRecord = record;
//    }
//    if (currRecord != null) {
//      return currRecord;
//    }
//    return null;
  }

  static class NextReturn {
    private Object[][][] ids;
    private String[] tableNames;
    private ConcurrentHashMap<String, String[]> fields = new ConcurrentHashMap<>();

    public Object[][][] getIds() {
      return ids;
    }

    public void setFields(ConcurrentHashMap<String, String[]> fields) {
      this.fields = fields;
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="EI_EXPOSE_REP2", justification="copying the passed in data is too slow")
    @SuppressWarnings("PMD.ArrayIsStoredDirectly") //copying the passed in data is too slow
    public NextReturn(String[] tableNames, Object[][][] ids) {
      this.tableNames = tableNames;
      this.ids = ids;
    }

    public NextReturn() {
    }

    public Object[][][] getKeys() {
      return ids;
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="EI_EXPOSE_REP2", justification="copying the passed in data is too slow")
    @SuppressWarnings("PMD.ArrayIsStoredDirectly") //copying the passed in data is too slow
    public void setIds(Object[][][] ids) {
      this.ids = ids;
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="EI_EXPOSE_REP2", justification="copying the passed in data is too slow")
    @SuppressWarnings("PMD.ArrayIsStoredDirectly") //copying the passed in data is too slow
    void setTableNames(String[] tableNames) {
      this.tableNames = tableNames;
    }

    void setFields(String tableName, String[] fields) {
      this.fields.put(tableName, fields);
    }

    String[] getTableNames() {
      return tableNames;
    }

    public ConcurrentHashMap<String, String[]> getFields() {
      return fields;
    }
  }

  public static class IdEntry {
    private int offset;
    private Object[] value;

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="EI_EXPOSE_REP2", justification="copying the passed in data is too slow")
    @SuppressWarnings("PMD.ArrayIsStoredDirectly") //copying the passed in data is too slow
    public IdEntry(int offset, Object[] value) {
      this.offset = offset;
      this.value = value;
    }

    public int getOffset() {
      return offset;
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="EI_EXPOSE_REP", justification="copying the returned data is too slow")
    public Object[] getValue() {
      return value;
    }
  }

  private static class BatchLookupReturn {
    private Map<Integer, Object[][]> keys;
    private Map<Integer, Record[]> records;
  }

  private static BatchLookupReturn batchLookupIds(
      final String dbName, final DatabaseCommon common, final DatabaseClient client, final boolean forceSelectOnServer, final int count, final TableSchema tableSchema,
      final BinaryExpression.Operator operator,
      final Map.Entry<String, IndexSchema> indexSchema, final List<ColumnImpl> columns, final List<IdEntry> srcValues, final int shard,
      AtomicReference<String> usedIndex, final RecordCache recordCache, final long viewVersion) {

    Timer.Context ctx = DatabaseClient.BATCH_INDEX_LOOKUP_STATS.time();
    try {
      final long previousSchemaVersion = common.getSchemaVersion();

      //todo: get replica count
      //final int replica = ThreadLocalRandom.current().nextInt(0, 2);

      usedIndex.set(indexSchema.getKey());

      final int threadCount = 1;

      Future[] futures = new Future[threadCount];
      for (int i = 0; i < threadCount; i++) {
        final int offset = i;
        futures[i] = client.getExecutor().submit(new Callable<BatchLookupReturn>() {
          @Override
          public BatchLookupReturn call() {
            try {
              ComObject cobj = new ComObject();
              cobj.put(ComObject.Tag.serializationVersion, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
              cobj.put(ComObject.Tag.tableName, tableSchema.getName());
              cobj.put(ComObject.Tag.indexName, indexSchema.getKey());
              cobj.put(ComObject.Tag.leftOperator, operator.getId());

              DataUtil.ResultLength resultLength = new DataUtil.ResultLength();

              ComArray columnArray = cobj.putArray(ComObject.Tag.columnOffsets, ComObject.Type.intType);
              writeColumns(tableSchema, columns, columnArray);

              int subCount = srcValues.size(); //(offset == (threadCount - 1) ? srcValues.size() - ((threadCount - 1) * srcValues.size() / threadCount) : srcValues.size() / threadCount);

              boolean writingLongs = false;
              if (srcValues.size() > 0 && srcValues.get(0).getValue().length == 1) {
                Object value = srcValues.get(0).getValue()[0];
                if (value instanceof Long) {
                  cobj.put(ComObject.Tag.singleValue, true);
                  writingLongs = true;
                }
                else {
                  cobj.put(ComObject.Tag.singleValue, false);
                  writingLongs = false;
                  //throw new DatabaseException("not supported");
                }
              }
              else {
                cobj.put(ComObject.Tag.singleValue, false);
              }

              ComArray keys = cobj.putArray(ComObject.Tag.keys, ComObject.Type.objectType);
              int k = 0; ///(offset * srcValues.size() / threadCount);
              for (; k < /*((offset * srcValues.size() / threadCount) + */subCount; k++) {
                ComObject key = new ComObject();
                keys.add(key);

                IdEntry entry = srcValues.get(k);
                key.put(ComObject.Tag.offset, entry.getOffset());
                if (writingLongs) {
                  key.put(ComObject.Tag.longKey, (long) entry.getValue()[0]);
                }
                else {
                  key.put(ComObject.Tag.keyBytes, DatabaseCommon.serializeKey(tableSchema, indexSchema.getKey(), entry.getValue()));
                }
              }

              cobj.put(ComObject.Tag.dbName, dbName);
              cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
              cobj.put(ComObject.Tag.count, count);
              cobj.put(ComObject.Tag.method, "batchIndexLookup");
              String command = "DatabaseServer:ComObject:batchIndexLookup:";
              byte[] lookupRet = client.send(null, shard, -1, command, cobj, DatabaseClient.Replica.def);
              if (previousSchemaVersion < common.getSchemaVersion()) {
                throw new SchemaOutOfSyncException();
              }
              AtomicLong serializedSchemaVersion = null;
              Record headerRecord = null;
//              ByteArrayInputStream bytes = new ByteArrayInputStream(lookupRet);
//              DataInputStream in = new DataInputStream(bytes);
              ComObject retObj = new ComObject(lookupRet);
              long serializationVersion = retObj.getLong(ComObject.Tag.serializationVersion);
              Map<Integer, Object[][]> retKeys = new HashMap<>();
              Map<Integer, Record[]> retRecords = new HashMap<>();
              ComArray retKeysArray = retObj.getArray(ComObject.Tag.retKeys);
              for (Object entryObj : retKeysArray.getArray()) {
                ComObject retEntryObj = (ComObject)entryObj;
                int offset = retEntryObj.getInt(ComObject.Tag.offset);
                ComArray keysArray = retEntryObj.getArray(ComObject.Tag.keys);
                Object[][] ids = null;
                if (keysArray.getArray().size() != 0) {
                  ids = new Object[keysArray.getArray().size()][];
                  for (int j = 0; j < ids.length; j++) {
                    byte[] keyBytes = (byte[])keysArray.getArray().get(j);
                    ids[j] = DatabaseCommon.deserializeKey(tableSchema, keyBytes);
                  }
                  aggregateKeys(retKeys, offset, ids);
                }

                ComArray recordsArray = retEntryObj.getArray(ComObject.Tag.records);
                if (recordsArray.getArray().size() != 0) {
                  Record[] records = new Record[recordsArray.getArray().size()];

                  for (int l = 0; l < records.length; l++) {
                    byte[] recordBytes = (byte[])recordsArray.getArray().get(l);
                    records[l] = new Record(dbName, common, recordBytes);
                  }

                  aggregateRecords(retRecords, offset, records);
                }
                else {
                  if (ids != null) {
                    Record[] records = new Record[ids.length];
                    for (int j = 0; j < ids.length; j++) {
                      Object[] key = ids[j];
                      Record record = doReadRecord(dbName, client, forceSelectOnServer, recordCache, key,
                          tableSchema.getName(), columns, null, null, viewVersion, false);
                      records[j] = record;
                    }
                    aggregateRecords(retRecords, offset, records);
                  }
                }
              }
              BatchLookupReturn batchReturn = new BatchLookupReturn();
              batchReturn.keys = retKeys;
              batchReturn.records = retRecords;
              return batchReturn;
            }
            catch (IOException e) {
              throw new DatabaseException(e);
            }
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

  static AtomicInteger indexCount = new AtomicInteger();
  static long indexBegin = System.currentTimeMillis();

  private static final MetricRegistry METRICS = new MetricRegistry();

  static class PreparedIndexLookup {
    private long preparedId;
    private long lastTimeUsed;
    private boolean[][] serversPrepared;
  }

  private static Thread preparedReaper;

  public static void stopPreparedReaper() {
    if (preparedReaper != null) {
      preparedReaper.interrupt();
    }
  }


  public static void startPreparedReaper(final DatabaseClient client) {
    preparedReaper = new Thread(new Runnable(){
      @Override
      public void run() {
        while (true) {
          try {
            for (Map.Entry<String, PreparedIndexLookup> prepared : preparedIndexLookups.entrySet()) {
              if (prepared.getValue().lastTimeUsed != 0 &&
                      prepared.getValue().lastTimeUsed < System.currentTimeMillis() - 15 * 60 * 1000) {
                preparedIndexLookups.remove(prepared.getKey());

                ComObject cobj = new ComObject();
                cobj.put(ComObject.Tag.dbName, "__none__");
                cobj.put(ComObject.Tag.schemaVersion, client.getCommon().getSchemaVersion());
                cobj.put(ComObject.Tag.method, "expirePreparedStatement");
                cobj.put(ComObject.Tag.preparedId, prepared.getValue().preparedId);
                String command = "DatabaseServer:ComObject:expirePreparedStatement:";

                client.sendToAllShards(null, 0, command, cobj, DatabaseClient.Replica.def);
              }
            }
            Thread.sleep(10 * 1000);
          }
          catch (InterruptedException e) {
            break;
          }
          catch (Exception e) {
            logger.error("Error in prepared reaper thread", e);
          }
        }
      }
    });
    preparedReaper.start();
  }

  private static ConcurrentHashMap<String, PreparedIndexLookup> preparedIndexLookups = new ConcurrentHashMap<>();

  public static SelectContextImpl lookupIds(
      String dbName, DatabaseCommon common, DatabaseClient client, int replica,
      int count, String tableName, String indexName, boolean forceSelectOnServer, BinaryExpression.Operator leftOperator,
      BinaryExpression.Operator rightOperator,
      List<OrderByExpressionImpl> orderByExpressions,
      Object[] leftValue, ParameterHandler parms, Expression expression, Object[] rightValue,
      Object[] originalLeftValue,
      Object[] originalRightValue,
      List<ColumnImpl> columns, String columnName, int shard, RecordCache recordCache,
      AtomicReference<String> usedIndex, boolean evaluateExpression, long viewVersion, Counter[] counters,
      GroupByContext groupByContext, boolean debug, AtomicLong currOffset, Limit limit, Offset offset) {

    Timer.Context ctx = DatabaseClient.INDEX_LOOKUP_STATS.time();
    StringBuilder preparedKey = new StringBuilder();
    while (true) {
      try {
        //todo: do we really want to change the view version?
        if (viewVersion == 0) {
          throw new DatabaseException("view version not set");
          //viewVersion = common.getSchemaVersion();
        }

        TableSchema tableSchema = common.getTables(dbName).get(tableName);
        IndexSchema indexSchema = tableSchema.getIndexes().get(indexName);
        int originalShard = shard;
        int lastShard = -1;
        boolean currPartitions = false;
        List<Integer> selectedShards = null;
        int currShardOffset = 0;
        long previousSchemaVersion = common.getSchemaVersion();
        Object[][][] retKeys;
        Record[] recordRet = null;
        Object[] nextKey = null;
        int nextShard = shard;
        int localShard = shard;
        Object[] localLeftValue = leftValue;

        List<Object> leftValues = new ArrayList<>();
        leftValues.add(localLeftValue);

        List<Object> rightValues = new ArrayList<>();
        rightValues.add(rightValue);

        String[] fields = indexSchema.getFields();
        boolean shouldIndex = true;
        if (fields.length == 1 && !fields[0].equals(columnName)) {
          shouldIndex = false;
        }

        //int replica = ThreadLocalRandom.current().nextInt(0,2);
        if (shouldIndex) {

          retKeys = null;

          String[] indexFields = indexSchema.getFields();
          int[] fieldOffsets = new int[indexFields.length];
          for (int k = 0; k < indexFields.length; k++) {
            fieldOffsets[k] = tableSchema.getFieldOffset(indexFields[k]);
          }
          Comparator[] comparators = indexSchema.getComparators();

          if (originalLeftValue != null && leftValue != null) {
            if (0 != DatabaseCommon.compareKey(comparators, originalLeftValue, leftValue)) {
              if (leftOperator == BinaryExpression.Operator.less) {
                leftOperator = BinaryExpression.Operator.lessEqual;
              }
              else if (leftOperator == BinaryExpression.Operator.greater) {
                leftOperator = BinaryExpression.Operator.greaterEqual;
              }
            }
          }
          //
          //        if (originalLeftValue != null && leftValue != null) {
          //          if (0 != DatabaseCommon.compareKey(comparators, originalLeftValue, leftValue)) {
          //            if (leftOperator == BinaryExpression.Operator.lessEqual) {
          //              leftOperator = BinaryExpression.Operator.less;
          //            }
          //            else if (leftOperator == BinaryExpression.Operator.greaterEqual) {
          //              leftOperator = BinaryExpression.Operator.greater;
          //            }
          //          }
          //        }

          if (originalRightValue != null && leftValue != null) {
            if (0 != DatabaseCommon.compareKey(comparators, originalRightValue, leftValue)) {
              if (rightOperator == BinaryExpression.Operator.less) {
                rightOperator = BinaryExpression.Operator.lessEqual;
              }
              else if (rightOperator == BinaryExpression.Operator.greater) {
                rightOperator = BinaryExpression.Operator.greaterEqual;
              }
            }
          }
          //
          //        if (originalRightValue != null && leftValue != null) {
          //          if (0 != DatabaseCommon.compareKey(comparators, originalRightValue, leftValue)) {
          //            if (rightOperator == BinaryExpression.Operator.lessEqual) {
          //              rightOperator = BinaryExpression.Operator.less;
          //            }
          //            else if (rightOperator == BinaryExpression.Operator.greaterEqual) {
          //              rightOperator = BinaryExpression.Operator.greater;
          //            }
          //          }
          //        }
          //
          if (nextShard == -2) {
            return new SelectContextImpl();
          }

          if (debug) {
            selectedShards = new ArrayList<>();
            for (int i = 0; i < client.getShardCount(); i++) {
              selectedShards.add(i);
            }
            localLeftValue = originalLeftValue;
          }
          else {

            currPartitions = false;
            selectedShards = new ArrayList<>();
            selectedShards = Repartitioner.findOrderedPartitionForRecord(false, true, fieldOffsets, common, tableSchema,
                indexSchema.getName(), orderByExpressions, leftOperator, rightOperator, originalLeftValue, originalRightValue);
            if (selectedShards.size() == 0) {
              currPartitions = true;
              selectedShards = Repartitioner.findOrderedPartitionForRecord(true, false, fieldOffsets, common, tableSchema,
                  indexSchema.getName(), orderByExpressions, leftOperator, rightOperator, originalLeftValue, originalRightValue);
              //              for (Integer curr : currSelectedShards) {
              //                boolean found = false;
              //                for (Integer last : selectedShards) {
              //                  if (last.equals(curr)) {
              //                    found = true;
              //                  }
              //                }
              //                if (!found) {
              //                  selectedShards.add(curr);
              //                }
              //              }
            }
            //              if (selectedShards.size() == 0) {
            //                throw new DatabaseException("No shards selected for query");
            //              }
            //            ''}
          }

          if (localShard == -1) {
            localShard = nextShard = selectedShards.get(currShardOffset);
            lastShard = localShard;

          }
          boolean found = false;
          for (int i = 0; i < selectedShards.size(); i++) {
            if (localShard == selectedShards.get(i)) {
              found = true;
            }
          }
          if (!found) {
            localShard = nextShard = selectedShards.get(currShardOffset);
            lastShard = localShard;
          }
          usedIndex.set(indexSchema.getName());

          preparedKey.append(dbName).append(":").append(count);
          preparedKey.append(":").append(tableSchema.getName()).append(":").append(indexSchema.getName());
          preparedKey.append(":").append(forceSelectOnServer);
          if (orderByExpressions != null) {
            //todo: this is printing an object
            for (OrderByExpressionImpl orderByExp : orderByExpressions) {
              preparedKey.append(":").append(orderByExp.toString());
            }
          }
          preparedKey.append(":").append(expression);
          if (columns != null) {
            for (ColumnImpl column : columns) {
              preparedKey.append(":").append(column.toString());
            }
          }
          preparedKey.append(":").append(columnName);
          preparedKey.append(":").append(evaluateExpression);

          String preparedKeyStr = preparedKey.toString();
          PreparedIndexLookup prepared = null;
          synchronized (preparedIndexLookups) {
            if (leftOperator == BinaryExpression.Operator.equal && rightValue == null) {
              prepared = preparedIndexLookups.get(preparedKeyStr);
            }
            if (prepared == null) {
              prepared = new PreparedIndexLookup();
              prepared.preparedId = client.allocateId(dbName);
              prepared.serversPrepared = new boolean[client.getShardCount()][];
              for (int i = 0; i < prepared.serversPrepared.length; i++) {
                prepared.serversPrepared[i] = new boolean[client.getReplicaCount()];
              }
              preparedIndexLookups.put(preparedKeyStr, prepared);
            }
            prepared.lastTimeUsed = System.currentTimeMillis();
          }

          String[] cfields = tableSchema.getPrimaryKey();
          int[] keyOffsets = new int[cfields.length];
          for (int i = 0; i < keyOffsets.length; i++) {
            keyOffsets[i] = tableSchema.getFieldOffset(cfields[i]);
          }

          boolean keyContainsColumns = true;
          if (columns == null || columns.size() == 0 || counters != null) {
            keyContainsColumns = false;
          }
          else {
            List<Integer> array = new ArrayList<>();
            for (ColumnImpl column : columns) {
              if (column.getTableName() == null || column.getTableName().equals(tableSchema.getName())) {
                Integer o = tableSchema.getFieldOffset(column.getColumnName());
                if (o == null) {
                  continue;
                }
                array.add(o);
              }
            }
            for (Integer columnOffset : array) {
              boolean cfound = false;
              for (int i = 0; i < keyOffsets.length; i++) {
                if (columnOffset == keyOffsets[i]) {
                  cfound = true;
                }
              }
              if (!cfound) {
                keyContainsColumns = false;
                break;
              }
            }
          }


          Object[] lastKey = null;
          boolean justSwitched = false;
          int attempt = 0;
          while (true) {
            lastKey = nextKey;
            lastShard = nextShard;
            boolean switchedShards = false;

            boolean isPrepared = prepared.serversPrepared[localShard][replica];
            long preparedId = prepared.preparedId;

            //TableSchema.Partition[] partitions = indexSchema.getValue().getCurrPartitions();
            Random rand = new Random(System.currentTimeMillis());
            ComObject cobj = new ComObject();
            cobj.put(ComObject.Tag.dbName, dbName);
            cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());

            cobj.put(ComObject.Tag.preparedId, preparedId);
            cobj.put(ComObject.Tag.isPrepared, isPrepared);

            if (!isPrepared) {
              cobj.put(ComObject.Tag.count, count);
            }
            cobj.put(ComObject.Tag.isExcpliciteTrans, client.isExplicitTrans());
            cobj.put(ComObject.Tag.isCommitting, client.isCommitting());
            cobj.put(ComObject.Tag.transactionId, client.getTransactionId());
            cobj.put(ComObject.Tag.viewVersion, viewVersion);

            cobj.put(ComObject.Tag.currOffset, currOffset.get());
            if (limit != null) {
              cobj.put(ComObject.Tag.limitLong, limit.getRowCount());
            }
            if (offset != null) {
              cobj.put(ComObject.Tag.offsetLong, offset.getOffset());
            }

            if (!isPrepared) {
              cobj.put(ComObject.Tag.tableId, tableSchema.getTableId());
              cobj.put(ComObject.Tag.indexId, indexSchema.getIndexId());
              cobj.put(ComObject.Tag.forceSelectOnServer, forceSelectOnServer);
            }
            if (parms != null) {
              byte[] bytes = parms.serialize();
              cobj.put(ComObject.Tag.parms, bytes);
            }
            if (!isPrepared) {
              cobj.put(ComObject.Tag.evaluateExpression, evaluateExpression);
              if (expression != null) {
                byte[] bytes = ExpressionImpl.serializeExpression((ExpressionImpl) expression);
                cobj.put(ComObject.Tag.legacyExpression, bytes);
              }
              if (orderByExpressions != null) {
                ComArray array = cobj.putArray(ComObject.Tag.orderByExpressions, ComObject.Type.byteArrayType);
                for (int j = 0; j < orderByExpressions.size(); j++) {
                  OrderByExpressionImpl orderByExpression = orderByExpressions.get(j);
                  byte[] bytes = orderByExpression.serialize();
                  array.add(bytes);
                }
              }
            }

            if (localLeftValue != null) {
              cobj.put(ComObject.Tag.leftKey, DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), localLeftValue));
            }
            if (originalLeftValue != null) {
              cobj.put(ComObject.Tag.originalLeftKey, DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), originalLeftValue));
            }
            cobj.put(ComObject.Tag.leftOperator, leftOperator.getId());

            if (rightOperator != null) {
              if (rightValue != null) {
                cobj.put(ComObject.Tag.rightKey, DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), rightValue));
              }

              if (originalRightValue != null) {
                cobj.put(ComObject.Tag.originalRightKey, DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), originalRightValue));
              }

              //out.writeInt(rightOperator.getId());
              cobj.put(ComObject.Tag.rightOperator, rightOperator.getId());
            }

            if (!isPrepared) {
              ComArray columnArray = cobj.putArray(ComObject.Tag.columnOffsets, ComObject.Type.intType);
              writeColumns(tableSchema, columns, columnArray);
            }

            if (counters != null) {
              ComArray array = cobj.putArray(ComObject.Tag.counters, ComObject.Type.byteArrayType);
              for (int i = 0; i < counters.length; i++) {
                array.add(counters[i].serialize());
              }
            }

            if (groupByContext != null) {
              cobj.put(ComObject.Tag.legacyGroupContext, groupByContext.serialize(client.getCommon()));
            }

            List<Integer> replicas = singletonList(replica);
            if (debug) {
              replicas = new ArrayList<>();
              replicas.add(0);
              replicas.add(1);
            }

//            TableSchema.Partition[] lastPartitions = indexSchema.getLastPartitions();
//            if (lastPartitions != null && lastPartitions[0].getUpperKey() != null) {
//              if (leftValue != null && nextShard == 0) {
//                if ((long)lastPartitions[0].getUpperKey()[0] - (long)leftValue[0] < 800) {
//                  System.out.println("almost");
//                }
//              }
//            }


            cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
            cobj.put(ComObject.Tag.dbName, dbName);
            cobj.put(ComObject.Tag.method, "indexLookup");
            String command = "DatabaseServer:ComObject:indexLookup:";

            //Timer.Context ctx = INDEX_LOOKUP_SEND_STATS.time();
            byte[] lookupRet = client.send(null, localShard, 0, command, cobj, DatabaseClient.Replica.def);
            //ctx.stop();

            prepared.serversPrepared[localShard][replica] = true;


            int calledShard = localShard;
            if (previousSchemaVersion < common.getSchemaVersion()) {
              throw new SchemaOutOfSyncException();
            }
            ComObject retObj = new ComObject(lookupRet);
            byte[] keyBytes = retObj.getByteArray(ComObject.Tag.keyBytes);
            if (keyBytes != null) {
              Object[] retKey = DatabaseCommon.deserializeKey(tableSchema, keyBytes);
              nextKey = retKey;
            }
            else {
              nextKey = null;
            }
            Long retOffset = retObj.getLong(ComObject.Tag.currOffset);
            if (retOffset != null) {
              currOffset.set(retOffset);
            }
            for (int i = 0; i < selectedShards.size(); i++) {
              if (localShard == selectedShards.get(i)) {
                if (nextKey == null && i >= selectedShards.size() - 1) {
                  localShard = nextShard = -2;
                  //System.out.println("nextKey == null && > shards");
                  break;
                }
                else {
                  if (nextKey == null) {
                    localShard = nextShard = selectedShards.get(i + 1);
                    switchedShards = true;
                    //System.out.println("nextKey == null, nextShard=" + localShard);
                  }
                  break;
                }
              }
            }
            //              if (retCount != 0 || nextKey != null) {
            //                localLeftValue = nextKey;
            //              }
            //              else {
            //                nextKey = localLeftValue;
            //              }
            if (debug && localLeftValue == null) {
              localLeftValue = originalLeftValue;
            }

            Object[][][] currRetKeys = null;
            ComArray keys = retObj.getArray(ComObject.Tag.keys);
            if (keys != null && keys.getArray().size() != 0) {
              currRetKeys = new Object[keys.getArray().size()][][];
              DataType.Type[] types = DatabaseCommon.deserializeKeyPrep(tableSchema, (byte[])keys.getArray().get(0));

              for (int k = 0; k < keys.getArray().size(); k++) {
                keyBytes = (byte[])keys.getArray().get(k);
               //Object[] key = DatabaseCommon.deserializeKey(tableSchema, keyBytes);
                Object[] key = DatabaseCommon.deserializeKey(tableSchema, types,  new DataInputStream(new ByteArrayInputStream(keyBytes)));
                currRetKeys[k] = new Object[][]{key};
                if (debug) {
                  System.out.println("hit key: shard=" + calledShard + ", replica=" + replica);
                }
              }
            }

            ComArray records = retObj.getArray(ComObject.Tag.records);
            Record[] currRetRecords = new Record[records == null ? 0 : records.getArray().size()];
            if (currRetRecords.length > 0) {
              for (int k = 0; k < currRetRecords.length; k++) {
                byte[] recordBytes = (byte[])records.getArray().get(k);
                try {
                  currRetRecords[k] = new Record(dbName, client.getCommon(), recordBytes);
                  if (debug) {
                    System.out.println("hit record: shard=" + calledShard + ", replica=" + replica);
                  }
                }
                catch (Exception e) {
                  throw e;
                }
              }
            }

            recordRet = aggregateResults(recordRet, currRetRecords);


            retKeys = aggregateResults(retKeys, currRetKeys);

            if (switchedShards && logger.isDebugEnabled()) {
              //long id = (Long) currRetRecords[currRetRecords.length - 1].getFields()[tableSchema.getFieldOffset("id")];
              logger.debug("Switched shards: id=" + (nextKey == null ? "null" : (long)nextKey[0]) +
                  ", retLen=" + (recordRet == null ? 0 : recordRet.length) + ", count=" + count + ", nextShard=" + nextShard);
            }

            Counter[] retCounters = null;
            ComArray countersArray = retObj.getArray(ComObject.Tag.counters);
            if (countersArray != null) {
              retCounters = new Counter[countersArray.getArray().size()];
              for (int i = 0; i < retCounters.length; i++) {
                retCounters[i] = new Counter();
                retCounters[i].deserialize((byte[])countersArray.getArray().get(i));
              }
              System.arraycopy(retCounters, 0, counters, 0, Math.min(counters.length, retCounters.length));
            }

            byte[] groupBytes = retObj.getByteArray(ComObject.Tag.legacyGroupContext);
            if (groupBytes != null) {
              groupByContext.deserialize(groupBytes, client.getCommon(), dbName);
            }

//            if (attempt >= 5) {
//              attempt = 0;
//            }
//            else {
//              if (justSwitched) {
//                justSwitched = false;
//                if (groupBytes == null && countersArray == null && (currRetKeys == null || currRetKeys.length == 0) && (currRetRecords == null || currRetRecords.length == 0)) {
//                  //System.out.println("just switched, found nothing");
//                  if (true || lastKey != null) {
//                    nextShard = lastShard;
//                    nextKey = lastKey;
//                    replica = (replica + 1) % client.getReplicaCount();
//                    attempt++;
//                  }
//                }
//                else {
//                  attempt = 0;
//                }
//              }
//            }

            if (switchedShards) {
              justSwitched = true;
            }

            if (switchedShards && logger.isDebugEnabled()) {
              long id = currRetRecords.length == 0 ? -1 : (Long) currRetRecords[currRetRecords.length - 1].getFields()[tableSchema.getFieldOffset("id")];
              logger.debug("Switched shards: id=" + id +
                  ", retLen=" + (recordRet == null ? 0 : recordRet.length) + ", count=" + count + ", nextShard=" + nextShard);
            }

            if (limit != null) {
              long tmpOffset = 1;
              if (offset != null) {
                tmpOffset = offset.getOffset();
              }
              if (currOffset.get() >= tmpOffset + limit.getRowCount() - 1) {
                nextShard = -2;
                nextKey = null;
                break;
              }
            }

            localLeftValue = nextKey;

            if (/*originalShard != -1 ||*/localShard == -1 || localShard == -2 || (retKeys != null && retKeys.length >= count) || (recordRet != null && recordRet.length >= count)) {
              break;
            }
          }
          if (recordRet == null) {
            String[] indexColumns = null;
            for (Map.Entry<String, IndexSchema> entry : tableSchema.getIndices().entrySet()) {
              if (entry.getValue().isPrimaryKey()) {
                indexColumns = entry.getValue().getFields();
                break;
              }
            }
            if (retKeys != null) {
              if (keyContainsColumns) {
                for (int i = 0; i < retKeys.length; i++) {
                  Object[][] key = retKeys[i];
                  Record keyRecord = new Record(tableSchema);
                  Object[] rfields = new Object[tableSchema.getFields().size()];
                  keyRecord.setFields(rfields);
                  for (int j = 0; j < keyOffsets.length; j++) {
                    keyRecord.getFields()[keyOffsets[j]] = key[0][j];
                  }

                  recordCache.put(tableSchema.getName(), key[0], new CachedRecord(keyRecord, null));
                }

              }
              else {
                List<IdEntry> keysToRead = new ArrayList<>();
                for (int i = 0; i < retKeys.length; i++) {
                  Object[][] id = retKeys[i];

                  if (!recordCache.containsKey(tableSchema.getName(), id[0])) {
                    keysToRead.add(new ExpressionImpl.IdEntry(i, id[0]));
                  }
                }
                doReadRecords(dbName, client, count, forceSelectOnServer, tableSchema, keysToRead, indexColumns,
                    columns, recordCache, viewVersion);
              }
            }
          }
          else {
            String[] primaryKeyFields = null;
            for (Map.Entry<String, IndexSchema> entry : tableSchema.getIndices().entrySet()) {
              if (entry.getValue().isPrimaryKey()) {
                primaryKeyFields = entry.getValue().getFields();
                break;
              }
            }
            retKeys = new Object[recordRet.length][][];
            for (int i = 0; i < recordRet.length; i++) {
              Record record = recordRet[i];

              Object[] key = new Object[primaryKeyFields.length];
              for (int j = 0; j < primaryKeyFields.length; j++) {
                key[j] = record.getFields()[tableSchema.getFieldOffset(primaryKeyFields[j])];
              }

              if (retKeys[i] == null) {
                retKeys[i] = new Object[][]{key};
              }

              nextKey = key;

              recordCache.put(tableSchema.getName(), key, new CachedRecord(record, null));
            }
          }
          if (previousSchemaVersion < common.getSchemaVersion()) {
            throw new SchemaOutOfSyncException();
          }

          return new SelectContextImpl(tableSchema.getName(), indexSchema.getName(), leftOperator, nextShard, nextKey,
              retKeys, recordCache, lastShard, currPartitions);
        }
        return new SelectContextImpl();
      }
      catch (Exception e) {
         if (handlePreparedNotFound(e)) {
          preparedIndexLookups.remove(preparedKey.toString());
          preparedKey = new StringBuilder();
          continue;
        }
        int index = ExceptionUtils.indexOfThrowable(e, SchemaOutOfSyncException.class);
        if (-1 != index) {
          continue;
        }
        throw new DatabaseException(e);
      }
      finally {
        ctx.stop();
      }
    }
  }

  private static boolean handlePreparedNotFound(Throwable e) {
    int index = ExceptionUtils.indexOfThrowable(e, PreparedIndexLookupNotFoundException.class);
    if (-1 != index) {
      return true;
    }
    while (true) {
      if (e.getMessage() == null) {
        break;
      }
      if (e.getMessage().contains("PreparedIndexLookupNotFoundException")) {
        return true;
      }
      e = e.getCause();
      if (e == null) {
        break;
      }
    }
    return false;
  }



  private static void writeColumns(
      TableSchema tableSchema, List<ColumnImpl> columns, DataOutputStream out, DataUtil.ResultLength resultLength) throws IOException {
    if (columns == null) {
      DataUtil.writeVLong(out, 0, resultLength);
    }
    else {
      int count = 0;
      for (ColumnImpl column : columns) {
        if (column.getTableName() == null || column.getTableName().equals(tableSchema.getName())) {
          if (tableSchema.getFieldOffset(column.getColumnName()) != null) {
            count++;
          }
        }
      }
      DataUtil.writeVLong(out, count, resultLength);

      for (ColumnImpl column : columns) {
        if (column.getTableName() == null || column.getTableName().equals(tableSchema.getName())) {
          Integer offset = tableSchema.getFieldOffset(column.getColumnName());
          if (offset == null) {
            continue;
          }
          DataUtil.writeVLong(out, offset, resultLength);
        }
      }
    }
  }

  private static void writeColumns(
      TableSchema tableSchema, List<ColumnImpl> columns, ComArray array) throws IOException {
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

  public static SelectContextImpl tableScan(
      String dbName, long viewVersion, DatabaseClient client, int count, TableSchema tableSchema, List<OrderByExpressionImpl> orderByExpressions,
      ExpressionImpl expression, ParameterHandler parms, List<ColumnImpl> columns, int shard, Object[] nextKey,
      RecordCache recordCache, Counter[] counters, GroupByContext groupByContext, AtomicLong currOffset, Limit limit, Offset offset) {
    try {
      int localShard = shard;
      Object[] localNextKey = nextKey;
      DatabaseCommon common = client.getCommon();
      long previousSchemaVersion = common.getSchemaVersion();

      List<Integer> selectedShards = new ArrayList<>();
      for (int i = 0; i < client.getShardCount(); i++) {
        selectedShards.add(i);
      }

      IndexSchema indexSchema = null;
      for (Map.Entry<String, IndexSchema> entry : common.getTables(dbName).get(tableSchema.getName()).getIndexes().entrySet()) {
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

      while (nextShard != -2 && (retKeys == null || retKeys.length < count)) {
        ComObject cobj = new ComObject();
        cobj.put(ComObject.Tag.tableId, tableSchema.getTableId());
        //DataUtil.writeVLong(out, indexSchema == null ? -1 : indexSchema.getIndexId(), resultLength);
        if (parms != null) {
          cobj.put(ComObject.Tag.parms, parms.serialize());
        }
        if (expression != null) {
          cobj.put(ComObject.Tag.legacyExpression, ExpressionImpl.serializeExpression((ExpressionImpl) expression));
        }
        if (orderByExpressions != null) {
          ComArray array = cobj.putArray(ComObject.Tag.orderByExpressions, ComObject.Type.byteArrayType);
          for (int j = 0; j < orderByExpressions.size(); j++) {
            OrderByExpressionImpl orderByExpression = orderByExpressions.get(j);
            array.add(orderByExpression.serialize());
          }
        }

        if (localNextKey != null) {
          cobj.put(ComObject.Tag.leftKey, DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), localNextKey));
        }

        cobj.put(ComObject.Tag.currOffset, currOffset.get());
        if (limit != null) {
          cobj.put(ComObject.Tag.limitLong, limit.getRowCount());
        }
        if (offset != null) {
          cobj.put(ComObject.Tag.offsetLong, offset.getOffset());
        }

        if (columns == null) {
          columns = new ArrayList<>();
        }
        for (IndexSchema schema : tableSchema.getIndexes().values()) {
          if (schema.isPrimaryKey()) {
            for (String field : schema.getFields()) {
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

        ComArray columnArray = cobj.putArray(ComObject.Tag.columnOffsets, ComObject.Type.intType);
        writeColumns(tableSchema, columns, columnArray);


        if (counters != null) {
          ComArray array = cobj.putArray(ComObject.Tag.counters, ComObject.Type.byteArrayType);
          for (int i = 0; i < counters.length; i++) {
            array.add(counters[i].serialize());
          }
        }

        if (groupByContext != null) {
          cobj.put(ComObject.Tag.legacyGroupContext, groupByContext.serialize(client.getCommon()));
        }
        cobj.put(ComObject.Tag.viewVersion, viewVersion);
        cobj.put(ComObject.Tag.count, count);
        cobj.put(ComObject.Tag.dbName, dbName);
        cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
        cobj.put(ComObject.Tag.method, "indexLookupExpression");
        String command = "DatabaseServer:ComObject:indexLookupExpression:";
        byte[] lookupRet = client.send(null, localShard, 0, command, cobj, DatabaseClient.Replica.def);
        if (previousSchemaVersion < common.getSchemaVersion()) {
          throw new SchemaOutOfSyncException();
        }
        ComObject retObj = new ComObject(lookupRet);

        Long retOffset = retObj.getLong(ComObject.Tag.currOffset);
        if (retOffset != null) {
          currOffset.set(retOffset);
        }

        localNextKey = null;
        byte[] keyBytes = retObj.getByteArray(ComObject.Tag.keyBytes);
        if (keyBytes != null) {
          Object[] retKey = DatabaseCommon.deserializeKey(tableSchema, keyBytes);
          localNextKey = retKey;
          // nextShard = shard;
        }

        if (localNextKey == null) {
          if (nextShard == selectedShards.size() - 1) {
            localShard = nextShard = -2;
          }
          else {
            localShard = nextShard = selectedShards.get(nextShard + 1);
          }
        }

        ComArray keyArray = retObj.getArray(ComObject.Tag.keys);
        Object[][][] currRetKeys = null;
        if (keyArray != null) {
          currRetKeys = new Object[keyArray.getArray().size()][][];
          for (int k = 0; k < currRetKeys.length; k++) {
            keyBytes = (byte[])keyArray.getArray().get(k);
            Object[] key = DatabaseCommon.deserializeKey(common.getTables(dbName).get(tableSchema.getName()), keyBytes);
            currRetKeys[k] = new Object[][]{key};
          }
        }

        ComArray recordArray = retObj.getArray(ComObject.Tag.records);
        int recordCount = recordArray == null ? 0 : recordArray.getArray().size();
        byte[][] currRetRecords = new byte[recordCount][];
        for (int k = 0; k < recordCount; k++) {
          byte[] recordBytes = (byte[])recordArray.getArray().get(k);
          currRetRecords[k] = recordBytes;
        }

        Counter[] retCounters = null;
        ComArray countersArray = retObj.getArray(ComObject.Tag.counters);
        if (countersArray != null) {
          retCounters = new Counter[countersArray.getArray().size()];
          for (int i = 0; i < countersArray.getArray().size(); i++) {
            retCounters[i] = new Counter();
            byte[] counterBytes = (byte[])countersArray.getArray().get(i);
            retCounters[i].deserialize(counterBytes);
          }
          System.arraycopy(retCounters, 0, counters, 0, Math.min(counters.length, retCounters.length));
        }

        byte[] groupBytes = retObj.getByteArray(ComObject.Tag.legacyGroupContext);
        if (groupBytes != null) {
          groupByContext.deserialize(groupBytes, client.getCommon(), dbName);
        }

        recordRet = aggregateResults(recordRet, currRetRecords);


        retKeys = aggregateResults(retKeys, currRetKeys);

        //    if (shard == -1 || shard == -2 || (retKeys != null && retKeys.length >= count) || (recordRet != null && recordRet.length >= count)) {
        //      b;
        //    }
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
      }

      return new SelectContextImpl(tableSchema.getName(),
          indexSchema.getName(), null, nextShard, localNextKey,
          retKeys, recordCache, -1, true);
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
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

  static Object[][][] aggregateResults(Object[][][] records1, Object[][][] records2) {
    if (records1 == null || records1.length == 0) {
      if (records2 == null || records2.length == 0) {
        return null;
      }
      return records2;
    }
    if (records2 == null || records2.length == 0) {
      if (records1 == null || records1.length == 0) {
        return null;
      }
      return records1;
    }

    Object[][][] retArray = new Object[records1.length + records2.length][][];
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
}
