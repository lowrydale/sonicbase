package com.lowryengineering.database.query.impl;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.lowryengineering.database.client.DatabaseClient;
import com.lowryengineering.database.common.DatabaseCommon;
import com.lowryengineering.database.common.Record;
import com.lowryengineering.database.common.SchemaOutOfSyncException;
import com.lowryengineering.database.index.Repartitioner;
import com.lowryengineering.database.jdbcdriver.ParameterHandler;
import com.lowryengineering.database.query.BinaryExpression;
import com.lowryengineering.database.query.DatabaseException;
import com.lowryengineering.database.query.Expression;
import com.lowryengineering.database.schema.DataType;
import com.lowryengineering.database.schema.IndexSchema;
import com.lowryengineering.database.schema.TableSchema;
import com.lowryengineering.database.server.ReadManager;
import com.lowryengineering.database.server.SnapshotManager;
import com.lowryengineering.database.util.DataUtil;
import net.sf.jsqlparser.statement.select.Limit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.singletonList;

public abstract class ExpressionImpl implements Expression {

  private static Logger logger = LoggerFactory.getLogger(ExpressionImpl.class);

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
  private int viewVersion;
  private int dbViewNum;
  private Counter[] counters;
  private Limit limit;
  private GroupByContext groupByContext;
  protected String dbName;

  public Counter[] getCounters() {
    return counters;
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

  public void setDebug(boolean debug) {
    this.debug = debug;
  }

  public void setViewVersion(int viewVersion) {
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
      out.writeInt(nextShard);
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  abstract public Type getType();

  public void deserialize(DataInputStream in) {
    try {
      nextShard = in.readInt();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  abstract public Object evaluateSingleRecord(
      TableSchema[] tableSchemas, Record[] records, ParameterHandler parms);

  abstract public NextReturn next();

  public abstract NextReturn next(int count);

  abstract public boolean canUseIndex();

  public abstract boolean canSortWithIndex();

  public abstract void queryRewrite();

  public abstract ColumnImpl getPrimaryColumn();

  public static void serializeExpression(ExpressionImpl expression, DataOutputStream out) {
    try {
      out.writeInt(expression.getType().getId());
      expression.serialize(out);
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

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


  public static class RecordCache {
    private Map<String, ConcurrentHashMap<Key, Record>> recordsForTable = new ConcurrentHashMap<>();

    public Map<String, ConcurrentHashMap<Key, Record>> getRecordsForTable() {
      return recordsForTable;
    }

    public void clear() {
      for (ConcurrentHashMap<Key, Record> records : recordsForTable.values()) {
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
          if (key[i] instanceof Long) {
            hashCode += ((Long)key[i]).hashCode();
          }
          else if (key[i] instanceof Integer) {
            hashCode += (Integer)key[i];
          }
          else if (key[i] instanceof Short) {
            hashCode += (Short)key[i];
          }
          else if (key[i] instanceof Byte) {
            hashCode += (Byte)key[i];
          }
          else if (key[i] instanceof Float) {
            hashCode += ((Float)key[i]).hashCode();
          }
          else if (key[i] instanceof Double) {
            hashCode += ((Double)key[i]).hashCode();
          }
          else if (key[i] instanceof Boolean) {
            hashCode += ((Boolean)key[i]).hashCode();
          }
          else if (key[i] instanceof BigDecimal) {
            hashCode += ((BigDecimal)key[i]).hashCode();
          }
          else if (key[i] instanceof java.sql.Date) {
            hashCode += ((java.sql.Date)key[i]).hashCode();
          }
          else if (key[i] instanceof Time) {
            hashCode += ((Time)key[i]).hashCode();
          }
          else if (key[i] instanceof Timestamp) {
            hashCode += ((Timestamp)key[i]).hashCode();
          }
          else if (key[i] instanceof byte[]) {
            hashCode += Arrays.hashCode((byte[])key[i]);
          }
          else if (key[i] instanceof Blob) {
            hashCode += Arrays.hashCode(((Blob) key[i]).getData());
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
          if (key[i] instanceof byte[]) {
            if (Arrays.equals((byte[])key[i], (byte[])((Key)o).key[i])) {
              continue;
            }
            return false;
          }
//          int value = comparators[i].compare(key[i], ((Key)o).key[i]);
//          if (value != 0) {
//            return false;
//          }
        }
        return true;
      }
    }


    public RecordCache() {

    }

    public boolean containsKey(String tableName, Object[] key) {
      ConcurrentHashMap<Key, Record> records = recordsForTable.get(tableName);
      if (records == null) {
        return false;
      }
      return records.containsKey(new Key(tableName, key));
    }

    public Record get(String tableName, Object[] key) {
      ConcurrentHashMap<Key, Record> records = recordsForTable.get(tableName);
      if (records == null) {
        return null;
      }
      return records.get(new Key(tableName, key));
    }

    public void put(String tableName, Object[] key, Record record) {
      ConcurrentHashMap<Key, Record> records = null;
      synchronized (this) {
        records = recordsForTable.get(tableName);
        if (records == null) {
          recordsForTable.put(tableName, new ConcurrentHashMap<Key, Record>());
          records = recordsForTable.get(tableName);
        }
      }
      records.put(new Key(tableName, key), record);
    }
  }

  public static HashMap<Integer, Object[][]> readRecords(
      String dbName, final DatabaseClient client, final TableSchema tableSchema,
      List<IdEntry> keysToRead, String[] columns, List<ColumnImpl> selectColumns, RecordCache recordCache, int viewVersion) {


    HashMap<Integer, Object[][]> ret = doReadRecords(dbName, client, tableSchema, keysToRead, columns, selectColumns, recordCache, viewVersion);

    return ret;
  }

  public static HashMap<Integer, Object[][]> doReadRecords(
      final String dbName, final DatabaseClient client, final TableSchema tableSchema, List<IdEntry> keysToRead, String[] columns, final List<ColumnImpl> selectColumns,
      final RecordCache recordCache, final int viewVersion) {
    try {
      int[] fieldOffsets = null;
      Comparator[] comparators = null;
      final AtomicReference<Map.Entry<String, IndexSchema>> indexSchema = new AtomicReference<>();
      for (Map.Entry<String, IndexSchema> entry : tableSchema.getIndices().entrySet()) {
        String[] fields = entry.getValue().getFields();
        boolean shouldIndex = false;
        if (fields.length == columns.length) {
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
        if (selectedShards.size() > 1) {
          if (!synced) {
            client.syncSchema();
            synced = true;
          }
          throw new DatabaseException("Invalid state. Multiple shards");
        }
        partitionedValues.get(selectedShards.get(0)).add(new IdEntry(keysToRead.get(i).getOffset(), keysToRead.get(i).getValue()));
      }


      final HashMap<Integer, Object[][]> fullMap = new HashMap<>();
      List<Future> futures = new ArrayList<>();
      for (final Map.Entry<Integer, List<IdEntry>> entry : partitionedValues.entrySet()) {
        futures.add(client.getExecutor().submit(new Callable() {
          @Override
          public Object call() {
            AtomicReference<String> usedIndex = new AtomicReference<>();
            Object rightRet = ExpressionImpl.batchLookupIds(dbName,
                client.getCommon(), client, ReadManager.SELECT_PAGE_SIZE, tableSchema,
                BinaryExpression.Operator.equal, indexSchema.get(), selectColumns, entry.getValue(), entry.getKey(), usedIndex, recordCache, viewVersion);
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
          for (Map.Entry<Integer, byte[][]> entry : curr.records.entrySet()) {
            Object[][] keys = new Object[entry.getValue().length][];
            for (int i = 0; i < entry.getValue().length; i++) {
              byte[] recordBytes = entry.getValue()[i];
              Record record = new Record(dbName, client.getCommon(), recordBytes);

              Object[] key = new Object[primaryKeyFields.length];
              for (int j = 0; j < primaryKeyFields.length; j++) {
                key[j] = record.getFields()[tableSchema.getFieldOffset(primaryKeyFields[j])];
              }

              recordCache.put(tableSchema.getName(), key, record);

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
      String dbName, DatabaseClient client, RecordCache recordCache, Object[] key, String tableName, List<ColumnImpl> columns, Expression expression,
      ParameterHandler parms, int viewVersion, boolean debug) {
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

    Record ret = recordCache.get(tableName, key);
    if (ret != null) {
      return ret;
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
    SelectContextImpl context = ExpressionImpl.lookupIds(dbName, client.getCommon(), client, replica, 1, tableSchema, primaryKeyIndex,
        BinaryExpression.Operator.equal, null, null, key, parms, expression, null, key, null,
        columns, primaryKeyIndex.getFields()[0], nextShard, recordCache, usedIndex, true, viewVersion, expression == null ? null : ((ExpressionImpl)expression).getCounters(),
        expression == null ? null : ((ExpressionImpl)expression).groupByContext, debug);
//
    Object[][][] currKeys = context.getCurrKeys();
    if (currKeys != null) {
      Object[] retKey = currKeys[0][0];
      if (retKey != null) {
        return recordCache.get(tableName, retKey);
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
      String dbName, DatabaseClient client, ParameterHandler parms, Expression expression, RecordCache recordCache, Object[] key,
      String tableName,
      List<ColumnImpl> columns, int viewVersion, boolean debug) {


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
    ExpressionImpl.lookupIds(dbName, client.getCommon(), client, replica, 1, tableSchema, primaryKeyIndex,
        BinaryExpression.Operator.equal, null, null, key, parms, expression, null, key, null,
        columns, primaryKeyIndex.getFields()[0], nextShard, recordCache, usedIndex, false, viewVersion,
        expression == null ? null : ((ExpressionImpl)expression).getCounters(),
        expression == null ? null : ((ExpressionImpl)expression).groupByContext, debug);

    return recordCache.get(tableName, key);

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
    private Map<Integer, byte[][]> records;
  }

  private static BatchLookupReturn batchLookupIds(
      final String dbName, final DatabaseCommon common, final DatabaseClient client, final int count, final TableSchema tableSchema,
      final BinaryExpression.Operator operator,
      final Map.Entry<String, IndexSchema> indexSchema, final List<ColumnImpl> columns, final List<IdEntry> srcValues, final int shard,
      AtomicReference<String> usedIndex, final RecordCache recordCache, final int viewVersion) {

    try {
      final int previousSchemaVersion = common.getSchemaVersion();

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
              ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
              DataOutputStream out = new DataOutputStream(bytesOut);
              DataUtil.writeVLong(out, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
              out.writeUTF(tableSchema.getName());
              out.writeUTF(indexSchema.getKey());
              out.writeInt(operator.getId());

              DataUtil.ResultLength resultLength = new DataUtil.ResultLength();
              writeColumns(tableSchema, columns, out, resultLength);

              int subCount = srcValues.size(); //(offset == (threadCount - 1) ? srcValues.size() - ((threadCount - 1) * srcValues.size() / threadCount) : srcValues.size() / threadCount);
              DataUtil.writeVLong(out, subCount, resultLength);

              boolean writingLongs = false;
              if (srcValues.size() > 0 && srcValues.get(0).getValue().length == 1) {
                out.writeBoolean(true);
                Object value = srcValues.get(0).getValue()[0];
                if (value instanceof Long) {
                  out.writeInt(DataType.Type.BIGINT.getValue());
                  writingLongs = true;
                }
                else {
                  throw new DatabaseException("not supported");
                }
              }
              else {
                out.writeBoolean(false);
              }

              int k = 0; ///(offset * srcValues.size() / threadCount);
              for (; k < /*((offset * srcValues.size() / threadCount) + */subCount; k++) {
                IdEntry entry = srcValues.get(k);
                DataUtil.writeVLong(out, entry.getOffset(), resultLength);
                if (writingLongs) {
                  DataUtil.writeVLong(out, (long) entry.getValue()[0], resultLength);
                }
                else {
                  out.write(DatabaseCommon.serializeKey(tableSchema, indexSchema.getKey(), entry.getValue()));
                }
              }

              out.close();


              String command = "DatabaseServer:batchIndexLookup:1:" + common.getSchemaVersion() + ":" + dbName + ":" + count;
              byte[] lookupRet = client.send(null, shard, -1, command, bytesOut.toByteArray(), DatabaseClient.Replica.def);
              if (previousSchemaVersion < common.getSchemaVersion()) {
                throw new SchemaOutOfSyncException();
              }
              ByteArrayInputStream bytes = new ByteArrayInputStream(lookupRet);
              DataInputStream in = new DataInputStream(bytes);
              long serializationVersion = DataUtil.readVLong(in);
              Map<Integer, Object[][]> retKeys = new HashMap<>();
              Map<Integer, byte[][]> retRecords = new HashMap<>();
              int count = (int) DataUtil.readVLong(in, resultLength);
              for (int i = 0; i < count; i++) {
                int offset = (int) DataUtil.readVLong(in, resultLength);
                int idCount = (int) DataUtil.readVLong(in, resultLength);
                Object[][] ids = null;
                if (idCount != 0) {
                  ids = new Object[idCount][];
                  for (int j = 0; j < ids.length; j++) {
                    int len = (int) DataUtil.readVLong(in, resultLength);
                    byte[] keyBytes = new byte[len];
                    in.readFully(keyBytes);
                    DataInputStream keyIn = new DataInputStream(new ByteArrayInputStream(keyBytes));
                    ids[j] = DatabaseCommon.deserializeKey(tableSchema, keyIn);
                  }
                  aggregateKeys(retKeys, offset, ids);
                }

                int recordCount = (int) DataUtil.readVLong(in, resultLength);
                if (recordCount != 0) {
                  byte[][] records = new byte[recordCount][];
                  for (int j = 0; j < records.length; j++) {
                    int len = (int) DataUtil.readVLong(in, resultLength);
                    byte[] record = new byte[len];
                    in.readFully(record);
                    records[j] = record;
                  }
                  aggregateRecords(retRecords, offset, records);
                }
                else {
                  if (ids != null) {
                    byte[][] records = new byte[ids.length][];
                    for (int j = 0; j < ids.length; j++) {
                      Object[] key = ids[j];
                      Record record = doReadRecord(dbName, client, recordCache, key, tableSchema.getName(), columns, null, null, viewVersion, false);
                      records[j] = record.serialize(common);
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
      Map<Integer, byte[][]> retRecords = new HashMap<>();
      for (int i = 0; i < futures.length; i++) {
        BatchLookupReturn currRet = (BatchLookupReturn) futures[i].get();
        for (Map.Entry<Integer, Object[][]> entry : currRet.keys.entrySet()) {
          aggregateKeys(ret, entry.getKey(), entry.getValue());
        }
        for (Map.Entry<Integer, byte[][]> entry : currRet.records.entrySet()) {
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

  public static final com.codahale.metrics.Timer INDEX_LOOKUP_SEND_STATS = METRICS.timer("indexLookupSend");

  public static SelectContextImpl lookupIds(
      String dbName, DatabaseCommon common, DatabaseClient client, int replica,
      int count, TableSchema tableSchema, IndexSchema indexSchema, BinaryExpression.Operator leftOperator,
      BinaryExpression.Operator rightOperator,
      List<OrderByExpressionImpl> orderByExpressions,
      Object[] leftValue, ParameterHandler parms, Expression expression, Object[] rightValue,
      Object[] originalLeftValue,
      Object[] originalRightValue,
      List<ColumnImpl> columns, String columnName, int shard, RecordCache recordCache,
      AtomicReference<String> usedIndex, boolean evalueExpression, int viewVersion, Counter[] counters, GroupByContext groupByContext, boolean debug) {

    try {
      int originalShard = shard;
      List<Integer> selectedShards = null;
      int currShardOffset = 0;
      int previousSchemaVersion = common.getSchemaVersion();
      Object[][][] retKeys;
      byte[][] recordRet = null;
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

        if (originalLeftValue != null && leftValue != null) {
          if (0 != DatabaseCommon.compareKey(comparators, originalLeftValue, leftValue)) {
            if (leftOperator == BinaryExpression.Operator.lessEqual) {
              leftOperator = BinaryExpression.Operator.less;
            }
            else if (leftOperator == BinaryExpression.Operator.greaterEqual) {
              leftOperator = BinaryExpression.Operator.greater;
            }
          }
        }

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

          selectedShards = Repartitioner.findOrderedPartitionForRecord(false, true, fieldOffsets, common, tableSchema,
              indexSchema.getName(), orderByExpressions, leftOperator, rightOperator, originalLeftValue, originalRightValue);
          if (selectedShards.size() == 0) {
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
        }

        usedIndex.set(indexSchema.getName());

        while (true) {
          //TableSchema.Partition[] partitions = indexSchema.getValue().getCurrPartitions();


          Random rand = new Random(System.currentTimeMillis());
          ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
          DataOutputStream out = new DataOutputStream(bytesOut);
          DataUtil.writeVLong(out, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
          DataUtil.ResultLength resultLength = new DataUtil.ResultLength();

          out.writeInt(count);
          out.writeBoolean(client.isExplicitTrans());
          out.writeBoolean(client.isCommitting());
          DataUtil.writeVLong(out, client.getTransactionId(), resultLength);
          DataUtil.writeVLong(out, viewVersion, resultLength);

          DataUtil.writeVLong(out, tableSchema.getTableId(), resultLength);
          DataUtil.writeVLong(out, indexSchema.getIndexId(), resultLength);
          if (!evalueExpression || parms == null) {
            out.writeBoolean(false);
          }
          else {
            out.writeBoolean(true);
            parms.serialize(out);
          }
          out.writeBoolean(evalueExpression);
          if (!evalueExpression || expression == null) {
            out.writeBoolean(false);
          }
          else {
            out.writeBoolean(true);
            ExpressionImpl.serializeExpression((ExpressionImpl) expression, out);
          }
          //          out.writeUTF(tableSchema.getName());
          //          out.writeUTF(indexSchema.getKey());
          if (orderByExpressions == null) {
            DataUtil.writeVLong(out, 0, resultLength);
            //out.writeInt(0);
          }
          else {
            //out.writeInt(orderByExpressions.size());
            DataUtil.writeVLong(out, orderByExpressions.size(), resultLength);
            for (int j = 0; j < orderByExpressions.size(); j++) {
              OrderByExpressionImpl orderByExpression = orderByExpressions.get(j);
              orderByExpression.serialize(out);
            }
          }
          if (localLeftValue == null) {
            out.writeBoolean(false);
          }
          else {
            out.writeBoolean(true);
            out.write(DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), localLeftValue));
          }
          DataUtil.writeVLong(out, leftOperator.getId(), resultLength);
          //out.writeInt(leftOperator.getId());

          if (rightOperator != null) {
            out.writeBoolean(true);
            out.writeBoolean(true);
            out.write(DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), rightValue));
            //out.writeInt(rightOperator.getId());
            DataUtil.writeVLong(out, rightOperator.getId(), resultLength);
          }
          else {
            out.writeBoolean(false);
          }

          writeColumns(tableSchema, columns, out, resultLength);

          if (counters == null) {
            out.writeInt(0);
          }
          else {
            out.writeInt(counters.length);
            for (int i = 0; i < counters.length; i++) {
              out.write(counters[i].serialize());
            }
          }

          if (groupByContext == null) {
            out.writeBoolean(false);
          }
          else {
            out.writeBoolean(true);
            out.write(groupByContext.serialize(client.getCommon()));
          }

          out.close();

          List<Integer> replicas = singletonList(replica);
          if (debug) {
            replicas = new ArrayList<>();
            replicas.add(0);
            replicas.add(1);
          }

          String command = "DatabaseServer:indexLookup:1:" + common.getSchemaVersion() + ":" + dbName + ":" + rand.nextLong();
          byte[] bytes = bytesOut.toByteArray();

          String batchKey = null;
          if (leftOperator == BinaryExpression.Operator.equal && rightOperator == null) {
            batchKey = "DatabaseServer:indexLookup:" + tableSchema.getName() + ":" + indexSchema.getName();
          }

          Timer.Context ctx = INDEX_LOOKUP_SEND_STATS.time();
          byte[] lookupRet = client.send(batchKey, localShard, replica, command, bytes, DatabaseClient.Replica.specified);
          ctx.stop();
          int calledShard = localShard;
          if (previousSchemaVersion < common.getSchemaVersion()) {
            throw new SchemaOutOfSyncException();
          }
          ByteArrayInputStream bytes2 = new ByteArrayInputStream(lookupRet);
          DataInputStream in = new DataInputStream(bytes2);
          long serializationVersion = DataUtil.readVLong(in);
          //nextKey = null;
          if (in.readBoolean()) {
            Object[] retKey = DatabaseCommon.deserializeKey(tableSchema, in);
            nextKey = retKey;
          }
          for (int i = 0; i < selectedShards.size(); i++) {
            if (localShard == selectedShards.get(i)) {
              if (debug) {
                if (replica == 1) {
                  if (nextKey == null && i >= selectedShards.size() - 1) {
                    localShard = nextShard = -2;
                    break;
                  }
                  else {
                    if (nextKey == null) {
                      localShard = nextShard = selectedShards.get(i + 1);
                    }
                    break;
                  }
                }
              }
              else {
                if (nextKey == null && i >= selectedShards.size() - 1) {
                  localShard = nextShard = -2;
                  //System.out.println("nextKey == null && > shards");
                  break;
                }
                else {
                  if (nextKey == null) {
                    localShard = nextShard = selectedShards.get(i + 1);
                    //System.out.println("nextKey == null, nextShard=" + localShard);
                  }
                  break;
                }
              }
            }
          }
          int retCount = (int) DataUtil.readVLong(in, resultLength);
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
          if (retCount != 0) {
            currRetKeys = new Object[retCount][][];
            for (int k = 0; k < retCount; k++) {
              int len = (int) DataUtil.readVLong(in, resultLength);
              byte[] keyBytes = new byte[len];
              in.readFully(keyBytes);
              DataInputStream keyIn = new DataInputStream(new ByteArrayInputStream(keyBytes));
              Object[] key = DatabaseCommon.deserializeKey(common.getTables(dbName).get(tableSchema.getName()), keyIn);
              currRetKeys[k] = new Object[][]{key};
              if (debug) {
                System.out.println("hit key: shard=" + calledShard + ", replica=" + replica);
              }
            }
          }

          int recordCount = (int) DataUtil.readVLong(in, resultLength);
          byte[][] currRetRecords = new byte[recordCount][];
          for (int k = 0; k < recordCount; k++) {
            int len = (int) DataUtil.readVLong(in, resultLength);
            byte[] recordBytes = new byte[len];
            in.readFully(recordBytes);
            currRetRecords[k] = recordBytes;
            if (debug) {
              System.out.println("hit record: shard=" + calledShard + ", replica=" + replica);
            }
          }

          recordRet = aggregateResults(recordRet, currRetRecords);


          retKeys = aggregateResults(retKeys, currRetKeys);


          Counter[] retCounters = null;
          int counterCount = in.readInt();
          if (counterCount > 0) {
            retCounters = new Counter[counterCount];
            for (int i = 0; i < counterCount; i++) {
              retCounters[i] = new Counter();
              retCounters[i].deserialize(in);
            }
            System.arraycopy(retCounters, 0, counters, 0, Math.min(counters.length, retCounters.length));
          }

          if (in.readBoolean()) {
            groupByContext.deserialize(in, client.getCommon(), dbName);
          }

          if (originalShard != -1 || localShard == -1 || localShard == -2 || (retKeys != null && retKeys.length >= count) || (recordRet != null && recordRet.length >= count)) {
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
            List<IdEntry> keysToRead = new ArrayList<>();
            for (int i = 0; i < retKeys.length; i++) {
              Object[][] id = retKeys[i];

              if (!recordCache.containsKey(tableSchema.getName(), id[0])) {
                keysToRead.add(new ExpressionImpl.IdEntry(i, id[0]));
              }
            }
            doReadRecords(dbName, client, tableSchema, keysToRead, indexColumns, columns, recordCache, viewVersion);
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
            byte[] curr = recordRet[i];

            Record record = new Record(dbName, client.getCommon(), curr);
            Object[] key = new Object[primaryKeyFields.length];
            for (int j = 0; j < primaryKeyFields.length; j++) {
              key[j] = record.getFields()[tableSchema.getFieldOffset(primaryKeyFields[j])];
            }

            if (retKeys[i] == null) {
              retKeys[i] = new Object[][]{key};
            }

            nextKey = key;

            recordCache.put(tableSchema.getName(), key, record);
          }
        }
        if (previousSchemaVersion < common.getSchemaVersion()) {
           throw new SchemaOutOfSyncException();
         }

        return new SelectContextImpl(tableSchema.getName(), indexSchema.getName(), leftOperator, nextShard, nextKey, retKeys, recordCache);
      }
      return new SelectContextImpl();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
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

  public static SelectContextImpl tableScan(
      String dbName, DatabaseClient client, int count, TableSchema tableSchema, List<OrderByExpressionImpl> orderByExpressions,
      ExpressionImpl expression, ParameterHandler parms, List<ColumnImpl> columns, int shard, Object[] nextKey,
      RecordCache recordCache, Counter[] counters, GroupByContext groupByContext) {
    try {
      int localShard = shard;
      Object[] localNextKey = nextKey;
      DatabaseCommon common = client.getCommon();
      int previousSchemaVersion = common.getSchemaVersion();

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
        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytesOut);
        DataUtil.writeVLong(out, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
        DataUtil.ResultLength resultLength = new DataUtil.ResultLength();
        DataUtil.writeVLong(out, tableSchema.getTableId(), resultLength);
        //DataUtil.writeVLong(out, indexSchema == null ? -1 : indexSchema.getIndexId(), resultLength);
        if (parms == null) {
          out.writeBoolean(false);
        }
        else {
          out.writeBoolean(true);
          parms.serialize(out);
        }
        if (expression == null) {
          out.writeBoolean(false);
        }
        else {
          out.writeBoolean(true);
          ExpressionImpl.serializeExpression((ExpressionImpl) expression, out);
        }
        if (orderByExpressions == null) {
          DataUtil.writeVLong(out, 0, resultLength);
        }
        else {
          DataUtil.writeVLong(out, orderByExpressions.size(), resultLength);
          for (int j = 0; j < orderByExpressions.size(); j++) {
            OrderByExpressionImpl orderByExpression = orderByExpressions.get(j);
            orderByExpression.serialize(out);
          }
        }

        if (localNextKey == null) {
          out.writeBoolean(false);
        }
        else {
          out.writeBoolean(true);
          out.write(DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), localNextKey));
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

        writeColumns(tableSchema, columns, out, resultLength);

        if (counters == null) {
          out.writeInt(0);
        }
        else {
          out.writeInt(counters.length);
          for (int i = 0; i < counters.length; i++) {
            out.write(counters[i].serialize());
          }
        }

        if (groupByContext == null) {
          out.writeBoolean(false);
        }
        else {
          out.writeBoolean(true);
          out.write(groupByContext.serialize(client.getCommon()));
        }

        out.close();

        String command = "DatabaseServer:indexLookupExpression:1:" + common.getSchemaVersion() + ":" + dbName + ":" + count;
        byte[] lookupRet = client.send(null, localShard, 0, command, bytesOut.toByteArray(), DatabaseClient.Replica.def);
        if (previousSchemaVersion < common.getSchemaVersion()) {
          throw new SchemaOutOfSyncException();
        }
        ByteArrayInputStream bytes = new ByteArrayInputStream(lookupRet);
        DataInputStream in = new DataInputStream(bytes);
        long serializationVersion = DataUtil.readVLong(in);

        localNextKey = null;
        if (in.readBoolean()) {
          Object[] retKey = DatabaseCommon.deserializeKey(tableSchema, in);
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

        int retCount = (int) DataUtil.readVLong(in, resultLength);

        Object[][][] currRetKeys = null;
        if (retCount != 0) {
          currRetKeys = new Object[retCount][][];
          for (int k = 0; k < retCount; k++) {
            int len = (int) DataUtil.readVLong(in, resultLength);
            byte[] keyBytes = new byte[len];
            in.readFully(keyBytes);
            DataInputStream keyIn = new DataInputStream(new ByteArrayInputStream(keyBytes));
            Object[] key = DatabaseCommon.deserializeKey(common.getTables(dbName).get(tableSchema.getName()), keyIn);
            currRetKeys[k] = new Object[][]{key};
          }
        }

        int recordCount = (int) DataUtil.readVLong(in, resultLength);
        byte[][] currRetRecords = new byte[recordCount][];
        for (int k = 0; k < recordCount; k++) {
          int len = (int) DataUtil.readVLong(in, resultLength);
          byte[] recordBytes = new byte[len];
          in.readFully(recordBytes);
          currRetRecords[k] = recordBytes;
        }

        Counter[] retCounters = null;
        int counterCount = in.readInt();
        if (counterCount > 0) {
          retCounters = new Counter[counterCount];
          for (int i = 0; i < counterCount; i++) {
            retCounters[i] = new Counter();
            retCounters[i].deserialize(in);
          }
          System.arraycopy(retCounters, 0, counters, 0, Math.min(counters.length, retCounters.length));
        }

        if (in.readBoolean()) {
          groupByContext.deserialize(in, client.getCommon(), dbName);
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

            recordCache.put(tableSchema.getName(), key, record);
          }

        }
      }

      return new SelectContextImpl(tableSchema.getName(),
          indexSchema.getName(), null, nextShard, localNextKey,
          retKeys, recordCache);
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
}
