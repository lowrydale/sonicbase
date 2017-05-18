package com.sonicbase.server;

import com.codahale.metrics.MetricRegistry;
import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.common.Logger;
import com.sonicbase.common.Record;
import com.sonicbase.common.SchemaOutOfSyncException;
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

/**
 * Responsible for
 */
public class ReadManager {

  private Logger logger;

  private final DatabaseServer server;
  private Thread preparedReaper;

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
  }


  public static final int SELECT_PAGE_SIZE = 30000;

  public byte[] countRecords(String command, byte[] body) {
    try {
      String[] parts = command.split(":");
      String dbName = parts[4];
      int schemaVersion = Integer.valueOf(parts[3]);
      if (schemaVersion < server.getSchemaVersion()) {
        throw new SchemaOutOfSyncException("currVer:" + server.getCommon().getSchemaVersion() + ":");
      }
      String fromTable = parts[5];

      Expression expression = null;
      DataInputStream in = new DataInputStream(new ByteArrayInputStream(body));
      long serializationVersion = DataUtil.readVLong(in);
      if (in.readBoolean()) {
        expression = ExpressionImpl.deserializeExpression(in);
      }
      ParameterHandler parms = null;
      if (in.readBoolean()) {
        parms = new ParameterHandler();
        parms.deserialize(in);
      }
      if (in.readBoolean()) {
        in.readUTF();
      }
      String countColumn = null;
      if (in.readBoolean()) {
        countColumn = in.readUTF();
      }

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
            if (entry.getValue() instanceof Long) {
              entry.setValue(index.get(entry.getKey()));
            }
            if (entry.getValue() != null) {
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

      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytesOut);
      DataUtil.writeVLong(out, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
      out.writeLong(count);

      out.close();
      return bytesOut.toByteArray();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public byte[] batchIndexLookup(String command, byte[] body) {
    try {
      String[] parts = command.split(":");
      String dbName = parts[4];
      int schemaVersion = Integer.valueOf(parts[3]);
      if (schemaVersion < server.getSchemaVersion()) {
        throw new SchemaOutOfSyncException("currVer:" + server.getCommon().getSchemaVersion() + ":");
      }
      int count = Integer.valueOf(parts[5]);
      DataInputStream in = new DataInputStream(new ByteArrayInputStream(body));
      long serializationVersion = DataUtil.readVLong(in);
      String tableName = in.readUTF();
      String indexName = in.readUTF();

      TableSchema tableSchema = server.getCommon().getSchema(dbName).getTables().get(tableName);
      IndexSchema indexSchema = tableSchema.getIndices().get(indexName);
      DataUtil.ResultLength resultLength = new DataUtil.ResultLength();

      Index index = server.getIndices(dbName).getIndices().get(tableSchema.getName()).get(indexName);
      Boolean ascending = null;

      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytesOut);
      DataUtil.writeVLong(out, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);

      BinaryExpression.Operator leftOperator = BinaryExpression.Operator.getOperator(in.readInt());

      Set<Integer> columnOffsets = getSimpleColumnOffsets(in, resultLength, tableName, tableSchema);

      int keyCount = (int) DataUtil.readVLong(in, resultLength);
      boolean singleValue = in.readBoolean();
      if (singleValue) {
        in.readInt(); //type
      }

      IndexSchema primaryKeyIndexSchema = null;
      Index primaryKeyIndex = null;
      for (Map.Entry<String, IndexSchema> entry : tableSchema.getIndices().entrySet()) {
        if (entry.getValue().isPrimaryKey()) {
          primaryKeyIndexSchema = entry.getValue();
          primaryKeyIndex = server.getIndices(dbName).getIndices().get(tableSchema.getName()).get(entry.getKey());
        }
      }

      //out.writeInt(SNAPSHOT_SERIALIZATION_VERSION);

      boolean firstResult = true;
      DataUtil.writeVLong(out, keyCount, resultLength);
      for (int i = 0; i < keyCount; i++) {
        int offset = (int) DataUtil.readVLong(in, resultLength);
        Object[] leftKey = null;
        if (singleValue) {
          leftKey = new Object[]{DataUtil.readVLong(in, resultLength)};
        }
        else {
          leftKey = DatabaseCommon.deserializeKey(tableSchema, in);
        }

        Counter[] counters = null;
        GroupByContext groupContext = null;

        List<byte[]> retKeys = new ArrayList<>();
        List<Record> retRecords = new ArrayList<>();

        boolean forceSelectOnServer = false;
        if (indexSchema.isPrimaryKey()) {
          doIndexLookupOneKey(dbName, count, tableSchema, indexSchema, null, false, null, columnOffsets, forceSelectOnServer, null, leftKey, leftKey, leftOperator, index, ascending, retKeys, retRecords, server.getCommon().getSchemaVersion(), false, counters, groupContext);
        }
        else {
          doIndexLookupOneKey(dbName, count, tableSchema, indexSchema, null, false, null, columnOffsets, forceSelectOnServer, null, leftKey, leftKey, leftOperator, index, ascending, retKeys, retRecords, server.getCommon().getSchemaVersion(), true, counters, groupContext);

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


        DataUtil.writeVLong(out, offset, resultLength);
        DataUtil.writeVLong(out, retKeys.size(), resultLength);
        for (byte[] key : retKeys) {
          DataUtil.writeVLong(out, key.length, resultLength);
          out.write(key);
        }
        DataUtil.writeVLong(out, retRecords.size(), resultLength);
        if (retRecords.size() > 0) {
          if (firstResult) {
            byte[] bytes = retRecords.get(0).serialize(server.getCommon());
            DataUtil.writeVLong(out, bytes.length, resultLength);
            out.write(bytes);
            firstResult = false;
          }
        }
        for (int j = 0; j < retRecords.size(); j++) {
          Record record = retRecords.get(j);
          DatabaseCommon.serializeFields(record.getFields(), out, tableSchema, schemaVersion, false);
        }
      }

      if (schemaVersion < server.getSchemaVersion()) {
        throw new SchemaOutOfSyncException("currVer:" + server.getCommon().getSchemaVersion() + ":");
      }

      return bytesOut.toByteArray();
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

  public byte[] indexLookup(String dbName, DataInputStream in) {
    //Timer.Context context = INDEX_LOOKUP_STATS.time();
    try {
      int schemaVersion = in.readInt();
      if (schemaVersion < server.getSchemaVersion()) {
        throw new SchemaOutOfSyncException("currVer:" + server.getCommon().getSchemaVersion() + ":");
      }
      else if (schemaVersion > server.getSchemaVersion()) {
        if (server.getShard() != 0 || server.getReplica() != 0) {
          server.getDatabaseClient().syncSchema();
          schemaVersion = server.getSchemaVersion();
        }
      }

      long serializationVersion = DataUtil.readVLong(in);
      DataUtil.ResultLength resultLength = new DataUtil.ResultLength();
      long preparedId = DataUtil.readVLong(in);
      boolean isPrepared = in.readBoolean();

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
        prepared.count = count = in.readInt();
      }
      boolean isExplicitTrans = in.readBoolean();
      boolean isCommitting = in.readBoolean();
      long transactionId = DataUtil.readVLong(in, resultLength);
      long viewVersion = DataUtil.readVLong(in, resultLength);

      int tableId = 0;
      int indexId = 0;
      boolean forceSelectOnServer = false;
      if (isPrepared) {
        tableId = prepared.tableId;
        indexId = prepared.indexId;
        forceSelectOnServer = prepared.forceSelectOnServer;
      }
      else {
        prepared.tableId = tableId = (int) (long) DataUtil.readVLong(in, resultLength);
        prepared.indexId = indexId = (int) (long) DataUtil.readVLong(in, resultLength);
        prepared.forceSelectOnServer = forceSelectOnServer = in.readBoolean();
      }
      ParameterHandler parms = null;
      if (in.readBoolean()) {
        parms = new ParameterHandler();
        parms.deserialize(in);
      }
      boolean evaluateExpression;
      if (isPrepared) {
        evaluateExpression = prepared.evaluateExpression;
      }
      else {
        prepared.evaluateExpression = evaluateExpression = in.readBoolean();
      }
      Expression expression = null;
      if (isPrepared) {
        expression = prepared.expression;
      }
      else {
        if (in.readBoolean()) {
          prepared.expression = expression = ExpressionImpl.deserializeExpression(in);
        }
      }
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
        //int srcCount = in.readInt();
        int srcCount = (int) DataUtil.readVLong(in, resultLength);
        prepared.orderByExpressions = orderByExpressions = new ArrayList<>();
        for (int i = 0; i < srcCount; i++) {
          OrderByExpressionImpl orderByExpression = new OrderByExpressionImpl();
          orderByExpression.deserialize(in);
          orderByExpressions.add(orderByExpression);
        }
      }
      Object[] leftKey = null;
      if (in.readBoolean()) {
        leftKey = DatabaseCommon.deserializeKey(tableSchema, in);
      }
      Object[] originalLeftKey = null;
      if (in.readBoolean()) {
        originalLeftKey = DatabaseCommon.deserializeKey(tableSchema, in);
      }
      //BinaryExpression.Operator leftOperator = BinaryExpression.Operator.getOperator(in.readInt());
      BinaryExpression.Operator leftOperator = BinaryExpression.Operator.getOperator((int) (long) DataUtil.readVLong(in, resultLength));

      BinaryExpression.Operator rightOperator = null;
      Object[] originalRightKey = null;
      Object[] rightKey = null;
      if (in.readBoolean()) {
        if (in.readBoolean()) {
          rightKey = DatabaseCommon.deserializeKey(tableSchema, in);
        }
        if (in.readBoolean()) {
          originalRightKey = DatabaseCommon.deserializeKey(tableSchema, in);
        }

        //      rightOperator = BinaryExpression.Operator.getOperator(in.readInt());
        rightOperator = BinaryExpression.Operator.getOperator((int) (long) DataUtil.readVLong(in, resultLength));
      }

      Set<Integer> columnOffsets = null;
      if (isPrepared) {
        columnOffsets = prepared.columnOffsets;
      }
      else {
        prepared.columnOffsets = columnOffsets = getSimpleColumnOffsets(in, resultLength, tableName, tableSchema);
      }

      Counter[] counters = null;
      int counterCount = in.readInt();
      if (counterCount > 0) {
        counters = new Counter[counterCount];
        for (int i = 0; i < counterCount; i++) {
          counters[i] = new Counter();
          counters[i].deserialize(in);
        }
      }

      GroupByContext groupContext = null;
      if (in.readBoolean()) {
        groupContext = new GroupByContext();
        groupContext.deserialize(in, server.getCommon(), dbName);
      }

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
      List<Record> retRecords = new ArrayList<>();

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
                retRecords.add(record);
              }
            }
          }
        }
      }


      if (indexSchema.isPrimaryKey()) {
        if (rightOperator == null) {
          entry = doIndexLookupOneKey(dbName, count, tableSchema, indexSchema, parms, evaluateExpression, expression, columnOffsets, forceSelectOnServer, excludeKeys, originalLeftKey, leftKey, leftOperator, index, ascending, retKeys, retRecords, viewVersion, false, counters, groupContext);
        }
        else {
          entry = doIndexLookupTwoKeys(dbName, count, tableSchema, indexSchema, forceSelectOnServer, excludeKeys, originalLeftKey, leftKey, columnOffsets, originalRightKey, rightKey, leftOperator, rightOperator, parms, evaluateExpression, expression, index, ascending, retKeys, retRecords, false, counters, groupContext);
        }
        //todo: support rightOperator
      }
      else {
        if (rightOperator == null) {
          entry = doIndexLookupOneKey(dbName, count, tableSchema, indexSchema, parms, evaluateExpression, expression, columnOffsets, forceSelectOnServer, excludeKeys, originalLeftKey, leftKey, leftOperator, index, ascending, retKeys, retRecords, viewVersion, true, counters, groupContext);
        }
        else {
          entry = doIndexLookupTwoKeys(dbName, count, tableSchema, indexSchema, forceSelectOnServer, excludeKeys, originalLeftKey, leftKey, columnOffsets, originalRightKey, rightKey, leftOperator, rightOperator, parms, evaluateExpression, expression, index, ascending, retKeys, retRecords, true, counters, groupContext);
        }
      }

      //}
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytesOut);
      DataUtil.writeVLong(out, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
      if (entry != null) {
        out.writeBoolean(true);
        out.write(DatabaseCommon.serializeKey(tableSchema, indexName, entry.getKey()));
      }
      else {
        out.writeBoolean(false);
      }
      DataUtil.writeVLong(out, retKeys.size(), resultLength);
      for (byte[] key : retKeys) {
        DataUtil.writeVLong(out, key.length, resultLength);
        out.write(key);
      }
      DataUtil.writeVLong(out, retRecords.size(), resultLength);
      if (retRecords.size() > 0) {
        byte[] bytes = retRecords.get(0).serialize(server.getCommon());
        DataUtil.writeVLong(out, bytes.length, resultLength);
        out.write(bytes);
      }
      for (int i = 1; i < retRecords.size(); i++) {
        Record record = retRecords.get(i);
        DatabaseCommon.serializeFields(record.getFields(), out, tableSchema, schemaVersion, false);
      }

      if (counters == null) {
        out.writeInt(0);
      }
      else {
        out.writeInt(counters.length);
        for (int i = 0; i < counters.length; i++) {
          out.write(counters[i].serialize());
        }
      }

      if (groupContext == null) {
        out.writeBoolean(false);
      }
      else {
        out.writeBoolean(true);
        out.write(groupContext.serialize(server.getCommon()));
      }

      out.close();

//      if (server.getShard() != 0 || server.getReplica() != 0) {
//        server.getDatabaseClient().syncSchema();
//      }

      if (schemaVersion < server.getSchemaVersion()) {
        throw new SchemaOutOfSyncException("currVer:" + server.getCommon().getSchemaVersion() + ":");
      }

      return bytesOut.toByteArray();
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

  public byte[] closeResultSet(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    long resultSetId = Long.valueOf(parts[4]);

    DiskBasedResultSet resultSet = new DiskBasedResultSet(server, resultSetId);
    resultSet.delete();

    return null;
  }

  public byte[] serverSelectDelete(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    long id = Long.valueOf(parts[5]);

    DiskBasedResultSet resultSet = new DiskBasedResultSet(server, id);
    resultSet.delete();
    return null;
  }

  public byte[] serverSelect(String command, byte[] body) {
    try {
      String[] parts = command.split(":");
      String dbName = parts[4];
      int schemaVersion = Integer.valueOf(parts[3]);
      if (schemaVersion < server.getSchemaVersion()) {
        throw new SchemaOutOfSyncException("currVer:" + server.getCommon().getSchemaVersion() + ":");
      }
      int count = Integer.valueOf(parts[5]);

      DataInputStream in = new DataInputStream(new ByteArrayInputStream(body));
      long serializationVersion = DataUtil.readVLong(in);
      in.readBoolean();

      SelectStatementImpl select = new SelectStatementImpl(server.getDatabaseClient());
      select.deserialize(in, dbName);
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
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytesOut);
      DataUtil.writeVLong(out, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
      select.setIsOnServer(false);
      select.serialize(out);

      DataUtil.ResultLength resultLength = new DataUtil.ResultLength();
      if (records == null) {
        DataUtil.writeVLong(out, 0, resultLength);
      }
      else {
        DataUtil.writeVLong(out, records.length, resultLength);
        for (byte[][] tableRecords : records) {
          for (byte[] record : tableRecords) {
            if (record == null) {
              out.writeBoolean(false);
            }
            else {
              out.writeBoolean(true);
              DataUtil.writeVLong(out, record.length, resultLength);
              out.write(record);
            }
          }
        }
      }
      out.close();

      return bytesOut.toByteArray();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public byte[] indexLookupExpression(String command, byte[] body) {
    try {
      String[] parts = command.split(":");
      String dbName = parts[4];
      int schemaVersion = Integer.valueOf(parts[3]);
      if (schemaVersion < server.getSchemaVersion()) {
        throw new SchemaOutOfSyncException("currVer:" + server.getCommon().getSchemaVersion() + ":");
      }
      int count = Integer.valueOf(parts[5]);

      DataInputStream in = new DataInputStream(new ByteArrayInputStream(body));
      long serializationVersion = DataUtil.readVLong(in);
      //    String tableName = in.readUTF();
      //    String indexName = in.readUTF();
      DataUtil.ResultLength resultLength = new DataUtil.ResultLength();
      int tableId = (int) (long) DataUtil.readVLong(in, resultLength);
      //    int indexId = (int) (long) DataUtil.readVLong(in, resultLength);
      ParameterHandler parms = null;
      if (in.readBoolean()) {
        parms = new ParameterHandler();
        parms.deserialize(in);
      }
      Expression expression = null;
      if (in.readBoolean()) {
        expression = ExpressionImpl.deserializeExpression(in);
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
      int srcCount = (int) DataUtil.readVLong(in, resultLength);
      List<OrderByExpressionImpl> orderByExpressions = new ArrayList<>();
      for (int i = 0; i < srcCount; i++) {
        OrderByExpressionImpl orderByExpression = new OrderByExpressionImpl();
        orderByExpression.deserialize(in);
        orderByExpressions.add(orderByExpression);
      }

      TableSchema tableSchema = server.getCommon().getSchema(dbName).getTables().get(tableName);
      IndexSchema indexSchema = tableSchema.getIndices().get(indexName);

      Object[] leftKey = null;
      if (in.readBoolean()) {
        leftKey = DatabaseCommon.deserializeKey(tableSchema, in);
      }

      Set<Integer> columnOffsets = getSimpleColumnOffsets(in, resultLength, tableName, tableSchema);

      Counter[] counters = null;
      int counterCount = in.readInt();
      if (counterCount > 0) {
        counters = new Counter[counterCount];
        for (int i = 0; i < counterCount; i++) {
          counters[i] = new Counter();
          counters[i].deserialize(in);
        }
      }

      GroupByContext groupByContext = null;
      if (in.readBoolean()) {
        groupByContext = new GroupByContext();
        groupByContext.deserialize(in, server.getCommon(), dbName);
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
      List<Record> retRecords = new ArrayList<>();

      if (tableSchema.getIndexes().get(indexName).isPrimaryKey()) {
        entry = doIndexLookupWithRecordsExpression(dbName, count, tableSchema, columnOffsets, parms, expression, index, leftKey,
            ascending, retRecords, counters, groupByContext);
      }
      else {
        //entry = doIndexLookupExpression(count, indexSchema, columnOffsets, index, leftKey, ascending, retKeys);
      }
      //}
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytesOut);
      DataUtil.writeVLong(out, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
      if (entry != null) {
        out.writeBoolean(true);
        out.write(DatabaseCommon.serializeKey(tableSchema, indexName, entry.getKey()));
      }
      else {
        out.writeBoolean(false);
      }
      DataUtil.writeVLong(out, retKeys.size(), resultLength);
      for (byte[] key : retKeys) {
        DataUtil.writeVLong(out, key.length, resultLength);
        out.write(key);
      }
      DataUtil.writeVLong(out, retRecords.size(), resultLength);
      for (Record record : retRecords) {
        byte[] bytes = record.serialize(server.getCommon());
        DataUtil.writeVLong(out, bytes.length, resultLength);
        out.write(bytes);
      }

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
        out.write(groupByContext.serialize(server.getCommon()));
      }

      out.close();

      if (schemaVersion < server.getSchemaVersion()) {
        throw new SchemaOutOfSyncException("currVer:" + server.getCommon().getSchemaVersion() + ":");
      }

      return bytesOut.toByteArray();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  private Map.Entry<Object[], Object> doIndexLookupWithRecordsExpression(
      String dbName, int count, TableSchema tableSchema, Set<Integer> columnOffsets, ParameterHandler parms,
      Expression expression,
      Index index, Object[] leftKey, Boolean ascending, List<Record> ret, Counter[] counters, GroupByContext groupByContext) {

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
    while (entry != null) {
      if (ret.size() >= count) {
        break;
      }
      boolean forceSelectOnServer = false;
      byte[][] records = null;
      synchronized (index.getMutex(entry.getKey())) {
        if (entry.getValue() instanceof Long) {
          entry.setValue(index.get(entry.getKey()));
        }
        if (entry.getValue() != null) {
          records = server.fromUnsafeToRecords(entry.getValue());
        }
      }
      if (parms != null && expression != null) {
        for (byte[] bytes : records) {
          Record record = new Record(tableSchema);
          record.deserialize(dbName, server.getCommon(), bytes, null, true);
          boolean pass = (Boolean) ((ExpressionImpl) expression).evaluateSingleRecord(new TableSchema[]{tableSchema}, new Record[]{record}, parms);
          if (pass) {
            byte[][] currRecords = new byte[][]{bytes};
            Record[] retRecords = applySelectToResultRecords(dbName, columnOffsets, forceSelectOnServer, currRecords, entry.getKey(), tableSchema, counters, groupByContext);
            if (counters == null) {
              ret.add(retRecords[0]);
            }
          }
        }
      }
      else {
        Record[] retRecords = applySelectToResultRecords(dbName, columnOffsets, forceSelectOnServer, records, entry.getKey(), tableSchema, counters, groupByContext);
        if (counters == null) {
          ret.addAll(Arrays.asList(retRecords));
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
      List<Record> retRecords,
      boolean keys,
      Counter[] counters,
      GroupByContext groupContext) {

    BinaryExpression.Operator greaterOp = leftOperator;
    Object[] greaterKey = leftKey;
    Object[] greaterOriginalKey = originalLeftKey;
    BinaryExpression.Operator lessOp = rightOperator;
    Object[] lessKey = leftKey;//rightKey;
    Object[] lessOriginalKey = originalRightKey;
    if (greaterOp == BinaryExpression.Operator.less ||
        greaterOp == BinaryExpression.Operator.lessEqual) {
      greaterOp = rightOperator;
      greaterKey = rightKey;
      greaterOriginalKey = originalRightKey;
      lessOp = leftOperator;
      lessKey = leftKey;
      lessOriginalKey = originalLeftKey;
    }

    Map.Entry<Object[], Object> entry = null;
    if (ascending == null || ascending) {
      if (greaterKey != null) {
        entry = index.floorEntry(greaterKey);
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
          if (entry.getValue() instanceof Long) {
            entry.setValue(index.get(entry.getKey()));
          }
          if (entry.getValue() != null) {
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
            retKeys.add(currKey);
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
                Record[] ret = applySelectToResultRecords(dbName, columnOffsets, forceSelectOnServer, currRecords, entry.getKey(), tableSchema, counters, groupContext);
                if (counters == null) {
                  retRecords.add(ret[0]);
                }
              }
            }
          }
          else {
            if (records.length > 2) {
              logger.error("Records size: " + records.length);
            }

            Record[] ret = applySelectToResultRecords(dbName, columnOffsets, forceSelectOnServer, records, entry.getKey(), tableSchema, counters, groupContext);
            if (counters == null) {
              retRecords.addAll(Arrays.asList(ret));
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

  private Record[] applySelectToResultRecords(String dbName, Set<Integer> columnOffsets, boolean forceSelectOnServer, byte[][] records, Object[] key,
                                              TableSchema tableSchema, Counter[] counters, GroupByContext groupContext) {
    if (columnOffsets == null || columnOffsets.size() == 0) {
      columnOffsets = null;
    }
    Record[] ret = new Record[records.length];
    for (int i = 0; i < records.length; i++) {
      byte[] recordBytes = records[i];

      Record record = new Record(dbName, server.getCommon(), recordBytes, columnOffsets , false);

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

      count(counters, record);

      ret[i] = record;
    }
    return ret;
  }

  private Map.Entry<Object[], Object> doIndexLookupOneKey(
      String dbName,
      int count,
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
      List<Record> retRecords,
      long viewVersion,
      boolean keys,
      Counter[] counters,
      GroupByContext groupContext) {
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
          if (value != null) {
            if (keys) {
              currKeys = server.fromUnsafeToKeys(value);
            }
            else {
              records = server.fromUnsafeToRecords(value);
            }
          }
        }
        if (value != null) {
          handleRecord(dbName, tableSchema, parms, evaluateExpresion, expression, columnOffsets, forceSelectOnServer, retKeys, retRecords, keys, counters, groupContext, records, currKeys);
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
              if (value instanceof Long) {
                value = index.get(entry.getKey());
              }
              if (value != null) {
                if (keys) {
                  currKeys = server.fromUnsafeToKeys(value);
                }
                else {
                  records = server.fromUnsafeToRecords(value);
                }
              }
            }
            handleRecord(dbName, tableSchema, parms, evaluateExpresion, expression, columnOffsets, forceSelectOnServer, retKeys, retRecords, keys, counters, groupContext, records, currKeys);
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
            if (currEntry.getValue() instanceof Long) {
              currEntry.setValue(index.get(currEntry.getKey()));
            }
            if (keys) {
              Object unsafeAddress = currEntry.getValue();//index.get(entry.getKey());
              if (unsafeAddress != null) {
                currKeys = server.fromUnsafeToKeys(unsafeAddress);
              }
            }
            else {
              Object unsafeAddress = currEntry.getValue();//index.get(entry.getKey());
              if (unsafeAddress != null) {
                records = server.fromUnsafeToRecords(unsafeAddress);
              }
            }
          }
          if (keys) {
            if (currKeys != null) {
              for (byte[] currKey : currKeys) {
                retKeys.add(currKey);
              }
            }
          }
          else {
            if (records != null) {
              if (server.getCommon().getTables(dbName).get(tableSchema.getName()).getIndices().get(indexSchema.getName()).getLastPartitions() != null) {
                List<byte[]> remaining = new ArrayList<>();
                for (byte[] bytes : records) {
                  long dbViewNum = Record.getDbViewNumber(bytes);
                  long dbViewFlags = Record.getDbViewFlags(bytes);
                  if (dbViewNum <= viewVersion) {
                    remaining.add(bytes);
                  }
                  else if (//dbViewNum < server.getCommon().getSchema().getVersion()  ||
                      (dbViewFlags & Record.DB_VIEW_FLAG_ADDING) == 0
                      ) {
                    remaining.add(bytes);
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
                    long dbViewNum = Record.getDbViewNumber(bytes);
                    long dbViewFlags = Record.getDbViewFlags(bytes);
                    if (dbViewNum <= viewVersion && (dbViewFlags & Record.DB_VIEW_FLAG_DELETING) == 0) {
                      remaining.add(bytes);
                    }
                    else if (dbViewNum == server.getSchemaVersion() ||
                        (dbViewFlags & Record.DB_VIEW_FLAG_DELETING) == 0) {
                      //        remaining.add(bytes);
                    }
                    else if ((dbViewFlags & Record.DB_VIEW_FLAG_DELETING) != 0) {
                      synchronized (index.getMutex(currEntry.getKey())) {
                        Object unsafeAddress = index.remove(currEntry.getKey());
                        if (unsafeAddress != null) {
                          server.freeUnsafeIds(unsafeAddress);
                        }
                      }
                    }
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
            if (records != null) {
              if (parms != null && expression != null && evaluateExpresion) {
                for (byte[] bytes : records) {
                  Record record = new Record(tableSchema);
                  record.deserialize(dbName, server.getCommon(), bytes, null);
                  boolean pass = (Boolean) ((ExpressionImpl) expression).evaluateSingleRecord(new TableSchema[]{tableSchema}, new Record[]{record}, parms);
                  if (pass) {
                    byte[][] currRecords = new byte[][]{bytes};
                    Record[] ret = applySelectToResultRecords(dbName, columnOffsets, forceSelectOnServer, currRecords, null, tableSchema, counters, groupContext);
                    if (counters == null) {
                      retRecords.add(ret[0]);
                    }
                  }
                }
              }
              else {
                if (records.length > 2) {
                  logger.error("Records size: " + records.length);
                }
                Record[] ret = applySelectToResultRecords(dbName, columnOffsets, forceSelectOnServer, records, entry.getKey(), tableSchema, counters, groupContext);
                if (counters == null) {
                  retRecords.addAll(Arrays.asList(ret));
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
        int diff = Math.max(retRecords.size(), retKeys.size());
        if (count - diff <= 0) {
          break outer;
        }
        entries = new Map.Entry[count - diff];
        if (ascending != null && !ascending) {
          entries = index.lowerEntries((entry.getKey()), entries);
        }
        else {
          entries = index.higherEntries(entry.getKey(), entries);
        }
        if (entries == null) {
          entry = null;
          break outer;
        }
        //}
      }
    }
    return entry;
  }

  private void handleRecord(String dbName, TableSchema tableSchema, ParameterHandler parms, boolean evaluateExpresion, Expression expression, Set<Integer> columnOffsets, boolean forceSelectOnServer, List<byte[]> retKeys, List<Record> retRecords, boolean keys, Counter[] counters, GroupByContext groupContext, byte[][] records, byte[][] currKeys) {
    if (keys) {
      for (byte[] currKey : currKeys) {
        retKeys.add(currKey);
      }
    }
    else {
      if (parms != null && expression != null && evaluateExpresion) {
        for (byte[] bytes : records) {
          Record record = new Record(tableSchema);
          record.deserialize(dbName, server.getCommon(), bytes, null, true);
          boolean pass = (Boolean) ((ExpressionImpl) expression).evaluateSingleRecord(new TableSchema[]{tableSchema}, new Record[]{record}, parms);
          if (pass) {
            byte[][] currRecords = new byte[][]{bytes};
            Record[] ret = applySelectToResultRecords(dbName, columnOffsets, forceSelectOnServer, currRecords, null, tableSchema, counters, groupContext);
            if (counters == null) {
              retRecords.add(ret[0]);
            }
          }
        }
      }
      else {
        Record[] ret = applySelectToResultRecords(dbName, columnOffsets, forceSelectOnServer, records, null, tableSchema, counters, groupContext);
        if (counters == null) {
          retRecords.addAll(Arrays.asList(ret));
        }
      }
    }
  }

  private void count(Counter[] counters, Record record) {
    if (counters != null && record != null) {
      for (Counter counter : counters) {
        counter.add(record.getFields());
      }
    }
  }

  public byte[] evaluateCounter(String command, byte[] body) {

    String[] parts = command.split(":");
    String dbName = parts[4];

    DataInputStream in = new DataInputStream(new ByteArrayInputStream(body));
    Counter counter = new Counter();
    try {
      counter.deserialize(in);

      String tableName = counter.getTableName();
      String columnName = counter.getColumnName();
      String indexName = null;
      TableSchema tableSchema = server.getCommon().getTables(dbName).get(tableName);
      for (IndexSchema indexSchema : tableSchema.getIndexes().values()) {
        if (indexSchema.getFields()[0].equals(columnName)) {
          indexName = indexSchema.getName();
        }
      }
      Index index = server.getIndices().get(dbName).getIndices().get(tableName).get(indexName);
      Map.Entry<Object[], Object> entry = index.lastEntry();
      if (entry != null) {
        byte[][] records = null;
        synchronized (index.getMutex(entry.getKey())) {
          Object unsafeAddress = entry.getValue();
          if (unsafeAddress instanceof Long) {
            unsafeAddress = index.get(entry.getKey());
          }
          if (unsafeAddress != null) {
            records = server.fromUnsafeToRecords(unsafeAddress);
          }
        }
        if (records != null) {
          Record record = new Record(dbName, server.getCommon(), records[0]);
          Object value = record.getFields()[tableSchema.getFieldOffset(columnName)];
          if (counter.isDestTypeLong()) {
            counter.setMaxLong((Long) DataType.getLongConverter().convert(value));
          }
          else {
            counter.setMaxDouble((Double) DataType.getDoubleConverter().convert(value));
          }
        }
      }
      entry = index.firstEntry();
      if (entry != null) {
        byte[][] records = null;
        synchronized (index.getMutex(entry.getKey())) {
          Object unsafeAddress = entry.getValue();
          if (unsafeAddress instanceof Long) {
            unsafeAddress = index.get(entry.getKey());
          }
          if (unsafeAddress != null) {
            records = server.fromUnsafeToRecords(unsafeAddress);
          }
        }
        if (records != null) {
          Record record = new Record(dbName, server.getCommon(), records[0]);
          Object value = record.getFields()[tableSchema.getFieldOffset(columnName)];
          if (counter.isDestTypeLong()) {
            counter.setMinLong((Long) DataType.getLongConverter().convert(value));
          }
          else {
            counter.setMinDouble((Double) DataType.getDoubleConverter().convert(value));
          }
        }
      }
      return counter.serialize();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }
}
