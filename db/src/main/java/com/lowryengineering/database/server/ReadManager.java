package com.lowryengineering.database.server;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.lowryengineering.database.common.DatabaseCommon;
import com.lowryengineering.database.common.Record;
import com.lowryengineering.database.common.SchemaOutOfSyncException;
import com.lowryengineering.database.index.Index;
import com.lowryengineering.database.jdbcdriver.ParameterHandler;
import com.lowryengineering.database.query.BinaryExpression;
import com.lowryengineering.database.query.DatabaseException;
import com.lowryengineering.database.query.Expression;
import com.lowryengineering.database.query.impl.*;
import com.lowryengineering.database.schema.FieldSchema;
import com.lowryengineering.database.schema.IndexSchema;
import com.lowryengineering.database.schema.TableSchema;
import com.lowryengineering.database.util.DataUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

/**
 * Responsible for
 */
public class ReadManager {

  private static Logger logger = LoggerFactory.getLogger(ReadManager.class);

  private final DatabaseServer server;

  public ReadManager(DatabaseServer databaseServer) {

    this.server = databaseServer;
    new java.util.Timer().scheduleAtFixedRate(new TimerTask(){
      @Override
      public void run() {
        logger.info("IndexLookup stats: count=" + INDEX_LOOKUP_STATS.getCount() + ", rate=" + INDEX_LOOKUP_STATS.getFiveMinuteRate() +
            ", durationAvg=" + INDEX_LOOKUP_STATS.getSnapshot().getMean() / 1000000d +
            ", duration99.9=" + INDEX_LOOKUP_STATS.getSnapshot().get999thPercentile()  / 1000000d);
      }
    }, 20 * 1000, 20 * 1000);
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
        Map.Entry<Object[], Long> entry = index.firstEntry();
        while (true) {
          if (entry == null) {
            break;
          }
          byte[][] records = server.fromUnsafeToRecords(entry.getValue());
          for (byte[] bytes : records) {
            Record record = new Record(tableSchema);
            record.deserialize(dbName, server.getCommon(), bytes);
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
        List<byte[]> retRecords = new ArrayList<>();
        if (indexSchema.isPrimaryKey()) {
          doIndexLookupOneKey(dbName, count, tableSchema, indexSchema, null, false, null, columnOffsets, null, leftKey, leftOperator, index, ascending, retRecords, server.getCommon().getSchemaVersion(), false, counters, groupContext);
        }
        else {
          doIndexLookupOneKey(dbName, count, tableSchema, indexSchema, null, false, null, columnOffsets, null, leftKey, leftOperator, index, ascending, retKeys, server.getCommon().getSchemaVersion(), true, counters, groupContext);

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
        for (byte[] record : retRecords) {
          DataUtil.writeVLong(out, record.length, resultLength);
          out.write(record);
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

  public byte[] indexLookup(String command, byte[] body) {
    Timer.Context context = INDEX_LOOKUP_STATS.time();
    try {
      String[] parts = command.split(":");
      String dbName = parts[4];
      int schemaVersion = Integer.valueOf(parts[3]);
      if (schemaVersion < server.getSchemaVersion()) {
        throw new SchemaOutOfSyncException("currVer:" + server.getCommon().getSchemaVersion() + ":");
      }
      else if (schemaVersion > server.getSchemaVersion()) {
        if (server.getShard() != 0 || server.getReplica() != 0) {
          server.getDatabaseClient().syncSchema();
          schemaVersion = server.getSchemaVersion();
        }
      }

      DataInputStream in = new DataInputStream(new ByteArrayInputStream(body));
      long serializationVersion = DataUtil.readVLong(in);
      DataUtil.ResultLength resultLength = new DataUtil.ResultLength();

      int count = in.readInt();
      boolean isExplicitTrans = in.readBoolean();
      boolean isCommitting = in.readBoolean();
      long transactionId = DataUtil.readVLong(in, resultLength);
      long viewVersion = DataUtil.readVLong(in, resultLength);

      int tableId = (int) (long) DataUtil.readVLong(in, resultLength);
      int indexId = (int) (long) DataUtil.readVLong(in, resultLength);
      ParameterHandler parms = null;
      if (in.readBoolean()) {
        parms = new ParameterHandler();
        parms.deserialize(in);
      }
      boolean evaluateExpression = in.readBoolean();
      Expression expression = null;
      if (in.readBoolean()) {
        expression = ExpressionImpl.deserializeExpression(in);
      }
      String tableName = null;
      String indexName = null;
      try {
        //  logger.info("indexLookup: tableid=" + tableId + ", tableCount=" + common.getTablesById().size() + ", tableNull=" + (common.getTablesById().get(tableId) == null));
        Map<Integer, TableSchema> tablesById = server.getCommon().getTablesById(dbName);
        if (tablesById == null) {
          logger.error("Error");
        }
        TableSchema table = tablesById.get(tableId);
        if (table == null) {
          logger.error("Error");
        }
          tableName = table.getName();
        indexName = server.getCommon().getTables(dbName).get(tableName).getIndexesById().get(indexId).getName();
      }
      catch (Exception e) {
        logger.info("indexLookup: tableName=" + tableName + ", tableid=" + tableId + ", tableByNameCount=" + server.getCommon().getTables(dbName).size() + ", tableCount=" + server.getCommon().getTablesById(dbName).size() +
            ", tableNull=" + (server.getCommon().getTablesById(dbName).get(tableId) == null) + ", indexName=" + indexName + ", indexId=" + indexId +
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
      //BinaryExpression.Operator leftOperator = BinaryExpression.Operator.getOperator(in.readInt());
      BinaryExpression.Operator leftOperator = BinaryExpression.Operator.getOperator((int) (long) DataUtil.readVLong(in, resultLength));

      BinaryExpression.Operator rightOperator = null;
      Object[] rightKey = null;
      if (in.readBoolean()) {
        if (in.readBoolean()) {
          rightKey = DatabaseCommon.deserializeKey(tableSchema, in);
        }
        //      rightOperator = BinaryExpression.Operator.getOperator(in.readInt());
        rightOperator = BinaryExpression.Operator.getOperator((int) (long) DataUtil.readVLong(in, resultLength));
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

      GroupByContext groupContext = null;
      if (in.readBoolean()) {
        groupContext = new GroupByContext();
        groupContext.deserialize(in, server.getCommon(), dbName);
      }

      Index index = server.getIndices(dbName).getIndices().get(tableSchema.getName()).get(indexName);
      Map.Entry<Object[], Long> entry = null;

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


      if (tableSchema.getIndexes().get(indexName).isPrimaryKey()) {
        if (rightOperator == null) {
            entry = doIndexLookupOneKey(dbName, count, tableSchema, indexSchema, parms, evaluateExpression, expression, columnOffsets, excludeKeys, leftKey, leftOperator, index, ascending, retRecords, viewVersion, false, counters, groupContext);
        }
        else {
          entry = doIndexLookupTwoKeys(dbName, count, tableSchema, indexSchema, excludeKeys, leftKey, columnOffsets, rightKey, leftOperator, rightOperator, parms, evaluateExpression, expression, index, ascending, retRecords, false, counters, groupContext);
        }
        //todo: support rightOperator
      }
      else {
        if (rightOperator == null) {
          entry = doIndexLookupOneKey(dbName, count, tableSchema, indexSchema, parms, evaluateExpression, expression, columnOffsets, excludeKeys, leftKey, leftOperator, index, ascending, retKeys, viewVersion, true, counters, groupContext);
        }
        else {
          entry = doIndexLookupTwoKeys(dbName, count, tableSchema, indexSchema, excludeKeys, leftKey, columnOffsets, rightKey, leftOperator, rightOperator, parms, evaluateExpression, expression, index, ascending, retKeys, true, counters, groupContext);
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
      for (byte[] record : retRecords) {
        DataUtil.writeVLong(out, record.length, resultLength);
        out.write(record);
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
      context.stop();
    }
  }

  private Set<Integer> getSimpleColumnOffsets(DataInputStream in, DataUtil.ResultLength resultLength, String tableName, TableSchema tableSchema) throws IOException {
    int count = (int)DataUtil.readVLong(in, resultLength);
    Set<Integer> columnOffsets = new HashSet<>();
    for (int i = 0; i < count; i++) {
      columnOffsets.add((int)DataUtil.readVLong(in, resultLength));
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
        ResultSetImpl resultSet = (ResultSetImpl) select.execute(dbName);
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
    Map.Entry<Object[], Long> entry = null;

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
    for (byte[] record : retRecords) {
      DataUtil.writeVLong(out, record.length, resultLength);
      out.write(record);
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

  private Map.Entry<Object[], Long> doIndexLookupWithRecordsExpression(
      String dbName, int count, TableSchema tableSchema, Set<Integer> columnOffsets, ParameterHandler parms,
      Expression expression,
      Index index, Object[] leftKey, Boolean ascending, List<byte[]> ret, Counter[] counters, GroupByContext groupByContext) {

    Map.Entry<Object[], Long> entry;
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
      byte[][] records = server.fromUnsafeToRecords(entry.getValue());
      if (parms != null && expression != null) {
        for (byte[] bytes : records) {
          Record record = new Record(tableSchema);
          record.deserialize(dbName, server.getCommon(), bytes);
          boolean pass = (Boolean) ((ExpressionImpl) expression).evaluateSingleRecord(new TableSchema[]{tableSchema}, new Record[]{record}, parms);
          if (pass) {
            byte[][] currRecords = new byte[][]{bytes};
            applySelectToResultRecords(dbName, columnOffsets, currRecords, entry.getKey(), tableSchema, counters, groupByContext);
            bytes = currRecords[0];
            ret.add(bytes);
          }
        }
      }
      else {
        applySelectToResultRecords(dbName, columnOffsets, records, entry.getKey(), tableSchema, counters, groupByContext);

        ret.addAll(Arrays.asList(records));
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


  private Map.Entry<Object[], Long> doIndexLookupTwoKeys(
      String dbName,
      int count,
      TableSchema tableSchema,
      IndexSchema indexSchema,
      List<Object[]> excludeKeys,
      Object[] leftKey,
      Set<Integer> columnOffsets,
      Object[] rightKey,
      BinaryExpression.Operator leftOperator,
      BinaryExpression.Operator rightOperator,
      ParameterHandler parms,
      boolean evaluateExpression,
      Expression expression,
      Index index,
      Boolean ascending,
      List<byte[]> ret,
      boolean keys,
      Counter[] counters,
      GroupByContext groupContext) {

    BinaryExpression.Operator greaterOp = leftOperator;
    Object[] greaterKey = leftKey;
    BinaryExpression.Operator lessOp = rightOperator;
    Object[] lessKey = rightKey;
    if (greaterOp == BinaryExpression.Operator.less ||
        greaterOp == BinaryExpression.Operator.lessEqual) {
      greaterOp = rightOperator;
      greaterKey = rightKey;
      lessOp = leftOperator;
      lessKey = leftKey;
    }

    Map.Entry<Object[], Long> entry;
    if (ascending == null || ascending) {
      if (greaterKey == null) {
        entry = index.firstEntry();
      }
      else {
        entry = index.floorEntry(greaterKey);
        if (entry == null) {
          entry = index.firstEntry();
        }
      }
    }
    else {
      if (ascending != null && !ascending && lessKey == null) {
        entry = index.lastEntry();
      }
      else {
        entry = index.floorEntry(lessKey);
        if (entry == null) {
          entry = index.firstEntry();
        }
      }
    }
    if (entry != null) {
      Object[] key = lessKey;
      //      rightKey = greaterKey;
      //      if (ascending == null || ascending) {
      key = greaterKey;
      rightKey = lessKey;
      //}
      if ((ascending != null && !ascending)) {
        if (lessOp.equals(BinaryExpression.Operator.less)) {
          boolean foundMatch = key != null && 0 == server.getCommon().compareKey(indexSchema.getComparators(), entry.getKey(), lessKey);
          if (foundMatch) {
            entry = index.lowerEntry((entry.getKey()));
          }
        }
      }
      else {
        if (greaterOp.equals(BinaryExpression.Operator.greater)) {
          boolean foundMatch = key != null && 0 == server.getCommon().compareKey(indexSchema.getComparators(), entry.getKey(), greaterKey);
          if (foundMatch) {
            entry = index.higherEntry((entry.getKey()));
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
        if (ret.size() >= count) {
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
          if (rightKey != null) {
            compareRight = server.getCommon().compareKey(indexSchema.getComparators(), entry.getKey(), rightKey);
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
        if (keys) {
          byte[][] currKeys = server.fromUnsafeToKeys(entry.getValue());
          for (byte[] currKey : currKeys) {
            ret.add(currKey);
          }
        }
        else {
          byte[][] records = server.fromUnsafeToRecords(entry.getValue());
          if (parms != null && expression != null && evaluateExpression) {
            for (byte[] bytes : records) {
              Record record = new Record(tableSchema);
              record.deserialize(dbName, server.getCommon(), bytes);
              boolean pass = (Boolean) ((ExpressionImpl) expression).evaluateSingleRecord(new TableSchema[]{tableSchema}, new Record[]{record}, parms);
              if (pass) {
                byte[][] currRecords = new byte[][]{bytes};
                applySelectToResultRecords(dbName, columnOffsets, currRecords, entry.getKey(), tableSchema, counters, groupContext);
                bytes = currRecords[0];
                ret.add(bytes);
              }
            }
          }
          else {
            if (records.length > 2) {
              logger.error("Records size: " + records.length);
            }

            applySelectToResultRecords(dbName, columnOffsets, records, entry.getKey(), tableSchema, counters, groupContext);

            ret.addAll(Arrays.asList(records));
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
          if (lessKey == null) {
            throw new DatabaseException("less key is null");
          }
          int compareValue = server.getCommon().compareKey(indexSchema.getComparators(), entry.getKey(), lessKey);
          if ((0 == compareValue || 1 == compareValue) && lessOp == BinaryExpression.Operator.less) {
            entry = null;
            break;
          }
          if (1 == compareValue) {
            entry = null;
            break;
          }
          compareValue = 1;
          if (greaterKey != null) {
            compareValue = server.getCommon().compareKey(indexSchema.getComparators(), entry.getKey(), greaterKey);
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

  private void applySelectToResultRecords(String dbName, Set<Integer> columnOffsets, byte[][] records, Object[] key,
                                          TableSchema tableSchema, Counter[] counters, GroupByContext groupContext) {
    if (columnOffsets.size() == 0) {
      return;
    }
    for (int i = 0; i < records.length; i++) {
      byte[] recordBytes = records[i];

      if (groupContext == null && counters == null && recordBytes.length < 1024) {
        continue;
      }
      Record record = new Record(dbName, server.getCommon(), recordBytes);

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

//      if (key != null && (long)record.getFields()[tableSchema.getFieldOffset("id")] != (long)key[0]) {
      count(counters, record);

//        logger.error("########################################################################################### record mismatch");
//      }
      for (int j = 0; j < record.getFields().length; j++) {
        if (!columnOffsets.contains(j)) {
          record.getFields()[j] = null;
        }
      }
      records[i] = record.serialize(server.getCommon());
    }
  }

  private Map.Entry<Object[], Long> doIndexLookupOneKey(
      String dbName,
      int count,
      TableSchema tableSchema,
      IndexSchema indexSchema,
      ParameterHandler parms,
      boolean evaluateExpresion,
      Expression expression,
      Set<Integer> columnOffsets,
      List<Object[]> excludeKeys,
      Object[] key,
      BinaryExpression.Operator operator,
      Index index,
      Boolean ascending,
      List<byte[]> ret, long viewVersion,
      boolean keys,
      Counter[] counters,
      GroupByContext groupContext) {
    Map.Entry<Object[], Long> entry;

    if (operator.equals(BinaryExpression.Operator.equal)) {
      if (key == null) {
        return null;
      }
      Long value = index.get(key);
      if (value == null) {
        return null;
      }
      if (excludeKeys != null) {
        for (Object[] excludeKey : excludeKeys) {
          if (server.getCommon().compareKey(indexSchema.getComparators(), excludeKey, key) == 0) {
            return null;
          }
        }
      }
      if (keys) {
        byte[][] currKeys = server.fromUnsafeToKeys(value);
        for (byte[] currKey : currKeys) {
          ret.add(currKey);
        }
      }
      else {
        byte[][] records = server.fromUnsafeToRecords(value);
        if (parms != null && expression != null && evaluateExpresion) {
          for (byte[] bytes : records) {
            Record record = new Record(tableSchema);
            record.deserialize(dbName, server.getCommon(), bytes);
            boolean pass = (Boolean) ((ExpressionImpl) expression).evaluateSingleRecord(new TableSchema[]{tableSchema}, new Record[]{record}, parms);
            if (pass) {
              byte[][] currRecords = new byte[][]{bytes};
              applySelectToResultRecords(dbName, columnOffsets, currRecords, null, tableSchema, counters, groupContext);
              if (counters == null) {
                bytes = currRecords[0];
                ret.add(bytes);
              }
            }
          }
        }
        else {
          applySelectToResultRecords(dbName, columnOffsets, records, null, tableSchema, counters, groupContext);
          if (counters == null) {
            ret.addAll(Arrays.asList(records));
          }
        }
      }
      entry = null;
    }
    else if ((ascending == null || ascending) /*&&
        (operator.equals(BinaryExpression.Operator.less) ||
            operator.equals(BinaryExpression.Operator.lessEqual))*/) {
      if (key == null) {
        entry = index.firstEntry();
      }
      else {
        //entry = index.floorEntry(key);
        if (operator.equals(BinaryExpression.Operator.greater) ||
             operator.equals(BinaryExpression.Operator.greaterEqual)) {
           entry = index.floorEntry(key);
          if (entry == null) {
            entry = index.firstEntry();
          }
         }
         else {
           entry = index.firstEntry();
         }
      }
    }
    else {
      //        if (key[0] == indexSchema.getCurrPartitions()[shard].getUpperKey()) {
      //
      //        }


      if (ascending != null && !ascending &&
          (key == null || operator.equals(BinaryExpression.Operator.greater) ||
              operator.equals(BinaryExpression.Operator.greaterEqual))) {
        entry = index.lastEntry();
      }
      else {
        if (key == null) {
          entry = index.firstEntry();
        }
        else {
          entry = index.ceilingEntry(key);
          if (entry == null) {
            entry = index.lastEntry();
          }
        }

      }
    }
    if (entry != null) {
      if ((ascending != null && !ascending)) {
        if (operator.equals(BinaryExpression.Operator.less)) {
          boolean foundMatch = key != null && 0 == server.getCommon().compareKey(indexSchema.getComparators(), entry.getKey(), key);
          if (foundMatch) {
            //todo: match below
            entry = index.lowerEntry((entry.getKey()));
          }
        }
      }
      else {
        if (operator.equals(BinaryExpression.Operator.greater)) {
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

      outer:
      while (entry != null) {
        if (ret.size() >= count) {
          break;
        }
        if (key != null) {
          int compare = server.getCommon().compareKey(indexSchema.getComparators(), entry.getKey(), key);
          if (compare == 0 &&
              (operator.equals(BinaryExpression.Operator.less) || operator.equals(BinaryExpression.Operator.greater))) {
            entry = null;
            break;
          }
          if (compare == 1 && (ascending == null || ascending) && operator.equals(BinaryExpression.Operator.lessEqual)) {
            entry = null;
            break;
          }
          if (compare == 1 && (ascending == null || ascending) && operator.equals(BinaryExpression.Operator.less)) {
            entry = null;
            break;
          }
          if (compare == -1 && (ascending != null && !ascending) && operator.equals(BinaryExpression.Operator.greaterEqual)) {
            entry = null;
            break;
          }
          if (compare == -1 && (ascending != null && !ascending) && operator.equals(BinaryExpression.Operator.greater)) {
            entry = null;
            break;
          }
        }

        if (excludeKeys != null) {
          for (Object[] excludeKey : excludeKeys) {
            if (server.getCommon().compareKey(indexSchema.getComparators(), excludeKey, key) == 0) {
              continue outer;
            }
          }
        }

        if (keys) {
          byte[][] currKeys = null;
          synchronized (index) {
            Long unsafeAddress = index.get(entry.getKey());
            if (unsafeAddress != null) {
              currKeys = server.fromUnsafeToKeys(unsafeAddress);
            }
          }
          if (currKeys != null) {
            for (byte[] currKey : currKeys) {
              ret.add(currKey);
            }
          }
        }
        else {
          byte[][] records = null;
          synchronized (index) {
            Long unsafeAddress = index.get(entry.getKey());
            if (unsafeAddress != null) {
              records = server.fromUnsafeToRecords(unsafeAddress);
            }
          }
          if (records != null) {
            if (server.getCommon().getTables(dbName).get(tableSchema.getName()).getIndices().get(indexSchema.getName()).getLastPartitions() != null) {
              List<byte[]> remaining = new ArrayList<>();
              for (byte[] bytes : records) {
                long dbViewNum = Record.getDbViewNumber(bytes);
                long dbViewFlags = Record.getDbViewFlags(bytes);
                if (dbViewNum <= viewVersion) {
                  remaining.add(bytes);
                }
                else
                  if (//dbViewNum < server.getCommon().getSchema().getVersion()  ||
                      (dbViewFlags & Record.DB_VIEW_FLAG_ADDING) == 0
                      )
                  {
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
                    synchronized (index) {
                      Long unsafeAddress = index.remove(entry.getKey());
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
                record.deserialize(dbName, server.getCommon(), bytes);
                boolean pass = (Boolean) ((ExpressionImpl) expression).evaluateSingleRecord(new TableSchema[]{tableSchema}, new Record[]{record}, parms);
                if (pass) {
                  byte[][] currRecords = new byte[][]{bytes};
                  applySelectToResultRecords(dbName, columnOffsets, currRecords, null, tableSchema, counters, groupContext);
                  if (counters == null) {
                    bytes = currRecords[0];
                    ret.add(bytes);
                  }
                }
              }
            }
            else {
              if (records.length > 2) {
                logger.error("Records size: " + records.length);
              }
              applySelectToResultRecords(dbName, columnOffsets, records, entry.getKey(), tableSchema, counters, groupContext);
              if (counters == null) {
                ret.addAll(Arrays.asList(records));
              }
            }
          }
        }
        //entry = null;
        if (operator.equals(BinaryExpression.Operator.equal)) {
          entry = null;
          break;
        }
//        if ((ascending == null || ascending) &&
//            (operator.equals(BinaryExpression.Operator.less) ||
//                operator.equals(BinaryExpression.Operator.lessEqual))) {
//          entry = index.lowerEntry((entry.getKey()));
//        }
//        else {
          if (ascending != null && !ascending) {
            entry = index.lowerEntry((entry.getKey()));
          }
          else {
            entry = index.higherEntry((entry.getKey()));
          }
        //}
      }
    }
    return entry;
  }

  private void count(Counter[] counters, Record record) {
    if (counters != null && record != null) {
      for (Counter counter : counters) {
        counter.add(record.getFields());
      }
    }
  }

}
