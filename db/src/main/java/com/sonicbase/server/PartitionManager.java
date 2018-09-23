package com.sonicbase.server;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.*;
import com.sonicbase.index.Index;
import com.sonicbase.index.Indices;
import com.sonicbase.query.BinaryExpression;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.DataType;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.Schema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.PartitionUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@SuppressWarnings({"squid:S1172", "squid:S1168", "squid:S00107"})
// all methods called from method invoker must have cobj and replayed command parms
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class PartitionManager extends Thread {

  private static final String INDEX_STR = ", index=";
  private static final String ERROR_MOVING_ENTRIES_STR = "Error moving entries";
  private static final String DO_PROCESS_ENTRIES_TABLE_INDEX_COUNT_STR = "doProcessEntries: table={}, index={}, count={}";
  private static final String DO_PROCESS_ENTRIES_FINISHED_TABLE_INDEX_COUNT_DURATION_STR =
      "doProcessEntries - finished: table={}, index={}, count={}, duration={}";
  private static final Logger logger = LoggerFactory.getLogger(PartitionManager.class);

  private final DatabaseServer databaseServer;
  private final DatabaseCommon common;
  private final Map<String, Indices> indices;
  private final Map<Integer, ShardState> stateIsShardRepartitioningComplete = new ConcurrentHashMap<>();
  private String stateTable = "none";
  private String stateIndex = "none";
  private RepartitionerState state = RepartitionerState.IDLE;
  private Exception shardRepartitionException;
  private static final ConcurrentHashMap<String, List<PartitionEntry>> previousPartitions = new ConcurrentHashMap<>();
  private boolean isShardRepartitioningComplete = true;
  private final AtomicLong countMoved = new AtomicLong();
  private final AtomicLong countDeleted = new AtomicLong();
  private boolean isRunning = false;
  final AtomicBoolean isRebalancing = new AtomicBoolean();
  private Integer batchOverride = null;
  private final AtomicBoolean isRepartitioningIndex = new AtomicBoolean();
  private boolean shutdown;
  private final AtomicBoolean isComplete = new AtomicBoolean(true);
  private AtomicLong beginMove = new AtomicLong();
  private AtomicLong moveDuration = new AtomicLong();
  private AtomicLong beginDelete = new AtomicLong();
  private AtomicLong deleteDuration = new AtomicLong();
  private Thread rebalanceThread;
  private boolean stopRepartitioning;

  public enum RepartitionerState {
    IDLE,
    PREP,
    REBALANCING,
    COMPLETE,
  }

  static class ShardState {
    private long count;
    private String exception;
    private boolean finished;

    ShardState(long count, String exception, boolean finished) {
      this.count = count;
      this.exception = exception;
      this.finished = finished;
    }

    ShardState() {
    }
  }

  PartitionManager(DatabaseServer databaseServer, DatabaseCommon common) {
    super("PartitionManager Thread");
    this.databaseServer = databaseServer;
    this.common = common;
    this.indices = databaseServer.getIndices();
  }

  public void setBatchOverride(Integer millis) {
    this.batchOverride = millis;
  }


  public static Map<String, List<PartitionEntry>> getPreviousPartitions() {
    return previousPartitions;
  }

  public ComObject getRepartitionerState(ComObject cobj, boolean replayedCommand) {
    logger.info("getRepartitionerState - begin: state={}, table={}, index={}", state.name(), stateTable, stateIndex);
    ComObject retObj = new ComObject();
    retObj.put(ComObject.Tag.STATE, state.name());
    if (state == RepartitionerState.REBALANCING) {
      retObj.put(ComObject.Tag.TABLE_NAME, stateTable);
      retObj.put(ComObject.Tag.INDEX_NAME, stateIndex);
      ComArray array = retObj.putArray(ComObject.Tag.SHARDS, ComObject.Type.OBJECT_TYPE);
      for (Map.Entry<Integer, ShardState> entry : stateIsShardRepartitioningComplete.entrySet()) {
        ComObject innerObj = new ComObject();
        innerObj.put(ComObject.Tag.SHARD, entry.getKey());
        innerObj.put(ComObject.Tag.COUNT_LONG, entry.getValue().count);
        innerObj.put(ComObject.Tag.FINISHED, entry.getValue().finished);
        if (entry.getValue().exception != null) {
          innerObj.put(ComObject.Tag.EXCEPTION, entry.getValue().exception);
        }
        array.getArray().add(innerObj);
      }
    }
    return retObj;
  }

  public byte[] beginRebalance(final String dbName, final List<String> toRebalance) {

    ThreadPoolExecutor executor = ThreadUtil.createExecutor(databaseServer.getShardCount(),
        "SonicBase beginRebalance Thread");
    try {
      while (!isComplete.compareAndSet(true, false)) {
        try {
          Thread.sleep(1000);
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new DatabaseException(e);
        }
      }

      state = RepartitionerState.PREP;

      logger.info("rebalance cycle - begin");

      long totalBegin = System.currentTimeMillis();

      try {
        String tableName = null;
        StringBuilder toRebalanceStr = new StringBuilder();
        for (String index : toRebalance) {
          toRebalanceStr.append(index).append(", ");
        }
        logger.info("master - Rebalancing index group: group={}", toRebalanceStr);

        long begin = System.currentTimeMillis();

        //rebalance with current partitions before resharding
        doRebalance(dbName, toRebalance, executor, totalBegin, tableName, begin);

        Map<String, ComArray[]> partitionSizes = new HashMap<>();
        for (String index : toRebalance) {

          String[] parts = index.split(" ");
          final String currTableName = parts[0];
          final String indexName = parts[1];
          final ComArray[] currPartitionSizes = new ComArray[databaseServer.getShardCount()];
          List<Future> futures = new ArrayList<>();
          int shardCount =  databaseServer.getShardCount();
          for (int i = 0; i < shardCount; i++) {
            final int offset = i;
            futures.add(executor.submit((Callable) () -> {
              currPartitionSizes[offset] = getPartitionSize(dbName, offset, currTableName, indexName);
              return null;
            }));
          }
          for (Future future : futures) {
            future.get();
          }
          logger.info("master - getPartitionSize finished: db={}, table={}, index={}, duration={}sec", dbName, tableName, indexName,
              (System.currentTimeMillis() - begin) / 1000f);
          partitionSizes.put(index, currPartitionSizes);
        }

        Map<String, List<TableSchema.Partition>> copiedPartitionsToApply = new HashMap<>();
        Map<String, List<TableSchema.Partition>> newPartitionsToApply = new HashMap<>();

        PrepareToReShardPartitions prepareToReshardPartitions = new PrepareToReShardPartitions(dbName, toRebalance,
            tableName, begin, partitionSizes, copiedPartitionsToApply, newPartitionsToApply).invoke();
        tableName = prepareToReshardPartitions.getTableName();

        begin = prepareToReshardPartitions.getBegin();

        reshardPartitions(dbName, toRebalance, copiedPartitionsToApply, newPartitionsToApply);

        isRepartitioningIndex.set(true);

        doRebalance(dbName, toRebalance, executor, totalBegin, tableName, begin);

        common.saveSchema(databaseServer.getDataDir());
        logger.info("master - Post-save schemaVersion={}, shard={}, replica={}", common.getSchemaVersion(),
            common.getShard(), common.getReplica());
        databaseServer.pushSchema();

        isRepartitioningIndex.set(false);
      }
      catch (Exception e) {
        logger.error("Error repartitioning", e);
        throw new DatabaseException(e);
      }
      finally {
        logger.info("rebalance cycle - finished: duration={}sec", (System.currentTimeMillis() - totalBegin)/1000f);
        isComplete.set(true);
        state = RepartitionerState.COMPLETE;
      }
    }
    finally {
      executor.shutdownNow();
    }
    return null;
  }

  private void doRebalance(final String dbName, List<String> toRebalance, ThreadPoolExecutor executor,
                           long totalBegin, String tableName, long begin) {
    for (String index : toRebalance) {
      try {
        Timings timings = new Timings();
        timings.begin = System.currentTimeMillis();

        String[] parts = index.split(" ");
        tableName = parts[0];
        final String finalTableName = tableName;
        final String indexName = parts[1];

        this.stateTable = tableName;
        this.stateIndex = indexName;
        state = RepartitionerState.REBALANCING;

        begin = System.currentTimeMillis();

        for (int i = 0; i < databaseServer.getShardCount(); i++) {
          stateIsShardRepartitioningComplete.put(i, new ShardState());
        }

        final int[] masters = new int[databaseServer.getShardCount()];
        logger.info("master - rebalance ordered index - begin: table={}, index={}", tableName, indexName);
        List<Future> futures = new ArrayList<>();

        startRebalanceOnShards(dbName, executor, finalTableName, indexName, masters, futures);

        for (Future future : futures) {
          future.get();
        }

        Map<Integer, Integer> countFailed = new HashMap<>();
        while (true) {
          boolean areAllComplete = checkIfRebalanceCompleteForEachShard(dbName, masters, countFailed, finalTableName,
              indexName, timings);
          if (areAllComplete) {
            break;
          }
          Thread.sleep(500);
        }

        common.getTables(dbName).get(tableName).getIndices().get(indexName).deleteLastPartitions();

        logger.info("master - rebalance ordered index - finished: db={}, table={}, index={}, duration={}sec, " +
            "moveMin={}({}), moveMinCount={}({}), moveMax={}({}), moveMaxCount={}({}), moveAvg={}, " +
            "moveCountAvg={}, moveCountTotal={}, deleteMin={}({}), deleteMinCount={}({}), deleteMax={}({}), deleteMaxCount={}({}), " +
            "deleteAvg={}, deleteCountAvg={}, deleteCountTotal={}", dbName, tableName,
            indexName, (System.currentTimeMillis() - begin) / 1000d, timings.moveMin, timings.moveMinShard,
            timings.moveMinCount, timings.moveMinCountShard, timings.moveMax, timings.moveMaxShard,
            timings.moveMaxCount, timings.moveMaxCountShard,
            timings.moveTotal / (double)databaseServer.getShardCount(),
            timings.moveTotalCount / (double)databaseServer.getShardCount(),
            timings.moveTotalCount,
            timings.deleteMin, timings.deleteMinShard,
            timings.deleteMinCount, timings.deleteMinCountShard,
            timings.deleteMax, timings.deleteMaxShard,
            timings.deleteMaxCount, timings.deleteMaxCountShard,
            timings.deleteTotal / (double)databaseServer.getShardCount(),
            timings.deleteTotalCount / (double)databaseServer.getShardCount(),
            timings.deleteTotalCount);
      }
      catch (Exception e) {
        logger.error("error rebalancing index: dbName={}, table={}, index={}, duration={}sec", dbName, tableName, index,
            (System.currentTimeMillis() - begin) / 1000d, e);
      }
    }
  }

  class Timings {
    private long moveTotalCount;
    private long deleteTotalCount;
    private long moveMinCount;
    private long moveMaxCount;
    private long deleteMinCount;
    private long deleteMaxCount;
    private int moveMinCountShard;
    private int moveMaxCountShard;
    private int deleteMinCountShard;
    private int deleteMaxCountShard;
    private int moveMinShard;
    private int moveMaxShard;
    private int deleteMinShard;
    private int deleteMaxShard;
    private long begin;
    private long duration;
    private int shardCount;
    private long moveMin;
    private long moveMax;
    private long moveTotal;
    private long deleteMin;
    private long deleteMax;
    private long deleteTotal;
  }

  private boolean checkIfRebalanceCompleteForEachShard(String dbName, int[] masters, Map<Integer, Integer> countFailed,
                                                       String tableName, String indexName, Timings timings) {
    Timings localTimings = new Timings();
    localTimings.moveMin = Long.MAX_VALUE;
    localTimings.deleteMin = Long.MAX_VALUE;
    localTimings.moveMinShard = -1;
    localTimings.moveMaxShard = -1;
    localTimings.deleteMinShard = -1;
    localTimings.deleteMaxShard = -1;
    localTimings.moveMinCount = Long.MAX_VALUE;
    localTimings.deleteMinCount = Long.MAX_VALUE;
    localTimings.moveMinCountShard = -1;
    localTimings.moveMaxCountShard = -1;
    localTimings.deleteMinCountShard = -1;
    localTimings.deleteMaxCountShard = -1;

    boolean areAllComplete = true;
    StringBuilder waitingFor = new StringBuilder();
    for (int shard = 0; shard < databaseServer.getShardCount(); shard++) {
      try {
        ComObject cobj = new ComObject();
        cobj.put(ComObject.Tag.DB_NAME, "__none__");
        cobj.put(ComObject.Tag.SCHEMA_VERSION, databaseServer.getCommon().getSchemaVersion());
        byte[] bytes = databaseServer.getClient().send("PartitionManager:isShardRepartitioningComplete", shard, masters[shard], cobj, DatabaseClient.Replica.SPECIFIED);
        ComObject retObj = new ComObject(bytes);
        long count = retObj.getLong(ComObject.Tag.COUNT_LONG);
        String exception = retObj.getString(ComObject.Tag.EXCEPTION);
        boolean finished = retObj.getBoolean(ComObject.Tag.FINISHED);
        stateIsShardRepartitioningComplete.put(shard, new ShardState(count, exception, finished));
        if (!retObj.getBoolean(ComObject.Tag.IS_COMPLETE)) {
          areAllComplete = false;
          waitingFor.append("," + shard);
        }
        long moveDuration = retObj.getLong(ComObject.Tag.MOVE_DURATION);
        long deleteDuration = retObj.getLong(ComObject.Tag.DELETE_DURATION);
        if (moveDuration < localTimings.moveMin) {
          localTimings.moveMin = moveDuration;
          localTimings.moveMinShard = shard;
        }
        if (moveDuration > localTimings.moveMax) {
          localTimings.moveMax = moveDuration;
          localTimings.moveMaxShard = shard;
        }
        localTimings.moveTotal += moveDuration;
        if (deleteDuration < localTimings.deleteMin) {
          localTimings.deleteMin = deleteDuration;
          localTimings.deleteMinShard = shard;
        }
        if (deleteDuration > localTimings.deleteMax) {
          localTimings.deleteMax = deleteDuration;
          localTimings.deleteMaxShard = shard;
        }
        localTimings.deleteTotal += deleteDuration;

        long moveCount = retObj.getLong(ComObject.Tag.MOVE_COUNT);
        long deleteCount = retObj.getLong(ComObject.Tag.DELETE_COUNT);
        if (moveCount < localTimings.moveMinCount) {
          localTimings.moveMinCount = moveCount;
          localTimings.moveMinCountShard = shard;
        }
        if (moveCount > localTimings.moveMaxCount) {
          localTimings.moveMaxCount = moveCount;
          localTimings.moveMaxCountShard = shard;
        }
        localTimings.moveTotalCount += moveCount;
        if (deleteCount < localTimings.deleteMinCount) {
          localTimings.deleteMinCount = deleteCount;
          localTimings.deleteMinCountShard = shard;
        }
        if (deleteCount > localTimings.deleteMaxCount) {
          localTimings.deleteMaxCount = deleteCount;
          localTimings.deleteMaxCountShard = shard;
        }
        localTimings.deleteTotalCount += deleteCount;
      }
      catch (Exception e) {
        Integer count = countFailed.get(shard);
        if (count == null) {
          count = 0;
        }
        count++;
        countFailed.put(shard, count);
        if (count > 10) {
          throw new DatabaseException("Shard failed to rebalance: db=" + dbName + ", table=" + tableName + ", index=" +
              indexName + ", shard=" + shard, e);
        }
        int i = ExceptionUtils.indexOfThrowable(e, DeadServerException.class);
        if (i != -1) {
          throw new DeadServerException("Repartitioning shard is dead: db=" + dbName + ", table=" + tableName + ", index=" +
              indexName + ", shard=" + shard);
        }
        else {
          throw e;
        }
      }
    }
    if (areAllComplete) {
      logger.info("Waiting for shards to complete repartitioning - finished all: db={}, table={}, index={}", dbName,
          tableName, indexName);
      timings.moveMin = localTimings.moveMin;
      timings.moveMax = localTimings.moveMax;
      timings.moveTotal = localTimings.moveTotal;
      timings.deleteMin = localTimings.deleteMin;
      timings.deleteMax = localTimings.deleteMax;
      timings.deleteTotal = localTimings.deleteTotal;
      timings.deleteMinShard = localTimings.deleteMinShard;
      timings.deleteMaxShard = localTimings.deleteMaxShard;
      timings.moveMinShard = localTimings.moveMinShard;
      timings.moveMaxShard = localTimings.moveMaxShard;

      timings.moveMinCount = localTimings.moveMinCount;
      timings.moveMaxCount = localTimings.moveMaxCount;
      timings.moveTotalCount = localTimings.moveTotalCount;
      timings.deleteMinCount = localTimings.deleteMinCount;
      timings.deleteMaxCount = localTimings.deleteMaxCount;
      timings.deleteTotalCount = localTimings.deleteTotalCount;
      timings.deleteMinCountShard = localTimings.deleteMinCountShard;
      timings.deleteMaxCountShard = localTimings.deleteMaxCountShard;
      timings.moveMinCountShard = localTimings.moveMinCountShard;
      timings.moveMaxCountShard = localTimings.moveMaxCountShard;
    }
    else {
      logger.info("Waiting for shards to complete repartitioning: db={}, table={}, index={}, shards={}", dbName, tableName,
          indexName, waitingFor);
    }
    return areAllComplete;
  }

  private void startRebalanceOnShards(String dbName, ThreadPoolExecutor executor, String finalTableName,
                                      String indexName, int[] masters, List<Future> futures) {
    for (int i = 0; i < databaseServer.getShardCount(); i++) {
      final int shard = i;
      logger.info("rebalance ordered index: db={}, table={}, index={}, shard={}", dbName, finalTableName, indexName, shard);
      futures.add(executor.submit((Callable) () -> {
        ComObject cobj = new ComObject();
        cobj.put(ComObject.Tag.DB_NAME, dbName);
        cobj.put(ComObject.Tag.SCHEMA_VERSION, common.getSchemaVersion());
        cobj.put(ComObject.Tag.TABLE_NAME, finalTableName);
        cobj.put(ComObject.Tag.INDEX_NAME, indexName);
        try {
          byte[] ret = databaseServer.getDatabaseClient().send("PartitionManager:rebalanceOrderedIndex",
              shard, 0, cobj, DatabaseClient.Replica.MASTER);
          ComObject retObj = new ComObject(ret);
          masters[shard] = retObj.getInt(ComObject.Tag.REPLICA);
        }
        catch (Exception e) {
          logger.error("Error sending rebalanceOrderedIndex to shard: shard={}", shard, e);
        }
        return null;
      }));

    }
  }

  private void reshardPartitions(String dbName, List<String> toRebalance, Map<String,
      List<TableSchema.Partition>> copiedPartitionsToApply, Map<String,
      List<TableSchema.Partition>> newPartitionsToApply) throws IOException {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytesOut);
    Schema schema = common.getSchema(dbName);
    schema.serialize(out);
    out.close();
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray()));
    schema = new Schema();
    schema.deserialize(in);

    for (String index : toRebalance) {
      String[] parts = index.split(" ");
      String tableName = parts[0];
      final String indexName = parts[1];
      TableSchema tableSchema = schema.getTables().get(tableName);
      if (copiedPartitionsToApply.containsKey(index)) {
        tableSchema.getIndices().get(indexName).reshardPartitions(copiedPartitionsToApply.get(index));
        logPartitionsToApply(dbName, tableName, indexName, copiedPartitionsToApply.get(index));
      }
      if (newPartitionsToApply.containsKey(index)) {
        tableSchema.getIndices().get(indexName).reshardPartitions(newPartitionsToApply.get(index));
        logPartitionsToApply(dbName, tableName, indexName, newPartitionsToApply.get(index));
      }

      TableSchema.Partition[] lastPartitions = tableSchema.getIndices().get(indexName).getLastPartitions();
      PartitionEntry entry = new PartitionEntry();
      entry.partitions = lastPartitions;
      synchronized (previousPartitions) {
        List<PartitionEntry> list = previousPartitions.get(tableName + ":" + indexName);
        if (list == null) {
          list = new ArrayList<>();
          previousPartitions.put(tableName + ":" + indexName, list);
        }
        if (list.size() >= 5) {
          list.remove(list.size() - 1);
        }
        list.add(entry);
      }
      SnapshotManager snapshotManager = databaseServer.getSnapshotManager();
      snapshotManager.saveIndexSchema(dbName, databaseServer.getCommon().getSchemaVersion() + 1,
          tableSchema, tableSchema.getIndices().get(indexName));
      databaseServer.pushIndexSchema(dbName, databaseServer.getCommon().getSchemaVersion() + 1,
          tableSchema, tableSchema.getIndices().get(indexName));
      databaseServer.pushSchema();
    }

    common.setSchema(dbName, schema);

    common.saveSchema(databaseServer.getDataDir());

    databaseServer.pushSchema();
  }

  public static class PartitionEntry {
    private TableSchema.Partition[] partitions;

    public TableSchema.Partition[] getPartitions() {
      return partitions;
    }
  }

  public ComObject isShardRepartitioningComplete(ComObject cobj, boolean replayedCommand) {
    ComObject retObj = new ComObject();
    retObj.put(ComObject.Tag.IS_COMPLETE, isShardRepartitioningComplete);
    retObj.put(ComObject.Tag.COUNT_LONG, countMoved.get());
    retObj.put(ComObject.Tag.FINISHED, isShardRepartitioningComplete);
    if (shardRepartitionException != null) {
      retObj.put(ComObject.Tag.EXCEPTION, ExceptionUtils.getFullStackTrace(shardRepartitionException));
    }
    retObj.put(ComObject.Tag.MOVE_DURATION, moveDuration.get());
    retObj.put(ComObject.Tag.DELETE_DURATION, deleteDuration.get());
    retObj.put(ComObject.Tag.MOVE_COUNT, countMoved.get());
    retObj.put(ComObject.Tag.DELETE_COUNT, countDeleted.get());
    return retObj;
  }

  void stopShardsFromRepartitioning() {
    logger.info("stopShardsFromRepartitioning - begin");
    final ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, "__none__");
    cobj.put(ComObject.Tag.METHOD, "PartitionManager:stopRepartitioning");
    cobj.put(ComObject.Tag.SCHEMA_VERSION, common.getSchemaVersion());
    List<Future> futures = new ArrayList<>();
    for (int shard = 0; shard < databaseServer.getShardCount(); shard++) {
      for (int replica = 0; replica < databaseServer.getReplicationFactor(); replica++) {
        final int localShard = shard;
        final int localReplica = replica;
        futures.add(databaseServer.getExecutor().submit((Callable) () -> {
          try {
            databaseServer.getClient().send(null, localShard, localReplica, cobj, DatabaseClient.Replica.SPECIFIED);
          }
          catch (Exception e) {
            logger.error("Error stopping repartitioning on server: shard={}, replica={}", localShard, localReplica);
          }
          return null;
        }));
      }
    }
    for (Future future : futures) {
      try {
        future.get();
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new DatabaseException(e);
      }
      catch (ExecutionException e) {
        logger.error("Error", e);
      }
    }
    logger.info("stopShardsFromRepartitioning - end");
  }

  public void shutdown() {
    this.shutdown = true;
    interrupt();
  }

  private interface GetKeyAtOffset {
    List<Object[]> getKeyAtOffset(String dbName, int shard, String tableName, String indexName,
                                  List<OffsetEntry> offsets) throws IOException;
  }


  public static class OffsetEntry {
    final long offset;
    final int partitionOffset;

    OffsetEntry(long offset, int partitionOffset) {
      this.offset = offset;
      this.partitionOffset = partitionOffset;
    }

    public long getOffset() {
      return offset;
    }
  }

  private void logPartitionsToApply(String dbName, String tableName, String indexName, List<TableSchema.Partition> partitions) {
    StringBuilder builder = new StringBuilder();
    for (TableSchema.Partition partition : partitions) {
      StringBuilder innerBuilder = new StringBuilder("[");
      boolean first = true;
      if (partition.getUpperKey() == null) {
        innerBuilder.append("null");
      }
      else {
        for (Object obj : partition.getUpperKey()) {
          if (!first) {
            innerBuilder.append(",");
          }
          first = false;
          innerBuilder.append(DataType.getStringConverter().convert(obj));
        }
      }
      innerBuilder.append("]");
      builder.append("{shard=").append(partition.getShardOwning()).append(", upperKey=").append(innerBuilder.toString()).append(", unboundUpper=").append(partition.isUnboundUpper()).append("}");
    }
    logger.info("Applying new partitions: dbName={}, tableName={}, indexName={}, partitions={}", dbName, tableName,
        indexName, builder);
  }

  @SchemaReadLock
  public ComObject isRepartitioningComplete(ComObject cobj, boolean replayedCommand) {
    ComObject retObj = new ComObject();
    retObj.put(ComObject.Tag.FINISHED, !isRebalancing.get());
    return retObj;
  }


  @SchemaReadLock
  public ComObject getKeyAtOffset(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.DB_NAME);
    String tableName = cobj.getString(ComObject.Tag.TABLE_NAME);
    String indexName = cobj.getString(ComObject.Tag.INDEX_NAME);
    try {
      List<Long> offsets = new ArrayList<>();
      ComArray offsetsArray = cobj.getArray(ComObject.Tag.OFFSETS);
      if (offsetsArray != null) {
        for (int i = 0; i < offsetsArray.getArray().size(); i++) {
          offsets.add((Long) offsetsArray.getArray().get(i));
        }
      }
      final IndexSchema indexSchema = common.getTables(dbName).get(tableName).getIndices().get(indexName);
      final Index index = databaseServer.getIndices(dbName).getIndices().get(tableName).get(indexName);

      TableSchema.Partition[] partitions = indexSchema.getCurrPartitions();
      Object[] minKey = null;
      Object[] maxKey = null;
      if (databaseServer.getShard() == 0) {
        if (index.firstEntry() != null) {
          minKey = index.firstEntry().getKey();
        }
      }
      else {
        minKey = partitions[databaseServer.getShard() - 1].getUpperKey();
      }
      maxKey = partitions[databaseServer.getShard()].getUpperKey();

      List<Object[]> keys = index.getKeyAtOffset(offsets, minKey, maxKey);

      if (keys != null) {
        return serializeKeys(dbName, tableName, indexName, keys);
      }
    }
    catch (Exception e) {
      throw new DatabaseException("PartitionManager.getKeyAtOffset error: db=" + dbName + ", table=" + tableName + ", index=" + indexName, e);
    }

    return null;
  }

  private ComObject serializeKeys(String dbName, String tableName, String indexName, List<Object[]> keys) {
    try {
      ComObject retObj = new ComObject();
      ComArray array = retObj.putArray(ComObject.Tag.KEYS, ComObject.Type.BYTE_ARRAY_TYPE);
      for (Object[] key : keys) {
        array.add(DatabaseCommon.serializeKey(common.getTables(dbName).get(tableName), indexName, key));
      }
      return retObj;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private ComArray getPartitionSize(String dbName, int shard, String tableName, String indexName) {
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, dbName);
    cobj.put(ComObject.Tag.SCHEMA_VERSION, common.getSchemaVersion());
    cobj.put(ComObject.Tag.TABLE_NAME, tableName);
    cobj.put(ComObject.Tag.INDEX_NAME, indexName);
    byte[] ret = databaseServer.getDatabaseClient().send("PartitionManager:getPartitionSize", shard, 0,
        cobj, DatabaseClient.Replica.MASTER);
    ComObject retObj = new ComObject(ret);
    return retObj.getArray(ComObject.Tag.SIZES);
  }

  @SchemaReadLock
  public ComObject getPartitionSize(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.DB_NAME);
    String tableName = cobj.getString(ComObject.Tag.TABLE_NAME);
    String indexName = cobj.getString(ComObject.Tag.INDEX_NAME);

    if (dbName == null || tableName == null || indexName == null) {
      logger.error("getPartitionSize: parm is null: db={}, table={}, index={}", dbName, tableName, indexName);
    }
    Indices tables = databaseServer.getIndices(dbName);
    if (tables == null) {
      logger.error("getPartitionSize: tables is null, db={}, table={}, index={}", dbName, tableName, indexName);
      return null;
    }
    ConcurrentHashMap<String, Index> localIndices = tables.getIndices().get(tableName);
    if (localIndices == null) {
      logger.error("getPartitionSize: indices is null, db={}, table={}, index={}", dbName, tableName, indexName);
      return null;
    }
    Index index = localIndices.get(indexName);
    TableSchema tableSchema = common.getTables(dbName).get(tableName);
    IndexSchema indexSchema = common.getTables(dbName).get(tableName).getIndices().get(indexName);

    ComObject retObj = new ComObject();
    TableSchema.Partition[] partitions = indexSchema.getCurrPartitions();
    ComArray array = retObj.putArray(ComObject.Tag.SIZES, ComObject.Type.OBJECT_TYPE);
    for (int shard = 0; shard < databaseServer.getShardCount(); shard++) {
      Object[] minKey = null;
      Object[] maxKey = null;
      if (shard == 0) {
        if (index.firstEntry() != null) {
          minKey = index.firstEntry().getKey();
        }
      }
      else {
        minKey = partitions[shard - 1].getUpperKey();
      }
      maxKey = partitions[shard].getUpperKey();

      long size = 0;
      long rawSize = 0;
      if (shard == 0 || minKey != null) {
        size = index.getSize(minKey, maxKey);
        rawSize = index.size();
      }

      logger.info("getPartitionSize: db={} table={}, index={}, shard={}, minKey={}, maxKey={}, size={}, rawSize={}", dbName,
          tableName, indexName, shard, DatabaseCommon.keyToString(minKey), DatabaseCommon.keyToString(maxKey),
          size, rawSize);

      ComObject sizeObj = new ComObject();
      sizeObj.put(ComObject.Tag.SHARD, shard);
      sizeObj.put(ComObject.Tag.SIZE, size);
      sizeObj.put(ComObject.Tag.RAW_SIZE, rawSize);
      sizeObj.put(ComObject.Tag.MIN_KEY, DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), minKey));
      sizeObj.put(ComObject.Tag.MAX_KEY, DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), maxKey));
      array.add(sizeObj);
    }
    return retObj;
  }

  public static class MoveRequest {
    private final boolean shouldDeleteNow;
    private Object[] key;
    private byte[][] content;

     MoveRequest(Object[] key, byte[][] value, boolean shouldDeleteNow) {
      this.key = key;
      this.content = value;
      this.shouldDeleteNow = shouldDeleteNow;
    }

    public Object[] getKey() {
      return key;
    }

    public void setKey(Object[] key) {
      this.key = key;
    }

    public byte[][] getContent() {
      return content;
    }

    public void setContent(byte[][] content) {
      this.content = content;
    }
  }

  public ComObject rebalanceOrderedIndex(ComObject cobj, boolean replayedCommand) {
    if (replayedCommand) {
      return null;
    }
    if (rebalanceThread != null) {
      rebalanceThread.interrupt();
      try {
        rebalanceThread.join();
        rebalanceThread = null;
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new DatabaseException(e);
      }
    }
    isShardRepartitioningComplete = false;
//
//    cobj.put(ComObject.Tag.METHOD, "PartitionManager:doRebalanceOrderedIndex");
//    databaseServer.getLongRunningCommands().addCommand(
//        databaseServer.getLongRunningCommands().createSingleCommand(cobj.serialize()));

    stopRepartitioning = false;

    rebalanceThread = ThreadUtil.createThread(() -> {
      doRebalanceOrderedIndex(cobj, replayedCommand);
      rebalanceThread = null;
        },
        "PartitionManager.rebalanceOrderedIndex Thread");
    rebalanceThread.start();

    ComObject retObj = new ComObject();
    retObj.put(ComObject.Tag.REPLICA, databaseServer.getReplica());
    return retObj;
  }

  public static class MapEntry implements Map.Entry {
    Object[] key;
    Object value;

    MapEntry() {

    }

    MapEntry(Object[] key, Object value) {
      this.key = key;
      this.value = value;
    }

    public Object getKey() {
      return key;
    }

    public Object getValue() {
      return value;
    }

    public Object setValue(Object value) {
      this.value = value;
      return value;
    }

    @Override
    public boolean equals(Object o) {
      return false;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void setKey(Object[] key) {
      this.key = key;
    }
  }

  public ComObject stopRepartitioning(final ComObject cobj, boolean replayedCommand) {
    logger.info("stopRepartitioning: shard=" + databaseServer.getShard() + ", replica=" + databaseServer.getReplica());
    stopRepartitioning = true;
    if (rebalanceThread != null) {
      rebalanceThread.interrupt();
      try {
        rebalanceThread.join();
        rebalanceThread = null;
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new DatabaseException(e);
      }
    }
    return null;
  }

  private class RebalanceContext {
    private final ConcurrentLinkedQueue<Object[]> keysToDelete = new ConcurrentLinkedQueue<>();
    public String dbName;
    public String tableName;
    public String indexName;
    public IndexSchema indexSchema;
    public ThreadPoolExecutor executor;
    public Index index;
    public TableSchema tableSchema;
  }


  public ComObject doRebalanceOrderedIndex(final ComObject cobj, boolean replayedCommand) {
    isShardRepartitioningComplete = false;
    countMoved.set(0);
    countDeleted.set(0);
    shardRepartitionException = null;
    beginMove.set(System.currentTimeMillis());

    final String dbName = cobj.getString(ComObject.Tag.DB_NAME);
    try {
      final String tableName = cobj.getString(ComObject.Tag.TABLE_NAME);
      final String indexName = cobj.getString(ComObject.Tag.INDEX_NAME);
      logger.info("doRebalanceOrderedIndex: shard={}, db={}, tableName={}, indexName={}", databaseServer.getShard(),
          dbName, tableName, indexName);

      final Index index = indices.get(dbName).getIndices().get(tableName).get(indexName);
      final TableSchema tableSchema = common.getTables(dbName).get(tableName);
      final IndexSchema indexSchema = tableSchema.getIndices().get(indexName);

      long begin = System.currentTimeMillis();


      String[] indexFields = indexSchema.getFields();
      final int[] fieldOffsets = new int[indexFields.length];
      for (int i = 0; i < indexFields.length; i++) {
        fieldOffsets[i] = tableSchema.getFieldOffset(indexFields[i]);
      }
      Map.Entry<Object[], Object> entry = index.firstEntry();
      if (entry != null) {
        final AtomicLong countVisited = new AtomicLong();
        final ThreadPoolExecutor executor = ThreadUtil.createExecutor(
            Runtime.getRuntime().availableProcessors() * 8, "SonicBase doRebalanceOrderedIndex Thread");
        final AtomicInteger countSubmitted = new AtomicInteger();
        final AtomicInteger countFinished = new AtomicInteger();

        RebalanceContext context = new RebalanceContext();
        context.dbName = dbName;
        context.tableName = tableName;
        context.tableSchema = tableSchema;
        context.indexName = indexName;
        context.indexSchema = indexSchema;
        context.index = index;
        context.executor = ThreadUtil.createExecutor(Runtime.getRuntime().availableProcessors() * 16,
            "SonicBase Repartitioner Move Processor");

        try {
          TableSchema.Partition currPartition = indexSchema.getCurrPartitions()[databaseServer.getShard()];
          final AtomicReference<ArrayList<MapEntry>> currEntries = new AtomicReference<>(new ArrayList<>());
          try {
            if (databaseServer.getShard() > 0) {
              doRebalanceOrderedIndexNonZeroShard(context, cobj, begin, fieldOffsets, countVisited, executor,
                  countSubmitted, countFinished, currEntries);
            }
            Object[] upperKey = currPartition.getUpperKey();
            if (upperKey != null) {
              doRebalanceVisitMap(context, cobj, begin, fieldOffsets, countVisited, executor, countSubmitted,
                  countFinished, currEntries, upperKey);
            }

            if (countSubmitted.get() > 0) {
              while (countSubmitted.get() > countFinished.get() && shardRepartitionException == null) {
                Thread.sleep(1000);
              }
              if (shardRepartitionException != null) {
                logger.error("Error processing entries", shardRepartitionException);
                throw shardRepartitionException;
              }
            }
          }
          finally {
            databaseServer.setThrottleInsert(false);
          }

          logger.info("doProcessEntries - all finished: db={}, table={}, index={}, count={}, countToDelete={}", dbName,
              tableName, indexName, countVisited.get(), context.keysToDelete.size());
        }
        finally {
          moveDuration.set(System.currentTimeMillis() - beginMove.get());
          finishRebalanceOrderedIndex(dbName, tableName, indexName, begin, context.keysToDelete, countVisited, executor);
          context.executor.shutdownNow();
        }
      }
    }
    catch (Exception e) {
      shardRepartitionException = e;
      logger.error("Error rebalancing index", e);
    }
    finally {
      isShardRepartitioningComplete = true;
    }
    return null;
  }

  private Object[] getUpperKey(Index index, TableSchema.Partition currPartition) {
    Object[] upperKey = currPartition.getUpperKey();
    if (upperKey == null) {
      upperKey = index.lastEntry().getKey();
    }
    return upperKey;
  }

  private void finishRebalanceOrderedIndex(String dbName, String tableName, String indexName, long begin,
                                           ConcurrentLinkedQueue<Object[]> keysToDelete, AtomicLong countVisited,
                                           ThreadPoolExecutor executor) {
    deleteRecordsOnOtherReplicas(dbName, tableName, indexName, keysToDelete);
    executor.shutdownNow();
    logger.info("doRebalanceOrderedIndex finished: db={}, table={}, index={}, countVisited={}, duration={}", dbName,
        tableName, indexName, countVisited.get(), System.currentTimeMillis() - begin);
  }

  private void doRebalanceOrderedIndexNonZeroShard(RebalanceContext context, ComObject cobj, long begin,
                                                   int[] fieldOffsets, AtomicLong countVisited, ThreadPoolExecutor executor,
                                                   AtomicInteger countSubmitted, AtomicInteger countFinished,
                                                   AtomicReference<ArrayList<MapEntry>> currEntries) {
    TableSchema.Partition lowerPartition = context.indexSchema.getCurrPartitions()[databaseServer.getShard() - 1];
    if (lowerPartition.getUpperKey() != null) {
      Object value = context.index.get(lowerPartition.getUpperKey());
      if (value != null) {
        doProcessEntry(context, lowerPartition.getUpperKey(), value, countVisited, currEntries, countSubmitted, executor,
            fieldOffsets, cobj, countFinished);
      }

      context.index.visitHeadMap(lowerPartition.getUpperKey(), (key, value1) -> {
        if (stopRepartitioning) {
          return false;
        }
        doProcessEntry(context, key, value1, countVisited, currEntries, countSubmitted, executor,
            fieldOffsets, cobj, countFinished);
        return true;
      });
      if (currEntries.get() != null && !currEntries.get().isEmpty()) {
        try {
          logger.info(DO_PROCESS_ENTRIES_TABLE_INDEX_COUNT_STR, context.tableName, context.indexName, currEntries.get().size());
          doProcessEntries(context, currEntries.get(), cobj);
        }
        catch (Exception e) {
          shardRepartitionException = e;
          throw e;
        }
        finally {
          logger.info(DO_PROCESS_ENTRIES_FINISHED_TABLE_INDEX_COUNT_DURATION_STR, context.tableName,
              context.indexName, currEntries.get().size(), System.currentTimeMillis() - begin);
        }
      }
    }
  }

  private void doRebalanceVisitMap(RebalanceContext context, final ComObject cobj,
                                   long begin, final int[] fieldOffsets,
                                   final AtomicLong countVisited, final ThreadPoolExecutor executor,
                                   final AtomicInteger countSubmitted, final AtomicInteger countFinished,
                                   final AtomicReference<ArrayList<MapEntry>> currEntries, Object[] upperKey) {
    context.index.visitTailMap(upperKey, (key, value) -> {
      if (stopRepartitioning) {
        return false;
      }
      countVisited.incrementAndGet();
      currEntries.get().add(new MapEntry(key, value));
      if (currEntries.get().size() >= (batchOverride == null ? 20_000 : batchOverride) *
          databaseServer.getShardCount()) {
        final List<MapEntry> toProcess = currEntries.get();
        currEntries.set(new ArrayList<>());
        countSubmitted.incrementAndGet();
        if (countSubmitted.get() > 2) {
          databaseServer.setThrottleInsert(true);
        }
        executor.submit(() -> {
          long begin1 = System.currentTimeMillis();
          try {
            logger.info(DO_PROCESS_ENTRIES_TABLE_INDEX_COUNT_STR, context.tableName, context.indexName, toProcess.size());
            doProcessEntries(context, toProcess, cobj);
          }
          catch (Exception e) {
            shardRepartitionException = e;
            logger.error(ERROR_MOVING_ENTRIES_STR, e);
          }
          finally {
            countFinished.incrementAndGet();
            logger.info(DO_PROCESS_ENTRIES_FINISHED_TABLE_INDEX_COUNT_DURATION_STR, context.tableName,
                context.indexName, toProcess.size(), System.currentTimeMillis() - begin1);
          }
        });
      }
      return true;
    });
    if (currEntries.get() != null && !currEntries.get().isEmpty()) {
      try {
        logger.info(DO_PROCESS_ENTRIES_TABLE_INDEX_COUNT_STR, context.tableName, context.indexName, currEntries.get().size());
        doProcessEntries(context, currEntries.get(), cobj);
      }
      catch (Exception e) {
        shardRepartitionException = e;
        throw e;
      }
      finally {
        logger.info(DO_PROCESS_ENTRIES_FINISHED_TABLE_INDEX_COUNT_DURATION_STR, context.tableName,
            context.indexName, currEntries.get().size(), System.currentTimeMillis() - begin);
      }
    }
  }

  private void doProcessEntry(RebalanceContext context, Object[] key, Object value, AtomicLong countVisited,
                              AtomicReference<ArrayList<MapEntry>> currEntries, AtomicInteger countSubmitted,
                              ThreadPoolExecutor executor, final int[] fieldOffsets, final ComObject cobj,
                              final AtomicInteger countFinished) {
    countVisited.incrementAndGet();
    currEntries.get().add(new MapEntry(key, value));
    if (currEntries.get().size() >= (batchOverride == null ? 20000 : batchOverride) * databaseServer.getShardCount()) {
      final List<MapEntry> toProcess = currEntries.get();
      currEntries.set(new ArrayList<>());
      countSubmitted.incrementAndGet();
      if (countSubmitted.get() > 2) {
        databaseServer.setThrottleInsert(true);
      }
      executor.submit(() -> {
        long begin = System.currentTimeMillis();
        try {
          logger.info(DO_PROCESS_ENTRIES_TABLE_INDEX_COUNT_STR, context.tableName, context.indexName, toProcess.size());
          doProcessEntries(context, toProcess, cobj);
        }
        catch (Exception e) {
          shardRepartitionException = e;
          logger.error(ERROR_MOVING_ENTRIES_STR, e);
        }
        finally {
          countFinished.incrementAndGet();
          logger.info(DO_PROCESS_ENTRIES_FINISHED_TABLE_INDEX_COUNT_DURATION_STR, context.tableName, context.indexName,
              toProcess.size(), System.currentTimeMillis() - begin);
        }
      });
    }
  }

  private void deleteRecordsOnOtherReplicas(final String dbName, String tableName, String indexName,
                                            ConcurrentLinkedQueue<Object[]> keysToDelete) {

    beginDelete.set(System.currentTimeMillis());
    int threadCount = Runtime.getRuntime().availableProcessors() * databaseServer.getReplicationFactor() * 2;
    ThreadPoolExecutor executor = ThreadUtil.createExecutor(threadCount, "SonicBase deleteRecordsOnOtherReplicas Thread");
    List<Future> futures = new ArrayList<>();
    try {
      int count = 0;
      TableSchema tableSchema = common.getTables(dbName).get(tableName);
      ComObject cobj = new ComObject();
      ComArray keys = cobj.putArray(ComObject.Tag.KEYS, ComObject.Type.BYTE_ARRAY_TYPE);
      int batchSize = 20_000;
      for (Object[] key : keysToDelete) {
        keys.add(DatabaseCommon.serializeKey(tableSchema, indexName, key));
        if (keys.getArray().size() > batchSize) {
          count += keys.getArray().size();

          cobj.put(ComObject.Tag.DB_NAME, dbName);
          cobj.put(ComObject.Tag.METHOD, "PartitionManager:deleteMovedRecords");
          cobj.put(ComObject.Tag.SCHEMA_VERSION, common.getSchemaVersion());
          cobj.put(ComObject.Tag.TABLE_NAME, tableName);
          cobj.put(ComObject.Tag.INDEX_NAME, indexName);

          final ComObject currObj = cobj;
          cobj = new ComObject();
          keys = cobj.putArray(ComObject.Tag.KEYS, ComObject.Type.BYTE_ARRAY_TYPE);

          sendDeletes(executor, currObj, futures);
          logger.info("delete moved entries progress: db={}, table={}, index={}, submittedCount{}=", dbName,
              tableName, indexName, count);
        }
      }
      if (!keys.getArray().isEmpty()) {
        cobj.put(ComObject.Tag.DB_NAME, dbName);
        cobj.put(ComObject.Tag.METHOD, "PartitionManager:deleteMovedRecords");
        cobj.put(ComObject.Tag.SCHEMA_VERSION, common.getSchemaVersion());
        cobj.put(ComObject.Tag.TABLE_NAME, tableName);
        cobj.put(ComObject.Tag.INDEX_NAME, indexName);

        sendDeletes(executor, cobj, futures);
        logger.info("delete moved entries progress: db={}, table={}, index={}, submittedCount{}=", dbName,
            tableName, indexName, count);
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
    finally {
      int count = 0;
      for (Future future : futures) {
        try {
          int currCount = (int) future.get();
          count += currCount;
          logger.info("delete moved entries progress: db={}, table={}, index={}, finishedCount={}", dbName, tableName,
              indexName, count);
        }
        catch (Exception e) {
          logger.error("Error deleting moved records on replica: db={}, table={}, index={}", dbName, tableName, indexName, e);
        }
      }
      countDeleted.set(count);
      deleteDuration.set(System.currentTimeMillis() - beginDelete.get());
      executor.shutdownNow();
    }
  }

  private void sendDeletes(ThreadPoolExecutor executor, final ComObject currObj, List<Future> futures) {
    int replicaCount = databaseServer.getReplicationFactor();
    for (int i = 0; i < replicaCount; i++) {
      final int replica = i;
      futures.add(executor.submit((Callable) () -> {
      databaseServer.getDatabaseClient().send(currObj.getString(ComObject.Tag.METHOD), databaseServer.getShard(), replica,
          currObj, DatabaseClient.Replica.SPECIFIED);
      return currObj.getArray(ComObject.Tag.KEYS).getArray().size();
      }));
    }
  }

  @SchemaReadLock
  public ComObject deleteMovedRecords(ComObject cobj, boolean replayedCommand) {
    try {
      ConcurrentLinkedQueue<DeleteManager.DeleteRequest> keysToDelete = new ConcurrentLinkedQueue<>();
      final ArrayList<DeleteManager.DeleteRequest> keysToDeleteExpanded = new ArrayList<>();
      final long sequence0 = cobj.getLong(ComObject.Tag.SEQUENCE_0);
      final long sequence1 = cobj.getLong(ComObject.Tag.SEQUENCE_1);
      String dbName = cobj.getString(ComObject.Tag.DB_NAME);
      String tableName = cobj.getString(ComObject.Tag.TABLE_NAME);
      String indexName = cobj.getString(ComObject.Tag.INDEX_NAME);
      TableSchema tableSchema = common.getTables(dbName).get(tableName);
      final IndexSchema indexSchema = tableSchema.getIndices().get(indexName);
      ComArray keys = cobj.getArray(ComObject.Tag.KEYS);

      getKeysToDelete(keysToDelete, tableSchema, indexSchema, keys);

      final Index index = databaseServer.getIndices().get(dbName).getIndices().get(tableName).get(indexName);

      if (false) {
        List<Future> futures = new ArrayList<>();
        List<DeleteManager.DeleteRequest> batch = new ArrayList<>();
        AtomicInteger count = new AtomicInteger();
        for (DeleteManager.DeleteRequest request : keysToDeleteExpanded) {
          batch.add(request);
          final List<DeleteManager.DeleteRequest> finalRequests = batch;
          batch = new ArrayList<>();
          if (batch.size() > 1_000)   {
            futures.add(databaseServer.getExecutor().submit((Callable) () -> {
              for (DeleteManager.DeleteRequest request1 : finalRequests) {
                synchronized (index.getMutex(request1.getKey())) {
                  Object obj = index.remove(request1.getKey());
                  if (obj != null) {
                    databaseServer.getAddressMap().freeUnsafeIds(obj);
                  }
                  if (count.incrementAndGet() % 100000 == 0) {
                    logger.info("deleteMovedRecords progress: db={}, table={}, index={}, count={}", dbName, tableName,
                        indexName, count.get());
                  }
                }
              }
              return null;
            }));
          }
        }

        for (Future future : futures) {
          future.get();
        }
      }
      else {
        final AtomicInteger count = new AtomicInteger();
        if (replayedCommand) {
          deleteMovedRecordsForReplayedCommand(keysToDelete, keysToDeleteExpanded, indexSchema, count, index);
        }
        else {
          for (DeleteManager.DeleteRequest request : keysToDelete) {
            doDeleteMovedEntry(keysToDeleteExpanded, indexSchema, index, request);
            if (count.incrementAndGet() % 100000 == 0) {
              logger.info("deleteMovedRecords progress: count={}", count.get());
            }
          }
        }

        saveDeletes(keysToDeleteExpanded, sequence0, sequence1, dbName, tableName, indexName, indexSchema);
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
    return null;
  }

  void getKeysToDelete(ConcurrentLinkedQueue<DeleteManager.DeleteRequest> keysToDelete, TableSchema tableSchema,
                       IndexSchema indexSchema, ComArray keys) throws EOFException {
    if (keys != null) {
      for (int i = 0; i < keys.getArray().size(); i++) {
        Object[] key = DatabaseCommon.deserializeKey(tableSchema, (byte[]) keys.getArray().get(i));
        if (indexSchema.isPrimaryKey()) {
          keysToDelete.add(new DeleteManager.DeleteRequestForRecord(key));
        }
        else {
          keysToDelete.add(new DeleteManager.DeleteRequestForKeyRecord(key));
        }
      }
    }
  }

  private void deleteMovedRecordsForReplayedCommand(ConcurrentLinkedQueue<DeleteManager.DeleteRequest> keysToDelete,
                                                    ArrayList<DeleteManager.DeleteRequest> keysToDeleteExpanded,
                                                    IndexSchema indexSchema, AtomicInteger count, Index index) throws InterruptedException, ExecutionException {
    List<Future> futures = new ArrayList<>();
    for (final DeleteManager.DeleteRequest request : keysToDelete) {
      doDeleteMovedEntry(keysToDeleteExpanded, indexSchema, index, request);
      if (count.incrementAndGet() % 100000 == 0) {
        logger.info("deleteMovedRecords progress: count={}", count.get());
      }
    }
    for (Future future : futures) {
      future.get();
    }
  }

  private void saveDeletes(List<DeleteManager.DeleteRequest> keysToDeleteExpanded, long sequence0,
                           long sequence1, String dbName, String tableName, String indexName, IndexSchema indexSchema) {
    if (indexSchema.isPrimaryKey()) {
      databaseServer.getDeleteManager().saveDeletesForRecords(dbName, tableName, indexName, sequence0, sequence1,
          keysToDeleteExpanded);
    }
    else {
      databaseServer.getDeleteManager().saveDeletesForKeyRecords(dbName, tableName, indexName, sequence0, sequence1,
          keysToDeleteExpanded);
    }
  }

  private void doDeleteMovedEntry(ArrayList<DeleteManager.DeleteRequest> keysToDeleteExpanded,
                                  IndexSchema indexSchema, Index index, DeleteManager.DeleteRequest request) {
    synchronized (index.getMutex(request.getKey())) {
      Object value = index.get(request.getKey());
      byte[][] content = null;
      if (value != null) {
        if (indexSchema.isPrimaryKey()) {
          content = databaseServer.getAddressMap().fromUnsafeToRecords(value);
        }
        else {
          content = databaseServer.getAddressMap().fromUnsafeToKeys(value);
        }
      }
      if (content != null) {
        if (indexSchema.isPrimaryKey()) {
          doDeleteMovedEntryForPrimaryKey(keysToDeleteExpanded, index, request, value, content);
        }
        else {
          doDeleteMovedEntryForNonPrimaryKey(keysToDeleteExpanded, index, request, value, content);
        }
      }
    }
  }

  private void doDeleteMovedEntryForNonPrimaryKey(List<DeleteManager.DeleteRequest> keysToDeleteExpanded,
                                                  Index index, DeleteManager.DeleteRequest request, Object value,
                                                  byte[][] content) {
    byte[][] newContent = new byte[content.length][];
    for (int i = 0; i < content.length; i++) {
      if (Record.DB_VIEW_FLAG_DELETING != KeyRecord.getDbViewFlags(content[i])) {
        index.addAndGetCount(-1);
      }
      KeyRecord.setDbViewFlags(content[i], Record.DB_VIEW_FLAG_DELETING);
      KeyRecord.setDbViewNumber(content[i], common.getSchemaVersion());
      newContent[i] = content[i];

      keysToDeleteExpanded.add(new DeleteManager.DeleteRequestForKeyRecord(request.getKey(),
          KeyRecord.getPrimaryKey(content[i])));
    }
    Object newValue = databaseServer.getAddressMap().toUnsafeFromRecords(newContent);
    index.put(request.getKey(), newValue);
    databaseServer.getAddressMap().freeUnsafeIds(value);
  }

  private void doDeleteMovedEntryForPrimaryKey(List<DeleteManager.DeleteRequest> keysToDeleteExpanded,
                                               Index index, DeleteManager.DeleteRequest request, Object value,
                                               byte[][] content) {
    keysToDeleteExpanded.add(new DeleteManager.DeleteRequestForRecord(request.getKey()));

    byte[][] newContent = new byte[content.length][];
    for (int i = 0; i < content.length; i++) {
      if (Record.DB_VIEW_FLAG_DELETING != Record.getDbViewFlags(content[i])) {
        index.addAndGetCount(-1);
      }
      Record.setDbViewFlags(content[i], Record.DB_VIEW_FLAG_DELETING);
      Record.setDbViewNumber(content[i], common.getSchemaVersion());
      newContent[i] = content[i];
    }
    databaseServer.getAddressMap().writeRecordstoExistingAddress((long)value, newContent);
  }

  class MoveRequestList {
    final List<MoveRequest> moveRequests;
    final CountDownLatch latch = new CountDownLatch(1);

    MoveRequestList(List<MoveRequest> list) {
      this.moveRequests = list;
    }
  }

  private void doProcessEntries(RebalanceContext context, List<MapEntry> toProcess, ComObject cobj) {
    final Map<Integer, List<MoveRequest>> moveRequests = prepareMoveRequests();
    List<MoveRequestList> lists = new ArrayList<>();
    int consecutiveErrors = 0;
    int lockCount = 0;
    AtomicInteger countDeleted = new AtomicInteger();
    for (MapEntry entry : toProcess) {
      if (stopRepartitioning) {
        break;
      }
      try {
        if (lockCount++ % 2 == 0) {
          lockCount = 0;
        }
        byte[][] content = null;
        int shard = 0;
        List<Integer> selectedShards = PartitionUtils.findOrderedPartitionForRecord(true,
            false, context.tableSchema, context.indexName, null,
            BinaryExpression.Operator.EQUAL, null,
            entry.key, null);
        ProcessEntry processEntry = new ProcessEntry(context.index, context.indexSchema, countDeleted, entry, content, shard,
            selectedShards).invoke();
        content = processEntry.getContent();
        shard = processEntry.getShard();

        enqueueMoveRequest(context, moveRequests, lists, entry, content, shard);
        consecutiveErrors = 0;
      }
      catch (Exception t) {
        if (consecutiveErrors++ > 50) {
          throw new DatabaseException("Error moving record: table=" + cobj.getString(ComObject.Tag.TABLE_NAME) +
              INDEX_STR + cobj.getString(ComObject.Tag.INDEX_NAME) + ", key=" + DatabaseCommon.keyToString(entry.key), t);
        }
        logger.error("Error moving record: db={}, table={}, index={}, key={}", cobj.getString(ComObject.Tag.DB_NAME),
            cobj.getString(ComObject.Tag.TABLE_NAME),
            cobj.getString(ComObject.Tag.INDEX_NAME), DatabaseCommon.keyToString(entry.key), t);
      }
    }

    try {
      for (int shard = 0; shard < databaseServer.getShardCount(); shard++) {
        final List<MoveRequest> list = moveRequests.get(shard);
        if (!list.isEmpty()) {
          MoveRequestList requestList = new MoveRequestList(list);
          lists.add(requestList);
          final int finalShard = shard;
          context.executor.submit(() -> moveIndexEntriesToShard(context, finalShard, requestList));
        }
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }

    context.index.addAndGetCount(-1 * countDeleted.get());

    for (MoveRequestList requestList : lists) {
      try {
        requestList.latch.await();
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new DatabaseException();
      }
    }
  }

  private void moveIndexEntriesToShard(RebalanceContext context, int shard, MoveRequestList list) {
    try {
      long begin = System.currentTimeMillis();
      doMoveIndexEntriesToShard(context, shard, list.moveRequests);
      for (MoveRequest request : list.moveRequests) {
        context.keysToDelete.add(request.getKey());
      }
      ThreadUtil.sleep(1_000);

      logger.info("moved entries: db={}, table={}, index={}, count={}, shard={}, duration={}", context.dbName, context.tableName,
          context.indexName, list.moveRequests.size(), shard, (System.currentTimeMillis() - begin));
    }
    catch (Exception e) {
      logger.error(ERROR_MOVING_ENTRIES_STR, e);
      shardRepartitionException = e;
    }
    finally {
      //countFinished.incrementAndGet();
      list.latch.countDown();
    }
  }


  private void doMoveIndexEntriesToShard(RebalanceContext context, int shard, List<MoveRequest> moveRequests) {
    int count = 0;
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, context.dbName);
    cobj.put(ComObject.Tag.TABLE_NAME, context.tableName);
    cobj.put(ComObject.Tag.INDEX_NAME, context.indexName);
    cobj.put(ComObject.Tag.SCHEMA_VERSION, common.getSchemaVersion());
    ComArray keys = cobj.putArray(ComObject.Tag.KEYS, ComObject.Type.OBJECT_TYPE);
    int consecutiveErrors = 0;
    for (MoveRequest moveRequest : moveRequests) {
      if (stopRepartitioning) {
        break;
      }
      try {
        count++;
        byte[] bytes = DatabaseCommon.serializeKey(context.tableSchema, context.indexName, moveRequest.key);
        ComObject innerObj = new ComObject();
        keys.add(innerObj);
        innerObj.remove(ComObject.Tag.SERIALIZATION_VERSION);
        innerObj.put(ComObject.Tag.KEY_BYTES, bytes);

        byte[][] content = moveRequest.getContent();

        setRecordFlags(context.indexSchema.isPrimaryKey(), content);

        ComArray records = innerObj.putArray(ComObject.Tag.RECORDS, ComObject.Type.BYTE_ARRAY_TYPE);
        for (int i = 0; i < content.length; i++) {
          records.add(content[i]);
        }
        consecutiveErrors = 0;
      }
      catch (Exception e) {
        if (consecutiveErrors++ >= 50) {
          throw new DatabaseException("Error moving record: db=" + context.dbName + ", table=" + context.tableName + ", index=" +
              context.indexName + ", key=" + DatabaseCommon.keyToString(moveRequest.key) + ", destShard=" + shard, e);
        }
        logger.error("Error moving record: db={}, table={}, index={}, key={}, destShard={}", context.dbName,
            context.tableName, context.indexName, DatabaseCommon.keyToString(moveRequest.key), shard, e);
      }
    }
    databaseServer.getDatabaseClient().send("PartitionManager:moveIndexEntries", shard, 0, cobj,
        DatabaseClient.Replica.DEF);
    countMoved.addAndGet(count);
  }

  private void setRecordFlags(boolean primaryKey, byte[][] content) {
    if (primaryKey) {
      for (int i = 0; i < content.length; i++) {
        byte[] recordBytes = content[i];
        Record.setDbViewNumber(recordBytes, common.getSchemaVersion());
        Record.setDbViewFlags(recordBytes, Record.DB_VIEW_FLAG_ADDING);
      }
    }
    else {
      for (int i = 0; i < content.length; i++) {
        byte[] recordBytes = content[i];
        KeyRecord.setDbViewNumber(recordBytes, common.getSchemaVersion());
        KeyRecord.setDbViewFlags(recordBytes, Record.DB_VIEW_FLAG_ADDING);
      }
    }
  }

  private Map<Integer, List<MoveRequest>> prepareMoveRequests() {
    final Map<Integer, List<MoveRequest>> moveRequests = new HashMap<>();
    for (int i = 0; i < databaseServer.getShardCount(); i++) {
      moveRequests.put(i, new ArrayList<>());
    }
    return moveRequests;
  }

  private void enqueueMoveRequest(RebalanceContext context, Map<Integer, List<MoveRequest>> moveRequests,
                                  List<MoveRequestList> lists, MapEntry entry, byte[][] content,
                                  int shard) throws InterruptedException {
    if (content != null) {
      final List<MoveRequest> list = moveRequests.get(shard);
      boolean shouldDeleteNow = false;
      list.add(new MoveRequest(entry.key, content, shouldDeleteNow));
      if (list.size() > (batchOverride == null ? 5_000 : batchOverride)) {
        moveRequests.put(shard, new ArrayList<>());
        MoveRequestList requestList = new MoveRequestList(list);
        lists.add(requestList);
        final int finalShard = shard;
        context.executor.submit(() -> moveIndexEntriesToShard(context, finalShard, requestList));
      }
    }
  }

  @SchemaReadLock
  public ComObject moveIndexEntries(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.DB_NAME);
    String tableName = cobj.getString(ComObject.Tag.TABLE_NAME);
    String indexName = cobj.getString(ComObject.Tag.INDEX_NAME);
    try {
      databaseServer.getBatchRepartCount().incrementAndGet();
      ComArray keys = cobj.getArray(ComObject.Tag.KEYS);
      List<MoveRequest> moveRequests = new ArrayList<>();
      if (keys != null) {
        logger.info("moveIndexEntries: db={}, table={}, index={}, count={}", dbName, tableName, indexName, keys.getArray().size());
        int lockCount = 0;
        for (int i = 0; i < keys.getArray().size(); i++) {
          try {
            if (lockCount++ == 2) {
              lockCount = 0;
            }
            ComObject keyObj = (ComObject) keys.getArray().get(i);
            Object[] key = DatabaseCommon.deserializeKey(common.getTables(dbName).get(tableName),
                keyObj.getByteArray(ComObject.Tag.KEY_BYTES));
            ComArray records = keyObj.getArray(ComObject.Tag.RECORDS);
            if (records != null) {
              byte[][] content = new byte[records.getArray().size()][];
              for (int j = 0; j < content.length; j++) {
                content[j] = (byte[]) records.getArray().get(j);
              }
              moveRequests.add(new MoveRequest(key, content, false));
            }
          }
          catch (Exception e) {
            logger.error("Error handling move request: db={}, table={}, index={}, count={}", dbName, tableName, indexName, keys.getArray().size());
          }
        }
      }
      Index index = databaseServer.getIndex(dbName, tableName, indexName);

      IndexSchema indexSchema = databaseServer.getIndexSchema(dbName, tableName, indexName);
      databaseServer.getUpdateManager().doInsertKeys(cobj, false, dbName, moveRequests, index, tableName, indexSchema,
          replayedCommand, true);

      return null;
    }
    finally {
      databaseServer.getBatchRepartCount().decrementAndGet();
    }
  }

  @SchemaReadLock
  public ComObject getIndexCounts(ComObject cobj, boolean replayedCommand) {
    try {
      String dbName = cobj.getString(ComObject.Tag.DB_NAME);

      logger.info("getIndexCounts - begin: dbName={}", dbName);

      ComObject retObj = new ComObject();
      ComArray tables = retObj.putArray(ComObject.Tag.TABLES, ComObject.Type.OBJECT_TYPE);
      for (Map.Entry<String, ConcurrentHashMap<String, Index>> entry : databaseServer.getIndices(dbName).getIndices().entrySet()) {
        String tableName = entry.getKey();
        ComObject tableObj = new ComObject();
        tables.add(tableObj);
        tableObj.remove(ComObject.Tag.SERIALIZATION_VERSION);
        tableObj.put(ComObject.Tag.TABLE_NAME, tableName);
        ComArray localIndices = tableObj.putArray(ComObject.Tag.INDICES, ComObject.Type.OBJECT_TYPE);
        logger.info("getIndexCounts: db={}, table={}, indexCount={}", dbName, tableName, entry.getValue().entrySet().size());
        for (Map.Entry<String, Index> indexEntry : entry.getValue().entrySet()) {
          ComObject indexObject = new ComObject();
          indexObject.remove(ComObject.Tag.SERIALIZATION_VERSION);
          localIndices.add(indexObject);
          String indexName = indexEntry.getKey();
          indexObject.put(ComObject.Tag.INDEX_NAME, indexName);
          Index index = indexEntry.getValue();
          long size = index.size();
          indexObject.put(ComObject.Tag.SIZE, size);
          logger.info("getIndexCounts: db={}, table={}, index={}, count={}", dbName, tableName, indexName, size);
        }
      }

      return retObj;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public boolean isRunning() {
    return isRunning;
  }

  @Override
  public void run() {
    shutdown = false;
    isRunning = true;
    try {
      try {
        Thread.sleep(15000);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
      while (!shutdown) {
        boolean ok = false;
        for (int shard = 0; shard < databaseServer.getShardCount(); shard++) {
          ok = false;
          ok = beginRebalanceCheckHealthOfServer(ok, shard);
          if (!ok) {
            break;
          }
        }
        if (!beginRebalanceHandleNotOk(ok)) {
          sendBeginRequest();
        }
      }
    }
    finally {
      isRunning = false;
    }
  }

  private boolean beginRebalanceCheckHealthOfServer(boolean ok, int shard) {
    for (int replica = 0; replica < databaseServer.getReplicationFactor(); replica++) {
      try {
        AtomicBoolean isHealthy = new AtomicBoolean();
        databaseServer.checkHealthOfServer(shard, replica, isHealthy);
        if (isHealthy.get()) {
          ok = true;
          break;
        }
      }
      catch (Exception e) {
        logger.error("Error", e);
      }
    }
    return ok;
  }

  private boolean beginRebalanceHandleNotOk(boolean ok) {
    if (!ok) {
      try {
        Thread.sleep(2000);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new DatabaseException(e);
      }
      return true;
    }
    return false;
  }

  private boolean sendBeginRequest() {
    try {
      for (String dbName : databaseServer.getDbNames(databaseServer.getDataDir())) {
        ComObject cobj = new ComObject();
        cobj.put(ComObject.Tag.DB_NAME, dbName);
        cobj.put(ComObject.Tag.SCHEMA_VERSION, common.getSchemaVersion());
        cobj.put(ComObject.Tag.METHOD, "PartitionManager:beginRebalance");
        cobj.put(ComObject.Tag.FORCE, false);
        beginRebalance(cobj, false);
      }
      Thread.sleep(2000);
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return true;
    }
    catch (Exception t) {
      if (-1 != ExceptionUtils.indexOfThrowable(t, InterruptedException.class)) {
        return true;
      }
      logger.error("Error in master thread", t);
      try {
        Thread.sleep(2 * 1000L);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.info("PartitionManager interrupted");
        return true;
      }
    }
    return false;
  }

  private void logCurrPartitions(String dbName, String tableName, String indexName, TableSchema.Partition[] partitions) {
    StringBuilder builder = new StringBuilder();
    for (TableSchema.Partition partition : partitions) {
      StringBuilder innerBuilder = new StringBuilder("[");
      boolean first = true;
      if (partition.getUpperKey() == null) {
        innerBuilder.append("null");
      }
      else {
        for (Object obj : partition.getUpperKey()) {
          if (!first) {
            innerBuilder.append(",");
          }
          first = false;
          innerBuilder.append(DataType.getStringConverter().convert(obj));
        }
      }
      innerBuilder.append("]");
      builder.append("{ shard=").append(partition.getShardOwning()).append(", upperKey=").append(innerBuilder.toString()).append(", unboundUpper=").append(partition.isUnboundUpper()).append("}");
    }
    logger.info("Current partitions to consider: dbName={}, table={}, index={}, partitions={}", dbName, tableName,
        indexName, builder);
  }

  public ComObject beginRebalance(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.DB_NAME);
    boolean force = cobj.getBoolean(ComObject.Tag.FORCE);
    try {

      while (isRebalancing.get()) {
        Thread.sleep(2000);
      }
      isRebalancing.set(true);

      Config config = databaseServer.getConfig();

      boolean isInternal = false;
      if (config.getBoolean("clientIsPrivate") != null) {
        isInternal = config.getBoolean("clientIsPrivate");
      }

      boolean optimizedForThroughput = true;
      if (config.getString("optimizeReadsFor") != null) {
        String text = config.getString("optimizeReadsFor");
        if (!text.equalsIgnoreCase("totalThroughput")) {
          optimizedForThroughput = false;
        }
      }

      ServersConfig newConfig = new ServersConfig(databaseServer.getCluster(),
          config.getShards(), isInternal, optimizedForThroughput);
      ServersConfig.Shard[] newShards = newConfig.getShards();

      synchronized (common) {
        for (int i = 0; i < databaseServer.getShardCount(); i++) {
          newShards[i].setMasterReplica(common.getServersConfig().getShards()[i].getMasterReplica());
          for (int j = 0; j < databaseServer.getReplicationFactor(); j++) {
            newShards[i].getReplicas()[j].setDead(common.getServersConfig().getShards()[i].getReplicas()[j].isDead());
          }
        }
        common.setServersConfig(newConfig);
      }
      common.saveServersConfig(databaseServer.getDataDir());
      logger.info("PartitionManager: shardCount={}", newShards.length);
      databaseServer.setShardCount(newShards.length);
      databaseServer.getDatabaseClient().configureServers();
      databaseServer.pushServersConfig();

      Map<String, TableSchema> tables = common.getTables(dbName);
      if (tables == null) {
        return null;
      }
      for (TableSchema table : tables.values()) {
        for (IndexSchema index : table.getIndices().values()) {
          logCurrPartitions(dbName, table.getName(), index.getName(), index.getCurrPartitions());
        }
      }

      List<List<String>> indexGroups = new ArrayList<>();
      PartitionUtils.GlobalIndexCounts counts = PartitionUtils.getIndexCounts(dbName, databaseServer.getDatabaseClient());
      for (Map.Entry<String, PartitionUtils.TableIndexCounts> entry : counts.getTables().entrySet()) {
        prepareToRebalance(dbName, force, indexGroups, entry);
      }

      beginRebalanceForAllIndexGroups(dbName, indexGroups);

      logger.info("Finished rebalance");
      return null;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
    finally {
      isRebalancing.set(false);
    }
  }

  private void beginRebalanceForAllIndexGroups(String dbName, List<List<String>> indexGroups) {
    for (List<String> group : indexGroups) {
      if (group.isEmpty()) {
        continue;
      }
      try {
        beginRebalance(dbName, group);
      }
      catch (Exception e) {
        StringBuilder builder = new StringBuilder();
        for (String entry : group) {
          builder.append(entry).append(",");
        }
        logger.error("Error rebalancing index group: group={}", builder.toString(), e);
      }
    }
  }

  private boolean prepareToRebalance(String dbName, boolean force, List<List<String>> indexGroups,
                                     Map.Entry<String, PartitionUtils.TableIndexCounts> entry) {
    List<String> toRebalance;
    String primaryKeyIndex = null;
    List<String> primaryKeyGroupIndices = new ArrayList<>();
    List<String> otherIndices = new ArrayList<>();
    TableSchema tableSchema = common.getTables(dbName).get(entry.getKey());
    if (tableSchema == null) {
      logger.error("beginRebalance, unknown table: db={}, name={}", dbName, entry.getKey());
      return true;
    }
    for (Map.Entry<String, PartitionUtils.IndexCounts> indexEntry : entry.getValue().getIndices().entrySet()) {
      IndexSchema indexSchema = tableSchema.getIndices().get(indexEntry.getKey());
      if (indexSchema == null) {
        logger.error("beginRebalance, unknown index: db={}, table={}, index={}", dbName, entry.getKey(), indexEntry.getKey());
        continue;
      }
      if (indexSchema.isPrimaryKey()) {
        primaryKeyIndex = indexEntry.getKey();
      }
      else if (indexSchema.isPrimaryKeyGroup()) {
        primaryKeyGroupIndices.add(indexEntry.getKey());
      }
      else {
        otherIndices.add(indexEntry.getKey());
      }

    }
    PartitionUtils.IndexCounts currCounts = entry.getValue().getIndices().get(primaryKeyIndex);
    toRebalance = new ArrayList<>();
    if (addToRebalance(dbName, toRebalance, entry, primaryKeyIndex, currCounts, force)) {
      for (int i = 0; i < primaryKeyGroupIndices.size(); i++) {
        addToRebalance(dbName, toRebalance, entry, primaryKeyGroupIndices.get(i), currCounts, true);
      }
    }
    if (!toRebalance.isEmpty()) {
      indexGroups.add(toRebalance);
    }
    for (int i = 0; i < otherIndices.size(); i++) {
      toRebalance = new ArrayList<>();
      addToRebalance(dbName, toRebalance, entry, otherIndices.get(i), currCounts, force);
      indexGroups.add(toRebalance);
    }
    return false;
  }

  private boolean addToRebalance(
      String dbName, List<String> toRebalance, Map.Entry<String, PartitionUtils.TableIndexCounts> entry,
      String indexName, PartitionUtils.IndexCounts counts, boolean force) {
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    long total = 0;
    for (Map.Entry<Integer, Long> countEntry : counts.getCounts().entrySet()) {
      long count = countEntry.getValue();
      if (count < min) {
        min = count;
      }
      if (count > max) {
        max = count;
      }
      total += count;
    }

    if (force || (double) min / (double) max < 0.90) {
      toRebalance.add(entry.getKey() + " " + indexName);
      logger.info("Adding toRebalance: db={}, table={}, index={}, min={}, max={}", dbName, entry.getKey(), indexName, min, max);
      return true;
    }
    logger.info("Not adding toRebalance: db={}, table={}, index={}, min={}, max={}, total={}, shardCount={}", dbName,
        entry.getKey(), indexName, min, max, total, counts.getCounts().size());
    return false;
  }

  private class PrepareToReShardPartitions {
    private final String dbName;
    private final List<String> toRebalance;
    private String tableName;
    private long begin;
    private final Map<String, ComArray[]> partitionSizes;
    private final Map<String, List<TableSchema.Partition>> copiedPartitionsToApply;
    private final Map<String, List<TableSchema.Partition>> newPartitionsToApply;

    PrepareToReShardPartitions(String dbName, List<String> toRebalance, String tableName, long begin,
                               Map<String, ComArray[]> partitionSizes,
                               Map<String, List<TableSchema.Partition>> copiedPartitionsToApply,
                               Map<String, List<TableSchema.Partition>> newPartitionsToApply) {
      this.dbName = dbName;
      this.toRebalance = toRebalance;
      this.tableName = tableName;
      this.begin = begin;
      this.partitionSizes = partitionSizes;
      this.copiedPartitionsToApply = copiedPartitionsToApply;
      this.newPartitionsToApply = newPartitionsToApply;
    }

    public String getTableName() {
      return tableName;
    }

    public long getBegin() {
      return begin;
    }

    public PrepareToReShardPartitions invoke() {
      for (String index : toRebalance) {
        List<TableSchema.Partition> newPartitions = new ArrayList<>();

        String[] parts = index.split(" ");
        tableName = parts[0];
        final String indexName = parts[1];
        TableSchema tableSchema = common.getTables(dbName).get(tableName);
        AtomicLong totalCount = new AtomicLong();
        long[] currPartitionSizes = calculatePartitionSizes(partitionSizes.get(index), totalCount);

        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < databaseServer.getShardCount(); i++) {
          builder.append(",").append(currPartitionSizes[i]);
        }

        begin = System.currentTimeMillis();

        logger.info("master - calculating partitions: db={}, table={}, index={}, curSizes={}", dbName, tableName, indexName, builder);
        calculatePartitions(dbName, databaseServer.getShardCount(), newPartitions, indexName,
            tableSchema.getName(), currPartitionSizes, totalCount, (localDbName, shard, localTableName, indexName1, offsets) ->
                getKeyAtOffset(localDbName, shard, localTableName, indexName1, offsets));
        logger.info("master - calculating partitions - finished: db={}, table={}, index={}, currSizes={}, duration={}sec",
            dbName, tableName, indexName, builder, System.currentTimeMillis() - begin / 1000d);

        Index dbIndex = databaseServer.getIndex(dbName, tableName, indexName);
        final Comparator[] comparators = dbIndex.getComparators();

        sortNewPartitions(newPartitions, comparators);

        prepPartitionsToApply(index, newPartitions, indexName, tableSchema);
      }
      return this;
    }

    private long[] calculatePartitionSizes(ComArray[] comArrays, AtomicLong totalCount) {
      long[] ret = new long[comArrays.length];
      for (int shard = 0; shard < comArrays.length; shard++) {
        ComArray array = comArrays[shard];
        for (int i = 0; i < array.getArray().size(); i++) {
          ComObject cobj = (ComObject) array.getArray().get(i);
          int currShard = cobj.getInt(ComObject.Tag.SHARD);
          long size = cobj.getLong(ComObject.Tag.SIZE);
          totalCount.addAndGet(size);
          if (currShard == shard) {
            ret[currShard] += size;
          }
        }
      }
      return ret;
    }

    private void prepPartitionsToApply(String index, List<TableSchema.Partition> newPartitions, String indexName,
                                       TableSchema tableSchema) {
      if (!tableSchema.getIndices().get(indexName).isPrimaryKey() && !newPartitions.isEmpty()) {
        List<TableSchema.Partition> copiedPartitions = new ArrayList<>();
        int columnCount = tableSchema.getIndices().get(indexName).getFields().length;
        for (TableSchema.Partition partition : newPartitions) {
          TableSchema.Partition copiedPartition = new TableSchema.Partition();
          copiedPartition.setShardOwning(partition.getShardOwning());
          copiedPartition.setUnboundUpper(partition.isUnboundUpper());
          Object[] upperKey = partition.getUpperKey();
          if (upperKey != null) {
            Object[] copiedUpperKey = new Object[columnCount];
            System.arraycopy(upperKey, 0, copiedUpperKey, 0, columnCount);
            copiedPartition.setUpperKey(copiedUpperKey);
          }
          copiedPartitions.add(copiedPartition);
        }
        copiedPartitionsToApply.put(index, copiedPartitions);
      }
      else {
        newPartitionsToApply.put(index, newPartitions);
      }
    }

    private void sortNewPartitions(List<TableSchema.Partition> newPartitions, Comparator[] comparators) {
      Collections.sort(newPartitions, (o1, o2) -> {
        for (int i = 0; i < comparators.length; i++) {
          if (!(o1.getUpperKey() == null || o1.getUpperKey()[i] == null || o2.getUpperKey() == null || o2.getUpperKey()[i] == null)) {
            int value = comparators[i].compare(o1.getUpperKey()[i], o2.getUpperKey()[i]);
            if (value != 0) {
              return value;
            }
          }
        }
        return 0;
      });
    }

    private List<Object[]> getKeyAtOffset(String dbName, int shard, String tableName, String indexName,
                                          List<OffsetEntry> offsets) throws IOException {
      logger.info("getKeyAtOffset: db={}, shard={}, table={}, index={}, offsetCount={}", dbName, shard, tableName,
          indexName, offsets.size());
      long localBegin = System.currentTimeMillis();
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.DB_NAME, dbName);
      cobj.put(ComObject.Tag.SCHEMA_VERSION, common.getSchemaVersion());
      cobj.put(ComObject.Tag.TABLE_NAME, tableName);
      cobj.put(ComObject.Tag.INDEX_NAME, indexName);
      ComArray array = cobj.putArray(ComObject.Tag.OFFSETS, ComObject.Type.LONG_TYPE);
      for (OffsetEntry offset : offsets) {
        array.add(offset.offset);
      }
      byte[] ret = databaseServer.getDatabaseClient().send("PartitionManager:getKeyAtOffset", shard,
          0, cobj, DatabaseClient.Replica.MASTER);

      if (ret == null) {
        throw new IllegalStateException("Key not found on shard: shard=" + shard +
            ", table=" + tableName + INDEX_STR + indexName);
      }

      ComObject retObj = new ComObject(ret);
      ComArray keyArray = retObj.getArray(ComObject.Tag.KEYS);
      List<Object[]> keys = new ArrayList<>();
      if (keyArray != null) {
        for (int i = 0; i < keyArray.getArray().size(); i++) {
          Object[] key = DatabaseCommon.deserializeKey(common.getTables(dbName).get(tableName), (byte[]) keyArray.getArray().get(i));
          keys.add(key);
        }
      }

      logger.info("getKeyAtOffset finished: db={}, shard={}, table={}, index={}, duration={}", dbName, shard,
          tableName, indexName, (System.currentTimeMillis() - localBegin));
      return keys;
    }
  }

  static void calculatePartitions(final String dbName, int shardCount, final List<TableSchema.Partition> newPartitions,
                                  final String indexName, final String tableName, final long[] currPartitionSizes,
                                  AtomicLong totalCount, final GetKeyAtOffset getKey) {
    for (int i = 0; i < shardCount; i++) {
      newPartitions.add(new TableSchema.Partition(i));
    }
    Map<Integer, List<OffsetEntry>> shards = new HashMap<>();
    long currOffset = currPartitionSizes[0];
    double newOffset = totalCount.get() / (double)shardCount;
    int currIdx = 1;
    int partitionOffset = 0;
    long prevOffset = 0;
    while (true) {
      if (partitionOffset == shardCount - 1 || currIdx == currPartitionSizes.length + 1) {
        break;
      }
      if (currOffset > newOffset) {
        RegisterOffset registerOffset = new RegisterOffset(totalCount, shardCount, shards, newOffset, currIdx,
            partitionOffset, prevOffset).invoke();
        newOffset = registerOffset.getNewOffset();
        partitionOffset = registerOffset.getPartitionOffset();
      }
      else if (currOffset <= newOffset) {
        if (currOffset == newOffset) {
          RegisterOffset registerOffset = new RegisterOffset(totalCount, shardCount, shards, newOffset, currIdx,
              partitionOffset, prevOffset).invoke();
          newOffset = registerOffset.getNewOffset();
          partitionOffset = registerOffset.getPartitionOffset();
        }
        if (currIdx <  currPartitionSizes.length) {
          currOffset += currPartitionSizes[currIdx];
          prevOffset += currPartitionSizes[currIdx - 1];
        }
        currIdx++;
      }
    }
    ThreadPoolExecutor executor = ThreadUtil.createExecutor(newPartitions.size(),
        "SonicBase CalculatePartitions Thread");
    try {
      List<Future> futures = new ArrayList<>();
      for (final Map.Entry<Integer, List<OffsetEntry>> entry : shards.entrySet()) {
        futures.add(executor.submit((Callable) () -> {
          List<Object[]> keys = getKey.getKeyAtOffset(dbName, entry.getKey(), tableName, indexName, entry.getValue());
          for (int i = 0; i < entry.getValue().size(); i++) {
            OffsetEntry currEntry = entry.getValue().get(i);
            TableSchema.Partition partition = newPartitions.get(currEntry.partitionOffset);
            if (keys.size() >= i + 1) {
              partition.setUpperKey(keys.get(i));
            }
          }
          return null;
        }));
      }
      for (Future future : futures) {
        try {
          future.get();
        }
        catch (Exception e) {
          throw new DatabaseException(e);
        }
      }
      newPartitions.get(newPartitions.size() - 1).setUnboundUpper(true);
      StringBuilder builder = new StringBuilder();
      for (int i = 0; i < newPartitions.size(); i++) {
        TableSchema.Partition partition = newPartitions.get(i);
        Object[] key = partition.getUpperKey();
        if (key == null) {
          builder.append(", ").append(i).append("=null");
        }
        else {
          builder.append(", ").append(i).append("=").append(DatabaseCommon.keyToString(key));
        }
      }
      logger.info("Calculated getKeyAtOffset: db={}, table={}, index={}, keys={}", dbName, tableName, indexName, builder);
    }
    finally {
      executor.shutdownNow();
    }
  }

  private class ProcessEntry {
    private final Index index;
    private final IndexSchema indexSchema;
    private final AtomicInteger countDeleted;
    private final MapEntry entry;
    private byte[][] content;
    private int shard;
    private final List<Integer> selectedShards;

    ProcessEntry(Index index, IndexSchema indexSchema, AtomicInteger countDeleted, MapEntry entry, byte[][] content,
                 int shard, List<Integer> selectedShards) {
      this.index = index;
      this.indexSchema = indexSchema;
      this.countDeleted = countDeleted;
      this.entry = entry;
      this.content = content;
      this.shard = shard;
      this.selectedShards = selectedShards;
    }

    public byte[][] getContent() {
      return content;
    }

    public int getShard() {
      return shard;
    }

    public ProcessEntry invoke() {
      synchronized (index.getMutex(entry.key)) {
        entry.value = index.get(entry.key);
        if (entry.value != null) {
          if (indexSchema.isPrimaryKey()) {
            content = databaseServer.getAddressMap().fromUnsafeToRecords(entry.value);
          }
          else {
            content = databaseServer.getAddressMap().fromUnsafeToKeys(entry.value);
          }
        }
        if (content != null) {
          insertRecord();
        }
        return this;
      }
    }

    private void insertRecord() {
      shard = selectedShards.get(0);
      if (shard != databaseServer.getShard()) {
        if (indexSchema.isPrimaryKey()) {
          insertRecordForPrimaryKeyNonMatchingShard();
        }
        else {
          insertRecordForNonPrimaryKeyNonMatchingShard();
        }
      }
      else {
        if (indexSchema.isPrimaryKey()) {
          insertRecordForPrimaryKey();
        }
        else {
          insertRecordForNonPrimaryKey();
        }
        content = null;
      }
    }

    private void insertRecordForNonPrimaryKey() {
      for (int i = 0; i < content.length; i++) {
        KeyRecord.setDbViewFlags(content[i], (short) 0);
        KeyRecord.setDbViewNumber(content[i], 0);
      }
      databaseServer.getAddressMap().writeRecordstoExistingAddress((long)entry.value, content);
    }

    private void insertRecordForPrimaryKey() {
      for (int i = 0; i < content.length; i++) {
        Record.setDbViewFlags(content[i], (short) 0);
        Record.setDbViewNumber(content[i], 0);
      }
      databaseServer.getAddressMap().writeRecordstoExistingAddress((long)entry.value, content);
    }

    private void insertRecordForNonPrimaryKeyNonMatchingShard() {
      byte[][] newContent = new byte[content.length][];
      for (int i = 0; i < content.length; i++) {
        byte[] newBytes = new byte[content[i].length];
        long existindDbFlags = KeyRecord.getDbViewFlags(content[i]);
        if (existindDbFlags != Record.DB_VIEW_FLAG_DELETING) {
          countDeleted.incrementAndGet();
        }
        System.arraycopy(content[i], 0, newBytes, 0, content[i].length);
        KeyRecord.setDbViewFlags(newBytes, Record.DB_VIEW_FLAG_DELETING);
        KeyRecord.setDbViewNumber(newBytes, common.getSchemaVersion());
        newContent[i] = newBytes;
      }
      databaseServer.getAddressMap().writeRecordstoExistingAddress((long)entry.value, newContent);
    }

    private void insertRecordForPrimaryKeyNonMatchingShard() {
      byte[][] newContent = new byte[content.length][];
      for (int i = 0; i < content.length; i++) {
        byte[] newBytes = new byte[content[i].length];
        long existindDbFlags = Record.getDbViewFlags(content[i]);
        if (existindDbFlags != Record.DB_VIEW_FLAG_DELETING) {
          countDeleted.incrementAndGet();
        }
        System.arraycopy(content[i], 0, newBytes, 0, content[i].length);
        Record.setDbViewFlags(newBytes, Record.DB_VIEW_FLAG_DELETING);
        Record.setDbViewNumber(newBytes, common.getSchemaVersion());
        newContent[i] = newBytes;
      }
      databaseServer.getAddressMap().writeRecordstoExistingAddress((long)entry.value, newContent);
    }
  }

  private static class RegisterOffset {
    private final AtomicLong totalCount;
    private int shardCount;
    private Map<Integer, List<OffsetEntry>> shards;
    private double newOffset;
    private int currIdx;
    private int partitionOffset;
    private long prevOffset;

    public RegisterOffset(AtomicLong totalCount, int shardCount, Map<Integer, List<OffsetEntry>> shards, double newOffset, int currIdx, int partitionOffset, long prevOffset) {
      this.totalCount = totalCount;
      this.shardCount = shardCount;
      this.shards = shards;
      this.newOffset = newOffset;
      this.currIdx = currIdx;
      this.partitionOffset = partitionOffset;
      this.prevOffset = prevOffset;
    }

    public double getNewOffset() {
      return newOffset;
    }

    public int getPartitionOffset() {
      return partitionOffset;
    }

    public RegisterOffset invoke() {
      List<OffsetEntry> offsets = shards.computeIfAbsent(currIdx - 1, (k) -> new ArrayList<>());
      long beginOffset = 0;
      if (currIdx >= 2) {
        beginOffset = prevOffset;
      }
      offsets.add(new OffsetEntry((long)(newOffset - beginOffset), partitionOffset++));

      newOffset += totalCount.get() / (double)shardCount;
      return this;
    }
  }
}
