package com.sonicbase.server;

import com.fasterxml.jackson.databind.node.ObjectNode;
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
  private static Logger logger = LoggerFactory.getLogger(PartitionManager.class);

  private final DatabaseServer databaseServer;
  private final DatabaseCommon common;
  private final Map<String, Indices> indices;
  private Map<Integer, ShardState> stateIsShardRepartitioningComplete = new ConcurrentHashMap<>();
  private String stateTable = "none";
  private String stateIndex = "none";
  private RepartitionerState state = RepartitionerState.IDLE;
  private Exception shardRepartitionException;
  private static ConcurrentHashMap<String, List<PartitionEntry>> previousPartitions = new ConcurrentHashMap<>();
  private boolean isShardRepartitioningComplete = true;
  private AtomicLong countMoved = new AtomicLong();
  private boolean isRunning = false;
  AtomicBoolean isRebalancing = new AtomicBoolean();
  private Integer batchOverride = null;
  private AtomicBoolean isRepartitioningIndex = new AtomicBoolean();
  private int minSizeForRepartition = 0;
  private boolean shutdown;
  private AtomicBoolean isComplete = new AtomicBoolean(true);

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

      long totalBegin = System.currentTimeMillis();

      try {
        String tableName = null;
        StringBuilder toRebalanceStr = new StringBuilder();
        for (String index : toRebalance) {
          toRebalanceStr.append(index).append(", ");
        }
        logger.info("master - Rebalancing index group: group={}", toRebalanceStr);

        long begin = System.currentTimeMillis();

        Map<String, long[]> partitionSizes = new HashMap<>();
        for (String index : toRebalance) {
          String[] parts = index.split(" ");
          final String currTableName = parts[0];
          final String indexName = parts[1];
          final long[] currPartitionSizes = new long[databaseServer.getShardCount()];
          List<Future> futures = new ArrayList<>();
          for (int i = 0; i < databaseServer.getShardCount(); i++) {
            final int offset = i;
            futures.add(executor.submit((Callable) () -> {
              currPartitionSizes[offset] = getPartitionSize(dbName, offset, currTableName, indexName);
              return null;
            }));
          }
          for (Future future : futures) {
            future.get();
          }
          logger.info("master - getPartitionSize finished: table={}, index={}, duration={}sed", tableName, indexName,
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

        Thread.sleep(1000);

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
          boolean areAllComplete = checkIfRebalanceCompleteForEachShard(masters, countFailed);
          if (areAllComplete) {
            break;
          }
          Thread.sleep(1000);
        }

        common.getTables(dbName).get(tableName).getIndices().get(indexName).deleteLastPartitions();

        logger.info("master - rebalance ordered index - finished: table={}, index={}, duration={}sec", tableName,
            indexName, (System.currentTimeMillis() - begin) / 1000d);
      }
      catch (Exception e) {
        logger.error("error rebalancing index: table={}, index={}, duration={}sec", tableName, index,
            (System.currentTimeMillis() - begin) / 1000d, e);
      }
    }
  }

  private boolean checkIfRebalanceCompleteForEachShard(int[] masters, Map<Integer, Integer> countFailed) {
    boolean areAllComplete = true;
    for (int shard = 0; shard < databaseServer.getShardCount(); shard++) {
      try {
        ComObject cobj = new ComObject();
        cobj.put(ComObject.Tag.DB_NAME, "__none__");
        cobj.put(ComObject.Tag.SCHEMA_VERSION, databaseServer.getCommon().getSchemaVersion());
        cobj.put(ComObject.Tag.METHOD, "PartitionManager:isShardRepartitioningComplete");
        byte[] bytes = databaseServer.getClient().send(null, shard, masters[shard], cobj, DatabaseClient.Replica.SPECIFIED);
        ComObject retObj = new ComObject(bytes);
        long count = retObj.getLong(ComObject.Tag.COUNT_LONG);
        String exception = retObj.getString(ComObject.Tag.EXCEPTION);
        boolean finished = retObj.getBoolean(ComObject.Tag.FINISHED);
        stateIsShardRepartitioningComplete.put(shard, new ShardState(count, exception, finished));
        if (!retObj.getBoolean(ComObject.Tag.IS_COMPLETE)) {
          areAllComplete = false;
          break;
        }
      }
      catch (Exception e) {
        Integer count = countFailed.get(shard);
        if (count == null) {
          count = 0;
        }
        count++;
        countFailed.put(shard, count);
        if (count > 10) {
          throw new DatabaseException("Shard failed to rebalance: shard=" + shard, e);
        }
        int i = ExceptionUtils.indexOfThrowable(e, DeadServerException.class);
        if (i != -1) {
          throw new DeadServerException("Repartitioning shard is dead: shard=" + shard);
        }
        else {
          throw e;
        }
      }
    }
    return areAllComplete;
  }

  private void startRebalanceOnShards(String dbName, ThreadPoolExecutor executor, String finalTableName,
                                      String indexName, int[] masters, List<Future> futures) {
    for (int i = 0; i < databaseServer.getShardCount(); i++) {
      final int shard = i;
      logger.info("rebalance ordered index: shard={}", shard);
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

  public interface GetKeyAtOffset {
    List<Object[]> getKeyAtOffset(String dbName, int shard, String tableName, String indexName,
                                  List<OffsetEntry> offsets) throws IOException;
  }


  public static class OffsetEntry {
    long offset;
    int partitionOffset;

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
      builder.append("{shard=" + partition.getShardOwning() + ", upperKey=" + innerBuilder.toString() +
          ", unboundUpper=" + partition.isUnboundUpper() + "}");
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
    try {
      String dbName = cobj.getString(ComObject.Tag.DB_NAME);
      String tableName = cobj.getString(ComObject.Tag.TABLE_NAME);
      String indexName = cobj.getString(ComObject.Tag.INDEX_NAME);
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
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }

    return null;
  }

  private long getPartitionSize(String dbName, int shard, String tableName, String indexName) {
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, dbName);
    cobj.put(ComObject.Tag.SCHEMA_VERSION, common.getSchemaVersion());
    cobj.put(ComObject.Tag.TABLE_NAME, tableName);
    cobj.put(ComObject.Tag.INDEX_NAME, indexName);
    byte[] ret = databaseServer.getDatabaseClient().send("PartitionManager:getPartitionSize", shard, 0,
        cobj, DatabaseClient.Replica.MASTER);
    ComObject retObj = new ComObject(ret);
    Long size = retObj.getLong(ComObject.Tag.SIZE);
    if (size == null) {
      throw new DatabaseException("Null size: table=" + tableName + INDEX_STR + indexName);
    }
    return size;
  }

  @SchemaReadLock
  public ComObject getPartitionSize(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.DB_NAME);
    String tableName = cobj.getString(ComObject.Tag.TABLE_NAME);
    String indexName = cobj.getString(ComObject.Tag.INDEX_NAME);

    if (dbName == null || tableName == null || indexName == null) {
      logger.error("getPartitionSize: parm is null: dbName={}, table={}, index={}", dbName, tableName, indexName);
    }
    Indices tables = databaseServer.getIndices(dbName);
    if (tables == null) {
      logger.error("getPartitionSize: tables is null");
      return null;
    }
    ConcurrentHashMap<String, Index> localIndices = tables.getIndices().get(tableName);
    if (localIndices == null) {
      logger.error("getPartitionSize: indices is null");
      return null;
    }
    Index index = localIndices.get(indexName);
    IndexSchema indexSchema = common.getTables(dbName).get(tableName).getIndices().get(indexName);

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

    long size = index.getSize(minKey, maxKey);
    long rawSize = index.size();

    logger.info("getPartitionSize: dbName={} table={}, index={}, minKey={}, maxKey={}, size={}, rawSize={}", dbName,
        tableName, indexName, DatabaseCommon.keyToString(minKey), DatabaseCommon.keyToString(maxKey),
        size, rawSize);

    ComObject retObj = new ComObject();
    retObj.put(ComObject.Tag.SIZE, size);

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
    isShardRepartitioningComplete = false;

    cobj.put(ComObject.Tag.METHOD, "PartitionManager:doRebalanceOrderedIndex");
    databaseServer.getLongRunningCommands().addCommand(
        databaseServer.getLongRunningCommands().createSingleCommand(cobj.serialize()));
    ComObject retObj = new ComObject();
    retObj.put(ComObject.Tag.REPLICA, databaseServer.getReplica());
    return retObj;
  }

  public static class MapEntry {
    Object[] key;
    Object value;

    MapEntry(Object[] key, Object value) {
      this.key = key;
      this.value = value;
    }

    public Object[] getKey() {
      return key;
    }

    public Object getValue() {
      return value;
    }
  }

  private class MoveProcessor {

    private final String dbName;
    private final String tableName;
    private final String indexName;
    private final boolean isPrimaryKey;
    private final int shard;
    private final Index index;
    private final ConcurrentLinkedQueue<Object[]> keysToDelete;
    private final ThreadPoolExecutor executor;
    private ArrayBlockingQueue<MoveRequestList> queue = new ArrayBlockingQueue<>(100000);
    private boolean shutdown;
    private Thread thread;
    private AtomicInteger countStarted = new AtomicInteger();
    private AtomicInteger countFinished = new AtomicInteger();

    MoveProcessor(String dbName, String tableName, String indexName, boolean isPrimaryKey,
                  Index index, ConcurrentLinkedQueue<Object[]> keysToDelete, int shard) {
      this.dbName = dbName;
      this.tableName = tableName;
      this.indexName = indexName;
      this.isPrimaryKey = isPrimaryKey;
      this.index = index;
      this.keysToDelete = keysToDelete;
      this.shard = shard;
      this.executor = ThreadUtil.createExecutor(Runtime.getRuntime().availableProcessors(),
          "SonicBase MoveProcessor Thread");
    }

    public void shutdown() {
      this.shutdown = true;
      thread.interrupt();
      executor.shutdownNow();
    }

    public void start() {
      thread = new Thread(() -> {
        while (!shutdown) {
          try {
            final MoveRequestList list = queue.poll(30_000, TimeUnit.MILLISECONDS);
            if (list == null) {
              continue;
            }
            countStarted.incrementAndGet();
            executor.submit(() -> moveIndexEntriesToShard(list));
          }
          catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            break;
          }
          catch (Exception e) {
            shardRepartitionException = e;
            throw new DatabaseException("Error processing move requests", e);
          }
          if (shardRepartitionException != null) {
            throw new DatabaseException(shardRepartitionException);
          }
        }
      }, "MoveProcessor - shard=" + shard);
      thread.start();
    }

    private void moveIndexEntriesToShard(MoveRequestList list) {
      try {
        final List<Object> toFree = new ArrayList<>();
        long begin = System.currentTimeMillis();
        doMoveIndexEntriesToShard(dbName, tableName, indexName, isPrimaryKey, shard, list.moveRequests);
        for (MoveRequest request : list.moveRequests) {
          if (request.shouldDeleteNow) {
            deleteIndexEntryNow(request);
          }
          else {
            keysToDelete.add(request.getKey());
          }
        }
        try {
          Thread.sleep(1000);
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new DatabaseException(e);
        }
        for (Object obj : toFree) {
          databaseServer.getAddressMap().freeUnsafeIds(obj);
        }

        logger.info("moved entries: table={}, index={}, count={}, shard={}, duration={}", tableName,
            indexName, list.moveRequests.size(), shard, (System.currentTimeMillis() - begin));
      }
      catch (Exception e) {
        logger.error(ERROR_MOVING_ENTRIES_STR, e);
        shardRepartitionException = e;
      }
      finally {
        countFinished.incrementAndGet();
        list.latch.countDown();
      }
    }

    private void deleteIndexEntryNow(MoveRequest request) {
      synchronized (index.getMutex(request.key)) {

        decrementIndexCountAsNeeded(request);

        Object value = index.remove(request.key);
        if (value != null) {
          databaseServer.getAddressMap().freeUnsafeIds(value);
        }
      }
    }

    private void decrementIndexCountAsNeeded(MoveRequest request) {
      if (isPrimaryKey) {
        decrementIndexCountAsNeededForPrimaryKey(request);
      }
      else {
        decrementIndexCountAsNeededForNonPrimaryKey(request);
      }
    }

    private void decrementIndexCountAsNeededForNonPrimaryKey(MoveRequest request) {
      byte[][] content = databaseServer.getAddressMap().fromUnsafeToKeys(index.get(request.key));
      if (content != null) {
        for (byte[] bytes : content) {
          if (Record.DB_VIEW_FLAG_DELETING != KeyRecord.getDbViewFlags(bytes)) {
            index.addAndGetCount(-1);
          }
        }
      }
    }

    private void decrementIndexCountAsNeededForPrimaryKey(MoveRequest request) {
      byte[][] content = databaseServer.getAddressMap().fromUnsafeToRecords(index.get(request.key));
      if (content != null) {
        for (byte[] bytes : content) {
          if (Record.DB_VIEW_FLAG_DELETING != Record.getDbViewFlags(bytes)) {
            index.addAndGetCount(-1);
          }
        }
      }
    }

    public void await() {
      while (!shutdown) {
        if (queue.isEmpty() && countStarted.get() == countFinished.get()) {
          break;
        }
        try {
          Thread.sleep(50);
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }

    private void doMoveIndexEntriesToShard(
        String dbName, String tableName, String indexName, boolean primaryKey, int shard, List<MoveRequest> moveRequests) {
      int count = 0;
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.DB_NAME, dbName);
      cobj.put(ComObject.Tag.TABLE_NAME, tableName);
      cobj.put(ComObject.Tag.INDEX_NAME, indexName);
      cobj.put(ComObject.Tag.SCHEMA_VERSION, common.getSchemaVersion());
      ComArray keys = cobj.putArray(ComObject.Tag.KEYS, ComObject.Type.OBJECT_TYPE);
      int consecutiveErrors = 0;
      for (MoveRequest moveRequest : moveRequests) {
        try {
          count++;
          byte[] bytes = DatabaseCommon.serializeKey(common.getTables(dbName).get(tableName), indexName, moveRequest.key);
          ComObject innerObj = new ComObject();
          keys.add(innerObj);
          innerObj.remove(ComObject.Tag.SERIALIZATION_VERSION);
          innerObj.put(ComObject.Tag.KEY_BYTES, bytes);

          byte[][] content = moveRequest.getContent();

          setRecordFlags(primaryKey, content);

          ComArray records = innerObj.putArray(ComObject.Tag.RECORDS, ComObject.Type.BYTE_ARRAY_TYPE);
          for (int i = 0; i < content.length; i++) {
            records.add(content[i]);
          }
          consecutiveErrors = 0;
        }
        catch (Exception e) {
          if (consecutiveErrors++ >= 50) {
            throw new DatabaseException("Error moving record: key=" + DatabaseCommon.keyToString(moveRequest.key) +
                ", destShard=" + shard, e);
          }
          logger.error("Error moving record: key={}, destShard={}", DatabaseCommon.keyToString(moveRequest.key),
              shard, e);
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
  }

  public ComObject stopRepartitioning(final ComObject cobj, boolean replayedCommand) {
    logger.info("stopRepartitioning: shard={}, replica={}", databaseServer.getShard(), databaseServer.getReplica());
    if (moveProcessors != null) {
      for (MoveProcessor processor : moveProcessors) {
        processor.shutdown();
      }
    }
    return null;
  }

  private MoveProcessor[] moveProcessors = null;

  public ComObject doRebalanceOrderedIndex(final ComObject cobj, boolean replayedCommand) {
    isShardRepartitioningComplete = false;
    countMoved.set(0);
    shardRepartitionException = null;


    final String dbName = cobj.getString(ComObject.Tag.DB_NAME);
    try {
      final String tableName = cobj.getString(ComObject.Tag.TABLE_NAME);
      final String indexName = cobj.getString(ComObject.Tag.INDEX_NAME);
      logger.info("doRebalanceOrderedIndex: shard={}, dbName={}, tableName={}, indexName={}", databaseServer.getShard(),
          dbName, tableName, indexName);

      final Index index = indices.get(dbName).getIndices().get(tableName).get(indexName);
      final TableSchema tableSchema = common.getTables(dbName).get(tableName);
      final IndexSchema indexSchema = tableSchema.getIndices().get(indexName);

      long begin = System.currentTimeMillis();

      final ConcurrentLinkedQueue<Object[]> keysToDelete = new ConcurrentLinkedQueue<>();

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

        startMoveProcessors(dbName, tableName, indexName, index, indexSchema, keysToDelete);

        try {
          TableSchema.Partition currPartition = indexSchema.getCurrPartitions()[databaseServer.getShard()];
          final AtomicReference<ArrayList<MapEntry>> currEntries = new AtomicReference<>(new ArrayList<MapEntry>());
          try {
            if (databaseServer.getShard() > 0) {
              doRebalanceOrderedIndexNonZeroShard(cobj, dbName, tableName, indexName, index, tableSchema, indexSchema,
                  begin, fieldOffsets, countVisited, executor, countSubmitted, countFinished, currEntries);
            }
            Object[] upperKey = getUpperKey(index, currPartition);

            doRebalanceVisitMap(cobj, dbName, tableName, indexName, index, tableSchema, indexSchema, begin, fieldOffsets,
                countVisited, executor, countSubmitted, countFinished, currEntries, upperKey);

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

          logger.info("doProcessEntries - all finished: table={}, index={}, count={}, countToDelete={}", tableName,
              indexName, countVisited.get(), keysToDelete.size());
        }
        finally {
          finishRebalanceOrderedIndex(dbName, tableName, indexName, begin, keysToDelete, countVisited, executor);
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
    for (MoveProcessor moveProcessor : moveProcessors) {
      if (moveProcessor != null) {
        moveProcessor.await();
        moveProcessor.shutdown();
      }
    }
    deleteRecordsOnOtherReplicas(dbName, tableName, indexName, keysToDelete);
    executor.shutdownNow();
    logger.info("doRebalanceOrderedIndex finished: table={}, index={}, countVisited={}, duration={}", tableName,
        indexName, countVisited.get(), System.currentTimeMillis() - begin);
  }

  private void startMoveProcessors(String dbName, String tableName, String indexName, Index index, IndexSchema
      indexSchema, ConcurrentLinkedQueue<Object[]> keysToDelete) {
    moveProcessors = new MoveProcessor[databaseServer.getShardCount()];
    for (int i = 0; i < moveProcessors.length; i++) {
      moveProcessors[i] = new MoveProcessor(dbName, tableName, indexName, indexSchema.isPrimaryKey(), index, keysToDelete, i);
      moveProcessors[i].start();
    }
  }

  private void doRebalanceOrderedIndexNonZeroShard(ComObject cobj, String dbName, String tableName, String indexName,
                                                   Index index, TableSchema tableSchema, IndexSchema indexSchema, long begin,
                                                   int[] fieldOffsets, AtomicLong countVisited, ThreadPoolExecutor executor,
                                                   AtomicInteger countSubmitted, AtomicInteger countFinished,
                                                   AtomicReference<ArrayList<MapEntry>> currEntries) {
    TableSchema.Partition lowerPartition = indexSchema.getCurrPartitions()[databaseServer.getShard() - 1];
    if (lowerPartition.getUpperKey() != null) {
      Object value = index.get(lowerPartition.getUpperKey());
      if (value != null) {
        doProcessEntry(lowerPartition.getUpperKey(), value, countVisited, currEntries, countSubmitted, executor,
            tableName, indexName, index, indexSchema, dbName, fieldOffsets, tableSchema, cobj, countFinished);
      }

      index.visitHeadMap(lowerPartition.getUpperKey(), (key, value1) -> {
        doProcessEntry(key, value1, countVisited, currEntries, countSubmitted, executor, tableName, indexName, index,
            indexSchema, dbName, fieldOffsets, tableSchema, cobj, countFinished);
        return true;
      });
      if (currEntries.get() != null && !currEntries.get().isEmpty()) {
        try {
          logger.info(DO_PROCESS_ENTRIES_TABLE_INDEX_COUNT_STR, tableName, indexName, currEntries.get().size());
          doProcessEntries(moveProcessors, indexName, currEntries.get(), index, indexSchema, tableSchema, cobj);
        }
        catch (Exception e) {
          shardRepartitionException = e;
          throw e;
        }
        finally {
          logger.info(DO_PROCESS_ENTRIES_FINISHED_TABLE_INDEX_COUNT_DURATION_STR, tableName,
              indexName, currEntries.get().size(), System.currentTimeMillis() - begin);
        }
      }
    }
  }

  private void doRebalanceVisitMap(final ComObject cobj, final String dbName, final String tableName,
                                   final String indexName, final Index index, final TableSchema tableSchema,
                                   final IndexSchema indexSchema, long begin, final int[] fieldOffsets,
                                   final AtomicLong countVisited, final ThreadPoolExecutor executor,
                                   final AtomicInteger countSubmitted, final AtomicInteger countFinished,
                                   final AtomicReference<ArrayList<MapEntry>> currEntries, Object[] upperKey) {
    index.visitTailMap(upperKey, (key, value) -> {
      countVisited.incrementAndGet();
      currEntries.get().add(new MapEntry(key, value));
      if (currEntries.get().size() >= (batchOverride == null ? 10000 : batchOverride) *
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
            logger.info(DO_PROCESS_ENTRIES_TABLE_INDEX_COUNT_STR, tableName, indexName, toProcess.size());
            doProcessEntries(moveProcessors, indexName, toProcess, index, indexSchema, tableSchema, cobj);
          }
          catch (Exception e) {
            shardRepartitionException = e;
            logger.error(ERROR_MOVING_ENTRIES_STR, e);
          }
          finally {
            countFinished.incrementAndGet();
            logger.info(DO_PROCESS_ENTRIES_FINISHED_TABLE_INDEX_COUNT_DURATION_STR, tableName,
                indexName, toProcess.size(), System.currentTimeMillis() - begin1);
          }
        });
      }
      return true;
    });
    if (currEntries.get() != null && !currEntries.get().isEmpty()) {
      try {
        logger.info(DO_PROCESS_ENTRIES_TABLE_INDEX_COUNT_STR, tableName, indexName, currEntries.get().size());
        doProcessEntries(moveProcessors, indexName, currEntries.get(), index, indexSchema, tableSchema, cobj);
      }
      catch (Exception e) {
        shardRepartitionException = e;
        throw e;
      }
      finally {
        logger.info(DO_PROCESS_ENTRIES_FINISHED_TABLE_INDEX_COUNT_DURATION_STR, tableName,
            indexName, currEntries.get().size(), System.currentTimeMillis() - begin);
      }
    }
  }

  private void doProcessEntry(Object[] key, Object value, AtomicLong countVisited,
                              AtomicReference<ArrayList<MapEntry>> currEntries, AtomicInteger countSubmitted,
                              ThreadPoolExecutor executor, final String tableName, final String indexName,
                              final Index index, final IndexSchema indexSchema, final String dbName,
                              final int[] fieldOffsets, final TableSchema tableSchema, final ComObject cobj,
                              final AtomicInteger countFinished) {
    countVisited.incrementAndGet();
    currEntries.get().add(new MapEntry(key, value));
    if (currEntries.get().size() >= (batchOverride == null ? 50000 : batchOverride) * databaseServer.getShardCount()) {
      final List<MapEntry> toProcess = currEntries.get();
      currEntries.set(new ArrayList<>());
      countSubmitted.incrementAndGet();
      if (countSubmitted.get() > 2) {
        databaseServer.setThrottleInsert(true);
      }
      executor.submit(() -> {
        long begin = System.currentTimeMillis();
        try {
          logger.info(DO_PROCESS_ENTRIES_TABLE_INDEX_COUNT_STR, tableName, indexName, toProcess.size());
          doProcessEntries(moveProcessors, indexName, toProcess, index, indexSchema, tableSchema, cobj);
        }
        catch (Exception e) {
          shardRepartitionException = e;
          logger.error(ERROR_MOVING_ENTRIES_STR, e);
        }
        finally {
          countFinished.incrementAndGet();
          logger.info(DO_PROCESS_ENTRIES_FINISHED_TABLE_INDEX_COUNT_DURATION_STR, tableName, indexName,
              toProcess.size(), System.currentTimeMillis() - begin);
        }
      });
    }
  }

  private void deleteRecordsOnOtherReplicas(final String dbName, String tableName, String indexName,
                                            ConcurrentLinkedQueue<Object[]> keysToDelete) {

    ThreadPoolExecutor executor = ThreadUtil.createExecutor(16, "SonicBase deleteRecordsOnOtherReplicas Thread");
    List<Future> futures = new ArrayList<>();
    try {
      int count = 0;
      TableSchema tableSchema = common.getTables(dbName).get(tableName);
      ComObject cobj = new ComObject();
      ComArray keys = cobj.putArray(ComObject.Tag.KEYS, ComObject.Type.BYTE_ARRAY_TYPE);
      int batchSize = (int) Math.nextUp((double)keysToDelete.size() / 16d);
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
          logger.info("delete moved entries progress: submittedCount{}=", count);
        }
      }
      if (!keys.getArray().isEmpty()) {
        cobj.put(ComObject.Tag.DB_NAME, dbName);
        cobj.put(ComObject.Tag.METHOD, "PartitionManager:deleteMovedRecords");
        cobj.put(ComObject.Tag.SCHEMA_VERSION, common.getSchemaVersion());
        cobj.put(ComObject.Tag.TABLE_NAME, tableName);
        cobj.put(ComObject.Tag.INDEX_NAME, indexName);

        sendDeletes(executor, cobj, futures);
        logger.info("delete moved entries progress: submittedCount={}", count);
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
          logger.info("delete moved entries progress: finishedCount={}", count);
        }
        catch (Exception e) {
          logger.error("Error deleting moved records on replica", e);
        }
      }
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
      final ConcurrentLinkedQueue<DeleteManager.DeleteRequest> keysToDeleteExpanded = new ConcurrentLinkedQueue<>();
      final long sequence0 = cobj.getLong(ComObject.Tag.SEQUENCE_0);
      final long sequence1 = cobj.getLong(ComObject.Tag.SEQUENCE_1);
      String dbName = cobj.getString(ComObject.Tag.DB_NAME);
      String tableName = cobj.getString(ComObject.Tag.TABLE_NAME);
      String indexName = cobj.getString(ComObject.Tag.INDEX_NAME);
      TableSchema tableSchema = common.getTables(dbName).get(tableName);
      final IndexSchema indexSchema = tableSchema.getIndices().get(indexName);
      ComArray keys = cobj.getArray(ComObject.Tag.KEYS);

      getKeysToDelete(keysToDelete, tableSchema, indexSchema, keys);

      final AtomicInteger count = new AtomicInteger();
      final List<Object> toFree = new ArrayList<>();
      final Index index = databaseServer.getIndices().get(dbName).getIndices().get(tableName).get(indexName);
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
      for (Object obj : toFree) {
        databaseServer.getAddressMap().freeUnsafeIds(obj);
      }

      saveDeletes(keysToDeleteExpanded, sequence0, sequence1, dbName, tableName, indexName, indexSchema);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
    return null;
  }

  private void getKeysToDelete(ConcurrentLinkedQueue<DeleteManager.DeleteRequest> keysToDelete, TableSchema tableSchema,
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
                                                    ConcurrentLinkedQueue<DeleteManager.DeleteRequest> keysToDeleteExpanded,
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

  private void saveDeletes(ConcurrentLinkedQueue<DeleteManager.DeleteRequest> keysToDeleteExpanded, long sequence0,
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

  private void doDeleteMovedEntry(ConcurrentLinkedQueue<DeleteManager.DeleteRequest> keysToDeleteExpanded,
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

  private void doDeleteMovedEntryForNonPrimaryKey(ConcurrentLinkedQueue<DeleteManager.DeleteRequest> keysToDeleteExpanded,
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

  private void doDeleteMovedEntryForPrimaryKey(ConcurrentLinkedQueue<DeleteManager.DeleteRequest> keysToDeleteExpanded,
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
    Object newValue = databaseServer.getAddressMap().toUnsafeFromRecords(newContent);
    index.put(request.getKey(), newValue);
    databaseServer.getAddressMap().freeUnsafeIds(value);
  }

  class MoveRequestList {
    List<MoveRequest> moveRequests;
    CountDownLatch latch = new CountDownLatch(1);

    MoveRequestList(List<MoveRequest> list) {
      this.moveRequests = list;
    }
  }

  private void doProcessEntries(MoveProcessor[] moveProcessors, final String indexName, List<MapEntry> toProcess,
                                final Index index, final IndexSchema indexSchema, TableSchema tableSchema, ComObject cobj) {
    final Map<Integer, List<MoveRequest>> moveRequests = prepareMoveRequests();
    List<MoveRequestList> lists = new ArrayList<>();
    int consecutiveErrors = 0;
    int lockCount = 0;
    AtomicInteger countDeleted = new AtomicInteger();
    for (MapEntry entry : toProcess) {
      try {
        if (lockCount++ % 2 == 0) {
          lockCount = 0;
        }
        byte[][] content = null;
        int shard = 0;
        List<Integer> selectedShards = PartitionUtils.findOrderedPartitionForRecord(true,
            false, tableSchema, indexName, null,
            BinaryExpression.Operator.EQUAL, null,
            entry.key, null);
        synchronized (index.getMutex(entry.key)) {
          ProcessEntry processEntry = new ProcessEntry(index, indexSchema, countDeleted, entry, content, shard,
              selectedShards).invoke();
          content = processEntry.getContent();
          shard = processEntry.getShard();
        }

        enqueueMoveRequest(moveProcessors, moveRequests, lists, entry, content, shard);
        consecutiveErrors = 0;
      }
      catch (Exception t) {
        if (consecutiveErrors++ > 50) {
          throw new DatabaseException("Error moving record: table=" + cobj.getString(ComObject.Tag.TABLE_NAME) +
              INDEX_STR + cobj.getString(ComObject.Tag.INDEX_NAME) + ", key=" + DatabaseCommon.keyToString(entry.key), t);
        }
        logger.error("Error moving record: table={}, index={}, key={}", cobj.getString(ComObject.Tag.TABLE_NAME),
            cobj.getString(ComObject.Tag.INDEX_NAME), DatabaseCommon.keyToString(entry.key), t);
      }
    }

    try {
      for (int i = 0; i < databaseServer.getShardCount(); i++) {
        final int shard = i;
        final List<MoveRequest> list = moveRequests.get(i);
        if (!list.isEmpty()) {
          MoveRequestList requestList = new MoveRequestList(list);
          lists.add(requestList);
          moveProcessors[shard].queue.put(requestList);
        }
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }

    index.addAndGetCount(-1 * countDeleted.get());

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

  private Map<Integer, List<MoveRequest>> prepareMoveRequests() {
    final Map<Integer, List<MoveRequest>> moveRequests = new HashMap<>();
    for (int i = 0; i < databaseServer.getShardCount(); i++) {
      moveRequests.put(i, new ArrayList<>());
    }
    return moveRequests;
  }

  private void enqueueMoveRequest(MoveProcessor[] moveProcessors, Map<Integer, List<MoveRequest>> moveRequests,
                                  List<MoveRequestList> lists, MapEntry entry, byte[][] content,
                                  int shard) throws InterruptedException {
    if (content != null) {
      final List<MoveRequest> list = moveRequests.get(shard);
      boolean shouldDeleteNow = false;
      list.add(new MoveRequest(entry.key, content, shouldDeleteNow));
      if (list.size() > (batchOverride == null ? 50000 : batchOverride)) {
        moveRequests.put(shard, new ArrayList<>());
        MoveRequestList requestList = new MoveRequestList(list);
        lists.add(requestList);
        moveProcessors[shard].queue.put(requestList);
      }
    }
  }

  @SchemaReadLock
  public ComObject moveIndexEntries(ComObject cobj, boolean replayedCommand) {
    try {
      databaseServer.getBatchRepartCount().incrementAndGet();
      String dbName = cobj.getString(ComObject.Tag.DB_NAME);

      String tableName = cobj.getString(ComObject.Tag.TABLE_NAME);
      String indexName = cobj.getString(ComObject.Tag.INDEX_NAME);
      ComArray keys = cobj.getArray(ComObject.Tag.KEYS);
      List<MoveRequest> moveRequests = new ArrayList<>();
      if (keys != null) {
        logger.info("moveIndexEntries: table={}, index={}, count={}", tableName, indexName, keys.getArray().size());
        int lockCount = 0;
        for (int i = 0; i < keys.getArray().size(); i++) {
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
      }
      Index index = databaseServer.getIndex(dbName, tableName, indexName);

      IndexSchema indexSchema = databaseServer.getIndexSchema(dbName, tableName, indexName);
      databaseServer.getUpdateManager().doInsertKeys(cobj, dbName, moveRequests, index, tableName, indexSchema,
          replayedCommand, true);

      return null;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
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
        logger.info("getIndexCounts: dbName={}, table={}, indexCount={}", dbName, tableName, entry.getValue().entrySet().size());
        for (Map.Entry<String, Index> indexEntry : entry.getValue().entrySet()) {
          ComObject indexObject = new ComObject();
          indexObject.remove(ComObject.Tag.SERIALIZATION_VERSION);
          localIndices.add(indexObject);
          String indexName = indexEntry.getKey();
          indexObject.put(ComObject.Tag.INDEX_NAME, indexName);
          Index index = indexEntry.getValue();
          long size = index.size();
          indexObject.put(ComObject.Tag.SIZE, size);
          logger.info("getIndexCounts: dbName={}, table={}, index={}, count={}", dbName, tableName, indexName, size);
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
        if (beginRebalanceHandleNotOk(ok)) {
          continue;
        }

        if (sendBeginRequest()) {
          break;
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
      builder.append("{ shard=" + partition.getShardOwning() + ", upperKey=" + innerBuilder.toString() +
          ", unboundUpper=" + partition.isUnboundUpper() + "}");
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

      ObjectNode config = databaseServer.getConfig();

      boolean isInternal = false;
      if (config.has("clientIsPrivate")) {
        isInternal = config.get("clientIsPrivate").asBoolean();
      }

      boolean optimizedForThroughput = true;
      if (config.has("optimizeReadsFor")) {
        String text = config.get("optimizeReadsFor").asText();
        if (!text.equalsIgnoreCase("totalThroughput")) {
          optimizedForThroughput = false;
        }
      }

      ServersConfig newConfig = new ServersConfig(databaseServer.getCluster(),
          config.withArray("shards"), isInternal, optimizedForThroughput);
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
      logger.error("beginRebalance, unknown table: name={}", entry.getKey());
      return true;
    }
    for (Map.Entry<String, PartitionUtils.IndexCounts> indexEntry : entry.getValue().getIndices().entrySet()) {
      IndexSchema indexSchema = tableSchema.getIndices().get(indexEntry.getKey());
      if (indexSchema == null) {
        logger.error("beginRebalance, unknown index: table={}, index={}", entry.getKey(), indexEntry.getKey());
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
    if (addToRebalance(toRebalance, entry, primaryKeyIndex, currCounts, force)) {
      for (int i = 0; i < primaryKeyGroupIndices.size(); i++) {
        addToRebalance(toRebalance, entry, primaryKeyGroupIndices.get(i), currCounts, true);
      }
    }
    if (!toRebalance.isEmpty()) {
      indexGroups.add(toRebalance);
    }
    for (int i = 0; i < otherIndices.size(); i++) {
      toRebalance = new ArrayList<>();
      addToRebalance(toRebalance, entry, otherIndices.get(i), currCounts, force);
      indexGroups.add(toRebalance);
    }
    return false;
  }

  private boolean addToRebalance(
      List<String> toRebalance, Map.Entry<String, PartitionUtils.TableIndexCounts> entry,
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
    if (total < minSizeForRepartition) {
      logger.info("Not adding toRebalance: table={}, index={}, min={}, max={}, total={}, shardCount={}", entry.getKey(),
          indexName, min, max, total, counts.getCounts().size());
      return false;
    }
    if (force || (double) min / (double) max < 0.90) {
      toRebalance.add(entry.getKey() + " " + indexName);
      logger.info("Adding toRebalance: table={}, index={}", entry.getKey(), indexName);
      return true;
    }
    logger.info("Not adding toRebalance: table={}, index={}, min={}, max={}, total={}, shardCount={}", entry.getKey(),
        indexName, min, max, total, counts.getCounts().size());
    return false;
  }


  private class PrepareToReShardPartitions {
    private String dbName;
    private List<String> toRebalance;
    private String tableName;
    private long begin;
    private Map<String, long[]> partitionSizes;
    private Map<String, List<TableSchema.Partition>> copiedPartitionsToApply;
    private Map<String, List<TableSchema.Partition>> newPartitionsToApply;

    PrepareToReShardPartitions(String dbName, List<String> toRebalance, String tableName, long begin,
                               Map<String, long[]> partitionSizes,
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
        long[] currPartitionSizes = partitionSizes.get(index);

        long totalCount = 0;
        for (long size : currPartitionSizes) {
          totalCount += size;
        }
        long newPartitionSize = totalCount / databaseServer.getShardCount();

        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < databaseServer.getShardCount(); i++) {
          builder.append(",").append(currPartitionSizes[i]);
        }

        begin = System.currentTimeMillis();

        logger.info("master - calculating partitions: table={}, index={}, curSizes={}", tableName, indexName, builder);
        calculatePartitions(dbName, databaseServer.getShardCount(), newPartitions, indexName,
            tableSchema.getName(), currPartitionSizes, newPartitionSize, (localDbName, shard, localTableName, indexName1, offsets) ->
                getKeyAtOffset(localDbName, shard, localTableName, indexName1, offsets));
        logger.info("master - calculating partitions - finished: table={}, index={}, currSizes={}, duration={}sec",
            tableName, indexName, builder, System.currentTimeMillis() - begin / 1000d);

        Index dbIndex = databaseServer.getIndex(dbName, tableName, indexName);
        final Comparator[] comparators = dbIndex.getComparators();

        sortNewPartitions(newPartitions, comparators);

        TableSchema.Partition lastPartition = new TableSchema.Partition();
        newPartitions.add(lastPartition);
        lastPartition.setUnboundUpper(true);
        lastPartition.setShardOwning(databaseServer.getShardCount() - 1);

        prepPartitionsToApply(index, newPartitions, indexName, tableSchema);
      }
      return this;
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
            for (int i = 0; i < columnCount; i++) {
              copiedUpperKey[i] = upperKey[i];
            }
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
          if (o1.getUpperKey()[i] == null || o2.getUpperKey()[i] == null) {
            continue;
          }
          int value = comparators[i].compare(o1.getUpperKey()[i], o2.getUpperKey()[i]);
          if (value == 0) {
            continue;
          }
          return value;
        }
        return 0;
      });
    }

    private void calculatePartitions(final String dbName, int shardCount, final List<TableSchema.Partition> newPartitions,
                                            final String indexName, final String tableName, final long[] currPartitionSizes,
                                            long newPartitionSize, final GetKeyAtOffset getKey) {
      final List<Integer> nList = new ArrayList<>();
      final List<Long> offsetList = new ArrayList<>();

      GetNewPartitionSizes getNewPartitionSizes = new GetNewPartitionSizes(shardCount, currPartitionSizes, newPartitionSize).invoke();
      long[] newPartitionSizes = getNewPartitionSizes.getNewPartitionSizes();
      long[] currPartitionSumSizes = getNewPartitionSizes.getCurrPartitionSumSizes();
      int x = 0;
      int n = 0;
      int assignedShard = 0;

      assignShards(shardCount, newPartitions, nList, offsetList, newPartitionSizes, currPartitionSumSizes, x, n, assignedShard);

      setUpperKeys(dbName, newPartitions, indexName, tableName, getKey, nList, offsetList);
    }

    private void assignShards(int shardCount, List<TableSchema.Partition> newPartitions, List<Integer> nList,
                              List<Long> offsetList, long[] newPartitionSizes, long[] currPartitionSumSizes,
                              int x, int n, int assignedShard) {
      while (true) {
        if (x > shardCount - 1 || n > shardCount - 1) {
          break;
        }
        AssignShardForCalculatePartitions assignShardForCalculatePartitions = new AssignShardForCalculatePartitions(
            shardCount, newPartitions, nList, offsetList, newPartitionSizes, currPartitionSumSizes, x, n, assignedShard).invoke();
        x = assignShardForCalculatePartitions.getX();
        n = assignShardForCalculatePartitions.getN();
        assignedShard = assignShardForCalculatePartitions.getAssignedShard();
        if (assignShardForCalculatePartitions.is()) {
          break;
        }

        AssignShard assign = new AssignShard(newPartitionSizes, currPartitionSumSizes, n, x,
            offsetList, newPartitions, assignedShard, nList, shardCount).invoke();
        n = assign.n;
        x = assign.x;
        assignedShard = assign.assignedShard;
        if (assign.continueOuter) {
          continue;
        }
        if (assign.breakOuter) {
          break;
        }
      }
    }

    class AssignShard {
      private List<Integer> nList;
      private int shardCount;
      private long[] newPartitionSizes;
      private long[] currPartitionSumSizes;
      private int n;
      private int x;
      private List<Long> offsetList;
      private List<TableSchema.Partition> newPartitions;
      private int assignedShard;
      private boolean continueOuter;
      private boolean breakOuter;

      AssignShard(long[] newPartitionSizes, long[] currPartitionSumSizes, int n, int x, List<Long> offsetList,
                  List<TableSchema.Partition> newPartitions, int assignedShard, List<Integer> nList, int shardCount) {
        this.newPartitionSizes = newPartitionSizes;
        this.currPartitionSumSizes = currPartitionSumSizes;
        this.n = n;
        this.x = x;
        this.offsetList = offsetList;
        this.newPartitions = newPartitions;
        this.assignedShard = assignedShard;
        this.nList = nList;
        this.shardCount = shardCount;
      }

      public AssignShard invoke() {
        while (n <= shardCount - 1 && x <= shardCount - 1 && newPartitionSizes[x] >= currPartitionSumSizes[n]) {
          long currOffset = 0;
          AssignShardContinueAsNeeded assignShardContinueAsNeeded = new AssignShardContinueAsNeeded().invoke();
          currOffset = assignShardContinueAsNeeded.getCurrOffset();
          if (assignShardContinueAsNeeded.is()) {
            return this;
          }

          nList.add(n + 1);
          offsetList.add(currOffset);
          TableSchema.Partition partition = new TableSchema.Partition();
          newPartitions.add(partition);
          partition.setShardOwning(assignedShard++);
          if (assignedShard >= shardCount - 1) {
            breakOuter = true;
            return this;
          }
          x++;
          n++;
          continueOuter = true;
          return this;
        }
        return this;
      }

      private class AssignShardContinueAsNeeded {
        private boolean myResult;
        private long currOffset;

        boolean is() {
          return myResult;
        }

        public long getCurrOffset() {
          return currOffset;
        }

        public AssignShardContinueAsNeeded invoke() {
          if (n > 0) {
            currOffset = newPartitionSizes[x] - currPartitionSumSizes[n] - 1;
            if (currOffset >= currPartitionSumSizes[n] - currPartitionSumSizes[n - 1]) {
              n++;
              continueOuter = true;
              myResult = true;
              return this;
            }
            if (currOffset < 0) {
              n++;
              continueOuter = true;
              myResult = true;
              return this;
            }
          }
          else {
            currOffset = newPartitionSizes[x] - currPartitionSumSizes[0] - 1;
            if (currOffset >= currPartitionSumSizes[n]) {
              n++;
              continueOuter = true;
              myResult = true;
              return this;
            }
            if (currOffset < 0) {
              n++;
              continueOuter = true;
              myResult = true;
              return this;
            }
          }
          myResult = false;
          return this;
        }
      }
    }

    private void setUpperKeys(String dbName, List<TableSchema.Partition> newPartitions, String indexName,
                              String tableName, GetKeyAtOffset getKey, List<Integer> nList, List<Long> offsetList) {
      ThreadPoolExecutor executor = ThreadUtil.createExecutor(newPartitions.size(),
          "SonicBase CalculatePartitions Thread");
      try {
        Map<Integer, List<OffsetEntry>> shards = new HashMap<>();
        for (int i = 0; i < newPartitions.size(); i++) {
          int shard = nList.get(i);
          long offset = offsetList.get(i);
          List<OffsetEntry> offsets = shards.get(shard);
          if (offsets == null) {
            offsets = new ArrayList<>();
            shards.put(shard, offsets);
          }
          offsets.add(new OffsetEntry(offset, i));
          Collections.sort(offsets, Comparator.comparingLong(o -> o.offset));
        }
        List<Future> futures = new ArrayList<>();
        for (final Map.Entry<Integer, List<OffsetEntry>> entry : shards.entrySet()) {
          futures.add(executor.submit((Callable) () -> {
            List<Object[]> keys = getKey.getKeyAtOffset(dbName, entry.getKey(), tableName, indexName, entry.getValue());
            for (int i = 0; i < entry.getValue().size(); i++) {
              OffsetEntry currEntry = entry.getValue().get(i);
              TableSchema.Partition partition = newPartitions.get(currEntry.partitionOffset);
              partition.setUpperKey(keys.get(i));
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
      }
      finally {
        executor.shutdownNow();
      }
    }

    private List<Object[]> getKeyAtOffset(String dbName, int shard, String tableName, String indexName,
                                          List<OffsetEntry> offsets) throws IOException {
      logger.info("getKeyAtOffset: dbName={}, shard={}, table={}, index={}, offsetCount={}", dbName, shard, tableName,
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

      logger.info("getKeyAtOffset finished: dbName={}, shard={}, table={}, index={}, duration={}", dbName, shard,
          tableName, indexName, (System.currentTimeMillis() - localBegin));
      return keys;
    }

    private class AssignShardForCalculatePartitions {
      private boolean myResult;
      private int shardCount;
      private List<TableSchema.Partition> newPartitions;
      private List<Integer> nList;
      private List<Long> offsetList;
      private long[] newPartitionSizes;
      private long[] currPartitionSumSizes;
      private int x;
      private int n;
      private int assignedShard;

      public AssignShardForCalculatePartitions(int shardCount, List<TableSchema.Partition> newPartitions,
                                               List<Integer> nList, List<Long> offsetList, long[] newPartitionSizes,
                                               long[] currPartitionSumSizes, int x, int n, int assignedShard) {
        this.shardCount = shardCount;
        this.newPartitions = newPartitions;
        this.nList = nList;
        this.offsetList = offsetList;
        this.newPartitionSizes = newPartitionSizes;
        this.currPartitionSumSizes = currPartitionSumSizes;
        this.x = x;
        this.n = n;
        this.assignedShard = assignedShard;
      }

      boolean is() {
        return myResult;
      }

      public int getX() {
        return x;
      }

      public int getN() {
        return n;
      }

      public int getAssignedShard() {
        return assignedShard;
      }

      public AssignShardForCalculatePartitions invoke() {
        while (n <= shardCount - 1 && x <= shardCount - 1 && newPartitionSizes[x] < currPartitionSumSizes[n]) {
          int currN = n;
          long currOffset = getCurrOffset();

          nList.add(currN);
          offsetList.add(currOffset);
          TableSchema.Partition partition = new TableSchema.Partition();
          newPartitions.add(partition);
          partition.setShardOwning(assignedShard++);
          if (x < shardCount - 1 && newPartitionSizes[x + 1] > currPartitionSumSizes[n]) {
            n++;
          }
          x++;

          if (assignedShard >= shardCount - 1) {
            myResult = true;
            return this;
          }
        }
        myResult = false;
        return this;
      }

      private long getCurrOffset() {
        long currOffset;
        if (n > 0) {
          currOffset = newPartitionSizes[x] - currPartitionSumSizes[n - 1] - 1;
          if (currOffset == -1) {
            currOffset = 0;
          }
        }
        else {
          currOffset = newPartitionSizes[x] - 1;
          if (currOffset == -1) {
            currOffset = 0;
          }
        }
        return currOffset;
      }
    }

    private class GetNewPartitionSizes {
      private int shardCount;
      private long[] currPartitionSizes;
      private long newPartitionSize;
      private long[] newPartitionSizes;
      private long[] currPartitionSumSizes;

      public GetNewPartitionSizes(int shardCount, long[] currPartitionSizes, long newPartitionSize) {
        this.shardCount = shardCount;
        this.currPartitionSizes = currPartitionSizes;
        this.newPartitionSize = newPartitionSize;
      }

      public long[] getNewPartitionSizes() {
        return newPartitionSizes;
      }

      public long[] getCurrPartitionSumSizes() {
        return currPartitionSumSizes;
      }

      public GetNewPartitionSizes invoke() {
        long prev = 0;
        newPartitionSizes = new long[shardCount];
        for (int i = 0; i < newPartitionSizes.length; i++) {
          newPartitionSizes[i] = prev + newPartitionSize;
          prev = newPartitionSizes[i];
        }
        prev = 0;
        currPartitionSumSizes = new long[shardCount];
        for (int i = 0; i < currPartitionSumSizes.length; i++) {
          currPartitionSumSizes[i] = currPartitionSizes[i] + prev;
          prev = currPartitionSumSizes[i];
        }
        return this;
      }
    }
  }

  private class ProcessEntry {
    private Index index;
    private IndexSchema indexSchema;
    private AtomicInteger countDeleted;
    private MapEntry entry;
    private byte[][] content;
    private int shard;
    private List<Integer> selectedShards;

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
      Object newValue = databaseServer.getAddressMap().toUnsafeFromRecords(content);
      index.put(entry.key, newValue);
      databaseServer.getAddressMap().freeUnsafeIds(entry.value);
    }

    private void insertRecordForPrimaryKey() {
      for (int i = 0; i < content.length; i++) {
        Record.setDbViewFlags(content[i], (short) 0);
        Record.setDbViewNumber(content[i], 0);
      }
      Object newValue = databaseServer.getAddressMap().toUnsafeFromRecords(content);
      index.put(entry.key, newValue);
      databaseServer.getAddressMap().freeUnsafeIds(entry.value);
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
      Object newValue = databaseServer.getAddressMap().toUnsafeFromRecords(newContent);
      index.put(entry.key, newValue);
      databaseServer.getAddressMap().freeUnsafeIds(entry.value);
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
      Object newValue = databaseServer.getAddressMap().toUnsafeFromRecords(newContent);
      index.put(entry.key, newValue);
      databaseServer.getAddressMap().freeUnsafeIds(entry.value);
    }
  }
}
