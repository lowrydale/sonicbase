package com.sonicbase.index;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.*;
import com.sonicbase.query.BinaryExpression;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.query.impl.OrderByExpressionImpl;
import com.sonicbase.schema.DataType;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.Schema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.server.DatabaseServer;
import com.sonicbase.server.DeleteManager;
import com.sonicbase.socket.DeadServerException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.exception.ExceptionUtils;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Responsible for
 */
public class Repartitioner extends Thread {

  private static final String INDEX_STR = ", index=";
  private static final String NAME_STR = "name";
  private static Logger logger;


  private final DatabaseServer databaseServer;
  private final DatabaseCommon common;
  private final Map<String, Indices> indices;
  private Map<Integer, ShardState> stateIsShardRepartitioningComplete = new ConcurrentHashMap<>();
  private String stateTable = "none";
  private String stateIndex = "none";
  private RepartitionerState state = RepartitionerState.idle;
  private Exception shardRepartitionException;

  public enum RepartitionerState {
    idle,
    prep,
    rebalancing,
    complete,
  }

  static class ShardState {
    private int shard;
    private long count;
    private String exception;
    private boolean finished;

    public ShardState(long count, String exception, boolean finished) {
      this.count = count;
      this.exception = exception;
      this.finished = finished;
    }

    public ShardState() {

    }
  }

  private AtomicBoolean isRepartitioningIndex = new AtomicBoolean();

  private String currIndexRepartitioning;
  private String currTableRepartitioning;
  private int minSizeForRepartition = 0;//10000;
  private boolean shutdown;

  public Repartitioner(DatabaseServer databaseServer, DatabaseCommon common) {
    super("Repartitioner Thread");
    logger = new Logger(databaseServer.getDatabaseClient());
    this.databaseServer = databaseServer;
    this.common = common;
    this.indices = databaseServer.getIndices();
  }

  private Thread beginRepartitioningThread = null;
  private AtomicBoolean isComplete = new AtomicBoolean(true);

  public ComObject getRepartitionerState(ComObject cobj) {
    logger.info("getRepartitionerState - begin: state=" + state.name() + ", table=" + stateTable + ", index=" + stateIndex);
    ComObject retObj = new ComObject();
    retObj.put(ComObject.Tag.state, state.name());
    if (state == RepartitionerState.rebalancing) {
      retObj.put(ComObject.Tag.tableName, stateTable);
      retObj.put(ComObject.Tag.indexName, stateIndex);
      ComArray array = retObj.putArray(ComObject.Tag.shards, ComObject.Type.objectType);
      for (Map.Entry<Integer, ShardState> entry : stateIsShardRepartitioningComplete.entrySet()) {
        ComObject innerObj = new ComObject();
        innerObj.put(ComObject.Tag.shard, entry.getKey());
        innerObj.put(ComObject.Tag.countLong, entry.getValue().count);
        innerObj.put(ComObject.Tag.finished, entry.getValue().finished);
        if (entry.getValue().exception != null) {
          innerObj.put(ComObject.Tag.exception, entry.getValue().exception);
        }
        array.getArray().add(innerObj);
      }
    }
    return retObj;
  }

  public void setMinSizeForRepartition(int minSizeForRepartition) {
    this.minSizeForRepartition = minSizeForRepartition;
  }

  public byte[] beginRebalance(final String dbName, final List<String> toRebalance) {

    ThreadPoolExecutor executor = new ThreadPoolExecutor(databaseServer.getShardCount(), databaseServer.getShardCount(), 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
    try {
      while (!isComplete.compareAndSet(true, false)) {
        try {
          Thread.sleep(1000);
        }
        catch (InterruptedException e) {
          throw new DatabaseException(e);
        }
      }

      state = RepartitionerState.prep;

      long totalBegin = System.currentTimeMillis();

      //    beginRepartitioningThread = new Thread(new Runnable() {
      //      @Override
      //      public void run() {
      try {
        String tableName = null;
        StringBuilder toRebalanceStr = new StringBuilder();
        for (String index : toRebalance) {
          toRebalanceStr.append(index).append(", ");
        }
        logger.info("master - Rebalancing index group: group=" + toRebalanceStr);


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
            futures.add(executor.submit(new Callable() {
              @Override
              public Object call() throws Exception {
                currPartitionSizes[offset] = getPartitionSize(dbName, offset, currTableName, indexName);
                return null;
              }
            }));
          }
          for (Future future : futures) {
            future.get();
          }
          logger.info("master - getPartitionSize finished: table=" + tableName + ", index=" + indexName + ", duration=" + (System.currentTimeMillis() - begin) / 1000f + "sec");
          partitionSizes.put(index, currPartitionSizes);
        }


        Map<String, List<TableSchema.Partition>> copiedPartitionsToApply = new HashMap<>();
        Map<String, List<TableSchema.Partition>> newPartitionsToApply = new HashMap<>();

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

//          Long minSize = databaseServer.getConfig().getLong("minShardSizeForRebalance");
//          if (minSize == null) {
//            minSize = 1_000_000L;
//          }
//          if (newPartitionSize < minSize) {
//            return null;
//          }

          StringBuilder builder = new StringBuilder();
          for (int i = 0; i < databaseServer.getShardCount(); i++) {
            builder.append(",").append(currPartitionSizes[i]);
          }

          begin = System.currentTimeMillis();

          logger.info("master - calculating partitions: table=" + tableName + ", index=" + indexName + ", currSizes=" + builder.toString());
          calculatePartitions(dbName, databaseServer.getShardCount(), newPartitions, indexName,
              tableSchema.getName(), currPartitionSizes, newPartitionSize, new GetKeyAtOffset() {
                @Override
                public List<Object[]> getKeyAtOffset(String dbName, int shard, String tableName, String indexName, List<OffsetEntry> offsets) throws IOException {
                  return Repartitioner.this.getKeyAtOffset(dbName, shard, tableName, indexName, offsets);
                }
              });
          logger.info("master - calculating partitions - finished: table=" + tableName + ", index=" + indexName +
              ", currSizes=" + builder.toString() + ", duration=" + (System.currentTimeMillis() - begin) / 1000d + "sec");

          Index dbIndex = databaseServer.getIndices().get(dbName).getIndices().get(tableName).get(indexName);
          final Comparator[] comparators = dbIndex.getComparators();
          Collections.sort(newPartitions, new Comparator<TableSchema.Partition>() {
            @Override
            public int compare(TableSchema.Partition o1, TableSchema.Partition o2) {
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
            }
          });

          TableSchema.Partition lastPartition = new TableSchema.Partition();
          newPartitions.add(lastPartition);
          lastPartition.setUnboundUpper(true);
          lastPartition.setShardOwning(databaseServer.getShardCount() - 1);


          //todo: send new schema to clients so they start sending to new shards

          if (!tableSchema.getIndices().get(indexName).isPrimaryKey() && newPartitions.size() != 0) {
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

        //common.getSchemaWriteLock(dbName).lock();
        try {
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
            tableName = parts[0];
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

          }

          common.setSchema(dbName, schema);

          common.saveSchema(databaseServer.getClient(), databaseServer.getDataDir());
        }
        finally {
          //common.getSchemaWriteLock(dbName).unlock();
        }
        databaseServer.pushSchema();

        isRepartitioningIndex.set(true);

        Thread.sleep(1000);
        //          common.saveSchema(databaseServer.getDataDir());
        //          databaseServer.pushSchema();

        for (String index : toRebalance) {
          try {
            String[] parts = index.split(" ");
            tableName = parts[0];
            final String finalTableName = tableName;
            final String indexName = parts[1];

            this.stateTable = tableName;
            this.stateIndex = indexName;
            state = RepartitionerState.rebalancing;

            begin = System.currentTimeMillis();

            for (int i = 0; i < databaseServer.getShardCount(); i++) {
              stateIsShardRepartitioningComplete.put(i, new ShardState());
            }

            final int[] masters = new int[databaseServer.getShardCount()];
            logger.info("master - rebalance ordered index - begin: table=" + tableName + INDEX_STR + indexName);
            List<Future> futures = new ArrayList<>();
            for (int i = 0; i < databaseServer.getShardCount(); i++) {
              final int shard = i;
              logger.info("rebalance ordered index: shard=" + shard);
              futures.add(executor.submit(new Callable() {
                @Override
                public Object call() {
                  ComObject cobj = new ComObject();
                  cobj.put(ComObject.Tag.dbName, dbName);
                  cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
                  cobj.put(ComObject.Tag.tableName, finalTableName);
                  cobj.put(ComObject.Tag.indexName, indexName);
                  cobj.put(ComObject.Tag.method, "rebalanceOrderedIndex");
                  Random rand = new Random(System.currentTimeMillis());
                  try {
                    byte[] ret = databaseServer.getDatabaseClient().send(null, shard, rand.nextLong(),
                        cobj, DatabaseClient.Replica.master);
                    ComObject retObj = new ComObject(ret);
                    masters[shard] = retObj.getInt(ComObject.Tag.replica);
                  }
                  catch (Exception e) {
                    logger.error("Error sending rebalanceOrderedIndex to shard: shard=" + shard, e);
                  }
                  return null;
                }
              }));

            }

            for (Future future : futures) {
              future.get();
            }

            Map<Integer, Integer> countFailed = new HashMap<>();
            while (true) {
              boolean areAllComplete = true;
              for (int shard = 0; shard < databaseServer.getShardCount(); shard++) {
                try {
                  ComObject cobj = new ComObject();
                  cobj.put(ComObject.Tag.dbName, "__none__");
                  cobj.put(ComObject.Tag.schemaVersion, databaseServer.getCommon().getSchemaVersion());
                  cobj.put(ComObject.Tag.method, "isShardRepartitioningComplete");
                  byte[] bytes = databaseServer.getClient().send(null, shard, masters[shard], cobj, DatabaseClient.Replica.specified);
                  ComObject retObj = new ComObject(bytes);
                  long count = retObj.getLong(ComObject.Tag.countLong);
                  String exception = retObj.getString(ComObject.Tag.exception);
                  boolean finished = retObj.getBoolean(ComObject.Tag.finished);
                  stateIsShardRepartitioningComplete.put(shard, new ShardState(count, exception, finished));
                  if (!retObj.getBoolean(ComObject.Tag.isComplete)) {
                    areAllComplete = false;
                    break;
                  }
                }
                catch (Exception e) {
                  areAllComplete = false;
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
              if (areAllComplete) {
                //isRepartitioningIndex.set(false);
                break;
              }
              Thread.sleep(1000);
            }

            common.getTables(dbName).get(tableName).getIndices().get(indexName).deleteLastPartitions();

            logger.info("master - rebalance ordered index - finished: table=" + tableName + INDEX_STR + indexName +
                ", duration=" + (System.currentTimeMillis() - begin) / 1000d + "sec");

            logger.info("master - rebalance ordered index - end: table=" + tableName + INDEX_STR + indexName +
                ", duration=" + (System.currentTimeMillis() - totalBegin) / 1000d + "sec");
          }
          catch (Exception e) {
            logger.error("error rebalancing index: table=" + tableName + ", index=" + index +
                ", duration=" + (System.currentTimeMillis() - begin) / 1000d + "sec", e);
          }
        }

        common.saveSchema(databaseServer.getClient(), databaseServer.getDataDir());
        common.loadSchema(databaseServer.getDataDir());
        logger.info("master - Post-save schemaVersion=" + common.getSchemaVersion() + ", shard=" + common.getShard() +
            ", replica=" + common.getReplica());
        databaseServer.pushSchema();

        isRepartitioningIndex.set(false);
      }
      catch (Exception e) {
        logger.error("Error repartitioning", e);
        throw new DatabaseException(e);
      }
      finally {
        beginRepartitioningThread = null;
        isComplete.set(true);
        state = RepartitionerState.complete;
      }
    }
    finally {
      executor.shutdownNow();
    }
    return null;
  }

  public static class PartitionEntry {
    public int version;
    public TableSchema.Partition[] partitions;
  }

  public static ConcurrentHashMap<String, List<PartitionEntry>> previousPartitions = new ConcurrentHashMap<>();

  private boolean isShardRepartitioningComplete = true;
  private long countProcessed = 0;

  public ComObject isShardRepartitioningComplete(ComObject cobj, boolean replayedCommand) {
    ComObject retObj = new ComObject();
    retObj.put(ComObject.Tag.isComplete, isShardRepartitioningComplete);
    retObj.put(ComObject.Tag.countLong, countMoved.get());
    retObj.put(ComObject.Tag.finished, isShardRepartitioningComplete);
    if (shardRepartitionException != null) {
      retObj.put(ComObject.Tag.exception, ExceptionUtils.getFullStackTrace(shardRepartitionException));
    }
    return retObj;
  }

  public void stopShardsFromRepartitioning() {
    logger.info("stopShardsFromRepartitioning - begin");
    final ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.dbName, "__none__");
    cobj.put(ComObject.Tag.method, "stopRepartitioning");
    cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
    List<Future> futures = new ArrayList<>();
    for (int shard = 0; shard < databaseServer.getShardCount(); shard++) {
      for (int replica = 0; replica < databaseServer.getReplicationFactor(); replica++) {
        final int localShard = shard;
        final int localReplica = replica;
        futures.add(databaseServer.getExecutor().submit(new Callable() {
          @Override
          public Object call() throws Exception {
            try {
              databaseServer.getClient().send(null, localShard, localReplica, cobj, DatabaseClient.Replica.specified);
            }
            catch (Exception e) {
              logger.error("Error stopping repartitioning on server: shard=" + localShard + ", replica=" + localReplica);
            }
            return null;
          }
        }));
      }
    }
    for (Future future : futures) {
      try {
        future.get();
      }
      catch (InterruptedException e) {
        throw new DatabaseException(e);
      }
      catch (ExecutionException e) {
        e.printStackTrace();
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

  public static void calculatePartitions(final String dbName, int shardCount, final List<TableSchema.Partition> newPartitions,
                                         final String indexName, final String tableName, final long[] currPartitionSizes,
                                         long newPartitionSize, final GetKeyAtOffset getKey) throws IOException {
    final List<Integer> nList = new ArrayList<>();
    final List<Long> offsetList = new ArrayList<>();
    long prev = 0;
    long[] newPartitionSizes = new long[shardCount];
    for (int i = 0; i < newPartitionSizes.length; i++) {
      newPartitionSizes[i] = prev + newPartitionSize;
      prev = newPartitionSizes[i];
    }
    prev = 0;
    long[] currPartitionSumSizes = new long[shardCount];
    for (int i = 0; i < currPartitionSumSizes.length; i++) {
      currPartitionSumSizes[i] = currPartitionSizes[i] + prev;
      prev = currPartitionSumSizes[i];
    }
    int x = 0;
    int n = 0;
    int assignedShard = 0;
    outer:
    while (true) {
      if (x > shardCount - 1 || n > shardCount - 1) {
        break;
      }
      while (n <= shardCount - 1 && x <= shardCount - 1 && newPartitionSizes[x] < currPartitionSumSizes[n]) {
        {
          long currOffset = 0;
          int currN = n;
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
            break outer;
          }

        }
      }

      while (n <= shardCount - 1 && x <= shardCount - 1 && newPartitionSizes[x] >= currPartitionSumSizes[n]) {
        long currOffset = 0;
        if (n > 0) {
          currOffset = newPartitionSizes[x] - currPartitionSumSizes[n] - 1;
          if (currOffset >= currPartitionSumSizes[n] - currPartitionSumSizes[n - 1]) {
            n++;
            continue outer;
          }
          if (currOffset < 0) {
            n++;
            continue outer;
          }
        }
        else {
          currOffset = newPartitionSizes[x] - currPartitionSumSizes[0] - 1;
          if (currOffset >= currPartitionSumSizes[n]) {
            n++;
            continue outer;
          }
          if (currOffset < 0) {
            n++;
            continue outer;
          }
        }

        nList.add(n + 1);
        offsetList.add(currOffset);
        TableSchema.Partition partition = new TableSchema.Partition();
        newPartitions.add(partition);
        partition.setShardOwning(assignedShard++);
        if (assignedShard >= shardCount - 1) {
          break outer;
        }
        x++;
        n++;
        continue outer;
      }
    }

    ThreadPoolExecutor executor = new ThreadPoolExecutor(newPartitions.size(), newPartitions.size(), 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
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
        Collections.sort(offsets, new Comparator<OffsetEntry>() {
          @Override
          public int compare(OffsetEntry o1, OffsetEntry o2) {
            return Long.compare(o1.offset, o2.offset);
          }
        });
      }
      List<Future> futures = new ArrayList<>();
      for (final Map.Entry<Integer, List<OffsetEntry>> entry : shards.entrySet()) {
        futures.add(executor.submit(new Callable() {
          @Override
          public Object call() throws Exception {
            List<Object[]> keys = getKey.getKeyAtOffset(dbName, entry.getKey(), tableName, indexName, entry.getValue());
            for (int i = 0; i < entry.getValue().size(); i++) {
              OffsetEntry currEntry = entry.getValue().get(i);
              TableSchema.Partition partition = newPartitions.get(currEntry.partitionOffset);
              partition.setUpperKey(keys.get(i));
            }
            return null;
          }
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

  public static class OffsetEntry {
    long offset;
    int partitionOffset;

    public OffsetEntry(long offset, int partitionOffset) {
      this.offset = offset;
      this.partitionOffset = partitionOffset;
    }

    public long getOffset() {
      return offset;
    }

    public int getPartitionOffset() {
      return partitionOffset;
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
      builder.append("{shard=" + partition.getShardOwning() + ", upperKey=" + innerBuilder.toString() + ", unboundUpper=" + partition.isUnboundUpper() + "}");
    }
    logger.info("Applying new partitions: dbName=" + dbName + ", tableName=" + tableName + ", indexName=" + indexName + ", partitions=" + builder.toString());
  }


//  public byte[] isRepartitioningRecordsByIdComplete(String command, byte[] body) {
//    try {
//      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
//      DataOutputStream out = new DataOutputStream(bytesOut);
//      Varint.writeSignedVarLong(out, SnapshotManager.SERIALIZATION_VERSION);
//      boolean finished = isRepartitioningRecordsByIdComplete();
//      out.writeBoolean(finished);
//      return bytesOut.toByteArray();
//    }
//    catch (IOException e) {
//      throw new DatabaseException(e);
//    }
//  }
//
//  private boolean isRepartitioningRecordsByIdComplete() {
//    boolean finished = true;
//    for (AtomicInteger entry : repartitioningRecordsByIdComplete.values()) {
//      if (entry.get() > 0) {
//        finished = false;
//        break;
//      }
//    }
//    return finished;
//  }

  public ComObject isRepartitioningComplete(ComObject cobj) {
    ComObject retObj = new ComObject();
    retObj.put(ComObject.Tag.finished, !isRebalancing.get());
    return retObj;
  }

//  public byte[] isDeletingComplete(String command, byte[] body) {
//    try {
//      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
//      DataOutputStream out = new DataOutputStream(bytesOut);
//      Varint.writeSignedVarLong(out, SnapshotManager.SERIALIZATION_VERSION);
//      boolean finished = isDeletingComplete();
//      out.writeBoolean(finished);
//      return bytesOut.toByteArray();
//    }
//    catch (IOException e) {
//      throw new DatabaseException(e);
//    }
//  }


  private List<Object[]> getKeyAtOffset(String dbName, int shard, String tableName, String indexName, List<OffsetEntry> offsets) throws IOException {
    logger.info("getKeyAtOffset: dbName=" + dbName + ", shard=" + shard + ", table=" + tableName +
        ", index=" + indexName + ", offsetCount=" + offsets.size());
    long begin = System.currentTimeMillis();
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.dbName, dbName);
    cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
    cobj.put(ComObject.Tag.tableName, tableName);
    cobj.put(ComObject.Tag.indexName, indexName);
    cobj.put(ComObject.Tag.method, "getKeyAtOffset");
    ComArray array = cobj.putArray(ComObject.Tag.offsets, ComObject.Type.longType);
    for (OffsetEntry offset : offsets) {
      array.add(offset.offset);
    }
    byte[] ret = databaseServer.getDatabaseClient().send(null, shard, 0, cobj, DatabaseClient.Replica.master);

    if (ret == null) {
      throw new IllegalStateException("Key not found on shard: shard=" + shard + ", table=" + tableName + ", index=" + indexName);
    }

    ComObject retObj = new ComObject(ret);
    ComArray keyArray = retObj.getArray(ComObject.Tag.keys);
    List<Object[]> keys = new ArrayList<>();
    if (keyArray != null) {
      for (int i = 0; i < keyArray.getArray().size(); i++) {
        Object[] key = DatabaseCommon.deserializeKey(common.getTables(dbName).get(tableName), (byte[]) keyArray.getArray().get(i));
        keys.add(key);
      }
    }

    logger.info("getKeyAtOffset finished: dbName=" + dbName + ", shard=" + shard + ", table=" + tableName +
        ", index=" + indexName +
        ", duration=" + (System.currentTimeMillis() - begin));
    return keys;
  }

  public ComObject getKeyAtOffset(ComObject cobj) {
    try {
      String dbName = cobj.getString(ComObject.Tag.dbName);
      String tableName = cobj.getString(ComObject.Tag.tableName);
      String indexName = cobj.getString(ComObject.Tag.indexName);
      List<Long> offsets = new ArrayList<>();
      ComArray offsetsArray = cobj.getArray(ComObject.Tag.offsets);
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

      //
      //    Map.Entry<Object[], Long> entry = index.firstEntry();
      //    if (entry != null) {
      //      index.visitTailMap(entry.getKey(), new Index.Visitor() {
      //        @Override
      //        public boolean visit(Object[] key, long value) throws IOException {
      //          int count = 0;
      //          if (value >= 0 || indexSchema.isPrimaryKey()) {
      //            count = 1;
      //          }
      //          else {
      //            if (indexSchema.isPrimaryKey()) {
      //              synchronized (index.getMutex(key)) {
      //                byte[][] records = databaseServer.fromUnsafeToRecords(value);
      //                count = records.length;
      //              }
      //            }
      //            else {
      //              synchronized (index.getMutex(key)) {
      //                byte[][] ids = databaseServer.fromUnsafeToKeys(value);
      //                count = ids.length;
      //              }
      //            }
      //          }
      //          offset.addAndGet(count);
      //          if ((double) offset.get() / (double) currSize >= desiredPercent) {
      //            foundKey.set(key);
      //            return false;
      //          }
      //          return true;
      //        }
      //      });


      if (keys != null) {
        try {
          ComObject retObj = new ComObject();
          ComArray array = retObj.putArray(ComObject.Tag.keys, ComObject.Type.byteArrayType);
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
    //}
    return null;
  }

  private long getPartitionSize(String dbName, int shard, String tableName, String indexName) {
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.dbName, dbName);
    cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
    cobj.put(ComObject.Tag.tableName, tableName);
    cobj.put(ComObject.Tag.indexName, indexName);
    cobj.put(ComObject.Tag.method, "getPartitionSize");
    Random rand = new Random(System.currentTimeMillis());
    byte[] ret = databaseServer.getDatabaseClient().send(null, shard, rand.nextLong(),
        cobj, DatabaseClient.Replica.master);
    ComObject retObj = new ComObject(ret);
    return retObj.getLong(ComObject.Tag.size);
  }

  public ComObject getPartitionSize(ComObject cobj) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    String tableName = cobj.getString(ComObject.Tag.tableName);
    String indexName = cobj.getString(ComObject.Tag.indexName);

    if (dbName == null || tableName == null || indexName == null) {
      logger.error("getPartitionSize: parm is null: dbName=" + dbName + ", table=" + tableName + ", index=" + indexName);
    }
    //todo: really need to read all the records to get an accurate count
    Indices tables = databaseServer.getIndices(dbName);
    if (tables == null) {
      logger.error("getPartitionSize: tables is null");
      return null;
    }
    ConcurrentHashMap<String, Index> indices = tables.getIndices().get(tableName);
    if (indices == null) {
      logger.error("getPartitionSize: indices is null");
      return null;
    }
    Index index = indices.get(indexName);
    IndexSchema indexSchema = common.getTables(dbName).get(tableName).getIndexes().get(indexName);

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

    logger.info("getPartitionSize: dbName=" + dbName + ", table=" + tableName + ", index=" + indexName +
        ", minKey=" + databaseServer.getCommon().keyToString(minKey) + ", maxKey=" + databaseServer.getCommon().keyToString(maxKey) +
        ", size=" + size + ", rawSize=" + rawSize);

    ComObject retObj = new ComObject();
    retObj.put(ComObject.Tag.size, size);

    return retObj;
  }

  public void deleteIndexEntry(String tableName, String indexName, Object[] primaryKey) {
//    if (tableName.equals(currTableRepartitioning) && indexName.equals(currIndexRepartitioning)) {
//      ConcurrentHashMap<String, ConcurrentLinkedQueue<Object[]>> indicesToDelete = getIndicesToDeleteFrom(tableName, indexName);
//      ConcurrentLinkedQueue<Object[]> keysToDelete = indicesToDelete.get(indexName);
//      keysToDelete.add(primaryKey);
//    }
  }

  public boolean undeleteIndexEntry(String dbName, String tableName, String indexName, Object[] primaryKey, byte[] recordBytes) {
//    if (tableName.equals(currTableRepartitioning) && indexName.equals(currIndexRepartitioning)) {
//      Comparator[] comparators = databaseServer.getIndices(dbName).getIndices().get(tableName).get(indexName).getComparators();
//
//      ConcurrentHashMap<String, ConcurrentLinkedQueue<Object[]>> indicesToDelete = getIndicesToDeleteFrom(tableName, indexName);
//      ConcurrentLinkedQueue<Object[]> keysToDelete = indicesToDelete.get(indexName);
//      for (Object[] key : keysToDelete) {
//        if (0 == DatabaseCommon.compareKey(comparators, key, primaryKey)) {
//          keysToDelete.remove(key);
//          break;
//        }
//      }
//      return false;
//    }
//    else {
//      return true;
//    }
  return true;
 }

  public static class MoveRequest {
    private final boolean shouldDeleteNow;
    private Object[] key;
    private byte[][] content;

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "EI_EXPOSE_REP2", justification = "copying the passed in data is too slow")
    @SuppressWarnings("PMD.ArrayIsStoredDirectly") //copying the passed in data is too slow
    public MoveRequest(Object[] key, byte[][] value, boolean shouldDeleteNow) {
      this.key = key;
      this.content = value;
      this.shouldDeleteNow = shouldDeleteNow;
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "EI_EXPOSE_REP", justification = "copying the returned data is too slow")
    public Object[] getKey() {
      return key;
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "EI_EXPOSE_REP2", justification = "copying the passed in data is too slow")
    @SuppressWarnings("PMD.ArrayIsStoredDirectly") //copying the passed in data is too slow
    public void setKey(Object[] key) {
      this.key = key;
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "EI_EXPOSE_REP", justification = "copying the returned data is too slow")
    public byte[][] getContent() {
      return content;
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "EI_EXPOSE_REP2", justification = "copying the passed in data is too slow")
    @SuppressWarnings("PMD.ArrayIsStoredDirectly") //copying the passed in data is too slow
    public void setContent(byte[][] content) {
      this.content = content;
    }
  }

  private ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentLinkedQueue<Object[]>>> entriesToDelete = new ConcurrentHashMap<>();
  private String tableToDeleteEntriesFrom = null;

//  private Index addedAfter;

//  public Index getAddedAfter() {
//    return addedAfter;
//  }

//  public void notifyAdded(Object[] key, String tableName, String indexName) {
//    if (tableName.equals(currTableRepartitioning) && indexName.equals(currIndexRepartitioning)) {
//      Index after = addedAfter;
//      if (after != null) {
//        after.put(key, 0);
//      }
//    }
//  }

  public ComObject rebalanceOrderedIndex(ComObject cobj) {
    isShardRepartitioningComplete = false;

    cobj.put(ComObject.Tag.method, "doRebalanceOrderedIndex");
    databaseServer.getLongRunningCommands().addCommand(
        databaseServer.getLongRunningCommands().createSingleCommand(cobj.serialize()));
    ComObject retObj = new ComObject();
    retObj.put(ComObject.Tag.replica, databaseServer.getReplica());
    return retObj;
  }

  static class MapEntry {
    Object[] key;
    Object value;

    public MapEntry(Object[] key, Object value) {
      this.key = key;
      this.value = value;
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

    public MoveProcessor(String dbName, String tableName, String indexName, boolean isPrimaryKey,
                         Index index, ConcurrentLinkedQueue<Object[]> keysToDelete, int shard) {
      this.dbName = dbName;
      this.tableName = tableName;
      this.indexName = indexName;
      this.isPrimaryKey = isPrimaryKey;
      this.index = index;
      this.keysToDelete = keysToDelete;
      this.shard = shard;
      this.executor = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors(),
          Runtime.getRuntime().availableProcessors(), 10000, TimeUnit.MILLISECONDS,
          new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
    }


    public void shutdown() {
      this.shutdown = true;
      thread.interrupt();
      executor.shutdownNow();
    }

    public void start() {
      thread = new Thread(new Runnable() {
        @Override
        public void run() {
          while (!shutdown) {
            try {
              final MoveRequestList list = queue.poll(30000, TimeUnit.MILLISECONDS);
              if (list == null) {
                continue;
              }
              countStarted.incrementAndGet();
              executor.submit(new Runnable() {
                @Override
                public void run() {
                  try {
                    final List<Object> toFree = new ArrayList<>();
                    long begin = System.currentTimeMillis();
                    moveIndexEntriesToShard(dbName, tableName, indexName, isPrimaryKey, shard, list.moveRequests);
                    for (MoveRequest request : list.moveRequests) {
                      if (request.shouldDeleteNow) {
                        synchronized (index.getMutex(request.key)) {
                          if (isPrimaryKey) {
                            byte[][] content = databaseServer.fromUnsafeToRecords(index.get(request.key));
                            if (content != null) {
                              for (byte[] bytes : content) {
                                if (Record.DB_VIEW_FLAG_DELETING != Record.getDbViewFlags(bytes)) {
                                  index.addAndGetCount(-1);
                                }
                              }
                            }
                          }
                          else {
                            byte[][] content = databaseServer.fromUnsafeToKeys(index.get(request.key));
                            if (content != null) {
                              for (byte[] bytes : content) {
                                if (Record.DB_VIEW_FLAG_DELETING != KeyRecord.getDbViewFlags(bytes)) {
                                  index.addAndGetCount(-1);
                                }
                              }
                            }
                          }
                          Object value = index.remove(request.key);
                          if (value != null) {
                            //toFree.add(value);
                            databaseServer.freeUnsafeIds(value);
                          }
                        }
                      }
                      else {
                        keysToDelete.add(request.getKey());
                      }
                    }
                    try {
                      Thread.sleep(1000);
                    }
                    catch (InterruptedException e) {
                      throw new DatabaseException(e);
                    }
//                    Timer timer = new Timer("Free memory");
//                    timer.schedule(new TimerTask(){
//                      @Override
//                      public void run() {
                    for (Object obj : toFree) {
                      databaseServer.freeUnsafeIds(obj);
                    }
//                      }
//                    }, 30 * 1000);

                    logger.info("moved entries: table=" + tableName + ", index=" + indexName + ", count=" + list.moveRequests.size() +
                        ", shard=" + shard + ", duration=" + (System.currentTimeMillis() - begin));
                  }
                  catch (Exception e) {
                    logger.error("Error moving entries", e);
                    shardRepartitionException = e;
                  }
                  finally {
                    countFinished.incrementAndGet();
                    list.latch.countDown();
                  }
                }
              });
            }
            catch (InterruptedException e) {
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
        }
      }, "MoveProcessor - shard=" + shard);
      thread.start();
    }

    public void await() {
      while (!shutdown) {
        if (queue.size() == 0 && countStarted.get() == countFinished.get()) {
          break;
        }
        try {
          Thread.sleep(50);
        }
        catch (InterruptedException e) {
          break;
        }
      }
    }
  }

  public ComObject stopRepartitioning(final ComObject cobj) {
    logger.info("stopRepartitioning: shard=" + databaseServer.getShard() + ", replica=" + databaseServer.getReplica());
    if (moveProcessors != null) {
      for (MoveProcessor processor : moveProcessors) {
        processor.shutdown();
      }
    }
    return null;
  }

  private MoveProcessor[] moveProcessors = null;

  public ComObject doRebalanceOrderedIndex(final ComObject cobj) {
    isShardRepartitioningComplete = false;
    countMoved.set(0);
    shardRepartitionException = null;


    final String dbName = cobj.getString(ComObject.Tag.dbName);
    try {
      final String tableName = cobj.getString(ComObject.Tag.tableName);
      final String indexName = cobj.getString(ComObject.Tag.indexName);
      logger.info("doRebalanceOrderedIndex: shard=" + databaseServer.getShard() + ", dbName=" + dbName +
          ", tableName=" + tableName + ", indexName=" + indexName);

      final Index index = indices.get(dbName).getIndices().get(tableName).get(indexName);
      final TableSchema tableSchema = common.getTables(dbName).get(tableName);
      final IndexSchema indexSchema = tableSchema.getIndices().get(indexName);

      long begin = System.currentTimeMillis();

//      common.getSchemaReadLock(dbName).lock();
//      try {
      currTableRepartitioning = tableName;
      currIndexRepartitioning = indexName;

      tableToDeleteEntriesFrom = tableName;

      final ConcurrentLinkedQueue<Object[]> keysToDelete = new ConcurrentLinkedQueue<>();

      String[] indexFields = indexSchema.getFields();
      final int[] fieldOffsets = new int[indexFields.length];
      for (int i = 0; i < indexFields.length; i++) {
        fieldOffsets[i] = tableSchema.getFieldOffset(indexFields[i]);
      }
      Map.Entry<Object[], Object> entry = index.firstEntry();
      if (entry != null) {
        final AtomicLong countVisited = new AtomicLong();
        final ThreadPoolExecutor executor = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors() * 8,
            Runtime.getRuntime().availableProcessors() * 8, 10000, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
        final AtomicInteger countSubmitted = new AtomicInteger();
        final AtomicInteger countFinished = new AtomicInteger();
        moveProcessors = new MoveProcessor[databaseServer.getShardCount()];
        for (int i = 0; i < moveProcessors.length; i++) {
          moveProcessors[i] = new MoveProcessor(dbName, tableName, indexName, indexSchema.isPrimaryKey(), index, keysToDelete, i);
          moveProcessors[i].start();
        }
        try {
          TableSchema.Partition currPartition = indexSchema.getCurrPartitions()[databaseServer.getShard()];
          //if (currPartition.getUpperKey() != null) {
          final AtomicReference<ArrayList<MapEntry>> currEntries = new AtomicReference<>(new ArrayList<MapEntry>());
          try {
            if (databaseServer.getShard() > 0) {
              TableSchema.Partition lowerPartition = indexSchema.getCurrPartitions()[databaseServer.getShard() - 1];
              if (lowerPartition.getUpperKey() != null) {
                Object value = index.get(lowerPartition.getUpperKey());
                if (value != null) {
                  doProcessEntry(lowerPartition.getUpperKey(), value, countVisited, currEntries, countSubmitted, executor, tableName, indexName, index, indexSchema, dbName, fieldOffsets, tableSchema, cobj, countFinished);
                }

                index.visitHeadMap(lowerPartition.getUpperKey(), new Index.Visitor() {
                  @Override
                  public boolean visit(Object[] key, Object value) throws IOException {
                    doProcessEntry(key, value, countVisited, currEntries, countSubmitted, executor, tableName, indexName, index, indexSchema, dbName, fieldOffsets, tableSchema, cobj, countFinished);
                    return true;
                  }
                });
                if (currEntries.get() != null && currEntries.get().size() != 0) {
                  try {
                    logger.info("doProcessEntries: table=" + tableName + ", index=" + indexName + ", count=" + currEntries.get().size());
                    doProcessEntries(moveProcessors, -1, tableName, indexName, currEntries.get(), index, indexSchema, dbName, fieldOffsets, tableSchema, cobj);
                  }
                  catch (Exception e) {
                    shardRepartitionException = e;
                    throw e;
                  }
                  finally {
                    logger.info("doProcessEntries - finished: table=" + tableName + ", index=" + indexName + ", count=" + currEntries.get().size() +
                        ", duration=" + (System.currentTimeMillis() - begin));
                  }
                }
              }
            }
            //if (databaseServer.getShard() < databaseServer.getShardCount() - 1) {
              //if (currPartition.getUpperKey() != null) {
            Object[] upperKey = currPartition.getUpperKey();
            if (upperKey == null) {
              upperKey = index.lastEntry().getKey();
            }
                index.visitTailMap(upperKey, new Index.Visitor() {
                  @Override
                  public boolean visit(Object[] key, Object value) throws IOException {
                    countVisited.incrementAndGet();
                    countProcessed = countVisited.get();
                    currEntries.get().add(new MapEntry(key, value));
                    if (currEntries.get().size() >= 10000 * databaseServer.getShardCount()) {
                      final List<MapEntry> toProcess = currEntries.get();
                      currEntries.set(new ArrayList<MapEntry>());
                      countSubmitted.incrementAndGet();
                      if (countSubmitted.get() > 2) {
                        databaseServer.setThrottleInsert(true);
                      }
                      executor.submit(new Runnable() {
                        @Override
                        public void run() {
                          long begin = System.currentTimeMillis();
                          try {
                            logger.info("doProcessEntries: table=" + tableName + ", index=" + indexName + ", count=" + toProcess.size());
                            doProcessEntries(moveProcessors, 1, tableName, indexName, toProcess, index, indexSchema, dbName, fieldOffsets, tableSchema, cobj);
                          }
                          catch (Exception e) {
                            shardRepartitionException = e;
                            logger.error("Error moving entries", e);
                          }
                          finally {
                            countFinished.incrementAndGet();
                            logger.info("doProcessEntries - finished: table=" + tableName + ", index=" + indexName + ", count=" + toProcess.size() +
                                ", duration=" + (System.currentTimeMillis() - begin));
                          }
                        }
                      });
                    }
                    return true;
                  }
                });
                if (currEntries.get() != null && currEntries.get().size() != 0) {
                  try {
                    logger.info("doProcessEntries: table=" + tableName + ", index=" + indexName + ", count=" + currEntries.get().size());
                    doProcessEntries(moveProcessors, 1, tableName, indexName, currEntries.get(), index, indexSchema, dbName, fieldOffsets, tableSchema, cobj);
                  }
                  catch (Exception e) {
                    shardRepartitionException = e;
                    throw e;
                  }
                  finally {
                    logger.info("doProcessEntries - finished: table=" + tableName + ", index=" + indexName + ", count=" + currEntries.get().size() +
                        ", duration=" + (System.currentTimeMillis() - begin));
                  }
                }
              //}
            //}

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

          //}
          logger.info("doProcessEntries - all finished: table=" + tableName + ", index=" + indexName +
              ", count=" + countVisited.get() + ", countToDelete=" + keysToDelete.size());
        }
        finally {
          for (MoveProcessor moveProcessor : moveProcessors) {
            if (moveProcessor != null) {
              moveProcessor.await();
              moveProcessor.shutdown();
            }
          }
          //databaseServer.getDeleteManager().saveStandardDeletes(dbName, tableName, indexName, keysToDelete);
          deleteRecordsOnOtherReplicas(dbName, tableName, indexName, keysToDelete);
          executor.shutdownNow();
          logger.info("doRebalanceOrderedIndex finished: table=" + tableName + ", index=" + indexName + ", countVisited=" + countVisited.get() +
              ", duration=" + (System.currentTimeMillis() - begin));
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

  private void doProcessEntry(Object[] key, Object value, AtomicLong countVisited, AtomicReference<ArrayList<MapEntry>> currEntries, AtomicInteger countSubmitted, ThreadPoolExecutor executor, final String tableName, final String indexName, final Index index, final IndexSchema indexSchema, final String dbName, final int[] fieldOffsets, final TableSchema tableSchema, final ComObject cobj, final AtomicInteger countFinished) {
    countVisited.incrementAndGet();
    countProcessed = countVisited.get();
    currEntries.get().add(new MapEntry(key, value));
    if (currEntries.get().size() >= 50000 * databaseServer.getShardCount()) {
      final List<MapEntry> toProcess = currEntries.get();
      currEntries.set(new ArrayList<MapEntry>());
      countSubmitted.incrementAndGet();
      if (countSubmitted.get() > 2) {
        databaseServer.setThrottleInsert(true);
      }
      executor.submit(new Runnable() {
        @Override
        public void run() {
          long begin = System.currentTimeMillis();
          try {
            logger.info("doProcessEntries: table=" + tableName + ", index=" + indexName + ", count=" + toProcess.size());
            doProcessEntries(moveProcessors, -1, tableName, indexName, toProcess, index, indexSchema, dbName, fieldOffsets, tableSchema, cobj);
          }
          catch (Exception e) {
            shardRepartitionException = e;
            logger.error("Error moving entries", e);
          }
          finally {
            countFinished.incrementAndGet();
            logger.info("doProcessEntries - finished: table=" + tableName + ", index=" + indexName + ", count=" + toProcess.size() +
                ", duration=" + (System.currentTimeMillis() - begin));
          }
        }
      });
    }
  }

  private void deleteRecordsOnOtherReplicas(final String dbName, String tableName, String indexName,
                                            ConcurrentLinkedQueue<Object[]> keysToDelete) {

    ThreadPoolExecutor executor = new ThreadPoolExecutor(16, 16, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
    List<Future> futures = new ArrayList<>();
    try {
      int count = 0;
      TableSchema tableSchema = common.getTables(dbName).get(tableName);
      ComObject cobj = new ComObject();
      ComArray keys = cobj.putArray(ComObject.Tag.keys, ComObject.Type.byteArrayType);
      int batchSize = (int) Math.nextUp((double)keysToDelete.size() / 16d);
      for (Object[] key : keysToDelete) {
        keys.add(DatabaseCommon.serializeKey(tableSchema, indexName, key));
        if (keys.getArray().size() > batchSize) {
          count += keys.getArray().size();

          cobj.put(ComObject.Tag.dbName, dbName);
          cobj.put(ComObject.Tag.method, "deleteMovedRecords");
          cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
          cobj.put(ComObject.Tag.tableName, tableName);
          cobj.put(ComObject.Tag.indexName, indexName);

          final ComObject currObj = cobj;
          cobj = new ComObject();
          keys = cobj.putArray(ComObject.Tag.keys, ComObject.Type.byteArrayType);

          sendDeletes(executor, currObj, futures);
          logger.info("delete moved entries progress: submittedCount=" + count);
        }
      }
      if (keys.getArray().size() > 0) {
        cobj.put(ComObject.Tag.dbName, dbName);
        cobj.put(ComObject.Tag.method, "deleteMovedRecords");
        cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
        cobj.put(ComObject.Tag.tableName, tableName);
        cobj.put(ComObject.Tag.indexName, indexName);

        sendDeletes(executor, cobj, futures);
        logger.info("delete moved entries progress: submittedCount=" + count);
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
          logger.info("delete moved entries progress: finishedCount=" + count);
        }
        catch (Exception e) {
          logger.error("Error deleting moved records on replica", e);
        }
      }
      executor.shutdownNow();
    }
  }

  public void sendDeletes(ThreadPoolExecutor executor, final ComObject currObj, List<Future> futures) {
    int replicaCount = databaseServer.getReplicationFactor();
    for (int i = 0; i < replicaCount; i++) {
      final int replica = i;
      //        if (replica == databaseServer.getReplica()) {
      //          continue;
      //        }
      futures.add(executor.submit(new Callable() {
        @Override
        public Object call() throws Exception {

          if (false && replica == databaseServer.getReplica()) {
            deleteMovedRecords(currObj, false);
            return currObj.getArray(ComObject.Tag.keys).getArray().size();
          }
          else {
            databaseServer.getDatabaseClient().send(null, databaseServer.getShard(), replica,
                currObj, DatabaseClient.Replica.specified);
            return currObj.getArray(ComObject.Tag.keys).getArray().size();
          }
        }
      }));
    }
  }

  public ComObject deleteMovedRecords(ComObject cobj, boolean replayedCommand) {
    try {
      ConcurrentLinkedQueue<DeleteManager.DeleteRequest> keysToDelete = new ConcurrentLinkedQueue<>();
      final ConcurrentLinkedQueue<DeleteManager.DeleteRequest> keysToDeleteExpanded = new ConcurrentLinkedQueue<>();
      final long sequence0 = cobj.getLong(ComObject.Tag.sequence0);
      final long sequence1 = cobj.getLong(ComObject.Tag.sequence1);
      String dbName = cobj.getString(ComObject.Tag.dbName);
      String tableName = cobj.getString(ComObject.Tag.tableName);
      String indexName = cobj.getString(ComObject.Tag.indexName);
      TableSchema tableSchema = common.getTables(dbName).get(tableName);
      final IndexSchema indexSchema = tableSchema.getIndexes().get(indexName);
      ComArray keys = cobj.getArray(ComObject.Tag.keys);
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
      final AtomicInteger count = new AtomicInteger();
      final List<Object> toFree = new ArrayList<>();
      final Index index = databaseServer.getIndices().get(dbName).getIndices().get(tableName).get(indexName);
      if (replayedCommand) {
        List<Future> futures = new ArrayList<>();
        for (final DeleteManager.DeleteRequest request : keysToDelete) {
          futures.add(databaseServer.getExecutor().submit(new Callable(){
            @Override
            public Object call() throws Exception {
              doDeleteMovedEntry(keysToDeleteExpanded, indexSchema, index, request);
              if (count.incrementAndGet() % 100000 == 0) {
                logger.info("deleteMovedRecords progress: count=" + count.get());
              }
              return null;
            }
          }));
        }
        for (Future future : futures) {
          future.get();
        }
      }
      else {
        for (DeleteManager.DeleteRequest request : keysToDelete) {
          doDeleteMovedEntry(keysToDeleteExpanded, indexSchema, index, request);
          if (count.incrementAndGet() % 100000 == 0) {
            logger.info("deleteMovedRecords progress: count=" + count.get());
          }
        }
      }
//      Timer timer = new Timer("Free memory");
//      timer.schedule(new TimerTask(){
//        @Override
//        public void run() {
      for (Object obj : toFree) {
        databaseServer.freeUnsafeIds(obj);
      }

      if (indexSchema.isPrimaryKey()) {
        databaseServer.getDeleteManager().saveDeletesForRecords(dbName, tableName, indexName, sequence0, sequence1, keysToDeleteExpanded);
      }
      else {
        databaseServer.getDeleteManager().saveDeletesForKeyRecords(dbName, tableName, indexName, sequence0, sequence1, keysToDeleteExpanded);
      }

//        }
//      }, 30 * 1000);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
    return null;
  }

  private void doDeleteMovedEntry(ConcurrentLinkedQueue<DeleteManager.DeleteRequest> keysToDeleteExpanded,
                                  IndexSchema indexSchema, Index index, DeleteManager.DeleteRequest request) {
    synchronized (index.getMutex(request.getKey())) {
//          Object toFree = index.remove(key);
//          if (toFree != null) {
//            //  toFreeBatch.add(toFree);
//            databaseServer.freeUnsafeIds(toFree);
//          }
      Object value = index.get(request.getKey());
      byte[][] content = null;
      if (value != null) {
        if (indexSchema.isPrimaryKey()) {
          content = databaseServer.fromUnsafeToRecords(value);
        }
        else {
          content = databaseServer.fromUnsafeToKeys(value);
        }
      }
      if (content != null) {
        if (indexSchema.isPrimaryKey()) {
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
          //toFree.add(value);
          Object newValue = databaseServer.toUnsafeFromRecords(newContent);
          index.put(request.getKey(), newValue);
          databaseServer.freeUnsafeIds(value);
        }
        else {
          byte[][] newContent = new byte[content.length][];
          for (int i = 0; i < content.length; i++) {
            if (Record.DB_VIEW_FLAG_DELETING != KeyRecord.getDbViewFlags(content[i])) {
              index.addAndGetCount(-1);
            }
            KeyRecord.setDbViewFlags(content[i], Record.DB_VIEW_FLAG_DELETING);
            KeyRecord.setDbViewNumber(content[i], common.getSchemaVersion());
            newContent[i] = content[i];

            keysToDeleteExpanded.add(new DeleteManager. DeleteRequestForKeyRecord(request.getKey(), KeyRecord.getPrimaryKey(content[i])));
          }
          //toFree.add(value);
          Object newValue = databaseServer.toUnsafeFromRecords(newContent);
          index.put(request.getKey(), newValue);
          databaseServer.freeUnsafeIds(value);
        }
      }
    }
  }

  class MoveRequestList {
    List<MoveRequest> moveRequests;
    CountDownLatch latch = new CountDownLatch(1);

    public MoveRequestList(List<MoveRequest> list) {
      this.moveRequests = list;
    }
  }

  private void doProcessEntries(MoveProcessor[] moveProcessors, int shardOffset, final String tableName, final String indexName,
                                List<MapEntry> toProcess, final Index index, final IndexSchema indexSchema, final String dbName,
                                int[] fieldOffsets, TableSchema tableSchema, ComObject cobj) {
    final Map<Integer, List<MoveRequest>> moveRequests = new HashMap<>();
    for (int i = 0; i < databaseServer.getShardCount(); i++) {
      moveRequests.put(i, new ArrayList<MoveRequest>());
    }

    List<MoveRequestList> lists = new ArrayList<>();

    int count = 0;
    int consecutiveErrors = 0;
    final List<Object> toFree = new ArrayList<>();
    int lockCount = 0;
    AtomicInteger countDeleted = new AtomicInteger();
    try {
      for (MapEntry entry : toProcess) {
        try {
          if (lockCount == 0) {
            //databaseServer.getThrottleReadLock().lock();
          }
          if (lockCount++ % 2 == 0) {
            //databaseServer.getThrottleReadLock().unlock();
            lockCount = 0;
          }
          byte[][] content = null;
          int shard = 0;
          List<Integer> selectedShards = findOrderedPartitionForRecord(true,
              false, fieldOffsets, common, tableSchema, indexName,
              null, BinaryExpression.Operator.equal, null,
              entry.key, null);
          synchronized (index.getMutex(entry.key)) {
            entry.value = index.get(entry.key);
            if (entry.value != null) {
              if (indexSchema.isPrimaryKey()) {
                content = databaseServer.fromUnsafeToRecords(entry.value);
              }
              else {
                content = databaseServer.fromUnsafeToKeys(entry.value);
              }
            }
            if (content != null) {
              shard = /*databaseServer.getShard() + shardOffset;// */selectedShards.get(0);
              if (shard != databaseServer.getShard()) {
                if (indexSchema.isPrimaryKey()) {
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
                  Object newValue = databaseServer.toUnsafeFromRecords(newContent);
                  index.put(entry.key, newValue);
                  databaseServer.freeUnsafeIds(entry.value);
                }
                else {
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
                  //toFree.add(entry.value);
                  Object newValue = databaseServer.toUnsafeFromRecords(newContent);
                  index.put(entry.key, newValue);
                  databaseServer.freeUnsafeIds(entry.value);
                }
              }
              else {
                if (indexSchema.isPrimaryKey()) {
                  for (int i = 0; i < content.length; i++) {
                    Record.setDbViewFlags(content[i], (short) 0);
                    Record.setDbViewNumber(content[i], 0);// common.getSchemaVersion() - 2);
                  }
                  //toFree.add(entry.value);
                  Object newValue = databaseServer.toUnsafeFromRecords(content);
                  index.put(entry.key, newValue);
                  databaseServer.freeUnsafeIds(entry.value);

                }
                else {
                  for (int i = 0; i < content.length; i++) {
                    KeyRecord.setDbViewFlags(content[i], (short) 0);
                    KeyRecord.setDbViewNumber(content[i], 0);// common.getSchemaVersion() - 2);
                  }
                  //toFree.add(entry.value);
                  Object newValue = databaseServer.toUnsafeFromRecords(content);
                  index.put(entry.key, newValue);
                  databaseServer.freeUnsafeIds(entry.value);
                }
                content = null;
              }
            }
          }

          if (content != null) {
            final List<MoveRequest> list = moveRequests.get(shard);
            boolean shouldDeleteNow = false;
            if (indexSchema.isPrimaryKey()) {
              long dbViewFlags = Record.getDbViewFlags(content[0]);
              if (dbViewFlags == Record.DB_VIEW_FLAG_DELETING) {
                //shouldDeleteNow = true;
              }
            }
            list.add(new MoveRequest(entry.key, content, shouldDeleteNow));
            if (list.size() > 50000) {
//              for (Object obj : toFree) {
//                databaseServer.freeUnsafeIds(obj);
//              }
//              toFree.clear();

              moveRequests.put(shard, new ArrayList<MoveRequest>());
              MoveRequestList requestList = new MoveRequestList(list);
              lists.add(requestList);
              moveProcessors[shard].queue.put(requestList);
            }
          }
          consecutiveErrors = 0;
          count++;
        }
        catch (Exception t) {

          if (consecutiveErrors++ > 50) {
            throw new DatabaseException("Error moving record: table=" + cobj.getString(ComObject.Tag.tableName) +
                INDEX_STR + cobj.getString(ComObject.Tag.indexName) + ", key=" + DatabaseCommon.keyToString(entry.key), t);
          }
          logger.error("Error moving record: table=" + cobj.getString(ComObject.Tag.tableName) +
              INDEX_STR + cobj.getString(ComObject.Tag.indexName) + ", key=" + DatabaseCommon.keyToString(entry.key), t);
        }
      }
    }
    finally {
      if (lockCount != 0) {
        //databaseServer.getThrottleReadLock().unlock();
      }
    }

    try {
//      for (Object obj : toFree) {
//        databaseServer.freeUnsafeIds(obj);
//      }

      for (int i = 0; i < databaseServer.getShardCount(); i++) {
        final int shard = i;
        final List<MoveRequest> list = moveRequests.get(i);
        if (list.size() != 0) {
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
        throw new DatabaseException();
      }
    }
  }

  private ConcurrentHashMap<String, ConcurrentLinkedQueue<Object[]>> getIndicesToDeleteFrom(String tableName, String indexName) {
    if (!entriesToDelete.containsKey(tableName)) {
      entriesToDelete.put(tableName, new ConcurrentHashMap<String, ConcurrentLinkedQueue<Object[]>>());
    }
    ConcurrentHashMap<String, ConcurrentLinkedQueue<Object[]>> indicesToDelete = entriesToDelete.get(tableName);
    if (!indicesToDelete.containsKey(indexName)) {
      indicesToDelete.put(indexName, new ConcurrentLinkedQueue<Object[]>());
    }
    return indicesToDelete;
  }

//  public byte[] deleteMovedIndexEntries(String command, final byte[] body) {
//    command = command.replace(":deleteMovedIndexEntries:", ":doDeleteMovedIndexEntries:");
//    databaseServer.getLongRunningCommands().addCommand(databaseServer.getLongRunningCommands().createSingleCommand(command, body));
//    return null;
//  }
//
//  public byte[] doDeleteMovedIndexEntries(final String command, final byte[] body) {
//    ThreadPoolExecutor executor = new ThreadPoolExecutor(16, 16, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
//    try {
//      long begin = System.currentTimeMillis();
//      String[] parts = command.split(":");
//      String dbName = parts[4];
//      String tableName = parts[5];
//      String indexName = parts[6];
//      final AtomicInteger freedCount = new AtomicInteger();
//      logger.info("doDeleteMovedIndexEntries: shard=" + databaseServer.getShard() + ", dbName=" + dbName +
//        ", tableName=" + tableName + ", indexName=" + indexName);
//      try {
//        currTableRepartitioning = null;
//        currIndexRepartitioning = null;
//
//        final Index index = indices.get(dbName).getIndices().get(tableName).get(indexName);
//        if (entriesToDelete.get(tableName) != null) {
//          ConcurrentLinkedQueue<Object[]> keys = entriesToDelete.get(tableName).get(indexName);
//          logger.info("Deleting moved index entries: table=" + tableToDeleteEntriesFrom + INDEX_STR + indexName + ", count=" + keys.size());
//          if (keys.size() != 0) {
//            List<Future> futures = new ArrayList<>();
//
//            int offset = 0;
//            Map<Integer, List<Object[]>> byMutex = new HashMap<>();
//            for (Object[] key : keys) {
//              //Object mutex = index.getMutex(key);
//              List<Object[]> list = byMutex.get(offset % 100);
//              if (list == null) {
//                list = new ArrayList<>();
//                byMutex.put(offset % 100, list);
//              }
//              list.add(key);
//              offset++;
//            }
//
//            for (final List<Object[]> list : byMutex.values()) {
//              futures.add(executor.submit(new Callable(){
//                @Override
//                public Object call() throws Exception {
//                  int offset = 0;
//                  List<Object> toFree = new ArrayList<>();
//                  outer:
//                  while (offset < list.size()) {
//                    //synchronized (index.getMutex(list.get(offset))) {//index.getMutex(list.get(0))) {
//                      for (int i = 0; i < 100000; i++) {
//                        if (offset >= list.size()) {
//                          break outer;
//                        }
//                        Object obj = index.remove(list.get(offset));
//                        if (obj != null) {
//                          freedCount.incrementAndGet();
//                          toFree.add(obj);
//                        }
//                        offset++;
//                      }
//                    //}
//                  }
//
//                  Thread.sleep(1000);
//
//                  for (Object address : toFree) {
//                    if (address != null) {
//                      databaseServer.freeUnsafeIds(address);
//                    }
//                  }
//                  return null;
//                }
//              }));
//            }
//            for (Future future : futures) {
//              future.get();
//            }
//
//            keys.clear();
//
//            tableToDeleteEntriesFrom = null;
//
//            logger.info("Deleting moved index entries from index - finished: table=" +
//                tableName + ", shard=" + databaseServer.getShard() +
//                ", freedCount=" + freedCount.get() +
//                ", replica=" + databaseServer.getReplica() + ", duration=" + (System.currentTimeMillis() - begin) / 1000f + "sec");
//          }
//        }
//      }
//      catch (Exception e) {
//        logger.error("Error deleting moved index entries", e);
//      }
//      String notifyCommand = "DatabaseServer:notifyDeletingComplete:1:" + common.getSchemaVersion() + ":" + dbName + ":" + databaseServer.getShard() + ":" + databaseServer.getReplica();
//      Random rand = new Random(System.currentTimeMillis());
//      databaseServer.getDatabaseClient().send(null, 0, rand.nextLong(), notifyCommand, null, DatabaseClient.Replica.master);
//    }
//    catch (Exception e) {
//      logger.error("Error rebalancing index", e);
//    }
//    finally {
//      executor.shutdownNow();
//    }
//    return null;
//  }

  private AtomicLong countMoved = new AtomicLong();

  private void moveIndexEntriesToShard(
      String dbName, String tableName, String indexName, boolean primaryKey, int shard, List<MoveRequest> moveRequests) {
    int count = 0;
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.dbName, dbName);
    cobj.put(ComObject.Tag.tableName, tableName);
    cobj.put(ComObject.Tag.indexName, indexName);
    cobj.put(ComObject.Tag.method, "moveIndexEntries");
    cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
    ComArray keys = cobj.putArray(ComObject.Tag.keys, ComObject.Type.objectType);
    int consecutiveErrors = 0;
    for (MoveRequest moveRequest : moveRequests) {
      try {
        count++;
        byte[] bytes = DatabaseCommon.serializeKey(common.getTables(dbName).get(tableName), indexName, moveRequest.key);
        ComObject innerObj = new ComObject();
        keys.add(innerObj);
        innerObj.remove(ComObject.Tag.serializationVersion);
        innerObj.put(ComObject.Tag.keyBytes, bytes);

        byte[][] content = moveRequest.getContent();
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
        ComArray records = innerObj.putArray(ComObject.Tag.records, ComObject.Type.byteArrayType);
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
        logger.error("Error moving record: key=" + DatabaseCommon.keyToString(moveRequest.key) +
            ", destShard=" + shard, e);
      }
    }
    databaseServer.getDatabaseClient().send(null, shard, 0, cobj, DatabaseClient.Replica.def);
    countMoved.addAndGet(count);
  }

  public ComObject moveIndexEntries(ComObject cobj, boolean replayedCommand) {
    try {
        databaseServer.getBatchRepartCount().incrementAndGet();
        String dbName = cobj.getString(ComObject.Tag.dbName);

        String tableName = cobj.getString(ComObject.Tag.tableName);
        String indexName = cobj.getString(ComObject.Tag.indexName);
        ComArray keys = cobj.getArray(ComObject.Tag.keys);
        List<MoveRequest> moveRequests = new ArrayList<>();
        if (keys != null) {
          logger.info("moveIndexEntries: table=" + tableName + ", index=" + indexName + ", count=" + keys.getArray().size());
          int lockCount = 0;
          try {
            for (int i = 0; i < keys.getArray().size(); i++) {
              if (lockCount == 0) {
                //databaseServer.getThrottleWriteLock().lock();
              }
              if (lockCount++ == 2) {
                //databaseServer.getThrottleWriteLock().unlock();
                lockCount = 0;
              }
              ComObject keyObj = (ComObject) keys.getArray().get(i);
              Object[] key = DatabaseCommon.deserializeKey(common.getTables(dbName).get(tableName), keyObj.getByteArray(ComObject.Tag.keyBytes));
              ComArray records = keyObj.getArray(ComObject.Tag.records);
              if (records != null) {
                byte[][] content = new byte[records.getArray().size()][];
                for (int j = 0; j < content.length; j++) {
                  content[j] = (byte[]) records.getArray().get(j);
                }
                moveRequests.add(new MoveRequest(key, content, false));
              }
            }
          }
          finally {
            if (lockCount != 0) {
              //databaseServer.getThrottleWriteLock().unlock();
            }
          }
        }
        TableSchema tableSchema = common.getTables(dbName).get(tableName);
        Index index = databaseServer.getIndices(dbName).getIndices().get(tableSchema.getName()).get(indexName);
        IndexSchema indexSchema = common.getTables(dbName).get(tableName).getIndices().get(indexName);
        databaseServer.getUpdateManager().doInsertKeys(dbName, moveRequests, index, tableName, indexSchema, replayedCommand);

      return null;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
    finally {
      databaseServer.getBatchRepartCount().decrementAndGet();
    }
  }

  public static List<Integer> findOrderedPartitionForRecord(
      boolean includeCurrPartitions, boolean includeLastPartitions, int[] fieldOffsets,
      DatabaseCommon common, TableSchema tableSchema, String indexName,
      List<OrderByExpressionImpl> orderByExpressions,
      BinaryExpression.Operator leftOperator,
      BinaryExpression.Operator rightOperator,
      Object[] leftKey, Object[] rightKey) {
    boolean ascending = true;
    if (orderByExpressions != null && orderByExpressions.size() != 0) {
      OrderByExpressionImpl expression = orderByExpressions.get(0);
      String columnName = expression.getColumnName();
      if (expression.getTableName() == null || !expression.getTableName().equals(tableSchema.getName()) ||
          columnName.equals(tableSchema.getIndices().get(indexName).getFields()[0])) {
        ascending = expression.isAscending();
      }
    }

    IndexSchema specifiedIndexSchema = tableSchema.getIndexes().get(indexName);
    Comparator[] comparators = specifiedIndexSchema.getComparators();
//    if (specifiedIndexSchema.isPrimaryKeyGroup()) {
//      for (Map.Entry<String, IndexSchema> findEntry : tableSchema.getIndices().entrySet()) {
//        if (findEntry.getValue().getFields().length == 1) {
//          indexName = findEntry.getKey();
//          comparators =findEntry.getValue().getComparators();
//          break;
//        }
//      }
//    }


    //synchronized (common.getSchema().getSchemaLock()) {


    List<Integer> ret = new ArrayList<>();

    List<Integer> selectedPartitions = new ArrayList<>();
    if (includeCurrPartitions) {
      TableSchema.Partition[] partitions = tableSchema.getIndices().get(indexName).getCurrPartitions();
      if (rightOperator == null) {
        doSelectPartitions(partitions, tableSchema, indexName, leftOperator, comparators, leftKey,
            ascending, ret);
      }
      else {
        doSelectPartitions(partitions, tableSchema, indexName, leftOperator, comparators, leftKey,
            rightKey, ascending, ret);
      }
    }

    if (includeLastPartitions) {
      List<Integer> selectedLastPartitions = new ArrayList<>();
      TableSchema.Partition[] lastPartitions = tableSchema.getIndices().get(indexName).getLastPartitions();
      if (lastPartitions != null) {
        if (rightOperator == null) {
          doSelectPartitions(lastPartitions, tableSchema, indexName, leftOperator, comparators, leftKey,
              ascending, selectedLastPartitions);
        }
        else {
          doSelectPartitions(lastPartitions, tableSchema, indexName, leftOperator, comparators, leftKey,
              rightKey, ascending, selectedLastPartitions);
        }
        for (int partitionOffset : selectedLastPartitions) {
          selectedPartitions.add(lastPartitions[partitionOffset].getShardOwning());
        }
        for (int partitionOffset : selectedLastPartitions) {
          int shard = lastPartitions[partitionOffset].getShardOwning();
          boolean found = false;
          for (int currShard : ret) {
            if (currShard == shard) {
              found = true;
              break;
            }
          }
          if (!found) {
            ret.add(shard);
          }
        }
      }

    }

    return ret;
    //}
  }

  private static void doSelectPartitions(
      TableSchema.Partition[] partitions, TableSchema tableSchema, String indexName,
      BinaryExpression.Operator operator, Comparator[] comparators, Object[] key,
      boolean ascending, List<Integer> selectedPartitions) {

    if (key == null) {
      if (ascending) {
        for (int i = 0; i < partitions.length; i++) {
          selectedPartitions.add(i);
        }
      }
      else {
        for (int i = partitions.length - 1; i >= 0; i--) {
          selectedPartitions.add(i);
        }
      }
      return;
    }

    if (operator == BinaryExpression.Operator.equal) {

      TableSchema.Partition partitionZero = partitions[0];
      if (partitionZero.getUpperKey() == null) {
        selectedPartitions.add(0);
        return;
      }

      for (int i = 0; i < partitions.length - 1; i++) {
        int compareValue = 0;
        //for (int j = 0; j < fieldOffsets.length; j++) {

        for (int k = 0; k < key.length; k++) {
          if (key[k] == null || partitions[0].getUpperKey()[k] == null) {
            continue;
          }
          int value = comparators[k].compare(key[k], partitions[i].getUpperKey()[k]);
          if (value < 0) {
            compareValue = -1;
            break;
          }
          if (value > 0) {
            compareValue = 1;
            break;
          }
        }

        if (i == 0 && compareValue == -1 || compareValue == 0) {
          selectedPartitions.add(i);
        }

        int compareValue2 = 0;
        if (partitions[i + 1].getUpperKey() == null) {
          if (compareValue == 1 || compareValue == 0) {
            selectedPartitions.add(i + 1);
          }
        }
        else {
          for (int k = 0; k < key.length; k++) {
            if (key[k] == null || partitions[0].getUpperKey()[k] == null) {
              continue;
            }
            int value = comparators[k].compare(key[k], partitions[i + 1].getUpperKey()[k]);
            if (value < 0) {
              compareValue2 = -1;
              break;
            }
            if (value > 0) {
              compareValue2 = 1;
              break;
            }
          }
          if ((compareValue == 1 || compareValue == 0) && compareValue2 == -1) {
            selectedPartitions.add(i + 1);
          }
        }
      }
      return;
    }

    //todo: do a binary search
    outer:
    for (int i = !ascending ? partitions.length - 1 : 0; (!ascending ? i >= 0 : i < partitions.length); i += (!ascending ? -1 : 1)) {
      Object[] lowerKey = partitions[i].getUpperKey();
      if (lowerKey == null) {


        if (i == 0 || (!ascending ? i == 0 : i == partitions.length - 1)) {
          selectedPartitions.add(i);
          break;
        }
        Object[] lowerLowerKey = partitions[i - 1].getUpperKey();
        if (lowerLowerKey == null) {
          continue;
        }
        String[] indexFields = tableSchema.getIndices().get(indexName).getFields();
        Object[] tempLowerKey = new Object[indexFields.length];
        for (int j = 0; j < indexFields.length; j++) {
          //int offset = tableSchema.getFieldOffset(indexFields[j]);
          tempLowerKey[j] = lowerLowerKey[j];
        }
        int compareValue = 0;
        //for (int j = 0; j < fieldOffsets.length; j++) {

        for (int k = 0; k < key.length; k++) {
          int value = comparators[k].compare(key[k], tempLowerKey[k]);
          if (value < 0) {
            compareValue = -1;
            break;
          }
          if (value > 0) {
            compareValue = 1;
            break;
          }
        }
        if (compareValue == 0) {
          if (operator == BinaryExpression.Operator.greater) {
            continue outer;
          }
        }
        //}
        if (compareValue == 1) {// && (operator == BinaryExpression.Operator.less || operator == BinaryExpression.Operator.lessEqual)) {
          selectedPartitions.add(i);
        }
        if (compareValue == -1 && (operator == BinaryExpression.Operator.greater || operator == BinaryExpression.Operator.greaterEqual)) {
          selectedPartitions.add(i);
        }
        if (ascending) {
          break;
        }
        continue;
      }

      String[] indexFields = tableSchema.getIndices().get(indexName).getFields();
      Object[] tempLowerKey = new Object[indexFields.length];
      for (int j = 0; j < indexFields.length; j++) {
        //int offset = tableSchema.getFieldOffset(indexFields[j]);
        tempLowerKey[j] = lowerKey[j];
      }

      int compareValue = 0;
      //for (int j = 0; j < fieldOffsets.length; j++) {

      for (int k = 0; k < comparators.length; k++) {
        int value = comparators[k].compare(key[k], tempLowerKey[k]);
        if (value < 0) {
          compareValue = -1;
          break;
        }
        if (value > 0) {
          compareValue = 1;
          break;
        }
      }
      if (compareValue == 0) {
        if (operator == BinaryExpression.Operator.greater) {
          continue outer;
        }
      }
      //}
      if (compareValue == 1 &&
          (operator == BinaryExpression.Operator.less ||
              operator == BinaryExpression.Operator.lessEqual)) {
        selectedPartitions.add(i);
      }
      if (compareValue == -1 || compareValue == 0 || i == partitions.length - 1) {
        selectedPartitions.add(i);
        if (operator == BinaryExpression.Operator.equal) {
          return;
        }
        continue outer;
      }
    }
  }

  private static void doSelectPartitions(
      TableSchema.Partition[] partitions, TableSchema tableSchema, String indexName,
      BinaryExpression.Operator leftOperator,
      Comparator[] comparators, Object[] leftKey,
      Object[] rightKey, boolean ascending, List<Integer> selectedPartitions) {
    //todo: do a binary search

    BinaryExpression.Operator greaterOp = leftOperator;
    Object[] greaterKey = leftKey;
    Object[] lessKey = rightKey;
    if (greaterOp == BinaryExpression.Operator.less ||
        greaterOp == BinaryExpression.Operator.lessEqual) {
      greaterKey = rightKey;
      lessKey = leftKey;
    }

    outer:
    for (int i = !ascending ? partitions.length - 1 : 0; (!ascending ? i >= 0 : i < partitions.length); i += (!ascending ? -1 : 1)) {
      if (partitions[i].isUnboundUpper()) {
        selectedPartitions.add(i);
        if (ascending) {
          break;
        }
      }
      Object[] lowerKey = partitions[i].getUpperKey();
      if (lowerKey == null) {
        continue;
      }
      String[] indexFields = tableSchema.getIndices().get(indexName).getFields();
      Object[] tempLowerKey = new Object[indexFields.length];
      for (int j = 0; j < indexFields.length; j++) {
        //int offset = tableSchema.getFieldOffset(indexFields[j]);
        tempLowerKey[j] = lowerKey[j];
      }

      int greaterCompareValue = getCompareValue(comparators, greaterKey, tempLowerKey);
      //int lessCompareValue = getCompareValue(comparators, lessKey, tempLowerKey);

      if (greaterCompareValue == -1 || greaterCompareValue == 0) {
        if (i == 0) {
          selectedPartitions.add(i);
        }
        else {
          int lessCompareValue2 = getCompareValue(comparators, lessKey, partitions[i - 1].getUpperKey());
          if (lessCompareValue2 == 1) {
            selectedPartitions.add(i);
          }
        }
      }
    }
  }

  private static int getCompareValue(
      Comparator[] comparators, Object[] leftKey, Object[] tempLowerKey) {
    int compareValue = 0;
    for (int k = 0; k < leftKey.length; k++) {
      int value = comparators[k].compare(leftKey[k], tempLowerKey[k]);
      if (value < 0) {
        compareValue = -1;
        break;
      }
      if (value > 0) {
        compareValue = 1;
        break;
      }
    }
    return compareValue;
  }

public static class IndexCounts {
  private ConcurrentHashMap<Integer, Long> counts = new ConcurrentHashMap<>();

  public ConcurrentHashMap<Integer, Long> getCounts() {
    return counts;
  }
}

public static class TableIndexCounts {
  private ConcurrentHashMap<String, IndexCounts> indices = new ConcurrentHashMap<>();

  public ConcurrentHashMap<String, IndexCounts> getIndices() {
    return indices;
  }
}

public static class GlobalIndexCounts {
  private ConcurrentHashMap<String, TableIndexCounts> tables = new ConcurrentHashMap<>();

  public ConcurrentHashMap<String, TableIndexCounts> getTables() {
    return tables;
  }

}

  public ComObject getIndexCounts(ComObject cobj) {
    try {
      String dbName = cobj.getString(ComObject.Tag.dbName);

      logger.info("getIndexCounts - begin: dbName=" + dbName);

      ComObject retObj = new ComObject();
      ComArray tables = retObj.putArray(ComObject.Tag.tables, ComObject.Type.objectType);
      for (Map.Entry<String, ConcurrentHashMap<String, Index>> entry : databaseServer.getIndices(dbName).getIndices().entrySet()) {
        String tableName = entry.getKey();
        ComObject tableObj = new ComObject();
        tables.add(tableObj);
        tableObj.remove(ComObject.Tag.serializationVersion);
        tableObj.put(ComObject.Tag.tableName, tableName);
        ComArray indices = tableObj.putArray(ComObject.Tag.indices, ComObject.Type.objectType);
        logger.info("getIndexCounts: dbName=" + dbName + ", table=" + tableName + ", indexCount=" + entry.getValue().entrySet().size());
        for (Map.Entry<String, Index> indexEntry : entry.getValue().entrySet()) {
          ComObject indexObject = new ComObject();
          indexObject.remove(ComObject.Tag.serializationVersion);
          indices.add(indexObject);
          String indexName = indexEntry.getKey();
          indexObject.put(ComObject.Tag.indexName, indexName);
          Index index = indexEntry.getValue();
          long size = index.size();
          indexObject.put(ComObject.Tag.size, size);
          logger.info("getIndexCounts: dbName=" + dbName + ", table=" + tableName + ", index=" + indexName + ", count=" + size);
        }
      }

      return retObj;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public static GlobalIndexCounts getIndexCounts(final String dbName, final DatabaseClient client) {
    try {
      final GlobalIndexCounts ret = new GlobalIndexCounts();
      List<Future> futures = new ArrayList<>();
      for (int i = 0; i < client.getShardCount(); i++) {
        final int shard = i;
        futures.add(client.getExecutor().submit(new Callable() {
          @Override
          public Object call() throws Exception {
            ComObject cobj = new ComObject();
            cobj.put(ComObject.Tag.dbName, dbName);
            cobj.put(ComObject.Tag.schemaVersion, client.getCommon().getSchemaVersion());
            cobj.put(ComObject.Tag.method, "getIndexCounts");
            byte[] response = client.send(null, shard, 0, cobj, DatabaseClient.Replica.master);
            synchronized (ret) {
              ComObject retObj = new ComObject(response);
              ComArray tables = retObj.getArray(ComObject.Tag.tables);
              if (tables != null) {
                for (int i = 0; i < tables.getArray().size(); i++) {
                  ComObject tableObj = (ComObject) tables.getArray().get(i);
                  String tableName = tableObj.getString(ComObject.Tag.tableName);

                  TableIndexCounts tableIndexCounts = ret.tables.get(tableName);
                  if (tableIndexCounts == null) {
                    tableIndexCounts = new TableIndexCounts();
                    ret.tables.put(tableName, tableIndexCounts);
                  }
                  ComArray indices = tableObj.getArray(ComObject.Tag.indices);
                  if (indices != null) {
                    for (int j = 0; j < indices.getArray().size(); j++) {
                      ComObject indexObj = (ComObject) indices.getArray().get(j);
                      String indexName = indexObj.getString(ComObject.Tag.indexName);
                      long size = indexObj.getLong(ComObject.Tag.size);
                      IndexCounts indexCounts = tableIndexCounts.indices.get(indexName);
                      if (indexCounts == null) {
                        indexCounts = new IndexCounts();
                        tableIndexCounts.indices.put(indexName, indexCounts);
                      }
                      indexCounts.counts.put(shard, size);
                    }
                  }
                }
              }
              return null;
            }
          }
        }));

      }
      for (Future future : futures) {
        future.get();
      }
      for (Map.Entry<String, TableIndexCounts> entry : ret.tables.entrySet()) {
        for (Map.Entry<String, IndexCounts> indexEntry : entry.getValue().indices.entrySet()) {
          for (int i = 0; i < client.getShardCount(); i++) {
            Long count = indexEntry.getValue().counts.get(i);
            if (count == null) {
              indexEntry.getValue().counts.put(i, 0L);
              count = 0L;
            }
          }
        }
      }
      return ret;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private boolean isRunning = false;

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
        return;
      }
      while (!shutdown) {

        boolean ok = false;
        for (int shard = 0; shard < databaseServer.getShardCount(); shard++) {
          ok = false;
          for (int replica = 0; replica < databaseServer.getReplicationFactor(); replica++) {
            try {
              AtomicBoolean isHealthy = new AtomicBoolean();
              databaseServer.checkHealthOfServer(shard, replica, isHealthy, true);
              if (isHealthy.get()) {
                ok = true;
                break;
              }
            }
            catch (Exception e) {
              e.printStackTrace();
            }
          }
          if (!ok) {
            break;
          }
        }
        if (!ok) {
          try {
            Thread.sleep(2000);
          }
          catch (InterruptedException e) {
            throw new DatabaseException(e);
          }
          continue;
        }

        try {
          for (String dbName : databaseServer.getDbNames(databaseServer.getDataDir())) {
            ComObject cobj = new ComObject();
            cobj.put(ComObject.Tag.dbName, dbName);
            cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
            cobj.put(ComObject.Tag.method, "beginRebalance");
            cobj.put(ComObject.Tag.force, false);
            String command = "DatabaseServer:ComObject:beginRebalance:";
            beginRebalance(cobj);
          }
          Thread.sleep(2000);
        }
        catch (InterruptedException e) {
          break;
        }
        catch (Exception t) {
          if (-1 != ExceptionUtils.indexOfThrowable(t, InterruptedException.class)) {
            break;
          }
          logger.error("Error in master thread", t);
          try {
            Thread.sleep(2 * 1000);
          }
          catch (InterruptedException e) {
            logger.info("Repartitioner interrupted");
            return;
          }
        }
      }
    }
    finally {
      isRunning = false;
    }
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
      builder.append("{ shard=" + partition.getShardOwning() + ", upperKey=" + innerBuilder.toString() + ", unboundUpper=" + partition.isUnboundUpper() + "}");
    }
    logger.info("Current partitions to consider: dbName=" + dbName + ", tableName=" + tableName + ", indexName=" + indexName + ", partitions=" + builder.toString());
  }

  public AtomicBoolean isRebalancing = new AtomicBoolean();

  public ComObject beginRebalance(ComObject cobj) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    boolean force = cobj.getBoolean(ComObject.Tag.force);
    try {

      while (isRebalancing.get()) {
        Thread.sleep(2000);
      }
      isRebalancing.set(true);

      File file = new File(System.getProperty("user.dir"), "config/config-" + databaseServer.getCluster() + ".json");
      if (!file.exists()) {
        file = new File(System.getProperty("user.dir"), "src/main/resources/config/config-" + databaseServer.getCluster() + ".json");
      }
      String configStr = IOUtils.toString(new BufferedInputStream(new FileInputStream(file)), "utf-8");
      ObjectMapper mapper = new ObjectMapper();
      ObjectNode config = (ObjectNode) mapper.readTree(configStr);

      boolean isInternal = false;
      if (config.has("clientIsPrivate")) {
        isInternal = config.get("clientIsPrivate").asBoolean();
      }

      DatabaseServer.ServersConfig newConfig = new DatabaseServer.ServersConfig(databaseServer.getCluster(),
          config.withArray("shards"), config.withArray("shards").get(0).withArray("replicas").size(), isInternal);
      DatabaseServer.Shard[] newShards = newConfig.getShards();

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
      logger.info("Repartitioner: shardCount=" + newShards.length);
      databaseServer.setShardCount(newShards.length);
      databaseServer.getDatabaseClient().configureServers();
      databaseServer.pushServersConfig();

      Map<String, TableSchema> tables = common.getTables(dbName);
      if (tables == null) {
        return null;
      }
      for (TableSchema table : tables.values()) {
        for (IndexSchema index : table.getIndexes().values()) {
          logCurrPartitions(dbName, table.getName(), index.getName(), index.getCurrPartitions());
        }
      }

      List<String> toRebalance = new ArrayList<>();
      List<List<String>> indexGroups = new ArrayList<>();
      GlobalIndexCounts counts = Repartitioner.getIndexCounts(dbName, databaseServer.getDatabaseClient());
      for (Map.Entry<String, TableIndexCounts> entry : counts.tables.entrySet()) {
        String primaryKeyIndex = null;
        List<String> primaryKeyGroupIndices = new ArrayList<>();
        List<String> otherIndices = new ArrayList<>();
        TableSchema tableSchema = common.getTables(dbName).get(entry.getKey());
        if (tableSchema == null) {
          logger.error("beginRebalance, unknown table: name=" + entry.getKey());
          continue;
        }
        for (Map.Entry<String, IndexCounts> indexEntry : entry.getValue().indices.entrySet()) {
          IndexSchema indexSchema = tableSchema.getIndices().get(indexEntry.getKey());
          if (indexSchema == null) {
            logger.error("beginRebalance, unknown index: table=" + entry.getKey() + ", index=" + indexEntry.getKey());
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
        IndexCounts currCounts = entry.getValue().indices.get(primaryKeyIndex);
        toRebalance = new ArrayList<>();
        if (addToRebalance(toRebalance, entry, primaryKeyIndex, currCounts, force)) {
          for (int i = 0; i < primaryKeyGroupIndices.size(); i++) {
            addToRebalance(toRebalance, entry, primaryKeyGroupIndices.get(i), currCounts, true);
          }
        }
        if (toRebalance.size() != 0) {
          indexGroups.add(toRebalance);
        }
        for (int i = 0; i < otherIndices.size(); i++) {
          toRebalance = new ArrayList<>();
          addToRebalance(toRebalance, entry, otherIndices.get(i), currCounts, force);
          indexGroups.add(toRebalance);
        }
      }

      for (List<String> group : indexGroups) {
        if (group.size() == 0) {
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
          logger.error("Error rebalancing index group: group=" + builder.toString(), e);
        }
      }
      System.out.println("Finished rebalance");
      return null;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
    finally {
      isRebalancing.set(false);
    }
  }

  private boolean addToRebalance(
      List<String> toRebalance, Map.Entry<String, TableIndexCounts> entry,
      String indexName, IndexCounts counts, boolean force) {
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    long total = 0;
    for (Map.Entry<Integer, Long> countEntry : counts.counts.entrySet()) {
      long count = countEntry.getValue();
      if (count < min) {
        min = count;
      }
      if (count > max) {
        max = count;
      }
      total += count;
    }
    if (total < minSizeForRepartition) {//40000000) { ////
      logger.info("Not adding toRebalance: table=" + entry.getKey() + ", index=" + indexName +
          ", min=" + min + ", max=" + max + ", total=" + total + ", shardCount=" + counts.counts.size());
      return false;
    }
    if (force || (double) min / (double) max < 0.90) {
      toRebalance.add(entry.getKey() + " " + indexName);
      logger.info("Adding toRebalance: table=" + entry.getKey() + ", index=" + indexName);
      return true;
    }
    logger.info("Not adding toRebalance: table=" + entry.getKey() + ", index=" + indexName +
        ", min=" + min + ", max=" + max + ", total=" + total + ", shardCount=" + counts.counts.size());
    return false;
  }
}
