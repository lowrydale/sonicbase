package com.sonicbase.index;

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
import com.sonicbase.server.SnapshotManager;
import com.sonicbase.util.DataUtil;
import com.sonicbase.util.JsonDict;
import com.sonicbase.util.StreamUtils;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.yaml.snakeyaml.tokens.Token.ID.Tag;

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
  private Map<Integer, Boolean> repartitioningComplete = new ConcurrentHashMap<>();
  private Map<String, Boolean> deletingComplete = new ConcurrentHashMap<>();
  private Map<Integer, AtomicInteger> repartitioningRecordsByIdComplete = new ConcurrentHashMap<>();
  private AtomicBoolean isRepartitioningIndex = new AtomicBoolean();

  private String currIndexRepartitioning;
  private String currTableRepartitioning;
  private int minSizeForRepartition = 10000;

  public Repartitioner(DatabaseServer databaseServer, DatabaseCommon common) {
    super("Repartitioner Thread");
    logger = new Logger(databaseServer.getDatabaseClient());
    this.databaseServer = databaseServer;
    this.common = common;
    this.indices = databaseServer.getIndices();
  }

  private Thread beginRepartitioningThread = null;
  private AtomicBoolean isComplete = new AtomicBoolean(true);

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
      long totalBegin = System.currentTimeMillis();

      for (int i = 0; i < databaseServer.getShardCount(); i++) {
        repartitioningComplete.put(i, false);
      }

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
            futures.add(executor.submit(new Callable(){
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
              ", currSizes=" + builder.toString() + ", duration=" + (System.currentTimeMillis() - begin)/1000d + "sec");

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
          }

          common.setSchema(dbName, schema);

          common.saveSchema(databaseServer.getDataDir());
        }
        finally {
          //common.getSchemaWriteLock(dbName).unlock();
        }
        databaseServer.pushSchema();

        Thread.sleep(1000);
        //          common.saveSchema(databaseServer.getDataDir());
        //          databaseServer.pushSchema();

        for (String index : toRebalance) {
          String[] parts = index.split(" ");
          tableName = parts[0];
          final String finalTableName = tableName;
          final String indexName = parts[1];

          resetRepartitioningComplete();

          begin = System.currentTimeMillis();

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
                String command = "DatabaseServer:ComObject:rebalanceOrderedIndex:";
                Random rand = new Random(System.currentTimeMillis());
                try {
                  databaseServer.getDatabaseClient().send(null, shard, rand.nextLong(), command,
                      cobj.serialize(), DatabaseClient.Replica.master);
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

          while (true) {
            if (isRepartitioningComplete()) {
              isRepartitioningIndex.set(false);
              break;
            }
            Thread.sleep(100);
          }

          logger.info("master - rebalance ordered index - finished: table=" + tableName + INDEX_STR + indexName +
              ", duration=" + (System.currentTimeMillis() - begin) / 1000d + "sec");

          begin = System.currentTimeMillis();
          resetDeletingComplete();

          logger.info("master - delete moved entries - begin: table=" + tableName + " index=" + indexName);
//          for (int i = 0; i < databaseServer.getShardCount(); i++) {
//            final int shard = i;
//
//            futures.add(executor.submit(new Callable() {
//              @Override
//              public Object call() {
//                String command = "DatabaseServer:deleteMovedIndexEntries:1:" + common.getSchemaVersion() + ":" + dbName + ":" + finalTableName + ":" + indexName;
//                Random rand = new Random(System.currentTimeMillis());
//                try {
//                  databaseServer.getDatabaseClient().send(null, shard, rand.nextLong(), command, null, DatabaseClient.Replica.all);
//                }
//                catch (Exception e) {
//                  logger.error("Error sending rebalanceOrderedIndex to shard: shard=" + shard, e);
//                }
//                return null;
//              }
//            }));
//
//          }
//
//          for (Future future : futures) {
//            future.get();
//          }

//          while (true) {
//            if (isDeletingComplete()) {
//              break;
//            }
//            Thread.sleep(100);
//          }
          logger.info("master - delete moved entries - finished: table=" + tableName + ", index=" + indexName +
            ", duration=" + (System.currentTimeMillis() - begin) / 1000d + "sec");


          logger.info("master - rebalance ordered index - end: table=" + tableName + INDEX_STR + indexName +
            ", duration=" + (System.currentTimeMillis() - totalBegin) / 1000d + "sec");
        }

        for (String index : toRebalance) {
          String[] parts = index.split(" ");
          tableName = parts[0];
          final String indexName = parts[1];
          common.getTables(dbName).get(tableName).getIndices().get(indexName).deleteLastPartitions();
        }

        common.saveSchema(databaseServer.getDataDir());
        common.loadSchema(databaseServer.getDataDir());
        logger.info("master - Post-save schemaVersion=" + common.getSchemaVersion() + ", shard=" + common.getShard() +
            ", replica=" + common.getReplica());
        databaseServer.pushSchema();

        isRepartitioningIndex.set(false);
      }
      catch (Exception e) {
        e.printStackTrace();
        logger.error("Error repartitioning", e);
      }
      finally {
        beginRepartitioningThread = null;
        isComplete.set(true);
      }
    }
    finally {
      executor.shutdownNow();
    }
    return null;
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

  private void resetRepartitioningComplete() {
    repartitioningComplete.clear();
    for (int i = 0; i < databaseServer.getShardCount(); i++) {
      final int shard = i;
      repartitioningComplete.put(shard, false);
    }
  }

  private void resetDeletingComplete() {
    deletingComplete.clear();
    for (int i = 0; i < databaseServer.getShardCount(); i++) {
      final int shard = i;
      for (int j = 0; j < databaseServer.getReplicationFactor(); j++) {
        deletingComplete.put(shard + ":" + j, false);
      }
    }
  }

  public byte[] finishRebalance(String command, byte[] body) {
    //todo: swap in the schema

    return null;
  }

//  public byte[] isRepartitioningRecordsByIdComplete(String command, byte[] body) {
//    try {
//      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
//      DataOutputStream out = new DataOutputStream(bytesOut);
//      DataUtil.writeVLong(out, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
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

  public byte[] isRepartitioningComplete(ComObject cobj) {
    ComObject retObj = new ComObject();
    retObj.put(ComObject.Tag.finished, isRepartitioningComplete());
    return retObj.serialize();
  }

//  public byte[] isDeletingComplete(String command, byte[] body) {
//    try {
//      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
//      DataOutputStream out = new DataOutputStream(bytesOut);
//      DataUtil.writeVLong(out, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
//      boolean finished = isDeletingComplete();
//      out.writeBoolean(finished);
//      return bytesOut.toByteArray();
//    }
//    catch (IOException e) {
//      throw new DatabaseException(e);
//    }
//  }

  private boolean isRepartitioningComplete() {
    boolean finished = true;
    for (boolean entry : repartitioningComplete.values()) {
      if (!entry) {
        finished = false;
        break;
      }
    }
    return finished;
  }

//  public boolean isDeletingComplete() {
//    boolean finished = true;
//    for (boolean entry : deletingComplete.values()) {
//      if (!entry) {
//        finished = false;
//        break;
//      }
//    }
//    return finished;
//  }

  public byte[] notifyRepartitioningComplete(ComObject cobj) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    int shard = cobj.getInt(ComObject.Tag.shard);
    repartitioningComplete.put(shard, true);
    return null;
  }

//  public byte[] notifyDeletingComplete(String command, byte[] body) {
//    String[] parts = command.split(":");
//    String dbName = parts[5];
//    int shard = Integer.valueOf(parts[6]);
//    int replica = Integer.valueOf(parts[7]);
//    deletingComplete.put(shard + ":" + replica, true);
//    return null;
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
    String command = "DatabaseServer:ComObject:getKeyAtOffset:";
    ComArray array = cobj.putArray(ComObject.Tag.offsets, ComObject.Type.longType);
    for (OffsetEntry offset : offsets) {
      array.add(offset.offset);
    }
    byte[] ret = databaseServer.getDatabaseClient().send(null, shard, 0, command, cobj.serialize(), DatabaseClient.Replica.master);

    if (ret == null) {
      throw new IllegalStateException("Key not found on shard: shard=" + shard + ", table=" + tableName + ", index=" + indexName );
    }

    ComObject retObj = new ComObject(ret);
    ComArray keyArray = retObj.getArray(ComObject.Tag.keys);
    List<Object[]> keys = new ArrayList<>();
    if (keyArray != null) {
      for (int i = 0; i < keyArray.getArray().size(); i++) {
        Object[] key = DatabaseCommon.deserializeKey(common.getTables(dbName).get(tableName), (byte[])keyArray.getArray().get(i));
        keys.add(key);
      }
    }

    logger.info("getKeyAtOffset finished: dbName=" + dbName + ", shard=" + shard + ", table=" + tableName +
        ", index=" + indexName +
        ", duration=" + (System.currentTimeMillis() - begin));
    return keys;
  }

  public byte[] getKeyAtOffset(ComObject cobj) {
    try {
      String dbName = cobj.getString(ComObject.Tag.dbName);
      String tableName = cobj.getString(ComObject.Tag.tableName);
      String indexName = cobj.getString(ComObject.Tag.indexName);
      List<Long> offsets = new ArrayList<>();
      ComArray offsetsArray = cobj.getArray(ComObject.Tag.offsets);
      if (offsetsArray != null) {
        for (int i = 0; i < offsetsArray.getArray().size(); i++) {
          offsets.add((Long)offsetsArray.getArray().get(i));
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
          return retObj.serialize();
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
    String command = "DatabaseServer:ComObject:getPartitionSize:";
    Random rand = new Random(System.currentTimeMillis());
    byte[] ret = databaseServer.getDatabaseClient().send(null, shard, rand.nextLong(), command,
        cobj.serialize(), DatabaseClient.Replica.master);
    ComObject retObj = new ComObject(ret);
    return retObj.getLong(ComObject.Tag.size);
  }

  public byte[] getPartitionSize(ComObject cobj) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    String tableName = cobj.getString(ComObject.Tag.tableName);
    String indexName = cobj.getString(ComObject.Tag.indexName);

    logger.info("getPartitionSize: dbName=" + dbName + ", table=" + tableName + ", index=" + indexName);
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

    ComObject retObj = new ComObject();
    retObj.put(ComObject.Tag.size, size);

    return retObj.serialize();
  }

  public void deleteIndexEntry(String tableName, String indexName, Object[] primaryKey) {
    if (tableName.equals(currTableRepartitioning) && indexName.equals(currIndexRepartitioning)) {
      ConcurrentHashMap<String, ConcurrentLinkedQueue<Object[]>> indicesToDelete = getIndicesToDeleteFrom(tableName, indexName);
      ConcurrentLinkedQueue<Object[]> keysToDelete = indicesToDelete.get(indexName);
      keysToDelete.add(primaryKey);
    }
  }

  public boolean undeleteIndexEntry(String dbName, String tableName, String indexName, Object[] primaryKey, byte[] recordBytes) {
    if (tableName.equals(currTableRepartitioning) && indexName.equals(currIndexRepartitioning)) {
      Comparator[] comparators = databaseServer.getIndices(dbName).getIndices().get(tableName).get(indexName).getComparators();

      ConcurrentHashMap<String, ConcurrentLinkedQueue<Object[]>> indicesToDelete = getIndicesToDeleteFrom(tableName, indexName);
      ConcurrentLinkedQueue<Object[]> keysToDelete = indicesToDelete.get(indexName);
      for (Object[] key : keysToDelete) {
        if (0 == DatabaseCommon.compareKey(comparators, key, primaryKey)) {
          keysToDelete.remove(key);
          break;
        }
      }
      return false;
    }
    else {
      return true;
    }
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

  public byte[] rebalanceOrderedIndex(ComObject cobj) {
    String command = "DatabaseServer:ComObject:doRebalanceOrderedIndex:";
    cobj.put(ComObject.Tag.method, "doRebalanceOrderedIndex");
    databaseServer.getLongRunningCommands().addCommand(databaseServer.getLongRunningCommands().createSingleCommand(command, cobj.serialize()));
    return null;
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
    private ArrayBlockingQueue<List<MoveRequest>> queue = new ArrayBlockingQueue<>(100000);
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
      this.executor = new ThreadPoolExecutor(8, 8, 10000, TimeUnit.MILLISECONDS,
          new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
    }


    public void shutdown() {
      this.shutdown = true;
      thread.interrupt();
      executor.shutdownNow();
    }

    public void start() {
      thread = new Thread(new Runnable(){
        @Override
        public void run() {
          while (!shutdown) {
            try {
              final List<MoveRequest> list = queue.poll(30000, TimeUnit.MILLISECONDS);
              if (list == null) {
                continue;
              }
              countStarted.incrementAndGet();
              executor.submit(new Runnable(){
                @Override
                public void run() {
                  try {
                    List<Object> toFree = new ArrayList<>();
                    long begin = System.currentTimeMillis();
                    moveIndexEntriesToShard(dbName, tableName, indexName, isPrimaryKey, shard, list);
                    for (MoveRequest request : list) {
                      if (request.shouldDeleteNow) {
                        //synchronized (index.getMutex(request.key)) {
                          Object value = index.remove(request.key);
                          if (value != null) {
                            toFree.add(value);
                          }
                        //}
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
                    for (Object obj : toFree) {
                      databaseServer.freeUnsafeIds(obj);
                    }

                    logger.info("moved entries: table=" + tableName + ", index=" + indexName + ", count=" + list.size() +
                        ", shard=" + shard + ", duration=" + (System.currentTimeMillis() - begin));
                  }
                  finally {
                    countFinished.incrementAndGet();
                  }
                }
              });
            }
            catch (InterruptedException e) {
              break;
            }
            catch (Exception e) {
              logger.error("Error processing move requests", e);
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

  public byte[] doRebalanceOrderedIndex(final ComObject cobj) {
    try {
      final String dbName = cobj.getString(ComObject.Tag.dbName);
      final String tableName = cobj.getString(ComObject.Tag.tableName);
      final String indexName = cobj.getString(ComObject.Tag.indexName);
      logger.info("doRebalanceOrderedIndex: shard=" + databaseServer.getShard() + ", dbName=" + dbName +
        ", tableName=" + tableName + ", indexName=" + indexName);

      final Index index = indices.get(dbName).getIndices().get(tableName).get(indexName);
      final TableSchema tableSchema = common.getTables(dbName).get(tableName);
      final IndexSchema indexSchema = tableSchema.getIndices().get(indexName);

      long begin = System.currentTimeMillis();

      common.getSchemaReadLock(dbName).lock();
      try {
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
          final ThreadPoolExecutor executor = new ThreadPoolExecutor(16, 16, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
          final AtomicInteger countSubmitted = new AtomicInteger();
          final AtomicInteger countFinished = new AtomicInteger();
          final MoveProcessor[] moveProcessors = new MoveProcessor[databaseServer.getShardCount()];
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
                  index.visitHeadMap(lowerPartition.getUpperKey(), new Index.Visitor() {
                    @Override
                    public boolean visit(Object[] key, Object value) throws IOException {
                      countVisited.incrementAndGet();
                      currEntries.get().add(new MapEntry(key, value));
                      if (currEntries.get().size() >= 10000 * databaseServer.getShardCount()) {
                        final List<MapEntry> toProcess = currEntries.get();
                        currEntries.set(new ArrayList<MapEntry>());
                        countSubmitted.incrementAndGet();
                        if (countSubmitted.get() > 20) {
                          databaseServer.setThrottleInsert(true);
                        }
                        executor.submit(new Runnable() {
                          @Override
                          public void run() {
                            long begin = System.currentTimeMillis();
                            try {
                              logger.info("doProcessEntries: table=" + tableName + ", index=" + indexName + ", count=" + toProcess.size());
                              doProcessEntries(moveProcessors, tableName, indexName, toProcess, index, indexSchema, dbName, fieldOffsets, tableSchema, cobj);
                            }
                            catch (Exception e) {
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
                }
                if (currPartition.getUpperKey() != null) {
                  index.visitTailMap(currPartition.getUpperKey(), new Index.Visitor() {
                    @Override
                    public boolean visit(Object[] key, Object value) throws IOException {
                      countVisited.incrementAndGet();
                      currEntries.get().add(new MapEntry(key, value));
                      if (currEntries.get().size() >= 10000 * databaseServer.getShardCount()) {
                        final List<MapEntry> toProcess = currEntries.get();
                        currEntries.set(new ArrayList<MapEntry>());
                        countSubmitted.incrementAndGet();
                        if (countSubmitted.get() > 20) {
                          databaseServer.setThrottleInsert(true);
                        }
                        executor.submit(new Runnable() {
                          @Override
                          public void run() {
                            long begin = System.currentTimeMillis();
                            try {
                              logger.info("doProcessEntries: table=" + tableName + ", index=" + indexName + ", count=" + toProcess.size());
                              doProcessEntries(moveProcessors, tableName, indexName, toProcess, index, indexSchema, dbName, fieldOffsets, tableSchema, cobj);
                            }
                            catch (Exception e) {
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
                }

                if (countSubmitted.get() > 0) {
                  while (countSubmitted.get() > countFinished.get()) {
                    Thread.sleep(1000);
                  }
                }
              }
              finally {
                databaseServer.setThrottleInsert(false);
              }

              if (currEntries.get() != null && currEntries.get().size() != 0) {
                try {
                  logger.info("doProcessEntries: table=" + tableName + ", index=" + indexName + ", count=" + currEntries.get().size());
                  doProcessEntries(moveProcessors, tableName, indexName, currEntries.get(), index, indexSchema, dbName, fieldOffsets, tableSchema, cobj);
                }
                finally {
                  logger.info("doProcessEntries - finished: table=" + tableName + ", index=" + indexName + ", count=" + currEntries.get().size() +
                      ", duration=" + (System.currentTimeMillis() - begin));
                }
              }
            //}
            logger.info("doProcessEntries - all finished: table=" + tableName + ", index=" + indexName +
                ", count=" + countVisited.get() + ", countToDelete=" + keysToDelete.size());
          }
          finally {
            for (MoveProcessor moveProcessor : moveProcessors) {
              moveProcessor.await();
              moveProcessor.shutdown();
            }
            databaseServer.getDeleteManager().saveDeletes(dbName, tableName, indexName, keysToDelete);
            deleteRecordsOnOtherReplicas(dbName, tableName, indexName, keysToDelete);
            executor.shutdownNow();
            logger.info("doRebalanceOrderedIndex finished: table=" + tableName + ", index=" + indexName + ", countVisited=" + countVisited.get() +
                ", duration=" + (System.currentTimeMillis() - begin));
          }
        }
      }
      finally {
        common.getSchemaReadLock(dbName).unlock();
      }
      ComObject cobj2 = new ComObject();
      cobj2.put(ComObject.Tag.dbName, dbName);
      cobj2.put(ComObject.Tag.shard, databaseServer.getShard());
      cobj2.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
      cobj2.put(ComObject.Tag.method, "notifyRepartitioningComplete");
      String notifyCommand = "DatabaseServer:ComObject:notifyRepartitioningComplete:";
      databaseServer.getDatabaseClient().sendToMaster(notifyCommand, cobj2.serialize());
    }
    catch (Exception e) {
      logger.error("Error rebalancing index", e);
    }
    return null;
  }

  private void deleteRecordsOnOtherReplicas(final String dbName, String tableName, String indexName, ConcurrentLinkedQueue<Object[]> keysToDelete) {

    try {
      TableSchema tableSchema = common.getTables(dbName).get(tableName);
      final ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.dbName, dbName);
      cobj.put(ComObject.Tag.method, "deleteMovedRecords");
      cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
      cobj.put(ComObject.Tag.tableName, tableName);
      cobj.put(ComObject.Tag.indexName, indexName);

      ComArray keys = cobj.putArray(ComObject.Tag.keys, ComObject.Type.byteArrayType);
      for (Object[] key : keysToDelete) {
        keys.add(DatabaseCommon.serializeKey(tableSchema, indexName, key));
      }

      final byte[] bytes = cobj.serialize();

      List<Future> futures = new ArrayList<>();
      int replicaCount = databaseServer.getReplicationFactor();
      ThreadPoolExecutor executor = new ThreadPoolExecutor(replicaCount - 1, replicaCount - 1, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
      for (int i = 0; i < replicaCount; i++) {
        final int replica = i;
        if (replica == databaseServer.getReplica()) {
          continue;
        }
        futures.add(executor.submit(new Callable(){
          @Override
          public Object call() throws Exception {

            String command = "DatabaseServer:ComObject:deleteMovedRecords:";
            databaseServer.getDatabaseClient().send(null, databaseServer.getShard(), replica,
                command, bytes, DatabaseClient.Replica.specified);
            return null;
          }
        }));
      }
      for (Future future : futures) {
        try {
          future.get();
        }
        catch (Exception e) {
          logger.error("Error deleting moved records on replica", e);
        }
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }


  public byte[] deleteMovedRecords(ComObject cobj) {
    try {
      ConcurrentLinkedQueue<Object[]> keysToDelete = new ConcurrentLinkedQueue<>();
      String dbName = cobj.getString(ComObject.Tag.dbName);
      String tableName = cobj.getString(ComObject.Tag.tableName);
      String indexName = cobj.getString(ComObject.Tag.indexName);
      TableSchema tableSchema = common.getTables(dbName).get(tableName);
      IndexSchema indexSchema = tableSchema.getIndexes().get(indexName);
      ComArray keys = cobj.getArray(ComObject.Tag.keys);
      if (keys != null) {
        for (int i = 0; i < keys.getArray().size(); i++) {
          Object[] key = DatabaseCommon.deserializeKey(tableSchema, (byte[])keys.getArray().get(i));
          keysToDelete.add(key);
        }
      }
      databaseServer.getDeleteManager().saveDeletes(dbName, tableName, indexName, keysToDelete);

      List<Object> toFree = new ArrayList<>();
      Index index = databaseServer.getIndices().get(dbName).getIndices().get(tableName).get(indexName);
      for (Object[] key : keysToDelete) {
        synchronized (index.getMutex(key)) {
          Object value = index.get(key);
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
              byte[][] newContent = new byte[content.length][];
              for (int i = 0; i < content.length; i++) {
                Record record = new Record(dbName, common, content[i]);
                record.setDbViewNumber(common.getSchemaVersion());
                record.setDbViewFlags(Record.DB_VIEW_FLAG_DELETING);
                newContent[i] = record.serialize(common);
              }
              toFree.add(value);
              Object newValue = databaseServer.toUnsafeFromRecords(newContent);
              index.put(key, newValue);
            }
          }
        }
      }
      for (Object obj : toFree) {
        databaseServer.freeUnsafeIds(obj);
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
    return null;
  }

  private void doProcessEntries(MoveProcessor[] moveProcessors, final String tableName, final String indexName,
                                List<MapEntry> toProcess, final Index index, final IndexSchema indexSchema, final String dbName,
                                int[] fieldOffsets, TableSchema tableSchema, ComObject cobj) {
    final Map<Integer, List<MoveRequest>> moveRequests = new HashMap<>();
    for (int i = 0; i < databaseServer.getShardCount(); i++) {
      moveRequests.put(i, new ArrayList<MoveRequest>());
    }

    List<Object> toFree = new ArrayList<>();
    for (MapEntry entry : toProcess) {
      try {
        byte[][] content = null;
        int shard = 0;
        List<Integer> selectedShards = findOrderedPartitionForRecord(true,
            false, fieldOffsets, common, tableSchema, indexName,
            null, BinaryExpression.Operator.equal, null,
            entry.key, null);
        synchronized (index.getMutex(entry.key)) {
          if (entry.value instanceof Long) {
            entry.value = index.get(entry.key);
          }
          if (entry.value != null) {
            if (indexSchema.isPrimaryKey()) {
              content = databaseServer.fromUnsafeToRecords(entry.value);
            }
            else {
              content = databaseServer.fromUnsafeToKeys(entry.value);
            }
          }
          if (content != null) {
            shard = selectedShards.get(0);
            if (shard != databaseServer.getShard()) {
              if (indexSchema.isPrimaryKey()) {
                byte[][] newContent = new byte[content.length][];
                for (int i = 0; i < content.length; i++) {
                  Record record = new Record(dbName, common, content[i]);
                  record.setDbViewNumber(common.getSchemaVersion());
                  record.setDbViewFlags(Record.DB_VIEW_FLAG_DELETING);
                  newContent[i] = record.serialize(common);
                }
                toFree.add(entry.value);
                //databaseServer.freeUnsafeIds(entry.value);
                Object newValue = databaseServer.toUnsafeFromRecords(newContent);
                index.put(entry.key, newValue);
              }
            }
            else {
              content = null;
            }
          }
        }
        if (content != null) {
          final List<MoveRequest> list = moveRequests.get(shard);
          boolean shouldDeleteNow = false;
          if (indexSchema.isPrimaryKey()) {
            Record record = new Record(dbName, common, content[0]);
            if (record.getDbViewFlags() == Record.DB_VIEW_FLAG_DELETING) {
              shouldDeleteNow = true;
            }
          }
          list.add(new MoveRequest(entry.key, content, shouldDeleteNow));
          if (list.size() > 50000) {
            moveRequests.put(shard, new ArrayList<MoveRequest>());
            moveProcessors[shard].queue.put(list);
          }
        }
      }
      catch (Exception t) {
        logger.error("Error moving record: table=" + cobj.getString(ComObject.Tag.tableName) +
            INDEX_STR + cobj.getString(ComObject.Tag.indexName), t);
      }
    }

    try {
      Thread.sleep(1000);
    }
    catch (InterruptedException e) {
      throw new DatabaseException(e);
    }

    for (Object obj : toFree) {
      databaseServer.freeUnsafeIds(obj);
    }

    try {
      for (int i = 0; i < databaseServer.getShardCount(); i++) {
        final int shard = i;
        final List<MoveRequest> list = moveRequests.get(i);
        if (list.size() != 0) {
          moveProcessors[shard].queue.put(list);
        }
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
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

  private void moveIndexEntriesToShard(
      String dbName, String tableName, String indexName, boolean primaryKey, int shard, List<MoveRequest> moveRequests) {
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.dbName, dbName);
    cobj.put(ComObject.Tag.tableName, tableName);
    cobj.put(ComObject.Tag.indexName, indexName);
    cobj.put(ComObject.Tag.method, "moveIndexEntries");
    cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
    ComArray keys = cobj.putArray(ComObject.Tag.keys, ComObject.Type.objectType);
    for (MoveRequest moveRequest : moveRequests) {
      byte[] bytes = DatabaseCommon.serializeKey(common.getTables(dbName).get(tableName), indexName, moveRequest.key);
      ComObject innerObj = new ComObject();
      keys.add(innerObj);
      innerObj.remove(ComObject.Tag.serializationVersion);
      innerObj.put(ComObject.Tag.keyBytes, bytes);

      byte[][] content = moveRequest.getContent();
      try {
        if (primaryKey) {
          for (int i = 0; i < content.length; i++) {
            byte[] recordBytes = content[i];
            Record record = new Record(dbName, common, recordBytes);
            record.setDbViewNumber(common.getSchemaVersion());
            record.setDbViewFlags(Record.DB_VIEW_FLAG_ADDING);
            content[i] = record.serialize(common);
          }
        }
        ComArray records = innerObj.putArray(ComObject.Tag.records, ComObject.Type.byteArrayType);
        for (int i = 0; i < content.length; i++) {
          records.add(content[i]);
        }
      }
      catch (Exception e) {
        logger.error("Error moving record", e);
      }
    }

    byte[] body = cobj.serialize();

    String command = "DatabaseServer:ComObject:moveIndexEntries:";
    databaseServer.getDatabaseClient().send(null, shard, 0, command, body, DatabaseClient.Replica.def);
  }

  public byte[] moveIndexEntries(ComObject cobj) {
    try {
      synchronized (databaseServer.getBatchRepartCount()) {
        databaseServer.getBatchRepartCount().incrementAndGet();
        String dbName = cobj.getString(ComObject.Tag.dbName);

        String tableName = cobj.getString(ComObject.Tag.tableName);
        String indexName = cobj.getString(ComObject.Tag.indexName);
        ComArray keys = cobj.getArray(ComObject.Tag.keys);
        List<MoveRequest> moveRequests = new ArrayList<>();
        if (keys != null) {
          for (int i = 0; i < keys.getArray().size(); i++) {
            ComObject keyObj = (ComObject)keys.getArray().get(i);
            Object[] key = DatabaseCommon.deserializeKey(common.getTables(dbName).get(tableName), keyObj.getByteArray(ComObject.Tag.keyBytes));
            ComArray records = keyObj.getArray(ComObject.Tag.records);
            if (records != null) {
              byte[][] content = new byte[records.getArray().size()][];
              for (int j = 0; j < content.length; j++) {
                content[j] = (byte[])records.getArray().get(j);
              }
              moveRequests.add(new MoveRequest(key, content, false));
            }
          }
        }
        TableSchema tableSchema = common.getTables(dbName).get(tableName);
        Index index = databaseServer.getIndices(dbName).getIndices().get(tableSchema.getName()).get(indexName);
        IndexSchema indexSchema = common.getTables(dbName).get(tableName).getIndices().get(indexName);
        databaseServer.getUpdateManager().doInsertKeys(moveRequests, index, tableName, indexSchema);
      }
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

  public byte[] getIndexCounts(ComObject cobj) {
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

      return retObj.serialize();
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
        futures.add(client.getExecutor().submit(new Callable(){
          @Override
          public Object call() throws Exception {
            ComObject cobj = new ComObject();
            cobj.put(ComObject.Tag.dbName, dbName);
            cobj.put(ComObject.Tag.schemaVersion, client.getCommon().getSchemaVersion());
            cobj.put(ComObject.Tag.method, "getIndexCounts");
            String command = "DatabaseServer:ComObject:getIndexCounts:";
            byte[] response = client.send(null, shard, 0, command, cobj.serialize(), DatabaseClient.Replica.master);
            synchronized (ret) {
              ComObject retObj = new ComObject(response);
              ComArray tables = retObj.getArray(ComObject.Tag.tables);
              if (tables != null) {
                for (int i = 0; i < tables.getArray().size(); i++) {
                  ComObject tableObj = (ComObject)tables.getArray().get(i);
                  String tableName = tableObj.getString(ComObject.Tag.tableName);

                  TableIndexCounts tableIndexCounts = ret.tables.get(tableName);
                  if (tableIndexCounts == null) {
                    tableIndexCounts = new TableIndexCounts();
                    ret.tables.put(tableName, tableIndexCounts);
                  }
                  ComArray indices = tableObj.getArray(ComObject.Tag.indices);
                  if (indices != null) {
                    for (int j = 0; j < indices.getArray().size(); j++) {
                      ComObject indexObj = (ComObject)indices.getArray().get(j);
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

  @Override
  public void run() {
    try {
      Thread.sleep(15000);
    }
    catch (InterruptedException e) {
      return;
    }
    while (true) {
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
      catch (Exception t) {
        logger.error("Error in master thread", t);
        try {
          Thread.sleep(10 * 1000);
        }
        catch (InterruptedException e) {
          logger.info("Repartitioner interrupted");
          return;
        }
      }
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

  private AtomicBoolean isRebalancing = new AtomicBoolean();

  public byte[] beginRebalance(ComObject cobj) {
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
      String configStr = StreamUtils.inputStreamToString(new BufferedInputStream(new FileInputStream(file)));
      logger.info("Config: " + configStr);
      JsonDict config = new JsonDict(configStr);

      boolean isInternal = false;
      if (config.hasKey("clientIsPrivate")) {
        isInternal = config.getBoolean("clientIsPrivate");
      }
      DatabaseServer.ServersConfig newConfig = new DatabaseServer.ServersConfig(databaseServer.getCluster(), config.getArray("shards"),
          config.getArray("shards").getDict(0).getArray("replicas").size(), isInternal);
      DatabaseServer.Shard[] newShards = newConfig.getShards();

      common.setServersConfig(newConfig);
      common.saveServersConfig(databaseServer.getDataDir());
      logger.info("Repartitioner: shardCount=" + newShards.length);
      databaseServer.setShardCount(newShards.length);
      databaseServer.getDatabaseClient().configureServers();
      databaseServer.pushServersConfig();

      for (TableSchema table : common.getTables(dbName).values()) {
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
        for (Map.Entry<String, IndexCounts> indexEntry : entry.getValue().indices.entrySet()) {
          IndexSchema indexSchema = common.getTables(dbName).get(entry.getKey()).getIndices().get(indexEntry.getKey());
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
        beginRebalance(dbName, group);
      }
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
