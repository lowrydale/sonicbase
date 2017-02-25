package com.lowryengineering.database.index;

import com.lowryengineering.database.client.DatabaseClient;
import com.lowryengineering.database.common.DatabaseCommon;
import com.lowryengineering.database.common.Record;
import com.lowryengineering.database.query.BinaryExpression;
import com.lowryengineering.database.query.DatabaseException;
import com.lowryengineering.database.query.impl.OrderByExpressionImpl;
import com.lowryengineering.database.schema.DataType;
import com.lowryengineering.database.schema.IndexSchema;
import com.lowryengineering.database.schema.TableSchema;
import com.lowryengineering.database.server.DatabaseServer;
import com.lowryengineering.database.server.SnapshotManager;
import com.lowryengineering.database.util.DataUtil;
import com.lowryengineering.database.util.JsonArray;
import com.lowryengineering.database.util.JsonDict;
import com.lowryengineering.database.util.StreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Responsible for
 */
public class Repartitioner extends Thread {

  private static final String INDEX_STR = ", index=";
  private static final String NAME_STR = "name";
  private static Logger logger = LoggerFactory.getLogger(Repartitioner.class);


  private final DatabaseServer databaseServer;
  private final DatabaseCommon common;
  private final ConcurrentHashMap<String, Indices> indices;
  private ConcurrentHashMap<String, Boolean> repartitioningComplete = new ConcurrentHashMap<>();
  private ConcurrentHashMap<String, Boolean> deletingComplete = new ConcurrentHashMap<>();
  private ConcurrentHashMap<Integer, AtomicInteger> repartitioningRecordsByIdComplete = new ConcurrentHashMap<>();
  private AtomicBoolean isRepartitioningIndex = new AtomicBoolean();

  private String currIndexRepartitioning;
  private String currTableRepartitioning;

  public Repartitioner(DatabaseServer databaseServer, DatabaseCommon common) {
    super("Repartitioner Thread");
    this.databaseServer = databaseServer;
    this.common = common;
    this.indices = databaseServer.getIndices();
  }

  private Thread beginRepartitioningThread = null;
  private AtomicBoolean isComplete = new AtomicBoolean(true);

  public byte[] beginRebalance(final String dbName, final List<String> toRebalance) {

    while (!isComplete.compareAndSet(true, false)) {
      try {
        Thread.sleep(1000);
      }
      catch (InterruptedException e) {
        throw new DatabaseException(e);
      }
    }
    for (int i = 0; i < databaseServer.getShardCount(); i++) {
      for (int j = 0; j < databaseServer.getReplicationFactor(); j++) {
        repartitioningComplete.put(i + ":" + j, false);
      }
    }

    beginRepartitioningThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          String tableName = null;
          StringBuilder toRebalanceStr = new StringBuilder();
          for (String index : toRebalance) {
            toRebalanceStr.append(index).append(", ");
          }
          logger.info("Rebalancing index group: group=" + toRebalanceStr);


          Map<String, long[]> partitionSizes = new HashMap<>();
          for (String index : toRebalance) {
            String[] parts = index.split(" ");
            tableName = parts[0];
            final String indexName = parts[1];
            long[] currPartitionSizes = new long[databaseServer.getShardCount()];
            for (int i = 0; i < databaseServer.getShardCount(); i++) {
              currPartitionSizes[i] = getPartitionSize(dbName, i, tableName, indexName);
            }
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
            long offset = newPartitionSize;
            int sourceShardOffset = 0;
            long currPartitionsOffset = -1;
            for (int currShardOffset = 0; currShardOffset < databaseServer.getShardCount() - 1; ) {
              currPartitionsOffset += currPartitionSizes[sourceShardOffset++];
              while (currPartitionsOffset < offset && sourceShardOffset < databaseServer.getShardCount()) {
                currPartitionsOffset += currPartitionSizes[sourceShardOffset++];
              }
              int prevPartitionsSize = 0;
              for (int i = 0; i < sourceShardOffset - 1; i++) {
                prevPartitionsSize += currPartitionSizes[i];
              }
              long currOffset = offset - prevPartitionsSize;
              do {
                Object[] key = getKeyAtOffset(dbName, sourceShardOffset - 1, tableSchema.getName(), indexName, currOffset);
                TableSchema.Partition partition = new TableSchema.Partition();
                newPartitions.add(partition);
                partition.setUpperKey(key);
                partition.setShardOwning(currShardOffset++);
                currOffset += newPartitionSize;
                offset += newPartitionSize;
              }
              while (currOffset < currPartitionSizes[sourceShardOffset - 1] && currShardOffset < databaseServer.getShardCount() - 1);
            }
            TableSchema.Partition lastPartition = new TableSchema.Partition();
            newPartitions.add(lastPartition);
            lastPartition.setUnboundUpper(true);
            lastPartition.setShardOwning(databaseServer.getShardCount() - 1);


            //      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
            //      DataOutputStream out = new DataOutputStream(bytesOut);
            //      out.writeInt(newPartitions.size());
            //      for (TableSchema.Partition partition : newPartitions) {
            //        partition.serialize(tableSchema, indexName, out);
            //      }
            //      out.close();
            //      final byte[] bytes = bytesOut.toByteArray();

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

          common.getSchemaWriteLock(dbName).lock();
          try {
            for (String index : toRebalance) {
              String[] parts = index.split(" ");
              tableName = parts[0];
              final String indexName = parts[1];
              TableSchema tableSchema = common.getTables(dbName).get(tableName);
              if (copiedPartitionsToApply.containsKey(index)) {
                tableSchema.getIndices().get(indexName).reshardPartitions(copiedPartitionsToApply.get(index));
                logPartitionsToApply(dbName, tableName, indexName, copiedPartitionsToApply.get(index));
              }
              if (newPartitionsToApply.containsKey(index)) {
                tableSchema.getIndices().get(indexName).reshardPartitions(newPartitionsToApply.get(index));
                logPartitionsToApply(dbName, tableName, indexName, newPartitionsToApply.get(index));
              }
            }

            common.saveSchema(databaseServer.getDataDir());
          }
          finally {
            common.getSchemaWriteLock(dbName).unlock();
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

            logger.info("rebalance ordered index - begin: table=" + tableName + INDEX_STR + indexName);
            List<Future> futures = new ArrayList<>();
            for (int i = 0; i < databaseServer.getShardCount(); i++) {
              final int shard = i;

              futures.add(databaseServer.getDatabaseClient().getExecutor().submit(new Callable() {
                @Override
                public Object call() {
                  String command = "DatabaseServer:rebalanceOrderedIndex:1:" + common.getSchemaVersion() + ":" + dbName + ":" + finalTableName + ":" + indexName;
                  Random rand = new Random(System.currentTimeMillis());
                  try {
                    databaseServer.getDatabaseClient().send(null, shard, rand.nextLong(), command, null, DatabaseClient.Replica.all);
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
//              if (lastPartitions != tableSchema.getIndices().get(indexName).getLastPartitions() ||
//      f            null == tableSchema.getIndices().get(indexName).getLastPartitions()) {
//                logger.error("##########################");
//              }
              if (isRepartitioningComplete()) {
//                tableSchema.getIndices().get(indexName).deleteLastPartitions();
//                common.saveSchema(databaseServer.getDataDir());
//                databaseServer.pushSchema();
                isRepartitioningIndex.set(false);
                break;
              }
              Thread.sleep(2);
              //}
            }

            logger.info("delete moved entries - begin");
            for (int i = 0; i < databaseServer.getShardCount(); i++) {
              final int shard = i;

              futures.add(databaseServer.getDatabaseClient().getExecutor().submit(new Callable() {
                @Override
                public Object call() {
                  String command = "DatabaseServer:deleteMovedIndexEntries:1:" + common.getSchemaVersion() + ":" + dbName + ":" + finalTableName + ":" + indexName;
                  Random rand = new Random(System.currentTimeMillis());
                  try {
                    databaseServer.getDatabaseClient().send(null, shard, rand.nextLong(), command, null, DatabaseClient.Replica.all);
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
              if (isDeletingComplete()) {
                break;
              }
              Thread.sleep(2);
            }
            resetDeletingComplete();
            logger.info("delete moved entries - end");


            logger.info("rebalance ordered index - end: table=" + tableName + INDEX_STR + indexName);
          }

          for (String index : toRebalance) {
            String[] parts = index.split(" ");
            tableName = parts[0];
            final String indexName = parts[1];
            common.getTables(dbName).get(tableName).getIndices().get(indexName).deleteLastPartitions();
          }

          common.saveSchema(databaseServer.getDataDir());
          common.loadSchema(databaseServer.getDataDir());
          logger.info("Post-save schemaVersion=" + common.getSchemaVersion() + ", shard=" + common.getShard() +
              ", replica=" + common.getReplica());
          databaseServer.pushSchema();

          isRepartitioningIndex.set(false);
        }
        catch (Exception e) {
          logger.error("Error repartitioning", e);
        }
        finally {
          beginRepartitioningThread = null;
          isComplete.set(true);
        }
      }
    }, "Begin Repartition Thread");
    beginRepartitioningThread.start();

    while (!isComplete.get()) {
      try {
        Thread.sleep(1000);
      }
      catch (InterruptedException e) {
        throw new DatabaseException(e);
      }
    }

    return null;
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
      for (int j = 0; j < databaseServer.getReplicationFactor(); j++) {
        repartitioningComplete.put(shard + ":" + j, false);
      }
    }
  }

  private void resetDeletingComplete() {
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

  public byte[] isRepartitioningRecordsByIdComplete(String command, byte[] body) {
    try {
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytesOut);
      DataUtil.writeVLong(out, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
      boolean finished = isRepartitioningRecordsByIdComplete();
      out.writeBoolean(finished);
      return bytesOut.toByteArray();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  private boolean isRepartitioningRecordsByIdComplete() {
    boolean finished = true;
    for (AtomicInteger entry : repartitioningRecordsByIdComplete.values()) {
      if (entry.get() > 0) {
        finished = false;
        break;
      }
    }
    return finished;
  }

  public byte[] isRepartitioningComplete(String command, byte[] body) {
    try {
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytesOut);
      DataUtil.writeVLong(out, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
      boolean finished = isRepartitioningComplete();
      out.writeBoolean(finished);
      return bytesOut.toByteArray();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public byte[] isDeletingComplete(String command, byte[] body) {
    try {
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytesOut);
      DataUtil.writeVLong(out, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
      boolean finished = isDeletingComplete();
      out.writeBoolean(finished);
      return bytesOut.toByteArray();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

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

  public boolean isDeletingComplete() {
    boolean finished = true;
    for (boolean entry : deletingComplete.values()) {
      if (!entry) {
        finished = false;
        break;
      }
    }
    return finished;
  }

  public byte[] notifyRepartitioningComplete(String command, byte[] body) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    int shard = Integer.valueOf(parts[5]);
    int replica = Integer.valueOf(parts[6]);
    repartitioningComplete.put(shard + ":" + replica, true);
    return null;
  }

  public byte[] notifyDeletingComplete(String command, byte[] body) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    int shard = Integer.valueOf(parts[5]);
    int replica = Integer.valueOf(parts[6]);
    deletingComplete.put(shard + ":" + replica, true);
    return null;
  }

  private Object[] getKeyAtOffset(String dbName, int shard, String tableName, String indexName, long offset) throws IOException {
    String command = "DatabaseServer:getKeyAtOffset:1:" + common.getSchemaVersion() + ":" + dbName + ":" + tableName + ":" + indexName + ":" + offset;
    Random rand = new Random(System.currentTimeMillis());
    byte[] ret = databaseServer.getDatabaseClient().send(null, shard, rand.nextLong(), command, null, DatabaseClient.Replica.master);

    if (ret == null) {
      throw new IllegalStateException("Key not found on shard: shard=" + shard + ", table=" + tableName + ", index=" + indexName + ", offset=" + offset);
    }

    DataInputStream in = new DataInputStream(new ByteArrayInputStream(ret));
    long serializationVersion = DataUtil.readVLong(in);
    return DatabaseCommon.deserializeKey(common.getTables(dbName).get(tableName), in);
  }

  public byte[] getKeyAtOffset(String command, byte[] body) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    String tableName = parts[5];
    String indexName = parts[6];
    long desiredOffset = Long.valueOf(parts[7]);

    long offset = 0;
    Object[] foundKey = null;
    IndexSchema indexSchema = common.getTables(dbName).get(tableName).getIndices().get(indexName);
    Index index = databaseServer.getIndices(dbName).getIndices().get(tableName).get(indexName);
    Map.Entry<Object[], Long> entry = index.firstEntry();
    if (entry != null) {
      do {
        int count = 0;
        if (entry.getValue() >= 0) {
          count = 1;
        }
        else {
          synchronized (index) {
            if (indexSchema.isPrimaryKey()) {
              Long value = index.get(entry.getKey());
              if (value == null) {
                entry = index.higherEntry(entry.getKey());
                if (entry == null) {
                  break;
                }
                continue;
              }
              byte[][] records = databaseServer.fromUnsafeToRecords(value);
              count = records.length;
            }
            else {
              Long value = index.get(entry.getKey());
              if (value == null) {
                entry = index.higherEntry(entry.getKey());
                if (entry == null) {
                  break;
                }
                continue;
              }
              byte[][] ids = databaseServer.fromUnsafeToKeys(value);
              count = ids.length;
            }
          }
        }
        offset += count;
        if (offset > desiredOffset) {
          foundKey = entry.getKey();
          break;
        }
        entry = index.higherEntry(entry.getKey());
        if (entry == null) {
          break;
        }
      }
      while (true);
      if (foundKey != null) {
        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytesOut);
        try {
          DataUtil.writeVLong(out, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
          out.write(DatabaseCommon.serializeKey(common.getTables(dbName).get(tableName), indexName, foundKey));
          out.close();
          return bytesOut.toByteArray();
        }
        catch (Exception e) {
          throw new DatabaseException(e);
        }
      }
    }
    return null;
  }

  private long getPartitionSize(String dbName, int shard, String tableName, String indexName) {
    try {
      String command = "DatabaseServer:getPartitionSize:1:" + common.getSchemaVersion() + ":" + dbName + ":" + tableName + ":" + indexName;
      Random rand = new Random(System.currentTimeMillis());
      byte[] ret = databaseServer.getDatabaseClient().send(null, shard, rand.nextLong(), command, null, DatabaseClient.Replica.master);
      DataInputStream in = new DataInputStream(new ByteArrayInputStream(ret));
      long serializationVersion = DataUtil.readVLong(in);
      return in.readLong();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public byte[] getPartitionSize(String command, byte[] body) {
    try {
      String[] parts = command.split(":");
      String dbName = parts[4];
      String tableName = parts[5];
      String indexName = parts[6];

      logger.info("getPartitionSize: dbName=" + dbName + ", table=" + tableName + ", index=" + indexName);

      //todo: really need to read all the records to get an accurate count
      Index index = databaseServer.getIndices(dbName).getIndices().get(tableName).get(indexName);
      long size = index.size();
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytesOut);
      DataUtil.writeVLong(out, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
      out.writeLong(size);
      out.close();

      return bytesOut.toByteArray();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public void deleteIndexEntry(String tableName, String indexName, Object[] primaryKey) {
    if (tableName.equals(currTableRepartitioning) && indexName.equals(currIndexRepartitioning)) {
      ConcurrentHashMap<String, List<Object[]>> indicesToDelete = getIndicesToDeleteFrom(tableName, indexName);
      List<Object[]> keysToDelete = indicesToDelete.get(indexName);
      synchronized (keysToDelete) {
        keysToDelete.add(primaryKey);
      }
    }
//    else {
//      Index index = indices.getIndices().get(tableName).get(indexName);
//      synchronized (index) {
//        Long existingValue = index.remove(primaryKey);
//        if (existingValue != null) {
//          databaseServer.freeUnsafeIds(existingValue);
//        }
//      }
//    }
  }

  public boolean undeleteIndexEntry(String dbName, String tableName, String indexName, Object[] primaryKey, byte[] recordBytes) {
    if (tableName.equals(currTableRepartitioning) && indexName.equals(currIndexRepartitioning)) {
      Comparator[] comparators = databaseServer.getIndices(dbName).getIndices().get(tableName).get(indexName).getComparators();

      ConcurrentHashMap<String, List<Object[]>> indicesToDelete = getIndicesToDeleteFrom(tableName, indexName);
      List<Object[]> keysToDelete = indicesToDelete.get(indexName);
      synchronized (keysToDelete) {
        for (Object[] key : keysToDelete) {
          if (0 == DatabaseCommon.compareKey(comparators, key, primaryKey)) {
            keysToDelete.remove(key);
            break;
          }
        }
      }
      return false;
    }
    else {
      return true;
    }
  }

  public static class MoveRequest {
    private Object[] key;
    private byte[][] content;

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "EI_EXPOSE_REP2", justification = "copying the passed in data is too slow")
    @SuppressWarnings("PMD.ArrayIsStoredDirectly") //copying the passed in data is too slow
    public MoveRequest(Object[] key, byte[][] value) {
      this.key = key;
      this.content = value;
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

  private ConcurrentHashMap<String, ConcurrentHashMap<String, List<Object[]>>> entriesToDelete = new ConcurrentHashMap<>();
  private String tableToDeleteEntriesFrom = null;

  private Index addedAfter;

  public Index getAddedAfter() {
    return addedAfter;
  }

  public void notifyAdded(Object[] key, String tableName, String indexName) {
    if (tableName.equals(currTableRepartitioning) && indexName.equals(currIndexRepartitioning)) {
      Index after = addedAfter;
      if (after != null) {
        after.put(key, 0);
      }
    }
  }

  public byte[] rebalanceOrderedIndex(String command, byte[] body) {
    command = command.replace(":rebalanceOrderedIndex:", ":doRebalanceOrderedIndex:");
    databaseServer.getLongRunningCommands().addCommand(databaseServer.getLongRunningCommands().createSingleCommand(command, body));
    return null;
  }

  public byte[] doRebalanceOrderedIndex(final String command, final byte[] body) {
    try {
      final String[] parts = command.split(":");
      final String dbName = parts[4];
      final String tableName = parts[5];
      final String indexName = parts[6];

      final Index index = indices.get(dbName).getIndices().get(tableName).get(indexName);
      final TableSchema tableSchema = common.getTables(dbName).get(tableName);
      final IndexSchema indexSchema = tableSchema.getIndices().get(indexName);

      addedAfter = new Index(tableSchema, indexName, index.getComparators());

      for (int k = 0; k < 3; k++) {
        common.getSchemaReadLock(dbName).lock();
        try {
          currTableRepartitioning = tableName;
          currIndexRepartitioning = indexName;

          tableToDeleteEntriesFrom = tableName;

          ConcurrentHashMap<String, List<Object[]>> indicesToDelete = getIndicesToDeleteFrom(tableName, indexName);
          final List<Object[]> keysToDelete = indicesToDelete.get(indexName);


          //          DataInputStream in = new DataInputStream(new ByteArrayInputStream(body));
          //          int partitionCount = in.readInt();
          //          TableSchema.Partition[] partitions = new TableSchema.Partition[partitionCount];
          //          for (int i = 0; i < partitions.length; i++) {
          //            partitions[i] = new TableSchema.Partition();
          //            partitions[i].deserialize(tableSchema, in);
          //          }
          //
          //          //todo: when should we change the schema?
          //          indexSchema.setPartitions(partitions);
          String[] indexFields = indexSchema.getFields();
          final int[] fieldOffsets = new int[indexFields.length];
          for (int i = 0; i < indexFields.length; i++) {
            fieldOffsets[i] = tableSchema.getFieldOffset(indexFields[i]);
          }
          Map.Entry<Object[], Long> entry = index.firstEntry();
          if (entry != null) {
            final Map<Integer, List<MoveRequest>> moveRequests = new HashMap<>();
            for (int i = 0; i < databaseServer.getShardCount(); i++) {
              moveRequests.put(i, new ArrayList<MoveRequest>());
            }

            index.iterate(new Index.Visitor(){
              @Override
              public void visit(Object[] key, long value) throws IOException {
                try {
                  List<Integer> selectedShards = findOrderedPartitionForRecord(true, false, fieldOffsets, common, tableSchema, indexName, null, BinaryExpression.Operator.equal, null, key, null);
                  int shard = selectedShards.get(0);
//                  if (selectedShards.size() != 1) {
//                    throw new IllegalStateException("Expected to select one partition to move entry to: found=" + selectedShards.size());
//                  }
                  if (shard != databaseServer.getShard()) {
                    byte[][] content = null;
  //                      if (addedAfter.get(entry.getKey()) != null) {
  //                        entry = index.higherEntry(entry.getKey());
  //                        if (entry == null) {
  //                          break;
  //                        }
  //                        continue;
  //                      }
                    synchronized (index) {
                      if (indexSchema.isPrimaryKey()) {
                        content = databaseServer.fromUnsafeToRecords(value);
                      }
                      else {
                        content = databaseServer.fromUnsafeToKeys(value);
                      }
                    }
                    if (indexSchema.isPrimaryKey()) {
                      byte[][] newContent = new byte[content.length][];
                      for (int i = 0; i < content.length; i++) {
                        Record record = new Record(dbName, common, content[i]);
                        record.setDbViewNumber(common.getSchemaVersion());
                        record.setDbViewFlags(Record.DB_VIEW_FLAG_DELETING);
                        newContent[i] = record.serialize(common);
                      }
                      synchronized (index) {
                        databaseServer.freeUnsafeIds(value);

                        value = databaseServer.toUnsafeFromRecords(newContent);
                        index.put(key, value);
                      }
                    }

                    List<MoveRequest> list = moveRequests.get(shard);
                    list.add(new MoveRequest(key, content));
                    if (list.size() > 1000) {
                      moveIndexEntriesToShard(dbName, tableName, indexName, indexSchema.isPrimaryKey(), shard, list);
                      for (MoveRequest request : list) {
                        //index.remove(request.getKey());
                        synchronized (keysToDelete) {
                          keysToDelete.add(request.getKey());
                        }
                      }
                      list.clear();
                    }
                  }
                }
                catch (Exception t) {
                  throw new DatabaseException("Error moving record: table=" + parts[4] + INDEX_STR + parts[5], t);
                }
              }
            });
            for (int i = 0; i < databaseServer.getShardCount(); i++) {
              List<MoveRequest> list = moveRequests.get(i);
              if (list.size() != 0) {
                moveIndexEntriesToShard(dbName, tableName, indexName, indexSchema.isPrimaryKey(), i, list);
                for (MoveRequest request : list) {
                  //index.remove(request.getKey());
                  synchronized (keysToDelete) {
                    keysToDelete.add(request.getKey());
                  }
                }
                list.clear();
              }
            }
          }
        }
        finally {
          common.getSchemaReadLock(dbName).unlock();
        }
        // Thread.sleep(5000);
      }
      String notifyCommand = "DatabaseServer:notifyRepartitioningComplete:1:" + common.getSchemaVersion() + ":" + dbName + ":" + databaseServer.getShard() + ":" + databaseServer.getReplica();
      Random rand = new Random(System.currentTimeMillis());
      databaseServer.getDatabaseClient().send(null, 0, rand.nextLong(), notifyCommand, null, DatabaseClient.Replica.master);
    }
    catch (Exception e) {
      logger.error("Error rebalancing index", e);
    }
    finally {
      addedAfter = null;
    }
    return null;
  }

  private ConcurrentHashMap<String, List<Object[]>> getIndicesToDeleteFrom(String tableName, String indexName) {
    if (!entriesToDelete.containsKey(tableName)) {
      entriesToDelete.put(tableName, new ConcurrentHashMap<String, List<Object[]>>());
    }
    ConcurrentHashMap<String, List<Object[]>> indicesToDelete = entriesToDelete.get(tableName);
    if (!indicesToDelete.containsKey(indexName)) {
      indicesToDelete.put(indexName, new ArrayList<Object[]>());
    }
    return indicesToDelete;
  }

  public byte[] deleteMovedIndexEntries(String command, final byte[] body) {
    command = command.replace(":deleteMovedIndexEntries:", ":doDeleteMovedIndexEntries:");
    databaseServer.getLongRunningCommands().addCommand(databaseServer.getLongRunningCommands().createSingleCommand(command, body));
    return null;
  }

  public byte[] doDeleteMovedIndexEntries(final String command, final byte[] body) {
    try {
      currTableRepartitioning = null;
      currIndexRepartitioning = null;

      String[] parts = command.split(":");
      String dbName = parts[4];
      String tableName = parts[5];
      String indexName = parts[6];
      Index index = indices.get(dbName).getIndices().get(tableName).get(indexName);
      if (entriesToDelete.get(tableName) == null) {
        return null;
      }
      List<Object[]> keys = entriesToDelete.get(tableName).get(indexName);
      logger.info("Deleting moved index entries: table=" + tableToDeleteEntriesFrom + INDEX_STR + indexName + ", count=" + keys.size());
      if (keys == null) {
        return null;
      }
      synchronized (keys) {
        for (Object[] key : keys) {
          synchronized (index) {
            Long existingValue = index.remove(key);
            if (existingValue != null) {
              databaseServer.freeUnsafeIds(existingValue);
            }
          }
        }
        keys.clear();
      }

      entriesToDelete.clear();
      tableToDeleteEntriesFrom = null;

      logger.info("Deleting moved index entries from index - end: table=" + tableName + ", shard=" + databaseServer.getShard() + ", replica=" + databaseServer.getReplica());

      String notifyCommand = "DatabaseServer:notifyDeletingComplete:1:" + common.getSchemaVersion() + ":" + dbName + ":" + databaseServer.getShard() + ":" + databaseServer.getReplica();
      Random rand = new Random(System.currentTimeMillis());
      databaseServer.getDatabaseClient().send(null, 0, rand.nextLong(), notifyCommand, null, DatabaseClient.Replica.master);

    }
    catch (Exception e) {
      logger.error("Error rebalancing index", e);
    }
    return null;
  }

  private void moveIndexEntriesToShard(
      String dbName, String tableName, String indexName, boolean primaryKey, int shard, List<MoveRequest> moveRequests) {
    try {
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytesOut);
      DataUtil.writeVLong(out, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
      out.writeUTF(tableName);
      out.writeUTF(indexName);
      DataUtil.ResultLength resultLength = new DataUtil.ResultLength();
      DataUtil.writeVLong(out, moveRequests.size(), resultLength);
      for (MoveRequest moveRequest : moveRequests) {
        byte[] bytes = DatabaseCommon.serializeKey(common.getTables(dbName).get(tableName), indexName, moveRequest.key);
        out.write(bytes);
        byte[][] content = moveRequest.getContent();
        if (primaryKey) {
          for (int i = 0; i < content.length; i++) {
            byte[] recordBytes = content[i];
            Record record = new Record(dbName, common, recordBytes);
            record.setDbViewNumber(common.getSchemaVersion());
            record.setDbViewFlags(Record.DB_VIEW_FLAG_ADDING);
            content[i] = record.serialize(common);
          }
        }
        DataUtil.writeVLong(out, content.length, resultLength);
        for (int i = 0; i < content.length; i++) {
          DataUtil.writeVLong(out, content[i].length, resultLength);
          out.write(content[i]);
        }
      }
      out.close();

      byte[] body = bytesOut.toByteArray();

      String command = "DatabaseServer:moveIndexEntries:1:" + common.getSchemaVersion() + ":" + dbName;
      databaseServer.getDatabaseClient().send(null, shard, databaseServer.getReplica(), command, body, DatabaseClient.Replica.specified);
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public byte[] moveIndexEntries(String command, byte[] body) {
    try {
      String[] parts = command.split(":");
      String dbName = parts[4];

      DataInputStream in = new DataInputStream(new ByteArrayInputStream(body));
      long serializationVersion = DataUtil.readVLong(in);
      String tableName = in.readUTF();
      String indexName = in.readUTF();
      DataUtil.ResultLength resultLength = new DataUtil.ResultLength();
      int count = (int) DataUtil.readVLong(in, resultLength);
      List<MoveRequest> moveRequests = new ArrayList<>();
      for (int i = 0; i < count; i++) {
        Object[] key = DatabaseCommon.deserializeKey(common.getTables(dbName).get(tableName), in);
        int contentCount = (int) DataUtil.readVLong(in, resultLength);
        byte[][] content = new byte[contentCount][];
        for (int j = 0; j < contentCount; j++) {
          int len = (int) DataUtil.readVLong(in, resultLength);
          byte[] bytes = new byte[len];
          in.readFully(bytes);
          content[j] = bytes;
        }
        moveRequests.add(new MoveRequest(key, content));
      }
      TableSchema tableSchema = common.getTables(dbName).get(tableName);
      Index index = databaseServer.getIndices(dbName).getIndices().get(tableSchema.getName()).get(indexName);
      IndexSchema indexSchema = common.getTables(dbName).get(tableName).getIndices().get(indexName);
      databaseServer.getUpdateManager().doInsertKeys(moveRequests, index, tableName, indexSchema);
      return null;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
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

      if (partitions[0].getUpperKey() == null) {
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

  private static class IndexCounts {
    private ConcurrentHashMap<Integer, Long> counts = new ConcurrentHashMap<>();
  }

  private static class TableIndexCounts {
    private ConcurrentHashMap<String, IndexCounts> indices = new ConcurrentHashMap<>();
  }

  private static class GlobalIndexCounts {
    private ConcurrentHashMap<String, TableIndexCounts> tables = new ConcurrentHashMap<>();
  }

  public byte[] getIndexCounts(String command, byte[] body) {
    try {
      String[] parts = command.split(":");
      String dbName = parts[4];
      JsonDict ret = new JsonDict();
      JsonArray tables = ret.putArray("tables");
      for (Map.Entry<String, ConcurrentHashMap<String, Index>> entry : databaseServer.getIndices(dbName).getIndices().entrySet()) {
        String tableName = entry.getKey();
        JsonDict tableDict = new JsonDict();
        tables.addDict(tableDict);
        tableDict.put(NAME_STR, tableName);
        JsonArray indicesArray = tableDict.putArray("indices");
        for (Map.Entry<String, Index> indexEntry : entry.getValue().entrySet()) {
          String indexName = indexEntry.getKey();
          JsonDict indexDict = new JsonDict();
          indexDict.put(NAME_STR, indexName);
          Index index = indexEntry.getValue();
          long size = index.size();
          indexDict.put("count", size);
          indicesArray.addDict(indexDict);
        }
      }
      return ret.toString(false).getBytes("utf-8");
    }
    catch (UnsupportedEncodingException e) {
      throw new DatabaseException(e);
    }
  }

  private GlobalIndexCounts getIndexCounts(String dbName) {
    try {
      GlobalIndexCounts ret = new GlobalIndexCounts();
      Random rand = new Random(System.currentTimeMillis());
      for (int shard = 0; shard < databaseServer.getShardCount(); shard++) {
        String command = "DatabaseServer:getIndexCounts:1:" + common.getSchemaVersion() + ":" + dbName;
        byte[] response = databaseServer.getDatabaseClient().send(null, shard, rand.nextLong(), command, null, DatabaseClient.Replica.master);
        JsonDict retDict = new JsonDict(new String(response, "utf-8"));
        JsonArray tablesArray = retDict.getArray("tables");
        for (int j = 0; j < tablesArray.size(); j++) {
          JsonDict tableDict = tablesArray.getDict(j);
          String tableName = tableDict.getString(NAME_STR);
          TableIndexCounts tableIndexCounts = ret.tables.get(tableName);
          if (tableIndexCounts == null) {
            tableIndexCounts = new TableIndexCounts();
            ret.tables.put(tableName, tableIndexCounts);
          }
          JsonArray indicesArray = tableDict.getArray("indices");
          for (int k = 0; k < indicesArray.size(); k++) {
            JsonDict indexDict = indicesArray.getDict(k);
            String indexName = indexDict.getString(NAME_STR);
            long count = indexDict.getLong("count");
            IndexCounts indexCounts = tableIndexCounts.indices.get(indexName);
            if (indexCounts == null) {
              indexCounts = new IndexCounts();
              tableIndexCounts.indices.put(indexName, indexCounts);
            }
            indexCounts.counts.put(shard, count);
          }
        }
      }
      for (Map.Entry<String, TableIndexCounts> entry : ret.tables.entrySet()) {
        for (Map.Entry<String, IndexCounts> indexEntry : entry.getValue().indices.entrySet()) {
          for (int i = 0; i < databaseServer.getShardCount(); i++) {
            Long count = indexEntry.getValue().counts.get(i);
            if (count == null) {
              indexEntry.getValue().counts.put(i, 0L);
            }
            logger.info("Repartitioner count: shard=" + i + ", dbName=" + dbName + ", table=" + entry.getKey() + ", index=" + indexEntry.getKey() + ", count=" + count);
          }
        }
      }
      return ret;
    }
    catch (UnsupportedEncodingException e) {
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
          String command = "DatabaseServer:beginRebalance:1:1:" + dbName + ":" + false;
          beginRebalance(command, (byte[])null);
        }
        Thread.sleep(100 * 1000);
      }
      catch (InterruptedException e) {
        return;
      }
      catch (Exception t) {
        logger.error("Error in master thread", t);
        try {
          Thread.sleep(100 * 1000);
        }
        catch (InterruptedException e) {
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

  public byte[] beginRebalance(String command, byte[] body) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    boolean force = Boolean.valueOf(parts[5]);
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

      config = config.getDict("database");
      boolean isInternal = false;
      if (config.hasKey("clientIsPrivate")) {
        isInternal = config.getBoolean("clientIsPrivate");
      }
      DatabaseServer.ServersConfig newConfig = new DatabaseServer.ServersConfig(config.getArray("shards"), config.getInt("replicationFactor"), isInternal);
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
      GlobalIndexCounts counts = getIndexCounts(dbName);
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
    for (Map.Entry<Integer, Long> countEntry : counts.counts.entrySet()) {
      long count = countEntry.getValue();
      if (count < min) {
        min = count;
      }
      if (count > max) {
        max = count;
      }
    }
    if (force || (double) min / (double) max < 0.90) {
      toRebalance.add(entry.getKey() + " " + indexName);
      logger.info("Adding toRebalance: table=" + entry.getKey() + ", index=" + indexName);
      return true;
    }
    logger.info("Not adding toRebalance: table=" + entry.getKey() + ", index=" + indexName + ", min=" + min + ", max=" + max + ", shardCount=" + counts.counts.size());
    return false;
  }
}
