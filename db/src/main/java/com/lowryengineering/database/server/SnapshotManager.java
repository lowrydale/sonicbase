package com.lowryengineering.database.server;

import com.lowryengineering.database.common.DatabaseCommon;
import com.lowryengineering.database.index.Index;
import com.lowryengineering.database.schema.IndexSchema;
import com.lowryengineering.database.schema.TableSchema;
import com.lowryengineering.database.util.DataUtil;
import org.codehaus.plexus.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class SnapshotManager {

  private static final String SNAPSHOT_STR = "snapshot/";
  private static final String INDEX_STR = ", index=";
  private static final String RATE_STR = ", rate=";
  private static final String DURATION_STR = ", duration(s)=";
  public static Logger logger = LoggerFactory.getLogger(SnapshotManager.class);

  public static final int SNAPSHOT_BUCKET_COUNT = 128;
  public static final int SNAPSHOT_SERIALIZATION_VERSION = 19;

  private final DatabaseServer server;
  private long lastSnapshot = -1;
  private ConcurrentHashMap<Integer, Integer> lockedSnapshots = new ConcurrentHashMap<Integer, Integer>();
  private boolean enableSnapshot = true;

  public SnapshotManager(DatabaseServer databaseServer) {
    this.server = databaseServer;
  }

  public void unlockSnapshot(int highestSnapshot) {
    lockedSnapshots.remove(highestSnapshot);
  }

  public String lockSnapshot(String dbName) {
    String dataRoot = new File(server.getDataDir(), SNAPSHOT_STR + server.getShard() + "/" + server.getReplica() + "/" + dbName).getAbsolutePath();
    File dataRootDir = new File(dataRoot);
    dataRootDir.mkdirs();

    int highestSnapshot = getHighestSafeSnapshotVersion(dataRootDir);

    lockedSnapshots.put(highestSnapshot, highestSnapshot);

    return new File(dataRootDir, String.valueOf(highestSnapshot)).getAbsolutePath();
  }

  private int getHighestSafeSnapshotVersion(File dataRootDir) {
    int highestSnapshot = -1;
    try {
      String[] dirs = dataRootDir.list();
      if (dirs != null) {
        for (String dir : dirs) {
          int pos = dir.indexOf('.');
          if (pos == -1) {
            try {
              int value = Integer.valueOf(dir);
              if (value > highestSnapshot) {
                highestSnapshot = value;
              }
            }
            catch (Exception t) {
              logger.error("Error parsing dir: " + dir, t);
            }
          }
        }
      }
    }
    catch (Exception t) {
      logger.error("Error getting highest snapshot version");
    }
    return highestSnapshot;
  }

  private int getHighestUnsafeSnapshotVersion(File dataRootDir) {
    String[] dirs = dataRootDir.list();
    int highestSnapshot = -1;
    if (dirs != null) {
      for (String dir : dirs) {
        if (dir.equals("schema.bin")) {
          continue;
        }
        int pos = dir.indexOf('.');
        if (pos != -1) {
          dir = dir.substring(0, pos);
        }
        try {
          int value = Integer.valueOf(dir);
          if (value > highestSnapshot) {
            highestSnapshot = value;
          }
        }
        catch (Exception t) {
          logger.error("Error parsing dir: " + dir, t);
        }
      }
    }
    return highestSnapshot;
  }

  public void recoverFromSnapshot(String dbName) throws Exception {

    server.purge(dbName);

    String dataRoot = new File(server.getDataDir(), SNAPSHOT_STR + server.getShard() + "/" + server.getReplica() + "/" + dbName).getAbsolutePath();
    File dataRootDir = new File(dataRoot);
    dataRootDir.mkdirs();
    int highestSnapshot = getHighestSafeSnapshotVersion(dataRootDir);

    if (highestSnapshot == -1) {
      return;
    }

    final File snapshotDir = new File(dataRoot, String.valueOf(highestSnapshot));

    logger.info("Recover from snapshot: dir=" + snapshotDir.getAbsolutePath());

    ThreadPoolExecutor executor = new ThreadPoolExecutor(SNAPSHOT_BUCKET_COUNT, SNAPSHOT_BUCKET_COUNT, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

    final AtomicLong recoveredCount = new AtomicLong();
    recoveredCount.set(0);

    Map<String, TableSchema> tables = server.getCommon().getTables(dbName);
    for (Map.Entry<String, TableSchema> schema : tables.entrySet()) {
      logger.info("Deserialized table schema: table=" + schema.getKey());
      for (Map.Entry<String, IndexSchema> index : schema.getValue().getIndices().entrySet()) {
        logger.info("Deserialized index: table=" + schema.getKey() + INDEX_STR + index.getKey());
        server.getSchemaManager().doCreateIndex(dbName, schema.getValue(), index.getKey(), index.getValue().getFields());
      }

    }

    final long indexBegin = System.currentTimeMillis();
    recoveredCount.set(0);
    final AtomicLong lastLogged = new AtomicLong(System.currentTimeMillis());
    File file = snapshotDir;
    if (file.exists()) {
      for (File tableFile : file.listFiles()) {
        final String tableName = tableFile.getName();
        if (!tableFile.isDirectory()) {
          continue;
        }
        final AtomicBoolean firstThread = new AtomicBoolean();
        for (File indexDir : tableFile.listFiles()) {
          final String indexName = indexDir.getName();
          List<Future> futures = new ArrayList<>();
          final AtomicInteger offset = new AtomicInteger();
          for (final File indexFile : indexDir.listFiles()) {
            final int currOffset = offset.get();
            logger.info("Recovering: table=" + tableName + INDEX_STR + indexName);
            final TableSchema tableSchema = server.getCommon().getTables(dbName).get(tableName);
            final IndexSchema indexSchema = tableSchema.getIndices().get(indexName);
            final Index index = server.getIndices(dbName).getIndices().get(tableName).get(indexName);
            logger.info("Table: table=" + tableName + ", indexName=" + indexName +
                ", schemaNull=" + (indexSchema == null) +
                ", byIdNull=" + (tableSchema.getIndexesById().get(indexSchema.getIndexId()) == null) +
                ", indexId=" + indexSchema.getIndexId());
            futures.add(executor.submit(new Callable<Boolean>() {
                                          @Override
                                          public Boolean call() throws Exception {
                                            int countForFile = 0;
                                            try (DataInputStream inStream = new DataInputStream(new BufferedInputStream(new FileInputStream(indexFile)))) {
                                              boolean isPrimaryKey = indexSchema.isPrimaryKey();
                                              DataUtil.ResultLength resultLength = new DataUtil.ResultLength();
                                              while (true) {

                                                if (!inStream.readBoolean()) {
                                                  break;
                                                }
                                                Object[] key = DatabaseCommon.deserializeKey(tableSchema, inStream);

                                                int count = (int) DataUtil.readVLong(inStream, resultLength);
                                                byte[][] records = new byte[count][];
                                                for (int i = 0; i < records.length; i++) {
                                                  int len = (int) DataUtil.readVLong(inStream, resultLength);
                                                  records[i] = new byte[len];
                                                  inStream.readFully(records[i]);
                                                }

                                                long address;
                                                if (isPrimaryKey) {
                                                  address = server.toUnsafeFromRecords(records);
                                                }
                                                else {
                                                  address = server.toUnsafeFromKeys(records);
                                                }

                                                index.put(key, address);

                                                countForFile++;
                                                recoveredCount.incrementAndGet();
                                                if (currOffset == 0 && (System.currentTimeMillis() - lastLogged.get()) > 2000) {
                                                  lastLogged.set(System.currentTimeMillis());
                                                  logger.info("Recover progress - table=" + tableName + INDEX_STR + indexName + ": count=" + recoveredCount.get() + RATE_STR +
                                                      ((float) recoveredCount.get() / (float) ((System.currentTimeMillis() - indexBegin)) * 1000f) +
                                                      DURATION_STR + (System.currentTimeMillis() - indexBegin) / 1000f);
                                                }
                                              }
                                            }
                                            catch (EOFException e) {
                                              throw new Exception(e);
                                            }
                                            return true;
                                          }
                                        }
            ));
            offset.incrementAndGet();
          }
          for (Future future : futures) {
            try {
              if (!(Boolean) future.get()) {
                throw new Exception("Error recovering from bucket");
              }
            }
            catch (Exception t) {
              throw new Exception("Error recovering from bucket", t);
            }
          }
          logger.info("Recover progress - finished index. table=" + tableName + INDEX_STR + indexName + ": count=" + recoveredCount.get() + RATE_STR +
              ((float) recoveredCount.get() / (float) ((System.currentTimeMillis() - indexBegin)) * 1000f) +
              DURATION_STR + (System.currentTimeMillis() - indexBegin) / 1000f);

        }
      }
    }
    logger.info("Recover progress - finished all indices. count=" + recoveredCount.get() + RATE_STR +
        ((float) recoveredCount.get() / (float) ((System.currentTimeMillis() - indexBegin)) * 1000f) +
        DURATION_STR + (System.currentTimeMillis() - indexBegin) / 1000f);
  }

  public void runSnapshotLoop() {

    while (true) {
      try {
        if (lastSnapshot != -1) {
          long timeToWait = 30 * 1000 - (System.currentTimeMillis() - lastSnapshot);
          if (timeToWait > 0) {
            Thread.sleep(timeToWait);
          }
        }
        while (!enableSnapshot) {
          Thread.sleep(1000);
        }

        List<String> dbNames = server.getDbNames(server.getDataDir());
        for (String dbName : dbNames) {
          runSnapshot(dbName);
        }
      }
      catch (Exception e) {
        logger.error("Error creating snapshot", e);
      }
    }
  }

  public void runSnapshot(String dbName) throws IOException, InterruptedException, ParseException {
    lastSnapshot = System.currentTimeMillis();
    long lastTimeStartedSnapshot = System.currentTimeMillis();

    long begin = System.currentTimeMillis();
    logger.info("Snapshot - begin");

    //todo: may want to gzip this
    String dataRoot = new File(server.getDataDir(), SNAPSHOT_STR + server.getShard() + "/" + server.getReplica() + "/" + dbName).getAbsolutePath();
    File dataRootDir = new File(dataRoot);
    dataRootDir.mkdirs();
    int highestSnapshot = getHighestUnsafeSnapshotVersion(dataRootDir);

    File file = new File(dataRoot, String.valueOf(highestSnapshot + 1) + ".in-process");
    file.mkdirs();

    logger.info("Snapshot to: dir=" + file.getAbsolutePath());

    File versionFile = new File(file, "version.txt");
    try (BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(versionFile))) {
      out.write(String.valueOf(SNAPSHOT_SERIALIZATION_VERSION).getBytes());
    }

    final AtomicLong countSaved = new AtomicLong();
    final AtomicLong lastLogged = new AtomicLong(System.currentTimeMillis());

    final AtomicInteger tableCount = new AtomicInteger();
    final AtomicInteger indexCount = new AtomicInteger();
    final DataUtil.ResultLength resultLength = new DataUtil.ResultLength();
    for (final Map.Entry<String, TableSchema> tableEntry : server.getCommon().getTables(dbName).entrySet()) {
      tableCount.incrementAndGet();
      for (final Map.Entry<String, IndexSchema> indexEntry : tableEntry.getValue().getIndices().entrySet()) {
        indexCount.incrementAndGet();
        final long subBegin = System.currentTimeMillis();
        final AtomicLong savedCount = new AtomicLong();
        final Index index = server.getIndices(dbName).getIndices().get(tableEntry.getKey()).get(indexEntry.getKey());

        final DataOutputStream[] outStreams = new DataOutputStream[SNAPSHOT_BUCKET_COUNT];
        try {
          for (int i = 0; i < outStreams.length; i++) {
            File currFile = new File(file, tableEntry.getKey() + "/" + indexEntry.getKey() + "/" + i + ".bin");
            currFile.getParentFile().mkdirs();
            outStreams[i] = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(currFile)));
          }

          final boolean isPrimaryKey = indexEntry.getValue().isPrimaryKey();
          index.iterate(new Index.Visitor() {
            @Override
            public void visit(Object[] key, long value) throws IOException {
              int bucket = (int) (countSaved.incrementAndGet() % SNAPSHOT_BUCKET_COUNT);

              outStreams[bucket].writeBoolean(true);
              byte[] keyBytes = DatabaseCommon.serializeKey(tableEntry.getValue(), indexEntry.getKey(), key);
              outStreams[bucket].write(keyBytes);

              byte[][] records = null;
              synchronized (index) {
                if (isPrimaryKey) {
                  records = server.fromUnsafeToRecords(value);
                }
                else {
                  records = server.fromUnsafeToKeys(value);
                }
              }
              DataUtil.writeVLong(outStreams[bucket], records.length, resultLength);
              for (byte[] record : records) {
                DataUtil.writeVLong(outStreams[bucket], record.length, resultLength);
                outStreams[bucket].write(record);

                savedCount.incrementAndGet();
                if (System.currentTimeMillis() - lastLogged.get() > 2000) {
                  lastLogged.set(System.currentTimeMillis());
                  logger.info("Snapshot progress - records: count=" + savedCount + RATE_STR +
                      ((float) savedCount.get() / (float) ((System.currentTimeMillis() - subBegin)) * 1000f) +
                      DURATION_STR + (System.currentTimeMillis() - subBegin) / 1000f +
                      ", table=" + tableEntry.getKey() + INDEX_STR + indexEntry.getKey());
                }
              }
            }
          });
          logger.info("Snapshot progress - finished index: count=" + savedCount + RATE_STR +
              ((float) savedCount.get() / (float) ((System.currentTimeMillis() - subBegin)) * 1000f) +
              DURATION_STR + (System.currentTimeMillis() - subBegin) / 1000f +
              ", table=" + tableEntry.getKey() + INDEX_STR + indexEntry.getKey());
        }
        catch (Exception e) {
          logger.error("Error creating snapshot", e);
        }
        finally {
          for (DataOutputStream outStream : outStreams) {
            outStream.writeBoolean(false);
            outStream.flush();
            outStream.close();
          }
        }
      }
    }

    File snapshotDir = new File(dataRoot, String.valueOf(highestSnapshot + 1));
    file.renameTo(snapshotDir);

    deleteOldSnapshots(dbName);

    try {
      server.getLogManager().deleteOldLogs(lastTimeStartedSnapshot);
    }
    catch (Exception e) {
      logger.error("Error deleting old logs", e);
    }

    logger.info("Snapshot - end: snapshotId=" + (highestSnapshot + 1) + ", duration=" + (System.currentTimeMillis() - begin));
  }

  private void deleteOldSnapshots(String dbName) throws IOException, InterruptedException, ParseException {
    String dataRoot = new File(server.getDataDir(), SNAPSHOT_STR + server.getShard() + "/" + server.getReplica() + "/" + dbName).getAbsolutePath();
    File dataRootDir = new File(dataRoot);
    dataRootDir.mkdirs();
    int highestSnapshot = getHighestSafeSnapshotVersion(dataRootDir);

    for (String dirStr : dataRootDir.list()) {
      int dirNum = -1;
      try {
        dirNum = Integer.valueOf(dirStr);
      }
      catch (Exception t) {
        //expected numeric format problems
      }
      if (dirStr.contains("in-process") || (dirNum != -1 && dirNum < (highestSnapshot - 1))) {
        if (!lockedSnapshots.containsKey(dirNum)) {
          File dir = new File(dataRootDir, dirStr);
          logger.info("Deleting snapshot: " + dir.getAbsolutePath());
          FileUtils.deleteDirectory(dir);
        }
      }
    }

  }

  public void enableSnapshot(boolean enable) {
    this.enableSnapshot = enable;
  }

}
