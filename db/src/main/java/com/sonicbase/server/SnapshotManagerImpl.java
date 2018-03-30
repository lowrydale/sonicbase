package com.sonicbase.server;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.*;
import com.sonicbase.index.Index;
import com.sonicbase.index.Indices;
import com.sonicbase.query.BinaryExpression;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import org.apache.commons.io.FileUtils;
import org.apache.giraph.utils.Varint;

import java.io.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class SnapshotManagerImpl implements SnapshotManager {

  public static final int SNAPSHOT_PARTITION_COUNT = 128;

  private static final String SNAPSHOT_STR = "snapshot/";
  private static final String INDEX_STR = ", index=";
  private static final String RATE_STR = ", rate=";
  private static final String DURATION_STR = ", duration(s)=";
  public Logger logger;

  private final DatabaseServer server;
  private long lastSnapshot = -1;
  private boolean enableSnapshot = true;
  private boolean isRecovering;
  private boolean shutdown;

  public SnapshotManagerImpl(DatabaseServer databaseServer) {
    this.server = databaseServer;
    this.logger = new Logger(databaseServer.getDatabaseClient());
  }

  public static int getHighestCommittedSnapshotVersion(File snapshotRootDir, Logger logger) {
    int highestSnapshot = -1;
    try {
      String[] dirs = snapshotRootDir.list();
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

  private int getHighestUnCommittedSnapshotVersion(File snapshotRootDir) {
    String[] dirs = snapshotRootDir.list();
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

  private long totalBytes = 0;
  private AtomicLong finishedBytes = new AtomicLong();
  private int totalFileCount = 0;
  private int finishedFileCount = 0;
  private Exception errorRecovering = null;

  public void getPercentRecoverComplete(ComObject retObj) {
    if (totalBytes == 0) {
      retObj.put(ComObject.Tag.percentComplete, 0);
    }
    else {
      retObj.put(ComObject.Tag.percentComplete, (double) finishedBytes.get() / (double) totalBytes);
    }
    retObj.put(ComObject.Tag.stage, "recoveringSnapshot");
  }


  public Exception getErrorRecovering() {
    return errorRecovering;
  }

  @Override
  public long getBackupLocalFileSystemSize() {
    File dir = getSnapshotReplicaDir();
    return com.sonicbase.common.FileUtils.sizeOfDirectory(dir);
  }

  public void recoverFromSnapshot(String dbName) throws Exception {

    totalFileCount = 0;
    finishedFileCount = 0;
    totalBytes = 0;
    finishedBytes.set(0);
    errorRecovering = null;
    isRecovering = true;
    try {
      server.purge(dbName);

      String dataRoot = getSnapshotRootDir(dbName);
      File dataRootDir = new File(dataRoot);
      dataRootDir.mkdirs();

      server.getIndices().put(dbName, new Indices());

      Map<String, TableSchema> tables = server.getCommon().getTables(dbName);
      for (Map.Entry<String, TableSchema> schema : tables.entrySet()) {
        logger.info("Deserialized table schema: table=" + schema.getKey());
        for (Map.Entry<String, IndexSchema> index : schema.getValue().getIndices().entrySet()) {
          logger.info("Deserialized index: table=" + schema.getKey() + INDEX_STR + index.getKey());
          server.getSchemaManager().doCreateIndex(dbName, schema.getValue(), index.getKey(), index.getValue().getFields());
        }
      }

      int highestSnapshot = getHighestCommittedSnapshotVersion(dataRootDir, logger);

      if (highestSnapshot == -1) {
        return;
      }

      final File snapshotDir = new File(dataRoot, String.valueOf(highestSnapshot));

      logger.info("Recover from snapshot: dir=" + snapshotDir.getAbsolutePath());

      ThreadPoolExecutor executor = ThreadUtil.createExecutor(SNAPSHOT_PARTITION_COUNT, "SonicBase SnapshotManagerImpl recoverFromSnapshot Thread");

      try {
        final AtomicLong recoveredCount = new AtomicLong();
        recoveredCount.set(0);

        final long indexBegin = System.currentTimeMillis();
        recoveredCount.set(0);
        final AtomicLong lastLogged = new AtomicLong(System.currentTimeMillis());
        File file = snapshotDir;

        if (file.exists()) {
          for (File tableFile : file.listFiles()) {
            if (!tableFile.isDirectory()) {
              continue;
            }
            for (File indexDir : tableFile.listFiles()) {
              for (final File indexFile : indexDir.listFiles()) {
                totalBytes += indexFile.length();
                totalFileCount++;
              }
            }
          }
        }
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
                final IndexSchema indexSchema = server.getIndexSchema(dbName, tableSchema.getName(), indexName);
                final Index index = server.getIndex(dbName, tableName, indexName);
                logger.info("Table: table=" + tableName + ", indexName=" + indexName +
                    ", schemaNull=" + (indexSchema == null) +
                    ", byIdNull=" + (tableSchema.getIndexesById().get(indexSchema.getIndexId()) == null) +
                    ", indexId=" + indexSchema.getIndexId());
                futures.add(executor.submit(new Callable<Boolean>() {
                                              @Override
                                              public Boolean call() throws Exception {
                                                int countForFile = 0;
                                                try (DataInputStream inStream = new DataInputStream(new BufferedInputStream(new ByteCounterStream(finishedBytes, new FileInputStream(indexFile))))) {
                                                  boolean isPrimaryKey = indexSchema.isPrimaryKey();
                                                  while (true) {

                                                    //logger.info("pre check");
                                                    if (!inStream.readBoolean()) {
                                                      break;
                                                    }
                                                    //logger.info("post check");
                                                    Object[] key = DatabaseCommon.deserializeKey(tableSchema, inStream);

                                                    long updateTime = Varint.readUnsignedVarLong(inStream);

                                                    int count = (int) Varint.readSignedVarLong(inStream);
                                                    byte[][] records = new byte[count][];
                                                    for (int i = 0; i < records.length; i++) {
                                                      int len = (int) Varint.readSignedVarLong(inStream);
                                                      records[i] = new byte[len];
                                                      inStream.readFully(records[i]);
                                                    }

                                                    Object address;
                                                    if (isPrimaryKey) {
                                                      address = server.toUnsafeFromRecords(updateTime, records);
                                                      for (byte[] record : records) {
                                                        if ((Record.getDbViewFlags(record) & Record.DB_VIEW_FLAG_DELETING) == 0) {
                                                          index.addAndGetCount(1);
                                                        }
                                                      }
                                                    }
                                                    else {
                                                      address = server.toUnsafeFromKeys(updateTime, records);
                                                      for (byte[] record : records) {
                                                        if ((Record.getDbViewFlags(record) & Record.DB_VIEW_FLAG_DELETING) == 0) {
                                                          index.addAndGetCount(1);
                                                        }
                                                      }
                                                    }

                                                    if (index.put(key, address) != null) {
                                                      throw new DatabaseException("Key already exists");
                                                    }

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
                  finishedFileCount++;
                }
                catch (Exception t) {
                  errorRecovering = t;
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
      finally {
        executor.shutdownNow();
      }
    }
    catch (Exception e) {
      errorRecovering = e;
      throw e;
    }
    finally {
      isRecovering = false;
    }
  }

  private File getSnapshotReplicaDir() {
    return new File(server.getDataDir(), SNAPSHOT_STR + server.getShard() + "/" + server.getReplica());
  }
  private String getSnapshotRootDir(String dbName) {
    return new File(getSnapshotReplicaDir(), dbName).getAbsolutePath();
  }

  Thread snapshotThread = null;
  public void runSnapshotLoop() {
    if (snapshotThread != null) {
      snapshotThread.interrupt();
      try {
        snapshotThread.join();
      }
      catch (InterruptedException e) {
        throw new DatabaseException(e);
      }
    }
    snapshotThread = ThreadUtil.createThread(new Runnable(){
      @Override
      public void run() {
        while (!Thread.interrupted()) {
          try {
            if (lastSnapshot != -1) {
              long timeToWait = 30 * 1000 - (System.currentTimeMillis() - lastSnapshot);
              if (timeToWait > 0) {
                Thread.sleep(timeToWait);
              }
            }
            else {
              Thread.sleep(10000);
            }
            while (!enableSnapshot) {
              Thread.sleep(1000);
            }

//            File file = new File(getSnapshotReplicaDir(), "serializationVersion");
//            file.delete();
//            file.getParentFile().mkdirs();
//            try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))) {
//              writer.write(String.valueOf(DatabaseClient.SERIALIZATION_VERSION));
//            }

            List<String> dbNames = server.getDbNames(server.getDataDir());
            for (String dbName : dbNames) {
              runSnapshot(dbName);
            }
            server.getCommon().saveSchema(server.getClient(), server.getDataDir());
          }
          catch (InterruptedException e) {
            break;
          }
          catch (Exception e) {
            logger.error("Error creating snapshot", e);
          }
        }
      }
    }, "SonicBase Snapshot Thread");
    snapshotThread.start();
  }

  public void deleteRecord(String dbName, String tableName, TableSchema tableSchema, IndexSchema indexSchema, Object[] key, byte[] record, int[] fieldOffsets) {

    List<Integer> selectedShards = DatabaseClient.findOrderedPartitionForRecord(true, false,
        fieldOffsets, server.getClient().getCommon(), tableSchema,
        indexSchema.getName(), null, BinaryExpression.Operator.equal, null, key, null);
    if (selectedShards.size() == 0) {
      throw new DatabaseException("No shards selected for query");
    }

    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.serializationVersion, DatabaseClient.SERIALIZATION_VERSION);
    cobj.put(ComObject.Tag.keyBytes, DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), key));
    cobj.put(ComObject.Tag.schemaVersion, server.getCommon().getSchemaVersion());
    cobj.put(ComObject.Tag.dbName, dbName);
    cobj.put(ComObject.Tag.tableName, tableName);
    cobj.put(ComObject.Tag.indexName, indexSchema.getName());
    cobj.put(ComObject.Tag.method, "deleteRecord");
    server.getClient().send("DatabaseServer:deleteRecord", selectedShards.get(0), 0, cobj, DatabaseClient.Replica.def);

    cobj = new ComObject();
    cobj.put(ComObject.Tag.dbName, dbName);
    cobj.put(ComObject.Tag.schemaVersion, server.getCommon().getSchemaVersion());
    cobj.put(ComObject.Tag.tableName, tableName);
    cobj.put(ComObject.Tag.method, "deleteIndexEntry");
    cobj.put(ComObject.Tag.recordBytes, record);

    server.getClient().sendToAllShards(null, 0, cobj, DatabaseClient.Replica.def);
  }

  public void runSnapshot(final String dbName) throws IOException, InterruptedException, ParseException {
    lastSnapshot = System.currentTimeMillis();
    long lastTimeStartedSnapshot = System.currentTimeMillis();

    long begin = System.currentTimeMillis();
    logger.info("Snapshot - begin");

    //todo: may want to gzip this

    File snapshotRootDir = new File(getSnapshotRootDir(dbName));
    snapshotRootDir.mkdirs();
    int highestSnapshot = getHighestUnCommittedSnapshotVersion(snapshotRootDir);

    File file = new File(snapshotRootDir, String.valueOf(highestSnapshot + 1) + ".tmp");
    file.mkdirs();

    logger.info("Snapshot to: dir=" + file.getAbsolutePath());

    File versionFile = new File(file, "version.txt");
    try (BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(versionFile))) {
      out.write(String.valueOf(DatabaseClient.SERIALIZATION_VERSION).getBytes());
    }

    ObjectNode config = server.getConfig();
    ObjectNode expire = (ObjectNode) config.get("expireRecords");
    final Long deleteIfOlder;
    if (expire != null) {
      long duration = expire.get("durationMinutes").asLong();
      deleteIfOlder = System.currentTimeMillis() - duration * 60;
    }
    else {
      deleteIfOlder = null;
    }
    final AtomicLong countSaved = new AtomicLong();
    final AtomicLong lastLogged = new AtomicLong(System.currentTimeMillis());

    final AtomicInteger tableCount = new AtomicInteger();
    final AtomicInteger indexCount = new AtomicInteger();
    for (final Map.Entry<String, TableSchema> tableEntry : server.getCommon().getTables(dbName).entrySet()) {
      tableCount.incrementAndGet();
      for (final Map.Entry<String, IndexSchema> indexEntry : tableEntry.getValue().getIndices().entrySet()) {
        indexCount.incrementAndGet();

        String[] indexFields = indexEntry.getValue().getFields();
        final int[] fieldOffsets = new int[indexFields.length];
        for (int k = 0; k < indexFields.length; k++) {
          fieldOffsets[k] = tableEntry.getValue().getFieldOffset(indexFields[k]);
        }

        final long subBegin = System.currentTimeMillis();
        final AtomicLong savedCount = new AtomicLong();
        final Index index = server.getIndex(dbName, tableEntry.getKey(), indexEntry.getKey());

        final DataOutputStream[] outStreams = new DataOutputStream[SNAPSHOT_PARTITION_COUNT];
        try {
          for (int i = 0; i < outStreams.length; i++) {
            File currFile = new File(file, tableEntry.getKey() + "/" + indexEntry.getKey() + "/" + i + ".bin");
            currFile.getParentFile().mkdirs();
            outStreams[i] = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(currFile), 65_000));
          }

          final boolean isPrimaryKey = indexEntry.getValue().isPrimaryKey();
          Map.Entry<Object[], Object> first = index.firstEntry();
          if (first != null) {
            final Map.Entry<Object[], Object> last = index.lastEntry();
            index.visitTailMap(first.getKey(), new Index.Visitor(){
              @Override
              public boolean visit(Object[] key, Object value) throws IOException {
                int bucket = (int) (countSaved.incrementAndGet() % SNAPSHOT_PARTITION_COUNT);

//                if (DatabaseCommon.compareKey(index.getComparators(), key, last.getKey()) > 0) {
//                  return false;
//                }

                byte[][] records = null;
                long updateTime = 0;
                synchronized (index.getMutex(key)) {
                  Object currValue = index.get(key);
                  if (currValue == null || currValue.equals(0L)) {
                    //logger.error("null record: key=" + DatabaseCommon.keyToString(key));
                  }
                  else {
                    if (isPrimaryKey) {
                      records = server.fromUnsafeToRecords(currValue);
                    }
                    else {
                      records = server.fromUnsafeToKeys(currValue);
                    }
                    updateTime = server.getUpdateTime(currValue);
                  }
                }
                if (records != null) {
                  outStreams[bucket].writeBoolean(true);
                  byte[] keyBytes = DatabaseCommon.serializeKey(tableEntry.getValue(), indexEntry.getKey(), key);
                  outStreams[bucket].write(keyBytes);

                  Varint.writeUnsignedVarLong(updateTime,  outStreams[bucket]);

                  Varint.writeSignedVarLong(records.length, outStreams[bucket]);
                  for (byte[] record : records) {

                    if (deleteIfOlder != null) {
                      updateTime = Record.getUpdateTime(record);
                      if (updateTime < deleteIfOlder) {
                        deleteRecord(dbName, tableEntry.getKey(), tableEntry.getValue(), indexEntry.getValue(),
                            key, record, fieldOffsets);
                      }
                    }

                    Varint.writeSignedVarLong(record.length, outStreams[bucket]);
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
                return true;
              }
            });
          }

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

    File snapshotDir = new File(snapshotRootDir, String.valueOf(highestSnapshot + 1));

    file.renameTo(snapshotDir);

    deleteOldSnapshots(dbName);

    try {
      server.getLogManager().deleteOldLogs(lastTimeStartedSnapshot, false);
    }
    catch (Exception e) {
      logger.error("Error deleting old logs", e);
    }

    logger.info("Snapshot - end: snapshotId=" + (highestSnapshot + 1) + ", duration=" + (System.currentTimeMillis() - begin));
  }

  private void deleteOldSnapshots(String dbName) throws IOException, InterruptedException, ParseException {
    File snapshotRootDir = new File(getSnapshotRootDir(dbName));
    snapshotRootDir.mkdirs();
    int highestSnapshot = getHighestCommittedSnapshotVersion(snapshotRootDir, logger);

    for (String dirStr : snapshotRootDir.list()) {
      int dirNum = -1;
      try {
        dirNum = Integer.valueOf(dirStr);
      }
      catch (Exception t) {
        //expected numeric format problems
      }
      if (dirStr.contains("tmp") || (dirNum != -1 && dirNum < (highestSnapshot))) {
        File dir = new File(snapshotRootDir, dirStr);
        logger.info("Deleting snapshot: " + dir.getAbsolutePath());
        FileUtils.deleteDirectory(dir);
      }
    }
  }

  public void enableSnapshot(boolean enable) {
    this.enableSnapshot = enable;
    if (snapshotThread != null) {
      snapshotThread.interrupt();
      try {
        snapshotThread.join();
      }
      catch (InterruptedException e) {
      }
      snapshotThread = null;
    }
    if (enable) {
      runSnapshotLoop();
    }
  }

  public void deleteSnapshots() {
    File dir = getSnapshotReplicaDir();
    try {
      FileUtils.deleteDirectory(dir);
      dir.mkdirs();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public void backupFileSystemSchema(String directory, String subDirectory) {
    try {
      File file = new File(directory, subDirectory);
      file = new File(file, SNAPSHOT_STR + server.getShard() + "/0/schema.bin");
      file.getParentFile().mkdirs();

      File sourceFile = new File(getSnapshotReplicaDir(), "schema.bin");
      FileUtils.copyFile(sourceFile, file);


      file = new File(directory, subDirectory);
      file = new File(file, SNAPSHOT_STR + server.getShard() + "/0/config.bin");
      file.getParentFile().mkdirs();

      sourceFile = new File(getSnapshotReplicaDir(), "config.bin");
      if (sourceFile.exists()) {
        FileUtils.copyFile(sourceFile, file);
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public void backupFileSystem(String directory, String subDirectory) {
    try {
      File file = new File(directory, subDirectory);
      file = new File(file, SNAPSHOT_STR + server.getShard() + "/0");
      FileUtils.deleteDirectory(file);
      file.mkdirs();

      if (getSnapshotReplicaDir().exists()) {
        FileUtils.copyDirectory(getSnapshotReplicaDir(), file);
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public void restoreFileSystem(String directory, String subDirectory) {
    try {
      File file = new File(directory, subDirectory);
      file = new File(file, SNAPSHOT_STR + server.getShard() + "/0");

      if (getSnapshotReplicaDir().exists()) {
        FileUtils.deleteDirectory(getSnapshotReplicaDir());
      }
      getSnapshotReplicaDir().mkdirs();

      if (file.exists()) {
        FileUtils.copyDirectory(file, getSnapshotReplicaDir());
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public void backupAWSSchema(String bucket, String prefix, String subDirectory) {
    AWSClient awsClient = server.getAWSClient();
    File srcFile = new File(getSnapshotReplicaDir(), "schema.bin");
    subDirectory += "/" + SNAPSHOT_STR + server.getShard() + "/0";

    awsClient.uploadFile(bucket, prefix, subDirectory, srcFile);

    srcFile = new File(getSnapshotReplicaDir(), "config.bin");

    if (srcFile.exists()) {
      awsClient.uploadFile(bucket, prefix, subDirectory, srcFile);
    }
  }

  public void backupAWS(String bucket, String prefix, String subDirectory) {
    AWSClient awsClient = server.getAWSClient();
    File srcDir = getSnapshotReplicaDir();
    subDirectory += "/" + SNAPSHOT_STR + server.getShard() + "/0";

    awsClient.uploadDirectory(bucket, prefix, subDirectory, srcDir);
  }

  public void restoreAWS(String bucket, String prefix, String subDirectory) {
    try {
      AWSClient awsClient = server.getAWSClient();
      File destDir = getSnapshotReplicaDir();
      subDirectory += "/" + SNAPSHOT_STR + server.getShard() + "/0";

      FileUtils.deleteDirectory(getSnapshotReplicaDir());
      getSnapshotReplicaDir().mkdirs();

      awsClient.downloadDirectory(bucket, prefix, subDirectory, destDir);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public void shutdown() {
    this.shutdown = true;
    if (snapshotThread != null) {
      snapshotThread.interrupt();
      try {
        snapshotThread.join();
      }
      catch (InterruptedException e) {
        throw new DatabaseException(e);
      }
    }
  }

  public void getFilesForCurrentSnapshot(List<String> files) {
    File replicaDir = getSnapshotReplicaDir();
    getFilesFromDirectory(replicaDir, files);
  }

  @Override
  public File getSortedDeltaFile(String dbName, String s, String key, String key1, int i) {
    return null;
  }

  @Override
  public File getDeletedDeltaDir(String dbName, String s, String key, String key1) {
    return null;
  }

  private void getFilesFromDirectory(File dir, List<String> files) {
    File[] currFiles = dir.listFiles();
    if (currFiles != null) {
      for (File file : currFiles) {
        if (file.isDirectory()) {
          getFilesFromDirectory(file, files);
        }
        else {
          files.add(file.getAbsolutePath());
        }
      }
    }
  }

  public void deleteTempDirs() {
    try {
      File dir = getSnapshotReplicaDir();
      doDeleteTempDirs(dir);
    }
    catch (Exception e) {

    }
  }

  @Override
  public void deleteDeletedDirs() {

  }

  private void doDeleteTempDirs(File dir) throws IOException {
    File[] files = dir.listFiles();
    if (files != null) {
      for (File file : files) {
        if (file.isDirectory()) {
          if (file.getName().contains("tmp")) {
            FileUtils.deleteDirectory(file);
          }
          else {
            doDeleteTempDirs(file);
          }
        }
      }
    }
  }

  public boolean isRecovering() {
    return isRecovering;
  }

  private class ByteCounterStream extends InputStream {
    private final FileInputStream stream;
    private final AtomicLong finishedBytes;

    public ByteCounterStream(AtomicLong finishedBytes, FileInputStream fileInputStream) {
      this.stream = fileInputStream;
      this.finishedBytes = finishedBytes;
    }

    public synchronized void reset() throws IOException {
      stream.reset();
    }

    public boolean markSupported() {
      return stream.markSupported();
    }

    public synchronized void mark(int readlimit) {
      stream.mark(readlimit);
    }

    public long skip(long n) throws IOException {
      return stream.skip(n);
    }

    public int available() throws IOException {
      return stream.available();
    }

    public void close() throws IOException {
      stream.close();
    }

    public int read(byte b[]) throws IOException {
      int read = stream.read(b);
      if (read != -1) {
        finishedBytes.addAndGet(read);
      }
      return read;
    }

    public int read(byte b[], int off, int len) throws IOException {
      int read = stream.read(b, off, len);
      if (read != -1) {
        finishedBytes.addAndGet(read);
      }
      return read;
    }

    @Override
    public int read() throws IOException {
      int read = stream.read();
      if (read != -1) {
        finishedBytes.addAndGet(read);
      }
      return read;
    }
  }
}
