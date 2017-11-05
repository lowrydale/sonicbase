package com.sonicbase.server;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.*;
import com.sonicbase.index.Index;
import com.sonicbase.index.Indices;
import com.sonicbase.index.Repartitioner;
import com.sonicbase.query.BinaryExpression;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.DateUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.giraph.utils.Varint;

import java.io.*;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class DeltaManager {

  public static final int SNAPSHOT_PARTITION_COUNT = 128;

  private static final String DELTA_STR = "delta/";
  private static final String INDEX_STR = ", index=";
  private static final String RATE_STR = ", rate=";
  private static final String DURATION_STR = ", duration(s)=";
  public Logger logger;

  private final DatabaseServer server;
  private long lastSnapshot = -1;
  private boolean enableSnapshot = true;
  private boolean isRecovering;

  public DeltaManager(DatabaseServer databaseServer) {
    this.server = databaseServer;
    this.logger = new Logger(databaseServer.getDatabaseClient());
  }

  public static int getHighestCommittedSnapshotVersion(File snapshotRootDir, Logger logger) {
    int highestSnapshot = -1;
    try {
      String[] dirs = snapshotRootDir.list();
      if (dirs != null) {
        for (String dir : dirs) {
          if (!dir.contains("full")) {
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

  private AtomicReference<String> currStage = new AtomicReference<>();
  private AtomicLong totalBytes = new AtomicLong();
  private AtomicLong finishedBytes = new AtomicLong();
  private int totalFileCount = 0;
  private int finishedFileCount = 0;
  private Exception errorRecovering = null;

  public void getPercentRecoverComplete(ComObject retObj) {
    if (totalBytes.get() == 0) {
      retObj.put(ComObject.Tag.percentComplete, 0);
    }
    else {
      retObj.put(ComObject.Tag.percentComplete, (double) finishedBytes.get() / (double) totalBytes.get());
    }
    retObj.put(ComObject.Tag.stage, currStage.get());
  }

  public Exception getErrorRecovering() {
    return errorRecovering;
  }


  private File getSnapshotReplicaDir() {
    return new File(server.getDataDir(), DELTA_STR + server.getShard() + "/" + server.getReplica());
  }
  private String getSnapshotRootDir(String dbName) {
    return new File(getSnapshotReplicaDir(), dbName).getAbsolutePath();
  }

  Thread snapshotThread = null;
  public void runSnapshotLoop() {
    snapshotThread = new Thread(new Runnable(){
      @Override
      public void run() {
        while (true) {
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
    });
    snapshotThread.start();
  }

  private Object[] lastkey;

  public void deleteRecord(String dbName, String tableName, TableSchema tableSchema, IndexSchema indexSchema, Object[] key, byte[] record, int[] fieldOffsets) {

    List<Integer> selectedShards = Repartitioner.findOrderedPartitionForRecord(true, false,
        fieldOffsets, server.getClient().getCommon(), tableSchema,
        indexSchema.getName(), null, BinaryExpression.Operator.equal, null, key, null);
    if (selectedShards.size() == 0) {
      throw new DatabaseException("No shards selected for query");
    }

    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.serializationVersion, DatabaseServer.SERIALIZATION_VERSION);
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

    long begin = System.currentTimeMillis();
    logger.info("Snapshot - begin");

    File snapshotRootDir = new File(getSnapshotRootDir(dbName));
    snapshotRootDir.mkdirs();
    int highestSnapshot = getHighestCommittedSnapshotVersion(snapshotRootDir, logger);

    int dirCount = 0;
    File[] dirs = snapshotRootDir.listFiles();
    if (dirs != null) {
      dirCount = dirs.length;
    }

    File fullDir = new File(snapshotRootDir, "full/sorted");
    final boolean isFull;
    if (dirCount > 20) {
      isFull = true;
    }
    else if (fullDir.exists()) {
      isFull = false;
    }
    else {
      isFull = true;
    }

    final File file;
    if (isFull) {
      file = new File(snapshotRootDir, "full.tmp/sorted");
    }
    else {
      file = new File(snapshotRootDir, String.valueOf(highestSnapshot + 1) + ".tmp/sorted");
    }
    file.mkdirs();

    File previousSnapsotDir = null;
    if (highestSnapshot == -1) {
      previousSnapsotDir = new File(snapshotRootDir, "full/sorted");
    }
    else {
      previousSnapsotDir = new File(snapshotRootDir, String.valueOf(highestSnapshot) + "/sorted");
    }

    logger.info("Snapshot to: dir=" + file.getAbsolutePath());

    File versionFile = new File(file, "version.txt");
    try (OutputStreamWriter out = new OutputStreamWriter(new BufferedOutputStream(new FileOutputStream(versionFile)))) {
      out.write(String.valueOf(DatabaseServer.SERIALIZATION_VERSION));
    }

    File beginTimeFile = new File(file, "begin-time.txt");
    try (OutputStreamWriter out = new OutputStreamWriter(new BufferedOutputStream(new FileOutputStream(beginTimeFile)))) {
      long seconds = (System.currentTimeMillis() - DatabaseServer.TIME_2017) / 1000;
      out.write(String.valueOf(seconds));
    }

    final Long beginTimeForLastSnapshot;
    if (previousSnapsotDir.exists()) {
      File lastTimeFile = new File(previousSnapsotDir, "begin-time.txt");
      String str = IOUtils.toString(new FileInputStream(lastTimeFile), "utf-8");
      beginTimeForLastSnapshot = Long.valueOf(str);
      logger.info("have previous snapshot dir: isFull=" + isFull + ", date=" + beginTimeForLastSnapshot);
    }
    else {
      beginTimeForLastSnapshot = null;
      logger.info("don't have previous snapshot dir: isFull=" + isFull);
    }

    final AtomicLong countChecked = new AtomicLong();
    final AtomicLong countWritten = new AtomicLong();
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
    List<Future> futures = new ArrayList<>();
    ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 10_000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
    try {
      for (final Map.Entry<String, TableSchema> tableEntry : server.getCommon().getTables(dbName).entrySet()) {
        tableCount.incrementAndGet();
        for (final Map.Entry<String, IndexSchema> indexEntry : tableEntry.getValue().getIndices().entrySet()) {
          indexCount.incrementAndGet();
          futures.add(executor.submit(new Callable() {
            @Override
            public Object call() throws Exception {

              String[] indexFields = indexEntry.getValue().getFields();
              final int[] fieldOffsets = new int[indexFields.length];
              for (int k = 0; k < indexFields.length; k++) {
                fieldOffsets[k] = tableEntry.getValue().getFieldOffset(indexFields[k]);
              }

              final long subBegin = System.currentTimeMillis();
              final AtomicLong savedCount = new AtomicLong();
              final Index index = server.getIndices(dbName).getIndices().get(tableEntry.getKey()).get(indexEntry.getKey());

              File currFile = new File(file, tableEntry.getKey() + "/" + indexEntry.getKey() + "/0.bin");
              currFile.getParentFile().mkdirs();
              final DataOutputStream outStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(currFile), 65_000));
              try {
                final boolean isPrimaryKey = indexEntry.getValue().isPrimaryKey();
                Map.Entry<Object[], Object> first = index.firstEntry();
                if (first != null) {
                  final long initialIndexSize = index.size();
                  index.visitTailMap(first.getKey(), new Index.Visitor() {
                    @Override
                    public boolean visit(Object[] key, Object value) throws IOException {
                      int bucket = (int) (countSaved.incrementAndGet() % SNAPSHOT_PARTITION_COUNT);
                      long updateTime = server.getUpdateTime(value);
                      if (countChecked.incrementAndGet() % 100000 == 0) {
                        System.out.println(String.valueOf(updateTime) + "-" + beginTimeForLastSnapshot + "-" + (updateTime < beginTimeForLastSnapshot - 10));
                      }
                      //todo: need to check if should delete
                      if (!isFull && updateTime < beginTimeForLastSnapshot - 10) {
                        return true;
                      }

                      //                if (DatabaseCommon.compareKey(index.getComparators(), key, last.getKey()) > 0) {
                      //                  return false;
                      //                }

//                      if (countWritten.get() > initialIndexSize) {
//                        return false;
//                      }
                      byte[][] records = null;
                      synchronized (index.getMutex(key)) {
                        Object currValue = index.get(key);
                        if (currValue == null || currValue.equals(0L)) {
                          logger.error("null record: key=" + DatabaseCommon.keyToString(key));
                        }
                        else {
                          if (isPrimaryKey) {
                            records = server.fromUnsafeToRecords(currValue);
                          }
                          else {
                            records = server.fromUnsafeToKeys(currValue);
                          }
                        }
                      }
                      if (records != null) {
                        countWritten.incrementAndGet();

                        outStream.writeBoolean(true);
                        byte[] keyBytes = DatabaseCommon.serializeKey(tableEntry.getValue(), indexEntry.getKey(), key);
                        outStream.write(keyBytes);

                        Varint.writeUnsignedVarLong(updateTime, outStream);

                        Varint.writeSignedVarLong(records.length, outStream);
                        for (byte[] record : records) {

                          if (deleteIfOlder != null) {
                            updateTime = Record.getUpdateTime(record);
                            if (updateTime < deleteIfOlder) {
                              deleteRecord(dbName, tableEntry.getKey(), tableEntry.getValue(), indexEntry.getValue(),
                                  key, record, fieldOffsets);
                            }
                          }

                          Varint.writeSignedVarLong(record.length, outStream);
                          outStream.write(record);

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
                outStream.writeBoolean(false);
                outStream.flush();
                outStream.close();
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
        catch (ExecutionException e) {
          throw new DatabaseException(e);
        }
      }
    }
    finally {
      executor.shutdownNow();
    }
    File snapshotDir = null;
    if (isFull) {
      snapshotDir = new File(snapshotRootDir, "full");
    }
    else {
      snapshotDir = new File(snapshotRootDir, String.valueOf(highestSnapshot + 1));
    }

    FileUtils.deleteDirectory(snapshotDir);

    file.getParentFile().renameTo(snapshotDir);

    if (isFull) {
      for (int i = 0; ; i++) {
        File currDir = new File(snapshotRootDir, String.valueOf(i));
        if (!currDir.exists()) {
          break;
        }
        FileUtils.deleteDirectory(currDir);
      }

      server.getDeleteManager().deleteOldLogs(lastSnapshot);
    }

    try {
      //if (beginTimeForLastSnapshot != null) {
        //long time = beginTimeForLastSnapshot * 1000 + DatabaseServer.TIME_2017;
        server.getLogManager().deleteOldLogs(lastSnapshot, false);
      //}
    }
    catch (Exception e) {
      logger.error("Error deleting old logs", e);
    }

    logger.info("Snapshot - end: snapshotId=" + (highestSnapshot + 1) + ", countChecked=" + countChecked.get() +
        ", countWritten=" + countWritten.get() + ", duration=" + (System.currentTimeMillis() - begin));
  }


  public void recoverFromSnapshot(final String dbName) throws Exception {

    totalFileCount = 0;
    finishedFileCount = 0;
    totalBytes.set(0);
    finishedBytes.set(0);
    errorRecovering = null;
    isRecovering = true;
    try {
      server.purge(dbName);

      final String dataRoot = getSnapshotRootDir(dbName);
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

      currStage.set("recoveringSnapshot - buildingDeletesFile");
      totalBytes.set(0);
      finishedBytes.set(0);
      long begin = System.currentTimeMillis();
      logger.info("recoveringSnapshot - buildingDeletesFile - begin");
      server.getDeleteManager().buildDeletionsFiles(dbName, currStage, totalBytes, finishedBytes);
      logger.info("recoveringSnapshot - buildingDeletesFile - end: duration=" + (System.currentTimeMillis() - begin));

      currStage.set("recoveringSnapshot - applying deletes");
      totalBytes.set(0);
      finishedBytes.set(0);
      begin = System.currentTimeMillis();
      logger.info("begin recoveringSnapshot - applying deletes - begin");
      int cores = Runtime.getRuntime().availableProcessors();

      ThreadPoolExecutor executor = new ThreadPoolExecutor(cores, cores, 10_000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
      try {
        List<Future> futures = new ArrayList<>();
        for (int currDeltaDirNum = getHighestCommittedSnapshotVersion(dataRootDir, logger); currDeltaDirNum >= -1; currDeltaDirNum--) {
          final int currDelta = currDeltaDirNum;
          futures.add(executor.submit(new Callable() {
            @Override
            public Object call() throws Exception {
              recoverDeltaPreprocess(dbName, dataRoot, currDelta);
              return null;
            }
          }));
        }
        for (Future future : futures) {
          future.get();
        }
      }
      finally {
        executor.shutdownNow();
      }
      logger.info("begin recoveringSnapshot - applying deletes - end: duration=" + (System.currentTimeMillis() - begin));

      begin = System.currentTimeMillis();
      logger.info("begin recoveringSnapshot - begin");
      executor = new ThreadPoolExecutor(cores * 32, cores * 32, 10_000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
      try {
        currStage.set("recoveringSnapshot");
        totalBytes.set(0);
        finishedBytes.set(0);
        for (int currDeltaDirNum = getHighestCommittedSnapshotVersion(dataRootDir, logger); currDeltaDirNum >= -1; currDeltaDirNum--) {
          getDeltaSize(dbName, dataRoot, currDeltaDirNum, executor);
        }
        for (int currDeltaDirNum = getHighestCommittedSnapshotVersion(dataRootDir, logger); currDeltaDirNum >= -1; currDeltaDirNum--) {
          recoverDelta(dbName, dataRoot, currDeltaDirNum, executor);
        }
      }
      finally {
        executor.shutdownNow();
      }
      logger.info("begin recoveringSnapshot - end: duration=" + (System.currentTimeMillis() - begin));
    }
    finally {
      isRecovering = false;
    }
  }

  public File getDeletedDeltaDir(String dbName, String deltaName, String tableName, String indexName) {
    File snapshotRootDir = new File(getSnapshotRootDir(dbName));
    return new File(snapshotRootDir, deltaName + "/deleted/" + tableName + "/" + indexName);
  }

  public File getDeletedDeltaDir(String dbName, String deltaName) {
    File snapshotRootDir = new File(getSnapshotRootDir(dbName));
    return new File(snapshotRootDir, deltaName + "/deleted/");
  }

  public File getSortedDeltaFile(String dbName, String deltaName, String tableName, String indexName) {
    File snapshotRootDir = new File(getSnapshotRootDir(dbName));
    return new File(snapshotRootDir, deltaName + "/sorted/" + tableName + "/" + indexName + "/0.bin");
  }

  public File getSortedDeltaDir(String dbName, String deltaName) {
    File snapshotRootDir = new File(getSnapshotRootDir(dbName));
    return new File(snapshotRootDir, deltaName + "/sorted");
  }

  public void writeEntry(DataOutputStream deltaOutStream, TableSchema tableSchema, String indexName, MergeEntry deltaEntry) {
    try {
      deltaOutStream.writeBoolean(true);
      deltaOutStream.write(DatabaseCommon.serializeKey(tableSchema, indexName, deltaEntry.key));
      Varint.writeUnsignedVarLong(deltaEntry.updateTime, deltaOutStream);

      Varint.writeSignedVarLong(deltaEntry.records.length, deltaOutStream);
      for (int i = 0; i < deltaEntry.records.length; i++) {
        Varint.writeSignedVarLong(deltaEntry.records[i].length, deltaOutStream);
        deltaOutStream.write(deltaEntry.records[i]);
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public MergeEntry readEntry(DataInputStream inStream, TableSchema tableSchema) {
    try {
      MergeEntry entry = new MergeEntry();
      boolean exists = inStream.readBoolean();
      if (!exists) {
        return null;
      }
      entry.key = DatabaseCommon.deserializeKey(tableSchema, inStream);
      entry.updateTime = Varint.readUnsignedVarLong(inStream);

      int count = (int) Varint.readSignedVarLong(inStream);
      entry.records = new byte[count][];
      for (int i = 0; i < entry.records.length; i++) {
        int len = (int) Varint.readSignedVarLong(inStream);
        entry.records[i] = new byte[len];
        inStream.readFully(entry.records[i]);
      }
      return entry;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }



  public class MergeEntry {
    private Object[] key;
    private long updateTime;
    private byte[][] records;


    public Object[] getKey() {
      return key;
    }

    public byte[][] getRecords() {
      return records;
    }

    public void setRecords(byte[][] records) {
      this.records = records;
    }
  }


  private void recoverDeltaPreprocess(String dbName, String dataRoot, int currDeltaDirNum) {
    File snapshotDir = getSortedDeltaDir(dbName, currDeltaDirNum == -1 ? "full" : String.valueOf(currDeltaDirNum));

    if (!snapshotDir.exists()) {
      return;
    }

    try {
      if (snapshotDir.exists()) {
        File[] tableDirs = snapshotDir.listFiles();
        if (tableDirs != null) {
          for (File tableFile : tableDirs) {
            if (!tableFile.isDirectory()) {
              continue;
            }
            File[] indexDirs = tableFile.listFiles();
            if (indexDirs != null) {
              for (File indexDir : indexDirs) {
                File[] indexFiles = indexDir.listFiles();
                if (indexFiles != null) {
                  for (final File indexFile : indexFiles) {
                    totalBytes.addAndGet(indexFile.length());
                    totalFileCount++;
                  }
                }
              }
            }
          }
        }
      }

      server.getDeleteManager().applyDeletesToSnapshot(dbName, currDeltaDirNum, finishedBytes);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private void getDeltaSize(String dbName, String dataRoot, int currDeltaDirNum, ThreadPoolExecutor executor) {
    File snapshotDir = getSortedDeltaDir(dbName, currDeltaDirNum == -1 ? "full" : String.valueOf(currDeltaDirNum));

    if (!snapshotDir.exists()) {
      return;
    }

    snapshotDir = getDeletedDeltaDir(dbName, currDeltaDirNum == -1 ? "full" : String.valueOf(currDeltaDirNum));

    if (snapshotDir.exists()) {
      File[] tableDirs = snapshotDir.listFiles();
      if (tableDirs != null) {
        for (File tableFile : tableDirs) {
          if (!tableFile.isDirectory()) {
            continue;
          }
          File[] indexDirs = tableFile.listFiles();
          if (indexDirs != null) {
            for (File indexDir : indexDirs) {
              File[] indexFiles = indexDir.listFiles();
              if (indexFiles != null) {
                for (final File indexFile : indexFiles) {
                  totalBytes.addAndGet(indexFile.length());
                  totalFileCount++;
                }
              }
            }
          }
        }
      }
    }
  }

  private void recoverDelta(String dbName, String dataRoot, int currDeltaDirNum, ThreadPoolExecutor executor) {
    File snapshotDir = getSortedDeltaDir(dbName, currDeltaDirNum == -1 ? "full" : String.valueOf(currDeltaDirNum));

    if (!snapshotDir.exists()) {
      return;
    }

    snapshotDir = getDeletedDeltaDir(dbName, currDeltaDirNum == -1 ? "full" : String.valueOf(currDeltaDirNum));

    try {
      logger.info("Recover from snapshot: dir=" + snapshotDir.getAbsolutePath());

      final AtomicLong recoveredCount = new AtomicLong();
      recoveredCount.set(0);

      final long indexBegin = System.currentTimeMillis();
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
                                              try (DataInputStream inStream = new DataInputStream(new BufferedInputStream(new ByteCounterStream(finishedBytes, new FileInputStream(indexFile))))) {
                                                boolean isPrimaryKey = indexSchema.isPrimaryKey();
                                                while (true) {

                                                   if (!inStream.readBoolean()) {
                                                    break;
                                                  }
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
                                                  }
                                                  else {
                                                    address = server.toUnsafeFromKeys(updateTime, records);
                                                  }
                                                  synchronized (index.getMutex(key)) {
                                                    Object prevValue = index.put(key, address);
                                                    if (prevValue != null) {
                                                      index.put(key, prevValue);
                                                      server.freeUnsafeIds(address);
                                                    }
                                                    else {
                                                      index.addAndGetCount(1);
                                                    }
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
                                                //throw new Exception(e);
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
            logger.info("Recover progress - finished index. currDelta=" + currDeltaDirNum + ", table=" + tableName + INDEX_STR + indexName + ": count=" + recoveredCount.get() + RATE_STR +
                ((float) recoveredCount.get() / (float) ((System.currentTimeMillis() - indexBegin)) * 1000f) +
                DURATION_STR + (System.currentTimeMillis() - indexBegin) / 1000f);

          }
        }
      }
      logger.info("Recover progress - finished all indices. currDelta=" + currDeltaDirNum + ", count=" + recoveredCount.get() + RATE_STR +
          ((float) recoveredCount.get() / (float) ((System.currentTimeMillis() - indexBegin)) * 1000f) +
          DURATION_STR + (System.currentTimeMillis() - indexBegin) / 1000f);
    }
    catch (Exception e) {
      errorRecovering = e;
      throw new DatabaseException(e);
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
      file = new File(file, DELTA_STR + server.getShard() + "/0/schema.bin");
      file.getParentFile().mkdirs();

      File sourceFile = new File(getSnapshotReplicaDir(), "schema.bin");
      FileUtils.copyFile(sourceFile, file);


      file = new File(directory, subDirectory);
      file = new File(file, DELTA_STR + server.getShard() + "/0/config.bin");
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
      file = new File(file, DELTA_STR + server.getShard() + "/0");
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
      file = new File(file, DELTA_STR + server.getShard() + "/0");

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
    subDirectory += "/" + DELTA_STR + server.getShard() + "/0";

    awsClient.uploadFile(bucket, prefix, subDirectory, srcFile);

    srcFile = new File(getSnapshotReplicaDir(), "config.bin");

    if (srcFile.exists()) {
      awsClient.uploadFile(bucket, prefix, subDirectory, srcFile);
    }
  }

  public void backupAWS(String bucket, String prefix, String subDirectory) {
    AWSClient awsClient = server.getAWSClient();
    File srcDir = getSnapshotReplicaDir();
    subDirectory += "/" + DELTA_STR + server.getShard() + "/0";

    awsClient.uploadDirectory(bucket, prefix, subDirectory, srcDir);
  }

  public void restoreAWS(String bucket, String prefix, String subDirectory) {
    try {
      AWSClient awsClient = server.getAWSClient();
      File destDir = getSnapshotReplicaDir();
      subDirectory += "/" + DELTA_STR + server.getShard() + "/0";

      FileUtils.deleteDirectory(getSnapshotReplicaDir());
      getSnapshotReplicaDir().mkdirs();

      awsClient.downloadDirectory(bucket, prefix, subDirectory, destDir);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public void getFilesForCurrentSnapshot(List<String> files) {
    File replicaDir = getSnapshotReplicaDir();
    getFilesFromDirectory(replicaDir, files);
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

  public static class ByteCounterStream extends InputStream {
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
