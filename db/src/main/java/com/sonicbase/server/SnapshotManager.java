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
import com.sonicbase.util.PartitionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.giraph.utils.Varint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@SuppressWarnings({"squid:S1172", "squid:S2629", "squid:S1168", "squid:S3516"})
// all methods called from method invoker must have cobj and replayed command parms
// info is always enabled, don't need to conditionally call
// I prefer to return null instead of an empty array
// all methods called from method invoker must return a ComObject even if they are all null
public class SnapshotManager {

  private static Logger logger = LoggerFactory.getLogger(SnapshotManager.class);
  private static final int SNAPSHOT_PARTITION_COUNT = 128;
  private static final String SNAPSHOT_STR = "snapshot" + File.separator;
  private static final String INDEX_STR = ", index=";
  private static final String RATE_STR = ", rate=";
  private static final String DURATION_STR = ", duration(s)=";

  private final com.sonicbase.server.DatabaseServer server;
  private long lastSnapshot = -1;
  private boolean enableSnapshot = true;
  private boolean isRecovering = true;
  private long totalBytes = 0;
  private AtomicLong finishedBytes = new AtomicLong();
  private Exception errorRecovering = null;

  SnapshotManager(DatabaseServer databaseServer) {
    this.server = databaseServer;
  }

  private static int getHighestCommittedSnapshotVersion(File snapshotRootDir, Logger logger) {
    int highestSnapshot = -1;
    try {
      String[] dirs = snapshotRootDir.list();
      if (dirs != null) {
        for (String dir : dirs) {
          int pos = dir.indexOf('.');
          if (pos == -1) {
            try {
              int value = Integer.parseInt(dir);
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
          int value = Integer.parseInt(dir);
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

  public ComObject finishServerReloadForSource(ComObject cobj, boolean replayedCommand) {

    enableSnapshot(true);

    return null;
  }

  void getPercentRecoverComplete(ComObject retObj) {
    if (totalBytes == 0) {
      retObj.put(ComObject.Tag.PERCENT_COMPLETE, 0d);
    }
    else {
      retObj.put(ComObject.Tag.PERCENT_COMPLETE, (double) finishedBytes.get() / (double) totalBytes);
    }
    retObj.put(ComObject.Tag.STAGE, "recoveringSnapshot");
  }

  Exception getErrorRecovering() {
    return errorRecovering;
  }

  void recoverFromSnapshot(String dbName) {
    logger.info("recovering snapshot: db={}", dbName);
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
        logger.info("Deserialized table schema: table={}", schema.getKey());
        for (Map.Entry<String, IndexSchema> index : schema.getValue().getIndices().entrySet()) {
          logger.info("Deserialized index: table={}, index={}", schema.getKey(), index.getKey());
          server.getSchemaManager().doCreateIndex(dbName, schema.getValue(), index.getKey(), index.getValue().getFields());
        }
      }

      int highestSnapshot = getHighestCommittedSnapshotVersion(dataRootDir, logger);

      if (highestSnapshot == -1) {
        return;
      }

      final File snapshotDir = new File(dataRoot, String.valueOf(highestSnapshot));

      logger.info("Recover from snapshot: dir={}", snapshotDir.getAbsolutePath());

      ThreadPoolExecutor executor = ThreadUtil.createExecutor(SNAPSHOT_PARTITION_COUNT,
          "SonicBase SnapshotManager recoverFromSnapshot Thread");

      try {
        final AtomicLong recoveredCount = new AtomicLong();
        recoveredCount.set(0);

        final long indexBegin = System.currentTimeMillis();
        recoveredCount.set(0);
        final  AtomicLong lastLogged = new AtomicLong(System.currentTimeMillis());
        File file = snapshotDir;

        if (file.exists()) {
          for (File tableFile : file.listFiles()) {
            if (!tableFile.isDirectory()) {
              continue;
            }
            for (File indexDir : tableFile.listFiles()) {
              for (final File indexFile : indexDir.listFiles()) {
                totalBytes += indexFile.length();
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
            for (File indexDir : tableFile.listFiles()) {
              final String indexName = indexDir.getName();
              List<Future> futures = new ArrayList<>();
              final AtomicInteger offset = new AtomicInteger();
              logger.info("Recovering: table={}, index={}", tableName, indexName);
              for (final File indexFile : indexDir.listFiles()) {
                final int currOffset = offset.get();
                final TableSchema tableSchema = server.getCommon().getTables(dbName).get(tableName);
                final IndexSchema indexSchema = server.getIndexSchema(dbName, tableSchema.getName(), indexName);
                final Index index = server.getIndex(dbName, tableName, indexName);
                futures.add(executor.submit(() -> {
                  try (DataInputStream inStream = new DataInputStream(new BufferedInputStream(
                      new ByteCounterStream(finishedBytes, new FileInputStream(indexFile))))) {
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
                        address = server.getAddressMap().toUnsafeFromRecords(updateTime, records);
                        for (byte[] record : records) {
                          if ((Record.getDbViewFlags(record) & Record.DB_VIEW_FLAG_DELETING) == 0) {
                            index.addAndGetCount(1);
                          }
                        }
                      }
                      else {
                        address = server.getAddressMap().toUnsafeFromKeys(updateTime, records);
                        for (byte[] record : records) {
                          if ((Record.getDbViewFlags(record) & Record.DB_VIEW_FLAG_DELETING) == 0) {
                            index.addAndGetCount(1);
                          }
                        }
                      }

                      if (index.put(key, address) != null) {
                        logger.error("Key already exists: key={}", DatabaseCommon.keyToString(key));
                      }

                      recoveredCount.incrementAndGet();
                      if (currOffset == 0 && (System.currentTimeMillis() - lastLogged.get()) > 2000) {
                        lastLogged.set(System.currentTimeMillis());
                        logger.info("Recover progress - table={}, index={}, count={}, rate={}, duration={}sec",
                            tableName, indexName, recoveredCount.get(),
                            ((float) recoveredCount.get() / (float) (System.currentTimeMillis() - indexBegin) * 1000f),
                            (System.currentTimeMillis() - indexBegin) / 1000f);
                      }
                    }
                  }
                  catch (Exception e) {
                    logger.error("Error recovering bucket: table={}, index={}", tableName, indexName, e);
                    throw new DatabaseException(e);
                  }
                  return true;
                }
                ));
                offset.incrementAndGet();
              }
              for (Future future : futures) {
                try {
                  if (!(Boolean) future.get()) {
                    throw new DatabaseException("Error recovering from bucket");
                  }
                }
                catch (Exception t) {
                  errorRecovering = t;
                  throw new DatabaseException("Error recovering from bucket", t);
                }
              }
              logger.info("Recover progress - finished index. table={}, index={}, count={}, rate={}, duration={}",
                  tableName, indexName, recoveredCount.get(),
                  ((float) recoveredCount.get() / (float) (System.currentTimeMillis() - indexBegin) * 1000f),
                  (System.currentTimeMillis() - indexBegin) / 1000f);
            }
          }
        }
        logger.info("Recover progress - finished all indices. count={}, rate={}, duration={}", recoveredCount.get(),
            ((float) recoveredCount.get() / (float) (System.currentTimeMillis() - indexBegin) * 1000f),
            (System.currentTimeMillis() - indexBegin) / 1000f);
      }
      finally {
        executor.shutdownNow();
      }
    }
    catch (Exception e) {
      errorRecovering = e;
      throw new DatabaseException(e);
    }
    finally {
      isRecovering = false;
    }
  }

  private File getSnapshotReplicaDir() {
    return new File(server.getDataDir(), SNAPSHOT_STR + server.getShard() + File.separator + server.getReplica());
  }

  private String getSnapshotRootDir(String dbName) {
    return new File(getSnapshotReplicaDir(), dbName).getAbsolutePath();
  }

  private String getSnapshotSchemaDir(String dbName) {
    return new File(getSnapshotReplicaDir(), "_sonicbase_schema" + File.separator + dbName).getAbsolutePath();
  }

  public File getIndexSchemaDir(String dbName, String tableName, String indexName) {
    return new File(getSnapshotSchemaDir(dbName), tableName + "/indices/" + indexName);
  }

  public File getTableSchemaDir(String dbName, String tableName) {
    return new File(getSnapshotSchemaDir(dbName), tableName + File.separator + "table");
  }

  void saveIndexSchema(String dbName, int schemaVersion, TableSchema tableSchema, IndexSchema indexSchema) {
    try {
      File file = new File(getSnapshotSchemaDir(dbName), tableSchema.getName() + File.separator + "indices" +
          File.separator + indexSchema.getName() + File.separator + "schema." + schemaVersion + ".bin");
      FileUtils.forceMkdirParent(file);
      try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)))) {
        out.writeShort(DatabaseClient.SERIALIZATION_VERSION);
        TableSchema.serializeIndexSchema(out, tableSchema, indexSchema);
      }
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  void saveTableSchema(String dbName, int schemaVersion, String tableName, TableSchema tableSchema) {
    try {
      File file = new File(getSnapshotSchemaDir(dbName), tableName + "/table/schema." + schemaVersion + ".bin");
      FileUtils.forceMkdirParent(file);
      try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)))) {
        out.writeShort(DatabaseClient.SERIALIZATION_VERSION);
        tableSchema.serialize(out);
      }
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  void deleteTableSchema(String dbName, int schemaVersion, String tableName) {
    File file = new File(getSnapshotSchemaDir(dbName), tableName);
    try {
      FileUtils.deleteDirectory(file);
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  void deleteIndexSchema(String dbName, int schemaVersion, String table, String indexName) {
    try {
      File file = new File(getSnapshotSchemaDir(dbName), table + "/indices/" + indexName);
      FileUtils.deleteDirectory(file);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }


  Thread snapshotThread = null;
  void runSnapshotLoop() {
    if (snapshotThread != null) {
      snapshotThread.interrupt();
      try {
        snapshotThread.join();
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new DatabaseException(e);
      }
    }
    snapshotThread = ThreadUtil.createThread(() -> {
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

          Set<String> dbNames = new HashSet<>();
          for (String dbName : server.getCommon().getDatabases().keySet()) {
            dbNames.add(dbName);
          }
          for (String dbName : server.getDbNames(server.getDataDir())) {
            dbNames.add(dbName);
          }
          for (String dbName : dbNames) {
            runSnapshot(dbName);
          }
          server.getCommon().saveSchema(server.getDataDir());
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
        catch (Exception e) {
          logger.error("Error creating snapshot", e);
        }
      }
    }, "SonicBase Snapshot Thread");
    snapshotThread.start();
  }

  private void deleteRecord(String dbName, String tableName, TableSchema tableSchema, IndexSchema indexSchema, Object[] key, byte[] record, int[] fieldOffsets) {

    List<Integer> selectedShards = PartitionUtils.findOrderedPartitionForRecord(true, false,
        tableSchema, indexSchema.getName(), null, BinaryExpression.Operator.EQUAL, null, key, null);
    if (selectedShards.isEmpty()) {
      throw new DatabaseException("No shards selected for query");
    }

    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.SERIALIZATION_VERSION, DatabaseClient.SERIALIZATION_VERSION);
    cobj.put(ComObject.Tag.KEY_BYTES, DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), key));
    cobj.put(ComObject.Tag.SCHEMA_VERSION, server.getCommon().getSchemaVersion());
    cobj.put(ComObject.Tag.DB_NAME, dbName);
    cobj.put(ComObject.Tag.TABLE_NAME, tableName);
    cobj.put(ComObject.Tag.INDEX_NAME, indexSchema.getName());
    cobj.put(ComObject.Tag.METHOD, "UpdateManager:deleteRecord");
    server.getClient().send("UpdateManager:deleteRecord", selectedShards.get(0), 0, cobj, DatabaseClient.Replica.DEF);

    cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, dbName);
    cobj.put(ComObject.Tag.SCHEMA_VERSION, server.getCommon().getSchemaVersion());
    cobj.put(ComObject.Tag.TABLE_NAME, tableName);
    cobj.put(ComObject.Tag.METHOD, "UpdateManager:deleteIndexEntry");
    cobj.put(ComObject.Tag.RECORD_BYTES, record);

    server.getClient().sendToAllShards(null, 0, cobj, DatabaseClient.Replica.DEF);
  }

  void runSnapshot(final String dbName) throws IOException{
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

    logger.info("Snapshot to: dir={}", file.getAbsolutePath());

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
            File currFile = new File(file, tableEntry.getKey() + File.separator + indexEntry.getKey() +
                File.separator + i + ".bin");
            currFile.getParentFile().mkdirs();
            outStreams[i] = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(currFile), 65_000));
          }

          final boolean isPrimaryKey = indexEntry.getValue().isPrimaryKey();
          Map.Entry<Object[], Object> first = index.firstEntry();
          if (first != null) {
            index.visitTailMap(first.getKey(), (key, value) -> {
              int bucket = (int) (countSaved.incrementAndGet() % SNAPSHOT_PARTITION_COUNT);
              byte[][] records = null;
              long updateTime = 0;
              synchronized (index.getMutex(key)) {
                Object currValue = index.get(key);
                if (!(currValue == null || currValue.equals(0L))) {
                  if (isPrimaryKey) {
                    records = server.getAddressMap().fromUnsafeToRecords(currValue);
                  }
                  else {
                    records = server.getAddressMap().fromUnsafeToKeys(currValue);
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
                        ((float) savedCount.get() / (float) (System.currentTimeMillis() - subBegin) * 1000f) +
                        DURATION_STR + (System.currentTimeMillis() - subBegin) / 1000f +
                        ", table=" + tableEntry.getKey() + INDEX_STR + indexEntry.getKey());
                  }
                }
              }
              return true;
            });
          }
          logger.info("Snapshot progress - finished index: count={}, rate={}, duration={}, table={}, index={}",
              savedCount, ((float) savedCount.get() / (float) (System.currentTimeMillis() - subBegin) * 1000f),
              (System.currentTimeMillis() - subBegin) / 1000f, tableEntry.getKey(), indexEntry.getKey());
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

    logger.info("Snapshot - end: snapshotId={}, duration={}", (highestSnapshot + 1), (System.currentTimeMillis() - begin));
  }

  private void deleteOldSnapshots(String dbName) throws IOException {
    File snapshotRootDir = new File(getSnapshotRootDir(dbName));
    snapshotRootDir.mkdirs();
    int highestSnapshot = getHighestCommittedSnapshotVersion(snapshotRootDir, logger);

    String[] files = snapshotRootDir.list();
    if (files != null) {
      for (String dirStr : files) {
        int dirNum = -1;
        try {
          dirNum = Integer.valueOf(dirStr);
        }
        catch (Exception t) {
          //expected numeric format problems
        }
        if (dirStr.contains("tmp") || (dirNum != -1 && dirNum < (highestSnapshot))) {
          File dir = new File(snapshotRootDir, dirStr);
          logger.info("Deleting snapshot: {}", dir.getAbsolutePath());
          FileUtils.deleteDirectory(dir);
        }
      }
    }
  }

  void enableSnapshot(boolean enable) {
    this.enableSnapshot = enable;
    if (snapshotThread != null) {
      snapshotThread.interrupt();
      try {
        snapshotThread.join();
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      snapshotThread = null;
    }
    if (enable) {
      runSnapshotLoop();
    }
  }

  public void shutdown() {
    if (snapshotThread != null) {
      snapshotThread.interrupt();
      try {
        snapshotThread.join();
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new DatabaseException(e);
      }
    }
  }

  boolean isRecovering() {
    return isRecovering;
  }

  private class ByteCounterStream extends InputStream {
    private final FileInputStream stream;
    private final AtomicLong finishedBytes;

    ByteCounterStream(AtomicLong finishedBytes, FileInputStream fileInputStream) {
      this.stream = fileInputStream;
      this.finishedBytes = finishedBytes;
    }

    @Override
    public synchronized void reset() throws IOException {
      stream.reset();
    }

    @Override
    public boolean markSupported() {
      return stream.markSupported();
    }

    @Override
    public synchronized void mark(int readlimit) {
      stream.mark(readlimit);
    }

    @Override
    public long skip(long n) throws IOException {
      return stream.skip(n);
    }

    @Override
    public int available() throws IOException {
      return stream.available();
    }

    @Override
    public void close() throws IOException {
      stream.close();
    }

    @Override
    public int read(byte[] b) throws IOException {
      int read = stream.read(b);
      if (read != -1) {
        finishedBytes.addAndGet(read);
      }
      return read;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
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
