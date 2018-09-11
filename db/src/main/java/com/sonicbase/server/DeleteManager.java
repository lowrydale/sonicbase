package com.sonicbase.server;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.*;
import com.sonicbase.index.Index;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.DateUtils;
import com.sonicbase.util.Varint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@SuppressWarnings({"squid:S1172", "squid:S1168", "squid:S00107"})
// all methods called from method invoker must have cobj and replayed command parms
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class DeleteManager {

  private static final String ERROR_PERFORMING_DELETES_STR = "Error performing deletes";
  private static final Logger logger = LoggerFactory.getLogger(DeleteManager.class);

  private final com.sonicbase.server.DatabaseServer databaseServer;
  private final ThreadPoolExecutor executor;
  private final ThreadPoolExecutor fileExecutor;
  private Thread mainThread;
  private final ThreadPoolExecutor freeExecutor;
  private boolean shutdown;
  private final LinkedBlockingQueue<Object> toFree = new LinkedBlockingQueue<>();
  private final Thread freeThread;
  private final AtomicLong countRead = new AtomicLong();

  DeleteManager(final DatabaseServer databaseServer) {
    this.databaseServer = databaseServer;
    this.executor = ThreadUtil.createExecutor(Runtime.getRuntime().availableProcessors() * 4,
        "SonicBase DeleteManager Thread");
    this.fileExecutor = ThreadUtil.createExecutor(Runtime.getRuntime().availableProcessors(),
        "SonicBase DeleteManager File Thread");
    this.freeExecutor = ThreadUtil.createExecutor(Runtime.getRuntime().availableProcessors() * 4,
        "SonicBase DeleteManager FreeExecutor Thread");
    freeThread = ThreadUtil.createThread(() -> {
      while (!shutdown) {
        try {
          final Object obj = toFree.poll(10_000, TimeUnit.MILLISECONDS);
          freeBatch(databaseServer, obj);
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
        catch (Exception e) {
          logger.error("Error in free thread", e);
        }
      }
    }, "SonicBase Free Thread");
    freeThread.start();
  }

  private void freeBatch(DatabaseServer databaseServer, Object obj) {
    if (obj != null) {
      final List<Object> batch = new ArrayList<>();
      toFree.drainTo(batch, 1000);
      freeExecutor.submit(() -> {
        for (Object currObj : batch) {
          databaseServer.getAddressMap().freeUnsafeIds(currObj);
        }
      });
    }
  }

  public static class DeleteRequest {
    private final Object[] key;

    DeleteRequest(Object[] key) {
      this.key = key;
    }

    public Object[] getKey() {
      return key;
    }
  }

  private void saveDeletes(String dbName, String tableName, String indexName,
                           List<DeleteRequest> deleteRequests) {
    try {
      String dateStr = DateUtils.toString(new Date(System.currentTimeMillis()));
      Random rand = new Random(System.currentTimeMillis());
      File file = new File(getReplicaRoot(), dateStr + "-" + System.nanoTime() + "-" +  rand.nextInt(50000) + ".bin");
      while (file.exists()) {
        file = new File(getReplicaRoot(), dateStr + "-" + System.nanoTime() + "-" +  rand.nextInt(50000) + ".bin");
      }
      file.getParentFile().mkdirs();
      TableSchema tableSchema = databaseServer.getCommon().getTables(dbName).get(tableName);
      try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)))) {
        Varint.writeSignedVarLong(DatabaseClient.SERIALIZATION_VERSION, out);
        out.writeUTF(dbName);
        out.writeUTF(tableName);
        out.writeUTF(indexName);
        out.writeInt(databaseServer.getCommon().getSchemaVersion() + 1);
        for (DeleteRequest key : deleteRequests) {
          out.write(DatabaseCommon.serializeKey(tableSchema, indexName, key.getKey()));
        }
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private void doDeletes(boolean ignoreVersion, File file) {
    try {
      File dir = getReplicaRoot();
      if (dir.exists()) {
        logger.info("DeleteManager deleting file - begin: file={}", file.getAbsolutePath());
        List<Future> futures = new ArrayList<>();
        if (doDeletesForStream(ignoreVersion, file, futures)) {
          return;
        }
        if (file.exists()) {
          Files.delete(file.toPath());
        }
        logger.info("DeleteManager deleting file - end: file={}", file.getAbsolutePath());
      }
    }
    catch (Exception e) {
      logger.error(ERROR_PERFORMING_DELETES_STR, e);
    }
  }

  private boolean doDeletesForStream(boolean ignoreVersion, File file, List<Future> futures) throws FileNotFoundException, InterruptedException, ExecutionException {
    InputStream countIn = new LogManager.ByteCounterStream(new FileInputStream(file), countRead);
    try (DataInputStream in = new DataInputStream(new BufferedInputStream(countIn))) {
      if (doDeletesProcessStream(ignoreVersion, futures, in)) {
        return true;
      }
    }
    catch (Exception e) {
      logger.error(ERROR_PERFORMING_DELETES_STR, e);
    }

    for (Future future : futures) {
      future.get();
    }
    return false;
  }

  private boolean doDeletesProcessStream(boolean ignoreVersion, List<Future> futures, DataInputStream in) throws IOException {
    Varint.readSignedVarLong(in); //serializationVersion
    String dbName = in.readUTF();
    String tableName = in.readUTF();
    TableSchema tableSchema = databaseServer.getCommon().getTables(dbName).get(tableName);

    String indexName = in.readUTF();
    final IndexSchema indexSchema = tableSchema.getIndices().get(indexName);
    String[] indexFields = indexSchema.getFields();
    int[] fieldOffsets = new int[indexFields.length];
    for (int k = 0; k < indexFields.length; k++) {
      fieldOffsets[k] = tableSchema.getFieldOffset(indexFields[k]);
    }

    long schemaVersionToDeleteAt = in.readInt();
    if (!ignoreVersion && schemaVersionToDeleteAt <= databaseServer.getCommon().getSchemaVersion()) {
      return true;
    }
    final Index index = databaseServer.getIndices().get(dbName).getIndices().get(tableName).get(indexName);
    List<Object[]> batch = new ArrayList<>();
    int errorsInARow = 0;
    while (!shutdown) {
      ProcessKey processKey = new ProcessKey(futures, in, tableSchema, indexSchema, index, batch, errorsInARow).invoke();
      batch = processKey.getBatch();
      errorsInARow = processKey.getErrorsInARow();
      if (processKey.is()) {
        break;
      }
    }
    processBatch(futures, indexSchema, index, batch);
    return false;
  }

  private List<Object[]> processBatch(List<Future> futures, final IndexSchema indexSchema, final Index index, List<Object[]> batch) {
    final List<Object[]> currBatch = batch;
    batch = new ArrayList<>();
    futures.add(executor.submit((Callable) () -> {
      final List<Object> toFreeBatch = new ArrayList<>();
      for (Object[] currKey : currBatch) {
        processValue(indexSchema, index, currKey);
      }
      doFreeMemory(toFreeBatch);
      return null;
    }));
    return batch;
  }

  private void processValue(IndexSchema indexSchema, Index index, Object[] currKey) throws InterruptedException {
    synchronized (index.getMutex(currKey)) {
      Object value = index.get(currKey);
      if (value != null) {
        byte[][] content = databaseServer.getAddressMap().fromUnsafeToRecords(value);
        if (content != null) {
          processRecords(indexSchema, index, currKey, content);
        }
      }
    }
  }

  private void processRecords(IndexSchema indexSchema, Index index, Object[] currKey, byte[][] content) throws InterruptedException {
    if (indexSchema.isPrimaryKey()) {
      if ((Record.DB_VIEW_FLAG_DELETING & Record.getDbViewFlags(content[0])) != 0) {
        Object o = index.remove(currKey);
        if (o != null) {
          toFree.put(o);
        }
      }
    }
    else {
      if ((Record.DB_VIEW_FLAG_DELETING & KeyRecord.getDbViewFlags(content[0])) != 0) {
        Object o = index.remove(currKey);
        if (o != null) {
          toFree.put(o);
        }
      }
    }
  }

  private void doFreeMemory(final List<Object> toFreeBatch) {
    for (Object obj : toFreeBatch) {
      databaseServer.getAddressMap().freeUnsafeIds(obj);
    }
  }

  //public for pro version
  public File getReplicaRoot() {
    return new File(databaseServer.getDataDir(), "deletes" + File.separator + databaseServer.getShard() +
        File.separator + databaseServer.getReplica() + File.separator);
  }

  public void getFiles(List<String> files) {
    File dir = getReplicaRoot();
    File[] currFiles = dir.listFiles();
    if (currFiles != null) {
      for (File file : currFiles) {
        files.add(file.getAbsolutePath());
      }
    }
  }
  public void start() {

    mainThread = new Thread(() -> {
      while (!shutdown) {
        try {
          if (processFilesForDeletes()) {
            break;
          }
        }
        catch (Exception e) {
          logger.error("Error procesing deletes file", e);
        }
      }
    }, "SonicBase Deletion Thread");
    mainThread.start();
  }

  private boolean processFilesForDeletes() {
    File dir = getReplicaRoot();
    if (dir.exists()) {

      if (doSleep()) {
        return true;
      }

      File[] files = dir.listFiles();
      if (files != null && files.length != 0) {
        Arrays.sort(files, Comparator.comparing(File::getAbsolutePath));

        if (false) {
          doDeletes(false, files[0]);
        }
        else {
          List<Future> futures = new ArrayList<>();
          for (int i = 0; i < files.length; i++) {
            final int offset = i;
            futures.add(fileExecutor.submit((Callable) () -> {
              doDeletes(false, files[offset]);
              return null;
            }));
          }
          for (int i = 0; i < futures.size(); i++) {
            Future future = futures.get(i);
            try {
              future.get();
            }
            catch (Exception e) {
              logger.error("Error deleting file: name={}", files[i].getAbsolutePath());
            }
          }
        }
      }
    }
    return false;
  }

  private boolean doSleep() {
    try {
      Thread.sleep(2_000);
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return true;
    }
    return false;
  }

  public void shutdown() {
    this.shutdown = true;
    try {
      if (freeThread != null) {
        freeThread.interrupt();
        freeThread.join();
      }
      if (mainThread != null) {
        mainThread.interrupt();
        mainThread.join();

      }
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new DatabaseException(e);
    }
  }

  private long totalBytes = 0;

  double getPercentDeleteComplete() {
    if (totalBytes == 0) {
      return 0;
    }
    return (double)countRead.get() / (double)totalBytes;
  }

  void saveDeletesForRecords(String dbName, String tableName, String indexName, long sequence0, long sequence1,
                             List<DeleteRequest> keysToDeleteExpanded) {
    saveDeletes(dbName, tableName, indexName, keysToDeleteExpanded);
  }

  void saveDeletesForKeyRecords(String dbName, String tableName, String indexName, long sequence0, long sequence1,
                                List<DeleteRequest> keysToDeleteExpanded) {
    saveDeletes(dbName, tableName, indexName, keysToDeleteExpanded);
  }

  static class DeleteRequestForKeyRecord extends DeleteRequest {

    DeleteRequestForKeyRecord(Object[] key) {
      super(key);
    }

    DeleteRequestForKeyRecord(Object[] key, byte[] primaryKeyBytes) {
      super(key);
    }
  }

  static class DeleteRequestForRecord extends DeleteRequest {
    DeleteRequestForRecord(Object[] key) {
      super(key);
    }
  }

  private final AtomicBoolean isForcingDeletes = new AtomicBoolean();

  boolean isForcingDeletes() {
    return isForcingDeletes.get();
  }

  public ComObject forceDeletes(ComObject cobj, boolean replayedCommand) {
    File dir = getReplicaRoot();
    totalBytes = 0;

    isForcingDeletes.set(true);
    ThreadPoolExecutor localExecutor = new ThreadPoolExecutor(8, 8,
        10_000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1_000),
        new ThreadPoolExecutor.CallerRunsPolicy());
    try {
      if (dir.exists()) {
        File[] files = dir.listFiles();
        for (File file : files) {
          totalBytes += file.length();
        }
        countRead.set(0);
        files = dir.listFiles();
        if (files == null || files.length == 0) {
          return null;
        }
        List<Future> futures = new ArrayList<>();
        for (final File file : files) {
          futures.add(localExecutor.submit((Callable) () -> {
            doDeletes(true, file);
            return null;
          }));
        }
        for (Future future : futures) {
          future.get();
        }
      }
    }
    catch (Exception e) {
      logger.error("Error deleting records", e);
    }
    finally {
      isForcingDeletes.set(false);
      localExecutor.shutdownNow();
    }
    return null;
  }

  private class ProcessKey {
    private boolean myResult;
    private final List<Future> futures;
    private final DataInputStream in;
    private final TableSchema tableSchema;
    private final IndexSchema indexSchema;
    private final Index index;
    private List<Object[]> batch;
    private int errorsInARow;

    ProcessKey(List<Future> futures, DataInputStream in, TableSchema tableSchema, IndexSchema indexSchema,
               Index index, List<Object[]> batch, int errorsInARow) {
      this.futures = futures;
      this.in = in;
      this.tableSchema = tableSchema;
      this.indexSchema = indexSchema;
      this.index = index;
      this.batch = batch;
      this.errorsInARow = errorsInARow;
    }

    boolean is() {
      return myResult;
    }

    public List<Object[]> getBatch() {
      return batch;
    }

    int getErrorsInARow() {
      return errorsInARow;
    }

    public ProcessKey invoke() {
      Object[] key = null;
      try {
        key = DatabaseCommon.deserializeKey(tableSchema, in);
        errorsInARow = 0;
        batch.add(key);
        if (batch.size() > 1_000) {
          batch = processBatch(futures, indexSchema, index, batch);
        }
      }
      catch (EOFException e) {
        //expected
        myResult = true;
        return this;
      }
      catch (Exception e) {
        logger.error("Error deserializing key: " + ((errorsInARow > 20) ? " aborting" : ""), e);
        if (errorsInARow++ > 20) {
          myResult = true;
          return this;
        }
      }
      myResult = false;
      return this;
    }
  }
}
