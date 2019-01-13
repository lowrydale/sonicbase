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
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.sonicbase.client.DatabaseClient.SERIALIZATION_VERSION_28;
import static com.sonicbase.server.DatabaseServer.METRIC_SAVE_DELETE;

@SuppressWarnings({"squid:S1172", "squid:S1168", "squid:S00107"})
// all methods called from method invoker must have cobj and replayed command parms
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class DeleteManager {

  private static final String ERROR_PERFORMING_DELETES_STR = "Error performing deletes";
  private static final Logger logger = LoggerFactory.getLogger(DeleteManager.class);

  private final com.sonicbase.server.DatabaseServer databaseServer;
  private final ThreadPoolExecutor executor;
  private ThreadPoolExecutor fileExecutor;
  private Thread mainThread;
  private final ThreadPoolExecutor freeExecutor;
  private boolean shutdown;
  private final LinkedBlockingQueue<Object> toFree = new LinkedBlockingQueue<>();
  private Thread freeThread;
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
          databaseServer.getAddressMap().delayedFreeUnsafeIds(currObj);
        }
      });
    }
  }

  public DatabaseServer getServer() {
    return databaseServer;
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
                           List<DeleteRequest> deleteRequests, int saveAsVersion) {
    final Index index = databaseServer.getIndices().get(dbName).getIndices().get(tableName).get(indexName);

    if (databaseServer.isNotDurable()) {
      IndexSchema indexSchema = databaseServer.getCommon().getTables(dbName).get(tableName).getIndices().get(indexName);
      for (DeleteRequest request : deleteRequests) {
        Object value = index.remove(request.getKey());
        if (value != null) {
          try {
            byte[][] content = databaseServer.getAddressMap().fromUnsafeToRecords(value);
            //todo: not sure we want to do this
            processRecords(indexSchema, index, request.getKey(), content, value);
          }
          catch (Exception e) {
            logger.error("Error deleting record: db={}, table={}, index={}, key={}", dbName, tableName, indexName, DatabaseCommon.keyToString(request.getKey()));
          }
        }
      }
      return;
    }

    try {
      String dateStr = DateUtils.toString(new Date(System.currentTimeMillis()));
      Random rand = new Random(System.currentTimeMillis());
      File file = null;
      int countToDelete = 0;
      getReplicaRoot().mkdirs();
      while (true) {
        file = new File(getReplicaRoot(), dateStr + "-" + System.nanoTime() + "-" +  rand.nextInt(50000) + ".bin.in-process");
        if (file.createNewFile()) {
          break;
        }
      }
      file.getParentFile().mkdirs();
      AtomicLong count = databaseServer.getStats().get(METRIC_SAVE_DELETE).getCount();
      TableSchema tableSchema = databaseServer.getCommon().getTables(dbName).get(tableName);
      try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)))) {
        Varint.writeSignedVarLong(DatabaseClient.SERIALIZATION_VERSION, out);
        out.writeUTF(dbName);
        out.writeUTF(tableName);
        out.writeUTF(indexName);
        out.writeInt(saveAsVersion);
        out.writeInt(deleteRequests.size());

        for (DeleteRequest key : deleteRequests) {
          byte[] bytes = DatabaseCommon.serializeKey(tableSchema, indexName, key.getKey());
          out.writeInt(bytes.length);

          out.write(bytes);
          count.incrementAndGet();
          countToDelete++;
        }

        out.writeInt(0);
      }
      index.addSize(-1 * countToDelete);
      File newFile = null;
      while (true) {
        try {
          if (!file.exists()) {
            logger.error("Deletes temp file doesn't exist: filename={}", file.getAbsolutePath());
            break;
          }
          newFile = new File(getReplicaRoot(), dateStr + "-" + System.nanoTime() + "-" +  rand.nextInt(50000) + ".bin");
          Files.move(file.toPath(), newFile.toPath(), StandardCopyOption.ATOMIC_MOVE);
          break;
        }
        catch (Exception e) {
          logger.error("Error renaming deletes file: src={}, dest={}", file.getAbsolutePath(), newFile.getAbsolutePath(), e);
        }
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private boolean doDeletes(boolean ignoreVersion, File file) {
    try {
      long fileLen = file.length();
      File dir = getReplicaRoot();
      if (dir.exists()) {
        logger.debug("DeleteManager deleting file - begin: file={}", file.getAbsolutePath());
        List<Future> futures = new ArrayList<>();
        try {
          if (doDeletesForStream(ignoreVersion, file, fileLen, futures)) {
            return true;
          }
        }
        finally {
          for (Future future : futures) {
            future.get();
          }
        }

        if (file.exists()) {
          if (file.getName().contains("in-process")) {
            logger.error("Name of file deleting includes in-process: file={}", file.getAbsolutePath());
          }
          else {
            Files.delete(file.toPath());
          }
        }
        logger.debug("DeleteManager deleting file - finished: file={}", file.getAbsolutePath());
      }
    }
    catch (Exception e) {
      logger.error("Error performing deletes: file={}", file.getAbsolutePath(), e);
    }
    return false;
  }

  private boolean doDeletesForStream(boolean ignoreVersion, File file, long fileLen, List<Future> futures) throws FileNotFoundException, InterruptedException, ExecutionException {
    AtomicLong countRead = new AtomicLong();
    try (DataInputStream in = new DataInputStream(new LogManager.ByteCounterStream(new BufferedInputStream(new FileInputStream(file)), countRead))) {
      if (doDeletesProcessStream(ignoreVersion, futures, in, file, fileLen, countRead)) {
        return true;
      }
    }
    catch (Exception e) {
      logger.error("Error performing deletes: file={}", file.getAbsolutePath(), e);
    }

    this.countRead.addAndGet(countRead.get());
    return false;
  }

  private boolean doDeletesProcessStream(boolean ignoreVersion, List<Future> futures, DataInputStream in, File file, long fileLen, AtomicLong countRead) throws IOException {
    long serializationVersion = Varint.readSignedVarLong(in);
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
    if (!ignoreVersion && schemaVersionToDeleteAt > databaseServer.getSchemaVersion()) {
      return true;
    }
    int keyCountInFile = in.readInt();
    final Index index = databaseServer.getIndices().get(dbName).getIndices().get(tableName).get(indexName);
    List<Object[]> batch = new ArrayList<>();
    int errorsInARow = 0;
    byte[] keyBuffer = new byte[100];
    int keyCountRead = 0;
    while (!shutdown) {
      ProcessKey processKey = new ProcessKey(dbName, futures, serializationVersion, in,
          tableSchema, indexSchema, index, batch, keyBuffer, errorsInARow, file, fileLen, countRead).invoke();
      batch = processKey.getBatch();
      keyBuffer = processKey.getKeyBuffer();
      errorsInARow = processKey.getErrorsInARow();
      if (processKey.is()) {
        break;
      }
      keyCountRead++;
    }
    if (keyCountRead != keyCountInFile) {
      logger.error("Incorrect key count read: countInFile={}, countRead={}, db={}, table={}, index={}",
          keyCountInFile, keyCountRead, dbName, tableName, indexName);
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
      int countToDelete = currBatch.size();
      //count was decremented at saveDeleteds and processValue
      //add back half the count so we don't get a double delete count
      index.addSize(countToDelete);
      return null;
    }));
    return batch;
  }

  private void processValue(IndexSchema indexSchema, Index index, Object[] currKey) throws InterruptedException {
    synchronized (index.getMutex(currKey)) {
      try {
        Index.setIsOpForRebalance(true);

        Object value = index.remove(currKey); // will likely delete, so go ahead and delete and re-add later if needed
        if (value != null) {
          byte[][] content = databaseServer.getAddressMap().fromUnsafeToRecords(value);
          if (content != null) {
            processRecords(indexSchema, index, currKey, content, value);
          }
          else {
            //wasn't deleted as expected so we need to adjust the count
            index.addSize(1);
          }
        }
      }
      finally {
        Index.setIsOpForRebalance(false);
      }
    }
  }

  private void processRecords(IndexSchema indexSchema, Index index, Object[] currKey, byte[][] content, Object value) throws InterruptedException {
    try {
      Index.setIsOpForRebalance(true);

      if (indexSchema.isPrimaryKey()) {
        if ((Record.DB_VIEW_FLAG_DELETING & Record.getDbViewFlags(content[0])) == 0) {
          index.put(currKey, value);
        }
        else {
          toFree.put(value);
        }
      }
      else {
        if ((Record.DB_VIEW_FLAG_DELETING & KeyRecord.getDbViewFlags(content[0])) == 0) {
          index.put(currKey, value);
        }
        else {
          toFree.put(value);
        }
      }
    }
    finally {
      Index.setIsOpForRebalance(false);
    }
  }

  private void doFreeMemory(final List<Object> toFreeBatch) {
    for (Object obj : toFreeBatch) {
      databaseServer.getAddressMap().delayedFreeUnsafeIds(obj);
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

    if (mainThread == null) {
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
          long begin = System.currentTimeMillis();

          List<Future> futures = new ArrayList<>();
          for (int i = 0; i < files.length; i++) {
            if (files[i].getName().contains("in-process")) {
              continue;
            }
            final int offset = i;
            futures.add(fileExecutor.submit((Callable) () -> doDeletes(false, files[offset])));
          }
          int countSkipped = 0;
          for (int i = 0; i < futures.size(); i++) {
            Future future = futures.get(i);
            try {
              if (true == (Boolean)future.get()) {
                countSkipped++;
              }
            }
            catch (Exception e) {
              logger.error("Error deleting file: name={}", files[i].getAbsolutePath());
            }
          }
          logger.info("deleted files: countDeleted={}, countSkipped={}, duration={}",
              files.length - countSkipped, countSkipped, files.length, System.currentTimeMillis() - begin);
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
        freeThread = null;
      }
      if (mainThread != null) {
        mainThread.interrupt();
        mainThread.join();
        mainThread = null;

      }
      fileExecutor.shutdownNow();
      fileExecutor = null;
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
    saveDeletes(dbName, tableName, indexName, keysToDeleteExpanded, databaseServer.getCommon().getSchemaVersion() + 1);
  }

  void saveDeletesForRecords(String dbName, String tableName, String indexName, long sequence0, long sequence1,
                             List<DeleteRequest> keysToDeleteExpanded, int saveAsVersion) {
    saveDeletes(dbName, tableName, indexName, keysToDeleteExpanded, saveAsVersion);
  }

  void saveDeletesForKeyRecords(String dbName, String tableName, String indexName, long sequence0, long sequence1,
                                List<DeleteRequest> keysToDeleteExpanded) {
    saveDeletes(dbName, tableName, indexName, keysToDeleteExpanded, databaseServer.getCommon().getSchemaVersion() + 1);
  }

  void saveDeletesForKeyRecords(String dbName, String tableName, String indexName, long sequence0, long sequence1,
                                List<DeleteRequest> keysToDeleteExpanded, int saveAsVersion) {
    saveDeletes(dbName, tableName, indexName, keysToDeleteExpanded, saveAsVersion);
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
          if (file.getName().contains("in-process")) {
            continue;
          }
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
    private final long serializationVersion;
    private final String dbName;
    private final File file;
    private final AtomicLong countRead;
    private final long fileLen;
    private byte[] keyBuffer;
    private boolean myResult;
    private final List<Future> futures;
    private final DataInputStream in;
    private final TableSchema tableSchema;
    private final IndexSchema indexSchema;
    private final Index index;
    private List<Object[]> batch;
    private int errorsInARow;

    ProcessKey(String dbName, List<Future> futures, long serializationVersion, DataInputStream in, TableSchema tableSchema, IndexSchema indexSchema,
               Index index, List<Object[]> batch, byte[] keyBuffer, int errorsInARow, File file, long fileLen, AtomicLong countRead) {
      this.dbName = dbName;
      this.futures = futures;
      this.serializationVersion = serializationVersion;
      this.in = in;
      this.tableSchema = tableSchema;
      this.indexSchema = indexSchema;
      this.index = index;
      this.batch = batch;
      this.keyBuffer = keyBuffer;
      this.errorsInARow = errorsInARow;
      this.file = file;
      this.fileLen = fileLen;
      this.countRead = countRead;
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

    public byte[] getKeyBuffer() {
      return keyBuffer;
    }

    public ProcessKey invoke() {
      Object[] key = null;
      boolean finishedRead = false;
      try {
        if (serializationVersion <= SERIALIZATION_VERSION_28) {
          key = DatabaseCommon.deserializeKey(tableSchema, in);
        }
        else {
          int len = in.readInt();
          if (len == 0) {
            myResult = true;
            return this;
          }
          if (countRead.get() >= fileLen) {
            logger.info("read past end of file: file={}, fileLen={}, countRead={}",
                file.getAbsolutePath(), file.length(), countRead.get());
            myResult = true;
            return this;
          }

          if (len > DatabaseCommon.MAX_KEY_LEN) {
            logger.error("DeleteManager.processKey, Key too large: max={}, len={}, db={}, table={}, index={}, file={}, fileLen={}, countRead={}",
                DatabaseCommon.MAX_KEY_LEN, len, dbName, tableSchema.getName(), indexSchema.getName(), file.getAbsolutePath(),
                file.length(), countRead.get());
            myResult = true;
            return this;
          }

          if (len > keyBuffer.length) {
            keyBuffer = allocBuffer(len);
            logger.debug("reading greater than keyBuffer: len={}, db={}, table={}, index={}, file={}",
                len, dbName, tableSchema.getName(), indexSchema.getName(), file.getAbsolutePath());
          }
          in.read(keyBuffer, 0, len);
          finishedRead = true;
          key = DatabaseCommon.deserializeKey(tableSchema, keyBuffer);
        }
        errorsInARow = 0;
        batch.add(key);
        if (batch.size() > 1_000) {
          batch = processBatch(futures, indexSchema, index, batch);
        }
      }
      catch (Exception e) {
        if (serializationVersion == SERIALIZATION_VERSION_28 && e instanceof EOFException) {
          myResult = true;
          return this;
        }
        logger.error("Error deserializing key: state={}, db={}, table={}, index={}, file={}",
            ((errorsInARow > 20) ? " aborting" : "continuing"), dbName, tableSchema.getName(), indexSchema.getName(),
            file.getAbsolutePath(), e);
        if (!finishedRead || errorsInARow++ > 20) {
          myResult = true;
          return this;
        }
      }
      myResult = false;
      return this;
    }

    private byte[] allocBuffer(int len) {
      return new byte[len];
    }
  }
}
