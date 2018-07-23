package com.sonicbase.server;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.*;
import com.sonicbase.index.Index;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.DateUtils;
import org.apache.giraph.utils.Varint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by lowryda on 5/15/17.
 */
@SuppressWarnings("squid:S1172") // all methods called from method invoker must have cobj and replayed command parms
public class DeleteManager {

  private static final String ERROR_PERFORMING_DELETES_STR = "Error performing deletes";
  private static Logger logger = LoggerFactory.getLogger(DeleteManager.class);

  private final com.sonicbase.server.DatabaseServer databaseServer;
  private ThreadPoolExecutor executor;
  private Thread mainThread;
  private ThreadPoolExecutor freeExecutor;
  private boolean shutdown;
  private LinkedBlockingQueue<Object> toFree = new LinkedBlockingQueue<>();
  private Thread freeThread;
  private AtomicLong countRead = new AtomicLong();

  DeleteManager(final DatabaseServer databaseServer) {
    this.databaseServer = databaseServer;
    this.executor = ThreadUtil.createExecutor(Runtime.getRuntime().availableProcessors() * 2, "SonicBase DeleteManager Thread");
    this.freeExecutor = ThreadUtil.createExecutor(4, "SonicBase DeleteManager FreeExecutor Thread");
    freeThread = ThreadUtil.createThread(() -> {
      while (!shutdown) {
        try {
          final Object obj = toFree.poll(10_000, TimeUnit.MILLISECONDS);
          if (obj == null) {
            continue;
          }
          final List<Object> batch = new ArrayList<>();
          toFree.drainTo(batch, 1000);
          freeExecutor.submit(() -> {
            for (Object currObj : batch) {
              databaseServer.getAddressMap().freeUnsafeIds(currObj);
            }
          });
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

  public static class DeleteRequest {
    private Object[] key;

    DeleteRequest(Object[] key) {
      this.key = key;
    }

    public Object[] getKey() {
      return key;
    }
  }

  private void saveDeletes(String dbName, String tableName, String indexName, ConcurrentLinkedQueue<DeleteRequest> deleteRequests) {
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
        InputStream countIn = new LogManager.ByteCounterStream(new FileInputStream(file), countRead);
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(countIn))) {
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
            return;
          }
          final Index index = databaseServer.getIndices().get(dbName).getIndices().get(tableName).get(indexName);
          List<Object[]> batch = new ArrayList<>();
          int errorsInARow = 0;
          while (!shutdown) {
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
              break;
            }
            catch (Exception e) {
              logger.error("Error deserializing key: " + ((errorsInARow > 20) ? " aborting" : ""), e);
              if (errorsInARow++ > 20) {
                break;
              }
            }
          }
          processBatch(futures, indexSchema, index, batch);
        }
        catch (Exception e) {
          logger.error(ERROR_PERFORMING_DELETES_STR, e);
        }

        for (Future future : futures) {
          future.get();
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

  private List<Object[]> processBatch(List<Future> futures, final IndexSchema indexSchema, final Index index, List<Object[]> batch) {
    final List<Object[]> currBatch = batch;
    batch = new ArrayList<>();
    futures.add(executor.submit((Callable) () -> {
      final List<Object> toFreeBatch = new ArrayList<>();
      for (Object[] currKey : currBatch) {
        synchronized (index.getMutex(currKey)) {
          Object value = index.get(currKey);
          if (value != null) {
            byte[][] content = databaseServer.getAddressMap().fromUnsafeToRecords(value);
            if (content != null) {
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
          }
        }
      }
      doFreeMemory(toFreeBatch);
      return null;
    }));
    return batch;
  }

  private void doFreeMemory(final List<Object> toFreeBatch) {
    for (Object obj : toFreeBatch) {
      databaseServer.getAddressMap().freeUnsafeIds(obj);
    }
  }

  private File getReplicaRoot() {
    return new File(databaseServer.getDataDir(), "deletes" + File.separator + databaseServer.getShard() +
        File.separator + databaseServer.getReplica() + File.separator);
  }

  public void start() {

    mainThread = new Thread(() -> {
      while (!shutdown) {
        try {
          File dir = getReplicaRoot();
          if (dir.exists()) {

            if (doSleep()) {
              break;
            }

            File[] files = dir.listFiles();
            if (files != null && files.length != 0) {
              Arrays.sort(files, Comparator.comparing(File::getAbsolutePath));

              doDeletes(false, files[0]);
            }
          }
        }
        catch (Exception e) {
          logger.error("Error procesing deletes file", e);
        }
      }
    }, "SonicBase Deletion Thread");
    mainThread.start();
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
    if (mainThread != null) {
      mainThread.interrupt();
      try {
        mainThread.join();
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new DatabaseException(e);
      }
    }
  }

  private long totalBytes = 0;

  double getPercentDeleteComplete() {

    if (totalBytes == 0) {
      return 0;
    }
    if (countRead == null) {
      return 0;
    }

    return (double)countRead.get() / (double)totalBytes;
  }

  void saveDeletesForRecords(String dbName, String tableName, String indexName, long sequence0, long sequence1, ConcurrentLinkedQueue<DeleteRequest> keysToDeleteExpanded) {
    saveDeletes(dbName, tableName, indexName, keysToDeleteExpanded);
  }

  void saveDeletesForKeyRecords(String dbName, String tableName, String indexName, long sequence0, long sequence1, ConcurrentLinkedQueue<DeleteRequest> keysToDeleteExpanded) {
    saveDeletes(dbName, tableName, indexName, keysToDeleteExpanded);
  }

  public static class DeleteRequestForKeyRecord extends DeleteRequest {

    DeleteRequestForKeyRecord(Object[] key) {
      super(key);
    }

    DeleteRequestForKeyRecord(Object[] key, byte[] primaryKeyBytes) {
      super(key);
    }
  }

  public static class DeleteRequestForRecord extends DeleteRequest {
    DeleteRequestForRecord(Object[] key) {
      super(key);
    }
  }

  private AtomicBoolean isForcingDeletes = new AtomicBoolean();

  boolean isForcingDeletes() {
    return isForcingDeletes.get();
  }

  public ComObject forceDeletes(ComObject cobj, boolean replayedCommand) {
    File dir = getReplicaRoot();
    totalBytes = 0;

    isForcingDeletes.set(true);
    ThreadPoolExecutor localExecutor = new ThreadPoolExecutor(8, 8,
        10_000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1_000), new ThreadPoolExecutor.CallerRunsPolicy());
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
}
