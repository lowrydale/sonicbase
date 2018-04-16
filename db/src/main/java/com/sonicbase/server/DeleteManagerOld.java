package com.sonicbase.server;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.*;
import com.sonicbase.index.Index;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.DateUtils;
import org.apache.commons.io.FileUtils;
import org.apache.giraph.utils.Varint;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by lowryda on 5/15/17.
 */
public class DeleteManagerOld implements DeleteManager {

  private Logger logger;

  private final DatabaseServer databaseServer;
  private ThreadPoolExecutor executor;
  private Thread mainThread;
  private ThreadPoolExecutor freeExecutor;
  private boolean shutdown;

  public DeleteManagerOld(DatabaseServer databaseServer) {
    this.databaseServer = databaseServer;
    logger = new Logger(/*databaseServer.getDatabaseClient()*/ null);
    this.executor = ThreadUtil.createExecutor(Runtime.getRuntime().availableProcessors() * 2, "SonicBase DeleteManagerOld Thread");
    this.freeExecutor = ThreadUtil.createExecutor(4, "SonicBase DeleteManagerOld FreeExecutor Thread");
  }

  public void saveDeletes(String dbName, String tableName, String indexName, ConcurrentLinkedQueue<DeleteManagerImpl.DeleteRequest> deleteRequests) {
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
        for (DeleteManagerImpl.DeleteRequest key : deleteRequests) {
          out.write(DatabaseCommon.serializeKey(tableSchema, indexName, key.getKey()));
        }
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private AtomicReference<LogManager.ByteCounterStream> counterStream = new AtomicReference<>();

  public void doDeletes(boolean ignoreVersion, File file) {
    try {
      int countDeleted = 0;
      synchronized (this) {
        File dir = getReplicaRoot();
        if (dir.exists()) {
          if (true) {
            try {
              logger.info("DeleteManager deleting file - begin: file=" + file.getAbsolutePath());
              List<Future> futures = new ArrayList<>();
              counterStream.set(new LogManager.ByteCounterStream(new FileInputStream(file)));
              try (DataInputStream in = new DataInputStream(new BufferedInputStream(counterStream.get()))) {
                short serializationVersion = (short) Varint.readSignedVarLong(in);
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
                    continue;
                  }
                  countDeleted++;
                  batch.add(key);
                  if (batch.size() > 100_000) {
                    final List<Object[]> currBatch = batch;
                    batch = new ArrayList<>();
                    futures.add(executor.submit(new Callable() {
                      @Override
                      public Object call() throws Exception {
                        final List<Object> toFreeBatch = new ArrayList<>();
                        for (Object[] currKey : currBatch) {
                          synchronized (index.getMutex(currKey)) {
                            Object value = index.get(currKey);
                            if (value != null) {
                              byte[][] content = databaseServer.fromUnsafeToRecords(value);
                              if (content != null) {
                                if (indexSchema.isPrimaryKey()) {
                                  if ((Record.DB_VIEW_FLAG_DELETING & Record.getDbViewFlags(content[0])) != 0) {
                                    Object toFree = index.remove(currKey);
                                    if (toFree != null) {
                                      //  toFreeBatch.add(toFree);
                                      databaseServer.freeUnsafeIds(toFree);
                                    }
                                  }
                                }
                                else {
                                  if ((Record.DB_VIEW_FLAG_DELETING & KeyRecord.getDbViewFlags(content[0])) != 0) {
                                    Object toFree = index.remove(currKey);
                                    if (toFree != null) {
                                      //  toFreeBatch.add(toFree);
                                      databaseServer.freeUnsafeIds(toFree);
                                    }
                                  }
                                }
                              }
                            }
                          }
                        }
                        doFreeMemory(toFreeBatch);
                        return null;
                      }
                    }));
                  }
                }
                final List<Object> toFreeBatch = new ArrayList<>();
                for (Object[] currKey : batch) {

                  if (shutdown) {
                    break;
                  }
                  //                List<Integer> selectedShards = Repartitioner.findOrderedPartitionForRecord(true, false,
                  //                    fieldOffsets, databaseServer.getCommon(), tableSchema,
                  //                    indexName, null, BinaryExpression.Operator.equal, null, currKey, null);
                  //                if (selectedShards.get(0) == databaseServer.getShard()) {
                  //                  synchronized (index.getMutex(currKey)) {
                  //                    Object value = index.get(currKey);
                  //                    byte[][] content = databaseServer.fromUnsafeToRecords(value);
                  //                    if (content != null) {
                  //                      for (int i = 0; i < content.length; i++) {
                  //                        Record.setDbViewFlags(content[i], (short)0);
                  //                      }
                  //                      Object newValue = databaseServer.toUnsafeFromKeys(content);
                  //                      index.put(currKey, newValue);
                  //                      databaseServer.freeUnsafeIds(value);
                  //                    }
                  //                  }
                  //                }
                  //                else {
                  synchronized (index.getMutex(currKey)) {
                    Object value = index.get(currKey);
                    if (value != null) {
                      byte[][] content = databaseServer.fromUnsafeToRecords(value);
                      if (content != null) {
                        if (indexSchema.isPrimaryKey()) {
                          if ((Record.DB_VIEW_FLAG_DELETING & Record.getDbViewFlags(content[0])) != 0) {
                            Object toFree = index.remove(currKey);
                            if (toFree != null) {
                              //toFreeBatch.add(toFree);
                              databaseServer.freeUnsafeIds(toFree);
                            }
                          }
                        }
                        else {
                          if ((Record.DB_VIEW_FLAG_DELETING & KeyRecord.getDbViewFlags(content[0])) != 0) {
                            Object toFree = index.remove(currKey);
                            if (toFree != null) {
                              //toFreeBatch.add(toFree);
                              databaseServer.freeUnsafeIds(toFree);
                            }
                          }
                        }
                      }
                    }
                  }
                  //                }
                }
                doFreeMemory(toFreeBatch);
              }
              catch (Exception e) {
                logger.error("Error performing deletes", e);
              }

              for (Future future : futures) {
                future.get();
              }
              bytesRead.addAndGet(counterStream.get().getCount());
              counterStream.set(null);
              file.delete();
              logger.info("DeleteManager deleting file - end: file=" + file.getAbsolutePath());
            }
            catch (Exception e) {
              logger.error("Error performing deletes", e);
            }
          }
        }
      }
      //System.out.println("deletes - finished: count=" + countDeleted + ", shard=" + databaseServer.getShard() + ", replica=" + databaseServer.getReplica());
    }
    catch (Exception e) {
      logger.error("Error performing deletes", e);
    }
  }

  private void doFreeMemory(final List<Object> toFreeBatch) {
//    Timer timer = new Timer("Free memory");
//    timer.schedule(new TimerTask(){
//      @Override
//      public void run() {
//        //limit to 4 threads
//        Future future = freeExecutor.submit(new Callable(){
//          @Override
//          public Object call() throws Exception {
    for (Object obj : toFreeBatch) {
      databaseServer.freeUnsafeIds(obj);
    }
//            return null;
//          }
//        });
//        try {
//          future.get();
//        }
//        catch (InterruptedException e) {
//        }
//        catch (ExecutionException e) {
//          logger.error("Error deleting values", e);
//        }
//      }
//    }, 30 * 1000);
  }

  private File getReplicaRoot() {
    return new File(databaseServer.getDataDir(), "deletes/" + databaseServer.getShard() + "/" + databaseServer.getReplica() + "/");
  }

  public void start() {

    mainThread = new Thread(new Runnable() {
      @Override
      public void run() {
        while (!shutdown) {
          try {
            File dir = getReplicaRoot();
            if (dir.exists()) {

              try {
                Thread.sleep(2_000);
              }
              catch (InterruptedException e) {
                break;
              }
              File[] files = dir.listFiles();
              if (files != null && files.length != 0) {
                Arrays.sort(files, new Comparator<File>() {
                  @Override
                  public int compare(File o1, File o2) {
                    return o1.getAbsolutePath().compareTo(o2.getAbsolutePath());
                  }
                });

                doDeletes(false, files[0]);
              }
            }
          }
          catch (Exception e) {
            logger.error("Error procesing deletes file", e);
          }
        }
      }
    }, "SonicBase Deletion Thread");
    mainThread.start();
  }

  public void backupAWS(String bucket, String prefix, String subDirectory) {
    AWSClient awsClient = databaseServer.getAWSClient();
    File srcDir = getReplicaRoot();
    subDirectory += "/deletes/" + databaseServer.getShard() + "/0";

    if (srcDir.exists()) {
      awsClient.uploadDirectory(bucket, prefix, subDirectory, srcDir);
    }
  }

  public void restoreAWS(String bucket, String prefix, String subDirectory) {
    try {
      AWSClient awsClient = databaseServer.getAWSClient();
      File destDir = getReplicaRoot();
      subDirectory += "/deletes/" + databaseServer.getShard() + "/0";

      FileUtils.deleteDirectory(destDir);
      destDir.mkdirs();

      awsClient.downloadDirectory(bucket, prefix, subDirectory, destDir);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public long getBackupLocalFileSystemSize() {
    File dir = getReplicaRoot();
    return com.sonicbase.common.FileUtils.sizeOfDirectory(dir);
  }

  @Override
  public void delteTempDirs() {

  }

  public void backupFileSystem(String directory, String subDirectory) {
    try {
      File dir = getReplicaRoot();
      File destDir = new File(directory, subDirectory + "/deletes/" + databaseServer.getShard() + "/0");
      if (dir.exists()) {
        FileUtils.copyDirectory(dir, destDir);
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public void restoreFileSystem(String directory, String subDirectory) {
    try {
      File destDir = getReplicaRoot();
      if (destDir.exists()) {
        FileUtils.deleteDirectory(destDir);
      }
      destDir.mkdirs();
      File srcDir = new File(directory, subDirectory + "/deletes/" + databaseServer.getShard() + "/0");
      if (srcDir.exists()) {
        FileUtils.copyDirectory(srcDir, destDir);
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
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

  @Override
  public void shutdown() {
    this.shutdown = true;
    if (mainThread != null) {
      mainThread.interrupt();
      try {
        mainThread.join();
      }
      catch (InterruptedException e) {
        throw new DatabaseException(e);
      }
    }
  }

  private long totalBytes = 0;
  private AtomicLong bytesRead = new AtomicLong();

  public double getPercentDeleteComplete() {

    if (totalBytes == 0) {
      return 0;
    }
    LogManager.ByteCounterStream stream = counterStream.get();
    long readBytes = bytesRead.get();
    if (stream != null) {
      readBytes += stream.getCount();
    }

    return (double)readBytes / (double)totalBytes;
  }

  @Override
  public void saveDeletesForRecords(String dbName, String tableName, String indexName, long sequence0, long sequence1, ConcurrentLinkedQueue<DeleteManagerImpl.DeleteRequest> keysToDeleteExpanded) {
//    Index index = databaseServer.getIndices().get(dbName).getIndices().get(tableName).get(indexName);
//    for (DeleteManagerImpl.DeleteRequest request : keysToDeleteExpanded) {
//      Object toFree = index.remove(request.getKey());
//      if (toFree != null) {
//        //  toFreeBatch.add(toFree);
//        databaseServer.freeUnsafeIds(toFree);
//      }
//    }

    saveDeletes(dbName, tableName, indexName, keysToDeleteExpanded);
  }

  @Override
  public void saveDeletesForKeyRecords(String dbName, String tableName, String indexName, long sequence0, long sequence1, ConcurrentLinkedQueue<DeleteManagerImpl.DeleteRequest> keysToDeleteExpanded) {
//    Index index = databaseServer.getIndices().get(dbName).getIndices().get(tableName).get(indexName);
//    for (DeleteManagerImpl.DeleteRequest request : keysToDeleteExpanded) {
//      Object toFree = index.remove(request.getKey());
//      if (toFree != null) {
//        //  toFreeBatch.add(toFree);
//        databaseServer.freeUnsafeIds(toFree);
//      }
//    }

    saveDeletes(dbName, tableName, indexName, keysToDeleteExpanded);
  }

  @Override
  public void saveDeleteForRecord(String dbName, String tableName, String indexName, long sequence0, long sequence1, DeleteManagerImpl.DeleteRequestForRecord request) {
//    ConcurrentLinkedQueue<DeleteManagerImpl.DeleteRequest> keysToDeleteExpanded = new ConcurrentLinkedQueue<>();
//    keysToDeleteExpanded.add(request);
//    saveDeletes(dbName, tableName, indexName, keysToDeleteExpanded);
  }

  @Override
  public void saveDeleteForKeyRecord(String dbName, String tableName, String indexName, long sequence0, long sequence1, DeleteManagerImpl.DeleteRequestForKeyRecord request) {
//    ConcurrentLinkedQueue<DeleteManagerImpl.DeleteRequest> keysToDeleteExpanded = new ConcurrentLinkedQueue<>();
//    keysToDeleteExpanded.add(request);
//    saveDeletes(dbName, tableName, indexName, keysToDeleteExpanded);
  }

  @Override
  public void deleteOldLogs(long lastSnapshot) {

  }

  @Override
  public void buildDeletionsFiles(String dbName, AtomicReference<String> currStage, AtomicLong totalBytes, AtomicLong finishedBytes) {

  }

  @Override
  public void applyDeletesToSnapshot(String dbName, int currDeltaDirNum, AtomicLong finishedBytes) {

  }

  private AtomicBoolean isForcingDeletes = new AtomicBoolean();

  public boolean isForcingDeletes() {
    return isForcingDeletes.get();
  }

  public void forceDeletes() {
    File dir = getReplicaRoot();
    totalBytes = 0;
    bytesRead.set(0);
    isForcingDeletes.set(true);
    try {
      ThreadPoolExecutor executor = new ThreadPoolExecutor(8, 8,
          10_000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1_000), new ThreadPoolExecutor.CallerRunsPolicy());
      try {
        if (dir.exists()) {
          File[] files = dir.listFiles();
          for (File file : files) {
            totalBytes += file.length();
          }
          files = dir.listFiles();
          if (files == null || files.length == 0) {
            return;
          }
          List<Future> futures = new ArrayList<>();
          for (final File file : files) {
            futures.add(executor.submit(new Callable(){
              @Override
              public Object call() throws Exception {
                doDeletes(true, file);
                return null;
              }
            }));
          }
          for (Future future : futures) {
            future.get();
          }
        }
      }
      finally {
        executor.shutdownNow();
      }
    }
    catch (Exception e) {
      logger.error("Error deleting records", e);
    }
    finally {
      isForcingDeletes.set(false);
    }
  }
}
