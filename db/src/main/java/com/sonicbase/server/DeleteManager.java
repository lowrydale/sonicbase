package com.sonicbase.server;

import com.sonicbase.common.AWSClient;
import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.common.Logger;
import com.sonicbase.common.Record;
import com.sonicbase.index.Index;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.ISO8601;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by lowryda on 5/15/17.
 */
public class DeleteManager {

  private Logger logger;

  private final DatabaseServer databaseServer;
  private ThreadPoolExecutor executor;
  private Thread mainThread;
  private ThreadPoolExecutor freeExecutor;

  public DeleteManager(DatabaseServer databaseServer) {
    this.databaseServer = databaseServer;
    logger = new Logger(databaseServer.getDatabaseClient());
    this.executor = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors() * 2,
        Runtime.getRuntime().availableProcessors() * 2, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
    this.freeExecutor = new ThreadPoolExecutor(4, 4, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
  }

  public void saveDeletes(String dbName, String tableName, String indexName, ConcurrentLinkedQueue<Object[]> keysToDelete) {
    try {
      String dateStr = ISO8601.to8601String(new Date(System.currentTimeMillis()));
      File file = new File(getReplicaRoot(), dateStr + ".bin");
      while (file.exists()) {
        Random rand = new Random(System.currentTimeMillis());
        file = new File(getReplicaRoot() + dateStr + "-" + rand.nextInt(50000) + ".bin");
      }
      file.getParentFile().mkdirs();
      TableSchema tableSchema = databaseServer.getCommon().getTables(dbName).get(tableName);
      try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)))) {
        out.writeUTF(dbName);
        out.writeUTF(tableName);
        out.writeUTF(indexName);
        out.writeLong(databaseServer.getCommon().getSchemaVersion() + 1);
        for (Object[] key : keysToDelete) {
          out.write(DatabaseCommon.serializeKey(tableSchema, indexName, key));
        }
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private AtomicReference<LogManager.ByteCounterStream> counterStream = new AtomicReference<>();

  public void doDeletes(boolean ignoreVersion) {
    try {
      synchronized (this) {
        File dir = getReplicaRoot();
        if (dir.exists()) {
          File[] files = dir.listFiles();
          if (files != null && files.length != 0) {
            Arrays.sort(files, new Comparator<File>() {
              @Override
              public int compare(File o1, File o2) {
                return o1.getAbsolutePath().compareTo(o2.getAbsolutePath());
              }
            });
            List<Future> futures = new ArrayList<>();
            counterStream.set(new LogManager.ByteCounterStream(new FileInputStream(files[0])));
            try (DataInputStream in = new DataInputStream(new BufferedInputStream(counterStream.get()))) {
              String dbName = in.readUTF();
              String tableName = in.readUTF();
              TableSchema tableSchema = databaseServer.getCommon().getTables(dbName).get(tableName);
              String indexName = in.readUTF();
              long schemaVersionToDeleteAt = in.readLong();
              if (!ignoreVersion && schemaVersionToDeleteAt > databaseServer.getCommon().getSchemaVersion()) {
                return;
              }
              final Index index = databaseServer.getIndices().get(dbName).getIndices().get(tableName).get(indexName);
              List<Object[]> batch = new ArrayList<>();
              int errorsInARow = 0;
              while (true) {
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
                  logger.error("Error deserializing key", e);
                  if (errorsInARow++ > 20) {
                    break;
                  }
                  continue;
                }
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
                          byte[][] content = databaseServer.fromUnsafeToRecords(value);
                          if (content != null) {
                            if ((Record.DB_VIEW_FLAG_DELETING & Record.getDbViewFlags(content[0])) != 0) {
                              Object toFree = index.remove(currKey);
                              if (toFree != null) {
                                //  toFreeBatch.add(toFree);
                                databaseServer.freeUnsafeIds(toFree);
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
                synchronized (index.getMutex(currKey)) {
                  Object value = index.get(currKey);
                  byte[][] content = databaseServer.fromUnsafeToRecords(value);
                  if (content != null) {
                    if ((Record.DB_VIEW_FLAG_DELETING & Record.getDbViewFlags(content[0])) != 0) {
                      Object toFree = index.remove(currKey);
                      if (toFree != null) {
                        //toFreeBatch.add(toFree);
                        databaseServer.freeUnsafeIds(toFree);
                      }
                    }
                  }
                }
              }
              doFreeMemory(toFreeBatch);
            }

            for (Future future : futures) {
              future.get();
            }
            bytesRead.addAndGet(counterStream.get().getCount());
            counterStream.set(null);
            files[0].delete();
          }
        }
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
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
        while (true) {
          try {
            Thread.sleep(2_000);
            doDeletes(false);
          }
          catch (Exception e) {
            logger.error("Error procesing deletes file", e);
          }
        }
      }
    });
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
      if (dir.exists()) {
        File[] files = dir.listFiles();
        for (File file : files) {
          totalBytes += file.length();
        }
        while (true) {
          files = dir.listFiles();
          if (files == null || files.length == 0) {
            return;
          }
          doDeletes(true);
        }
      }
    }
    finally {
      isForcingDeletes.set(false);
    }
  }
}
