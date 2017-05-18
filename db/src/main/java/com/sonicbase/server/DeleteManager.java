package com.sonicbase.server;

import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.common.Logger;
import com.sonicbase.index.Index;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.ISO8601;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by lowryda on 5/15/17.
 */
public class DeleteManager {

  private Logger logger;

  private final DatabaseServer databaseServer;
  private ThreadPoolExecutor executor;
  private Thread mainThread;

  public DeleteManager(DatabaseServer databaseServer) {
    this.databaseServer = databaseServer;
    logger = new Logger(databaseServer.getDatabaseClient());
  }

  public void saveDeletes(String dbName, String tableName, String indexName, ConcurrentLinkedQueue<Object[]> keysToDelete) {
    try {
      String dateStr = ISO8601.to8601String(new Date(System.currentTimeMillis()));
      File file = new File(databaseServer.getDataDir(), "deletes/" + databaseServer.getShard() + "/" + databaseServer.getReplica() + "/" + dateStr + ".bin");
      while (file.exists()) {
        Random rand = new Random(System.currentTimeMillis());
        file = new File(databaseServer.getDataDir(), "deletes/" + databaseServer.getShard() + "/" + databaseServer.getReplica() + "/" + dateStr + "-" + rand.nextInt(50000) + ".bin");
      }
      file.getParentFile().mkdirs();
      TableSchema tableSchema = databaseServer.getCommon().getTables(dbName).get(tableName);
      try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)))) {
        out.writeUTF(dbName);
        out.writeUTF(tableName);
        out.writeUTF(indexName);
        for (Object[] key : keysToDelete) {
          DatabaseCommon.serializeKey(tableSchema, indexName, key);
        }
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public void start() {
    this.executor = new ThreadPoolExecutor(8, 8, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

    mainThread = new Thread(new Runnable() {
      @Override
      public void run() {
        while (true) {
          try {
            Thread.sleep(10000);
            File dir = new File(databaseServer.getDataDir(), "deletes/" + databaseServer.getShard() + "/" + databaseServer.getReplica() + "/");
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
                try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(files[0])))) {
                  String dbName = in.readUTF();
                  String tableName = in.readUTF();
                  TableSchema tableSchema = databaseServer.getCommon().getTables(dbName).get(tableName);
                  String indexName = in.readUTF();
                  final Index index = databaseServer.getIndices().get(dbName).getIndices().get(tableName).get(indexName);
                  List<Object[]> batch = new ArrayList<>();
                  while (true) {
                    Object[] key = null;
                    try {
                      key = DatabaseCommon.deserializeKey(tableSchema, in);
                    }
                    catch (EOFException e) {
                      //expected
                      break;
                    }
                    batch.add(key);
                    if (batch.size() > 5000) {
                      final List<Object[]> currBatch = batch;
                      batch = new ArrayList<>();
                      futures.add(executor.submit(new Callable() {
                        @Override
                        public Object call() throws Exception {
                          for (Object[] key : currBatch) {
                            Object toFree = index.remove(key);
                            if (toFree != null) {
                              databaseServer.freeUnsafeIds(toFree);
                            }
                          }
                          return null;
                        }
                      }));
                    }
                  }
                  for (Object[] key : batch) {
                    Object toFree = index.remove(key);
                    if (toFree != null) {
                      databaseServer.freeUnsafeIds(toFree);
                    }
                  }
                }

                for (Future future : futures) {
                  future.get();
                }
                files[0].delete();
              }
            }
          }
          catch (Exception e) {
            logger.error("Error procesing deletes file", e);
          }
        }
      }
    });
    mainThread.start();
  }
}
