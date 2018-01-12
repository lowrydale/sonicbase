package com.sonicbase.misc;

import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.common.Logger;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.server.DeltaManager;
import org.apache.giraph.utils.Varint;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.sonicbase.server.DeltaManager.SNAPSHOT_PARTITION_COUNT;


/**
 * Created by lowryda on 9/4/17.
 */
public class FindIdInSnapshot {

  static Logger logger = new Logger(null);

  private static final String SNAPSHOT_STR = "snapshot/";
  private static final String INDEX_STR = ", index=";
  private static final String RATE_STR = ", rate=";
  private static final String DURATION_STR = ", duration(s)=";

  public static void main(String[] args) throws Exception {

    File dataRoot = new File(args[0]);
    String dbName = args[1];
    final int shard = Integer.valueOf(args[2]);
    final int replica = Integer.valueOf(args[3]);
    final String tableName = args[4];
    final String indexName = args[5];
    final long id = Long.valueOf(args[6]);

    File snapshotBaseDir = new File(dataRoot, "snapshot/" + shard + "/" + replica + "/" +
        dbName);
    int highestSnapshot = DeltaManager.getHighestCommittedSnapshotVersion(snapshotBaseDir, logger);

    if (highestSnapshot == -1) {
      System.out.println("highestSnapshot=-1");
      return;
    }

    final File snapshotDir = new File(snapshotBaseDir, String.valueOf(highestSnapshot));

    ThreadPoolExecutor executor = new ThreadPoolExecutor(SNAPSHOT_PARTITION_COUNT, SNAPSHOT_PARTITION_COUNT, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

    final AtomicLong recoveredCount = new AtomicLong();
    recoveredCount.set(0);

    final long indexBegin = System.currentTimeMillis();
    recoveredCount.set(0);
    final AtomicLong lastLogged = new AtomicLong(System.currentTimeMillis());
    File file = snapshotDir;


    final AtomicBoolean found = new AtomicBoolean();
    List<Future> futures = new ArrayList<>();
    if (!file.exists()) {
      System.out.println("Checking table=" + tableName + ", index=" + indexName + " file doesn't exist: " + file.getAbsolutePath());
    }
    else {
      System.out.println("Checking table=" + tableName + ", index=" + indexName);
      File tableFile = new File(file, tableName);
      File indexDir = new File(tableFile, indexName);
      for (final File indexFile : indexDir.listFiles()) {
        DatabaseCommon common = new DatabaseCommon();
        common.setShard(shard);
        common.setReplica(replica);
        common.loadSchema(dataRoot.getAbsolutePath());
        final TableSchema tableSchema = common.getTables(dbName).get(tableName);

        futures.add(executor.submit(new Callable<Boolean>() {
                                      @Override
                                      public Boolean call() throws Exception {
                                        try (DataInputStream inStream = new DataInputStream(new BufferedInputStream(new FileInputStream(indexFile)))) {
                                          while (true) {

                                            if (!inStream.readBoolean()) {
                                              break;
                                            }
                                            Object[] key = DatabaseCommon.deserializeKey(tableSchema, inStream);

                                            if (id == (long)key[0]) {
                                              System.out.println("Found id=" + id + " ************************");
                                              found.set(true);
                                              break;
                                            }
                                            int count = (int) Varint.readSignedVarLong(inStream);
                                            byte[][] records = new byte[count][];
                                            for (int i = 0; i < records.length; i++) {
                                              int len = (int) Varint.readSignedVarLong(inStream);
                                              records[i] = new byte[len];
                                              inStream.readFully(records[i]);
                                            }

                                            recoveredCount.incrementAndGet();
                                            if ((System.currentTimeMillis() - lastLogged.get()) > 2000) {
                                              lastLogged.set(System.currentTimeMillis());
                                              System.out.println("Recover progress - table=" + tableName + INDEX_STR + indexName + ": count=" + recoveredCount.get() + RATE_STR +
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
      if (!found.get()) {
        System.out.println("Didn't find id: " + id + " *********************");
      }
      System.out.println("Recover progress - finished index. table=" + tableName + INDEX_STR + indexName + ": count=" + recoveredCount.get() + RATE_STR +
          ((float) recoveredCount.get() / (float) ((System.currentTimeMillis() - indexBegin)) * 1000f) +
          DURATION_STR + (System.currentTimeMillis() - indexBegin) / 1000f);

    }
  }
}
