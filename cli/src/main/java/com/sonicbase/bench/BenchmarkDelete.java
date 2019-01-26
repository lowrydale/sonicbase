package com.sonicbase.bench;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class BenchmarkDelete {

  public static final Logger logger = LoggerFactory.getLogger(BenchmarkDelete.class);


  private static final MetricRegistry METRICS = new MetricRegistry();

  public static final String ERROR_STR = "Error";


  private static final com.codahale.metrics.Timer LOOKUP_STATS = METRICS.timer("lookup");
  private Thread mainThread;
  private boolean shutdown;
  final AtomicLong totalBegin = new AtomicLong(System.currentTimeMillis());
  final AtomicLong totalSelectDuration = new AtomicLong();
  final AtomicLong selectErrorCount = new AtomicLong();
  final AtomicLong selectBegin = new AtomicLong(System.currentTimeMillis());
  final AtomicLong selectOffset = new AtomicLong();
  final AtomicLong selectCount = new AtomicLong();
  final AtomicLong readCount = new AtomicLong();

  public void start(String address, final String cluster, final int shardCount, final Integer shard, final long count) {
    shutdown = false;
    selectBegin.set(System.currentTimeMillis());
    doResetStats();
    mainThread = new Thread(() -> {
      try {
        final AtomicInteger cycle = new AtomicInteger();
        final long startId = shard * count;
        logger.info("startId={}, count={}, shard={}", startId, count, shard);

        File file = new File(System.getProperty("user.dir"), "config/config-" + cluster + ".yaml");
        if (!file.exists()) {
          file = new File(System.getProperty("user.dir"), "db/src/main/resources/config/config-" + cluster + ".yaml");
          logger.info("Loaded config resource dir");
        }
        else {
          logger.info("Loaded config default dir");
        }

        logger.info("Using address: address={}", address);

        Class.forName("com.sonicbase.jdbcdriver.Driver");

        final java.sql.Connection conn = DriverManager.getConnection("jdbc:sonicbase:" + address + ":9010/db", "user", "password");

        //test insert
        try {
          Thread[] threads = new Thread[32];
          for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
              outer:
              while (!shutdown) {
                try {
                  DatabaseClient client = ((ConnectionProxy)conn).getDatabaseClient();
                  client.syncSchema();
                  IndexSchema indexSchema = client.getSchema("db").getTables().get("persons").getIndices().get("_primarykey");
                  TableSchema.Partition[] partitions = indexSchema.getCurrPartitions();
                  Object[][] keys = new Object[partitions.length][];
                  keys[0] = new Object[]{0L};
                  for (int m = 1; m < partitions.length; m++) {
                    Object[] upperKey = partitions[m - 1].getUpperKey();
                    keys[m] = upperKey;
                  }
                  int offset = shard % partitions.length;
                  long actualStartId = (long)keys[offset][0];
                  logger.info("starting id=" + actualStartId);
                  cycle.incrementAndGet();

                  PreparedStatement delStmt = conn.prepareStatement("delete from persons where id1 >=" + actualStartId + " limit 1000");
                  long begin = System.nanoTime();

                  delStmt.executeQuery();
                  delStmt.executeUpdate();
                  selectCount.addAndGet(1000);
                  totalSelectDuration.addAndGet(System.nanoTime() - begin);
                }
                catch (Exception e) {
                  logger.error(ERROR_STR, e);
                }
              }
            });
            threads[i].start();
          }
        }
        catch (Exception e) {
          logger.error(ERROR_STR, e);
        }
        while (true) {
          Thread.sleep(1000);
        }
      }
      catch (Exception e) {
        logger.error(ERROR_STR, e);
      }
    });
    mainThread.start();
  }

  private void doResetStats() {
    totalBegin.set(System.currentTimeMillis());
    totalSelectDuration.set(0);
    selectErrorCount.set(0);
    selectBegin.set(System.currentTimeMillis());
    selectOffset.set(0);
    readCount.set(0);
  }

  public void stop() {
    shutdown = true;
    mainThread.interrupt();
  }

  public String stats() {
    ObjectNode dict = new ObjectNode(JsonNodeFactory.instance);
    dict.put("begin", selectBegin.get());
    dict.put("count", readCount.get());
    dict.put("errorCount", selectErrorCount.get());
    dict.put("totalDuration", totalSelectDuration.get());
    dict.put("countDead", 0);
    dict.put("activeThreads", 0);
    return dict.toString();
  }

  public void resetStats() {
    doResetStats();
  }

}
