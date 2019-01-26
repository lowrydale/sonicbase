package com.sonicbase.bench;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.sonicbase.bench.BenchmarkInsert.STRING_KEY;

public class BenchmarkRangeQuery {

  public static final Logger logger = LoggerFactory.getLogger(BenchmarkRangeQuery.class);

  private static final MetricRegistry METRICS = new MetricRegistry();

  private static final com.codahale.metrics.Timer LOOKUP_STATS = METRICS.timer("lookup");
  public static final String ERROR_STR = "Error";
  public static final String SELECT_PERSONS_ID_STR = "select persons.id  ";
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
              while (!shutdown) {
                try {
                  PreparedStatement stmt = null;
                  ResultSet ret = null;
                  if (true) {
                    DatabaseClient client = ((ConnectionProxy)conn).getDatabaseClient();
                    client.syncSchema();
                    IndexSchema indexSchema = null;
                    if (STRING_KEY) {
                      indexSchema = client.getSchema("db").getTables().get("strings").getIndices().get("_primarykey");
                    }
                    else {
                      indexSchema = client.getSchema("db").getTables().get("persons").getIndices().get("_primarykey");
                    }
                    TableSchema.Partition[] partitions = indexSchema.getCurrPartitions();
                    Object[][] keys = new Object[partitions.length][];
                    if (STRING_KEY) {
                      keys[0] = new Object[]{"00000000000000000000".getBytes("utf-8")};
                    }
                    else {
                      keys[0] = new Object[]{0L};
                    }
                    for (int m = 1; m < partitions.length; m++) {
                      Object[] upperKey = partitions[m - 1].getUpperKey();
                      keys[m] = upperKey;
                    }
                    int offset = shard % partitions.length;
                    long actualLongStartId = 0;
                    byte[] actualStringStartId = null;
                    if (STRING_KEY) {
                      actualStringStartId = (byte[])keys[offset][0];
                    }
                    else {
                      actualLongStartId = (long) keys[offset][0];
                    }
                    cycle.incrementAndGet();
                    long begin = System.nanoTime();

                    if (STRING_KEY) {
                      logger.info("starting id='" +  new String(actualStringStartId, "utf-8"));
                      stmt = conn.prepareStatement("select id1  " +
                          "from strings where id1 >= '" + new String(actualStringStartId, "utf-8") + "' order by id1 asc");
                    }
                    else {
                      logger.info("starting id=" + actualLongStartId);
                      stmt = conn.prepareStatement("select persons.id1  " +
                          "from persons where id1 >= " + actualLongStartId + " order by id1 asc");
                    }
                    long beginNano = System.nanoTime();
                    ret = stmt.executeQuery();
                    totalSelectDuration.addAndGet(System.nanoTime() - begin);

                    long lastId = 0;
                    int countThisPass = 0;
                    while (true) {
                      begin = System.nanoTime();
                      if (!ret.next()) {
                        break;
                      }
                      totalSelectDuration.addAndGet(System.nanoTime() - begin);

                      if (!STRING_KEY) {
                        if (countThisPass == 0) {
                          logger.info("first key returned=" + ret.getLong("id1"));
                        }
                        long idRead = ret.getLong("id1");
                        if (idRead < lastId) {
                          logger.error("key returned out of order: idRead={}, lastId={} #############################################", idRead, lastId);
                        }
                        lastId = idRead;
                      }
                      countThisPass++;
                      if (readCount.incrementAndGet() % 100000 == 0) {
                        StringBuilder builder = new StringBuilder();
                        if (STRING_KEY) {
                          builder.append("currKey=").append(ret.getString("id1"));
                        }
                        else {
                          builder.append("currKey=").append(ret.getLong("id1"));
                        }
                        builder.append(", count=").append(readCount.get());
                        builder.append(", countThisPass=").append(countThisPass);
                        Snapshot snapshot = LOOKUP_STATS.getSnapshot();
                        builder.append(String.format(", rate=%.2f", readCount.get() / (double) (System.currentTimeMillis() - totalBegin.get()) * 1000f));
                        builder.append(String.format(", avg=%.2f nanos", totalSelectDuration.get() / (double) readCount.get()));
                        builder.append(String.format(", 99th=%.2f nanos", snapshot.get99thPercentile()));
                        builder.append(String.format(", max=%d nanos", snapshot.getMax()));
                        builder.append(", errorCount=").append(selectErrorCount.get());

                        logger.info(builder.toString());
                      }
                    }
                    logger.info("countThisPass - finished={}", countThisPass);
                    totalSelectDuration.addAndGet(System.nanoTime() - beginNano);
                  }

                  selectCount.incrementAndGet();
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
