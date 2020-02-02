/* © 2020 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.bench;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.ComObject;
import com.sonicbase.common.Config;
import com.sonicbase.common.FileUtils;
import com.sonicbase.embedded.EmbeddedDatabase;
import com.sonicbase.index.Index;
import com.sonicbase.index.Indices;
import com.sonicbase.index.NativePartitionedTreeImpl;
import com.sonicbase.index.NativeSkipListMapImpl;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.schema.DataType;
import com.sonicbase.schema.FieldSchema;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.server.DatabaseServer;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class BenchNativePut {

  public static void main(String[] args) throws SQLException, IOException, InterruptedException {

    int algo = Integer.parseInt(args[0]);
    int threadCount = Integer.valueOf(args[1]);
    int batchSize = Integer.valueOf(args[2]);
    long count = Long.valueOf(args[3]);


    if (algo == 0) {
      benchSkipListMap(threadCount, batchSize, count);
    }
    else if (algo == 1){
      benchPartitionedTree(threadCount, batchSize, count);
    }
    else {
      benchJavaSkipListMap(threadCount, batchSize, count);
    }
  }

  private static void benchJavaSkipListMap(int threadCount, int batchSize, long count) throws InterruptedException {
    ConcurrentSkipListMap<Long, Long> map = new ConcurrentSkipListMap<>();

    final long begin = System.currentTimeMillis();
    AtomicLong offset = new AtomicLong();
    Thread[] threads = new Thread[threadCount];

    for (int k = 0; k < threads.length; k++) {
      final int threadOffset = k;
      threads[k] = new Thread(() -> {
        for (int i = 0; i < 1_000_000_000; i++) {
          long key = threadOffset * 1_000_000_000 + i;
          if (offset.incrementAndGet() % 1_000_000 == 0) {
            System.out.println("progress: count==" + offset + ", rate=" + ((double) offset.get() / ((double) System.currentTimeMillis() - begin) * 1000f));
          }
          map.put(key, key);

          if (offset.get() > count) {
            return;
          }
        }
      });
      threads[k].start();
    }

    for (Thread thread : threads) {
      thread.join();
    }
  }

  public static void benchSkipListMap(int threadCount, int batchSize, long count) throws InterruptedException, IOException {

    Index index = initIndex();

    NativeSkipListMapImpl map = (NativeSkipListMapImpl) index.getImpl();

    final long begin = System.currentTimeMillis();
    AtomicLong offset = new AtomicLong();
    Thread[] threads = new Thread[threadCount];

    for (int k = 0; k < threads.length; k++) {
      final int threadOffset = k;
      threads[k] = new Thread(() -> {
        AtomicLong localOffset = new AtomicLong();
        for (long i = 0; i < 1_000_000_000_000L; i++) {
          if (batchSize == 1) {
            Object[] key = new Object[]{threadOffset * 1_000_000_000L + localOffset.incrementAndGet()};
            map.put(key, 0L);

            if (offset.incrementAndGet() % 1_000_000 == 0) {
              System.out.println("progress: count==" + offset + ", rate=" + ((double) offset.get() / ((double) System.currentTimeMillis() - begin) * 1000f));
            }
          }
          else {
            Object[][] keys = new Object[batchSize][];
            long[] values = new long[keys.length];
            long[] retValues = new long[keys.length];
            for (int j = 0; j < keys.length; j++) {
              keys[j] = new Object[]{threadOffset * 1_000_000_000L + localOffset.incrementAndGet()};

              if (offset.incrementAndGet() % 1_000_000 == 0) {
                System.out.println("progress: count==" + offset + ", rate=" + ((double) offset.get() / ((double) System.currentTimeMillis() - begin) * 1000f));
              }
            }
            map.put(0, keys, values, retValues);
          }

          if (offset.get() > count) {
            return;
          }
        }
      });
      threads[k].start();
    }

    for (Thread thread : threads) {
      thread.join();
    }
  }

  private static Index initIndex() throws IOException {
    String configStr = IOUtils.toString(BenchNativePut.class.getResourceAsStream("/config/config-1-local.yaml"), "utf-8");
    Config config = new Config(configStr);
    com.sonicbase.server.DatabaseServer server = new DatabaseServer();
    Config.copyConfig("test");
    server.setConfig(config, "localhost", 9010, true, new AtomicBoolean(), new AtomicBoolean(), "gc.log", false);
    server.setIsRunning(true);

    FileUtils.deleteDirectory(new File(server.getDataDir()));
    server.setRecovered(true);

    server.getCommon().addDatabase("test");
    File file = new File(server.getDataDir(), "snapshot/0/0/test");
    file.mkdirs();

    TableSchema tableSchema = createTable();
    IndexSchema indexSchema = createIndexSchema(tableSchema);
    server.getCommon().getTables("test").put(tableSchema.getName(), tableSchema);

    server.getIndices().put("test", new Indices());
    Comparator[] comparators = tableSchema.getComparators(new String[]{"field1"});

    server.getIndices("test").addIndex(server.getPort(), new HashMap<Long, Boolean>(),tableSchema, indexSchema.getName(), comparators);

    Index index = server.getIndex("test", tableSchema.getName(), indexSchema.getName());
    return index;
  }
  public static void benchPartitionedTree(int threadCount, int batchSize, long count) throws IOException, InterruptedException {
    Index index = initIndex();

    long begin = System.currentTimeMillis();
    NativePartitionedTreeImpl partitionedTree = (NativePartitionedTreeImpl)index.getImpl();
    if (true) {
      AtomicLong offset = new AtomicLong();
      Thread[] threads = new Thread[threadCount];

      for (int k = 0; k < threads.length; k++) {
        threads[k] = new Thread(new Runnable() {
          @Override
          public void run() {
            for (int i = 0; i < 25_000_000; i++) {
              Object[] key = new Object[]{offset.incrementAndGet()};
              if ((long)key[0] % 1_000_000 == 0) {
                System.out.println("progress: count==" + offset + ", rate=" + ((double) offset.get() / ((double) System.currentTimeMillis() - begin) * 1000f));
              }
              partitionedTree.put(key, 0L);
              if (offset.get() > count) {
                return;
              }
            }
          }
        });
        threads[k].start();
      }
      for (Thread thread : threads) {
        thread.join();
      }
    }
    else {
      AtomicLong offset = new AtomicLong();
      Thread[] threads = new Thread[threadCount];

      for (int k = 0; k < threads.length; k++) {
        threads[k] = new Thread(new Runnable() {
          @Override
          public void run() {
            for (int i = 0; i < 10_000_000; i++) {
              Object[][] keys = new Object[batchSize][];
              long[] values = new long[keys.length];
              long[] retValues = new long[keys.length];
              for (int j = 0; j < keys.length; j++) {
                keys[j] = new Object[]{offset.incrementAndGet()};
                if ((long)keys[j][0] % 1_000_000 == 0) {
                  System.out.println("progress: count==" + offset + ", rate=" + ((double) offset.get() / ((double) System.currentTimeMillis() - begin) * 1000f));
                }
              }
              partitionedTree.put(keys, values, retValues);

              if (offset.get() > count) {
                return;
              }
            }
          }
        });
        threads[k].start();
      }

      for (Thread thread : threads) {
        thread.join();
      }
    }

  }

  public static IndexSchema createIndexSchema(TableSchema tableSchema) {
    return createIndexSchema(tableSchema, 1);
  }

  public static IndexSchema createIndexSchema(TableSchema tableSchema, int partitionCount) {
    IndexSchema indexSchema = new IndexSchema();
    indexSchema.setFields(new String[]{"field1"}, tableSchema);
    indexSchema.setIndexId(1);
    indexSchema.setIsPrimaryKey(true);
    indexSchema.setName("_primarykey");
    indexSchema.setComparators(tableSchema.getComparators(new String[]{"field1"}));

    TableSchema.Partition[] partitions = new TableSchema.Partition[partitionCount];
    for (int i = 0; i < partitionCount; i++) {
      partitions[i] = new TableSchema.Partition();
      partitions[i].setUnboundUpper(true);
    }
    indexSchema.setCurrPartitions(partitions);
    tableSchema.addIndex(indexSchema);
    return indexSchema;
  }


  public static TableSchema createTable() {
    TableSchema tableSchema = new TableSchema();
    tableSchema.setName("table1");
    tableSchema.setTableId(100);
    List<FieldSchema> fields = new ArrayList<>();
    FieldSchema fSchema = new FieldSchema();
    fSchema.setName("_id");
    fSchema.setType(DataType.Type.BIGINT);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field1");
    fSchema.setType(DataType.Type.BIGINT);
    fields.add(fSchema);
    tableSchema.setFields(fields);
    List<String> primaryKey = new ArrayList<>();
    primaryKey.add("field1");
    tableSchema.setPrimaryKey(primaryKey);
    return tableSchema;
  }

}
