/* © 2019 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.bench;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.embedded.EmbeddedDatabase;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.sonicbase.index.AddressMap.unsafe;

public class BenchEmbedded {

  private static boolean skip = false;
  private static ConcurrentSkipListMap<Long, Long> map = new ConcurrentSkipListMap<>();

  public static void main(String[] args) throws SQLException, InterruptedException {
    int threadCount = Integer.parseInt(args[0]);
    int tableCount = Integer.parseInt(args[1]);
    long insertCount = Long.parseLong(args[2]);
    long deleteCount = Long.parseLong(args[3]);
    long readCount = Long.parseLong(args[4]);

    Date date = new Date(1365994377928L);
    System.out.println("year=" + (date.getYear() + 1900) + ", month=" + date.getMonth() + ", day=" + date.getDay());


    int insertThreadCount = threadCount;//Runtime.getRuntime().availableProcessors();//8;
    int rangeThreadCount = threadCount;//Runtime.getRuntime().availableProcessors();
    int identityThreadCount = threadCount;//Runtime.getRuntime().availableProcessors();

//    List<Long> allocations = new ArrayList<>();
//    for (int i = 0; i < 3_000_000; i++) {
//      allocations.add(unsafe.allocateMemory(1_000));
//    }
//    System.out.println("finished allocating");
//    System.out.flush();
//
//    Thread.sleep(10_000);
//    for (long address : allocations) {
//      unsafe.freeMemory(address);
//    }
//    System.gc();
//    System.out.println("finished freeing");
//    System.out.flush();
//
//    Thread.sleep(10_000);
//
//    allocations.clear();
//    for (int i = 0; i < 3_000_000; i++) {
//      allocations.add(unsafe.allocateMemory(1_000));
//    }
//    System.out.println("finished allocating");
//    System.out.flush();
//
//    Thread.sleep(10_000);
//    for (long address : allocations) {
//      unsafe.freeMemory(address);
//    }
//    System.gc();
//    System.out.println("finished freeing");
//    System.out.flush();
//
//
//    Thread.sleep(100_000);

    EmbeddedDatabase embedded = new EmbeddedDatabase();

    //embedded.enableDurability(System.getProperty("user.home") + "/db-data.embedded");
    embedded.disableDurability();
    //embedded.purge();
    embedded.start();
    embedded.createDatabaseIfNeeded("test");
    Connection embeddedConn = embedded.getConnection("test");

    String[] tables = new String[tableCount];
    for (int i = 0; i < tableCount; i++) {
      tables[i] = "table" + i;
      try (PreparedStatement stmt = embeddedConn.prepareStatement("create table " + tables[i] + " (id BIGINT, id2 BIGINT, PRIMARY KEY (id))")) {
        stmt.executeUpdate();
      }
    }

    doInserts(tableCount, insertCount, insertThreadCount, embeddedConn, tables);

    //doSimpleGets(tableCount, deleteCount, insertThreadCount, embeddedConn, tables);

    for (int i = 0; i < 1; i++){
      doDeletes(tableCount, deleteCount, insertThreadCount, embeddedConn, tables);

      System.gc();

      doInserts(tableCount, deleteCount, insertThreadCount, embeddedConn, tables);
    }
    final AtomicLong totalDuration = new AtomicLong();

    totalDuration.set(0);
    final long rangeBegin = System.currentTimeMillis();
    final AtomicLong rangeCount = new AtomicLong();
    final AtomicLong tableOffset = new AtomicLong();
    final Thread[] rangeThreads = new Thread[rangeThreadCount];
    for (int i = 0; i < rangeThreads.length; i++) {
      rangeThreads[i] = new Thread(() -> {
        try {
          while (true) {
            int currTableOffset = (int) (tableOffset.incrementAndGet() % tableCount);
            try (PreparedStatement stmt = embeddedConn.prepareStatement("select * from " + tables[currTableOffset] + " where id >= 0")) {
              long currBegin = System.nanoTime();
              ResultSet rs = stmt.executeQuery();
              for (int k = 0; k < 10_000_000 && rs.next(); k++) {
                if (rangeCount.incrementAndGet() % 100_000 == 0) {
                  System.out.println("range progress: count=" + rangeCount.get() +
                      ", rate=" + ((double) rangeCount.get() / (double) (System.currentTimeMillis() - rangeBegin) * 1000f) +
                      ", avgDuration=" + ((double) totalDuration.get() / (double) rangeCount.get()));
                }
                if (rangeCount.get() > readCount) {
                  return;
                }
              }
              totalDuration.addAndGet(System.nanoTime() - currBegin);
            }
          }
        }
        catch (Exception e) {
          e.printStackTrace();
        }
      });
      rangeThreads[i].start();
    }
    for (Thread thread : rangeThreads) {
      thread.join();
    }
    System.out.println("finished range");

    totalDuration.set(0);
    final long identityBegin = System.currentTimeMillis();
    final AtomicLong identityCount = new AtomicLong();
    tableOffset.set(0);
    final Thread[] identityThreads = new Thread[identityThreadCount];
    for (int i = 0; i < identityThreads.length; i++) {
      identityThreads[i] = new Thread(() -> {
        try {
          while (true) {
            int currTableOffset = (int) (tableOffset.incrementAndGet() % tableCount);
            StringBuilder builder = new StringBuilder("select id from " + tables[currTableOffset] + " where id in (");
            for (int k = 0; k < 3200; k++) {
              if (k != 0) {
                builder.append(",");
              }
              builder.append("?");
            }
            builder.append(")");;

            try (PreparedStatement stmt = embeddedConn.prepareStatement(builder.toString())) {
              for (int k = 0; k < 3200; k++) {
                stmt.setLong(k + 1, k);
              }
              long currBegin = System.nanoTime();
              ResultSet rs = stmt.executeQuery();
              while (rs.next()) {
                if (identityCount.incrementAndGet() % 100_000 == 0) {
                  System.out.println("identity progress: count=" + identityCount.get() +
                      ", rate=" + ((double) identityCount.get() / (double) (System.currentTimeMillis() - identityBegin) * 1000f) +
                      ", avgDuration=" + ((double)totalDuration.get() / (double)identityCount.get()));
                }
                if (identityCount.get() > readCount) {
                  return;
                }
              }
              totalDuration.addAndGet(System.nanoTime() - currBegin);
            }
          }
        }
        catch (Exception e) {
          e.printStackTrace();
        }
      });
      identityThreads[i].start();
    }
    for (Thread thread : identityThreads) {
      thread.join();
    }
    System.out.println("finished identity");


    embeddedConn.close();

    embedded.shutdown();
  }

  private static void doDeletes(int tableCount, long count, int insertThreadCount, Connection embeddedConn, String[] tables) throws InterruptedException {
    final AtomicLong totalDuration = new AtomicLong();
    final AtomicLong tableOffset = new AtomicLong();
    totalDuration.set(0);
    final long identityBegin = System.currentTimeMillis();
    final AtomicLong identityCount = new AtomicLong();
    tableOffset.set(0);
    final Thread[] identityThreads = new Thread[insertThreadCount];
    for (int i = 0; i < identityThreads.length; i++) {
      identityThreads[i] = new Thread(() -> {
        try {
          while (true) {
            if (skip) {
              for (int k = 0; k < 1000; k++) {
                long id = identityCount.getAndIncrement();
                map.remove(id);;
                if (id % 100_000 == 0) {
                  System.out.println("delete progress: count=" + identityCount.get() +
                      ", rate=" + ((double) identityCount.get() / (double) (System.currentTimeMillis() - identityBegin) * 1000f) +
                      ", avgDuration=" + ((double) totalDuration.get() / (double) identityCount.get()));
                }
              }
              long currBegin = System.nanoTime();
              if (identityCount.get() > count) {
                return;
              }

              totalDuration.addAndGet(System.nanoTime() - currBegin);
            }
            else {
              int currTableOffset = (int) (tableOffset.incrementAndGet() % tableCount);
              StringBuilder builder = new StringBuilder("delete from " + tables[currTableOffset] + " where id in (");
              for (int k = 0; k < 1000; k++) {
                if (k != 0) {
                  builder.append(",");
                }
                builder.append("?");
              }
              builder.append(")");
              ;

              try (PreparedStatement stmt = embeddedConn.prepareStatement(builder.toString())) {
                for (int k = 0; k < 1000; k++) {
                  long id = identityCount.getAndIncrement();
                  stmt.setLong(k + 1, id);
                  if (id % 100_000 == 0) {
                    System.out.println("delete progress: count=" + identityCount.get() +
                        ", rate=" + ((double) identityCount.get() / (double) (System.currentTimeMillis() - identityBegin) * 1000f) +
                        ", avgDuration=" + ((double) totalDuration.get() / (double) identityCount.get()));
                  }
                }
                long currBegin = System.nanoTime();
                stmt.executeUpdate();
                if (identityCount.get() > count) {
                  return;
                }

                totalDuration.addAndGet(System.nanoTime() - currBegin);
              }
            }
          }
        }
        catch (Exception e) {
          e.printStackTrace();
        }
      });
      identityThreads[i].start();
    }
    for (Thread thread : identityThreads) {
      thread.join();
    }
    System.out.println("finished deletes");

//    final long deleteBegin = System.currentTimeMillis();
//    final AtomicLong deletedCount = new AtomicLong();
//    final AtomicLong totalDuration = new AtomicLong();
//    Thread[] threads = new Thread[insertThreadCount];
//    for (int i = 0; i < threads.length; i++) {
//      final int offset = i;
//      threads[i] = new Thread(() -> {
//        try {
//          AtomicLong[] tableOffsets = new AtomicLong[tableCount];
//          for (int j = 0; j < tableCount; j++) {
//            tableOffsets[j] = new AtomicLong();
//          }
//
//          AtomicLong deletedCurr = new AtomicLong();
//          long startOffset = offset * count / threads.length;
//          for (long j = startOffset; j < (offset + 1) * count / threads.length; j += 500) {
//            long currBegin = System.nanoTime();
//            long currOffset = deletedCurr.getAndIncrement();
//            int tableOffset = (int) (currOffset % tableCount);
//            for (int k = 0; k < 500; k++) {
//              try (PreparedStatement stmt1 = embeddedConn.prepareStatement("delete from " + tables[tableOffset] + " where id = ?")) {
//                stmt1.setLong(1, startOffset + tableOffsets[tableOffset].getAndIncrement());
//                stmt1.executeUpdate();
//                if (deletedCount.incrementAndGet() % 100_000 == 0) {
//                  System.out.println("delete progress: count=" + deletedCount.get() +
//                      ", rate=" + ((double)deletedCount.get() / (double)(System.currentTimeMillis() - deleteBegin) * 1000f) +
//                      ", avgDuration=" + ((double)totalDuration.get() / (double)deletedCount.get()));
//                }
//              }
//              totalDuration.addAndGet(System.nanoTime() - currBegin);
//            }
//          }
//        }
//        catch (Exception e) {
//          e.printStackTrace();
//        }
//      });
//      threads[i].start();
//    }
//    for (Thread thread : threads) {
//      thread.join();
//    }
//    System.out.println("finished deletes");
  }

  private static void doSimpleGets(int tableCount, long count, int insertThreadCount, Connection embeddedConn, String[] tables) throws InterruptedException {
    final long deleteBegin = System.currentTimeMillis();
    final AtomicLong deletedCount = new AtomicLong();
    final AtomicLong totalDuration = new AtomicLong();
    Thread[] threads = new Thread[insertThreadCount];
    for (int i = 0; i < threads.length; i++) {
      final int offset = i;
      threads[i] = new Thread(() -> {
        try {
          AtomicLong[] tableOffsets = new AtomicLong[tableCount];
          for (int j = 0; j < tableCount; j++) {
            tableOffsets[j] = new AtomicLong();
          }

          AtomicLong deletedCurr = new AtomicLong();
          long startOffset = offset * count / threads.length;
          for (long j = startOffset; j < (offset + 1) * count / threads.length; j += 500) {
            long currBegin = System.nanoTime();
            long currOffset = deletedCurr.getAndIncrement();
            int tableOffset = (int) (currOffset % tableCount);
            for (int k = 0; k < 500; k++) {
              try (PreparedStatement stmt1 = embeddedConn.prepareStatement("select * from " + tables[tableOffset] + " where id = ?")) {
                stmt1.setLong(1, startOffset + tableOffsets[tableOffset].getAndIncrement());
                stmt1.executeQuery();
                if (deletedCount.incrementAndGet() % 100_000 == 0) {
                  System.out.println("get progress: count=" + deletedCount.get() +
                      ", rate=" + ((double)deletedCount.get() / (double)(System.currentTimeMillis() - deleteBegin) * 1000f) +
                      ", avgDuration=" + ((double)totalDuration.get() / (double)deletedCount.get()));
                }
              }
              totalDuration.addAndGet(System.nanoTime() - currBegin);
            }
          }
        }
        catch (Exception e) {
          e.printStackTrace();
        }
      });
      threads[i].start();
    }
    for (Thread thread : threads) {
      thread.join();
    }
    System.out.println("finished gets");
  }

  @NotNull
  private static void doInserts(int tableCount, long count, int insertThreadCount, Connection embeddedConn, String[] tables) throws InterruptedException {
    final long insertBegin = System.currentTimeMillis();
    final AtomicLong insertedCount = new AtomicLong();
    final AtomicLong totalDuration = new AtomicLong();
    Thread[] threads = new Thread[insertThreadCount];
    for (int i = 0; i < threads.length; i++) {
      final int offset = i;
      threads[i] = new Thread(() -> {
        try {
          AtomicLong[] tableOffsets = new AtomicLong[tableCount];
          for (int j = 0; j < tableCount; j++) {
            tableOffsets[j] = new AtomicLong();
          }

          AtomicLong insertedCurr = new AtomicLong();
          long startOffset = offset * count / threads.length;
          for (long j = startOffset; j < (offset + 1) * count / threads.length; j += 500) {
            long currBegin = System.nanoTime();
            long currOffset = insertedCurr.getAndIncrement();
            if (skip) {
              int tableOffset = (int) (currOffset % tableCount);
              for (int k = 0; k < 500; k++) {
                long key = startOffset + tableOffsets[tableOffset].getAndIncrement();
                map.put(key, key);

                if (insertedCount.incrementAndGet() % 100_000 == 0) {
                  System.out.println("insert progress: count=" + insertedCount.get() +
                      ", rate=" + ((double) insertedCount.get() / (double) (System.currentTimeMillis() - insertBegin) * 1000f) +
                      ", avgDuration=" + ((double) totalDuration.get() / (double) insertedCount.get()));
                }
              }
            }
            else {
              int tableOffset = (int) (currOffset % tableCount);
              try (PreparedStatement stmt1 = embeddedConn.prepareStatement("insert into " + tables[tableOffset] + " (id) values(?)")) {
                for (int k = 0; k < 500; k++) {
                  stmt1.setLong(1, startOffset + tableOffsets[tableOffset].getAndIncrement());
                  stmt1.addBatch();

                  if (insertedCount.incrementAndGet() % 100_000 == 0) {
                    System.out.println("insert progress: count=" + insertedCount.get() +
                        ", rate=" + ((double) insertedCount.get() / (double) (System.currentTimeMillis() - insertBegin) * 1000f) +
                        ", avgDuration=" + ((double) totalDuration.get() / (double) insertedCount.get()));
                  }
                }
                stmt1.executeBatch();
              }
              totalDuration.addAndGet(System.nanoTime() - currBegin);
            }
          }
        }
        catch (Exception e) {
          e.printStackTrace();
        }
      });
      threads[i].start();
    }
    for (Thread thread : threads) {
      thread.join();
    }
    System.out.println("finished load");
  }
}
