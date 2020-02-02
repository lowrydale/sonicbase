/* © 2019 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.bench;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.embedded.EmbeddedDatabase;
import com.sonicbase.jdbcdriver.ConnectionProxy;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicLong;

public class BenchEmbedded {

  public static void main(String[] args) throws SQLException, InterruptedException {
    int threadCount = Integer.parseInt(args[0]);
    int tableCount = Integer.parseInt(args[1]);
    long count = Long.parseLong(args[2]);

    int insertThreadCount = threadCount;//Runtime.getRuntime().availableProcessors();//8;
    int rangeThreadCount = threadCount;//Runtime.getRuntime().availableProcessors();
    int identityThreadCount = threadCount;//Runtime.getRuntime().availableProcessors();

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
            int tableOffset = (int) (currOffset % tableCount);
            try (PreparedStatement stmt1 = embeddedConn.prepareStatement("insert into " + tables[tableOffset] + " (id) values(?)")) {
              for (int k = 0; k < 500; k++) {
                stmt1.setLong(1, startOffset + tableOffsets[tableOffset].getAndIncrement());
                stmt1.addBatch();

                if (insertedCount.incrementAndGet() % 100_000 == 0) {
                  System.out.println("insert progress: count=" + insertedCount.get() +
                      ", rate=" + ((double)insertedCount.get() / (double)(System.currentTimeMillis() - insertBegin) * 1000f) +
                      ", avgDuration=" + ((double)totalDuration.get() / (double)insertedCount.get()));
                }
              }
              stmt1.executeBatch();
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
                if (rangeCount.get() > 640_000_000) {
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
          for (int j = 0; j < 10_000; j++) {
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
}
