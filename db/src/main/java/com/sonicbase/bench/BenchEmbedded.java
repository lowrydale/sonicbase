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
    int count = Integer.parseInt(args[0]);

    EmbeddedDatabase embedded = new EmbeddedDatabase();

    //embedded.enableDurability(System.getProperty("user.home") + "/db-data.embedded");
    embedded.disableDurability();
    //embedded.purge();
    embedded.start();
    embedded.createDatabaseIfNeeded("test");
    Connection embeddedConn = embedded.getConnection("test");

    try (PreparedStatement stmt = embeddedConn.prepareStatement("create table persons (id BIGINT, id2 BIGINT, PRIMARY KEY (id))")) {
      stmt.executeUpdate();
    }

    final long insertBegin = System.currentTimeMillis();
    final AtomicLong insertedCount = new AtomicLong();
    Thread[] threads = new Thread[8];
    for (int i = 0; i < threads.length; i++) {
      final int offset = i;
      threads[i] = new Thread(() -> {
        try {
          for (int j = offset * count / threads.length; j < (offset + 1) * count / threads.length; j += 500) {
            try (PreparedStatement stmt1 = embeddedConn.prepareStatement("insert into persons (id) values(?)")) {
              for (int k = 0; k < 500; k++) {
                stmt1.setLong(1, j + k);
                stmt1.addBatch();

                if (insertedCount.incrementAndGet() % 100_000 == 0) {
                  System.out.println("insert progress: count=" + insertedCount.get() + ", rate=" + ((double)insertedCount.get() / (double)(System.currentTimeMillis() - insertBegin) * 1000f));
                }
              }
              stmt1.executeBatch();
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

    final long rangeBegin = System.currentTimeMillis();
    final AtomicLong rangeCount = new AtomicLong();
    final Thread[] rangeThreads = new Thread[8];
    for (int i = 0; i < rangeThreads.length; i++) {
      rangeThreads[i] = new Thread(() -> {
        try {
          try (PreparedStatement stmt = embeddedConn.prepareStatement("select * from persons where id >= 0")) {
            ResultSet rs = stmt.executeQuery();
            for (int j = 0; j < count / 2 && rs.next(); j++) {
              if (rangeCount.incrementAndGet() % 100_000 == 0) {
                System.out.println("range progress: count=" + rangeCount.get() + ", rate=" + ((double) rangeCount.get() / (double) (System.currentTimeMillis() - rangeBegin) * 1000f));
              }
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

    final long identityBegin = System.currentTimeMillis();
    final AtomicLong identityCount = new AtomicLong();
    final Thread[] identityThreads = new Thread[8];
    for (int i = 0; i < identityThreads.length; i++) {
      identityThreads[i] = new Thread(() -> {
        try {
          for (int j = 0; j < 10_000; j++) {
            StringBuilder builder = new StringBuilder("select id from persons where persons.id in (");
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
              ResultSet rs = stmt.executeQuery();
              while (rs.next()) {
                if (identityCount.incrementAndGet() % 100_000 == 0) {
                  System.out.println("identity progress: count=" + identityCount.get() + ", rate=" + ((double) identityCount.get() / (double) (System.currentTimeMillis() - identityBegin) * 1000f));
                }
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
    System.out.println("finished identity");


    embeddedConn.close();

    embedded.shutdown();
  }
}
