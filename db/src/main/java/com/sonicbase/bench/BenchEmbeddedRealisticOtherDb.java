/* © 2019 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.bench;

import com.sonicbase.embedded.EmbeddedDatabase;

import java.sql.*;
import java.util.concurrent.atomic.AtomicLong;

public class BenchEmbeddedRealisticOtherDb {

  public static void main(String[] args) throws SQLException, InterruptedException, ClassNotFoundException {
    int count = Integer.parseInt(args[0]);

    Class.forName("org.hsqldb.jdbcDriver" );
    Connection embeddedConn = DriverManager.getConnection("jdbc:hsqldb:testdb", "sa", "");

    try (PreparedStatement stmt = embeddedConn.prepareStatement("drop table persons")) {
      stmt.executeUpdate();
    }

    try (PreparedStatement stmt = embeddedConn.prepareStatement("create table persons (ssn VARCHAR(20), given VARCHAR(20), surname VARCHAR(30), gender VARCHAR(10), birth DATE, PRIMARY KEY (ssn))")) {
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
            try (PreparedStatement stmt1 = embeddedConn.prepareStatement("insert into persons (ssn, given, surname, gender, birth) values(?, ?, ?, ?, ?)")) {
              for (int k = 0; k < 500; k++) {
                stmt1.setString(1, String.valueOf(j + 10_000_000 + k));
                stmt1.setString(2, String.valueOf(j + 100_000 + k));
                stmt1.setString(3, String.valueOf(j + 1_000_000 + k));
                stmt1.setString(4, "male");
                stmt1.setDate(5, new Date(System.currentTimeMillis()));;
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
          try (PreparedStatement stmt = embeddedConn.prepareStatement("select * from persons where SSN >= '0'")) {
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
            StringBuilder builder = new StringBuilder("select * from persons where persons.ssn in (");
            for (int k = 0; k < 3200; k++) {
              if (k != 0) {
                builder.append(",");
              }
              builder.append("?");
            }
            builder.append(")");;

            try (PreparedStatement stmt = embeddedConn.prepareStatement(builder.toString())) {
              for (int k = 0; k < 3200; k++) {
                stmt.setString(k + 1, String.valueOf(k + 10_000_000));
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
  }
}
