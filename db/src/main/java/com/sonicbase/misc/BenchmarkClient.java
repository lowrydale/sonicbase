package com.sonicbase.misc;

import java.sql.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class BenchmarkClient {

  public static void main(final String[] args) throws ClassNotFoundException, SQLException, InterruptedException {
    // Register JDBC driver.
    Class.forName("org.apache.ignite.IgniteJdbcThinDriver");
    Class.forName("com.sonicbase.jdbcdriver.Driver");

    try {
// Open JDBC connection.
      Connection conn = null;
      if (args[0].equals("sonicbase")) {
        conn = DriverManager.getConnection("jdbc:sonicbase:" + args[2] + ":9010/db");
      }
      else {
        conn = DriverManager.getConnection("jdbc:ignite:thin://" + args[2] + "/");
      }

// Create database tables.
      try (Statement stmt = conn.createStatement()) {

        if (args[1].equals("schema")) {
          PreparedStatement stmt2 = conn.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, id3 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
          stmt2.executeUpdate();
        }
      }

      final AtomicLong count = new AtomicLong();
      final AtomicLong offset = new AtomicLong();
      final long begin = System.currentTimeMillis();
      final CountDownLatch latch = new CountDownLatch(16);

      int threadCount = 16;
      if (args[1].equalsIgnoreCase("range")) {
        threadCount = 32;
      }
      else if (args[1].equalsIgnoreCase("identity")) {
        threadCount = 64;
      }
      for (int y = 0; y < threadCount; y++) {
        Thread thread = new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              Connection conn = null;
              if (args[0].equals("sonicbase")) {
                conn = DriverManager.getConnection("jdbc:sonicbase:" + args[2] + ":9010/db");
              }
              else {
                conn = DriverManager.getConnection("jdbc:ignite:thin://" + args[2] + "/");
              }
              int max = 100 * 5_000 / 16;
              if (args[1].equalsIgnoreCase("range")) {
                max = 1_000_000_000;
              }
              for (int i = 0; i < max; i++) {
                try {
                  if (args[1].equalsIgnoreCase("insert")) {
                    PreparedStatement stmt = conn.prepareStatement("insert into persons (id, id2, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?)");
                    for (int j = 0; j < 200; j++) {
                      long currOffset = offset.incrementAndGet();

                      stmt.setLong(1, currOffset + 100 + 10_000_000);
                      stmt.setLong(2, (currOffset + 100) % 2);
                      stmt.setString(3, "933-28-" + (currOffset % 4));
                      stmt.setString(4, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
                      stmt.setBoolean(5, false);
                      stmt.setString(6, "m");
                      stmt.addBatch();
                      offset.incrementAndGet();

                      if (count.incrementAndGet() % 100_000 == 0) {
                        System.out.println("count=" + count.get() + ", duration=" + (System.currentTimeMillis() - begin) + ", rate=" + (double) count.get() / (double) (System.currentTimeMillis() - begin) * 1000f);
                      }
                    }
                    stmt.executeBatch();
                    stmt.close();
                  }
                  else if (args[1].equalsIgnoreCase("range")) {
                    PreparedStatement stmt = conn.prepareStatement("select * from persons where id > 0");
                    ResultSet rs = stmt.executeQuery();
                    while (rs.next()) {
                      long id = rs.getLong("id");

                      if (count.incrementAndGet() % 100_000 == 0) {
                        System.out.println("count=" + count.get() + ", duration=" + (System.currentTimeMillis() - begin) + ", rate=" + (double) count.get() / (double) (System.currentTimeMillis() - begin) * 1000f);
                      }
                    }
                  }
                  else if (args[1].equalsIgnoreCase("identity")) {
                    for (int x = 0; x < 100_000; x++) {
                      PreparedStatement stmt = conn.prepareStatement("select * from persons where id = " + i);
                      ResultSet rs = stmt.executeQuery();
                      if (rs.next()) {
                        long id = rs.getLong("id");

                        if (count.incrementAndGet() % 10_000 == 0) {
                          System.out.println("count=" + count.get() + ", duration=" + (System.currentTimeMillis() - begin) + ", rate=" + (double) count.get() / (double) (System.currentTimeMillis() - begin) * 1000f);
                        }
                      }
                    }
                  }
                }
                catch (Exception e) {
                  e.printStackTrace();
                }
              }
              latch.countDown();
            }
            catch (Exception e) {
              e.printStackTrace();
            }
          }
        });
        thread.start();
      }
      latch.await();

    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }
}
