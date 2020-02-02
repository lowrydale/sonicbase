/* © 2019 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.bench;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.Config;
import com.sonicbase.common.PreparedIndexLookupNotFoundException;
import com.sonicbase.server.DatabaseServer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class SimpleJdbcBenchmark {

  private static Connection conn;

  public static void main(String[] args) throws SQLException, ClassNotFoundException, IOException, InterruptedException, ExecutionException {
    int threadCount = Integer.parseInt(args[0]);
    long recordCount = Long.parseLong(args[1]);
    String configStr = IOUtils.toString(new BufferedInputStream(SimpleJdbcBenchmark.class.getResourceAsStream("/config/config-1-local.yaml")), "utf-8");
    Config config = new Config(configStr);

    FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db-data"));

    final DatabaseServer[] dbServers = new DatabaseServer[1];
    ThreadPoolExecutor executor = new ThreadPoolExecutor(32, 32, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

    String role = "primaryMaster";

    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < dbServers.length; i++) {
      final int shard = i;
      futures.add(executor.submit((Callable) () -> {
        String role1 = "primaryMaster";

        dbServers[shard] = new DatabaseServer();
        Config.copyConfig("test");
        dbServers[shard].setConfig(config, "localhost", 9010 + (50 * shard), true, new AtomicBoolean(true), new AtomicBoolean(true), null, false);
        dbServers[shard].setRole(role1);
        return null;
      }));
    }
    for (Future future : futures) {
      future.get();
    }

    for (DatabaseServer server : dbServers) {
      server.shutdownRepartitioner();
    }

    Class.forName("com.sonicbase.jdbcdriver.Driver");

    DatabaseClient client = new DatabaseClient("localhost", 9010, -1, -1, true);

    client.createDatabase("db");

    Class.forName("org.postgresql.Driver");

    conn = DriverManager.getConnection("jdbc:postgresql://54.144.0.23:5432/test", "postgres", "postgres");

    //conn = DriverManager.getConnection("jdbc:sonicbase:localhost:9010/db", "user", "password");

    try (PreparedStatement stmt = conn.prepareStatement("drop table persons")) {
      stmt.executeUpdate();
    }
    catch (Exception e) {
      e.printStackTrace();
    }

    try {
      PreparedStatement stmt = conn.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
      stmt.executeUpdate();
    }
    catch (Exception e) {
      e.printStackTrace();
    }


    AtomicLong countInserted = new AtomicLong();
    final long begin = System.currentTimeMillis();
    Thread[] threads = new Thread[threadCount];
    for (int j = 0; j < threads.length; j++) {
      final int offset = j;
      threads[j] = new Thread(() -> {
        for (int i = 0; ; i += 5000) {
          try {
            try (PreparedStatement stmt1 = conn.prepareStatement("insert into persons (id) VALUES (?)")) {
              for (int k = 0; k < 5000; k++) {
                stmt1.setLong(1, (i + k) + offset * 10_000_000_000L);
                if (countInserted.incrementAndGet() % 100_000 == 0) {
                  System.out.println("insert progress: count=" + countInserted.get() + ", rate=" +
                      ((double) countInserted.get() / (System.currentTimeMillis() - begin) * 1000f));
                }

                stmt1.addBatch();
              }
              int[] batchRet = stmt1.executeBatch();

              if (countInserted.get() > recordCount) {
                return;
              }
            }
          }
          catch (Exception e) {
            e.printStackTrace();
          }
        }
      });
      threads[j].start();

    }

    for (Thread thread : threads) {
      thread.join();
     }

    client.beginRebalance("db");

    while (true) {
      if (client.isRepartitioningComplete("db")) {
        break;
      }
      Thread.sleep(1000);
    }

    AtomicLong countRead = new AtomicLong();
    final long begin2 = System.currentTimeMillis();
    threads = new Thread[threadCount];
    for (int j = 0; j < threads.length; j++) {
      threads[j] = new Thread(() -> {
        try {
          while (true) {
            PreparedStatement stmt2 = conn.prepareStatement("select persons.id from persons where persons.id>0");                                              //
            ResultSet ret = stmt2.executeQuery();


            while (ret.next()) {
              if (countRead.incrementAndGet() % 100_000 == 0) {
                System.out.println("read progress: count=" + countRead.get() + ", rate=" +
                    ((double) countRead.get() / (System.currentTimeMillis() - begin2) * 1000f));
              }
            }
          }
        }
        catch (Exception e) {
          e.printStackTrace();
        }
      });
    }
    for (int j = 0; j < threads.length; j++) {
      threads[j].start();
    }
    for (int j = 0; j < threads.length; j++) {
      threads[j].join();
    }
  }
}
