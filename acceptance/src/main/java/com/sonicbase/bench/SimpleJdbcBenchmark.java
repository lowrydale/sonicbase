/* © 2019 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.accept.bench;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.Config;
import com.sonicbase.server.DatabaseServer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class SimpleJdbcBenchmark {

  private static Connection conn;
  private static final int recordCount = 2_000_000;

  public static void main(String[] args) throws SQLException, ClassNotFoundException, IOException, InterruptedException, ExecutionException {
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
        dbServers[shard].setConfig(config, "test", "localhost", 9010 + (50 * shard), true, new AtomicBoolean(true), new AtomicBoolean(true), null, false);
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
    conn = DriverManager.getConnection("jdbc:sonicbase:localhost:9010/db", "user", "password");

    PreparedStatement stmt = conn.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
    stmt.executeUpdate();


    AtomicLong countInserted = new AtomicLong();
    long begin = System.currentTimeMillis();
    Thread[] threads = new Thread[8];
    for (int j = 0; j < threads.length; j++) {
      final int offset = j;
      threads[j] = new Thread(() -> {
        for (int i = 0; i < recordCount; i += 500) {
          try {
            PreparedStatement stmt1 = conn.prepareStatement("insert into persons (id) VALUES (?)");
            for (int k = 0; k < 500; k++) {
              stmt1.setLong(1, (i + 1) + offset * 10_000_000);
              if (countInserted.incrementAndGet() % 100_000 == 0) {
                System.out.println("insert progress: count=" + i + ", rate=" + ((double) i / (System.currentTimeMillis() - begin) * 1000f));
              }

              stmt1.addBatch();
            }
            int[] batchRet = stmt1.executeBatch();
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




  }
}
