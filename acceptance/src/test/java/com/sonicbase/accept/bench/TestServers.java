package com.sonicbase.accept.bench;

import com.sonicbase.server.NettyServer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class TestServers {

  public static void main(String[] args) throws Exception {
    //FileUtils.deleteDirectory(new File("/data/database"));

    final NettyServer[] dbServers = new NettyServer[4];
    for (int shard = 0; shard < dbServers.length; shard++) {
      dbServers[shard] = new NettyServer();
    }

    final ThreadPoolExecutor executor = new ThreadPoolExecutor(32, 32, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
    final ThreadPoolExecutor selectExecutor = new ThreadPoolExecutor(256, 256, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

    executor.submit((Callable) () -> {
      try {
        dbServers[0].startServer(new String[]{"-port", String.valueOf(9010 + (50 * 0)), "-host", "localhost",
            "-mport", String.valueOf(9010), "-mhost", "localhost", "-cluster", "4-bench", "-shard", String.valueOf(0)});
        //dbServers[0].getDatabaseServer().shutdownRepartitioner();
      }
      catch (Exception e) {
        e.printStackTrace();
      }
      return null;
    });
    while (!dbServers[0].isRunning()) {
      Thread.sleep(1000);
    }
    Thread.sleep(10000);

    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < dbServers.length; i++) {
      final int shard = i;
      futures.add(executor.submit((Callable) () -> {
        if (shard == 0) {
          return null;
        }
        try {
          dbServers[shard].startServer(new String[]{"-port", String.valueOf(9010 + (50 * shard)), "-host", "localhost",
              "-mport", String.valueOf(9010), "-mhost", "localhost",
              "-shard", String.valueOf(shard), "-cluster", "4-bench"});
          //dbServers[shard].getDatabaseServer().shutdownRepartitioner();
        }
        catch (Exception e) {
          e.printStackTrace();
        }
        //          String role = "primaryMaster";
        //          dbServers[shard] = new DatabaseServer();
        //          dbServers[shard].setConfig(config, Integer.valueOf(shard));
        //          dbServers[shard].setRole(role);
        //          dbServers[shard].disableLogProcessor();
        return null;
      }));
    }
        for (Future future : futures) {
          future.get();
        }
    for (NettyServer server : dbServers) {
      while (!server.isRunning()) {
        Thread.sleep(1000);
      }
    }

    while (true) {
      Thread.sleep(1000);
    }
  }

}
