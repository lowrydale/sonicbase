/* © 2019 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.accept.server;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.ComObject;
import com.sonicbase.common.Config;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.server.DatabaseServer;
import com.sonicbase.server.NettyServer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestPartitionedIndex {
//
  private Connection conn;
//  private final int recordCount = 10;
//  private DatabaseServer[] dbServers;
  private DatabaseClient client;
//
//  @AfterClass(alwaysRun = true)
//  public void afterClass() throws SQLException {
//    conn.close();
//    for (DatabaseServer server : dbServers) {
//      server.shutdown();
//    }
//    System.out.println("client refCount=" + DatabaseClient.clientRefCount.get() + ", sharedClients=" + DatabaseClient.sharedClients.size() + ", class=TestBatch");
//    for (DatabaseClient client : DatabaseClient.allClients) {
//      System.out.println("Stack:\n" + client.getAllocatedStack());
//    }
//  }

  private Connection connA;
  private Connection connB;
  private DatabaseClient clientA;
  private DatabaseClient clientB;
  private com.sonicbase.server.DatabaseServer[] dbServers;
  NettyServer serverA1;
  NettyServer serverA2;
  NettyServer serverB1;
  NettyServer serverB2;
  private Set<Long> ids;

  @AfterClass(alwaysRun = true)
  public void afterClass() throws SQLException {
    conn.close();
    //connB.close();

    for (com.sonicbase.server.DatabaseServer server : dbServers) {
      server.shutdown();
    }
    serverA1.shutdown();
    serverA2.shutdown();
    serverB1.shutdown();
    serverB2.shutdown();

    System.out.println("client refCount=" + DatabaseClient.clientRefCount.get());
    for (DatabaseClient client : DatabaseClient.allClients) {
      System.out.println("Stack:\n" + client.getAllocatedStack());
    }

  }

  @BeforeClass
  public void beforeClass() throws Exception {
    System.setProperty("log4j.configuration", "test-log4j.xml");

    FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db-data"));

    DatabaseClient.getServers().clear();

    dbServers = new com.sonicbase.server.DatabaseServer[4];

    String role = "primaryMaster";

    Config.copyConfig("4-shards");

    final CountDownLatch latch = new CountDownLatch(4);
    serverA1 = new NettyServer(128);
    Thread thread = new Thread(new Runnable(){
      @Override
      public void run() {
        serverA1.startServer(new String[]{"-port", String.valueOf(9010), "-host", "localhost",
            "-mport", String.valueOf(9010), "-mhost", "localhost", "-shard",
            String.valueOf(0)});
        latch.countDown();
      }
    });
    thread.start();
    while (true) {
      if (serverA1.isRunning()) {
        break;
      }
      Thread.sleep(100);
    }

    serverA2 = new NettyServer(128);
    thread = new Thread(new Runnable(){
      @Override
      public void run() {
        serverA2.startServer(new String[]{"-port", String.valueOf(9060), "-host", "localhost",
            "-mport", String.valueOf(9060), "-mhost", "localhost", "-shard", String.valueOf(1)});
        latch.countDown();
      }
    });
    thread.start();

    while (true) {
      if (serverA2.isRunning()) {
        break;
      }
      Thread.sleep(100);
    }

    serverB1 = new NettyServer(128);
    thread = new Thread(new Runnable(){
      @Override
      public void run() {
        serverB1.startServer(new String[]{"-port", String.valueOf(9110), "-host", "localhost",
            "-mport", String.valueOf(9110), "-mhost", "localhost", "-shard", String.valueOf(0)});
        latch.countDown();
      }
    });
    thread.start();
    while (true) {
      if (serverB1.isRunning()) {
        break;
      }
      Thread.sleep(100);
    }

    serverB2 = new NettyServer(128);
    thread = new Thread(new Runnable(){
      @Override
      public void run() {
        serverB2.startServer(new String[]{"-port", String.valueOf(9160), "-host", "localhost",
            "-mport", String.valueOf(9160), "-mhost", "localhost", "-shard", String.valueOf(1)});
        latch.countDown();
      }
    });
    thread.start();
    while (true) {
      if (serverB2.isRunning()) {
        break;
      }
      Thread.sleep(100);
    }

    while (true) {
      if (serverA1.isRecovered() && serverA2.isRecovered() && serverB1.isRecovered() && serverB2.isRecovered()) {
        break;
      }
      Thread.sleep(100);
    }

    dbServers[0] = serverA1.getDatabaseServer();
    dbServers[1] = serverA2.getDatabaseServer();
    dbServers[2] = serverB1.getDatabaseServer();
    dbServers[3] = serverB2.getDatabaseServer();
//
//    System.setProperty("log4j.configuration", "test-log4j.xml");
//
//    String configStr = IOUtils.toString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-4-shards.yaml")), "utf-8");
//    Config config = new Config(configStr);
//
//    FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db-data"));
//
//    //DatabaseServer.getAddressMap().clear();
//    DatabaseClient.getServers().clear();
//
//    dbServers = new DatabaseServer[4];
//    ThreadPoolExecutor executor = new ThreadPoolExecutor(32, 32, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
//
//    String role = "primaryMaster";
//
//    List<Future> futures = new ArrayList<>();
//    for (int i = 0; i < dbServers.length; i++) {
//      dbServers[i] = new DatabaseServer();
//      dbServers[i].setConfig(config, "4-shards", "localhost", 9010 + (50 * i), true, new AtomicBoolean(true), new AtomicBoolean(true),null, false);
//      dbServers[i].setRole(role);
//    }
//    for (Future future : futures) {
//      future.get();
//    }

    for (DatabaseServer server : dbServers) {
      server.shutdownRepartitioner();
    }

    Class.forName("com.sonicbase.jdbcdriver.Driver");

    conn = DriverManager.getConnection("jdbc:sonicbase:localhost:9010", "user", "password");

    client = ((ConnectionProxy)conn).getDatabaseClient();
    client.createDatabase("test");

    conn.close();

    conn = DriverManager.getConnection("jdbc:sonicbase:localhost:9010/test", "user", "password");

    client = ((ConnectionProxy)conn).getDatabaseClient();


    PreparedStatement stmt = conn.prepareStatement("create table Persons (id1 BIGINT, id2 BIGINT, PRIMARY KEY (id1))");
    stmt.executeUpdate();

    ids = new HashSet<>();

    Random rand = new Random();
    for (int i = 0; i < 400_000; i++) {
      stmt = conn.prepareStatement("insert into persons (id1, id2) VALUES (?, ?)");
      long id = 0;
      while (true) {
        id = (long) Math.abs(rand.nextLong());
        if (ids.add(id)) {
          break;
        }
      }
      stmt.setLong(1, id);//9223174091577576305L + i);
      stmt.setLong(2, (i + 100) % 2);
      assertEquals(stmt.executeUpdate(), 1);
      if (i % 10_000 == 0) {
        System.out.println("count=" + i);
      }
      if (i == 100_000) {
        client.beginRebalance("test");
      }
    }


    while (true) {
      if (client.isRepartitioningComplete("test")) {
        break;
      }
      Thread.sleep(1_000);
    }

    client.beginRebalance("test");

    while (true) {
      if (client.isRepartitioningComplete("test")) {
        break;
      }
      Thread.sleep(1_000);
    }

    client.beginRebalance("test");

    while (true) {
      if (client.isRepartitioningComplete("test")) {
        break;
      }
      Thread.sleep(1_000);
    }

    for (DatabaseServer server : dbServers) {
      server.shutdownRepartitioner();
    }

//    for (DatabaseServer server : dbServers) {
//      server.getDeleteManager().forceDeletes(null, false);
//    }

    Thread.sleep(5_000);

    //executor.shutdownNow();
  }

  @Test
  public void testTraverse() throws SQLException {

    PreparedStatement stmt = conn.prepareStatement("select count(*) from persons");
    ResultSet rs = stmt.executeQuery();
    assertEquals(rs.getLong(1), 400_000);

    stmt = conn.prepareStatement("select * from persons where id1 >= 0");
    rs = stmt.executeQuery();

    List<Long> kept = new ArrayList<>();
    int countReturned = 0;
    long last = Long.MIN_VALUE;
    while (rs.next()) {
      long curr = rs.getLong("id1");
      kept.add(curr);
      ids.remove(curr);
      assertTrue(last < curr, "currId=" + curr + ", last=" + last);
      countReturned++;
      if (countReturned % 10 == 0) {
        System.out.println(countReturned);
      }
    }

    List<Long> sorted = new ArrayList<>(ids);
    sorted.sort(Long::compare);
    kept.sort(Long::compare);

    System.out.println("kept: lower");
    for (int i = 0; i < 10; i++) {
      System.out.println("id="  + kept.get(i));
    }

    System.out.println("kept: upper");
    for (int i = 0; i < 10; i++) {
      System.out.println("id="  + kept.get(kept.size() - i - 1));
    }

    System.out.println("lost: lower");
    for (int i = 0; i < Math.min(sorted.size(), 10); i++) {
      System.out.println("id="  + sorted.get(i));
    }

    System.out.println("lost: upper");
    for (int i = 0; i < Math.min(10, sorted.size()); i++) {
      System.out.println("id="  + sorted.get(sorted.size() - i - 1));
    }

    for (int i = 0; i < dbServers.length; i++) {
      ComObject cobj = new ComObject(3);
      cobj.put(ComObject.Tag.DB_NAME, "test");
      cobj.put(ComObject.Tag.TABLE_NAME, "persons");
      cobj.put(ComObject.Tag.INDEX_NAME, "_primarykey");
      ComObject ret = dbServers[i].getReadManager().traverseIndex(cobj, false);
      System.out.println("shard=" + i + ", count=" + ret.getInt(ComObject.Tag.COUNT) +
          ", deleteCount=" + ret.getLong(ComObject.Tag.DELETE_COUNT) +
          ", addCount=" + ret.getLong(ComObject.Tag.ADD_COUNT));
    }



    assertEquals(countReturned, 400_000);
  }

}
