package com.sonicbase.accept.server;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.Config;
import com.sonicbase.index.NativeSkipListMapImpl;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.testng.Assert.*;

public class TestLogManager {

  private Connection conn;
  DatabaseClient client = null;
  com.sonicbase.server.DatabaseServer[] dbServers;


  @AfterClass(alwaysRun = true)
  public void afterClass() throws SQLException {
    conn.close();

    for (com.sonicbase.server.DatabaseServer server : dbServers) {
      server.shutdown();
    }

    System.out.println("client refCount=" + DatabaseClient.clientRefCount.get() + ", class=TestLogManager");
    for (DatabaseClient client : DatabaseClient.allClients) {
      System.out.println("Stack:\n" + client.getAllocatedStack());
    }

  }

  @BeforeClass
  public void beforeClass() throws Exception {
    System.setProperty("log4j.configuration", "test-log4j.xml");


    String configStr = IOUtils.toString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-4-servers.yaml")), "utf-8");
    Config config = new Config(configStr);

    FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db-data"));

    DatabaseClient.getServers().clear();

    dbServers = new com.sonicbase.server.DatabaseServer[4];
    ThreadPoolExecutor executor = new ThreadPoolExecutor(32, 32, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

    String role = "primaryMaster";

    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < dbServers.length; i++) {
      //      futures.add(executor.submit(new Callable() {
      //        @Override
      //        public Object call() throws Exception {
      //          String role = "primaryMaster";

      dbServers[i] = new com.sonicbase.server.DatabaseServer();
      Config.copyConfig("4-servers");
      dbServers[i].setConfig(config, "localhost", 9010 + (50 * i), true, new AtomicBoolean(true), new AtomicBoolean(true),null, false);
      dbServers[i].setRole(role);

      //          return null;
      //        }
      //      }));
    }
    for (Future future : futures) {
      future.get();
    }

    com.sonicbase.server.DatabaseServer.initDeathOverride(2, 2);
    com.sonicbase.server.DatabaseServer.deathOverride[0][0] = false;
    com.sonicbase.server.DatabaseServer.deathOverride[0][1] = false;
    com.sonicbase.server.DatabaseServer.deathOverride[1][0] = false;
    com.sonicbase.server.DatabaseServer.deathOverride[1][1] = false;

    dbServers[0].enableSnapshot(false);
    dbServers[1].enableSnapshot(false);
    dbServers[2].enableSnapshot(false);
    dbServers[3].enableSnapshot(false);


    Thread.sleep(5000);

    //DatabaseClient client = new DatabaseClient("localhost", 9010, true);

    Class.forName("com.sonicbase.jdbcdriver.Driver");

    conn = DriverManager.getConnection("jdbc:sonicbase:localhost:9010", "user", "password");

    try {
      ((ConnectionProxy) conn).getDatabaseClient().createDatabase("test");
    }
    catch (Exception e) {
      e.printStackTrace();
    }

    conn.close();

    conn = DriverManager.getConnection("jdbc:sonicbase:localhost:9010/test", "user", "password");

    client = ((ConnectionProxy) conn).getDatabaseClient();


    PreparedStatement stmt = conn.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
    stmt.executeUpdate();

    futures = new ArrayList<>();
    for (int i = 0; i < 100_000; i++) {
      final int offset = i;
      futures.add(executor.submit((Callable) () -> {
        PreparedStatement stmt2 = conn.prepareStatement("insert into persons (id, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?)");
        stmt2.setLong(1, offset);
        stmt2.setString(2, "933-28-" + offset);
        stmt2.setString(3, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
        stmt2.setBoolean(4, false);
        stmt2.setString(5, "m");
        assertEquals(stmt2.executeUpdate(), 1);
        return null;
      }));
    }
    for (Future future : futures) {
      future.get();
    }

    for (com.sonicbase.server.DatabaseServer server : dbServers) {
      server.shutdownRepartitioner();
    }
    client.beginRebalance("test");


    while (true) {
      if (client.isRepartitioningComplete("test")) {
        break;
      }
      Thread.sleep(1000);
    }

    for (com.sonicbase.server.DatabaseServer server : dbServers) {
      server.shutdownRepartitioner();
    }
    client.beginRebalance("test");


    while (true) {
      if (client.isRepartitioningComplete("test")) {
        break;
      }
      Thread.sleep(1000);
    }

    for (com.sonicbase.server.DatabaseServer server : dbServers) {
      server.shutdownRepartitioner();
    }
    client.beginRebalance("test");


    while (true) {
      if (client.isRepartitioningComplete("test")) {
        break;
      }
      Thread.sleep(1000);
    }

    for (com.sonicbase.server.DatabaseServer server : dbServers) {
      server.shutdownRepartitioner();
    }

    client.beginRebalance("test");


    while (true) {
      if (client.isRepartitioningComplete("test")) {
        break;
      }
      Thread.sleep(1000);
    }

    for (com.sonicbase.server.DatabaseServer server : dbServers) {
      server.shutdownRepartitioner();
    }

    client.beginRebalance("test");


    while (true) {
      if (client.isRepartitioningComplete("test")) {
        break;
      }
      Thread.sleep(1000);
    }

    for (com.sonicbase.server.DatabaseServer server : dbServers) {
      server.shutdownRepartitioner();
    }


    client.beginRebalance("test");


    while (true) {
      if (client.isRepartitioningComplete("test")) {
        break;
      }
      Thread.sleep(1000);
    }

    for (com.sonicbase.server.DatabaseServer server : dbServers) {
      server.shutdownRepartitioner();
    }

    Thread.sleep(10_000);

    stmt = conn.prepareStatement("describe shards");
    ResultSet ret = stmt.executeQuery();
    while (ret.next()) {
      System.out.println(ret.getString(1));
    }
  }

  @Test
  public void test() throws SQLException, InterruptedException {

    PreparedStatement stmt = conn.prepareStatement("select id, id2 from persons where id>=0 order by id asc");
    ResultSet ret = stmt.executeQuery();

    boolean inError = false;
    for (int i = 0; i < 100_000; i++) {
      assertTrue(ret.next());
      assertEquals(ret.getInt("id"), i);
    }
    assertFalse(ret.next());

    for (com.sonicbase.server.DatabaseServer server : dbServers) {
      server.unsafePurgeMemoryForTests();
    }

    try {
      //((ConnectionProxy) conn).getDatabaseClient().createDatabase("test");

      stmt = conn.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
      stmt.executeUpdate();
    }
    catch (Exception e) {

    }

//    for (com.sonicbase.server.DatabaseServer server : dbServers) {
//      System.out.println("count logged: shard=" + server.getShard() + ", replica=" + server.getReplica() + ", count=" + server.getLogManager().getCountLogged());
//    }

    //NativeSkipListMapImpl.added.clear();

    for (com.sonicbase.server.DatabaseServer server : dbServers) {
      server.replayLogs();
    }

//    for (DatabaseServer server : dbServers) {
//      System.out.println("count replayed: shard=" + server.getShard() + ", replica=" + server.getReplica() + ", count=" + server.getLogManager().getCountReplayed());
//    }

    for (com.sonicbase.server.DatabaseServer server : dbServers) {
      server.shutdownRepartitioner();
    }

    client.beginRebalance("test");


    while (true) {
      if (client.isRepartitioningComplete("test")) {
        break;
      }
      Thread.sleep(1000);
    }

    for (com.sonicbase.server.DatabaseServer server : dbServers) {
      server.shutdownRepartitioner();
    }

    client.beginRebalance("test");


    while (true) {
      if (client.isRepartitioningComplete("test")) {
        break;
      }
      Thread.sleep(1000);
    }

    for (com.sonicbase.server.DatabaseServer server : dbServers) {
      server.shutdownRepartitioner();
    }


    client.beginRebalance("test");


    while (true) {
      if (client.isRepartitioningComplete("test")) {
        break;
      }
      Thread.sleep(1000);
    }

    for (com.sonicbase.server.DatabaseServer server : dbServers) {
      server.shutdownRepartitioner();
    }

//    Long key = NativeSkipListMapImpl.added.first();
//    int currKey = 0;
//    for (int i = 0; i < 100_000; i++) {
//      assertEquals((int)(long)key, i);
//      key = NativeSkipListMapImpl.added.higher(key);
//    }
//    assertNull(key);
//
//    Map.Entry<Object[], Object> entry = NativeSkipListMapImpl.map.firstEntry();
//    Object[][] keys = new Object[100_000][];
//    long[] values = new long[100_000];
//    int c = NativeSkipListMapImpl.map.tailBlock(entry.getKey(), 100_000, true, keys, values);
//    assertEquals(c, 50_000);
//    for (int i = 50_000; i < 100_000; i++) {
//      assertEquals((int)(long)(Long)keys[i - 50_000][0], i);
//    }

    Thread.sleep(5_000);

    stmt = conn.prepareStatement("select count(*) from persons");
    ret = stmt.executeQuery();
    assertEquals(ret.getInt(1), 100_000);

    //test select returns multiple records with an index using operator '<'
    stmt = conn.prepareStatement("select id, id2 from persons where id>=0 order by id asc");
    ret = stmt.executeQuery();

    for (int i = 0; i < 100_000; i++) {
      assertTrue(ret.next());
      assertEquals(ret.getInt("id"), i);
    }
    assertFalse(ret.next());

//    inError = false;
//    int missing = 0;
//    for (int i = 0; i < 100_000; i++) {
//      if(ret.next()) {
//        if (i != ret.getInt("id")) {
//          missing++;
//        }
//      }
//      //assertEquals(ret.getInt("id"), i);
//    }
//    assertFalse(ret.next());
//
//    stmt = conn.prepareStatement("select id, id2 from persons where id>=0 order by id asc");
//    ret = stmt.executeQuery();
//    missing = 0;
//    for (int i = 0; i < 100_000; i++) {
//      if(ret.next()) {
//        if (i != ret.getInt("id")) {
//          missing++;
//        }
//      }
//      //assertEquals(ret.getInt("id"), i);
//    }
//
//    assertFalse(ret.next());
//    assertEquals(missing, 0);
  }
}
