package com.sonicbase.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.Logger;
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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Responsible for
 */
public class TestLogManager {

  private Connection conn;
  DatabaseClient client = null;
  DatabaseServer[] dbServers;


  @AfterClass(alwaysRun = true)
  public void afterClass() throws SQLException {
    conn.close();

    for (DatabaseServer server : dbServers) {
      server.shutdown();
    }
    Logger.queue.clear();
    System.out.println("client refCount=" + DatabaseClient.clientRefCount.get() + ", sharedClients=" + DatabaseClient.sharedClients.size() + ", class=TestLogManager");
    for (DatabaseClient client : DatabaseClient.allClients) {
      System.out.println("Stack:\n" + client.getAllocatedStack());
    }

  }

  @BeforeClass
  public void beforeClass() throws Exception {
    Logger.disable();

    String configStr = IOUtils.toString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-4-servers.json")), "utf-8");
    ObjectMapper mapper = new ObjectMapper();
    final ObjectNode config = (ObjectNode) mapper.readTree(configStr);

    ArrayNode array = new ArrayNode(JsonNodeFactory.instance);
    config.put("licenseKeys", array);
    array.add(DatabaseServer.FOUR_SERVER_LICENSE);

    FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db"));

    DatabaseClient.getServers().clear();

    dbServers = new DatabaseServer[4];
    ThreadPoolExecutor executor = new ThreadPoolExecutor(32, 32, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

    String role = "primaryMaster";

    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < dbServers.length; i++) {
      final int shard = i;
      //      futures.add(executor.submit(new Callable() {
      //        @Override
      //        public Object call() throws Exception {
      //          String role = "primaryMaster";

      dbServers[shard] = new DatabaseServer();
      dbServers[shard].setConfig(config, "4-servers", "localhost", 9010 + (50 * shard), true, new AtomicBoolean(true), new AtomicBoolean(true),null, true);
      dbServers[shard].setRole(role);
      dbServers[shard].disableLogProcessor();
      dbServers[shard].setMinSizeForRepartition(0);
      //          return null;
      //        }
      //      }));
    }
    for (Future future : futures) {
      future.get();
    }

    DatabaseServer.initDeathOverride(2, 2);
    DatabaseServer.deathOverride[0][0] = false;
    DatabaseServer.deathOverride[0][1] = false;
    DatabaseServer.deathOverride[1][0] = false;
    DatabaseServer.deathOverride[1][1] = false;

    dbServers[0].enableSnapshot(false);
    dbServers[1].enableSnapshot(false);
    dbServers[2].enableSnapshot(false);
    dbServers[3].enableSnapshot(false);


    Thread.sleep(5000);

    //DatabaseClient client = new DatabaseClient("localhost", 9010, true);

    Class.forName("com.sonicbase.jdbcdriver.Driver");

    conn = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9000", "user", "password");

    try {
      ((ConnectionProxy) conn).getDatabaseClient().createDatabase("_sonicbase_sys");
    }
    catch (Exception e) {
      e.printStackTrace();
    }

    conn.close();

    conn = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9000/_sonicbase_sys", "user", "password");

    client = ((ConnectionProxy) conn).getDatabaseClient();


    PreparedStatement stmt = conn.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
    stmt.executeUpdate();

    futures = new ArrayList<>();
    for (int i = 0; i < 100_000; i++) {
      final int offset = i;
      futures.add(executor.submit(new Callable() {
        @Override
        public Object call() throws Exception {
          PreparedStatement stmt2 = conn.prepareStatement("insert into persons (id, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?)");
          stmt2.setLong(1, offset);
          stmt2.setString(2, "933-28-" + offset);
          stmt2.setString(3, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
          stmt2.setBoolean(4, false);
          stmt2.setString(5, "m");
          assertEquals(stmt2.executeUpdate(), 1);
          return null;
        }
      }));
    }
    for (Future future : futures) {
      future.get();
    }

    for (DatabaseServer server : dbServers) {
      server.shutdownRepartitioner();
    }
    client.beginRebalance("test", "persons", "_1__primarykey");


    while (true) {
      if (client.isRepartitioningComplete("test")) {
        break;
      }
      Thread.sleep(1000);
    }

    for (DatabaseServer server : dbServers) {
      server.shutdownRepartitioner();
    }
    client.beginRebalance("test", "persons", "_1__primarykey");


    while (true) {
      if (client.isRepartitioningComplete("test")) {
        break;
      }
      Thread.sleep(1000);
    }

    for (DatabaseServer server : dbServers) {
      server.shutdownRepartitioner();
    }
    client.beginRebalance("test", "persons", "_1__primarykey");


    while (true) {
      if (client.isRepartitioningComplete("test")) {
        break;
      }
      Thread.sleep(1000);
    }

    for (DatabaseServer server : dbServers) {
      server.shutdownRepartitioner();
    }

    stmt = conn.prepareStatement("describe shards");
    ResultSet ret = stmt.executeQuery();
    while (ret.next()) {
      System.out.println(ret.getString(1));
    }
  }

  @Test
  public void test() throws SQLException {

    PreparedStatement stmt = conn.prepareStatement("select id, id2 from persons where id>=0 order by id asc");
    ResultSet ret = stmt.executeQuery();

    boolean inError = false;
    for (int i = 0; i < 100_000; i++) {
      assertTrue(ret.next());
      assertEquals(ret.getInt("id"), i);
    }
    assertFalse(ret.next());

    for (DatabaseServer server : dbServers) {
      server.truncateTablesQuietly();
    }

    try {
      //((ConnectionProxy) conn).getDatabaseClient().createDatabase("test");

      stmt = conn.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
      stmt.executeUpdate();
    }
    catch (Exception e) {

    }

    for (DatabaseServer server : dbServers) {
      System.out.println("count logged: shard=" + server.getShard() + ", replica=" + server.getReplica() + ", count=" + server.getLogManager().getCountLogged());
    }

    for (DatabaseServer server : dbServers) {
      server.replayLogs();
    }

    for (DatabaseServer server : dbServers) {
      System.out.println("count replayed: shard=" + server.getShard() + ", replica=" + server.getReplica() + ", count=" + server.getLogManager().getCountReplayed());
    }

    stmt = conn.prepareStatement("select count(*) from persons");
    ret = stmt.executeQuery();
    assertEquals(ret.getInt(1), 100_000);

    //test select returns multiple records with an index using operator '<'
    stmt = conn.prepareStatement("select id, id2 from persons where id>=0 order by id asc");
    ret = stmt.executeQuery();

    inError = false;
    int missing = 0;
    for (int i = 0; i < 100_000; i++) {
      if(ret.next()) {
        if (i != ret.getInt("id")) {
          missing++;
        }
      }
      //assertEquals(ret.getInt("id"), i);
    }
    assertFalse(ret.next());
    assertEquals(missing, 0);
  }
}
