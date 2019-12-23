package com.sonicbase.accept.database;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.Config;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.server.DatabaseServer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.testng.Assert.*;

public class TestDeltaManager {

  private int recordCount = 10;

  DatabaseServer[] dbServers;

  @AfterClass
  public void afterClass() {
    for (DatabaseServer server : dbServers) {
      server.shutdown();
    }

  }

  @Test
  public void test() {
    try {


      String configStr = IOUtils.toString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-4-servers.yaml")), "utf-8");
      Config config = new Config(configStr);

      FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db-data"));

      DatabaseClient.getServers().clear();

      dbServers = new DatabaseServer[4];
      ThreadPoolExecutor executor = new ThreadPoolExecutor(32, 32, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

      String role = "primaryMaster";

      List<Future> futures = new ArrayList<>();
      for (int i = 0; i < dbServers.length; i++) {
        //      futures.add(executor.submit(new Callable() {
        //        @Override
        //        public Object call() throws Exception {
        //          String role = "primaryMaster";

        dbServers[i] = new DatabaseServer();
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

      DatabaseServer.initDeathOverride(2, 2);
      DatabaseServer.deathOverride[0][0] = false;
      DatabaseServer.deathOverride[0][1] = false;
      DatabaseServer.deathOverride[1][0] = false;
      DatabaseServer.deathOverride[1][1] = false;

      dbServers[0].enableSnapshot(false);
      dbServers[1].enableSnapshot(false);
      dbServers[2].enableSnapshot(false);
      dbServers[3].enableSnapshot(false);

      for (DatabaseServer server : dbServers) {
        server.shutdownRepartitioner();
      }

      Class.forName("com.sonicbase.jdbcdriver.Driver");

      Connection conn = DriverManager.getConnection("jdbc:sonicbase:localhost:9010", "user", "password");

      ((ConnectionProxy) conn).getDatabaseClient().createDatabase("test");

      conn.close();

      conn = DriverManager.getConnection("jdbc:sonicbase:localhost:9010/test", "user", "password");



      DatabaseClient client = ((ConnectionProxy) conn).getDatabaseClient();

      client.setPageSize(3);

      PreparedStatement stmt = conn.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
      stmt.executeUpdate();

      insertRecord(conn, 0);
      insertRecord(conn, 1);
      validateRecord(conn, 0);
      validateRecord(conn, 1);

      runSnapshot(dbServers);

      insertRecord(conn, 2);
      insertRecord(conn, 3);

      deleteRecord(conn, 2);

      runSnapshot(dbServers);

      insertRecord(conn, 4);
      insertRecord(conn, 5);
      insertRecord(conn, 2);

      runSnapshot(dbServers);

      restartServers(dbServers);

      validateRecord(conn, 0);
      validateRecord(conn, 1);
      validateRecord(conn, 2);
      validateRecord(conn, 3);
      validateRecord(conn, 4);
      validateRecord(conn, 5);

      insertRecord(conn, 6);
      insertRecord(conn, 7);

      deleteRecord(conn, 6);

      runSnapshot(dbServers);

      insertRecord(conn, 8);
      insertRecord(conn, 9);

      FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db/database/log"));

      restartServers(dbServers);

      validateRecord(conn, 0);
      validateRecord(conn, 1);
      validateRecord(conn, 2);
      validateRecord(conn, 3);
      validateRecord(conn, 4);
      validateRecord(conn, 5);
      validateDeleted(conn, 6);
      validateRecord(conn, 7);
//      validateRecord(conn, 8);
//      validateRecord(conn, 9);

      for (int i = 100; i < 10_000; i++) {
        insertRecord(conn, i);
      }
      runSnapshot(dbServers);

      FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db/database/log"));

      restartServers(dbServers);

      for (int i = 100; i < 10_000; i++) {
        validateRecord(conn, i);
      }

      for (int i = 100; i < 10_000; i++) {
        if (i % 2 == 0) {
          deleteRecord(conn, i);
        }
      }
      runSnapshot(dbServers);

      FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db/database/log"));

      restartServers(dbServers);

      for (int i = 100; i < 10_000; i++) {
        if (i % 2 == 0) {
          validateDeleted(conn, i);
        }
        else {
          validateRecord(conn, i);
        }
      }
      for (int i = 100; i < 10_000; i++) {
        if (i % 2 == 0) {
          deleteRecord(conn, i);
        }
      }
      runSnapshot(dbServers);

      FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db/database/log"));

      restartServers(dbServers);

      for (int i = 100; i < 10_000; i++) {
        if (i % 2 == 0) {
          validateDeleted(conn, i);
        }
        else {
          validateRecord(conn, i);
        }
      }
      for (int i = 100; i < 10_000; i++) {
        if (i % 2 == 0) {
          insertRecord(conn, i);
        }
      }

      runSnapshot(dbServers);

      FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db/database/log"));

      restartServers(dbServers);

      for (int i = 100; i < 10_000; i++) {
        validateRecord(conn, i);
      }

      executor.shutdownNow();
    }
    catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testRolling() {
    try {
      String configStr = IOUtils.toString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-4-servers.yaml")), "utf-8");
      Config config = new Config(configStr);

      FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db-data"));

      DatabaseClient.getServers().clear();

      final DatabaseServer[] dbServers = new DatabaseServer[4];
      ThreadPoolExecutor executor = new ThreadPoolExecutor(32, 32, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

      String role = "primaryMaster";

      List<Future> futures = new ArrayList<>();
      for (int i = 0; i < dbServers.length; i++) {
        //      futures.add(executor.submit(new Callable() {
        //        @Override
        //        public Object call() throws Exception {
        //          String role = "primaryMaster";

        dbServers[i] = new DatabaseServer();
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

      DatabaseServer.initDeathOverride(2, 2);
      DatabaseServer.deathOverride[0][0] = false;
      DatabaseServer.deathOverride[0][1] = false;
      DatabaseServer.deathOverride[1][0] = false;
      DatabaseServer.deathOverride[1][1] = false;

      dbServers[0].enableSnapshot(false);
      dbServers[1].enableSnapshot(false);
      dbServers[2].enableSnapshot(false);
      dbServers[3].enableSnapshot(false);

      for (DatabaseServer server : dbServers) {
        server.shutdownRepartitioner();
      }

      Class.forName("com.sonicbase.jdbcdriver.Driver");

      Connection conn = DriverManager.getConnection("jdbc:sonicbase:localhost:9010", "user", "password");

      ((ConnectionProxy) conn).getDatabaseClient().createDatabase("test");

      conn.close();

      conn = DriverManager.getConnection("jdbc:sonicbase:localhost:9010/test", "user", "password");

      DatabaseClient client = ((ConnectionProxy) conn).getDatabaseClient();

      client.setPageSize(3);

      PreparedStatement stmt = conn.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
      stmt.executeUpdate();

      for (int i = 0; i < 50; i++) {
        insertRecord(conn, i);
        insertRecord(conn, i + 100);
        deleteRecord(conn, i);
        runSnapshot(dbServers);
      }

      FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db/database/log"));

      restartServers(dbServers);

      for (int i = 0; i < 50; i++) {
        validateDeleted(conn, i);

      }
      for (int i = 100; i < 150; i++) {
        validateRecord(conn, i);
      }
      executor.shutdownNow();
    }
    catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testKeys() {
    try {
      String configStr = IOUtils.toString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-4-servers.yaml")), "utf-8");
      Config config = new Config(configStr);

      FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db-data"));

      DatabaseClient.getServers().clear();

      final DatabaseServer[] dbServers = new DatabaseServer[4];
      ThreadPoolExecutor executor = new ThreadPoolExecutor(32, 32, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

      String role = "primaryMaster";

      List<Future> futures = new ArrayList<>();
      for (int i = 0; i < dbServers.length; i++) {
        //      futures.add(executor.submit(new Callable() {
        //        @Override
        //        public Object call() throws Exception {
        //          String role = "primaryMaster";

        dbServers[i] = new DatabaseServer();
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

      DatabaseServer.initDeathOverride(2, 2);
      DatabaseServer.deathOverride[0][0] = false;
      DatabaseServer.deathOverride[0][1] = false;
      DatabaseServer.deathOverride[1][0] = false;
      DatabaseServer.deathOverride[1][1] = false;

      dbServers[0].enableSnapshot(false);
      dbServers[1].enableSnapshot(false);
      dbServers[2].enableSnapshot(false);
      dbServers[3].enableSnapshot(false);

      for (DatabaseServer server : dbServers) {
        server.shutdownRepartitioner();
      }

      Class.forName("com.sonicbase.jdbcdriver.Driver");

      Connection conn = DriverManager.getConnection("jdbc:sonicbase:localhost:9010", "user", "password");

      ((ConnectionProxy) conn).getDatabaseClient().createDatabase("test");

      conn.close();

      conn = DriverManager.getConnection("jdbc:sonicbase:localhost:9010/test", "user", "password");

      DatabaseClient client = ((ConnectionProxy) conn).getDatabaseClient();

      client.setPageSize(3);

      PreparedStatement stmt = conn.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8))");
      stmt.executeUpdate();

      stmt = conn.prepareStatement("create index id on persons(id)");
      stmt.executeUpdate();

      insertRecord(conn, 0);
      insertRecord(conn, 1);

      runSnapshot(dbServers);

      insertRecord(conn, 2);
      insertRecord(conn, 3);

      deleteRecord(conn, 2);

      runSnapshot(dbServers);

      insertRecord(conn, 4);
      insertRecord(conn, 5);
      insertRecord(conn, 2);

      runSnapshot(dbServers);

      restartServers(dbServers);

      validateRecord(conn, 0);
      validateRecord(conn, 1);
      validateRecord(conn, 2);
      validateRecord(conn, 3);
      validateRecord(conn, 4);
      validateRecord(conn, 5);

      insertRecord(conn, 6);
      insertRecord(conn, 7);

      deleteRecord(conn, 6);

      runSnapshot(dbServers);

      insertRecord(conn, 8);
      insertRecord(conn, 9);

      FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db/database/log"));

      restartServers(dbServers);

      validateRecord(conn, 0);
      validateRecord(conn, 1);
      validateRecord(conn, 2);
      validateRecord(conn, 3);
      validateRecord(conn, 4);
      validateRecord(conn, 5);
      validateDeleted(conn, 6);
      validateRecord(conn, 7);
//      validateRecord(conn, 8);
//      validateRecord(conn, 9);

      for (int i = 100; i < 10_000; i++) {
        insertRecord(conn, i);
      }
      runSnapshot(dbServers);

      FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db/database/log"));

      restartServers(dbServers);

      for (int i = 100; i < 10_000; i++) {
        validateRecord(conn, i);
      }

      for (int i = 100; i < 10_000; i++) {
        if (i % 2 == 0) {
          deleteRecord(conn, i);
        }
      }
      runSnapshot(dbServers);

      FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db/database/log"));

      restartServers(dbServers);

      for (int i = 100; i < 10_000; i++) {
        if (i % 2 == 0) {
          try {
            validateDeleted(conn, i);
          }
          catch (Exception e) {
            System.out.println("boo");
          }
        }
        else {
          validateRecord(conn, i);
        }
      }
      for (int i = 100; i < 10_000; i++) {
        if (i % 2 == 0) {
          deleteRecord(conn, i);
        }
      }
      runSnapshot(dbServers);

      FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db/database/log"));

      restartServers(dbServers);

      for (int i = 100; i < 10_000; i++) {
        if (i % 2 == 0) {
          validateDeleted(conn, i);
        }
        else {
          validateRecord(conn, i);
        }
      }
      for (int i = 100; i < 10_000; i++) {
        if (i % 2 == 0) {
          insertRecord(conn, i);
        }
      }

      runSnapshot(dbServers);

      FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db/database/log"));

      restartServers(dbServers);

      for (int i = 100; i < 10_000; i++) {
        validateRecord(conn, i);
      }

      executor.shutdownNow();
    }
    catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testDuplicateKeys() {
    try {
      String configStr = IOUtils.toString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-4-servers.yaml")), "utf-8");
      Config config = new Config(configStr);

      FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db-data"));

      DatabaseClient.getServers().clear();

      final DatabaseServer[] dbServers = new DatabaseServer[4];
      ThreadPoolExecutor executor = new ThreadPoolExecutor(32, 32, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

      String role = "primaryMaster";

      List<Future> futures = new ArrayList<>();
      for (int i = 0; i < dbServers.length; i++) {
        //      futures.add(executor.submit(new Callable() {
        //        @Override
        //        public Object call() throws Exception {
        //          String role = "primaryMaster";

        dbServers[i] = new DatabaseServer();
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

      DatabaseServer.initDeathOverride(2, 2);
      DatabaseServer.deathOverride[0][0] = false;
      DatabaseServer.deathOverride[0][1] = false;
      DatabaseServer.deathOverride[1][0] = false;
      DatabaseServer.deathOverride[1][1] = false;

      dbServers[0].enableSnapshot(false);
      dbServers[1].enableSnapshot(false);
      dbServers[2].enableSnapshot(false);
      dbServers[3].enableSnapshot(false);

      for (DatabaseServer server : dbServers) {
        server.shutdownRepartitioner();
      }

      Class.forName("com.sonicbase.jdbcdriver.Driver");

      Connection conn = DriverManager.getConnection("jdbc:sonicbase:localhost:9010", "user", "password");

      ((ConnectionProxy) conn).getDatabaseClient().createDatabase("test");

      conn.close();

      conn = DriverManager.getConnection("jdbc:sonicbase:localhost:9010/test", "user", "password");

      DatabaseClient client = ((ConnectionProxy) conn).getDatabaseClient();

      client.setPageSize(3);

      PreparedStatement stmt = conn.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8)) ");
      stmt.executeUpdate();

      stmt = conn.prepareStatement("create index id2 on persons(id2)");
      stmt.executeUpdate();

      insertRecord(conn, 0, 0);
      insertRecord(conn, 1, 0);

      runSnapshot(dbServers);

      insertRecord(conn, 2, 0);
      insertRecord(conn, 2, 1);
      validateRecord(conn, 2, 0);
      validateRecord(conn, 2, 1);

      deleteRecord(conn, 2, 0);

      runSnapshot(dbServers);

      insertRecord(conn, 4, 0);
      insertRecord(conn, 4, 1);
      insertRecord(conn, 4, 2);

      runSnapshot(dbServers);

      restartServers(dbServers);

      validateRecord(conn, 0, 0);
      validateRecord(conn, 1, 0);
      validateRecord(conn, 2, 1);
      validateRecord(conn, 4, 0);
      validateRecord(conn, 4, 1);
      validateRecord(conn, 4, 2);
      validateDeleted(conn, 2, 0);

      insertRecord(conn, 6, 0);
      insertRecord(conn, 6, 1);

      deleteRecord(conn, 6, 0);

      runSnapshot(dbServers);

      FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db/database/log"));

      restartServers(dbServers);

      validateRecord(conn, 0, 0);
      validateRecord(conn, 1, 0);
      validateDeleted(conn, 2, 0);
      validateRecord(conn, 2, 1);
      validateRecord(conn, 4, 0);
      validateRecord(conn, 4, 1);
      validateRecord(conn, 4, 2);
      validateRecord(conn, 6, 1);
      validateDeleted(conn, 6, 0);
//      validateRecord(conn, 8);
//      validateRecord(conn, 9);

      for (int i = 100; i < 10_000; i++) {
        insertRecord(conn, 100, i);
      }
      runSnapshot(dbServers);

      FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db/database/log"));

      restartServers(dbServers);


      for (int i = 100; i < 10_000; i++) {
        validateRecord(conn, 100, i);
      }

      for (int i = 100; i < 10_000; i++) {
        if (i % 2 == 0) {
          deleteRecord(conn, 100, i);
        }
      }
      runSnapshot(dbServers);

      FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db/database/log"));

      restartServers(dbServers);

      for (int i = 100; i < 10_000; i++) {
        if (i % 2 == 0) {
          try {
            validateDeleted(conn, 100, i);
          }
          catch (Exception e) {
            System.out.println("boo");
          }
        }
        else {
          validateRecord(conn, 100, i);
        }
      }
      for (int i = 100; i < 10_000; i++) {
        if (i % 2 == 0) {
          deleteRecord(conn, 100, i);
        }
      }
      runSnapshot(dbServers);

      FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db/database/log"));

      restartServers(dbServers);

      for (int i = 100; i < 10_000; i++) {
        if (i % 2 == 0) {
          validateDeleted(conn, 100, i);
        }
        else {
          validateRecord(conn, 100, i);
        }
      }
      for (int i = 100; i < 10_000; i++) {
        if (i % 2 == 0) {
          insertRecord(conn, 100, i);
        }
      }

      runSnapshot(dbServers);

      FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db/database/log"));

      restartServers(dbServers);

      for (int i = 100; i < 10_000; i++) {
        validateRecord(conn, 100, i);
      }

      executor.shutdownNow();
    }
    catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  private void insertRecord(Connection conn, int value1, int value2) throws SQLException {
    PreparedStatement stmt;
    stmt = conn.prepareStatement("insert into persons (id, id2, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?)");
    stmt.setLong(1, value1);
    stmt.setLong(2, value2);
    stmt.setString(3, "933-28-" + value1);
    stmt.setString(4, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
    stmt.setBoolean(5, false);
    stmt.setString(6, "m");
    assertEquals(stmt.executeUpdate(), 1);
  }

  private void validateDeleted(Connection conn, int id) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id = " + id);
    ResultSet ret = stmt.executeQuery();
    assertFalse(ret.next(), String.valueOf(id));
  }

  private void validateDeleted(Connection conn, int value1, int value2) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id = " + value1 + " and id2=" + value2);
    ResultSet ret = stmt.executeQuery();
    assertFalse(ret.next(), String.valueOf(value1) + "-" + String.valueOf(value2));
  }

  private void deleteRecord(Connection conn, int id) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("delete from persons where id = " + id);
    stmt.executeUpdate();
  }

  private void deleteRecord(Connection conn, int value1, int value2) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("delete from persons where id = " + value1 + " and id2=" + value2);
    stmt.executeUpdate();
  }

  private void validateRecord(Connection conn, long id) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id = " + id);
    ResultSet ret = stmt.executeQuery();
    assertTrue(ret.next(), String.valueOf(id));
    assertEquals(ret.getLong("id"), id, String.valueOf(id));
  }

  private void validateRecord(Connection conn, long value1, long value2) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id = " + value1 + " and id2=" + value2);
    ResultSet ret = stmt.executeQuery();
    assertTrue(ret.next(), String.valueOf(value1));
    assertEquals(ret.getLong("id"), value1, String.valueOf(value1));
    assertEquals(ret.getLong("id2"), value2, String.valueOf(value2));
  }

  private void restartServers(DatabaseServer[] dbServers) {
    for (DatabaseServer server : dbServers) {
      server.unsafePurgeMemoryForTests();
    }

    for (DatabaseServer server : dbServers) {
      server.getCommon().loadSchema(server.getDataDir());
      server.recoverFromSnapshot();
      server.getLogManager().applyLogs();
    }
  }

  private void runSnapshot(DatabaseServer[] dbServers) throws IOException {
    dbServers[0].runSnapshot();
    dbServers[1].runSnapshot();
    dbServers[2].runSnapshot();
    dbServers[3].runSnapshot();
  }

  private void insertRecord(Connection conn, int i) throws SQLException {
    PreparedStatement stmt;
    stmt = conn.prepareStatement("insert into persons (id, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?)");
    stmt.setLong(1, i);
    stmt.setString(2, "933-28-" + i);
    stmt.setString(3, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
    stmt.setBoolean(4, false);
    stmt.setString(5, "m");
    assertEquals(stmt.executeUpdate(), 1);
  }
}
