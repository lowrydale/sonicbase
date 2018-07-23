package com.sonicbase.accept.bench;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.server.DatabaseServer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.testng.Assert.*;

/**
 * Responsible for
 */
public class TestTransactions {

  private Connection conn;
  private Connection conn2;

  @BeforeClass
  public void beforeClass() throws Exception {
    String configStr = IOUtils.toString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-4-servers.json")), "utf-8");
    ObjectMapper mapper = new ObjectMapper();
    final ObjectNode config = (ObjectNode) mapper.readTree(configStr);

    FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db"));

    DatabaseClient.getServers().clear();

    final DatabaseServer[] dbServers = new DatabaseServer[4];
    ThreadPoolExecutor executor = new ThreadPoolExecutor(32, 32, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

    String role = "primaryMaster";

    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < dbServers.length; i++) {
      final int shard = i;
      futures.add(executor.submit(new Callable() {
        @Override
        public Object call() throws Exception {
          String role = "primaryMaster";

          dbServers[shard] = new DatabaseServer();
          dbServers[shard].setConfig(config, "test", "localhost", 9010 + (50 * shard), true, new AtomicBoolean(true), new AtomicBoolean(true),null);
          dbServers[shard].setRole(role);
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

    DatabaseClient client = new DatabaseClient("localhost", 9010, -1, -1, true);

    Class.forName("com.sonicbase.jdbcdriver.Driver");


    Class.forName("com.mysql.jdbc.Driver").newInstance();

//    final java.sql.Connection conn = DriverManager.getConnection("jdbc:voltdb://localhost:21212");

    conn = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9000", "user", "password");
    conn2 = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9000", "user", "password");

//    conn = java.sql.DriverManager.getConnection("jdbc:mysql://localhost/test", "test", "test");
//    conn2 = java.sql.DriverManager.getConnection("jdbc:mysql://localhost/test", "test", "test");

    try {
      PreparedStatement stmt = conn.prepareStatement("drop table persons");
      //stmt.executeUpdate();
      stmt = conn.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(21000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id)) ENGINE = InnoDB");
      stmt.executeUpdate();
    }
    catch (Exception e) {
      e.printStackTrace();
    }
    try {
      PreparedStatement stmt = conn.prepareStatement("drop table memberships");
      //stmt.executeUpdate();
      stmt = conn.prepareStatement("create table Memberships (personId BIGINT, membershipName VARCHAR(20), resortId BIGINT, PRIMARY KEY (personId, membershipName)) ENGINE = InnoDB");
      stmt.executeUpdate();
    }
    catch (Exception e) {
      e.printStackTrace();
    }
    try {
      PreparedStatement stmt = conn.prepareStatement("drop table resorts");
      //stmt.executeUpdate();
      stmt = conn.prepareStatement("create table Resorts (resortId BIGINT, resortName VARCHAR(20), PRIMARY KEY (resortId)) ENGINE = InnoDB");
      stmt.executeUpdate();
    }
    catch (Exception e) {
      e.printStackTrace();
    }
    try {
      PreparedStatement stmt = conn.prepareStatement("create index socialSecurityNumber on persons(socialSecurityNumber)");
      stmt.executeUpdate();
    }
    catch (Exception e) {
      e.printStackTrace();
    }

    try {
      PreparedStatement stmt = conn.prepareStatement("truncate table persons");
      stmt.executeUpdate();
    }
    catch (Exception e) {
      e.printStackTrace();
    }

    try {
      PreparedStatement stmt = conn.prepareStatement("set global transaction isolation level REPEATABLE READ");
      stmt.executeUpdate();
    }
    catch (Exception e) {
      e.printStackTrace();
    }

    ((ConnectionProxy)conn).getDatabaseClient().syncSchema();
    ((ConnectionProxy)conn2).getDatabaseClient().syncSchema();
  }

  @Test
  public void test() throws SQLException, InterruptedException {

    PreparedStatement stmt = conn.prepareStatement("truncate table persons");
    stmt.execute();

    conn.setAutoCommit(false);

    stmt = conn.prepareStatement("insert into persons (id, id2, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?)");
    stmt.setLong(1, 100);
    stmt.setLong(2, (100) % 2);
    stmt.setString(3, "933-28-" + (4));
    stmt.setString(4, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
    stmt.setBoolean(5, false);
    stmt.setString(6, "m");
    int count = stmt.executeUpdate();
    assertEquals(count, 1);

    stmt = conn.prepareStatement("update persons set id = ?, socialSecurityNumber=? where id=?");
    stmt.setLong(1, 1000);
    stmt.setString(2, "ssn");
    stmt.setLong(3, 100);
    count = stmt.executeUpdate();
    //assertEquals(count, 1);

    conn.commit();

    stmt = conn.prepareStatement("select * from persons where id=1000");
    ResultSet resultSet = stmt.executeQuery();
    assertTrue(resultSet.next());
    assertEquals(resultSet.getLong("id"), 1000);
    assertEquals(resultSet.getString("socialsecuritynumber"), "ssn");
  }

  @Test
  public void testConcurrent() throws SQLException, InterruptedException {

    PreparedStatement stmt = conn.prepareStatement("truncate table persons");
    stmt.execute();

    conn.setAutoCommit(false);

    stmt = conn.prepareStatement("insert into persons (id, id2, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?)");
    stmt.setLong(1, 100);
    stmt.setLong(2, (100) % 2);
    stmt.setString(3, "933-28-" + (4));
    stmt.setString(4, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
    stmt.setBoolean(5, false);
    stmt.setString(6, "m");
    int count = stmt.executeUpdate();
    assertEquals(count, 1);

    final CountDownLatch latch = new CountDownLatch(1);
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          PreparedStatement stmt = conn2.prepareStatement("update persons set id = ?, socialSecurityNumber=? where id=?");
          stmt.setLong(1, 1000);
          stmt.setString(2, "ssn");
          stmt.setLong(3, 100);
          int count = stmt.executeUpdate();
          assertEquals(count, 0); // record not found
        }
        catch (Exception e) {
          e.printStackTrace();
        }
        finally {
          latch.countDown();
        }
      }
    });
    thread.start();

    latch.await();

    stmt = conn.prepareStatement("update persons set id = ?, socialSecurityNumber=? where id=?");
    stmt.setLong(1, 1000);
    stmt.setString(2, "ssn");
    stmt.setLong(3, 100);
    count = stmt.executeUpdate();
    assertEquals(count, 1);

    conn.commit();

    stmt = conn.prepareStatement("select * from persons where id=1000");
    ResultSet resultSet = stmt.executeQuery();
    assertTrue(resultSet.next());
    assertEquals(resultSet.getLong("id"), 1000);
    assertEquals(resultSet.getString("socialsecuritynumber"), "ssn");
  }


  @Test
  public void testConcurrent2() throws SQLException, InterruptedException {

    PreparedStatement stmt = conn.prepareStatement("truncate table persons");
    stmt.execute();

    conn.setAutoCommit(false);

    stmt = conn.prepareStatement("insert into persons (id, id2, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?)");
    stmt.setLong(1, 100);
    stmt.setLong(2, (100) % 2);
    stmt.setString(3, "933-28-" + (4));
    stmt.setString(4, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
    stmt.setBoolean(5, false);
    stmt.setString(6, "m");
    int count = stmt.executeUpdate();
    assertEquals(count, 1);

    conn.commit();

    conn.setAutoCommit(false);

    stmt = conn.prepareStatement("update persons set id = ?, socialSecurityNumber=? where id=?");
    stmt.setLong(1, 1000);
    stmt.setString(2, "ssn");
    stmt.setLong(3, 100);
    count = stmt.executeUpdate();
    assertEquals(count, 1);


    final AtomicBoolean updated = new AtomicBoolean();
    final CountDownLatch latch = new CountDownLatch(1);
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {

          PreparedStatement stmt = conn2.prepareStatement("update persons set id = ?, socialSecurityNumber=? where id=?");
          stmt.setLong(1, 1000);
          stmt.setString(2, "ssn");
          stmt.setLong(3, 100);
          try {
            int count = stmt.executeUpdate();
            updated.set(true);
          }
          catch (Exception e) {
            //expected
          }
          latch.countDown();
        }
        catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
    thread.start();

    latch.await(1000, TimeUnit.MILLISECONDS);
    thread.interrupt();
    assertFalse(updated.get());

    stmt = conn.prepareStatement("update persons set id = ?, socialSecurityNumber=? where id=?");
    stmt.setLong(1, 1000);
    stmt.setString(2, "ssn-2");
    stmt.setLong(3, 1000);
    count = stmt.executeUpdate();
    assertEquals(count, 1);

    conn.commit();

    stmt = conn.prepareStatement("select * from persons where id=1000");
    ResultSet resultSet = stmt.executeQuery();
    assertTrue(resultSet.next());
    assertEquals(resultSet.getLong("id"), 1000);
    assertEquals(resultSet.getString("socialsecuritynumber"), "ssn-2");
  }

  @Test
  public void testConcurrent3() throws SQLException, InterruptedException {

    PreparedStatement stmt = conn.prepareStatement("truncate table persons");
    stmt.execute();

    conn.setAutoCommit(false);

    stmt = conn.prepareStatement("insert into persons (id, id2, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?)");
    stmt.setLong(1, 100);
    stmt.setLong(2, (100) % 2);
    stmt.setString(3, "933-28-" + (4));
    stmt.setString(4, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
    stmt.setBoolean(5, false);
    stmt.setString(6, "m");
    int count = stmt.executeUpdate();
    assertEquals(count, 1);

//    conn.commit();
//
//    conn.setAutoCommit(false);

    stmt = conn.prepareStatement("insert into persons (id, id2, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?)");
    stmt.setLong(1, 101);
    stmt.setLong(2, (101) % 2);
    stmt.setString(3, "933-28-" + (4));
    stmt.setString(4, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
    stmt.setBoolean(5, false);
    stmt.setString(6, "m");
    count = stmt.executeUpdate();
    assertEquals(count, 1);


    final AtomicBoolean updated = new AtomicBoolean();
    final CountDownLatch latch = new CountDownLatch(1);
    final CountDownLatch latch2 = new CountDownLatch(1);
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {

          conn2.setAutoCommit(false);
          PreparedStatement stmt = conn2.prepareStatement("update persons set id = ?, socialSecurityNumber=? where id=?");
          stmt.setLong(1, 100);
          stmt.setString(2, "ssn2");
          stmt.setLong(3, 100);
          int count = stmt.executeUpdate();
          assertEquals(count, 1);
          latch.countDown();

          stmt = conn2.prepareStatement("update persons set id = ?, socialSecurityNumber=? where id=?");
          stmt.setLong(1, 100);
          stmt.setString(2, "ssn2");
          stmt.setLong(3, 100);
          count = stmt.executeUpdate();
          assertEquals(count, 1);
          latch2.countDown();
          conn2.commit();
          System.out.println("committed");
          updated.set(true);
        }
        catch (Exception e) {
          try {
            conn2.rollback();
          }
          catch (SQLException e1) {
            e1.printStackTrace();
          }
          e.printStackTrace();
          latch.countDown();
          latch2.countDown();
        }
      }
    });
    thread.start();

    latch.await(1000, TimeUnit.MILLISECONDS);
    assertFalse(updated.get());

    stmt = conn.prepareStatement("update persons set id = ?, socialSecurityNumber=? where id=?");
    stmt.setLong(1, 100);
    stmt.setString(2, "ssn-3");
    stmt.setLong(3, 100);
    try {
      count = stmt.executeUpdate();
      assertEquals(count, 1);
      conn.commit();
    }
    catch (Exception e) {
      //expected
      conn.rollback();
    }

    latch2.await(1000, TimeUnit.MILLISECONDS);


    stmt = conn.prepareStatement("select * from persons where id=101");
    ResultSet resultSet = stmt.executeQuery();
    assertTrue(resultSet.next());
  }

  @Test
  public void testConcurrent4() throws SQLException, InterruptedException {

    PreparedStatement stmt = conn.prepareStatement("truncate table persons");
    stmt.execute();

    conn.setAutoCommit(false);

    stmt = conn.prepareStatement("insert into persons (id, id2, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?)");
    stmt.setLong(1, 100);
    stmt.setLong(2, (100) % 2);
    stmt.setString(3, "933-28-" + (4));
    stmt.setString(4, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
    stmt.setBoolean(5, false);
    stmt.setString(6, "m");
    int count = stmt.executeUpdate();
    assertEquals(count, 1);

    conn.commit();

    conn.setAutoCommit(false);

    stmt = conn.prepareStatement("insert into persons (id, id2, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?)");
    stmt.setLong(1, 101);
    stmt.setLong(2, (101) % 2);
    stmt.setString(3, "933-28-" + (4));
    stmt.setString(4, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
    stmt.setBoolean(5, false);
    stmt.setString(6, "m");
    count = stmt.executeUpdate();
    assertEquals(count, 1);


    final CountDownLatch latch = new CountDownLatch(1);
    final CountDownLatch latch2 = new CountDownLatch(1);
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          conn2.setAutoCommit(false);
          PreparedStatement stmt = conn2.prepareStatement("update persons set id = ?, socialSecurityNumber=? where id=?");
          stmt.setLong(1, 100);
          stmt.setString(2, "ssn2");
          stmt.setLong(3, 100);
          int count = stmt.executeUpdate();
          assertEquals(count, 1);
          conn2.commit();
          latch.countDown();

          latch2.await();

          conn2.setAutoCommit(false);

          stmt = conn2.prepareStatement("update persons set id = ?, socialSecurityNumber=? where id=?");
          stmt.setLong(1, 1000);
          stmt.setString(2, "ssn2");
          stmt.setLong(3, 1000);
          count = stmt.executeUpdate();
          assertEquals(count, 1);
          conn2.commit();

        }
        catch (Exception e) {
          try {
            conn2.rollback();
          }
          catch (SQLException e1) {
            e1.printStackTrace();
          }
          e.printStackTrace();
        }
      }
    });
    thread.start();

    latch.await();


    stmt = conn.prepareStatement("update persons set id = ?, socialSecurityNumber=? where id=?");
    stmt.setLong(1, 1000);
    stmt.setString(2, "ssn-3");
    stmt.setLong(3, 100);
    try {
      count = stmt.executeUpdate();
      assertEquals(count, 1);
      conn.commit();
    }
    catch (Exception e) {
      //expected
      conn.rollback();
    }

    latch2.countDown();


    stmt = conn.prepareStatement("select * from persons where id=101");
    ResultSet resultSet = stmt.executeQuery();
    assertTrue(resultSet.next());
  }


  @Test
    public void testConcurrentSecondaryKeys() throws SQLException, InterruptedException {

      PreparedStatement stmt = conn.prepareStatement("truncate table persons");
      stmt.execute();

      conn.setAutoCommit(false);

      stmt = conn.prepareStatement("insert into persons (id, id2, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?)");
      stmt.setLong(1, 100);
      stmt.setLong(2, (100) % 2);
      stmt.setString(3, "933-28-" + (4));
      stmt.setString(4, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
      stmt.setBoolean(5, false);
      stmt.setString(6, "m");
      int count = stmt.executeUpdate();
      assertEquals(count, 1);

      conn.commit();

      conn.setAutoCommit(false);

      stmt = conn.prepareStatement("update persons set id = ?, socialSecurityNumber=? where id=?");
      stmt.setLong(1, 1000);
      stmt.setString(2, "ssn");
      stmt.setLong(3, 100);
      count = stmt.executeUpdate();
      assertEquals(count, 1);


      final AtomicBoolean updated = new AtomicBoolean();
      final CountDownLatch latch = new CountDownLatch(1);
      Thread thread = new Thread(new Runnable() {
        @Override
        public void run() {
          try {

            PreparedStatement stmt = conn2.prepareStatement("update persons set id = ?, socialSecurityNumber=? where id=?");
            stmt.setLong(1, 1000);
            stmt.setString(2, "933-28-" + (4));
            stmt.setLong(3, 100);
            try {
              int count = stmt.executeUpdate();
              updated.set(true);
            }
            catch (Exception e) {
              //expected
            }
            latch.countDown();
          }
          catch (Exception e) {
            e.printStackTrace();
          }
        }
      });
      thread.start();

      latch.await(1000, TimeUnit.MILLISECONDS);
      thread.interrupt();
      assertFalse(updated.get());

      stmt = conn.prepareStatement("update persons set id = ?, socialSecurityNumber=? where id=?");
      stmt.setLong(1, 1000);
      stmt.setString(2, "ssn-2");
      stmt.setLong(3, 1000);
      count = stmt.executeUpdate();
      assertEquals(count, 1);

      conn.commit();

      stmt = conn.prepareStatement("select * from persons where id=1000");
      ResultSet resultSet = stmt.executeQuery();
      assertTrue(resultSet.next());
      assertEquals(resultSet.getLong("id"), 1000);
      assertEquals(resultSet.getString("socialsecuritynumber"), "ssn-2");
    }

}
