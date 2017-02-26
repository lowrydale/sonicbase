package com.sonicbase.database;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.server.DatabaseServer;
import com.sonicbase.util.JsonArray;
import com.sonicbase.util.JsonDict;
import com.sonicbase.util.StreamUtils;
import org.codehaus.plexus.util.FileUtils;
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
public class TestTransactions {

  private Connection conn;
  private int recordCount = 10;
  List<Long> ids = new ArrayList<>();
  private Connection conn2;

  @BeforeClass
  public void beforeClass() throws Exception {
    String configStr = StreamUtils.inputStreamToString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-4-servers.json")));
    final JsonDict config = new JsonDict(configStr);

    JsonArray array = config.getDict("database").putArray("licenseKeys");
    array.add(DatabaseServer.FOUR_SERVER_LICENSE);


    FileUtils.deleteDirectory(new File("/data/database"));

    DatabaseServer.getServers().clear();

    final DatabaseServer[] dbServers = new DatabaseServer[4];
    ThreadPoolExecutor executor = new ThreadPoolExecutor(32, 32, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

    String role = "primaryMaster";

    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < dbServers.length; i++) {
      final int shard = i;
      dbServers[shard] = new DatabaseServer();
      dbServers[shard].setConfig(config, "test", "localhost", 9010 + (50 * shard), true, new AtomicBoolean(true));
      dbServers[shard].setRole(role);
      dbServers[shard].disableLogProcessor();
    }
    for (Future future : futures) {
      future.get();
    }

    for (DatabaseServer server : dbServers) {
      server.disableRepartitioner();
    }

    Class.forName("com.sonicbase.jdbcdriver.Driver");

    conn = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9000", "user", "password");

    ((ConnectionProxy)conn).getDatabaseClient().createDatabase("test");

    conn.close();

    conn = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9000/test", "user", "password");

    DatabaseClient client = ((ConnectionProxy)conn).getDatabaseClient();


    conn2 = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9000/test", "user", "password");

    PreparedStatement stmt = conn.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create table Memberships (personId BIGINT, membershipName VARCHAR(20), resortId BIGINT, PRIMARY KEY (personId, membershipName))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create table Resorts (resortId BIGINT, resortName VARCHAR(20), PRIMARY KEY (resortId))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create index socialSecurityNumber on persons(socialSecurityNumber)");
    stmt.executeUpdate();

    executor.shutdownNow();
  }

  @Test
  public void testTransactions() throws SQLException {
    conn.setAutoCommit(false);

    PreparedStatement stmt = conn.prepareStatement("insert into persons (id) VALUES (?)");

    stmt.setLong(1, 100000);
    int count = stmt.executeUpdate();
    assertEquals(count, 1);

    conn.commit();

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
             if (count == 1) {
               updated.set(true);
             }
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

     latch.await(1000, TimeUnit.SECONDS);
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

   @Test(enabled=false)
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

   @Test(enabled=false)
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


   @Test(enabled=false)
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
