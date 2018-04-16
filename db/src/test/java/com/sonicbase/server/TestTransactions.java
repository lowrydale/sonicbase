package com.sonicbase.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.common.KeyRecord;
import com.sonicbase.common.Logger;
import com.sonicbase.index.Index;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.schema.TableSchema;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.UnsupportedEncodingException;
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
  private int recordCount = 10;
  List<Long> ids = new ArrayList<>();
  private Connection conn2;
  DatabaseServer[] dbServers;

  @AfterClass(alwaysRun = true)
  public void afterClass() throws SQLException {
    conn.close();
    conn2.close();

    for (DatabaseServer server : dbServers) {
      server.shutdown();
    }
    Logger.queue.clear();
    System.out.println("client refCount=" + DatabaseClient.clientRefCount.get() + ", sharedClients=" + DatabaseClient.sharedClients.size());
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

    FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db"));

    ArrayNode array = new ArrayNode(JsonNodeFactory.instance);
    array.add(DatabaseServer.FOUR_SERVER_LICENSE);
    config.put("licenseKeys", array);
    DatabaseClient.getServers().clear();

    dbServers = new DatabaseServer[4];
    ThreadPoolExecutor executor = new ThreadPoolExecutor(32, 32, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

    String role = "primaryMaster";

    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < dbServers.length; i++) {
      final int shard = i;
      dbServers[shard] = new DatabaseServer();
      dbServers[shard].setConfig(config, "test", "localhost", 9010 + (50 * shard), true, new AtomicBoolean(true), new AtomicBoolean(true),null, true);
      dbServers[shard].setRole(role);
      dbServers[shard].disableLogProcessor();
      dbServers[shard].setMinSizeForRepartition(0);
    }
    for (Future future : futures) {
      future.get();
    }

    for (DatabaseServer server : dbServers) {
      server.shutdownRepartitioner();
    }

    Class.forName("com.sonicbase.jdbcdriver.Driver");

    conn = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9000", "user", "password");

    ((ConnectionProxy)conn).getDatabaseClient().createDatabase("test");

    conn.close();

    conn = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9000/test", "user", "password");

    DatabaseClient client = ((ConnectionProxy)conn).getDatabaseClient();


    conn2 = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9000/test", "user", "password");

    Logger.setReady(false);

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
  public void testDelete() throws SQLException, InterruptedException, UnsupportedEncodingException, EOFException {
    PreparedStatement stmt = conn.prepareStatement("create table secondary_delete (id BIGINT, make VARCHAR(1024), model VARCHAR(1024))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create index make_model on secondary_delete(make, model)");
    stmt.executeUpdate();

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into secondary_delete (id, make, model) VALUES (?, ?, ?)");
      stmt.setLong(1, i);
      stmt.setString(2, "make-" + i);
      stmt.setString(3, "model-" + i);
      assertEquals(stmt.executeUpdate(), 1);
    }


    conn.setAutoCommit(false);

    Index index = ((DatabaseServer)DatabaseClient.getServers().get(0).get(0)).getIndices().get("test").getIndices().get("secondary_delete").get("_2_make_model");
    Object value = index.get(new Object[]{"make-0".getBytes("utf-8")});
    byte[][] keys = ((DatabaseServer)DatabaseClient.getServers().get(0).get(0)).fromUnsafeToKeys(value);

    index = ((DatabaseServer)DatabaseClient.getServers().get(0).get(0)).getIndices().get("test").getIndices().get("secondary_delete").get("_1__primarykey");
    TableSchema tableSchema = ((DatabaseServer)DatabaseClient.getServers().get(0).get(0)).getCommon().getTables("test").get("secondary_delete");
    KeyRecord keyRecord = new KeyRecord(keys[0]);
    Object[] primaryKey = DatabaseCommon.deserializeKey(tableSchema, keyRecord.getPrimaryKey());
    value = index.get(primaryKey);
    assertNotNull(value);

    stmt = conn.prepareStatement("delete from secondary_delete where make=? and model=?");
    stmt.setString(1,"make-0");
    stmt.setString(2,"model-0");
    stmt.executeUpdate();

    Thread thread = new Thread(new Runnable(){
      @Override
      public void run() {
        try {
          PreparedStatement stmt = conn.prepareStatement("select * from secondary_delete where make='make-0'");
          ResultSet rs = stmt.executeQuery();
          assertTrue(rs.next());
        }
        catch (SQLException e) {
          e.printStackTrace();
        }

      }
    });
    thread.start();
    thread.join();

//    stmt = conn.prepareStatement("select * from secondary_delete where make='make-0'");
//    ResultSet rs = stmt.executeQuery();
//    assertFalse(rs.next());

    conn.commit();

    index = ((DatabaseServer)DatabaseClient.getServers().get(0).get(0)).getIndices().get("test").getIndices().get("secondary_delete").get("_2_make_model");
    value = index.get(new Object[]{"make-0".getBytes("utf-8")});
    assertEquals(value, null);

    index = ((DatabaseServer)DatabaseClient.getServers().get(0).get(0)).getIndices().get("test").getIndices().get("secondary_delete").get("_1__primarykey");
    primaryKey = DatabaseCommon.deserializeKey(tableSchema, keyRecord.getPrimaryKey());
    value = index.get(primaryKey);
    assertNull(value);

    stmt = conn.prepareStatement("select * from secondary_delete where make='make-0'");
    ResultSet rs = stmt.executeQuery();
    assertFalse(rs.next());

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
  public void testRollback() throws SQLException, InterruptedException {

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

    conn.rollback();

    stmt = conn.prepareStatement("insert into persons (id, id2, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?)");
    stmt.setLong(1, 100);
    stmt.setLong(2, (100) % 2);
    stmt.setString(3, "933-28-" + (4));
    stmt.setString(4, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
    stmt.setBoolean(5, false);
    stmt.setString(6, "m");
    count = stmt.executeUpdate();
    assertEquals(count, 1);

    stmt = conn.prepareStatement("insert into persons (id, id2, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?)");
    stmt.setLong(1, 1000);
    stmt.setLong(2, (100) % 2);
    stmt.setString(3, "933-28-" + (4));
    stmt.setString(4, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
    stmt.setBoolean(5, false);
    stmt.setString(6, "m");
    count = stmt.executeUpdate();
    assertEquals(count, 1);

    stmt = conn.prepareStatement("select * from persons where id=1000");
    ResultSet resultSet = stmt.executeQuery();
    assertTrue(resultSet.next());
    assertEquals(resultSet.getLong("id"), 1000);
    assertEquals(resultSet.getString("socialsecuritynumber"), "933-28-" + (4));
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
