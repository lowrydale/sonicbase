package com.sonicbase.accept.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.ComArray;
import com.sonicbase.common.ComObject;
import com.sonicbase.common.Config;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.server.DatabaseServer;
import com.sonicbase.server.NettyServer;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.*;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.text.NumberFormat;
import java.util.concurrent.*;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class TestBulkImport {
  private Connection connA;
  private Connection connB;
  private DatabaseClient clientA;
  private DatabaseClient clientB;
  private com.sonicbase.server.DatabaseServer[] dbServers;
  private NettyServer serverA1;
  private NettyServer serverA2;
  private NettyServer serverB1;
  private NettyServer serverB2;

  @AfterMethod(alwaysRun = true)
  public void afterClass() throws SQLException {
    connA.close();
    connB.close();

    for (com.sonicbase.server.DatabaseServer server : dbServers) {
      server.shutdown();
    }

    serverA1.shutdown();
    serverA2.shutdown();
    serverB1.shutdown();
    serverB2.shutdown();

    System.out.println("client refCount=" + DatabaseClient.getClientRefCount().get());

    System.out.println("finished");
  }

  @BeforeMethod
  public void beforeClass() throws IOException, InterruptedException, SQLException, ClassNotFoundException {
    try {
      System.setProperty("log4j.configuration", "test-log4j.xml");
      final String configStr = IOUtils.toString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-2-servers-a.yaml")), "utf-8");
      Config config = new Config(configStr);

      FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db-data"));

      DatabaseClient.getServers().clear();

      dbServers = new com.sonicbase.server.DatabaseServer[4];

      String role = "primaryMaster";

      final CountDownLatch latch = new CountDownLatch(4);
      serverA1 = new NettyServer(128);
      Thread thread = new Thread(() -> {
        serverA1.startServer(new String[]{"-port", String.valueOf(9010), "-host", "localhost",
            "-mport", String.valueOf(9010), "-mhost", "localhost", "-cluster", "2-servers-a", "-config", configStr, "-shard", String.valueOf(0)});
        latch.countDown();
      });
      thread.start();
      while (true) {
        if (serverA1.isRunning()) {
          break;
        }
        Thread.sleep(100);
      }

      serverA2 = new NettyServer(128);
      thread = new Thread(() -> {
        serverA2.startServer(new String[]{"-port", String.valueOf(9060), "-host", "localhost",
            "-mport", String.valueOf(9060), "-mhost", "localhost", "-cluster", "2-servers-a", "-config", configStr, "-shard", String.valueOf(1)});
        latch.countDown();
      });
      thread.start();
      while (true) {
        if (serverA2.isRunning()) {
          break;
        }
        Thread.sleep(100);
      }

      final String configStrB = IOUtils.toString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-2-servers-b.yaml")), "utf-8");
      serverB1 = new NettyServer(128);
      thread = new Thread(() -> {
        serverB1.startServer(new String[]{"-port", String.valueOf(9110), "-host", "localhost",
            "-mport", String.valueOf(9110), "-mhost", "localhost", "-cluster", "2-servers-b", "-config", configStrB, "-shard", String.valueOf(0)});
        latch.countDown();
      });
      thread.start();
      while (true) {
        if (serverB1.isRunning()) {
          break;
        }
        Thread.sleep(100);
      }

      serverB2 = new NettyServer(128);
      thread = new Thread(() -> {
        serverB2.startServer(new String[]{"-port", String.valueOf(9160), "-host", "localhost",
            "-mport", String.valueOf(9160), "-mhost", "localhost", "-cluster", "2-servers-b", "-config", configStrB, "-shard", String.valueOf(1)});
        latch.countDown();
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

      System.out.println("Started 4 servers");

//
//      //DatabaseClient client = new DatabaseClient("localhost", 9010, true);
//
      Class.forName("com.sonicbase.jdbcdriver.Driver");

      connA = DriverManager.getConnection("jdbc:sonicbase:localhost:9010", "user", "password");

      ((ConnectionProxy) connA).getDatabaseClient().createDatabase("test");

      connA.close();

      connA = DriverManager.getConnection("jdbc:sonicbase:localhost:9010/test", "user", "password");
      clientA = ((ConnectionProxy)connA).getDatabaseClient();
      clientA.syncSchema();

      connB = DriverManager.getConnection("jdbc:sonicbase:localhost:9110", "user", "password");

      ((ConnectionProxy) connB).getDatabaseClient().createDatabase("test");

      connB.close();

      connB = DriverManager.getConnection("jdbc:sonicbase:localhost:9110/test", "user", "password");
      clientB = ((ConnectionProxy)connB).getDatabaseClient();
      clientB.syncSchema();


      //
      PreparedStatement stmt = connA.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
      stmt.executeUpdate();

      stmt = connB.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
      stmt.executeUpdate();

      //rebalance
      for (com.sonicbase.server.DatabaseServer server : dbServers) {
        server.shutdownRepartitioner();
      }


      for (int i = 0; i < 10_000; i++) {
        stmt = connA.prepareStatement("insert into persons (id, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?)");
        stmt.setLong(1, i);
        stmt.setString(2, "933-28-" + i);
        stmt.setString(3, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
        stmt.setBoolean(4, false);
        stmt.setString(5, "m");
        assertEquals(stmt.executeUpdate(), 1);
        if (i % 1000 == 0) {
          System.out.println("progress: count=" + i);
        }
      }


//      long size = client.getPartitionSize("test", 0, "children", "_1_socialsecuritynumber");
//      assertEquals(size, 10);

      clientA.beginRebalance("test");


      while (true) {
        if (clientA.isRepartitioningComplete("test")) {
          break;
        }
        Thread.sleep(200);
      }

      for (com.sonicbase.server.DatabaseServer server : dbServers) {
        server.shutdownRepartitioner();
      }

      clientA.beginRebalance("test");


      while (true) {
        if (clientA.isRepartitioningComplete("test")) {
          break;
        }
        Thread.sleep(200);
      }

      for (DatabaseServer server : dbServers) {
        server.shutdownRepartitioner();
      }

//      dbServers[0].getDeleteManager().forceDeletes();
//      dbServers[1].getDeleteManager().forceDeletes();
//      dbServers[2].getDeleteManager().forceDeletes();
//      dbServers[3].getDeleteManager().forceDeletes();
    }
    catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  public void test() throws Exception {

    PreparedStatement stmt = connB.prepareStatement("truncate table persons");
    stmt.executeUpdate();

    stmt = connA.prepareStatement("select count(*) from persons");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    //assertEquals(rs.getLong(1), 10_000);

    stmt = connA.prepareStatement("select * from persons where (id < 2251) and (id > 1000)");
    rs = stmt.executeQuery();
    for (int i = 1001; i < 2251; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), i);
      assertEquals(rs.getString("socialsecuritynumber"), "933-28-" + i);
    }
    assertFalse(rs.next());


    stmt = connA.prepareStatement("select * from persons where id>=0");
    rs = stmt.executeQuery();
    for (int i = 0; i < 10_000; i++) {
      assertTrue(rs.next());
       assertEquals(rs.getLong("id"), i);
      assertEquals(rs.getString("socialsecuritynumber"), "933-28-" + i);
    }
    assertFalse(rs.next());

    stmt = connA.prepareStatement("select * from persons where (id >= 2500 and id < 3750)");
    rs = stmt.executeQuery();
    for (int i = 2500; i < 3750; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), i);
      assertEquals(rs.getString("socialsecuritynumber"), "933-28-" + i);
    }
    assertFalse(rs.next());

    stmt = connA.prepareStatement("select * from persons where (id >= 2500 and id < 3750) and id > 3000");
    rs = stmt.executeQuery();
    for (int i = 3001; i < 3750; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), i);
      assertEquals(rs.getString("socialsecuritynumber"), "933-28-" + i);
    }
    assertFalse(rs.next());

    stmt = connA.prepareStatement("select * from persons where (id >= 2500 and id < 3750) and id > 1000");
    rs = stmt.executeQuery();
    for (int i = 2500; i < 3750; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), i);
      assertEquals(rs.getString("socialsecuritynumber"), "933-28-" + i);
    }
    assertFalse(rs.next());



    startBulkImport((ConnectionProxy) connB, "start bulk import from persons(com.sonicbase.jdbcdriver.Driver, jdbc:sonicbase:localhost:9010/test)");

    while (true) {
      String ret = bulkImportStatus((ConnectionProxy) connB);
      System.out.println("status=" + ret);
      if (ret.contains("finished=true") && ret.startsWith("processing")) {
        break;
      }
      Thread.sleep(500);
    }
    System.out.println("Finished load");

    //validate data is in cluster B
    stmt = connB.prepareStatement("select * from persons where id>=0");
    rs = stmt.executeQuery();
    for (int i = 0; i < 10_000; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), i);
      assertEquals(rs.getString("socialsecuritynumber"), "933-28-" + i);
    }
    assertFalse(rs.next());

  }

  @Test
  public void testWithWhere() throws Exception {

    PreparedStatement stmt = connB.prepareStatement("truncate table persons");
    stmt.executeUpdate();

    stmt = connA.prepareStatement("select count(*) from persons");
    ResultSet rs = stmt.executeQuery();
    rs.next();
//    assertEquals(rs.getLong(1), 10_000);

    stmt = connA.prepareStatement("select * from persons where (id >= 3501 and id < 4751 and (id > 1000)) ");//
    rs = stmt.executeQuery();
    for (int i = 3501; i < 4_751; i++) {
      assertTrue(rs.next(), String.valueOf(i));
      assertEquals(rs.getLong("id"), i);
    }
    assertFalse(rs.next());

    startBulkImport((ConnectionProxy) connB, "start bulk import from persons(com.sonicbase.jdbcdriver.Driver, jdbc:sonicbase:localhost:9010/test) where id > 1000");

    while (true) {
      String ret = bulkImportStatus((ConnectionProxy) connB);
      System.out.println("status=" + ret);
      if (ret.contains("finished=true") && ret.startsWith("processing")) {
        break;
      }
      Thread.sleep(500);
    }
    System.out.println("Finished load");

    //validate data is in cluster B
    stmt = connB.prepareStatement("select * from persons where id>=0");
    rs = stmt.executeQuery();
    for (int i = 1001; i < 10_000; i++) {
      assertTrue(rs.next(), String.valueOf(i));
      assertEquals(rs.getLong("id"), i);
    }
    assertFalse(rs.next());

  }


  private String bulkImportStatus(ConnectionProxy conn) throws Exception {
    ComObject cobj = new ComObject(2);
    cobj.put(ComObject.Tag.METHOD, "BulkImportManager:getBulkImportProgress");
    cobj.put(ComObject.Tag.DB_NAME, "test");
    ComObject retObj = conn.sendToMaster(cobj);
    ComArray array = retObj.getArray(ComObject.Tag.PROGRESS_ARRAY);
    for (int i = 0; i < array.getArray().size(); i++) {
      ComObject tableObj = (ComObject) array.getArray().get(i);
      String tableName = tableObj.getString(ComObject.Tag.TABLE_NAME);
      long countProcessed = tableObj.getLong(ComObject.Tag.COUNT_LONG);
      long expectedCount = tableObj.getLong(ComObject.Tag.EXPECTED_COUNT);
      boolean finished = tableObj.getBoolean(ComObject.Tag.FINISHED);
      long preProcessCountProcessed = tableObj.getLong(ComObject.Tag.PRE_PROCESS_COUNT_PROCESSED);
      long preProcessExpectedCount = tableObj.getLong(ComObject.Tag.PRE_PROCESS_EXPECTED_COUNT);
      boolean preProcessFinished = tableObj.getBoolean(ComObject.Tag.PRE_PROCESS_FINISHED);
      if (!preProcessFinished) {
        if (tableObj.getString(ComObject.Tag.PRE_PROCESS_EXCEPTION) != null) {
          throw new Exception(tableObj.getString(ComObject.Tag.PRE_PROCESS_EXCEPTION));
        }
        return String.format("preprocessing table=%s, countFinished=%s, percentComplete=%.2f, finished=%b",
            tableName, NumberFormat.getIntegerInstance().format(preProcessCountProcessed),
            (double) preProcessCountProcessed / (double) preProcessExpectedCount * 100d, preProcessFinished);
      }
      else {
        if (tableObj.getString(ComObject.Tag.EXCEPTION) != null) {
          throw new Exception(tableObj.getString(ComObject.Tag.EXCEPTION));
        }
        return String.format("processing table=%s, countFinished=%s, percentComplete=%.2f, finished=%b",
            tableName, NumberFormat.getIntegerInstance().format(countProcessed),
            (double) countProcessed / (double) expectedCount * 100d, finished);
      }
    }

    return "";
  }


  private void startBulkImport(ConnectionProxy conn, String command) {
    ComObject cobj = new ComObject(8);
    cobj.put(ComObject.Tag.DB_NAME, "test");
    int pos = command.indexOf("from");
    int pos1 = command.indexOf("(", pos);
    String tableNames = command.substring(pos + "from".length(), pos1).trim();
    cobj.put(ComObject.Tag.TABLE_NAME, tableNames);

    pos = command.indexOf(", ", pos1);
    String driverName = command.substring(pos1 + 1, pos).trim();
    cobj.put(ComObject.Tag.DRIVER_NAME, driverName);
    pos1 = command.indexOf(", ", pos + 1);
    int pos2 = command.indexOf(")", pos1);
    String jdbcUrl = command.substring(pos + 1, pos1 == -1 ? pos2 : pos1).trim();
    cobj.put(ComObject.Tag.CONNECT_STRING, jdbcUrl);
    String user = null;
    String password = null;
    int endParenPos = pos2;
    if (pos1 != -1) {
      //has user/password
      pos = command.indexOf(", ", pos1);
      user = command.substring(pos1 + 1, pos).trim();
      endParenPos = command.indexOf(")", pos);
      password = command.substring(pos + 1, endParenPos).trim();
      cobj.put(ComObject.Tag.USER, user);
      cobj.put(ComObject.Tag.PASSWORD, password);
    }
    String whereClause = command.substring(endParenPos + 1).trim();
    if (whereClause.length() != 0) {
      if (tableNames.contains(",")) {
        throw new DatabaseException("You cannot have a where clause with multiple tables");
      }
      cobj.put(ComObject.Tag.WHERE_CLAUSE, whereClause);
    }

    cobj.put(ComObject.Tag.METHOD, "BulkImportManager:startBulkImport");

    conn.sendToMaster(cobj);

  }
}
