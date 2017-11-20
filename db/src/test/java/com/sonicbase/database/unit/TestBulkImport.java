/* © 2017 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.database.unit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.ComArray;
import com.sonicbase.common.ComObject;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.research.socket.NettyServer;
import com.sonicbase.server.DatabaseServer;
import org.apache.commons.io.IOUtils;
import org.codehaus.plexus.util.FileUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

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

  @BeforeClass
  public void beforeClass() throws IOException, InterruptedException, SQLException, ClassNotFoundException {
    try {
      String configStr = IOUtils.toString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-2-servers-a.json")), "utf-8");
      ObjectMapper mapper = new ObjectMapper();
      final ObjectNode config = (ObjectNode) mapper.readTree(configStr);

      FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db"));

      ArrayNode array = new ArrayNode(JsonNodeFactory.instance);
      array.add(DatabaseServer.FOUR_SERVER_LICENSE);
      config.put("licenseKeys", array);

      DatabaseClient.getServers().clear();

      final DatabaseServer[] dbServers = new DatabaseServer[4];

      String role = "primaryMaster";


      final CountDownLatch latch = new CountDownLatch(4);
      final NettyServer serverA1 = new NettyServer(128);
      Thread thread = new Thread(new Runnable(){
        @Override
        public void run() {
          serverA1.startServer(new String[]{"-port", String.valueOf(9010), "-host", "localhost",
              "-mport", String.valueOf(9010), "-mhost", "localhost", "-cluster", "2-servers-a", "-shard", String.valueOf(0)}, "db/src/main/resources/config/config-2-servers-a.json", true);
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

      final NettyServer serverA2 = new NettyServer(128);
      thread = new Thread(new Runnable(){
        @Override
        public void run() {
          serverA2.startServer(new String[]{"-port", String.valueOf(9060), "-host", "localhost",
              "-mport", String.valueOf(9060), "-mhost", "localhost", "-cluster", "2-servers-a", "-shard", String.valueOf(1)}, "db/src/main/resources/config/config-2-servers-a.json", true);
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

      final NettyServer serverB1 = new NettyServer(128);
      thread = new Thread(new Runnable(){
        @Override
        public void run() {
          serverB1.startServer(new String[]{"-port", String.valueOf(9110), "-host", "localhost",
              "-mport", String.valueOf(9110), "-mhost", "localhost", "-cluster", "2-servers-b", "-shard", String.valueOf(0)}, "db/src/main/resources/config/config-2-servers-b.json", true);
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

      final NettyServer serverB2 = new NettyServer(128);
      thread = new Thread(new Runnable(){
        @Override
        public void run() {
          serverB2.startServer(new String[]{"-port", String.valueOf(9160), "-host", "localhost",
              "-mport", String.valueOf(9160), "-mhost", "localhost", "-cluster", "2-servers-b", "-shard", String.valueOf(1)}, "db/src/main/resources/config/config-2-servers-b.json", true);
          latch.countDown();
        }
      });
      thread.start();

      while (true) {
        if (serverA1.isRunning() && serverA2.isRunning() && serverB1.isRunning() && serverB2.isRunning()) {
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

      connA = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9010", "user", "password");

      ((ConnectionProxy) connA).getDatabaseClient().createDatabase("test");

      connA.close();

      connA = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9010/test", "user", "password");
      clientA = ((ConnectionProxy)connA).getDatabaseClient();
      clientA.syncSchema();

      connB = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9110", "user", "password");

      ((ConnectionProxy) connB).getDatabaseClient().createDatabase("test");

      connB.close();

      connB = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9110/test", "user", "password");
      clientB = ((ConnectionProxy)connB).getDatabaseClient();
      clientB.syncSchema();

      //
      PreparedStatement stmt = connA.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
      stmt.executeUpdate();

      stmt = connB.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
      stmt.executeUpdate();

      //rebalance
      for (DatabaseServer server : dbServers) {
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

      clientA.beginRebalance("test", "persons", "_1__primarykey");


      while (true) {
        if (clientA.isRepartitioningComplete("test")) {
          break;
        }
        Thread.sleep(200);
      }

      for (DatabaseServer server : dbServers) {
        server.shutdownRepartitioner();
      }
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
    assertEquals(rs.getLong(1), 10_000);

    stmt = connA.prepareStatement("select * from persons where (id < 2251) and (id > 1000)");
    rs = stmt.executeQuery();
    for (int i = 1001; i < 2251; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), i);
    }
    assertFalse(rs.next());


    stmt = connA.prepareStatement("select * from persons where id>=0");
    rs = stmt.executeQuery();
    for (int i = 0; i < 10_000; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), i);
    }
    assertFalse(rs.next());

    stmt = connA.prepareStatement("select * from persons where (id >= 2500 and id < 3750)");
    rs = stmt.executeQuery();
    for (int i = 2500; i < 3750; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), i);
    }
    assertFalse(rs.next());

    stmt = connA.prepareStatement("select * from persons where (id >= 2500 and id < 3750) and id > 3000");
    rs = stmt.executeQuery();
    for (int i = 3001; i < 3750; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), i);
    }
    assertFalse(rs.next());

    stmt = connA.prepareStatement("select * from persons where (id >= 2500 and id < 3750) and id > 1000");
    rs = stmt.executeQuery();
    for (int i = 2500; i < 3750; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), i);
    }
    assertFalse(rs.next());



    startBulkImport((ConnectionProxy) connB, "start bulk import from persons(com.sonicbase.jdbcdriver.Driver, jdbc:sonicbase:127.0.0.1:9010/test)");

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
    assertEquals(rs.getLong(1), 10_000);

    stmt = connA.prepareStatement("select * from persons where (id >= 3501 and id < 4751) and (id > 1000)");
    rs = stmt.executeQuery();
    for (int i = 3501; i < 4_751; i++) {
      assertTrue(rs.next(), String.valueOf(i));
      assertEquals(rs.getLong("id"), i);
    }
    assertFalse(rs.next());

    startBulkImport((ConnectionProxy) connB, "start bulk import from persons(com.sonicbase.jdbcdriver.Driver, jdbc:sonicbase:127.0.0.1:9010/test) where id > 1000");

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


  public String bulkImportStatus(ConnectionProxy conn) throws Exception {
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.method, "getBulkImportProgress");
    cobj.put(ComObject.Tag.dbName, "test");
    byte[] bytes = conn.sendToMaster(cobj);
    ComObject retObj = new ComObject(bytes);

    ComArray array = retObj.getArray(ComObject.Tag.progressArray);
    for (int i = 0; i < array.getArray().size(); i++) {
      ComObject tableObj = (ComObject) array.getArray().get(i);
      String tableName = tableObj.getString(ComObject.Tag.tableName);
      long countProcessed = tableObj.getLong(ComObject.Tag.countLong);
      long expectedCount = tableObj.getLong(ComObject.Tag.expectedCount);
      boolean finished = tableObj.getBoolean(ComObject.Tag.finished);
      long preProcessCountProcessed = tableObj.getLong(ComObject.Tag.prePocessCountProcessed);
      long preProcessExpectedCount = tableObj.getLong(ComObject.Tag.preProcessExpectedCount);
      boolean preProcessFinished = tableObj.getBoolean(ComObject.Tag.preProcessFinished);
      if (!preProcessFinished) {
        if (tableObj.getString(ComObject.Tag.preProcessException) != null) {
          throw new Exception(tableObj.getString(ComObject.Tag.preProcessException));
        }
        return String.format("preprocessing table=%s, countFinished=%s, percentComplete=%.2f, finished=%b",
            tableName, NumberFormat.getIntegerInstance().format(preProcessCountProcessed),
            (double) preProcessCountProcessed / (double) preProcessExpectedCount * 100d, preProcessFinished);
      }
      else {
        if (tableObj.getString(ComObject.Tag.exception) != null) {
          throw new Exception(tableObj.getString(ComObject.Tag.exception));
        }
        return String.format("processing table=%s, countFinished=%s, percentComplete=%.2f, finished=%b",
            tableName, NumberFormat.getIntegerInstance().format(countProcessed),
            (double) countProcessed / (double) expectedCount * 100d, finished);
      }
    }

    return "";
  }


  public void startBulkImport(ConnectionProxy conn, String command) {
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.dbName, "test");
    int pos = command.indexOf("from");
    int pos1 = command.indexOf("(", pos);
    String tableNames = command.substring(pos + "from".length(), pos1).trim();
    cobj.put(ComObject.Tag.tableName, tableNames);

    pos = command.indexOf(", ", pos1);
    String driverName = command.substring(pos1 + 1, pos).trim();
    cobj.put(ComObject.Tag.driverName, driverName);
    pos1 = command.indexOf(", ", pos + 1);
    int pos2 = command.indexOf(")", pos1);
    String jdbcUrl = command.substring(pos + 1, pos1 == -1 ? pos2 : pos1).trim();
    cobj.put(ComObject.Tag.connectString, jdbcUrl);
    String user = null;
    String password = null;
    int endParenPos = pos2;
    if (pos1 != -1) {
      //has user/password
      pos = command.indexOf(", ", pos1);
      user = command.substring(pos1 + 1, pos).trim();
      endParenPos = command.indexOf(")", pos);
      password = command.substring(pos + 1, endParenPos).trim();
      cobj.put(ComObject.Tag.user, user);
      cobj.put(ComObject.Tag.password, password);
    }
    String whereClause = command.substring(endParenPos + 1).trim();
    if (whereClause.length() != 0) {
      if (tableNames.contains(",")) {
        throw new DatabaseException("You cannot have a where clause with multiple tables");
      }
      cobj.put(ComObject.Tag.whereClause, whereClause);
    }

    cobj.put(ComObject.Tag.method, "startBulkImport");

    conn.sendToMaster(cobj);

  }
}
