package com.sonicbase.accept.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.ComArray;
import com.sonicbase.common.ComObject;
import com.sonicbase.common.Config;
import com.sonicbase.common.Record;
import com.sonicbase.index.Index;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.schema.FieldSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.server.DatabaseServer;
import com.sonicbase.server.NettyServer;
import com.sonicbase.streams.Message;
import com.sonicbase.streams.StreamManager;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.testng.Assert.*;

public class TestStreams {


  private Connection connA;
  private Connection connB;
  private DatabaseClient clientA;
  private DatabaseClient clientB;
  private com.sonicbase.server.DatabaseServer[] dbServers;
  NettyServer serverA1;
  NettyServer serverA2;
  NettyServer serverB1;
  NettyServer serverB2;

  @AfterClass(alwaysRun = true)
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

    System.out.println("client refCount=" + DatabaseClient.clientRefCount.get());
    for (DatabaseClient client : DatabaseClient.allClients) {
      System.out.println("Stack:\n" + client.getAllocatedStack());
    }

  }

  @BeforeClass
  public void beforeClass() throws IOException, InterruptedException, SQLException, ClassNotFoundException {
    try {
      System.setProperty("log4j.configuration", "test-log4j.xml");

      FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db"));

      DatabaseClient.getServers().clear();

      dbServers = new com.sonicbase.server.DatabaseServer[4];

      String role = "primaryMaster";


      Config.copyConfig("2-servers-a-producer");

      final CountDownLatch latch = new CountDownLatch(4);
      serverA1 = new NettyServer(128);
      Thread thread = new Thread(() -> {
        serverA1.startServer(new String[]{"-port", String.valueOf(9010), "-host", "localhost",
            "-mport", String.valueOf(9010), "-mhost", "localhost", "-shard",
            String.valueOf(0)});
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
            "-mport", String.valueOf(9060), "-mhost", "localhost", "-shard", String.valueOf(1)});
        latch.countDown();
      });
      thread.start();

      while (true) {
        if (serverA2.isRunning()) {
          break;
        }
        Thread.sleep(100);
      }

      Config.copyConfig("2-servers-b-consumer");

      serverB1 = new NettyServer(128);
      thread = new Thread(() -> {
        serverB1.startServer(new String[]{"-port", String.valueOf(9110), "-host", "localhost",
            "-mport", String.valueOf(9110), "-mhost", "localhost", "-shard", String.valueOf(0)});
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
            "-mport", String.valueOf(9160), "-mhost", "localhost", "-shard", String.valueOf(1)});
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

      ComObject cobj = new ComObject(1);
      ((ConnectionProxy)connA).sendToMaster("MonitorManager:initMonitoringTables", cobj);
      ((ConnectionProxy)connA).sendToMaster("OSStatsManager:initMonitoringTables", cobj);

      for (int shard = 0; shard < ((ConnectionProxy)connA).getShardCount(); shard++) {
        for (int replica = 0; replica < ((ConnectionProxy)connA).getReplicaCount(); replica++) {
          ((ConnectionProxy)connA).send("MonitorManager:initConnection", shard, replica, cobj, ConnectionProxy.Replica.SPECIFIED);
          ((ConnectionProxy)connA).send("OSStatsManager:initConnection", shard, replica, cobj, ConnectionProxy.Replica.SPECIFIED);
          ((ConnectionProxy)connA).send("StreamManager:initConnection", shard, replica, cobj, ConnectionProxy.Replica.SPECIFIED);
        }
      }

      ((ConnectionProxy)connB).sendToMaster("MonitorManager:initMonitoringTables", cobj);
      ((ConnectionProxy)connB).sendToMaster("OSStatsManager:initMonitoringTables", cobj);

      for (int shard = 0; shard < ((ConnectionProxy)connB).getShardCount(); shard++) {
        for (int replica = 0; replica < ((ConnectionProxy)connB).getReplicaCount(); replica++) {
          ((ConnectionProxy)connB).send("MonitorManager:initConnection", shard, replica, cobj, ConnectionProxy.Replica.SPECIFIED);
          ((ConnectionProxy)connB).send("OSStatsManager:initConnection", shard, replica, cobj, ConnectionProxy.Replica.SPECIFIED);
          ((ConnectionProxy)connB).send("StreamManager:initConnection", shard, replica, cobj, ConnectionProxy.Replica.SPECIFIED);
        }
      }

      cobj = new ComObject(1);
      cobj.put(ComObject.Tag.METHOD, "StreamManager:startStreaming");

      for (int shard = 0; shard < clientA.getShardCount(); shard++) {
        clientA.send(null, shard, 0, cobj, DatabaseClient.Replica.ALL);
      }

      for (int shard = 0; shard < clientB.getShardCount(); shard++) {
        clientB.send(null, shard, 0, cobj, DatabaseClient.Replica.ALL);
      }

      //
      PreparedStatement stmt = connA.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), date DATE, time TIME, timestamp TIMESTAMP, PRIMARY KEY (id))");
      stmt.executeUpdate();

      stmt = connA.prepareStatement("create table nokey (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20))");
      stmt.executeUpdate();

      stmt = connB.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), date DATE, time TIME, timestamp TIMESTAMP,  PRIMARY KEY (id))");
      stmt.executeUpdate();

      stmt = connB.prepareStatement("create table nokey (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20))");
      stmt.executeUpdate();


      for (DatabaseServer server : dbServers) {
        server.shutdownRepartitioner();
      }

      Thread.sleep(10_000);

      for (int i = 0; i < 10; i++) {
        stmt = connA.prepareStatement("insert into persons (id, socialSecurityNumber, relatives, restricted, gender, id3, date, time, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
        stmt.setLong(1, i);
        stmt.setString(2, "933-28-" + i);
        stmt.setString(3, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
        stmt.setBoolean(4, false);
        stmt.setString(5, "m");
        stmt.setLong(6, i + 1000);
        stmt.setDate(7, new Date(1975 - 1900, 11, 12));
        stmt.setTime(8, new Time(12, 10, 9));
        stmt.setTimestamp(9, new Timestamp(1975 - 1900, 11, 12, 12, 10, 9, 0));
        assertEquals(stmt.executeUpdate(), 1);
      }

      Thread.sleep(10_000);

    }
    catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  public void serialize() {
    ComObject cobj = new ComObject(2);
    ComArray array = cobj.putArray(ComObject.Tag.MESSAGES, ComObject.Type.STRING_TYPE, 100);
    for (int i = 0; i < 100; i++) {
      array.add(String.valueOf(i));
    }

    byte[] bytes = cobj.serialize();
    ComObject retObj = new ComObject(bytes);
    ComArray retArray = retObj.getArray(ComObject.Tag.MESSAGES);
    for (int i = 0; i < 100; i++) {
      assertEquals(retArray.getArray().get(i), String.valueOf(i));
    }
    assertEquals(retArray.getArray().size(), 100);
  }

  @Test
  public void test() throws InterruptedException, SQLException {

    Thread.sleep(20_000);

    PreparedStatement stmt = connB.prepareStatement("select * from persons");
    ResultSet ret = stmt.executeQuery();
    for (int i = 0; i < 10; i++) {
      assertTrue(ret.next(), String.valueOf(i));
      assertEquals(ret.getInt("id"), i);
    }
    assertFalse(ret.next());

    stmt = connA.prepareStatement("update persons set relatives='xxx' where id=0");
    stmt.executeUpdate();

    Thread.sleep(20_000);

    stmt = connB.prepareStatement("select * from persons");
    ret = stmt.executeQuery();
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 0);
    assertEquals(ret.getString("relatives"), "xxx");

    stmt = connA.prepareStatement("delete from persons where id=0");
    stmt.executeUpdate();

    Thread.sleep(10_000);

    stmt = connB.prepareStatement("select * from persons");
    ret = stmt.executeQuery();
    for (int i = 1; i < 10; i++) {
      assertTrue(ret.next(), String.valueOf(i));
      assertEquals(ret.getInt("id"), i);
    }
    assertFalse(ret.next());

  }

  @Test
  public void testNoKey() throws InterruptedException, SQLException {

    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 2; j++) {
        PreparedStatement stmt = connA.prepareStatement("insert into nokey (id, id2, socialSecurityNumber) VALUES (?, ?, ?)");
        stmt.setLong(1, i);
        stmt.setLong(2, 0);
        stmt.setString(3, "933-28-" + i + "-" + j);
        assertEquals(stmt.executeUpdate(), 1);
      }
    }

    Thread.sleep(20_000);

    PreparedStatement stmt = connB.prepareStatement("select * from nokey order by id, id2, socialsecuritynumber");
    ResultSet ret = stmt.executeQuery();
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 2; j++) {
        assertTrue(ret.next(), String.valueOf(i));
        assertEquals(ret.getInt("id"), i);
        assertEquals(ret.getInt("id2"), 0);
        assertEquals(ret.getString("socialsecuritynumber"), "933-28-" + i + "-" + j);
      }
    }
    assertFalse(ret.next());

    stmt = connA.prepareStatement("update nokey set socialsecuritynumber='xxx' where id=0 and id2=0 and socialsecuritynumber='933-28-0-1'");
    stmt.executeUpdate();

    Thread.sleep(10_000);

    stmt = connB.prepareStatement("select * from nokey order by id,id2,socialsecuritynumber ");
    ret = stmt.executeQuery();
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 0);
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getString("socialsecuritynumber"), "933-28-0-0");
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 0);
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getString("socialsecuritynumber"), "xxx");

    stmt = connA.prepareStatement("delete from nokey where id=0");
    stmt.executeUpdate();

    Thread.sleep(10_000);

    stmt = connB.prepareStatement("select * from nokey order by id,id2,socialsecuritynumber");
    ret = stmt.executeQuery();
    for (int i = 1; i < 10; i++) {
      for (int j = 0; j < 2; j++) {
        assertTrue(ret.next(), String.valueOf(i));
        assertEquals(ret.getInt("id"), i);
        assertEquals(ret.getInt("id2"), 0);
        assertEquals(ret.getString("socialsecuritynumber"), "933-28-" + i + "-" + j);
      }
    }
    assertFalse(ret.next());

  }

  @Test
  public void testDates() throws SQLException, IOException {
    StringBuilder builder = new StringBuilder();
    TableSchema tableSchema = clientA.getCommon().getTables("test").get("persons");
    Index index = dbServers[0].getIndices().get("test").getIndices().get("persons").get("_primarykey");
    Map.Entry<Object[], Object> entry = index.lastEntry();
    byte[][] bytes = dbServers[0].getAddressMap().fromUnsafeToRecords(entry.getValue());
    Record record = new Record("test", clientA.getCommon(), bytes[0]);

    builder.append("{");
    StreamManager.getJsonFromRecord(builder, tableSchema, record);
    builder.append("}");

    ObjectMapper mapper = new ObjectMapper();
    ObjectNode node = (ObjectNode) mapper.readTree(builder.toString());
    node.remove("_sonicbase_sequence0");
    node.remove("_sonicbase_sequence1");
    node.remove("_sonicbase_sequence2");
    node.remove("_sonicbase_id");

    assertEquals(node.toString(), "{\"id\":9,\"socialsecuritynumber\":\"933-28-9\",\"relatives\":\"12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901\",\"restricted\":false,\"gender\":\"m\",\"date\":\"1975-12-12 07:00:00.000+0000\",\"time\":\"19:10:09.000+0000\",\"timestamp\":\"1975-12-12 19:10:09.000+0000\"}");
    System.out.println(node.toString());
    JsonNode json = mapper.readTree(builder.toString());
    List<FieldSchema> fields = clientA.getCommon().getTables("test").get("persons").getFields();
//    Object[] ret = StreamManager.getCurrRecordFromJson(json, fields);
//
//    Object field = ret[tableSchema.getFieldOffset("date")];
//    assertEquals(field, new Date(1975 - 1900, 11, 12));
//    assertEquals(ret[tableSchema.getFieldOffset("time")], new Time(12, 10, 9));
//    assertEquals(ret[tableSchema.getFieldOffset("timestamp")], new Timestamp(1975 - 1900, 11, 12, 12, 10, 9, 0));
  }
}
