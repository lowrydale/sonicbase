package com.sonicbase.accept.database;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.ComObject;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.server.DatabaseServer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.testng.Assert.*;

public class TestDataTypes2 {

  private Connection conn;
  private int recordCount = 10;
  List<Long> ids = new ArrayList<>();

  DatabaseServer[] dbServers;

  @AfterClass
  public void afterClass() throws SQLException {
    conn.close();

    for (DatabaseServer server : dbServers) {
      server.shutdown();
    }

  }

  @BeforeClass
  public void beforeClass() throws Exception {
    // Logger.disable();

    String configStr = IOUtils.toString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-4-servers.json")), "utf-8");
    ObjectMapper mapper = new ObjectMapper();
    final ObjectNode config = (ObjectNode) mapper.readTree(configStr);

    FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db"));

    DatabaseClient.getServers().clear();

    dbServers = new DatabaseServer[4];
    ThreadPoolExecutor executor = new ThreadPoolExecutor(32, 32, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

    String role = "primaryMaster";

    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < dbServers.length; i++) {
      final int shard = i;
      dbServers[shard] = new DatabaseServer();
      dbServers[shard].setConfig(config, "4-servers", "localhost", 9010 + (50 * shard), true, new AtomicBoolean(true), new AtomicBoolean(true),null);
      dbServers[shard].setRole(role);
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

    executor.shutdownNow();

    PreparedStatement stmt = conn.prepareStatement("create table query_stats(db_name VARCHAR, id BIGINT, query VARCHAR, lat_avg DOUBLE,  lat_95 DOUBLE, lat_99 DOUBLE, lat_999 DOUBLE, lat_max DOUBLE, PRIMARY KEY (id))");
    stmt.executeUpdate();


    //test upsert

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into query_stats (id, query, lat_avg, lat_95, lat_99, lat_999, lat_max) VALUES (?, ?, ?, ?, ?, ?, ?)");
      stmt.setLong(1, i);
      stmt.setString(2, "query");
      stmt.setDouble(3, i + 1000111222333.2d);
      stmt.setDouble(4, i + 2000111222333.2d);
      stmt.setDouble(5, i + 3000111222333.2d);
      stmt.setDouble(6, i + 4000111222333.2d);
      stmt.setDouble(7, i + 5000111222333.2d);
      assertEquals(stmt.executeUpdate(), 1);
      ids.add((long) i);
    }

    client = new DatabaseClient("localhost", 9010, -1, -1, true);

    client.beginRebalance("test");

    while (true) {
      if (client.isRepartitioningComplete("test")) {
        break;
      }
      Thread.sleep(1000);
    }

  }

  @Test
  public void test() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from query_stats");
    ResultSet rs = stmt.executeQuery();

    for (int i = 0; i < recordCount; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), i);
      assertEquals(rs.getString("query"), "query");
      assertEquals(rs.getDouble("lat_avg"), i + 1000111222333.2d);
      assertEquals(rs.getDouble("lat_95"), i + 2000111222333.2d);
      assertEquals(rs.getDouble("lat_99"), i + 3000111222333.2d);
      assertEquals(rs.getDouble("lat_999"), i + 4000111222333.2d);
      assertEquals(rs.getDouble("lat_max"), i + 5000111222333.2d);
    }
    assertFalse(rs.next());
  }

  @Test
  public void testComObject() {
    ComObject cobj = new ComObject();
    long duration = 40404022;
    cobj.put(ComObject.Tag.DB_NAME,"db");
    cobj.put(ComObject.Tag.ID, 100);
    cobj.put(ComObject.Tag.DURATION, duration);

    byte[] bytes = cobj.serialize();
    cobj = new ComObject(bytes);
    assertEquals(cobj.getString(ComObject.Tag.DB_NAME), "db");
    assertEquals((long)cobj.getLong(ComObject.Tag.ID), 100);

    assertEquals((long)cobj.getLong(ComObject.Tag.DURATION), duration);
  }
}


