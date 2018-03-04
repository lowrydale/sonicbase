/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.Logger;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.streams.LocalProducer;
import org.apache.commons.io.IOUtils;
import org.codehaus.plexus.util.FileUtils;
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

import static org.testng.Assert.assertEquals;

public class TestStoredProcedures {

  private Connection conn;
  List<Long> ids = new ArrayList<>();
  DatabaseServer[] dbServers;

  @AfterClass
  public void afterClass() {
    for (DatabaseServer server : dbServers) {
      server.shutdown();
    }
    Logger.queue.clear();
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
      dbServers[shard].setConfig(config, "4-servers", "localhost", 9010 + (50 * shard), true, new AtomicBoolean(true), null, true);
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

    Logger.setReady(false);

    DatabaseClient client = ((ConnectionProxy)conn).getDatabaseClient();


    PreparedStatement stmt = conn.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, id3 BIGINT, id4 BIGINT, id5 BIGINT, num DOUBLE, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), timestamp TIMESTAMP, PRIMARY KEY (id))");
    stmt.executeUpdate();

    //test upsert

    LocalProducer.queue.clear();

    for (int i = 0; i < 4; i++) {
      stmt = conn.prepareStatement("insert into persons (id, id2, id3, id4, id5, num, socialSecurityNumber, relatives, restricted, gender, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
      stmt.setLong(1, i);
      stmt.setLong(2, (i + 100) % 2);
      stmt.setLong(3, -1 * (i + 100));
      stmt.setLong(4, (i + 100) % 3);
      stmt.setDouble(5, 1);
      stmt.setDouble(6, i * 0.5);
      stmt.setString(7, "s" + i);
      stmt.setString(8, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
      stmt.setBoolean(9, false);
      stmt.setString(10, "m");
      Timestamp timestamp = new Timestamp(2000 + i - 1900, i % 12 - 1, i % 30, i % 24, i % 60, i % 60, 0);
      stmt.setTimestamp(11, timestamp);
      assertEquals(stmt.executeUpdate(), 1);
      ids.add((long) i);
    }

    Thread.sleep(10000);

    executor.shutdownNow();
  }

  @Test
  public void test() throws SQLException {
    String tableName = null;
    try {
      String query = "call procedure 'com.sonicbase.procedure.MyStoredProcedure2', 'select * from persons where id>1 and gender=\"m\"', 100000";
      PreparedStatement procedureStmt = conn.prepareStatement(query);
      ResultSet procedureRs = procedureStmt.executeQuery();
      if (procedureRs.next()) {
        tableName = procedureRs.getString("tableName");
        PreparedStatement resultsStmt = conn.prepareStatement("select * from " + tableName);
        ResultSet rsultsRs = resultsStmt.executeQuery();
        while (rsultsRs.next()) {
          System.out.println(
              "id=" + rsultsRs.getLong("id") +
                  ", socialsecuritynumber=" + rsultsRs.getString("socialsecuritynumber") +
                  ", gender=" + rsultsRs.getString("gender"));
        }
        rsultsRs.close();
        resultsStmt.close();

      }
      procedureRs.close();
      procedureStmt.close();
    }
    finally {
      try {
        if (tableName != null) {
          PreparedStatement stmt = conn.prepareStatement("drop table " + tableName);
          stmt.executeUpdate();
          stmt.close();
        }
      }
      catch (Exception e) {
        e.printStackTrace();
      }

      conn.close();
    }
  }

}
