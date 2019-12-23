/* © 2019 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.accept.server;

import com.sonicbase.bench.SimpleJdbcBenchmark;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.Config;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.server.DatabaseServer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.testng.Assert.*;

public class TestNativePartitionedTreeImpl {

  private DatabaseClient clusterClient;

  @BeforeClass
  public void beforeClass() {
    System.setProperty("log4j.configuration", "test-log4j.xml");
  }

  private Connection initDb(String createTableStatement) throws IOException, InterruptedException, ExecutionException, ClassNotFoundException, SQLException {
    String configStr = IOUtils.toString(new BufferedInputStream(SimpleJdbcBenchmark.class.getResourceAsStream("/config/config-2-servers-a.yaml")), "utf-8");
    Config config = new Config(configStr);

    FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db-data"));

    final DatabaseServer[] dbServers = new DatabaseServer[2];
    ThreadPoolExecutor executor = new ThreadPoolExecutor(32, 32, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

    String role = "primaryMaster";

    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < dbServers.length; i++) {
      final int shard = i;
      futures.add(executor.submit((Callable) () -> {
        String role1 = "primaryMaster";

        dbServers[shard] = new DatabaseServer();
        Config.copyConfig("4-servers");
        dbServers[shard].setConfig(config, "localhost", 9010 + (50 * shard), true, new AtomicBoolean(true), new AtomicBoolean(true), null, false);
        dbServers[shard].setRole(role1);
        return null;
      }));
    }
    for (Future future : futures) {
      future.get();
    }

//    for (DatabaseServer server : dbServers) {
//      server.shutdownRepartitioner();
//    }

    Class.forName("com.sonicbase.jdbcdriver.Driver");

    DatabaseClient client = new DatabaseClient("localhost", 9010, -1, -1, true);

    client.createDatabase("db");
    Connection conn = DriverManager.getConnection("jdbc:sonicbase:localhost:9010/db", "user", "password");

    clusterClient = ((ConnectionProxy) conn).getDatabaseClient();

    PreparedStatement stmt = conn.prepareStatement(createTableStatement);
    stmt.executeUpdate();
    return conn;
  }

  @Test
  public void test() throws IOException, InterruptedException, ExecutionException, ClassNotFoundException, SQLException {
    Connection conn = initDb("create table Persons (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");

    int recordCount = 100;
    for (int i = 0; i < recordCount; i++) {
      PreparedStatement stmt1 = conn.prepareStatement("insert into persons (id) VALUES (?)");
      stmt1.setLong(1, i);
      stmt1.executeUpdate();
    }

    PreparedStatement stmt2 = conn.prepareStatement("select persons.id from persons where persons.id>=0");                                              //
    ResultSet ret = stmt2.executeQuery();

    for (int i = 0; i < recordCount; i++) {
      assertTrue(ret.next(), "" + i);
      assertEquals(ret.getLong("id"), i);
    }
    assertFalse(ret.next());
  }

  @Test
  public void testInt() throws IOException, InterruptedException, ExecutionException, ClassNotFoundException, SQLException {
    Connection conn = initDb("create table Persons (id INTEGER, PRIMARY KEY (id))");

    int recordCount = 100;
    for (int i = 0; i < recordCount; i++) {
      PreparedStatement stmt1 = conn.prepareStatement("insert into persons (id) VALUES (?)");
      stmt1.setInt(1, i);
      stmt1.executeUpdate();
    }

    PreparedStatement stmt2 = conn.prepareStatement("select persons.id from persons where persons.id>=0");                                              //
    ResultSet ret = stmt2.executeQuery();

    for (int i = 0; i < recordCount; i++) {
      assertTrue(ret.next(), "" + i);
      assertEquals(ret.getInt("id"), i);
    }
    assertFalse(ret.next());
  }

  @Test
  public void testVarchar() throws IOException, InterruptedException, ExecutionException, ClassNotFoundException, SQLException {
    Connection conn = initDb("create table Persons (id VARCHAR, PRIMARY KEY (id))");

    int recordCount = 100;
    for (int i = 0; i < recordCount; i++) {
      PreparedStatement stmt1 = conn.prepareStatement("insert into persons (id) VALUES (?)");
      stmt1.setString(1, pad(String.valueOf(i)));
      stmt1.executeUpdate();
    }

    PreparedStatement stmt2 = conn.prepareStatement("select persons.id from persons where persons.id>=0");                                              //
    ResultSet ret = stmt2.executeQuery();

    for (int i = 0; i < recordCount; i++) {
      assertTrue(ret.next(), "" + i);
      assertEquals(ret.getString("id"), pad(String.valueOf(i)));
    }
    assertFalse(ret.next());
  }

  @Test
  public void testBigDecimal() throws IOException, InterruptedException, ExecutionException, ClassNotFoundException, SQLException {
    Connection conn = initDb("create table Persons (id NUMERIC, PRIMARY KEY (id))");

    int recordCount = 100;
    for (int i = 0; i < recordCount; i++) {
      PreparedStatement stmt1 = conn.prepareStatement("insert into persons (id) VALUES (?)");
      stmt1.setBigDecimal(1, new BigDecimal(i));
      stmt1.executeUpdate();
    }

    PreparedStatement stmt2 = conn.prepareStatement("select persons.id from persons where persons.id>=0");                                              //
    ResultSet ret = stmt2.executeQuery();

    for (int i = 0; i < recordCount; i++) {
      assertTrue(ret.next(), "" + i);
      assertEquals(ret.getBigDecimal("id"), new BigDecimal(i));
    }
    assertFalse(ret.next());
  }

  @Test
  public void testDouble() throws IOException, InterruptedException, ExecutionException, ClassNotFoundException, SQLException {
    Connection conn = initDb("create table Persons (id DOUBLE, PRIMARY KEY (id))");

    int recordCount = 100;
    for (int i = 0; i < recordCount; i++) {
      PreparedStatement stmt1 = conn.prepareStatement("insert into persons (id) VALUES (?)");
      stmt1.setDouble(1, i);
      stmt1.executeUpdate();
    }

    PreparedStatement stmt2 = conn.prepareStatement("select persons.id from persons where persons.id>=0");                                              //
    ResultSet ret = stmt2.executeQuery();

    for (int i = 0; i < recordCount; i++) {
      assertTrue(ret.next(), "" + i);
      assertEquals(ret.getDouble("id"), (double)i);
    }
    assertFalse(ret.next());
  }

  @Test(enabled=false)
  public void testTimestamp() throws IOException, InterruptedException, ExecutionException, ClassNotFoundException, SQLException {
    Connection conn = initDb("create table Persons (id TIMESTAMP, PRIMARY KEY (id))");

    int recordCount = 100;
    for (int i = 0; i < recordCount; i++) {
      PreparedStatement stmt1 = conn.prepareStatement("insert into persons (id) VALUES (?)");
      Timestamp t = new Timestamp(i);
      t.setTime(i);
      t.setNanos(i);
      stmt1.setTimestamp(1, t);
      stmt1.executeUpdate();
    }

    PreparedStatement stmt2 = conn.prepareStatement("select persons.id from persons where persons.id>=0");                                              //
    ResultSet ret = stmt2.executeQuery();

    for (int i = 0; i < recordCount; i++) {
      assertTrue(ret.next(), "" + i);
      Timestamp t = ret.getTimestamp("id");
      assertEquals(t.getTime(), (long)i);
      assertEquals(t.getNanos(), i);
    }
    assertFalse(ret.next());
  }

  @Test(enabled = false)
  public void testTime() throws IOException, InterruptedException, ExecutionException, ClassNotFoundException, SQLException {
    Connection conn = initDb("create table Persons (id TIME, PRIMARY KEY (id))");

    int recordCount = 100;
    for (int i = 0; i < recordCount; i++) {
      PreparedStatement stmt1 = conn.prepareStatement("insert into persons (id) VALUES (?)");
      Time t = new Time(0);
      t.setSeconds(i);
      stmt1.setTime(1, t);
      stmt1.executeUpdate();
    }

    PreparedStatement stmt2 = conn.prepareStatement("select persons.id from persons where persons.id>=?");                                              //
    stmt2.setTime(1, new Time(0));
    ResultSet ret = stmt2.executeQuery();

    for (int i = 0; i < recordCount; i++) {
      assertTrue(ret.next(), "" + i);
      Time t = new Time(0);
      t.setSeconds(i);
      assertEquals(ret.getTime("id").getTime(), t.getTime());
    }
    assertFalse(ret.next());
  }

  private String pad(String str) {
    while (str.length() < 5) {
      str = 0 + str;
    }
    return str;
  }

  @Test
  public void testMultipe() throws IOException, InterruptedException, ExecutionException, ClassNotFoundException, SQLException {
    Connection conn = initDb("create table Persons (" +
        "id1 BIGINT, " +
        "id2 INTEGER, " +
        "id3 SMALLINT, " +
        "id4 TINYINT, " +
        "id5 DOUBLE, " +
        "id6 REAL, " +
        "id7 VARCHAR, " +
        "id8 DATE, " +
        "id9 TIME, " +
        "id10 TIMESTAMP, " +
        "id11 DECIMAL, " +
        "id12 BOOLEAN, " +
        "id13 BIT, " +
        "PRIMARY KEY (id1, id2, id3, id5, id6, id7, id8, id9, id10, id11))");

    int recordCount = 100_000;
    for (int i = 0; i < recordCount; i++) {
      PreparedStatement stmt1 = conn.prepareStatement("insert into persons (id1, id2, id3, id4, id5, id6, id7, id8, id9, id10, id11, id12, id13) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
      stmt1.setLong(1, i);
      stmt1.setInt(2, i);
      stmt1.setShort(3, (short) i);
      stmt1.setByte(4, (byte) i);
      stmt1.setDouble(5, (double)i);
      stmt1.setFloat(6, (float) i);
      stmt1.setString(7, String.valueOf(i));
      stmt1.setDate(8, new Date(i));
      stmt1.setTime(9, new Time(i));
      stmt1.setTimestamp(10, new Timestamp(i));
      stmt1.setBigDecimal(11, new BigDecimal(i));
      stmt1.setBoolean(12, true);
      stmt1.setByte(13, (byte)1);
      stmt1.executeUpdate();
    }

    clusterClient.beginRebalance("db");


    while (true) {
      if (clusterClient.isRepartitioningComplete("db")) {
        break;
      }
      Thread.sleep(1000);
    }

    while (true) {
      if (clusterClient.isRepartitioningComplete("db")) {
        break;
      }
      Thread.sleep(1000);
    }

    while (true) {
      if (clusterClient.isRepartitioningComplete("db")) {
        break;
      }
      Thread.sleep(1000);
    }


    PreparedStatement stmt2 = conn.prepareStatement("select * from persons where persons.id1>=0");                                              //
    ResultSet ret = stmt2.executeQuery();

    for (int i = 0; i < recordCount; i++) {
      assertTrue(ret.next(), "" + i);
      assertEquals(ret.getLong("id1"), (long)i);
      assertEquals(ret.getInt("id2"), i);
      assertEquals(ret.getShort("id3"), (short)i);
      assertEquals(ret.getByte("id4"), (byte)i);
      assertEquals(ret.getDouble("id5"), (double)i);
      assertEquals(ret.getFloat("id6"), (float)i);
      assertEquals(ret.getString("id7"), String.valueOf(i));
      assertEquals(ret.getDate("id8").toString(), new Date(i).toString());
      assertEquals(ret.getTime("id9").toString(), new Time(i).toString());
      assertEquals(ret.getTimestamp("id10").toString(), new Timestamp(i).toString());
      assertEquals(ret.getBigDecimal("id11").toString(), new BigDecimal(i).toString());
      assertEquals(ret.getBoolean("id12"), true);
      //assertEquals(ret.getByte("id13"), (byte)1);
    }
    assertFalse(ret.next());


    PreparedStatement stmt3 = conn.prepareStatement("select * from persons where persons.id1>=0 order by id1 desc");                                              //
    ret = stmt3.executeQuery();

    for (int i = recordCount - 1; i >= 0; i--) {
      assertTrue(ret.next(), "" + i);
      assertEquals(ret.getLong("id1"), (long)i);
      assertEquals(ret.getInt("id2"), i);
      assertEquals(ret.getShort("id3"), (short)i);
      assertEquals(ret.getByte("id4"), (byte)i);
      assertEquals(ret.getDouble("id5"), (double)i);
      assertEquals(ret.getFloat("id6"), (float)i);
      assertEquals(ret.getString("id7"), String.valueOf(i));
      assertEquals(ret.getDate("id8").toString(), new Date(i).toString());
      assertEquals(ret.getTime("id9").toString(), new Time(i).toString());
      assertEquals(ret.getTimestamp("id10").toString(), new Timestamp(i).toString());
      assertEquals(ret.getBigDecimal("id11").toString(), new BigDecimal(i).toString());
      assertEquals(ret.getBoolean("id12"), true);
      //assertEquals(ret.getByte("id13"), (byte)1);
    }
    assertFalse(ret.next());
  }


}
