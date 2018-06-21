package com.sonicbase.accept.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.Logger;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.server.DatabaseServer;
import com.sonicbase.streams.LocalProducer;
import com.sonicbase.streams.Message;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.*;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.testng.Assert.*;

public class TestDataTypes {

  private Connection conn;
  private int recordCount = 10;
  List<Long> ids = new ArrayList<>();

  com.sonicbase.server.DatabaseServer[] dbServers;

  @AfterClass(alwaysRun = true)
  public void afterClass() throws SQLException {
    conn.close();

    for (com.sonicbase.server.DatabaseServer server : dbServers) {
      server.shutdown();
    }
    Logger.queue.clear();
    System.out.println("client refCount=" + DatabaseClient.clientRefCount.get() + ", sharedClients=" + DatabaseClient.sharedClients.size() + ", class=TestDataTypes");
    for (DatabaseClient client : DatabaseClient.allClients) {
      System.out.println("Stack:\n" + client.getAllocatedStack());
    }
  }

  @BeforeClass
  public void beforeClass() throws Exception {
   // Logger.disable();

    String configStr = IOUtils.toString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-4-servers.json")), "utf-8");
    ObjectMapper mapper = new ObjectMapper();
    final ObjectNode config = (ObjectNode) mapper.readTree(configStr);

    FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db"));

    ArrayNode array = new ArrayNode(JsonNodeFactory.instance);
    array.add(com.sonicbase.server.DatabaseServer.FOUR_SERVER_LICENSE);
    config.put("licenseKeys", array);

    DatabaseClient.getServers().clear();

    dbServers = new com.sonicbase.server.DatabaseServer[4];
    ThreadPoolExecutor executor = new ThreadPoolExecutor(32, 32, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

    String role = "primaryMaster";

    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < dbServers.length; i++) {
      final int shard = i;
      dbServers[shard] = new com.sonicbase.server.DatabaseServer();
      dbServers[shard].setConfig(config, "4-servers", "localhost", 9010 + (50 * shard), true, new AtomicBoolean(true),new AtomicBoolean(true), null, true);
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

    executor.shutdownNow();
  }

  public void initTypes() throws Exception {
    PreparedStatement stmt = conn.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, int INTEGER, bool BOOLEAN, char CHAR, smallInt SMALLINT, tinyInt TINYINT, float FLOAT, double DOUBLE, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), blob BLOB, numeric NUMERIC, decimal DECIMAL, bin VARBINARY(64000), date DATE, time TIME, timestamp TIMESTAMP, bit BIT, real REAL, nchar NCHAR, nvarchar NVARCHAR, longnvarchar LONGNVARCHAR, longvarchar LONGVARCHAR, longvarbinary LONGVARBINARY, PRIMARY KEY (id))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create table Memberships (personId BIGINT, membershipName VARCHAR(20), resortId BIGINT, PRIMARY KEY (personId, membershipName))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create table Resorts (resortId BIGINT, resortName VARCHAR(20), PRIMARY KEY (resortId))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create index socialSecurityNumber on persons(socialSecurityNumber)");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create table date (id DATE, id2 INTEGER, name VARCHAR(20), PRIMARY KEY (id))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create table time (id TIME, id2 INTEGER, name VARCHAR(20), PRIMARY KEY (id))");
    stmt.executeUpdate();


    //test upsert

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into persons (id, socialSecurityNumber, relatives, restricted, gender, int, bool, smallInt, char, float, double, blob, numeric, decimal, bin, date, time, timestamp, tinyint, bit, real, nchar, nvarchar, longnvarchar, longvarchar, longvarbinary) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
      stmt.setLong(1, i);
      stmt.setString(2, "933-28-" + i);
      stmt.setString(3, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
      stmt.setBoolean(4, false);
      stmt.setString(5, "m");
      stmt.setInt(6, i);
      stmt.setBoolean(7, i % 2 == 0);
      stmt.setInt(8, i);
      stmt.setString(9, "char-" + i);
      stmt.setDouble(10, i + 0.2d);
      stmt.setDouble(11, i + 0.2d);
      ByteArrayInputStream in = new ByteArrayInputStream(("testing blob-" + String.valueOf(i)).getBytes("utf-8"));
      stmt.setBlob(12, in);
      stmt.setBigDecimal(13, new BigDecimal(i + ".01"));
      stmt.setBigDecimal(14, new BigDecimal(i + ".01"));
      stmt.setBytes(15, ("testing blob-" + String.valueOf(i)).getBytes("utf-8"));
      Date date = new Date(1920 - 1900, 10, i);
      stmt.setDate(16, date);
      Time time = new Time(10, 12, i);
      stmt.setTime(17, time);
      Timestamp timestamp = new Timestamp(1990 - 1900, 11, 13, 1, 2, i, 0);
      stmt.setTimestamp(18, timestamp);
      stmt.setByte(19, (byte) i);
      stmt.setBoolean(20, i % 2 == 0);
      stmt.setFloat(21, i + 0.2f);
      stmt.setString(22, "933-28-" + i);
      stmt.setString(23, "933-28-" + i);
      stmt.setString(24, "933-28-" + i);
      stmt.setString(25, "933-28-" + i);
      in = new ByteArrayInputStream(("testing blob-" + String.valueOf(i)).getBytes("utf-8"));
      stmt.setBlob(26, in);
      assertEquals(stmt.executeUpdate(), 1);
      ids.add((long) i);
    }

//    int recordsConsumed = 0;
//    LocalConsumer consumer = new LocalConsumer();
//    List<Message> msgs = consumer.receive();
//    recordsConsumed += countRecords(msgs);
//    String body = msgs.get(0).getBody();
//    System.out.println(body);
//    ObjectMapper mapper = new ObjectMapper();
//    ObjectNode dict = (ObjectNode) mapper.readTree(body);
//    assertEquals(dict.get("database").asText(), "test");
//    assertEquals(dict.get("table").asText(), "persons");
//    assertEquals(dict.get("action").asText(), "insert");
//    ObjectNode record = (ObjectNode) dict.withArray("records").get(0);
//    assertNotNull(record.get("_sequence0").asLong());
//    assertNotNull(record.get("_sequence1").asLong());
//    assertNotNull(record.get("_sequence2").asLong());
//    record.remove("_sequence0");
//    record.remove("_sequence1");
//    record.remove("_sequence2");
//    assertJsonEquals(record.toString(),
//        "{\"date\":\"1920-10-31\",\"bool\":true,\"gender\":\"m\",\"longnvarchar\":\"933-28-0\",\"bin\":\"dGVzdGluZyBibG9iLTA=\",\"numeric\":0.01,\"float\":0.2,\"bit\":true,\"smallint\":0,\"nvarchar\":\"933-28-0\",\"longvarbinary\":\"dGVzdGluZyBibG9iLTA=\",\"id\":0,\"timestamp\":\"1990-12-13 01:02:00.0\",\"double\":0.2,\"tinyint\":0,\"real\":0.2,\"relatives\":\"12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901\",\"int\":0,\"longvarchar\":\"933-28-0\",\"socialsecuritynumber\":\"933-28-0\",\"blob\":\"dGVzdGluZyBibG9iLTA=\",\"restricted\":false,\"char\":\"char-0\",\"nchar\":\"933-28-0\",\"time\":\"10:12:00\",\"decimal\":0.01}");
//
//    while (true) {
//      msgs = consumer.receive();
//      if (msgs == null || msgs.size() == 0) {
//        break;
//      }
//      recordsConsumed += countRecords(msgs);
//    }

//    assertEquals(recordsConsumed, recordCount);

    stmt = conn.prepareStatement("insert into Resorts (resortId, resortName) VALUES (?, ?)");
    stmt.setLong(1, 1000);
    stmt.setString(2, "resort-1000");
    assertEquals(stmt.executeUpdate(), 1);

    stmt = conn.prepareStatement("insert into Resorts (resortId, resortName) VALUES (?, ?)");
    stmt.setLong(1, 2000);
    stmt.setString(2, "resort-2000");
    assertEquals(stmt.executeUpdate(), 1);

    for (int i = 0; i < recordCount; i++) {
      for (int j = 0; j < recordCount; j++) {
        stmt = conn.prepareStatement("insert into Memberships (personId, membershipName, resortId) VALUES (?, ?, ?)");
        stmt.setLong(1, i);
        stmt.setString(2, "membership-" + j);
        stmt.setLong(3, new long[]{1000, 2000}[j % 2]);
        assertEquals(stmt.executeUpdate(), 1);
      }
    }


    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into persons (id, id2, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?)");
      stmt.setLong(1, i + 100);
      stmt.setLong(2, (i + 100) % 2);
      stmt.setString(3, "933-28-" + (i % 4));
      stmt.setString(4, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
      stmt.setBoolean(5, false);
      stmt.setString(6, "m");
      int count = stmt.executeUpdate();
      assertEquals(count, 1);
      ids.add((long) (i + 100));
    }

    DatabaseClient client = new DatabaseClient("localhost", 9010, -1, -1, true);

    client.beginRebalance("test", "persons", "_primarykey");

    while (true) {
      if (client.isRepartitioningComplete("test")) {
        break;
      }
      Thread.sleep(1000);
    }

    assertEquals(client.getPartitionSize("test", 0, "persons", "_primarykey"), 9);
    assertEquals(client.getPartitionSize("test", 1, "persons", "_primarykey"), 11);
//    assertEquals(client.getPartitionSize(2, "persons", "_1__primarykey"), 9);
//    assertEquals(client.getPartitionSize(3, "persons", "_1__primarykey"), 8);

    client.shutdown();
  }

  private int countRecords(List<Message> msgs) {
      int count = 0;
      for (Message msg : msgs) {
        try {
          ObjectMapper mapper = new ObjectMapper();
          ObjectNode dict = (ObjectNode) mapper.readTree(msg.getBody());
          ArrayNode array = dict.withArray("records");
          count += array.size();
        }
        catch (Exception e) {
          System.out.println("bad json=" + msg.getBody());
        }
      }
      return count;
  }

  public static void assertJsonEquals(String lhs, String rhs) {
    JsonParser parser = new JsonParser();
    JsonElement o1 = parser.parse(lhs);
    JsonElement o2 = parser.parse(rhs);
    assertEquals(o1, o2);
  }

  @Test
  public void testBasics() throws Exception {

    LocalProducer.queue.clear();

    initTypes();

    //test select returns multiple records with an index using operator '<'
    PreparedStatement stmt = conn.prepareStatement("select id, int, smallint, bool, char, float, double, blob, numeric, decimal, bin, date, time, timestamp, tinyint, bit, real, nchar, nvarchar, longnvarchar, longvarchar, longvarbinary from persons where int < ? and smallint < ? and bool = ? and id < ? and nchar < ? and float < ? and double < ? and tinyint < ? order by longvarchar desc"); //force server select for serialization
    stmt.setInt(1, 5);
    stmt.setShort(2, (short) 5);
    stmt.setBoolean(3, true);
    stmt.setLong(4, 5);
    stmt.setString(5, "933-28-5");
    stmt.setFloat(6, 5f);
    stmt.setDouble(7, 5f);
    stmt.setByte(8, (byte) 5);

    //stmt.setBinaryStream(1, new ByteArrayInputStream("testing blob-4".getBytes("utf-8")));
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 4);
    assertEquals(ret.getInt("id"), 4);
    assertEquals(ret.getShort("id"), 4);
    assertEquals(ret.getByte("id"), 4);

    assertEquals(ret.getLong("int"), 4);
    assertEquals(ret.getInt("int"), 4);
    assertEquals(ret.getShort("int"), 4);
    assertEquals(ret.getByte("int"), 4);

    assertEquals(ret.getLong("smallInt"), 4);
    assertEquals(ret.getInt("smallInt"), 4);
    assertEquals(ret.getShort("smallInt"), 4);
    assertEquals(ret.getByte("smallInt"), 4);

    assertEquals(ret.getBoolean("bool"), true);
    //assertEquals(ret.getLong("bool"), 1);

    assertEquals(ret.getString("char"), "char-4");

    assertEquals(ret.getDouble("float"), 4.2d);
    assertEquals(ret.getFloat("float"), 4.2f);

    assertEquals(ret.getDouble("double"), 4.2d);
    assertEquals(ret.getFloat("double"), 4.2f);

    InputStream in = ret.getBinaryStream("blob");
    String blob = IOUtils.toString(in, "utf-8");
    assertEquals(blob, "testing blob-4");
    byte[] bytes = ret.getBytes("blob");
    assertEquals(new String(bytes, "utf-8"), "testing blob-4");

    BigDecimal bd = ret.getBigDecimal("numeric");
    assertEquals(new BigDecimal("4.01"), bd);

    bd = ret.getBigDecimal("decimal");
    assertEquals(new BigDecimal("4.01"), bd);

    bytes = ret.getBytes("bin");
    assertEquals(new String(bytes, "utf-8"), "testing blob-4");
    in = ret.getBinaryStream("bin");
    blob = IOUtils.toString(in, "utf-8");
    assertEquals(blob, "testing blob-4");

    Date date = ret.getDate("date");
    Date lhsDate = new Date(1920 - 1900, 10, 4);
    assertEquals(date, lhsDate);

    Time time = ret.getTime("time");
    Time lhsTime = new Time(10, 12, 4);
    assertEquals(time, lhsTime);

    Timestamp timestamp = ret.getTimestamp("timestamp");
    Timestamp lhsTimestamp = new Timestamp(1990 - 1900, 11, 13, 1, 2, 4, 0);
    assertEquals(timestamp, lhsTimestamp);

    assertEquals(ret.getString("nchar"), "933-28-4");

    assertEquals(ret.getString("nvarchar"), "933-28-4");

    assertEquals(ret.getString("longnvarchar"), "933-28-4");

    assertEquals(ret.getString("longvarchar"), "933-28-4");

    in = ret.getBinaryStream("longvarbinary");
    blob = IOUtils.toString(in, "utf-8");
    assertEquals(blob, "testing blob-4");
    bytes = ret.getBytes("longvarbinary");
    assertEquals(new String(bytes, "utf-8"), "testing blob-4");

    assertEquals(ret.getLong("tinyint"), 4);
    assertEquals(ret.getInt("tinyint"), 4);
    assertEquals(ret.getShort("tinyint"), 4);
    assertEquals(ret.getByte("tinyint"), 4);

    assertEquals(ret.getBoolean("bit"), true);

    assertEquals(ret.getFloat("real"), 4.2f);
    assertEquals(ret.getDouble("real"), 4.199999809265137d);

    //#######################################
    assertEquals(ret.getLong(1), 4);
    assertEquals(ret.getInt(1), 4);
    assertEquals(ret.getShort(1), 4);
    assertEquals(ret.getByte(1), 4);

    assertEquals(ret.getLong(2), 4);
    assertEquals(ret.getInt(2), 4);
    assertEquals(ret.getShort(2), 4);
    assertEquals(ret.getByte(2), 4);

    assertEquals(ret.getLong(3), 4);
    assertEquals(ret.getInt(3), 4);
    assertEquals(ret.getShort(3), 4);
    assertEquals(ret.getByte(3), 4);

    assertEquals(ret.getBoolean(4), true);

    assertEquals(ret.getString(5), "char-4");

    assertEquals(ret.getDouble(6), 4.2d);
    assertEquals(ret.getFloat(6), 4.2f);

    assertEquals(ret.getDouble(7), 4.2d);
    assertEquals(ret.getFloat(7), 4.2f);

    in = ret.getBinaryStream(8);
    blob = IOUtils.toString(in, "utf-8");
    assertEquals(blob, "testing blob-4");
    bytes = ret.getBytes(8);
    assertEquals(new String(bytes, "utf-8"), "testing blob-4");

    bd = ret.getBigDecimal(9);
    assertEquals(new BigDecimal("4.01"), bd);

    bd = ret.getBigDecimal(10);
    assertEquals(new BigDecimal("4.01"), bd);

    in = ret.getBinaryStream(11);
    blob = IOUtils.toString(in, "utf-8");
    assertEquals(blob, "testing blob-4");
    bytes = ret.getBytes(11);
    assertEquals(new String(bytes, "utf-8"), "testing blob-4");

    date = ret.getDate(12);
    lhsDate = new Date(1920-1900, 10, 4);
    assertEquals(date, lhsDate);

    time = ret.getTime(13);
    lhsTime = new Time(10, 12, 4);
    assertEquals(time, lhsTime);

    timestamp = ret.getTimestamp(14);
    lhsTimestamp = new Timestamp(1990 - 1900, 11, 13, 1, 2, 4, 0);
    assertEquals(timestamp, lhsTimestamp);

    assertEquals(ret.getLong(15), 4);
    assertEquals(ret.getInt(15), 4);
    assertEquals(ret.getShort(15), 4);
    assertEquals(ret.getByte(15), 4);

    assertEquals(ret.getBoolean(16), true);

    assertEquals(ret.getFloat(17), 4.2f);
    assertEquals(ret.getDouble(17), 4.199999809265137d);

    assertEquals(ret.getString(18), "933-28-4");

    assertEquals(ret.getString(19), "933-28-4");

    assertEquals(ret.getString(20), "933-28-4");

    assertEquals(ret.getString(21), "933-28-4");

    in = ret.getBinaryStream(22);
    blob = IOUtils.toString(in, "utf-8");
    assertEquals(blob, "testing blob-4");
    bytes = ret.getBytes(22);
    assertEquals(new String(bytes, "utf-8"), "testing blob-4");

  }

  @Test
  public void testInt() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("create table int (id INTEGER, name VARCHAR(20), PRIMARY KEY (id))");
    stmt.executeUpdate();

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into int (id, name) VALUES (?, ?)");
      stmt.setInt(1, i);
      stmt.setString(2, "name-" + i);
      assertEquals(stmt.executeUpdate(), 1);
    }

    stmt = conn.prepareStatement("select id, name from int where id<5 order by id desc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getInt("id"), 4);
    assertEquals(ret.getString("name"), "name-4");
    ret.next();
    assertEquals(ret.getInt("id"), 3);
    ret.next();
    assertEquals(ret.getInt("id"), 2);
    ret.next();
    assertEquals(ret.getInt("id"), 1);
    ret.next();
    assertEquals(ret.getInt("id"), 0);
  }

  @Test
  public void testShort() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("create table smallint (id SMALLINT, name VARCHAR(20), PRIMARY KEY (id))");
    stmt.executeUpdate();

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into smallint (id, name) VALUES (?, ?)");
      stmt.setInt(1, i);
      stmt.setString(2, "name-" + i);
      assertEquals(stmt.executeUpdate(), 1);
    }

    stmt = conn.prepareStatement("select id, name from smallint where id<5 order by id desc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getInt("id"), 4);
    assertEquals(ret.getString("name"), "name-4");
    ret.next();
    assertEquals(ret.getInt("id"), 3);
    ret.next();
    assertEquals(ret.getInt("id"), 2);
    ret.next();
    assertEquals(ret.getInt("id"), 1);
    ret.next();
    assertEquals(ret.getInt("id"), 0);
  }

  @Test
  public void testCharacterStream() throws SQLException, IOException {
    PreparedStatement stmt = conn.prepareStatement("create table charStream (id VARCHAR, name VARCHAR(20), PRIMARY KEY (id))");
    stmt.executeUpdate();

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into charStream (id, name) VALUES (?, ?)");
      stmt.setCharacterStream(1, new InputStreamReader(new ByteArrayInputStream(String.valueOf(i).getBytes("utf-8"))));
      stmt.setString(2, "name-" + i);
      assertEquals(stmt.executeUpdate(), 1);
    }

    stmt = conn.prepareStatement("select id, name from charStream where id<'5' order by id desc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(IOUtils.toString(ret.getCharacterStream("id")), "4");

    assertEquals(ret.getString("name"), "name-4");
    ret.next();
    assertEquals(ret.getInt("id"), 3);
    ret.next();
    assertEquals(ret.getInt("id"), 2);
    ret.next();
    assertEquals(ret.getInt("id"), 1);
    ret.next();
    assertEquals(ret.getInt("id"), 0);
  }

  @Test
  public void testBinaryStream() throws SQLException, IOException {
    PreparedStatement stmt = conn.prepareStatement("create table binaryStream (id VARCHAR, name VARCHAR(20), PRIMARY KEY (id))");
    stmt.executeUpdate();

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into binaryStream (id, name) VALUES (?, ?)");
      stmt.setBinaryStream(1, new ByteArrayInputStream(String.valueOf(i).getBytes("utf-8")));
      stmt.setString(2, "name-" + i);
      assertEquals(stmt.executeUpdate(), 1);
    }

    stmt = conn.prepareStatement("select id, name from binaryStream where id<'5' order by id desc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(IOUtils.toString(ret.getBinaryStream("id"), "utf-8"), "4");

    assertEquals(ret.getString("name"), "name-4");
    ret.next();
    assertEquals(ret.getInt("id"), 3);
    ret.next();
    assertEquals(ret.getInt("id"), 2);
    ret.next();
    assertEquals(ret.getInt("id"), 1);
    ret.next();
    assertEquals(ret.getInt("id"), 0);
  }

  @Test
  public void testTinyInt() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("create table tinyint (id TINYINT, name VARCHAR(20), PRIMARY KEY (id))");
    stmt.executeUpdate();

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into tinyint (id, name) VALUES (?, ?)");
      stmt.setInt(1, i);
      stmt.setString(2, "name-" + i);
      assertEquals(stmt.executeUpdate(), 1);
    }

    stmt = conn.prepareStatement("select id, name from tinyint where id<5 order by id desc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getInt("id"), 4);
    assertEquals(ret.getString("name"), "name-4");
    ret.next();
    assertEquals(ret.getInt("id"), 3);
    ret.next();
    assertEquals(ret.getInt("id"), 2);
    ret.next();
    assertEquals(ret.getInt("id"), 1);
    ret.next();
    assertEquals(ret.getInt("id"), 0);
  }

  @Test
  public void testDate() throws SQLException {
    for (int i = 0; i < recordCount; i++) {
      PreparedStatement stmt = conn.prepareStatement("insert into date (id, id2, name) VALUES (?, ?, ?)");
      stmt.setDate(1, new Date(i + 1, i + 1, i + 1));
      stmt.setInt(2, i);
      stmt.setString(3, "name-" + i);
      assertEquals(stmt.executeUpdate(), 1);
    }

    PreparedStatement stmt = conn.prepareStatement("select id, id2, name from date where id<? order by id desc");
    stmt.setDate(1, new Date(6, 6, 6));
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getDate("id").getYear(), 5);
    assertEquals(ret.getInt("id2"), 4);
    assertEquals(ret.getString("name"), "name-4");
    ret.next();
    assertEquals(ret.getDate("id").getYear(), 4);
    ret.next();
    assertEquals(ret.getDate("id").getYear(), 3);
    ret.next();
    assertEquals(ret.getDate("id").getYear(), 2);
    ret.next();
    assertEquals(ret.getDate("id").getYear(), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testDateString() throws Exception {
    //beforeClass();
    for (int i = 0; i < recordCount; i++) {
      PreparedStatement stmt = conn.prepareStatement("insert into date (id, id2, name) VALUES (" +
          "'200" + i + "-" + "0" + i + "-" + "0" + i + "', ?, ?)");
      stmt.setInt(1, i);
      stmt.setString(2, "name-" + i);
      assertEquals(stmt.executeUpdate(), 1);
    }

    PreparedStatement stmt = conn.prepareStatement("select id, id2, name from date where id<? and id >? order by id desc");
    stmt.setDate(1, new java.sql.Date(106, 1, 1));
    stmt.setDate(2, new java.sql.Date(100, 1, 1));
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getDate("id").getYear(), 105);
    assertEquals(ret.getInt("id2"), 5);
    assertEquals(ret.getString("name"), "name-5");
    ret.next();
    assertEquals(ret.getDate("id").getYear(), 104);
    ret.next();
    assertEquals(ret.getDate("id").getYear(), 103);
    ret.next();
    assertEquals(ret.getDate("id").getYear(), 102);
    ret.next();
    assertEquals(ret.getDate("id").getYear(), 101);
    assertFalse(ret.next());
  }

  @Test
  public void testTime() throws SQLException {
    for (int i = 0; i < recordCount; i++) {
      PreparedStatement stmt = conn.prepareStatement("insert into time (id, id2, name) VALUES (?, ?, ?)");
      stmt.setTime(1, new Time(i + 1, i + 1, i + 1));
      stmt.setInt(2, i);
      stmt.setString(3, "name-" + i);
      assertEquals(stmt.executeUpdate(), 1);
    }

    PreparedStatement stmt = conn.prepareStatement("select id, id2, name from time where id<? order by id desc");
    stmt.setTime(1, new Time(6, 6, 6));
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getTime("id").getHours(), 5);
    assertEquals(ret.getInt("id2"), 4);
    assertEquals(ret.getString("name"), "name-4");
    ret.next();
    assertEquals(ret.getTime("id").getHours(), 4);
    ret.next();
    assertEquals(ret.getTime("id").getHours(), 3);
    ret.next();
    assertEquals(ret.getTime("id").getHours(), 2);
    ret.next();
    assertEquals(ret.getTime("id").getHours(), 1);
  }

  @Test
  public void testTimeString() throws SQLException {
    for (int i = 0; i < recordCount; i++) {
      PreparedStatement stmt = conn.prepareStatement("insert into time (id, id2, name) VALUES (" +
          "'" + (i + 11) + ":"  + (i + 11) + ":" + (i + 11) + "', ?, ?)");
      stmt.setInt(1, i);
      stmt.setString(2, "name-" + i);
      assertEquals(stmt.executeUpdate(), 1);
    }

    PreparedStatement stmt = conn.prepareStatement("select id, id2, name from time where id<? and id>? order by id desc");
    stmt.setTime(1, new Time(16, 16, 16));
    stmt.setTime(2, new Time(10, 10, 10));
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getTime("id").getHours(), 15);
    assertEquals(ret.getInt("id2"), 4);
    assertEquals(ret.getString("name"), "name-4");
    ret.next();
    assertEquals(ret.getTime("id").getHours(), 14);
    ret.next();
    assertEquals(ret.getTime("id").getHours(), 13);
    ret.next();
    assertEquals(ret.getTime("id").getHours(), 12);
    ret.next();
    assertEquals(ret.getTime("id").getHours(), 11);
    assertFalse(ret.next());
  }


  @Test
  public void testTimestamp() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("create table timestamp (id TIMESTAMP, idb TIMESTAMP, id2 INTEGER, name VARCHAR(20), PRIMARY KEY (id))");
    stmt.executeUpdate();

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into timestamp (id, idb, id2, name) VALUES ('"  +
          "1990-12-13 01:02:0" + i + "', ?, ?, ?)");
      //stmt.setTimestamp(1, new Timestamp(1990 - 1900, 11, 13, 1, 2, i, 0));
      if (i != 4) {
        stmt.setTimestamp(1, new Timestamp(1990 - 1900, 11, 13, 1, 2, i + 100, 0));
      }
      else {
        stmt.setTimestamp(1, new Timestamp(1990 - 1900, 11, 13, 1, 2, i, 0));
      }
      stmt.setInt(2, i);
      stmt.setString(3, "name-" + i);
      assertEquals(stmt.executeUpdate(), 1);
    }

    stmt = conn.prepareStatement("select id, id2, name from timestamp where id<? order by id desc");
    stmt.setTimestamp(1, new Timestamp(1990 - 1900, 11, 13, 1, 2, 6, 0));
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getTimestamp("id"), new Timestamp(1990 - 1900, 11, 13, 1, 2, 5, 0));
    assertEquals(ret.getInt("id2"), 5);
    assertEquals(ret.getString("name"), "name-5");
    ret.next();
    assertEquals(ret.getTimestamp("id"), new Timestamp(1990 - 1900, 11, 13, 1, 2, 4, 0));
    ret.next();
    assertEquals(ret.getTimestamp("id"), new Timestamp(1990 - 1900, 11, 13, 1, 2, 3, 0));
    ret.next();
    assertEquals(ret.getTimestamp("id"), new Timestamp(1990 - 1900, 11, 13, 1, 2, 2, 0));
    ret.next();
    assertEquals(ret.getTimestamp("id"), new Timestamp(1990 - 1900, 11, 13, 1, 2, 1, 0));

    stmt = conn.prepareStatement("select id, id2, name from timestamp where id<? order by id desc");
    stmt.setString(1, "1990-12-13 01:02:06.0");
    ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getTimestamp("id"), new Timestamp(1990 - 1900, 11, 13, 1, 2, 5, 0));
    assertEquals(ret.getInt("id2"), 5);
    assertEquals(ret.getString("name"), "name-5");
    ret.next();
    assertEquals(ret.getTimestamp("id"), new Timestamp(1990 - 1900, 11, 13, 1, 2, 4, 0));
    ret.next();
    assertEquals(ret.getTimestamp("id"), new Timestamp(1990 - 1900, 11, 13, 1, 2, 3, 0));
    ret.next();
    assertEquals(ret.getTimestamp("id"), new Timestamp(1990 - 1900, 11, 13, 1, 2, 2, 0));
    ret.next();
    assertEquals(ret.getTimestamp("id"), new Timestamp(1990 - 1900, 11, 13, 1, 2, 1, 0));


    stmt = conn.prepareStatement("select id, id2, name from timestamp where id=idb order by id desc");
    ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getTimestamp("id"), new Timestamp(1990 - 1900, 11, 13, 1, 2, 4, 0));
    assertEquals(ret.getInt("id2"), 4);
    assertEquals(ret.getString("name"), "name-4");
    assertFalse(ret.next());
  }


  @Test
  public void testBytes() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("create table bytes (id BLOB, id2 INTEGER, name VARCHAR(20), PRIMARY KEY (id))");
    stmt.executeUpdate();

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into bytes (id, id2, name) VALUES (?, ?, ?)");
      byte[] id = new byte[]{(byte) i, (byte) i, (byte) i};
      stmt.setBytes(1, id);
      stmt.setInt(2, i);
      stmt.setString(3, "name-" + i);
      assertEquals(stmt.executeUpdate(), 1);
    }

    stmt = conn.prepareStatement("select id, id2, name from bytes where id<? order by id desc");
    byte[] id = new byte[]{(byte) 6, (byte) 6, (byte) 6};
    stmt.setBytes(1, id);
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getBytes("id")[0], 5);
    assertEquals(ret.getInt("id2"), 5);
    assertEquals(ret.getString("name"), "name-5");
    ret.next();
    assertEquals(ret.getBytes("id")[0], 4);
    ret.next();
    assertEquals(ret.getBytes("id")[0], 3);
    ret.next();
    assertEquals(ret.getBytes("id")[0], 2);
    ret.next();
    assertEquals(ret.getBytes("id")[0], 1);
  }

  @Test
  public void testBlob() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("create table blob (id BLOB, id2 INTEGER, name VARCHAR(20), PRIMARY KEY (id))");
    stmt.executeUpdate();

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into blob (id, id2, name) VALUES (?, ?, ?)");
      byte[] id = new byte[]{(byte) i, (byte) i, (byte) i};
      Blob blob = conn.createBlob();
      blob.setBytes(0, id);
      stmt.setBlob(1, blob);
      stmt.setInt(2, i);
      stmt.setString(3, "name-" + i);
      assertEquals(stmt.executeUpdate(), 1);
    }

    stmt = conn.prepareStatement("select id, id2, name from blob where id<? order by id desc");
    byte[] id = new byte[]{(byte) 6, (byte) 6, (byte) 6};
    stmt.setBytes(1, id);
    ResultSet ret = stmt.executeQuery();

    ret.next();
    Blob blob = ret.getBlob("id");
    assertEquals(blob.getBytes(0, (int) blob.length())[0], 5);
    assertEquals(ret.getInt("id2"), 5);
    assertEquals(ret.getString("name"), "name-5");
    ret.next();
    blob = ret.getBlob(1);
    assertEquals(blob.getBytes(0, (int) blob.length())[0], 4);
    blob = ret.getBlob("id");
    assertEquals(blob.getBytes(0, (int) blob.length())[0], 4);
    ret.next();
    blob = ret.getBlob("id");
    assertEquals(blob.getBytes(0, (int) blob.length())[0], 3);
    ret.next();
    blob = ret.getBlob("id");
    assertEquals(blob.getBytes(0, (int) blob.length())[0], 2);
    ret.next();
    blob = ret.getBlob("id");
    assertEquals(blob.getBytes(0, (int) blob.length())[0], 1);
  }


  @Test
  public void testNumeric() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("create table numeric (id NUMERIC, id2 INTEGER, name VARCHAR(20), PRIMARY KEY (id))");
    stmt.executeUpdate();

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into numeric (id, id2, name) VALUES (?, ?, ?)");
      BigDecimal bd = new BigDecimal(i);
      stmt.setBigDecimal(1, bd);
      stmt.setInt(2, i);
      stmt.setString(3, "name-" + i);
      assertEquals(stmt.executeUpdate(), 1);
    }

    stmt = conn.prepareStatement("select id, id2, name from numeric where id<? order by id desc");
    stmt.setBigDecimal(1, new BigDecimal(6));
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getBigDecimal("id"), new BigDecimal(5));
    assertEquals(ret.getInt("id2"), 5);
    assertEquals(ret.getString("name"), "name-5");
    ret.next();
    assertEquals(ret.getBigDecimal("id"), new BigDecimal(4));
    ret.next();
    assertEquals(ret.getBigDecimal("id"), new BigDecimal(3));
    ret.next();
    assertEquals(ret.getBigDecimal("id"), new BigDecimal(2));
    ret.next();
    assertEquals(ret.getBigDecimal("id"), new BigDecimal(1));
  }

  @Test
  public void testDouble() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("create table double (id DOUBLE, id2 INTEGER, name VARCHAR(20), PRIMARY KEY (id))");
    stmt.executeUpdate();

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into double (id, id2, name) VALUES (?, ?, ?)");
      stmt.setDouble(1, new Double(i));
      stmt.setInt(2, i);
      stmt.setString(3, "name-" + i);
      assertEquals(stmt.executeUpdate(), 1);
    }

    stmt = conn.prepareStatement("select id, id2, name from double where id<? order by id desc");
    stmt.setDouble(1, new Double(6));
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getDouble("id"), 5d);
    assertEquals(ret.getInt("id2"), 5);
    assertEquals(ret.getString("name"), "name-5");
    ret.next();
    assertEquals(ret.getDouble("id"), 4d);
    ret.next();
    assertEquals(ret.getDouble("id"), 3d);
    ret.next();
    assertEquals(ret.getDouble("id"), 2d);
    ret.next();
    assertEquals(ret.getDouble("id"), 1d);
  }


  @Test
  public void testReal() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("create table real (id REAL, id2 INTEGER, name VARCHAR(20), PRIMARY KEY (id))");
    stmt.executeUpdate();

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into real (id, id2, name) VALUES (?, ?, ?)");
      stmt.setFloat(1, i);
      stmt.setInt(2, i);
      stmt.setString(3, "name-" + i);
      assertEquals(stmt.executeUpdate(), 1);
    }

    stmt = conn.prepareStatement("select id, id2, name from real where id<? order by id desc");
    stmt.setFloat(1, 6);
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getFloat("id"), 5f);
    assertEquals(ret.getInt("id2"), 5);
    assertEquals(ret.getString("name"), "name-5");
    ret.next();
    assertEquals(ret.getFloat("id"), 4f);
    ret.next();
    assertEquals(ret.getFloat("id"), 3f);
    ret.next();
    assertEquals(ret.getFloat("id"), 2f);
    ret.next();
    assertEquals(ret.getFloat("id"), 1f);
  }

  @Test
  public void testWidth() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("create table width (id VARCHAR(16), id2 INTEGER, name VARCHAR(20), PRIMARY KEY (id))");
    stmt.executeUpdate();

    try {
      stmt = conn.prepareStatement("insert into width (id, id2, name) VALUES (?, ?, ?)");
      stmt.setString(1, "xxxxxxxxxxxxxxxxxxxx");
      stmt.setInt(2, 1);
      stmt.setString(3, "name-" + 1);
      stmt.executeUpdate();
      fail();
    }
    catch (SQLException e) {
      assertTrue(e.getCause().getMessage().contains("java.sql.SQLException: value too long: field=id, width=16"));
    }

    stmt = conn.prepareStatement("insert into width (id, id2, name) VALUES (?, ?, ?)");
    stmt.setString(1, "xxxxxxxx");
    stmt.setInt(2, 1);
    stmt.setString(3, "name-" + 1);
    assertEquals(stmt.executeUpdate(), 1);

    stmt = conn.prepareStatement("select id, id2, name from width order by id desc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getString("id"), "xxxxxxxx");


    try {
      stmt = conn.prepareStatement("update width set id = ? where id=?");
      stmt.setString(1, "xxxxxxxxxxxxxxxxxx");
      stmt.setString(2, "xxxxxxxx");
      stmt.executeUpdate();
      fail();
    }
    catch (SQLException e) {
      assertTrue(e.getMessage().contains("java.sql.SQLException: value too long: field=id, width=16"));
    }

    stmt = conn.prepareStatement("update width set id = ? where id=?");
    stmt.setString(1, "xxxxxxxxxxxxxx");
    stmt.setString(2, "xxxxxxxx");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("select * from width where id='xxxxxxxxxxxxxx'");
    ret = stmt.executeQuery();
    assertTrue(ret.next());
    assertEquals(ret.getString("id"), "xxxxxxxxxxxxxx");

  }

  @Test
  public void testVarchar() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("create table varchar (id VARCHAR, id2 INTEGER, name VARCHAR(20), PRIMARY KEY (id))");
    stmt.executeUpdate();

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into varchar (id, id2, name) VALUES (?, ?, ?)");
      stmt.setString(1, String.valueOf(i));
      stmt.setInt(2, i);
      stmt.setString(3, "name-" + i);
      assertEquals(stmt.executeUpdate(), 1);
    }

    stmt = conn.prepareStatement("select id, id2, name from varchar where id<? order by id desc");
    stmt.setString(1, "6");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getString("id"), "5");
    assertEquals(ret.getInt("id2"), 5);
    assertEquals(ret.getString("name"), "name-5");
    ret.next();
    assertEquals(ret.getString("id"), "4");
    ret.next();
    assertEquals(ret.getString("id"), "3");
    ret.next();
    assertEquals(ret.getString("id"), "2");
    ret.next();
    assertEquals(ret.getString("id"), "1");
  }

  @Test
  public void testNVarchar() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("create table nvarchar (id NVARCHAR, id2 INTEGER, name VARCHAR(20), PRIMARY KEY (id))");
    stmt.executeUpdate();

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into nvarchar (id, id2, name) VALUES (?, ?, ?)");
      stmt.setString(1, String.valueOf(i));
      stmt.setInt(2, i);
      stmt.setString(3, "name-" + i);
      assertEquals(stmt.executeUpdate(), 1);
    }

    stmt = conn.prepareStatement("select id, id2, name from nvarchar where id<? order by id desc");
    stmt.setString(1, "6");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getString("id"), "5");
    assertEquals(ret.getInt("id2"), 5);
    assertEquals(ret.getString("name"), "name-5");
    ret.next();
    assertEquals(ret.getString("id"), "4");
    ret.next();
    assertEquals(ret.getString("id"), "3");
    ret.next();
    assertEquals(ret.getString("id"), "2");
    ret.next();
    assertEquals(ret.getString("id"), "1");
  }


  @Test
  public void testClob() throws SQLException, IOException {
    PreparedStatement stmt = conn.prepareStatement("create table clob (id CLOB, id2 INTEGER, name VARCHAR(20), PRIMARY KEY (id))");
    stmt.executeUpdate();

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into clob (id, id2, name) VALUES (?, ?, ?)");
      Clob clob = conn.createClob();
      clob.setString(0, String.valueOf(i));
      stmt.setClob(1, clob);
      stmt.setInt(2, i);
      stmt.setString(3, "name-" + i);
      assertEquals(stmt.executeUpdate(), 1);
    }

    stmt = conn.prepareStatement("select id, id2, name from clob where id<? order by id desc");
    Clob clob = conn.createClob();
    clob.setString(0, "6");
    stmt.setClob(1, clob);
    ResultSet ret = stmt.executeQuery();

    ret.next();
    clob = ret.getClob("id");
    assertEquals(IOUtils.toString(clob.getCharacterStream()), "5");
    assertEquals(ret.getInt("id2"), 5);
    assertEquals(ret.getString("name"), "name-5");
    ret.next();
    clob = ret.getClob(1);
    assertEquals(IOUtils.toString(clob.getCharacterStream()), "4");
    clob = ret.getClob("id");
    assertEquals(IOUtils.toString(clob.getCharacterStream()), "4");
    ret.next();
    clob = ret.getClob("id");
    assertEquals(IOUtils.toString(clob.getCharacterStream()), "3");
    ret.next();
    clob = ret.getClob("id");
    assertEquals(IOUtils.toString(clob.getCharacterStream()), "2");
    ret.next();
    clob = ret.getClob("id");
    assertEquals(IOUtils.toString(clob.getCharacterStream()), "1");
  }

  @Test
  public void testNClob() throws SQLException, IOException {
    PreparedStatement stmt = conn.prepareStatement("create table nclob (id NCLOB, id2 INTEGER, name VARCHAR(20), PRIMARY KEY (id))");
    stmt.executeUpdate();

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into nclob (id, id2, name) VALUES (?, ?, ?)");
      NClob clob = conn.createNClob();
      clob.setString(0, String.valueOf(i));
      stmt.setClob(1, clob);
      stmt.setInt(2, i);
      stmt.setString(3, "name-" + i);
      assertEquals(stmt.executeUpdate(), 1);
    }

    stmt = conn.prepareStatement("select id, id2, name from nclob where id<? order by id desc");
    NClob nclob = conn.createNClob();
    nclob.setString(0, "6");
    stmt.setNClob(1, nclob);
    ResultSet ret = stmt.executeQuery();

    ret.next();
    nclob = ret.getNClob("id");
    assertEquals(IOUtils.toString(nclob.getCharacterStream()), "5");
    assertEquals(ret.getInt("id2"), 5);
    assertEquals(ret.getString("name"), "name-5");
    ret.next();
    nclob = ret.getNClob(1);
    assertEquals(IOUtils.toString(nclob.getCharacterStream()), "4");
    nclob = ret.getNClob("id");
    assertEquals(IOUtils.toString(nclob.getCharacterStream()), "4");
    ret.next();
    nclob = ret.getNClob("id");
    assertEquals(IOUtils.toString(nclob.getCharacterStream()), "3");
    ret.next();
    nclob = ret.getNClob("id");
    assertEquals(IOUtils.toString(nclob.getCharacterStream()), "2");
    ret.next();
    nclob = ret.getNClob("id");
    assertEquals(IOUtils.toString(nclob.getCharacterStream()), "1");
  }
}


