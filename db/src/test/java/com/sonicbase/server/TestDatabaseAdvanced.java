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
import com.sonicbase.streams.LocalProducer;
import com.sonicbase.schema.FieldSchema;
import com.sonicbase.schema.TableSchema;
import org.apache.commons.io.IOUtils;
import org.codehaus.plexus.util.FileUtils;
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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.testng.Assert.*;

/**
 * Responsible for
 */
public class TestDatabaseAdvanced {

  private Connection conn;
  private int recordCount = 10;
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

    //DatabaseServer.getAddressMap().clear();
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


    PreparedStatement stmt = conn.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, id3 BIGINT, id4 BIGINT, id5 BIGINT, num DOUBLE, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create table Memberships (personId BIGINT, membershipName VARCHAR(20), resortId BIGINT, PRIMARY KEY (personId, membershipName))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create table Resorts (resortId BIGINT, resortName VARCHAR(20), PRIMARY KEY (resortId))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create index socialSecurityNumber on persons(socialSecurityNumber)");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create table nokey (id BIGINT, id2 BIGINT)");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create table nokeysecondaryindex (id BIGINT, id2 BIGINT)");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create index id on nokeysecondaryindex(id)");
    stmt.executeUpdate();

    //test upsert

    LocalProducer.queue.clear();

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

    for (int i = 0; i < 4; i++) {
      stmt = conn.prepareStatement("insert into persons (id, id2, id3, id4, id5, num, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
      stmt.setLong(1, i);
      stmt.setLong(2, (i + 100) % 2);
      stmt.setLong(3, i + 100);
      stmt.setLong(4, (i + 100) % 3);
      stmt.setDouble(5, 1);
      stmt.setDouble(6, i * 0.5);
      stmt.setString(7, "ssN-933-28-" + i);
      stmt.setString(8, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
      stmt.setBoolean(9, false);
      stmt.setString(10, "m");
      assertEquals(stmt.executeUpdate(), 1);
      ids.add((long) i);
    }

    for (int i = 4; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into persons (id, id3, id4, num, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
      stmt.setLong(1, i);
      stmt.setLong(2, i + 100);
      stmt.setLong(3, (i + 100) % 3);
      stmt.setDouble(4, i * 0.5);
      stmt.setString(5, "ssN-933-28-" + i);
      stmt.setString(6, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
      stmt.setBoolean(7, false);
      stmt.setString(8, "m");
      assertEquals(stmt.executeUpdate(), 1);
      ids.add((long) i);
    }

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into persons (id, id2, id3, id4, num, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
      stmt.setLong(1, i + 100);
      stmt.setLong(2, (i + 100) % 2);
      stmt.setLong(3, i + 100);
      stmt.setLong(4, (i + 100) % 3);
      stmt.setDouble(5, (i + 100) * 0.5);
      stmt.setString(6, "ssN-933-28-" + (i % 4));
      stmt.setString(7, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
      stmt.setBoolean(8, false);
      stmt.setString(9, "m");
      int count = stmt.executeUpdate();
      assertEquals(count, 1);
      ids.add((long) (i + 100));
    }

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into nokey (id, id2) VALUES (?, ?)");
      stmt.setLong(1, i);
      stmt.setLong(2, i * 2);
      assertEquals(stmt.executeUpdate(), 1);

      stmt = conn.prepareStatement("insert into nokey (id, id2) VALUES (?, ?)");
      stmt.setLong(1, i);
      stmt.setLong(2, i * 2);
      assertEquals(stmt.executeUpdate(), 1);
    }

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into nokeysecondaryindex (id, id2) VALUES (?, ?)");
      stmt.setLong(1, i);
      stmt.setLong(2, i * 2);
      assertEquals(stmt.executeUpdate(), 1);
    }

    client.beginRebalance("test", "persons", "_1__primarykey");

    while (true) {
      if (client.isRepartitioningComplete("test")) {
        break;
      }
      Thread.sleep(1000);
    }

//    Thread.sleep(30000);

//    assertEquals(client.getPartitionSize("test", 0, "persons", "_1__primarykey"), 9);
//    assertEquals(client.getPartitionSize("test", 1, "persons", "_1__primarykey"), 11);

//    assertEquals(client.getPartitionSize(2, "persons", "_1__primarykey"), 9);
//    assertEquals(client.getPartitionSize(3, "persons", "_1__primarykey"), 8);

    Thread.sleep(10000);

    executor.shutdownNow();
  }

  @Test
  public void testMathLeftExpressionJustMath() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id5 from persons where id < 2 + 1 order by id asc");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 0);
    assertEquals(ret.getLong("id5"), 1);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 1);
    assertEquals(ret.getLong("id5"), 1);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 2);
    assertEquals(ret.getLong("id5"), 1);
    assertFalse(ret.next());
  }

  @Test (enabled=false)
  public void testMathLeftExpressionTwoColumns() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id5 from persons where 5 < id + id5 order by id asc");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 0);
    assertEquals(ret.getLong("id5"), 1);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 1);
    assertEquals(ret.getLong("id5"), 1);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 2);
    assertEquals(ret.getLong("id5"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testMathLeftExpressionTwoColumns2() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id5 from persons where id < id5 + id5 order by id asc");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 0);
    assertEquals(ret.getLong("id5"), 1);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 1);
    assertEquals(ret.getLong("id5"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testMathLeftExpressionGreaterEqual() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id5 from persons where id >= id5 + 1 and id > 1 order by id asc");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 2);
    assertEquals(ret.getLong("id5"), 1);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 3);
    assertEquals(ret.getLong("id5"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testMathLeftExpression() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id5 from persons where id = id5 + 1 and id > 1 order by id asc");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 2);
    assertEquals(ret.getLong("id5"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testMathRightExpression() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id5 from persons where id > 1 and id = id5 + 1 order by id asc");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 2);
    assertEquals(ret.getLong("id5"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testMath() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id5 from persons where id = id5 + 1 order by id asc");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 2);
    assertEquals(ret.getLong("id5"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testMathMultiply() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id5 from persons where id = id5 * 2 order by id asc");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 2);
    assertEquals(ret.getLong("id5"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testMathDivide() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id5 from persons where id = id5 / 2 order by id asc");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 0);
    assertEquals(ret.getLong("id5"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testMathMinus() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id5 from persons where id = id5 - 1 order by id asc");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 0);
    assertEquals(ret.getLong("id5"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testMathBitwiseAnd() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id5 from persons where id = id5 & 1 order by id asc");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 1);
    assertEquals(ret.getLong("id5"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testMathBitwiseOr() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id5 from persons where id = id5 | 2 order by id asc");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 3);
    assertEquals(ret.getLong("id5"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testMathBitwiseXOr() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id5 from persons where id = id5 ^ 1 order by id asc");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 0);
    assertEquals(ret.getLong("id5"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testMathDouble() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id5 from persons where id < id5 * 1.5 order by id asc");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 0);
    assertEquals(ret.getLong("id5"), 1);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 1);
    assertEquals(ret.getLong("id5"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testMathModulo() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id5 from persons where id = id5 % 1 order by id asc");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 0);
    assertEquals(ret.getLong("id5"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testIncompatibleTypes() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id5 from persons where id < 5.4 order by id asc");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 0);
    assertEquals(ret.getLong("id5"), 1);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 1);
    assertEquals(ret.getLong("id5"), 1);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 2);
    assertEquals(ret.getLong("id5"), 1);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 3);
    assertEquals(ret.getLong("id5"), 1);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 4);
    assertEquals(ret.getLong("id5"), 0);
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 5);
    assertEquals(ret.getLong("id5"), 0);
    assertFalse(ret.next());
  }

  @Test
  public void testJson() throws SQLException {
    String dbName = "test";
    String tableName = "persons";
    final TableSchema tableSchema = ((ConnectionProxy)conn).getTables(dbName).get(tableName);
    final List<FieldSchema> fields = tableSchema.getFields();

    final StringBuilder fieldsStr = new StringBuilder();
    final StringBuilder parmsStr = new StringBuilder();
    boolean first = true;
    for (FieldSchema field : fields) {
      if (field.getName().equals("_sonicbase_id")) {
        continue;
      }
      if (first) {
        first = false;
      }
      else {
        fieldsStr.append(",");
        parmsStr.append(",");
      }
      fieldsStr.append(field.getName());
      parmsStr.append("?");
    }

    PreparedStatement stmt = conn.prepareStatement("insert into " + tableName + " (" + fieldsStr.toString() +
        ") VALUES (" + parmsStr.toString() + ")");


    ObjectNode recordJson = new ObjectNode(JsonNodeFactory.instance);
    recordJson.put("id", 1000000);
    recordJson.put("socialSecurityNumber", "529-17-2010");
    recordJson.put("relatives", "xxxyyyxxx");
    recordJson.put("restricted", true);
    recordJson.put("gender", "m");

    Object[] record = StreamManager.getCurrRecordFromJson(recordJson, fields);
    BulkImportManager.setFieldsInInsertStatement(stmt, 1, record, fields);

    assertEquals(stmt.executeUpdate(), 1);

    stmt = conn.prepareStatement("select * from persons where id = 1000000");
    ResultSet ret = stmt.executeQuery();
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 1000000);
    assertEquals(ret.getString("socialsecuritynumber"), "529-17-2010");
    assertEquals(ret.getString("relatives"), "xxxyyyxxx");
    assertEquals(ret.getBoolean("restricted"), true);
    assertEquals(ret.getString("gender"), "m");


    stmt = conn.prepareStatement("delete from persons where id=1000000");
    stmt.executeUpdate();
  }



  @Test
  public void testAlias() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id as i from persons where id < 2 order by id asc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("i"), 0);
    ret.next();
    assertEquals(ret.getLong("i"), 1);

    assertFalse(ret.next());

  }

  @Test
  public void testNot() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id as i from persons where not (id > 2) order by id asc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("i"), 0);
    ret.next();
    assertEquals(ret.getLong("i"), 1);
    ret.next();
    assertEquals(ret.getLong("i"), 2);
    assertFalse(ret.next());

  }

  @Test
  public void testParens() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id as i from persons where (id < 2) order by id asc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("i"), 0);
    ret.next();
    assertEquals(ret.getLong("i"), 1);
    assertFalse(ret.next());

  }

  @Test
  public void testAlias2() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select persons.id as i from persons where id < 2 order by id asc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("i"), 0);
    ret.next();
    assertEquals(ret.getLong("i"), 1);
    assertFalse(ret.next());
  }


  @Test(enabled=false)
  public void testNow() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id as i, now() as now from persons where id < 2 order by id asc");
    ResultSet ret = stmt.executeQuery();

    assertTrue(false);
    ret.next();
    assertEquals(ret.getLong("i"), 0);
    ret.next();
    assertEquals(ret.getLong("i"), 1);

    assertFalse(ret.next());
  }

  @Test
  public void testLength() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select socialsecuritynumber, length(socialsecuritynumber) as Length from persons where id = 0");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getString("socialsecuritynumber"), "ssN-933-28-0");
    assertEquals(ret.getLong("length"), 12);
    assertEquals(ret.getInt("length"), 12);
    assertFalse(ret.next());
  }


  @Test
  public void testDistinct() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select distinct id2 from persons where id > 102 order by id asc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 1);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertFalse(ret.next());

  }

  @Test
  public void testGroupByNoKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id2 from nokey where id < 2 group by id2 order by id");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 0);
    ret.next();
    assertEquals(ret.getLong("id2"), 2);
    assertEquals(ret.getLong("id"), 1);
    assertFalse(ret.next());

  }

  @Test
  public void testGroupBy() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id2 from persons group by id2 order by id");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 108);
    ret.next();
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getLong("id"), 109);
    assertFalse(ret.next());

  }

  @Test
  public void testServerSort() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id2 from persons order by id2 ASC");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 0);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 2);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 100);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 102);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 104);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 106);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 108);
    ret.next();
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getLong("id"), 1);
    ret.next();
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getLong("id"), 3);
    ret.next();
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getLong("id"), 101);
    ret.next();
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getLong("id"), 103);
    ret.next();
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getLong("id"), 105);
    ret.next();
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getLong("id"), 107);
    ret.next();
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getLong("id"), 109);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertTrue(ret.wasNull());
    assertEquals(ret.getLong("id"), 4);
    //assertFalse(ret.next());

  }

  @Test
  public void testServerSortNoKeySecondaryIndex() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id2 from nokeysecondaryindex where id < 2 order by id2 ASC");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 0);
    ret.next();
    assertEquals(ret.getLong("id2"), 2);
    assertEquals(ret.getLong("id"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testServerSortNoKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id2 from nokey where id < 2 order by id2 ASC");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 0);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 0);
    ret.next();
    assertEquals(ret.getLong("id2"), 2);
    assertEquals(ret.getLong("id"), 1);
    ret.next();
    assertEquals(ret.getLong("id2"), 2);
    assertEquals(ret.getLong("id"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testGroupByNestedNoKeySecondaryIndex() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from nokeysecondaryindex where id < 2 group by id2,id");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 0);
    ret.next();
    assertEquals(ret.getLong("id2"), 2);
    assertEquals(ret.getLong("id"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testGroupByNestedNoKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from nokey where id < 2 group by id2,id");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 0);
    ret.next();
    assertEquals(ret.getLong("id2"), 2);
    assertEquals(ret.getLong("id"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testGroupByNested() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons group by id2,id4");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id4"), 0);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id4"), 1);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id4"), 2);
    ret.next();
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getLong("id4"), 0);
    ret.next();
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getLong("id4"), 1);
    ret.next();
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getLong("id4"), 2);
    assertFalse(ret.next());
  }

  @Test
  public void testGroupByNestedMinNoKeySecondaryIndex() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select min(id) as minValue from nokeysecondaryindex where id < 2 group by id2,id");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 0);
    assertEquals(ret.getLong("minValue"), 0);
    ret.next();
    assertEquals(ret.getLong("id2"), 2);
    assertEquals(ret.getLong("id"), 1);
    assertEquals(ret.getLong("minValue"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testGroupByNestedMinNoKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select min(id) as minValue from nokey where id < 2 group by id2,id");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 0);
    assertEquals(ret.getLong("minValue"), 0);
    ret.next();
    assertEquals(ret.getLong("id2"), 2);
    assertEquals(ret.getLong("id"), 1);
    assertEquals(ret.getLong("minValue"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testGroupByNestedMin() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select min(id) as minValue from persons group by id2,id4");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id4"), 0);
    assertEquals(ret.getLong("minValue"), 2);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id4"), 1);
    assertEquals(ret.getLong("minValue"), 0);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id4"), 2);
    assertEquals(ret.getLong("minValue"), 104);
    ret.next();
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getLong("id4"), 0);
    assertEquals(ret.getLong("minValue"), 105);
    ret.next();
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getLong("id4"), 1);
    assertEquals(ret.getLong("minValue"), 3);
    ret.next();
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getLong("id4"), 2);
    assertEquals(ret.getLong("minValue"), 1);

    assertFalse(ret.next());
  }

  @Test
  public void testGroupByMaxNoKeySecondaryIndex() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id2, max(id) as maxValue from nokeysecondaryindex where id < 2 group by id2");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getInt("maxValue"), 0);
    ret.next();
    assertEquals(ret.getLong("id2"), 2);
    assertEquals(ret.getInt("maxValue"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testGroupByMaxNoKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id2, max(id) as maxValue from nokey where id < 2 group by id2");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getInt("maxValue"), 0);
    ret.next();
    assertEquals(ret.getLong("id2"), 2);
    assertEquals(ret.getInt("maxValue"), 1);
    assertFalse(ret.next());

  }

  @Test
  public void testGroupByMax() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id2, max(id) as maxValue from persons where id < 200 group by id2");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getInt("maxValue"), 108);
//
//    ret.next();
//     assertEquals(ret.getLong("id2"), 0);
//     assertEquals(ret.getInt("maxValue"), 108);

    ret.next();
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getInt("maxValue"), 109);
    assertFalse(ret.next());

  }

  @Test
  public void testGroupBySumDouble() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id2, sum(num) as sumValue from persons group by id2");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getDouble("sumValue"), 261.0D);

    ret.next();
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getDouble("sumValue"), 264.5D);
    assertFalse(ret.next());
  }

  @Test
  public void testGroupByMinNoKeySecondaryIndex() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id2, min(id) as minValue from nokeysecondaryindex where id < 2 group by id2");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getInt("minValue"), 0);

    ret.next();
    assertEquals(ret.getLong("id2"), 2);
    assertEquals(ret.getLong("minValue"), 1);
    assertFalse(ret.next());

  }

  @Test
  public void testGroupByMinNoKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id2, min(id) as minValue from nokey where id < 2 group by id2");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getInt("minValue"), 0);

    ret.next();
    assertEquals(ret.getLong("id2"), 2);
    assertEquals(ret.getLong("minValue"), 1);
    assertFalse(ret.next());

  }

  @Test
  public void testGroupByMin() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id2, min(id) as minValue from persons group by id2");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getInt("minValue"), 0);

    ret.next();
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getLong("minValue"), 1);
    assertFalse(ret.next());

  }

  @Test
  public void testGroupByMinMaxSumAvgNoKeySecondaryIndex() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id2, min(id) as minValue, max(id) as maxValue, sum(id) as sumValue, avg(id) as avgValue from nokeysecondaryindex where id < 2 group by id2");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getInt("minValue"), 0);
    assertEquals(ret.getInt("maxValue"), 0);
    assertEquals(ret.getInt("sumValue"), 0);
    assertEquals(ret.getInt("avgValue"), 0);

    ret.next();
    assertEquals(ret.getLong("id2"), 2);
    assertEquals(ret.getLong("minValue"), 1);
    assertEquals(ret.getLong("maxValue"), 1);
    assertEquals(ret.getLong("sumValue"), 1);
    assertEquals(ret.getLong("avgValue"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testGroupByMinMaxSumAvgNoKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id2, min(id) as minValue, max(id) as maxValue, sum(id) as sumValue, avg(id) as avgValue from nokey where id < 2 group by id2");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getInt("minValue"), 0);
    assertEquals(ret.getInt("maxValue"), 0);
    assertEquals(ret.getInt("sumValue"), 0);
    assertEquals(ret.getInt("avgValue"), 0);

    ret.next();
    assertEquals(ret.getLong("id2"), 2);
    assertEquals(ret.getLong("minValue"), 1);
    assertEquals(ret.getLong("maxValue"), 1);
    assertEquals(ret.getLong("sumValue"), 2);
    assertEquals(ret.getLong("avgValue"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testGroupByMinMaxSumAvg() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id2, min(id) as minValue, max(id) as maxValue, sum(id) as sumValue, avg(id) as avgValue from persons group by id2");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getInt("minValue"), 0);
    assertEquals(ret.getInt("maxValue"), 108);
    assertEquals(ret.getInt("sumValue"), 522);
    assertEquals(ret.getInt("avgValue"), 74);

    ret.next();
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getLong("minValue"), 1);
    assertEquals(ret.getLong("maxValue"), 109);
    assertEquals(ret.getLong("sumValue"), 529);
    assertEquals(ret.getLong("avgValue"), 75);
    assertFalse(ret.next());
  }

  @Test
    public void testGroupByMinTwoFieldsNoKeySecondaryIndex() throws SQLException {
      PreparedStatement stmt = conn.prepareStatement("select id2, min(id) as minValue, min(id2) as minId3Value from nokeysecondaryindex where id < 2 group by id2");
      ResultSet ret = stmt.executeQuery();

      ret.next();
      assertEquals(ret.getLong("id2"), 0);
      assertEquals(ret.getInt("minValue"), 0);
      assertEquals(ret.getInt("minId3Value"), 0);

      ret.next();
      assertEquals(ret.getLong("id2"), 2);
      assertEquals(ret.getLong("minValue"), 1);
      assertEquals(ret.getLong("minId3Value"), 2);
      assertFalse(ret.next());
    }

  @Test
  public void testGroupByMinTwoFieldsNoKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id2, min(id) as minValue, min(id2) as minId3Value from nokey where id < 2 group by id2");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getInt("minValue"), 0);
    assertEquals(ret.getInt("minId3Value"), 0);

    ret.next();
    assertEquals(ret.getLong("id2"), 2);
    assertEquals(ret.getLong("minValue"), 1);
    assertEquals(ret.getLong("minId3Value"), 2);
    assertFalse(ret.next());
  }

  @Test
  public void testGroupByMinTwoFields() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id2, min(id) as minValue, min(id3) as minId3Value from persons group by id2");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getInt("minValue"), 0);
    assertEquals(ret.getInt("minId3Value"), 100);

    ret.next();
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getLong("minValue"), 1);
    assertEquals(ret.getLong("minId3Value"), 101);
    assertFalse(ret.next());
  }

  @Test
  public void testGroupByMinWhereTablescanNoKeySecondaryIndex() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id2, min(id) as minValue from nokeysecondaryindex where id2 > 0 AND id < 2 group by id2");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 2);
    assertEquals(ret.getLong("minValue"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testGroupByMinWhereTablescanNoKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id2, min(id) as minValue from nokey where id2 > 0 AND id < 2 group by id2");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 2);
    assertEquals(ret.getLong("minValue"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testGroupByMinWhereTablescan() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id2, min(id) as minValue from persons where id2 > 0 group by id2");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getLong("minValue"), 1);
    assertFalse(ret.next());
  }


  @Test
  public void testGroupByMinWhere() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id2, min(id) as minValue from persons where id > 5 group by id2");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getInt("minValue"), 100);

    ret.next();
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getLong("minValue"), 101);
    assertFalse(ret.next());

  }

  @Test
  public void testGroupCountNoKeySecondaryIndex() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id2, count(id) as countValue from nokeysecondaryindex where id < 2 group by id2");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("countValue"), 1);

    ret.next();
    assertEquals(ret.getLong("id2"), 2);
    assertEquals(ret.getLong("countValue"), 1);
    assertFalse(ret.next());

  }

  @Test
  public void testGroupCountNoKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id2, count(id) as countValue from nokey where id < 2 group by id2");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("countValue"), 2);

    ret.next();
    assertEquals(ret.getLong("id2"), 2);
    assertEquals(ret.getLong("countValue"), 2);
    assertFalse(ret.next());

  }

  @Test
  public void testGroupCount() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id2, count(id) as countValue from persons group by id2");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("countValue"), 7);

    ret.next();
    assertEquals(ret.getLong("id2"), 1);
    assertEquals(ret.getLong("countValue"), 7);
    assertFalse(ret.next());

  }

  @Test
  public void testCountDistinct() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select count(distinct id2) as count from persons where id > 100 order by id asc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getInt("count"), 2);
    assertFalse(ret.next());

  }

  @Test
  public void testDistinctWhere() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select distinct id2 from persons where id >= 100 order by id asc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getInt("id"), 100);
    assertEquals(ret.getInt("id2"), 0);
    ret.next();
    assertEquals(ret.getInt("id"), 101);
    assertEquals(ret.getInt("id2"), 1);
//    ret.next();
//    assertEquals(ret.getInt("id"), 102);
//    assertEquals(ret.getInt("id2"), 0);
//    ret.next();
//    assertEquals(ret.getInt("id"), 103);
//    assertEquals(ret.getInt("id2"), 1);
//    ret.next();
//    assertEquals(ret.getInt("id"), 104);
//    assertEquals(ret.getInt("id2"), 0);
//    ret.next();
//    assertEquals(ret.getInt("id"), 105);
//    assertEquals(ret.getInt("id2"), 1);
//    ret.next();
//    assertEquals(ret.getInt("id"), 106);
//    assertEquals(ret.getInt("id2"), 0);
//    ret.next();
//    assertEquals(ret.getInt("id"), 107);
//    assertEquals(ret.getInt("id2"), 1);
//    ret.next();
//    assertEquals(ret.getInt("id"), 108);
//    assertEquals(ret.getInt("id2"), 0);
//    ret.next();
//    assertEquals(ret.getInt("id"), 109);
//    assertEquals(ret.getInt("id2"), 1);
    assertFalse(ret.next());

  }

  @Test
  public void testDistinct2() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select distinct id2, socialsecuritynumber from persons where id > 102 order by id asc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getString("socialsecuritynumber"), "ssN-933-28-3");
    assertEquals(ret.getLong("id2"), 1);
    ret.next();
    assertEquals(ret.getString("socialsecuritynumber"), "ssN-933-28-0");
    assertEquals(ret.getLong("id2"), 0);
    ret.next();
    assertEquals(ret.getString("socialsecuritynumber"), "ssN-933-28-1");
    assertEquals(ret.getLong("id2"), 1);
    ret.next();
    assertEquals(ret.getString("socialsecuritynumber"), "ssN-933-28-2");
    assertEquals(ret.getLong("id2"), 0);
    ret.next();
    assertFalse(ret.next());

  }

  @Test
  public void testLower() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, socialsecuritynumber, lower(socialsecuritynumber) as lower from persons where id < 5");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 0);
    assertEquals(ret.getString("lower"), "ssn-933-28-0");
    ret.next();
    assertEquals(ret.getLong("id"), 1);
    assertEquals(ret.getString("lower"), "ssn-933-28-1");
    ret.next();
    assertEquals(ret.getLong("id"), 2);
    assertEquals(ret.getString("lower"), "ssn-933-28-2");
    ret.next();
    assertEquals(ret.getLong("id"), 3);
    assertEquals(ret.getString("lower"), "ssn-933-28-3");
    ret.next();
    assertEquals(ret.getLong("id"), 4);
    assertEquals(ret.getString("lower"), "ssn-933-28-4");
    assertFalse(ret.next());
  }

  @Test
  public void testSubstring() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select substring(socialsecuritynumber, 11, 12) as str from persons where id < 5");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getString("str"), "0");
    ret.next();
    assertEquals(ret.getString("str"), "1");
    ret.next();
    assertEquals(ret.getString("str"), "2");
    ret.next();
    assertEquals(ret.getString("str"), "3");
    ret.next();
    assertEquals(ret.getString("str"), "4");
    assertFalse(ret.next());
  }

  @Test
  public void testSubstring2() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select substring(socialsecuritynumber, 10) as str from persons where id < 5");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getString("str"), "-0");
    ret.next();
    assertEquals(ret.getString("str"), "-1");
    ret.next();
    assertEquals(ret.getString("str"), "-2");
    ret.next();
    assertEquals(ret.getString("str"), "-3");
    ret.next();
    assertEquals(ret.getString("str"), "-4");
    assertFalse(ret.next());
  }

  @Test
  public void testUpper() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select socialsecuritynumber, upper(socialsecuritynumber) as upper from persons where id < 5");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 0);
    assertEquals(ret.getString("upper"), "SSN-933-28-0");
    ret.next();
    assertEquals(ret.getLong("id"), 1);
    assertEquals(ret.getString("upper"), "SSN-933-28-1");
    ret.next();
    assertEquals(ret.getLong("id"), 2);
    assertEquals(ret.getString("upper"), "SSN-933-28-2");
    ret.next();
    assertEquals(ret.getLong("id"), 3);
    assertEquals(ret.getString("upper"), "SSN-933-28-3");
    ret.next();
    assertEquals(ret.getLong("id"), 4);
    assertEquals(ret.getString("upper"), "SSN-933-28-4");
    assertFalse(ret.next());
  }

  @Test
  public void testBetween() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id between 1 and 5 order by id asc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 1);
    ret.next();
    assertEquals(ret.getLong("id"), 2);
    ret.next();
    assertEquals(ret.getLong("id"), 3);
    ret.next();
    assertEquals(ret.getLong("id"), 4);
    ret.next();
    assertEquals(ret.getLong("id"), 5);
    assertFalse(ret.next());
  }

  @Test
  public void testNotBetween() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id not between 1 and 5 and id < 10");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 0);
    ret.next();
    assertEquals(ret.getLong("id"), 6);
    ret.next();
    assertEquals(ret.getLong("id"), 7);
    ret.next();
    assertEquals(ret.getLong("id"), 8);
    ret.next();
    assertEquals(ret.getLong("id"), 9);
    assertFalse(ret.next());
  }

  @Test
  public void testNotBetweenIn() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id between 1 and 5 AND id not in (1,2)");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 3);
    ret.next();
    assertEquals(ret.getLong("id"), 4);
    ret.next();
    assertEquals(ret.getLong("id"), 5);
    assertFalse(ret.next());
  }

  @Test(enabled=false)
  public void testUpsert() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("upsert into persons (id) values (?)");
    stmt.setLong(1, 1000000L);
    stmt.executeQuery();
  }

  @Test(enabled=false)
  public void testUnion() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id from persons union select id from memberships");
    stmt.executeUpdate();
  }

  @Test
  public void testCeiling() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, num from persons where ceiling(num) < 2.0");
    ResultSet rs = stmt.executeQuery();
    rs.next();
    assertEquals(rs.getLong("id"), 0);
    rs.next();
    assertEquals(rs.getLong("id"), 1);
    rs.next();
    assertEquals(rs.getLong("id"), 2);
    assertFalse(rs.next());
  }

  @Test
  public void testLike() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, socialsecuritynumber from persons where socialsecuritynumber like '%3'");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 3);
    assertEquals(ret.getString("socialsecuritynumber"), "ssN-933-28-3");
    ret.next();
    assertEquals(ret.getLong("id"), 103);
    assertEquals(ret.getString("socialsecuritynumber"), "ssN-933-28-3");
    ret.next();
    assertEquals(ret.getLong("id"), 107);
    assertEquals(ret.getString("socialsecuritynumber"), "ssN-933-28-3");
    assertFalse(ret.next());
  }

  @Test
   public void testLikeAnd() throws SQLException {
     PreparedStatement stmt = conn.prepareStatement("select id, socialsecuritynumber from persons where socialsecuritynumber like '%3' and id < 5");
     ResultSet ret = stmt.executeQuery();

     ret.next();
     assertEquals(ret.getLong("id"), 3);
     assertEquals(ret.getString("socialsecuritynumber"), "ssN-933-28-3");
     assertFalse(ret.next());
   }

  @Test
   public void testLikeOR() throws SQLException {
     PreparedStatement stmt = conn.prepareStatement("select id, socialsecuritynumber from persons where socialsecuritynumber like '%3' or id = 5 order by id desc");
     ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 107);
    assertEquals(ret.getString("socialsecuritynumber"), "ssN-933-28-3");
    ret.next();
    assertEquals(ret.getLong("id"), 103);
    assertEquals(ret.getString("socialsecuritynumber"), "ssN-933-28-3");
    ret.next();
    assertEquals(ret.getLong("id"), 5);
    assertEquals(ret.getString("socialsecuritynumber"), "ssN-933-28-5");
     ret.next();
     assertEquals(ret.getLong("id"), 3);
     assertEquals(ret.getString("socialsecuritynumber"), "ssN-933-28-3");
     assertFalse(ret.next());
   }

  @Test
  public void testNotLike() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, sociasecuritynumber from persons where socialsecuritynumber not like '%3' and id < 5");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 0);
    assertEquals(ret.getString("socialsecuritynumber"), "ssN-933-28-0");
    ret.next();
    assertEquals(ret.getLong("id"), 1);
    assertEquals(ret.getString("socialsecuritynumber"), "ssN-933-28-1");
    ret.next();
    assertEquals(ret.getLong("id"), 2);
    assertEquals(ret.getString("socialsecuritynumber"), "ssN-933-28-2");
    ret.next();
    assertEquals(ret.getLong("id"), 4);
    assertEquals(ret.getString("socialsecuritynumber"), "ssN-933-28-4");
    assertFalse(ret.next());
  }

  @Test
  public void testColumnEqualsColumn() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id = id2 order by id desc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 1);
    ret.next();
    assertEquals(ret.getLong("id"), 0);
    assertFalse(ret.next());
  }

  @Test
  public void testColumnEqualsColumnAnd() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id = id2 and id < 1 order by id desc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 0);
    assertFalse(ret.next());
  }

  @Test(invocationCount = 1)
  public void testColumnEqualsColumnAndIn() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id = id2 and id in (0, 1) order by id desc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    System.out.println(ret.getLong("id"));
    assertEquals(ret.getLong("id"), 1);
    ret.next();
    System.out.println(ret.getLong("id"));
    assertEquals(ret.getLong("id"), 0);
    assertFalse(ret.next());
  }

  @Test
  public void testColumnEqualsColumnAndNotIn() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id = id2 and id not in (0) order by id desc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 1);
    assertFalse(ret.next());
  }


  @Test
  public void testCount() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select count(*) from persons");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.next());
    assertEquals(ret.getInt(1), 20);
  }

  @Test
  public void testCountAs() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select count(*) As personCount from persons");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getInt(1), 20);
  }

  @Test
  public void testCount2NoKeySecondaryIndex() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select count(id2) from nokeysecondaryindex");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getInt(1), 10);
  }

  @Test
  public void testCount2NoKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select count(id2) from nokey");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getInt(1), 20);
  }

  @Test
  public void testCount2() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select count(id2) from persons");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getInt(1), 14);
  }

  @Test
  public void testCount3NoKeySecondaryIndex() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select count(*) from nokeysecondaryindex where id2 = 0");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong(1), 1);
  }

  @Test
  public void testCount3NoKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select count(*) from nokey where id2 = 0");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong(1), 2);
  }

  @Test
  public void testCount3() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select count(*) from persons where id2 = 0");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong(1), 7);
  }

  @Test
  public void testTruncateTable() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("create table tvs (id BIGINT, make VARCHAR(1024), model VARCHAR(1024), PRIMARY KEY (id))");
    stmt.executeUpdate();

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into tvs (id, make, model) VALUES (?, ?, ?)");
      stmt.setLong(1, i);
      stmt.setString(2, "make-" + i);
      stmt.setString(3, "model-" + i);
      assertEquals(stmt.executeUpdate(), 1);
    }

    stmt = conn.prepareStatement("select count(*) from tvs");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong(1), recordCount);

    stmt = conn.prepareStatement("truncate table tvs");
    boolean bool = stmt.execute();

    stmt = conn.prepareStatement("select count(*) from tvs");
    ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong(1), 0);
  }

  @Test
  public void testUpdateSecondary() throws SQLException, UnsupportedEncodingException {
    PreparedStatement stmt = conn.prepareStatement("create table secondary_update (id BIGINT, make VARCHAR(1024), model VARCHAR(1024))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create index make_model on secondary_update(make, model)");
    stmt.executeUpdate();

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into secondary_update (id, make, model) VALUES (?, ?, ?)");
      stmt.setLong(1, i);
      stmt.setString(2, "make-" + i);
      stmt.setString(3, "model-" + i);
      assertEquals(stmt.executeUpdate(), 1);
    }

    stmt = conn.prepareStatement("update secondary_update set make=?, model=? where make=? and model=?");
    stmt.setString(1, "my-make");
    stmt.setString(2, "my-model");
    stmt.setString(3, "make-0");
    stmt.setString(4, "model-0");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("select * from secondary_update where make=?");
    stmt.setString(1, "my-make");
    ResultSet rs = stmt.executeQuery();

    assertTrue(rs.next());
    assertEquals(rs.getString("make"), "my-make");
    assertEquals(rs.getString("model"), "my-model");
    assertFalse(rs.next());

    Index index = ((DatabaseServer)DatabaseClient.getServers().get(0).get(0)).getIndices().get("test").getIndices().get("secondary_update").get("_2_make_model");
    Object value = index.get(new Object[]{"make-0".getBytes("utf-8")});
    assertEquals(value, null);

    stmt = conn.prepareStatement("select * from secondary_update where make=?");
    stmt.setString(1, "make-0");
    rs = stmt.executeQuery();

    assertFalse(rs.next());
  }

  @Test
  public void testDeleteSecondary() throws SQLException, UnsupportedEncodingException, EOFException {
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
    stmt.setString(1, "make-0");
    stmt.setString(2, "model-0");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("select * from secondary_delete where make=?");
    stmt.setString(1, "make-0");
    ResultSet rs = stmt.executeQuery();

    assertFalse(rs.next());

    index = ((DatabaseServer)DatabaseClient.getServers().get(0).get(0)).getIndices().get("test").getIndices().get("secondary_delete").get("_2_make_model");
    value = index.get(new Object[]{"make-0".getBytes("utf-8")});
    assertEquals(value, null);

    index = ((DatabaseServer)DatabaseClient.getServers().get(0).get(0)).getIndices().get("test").getIndices().get("secondary_delete").get("_1__primarykey");
    primaryKey = DatabaseCommon.deserializeKey(tableSchema, keyRecord.getPrimaryKey());
    value = index.get(primaryKey);
    assertNull(value);
  }

  @Test
  public void testTableScan() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where restricted='false'");
    ResultSet ret = stmt.executeQuery();

    for (int i = 0; i < recordCount; i++) {
      assertTrue(ret.next());
    }
  }

  @Test
  public void testColumnsInKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id from persons where id = 2 order by id desc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 2);
    assertFalse(ret.next());
  }

  @Test
    public void testColumnsInKeyRange() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id from persons where id < 5 order by id asc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 0);
    ret.next();
    assertEquals(ret.getLong("id"), 1);
    ret.next();
    assertEquals(ret.getLong("id"), 2);
    ret.next();
    assertEquals(ret.getLong("id"), 3);
    ret.next();
    assertEquals(ret.getLong("id"), 4);
    assertFalse(ret.next());
  }

  @Test
  public void testColumnsInKeyRangeTwoKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id from persons where id < 5 and id > 1 order by id asc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 2);
    ret.next();
    assertEquals(ret.getLong("id"), 3);
    ret.next();
    assertEquals(ret.getLong("id"), 4);
    assertFalse(ret.next());
  }
}
