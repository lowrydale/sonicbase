package com.lowryengineering.database;

import com.lowryengineering.database.client.DatabaseClient;
import com.lowryengineering.database.common.DatabaseCommon;
import com.lowryengineering.database.jdbcdriver.ConnectionProxy;
import com.lowryengineering.database.jdbcdriver.ParameterHandler;
import com.lowryengineering.database.query.BinaryExpression;
import com.lowryengineering.database.query.impl.ColumnImpl;
import com.lowryengineering.database.query.impl.ExpressionImpl;
import com.lowryengineering.database.query.impl.SelectContextImpl;
import com.lowryengineering.database.schema.IndexSchema;
import com.lowryengineering.database.schema.TableSchema;
import com.lowryengineering.database.server.DatabaseServer;
import com.lowryengineering.database.util.JsonArray;
import com.lowryengineering.database.util.JsonDict;
import com.lowryengineering.database.util.StreamUtils;
import com.sun.jersey.core.util.Base64;
import org.apache.commons.codec.binary.Hex;
import org.codehaus.plexus.util.FileUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.crypto.*;
import javax.crypto.spec.SecretKeySpec;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.security.InvalidKeyException;
import java.security.Key;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.testng.Assert.*;

public class TestDatabase {

  private Connection conn;
  private int recordCount = 10;
  List<Long> ids = new ArrayList<>();

  DatabaseClient client = null;

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
//      futures.add(executor.submit(new Callable() {
//        @Override
//        public Object call() throws Exception {
//          String role = "primaryMaster";

      dbServers[shard] = new DatabaseServer();
      dbServers[shard].setConfig(config, "4-servers", "localhost", 9010 + (50 * shard), true, new AtomicBoolean(true));
      dbServers[shard].setRole(role);
      dbServers[shard].disableLogProcessor();
//          return null;
//        }
//      }));
    }
    for (Future future : futures) {
      future.get();
    }

    for (DatabaseServer server : dbServers) {
      server.disableRepartitioner();
    }

    //DatabaseClient client = new DatabaseClient("localhost", 9010, true);

    Class.forName("com.lowryengineering.database.jdbcdriver.DriverProxy");

    conn = DriverManager.getConnection("jdbc:dbproxy:127.0.0.1:9000", "user", "password");

    ((ConnectionProxy) conn).getDatabaseClient().createDatabase("test");

    conn.close();

    conn = DriverManager.getConnection("jdbc:dbproxy:127.0.0.1:9000/test", "user", "password");

    client = ((ConnectionProxy) conn).getDatabaseClient();

    client.setPageSize(3);

    PreparedStatement stmt = conn.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create table Memberships (personId BIGINT, membershipName VARCHAR(20), resortId BIGINT, PRIMARY KEY (personId, membershipName))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create table Resorts (resortId BIGINT, resortName VARCHAR(20), PRIMARY KEY (resortId))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create table nokey (id BIGINT, id2 BIGINT)");
    stmt.executeUpdate();

    //test insert

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
      stmt = conn.prepareStatement("insert into persons (id, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?)");
      stmt.setLong(1, i);
      stmt.setString(2, "933-28-" + i);
      stmt.setString(3, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
      stmt.setBoolean(4, false);
      stmt.setString(5, "m");
      assertEquals(stmt.executeUpdate(), 1);
      ids.add((long) i);
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

    stmt = conn.prepareStatement("create index socialSecurityNumber on persons(socialSecurityNumber)");
    stmt.executeUpdate();

    //create index ssn2 on persons(socialSecurityNumber)
//    stmt = conn.prepareStatement("create index ssn on persons(socialSecurityNumber)");
//    stmt.executeUpdate();

    while (true) {
      byte[] bytes = ((ConnectionProxy) conn).getDatabaseClient().send(null, 0, 0, "DatabaseServer:areAllLongRunningCommandsComplete:1:test", null, DatabaseClient.Replica.master);
      if (new String(bytes).equals("true")) {
        break;
      }
      Thread.sleep(1000);
    }

    IndexSchema indexSchema = null;
    for (Map.Entry<String, IndexSchema> entry : client.getCommon().getTables("test").get("persons").getIndices().entrySet()) {
      if (entry.getValue().getFields()[0].equalsIgnoreCase("socialsecuritynumber")) {
        indexSchema = entry.getValue();
      }
    }
    List<ColumnImpl> columns = new ArrayList<>();
    columns.add(new ColumnImpl(null, null, "persons", "socialsecuritynumber", null));

    AtomicReference<String> usedIndex = new AtomicReference<String>();
    ExpressionImpl.RecordCache recordCache = new ExpressionImpl.RecordCache();
    ParameterHandler parms = new ParameterHandler();
    SelectContextImpl ret = ExpressionImpl.lookupIds(
          "test", client.getCommon(), client, 0,
          1000, client.getCommon().getTables("test").get("persons"), indexSchema, false, BinaryExpression.Operator.equal,
          null,
          null,
          new Object[]{"933-28-0".getBytes()}, parms, null, null,
      new Object[]{"933-28-0".getBytes()},
          null,
          columns, "socialsecuritynumber", 0, recordCache,
          usedIndex, false, client.getCommon().getSchemaVersion(), null, null, false);

    assertEquals(ret.getCurrKeys().length, 4);

    client.beginRebalance("test", "persons", "_1__primarykey");

    while (true) {
      if (client.isRepartitioningComplete("test")) {
        break;
      }
      Thread.sleep(1000);
    }

    assertEquals(client.getPartitionSize("test", 0, "persons", "_1__primarykey"), 11);
    assertEquals(client.getPartitionSize("test", 1, "persons", "_1__primarykey"), 9);

    dbServers[0].enableSnapshot(false);
    dbServers[1].enableSnapshot(false);

    dbServers[0].runSnapshot();
    dbServers[0].recoverFromSnapshot();
    dbServers[0].getSnapshotManager().lockSnapshot("test");
    dbServers[0].getSnapshotManager().unlockSnapshot(1);

    long commandCount = dbServers[1].getCommandCount();
    dbServers[1].purgeMemory();
    dbServers[1].replayLogs();
//    assertEquals(dbServers[1].getLogManager().getCountLogged(), commandCount);
//    assertEquals(dbServers[1].getCommandCount(), commandCount * 2);

    executor.shutdownNow();
  }

  @Test
  public void testDebug() {
    ((ConnectionProxy) conn).getDatabaseClient().debugRecord("test", "persons", "_1__primarykey", "[100]");
  }

  @Test(enabled = false)
  public void testMulti() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("create table Persons2 (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
    stmt.executeUpdate();

    for (int i = 0; i < 100000; i++) {
      stmt = conn.prepareStatement("insert into persons2 (id, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?)");
      stmt.setLong(1, i);
      stmt.setString(2, "933-28-" + i);
      stmt.setString(3, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
      stmt.setBoolean(4, false);
      stmt.setString(5, "m");
      assertEquals(stmt.executeUpdate(), 1);
      ids.add((long) i);

      stmt = conn.prepareStatement("select * from persons2 where id=" + i + " order by id asc");
      ResultSet ret = stmt.executeQuery();

      ret.next();
      assertEquals(ret.getLong("id"), i);
    }
    System.out.println("complete");
  }

  @Test
  public void testIndex() {
    ConcurrentSkipListMap<Long, Object> map = new ConcurrentSkipListMap<>();
    map.put((long) 6, 6);
    map.put((long) 7, 7);
    Object obj = map.ceilingEntry((long) 5);
    if (obj == null) {
      System.out.println("bad");
    }
    System.out.println(obj);

    obj = map.floorEntry((long) 5);
    System.out.println(obj);
  }

  @Test
  public void testDropIndex() throws SQLException, InterruptedException {

    PreparedStatement stmt = conn.prepareStatement("drop index persons.socialSecurityNumber");
     stmt.executeUpdate();

    IndexSchema indexSchema = null;
        for (Map.Entry<String, IndexSchema> entry : client.getCommon().getTables("test").get("persons").getIndices().entrySet()) {
          if (entry.getValue().getFields()[0].equalsIgnoreCase("socialsecuritynumber")) {
            indexSchema = entry.getValue();
            if (indexSchema.getName().contains("socialsecuritynumber")) {
              fail();
            }
          }
        }



    stmt = conn.prepareStatement("create index socialSecurityNumber on persons(socialSecurityNumber)");
     stmt.executeUpdate();

     Thread.sleep(5000);

    for (Map.Entry<String, IndexSchema> entry : client.getCommon().getTables("test").get("persons").getIndices().entrySet()) {
      if (entry.getValue().getFields()[0].equalsIgnoreCase("socialsecuritynumber")) {
        indexSchema = entry.getValue();
      }
    }

    List<ColumnImpl> columns = new ArrayList<>();
     columns.add(new ColumnImpl(null, null, "persons", "socialsecuritynumber", null));

     AtomicReference<String> usedIndex = new AtomicReference<String>();
     ExpressionImpl.RecordCache recordCache = new ExpressionImpl.RecordCache();
     ParameterHandler parms = new ParameterHandler();
     SelectContextImpl ret = ExpressionImpl.lookupIds(
           "test", client.getCommon(), client, 0,
           1000, client.getCommon().getTables("test").get("persons"), indexSchema, false, BinaryExpression.Operator.equal,
           null,
           null,
           new Object[]{"933-28-0".getBytes()}, parms, null, null,
       new Object[]{"933-28-0".getBytes()},
           null,
           columns, "socialsecuritynumber", 0, recordCache,
           usedIndex, false, client.getCommon().getSchemaVersion(), null, null, false);

     assertEquals(ret.getCurrKeys().length, 4);

  }

  @Test
  public void testDualKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("create table dualKey (id BIGINT, id2 BIGINT, PRIMARY KEY (id, id2))");
    stmt.executeUpdate();

    for (int i = 0; i < 2; i++) {
      stmt = conn.prepareStatement("insert into dualKey (id, id2) VALUES (?, ?)");
      stmt.setLong(1, i);
      stmt.setLong(2, i * 2);
      assertEquals(stmt.executeUpdate(), 1);

      stmt = conn.prepareStatement("insert into dualKey (id, id2) VALUES (?, ?)");
      stmt.setLong(1, i);
      stmt.setLong(2, i * 4 + 1);
      assertEquals(stmt.executeUpdate(), 1);
    }

    stmt = conn.prepareStatement("select id, id2 from dualKey where id >= 0");
    ResultSet ret = stmt.executeQuery();
    ret.next();
    assertEquals(ret.getLong("id"), 0);
    assertEquals(ret.getLong("id2"), 0);
    ret.next();
    assertEquals(ret.getLong("id"), 0);
    assertEquals(ret.getLong("id2"), 1);
    ret.next();
    assertEquals(ret.getLong("id"), 1);
    assertEquals(ret.getLong("id2"), 2);
    ret.next();
      assertEquals(ret.getLong("id"), 1);
    assertEquals(ret.getLong("id2"), 5);
    assertFalse(ret.next());
  }

  @Test
  public void testAddColumn() throws SQLException {

    PreparedStatement stmt = conn.prepareStatement("create table addColumn (id BIGINT, id2 BIGINT, PRIMARY KEY (id))");
    stmt.executeUpdate();

    for (int i = 0; i < 10; i++) {
      stmt = conn.prepareStatement("insert into addColumn (id, id2) VALUES (?, ?)");
      stmt.setLong(1, i);
      stmt.setLong(2, i * 2);
      assertEquals(stmt.executeUpdate(), 1);
    }

    stmt = conn.prepareStatement("alter table addColumn add column id3 BIGINT");
    stmt.executeUpdate();

    for (int i = 10; i < 20; i++) {
      stmt = conn.prepareStatement("insert into addColumn (id, id2, id3) VALUES (?, ?, ?)");
      stmt.setLong(1, i);
      stmt.setLong(2, i * 2);
      stmt.setLong(3, i * 3);
      assertEquals(stmt.executeUpdate(), 1);
    }

    stmt = conn.prepareStatement("select id, id2, id3 from addColumn where id >= 0");
    ResultSet ret = stmt.executeQuery();
    for (int i = 0; i < 20; i++) {
      ret.next();
      assertEquals(ret.getLong("id"), i);
      if (i >= 10) {
        assertEquals(ret.getLong("id3"), i * 3);
      }
    }

    stmt = conn.prepareStatement("alter table addColumn drop column id3");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("select id, id2, id3 from addColumn where id >= 0");
    ret = stmt.executeQuery();
    for (int i = 0; i < 20; i++) {
      ret.next();
      assertEquals(ret.getLong("id"), i);
      if (i >= 10) {
        assertEquals(ret.getLong("id3"), 0);
      }
    }

    assertFalse(ret.next());


  }

  @Test
  public void testSchema() throws Exception {
    DatabaseCommon common = new DatabaseCommon();
    common.addDatabase("test");
    TableSchema tableSchema = new TableSchema();
    tableSchema.setName("table1");
    List<String> primaryKey = new ArrayList<>();
    primaryKey.add("id");
    tableSchema.setPrimaryKey(primaryKey);
    common.addTable("test", "/data/database", tableSchema);
    common.saveSchema("/data/database");

    common.getTables("test").clear();
    common.loadSchema("/data/database");

    assertEquals(common.getTables("test").size(), 1);
  }

  @Test
  public void testLess() throws Exception {

    PreparedStatement stmt = conn.prepareStatement("select * from persons where id<106 and id>100 order by id2 asc, id desc");
    ResultSet ret = stmt.executeQuery();
    assertTrue(ret.next());
    assertEquals(ret.getInt("id2"), 0);
    assertEquals(ret.getInt("id"), 104);
    assertTrue(ret.next());
    assertEquals(ret.getInt("id2"), 0);
    assertEquals(ret.getInt("id"), 102);
    assertTrue(ret.next());
    assertEquals(ret.getInt("id2"), 1);
    assertEquals(ret.getInt("id"), 105);
    assertTrue(ret.next());
    assertEquals(ret.getInt("id2"), 1);
    assertEquals(ret.getInt("id"), 103);
    assertTrue(ret.next());
    assertEquals(ret.getInt("id2"), 1);
    assertEquals(ret.getInt("id"), 101);
  }

  @Test
  public void testBasics() throws Exception {

    //test select returns multiple records with an index using operator '<'
    PreparedStatement stmt = conn.prepareStatement("select id, id2 from persons where id<5 order by id desc");
    ResultSet ret = stmt.executeQuery();

    assertTrue(ret.isBeforeFirst());
    assertTrue(ret.next());
//    System.out.println(ret.getLong("id"));
//    System.out.println(ret.getLong("id2"));
    ret.getLong("id2");
    assertTrue(ret.wasNull());
    ret.getInt(2);
    assertTrue(ret.wasNull());
    ret.getLong("id2");
    assertTrue(ret.wasNull());
    ret.getLong(2);
    assertTrue(ret.wasNull());
    ret.getDouble("id2");
    assertTrue(ret.wasNull());
    ret.getDouble(2);
    assertTrue(ret.wasNull());
    ret.getFloat("id2");
    assertTrue(ret.wasNull());
    ret.getFloat(2);
    assertTrue(ret.wasNull());
    ret.getString("id2");
    assertTrue(ret.wasNull());
    ret.getString(2);
    assertTrue(ret.wasNull());
    ret.getBigDecimal("id2");
    assertTrue(ret.wasNull());
    ret.getBigDecimal(2);
    assertTrue(ret.wasNull());
    ret.getBinaryStream("id2");
    assertTrue(ret.wasNull());
    ret.getBinaryStream(2);
    assertTrue(ret.wasNull());
    ret.getBytes("id2");
    assertTrue(ret.wasNull());
    ret.getBytes(2);
    assertTrue(ret.wasNull());
    ret.getBoolean("id2");
    assertTrue(ret.wasNull());
    ret.getBoolean(2);
    assertTrue(ret.wasNull());
    ret.getByte("id2");
    assertTrue(ret.wasNull());
    ret.getByte(2);
    assertTrue(ret.wasNull());
    ret.getDate("id2");
    assertTrue(ret.wasNull());
    ret.getDate(2);
    assertTrue(ret.wasNull());
    ret.getCharacterStream("id2");
    assertTrue(ret.wasNull());
//    ret.getCharacterStream(2);
//    assertTrue(ret.wasNull());
    ret.getNClob("id2");
    assertTrue(ret.wasNull());
    ret.getNClob(2);
    assertTrue(ret.wasNull());
    ret.getNString("id2");
    assertTrue(ret.wasNull());
    ret.getNString(2);
    assertTrue(ret.wasNull());
    ret.getShort("id2");
    assertTrue(ret.wasNull());
    ret.getShort(2);
    assertTrue(ret.wasNull());
    ret.getTime("id2");
    assertTrue(ret.wasNull());
    ret.getTime(2);
    assertTrue(ret.wasNull());
    ret.getTimestamp("id2");
    assertTrue(ret.wasNull());
    ret.getTimestamp(2);
    assertTrue(ret.wasNull());
    assertEquals(ret.getRow(), 1);
    assertTrue(ret.isFirst());
    assertEquals(ret.getLong("id"), 4);
    ret.next();
    assertEquals(ret.getLong("id"), 3);
    ret.next();
    assertEquals(ret.getLong("id"), 2);
    ret.next();
    assertEquals(ret.getLong("id"), 1);
    ret.next();
    assertEquals(ret.getLong("id"), 0);
    assertTrue(ret.isLast());
    assertFalse(ret.next());
    assertTrue(ret.isAfterLast());

  }

  @Test
  public void testNotIn() throws SQLException {
    //test select with not in expression
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id not in (5, 6, 7, 8, 9, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109) order by id asc");
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
  public void testIdentity() throws SQLException {
    //test select with not in expression
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id = 5");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 5);
    assertFalse(ret.next());
  }


  @Test
  public void testNoKey() throws SQLException {
    //test select with not in expression
    PreparedStatement stmt = conn.prepareStatement("select * from nokey where id < 5 order by id asc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 0);
    assertEquals(ret.getLong("id2"), 0);
    ret.next();
    assertEquals(ret.getLong("id"), 0);
    assertEquals(ret.getLong("id2"), 0);
    ret.next();
    assertEquals(ret.getLong("id"), 1);
    assertEquals(ret.getLong("id2"), 2);
    ret.next();
    assertEquals(ret.getLong("id"), 1);
    assertEquals(ret.getLong("id2"), 2);
    ret.next();
    assertEquals(ret.getLong("id"), 2);
    assertEquals(ret.getLong("id2"), 4);
  }

  @Test
  public void testNoKey2() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from nokey where id <= 2 and id2 = 4 order by id asc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 2);
    assertEquals(ret.getLong("id2"), 4);
    ret.next();
    assertEquals(ret.getLong("id"), 2);
    assertEquals(ret.getLong("id2"), 4);
    assertFalse(ret.next());
  }


  @Test
  public void testNotInAnd() throws SQLException {
    //test select with not in expression
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id < 4 and id > 1 and id not in (5, 6, 7, 8, 9, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109) order by id desc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 3);
    ret.next();
    assertEquals(ret.getLong("id"), 2);
    assertFalse(ret.next());
  }

  @Test
  public void testAll() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons");
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
    ret.next();
    assertEquals(ret.getLong("id"), 5);
    ret.next();
    assertEquals(ret.getLong("id"), 6);
    ret.next();
    assertEquals(ret.getLong("id"), 7);
    ret.next();
    assertEquals(ret.getLong("id"), 8);
    ret.next();
    assertEquals(ret.getLong("id"), 9);
    ret.next();
    assertEquals(ret.getLong("id"), 100);
    ret.next();
    assertEquals(ret.getLong("id"), 101);
    ret.next();
    assertEquals(ret.getLong("id"), 102);
    ret.next();
    assertEquals(ret.getLong("id"), 103);
    ret.next();
    assertEquals(ret.getLong("id"), 104);
    ret.next();
    assertEquals(ret.getLong("id"), 105);
    ret.next();
    assertEquals(ret.getLong("id"), 106);
    ret.next();
    assertEquals(ret.getLong("id"), 107);
    ret.next();
    assertEquals(ret.getLong("id"), 108);
    ret.next();
    assertEquals(ret.getLong("id"), 109);
    assertFalse(ret.next());
  }

  @Test
  public void testParameters() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id < ? and id > ?");
    stmt.setLong(1, 5);
    stmt.setLong(2, 2);
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 3);
    ret.next();
    assertEquals(ret.getLong("id"), 4);
    assertFalse(ret.next());
  }


  @Test
  public void test2FieldKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from memberships where personId=? and membershipName=?");
    stmt.setLong(1, 0);
    stmt.setString(2, "membership-0");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("personId"), 0);
    assertEquals(ret.getString("membershipName"), "membership-0");
    assertFalse(ret.next());
  }

  @Test
  public void testMax() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select max(id) as maxValue from persons");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("maxValue"), 109);
    assertFalse(ret.next());
  }

  @Test
  public void testMin() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select min(id) as minValue from persons");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("minValue"), 0);
    assertFalse(ret.next());
  }

  @Test
  public void testMaxTableScan() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select max(id) as maxValue from persons where id2 < 1");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("maxValue"), 108);
    assertFalse(ret.next());
  }

  @Test
  public void testMaxWhere() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select max(id) as maxValue from persons where id < 100");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("maxValue"), 9);
    assertFalse(ret.next());
  }

  @Test
  public void testSum() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select sum(id) as sumValue from persons");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("sumValue"), 1090);
    assertFalse(ret.next());
  }

  @Test
  public void testLimit() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id < ? and id > ? limit 3");
    stmt.setLong(1, 100);
    stmt.setLong(2, 2);
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 3);
    ret.next();
    assertEquals(ret.getLong("id"), 4);
    ret.next();
    assertEquals(ret.getLong("id"), 5);
    assertFalse(ret.next());
  }


  @Test
  public void testLimitOffset() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id < ? and id > ? limit 3 offset 2");
    stmt.setLong(1, 100);
    stmt.setLong(2, 2);
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 4);
    ret.next();
    assertEquals(ret.getLong("id"), 5);
    assertFalse(ret.next());
  }


  @Test
  public void tesSort2() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id2 from persons order by id2 asc, id asc");
    ResultSet ret = stmt.executeQuery();

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
    assertEquals(ret.getLong("id"), 0);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertTrue(ret.wasNull());
    assertEquals(ret.getLong("id"), 1);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertTrue(ret.wasNull());
    assertEquals(ret.getLong("id"), 2);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertTrue(ret.wasNull());
    assertEquals(ret.getLong("id"), 3);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertTrue(ret.wasNull());
    assertEquals(ret.getLong("id"), 4);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertTrue(ret.wasNull());
    assertEquals(ret.getLong("id"), 5);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertTrue(ret.wasNull());
    assertEquals(ret.getLong("id"), 6);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertTrue(ret.wasNull());
    assertEquals(ret.getLong("id"), 7);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertTrue(ret.wasNull());
    assertEquals(ret.getLong("id"), 8);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertTrue(ret.wasNull());
    assertEquals(ret.getLong("id"), 9);
    assertFalse(ret.next());
  }


  @Test
  public void testAllSortAnd() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id > 100 and id2 = 0 order by id2 asc, id desc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 108);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 106);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 104);
    ret.next();
    assertEquals(ret.getLong("id2"), 0);
    assertEquals(ret.getLong("id"), 102);
    assertFalse(ret.next());
  }


  @Test
  public void testComplex() throws SQLException {
    //fails

    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select persons.id  " +
        "from persons where persons.id>=100 AND id < 105 AND ID2=0 OR id> 6 AND ID < 10");                                              //
    ResultSet ret = stmt.executeQuery();


    ret.next();
    assertEquals(ret.getLong("id"), 100);
    ret.next();
    assertEquals(ret.getLong("id"), 102);
    ret.next();
    assertEquals(ret.getLong("id"), 104);
    ret.next();
    assertEquals(ret.getLong("id"), 7);
    ret.next();
    assertEquals(ret.getLong("id"), 8);
    ret.next();
    assertEquals(ret.getLong("id"), 9);

    assertFalse(ret.next());
  }

  @Test
  public void testParens() throws SQLException {
    //fails

    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select persons.id  " +
        "from persons where persons.id<=100 AND (id < 6 OR id> 8)");                                              //
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
    ret.next();
    assertEquals(ret.getLong("id"), 5);
    ret.next();
    assertEquals(ret.getLong("id"), 9);
    ret.next();
    assertEquals(ret.getLong("id"), 100);

    assertFalse(ret.next());
  }

  @Test
  public void testPrecedence() throws SQLException {
    //fails

    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select persons.id  " +
        "from persons where persons.id<=100 AND id > 4 OR id> 103");                                              //
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 5);
    ret.next();
    assertEquals(ret.getLong("id"), 6);
    ret.next();
    assertEquals(ret.getLong("id"), 7);
    ret.next();
    assertEquals(ret.getLong("id"), 8);
    ret.next();
    assertEquals(ret.getLong("id"), 9);
    ret.next();
    assertEquals(ret.getLong("id"), 100);
    ret.next();
    assertEquals(ret.getLong("id"), 104);
    ret.next();
    assertEquals(ret.getLong("id"), 105);
    ret.next();
    assertEquals(ret.getLong("id"), 106);
    ret.next();
    assertEquals(ret.getLong("id"), 107);
    ret.next();
    assertEquals(ret.getLong("id"), 108);
    ret.next();
    assertEquals(ret.getLong("id"), 109);

    assertFalse(ret.next());
  }


  @Test
  public void testOverlapPrecedence() throws SQLException {
    //fails

    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select persons.id  " +
        "from persons where persons.id<=100 AND id < 4 OR id> 103");                                              //
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
    assertEquals(ret.getLong("id"), 104);
    ret.next();
    assertEquals(ret.getLong("id"), 105);
    ret.next();
    assertEquals(ret.getLong("id"), 106);
    ret.next();
    assertEquals(ret.getLong("id"), 107);
    ret.next();
    assertEquals(ret.getLong("id"), 108);
    ret.next();
    assertEquals(ret.getLong("id"), 109);

    assertFalse(ret.next());
  }

  @Test
  public void testOverlapPrecedence2() throws SQLException {
    //fails

    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select persons.id  " +
        "from persons where persons.id<=100 AND id = 4 OR id> 103");                                              //
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 4);
    ret.next();
    assertEquals(ret.getLong("id"), 104);
    ret.next();
    assertEquals(ret.getLong("id"), 105);
    ret.next();
    assertEquals(ret.getLong("id"), 106);
    ret.next();
    assertEquals(ret.getLong("id"), 107);
    ret.next();
    assertEquals(ret.getLong("id"), 108);
    ret.next();
    assertEquals(ret.getLong("id"), 109);

    assertFalse(ret.next());
  }

  @Test
  public void testAvg() throws SQLException {
    //fails

    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select avg(persons.id) as avgValue from persons");                                              //
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getDouble("avgValue"), 54.5d);
    assertFalse(ret.next());
  }

  @Test(enabled = false)
  public void testAvg2() throws SQLException {
    //fails

    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select persons.id from persons where id > (select avg(id) from persons)");                                              //
    ResultSet ret = stmt.executeQuery();

    assertTrue(false);
    ret.next();
    assertEquals(ret.getLong("id"), 100);
    ret.next();
    assertEquals(ret.getLong("id"), 102);
    ret.next();
    assertEquals(ret.getLong("id"), 104);
    ret.next();
    assertEquals(ret.getLong("id"), 7);
    ret.next();
    assertEquals(ret.getLong("id"), 8);
    ret.next();
    assertEquals(ret.getLong("id"), 9);

    assertFalse(ret.next());
  }

  @Test
  public void testOr() throws SQLException {
    //fails

    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id>105 and id2=0 or id<105 and id2=1 order by id desc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 108);
    ret.next();
    assertEquals(ret.getLong("id"), 106);
    ret.next();
    assertEquals(ret.getLong("id"), 103);
    ret.next();
    assertEquals(ret.getLong("id"), 101);
    assertFalse(ret.next());
//    assertFalse(ret.next());

//    Set<Long> found = new HashSet<>();
//    while (true) {
//      if (!ret.next()) {
//        break;
//      }
//      found.add(ret.getLong("id"));
//    }
//    assertEquals(found.size(), 5);
//
//    assertTrue(found.contains(0L));
//    assertTrue(found.contains(1L));
//    assertTrue(found.contains(2L));
//    assertTrue(found.contains(3L));
//    assertTrue(found.contains(4L));
  }

  @Test
  public void serverSort() throws SQLException {

    PreparedStatement stmt = conn.prepareStatement("select persons.id, socialsecuritynumber as s " +
        "from persons where persons.id>100 AND id < 105 order by socialsecuritynumber desc");                                              //
    ResultSet ret = stmt.executeQuery();
    try {
      ret.next();
      assertEquals(ret.getLong("id"), 103);
      assertEquals(ret.getString("socialsecuritynumber"), "933-28-3");
      ret.next();
      assertEquals(ret.getLong("id"), 102);
      assertEquals(ret.getString("socialsecuritynumber"), "933-28-2");
      ret.next();
      assertEquals(ret.getLong("id"), 101);
      assertEquals(ret.getString("socialsecuritynumber"), "933-28-1");
      ret.next();
      assertEquals(ret.getLong("id"), 104);
      assertEquals(ret.getString("socialsecuritynumber"), "933-28-0");
      assertFalse(ret.next());
    }
    finally {
      ret.close();
    }
  }

  @Test
  public void testMixed() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select persons.id, socialsecuritynumber as s " +
        "from persons where socialsecuritynumber > '933-28-6' AND persons.id>5 AND id < 10");                                              //
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 7);
    assertEquals(ret.getString("s"), "933-28-7");
    ret.next();
    assertEquals(ret.getLong("id"), 8);
    assertEquals(ret.getString("s"), "933-28-8");
    ret.next();
    assertEquals(ret.getLong("id"), 9);
    assertEquals(ret.getString("s"), "933-28-9");
    assertFalse(ret.next());
  }

  @Test
  public void testOrAnd() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select persons.id  " +
        "from persons where persons.id>2 AND id < 4 OR id> 6 AND ID < 8");                                              //
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 3);
    ret.next();
    assertEquals(ret.getLong("id"), 7);
    ret.next();
    assertFalse(ret.next());
  }

  @Test
  public void testEqualNonIndex() throws SQLException {
    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id2=1 order by id desc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 109);
    ret.next();
    assertEquals(ret.getLong("id"), 107);
    ret.next();
    assertEquals(ret.getLong("id"), 105);
    ret.next();
    assertEquals(ret.getLong("id"), 103);
    ret.next();
    assertEquals(ret.getLong("id"), 101);
    assertFalse(ret.next());
  }

  @Test
  public void testEqualIndex() throws SQLException {
    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id=1 order by id desc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testIn() throws SQLException {

    //test select with in expression
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id in (0, 1, 2, 3, 4)");
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
  public void testSecondaryIndex() throws SQLException {

    //test select on secondary index
    PreparedStatement stmt;
    ResultSet ret;
    for (int i = 0; i < recordCount; i++) {

      //test jdbc select
      stmt = conn.prepareStatement("select * from persons where socialSecurityNumber=? order by id");
      stmt.setString(1, "933-28-" + i);
      ret = stmt.executeQuery();
      assertTrue(ret.next());

      long retId = ret.getLong("id");
      String socialSecurityNumber = ret.getString("socialSecurityNumber");
      String retRelatives = ret.getString("relatives");
      boolean restricted = ret.getBoolean("restricted");
      String gender = ret.getString("gender");

      assertEquals(retId, i, "Returned id doesn't match: id=" + i + ", retId=" + retId);
      assertEquals(socialSecurityNumber, "933-28-" + i);
      assertNotNull(retRelatives);
      assertNotEquals(retRelatives.length(), 0);
      assertFalse(restricted);
      assertNotNull(gender);
      assertNotEquals(gender.length(), 0);
      assertEquals(gender.charAt(0), 'm');
    }
  }

  @Test
  public void testMultipleFields() throws SQLException {
    //test select on multiple fields
    PreparedStatement stmt;
    ResultSet ret;
    for (int i = 0; i < recordCount; i++) {

      //test jdbc select
      stmt = conn.prepareStatement("select * from persons where id=" + (i + 100) + " AND id2=" + ((i + 100) % 2));
      ret = stmt.executeQuery();

      assertTrue(ret.next());

      long retId = ret.getLong("id");
      long retId2 = ret.getLong("id2");
      String socialSecurityNumber = ret.getString("socialSecurityNumber");
      String retRelatives = ret.getString("relatives");
      boolean restricted = ret.getBoolean("restricted");
      String gender = ret.getString("gender");

      assertEquals(retId, i + 100, "Returned id doesn't match: id=" + i + ", retId=" + retId);
      assertEquals(retId2, (i + 100) % 2, "Returned id2 doesn't match: id2=" + (i + 100) % 2 + ", retId2=" + retId2);
      assertEquals(socialSecurityNumber, "933-28-" + (i % 4));
      assertNotNull(retRelatives);
      assertNotEquals(retRelatives.length(), 0);
      assertFalse(restricted);
      assertNotNull(gender);
      assertNotEquals(gender.length(), 0);
      assertEquals(gender.charAt(0), 'm');
    }
  }

  @Test
  public void testAnd() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id!=0 AND id!=1 AND id!=2 AND id!=3 AND id!=4 order by id asc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 5);
    ret.next();
    assertEquals(ret.getLong("id"), 6);
    ret.next();
    assertEquals(ret.getLong("id"), 7);
    ret.next();
    assertEquals(ret.getLong("id"), 8);
    ret.next();
    assertEquals(ret.getLong("id"), 9);
    ret.next();
    assertEquals(ret.getLong("id"), 100);
    ret.next();
    assertEquals(ret.getLong("id"), 101);
    ret.next();
    assertEquals(ret.getLong("id"), 102);
    ret.next();
    assertEquals(ret.getLong("id"), 103);
    ret.next();
    assertEquals(ret.getLong("id"), 104);
    ret.next();
    assertEquals(ret.getLong("id"), 105);
    ret.next();
    assertEquals(ret.getLong("id"), 106);
    ret.next();
    assertEquals(ret.getLong("id"), 107);
    ret.next();
    assertEquals(ret.getLong("id"), 108);
    ret.next();
    assertEquals(ret.getLong("id"), 109);
    assertFalse(ret.next());
  }

  @Test
  public void testOrTableScan() throws SQLException {
    //test select returns multiple records with a table scan
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id2=1 or id2=0 order by id2 asc, id desc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 108);
    ret.next();
    assertEquals(ret.getLong("id"), 106);
    ret.next();
    assertEquals(ret.getLong("id"), 104);
    ret.next();
    assertEquals(ret.getLong("id"), 102);
    ret.next();
    assertEquals(ret.getLong("id"), 100);
    ret.next();
    assertEquals(ret.getLong("id"), 109);
    ret.next();
    assertEquals(ret.getLong("id"), 107);
    ret.next();
    assertEquals(ret.getLong("id"), 105);
    ret.next();
    assertEquals(ret.getLong("id"), 103);
    ret.next();
    assertEquals(ret.getLong("id"), 101);
    assertFalse(ret.next());

//    Set<Long> found = new HashSet<>();
//    for (int i = 101; i < 110; i += 2) {
//      ret.next();
//      found.add(ret.getLong("id"));
//    }
//    assertEquals(found.size(), 5);
//
//    assertTrue(found.contains(101L));
//    assertTrue(found.contains(103L));
//    assertTrue(found.contains(105L));
//    assertTrue(found.contains(107L));
//    assertTrue(found.contains(109L));
  }

  @Test
  public void testOrIndex() throws SQLException {
    //test select returns multiple records with an index
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id=0 OR id=1 OR id=2 OR id=3 OR id=4");
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
  public void testLessEqual() throws SQLException {

    //test select returns multiple records with an index using operator '<='
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id<=5 order by id desc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 5);
    ret.next();
    assertEquals(ret.getLong("id"), 4);
    ret.next();
    assertEquals(ret.getLong("id"), 3);
    ret.next();
    assertEquals(ret.getLong("id"), 2);
    ret.next();
    assertEquals(ret.getLong("id"), 1);
    ret.next();
    assertEquals(ret.getLong("id"), 0);
    assertFalse(ret.next());
  }

  @Test
  public void testLessEqualAndGreaterEqual() throws SQLException {

    //test select returns multiple records with an index using operator '<='
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id<=5 and id>=1 order by id desc");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 5);
    ret.next();
    assertEquals(ret.getLong("id"), 4);
    ret.next();
    assertEquals(ret.getLong("id"), 3);
    ret.next();
     assertEquals(ret.getLong("id"), 2);
    ret.next();
    assertEquals(ret.getLong("id"), 1);
    assertFalse(ret.next());
  }

  @Test
  public void testGreater() throws SQLException {
    //test select returns multiple records with an index using operator '>'
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id>5");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 6);
    ret.next();
    assertEquals(ret.getLong("id"), 7);
    ret.next();
    assertEquals(ret.getLong("id"), 8);
    ret.next();
    assertEquals(ret.getLong("id"), 9);
    ret.next();
    assertEquals(ret.getLong("id"), 100);
    ret.next();
    assertEquals(ret.getLong("id"), 101);
    ret.next();
    assertEquals(ret.getLong("id"), 102);
    ret.next();
    assertEquals(ret.getLong("id"), 103);
    ret.next();
    assertEquals(ret.getLong("id"), 104);
    ret.next();
    assertEquals(ret.getLong("id"), 105);
    ret.next();
    assertEquals(ret.getLong("id"), 106);
    ret.next();
    assertEquals(ret.getLong("id"), 107);
    ret.next();
    assertEquals(ret.getLong("id"), 108);
    ret.next();
    assertEquals(ret.getLong("id"), 109);
    assertFalse(ret.next());
  }

  @Test
  public void testGreaterEqual() throws SQLException {
    //test select returns multiple records with an index using operator '<='
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id>=5");
    ResultSet ret = stmt.executeQuery();

    ret.next();
    assertEquals(ret.getLong("id"), 5);
    ret.next();
    assertEquals(ret.getLong("id"), 6);
    ret.next();
    assertEquals(ret.getLong("id"), 7);
    ret.next();
    assertEquals(ret.getLong("id"), 8);
    ret.next();
    assertEquals(ret.getLong("id"), 9);
    ret.next();
    assertEquals(ret.getLong("id"), 100);
    ret.next();
    assertEquals(ret.getLong("id"), 101);
    ret.next();
    assertEquals(ret.getLong("id"), 102);
    ret.next();
    assertEquals(ret.getLong("id"), 103);
    ret.next();
    assertEquals(ret.getLong("id"), 104);
    ret.next();
    assertEquals(ret.getLong("id"), 105);
    ret.next();
    assertEquals(ret.getLong("id"), 106);
    ret.next();
    assertEquals(ret.getLong("id"), 107);
    ret.next();
    assertEquals(ret.getLong("id"), 108);
    ret.next();
    assertEquals(ret.getLong("id"), 109);
    assertFalse(ret.next());
  }

  @Test
  public void testEqual2() throws SQLException {
    //test select
    PreparedStatement stmt;
    ResultSet ret;
    for (int i = 0; i < recordCount; i++) {

      //test jdbc select
      stmt = conn.prepareStatement("select * from persons where id=" + i);
      ret = stmt.executeQuery();

      ret.next();
      long retId = ret.getLong("id");
      String socialSecurityNumber = ret.getString("socialSecurityNumber");
      String retRelatives = ret.getString("relatives");
      boolean restricted = ret.getBoolean("restricted");
      String gender = ret.getString("gender");

      assertEquals(retId, i, "Returned id doesn't match: id=" + i + ", retId=" + retId);
      assertEquals(socialSecurityNumber, "933-28-" + i);
      assertNotNull(retRelatives);
      assertNotEquals(retRelatives.length(), 0);
      assertFalse(restricted);
      assertNotNull(gender);
      assertNotEquals(gender.length(), 0);
      assertEquals(gender.charAt(0), 'm');

    }
  }

  @Test
  public void testUpdate() throws SQLException {
    //fails

    PreparedStatement stmt = conn.prepareStatement("update persons set id = ?, socialSecurityNumber=? where id=?");
    stmt.setLong(1, 1000);
    stmt.setString(2, "ssn");
    stmt.setLong(3, 0);
    stmt.executeUpdate();

    stmt = conn.prepareStatement("select * from persons where id=" + 0);
    ResultSet ret = stmt.executeQuery();
    assertFalse(ret.next());

    stmt = conn.prepareStatement("select * from persons where socialSecurityNumber='933-28-0'");
    ret = stmt.executeQuery();
    assertTrue(ret.next());
    assertTrue(ret.next());
    assertTrue(ret.next());
    assertFalse(ret.next());

    stmt = conn.prepareStatement("select * from persons where socialSecurityNumber='ssn'");
    ret = stmt.executeQuery();
    assertTrue(ret.next());
    assertEquals(ret.getLong("id"), 1000);
    assertFalse(ret.next());

    stmt = conn.prepareStatement("select * from persons where id=" + 1000);
    ret = stmt.executeQuery();
    assertTrue(ret.next());

    stmt = conn.prepareStatement("select * from persons where socialSecurityNumber='ssn'");
    ret = stmt.executeQuery();
    assertTrue(ret.next());
  }

  @Test
  public void testUpdate2() throws SQLException, InterruptedException {

    PreparedStatement stmt = conn.prepareStatement("insert into persons (id, id2, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?)");
    stmt.setLong(1, 100000);
    stmt.setLong(2, (100) % 2);
    stmt.setString(3, "ssn");
    stmt.setString(4, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
    stmt.setBoolean(5, false);
    stmt.setString(6, "m");
    int count = stmt.executeUpdate();
    assertEquals(count, 1);

    stmt = conn.prepareStatement("update persons set id = ?, socialSecurityNumber=? where id=?");
    stmt.setLong(1, 101000);
    stmt.setString(2, "ssn2");
    stmt.setLong(3, 100000);
    count = stmt.executeUpdate();
    assertEquals(count, 1);

    stmt = conn.prepareStatement("select * from persons where id=101000");
    ResultSet resultSet = stmt.executeQuery();
    assertTrue(resultSet.next());

    stmt = conn.prepareStatement("select * from persons where id=100000");
    resultSet = stmt.executeQuery();
    assertFalse(resultSet.next());

    stmt = conn.prepareStatement("delete from persons where id=101000");
    stmt.executeUpdate();
  }

  @Test
  public void testInsert() throws SQLException, InterruptedException {

    PreparedStatement stmt = conn.prepareStatement("insert into persons (id, id2, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?)");
    stmt.setLong(1, 200000);
    stmt.setLong(2, (100) % 2);
    stmt.setString(3, "ssn");
    stmt.setString(4, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
    stmt.setBoolean(5, false);
    stmt.setString(6, "m");
    int count = stmt.executeUpdate();
    assertEquals(count, 1);

    try {
      stmt = conn.prepareStatement("insert into persons (id, id2, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?)");
      stmt.setLong(1, 200000);
      stmt.setLong(2, (100) % 2);
      stmt.setString(3, "ssn");
      stmt.setString(4, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
      stmt.setBoolean(5, false);
      stmt.setString(6, "m");
      count = stmt.executeUpdate();
      assertEquals(count, 1);
    }
    catch (Exception e) {
      //expected
    }

    stmt = conn.prepareStatement("select * from persons where id=200000");
    ResultSet resultSet = stmt.executeQuery();
    assertTrue(resultSet.next());
    assertFalse(resultSet.next());

    stmt = conn.prepareStatement("delete from persons where id=200000");
    stmt.executeUpdate();
  }

  @Test
  public void testDelete() throws SQLException {


    for (int i = 2000; i < recordCount; i++) {
      PreparedStatement stmt = conn.prepareStatement("insert into persons (id, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?)");
      stmt.setLong(1, i);
      stmt.setString(2, "933-28-" + i);
      stmt.setString(3, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
      stmt.setBoolean(4, false);
      stmt.setString(5, "m");
      assertEquals(stmt.executeUpdate(), 1);
      ids.add((long) i);
    }

    //test remove
    PreparedStatement stmt;
    for (int i = 2000; i < recordCount; i++) {
      //if (i % 2 == 0) {
      //test jdbc remove

      stmt = conn.prepareStatement("delete from persons where id=?");
      stmt.setLong(1, i);
      assertEquals(1, stmt.executeUpdate());

    }


    for (int i = 2000; i < recordCount; i++) {
      stmt = conn.prepareStatement("select * from persons where id=" + i);
      ResultSet ret = stmt.executeQuery();
      assertFalse(ret.next());
    }

  }


  @Test(enabled = false)
  public void testPaging() throws Exception {
    String configStr = StreamUtils.inputStreamToString(new BufferedInputStream(new FileInputStream("config/config-4-servers-large.json")));
    final JsonDict config = new JsonDict(configStr);

    FileUtils.deleteDirectory(new File("/data/database"));

    DatabaseServer.getServers().clear();

    final DatabaseServer[] dbServers = new DatabaseServer[4];
    ThreadPoolExecutor executor = new ThreadPoolExecutor(32, 32, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < dbServers.length; i++) {
      final int shard = i;
      futures.add(executor.submit(new Callable() {
        @Override
        public Object call() throws Exception {
          String role = "primaryMaster";
          dbServers[shard] = new DatabaseServer();
          dbServers[shard].setConfig(config, "test", "localhost", 9010 + (50 * shard), true, new AtomicBoolean(true));
          dbServers[shard].setRole(role);
          dbServers[shard].disableLogProcessor();
          return null;
        }
      }));
    }
    for (Future future : futures) {
      future.get();
    }


    DatabaseClient client = new DatabaseClient("localhost", 9010, true);

    Class.forName("com.lowryengineering.database.jdbcdriver.DriverProxy");

    Connection conn = DriverManager.getConnection("jdbc:dbproxy:127.0.0.1:9000", "user", "password");

    PreparedStatement stmt = conn.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create index socialSecurityNumber on persons(socialSecurityNumber)");
    stmt.executeUpdate();

    List<Long> ids = new ArrayList<>();

    //test insert
    int recordCount = 2000;

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into persons (id, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?)");
      stmt.setLong(1, i);
      stmt.setString(2, "933-28-" + i);
      stmt.setString(3, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
      stmt.setBoolean(4, false);
      stmt.setString(5, "m");
      assertEquals(stmt.executeUpdate(), 1);
      ids.add((long) i);
    }

    client.beginRebalance("test", "persons", "_primarykey");

    while (true) {
      if (client.isRepartitioningComplete("test")) {
        break;
      }
      Thread.sleep(1000);
    }

    System.out.println(client.getPartitionSize("test", 0, "persons", "_primarykey"));
    System.out.println(client.getPartitionSize("test", 1, "persons", "_primarykey"));
//    System.out.println(client.getPartitionSize(2, "persons", "_primarykey"));
//    System.out.println(client.getPartitionSize(3, "persons", "_primarykey"));


//    stmt = conn.prepareStatement("select * from persons where id = 0");
//    ResultSet ret = stmt.executeQuery();

    //test select returns multiple records with an index using operator '<='
    stmt = conn.prepareStatement("select * from persons where id>=0");
    ResultSet ret = stmt.executeQuery();

    for (int i = 0; i < recordCount; i++) {
      ret.next();
      try {
        assertEquals(ret.getLong("id"), i);
      }
      catch (Exception e) {
        System.out.println("currId=" + i);
        throw e;
      }
    }

    executor.shutdownNow();
  }

  @Test(enabled = false)
  public void testMultiValue() throws Exception {
    String configStr = StreamUtils.inputStreamToString(new BufferedInputStream(new FileInputStream("config/config-4-servers-large.json")));
    final JsonDict config = new JsonDict(configStr);

    FileUtils.deleteDirectory(new File("/data/database"));

    DatabaseServer.getServers().clear();

    final DatabaseServer[] dbServers = new DatabaseServer[4];
    ThreadPoolExecutor executor = new ThreadPoolExecutor(32, 32, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < dbServers.length; i++) {
      final int shard = i;
      futures.add(executor.submit(new Callable() {
        @Override
        public Object call() throws Exception {
          String role = "primaryMaster";
          dbServers[shard] = new DatabaseServer();
          dbServers[shard].setConfig(config, "test", "localhost", 9010 + (50 * shard), true, new AtomicBoolean(true));
          dbServers[shard].setRole(role);
          dbServers[shard].disableLogProcessor();
          return null;
        }
      }));
    }
    for (Future future : futures) {
      future.get();
    }


    DatabaseClient client = new DatabaseClient("localhost", 9010, true);

    Class.forName("com.lowryengineering.database.jdbcdriver.DriverProxy");

    Connection conn = DriverManager.getConnection("jdbc:dbproxy:127.0.0.1:9000", "user", "password");

    PreparedStatement stmt = conn.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create index socialSecurityNumber on persons(socialSecurityNumber)");
    stmt.executeUpdate();

    List<Long> ids = new ArrayList<>();

    //test insert
    int recordCount = 20000;

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into persons (id, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?)");
      stmt.setLong(1, i);
      stmt.setString(2, "933-28-" + (i % 4));
      stmt.setString(3, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
      stmt.setBoolean(4, false);
      stmt.setString(5, "m");
      assertEquals(stmt.executeUpdate(), 1);
      ids.add((long) i);
    }

    client.beginRebalance("test", "persons", "_primarykey");

    while (true) {
      if (client.isRepartitioningComplete("test")) {
        break;
      }
      Thread.sleep(1000);
    }

    System.out.println(client.getPartitionSize("test", 0, "persons", "_primarykey"));
    System.out.println(client.getPartitionSize("test", 1, "persons", "_primarykey"));
    //    System.out.println(client.getPartitionSize(2, "persons", "_primarykey"));
    //    System.out.println(client.getPartitionSize(3, "persons", "_primarykey"));

    stmt = conn.prepareStatement("select * from persons where socialsecuritynumber > '933-28-' order by id asc");
    ResultSet ret = stmt.executeQuery();

    for (int i = 0; i < recordCount; i++) {
      ret.next();
      try {
        assertEquals(ret.getLong("id"), i, "index=" + i);
      }
      catch (Exception e) {
        throw new Exception("index=" + i, e);
      }
    }

    executor.shutdownNow();
  }

  @Test(enabled = false)
  public void testDescending() throws Exception {
    String configStr = StreamUtils.inputStreamToString(new BufferedInputStream(new FileInputStream("config/config-4-servers-large.json")));
    final JsonDict config = new JsonDict(configStr);

    FileUtils.deleteDirectory(new File("/data/database"));

    DatabaseServer.getServers().clear();

    final DatabaseServer[] dbServers = new DatabaseServer[4];
    ThreadPoolExecutor executor = new ThreadPoolExecutor(32, 32, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < dbServers.length; i++) {
      final int shard = i;
      futures.add(executor.submit(new Callable() {
        @Override
        public Object call() throws Exception {
          String role = "primaryMaster";
          dbServers[shard] = new DatabaseServer();
          dbServers[shard].setConfig(config, "test", "localhost", 9010 + (50 * shard), true, new AtomicBoolean(true));
          dbServers[shard].setRole(role);
          dbServers[shard].disableLogProcessor();
          return null;
        }
      }));
    }
    for (Future future : futures) {
      future.get();
    }


    DatabaseClient client = new DatabaseClient("localhost", 9010, true);

    Class.forName("com.lowryengineering.database.jdbcdriver.DriverProxy");

    Connection conn = DriverManager.getConnection("jdbc:dbproxy:127.0.0.1:9000", "user", "password");

    PreparedStatement stmt = conn.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create index socialSecurityNumber on persons(socialSecurityNumber)");
    stmt.executeUpdate();

    List<Long> ids = new ArrayList<>();

    //test insert
    int recordCount = 20000;

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into persons (id, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?)");
      stmt.setLong(1, i);
      stmt.setString(2, "933-28-" + (i % 4));
      stmt.setString(3, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
      stmt.setBoolean(4, false);
      stmt.setString(5, "m");
      assertEquals(stmt.executeUpdate(), 1);
      ids.add((long) i);
    }

    client.beginRebalance("test", "persons", "_primarykey");

    while (true) {
      if (client.isRepartitioningComplete("test")) {
        break;
      }
      Thread.sleep(1000);
    }

    System.out.println(client.getPartitionSize("test", 0, "persons", "_primarykey"));
    System.out.println(client.getPartitionSize("test", 1, "persons", "_primarykey"));
    //    System.out.println(client.getPartitionSize(2, "persons", "_primarykey"));
    //    System.out.println(client.getPartitionSize(3, "persons", "_primarykey"));


//    stmt = conn.prepareStatement("select * from persons where id = 0");
//    ResultSet ret = stmt.executeQuery();

    stmt = conn.prepareStatement("select * from persons where id <= 19999 order by id asc");
    ResultSet ret = stmt.executeQuery();

    for (int i = 0; i < recordCount; i++) {
      ret.next();
      try {
        assertEquals(ret.getLong("id"), i, "index=" + i);
      }
      catch (Exception e) {
        throw new Exception("index=" + i, e);
      }
    }

    stmt = conn.prepareStatement("select * from persons where id < 20000 order by id desc");
    ret = stmt.executeQuery();

    for (int i = recordCount - 1; i >= 1; i--) {
      ret.next();
      try {
        assertEquals(ret.getLong("id"), i, "index=" + i);
      }
      catch (Exception e) {
        throw new Exception("index=" + i, e);
      }
    }


    stmt = conn.prepareStatement("select * from persons where id < 20000 order by id asc");
    ret = stmt.executeQuery();

    for (int i = 0; i < recordCount; i++) {
      ret.next();
      try {
        assertEquals(ret.getLong("id"), i, "index=" + i);
      }
      catch (Exception e) {
        throw new Exception("index=" + i, e);
      }
    }

    stmt = conn.prepareStatement("select * from persons where id > 0 order by id desc");
    ret = stmt.executeQuery();

    for (int i = recordCount - 1; i >= 1; i--) {
      ret.next();
      try {
        assertEquals(ret.getLong("id"), i, "index=" + i);
      }
      catch (Exception e) {
        throw new Exception("index=" + i, e);
      }
    }

    stmt = conn.prepareStatement("select * from persons where id > 0 order by id asc");
    ret = stmt.executeQuery();

    for (int i = 1; i < recordCount; i++) {
      ret.next();
      try {
        assertEquals(ret.getLong("id"), i, "index=" + i);
      }
      catch (Exception e) {
        throw new Exception("index=" + i, e);
      }
    }

    executor.shutdownNow();
  }


  @Test
  public void testTruncate() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("create table ToTruncate (id BIGINT, id2 BIGINT, PRIMARY KEY (id))");
    stmt.executeUpdate();

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into ToTruncate (id, id2) VALUES (?, ?)");
      stmt.setLong(1, i);
      stmt.setLong(2, i * 2);
      assertEquals(stmt.executeUpdate(), 1);
    }

    stmt = conn.prepareStatement("select * from ToTruncate where id > 0");
    ResultSet rs = stmt.executeQuery();
    assertTrue(rs.next());
    assertEquals(rs.getLong("id"), 1);

    PreparedStatement stmt2 = conn.prepareStatement("truncate table ToTruncate");
    assertTrue(stmt2.execute());

    rs = stmt.executeQuery();
    assertFalse(rs.next());
  }

  @Test
  public void testDropTable() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("create table ToDrop (id BIGINT, id2 BIGINT, PRIMARY KEY (id))");
    stmt.executeUpdate();

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into ToDrop (id, id2) VALUES (?, ?)");
      stmt.setLong(1, i);
      stmt.setLong(2, i * 2);
      assertEquals(stmt.executeUpdate(), 1);
    }

    stmt = conn.prepareStatement("select * from ToDrop where id > 0");
    ResultSet rs = stmt.executeQuery();
    assertTrue(rs.next());
    assertEquals(rs.getLong("id"), 1);

    PreparedStatement stmt2 = conn.prepareStatement("drop table ToDrop");
    assertTrue(stmt2.execute());

    try {
      rs = stmt.executeQuery();
      fail();
    }
    catch (SQLException e) {
      //expected
    }

    try {
      stmt = conn.prepareStatement("insert into ToDrop (id, id2) VALUES (?, ?)");
      stmt.setLong(1, 0);
      stmt.setLong(2, 0 * 2);
      assertEquals(stmt.executeUpdate(), 0);
      fail();
    }
    catch (SQLException e) {
      //expected
    }
  }

  static String algorithm = "DESede";

  public static void xmain(String[] args) throws Exception {

    SecretKey symKey = KeyGenerator.getInstance(algorithm).generateKey();
    symKey = new SecretKeySpec(Base64.decode(DatabaseServer.LICENSE_KEY), algorithm);

    System.out.println("key=" + new String(Base64.encode(symKey.getEncoded()), "utf-8"));

    Cipher c = Cipher.getInstance(algorithm);

    byte[] encryptionBytes = encryptF("sonicbase:pro:4", symKey, c);

    System.out.println("encrypted: " + new String(new Hex().encode(encryptionBytes), "utf-8"));
    System.out.println("Decrypted: " + decryptF(encryptionBytes, symKey, c));

    symKey = new SecretKeySpec(Base64.decode(DatabaseServer.LICENSE_KEY), algorithm);
    System.out.println("Decrypted: " + decryptF(encryptionBytes, symKey, c));

  }

  private static byte[] encryptF(String input, Key pkey, Cipher c) throws InvalidKeyException, BadPaddingException,

      IllegalBlockSizeException {

    c.init(Cipher.ENCRYPT_MODE, pkey);

    byte[] inputBytes = input.getBytes();
    return c.doFinal(inputBytes);
  }

  private static String decryptF(byte[] encryptionBytes, Key pkey, Cipher c) throws InvalidKeyException,
      BadPaddingException, IllegalBlockSizeException {
    c.init(Cipher.DECRYPT_MODE, pkey);
    byte[] decrypt = c.doFinal(encryptionBytes);
    String decrypted = new String(decrypt);

    return decrypted;
  }


  @Test
   public void testExplain() throws SQLException {
     PreparedStatement stmt = conn.prepareStatement("explain select * from persons where id < 100");
     ResultSet ret = stmt.executeQuery();

     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement("explain select * from persons where id < 100 and id > 10");
     ret = stmt.executeQuery();

     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement("explain select * from persons where id < 100 or id > 10");
     ret = stmt.executeQuery();

     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement("explain select * from persons where id < 100 and id > 10 and id2 > 10");
     ret = stmt.executeQuery();

     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement("explain select * from persons where id < 100 and id > 10 and id2 > 10 and id2 < 1");
     ret = stmt.executeQuery();

     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement("explain select * from persons where id < 100 and id > 10 and id > 10 and id < 1");
     ret = stmt.executeQuery();

     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement("explain select * from persons where id2 < 100");
     ret = stmt.executeQuery();

     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement("explain select personId, membershipname from memberships where personid=1 and membershipname='name'");
     ret = stmt.executeQuery();

     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();


     stmt = conn.prepareStatement("explain select max(id) as maxValue from persons");
     ret = stmt.executeQuery();

     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement("explain select max(id) as maxValue from persons where id2 < 1");
     ret = stmt.executeQuery();

     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement("explain select max(id) as maxValue from persons where id < 100");
     ret = stmt.executeQuery();

     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement("explain select sum(id) as sumValue from persons");
     ret = stmt.executeQuery();

     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement("explain select * from persons where id < 100 and id > 200 limit 3");
     ret = stmt.executeQuery();

     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement("explain select * from persons where id < 100 and id > 200 limit 3 offset 2");
     ret = stmt.executeQuery();

     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement("explain select * from persons order by id2 asc, id desc");
     ret = stmt.executeQuery();

     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement("explain select persons.id  from persons where persons.id>=100 AND id < 100AND ID2=0 OR id> 6 AND ID < 100");
     ret = stmt.executeQuery();

     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement("explain select * from persons where id>105 and id2=0 or id<105 and id2=1 order by id desc");
     ret = stmt.executeQuery();

     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement("explain select persons.id from persons where persons.id>2 AND id < 100 OR id> 6 AND ID < 200");
     ret = stmt.executeQuery();

     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement("explain select * from persons where id2=1 order by id desc");
     ret = stmt.executeQuery();

     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement("explain select * from persons where id in (0, 1, 2, 3, 4)");
     ret = stmt.executeQuery();

     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement("explain select * from persons where socialSecurityNumber='555'");
     ret = stmt.executeQuery();

     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement("explain select * from persons where id2=1 or id2=0 order by id2 asc, id desc");
     ret = stmt.executeQuery();

     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement("explain select * from persons where id=0 OR id=1 OR id=2 OR id=3 OR id=4");
     ret = stmt.executeQuery();

     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();


     stmt = conn.prepareStatement("explain select persons.id, socialsecuritynumber, memberships.personid  " +
         "from persons inner join Memberships on persons.id = Memberships.personId inner join resorts on memberships.resortid = resorts.resortid" +
         " where persons.id<1000");
     ret = stmt.executeQuery();
     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement(
         "explain select persons.id, persons.id2, persons.socialsecuritynumber, memberships.personId, memberships.personid, memberships.membershipname from persons " +
             " inner join Memberships on persons.id = Memberships.PersonId and memberships.personid = persons.id2  where persons.id > 0 order by persons.id desc");
     ret = stmt.executeQuery();
     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement(
         "explain select persons.id, persons.socialsecuritynumber, memberships.membershipname, memberships.personid from memberships " +
             "left outer join persons on persons.id = Memberships.PersonId where memberships.personid<1000 order by memberships.personid desc");
     ret = stmt.executeQuery();
     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement(
         "explain select persons.id, persons.socialsecuritynumber, memberships.membershipname from persons " +
             " inner join Memberships on Memberships.PersonId = persons.id and memberships.personid < 1000 where persons.id > 0 order by persons.id desc");
     ret = stmt.executeQuery();
     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();

     stmt = conn.prepareStatement(
         "select persons.id, persons.socialsecuritynumber, memberships.personId, memberships.membershipname from persons " +
             "right outer join Memberships on persons.id = Memberships.PersonId where persons.id<1000 order by persons.id desc");
     ret = stmt.executeQuery();
     while (ret.next()) {
       System.out.println(ret.getString(1));
     }
     System.out.println();
   }
}

