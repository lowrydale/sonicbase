package com.sonicbase.database.basics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.Record;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.query.BinaryExpression;
import com.sonicbase.query.impl.ExpressionImpl;
import com.sonicbase.query.impl.SelectContextImpl;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.server.DatabaseServer;
import org.apache.commons.io.IOUtils;
import org.testng.annotations.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.testng.Assert.assertEquals;

/**
 * Responsible for
 */
public class TestPartsOfJoin {

  @Test(enabled=false)
   public void testBasics() throws Exception {
    String configStr = IOUtils.toString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-4-servers.json")), "utf-8");
    ObjectMapper mapper = new ObjectMapper();
    final ObjectNode config = (ObjectNode) mapper.readTree(configStr);

    org.codehaus.plexus.util.FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db"));

    ArrayNode array = new ArrayNode(JsonNodeFactory.instance);
    array.add(DatabaseServer.FOUR_SERVER_LICENSE);
    config.put("licenseKeys", array);

    DatabaseServer.getServers().clear();

    final DatabaseServer[] dbServers = new DatabaseServer[4];
    ThreadPoolExecutor executor = new ThreadPoolExecutor(32, 32, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

    String role = "primaryMaster";

    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < dbServers.length; i++) {
      final int shard = i;
      futures.add(executor.submit(new Callable() {
        @Override
        public Object call() throws Exception {
          String role = "primaryMaster";

          dbServers[shard] = new DatabaseServer();
          dbServers[shard].setConfig(config, "test", "localhost", 9010 + (50 * shard), true, new AtomicBoolean(true), null, true);
          dbServers[shard].setRole(role);
          dbServers[shard].disableLogProcessor();
          return null;
        }
      }));
    }
    for (Future future : futures) {
      future.get();
    }

    for (DatabaseServer server : dbServers) {
      server.shutdownRepartitioner();
    }

    Class.forName("com.sonicbase.jdbcdriver.Driver");

    Connection conn = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9000", "user", "password");

    PreparedStatement stmt = conn.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create table Memberships (personId BIGINT, membershipName VARCHAR(20), resortId BIGINT, PRIMARY KEY (personId, membershipName))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create table Resorts (resortId BIGINT, resortName VARCHAR(20), PRIMARY KEY (resortId))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create index socialSecurityNumber on persons(socialSecurityNumber)");
    stmt.executeUpdate();

    List<Long> ids = new ArrayList<>();

    //test insert
    int recordCount = 10;

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
      stmt.setString(3, "933-28-" + (i + 100));
      stmt.setString(4, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
      stmt.setBoolean(5, false);
      stmt.setString(6, "m");
      int count = stmt.executeUpdate();
      assertEquals(count, 1);
      ids.add((long) (i + 100));
    }

    DatabaseClient client = ((ConnectionProxy)conn).getDatabaseClient();

    TableSchema tableSchema = client.getCommon().getTables("test").get("persons");

    IndexSchema indexSchema = null;
    for (IndexSchema schema : client.getCommon().getSchema("test").getTables().get("persons").getIndexes().values()) {
      if (schema.isPrimaryKey()) {
        indexSchema = schema;
      }
    }

    ParameterHandler parms = new ParameterHandler();
    ExpressionImpl.RecordCache recordCache = new ExpressionImpl.RecordCache();
    AtomicReference<String> usedIndex = new AtomicReference<>();
    SelectContextImpl ret = ExpressionImpl.lookupIds("test",
          client.getCommon(), client, 0, 1000, tableSchema.getName(), indexSchema.getName(), false, BinaryExpression.Operator.less,
          null, null, new Object[]{100L}, null, null, null, new Object[]{100L}, null, null, "id", 0, recordCache, usedIndex,
        false, client.getCommon().getSchemaVersion(), null, null, false,
        new AtomicLong(), null, null);

    Object[][][] keys = ret.getCurrKeys();
    assertEquals(keys[0][0][0], 9L);
    assertEquals(keys[9][0][0], 0L);
    assertEquals(keys.length, 10);

    Record record = recordCache.get("persons", new Object[]{9L}).getRecord();
    assertEquals(record.getField("id"), 9L);
    record = recordCache.get("persons", new Object[]{0L}).getRecord();
    assertEquals(record.getField("id"), 0L);

    List<ExpressionImpl.IdEntry> keysToRead = new ArrayList<>();
    for (int i = 0; i < keys.length; i++) {
      Object[][] entry = keys[i];
      keysToRead.add(new ExpressionImpl.IdEntry(i, entry[0]));
    }
    tableSchema = client.getCommon().getTables("test").get("memberships");
    Map<Integer, Object[][]> readKeys = ExpressionImpl.readRecords("test", client, 30000, false, tableSchema, keysToRead, new String[]{"personid"}, null, recordCache, 1000);

    List<ExpressionImpl.IdEntry> keysToRead2 = new ArrayList<>();
    for (int i = 0; i < readKeys.size(); i++) {
      Object[][] key = readKeys.get(i);
      for (int j = 0; j < key.length; j++) {
        keysToRead2.add(new ExpressionImpl.IdEntry(i, key[j]));
      }
    }
//
//    Map<Integer, Object[][]> readKeys2 = ExpressionImpl.readRecords(client, tableSchema, keysToRead2, new String[]{"personid", "membershipname"}, recordCache);
//
//    assertEquals(recordCache.get("memberships", readKeys2.get(0)[0]).getFieldName("personId"), 9L);
////    assertEquals(recordCache.get("memberships", readKeys2.get(1)[1]).getFieldName("personId"), 8L);
////    assertEquals(recordCache.get("memberships", readKeys2.get(9)[0]).getFieldName("personId"), 0L);
//    assertEquals(readKeys2.size(), 100);

    stmt = conn.prepareStatement("select id, socialsecuritynumber, memberships.personId, memberships.membershipname, resorts.resortname " +
        "from persons inner join Memberships on persons.id = Memberships.PersonId inner join resorts on memberships.resortid = resorts.resortId " +
        "where persons.id<2  order by persons.id desc, memberships.membershipname desc, resorts.resortname asc");

    ResultSet resultSet = stmt.executeQuery();


    resultSet.next();
      assertEquals(resultSet.getLong("id"), 1);
      assertEquals(resultSet.getString("membershipname"), "membership-9");
      assertEquals(resultSet.getString("resortname"), "resort-2000");
      resultSet.next();
      assertEquals(resultSet.getLong("id"), 1);
      assertEquals(resultSet.getString("membershipname"), "membership-8");
      assertEquals(resultSet.getString("resortname"), "resort-1000");
      resultSet.next();
      assertEquals(resultSet.getLong("id"), 1);
      assertEquals(resultSet.getString("membershipname"), "membership-7");
      assertEquals(resultSet.getString("resortname"), "resort-2000");
      resultSet.next();
      assertEquals(resultSet.getLong("id"), 1);
      assertEquals(resultSet.getString("membershipname"), "membership-6");
      assertEquals(resultSet.getString("resortname"), "resort-1000");
      resultSet.next();
      assertEquals(resultSet.getLong("id"), 1);
      assertEquals(resultSet.getString("membershipname"), "membership-5");
      assertEquals(resultSet.getString("resortname"), "resort-2000");
      resultSet.next();
      assertEquals(resultSet.getLong("id"), 1);
      assertEquals(resultSet.getString("membershipname"), "membership-4");
      assertEquals(resultSet.getString("resortname"), "resort-1000");
      resultSet.next();
      assertEquals(resultSet.getLong("id"), 1);
      assertEquals(resultSet.getString("membershipname"), "membership-3");
      assertEquals(resultSet.getString("resortname"), "resort-2000");
      resultSet.next();
      assertEquals(resultSet.getLong("id"), 1);
      assertEquals(resultSet.getString("membershipname"), "membership-2");
      assertEquals(resultSet.getString("resortname"), "resort-1000");
      resultSet.next();
      assertEquals(resultSet.getLong("id"), 1);
      assertEquals(resultSet.getString("membershipname"), "membership-1");
      assertEquals(resultSet.getString("resortname"), "resort-2000");
      resultSet.next();
      assertEquals(resultSet.getLong("id"), 1);
      assertEquals(resultSet.getString("membershipname"), "membership-0");
      assertEquals(resultSet.getString("resortname"), "resort-1000");
      resultSet.next();
      assertEquals(resultSet.getLong("id"), 0);
      assertEquals(resultSet.getString("membershipname"), "membership-9");
      assertEquals(resultSet.getString("resortname"), "resort-2000");


  }




}
