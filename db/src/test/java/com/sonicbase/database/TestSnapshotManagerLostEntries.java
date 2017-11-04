package com.sonicbase.database;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.ComObject;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.server.DatabaseServer;
import org.apache.commons.io.IOUtils;
import org.codehaus.plexus.util.FileUtils;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.sql.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.testng.Assert.assertEquals;

public class TestSnapshotManagerLostEntries {

  ConcurrentHashMap<Long, Long> foundIds = new ConcurrentHashMap<>();
  AtomicInteger countPlayed = new AtomicInteger();
  int recordCount = 10_000_000;

  class MonitorServer extends DatabaseServer {
    public byte[] invokeMethod(final byte[] body, long logSequence0, short logSequence1,
                                boolean replayedCommand, boolean enableQueuing, AtomicLong timeLogging, AtomicLong handlerTime) {
      if (replayedCommand) {
        if (countPlayed.incrementAndGet() % 10000 == 0) {
          System.out.println("count=" + countPlayed.get());
        }
        ComObject cobj = new ComObject(body);
        long value = cobj.getLong(ComObject.Tag.countLong);
        if (null != foundIds.put(value, value)) {
          System.out.println("Value already set");
        }
      }
      return super.invokeMethod(body, logSequence0, logSequence1, replayedCommand, enableQueuing, timeLogging, handlerTime);
    }
  }


  @Test
  public void test() throws Exception {
    String configStr = IOUtils.toString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-4-servers.json")), "utf-8");
    ObjectMapper mapper = new ObjectMapper();
    final ObjectNode config = (ObjectNode) mapper.readTree(configStr);

    FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db"));

    ArrayNode array = new ArrayNode(JsonNodeFactory.instance);
    array.add(DatabaseServer.FOUR_SERVER_LICENSE);
    config.put("licenseKeys", array);

    DatabaseServer.getServers().clear();

    final DatabaseServer[] dbServers = new DatabaseServer[4];
    ThreadPoolExecutor executor = new ThreadPoolExecutor(32, 32, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

    String role = "primaryMaster";

    for (int i = 0; i < dbServers.length; i++) {
      final int shard = i;
      dbServers[shard] = new MonitorServer();
      dbServers[shard].setConfig(config, "4-servers", "localhost", 9010 + (50 * shard), true, new AtomicBoolean(true), null, true);
      dbServers[shard].setRole(role);
      dbServers[shard].disableLogProcessor();
      dbServers[shard].setMinSizeForRepartition(0);
    }


    DatabaseServer.initDeathOverride(2, 2);
    DatabaseServer.deathOverride[0][0] = false;
    DatabaseServer.deathOverride[0][1] = false;
    DatabaseServer.deathOverride[1][0] = false;
    DatabaseServer.deathOverride[1][1] = false;

    dbServers[0].enableSnapshot(false);
    dbServers[1].enableSnapshot(false);
    dbServers[2].enableSnapshot(false);
    dbServers[3].enableSnapshot(false);


    Class.forName("com.sonicbase.jdbcdriver.Driver");

    Connection conn = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9000", "user", "password");

    ((ConnectionProxy) conn).getDatabaseClient().createDatabase("test");

    conn.close();

    conn = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9000/test", "user", "password");

    DatabaseClient client = ((ConnectionProxy) conn).getDatabaseClient();

    PreparedStatement stmt = conn.prepareStatement("create table Persons (id BIGINT, PRIMARY KEY (id))");
    stmt.executeUpdate();

    for (int i = 0; i < recordCount; i++) {
      stmt = conn.prepareStatement("insert into persons (id) VALUES (?)");
      stmt.setLong(1, i);
      assertEquals(stmt.executeUpdate(), 1);
      if (i % 10_000 == 0) {
        System.out.println("insert progress: count=" + i);
      }
    }

    dbServers[0].runSnapshot();
    dbServers[1].runSnapshot();
    dbServers[2].runSnapshot();
    dbServers[3].runSnapshot();


  }

  @Test
  public void validate() throws Exception {
    String configStr = IOUtils.toString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-4-servers.json")), "utf-8");
    ObjectMapper mapper = new ObjectMapper();
    final ObjectNode config = (ObjectNode) mapper.readTree(configStr);

    FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db"));

    ArrayNode array = new ArrayNode(JsonNodeFactory.instance);
    array.add(DatabaseServer.FOUR_SERVER_LICENSE);
    config.put("licenseKeys", array);

    final DatabaseServer[] dbServers = new DatabaseServer[4];

    String role = "primaryMaster";

    for (int i = 0; i < dbServers.length; i++) {
      final int shard = i;
      dbServers[shard] = new MonitorServer();
      dbServers[shard].setConfig(config, "4-servers", "localhost", 9010 + (50 * shard), true, new AtomicBoolean(true), null, true);
      dbServers[shard].setRole(role);
      dbServers[shard].disableLogProcessor();
      dbServers[shard].setMinSizeForRepartition(0);
    }


    DatabaseServer.initDeathOverride(2, 2);
    DatabaseServer.deathOverride[0][0] = false;
    DatabaseServer.deathOverride[0][1] = false;
    DatabaseServer.deathOverride[1][0] = false;
    DatabaseServer.deathOverride[1][1] = false;

    dbServers[0].enableSnapshot(false);
    dbServers[1].enableSnapshot(false);
    dbServers[2].enableSnapshot(false);
    dbServers[3].enableSnapshot(false);

    dbServers[0].recoverFromSnapshot();
    dbServers[1].recoverFromSnapshot();
    dbServers[2].recoverFromSnapshot();
    dbServers[3].recoverFromSnapshot();


    Class.forName("com.sonicbase.jdbcdriver.Driver");

    Connection conn = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9000/test", "user", "password");

    DatabaseClient client = ((ConnectionProxy) conn).getDatabaseClient();

    int countMissing = 0;
    Long firstMissing = null;
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id=?");
    for (int i = 0; i < recordCount; i++) {
      stmt.setLong(1, i);
      ResultSet rs = stmt.executeQuery();
      if (!rs.next()) {
        if (firstMissing == null) {
          firstMissing = rs.getLong("id");
        }
        countMissing++;
      }
      else {
        assertEquals(rs.getLong("id"), i);
      }
      if (i % 10_000 == 0) {
        System.out.println("read progress: count=" + i);
      }
    }
    System.out.println("missing count=" + countMissing + ", firstMissing=" + firstMissing);
  }
}
