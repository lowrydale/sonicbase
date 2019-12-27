package com.sonicbase.accept.server;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.ComObject;
import com.sonicbase.common.Config;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.server.DatabaseServer;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.testng.Assert.assertEquals;

public class TestSnapshotManagerLostEntries {

  final ConcurrentHashMap<Long, Long> foundIds = new ConcurrentHashMap<>();
  final AtomicInteger countPlayed = new AtomicInteger();
  final int recordCount = 10_000;

  class MonitorServer extends com.sonicbase.server.DatabaseServer {
    public byte[] invokeMethod(final byte[] body, long logSequence0, long logSequence1,
                               boolean replayedCommand, boolean enableQueuing, AtomicLong timeLogging, AtomicLong handlerTime) {
      if (replayedCommand) {
        if (countPlayed.incrementAndGet() % 10000 == 0) {
          System.out.println("count=" + countPlayed.get());
        }
        ComObject cobj = new ComObject(body);
        long value = cobj.getLong(ComObject.Tag.COUNT_LONG);
        if (null != foundIds.put(value, value)) {
          System.out.println("Value already set");
        }
      }
      return super.invokeMethod(body, logSequence0, logSequence1, replayedCommand, enableQueuing, timeLogging, handlerTime);
    }
  }


  @Test
  public void test() throws Exception {
    System.setProperty("log4j.configuration", "test-log4j.xml");

    String configStr = IOUtils.toString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-4-servers.yaml")), "utf-8");
    Config config = new Config(configStr);

    FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db-data"));

    DatabaseClient.getServers().clear();

    com.sonicbase.server.DatabaseServer[] dbServers = new com.sonicbase.server.DatabaseServer[4];
    ThreadPoolExecutor executor = new ThreadPoolExecutor(32, 32, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

    String role = "primaryMaster";

    for (int i = 0; i < dbServers.length; i++) {
      dbServers[i] = new MonitorServer();
      Config.copyConfig("4-servers");
      dbServers[i].setConfig(config, "localhost", 9010 + (50 * i), true, new AtomicBoolean(true), new AtomicBoolean(true),null, false);
      dbServers[i].setRole(role);
    }

    Connection conn = null;
    try {
      com.sonicbase.server.DatabaseServer.initDeathOverride(2, 2);
      com.sonicbase.server.DatabaseServer.deathOverride[0][0] = false;
      com.sonicbase.server.DatabaseServer.deathOverride[0][1] = false;
      com.sonicbase.server.DatabaseServer.deathOverride[1][0] = false;
      com.sonicbase.server.DatabaseServer.deathOverride[1][1] = false;

      dbServers[0].enableSnapshot(false);
      dbServers[1].enableSnapshot(false);
      dbServers[2].enableSnapshot(false);
      dbServers[3].enableSnapshot(false);


      Class.forName("com.sonicbase.jdbcdriver.Driver");

      conn = DriverManager.getConnection("jdbc:sonicbase:localhost:9010", "user", "password");

      ((ConnectionProxy) conn).getDatabaseClient().createDatabase("test");

      conn.close();

      conn = DriverManager.getConnection("jdbc:sonicbase:localhost:9010/test", "user", "password");

      DatabaseClient client = ((ConnectionProxy) conn).getDatabaseClient();

      PreparedStatement stmt = conn.prepareStatement("create table Persons (id BIGINT, PRIMARY KEY (id))");
      stmt.executeUpdate();

      for (int i = 0; i < recordCount; i++) {
        stmt = conn.prepareStatement("insert into persons (id) VALUES (?)");
        stmt.setLong(1, i);
        assertEquals(stmt.executeUpdate(), 1);
        if (i % 10_000 == 0) {
          System.out.println("upsert progress: count=" + i);
        }
      }

      dbServers[0].runSnapshot();
      dbServers[1].runSnapshot();
      dbServers[2].runSnapshot();
      dbServers[3].runSnapshot();
    }
    finally {
      conn.close();

      dbServers[0].shutdown();
      dbServers[1].shutdown();
      dbServers[2].shutdown();
      dbServers[3].shutdown();
    }

    configStr = IOUtils.toString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-4-servers.yaml")), "utf-8");
    config = new Config(configStr);

    dbServers = new com.sonicbase.server.DatabaseServer[4];

    role = "primaryMaster";

    for (int i = 0; i < dbServers.length; i++) {
      dbServers[i] = new MonitorServer();
      Config.copyConfig("4-servers");
      dbServers[i].setConfig(config, "localhost", 9010 + (50 * i), true, new AtomicBoolean(true), new AtomicBoolean(true),null, false);
      dbServers[i].setRole(role);
    }

    try {
      com.sonicbase.server.DatabaseServer.initDeathOverride(2, 2);
      com.sonicbase.server.DatabaseServer.deathOverride[0][0] = false;
      com.sonicbase.server.DatabaseServer.deathOverride[0][1] = false;
      com.sonicbase.server.DatabaseServer.deathOverride[1][0] = false;
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

      conn = DriverManager.getConnection("jdbc:sonicbase:localhost:9010/test", "user", "password");

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
    finally {
      conn.close();

      dbServers[0].shutdown();
      dbServers[1].shutdown();
      dbServers[2].shutdown();
      dbServers[3].shutdown();
    }

    System.out.println("client refCount=" + DatabaseClient.clientRefCount.get());
  }
}
