
package com.sonicbase.accept.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.ComObject;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.FileUtils;
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
import static org.testng.Assert.assertFalse;

public class TestSecondaryIndex {

  private Connection conn;
  private int recordCount = 10;
  List<Long> ids = new ArrayList<>();

  DatabaseClient client = null;
  private com.sonicbase.server.DatabaseServer[] dbServers;

  @AfterClass(alwaysRun = true)
  public void afterClass() throws SQLException {
    conn.close();

    for (com.sonicbase.server.DatabaseServer server : dbServers) {
      server.shutdown();
    }

    System.out.println("client refCount=" + DatabaseClient.clientRefCount.get() + ", sharedClients=" + DatabaseClient.sharedClients.size());
    for (DatabaseClient client : DatabaseClient.allClients) {
      System.out.println("Stack:\n" + client.getAllocatedStack());
    }

  }

  @BeforeClass
  public void beforeClass() throws Exception {
    try {
      String configStr = IOUtils.toString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-4-servers.json")), "utf-8");
      ObjectMapper mapper = new ObjectMapper();
      final ObjectNode config = (ObjectNode) mapper.readTree(configStr);

      FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db"));
      FileUtils.deleteDirectory(new File("/data/db-backup"));

      DatabaseClient.getServers().clear();

      dbServers = new com.sonicbase.server.DatabaseServer[4];
      ThreadPoolExecutor executor = new ThreadPoolExecutor(32, 32, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

      String role = "primaryMaster";

      List<Future> futures = new ArrayList<>();
      for (int i = 0; i < dbServers.length; i++) {
        final int shard = i;
        //      futures.add(executor.submit(new Callable() {
        //        @Override
        //        public Object call() throws Exception {
        //          String role = "primaryMaster";

        dbServers[shard] = new com.sonicbase.server.DatabaseServer();
        dbServers[shard].setConfig(config, "4-servers", "localhost", 9010 + (50 * shard), true,
            new AtomicBoolean(true), new AtomicBoolean(true),null);
        dbServers[shard].setRole(role);
//        dbServers[shard].disableLogProcessor();
//        dbServers[shard].setMinSizeForRepartition(0);
        //          return null;
        //        }
        //      }));
      }
      for (Future future : futures) {
        future.get();
      }

      com.sonicbase.server.DatabaseServer.initDeathOverride(2, 2);
      com.sonicbase.server.DatabaseServer.deathOverride[0][0] = false;
      com.sonicbase.server.DatabaseServer.deathOverride[0][1] = false;
      com.sonicbase.server.DatabaseServer.deathOverride[1][0] = false;
      com.sonicbase.server.DatabaseServer.deathOverride[1][1] = false;

      for (com.sonicbase.server.DatabaseServer server : dbServers) {
        server.shutdownRepartitioner();
      }

//      dbServers[0].enableSnapshot(false);
//      dbServers[1].enableSnapshot(false);
//      dbServers[2].enableSnapshot(false);
//      dbServers[3].enableSnapshot(false);


      Thread.sleep(5000);

      //DatabaseClient client = new DatabaseClient("localhost", 9010, true);

      Class.forName("com.sonicbase.jdbcdriver.Driver");

      conn = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9000", "user", "password");

      ((ConnectionProxy) conn).getDatabaseClient().createDatabase("test");

      conn.close();

      conn = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9000/test", "user", "password");

      client = ((ConnectionProxy) conn).getDatabaseClient();

      client.setPageSize(3);

      PreparedStatement stmt = conn.prepareStatement("create table nokeysecondaryindex (id BIGINT, id2 BIGINT)");
      stmt.executeUpdate();

      stmt = conn.prepareStatement("create index id on nokeysecondaryindex(id)");
      stmt.executeUpdate();

      for (int i = 0; i < recordCount; i++) {
        stmt = conn.prepareStatement("insert into nokeysecondaryindex (id, id2) VALUES (?, ?)");
        stmt.setLong(1, i);
        stmt.setLong(2, i * 2);
        assertEquals(stmt.executeUpdate(), 1);
      }

      while (true) {
        ComObject cobj = new ComObject();
        cobj.put(ComObject.Tag.METHOD, "DatabaseServer:areAllLongRunningCommandsComplete");
        byte[] bytes = ((ConnectionProxy) conn).getDatabaseClient().sendToMaster(cobj);
        ComObject retObj = new ComObject(bytes);
        if (retObj.getBoolean(ComObject.Tag.IS_COMPLETE)) {
          break;
        }
        Thread.sleep(1000);
      }


      client.beginRebalance("test");


      while (true) {
        if (client.isRepartitioningComplete("test")) {
          break;
        }
        Thread.sleep(1000);
      }

      //Thread.sleep(60000);

      long count = client.getPartitionSize("test", 0, "nokeysecondaryindex", "_primarykey");
      assertEquals(count, 4);
      count = client.getPartitionSize("test", 1, "nokeysecondaryindex", "_primarykey");
      assertEquals(count, 6);
      count = client.getPartitionSize("test", 0, "nokeysecondaryindex", "id");
      assertEquals(count, 4);
      count = client.getPartitionSize("test", 1, "nokeysecondaryindex", "id");
      assertEquals(count, 6);

//      long commandCount = dbServers[1].getCommandCount();
//      dbServers[2].purgeMemory();
//      dbServers[2].recoverFromSnapshot();
//      dbServers[2].replayLogs();
//      dbServers[3].purgeMemory();
//      dbServers[3].recoverFromSnapshot();
//      dbServers[3].replayLogs();

//      Thread.sleep(10000);

      // Thread.sleep(10000);
      executor.shutdownNow();
    }
    catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  public void test() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from nokeysecondaryindex");
    ResultSet rs = stmt.executeQuery();
    for (int i = 0 ; i < recordCount; i++) {
      boolean hasNext = rs.next();
      long value = rs.getLong("id");
      if (!hasNext ||  value != i) {
        System.out.println("not");
      }
      System.out.println("Checking: " + i);

      assertEquals(rs.getLong("id"), i);
      System.out.println("found: _sonicbase_id=" + rs.getLong("_sonicbase_id") + ", id=" + rs.getLong("id"));
    }
    assertFalse(rs.next());
  }

}
