
package com.sonicbase.accept.database;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.ComObject;
import com.sonicbase.common.Config;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.server.DatabaseServer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
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

public class TestNoKey {

  private Connection conn;
  private final int recordCount = 10;
  List<Long> ids = new ArrayList<>();

  DatabaseClient client = null;

  @BeforeClass
  public void beforeClass() throws Exception {
    try {
      String configStr = IOUtils.toString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-4-servers.yaml")), "utf-8");
      Config config = new Config(configStr);

      FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db-data"));

      DatabaseClient.getServers().clear();

      final DatabaseServer[] dbServers = new DatabaseServer[4];
      ThreadPoolExecutor executor = new ThreadPoolExecutor(32, 32, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

      String role = "primaryMaster";

      List<Future> futures = new ArrayList<>();
      for (int i = 0; i < dbServers.length; i++) {
        //      futures.add(executor.submit(new Callable() {
        //        @Override
        //        public Object call() throws Exception {
        //          String role = "primaryMaster";

        dbServers[i] = new DatabaseServer();
        Config.copyConfig("4-servers");
        dbServers[i].setConfig(config, "localhost", 9010 + (50 * i), true, new AtomicBoolean(true), new AtomicBoolean(true),null, false);
        dbServers[i].setRole(role);

        //          return null;
        //        }
        //      }));
      }
      for (Future future : futures) {
        future.get();
      }

      DatabaseServer.initDeathOverride(2, 2);
      DatabaseServer.deathOverride[0][0] = false;
      DatabaseServer.deathOverride[0][1] = false;
      DatabaseServer.deathOverride[1][0] = false;
      DatabaseServer.deathOverride[1][1] = false;

      for (DatabaseServer server : dbServers) {
        server.shutdownRepartitioner();
      }

      dbServers[0].enableSnapshot(false);
      dbServers[1].enableSnapshot(false);
      dbServers[2].enableSnapshot(false);
      dbServers[3].enableSnapshot(false);


      Thread.sleep(5000);

      //DatabaseClient client = new DatabaseClient("localhost", 9010, true);

      Class.forName("com.sonicbase.jdbcdriver.Driver");

      conn = DriverManager.getConnection("jdbc:sonicbase:localhost:9010", "user", "password");

      ((ConnectionProxy) conn).getDatabaseClient().createDatabase("test");

      conn.close();

      conn = DriverManager.getConnection("jdbc:sonicbase:localhost:9010/test", "user", "password");

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
        ComObject cobj = new ComObject(1);
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

      //long commandCount = dbServers[1].getCommandCount();
//      dbServers[2].unsafePurgeMemoryForTests();
//      dbServers[2].recoverFromSnapshot();
//      dbServers[2].replayLogs();
//      dbServers[3].unsafePurgeMemoryForTests();
//      dbServers[3].recoverFromSnapshot();
//      dbServers[3].replayLogs();


//      File file = new File("/data/db-backup");
//      File[] dirs = file.listFiles();
//
//      client.startRestore(dirs[0].getName());
//      while (true) {
//        Thread.sleep(1000);
//        if (client.isRestoreComplete()) {
//          break;
//        }
//      }

//      Thread.sleep(10000);

//      ComObject cobj = new ComObject();
//      cobj.put(ComObject.Tag.dbName, "test");
//      cobj.put(ComObject.Tag.schemaVersion, client.getCommon().getSchemaVersion());
//      cobj.put(ComObject.Tag.method, "forceDeletes");
//      String command = "DatabaseServer:ComObject:forceDeletes:";
//      client.sendToAllShards(null, 0, command, cobj, DatabaseClient.Replica.ALL);

      // Thread.sleep(10000);
      executor.shutdownNow();
    }
    catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  @Test(enabled=false)
  public void test() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from nokeysecondaryindex");
    ResultSet rs = stmt.executeQuery();
    for (int i = 0 ; i < recordCount; i++) {
      rs.next();
      assertEquals(rs.getLong("id"), i);
    }
    assertFalse(rs.next());
  }

}
