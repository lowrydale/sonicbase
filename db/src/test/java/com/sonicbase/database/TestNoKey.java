/* © 2017 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.database;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.ComObject;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.server.DatabaseServer;
import com.sonicbase.util.JsonArray;
import com.sonicbase.util.JsonDict;
import com.sonicbase.util.StreamUtils;
import org.codehaus.plexus.util.FileUtils;
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

/**
 * Created by lowryda on 8/26/17.
 */
public class TestNoKey {

  private Connection conn;
  private int recordCount = 10;
  List<Long> ids = new ArrayList<>();

  DatabaseClient client = null;

  @BeforeClass
  public void beforeClass() throws Exception {
    try {
      String configStr = StreamUtils.inputStreamToString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-4-servers.json")));
      final JsonDict config = new JsonDict(configStr);

      JsonArray array = config.putArray("licenseKeys");
      array.add(DatabaseServer.FOUR_SERVER_LICENSE);

      FileUtils.deleteDirectory(new File("/data/database"));
      FileUtils.deleteDirectory(new File("/data/db-backup"));

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
        dbServers[shard].setConfig(config, "4-servers", "localhost", 9010 + (50 * shard), true, new AtomicBoolean(true), null, true);
        dbServers[shard].setRole(role);
        dbServers[shard].disableLogProcessor();
        dbServers[shard].setMinSizeForRepartition(0);
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
        cobj.put(ComObject.Tag.method, "areAllLongRunningCommandsComplete");
        byte[] bytes = ((ConnectionProxy) conn).getDatabaseClient().sendToMaster("DatabaseServer:ComObject:areAllLongRunningCommandsComplete:1:test", cobj);
        ComObject retObj = new ComObject(bytes);
        if (retObj.getBoolean(ComObject.Tag.isComplete)) {
          break;
        }
        Thread.sleep(1000);
      }


      client.beginRebalance("test", "persons", "_1__primarykey");


      while (true) {
        if (client.isRepartitioningComplete("test")) {
          break;
        }
        Thread.sleep(1000);
      }

      //Thread.sleep(60000);

      long count = client.getPartitionSize("test", 0, "nokeysecondaryindex", "_1__primarykey");
      assertEquals(count, 4);
      count = client.getPartitionSize("test", 1, "nokeysecondaryindex", "_1__primarykey");
      assertEquals(count, 6);
      count = client.getPartitionSize("test", 0, "nokeysecondaryindex", "_1_id");
      assertEquals(count, 4);
      count = client.getPartitionSize("test", 1, "nokeysecondaryindex", "_1_id");
      assertEquals(count, 6);

      long commandCount = dbServers[1].getCommandCount();
//      dbServers[2].purgeMemory();
//      dbServers[2].recoverFromSnapshot();
//      dbServers[2].replayLogs();
//      dbServers[3].purgeMemory();
//      dbServers[3].recoverFromSnapshot();
//      dbServers[3].replayLogs();


//      JsonDict backupConfig = new JsonDict("{\n" +
//          "    \"type\" : \"fileSystem\",\n" +
//          "    \"directory\": \"/data/db-backup\",\n" +
//          "    \"period\": \"daily\",\n" +
//          "    \"time\": \"23:00\",\n" +
//          "    \"maxBackupCount\": 10,\n" +
//          "    \"sharedDirectory\": true\n" +
//          "  }");
//
//      for (DatabaseServer dbServer : dbServers) {
//        dbServer.setBackupConfig(backupConfig);
//      }
//
//      client.startBackup();
//      while (true) {
//        Thread.sleep(1000);
//        if (client.isBackupComplete()) {
//          break;
//        }
//      }
//
//      Thread.sleep(5000);
//
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
//      client.sendToAllShards(null, 0, command, cobj, DatabaseClient.Replica.all);

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
      rs.next();
      assertEquals(rs.getLong("id"), i);
    }
    assertFalse(rs.next());
  }

}
