
package com.sonicbase.database;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.common.Record;
import com.sonicbase.index.Index;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.server.DatabaseServer;
import com.sonicbase.util.JsonArray;
import com.sonicbase.util.JsonDict;
import com.sonicbase.util.StreamUtils;
import org.codehaus.plexus.util.FileUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Created by lowryda on 8/28/17.
 */
public class TestRepartitionerConsistency {

  private Connection conn;

  DatabaseClient client = null;
  final DatabaseServer[] dbServers = new DatabaseServer[4];

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


      String role = "primaryMaster";

      for (int i = 0; i < dbServers.length; i++) {
        final int shard = i;
        dbServers[shard] = new DatabaseServer();
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

      for (DatabaseServer server : dbServers) {
        server.shutdownRepartitioner();
      }

      //DatabaseClient client = new DatabaseClient("localhost", 9010, true);

      Class.forName("com.sonicbase.jdbcdriver.Driver");

      conn = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9000", "user", "password");

      ((ConnectionProxy) conn).getDatabaseClient().createDatabase("test");

      conn.close();

      conn = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9000/test", "user", "password");

      client = ((ConnectionProxy) conn).getDatabaseClient();

      //client.setPageSize(3);

      PreparedStatement stmt = conn.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
      stmt.executeUpdate();
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void test() {
    try {

      final AtomicLong highestId = new AtomicLong();
      Thread thread = new Thread(new Runnable(){
        @Override
        public void run() {
          try {
            for (int i = 0; ; i++) {
              PreparedStatement stmt = conn.prepareStatement("insert into persons (id, id2) VALUES (?, ?)");
              stmt.setLong(1, i );
              stmt.setLong(2, (i + 100) % 2);
              int count = stmt.executeUpdate();
              assertEquals(count, 1);
              highestId.set(i);

              if (highestId.get() % 10000 == 0) {
                System.out.println("insert progress: count=" + highestId.get());
              }
            }
          }
          catch (Exception e) {
            e.printStackTrace();
          }
        }
      });
      thread.start();

      thread = new Thread(new Runnable(){
        @Override
        public void run() {
          while (true) {
            try {
//              client.beginRebalance("test", "persons", "_1__primarykey");
//
//
//              while (true) {
//                if (client.isRepartitioningComplete("test")) {
//                  break;
//                }
//                Thread.sleep(1000);
//              }
              Thread.sleep(5000);
              System.out.println("finished repartitioning");
            }
            catch (Exception e) {
              e.printStackTrace();
            }
          }
        }
      });
      thread.start();

      thread = new Thread(new Runnable(){
        @Override
        public void run() {
          long lastHighest = 0;
          while (true) {
            try {
              PreparedStatement stmt = conn.prepareStatement("select * from persons where id >= 0");
              ResultSet rs = stmt.executeQuery();
              while (lastHighest == highestId.get()) {
                Thread.sleep(500);
              }
              long highest = highestId.get();
              lastHighest = highest;
              for (int i = 0; i < highest; i++) {
                //Thread.sleep(1);
                long id = -1;
//                rs.next();
                if (!rs.next()) {
                  System.out.println("didn't reach end: " + i);
                }
                else {
                  id = rs.getLong("id");
                }
                if (id != i) {
                  IndexSchema schema = dbServers[0].getCommon().getTables("test").get("persons").getIndexes().get("_1__primarykey");
                  Index index0 = dbServers[0].getIndices().get("test").getIndices().get("persons").get("_1__primarykey");
                  Index index1 = dbServers[2].getIndices().get("test").getIndices().get("persons").get("_1__primarykey");
                  Index index0_1 = dbServers[1].getIndices().get("test").getIndices().get("persons").get("_1__primarykey");
                  Index index1_1 = dbServers[3].getIndices().get("test").getIndices().get("persons").get("_1__primarykey");

                  String debug0 = getRecordDebug(i, index0, dbServers[0]);
                  String debug1 = getRecordDebug(i, index1, dbServers[2]);
                  String debug0_1 = getRecordDebug(i, index0_1, dbServers[1]);
                  String debug1_1 = getRecordDebug(i, index1_1, dbServers[3]);
                  String debuglast0 = getRecordDebug((long)index0.lastEntry().getKey()[0], index0, dbServers[0]);
                  String debuglast1 = index1.lastEntry() == null ? "" : getRecordDebug((long)index1.lastEntry().getKey()[0], index1, dbServers[2]);
                  String debugId0 = getRecordDebug(id, index0, dbServers[0]);
                  String debugId1 = getRecordDebug(id, index1, dbServers[2]);
                  TableSchema.Partition[] partitions = schema.getCurrPartitions();
                  TableSchema.Partition[] lastPartitions = schema.getLastPartitions();
                  System.out.println("schemaVersion=" + dbServers[0].getSchemaVersion() + ", lastUpperKey=" + (lastPartitions == null ? "null" : DatabaseCommon.keyToString(lastPartitions[0].getUpperKey())) +
                      ", upperKey=" + DatabaseCommon.keyToString(partitions[0].getUpperKey()) +
                    ", last0=" + DatabaseCommon.keyToString(index0.lastEntry().getKey()) + ", shard0=" + dbServers[0].getShard() +
                      ", lastRecord0=" + debuglast0 +
                      ", last1=" + (index1.lastEntry() == null ? "" : DatabaseCommon.keyToString(index1.lastEntry().getKey())) + ", shard1=" + dbServers[2].getShard() +
                      ", lastRecord1=" + debuglast1 +
                  ", first0=" + DatabaseCommon.keyToString(index0.firstEntry().getKey()) + ", shard0=" + dbServers[0].getShard() +
                      ", first1=" + (index1.firstEntry() == null ? "" : DatabaseCommon.keyToString(index1.firstEntry().getKey())) + ", shard1=" + dbServers[2].getShard() +
                    ", size0=" + index0.size() + ", size1=" + index1.size() + ", nextRecord0: " + debug0 + ", nextRecord1=" + debug1 +
                      ", nextRecord0_1: " + debug0_1 + ", nextRecord1_1=" + debug1_1 +
                      ", foundRecord0: " + debugId0 + ", foundRecord1=" + debugId1);

                  throw new Exception(id + " != " + i);
                }
              }
              System.out.println("finished range: " + lastHighest);
            }
            catch (Exception e) {
              try {
                Thread.sleep(1000);
              }
              catch (InterruptedException e1) {
                e1.printStackTrace();
              }
              e.printStackTrace();
            }
          }
        }
      });
      thread.start();

      while (true) {
        Thread.sleep(1000);
      }
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }

  private String getRecordDebug(long i, Index index1, DatabaseServer dbServer) {
    String debug = "recordNotFound=" + i;
    Object[] key = new Object[]{(long)i};
    synchronized (index1.getMutex(key)) {
      Object value = index1.get(key);
      if (value != null && !value.equals(0L)) {
        debug = "recordFound=" + i;
        byte[][] bytes = dbServer.fromUnsafeToRecords(value);
        if (bytes == null) {
//          while (bytes == null) {
//            System.out.println("nullRecord value=" + value);
//            try {
//              Thread.sleep(100);
//            }
//            catch (InterruptedException e) {
//              e.printStackTrace();
//            }
//            value = index1.get(key);
//            if (value != null && !value.equals(0L)) {
//              bytes = dbServers[0].fromUnsafeToRecords(value);
//            }
//          }

          debug += ", nullRecord";
        }
        else {
          if (bytes.length > 1) {
            throw new DatabaseException("More than one record");
          }
          long flags = Record.getDbViewFlags(bytes[0]);
          if ((flags & Record.DB_VIEW_FLAG_DELETING) != 0) {
            debug += ", flag=deleting";
          }
          if ((flags & Record.DB_VIEW_FLAG_ADDING) != 0) {
            debug += ", flag=adding";
          }
          debug += ", ver=" + Record.getDbViewNumber(bytes[0]);
        }
      }
    }
    return debug;
  }

}
