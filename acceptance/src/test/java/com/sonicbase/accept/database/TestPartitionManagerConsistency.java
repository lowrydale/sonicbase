
package com.sonicbase.accept.database;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.Config;
import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.common.Record;
import com.sonicbase.index.Index;
import com.sonicbase.server.PartitionManager;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.jdbcdriver.ResultSetProxy;
import com.sonicbase.query.BinaryExpression;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.server.DatabaseServer;
import com.sonicbase.util.PartitionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.testng.Assert.assertEquals;

public class TestPartitionManagerConsistency {

  private Connection conn;

  DatabaseClient client = null;
  final DatabaseServer[] dbServers = new DatabaseServer[4];

  @BeforeClass
  public void beforeClass() {
    try {
      String configStr = IOUtils.toString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-4-servers.yaml")), "utf-8");
      Config config = new Config(configStr);

      FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db-data"));

      DatabaseClient.getServers().clear();


      String role = "primaryMaster";

      for (int i = 0; i < dbServers.length; i++) {
        dbServers[i] = new DatabaseServer();
        dbServers[i].setConfig(config, "4-servers", "localhost", 9010 + (50 * i), true, new AtomicBoolean(true), new AtomicBoolean(true),null, false);
        dbServers[i].setRole(role);
      }

      dbServers[0].getMasterManager().promoteToMaster(null, false);

      DatabaseServer.initDeathOverride(2, 2);
      DatabaseServer.deathOverride[0][0] = false;
      DatabaseServer.deathOverride[0][1] = false;
      DatabaseServer.deathOverride[1][0] = false;
      DatabaseServer.deathOverride[1][1] = false;

      //DatabaseClient client = new DatabaseClient("localhost", 9010, true);

      Class.forName("com.sonicbase.jdbcdriver.Driver");

      conn = DriverManager.getConnection("jdbc:sonicbase:localhost:9010", "user", "password");

      ((ConnectionProxy) conn).getDatabaseClient().createDatabase("test");

      conn.close();

      conn = DriverManager.getConnection("jdbc:sonicbase:localhost:9010/test", "user", "password");

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
      Thread thread = new Thread(() -> {
        try {
          for (int i = 0; ;) {
            PreparedStatement stmt = conn.prepareStatement("insert into persons (id, id2) VALUES (?, ?)");
            for (int j = 0; j < 5; j++) {
              stmt.setLong(1, i);
              stmt.setLong(2, (i + 100) % 2);
              stmt.addBatch();
              highestId.set(i);

              Thread.sleep(1);

              if (highestId.get() % 10000 == 0) {
                System.out.println("upsert progress: count=" + highestId.get());
              }
              i++;
            }
            stmt.executeBatch();
          }
        }
        catch (Exception e) {
          e.printStackTrace();
        }
      });
      thread.start();

      thread = new Thread(() -> {
        long lastHighest = 0;
        while (true) {
          try {
            PreparedStatement stmt = conn.prepareStatement("select * from persons where id >= 0");
            ResultSet rs = stmt.executeQuery();
            while (lastHighest == highestId.get()) {
              Thread.sleep(500);
            }
            Thread.sleep(50);
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
                Map<Integer, Index> indices = new HashMap<>();
                Map<Integer, DatabaseServer> dbServersByShard = new HashMap<>();
                for (int j = 0; j < dbServers.length; j++) {
                  if (dbServers[j].getReplica() == 0) {
                    indices.put(dbServers[j].getShard(), dbServers[j].getIndices().get("test").getIndices().get("persons").get("_primarykey"));
                    dbServersByShard.put(dbServers[j].getShard(), dbServers[j]);
                  }
                }
                IndexSchema schema = dbServers[0].getCommon().getTables("test").get("persons").getIndices().get("_primarykey");
                Index index0 = dbServers[0].getIndices().get("test").getIndices().get("persons").get("_primarykey");
                Index index1 = dbServers[2].getIndices().get("test").getIndices().get("persons").get("_primarykey");
                Index index0_1 = dbServers[1].getIndices().get("test").getIndices().get("persons").get("_primarykey");
                Index index1_1 = dbServers[3].getIndices().get("test").getIndices().get("persons").get("_primarykey");

                TableSchema.Partition[] partitions = schema.getCurrPartitions();
                TableSchema.Partition[] lastPartitions = schema.getLastPartitions();
                StringBuilder last = new StringBuilder();
                StringBuilder curr = new StringBuilder();
                for (int j = 0; j < partitions.length; j++) {
                  appendUpperKey(j, partitions, curr);
                }
                synchronized (PartitionManager.getPreviousPartitions()) {
                  List<PartitionManager.PartitionEntry> list = PartitionManager.getPreviousPartitions().get("persons:_primarykey");
                  for (int j = Math.min(4, list.size() - 1); j >= 0; j--) {
                    last.append("last(").append(j).append(")");
                    PartitionManager.PartitionEntry entry = list.get(j);
                    if (entry == null || entry.getPartitions() == null) {
                      last.append("null");
                    }
                    else {
                      for (int k = 0; k < entry.getPartitions().length; k++) {
                        appendUpperKey(k, entry.getPartitions(), last);
                      }
                    }
                  }
                }

                TableSchema tableSchema = dbServers[0].getCommon().getTables("test").get("persons");
                IndexSchema indexSchema = tableSchema.getIndices().get("_primarykey");
                String[] indexFields = indexSchema.getFields();
                int[] fieldOffsets = new int[indexFields.length];
                for (int k = 0; k < indexFields.length; k++) {
                  fieldOffsets[k] = tableSchema.getFieldOffset(indexFields[k]);
                }

                int lastShard = ((ResultSetProxy)rs).getLastShard();
                boolean isCurrPartitions = ((ResultSetProxy)rs).isCurrPartitions();

                boolean currPartitions = false;
                List<Integer> selectedShards = PartitionUtils.findOrderedPartitionForRecord(false, true, tableSchema,
                    "_primarykey", null, BinaryExpression.Operator.EQUAL, null, new Object[]{i},
                    null);
                if (selectedShards.size() == 0) {
                  selectedShards = PartitionUtils.findOrderedPartitionForRecord(true, false, tableSchema,
                      indexSchema.getName(), null, BinaryExpression.Operator.EQUAL, null, new Object[]{i}, null);
                  currPartitions = true;
                }
                StringBuilder found = new StringBuilder();
                found.append(i).append("=");
                for (Map.Entry<Integer, Index> entry : indices.entrySet()) {
                  found.append(getRecordDebug(i, entry.getValue(), dbServersByShard.get(entry.getKey()))).append(",");
                }
                long viewVersion = ((ResultSetProxy)rs).getViewVersion();
                System.out.println("schemaVersion=" + dbServers[0].getSchemaVersion() + ", viewVersion=" + viewVersion  +
                    ", currShard(" + (isCurrPartitions ? "curr" : "last") + ")=" + lastShard + //selectedShards.get(0) +
                    ",currUpperKey=" + curr.toString() +
                    ", lastUpperKey=" + last.toString() +
                    ", found=" + found.toString() +
                  ", last0=" + DatabaseCommon.keyToString(index0.lastEntry().getKey()) + ", shard0=" + dbServers[0].getShard() +
                    ", last1=" + (index1.lastEntry() == null ? "" : DatabaseCommon.keyToString(index1.lastEntry().getKey())) + ", shard1=" + dbServers[2].getShard() +
                ", first0=" + DatabaseCommon.keyToString(index0.firstEntry().getKey()) + ", shard0=" + dbServers[0].getShard() +
                    ", first1=" + (index1.firstEntry() == null ? "" : DatabaseCommon.keyToString(index1.firstEntry().getKey())) + ", shard1=" + dbServers[2].getShard() +
                  ", size0=" + index0.size() + ", size1=" + index1.size());

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
  private void appendUpperKey(int i, TableSchema.Partition[] partitions, StringBuilder curr) {
    curr.append(partitions[i].getUpperKey() == null ? "null" : DatabaseCommon.keyToString(partitions[i].getUpperKey())).append(",");
  }



  private String getRecordDebug(long i, Index index1, DatabaseServer dbServer) {
    String debug = "recordNotFound=" + i;
    Object[] key = new Object[]{(long)i};
    synchronized (index1.getMutex(key)) {
      Object value = index1.get(key);
      if (value != null && !value.equals(0L)) {
        debug = "recordFound=" + i;
        byte[][] bytes = dbServer.getAddressMap().fromUnsafeToRecords(value);
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
