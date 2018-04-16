
package com.sonicbase.database;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.common.Record;
import com.sonicbase.index.Index;
import com.sonicbase.index.Repartitioner;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.jdbcdriver.ResultSetProxy;
import com.sonicbase.query.BinaryExpression;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.server.DatabaseServer;
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

/**
 * Created by lowryda on 8/28/17.
 */
public class TestRepartitionerConsistencyIdentity {

  private Connection conn;

  DatabaseClient client = null;
  final DatabaseServer[] dbServers = new DatabaseServer[16];

  @BeforeClass
  public void beforeClass() throws Exception {
    try {
      String configStr = IOUtils.toString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-16-servers.json")), "utf-8");
      ObjectMapper mapper = new ObjectMapper();
      final ObjectNode config = (ObjectNode) mapper.readTree(configStr);

      FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db"));

      ArrayNode array = new ArrayNode(JsonNodeFactory.instance);
      array.add(DatabaseServer.FOUR_SERVER_LICENSE);
      config.put("licenseKeys", array);

      DatabaseClient.getServers().clear();


      String role = "primaryMaster";

      for (int i = 0; i < dbServers.length; i++) {
        final int shard = i;
        dbServers[shard] = new DatabaseServer();
        dbServers[shard].setConfig(config, "16-servers", "localhost", 9010 + (50 * shard), true, new AtomicBoolean(true),new AtomicBoolean(true), null, true);
        dbServers[shard].setRole(role);
        dbServers[shard].disableLogProcessor();
        dbServers[shard].setMinSizeForRepartition(0);
      }
      dbServers[0].promoteToMaster(null);

      DatabaseServer.initDeathOverride(8, 2);
      DatabaseServer.deathOverride[0][0] = false;
      DatabaseServer.deathOverride[0][1] = false;
      DatabaseServer.deathOverride[1][0] = false;
      DatabaseServer.deathOverride[1][1] = false;
      DatabaseServer.deathOverride[2][0] = false;
      DatabaseServer.deathOverride[2][1] = false;
      DatabaseServer.deathOverride[3][0] = false;
      DatabaseServer.deathOverride[3][1] = false;
      DatabaseServer.deathOverride[4][0] = false;
      DatabaseServer.deathOverride[4][1] = false;
      DatabaseServer.deathOverride[5][0] = false;
      DatabaseServer.deathOverride[5][1] = false;
      DatabaseServer.deathOverride[6][0] = false;
      DatabaseServer.deathOverride[6][1] = false;
      DatabaseServer.deathOverride[7][0] = false;
      DatabaseServer.deathOverride[7][1] = false;

//      for (DatabaseServer server : dbServers) {
//        server.shutdownRepartitioner();
//      }

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
            for (int i = 0; i < 400000; i++) {
              PreparedStatement stmt = conn.prepareStatement("insert into persons (id, id2) VALUES (?, ?)");
              stmt.setLong(1, i );
              stmt.setLong(2, (i + 100) % 2);
              int count = stmt.executeUpdate();
              assertEquals(count, 1);
              highestId.set(i);

              Thread.sleep(1);

              if (highestId.get() % 10000 == 0) {
                System.out.println("upsert progress: count=" + highestId.get());
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
          long lastHighest = 0;
          while (true) {
            try {
              PreparedStatement stmt = conn.prepareStatement("select * from persons where id = ?");
//              while (lastHighest == highestId.get()) {
//                Thread.sleep(500);
//              }
              long highest = highestId.get();
              lastHighest = highest;
              for (int i = 0; i < highest; i++) {
                stmt.setLong(1, i);
                ResultSet rs = stmt.executeQuery();
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
                      indices.put(dbServers[j].getShard(), dbServers[j].getIndices().get("test").getIndices().get("persons").get("_1__primarykey"));
                      dbServersByShard.put(dbServers[j].getShard(), dbServers[j]);
                    }
                  }
                  IndexSchema schema = dbServers[0].getCommon().getTables("test").get("persons").getIndexes().get("_1__primarykey");
                  Index index0 = dbServers[0].getIndices().get("test").getIndices().get("persons").get("_1__primarykey");
                  Index index1 = dbServers[2].getIndices().get("test").getIndices().get("persons").get("_1__primarykey");
                  Index index0_1 = dbServers[1].getIndices().get("test").getIndices().get("persons").get("_1__primarykey");
                  Index index1_1 = dbServers[3].getIndices().get("test").getIndices().get("persons").get("_1__primarykey");

                  StringBuilder found = new StringBuilder();
                  found.append(i).append("=");
                  for (Map.Entry<Integer, Index> entry : indices.entrySet()) {
                    found.append(getRecordDebug(i, entry.getValue(), dbServersByShard.get(entry.getKey()))).append(",");
                  }
                  long viewVersion = ((ResultSetProxy)rs).getViewVersion();
//                  String debug0 = getRecordDebug(i, index0, dbServers[0]);
//                  String debug1 = getRecordDebug(i, index1, dbServers[2]);
//                  String debug0_1 = getRecordDebug(i, index0_1, dbServers[1]);
//                  String debug1_1 = getRecordDebug(i, index1_1, dbServers[3]);
//                  String debuglast0 = getRecordDebug((long)index0.lastEntry().getKey()[0], index0, dbServers[0]);
//                  String debuglast1 = index1.lastEntry() == null ? "" : getRecordDebug((long)index1.lastEntry().getKey()[0], index1, dbServers[2]);
//                  String debugId0 = getRecordDebug(id, index0, dbServers[0]);
//                  String debugId1 = getRecordDebug(id, index1, dbServers[2]);
                  TableSchema.Partition[] partitions = schema.getCurrPartitions();
                  TableSchema.Partition[] lastPartitions = schema.getLastPartitions();
                  StringBuilder last = new StringBuilder();
                  StringBuilder curr = new StringBuilder();
                  for (int j = 0; j < partitions.length; j++) {
                    appendUpperKey(j, partitions, curr);
                  }
                  synchronized (Repartitioner.previousPartitions) {
                    List<Repartitioner.PartitionEntry> list = Repartitioner.previousPartitions.get("persons:_1__primarykey");
                    for (int j = Math.min(4, list.size() - 1); j >= 0; j--) {
                      last.append("last(" + j + ")");
                      Repartitioner.PartitionEntry entry = list.get(j);
                      for (int k = 0; k < entry.partitions.length; k++) {
                        appendUpperKey(k, entry.partitions, last);
                      }
                    }
                  }
                  int lastShard = ((ResultSetProxy)rs).getLastShard();
                  boolean isCurrPartitions = ((ResultSetProxy)rs).isCurrPartitions();
//                  if (lastPartitions != null) {
//                    for (int j = 0; j < lastPartitions.length; j++) {
//                      appendUpperKey(j, lastPartitions, last);
//                    }
//                  }

                  TableSchema tableSchema = dbServers[0].getCommon().getTables("test").get("persons");
                  IndexSchema indexSchema = tableSchema.getIndices().get("_1__primarykey");
                  String[] indexFields = indexSchema.getFields();
                  int[] fieldOffsets = new int[indexFields.length];
                  for (int k = 0; k < indexFields.length; k++) {
                    fieldOffsets[k] = tableSchema.getFieldOffset(indexFields[k]);
                  }

                  boolean currPartitions = false;
                  List<Integer> selectedShards = DatabaseClient.findOrderedPartitionForRecord(false, true, fieldOffsets, dbServers[0].getCommon(), tableSchema,
                      "_1__primarykey", null, BinaryExpression.Operator.equal, null, new Object[]{i},
                      null);
                  if (selectedShards.size() == 0) {
                    selectedShards = DatabaseClient.findOrderedPartitionForRecord(true, false, fieldOffsets, dbServers[0].getCommon(), tableSchema,
                        indexSchema.getName(), null, BinaryExpression.Operator.equal, null, new Object[]{i}, null);
                    currPartitions = true;
                  }

                    System.out.println("schemaVersion=" + dbServers[0].getSchemaVersion() + ", viewVersion=" + viewVersion  +
                          ", currShard(" + (isCurrPartitions ? "curr" : "last") + ")=" + lastShard + //selectedShards.get(0) +
                          ",currUpperKey=" + curr.toString() +
                      ", lastUpperKey=" + last.toString() +
                    ", last0=" + DatabaseCommon.keyToString(index0.lastEntry().getKey()) + ", shard0=" + dbServers[0].getShard() +
                      ", found=" + found.toString() +
                      //", lastRecord0=" + debuglast0 +
                      ", last1=" + (index1.lastEntry() == null ? "" : DatabaseCommon.keyToString(index1.lastEntry().getKey())) + ", shard1=" + dbServers[2].getShard() +
                      //", lastRecord1=" + debuglast1 +
                  ", first0=" + DatabaseCommon.keyToString(index0.firstEntry().getKey()) + ", shard0=" + dbServers[0].getShard() +
                      ", first1=" + (index1.firstEntry() == null ? "" : DatabaseCommon.keyToString(index1.firstEntry().getKey())) + ", shard1=" + dbServers[2].getShard() +
                    ", size0=" + index0.size() + ", size1=" + index1.size()
                      //", nextRecord0: " + debug0 + ", nextRecord1=" + debug1 +
                      //", nextRecord0_1: " + debug0_1 + ", nextRecord1_1=" + debug1_1 +
                      //", foundRecord0: " + debugId0 + ", foundRecord1=" + debugId1);
                  );

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

  private void appendUpperKey(int i, TableSchema.Partition[] partitions, StringBuilder curr) {
    curr.append(partitions[i].getUpperKey() == null ? "null" : DatabaseCommon.keyToString(partitions[i].getUpperKey())).append(",");
  }

  private String getRecordDebug(long i, Index index1, DatabaseServer dbServer) {
    String debug = "null";
    Object[] key = new Object[]{(long)i};
    synchronized (index1.getMutex(key)) {
      Object value = index1.get(key);
      if (value != null && !value.equals(0L)) {
        debug = "found";
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

          debug += "nullRecord";
        }
        else {
          if (bytes.length > 1) {
            throw new DatabaseException("More than one record");
          }
          long flags = Record.getDbViewFlags(bytes[0]);
          if ((flags & Record.DB_VIEW_FLAG_DELETING) != 0) {
            debug += ":deleting";
          }
          if ((flags & Record.DB_VIEW_FLAG_ADDING) != 0) {
            debug += ":adding";
          }
          debug += ":" + Record.getDbViewNumber(bytes[0]);
        }
      }
    }
    return debug;
  }

}
