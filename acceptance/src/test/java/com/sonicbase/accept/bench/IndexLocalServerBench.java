/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.accept.bench;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.ComObject;
import com.sonicbase.common.Config;
import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.embedded.EmbeddedDatabase;
import com.sonicbase.index.Index;
import com.sonicbase.index.NativePartitionedTreeImpl;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.schema.DataType;
import com.sonicbase.schema.FieldSchema;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.server.DatabaseServer;
import com.sonicbase.server.NettyServer;
import com.sonicbase.server.ProServer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.yaml.snakeyaml.Yaml;
import sun.misc.Unsafe;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.sonicbase.client.InsertStatementHandler.BATCH_STATUS_FAILED;
import static com.sonicbase.client.InsertStatementHandler.BATCH_STATUS_SUCCCESS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class IndexLocalServerBench {

  private static NativePartitionedTreeImpl nativeIndex;
  private static Index index;


  private Connection clusterConn;

  DatabaseClient clusterClient = null;
  DatabaseServer[] dbServers;

  @AfterClass(alwaysRun = true)
  public void afterClass() throws SQLException {
    clusterConn.close();
    for (DatabaseServer server : dbServers) {
      server.shutdown();
    }

    System.out.println("client refCount=" + DatabaseClient.getClientRefCount().get() + ", sharedClients=" + DatabaseClient.getSharedClients().size() + ", class=TestDatabase");
    for (DatabaseClient client : DatabaseClient.getAllClients()) {
      System.out.println("Stack:\n" + client.getAllocatedStack());
    }
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    try {
      System.setProperty("log4j.configuration", "test-log4j.xml");
//      System.setProperty("log4j.configuration", "/Users/lowryda/Dropbox/git/sonicbase/db/src/main/resources/log4j.xml");

      String configStr = IOUtils.toString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-4-servers.yaml")), "utf-8");
      Config config = new Config(configStr);

      FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db-data"));

      DatabaseClient.getServers().clear();

      dbServers = new DatabaseServer[4];
      ThreadPoolExecutor executor = new ThreadPoolExecutor(32, 32, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

      String role = "primaryMaster";

      List<Future> futures = new ArrayList<>();
      for (int i = 0; i < dbServers.length; i++) {
        //      futures.add(executor.submit(new Callable() {
        //        @Override
        //        public Object call(Connection conn, DatabaseClient client) throws Exception {
        //          String role = "primaryMaster";

        dbServers[i] = new DatabaseServer();
        dbServers[i].setConfig(config, "4-servers", "localhost", 9010 + (50 * i), true, new AtomicBoolean(true), new AtomicBoolean(true),null, false);
        dbServers[i].setRole(role);

//        if (shard == 0) {
//          Map<Integer, Object> map = new HashMap<>();
//          DatabaseClient.getServers2().put(0, map);
//          map.put(0, dbServers[shard]);
//        }
//        if (shard == 1) {
//          DatabaseClient.getServers2().get(0).put(1, dbServers[shard]);
//        }
//        if (shard == 2) {
//          Map<Integer, Object> map = new HashMap<>();
//          DatabaseClient.getServers2().put(1, map);
//          map.put(0, dbServers[shard]);
//        }
//        if (shard == 3) {
//          DatabaseClient.getServers2().get(1).put(1, dbServers[shard]);
//        }
        //          return null;
        //        }
        //      }));
      }
      for (Future future : futures) {
        future.get();
      }

      DatabaseServer.initDeathOverride(2, 2);
      DatabaseServer.getDeathOverride()[0][0] = false;
      DatabaseServer.getDeathOverride()[0][1] = false;
      DatabaseServer.getDeathOverride()[1][0] = false;
      DatabaseServer.getDeathOverride()[1][1] = false;

      dbServers[0].enableSnapshot(false);
      dbServers[1].enableSnapshot(false);
      dbServers[2].enableSnapshot(false);
      dbServers[3].enableSnapshot(false);


      Thread.sleep(5000);

      //DatabaseClient client = new DatabaseClient("localhost", 9010, true);

      Class.forName("com.sonicbase.jdbcdriver.Driver");

      clusterConn = DriverManager.getConnection("jdbc:sonicbase:localhost:9010", "user", "password");
      clusterClient = ((ConnectionProxy) clusterConn).getDatabaseClient();

      ((ConnectionProxy) clusterConn).getDatabaseClient().createDatabase("test");

      clusterConn.close();

      clusterConn = DriverManager.getConnection("jdbc:sonicbase:localhost:9010/test", "user", "password");

      clusterClient = ((ConnectionProxy) clusterConn).getDatabaseClient();
      clusterClient.setPageSize(3);

      initDatabase(clusterConn);


      //Thread.sleep(60000);

      dbServers[0].runSnapshot();
      dbServers[1].runSnapshot();
      dbServers[2].runSnapshot();
      dbServers[3].runSnapshot();

      ObjectMapper mapper = new ObjectMapper();
      Map<String, Object> backupConfig = new Yaml().loadAs(
          "type: fileSystem\n" +
              "directory: $HOME/db/backup\n" +
              "period: daily\n" +
              "time: 23:00\n" +
              "maxBackupCount: 10\n" +
              "sharedDirectory: true\n", Map.class);

//          "    \"type\" : \"fileSystem\",\n" +
//          "    \"directory\": \"$HOME/db/backup\",\n" +
//          "    \"period\": \"daily\",\n" +
//          "    \"time\": \"23:00\",\n" +
//          "    \"maxBackupCount\": 10,\n" +
//          "    \"sharedDirectory\": true\n" +
//          "  }");

      for (DatabaseServer dbServer : dbServers) {
        ((ProServer)dbServer.getProServer()).getBackupManager().setBackupConfig(backupConfig);
      }

      clusterClient.syncSchema();

      for (Map.Entry<String, TableSchema> entry : clusterClient.getCommon().getTables("test").entrySet()) {
        for (Map.Entry<String, IndexSchema> indexEntry : entry.getValue().getIndices().entrySet()) {
          Object[] upperKey = indexEntry.getValue().getCurrPartitions()[0].getUpperKey();
          System.out.println("table=" + entry.getKey() + ", index=" + indexEntry.getKey() + ", upper=" + (upperKey == null ? null : DatabaseCommon.keyToString(upperKey)));
        }
      }

      File dir = new File(System.getProperty("user.home"), "/db/backup");
      FileUtils.deleteDirectory(dir);
      dir.mkdirs();

      clusterClient.startBackup();
      while (true) {
        Thread.sleep(1000);
        if (clusterClient.isBackupComplete()) {
          break;
        }
      }

      Thread.sleep(5000);

      File file = new File(System.getProperty("user.home"), "/db/backup");
      File[] dirs = file.listFiles();

      clusterClient.startRestore(dirs[0].getName());
      while (true) {
        Thread.sleep(1000);
        if (clusterClient.isRestoreComplete()) {
          break;
        }
      }
      dbServers[0].enableSnapshot(false);
      dbServers[1].enableSnapshot(false);
      dbServers[2].enableSnapshot(false);
      dbServers[3].enableSnapshot(false);

      clusterClient.syncSchema();
      for (Map.Entry<String, TableSchema> entry : clusterClient.getCommon().getTables("test").entrySet()) {
        for (Map.Entry<String, IndexSchema> indexEntry : entry.getValue().getIndices().entrySet()) {
          Object[] upperKey = indexEntry.getValue().getCurrPartitions()[0].getUpperKey();
          System.out.println("table=" + entry.getKey() + ", index=" + indexEntry.getKey() + ", upper=" + (upperKey == null ? null : DatabaseCommon.keyToString(upperKey)));
        }
      }

      for (DatabaseServer server : dbServers) {
        server.shutdownRepartitioner();
      }

      ComObject cobj = new ComObject(3);
      cobj.put(ComObject.Tag.DB_NAME, "test");
      cobj.put(ComObject.Tag.SCHEMA_VERSION, clusterClient.getCommon().getSchemaVersion());
      cobj.put(ComObject.Tag.METHOD, "DeleteManager:forceDeletes");
      clusterClient.sendToAllShards(null, 0, cobj, DatabaseClient.Replica.ALL);

      // Thread.sleep(10000);
      executor.shutdownNow();
    }
    catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  private void initDatabase(Connection conn) throws SQLException, InterruptedException {
    PreparedStatement stmt = conn.prepareStatement("create table Persons (id1 BIGINT, id2 BIGINT, id3 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id1))");
    stmt.executeUpdate();
  }

  public static void main(String[] args) throws Exception {
    new IndexLocalServerBench().bench();
  }

  private static int BLOCK_SIZE = DatabaseClient.SELECT_PAGE_SIZE;

  private final Unsafe unsafe = getUnsafe();
  public static Unsafe getUnsafe() {
    try {
      Field singleoneInstanceField = Unsafe.class.getDeclaredField("theUnsafe");
      singleoneInstanceField.setAccessible(true);
      return (Unsafe) singleoneInstanceField.get(null);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private final Object[] mutexes = new Object[100_000];

  private void bench() throws Exception {
    beforeClass();

    boolean mixg = false;
    boolean serial = true;
    getUnsafe();
    for (int i = 0; i < mutexes.length; i++) {
      mutexes[i] = new Object();
    }

    ThreadPoolExecutor executor = new ThreadPoolExecutor(32, 32, 10_000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
    ThreadPoolExecutor executor2 = new ThreadPoolExecutor(128, 128, 10_000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
    ThreadPoolExecutor executor3 = new ThreadPoolExecutor(24, 24, 10_000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
    final AtomicInteger countInserted = new AtomicInteger();
    final AtomicLong countRead = new AtomicLong();
    final List<Future> futures = new ArrayList<>();
    final AtomicLong last = new AtomicLong(System.currentTimeMillis());
    final long begin = System.currentTimeMillis();

    final AtomicLong localCountInserted = new AtomicLong();
    List<Future> futures2 = new ArrayList<>();
    long beginInsert = System.currentTimeMillis();
    for (int i = 0; i < 4; i++) {
      final int offset = i;
      futures2.add(executor3.submit((Callable) () -> {
        for (int i1 = 0; i1 < 1_000_000; i1++) {
          doInsert(countInserted, begin, localCountInserted, offset, i1);
        }
        return null;
      }));
    }
    if (serial) {
      for (Future future : futures2) {
        future.get();
      }
    }
    System.out.println("insert rate=" + (100_000_000d / System.currentTimeMillis() / (System.currentTimeMillis() - beginInsert) * 1000f));

    futures2.clear();
    localCountInserted.set(0);
//    for (int i = 0; i < 4; i++) {
//      final int offset = i;
//      futures2.add(executor3.submit((Callable) () -> {
//        for (int i1 = 0; i1 < 100_000_000; i1++) {
//          doDelete(countInserted, begin, localCountInserted, offset, i1);
//        }
//        return null;
//      }));
//    }
//    if (serial) {
//      for (Future future : futures2) {
//        future.get();
//      }
//    }
    System.out.println("delete rate=" + (100_000_000d / System.currentTimeMillis() / (System.currentTimeMillis() - beginInsert) * 1000f));

    for (int i = 0; i < 32; i++) {
      final int currIndex = i;
      futures.add(executor.submit((Callable) () -> {

        if (true  ) {
          futures2.add(executor2.submit((Callable) () -> {
            try {
              for (int j = 0; j < (mixg ? 1000 : 100_000); j++) {
                System.out.println("starting over");
                PreparedStatement stmt = clusterConn.prepareStatement("select * from persons where id1 >= 0");
                ResultSet rs = stmt.executeQuery();
                while (rs.next()) {
                  countRead.addAndGet(1);
                  if (System.currentTimeMillis() - last.get() > 1_000) {
                    last.set(System.currentTimeMillis());
                    System.out.println("read progress: count=" + countRead.get() + ", rate=" + ((float) countRead.get() / (System.currentTimeMillis() - begin) * 1000f));
                    System.out.flush();
                  }
                }
              }
              System.out.println("read progress: count=" + countRead.get() + ", rate=" + ((float) countRead.get() / (System.currentTimeMillis() - begin) * 1000f));
              System.out.flush();
//          }
            }
            catch (Exception e) {
              e.printStackTrace();
            }
            return null;
          }));
          for (Future future : futures2) {
            future.get();
          }
        }
        return null;
      }));
    }

    for (Future future : futures) {
      future.get();
    }

    Thread.sleep(10000000);

    afterClass();
    System.out.println("read progress - finished: count=" + countRead.get() + ", rate=" + ((float) countRead.get() / (System.currentTimeMillis() - begin) * 1000f));
    System.out.flush();

    executor.shutdownNow();
    executor2.shutdownNow();
  }

  private void doInsert(AtomicInteger countInserted, long begin, AtomicLong localCountInserted, int offset, long i1) throws SQLException {
    if (i1 % 4 == offset) {
      byte[] keyBytes = new byte[8];
      writeLong(keyBytes, i1);

      if (true) {
        long address = unsafe.allocateMemory(75);
        for (int l = 0; l < 75; l++) {
          unsafe.putByte(address + l, (byte) 0);
        }

        PreparedStatement stmt = clusterConn.prepareStatement("insert into persons (id1, id2) values(?, ?)");
        stmt.setLong(1, (long)i1);
        stmt.setLong(2, (long)i1);
        stmt.executeUpdate();

        //                          if (i1 % 150 == 0) {
        //                Thread.sleep(5);
        //              }
        localCountInserted.incrementAndGet();
        if (countInserted.incrementAndGet() % 100_000 == 0) {
          System.out.println("insert progress: count=" + countInserted.get() + ", rate=" + ((float) countInserted.get() / (System.currentTimeMillis() - begin) * 1000f));
        }
      }
    }
  }


  public final long readLong(byte[] bytes, int offset) throws IOException {
    long result = 0;
    for (int i = 0; i < 8; i++) {
      result <<= 8;
      result |= (bytes[i + offset] & 0xFF);
    }
    return result;
  }

  public final void writeLong(byte[] bytes, long v) {
    bytes[0] = (byte)(v >>> 56);
    bytes[1] = (byte)(v >>> 48);
    bytes[2] = (byte)(v >>> 40);
    bytes[3] = (byte)(v >>> 32);
    bytes[4] = (byte)(v >>> 24);
    bytes[5] = (byte)(v >>> 16);
    bytes[6] = (byte)(v >>>  8);
    bytes[7] = (byte)(v >>>  0);
  }

  public final int readInt(byte[] bytes, int offset) throws IOException {
    return bytes[offset] << 24 |
        (bytes[1 + offset] & 0xFF) << 16 |
        (bytes[2 + offset] & 0xFF) << 8 |
        (bytes[3 + offset] & 0xFF);
//    int ch1 = bytes[offset.get()];
//    int ch2 = bytes[offset.get() + 1];
//    int ch3 = bytes[offset.get() + 1];
//    int ch4 = bytes[offset.get() + 1];
//    if ((ch1 | ch2 | ch3 | ch4) < 0)
//      throw new EOFException();
//    offset.addAndGet(4);
//    return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
  }

  public static int bytesToInt(byte[] bytes, int offset) {
    return bytes[offset] << 24 |
        (bytes[1 + offset] & 0xFF) << 16 |
        (bytes[2 + offset] & 0xFF) << 8 |
        (bytes[3 + offset] & 0xFF);
  }

  public static long bytesToLong(byte[] b, int offset) {
    long result = 0;
    for (int i = 0; i < 8; i++) {
      result <<= 8;
      result |= (b[i + offset] & 0xFF);
    }
    return result;
  }


}
