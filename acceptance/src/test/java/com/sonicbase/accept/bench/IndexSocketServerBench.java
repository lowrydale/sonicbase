/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.accept.bench;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.index.Index;
import com.sonicbase.index.NativePartitionedTreeImpl;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.schema.DataType;
import com.sonicbase.schema.FieldSchema;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.server.DatabaseServer;
import com.sonicbase.server.NettyServer;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import sun.misc.Unsafe;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class IndexSocketServerBench {

  private static NativePartitionedTreeImpl nativeIndex;
  private static Index index;


  private Connection conn;
  //  private final int recordCount = 10;
//  private DatabaseServer[] dbServers;
  private DatabaseClient client;
//
//  @AfterClass(alwaysRun = true)
//  public void afterClass() throws SQLException {
//    conn.close();
//    for (DatabaseServer server : dbServers) {
//      server.shutdown();
//    }
//    System.out.println("client refCount=" + DatabaseClient.clientRefCount.get() + ", sharedClients=" + DatabaseClient.sharedClients.size() + ", class=TestBatch");
//    for (DatabaseClient client : DatabaseClient.allClients) {
//      System.out.println("Stack:\n" + client.getAllocatedStack());
//    }
//  }

  private Connection connA;
  private Connection connB;
  private DatabaseClient clientA;
  private DatabaseClient clientB;
  private DatabaseServer[] dbServers;
  NettyServer serverA1;
  NettyServer serverA2;
  NettyServer serverB1;
  NettyServer serverB2;
  private Set<Long> ids;

  @AfterClass(alwaysRun = true)
  public void afterClass() throws SQLException {
//    connA.close();
//    connB.close();

    for (DatabaseServer server : dbServers) {
      server.shutdown();
    }
    serverA1.shutdown();
    serverA2.shutdown();
    serverB1.shutdown();
    serverB2.shutdown();

    System.out.println("client refCount=" + DatabaseClient.clientRefCount.get() + ", sharedClients=" + DatabaseClient.sharedClients.size());
    for (DatabaseClient client : DatabaseClient.allClients) {
      System.out.println("Stack:\n" + client.getAllocatedStack());
    }

  }

  @BeforeClass
  public void beforeClass() throws Exception {
    System.setProperty("log4j.configuration", "test-log4j.xml");

    FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db-data"));

    DatabaseClient.getServers().clear();

    dbServers = new DatabaseServer[4];

    String role = "primaryMaster";


    final CountDownLatch latch = new CountDownLatch(4);
    serverA1 = new NettyServer(128);
    Thread thread = new Thread(new Runnable(){
      @Override
      public void run() {
        serverA1.startServer(new String[]{"-port", String.valueOf(9010), "-host", "localhost",
            "-mport", String.valueOf(9010), "-mhost", "localhost", "-cluster", "4-shards", "-shard",
            String.valueOf(0)});
        latch.countDown();
      }
    });
    thread.start();
    while (true) {
      if (serverA1.isRunning()) {
        break;
      }
      Thread.sleep(100);
    }

    serverA2 = new NettyServer(128);
    thread = new Thread(new Runnable(){
      @Override
      public void run() {
        serverA2.startServer(new String[]{"-port", String.valueOf(9060), "-host", "localhost",
            "-mport", String.valueOf(9060), "-mhost", "localhost", "-cluster", "4-shards", "-shard", String.valueOf(1)});
        latch.countDown();
      }
    });
    thread.start();

    while (true) {
      if (serverA2.isRunning()) {
        break;
      }
      Thread.sleep(100);
    }

    serverB1 = new NettyServer(128);
    thread = new Thread(new Runnable(){
      @Override
      public void run() {
        serverB1.startServer(new String[]{"-port", String.valueOf(9110), "-host", "localhost",
            "-mport", String.valueOf(9110), "-mhost", "localhost", "-cluster", "4-shards", "-shard", String.valueOf(0)});
        latch.countDown();
      }
    });
    thread.start();
    while (true) {
      if (serverB1.isRunning()) {
        break;
      }
      Thread.sleep(100);
    }

    serverB2 = new NettyServer(128);
    thread = new Thread(new Runnable(){
      @Override
      public void run() {
        serverB2.startServer(new String[]{"-port", String.valueOf(9160), "-host", "localhost",
            "-mport", String.valueOf(9160), "-mhost", "localhost", "-cluster", "4-shards", "-shard", String.valueOf(1)});
        latch.countDown();
      }
    });
    thread.start();
    while (true) {
      if (serverB2.isRunning()) {
        break;
      }
      Thread.sleep(100);
    }

    while (true) {
      if (serverA1.isRecovered() && serverA2.isRecovered() && serverB1.isRecovered() && serverB2.isRecovered()) {
        break;
      }
      Thread.sleep(100);
    }

    dbServers[0] = serverA1.getDatabaseServer();
    dbServers[1] = serverA2.getDatabaseServer();
    dbServers[2] = serverB1.getDatabaseServer();
    dbServers[3] = serverB2.getDatabaseServer();
//
//    System.setProperty("log4j.configuration", "test-log4j.xml");
//
//    String configStr = IOUtils.toString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-4-shards.yaml")), "utf-8");
//    Config config = new Config(configStr);
//
//    FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db-data"));
//
//    //DatabaseServer.getAddressMap().clear();
//    DatabaseClient.getServers().clear();
//
//    dbServers = new DatabaseServer[4];
//    ThreadPoolExecutor executor = new ThreadPoolExecutor(32, 32, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
//
//    String role = "primaryMaster";
//
//    List<Future> futures = new ArrayList<>();
//    for (int i = 0; i < dbServers.length; i++) {
//      dbServers[i] = new DatabaseServer();
//      dbServers[i].setConfig(config, "4-shards", "localhost", 9010 + (50 * i), true, new AtomicBoolean(true), new AtomicBoolean(true),null, false);
//      dbServers[i].setRole(role);
//    }
//    for (Future future : futures) {
//      future.get();
//    }

    for (DatabaseServer server : dbServers) {
      server.shutdownRepartitioner();
    }

    Class.forName("com.sonicbase.jdbcdriver.Driver");

    conn = DriverManager.getConnection("jdbc:sonicbase:localhost:9010", "user", "password");

    client = ((ConnectionProxy)conn).getDatabaseClient();
    client.createDatabase("test");

    conn.close();

    conn = DriverManager.getConnection("jdbc:sonicbase:localhost:9010/test", "user", "password");

    client = ((ConnectionProxy)conn).getDatabaseClient();


    PreparedStatement stmt = conn.prepareStatement("create table Persons (id1 BIGINT, id2 BIGINT, PRIMARY KEY (id1))");
    stmt.executeUpdate();

    ids = new HashSet<>();

    client.beginRebalance("test");

    while (true) {
      if (client.isRepartitioningComplete("test")) {
        break;
      }
      Thread.sleep(1_000);
    }

    for (DatabaseServer server : dbServers) {
      server.shutdownRepartitioner();
    }

//    for (DatabaseServer server : dbServers) {
//      server.getDeleteManager().forceDeletes(null, false);
//    }

    Thread.sleep(5_000);

    //executor.shutdownNow();
  }

  public static TableSchema createTable() {
    TableSchema tableSchema = new TableSchema();
    tableSchema.setName("table1");
    tableSchema.setTableId(100);
    List<FieldSchema> fields = new ArrayList<>();
    FieldSchema fSchema = new FieldSchema();
    fSchema.setName("_id");
    fSchema.setType(DataType.Type.BIGINT);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field1");
    fSchema.setType(DataType.Type.BIGINT);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field2");
    fSchema.setType(DataType.Type.VARCHAR);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field3");
    fSchema.setType(DataType.Type.TIMESTAMP);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field4");
    fSchema.setType(DataType.Type.INTEGER);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field5");
    fSchema.setType(DataType.Type.SMALLINT);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field6");
    fSchema.setType(DataType.Type.TINYINT);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field7");
    fSchema.setType(DataType.Type.CHAR);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field8");
    fSchema.setType(DataType.Type.NCHAR);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field9");
    fSchema.setType(DataType.Type.FLOAT);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field10");
    fSchema.setType(DataType.Type.REAL);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field11");
    fSchema.setType(DataType.Type.DOUBLE);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field12");
    fSchema.setType(DataType.Type.BOOLEAN);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field13");
    fSchema.setType(DataType.Type.BIT);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field14");
    fSchema.setType(DataType.Type.VARCHAR);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field15");
    fSchema.setType(DataType.Type.CLOB);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field16");
    fSchema.setType(DataType.Type.NCLOB);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field17");
    fSchema.setType(DataType.Type.LONGVARCHAR);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field18");
    fSchema.setType(DataType.Type.NVARCHAR);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field19");
    fSchema.setType(DataType.Type.LONGNVARCHAR);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field20");
    fSchema.setType(DataType.Type.LONGVARBINARY);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field21");
    fSchema.setType(DataType.Type.VARBINARY);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field22");
    fSchema.setType(DataType.Type.BLOB);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field23");
    fSchema.setType(DataType.Type.NUMERIC);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field24");
    fSchema.setType(DataType.Type.DECIMAL);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field25");
    fSchema.setType(DataType.Type.DATE);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field26");
    fSchema.setType(DataType.Type.TIME);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field27");
    fSchema.setType(DataType.Type.TIMESTAMP);
    fields.add(fSchema);
    tableSchema.setFields(fields);
    List<String> primaryKey = new ArrayList<>();
    primaryKey.add("field1");
    tableSchema.setPrimaryKey(primaryKey);
    return tableSchema;
  }

  public static IndexSchema createIndexSchema(TableSchema tableSchema) {
    return createIndexSchema(tableSchema, 1);
  }

  public static IndexSchema createIndexSchema(TableSchema tableSchema, int partitionCount) {
    IndexSchema indexSchema = new IndexSchema();
    indexSchema.setFields(new String[]{"field1"}, tableSchema);
    indexSchema.setIndexId(1);
    indexSchema.setIsPrimaryKey(true);
    indexSchema.setName("_primarykey");
    indexSchema.setComparators(tableSchema.getComparators(new String[]{"field1"}));

    TableSchema.Partition[] partitions = new TableSchema.Partition[partitionCount];
    for (int i = 0; i < partitionCount; i++) {
      partitions[i] = new TableSchema.Partition();
      partitions[i].setUnboundUpper(true);
    }
    indexSchema.setCurrPartitions(partitions);
    tableSchema.addIndex(indexSchema);
    return indexSchema;
  }

  public static void main(String[] args) throws Exception {
    new IndexSocketServerBench().bench();
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
        for (int i1 = 0; i1 < 10_000_000; i1++) {
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
                PreparedStatement stmt = conn.prepareStatement("select * from persons where id1 >= 0");
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

        PreparedStatement stmt = conn.prepareStatement("insert into persons (id1, id2) values(?, ?)");
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
