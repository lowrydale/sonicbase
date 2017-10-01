package com.sonicbase.database;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.index.Index;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.schema.*;
import com.sonicbase.server.DatabaseServer;
import com.sonicbase.util.JsonArray;
import com.sonicbase.util.JsonDict;
import com.sonicbase.util.StreamUtils;
import org.codehaus.plexus.util.FileUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;

/**
 * Responsible for
 */
public class TestIndex {

  DatabaseClient client;

  @BeforeClass
  public void before() throws ClassNotFoundException, SQLException, IOException, ExecutionException, InterruptedException {
    String configStr = StreamUtils.inputStreamToString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-4-servers.json")));
        final JsonDict config = new JsonDict(configStr);

        JsonArray array = config.putArray("licenseKeys");
        array.add(DatabaseServer.FOUR_SERVER_LICENSE);

        FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db"));

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

        for (DatabaseServer server : dbServers) {
          server.shutdownRepartitioner();
        }
    Class.forName("com.sonicbase.jdbcdriver.Driver");

     Connection conn = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9000", "user", "password");

     ((ConnectionProxy) conn).getDatabaseClient().createDatabase("test");

     conn.close();

     conn = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9000/test", "user", "password");

     client = ((ConnectionProxy) conn).getDatabaseClient();

     client.setPageSize(3);


     PreparedStatement stmt = conn.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
     stmt.executeUpdate();

     stmt = conn.prepareStatement("create table Memberships (personId BIGINT, personId2 BIGINT, membershipName VARCHAR(20), resortId BIGINT, PRIMARY KEY (personId, membershipName))");
     stmt.executeUpdate();

     stmt = conn.prepareStatement("create table Resorts (resortId BIGINT, resortName VARCHAR(20), PRIMARY KEY (resortId))");
     stmt.executeUpdate();

     stmt = conn.prepareStatement("create table nokey (id BIGINT, id2 BIGINT)");
     stmt.executeUpdate();

  }

  @Test
  public void testStringOrder() {
    List<String> list = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      list.add(String.valueOf(i));
    }
    for (int i = 100; i < 110; i++) {
      list.add(String.valueOf(i));
    }

    list.sort(new Comparator<String>(){
      @Override
      public int compare(String o1, String o2) {
        return o1.compareTo(o2);
      }
    });
    for (String str : list) {
      System.out.println(str);
    }
  }
  @Test
  public void test() {

    Schema schema = client.getSchema("test");
    TableSchema tableSchema = schema.getTables().get("memberships");
    IndexSchema indexSchema = tableSchema.getIndexes().get("_2__primarykey");
    Index index = new Index(tableSchema, indexSchema.getName(), indexSchema.getComparators());

    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        index.put(new Object[]{i, String.valueOf(j).getBytes()}, i + 100);
      }
    }
    Map.Entry<Object[], Object>[] ret = new Map.Entry[3];
    index.higherEntries(new Object[]{2, "2".getBytes()}, ret);
    assertEquals(ret[0].getKey()[0], 2);
    assertEquals(new String((byte[])ret[0].getKey()[1]), "3");
    assertEquals(ret[1].getKey()[0], 2);
    assertEquals(new String((byte[])ret[1].getKey()[1]), "4");
    assertEquals(ret[2].getKey()[0], 2);
    assertEquals(new String((byte[])ret[2].getKey()[1]), "5");

    Map.Entry<Object[], Object> curr = index.higherEntry(new Object[]{3});
//    assertEquals(curr.getKey()[0], 3);
//    assertEquals(new String((byte[])curr.getKey()[1]), "0");

    curr = index.higherEntry(new Object[]{3, "9".getBytes()});
    assertEquals(curr.getKey()[0], 4);

    curr = index.floorEntry(new Object[]{3, "3".getBytes()});
    assertEquals(curr.getKey()[0], 3);

    curr = index.lastEntry();
    assertEquals(curr.getKey()[0], 9);

    curr = index.firstEntry();
    assertEquals(curr.getKey()[0], 0);

    index.lowerEntries(new Object[]{5, "0".getBytes()}, ret);
    assertEquals(ret[0].getKey()[0], 4);
    assertEquals(ret[1].getKey()[0], 4);
    assertEquals(ret[2].getKey()[0], 4);

    curr = index.lowerEntry(new Object[]{3, "0".getBytes()});
    assertEquals(curr.getKey()[0], 2);

    curr = index.ceilingEntry(new Object[]{3, "3".getBytes()});
    assertEquals(curr.getKey()[0], 3);
  }

  @Test
  public void testLong() {

    Schema schema = client.getSchema("test");
    TableSchema tableSchema = schema.getTables().get("persons");
    IndexSchema indexSchema = tableSchema.getIndexes().get("_1__primarykey");
    Index index = new Index(tableSchema, indexSchema.getName(), indexSchema.getComparators());

    for (int i = 0; i < 10; i++) {
      index.put(new Object[]{(long)i}, i + 100);
    }
    Map.Entry<Object[], Object>[] ret = new Map.Entry[3];
    index.higherEntries(new Object[]{(long)2}, ret);
    assertEquals(ret[0].getKey()[0], (long)3);
    assertEquals(ret[1].getKey()[0], (long)4);
    assertEquals(ret[2].getKey()[0], (long)5);

    Map.Entry<Object[], Object> curr = index.higherEntry(new Object[]{(long)3});
    assertEquals(curr.getKey()[0], (long)4);

    curr = index.floorEntry(new Object[]{(long)3});
    assertEquals(curr.getKey()[0], (long)3);

    curr = index.lastEntry();
    assertEquals(curr.getKey()[0], (long)9);

    curr = index.firstEntry();
    assertEquals(curr.getKey()[0], (long)0);

    index.lowerEntries(new Object[]{(long)5}, ret);
    assertEquals(ret[0].getKey()[0], (long)4);
    assertEquals(ret[1].getKey()[0], (long)3);
    assertEquals(ret[2].getKey()[0], (long)2);

    curr = index.lowerEntry(new Object[]{(long)3});
    assertEquals(curr.getKey()[0], (long)2);

    curr = index.ceilingEntry(new Object[]{(long)3});
    assertEquals(curr.getKey()[0], (long)3);
  }

  @Test
  public void testVisitTailMap() {

    Schema schema = client.getSchema("test");
    TableSchema tableSchema = schema.getTables().get("persons");
    IndexSchema indexSchema = tableSchema.getIndexes().get("_1__primarykey");
    Index index = new Index(tableSchema, indexSchema.getName(), indexSchema.getComparators());

    for (int i = 0; i < 10021; i++) {
      index.put(new Object[]{(long) i}, (long) i + 1000);
    }

    final AtomicInteger offset = new AtomicInteger(201);
    index.visitTailMap(index.floorEntry(new Object[]{(long)201}).getKey(), new Index.Visitor(){
      @Override
      public boolean visit(Object[] key, Object value) throws IOException {
        assertEquals((long)key[0], offset.getAndIncrement());
        return true;
      }
    });
    assertEquals(offset.get(), 10021);
  }

  @Test
  public void testVisitHeadMap() {

    Schema schema = client.getSchema("test");
    TableSchema tableSchema = schema.getTables().get("persons");
    IndexSchema indexSchema = tableSchema.getIndexes().get("_1__primarykey");
    Index index = new Index(tableSchema, indexSchema.getName(), indexSchema.getComparators());

    for (int i = 0; i < 10000; i++) {
      index.put(new Object[]{(long) i}, (long) i + 1000);
    }

    final AtomicInteger offset = new AtomicInteger(10000 - 2);
    index.visitHeadMap(index.lastEntry().getKey(), new Index.Visitor(){
      @Override
      public boolean visit(Object[] key, Object value) throws IOException {
        assertEquals((long)key[0], offset.getAndDecrement());
        return true;
      }
    });
    assertEquals(offset.get(), -1);
  }

  @Test
  public void testSimple() {

    Schema schema = client.getSchema("test");
    TableSchema tableSchema = schema.getTables().get("persons");
    IndexSchema indexSchema = tableSchema.getIndexes().get("_1__primarykey");
    Index index = new Index(tableSchema, indexSchema.getName(), indexSchema.getComparators());

    for (int i = 0; i < 1000; i++) {
      index.put(new Object[]{(long) i}, (long)i + 1000);
    }

    assertEquals((long)index.firstEntry().getKey()[0], 0);
    assertEquals((long)index.higherEntry(new Object[]{0L}).getKey()[0], 1);
    assertEquals((long)index.lastEntry().getKey()[0], 999);
    assertEquals((long)index.lowerEntry(new Object[]{999L}).getKey()[0], 998);
    assertEquals((Long)index.get(new Object[]{0L}), (Long)(long)1000);
    assertEquals(index.size(), 1000);
    assertEquals((long)index.floorEntry(new Object[]{500L}).getKey()[0], 500);
    assertEquals((long)index.ceilingEntry(new Object[]{500L}).getKey()[0], 500);

    final AtomicInteger countVisited = new AtomicInteger();
    index.visitTailMap(new Object[]{500L}, new Index.Visitor(){
      @Override
      public boolean visit(Object[] key, Object value) throws IOException {
        countVisited.incrementAndGet();
        return true;
      }
    });
    //assertEquals(countVisited.get(), 500);

    countVisited.set(0);
    index.visitHeadMap(new Object[]{100L}, new Index.Visitor(){
      @Override
      public boolean visit(Object[] key, Object value) throws IOException {
        countVisited.incrementAndGet();
        return true;
      }
    });
//    assertEquals(countVisited.get(), 100);

    for (int i = 0; i < 500; i++) {
      Object value = index.remove(new Object[]{(long)i});
      assertEquals(value, (Long)(long)(1000 + i));
    }
    assertEquals(index.size(), 500);
    assertEquals((long)index.firstEntry().getKey()[0], 500);
  }

  @Test
  public void testLocks() {
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
    ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

    writeLock.lock();
    writeLock.lock();
    System.out.println("got there!");
  }
  @Test
  public void testRebalance() throws InterruptedException {

    Schema schema = client.getSchema("test");
    TableSchema tableSchema = schema.getTables().get("persons");
    IndexSchema indexSchema = tableSchema.getIndexes().get("_1__primarykey");
    Index index = new Index(tableSchema, indexSchema.getName(), indexSchema.getComparators());

    for (int i = 0; i < 100000; i++) {
      index.put(new Object[]{(long) i}, (long)i);
    }

    Thread.sleep(10000);

    System.out.println("key at offset: " + index.getKeyAtOffset(asList(new Long[]{50000L}), null, null).get(0)[0]);

    assertEquals((long)index.firstEntry().getKey()[0], 0);
    assertEquals((long)index.higherEntry(new Object[]{0L}).getKey()[0], 1);
    assertEquals((long)index.lastEntry().getKey()[0], 99999);
    assertEquals((long)index.lowerEntry(new Object[]{99999L}).getKey()[0], 99998);
    assertEquals((Long)index.get(new Object[]{0L}), (Long)(long)0);
    assertEquals(index.size(), 100000);
    assertEquals((long)index.floorEntry(new Object[]{500L}).getKey()[0], 500);
    assertEquals((long)index.ceilingEntry(new Object[]{500L}).getKey()[0], 500);

    final AtomicInteger countVisited = new AtomicInteger();
    index.visitTailMap(new Object[]{500L}, new Index.Visitor(){
      @Override
      public boolean visit(Object[] key, Object value) throws IOException {
        countVisited.incrementAndGet();
        return true;
      }
    });
    //assertEquals(countVisited.get(), 500);

    countVisited.set(0);
    index.visitHeadMap(new Object[]{100L}, new Index.Visitor(){
      @Override
      public boolean visit(Object[] key, Object value) throws IOException {
        countVisited.incrementAndGet();
        return true;
      }
    });
//    assertEquals(countVisited.get(), 100);

    for (int i = 0; i < 500; i++) {
      Object value = index.remove(new Object[]{(long)i});
      assertEquals(value, (Long)(long)(i));
    }
    assertEquals(index.size(), 99500);
    assertEquals((long)index.firstEntry().getKey()[0], 500);
  }


  final Comparator[] comparators = new Comparator[]{DataType.getLongComparator()};


  Comparator<Object[]> comparator = new Comparator<Object[]>() {
    @Override
    public int compare(Object[] o1, Object[] o2) {
      for (int i = 0; i < Math.min(o1.length, o2.length); i++) {
        if (o1[i] == null || o2[i] == null) {
          continue;
        }
        int value = comparators[i].compare(o1[i], o2[i]);
        if (value < 0) {
          return -1;
        }
        if (value > 0) {
          return 1;
        }
      }
      return 0;
    }
  };

  @Test(enabled=false)
  public void getKeyAtOffset() {

    final ConcurrentSkipListMap<Object[], Object> map = new ConcurrentSkipListMap<>(comparator);
    for (int i = 0; i < 15000000; i++) {
      map.put(new Object[]{(long)i}, new Object());
    }

    Thread thread = new Thread(new Runnable(){
      @Override
      public void run() {
        int offset = 15000000;
        while (true) {
          map.put(new Object[]{(long)offset++}, new Object());
        }
      }
    });
    thread.start();
    long begin = System.currentTimeMillis();

    AtomicLong offset = new AtomicLong();
    for (Map.Entry<Object[], Object> entry : map.tailMap(map.firstKey()).entrySet()) {
      offset.incrementAndGet();
    }
    System.out.println("duration=" + (System.currentTimeMillis() - begin) + ", offset=" + offset.get());
  }

  final Comparator[] memComparators = new Comparator[]{DataType.getLongComparator(), DataType.getLongComparator()};


  Comparator<Object[]> memComparator = new Comparator<Object[]>() {
    @Override
    public int compare(Object[] o1, Object[] o2) {
      for (int i = 0; i < Math.min(o1.length, o2.length); i++) {
        if (o1[i] == null || o2[i] == null) {
          continue;
        }
        int value = memComparators[i].compare(o1[i], o2[i]);
        if (value < 0) {
          return -1;
        }
        if (value > 0) {
          return 1;
        }
      }
      return 0;
    }
  };
  @Test
  public void testCompound() {
    ConcurrentSkipListMap<Object[], Object> map = new ConcurrentSkipListMap<>(memComparator);
    for (int i = 0; i < 1000000; i++) {
      for (int j = 0; j < 2; j++) {
        map.put(new Object[]{(long)i, (long)j}, new Object());
      }
    }

    int offset = 0;
    for (Map.Entry<Object[], Object> entry : map.tailMap(map.firstKey()).entrySet()) {
      if (offset == 200000 || offset == 500000 || offset== 500001 || offset == 800000) {
        System.out.println(DatabaseCommon.keyToString(entry.getKey()));
      }
      offset++;
    }
  }

  @Test
  public void testMem() {
    char qualifier = "0.034t".toLowerCase().charAt(5);
    double value = Double.valueOf("0.034t".substring(0, 5).trim());
    if (qualifier == 't') {
      value = value * 1024d;
    }
    System.out.println(value);
  }
  @Test
  public void testCopy() {

    ConcurrentSkipListMap<Object[], Object> map = new ConcurrentSkipListMap<>(comparator);
    for (int i = 0; i < 30000000; i++) {
      map.put(new Object[]{(long)i}, new Object());
    }

    long begin = System.currentTimeMillis();
    List<Object[]> list = new ArrayList<>();
    for (Map.Entry<Object[], Object> entry : map.tailMap(map.firstKey()).entrySet()) {
      list.add(entry.getKey());
    }
    System.out.println("duration=" + (System.currentTimeMillis() - begin));

    begin = System.currentTimeMillis();
    for (Object[] key : list) {
      map.containsKey(key);
    }

    System.out.println("duration=" + (System.currentTimeMillis() - begin));
  }

  @Test
  public void testSort() {
    List<Long> list = new ArrayList<>();
    Random rand = new Random(System.currentTimeMillis());
    for (int i = 0; i < 15000000; i++) {
      list.add((long)rand.nextInt(30000000));
    }

    long begin = System.currentTimeMillis();
    Collections.sort(list);
    System.out.println("duration=" + (System.currentTimeMillis() - begin));
  }
}
