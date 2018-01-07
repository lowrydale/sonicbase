/* © 2017 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.bench;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.Logger;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.research.socket.NettyServer;
import com.sonicbase.server.DatabaseServer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class TestPerformance {


  private Connection conn;
  private DatabaseClient client;
  private List<Thread> serverThreads = new ArrayList<>();
  DatabaseServer[] dbServers;
  private float outerFactor = 1;
  private boolean validate;
  private boolean server;


  public static void main(String[] args) throws InterruptedException, SQLException, ClassNotFoundException, ExecutionException, IOException {
    TestPerformance test = new TestPerformance();
    test.run(args);

  }

  public void run(String[] args) throws InterruptedException, SQLException, ClassNotFoundException, ExecutionException, IOException {

    String outerFactorStr = args.length > 0 ? args[0] : null;
    outerFactor = 1;
    if (outerFactorStr != null) {
      outerFactor = Float.valueOf(outerFactorStr);
    }

    String validateStr = args.length > 1 ? args[1] : null;
    validate = false;
    if (validateStr != null) {
      validate = Boolean.valueOf(validateStr);
    }

    String serverStr = args.length > 2 ? args[2] : null;
    server = true;
    if (serverStr != null) {
      server = Boolean.valueOf(serverStr);
    }

    setup(outerFactor);

    List<String> methods = new ArrayList<>();


    methods.add("testInnerJoin");
    methods.add("testLeftOuterJoin");
    methods.add("testRightOuterJoin");
    methods.add("testIdLookup");
    methods.add("testMath");
    methods.add("testIdNoKey");
    methods.add("testRangeNoKey");
    methods.add("testRangeThreeKey");
    methods.add("testRangeThreeKeyBackwards");
    methods.add("testRangeThreeKeyMixed");
    methods.add("testRangeThreeKeySingle");
    methods.add("testNoKeyTwoKeyGreaterEqual");
    methods.add("testNoKeyTwoKeyGreater");
    methods.add("testNoKeyTwoKeyGreaterLeftSided");
    methods.add("notIn");
    methods.add("notInSecondary");
    methods.add("notInTableScan");
    methods.add("test2keyRange");
    methods.add("testSecondaryKey");
    methods.add("testTableScan");
    methods.add("testTwoKey");
    methods.add("testTwoKeyRightSided");
    methods.add("testTwoKeyLeftSidedGreater");
    methods.add("testTwoKeyGreater");
    methods.add("testTwoKeyGreaterBackwards");
    methods.add("testTwoKeyLeftSidedGreaterEqual");
    methods.add("testCountTwoKeyGreaterEqual");
    methods.add("testMaxWhere");
    //methods.add("testMax");
    methods.add("testCount");
    methods.add("testSort");
    methods.add("testSortDisk");
    methods.add("testId2");
    methods.add("testId2Range");
    methods.add("testOtherExpression");
    methods.add("testNoWhereClause");

    methods.add("testRangeGreaterDescend");
    methods.add("testRange");
    methods.add("testRangeLess");

    methods.add("testRangeOtherExpression");
    methods.add("testSecondary");
    methods.add("testUnion");
    methods.add("testUnionInMemory");
    methods.add("testUnionAll"); //
    methods.add("testIntersect");
    methods.add("testExcept");
    methods.add("testNot");
    methods.add("testFunctionAvg");
    methods.add("testFunctionMin");
    methods.add("testFunctionCustom");


    for (String method : methods) {
      try {
        Method methodObj = this.getClass().getMethod(method);
        methodObj.invoke(this);
      }
      catch (Exception e) {
        e.printStackTrace();
      }
    }


    summarize();
  }

  public void assertTrue(boolean value) {
    if (validate) {
      if (!value) {
        throw new DatabaseException("Error assertTrue=false");
      }
    }

  }
  public void assertTrue(boolean value, String msg) {
    if (validate) {
      if (!value) {
        throw new DatabaseException(msg);
      }
    }
  }
  public void assertFalse(boolean value) {
    if (validate) {
      if (value) {
        throw new DatabaseException("Error assertFalse=true");
      }
    }
  }
  public void assertEquals(Object lhs, Object rhs) {
    if (validate) {
      if (!lhs.equals(rhs)) {
        throw new DatabaseException("lhs != rhs: lhs=" + lhs + ", rhs=" + rhs);
      }
    }
  }
  public void assertEquals(Object lhs, Object rhs, String msg) {
    if (validate) {
      if (!lhs.equals(rhs)) {
        throw new DatabaseException("lhs != rhs: lhs=" + lhs + ", rhs=" + rhs + ", msg=" + msg);
      }
    }
  }
  public void summarize() {

    int maxLen = 0;
    for (String name : results.keySet()) {
      maxLen = Math.max(name.length(), maxLen);
    }
    for (Map.Entry<String, Result> entry : results.entrySet()) {
      String name = entry.getKey();
      String prefix = "";
      for (int i = 0; i < maxLen - name.length(); i++) {
        prefix += " ";
      }
      System.out.println(prefix + name + ": " + entry.getValue().duration / 1_000_000 + " " +
          entry.getValue().duration / entry.getValue().count / 1_000_000D + " " +
          (double)entry.getValue().count / (double)entry.getValue().duration * 1_000_000D * 1000D);
    }

//    for (DatabaseServer server : dbServers) {
//      server.shutdown();
//    }
//    for (Thread thread : serverThreads) {
//      thread.interrupt();
//    }
//    Logger.queue.clear();
  }

  public void setup(float outerFactor) throws IOException, InterruptedException, SQLException, ClassNotFoundException, ExecutionException {
    try {
      int outerCount = (int) (outerFactor * 5_000);
      Logger.disable();
      String configStr = IOUtils.toString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-2-servers-a.json")), "utf-8");
      ObjectMapper mapper = new ObjectMapper();
      final ObjectNode config = (ObjectNode) mapper.readTree(configStr);

      FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db"));

      ArrayNode array = new ArrayNode(JsonNodeFactory.instance);
      array.add(DatabaseServer.FOUR_SERVER_LICENSE);
      config.put("licenseKeys", array);

      DatabaseClient.getServers().clear();

      dbServers = new DatabaseServer[2];

      String role = "primaryMaster";

      Logger.setReady(false);

      if (server) {
        final CountDownLatch latch = new CountDownLatch(4);
        final NettyServer server0_0 = new NettyServer(128);
        Thread thread = new Thread(new Runnable() {
          @Override
          public void run() {
            server0_0.startServer(new String[]{"-port", String.valueOf(9010), "-host", "localhost",
                "-mport", String.valueOf(9010), "-mhost", "localhost", "-cluster", "2-servers-a", "-shard", String.valueOf(0)}, "db/src/main/resources/config/config-4-servers.json", true);
            latch.countDown();
          }
        });
        serverThreads.add(thread);
        thread.start();
        while (true) {
          Logger.setReady(false);
          if (server0_0.isRunning()) {
            break;
          }
          Thread.sleep(100);
        }

        final NettyServer server0_1 = new NettyServer(128);
        thread = new Thread(new Runnable() {
          @Override
          public void run() {
            server0_1.startServer(new String[]{"-port", String.valueOf(9060), "-host", "localhost",
                "-mport", String.valueOf(9060), "-mhost", "localhost", "-cluster", "2-servers-a", "-shard", String.valueOf(0)}, "db/src/main/resources/config/config-4-servers.json", true);
            latch.countDown();
          }
        });
        serverThreads.add(thread);
        thread.start();

        while (true) {
          Logger.setReady(false);
          if (server0_1.isRunning()) {
            break;
          }
          Thread.sleep(100);
        }

//        final NettyServer server1_0 = new NettyServer(128);
//        thread = new Thread(new Runnable() {
//          @Override
//          public void run() {
//            server1_0.startServer(new String[]{"-port", String.valueOf(9110), "-host", "localhost",
//                "-mport", String.valueOf(9110), "-mhost", "localhost", "-cluster", "4-servers", "-shard", String.valueOf(1)}, "db/src/main/resources/config/config-4-servers.json", true);
//            latch.countDown();
//          }
//        });
//        serverThreads.add(thread);
//        thread.start();
//        while (true) {
//          Logger.setReady(false);
//          if (server1_0.isRunning()) {
//            break;
//          }
//          Thread.sleep(100);
//        }
//
//        final NettyServer server1_1 = new NettyServer(128);
//        thread = new Thread(new Runnable() {
//          @Override
//          public void run() {
//            server1_1.startServer(new String[]{"-port", String.valueOf(9160), "-host", "localhost",
//                "-mport", String.valueOf(9160), "-mhost", "localhost", "-cluster", "4-servers", "-shard", String.valueOf(1)}, "db/src/main/resources/config/config-4-servers.json", true);
//            latch.countDown();
//          }
//        });
//        serverThreads.add(thread);
//        thread.start();
//        while (true) {
//          Logger.setReady(false);
//          if (server1_1.isRunning()) {
//            break;
//          }
//          Thread.sleep(100);
//        }

        while (true) {
          Logger.setReady(false);
          if (server0_0.isRunning() && server0_1.isRunning() /*&& server1_0.isRunning() && server1_1.isRunning()*/) {
            break;
          }
          Thread.sleep(100);
        }

        Thread.sleep(5_000);

        dbServers[0] = server0_0.getDatabaseServer();
        dbServers[1] = server0_1.getDatabaseServer();
//        dbServers[2] = server1_0.getDatabaseServer();
//        dbServers[3] = server1_1.getDatabaseServer();

        System.out.println("Started 4 servers");

        //
        //      //DatabaseClient client = new DatabaseClient("localhost", 9010, true);
        //
      }

      Class.forName("com.sonicbase.jdbcdriver.Driver");

      boolean sonicbase = true;
      if (!sonicbase) {
        Class.forName("com.mysql.jdbc.Driver");
        conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/db", "root", "pass");
      }
      else {
        conn = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9010", "user", "password");
        ((ConnectionProxy) conn).getDatabaseClient().createDatabase("test");
        conn.close();

        conn = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9010/test", "user", "password");
        client = ((ConnectionProxy) conn).getDatabaseClient();
        client.syncSchema();
      }


      Logger.setReady(false);

      if (server) {
        //
        PreparedStatement stmt = null;
        try {
          stmt = conn.prepareStatement("drop table persons");
          stmt.executeUpdate();
        }
        catch (Exception e) {

        }

        stmt = conn.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
        stmt.executeUpdate();

        try {
          stmt = conn.prepareStatement("drop table Persons2");
          stmt.executeUpdate();
        }
        catch (Exception e) {

        }

        stmt = conn.prepareStatement("create table Persons2 (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
        stmt.executeUpdate();

        try {
          stmt = conn.prepareStatement("drop index id2");
          stmt.executeUpdate();
        }
        catch (Exception e) {

        }

        stmt = conn.prepareStatement("create index id2 on persons(id2)");
        stmt.executeUpdate();

        try {
          stmt = conn.prepareStatement("drop table Employee");
          stmt.executeUpdate();
        }
        catch (Exception e) {

        }

        stmt = conn.prepareStatement("create table Employee (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20))");
        stmt.executeUpdate();

        try {
          stmt = conn.prepareStatement("drop index socialsecuritynumber");
          stmt.executeUpdate();
        }
        catch (Exception e) {

        }

        stmt = conn.prepareStatement("create index socialsecuritynumber on employee(socialsecurityNumber)");
        stmt.executeUpdate();

        try {
          stmt = conn.prepareStatement("drop table Residence");
          stmt.executeUpdate();
        }
        catch (Exception e) {

        }

        stmt = conn.prepareStatement("create table Residence (id BIGINT, id2 BIGINT, id3 BIGINT, address VARCHAR(20), PRIMARY KEY (id, id2, id3))");
        stmt.executeUpdate();


        //rebalance
        for (DatabaseServer server : dbServers) {
          server.shutdownRepartitioner();
        }

        List<Future> futures = new ArrayList<>();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(8, 8, 10_000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
        int offset = 0;
        for (int i = 0; i < outerCount; i++) {
          final int currOffset = offset;
          offset += 100;
          futures.add(executor.submit(new Callable() {
            @Override
            public Object call() throws Exception {
              insertPersons("Persons", currOffset);
              insertPersons("Persons2", currOffset);
              return null;
            }
          }));

        }

        for (Future future : futures) {
          future.get();
        }

        offset = 0;
        for (int i = 0; i < outerCount; i++) {
          final int currOffset = offset;
          offset += 100;
          futures.add(executor.submit(new Callable() {
            @Override
            public Object call() throws Exception {
              int offset = currOffset;
              PreparedStatement stmt = conn.prepareStatement("insert into residence (id, id2, id3, address) VALUES (?, ?, ?, ?)");
              for (int j = 0; j < 100; j++) {
                stmt.setLong(1, offset);
                stmt.setLong(2, offset + 1000);
                stmt.setLong(3, offset + 2000);
                stmt.setString(4, "5078 West Black");
                if (offset++ % 1000 == 0) {
                  System.out.println("progress: count=" + offset);
                }
                stmt.addBatch();
              }
              stmt.executeBatch();
              return null;
            }
          }));
        }

        for (Future future : futures) {
          future.get();
        }

        offset = 0;
        for (int i = 0; i < outerCount; i++) {
          final int currOffset = offset;
          offset += 100;
          futures.add(executor.submit(new Callable() {
            @Override
            public Object call() throws Exception {
              int offset = currOffset;
              PreparedStatement stmt = conn.prepareStatement("insert into employee (id, id2, socialSecurityNumber) VALUES (?, ?, ?)");
              for (int j = 0; j < 100; j++) {
                stmt.setLong(1, offset);
                String leading = padNumericString(offset);
                stmt.setLong(2, offset + 1000);
                stmt.setString(3, leading);
                if (offset++ % 1000 == 0) {
                  System.out.println("progress: count=" + offset);
                }
                stmt.addBatch();
              }
              stmt.executeBatch();
              return null;
            }
          }));
        }
        for (Future future : futures) {
          future.get();
        }


        //      long size = client.getPartitionSize("test", 0, "children", "_1_socialsecuritynumber");
        //      assertEquals(size, 10);

        if (client != null) {
          client.beginRebalance("test", "persons", "_1__primarykey");

          while (true) {
            if (client.isRepartitioningComplete("test")) {
              break;
            }
            Thread.sleep(200);
          }

          for (DatabaseServer server : dbServers) {
            server.shutdownRepartitioner();
          }

          client.beginRebalance("test", "persons", "_1__primarykey");

          while (true) {
            if (client.isRepartitioningComplete("test")) {
              break;
            }
            Thread.sleep(200);
          }

          for (DatabaseServer server : dbServers) {
            server.shutdownRepartitioner();
          }
        }
      }
    }
    catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  private String padNumericString(int offset) {
    String leading = "";
    if (offset < 10) {
      leading = "00000";
    }
    else if (offset < 100) {
      leading = "0000";
    }
    else if (offset < 1000) {
      leading = "000";
    }
    else if (offset < 10000) {
      leading = "00";
    }
    else if (offset < 100_000) {
      leading = "0";
    }
    else if (offset < 1_000_000) {
      leading = "";
    }
    return leading + offset;
  }

  private void insertPersons(String tableName, int currOffset) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("insert into " + tableName  + " (id, id2, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?)");
    int offset = currOffset;
    for (int j = 0; j < 100; j++) {
      stmt.setLong(1, offset);
      stmt.setLong(2, offset + 1000);
      String leading = padNumericString(offset);
      if (offset == 99_999) {
        System.out.println("here");
      }
      stmt.setString(3, leading);
      stmt.setString(4, "");//12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
      stmt.setBoolean(5, false);
      stmt.setString(6, "m");
      stmt.addBatch();
      if (offset++ % 1000 == 0) {
        System.out.println("progress: count=" + offset);
      }
    }
    stmt.executeBatch();
  }

  class Result {
    private long duration;
    private int count;

    public Result(long duration, int count) {
      this.duration = duration;
      this.count = count;
    }
  }

  private Map<String, Result> results = new ConcurrentHashMap<>();

  private void registerResults(String testName, long duration, int count) {
    System.out.println(testName + ": " + duration / 1_000_000 + " " +
        duration / count / 1_000_000D + " " +
        count / duration / 1_000_000D * 1000);

    if (results.put(testName, new Result(duration, count)) != null) {
      throw new DatabaseException("non-unique test: name=" + testName);
    }
  }

  public void testNoWhereClause() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons");
    long begin = System.nanoTime();
    int count = 0;
    ResultSet rs = stmt.executeQuery();
    for (int i = 0; i < outerFactor * 500_000; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), (long)i);
      count++;
    }
    long end = System.nanoTime();
    registerResults("no where clause", end-begin, count);
    //assertTrue((end - begin) < (8000 * 1_000_000L), String.valueOf(end-begin));
  }


  public void testInnerJoin() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement(
        "select persons.id, persons.socialsecuritynumber, persons2.socialsecuritynumber from persons " +
            " inner join persons2 on persons.id = persons2.id where persons.id>1000  order by persons.id desc");
    ResultSet rs = stmt.executeQuery();
    long begin = System.nanoTime();
    int count = 0;
    for (int i = (int)(outerFactor * 500_000) - 1; i > 1000; i--) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), (long)i);
      count++;
    }
    long end = System.nanoTime();
    registerResults("inner join", end-begin, count);
    //assertTrue((end - begin) < (3300 * 1_000_000L), String.valueOf(end-begin));
  }

  public void testLeftOuterJoin() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement(
        "select persons.id, persons.socialsecuritynumber, persons2.id from persons " +
            "left outer join persons2 on persons.id = persons2.id where persons2.id > 1000  order by persons2.id desc");
    ResultSet rs = stmt.executeQuery();
    long begin = System.nanoTime();
    int count = 0;
    for (int i = (int)(outerFactor * 500_000) - 1; i > 1000; i--) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), (long)i);
      count++;
    }
    long end = System.nanoTime();
    registerResults("left outer join", end-begin, count);
    //assertTrue((end - begin) < (3300 * 1_000_000L), String.valueOf(end-begin));
  }

  public void testRightOuterJoin() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement(
        "select persons.id, persons.socialsecuritynumber, persons2.id from persons " +
            "right outer join persons2 on persons.id = persons2.id where persons.id > 1000  order by persons.id desc");
    ResultSet rs = stmt.executeQuery();

    long begin = System.nanoTime();
    int count = 0;
    for (int i = (int)(outerFactor * 500_000) - 1; i > 1000; i--) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), (long)i);
      count++;
    }
    long end = System.nanoTime();
    registerResults("right outer join", end-begin, count);
    //assertTrue((end - begin) < (3300 * 1_000_000L), String.valueOf(end-begin));
  }



  public void testIdLookup() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id=1000");
    long begin = System.nanoTime();
    int count = 0;
    for (int i = 0; i < outerFactor * 10_000; i++) {
      ResultSet rs = stmt.executeQuery();
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), 1_000L);
      count++;
    }
    long end = System.nanoTime();
    registerResults("identity id=", end-begin, count);
    //assertTrue((end - begin) < (3300 * 1_000_000L), String.valueOf(end-begin));
  }

  public void testMath() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select id, id5 from persons where id >= 0 and id = id2 - 1000 order by id asc");
    ResultSet ret = stmt.executeQuery();

    long begin = System.nanoTime();
    int count = 0;
    ResultSet rs = stmt.executeQuery();
    for (int i = 0; i < outerFactor * 500_000; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), (long)i);
      count++;
    }
    System.out.println("math - " + count);
    long end = System.nanoTime();
    registerResults("math", end-begin, count);
    //assertTrue((end - begin) < (5000 * 1_000_000L), String.valueOf(end-begin));
  }

  public void testIdNoKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from employee where socialsecurityNumber='" + padNumericString(9 ) + "'");
    long begin = System.nanoTime();
    int count = 0;
    for (int i = 0; i < outerFactor * 10_000; i++) {
      ResultSet rs = stmt.executeQuery();
      assertTrue(rs.next());
      assertEquals(rs.getString("socialsecuritynumber"), "000009");
      count++;
    }
    long end = System.nanoTime();
    registerResults("identity secondary key id=", end-begin, count);
    //assertTrue((end - begin) < (8000 * 1_000_000L), String.valueOf(end-begin));
  }

  public void testRangeNoKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from employee where socialsecurityNumber>='" + padNumericString(1000) + "'");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = 1_000; i < outerFactor * 500_000; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getString("socialsecuritynumber"), padNumericString(i));
      count++;
    }
    assertFalse(rs.next());
    long end = System.nanoTime();
    registerResults("range secondary key", end-begin, count);
    //assertTrue((end - begin) < (5500 * 1_000_000L), String.valueOf(end-begin));
  }


  public void testRangeThreeKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from residence where id>1000 and id2>2000 and id3>3000");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = 1_001; i < outerFactor * 500_000; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), (long)i);
      assertEquals(rs.getLong("id2"), i + 1000L);
      assertEquals(rs.getLong("id3"), i + 2000L);
      count++;
    }
    assertFalse(rs.next());
    System.out.println("range three key - " + count);
    long end = System.nanoTime();
    registerResults("range three key", end-begin, count);
    //assertTrue((end - begin) < (3200 * 1_000_000L), String.valueOf(end-begin));
  }


  public void testRangeThreeKeyBackwards() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from residence where id3>3000 and id2>2000 and id>1000");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = 1_001; i < outerFactor * 500_000; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), (long)i);
      assertEquals(rs.getLong("id2"), i + 1000L);
      assertEquals(rs.getLong("id3"), i + 2000L);
      count++;
    }
    assertFalse(rs.next());
    System.out.println("rnage secondary key backwards - " + count);
    long end = System.nanoTime();
    registerResults("range secondary key backwards", end-begin, count);
    //assertTrue((end - begin) < (3200 * 1_000_000L), String.valueOf(end-begin));
  }


  public void testRangeThreeKeyMixed() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from residence where id3<" + (int)(outerFactor * 110000) +
        " and id2>4000 and id>1000");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = 3_001; i < outerFactor * 110_000 - 2_000; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), (long)i);
      assertEquals(rs.getLong("id2"), i + 1000L);
      assertEquals(rs.getLong("id3"), i + 2000L);
      count++;
    }
    assertFalse(rs.next());
    System.out.println("range secondary key mixed - " + count);
    long end = System.nanoTime();
    registerResults("range secondary key mixed", end-begin, count);
    //assertTrue((end - begin) < (2500 * 1_000_000L), String.valueOf(end-begin));
  }


  public void testRangeThreeKeySingle() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from residence where id>1000");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = 1_001; i < outerFactor * 500_000; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), (long)i);
      assertEquals(rs.getLong("id2"), i + 1000L);
      assertEquals(rs.getLong("id3"), i + 2000L);
      count++;
    }
    assertFalse(rs.next());
    long end = System.nanoTime();
    registerResults("range secondary key single", end-begin, count);
    //assertTrue((end - begin) < (3200 * 1_000_000L), String.valueOf(end-begin));
  }


  public void testNoKeyTwoKeyGreaterEqual() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from employee where socialsecurityNumber>='001000' and socialsecurityNumber<'" + padNumericString((int)(outerFactor * 95000)) + "' order by socialsecuritynumber desc");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = (int) (outerFactor * 95_000) - 1; i >= 1_000; i--) {
      assertTrue(rs.next(), String.valueOf(i));
      assertEquals(rs.getString("socialsecuritynumber"), padNumericString(i));
      count++;
    }
    assertFalse(rs.next());
    long end = System.nanoTime();
    registerResults("range two key greater equal2", end-begin, count);
    //assertTrue((end - begin) < (2000 * 1_000_000L), String.valueOf(end-begin));
  }


  public void testNoKeyTwoKeyGreater() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from employee where socialsecurityNumber>'001000' and socialsecurityNumber<'" + padNumericString((int)(outerFactor * 95000)) + "' order by socialsecuritynumber desc");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = (int) (outerFactor * 95_000) - 1; i > 1_000; i--) {
      assertTrue(rs.next(), String.valueOf(i));
      assertEquals(rs.getString("socialsecuritynumber"), padNumericString(i));
      count++;
    }
    assertFalse(rs.next());
    long end = System.nanoTime();
    registerResults("range two key greater", end-begin, count);
    //assertTrue((end - begin) < (1900 * 1_000_000L), String.valueOf(end-begin));
  }


  public void testNoKeyTwoKeyGreaterLeftSided() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from employee where socialsecurityNumber>'" + padNumericString(1000) +
        "' and (socialsecurityNumber>'" + padNumericString(3000) + "' and socialsecurityNumber<'" + padNumericString((int)(outerFactor * 95000)) +
        "') order by socialsecuritynumber desc");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = (int) (outerFactor * 95_000) - 1; i > 3_000; i--) {
      assertTrue(rs.next(), String.valueOf(i));
      assertEquals(rs.getString("socialsecuritynumber"), padNumericString(i));
      count++;
    }
    assertFalse(rs.next());
    long end = System.nanoTime();
    registerResults("range two key greater left sided", end-begin, count);
    //assertTrue((end - begin) < (2650 * 1_000_000L), String.valueOf(end-begin));
  }


  public void notIn() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id < " + (int)(outerFactor * 100000) + " and id > 10 and id not in (5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19) order by id desc");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = (int) (outerFactor * 100_000) - 1; i > 19; i--) {
      assertTrue(rs.next(), String.valueOf(i));
      assertEquals(rs.getLong("id"), (long)i);
      count++;
    }
    assertFalse(rs.next());
    long end = System.nanoTime();
    registerResults("not in", end-begin, count);
    //assertTrue((end - begin) < (1500 * 1_000_000L), String.valueOf(end-begin));
  }


  public void notInSecondary() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id2 < " + ((int)(outerFactor * 100000) + 1000) + " and id2 > 1010 and id2 not in (1005, 1006, 1007, 1008, 1009, 1010, 1011, 1012, 1013, 1014, 1015, 1016, 1017, 1018, 1019) order by id2 desc");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = (int) (outerFactor * 100000) + 999; i > 1019; i--) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id2"), (long)i);
      count++;
    }
    assertFalse(rs.next());
    long end = System.nanoTime();
    registerResults("range not in secondary", end-begin, count);
    //assertTrue((end - begin) < (2200 * 1_000_000L), String.valueOf(end-begin));
  }


  public void notInTableScan() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where socialsecuritynumber not in ('" +
        padNumericString(0) + "') order by id2 asc");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = 1001; i < (outerFactor * 500_000) + 1000; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id2"), (long)i);
      count++;
    }
    assertFalse(rs.next());
    long end = System.nanoTime();
    registerResults("range not in table scan", end-begin, count);
    //assertTrue((end - begin) < (8500 * 1_000_000L), String.valueOf(end-begin));
  }


  public void test2keyRange() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where (id < " + (int)(outerFactor * 92251) + ")  and (id > 1000)");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = 1001; i < (int)(outerFactor * 92251); i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), (long)i);
      count++;
    }
    assertFalse(rs.next());
    long end = System.nanoTime();
    registerResults("range two key", end-begin, count);
    //assertTrue((end - begin) < (1200 * 1_000_000L), String.valueOf(end-begin));
  }


  public void testSecondaryKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id2>1000");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = 1001; i < (outerFactor * 500_000) + 1000; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id2"), (long)i);
      count++;
    }
    assertFalse(rs.next());
    long end = System.nanoTime();
    registerResults("range two key seocndyary key", end-begin, count);
    //assertTrue((end - begin) < (4500 * 1_000_000L), String.valueOf(end-begin));
  }


  public void testTableScan() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where socialsecuritynumber>'" + padNumericString(0) + "'");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = 1; i < outerFactor * 500_000; i++) {
      assertTrue(rs.next());
      String leading = padNumericString(i);
      assertEquals(rs.getString("socialsecuritynumber"), leading);
      count++;
    }
    assertFalse(rs.next());
    System.out.println("table scan - " + count);
    long end = System.nanoTime();
    registerResults("table scan", end-begin, count);
    //assertTrue((end - begin) < (2500 * 1_000_000L), String.valueOf(end-begin));
  }


  public void testTwoKey() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id>=1000 and id<=" + (int)(outerFactor * 95000));
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = 1000; i <= outerFactor * 95_000; i++) {
      assertTrue(rs.next());
       assertEquals(rs.getLong("id"), (long)i);
      count++;
    }
    assertFalse(rs.next());
    long end = System.nanoTime();
    registerResults("range two key2", end-begin, count);
    //assertTrue((end - begin) < (1200 * 1_000_000L), String.valueOf(end-begin));
  }


  public void testTwoKeyRightSided() throws SQLException {

    PreparedStatement stmt = conn.prepareStatement("select * from persons where (id >= 3501 and id < " + (int)(outerFactor * 94751) + ") and (id > 1000)");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = 3501; i < (int)(outerFactor * 94_751); i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), (long)i);
      count++;
    }
    assertFalse(rs.next());
    System.out.println("range two key right sided - " + count);
    long end = System.nanoTime();
    registerResults("range two key right sided", end-begin, count);
    //assertTrue((end - begin) < (1200 * 1_000_000L), String.valueOf(end-begin));
  }


  public void testTwoKeyLeftSidedGreater() throws SQLException {

    PreparedStatement stmt = conn.prepareStatement("select * from persons where (id > 1000) and (id >= 3501 and id < " + (int)(outerFactor * 94751) + ")");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = 3501; i < (int)(outerFactor * 94_751); i++) {
      assertTrue(rs.next(), String.valueOf(i));
      assertEquals(rs.getLong("id"), (long)i);
      count++;
    }
    assertFalse(rs.next());
    long end = System.nanoTime();
    registerResults("range two key left sided greater", end-begin, count);
    //assertTrue((end - begin) < (1300 * 1_000_000L), String.valueOf(end-begin));
  }


  public void testTwoKeyGreater() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id>1000 and id<" + (int)(outerFactor * 95000));
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = 1001; i < outerFactor * 95_000; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), (long)i);
      count++;
    }
    assertFalse(rs.next());
    long end = System.nanoTime();
    registerResults("range two key greater2", end-begin, count);
    //assertTrue((end - begin) < (1600 * 1_000_000L), String.valueOf(end-begin));
  }


  public void testTwoKeyGreaterBackwards() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id<" + (int)(outerFactor * 95000) + " and id>1000");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = 1001; i < outerFactor * 95_000; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), (long)i);
      count++;
    }
    assertFalse(rs.next());
    long end = System.nanoTime();
    registerResults("range two key greater backwards", end-begin, count);
    //assertTrue((end - begin) < (1600 * 1_000_000L), String.valueOf(end-begin));
  }



  public void testTwoKeyLeftSidedGreaterEqual() throws SQLException {

    PreparedStatement stmt = conn.prepareStatement("select * from persons where (id >= 1000) and (id >= 3501 and id < " +
        (int)(outerFactor * 94751) + ")");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = 3501; i < (int)(outerFactor * 94_751); i++) {
      assertTrue(rs.next(), String.valueOf(i));
      assertEquals(rs.getLong("id"), (long)i);
      count++;
    }
    assertFalse(rs.next());
    long end = System.nanoTime();
    registerResults("range two key left sided greater equal", end-begin, count);
    //assertTrue((end - begin) < (800 * 1_000_000L), String.valueOf(end-begin));
  }


  public void testCountTwoKeyGreaterEqual() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select count(*) from persons where id>=1000 and id<=5000");
    long begin = System.nanoTime();
    int count = 0;
    for (int i = 0; i < (int)(outerFactor  * 1020); i++) {
      ResultSet rs = stmt.executeQuery();
      assertTrue(rs.next());
      assertEquals(rs.getLong(1), 4001L);
      count++;
    }
    long end = System.nanoTime();
    registerResults("count range two key greater equal", end-begin, count);
    //assertTrue((end - begin) < (12000 * 1_000_000L), String.valueOf(end-begin));
  }


  public void testMaxWhere() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select max(id) as maxValue from persons where id<=25000");
    long begin = System.nanoTime();
    int count = 0;
    for (int i = 0; i < outerFactor * 10; i++) {
      ResultSet rs = stmt.executeQuery();
      assertTrue(rs.next());
      assertEquals(rs.getLong("maxValue"), 25_000L);
      count++;
    }
    long end = System.nanoTime();
    registerResults("max where", end-begin, count);
    //assertTrue((end - begin) < (2000 * 1_000_000L), String.valueOf(end-begin));
  }


  public void testMax() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select max(id) as maxValue from persons");
    long begin = System.nanoTime();
    int count = 0;
    for (int i = 0; i < outerFactor * 10; i++) {
      ResultSet rs = stmt.executeQuery();
      assertTrue(rs.next());
      assertEquals(rs.getLong("maxValue"), (long)((outerFactor * 500_000) - 1L));
      assertEquals(rs.getInt("maxValue"), (int)(long)((outerFactor * 500_000) - 1L));
      assertEquals(rs.getString("maxValue"), String.valueOf((long)((outerFactor * 500_000) - 1L)));
      assertEquals(rs.getDouble("maxValue"), (double)(long)((outerFactor * 500_000) - 1L));
      assertEquals(rs.getFloat("maxValue"), (float)(long)((outerFactor * 500_000) - 1L));
      count++;
    }
    long end = System.nanoTime();
    registerResults("max", end-begin, count);
    //assertTrue((end - begin) < (18000 * 1_000_000L), String.valueOf(end-begin));
  }


  public void testCount() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select count(*)  from persons");
    long begin = System.nanoTime();
    int count = 0;
    for (int i = 0; i < outerFactor * 1020; i++) {
      ResultSet rs = stmt.executeQuery();
      assertTrue(rs.next());
      assertEquals(rs.getLong(1), (long)(outerFactor * 500_000L));
      assertEquals(rs.getInt(1), (int)(long)(outerFactor * 500_000L));
      assertEquals(rs.getString(1), String.valueOf((long)(outerFactor * 500_000L)));
      assertEquals(rs.getDouble(1), (double)(long)(outerFactor * 500_000L));
      assertEquals(rs.getFloat(1),(float)(long)(outerFactor * 500_000L));
      count++;
    }
    long end = System.nanoTime();
    registerResults("count", end-begin, count);
    //assertTrue((end - begin) < (50 * 1_000_000L), String.valueOf(end-begin));
  }


  public void testSort() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id>=1000 order by id desc");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = (int) (outerFactor * 500_000) - 1; i >=  1000; i--) {
      assertTrue(rs.next());
      String leading = padNumericString(i);
      assertEquals(rs.getString("socialsecuritynumber"), leading);
      count++;
    }
    long end = System.nanoTime();
    registerResults("range sort", end-begin, count);
    //assertTrue((end - begin) < (2300 * 1_000_000L), String.valueOf(end-begin));
  }


  public void testSortDisk() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id>=1000 order by socialsecuritynumber desc");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = (int) (outerFactor * 500_000) - 1; i >= 1000; i--) {
      assertTrue(rs.next(), String.valueOf(i));
      String leading = padNumericString(i);
      assertEquals(rs.getString("socialsecuritynumber"), leading);
      count++;
    }
    long end = System.nanoTime();
    registerResults("range sort disk", end-begin, count);
    //assertTrue((end - begin) < (12000 * 1_000_000L), String.valueOf(end-begin));
  }


  public void testId2() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id2=2000");
    long begin = System.nanoTime();
    int count = 0;
    for (int i = 0; i < outerFactor * 10_000; i++) {
      ResultSet rs = stmt.executeQuery();
      assertTrue(rs.next());
      assertEquals(rs.getLong("id2"), 2_000L);
      count++;
    }
    long end = System.nanoTime();
    registerResults("id2", end-begin, count);
    //assertTrue((end - begin) < (9000 * 1_000_000L), String.valueOf(end-begin));
  }


  public void testId2Range() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id2>2000");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = 2001; i < (outerFactor * 500_000) + 1000; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id2"), (long)i);
      count++;
    }
    assertFalse(rs.next());
    long end = System.nanoTime();
    registerResults("range id2", end-begin, count);
    //assertTrue((end - begin) < (6200 * 1_000_000L), String.valueOf(end-begin));
  }


  public void testOtherExpression() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id=1000 and id2 < 10000 and id2 > 1000");
    long begin = System.nanoTime();
    int count = 0;
    for (int i = 0; i < outerFactor * 10_000; i++) {
      ResultSet rs = stmt.executeQuery();
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), 1_000L);
      count++;
    }
    long end = System.nanoTime();
    registerResults("range other expression", end-begin, count);
    //assertTrue((end - begin) < (3300 * 1_000_000L), String.valueOf(end-begin));
  }


  public void testRange() throws SQLException {
    for (int j = 0; j < 10; j++) {
      int max = 0;
      PreparedStatement stmt = conn.prepareStatement("select * from persons where id>=100");
      ResultSet rs = stmt.executeQuery();
      for (int i = 100; i < outerFactor * 500_000; i++) {
        assertTrue(rs.next());
        assertEquals(rs.getLong("id"), (long) i);
        //assertEquals(rs.getLong("id2"), i + 1000L);
        max++;
      }
      System.out.println("max=" + max);
      assertFalse(rs.next());
    }
    int count = 0;
    long begin = System.nanoTime();
    for (int j = 0; j < 10; j++) {
      int max = 0;
      PreparedStatement stmt = conn.prepareStatement("select * from persons where id>=100");
      ResultSet rs = stmt.executeQuery();
      for (int i = 100; i < outerFactor * 500_000; i++) {
        assertTrue(rs.next());
        assertEquals(rs.getLong("id"), (long) i);
        //assertEquals(rs.getLong("id2"), i + 1000L);
        count++;
        max++;
      }
      System.out.println("max=" + max);
      assertFalse(rs.next());
    }
    long end = System.nanoTime();
    registerResults("range", end-begin, count);
    //assertTrue((end - begin) < (1800 * 1_000_000L), String.valueOf(end-begin));
  }

  public void testRangeGreaterDescend() throws SQLException {
    for (int j = 0; j < 10; j++) {
      int max = 0;
      PreparedStatement stmt = conn.prepareStatement("select * from persons where id>=100 order by id desc");
      ResultSet rs = stmt.executeQuery();
      for (int i = (int)(outerFactor * 500_000) - 1; i >= 100; i--) {
        try {
          if (i == 100 || i == 101) {
            System.out.println("i=100");
          }
          assertTrue(rs.next());
          assertEquals(rs.getLong("id"), (long) i);
          //assertEquals(rs.getLong("id2"), i + 1000L);
          max++;
        }
        catch (Exception e) {
          System.out.println("i=" + i);
          throw new DatabaseException(e);
        }
      }
      System.out.println("max=" + max);
      assertFalse(rs.next());
    }
    int count = 0;
    long begin = System.nanoTime();
    for (int j = 0; j < 10; j++) {
      int max = 0;
      PreparedStatement stmt = conn.prepareStatement("select * from persons where id>=100 order by id desc");
      ResultSet rs = stmt.executeQuery();
      for (int i = (int)(outerFactor * 500_000) - 1; i >= 100; i--) {
        assertTrue(rs.next());
        assertEquals(rs.getLong("id"), (long) i);
        //assertEquals(rs.getLong("id2"), i + 1000L);
        count++;
        max++;
      }
      System.out.println("max=" + max);
      assertFalse(rs.next());
    }
    long end = System.nanoTime();
    registerResults("range greater descend", end-begin, count);
    //assertTrue((end - begin) < (1800 * 1_000_000L), String.valueOf(end-begin));
  }

  public void testRangeLess() throws SQLException {
    for (int j = 0; j < 10; j++) {
      int max = 0;
      PreparedStatement stmt = conn.prepareStatement("select * from persons where id< " + (int)(outerFactor * 490_000));
      ResultSet rs = stmt.executeQuery();
      for (int i = 0; i < outerFactor * 490_000; i++) {
        assertTrue(rs.next());
        assertEquals(rs.getLong("id"), (long) i);
        //assertEquals(rs.getLong("id2"), i + 1000L);
        max++;
      }
      System.out.println("max=" + max);
      assertFalse(rs.next());
    }
    int count = 0;
    long begin = System.nanoTime();
    for (int j = 0; j < 10; j++) {
      int max = 0;
      PreparedStatement stmt = conn.prepareStatement("select * from persons where id< " + (int)(outerFactor * 490_000));
      ResultSet rs = stmt.executeQuery();
      for (int i = 0; i < outerFactor * 490_000; i++) {
        assertTrue(rs.next());
        assertEquals(rs.getLong("id"), (long) i);
        //assertEquals(rs.getLong("id2"), i + 1000L);
        count++;
        max++;
      }
      System.out.println("max=" + max);
      assertFalse(rs.next());
    }
    long end = System.nanoTime();
    registerResults("range less", end-begin, count);
    //assertTrue((end - begin) < (1800 * 1_000_000L), String.valueOf(end-begin));
  }

  public void testRangeOtherExpression() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id>1000 and id2 < " + (int)(outerFactor * 500000) + " and id2 > 1000");
    long begin = System.nanoTime();
    ResultSet rs = stmt.executeQuery();
    int count = 0;
    for (int i = 1001; i < (outerFactor * 500_000) - 1000; i++) {
      try {
        assertTrue(rs.next());
        assertEquals(rs.getLong("id"), (long)i);
        count++;
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }
    assertFalse(rs.next());
    long end = System.nanoTime();
    registerResults("range other expression2", end-begin, count);
    //assertTrue((end - begin) < (2500 * 1_000_000L), String.valueOf(end-begin));
  }


  public void testSecondary() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id2=2000");
    long begin = System.nanoTime();
    int count = 0;
    for (int i = 0; i < outerFactor * 10_000; i++) {
      ResultSet rs = stmt.executeQuery();
      assertTrue(rs.next());
      assertEquals(rs.getLong("id2"), 2_000L);
      count++;
    }
    long end = System.nanoTime();
    registerResults("id secdonary", end-begin, count);
    //assertTrue((end - begin) < (8000 * 1_000_000L), String.valueOf(end-begin));
  }

  public void testNot() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where not(id > 100000 and id <= " + outerFactor * 500_000 + ")");
    long begin = System.nanoTime();
    int count = 0;
    ResultSet rs = stmt.executeQuery();
    for (int i = 0; i <= outerFactor * 100_000; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), (long)i);
      count++;
    }
    long end = System.nanoTime();
    registerResults("not", end-begin, count);
    //assertTrue((end - begin) < (8000 * 1_000_000L), String.valueOf(end-begin));
  }

  public void testFunctionAvg() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where avg(id, id2) > " + outerFactor * 250_000);
    long begin = System.nanoTime();
    int count = 0;
    ResultSet rs = stmt.executeQuery();
    for (int i = (int)(outerFactor * 250_000) - 499; i < outerFactor * 500_000; i++) {
      try {
        assertTrue(rs.next(), String.valueOf(i));
        assertEquals(rs.getLong("id"), (long)i, String.valueOf(i));
        count++;
      }
      catch (Exception e) {
        e.printStackTrace();
        throw e;
      }
    }
    long end = System.nanoTime();
    registerResults("function avg", end-begin, count);
    //assertTrue((end - begin) < (8000 * 1_000_000L), String.valueOf(end-begin));
  }

  public void testFunctionCustom() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where custom('com.sonicbase.bench.CustomFunctions', 'min', id, id2) > " + 50_000);
    long begin = System.nanoTime();
    int count = 0;
    ResultSet rs = stmt.executeQuery();
    for (int i = 50_001; i < outerFactor * 500_000; i++) {
      try {
        assertTrue(rs.next(), String.valueOf(i));
        assertEquals(rs.getLong("id"), (long)i, String.valueOf(i));
        count++;
      }
      catch (Exception e) {
        e.printStackTrace();
        throw e;
      }
    }
    long end = System.nanoTime();
    registerResults("function custom min", end-begin, count);
    //assertTrue((end - begin) < (8000 * 1_000_000L), String.valueOf(end-begin));
  }

  public void testFunctionMin() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select * from persons where min(id, id2) > " + 50_000);
    long begin = System.nanoTime();
    int count = 0;
    ResultSet rs = stmt.executeQuery();
    for (int i = 50_001; i < outerFactor * 500_000; i++) {
      try {
        assertTrue(rs.next(), String.valueOf(i));
        assertEquals(rs.getLong("id"), (long)i, String.valueOf(i));
        count++;
      }
      catch (Exception e) {
        e.printStackTrace();
        throw e;
      }
    }
    long end = System.nanoTime();
    registerResults("function min", end-begin, count);
    //assertTrue((end - begin) < (8000 * 1_000_000L), String.valueOf(end-begin));
  }

  public void testUnion() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("(select id from persons where id > 1000) union (select id from persons2 where id > 1000) order by id asc");
    long begin = System.nanoTime();
    int count = 0;
    ResultSet rs = stmt.executeQuery();
    for (int i = 0; i < outerFactor * 400_000; i++) {
      assertTrue(rs.next());
      try {
        assertEquals(rs.getLong("id"), i + 1001L);
      }
      catch (Exception e) {
        System.out.println("Error: count=" + count);
        break;
      }
      count++;
    }
    long end = System.nanoTime();
    registerResults("union", end-begin, count);
    //assertTrue((end - begin) < (8000 * 1_000_000L), String.valueOf(end-begin));
  }

  public void testUnionInMemory() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("(select id from persons where id > 1000 and id < " + (int)(outerFactor * 31000) +
        ") union (select id from persons2 where id > 1000 and id < " + (int)(outerFactor * 31000) + ") order by id asc");
    long begin = System.nanoTime();
    int count = 0;
    ResultSet rs = stmt.executeQuery();
    for (int i = 1001; i < outerFactor * 31_000; i++) {
      assertTrue(rs.next());
      try {
        assertEquals(rs.getLong("id"), (long)i);
      }
      catch (Exception e) {
        System.out.println("Error: count=" + count);
        break;
      }
      count++;
    }
    long end = System.nanoTime();
    registerResults("union inMemory", end-begin, count);
    //assertTrue((end - begin) < (8000 * 1_000_000L), String.valueOf(end-begin));
  }

  public void testUnionAll() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("(select id from persons where id > 1000) union all (select id from persons2 where id > 1000) order by id asc");
    long begin = System.nanoTime();
    int count = 0;
    ResultSet rs = stmt.executeQuery();
    for (int i = 1001; i < outerFactor * 500_000; i++) {
      assertTrue(rs.next());
       assertEquals(rs.getLong("id"), (long)i);
      count++;
    }
    long end = System.nanoTime();
    registerResults("union all", end-begin, count);
    //assertTrue((end - begin) < (8000 * 1_000_000L), String.valueOf(end-begin));
  }

  public void testIntersect() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("(select id from persons where id > 1000 and id < "+
        (int)(outerFactor * 100_000) + ") intersect (select id from persons2 where id > 1000) order by id asc");
    long begin = System.nanoTime();
    int count = 0;
    ResultSet rs = stmt.executeQuery();
    for (int i = 1001; i < outerFactor * 100_000; i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), (long)i);
      count++;
    }
    long end = System.nanoTime();
    registerResults("intersect", end-begin, count);
    //assertTrue((end - begin) < (8000 * 1_000_000L), String.valueOf(end-begin));
  }

  public void testExcept() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("(select id from persons where id > 1000 and id < "+
        (int)(outerFactor * 500_000) + ") except (select id from persons2 where id > 1000 and id < " + (int)(outerFactor * 100_000) + ") order by id asc");
    long begin = System.nanoTime();
    int count = 0;
    ResultSet rs = stmt.executeQuery();
    for (int i = (int)(outerFactor * 100_000); i < (int)(outerFactor * 500_000); i++) {
      assertTrue(rs.next());
      assertEquals(rs.getLong("id"), (long)i);
      count++;
    }
    long end = System.nanoTime();
    registerResults("except", end-begin, count);
    //assertTrue((end - begin) < (8000 * 1_000_000L), String.valueOf(end-begin));
  }
}
