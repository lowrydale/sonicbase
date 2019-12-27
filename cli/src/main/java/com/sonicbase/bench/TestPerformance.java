package com.sonicbase.bench;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.server.DatabaseServer;
import com.sonicbase.server.NettyServer;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class TestPerformance {

  public static final Logger logger = LoggerFactory.getLogger(TestPerformance.class);

  public static final String PROGRESS_COUNT_STR = "progress: count=";
  public static final String ORDER_BY_ID_ASC_STR = ") order by id asc";
  public static final String MAX_STR = "max={}";
  public static final String MIN_VALUE_STR = "minValue";
  public static final String LOCALHOST_STR = "localhost";
  private Connection conn;
  private DatabaseClient client;
  private final List<Thread> serverThreads = new ArrayList<>();
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

//    methods.add("testBatchIdentityOr");
//    methods.add("testBatchIdentityIn");
//    methods.add("testIdLookup");
//    methods.add("testMath");
//    methods.add("testIdNoKey");
//    methods.add("testRangeNoKey");
//    methods.add("testRangeThreeKey");
//    methods.add("testRangeThreeKeyBackwards");
//    methods.add("testRangeThreeKeyMixed");
//    methods.add("testRangeThreeKeySingle");
//    methods.add("testNoKeyTwoKeyGreaterEqual");
//    methods.add("testNoKeyTwoKeyGreater");
//    methods.add("testNoKeyTwoKeyGreaterLeftSided");
//    methods.add("notIn");
//    methods.add("notInSecondary");
//    methods.add("notInTableScan");
//    methods.add("test2keyRange");
//    methods.add("testSecondaryKey");
//    methods.add("testTableScan");
//    methods.add("testTwoKey");
//    methods.add("testTwoKeyRightSided");
//    methods.add("testTwoKeyLeftSidedGreater");
//    methods.add("testTwoKeyGreater");
//    methods.add("testTwoKeyGreaterBackwards");
//    methods.add("testTwoKeyLeftSidedGreaterEqual");
//    methods.add("testCountTwoKeyGreaterEqual");
//    methods.add("testMaxWhere");
//    methods.add("testMax");
//    methods.add("testMin");
//    methods.add("testCount");
//    methods.add("testSort");
//    methods.add("testSortDisk");
//    methods.add("testId2");
//    methods.add("testId2Range");
//    methods.add("testOtherExpression");
//    methods.add("testNoWhereClause");
//
//    methods.add("testRangeGreaterDescend");
    methods.add("testRange");
//    methods.add("testRangeLess");
//
//    methods.add("testRangeOtherExpression");
//    methods.add("testSecondary");
//    methods.add("testUnion");
//    methods.add("testUnionInMemory");
//    methods.add("testUnionAll"); //
//    methods.add("testIntersect");
//    methods.add("testExcept");
//    methods.add("testNot");
//    methods.add("testFunctionAvg");
//    methods.add("testFunctionMin");
//    methods.add("testFunctionCustom");

    for (String method : methods) {
      try {
        Method methodObj = this.getClass().getMethod(method);
        methodObj.invoke(this);
      }
      catch (Exception e) {
        logger.error("Error", e);
      }
    }
    summarize();
  }

  public void assertTrue(boolean value) {
    if (validate && !value) {
      throw new DatabaseException("Error assertTrue=false");
    }
  }

  public void assertTrue(boolean value, String msg) {
    if (validate && !value) {
      throw new DatabaseException(msg);
    }
  }

  public void assertFalse(boolean value) {
    if (validate && value) {
      throw new DatabaseException("Error assertFalse=true");
    }
  }

  public void assertEquals(Object lhs, Object rhs) {
    if (validate && !lhs.equals(rhs)) {
      throw new DatabaseException("lhs != rhs: lhs=" + lhs + ", rhs=" + rhs);
    }
  }

  public void assertEquals(Object lhs, Object rhs, String msg) {
    if (validate && !lhs.equals(rhs)) {
      throw new DatabaseException("lhs != rhs: lhs=" + lhs + ", rhs=" + rhs + ", msg=" + msg);
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
      logger.info(prefix + name + ": " + entry.getValue().duration / 1_000_000 + " " +
          entry.getValue().duration / entry.getValue().count / 1_000_000D + " " +
          (double)entry.getValue().count / (double)entry.getValue().duration * 1_000_000D * 1000D);
      System.out.println(prefix + name + ": " + entry.getValue().duration / 1_000_000 + " " +
          entry.getValue().duration / entry.getValue().count / 1_000_000D + " " +
          (double)entry.getValue().count / (double)entry.getValue().duration * 1_000_000D * 1000D);
    }
  }

  public void setup(float outerFactor) throws IOException, InterruptedException, SQLException, ClassNotFoundException, ExecutionException {
    try {
      int outerCount = (int) (outerFactor * 5_000);

      FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db-data"));

      DatabaseClient.getServers().clear();

      dbServers = new DatabaseServer[2];

      if (server) {
        final CountDownLatch latch = new CountDownLatch(4);
        final NettyServer server0_0 = new NettyServer(128);
        Thread thread = new Thread(() -> {
          server0_0.startServer(new String[]{"-port", String.valueOf(9010), "-host", LOCALHOST_STR,
              "-mport", String.valueOf(9010), "-mhost", LOCALHOST_STR, "-shard", String.valueOf(0)});
          latch.countDown();
        });
        serverThreads.add(thread);
        thread.start();
        while (true) {

          if (server0_0.isRunning()) {
            break;
          }
          Thread.sleep(100);
        }

        final NettyServer server0_1 = new NettyServer(128);
        thread = new Thread(() -> {
          server0_1.startServer(new String[]{"-port", String.valueOf(9060), "-host", LOCALHOST_STR,
              "-mport", String.valueOf(9060), "-mhost", LOCALHOST_STR, "-shard", String.valueOf(0)});
          latch.countDown();
        });
        serverThreads.add(thread);
        thread.start();

        while (true) {

          if (server0_1.isRunning()) {
            break;
          }
          Thread.sleep(100);
        }

        while (true) {

          if (server0_0.isRunning() && server0_1.isRunning() /*&& server1_0.isRunning() && server1_1.isRunning()*/) {
            break;
          }
          Thread.sleep(100);
        }

        Thread.sleep(5_000);

        dbServers[0] = server0_0.getDatabaseServer();
        dbServers[1] = server0_1.getDatabaseServer();

        logger.info("Started 2 servers");
        System.out.println("Started 2 servers");

      }

      Class.forName("com.sonicbase.jdbcdriver.Driver");

      boolean sonicbase = true;
      if (!sonicbase) {
        Class.forName("com.mysql.jdbc.Driver");
        conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/db", "root", "pass");
      }
      else {
        if (server) {
          conn = DriverManager.getConnection("jdbc:sonicbase:localhost:9010", "user", "password");
          ((ConnectionProxy) conn).getDatabaseClient().createDatabase("test");
          conn.close();
        }

        conn = DriverManager.getConnection("jdbc:sonicbase:localhost:9010/test", "user", "password");
        client = ((ConnectionProxy) conn).getDatabaseClient();
        client.syncSchema();
      }

      if (true || server) {
        //
        try (PreparedStatement stmt = conn.prepareStatement("drop table persons")) {
          stmt.executeUpdate();
        }
        catch (Exception e) {

        }

        try (PreparedStatement stmt = conn.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))")) {
          stmt.executeUpdate();
        }

        try (PreparedStatement stmt = conn.prepareStatement("drop table Persons2")) {
          stmt.executeUpdate();
        }
        catch (Exception e) {

        }

        try (PreparedStatement stmt = conn.prepareStatement("create table Persons2 (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))")) {
          stmt.executeUpdate();
        }

        try (PreparedStatement stmt = conn.prepareStatement("drop index id2")) {
          stmt.executeUpdate();
        }
        catch (Exception e) {

        }

        try (PreparedStatement stmt = conn.prepareStatement("create index id2 on persons(id2)")) {
          stmt.executeUpdate();
        }

        try (PreparedStatement stmt = conn.prepareStatement("drop table Employee")) {
          stmt.executeUpdate();
        }
        catch (Exception e) {

        }

        try (PreparedStatement stmt = conn.prepareStatement("create table Employee (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20))")) {
          stmt.executeUpdate();
        }

        try (PreparedStatement stmt = conn.prepareStatement("drop index socialsecuritynumber")) {
          stmt.executeUpdate();
        }
        catch (Exception e) {

        }

        try (PreparedStatement stmt = conn.prepareStatement("create index socialsecuritynumber on employee(socialsecurityNumber)")) {
          stmt.executeUpdate();
        }

        try (PreparedStatement stmt = conn.prepareStatement("drop table Residence")) {
          stmt.executeUpdate();
        }
        catch (Exception e) {

        }

        try (PreparedStatement stmt = conn.prepareStatement("create table Residence (id BIGINT, id2 BIGINT, id3 BIGINT, address VARCHAR(20), PRIMARY KEY (id, id2, id3))")) {
          stmt.executeUpdate();
        }


        //rebalance
        if (server) {
          for (DatabaseServer server : dbServers) {
            server.shutdownRepartitioner();
          }
        }

        List<Future> futures = new ArrayList<>();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(8, 8, 10_000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
        int offset = 0;
        for (int i = 0; i < outerCount; i++) {
          final int currOffset = offset;
          offset += 100;
          futures.add(executor.submit((Callable) () -> {
            insertPersons("Persons", currOffset);
            insertPersons("Persons2", currOffset);
            return null;
          }));

        }

        for (Future future : futures) {
          future.get();
        }

        offset = 0;
        for (int i = 0; i < outerCount; i++) {
          final int currOffset = offset;
          offset += 100;
          futures.add(executor.submit((Callable) () -> {
            int offset1 = currOffset;
            PreparedStatement stmt1 = conn.prepareStatement("insert into residence (id, id2, id3, address) VALUES (?, ?, ?, ?)");
            for (int j = 0; j < 100; j++) {
              stmt1.setLong(1, offset1);
              stmt1.setLong(2, offset1 + 1000L);
              stmt1.setLong(3, offset1 + 2000L);
              stmt1.setString(4, "5078 West Black");
              if (offset1++ % 1000 == 0) {
                logger.info(PROGRESS_COUNT_STR + offset1);
                System.out.println(PROGRESS_COUNT_STR + offset1);
              }
              stmt1.addBatch();
            }
            stmt1.executeBatch();
            return null;
          }));
        }

        for (Future future : futures) {
          future.get();
        }

        offset = 0;
        for (int i = 0; i < outerCount; i++) {
          final int currOffset = offset;
          offset += 100;
          futures.add(executor.submit((Callable) () -> {
            int offset12 = currOffset;
            PreparedStatement stmt12 = conn.prepareStatement("insert into employee (id, id2, socialSecurityNumber) VALUES (?, ?, ?)");
            for (int j = 0; j < 100; j++) {
              stmt12.setLong(1, offset12);
              String leading = padNumericString(offset12);
              stmt12.setLong(2, offset12 + 1000L);
              stmt12.setString(3, leading);
              if (offset12++ % 1000 == 0) {
                logger.info(PROGRESS_COUNT_STR + offset12);
                System.out.println(PROGRESS_COUNT_STR + offset12);
              }
              stmt12.addBatch();
            }
            stmt12.executeBatch();
            return null;
          }));
        }
        for (Future future : futures) {
          future.get();
        }

        if (client != null) {
          client.beginRebalance("test");

          while (true) {
            if (client.isRepartitioningComplete("test")) {
              break;
            }
            Thread.sleep(200);
          }

          if (server) {
            for (DatabaseServer server : dbServers) {
              server.shutdownRepartitioner();
            }
          }

          client.beginRebalance("test");

          while (true) {
            if (client.isRepartitioningComplete("test")) {
              break;
            }
            Thread.sleep(200);
          }

          if (server) {
            for (DatabaseServer server : dbServers) {
              server.shutdownRepartitioner();
            }
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
    try (PreparedStatement stmt = conn.prepareStatement("insert into " + tableName  + " (id, id2, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?)")) {
      int offset = currOffset;
      for (int j = 0; j < 100; j++) {
        stmt.setLong(1, offset);
        stmt.setLong(2, offset + 1000L);
        String leading = padNumericString(offset);
        stmt.setString(3, leading);
        stmt.setString(4, "");
        stmt.setBoolean(5, false);
        stmt.setString(6, "m");
        stmt.addBatch();
        if (offset++ % 1000 == 0) {
          logger.info(PROGRESS_COUNT_STR + offset);
          System.out.println(PROGRESS_COUNT_STR + offset);
        }
      }
      stmt.executeBatch();
    }
  }

  class Result {
    private final long duration;
    private final int count;

    public Result(long duration, int count) {
      this.duration = duration;
      this.count = count;
    }
  }

  private final Map<String, Result> results = new ConcurrentHashMap<>();

  private void registerResults(String testName, long duration, int count) {
    if (count == 0) {
     logger.info(testName + ":" + 0 + " " + 0 + " " + 0);
     System.out.println(testName + ":" + 0 + " " + 0 + " " + 0);
    }
    else {
      logger.info(testName + ": " + duration / 1_000_000d + " " +
          (double)duration / count / 1_000_000d + " " +
          (double)count / duration / 1_000_000d * 1000d);
      System.out.println(testName + ": " + duration / 1_000_000d + " " +
          (double)duration / count / 1_000_000d + " " +
          (double)count / duration / 1_000_000d * 1000d);

      if (results.put(testName, new Result(duration, count)) != null) {
        throw new DatabaseException("non-unique test: name=" + testName);
      }
    }
  }

  public void testNoWhereClause() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("select * from persons")) {
      long begin = System.nanoTime();
      int count = 0;
      try (ResultSet rs = stmt.executeQuery()) {
        for (int i = 0; i < outerFactor * 500_000; i++) {
          assertTrue(rs.next());
          assertEquals(rs.getLong("id"), (long) i);
          count++;
        }
        long end = System.nanoTime();
        registerResults("no where clause", end - begin, count);
      }
    }
  }


  public void testInnerJoin() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(
        "select persons.id, persons.socialsecuritynumber, persons2.socialsecuritynumber from persons " +
            " inner join persons2 on persons.id = persons2.id where persons.id>1000  order by persons.id desc")) {
      try (ResultSet rs = stmt.executeQuery()) {
        long begin = System.nanoTime();
        int count = 0;
        for (int i = (int) (outerFactor * 500_000) - 1; i > 1000; i--) {
          assertTrue(rs.next());
          assertEquals(rs.getLong("id"), (long) i);
          count++;
        }
        long end = System.nanoTime();
        registerResults("inner join", end - begin, count);
      }
    }
  }

  public void testLeftOuterJoin() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(
        "select persons.id, persons.socialsecuritynumber, persons2.id from persons " +
            "left outer join persons2 on persons.id = persons2.id where persons2.id > 1000  order by persons2.id desc")) {
      try (ResultSet rs = stmt.executeQuery()) {
        long begin = System.nanoTime();
        int count = 0;
        for (int i = (int) (outerFactor * 500_000) - 1; i > 1000; i--) {
          assertTrue(rs.next());
          assertEquals(rs.getLong("id"), (long) i);
          count++;
        }
        long end = System.nanoTime();
        registerResults("left outer join", end - begin, count);
      }
    }
  }

  public void testBatchIdentityIn() throws SQLException {
    int offset = 0;
    int count = 0;
    long begin = System.nanoTime();
    while (offset < outerFactor * 500_000) {
      StringBuilder builder = new StringBuilder("select * from persons where id in (");
      for (int i = 0; i < 200; i++) {
        if (i == 0) {
          builder.append("?");
        }
        else {
          builder.append(", ?");
        }
      }
      builder.append(ORDER_BY_ID_ASC_STR);
      try (PreparedStatement stmt = conn.prepareStatement(builder.toString())) {
        int beginOffset = offset;
        for (int i = 0; i < 200; i++) {
          stmt.setLong(i + 1, offset++);
        }
        try (ResultSet rs = stmt.executeQuery()) {
          for (int i = beginOffset; i < offset; i++) {
            assertTrue(rs.next());
            assertEquals(rs.getLong("id"), (long) i);
            count++;
          }
        }
      }
    }
    long end = System.nanoTime();
    registerResults("batch identity in", end-begin, count);
  }

  public void testBatchIdentityOr() throws SQLException {
    int offset = 0;
    int count = 0;
    long begin = System.nanoTime();
    while (offset < outerFactor * 500_000) {
      StringBuilder builder = new StringBuilder("select * from persons where id = ? ");
      for (int i = 0; i < 199; i++) {
        builder.append(" or id = ?");
      }
      builder.append(" order by id asc");
      try (PreparedStatement stmt = conn.prepareStatement(builder.toString())) {
        int beginOffset = offset;
        for (int i = 0; i < 200; i++) {
          stmt.setLong(i + 1, offset++);
        }
        try (ResultSet rs = stmt.executeQuery()) {
          for (int i = beginOffset; i < offset; i++) {
            assertTrue(rs.next());
            assertEquals(rs.getLong("id"), (long) i);
            count++;
          }
        }
      }
    }
    long end = System.nanoTime();
    registerResults("batch identity or", end-begin, count);
  }

  public void testRightOuterJoin() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(
        "select persons.id, persons.socialsecuritynumber, persons2.id from persons " +
            "right outer join persons2 on persons.id = persons2.id where persons.id > 1000  order by persons.id desc")) {
      try (ResultSet rs = stmt.executeQuery()) {
        long begin = System.nanoTime();
        int count = 0;
        for (int i = (int) (outerFactor * 500_000) - 1; i > 1000; i--) {
          assertTrue(rs.next());
          assertEquals(rs.getLong("id"), (long) i);
          count++;
        }
        long end = System.nanoTime();
        registerResults("right outer join", end - begin, count);
      }
    }
  }



  public void testIdLookup() throws SQLException {
    long begin = System.nanoTime();
    int count = 0;
    for (int j = 0; j < 100; j++) {
      try (PreparedStatement stmt = conn.prepareStatement("select id, id2 from persons where id=1000")) {
        for (int i = 0; i < outerFactor * 10_000; i++) {
          try (ResultSet rs = stmt.executeQuery()) {
            assertTrue(rs.next());
            assertEquals(rs.getLong("id"), 1_000L);
            count++;
          }
        }
      }
    }
    long end = System.nanoTime();
    registerResults("identity id=", end - begin, count);
      //assertTrue((end - begin) < (3300 * 1_000_000L), String.valueOf(end-begin));
  }

  public void testMath() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("select id, id5 from persons where id >= 0 and id = id2 - 1000 order by id asc")) {
      long begin = System.nanoTime();
      int count = 0;
      try (ResultSet rs = stmt.executeQuery()) {
        for (int i = 0; i < outerFactor * 500_000; i++) {
          assertTrue(rs.next());
          assertEquals(rs.getLong("id"), (long) i);
          count++;
        }
        long end = System.nanoTime();
        registerResults("math", end - begin, count);
      }
    }
  }

  public void testIdNoKey() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("select * from employee where socialsecurityNumber='" + padNumericString(9 ) + "'")) {
      long begin = System.nanoTime();
      int count = 0;
      for (int i = 0; i < outerFactor * 10_000; i++) {
        try (ResultSet rs = stmt.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(rs.getString("socialsecuritynumber"), "000009");
          count++;
        }
      }
      long end = System.nanoTime();
      registerResults("identity secondary key id=", end - begin, count);
    }
  }

  public void testRangeNoKey() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("select * from employee where socialsecurityNumber>='" + padNumericString(1000) + "'")) {
      long begin = System.nanoTime();
      try (ResultSet rs = stmt.executeQuery()) {
        int count = 0;
        for (int i = 1_000; i < outerFactor * 500_000; i++) {
          assertTrue(rs.next());
          assertEquals(rs.getString("socialsecuritynumber"), padNumericString(i));
          count++;
        }
        assertFalse(rs.next());
        long end = System.nanoTime();
        registerResults("range secondary key", end - begin, count);
      }
    }
  }


  public void testRangeThreeKey() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("select * from residence where id>1000 and id2>2000 and id3>3000")) {
      long begin = System.nanoTime();
      try (ResultSet rs = stmt.executeQuery()) {
        int count = 0;
        for (int i = 1_001; i < outerFactor * 500_000; i++) {
          assertTrue(rs.next());
          assertEquals(rs.getLong("id"), (long) i);
          assertEquals(rs.getLong("id2"), i + 1000L);
          assertEquals(rs.getLong("id3"), i + 2000L);
          count++;
        }
        assertFalse(rs.next());
        long end = System.nanoTime();
        registerResults("range three key", end - begin, count);
      }
    }
  }


  public void testRangeThreeKeyBackwards() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("select * from residence where id3>3000 and id2>2000 and id>1000")) {
      long begin = System.nanoTime();
      try (ResultSet rs = stmt.executeQuery()) {
        int count = 0;
        for (int i = 1_001; i < outerFactor * 500_000; i++) {
          assertTrue(rs.next());
          assertEquals(rs.getLong("id"), (long) i);
          assertEquals(rs.getLong("id2"), i + 1000L);
          assertEquals(rs.getLong("id3"), i + 2000L);
          count++;
        }
        assertFalse(rs.next());
        long end = System.nanoTime();
        registerResults("range secondary key backwards", end - begin, count);
      }
    }
  }


  public void testRangeThreeKeyMixed() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("select * from residence where id3<" + (int)(outerFactor * 110000) +
        " and id2>4000 and id>1000")) {
      long begin = System.nanoTime();
      try (ResultSet rs = stmt.executeQuery()) {
        int count = 0;
        for (int i = 3_001; i < outerFactor * 110_000 - 2_000; i++) {
          assertTrue(rs.next());
          assertEquals(rs.getLong("id"), (long) i);
          assertEquals(rs.getLong("id2"), i + 1000L);
          assertEquals(rs.getLong("id3"), i + 2000L);
          count++;
        }
        assertFalse(rs.next());
        System.out.println("range secondary key mixed - " + count);
        long end = System.nanoTime();
        registerResults("range secondary key mixed", end - begin, count);
      }
    }
  }


  public void testRangeThreeKeySingle() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("select * from residence where id>1000")) {
      long begin = System.nanoTime();
      try (ResultSet rs = stmt.executeQuery()) {
        int count = 0;
        for (int i = 1_001; i < outerFactor * 500_000; i++) {
          assertTrue(rs.next());
          assertEquals(rs.getLong("id"), (long) i);
          assertEquals(rs.getLong("id2"), i + 1000L);
          assertEquals(rs.getLong("id3"), i + 2000L);
          count++;
        }
        assertFalse(rs.next());
        long end = System.nanoTime();
        registerResults("range secondary key SINGLE", end - begin, count);
      }
    }
  }


  public void testNoKeyTwoKeyGreaterEqual() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("select * from employee where socialsecurityNumber>='001000' and socialsecurityNumber<'" + padNumericString((int)(outerFactor * 95000)) + "' order by socialsecuritynumber desc")) {
      long begin = System.nanoTime();
      try (ResultSet rs = stmt.executeQuery()) {
        int count = 0;
        for (int i = (int) (outerFactor * 95_000) - 1; i >= 1_000; i--) {
          assertTrue(rs.next(), String.valueOf(i));
          assertEquals(rs.getString("socialsecuritynumber"), padNumericString(i));
          count++;
        }
        assertFalse(rs.next());
        long end = System.nanoTime();
        registerResults("range two key greater equal2", end - begin, count);
      }
    }
  }


  public void testNoKeyTwoKeyGreater() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("select * from employee where socialsecurityNumber>'001000' and socialsecurityNumber<'" + padNumericString((int)(outerFactor * 95000)) + "' order by socialsecuritynumber desc")) {
      long begin = System.nanoTime();
      try (ResultSet rs = stmt.executeQuery()) {
        int count = 0;
        for (int i = (int) (outerFactor * 95_000) - 1; i > 1_000; i--) {
          assertTrue(rs.next(), String.valueOf(i));
          assertEquals(rs.getString("socialsecuritynumber"), padNumericString(i));
          count++;
        }
        assertFalse(rs.next());
        long end = System.nanoTime();
        registerResults("range two key greater", end - begin, count);
      }
    }
  }


  public void testNoKeyTwoKeyGreaterLeftSided() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("select * from employee where socialsecurityNumber>'" + padNumericString(1000) +
        "' and (socialsecurityNumber>'" + padNumericString(3000) + "' and socialsecurityNumber<'" + padNumericString((int)(outerFactor * 95000)) +
        "') order by socialsecuritynumber desc")) {
      long begin = System.nanoTime();
      try (ResultSet rs = stmt.executeQuery()) {
        int count = 0;
        for (int i = (int) (outerFactor * 95_000) - 1; i > 3_000; i--) {
          assertTrue(rs.next(), String.valueOf(i));
          assertEquals(rs.getString("socialsecuritynumber"), padNumericString(i));
          count++;
        }
        assertFalse(rs.next());
        long end = System.nanoTime();
        registerResults("range two key greater left sided", end - begin, count);
      }
    }
  }


  public void notIn() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("select * from persons where id < " + (int)(outerFactor * 100000) + " and id > 10 and id not in (5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19) order by id desc")) {
      long begin = System.nanoTime();
      try (ResultSet rs = stmt.executeQuery()) {
        int count = 0;
        for (int i = (int) (outerFactor * 100_000) - 1; i > 19; i--) {
          assertTrue(rs.next(), String.valueOf(i));
          assertEquals(rs.getLong("id"), (long) i);
          count++;
        }
        assertFalse(rs.next());
        long end = System.nanoTime();
        registerResults("not in", end - begin, count);
      }
    }
  }


  public void notInSecondary() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("select * from persons where id2 < " + ((int)(outerFactor * 100000) + 1000) + " and id2 > 1010 and id2 not in (1005, 1006, 1007, 1008, 1009, 1010, 1011, 1012, 1013, 1014, 1015, 1016, 1017, 1018, 1019) order by id2 desc")) {
      long begin = System.nanoTime();
      try (ResultSet rs = stmt.executeQuery()) {
        int count = 0;
        for (int i = (int) (outerFactor * 100000) + 999; i > 1019; i--) {
          assertTrue(rs.next());
          assertEquals(rs.getLong("id2"), (long) i);
          count++;
        }
        assertFalse(rs.next());
        long end = System.nanoTime();
        registerResults("range not in secondary", end - begin, count);
      }
    }
  }


  public void notInTableScan() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("select * from persons where socialsecuritynumber not in ('" +
        padNumericString(0) + "') order by id2 asc")) {
      long begin = System.nanoTime();
      try (ResultSet rs = stmt.executeQuery()) {
        int count = 0;
        for (int i = 1001; i < (outerFactor * 500_000) + 1000; i++) {
          assertTrue(rs.next());
          assertEquals(rs.getLong("id2"), (long) i);
          count++;
        }
        assertFalse(rs.next());
        long end = System.nanoTime();
        registerResults("range not in table scan", end - begin, count);
      }
    }
  }


  public void test2keyRange() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("select * from persons where (id < " + (int)(outerFactor * 92251) + ")  and (id > 1000)")) {
      long begin = System.nanoTime();
      try (ResultSet rs = stmt.executeQuery()) {
        int count = 0;
        for (int i = 1001; i < (int) (outerFactor * 92251); i++) {
          assertTrue(rs.next());
          assertEquals(rs.getLong("id"), (long) i);
          count++;
        }
        assertFalse(rs.next());
        long end = System.nanoTime();
        registerResults("range two key", end - begin, count);
      }
    }
  }


  public void testSecondaryKey() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("select * from persons where id2>1000")) {
      long begin = System.nanoTime();
      try (ResultSet rs = stmt.executeQuery()) {
        int count = 0;
        for (int i = 1001; i < (outerFactor * 500_000) + 1000; i++) {
          assertTrue(rs.next());
          assertEquals(rs.getLong("id2"), (long) i);
          count++;
        }
        assertFalse(rs.next());
        long end = System.nanoTime();
        registerResults("range two key seocndyary key", end - begin, count);
      }
    }
  }


  public void testTableScan() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("select * from persons where socialsecuritynumber>'" + padNumericString(0) + "'")) {
      long begin = System.nanoTime();
      try (ResultSet rs = stmt.executeQuery()) {
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
        registerResults("table scan", end - begin, count);
      }
    }
  }


  public void testTwoKey() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("select * from persons where id>=1000 and id<=" + (int)(outerFactor * 95000))) {
      long begin = System.nanoTime();
      try (ResultSet rs = stmt.executeQuery()) {
        int count = 0;
        for (int i = 1000; i <= outerFactor * 95_000; i++) {
          assertTrue(rs.next());
          assertEquals(rs.getLong("id"), (long) i);
          count++;
        }
        assertFalse(rs.next());
        long end = System.nanoTime();
        registerResults("range two key2", end - begin, count);
      }
    }
  }


  public void testTwoKeyRightSided() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("select * from persons where (id >= 3501 and id < " + (int)(outerFactor * 94751) + ") and (id > 1000)")) {
      long begin = System.nanoTime();
      try (ResultSet rs = stmt.executeQuery()) {
        int count = 0;
        for (int i = 3501; i < (int) (outerFactor * 94_751); i++) {
          assertTrue(rs.next());
          assertEquals(rs.getLong("id"), (long) i);
          count++;
        }
        assertFalse(rs.next());
        logger.info("range two key right sided - {}", count);
        long end = System.nanoTime();
        registerResults("range two key right sided", end - begin, count);
      }
    }
  }


  public void testTwoKeyLeftSidedGreater() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("select * from persons where (id > 1000) and (id >= 3501 and id < " + (int)(outerFactor * 94751) + ")")) {
      long begin = System.nanoTime();
      try (ResultSet rs = stmt.executeQuery()) {
        int count = 0;
        for (int i = 3501; i < (int) (outerFactor * 94_751); i++) {
          assertTrue(rs.next(), String.valueOf(i));
          assertEquals(rs.getLong("id"), (long) i);
          count++;
        }
        assertFalse(rs.next());
        long end = System.nanoTime();
        registerResults("range two key left sided greater", end - begin, count);
      }
    }
  }


  public void testTwoKeyGreater() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("select * from persons where id>1000 and id<" + (int)(outerFactor * 95000))) {
      long begin = System.nanoTime();
      try (ResultSet rs = stmt.executeQuery()) {
        int count = 0;
        for (int i = 1001; i < outerFactor * 95_000; i++) {
          assertTrue(rs.next());
          assertEquals(rs.getLong("id"), (long) i);
          count++;
        }
        assertFalse(rs.next());
        long end = System.nanoTime();
        registerResults("range two key greater2", end - begin, count);
      }
    }
  }


  public void testTwoKeyGreaterBackwards() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("select * from persons where id<" + (int)(outerFactor * 95000) + " and id>1000")) {
      long begin = System.nanoTime();
      try (ResultSet rs = stmt.executeQuery()) {
        int count = 0;
        for (int i = 1001; i < outerFactor * 95_000; i++) {
          assertTrue(rs.next());
          assertEquals(rs.getLong("id"), (long) i);
          count++;
        }
        assertFalse(rs.next());
        long end = System.nanoTime();
        registerResults("range two key greater backwards", end - begin, count);
      }
    }
  }



  public void testTwoKeyLeftSidedGreaterEqual() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("select * from persons where (id >= 1000) and (id >= 3501 and id < " +
        (int)(outerFactor * 94751) + ")")) {
      long begin = System.nanoTime();
      try (ResultSet rs = stmt.executeQuery()) {
        int count = 0;
        for (int i = 3501; i < (int) (outerFactor * 94_751); i++) {
          assertTrue(rs.next(), String.valueOf(i));
          assertEquals(rs.getLong("id"), (long) i);
          count++;
        }
        assertFalse(rs.next());
        long end = System.nanoTime();
        registerResults("range two key left sided greater equal", end - begin, count);
      }
    }
  }


  public void testCountTwoKeyGreaterEqual() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("select count(*) from persons where id>=1000 and id<=5000")) {
      long begin = System.nanoTime();
      int count = 0;
      for (int i = 0; i < (int) (outerFactor * 1020); i++) {
        try (ResultSet rs = stmt.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(rs.getLong(1), 4001L);
          count++;
        }
      }
      long end = System.nanoTime();
      registerResults("count range two key greater equal", end - begin, count);
    }
  }


  public void testMaxWhere() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("select max(id) as maxValue from persons where id<=25000")) {
      long begin = System.nanoTime();
      int count = 0;
      for (int i = 0; i < outerFactor * 10; i++) {
        try (ResultSet rs = stmt.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(rs.getLong("maxValue"), 25_000L);
          count++;
        }
      }
      long end = System.nanoTime();
      registerResults("max where", end - begin, count);
    }
  }


  public void testMax() throws SQLException {
    long begin = System.nanoTime();
    int count = 0;
    try (PreparedStatement stmt = conn.prepareStatement("select max(id) as maxValue from persons")) {
      for (int i = 0; i < outerFactor * 10000; i++) {
        try (ResultSet rs = stmt.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(rs.getLong("maxValue"), (long) ((outerFactor * 500_000) - 1L));
          assertEquals(rs.getInt("maxValue"), (int) (long) ((outerFactor * 500_000) - 1L));
          assertEquals(rs.getString("maxValue"), String.valueOf((long) ((outerFactor * 500_000) - 1L)));
          assertEquals(rs.getDouble("maxValue"), (double) (long) ((outerFactor * 500_000) - 1L));
          assertEquals(rs.getFloat("maxValue"), (float) (long) ((outerFactor * 500_000) - 1L));
          count++;
        }
      }
      long end = System.nanoTime();
      registerResults("max", end - begin, count);
    }
  }

  public void testMin() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("select min(id) as minValue from persons")) {
      long begin = System.nanoTime();
      int count = 0;
      for (int i = 0; i < outerFactor * 10000; i++) {
        try (ResultSet rs = stmt.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(rs.getLong(MIN_VALUE_STR), (long) 0);
          assertEquals(rs.getInt(MIN_VALUE_STR), (int) 0);
          assertEquals(rs.getString(MIN_VALUE_STR), String.valueOf(0));
          assertEquals(rs.getDouble(MIN_VALUE_STR), (double) 0);
          assertEquals(rs.getFloat(MIN_VALUE_STR), (float) 0);
          count++;
        }
      }
      long end = System.nanoTime();
      registerResults("min", end - begin, count);
    }
  }

  public void testCount() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("select count(*)  from persons")) {
      long begin = System.nanoTime();
      int count = 0;
      for (int i = 0; i < outerFactor * 1020; i++) {
        try (ResultSet rs = stmt.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(rs.getLong(1), (long) (outerFactor * 500_000L));
          assertEquals(rs.getInt(1), (int) (long) (outerFactor * 500_000L));
          assertEquals(rs.getString(1), String.valueOf((long) (outerFactor * 500_000L)));
          assertEquals(rs.getDouble(1), (double) (long) (outerFactor * 500_000L));
          assertEquals(rs.getFloat(1), (float) (long) (outerFactor * 500_000L));
          count++;
        }
      }
      long end = System.nanoTime();
      registerResults("count", end - begin, count);
    }
  }


  public void testSort() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("select * from persons where id>=1000 order by id desc")) {
      long begin = System.nanoTime();
      try (ResultSet rs = stmt.executeQuery()) {
        int count = 0;
        for (int i = (int) (outerFactor * 500_000) - 1; i >= 1000; i--) {
          assertTrue(rs.next());
          String leading = padNumericString(i);
          assertEquals(rs.getString("socialsecuritynumber"), leading);
          count++;
        }
        long end = System.nanoTime();
        registerResults("range sort", end - begin, count);
      }
    }
  }


  public void testSortDisk() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("select * from persons where id>=1000 order by socialsecuritynumber desc")) {
      long begin = System.nanoTime();
      try (ResultSet rs = stmt.executeQuery()) {
        int count = 0;
        for (int i = (int) (outerFactor * 500_000) - 1; i >= 1000; i--) {
          assertTrue(rs.next(), String.valueOf(i));
          String leading = padNumericString(i);
          assertEquals(rs.getString("socialsecuritynumber"), leading);
          count++;
        }
        long end = System.nanoTime();
        registerResults("range sort disk", end - begin, count);
      }
    }
  }


  public void testId2() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("select * from persons where id2=2000")) {
      long begin = System.nanoTime();
      int count = 0;
      for (int i = 0; i < outerFactor * 10_000; i++) {
        try (ResultSet rs = stmt.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(rs.getLong("id2"), 2_000L);
          count++;
        }
      }
      long end = System.nanoTime();
      registerResults("id2", end - begin, count);
    }
  }


  public void testId2Range() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("select * from persons where id2>2000")) {
      long begin = System.nanoTime();
      try (ResultSet rs = stmt.executeQuery()) {
        int count = 0;
        for (int i = 2001; i < (outerFactor * 500_000) + 1000; i++) {
          assertTrue(rs.next());
          assertEquals(rs.getLong("id2"), (long) i);
          count++;
        }
        assertFalse(rs.next());
        long end = System.nanoTime();
        registerResults("range id2", end - begin, count);
      }
    }
  }


  public void testOtherExpression() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("select * from persons where id=1000 and id2 < 10000 and id2 > 1000")) {
      long begin = System.nanoTime();
      int count = 0;
      for (int i = 0; i < outerFactor * 10_000; i++) {
        try (ResultSet rs = stmt.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(rs.getLong("id"), 1_000L);
          count++;
        }
      }
      long end = System.nanoTime();
      registerResults("range other expression", end - begin, count);
    }
  }


  public void testRange() throws SQLException {
    for (int j = 0; j < 10; j++) {
      int max = 0;
      try (PreparedStatement stmt = conn.prepareStatement("select * from persons where id>=100")) {
        try (ResultSet rs = stmt.executeQuery()) {
          for (int i = 100; i < outerFactor * 500_000; i++) {
            assertTrue(rs.next());
            assertEquals(rs.getLong("id"), (long) i);
            max++;
          }
          logger.info(MAX_STR, max);
          assertFalse(rs.next());
        }
      }
    }
    int count = 0;
    long begin = System.nanoTime();
    for (int j = 0; j < 10; j++) {
      int max = 0;
      try (PreparedStatement stmt = conn.prepareStatement("select * from persons where id>=100")) {
        try (ResultSet rs = stmt.executeQuery()) {
          for (int i = 100; i < outerFactor * 500_000; i++) {
            assertTrue(rs.next());
            assertEquals(rs.getLong("id"), (long) i);
            count++;
            max++;
          }
          assertFalse(rs.next());
        }
      }
    }
    long end = System.nanoTime();
    registerResults("range", end-begin, count);
  }

  public void testRangeGreaterDescend() throws SQLException {
    for (int j = 0; j < 10; j++) {
      int max = 0;
      try (PreparedStatement stmt = conn.prepareStatement("select * from persons where id>=100 order by id desc")) {
        try (ResultSet rs = stmt.executeQuery()) {
          for (int i = (int) (outerFactor * 500_000) - 1; i >= 100; i--) {
            try {

              assertTrue(rs.next());
              assertEquals(rs.getLong("id"), (long) i);
              max++;
            }
            catch (Exception e) {
              throw new DatabaseException(e);
            }
          }
          logger.info(MAX_STR, max);
          assertFalse(rs.next());
        }
      }
    }
    int count = 0;
    long begin = System.nanoTime();
    for (int j = 0; j < 10; j++) {
      int max = 0;
      try (PreparedStatement stmt = conn.prepareStatement("select * from persons where id>=100 order by id desc")) {
        try (ResultSet rs = stmt.executeQuery()) {
          for (int i = (int) (outerFactor * 500_000) - 1; i >= 100; i--) {
            assertTrue(rs.next());
            assertEquals(rs.getLong("id"), (long) i);
            count++;
            max++;
          }
          logger.info(MAX_STR, max);
          assertFalse(rs.next());
        }
      }
    }
    long end = System.nanoTime();
    registerResults("range greater descend", end-begin, count);
  }

  public void testRangeLess() throws SQLException {
    for (int j = 0; j < 10; j++) {
      int max = 0;
      try (PreparedStatement stmt = conn.prepareStatement("select * from persons where id< " + (int)(outerFactor * 490_000))) {
        try (ResultSet rs = stmt.executeQuery()) {
          for (int i = 0; i < outerFactor * 490_000; i++) {
            assertTrue(rs.next());
            assertEquals(rs.getLong("id"), (long) i);
            max++;
          }
          logger.info(MAX_STR, max);
          assertFalse(rs.next());
        }
      }
    }
    int count = 0;
    long begin = System.nanoTime();
    for (int j = 0; j < 10; j++) {
      int max = 0;
      try (PreparedStatement stmt = conn.prepareStatement("select * from persons where id< " + (int)(outerFactor * 490_000))) {
        try (ResultSet rs = stmt.executeQuery()) {
          for (int i = 0; i < outerFactor * 490_000; i++) {
            assertTrue(rs.next());
            assertEquals(rs.getLong("id"), (long) i);
            count++;
            max++;
          }
          logger.info(MAX_STR, max);
          assertFalse(rs.next());
        }
      }
    }
    long end = System.nanoTime();
    registerResults("range less", end-begin, count);
  }

  public void testRangeOtherExpression() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("select * from persons where id>1000 and id2 < " + (int)(outerFactor * 500000) + " and id2 > 1000")) {
      long begin = System.nanoTime();
      try (ResultSet rs = stmt.executeQuery()) {
        int count = 0;
        for (int i = 1001; i < (outerFactor * 500_000) - 1000; i++) {
          try {
            assertTrue(rs.next());
            assertEquals(rs.getLong("id"), (long) i);
            count++;
          }
          catch (Exception e) {
            throw new DatabaseException("i=" + i, e);
          }
        }
        assertFalse(rs.next());
        long end = System.nanoTime();
        registerResults("range other expression2", end - begin, count);
      }
    }
  }


  public void testSecondary() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("select * from persons where id2=2000")) {
      long begin = System.nanoTime();
      int count = 0;
      for (int i = 0; i < outerFactor * 10_000; i++) {
        try (ResultSet rs = stmt.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(rs.getLong("id2"), 2_000L);
          count++;
        }
      }
      long end = System.nanoTime();
      registerResults("id secdonary", end - begin, count);
    }
  }

  public void testNot() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("select * from persons where not(id > 100000 and id <= " + outerFactor * 500_000 + ")")) {
      long begin = System.nanoTime();
      int count = 0;
      try (ResultSet rs = stmt.executeQuery()) {
        for (int i = 0; i <= outerFactor * 100_000; i++) {
          assertTrue(rs.next());
          assertEquals(rs.getLong("id"), (long) i);
          count++;
        }
        long end = System.nanoTime();
        registerResults("not", end - begin, count);
      }
    }
  }

  public void testFunctionAvg() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("select * from persons where avg(id, id2) > " + outerFactor * 250_000)) {
      long begin = System.nanoTime();
      int count = 0;
      try (ResultSet rs = stmt.executeQuery()) {
        for (int i = (int) (outerFactor * 250_000) - 499; i < outerFactor * 500_000; i++) {
          try {
            assertTrue(rs.next(), String.valueOf(i));
            assertEquals(rs.getLong("id"), (long) i, String.valueOf(i));
            count++;
          }
          catch (Exception e) {
            logger.error("Error", e);
            throw e;
          }
        }
        long end = System.nanoTime();
        registerResults("function avg", end - begin, count);
      }
    }
  }

  public void testFunctionCustom() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("select * from persons where custom(CustomFunctions, 'min', id, id2) > " + 50_000)) {
      long begin = System.nanoTime();
      int count = 0;
      try (ResultSet rs = stmt.executeQuery()) {
        for (int i = 50_001; i < outerFactor * 500_000; i++) {
          try {
            assertTrue(rs.next(), String.valueOf(i));
            assertEquals(rs.getLong("id"), (long) i, String.valueOf(i));
            count++;
          }
          catch (Exception e) {
            logger.error("Error", e);
            throw e;
          }
        }
        long end = System.nanoTime();
        registerResults("function custom min", end - begin, count);
      }
    }
  }

  public void testFunctionMin() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("select * from persons where min(id, id2) > " + 50_000)) {
      long begin = System.nanoTime();
      int count = 0;
      try (ResultSet rs = stmt.executeQuery()) {
        for (int i = 50_001; i < outerFactor * 500_000; i++) {
          try {
            assertTrue(rs.next(), String.valueOf(i));
            assertEquals(rs.getLong("id"), (long) i, String.valueOf(i));
            count++;
          }
          catch (Exception e) {
            logger.error("Error", e);
            throw e;
          }
        }
        long end = System.nanoTime();
        registerResults("function min", end - begin, count);
      }
    }
  }

  public void testUnion() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("(select id from persons where id > 1000) union (select id from persons2 where id > 1000) order by id asc")) {
      long begin = System.nanoTime();
      int count = 0;
      try (ResultSet rs = stmt.executeQuery()) {
        for (int i = 0; i < outerFactor * 400_000; i++) {
          assertTrue(rs.next());
          try {
            assertEquals(rs.getLong("id"), i + 1001L);
          }
          catch (Exception e) {
            logger.error("Error: count={}", count);
            break;
          }
          count++;
        }
        long end = System.nanoTime();
        registerResults("union", end - begin, count);
      }
    }
  }

  public void testUnionInMemory() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("(select id from persons where id > 1000 and id < " + (int)(outerFactor * 31000) +
        ") union (select id from persons2 where id > 1000 and id < " + (int)(outerFactor * 31000) + ORDER_BY_ID_ASC_STR)) {
      long begin = System.nanoTime();
      int count = 0;
      try (ResultSet rs = stmt.executeQuery()) {
        for (int i = 1001; i < outerFactor * 31_000; i++) {
          assertTrue(rs.next());
          try {
            assertEquals(rs.getLong("id"), (long) i);
          }
          catch (Exception e) {
            logger.error("Error: count={}", count);
            break;
          }
          count++;
        }
        long end = System.nanoTime();
        registerResults("union inMemory", end - begin, count);
      }
    }
  }

  public void testUnionAll() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("(select id from persons where id > 1000) union all (select id from persons2 where id > 1000) order by id asc")) {
      long begin = System.nanoTime();
      int count = 0;
      try (ResultSet rs = stmt.executeQuery()) {
        for (int i = 1001; i < outerFactor * 500_000; i++) {
          assertTrue(rs.next());
          assertEquals(rs.getLong("id"), (long) i);
          count++;
        }
        long end = System.nanoTime();
        registerResults("union all", end - begin, count);
      }
    }
  }

  public void testIntersect() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("(select id from persons where id > 1000 and id < "+
        (int)(outerFactor * 100_000) + ") intersect (select id from persons2 where id > 1000) order by id asc")) {
      long begin = System.nanoTime();
      int count = 0;
      try (ResultSet rs = stmt.executeQuery()) {
        for (int i = 1001; i < outerFactor * 100_000; i++) {
          assertTrue(rs.next());
          assertEquals(rs.getLong("id"), (long) i);
          count++;
        }
        long end = System.nanoTime();
        registerResults("intersect", end - begin, count);
      }
    }
  }

  public void testExcept() throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("(select id from persons where id > 1000 and id < "+
        (int)(outerFactor * 500_000) + ") except (select id from persons2 where id > 1000 and id < " + (int)(outerFactor * 100_000) + ORDER_BY_ID_ASC_STR)) {
      long begin = System.nanoTime();
      int count = 0;
      try (ResultSet rs = stmt.executeQuery()) {
        for (int i = (int) (outerFactor * 100_000); i < (int) (outerFactor * 500_000); i++) {
          assertTrue(rs.next());
          assertEquals(rs.getLong("id"), (long) i);
          count++;
        }
        long end = System.nanoTime();
        registerResults("except", end - begin, count);
      }
    }
  }
}
