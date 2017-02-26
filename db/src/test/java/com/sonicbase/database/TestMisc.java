package com.sonicbase.database;

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
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.testng.Assert.assertEquals;

/**
 * Responsible for
 */
public class TestMisc {

  private Connection conn;

  @BeforeClass
  public void beforeClass() throws Exception {
    String configStr = StreamUtils.inputStreamToString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-4-servers.json")));
    final JsonDict config = new JsonDict(configStr);

    JsonArray array = config.getDict("database").putArray("licenseKeys");
    array.add(DatabaseServer.FOUR_SERVER_LICENSE);

    FileUtils.deleteDirectory(new File("/data/database"));

    DatabaseServer.getServers().clear();

    final DatabaseServer[] dbServers = new DatabaseServer[4];

    String role = "primaryMaster";

    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < dbServers.length; i++) {
      final int shard = i;
//      futures.add(executor.submit(new Callable() {
//        @Override
//        public Object call() throws Exception {
//          String role = "primaryMaster";

      dbServers[shard] = new DatabaseServer();
      dbServers[shard].setConfig(config, "4-servers", "localhost", 9010 + (50 * shard), true, new AtomicBoolean(true));
      dbServers[shard].setRole(role);
      dbServers[shard].disableLogProcessor();
//          return null;
//        }
//      }));
    }
    for (Future future : futures) {
      future.get();
    }

    for (DatabaseServer server : dbServers) {
      server.disableRepartitioner();
    }

    //DatabaseClient client = new DatabaseClient("localhost", 9010, true);

    Class.forName("com.sonicbase.jdbcdriver.Driver");

    conn = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9000", "user", "password");

    ((ConnectionProxy) conn).getDatabaseClient().createDatabase("test");

    conn.close();

    conn = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9000/test", "user", "password");

    PreparedStatement stmt = conn.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create table Memberships (id1 BIGINT, id2 BIGINT, resortId BIGINT, PRIMARY KEY (id1, id2))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create table Resorts (resortId BIGINT, resortName VARCHAR(20), PRIMARY KEY (resortId))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create table nokey (id BIGINT, id2 BIGINT)");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create index socialSecurityNumber on persons(socialSecurityNumber)");
     stmt.executeUpdate();


  }

  @Test
  public void testWhite() {
    String str = "s   a b  c ";
    String[] parts = str.split("\\s+");
    System.out.println("test");
  }

  @Test
  public void testMath() {
    double value = 17179869184d;
    value = value / 1024d / 1024d / 1024d;
    System.out.println(value);
  }

  @Test
  public void testIndex() {
    ConcurrentSkipListMap<Object[], Integer> index = new ConcurrentSkipListMap<>(new Comparator<Object[]>() {
      @Override
      public int compare(Object[] o1, Object[] o2) {
        for (int i = 0; i < o1.length; i++) {
          if (o1[i] == null) {
            continue;
          }
          if (o2[i] == null) {
            continue;
          }
          if ((int) o1[i] > (int) o2[i]) {
            return 1;
          }
          if ((int) o1[i] < (int) o2[i]) {
            return -1;
          }
        }
        return 0;
      }
    });

    index.put(new Object[]{1, 100}, 1);
    index.put(new Object[]{1, 101}, 1);
    index.put(new Object[]{1, 102}, 1);
    index.put(new Object[]{1, 103}, 1);
    index.put(new Object[]{1, 104}, 1);
    index.put(new Object[]{1, 105}, 1);

    index.put(new Object[]{2, 100}, 1);
    index.put(new Object[]{2, 101}, 1);
    index.put(new Object[]{2, 102}, 1);
    index.put(new Object[]{2, 103}, 1);
    index.put(new Object[]{2, 104}, 1);
    index.put(new Object[]{2, 105}, 1);

    index.put(new Object[]{3, 100}, 1);
    index.put(new Object[]{3, 101}, 1);
    index.put(new Object[]{3, 102}, 1);
    index.put(new Object[]{3, 103}, 1);
    index.put(new Object[]{3, 104}, 1);
    index.put(new Object[]{3, 105}, 1);

    Object[] key = index.firstKey();
    key = index.higherKey(new Object[]{1, null});
    assertEquals((int) key[0], 2);
    key = index.higherKey(new Object[]{2, null});
    assertEquals((int) key[0], 3);
    key = index.higherKey(new Object[]{3, 100});
    assertEquals((int) key[0], 3);
    assertEquals((int) key[1], 101);
  }


}
