package com.lowryengineering.database;

import com.lowryengineering.database.jdbcdriver.ConnectionProxy;
import com.lowryengineering.database.server.DatabaseServer;
import com.lowryengineering.database.util.JsonArray;
import com.lowryengineering.database.util.JsonDict;
import com.lowryengineering.database.util.StreamUtils;
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

    Class.forName("com.lowryengineering.database.jdbcdriver.DriverProxy");

    conn = DriverManager.getConnection("jdbc:dbproxy:127.0.0.1:9000", "user", "password");

    ((ConnectionProxy) conn).getDatabaseClient().createDatabase("test");

    conn.close();

    conn = DriverManager.getConnection("jdbc:dbproxy:127.0.0.1:9000/test", "user", "password");

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

  @Test
  public void testExplain() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("explain select * from persons where id < 100");
    ResultSet ret = stmt.executeQuery();

    while (ret.next()) {
      System.out.println(ret.getString(1));
    }
    System.out.println();

    stmt = conn.prepareStatement("explain select * from persons where id < 100 and id > 10");
    ret = stmt.executeQuery();

    while (ret.next()) {
      System.out.println(ret.getString(1));
    }
    System.out.println();

    stmt = conn.prepareStatement("explain select * from persons where id < 100 or id > 10");
    ret = stmt.executeQuery();

    while (ret.next()) {
      System.out.println(ret.getString(1));
    }
    System.out.println();

    stmt = conn.prepareStatement("explain select * from persons where id < 100 and id > 10 and id2 > 10");
    ret = stmt.executeQuery();

    while (ret.next()) {
      System.out.println(ret.getString(1));
    }
    System.out.println();

    stmt = conn.prepareStatement("explain select * from persons where id < 100 and id > 10 and id2 > 10 and id2 < 1");
    ret = stmt.executeQuery();

    while (ret.next()) {
      System.out.println(ret.getString(1));
    }
    System.out.println();

    stmt = conn.prepareStatement("explain select * from persons where id < 100 and id > 10 and id > 10 and id < 1");
    ret = stmt.executeQuery();

    while (ret.next()) {
      System.out.println(ret.getString(1));
    }
    System.out.println();

    stmt = conn.prepareStatement("explain select * from persons where id2 < 100");
    ret = stmt.executeQuery();

    while (ret.next()) {
      System.out.println(ret.getString(1));
    }
    System.out.println();

    stmt = conn.prepareStatement("explain select id1, id2 from memberships where id1=1 and id2=2");
    ret = stmt.executeQuery();

    while (ret.next()) {
      System.out.println(ret.getString(1));
    }
    System.out.println();


    stmt = conn.prepareStatement("explain select max(id) as maxValue from persons");
    ret = stmt.executeQuery();

    while (ret.next()) {
      System.out.println(ret.getString(1));
    }
    System.out.println();

    stmt = conn.prepareStatement("explain select max(id) as maxValue from persons where id2 < 1");
    ret = stmt.executeQuery();

    while (ret.next()) {
      System.out.println(ret.getString(1));
    }
    System.out.println();

    stmt = conn.prepareStatement("explain select max(id) as maxValue from persons where id < 100");
    ret = stmt.executeQuery();

    while (ret.next()) {
      System.out.println(ret.getString(1));
    }
    System.out.println();

    stmt = conn.prepareStatement("explain select sum(id) as sumValue from persons");
    ret = stmt.executeQuery();

    while (ret.next()) {
      System.out.println(ret.getString(1));
    }
    System.out.println();

    stmt = conn.prepareStatement("explain select * from persons where id < 100 and id > 200 limit 3");
    ret = stmt.executeQuery();

    while (ret.next()) {
      System.out.println(ret.getString(1));
    }
    System.out.println();

    stmt = conn.prepareStatement("explain select * from persons where id < 100 and id > 200 limit 3 offset 2");
    ret = stmt.executeQuery();

    while (ret.next()) {
      System.out.println(ret.getString(1));
    }
    System.out.println();

    stmt = conn.prepareStatement("explain select * from persons order by id2 asc, id desc");
    ret = stmt.executeQuery();

    while (ret.next()) {
      System.out.println(ret.getString(1));
    }
    System.out.println();

    stmt = conn.prepareStatement("explain select persons.id  from persons where persons.id>=100 AND id < 100AND ID2=0 OR id> 6 AND ID < 100");
    ret = stmt.executeQuery();

    while (ret.next()) {
      System.out.println(ret.getString(1));
    }
    System.out.println();

    stmt = conn.prepareStatement("explain select * from persons where id>105 and id2=0 or id<105 and id2=1 order by id desc");
    ret = stmt.executeQuery();

    while (ret.next()) {
      System.out.println(ret.getString(1));
    }
    System.out.println();

    stmt = conn.prepareStatement("explain select persons.id from persons where persons.id>2 AND id < 100 OR id> 6 AND ID < 200");
    ret = stmt.executeQuery();

    while (ret.next()) {
      System.out.println(ret.getString(1));
    }
    System.out.println();

    stmt = conn.prepareStatement("explain select * from persons where id2=1 order by id desc");
    ret = stmt.executeQuery();

    while (ret.next()) {
      System.out.println(ret.getString(1));
    }
    System.out.println();

    stmt = conn.prepareStatement("explain select * from persons where id in (0, 1, 2, 3, 4)");
    ret = stmt.executeQuery();

    while (ret.next()) {
      System.out.println(ret.getString(1));
    }
    System.out.println();

    stmt = conn.prepareStatement("explain select * from persons where socialSecurityNumber='555'");
    ret = stmt.executeQuery();

    while (ret.next()) {
      System.out.println(ret.getString(1));
    }
    System.out.println();

    stmt = conn.prepareStatement("explain select * from persons where id2=1 or id2=0 order by id2 asc, id desc");
    ret = stmt.executeQuery();

    while (ret.next()) {
      System.out.println(ret.getString(1));
    }
    System.out.println();

    stmt = conn.prepareStatement("explain select * from persons where id=0 OR id=1 OR id=2 OR id=3 OR id=4");
    ret = stmt.executeQuery();

    while (ret.next()) {
      System.out.println(ret.getString(1));
    }
    System.out.println();


    stmt = conn.prepareStatement("explain select persons.id, socialsecuritynumber, memberships.id  " +
        "from persons inner join Memberships on persons.id = Memberships.id inner join resorts on memberships.resortid = resorts.id" +
        " where persons.id<1000");
    ret = stmt.executeQuery();
    while (ret.next()) {
      System.out.println(ret.getString(1));
    }
    System.out.println();

    stmt = conn.prepareStatement(
        "explain select persons.id, persons.id2, persons.socialsecuritynumber, memberships.personId, memberships.personid2, memberships.membershipname from persons " +
            " inner join Memberships on persons.id = Memberships.PersonId and memberships.personid2 = persons.id2  where persons.id > 0 order by persons.id desc");
    ret = stmt.executeQuery();
    while (ret.next()) {
      System.out.println(ret.getString(1));
    }
    System.out.println();

    stmt = conn.prepareStatement(
        "explain select persons.id, persons.socialsecuritynumber, memberships.membershipname, memberships.personid from memberships " +
            "left outer join Memberships on persons.id = Memberships.PersonId where memberships.personid<1000 order by memberships.personid desc");
    ret = stmt.executeQuery();
    while (ret.next()) {
      System.out.println(ret.getString(1));
    }
    System.out.println();

    stmt = conn.prepareStatement(
        "explain select persons.id, persons.socialsecuritynumber, memberships.membershipname from persons " +
            " inner join Memberships on Memberships.PersonId = persons.id and memberships.personid < 1000 where persons.id > 0 order by persons.id desc");
    ret = stmt.executeQuery();
    while (ret.next()) {
      System.out.println(ret.getString(1));
    }
    System.out.println();

    stmt = conn.prepareStatement(
        "select persons.id, persons.socialsecuritynumber, memberships.personId, memberships.membershipname from persons " +
            "right outer join Memberships on persons.id = Memberships.PersonId where persons.id<1000 order by persons.id desc");
    ret = stmt.executeQuery();
    while (ret.next()) {
      System.out.println(ret.getString(1));
    }
    System.out.println();
  }
}
