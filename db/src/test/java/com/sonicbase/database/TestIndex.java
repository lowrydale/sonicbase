package com.sonicbase.database;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.index.Index;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.Schema;
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
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

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

        JsonArray array = config.getDict("database").putArray("licenseKeys");
        array.add(DatabaseServer.FOUR_SERVER_LICENSE);

        FileUtils.deleteDirectory(new File("/data/database"));

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
    Map.Entry<Object[], Long>[] ret = new Map.Entry[3];
    index.higherEntries(new Object[]{2, "2".getBytes()}, ret);
    assertEquals(ret[0].getKey()[0], 2);
    assertEquals(new String((byte[])ret[0].getKey()[1]), "3");
    assertEquals(ret[1].getKey()[0], 2);
    assertEquals(new String((byte[])ret[1].getKey()[1]), "4");
    assertEquals(ret[2].getKey()[0], 2);
    assertEquals(new String((byte[])ret[2].getKey()[1]), "5");

    Map.Entry<Object[], Long> curr = index.higherEntry(new Object[]{3});
    assertEquals(curr.getKey()[0], 3);
    assertEquals(new String((byte[])curr.getKey()[1]), "0");

    curr = index.higherEntry(new Object[]{3, "3".getBytes()});
    assertEquals(curr.getKey()[0], 4);

    curr = index.floorEntry(new Object[]{3, "3".getBytes()});
    assertEquals(curr.getKey()[0], 3);

    curr = index.lastEntry();
    assertEquals(curr.getKey()[0], 9);

    curr = index.firstEntry();
    assertEquals(curr.getKey()[0], 0);

    index.lowerEntries(new Object[]{5, "5".getBytes()}, ret);
    assertEquals(ret[0].getKey()[0], 4);
    assertEquals(ret[1].getKey()[0], 3);
    assertEquals(ret[2].getKey()[0], 2);

    curr = index.lowerEntry(new Object[]{3, "3".getBytes()});
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
    Map.Entry<Object[], Long>[] ret = new Map.Entry[3];
    index.higherEntries(new Object[]{(long)2}, ret);
    assertEquals(ret[0].getKey()[0], (long)3);
    assertEquals(ret[1].getKey()[0], (long)4);
    assertEquals(ret[2].getKey()[0], (long)5);

    Map.Entry<Object[], Long> curr = index.higherEntry(new Object[]{(long)3});
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
}
