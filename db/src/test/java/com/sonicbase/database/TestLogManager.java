package com.sonicbase.database;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.server.DatabaseServer;
import com.sonicbase.util.JsonArray;
import com.sonicbase.util.JsonDict;
import com.sonicbase.util.StreamUtils;
import com.sonicbase.research.socket.NettyServer;
import org.codehaus.plexus.util.FileUtils;
import org.testng.annotations.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.testng.Assert.*;

/**
 * Responsible for
 */
public class TestLogManager {


  private Connection conn;

  @Test
  public void test() throws IOException, ExecutionException, InterruptedException, ClassNotFoundException, SQLException {
    String configStr = StreamUtils.inputStreamToString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-4-servers.json")));
     final JsonDict config = new JsonDict(configStr);

     JsonArray array = config.putArray("licenseKeys");
     array.add(DatabaseServer.FOUR_SERVER_LICENSE);

    FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db"));

    DatabaseServer.getServers().clear();

     final DatabaseServer[] dbServers = new DatabaseServer[4];
     ThreadPoolExecutor executor = new ThreadPoolExecutor(128, 128, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

     String role = "primaryMaster";

     List<Future> futures = new ArrayList<>();

    final NettyServer[] nettyServers = new NettyServer[dbServers.length];
    for (int i = 0; i < dbServers.length; i++) {
      nettyServers[i] = new NettyServer();
    }

     for (int i = 0; i < dbServers.length; i++) {
       final int shard = i;
       if (i == 1) {
         Thread.sleep(2000);
       }
       futures.add(executor.submit(new Callable() {
         @Override
         public Object call() throws Exception {
 //          String role = "primaryMaster";

       nettyServers[shard].startServer(new String[]{"-host", "localhost", "-port", String.valueOf(9010 + (50 * shard)), "-cluster", "4-servers"}, null, true);
//       dbServers[shard].setConfig(config, "4-servers", "localhost", 9010 + (50 * shard), true, new AtomicBoolean(true));
//       dbServers[shard].setRole(role);
//       dbServers[shard].disableLogProcessor();
 //          return null;
 //        }
           return null;
       }}));
     }
     for (int i = 0; i < dbServers.length; i++) {
       while (true) {
         if (nettyServers[i].isRunning()) {
           DatabaseServer server = nettyServers[i].getDatabaseServer();//new DatabaseServer();
           if (server != null) {
             server.enableSnapshot(false);
             dbServers[i] = server;
             if (dbServers[i].isRunning()) {
               break;
             }
           }
         }
         Thread.sleep(1000);
       }
     }
//     for (Future future : futures) {
//       future.get();
//     }


    Class.forName("com.sonicbase.jdbcdriver.Driver");

    conn = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9010", "user", "password");

    ((ConnectionProxy) conn).getDatabaseClient().createDatabase("test");

    conn.close();

    conn = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9010/test", "user", "password");

    DatabaseClient client = ((ConnectionProxy) conn).getDatabaseClient();

    PreparedStatement stmt = conn.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
    stmt.executeUpdate();

    futures = new ArrayList<>();
    for (int i = 0; i < 100000; i++) {
      final int offset =i;
      futures.add(executor.submit(new Callable(){
        @Override
        public Object call() throws Exception {
          PreparedStatement stmt2 = conn.prepareStatement("insert into persons (id, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?)");
          stmt2.setLong(1, offset);
          stmt2.setString(2, "933-28-" + offset);
          stmt2.setString(3, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
          stmt2.setBoolean(4, false);
          stmt2.setString(5, "m");
          assertEquals(stmt2.executeUpdate(), 1);
          return null;
        }
      }));
    }
    for (Future future : futures) {
      future.get();
    }

    stmt = conn.prepareStatement("select id, id2 from persons where id>=0 order by id asc");
    ResultSet ret = stmt.executeQuery();

     for (int i = 0; i < 100000; i++) {
       assertTrue(ret.next());
       assertEquals(ret.getInt("id"), i);
     }

    for (DatabaseServer server : dbServers) {
      server.truncateTablesQuietly();
    }

    try {
      //((ConnectionProxy) conn).getDatabaseClient().createDatabase("test");

      stmt = conn.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
      stmt.executeUpdate();
    }
    catch (Exception e){

    }

    for (DatabaseServer server : dbServers) {
      server.replayLogs();
    }

     //test select returns multiple records with an index using operator '<'
     stmt = conn.prepareStatement("select id, id2 from persons where id>=0 order by id asc");
    ret = stmt.executeQuery();

      for (int i = 0; i < 100000; i++) {
        assertTrue(ret.next(), "count=" + i);
        assertEquals(ret.getInt("id"), i);
      }
  }
}
