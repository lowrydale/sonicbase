
package com.sonicbase.misc;

import com.sonicbase.util.JsonArray;
import com.sonicbase.util.JsonDict;
import com.sonicbase.util.StreamUtils;

import java.io.*;
import java.sql.*;

public class RecordValidator {

  public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
    String cluster = args[0];
    long max = Long.valueOf(args[1]);

    File file = new File(System.getProperty("user.dir"), "config/config-" + cluster + ".json");
    if (!file.exists()) {
      file = new File(System.getProperty("user.dir"), "db/src/main/resources/config/config-" + cluster + ".json");
      System.out.println("Loaded config resource dir");
    }
    else {
      System.out.println("Loaded config default dir");
    }
    String configStr = StreamUtils.inputStreamToString(new BufferedInputStream(new FileInputStream(file)));

    JsonDict dict = new JsonDict(configStr);
    JsonDict databaseDict = dict;
    JsonArray array = databaseDict.getArray("shards");
    JsonDict replica = array.getDict(0);
    JsonArray replicasArray = replica.getArray("replicas");
    String address = replicasArray.getDict(0).getString("publicAddress");
    if (databaseDict.getBoolean("clientIsPrivate")) {
      address = replicasArray.getDict(0).getString("privateAddress");
    }
    System.out.println("Using address: address=" + address);

    Class.forName("com.sonicbase.jdbcdriver.Driver");

    Connection conn = DriverManager.getConnection("jdbc:sonicbase:" + address + ":9010/db");

    PreparedStatement stmt = null;

    long begin = System.currentTimeMillis();
    stmt = conn.prepareStatement("select persons.id1, id2 from persons where id1 >= 0 order by id1 asc");                                              //
    ResultSet rs = stmt.executeQuery();
    for (long i = 0; i < max; i++) {
      if (!rs.next()) {
        System.out.println("ran out of records: id=" + i);
        break;
      }
      long id = rs.getLong("id1");
      if (id != i) {
        System.out.println("Id mismatch: id=" + id + ", i=" + i);
        i = id;
      }
      if (i % 100_000 == 0) {
        System.out.println("progress: count=" + i + ", rate=" + (double)i / (double)(System.currentTimeMillis() - begin) * 1000d +
          ", latency=" + (double)(System.currentTimeMillis() - begin) / (double)i);
      }
    }
    System.out.println("finished traversal");
  }
}
