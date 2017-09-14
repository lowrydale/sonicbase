package com.sonicbase.misc;

import com.sonicbase.util.JsonArray;
import com.sonicbase.util.JsonDict;
import com.sonicbase.util.StreamUtils;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class RecordLoader {

  public static void main(String[] args) throws SQLException, ClassNotFoundException, IOException {
    String cluster = args[0];

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

    PreparedStatement stmt = conn.prepareStatement("create table Persons (id1 BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id1))");
    stmt.executeUpdate();

    int id = 0;
    int batchSize = 100;
    long begin = System.currentTimeMillis();
    while (true) {
      stmt = conn.prepareStatement("insert into persons (id1, id2, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?)");
      for (int i = 0; i < batchSize; i++) {
        stmt.setLong(1, id);
        stmt.setLong(2, id % 2);
        stmt.setString(3, "933-28-" + (id + i + 1));
        stmt.setString(4, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
        stmt.setBoolean(5, false);
        stmt.setString(6, "m");

        stmt.addBatch();

        if (id++ % 100_000 == 0) {
          System.out.println("progress: count=" + id + ", rate=" + (double)id / (double)(System.currentTimeMillis() - begin) * 1000f +
            ", latency=" + ((double)System.currentTimeMillis() - begin) / (double)id);
        }
      }
      stmt.executeBatch();
    }
  }
}
