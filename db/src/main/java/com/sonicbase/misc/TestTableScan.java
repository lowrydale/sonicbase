/* © 2017 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.misc;

import com.sonicbase.util.JsonArray;
import com.sonicbase.util.JsonDict;
import com.sonicbase.util.StreamUtils;

import java.io.*;
import java.sql.*;

public class TestTableScan {

  public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {

    String cluster = args[0];
    boolean sort = Boolean.valueOf(args[1]);

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

    int count = 0;
    long begin = System.currentTimeMillis();
    PreparedStatement stmt = null;
    if (sort) {
      stmt = conn.prepareStatement("select * from persons where restricted=0 order by id1 asc");
    }
    else {
      stmt = conn.prepareStatement("select * from persons where restricted=0");
    }
    ResultSet rs = stmt.executeQuery();
    while (rs.next()) {
      if (count++ % 100000 == 0) {
        System.out.println("progress: count=" + count + ", rate=" + (double)count / (System.currentTimeMillis() - begin) * 1000d);
      }
    }

  }
}
