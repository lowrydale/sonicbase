package com.sonicbase.misc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
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
    String configStr = IOUtils.toString(new BufferedInputStream(new FileInputStream(file)), "utf-8");

    ObjectMapper mapper = new ObjectMapper();
    ObjectNode dict = (ObjectNode) mapper.readTree(configStr);
    ObjectNode databaseDict = dict;
    ArrayNode array = databaseDict.withArray("shards");
    ObjectNode replica = (ObjectNode) array.get(0);
    ArrayNode replicasArray = replica.withArray("replicas");
    String address = replicasArray.get(0).get("publicAddress").asText();
    if (databaseDict.get("clientIsPrivate").asBoolean()) {
      address = replicasArray.get(0).get("privateAddress").asText();
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
