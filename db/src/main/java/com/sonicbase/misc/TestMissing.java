package com.sonicbase.misc;

import java.sql.*;

public class TestMissing {

  public static void main(String[] args) throws ClassNotFoundException, SQLException {
    Class.forName("com.sonicbase.jdbcdriver.Driver");
    Connection conn = DriverManager.getConnection("jdbc:sonicbase:" + args[0] + ":9010/db", "user", "password");

    if (args[1].equalsIgnoreCase("load")) {
      load(conn, args);
    }
    else if (args[1].equalsIgnoreCase("validate")) {
      validate(conn, args);
    }
  }

  private static void validate(Connection conn, String[] args) throws SQLException {
    int group = Integer.valueOf(args[2]);
    int count = (group + 1) * 10_000_000;
    PreparedStatement stmt = conn.prepareStatement("select * from persons where id1 >= 0 and id1 < " + count);
    ResultSet rs = stmt.executeQuery();
    int offset = 0;
    int errorCount = 0;
    long begin = System.currentTimeMillis();
    while (rs.next()) {
      if (rs.getLong("id1") != offset) {
        System.out.println("validate mismatch: id=" + rs.getLong("id1") + ", expected=" + offset);
//        if (errorCount++ > 100) {
//          break;
//        }
        offset = (int)rs.getLong("id1");
      }
      if (offset++ % 100_000 == 0) {
        int currCount = (offset - (group * 10_000_000));
        System.out.println("load progress: count=" + currCount + ", rate=" + (currCount / (float)(System.currentTimeMillis() - begin) * 1000f));
      }
    }
  }

  private static void load(Connection conn, String[] args) throws SQLException {
    int group = Integer.valueOf(args[2]);
    long begin = System.currentTimeMillis();
    for (int offset = group * 10_000_000; offset < (group + 1) * 10_000_000;) {
      PreparedStatement stmt = conn.prepareStatement("insert into persons (id1, id2, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?)");
      for (int i = 0; i < 200; i++) {
        stmt.setLong(1, offset);
        stmt.setLong(2, (offset + 100) % 2);
        stmt.setString(3, "933-28-" + (offset + 1));
        stmt.setString(4, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
        stmt.setBoolean(5, false);
        stmt.setString(6, "m");
        if (offset++ % 100_000 == 0) {
          int count = (offset - (group * 10_000_000));
          System.out.println("load progress: count=" + count + ", rate=" + (count / (float)(System.currentTimeMillis() - begin) * 1000f));
        }
        stmt.addBatch();
      }
      stmt.executeBatch();
    }
  }
}
