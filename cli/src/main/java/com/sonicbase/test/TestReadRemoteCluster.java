package com.sonicbase.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class TestReadRemoteCluster {
  private static final Logger logger = LoggerFactory.getLogger(TestReadRemoteCluster.class);

  public static void main(String[] args) throws ClassNotFoundException, SQLException {
    Class.forName("com.sonicbase.jdbcdriver.Driver");

    Connection conn = DriverManager.getConnection("jdbc:sonicbase:localhost:9010/db");

    while (true) {
      long begin = System.currentTimeMillis();
      AtomicLong readCount = new AtomicLong();
      AtomicInteger errorCount = new AtomicInteger();
      try (PreparedStatement stmt = conn.prepareStatement("select persons.id1 from persons where id1 >= 0 order by id1 asc")) {
        ResultSet ret = stmt.executeQuery();
        long currId = 0;
        while (true) {
          if (!ret.next()) {
            break;
          }
          long actualId = ret.getLong("id1");
          if (actualId != currId) {
            errorCount.incrementAndGet();
            System.out.println("error: expected=" + currId + ", actual=" + actualId);
          }
          currId = actualId + 1;

          if (readCount.incrementAndGet() % 1_000_000 == 0) {
            StringBuilder builder = new StringBuilder();
            builder.append("count=").append(readCount.get());
            builder.append(String.format(", rate=%.2f", readCount.get() / (double) (System.currentTimeMillis() - begin) * 1000f));
            builder.append(", errorCount=").append(errorCount.get());

            System.out.println(builder.toString());
          }
        }
        System.out.println("read count=" + readCount.get());
      }
    }
  }

}
