package com.sonicbase.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicLong;

public class TestInsertRemoteCluster {
  private static final Logger logger = LoggerFactory.getLogger(TestInsertRemoteCluster.class);

  private static int batchSize = 500;
  public static void main(String[] args) throws ClassNotFoundException, SQLException {
    Class.forName("com.sonicbase.jdbcdriver.Driver");

    Connection conn = DriverManager.getConnection("jdbc:sonicbase:localhost:9010/db");

    AtomicLong countInserted = new AtomicLong();
    int offset = 0;
    long begin = System.currentTimeMillis();
    while (true) {
      try (PreparedStatement stmt = conn.prepareStatement("insert into persons (id1, id2, socialSecurityNumber, relatives, restricted, gender) VALUES (?, ?, ?, ?, ?, ?)")) {
        for (int i1 = 0; i1 < batchSize; i1++) {
          stmt.setLong(1, offset + i1);
          stmt.setLong(2, (offset + i1 + 100) % 2);
          stmt.setString(3, "933-28-" + (offset + i1 + 1));
          stmt.setString(4, "12345678901,12345678901|12345678901,12345678901,12345678901,12345678901|12345678901");
          stmt.setBoolean(5, false);
          stmt.setString(6, "m");
          stmt.addBatch();
        }
        stmt.executeBatch();

        offset += batchSize;

        if (offset % 100_000 == 0) {
          System.out.println("insert progress: count=" + offset + ", rate=" + (double)offset / ((double)System.currentTimeMillis() - begin) * 1000d);
        }

        countInserted.addAndGet(batchSize);
      }
    }
  }

}
