package com.sonicbase.misc;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Inserter {
  private static final Logger logger = LoggerFactory.getLogger(Inserter.class);
  private static AtomicLong totalDuration = new AtomicLong();

  public static void main(String[] args) throws ClassNotFoundException, PropertyVetoException {

    Class.forName("com.sonicbase.jdbcdriver.Driver");

    ComboPooledDataSource cpds = new ComboPooledDataSource();
    cpds.setDriverClass("com.sonicbase.jdbcdriver.Driver"); //loads the jdbc driver
    cpds.setJdbcUrl("jdbc:sonicbase:localhost:9010/db");

    cpds.setMinPoolSize(5);
    cpds.setAcquireIncrement(1);
    cpds.setMaxPoolSize(60);


    final long begin = System.currentTimeMillis();
    int batchSize = 200;
    final AtomicInteger countInserted = new AtomicInteger();
    List<Thread> threads = new ArrayList<>();
    final int threadCount = 32;
    for (int i = 0; i < threadCount; i++) {
      final int threadOffset = i;
      final AtomicLong lastLogged = new AtomicLong(System.currentTimeMillis());
      Thread insertThread = new Thread(() -> {
        int countOnThread = 0;
        while (true) {

          long offset1 = threadOffset * 100_000_000 + countOnThread;
          try {

            long thisDuration = 0;
            Connection conn = cpds.getConnection();
            try {
              PreparedStatement stmt = conn.prepareStatement("insert into persons (id1) VALUES (?)");
              for (int i1 = 0; i1 < batchSize; i1++) {
                    /*
                     create database db
                     create table Employers (id VARCHAR(64), name VARCHAR(256))
                     create index employerId on Employers(id)
                     create table Persons (id1 BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id1))
                     create table Memberships (personId BIGINT, personId2 BIGINT, membershipName VARCHAR(20), resortId BIGINT, PRIMARY KEY (personId, personId2))
                     create table Resorts (resortId BIGINT, resortName VARCHAR(20), PRIMARY KEY (resortId))
                     */
                stmt.setLong(1, offset1 + i1);
                stmt.addBatch();
                countOnThread++;
                countInserted.incrementAndGet();
              }
              long currBegin = System.nanoTime();
              stmt.executeBatch();
              thisDuration += System.nanoTime() - currBegin;
            }
            catch (Exception e) {
              e.printStackTrace();
            }
            finally {
              conn.close();
            }

            totalDuration.addAndGet(thisDuration);

            logProgress(threadOffset, countInserted, lastLogged, begin, totalDuration);
          }
          catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().contains("Unique constraint violated")) {
              System.out.println("Unique constraint violation");
            }
            else {
              e.printStackTrace();
            }
          }
          finally {
            logProgress(threadOffset, countInserted, lastLogged, begin, totalDuration);
          }
        }

      });
      insertThread.start();
      threads.add(insertThread);
    }

  }

  private static void logProgress(int threadOffset, AtomicInteger countInserted, AtomicLong lastLogged, long begin,
                                  AtomicLong totalDuration) {
    if (threadOffset == 0 && System.currentTimeMillis() - lastLogged.get() > 2000) {
      lastLogged.set(System.currentTimeMillis());
      StringBuilder builder = new StringBuilder();
      builder.append("count=").append(countInserted.get());
      builder.append(String.format(", rate=%.2f", countInserted.get() / (double) (System.currentTimeMillis() - begin) * 1000f)); //INSERT_STATS.getFiveMinuteRate()));
      builder.append(String.format(", avg=%.2f", (double)totalDuration.get() / (countInserted.get()) / 1000000d));//snapshot.getMean() / 1000000d));
      System.out.println(builder.toString());
    }
  }

}
