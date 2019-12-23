package com.sonicbase.bench;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class BenchmarkJoins {

  private static final String MILLIS_STR = " millis, ";
  private static final Logger logger = LoggerFactory.getLogger(BenchmarkJoins.class);
  private Thread mainThread;
  private boolean shutdown;

  final AtomicLong totalSelectDuration = new AtomicLong();
  final AtomicLong selectErrorCount = new AtomicLong();
  final AtomicLong selectBegin = new AtomicLong(System.currentTimeMillis());
  final AtomicLong selectOffset = new AtomicLong();
  final AtomicLong maxDuration = new AtomicLong();

  public void start(String address, final int shardCount, final Integer shard, final long count, final String queryType) {
    shutdown = false;
    selectBegin.set(System.currentTimeMillis());
    doResetStats();
    mainThread = new Thread(() -> {
      try {
        final AtomicInteger cycle = new AtomicInteger();
        final long startId = shard * count;

        logger.info("Using address: address={}", address);

        Class.forName("com.sonicbase.jdbcdriver.Driver");

        final java.sql.Connection conn = DriverManager.getConnection("jdbc:sonicbase:" + address + ":9010/db", "user", "password");

        //test insert
        final AtomicLong selectBegin = new AtomicLong(System.currentTimeMillis());
        final AtomicLong maxDuration = new AtomicLong();

        Thread[] threads = new Thread[1];
        for (int i = 0; i < threads.length; i++) {
          threads[i] = new Thread(() -> {
            while (!shutdown) {
              try {
                  long actualStartId = cycle.get() == 0 ? startId : 0;
                  cycle.incrementAndGet();
                  long beginSelect = System.nanoTime();
                  ResultSet ret = null;
                  PreparedStatement stmt = null;
                  if (queryType.equals("3wayInner")) {
                    stmt = conn.prepareStatement("select persons.id1, socialsecuritynumber, memberships.personId2  " +
                        "from persons inner join Memberships on persons.id1 = Memberships.personId1 inner join resorts on memberships.resortid = resorts.id" +
                        " where persons.id>" + actualStartId);

                    ret = stmt.executeQuery();
                  }
                  else if (queryType.equals("2wayInner")) {
                    //select persons.id1, persons.id2, persons.socialsecuritynumber, memberships.personId, memberships.personid2, memberships.membershipname from persons inner join Memberships on persons.id1 = Memberships.PersonId where persons.id1 > 0 order by persons.id1 desc
                    stmt = conn.prepareStatement(
                        "select persons.id1, persons.id2, persons.socialsecuritynumber, memberships.personId, memberships.personid2, memberships.membershipname from persons " +
                            " inner join Memberships on persons.id1 = Memberships.PersonId where persons.id1 > " + actualStartId + " order by persons.id1 asc");
                    ret = stmt.executeQuery();
                  }
                  else if (queryType.equals("2wayLeftOuter")) {
                    stmt = conn.prepareStatement(
                        "select persons.id1, persons.socialsecuritynumber, memberships.membershipname, memberships.personid from memberships " +
                            "left outer join Memberships on persons.id1 = Memberships.PersonId where memberships.personid>" + actualStartId + " order by memberships.personid desc");
                    ret = stmt.executeQuery();
                  }
                  else if (queryType.equals("2wayInner2Field")) {
                    stmt = conn.prepareStatement(
                        "select persons.id1, persons.socialsecuritynumber, memberships.membershipname from persons " +
                            " inner join Memberships on Memberships.PersonId = persons.id1 and memberships.personid > " + actualStartId + " where persons.id1 > 0 order by persons.id1 desc");
                    ret = stmt.executeQuery();
                  }
                  else if (queryType.equals("2wayRightOuter")) {
                    stmt = conn.prepareStatement(
                        "select persons.id1, persons.socialsecuritynumber, memberships.personId, memberships.membershipname from persons " +
                            "right outer join Memberships on persons.id1 = Memberships.PersonId where persons.id1>" + actualStartId + " order by persons.id1 desc");
                    ret = stmt.executeQuery();
                  }
                  totalSelectDuration.addAndGet(System.nanoTime() - beginSelect);
                  while (true) {
                    if (selectOffset.incrementAndGet() % 100000 == 0) {

                      String builder = "select: count=" + selectOffset.get() + ", rate=" +
                          selectOffset.get() / (float) (System.currentTimeMillis() - selectBegin.get()) * 1000f + "/sec" +
                          ", avgDuration=" + (totalSelectDuration.get() / (float) selectOffset.get() / 1000000f) + MILLIS_STR +
                          ", maxDuration=" + maxDuration.get() / 1000000f + MILLIS_STR +
                          "errorCount=" + selectErrorCount.get();
                      logger.info(builder);
                    }
                    long begin = System.nanoTime();
                    if (!ret.next()) {
                      ret.close();
                      stmt.close();
                      break;
                    }
                    totalSelectDuration.addAndGet(System.nanoTime() - begin);
                  }
              }
              catch (Exception e) {
                selectErrorCount.incrementAndGet();
                logger.error("Error", e);
              }
              finally {
                if (selectOffset.incrementAndGet() % 10000 == 0) {
                  String builder = "select: count=" + selectOffset.get() + ", rate=" +
                      selectOffset.get() / (float) (System.currentTimeMillis() - selectBegin.get()) * 1000f + "/sec" +
                      ", avgDuration=" + totalSelectDuration.get() / selectOffset.get() / 1000000 + MILLIS_STR +
                      "errorCount=" + selectErrorCount.get();
                  logger.info(builder);
                }
              }

            }
          });
          threads[i].start();
        }
      }
      catch (Exception e) {
        logger.error("Error", e);
      }
    });
    mainThread.start();
  }

  private void doResetStats() {
    totalSelectDuration.set(0);
    selectErrorCount.set(0);
    selectBegin.set(System.currentTimeMillis());
    selectOffset.set(0);
    maxDuration.set(0);
  }


  public void stop() {
    shutdown = true;
    mainThread.interrupt();
  }

  public String stats() {
    ObjectNode dict = new ObjectNode(JsonNodeFactory.instance);
    dict.put("begin", selectBegin.get());
    dict.put("count", selectOffset.get());
    dict.put("errorCount", selectErrorCount.get());
    dict.put("totalDuration", totalSelectDuration.get());
    dict.put("countDead", 0);
    dict.put("activeThreads", 0);
    return dict.toString();
  }

  public void resetStats() {
    doResetStats();
  }
}
