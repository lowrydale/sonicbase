package com.sonicbase.database;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.ComObject;
import com.sonicbase.server.DatabaseServer;
import com.sonicbase.server.LogManager;
import org.apache.commons.io.IOUtils;
import org.codehaus.plexus.util.FileUtils;
import org.testng.annotations.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.testng.Assert.assertTrue;

/**
 * Created by lowryda on 9/4/17.
 */
public class TestLongManagerLostEntries {

  ConcurrentHashMap<Long, Long> foundIds = new ConcurrentHashMap<>();
  AtomicInteger countPlayed = new AtomicInteger();

  class MonitorServer extends DatabaseServer {
    public byte[] invokeMethod(final byte[] body, long logSequence0, long logSequence1,
                                boolean replayedCommand, boolean enableQueuing, AtomicLong timeLogging, AtomicLong handlerTime) {
      if (replayedCommand) {
        if (countPlayed.incrementAndGet() % 10000 == 0) {
          System.out.println("count=" + countPlayed.get());
        }
        ComObject cobj = new ComObject(body);
        long value = cobj.getLong(ComObject.Tag.countLong);
        if (null != foundIds.put(value, value)) {
          System.out.println("Value already set");
        }
      }
      return super.invokeMethod(body, logSequence0, logSequence1, replayedCommand, enableQueuing, timeLogging, handlerTime);
    }
  }

  @Test
  public void test() throws IOException, InterruptedException {
    String configStr = IOUtils.toString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-4-servers.json")), "utf-8");
    ObjectMapper mapper = new ObjectMapper();
    final ObjectNode config = (ObjectNode) mapper.readTree(configStr);

    FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db"));

    ArrayNode array = new ArrayNode(JsonNodeFactory.instance);
    array.add(DatabaseServer.FOUR_SERVER_LICENSE);
    config.put("licenseKeys", array);

    DatabaseClient.getServers().clear();

    final DatabaseServer[] dbServers = new DatabaseServer[4];
    ThreadPoolExecutor executor = new ThreadPoolExecutor(32, 32, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

    String role = "primaryMaster";

    for (int i = 0; i < dbServers.length; i++) {
      final int shard = i;
      dbServers[shard] = new MonitorServer();
      dbServers[shard].setConfig(config, "4-servers", "localhost", 9010 + (50 * shard), true, new AtomicBoolean(true), null, true);
      dbServers[shard].setRole(role);
      dbServers[shard].disableLogProcessor();
      dbServers[shard].setMinSizeForRepartition(0);
    }


    DatabaseServer.initDeathOverride(2, 2);
    DatabaseServer.deathOverride[0][0] = false;
    DatabaseServer.deathOverride[0][1] = false;
    DatabaseServer.deathOverride[1][0] = false;
    DatabaseServer.deathOverride[1][1] = false;

    dbServers[0].enableSnapshot(false);
    dbServers[1].enableSnapshot(false);
    dbServers[2].enableSnapshot(false);
    dbServers[3].enableSnapshot(false);

    int countToProcess = 1_000_000;
    LogManager logManager = dbServers[0].getLogManager();
    AtomicLong timeLogging = new AtomicLong();
    List<DatabaseServer.LogRequest> requests = new ArrayList<>();
    for (int i = 0; i < countToProcess; i++) {
      if (i % 1000 == 0) {
        Thread.sleep(100);
      }
      if (i % 10_000 == 0) {
        System.out.println("upsert progress: count=" + i);
      }
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.countLong, (long)i);
      cobj.put(ComObject.Tag.method, "echoWrite");
      requests.add(logManager.logRequest(cobj.serialize(), true, "echoWrite", (long)i, (long)i, timeLogging));
    }

    for (DatabaseServer.LogRequest request : requests) {
      request.getLatch().await();
    }

    countPlayed.set(0);
    logManager.applyLogs();

    boolean first = true;
    int countMissing = 0;
    for (int i = 0; i < countToProcess; i++) {
      if (!foundIds.containsKey((long)i)) {
        if (first) {
          System.out.println("doesn't contain: " + i);
          first = false;
        }
        countMissing++;
      }
    }
    System.out.println("count missing=" + countMissing);
    foundIds.clear();

//    int count = 0;
//    File dir = new File(dbServers[0].getDataDir(), "/queue/0/0/self");
//    File[] files = dir.listFiles();
//    for (File file : files) {
//      LogManager.LogSource source = new LogManager.LogSource(file, dbServers[0], dbServers[0].getLogger());
//      while (true) {
//        if (source.getCommand() == null) {
//          break;
//        }
//        if (count++ % 10000 == 0) {
//          System.out.println("progress: " + count);
//        }
//        byte[] body = source.getBuffer();
//        ComObject cobj = new ComObject(body);
//        long id = cobj.getLong(ComObject.Tag.countLong);
//        foundIds.put(id, id);
//
//        source.readNext(dbServers[0], dbServers[0].getLogger());
//      }
//    }
//    first = true;
//    countMissing = 0;
//    for (int i = 0; i < countToProcess; i++) {
//      if (!foundIds.containsKey((long)i)) {
//        if (first) {
//          System.out.println("doesn't contain: " + i);
//          first = false;
//        }
//        countMissing++;
//      }
//    }

    System.out.println("count missing=" + countMissing);
  }
}
