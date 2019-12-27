package com.sonicbase.accept.server;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.ComObject;
import com.sonicbase.common.Config;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.server.DatabaseServer;
import com.sonicbase.server.LogManager;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.giraph.utils.Varint;
import org.testng.annotations.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.testng.Assert.assertEquals;

public class TestLongManagerLostEntries {

  final ConcurrentHashMap<Long, Long> foundIds = new ConcurrentHashMap<>();
  final AtomicInteger countPlayed = new AtomicInteger();

  class MonitorServer extends com.sonicbase.server.DatabaseServer {
    public byte[] invokeMethod(final byte[] body, long logSequence0, long logSequence1,
                                boolean replayedCommand, boolean enableQueuing, AtomicLong timeLogging, AtomicLong handlerTime) {
      if (replayedCommand) {
        if (countPlayed.incrementAndGet() % 10000 == 0) {
          System.out.println("count=" + countPlayed.get());
        }
        ComObject cobj = new ComObject(body);
        long value = cobj.getInt(ComObject.Tag.COUNT);
        if (null != foundIds.put(value, value)) {
          System.out.println("Value already set");
        }
      }
      return super.invokeMethod(body, logSequence0, logSequence1, replayedCommand, enableQueuing, timeLogging, handlerTime);
    }
  }

  public static void main(String[] args) throws IOException {
    int countRead = 0;
    File dir = new File("/Users/lowryda/db/database/log/0/0/self");
    for (File file : dir.listFiles()) {
      DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
      while (true) {
        try {
          in.readInt();
          short serializationVersion = (short) Varint.readSignedVarLong(in);
          Varint.readSignedVarLong(in);
          Varint.readSignedVarLong(in);
          int size = in.readInt();
          if (size == 0) {
            throw new DatabaseException("Invalid size: size=0");
          }
          byte[] buffer = size == 0 ? null : new byte[size];
          if (buffer != null) {
            in.readFully(buffer);
          }
          countRead++;
        }
        catch (EOFException e) {
          break;
        }
      }
    }
    System.out.println("count=" + countRead);
  }

  @Test
  public void test() throws IOException, InterruptedException {
    String configStr = IOUtils.toString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-4-servers.yaml")), "utf-8");
    Config config = new Config(configStr);

    FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db-data"));

    DatabaseClient.getServers().clear();

    final com.sonicbase.server.DatabaseServer[] dbServers = new com.sonicbase.server.DatabaseServer[4];

    String role = "primaryMaster";

    for (int i = 0; i < dbServers.length; i++) {
      dbServers[i] = new MonitorServer();
      Config.copyConfig("4-servers");
      dbServers[i].setConfig(config, "localhost", 9010 + (50 * i), true, new AtomicBoolean(true), new AtomicBoolean(true),null, false);
      dbServers[i].setRole(role);
    }

    try {
      com.sonicbase.server.DatabaseServer.initDeathOverride(2, 2);
      com.sonicbase.server.DatabaseServer.deathOverride[0][0] = false;
      com.sonicbase.server.DatabaseServer.deathOverride[0][1] = false;
      com.sonicbase.server.DatabaseServer.deathOverride[1][0] = false;
      DatabaseServer.deathOverride[1][1] = false;

      dbServers[0].enableSnapshot(false);
      dbServers[1].enableSnapshot(false);
      dbServers[2].enableSnapshot(false);
      dbServers[3].enableSnapshot(false);

      int countToProcess = 100_000;
      com.sonicbase.server.LogManager logManager = dbServers[0].getLogManager();

      logManager.setCycleLogsMillis(100);

      AtomicLong timeLogging = new AtomicLong();
      List<com.sonicbase.server.LogManager.LogRequest> requests = new ArrayList<>();
      for (int i = 0; i < countToProcess; i++) {
        if (i % 1000 == 0) {
          Thread.sleep(100);
        }
        if (i % 10_000 == 0) {
          System.out.println("upsert progress: count=" + i);
        }
        ComObject cobj = new ComObject(2);
        cobj.put(ComObject.Tag.COUNT, i);
        cobj.put(ComObject.Tag.METHOD, "echoWrite");
        requests.add(logManager.logRequest(cobj.serialize(), true, "echoWrite", (long) i, (long) i, timeLogging));
      }

      for (LogManager.LogRequest request : requests) {
        request.getLatch().await();
      }

      //System.out.println("count logged: " + logManager.getCountLogged());
      countPlayed.set(0);
      logManager.applyLogs();

      boolean first = true;
      int countMissing = 0;
      for (int i = 0; i < countToProcess; i++) {
        if (!foundIds.containsKey((long) i)) {
          if (first) {
            System.out.println("doesn't contain: " + i);
            first = false;
          }
          countMissing++;
        }
      }
      assertEquals(countMissing, 0);
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
    finally {
      dbServers[0].shutdown();
      dbServers[1].shutdown();
      dbServers[2].shutdown();
      dbServers[3].shutdown();
    }

    System.out.println("client refCount=" + DatabaseClient.clientRefCount.get());

  }
}
