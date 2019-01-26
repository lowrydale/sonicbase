/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.server.osstats;

import com.sonicbase.common.ThreadUtil;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.server.DatabaseServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class GcLogAnalyzer {

  private static Logger logger = LoggerFactory.getLogger(GcLogAnalyzer.class);
  private final DatabaseServer server;

  private Thread fileThread;
  private Thread analyzerThread;

  public GcLogAnalyzer(DatabaseServer server) {
    this.server = server;
  }

  public void shutdown() {
    if (fileThread != null) {
      fileThread.interrupt();
    }
    if (analyzerThread != null) {
      analyzerThread.interrupt();
    }
    try {
      if (fileThread != null) {
        fileThread.join();
      }
      if (analyzerThread != null) {
        analyzerThread.join();
      }
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new DatabaseException(e);
    }
    fileThread = null;
    analyzerThread = null;
  }

  public void start() {

    shutdown();


    final String gcLogName = server.getGcLog();
    if (gcLogName == null || gcLogName.length() == 0) {
      return;
    }

    fileThread = ThreadUtil.createThread(() -> {
      File file = new File(gcLogName);
      LogFileTailer tailer = new LogFileTailer(file, false);
      tailer.addLogFileTailerListener(line -> {
        handleLine(line);
      });
      tailer.run();
    }, "SonicBase GC File Tailer");
    fileThread.start();

    analyzerThread = ThreadUtil.createThread(() -> runAnalyzer(), "SonicBase GC Analyzer Thread");
    analyzerThread.start();
  }

  private void handleLine(String line) {
    line = line.trim();
    if (line.contains("[Times:")) {
      int pos0 = line.indexOf("[Times:");
      int pos = line.indexOf("user=", pos0);
      int pos2 = line.indexOf(' ', pos);
      double user = Double.parseDouble(line.substring(pos + "user=".length(), pos2));
      pos = line.indexOf("sys=");
      pos2 = line.indexOf(',', pos);
      double sys = Double.parseDouble(line.substring(pos + "sys=".length(), pos2));
      double time = user + sys;
      synchronized (events) {
        events.add(new Event(System.currentTimeMillis(), time));
      }
    }
  }

  private static class Event {
    private long timeOfEvent;
    private double gcTime;

    public Event(long timeOfEvent, double gcTime) {
      this.timeOfEvent = timeOfEvent;
      this.gcTime = gcTime;
    }
  }

  private List<Event> events = new ArrayList<>();

  private void runAnalyzer() {
    while (!server.getShutdown()) {
      try {
        Thread.sleep(10_000);
        long currTime = System.currentTimeMillis();
        double max60Secs = 0;
        double max120Secs = 0;
        double max300Secs = 0;
        double total60Secs = 0;
        double total120Secs = 0;
        double total300Secs = 0;
        int deleteTo = 0;
        int count60 = 0;
        int count120 = 0;
        int count300 = 0;
        synchronized (events) {
          for (int i = events.size() - 1; i >= 0; i--) {
            Event event = events.get(i);
            if (event.timeOfEvent > currTime - 60_000) {
              total60Secs += event.gcTime;
              max60Secs = Math.max(event.gcTime, max60Secs);
              count60++;
            }
            if (event.timeOfEvent > currTime - 120_000) {
              total120Secs += event.gcTime;
              max120Secs = Math.max(event.gcTime, max120Secs);
              count120++;
            }
            if (event.timeOfEvent > currTime - 300_000) {
              total300Secs += event.gcTime;
              max300Secs = Math.max(event.gcTime, max300Secs);
              count300++;
            }
            else {
              deleteTo = i;
              break;
            }
          }
          for (int i = 0; i < deleteTo; i++) {
            events.remove(0);
          }
        }
        String str = String.format("GC Times: maxGcIn60Secs=%.2f, maxGcIn120Secs=%.2f, maxGcIn300Secs=%.2f, totalGcIn60Secs=%.2f, " +
            "totalGcIn120Secs=%.2f, totalGcsIn300Secs=%.2f, count60=%d, count120=%d, count300=%d, countDeleted=%s",
            max60Secs, max120Secs, max300Secs, total60Secs, total120Secs, total300Secs, count60, count120, count300, deleteTo);
        logger.info(str);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new DatabaseException(e);
      }
    }
  }
}
