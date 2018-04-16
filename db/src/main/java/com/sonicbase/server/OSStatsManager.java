/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.server;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.ComObject;
import com.sonicbase.common.ServersConfig;
import com.sonicbase.common.ThreadUtil;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.jdbcdriver.StatementProxy;
import com.sonicbase.query.DatabaseException;
import org.apache.commons.io.input.ReversedLinesFileReader;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.nio.charset.Charset;
import java.sql.*;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.sonicbase.common.MemUtil.getMemValue;
import static com.sonicbase.server.DatabaseServer.SONICBASE_SYS_DB_STR;

public class OSStatsManager {

  public static final String TIME_FORMAT_STR = "yyyy-MM-dd'T'HH:mm:ss";

  private final DatabaseServer server;
  private Thread persisterThread;

  private AtomicBoolean initialized = new AtomicBoolean();
  private Connection conn;
  private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("com.sonicbase.logger");
  private boolean shutdown;
  private double avgTransRate = 0;
  private double avgRecRate = 0;
  private Thread netMonitorThread;

  public OSStatsManager(DatabaseServer server) {
    this.server = server;
    //this.logger = new Logger(server.getDatabaseClient());
  }

  public void shutdown() {
    try {
      this.shutdown = true;

      if (persisterThread != null) {
        persisterThread.interrupt();
        persisterThread.join();
        persisterThread = null;
      }
      if (netMonitorThread != null) {
        netMonitorThread.interrupt();
        netMonitorThread.join();
        netMonitorThread = null;
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public void startStatsMonitoring() {

    if (persisterThread != null) {
      persisterThread.interrupt();
      try {
        persisterThread.join();
      }
      catch (InterruptedException e) {
        throw new DatabaseException(e);
      }
    }

    netMonitorThread = ThreadUtil.createThread(new NetMonitor(), "SonicBase Network Monitor Thread");
    netMonitorThread.start();

    persisterThread = ThreadUtil.createThread(new StatsPersister(), "SonicBase OS Stats Persister");
    persisterThread.start();
  }

  public void initMonitoringTables() {
    if (initialized.get()) {
      return;
    }
    Connection connection = server.getSysConnection();
    synchronized (this) {
      if (conn == null) {
        conn = connection;
      }
      else {
        return;
      }
      while (!shutdown) {
        try {
          int healthyShards = 0;
          for (int i = 0; i < server.getShardCount(); i++) {
            ComObject cobj = new ComObject();
            cobj.put(ComObject.Tag.method, "healthCheck");
            server.getClient().send(null, i, 0, cobj, DatabaseClient.Replica.def);
            healthyShards++;
          }
          if (healthyShards == server.getShardCount()) {
            break;
          }
        }
        catch (Exception e) {
          logger.error("Error checking health of servers: " + e.getMessage());
        }
        if (!shutdown) {
          try {
            Thread.sleep(10_000);
          }
          catch (InterruptedException e) {
            break;
          }
        }
      }

      if (shutdown) {
        return;
      }

      PreparedStatement stmt = null;
      try {
        try {
          if (!((ConnectionProxy) conn).databaseExists(SONICBASE_SYS_DB_STR)) {
            ((ConnectionProxy) conn).createDatabase(SONICBASE_SYS_DB_STR);
          }
        }
        catch (Exception e) {
          if (!ExceptionUtils.getFullStackTrace(e).toLowerCase().contains("database already exists")) {
            throw new DatabaseException(e);
          }
        }
        StringBuilder builder = new StringBuilder();
        ((ConnectionProxy)conn).getDatabaseClient().syncSchema();
        if (null == ((ConnectionProxy)conn).getDatabaseClient().getCommon().getTables("_sonicbase_sys").get("os_stats")) {
          stmt = conn.prepareStatement("create table os_stats(host VARCHAR, time_val VARCHAR, cpu double, res_mem double, jmem_min double, jmem_max double, net_bytes_in double, net_bytes_out double, disk_avail double, PRIMARY KEY (host, time_val))");
          ((StatementProxy) stmt).disableStats();
          stmt.executeUpdate();
          stmt.close();
        }
        stmt = conn.prepareStatement("describe table os_stats");
        ((StatementProxy) stmt).disableStats();
        ResultSet rs = stmt.executeQuery();
        while (rs.next()) {
          builder.append(rs.getString(1) + "\n");
        }
        rs.close();
        stmt.close();

        String tableDescription = builder.toString();
        if (!tableDescription.contains("Index=_1_time_val")) {
          stmt = conn.prepareStatement("create index time_val on os_stats(time_val)");
          ((StatementProxy) stmt).disableStats();
          stmt.executeUpdate();
          stmt.close();
        }

        if (null == ((ConnectionProxy)conn).getDatabaseClient().getCommon().getTables("_sonicbase_sys").get("os_totals")) {
          stmt = conn.prepareStatement("create table os_totals(host VARCHAR, mem double, disk double, PRIMARY KEY (host))");
          ((StatementProxy) stmt).disableStats();
          stmt.executeUpdate();
          stmt.close();
        }
      }
      catch (Exception e) {
        if (!ExceptionUtils.getFullStackTrace(e).toLowerCase().contains("table already exists")) {
          throw new DatabaseException(e);
        }
      }
      finally {
        initialized.set(true);
        if (stmt != null) {
          try {
            stmt.close();
          }
          catch (SQLException e) {
            throw new DatabaseException(e);
          }
        }
      }
    }
  }

  class StatsPersister implements Runnable {

    @Override
    public void run() {
      outer:
      while (!shutdown) {
        try {
          logger.info("os_stats init tables- begin");

          ServersConfig.Shard[] shards = server.getCommon().getServersConfig().getShards();
          for (ServersConfig.Shard shard : shards) {
            boolean haveReplica = false;
            for (ServersConfig.Host host : shard.getReplicas()) {
              if (!host.isDead()) {
                haveReplica = true;
                break;
              }
            }
            if (!haveReplica) {
              Thread.sleep(10_000);
              continue outer;
            }
          }
          Thread.sleep(10_000);
          initMonitoringTables();

          logger.info("os_stats init tables- end");

          break;
        }
        catch (InterruptedException e) {
          break;
        }
        catch (Exception e) {
          logger.error("Error initializing OSStatsMonitor" + e.getMessage());
          continue;
        }
      }

      try {
        Double totalMem = server.getTotalMemory();
        String[] avail = getDiskAvailable();
        String totalDisk = avail == null ? "" : avail[0];

        if (!shutdown) {
          try {
            logger.info("os_stats init os_totals- begin");

            try (PreparedStatement stmt = conn.prepareStatement("insert ignore into os_totals (host, mem, disk) values (?,?,?)")) {
              stmt.setString(1, server.getHost() + ":" + server.getPort());
              stmt.setDouble(2, totalMem);
              stmt.setDouble(3, getNumber(totalDisk));
              stmt.executeUpdate();
            }
            logger.info("os_stats init os_totals- end");
          }
          catch (Exception e) {
            throw new DatabaseException(e);
          }
        }
      }
      catch (Exception e) {
        logger.error("Error in OS Stats Persister thread", e);
      }

      while (!shutdown) {
        try {
          logger.info("get os_stats - begin");
          Date date = new Date(System.currentTimeMillis());
          SimpleDateFormat df = new SimpleDateFormat(TIME_FORMAT_STR);
          df.setTimeZone(TimeZone.getTimeZone("UTC"));
          String timeStr = df.format(date);

          OSStats stats = doGetOSStats();
          PreparedStatement stmt = conn.prepareStatement("insert into os_stats (host, time_val, cpu, res_mem, jmem_min, jmem_max, net_bytes_in, net_bytes_out, disk_avail) values (?, ?, ?, ?, ?, ?, ?, ?, ?)");
          stmt.setString(1,server.getHost() + ":" + server.getPort());
          stmt.setString(2, timeStr);
          stmt.setDouble(3, stats.cpu);
          stmt.setDouble(4, stats.resGig);
          stmt.setDouble(5, stats.javaMemMin);
          stmt.setDouble(6, stats.javaMemMax);
          stmt.setDouble(7, avgRecRate);
          stmt.setDouble(8, avgTransRate);
          stmt.setDouble(9, getNumber(stats.diskAvail));
          stmt.executeUpdate();

          logger.info("get os_stats - end");

          evictOldStats();

          Thread.sleep(60_000);
        }
        catch (InterruptedException e) {
          break;
        }
        catch (Exception e) {
          logger.error("Error gathering OS stats", e);
        }
      }

    }
  }

  private void evictOldStats() {
    try {
      Date referenceDate = new Date(System.currentTimeMillis());
      Calendar c = Calendar.getInstance();
      c.setTime(referenceDate);
      c.add(Calendar.MONTH, -1);

      String dateStr = new SimpleDateFormat(TIME_FORMAT_STR).format(new Date(c.getTimeInMillis()));

      PreparedStatement delStmt = conn.prepareStatement("delete from os_stats where time_val<?");
      delStmt.setString(1, dateStr);
      delStmt.executeUpdate();
    }
    catch (Exception e) {
      logger.error("Error evicting old stats", e);
    }
  }

  @NotNull
  private Double getNumber(String value) {
    StringBuilder numeric = new StringBuilder();
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      if (c == '.' || StringUtils.isNumeric(String.valueOf(c))) {
        numeric.append(value.charAt(i));
      }
    }
    return Double.valueOf(numeric.toString());
  }

  class OSStats {
    double resGig;
    double cpu;
    double javaMemMin;
    double javaMemMax;
    double avgRecRate;
    double avgTransRate;
    String diskAvail;
  }

  public OSStats doGetOSStats() throws InterruptedException {
    logger.info("doGetOSStats - begin");
    OSStats ret = new OSStats();
    String secondToLastLine = null;
    String lastLine = null;
    AtomicReference<Double> javaMemMax = new AtomicReference<>();
    AtomicReference<Double> javaMemMin = new AtomicReference<>();
    try {
      if (server.isMac()) {
//        ProcessBuilder builder = new ProcessBuilder().command("top", "-l", "1", "-pid", String.valueOf(server.getPid()));
//        Process p = builder.start();
//        try (BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
//          while (!shutdown) {
//            String line = in.readLine();
//            if (line == null) {
//              break;
//            }
//            secondToLastLine = lastLine;
//            lastLine = line;
//          }
//          p.waitFor();
//        }
//        finally {
//          p.destroy();
//        }

        logger.info("getting mac stats");
        ProcessBuilder builder = new ProcessBuilder().command("top", "-l", "1", "-n" , "0");
        Process p = builder.start();
        boolean haveMem = false;
        boolean haveCpu = false;
        int linesRead = 0;
        try (BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
          while (!shutdown) {
            String line = in.readLine();
            if (line == null) {
              break;
            }
            linesRead++;
            if (line.startsWith("PhysMem:")) {
              String[] parts = line.split(" ");
              ret.resGig = getNumber(parts[1]);
              haveMem = true;
              logger.info("physMem: " + ret.resGig + ", line=" + line);
            }
            else if (line.startsWith("CPU usage:")) {
              String[] parts = line.split(" ");
              logger.info("cpu: line=" + line);
              ret.cpu = 100 - getNumber(parts[6]);
              logger.info("cpu: " + ret.cpu + ", line=" + line);
              haveCpu = true;
            }
            if (haveMem && haveCpu) {
              break;
            }
          }
          p.waitFor();
        }
        finally {
          p.destroy();
        }
        logger.info("lines=" + linesRead);
//        secondToLastLine = secondToLastLine.trim();
//        lastLine = lastLine.trim();
//        String[] headerParts = secondToLastLine.split("\\s+");
//        String[] parts = lastLine.split("\\s+");
//        for (int i = 0; i < headerParts.length; i++) {
//          if (headerParts[i].toLowerCase().trim().equals("mem")) {
//            ret.resGig = getMemValue(parts[i]);
//          }
//          else if (headerParts[i].toLowerCase().trim().equals("%cpu")) {
//            ret.cpu = Double.valueOf(parts[i]);
//          }
//        }
        ret.diskAvail = getDiskAvailable()[1];
      }
      else if (server.isUnix()) {
        String cpuLine = null;
        ProcessBuilder builder = new ProcessBuilder().command("top", "-b", "-n", "1", "-p", String.valueOf(server.getPid()));
        Process p = builder.start();
        try (BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
          while (!shutdown) {
            String line = in.readLine();
            if (line == null) {
              break;
            }
            if (line.toLowerCase().startsWith("cpu")) {
              cpuLine = line;
            }
            if (lastLine != null || line.trim().toLowerCase().startsWith("pid")) {
              secondToLastLine = lastLine;
              lastLine = line;
            }
            if (lastLine != null && secondToLastLine != null) {
              break;
            }
          }
          p.waitFor();
        }
        finally {
          p.destroy();
        }

        secondToLastLine = secondToLastLine.trim();
        lastLine = lastLine.trim();
        String[] headerParts = secondToLastLine.split("\\s+");
        String[] parts = lastLine.split("\\s+");
        for (int i = 0; i < headerParts.length; i++) {
          if (headerParts[i].toLowerCase().trim().equals("res")) {
            String memStr = parts[i];
            ret.resGig = getMemValue(memStr);
          }
//          else if (headerParts[i].toLowerCase().trim().equals("%cpu")) {
//            ret.cpu = Double.valueOf(parts[i]);
//          }
        }

        int pos = cpuLine.indexOf("%id");
        int pos2 = cpuLine.lastIndexOf(" ", pos);
        ret.cpu = 100d - Double.valueOf(cpuLine.substring(pos2, pos).trim());

        ret.diskAvail = getDiskAvailable()[1];
      }
      else if (server.isWindows()) {
        ret.resGig = getResGigWindows();
        ret.cpu = getCpuUtilizationWindows();
        ret.diskAvail = getDiskAvailWindows();
      }

      getJavaMemStats(javaMemMin, javaMemMax);

      ret.javaMemMin = javaMemMin.get() == null ? 0 : javaMemMin.get();
      ret.javaMemMax = javaMemMax.get() == null ? 0 : javaMemMax.get();
      ret.avgRecRate = avgRecRate;
      ret.avgTransRate = avgTransRate;
    }
    catch (InterruptedException e) {
      throw e;
    }
    catch (Exception e) {
      logger.error("Error checking memory: line2=" + secondToLastLine + ", line1=" + lastLine, e);
      throw new DatabaseException(e);
    }
    return ret;
  }

  class NetMonitor implements Runnable {
    public void run() {
      List<Double> transRate = new ArrayList<>();
      List<Double> recRate = new ArrayList<>();
      try {
        if (server.isMac()) {
          while (!shutdown) {
            ProcessBuilder builder = new ProcessBuilder().command("ifstat");
            Process p = builder.start();
            try (BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
              String firstLine = null;
              String secondLine = null;
              Set<Integer> toSkip = new HashSet<>();
              while (!shutdown) {
                String line = in.readLine();
                if (line == null) {
                  break;
                }
                String[] parts = line.trim().split("\\s+");
                if (firstLine == null) {
                  for (int i = 0; i < parts.length; i++) {
                    if (parts[i].toLowerCase().contains("bridge")) {
                      toSkip.add(i);
                    }
                  }
                  firstLine = line;
                }
                else if (secondLine == null) {
                  secondLine = line;
                }
                else {
                  try {
                    double trans = 0;
                    double rec = 0;
                    for (int i = 0; i < parts.length; i++) {
                      if (toSkip.contains(i / 2)) {
                        continue;
                      }
                      if (i % 2 == 0) {
                        rec += Double.valueOf(parts[i]);
                      }
                      else if (i % 2 == 1) {
                        trans += Double.valueOf(parts[i]);
                      }
                    }
                    transRate.add(trans);
                    recRate.add(rec);
                    if (transRate.size() > 10) {
                      transRate.remove(0);
                    }
                    if (recRate.size() > 10) {
                      recRate.remove(0);
                    }
                    Double total = 0d;
                    for (Double currRec : recRate) {
                      total += currRec;
                    }
                    avgRecRate = total / recRate.size();

                    total = 0d;
                    for (Double currTrans : transRate) {
                      total += currTrans;
                    }
                    avgTransRate = total / transRate.size();
                  }
                  catch (Exception e) {
                    logger.error("Error reading net traffic line: line=" + line, e);
                  }
                  break;
                }
              }
              p.waitFor();
            }
            finally {
              p.destroy();
            }
            Thread.sleep(1000);
          }
        }
        else if (server.isUnix()) {
          while (!shutdown) {
            int count = 0;
            File file = new File(server.getInstallDir(), "tmp/dstat.out");
            file.getParentFile().mkdirs();
            file.delete();
            ProcessBuilder builder = new ProcessBuilder().command("/usr/bin/dstat", "--output", file.getAbsolutePath());
            Process p = builder.start();
            try {
              while (!file.exists()) {
                Thread.sleep(1000);
              }
              outer:
              while (!shutdown) {
                try (BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(file)))) {
                  int recvPos = 0;
                  int sendPos = 0;
                  boolean nextIsValid = false;
                  while (!shutdown) {
                    String line = in.readLine();
                    if (line == null) {
                      break;
                    }
                    try {
                      if (count++ > 10000) {
                        p.destroy();
                        ;
                        break outer;
                      }

          /*

          "Dstat 0.7.0 CSV output"
        "Author:","Dag Wieers <dag@wieers.com>",,,,"URL:","http://dag.wieers.com/home-made/dstat/"
        "Host:","ip-10-0-0-79",,,,"User:","ec2-user"
        "Cmdline:","dstat --output out",,,,"Date:","09 Apr 2017 02:48:19 UTC"

        "total cpu usage",,,,,,"dsk/total",,"net/total",,"paging",,"system",
        "usr","sys","idl","wai","hiq","siq","read","writ","recv","send","in","out","int","csw"
        20.409,1.659,74.506,3.404,0.0,0.021,707495.561,82419494.859,0.0,0.0,0.0,0.0,17998.361,18691.991
        8.794,1.131,77.010,13.065,0.0,0.0,0.0,272334848.0,54.0,818.0,0.0,0.0,1514.0,477.0
        9.217,1.641,75.758,13.384,0.0,0.0,0.0,276201472.0,54.0,346.0,0.0,0.0,1481.0,392.0

           */
                      String[] parts = line.split(",");
                      if (line.contains("usr") && line.contains("recv") && line.contains("send")) {
                        for (int i = 0; i < parts.length; i++) {
                          if (parts[i].equals("\"recv\"")) {
                            recvPos = i;
                          }
                          else if (parts[i].equals("\"send\"")) {
                            sendPos = i;
                          }
                        }
                        nextIsValid = true;
                      }
                      else if (nextIsValid) {
                        Double trans = Double.valueOf(parts[sendPos]);
                        Double rec = Double.valueOf(parts[recvPos]);
                        transRate.add(trans);
                        recRate.add(rec);
                        if (transRate.size() > 10) {
                          transRate.remove(0);
                        }
                        if (recRate.size() > 10) {
                          recRate.remove(0);
                        }
                        Double total = 0d;
                        for (Double currRec : recRate) {
                          total += currRec;
                        }
                        avgRecRate = total / recRate.size();

                        total = 0d;
                        for (Double currTrans : transRate) {
                          total += currTrans;
                        }
                        avgTransRate = total / transRate.size();
                      }
                    }
                    catch (Exception e) {
                      logger.error("Error reading net traffic line: line=" + line, e);
                      Thread.sleep(5000);
                      break outer;
                    }
                  }
                }
                Thread.sleep(1000);
              }
              p.waitFor();
            }
            finally {
              p.destroy();
            }
          }
        }
        else if (server.isWindows()) {
          Double lastReceive = null;
          Double lastTransmit = null;
          long lastRecorded = 0;
          while (!shutdown) {
            ProcessBuilder builder = new ProcessBuilder().command("netstat", "-e");
            Process p = builder.start();
            try (BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
              while (!shutdown) {
                String line = in.readLine();
                if (line.startsWith("Bytes")) {
                  String[] parts = line.split("\\s+");
                  String receiveStr = parts[1];
                  String transmitStr = parts[2];
                  if (lastReceive == null) {
                    lastReceive = Double.valueOf(receiveStr);
                    lastTransmit = Double.valueOf(transmitStr);
                    lastRecorded = System.currentTimeMillis();
                  }
                  else {
                    Double receive = Double.valueOf(receiveStr);
                    Double transmit = Double.valueOf(transmitStr);
                    avgRecRate = (receive - lastReceive) / (System.currentTimeMillis() - lastRecorded) / 1000d;
                    avgTransRate = (transmit - lastTransmit) / (System.currentTimeMillis() - lastRecorded) / 1000d;
                    lastReceive = receive;
                    lastTransmit = transmit;
                    lastRecorded = System.currentTimeMillis();
                  }
                  break;
                }
                Thread.sleep(2000);
              }
              p.waitFor();
            }
            finally {
              p.destroy();
            }
          }
        }
      }
      catch (Exception e) {
        logger.error("Error in net monitor thread", e);
      }
    }
  }


  private void getJavaMemStats(AtomicReference<Double> javaMemMin, AtomicReference<Double> javaMemMax) {
    String line = null;
    File file = new File(server.getGcLog() + ".0.current");
    try (ReversedLinesFileReader fr = new ReversedLinesFileReader(file, Charset.forName("utf-8"))) {
      String ch;
      do {
        ch = fr.readLine();
        if (ch == null) {
          break;
        }
        if (ch.indexOf("[Eden") != -1) {
          int pos = ch.indexOf("Heap:");
          if (pos != -1) {
            int pos2 = ch.indexOf("(", pos);
            if (pos2 != -1) {
              String value = ch.substring(pos + "Heap:".length(), pos2).trim().toLowerCase();
              double maxGig = 0;
              if (value.contains("g")) {
                maxGig = Double.valueOf(value.substring(0, value.length() - 1));
              }
              else if (value.contains("m")) {
                maxGig = Double.valueOf(value.substring(0, value.length() - 1)) / 1000d;
              }
              else if (value.contains("t")) {
                maxGig = Double.valueOf(value.substring(0, value.length() - 1)) * 1000d;
              }
              javaMemMax.set(maxGig);
            }

            pos2 = ch.indexOf("->", pos);
            if (pos2 != -1) {
              int pos3 = ch.indexOf("(", pos2);
              if (pos3 != -1) {
                line = ch;
                String value = ch.substring(pos2 + 2, pos3);
                value = value.trim().toLowerCase();
                double minGig = 0;
                if (value.contains("g")) {
                  minGig = Double.valueOf(value.substring(0, value.length() - 1));
                }
                else if (value.contains("m")) {
                  minGig = Double.valueOf(value.substring(0, value.length() - 1)) / 1000d;
                }
                else if (value.contains("t")) {
                  minGig = Double.valueOf(value.substring(0, value.length() - 1)) * 1000d;
                }
                javaMemMin.set(minGig);
              }
            }
            break;
          }
        }
      }
      while (!shutdown && ch != null);
    }
    catch (Exception e) {
      logger.error("Error getting java mem stats: line=" + line);
    }
  }



  public double getResGigWindows() throws IOException, InterruptedException {
    if (shutdown) {
      return 0;
    }
    ProcessBuilder builder = new ProcessBuilder().command("tasklist", "/fi", "\"pid eq " + server.getPid() + "\"");
    Process p = builder.start();
    try (BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
      String header = in.readLine();
      String separator = in.readLine();
      String separator2 = in.readLine();
      String values = in.readLine();
      p.waitFor();

      String[] parts = values.split("\\s+");
      String kbytes = parts[4];
      kbytes = kbytes.replaceAll(",", "");
      return Double.valueOf(kbytes) / 1000d / 1000d;
    }
    finally {
      p.destroy();
    }
  }

  public double getCpuUtilizationWindows() throws IOException, InterruptedException {

    if (shutdown) {
      return 0;
    }
    ProcessBuilder builder = new ProcessBuilder().command("bin/get-cpu.bat", String.valueOf(server.getPid()));
//    "wmic", "path", "Win32_PerfFormattedData_PerfProc_Process",
//        "where", "\"IDProcess=" + pid + "\"", "get", "IDProcess,PercentProcessorTime");
    Process p = builder.start();
    try (BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
      String values = in.readLine();
      p.waitFor();

      logger.info("cpu utilization str=" + values);
      String[] parts = values.split("\\s+");
      String cpu = parts[1];
      return Double.valueOf(cpu);
    }
    finally {
      p.destroy();
    }
  }

  private String[] getDiskAvailable() {
    try {
      if (shutdown) {
        return null;
      }
      ProcessBuilder builder = new ProcessBuilder().command("df", "-h");
      Process p = builder.start();
      try {
        Integer availPos = null;
        Integer mountedPos = null;
        Integer totalSizePos = null;
        List<String> avails = new ArrayList<>();
        List<String> totals = new ArrayList<>();
        int bestLineMatching = -1;
        int bestLineAmountMatch = 0;
        int mountOffset = 0;
        try (BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
          while (!shutdown) {
            String line = in.readLine();
            if (line == null) {
              break;
            }
            String[] parts = line.split("\\s+");
            if (availPos == null) {
              for (int i = 0; i < parts.length; i++) {
                if (parts[i].toLowerCase().trim().equals("avail")) {
                  availPos = i;
                }
                else if (parts[i].toLowerCase().trim().startsWith("mounted")) {
                  mountedPos = i;
                }
                else if (parts[i].toLowerCase().trim().startsWith("size")) {
                  totalSizePos = i;
                }
              }
            }
            else {
              String mountPoint = parts[mountedPos];
              if (server.getDataDir().startsWith(mountPoint)) {
                if (mountPoint.length() > bestLineAmountMatch) {
                  bestLineAmountMatch = mountPoint.length();
                  bestLineMatching = mountOffset;
                }
              }
              avails.add(parts[availPos]);
              totals.add(parts[totalSizePos]);
              mountOffset++;
            }
          }
          p.waitFor();

          if (bestLineMatching != -1) {
            return new String[]{totals.get(bestLineMatching), avails.get(bestLineMatching)};
          }
        }
      }
      finally {
        p.destroy();
      }
      return null;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public String getDiskAvailWindows() throws IOException, InterruptedException {
    // disk avail:
    //        SETLOCAL
    //
    //        FOR /F "usebackq tokens=1,2" %%f IN (`PowerShell -NoProfile -EncodedCommand "CgBnAHcAbQBpACAAVwBpAG4AMwAyAF8ATABvAGcAaQBjAGEAbABEAGkAcwBrACAALQBGAGkAbAB0AGUAcgAgACIAQwBhAHAAdABpAG8AbgA9ACcAQwA6ACcAIgB8ACUAewAkAGcAPQAxADAANwAzADcANAAxADgAMgA0ADsAWwBpAG4AdABdACQAZgA9ACgAJABfAC4ARgByAGUAZQBTAHAAYQBjAGUALwAkAGcAKQA7AFsAaQBuAHQAXQAkAHQAPQAoACQAXwAuAFMAaQB6AGUALwAkAGcAKQA7AFcAcgBpAHQAZQAtAEgAbwBzAHQAIAAoACQAdAAtACQAZgApACwAJABmAH0ACgA="`) DO ((SET U=%%f)&(SET F=%%g))
    //
    //        @ECHO Used: %U%
    //            @ECHO Free: %F%

    if (shutdown) {
      return null;
    }
    ProcessBuilder builder = new ProcessBuilder().command("bin/disk-avail.bat");
    Process p = builder.start();
    try (BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
      String values = in.readLine();
      p.waitFor();

      return values;
    }
    finally {
      p.destroy();
    }
  }


}

