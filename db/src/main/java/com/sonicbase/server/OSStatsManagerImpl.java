package com.sonicbase.server;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.*;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.jdbcdriver.StatementProxy;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.Schema;
import com.sonicbase.schema.TableSchema;
import org.apache.commons.io.input.ReversedLinesFileReader;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.sonicbase.common.MemUtil.getMemValue;
import static com.sonicbase.server.ProServer.SONICBASE_SYS_DB_STR;

public class OSStatsManagerImpl {

  public static final String TIME_FORMAT_STR = "yyyy-MM-dd'T'HH:mm:ss";

  private final DatabaseServer server;
  private final ProServer proServer;
  private Thread persisterThread;

  private AtomicBoolean initialized = new AtomicBoolean();
  private Connection conn;
  private static Logger logger = LoggerFactory.getLogger(OSStatsManagerImpl.class);

  private boolean shutdown;
  private double avgTransRate = 0;
  private double avgRecRate = 0;
  private Thread netMonitorThread;
  private AtomicBoolean aboveMemoryThreshold = new AtomicBoolean();
  private Thread memoryMonitorThread;
  private Thread statsThread;
  private boolean enable = true;
  private boolean isDatabaseInitialized;


  public OSStatsManagerImpl(ProServer proServer, DatabaseServer server) {
    this.proServer = proServer;
    this.server = server;

    startStatsMonitoring();
  }

  public void shutdown() {
    try {
      this.shutdown = true;


      if (statsThread != null) {
        statsThread.interrupt();
        statsThread.join();
        statsThread = null;
      }

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

      if (memoryMonitorThread != null) {
        memoryMonitorThread.interrupt();
        memoryMonitorThread.join();
        memoryMonitorThread = null;
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

    statsThread = ThreadUtil.createThread(new StatsMonitor(), "SonicBase Stats Monitor Thread");
    statsThread.start();


    netMonitorThread = ThreadUtil.createThread(new NetMonitor(), "SonicBase Network Monitor Thread");
    netMonitorThread.start();

    persisterThread = ThreadUtil.createThread(new StatsPersister(), "SonicBase OS Stats Persister");
    persisterThread.start();
  }

  public ComObject initConnection(ComObject cobj, boolean replayedCommand) {
    logger.info("initConnection - begin");

    initConnection();

    logger.info("initConnection - end");
    return null;
  }

  private void initConnection() {
    if (conn == null) {
      conn = proServer.getSysConnection();
    }
  }


  public ComObject initMonitoringTables(ComObject cobj, boolean replayedCommand) {
    logger.info("initMonitoringTables - begin");

    conn = proServer.getSysConnection();

    synchronized (this) {
      while (!shutdown) {
        try {
          int healthyShards = 0;
          for (int i = 0; i < server.getShardCount(); i++) {
            cobj = new ComObject();
            server.getClient().send("DatabaseServer:healthCheck", i, 0, cobj, DatabaseClient.Replica.DEF);
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
        DatabaseClient client = ((ConnectionProxy)conn).getDatabaseClient();

        client.syncSchema();
        if (null == client.getCommon().getTables("_sonicbase_sys").get("os_stats")) {
          try (PreparedStatement stmt = conn.prepareStatement("create table os_stats(host VARCHAR, time_val VARCHAR, cpu double, res_mem double, jmem_min double, jmem_max double, net_bytes_in double, net_bytes_out double, disk_avail double, PRIMARY KEY (host, time_val))")) {
            ((StatementProxy) stmt).disableStats(true);
            stmt.executeUpdate();
          }
        }

        client.syncSchema();

        if (null == client.getCommon().getTables("_sonicbase_sys").get("os_stats").getIndices().get("time_val")) {
          try (PreparedStatement stmt = conn.prepareStatement("create index time_val on os_stats(time_val)")) {
            ((StatementProxy) stmt).disableStats(true);
            stmt.executeUpdate();
          }
        }

        client.syncSchema();

        if (null == client.getCommon().getTables("_sonicbase_sys").get("os_totals")) {
          try (PreparedStatement stmt = conn.prepareStatement("create table os_totals(host VARCHAR, mem double, disk double, PRIMARY KEY (host))")) {
            ((StatementProxy) stmt).disableStats(true);
            stmt.executeUpdate();
          }
        }
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
      finally {
        initialized.set(true);
      }
    }
    logger.info("initMonitoringTables - finished");
    return null;
  }

  class StatsPersister implements Runnable {

    @Override
    public void run() {
      outer:
      while (!shutdown) {
        try {
          if (!enable) {
            Thread.sleep(2_000);
            continue;
          }
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

          if (!isDatabaseInitialized()) {
            logger.info("OSStatsMonitor database not initialized, skipping.");
            continue;
          }

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
        Double totalMem = getTotalMemory();
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

          try (PreparedStatement stmt = conn.prepareStatement("insert into os_stats (host, time_val, cpu, res_mem, jmem_min, jmem_max, net_bytes_in, net_bytes_out, disk_avail) values (?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
            stmt.setString(1, server.getHost() + ":" + server.getPort());
            stmt.setString(2, timeStr);
            stmt.setDouble(3, stats.cpu);
            stmt.setDouble(4, stats.resGig);
            stmt.setDouble(5, stats.javaMemMin);
            stmt.setDouble(6, stats.javaMemMax);
            stmt.setDouble(7, avgRecRate);
            stmt.setDouble(8, avgTransRate);
            stmt.setDouble(9, getNumber(stats.diskAvail));
            stmt.executeUpdate();
          }

          logger.info("get os_stats - end");

          evictOldStats();

        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
        catch (Exception e) {
          logger.error("Error gathering OS stats", e);
        }
        finally {
          ThreadUtil.sleep(60_000);
        }
      }
    }
  }

  private boolean isDatabaseInitialized() {

    if (isDatabaseInitialized) {
      return true;
    }
    if (conn == null) {
      return false;
    }
    DatabaseClient client = ((ConnectionProxy)conn).getDatabaseClient();
    DatabaseCommon common = client.getCommon();

    Schema schema = common.getDatabases().get("_sonicbase_sys");
    if (schema == null) {
      return false;
    }
    TableSchema tableSchema = schema.getTables().get("os_stats");
    if (tableSchema == null) {
      return false;
    }
    IndexSchema indexSchema = tableSchema.getIndices().get("time_val");
    if (indexSchema == null) {
      return false;
    }
    isDatabaseInitialized = true;
    return true;
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
    AtomicReference<Double> javaMemMax = new AtomicReference<>();
    AtomicReference<Double> javaMemMin = new AtomicReference<>();
    try {
      if (server.isMac()) {
        getMacStats(ret);
      }
      else if (server.isUnix()) {
        getUnixStats(ret);
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
      logger.error("Error getting stats", e);
      throw new DatabaseException(e);
    }
    return ret;
  }

  private void getUnixStats(OSStats ret) throws IOException, InterruptedException {
    String secondToLastLine = null;
    String lastLine = null;
    String cpuLine = null;
    boolean firstGroup = true;
    ProcessBuilder builder = new ProcessBuilder().command("top", "-b", "-n", "2", "-p", String.valueOf(getPid()));
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
          if (firstGroup) {
            firstGroup = false;
          }
          else {
            secondToLastLine = lastLine;
            lastLine = line;
          }
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
    }

    int pos = cpuLine.indexOf("%id");
    int pos2 = cpuLine.lastIndexOf(" ", pos);
    ret.cpu = 100d - Double.valueOf(cpuLine.substring(pos2, pos).trim());

    ret.diskAvail = getDiskAvailable()[1];
  }

  private void getMacStats(OSStats ret) throws IOException, InterruptedException {
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
    ret.diskAvail = getDiskAvailable()[1];
  }

  class NetMonitor implements Runnable {
    public void run() {
      List<Double> transRate = new ArrayList<>();
      List<Double> recRate = new ArrayList<>();
      try {
        if (server.isMac()) {
          getMacNetStats(transRate, recRate);
        }
        else if (server.isUnix()) {
          getUnixNetStats(transRate, recRate);
        }
        else if (server.isWindows()) {
          getWindowsNetStats();
        }
      }
      catch (Exception e) {
        logger.error("Error in net monitor thread", e);
      }
    }
  }

  private void getWindowsNetStats() throws IOException, InterruptedException {
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

  private void getUnixNetStats(List<Double> transRate, List<Double> recRate) throws IOException, InterruptedException {
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
                  //I'm not sure why I need to multiply by 60, but that gets the stats in line with
                  // what AWS is reporting
                  avgRecRate = 60 * total / recRate.size();

                  total = 0d;
                  for (Double currTrans : transRate) {
                    total += currTrans;
                  }
                  avgTransRate = 60 * total / transRate.size();
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

  private void getMacNetStats(List<Double> transRate, List<Double> recRate) throws IOException, InterruptedException {
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
    ProcessBuilder builder = new ProcessBuilder().command("tasklist", "/fi", "\"pid eq " + getPid() + "\"");
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
    ProcessBuilder builder = new ProcessBuilder().command("bin/get-cpu.bat", String.valueOf(getPid()));
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

  public AtomicBoolean getAboveMemoryThreshold() {
    return aboveMemoryThreshold;
  }

  private static final int pid;

  static {
    try {
      java.lang.management.RuntimeMXBean runtime =
          java.lang.management.ManagementFactory.getRuntimeMXBean();
      java.lang.reflect.Field jvm = runtime.getClass().getDeclaredField("jvm");
      jvm.setAccessible(true);
      sun.management.VMManagement mgmt =
          (sun.management.VMManagement) jvm.get(runtime);
      java.lang.reflect.Method pid_method =
          mgmt.getClass().getDeclaredMethod("getProcessId");
      pid_method.setAccessible(true);

      pid = (Integer) pid_method.invoke(mgmt);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public int getPid() {
    return pid;
  }

  public void startMemoryMonitor() {
    memoryMonitorThread = ThreadUtil.createThread(new Runnable() {
      @Override
      public void run() {
        while (!shutdown && !Thread.interrupted()) {
          try {
            Thread.sleep(30000);

            Double totalGig = checkResidentMemory();

            checkJavaHeap(totalGig);
          }
          catch (InterruptedException e) {
            break;
          }
          catch (Exception e) {
            logger.error("Error in memory check thread", e);
          }
        }

      }
    }, "SonicBase Memory Monitor Thread");
    memoryMonitorThread.start();
  }

  private Double checkResidentMemory() throws InterruptedException {
    Double totalGig = null;
    String node = server.getConfig().getString("maxProcessMemoryTrigger");
    String max = node == null ? null : node;

    Double resGig = null;
    String secondToLastLine = null;
    String lastLine = null;
    try {
      if (server.isMac()) {
        ProcessBuilder builder = new ProcessBuilder().command("sysctl", "hw.memsize");
        Process p = builder.start();
        try (BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
          String line = in.readLine();
          p.waitFor();
          String[] parts = line.split(" ");
          String memStr = parts[1];
          totalGig = getMemValue(memStr);
        }
        finally {
          p.destroy();
        }
        builder = new ProcessBuilder().command("top", "-l", "1", "-pid", String.valueOf(pid));
        p = builder.start();
        try (BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
          while (!shutdown) {
            String line = in.readLine();
            if (line == null) {
              break;
            }
            secondToLastLine = lastLine;
            lastLine = line;
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
          if (headerParts[i].toLowerCase().trim().equals("mem")) {
            resGig = getMemValue(parts[i]);
          }
        }
      }
      else if (server.isUnix()) {
        ProcessBuilder builder = new ProcessBuilder().command("grep", "MemTotal", "/proc/meminfo");
        Process p = builder.start();
        try {
          BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
          String line = in.readLine();
          p.waitFor();
          line = line.substring("MemTotal:".length()).trim();
          totalGig = getMemValue(line);
        }
        finally {
          p.destroy();
        }
        builder = new ProcessBuilder().command("top", "-b", "-n", "1", "-p", String.valueOf(pid));
        p = builder.start();
        try {
          BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
          while (!shutdown) {
            String line = in.readLine();
            if (line == null) {
              break;
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
            resGig = getMemValue(memStr);
          }
        }
      }
      if (max != null) {
        if (totalGig == null || resGig == null) {
          logger.error("Unable to obtain os memory info: pid=" + pid + ", totalGig=" + totalGig + ", residentGig=" + resGig +
              ", line2=" + secondToLastLine + ", line1=" + lastLine);
        }
        else {
          if (max.contains("%")) {
            max = max.replaceAll("\\%", "").trim();
            double maxPercent = Double.valueOf(max);
            double actualPercent = resGig / totalGig * 100d;
            if (actualPercent > maxPercent) {
              logger.info(String.format("Above max memory threshold: pid=" + pid + ", totalGig=%.2f, residentGig=%.2f, percentMax=%.2f, percentActual=%.2f ",
                  totalGig, resGig, maxPercent, actualPercent) + ", line2=" + secondToLastLine + ", line1=" + lastLine);
              aboveMemoryThreshold.set(true);
            }
            else {
              logger.info(String.format("Not above max memory threshold: pid=" + pid + ", totalGig=%.2f, residentGig=%.2f, percentMax=%.2f, percentActual=%.2f ",
                  totalGig, resGig, maxPercent, actualPercent) + ", line2=" + secondToLastLine + ", line1=" + lastLine);
              aboveMemoryThreshold.set(false);
            }
          }
          else {
            double maxGig = getMemValue(max);
            if (resGig > maxGig) {
              logger.info(String.format("Above max memory threshold: totalGig=%.2f, residentGig=%.2f, maxGig=%.2f ", totalGig, resGig, maxGig) + "line2=" + secondToLastLine + ", line1=" + lastLine);
              aboveMemoryThreshold.set(true);
            }
            else {
              logger.info(String.format("Not above max memory threshold: totalGig=%.2f, residentGig=%.2f, maxGig=%.2f, ", totalGig, resGig, maxGig) + "line2=" + secondToLastLine + ", line1=" + lastLine);
              aboveMemoryThreshold.set(false);
            }
          }
        }
      }
    }
    catch (InterruptedException e) {
      throw e;
    }
    catch (Exception e) {
      logger.error("Error checking memory: line2=" + secondToLastLine + ", line1=" + lastLine, e);
    }

    return totalGig;
  }

  public Double getTotalMemory() {
    try {
      Double totalGig = null;
      String node = server.getConfig().getString("maxProcessMemoryTrigger");
      String max = node == null ? null : node;
      Double resGig = null;
      String secondToLastLine = null;
      String lastLine = null;
      try {
        if (server.isMac()) {
          ProcessBuilder builder = new ProcessBuilder().command("sysctl", "hw.memsize");
          Process p = builder.start();
          try (BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
            String line = in.readLine();
            p.waitFor();
            String[] parts = line.split(" ");
            String memStr = parts[1];
            totalGig = getMemValue(memStr);
          }
          finally {
            p.destroy();
          }
          return totalGig;
        }
        else if (server.isUnix()) {
          ProcessBuilder builder = new ProcessBuilder().command("grep", "MemTotal", "/proc/meminfo");
          Process p = builder.start();
          try {
            BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line = in.readLine();
            p.waitFor();
            line = line.substring("MemTotal:".length()).trim();
            totalGig = getMemValue(line);
          }
          finally {
            p.destroy();
          }
          return totalGig;
        }
        else if (server.isWindows()) {
          ProcessBuilder builder = new ProcessBuilder().command("wmic", "ComputerSystem", "get", "TotalPhysicalMemory");
          Process p = builder.start();
          try {
            BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line = in.readLine();
            line = in.readLine();
            p.waitFor();
            totalGig = getMemValue(line);
          }
          finally {
            p.destroy();
          }
          return totalGig;
        }
      }
      catch (InterruptedException e) {
        throw e;
      }
      catch (Exception e) {
        logger.error("Error checking memory: line2=" + secondToLastLine + ", line1=" + lastLine, e);
      }

      return totalGig;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private class StatsMonitor implements Runnable {
    @Override
    public void run() {
      while (!shutdown && !Thread.interrupted()) {
        try {
          for (int i = 0; i < 100; i++) {
            Thread.sleep(300);
          }
          if (!enable) {
            continue;
          }
          OSStatsManagerImpl.OSStats stats = doGetOSStats();
          logger.info("OS Stats: CPU=" + String.format("%.2f", stats.cpu) + ", resGig=" + String.format("%.2f", stats.resGig) +
              ", javaMemMin=" + String.format("%.2f", stats.javaMemMin) + ", javaMemMax=" + String.format("%.2f", stats.javaMemMax) +
              ", NetOut=" + String.format("%.2f", stats.avgTransRate) + ", NetIn=" + String.format("%.2f", stats.avgRecRate) +
              ", DiskAvail=" + stats.diskAvail);
        }
        catch (InterruptedException e) {
          break;
        }

      }
    }
  }


  public ComObject getOSStats(ComObject cobj, boolean replayedCommand) {
    try {
      OSStatsManagerImpl.OSStats stats = doGetOSStats();
      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.RES_GIG, stats.resGig);
      retObj.put(ComObject.Tag.CPU, stats.cpu);
      retObj.put(ComObject.Tag.JAVA_MEM_MIN, stats.javaMemMin);
      retObj.put(ComObject.Tag.JAVA_MEM_MAX, stats.javaMemMax);
      retObj.put(ComObject.Tag.AVG_REC_RATE, stats.avgRecRate);
      retObj.put(ComObject.Tag.AVG_TRANS_RATE, stats.avgTransRate);
      retObj.put(ComObject.Tag.DISK_AVAIL, stats.diskAvail);
      retObj.put(ComObject.Tag.HOST, server.getHost());
      retObj.put(ComObject.Tag.PORT, server.getPort());

      return retObj;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }


  private void checkJavaHeap(Double totalGig) throws IOException, InterruptedException {
    String line = null;
    try {
      String max = null;
      if (server.getConfig().getString("maxJavaHeapTrigger") != null) {
        max = server.getConfig().getString("maxJavaHeapTrigger");
      }
      max = null;//disable for now
      if (max == null) {
        logger.info("Max java heap trigger not set in config. Not enforcing max");
        return;
      }

      File file = new File(server.getGcLog() + ".0.current");
      if (!file.exists()) {
        return;
      }
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
              int pos2 = ch.indexOf("->", pos);
              if (pos2 != -1) {
                int pos3 = ch.indexOf("(", pos2);
                if (pos3 != -1) {
                  line = ch;
                  String value = ch.substring(pos2 + 2, pos3);
                  value = value.trim().toLowerCase();
                  double actualGig = 0;
                  if (value.contains("g")) {
                    actualGig = Double.valueOf(value.substring(0, value.length() - 1));
                  }
                  else if (value.contains("m")) {
                    actualGig = Double.valueOf(value.substring(0, value.length() - 1)) / 1000d;
                  }
                  else if (value.contains("t")) {
                    actualGig = Double.valueOf(value.substring(0, value.length() - 1)) * 1000d;
                  }

                  double xmxValue = 0;
                  if (value.contains("g")) {
                    xmxValue = Double.valueOf(server.getXmx().substring(0, server.getXmx().length() - 1));
                  }
                  else if (value.contains("m")) {
                    xmxValue = Double.valueOf(server.getXmx().substring(0, server.getXmx().length() - 1)) / 1000d;
                  }
                  else if (value.contains("t")) {
                    xmxValue = Double.valueOf(server.getXmx().substring(0, server.getXmx().length() - 1)) * 1000d;
                  }

                  if (max.contains("%")) {
                    max = max.replaceAll("\\%", "").trim();
                    double maxPercent = Double.valueOf(max);
                    double actualPercent = actualGig / xmxValue * 100d;
                    if (actualPercent > maxPercent) {
                      logger.info(String.format("Above max java heap memory threshold: pid=" + pid + ", xmx=%s, percentOfXmx=%.2f ",
                          server.getXmx(), actualPercent) + ", line=" + ch);
                      aboveMemoryThreshold.set(true);
                    }
                    else {
                      logger.info(String.format("Not above max java heap memory threshold: pid=" + pid + ", xmx=%s, percentOfXmx=%.2f ",
                          server.getXmx(), actualPercent) + ", line=" + ch);
                      aboveMemoryThreshold.set(false);
                    }
                  }
                  else {
                    double maxGig = getMemValue(max);
                    if (actualGig > maxGig) {
                      logger.info(String.format("Above max java heap memory threshold: xmx=%s, usedHeap=%.2f ",
                          server.getXmx(), actualGig) + "line=" + ch);
                      aboveMemoryThreshold.set(true);
                    }
                    else {
                      logger.info(String.format("Not above max java heap memory threshold: xmx=%s, usedHeap=%.2f ",
                          server.getXmx(), actualGig) + "line=" + ch);
                      aboveMemoryThreshold.set(false);
                    }
                  }
                }
              }
              return;
            }
          }
        }
        while (ch != null);
      }
    }

    catch (Exception e) {
      logger.error("Error checking java memory: line=" + line, e);
    }
  }
}

