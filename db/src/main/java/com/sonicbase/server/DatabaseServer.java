package com.sonicbase.server;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.common.Logger;
import com.sonicbase.common.Record;
import com.sonicbase.common.SchemaOutOfSyncException;
import com.sonicbase.index.Index;
import com.sonicbase.index.Indices;
import com.sonicbase.index.Repartitioner;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.query.impl.ExpressionImpl;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.DataUtil;
import com.sonicbase.util.JsonArray;
import com.sonicbase.util.JsonDict;
import com.sonicbase.util.StreamUtils;
import com.sonicbase.research.socket.NettyServer;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.input.ReversedLinesFileReader;
import org.apache.commons.lang.exception.ExceptionUtils;
import sun.misc.Unsafe;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.security.InvalidKeyException;
import java.security.Key;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;


/**
 * User: lowryda
 * Date: 12/27/13
 * Time: 4:39 PM
 */
public class DatabaseServer {

  private Logger logger;
  private static org.apache.log4j.Logger errorLogger = org.apache.log4j.Logger.getLogger("com.sonicbase.errorLogger");
  private static org.apache.log4j.Logger clientErrorLogger = org.apache.log4j.Logger.getLogger("com.sonicbase.clientErrorLogger");

  public static final boolean ENABLE_RECORD_COMPRESSION = false;
  private AtomicLong commandCount = new AtomicLong();
  private int port;
  private String host;
  private String cluster;

  public static final String LICENSE_KEY = "CPuDJRkHB3nq45LObWTCHLNzwWFn8bUT";
  public static final String FOUR_SERVER_LICENSE = "15443f8a6727fcd935fad36afcd9125e";
  public AtomicBoolean isRunning;
  private List<byte[]> buffers;
  private ThreadPoolExecutor executor;
  private AtomicBoolean aboveMemoryThreshold = new AtomicBoolean();
  private Exception exception;
  private byte[] bytes;
  private boolean compressRecords = false;
  private boolean useUnsafe;
  private String gclog;
  private String xmx;
  private String installDir;

  @SuppressWarnings("restriction")
  private static Unsafe getUnsafe() {
    try {

      Field singleoneInstanceField = Unsafe.class.getDeclaredField("theUnsafe");
      singleoneInstanceField.setAccessible(true);
      return (Unsafe) singleoneInstanceField.get(null);

    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private DatabaseCommon common = new DatabaseCommon();
  private AtomicReference<DatabaseClient> client = new AtomicReference<>();
  private Unsafe unsafe = getUnsafe();
  private Repartitioner repartitioner;
  private AtomicLong nextRecordId = new AtomicLong();
  private int recordsByIdPartitionCount = 50000;
  private JsonDict config;
  private DatabaseClient.Replica role;
  private int shard;
  private int shardCount;
  private ConcurrentHashMap<String, Indices> indexes = new ConcurrentHashMap<>();
  private LongRunningCommands longRunningCommands;

  private static ConcurrentHashMap<Integer, Map<Integer, DatabaseServer>> servers = new ConcurrentHashMap<>();
  private static ConcurrentHashMap<Integer, Map<Integer, DatabaseServer>> debugServers = new ConcurrentHashMap<>();
  private String dataDir;
  private int replica;
  private int replicationFactor;
  private String masterAddress;
  private int masterPort;
  private UpdateManager updateManager;
  private SnapshotManager snapshotManager;
  private TransactionManager transactionManager;
  private ReadManager readManager;
  private LogManager logManager;
  private SchemaManager schemaManager;

  public DatabaseServer() {

    /*
    new Timer().scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        DatabaseServer.this.snapshotQueue();
      }
    }, QUEUE_SNAPSHOT_INTERVAL, QUEUE_SNAPSHOT_INTERVAL);

    new Timer().scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        DatabaseServer.this.snapshotReplicasQueue();
      }
    }, QUEUE_SNAPSHOT_INTERVAL, QUEUE_SNAPSHOT_INTERVAL);
*/

  }

  public void setConfig(
      final JsonDict config, String cluster, String host, int port, AtomicBoolean isRunning, boolean skipLicense, String gclog, String xmx) {
    setConfig(config, cluster, host, port, false, isRunning, false, gclog, xmx);
  }

  public void setConfig(
      final JsonDict config, String cluster, String host, int port,
      boolean unitTest, AtomicBoolean isRunning, String gclog) {
    setConfig(config, cluster, host, port, unitTest, isRunning, false, gclog, null);
  }

  public void setConfig(
      final JsonDict config, String cluster, String host, int port,
      boolean unitTest, AtomicBoolean isRunning, boolean skipLicense, String gclog, String xmx) {


    this.isRunning = isRunning;
    this.config = config;
    this.cluster = cluster;
    this.host = host;
    this.port = port;
    this.gclog = gclog;
    this.xmx = xmx;

    JsonDict databaseDict = config.getDict("database");
    this.dataDir = databaseDict.getString("dataDirectory");
    this.dataDir = dataDir.replace("$HOME", System.getProperty("user.home"));
    this.installDir = databaseDict.getString("installDirectory");
    this.installDir = installDir.replace("$HOME", System.getProperty("user.home"));
    JsonArray shards = databaseDict.getArray("shards");
    JsonDict firstServer = shards.getDict(0).getArray("replicas").getDict(0);
    ServersConfig serversConfig = null;
    executor = new ThreadPoolExecutor(256, 256, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
    if (!skipLicense) {
      validateLicense(config);
    }

    if (databaseDict.hasKey("compressRecords")) {
      compressRecords = databaseDict.getBoolean("compressRecords");
    }
    if (databaseDict.hasKey("useUnsafe")) {
      useUnsafe = databaseDict.getBoolean("useUnsafe");
    }
    else {
      useUnsafe = true;
    }

    this.masterAddress = firstServer.getString("privateAddress");
    this.masterPort = firstServer.getInt("port");

    if (firstServer.getString("privateAddress").equals(host) && firstServer.getLong("port") == port) {
      this.shard = 0;
      this.replica = 0;
      common.setShard(0);
      common.setReplica(0);

    }
    boolean isInternal = false;
    if (databaseDict.hasKey("clientIsPrivate")) {
      isInternal = databaseDict.getBoolean("clientIsPrivate");
    }
    serversConfig = new ServersConfig(shards, replicationFactor, isInternal);
    this.replica = serversConfig.getThisReplica(host, port);

    common.setShard(serversConfig.getThisShard(host, port));
    common.setReplica(this.replica);
    this.shard = common.getShard();
    this.shardCount = serversConfig.getShardCount();

    common.setServersConfig(serversConfig);

    logger = new Logger(getDatabaseClient());
    logger.info("config=" + config.toString());

    this.updateManager = new UpdateManager(this);
    this.snapshotManager = new SnapshotManager(this);
    this.transactionManager = new TransactionManager(this);
    this.readManager = new ReadManager(this);
    this.logManager = new LogManager(this);
    this.schemaManager = new SchemaManager(this);
    //recordsById = new IdIndex(!unitTest, (int) (long) databaseDict.getLong("subPartitionsForIdIndex"), (int) (long) databaseDict.getLong("initialIndexSize"), (int) (long) databaseDict.getLong("indexEntrySize"));

    this.replicationFactor = shards.getDict(0).getArray("replicas").size();
//    if (replicationFactor < 2) {
//      throw new DatabaseException("Replication Factor must be at least two");
//    }


    Thread thread = new Thread(new NetMonitor());
    thread.start();

    Thread statsThread = new Thread(new StatsMonitor());
    statsThread.start();

    if (shard != 0 || replica != 0) {
      syncDbNames();
    }

    List<String> dbNames = getDbNames(dataDir);
    for (String dbName : dbNames) {

      getCommon().addDatabase(dbName);
    }

    if (shard != 0 || replica != 0) {
      getDatabaseClient().syncSchema();
    }
    else {
      common.loadSchema(dataDir);
    }

    common.saveSchema(dataDir);

    for (String dbName : dbNames) {
      logger.info("Loaded database schema: dbName=" + dbName + ", tableCount=" + common.getTables(dbName).size());
      getIndices().put(dbName, new Indices());

      schemaManager.addAllIndices(dbName);
    }

    common.setServersConfig(serversConfig);

    initServersForUnitTest(host, port, unitTest, serversConfig);

    repartitioner = new Repartitioner(this, common);

    //common.getSchema().initRecordsById(shardCount, (int) (long) databaseDict.getLong("partitionCountForRecordIndex"));

    //logger.info("RecordsById: partitionCount=" + common.getSchema().getRecordIndexPartitions().length);

    longRunningCommands = new LongRunningCommands(this);
    startLongRunningCommands();

    startMemoryMonitor();

    logger.info("Started server");

    //logger.error("Testing errors", new DatabaseException());

//    File file = new File(dataDir, "queue/" + shard + "/" + replica);
//    queue = new Queue(file.getAbsolutePath(), "request-log", 0);
//    Thread logThread = new Thread(queue);
//    logThread.start();

  }

  public void setMinSizeForRepartition(int minSizeForRepartition) {
    repartitioner.setMinSizeForRepartition(minSizeForRepartition);
  }

  public long getCommandCount() {
    return commandCount.get();
  }

  public static Map<Integer, Map<Integer, DatabaseServer>> getServers() {
    return servers;
  }

  public static Map<Integer, Map<Integer, DatabaseServer>> getDebugServers() {
    return debugServers;
  }

  public int getReplicationFactor() {
    return replicationFactor;
  }

  public DatabaseClient getDatabaseClient() {
    synchronized (this.client) {
      if (this.client.get() != null) {
        return this.client.get();
      }
      DatabaseClient client = new DatabaseClient(masterAddress, masterPort, common.getShard(), common.getReplica(), false, common);
//      if (client.getCommon().getSchema().getVersion() <= common.getSchema().getVersion()) {
//        client.getCommon().getSchema().setTables(common.getSchema().getTables());
//        client.getCommon().getSchema().setServersConfig(common.getSchema().getServersConfig());
//      }
//      else {
//        common.getSchema().setTables(client.getCommon().getTables());
//        common.getSchema().setServersConfig(client.getCommon().getSchema().getServersConfig());
//      }

      this.client.set(client);
      return this.client.get();
    }
  }

  public int getSchemaVersion() {
    return common.getSchemaVersion();
  }

  public DatabaseCommon getCommon() {
    return common;
  }

  public TransactionManager getTransactionManager() {
    return transactionManager;
  }

  public UpdateManager getUpdateManager() {
    return updateManager;
  }

  public SnapshotManager getSnapshotManager() {
    return snapshotManager;
  }

  public LogManager getLogManager() {
    return logManager;
  }

  public SchemaManager getSchemaManager() {
    return schemaManager;
  }

  public Repartitioner getRepartitioner() {
    return repartitioner;
  }

  public void enableSnapshot(boolean enable) {
    snapshotManager.enableSnapshot(enable);
  }

  public void runSnapshot() throws InterruptedException, ParseException, IOException {
    for (String dbName : getDbNames(dataDir)) {
      //snapshotManager.runSnapshot(dbName);
    }
  }

  public void recoverFromSnapshot() throws Exception {
    for (String dbName : getDbNames(dataDir)) {
      //snapshotManager.recoverFromSnapshot(dbName);
    }
  }

  public void purgeMemory() {
    for (String dbName : indexes.keySet()) {
      for (ConcurrentHashMap<String, Index> index : indexes.get(dbName).getIndices().values()) {
        for (Index innerIndex : index.values()) {
          innerIndex.clear();
        }
      }
      common.getTables(dbName).clear();
    }
    common.saveSchema(dataDir);
  }

  public void replayLogs() {
    logManager.replayLogs();
  }

  public String getCluster() {
    return cluster;
  }

  public void setShardCount(int shardCount) {
    this.shardCount = shardCount;
  }

  public void truncateTablesQuietly() {
    for (Indices indices : indexes.values()) {
      for (ConcurrentHashMap<String, Index> innerIndices : indices.getIndices().values()) {
        for (Index index : innerIndices.values()) {
          index.clear();
        }
      }
    }
  }

  public static class Host {
    private String publicAddress;
    private String privateAddress;
    private int port;

    public Host(String publicAddress, String privateAddress, int port) {
      this.publicAddress = publicAddress;
      this.privateAddress = privateAddress;
      this.port = port;
    }

    public String getPublicAddress() {
      return publicAddress;
    }

    public String getPrivateAddress() {
      return privateAddress;
    }

    public int getPort() {
      return port;
    }

    public Host(DataInputStream in) throws IOException {
      publicAddress = in.readUTF();
      privateAddress = in.readUTF();
      port = in.readInt();
    }

    public void serialize(DataOutputStream out) throws IOException {
      out.writeUTF(publicAddress);
      out.writeUTF(privateAddress);
      out.writeInt(port);
    }
  }

  public static class Shard {
    private Host[] replicas;

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "EI_EXPOSE_REP2", justification = "copying the passed in data is too slow")
    @SuppressWarnings("PMD.ArrayIsStoredDirectly") //copying the passed in data is too slow
    public Shard(Host[] hosts) {
      this.replicas = hosts;
    }

    public Shard(DataInputStream in) throws IOException {
      int count = in.readInt();
      replicas = new Host[count];
      for (int i = 0; i < replicas.length; i++) {
        replicas[i] = new Host(in);
      }
    }

    public void serialize(DataOutputStream out) throws IOException {
      out.writeInt(replicas.length);
      for (Host host : replicas) {
        host.serialize(out);
      }
    }

    public boolean contains(String host, int port) {
      for (int i = 0; i < replicas.length; i++) {
        if (replicas[i].privateAddress.equals(host) && replicas[i].port == port) {
          return true;
        }
      }
      return false;
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "EI_EXPOSE_REP", justification = "copying the returned data is too slow")
    public Host[] getReplicas() {
      return replicas;
    }

  }

  public static class ServersConfig {
    private Shard[] shards;
    private boolean clientIsInternal;

    public ServersConfig(DataInputStream in) throws IOException {
      int count = in.readInt();
      shards = new Shard[count];
      for (int i = 0; i < count; i++) {
        shards[i] = new Shard(in);
      }
      clientIsInternal = in.readBoolean();
    }

    public void serialize(DataOutputStream out) throws IOException {
      out.writeInt(shards.length);
      for (Shard shard : shards) {
        shard.serialize(out);
      }
      out.writeBoolean(clientIsInternal);
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "EI_EXPOSE_REP", justification = "copying the returned data is too slow")
    public Shard[] getShards() {
      return shards;
    }

    public int getShardCount() {
      return shards.length;
    }

    public ServersConfig(JsonArray inShards, int replicationFactor, boolean clientIsInternal) {
      int currServerOffset = 0;
      int shardCount = inShards.size();
      shards = new Shard[shardCount];
      for (int i = 0; i < shardCount; i++) {
        JsonArray replicas = inShards.getDict(i).getArray("replicas");
        Host[] hosts = new Host[replicas.size()];
        for (int j = 0; j < hosts.length; j++) {
          hosts[j] = new Host(replicas.getDict(j).getString("publicAddress"), replicas.getDict(j).getString("privateAddress"), (int) (long) replicas.getDict(j).getLong("port"));
          currServerOffset++;
        }
        shards[i] = new Shard(hosts);

      }
      this.clientIsInternal = clientIsInternal;
    }

    public int getThisReplica(String host, int port) {
      for (int i = 0; i < shards.length; i++) {
        for (int j = 0; j < shards[i].replicas.length; j++) {
          Host currHost = shards[i].replicas[j];
          if (currHost.privateAddress.equals(host) && currHost.port == port) {
            return j;
          }
        }
      }
      return -1;
    }

    public int getThisShard(String host, int port) {
      for (int i = 0; i < shards.length; i++) {
        if (shards[i].contains(host, port)) {
          return i;
        }
      }
      return -1;
    }

    public boolean clientIsInternal() {
      return clientIsInternal;
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

  private void startMemoryMonitor() {
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        while (true) {
          try {
            Thread.sleep(30000);

            Double totalGig = checkResidentMemory();

            checkJavaHeap(totalGig);
          }
          catch (Exception e) {
            logger.error("Error in memory check thread", e);
          }
        }

      }
    });
    thread.start();
  }

  private Double checkResidentMemory() {
    Double totalGig = null;
    String max = config.getDict("database").getString("maxProcessMemoryTrigger");
    if (max == null) {
      logger.info("Max process memory not set in config. Not enforcing max memory");
    }

    Double resGig = null;
    String secondToLastLine = null;
    String lastLine = null;
    try {
      if (isMac()) {
        ProcessBuilder builder = new ProcessBuilder().command("sysctl", "hw.memsize");
        Process p = builder.start();
        BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
        String line = in.readLine();
        p.waitFor();
        String[] parts = line.split(" ");
        String memStr = parts[1];
        totalGig = getMemValue(memStr);

        builder = new ProcessBuilder().command("top", "-l", "1", "-pid", String.valueOf(pid));
        p = builder.start();
        in = new BufferedReader(new InputStreamReader(p.getInputStream()));
        while (true) {
          line = in.readLine();
          if (line == null) {
            break;
          }
          secondToLastLine = lastLine;
          lastLine = line;
        }
        p.waitFor();

        secondToLastLine = secondToLastLine.trim();
        lastLine = lastLine.trim();
        String[] headerParts = secondToLastLine.split("\\s+");
        parts = lastLine.split("\\s+");
        for (int i = 0; i < headerParts.length; i++) {
          if (headerParts[i].toLowerCase().trim().equals("mem")) {
            resGig = getMemValue(parts[i]);
          }
        }
      }
      else if (isUnix()) {
        ProcessBuilder builder = new ProcessBuilder().command("grep", "MemTotal", "/proc/meminfo");
        Process p = builder.start();
        BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
        String line = in.readLine();
        p.waitFor();
        line = line.substring("MemTotal:".length()).trim();
        totalGig = getMemValue(line);

        builder = new ProcessBuilder().command("top", "-b", "-n", "1", "-p", String.valueOf(pid));
        p = builder.start();
        in = new BufferedReader(new InputStreamReader(p.getInputStream()));
        while (true) {
          line = in.readLine();
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
    catch (Exception e) {
      logger.error("Error checking memory: line2=" + secondToLastLine + ", line1=" + lastLine, e);
    }

    return totalGig;
  }

  private double avgTransRate = 0;
  private double avgRecRate = 0;


  class NetMonitor implements Runnable {
    public void run() {
      List<Double> transRate = new ArrayList<>();
      List<Double> recRate = new ArrayList<>();
      try {
        if (isMac()) {
          while (true) {
            ProcessBuilder builder = new ProcessBuilder().command("ifstat");
            Process p = builder.start();
            BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String firstLine = null;
            String secondLine = null;
            Set<Integer> toSkip = new HashSet<>();
            while (true) {
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
            Thread.sleep(1000);
          }
        }
        else if (isUnix()) {
          while (true) {
            int count = 0;
            File file = new File(installDir, "tmp/dstat.out");
            file.getParentFile().mkdirs();
            file.delete();
            ProcessBuilder builder = new ProcessBuilder().command("/usr/bin/dstat", "--output", file.getAbsolutePath());
            Process p = builder.start();
            while (!file.exists()) {
              Thread.sleep(1000);
            }
            outer:
            while (true) {
              try (BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(file)))) {
                int recvPos = 0;
                int sendPos = 0;
                boolean nextIsValid = false;
                while (true) {
                  String line = in.readLine();
                  if (line == null) {
                    break;
                  }
                  try {
                    if (count++ > 10000) {
                      p.destroyForcibly();
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
        }
      }
      catch (Exception e) {
        logger.error("Error in net monitor thread", e);
      }
    }
  }

  private String getDiskAvailable() throws IOException, InterruptedException {
    ProcessBuilder builder = new ProcessBuilder().command("df", "-h");
    Process p = builder.start();
    Integer availPos = null;
    Integer mountedPos = null;
    List<String> avails = new ArrayList<>();
    int bestLineMatching = -1;
    int bestLineAmountMatch = 0;
    int mountOffset = 0;
    BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
    while (true) {
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
        }
      }
      else {
        String mountPoint = parts[mountedPos];
        if (dataDir.startsWith(mountPoint)) {
          if (mountPoint.length() > bestLineAmountMatch) {
            bestLineAmountMatch = mountPoint.length();
            bestLineMatching = mountOffset;
          }
        }
        avails.add(parts[availPos]);
        mountOffset++;
      }
    }
    p.waitFor();

    if (bestLineMatching != -1) {
      return avails.get(bestLineMatching);
    }
    return null;
  }

  public byte[] logError(String command, byte[] body, boolean replayedCommand) {
    try {
      DataInputStream in = new DataInputStream(new ByteArrayInputStream(body));
      boolean isClient = in.readBoolean();
      String hostName = in.readUTF();
      String msg = in.readUTF();
      String exception = null;
      if (in.readBoolean()) {
        exception = in.readUTF();
      }
      StringBuilder actualMsg = new StringBuilder();
      actualMsg.append("host=").append(hostName).append("\n");
      actualMsg.append("msg=").append(msg).append("\n");
      if (exception != null) {
        actualMsg.append("exception=").append(exception);
      }

      if (isClient) {
        clientErrorLogger.error(actualMsg.toString());
      }
      else {
        errorLogger.error(actualMsg.toString());
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
    return null;
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

  public OSStats doGetOSStats() {
    OSStats ret = new OSStats();
    String secondToLastLine = null;
    String lastLine = null;
    AtomicReference<Double> javaMemMax = new AtomicReference<>();
    AtomicReference<Double> javaMemMin = new AtomicReference<>();
    try {
      if (isMac()) {
        ProcessBuilder builder = new ProcessBuilder().command("top", "-l", "1", "-pid", String.valueOf(pid));
        Process p = builder.start();
        BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
        while (true) {
          String line = in.readLine();
          if (line == null) {
            break;
          }
          secondToLastLine = lastLine;
          lastLine = line;
        }
        p.waitFor();

        secondToLastLine = secondToLastLine.trim();
        lastLine = lastLine.trim();
        String[] headerParts = secondToLastLine.split("\\s+");
        String[] parts = lastLine.split("\\s+");
        for (int i = 0; i < headerParts.length; i++) {
          if (headerParts[i].toLowerCase().trim().equals("mem")) {
            ret.resGig = getMemValue(parts[i]);
          }
          else if (headerParts[i].toLowerCase().trim().equals("%cpu")) {
            ret.cpu = Double.valueOf(parts[i]);
          }
        }
      }
      else if (isUnix()) {
        ProcessBuilder builder = new ProcessBuilder().command("top", "-b", "-n", "1", "-p", String.valueOf(pid));
        Process p = builder.start();
        BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
        while (true) {
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

        secondToLastLine = secondToLastLine.trim();
        lastLine = lastLine.trim();
        String[] headerParts = secondToLastLine.split("\\s+");
        String[] parts = lastLine.split("\\s+");
        for (int i = 0; i < headerParts.length; i++) {
          if (headerParts[i].toLowerCase().trim().equals("res")) {
            String memStr = parts[i];
            ret.resGig = getMemValue(memStr);
          }
          else if (headerParts[i].toLowerCase().trim().equals("%cpu")) {
            ret.cpu = Double.valueOf(parts[i]);
          }
        }
      }

      getJavaMemStats(javaMemMin, javaMemMax);

      ret.diskAvail = getDiskAvailable();

      ret.javaMemMin = javaMemMin.get() == null ? 0 : javaMemMin.get();
      ret.javaMemMax = javaMemMax.get() == null ? 0 : javaMemMax.get();
      ret.avgRecRate = avgRecRate;
      ret.avgTransRate = avgTransRate;
    }
    catch (Exception e) {
      logger.error("Error checking memory: line2=" + secondToLastLine + ", line1=" + lastLine, e);
      throw new DatabaseException(e);
    }
    return ret;
  }

  public byte[] getOSStats(String command, byte[] body, boolean replayedCommand) {
    try {
      OSStats stats = doGetOSStats();
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytesOut);
      out.writeLong(SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
      out.writeDouble(stats.resGig);
      out.writeDouble(stats.cpu);
      out.writeDouble(stats.javaMemMin);
      out.writeDouble(stats.javaMemMax);
      out.writeDouble(stats.avgRecRate);
      out.writeDouble(stats.avgTransRate);
      out.writeUTF(stats.diskAvail);
      out.writeUTF(host);
      out.close();
      return bytesOut.toByteArray();
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private void getJavaMemStats(AtomicReference<Double> javaMemMin, AtomicReference<Double> javaMemMax) {
    String line = null;
    File file = new File(gclog + ".0.current");
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
      while (ch != null);
    }
    catch (Exception e) {
      logger.error("Error getting java mem stats: line=" + line);
    }
  }


  private void checkJavaHeap(Double totalGig) throws IOException {
    String line = null;
    try {
      String max = config.getDict("database").getString("maxJavaHeapTrigger");
      if (max == null) {
        logger.info("Max java heap trigger not set in config. Not enforcing max");
        return;
      }

      File file = new File(gclog + ".0.current");
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
                    xmxValue = Double.valueOf(xmx.substring(0, xmx.length() - 1));
                  }
                  else if (value.contains("m")) {
                    xmxValue = Double.valueOf(xmx.substring(0, xmx.length() - 1)) / 1000d;
                  }
                  else if (value.contains("t")) {
                    xmxValue = Double.valueOf(xmx.substring(0, xmx.length() - 1)) * 1000d;
                  }

                  if (max.contains("%")) {
                    max = max.replaceAll("\\%", "").trim();
                    double maxPercent = Double.valueOf(max);
                    double actualPercent = actualGig / xmxValue * 100d;
                    if (actualPercent > maxPercent) {
                      logger.info(String.format("Above max java heap memory threshold: pid=" + pid + ", xmx=%s, percentOfXmx=%.2f ",
                          xmx, actualPercent) + ", line=" + ch);
                      aboveMemoryThreshold.set(true);
                    }
                    else {
                      logger.info(String.format("Not above max java heap memory threshold: pid=" + pid + ", xmx=%s, percentOfXmx=%.2f ",
                          xmx, actualPercent) + ", line=" + ch);
                      aboveMemoryThreshold.set(false);
                    }
                  }
                  else {
                    double maxGig = getMemValue(max);
                    if (actualGig > maxGig) {
                      logger.info(String.format("Above max java heap memory threshold: xmx=%s, usedHeap=%.2f ",
                          xmx, actualGig) + "line=" + ch);
                      aboveMemoryThreshold.set(true);
                    }
                    else {
                      logger.info(String.format("Not above max java heap memory threshold: xmx=%s, usedHeap=%.2f ",
                          xmx, actualGig) + "line=" + ch);
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

  public static double getMemValue(String memStr) {
    int qualifierPos = memStr.toLowerCase().indexOf("m");
    if (qualifierPos == -1) {
      qualifierPos = memStr.toLowerCase().indexOf("g");
      if (qualifierPos == -1) {
        qualifierPos = memStr.toLowerCase().indexOf("t");
        if (qualifierPos == -1) {
          qualifierPos = memStr.toLowerCase().indexOf("k");
          if (qualifierPos == -1) {
            qualifierPos = memStr.toLowerCase().indexOf("b");
          }
        }
      }
    }
    double value = 0;
    if (qualifierPos == -1) {
      value = Double.valueOf(memStr.trim());
      value = value / 1024d / 1024d / 1024d;
    }
    else {
      char qualifier = memStr.toLowerCase().charAt(qualifierPos);
      value = Double.valueOf(memStr.substring(0, qualifierPos).trim());
      if (qualifier == 't') {
        value = value * 1024d;
      }
      else if (qualifier == 'm') {
        value = value / 1024d;
      }
      else if (qualifier == 'k') {
        value = value / 1024d / 1024d;
      }
    }
    return value;
  }

  private static String OS = System.getProperty("os.name").toLowerCase();

  private static boolean isWindows() {
    return OS.contains("win");
  }

  private static boolean isMac() {
    return OS.contains("mac");
  }

  private static boolean isUnix() {
    return OS.contains("nux");
  }

  public boolean isRunning() {
    return isRunning.get();
  }

  public LongRunningCommands getLongRunningCommands() {
    return longRunningCommands;
  }

  public byte[] areAllLongRunningCommandsComplete(String command, byte[] bodyzz, boolean replayedCommand) {
    if (longRunningCommands.getCommandCount() == 0) {
      return "true".getBytes();
    }
    return "false".getBytes();
  }

  private void startLongRunningCommands() {
    longRunningCommands.load();

    longRunningCommands.execute();
  }

  private static String algorithm = "DESede";

  public static String createLicense(int serverCount) {
    try {
      SecretKey symKey = new SecretKeySpec(com.sun.jersey.core.util.Base64.decode(DatabaseServer.LICENSE_KEY), algorithm);
      Cipher c = Cipher.getInstance(algorithm);
      byte[] bytes = encryptF("sonicbase:pro:" + serverCount, symKey, c);
      return Hex.encodeHexString(bytes);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public static void validateLicense(JsonDict config) {
/*
    try {
      int licensedServerCount = 0;
      int actualServerCount = 0;
      boolean pro = false;
      JsonArray keys = config.getDict("database").getArray("licenseKeys");
      if (keys == null || keys.size() == 0) {
        pro = false;
      }
      else {
        for (int i = 0; i < keys.size(); i++) {
          String key = keys.getString(i);
          SecretKey symKey = new SecretKeySpec(com.sun.jersey.core.util.Base64.decode(DatabaseServer.LICENSE_KEY), algorithm);

          Cipher c = Cipher.getInstance(algorithm);

          String decrypted = null;
          try {
            decrypted = decryptF(Hex.decodeHex(key.toCharArray()), symKey, c);
          }
          catch (Exception e) {
            throw new DatabaseException("Invalid license key");
          }
          decrypted = decrypted.toLowerCase();
          String[] parts = decrypted.split(":");
          if (!parts[0].equals("sonicbase")) {
            throw new DatabaseException("Invalid license key");
          }
          if (parts[1].equals("pro")) {
            licensedServerCount += Integer.valueOf(parts[2]);
            pro = true;
          }
          else {
            pro = false;
          }
        }
      }

      JsonArray shards = config.getDict("database").getArray("shards");
      for (int i = 0; i < shards.size(); i++) {
        JsonDict shard = shards.getDict(i);
        JsonArray replicas = shard.getArray("replicas");
        if (replicas.size() > 1 && !pro) {
          throw new DatabaseException("Replicas are only supported with 'pro' license");
        }
        actualServerCount += replicas.size();
      }
      if (pro) {
        if (actualServerCount > licensedServerCount) {
          throw new DatabaseException("Not enough licensed servers: licensedCount=" + licensedServerCount + ", actualCount=" + actualServerCount);
        }
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  */
  }

  private static byte[] encryptF(String input, Key pkey, Cipher c) throws InvalidKeyException, BadPaddingException, IllegalBlockSizeException {

    c.init(Cipher.ENCRYPT_MODE, pkey);

    byte[] inputBytes = input.getBytes();
    return c.doFinal(inputBytes);
  }

  private static String decryptF(byte[] encryptionBytes, Key pkey, Cipher c) throws InvalidKeyException,
      BadPaddingException, IllegalBlockSizeException {
    c.init(Cipher.DECRYPT_MODE, pkey);
    byte[] decrypt = c.doFinal(encryptionBytes);
    String decrypted = new String(decrypt);

    return decrypted;
  }

  private void syncDbNames() {
    try {
      logger.info("Syncing database names: shard=" + shard + ", replica=" + replica);
      String command = "DatabaseServer:getDbNames:1:1:__none__";
      byte[] ret = getDatabaseClient().send(null, 0, 0, command, null, DatabaseClient.Replica.specified);
      DataInputStream in = new DataInputStream(new ByteArrayInputStream(ret));
      long serializationVersion = DataUtil.readVLong(in);
      int count = in.readInt();
      for (int i = 0; i < count; i++) {
        String dbName = in.readUTF();
        File file = new File(dataDir, "snapshot/" + shard + "/" + replica + "/" + dbName);
        file.mkdirs();
        logger.info("Received database name: name=" + dbName);
      }
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public byte[] getDbNames(String command, byte[] body, boolean replayedCommand) {

    try {
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytesOut);
      List<String> dbNames = getDbNames(dataDir);
      DataUtil.writeVLong(out, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
      out.writeInt(dbNames.size());
      for (String dbName : dbNames) {
        out.writeUTF(dbName);
      }
      out.close();

      return bytesOut.toByteArray();
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public List<String> getDbNames(String dataDir) {
    File file = new File(dataDir, "snapshot/" + shard + "/" + replica);
    String[] dirs = file.list();
    List<String> ret = new ArrayList<>();
    if (dirs != null) {
      for (String dir : dirs) {
        if (dir.equals("config.bin")) {
          continue;
        }
        if (dir.equals("schema.bin")) {
          continue;
        }
        ret.add(dir);
      }
    }
    return ret;
  }

  public void startRepartitioner() {
    if (shard == 0 && replica == 0) {
      repartitioner.start();
    }
  }

  public int getReplica() {
    return replica;
  }

  private void initServersForUnitTest(
      String host, int port, boolean unitTest, ServersConfig serversConfig) {
    if (unitTest) {
      int thisShard = serversConfig.getThisShard(host, port);
      int thisReplica = serversConfig.getThisReplica(host, port);
      Map<Integer, DatabaseServer> currShard = DatabaseServer.servers.get(thisShard);
      if (currShard == null) {
        currShard = new ConcurrentHashMap<>();
        DatabaseServer.servers.put(thisShard, currShard);
      }
      currShard.put(thisReplica, this);
    }
    int thisShard = serversConfig.getThisShard(host, port);
    int thisReplica = serversConfig.getThisReplica(host, port);
    Map<Integer, DatabaseServer> currShard = DatabaseServer.debugServers.get(thisShard);
    if (currShard == null) {
      currShard = new ConcurrentHashMap<>();
      DatabaseServer.debugServers.put(thisShard, currShard);
    }
    currShard.put(thisReplica, this);
  }

  private boolean isIdInField(String existingValue, String id) {
    String[] parts = existingValue.split("|");
    for (String part : parts) {
      if (part.equals(id)) {
        return true;
      }
    }
    return false;
  }

  public Indices getIndices(String dbName) {
    return indexes.get(dbName);
  }

  public ConcurrentHashMap<String, Indices> getIndices() {
    return indexes;
  }

  public DatabaseClient getClient() {
    return getDatabaseClient();
  }

  public int getShard() {
    return shard;
  }

  public int getShardCount() {
    return shardCount;
  }

  public int getRecordsByIdPartitionCount() {
    return recordsByIdPartitionCount;
  }

  public void disableLogProcessor() {
    logManager.enableLogProcessor(false);
  }

  public void disableRepartitioner() {
    repartitioner.interrupt();
  }

  public byte[] updateSchema(String command, byte[] body, boolean replayedCommand) throws IOException {
    if (shard == 0 && replica == 0) {
      return null;
    }
    String[] parts = command.split(":");
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(body));
    long serializationVersion = DataUtil.readVLong(in);
    common.deserializeSchema(common, in);
    common.saveSchema(dataDir);
    return null;
  }

  public void pushSchema() {
    //common.saveSchema(dataDir);

    try {
      for (int i = 0; i < shardCount; i++) {


        for (int j = 0; j < replicationFactor; j++) {
          if (i == 0 && j == 0) {
            continue;
          }
          ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
          DataOutputStream out = new DataOutputStream(bytesOut);
          DataUtil.writeVLong(out, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
          common.serializeSchema(out);
          out.close();

          String command = "DatabaseServer:updateSchema:1:" + common.getSchemaVersion() + ":__none__";
          getDatabaseClient().send(null, i, j, command, bytesOut.toByteArray(), DatabaseClient.Replica.specified);
        }
      }
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public byte[] updateServersConfig(String command, byte[] body, boolean replayedCommand) {
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(body));
    try {
      long serializationVersion = DataUtil.readVLong(in);
      ServersConfig serversConfig = new ServersConfig(in);

      common.setServersConfig(serversConfig);
      common.saveServersConfig(getDataDir());
      setShardCount(serversConfig.getShards().length);
      getDatabaseClient().configureServers();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
    return null;
  }

  public void pushServersConfig() {
    try {
      for (int i = 0; i < shardCount; i++) {
        for (int j = 0; j < replicationFactor; j++) {
          if (i == 0 && j == 0) {
            continue;
          }
          ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
          DataOutputStream out = new DataOutputStream(bytesOut);
          DataUtil.writeVLong(out, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
          common.getServersConfig().serialize(out);
          out.close();

          String command = "DatabaseServer:updateServersConfig:1:1:__none__";
          getDatabaseClient().send(null, i, j, command, bytesOut.toByteArray(), DatabaseClient.Replica.specified);
        }
      }
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public String getDataDir() {
    return dataDir;
  }


  public void setRole(String role) {
    //if (role.equals("primaryMaster")) {
    this.role = DatabaseClient.Replica.primary;
//    }
//    else {
//      this.role = DatabaseClient.Replica.secondary;
//    }
  }

  public JsonDict getConfig() {
    return config;
  }

  public DatabaseClient.Replica getRole() {
    return role;
  }

  private boolean shutdown = false;

  public void shutdown() {
    shutdown = true;
    executor.shutdownNow();
  }

  public Object toUnsafeFromRecords(byte[][] records) {
    if (!useUnsafe) {
      return records;
    }
    else {
      try {
        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytesOut);
        DataUtil.ResultLength resultLength = new DataUtil.ResultLength();
        //out.writeInt(0);
        out.writeInt(records.length);
        for (byte[] record : records) {
          DataUtil.writeVLong(out, record.length, resultLength);
          out.write(record);
        }
        out.close();
        byte[] bytes = bytesOut.toByteArray();

        int origLen = -1;
        if (compressRecords) {
          origLen = bytes.length;

          LZ4Factory factory = LZ4Factory.fastestInstance();

          LZ4Compressor compressor = factory.fastCompressor();
          int maxCompressedLength = compressor.maxCompressedLength(bytes.length);
          byte[] compressed = new byte[maxCompressedLength];
          int compressedLength = compressor.compress(bytes, 0, bytes.length, compressed, 0, maxCompressedLength);
          bytes = new byte[compressedLength];
          System.arraycopy(compressed, 0, bytes, 0, compressedLength);
        }

        bytesOut = new ByteArrayOutputStream();
        out = new DataOutputStream(bytesOut);
        out.writeInt(bytes.length);
        out.writeInt(origLen);
        out.close();
        byte[] lenBuffer = bytesOut.toByteArray();

        //System.arraycopy(lenBuffer, 0, bytes, 0, lenBuffer.length);

        if (bytes.length > 1000000000) {
          throw new DatabaseException("Invalid allocation: size=" + bytes.length);
        }
        long address = unsafe.allocateMemory(bytes.length + 8);
        for (int i = 0; i < lenBuffer.length; i++) {
          unsafe.putByte(address + i, lenBuffer[i]);
        }
        for (int i = lenBuffer.length; i < lenBuffer.length + bytes.length; i++) {
          unsafe.putByte(address + i, bytes[i - lenBuffer.length]);
        }
        return -1 * address;
      }
      catch (IOException e) {
        throw new DatabaseException(e);
      }
    }
  }

  public Object toUnsafeFromKeys(byte[][] records) {
    if (!useUnsafe) {
      return records;
    }
    else {
      try {
        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytesOut);
        DataUtil.ResultLength resultLength = new DataUtil.ResultLength();
        //out.writeInt(0);
        out.writeInt(records.length);
        for (byte[] record : records) {
          DataUtil.writeVLong(out, record.length, resultLength);
          out.write(record);
        }
        out.close();
        byte[] bytes = bytesOut.toByteArray();
        int origLen = -1;

        if (compressRecords) {
          origLen = bytes.length;

          LZ4Factory factory = LZ4Factory.fastestInstance();

          LZ4Compressor compressor = factory.fastCompressor();
          int maxCompressedLength = compressor.maxCompressedLength(bytes.length);
          byte[] compressed = new byte[maxCompressedLength];
          int compressedLength = compressor.compress(bytes, 0, bytes.length, compressed, 0, maxCompressedLength);
          bytes = new byte[compressedLength];
          System.arraycopy(compressed, 0, bytes, 0, compressedLength);
        }

        bytesOut = new ByteArrayOutputStream();
        out = new DataOutputStream(bytesOut);
        out.writeInt(bytes.length);
        out.writeInt(origLen);
        out.close();
        byte[] lenBuffer = bytesOut.toByteArray();

        //System.arraycopy(lenBuffer, 0, bytes, 0, lenBuffer.length);


        if (bytes.length > 1000000000) {
          throw new DatabaseException("Invalid allocation: size=" + bytes.length);
        }

        long address = unsafe.allocateMemory(bytes.length + 8);
        for (int i = 0; i < lenBuffer.length; i++) {
          unsafe.putByte(address + i, lenBuffer[i]);
        }
        for (int i = lenBuffer.length; i < lenBuffer.length + bytes.length; i++) {
          unsafe.putByte(address + i, bytes[i - lenBuffer.length]);
        }
        return -1 * address;
      }
      catch (IOException e) {
        throw new DatabaseException(e);
      }
    }
  }

  public byte[][] fromUnsafeToRecords(Object obj) {
    try {
      if (obj instanceof Long) {
        long address = (long) obj;

        address *= -1;
        byte[] lenBuffer = new byte[8];
        lenBuffer[0] = unsafe.getByte(address + 0);
        lenBuffer[1] = unsafe.getByte(address + 1);
        lenBuffer[2] = unsafe.getByte(address + 2);
        lenBuffer[3] = unsafe.getByte(address + 3);
        lenBuffer[4] = unsafe.getByte(address + 4);
        lenBuffer[5] = unsafe.getByte(address + 5);
        lenBuffer[6] = unsafe.getByte(address + 6);
        lenBuffer[7] = unsafe.getByte(address + 7);
        ByteArrayInputStream bytesIn = new ByteArrayInputStream(lenBuffer);
        DataInputStream in = new DataInputStream(bytesIn);
        int count = in.readInt();
        int origLen = in.readInt();
        byte[] bytes = new byte[count];
        for (int i = 0; i < count; i++) {
          bytes[i] = unsafe.getByte(address + i + 8);
        }

        if (origLen != -1) {
          LZ4Factory factory = LZ4Factory.fastestInstance();

          LZ4FastDecompressor decompressor = factory.fastDecompressor();
          byte[] restored = new byte[origLen];
          decompressor.decompress(bytes, 0, restored, 0, origLen);
          bytes = restored;
        }

        in = new DataInputStream(new ByteArrayInputStream(bytes));
        DataUtil.ResultLength resultLength = new DataUtil.ResultLength();
        //in.readInt(); //byte count
        //in.readInt(); //orig len
        byte[][] ret = new byte[in.readInt()][];
        for (int i = 0; i < ret.length; i++) {
          int len = (int) DataUtil.readVLong(in, resultLength);
          byte[] record = new byte[len];
          in.readFully(record);
          ret[i] = record;
        }
        return ret;
      }
      else {
        return (byte[][]) obj;
      }
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public byte[][] fromUnsafeToKeys(Object obj) {
    try {
      if (obj instanceof Long) {
        long address = (long) obj;

        address *= -1;
        byte[] lenBuffer = new byte[8];
        lenBuffer[0] = unsafe.getByte(address + 0);
        lenBuffer[1] = unsafe.getByte(address + 1);
        lenBuffer[2] = unsafe.getByte(address + 2);
        lenBuffer[3] = unsafe.getByte(address + 3);
        lenBuffer[4] = unsafe.getByte(address + 4);
        lenBuffer[5] = unsafe.getByte(address + 5);
        lenBuffer[6] = unsafe.getByte(address + 6);
        lenBuffer[7] = unsafe.getByte(address + 7);
        ByteArrayInputStream bytesIn = new ByteArrayInputStream(lenBuffer);
        DataInputStream in = new DataInputStream(bytesIn);
        int count = in.readInt();
        int origLen = in.readInt();
        byte[] bytes = new byte[count];
        for (int i = 0; i < count; i++) {
          bytes[i] = unsafe.getByte(address + i + 8);
        }

        if (origLen != -1) {
          LZ4Factory factory = LZ4Factory.fastestInstance();

          LZ4FastDecompressor decompressor = factory.fastDecompressor();
          byte[] restored = new byte[origLen];
          decompressor.decompress(bytes, 0, restored, 0, origLen);
          bytes = restored;
        }

        in = new DataInputStream(new ByteArrayInputStream(bytes));
        DataUtil.ResultLength resultLength = new DataUtil.ResultLength();
        //in.readInt(); //byte count
        //in.readInt(); //orig len
        byte[][] ret = new byte[in.readInt()][];
        for (int i = 0; i < ret.length; i++) {
          int len = (int) DataUtil.readVLong(in, resultLength);
          byte[] record = new byte[len];
          in.readFully(record);
          ret[i] = record;
        }
        return ret;
      }
      else {
        return (byte[][]) obj;
      }
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public void freeUnsafeIds(Object obj) {
    if (obj instanceof Long) {
      long address = (long) obj;
      if (address == 0) {
        return;
      }
      unsafe.freeMemory(-1 * address);
    }
  }


//todo: snapshot queue periodically

//todo: implement restoreSnapshot()

  public static class LogRequest {
    private byte[] buffer;
    private CountDownLatch latch = new CountDownLatch(1);
    private List<byte[]> buffers;

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "EI_EXPOSE_REP", justification = "copying the returned data is too slow")
    public byte[] getBuffer() {
      return buffer;
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "EI_EXPOSE_REP2", justification = "copying the passed in data is too slow")
    @SuppressWarnings("PMD.ArrayIsStoredDirectly") //copying the passed in data is too slow
    public void setBuffer(byte[] buffer) {
      this.buffer = buffer;
    }

    public CountDownLatch getLatch() {
      return latch;
    }

    public void setLatch(CountDownLatch latch) {
      this.latch = latch;
    }

    public void setBuffers(List<byte[]> buffers) {
      this.buffers = buffers;
    }

    public List<byte[]> getBuffers() {
      return buffers;
    }
  }

  private static Set<String> priorityCommands = new HashSet<>();

  static {
    priorityCommands.add("getSchema");
    priorityCommands.add("synchSchema");
    priorityCommands.add("updateSchema");
    priorityCommands.add("getConfig");
    priorityCommands.add("healthCheckPriority");
    priorityCommands.add("getDbNames");
    priorityCommands.add("updateServersConfig");
  }

  public static class Response {
    private byte[] bytes;
    private Exception exception;

    public Response(Exception e) {
      this.exception = e;
    }

    public Response(byte[] bytes) {
      this.bytes = bytes;
    }

    public Exception getException() {
      return exception;
    }

    public byte[] getBytes() {
      return bytes;
    }
  }

  public List<Response> handleCommands(List<NettyServer.Request> requests, final boolean replayedCommand, boolean enableQueuing) throws IOException {
    List<Response> retList = new ArrayList<>();
    try {
      if (shutdown) {
        throw new DatabaseException("Shutdown in progress");
      }
      LogRequest logRequest = logManager.logRequests(requests, enableQueuing);

      List<Future<byte[]>> futures = new ArrayList<>();
      for (final NettyServer.Request request : requests) {
        futures.add(executor.submit(new Callable<byte[]>() {
          @Override
          public byte[] call() throws Exception {
            String command = request.getCommand();
            byte[] body = request.getBody();

            int pos = command.indexOf(':');
            int pos2 = command.indexOf(':', pos + 1);
            String methodStr = null;
            if (pos2 == -1) {
              methodStr = command.substring(pos + 1);
            }
            else {
              methodStr = command.substring(pos + 1, pos2);
            }
            byte[] ret = null;

            if (!replayedCommand && !isRunning.get() && !priorityCommands.contains(methodStr)) {
              throw new DatabaseException("Server not running: command=" + command);
            }

            Method method = DatabaseServer.class.getMethod(methodStr, String.class, byte[].class, boolean.class);
            try {
              ret = (byte[]) method.invoke(DatabaseServer.this, command, body, replayedCommand);
            }
            catch (Exception e) {
              boolean schemaOutOfSync = false;
              if (SchemaOutOfSyncException.class.isAssignableFrom(e.getClass())) {
                schemaOutOfSync = true;
              }
              else {
                if (e.getMessage() != null && e.getMessage().contains("SchemaOutOfSyncException")) {
                  schemaOutOfSync = true;
                }
                else {
                  int index = ExceptionUtils.indexOfThrowable(e, SchemaOutOfSyncException.class);
                  if (-1 != index) {
                    schemaOutOfSync = true;
                  }
                }
              }

              if (!schemaOutOfSync) {
                logger.error("Error processing request", e);
              }
              else {
                logger.info("Schema out of sync: schemaVersion=" + common.getSchemaVersion());
              }
              throw new DatabaseException(e);
            }
            return ret;
          }
        }));
      }
      for (Future<byte[]> future : futures) {
        commandCount.incrementAndGet();
        try {
          retList.add(new Response(future.get()));
        }
        catch (Exception e) {
          retList.add(new Response(e));
        }
      }
      if (logRequest != null) {
        logRequest.latch.await();
      }
      return retList;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }


  public byte[] handleCommand(String command, byte[] body, boolean replayedCommand, boolean enableQueuing) {
    try {
      if (shutdown) {
        throw new DatabaseException("Shutdown in progress");
      }
      int pos = command.indexOf(':');
      int pos2 = command.indexOf(':', pos + 1);
      String methodStr = null;
      if (pos2 == -1) {
        methodStr = command.substring(pos + 1);
      }
      else {
        methodStr = command.substring(pos + 1, pos2);
      }

      LogRequest request = null;
      request = logManager.logRequest(command, body, enableQueuing, methodStr, request);

      byte[] ret = null;

      if (!replayedCommand && !isRunning.get() && !priorityCommands.contains(methodStr)) {
        throw new DatabaseException("Server not running: command=" + command);
      }

      Method method = getClass().getMethod(methodStr, String.class, byte[].class, boolean.class);
      try {
        ret = (byte[]) method.invoke(this, command, body, replayedCommand);
      }
      catch (Exception e) {
        boolean schemaOutOfSync = false;
        int index = ExceptionUtils.indexOfThrowable(e, SchemaOutOfSyncException.class);
        if (-1 != index) {
          schemaOutOfSync = true;
        }
        else if (e.getMessage() != null && e.getMessage().contains("SchemaOutOfSyncException")) {
          schemaOutOfSync = true;
        }

        if (!schemaOutOfSync) {
          logger.error("Error processing request", e);
        }
        throw new DatabaseException(e);
      }

      commandCount.incrementAndGet();
      if (request != null) {
        request.latch.await();
      }

      return ret;
    }
    catch (InterruptedException | NoSuchMethodException e) {
      throw new DatabaseException(e);
    }
  }


  public void purge(String dbName) {
    if (null != getIndices(dbName) && null != getIndices(dbName).getIndices()) {
      for (Map.Entry<String, ConcurrentHashMap<String, Index>> table : getIndices(dbName).getIndices().entrySet()) {
        for (Map.Entry<String, Index> indexEntry : table.getValue().entrySet()) {
          indexEntry.getValue().clear();
        }
      }
    }
  }

  public static String format8601(Date date) throws ParseException {
    DateFormat df1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    return df1.format(date);
  }

  public byte[] healthCheck(String command, byte[] body, boolean replayedCommand) {

    try {
      return "{\"status\" : \"ok\"}".getBytes("utf-8");
    }
    catch (UnsupportedEncodingException e) {
      throw new DatabaseException(e);
    }
  }

  public byte[] healthCheckPriority(String command, byte[] body, boolean replayedCommand) {

    try {
      return "{\"status\" : \"ok\"}".getBytes("utf-8");
    }
    catch (UnsupportedEncodingException e) {
      throw new DatabaseException(e);
    }
  }

  public byte[] reconfigureCluster(String command, byte[] body, boolean replayedCommand) {
    ServersConfig oldConfig = common.getServersConfig();
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytesOut);
    try {
      DataUtil.writeVLong(out, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
      File file = new File(System.getProperty("user.dir"), "config/config-" + getCluster() + ".json");
      if (!file.exists()) {
        file = new File(System.getProperty("user.dir"), "db/src/main/resources/config/config-" + getCluster() + ".json");
      }
      String configStr = StreamUtils.inputStreamToString(new BufferedInputStream(new FileInputStream(file)));
      logger.info("Config: " + configStr);
      JsonDict config = new JsonDict(configStr);
      config = config.getDict("database");

      validateLicense(config);

      boolean isInternal = false;
      if (config.hasKey("clientIsPrivate")) {
        isInternal = config.getBoolean("clientIsPrivate");
      }
      DatabaseServer.ServersConfig newConfig = new DatabaseServer.ServersConfig(config.getArray("shards"), config.getInt("replicationFactor"), isInternal);

      common.setServersConfig(newConfig);

      common.saveSchema(getDataDir());

      pushSchema();

      Shard[] oldShards = oldConfig.getShards();
      Shard[] newShards = newConfig.getShards();

      int count = newShards.length - oldShards.length;
      out.writeInt(count);

      out.close();
      return bytesOut.toByteArray();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public byte[] getConfig(String command, byte[] body, boolean replayedCommand) {
    try {
      return common.serializeConfig();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public byte[] getSchema(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");

    try {
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytesOut);
      DataUtil.writeVLong(out, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
      common.serializeSchema(out);
      out.close();

      return bytesOut.toByteArray();
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }


  public static AtomicInteger blockCount = new AtomicInteger();

  public byte[] block(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    logger.info("called block");
    blockCount.incrementAndGet();

    try {
      Thread.sleep(1000000);
    }
    catch (InterruptedException e) {
      throw new DatabaseException(e);
    }
    return body;
  }

  public static AtomicInteger echoCount = new AtomicInteger(0);
  public static AtomicInteger echo2Count = new AtomicInteger(0);

  public byte[] echo(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    logger.info("called echo");
    echoCount.set(Integer.valueOf(parts[5]));
    return body;
  }

  public byte[] echo2(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    logger.info("called echo2");
    if (parts.length == 6) {
      if (echoCount.get() != Integer.valueOf(parts[5])) {
        throw new DatabaseException("InvalidState");
      }
    }
    echo2Count.set(Integer.valueOf(parts[5]));
    return body;
  }

  public byte[] reserveNextIdFromReplica(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    common.getSchemaReadLock(dbName).lock();
    try {
      int schemaVersion = Integer.valueOf(parts[3]);
      if (schemaVersion < getSchemaVersion()) {
        throw new SchemaOutOfSyncException("currVer:" + common.getSchemaVersion() + ":");
      }
      long highestId = Long.valueOf(parts[4]);
      long id = -1;
      synchronized (nextRecordId) {
        if (highestId > nextRecordId.get()) {
          nextRecordId.set(highestId);
        }
        id = nextRecordId.getAndIncrement();
      }
      return String.valueOf(id).getBytes("utf-8");
    }
    catch (UnsupportedEncodingException e) {
      throw new DatabaseException(e);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] noOp(String command, byte[] body, boolean replayedCommand) {
    return null;
  }


  private final Object nextIdLock = new Object();

  public byte[] allocateRecordIds(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    common.getSchemaReadLock(dbName).lock();
    try {
      logger.info("Requesting next record id - begin");
      int schemaVersion = Integer.valueOf(parts[3]);
      if (schemaVersion < getSchemaVersion()) {
        throw new SchemaOutOfSyncException("currVer:" + common.getSchemaVersion() + ":");
      }
      //todo: replicate file to other replicasz
      long nextId;
      long maxId;
      synchronized (nextIdLock) {
        File file = new File(dataDir, "nextRecordId/" + getShard() + "/" + getReplica() + "/nextRecorId.txt");
        file.getParentFile().mkdirs();
        if (!file.exists()) {
          nextId = 1;
          maxId = 100000;
        }
        else {
          try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)))) {
            maxId = Long.valueOf(reader.readLine());
            nextId = maxId + 1;
            maxId += 100000;
          }
          file.delete();
        }
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))) {
          writer.write(String.valueOf(maxId));
        }
      }

      logger.info("Requesting next record id - finished: nextId=" + nextId + ", maxId=" + maxId);

      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytesOut);
      DataUtil.writeVLong(out, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
      out.writeLong(nextId);
      out.writeLong(maxId);
      out.close();
      return bytesOut.toByteArray();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }


  public byte[] deleteIndexEntryByKey(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    common.getSchemaReadLock(dbName).lock();
    try {
      return updateManager.deleteIndexEntryByKey(command, body, replayedCommand);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] commit(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    common.getSchemaReadLock(dbName).lock();
    try {
      return updateManager.commit(command, body, replayedCommand);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] rollback(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    common.getSchemaReadLock(dbName).lock();
    try {
      return updateManager.rollback(command, body, replayedCommand);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] insertIndexEntryByKey(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    common.getSchemaReadLock(dbName).lock();
    try {
      return updateManager.insertIndexEntryByKey(command, body, replayedCommand);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] insertIndexEntryByKeyWithRecord(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    common.getSchemaReadLock(dbName).lock();
    try {
      return updateManager.insertIndexEntryByKeyWithRecord(command, body, replayedCommand);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] batchInsertIndexEntryByKey(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    common.getSchemaReadLock(dbName).lock();
    try {
      return updateManager.batchInsertIndexEntryByKey(command, body, replayedCommand);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] batchInsertIndexEntryByKeyWithRecord(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    common.getSchemaReadLock(dbName).lock();
    try {
      return updateManager.batchInsertIndexEntryByKeyWithRecord(command, body, replayedCommand);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] abortTransaction(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    common.getSchemaReadLock(dbName).lock();
    try {
      return transactionManager.abortTransaction(command, body);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] updateRecord(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    common.getSchemaReadLock(dbName).lock();
    try {
      return updateManager.updateRecord(command, body, replayedCommand);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  class Entry {
    private long id;
    private Object[] key;
    private CountDownLatch latch = new CountDownLatch(1);

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "EI_EXPOSE_REP2", justification = "copying the passed in data is too slow")
    @SuppressWarnings("PMD.ArrayIsStoredDirectly") //copying the passed in data is too slow
    public Entry(long id, Object[] key) {
      this.id = id;
      this.key = key;
    }
  }

  public byte[] deleteRecord(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    common.getSchemaReadLock(dbName).lock();
    try {
      return updateManager.deleteRecord(command, body, replayedCommand);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] deleteIndexEntry(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    common.getSchemaReadLock(dbName).lock();
    try {
      return updateManager.deleteIndexEntry(command, body, replayedCommand);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public Record evaluateRecordForQuery(
      TableSchema tableSchema, Record record,
      ExpressionImpl whereClause, ParameterHandler parms) {

    boolean pass = (Boolean) whereClause.evaluateSingleRecord(new TableSchema[]{tableSchema}, new Record[]{record}, parms);
    if (pass) {
      return record;
    }
    return null;
  }

  public byte[] truncateTable(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    common.getSchemaReadLock(dbName).lock();
    try {
      return updateManager.truncateTable(command, body, replayedCommand);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] countRecords(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    common.getSchemaReadLock(dbName).lock();
    try {
      return readManager.countRecords(command, body);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] batchIndexLookup(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    common.getSchemaReadLock(dbName).lock();
    try {
      return readManager.batchIndexLookup(command, body);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] indexLookup(String command, byte[] body, boolean replayedCommand) {
    try {
      DataInputStream in = new DataInputStream(new ByteArrayInputStream(body));
      String dbName = in.readUTF();
      common.getSchemaReadLock(dbName).lock();
      try {
        return readManager.indexLookup(dbName, in);
      }
      finally {
        common.getSchemaReadLock(dbName).unlock();
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }


  public byte[] closeResultSet(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    common.getSchemaReadLock(dbName).lock();
    try {
      return readManager.closeResultSet(command, body, replayedCommand);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] serverSelectDelete(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    common.getSchemaReadLock(dbName).lock();
    try {
      return readManager.serverSelectDelete(command, body, replayedCommand);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] serverSelect(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    common.getSchemaReadLock(dbName).lock();
    try {
      return readManager.serverSelect(command, body);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] indexLookupExpression(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    common.getSchemaReadLock(dbName).lock();
    try {
      return readManager.indexLookupExpression(command, body);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] evaluateCounter(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    common.getSchemaReadLock(dbName).lock();
    try {
      return readManager.evaluateCounter(command, body);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] getIndexCounts(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    common.getSchemaReadLock(dbName).lock();
    try {
      return repartitioner.getIndexCounts(command, body);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] isRepartitioningRecordsByIdComplete(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    common.getSchemaReadLock(dbName).lock();
    try {
      return repartitioner.isRepartitioningRecordsByIdComplete(command, body);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] isRepartitioningComplete(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    common.getSchemaReadLock(dbName).lock();
    try {
      return repartitioner.isRepartitioningComplete(command, body);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] isDeletingComplete(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    common.getSchemaReadLock(dbName).lock();
    try {
      return repartitioner.isDeletingComplete(command, body);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] notifyRepartitioningComplete(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    common.getSchemaReadLock(dbName).lock();
    try {
      return repartitioner.notifyRepartitioningComplete(command, body);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] notifyDeletingComplete(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    common.getSchemaReadLock(dbName).lock();
    try {
      return repartitioner.notifyDeletingComplete(command, body);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] beginRebalance(String command, byte[] body, boolean replayedCommand) {

    //schema lock below
    return repartitioner.beginRebalance(command, body);
  }

  public byte[] getKeyAtOffset(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    common.getSchemaReadLock(dbName).lock();
    try {
      return repartitioner.getKeyAtOffset(command, body);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] getPartitionSize(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    common.getSchemaReadLock(dbName).lock();
    try {
      return repartitioner.getPartitionSize(command, body);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] doRebalanceOrderedIndex(final String command, final byte[] body, boolean replayedCommand) {
    return repartitioner.doRebalanceOrderedIndex(command, body);
  }

  public byte[] rebalanceOrderedIndex(String command, byte[] body, boolean replayedCommand) {
    //schema lock below
    return repartitioner.rebalanceOrderedIndex(command, body);
  }

  public byte[] moveIndexEntries(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    common.getSchemaReadLock(dbName).lock();
    try {
      return repartitioner.moveIndexEntries(command, body);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] doDeleteMovedIndexEntries(final String command, final byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    common.getSchemaReadLock(dbName).lock();
    try {
      return repartitioner.doDeleteMovedIndexEntries(command, body);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] deleteMovedIndexEntries(final String command, final byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    common.getSchemaReadLock(dbName).lock();
    try {
      return repartitioner.deleteMovedIndexEntries(command, body);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] listIndexFiles(String command, byte[] body, boolean replayedCommand) {
    return null;
  }

  public byte[] createTable(String command, byte[] body, boolean replayedCommand) {
    return schemaManager.createTable(command, body, replayedCommand);
  }

  public byte[] createTableSlave(String command, byte[] body, boolean replayedCommand) {
    return schemaManager.createTableSlave(command, body, replayedCommand);
  }

  public byte[] dropTable(String command, byte[] body, boolean replayedCommand) {
    return schemaManager.dropTable(command, body, replayedCommand);
  }

  public byte[] createDatabase(String command, byte[] body, boolean replayedCommand) {
    return schemaManager.createDatabase(command, body, replayedCommand);
  }

  public enum ResultType {
    records(0),
    integer(1),
    bool(2),
    schema(3);

    private final int type;

    ResultType(int type) {
      this.type = type;
    }

    public int getType() {
      return type;
    }

  }

  public byte[] addColumn(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    common.getSchemaWriteLock(dbName).lock();
    try {
      return schemaManager.addColumn(command, body);
    }
    finally {
      common.getSchemaWriteLock(dbName).unlock();
    }
  }

  public byte[] dropColumn(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    common.getSchemaWriteLock(dbName).lock();
    try {
      return schemaManager.dropColumn(command, body);
    }
    finally {
      common.getSchemaWriteLock(dbName).unlock();
    }
  }

  public byte[] dropIndexSlave(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    common.getSchemaWriteLock(dbName).lock();
    try {
      return schemaManager.dropIndexSlave(command, body);
    }
    finally {
      common.getSchemaWriteLock(dbName).unlock();
    }
  }

  public byte[] dropIndex(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    common.getSchemaWriteLock(dbName).lock();
    try {
      return schemaManager.dropIndex(command, body);
    }
    finally {
      common.getSchemaWriteLock(dbName).unlock();
    }
  }

  public byte[] createIndexSlave(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    common.getSchemaWriteLock(dbName).lock();
    try {
      return schemaManager.createIndexSlave(command, body);
    }
    finally {
      common.getSchemaWriteLock(dbName).unlock();
    }
  }

  public byte[] doPopulateIndex(final String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    common.getSchemaReadLock(dbName).lock();
    try {
      return updateManager.doPopulateIndex(command, body);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] populateIndex(final String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    common.getSchemaReadLock(dbName).lock();
    try {
      return updateManager.populateIndex(command, body);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] createIndex(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    String dbName = parts[4];
    common.getSchemaWriteLock(dbName).lock();
    try {
      return schemaManager.createIndex(command, body, replayedCommand);
    }
    finally {
      common.getSchemaWriteLock(dbName).unlock();
    }
  }

  public byte[] expirePreparedStatement(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    long preparedId = Long.valueOf(parts[5]);
    readManager.expirePreparedStatement(preparedId);
    return null;
  }

  private class StatsMonitor implements Runnable {
    @Override
    public void run() {
      while (true) {
        try {
          Thread.sleep(30000);
          OSStats stats = doGetOSStats();
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
}
