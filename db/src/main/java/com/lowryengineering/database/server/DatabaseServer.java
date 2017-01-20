package com.lowryengineering.database.server;

import com.lowryengineering.database.client.DatabaseClient;
import com.lowryengineering.database.common.DatabaseCommon;
import com.lowryengineering.database.common.Record;
import com.lowryengineering.database.common.SchemaOutOfSyncException;
import com.lowryengineering.database.index.Index;
import com.lowryengineering.database.index.Indices;
import com.lowryengineering.database.index.Repartitioner;
import com.lowryengineering.database.jdbcdriver.ParameterHandler;
import com.lowryengineering.database.query.DatabaseException;
import com.lowryengineering.database.query.impl.ExpressionImpl;
import com.lowryengineering.database.schema.TableSchema;
import com.lowryengineering.database.util.DataUtil;
import com.lowryengineering.database.util.JsonArray;
import com.lowryengineering.database.util.JsonDict;
import com.lowryengineering.database.util.StreamUtils;
import com.lowryengineering.research.socket.NettyServer;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
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

  private static Logger logger = LoggerFactory.getLogger(DatabaseServer.class);

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
      DatabaseClient client = new DatabaseClient(masterAddress, masterPort, false);
      client.setCommon(common);
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
      snapshotManager.runSnapshot(dbName);
    }
  }

  public void recoverFromSnapshot() throws Exception {
    for (String dbName : getDbNames(dataDir)) {
      snapshotManager.recoverFromSnapshot(dbName);
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

  public void setConfig(
      final JsonDict config, String cluster, String host, int port, AtomicBoolean isRunning) {
    setConfig(config, cluster, host, port, false, isRunning, false);
  }

  public void setConfig(
      final JsonDict config, String cluster, String host, int port, AtomicBoolean isRunning, boolean skipLicense) {
    setConfig(config, cluster, host, port, false, isRunning, skipLicense);
  }

  public void setConfig(
      final JsonDict config, String cluster, String host, int port,
      boolean unitTest, AtomicBoolean isRunning) {
    setConfig(config, cluster, host, port, unitTest, isRunning, false);
  }

  public void setConfig(
      final JsonDict config, String cluster, String host, int port,
      boolean unitTest, AtomicBoolean isRunning, boolean skipLicense) {
    this.isRunning = isRunning;
    this.config = config;
    this.cluster = cluster;
    this.host = host;
    this.port = port;

    logger.info("config=" + config.toString());
    JsonDict databaseDict = config.getDict("database");
    this.dataDir = databaseDict.getString("dataDirectory");
    this.dataDir = dataDir.replace("$HOME", System.getProperty("user.home"));
    JsonArray shards = databaseDict.getArray("shards");
    JsonDict firstServer = shards.getDict(0).getArray("replicas").getDict(0);
    ServersConfig serversConfig = null;
    executor = new ThreadPoolExecutor(256, 256, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
    if (!skipLicense) {
      validateLicense(config);
    }

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

    this.masterAddress = firstServer.getString("privateAddress");
    this.masterPort = firstServer.getInt("port");

    if (firstServer.getString("privateAddress").equals(host) && firstServer.getLong("port") == port) {
      this.shard = 0;
      this.replica = 0;
      common.setShard(0);
      common.setReplica(0);

    }

    boolean isInternal = false;
    if (config.hasKey("clientIsPrivate")) {
      isInternal = config.getBoolean("clientIsPrivate");
    }
    serversConfig = new ServersConfig(shards, replicationFactor, isInternal);
    this.replica = serversConfig.getThisReplica(host, port);

    common.setShard(serversConfig.getThisShard(host, port));
    common.setReplica(this.replica);
    this.shard = common.getShard();
    this.shardCount = serversConfig.getShardCount();

    common.setServersConfig(serversConfig);

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


//    File file = new File(dataDir, "queue/" + shard + "/" + replica);
//    queue = new Queue(file.getAbsolutePath(), "request-log", 0);
//    Thread logThread = new Thread(queue);
//    logThread.start();

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

            String max = config.getDict("database").getString("maxProcessMemory");
            if (max == null) {
              logger.info("Max process memory not set in config. Not enforcing max memory");
              continue;
            }
            Double totalGig = null;
            Double resGig = null;
            if (isMac()) {
              ProcessBuilder builder = new ProcessBuilder().command("sysctl", "hw.memsize");
              Process p = builder.start();
              BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
              String line = in.readLine();
              p.waitFor();
              String[] parts = line.split(" ");
              String memStr = parts[1];
              totalGig =  getMemValue(memStr);

              builder = new ProcessBuilder().command("top", "-l", "1", "-pid", String.valueOf(pid));
              p = builder.start();
              in = new BufferedReader(new InputStreamReader(p.getInputStream()));
              String lastLine = null;
              String secondToLastLine = null;
              while (true) {
                line = in.readLine();
                if (line == null) {
                  break;
                }
                secondToLastLine = lastLine;
                lastLine = line;
              }
              p.waitFor();

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
              totalGig =  getMemValue(line);

              builder = new ProcessBuilder().command("top", "-b", "-n", "1", "-o", "RES", "-p", String.valueOf(pid));
              p = builder.start();
              in = new BufferedReader(new InputStreamReader(p.getInputStream()));
              String lastLine = null;
              String secondToLastLine = null;
              while (true) {
                line = in.readLine();
                if (line == null) {
                  break;
                }
                secondToLastLine = lastLine;
                lastLine = line;
              }
              p.waitFor();

              String[] headerParts = secondToLastLine.split("\\s+");
              String[] parts = lastLine.split("\\s+");
              for (int i = 0; i < headerParts.length; i++) {
                if (headerParts[i].toLowerCase().trim().equals("res")) {
                  String memStr = parts[i];
                  resGig = getMemValue(memStr);
                }
              }
            }
            if (totalGig == null || resGig == null) {
              logger.error("Unable to obtain os memory info: totalGig=" + totalGig + ", residentGig=" + resGig);
            }
            else {
              if (max.contains("%")) {
                max = max.replaceAll("\\%", "").trim();
                double maxPercent = Double.valueOf(max);
                double actualPercent = resGig  / totalGig * 100d;
                if (actualPercent > maxPercent) {
                  logger.info(String.format("Above max memory threshold: totalGig=%.2f, residentGig=%.2f, percentMax=%.2f, percentActual=%.2f", totalGig, resGig, maxPercent, actualPercent));
                  aboveMemoryThreshold.set(true);
                }
                else {
                  logger.info(String.format("Not above max memory threshold: totalGig=%.2f, residentGig=%.2f, percentMax=%.2f, percentActual=%.2f", totalGig, resGig, maxPercent, actualPercent));
                  aboveMemoryThreshold.set(false);
                }
              }
              else {
                double maxGig = getMemValue(max);
                if (resGig > maxGig) {
                  logger.info(String.format("Above max memory threshold: totalGig=%.2f, residentGig=%.2f, maxGig=%.2f", totalGig, resGig, maxGig));
                  aboveMemoryThreshold.set(true);
                }
                else {
                  logger.info(String.format("Not above max memory threshold: totalGig=%.2f, residentGig=%.2f, maxGig=%.2f", totalGig, resGig, maxGig));
                  aboveMemoryThreshold.set(false);
                }
              }
            }
          }
          catch (Exception e) {
            logger.error("Error in memory check thread", e);
          }
        }

      }
    });
    thread.start();
  }

  public static double getMemValue(String memStr) {
    int qualifierPos = memStr.toLowerCase().indexOf("m");
    if (qualifierPos == -1) {
      qualifierPos = memStr.toLowerCase().indexOf("g");
      if (qualifierPos == -1) {
        qualifierPos = memStr.toLowerCase().indexOf("t");
        if (qualifierPos == -1) {
          qualifierPos = memStr.toLowerCase().indexOf("b");
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


  public long toUnsafeFromIds(long[] ids) throws IOException {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytesOut);
    DataUtil.ResultLength resultLength = new DataUtil.ResultLength();
    out.writeInt(0);
    out.writeInt(ids.length);
    for (long id : ids) {
      DataUtil.writeVLong(out, id, resultLength);
    }
    out.close();
    byte[] bytes = bytesOut.toByteArray();
    bytesOut = new ByteArrayOutputStream();
    out = new DataOutputStream(bytesOut);
    out.writeInt(bytes.length);
    out.close();
    byte[] lenBuffer = bytesOut.toByteArray();

    System.arraycopy(lenBuffer, 0, bytes, 0, lenBuffer.length);

    long address = unsafe.allocateMemory(bytes.length);
    for (int i = 0; i < bytes.length; i++) {
      unsafe.putByte(address + i, bytes[i]);
    }
    return -1 * address;
  }

  public long toUnsafeFromRecords(byte[][] records) {
    try {
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytesOut);
      DataUtil.ResultLength resultLength = new DataUtil.ResultLength();
      out.writeInt(0);
      out.writeInt(records.length);
      for (byte[] record : records) {
        DataUtil.writeVLong(out, record.length, resultLength);
        out.write(record);
      }
      out.close();
      byte[] bytes = bytesOut.toByteArray();
      bytesOut = new ByteArrayOutputStream();
      out = new DataOutputStream(bytesOut);
      out.writeInt(bytes.length);
      out.close();
      byte[] lenBuffer = bytesOut.toByteArray();

      System.arraycopy(lenBuffer, 0, bytes, 0, lenBuffer.length);

      if (bytes.length > 1000000000) {
        throw new DatabaseException("Invalid allocation: size=" + bytes.length);
      }
      long address = unsafe.allocateMemory(bytes.length);
      for (int i = 0; i < bytes.length; i++) {
        unsafe.putByte(address + i, bytes[i]);
      }
      return -1 * address;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public long toUnsafeFromKeys(byte[][] records) {
    try {
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytesOut);
      DataUtil.ResultLength resultLength = new DataUtil.ResultLength();
      out.writeInt(0);
      out.writeInt(records.length);
      for (byte[] record : records) {
        DataUtil.writeVLong(out, record.length, resultLength);
        out.write(record);
      }
      out.close();
      byte[] bytes = bytesOut.toByteArray();
      bytesOut = new ByteArrayOutputStream();
      out = new DataOutputStream(bytesOut);
      out.writeInt(bytes.length);
      out.close();
      byte[] lenBuffer = bytesOut.toByteArray();

      System.arraycopy(lenBuffer, 0, bytes, 0, lenBuffer.length);

      if (bytes.length > 1000000000) {
        throw new DatabaseException("Invalid allocation: size=" + bytes.length);
      }

      long address = unsafe.allocateMemory(bytes.length);
      for (int i = 0; i < bytes.length; i++) {
        unsafe.putByte(address + i, bytes[i]);
      }
      return -1 * address;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public long[] fromUnsafeToIds(long address) throws IOException {
    address *= -1;
    byte[] lenBuffer = new byte[4];
    lenBuffer[0] = unsafe.getByte(address + 0);
    lenBuffer[1] = unsafe.getByte(address + 1);
    lenBuffer[2] = unsafe.getByte(address + 2);
    lenBuffer[3] = unsafe.getByte(address + 3);
    ByteArrayInputStream bytesIn = new ByteArrayInputStream(lenBuffer);
    DataInputStream in = new DataInputStream(bytesIn);
    int count = in.readInt();
    byte[] bytes = new byte[count];
    for (int i = 0; i < count; i++) {
      bytes[i] = unsafe.getByte(address + i);
    }
    in = new DataInputStream(new ByteArrayInputStream(bytes));
    DataUtil.ResultLength resultLength = new DataUtil.ResultLength();
    in.readInt(); //byte count
    long[] ret = new long[in.readInt()];
    for (int i = 0; i < ret.length; i++) {
      ret[i] = DataUtil.readVLong(in, resultLength);
    }
    return ret;
  }

  public byte[][] fromUnsafeToRecords(long address) {
    try {
      address *= -1;
      byte[] lenBuffer = new byte[4];
      lenBuffer[0] = unsafe.getByte(address + 0);
      lenBuffer[1] = unsafe.getByte(address + 1);
      lenBuffer[2] = unsafe.getByte(address + 2);
      lenBuffer[3] = unsafe.getByte(address + 3);
      ByteArrayInputStream bytesIn = new ByteArrayInputStream(lenBuffer);
      DataInputStream in = new DataInputStream(bytesIn);
      int count = in.readInt();
      byte[] bytes = new byte[count];
      for (int i = 0; i < count; i++) {
        bytes[i] = unsafe.getByte(address + i);
      }
      in = new DataInputStream(new ByteArrayInputStream(bytes));
      DataUtil.ResultLength resultLength = new DataUtil.ResultLength();
      in.readInt(); //byte count
      byte[][] ret = new byte[in.readInt()][];
      for (int i = 0; i < ret.length; i++) {
        int len = (int) DataUtil.readVLong(in, resultLength);
        byte[] record = new byte[len];
        in.readFully(record);
        ret[i] = record;
      }
      return ret;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public byte[][] fromUnsafeToKeys(long address) {
    try {
      address *= -1;
      byte[] lenBuffer = new byte[4];
      lenBuffer[0] = unsafe.getByte(address + 0);
      lenBuffer[1] = unsafe.getByte(address + 1);
      lenBuffer[2] = unsafe.getByte(address + 2);
      lenBuffer[3] = unsafe.getByte(address + 3);
      ByteArrayInputStream bytesIn = new ByteArrayInputStream(lenBuffer);
      DataInputStream in = new DataInputStream(bytesIn);
      int count = in.readInt();
      byte[] bytes = new byte[count];
      for (int i = 0; i < count; i++) {
        bytes[i] = unsafe.getByte(address + i);
      }
      in = new DataInputStream(new ByteArrayInputStream(bytes));
      DataUtil.ResultLength resultLength = new DataUtil.ResultLength();
      in.readInt(); //byte count
      byte[][] ret = new byte[in.readInt()][];
      for (int i = 0; i < ret.length; i++) {
        int len = (int) DataUtil.readVLong(in, resultLength);
        byte[] record = new byte[len];
        in.readFully(record);
        ret[i] = record;
      }
      return ret;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public void freeUnsafeIds(long address) {
    unsafe.freeMemory(-1 * address);
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
    String[] parts = command.split(":");
    String dbName = parts[4];
    common.getSchemaReadLock(dbName).lock();
    try {
      return readManager.indexLookup(command, body);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
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
}
