package com.sonicbase.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.*;
import com.sonicbase.common.Record;
import com.sonicbase.index.Index;
import com.sonicbase.index.Indices;
import com.sonicbase.index.Repartitioner;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.procedure.*;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.query.impl.ExpressionImpl;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.socket.DeadServerException;
import com.sonicbase.util.DateUtils;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.execute.Execute;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.ReversedLinesFileReader;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.giraph.utils.Varint;
import org.jetbrains.annotations.Nullable;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import sun.misc.Unsafe;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import javax.net.ssl.*;
import java.io.*;
import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.*;
import java.util.Calendar;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.sonicbase.common.DatabaseCommon.sortSchemaFiles;
import static com.sonicbase.common.MemUtil.getMemValue;
import static java.util.Calendar.HOUR;
import static jdk.nashorn.internal.parser.DateParser.DAY;
import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;


/**
 * User: lowryda
 * Date: 12/27/13
 * Time: 4:39 PM
 */
public class DatabaseServer {

  public static final String SONICBASE_SYS_DB_STR = "_sonicbase_sys";
  public static Object deathOverrideMutex = new Object();
  public static boolean[][] deathOverride;
  private Logger logger;
  private static org.apache.log4j.Logger errorLogger = org.apache.log4j.Logger.getLogger("com.sonicbase.errorLogger");
  private static org.apache.log4j.Logger clientErrorLogger = org.apache.log4j.Logger.getLogger("com.sonicbase.clientErrorLogger");

  public static final boolean USE_SNAPSHOT_MGR_OLD = true;
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
  private boolean compressRecords = false;
  private boolean useUnsafe;
  private String gclog;
  private String xmx;
  private String installDir;
  private boolean throttleInsert;
  private DeleteManager deleteManager;
  private ReentrantReadWriteLock batchLock = new ReentrantReadWriteLock();
  private ReentrantReadWriteLock.ReadLock batchReadLock = batchLock.readLock();
  private ReentrantReadWriteLock.WriteLock batchWriteLock = batchLock.writeLock();
  private AtomicInteger batchRepartCount = new AtomicInteger();
  private boolean usingMultipleReplicas = false;
  private Boolean disableNow = false;
  private boolean haveProLicense;
  private boolean overrideProLicense;
  private String logSlicePoint;
  private boolean isBackupComplete;
  private boolean isRestoreComplete;
  private Exception backupException;
  private Exception restoreException;
  private AWSClient awsClient;
  private boolean doingBackup;
  private boolean onlyQueueCommands;
  private boolean doingRestore;
  private ObjectNode backupConfig;
  private static Object restoreAwsMutex = new Object();
  private boolean dead;
  private boolean applyingQueuesAndInteractive;
  private MethodInvoker methodInvoker;
  private static AddressMap addressMap;
  private boolean shutdownMasterValidatorThread = false;
  private Thread masterLicenseValidatorThread;
  private String disableDate;
  private Boolean multipleLicenseServers;
  private BulkImportManager bulkImportManager;
  private StreamManager streamManager;
  private boolean waitingForServersToStart = false;

  static {
    addressMap = new AddressMap();
  }

  private boolean finishedRestoreFileCopy;
  private Connection sysConnection;
  private Thread streamsConsumerMonitorthread;
  private Thread statsThread;
  private ArrayList<Thread> masterMonitorThreadsForShards;
  private Thread masterMonitorThread;
  private Thread licenseValidatorThread;
  private Thread backupFileSystemThread;
  private Thread backupAWSThread;
  private Thread backupMainThread;
  private Thread restoreFileSystemThread;
  private Thread restoreAWSThread;
  private Thread restoreMainThread;
  private Thread memoryMonitorThread;
  private Thread reloadServerThread;
  private Connection connectionForStoredProcedure;
  private Timer isStreamingStartedTimer;
  private MonitorManager monitorManager;
  private OSStatsManager osStatsManager;
  private HttpServer httpServer;
  private Integer httpPort;
  private AtomicBoolean isRecovered;
  private Timer fixSchemaTimer;

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
  private ObjectNode config;
  private DatabaseClient.Replica role;
  private int shard;
  private int shardCount;
  private Map<String, Indices> indexes = new ConcurrentHashMap<>();
  private LongRunningCalls longRunningCommands;

  private String dataDir;
  private int replica;
  private int replicationFactor;
  private String masterAddress;
  private int masterPort;
  private UpdateManager updateManager;
  //private SnapshotManagerImpl snapshotManager;
  private SnapshotManager deltaManager;
  private TransactionManager transactionManager;
  private ReadManager readManager;
  private LogManager logManager;
  private SchemaManager schemaManager;
  private int cronIdentity = 0;

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

  public boolean isWaitingForServersToStart() {
    return waitingForServersToStart;
  }

  public void setWaitingForServersToStart(boolean waitingForServersToStart) {
    this.waitingForServersToStart = waitingForServersToStart;
  }

  public static boolean[][] getDeathOverride() {
    return deathOverride;
  }

  public void shutdown() {
    try {
      shutdown = true;

      if (httpServer != null) {
        httpServer.shutdown();
      }
      if (monitorManager != null) {
        monitorManager.shutdown();
      }
      if (isStreamingStartedTimer != null) {
        isStreamingStartedTimer.cancel();
      }
      if (reloadServerThread != null) {
        reloadServerThread.interrupt();
        reloadServerThread.join();
      }

      if (fixSchemaTimer != null) {
        fixSchemaTimer.cancel();
        fixSchemaTimer = null;
      }

      if (memoryMonitorThread != null) {
        memoryMonitorThread.interrupt();
        memoryMonitorThread.join();
      }

      if (restoreMainThread != null) {
        restoreMainThread.interrupt();
        restoreMainThread.join();
      }
      if (restoreAWSThread != null) {
        restoreAWSThread.interrupt();
        restoreAWSThread.join();
      }

      if (restoreFileSystemThread != null) {
        restoreFileSystemThread.interrupt();
        restoreFileSystemThread.join();
      }
      if (backupAWSThread != null) {
        backupAWSThread.interrupt();
        backupAWSThread.join();
      }

      if (backupFileSystemThread != null) {
        backupFileSystemThread.interrupt();
        backupFileSystemThread.join();
      }
      if (licenseValidatorThread != null) {
        licenseValidatorThread.interrupt();
        licenseValidatorThread.join();
      }
      if (masterMonitorThread != null) {
        masterMonitorThread.interrupt();
        masterMonitorThread.join();
      }

      if (masterMonitorThreadsForShards != null) {
        for (Thread thread : masterMonitorThreadsForShards) {
          thread.interrupt();
        }
        for (Thread thread : masterMonitorThreadsForShards) {
          thread.join();
        }
      }

      statsThread.interrupt();
      statsThread.join();

      if (streamsConsumerMonitorthread != null) {
        streamsConsumerMonitorthread.interrupt();
        streamsConsumerMonitorthread.join();
      }
      streamManager.shutdown();

      if (longRunningCommands != null) {
        longRunningCommands.shutdown();
      }
      shutdownDeathMonitor();
      shutdownRepartitioner();
      shutdownMasterLicenseValidator();
      deleteManager.shutdown();
      deltaManager.shutdown();
      logManager.shutdown();
      executor.shutdownNow();
      methodInvoker.shutdown();
      client.get().shutdown();
      if (sysConnection != null) {
        sysConnection.close();
      }
      if (connectionForStoredProcedure != null) {
        connectionForStoredProcedure.close();
      }
      readManager.shutdown();
      transactionManager.shutdown();
      updateManager.shutdown();
      bulkImportManager.shutdown();
      if (osStatsManager != null) {
        osStatsManager.shutdown();
      }

      executor.shutdownNow();
    }
    catch (Exception e) {
      throw new DatabaseException("Error shutting down DatabaseServer", e);
    }
  }

  public MethodInvoker getMethodInvoker() {
    return methodInvoker;
  }

  public org.apache.log4j.Logger getErrorLogger() {
    return errorLogger;
  }

  public org.apache.log4j.Logger getClientErrorLogger() {
    return clientErrorLogger;
  }

  public void setConfig(
      final ObjectNode config, String cluster, String host, int port, AtomicBoolean isRunning, AtomicBoolean isRecovered, String gclog, String xmx,
      boolean overrideProLicense) {
    setConfig(config, cluster, host, port, false, isRunning, isRecovered, false, gclog, xmx, overrideProLicense);
  }

  public void setConfig(
      final ObjectNode config, String cluster, String host, int port,
      boolean unitTest, AtomicBoolean isRunning, AtomicBoolean isRecovered, String gclog, boolean overrideProLicense) {
    setConfig(config, cluster, host, port, unitTest, isRunning, isRecovered, false, gclog, null, overrideProLicense);
  }

  public void setConfig(
      final ObjectNode config, String cluster, String host, int port,
      boolean unitTest, AtomicBoolean isRunning, AtomicBoolean isRecovered, boolean skipLicense, String gclog, String xmx, boolean overrideProLicense) {

//    for (int i = 0; i < shardCount; i++) {
//      for (int j = 0; j < replicationFactor; j++) {
//        common.getServersConfig().getShards()[shard].getReplicas()[replica].dead = true;
//      }
//    }

    this.haveProLicense = true;
    this.disableNow = false;
    this.isRunning = isRunning;
    this.isRecovered = isRecovered;
    this.config = config;
    this.cluster = cluster;
    this.host = host;
    this.port = port;
    this.gclog = gclog;
    this.xmx = xmx;
    this.overrideProLicense = overrideProLicense;

    ObjectNode databaseDict = config;
    this.dataDir = databaseDict.get("dataDirectory").asText();
    this.dataDir = dataDir.replace("$HOME", System.getProperty("user.home"));
    this.installDir = databaseDict.get("installDirectory").asText();
    this.installDir = installDir.replace("$HOME", System.getProperty("user.home"));
    ArrayNode shards = databaseDict.withArray("shards");
    int replicaCount = shards.get(0).withArray("replicas").size();
    if (replicaCount > 1) {
      usingMultipleReplicas = true;
    }

    ObjectMapper mapper = new ObjectMapper();


//    try {
//      ObjectNode licenseConfig = (ObjectNode) mapper.readTree(json);
//
//      AtomicInteger licensePort = new AtomicInteger();
//      licensePort.set(licenseConfig.get("server").get("port").asInt());
//      final String address = licenseConfig.get("server").get("privateAddress").asText();
//
//      final AtomicBoolean lastHaveProLicense = new AtomicBoolean(haveProLicense);
//      AtomicBoolean haventSet = new AtomicBoolean();
//
//      doValidateLicense(address, licensePort, lastHaveProLicense, haventSet, true);
//    }
//    catch (IOException e) {
//      logger.error("Error initializing license validator", e);
//      common.setHaveProLicense(false);
//      common.saveSchema(getClient(), dataDir);
//      DatabaseServer.this.haveProLicense = false;
//      disableNow = true;
//    }


    ObjectNode firstServer = (ObjectNode) shards.get(0).withArray("replicas").get(0);
    ServersConfig serversConfig = null;
    executor = ThreadUtil.createExecutor(Runtime.getRuntime().availableProcessors() * 128, "Sonicbase DatabaseServer Thread");
//    if (!skipLicense) {
//      validateLicense(config);
//    }

    if (databaseDict.has("compressRecords")) {
      compressRecords = databaseDict.get("compressRecords").asBoolean();
    }
    if (databaseDict.has("useUnsafe")) {
      useUnsafe = databaseDict.get("useUnsafe").asBoolean();
    }
    else {
      useUnsafe = true;
    }

    this.masterAddress = firstServer.get("privateAddress").asText();
    this.masterPort = firstServer.get("port").asInt();

    if (firstServer.get("privateAddress").asText().equals(host) && firstServer.get("port").asLong() == port) {
      this.shard = 0;
      this.replica = 0;
      common.setShard(0);
      common.setReplica(0);

    }
    boolean isInternal = false;
    if (databaseDict.has("clientIsPrivate")) {
      isInternal = databaseDict.get("clientIsPrivate").asBoolean();
    }
    boolean optimizedForThroughput = true;
    if (databaseDict.has("optimizeReadsFor")) {
      String text = databaseDict.get("optimizeReadsFor").asText();
      if (!text.equalsIgnoreCase("totalThroughput")) {
        optimizedForThroughput = false;
      }
    }
    serversConfig = new ServersConfig(cluster, shards, replicationFactor, isInternal, optimizedForThroughput);

    initServersForUnitTest(host, port, unitTest, serversConfig);

    this.replica = serversConfig.getThisReplica(host, port);

    common.setShard(serversConfig.getThisShard(host, port));
    common.setReplica(this.replica);
    common.setServersConfig(serversConfig);
    this.shard = common.getShard();
    this.shardCount = serversConfig.getShardCount();

    common.setServersConfig(serversConfig);

    logger = new Logger(null /*getDatabaseClient()*/, shard, replica);
    logger.info("config=" + config.toString());

    logger.info("useUnsafe=" + useUnsafe);

    String json = null;
//    if (overrideProLicense) {
      common.setHaveProLicense(true);
//      common.saveSchema(getClient(), dataDir);
      DatabaseServer.this.haveProLicense = true;
      disableNow = false;
//    }
//    else {
//      try {
//        json = IOUtils.toString(DatabaseServer.class.getResourceAsStream("/config-license-server.json"), "utf-8");
//      }
//      catch (Exception e) {
//        logger.error("Error initializing license validator", e);
//        common.setHaveProLicense(false);
//        common.saveSchema(getClient(), dataDir);
//        DatabaseServer.this.haveProLicense = false;
//        disableNow = true;
//      }
//    }

    this.awsClient = new AWSClient(client.get());

    if (USE_SNAPSHOT_MGR_OLD) {
      this.deleteManager = new DeleteManagerOld(this);
    }
    else {
      this.deleteManager = new DeleteManagerImpl(this);
    }
    this.updateManager = new UpdateManager(this);
    //this.snapshotManager = new SnapshotManagerImpl(this);
    if (USE_SNAPSHOT_MGR_OLD) {
      this.deltaManager = new SnapshotManagerImpl(this);
    }
    else {
      this.deltaManager = new DeltaManager(this);
    }
    this.transactionManager = new TransactionManager(this);
    this.readManager = new ReadManager(this);
    this.logManager = new LogManager(this, new File(dataDir, "log"));
    this.schemaManager = new SchemaManager(this);
    this.bulkImportManager = new BulkImportManager(this);
    this.monitorManager = new MonitorManager(this);

    this.methodInvoker = new MethodInvoker(this, bulkImportManager, deleteManager, deltaManager, updateManager,
        transactionManager, readManager, logManager, schemaManager, monitorManager);
    this.osStatsManager = new OSStatsManager(this);
    this.osStatsManager.startStatsMonitoring();

    outer:
    for (int i = 0; i < shards.size(); i++) {
      ArrayNode replicas = (ArrayNode) shards.get(i).withArray("replicas");
      for (int j = 0; j < replicas.size(); j++) {
        JsonNode replica = replicas.get(j);
        if (replica.get("privateAddress").asText().equals(host) && replica.get("port").asInt() == port) {
          if (replica.get("httpPort") != null) {
            httpPort = replica.get("httpPort").asInt();

            this.httpServer = new HttpServer(this);
            this.httpServer.startMonitorServer(host, httpPort);
            break outer;
          }
        }
      }
    }



    //recordsById = new IdIndex(!unitTest, (int) (long) databaseDict.getLong("subPartitionsForIdIndex"), (int) (long) databaseDict.getLong("initialIndexSize"), (int) (long) databaseDict.getLong("indexEntrySize"));

    this.replicationFactor = shards.get(0).withArray("replicas").size();
//    if (replicationFactor < 2) {
//      throw new DatabaseException("Replication Factor must be at least two");
//    }

    scheduleBackup();

    statsThread = ThreadUtil.createThread(new StatsMonitor(), "SonicBase Stats Monitor Thread");
    statsThread.start();

    while (!shutdown) {
      try {
        if (shard != 0 || replica != 0) {
          syncDbNames();
        }
        break;
      }
      catch (Exception e) {
        logger.error("Error syncing dbs", e);
      }
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

    common.saveSchema(getClient(), dataDir);

    for (String dbName : dbNames) {
      logger.info("Loaded database schema: dbName=" + dbName + ", tableCount=" + common.getTables(dbName).size());
      getIndices().put(dbName, new Indices());

      schemaManager.addAllIndices(dbName);
    }

    common.setServersConfig(serversConfig);

    repartitioner = new Repartitioner(this, common);

    //common.getSchema().initRecordsById(shardCount, (int) (long) databaseDict.getLong("partitionCountForRecordIndex"));

    //logger.info("RecordsById: partitionCount=" + common.getSchema().getRecordIndexPartitions().length);

    longRunningCommands = new LongRunningCalls(this);
    startLongRunningCommands();

    startMemoryMonitor();

    disable();
    this.disableNow = false;
    this.haveProLicense = true;
    //startMasterLicenseValidator();

    try {
      checkLicense(new AtomicBoolean(false), new AtomicBoolean(false), new AtomicBoolean(true));
    }
    catch (Exception e) {
      logger.error("Error checking license", e);
    }

    startLicenseValidator();

    this.streamManager = new StreamManager(this);

    //startMasterMonitor();

    logger.info("Started server");


    isStreamingStartedTimer = new Timer("SonicBase IsStreamingStarted Timer");
    isStreamingStartedTimer.schedule(new TimerTask(){
      @Override
      public void run() {
        ComObject cobj = new ComObject();
        cobj.put(ComObject.Tag.method, "isStreamingStarted");

        ComObject retObj = new ComObject(getDatabaseClient().sendToMaster(cobj));
        if (retObj.getBoolean(ComObject.Tag.isStarted)) {
          streamManager.startStreaming(null);
        }
      }
    }, 20_000);

    //logger.error("Testing errors", new DatabaseException());

//    File file = new File(dataDir, "queue/" + shard + "/" + replica);
//    queue = new Queue(file.getAbsolutePath(), "request-log", 0);
//    Thread logThread = new Thread(queue);
//    logThread.start();

  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public String getGcLog() {
    return gclog;
  }

  public void setBackupConfig(ObjectNode backup) {
    this.backupConfig = backup;
  }

  public static void initDeathOverride(int shardCount, int replicaCount) {
    synchronized (deathOverrideMutex) {
      if (deathOverride == null) {
        deathOverride = new boolean[shardCount][];
        for (int i = 0; i < shardCount; i++) {
          deathOverride[i] = new boolean[replicaCount];
        }
      }
    }
  }

  public int getTestWriteCallCount() {
    return methodInvoker.getTestWriteCallCount();
  }

  public void startMasterMonitor() {
    if (replicationFactor == 1) {
      if (shard != 0) {
        return;
      }
      Thread thread = new Thread(new Runnable() {
        @Override
        public void run() {
          while (!shutdown) {
            try {
              promoteToMaster(null);
              break;
            }
            catch (Exception e) {
              logger.error("Error promoting to master", e);
              try {
                Thread.sleep(2000);
              }
              catch (InterruptedException e1) {
                break;
              }
            }
          }
        }
      });
      thread.start();
      return;
    }

    masterMonitorThreadsForShards = new ArrayList<>();

    masterMonitorThread = ThreadUtil.createThread(new Runnable() {
      @Override
      public void run() {
        final int[] monitorShards = {0, 0, 0};
        final int[] monitorReplicas = {0, 1, 2};

        ArrayNode shards = config.withArray("shards");
        ObjectNode replicasNode = (ObjectNode) shards.get(0);
        ArrayNode replicas = replicasNode.withArray("replicas");
        if (replicas.size() < 3) {
          monitorShards[2] = 1;
          monitorReplicas[2] = 0;
        }
        boolean shouldMonitor = false;
        if (shard == 0 && (replica == 0 || replica == 1 || replica == 3)) {
          shouldMonitor = true;
        }
        else if (replicas.size() < 3 && shard == 1 && replica == 0) {
          shouldMonitor = true;
        }
        if (shouldMonitor) {

          for (int i = 0; i < shardCount; i++) {
            final int shard = i;
            Thread masterThreadForShard = ThreadUtil.createThread(new Runnable() {
              @Override
              public void run() {
                AtomicInteger nextMonitor = new AtomicInteger(-1);
                while (!shutdown && nextMonitor.get() == -1) {
                  try {
                    Thread.sleep(2000);
                    electNewMaster(shard, -1, monitorShards, monitorReplicas, nextMonitor);
                  }
                  catch (Exception e) {
                    logger.error("Error electing master: shard=" + shard, e);
                  }
                }
                while (!shutdown) {
                  try {
                    Thread.sleep(deathOverride == null ? 2000 : 1000);

                    final int masterReplica = common.getServersConfig().getShards()[shard].getMasterReplica();
                    if (masterReplica == -1) {
                      electNewMaster(shard, masterReplica, monitorShards, monitorReplicas, nextMonitor);
                    }
                    else {
                      final AtomicBoolean isHealthy = new AtomicBoolean(false);
                      for (int i = 0; i < 5; i++) {
                        checkHealthOfServer(shard, masterReplica, isHealthy, true);
                        if (isHealthy.get()) {
                          break;
                        }
                      }

                      if (!isHealthy.get()) {
                        electNewMaster(shard, masterReplica, monitorShards, monitorReplicas, nextMonitor);
                      }
                    }
                  }
                  catch (Exception e) {
                    logger.error("Error in master monitor: shard=" + shard, e);
                  }
                }
              }
            }, "SonicBase Master Monitor Thread for Shard: " + shard);
            masterThreadForShard.start();
            masterMonitorThreadsForShards.add(masterThreadForShard);
          }
        }
      }
    }, "SonicBase Maste Monitor Thread");
    masterMonitorThread.start();
  }

  public void removeIndices(String dbName, String tableName) {
    if (getIndices() != null) {
      if (getIndices().get(dbName) != null) {
        if (getIndices().get(dbName).getIndices() != null) {
          getIndices().get(dbName).getIndices().remove(tableName);
        }
      }
    }
  }


  private boolean electNewMaster(int shard, int oldMasterReplica, int[] monitorShards, int[] monitorReplicas, AtomicInteger nextMonitor) throws InterruptedException, IOException {
    int electedMaster = -1;
    boolean isFirst = false;
    nextMonitor.set(-1);
    logger.info("electNewMaster - begin: shard=" + shard + ", oldMasterReplica=" + oldMasterReplica);
    for (int i = 0; i < monitorShards.length; i++) {
      if (monitorShards[i] == this.shard && monitorReplicas[i] == this.replica) {
        isFirst = true;
        continue;
      }
      final AtomicBoolean isHealthy = new AtomicBoolean(false);
      checkHealthOfServer(monitorShards[i], monitorReplicas[i], isHealthy, true);
      if (!isHealthy.get()) {
        continue;
      }
      nextMonitor.set(i);
      break;
//      // let the lowest monitor initiate the election
//      if (monitorShards[i] != 0 || monitorReplicas[i] != oldMasterReplica) {
//        if (monitorShards[i] != shard || monitorReplicas[i] != replica) {
//          isFirst = false;
//          break;
//        }
//        nextMonitor = i;
//        break;
//      }
    }
    if (!isFirst) {
      logger.info("ElectNewMaster shard=" + shard + ", !isFirst, nextMonitor=" + nextMonitor);
      return isFirst;
    }
    if (nextMonitor.get() == -1) {
      logger.error("ElectNewMaster shard=" + shard + ", isFirst, nextMonitor==-1");
      Thread.sleep(5000);
    }
    else {
      logger.error("ElectNewMaster, shard=" + shard + ", checking candidates");
      outer:
      while (!shutdown) {
        for (int j = 0; j < replicationFactor; j++) {
          if (j == oldMasterReplica) {
            continue;
          }
          Thread.sleep(deathOverride == null ? 2000 : 50);

          AtomicBoolean isHealthy = new AtomicBoolean();
          if (shard == this.shard && j == this.replica) {
            isHealthy.set(true);
          }
          else {
            checkHealthOfServer(shard, j, isHealthy, false);
          }
          if (isHealthy.get()) {
            try {
              logger.info("ElectNewMaster, electing new: nextMonitor=" + nextMonitor.get() + ", shard=" + shard + ", replica=" + j);
              ComObject cobj = new ComObject();
              cobj.put(ComObject.Tag.dbName, "__non__");
              cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
              cobj.put(ComObject.Tag.method, "electNewMaster");
              cobj.put(ComObject.Tag.requestedMasterShard, shard);
              cobj.put(ComObject.Tag.requestedMasterReplica, j);
              byte[] bytes = getDatabaseClient().send(null, monitorShards[nextMonitor.get()], monitorReplicas[nextMonitor.get()],
                  cobj, DatabaseClient.Replica.specified);
              ComObject retObj = new ComObject(bytes);
              int otherServersElectedMaster = retObj.getInt(ComObject.Tag.selectedMasteReplica);
              if (otherServersElectedMaster != j) {
                logger.info("Other server elected different master: shard=" + shard + ", other=" + otherServersElectedMaster);
                Thread.sleep(2000);
                continue;
              }
              else {
                logger.info("Other server elected same master: shard=" + shard + ", master=" + otherServersElectedMaster);
                electedMaster = otherServersElectedMaster;
                break;
              }
            }
            catch (Exception e) {
              logger.error("Error electing new master: shard=" + shard, e);
              Thread.sleep(2000);
              continue;
            }
          }
        }
        try {
          if (electedMaster != -1) {
            logger.info("Elected master: shard=" + shard + ", replica=" + electedMaster);
            ComObject cobj = new ComObject();
            cobj.put(ComObject.Tag.dbName, "__none__");
            cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
            cobj.put(ComObject.Tag.method, "promoteToMasterAndPushSchema");
            cobj.put(ComObject.Tag.shard, shard);
            cobj.put(ComObject.Tag.replica, electedMaster);
            common.getServersConfig().getShards()[shard].setMasterReplica(electedMaster);

            getDatabaseClient().sendToMaster(cobj);


            cobj = new ComObject();
            cobj.put(ComObject.Tag.dbName, "__none__");
            cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
            cobj.put(ComObject.Tag.method, "promoteToMaster");
            cobj.put(ComObject.Tag.shard, shard);
            cobj.put(ComObject.Tag.electedMaster, electedMaster);

            getDatabaseClient().send(null, shard, electedMaster, cobj, DatabaseClient.Replica.specified);
            return true;
          }
        }
        catch (Exception e) {
          logger.error("Error promoting master: shard=" + shard + ", electedMaster=" + electedMaster, e);
          continue;
        }
      }
    }
    return false;
  }

  public ComObject promoteEntireReplicaToMaster(ComObject cobj) {
    final int newReplica = cobj.getInt(ComObject.Tag.replica);
    for (int shard = 0; shard < shardCount; shard++) {
      logger.info("promoting to master: shard=" + shard + ", replica=" + newReplica);
      common.getServersConfig().getShards()[shard].setMasterReplica(newReplica);
    }

    common.saveSchema(getClient(), getDataDir());
    pushSchema();

    List<Future> futures = new ArrayList<>();
    for (int shard = 0; shard < shardCount; shard++) {
      final int localShard = shard;
      futures.add(getExecutor().submit(new Callable() {
        @Override
        public Object call() throws Exception {
          ComObject cobj = new ComObject();
          cobj.put(ComObject.Tag.dbName, "__none__");
          cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
          cobj.put(ComObject.Tag.method, "promoteToMaster");
          cobj.put(ComObject.Tag.shard, localShard);
          cobj.put(ComObject.Tag.electedMaster, newReplica);

          getDatabaseClient().send(null, localShard, newReplica, cobj, DatabaseClient.Replica.specified);
          return null;
        }
      }));
    }

    for (Future future : futures) {
      try {
        future.get();
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }

    return null;
  }

  public ComObject electNewMaster(ComObject cobj) throws InterruptedException, IOException {
    int requestedMasterShard = cobj.getInt(ComObject.Tag.requestedMasterShard);
    int requestedMasterReplica = cobj.getInt(ComObject.Tag.requestedMasterReplica);

    final AtomicBoolean isHealthy = new AtomicBoolean(false);
    checkHealthOfServer(requestedMasterShard, requestedMasterReplica, isHealthy, false);
    if (!isHealthy.get()) {
      logger.info("candidate master is unhealthy, rejecting: shard=" + requestedMasterShard + ", replica=" + requestedMasterReplica);
      requestedMasterReplica = -1;
    }
    else {
      logger.info("candidate master is healthy, accepting: shard=" + requestedMasterShard + ", candidateMaster=" + requestedMasterReplica);
    }
    ComObject retObj = new ComObject();
    retObj.put(ComObject.Tag.selectedMasteReplica, requestedMasterReplica);
    return retObj;
  }

  public ComObject promoteToMaster(ComObject cobj) {
    try {
      logger.info("Promoting to master: shard=" + shard + ", replica=" + replica);
      logManager.skipToMaxSequenceNumber();

      if (shard == 0) {
        shutdownMasterLicenseValidator();
        startMasterLicenseValidator();

        fixSchemaTimer = new Timer();
        fixSchemaTimer.schedule(new TimerTask(){
          @Override
          public void run() {
            reconcileSchema();
          }
        }, 5 * 60 * 1000, 5 * 60 * 1000);

        monitorManager.startMasterMonitor();

        startStreamsConsumerMonitor();

        shutdownDeathMonitor();

        logger.info("starting death monitor");
        startDeathMonitor();

        logger.info("starting repartitioner");
        startRepartitioner();
      }
      return null;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private final Object connMutex = new Object();
  public Connection getSysConnection() {
    try {
      ConnectionProxy conn = null;
      try {
        synchronized (connMutex) {
          if (sysConnection != null) {
            return sysConnection;
          }
          ArrayNode array = config.withArray("shards");
          ObjectNode replicaDict = (ObjectNode) array.get(0);
          ArrayNode replicasArray = replicaDict.withArray("replicas");
          JsonNode node = config.get("clientIsPrivate");
          final String address = node != null && node.asBoolean() ?
              replicasArray.get(0).get("privateAddress").asText() :
              replicasArray.get(0).get("publicAddress").asText();
          final int port = replicasArray.get(0).get("port").asInt();

          Class.forName("com.sonicbase.jdbcdriver.Driver");
          conn = new ConnectionProxy("jdbc:sonicbase:" + address + ":" + port, this);
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

          sysConnection = new ConnectionProxy("jdbc:sonicbase:" + address + ":" + port + "/_sonicbase_sys", this);
        }
      }
      finally {
        if (conn != null) {
          conn.close();
        }
      }

      return sysConnection;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private void startStreamsConsumerMonitor() {
    if (streamsConsumerMonitorthread == null) {
      streamsConsumerMonitorthread = ThreadUtil.createThread(new Runnable() {
        @Override
        public void run() {
          try {
            Connection conn = getSysConnection();
            StreamManager.initStreamsConsumerTable(conn);

            while (!shutdown && !Thread.interrupted()) {
              try {
                StringBuilder builder = new StringBuilder();
                Map<String, Boolean> currState = null;
                try {
                  currState = readStreamConsumerState();
                }
                catch (Exception e) {
                  logger.error("Error reading stream consumer state - will retry: msg=" + e.getMessage());
                  Thread.sleep(2_000);
                  continue;
                }
                for (int shard = 0; shard < shardCount; shard++) {
                  int aliveReplica = -1;
                  for (int replica = 0; replica < replicationFactor; replica++) {
                    boolean dead = common.getServersConfig().getShards()[shard].getReplicas()[replica].isDead();
                    if (!dead) {
                      aliveReplica = replica;
                    }
                  }
                  builder.append(",").append(shard).append(":").append(aliveReplica);
                  Boolean currActive = currState.get(shard + ":" + aliveReplica);
                  if (currActive == null || !currActive) {
                    setStreamConsumerState(shard, aliveReplica);
                  }
                }
                logger.info("active streams consumers: " + builder.toString());
                Thread.sleep(10_000);
              }
              catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
              }
              catch (Exception e) {
                logger.error("Error in stream consumer monitor", e);
              }
            }
          }
          finally {
            streamsConsumerMonitorthread = null;
          }
        }
      }, "SonicBase Streams Consumer Monitor Thread");
      streamsConsumerMonitorthread.start();
    }
  }

  private void setStreamConsumerState(int shard, int replica) {
    try {
      for (int i = 0; i < getReplicationFactor(); i++) {
        PreparedStatement stmt = null;
        try {
          stmt = sysConnection.prepareStatement("insert ignore into streams_consumer_state (shard, replica, active) VALUES (?, ?, ?)");
          stmt.setInt(1, shard);
          stmt.setInt(2, i);
          stmt.setBoolean(3, replica == i);
          int count = stmt.executeUpdate();
          stmt.close();
        }
        finally {
          stmt.close();
        }
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private Map<String, Boolean> readStreamConsumerState() {
    Map<String, Boolean> ret = new HashMap<>();
    PreparedStatement stmt = null;
    try {
      stmt = sysConnection.prepareStatement("select * from streams_consumer_state");
      ResultSet rs = stmt.executeQuery();
      while (rs.next()) {
        int shard = rs.getInt("shard");
        int replica = rs.getInt("replica");
        boolean isActive = rs.getBoolean("active");
        ret.put(shard + ":" + replica, isActive);
      }
      return ret;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
    finally {
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

  private void shutdownDeathMonitor() {
    synchronized (deathMonitorMutex) {
      logger.info("Stopping death monitor");
      shutdownDeathMonitor = true;
      try {
        if (deathMonitorThreads != null) {
          for (int i = 0; i < shardCount; i++) {
            for (int j = 0; j < replicationFactor; j++) {
              if (deathMonitorThreads[i] != null && deathMonitorThreads[i][j] != null) {
                deathMonitorThreads[i][j].interrupt();
              }
            }
          }
        }
      }
      catch (Exception e) {
        logger.error("Error shutting down death monitor", e);
      }
      if (deathReportThread != null) {
        deathReportThread.interrupt();
      }
      deathReportThread = null;
      deathMonitorThreads = null;
    }
  }

  private Thread[][] deathMonitorThreads = null;
  boolean shutdownDeathMonitor = false;
  private Object deathMonitorMutex = new Object();
  private Thread deathReportThread = null;

  private void startDeathMonitor() {
    synchronized (deathMonitorMutex) {

      deathReportThread = ThreadUtil.createThread(new Runnable() {
        @Override
        public void run() {
          while (!shutdown) {
            try {
              Thread.sleep(10000);
              StringBuilder builder = new StringBuilder();
              for (int i = 0; i < shardCount; i++) {
                for (int j = 0; j < replicationFactor; j++) {
                  builder.append("[").append(i).append(",").append(j).append("=");
                  builder.append(common.getServersConfig().getShards()[i].getReplicas()[j].isDead() ? "dead" : "alive").append("]");
                }
              }
              logger.info("Death status=" + builder.toString());

              if (replicationFactor > 1 && isNoLongerMaster()) {
                logger.info("No longer master. Shutting down resources");
                shutdownMasterLicenseValidator();
                shutdownDeathMonitor();
                shutdownRepartitioner();

                fixSchemaTimer.cancel();
                fixSchemaTimer = null;

                monitorManager.shutdown();

                if (streamsConsumerMonitorthread != null) {
                  streamsConsumerMonitorthread.interrupt();
                  streamsConsumerMonitorthread.join();
                  streamsConsumerMonitorthread = null;
                }

                break;
              }
            }
            catch (InterruptedException e) {
              break;
            }
            catch (Exception e) {
              logger.error("Error in death reporting thread", e);
            }
          }
        }
      }, "SonicBase Death Reporting Thread");
      deathReportThread.start();

      shutdownDeathMonitor = false;
      logger.info("Starting death monitor");
      deathMonitorThreads = new Thread[shardCount][];
      for (int i = 0; i < shardCount; i++) {
        final int shard = i;
        deathMonitorThreads[i] = new Thread[replicationFactor];
        for (int j = 0; j < replicationFactor; j++) {
          final int replica = j;
          if (shard == this.shard && replica == this.replica) {
            boolean wasDead = common.getServersConfig().getShards()[shard].getReplicas()[replica].isDead();
            if (wasDead) {
              AtomicBoolean isHealthy = new AtomicBoolean(true);
              handleHealthChange(isHealthy, wasDead, true, shard, replica);
            }
            continue;
          }
          deathMonitorThreads[i][j] = ThreadUtil.createThread(new Runnable() {
            @Override
            public void run() {
              while (!shutdownDeathMonitor) {
                try {
                  Thread.sleep(deathOverride == null ? 2000 : 50);
                  AtomicBoolean isHealthy = new AtomicBoolean();
                  for (int i = 0; i < 5; i++) {
                    checkHealthOfServer(shard, replica, isHealthy, true);
                    if (isHealthy.get()) {
                      break;
                    }
                    Thread.sleep(1_000);
                  }
                  boolean wasDead = common.getServersConfig().getShards()[shard].getReplicas()[replica].isDead();
                  boolean changed = false;
                  if (wasDead && isHealthy.get()) {
                    changed = true;
                  }
                  else if (!wasDead && !isHealthy.get()) {
                    changed = true;
                  }
                  if (changed && isHealthy.get()) {
                    ComObject cobj = new ComObject();
                    cobj.put(ComObject.Tag.dbName, "__none__");
                    cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
                    cobj.put(ComObject.Tag.method, "prepareToComeAlive");

                    getDatabaseClient().send(null, shard, replica, cobj, DatabaseClient.Replica.specified, true);
                  }
                  handleHealthChange(isHealthy, wasDead, changed, shard, replica);
                }
                catch (InterruptedException e) {
                  break;
                }
                catch (Exception e) {
                  if (!shutdownDeathMonitor) {
                    logger.error("Error in death monitor thread: shard=" + shard + ", replica=" + replica, e);
                  }
                }
              }
            }
          }, "SonicBase Death Monitor Thread: shard=" + i + ", replica=" + j);
          deathMonitorThreads[i][j].start();
        }
      }
    }
  }

  private void handleHealthChange(AtomicBoolean isHealthy, boolean wasDead, boolean changed, int shard, int replica) {
    synchronized (common) {
      if (wasDead && isHealthy.get()) {
        common.getServersConfig().getShards()[shard].getReplicas()[replica].setDead(false);
        changed = true;
      }
      else if (!wasDead && !isHealthy.get()) {
        common.getServersConfig().getShards()[shard].getReplicas()[replica].setDead(true);
        changed = true;
      }
    }
    if (changed) {
      logger.info("server health changed: shard=" + shard + ", replica=" + replica + ", isHealthy=" + isHealthy.get());
      common.saveSchema(getClient(), getDataDir());
      pushSchema();
    }
  }

  private int replicaDeadForRestart = -1;

  public void checkHealthOfServer(final int shard, final int replica, final AtomicBoolean isHealthy, final boolean ignoreDeath) throws InterruptedException {

    if (deathOverride != null) {
      isHealthy.set(!deathOverride[shard][replica]);
      return;
    }

    if (replicaDeadForRestart == replica) {
      isHealthy.set(false);
      return;
    }

    if (shard == this.shard && replica == this.replica) {
      isHealthy.set(true);
      return;
    }

    final AtomicBoolean finished = new AtomicBoolean();
    isHealthy.set(false);
    Thread checkThread = ThreadUtil.createThread(new Runnable() {
      private ComObject recoverStatus;

      @Override
      public void run() {
        int backoff = 100;
        //while (true) {
          ServersConfig.Host host = common.getServersConfig().getShards()[shard].getReplicas()[replica];
          boolean wasDead = host.isDead();
          try {
            ComObject cobj = new ComObject();
            cobj.put(ComObject.Tag.dbName, "__none__");
            cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
            cobj.put(ComObject.Tag.method, "healthCheck");
            byte[] bytes = getDatabaseClient().send(null, shard, replica, cobj, DatabaseClient.Replica.specified, true);
            ComObject retObj = new ComObject(bytes);
            if (retObj.getString(ComObject.Tag.status).equals("{\"status\" : \"ok\"}")) {
              isHealthy.set(true);
            }
            return;
          }
          catch (Exception e) {
            //int index = ExceptionUtils.indexOfThrowable(e, DeadServerException.class);
            if (e instanceof DeadServerException) {//-1 != index) {
              //if (!wasDead) {
              logger.error("Error checking health of server - dead server: shard=" + shard + ", replica=" + replica);
              //}
            }
            else {
              logger.error("Error checking health of server: shard=" + shard + ", replica=" + replica, e);
            }
//            try {
//              Thread.sleep(backoff);
//              backoff *= 2;
//            }
//            catch (InterruptedException e1) {
//              return;
//            }
          }
          finally {
            finished.set(true);
          }
        //}
      }
    }, "SonicBase Check Health of Server Thread");
    checkThread.start();

    int i = 0;
    while (!finished.get()) {
      Thread.sleep(deathOverride == null ? 100 : 100);
      if (i++ > 50) {
        checkThread.interrupt();
        break;
      }
    }
  }

  public AWSClient getAWSClient() {
    return awsClient;
  }

  public static void disable() {
    try {
      SSLContext sslc = SSLContext.getInstance("TLS");
      TrustManager[] trustManagerArray = {new NullX509TrustManager()};
      sslc.init(null, trustManagerArray, null);
      HttpsURLConnection.setDefaultSSLSocketFactory(sslc.getSocketFactory());
      HttpsURLConnection.setDefaultHostnameVerifier(new NullHostnameVerifier());
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }

  public ThreadPoolExecutor getExecutor() {
    return executor;
  }

  public Thread[][] getDeathMonitorThreads() {
    return deathMonitorThreads;
  }

  /**
   * make sure you're really still the master before making death decisions
   */
  public void getSchemaFromPossibleMaster() {
    final int[] monitorShards = {0, 0, 0};
    final int[] monitorReplicas = {0, 1, 2};

    ArrayNode shards = config.withArray("shards");
    ArrayNode replicas = (ArrayNode) shards.get(0).withArray("replicas");
    if (replicas.size() < 3) {
      monitorShards[2] = 1;
      monitorReplicas[2] = 0;
    }

    int[] masterAccordingToOther = new int[]{-1, -1};
    int otherCount = 0;
    for (int i = 0; i < monitorShards.length; i++) {
      if (monitorShards[i] == this.shard && monitorReplicas[i] == this.replica) {
        continue;
      }
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.dbName, "__none__");
      cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
      cobj.put(ComObject.Tag.method, "getSchema");
      try {

        byte[] ret = getClient().send(null, monitorShards[i], monitorReplicas[i], cobj, DatabaseClient.Replica.specified, true);
        DatabaseCommon tempCommon = new DatabaseCommon();
        ComObject retObj = new ComObject(ret);
        tempCommon.deserializeSchema(retObj.getByteArray(ComObject.Tag.schemaBytes));
        masterAccordingToOther[otherCount++] = tempCommon.getServersConfig().getShards()[0].getMasterReplica();
        if (otherCount == 2) {
          if (masterAccordingToOther[0] == masterAccordingToOther[1] && masterAccordingToOther[0] != this.replica) {
            common.deserializeSchema(retObj.getByteArray(ComObject.Tag.schemaBytes));
          }
        }
      }
      catch (Exception e) {
        logger.error("Error getting schema: shard=" + monitorShards[i] + ", replica=" + monitorReplicas[i], e);
      }
    }
    return;
  }

  public boolean isNoLongerMaster() throws InterruptedException {

    final int[] monitorShards = {0, 0, 0};
    final int[] monitorReplicas = {0, 1, 2};

    ArrayNode shards = config.withArray("shards");
    ArrayNode replicas = (ArrayNode) shards.get(0).withArray("replicas");
    if (replicas.size() < 3) {
      monitorShards[2] = 1;
      monitorReplicas[2] = 0;
    }

    int countAgree = 0;
    int countDisagree = 0;
    for (int i = 0; i < monitorShards.length; i++) {
      try {
        AtomicBoolean isHealthy = new AtomicBoolean();
        checkHealthOfServer(monitorShards[i], monitorReplicas[i], isHealthy, true);

        if (isHealthy.get()) {
          ComObject cobj = new ComObject();
          cobj.put(ComObject.Tag.dbName, "__none__");
          cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
          cobj.put(ComObject.Tag.method, "getSchema");

          byte[] ret = getClient().send(null, monitorShards[i], monitorReplicas[i], cobj, DatabaseClient.Replica.specified, true);
          DatabaseCommon tempCommon = new DatabaseCommon();
          ComObject retObj = new ComObject(ret);
          tempCommon.deserializeSchema(retObj.getByteArray(ComObject.Tag.schemaBytes));
          int masterReplica = tempCommon.getServersConfig().getShards()[0].getMasterReplica();
          if (masterReplica == this.replica) {
            countAgree++;
          }
          else {
            countDisagree++;
          }
        }
      }
      catch (InterruptedException e) {
        throw e;
      }
      catch (Exception e) {
        if (!shutdownDeathMonitor) {
          logger.error("Error checking if master", e);
        }
      }
    }
    if (countAgree < countDisagree) {
      return true;
    }
    return false;
  }

  public void setReplicaDeadForRestart(int replicaDeadForRestart) {
    this.replicaDeadForRestart = replicaDeadForRestart;
  }

  public boolean isApplyingQueuesAndInteractive() {
    return applyingQueuesAndInteractive;
  }

  public boolean shouldDisableNow() {
    return disableNow;
  }

  public boolean isUsingMultipleReplicas() {
    return usingMultipleReplicas;
  }

  public boolean onlyQueueCommands() {
    return onlyQueueCommands;
  }

  public String getInstallDir() {
    return installDir;
  }

  public boolean haveProLicense() {
    return haveProLicense;
  }

  public Logger getLogger() {
    return logger;
  }

  public StreamManager getStreamManager() {
    return streamManager;
  }

  public SnapshotManager getDeltaManager() {
    return deltaManager;
  }

  public ComObject getRecoverProgress() {
    ComObject retObj = new ComObject();
    if (waitingForServersToStart) {
      retObj.put(ComObject.Tag.percentComplete, 0d);
      retObj.put(ComObject.Tag.stage, "waitingForServersToStart");
    }
    else if (deltaManager.isRecovering()) {
      deltaManager.getPercentRecoverComplete(retObj);
    }
    else if (!getDeleteManager().isForcingDeletes()) {
      retObj.put(ComObject.Tag.percentComplete, logManager.getPercentApplyQueuesComplete());
      retObj.put(ComObject.Tag.stage, "applyingLogs");
    }
    else {
      retObj.put(ComObject.Tag.percentComplete, getDeleteManager().getPercentDeleteComplete());
      retObj.put(ComObject.Tag.stage, "forcingDeletes");
    }
    Exception error = deltaManager.getErrorRecovering();
    if (error != null) {
      retObj.put(ComObject.Tag.error, true);
    }
    return retObj;

  }


  public Parameters getParametersFromStoredProcedure(Execute execute) {
    ExpressionList expressions = execute.getExprList();
    Object[] parmsArray = new Object[expressions.getExpressions().size()];
    int offset = 0;
    for (Expression expression : expressions.getExpressions()) {
      if (expression instanceof StringValue) {
        parmsArray[offset] = ((StringValue)expression).getValue();
      }
      else if (expression instanceof LongValue) {
        parmsArray[offset] = ((LongValue)expression).getValue();
      }
      else if (expression instanceof DateValue) {
        parmsArray[offset] = ((DateValue)expression).getValue();
      }
      else if (expression instanceof DoubleValue) {
        parmsArray[offset] = ((DoubleValue)expression).getValue();
      }
      else if (expression instanceof TimeValue) {
        parmsArray[offset] = ((TimeValue)expression).getValue();
      }
      else if (expression instanceof TimestampValue) {
        parmsArray[offset] = ((TimestampValue)expression).getValue();
      }
      else if (expression instanceof NullValue) {
        parmsArray[offset] = null;
      }
      offset++;
    }
    ParametersImpl parms = new ParametersImpl(parmsArray);
    return parms;
  }

  public ConnectionProxy getConnectionForStoredProcedure(String dbName) throws ClassNotFoundException, SQLException {
    synchronized (this) {
      if (connectionForStoredProcedure != null) {
        return (ConnectionProxy) connectionForStoredProcedure;
      }
      ArrayNode array = config.withArray("shards");
      ObjectNode replicaDict = (ObjectNode) array.get(0);
      ArrayNode replicasArray = replicaDict.withArray("replicas");
      JsonNode node = config.get("clientIsPrivate");
      final String address = node != null && node.asBoolean() ?
          replicasArray.get(0).get("privateAddress").asText() :
          replicasArray.get(0).get("publicAddress").asText();
      final int port = replicasArray.get(0).get("port").asInt();

      Class.forName("com.sonicbase.jdbcdriver.Driver");
      connectionForStoredProcedure = new ConnectionProxy("jdbc:sonicbase:" + address + ":" + port + "/" + dbName, this);

      ((ConnectionProxy)connectionForStoredProcedure).getDatabaseClient().syncSchema();
      return (ConnectionProxy) connectionForStoredProcedure;
    }
  }

  public ComObject executeProcedure(final ComObject cobj) {
    ConnectionProxy conn = null;
    try {
      if (!common.haveProLicense()) {
        throw new InsufficientLicense("You must have a pro license to use stored procedures");
      }

      String sql = cobj.getString(ComObject.Tag.sql);
      CCJSqlParserManager parser = new CCJSqlParserManager();
      Statement statement = parser.parse(new StringReader(sql));
      if (!(statement instanceof Execute)) {
        throw new DatabaseException("Invalid command: sql=" + sql);
      }
      Execute execute = (Execute) statement;

      Parameters parms = getParametersFromStoredProcedure(execute);

      String dbName = cobj.getString(ComObject.Tag.dbName);
      String className = parms.getString(1);
      StoredProcedure procedure = (StoredProcedure) Class.forName(className).newInstance();
      StoredProcedureContextImpl context = new StoredProcedureContextImpl();
      context.setConfig(config);
      context.setShard(shard);
      context.setReplica(replica);

      conn = getConnectionForStoredProcedure(dbName);
      int viewVersion = conn.getDatabaseClient().getCommon().getSchemaVersion();
      context.setViewVersion(viewVersion);

      context.setConnection(new SonicBaseConnectionImpl(conn));

      long storedProcedureId = cobj.getLong(ComObject.Tag.id);
      context.setStoredProdecureId(storedProcedureId);
      context.setParameters(parms);
      StoredProcedureResponse response = procedure.execute(context);
      if (response == null) {
        return null;
      }
      return ((StoredProcedureResponseImpl)response).serialize();
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public ComObject executeProcedurePrimary(final ComObject cobj) {
    ThreadPoolExecutor executor = null;
    try {
      if (!common.haveProLicense()) {
        throw new InsufficientLicense("You must have a pro license to use stored procedures");
      }
      String sql = cobj.getString(ComObject.Tag.sql);
      CCJSqlParserManager parser = new CCJSqlParserManager();
      Statement statement = parser.parse(new StringReader(sql));
      if (!(statement instanceof Execute)) {
        throw new DatabaseException("Invalid command: sql=" + sql);
      }
      Execute execute = (Execute) statement;

      Parameters parms = getParametersFromStoredProcedure(execute);

      String dbName = cobj.getString(ComObject.Tag.dbName);
      String className = parms.getString(1);
      StoredProcedure procedure = (StoredProcedure) Class.forName(className).newInstance();
      StoredProcedureContextImpl context = new StoredProcedureContextImpl();
      context.setConfig(config);
      context.setShard(shard);
      context.setReplica(replica);

      Connection conn = getConnectionForStoredProcedure(dbName);

      context.setConnection(new SonicBaseConnectionImpl(conn));

      long storedProcedureId = getDatabaseClient().allocateId(dbName);
      context.setStoredProdecureId(storedProcedureId);
      context.setParameters(parms);
      procedure.init(context);

      cobj.put(ComObject.Tag.method, "executeProcedure");
      cobj.put(ComObject.Tag.id, storedProcedureId);

      executor = ThreadUtil.createExecutor(shardCount, "SonicBase executeProcedurePrimary Thread");
      // call all shards
      List<Future> futures = new ArrayList<>();
      for (int i = 0; i < shardCount; i++) {
        final int shard = i;
        futures.add(executor.submit(new Callable(){
          @Override
          public Object call() throws Exception {
            return getDatabaseClient().send(null, shard, 0, cobj, DatabaseClient.Replica.def);
          }
        }));
      }

      List<StoredProcedureResponse> responses = new ArrayList<>();
      for (Future future : futures) {
        byte[] ret = (byte[]) future.get();
        if (ret != null) {
          StoredProcedureResponseImpl response = new StoredProcedureResponseImpl(common, new ComObject(ret));
          responses.add(response);
        }
      }

      StoredProcedureResponseImpl ret = (StoredProcedureResponseImpl) procedure.finalize(context, responses);
      if (ret.getRecords().size() > 50_000) {
        throw new DatabaseException("Too many results returned: allowed=50000, attemptedToReturn=" + ret.getRecords().size());
      }
      return ret.serialize();
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
    finally {
      if (executor != null) {
        executor.shutdownNow();
      }
    }
  }

  public SnapshotManager getSnapshotManager() {
    return deltaManager;
  }

  public void reconcileSchema() {
    try {
      if (shard != 0 || replica != 0) {
        return;
      }
      logger.info("reconcile schema - begin");
      int threadCount = getShardCount() * getReplicationFactor();
      ThreadPoolExecutor executor = new ThreadPoolExecutor(threadCount, threadCount, 10_000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
      try {
        final ComObject highestSchemaVersions = readSchemaVersions();
        List<Future> futures = new ArrayList<>();
        for (int i = 0; i < getShardCount(); i++) {
          for (int j = 0; j < getReplicationFactor(); j++) {
            final int shard = i;
            final int replica = j;
            futures.add(executor.submit(new Callable() {
              @Override
              public Object call() throws Exception {
                long beginTime = System.currentTimeMillis();
                while (!shutdown) {
                  try {
                    if (getShard() == shard && getReplica() == replica) {
                      break;
                    }
                    ComObject cobj = new ComObject();
                    cobj.put(ComObject.Tag.method, "getSchemaVersions");
                    byte[] ret = getDatabaseClient().send(null, shard, replica, cobj, DatabaseClient.Replica.specified);
                    if (ret != null) {
                      ComObject retObj = new ComObject(ret);
                      retObj.put(ComObject.Tag.shard, shard);
                      retObj.put(ComObject.Tag.replica, replica);
                      return retObj;
                    }
                    Thread.sleep(1_000);
                  }
                  catch (InterruptedException e) {
                    return null;
                  }
                  catch (Exception e) {
                    logger.error("Error checking if server is healthy: shard=" + shard + ", replica=" + replica);
                  }
                  if (System.currentTimeMillis() - beginTime > 2 * 60 * 1000) {
                    logger.error("Server appears to be dead, skipping: shard=" + shard + ", replica=" + replica);
                    break;
                  }
                }
                return null;
              }
            }));

          }
        }
        try {
          List<ComObject> retObjs = new ArrayList<>();
          for (Future future : futures) {
            ComObject cobj = (ComObject) future.get();
            if (cobj != null) {
              retObjs.add(cobj);
            }
          }
          for (ComObject retObj : retObjs) {
            getHighestSchemaVersions(retObj.getInt(ComObject.Tag.shard), retObj.getInt(ComObject.Tag.replica), highestSchemaVersions, retObj);
          }

          pushHighestSchema(highestSchemaVersions);

          if (getShard() == 0 && getReplica() == 0) {
            pushSchema();
          }
        }
        catch (Exception e) {
          logger.error("Error pushing schema", e);
        }
      }
      finally {
        executor.shutdownNow();
      }
      logger.info("reconcile schema - end");
    }
    catch (Exception e) {
      logger.error("Error reconciling schema", e);
    }
  }

  public ComObject getSchemaVersions(ComObject cobj, boolean replayedCommand) {
    return readSchemaVersions();
  }

  private void pushHighestSchema(ComObject highestSchemaVersions) {
    ComArray databases = highestSchemaVersions.getArray(ComObject.Tag.databases);
    for (int i = 0; i < databases.getArray().size(); i++) {
      ComObject db = (ComObject) databases.getArray().get(i);
      String currDbName = db.getString(ComObject.Tag.dbName);
      ComArray tables = db.getArray(ComObject.Tag.tables);
      for (int j = 0; j < tables.getArray().size(); j++) {
        ComObject table = (ComObject) tables.getArray().get(i);
        String currTableName = table.getString(ComObject.Tag.tableName);
        byte[] tableSchema = getHighestTableSchema(currDbName, currTableName, table.getInt(ComObject.Tag.shard),
            table.getInt(ComObject.Tag.replica));
        if (table.getBoolean(ComObject.Tag.hasDiscrepancy)) {
          logger.info("Table schema has discrepancy, will push schema: db=" + currDbName + ", table=" + currTableName);
          pushTableSchema(currDbName, currTableName, tableSchema, table.getInt(ComObject.Tag.schemaVersion));
        }
        ComArray indices = table.getArray(ComObject.Tag.indices);
        for (int k = 0; k < indices.getArray().size(); k++) {
          ComObject index = (ComObject) indices.getArray().get(k);
          String currIndexName = index.getString(ComObject.Tag.indexName);
          if (index.getBoolean(ComObject.Tag.hasDiscrepancy)) {
            byte[] indexSchema = getHighestIndexSchema(currDbName, currTableName, currIndexName, index.getInt(ComObject.Tag.shard),
                index.getInt(ComObject.Tag.replica));
            logger.info("Index schema has discrepancy, will push schema: db=" + currDbName + ", table=" + currTableName + ", index=" + currIndexName);
            pushIndexSchema(currDbName, currTableName, currIndexName, indexSchema, index.getInt(ComObject.Tag.schemaVersion));
          }
        }
      }
    }

  }

  private void pushIndexSchema(String currDbName, String currTableName, String currIndexName,
                               byte[] indexSchema, Integer schemaVersion) {
    try {
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.dbName, currDbName);
      cobj.put(ComObject.Tag.tableName, currTableName);
      cobj.put(ComObject.Tag.indexName, currIndexName);
      cobj.put(ComObject.Tag.indexSchema, indexSchema);
      cobj.put(ComObject.Tag.schemaVersion, schemaVersion);
      cobj.put(ComObject.Tag.method, "updateIndexSchema");

      getDatabaseClient().sendToAllShards(null, 0, cobj, DatabaseClient.Replica.all);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }

  }

  private byte[] getHighestIndexSchema(String currDbName, String currTableName, String currIndexName, Integer shard, Integer replica) {
    try {
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.method, "getIndexSchema");
      cobj.put(ComObject.Tag.dbName, currDbName);
      cobj.put(ComObject.Tag.tableName, currTableName);
      cobj.put(ComObject.Tag.indexName, currIndexName);

      byte[] ret = getDatabaseClient().send(null, shard, replica, cobj, DatabaseClient.Replica.specified);
      ComObject retObj = new ComObject(ret);
      return retObj.getByteArray(ComObject.Tag.indexSchema);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private void pushTableSchema(String currDbName, String currTableName, byte[] tableSchema, Integer schemaVersion) {
    try {
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.dbName, currDbName);
      cobj.put(ComObject.Tag.tableName, currTableName);
      cobj.put(ComObject.Tag.tableSchema, tableSchema);
      cobj.put(ComObject.Tag.schemaVersion, schemaVersion);
      cobj.put(ComObject.Tag.method, "updateTableSchema");

      getDatabaseClient().sendToAllShards(null, 0, cobj, DatabaseClient.Replica.all);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public ComObject updateIndexSchema(ComObject cobj, boolean replayedCommand) {
    try {
      String dbName = cobj.getString(ComObject.Tag.dbName);
      String tableName = cobj.getString(ComObject.Tag.tableName);
      String indexName = cobj.getString(ComObject.Tag.indexName);
      byte[] bytes = cobj.getByteArray(ComObject.Tag.indexSchema);
      int schemaVersion = cobj.getInt(ComObject.Tag.schemaVersion);

      logger.info("Updating schema for Index: db=" + dbName + ", table=" + tableName + ", index=" + indexName + ", schemaVersion=" + schemaVersion);

      File indexDir = deltaManager.getIndexSchemaDir(dbName, tableName, indexName);
      File newSchemaFile = new File(indexDir, "schema." + schemaVersion + ".bin");
      File[] indexSchemas = indexDir.listFiles();
      if (indexSchemas != null && indexSchemas.length > 0) {
        sortSchemaFiles(indexSchemas);
        File indexSchemaFile = indexSchemas[indexSchemas.length - 1];
        String filename = indexSchemaFile.getName();
        int pos = filename.indexOf(".");
        int pos2 = filename.indexOf(".", pos + 1);
        int currSchemaVersion = Integer.valueOf(filename.substring(pos + 1, pos2));
        if (currSchemaVersion < schemaVersion) {
          newSchemaFile.delete();
          try (FileOutputStream out = new FileOutputStream(newSchemaFile)) {
            out.write(bytes);
          }
          common.loadSchema(getDataDir());
        }
      }
      else {
        newSchemaFile.delete();
        newSchemaFile.getParentFile().mkdirs();
        try (FileOutputStream out = new FileOutputStream(newSchemaFile)) {
          out.write(bytes);
        }
        common.loadSchema(getDataDir());
      }
      return null;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public ComObject updateTableSchema(ComObject cobj, boolean replayedCommand) {
    try {

      String dbName = cobj.getString(ComObject.Tag.dbName);
      String tableName = cobj.getString(ComObject.Tag.tableName);
      byte[] bytes = cobj.getByteArray(ComObject.Tag.tableSchema);
      int schemaVersion = cobj.getInt(ComObject.Tag.schemaVersion);

      logger.info("Updating schema for table: db=" + dbName + ", table=" + tableName + ", schemaVersion=" + schemaVersion);

      File tableDir = deltaManager.getTableSchemaDir(dbName, tableName);
      File newSchemaFile = new File(tableDir, "schema." + schemaVersion + ".bin");
      File[] tableSchemas = tableDir.listFiles();
      if (tableSchemas != null && tableSchemas.length > 0) {
        sortSchemaFiles(tableSchemas);
        File tableSchemaFile = tableSchemas[tableSchemas.length - 1];
        String filename = tableSchemaFile.getName();
        int pos = filename.indexOf(".");
        int pos2 = filename.indexOf(".", pos + 1);
        int currSchemaVersion = Integer.valueOf(filename.substring(pos + 1, pos2));
        if (currSchemaVersion < schemaVersion) {
          newSchemaFile.delete();
          try (FileOutputStream out = new FileOutputStream(newSchemaFile)) {
            out.write(bytes);
          }
          common.loadSchema(getDataDir());
        }
      }
      else {
        newSchemaFile.delete();
        newSchemaFile.getParentFile().mkdirs();
        try (FileOutputStream out = new FileOutputStream(newSchemaFile)) {
          out.write(bytes);
        }
        common.loadSchema(getDataDir());
      }
      return null;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public ComObject getIndexSchema(ComObject cobj, boolean replayedCommand) {
    try {
      String dbName = cobj.getString(ComObject.Tag.dbName);
      String tableName = cobj.getString(ComObject.Tag.tableName);
      String indexName = cobj.getString(ComObject.Tag.indexName);

      ComObject retObj = new ComObject();
      File indexDir = deltaManager.getIndexSchemaDir(dbName, tableName, indexName);
      File[] indexSchemas = indexDir.listFiles();
      if (indexSchemas != null && indexSchemas.length > 0) {
        sortSchemaFiles(indexSchemas);
        File indexSchemaFile = indexSchemas[indexSchemas.length - 1];
        byte[] bytes = IOUtils.toByteArray(new FileInputStream(indexSchemaFile));
        retObj.put(ComObject.Tag.indexSchema, bytes);
      }
      return retObj;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public ComObject getTableSchema(ComObject cobj, boolean replayedCommand) {
    try {
      String dbName = cobj.getString(ComObject.Tag.dbName);
      String tableName = cobj.getString(ComObject.Tag.tableName);

      ComObject retObj = new ComObject();
      File tableDir = deltaManager.getTableSchemaDir(dbName, tableName);
      File[] tableSchemas = tableDir.listFiles();
      if (tableSchemas != null && tableSchemas.length > 0) {
        sortSchemaFiles(tableSchemas);
        File tableSchemaFile = tableSchemas[tableSchemas.length - 1];
        byte[] bytes = IOUtils.toByteArray(new FileInputStream(tableSchemaFile));
        retObj.put(ComObject.Tag.tableSchema, bytes);
      }
      return retObj;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private byte[] getHighestTableSchema(String currDbName, String currTableName, int shard, Integer replica) {
    try {
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.method, "getTableSchema");
      cobj.put(ComObject.Tag.dbName, currDbName);
      cobj.put(ComObject.Tag.tableName, currTableName);

      byte[] ret = getDatabaseClient().send(null, shard, replica, cobj, DatabaseClient.Replica.specified);
      ComObject retObj = new ComObject(ret);
      return retObj.getByteArray(ComObject.Tag.tableSchema);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private ComObject readSchemaVersions() {
    ComObject cobj = new ComObject();
    List<String> dbNames = common.getDbNames(getDataDir());
    ComArray databases = cobj.putArray(ComObject.Tag.databases, ComObject.Type.objectType);
    for (String dbName : dbNames) {
      ComObject dbObj = new ComObject();
      databases.add(dbObj);
      dbObj.put(ComObject.Tag.dbName, dbName);
      ComArray tables = dbObj.putArray(ComObject.Tag.tables, ComObject.Type.objectType);
      for (TableSchema tableSchema : common.getTables(dbName).values()) {
        ComObject tableObj = new ComObject();
        tables.add(tableObj);
        tableObj.put(ComObject.Tag.tableName, tableSchema.getName());
        File tableDir = deltaManager.getTableSchemaDir(dbName, tableSchema.getName());
        File[] tableSchemas = tableDir.listFiles();
        if (tableSchemas != null && tableSchemas.length > 0) {
          sortSchemaFiles(tableSchemas);
          File tableSchemaFile = tableSchemas[tableSchemas.length - 1];
          tableObj.put(ComObject.Tag.schemaVersion, getSchemVersionFromFile(tableSchemaFile));
        }
        else {
          tableObj.put(ComObject.Tag.schemaVersion, 0);
        }
        tableObj.put(ComObject.Tag.shard, getShard());
        tableObj.put(ComObject.Tag.replica, getReplica());
        ComArray indices = tableObj.putArray(ComObject.Tag.indices, ComObject.Type.objectType);
        for (IndexSchema indexSchema : tableSchema.getIndexes().values()) {
          ComObject indexObj = new ComObject();
          indices.add(indexObj);
          indexObj.put(ComObject.Tag.indexName, indexSchema.getName());
          File indexDir = deltaManager.getIndexSchemaDir(dbName, tableSchema.getName(), indexSchema.getName());
          File[] indexSchemas = indexDir.listFiles();
          if (indexSchemas != null && indexSchemas.length > 0) {
            sortSchemaFiles(indexSchemas);
            File indexSchemaFile = indexSchemas[indexSchemas.length - 1];
            indexObj.put(ComObject.Tag.schemaVersion, getSchemVersionFromFile(indexSchemaFile));
          }
          else {
            indexObj.put(ComObject.Tag.schemaVersion, 0);
          }
          indexObj.put(ComObject.Tag.shard, getShard());
          indexObj.put(ComObject.Tag.replica, getReplica());
        }
      }
    }
    return cobj;
  }

  private int getSchemVersionFromFile(File schemaFile) {
    String name = schemaFile.getName();
    int pos = name.indexOf(".");
    int pos2 = name.indexOf(".", pos + 1);
    return Integer.valueOf(name.substring(pos + 1, pos2));
  }

  private void getHighestSchemaVersions(int shard, int replica, ComObject highestSchemaVersions, ComObject retObj) {
    ComArray databases = retObj.getArray(ComObject.Tag.databases);
    for (int i = 0; i < databases.getArray().size(); i++) {
      ComObject db = (ComObject) databases.getArray().get(i);
      String dbName = db.getString(ComObject.Tag.dbName);
      ComArray tables = db.getArray(ComObject.Tag.tables);
      ConcurrentHashMap<String, Integer> tablesFound = new ConcurrentHashMap<>();
      for (int j = 0; j < tables.getArray().size(); j++) {
        ComObject table = (ComObject) tables.getArray().get(j);
        String tableName = table.getString(ComObject.Tag.tableName);
        int tableSchemaVersion = table.getInt(ComObject.Tag.schemaVersion);
        tablesFound.put(tableName, tableSchemaVersion);
        ComObject highestTable = getSchemaVersion(dbName, tableName, highestSchemaVersions);
        if (highestTable != null) {
          if (tableSchemaVersion > highestTable.getInt(ComObject.Tag.schemaVersion)) {
            setHighestSchemaVersion(dbName, tableName, tableSchemaVersion, shard, replica, highestSchemaVersions, tablesFound);
          }
          else if (tableSchemaVersion < highestTable.getInt(ComObject.Tag.schemaVersion)) {
            setHighestSchemaVersion(dbName, tableName, highestTable.getInt(ComObject.Tag.schemaVersion), highestTable.getInt(ComObject.Tag.shard),
                highestTable.getInt(ComObject.Tag.replica), highestSchemaVersions, tablesFound);
          }
          else {
            tablesFound.remove(tableName);
          }
        }
        ConcurrentHashMap<String, Integer> indicesFound = new ConcurrentHashMap<>();
        ComArray indices = table.getArray(ComObject.Tag.indices);
        for (int k = 0; k < indices.getArray().size(); k++) {
          ComObject index = (ComObject) indices.getArray().get(k);
          int indexSchemaVersion = index.getInt(ComObject.Tag.schemaVersion);
          String indexName = index.getString(ComObject.Tag.indexName);
          indicesFound.put(indexName, indexSchemaVersion);
          ComObject highestIndex = getSchemaVersion(dbName, tableName, indexName, highestSchemaVersions);
          if (highestIndex != null) {
            if (indexSchemaVersion > highestIndex.getInt(ComObject.Tag.schemaVersion)) {
              setHighestSchemaVersion(dbName, tableName, indexName, indexSchemaVersion, shard, replica, highestSchemaVersions, indicesFound);
            }
            else if (indexSchemaVersion < highestIndex.getInt(ComObject.Tag.schemaVersion)) {
              setHighestSchemaVersion(dbName, tableName, indexName, highestIndex.getInt(ComObject.Tag.schemaVersion),
                  highestIndex.getInt(ComObject.Tag.shard), highestIndex.getInt(ComObject.Tag.replica), highestSchemaVersions, indicesFound);
            }
            else {
              indicesFound.remove(indexName);
            }
          }
        }
        for (Map.Entry<String, Integer> entry: indicesFound.entrySet()) {
          setHighestSchemaVersion(dbName, tableName, entry.getKey(), entry.getValue(), shard, replica, highestSchemaVersions, indicesFound);
        }
      }
      for (Map.Entry<String, Integer> entry: tablesFound.entrySet()) {
        setHighestSchemaVersion(dbName, entry.getKey(), entry.getValue(), shard, replica, highestSchemaVersions, tablesFound);
      }
    }
  }

  private void setHighestSchemaVersion(String dbName, String tableName, String indexName, int indexSchemaVersion,
                                       int shard, int replica, ComObject highestSchemaVersions, ConcurrentHashMap<String, Integer> indicesFound) {
    ComArray databases = highestSchemaVersions.getArray(ComObject.Tag.databases);
    for (int i = 0; i < databases.getArray().size(); i++) {
      ComObject db = (ComObject) databases.getArray().get(i);
      String currDbName = db.getString(ComObject.Tag.dbName);
      if (currDbName.equals(dbName)) {
        ComArray tables = db.getArray(ComObject.Tag.tables);
        for (int j = 0; j < tables.getArray().size(); j++) {
          ComObject table = (ComObject) tables.getArray().get(i);
          String currTableName = table.getString(ComObject.Tag.tableName);
          if (currTableName.equals(tableName)) {
            ComArray indices = table.getArray(ComObject.Tag.indices);
            for (int k = 0; k < indices.getArray().size(); k++) {
              ComObject index = (ComObject) indices.getArray().get(k);
              String currIndexName = index.getString(ComObject.Tag.indexName);
              indicesFound.remove(currIndexName);
              if (currIndexName.equals(indexName)) {
                index.put(ComObject.Tag.schemaVersion, indexSchemaVersion);
                index.put(ComObject.Tag.shard, shard);
                index.put(ComObject.Tag.replica, replica);
                index.put(ComObject.Tag.hasDiscrepancy, true);
                return;
              }
            }
            ComObject index = new ComObject();
            indices.add(index);
            index.put(ComObject.Tag.schemaVersion, indexSchemaVersion);
            index.put(ComObject.Tag.shard, shard);
            index.put(ComObject.Tag.replica, replica);
            index.put(ComObject.Tag.hasDiscrepancy, true);

          }
        }
      }
    }
  }

  private ComObject getSchemaVersion(String dbName, String tableName, String indexName, ComObject highestSchemaVersions) {
    ComArray databases = highestSchemaVersions.getArray(ComObject.Tag.databases);
    for (int i = 0; i < databases.getArray().size(); i++) {
      ComObject db = (ComObject) databases.getArray().get(i);
      String currDbName = db.getString(ComObject.Tag.dbName);
      if (currDbName.equals(dbName)) {
        ComArray tables = db.getArray(ComObject.Tag.tables);
        for (int j = 0; j < tables.getArray().size(); j++) {
          ComObject table = (ComObject) tables.getArray().get(j);
          String currTableName = table.getString(ComObject.Tag.tableName);
          if (currTableName.equals(tableName)) {
            ComArray indices = table.getArray(ComObject.Tag.indices);
            for (int k = 0; k < indices.getArray().size(); k++) {
              ComObject index = (ComObject) indices.getArray().get(k);
              String currIndexName = index.getString(ComObject.Tag.indexName);
              if (currIndexName.equals(indexName)) {
                return index;
              }
            }
          }
        }
      }
    }
    return null;
  }

  private void setHighestSchemaVersion(String dbName, String tableName, int tableSchemaVersion, int shard,
                                       int replica, ComObject highestSchemaVersions, ConcurrentHashMap<String, Integer> tablesFound) {
    ComArray databases = highestSchemaVersions.getArray(ComObject.Tag.databases);
    for (int i = 0; i < databases.getArray().size(); i++) {
      ComObject db = (ComObject) databases.getArray().get(i);
      String currDbName = db.getString(ComObject.Tag.dbName);
      if (currDbName.equals(dbName)) {
        ComArray tables = db.getArray(ComObject.Tag.tables);

        for (int j = 0; j < tables.getArray().size(); j++) {
          ComObject table = (ComObject) tables.getArray().get(i);
          String currTableName = table.getString(ComObject.Tag.tableName);
          tablesFound.remove(currTableName);
          if (currTableName.equals(tableName)) {
            table.put(ComObject.Tag.schemaVersion, tableSchemaVersion);
            table.put(ComObject.Tag.shard, shard);
            table.put(ComObject.Tag.replica, replica);
            table.put(ComObject.Tag.hasDiscrepancy, true);
            return;
          }
        }
        ComObject table = new ComObject();
        tables.add(table);
        table.put(ComObject.Tag.schemaVersion, tableSchemaVersion);
        table.put(ComObject.Tag.shard, shard);
        table.put(ComObject.Tag.replica, replica);
        table.put(ComObject.Tag.hasDiscrepancy, true);
      }
    }
  }

  private ComObject getSchemaVersion(String dbName, String tableName, ComObject highestSchemaVersions) {
    ComArray databases = highestSchemaVersions.getArray(ComObject.Tag.databases);
    for (int i = 0; i < databases.getArray().size(); i++) {
      ComObject db = (ComObject) databases.getArray().get(i);
      String currDbName = db.getString(ComObject.Tag.dbName);
      if (currDbName.equals(dbName)) {
        ComArray tables = db.getArray(ComObject.Tag.tables);
        for (int j = 0; j < tables.getArray().size(); j++) {
          ComObject table = (ComObject) tables.getArray().get(j);
          String currTableName = table.getString(ComObject.Tag.tableName);
          if (currTableName.equals(tableName)) {
            return table;
          }
        }
      }
    }
    return null;
  }

  private static class NullX509TrustManager implements X509TrustManager {
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
    }

    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
    }

    public X509Certificate[] getAcceptedIssuers() {
      return new X509Certificate[0];
    }
  }

  private static class NullHostnameVerifier implements HostnameVerifier {
    public boolean verify(String hostname, SSLSession session) {
      return true;
    }
  }


  private Map<Integer, Map<Integer, Integer>> numberOfCoresPerServer = new HashMap<>();

  private void startMasterLicenseValidator() {

    shutdownMasterValidatorThread = false;

    if (overrideProLicense) {
      logger.info("Overriding pro license");
      haveProLicense = true;
      common.setHaveProLicense(haveProLicense);
      common.saveSchema(getClient(), dataDir);
      return;
    }
    haveProLicense = true;

    final AtomicBoolean haventSet = new AtomicBoolean(true);
    //    if (usingMultipleReplicas) {
    //      throw new InsufficientLicense("You must have a pro license to use multiple replicas");
    //    }

    final AtomicInteger licensePort = new AtomicInteger();
    String json = null;
    try {
      json = IOUtils.toString(DatabaseServer.class.getResourceAsStream("/config-license-server.json"), "utf-8");
    }
    catch (Exception e) {
      logger.error("Error initializing license validator", e);
//      common.setHaveProLicense(false);
//      common.saveSchema(getClient(), dataDir);
      DatabaseServer.this.haveProLicense = false;
      DatabaseServer.this.disableNow = true;
//      this.haveProLicense = true;
//      this.disableNow = false;
//      this.disableDate = null;
//      this.multipleLicenseServers = false;

      haventSet.set(false);
      return;
    }
    try {
      ObjectMapper mapper = new ObjectMapper();
      ObjectNode config = (ObjectNode) mapper.readTree(json);
      licensePort.set(config.get("server").get("port").asInt());
      final String address = config.get("server").get("privateAddress").asText();

      final AtomicBoolean lastHaveProLicense = new AtomicBoolean(false);
      final AtomicBoolean haveHadProLicense = new AtomicBoolean(false);

      doValidateLicense(address, licensePort, lastHaveProLicense, haveHadProLicense, haventSet, false);
      masterLicenseValidatorThread = ThreadUtil.createThread(new Runnable() {
      @Override
      public void run() {
        while (!shutdownMasterValidatorThread) {
          try {
//            DatabaseServer.this.haveProLicense = true;
//            DatabaseServer.this.disableNow = false;
//            DatabaseServer.this.disableDate = null;
//            DatabaseServer.this.multipleLicenseServers = false;

            doValidateLicense(address, licensePort, lastHaveProLicense, haveHadProLicense, haventSet, false);
          }
          catch (Exception e) {
            logger.error("license server not found", e);
            if (haveHadProLicense.get()) {
              DatabaseServer.this.haveProLicense = true;
              lastHaveProLicense.set(true);
              haventSet.set(false);
              DatabaseServer.this.disableNow = false;
              disableDate = DateUtils.fromDate(new Date(System.currentTimeMillis() + 7 * 24 * 60 * 60 * 1000));
            }
            else {
          //  if (haventSet.get() || lastHaveProLicense.get() != false) {
//              common.setHaveProLicense(false);
//              common.saveSchema(getClient(), dataDir);
              DatabaseServer.this.haveProLicense = false;
              lastHaveProLicense.set(false);
              haventSet.set(false);
              DatabaseServer.this.disableNow = true;
              disableDate = DateUtils.fromDate(new Date(System.currentTimeMillis()));
//              DatabaseServer.this.haveProLicense = true;
//              DatabaseServer.this.disableNow = false;
//              DatabaseServer.this.disableDate = null;
//              DatabaseServer.this.multipleLicenseServers = false;

            }
            errorLogger.debug("License server not found", e);
          }
          try {
            Thread.sleep(60 * 1000);
          }
          catch (InterruptedException e) {
            logger.error("Error checking licenses", e);
          }
        }
          }
    }, "SonicBase Master License Validator Thread");
      masterLicenseValidatorThread.start();
    }
    catch (Exception e) {
      logger.error("Error validating licenses", e);
    }
  }

  private void doValidateLicense(String address, AtomicInteger licensePort, AtomicBoolean lastHaveProLicense,
                                 AtomicBoolean haveHadProLicense, AtomicBoolean haventSet, boolean standalone) throws IOException {
    int cores = 0;
    if (!standalone) {
      synchronized (numberOfCoresPerServer) {
        for (Map.Entry<Integer, Map<Integer, Integer>> shardEntry : numberOfCoresPerServer.entrySet()) {
          for (Map.Entry<Integer, Integer> replicaEntry : shardEntry.getValue().entrySet()) {
            cores += replicaEntry.getValue();
          }
        }
      }
    }

    try {
      TrustManager[] trustAllCerts = new TrustManager[]{
          new X509TrustManager() {
            public java.security.cert.X509Certificate[] getAcceptedIssuers() {
              return null;
            }

            public void checkClientTrusted(X509Certificate[] certs, String authType) {
            }

            public void checkServerTrusted(X509Certificate[] certs, String authType) {
            }

          }
      };

      SSLContext sc = SSLContext.getInstance("SSL");
      sc.init(null, trustAllCerts, new java.security.SecureRandom());
      HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());

      // Create all-trusting host name verifier
      HostnameVerifier allHostsValid = new HostnameVerifier() {

        public boolean verify(String hostname, SSLSession session) {
          return true;
        }
      };
      // Install the all-trusting host verifier
      HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);
      /*
       * end of the fix
       */

      URL url = new URL("https://" + address + ":" + licensePort.get() + "/license/checkIn?" +
          "primaryAddress=" + common.getServersConfig().getShards()[0].getReplicas()[0].getPrivateAddress() +
          "&primaryPort=" + common.getServersConfig().getShards()[0].getReplicas()[0].getPort() +
          "&cluster=" + cluster + "&cores=" + cores);
      URLConnection con = url.openConnection();
      InputStream in = new BufferedInputStream(con.getInputStream());

//      HttpResponse response = restGet("https://" + config.getDict("server").getString("publicAddress") + ":" +
//          config.getDict("server").getInt("port") + "/license/currUsage");
      ObjectMapper mapper = new ObjectMapper();
      ObjectNode dict = (ObjectNode) mapper.readTree(IOUtils.toString(in, "utf-8"));

//      HttpResponse response = DatabaseClient.restGet("https://" + address + ":" + licensePort.get() + "/license/checkIn?" +
//        "primaryAddress=" + common.getServersConfig().getShards()[0].getReplicas()[0].getPrivateAddress() +
//        "&primaryPort=" + common.getServersConfig().getShards()[0].getReplicas()[0].getPort() +
//        "&cluster=" + cluster + "&cores=" + cores);
//    String responseStr = StreamUtils.inputStreamToString(response.getContent());
//    logger.info("CheckIn response: " + responseStr);

      this.haveProLicense = dict.get("inCompliance").asBoolean();
      this.disableNow = dict.get("disableNow").asBoolean();
      this.disableDate = dict.get("disableDate").asText();
      this.multipleLicenseServers = dict.get("multipleLicenseServers").asBoolean();
      if (haveProLicense) {
        haveHadProLicense.set(true);
      }
//      this.haveProLicense = true;
//      this.disableNow = false;
//      this.multipleLicenseServers = false;
      logger.info("licenseValidator: cores=" + cores + ", lastHaveProLicense=" + lastHaveProLicense.get() +
          ", haveProLicense=" + haveProLicense + ",  disableNow=" + disableNow);
      if (haventSet.get() || lastHaveProLicense.get() != haveProLicense) {
//        common.setHaveProLicense(haveProLicense);
//        if (!standalone) {
//          common.saveSchema(getClient(), dataDir);
//        }
        lastHaveProLicense.set(haveProLicense);
        haventSet.set(true);
        logger.info("Saving schema with haveProLicense=" + haveProLicense);
      }
    }
    catch (Exception e) {
//      this.haveProLicense = true;
//      this.disableNow = false;
//      this.disableDate = null;
//      this.multipleLicenseServers = false;

      if (haveHadProLicense.get()) {
        Date date = new Date(System.currentTimeMillis());
        Calendar cal = new GregorianCalendar();
        cal.setTime(date);
        cal.add(DAY, 7);
        DatabaseServer.this.disableDate = DateUtils.fromDate(cal.getTime());

        this.haveProLicense = true;
        this.disableNow = false;
        this.multipleLicenseServers = false;
      }
      else {
        this.haveProLicense = false;
        this.disableNow = true;
        DatabaseServer.this.disableDate = DateUtils.fromDate(new Date(System.currentTimeMillis()));
        this.multipleLicenseServers = false;
      }
//      common.setHaveProLicense(haveProLicense);
//      if (!standalone) {
//        common.saveSchema(getClient(), dataDir);
//        logger.error("Error validating license", e);
//      }
      lastHaveProLicense.set(haveProLicense);
      haventSet.set(true);
      logger.error("MasterLicenseValidator error checking licenses", e);
    }
  }

  private void shutdownMasterLicenseValidator() {
    shutdownMasterValidatorThread = true;
    if (masterLicenseValidatorThread != null) {
      masterLicenseValidatorThread.interrupt();

      try {
        masterLicenseValidatorThread.join();
        masterLicenseValidatorThread = null;
      }
      catch (InterruptedException e) {
        throw new DatabaseException(e);
      }
    }
  }

  private void startLicenseValidator() {
    if (overrideProLicense) {
      logger.info("Overriding pro license");
      haveProLicense = true;
      disableNow = false;
//      common.setHaveProLicense(haveProLicense);
//      common.saveSchema(getClient(), dataDir);
      return;
    }
    haveProLicense = true;

    final AtomicBoolean haventSet = new AtomicBoolean(true);
//    if (usingMultipleReplicas) {
//      throw new InsufficientLicense("You must have a pro license to use multiple replicas");
//    }

    final AtomicInteger licensePort = new AtomicInteger();
    String json = null;
    try {
      json = IOUtils.toString(DatabaseServer.class.getResourceAsStream("/config-license-server.json"), "utf-8");
    }
    catch (Exception e) {
      logger.error("Error initializing license validator", e);

//      this.haveProLicense = true;
//      this.disableNow = false;
//      this.disableDate = null;
//      this.multipleLicenseServers = false;

//      common.setHaveProLicense(false);
//      common.saveSchema(getClient(), dataDir);
      DatabaseServer.this.haveProLicense = false;
      DatabaseServer.this.disableNow = true;
      haventSet.set(false);
      return;
    }
    try {
      ObjectMapper mapper = new ObjectMapper();
      ObjectNode config = (ObjectNode) mapper.readTree(json);
      licensePort.set(config.get("server").get("port").asInt());
      final String address = config.get("server").get("privateAddress").asText();

      final AtomicBoolean lastHaveProLicense = new AtomicBoolean(haveProLicense);
      final AtomicBoolean haveHadProLicense = new AtomicBoolean();

      licenseValidatorThread = ThreadUtil.createThread(new Runnable() {
        @Override
        public void run() {
          while (!shutdown) {
            boolean hadError = false;
            try {
              if (!(shard == 0 && common.getServersConfig().getShards()[0].getMasterReplica() == getReplica())) {
                checkLicense(lastHaveProLicense, haveHadProLicense, haventSet);
              }
              else {
                if (haveProLicense) {
                  Date date = new Date(System.currentTimeMillis());
                  Calendar cal = new GregorianCalendar();
                  cal.setTime(date);
                  cal.add(HOUR, 1);
                  DatabaseServer.this.disableDate = DateUtils.fromDate(cal.getTime());

                }
                else {
                  Date date = new Date(System.currentTimeMillis());
                  DatabaseServer.this.disableDate = DateUtils.fromDate(date);
                }
              }
            }
            catch (Exception e) {
              try {
                Date date = disableDate == null ? null : DateUtils.fromString(DatabaseServer.this.disableDate);
                if (date == null || date.getTime() < System.currentTimeMillis()) {
                  disableNow = true;
                  haveProLicense = false;
                }
              }
              catch (ParseException e1) {
                logger.error("Error validating license", e);
              }

              hadError = true;
              logger.error("license server not found", e);
              if (haventSet.get() || lastHaveProLicense.get() != false) {
//                common.setHaveProLicense(false);
                //common.saveSchema(getClient(), dataDir);
                DatabaseServer.this.haveProLicense = false;
                lastHaveProLicense.set(false);
                haventSet.set(false);
                disableNow = true;
              }
              errorLogger.debug("License server not found", e);
            }
            try {
              if (hadError) {
                Thread.sleep(1000);
              }
              else {
                Thread.sleep(60 * 1000);
              }
            }
            catch (InterruptedException e) {
              logger.error("Error checking licenses", e);
            }
          }
        }
      }, "SonicBase License Validator Thread");
      licenseValidatorThread.start();
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private void checkLicense(AtomicBoolean lastHaveProLicense, AtomicBoolean haveHadProLicense, AtomicBoolean haventSet) {
    int cores = Runtime.getRuntime().availableProcessors();

    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.dbName, "__none__");
    cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
    cobj.put(ComObject.Tag.method, "licenseCheckin");
    cobj.put(ComObject.Tag.shard, shard);
    cobj.put(ComObject.Tag.replica, replica);
    cobj.put(ComObject.Tag.coreCount, cores);
    byte[] ret = null;
    if (0 == shard && common.getServersConfig().getShards()[0].getMasterReplica() == replica) {
      ret = licenseCheckin(cobj).serialize();
    }
    else {
      ret = client.get().sendToMaster(cobj);
    }
    ComObject retObj = new ComObject(ret);

    DatabaseServer.this.haveProLicense = retObj.getBoolean(ComObject.Tag.inCompliance);
    DatabaseServer.this.disableNow = retObj.getBoolean(ComObject.Tag.disableNow);
    //DatabaseServer.this.disableDate = retObj.getString(ComObject.Tag.disableDate);

    if (haveProLicense) {
      Date date = new Date(System.currentTimeMillis());
      Calendar cal = new GregorianCalendar();
      cal.setTime(date);
      cal.add(HOUR, 1);
      DatabaseServer.this.disableDate = DateUtils.fromDate(cal.getTime());

    }
    else {
      Date date = new Date(System.currentTimeMillis());
      DatabaseServer.this.disableDate = DateUtils.fromDate(date);
    }

    haveHadProLicense.set(haveProLicense);

    DatabaseServer.this.multipleLicenseServers = retObj.getBoolean(ComObject.Tag.multipleLicenseServers);

//    DatabaseServer.this.haveProLicense = true;
//    DatabaseServer.this.disableNow = false;
//    DatabaseServer.this.disableDate = null;
//    DatabaseServer.this.multipleLicenseServers = false;

    logger.info("licenseCheckin: lastHaveProLicense=" + lastHaveProLicense.get() + ", haveProLicense=" + haveProLicense +
        ", disableNow=" + disableNow + ", disableDate=" + disableDate + ", multipleLicenseServers=" + multipleLicenseServers);
    if (haventSet.get() || lastHaveProLicense.get() != haveProLicense) {
//      common.setHaveProLicense(haveProLicense);
      //common.saveSchema(getClient(), dataDir);
      lastHaveProLicense.set(haveProLicense);
      haventSet.set(false);
      logger.info("Saving schema with haveProLicense=" + haveProLicense);
    }
  }

  public ComObject licenseCheckin(ComObject cobj) {
    int shard = cobj.getInt(ComObject.Tag.shard);
    int replica = cobj.getInt(ComObject.Tag.replica);
    int cores = cobj.getInt(ComObject.Tag.coreCount);
    synchronized (numberOfCoresPerServer) {
      Map<Integer, Integer> replicaMap = numberOfCoresPerServer.get(shard);
      if (replicaMap == null) {
        replicaMap = new HashMap<>();
        numberOfCoresPerServer.put(shard, replicaMap);
      }
      replicaMap.put(replica, cores);
    }

    ComObject retObj = new ComObject();
    retObj.put(ComObject.Tag.inCompliance, this.haveProLicense);
    retObj.put(ComObject.Tag.disableNow, this.disableNow);
    if (disableDate != null) {
      retObj.put(ComObject.Tag.disableDate, this.disableDate);
    }
    if (multipleLicenseServers == null) {
      retObj.put(ComObject.Tag.multipleLicenseServers, false);
    }
    else {
      retObj.put(ComObject.Tag.multipleLicenseServers, this.multipleLicenseServers);
    }
    return retObj;
  }

  public ComObject prepareForBackup(ComObject cobj) {

    deltaManager.enableSnapshot(false);

    logSlicePoint = logManager.sliceLogs(true);

    isBackupComplete = false;

    backupException = null;

    ComObject ret = new ComObject();
    ret.put(ComObject.Tag.replica, replica);
    return ret;
  }

  public long getBackupLocalFileSystemSize() {
    long size = 0;
    size += deleteManager.getBackupLocalFileSystemSize();
    size += longRunningCommands.getBackupLocalFileSystemSize();
    size += deltaManager.getBackupLocalFileSystemSize();
    //size += logManager.getBackupLocalFileSystemSize();
    return size;
  }

  public long getBackupS3Size(String bucket, String prefix, String subDirectory) {
    AWSClient client = new AWSClient(/*getDatabaseClient()*/ null);
    return client.getDirectorySize(bucket, prefix, subDirectory);
  }

  public ComObject getBackupStatus(final ComObject obj) {

    List<Future> futures = new ArrayList<>();
    long srcSize = 0;
    long destSize = 0;
    for (int i = 0; i < shardCount; i++) {
      for (int j = 0; j < replicationFactor; j++) {

        final int finalI = i;
        final int finalJ = j;
        futures.add(executor.submit(new Callable(){
          @Override
          public Object call() throws Exception {
            ComObject cobj = new ComObject();
            cobj.put(ComObject.Tag.dbName, "__none__");
            cobj.put(ComObject.Tag.schemaVersion, getCommon().getSchemaVersion());
            cobj.put(ComObject.Tag.method, "doGetBackupSizes");

            ComObject ret = new ComObject(getDatabaseClient().send(null, finalI, finalJ, cobj, DatabaseClient.Replica.specified));
            return ret;
          }
        }));
      }
    }

    boolean error = false;
    for (Future future : futures) {
      try {
        ComObject ret = (ComObject) future.get();
        srcSize += ret.getLong(ComObject.Tag.sourceSize);
        String type = backupConfig.get("type").asText();
        if (type.equals("AWS")) {
          destSize = ret.getLong(ComObject.Tag.destSize);
        }
        else if (backupConfig.has("sharedDirectory")) {
          destSize = ret.getLong(ComObject.Tag.destSize);
        }
        else {
          destSize += ret.getLong(ComObject.Tag.destSize);
        }
        boolean localError = ret.getBoolean(ComObject.Tag.error);
        if (localError) {
          error = true;
        }
      }
      catch (InterruptedException e) {
        throw new DatabaseException(e);
      }
      catch (ExecutionException e) {
        e.printStackTrace();
      }
    }
    ComObject retObj = new ComObject();
    double percent = destSize == 0 || srcSize == 0 ? 0d : (double)destSize / (double)srcSize;
    percent = Math.min(percent, 01.0d);
    retObj.put(ComObject.Tag.percentComplete, percent     );
    retObj.put(ComObject.Tag.error, error);
    return retObj;
  }

  public ComObject doGetBackupSizes(final ComObject obj) {

    long begin = System.currentTimeMillis();
    long srcSize = 0;
    long end = 0;
    long destSize = 0;
    long beginDest = 0;
    boolean error = false;
    try {
      srcSize = getBackupLocalFileSystemSize();
      end = System.currentTimeMillis();
      destSize = 0;
      String type = backupConfig.get("type").asText();
      beginDest = System.currentTimeMillis();
      if (type.equals("AWS")) {
        String bucket = backupConfig.get("bucket").asText();
        String prefix = backupConfig.get("prefix").asText();
        destSize = getBackupS3Size(bucket, prefix, lastBackupDir);
      }
      else {
        destSize = FileUtils.sizeOfDirectory(new File(backupFileSystemDir, lastBackupDir));
      }
    }
    catch (Exception e) {
      logger.error("Error getting backup sizes", e);
      error = true;
    }
    long endDest = System.currentTimeMillis();
    ComObject retObj = new ComObject();
    retObj.put(ComObject.Tag.sourceSize, srcSize);
    retObj.put(ComObject.Tag.destSize, destSize);
    retObj.put(ComObject.Tag.error, error || backupException != null);

    logger.info("backup sizes: shard=" + shard + ", replica=" + replica +
        ", srcTime=" + (end - begin) + ", destTime=" + (endDest - beginDest) + ", srcSize=" + srcSize + ", destSize=" + destSize);
    return retObj;
  }

  public ComObject getRestoreStatus(final ComObject obj) {

    if (finishedRestoreFileCopy) {
      return getRecoverProgress();
    }
    else {
      List<Future> futures = new ArrayList<>();
      long srcSize = 0;
      long destSize = 0;
      for (int i = 0; i < shardCount; i++) {
        for (int j = 0; j < replicationFactor; j++) {

          final int finalI = i;
          final int finalJ = j;
          futures.add(executor.submit(new Callable(){
            @Override
            public Object call() throws Exception {
              ComObject cobj = new ComObject();
              cobj.put(ComObject.Tag.dbName, "__none__");
              cobj.put(ComObject.Tag.schemaVersion, getCommon().getSchemaVersion());
              cobj.put(ComObject.Tag.method, "doGetRestoreSizes");

              ComObject ret = new ComObject(getDatabaseClient().send(null, finalI, finalJ, cobj, DatabaseClient.Replica.specified));
              return ret;
            }
          }));
        }
      }

      boolean error = false;
      for (Future future : futures) {
        try {
          ComObject ret = (ComObject) future.get();
          String type = backupConfig.get("type").asText();
          if (type.equals("AWS")) {
            srcSize = ret.getLong(ComObject.Tag.sourceSize);
          }
          else if (backupConfig.has("sharedDirectory")) {
            srcSize = ret.getLong(ComObject.Tag.sourceSize);
          }
          else {
            srcSize += ret.getLong(ComObject.Tag.sourceSize);
          }
          boolean localError = ret.getBoolean(ComObject.Tag.error);
          if (localError) {
            error = true;
          }
          srcSize *= (double)replicationFactor;
          destSize += ret.getLong(ComObject.Tag.destSize);
        }
        catch (InterruptedException e) {
          throw new DatabaseException(e);
        }
        catch (ExecutionException e) {
          e.printStackTrace();
        }
      }
      ComObject retObj = new ComObject();
      double percent = destSize == 0 || srcSize == 0 ? 0d : (double)destSize / (double)srcSize;
      percent = Math.min(percent, 01.0d);
      retObj.put(ComObject.Tag.percentComplete, percent);
      retObj.put(ComObject.Tag.stage, "copyingFiles");
      retObj.put(ComObject.Tag.error, error);
      return retObj;
    }
  }

  public ComObject doGetRestoreSizes(final ComObject obj) {
    boolean error = false;
    long destSize = 0;
    long srcSize = 0;
    try {
      destSize = getBackupLocalFileSystemSize();
      srcSize = 0;
      String type = backupConfig.get("type").asText();
      if (type.equals("AWS")) {
        String bucket = backupConfig.get("bucket").asText();
        String prefix = backupConfig.get("prefix").asText();
        srcSize = getBackupS3Size(bucket, prefix, lastBackupDir);
      }
      else {
        srcSize = FileUtils.sizeOfDirectory(new File(backupFileSystemDir, lastBackupDir));
      }
    }
    catch (Exception e) {
      logger.error("Error getting restore sizes", e);
      error = true;
    }
    ComObject retObj = new ComObject();
    retObj.put(ComObject.Tag.sourceSize, srcSize);
    retObj.put(ComObject.Tag.destSize, destSize);
    retObj.put(ComObject.Tag.error, error || restoreException != null);
    logger.info("restore sizes: shard=" + shard + ", replica=" + replica + ", srcSize=" + srcSize + ", destSize=" + destSize);
    return retObj;
  }

  public ComObject doBackupFileSystem(final ComObject cobj) {
    backupFileSystemThread = ThreadUtil.createThread(new Runnable() {

      @Override
      public void run() {
        try {
          String directory = cobj.getString(ComObject.Tag.directory);
          String subDirectory = cobj.getString(ComObject.Tag.subDirectory);

          directory = directory.replace("$HOME", System.getProperty("user.home"));

          lastBackupDir = subDirectory;
          backupFileSystemDir = directory;

          backupFileSystemDir = directory;
          backupFileSystemSingleDir(directory, subDirectory, "logSequenceNum");
          backupFileSystemSingleDir(directory, subDirectory, "nextRecordId");
          deleteManager.delteTempDirs();
          deleteManager.backupFileSystem(directory, subDirectory);
          longRunningCommands.backupFileSystem(directory, subDirectory);
//          snapshotManager.deleteTempDirs();
//          snapshotManager.backupFileSystem(directory, subDirectory);
          deltaManager.enableSnapshot(false);
          deltaManager.deleteTempDirs();
          deltaManager.deleteDeletedDirs();
          deltaManager.backupFileSystem(directory, subDirectory);
          synchronized (common) {
            //snapshotManager.backupFileSystemSchema(directory, subDirectory);
            deltaManager.backupFileSystemSchema(directory, subDirectory);
          }
          logManager.backupFileSystem(directory, subDirectory, logSlicePoint);

          deltaManager.enableSnapshot(true);

          isBackupComplete = true;

        }
        catch (Exception e) {
          logger.error("Error backing up database", e);
          backupException = e;
        }
      }
    }, "SonicBase Backup FileSystem Thread");
    backupFileSystemThread.start();
    return null;
  }

  private void backupFileSystemSingleDir(String directory, String subDirectory, String dirName) {
    try {
      File dir = new File(getDataDir(), dirName + "/" + shard + "/0");
      File destDir = new File(directory, subDirectory + "/" + dirName + "/" + shard + "/0");
      if (dir.exists()) {
        FileUtils.copyDirectory(dir, destDir);
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public ComObject doBackupAWS(final ComObject cobj) {
    backupAWSThread = ThreadUtil.createThread(new Runnable() {
      @Override
      public void run() {
        try {
          String subDirectory = cobj.getString(ComObject.Tag.subDirectory);
          String bucket = cobj.getString(ComObject.Tag.bucket);
          String prefix = cobj.getString(ComObject.Tag.prefix);

          lastBackupDir = subDirectory;

          backupAWSSingleDir(bucket, prefix, subDirectory, "logSequenceNum");
          backupAWSSingleDir(bucket, prefix, subDirectory, "nextRecordId");
          deleteManager.delteTempDirs();
          deleteManager.backupAWS(bucket, prefix, subDirectory);
          longRunningCommands.backupAWS(bucket, prefix, subDirectory);
          //snapshotManager.deleteTempDirs();
          deltaManager.enableSnapshot(false);
          deltaManager.deleteDeletedDirs();
          deltaManager.deleteTempDirs();
          //snapshotManager.backupAWS(bucket, prefix, subDirectory);
          deltaManager.backupAWS(bucket, prefix, subDirectory);
          synchronized (common) {
            //snapshotManager.backupAWSSchema(bucket, prefix, subDirectory);
            deltaManager.backupAWSSchema(bucket, prefix, subDirectory);
          }
          logManager.backupAWS(bucket, prefix, subDirectory, logSlicePoint);

          deltaManager.enableSnapshot(true);

          isBackupComplete = true;
        }
        catch (Exception e) {
          logger.error("Error backing up database", e);
          backupException = e;
        }
      }
    }, "SonicBase Backup AWS Thread");
    backupAWSThread.start();
    return null;
  }

  private void backupAWSSingleDir(String bucket, String prefix, String subDirectory, String dirName) {
    AWSClient awsClient = getAWSClient();
    File srcDir = new File(getDataDir(), dirName + "/" + shard + "/" + replica);
    if (srcDir.exists()) {
      subDirectory += "/" + dirName + "/" + shard + "/0";

      awsClient.uploadDirectory(bucket, prefix, subDirectory, srcDir);
    }
  }

  public ComObject isBackupComplete(ComObject cobj) {
    try {
//      if (backupException != null) {
//        throw new DatabaseException(backupException);
//      }
      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.isComplete, isBackupComplete);
      return retObj;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public ComObject finishBackup(ComObject cobj) {
    try {
      boolean shared = cobj.getBoolean(ComObject.Tag.shared);
      String directory = cobj.getString(ComObject.Tag.directory);
      int maxBackupCount = cobj.getInt(ComObject.Tag.maxBackupCount);
      String type = cobj.getString(ComObject.Tag.type);

      if (type.equals("fileSystem")) {
        if (!shared) {
          doDeleteFileSystemBackups(directory, maxBackupCount);
        }
      }
      deltaManager.enableSnapshot(true);
      isBackupComplete = false;
      return null;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private void doDeleteFileSystemBackups(String directory, int maxBackupCount) {
    File file = new File(directory);
    File[] backups = file.listFiles();
    if (backups != null) {
      Arrays.sort(backups, new Comparator<File>() {
        @Override
        public int compare(File o1, File o2) {
          return o1.getAbsolutePath().compareTo(o2.getAbsolutePath());
        }
      });
      for (int i = 0; i < backups.length; i++) {
        if (i > maxBackupCount) {
          try {
            FileUtils.deleteDirectory(backups[i]);
          }
          catch (Exception e) {
            logger.error("Error deleting backup: dir=" + backups[i].getAbsolutePath(), e);
          }
        }
      }
    }
  }

  public ComObject isEntireBackupComplete(ComObject cobj) {
    try {
      if (finalBackupException != null) {
        throw new DatabaseException("Error performing backup", finalBackupException);
      }
      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.isComplete, !doingBackup);

      return retObj;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public byte[] startBackup(ComObject cobj) {

    if (!common.haveProLicense()) {
      throw new InsufficientLicense("You must have a pro license to start a backup");
    }

    final boolean wasDoingBackup = doingBackup;
    doingBackup = true;
    backupMainThread = ThreadUtil.createThread(new Runnable() {
      @Override
      public void run() {
        try {
          if (wasDoingBackup) {
            while (doingBackup) {
              Thread.sleep(2000);
            }
          }
          doingBackup = true;
          doBackup();
        }
        catch (Exception e) {
          logger.error("Error doing backup", e);
        }
      }
    }, "SonicBase Backup Main Thread");
    backupMainThread.start();
    return null;
  }

  static class BackupJob implements Job {

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
      JobDataMap map = jobExecutionContext.getMergedJobDataMap();
      DatabaseServer server = (DatabaseServer) map.get("server");
      server.doBackup();
    }
  }

  public void scheduleBackup() {
    try {
      ObjectNode backup = (ObjectNode) config.get("backup");
      if (backupConfig == null) {
        backupConfig = backup;
      }
      if (backupConfig == null) {
        return;
      }
      JsonNode cronSchedule = backupConfig.get("cronSchedule");
      if (cronSchedule == null) {
        return;
      }
      JobDataMap map = new JobDataMap();
      map.put("server", this);

      logger.info("Scheduling backup: cronSchedule=" + cronSchedule);

      JobDetail job = newJob(BackupJob.class)
          .withIdentity("job" + cronIdentity, "group1")
          .usingJobData(map)
          .build();
      Trigger trigger = newTrigger()
          .withIdentity("trigger" + cronIdentity, "group1")
          .withSchedule(cronSchedule(cronSchedule.asText()))
          .forJob("myJob" + cronIdentity, "group1")
          .build();
      cronIdentity++;

      SchedulerFactory sf = new StdSchedulerFactory();
      Scheduler sched = sf.getScheduler();
      sched.scheduleJob(job, trigger);
      sched.start();
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private String lastBackupDir;
  private String backupFileSystemDir;


  public ComObject getLastBackupDir(ComObject cobj) {
    ComObject retObj = new ComObject();
    if (lastBackupDir != null) {
      retObj.put(ComObject.Tag.directory, lastBackupDir);
    }
    return retObj;
  }

  public void doBackup() {
    try {
      shutdownRepartitioner();
      finalBackupException = null;
      doingBackup = true;

      logger.info("Backup Master - begin");

      logger.info("Backup Master - prepareForBackup - begin");
      // tell all servers to pause snapshot and slice the queue
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.dbName, "__none__");
      cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
      cobj.put(ComObject.Tag.method, "prepareForBackup");
      byte[][] ret = getDatabaseClient().sendToAllShards(null, 0, cobj, DatabaseClient.Replica.master);
      int[] masters = new int[shardCount];
      for (int i = 0; i < ret.length; i++) {
        ComObject retObj = new ComObject(ret[i]);
        masters[i] = retObj.getInt(ComObject.Tag.replica);
      }
      logger.info("Backup Master - prepareForBackup - finished");

      String subDirectory = DateUtils.toString(new Date(System.currentTimeMillis()));
      lastBackupDir = subDirectory;
      ObjectNode backup = (ObjectNode) config.get("backup");
      if (backupConfig == null) {
        backupConfig = backup;
      }

      String type = backupConfig.get("type").asText();
      if (type.equals("AWS")) {
        String bucket = backupConfig.get("bucket").asText();
        String prefix = backupConfig.get("prefix").asText();

        // if aws
        //    tell all servers to upload with a specific root directory
        logger.info("Backup Master - doBackupAWS - begin");

        ComObject docobj = new ComObject();
        docobj.put(ComObject.Tag.dbName, "__none__");
        docobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
        docobj.put(ComObject.Tag.method, "doBackupAWS");
        docobj.put(ComObject.Tag.subDirectory, subDirectory);
        docobj.put(ComObject.Tag.bucket, bucket);
        docobj.put(ComObject.Tag.prefix, prefix);

        for (int i = 0; i < shardCount; i++) {
          getDatabaseClient().send(null, i, masters[i], docobj, DatabaseClient.Replica.specified);
        }

        logger.info("Backup Master - doBackupAWS - end");
      }
      else if (type.equals("fileSystem")) {
        // if fileSystem
        //    tell all servers to copy files to backup directory with a specific root directory
        String directory = backupConfig.get("directory").asText();

        logger.info("Backup Master - doBackupFileSystem - begin");

        ComObject docobj = new ComObject();
        docobj.put(ComObject.Tag.dbName, "__none__");
        docobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
        docobj.put(ComObject.Tag.method, "doBackupFileSystem");
        docobj.put(ComObject.Tag.directory, directory);
        docobj.put(ComObject.Tag.subDirectory, subDirectory);
        for (int i = 0; i < shardCount; i++) {
          getDatabaseClient().send(null, i, masters[i], docobj, DatabaseClient.Replica.specified);
        }

        logger.info("Backup Master - doBackupFileSystem - end");
      }

      while (!shutdown) {
        ComObject iscobj = new ComObject();
        iscobj.put(ComObject.Tag.dbName, "__none__");
        iscobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
        iscobj.put(ComObject.Tag.method, "isBackupComplete");

        boolean finished = false;
        outer:
        for (int shard = 0; shard < shardCount; shard++) {
          try {
            byte[] currRet = getDatabaseClient().send(null, shard, masters[shard], iscobj, DatabaseClient.Replica.specified);
            ComObject retObj = new ComObject(currRet);
            finished = retObj.getBoolean(ComObject.Tag.isComplete);
            if (!finished) {
              break outer;
            }
          }
          catch (Exception e) {
            finished = false;
            logger.error("Error checking if backup complete: shard=" + shard);
            break outer;
          }
        }
        if (finished) {
          break;
        }
        Thread.sleep(1000);
      }
      logger.info("Backup Master - doBackup finished");

      logger.info("Backup Master - delete old backups - begin");

      Integer maxBackupCount = backupConfig.get("maxBackupCount").asInt();
      String directory = null;
      if (backupConfig.has("directory")) {
        directory = backupConfig.get("directory").asText();
      }
      Boolean shared = null;
      if (backupConfig.has("sharedDirectory")) {
        shared = backupConfig.get("sharedDirectory").asBoolean();
      }
      if (shared == null) {
        shared = false;
      }
      if (maxBackupCount != null) {
        try {
          // delete old backups
          if (type.equals("AWS")) {
            String bucket = backupConfig.get("bucket").asText();
            String prefix = backupConfig.get("prefix").asText();
            shared = true;
            doDeleteAWSBackups(bucket, prefix, maxBackupCount);
          }
          else if (type.equals("fileSystem")) {
            if (shared) {
              doDeleteFileSystemBackups(directory, maxBackupCount);
            }
          }
        }
        catch (Exception e) {
          logger.error("Error deleting old backups", e);
        }
      }
      logger.info("Backup Master - delete old backups - finished");

      logger.info("Backup Master - finishBackup - begin");

      ComObject fcobj = new ComObject();
      fcobj.put(ComObject.Tag.dbName, "__none__");
      fcobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
      fcobj.put(ComObject.Tag.method, "finishBackup");
      fcobj.put(ComObject.Tag.shared, shared);
      if (directory != null) {
        fcobj.put(ComObject.Tag.directory, directory);
      }
      fcobj.put(ComObject.Tag.type, type);
      fcobj.put(ComObject.Tag.maxBackupCount, maxBackupCount);
      for (int i = 0; i < shardCount; i++) {
        getDatabaseClient().send(null, i, masters[i], fcobj, DatabaseClient.Replica.specified);
      }

      logger.info("Backup Master - finishBackup - finished");
    }
    catch (Exception e) {
      finalBackupException = e;
      logger.error("Error performing backup", e);
      throw new DatabaseException(e);
    }
    finally {
      doingBackup = false;
      startRepartitioner();
      logger.info("Backup - finished");
    }
  }

  private void doDeleteAWSBackups(String bucket, String prefix, Integer maxBackupCount) {
    List<String> dirs = awsClient.listDirectSubdirectories(bucket, prefix);
    Collections.sort(dirs, new Comparator<String>() {
      @Override
      public int compare(String o1, String o2) {
        return -1 * o1.compareTo(o2);
      }
    });
    for (int i = 0; i < dirs.size(); i++) {
      if (i >= maxBackupCount) {
        try {
          awsClient.deleteDirectory(bucket, prefix + "/" + dirs.get(i));
        }
        catch (Exception e) {
          logger.error("Error deleting backup from AWS: dir=" + prefix + "/" + dirs.get(i), e);
        }
      }
    }
  }

  public ComObject prepareForRestore(ComObject cobj) {
    try {
      isRestoreComplete = false;

      purgeMemory();

      //isRunning.set(false);
      deltaManager.enableSnapshot(false);
      Thread.sleep(5000);
      //snapshotManager.deleteSnapshots();
      deltaManager.deleteSnapshots();

      File file = new File(getDataDir(), "result-sets/" + getShard() + "/" + getReplica());
      FileUtils.deleteDirectory(file);

      logManager.deleteLogs();

      synchronized (common) {
        getClient().syncSchema();
        common.saveSchema(getClient(), getDataDir());
      }

      return null;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public ComObject doRestoreFileSystem(final ComObject cobj) {

    restoreFileSystemThread = ThreadUtil.createThread(new Runnable() {
      @Override
      public void run() {
        try {
          finishedRestoreFileCopy = false;
          restoreException = null;

          String directory = cobj.getString(ComObject.Tag.directory);
          String subDirectory = cobj.getString(ComObject.Tag.subDirectory);

          backupFileSystemDir = directory;
          lastBackupDir = subDirectory;
          directory = directory.replace("$HOME", System.getProperty("user.home"));

          restoreFileSystemSingleDir(directory, subDirectory, "logSequenceNum");
          restoreFileSystemSingleDir(directory, subDirectory, "nextRecordId");
          deleteManager.restoreFileSystem(directory, subDirectory);
          longRunningCommands.restoreFileSystem(directory, subDirectory);
          //snapshotManager.restoreFileSystem(directory, subDirectory);
          deltaManager.restoreFileSystem(directory, subDirectory);

//          File file = new File(directory, subDirectory);
//          file = new File(file, "snapshot/" + getShard() + "/0/schema.bin");
//          BufferedInputStream in = new BufferedInputStream(new FileInputStream(file));
//          ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
//          IOUtils.copy(in, bytesOut);
          // bytesOut.close();
//
//          synchronized (common) {
//            common.deserializeSchema(bytesOut.toByteArray());
//            common.saveSchema(getClient(), getDataDir());
//          }
          logManager.restoreFileSystem(directory, subDirectory);

          finishedRestoreFileCopy = true;

          prepareDataFromRestore();

          //deleteManager.forceDeletes();

//          synchronized (common) {
//            if (shard != 0 || common.getServersConfig().getShards()[0].masterReplica != replica) {
//              long schemaVersion = common.getSchemaVersion() + 100;
//              common.setSchemaVersion(schemaVersion);
//            }
//            common.saveSchema(getClient(), getDataDir());
//          }
//          pushSchema();

          isRestoreComplete = true;
        }
        catch (Exception e) {
          logger.error("Error restoring backup", e);
          restoreException = e;
        }
      }
    }, "SonicBase Restore FileSystem Thread");
    restoreFileSystemThread.start();

    return null;
  }

  private void restoreFileSystemSingleDir(String directory, String subDirectory, String dirName) {
    try {
      File destDir = new File(getDataDir(), dirName + "/" + shard + "/" + replica);
      if (destDir.exists()) {
        FileUtils.deleteDirectory(destDir);
      }
      destDir.mkdirs();
      File srcDir = new File(directory, subDirectory + "/" + dirName + "/" + shard + "/0");
      if (srcDir.exists()) {
        FileUtils.copyDirectory(srcDir, destDir);
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }

  }

  public ComObject doRestoreAWS(final ComObject cobj) {
    restoreAWSThread = ThreadUtil.createThread(new Runnable() {
      @Override
      public void run() {
        try {
          restoreException = null;
          finishedRestoreFileCopy = false;
          //synchronized (restoreAwsMutex) {
          String subDirectory = cobj.getString(ComObject.Tag.subDirectory);
          String bucket = cobj.getString(ComObject.Tag.bucket);
          String prefix = cobj.getString(ComObject.Tag.prefix);

          lastBackupDir = subDirectory;
          restoreAWSSingleDir(bucket, prefix, subDirectory, "logSequenceNum");
          restoreAWSSingleDir(bucket, prefix, subDirectory, "nextRecordId");
          deleteManager.restoreAWS(bucket, prefix, subDirectory);
          longRunningCommands.restoreAWS(bucket, prefix, subDirectory);
          //snapshotManager.restoreAWS(bucket, prefix, subDirectory);
          deltaManager.restoreAWS(bucket, prefix, subDirectory);
          logManager.restoreAWS(bucket, prefix, subDirectory);

          finishedRestoreFileCopy = true;

          prepareDataFromRestore();

          isRestoreComplete = true;
          //}
        }
        catch (Exception e) {
          logger.error("Error restoring backup", e);
          restoreException = e;
        }
      }
    }, "SonicBase Restore AWS Thread");
    restoreAWSThread.start();

    return null;
  }

  private void restoreAWSSingleDir(String bucket, String prefix, String subDirectory, String dirName) {
    try {
      AWSClient awsClient = getAWSClient();
      File destDir = new File(getDataDir(), dirName + "/" + shard + "/" + replica);
      subDirectory += "/" + dirName + "/" + shard + "/0";

      FileUtils.deleteDirectory(destDir);
      destDir.mkdirs();

      awsClient.downloadDirectory(bucket, prefix, subDirectory, destDir);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public ComObject isRestoreComplete(ComObject cobj) {
    try {
//      if (restoreException != null) {
//        throw new DatabaseException(restoreException);
//      }
      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.isComplete, isRestoreComplete);
      return retObj;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }


  public ComObject finishRestore(ComObject cobj) {
    try {
      deltaManager.enableSnapshot(true);
      //isRunning.set(true);
      isRestoreComplete = false;
      return null;
    }
    catch (Exception e) {
      logger.error("Error finishing restore", e);
      throw new DatabaseException(e);
    }
  }

  private void prepareDataFromRestore() throws Exception {
    for (String dbName : getDbNames(getDataDir())) {
      getDeltaManager().recoverFromSnapshot(dbName);
    }
    getLogManager().applyLogs();
  }

  public ComObject isEntireRestoreComplete(ComObject cobj) {
    try {
      if (finalRestoreException != null) {
        throw new DatabaseException("Error restoring backup", finalRestoreException);
      }
      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.isComplete, !doingRestore);

      return retObj;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public ComObject startRestore(final ComObject cobj) {
    if (!common.haveProLicense()) {
      throw new InsufficientLicense("You must have a pro license to start a restore");
    }
    doingRestore = true;
    restoreMainThread = ThreadUtil.createThread(new Runnable() {
      @Override
      public void run() {
        try {
          String directory = cobj.getString(ComObject.Tag.directory);

          doRestore(directory);
        }
        catch (Exception e) {
          logger.error("Error restoring backup", e);
        }
      }
    }, "SonicBase Restore Main Thread");
    restoreMainThread.start();
    return null;
  }

  private Exception finalRestoreException;
  private Exception finalBackupException;

  private void doRestore(String subDirectory) {
    try {
      shutdownRepartitioner();
      deltaManager.shutdown();

      finalRestoreException = null;
      doingRestore = true;

      ObjectNode backup = (ObjectNode) config.get("backup");
      if (backupConfig == null) {
        backupConfig = backup;
      }
      String type = backupConfig.get("type").asText();

      if (type.equals("AWS")) {
        String bucket = backupConfig.get("bucket").asText();
        String prefix = backupConfig.get("prefix").asText();

        String key = null;
        if (USE_SNAPSHOT_MGR_OLD) {
          key = prefix + "/" + subDirectory + "/snapshot/" + getShard() + "/0/schema.bin";
        }
        else {
          key = prefix + "/" + subDirectory + "/delta/" + getShard() + "/0/schema.bin";
        }
        byte[] bytes = awsClient.downloadBytes(bucket, key);
        synchronized (common) {
          common.deserializeSchema(bytes);
          common.saveSchema(getClient(), getDataDir());
        }
      }
      else if (type.equals("fileSystem")) {
        String currDirectory = backupConfig.get("directory").asText();
        currDirectory = currDirectory.replace("$HOME", System.getProperty("user.home"));

        File file = new File(currDirectory, subDirectory);
        if (USE_SNAPSHOT_MGR_OLD) {
          file = new File(file, "snapshot/" + getShard() + "/0/schema.bin");
        }
        else {
          file = new File(file, "delta/" + getShard() + "/0/schema.bin");
        }
        BufferedInputStream in = new BufferedInputStream(new FileInputStream(file));
        byte[] bytes = IOUtils.toByteArray(in);

        synchronized (common) {
          common.deserializeSchema(bytes);
          common.saveSchema(getClient(), getDataDir());
        }
      }
      //pushSchema();

      //common.clearSchema();

      // delete snapshots and logs
      // enter recovery mode (block commands)
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.dbName, "__none__");
      cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
      cobj.put(ComObject.Tag.method, "prepareForRestore");
      byte[][] ret = getDatabaseClient().sendToAllShards(null, 0, cobj, DatabaseClient.Replica.all, true);



      if (type.equals("AWS")) {
        // if aws
        //    tell all servers to upload with a specific root directory

        String bucket = backupConfig.get("bucket").asText();
        String prefix = backupConfig.get("prefix").asText();
        cobj = new ComObject();
        cobj.put(ComObject.Tag.dbName, "__none__");
        cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
        cobj.put(ComObject.Tag.method, "doRestoreAWS");
        cobj.put(ComObject.Tag.bucket, bucket);
        cobj.put(ComObject.Tag.prefix, prefix);
        cobj.put(ComObject.Tag.subDirectory, subDirectory);
        ret = getDatabaseClient().sendToAllShards(null, 0, cobj, DatabaseClient.Replica.all, true);
      }
      else if (type.equals("fileSystem")) {
        // if fileSystem
        //    tell all servers to copy files to backup directory with a specific root directory
        String directory = backupConfig.get("directory").asText();
        cobj = new ComObject();
        cobj.put(ComObject.Tag.dbName, "__none__");
        cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
        cobj.put(ComObject.Tag.method, "doRestoreFileSystem");
        cobj.put(ComObject.Tag.directory, directory);
        cobj.put(ComObject.Tag.subDirectory, subDirectory);
        ret = getDatabaseClient().sendToAllShards(null, 0, cobj, DatabaseClient.Replica.all, true);
      }

      while (!shutdown) {
        cobj = new ComObject();
        cobj.put(ComObject.Tag.dbName, "__none__");
        cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
        cobj.put(ComObject.Tag.method, "isRestoreComplete");

        boolean finished = false;
        outer:
        for (int shard = 0; shard < shardCount; shard++) {
          for (int replica = 0; replica < replicationFactor; replica++) {
            try {
              byte[] currRet = getDatabaseClient().send(null, shard, replica, cobj, DatabaseClient.Replica.specified, true);
              ComObject retObj = new ComObject(currRet);
              finished = retObj.getBoolean(ComObject.Tag.isComplete);
              if (!finished) {
                break outer;
              }
            }
            catch (Exception e) {
              logger.error("Error checking if restore is complete", e);
              finished = true;
              throw e;
            }
          }
        }
        if (finished) {
          break;
        }
        Thread.sleep(2000);
      }

      synchronized (common) {
        if (shard != 0 || common.getServersConfig().getShards()[0].getMasterReplica() != replica) {
          int schemaVersion = common.getSchemaVersion() + 100;
          common.setSchemaVersion(schemaVersion);
        }
        common.saveSchema(getClient(), getDataDir());
      }
      pushSchema();

      cobj = new ComObject();
      cobj.put(ComObject.Tag.dbName, "__none__");
      cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
      cobj.put(ComObject.Tag.method, "finishRestore");

      ret = getDatabaseClient().sendToAllShards(null, 0, cobj, DatabaseClient.Replica.all, true);

      for (String dbName : getDbNames(getDataDir())) {
        getClient().beginRebalance(dbName, "persons", "_1__primarykey");

        while (!shutdown) {
          if (getClient().isRepartitioningComplete(dbName)) {
            break;
          }
          Thread.sleep(1000);
        }
      }
      cobj = new ComObject();
      cobj.put(ComObject.Tag.dbName, "test");
      cobj.put(ComObject.Tag.schemaVersion, getClient().getCommon().getSchemaVersion());
      cobj.put(ComObject.Tag.method, "forceDeletes");
      getClient().sendToAllShards(null, 0, cobj, DatabaseClient.Replica.all);

    }
    catch (Exception e) {
      finalRestoreException = e;
      logger.error("Error restoring backup", e);
      throw new DatabaseException(e);
    }
    finally {
      doingRestore = false;
      startRepartitioner();
      deltaManager.runSnapshotLoop();
    }
  }

  public void setMinSizeForRepartition(int minSizeForRepartition) {
    //repartitioner.setMinSizeForRepartition(minSizeForRepartition);
  }

  public long getCommandCount() {
    return commandCount.get();
  }

  public int getReplicationFactor() {
    return replicationFactor;
  }

  public DatabaseClient getDatabaseClient() {
    synchronized (this.client) {
      if (this.client.get() != null) {
        return this.client.get();
      }
      DatabaseClient client = new DatabaseClient(masterAddress, masterPort, common.getShard(), common.getReplica(), false, common, this);
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

  public IndexSchema getIndexSchema(String dbName, String tableName, String indexName) {
    TableSchema tableSchema = common.getTables(dbName).get(tableName);
    IndexSchema indexSchema = null;
    for (int i = 0; i < 10; i++) {
      if (tableSchema == null) {
        getClient().syncSchema();
        tableSchema = common.getTables(dbName).get(tableName);
      }
      if (tableSchema != null) {
        indexSchema = tableSchema.getIndices().get(indexName);
        if (indexSchema == null) {
          getClient().syncSchema();
          indexSchema = tableSchema.getIndices().get(indexName);
          if (indexSchema != null) {
            tableSchema.getIndexes().put(indexName, indexSchema);
            tableSchema.getIndexesById().put(indexSchema.getIndexId(), indexSchema);
            common.saveSchema(getClient(), getDataDir());
          }
        }
      }
      if (indexSchema != null) {
        break;
      }
      break;
//      try {
//        Thread.sleep(1_000);
//      }
//      catch (InterruptedException e) {
//        throw new DatabaseException(e);
//      }
    }
    return indexSchema;
  }


  @Nullable
  public Index getIndex(String dbName, String tableName, String indexName) {
    TableSchema tableSchema = common.getTables(dbName).get(tableName);
    ConcurrentHashMap<String, ConcurrentHashMap<String, Index>> indices = getIndices(dbName).getIndices();
    Index index = null;
    for (int i = 0; i < 10; i++) {
      ConcurrentHashMap<String, Index> table = indices.get(tableSchema.getName());
      if (table == null) {
        getClient().syncSchema();
        table = indices.get(tableSchema.getName());
      }
      if (table != null) {
        index = table.get(indexName);
        if (index == null) {
          getClient().syncSchema();
          index = getIndices(dbName).getIndices().get(tableSchema.getName()).get(indexName);
        }
        if (index == null) {
          schemaManager.doCreateIndex(dbName, tableSchema, indexName, tableSchema.getIndices().get(indexName).getFields());
          index = getIndices(dbName).getIndices().get(tableSchema.getName()).get(indexName);
        }
      }
      if (index != null) {
        break;
      }
      break;
//      try {
//        Thread.sleep(1_000);
//      }
//      catch (InterruptedException e) {
//        throw new DatabaseException(e);
//      }
    }
    return index;
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
    deltaManager.enableSnapshot(enable);
  }

  public void runSnapshot() throws InterruptedException, ParseException, IOException {
    for (String dbName : getDbNames(dataDir)) {
      deltaManager.runSnapshot(dbName);
    }
    getCommon().saveSchema(getClient(), getDataDir());

  }

  public void recoverFromSnapshot() throws Exception {
    common.loadSchema(dataDir);
    Set<String> dbNames = new HashSet<>();
    for (String dbName : common.getDatabases().keySet()) {
      dbNames.add(dbName);
    }
    for (String dbName : getDbNames(dataDir)) {
      dbNames.add(dbName);
    }
    for (String dbName : dbNames) {
      deltaManager.recoverFromSnapshot(dbName);
    }
  }

  public void purgeMemory() {
    for (String dbName : indexes.keySet()) {
      for (ConcurrentHashMap<String, Index> index : indexes.get(dbName).getIndices().values()) {
        for (Index innerIndex : index.values()) {
          innerIndex.clear();
        }
      }
      //common.getTables(dbName).clear();
    }
    //common.saveSchema(dataDir);
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

  public void setThrottleInsert(boolean throttle) {
    this.throttleInsert = throttle;
  }

  public boolean isThrottleInsert() {
    return throttleInsert;
  }

  public DeleteManager getDeleteManager() {
    return deleteManager;
  }

  public ReentrantReadWriteLock.ReadLock getBatchReadLock() {
    return batchReadLock;
  }

  public ReentrantReadWriteLock.WriteLock getBatchWriteLock() {
    return batchWriteLock;
  }


  private ReentrantReadWriteLock throttleLock = new ReentrantReadWriteLock();
  private Lock throttleWriteLock = throttleLock.writeLock();
  private Lock throttleReadLock = throttleLock.readLock();

  public Lock getThrottleWriteLock() {
    return throttleWriteLock;
  }

  public Lock getThrottleReadLock() {
    return throttleReadLock;
  }

  public AtomicInteger getBatchRepartCount() {
    return batchRepartCount;
  }

  public void overrideProLicense() {
    this.overrideProLicense = true;
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

  private void startMemoryMonitor() {
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
    JsonNode node = config.get("maxProcessMemoryTrigger");
    String max = node == null ? null : node.asText();
//    max = null; //disable for now
//    if (max == null) {
//      logger.info("Max process memory not set in config. Not enforcing max memory");
//    }

    Double resGig = null;
    String secondToLastLine = null;
    String lastLine = null;
    try {
      if (isMac()) {
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
      else if (isUnix()) {
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
      JsonNode node = config.get("maxProcessMemoryTrigger");
      String max = node == null ? null : node.asText();
      //    max = null; //disable for now
      //    if (max == null) {
      //      logger.info("Max process memory not set in config. Not enforcing max memory");
      //    }

      Double resGig = null;
      String secondToLastLine = null;
      String lastLine = null;
      try {
        if (isMac()) {
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
        else if (isUnix()) {
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
        else if (isWindows()) {
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


  public ComObject getOSStats(ComObject cobj) {
    try {
      OSStatsManager.OSStats stats = osStatsManager.doGetOSStats();
      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.resGig, stats.resGig);
      retObj.put(ComObject.Tag.cpu, stats.cpu);
      retObj.put(ComObject.Tag.javaMemMin, stats.javaMemMin);
      retObj.put(ComObject.Tag.javaMemMax, stats.javaMemMax);
      retObj.put(ComObject.Tag.avgRecRate, stats.avgRecRate);
      retObj.put(ComObject.Tag.avgTransRate, stats.avgTransRate);
      retObj.put(ComObject.Tag.diskAvail, stats.diskAvail);
      retObj.put(ComObject.Tag.host, host);
      retObj.put(ComObject.Tag.port, port);

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
      if (config.has("maxJavaHeapTrigger")) {
        max = config.get("maxJavaHeapTrigger").asText();
      }
      max = null;//disable for now
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

  private static String OS = System.getProperty("os.name").toLowerCase();

  public static boolean isWindows() {
    return OS.contains("win");
  }

  public static boolean isMac() {
    return OS.contains("mac");
  }

  public static boolean isUnix() {
    return OS.contains("nux");
  }

  public boolean isRunning() {
    return isRunning.get();
  }

  public boolean isRecovered() {
    return isRecovered.get();
  }

  public LongRunningCalls getLongRunningCommands() {
    return longRunningCommands;
  }

  public ComObject areAllLongRunningCommandsComplete(ComObject cobj) {
    ComObject retObj = new ComObject();
    if (longRunningCommands.getCommandCount() == 0) {
      retObj.put(ComObject.Tag.isComplete, true);
    }
    else {
      retObj.put(ComObject.Tag.isComplete, false);
    }
    return retObj;
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

  public static void validateLicense(ObjectNode config) {

    try {
      int licensedServerCount = 0;
      int actualServerCount = 0;
      boolean pro = false;
      ArrayNode keys = config.withArray("licenseKeys");
      if (keys == null || keys.size() == 0) {
        pro = false;
      }
      else {
        for (int i = 0; i < keys.size(); i++) {
          String key = keys.get(i).asText();
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

      ArrayNode shards = config.withArray("shards");
      for (int i = 0; i < shards.size(); i++) {
        ObjectNode shard = (ObjectNode) shards.get(i);
        ArrayNode replicas = shard.withArray("replicas");
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

  private static byte[] encryptF(String input, Key pkey, Cipher c) throws
      InvalidKeyException, BadPaddingException, IllegalBlockSizeException {

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
    logger.info("Syncing database names: shard=" + shard + ", replica=" + replica);
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.dbName, "__none__");
    cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
    cobj.put(ComObject.Tag.method, "getDbNames");
    byte[] ret = getDatabaseClient().send(null, 0, 0, cobj, DatabaseClient.Replica.master, true);
    ComObject retObj = new ComObject(ret);
    ComArray array = retObj.getArray(ComObject.Tag.dbNames);
    for (int i = 0; i < array.getArray().size(); i++) {
      String dbName = (String) array.getArray().get(i);
      File file = null;
      if (USE_SNAPSHOT_MGR_OLD) {
        file = new File(dataDir, "snapshot/" + shard + "/" + replica + "/" + dbName);
      }
      else {
        file = new File(dataDir, "delta/" + shard + "/" + replica + "/" + dbName);
      }
      file.mkdirs();
      logger.info("Received database name: name=" + dbName);
    }
  }


  public List<String> getDbNames(String dataDir) {
    return common.getDbNames(dataDir);
  }

  public void startRepartitioner() {
    logger.info("startRepartitioner - begin");
    //shutdownRepartitioner();

    //if (shard == 0 && replica == common.getServersConfig().getShards()[0].getMasterReplica()) {
    //repartitioner = new Repartitioner(this, common);
    if (!repartitioner.isRunning())
      repartitioner.start();

    //}
    logger.info("startRepartitioner - end");
  }

  public int getReplica() {
    return replica;
  }

  private void initServersForUnitTest(
      String host, int port, boolean unitTest, ServersConfig serversConfig) {
    if (unitTest) {
      int thisShard = serversConfig.getThisShard(host, port);
      int thisReplica = serversConfig.getThisReplica(host, port);
      Map<Integer, Object> currShard = DatabaseClient.dbservers.get(thisShard);
      if (currShard == null) {
        currShard = new ConcurrentHashMap<>();
        DatabaseClient.dbservers.put(thisShard, currShard);
      }
      currShard.put(thisReplica, this);
    }
    int thisShard = serversConfig.getThisShard(host, port);
    int thisReplica = serversConfig.getThisReplica(host, port);
    Map<Integer, Object> currShard = DatabaseClient.dbdebugServers.get(thisShard);
    if (currShard == null) {
      currShard = new ConcurrentHashMap<>();
      DatabaseClient.dbdebugServers.put(thisShard, currShard);
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
    Indices ret = indexes.get(dbName);
    if (ret == null) {
      ret = new Indices();
      indexes.put(dbName, ret);
    }
    return ret;
  }

  public Map<String, Indices> getIndices() {
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
    logManager.enableLogWriter(false);
  }

  public void shutdownRepartitioner() {
    if (repartitioner == null) {
      return;
    }
    logger.info("Shutdown repartitioner - begin");
    repartitioner.shutdown();
    repartitioner.isRebalancing.set(false);
    repartitioner.stopShardsFromRepartitioning();
    repartitioner = new Repartitioner(this, common);
    logger.info("Shutdown repartitioner - end");
  }


  public void pushSchema() {
    try {
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.dbName, "__none__");
      cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
      cobj.put(ComObject.Tag.method, "updateSchema");
      cobj.put(ComObject.Tag.schemaBytes, common.serializeSchema(DatabaseClient.SERIALIZATION_VERSION));

      for (int i = 0; i < shardCount; i++) {
        for (int j = 0; j < replicationFactor; j++) {
          if (shard == 0 && replica == common.getServersConfig().getShards()[0].getMasterReplica()) {
            if (i == shard && j == replica) {
              continue;
            }
            try {
              getDatabaseClient().send(null, i, j, cobj, DatabaseClient.Replica.specified);
            }
            catch (Exception e) {
              logger.error("Error pushing schema to server: shard=" + i + ", replica=" + j);
            }
          }
        }
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public ComObject prepareSourceForServerReload(ComObject cobj) {
    try {
      List<String> files = new ArrayList<>();

      deltaManager.enableSnapshot(false);
      logSlicePoint = logManager.sliceLogs(true);

      BufferedReader reader = new BufferedReader(new StringReader(logSlicePoint));
      while (!shutdown) {
        String line = reader.readLine();
        if (line == null) {
          break;
        }
        files.add(line);
      }
      //snapshotManager.getFilesForCurrentSnapshot(files);
      deltaManager.getFilesForCurrentSnapshot(files);

      File file = new File(getDataDir(), "logSequenceNum/" + shard + "/" + replica + "/logSequenceNum.txt");
      if (file.exists()) {
        files.add(file.getAbsolutePath());
      }
      file = new File(getDataDir(), "nextRecordId/" + shard + "/" + replica + "/nextRecorId.txt");
      if (file.exists()) {
        files.add(file.getAbsolutePath());
      }

      deleteManager.getFiles(files);
      longRunningCommands.getFiles(files);

      ComObject retObj = new ComObject();
      ComArray array = retObj.putArray(ComObject.Tag.filenames, ComObject.Type.stringType);
      for (String filename : files) {
        array.add(filename);
      }
      return retObj;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public ComObject isServerReloadFinished(ComObject cobj) {
    ComObject retObj = new ComObject();
    retObj.put(ComObject.Tag.isComplete, !isServerRoloadRunning);

    return retObj;
  }

  private boolean isServerRoloadRunning = false;

  public ComObject reloadServer(ComObject cobj) {
    reloadServerThread = ThreadUtil.createThread(new Runnable() {
      @Override
      public void run() {
        try {
          isServerRoloadRunning = true;
          isRunning.set(false);
          deltaManager.enableSnapshot(false);
          Thread.sleep(5000);
          //snapshotManager.deleteSnapshots();
          deltaManager.deleteSnapshots();

          File file = new File(getDataDir(), "result-sets");
          FileUtils.deleteDirectory(file);

          logManager.deleteLogs();

          String command = "DatabaseServer:ComObject:prepareSourceForServerReload:";
          ComObject rcobj = new ComObject();
          rcobj.put(ComObject.Tag.dbName, "__none__");
          rcobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
          byte[] bytes = getClient().send(null, shard, 0, rcobj, DatabaseClient.Replica.master);
          ComObject retObj = new ComObject(bytes);
          ComArray files = retObj.getArray(ComObject.Tag.filenames);

          downloadFilesForReload(files);

          common.loadSchema(getDataDir());
          DatabaseServer.this.getClient().syncSchema();
          prepareDataFromRestore();
          deltaManager.enableSnapshot(true);
          isRunning.set(true);
        }
        catch (Exception e) {
          throw new DatabaseException(e);
        }
        finally {
          ComObject rcobj = new ComObject();
          rcobj.put(ComObject.Tag.dbName, "__none__");
          rcobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
          rcobj.put(ComObject.Tag.method, "finishServerReloadForSource");
          byte[] bytes = getClient().send(null, shard, 0, rcobj, DatabaseClient.Replica.master);

          isServerRoloadRunning = false;
        }
      }
    }, "SonicBase Reload Server Thread");
    reloadServerThread.start();

    return null;
  }

  public ComObject getDatabaseFile(ComObject cobj) {
    try {
      String filename = cobj.getString(ComObject.Tag.filename);
      File file = new File(filename);
      BufferedInputStream in = new BufferedInputStream(new FileInputStream(file));
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      IOUtils.copy(in, out);
      out.close();
      byte[] bytes = out.toByteArray();
      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.binaryFileContent, bytes);

      return retObj;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private void downloadFilesForReload(ComArray files) {
    for (Object obj : files.getArray()) {
      String filename = (String) obj;
      try {

        ComObject cobj = new ComObject();
        cobj.put(ComObject.Tag.dbName, "__none__");
        cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
        cobj.put(ComObject.Tag.method, "getDatabaseFile");
        cobj.put(ComObject.Tag.filename, filename);
        byte[] bytes = getClient().send(null, shard, 0, cobj, DatabaseClient.Replica.master);
        ComObject retObj = new ComObject(bytes);
        byte[] content = retObj.getByteArray(ComObject.Tag.binaryFileContent);

        filename = fixReplica("deletes", filename);
        filename = fixReplica("lrc", filename);
        if (USE_SNAPSHOT_MGR_OLD) {
          filename = fixReplica("snapshot", filename);
        }
        else {
          filename = fixReplica("delta", filename);
        }
        filename = fixReplica("log", filename);
        filename = fixReplica("nextRecordId", filename);
        filename = fixReplica("logSequenceNum", filename);

        File file = new File(filename);
        file.getParentFile().mkdirs();
        file.delete();
        try (BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(file))) {
          out.write(content);
        }
      }
      catch (Exception e) {
        logger.error("Error copying file from source server for reloadServer: file=" + filename, e);
      }
    }
  }

  private String fixReplica(String dir, String filename) {
    String prefix = dir + File.separator + shard + File.separator;
    int pos = filename.indexOf(prefix);
    if (pos != -1) {
      int pos2 = filename.indexOf(File.separator, pos + prefix.length());
      filename = filename.substring(0, pos + prefix.length()) + replica + filename.substring(pos2);
    }
    return filename;
  }

  public ComObject updateServersConfig(ComObject cobj) {
    try {
      short serializationVersion = cobj.getShort(ComObject.Tag.serializationVersion);
      ServersConfig serversConfig = new ServersConfig(cobj.getByteArray(ComObject.Tag.serversConfig), serializationVersion);

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
    for (int i = 0; i < shardCount; i++) {
      for (int j = 0; j < replicationFactor; j++) {
        if (shard == 0 && replica == common.getServersConfig().getShards()[0].getMasterReplica()) {
          continue;
        }
        try {
          ComObject cobj = new ComObject();
          cobj.put(ComObject.Tag.dbName, "__none__");
          cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
          cobj.put(ComObject.Tag.method, "updateServersConfig");
          cobj.put(ComObject.Tag.serversConfig, common.getServersConfig().serialize(DatabaseClient.SERIALIZATION_VERSION));
          getDatabaseClient().send(null, i, j, cobj, DatabaseClient.Replica.specified);
        }
        catch (Exception e) {
          logger.error("Error pushing servers config: shard=" + i + ", replica=" + j);
        }
      }
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

  public ObjectNode getConfig() {
    return config;
  }

  public DatabaseClient.Replica getRole() {
    return role;
  }

  private boolean shutdown = false;


//  public static class AddressMap {
//    private ConcurrentHashMap<Long, Long>  map = new ConcurrentHashMap<>();
//    private AtomicLong currOuterAddress = new AtomicLong();
//    Object[] mutex = new Object[10000];
//
//    public AddressMap() {
//      for (int i = 0; i < mutex.length; i++) {
//        mutex[i] = new Object();
//      }
//    }
//
//    public Object getMutex(long outerAddress) {
//      return mutex[(int)(outerAddress % mutex.length)];
//    }
//
//    public long addAddress(long innerAddress) {
//      long outerAddress = currOuterAddress.incrementAndGet();
//      if (innerAddress == 0 || innerAddress == -1L) {
//        throw new DatabaseException("Adding invalid address");
//      }
//      synchronized (getMutex(outerAddress)) {
//        map.put(outerAddress, innerAddress);
//      }
//      return outerAddress;
//    }
//
//    public Long getAddress(long outerAddress) {
//      synchronized (getMutex(outerAddress)) {
//        Long ret = map.get(outerAddress);
//        return ret;
//      }
//    }
//
//    public Long removeAddress(long outerAddress) {
//      synchronized (getMutex(outerAddress)) {
//        Long ret = map.remove(outerAddress);
//        return ret;
//      }
//    }
//  }

  class IndexValue {
    long updateTime;
    byte[][] records;
    byte[] bytes;

    public IndexValue(long updateTime, byte[][] records) {
      this.updateTime = updateTime;
      this.records = records;
    }

    public IndexValue(long updateTime, byte[] bytes) {
      this.updateTime = updateTime;
      this.bytes = bytes;
    }
  }

  public Object toUnsafeFromRecords(byte[][] records) {
    long seconds = (System.currentTimeMillis() - TIME_2017) / 1000;
    return toUnsafeFromRecords(seconds, records);
  }

  public Object toUnsafeFromRecords(long updateTime, byte[][] records) {
    if (!useUnsafe) {
      try {
        if (!compressRecords) {
          return new IndexValue(updateTime, records);
        }
        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytesOut);
        AtomicInteger AtomicInteger = new AtomicInteger();
        //out.writeInt(0);
        Varint.writeUnsignedVarInt(records.length, out);
        for (byte[] record : records) {
          Varint.writeUnsignedVarInt(record.length, out);
          out.write(record);
        }
        out.close();
        byte[] bytes = bytesOut.toByteArray();
        int origLen = -1;

        if (compressRecords) {
          origLen = bytes.length;


          LZ4Compressor compressor = factory.highCompressor();//fastCompressor();
          int maxCompressedLength = compressor.maxCompressedLength(bytes.length);
          byte[] compressed = new byte[maxCompressedLength];
          int compressedLength = compressor.compress(bytes, 0, bytes.length, compressed, 0, maxCompressedLength);
          bytes = new byte[compressedLength + 8];
          System.arraycopy(compressed, 0, bytes, 8, compressedLength);
        }

        bytesOut = new ByteArrayOutputStream();
        out = new DataOutputStream(bytesOut);
        out.writeInt(bytes.length);
        out.writeInt(origLen);
        out.close();
        byte[] lenBuffer = bytesOut.toByteArray();

        System.arraycopy(lenBuffer, 0, bytes, 0, lenBuffer.length);

        return new IndexValue(updateTime, bytes);
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }
    else {
        int recordsLen = 0;
        for (byte[] record : records) {
          recordsLen += record.length;
        }

        byte[] bytes = new byte[4 + (4 * records.length) + recordsLen];
        //out.writeInt(0);
        int actualLen = 0;
        int offset = 0; //update time
        DataUtils.intToBytes(records.length, bytes, offset);
        offset += 4;
        for (byte[] record : records) {
          DataUtils.intToBytes(record.length, bytes, offset);
          offset += 4;
          System.arraycopy(record, 0, bytes, offset, record.length);
          offset += record.length;
        }
        actualLen = bytes.length;

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
          actualLen = bytes.length;
        }


        //System.arraycopy(lenBuffer, 0, bytes, 0, lenBuffer.length);

        if (bytes.length > 1000000000) {
          throw new DatabaseException("Invalid allocation: size=" + bytes.length);
        }

        long address = unsafe.allocateMemory(bytes.length + 16);

        DataUtils.intToAddress(origLen, address + 8, unsafe);
        DataUtils.intToAddress(actualLen, address + 8 + 4, unsafe);

        for (int i = 7; i >= 0; i--) {
          unsafe.putByte(address + 16 + i, (byte)(updateTime & 0xFF));
          updateTime >>= 8;
        }

        for (int i = 0; i < bytes.length; i++) {
          unsafe.putByte(address + 16 + i, bytes[i]);
        }

        if (address == 0 || address == -1L) {
          throw new DatabaseException("Inserted null address *****************");
        }

        return addressMap.addAddress(address, updateTime);
    }
  }

  LZ4Factory factory = LZ4Factory.fastestInstance();

  public static long TIME_2017 = new Date(2017 - 1900, 0, 1, 0, 0, 0).getTime();

  public Object toUnsafeFromKeys(byte[][] records) {
    long seconds = (System.currentTimeMillis() - TIME_2017) / 1000;
    return toUnsafeFromKeys(seconds, records);
  }


  public Object toUnsafeFromKeys(long updateTime, byte[][] records) {
    return toUnsafeFromRecords(updateTime, records);
  }

  public long getUpdateTime(Object value) {
    try {
      if (value instanceof Long) {
        return addressMap.getUpdateTime((Long) value);
      }
      else {
        return ((IndexValue)value).updateTime;
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }


  public static int getInt(byte[] array, int offset) {
    return
        ((array[offset]   & 0xff) << 24) |
            ((array[offset+1] & 0xff) << 16) |
            ((array[offset+2] & 0xff) << 8) |
            (array[offset+3] & 0xff);
  }

  public static void putLong(long value, byte[] array, int offset) {
    array[offset]   = (byte)(0xff & (value >> 56));
    array[offset+1] = (byte)(0xff & (value >> 48));
    array[offset+2] = (byte)(0xff & (value >> 40));
    array[offset+3] = (byte)(0xff & (value >> 32));
    array[offset+4] = (byte)(0xff & (value >> 24));
    array[offset+5] = (byte)(0xff & (value >> 16));
    array[offset+6] = (byte)(0xff & (value >> 8));
    array[offset+7] = (byte)(0xff & value);
  }

  public static long getLong(byte[] array, int offset) {
    return
        ((long)(array[offset]   & 0xff) << 56) |
            ((long)(array[offset+1] & 0xff) << 48) |
            ((long)(array[offset+2] & 0xff) << 40) |
            ((long)(array[offset+3] & 0xff) << 32) |
            ((long)(array[offset+4] & 0xff) << 24) |
            ((long)(array[offset+5] & 0xff) << 16) |
            ((long)(array[offset+6] & 0xff) << 8) |
            ((long)(array[offset+7] & 0xff));
  }


  public static long readUnsignedVarLong(DataInput in) throws IOException {
    long tmp;
    if ((tmp = (long)in.readByte()) >= 0L) {
      return tmp;
    } else {
      long result = tmp & 127L;
      if ((tmp = (long)in.readByte()) >= 0L) {
        result |= tmp << 7;
      } else {
        result |= (tmp & 127L) << 7;
        if ((tmp = (long)in.readByte()) >= 0L) {
          result |= tmp << 14;
        } else {
          result |= (tmp & 127L) << 14;
          if ((tmp = (long)in.readByte()) >= 0L) {
            result |= tmp << 21;
          } else {
            result |= (tmp & 127L) << 21;
            if ((tmp = (long)in.readByte()) >= 0L) {
              result |= tmp << 28;
            } else {
              result |= (tmp & 127L) << 28;
              if ((tmp = (long)in.readByte()) >= 0L) {
                result |= tmp << 35;
              } else {
                result |= (tmp & 127L) << 35;
                if ((tmp = (long)in.readByte()) >= 0L) {
                  result |= tmp << 42;
                } else {
                  result |= (tmp & 127L) << 42;
                  if ((tmp = (long)in.readByte()) >= 0L) {
                    result |= tmp << 49;
                  } else {
                    result |= (tmp & 127L) << 49;
                    if ((tmp = (long)in.readByte()) >= 0L) {
                      result |= tmp << 56;
                    } else {
                      result |= (tmp & 127L) << 56;
                      result |= (long)in.readByte() << 63;
                    }
                  }
                }
              }
            }
          }
        }
      }

      return result;
    }
  }

  public int readUnsignedVarInt(long address, AtomicInteger fieldLen) throws IOException {
    byte tmp;
    fieldLen.incrementAndGet();
    if ((tmp = unsafe.getByte(address + 0)) >= 0) {
      return tmp;
    } else {
      int result = tmp & 127;
      fieldLen.incrementAndGet();
      if ((tmp = unsafe.getByte(address + 1)) >= 0) {
        result |= tmp << 7;
      } else {
        result |= (tmp & 127) << 7;
        fieldLen.incrementAndGet();
        if ((tmp = unsafe.getByte(address + 2)) >= 0) {
          result |= tmp << 14;
        } else {
          result |= (tmp & 127) << 14;
          fieldLen.incrementAndGet();
          if ((tmp = unsafe.getByte(address + 3)) >= 0) {
            result |= tmp << 21;
          } else {
            result |= (tmp & 127) << 21;
            fieldLen.incrementAndGet();
            result |= unsafe.getByte(address + 4) << 28;
          }
        }
      }
      return result;
    }
  }

  public byte[][] fromUnsafeToRecords(Object obj) {
    try {
      if (obj instanceof Long) {

//        try read-write lock
//            try lock for outer and lock for map
        ReentrantReadWriteLock.ReadLock readLock = addressMap.getReadLock((Long)obj);
        readLock.lock();
          try {
            Long innerAddress = addressMap.getAddress((Long) obj);
            if (innerAddress == null) {
//              System.out.println("null address ******************* outerAddress=" + (long) obj);
//              new Exception().printStackTrace();
              return null;
            }

            //          long updateTime = 0;
            //          for (int i = 0; i < 8; i++) {
            //            updateTime <<= 8;
            //            updateTime |= (unsafe.getByte(innerAddress + i) & 0xFF);
            //          }

            //          int count = ((unsafe.getByte(innerAddress + 8 + 0)   & 0xff) << 24) |
            //              ((unsafe.getByte(innerAddress + 8 + 1) & 0xff) << 16) |
            //              ((unsafe.getByte(innerAddress + 8 + 2) & 0xff) << 8) |
            //              (unsafe.getByte(innerAddress + 8 + 3) & 0xff);

            int origLen = ((unsafe.getByte(innerAddress + 8 + 0) & 0xff) << 24) |
                ((unsafe.getByte(innerAddress + 8 + 1) & 0xff) << 16) |
                ((unsafe.getByte(innerAddress + 8 + 2) & 0xff) << 8) |
                (unsafe.getByte(innerAddress + 8 + 3) & 0xff);

            int actualLen = ((unsafe.getByte(innerAddress + 12 + 0) & 0xff) << 24) |
                ((unsafe.getByte(innerAddress + 12 + 1) & 0xff) << 16) |
                ((unsafe.getByte(innerAddress + 12 + 2) & 0xff) << 8) |
                (unsafe.getByte(innerAddress + 12 + 3) & 0xff);
            //          byte[] bytes = new byte[count];
            //          for (int i = 0; i < count; i++) {
            //            bytes[i] = unsafe.getByte(innerAddress + i + 8 + 8);
            //          }
            if (origLen == -1) {
              int offset = 0;
              int recCount = DataUtils.addressToInt(innerAddress + offset + 16, unsafe);
              offset += 4;
              byte[][] ret = new byte[recCount][];
              for (int i = 0; i < ret.length; i++) {
                int len = DataUtils.addressToInt(innerAddress + offset + 16, unsafe);
                offset += 4;
                byte[] record = new byte[len];
                for (int j = 0; j < len; j++) {
                  record[j] = unsafe.getByte(innerAddress + offset + 16);
                  offset++;
                }
                ret[i] = record;
              }
              return ret;
            }

            if (origLen != -1) {
              LZ4Factory factory = LZ4Factory.fastestInstance();

              byte[] bytes = new byte[actualLen];
              for (int j = 0; j < actualLen; j++) {
                bytes[j] = unsafe.getByte(innerAddress + 16 + j);
              }

              LZ4FastDecompressor decompressor = factory.fastDecompressor();
              byte[] restored = new byte[origLen];
              decompressor.decompress(bytes, 0, restored, 0, origLen);
              bytes = restored;

              DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
              //in.readInt(); //byte count
              //in.readInt(); //orig len
              byte[][] ret = new byte[(int) in.readInt()][];
              for (int i = 0; i < ret.length; i++) {
                int len = (int) in.readInt();
                byte[] record = new byte[len];
                in.readFully(record);
                ret[i] = record;
              }
              return ret;
            }
        }
        finally {
          readLock.unlock();
        }
      }
      else {
        if (!compressRecords) {
          return ((IndexValue)obj).records;
        }
        byte[] bytes = ((IndexValue)obj).bytes;
        byte[] lenBuffer = new byte[8];
        System.arraycopy(bytes, 0, lenBuffer, 0, lenBuffer.length);
        ByteArrayInputStream bytesIn = new ByteArrayInputStream(lenBuffer);
        DataInputStream in = new DataInputStream(bytesIn);
        int count = in.readInt();
        int origLen = in.readInt();

        if (origLen != -1) {
          LZ4FastDecompressor decompressor = factory.fastDecompressor();
          byte[] restored = new byte[origLen];
          decompressor.decompress(bytes, lenBuffer.length, restored, 0, origLen);
          bytes = restored;
        }

        in = new DataInputStream(new ByteArrayInputStream(bytes));
        //in.readInt(); //byte count
        //in.readInt(); //orig len
        byte[][] ret = new byte[(int) Varint.readSignedVarLong(in)][];
        for (int i = 0; i < ret.length; i++) {
          int len = (int) Varint.readSignedVarLong(in);
          byte[] record = new byte[len];
          in.readFully(record);
          ret[i] = record;
        }
        return ret;
      }
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
    return null;
  }

  public byte[][] fromUnsafeToKeys(Object obj) {
    return fromUnsafeToRecords(obj);
//    try {
//      if (obj instanceof byte[]) {
//        DataInputStream in = new DataInputStream(new ByteArrayInputStream((byte[])obj));
//        long seconds = Varint.readUnsignedVarLong(in);
//        long outerAddress = Varint.readUnsignedVarLong(in);
//
//        synchronized (addressMap.getMutex(outerAddress)) {
//          Long address = addressMap.getAddress(outerAddress);
//          if (address == null) {
//            return null;
//          }
//
//          byte[] lenBuffer = new byte[8];
//          lenBuffer[0] = unsafe.getByte(address + 0);
//          lenBuffer[1] = unsafe.getByte(address + 1);
//          lenBuffer[2] = unsafe.getByte(address + 2);
//          lenBuffer[3] = unsafe.getByte(address + 3);
//          lenBuffer[4] = unsafe.getByte(address + 4);
//          lenBuffer[5] = unsafe.getByte(address + 5);
//          lenBuffer[6] = unsafe.getByte(address + 6);
//          lenBuffer[7] = unsafe.getByte(address + 7);
//          ByteArrayInputStream bytesIn = new ByteArrayInputStream(lenBuffer);
//          in = new DataInputStream(bytesIn);
//          int count = in.readInt();
//          int origLen = in.readInt();
//          byte[] bytes = new byte[count];
//          for (int i = 0; i < count; i++) {
//            bytes[i] = unsafe.getByte(address + i + 8);
//          }
//
//          if (origLen != -1) {
//            LZ4Factory factory = LZ4Factory.fastestInstance();
//
//            LZ4FastDecompressor decompressor = factory.fastDecompressor();
//            byte[] restored = new byte[origLen];
//            decompressor.decompress(bytes, 0, restored, 0, origLen);
//            bytes = restored;
//          }
//
//          in = new DataInputStream(new ByteArrayInputStream(bytes));
//          //in.readInt(); //byte count
//          //in.readInt(); //orig len
//          byte[][] ret = new byte[(int) Varint.readSignedVarLong(in)][];
//          for (int i = 0; i < ret.length; i++) {
//            int len = (int) Varint.readSignedVarLong(in);
//            byte[] record = new byte[len];
//            in.readFully(record);
//            ret[i] = record;
//          }
//          return ret;
//        }
//      }
//      else {
//        if (!compressRecords) {
//          return ((IndexValue)obj).records;
//        }
//        byte[] bytes = ((IndexValue)obj).bytes;
//        byte[] lenBuffer = new byte[8];
//        System.arraycopy(bytes, 0, lenBuffer, 0, lenBuffer.length);
//        ByteArrayInputStream bytesIn = new ByteArrayInputStream(lenBuffer);
//        DataInputStream in = new DataInputStream(bytesIn);
//        int count = in.readInt();
//        int origLen = in.readInt();
//
//        if (origLen != -1) {
//          LZ4Factory factory = LZ4Factory.fastestInstance();
//
//          LZ4FastDecompressor decompressor = factory.fastDecompressor();
//          byte[] restored = new byte[origLen];
//          decompressor.decompress(bytes, lenBuffer.length, restored, 0, origLen);
//          bytes = restored;
//        }
//
//        in = new DataInputStream(new ByteArrayInputStream(bytes));
//        //in.readInt(); //byte count
//        //in.readInt(); //orig len
//        byte[][] ret = new byte[(int) Varint.readSignedVarLong(in)][];
//        for (int i = 0; i < ret.length; i++) {
//          int len = (int) Varint.readSignedVarLong(in);
//          byte[] record = new byte[len];
//          in.readFully(record);
//          ret[i] = record;
//        }
//        return ret;
//      }
//    }
//    catch (IOException e) {
//      throw new DatabaseException(e);
//    }
  }

  public void freeUnsafeIds(Object obj) {
    try {
      if (obj instanceof Long) {
        addressMap.removeAddress((Long)obj, unsafe);
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public static AddressMap getAddressMap() {
    return addressMap;
  }


//todo: snapshot queue periodically

//todo: implement restoreSnapshot()

  public static class LogRequest {
    private byte[] buffer;
    private CountDownLatch latch = new CountDownLatch(1);
    private List<byte[]> buffers;
    private long[] sequenceNumbers;
    private long[] times;
    private long begin;
    private AtomicLong timeLogging;

    public LogRequest(int size) {
      this.sequenceNumbers = new long[size];
      this.times = new long[size];
    }

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

    public List<byte[]> getBuffers() {
      return buffers;
    }

    public long[] getSequences1() {
      return sequenceNumbers;
    }

    public long[] getSequences0() {
      return times;
    }

    public void setBegin(long begin) {
      this.begin = begin;
    }

    public void setTimeLogging(AtomicLong timeLogging) {
      this.timeLogging = timeLogging;
    }

    public AtomicLong getTimeLogging() {
      return timeLogging;
    }

    public long getBegin() {
      return begin;
    }
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

  public byte[] invokeMethod(final byte[] body, boolean replayedCommand, boolean enableQueuing) {
    return invokeMethod(body, -1L, (short) -1L, replayedCommand, enableQueuing, null, null);
  }

  public byte[] invokeMethod(final byte[] body, long logSequence0, long logSequence1,
                             boolean replayedCommand, boolean enableQueuing, AtomicLong timeLogging, AtomicLong handlerTime) {
    if (methodInvoker == null) {
      throw new DeadServerException("Server not running");
    }
    return methodInvoker.invokeMethod(body, logSequence0, logSequence1, replayedCommand, enableQueuing, timeLogging, handlerTime);
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

  public ComObject prepareToComeAlive(ComObject cobj) {
    String slicePoint = null;
    try {
      ComObject pcobj = new ComObject();
      pcobj.put(ComObject.Tag.dbName, "__none__");
      pcobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
      pcobj.put(ComObject.Tag.method, "pushMaxSequenceNum");
      getClient().send(null, shard, 0, pcobj, DatabaseClient.Replica.master,
          true);

      if (shard == 0) {
        pcobj = new ComObject();
        pcobj.put(ComObject.Tag.dbName, "__none__");
        pcobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
        pcobj.put(ComObject.Tag.method, "pushMaxRecordId");
        getClient().send(null, shard, 0, pcobj, DatabaseClient.Replica.master,
            true);
      }

      for (int replica = 0; replica < replicationFactor; replica++) {
        if (replica != this.replica) {
          logManager.getLogsFromPeer(replica);
        }
      }
      onlyQueueCommands = true;
      slicePoint = logManager.sliceLogs(false);
      logManager.applyLogsFromPeers(slicePoint);
    }
    finally {
      onlyQueueCommands = false;
    }
    try {
      applyingQueuesAndInteractive = true;
      logManager.applyLogsAfterSlice(slicePoint);
    }
    finally {
      applyingQueuesAndInteractive = false;
    }

    return null;
  }

  public ComObject reconfigureCluster(ComObject cobj) {
    ServersConfig oldConfig = common.getServersConfig();
    try {
      File file = new File(System.getProperty("user.dir"), "config/config-" + getCluster() + ".json");
      if (!file.exists()) {
        file = new File(System.getProperty("user.dir"), "db/src/main/resources/config/config-" + getCluster() + ".json");
      }
      String configStr = IOUtils.toString(new BufferedInputStream(new FileInputStream(file)), "utf-8");
      logger.info("Config: " + configStr);
      ObjectMapper mapper = new ObjectMapper();
      ObjectNode config = (ObjectNode) mapper.readTree(configStr);

      //validateLicense(config);

      boolean isInternal = false;
      if (config.has("clientIsPrivate")) {
        isInternal = config.get("clientIsPrivate").asBoolean();
      }

      boolean optimizedForThroughput = true;
      if (config.has("optimizeReadsFor")) {
        String text = config.get("optimizeReadsFor").asText();
        if (!text.equalsIgnoreCase("totalThroughput")) {
          optimizedForThroughput = false;
        }
      }

      ServersConfig newConfig = new ServersConfig(cluster, config.withArray("shards"),
          config.withArray("shards").get(0).withArray("replicas").size(), isInternal, optimizedForThroughput);

      common.setServersConfig(newConfig);

      common.saveSchema(getClient(), getDataDir());

      pushSchema();

      ServersConfig.Shard[] oldShards = oldConfig.getShards();
      ServersConfig.Shard[] newShards = newConfig.getShards();

      int count = newShards.length - oldShards.length;
      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.count, count);

      return retObj;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }


  public ComObject reserveNextIdFromReplica(ComObject cobj) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      int schemaVersion = cobj.getInt(ComObject.Tag.schemaVersion);
      if (schemaVersion < getSchemaVersion()) {
        throw new SchemaOutOfSyncException("currVer:" + common.getSchemaVersion() + ":");
      }
      long highestId = cobj.getLong(ComObject.Tag.highestId);
      long id = -1;
      synchronized (nextRecordId) {
        if (highestId > nextRecordId.get()) {
          nextRecordId.set(highestId);
        }
        id = nextRecordId.getAndIncrement();
      }
      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.highestId, id);
      return retObj;
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  private final Object nextIdLock = new Object();

  public ComObject allocateRecordIds(ComObject cobj) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      logger.info("Requesting next record id - begin");
//      int schemaVersion = cobj.getInt(ComObject.Tag.schemaVersion);
//      if (schemaVersion < getSchemaVersion()) {
//        throw new SchemaOutOfSyncException("currVer:" + common.getSchemaVersion() + ":");
//      }
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

      pushMaxRecordId(dbName, maxId);

      logger.info("Requesting next record id - finished: nextId=" + nextId + ", maxId=" + maxId);

      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.nextId, nextId);
      retObj.put(ComObject.Tag.maxId, maxId);
      return retObj;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public ComObject pushMaxRecordId(ComObject cobj) {
    try {
      synchronized (nextIdLock) {
        File file = new File(dataDir, "nextRecordId/" + getShard() + "/" + getReplica() + "/nextRecorId.txt");
        file.getParentFile().mkdirs();
        if (file.exists()) {
          try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)))) {
            long maxId = Long.valueOf(reader.readLine());
            pushMaxRecordId("__none__", maxId);
          }
        }
      }
      return null;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private void pushMaxRecordId(String dbName, long maxId) {
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.dbName, dbName);
    cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
    cobj.put(ComObject.Tag.method, "setMaxRecordId");
    cobj.put(ComObject.Tag.maxId, maxId);

    for (int replica = 0; replica < replicationFactor; replica++) {
      if (replica == this.replica) {
        continue;
      }
      try {
        getDatabaseClient().send(null, 0, replica, cobj, DatabaseClient.Replica.specified, true);
      }
      catch (Exception e) {
        logger.error("Error pushing maxRecordId: replica=" + replica, e);
      }
    }
  }

  public ComObject setMaxRecordId(ComObject cobj) {
    if (shard == 0 && replica == common.getServersConfig().getShards()[0].getMasterReplica()) {
      return null;
    }
//    String dbName = cobj.getString(ComObject.Tag.dbName);
    Long maxId = cobj.getLong(ComObject.Tag.maxId);
//    common.getSchemaReadLock(dbName).lock();
    try {
      logger.info("setMaxRecordId - begin");
      synchronized (nextIdLock) {
        File file = new File(dataDir, "nextRecordId/" + getShard() + "/" + getReplica() + "/nextRecorId.txt");
        file.getParentFile().mkdirs();
        file.delete();

        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))) {
          writer.write(String.valueOf(maxId));
        }
      }

      logger.info("setMaxRecordId - end");

      return null;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
//    finally {
//      common.getSchemaReadLock(dbName).unlock();
//    }
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

  public Record evaluateRecordForQuery(
      TableSchema tableSchema, Record record,
      ExpressionImpl whereClause, ParameterHandler parms) {

    boolean pass = (Boolean) whereClause.evaluateSingleRecord(new TableSchema[]{tableSchema}, new Record[]{record}, parms);
    if (pass) {
      return record;
    }
    return null;
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

  private class StatsMonitor implements Runnable {
    @Override
    public void run() {
      while (!shutdown && !Thread.interrupted()) {
        try {
          for (int i = 0; i < 100; i++) {
            Thread.sleep(300);
          }
          OSStatsManager.OSStats stats = osStatsManager.doGetOSStats();
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
