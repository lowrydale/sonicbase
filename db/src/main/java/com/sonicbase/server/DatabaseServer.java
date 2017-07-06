package com.sonicbase.server;

import com.google.api.client.http.HttpResponse;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.*;
import com.sonicbase.index.Index;
import com.sonicbase.index.Indices;
import com.sonicbase.index.Repartitioner;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.query.impl.ExpressionImpl;
import com.sonicbase.research.socket.NettyServer;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.socket.DeadServerException;
import com.sonicbase.util.*;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.input.ReversedLinesFileReader;
import org.apache.commons.lang.exception.ExceptionUtils;
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
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;
import static org.yaml.snakeyaml.tokens.Token.ID.Tag;


/**
 * User: lowryda
 * Date: 12/27/13
 * Time: 4:39 PM
 */
public class DatabaseServer {

  public static Object deathOverrideMutex = new Object();
  public static boolean[][] deathOverride;
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
  private AtomicInteger testWriteCallCount = new AtomicInteger();
  private boolean doingRestore;
  private JsonDict backupConfig;
  private static Object restoreAwsMutex = new Object();

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
  private Map<String, Indices> indexes = new ConcurrentHashMap<>();
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

  public void setConfig(
      final JsonDict config, String cluster, String host, int port, AtomicBoolean isRunning, String gclog, String xmx,
      boolean overrideProLicense) {
    setConfig(config, cluster, host, port, false, isRunning, false, gclog, xmx, overrideProLicense);
  }

  public void setConfig(
      final JsonDict config, String cluster, String host, int port,
      boolean unitTest, AtomicBoolean isRunning, String gclog, boolean overrideProLicense) {
    setConfig(config, cluster, host, port, unitTest, isRunning, false, gclog, null, overrideProLicense);
  }

  public void setConfig(
      final JsonDict config, String cluster, String host, int port,
      boolean unitTest, AtomicBoolean isRunning, boolean skipLicense, String gclog, String xmx, boolean overrideProLicense) {


    this.isRunning = isRunning;
    this.config = config;
    this.cluster = cluster;
    this.host = host;
    this.port = port;
    this.gclog = gclog;
    this.xmx = xmx;
    this.overrideProLicense = overrideProLicense;

    JsonDict databaseDict = config;
    this.dataDir = databaseDict.getString("dataDirectory");
    this.dataDir = dataDir.replace("$HOME", System.getProperty("user.home"));
    this.installDir = databaseDict.getString("installDirectory");
    this.installDir = installDir.replace("$HOME", System.getProperty("user.home"));
    JsonArray shards = databaseDict.getArray("shards");
    int replicaCount = shards.getDict(0).getArray("replicas").size();
    if (replicaCount > 1) {
      usingMultipleReplicas = true;
    }
    JsonDict firstServer = shards.getDict(0).getArray("replicas").getDict(0);
    ServersConfig serversConfig = null;
    executor = new ThreadPoolExecutor(256, 256, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
//    if (!skipLicense) {
//      validateLicense(config);
//    }

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
    serversConfig = new ServersConfig(cluster, shards, replicationFactor, isInternal);
    this.replica = serversConfig.getThisReplica(host, port);

    common.setShard(serversConfig.getThisShard(host, port));
    common.setReplica(this.replica);
    common.setServersConfig(serversConfig);
    this.shard = common.getShard();
    this.shardCount = serversConfig.getShardCount();

    common.setServersConfig(serversConfig);

    logger = new Logger(getDatabaseClient());
    logger.info("config=" + config.toString());

    this.awsClient = new AWSClient(client.get());

    this.deleteManager = new DeleteManager(this);
    this.deleteManager.start();
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

    scheduleBackup();

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

    disable();
    startLicenseValidator();

    startMasterMonitor();

    logger.info("Started server");

    //logger.error("Testing errors", new DatabaseException());

//    File file = new File(dataDir, "queue/" + shard + "/" + replica);
//    queue = new Queue(file.getAbsolutePath(), "request-log", 0);
//    Thread logThread = new Thread(queue);
//    logThread.start();

  }

  public void setBackupConfig(JsonDict backup) {
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
    return testWriteCallCount.get();
  }

  final int[] monitorShards = {0, 0, 0};
  final int[] monitorReplicas = {0, 1, 2};

  private void startMasterMonitor() {
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        JsonArray shards = config.getArray("shards");
        JsonArray replicas = shards.getDict(0).getArray("replicas");
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
            Thread masterThread = new Thread(new Runnable() {
              @Override
              public void run() {
                try {
                  electNewMaster(shard, -1, monitorShards, monitorReplicas);
                }
                catch (Exception e) {
                  throw new DatabaseException(e);
                }
                while (true) {
                  try {
                    Thread.sleep(deathOverride == null ? 5000 : 50);

                    final int masterReplica = common.getServersConfig().getShards()[shard].getMasterReplica();
                    final AtomicBoolean isHealthy = new AtomicBoolean(false);
                    checkHealthOfServer(shard, masterReplica, isHealthy);

                    if (!isHealthy.get()) {
                      electNewMaster(shard, masterReplica, monitorShards, monitorReplicas);
                    }
                  }
                  catch (Exception e) {
                    logger.error("Error in master monitor: shard=" + shard, e);
                  }
                }
              }
            });
            masterThread.start();
          }
        }
      }
    });
    thread.start();
  }

  private void electNewMaster(int shard, int oldMasterReplica, int[] monitorShards, int[] monitorReplicas) throws InterruptedException, IOException {
    int electedMaster = -1;
    boolean isFirst = false;
    int nextMonitor = -1;
    logger.info("electNewMaster - begin: shard=" + shard + ", oldMasterReplica=" + oldMasterReplica);
    for (int i = 0; i < monitorShards.length; i++) {
      if (common.getServersConfig().getShards()[monitorShards[i]].getReplicas()[monitorReplicas[i]].dead) {
        continue;
      }
      if (monitorShards[i] == this.shard && monitorReplicas[i] == this.replica) {
        isFirst = true;
        continue;
      }
      if (!common.getServersConfig().getShards()[monitorShards[i]].getReplicas()[monitorReplicas[i]].dead) {
        nextMonitor = i;
      }
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
      return;
    }
    if (nextMonitor != -1) {
      outer:
      while (true) {
        for (int j = 0; j < replicationFactor; j++) {
          if (j == oldMasterReplica) {
            continue;
          }
          Thread.sleep(deathOverride == null ? 2000 : 50);

          AtomicBoolean isHealthy = new AtomicBoolean();
          checkHealthOfServer(shard, j, isHealthy);
          if (isHealthy.get()) {
            try {
              ComObject cobj = new ComObject();
              cobj.put(ComObject.Tag.dbName, "__non__");
              cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
              cobj.put(ComObject.Tag.method, "electNewMaster");
              cobj.put(ComObject.Tag.requestedMasterShard, shard);
              cobj.put(ComObject.Tag.requestedMasterReplica, j);
              final String command = "DatabaseServer:ComObject:electNewMaster:";
              byte[] bytes = getDatabaseClient().send(null, monitorShards[nextMonitor], monitorReplicas[nextMonitor],
                  command, cobj.serialize(), DatabaseClient.Replica.specified);
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
            String command = "DatabaseServer:ComObject:promoteToMasterAndPushSchema:";
            common.getServersConfig().getShards()[shard].setMasterReplica(electedMaster);

            getDatabaseClient().sendToMaster(command, cobj.serialize());


            cobj = new ComObject();
            cobj.put(ComObject.Tag.dbName, "__none__");
            cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
            cobj.put(ComObject.Tag.method, "promoteToMaster");
            cobj.put(ComObject.Tag.shard, shard);
            cobj.put(ComObject.Tag.electedMaster, electedMaster);
            command = "DatabaseServer:ComObject:promoteToMaster:1:";

            getDatabaseClient().send(null, shard, electedMaster, command, cobj.serialize(), DatabaseClient.Replica.specified);
            break;
          }
        }
        catch (Exception e) {
          logger.error("Error promoting master: shard=" + shard + ", electedMaster=" + electedMaster, e);
          continue;
        }
      }
    }
  }

  public byte[] electNewMaster(ComObject cobj, boolean replayedCommand) throws InterruptedException, IOException {
    int requestedMasterShard = cobj.getInt(ComObject.Tag.requestedMasterShard);
    int requestedMasterReplica = cobj.getInt(ComObject.Tag.requestedMasterReplica);

    final AtomicBoolean isHealthy = new AtomicBoolean(false);
    checkHealthOfServer(requestedMasterShard, requestedMasterReplica, isHealthy);
    if (!isHealthy.get()) {
      logger.info("candidate master is unhealthy, rejecting: shard=" + requestedMasterShard + ", replica=" + requestedMasterReplica);
      requestedMasterReplica = -1;
    }
    else {
      logger.info("candidate master is health, accepting: candidateMaster=" + requestedMasterReplica);
    }
    ComObject retObj = new ComObject();
    retObj.put(ComObject.Tag.selectedMasteReplica, requestedMasterReplica);
    return retObj.serialize();
  }

  public byte[] promoteToMaster(ComObject cobj, boolean replayedCommand) {
    try {
      logger.info("Promoting to master: shard=" + shard + ", replica=" + replica);
      logManager.skipToMaxSequenceNumber();

      if (shard == 0) {
        shutdownDeathMonitor();

        startDeathMonitor();
      }
      return null;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private void shutdownDeathMonitor() {
    synchronized (deathMonitorMutex) {
      shutdownDeathMonitor = true;
      try {
        if (deathMonitorThreads != null) {
          for (int i = 0; i < shardCount; i++) {
            for (int j = 0; j < replicationFactor; j++) {
              deathMonitorThreads[i][j].interrupt();
            }
          }
        }
      }
      catch (Exception e) {
        logger.error("Error shutting down death monitor", e);
      }
      deathMonitorThreads = null;
    }
  }

  private Thread[][] deathMonitorThreads = null;
  boolean shutdownDeathMonitor = false;
  private Object deathMonitorMutex = new Object();

  private void startDeathMonitor() {
    synchronized (deathMonitorMutex) {
      shutdownDeathMonitor = false;
      logger.info("Starting death monitor");
      deathMonitorThreads = new Thread[shardCount][];
      for (int i = 0; i < shardCount; i++) {
        final int shard = i;
        deathMonitorThreads[i] = new Thread[replicationFactor];
        for (int j = 0; j < replicationFactor; j++) {
          final int replica = j;
          deathMonitorThreads[i][j] = new Thread(new Runnable() {
            @Override
            public void run() {
              while (!shutdownDeathMonitor) {
                try {
                  Thread.sleep(deathOverride == null ? 2000 : 50);
                  AtomicBoolean isHealthy = new AtomicBoolean();
                  checkHealthOfServer(shard, replica, isHealthy);
                  boolean wasDead = common.getServersConfig().getShards()[shard].getReplicas()[replica].dead;
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
                    String command = "DatabaseServer:ComObject:prepareToComeAlive:";

                    getDatabaseClient().send(null, shard, replica, command, cobj.serialize(), DatabaseClient.Replica.specified, true);
                  }
                  if (wasDead && isHealthy.get()) {
                    common.getServersConfig().getShards()[shard].getReplicas()[replica].dead = false;
                    changed = true;
                  }
                  else if (!wasDead && !isHealthy.get()) {
                    common.getServersConfig().getShards()[shard].getReplicas()[replica].dead = true;
                    changed = true;
                  }
                  getSchemaFromPossibleMaster();
                  if (DatabaseServer.this.shard == 0 &&
                      common.getServersConfig().getShards()[0].getMasterReplica() != DatabaseServer.this.replica) {
                    shutdownDeathMonitor();
                  }
                  if (changed) {
                    logger.info("server health changed: shard=" + shard + ", replica=" + replica + ", isHealthy=" + isHealthy.get());
                    common.saveSchema(getDataDir());
                    pushSchema();
                  }
                }
                catch (InterruptedException e) {
                  break;
                }
                catch (Exception e) {
                  logger.error("Error in death monitor thread: shard=" + shard + ", replica=" + replica, e);
                }
              }
            }
          });
          deathMonitorThreads[i][j].start();
        }
      }
    }
  }

  public byte[] promoteToMasterAndPushSchema(ComObject cobj, boolean replayedCommand) {
    int shard = cobj.getInt(ComObject.Tag.shard);
    int replica = cobj.getInt(ComObject.Tag.replica);

    logger.info("promoting to master: shard=" + shard + ", replica=" + replica);
    common.getServersConfig().getShards()[shard].setMasterReplica(replica);
    common.saveSchema(getDataDir());
    pushSchema();
    return null;
  }

  private void checkHealthOfServer(final int shard, final int replica, final AtomicBoolean isHealthy) throws InterruptedException {
    if (deathOverride != null) {
      isHealthy.set(!deathOverride[shard][replica]);
      return;
    }

    final AtomicBoolean finished = new AtomicBoolean();
    isHealthy.set(false);
    Thread checkThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          ComObject cobj = new ComObject();
          cobj.put(ComObject.Tag.dbName, "__none__");
          cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
          cobj.put(ComObject.Tag.method, "healthCheck");
          String command = "DatabaseServer:ComObject:healthCheck:";
         byte[] bytes = getDatabaseClient().send(null, shard, replica, command, cobj.serialize(), DatabaseClient.Replica.specified, true);
         ComObject retObj = new ComObject(bytes);
         if (retObj.getString(ComObject.Tag.status).equals("{\"status\" : \"ok\"}")) {
            isHealthy.set(true);
          }
          finished.set(true);
        }
        catch (Exception e) {
          logger.error("Error checking health of server: shard=" + shard + ", replica=" + replica);
        }
      }
    });
    checkThread.start();

    int i = 0;
    while (!finished.get()) {
      Thread.sleep(deathOverride == null ? 100 : 20);
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
      String command = "DatabaseServer:ComObject:getSchema:";
      try {

        byte[] ret = getClient().send(null, monitorShards[i], monitorReplicas[i], command, cobj.serialize(), DatabaseClient.Replica.specified);
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

  private static class NullX509TrustManager implements X509TrustManager {
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
      System.out.println();
    }

    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
      System.out.println();
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

  private void startLicenseValidator() {
    if (overrideProLicense) {
      logger.info("Overriding pro license");
      haveProLicense = true;
      common.setHaveProLicense(haveProLicense);
      common.saveSchema(dataDir);
      return;
    }
    haveProLicense = false;

//    if (usingMultipleReplicas) {
//      throw new InsufficientLicense("You must have a pro license to use multiple replicas");
//    }

    final AtomicInteger licensePort = new AtomicInteger();
    String json = null;
    try {
      json = StreamUtils.inputStreamToString(DatabaseServer.class.getResourceAsStream("/config-license-server.json"));
    }
    catch (Exception e) {
      logger.error("Error initializing license validator", e);
      return;
    }
    JsonDict config = new JsonDict(json);
    licensePort.set(config.getDict("server").getInt("port"));
    final String address = config.getDict("server").getString("privateAddress");

    final AtomicBoolean lastHaveProLicense = new AtomicBoolean(haveProLicense);

    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        while (true) {
          try {
            int cores = Runtime.getRuntime().availableProcessors();
            HttpResponse response = DatabaseClient.restGet("https://" + address + ":" + licensePort.get() + "/license/checkIn?" +
                "address=" + DatabaseServer.this.host + "&port=" + DatabaseServer.this.port + "&cores=" + cores);
            String responseStr = StreamUtils.inputStreamToString(response.getContent());
            logger.info("CheckIn response: " + responseStr);

            JsonDict dict = new JsonDict(responseStr);

            DatabaseServer.this.haveProLicense = dict.getBoolean("inCompliance");
            DatabaseServer.this.disableNow = dict.getBoolean("disableNow");

            logger.info("lastHaveProLicense=" + lastHaveProLicense.get() + ", haveProLicense=" + haveProLicense);
            if (lastHaveProLicense.get() != haveProLicense) {
              common.setHaveProLicense(haveProLicense);
              common.saveSchema(dataDir);
              lastHaveProLicense.set(haveProLicense);
              logger.info("Saving schema with haveProLicense=" + haveProLicense);
            }
          }
          catch (Exception e) {
            logger.error("license server not found");
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
    });
    thread.start();
  }

  public byte[] prepareForBackup(ComObject cobj, boolean replayedCommand) {

    snapshotManager.enableSnapshot(false);

    logSlicePoint = logManager.sliceLogs(true);

    isBackupComplete = false;

    backupException = null;

    return null;
  }

  public byte[] doBackupFileSystem(final ComObject cobj, boolean replayedCommand) {
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          String directory = cobj.getString(ComObject.Tag.directory);
          String subDirectory = cobj.getString(ComObject.Tag.subDirectory);

          directory = directory.replace("$HOME", System.getProperty("user.home"));

          backupFileSystemSingleDir(directory, subDirectory, "logSequenceNum");
          backupFileSystemSingleDir(directory, subDirectory, "nextRecordId");
          deleteManager.backupFileSystem(directory, subDirectory);
          longRunningCommands.backupFileSystem(directory, subDirectory);
          snapshotManager.backupFileSystem(directory, subDirectory);
          logManager.backupFileSystem(directory, subDirectory, logSlicePoint);

          isBackupComplete = true;
        }
        catch (Exception e) {
          logger.error("Error backing up database", e);
          backupException = e;
        }
      }
    });
    thread.start();
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

  public byte[] doBackupAWS(final ComObject cobj, boolean replayedCommand) {
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          String subDirectory = cobj.getString(ComObject.Tag.subDirectory);
          String bucket = cobj.getString(ComObject.Tag.bucket);
          String prefix = cobj.getString(ComObject.Tag.prefix);

          backupAWSSingleDir(bucket, prefix, subDirectory, "logSequenceNum");
          backupAWSSingleDir(bucket, prefix, subDirectory, "nextRecordId");
          deleteManager.backupAWS(bucket, prefix, subDirectory);
          longRunningCommands.backupAWS(bucket, prefix, subDirectory);
          snapshotManager.backupAWS(bucket, prefix, subDirectory);
          logManager.backupAWS(bucket, prefix, subDirectory, logSlicePoint);

          isBackupComplete = true;
        }
        catch (Exception e) {
          logger.error("Error backing up database", e);
          backupException = e;
        }
      }
    });
    thread.start();
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

  public byte[] isBackupComplete(ComObject cobj, boolean replayedCommand) {
    try {
      if (backupException != null) {
        throw new DatabaseException(backupException);
      }
      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.isComplete, isBackupComplete);
      return retObj.serialize();
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public byte[] finishBackup(ComObject cobj, boolean replayedCommand) {
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
      snapshotManager.enableSnapshot(true);
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

  public byte[] isEntireBackupComplete(ComObject cobj, boolean replayedCommand) {
    try {
      if (finalBackupException != null) {
        throw new DatabaseException("Error performing backup", finalBackupException);
      }
      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.isComplete, !doingBackup);

      return retObj.serialize();
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public byte[] startBackup(ComObject cobj, boolean replayedCommand) {

    final boolean wasDoingBackup = doingBackup;
    doingBackup = true;
    Thread thread = new Thread(new Runnable(){
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
    });
    thread.start();
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
      JsonDict backup = config.getDict("backup");
      if (backupConfig == null) {
        backupConfig = backup;
      }
      if (backupConfig == null) {
        return;
      }
      String cronSchedule = backupConfig.getString("cronSchedule");
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
          .withSchedule(cronSchedule(cronSchedule))
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
  public byte[] getLastBackupDir(ComObject cobj, boolean replayedCommand) {
    ComObject retObj = new ComObject();
    if (lastBackupDir != null) {
      retObj.put(ComObject.Tag.directory, lastBackupDir);
    }
    return retObj.serialize();
  }

  public void doBackup() {
    try {
      disableRepartitioner();
      finalBackupException = null;
      doingBackup = true;

      logger.info("Backup Master - begin");

      logger.info("Backup Master - prepareForBackup - begin");
      // tell all servers to pause snapshot and slice the queue
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.dbName, "__none__");
      cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
      cobj.put(ComObject.Tag.method, "prepareForBackup");
      String command = "DatabaseServer:ComObject:prepareForBackup:";
      byte[][] ret = getDatabaseClient().sendToAllShards(null, 0, command, cobj.serialize(), DatabaseClient.Replica.master);
      logger.info("Backup Master - prepareForBackup - finished");

      String subDirectory = ISO8601.to8601String(new Date(System.currentTimeMillis()));
      lastBackupDir = subDirectory;
      JsonDict backup = config.getDict("backup");
      if (backupConfig == null) {
        backupConfig = backup;
      }
      String bucket = backupConfig.getString("bucket");
      String prefix = backupConfig.getString("prefix");

      String type = backupConfig.getString("type");
      if (type.equals("AWS")) {
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
        command = "DatabaseServer:ComObject:doBackupAWS:";
        ret = getDatabaseClient().sendToAllShards(null, 0, command, docobj.serialize(), DatabaseClient.Replica.master);

        logger.info("Backup Master - doBackupAWS - end");
      }
      else if (type.equals("fileSystem")) {
        // if fileSystem
        //    tell all servers to copy files to backup directory with a specific root directory
        String directory = backupConfig.getString("directory");

        logger.info("Backup Master - doBackupFileSystem - begin");

        ComObject docobj = new ComObject();
        docobj.put(ComObject.Tag.dbName, "__none__");
        docobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
        docobj.put(ComObject.Tag.method, "doBackupFileSystem");
        docobj.put(ComObject.Tag.directory, directory);
        docobj.put(ComObject.Tag.subDirectory, subDirectory);
        command = "DatabaseServer:ComObject:doBackupFileSystem:";
        ret = getDatabaseClient().sendToAllShards(null, 0, command, docobj.serialize(), DatabaseClient.Replica.master);

        logger.info("Backup Master - doBackupFileSystem - end");
      }

      while (true) {
        ComObject iscobj = new ComObject();
        iscobj.put(ComObject.Tag.dbName, "__none__");
        iscobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
        iscobj.put(ComObject.Tag.method, "isBackupComplete");
        command = "DatabaseServer:ComObject:isBackupComplete:";

        boolean finished = false;
        outer:
        for (int shard = 0; shard < shardCount; shard++) {
          byte[] currRet = getDatabaseClient().send(null, shard, 0, command, iscobj.serialize(), DatabaseClient.Replica.master);
          ComObject retObj = new ComObject(currRet);
          finished = retObj.getBoolean(ComObject.Tag.isComplete);
          if (!finished) {
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

      String directory = backupConfig.getString("directory");
      Integer maxBackupCount = backupConfig.getInt("maxBackupCount");
      Boolean shared = backupConfig.getBoolean("sharedDirectory");
      if (shared == null) {
        shared = false;
      }
      if (maxBackupCount != null) {
        // delete old backups
        if (type.equals("AWS")) {
          shared = true;
          doDeleteAWSBackups(bucket, prefix, maxBackupCount);
        }
        else if (type.equals("fileSystem")) {
          if (shared) {
            doDeleteFileSystemBackups(directory, maxBackupCount);
          }
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
      command = "DatabaseServer:ComObject:finishBackup:";
      ret = getDatabaseClient().sendToAllShards(null, 0, command, fcobj.serialize(), DatabaseClient.Replica.master);

      logger.info("Backup Master - finishBackup - finished");
    }
    catch (Exception e) {
      finalBackupException = e;
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

  public byte[] prepareForRestore(ComObject cobj, boolean replayedCommand) {
    try {
      isRestoreComplete = false;

      isRunning.set(false);
      snapshotManager.enableSnapshot(false);
      Thread.sleep(5000);
      snapshotManager.deleteSnapshots();

      File file = new File(getDataDir(), "result-sets");
      FileUtils.deleteDirectory(file);

      logManager.deleteLogs();

      return null;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public byte[] doRestoreFileSystem(final ComObject cobj, boolean replayedCommand) {
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          String directory = cobj.getString(ComObject.Tag.directory);
          String subDirectory = cobj.getString(ComObject.Tag.subDirectory);

          directory = directory.replace("$HOME", System.getProperty("user.home"));

          restoreFileSystemSingleDir(directory, subDirectory, "logSequenceNum");
          restoreFileSystemSingleDir(directory, subDirectory, "nextRecordId");
          deleteManager.restoreFileSystem(directory, subDirectory);
          longRunningCommands.restoreFileSystem(directory, subDirectory);
          snapshotManager.restoreFileSystem(directory, subDirectory);
          common.loadSchema(getDataDir());
          logManager.restoreFileSystem(directory, subDirectory);

          isRestoreComplete = true;
        }
        catch (Exception e) {
          logger.error("Error restoring backup", e);
          restoreException = e;
        }
      }
    });
    thread.start();

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

  public byte[] doRestoreAWS(final ComObject cobj, boolean replayedCommand) {
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          //synchronized (restoreAwsMutex) {
            String subDirectory = cobj.getString(ComObject.Tag.subDirectory);
            String bucket = cobj.getString(ComObject.Tag.bucket);
            String prefix = cobj.getString(ComObject.Tag.prefix);

            restoreAWSSingleDir(bucket, prefix, subDirectory, "logSequenceNum");
            restoreAWSSingleDir(bucket, prefix, subDirectory, "nextRecordId");
            deleteManager.restoreAWS(bucket, prefix, subDirectory);
            longRunningCommands.restoreAWS(bucket, prefix, subDirectory);
            snapshotManager.restoreAWS(bucket, prefix, subDirectory);
            common.loadSchema(getDataDir());
            logManager.restoreAWS(bucket, prefix, subDirectory);

            isRestoreComplete = true;
          //}
        }
        catch (Exception e) {
          logger.error("Error restoring backup", e);
          restoreException = e;
        }
      }
    });
    thread.start();

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

  public byte[] isRestoreComplete(ComObject cobj, boolean replayedCommand) {
    try {
      if (restoreException != null) {
        throw new DatabaseException(restoreException);
      }
      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.isComplete, isRestoreComplete);
      return retObj.serialize();
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }


  public byte[] finishRestore(ComObject cobj, boolean replayedCommand) {
    try {
      for (String dbName : getDbNames(getDataDir())) {
        getSnapshotManager().recoverFromSnapshot(dbName);
      }
      getLogManager().applyQueues();
      snapshotManager.enableSnapshot(true);
      isRunning.set(true);
      isRestoreComplete = false;
      System.out.println("Finished restore: shard=" + shard + ", replica=" + replica);

      return null;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public byte[] isEntireRestoreComplete(ComObject cobj, boolean replayedCommand) {
    try {
      if (finalRestoreException != null) {
        throw new DatabaseException("Error restoring backup", finalRestoreException);
      }
      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.isComplete, !doingRestore);

      return retObj.serialize();
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public byte[] startRestore(final ComObject cobj, boolean replayedCommand) {
    doingRestore = true;
    Thread thread = new Thread(new Runnable() {
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
    });
    thread.run();
    return null;
  }

  private Exception finalRestoreException;
  private Exception finalBackupException;

  private void doRestore(String subDirectory) {
    try {
      disableRepartitioner();
      finalRestoreException = null;
      doingRestore = true;
      // delete snapshots and logs
      // enter recovery mode (block commands)
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.dbName, "__none__");
      cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
      cobj.put(ComObject.Tag.method, "prepareForRestore");
      String command = "DatabaseServer:ComObject:prepareForRestore:";
      byte[][] ret = getDatabaseClient().sendToAllShards(null, 0, command, cobj.serialize(), DatabaseClient.Replica.all);

      JsonDict backup = config.getDict("backup");
      if (backupConfig == null) {
        backupConfig = backup;
      }
      String type = backupConfig.getString("type");
      if (type.equals("AWS")) {
        // if aws
        //    tell all servers to upload with a specific root directory

        String bucket = backupConfig.getString("bucket");
        String prefix = backupConfig.getString("prefix");
        cobj = new ComObject();
        cobj.put(ComObject.Tag.dbName, "__none__");
        cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
        cobj.put(ComObject.Tag.method, "doRestoreAWS");
        cobj.put(ComObject.Tag.bucket, bucket);
        cobj.put(ComObject.Tag.prefix, prefix);
        cobj.put(ComObject.Tag.subDirectory, subDirectory);
        command = "DatabaseServer:ComObject:doRestoreAWS:";
        ret = getDatabaseClient().sendToAllShards(null, 0, command, cobj.serialize(), DatabaseClient.Replica.all);
      }
      else if (type.equals("fileSystem")) {
        // if fileSystem
        //    tell all servers to copy files to backup directory with a specific root directory
        String directory = backupConfig.getString("directory");
        cobj = new ComObject();
        cobj.put(ComObject.Tag.dbName, "__none__");
        cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
        cobj.put(ComObject.Tag.method, "doRestoreFileSystem");
        cobj.put(ComObject.Tag.directory, directory);
        cobj.put(ComObject.Tag.subDirectory, subDirectory);
        command = "DatabaseServer:ComObject:doRestoreFileSystem:";
        ret = getDatabaseClient().sendToAllShards(null, 0, command, cobj.serialize(), DatabaseClient.Replica.all);
      }

      while (true) {
        cobj = new ComObject();
        cobj.put(ComObject.Tag.dbName, "__none__");
        cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
        cobj.put(ComObject.Tag.method, "isRestoreComplete");
        command = "DatabaseServer:ComObject:isRestoreComplete:";

        byte[] bytes = cobj.serialize();
        boolean finished = false;
        outer:
        for (int shard = 0; shard < shardCount; shard++) {
          for (int replica = 0; replica < replicationFactor; replica++) {
            byte[] currRet = getDatabaseClient().send(null, shard, replica, command, bytes, DatabaseClient.Replica.specified);
            ComObject retObj = new ComObject(currRet);
            finished = retObj.getBoolean(ComObject.Tag.isComplete);
            if (!finished) {
              break outer;
            }
          }
        }
        if (finished) {
          break;
        }
      }

      cobj = new ComObject();
      cobj.put(ComObject.Tag.dbName, "__none__");
      cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
      cobj.put(ComObject.Tag.method, "finishRestore");
      command = "DatabaseServer:ComObject:finishRestore:";
      ret = getDatabaseClient().sendToAllShards(null, 0, command, cobj.serialize(), DatabaseClient.Replica.all);

      pushSchema();
    }
    catch (Exception e) {
      finalRestoreException = e;
      e.printStackTrace();
      throw new DatabaseException(e);
    }
    finally {
      doingRestore = false;
      startRepartitioner();
    }
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

  public double getResGigWindows() throws IOException, InterruptedException {
    ProcessBuilder builder = new ProcessBuilder().command("tasklist", "/fi", "\"pid eq " + pid + "\"");
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

    ProcessBuilder builder = new ProcessBuilder().command("bin/get-cpu.bat", String.valueOf(pid));
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

  public String getDiskAvailWindows() throws IOException, InterruptedException {
    // disk avail:
    //        SETLOCAL
    //
    //        FOR /F "usebackq tokens=1,2" %%f IN (`PowerShell -NoProfile -EncodedCommand "CgBnAHcAbQBpACAAVwBpAG4AMwAyAF8ATABvAGcAaQBjAGEAbABEAGkAcwBrACAALQBGAGkAbAB0AGUAcgAgACIAQwBhAHAAdABpAG8AbgA9ACcAQwA6ACcAIgB8ACUAewAkAGcAPQAxADAANwAzADcANAAxADgAMgA0ADsAWwBpAG4AdABdACQAZgA9ACgAJABfAC4ARgByAGUAZQBTAHAAYQBjAGUALwAkAGcAKQA7AFsAaQBuAHQAXQAkAHQAPQAoACQAXwAuAFMAaQB6AGUALwAkAGcAKQA7AFcAcgBpAHQAZQAtAEgAbwBzAHQAIAAoACQAdAAtACQAZgApACwAJABmAH0ACgA="`) DO ((SET U=%%f)&(SET F=%%g))
    //
    //        @ECHO Used: %U%
    //            @ECHO Free: %F%

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

  public AtomicInteger getBatchRepartCount() {
    return batchRepartCount;
  }

  public void overrideProLicense() {
    this.overrideProLicense = true;
  }

  public static class Host {
    private String publicAddress;
    private String privateAddress;
    private int port;
    private boolean dead;

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

    public Host(DataInputStream in, long serializationVersionNumber) throws IOException {
      publicAddress = in.readUTF();
      privateAddress = in.readUTF();
      port = in.readInt();
      if (serializationVersionNumber >= SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION_21) {
        dead = in.readBoolean();
      }
    }

    public void serialize(DataOutputStream out, long serializationVersionNumber) throws IOException {
      out.writeUTF(publicAddress);
      out.writeUTF(privateAddress);
      out.writeInt(port);
      if (serializationVersionNumber >= SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION_21) {
        out.writeBoolean(dead);
      }
    }

    public boolean isDead() {
      return dead;
    }
  }

  public static class Shard {
    private Host[] replicas;
    private int masterReplica;

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "EI_EXPOSE_REP2", justification = "copying the passed in data is too slow")
    @SuppressWarnings("PMD.ArrayIsStoredDirectly") //copying the passed in data is too slow
    public Shard(Host[] hosts) {
      this.replicas = hosts;
    }

    public Shard(DataInputStream in, long serializationVersionNumber) throws IOException {
      int count = in.readInt();
      replicas = new Host[count];
      for (int i = 0; i < replicas.length; i++) {
        replicas[i] = new Host(in, serializationVersionNumber);
      }
      if (serializationVersionNumber >= SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION_21) {
        masterReplica = (int) DataUtil.readVLong(in);
      }
    }

    public void serialize(DataOutputStream out, long serializationVersionNumber) throws IOException {
      out.writeInt(replicas.length);
      for (Host host : replicas) {
        host.serialize(out, serializationVersionNumber);
      }
      if (serializationVersionNumber >= SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION_21) {
        DataUtil.writeVLong(out, masterReplica);
      }
    }

    public void setMasterReplica(int masterReplica) {
      this.masterReplica = masterReplica;
    }

    public int getMasterReplica() {
      return this.masterReplica;
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
    private String cluster;
    private Shard[] shards;
    private boolean clientIsInternal;

    public ServersConfig(byte[] bytes, long serializationVersion) throws IOException {
      this(new DataInputStream(new ByteArrayInputStream(bytes)), serializationVersion);
    }

    /**
     * ###############################
     * DON"T MODIFY THIS SERIALIZATION
     * ###############################
     */
    public ServersConfig(DataInputStream in, long serializationVersion) throws IOException {
      if (serializationVersion >= SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION_21) {
        cluster = in.readUTF();
      }
      int count = in.readInt();
      shards = new Shard[count];
      for (int i = 0; i < count; i++) {
        shards[i] = new Shard(in, serializationVersion);
      }
      clientIsInternal = in.readBoolean();
    }

    /**
     * ###############################
     * DON"T MODIFY THIS SERIALIZATION
     * ###############################
     */
    public byte[] serialize(long serializationVersionNumber) throws IOException {
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytesOut);
      serialize(out, serializationVersionNumber);
      out.close();
      return bytesOut.toByteArray();
    }

    public void serialize(DataOutputStream out, long serializationVersionNumber) throws IOException {
      out.writeUTF(cluster);
      out.writeInt(shards.length);
      for (Shard shard : shards) {
        shard.serialize(out, serializationVersionNumber);
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

    public String getCluster() {
      return cluster;
    }

    public ServersConfig(String cluster, JsonArray inShards, int replicationFactor, boolean clientIsInternal) {
      int currServerOffset = 0;
      this.cluster = cluster;
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
    String max = config.getString("maxProcessMemoryTrigger");
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
          while (true) {
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
            try (BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
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
            }
            finally {
              p.destroy();
            }
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
            try {
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
        else if (isWindows()) {
          Double lastReceive = null;
          Double lastTransmit = null;
          long lastRecorded = 0;
          while (true) {
            ProcessBuilder builder = new ProcessBuilder().command("netstat", "-e");
            Process p = builder.start();
            try (BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
              while (true) {
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

  private String getDiskAvailable() throws IOException, InterruptedException {
    ProcessBuilder builder = new ProcessBuilder().command("df", "-h");
    Process p = builder.start();
    try {
      Integer availPos = null;
      Integer mountedPos = null;
      List<String> avails = new ArrayList<>();
      int bestLineMatching = -1;
      int bestLineAmountMatch = 0;
      int mountOffset = 0;
      try (BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
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
      }
    }
    finally {
      p.destroy();
    }
    return null;
  }

  public byte[] getFile(ComObject cobj, boolean replayedCommand) {
    try {
      String filename = cobj.getString(ComObject.Tag.filename);
      File file = new File(installDir, filename);
      if (!file.exists()) {
        return null;
      }
      try (FileInputStream fileIn = new FileInputStream(file)) {
        String ret = StreamUtils.inputStreamToString(fileIn);
        ComObject retObj = new ComObject();
        retObj.put(ComObject.Tag.fileContent, ret);
        return retObj.serialize();
      }
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public byte[] logError(ComObject cobj, boolean replayedCommand) {
    try {
      boolean isClient = cobj.getBoolean(ComObject.Tag.isClient);
      String hostName = cobj.getString(ComObject.Tag.host);
      String msg = cobj.getString(ComObject.Tag.message);
      String exception = cobj.getString(ComObject.Tag.exception);

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
        try (BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
          while (true) {
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
            ret.resGig = getMemValue(parts[i]);
          }
          else if (headerParts[i].toLowerCase().trim().equals("%cpu")) {
            ret.cpu = Double.valueOf(parts[i]);
          }
        }
        ret.diskAvail = getDiskAvailable();
      }
      else if (isUnix()) {
        ProcessBuilder builder = new ProcessBuilder().command("top", "-b", "-n", "1", "-p", String.valueOf(pid));
        Process p = builder.start();
        try (BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
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
          else if (headerParts[i].toLowerCase().trim().equals("%cpu")) {
            ret.cpu = Double.valueOf(parts[i]);
          }
        }
        ret.diskAvail = getDiskAvailable();
      }
      else if (isWindows()) {
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
    catch (Exception e) {
      logger.error("Error checking memory: line2=" + secondToLastLine + ", line1=" + lastLine, e);
      throw new DatabaseException(e);
    }
    return ret;
  }

  public byte[] getOSStats(ComObject cobj, boolean replayedCommand) {
    try {
      OSStats stats = doGetOSStats();
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

      return retObj.serialize();
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
      String max = config.getString("maxJavaHeapTrigger");
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

    try {
      int licensedServerCount = 0;
      int actualServerCount = 0;
      boolean pro = false;
      JsonArray keys = config.getArray("licenseKeys");
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

      JsonArray shards = config.getArray("shards");
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
    logger.info("Syncing database names: shard=" + shard + ", replica=" + replica);
    String command = "DatabaseServer:ComObject:getDbNames:";
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.dbName, "__none__");
    cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
    cobj.put(ComObject.Tag.method, "getDbNames");
    byte[] ret = getDatabaseClient().send(null, 0, 0, command, cobj.serialize(), DatabaseClient.Replica.master);
    ComObject retObj = new ComObject(ret);
    ComArray array = retObj.getArray(ComObject.Tag.dbNames);
    for (int i = 0; i < array.getArray().size(); i++) {
      String dbName = (String)array.getArray().get(i);
      File file = new File(dataDir, "snapshot/" + shard + "/" + replica + "/" + dbName);
      file.mkdirs();
      logger.info("Received database name: name=" + dbName);
    }
  }

  public byte[] getDbNames(ComObject cobj, boolean replayedCommand) {

    try {
      ComObject retObj = new ComObject();
      List<String> dbNames = getDbNames(dataDir);
      ComArray array = retObj.putArray(ComObject.Tag.dbNames, ComObject.Type.stringType);
      for (String dbName : dbNames) {
        array.add(dbName);
      }
      return retObj.serialize();
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
    logManager.enableLogProcessor(false);
  }

  public void disableRepartitioner() {
    repartitioner.interrupt();
    String command = "DatabaseServer:ComObject:stopRepartitioning:";
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.dbName, "__none__");
    cobj.put(ComObject.Tag.method, "stopRepartitioning");
    cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
    getClient().sendToAllShards(null, 0, command, cobj.serialize(), DatabaseClient.Replica.all);
  }

  public byte[] updateSchema(ComObject cobj, boolean replayedCommand) throws IOException {
    if (getShard() == 0 &&
        getCommon().getServersConfig().getShards()[0].getMasterReplica() == getReplica()) {
      return null;
    }

    DatabaseCommon tempCommon = new DatabaseCommon();
    tempCommon.deserializeSchema(cobj.getByteArray(ComObject.Tag.schemaBytes));

    if (tempCommon.getSchemaVersion() > common.getSchemaVersion()) {
      common.deserializeSchema(cobj.getByteArray(ComObject.Tag.schemaBytes));
      common.saveSchema(dataDir);
    }
    return null;
  }

  public void pushSchema() {
    //common.saveSchema(dataDir);

    for (int i = 0; i < shardCount; i++) {


      for (int j = 0; j < replicationFactor; j++) {
        if (i == 0 && j == 0) {
          continue;
        }
        try {
          ComObject cobj = new ComObject();
          cobj.put(ComObject.Tag.dbName, "__none__");
          cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
          cobj.put(ComObject.Tag.method, "updateSchema");
          cobj.put(ComObject.Tag.schemaBytes, common.serializeSchema(SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION));
          String command = "DatabaseServer:ComObject:updateSchema:";
          getDatabaseClient().send(null, i, j, command, cobj.serialize(), DatabaseClient.Replica.specified);
        }
        catch (Exception e) {
          logger.error("Error pushing schema to server: shard=" + i + ", replica=" + j);
        }
      }
    }
  }

  public byte[] updateServersConfig(ComObject cobj, boolean replayedCommand) {

    try {

      long serializationVersion = cobj.getLong(ComObject.Tag.serializationVersion);
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
        if (i == 0 && j == 0) {
          continue;
        }
        try {
          ComObject cobj = new ComObject();
          cobj.put(ComObject.Tag.dbName, "__none__");
          cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
          cobj.put(ComObject.Tag.method, "updateServersConfig");
          cobj.put(ComObject.Tag.serversConfig, common.getServersConfig().serialize(SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION));
          String command = "DatabaseServer:ComObject:updateServersConfig:";
          getDatabaseClient().send(null, i, j, command, cobj.serialize(), DatabaseClient.Replica.specified);
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
    private long[] sequenceNumbers;

    public LogRequest(int size) {
      this.sequenceNumbers = new long[size];
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

    public void setLatch(CountDownLatch latch) {
      this.latch = latch;
    }

    public void setBuffers(List<byte[]> buffers) {
      this.buffers = buffers;
    }

    public List<byte[]> getBuffers() {
      return buffers;
    }

    public long[] getSequenceNumbers() {
      return sequenceNumbers;
    }
  }

  private static Set<String> priorityCommands = new HashSet<>();

  static {
    priorityCommands.add("setMaxRecordId");
    priorityCommands.add("setMaxSequenceNum");
    priorityCommands.add("sendQueueFile");
    priorityCommands.add("sendLogsToPeer");
    priorityCommands.add("pushMaxRecordId");
    priorityCommands.add("pushMaxSequenceNum");
    priorityCommands.add("getSchema");
    priorityCommands.add("synchSchema");
    priorityCommands.add("updateSchema");
    priorityCommands.add("getConfig");
    priorityCommands.add("getRecoverProgress");
    priorityCommands.add("healthCheckPriority");
    priorityCommands.add("getDbNames");
    priorityCommands.add("updateServersConfig");
    priorityCommands.add("prepareForRestore");
    priorityCommands.add("doRestoreAWS");
    priorityCommands.add("doRestoreFileSystem");
    priorityCommands.add("isRestoreComplete");
    priorityCommands.add("finishRestore");
    priorityCommands.add("prepareForBackup");
    priorityCommands.add("doBackupAWS");
    priorityCommands.add("doBackupFileSystem");
    priorityCommands.add("isBackupComplete");
    priorityCommands.add("finishBackup");
    priorityCommands.add("sendLogsToPeer");
    priorityCommands.add("isEntireRestoreComplete");
    priorityCommands.add("isEntireBackupComplete");
    //priorityCommands.add("doPopulateIndex");
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

  public List<Response> dont_use_handleCommands(List<NettyServer.Request> requests, final boolean replayedCommand, boolean enableQueuing) throws IOException {
    List<Response> retList = new ArrayList<>();
    try {
      if (shutdown) {
        throw new DatabaseException("Shutdown in progress");
      }
      LogRequest logRequest = logManager.logRequests(requests, enableQueuing);

      List<Future<byte[]>> futures = new ArrayList<>();
      for (int requestOffset = 0; requestOffset < requests.size(); requestOffset++) {
        NettyServer.Request request = requests.get(requestOffset);
//        futures.add(executor.submit(new Callable<byte[]>() {
//          @Override
//          public byte[] call() throws Exception {
        try {
          if (disableNow && usingMultipleReplicas) {
            throw new LicenseOutOfComplianceException("Licenses out of compliance");
          }

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

          if (!replayedCommand && !isRunning.get() && false /*!priorityCommands.contains(methodStr)*/) {
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

          int masterReplica = common.getServersConfig().getShards()[shard].getMasterReplica();
          if (replica == masterReplica) {
            if (command.contains("xx_repl_xx")) {
              if (DatabaseClient.getWriteVerbs().contains(methodStr)) {
                command += ":xx_sn_xx=" + logRequest.getSequenceNumbers()[requestOffset];
                for (int i = 0; i < replicationFactor; i++) {
                  if (i != replica) {
                    client.get().send(null, shard, i, command, body, DatabaseClient.Replica.specified);
                  }
                }
              }
            }
          }
          retList.add(new Response(ret));
        }
        catch (Exception e) {
          retList.add(new Response(e));
        }
        //return ret;
        //}
        //}));
      }
//      for (Future<byte[]> future : futures) {
//        commandCount.incrementAndGet();
//        try {
//          retList.add(new Response(future.get()));
//        }
//        catch (Exception e) {
//          retList.add(new Response(e));
//        }
//      }
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

      ComObject cobj = null;
      if (methodStr.equals("ComObject")) {
        cobj = new ComObject(body);
        methodStr = cobj.getString(ComObject.Tag.method);
      }

      if (disableNow && usingMultipleReplicas) {
        throw new LicenseOutOfComplianceException("Licenses out of compliance");
      }

//      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
//      DataOutputStream out = new DataOutputStream(bytesOut);
//      out.writeUTF(localCommand);
//      int len = body == null ? 0 : body.length;
//      out.writeInt(len);
//      if (len > 0) {
//        out.write(body);
//      }
//      out.close();
//      String queueCommand = "DatabaseServer:queueForOtherServer:1:" + SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION + ":1:__none__:" + i;
//      replicas[common.getServersConfig().getShards()[shard].getMasterReplica()].do_send(
//          null, queueCommand, bytesOut.toByteArray())
//
      if (methodStr.equals("queueForOtherServer")) {
        try {
          String[] parts = command.split(":");
          int replica = Integer.valueOf(parts[6]);

          DataInputStream in = new DataInputStream(new ByteArrayInputStream(body));
          String queueCommand = in.readUTF();
          int len = in.readInt();
          byte[] queueBody = new byte[len];
          in.readFully(queueBody);
          logManager.logRequestForPeer(queueCommand, queueBody, replica);
          return null;
        }
        catch (Exception e) {
          throw new DatabaseException(e);
        }
      }

      Long existingSequenceNumber = getExistingSequenceNumber(command);

      LogRequest logRequest = logManager.logRequest(command, body, enableQueuing, methodStr, existingSequenceNumber);

      byte[] ret = null;

      if (!replayedCommand && !isRunning.get() && !priorityCommands.contains(methodStr)) {
        throw new DatabaseException("Server not running: command=" + command);
      }

      if (!onlyQueueCommands || !enableQueuing) {
        try {
          if (cobj != null) {
            methodStr = cobj.getString(ComObject.Tag.method);
            Method method = getClass().getMethod(methodStr, ComObject.class, boolean.class);
            ret = (byte[]) method.invoke(this, cobj, replayedCommand);
          }
          else {
            Method method = getClass().getMethod(methodStr, String.class, byte[].class, boolean.class);
            ret = (byte[]) method.invoke(this, command, body, replayedCommand);
          }
        }
        catch (Exception e) {
          e.printStackTrace();
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

        Shard currShard = common.getServersConfig().getShards()[shard];
        int masterReplica = currShard.getMasterReplica();
        if (replica == masterReplica) {
          if (command.contains("xx_repl_xx")) {
            if (DatabaseClient.getWriteVerbs().contains(methodStr)) {
              command += ":xx_sn_xx=" + logRequest.getSequenceNumbers()[0];
              for (int i = 0; i < replicationFactor; i++) {
                if (i != replica) {
                  if (currShard.getReplicas()[i].dead) {
                    logManager.logRequestForPeer(command, body, i);
                  }
                  else {
                    try {
                      client.get().send(null, shard, i, command, body, DatabaseClient.Replica.specified);
                    }
                    catch (DeadServerException e) {
                      logManager.logRequestForPeer(command, body, i);
                    }
                  }
                }
              }
            }
          }
        }
      }

      commandCount.incrementAndGet();
      if (logRequest != null) {
        logRequest.latch.await();
      }

      return ret;
    }
    catch (InterruptedException e) {
      throw new DatabaseException(e);
    }
  }

  private Long getExistingSequenceNumber(String command) {
    Long existingSequenceNumber = null;
    int snPos = command.indexOf("xx_sn_xx=");
    if (snPos != -1) {
      String sn = null;
      int endPos = command.indexOf(":", snPos);
      if (endPos == -1) {
        sn = command.substring(snPos + "xx_sn_xx=".length());
      }
      else {
        sn = command.substring(snPos + "xx_sn_xx=".length(), endPos);
      }
      existingSequenceNumber = Long.valueOf(sn);
    }
    return existingSequenceNumber;
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

  public byte[] healthCheck(ComObject cobj, boolean replayedCommand) {
    ComObject retObj = new ComObject();
    retObj.put(ComObject.Tag.status, "{\"status\" : \"ok\"}");
    return retObj.serialize();
  }

  public byte[] healthCheckPriority(ComObject cobj, boolean replayedCommand) {
    ComObject retObj = new ComObject();
    retObj.put(ComObject.Tag.status, "{\"status\" : \"ok\"}");
    return retObj.serialize();
  }

  public byte[] setMaxSequenceNum(ComObject cobj, boolean replayedCommand) {
    return logManager.setMaxSequenceNum(cobj);
  }

  public byte[] getRecoverProgress(ComObject cobj, boolean replayedCommand) {

    JsonDict dict = new JsonDict();
    dict.put("percentComplete", snapshotManager.getPercentRecoverComplete());
    Exception error = snapshotManager.getErrorRecovering();
    if (error != null) {
      dict.put("error", true);
    }
    ComObject retObj = new ComObject();
    retObj.put(ComObject.Tag.status, dict.toString());
    return retObj.serialize();
  }

  public byte[] pushMaxSequenceNum(ComObject cobj, boolean replayedCommand) {
    logManager.pushMaxSequenceNum();
    return null;
  }

  public byte[] prepareToComeAlive(ComObject cobj, boolean replayedCommand) {
    String slicePoint = null;
    try {
      ComObject pcobj = new ComObject();
      pcobj.put(ComObject.Tag.dbName, "__none__");
      pcobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
      pcobj.put(ComObject.Tag.method, "pushMaxSequenceNum");
      String command = "DatabaseServer:ComObject:pushMaxSequenceNum:";
      getClient().send(null, shard, 0, command, pcobj.serialize(), DatabaseClient.Replica.master,
          true);

      if (shard == 0) {
        pcobj = new ComObject();
        pcobj.put(ComObject.Tag.dbName, "__none__");
        pcobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
        pcobj.put(ComObject.Tag.method, "pushMaxRecordId");
        command = "DatabaseServer:ComObject:pushMaxRecordId:";
        getClient().send(null, shard, 0, command, pcobj.serialize(), DatabaseClient.Replica.master,
            true);
      }

      for (int replica = 0; replica < replicationFactor; replica++) {
        if (replica != this.replica) {
          cobj = new ComObject();
          cobj.put(ComObject.Tag.dbName, "__none__");
          cobj.put(ComObject.Tag.schemaVersion, getCommon().getSchemaVersion());
          cobj.put(ComObject.Tag.method, "sendLogsToPeer");
          cobj.put(ComObject.Tag.replica, this.replica);
          command = "DatabaseServer:ComObject:sendLogsToPeer:";
          getClient().send(null, shard, replica, command, cobj.serialize(), DatabaseClient.Replica.specified,
              true);
        }
      }
      onlyQueueCommands = true;
      slicePoint = logManager.sliceLogs(false);
      logManager.applyLogsFromPeers(slicePoint);
    }
    finally {
      onlyQueueCommands = false;
    }
    logManager.applyLogsAfterSlice(slicePoint);

    return null;
  }

  public byte[] reconfigureCluster(ComObject cobj, boolean replayedCommand) {
    ServersConfig oldConfig = common.getServersConfig();
    try {
      File file = new File(System.getProperty("user.dir"), "config/config-" + getCluster() + ".json");
      if (!file.exists()) {
        file = new File(System.getProperty("user.dir"), "db/src/main/resources/config/config-" + getCluster() + ".json");
      }
      String configStr = StreamUtils.inputStreamToString(new BufferedInputStream(new FileInputStream(file)));
      logger.info("Config: " + configStr);
      JsonDict config = new JsonDict(configStr);

      //validateLicense(config);

      boolean isInternal = false;
      if (config.hasKey("clientIsPrivate")) {
        isInternal = config.getBoolean("clientIsPrivate");
      }
      DatabaseServer.ServersConfig newConfig = new DatabaseServer.ServersConfig(cluster, config.getArray("shards"),
          config.getArray("shards").getDict(0).getArray("replicas").size(), isInternal);

      common.setServersConfig(newConfig);

      common.saveSchema(getDataDir());

      pushSchema();

      Shard[] oldShards = oldConfig.getShards();
      Shard[] newShards = newConfig.getShards();

      int count = newShards.length - oldShards.length;
      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.count, count);

      return retObj.serialize();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public byte[] getConfig(ComObject cobj, boolean replayedCommand) {
    long serializationVersionNumber = cobj.getLong(ComObject.Tag.serializationVersion);
    try {
      byte[] bytes = common.serializeConfig(serializationVersionNumber);
      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.configBytes, bytes);
      return retObj.serialize();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public byte[] getSchema(ComObject cobj, boolean replayedCommand) {
    long serializationVersionNumber = cobj.getLong(ComObject.Tag.serializationVersion);
    try {
      ComObject retObj = new ComObject();

      retObj.put(ComObject.Tag.schemaBytes, common.serializeSchema(serializationVersionNumber));

      return retObj.serialize();
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }


  public static AtomicInteger blockCount = new AtomicInteger();

  public byte[] block(ComObject cobj, boolean replayedCommand) {
    logger.info("called block");
    blockCount.incrementAndGet();

    try {
      Thread.sleep(1000000);
    }
    catch (InterruptedException e) {
      throw new DatabaseException(e);
    }
    return cobj.serialize();
  }

  public static AtomicInteger echoCount = new AtomicInteger(0);
  public static AtomicInteger echo2Count = new AtomicInteger(0);

  public byte[] echo(ComObject cobj, boolean replayedCommand) {
    logger.info("called echo");
    echoCount.set(cobj.getInt(ComObject.Tag.count));
    return cobj.serialize();
  }

  public byte[] echo2(ComObject cobj, boolean replayedCommand) {
    logger.info("called echo2");
    throw new DatabaseException("not supported");
//    if (cobj.getInt(ComObject.Tag.count) != null) {
//      if (echoCount.get() != cobj.getInt(ComObject.Tag.count)) {
//        throw new DatabaseException("InvalidState");
//      }
//    }
//    echo2Count.set(Integer.valueOf(parts[5]));
//    return body;
  }

  public byte[] reserveNextIdFromReplica(ComObject cobj, boolean replayedCommand) {
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
      return retObj.serialize();
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] noOp(ComObject cobj, boolean replayedCommand) {
    return null;
  }


  private final Object nextIdLock = new Object();

  public byte[] allocateRecordIds(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      logger.info("Requesting next record id - begin");
      int schemaVersion = cobj.getInt(ComObject.Tag.schemaVersion);
      if (schemaVersion < getSchemaVersion()) {
        throw new SchemaOutOfSyncException("currVer:" + common.getSchemaVersion() + ":");
      }
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
      return retObj.serialize();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] pushMaxRecordId(ComObject cobj, boolean replayedCommand) {
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
    String command;
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.dbName, dbName);
    cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
    cobj.put(ComObject.Tag.method, "setMaxRecordId");
    cobj.put(ComObject.Tag.maxId, maxId);
    command = "DatabaseServer:ComObject:setMaxRecordId:";

    getDatabaseClient().send(null, 0, 0, command, cobj.serialize(), DatabaseClient.Replica.def, true);
  }

  public byte[] setMaxRecordId(ComObject cobj, boolean replayedCommand) {
    if (replica == common.getServersConfig().getShards()[0].getMasterReplica()) {
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

  public byte[] sendLogsToPeer(ComObject cobj, boolean replayedCommand) {
    int replicaNum = cobj.getInt(ComObject.Tag.replica);

    logManager.sendLogsToPeer(replicaNum);

    return null;
  }

  public byte[] sendQueueFile(ComObject cobj, boolean replayedCommand) {
    try {
      int peerReplica = cobj.getInt(ComObject.Tag.replica);
      String filename = cobj.getString(ComObject.Tag.filename);
      byte[] queueFile = cobj.getByteArray(ComObject.Tag.binaryFileContent);

      logManager.receiveExternalLog(peerReplica, filename, queueFile);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
    return null;
  }

  public byte[] deleteIndexEntryByKey(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return updateManager.deleteIndexEntryByKey(cobj, replayedCommand);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] commit(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return updateManager.commit(cobj, replayedCommand);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] rollback(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return updateManager.rollback(cobj, replayedCommand);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] insertIndexEntryByKey(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return updateManager.insertIndexEntryByKey(cobj, replayedCommand);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] insertIndexEntryByKeyWithRecord(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return updateManager.insertIndexEntryByKeyWithRecord(cobj, replayedCommand);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] batchInsertIndexEntryByKey(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return updateManager.batchInsertIndexEntryByKey(cobj, replayedCommand);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] batchInsertIndexEntryByKeyWithRecord(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return updateManager.batchInsertIndexEntryByKeyWithRecord(cobj, replayedCommand);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] abortTransaction(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    String dbName = parts[5];
    common.getSchemaReadLock(dbName).lock();
    try {
      return transactionManager.abortTransaction(command, body);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] updateRecord(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return updateManager.updateRecord(cobj, replayedCommand);
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

  public byte[] deleteRecord(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return updateManager.deleteRecord(cobj, replayedCommand);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] deleteIndexEntry(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return updateManager.deleteIndexEntry(cobj, replayedCommand);
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

  public byte[] truncateTable(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return updateManager.truncateTable(cobj, replayedCommand);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] countRecords(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return readManager.countRecords(cobj);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] batchIndexLookup(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return readManager.batchIndexLookup(cobj);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] indexLookup(ComObject cobj, boolean replayedCommand) {
    try {
      String dbName = cobj.getString(ComObject.Tag.dbName);
      common.getSchemaReadLock(dbName).lock();
      try {
        return readManager.indexLookup(cobj);
      }
      finally {
        common.getSchemaReadLock(dbName).unlock();
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }


  public byte[] closeResultSet(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return readManager.closeResultSet(cobj, replayedCommand);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] serverSelectDelete(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return readManager.serverSelectDelete(cobj, replayedCommand);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] serverSelect(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return readManager.serverSelect(cobj);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] indexLookupExpression(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return readManager.indexLookupExpression(cobj);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] evaluateCounter(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return readManager.evaluateCounter(cobj);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] getIndexCounts(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return repartitioner.getIndexCounts(cobj);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] testWrite(ComObject cobj, boolean replayedCommand) {
    logger.info("Called testWrite");
    testWriteCallCount.incrementAndGet();
    return null;
  }

  public byte[] deleteMovedRecords(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return repartitioner.deleteMovedRecords(cobj);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

//  public byte[] isRepartitioningRecordsByIdComplete(String command, byte[] body, boolean replayedCommand) {
//    String[] parts = command.split(":");
//    String dbName = parts[5];
//    common.getSchemaReadLock(dbName).lock();
//    try {
//      return repartitioner.isRepartitioningRecordsByIdComplete(command, body);
//    }
//    finally {
//      common.getSchemaReadLock(dbName).unlock();
//    }
//  }

  public byte[] isRepartitioningComplete(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return repartitioner.isRepartitioningComplete(cobj);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

//  public byte[] isDeletingComplete(String command, byte[] body, boolean replayedCommand) {
//    String[] parts = command.split(":");
//    String dbName = parts[5];
//    common.getSchemaReadLock(dbName).lock();
//    try {
//      return repartitioner.isDeletingComplete(command, body);
//    }
//    finally {
//      common.getSchemaReadLock(dbName).unlock();
//    }
//  }

  public byte[] notifyRepartitioningComplete(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return repartitioner.notifyRepartitioningComplete(cobj);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }
//
//  public byte[] notifyDeletingComplete(String command, byte[] body, boolean replayedCommand) {
//    String[] parts = command.split(":");
//    String dbName = parts[5];
//    common.getSchemaReadLock(dbName).lock();
//    try {
//      return repartitioner.notifyDeletingComplete(command, body);
//    }
//    finally {
//      common.getSchemaReadLock(dbName).unlock();
//    }
//  }

  public byte[] beginRebalance(ComObject cobj, boolean replayedCommand) {

    //schema lock below
    return repartitioner.beginRebalance(cobj);
  }

  public byte[] getKeyAtOffset(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return repartitioner.getKeyAtOffset(cobj);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] getPartitionSize(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return repartitioner.getPartitionSize(cobj);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public byte[] stopRepartitioning(ComObject cobj, boolean replayedCommand) {
    return null;
  }

  public byte[] doRebalanceOrderedIndex(ComObject cobj, boolean replayedCommand) {
    return repartitioner.doRebalanceOrderedIndex(cobj);
  }

  public byte[] rebalanceOrderedIndex(ComObject cobj, boolean replayedCommand) {
    //schema lock below
    return repartitioner.rebalanceOrderedIndex(cobj);
  }

  public byte[] moveIndexEntries(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return repartitioner.moveIndexEntries(cobj);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

//  public byte[] doDeleteMovedIndexEntries(final String command, final byte[] body, boolean replayedCommand) {
//    String[] parts = command.split(":");
//    String dbName = parts[4];
//    common.getSchemaReadLock(dbName).lock();
//    try {
//      return repartitioner.doDeleteMovedIndexEntries(command, body);
//    }
//    finally {
//      common.getSchemaReadLock(dbName).unlock();
//    }
//  }
//
//  public byte[] deleteMovedIndexEntries(final String command, final byte[] body, boolean replayedCommand) {
//    String[] parts = command.split(":");
//    String dbName = parts[4];
//    common.getSchemaReadLock(dbName).lock();
//    try {
//      return repartitioner.deleteMovedIndexEntries(command, body);
//    }
//    finally {
//      common.getSchemaReadLock(dbName).unlock();
//    }
//  }

  public byte[] createTable(ComObject cobj, boolean replayedCommand) {
    return schemaManager.createTable(cobj, replayedCommand);
  }

  public byte[] createTableSlave(ComObject cobj, boolean replayedCommand) {
    return schemaManager.createTableSlave(cobj, replayedCommand);
  }

  public byte[] dropTable(ComObject cobj, boolean replayedCommand) {
    return schemaManager.dropTable(cobj, replayedCommand);
  }

  public byte[] createDatabase(ComObject cobj, boolean replayedCommand) {
    return schemaManager.createDatabase(cobj, replayedCommand);
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

  public byte[] addColumn(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaWriteLock(dbName).lock();
    try {
      return schemaManager.addColumn(cobj);
    }
    finally {
      common.getSchemaWriteLock(dbName).unlock();
    }
  }

  public byte[] dropColumn(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaWriteLock(dbName).lock();
    try {
      return schemaManager.dropColumn(cobj);
    }
    finally {
      common.getSchemaWriteLock(dbName).unlock();
    }
  }

  public byte[] dropIndexSlave(ComObject cobj, boolean replayedCommand) {

    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaWriteLock(dbName).lock();
    try {
      return schemaManager.dropIndexSlave(cobj);
    }
    finally {
      common.getSchemaWriteLock(dbName).unlock();
    }
  }

  public byte[] dropIndex(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaWriteLock(dbName).lock();
    try {
      return schemaManager.dropIndex(cobj);
    }
    finally {
      common.getSchemaWriteLock(dbName).unlock();
    }
  }

  public byte[] createIndexSlave(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaWriteLock(dbName).lock();
    try {
      return schemaManager.createIndexSlave(cobj);
    }
    finally {
      common.getSchemaWriteLock(dbName).unlock();
    }
  }

  public byte[] doPopulateIndex(ComObject cobj, boolean replayedCommand) {
    return updateManager.doPopulateIndex(cobj);
  }

  public byte[] populateIndex(ComObject cobj, boolean replayedCommand) {

//    common.getSchemaReadLock(dbName).lock();
//    try {
    return updateManager.populateIndex(cobj);
//    }
//    finally {
//      common.getSchemaReadLock(dbName).unlock();
//    }
  }

  public byte[] forceDeletes(ComObject cobj, boolean replayedCommand) {
    deleteManager.doDeletes();
    return null;
  }

  public byte[] createIndex(ComObject cobj, boolean replayedCommand) {
    return schemaManager.createIndex(cobj, replayedCommand);
  }

  public byte[] expirePreparedStatement(ComObject cobj, boolean replayedCommand) {
    long preparedId = cobj.getLong(ComObject.Tag.preparedId);
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
