package com.sonicbase.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.*;
import com.sonicbase.index.AddressMap;
import com.sonicbase.index.Index;
import com.sonicbase.index.Indices;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.procedure.*;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.execute.Execute;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.sql.Connection;
import java.sql.SQLException;
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
@SuppressWarnings("squid:S1172") // all methods called from method invoker must have cobj and replayed command parms
public class DatabaseServer {

  private static final String SHARDS_STR = "shards";
  private static final String REPLICAS_STR = "replicas";
  private static final String PRIVATE_ADDRESS_STR = "privateAddress";
  private static final String CLIENT_IS_PRIVATE_STR = "clientIsPrivate";
  private static final String OPTIMIZE_READS_FOR_STR = "optimizeReadsFor";
  private static final String NONE_STR = "__none__";
  private static final String STATUS_OK_STR = "{\"status\" : \"ok\"}";
  private static final String NEXT_RECOR_ID_TXT_STR = "/nextRecorId.txt";
  private static final String NEXT_RECORD_ID_STR = "nextRecordId/";
  private static final String REPLICA_STR = ", replica=";
  private static Object deathOverrideMutex = new Object();
  public static boolean[][] deathOverride;
  private static Logger logger = LoggerFactory.getLogger(DatabaseServer.class);

  private static org.apache.log4j.Logger errorLogger = org.apache.log4j.Logger.getLogger("com.sonicbase.errorLogger");
  private static org.apache.log4j.Logger clientErrorLogger = org.apache.log4j.Logger.getLogger("com.sonicbase.clientErrorLogger");

  static final boolean USE_SNAPSHOT_MGR_OLD = true;
  private int port;
  private String host;
  private String cluster;

  private AtomicBoolean isRunning;
  private ThreadPoolExecutor executor;
  private boolean compressRecords = false;
  private boolean useUnsafe;
  private String xmx;
  private String installDir;
  private boolean throttleInsert;
  private DeleteManager deleteManager;
  private AtomicInteger batchRepartCount = new AtomicInteger();
  private boolean usingMultipleReplicas = false;
  private AWSClient awsClient;
  private boolean onlyQueueCommands;
  private boolean applyingQueuesAndInteractive;
  private MethodInvoker methodInvoker;
  private AddressMap addressMap;
  private BulkImportManager bulkImportManager;
  private boolean waitingForServersToStart = false;
  private Thread streamsConsumerMonitorthread;

  private Connection connectionForStoredProcedure;
  private AtomicBoolean isRecovered;
  private MasterManager masterManager;

  private DatabaseCommon common = new DatabaseCommon();
  private AtomicReference<DatabaseClient> client = new AtomicReference<>();
  private PartitionManager partitionManager;
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
  private SnapshotManager snapshotManager;
  private TransactionManager transactionManager;
  private ReadManager readManager;
  private LogManager logManager;
  private SchemaManager schemaManager;
  private Object proServer;
  private LicenseManagerProxy licenseManager;

  public static boolean[][] getDeathOverride() {
    return deathOverride;
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
      final ObjectNode config, String cluster, String host, int port, AtomicBoolean isRunning,
      AtomicBoolean isRecovered, String gclog, String xmx) {
    setConfig(config, cluster, host, port, false, isRunning, isRecovered, gclog, xmx);
  }

  public void setConfig(
      final ObjectNode config, String cluster, String host, int port,
      boolean unitTest, AtomicBoolean isRunning, AtomicBoolean isRecovered, String gclog) {
    setConfig(config, cluster, host, port, unitTest, isRunning, isRecovered, gclog, null);
  }

  public void setConfig(
      final ObjectNode config, String cluster, String host, int port,
      boolean unitTest, AtomicBoolean isRunning, AtomicBoolean isRecovered, String gclog, String xmx) {

    this.isRunning = isRunning;
    this.isRecovered = isRecovered;
    this.config = config;
    this.cluster = cluster;
    this.host = host;
    this.port = port;
    this.xmx = xmx;

    ObjectNode databaseDict = config;
    this.dataDir = databaseDict.get("dataDirectory").asText();
    this.dataDir = dataDir.replace("$HOME", System.getProperty("user.home"));
    this.installDir = databaseDict.get("installDirectory").asText();
    this.installDir = installDir.replace("$HOME", System.getProperty("user.home"));
    ArrayNode shards = databaseDict.withArray(SHARDS_STR);
    int replicaCount = shards.get(0).withArray(REPLICAS_STR).size();
    if (replicaCount > 1) {
      usingMultipleReplicas = true;
    }

    ObjectNode firstServer = (ObjectNode) shards.get(0).withArray(REPLICAS_STR).get(0);
    ServersConfig serversConfig = null;
    executor = ThreadUtil.createExecutor(Runtime.getRuntime().availableProcessors() * 128, "Sonicbase DatabaseServer Thread");

    if (databaseDict.has("compressRecords")) {
      compressRecords = databaseDict.get("compressRecords").asBoolean();
    }
    if (databaseDict.has("useUnsafe")) {
      useUnsafe = databaseDict.get("useUnsafe").asBoolean();
    }
    else {
      useUnsafe = true;
    }

    this.masterAddress = firstServer.get(PRIVATE_ADDRESS_STR).asText();
    this.masterPort = firstServer.get("port").asInt();

    if (firstServer.get(PRIVATE_ADDRESS_STR).asText().equals(host) && firstServer.get("port").asLong() == port) {
      this.shard = 0;
      this.replica = 0;
      common.setShard(0);
      common.setReplica(0);

    }
    boolean isInternal = false;
    if (databaseDict.has(CLIENT_IS_PRIVATE_STR)) {
      isInternal = databaseDict.get(CLIENT_IS_PRIVATE_STR).asBoolean();
    }
    boolean optimizedForThroughput = true;
    if (databaseDict.has(OPTIMIZE_READS_FOR_STR)) {
      String text = databaseDict.get(OPTIMIZE_READS_FOR_STR).asText();
      if (!text.equalsIgnoreCase("totalThroughput")) {
        optimizedForThroughput = false;
      }
    }
    serversConfig = new ServersConfig(cluster, shards, isInternal, optimizedForThroughput);

    initServersForUnitTest(host, port, unitTest, serversConfig);

    this.replica = serversConfig.getThisReplica(host, port);

    common.setShard(serversConfig.getThisShard(host, port));
    common.setReplica(this.replica);
    common.setServersConfig(serversConfig);
    this.shard = common.getShard();
    this.shardCount = serversConfig.getShardCount();

    common.setServersConfig(serversConfig);

    logger.info("config={}", config);

    logger.info("useUnsafe={}", useUnsafe);

    common.setHaveProLicense(true);

    this.awsClient = new AWSClient(getDatabaseClient());

    addressMap = new AddressMap(this);

    common.setServersConfig(serversConfig);

    this.deleteManager = new DeleteManager(this);

    this.updateManager = new UpdateManager(this);
    this.snapshotManager = new SnapshotManager(this);

    this.transactionManager = new TransactionManager(this);
    this.readManager = new ReadManager(this);
    this.logManager = new LogManager(this, new File(dataDir, "log"));
    this.schemaManager = new SchemaManager(this);
    this.bulkImportManager = new BulkImportManager(this);
    this.masterManager = new MasterManager(this);
    this.partitionManager = new PartitionManager(this, common);

    this.methodInvoker = new MethodInvoker(this, logManager);
    this.methodInvoker.registerMethodProvider("BulkImportManager", bulkImportManager);
    this.methodInvoker.registerMethodProvider("DeleteManager", deleteManager);
    this.methodInvoker.registerMethodProvider("LogManager", logManager);
    this.methodInvoker.registerMethodProvider("MasterManager", masterManager);
    this.methodInvoker.registerMethodProvider("PartitionManager", partitionManager);
    this.methodInvoker.registerMethodProvider("ReadManager", readManager);
    this.methodInvoker.registerMethodProvider("SchemaManager", schemaManager);
    this.methodInvoker.registerMethodProvider("SnapshotManager", snapshotManager);
    this.methodInvoker.registerMethodProvider("TransactionManager", transactionManager);
    this.methodInvoker.registerMethodProvider("UpdateManager", updateManager);
    this.methodInvoker.registerMethodProvider("DatabaseServer", this);
    this.longRunningCommands = new LongRunningCalls(this);

    try {
      Class proClz = Class.forName("com.sonicbase.server.ProServer");
      Constructor ctor = proClz.getConstructor(DatabaseServer.class);
      proServer = ctor.newInstance(this);
    }
    catch (Exception e) {
      logger.error("Error initializing pro server", e);
    }

    updateManager.initStreamManager();
    licenseManager = new LicenseManagerProxy(proServer);

    this.replicationFactor = shards.get(0).withArray(REPLICAS_STR).size();

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

    common.saveSchema(dataDir);

    for (String dbName : dbNames) {
      logger.info("Loaded database schema: dbName={}, tableCount={}", dbName, common.getTables(dbName).size());
      getIndices().put(dbName, new Indices());

      schemaManager.addAllIndices(dbName);
    }

    startLongRunningCommands();

    disable();

    logger.info("Started server");

  }

  public void shutdown() {
    try {
      shutdown = true;

      shutdownProServer();

      if (streamsConsumerMonitorthread != null) {
        streamsConsumerMonitorthread.interrupt();
        streamsConsumerMonitorthread.join();
      }

      if (longRunningCommands != null) {
        longRunningCommands.shutdown();
      }
      shutdownDeathMonitor();
      shutdownRepartitioner();

      deleteManager.shutdown();
      snapshotManager.shutdown();
      logManager.shutdown();
      executor.shutdownNow();
      methodInvoker.shutdown();
      client.get().shutdown();
      if (connectionForStoredProcedure != null) {
        connectionForStoredProcedure.close();
      }
      readManager.shutdown();
      bulkImportManager.shutdown();

      executor.shutdownNow();
    }
    catch (Exception e) {
      throw new DatabaseException("Error shutting down DatabaseServer", e);
    }
  }

  private void shutdownProServer() {
    if (proServer != null) {
      try {
        Class proClz = Class.forName("com.sonicbase.server.ProServer");
        Method method = proClz.getMethod("shutdown");
        method.invoke(proServer);
      }
      catch (Exception e) {
        logger.error("Error shutting down pro server");
      }
    }
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
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

  public void removeIndices(String dbName, String tableName) {
    if (getIndices() != null && getIndices().get(dbName) != null && getIndices().get(dbName).getIndices() != null) {
      getIndices().get(dbName).getIndices().remove(tableName);
    }
  }

  public void startStreamsConsumerMonitor() {
    updateManager.startStreamsConsumerMasterMonitor();
  }

  public void shutdownDeathMonitor() {
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

  public void startDeathMonitor() {
    startDeathMonitor(null);
  }

  public void startDeathMonitor(Long timeoutOverride) {
    synchronized (deathMonitorMutex) {

      startDeathReportThread(timeoutOverride);

      shutdownDeathMonitor = false;
      logger.info("Starting death monitor");
      deathMonitorThreads = new Thread[shardCount][];
      for (int i = 0; i < shardCount; i++) {
        final int localShard = i;
        deathMonitorThreads[i] = new Thread[replicationFactor];
        for (int j = 0; j < replicationFactor; j++) {
          final int localReplica = j;
          if (localShard == this.shard && localReplica == this.replica) {
            boolean wasDead = common.getServersConfig().getShards()[localShard].getReplicas()[localReplica].isDead();
            if (wasDead) {
              AtomicBoolean isHealthy = new AtomicBoolean(true);
              handleHealthChange(isHealthy, wasDead, true, localShard, localReplica);
            }
            continue;
          }
          startDeathMonitorThread(timeoutOverride, i, localShard, j, localReplica);
        }
      }
    }
  }

  private void startDeathReportThread(Long timeoutOverride) {
    deathReportThread = ThreadUtil.createThread(() -> {
      while (!shutdown) {
        try {
          Thread.sleep(timeoutOverride == null ? 10_000 : timeoutOverride);
          StringBuilder builder = new StringBuilder();
          for (int i = 0; i < shardCount; i++) {
            for (int j = 0; j < replicationFactor; j++) {
              builder.append("[").append(i).append(",").append(j).append("=");
              builder.append(common.getServersConfig().getShards()[i].getReplicas()[j].isDead() ? "dead" : "alive").append("]");
            }
          }
          logger.info("Death status={}", builder);

          if (replicationFactor > 1 && masterManager.isNoLongerMaster()) {
            logger.info("No longer master. Shutting down resources");
            shutdownDeathMonitor();
            shutdownRepartitioner();

            licenseManager.shutdownMasterLicenseValidator();

            masterManager.shutdownFixSchemaTimer();

            if (streamsConsumerMonitorthread != null) {
              streamsConsumerMonitorthread.interrupt();
              streamsConsumerMonitorthread.join();
              streamsConsumerMonitorthread = null;
            }

            break;
          }
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
        catch (Exception e) {
          logger.error("Error in death reporting thread", e);
        }
      }
    }, "SonicBase Death Reporting Thread");
    deathReportThread.start();
  }

  private void startDeathMonitorThread(Long timeoutOverride, int i, int localShard, int j, int localReplica) {
    deathMonitorThreads[i][j] = ThreadUtil.createThread(() -> {
      while (!shutdownDeathMonitor) {
        try {
          Thread.sleep((deathOverride == null && timeoutOverride == null) ? 2000 : timeoutOverride);
          AtomicBoolean isHealthy = new AtomicBoolean();
          for (int i1 = 0; i1 < 5; i1++) {
            checkHealthOfServer(localShard, localReplica, isHealthy);
            if (isHealthy.get()) {
              break;
            }
            Thread.sleep(timeoutOverride == null ? 1_000 : timeoutOverride);
          }
          boolean wasDead = common.getServersConfig().getShards()[localShard].getReplicas()[localReplica].isDead();
          boolean changed = false;
          if (wasDead && isHealthy.get() || !wasDead && !isHealthy.get()) {
            changed = true;
          }
          if (changed && isHealthy.get()) {
            ComObject cobj = new ComObject();
            cobj.put(ComObject.Tag.DB_NAME, NONE_STR);
            cobj.put(ComObject.Tag.SCHEMA_VERSION, common.getSchemaVersion());
            cobj.put(ComObject.Tag.METHOD, "DatabaessServer:prepareToComeAlive");

            getDatabaseClient().send(null, localShard, localReplica, cobj, DatabaseClient.Replica.SPECIFIED, true);
          }
          handleHealthChange(isHealthy, wasDead, changed, localShard, localReplica);
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
        catch (Exception e) {
          if (!shutdownDeathMonitor) {
            logger.error("Error in death monitor thread: shard={}, replica={}", localShard, localReplica, e);
          }
        }
      }
    }, "SonicBase Death Monitor Thread: shard=" + i + REPLICA_STR + j);
    deathMonitorThreads[i][j].start();
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
      logger.info("server health changed: shard={}, replica={}, isHealthy={}", shard, replica, isHealthy.get());
      common.saveSchema(getDataDir());
      pushSchema();
    }
  }

  private int replicaDeadForRestart = -1;

  public void checkHealthOfServer(final int shard, final int replica, final AtomicBoolean isHealthy) throws InterruptedException {

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
    Thread checkThread = ThreadUtil.createThread(() -> {
        try {
          ComObject cobj = new ComObject();
          cobj.put(ComObject.Tag.DB_NAME, NONE_STR);
          cobj.put(ComObject.Tag.SCHEMA_VERSION, common.getSchemaVersion());
          cobj.put(ComObject.Tag.METHOD, "DatabaseServer:healthCheck");
          byte[] bytes = getDatabaseClient().send(null, shard, replica, cobj, DatabaseClient.Replica.SPECIFIED, true);
          ComObject retObj = new ComObject(bytes);
          if (retObj.getString(ComObject.Tag.STATUS).equals(STATUS_OK_STR)) {
            isHealthy.set(true);
          }
        }
        catch (DeadServerException e) {
          logger.error("Error checking health of server - dead server: shard={}, replica={}", shard, replica);
        }
        catch (Exception e) {
          logger.error("Error checking health of server: shard={}, replica={}", shard, replica, e);
        }
        finally {
          finished.set(true);
        }
    }, "SonicBase Check Health of Server Thread");
    checkThread.start();

    int i = 0;
    while (!finished.get()) {
      Thread.sleep(100);
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
      logger.error("error disabling", e);
    }
  }

  public ThreadPoolExecutor getExecutor() {
    return executor;
  }

  public Thread[][] getDeathMonitorThreads() {
    return deathMonitorThreads;
  }

  public void setReplicaDeadForRestart(int replicaDeadForRestart) {
    this.replicaDeadForRestart = replicaDeadForRestart;
  }

  public boolean isApplyingQueuesAndInteractive() {
    return applyingQueuesAndInteractive;
  }

  public boolean shouldDisableNow() {
    return false;
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

  public SnapshotManager getSnapshotManager() {
    return snapshotManager;
  }

  @SuppressWarnings("squid:S1172") // cobj and replayedCommand are required
  public ComObject getRecoverProgress(ComObject cobj, boolean replayedCommand) {
    ComObject retObj = new ComObject();
    if (waitingForServersToStart) {
      retObj.put(ComObject.Tag.PERCENT_COMPLETE, 0d);
      retObj.put(ComObject.Tag.STAGE, "waitingForServersToStart");
    }
    else if (snapshotManager.isRecovering()) {
      snapshotManager.getPercentRecoverComplete(retObj);
    }
    else if (!getDeleteManager().isForcingDeletes()) {
      retObj.put(ComObject.Tag.PERCENT_COMPLETE, logManager.getPercentApplyQueuesComplete());
      retObj.put(ComObject.Tag.STAGE, "applyingLogs");
    }
    else {
      retObj.put(ComObject.Tag.PERCENT_COMPLETE, getDeleteManager().getPercentDeleteComplete());
      retObj.put(ComObject.Tag.STAGE, "forcingDeletes");
    }
    Exception error = snapshotManager.getErrorRecovering();
    if (error != null) {
      retObj.put(ComObject.Tag.ERROR, true);
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
    return new ParametersImpl(parmsArray);
  }

  public ConnectionProxy getConnectionForStoredProcedure(String dbName) throws ClassNotFoundException, SQLException {
    synchronized (this) {
      if (connectionForStoredProcedure != null) {
        return (ConnectionProxy) connectionForStoredProcedure;
      }
      ArrayNode array = config.withArray(SHARDS_STR);
      ObjectNode replicaDict = (ObjectNode) array.get(0);
      ArrayNode replicasArray = replicaDict.withArray(REPLICAS_STR);
      JsonNode node = config.get(CLIENT_IS_PRIVATE_STR);
      final String address = node != null && node.asBoolean() ?
          replicasArray.get(0).get(PRIVATE_ADDRESS_STR).asText() :
          replicasArray.get(0).get("publicAddress").asText();
      final int localPort = replicasArray.get(0).get("port").asInt();

      Class.forName("com.sonicbase.jdbcdriver.Driver");
      connectionForStoredProcedure = new ConnectionProxy("jdbc:sonicbase:" + address + ":" + localPort + "/" + dbName, this);

      ((ConnectionProxy)connectionForStoredProcedure).getDatabaseClient().syncSchema();
      return (ConnectionProxy) connectionForStoredProcedure;
    }
  }

  public ComObject executeProcedure(final ComObject cobj, boolean replayedCommand) {
    ConnectionProxy conn = null;
    try {
      if (!common.haveProLicense()) {
        throw new InsufficientLicense("You must have a pro license to use stored procedures");
      }

      String sql = cobj.getString(ComObject.Tag.SQL);
      CCJSqlParserManager parser = new CCJSqlParserManager();
      Statement statement = parser.parse(new StringReader(sql));
      if (!(statement instanceof Execute)) {
        throw new DatabaseException("Invalid command: sql=" + sql);
      }
      Execute execute = (Execute) statement;

      Parameters parms = getParametersFromStoredProcedure(execute);

      String dbName = cobj.getString(ComObject.Tag.DB_NAME);
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

      long storedProcedureId = cobj.getLong(ComObject.Tag.ID);
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

  public ComObject executeProcedurePrimary(final ComObject cobj, boolean replayedCommand) {
    ThreadPoolExecutor localExecutor = null;
    try {
      if (!common.haveProLicense()) {
        throw new InsufficientLicense("You must have a pro license to use stored procedures");
      }
      String sql = cobj.getString(ComObject.Tag.SQL);
      CCJSqlParserManager parser = new CCJSqlParserManager();
      Statement statement = parser.parse(new StringReader(sql));
      if (!(statement instanceof Execute)) {
        throw new DatabaseException("Invalid command: sql=" + sql);
      }
      Execute execute = (Execute) statement;

      Parameters parms = getParametersFromStoredProcedure(execute);

      String dbName = cobj.getString(ComObject.Tag.DB_NAME);
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

      cobj.put(ComObject.Tag.ID, storedProcedureId);

      localExecutor = ThreadUtil.createExecutor(shardCount, "SonicBase executeProcedurePrimary Thread");
      // call all shards
      List<Future> futures = new ArrayList<>();
      for (int i = 0; i < shardCount; i++) {
        final int localShard = i;
        futures.add(localExecutor.submit(new Callable(){
          @Override
          public Object call() throws Exception {
            return getDatabaseClient().send("DatabaseServer:executeProcedure", localShard, 0, cobj, DatabaseClient.Replica.DEF);
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
      if (localExecutor != null) {
        localExecutor.shutdownNow();
      }
    }
  }

  public boolean getShutdown() {
    return shutdown;
  }

  public String getXmx() {
    return xmx;
  }

  public MasterManager getMasterManager() {
    return masterManager;
  }

  public boolean getShutdownDeathMonitor() {
    return shutdownDeathMonitor;
  }

  public boolean useUnsafe() {
    return useUnsafe;
  }

  public boolean compressRecords() {
    return compressRecords;
  }

  public void removeIndex(String dbName, String table, String indexName) {
    getIndices().get(dbName).getIndices().get(table).remove(indexName);
  }

  public void setDatabaseClient(DatabaseClient databaseClient) {
    this.client.set(databaseClient);
  }

  public void setRecovered(boolean recovered) {
    this.isRecovered.set(recovered);
  }

  public void setShard(int shard) {
    this.shard = shard;
  }

  public void setReplica(int replica) {
    this.replica = replica;
  }

  public void setReplicationFactor(int replicationFactor) {
    this.replicationFactor = replicationFactor;
  }

  public void setDataDir(String dataDir) {
    this.dataDir = dataDir;
  }

  public Object getProServer() {
    return proServer;
  }

  public void startMasterLicenseValidator() {
    licenseManager.startMasterLicenseValidator();
  }

  @SuppressWarnings("squid:S1186") // the NullX509TrustManager isn't suppose to do anything
  private static class NullX509TrustManager implements X509TrustManager {
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
    }

    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
    }

    public X509Certificate[] getAcceptedIssuers() {
      return new X509Certificate[0];
    }
  }

  @SuppressWarnings("squid:S3510") // the point of the NullHostnameVerifier is to always return true
  private static class NullHostnameVerifier implements HostnameVerifier {
    public boolean verify(String hostname, SSLSession session) {
      return true;
    }
  }

  public int getReplicationFactor() {
    return replicationFactor;
  }

  public DatabaseClient getDatabaseClient() {
    synchronized (this.client) {
      if (this.client.get() != null) {
        return this.client.get();
      }
      DatabaseClient localClient = new DatabaseClient(masterAddress, masterPort, common.getShard(), common.getReplica(), false, common, this);
      this.client.set(localClient);
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
            tableSchema.getIndices().put(indexName, indexSchema);
            tableSchema.getIndexesById().put(indexSchema.getIndexId(), indexSchema);
            common.saveSchema(getDataDir());
          }
        }
      }
      if (indexSchema != null) {
        break;
      }
    }
    return indexSchema;
  }

  public Index getIndex(String dbName, String tableName, String indexName) {
    TableSchema tableSchema = common.getTables(dbName).get(tableName);
    Map<String, ConcurrentHashMap<String, Index>> indices = getIndices(dbName).getIndices();
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

  public PartitionManager getPartitionManager() {
    return partitionManager;
  }

  public void enableSnapshot(boolean enable) {
    snapshotManager.enableSnapshot(enable);
  }

  public void runSnapshot() throws IOException {
    for (String dbName : getDbNames(dataDir)) {
      snapshotManager.runSnapshot(dbName);
    }
    getCommon().saveSchema(getDataDir());
  }

  public void recoverFromSnapshot() {
    common.loadSchema(dataDir);
    Set<String> dbNames = new HashSet<>();
    for (String dbName : common.getDatabases().keySet()) {
      dbNames.add(dbName);
    }
    for (String dbName : getDbNames(dataDir)) {
      dbNames.add(dbName);
    }
    for (String dbName : dbNames) {
      snapshotManager.recoverFromSnapshot(dbName);
    }
  }

  public void purgeMemory() {
    for (Indices indices : indexes.values()) {
      for (ConcurrentHashMap<String, Index> index : indices.getIndices().values()) {
        for (Index innerIndex : index.values()) {
          innerIndex.clear();
        }
      }
    }
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

  public void setThrottleInsert(boolean throttle) {
    this.throttleInsert = throttle;
  }

  public boolean isThrottleInsert() {
    return throttleInsert;
  }

  public DeleteManager getDeleteManager() {
    return deleteManager;
  }

  public AtomicInteger getBatchRepartCount() {
    return batchRepartCount;
  }

  private static final String OS = System.getProperty("os.name").toLowerCase();

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

  public void setIsRunning(boolean isRunning) {
    this.isRunning.set(isRunning);
  }

  public boolean isRecovered() {
    return isRecovered.get();
  }

  public LongRunningCalls getLongRunningCommands() {
    return longRunningCommands;
  }

  public ComObject areAllLongRunningCommandsComplete(ComObject cobj, boolean replayedCommand) {
    ComObject retObj = new ComObject();
    if (longRunningCommands.getCommandCount() == 0) {
      retObj.put(ComObject.Tag.IS_COMPLETE, true);
    }
    else {
      retObj.put(ComObject.Tag.IS_COMPLETE, false);
    }
    return retObj;
  }

  private void startLongRunningCommands() {
    longRunningCommands.load();

    longRunningCommands.execute();
  }

  public ComObject getDbNames(ComObject cobj, boolean replayedCommand) {

    try {
      ComObject retObj = new ComObject();
      List<String> dbNames = getDbNames(getDataDir());
      ComArray array = retObj.putArray(ComObject.Tag.DB_NAMES, ComObject.Type.STRING_TYPE);
      for (String dbName : dbNames) {
        array.add(dbName);
      }
      return retObj;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  @SuppressWarnings("squid:S3516") //invoked method must always return a value
  public ComObject updateSchema(ComObject cobj, boolean replayedCommand) {
    if (replayedCommand) {
      return null;
    }
    DatabaseCommon tempCommon = new DatabaseCommon();
    tempCommon.deserializeSchema(cobj.getByteArray(ComObject.Tag.SCHEMA_BYTES));

    synchronized (common) {
      if (tempCommon.getSchemaVersion() > common.getSchemaVersion()) {
        common.deserializeSchema(cobj.getByteArray(ComObject.Tag.SCHEMA_BYTES));
        common.saveSchema(getDataDir());
      }
    }
    return null;
  }



  public ComObject logError(ComObject cobj, boolean replayedCommand) {
    try {
      boolean isClient = cobj.getBoolean(ComObject.Tag.IS_CLIENT);
      String hostName = cobj.getString(ComObject.Tag.HOST);
      String msg = cobj.getString(ComObject.Tag.MESSAGE);
      String exception = cobj.getString(ComObject.Tag.EXCEPTION);

      StringBuilder actualMsg = new StringBuilder();
      actualMsg.append("host=").append(hostName).append("\n");
      actualMsg.append("msg=").append(msg).append("\n");
      if (exception != null) {
        actualMsg.append("exception=").append(exception);
      }

      if (isClient) {
        getClientErrorLogger().error(actualMsg.toString());
      }
      else {
        getErrorLogger().error(actualMsg.toString());
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
    return null;
  }


  public ComObject promoteToMasterAndPushSchema(ComObject cobj, boolean replayedCommand) {
    int localShard = cobj.getInt(ComObject.Tag.SHARD);
    int localReplica = cobj.getInt(ComObject.Tag.REPLICA);

    logger.info("promoting to master: shard={}, replica={}", localShard, localReplica);
    common.getServersConfig().getShards()[localShard].setMasterReplica(localReplica);
    common.saveSchema(getDataDir());
    pushSchema();
    return null;
  }

  public ComObject markReplicaAlive(ComObject cobj, boolean replayedCommand) {
    int replicaToMarkAlive = cobj.getInt(ComObject.Tag.REPLICA);
    logger.info("Marking replica alive: replica={}", replicaToMarkAlive);
    for (int localShard = 0; localShard < getShardCount(); localShard++) {
      common.getServersConfig().getShards()[localShard].getReplicas()[replicaToMarkAlive].setDead(false);
    }
    common.saveSchema(getDataDir());
    pushSchema();

    setReplicaDeadForRestart(-1);
    return null;
  }


  public ComObject markReplicaDead(ComObject cobj, boolean replayedCommand) {
    int replicaToKill = cobj.getInt(ComObject.Tag.REPLICA);
    logger.info("Marking replica dead: replica={}", replicaToKill);
    for (int localShard = 0; localShard < getShardCount(); localShard++) {
      common.getServersConfig().getShards()[localShard].getReplicas()[replicaToKill].setDead(true);
    }
    common.saveSchema(getDataDir());
    pushSchema();

    setReplicaDeadForRestart(replicaToKill);
    return null;
  }


  protected void syncDbNames() {
    logger.info("Syncing database names: shard={}, replica={}", shard, replica);
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, NONE_STR);
    cobj.put(ComObject.Tag.SCHEMA_VERSION, common.getSchemaVersion());
    byte[] ret = getDatabaseClient().send("DatabaseServer:getDbNames", 0, 0, cobj, DatabaseClient.Replica.MASTER, true);
    ComObject retObj = new ComObject(ret);
    ComArray array = retObj.getArray(ComObject.Tag.DB_NAMES);
    for (int i = 0; i < array.getArray().size(); i++) {
      String dbName = (String) array.getArray().get(i);
      File file = null;
      if (USE_SNAPSHOT_MGR_OLD) {
        file = new File(dataDir, "snapshot/" + shard + File.separator + replica + File.separator + dbName);
      }
      else {
        file = new File(dataDir, "delta/" + shard + File.separator + replica + File.separator + dbName);
      }
      file.mkdirs();
      logger.info("Received database name: name={}", dbName);
    }
  }


  public List<String> getDbNames(String dataDir) {
    return common.getDbNames(dataDir);
  }

  public void startRepartitioner() {
    logger.info("startRepartitioner - begin");
    if (!partitionManager.isRunning()) {
      partitionManager.start();
    }
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

  public Indices getIndices(String dbName) {
    return indexes.computeIfAbsent(dbName, k -> new Indices());
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

  public void shutdownRepartitioner() {
    if (partitionManager == null) {
      return;
    }
    logger.info("Shutdown partitionManager - begin");
    partitionManager.shutdown();
    partitionManager.isRebalancing.set(false);
    partitionManager.stopShardsFromRepartitioning();
    partitionManager = new PartitionManager(this, common);
    logger.info("Shutdown partitionManager - end");
  }

  public ComObject updateIndexSchema(ComObject cobj, boolean replayedCommand) {

    String dbName = cobj.getString(ComObject.Tag.DB_NAME);
    String tableName = cobj.getString(ComObject.Tag.TABLE_NAME);
    String indexName = cobj.getString(ComObject.Tag.INDEX_NAME);
    byte[] bytes = cobj.getByteArray(ComObject.Tag.SCHEMA_BYTES);
    int schemaVersion = cobj.getInt(ComObject.Tag.SCHEMA_VERSION);
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
    TableSchema tableSchema = common.getTables(dbName).get(tableName);
    try {
      TableSchema.deserializeIndexSchema(in, tableSchema);
      IndexSchema indexSchema = tableSchema.getIndices().get(indexName);
      snapshotManager.saveIndexSchema(dbName, schemaVersion, tableSchema, indexSchema);
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
    return null;
  }


  public void pushIndexSchema(String dbName, int schemaVersion, TableSchema tableSchema, IndexSchema indexSchema) {
    try {
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.DB_NAME, dbName);
      cobj.put(ComObject.Tag.SCHEMA_VERSION, schemaVersion);
      cobj.put(ComObject.Tag.METHOD, "DatabaseServer:updateIndexSchema");
      cobj.put(ComObject.Tag.TABLE_NAME, tableSchema.getName());
      cobj.put(ComObject.Tag.INDEX_NAME, indexSchema.getName());
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytesOut);
      TableSchema.serializeIndexSchema(out, tableSchema, indexSchema);

      cobj.put(ComObject.Tag.SCHEMA_BYTES, bytesOut.toByteArray());

      for (int i = 0; i < shardCount; i++) {
        for (int j = 0; j < replicationFactor; j++) {
          if (shard == 0 && replica == common.getServersConfig().getShards()[0].getMasterReplica()) {
            if (i == shard && j == replica) {
              continue;
            }
            doPushIndexSchema(cobj, i, j);
          }
        }
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private void doPushIndexSchema(ComObject cobj, int i, int j) {
    try {
      getDatabaseClient().send(null, i, j, cobj, DatabaseClient.Replica.SPECIFIED);
    }
    catch (Exception e) {
      logger.error("Error pushing index schema to server: shard={}, replica={}", i, j);
    }
  }

  public void pushSchema() {
    try {
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.DB_NAME, NONE_STR);
      cobj.put(ComObject.Tag.SCHEMA_VERSION, common.getSchemaVersion());
      cobj.put(ComObject.Tag.METHOD, "DatabaseServer:updateSchema");
      cobj.put(ComObject.Tag.SCHEMA_BYTES, common.serializeSchema(DatabaseClient.SERIALIZATION_VERSION));

      for (int i = 0; i < shardCount; i++) {
        for (int j = 0; j < replicationFactor; j++) {
          if (shard == 0 && replica == common.getServersConfig().getShards()[0].getMasterReplica()) {
            if (i == shard && j == replica) {
              continue;
            }
            doPushSchema(cobj, i, j);
          }
        }
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private void doPushSchema(ComObject cobj, int i, int j) {
    try {
      getDatabaseClient().send(null, i, j, cobj, DatabaseClient.Replica.SPECIFIED);
    }
    catch (Exception e) {
      logger.error("Error pushing schema to server: shard={}, replica={}", i, j);
    }
  }


  public ComObject healthCheck(ComObject cobj, boolean replayedCommand) {
    return doHealthCheck();
  }

  private ComObject doHealthCheck() {
    ComObject retObj = new ComObject();
    retObj.put(ComObject.Tag.STATUS, STATUS_OK_STR);
    return retObj;
  }

  public ComObject healthCheckPriority(ComObject cobj, boolean replayedCommand) {
    return doHealthCheck();
  }

  public ComObject updateServersConfig(ComObject cobj, boolean replayedCommand) {
    try {
      short serializationVersion = cobj.getShort(ComObject.Tag.SERIALIZATION_VERSION);
      ServersConfig serversConfig = new ServersConfig(cobj.getByteArray(ComObject.Tag.SERVERS_CONFIG), serializationVersion);

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
          cobj.put(ComObject.Tag.DB_NAME, NONE_STR);
          cobj.put(ComObject.Tag.SCHEMA_VERSION, common.getSchemaVersion());
          cobj.put(ComObject.Tag.METHOD, "DatabaseServer:updateServersConfig");
          cobj.put(ComObject.Tag.SERVERS_CONFIG, common.getServersConfig().serialize(DatabaseClient.SERIALIZATION_VERSION));
          getDatabaseClient().send(null, i, j, cobj, DatabaseClient.Replica.SPECIFIED);
        }
        catch (Exception e) {
          logger.error("Error pushing servers config: shard={}, replica={}", i, j);
        }
      }
    }
  }

  public String getDataDir() {
    return dataDir;
  }

  public void setRole(String role) {
    this.role = DatabaseClient.Replica.PRIMARY;
  }

  public ObjectNode getConfig() {
    return config;
  }

  public DatabaseClient.Replica getRole() {
    return role;
  }

  private boolean shutdown = false;

  public static final long TIME_2017;

  static {
    Calendar cal = new GregorianCalendar();
    cal.set(2017 - 1900, 0, 1, 0, 0, 0);
    TIME_2017 = cal.getTimeInMillis();
  }

  public long getUpdateTime(Object value) {
    try {
      if (value instanceof Long) {
        return addressMap.getUpdateTime((Long) value);
      }
      else {
        return ((AddressMap.IndexValue)value).getUpdateTime();
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  //dynamically invoked
  public ComObject serverSelect(ComObject cobj, boolean restrictToThisServer, StoredProcedureContextImpl procedureContext) {
    return readManager.serverSelect(cobj, restrictToThisServer, procedureContext);
  }

  //dynamically invoked
  public ComObject serverSetSelect(ComObject cobj, boolean restrictToThisServer, StoredProcedureContextImpl procedureContext) {
    return readManager.serverSetSelect(cobj, restrictToThisServer, procedureContext);
  }

  //dynamically invoked
  public ComObject indexLookupExpression(ComObject cobj, StoredProcedureContextImpl context) {
    return readManager.indexLookupExpression(cobj, context);
  }

  //dynamically invoked
  public ComObject indexLookup(ComObject cobj, StoredProcedureContextImpl context) {
    return readManager.indexLookup(cobj, context);
  }

  public AddressMap getAddressMap() {
    return addressMap;
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

  public ComObject prepareToComeAlive(ComObject cobj, boolean replayedCommand) {
    String slicePoint = null;
    try {
      ComObject pcobj = new ComObject();
      pcobj.put(ComObject.Tag.DB_NAME, NONE_STR);
      pcobj.put(ComObject.Tag.SCHEMA_VERSION, common.getSchemaVersion());
      pcobj.put(ComObject.Tag.METHOD, "LogManager:pushMaxSequenceNum");
      getClient().send(null, shard, 0, pcobj, DatabaseClient.Replica.MASTER,
          true);

      if (shard == 0) {
        pcobj = new ComObject();
        pcobj.put(ComObject.Tag.DB_NAME, NONE_STR);
        pcobj.put(ComObject.Tag.SCHEMA_VERSION, common.getSchemaVersion());
        pcobj.put(ComObject.Tag.METHOD, "DatabaseServer:pushMaxRecordId");
        getClient().send(null, shard, 0, pcobj, DatabaseClient.Replica.MASTER,
            true);
      }

      for (int localReplica = 0; localReplica < replicationFactor; localReplica++) {
        if (localReplica != this.replica) {
          logManager.getLogsFromPeer(localReplica);
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

  public ComObject getSchema(ComObject cobj, boolean replayedCommand) {
    short serializationVersionNumber = cobj.getShort(ComObject.Tag.SERIALIZATION_VERSION);
    try {

      if (cobj.getBoolean(ComObject.Tag.FORCE) != null && cobj.getBoolean(ComObject.Tag.FORCE)) {
        final ComObject cobj2 = new ComObject();
        cobj2.put(ComObject.Tag.DB_NAME, NONE_STR);
        cobj2.put(ComObject.Tag.SCHEMA_VERSION, common.getSchemaVersion());
        cobj2.put(ComObject.Tag.METHOD, "DatabaseServer:getSchema");

        doGetSchema(cobj2);
      }
      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.SCHEMA_BYTES, common.serializeSchema(serializationVersionNumber));
      return retObj;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private void doGetSchema(ComObject cobj2) {
    int threadCount = getShardCount() * getReplicationFactor();
    ThreadPoolExecutor localExecutor = new ThreadPoolExecutor(threadCount, threadCount, 10_000, TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
    try {
      logger.info("forcing schema sync: version={}", common.getSchemaVersion());
      List<Future> futures = new ArrayList<>();
      for (int i = 0; i < getShardCount(); i++) {
        for (int j = 0; j < getReplicationFactor(); j++) {
          doGetSchemaForReplica(cobj2, localExecutor, futures, i, j);
        }
      }
      for (Future future : futures) {
        future.get();
      }
      pushSchema();
    }
    catch (Exception e) {
      logger.error("Error pushing schema", e);
    }
    finally {
      localExecutor.shutdownNow();
    }
  }

  private void doGetSchemaForReplica(ComObject cobj2, ThreadPoolExecutor localExecutor, List<Future> futures, int i, int j) {
    try {
      if (common.getServersConfig().getShards()[i].getReplicas()[j].isDead() || i == getShard() && j == getReplica()) {
        return;
      }

      final int localShard = i;
      final int localReplica = j;

      futures.add(localExecutor.submit((Callable) () -> {
        byte[] bytes = getClient().send(null, localShard, localReplica, cobj2, DatabaseClient.Replica.SPECIFIED);
        ComObject retObj = new ComObject(bytes);
        DatabaseCommon tmpCommon = new DatabaseCommon();
        tmpCommon.deserializeSchema(retObj.getByteArray(ComObject.Tag.SCHEMA_BYTES));
        synchronized (common) {
          if (tmpCommon.getSchemaVersion() > common.getSchemaVersion()) {
            logger.info("Found schema with higher version: version=" + tmpCommon.getSchemaVersion() +
                ", currVersion=" + common.getSchemaVersion() + ", shard=" + localShard + REPLICA_STR + localReplica);
            common.deserializeSchema(retObj.getByteArray(ComObject.Tag.SCHEMA_BYTES));
          }
        }
        return null;
      }));
    }
    catch (Exception e) {
      logger.error("Error getting schema: shard={}, replica={}", i, j, e);
    }
  }


  public ComObject getConfig(ComObject cobj, boolean replayedCommand) {
    short serializationVersionNumber = cobj.getShort(ComObject.Tag.SERIALIZATION_VERSION);
    try {
      byte[] bytes = common.serializeConfig(serializationVersionNumber);
      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.CONFIG_BYTES, bytes);
      return retObj;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public ComObject reconfigureCluster(ComObject cobj, boolean replayedCommand) {
    ServersConfig oldConfig = common.getServersConfig();
    try {
      File file = new File(System.getProperty("user.dir"), "config/config-" + getCluster() + ".json");
      if (!file.exists()) {
        file = new File(System.getProperty("user.dir"), "db/src/main/resources/config/config-" + getCluster() + ".json");
      }
      String configStr = IOUtils.toString(new BufferedInputStream(new FileInputStream(file)), "utf-8");
      logger.info("Config: {}", configStr);
      ObjectMapper mapper = new ObjectMapper();
      ObjectNode localConfig = (ObjectNode) mapper.readTree(configStr);

      boolean isInternal = false;
      if (localConfig.has(CLIENT_IS_PRIVATE_STR)) {
        isInternal = localConfig.get(CLIENT_IS_PRIVATE_STR).asBoolean();
      }

      boolean optimizedForThroughput = true;
      if (localConfig.has(OPTIMIZE_READS_FOR_STR)) {
        String text = localConfig.get(OPTIMIZE_READS_FOR_STR).asText();
        if (!text.equalsIgnoreCase("totalThroughput")) {
          optimizedForThroughput = false;
        }
      }

      ServersConfig newConfig = new ServersConfig(cluster, localConfig.withArray(SHARDS_STR),
           isInternal, optimizedForThroughput);

      common.setServersConfig(newConfig);

      common.saveSchema(getDataDir());

      pushSchema();

      ServersConfig.Shard[] oldShards = oldConfig.getShards();
      ServersConfig.Shard[] newShards = newConfig.getShards();

      int count = newShards.length - oldShards.length;
      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.COUNT, count);

      return retObj;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  private final Object nextIdLock = new Object();

  @SchemaReadLock
  public ComObject allocateRecordIds(ComObject cobj, boolean replayedCommand) {
    try {
      String dbName = cobj.getString(ComObject.Tag.DB_NAME);
      logger.info("Requesting next record id - begin");

      long nextId;
      long maxId;
      synchronized (nextIdLock) {
        File file = new File(dataDir, NEXT_RECORD_ID_STR + getShard() + File.separator + getReplica() + NEXT_RECOR_ID_TXT_STR);
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
          if (file.exists()) {
            Files.delete(file.toPath());
          }
        }
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))) {
          writer.write(String.valueOf(maxId));
        }
      }

      pushMaxRecordId(dbName, maxId);

      logger.info("Requesting next record id - finished: nextId={}, maxId={}", nextId, maxId);

      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.NEXT_ID, nextId);
      retObj.put(ComObject.Tag.MAX_ID, maxId);
      return retObj;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public ComObject pushMaxRecordId(ComObject cobj, boolean replayedCommand) {
    try {
      synchronized (nextIdLock) {
        File file = new File(dataDir, NEXT_RECORD_ID_STR + getShard() + File.separator + getReplica() + NEXT_RECOR_ID_TXT_STR);
        file.getParentFile().mkdirs();
        if (file.exists()) {
          try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)))) {
            long maxId = Long.parseLong(reader.readLine());
            pushMaxRecordId(NONE_STR, maxId);
          }
        }
      }
      return null;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  protected void pushMaxRecordId(String dbName, long maxId) {
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, dbName);
    cobj.put(ComObject.Tag.SCHEMA_VERSION, common.getSchemaVersion());
    cobj.put(ComObject.Tag.MAX_ID, maxId);

    for (int localReplica = 0; localReplica < replicationFactor; localReplica++) {
      if (localReplica == this.replica) {
        continue;
      }
      try {
        getDatabaseClient().send("DatabaseServer:setMaxRecordId", 0, localReplica, cobj, DatabaseClient.Replica.SPECIFIED, true);
      }
      catch (Exception e) {
        logger.error("Error pushing maxRecordId: replica=" + localReplica, e);
      }
    }
  }

  public ComObject setMaxRecordId(ComObject cobj, boolean replayedCommand) {
    if (shard == 0 && replica == common.getServersConfig().getShards()[0].getMasterReplica()) {
      return null;
    }
    Long maxId = cobj.getLong(ComObject.Tag.MAX_ID);
    try {
      logger.info("setMaxRecordId - begin");
      synchronized (nextIdLock) {
        File file = new File(dataDir, NEXT_RECORD_ID_STR + getShard() + File.separator + getReplica() + NEXT_RECOR_ID_TXT_STR);
        org.apache.commons.io.FileUtils.forceMkdirParent(file);
        if (file.exists()) {
          Files.delete(file.toPath());
        }

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
  }
}
