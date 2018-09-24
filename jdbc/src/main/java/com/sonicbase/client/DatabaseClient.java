package com.sonicbase.client;

import com.codahale.metrics.MetricRegistry;
import com.sonicbase.common.*;
import com.sonicbase.common.DeadServerException;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.procedure.StoredProcedureContextImpl;
import com.sonicbase.query.*;
import com.sonicbase.query.impl.*;
import com.sonicbase.schema.Schema;
import com.sonicbase.socket.DatabaseSocketClient;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class DatabaseClient {
  public static final short SERIALIZATION_VERSION = 28;
  public static final short SERIALIZATION_VERSION_28 = 28;
  public static final short SERIALIZATION_VERSION_27 = 27;
  public static final short SERIALIZATION_VERSION_26 = 26;
  public static final short SERIALIZATION_VERSION_25 = 25;
  public static final short SERIALIZATION_VERSION_24 = 24;
  public static final short SERIALIZATION_VERSION_23 = 23;
  public static final short SERIALIZATION_VERSION_22 = 22;
  public static final short SERIALIZATION_VERSION_21 = 21;
  public static final short SERIALIZATION_VERSION_20 = 20;
  public static final short SERIALIZATION_VERSION_19 = 19;

  public static final int SELECT_PAGE_SIZE = 1000;
  public static final int OPTIMIZED_RANGE_PAGE_SIZE = 4_000;
  private static final String SHUTTING_DOWN_STR = "Shutting down";
  private static final String NONE_STR = "__none__";
  private static final String DATABASE_SERVER_GET_SCHEMA_STR = "DatabaseServer:getSchema";
  private static final String SCHEMA_OUT_OF_SYNC_EXCEPTION_STR = "SchemaOutOfSyncException";
  private static final String CURR_VER_STR = "currVer:";
  private static final String HOST_STR = "Host=";
  private static final String METHOD_STR = ", method=";
  private static final String EXPLAIN_STR = "explain";

  private final boolean isClient;
  private final int shard;
  private final int replica;
  private static final ConcurrentHashMap<String, Thread> statsRecorderThreads = new ConcurrentHashMap<>();
  private final String allocatedStack;
  private final StatementHandlerFactory statementHandlerFactory;
  private Object databaseServer;
  private Server[][] servers;
  private final AtomicBoolean isShutdown = new AtomicBoolean();
  public static final AtomicInteger clientRefCount = new AtomicInteger();
  public static final Map<String, DatabaseClient> sharedClients = new ConcurrentHashMap<>();
  public static final Queue<DatabaseClient> allClients = new ConcurrentLinkedQueue<>();

  private DatabaseCommon common = new DatabaseCommon();
  private static ThreadPoolExecutor executor = null;

  private static final Logger localLogger = LoggerFactory.getLogger(DatabaseClient.class);
  private static final Logger logger = LoggerFactory.getLogger(DatabaseClient.class);


  private int pageSize = SELECT_PAGE_SIZE;

  private final ThreadLocal<Boolean> isExplicitTrans = new ThreadLocal<>();
  private final ThreadLocal<Boolean> isCommitting = new ThreadLocal<>();
  private final ThreadLocal<Long> transactionId = new ThreadLocal<>();
  private Timer statsTimer;
  private final ConcurrentHashMap<String, StatementCacheEntry> statementCache = new ConcurrentHashMap<>();

  public static final Map<Integer, Map<Integer, Object>> dbservers = new ConcurrentHashMap<>();
  public static final Map<Integer, Map<Integer, Object>> dbdebugServers = new ConcurrentHashMap<>();

  private static final MetricRegistry METRICS = new MetricRegistry();

  private final Object idAllocatorLock = new Object();
  private final AtomicLong nextId = new AtomicLong(-1L);
  private final AtomicLong maxAllocatedId = new AtomicLong(-1L);
  private final DescribeStatementHandler describeHandler = new DescribeStatementHandler(this);
  private static final ConcurrentHashMap<String, String> lowered = new ConcurrentHashMap<>();

  public static final com.codahale.metrics.Timer INDEX_LOOKUP_STATS = METRICS.timer("indexLookup");
  public static final com.codahale.metrics.Timer BATCH_INDEX_LOOKUP_STATS = METRICS.timer("batchIndexLookup");
  public static final com.codahale.metrics.Timer JOIN_EVALUATE = METRICS.timer("joinEvaluate");
  private final Random rand = new Random(System.currentTimeMillis());

  private static final String[] writeVerbsArray = new String[]{
      "SchemaManager:dropTable",
      "SchemaManager:dropIndex",
      "SchemaManager:dropDatabase",
      "SchemaManager:dropColumn",
      "SchemaManager:dropColumnSlave",
      "SchemaManager:addColumn",
      "SchemaManager:addColumnSlave",
      "SchemaManager:dropIndexSlave",
      "SchemaManager:createIndex",
      "SchemaManager:createIndexSlave",
      "SchemaManager:createTable",
      "SchemaManager:createTableSlave",
      "SchemaManager:createDatabase",
      "SchemaManager:createDatabaseSlave",
      "echoWrite",
      "UpdateManager:deleteRecord",
      "UpdateManager:deleteIndexEntryByKey",
      "UpdateManager:deleteIndexEntry",
      "UpdateManager:updateRecord",
      "UpdateManager:populateIndex",
      "UpdateManager:insertIndexEntryByKey",
      "UpdateManager:insertIndexEntryByKeyWithRecord",
      "removeRecord",
      "DatabaseServer:updateServersConfig",
      "UpdateManager:deleteRecord",
      "DatabaseServer:allocateRecordIds",
      "DatabaseServer:setMaxRecordId",
      "DatabaseServer:updateIndexSchema",
      "DatabaseServer:updateSchema",
      "reserveNextId",
      "DatabaseServer:updateSchema",
      "expirePreparedStatement",
      "PartitionManager:rebalanceOrderedIndex",
      "beginRebalanceOrderedIndex",
      "PartitionManager:moveIndexEntries",
      "notifyDeletingComplete",
      "notifyRepartitioningComplete",
      "notifyRepartitioningRecordsByIdComplete",
      "UpdateManager:batchInsertIndexEntryByKeyWithRecord",
      "UpdateManager:batchInsertIndexEntryByKey",
      "moveHashPartition",
      "notifyRepartitioningComplete",
      "UpdateManager:truncateTable",
      "purge",
      "DatabaseServer:reserveNextIdFromReplica",
      "reserveNextId",
      "TransactionManager:abortTransaction",
      "ReadManager:serverSelectDelete",
      "UpdateManager:commit",
      "UpdateManager:rollback",
      "testWrite",
      "saveSchema",
      "MonitorManager:registerStats"
  };

  private static final Set<String> writeVerbs = new HashSet<>();

  private static final String[] parallelVerbsArray = new String[]{
      "UpdateManager:insertIndexEntryByKey",
      "UpdateManager:insertIndexEntryByKeyWithRecord",
      "PartitionManager:moveIndexEntries",
      "UpdateManager:batchInsertIndexEntryByKeyWithRecord",
      "UpdateManager:batchInsertIndexEntryByKey",
      "PartitionManager:moveIndexEntries",
      "PartitionManager:deleteMovedRecords",
      "MonitorManager:registerStats",
  };

  private static final Set<String> parallelVerbs = new HashSet<>();
  private String cluster;
  private ClientStatsHandler clientStatsHandler = new ClientStatsHandler();

  public Server[][] getServersArray() {
    return servers;
  }

  public DatabaseClient(String host, int port, int shard, int replica, boolean isClient) {
    this(null, new String[]{host + ":" + port}, shard, replica, isClient, null, null, false);
  }

  public DatabaseClient(String[] hosts, int shard, int replica, boolean isClient) {
    this(null, hosts, shard, replica, isClient, null, null, false);
  }

  public DatabaseClient(String cluster, String host, int port, int shard, int replica, boolean isClient,
                        DatabaseCommon common, Object databaseServer) {
    this(cluster, new String[]{host + ":" + port}, shard, replica, isClient, common, databaseServer, false);
  }

  public DatabaseClient(String cluster, String[] hosts, int shard, int replica, boolean isClient, DatabaseCommon common,
                        Object databaseServer, boolean isShared) {
    this.cluster = cluster;
    synchronized (DatabaseClient.class) {
      if (executor == null) {
        executor = ThreadUtil.createExecutor(128, "SonicBase Client Thread");
      }
      clientRefCount.incrementAndGet();
    }
    statementHandlerFactory = new StatementHandlerFactory(this);
    allocatedStack = ExceptionUtils.getStackTrace(new Exception());
    allClients.add(this);
    servers = new Server[1][];
    servers[0] = new Server[hosts.length];
    this.shard = shard;
    this.replica = replica;
    this.databaseServer = databaseServer;
    for (int i = 0; i < hosts.length; i++) {
      String[] parts = hosts[i].split(":");
      String host = parts[0];
      int port = Integer.parseInt(parts[1]);
      servers[0][i] = new Server(host, port);
      localLogger.info("Adding startup server: host={}:{}", host, port);
    }
    this.isClient = isClient;
    if (common != null) {
      this.common = common;
    }

    if (shard != 0 && replica != 0) {
      syncConfig();
    }

    configureServers();

    statsTimer = new java.util.Timer();

    if (!isShared) {
      cluster = getCluster();
      synchronized (DatabaseClient.class) {
          DatabaseClient sharedClient = sharedClients.get(cluster);
        if (sharedClient == null) {
          logger.info("Initializing sharedClient: cluster=" + cluster);

          sharedClient = new DatabaseClient(cluster, hosts, shard, replica, isClient, common, databaseServer, true);
          sharedClients.put(cluster, sharedClient);

          Thread statsRecorderThread = ThreadUtil.createThread(new ClientStatsHandler.QueryStatsRecorder(
              sharedClient, cluster), "SonicBase Stats Recorder - cluster=" + cluster);
          statsRecorderThread.start();
          statsRecorderThreads.put(getCluster(), statsRecorderThread);
        }
        else {
          logger.info("Initializing sharedClient - already initialized: cluster=" + cluster);
        }
      }
    }
  }


  public static AtomicInteger getClientRefCount() {
    return clientRefCount;
  }

  public static Map<String, DatabaseClient> getSharedClients() {
    return sharedClients;
  }

  public static Queue<DatabaseClient> getAllClients() {
    return allClients;
  }

  public static Set<String> getWriteVerbs() {
    return writeVerbs;
  }

  public static Set<String> getParallelVerbs() {
    return parallelVerbs;
  }

  static {
    Collections.addAll(writeVerbs, writeVerbsArray);
    parallelVerbs.addAll(Arrays.asList(parallelVerbsArray));
  }

  public int getPageSize() {
    return pageSize;
  }

  public void setPageSize(int pageSize) {
    this.pageSize = pageSize;
  }

  public Schema getSchema(String dbName) {
    return common.getSchema(dbName);
  }

  public DatabaseCommon getCommon() {
    return common;
  }

  public void setCommon(DatabaseCommon common) {
    this.common = common;
  }

  public SelectStatement createSelectStatement() {
    return new SelectStatementImpl(this);
  }

  public UpdateStatement createUpdateStatement() {
    return new UpdateStatementImpl(this);
  }

  public InsertStatement createInsertStatement() {
    return new InsertStatementImpl(this);
  }

  public CreateTableStatement createCreateTableStatement() {
    return new CreateTableStatementImpl(this);
  }

  public CreateIndexStatement createCreateIndexStatement() {
    return new CreateIndexStatementImpl(this);
  }

  public ThreadPoolExecutor getExecutor() {
    return executor;
  }

  public boolean isExplicitTrans() {
    Boolean explicit = isExplicitTrans.get();
    if (explicit == null) {
      isExplicitTrans.set(false);
      return false;
    }
    return explicit;
  }

  public boolean isCommitting() {
    Boolean committing = isCommitting.get();
    if (committing == null) {
      isCommitting.set(false);
      return false;
    }
    return committing;
  }

  public long getTransactionId() {
    Long id = transactionId.get();
    if (id == null) {
      transactionId.set(0L);
      return 0;
    }
    return id;
  }

  public void beginExplicitTransaction(String dbName) {
    if (!common.haveProLicense()) {
      throw new InsufficientLicense("You must have a pro license to use explicit transactions");
    }

    isExplicitTrans.set(true);
    isCommitting.set(false);
    try {
      transactionId.set(allocateId(dbName));
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public void commit(String dbName) {
    isCommitting.set(true);
    int schemaRetryCount = 0;
    while (true) {
      if (isShutdown.get()) {
        throw new DatabaseException(SHUTTING_DOWN_STR);
      }

      try {
        ComObject cobj = new ComObject();
        cobj.put(ComObject.Tag.DB_NAME, dbName);
        if (schemaRetryCount < 2) {
          cobj.put(ComObject.Tag.SCHEMA_VERSION, common.getSchemaVersion());
        }
        cobj.put(ComObject.Tag.TRANSACTION_ID, transactionId.get());
        sendToAllShards("UpdateManager:commit", 0, cobj, DatabaseClient.Replica.DEF);

        isExplicitTrans.set(false);
        isCommitting.set(false);
        transactionId.set(null);

        break;
      }
      catch (Exception e) {
        int index = ExceptionUtils.indexOfThrowable(e, SchemaOutOfSyncException.class);
        if (-1 != index) {
          schemaRetryCount++;
          continue;
        }
        throw new DatabaseException(e);
      }
    }


  }

  public void rollback(String dbName) {

    int schemaRetryCount = 0;
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, dbName);
    if (schemaRetryCount < 2) {
      cobj.put(ComObject.Tag.SCHEMA_VERSION, common.getSchemaVersion());
    }
    cobj.put(ComObject.Tag.TRANSACTION_ID, transactionId.get());
    sendToAllShards("UpdateManager:rollback", 0, cobj, DatabaseClient.Replica.DEF);

    isExplicitTrans.set(false);
    isCommitting.set(false);
    transactionId.set(null);
  }

  public int getReplicaCount() {
    return servers[0].length;
  }

  public int getShardCount() {
    return servers.length;
  }


  public void createDatabase(String dbName) {
    dbName = dbName.toLowerCase();

    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, dbName);
    cobj.put(ComObject.Tag.SCHEMA_VERSION, common.getSchemaVersion());
    cobj.put(ComObject.Tag.METHOD, "SchemaManager:createDatabase");
    cobj.put(ComObject.Tag.MASTER_SLAVE, "master");

    sendToMaster(cobj);
  }

  public void shutdown() {
    if (isShutdown.get()) {
      return;
    }
    isShutdown.set(true);
    if (statsTimer != null) {
      statsTimer.cancel();
    }

    clientStatsHandler.shutdown();

    allClients.remove(this);

    List<DatabaseClient> toShutdown = new ArrayList<>();
    boolean shouldShutdown = false;
    synchronized (DatabaseClient.class) {
      if (clientRefCount.decrementAndGet() == sharedClients.values().size()) {
        shouldShutdown = true;
        toShutdown.addAll(sharedClients.values());
        sharedClients.clear();
      }
    }
    if (shouldShutdown) {
      logger.info("Shutting down shared client");
      for (Thread thread : statsRecorderThreads.values()) {
        thread.interrupt();
        try {
          thread.join();
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new DatabaseException(e);
        }
      }

      statsRecorderThreads.clear();

      for (DatabaseClient client : toShutdown) {
        client.shutdown();
      }
      toShutdown.clear();
      if (executor != null) {
        executor.shutdownNow();
        executor = null;
      }

      DatabaseSocketClient.shutdown();
    }
  }



  public String getCluster() {
    if (this.cluster != null) {
      return this.cluster;
    }
    syncConfig();
    this.cluster = common.getServersConfig().getCluster();
    return this.cluster;
  }

  public ReconfigureResults reconfigureCluster() {

    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, NONE_STR);
    cobj.put(ComObject.Tag.SCHEMA_VERSION, common.getSchemaVersion());
    cobj.put(ComObject.Tag.METHOD, "DatabaseServer:healthCheck");

    try {
      byte[] bytes = sendToMaster(cobj);
      ComObject retObj = new ComObject(bytes);
      if (retObj.getString(ComObject.Tag.STATUS).equals("{\"status\" : \"ok\"}")) {
        ComObject rcobj = new ComObject();
        rcobj.put(ComObject.Tag.DB_NAME, NONE_STR);
        rcobj.put(ComObject.Tag.SCHEMA_VERSION, common.getSchemaVersion());
        rcobj.put(ComObject.Tag.METHOD, "DatabaseServer:reconfigureCluster");
        bytes = sendToMaster(null);
        retObj = new ComObject(bytes);
        int count = retObj.getInt(ComObject.Tag.COUNT);
        return new ReconfigureResults(true, count);
      }
    }
    catch (Exception e) {
      logger.error("Error reconfiguring cluster. Master server not running", e);
    }
    return new ReconfigureResults(false, 0);
  }

  public static String toLower(String value) {
    String lower = lowered.get(value);
    if (lower == null) {
      lower = value.toLowerCase();
      lowered.putIfAbsent(value, lower);
    }
    return lower;
  }

  public Object getDatabaseServer() {
    return databaseServer;
  }

  public void setDatabaseServer(Object databaseServer) {
    this.databaseServer = databaseServer;
  }

  public String getAllocatedStack() {
    return allocatedStack;
  }

  public boolean getShutdown() {
    return isShutdown.get();
  }

  public StatementHandlerFactory getStatementHandlerFactory() {
    return statementHandlerFactory;
  }

  public ClientStatsHandler getClientStatsHandler() {
    return clientStatsHandler;
  }

  public void setIsExplicitTrans(boolean isExplicitTrans) {
    this.isExplicitTrans.set(isExplicitTrans);
  }

  public void setIsCommitting(boolean isCommitting) {
    this.isCommitting.set(isCommitting);
  }

  void setTransactionId(long transactionId) {
    this.transactionId.set(transactionId);
  }

  public void setServers(Server[][] servers) {
    this.servers = servers;
  }

  void setClientStatsHandler(ClientStatsHandler clientStatsHandler) {
    this.clientStatsHandler = clientStatsHandler;
  }

  public static class Server {
    private boolean dead;
    private final String hostPort;
    private final AtomicInteger replicaOffset = new AtomicInteger();
    private final DatabaseSocketClient socketClient = new DatabaseSocketClient();

    public Server(String host, int port) {
      this.hostPort = host + ":" + port;
      this.dead = false;
    }

    public byte[] doSend(String batchKey, ComObject body) {
      return socketClient.doSend(batchKey, body.serialize(), hostPort);
    }

    public byte[] doSend(String batchKey, byte[] body) {
      return socketClient.doSend(batchKey, body, hostPort);
    }

    public boolean isDead() {
      return dead;
    }

    public void setDead(boolean dead) {
      this.dead = dead;
    }
  }

  public void configureServers() {
    ServersConfig serversConfig = common.getServersConfig();

    boolean isPrivate = !isClient || serversConfig.clientIsInternal();

    ServersConfig.Shard[] shards = serversConfig.getShards();

    servers = new Server[shards.length][];
    for (int i = 0; i < servers.length; i++) {
      ServersConfig.Shard localShard = shards[i];
      servers[i] = new Server[localShard.getReplicas().length];
      for (int j = 0; j < servers[i].length; j++) {
        ServersConfig.Host replicaHost = localShard.getReplicas()[j];

        servers[i][j] = new Server(isPrivate ? replicaHost.getPrivateAddress() : replicaHost.getPublicAddress(),
            replicaHost.getPort());
      }
    }
  }

  protected void syncConfig() {
    while (true) {
      if (isShutdown.get()) {
        throw new DatabaseException(SHUTTING_DOWN_STR);
      }

      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.DB_NAME, NONE_STR);
      cobj.put(ComObject.Tag.SCHEMA_VERSION, common.getSchemaVersion());
      try {
        byte[] ret = null;
        int receivedReplica = -1;
        GetConfig getConfig = new GetConfig(cobj, ret, receivedReplica).invoke();
        ret = getConfig.getRet();
        receivedReplica = getConfig.getReceivedReplica();

        processGetConfigRet(ret, receivedReplica);

        if (common.getServersConfig() == null) {
          if (isShutdown.get()) {
            throw new DatabaseException(SHUTTING_DOWN_STR);
          }
          Thread.sleep(1_000);
          continue;
        }
        break;
      }
      catch (Exception t) {
        handleGetConfigException(t);
      }
    }
  }

  private void handleGetConfigException(Exception t) {
    if (isShutdown.get()) {
      throw new DatabaseException(SHUTTING_DOWN_STR);
    }
    logger.error("Error syncing config", t);
    try {
      Thread.sleep(2000);
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new DatabaseException(e);
    }
  }

  private void processGetConfigRet(byte[] ret, int receivedReplica) throws IOException {
    if (ret == null) {
      localLogger.error("Error getting config from any replica");
    }
    else {
      ComObject retObj = new ComObject(ret);
      common.deserializeConfig(retObj.getByteArray(ComObject.Tag.CONFIG_BYTES));
      localLogger.info("Client received config from server: sourceReplica={}, config={}", receivedReplica,
          common.getServersConfig());
    }
  }

  public void initDb() {
    while (true) {
      if (isShutdown.get()) {
        throw new DatabaseException(SHUTTING_DOWN_STR);
      }

      try {
        syncSchema();
        break;
      }
      catch (Exception e) {
        if (isShutdown.get()) {
          throw new DatabaseException(SHUTTING_DOWN_STR);
        }
        logger.error("Error synching schema", e);
        try {
          Thread.sleep(1000);
        }
        catch (InterruptedException e1) {
          Thread.currentThread().interrupt();
          throw new DatabaseException(e1);
        }
      }
    }
  }

  public byte[][] sendToAllShards(
      final String method,
      final long authUser, final ComObject body, final Replica replica) {
    return sendToAllShards(method, authUser, body, replica, false);
  }

  public byte[][] sendToAllShards(
      final String method,
      final long authUser, final ComObject body, final Replica replica, final boolean ignoreDeath) {
    List<Future<byte[]>> futures = new ArrayList<>();
    try {
      final byte[] bodyBytes = body.serialize();
      for (int i = 0; i < servers.length; i++) {
        final int localShard = i;
        futures.add(executor.submit(() -> send(method, localShard, authUser, new ComObject(bodyBytes), replica, ignoreDeath)));
      }
      byte[][] ret = new byte[futures.size()][];
      for (int i = 0; i < futures.size(); i++) {
        ret[i] = futures.get(i).get(120000000, TimeUnit.MILLISECONDS);
      }
      return ret;
    }
    catch (SchemaOutOfSyncException e) {
      throw e;
    }
    catch (Exception e) {
      handleSchemaOutOfSyncException(e);
      throw new DatabaseException(e);
    }
  }

  public byte[] send(String method,
                     int shard, long authUser, ComObject body, Replica replica) {
    return send(method, shard, authUser, body, replica, false);
  }

  public byte[] send(String method,
                     int shard, long authUser, ComObject body, Replica replica, boolean ignoreDeath) {
    return send(method, servers[shard], shard, authUser, body, replica, ignoreDeath);
  }

  public byte[] sendToMaster(String method, ComObject body) {
    if (method != null) {
      body.put(ComObject.Tag.METHOD, method);
    }
    return sendToMaster(body);
  }

  public byte[] sendToMaster(ComObject body) {
    Exception lastException = null;
    for (int j = 0; j < 2; j++) {
      if (isShutdown.get()) {
        throw new DatabaseException(SHUTTING_DOWN_STR);
      }

      int masterReplica = 0;
      if (common.getServersConfig() != null) {
        masterReplica = common.getServersConfig().getShards()[0].getMasterReplica();
      }
      try {
        return send(null, servers[0], 0, masterReplica, body, Replica.SPECIFIED);
      }
      catch (DeadServerException e1) {
        throw e1;
      }
      catch (SchemaOutOfSyncException e) {
        throw e;
      }
      catch (Exception e) {
        lastException = sendToMasterHandleException(masterReplica, e);
      }
      if (j == 1) {
        throw new DatabaseException(lastException);
      }
    }
    return null;
  }

  private Exception sendToMasterHandleException(int masterReplica, Exception e) {
    Exception lastException;
    lastException = e;
    if (getReplicaCount() == 1) {
      throw new DatabaseException(e);
    }
    for (int i = 0; i < getReplicaCount(); i++) {
      if (i == masterReplica) {
        continue;
      }
      if (isShutdown.get()) {
        throw new DatabaseException(SHUTTING_DOWN_STR);
      }

      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.DB_NAME, NONE_STR);
      cobj.put(ComObject.Tag.SCHEMA_VERSION, common.getSchemaVersion());
      cobj.put(ComObject.Tag.METHOD, DATABASE_SERVER_GET_SCHEMA_STR);
      try {

        byte[] ret = send(null, 0, i, cobj, Replica.SPECIFIED);
        if (ret != null) {
          ComObject retObj = new ComObject(ret);
          byte[] bytes = retObj.getByteArray(ComObject.Tag.SCHEMA_BYTES);
          if (bytes != null) {
            common.deserializeSchema(bytes);

            logger.info("Schema received from server: currVer={}", common.getSchemaVersion());
            if (common.getServersConfig().getShards()[0].getMasterReplica() == masterReplica) {
              throw e;
            }
            break;
          }
        }
      }
      catch (Exception t) {
        throw new DatabaseException(t);
      }
    }
    return lastException;
  }

  void handleSchemaOutOfSyncException(Exception e) {
    try {
      boolean schemaOutOfSync = false;
      String msg = null;
      int index = ExceptionUtils.indexOfThrowable(e, SchemaOutOfSyncException.class);
      if (-1 != index) {
        schemaOutOfSync = true;
        msg = ExceptionUtils.getThrowables(e)[index].getMessage();
      }
      else if (e.getMessage() != null && e.getMessage().contains(SCHEMA_OUT_OF_SYNC_EXCEPTION_STR)) {
        schemaOutOfSync = true;
        msg = e.getMessage();
      }
      else {
        GetSchemaOutOfSyncCause getSchemaOutOfSyncCause = new GetSchemaOutOfSyncCause(e, schemaOutOfSync, msg).invoke();
        schemaOutOfSync = getSchemaOutOfSyncCause.isSchemaOutOfSync();
        msg = getSchemaOutOfSyncCause.getMsg();
      }
      if (!schemaOutOfSync) {
        throw e;
      }

      syncSchema(msg);

      throw new SchemaOutOfSyncException();
    }
    catch (SchemaOutOfSyncException e1) {
      throw e1;
    }
    catch (DatabaseException | DeadServerException e2) {
      throw e2;
    }
    catch (Exception e1) {
      throw new DatabaseException(e1);
    }
  }

  private void syncSchema(String msg) {
    synchronized (this) {
      Integer serverVersion = null;
      if (msg != null) {
        int pos = msg.indexOf(CURR_VER_STR);
        if (pos != -1) {
          int pos2 = msg.indexOf(':', pos + CURR_VER_STR.length());
          serverVersion = Integer.valueOf(msg.substring(pos + CURR_VER_STR.length(), pos2));
        }
      }

      if (serverVersion == null || serverVersion > common.getSchemaVersion()) {
        syncSchema(serverVersion);
      }
    }
  }

  public byte[] send(String batchKey, Server[] replicas, int shard, long authUser, ComObject body, Replica replica) {
    return send(batchKey, replicas, shard, authUser, body, replica, false);
  }

  private byte[] sendReplicaAll(String method, ComObject body, int shard, Server[] replicas) {
    byte[] ret = null;
    try {
      for (int i = 0; i < replicas.length; i++) {
        if (isShutdown.get()) {
          throw new DatabaseException(SHUTTING_DOWN_STR);
        }
        Server server = replicas[i];
        if (server.dead) {
          throw new DeadServerException(HOST_STR + server.hostPort + METHOD_STR + method);
        }
        DoSendReplicaAll doSendReplicaAll = new DoSendReplicaAll(body, shard, ret, i, server).invoke();
        ret = doSendReplicaAll.getRet();
      }
      return ret;
    }
    catch (DeadServerException e) {
      throw e;
    }
    catch (Exception e) {
      try {
        handleSchemaOutOfSyncException(e);
      }
      catch (Exception t) {
        throw new DatabaseException(t);
      }
    }
    return null;
  }

  private byte[] sendReplicaMaster(String method, ComObject body, int shard, Server[] replicas, boolean ignoreDeath) {
    int masterReplica = -1;
    while (true) {
      if (isShutdown.get()) {
        throw new DatabaseException(SHUTTING_DOWN_STR);
      }

      masterReplica = common.getServersConfig().getShards()[shard].getMasterReplica();
      if (masterReplica != -1) {
        break;
      }
      syncSchema();
    }

    Server currReplica = replicas[masterReplica];
    try {
      if (!ignoreDeath && currReplica.dead) {
        throw new DeadServerException(HOST_STR + currReplica.hostPort + METHOD_STR + method);
      }
      if (shard == this.shard && masterReplica == this.replica && databaseServer != null) {
        return invokeOnServer(databaseServer, body.serialize(), false, true);
      }
      Object dbServer = getLocalDbServer(shard, masterReplica);
      if (dbServer != null) {
        return invokeOnServer(dbServer, body.serialize(), false, true);
      }
      return currReplica.doSend(null, body);
    }
    catch (Exception e) {
      syncSchema();

      masterReplica = common.getServersConfig().getShards()[shard].getMasterReplica();
      currReplica = replicas[masterReplica];
      try {
        return sendReplicaMasterHandleException(method, body, shard, ignoreDeath, masterReplica, currReplica);
      }
      catch (DeadServerException e1) {
        throw new DatabaseException(e);
      }
      catch (Exception e1) {
        e = new DatabaseException(HOST_STR + currReplica.hostPort + METHOD_STR + method, e1);
        handleSchemaOutOfSyncException(e);
      }
    }
    return null;
  }

  private byte[] sendReplicaMasterHandleException(String method, ComObject body, int shard, boolean ignoreDeath,
                                                  int masterReplica, Server currReplica) {
    if (!ignoreDeath && currReplica.dead) {
      throw new DeadServerException(HOST_STR + currReplica.hostPort + METHOD_STR + method);
    }
    if (shard == this.shard && masterReplica == this.replica && databaseServer != null) {
      return invokeOnServer(databaseServer, body.serialize(), false, true);
    }
    Object dbServer = getLocalDbServer(shard, masterReplica);
    if (dbServer != null) {
      return invokeOnServer(dbServer, body.serialize(), false, true);
    }
    return currReplica.doSend(null, body);
  }

  private byte[] sendReplicaSpecified(String method, ComObject body, int shard, long authUser, Server[] replicas,
                                      boolean ignoreDeath) {
    boolean skip = false;
    if (!ignoreDeath && replicas[(int) authUser].dead && writeVerbs.contains(method)) {
      queueForOtherServer(body, shard, (int) authUser, replicas);
      skip = true;
    }
    if (!skip) {
      try {
        if (shard == this.shard && authUser == this.replica && databaseServer != null) {
          return invokeOnServer(databaseServer, body.serialize(), false, true);
        }
        Object dbServer = getLocalDbServer(shard, (int) authUser);
        if (dbServer != null) {
          return invokeOnServer(dbServer, body.serialize(), false, true);
        }
        return replicas[(int) authUser].doSend(null, body);
      }
      catch (DeadServerException e) {
        throw e;
      }
      catch (Exception e) {
        e = new DatabaseException(HOST_STR + replicas[(int) authUser].hostPort + METHOD_STR + method, e);
        try {
          handleSchemaOutOfSyncException(e);
        }
        catch (Exception t) {
          throw new DatabaseException(t);
        }
      }
    }
    return null;
  }

  private void queueForOtherServer(ComObject body, int shard, int authUser, Server[] replicas) {
    ComObject header = new ComObject();
    header.put(ComObject.Tag.METHOD, body.getString(ComObject.Tag.METHOD));
    header.put(ComObject.Tag.REPLICA, authUser);
    body.put(ComObject.Tag.HEADER, header);

    body.put(ComObject.Tag.METHOD, "queueForOtherServer");

    int masterReplica = common.getServersConfig().getShards()[shard].getMasterReplica();
    if (shard == this.shard && masterReplica == this.replica && databaseServer != null) {
      invokeOnServer(databaseServer, body.serialize(), false, true);
    }
    else {
      Object dbServer = getLocalDbServer(shard, (int) masterReplica);
      if (dbServer != null) {
        invokeOnServer(dbServer, body.serialize(), false, true);
      }
      else {
        replicas[masterReplica].doSend(null, body);
      }
    }
  }

  private byte[] sendReplicaDef(String method, ComObject body, int shard, long authUser, Server[] replicas,
                                boolean ignoreDeath) {
    byte[] ret = null;
    if (writeVerbs.contains(method)) {
      SendToMasterReplica sendToMasterReplica = new SendToMasterReplica(method, body, shard, replicas, ignoreDeath,
          ret).invoke();
      ret = sendToMasterReplica.getRet();
      int masterReplica = sendToMasterReplica.getMasterReplica();

      while (true) {
        if (isShutdown.get()) {
          throw new DatabaseException(SHUTTING_DOWN_STR);
        }

        SendReplicaDefSendToAllReplicas sendReplicaDefSendToAllReplicas = new SendReplicaDefSendToAllReplicas(method,
            body, shard, (int) authUser, replicas, ignoreDeath, masterReplica).invoke();
        if (sendReplicaDefSendToAllReplicas.is()) {
          return ret;
        }
        masterReplica = sendReplicaDefSendToAllReplicas.getMasterReplica();
      }
    }
    else {
      return sendReplicaDefNonWriteVerb(method, body, shard, replicas);
    }
  }

  private byte[] sendReplicaDefNonWriteVerb(String method, ComObject body, int shard, Server[] replicas) {
    Exception lastException = null;
    boolean success = false;
    int offset = servers[shard][0].replicaOffset.incrementAndGet() % replicas.length;
    for (int i = 0; i < 10; i++) {
      if (isShutdown.get()) {
        throw new DatabaseException(SHUTTING_DOWN_STR);
      }
      for (long localRand = offset; localRand < offset + replicas.length; localRand++) {
        if (isShutdown.get()) {
          throw new DatabaseException(SHUTTING_DOWN_STR);
        }
        int replicaOffset = Math.abs((int) (localRand % replicas.length));
        if (!replicas[replicaOffset].dead) {
          try {
            return doSendToReplica(body, shard, replicas, replicaOffset);
          }
          catch (Exception e) {
            lastException = sendReplicaDefHandleException(e);
          }
        }
      }
    }
    sendReplicaDefHandleNonSuccess(method, lastException, success);
    return null;
  }

  private void sendReplicaDefHandleNonSuccess(String method, Exception lastException, boolean success) {
    if (!success) {
      if (lastException != null) {
        throw new DatabaseException("Failed to send to any replica: method=" + method, lastException);
      }
      throw new DatabaseException("Failed to send to any replica: method=" + method);
    }
  }

  private Exception sendReplicaDefHandleException(Exception e) {
    Exception lastException;
    try {
      handleSchemaOutOfSyncException(e);
      lastException = e;
    }
    catch (SchemaOutOfSyncException s) {
      throw s;
    }
    catch (Exception t) {
      lastException = t;
    }
    return lastException;
  }

  private byte[] doSendToReplica(ComObject body, int shard, Server[] replicas, int replicaOffset) {
    if (shard == this.shard && replicaOffset == this.replica && databaseServer != null) {
      return invokeOnServer(databaseServer, body.serialize(), false, true);
    }
    Object dbServer = getLocalDbServer(shard, replicaOffset);
    if (dbServer != null) {
      return invokeOnServer(dbServer, body.serialize(), false, true);
    }
    else {
      return replicas[replicaOffset].doSend(null, body);
    }
  }


  public byte[] send(
      String methodStr, Server[] replicas, int shard, long authUser,
      ComObject body, Replica replica, boolean ignoreDeath) {
    try {
      body = prepareCobj(methodStr, body);
      String method = body.getString(ComObject.Tag.METHOD);

      if (isShutdown.get()) {
        throw new DatabaseException(SHUTTING_DOWN_STR);
      }
      return doSend(replicas, shard, authUser, body, replica, ignoreDeath, method);
    }
    catch (SchemaOutOfSyncException | DeadServerException e) {
      throw e;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private ComObject prepareCobj(String methodStr, ComObject body) {
    if (methodStr != null) {
      body.put(ComObject.Tag.METHOD, methodStr);
    }
    if (body == null) {
      body = new ComObject();
    }
    return body;
  }

  private byte[] doSend(Server[] replicas, int shard, long authUser, ComObject body, Replica replica, boolean ignoreDeath,
                        String method) {
    if (replica == Replica.ALL) {
      return sendReplicaAll(method, body, shard, replicas);
    }
    else if (replica == Replica.MASTER) {
      return sendReplicaMaster(method, body, shard, replicas, ignoreDeath);
    }
    else if (replica == Replica.SPECIFIED) {
      return sendReplicaSpecified(method, body, shard, authUser, replicas, ignoreDeath);
    }
    else if (replica == Replica.DEF) {
      return sendReplicaDef(method, body, shard, authUser, replicas, ignoreDeath);
    }
    return null;
  }

  public byte[] doSendOnSocket(List<DatabaseSocketClient.Request> requests) {
    byte[] ret;
    ret = DatabaseSocketClient.doSend(requests);
    return ret;
  }

  private byte[] invokeOnServer(Object dbServer, byte[] body, boolean replayedCommand, boolean enableQueuing) {
    try {
      return DatabaseServerProxy.invokeMethod(dbServer, body, replayedCommand, enableQueuing);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private Object getLocalDbServer(int shard, int replica) {
    Map<Integer, Map<Integer, Object>> dbServers = DatabaseClient.getServers();
    Object dbserver = null;
    if (dbServers != null && dbServers.get(shard) != null) {
      dbserver = dbServers.get(shard).get(replica);
    }
    return dbserver;
  }

  private int selectShard(long objectId) {
    return (int) Math.abs((objectId % servers.length));
  }

  public enum Replica {
    PRIMARY,
    SECONDARY,
    ALL,
    DEF,
    SPECIFIED,
    MASTER
  }

  public boolean isBackupComplete() {
    try {
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.DB_NAME, NONE_STR);
      cobj.put(ComObject.Tag.SCHEMA_VERSION, common.getSchemaVersion());
      byte[] ret = send("BackupManager:isEntireBackupComplete", 0, 0, cobj, DatabaseClient.Replica.MASTER);
      ComObject retObj = new ComObject(ret);
      return retObj.getBoolean(ComObject.Tag.IS_COMPLETE);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public boolean isRestoreComplete() {
    try {
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.DB_NAME, NONE_STR);
      cobj.put(ComObject.Tag.SCHEMA_VERSION, common.getSchemaVersion());
      byte[] ret = send("BackupManager:isEntireRestoreComplete", 0, 0, cobj, DatabaseClient.Replica.MASTER);
      ComObject retObj = new ComObject(ret);
      return retObj.getBoolean(ComObject.Tag.IS_COMPLETE);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public void startRestore(String subDir) {
    try {
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.DB_NAME, NONE_STR);
      cobj.put(ComObject.Tag.SCHEMA_VERSION, common.getSchemaVersion());
      cobj.put(ComObject.Tag.DIRECTORY, subDir);
      send("BackupManager:startRestore", 0, 0, cobj, DatabaseClient.Replica.MASTER);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public void startBackup() {
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, NONE_STR);
    cobj.put(ComObject.Tag.SCHEMA_VERSION, common.getSchemaVersion());
    send("BackupManager:startBackup", 0, 0, cobj, DatabaseClient.Replica.MASTER);
  }


  private static class StatementCacheEntry {
    private final AtomicLong whenUsed = new AtomicLong();
    private Statement statement;

  }

  public Object executeQuery(String dbName, String sql, ParameterHandler parms, boolean restrictToThisServer,
                             StoredProcedureContextImpl procedureContext, boolean disableStats) throws SQLException {
    return executeQuery(dbName, sql, parms, null, null, null,
        restrictToThisServer, procedureContext, disableStats);
  }

  public Object executeQuery(String dbName, String sql, ParameterHandler parms,
                             Long sequence0, Long sequence1, Short sequence2, boolean restrictToThisServer,
                             StoredProcedureContextImpl procedureContext, boolean disableStats) throws SQLException {
    int schemaRetryCount = 0;
    while (true) {
      if (isShutdown.get()) {
        throw new DatabaseException(SHUTTING_DOWN_STR);
      }

      try {
        sql = sql.trim();

        checkParms(dbName);

        Statement statement;
        sql = convertCallToProcedure(sql);

        if (toLower(sql.substring(0, "describe".length())).startsWith("describe")) {
          return describeHandler.doDescribe(dbName, sql);
        }
        else if (toLower(sql.substring(0, EXPLAIN_STR.length())).startsWith(EXPLAIN_STR)) {
          return doExplain(dbName, sql, parms);
        }
        else {
          statement = cacheQuery(sql);
          String sqlToUse = prepareSQL(sql, statement);

          return doExecuteQuery(dbName, parms, sequence0, sequence1, sequence2, restrictToThisServer, procedureContext,
              disableStats, schemaRetryCount, statement, sqlToUse);
        }
      }
      catch (Exception e) {
        int index = ExceptionUtils.indexOfThrowable(e, SchemaOutOfSyncException.class);
        if (-1 != index) {
          schemaRetryCount++;
          continue;
        }
        logger.error("Error processing request", e);
        throw new SQLException(e);
      }
    }
  }

  private String convertCallToProcedure(String sql) {
    if (toLower(sql.toLowerCase().substring(0, "call".length())).startsWith("call")) {
      int pos = sql.toLowerCase().indexOf("procedure");
      sql = sql.substring(pos + "procedure".length()).trim();
      if (sql.charAt(0) == '(') {
        sql = sql.substring(1);
        sql = sql.substring(0, sql.lastIndexOf(')'));
      }
      sql = "execute procedure " + sql;
    }
    return sql;
  }

  private void checkParms(String dbName) {
    if (common == null) {
      throw new DatabaseException("null common");
    }
    if (dbName != null && (common.getDatabases() == null || !common.getDatabases().containsKey(dbName))) {
      syncSchema();
      if (!common.getDatabases().containsKey(dbName)) {
        throw new DatabaseException("Database does not exist: dbName=" + dbName);
      }
    }
  }

  private String prepareSQL(String sql, Statement statement) {
    String sqlToUse = sql;
    if (statement instanceof Select) {
      SelectBody body = ((Select) statement).getSelectBody();
      if (body instanceof PlainSelect) {
        Limit limit = ((PlainSelect) body).getLimit();
        Offset offset = ((PlainSelect) body).getOffset();
        sqlToUse = removeOffsetAndLimit(sql, limit, offset);
      }
      else if (body instanceof SetOperationList) {
        Limit limit = ((SetOperationList) body).getLimit();
        Offset offset = ((SetOperationList) body).getOffset();
        sqlToUse = removeOffsetAndLimit(sql, limit, offset);
      }
      else {
        throw new DatabaseException("Unexpected query type: type=" + body.getClass().getName());
      }
    }
    return sqlToUse;
  }

  private Object doExecuteQuery(String dbName, ParameterHandler parms, Long sequence0, Long sequence1, Short sequence2,
                                boolean restrictToThisServer, StoredProcedureContextImpl procedureContext,
                                boolean disableStats, int schemaRetryCount, Statement statement,
                                String sqlToUse) throws SQLException {
    ClientStatsHandler.HistogramEntry histogramEntry = disableStats ? null :
        clientStatsHandler.registerQueryForStats(getCluster(), dbName, sqlToUse);
    long beginNanos = System.nanoTime();
    boolean success = false;
    try {
      Object ret = null;
      StatementHandler handler = getHandler(statement);
      if (handler != null) {
        ret = handler.execute(dbName, parms, sqlToUse, statement, null, sequence0, sequence1, sequence2,
            restrictToThisServer, procedureContext, schemaRetryCount);
      }
      else {
        throw new DatabaseException("UnhandledStatement: class=" + statement.getClass().getName());
      }
      success = true;
      return ret;
    }
    finally {
      if (!disableStats && success && histogramEntry != null) {
        clientStatsHandler.registerCompletedQueryForStats( histogramEntry, beginNanos);
      }
    }
  }

  private Statement cacheQuery(String sql) throws JSQLParserException {
    Statement statement;
    StatementCacheEntry entry = statementCache.get(sql);
    if (entry == null) {
      CCJSqlParserManager parser = new CCJSqlParserManager();
      statement = parser.parse(new StringReader(sql));
      entry = new StatementCacheEntry();
      entry.statement = statement;
      entry.whenUsed.set(System.currentTimeMillis());
      synchronized (statementCache) {
        if (statementCache.size() > 10000) {
          Long lowestDate = null;
          String lowestKey = null;
          for (Map.Entry<String, StatementCacheEntry> currEntry : statementCache.entrySet()) {
            if (lowestDate == null || currEntry.getValue().whenUsed.get() < lowestDate) {
              lowestDate = currEntry.getValue().whenUsed.get();
              lowestKey = currEntry.getKey();
            }
          }
          if (lowestKey != null) {
            statementCache.remove(lowestKey);
          }
        }
      }
      statementCache.put(sql, entry);
    }
    else {
      statement = entry.statement;
      entry.whenUsed.set(System.currentTimeMillis());
    }
    return statement;
  }

  public StatementHandler getHandler(Statement statement) {
    return statementHandlerFactory.getHandler(statement);
  }

  public static String removeOffsetAndLimit(String sql, Limit limit, Offset offset) {
    String sqlToUse = sql;
    String lowerSql = sql.toLowerCase();
    if (limit != null) {
      int limitPos = lowerSql.lastIndexOf("limit");
      int limitValuePos = lowerSql.indexOf(String.valueOf(limit.getRowCount()), limitPos);
      int endLimitValuePos = lowerSql.indexOf(' ', limitValuePos);

      sqlToUse = sql.substring(0, limitValuePos);
      sqlToUse += "x";
      if (endLimitValuePos != -1 && offset == null) {
        sqlToUse += sql.substring(endLimitValuePos);
      }
    }
    if (offset != null) {
      int offsetPos = lowerSql.lastIndexOf("offset");
      int offsetValuePos = lowerSql.indexOf(String.valueOf(offset.getOffset()), offsetPos);
      int endOffsetValuePos = lowerSql.indexOf(' ', offsetValuePos);

      if (limit == null) {
        sqlToUse = sql.substring(0, offsetValuePos);
      }
      else {
        sqlToUse += " offset ";
      }
      sqlToUse += "x";
      if (endOffsetValuePos != -1) {
        sqlToUse += sql.substring(endOffsetValuePos);
      }
    }
    return sqlToUse;
  }

  private Object doExplain(String dbName, String sql, ParameterHandler parms) {

    try {
      sql = sql.trim().substring(EXPLAIN_STR.length()).trim();
      String[] parts = sql.split(" ");
      if (!parts[0].trim().equalsIgnoreCase("select")) {
        throw new DatabaseException("Verb not supported: verb=" + parts[0].trim());
      }

      CCJSqlParserManager parser = new CCJSqlParserManager();
      Statement statement = parser.parse(new StringReader(sql));
      SelectStatementImpl.Explain explain = new SelectStatementImpl.Explain();
      StatementHandler handler = getHandler(statement);
      return handler.execute(dbName, parms, null, (Select) statement, explain, null,
          null, null, false, null, 0);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }


  public static void populateOrderedKeyInfo(
      Map<String, ConcurrentSkipListMap<Object[], InsertStatementHandler.KeyInfo>> orderedKeyInfos,
      List<InsertStatementHandler.KeyInfo> keys) {
    for (final InsertStatementHandler.KeyInfo keyInfo : keys) {
      ConcurrentSkipListMap<Object[], InsertStatementHandler.KeyInfo> indexMap =
          orderedKeyInfos.get(keyInfo.getIndexSchema().getName());
      if (indexMap == null) {
        indexMap = new ConcurrentSkipListMap<>((o1, o2) -> getComparator(keyInfo, o1, o2));
        orderedKeyInfos.put(keyInfo.getIndexSchema().getName(), indexMap);
      }
      indexMap.put(keyInfo.getKey(), keyInfo);
    }
  }

  private static int getComparator(InsertStatementHandler.KeyInfo keyInfo, Object[] o1, Object[] o2) {
    for (int i = 0; i < o1.length; i++) {
      int value = keyInfo.getIndexSchema().getComparators()[i].compare(o1[i], o2[i]);
      if (value < 0) {
        return -1;
      }
      if (value > 0) {
        return 1;
      }
    }
    return 0;
  }

  public long allocateId(String dbName) {
    long id = -1;
    synchronized (idAllocatorLock) {
      if (nextId.get() != -1 && nextId.get() <= maxAllocatedId.get()) {
        id = nextId.getAndIncrement();
      }
      else {
        ComObject cobj = new ComObject();
        cobj.put(ComObject.Tag.DB_NAME, dbName);
        cobj.put(ComObject.Tag.SCHEMA_VERSION, common.getSchemaVersion());
        byte[] ret = sendToMaster("DatabaseServer:allocateRecordIds", cobj);
        ComObject retObj = new ComObject(ret);
        nextId.set(retObj.getLong(ComObject.Tag.NEXT_ID));
        maxAllocatedId.set(retObj.getLong(ComObject.Tag.MAX_ID));
        id = nextId.getAndIncrement();
      }
    }
    return id;
  }

  public static Map<Integer, Map<Integer, Object>> getServers() {
    return dbservers;
  }

  public static Map<Integer, Map<Integer, Object>> getDebugServers() {
    return dbdebugServers;
  }

  public boolean isRepartitioningComplete(String dbName) {
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, dbName);
    cobj.put(ComObject.Tag.SCHEMA_VERSION, common.getSchemaVersion());
    byte[] bytes = sendToMaster("PartitionManager:isRepartitioningComplete", cobj);
    ComObject retObj = new ComObject(bytes);
    return retObj.getBoolean(ComObject.Tag.FINISHED);
  }

  public long getPartitionSize(String dbName, int shard, String tableName, String indexName) {
    return getPartitionSize(dbName, shard, 0, tableName, indexName);
  }

  public long getPartitionSize(String dbName, int shard, int replica, String tableName, String indexName) {
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, dbName);
    cobj.put(ComObject.Tag.SCHEMA_VERSION, common.getSchemaVersion());
    cobj.put(ComObject.Tag.TABLE_NAME, tableName);
    cobj.put(ComObject.Tag.INDEX_NAME, indexName);
    byte[] bytes = send("PartitionManager:getPartitionSize", shard, replica, cobj, DatabaseClient.Replica.SPECIFIED);
    ComObject retObj = new ComObject(bytes);
    ComArray array = retObj.getArray(ComObject.Tag.SIZES);
    return ((ComObject)array.getArray().get(shard)).getLong(ComObject.Tag.SIZE);
  }

  public void syncSchema(Integer serverVersion) {
    synchronized (common) {
      if (serverVersion == null) {
        syncSchema();
      }
      else {
        if (serverVersion > common.getSchemaVersion()) {
          doSyncSchema();
        }
      }
    }
  }

  public void syncSchema() {
    doSyncSchema();
  }

  void doSyncSchema() {
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, NONE_STR);
    cobj.put(ComObject.Tag.SCHEMA_VERSION, common.getSchemaVersion());
    byte[] ret = getSchemaFromMaster(cobj);
    if (ret == null) {
      ret = getSchemaFromAReplica(cobj, ret);
    }
    if (ret == null) {
      logger.error("Error getting schema from any replica");
    }
    synchronized (common) {
      try {

        if (ret != null) {
          ComObject retObj = new ComObject(ret);
          common.deserializeSchema(retObj.getByteArray(ComObject.Tag.SCHEMA_BYTES));

          ServersConfig serversConfig = common.getServersConfig();
          for (int i = 0; i < serversConfig.getShards().length; i++) {
            for (int j = 0; j < serversConfig.getShards()[0].getReplicas().length; j++) {
              servers[i][j].dead = serversConfig.getShards()[i].getReplicas()[j].isDead();
            }
          }

          logger.info(" Schema received from server: currVer={}", common.getSchemaVersion());
        }
      }
      catch (Exception t) {
        throw new DatabaseException(t);
      }
    }
  }

  private byte[] getSchemaFromMaster(ComObject cobj) {
    byte[] ret = null;
    try {
      ret = sendToMaster(DATABASE_SERVER_GET_SCHEMA_STR, cobj);
    }
    catch (Exception e) {
      logger.error("Error getting schema from master", e);
    }
    return ret;
  }

  private byte[] getSchemaFromAReplica(ComObject cobj, byte[] ret) {
    int masterReplica = common.getServersConfig().getShards()[0].getMasterReplica();
    for (int localReplica = 0; localReplica < getReplicaCount(); localReplica++) {
      if (isShutdown.get()) {
        throw new DatabaseException(SHUTTING_DOWN_STR);
      }

      if (localReplica == masterReplica) {
        continue;
      }
      if (common.getServersConfig().getShards()[0].getReplicas()[localReplica].isDead()) {
        continue;
      }
      try {
        ret = send(DATABASE_SERVER_GET_SCHEMA_STR, 0, localReplica, cobj, Replica.SPECIFIED);
        break;
      }
      catch (Exception e) {
        logger.error("Error getting schema from replica: replica=" + localReplica, e);
      }
    }
    return ret;
  }

  public void getConfig() {
    try {
      long authUser = rand.nextLong();
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.DB_NAME, NONE_STR);
      cobj.put(ComObject.Tag.SCHEMA_VERSION, common.getSchemaVersion());

      byte[] ret = send("DatabaseServer:getConfig", selectShard(0), authUser, cobj, DatabaseClient.Replica.DEF);
      ComObject retObj = new ComObject(ret);
      common.deserializeConfig(retObj.getByteArray(ComObject.Tag.CONFIG_BYTES));
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public void beginRebalance(String dbName) {
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, dbName);
    cobj.put(ComObject.Tag.SCHEMA_VERSION, common.getSchemaVersion());
    cobj.put(ComObject.Tag.FORCE, false);
    sendToMaster("PartitionManager:beginRebalance", cobj);
  }

  private class HandleDeadOnWriteVerb {
    private boolean myResult;
    private final String method;
    private final ComObject body;
    private final int shard;
    private final int authUser;
    private final Server[] replicas;
    private int masterReplica;
    private boolean skip;

    HandleDeadOnWriteVerb(String method, ComObject body, int shard, int authUser, Server[] replicas,
                          int masterReplica, boolean skip) {
      this.method = method;
      this.body = body;
      this.shard = shard;
      this.authUser = authUser;
      this.replicas = replicas;
      this.masterReplica = masterReplica;
      this.skip = skip;
    }

    boolean hadError() {
      return myResult;
    }

    public int getMasterReplica() {
      return masterReplica;
    }

    boolean isSkip() {
      return skip;
    }

    public HandleDeadOnWriteVerb invoke() {
      try {
        if (writeVerbs.contains(method)) {
          ComObject header = new ComObject();
          header.put(ComObject.Tag.METHOD, body.getString(ComObject.Tag.METHOD));
          header.put(ComObject.Tag.REPLICA, authUser);
          body.put(ComObject.Tag.HEADER, header);

          body.put(ComObject.Tag.METHOD, "queueForOtherServer");

          masterReplica = common.getServersConfig().getShards()[shard].getMasterReplica();
          if (shard == DatabaseClient.this.shard && masterReplica == DatabaseClient.this.replica && databaseServer != null) {
            invokeOnServer(databaseServer, body.serialize(), false, true);
          }
          else {
            Object dbServer = getLocalDbServer(shard, masterReplica);
            if (dbServer != null) {
              invokeOnServer(dbServer, body.serialize(), false, true);
            }
            else {
              replicas[masterReplica].doSend(null, body.serialize());
            }
          }
          skip = true;
        }
      }
      catch (Exception e) {
        if (e.getMessage().contains(SCHEMA_OUT_OF_SYNC_EXCEPTION_STR)) {
          syncSchema();
          body.put(ComObject.Tag.SCHEMA_VERSION, common.getSchemaVersion());
          myResult = true;
          return this;
        }
        throw e;
      }
      myResult = false;
      return this;
    }
  }

  private class SendWriteVerbToOtherReplica {
    private boolean myResult;
    private final ComObject body;
    private final int shard;
    private final Server currReplica;
    private final int i;
    private boolean dead;

    SendWriteVerbToOtherReplica(ComObject body, int shard, Server currReplica, int i) {
      this.body = body;
      this.shard = shard;
      this.currReplica = currReplica;
      this.i = i;
    }

    boolean hadError() {
      return myResult;
    }

    public boolean isDead() {
      return dead;
    }

    public SendWriteVerbToOtherReplica invoke() {
      try {
        if (shard == DatabaseClient.this.shard && i == DatabaseClient.this.replica && databaseServer != null) {
          invokeOnServer(databaseServer, body.serialize(), false, true);
        }
        else {
          Object dbServer = getLocalDbServer(shard, i);
          if (dbServer != null) {
            invokeOnServer(dbServer, body.serialize(), false, true);
          }
          else {
            currReplica.doSend(null, body.serialize());
          }
        }
      }
      catch (Exception e) {
        if (-1 != ExceptionUtils.indexOfThrowable(e, DeadServerException.class)) {
          throw new DeadServerException(e);
        }
        try {
          handleSchemaOutOfSyncException(e);
        }
        catch (SchemaOutOfSyncException e1) {
          body.put(ComObject.Tag.SCHEMA_VERSION, common.getSchemaVersion());
          myResult = true;
          return this;
        }
        throw e;
      }
      myResult = false;
      return this;
    }
  }

  private class SendToMasterReplica {
    private final String method;
    private final ComObject body;
    private final int shard;
    private final Server[] replicas;
    private final boolean ignoreDeath;
    private byte[] ret;
    private int masterReplica;

    SendToMasterReplica(String method, ComObject body, int shard, Server[] replicas, boolean ignoreDeath, byte... ret) {
      this.method = method;
      this.body = body;
      this.shard = shard;
      this.replicas = replicas;
      this.ignoreDeath = ignoreDeath;
      this.ret = ret;
    }

    public byte[] getRet() {
      return ret;
    }

    public int getMasterReplica() {
      return masterReplica;
    }

    public SendToMasterReplica invoke() {
      masterReplica = -1;
      for (int i = 0; i < 10; i++) {
        if (isShutdown.get()) {
          throw new DatabaseException(SHUTTING_DOWN_STR);
        }
        masterReplica = common.getServersConfig().getShards()[shard].getMasterReplica();
        Server currReplica = replicas[masterReplica];
        try {
          doSendToMaster(currReplica);

          body.remove(ComObject.Tag.REPLICATION_MASTER);

          if (ret != null) {
            ComObject retObj = new ComObject(ret);
            body.put(ComObject.Tag.SEQUENCE_0, retObj.getLong(ComObject.Tag.SEQUENCE_0));
            body.put(ComObject.Tag.SEQUENCE_1, retObj.getLong(ComObject.Tag.SEQUENCE_1));
          }
          break;
        }
        catch (SchemaOutOfSyncException e) {
          throw e;
        }
        catch (DeadServerException e) {
          handleDeadServer();
        }
        catch (Exception e) {
          e = new DatabaseException(HOST_STR + currReplica.hostPort + METHOD_STR + method, e);
          handleSchemaOutOfSyncException(e);
        }
      }
      return this;
    }

    private void handleDeadServer() {
      if (isShutdown.get()) {
        throw new DatabaseException(SHUTTING_DOWN_STR);
      }
      ThreadUtil.sleep(1_000);
      try {
        syncSchema();
      }
      catch (Exception e1) {
        logger.error("Error syncing schema", e1);
      }
    }

    private void doSendToMaster(Server currReplica) {
      if (!ignoreDeath && replicas[masterReplica].dead) {
        logger.error("dead server: master={}", masterReplica);
        throw new DeadServerException(HOST_STR + currReplica.hostPort + METHOD_STR + method);
      }
      body.put(ComObject.Tag.REPLICATION_MASTER, masterReplica);
      if (shard == DatabaseClient.this.shard && masterReplica == DatabaseClient.this.replica && databaseServer != null) {
        ret = invokeOnServer(databaseServer, body.serialize(), false, true);
      }
      else {
        Object dbServer = getLocalDbServer(shard, (int) masterReplica);
        if (dbServer != null) {
          ret = invokeOnServer(dbServer, body.serialize(), false, true);
        }
        else {
          ret = currReplica.doSend(null, body.serialize());
        }
      }
    }
  }

  private class GetConfig {
    private final ComObject cobj;
    private byte[] ret;
    private int receivedReplica;

    GetConfig(ComObject cobj, byte[] ret, int receivedReplica) {
      this.cobj = cobj;
      this.ret = ret;
      this.receivedReplica = receivedReplica;
    }

    public byte[] getRet() {
      return ret;
    }

    int getReceivedReplica() {
      return receivedReplica;
    }

    public GetConfig invoke() {
      try {
        ret = send("DatabaseServer:getConfig", 0, 0, cobj, Replica.SPECIFIED);
        receivedReplica = 0;
      }
      catch (Exception e) {
        localLogger.error("Error getting config from master", e);
      }
      if (ret == null) {
        for (int localReplica = 1; localReplica < getReplicaCount(); localReplica++) {
          if (isShutdown.get()) {
            throw new DatabaseException(SHUTTING_DOWN_STR);
          }
          try {
            ret = send("DatabaseServer:getConfig", 0, localReplica, cobj, Replica.SPECIFIED);
            receivedReplica = localReplica;
            break;
          }
          catch (Exception e) {
            localLogger.error("Error getting config from replica: replica=" + localReplica, e);
          }
        }
      }
      return this;
    }
  }

  private class GetSchemaOutOfSyncCause {
    private final Exception e;
    private boolean schemaOutOfSync;
    private String msg;

    GetSchemaOutOfSyncCause(Exception e, boolean schemaOutOfSync, String msg) {
      this.e = e;
      this.schemaOutOfSync = schemaOutOfSync;
      this.msg = msg;
    }

    boolean isSchemaOutOfSync() {
      return schemaOutOfSync;
    }

    public String getMsg() {
      return msg;
    }

    public GetSchemaOutOfSyncCause invoke() {
      Throwable t = e;
      while (true) {
        if (isShutdown.get()) {
          throw new DatabaseException(SHUTTING_DOWN_STR);
        }

        t = t.getCause();
        if (t == null) {
          break;
        }
        if (t.getMessage() != null && t.getMessage().contains(SCHEMA_OUT_OF_SYNC_EXCEPTION_STR)) {
          schemaOutOfSync = true;
          msg = t.getMessage();
        }
      }
      return this;
    }
  }

  private class DoSendReplicaAll {
    private final ComObject body;
    private final int shard;
    private byte[] ret;
    private final int i;
    private final Server server;

    DoSendReplicaAll(ComObject body, int shard, byte[] ret, int i, Server server) {
      this.body = body;
      this.shard = shard;
      this.ret = ret;
      this.i = i;
      this.server = server;
    }

    public byte[] getRet() {
      return ret;
    }

    public DoSendReplicaAll invoke() {

      if (shard == DatabaseClient.this.shard && i == DatabaseClient.this.replica && databaseServer != null) {
        ret = invokeOnServer(databaseServer, body.serialize(), false, true);
      }
      else {
        Object dbServer = getLocalDbServer(shard, i);
        if (dbServer != null) {
          ret = invokeOnServer(dbServer, body.serialize(), false, true);
        }
        else {
          ret = server.doSend(null, body);
        }
      }
      return this;
    }
  }

  private class SendReplicaDefSendToAllReplicas {
    private boolean myResult;
    private final String method;
    private final ComObject body;
    private final int shard;
    private final int authUser;
    private final Server[] replicas;
    private final boolean ignoreDeath;
    private int masterReplica;

    SendReplicaDefSendToAllReplicas(String method, ComObject body, int shard, int authUser, Server[] replicas,
                                    boolean ignoreDeath, int masterReplica) {
      this.method = method;
      this.body = body;
      this.shard = shard;
      this.authUser = authUser;
      this.replicas = replicas;
      this.ignoreDeath = ignoreDeath;
      this.masterReplica = masterReplica;
    }

    boolean is() {
      return myResult;
    }

    public int getMasterReplica() {
      return masterReplica;
    }

    public SendReplicaDefSendToAllReplicas invoke() {
      Server currReplica = null;
      try {
        for (int i = 0; i < getReplicaCount(); i++) {
          if (i == masterReplica) {
            continue;
          }
          if (isShutdown.get()) {
            throw new DatabaseException(SHUTTING_DOWN_STR);
          }
          currReplica = replicas[i];
          boolean dead = currReplica.dead;
          boolean failed = false;
          for (int j = 0; j < 10; j++) {
            if (isShutdown.get()) {
              throw new DatabaseException(SHUTTING_DOWN_STR);
            }

            if (doSendToReplica(currReplica, i, dead)) {
              failed = true;
              continue;
            }
            failed = false;
            break;
          }
          if (failed) {
            throw new DatabaseException("Error sending message to replica: replica=" + currReplica);
          }
        }
        myResult = true;
        return this;
      }
      catch (SchemaOutOfSyncException e) {
        throw e;
      }
      catch (DeadServerException e) {
        handleDeadServer();
      }
      catch (Exception e) {
        e = new DatabaseException(HOST_STR + currReplica.hostPort + METHOD_STR + method, e);
        handleSchemaOutOfSyncException(e);
      }
      myResult = false;
      return this;
    }

    private void handleDeadServer() {
      if (isShutdown.get()) {
        throw new DatabaseException(SHUTTING_DOWN_STR);
      }
      ThreadUtil.sleep(1_000);
      try {
        syncSchema();
      }
      catch (Exception e1) {
        logger.error("Error syncing schema", e1);
      }
    }

    private boolean doSendToReplica(Server currReplica, int i, boolean dead) {
      if (isShutdown.get()) {
        throw new DatabaseException(SHUTTING_DOWN_STR);
      }

      boolean skip = false;
      if (!ignoreDeath && dead) {
        HandleDeadOnWriteVerb handleDeadOnWriteVerb = new HandleDeadOnWriteVerb(method, body, shard,
            authUser, replicas, masterReplica, skip).invoke();
        if (handleDeadOnWriteVerb.hadError()) {
          return true;
        }
        masterReplica = handleDeadOnWriteVerb.getMasterReplica();
        skip = handleDeadOnWriteVerb.isSkip();
      }
      if (!skip) {
        SendWriteVerbToOtherReplica sendWriteVerbToOtherReplica = new SendWriteVerbToOtherReplica(body, shard,
            currReplica, i).invoke();
        return sendWriteVerbToOtherReplica.hadError();
      }
      return false;
    }
  }
}
