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
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringReader;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * User: lowryda
 * Date: 1/3/14
 * Time: 7:10 PM
 */
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
  public static final String SHUTTING_DOWN_STR = "Shutting down";
  public static final String NONE_STR = "__none__";
  public static final String DATABASE_SERVER_GET_SCHEMA_STR = "DatabaseServer:getSchema";
  public static final String SCHEMA_OUT_OF_SYNC_EXCEPTION_STR = "SchemaOutOfSyncException";
  public static final String CURR_VER_STR = "currVer:";
  public static final String HOST_STR = "Host=";
  public static final String METHOD_STR = ", method=";
  public static final String EXPLAIN_STR = "explain";

  private final boolean isClient;
  private final int shard;
  private final int replica;
  private static ConcurrentHashMap<String, Thread> statsRecorderThreads = new ConcurrentHashMap<>();
  private final String allocatedStack;
  private final StatementHandlerFactory statementHandlerFactory;
  private Object databaseServer;
  private Server[][] servers;
  private AtomicBoolean isShutdown = new AtomicBoolean();
  public static final AtomicInteger clientRefCount = new AtomicInteger();
  public static final Map<String, DatabaseClient> sharedClients = new ConcurrentHashMap<>();
  public static final Queue<DatabaseClient> allClients = new ConcurrentLinkedQueue<>();

  private DatabaseCommon common = new DatabaseCommon();
  private static ThreadPoolExecutor executor = null;

  private static org.apache.log4j.Logger localLogger = org.apache.log4j.Logger.getLogger("com.sonicbase.logger");
  private static Logger logger = LoggerFactory.getLogger(DatabaseClient.class);


  private int pageSize = SELECT_PAGE_SIZE;

  private ThreadLocal<Boolean> isExplicitTrans = new ThreadLocal<>();
  private ThreadLocal<Boolean> isCommitting = new ThreadLocal<>();
  private ThreadLocal<Long> transactionId = new ThreadLocal<>();
  private ThreadLocal<List<TransactionOperation>> transactionOps = new ThreadLocal<>();
  private Timer statsTimer;
  private ConcurrentHashMap<String, StatementCacheEntry> statementCache = new ConcurrentHashMap<>();

  public static final Map<Integer, Map<Integer, Object>> dbservers = new ConcurrentHashMap<>();
  public static final Map<Integer, Map<Integer, Object>> dbdebugServers = new ConcurrentHashMap<>();

  private static final MetricRegistry METRICS = new MetricRegistry();

  private final Object idAllocatorLock = new Object();
  private final AtomicLong nextId = new AtomicLong(-1L);
  private final AtomicLong maxAllocatedId = new AtomicLong(-1L);
  private DescribeStatementHandler describeHandler = new DescribeStatementHandler(this);

  public static final com.codahale.metrics.Timer INDEX_LOOKUP_STATS = METRICS.timer("indexLookup");
  public static final com.codahale.metrics.Timer BATCH_INDEX_LOOKUP_STATS = METRICS.timer("batchIndexLookup");
  public static final com.codahale.metrics.Timer JOIN_EVALUATE = METRICS.timer("joinEvaluate");

  private static String[] writeVerbsArray = new String[]{
      "SchemaManager:dropTable",
      "SchemaManager:dropIndex",
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
      "PartitionManager:moveIndexEntries",
      "notifyRepartitioningComplete",
      "UpdateManager:truncateTable",
      "purge",
      "DatabaseServer:reserveNextIdFromReplica",
      "reserveNextId",
      "DatabaseServer:allocateRecordIds",
      "TransactionManager:abortTransaction",
      "ReadManager:serverSelectDelete",
      "UpdateManager:commit",
      "UpdateManager:rollback",
      "testWrite",
      "saveSchema",
      "MonitorManager:registerStats"
  };

  private static Set<String> writeVerbs = new HashSet<>();

  private static String[] parallelVerbsArray = new String[]{
      "UpdateManager:insertIndexEntryByKey",
      "UpdateManager:insertIndexEntryByKeyWithRecord",
      "PartitionManager:moveIndexEntries",
      "UpdateManager:batchInsertIndexEntryByKeyWithRecord",
      "UpdateManager:batchInsertIndexEntryByKey",
      "PartitionManager:moveIndexEntries",
      "PartitionManager:deleteMovedRecords",
      "MonitorManager:registerStats",
  };

  private static Set<String> parallelVerbs = new HashSet<>();
  private String cluster;
  private ClientStatsHandler clientStatsHandler = new ClientStatsHandler(this);

  public Server[][] getServersArray() {
    return servers;
  }

  public DatabaseClient(String host, int port, int shard, int replica, boolean isClient) {
    this(new String[]{host + ":" + port}, shard, replica, isClient, null, null, false);
  }

  public DatabaseClient(String[] hosts, int shard, int replica, boolean isClient) {
    this(hosts, shard, replica, isClient, null, null, false);
  }

  public DatabaseClient(String host, int port, int shard, int replica, boolean isClient, DatabaseCommon common, Object databaseServer) {
    this(new String[]{host + ":" + port}, shard, replica, isClient, common, databaseServer, false);
  }

  public DatabaseClient(String[] hosts, int shard, int replica, boolean isClient, DatabaseCommon common, Object databaseServer, boolean isShared) {
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
      localLogger.info("Adding startup server: host=" + host + ":" + port);
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
      synchronized (DatabaseClient.class) {
        DatabaseClient sharedClient = sharedClients.get(getCluster());
        if (sharedClient == null) {
          sharedClient = new DatabaseClient(hosts, shard, replica, isClient, common, databaseServer, true);
          sharedClients.put(getCluster(), sharedClient);

          Thread statsRecorderThread = ThreadUtil.createThread(new ClientStatsHandler.QueryStatsRecorder(this, getCluster()), "SonicBase Stats Recorder - cluster=" + getCluster());
          statsRecorderThread.start();
          statsRecorderThreads.put(getCluster(), statsRecorderThread);
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
    for (String verb : writeVerbsArray) {
      writeVerbs.add(verb);
    }
    for (String verb : parallelVerbsArray) {
      parallelVerbs.add(verb);
    }
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
    transactionOps.set(null);
    isCommitting.set(false);
    try {
      transactionId.set(allocateId(dbName));
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public void commit(String dbName) throws DatabaseException {
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
        transactionOps.set(null);
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
    transactionOps.set(null);
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
    try {
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
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private static ConcurrentHashMap<String, String> lowered = new ConcurrentHashMap<>();

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

  public ThreadLocal<List<TransactionOperation>> getTransactionOps() {
    return transactionOps;
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

  public void setTransactionId(long transactionId) {
    this.transactionId.set(transactionId);
  }

  public void setServers(Server[][] servers) {
    this.servers = servers;
  }

  public void setClientStatsHandler(ClientStatsHandler clientStatsHandler) {
    this.clientStatsHandler = clientStatsHandler;
  }

  public static class Server {
    private boolean dead;
    private String hostPort;
    private AtomicInteger replicaOffset = new AtomicInteger();
    private DatabaseSocketClient socketClient = new DatabaseSocketClient();

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

        servers[i][j] = new Server(isPrivate ? replicaHost.getPrivateAddress() : replicaHost.getPublicAddress(), replicaHost.getPort());
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
              ret = send(null, 0, localReplica, cobj, Replica.SPECIFIED);
              receivedReplica = localReplica;
              break;
            }
            catch (Exception e) {
              localLogger.error("Error getting config from replica: replica=" + localReplica, e);
            }
          }
        }
        if (ret == null) {
          localLogger.error("Error getting config from any replica");
        }
        else {
          ComObject retObj = new ComObject(ret);
          common.deserializeConfig(retObj.getByteArray(ComObject.Tag.CONFIG_BYTES));
          localLogger.info("Client received config from server: sourceReplica=" + receivedReplica +
              ", config=" + common.getServersConfig());
        }
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
        if (isShutdown.get()) {
          throw new DatabaseException(SHUTTING_DOWN_STR);
        }
        logger.error("Error syncing config", t);
        try {
          Thread.sleep(2000);
        }
        catch (InterruptedException e) {
          throw new DatabaseException(e);
        }
      }
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
    finally {
      for (Future future : futures) {
        executor.getQueue().remove(future);
        future.cancel(true);
      }
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
        lastException = null;
        return send(null, servers[0], 0, masterReplica, body, Replica.SPECIFIED);
      }
      catch (DeadServerException e1) {
        throw e1;
      }
      catch (SchemaOutOfSyncException e) {
        throw e;
      }
      catch (Exception e) {
        lastException = e;
        if (getReplicaCount() == 1) {
          throw e;
        }
        for (int i = 0; i < getReplicaCount(); i++) {
          if (i == masterReplica) {
            continue;
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

                logger.info("Schema received from server: currVer=" + common.getSchemaVersion());
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
      }
      if (j == 1 && lastException != null) {
        throw new DatabaseException(lastException);
      }
    }
    return null;
  }

  protected void handleSchemaOutOfSyncException(Exception e) {
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
      }
      if (!schemaOutOfSync) {
        throw e;
      }
      synchronized (this) {
        Integer serverVersion = null;
        if (msg != null) {
          int pos = msg.indexOf(CURR_VER_STR);
          if (pos != -1) {
            int pos2 = msg.indexOf(":", pos + CURR_VER_STR.length());
            serverVersion = Integer.valueOf(msg.substring(pos + CURR_VER_STR.length(), pos2));
          }
        }

        if (serverVersion == null || serverVersion > common.getSchemaVersion()) {
          syncSchema(serverVersion);
        }
      }

      throw new SchemaOutOfSyncException();
    }
    catch (SchemaOutOfSyncException e1) {
      throw e1;
    }
    catch (DeadServerException e2) {
      throw e2;
    }
    catch (DatabaseException e3) {
      throw e3;
    }
    catch (Exception e1) {
      throw new DatabaseException(e1);
    }
  }

  public byte[] send(String batchKey, Server[] replicas, int shard, long authUser, ComObject body, Replica replica) {
    return send(batchKey, replicas, shard, authUser, body, replica, false);
  }

  private ConcurrentHashMap<String, String> inserted = new ConcurrentHashMap<>();

  public byte[] send(
      String methodStr, Server[] replicas, int shard, long authUser,
      ComObject body, Replica replica, boolean ignoreDeath) {
    try {
      if (methodStr != null) {
        body.put(ComObject.Tag.METHOD, methodStr);
      }
      if (body == null) {
        body = new ComObject();
      }
      String method = body.getString(ComObject.Tag.METHOD);

      byte[] ret = null;
      for (int attempt = 0; attempt < 1; attempt++) {
        if (isShutdown.get()) {
          throw new DatabaseException(SHUTTING_DOWN_STR);
        }
        try {
          if (replica == Replica.ALL) {
            try {
              boolean local = false;
              List<DatabaseSocketClient.Request> requests = new ArrayList<>();
              for (int i = 0; i < replicas.length; i++) {
                if (isShutdown.get()) {
                  throw new DatabaseException(SHUTTING_DOWN_STR);
                }
                Server server = replicas[i];
                if (server.dead) {
                  throw new DeadServerException(HOST_STR + server.hostPort + METHOD_STR + method);
                }
                if (shard == this.shard && i == this.replica && databaseServer != null) {
                  local = true;
                  ret = invokeOnServer(databaseServer, body.serialize(), false, true);
                }
                else {
                  Object dbServer = getLocalDbServer(shard, i);
                  if (dbServer != null) {
                    local = true;
                    ret = invokeOnServer(dbServer, body.serialize(), false, true);
                  }
                  else {
                    DatabaseSocketClient.Request request = new DatabaseSocketClient.Request();
                    request.setBody(body.serialize());
                    request.setHostPort(server.hostPort);
                    requests.add(request);
                  }
                }
              }
              if (!local) {
                ret = doSendOnSocket(requests);
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
          }
          else if (replica == Replica.MASTER) {
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
              catch (DeadServerException e1) {
                throw e;
              }
              catch (Exception e1) {
                e = new DatabaseException(HOST_STR + currReplica.hostPort + METHOD_STR + method, e1);
                handleDeadServer(e, currReplica);
                handleSchemaOutOfSyncException(e);
              }
            }
          }
          else if (replica == Replica.SPECIFIED) {
            boolean skip = false;
            if (!ignoreDeath && replicas[(int) authUser].dead && writeVerbs.contains(method)) {
              ComObject header = new ComObject();
              header.put(ComObject.Tag.METHOD, body.getString(ComObject.Tag.METHOD));
              header.put(ComObject.Tag.REPLICA, (int) authUser);
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
                  handleDeadServer(e, replicas[(int) authUser]);
                  handleSchemaOutOfSyncException(e);
                }
                catch (Exception t) {
                  throw new DatabaseException(t);
                }
              }
            }
          }
          else if (replica == Replica.DEF) {
            if (writeVerbs.contains(method)) {
              int masterReplica = -1;
              for (int i = 0; i < 10; i++) {
                if (isShutdown.get()) {
                  throw new DatabaseException(SHUTTING_DOWN_STR);
                }
                masterReplica = common.getServersConfig().getShards()[shard].getMasterReplica();
                Server currReplica = replicas[masterReplica];
                try {
                  if (!ignoreDeath && replicas[masterReplica].dead) {
                    logger.error("dead server: master={}", masterReplica);
                    throw new DeadServerException(HOST_STR + currReplica.hostPort + METHOD_STR + method);
                  }
                  body.put(ComObject.Tag.REPLICATION_MASTER, masterReplica);
                  if (shard == this.shard && masterReplica == this.replica && databaseServer != null) {
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
                  if (isShutdown.get()) {
                    throw new DatabaseException(SHUTTING_DOWN_STR);
                  }
                  Thread.sleep(1000);
                  try {
                    syncSchema();
                  }
                  catch (Exception e1) {
                    logger.error("Error syncing schema", e1);
                  }
                }
                catch (Exception e) {
                  e = new DatabaseException(HOST_STR + currReplica.hostPort + METHOD_STR + method, e);
                  handleSchemaOutOfSyncException(e);
                }
              }
              while (true) {
                if (isShutdown.get()) {
                  throw new DatabaseException(SHUTTING_DOWN_STR);
                }

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
                    while (true) {
                      if (isShutdown.get()) {
                        throw new DatabaseException(SHUTTING_DOWN_STR);
                      }

                      boolean skip = false;
                      if (!ignoreDeath && dead) {
                        try {
                          if (writeVerbs.contains(method)) {
                            ComObject header = new ComObject();
                            header.put(ComObject.Tag.METHOD, body.getString(ComObject.Tag.METHOD));
                            header.put(ComObject.Tag.REPLICA, (int) authUser);
                            body.put(ComObject.Tag.HEADER, header);

                            body.put(ComObject.Tag.METHOD, "queueForOtherServer");

                            masterReplica = common.getServersConfig().getShards()[shard].getMasterReplica();
                            if (shard == this.shard && masterReplica == this.replica && databaseServer != null) {
                              invokeOnServer(databaseServer, body.serialize(), false, true);
                            }
                            else {
                              Object dbServer = getLocalDbServer(shard, (int) masterReplica);
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
                            continue;
                          }
                          throw e;
                        }
                      }
                      if (!skip) {
                        try {
                          if (shard == this.shard && i == this.replica && databaseServer != null) {
                            invokeOnServer(databaseServer, body.serialize(), false, true);
                          }
                          else {
                            Object dbServer = getLocalDbServer(shard, (int) i);
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
                            dead = true;
                            continue;
                          }
                          try {
                            handleSchemaOutOfSyncException(e);
                          }
                          catch (SchemaOutOfSyncException e1) {
                            body.put(ComObject.Tag.SCHEMA_VERSION, common.getSchemaVersion());
                            continue;
                          }
                          throw e;
                        }
                      }
                      break;
                    }
                  }
                  return ret;
                }
                catch (SchemaOutOfSyncException e) {
                  throw e;
                }
                catch (DeadServerException e) {
                  if (isShutdown.get()) {
                    throw new DatabaseException(SHUTTING_DOWN_STR);
                  }
                  Thread.sleep(1000);
                  try {
                    syncSchema();
                  }
                  catch (Exception e1) {
                    logger.error("Error syncing schema", e1);
                  }
                }
                catch (Exception e) {
                  e = new DatabaseException(HOST_STR + currReplica.hostPort + METHOD_STR + method, e);
                  handleSchemaOutOfSyncException(e);
                }
              }
            }
            else {
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
                      if (shard == this.shard && replicaOffset == this.replica && databaseServer != null) {
                        return invokeOnServer(databaseServer, body.serialize(), false, true);
                      }
                      Object dbServer = getLocalDbServer(shard, (int) replicaOffset);
                      if (dbServer != null) {
                        return invokeOnServer(dbServer, body.serialize(), false, true);
                      }
                      else {
                        return replicas[replicaOffset].doSend(null, body);
                      }
                    }
                    catch (Exception e) {
                      try {
                        handleDeadServer(e, replicas[replicaOffset]);
                        handleSchemaOutOfSyncException(e);
                        lastException = e;
                      }
                      catch (SchemaOutOfSyncException s) {
                        throw s;
                      }
                      catch (Exception t) {
                        lastException = t;
                      }
                    }
                  }
                }
              }
              if (!success) {
                if (lastException != null) {
                  throw new DatabaseException("Failed to send to any replica: method=" + method, lastException);
                }
                throw new DatabaseException("Failed to send to any replica: method=" + method);
              }
            }
          }
          if (attempt == 9) {
            throw new DatabaseException("Error sending message");
          }
        }
        catch (SchemaOutOfSyncException | DeadServerException e) {
          throw e;
        }
        catch (Exception e) {
          if (attempt == 0) {
            throw new DatabaseException(e);
          }
        }
      }
    }
    catch (SchemaOutOfSyncException | DeadServerException e) {
      throw e;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
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

  private void handleDeadServer(Throwable t, Server replica) {
  }

  private Object getLocalDbServer(int shard, int replica) {
    Map<Integer, Map<Integer, Object>> dbServers = DatabaseClient.getServers();
    Object dbserver = null;
    if (dbServers != null && dbServers.get(shard) != null) {
      dbserver = dbServers.get(shard).get(replica);
    }
    return dbserver;
  }

  public int selectShard(long objectId) {
    return (int) Math.abs((objectId % servers.length));
  }

  private Random rand = new Random(System.currentTimeMillis());

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
    private AtomicLong whenUsed = new AtomicLong();
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
        if (common == null) {
          throw new DatabaseException("null common");
        }
        if (dbName == null) {
          throw new DatabaseException("null dbName");
        }
        if (common.getDatabases() == null || !common.getDatabases().containsKey(dbName)) {
          syncSchema();
          if (!common.getDatabases().containsKey(dbName)) {
            throw new DatabaseException("Database does not exist: dbName=" + dbName);
          }
        }
        Statement statement;
        if (toLower(sql.toLowerCase().substring(0, "call".length())).startsWith("call")) {
          int pos = sql.toLowerCase().indexOf("procedure");
          sql = sql.substring(pos + "procedure".length()).trim();
          if (sql.charAt(0) == '(') {
            sql = sql.substring(1);
            sql = sql.substring(0, sql.lastIndexOf(')'));
          }
          sql = "execute procedure " + sql;
        }

        if (toLower(sql.substring(0, "describe".length())).startsWith("describe")) {
          return describeHandler.doDescribe(dbName, sql);
        }
        else if (toLower(sql.substring(0, EXPLAIN_STR.length())).startsWith(EXPLAIN_STR)) {
          return doExplain(dbName, sql, parms);
        }
        else {
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
      ConcurrentSkipListMap<Object[], InsertStatementHandler.KeyInfo> indexMap = orderedKeyInfos.get(keyInfo.getIndexSchema().getName());
      if (indexMap == null) {
        indexMap = new ConcurrentSkipListMap<>(new Comparator<Object[]>() {
          @Override
          public int compare(Object[] o1, Object[] o2) {
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
        });
        orderedKeyInfos.put(keyInfo.getIndexSchema().getName(), indexMap);
      }
      indexMap.put(keyInfo.getKey(), keyInfo);
    }
  }

  static class TransactionOperation {
    private StatementImpl statement;
    private ParameterHandler parms;

    public TransactionOperation(StatementImpl statement, ParameterHandler parms) {
      this.statement = statement;
      this.parms = parms;
    }
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
    return retObj.getLong(ComObject.Tag.SIZE);
  }

  public void syncSchema(Integer serverVersion) {
    synchronized (common) {
      if (serverVersion == null) {
        syncSchema();
      }
      else {
        if (serverVersion > common.getSchemaVersion()) {
          doSyncSchema(serverVersion);
        }
      }
    }
  }

  public void syncSchema() {
    doSyncSchema(null);
  }

  public void doSyncSchema(Integer serverVersion) {
    synchronized (common) {
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.DB_NAME, NONE_STR);
      cobj.put(ComObject.Tag.SCHEMA_VERSION, common.getSchemaVersion());
      try {

        byte[] ret = null;
        try {
          ret = sendToMaster(DATABASE_SERVER_GET_SCHEMA_STR, cobj);
        }
        catch (Exception e) {
          logger.error("Error getting schema from master", e);
        }
        if (ret == null) {
          int masterReplica = common.getServersConfig().getShards()[0].getMasterReplica();
          for (int localReplica = 0; localReplica < getReplicaCount(); localReplica++) {
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
        }
        if (ret == null) {
          logger.error("Error getting schema from any replica");
        }
        else {
          ComObject retObj = new ComObject(ret);
          common.deserializeSchema(retObj.getByteArray(ComObject.Tag.SCHEMA_BYTES));

          if ((serverVersion != null && common.getSchemaVersion() < serverVersion)) {
            try {
              cobj.put(ComObject.Tag.FORCE, true);
              ret = sendToMaster(cobj);
              retObj = new ComObject(ret);
              common.deserializeSchema(retObj.getByteArray(ComObject.Tag.SCHEMA_BYTES));
            }
            catch (Exception e) {
              logger.error("Error getting schema from replica: replica=" + replica, e);
            }
          }
          ServersConfig serversConfig = common.getServersConfig();
          for (int i = 0; i < serversConfig.getShards().length; i++) {
            for (int j = 0; j < serversConfig.getShards()[0].getReplicas().length; j++) {
              servers[i][j].dead = serversConfig.getShards()[i].getReplicas()[j].isDead();
            }
          }

          logger.info(" Schema received from server: currVer=" + common.getSchemaVersion() + " " /*+ ExceptionUtils.getStackTrace(new Exception())*/);
        }
      }
      catch (Exception t) {
        throw new DatabaseException(t);
      }
    }
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
}
