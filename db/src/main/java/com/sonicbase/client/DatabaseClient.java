package com.sonicbase.client;

import com.codahale.metrics.MetricRegistry;
import com.sonicbase.common.*;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.jdbcdriver.QueryType;
import com.sonicbase.procedure.StoredProcedureContextImpl;
import com.sonicbase.query.*;
import com.sonicbase.query.impl.*;
import com.sonicbase.schema.Schema;
import com.sonicbase.server.DatabaseServer;
import com.sonicbase.socket.DatabaseSocketClient;
import com.sonicbase.socket.DeadServerException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import org.apache.commons.lang.exception.ExceptionUtils;

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
  private final boolean isClient;
  private final int shard;
  private final int replica;
  private static ConcurrentHashMap<String, Thread> statsRecorderThreads = new ConcurrentHashMap<>();
  private final String allocatedStack;
  private final StatementHandlerFactory statementHandlerFactory;
  private Object databaseServer;
  private Server[][] servers;
  private AtomicBoolean isShutdown = new AtomicBoolean();
  public static AtomicInteger clientRefCount = new AtomicInteger();
  public static ConcurrentHashMap<String, DatabaseClient> sharedClients = new ConcurrentHashMap<>();
  public static ConcurrentLinkedQueue<DatabaseClient> allClients = new ConcurrentLinkedQueue<>();

  private DatabaseCommon common = new DatabaseCommon();
  private static ThreadPoolExecutor executor = null;

  private static org.apache.log4j.Logger localLogger = org.apache.log4j.Logger.getLogger("com.sonicbase.logger");
  private static Logger logger;


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

  private int pageSize = SELECT_PAGE_SIZE;

  private ThreadLocal<Boolean> isExplicitTrans = new ThreadLocal<>();
  private ThreadLocal<Boolean> isCommitting = new ThreadLocal<>();
  private ThreadLocal<Long> transactionId = new ThreadLocal<>();
  private ThreadLocal<List<TransactionOperation>> transactionOps = new ThreadLocal<>();
  Timer statsTimer;
  private ConcurrentHashMap<String, StatementCacheEntry> statementCache = new ConcurrentHashMap<>();

  public static ConcurrentHashMap<Integer, Map<Integer, Object>> dbservers = new ConcurrentHashMap<>();
  public static ConcurrentHashMap<Integer, Map<Integer, Object>> dbdebugServers = new ConcurrentHashMap<>();

  private static final MetricRegistry METRICS = new MetricRegistry();

  private final Object idAllocatorLock = new Object();
  private final AtomicLong nextId = new AtomicLong(-1L);
  private final AtomicLong maxAllocatedId = new AtomicLong(-1L);
  private DescribeStatementHandler describeHandler = new DescribeStatementHandler(this);

  public static final com.codahale.metrics.Timer INDEX_LOOKUP_STATS = METRICS.timer("indexLookup");
  public static final com.codahale.metrics.Timer BATCH_INDEX_LOOKUP_STATS = METRICS.timer("batchIndexLookup");
  public static final com.codahale.metrics.Timer JOIN_EVALUATE = METRICS.timer("joinEvaluate");

  private Set<String> write_verbs = new HashSet<String>();
  private static String[] write_verbs_array = new String[]{
      "insert",
      "dropTable",
      "dropIndex",
      "dropColumn",
      "dropColumnSlave",
      "addColumn",
      "addColumnSlave",
      "dropIndexSlave",
      "doCreateIndex",
      "createIndex",
      "createIndexSlave",
      "createTable",
      "createTableSlave",
      "createDatabase",
      "createDatabaseSlave",
      "echoWrite",
      "delete",
      "deleteRecord",
      "deleteIndexEntryByKey",
      "deleteIndexEntry",
      "updateRecord",
      "populateIndex",
      "insertIndexEntryByKey",
      "insertIndexEntryByKeyWithRecord",
      "removeRecord",
      //"beginRebalance",
      "updateServersConfig",
      "deleteRecord",
      "allocateRecordIds",
      "setMaxRecordId",
      "reserveNextId",
      "updateSchema",
      "expirePreparedStatement",
      "rebalanceOrderedIndex",
      "beginRebalanceOrderedIndex",
      "moveIndexEntries",
      "notifyDeletingComplete",
      "notifyRepartitioningComplete",
      "notifyRepartitioningRecordsByIdComplete",
      "batchInsertIndexEntryByKeyWithRecord",
      "batchInsertIndexEntryByKey",
      "moveHashPartition",
      "moveIndexEntries",
      "moveRecord",
      "notifyRepartitioningComplete",
      "truncateTable",
      "purge",
      "reserveNextIdFromReplica",
      "reserveNextId",
      "allocateRecordIds",
      "abortTransaction",
      "serverSelectDelete",
      "commit",
      "rollback",
      "testWrite",
      "saveSchema",
      "registerStats"
  };

  private static Set<String> writeVerbs = new HashSet<String>();

  private Set<String> parallel_verbs = new HashSet<String>();
  private static String[] parallel_verbs_array = new String[]{
      "insert",
      "insertIndexEntryByKey",
      "insertIndexEntryByKeyWithRecord",
      "moveIndexEntries",
      "batchInsertIndexEntryByKeyWithRecord",
      "batchInsertIndexEntryByKey",
      "moveIndexEntries",
      "moveRecord",
      "deleteMovedRecords",
      "registerStats",
  };

  private static Set<String> parallelVerbs = new HashSet<String>();
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
      int port = Integer.valueOf(parts[1]);
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

    logger = new Logger(this);

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
    for (String verb : write_verbs_array) {
      write_verbs.add(verb);
    }

  }


  public static AtomicInteger getClientRefCount() {
    return clientRefCount;
  }

  public static ConcurrentHashMap<String, DatabaseClient> getSharedClients() {
    return sharedClients;
  }

  public static ConcurrentLinkedQueue<DatabaseClient> getAllClients() {
    return allClients;
  }

  public Set<String> getWrite_verbs() {
    return write_verbs;
  }

  public static String[] getWrite_verbs_array() {
    return write_verbs_array;
  }

  public static Set<String> getWriteVerbs() {
    return writeVerbs;
  }

  public static Set<String> getParallelVerbs() {
    return parallelVerbs;
  }

  static {
    for (String verb : write_verbs_array) {
      writeVerbs.add(verb);
    }
    for (String verb : parallel_verbs_array) {
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

  public void commit(String dbName, SelectStatementImpl.Explain explain) throws DatabaseException {
    isCommitting.set(true);
    int schemaRetryCount = 0;
    while (true) {
      if (isShutdown.get()) {
        throw new DatabaseException("Shutting down");
      }

      try {
        ComObject cobj = new ComObject();
        cobj.put(ComObject.Tag.dbName, dbName);
        if (schemaRetryCount < 2) {
          cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
        }
        cobj.put(ComObject.Tag.method, "commit");
        cobj.put(ComObject.Tag.transactionId, transactionId.get());
        sendToAllShards(null, 0, cobj, DatabaseClient.Replica.def);

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
    cobj.put(ComObject.Tag.dbName, dbName);
    if (schemaRetryCount < 2) {
      cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
    }
    cobj.put(ComObject.Tag.method, "rollback");
    cobj.put(ComObject.Tag.transactionId, transactionId.get());
    sendToAllShards(null, 0, cobj, DatabaseClient.Replica.def);

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
    cobj.put(ComObject.Tag.dbName, dbName);
    cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
    cobj.put(ComObject.Tag.method, "createDatabase");
    cobj.put(ComObject.Tag.masterSlave, "master");

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
      cobj.put(ComObject.Tag.dbName, "__none__");
      cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
      cobj.put(ComObject.Tag.method, "healthCheck");

      try {
        byte[] bytes = sendToMaster(cobj);
        ComObject retObj = new ComObject(bytes);
        if (retObj.getString(ComObject.Tag.status).equals("{\"status\" : \"ok\"}")) {
          ComObject rcobj = new ComObject();
          rcobj.put(ComObject.Tag.dbName, "__none__");
          rcobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
          rcobj.put(ComObject.Tag.method, "reconfigureCluster");
          bytes = sendToMaster(null);
          retObj = new ComObject(bytes);
          int count = retObj.getInt(ComObject.Tag.count);
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

  public void setDatabaseServer(DatabaseServer databaseServer) {
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

  static class SocketException extends Exception {
    public SocketException(String s, Throwable t) {
      super(s, t);
    }

    public SocketException(String s) {
      super(s);
    }
  }

  public static class Server {
    private boolean dead;
    private String hostPort;

    public Server(String host, int port) {
      this.hostPort = host + ":" + port;
      this.dead = false;
    }

    private DatabaseSocketClient socketClient = new DatabaseSocketClient();

    public DatabaseSocketClient getSocketClient() {
      return socketClient;
    }

    public byte[] do_send(String batchKey, ComObject body) {
      return socketClient.do_send(batchKey, body.serialize(), hostPort);
    }

    public byte[] do_send(String batchKey, byte[] body) {
      return socketClient.do_send(batchKey, body, hostPort);
    }

    public boolean isDead() {
      return dead;
    }
  }

  public byte[] do_send(List<DatabaseSocketClient.Request> requests) {
    return DatabaseSocketClient.do_send(requests);
  }

  public void configureServers() {
    ServersConfig serversConfig = common.getServersConfig();

    boolean isPrivate = !isClient || serversConfig.clientIsInternal();

    ServersConfig.Shard[] shards = serversConfig.getShards();

    servers = new Server[shards.length][];
    for (int i = 0; i < servers.length; i++) {
      ServersConfig.Shard shard = shards[i];
      servers[i] = new Server[shard.getReplicas().length];
      for (int j = 0; j < servers[i].length; j++) {
        ServersConfig.Host replicaHost = shard.getReplicas()[j];

        servers[i][j] = new Server(isPrivate ? replicaHost.getPrivateAddress() : replicaHost.getPublicAddress(), replicaHost.getPort());
      }
    }
  }

  private void syncConfig() {
    while (true) {
      if (isShutdown.get()) {
        throw new DatabaseException("Shutting down");
      }

      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.dbName, "__none__");
      cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
      cobj.put(ComObject.Tag.method, "getConfig");
      try {
        byte[] ret = null;
        int receivedReplica = -1;
        try {
          ret = send(null, 0, 0, cobj, Replica.specified);
          receivedReplica = 0;
        }
        catch (Exception e) {
          localLogger.error("Error getting config from master", e);
        }
        if (ret == null) {
          for (int replica = 1; replica < getReplicaCount(); replica++) {
            if (isShutdown.get()) {
              throw new DatabaseException("Shutting down");
            }
            try {
              ret = send(null, 0, replica, cobj, Replica.specified);
              receivedReplica = replica;
              break;
            }
            catch (Exception e) {
              localLogger.error("Error getting config from replica: replica=" + replica, e);
            }
          }
        }
        if (ret == null) {
          localLogger.error("Error getting config from any replica");
        }
        else {
          ComObject retObj = new ComObject(ret);
          common.deserializeConfig(retObj.getByteArray(ComObject.Tag.configBytes));
          localLogger.info("Client received config from server: sourceReplica=" + receivedReplica +
              ", config=" + common.getServersConfig());
        }
        if (common.getServersConfig() == null) {
          if (isShutdown.get()) {
            throw new DatabaseException("Shutting down");
          }
          Thread.sleep(1_000);
          continue;
        }
        break;
      }
      catch (Exception t) {
        if (isShutdown.get()) {
          throw new DatabaseException("Shutting down");
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

  public void initDb(String dbName) {
    while (true) {
      if (isShutdown.get()) {
        throw new DatabaseException("Shutting down");
      }

      try {
        syncSchema();
        break;
      }
      catch (Exception e) {
        if (isShutdown.get()) {
          throw new DatabaseException("Shutting down");
        }
        logger.error("Error synching schema", e);
        try {
          Thread.sleep(1000);
        }
        catch (InterruptedException e1) {
          throw new DatabaseException(e1);
        }
        continue;
      }
    }

  }

  public byte[][] sendToAllShards(
      final String batchKey,
      final long auth_user, final ComObject body, final Replica replica) {
    return sendToAllShards(batchKey, auth_user, body, replica, false);
  }

  public byte[][] sendToAllShards(
      final String batchKey,
      final long auth_user, final ComObject body, final Replica replica, final boolean ignoreDeath) {
    List<Future<byte[]>> futures = new ArrayList<Future<byte[]>>();
    try {
      final byte[] bodyBytes = body.serialize();
      for (int i = 0; i < servers.length; i++) {
        final int shard = i;
        futures.add(executor.submit(new Callable<byte[]>() {
          @Override
          public byte[] call() {
            return send(batchKey, shard, auth_user, new ComObject(bodyBytes), replica, ignoreDeath);
          }
        }));
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

  public byte[] send(String batchKey,
                     int shard, long auth_user, ComObject body, Replica replica) {
    return send(batchKey, shard, auth_user, body, replica, false);
  }

  public byte[] send(String batchKey,
                     int shard, long auth_user, ComObject body, Replica replica, boolean ignoreDeath) {
    return send(batchKey, servers[shard], shard, auth_user, body, replica, ignoreDeath);
  }

  public byte[] sendToMaster(ComObject body) {
    Exception lastException = null;
    for (int j = 0; j < 2; j++) {
      if (isShutdown.get()) {
        throw new DatabaseException("Shutting down");
      }

      int masterReplica = 0;
      if (common.getServersConfig() != null) {
        masterReplica = common.getServersConfig().getShards()[0].getMasterReplica();
      }
      try {
        lastException = null;
        return send(null, servers[0], 0, masterReplica, body, Replica.specified);
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
          cobj.put(ComObject.Tag.dbName, "__none__");
          cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
          cobj.put(ComObject.Tag.method, "getSchema");
          try {

            byte[] ret = send(null, 0, i, cobj, Replica.specified);
            if (ret != null) {
              ComObject retObj = new ComObject(ret);
              byte[] bytes = retObj.getByteArray(ComObject.Tag.schemaBytes);
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

  private void handleSchemaOutOfSyncException(Exception e) {
    try {
      boolean schemaOutOfSync = false;
      String msg = null;
      int index = ExceptionUtils.indexOfThrowable(e, SchemaOutOfSyncException.class);
      if (-1 != index) {
        schemaOutOfSync = true;
        msg = ExceptionUtils.getThrowables(e)[index].getMessage();
      }
      else if (e.getMessage() != null && e.getMessage().contains("SchemaOutOfSyncException")) {
        schemaOutOfSync = true;
        msg = e.getMessage();
      }
      else {
        Throwable t = e;
        while (true) {
          if (isShutdown.get()) {
            throw new DatabaseException("Shutting down");
          }

          t = t.getCause();
          if (t == null) {
            break;
          }
          if (t.getMessage() != null && t.getMessage().contains("SchemaOutOfSyncException")) {
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
          int pos = msg.indexOf("currVer:");
          if (pos != -1) {
            int pos2 = msg.indexOf(":", pos + "currVer:".length());
            serverVersion = Integer.valueOf(msg.substring(pos + "currVer:".length(), pos2));
          }
        }

        if (serverVersion == null || serverVersion > common.getSchemaVersion()) {
          //logger.info("Schema out of sync: currVersion=" + common.getSchemaVersion());
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

  public byte[] send(
      String batchKey, Server[] replicas, int shard, long auth_user,
      ComObject body, Replica replica) {
    return send(batchKey, replicas, shard, auth_user, body, replica, false);
  }

  private ConcurrentHashMap<String, String> inserted = new ConcurrentHashMap<>();

  public byte[] send(
      String batchKey, Server[] replicas, int shard, long auth_user,
      ComObject body, Replica replica, boolean ignoreDeath) {
    try {
      if (body == null) {
        body = new ComObject();
      }
      String method = body.getString(ComObject.Tag.method);

      byte[] ret = null;
      for (int attempt = 0; attempt < 1; attempt++) {
        if (isShutdown.get()) {
          throw new DatabaseException("Shutting down");
        }
        try {
          if (replica == Replica.all) {
            try {
              boolean local = false;
              List<DatabaseSocketClient.Request> requests = new ArrayList<>();
              for (int i = 0; i < replicas.length; i++) {
                if (isShutdown.get()) {
                  throw new DatabaseException("Shutting down");
                }
                Server server = replicas[i];
                if (server.dead) {
                  throw new DeadServerException("Host=" + server.hostPort + ", method=" + method);
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
                    request.setBatchKey(batchKey);
                    request.setBody(body.serialize());
                    request.setHostPort(server.hostPort);
                    request.setSocketClient(server.socketClient);
                    requests.add(request);
                  }
                }
              }
              if (!local) {
                ret = DatabaseSocketClient.do_send(requests);
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
                throw t;
              }
            }
          }
          else if (replica == Replica.master) {
            int masterReplica = -1;
            while (true) {
              if (isShutdown.get()) {
                throw new DatabaseException("Shutting down");
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
                throw new DeadServerException("Host=" + currReplica.hostPort + ", method=" + method);
              }
              if (shard == this.shard && masterReplica == this.replica && databaseServer != null) {
                return invokeOnServer(databaseServer, body.serialize(), false, true);
              }
              Object dbServer = getLocalDbServer(shard, masterReplica);
              if (dbServer != null) {
                return invokeOnServer(dbServer, body.serialize(), false, true);
              }
              return currReplica.do_send(batchKey, body);
            }
            catch (Exception e) {
              syncSchema();

              masterReplica = common.getServersConfig().getShards()[shard].getMasterReplica();
              currReplica = replicas[masterReplica];
              try {
                if (!ignoreDeath && currReplica.dead) {
                  throw new DeadServerException("Host=" + currReplica.hostPort + ", method=" + method);
                }
                if (shard == this.shard && masterReplica == this.replica && databaseServer != null) {
                  return invokeOnServer(databaseServer, body.serialize(), false, true);
                }
                Object dbServer = getLocalDbServer(shard, masterReplica);
                if (dbServer != null) {
                  return invokeOnServer(dbServer, body.serialize(), false, true);
                }
                return currReplica.do_send(batchKey, body);
              }
              catch (DeadServerException e1) {
                throw e;
              }
              catch (Exception e1) {
                e = new DatabaseException("Host=" + currReplica.hostPort + ", method=" + method, e1);
                handleDeadServer(e1, currReplica);
                handleSchemaOutOfSyncException(e1);
              }
            }
          }
          else if (replica == Replica.specified) {
            boolean skip = false;
            if (!ignoreDeath && replicas[(int) auth_user].dead) {
              if (writeVerbs.contains(method)) {
                ComObject header = new ComObject();
                header.put(ComObject.Tag.method, body.getString(ComObject.Tag.method));
                header.put(ComObject.Tag.replica, (int) auth_user);
                body.put(ComObject.Tag.header, header);

                body.put(ComObject.Tag.method, "queueForOtherServer");

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
                    replicas[masterReplica].do_send(null, body);
                  }
                }
                skip = true;
              }
            }
            if (!skip) {
              try {
                if (shard == this.shard && auth_user == this.replica && databaseServer != null) {
                  return invokeOnServer(databaseServer, body.serialize(), false, true);
                }
                Object dbServer = getLocalDbServer(shard, (int) auth_user);
                if (dbServer != null) {
                  return invokeOnServer(dbServer, body.serialize(), false, true);
                }
                return replicas[(int) auth_user].do_send(batchKey, body);
              }
              catch (DeadServerException e) {
                throw e;
              }
              catch (Exception e) {
                e = new DatabaseException("Host=" + replicas[(int) auth_user].hostPort + ", method=" + method, e);
                try {
                  handleDeadServer(e, replicas[(int) auth_user]);
                  handleSchemaOutOfSyncException(e);
                }
                catch (Exception t) {
                  throw t;
                }
              }
            }
          }
          else if (replica == Replica.def) {
            if (write_verbs.contains(method)) {
              int masterReplica = -1;
              for (int i = 0; i < 10; i++) {
                if (isShutdown.get()) {
                  throw new DatabaseException("Shutting down");
                }
                masterReplica = common.getServersConfig().getShards()[shard].getMasterReplica();
                Server currReplica = replicas[masterReplica];
                try {
                  //int successCount = 0;
                  if (!ignoreDeath && replicas[masterReplica].dead) {
                    logger.error("dead server: master=" + masterReplica);
                    throw new DeadServerException("Host=" + currReplica.hostPort + ", method=" + method);
                  }
                  body.put(ComObject.Tag.replicationMaster, masterReplica);
                  if (shard == this.shard && masterReplica == this.replica && databaseServer != null) {
                    ret = invokeOnServer(databaseServer, body.serialize(), false, true);
                  }
                  else {
                    Object dbServer = getLocalDbServer(shard, (int) masterReplica);
                    if (dbServer != null) {
                      ret = invokeOnServer(dbServer, body.serialize(), false, true);
                    }
                    else {
                      ret = currReplica.do_send(batchKey, body.serialize());
                    }
                  }

                  body.remove(ComObject.Tag.replicationMaster);

                  if (ret != null) {
                    ComObject retObj = new ComObject(ret);
                    body.put(ComObject.Tag.sequence0, retObj.getLong(ComObject.Tag.sequence0));
                    body.put(ComObject.Tag.sequence1, retObj.getLong(ComObject.Tag.sequence1));
                  }
                  break;
                }
                catch (SchemaOutOfSyncException e) {
                  throw e;
                }
                catch (DeadServerException e) {
                  if (isShutdown.get()) {
                    throw new DatabaseException("Shutting down");
                  }
                  Thread.sleep(1000);
                  try {
                    syncSchema();
                  }
                  catch (Exception e1) {
                    logger.error("Error syncing schema", e1);
                  }
                  continue;
                }
                catch (Exception e) {
                  e = new DatabaseException("Host=" + currReplica.hostPort + ", method=" + method, e);
                  handleSchemaOutOfSyncException(e);
                }
              }
              while (true) {
                if (isShutdown.get()) {
                  throw new DatabaseException("Shutting down");
                }

                Server currReplica = null;
                try {
                  for (int i = 0; i < getReplicaCount(); i++) {
                    if (i == masterReplica) {
                      continue;
                    }
                    if (isShutdown.get()) {
                      throw new DatabaseException("Shutting down");
                    }
                    currReplica = replicas[i];
                    boolean dead = currReplica.dead;
                    while (true) {
                      if (isShutdown.get()) {
                        throw new DatabaseException("Shutting down");
                      }

                      boolean skip = false;
                      if (!ignoreDeath && dead) {
                        try {
                          if (writeVerbs.contains(method)) {
                            ComObject header = new ComObject();
                            header.put(ComObject.Tag.method, body.getString(ComObject.Tag.method));
                            header.put(ComObject.Tag.replica, (int) auth_user);
                            body.put(ComObject.Tag.header, header);

                            body.put(ComObject.Tag.method, "queueForOtherServer");

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
                                replicas[masterReplica].do_send(null, body.serialize());
                              }
                            }
                            skip = true;
                          }
                        }
                        catch (Exception e) {
                          if (e.getMessage().contains("SchemaOutOfSyncException")) {
                            syncSchema();
                            body.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
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
                              currReplica.do_send(batchKey, body.serialize());
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
                            body.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
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
                    throw new DatabaseException("Shutting down");
                  }
                  Thread.sleep(1000);
                  try {
                    syncSchema();
                  }
                  catch (Exception e1) {
                    logger.error("Error syncing schema", e1);
                  }
                  continue;
                }
                catch (Exception e) {
                  e = new DatabaseException("Host=" + currReplica.hostPort + ", method=" + method, e);
                  handleSchemaOutOfSyncException(e);
                }
              }
            }
            else {
              Exception lastException = null;
              boolean success = false;
              int offset = ThreadLocalRandom.current().nextInt(replicas.length);
              outer:
              for (int i = 0; i < 10; i++) {
                if (isShutdown.get()) {
                  throw new DatabaseException("Shutting down");
                }
                for (long rand = offset; rand < offset + replicas.length; rand++) {
                  if (isShutdown.get()) {
                    throw new DatabaseException("Shutting down");
                  }
                  int replicaOffset = Math.abs((int) (rand % replicas.length));
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
                        return replicas[replicaOffset].do_send(batchKey, body);
                      }
                    }
                    catch (Exception e) {
                      Server currReplica = replicas[replicaOffset];
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
          //return ret;
          if (attempt == 9) {
            throw new DatabaseException("Error sending message");
          }
        }
        catch (SchemaOutOfSyncException e) {
          throw e;
        }
        catch (DeadServerException e) {
          throw e;
        }
        catch (Exception e) {
          if (attempt == 0) {
            throw new DatabaseException(e);
          }
        }
      }
    }
    catch (SchemaOutOfSyncException e) {
      throw e;
    }
    catch (DeadServerException e) {
      throw e;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
    return null;
  }

  private byte[] invokeOnServer(Object dbServer, byte[] body, boolean replayedCommand, boolean enableQueuing) {
    try {
      return ((DatabaseServer) dbServer).invokeMethod(body, replayedCommand, enableQueuing);
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
    primary,
    secondary,
    all,
    def,
    specified,
    master
  }

  private AtomicLong nextRecordId = new AtomicLong();

  public boolean isBackupComplete() {
    try {
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.dbName, "__none__");
      cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
      cobj.put(ComObject.Tag.method, "isEntireBackupComplete");
      byte[] ret = send(null, 0, 0, cobj, DatabaseClient.Replica.master);
      ComObject retObj = new ComObject(ret);
      return retObj.getBoolean(ComObject.Tag.isComplete);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public boolean isRestoreComplete() {
    try {
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.dbName, "__none__");
      cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
      cobj.put(ComObject.Tag.method, "isEntireRestoreComplete");
      byte[] ret = send(null, 0, 0, cobj, DatabaseClient.Replica.master);
      ComObject retObj = new ComObject(ret);
      return retObj.getBoolean(ComObject.Tag.isComplete);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public void startRestore(String subDir) {
    try {
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.dbName, "__none__");
      cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
      cobj.put(ComObject.Tag.method, "startRestore");
      cobj.put(ComObject.Tag.directory, subDir);
      byte[] ret = send(null, 0, 0, cobj, DatabaseClient.Replica.master);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public void startBackup() {
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.dbName, "__none__");
    cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
    cobj.put(ComObject.Tag.method, "startBackup");
    byte[] ret = send(null, 0, 0, cobj, DatabaseClient.Replica.master);
  }


  private static class StatementCacheEntry {
    private AtomicLong whenUsed = new AtomicLong();
    private Statement statement;

  }

  public Object executeQuery(String dbName, QueryType queryType, String sql, ParameterHandler parms, boolean restrictToThisServer, StoredProcedureContextImpl procedureContext, boolean disableStats) throws SQLException {
    return executeQuery(dbName, queryType, sql, parms, false, null, null, null,
        restrictToThisServer, procedureContext, disableStats);
  }

  public Object executeQuery(String dbName, QueryType queryType, String sql, ParameterHandler parms, boolean debug,
                             Long sequence0, Long sequence1, Short sequence2, boolean restrictToThisServer,
                             StoredProcedureContextImpl procedureContext, boolean disableStats) throws SQLException {
    int schemaRetryCount = 0;
    while (true) {
      if (isShutdown.get()) {
        throw new DatabaseException("Shutting down");
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
        else if (toLower(sql.substring(0, "explain".length())).startsWith("explain")) {
          return doExplain(dbName, sql, parms, schemaRetryCount);
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
          long beginMillis = System.currentTimeMillis();
          boolean success = false;
          try {
            Object ret = null;
            StatementHandler handler = statementHandlerFactory.getHandler(statement);
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
              clientStatsHandler.registerCompletedQueryForStats(dbName, histogramEntry, beginMillis, beginNanos);
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
        logger.sendErrorToServer("Error processing request", e);
        throw new SQLException(e);
      }
    }
  }

  public static String removeOffsetAndLimit(String sql, Limit limit, Offset offset) {
    String sqlToUse = sql;
    String lowerSql = sql.toLowerCase();
    if (limit != null) {
      int limitPos = lowerSql.lastIndexOf("limit");
      int limitValuePos = lowerSql.indexOf(String.valueOf(limit.getRowCount()), limitPos);
      int endLimitValuePos = lowerSql.indexOf(" ", limitValuePos);

      sqlToUse = sql.substring(0, limitValuePos);
      sqlToUse += "x";
      if (endLimitValuePos != -1 && offset == null) {
        sqlToUse += sql.substring(endLimitValuePos);
      }
    }
    if (offset != null) {
      int offsetPos = lowerSql.lastIndexOf("offset");
      int offsetValuePos = lowerSql.indexOf(String.valueOf(offset.getOffset()), offsetPos);
      int endOffsetValuePos = lowerSql.indexOf(" ", offsetValuePos);

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

  private Object doExplain(String dbName, String sql, ParameterHandler parms, int schemaRetryCount) {

    try {
      sql = sql.trim().substring("explain".length()).trim();
      String[] parts = sql.split(" ");
      if (!parts[0].trim().toLowerCase().equals("select")) {
        throw new DatabaseException("Verb not supported: verb=" + parts[0].trim());
      }

      CCJSqlParserManager parser = new CCJSqlParserManager();
      Statement statement = parser.parse(new StringReader(sql));
      SelectStatementImpl.Explain explain = new SelectStatementImpl.Explain();
      StatementHandler handler = statementHandlerFactory.getHandler(statement);
      return (ResultSet) handler.execute(dbName, parms, null, (Select) statement, explain, null, null, null, false, null, 0);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }


  public void populateOrderedKeyInfo(
      Map<String, ConcurrentSkipListMap<Object[], InsertStatementHandler.KeyInfo>> orderedKeyInfos,
      List<InsertStatementHandler.KeyInfo> keys) {
    for (final InsertStatementHandler.KeyInfo keyInfo : keys) {
      ConcurrentSkipListMap<Object[], InsertStatementHandler.KeyInfo> indexMap = orderedKeyInfos.get(keyInfo.getIndexSchema().getKey());
      if (indexMap == null) {
        indexMap = new ConcurrentSkipListMap<>(new Comparator<Object[]>() {
          @Override
          public int compare(Object[] o1, Object[] o2) {
            for (int i = 0; i < o1.length; i++) {
              int value = keyInfo.getIndexSchema().getValue().getComparators()[i].compare(o1[i], o2[i]);
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
        orderedKeyInfos.put(keyInfo.getIndexSchema().getKey(), indexMap);
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



  private static ConcurrentHashMap<Long, Integer> addedRecords = new ConcurrentHashMap<>();

  public byte[] checkAddedRecords(String command, byte[] body) {
    logger.info("begin checkAddedRecords");
    for (int i = 0; i < 1000000; i++) {
      if (addedRecords.get((long) i) == null) {
        logger.error("missing record: id=" + i + ", count=0");
      }
    }
    logger.info("finished checkAddedRecords");
    return null;
  }





  public long allocateId(String dbName) {
    long id = -1;
    synchronized (idAllocatorLock) {
      if (nextId.get() != -1 && nextId.get() <= maxAllocatedId.get()) {
        id = nextId.getAndIncrement();
      }
      else {
        ComObject cobj = new ComObject();
        cobj.put(ComObject.Tag.dbName, dbName);
        cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
        cobj.put(ComObject.Tag.method, "allocateRecordIds");
        byte[] ret = sendToMaster(cobj);
        ComObject retObj = new ComObject(ret);
        nextId.set(retObj.getLong(ComObject.Tag.nextId));
        maxAllocatedId.set(retObj.getLong(ComObject.Tag.maxId));
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
    cobj.put(ComObject.Tag.dbName, dbName);
    cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
    cobj.put(ComObject.Tag.method, "isRepartitioningComplete");
    byte[] bytes = sendToMaster(cobj);
    ComObject retObj = new ComObject(bytes);
    return retObj.getBoolean(ComObject.Tag.finished);
  }

  public long getPartitionSize(String dbName, int shard, String tableName, String indexName) {
    return getPartitionSize(dbName, shard, 0, tableName, indexName);
  }

  public long getPartitionSize(String dbName, int shard, int replica, String tableName, String indexName) {
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.dbName, dbName);
    cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
    cobj.put(ComObject.Tag.tableName, tableName);
    cobj.put(ComObject.Tag.indexName, indexName);
    cobj.put(ComObject.Tag.method, "getPartitionSize");
    byte[] bytes = send(null, shard, replica, cobj, DatabaseClient.Replica.specified);
    ComObject retObj = new ComObject(bytes);
    return retObj.getLong(ComObject.Tag.size);
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
      int prevVersion = common.getSchemaVersion();
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.dbName, "__none__");
      cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
      cobj.put(ComObject.Tag.method, "getSchema");
      try {

        byte[] ret = null;
        try {
          ret = sendToMaster(cobj);
        }
        catch (Exception e) {
          logger.error("Error getting schema from master", e);
        }
        if (ret == null) {
          int masterReplica = common.getServersConfig().getShards()[0].getMasterReplica();
          for (int replica = 0; replica < getReplicaCount(); replica++) {
            if (replica == masterReplica) {
              continue;
            }
            if (common.getServersConfig().getShards()[0].getReplicas()[replica].isDead()) {
              continue;
            }
            try {
              ret = send(null, 0, replica, cobj, Replica.specified);
              break;
            }
            catch (Exception e) {
              logger.error("Error getting schema from replica: replica=" + replica, e);
            }
          }
        }
        if (ret == null) {
          logger.error("Error getting schema from any replica");
        }
        else {
          ComObject retObj = new ComObject(ret);
          common.deserializeSchema(retObj.getByteArray(ComObject.Tag.schemaBytes));

          if ((serverVersion != null && common.getSchemaVersion() < serverVersion)) {
            try {
              cobj.put(ComObject.Tag.force, true);
              ret = sendToMaster(cobj);
              retObj = new ComObject(ret);
              common.deserializeSchema(retObj.getByteArray(ComObject.Tag.schemaBytes));
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
      long auth_user = rand.nextLong();
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.dbName, "__none__");
      cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
      cobj.put(ComObject.Tag.method, "getConfig");

      byte[] ret = send(null, selectShard(0), auth_user, cobj, DatabaseClient.Replica.def);
      ComObject retObj = new ComObject(ret);
      common.deserializeConfig(retObj.getByteArray(ComObject.Tag.configBytes));
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public void beginRebalance(String dbName, String tableName, String indexName) {
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.dbName, dbName);
    cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
    cobj.put(ComObject.Tag.method, "beginRebalance");
    cobj.put(ComObject.Tag.force, false);
    sendToMaster(cobj);
  }
}
