package com.lowryengineering.database.client;

import com.codahale.metrics.MetricRegistry;
import com.lowryengineering.database.common.DatabaseCommon;
import com.lowryengineering.database.common.Record;
import com.lowryengineering.database.common.SchemaOutOfSyncException;
import com.lowryengineering.database.index.Repartitioner;
import com.lowryengineering.database.jdbcdriver.ParameterHandler;
import com.lowryengineering.database.jdbcdriver.QueryType;
import com.lowryengineering.database.query.BinaryExpression;
import com.lowryengineering.database.query.*;
import com.lowryengineering.database.query.impl.*;
import com.lowryengineering.database.schema.*;
import com.lowryengineering.database.server.DatabaseServer;
import com.lowryengineering.database.server.ReadManager;
import com.lowryengineering.database.server.SnapshotManager;
import com.lowryengineering.database.socket.DatabaseSocketClient;
import com.lowryengineering.database.util.DataUtil;
import com.lowryengineering.database.util.JsonDict;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * User: lowryda
 * Date: 1/3/14
 * Time: 7:10 PM
 */
public class DatabaseClient {
  private final boolean isClient;
  private Server[][] servers;
  private DatabaseCommon common = new DatabaseCommon();
  private ThreadPoolExecutor executor = new ThreadPoolExecutor(128, 128, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

  private static Logger logger = LoggerFactory.getLogger(DatabaseClient.class);

  private int pageSize = ReadManager.SELECT_PAGE_SIZE;

  private Set<String> write_verbs = new HashSet<String>();
  private static String[] write_verbs_array = new String[]{
      "insert",
      "dropTable",
      "dropIndex",
      "dropIndexSlave",
      "doCreateIndex",
      "createIndex",
      "createIndexSlave",
      "createTable",
      "createTableSlave",
      "createDatabase",
      "delete",
      "deleteRecord",
      "deleteIndexEntryByKey",
      "deleteIndexEntry",
      "updateRecord",
      "insertIndexEntryByKey",
      "insertIndexEntryByKeyWithRecord",
      "removeRecord",
      "beginRebalance",
      "updateServersConfig",
      "deleteRecord",
      "allocateRecordIds",
      "reserveNextId",
      "updateSchema",
      "beginRebalanceOrderedIndex",
      "moveIndexEntries",
      "rebalanceOrderedIndex",
      "notifyDeletingComplete",
      "notifyRepartitioningComplete",
      "notifyRepartitioningRecordsByIdComplete",
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

  };

  private static Set<String> writeVerbs = new HashSet<String>();

  public Set<String> getWrite_verbs() {
    return write_verbs;
  }

  public static String[] getWrite_verbs_array() {
    return write_verbs_array;
  }

  public static Set<String> getWriteVerbs() {
    return writeVerbs;
  }

  static {
    for (String verb : write_verbs_array) {
      writeVerbs.add(verb);
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

  public int getShardCount() {
    return servers.length;
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

  private ThreadLocal<Boolean> isExplicitTrans = new ThreadLocal<>();
  private ThreadLocal<Boolean> isCommitting = new ThreadLocal<>();
  private ThreadLocal<Long> transactionId = new ThreadLocal<>();
  private ThreadLocal<List<TransactionOperation>> transactionOps = new ThreadLocal<>();

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
    List<TransactionOperation> ops = transactionOps.get();
    for (TransactionOperation op : ops) {
      op.statement.setParms(op.parms);
      op.statement.execute(dbName, explain);
    }

    isExplicitTrans.set(false);
    transactionOps.set(null);
    isCommitting.set(false);
    transactionId.set(null);
  }

  public void rollback() {
    isExplicitTrans.set(false);
    transactionOps.set(null);
    isCommitting.set(false);
    transactionId.set(null);
  }

  public int getReplicaCount() {
    return servers[0].length;
  }

  public void createDatabase(String dbName) {
    dbName = dbName.toLowerCase();

    String command = "DatabaseServer:createDatabase:1:1:" + dbName + ":master";

    send(null, 0, 0, command, null, DatabaseClient.Replica.master);
  }

  public String debugRecord(String dbName, String tableName, String indexName, String key) {
    while (true) {
      try {
        logger.info("Debug record: dbName=" + dbName + ", table=" + tableName + ", index=" + indexName + ", key=" + key);
        StringBuilder builder = new StringBuilder();
        TableSchema tableSchema = common.getTables(dbName).get(tableName);
        IndexSchema indexSchema = tableSchema.getIndexes().get(indexName);
        String columnName = indexSchema.getFields()[0];
        List<ColumnImpl> columns = new ArrayList<>();
        for (FieldSchema field : tableSchema.getFields()) {
          ColumnImpl column = new ColumnImpl();
          column.setTableName(tableName);
          column.setColumnName(field.getName());
          column.setDbName(dbName);
          columns.add(column);
        }
        key = key.replaceAll("\\[", "");
        key = key.replaceAll("\\]", "");
        String[] parts = key.split(",");
        Object[] keyObj = new Object[parts.length];
        for (int i = 0; i < parts.length; i++) {
          String fieldName = indexSchema.getFields()[i];
          int offset = tableSchema.getFieldOffset(fieldName);
          FieldSchema field = tableSchema.getFields().get(offset);
          keyObj[i] = field.getType().getConverter().convert(parts[i]);
        }
        ExpressionImpl.RecordCache recordCache = new ExpressionImpl.RecordCache();
        ParameterHandler parms = new ParameterHandler();
        AtomicReference<String> usedIndex = new AtomicReference<>();

        for (int shard = 0; shard < getShardCount(); shard++) {
          for (int replica = 0; replica < getReplicaCount(); replica++) {
            String port = servers[shard][replica].hostPort;
            logger.info("calling server: port=" + port);
            boolean forceSelectOnServer = false;
            SelectContextImpl context = ExpressionImpl.lookupIds(dbName, common, this, replica, 1, tableSchema, indexSchema, forceSelectOnServer, BinaryExpression.Operator.equal, null, null, keyObj, parms,
                null, null, keyObj, null, columns, columnName, shard, recordCache, usedIndex, false, common.getSchemaVersion(), null, null, false);
            Object[][][] keys = context.getCurrKeys();
            if (keys != null && keys.length > 0 && keys[0].length > 0 && keys[0][0].length > 0) {
              builder.append("[shard=" + shard + ", replica=" + replica + "]");
            }
          }
        }
        if (builder.length() == 0) {
          builder.append("[not found]");
        }
        return builder.toString();
      }
      catch (SchemaOutOfSyncException e) {
        continue;
      }
      catch (Exception e) {
        logger.error("Error debugging record", e);
        break;
      }
    }
    return "[not found]";
  }

  public static class ReconfigureResults {
    private boolean handedOffToMaster;
    private int shardCount;

    public ReconfigureResults(boolean handedOffToMaster, int shardCount) {
      this.handedOffToMaster = handedOffToMaster;
      this.shardCount = shardCount;
    }

    public boolean isHandedOffToMaster() {
      return handedOffToMaster;
    }

    public int getShardCount() {
      return shardCount;
    }
  }

  public ReconfigureResults reconfigureCluster() {
    try {
      String command = "DatabaseServer:healthCheck:1:1:__none__";

      try {
        byte[] bytes = send(null, 0, 0, command, null, DatabaseClient.Replica.master);
        if (new String(bytes, "utf-8").equals("{\"status\" : \"ok\"}")) {
          command = "DatabaseServer:reconfigureCluster:1:1:__none__";
          bytes = send(null, 0, 0, command, null, DatabaseClient.Replica.master);
          DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
          int count = in.readInt();
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

    public byte[] do_send(String batchKey, String command, byte[] body) {
      return socketClient.do_send(batchKey, command, body, hostPort);
    }
  }

  public byte[] do_send(List<DatabaseSocketClient.Request> requests) {
    return DatabaseSocketClient.do_send(requests);
  }

  public DatabaseClient(String host, int port, boolean isClient) {
    servers = new Server[1][];
    servers[0] = new Server[1];
    servers[0][0] = new Server(host, port);
    this.isClient = isClient;

    syncConfig();

    configureServers();

    new java.util.Timer().scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        System.out.println("IndexLookup stats: count=" + INDEX_LOOKUP_STATS.getCount() + ", rate=" + INDEX_LOOKUP_STATS.getFiveMinuteRate() +
            ", durationAvg=" + INDEX_LOOKUP_STATS.getSnapshot().getMean() / 1000000d +
            ", duration99.9=" + INDEX_LOOKUP_STATS.getSnapshot().get999thPercentile() / 1000000d);
        System.out.println("BatchIndexLookup stats: count=" + BATCH_INDEX_LOOKUP_STATS.getCount() + ", rate=" + BATCH_INDEX_LOOKUP_STATS.getFiveMinuteRate() +
            ", durationAvg=" + BATCH_INDEX_LOOKUP_STATS.getSnapshot().getMean() / 1000000d +
            ", duration99.9=" + BATCH_INDEX_LOOKUP_STATS.getSnapshot().get999thPercentile() / 1000000d);
        System.out.println("BatchIndexLookup stats: count=" + JOIN_EVALUATE.getCount() + ", rate=" + JOIN_EVALUATE.getFiveMinuteRate() +
            ", durationAvg=" + JOIN_EVALUATE.getSnapshot().getMean() / 1000000d +
            ", duration99.9=" + JOIN_EVALUATE.getSnapshot().get999thPercentile() / 1000000d);
      }
    }, 20 * 1000, 20 * 1000);

    for (String verb : write_verbs_array) {
      write_verbs.add(verb);
    }

  }

  private static final MetricRegistry METRICS = new MetricRegistry();

  public static final com.codahale.metrics.Timer INDEX_LOOKUP_STATS = METRICS.timer("indexLookup");
  public static final com.codahale.metrics.Timer BATCH_INDEX_LOOKUP_STATS = METRICS.timer("batchIndexLookup");
  public static final com.codahale.metrics.Timer JOIN_EVALUATE = METRICS.timer("joinEvaluate");

  public void configureServers() {
    DatabaseServer.ServersConfig serversConfig = common.getServersConfig();

    boolean isPrivate = !isClient || serversConfig.clientIsInternal();

    DatabaseServer.Shard[] shards = serversConfig.getShards();

    List<Thread> threads = new ArrayList<>();
    if (servers != null) {
      for (Server[] server : servers) {
        for (Server innerServer : server) {
          threads.addAll(innerServer.getSocketClient().getBatchThreads());
        }
      }
    }
    servers = new Server[shards.length][];
    for (int i = 0; i < servers.length; i++) {
      DatabaseServer.Shard shard = shards[i];
      servers[i] = new Server[shard.getReplicas().length];
      for (int j = 0; j < servers[i].length; j++) {
        DatabaseServer.Host replicaHost = shard.getReplicas()[j];
        servers[i][j] = new Server(isPrivate ? replicaHost.getPrivateAddress() : replicaHost.getPublicAddress(), replicaHost.getPort());
      }
    }
    for (Thread thread : threads) {
      thread.interrupt();
    }
  }

  private void syncConfig() {

    String command = "DatabaseServer:getConfig:1:1:null";
    try {
      byte[] ret = send(null, 0, 0, command, null, Replica.master);
      if (ret != null) {
        common.deserializeConfig(new DataInputStream(new ByteArrayInputStream(ret)));

        logger.info("Client received config from server");
      }
    }
    catch (Exception t) {
      throw new DatabaseException(t);
    }

  }

  public void initDb(String dbName) {
    while (true) {
      try {
        syncSchema();
        break;
      }
      catch (Exception e) {
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
      final long auth_user, final String command, final byte[] body, final Replica replica) {
    List<Future<byte[]>> futures = new ArrayList<Future<byte[]>>();
    try {
      for (int i = 0; i < servers.length; i++) {
        final int shard = i;
        futures.add(executor.submit(new Callable<byte[]>() {
          @Override
          public byte[] call() {
            return send(batchKey, shard, auth_user, command, body, replica);
          }
        }));
      }
      byte[][] ret = new byte[futures.size()][];
      for (int i = 0; i < futures.size(); i++) {
        ret[i] = futures.get(i).get(120000000, TimeUnit.MILLISECONDS);
      }
      return ret;
    }
    catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new DatabaseException(e);
    }
    finally {
      for (Future future : futures) {
        executor.getQueue().remove(future);
        future.cancel(true);
      }
    }

//    String[] ret = new String[shardCount];
//    for (int i = 0; i < shardCount; i++) {
//      ret[i] = send(i, auth_user, command, replica, timeout);
//    }
//    return ret;
  }

  public byte[] send(String batchKey,
      int shard, long auth_user, String command, byte[] body, Replica replica) {
//    DatabaseServer server = DatabaseServer.getServers().get(shard).get(0);
//    while (true) {
//      try {
//        if (server != null) {
//          return server.handleCommand(command, body, false);
//        }

    return send(batchKey, servers[shard], shard, auth_user, command, body, replica);
//      }
//      catch (Exception e) {
//        command = handleSchemaOutOfSyncException(command, e);
//      }
//    }
  }

  private String handleSchemaOutOfSyncException(String command, Exception e) {
    try {
      String[] parts = command.split(":");
      String dbName = parts[4];
      int previousVersion = common.getSchemaVersion();
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
      if (!schemaOutOfSync) {
        throw e;
      }
      synchronized (this) {
        Integer serverVersion = null;
        int pos = msg.indexOf("currVer:");
        if (pos != -1) {
          int pos2 = msg.indexOf(":", pos + "currVer:".length());
          serverVersion = Integer.valueOf(msg.substring(pos + "currVer:".length(), pos2));
        }

        if (serverVersion == null || serverVersion > common.getSchemaVersion()) {
          //logger.info("Schema out of sync: currVersion=" + common.getSchemaVersion());
          syncSchema();
        }
      }

      //if (previousVersion < common.getSchemaVersion()) {
        throw new SchemaOutOfSyncException();
      //}

//      String newCommand = "";
//      String[] parts = command.split(":");
//      for (int i = 0; i < parts.length; i++) {
//        if (i != 0) {
//          newCommand += ":";
//        }
//        if (i == 3) {
//          newCommand += common.getSchema().getVersion();
//        }
//        else {
//          newCommand += parts[i];
//        }
//      }
//      return newCommand;
    }
    catch (SchemaOutOfSyncException e1) {
      throw e1;
    }
    catch (Exception e1) {
      throw new DatabaseException(e1);
    }
  }

  public byte[] send(
      String batchKey, Server[] replicas, int shard, long auth_user,
      String command, byte[] body, Replica replica) {
    try {
      String localCommand = command;
      int pos = localCommand.indexOf(":");
      int pos2 = localCommand.indexOf(":", pos + 1);
      String verb = localCommand.substring(pos + 1, pos2);

      byte[] ret = null;
      for (int attempt = 0; attempt < 1; attempt++) {
        try {
          outer:
          while (true) {
            for (Server server : replicas) {
              if (!server.dead) {
                break outer;
              }
              Thread.sleep(1000);
            }
          }
          if (write_verbs.contains(verb)) {
            while (true) {
              int liveCount = 0;
              for (Server server : replicas) {
                if (!server.dead) {
                  liveCount++;
                }
              }
              if (liveCount >= getReplicaCount()) {
                break;
              }
              Thread.sleep(1000);
            }
          }

          if (replica == Replica.all) {
            try {
              boolean local = false;
              List<DatabaseSocketClient.Request> requests = new ArrayList<>();
              for (int i = 0; i < replicas.length; i++) {
                Server server = replicas[i];
                DatabaseServer dbserver = getLocalDbServer(shard, i);
                if (dbserver != null) {
                  local = true;
                  ret = dbserver.handleCommand(localCommand, body, false, true);
                }
                else {
                  DatabaseSocketClient.Request request = new DatabaseSocketClient.Request();
                  request.setBatchKey(batchKey);
                  request.setCommand(localCommand);
                  request.setBody(body);
                  request.setHostPort(server.hostPort);
                  request.setSocketClient(server.socketClient);
                  requests.add(request);
                }
              }
              if (!local) {
                ret = DatabaseSocketClient.do_send(requests);
              }
              return ret;
            }
            catch (Exception e) {
              try {
                localCommand = handleSchemaOutOfSyncException(localCommand, e);
              }
              catch (Exception t) {
                throw t;
              }
            }
          }
          else if (replica == Replica.master) {
            DatabaseServer dbserver = getLocalDbServer(shard, 0);
            try {
              if (dbserver != null) {
                return dbserver.handleCommand(localCommand, body, false, true);
              }
              return replicas[0].do_send(batchKey, localCommand, body);
              //todo: make master dynamic
            }
            catch (Exception e) {
              try {
                localCommand = handleSchemaOutOfSyncException(localCommand, e);
              }
              catch (Exception t) {
                throw t;
              }
            }
          }
          else if (replica == Replica.specified) {
            DatabaseServer dbserver = getLocalDbServer(shard, (int) auth_user);
            try {
              if (dbserver != null) {
                return dbserver.handleCommand(localCommand, body, false, true);
              }
              return replicas[(int) auth_user].do_send(batchKey, localCommand, body);
            }
            catch (Exception e) {
              try {
                localCommand = handleSchemaOutOfSyncException(localCommand, e);
              }
              catch (Exception t) {
                throw t;
              }
            }
          }
          else if (replica == Replica.def) {
            if (write_verbs.contains(verb)) {
              int successCount = 0;
              try {
                for (int i = 0; i < replicas.length; i++) {
                  Server currReplica = replicas[i];
                  DatabaseServer dbserver = getLocalDbServer(shard, i);
                  if (dbserver != null) {
                    ret = dbserver.handleCommand(localCommand, body, false, true);
                  }
                  else {
                    ret = currReplica.do_send(batchKey, localCommand, body);
                  }
                  successCount++;
                }
                if (successCount < 2) {
                  throw new SocketException("Failed to send update to 2 or more replicas: command=" + localCommand);
                }
                return ret;
              }
              catch (Exception e) {
                try {
                  localCommand = handleSchemaOutOfSyncException(localCommand, e);
                }
                catch (Exception t) {
                  logger.error("Error synching schema", t);
                  logger.error("Error sending request", e);
                  throw t;
                }
              }
            }
            else {
              boolean success = false;
              for (long rand = auth_user; rand < auth_user + replicas.length; rand++) {
                int replicaOffset = Math.abs((int) (rand % replicas.length));
                if (!replicas[replicaOffset].dead) {
                  try {
                    DatabaseServer dbserver = getLocalDbServer(shard, replicaOffset);
                    if (dbserver != null) {
                      return dbserver.handleCommand(localCommand, body, false, true);
                    }
                    else {
                      return replicas[replicaOffset].do_send(batchKey, localCommand, body);
                    }
                    //success = true;
                  }
                  catch (Exception e) {
                    try {
                      localCommand = handleSchemaOutOfSyncException(localCommand, e);
                      rand--;
                    }
                    catch (SchemaOutOfSyncException s) {
                      throw s;
                    }
                    catch (Exception t) {
                      logger.error("Error synching schema", t);
                      logger.error("Error sending request", e);
                      throw t;
                    }
                  }
                }
              }
              if (!success) {
                throw new SocketException("Failed to send to any replica: command=" + localCommand);
              }
            }
          }
          //return ret;
          if (attempt == 9) {
            throw new DatabaseException("Error sending message");
          }
        }
        catch (SocketException e) {
          logger.error("Error sending message - will retry:", e);
          if (attempt == 0) {
            throw new DatabaseException(e);
          }
        }
      }
    }
    catch (InterruptedException e) {
      throw new DatabaseException(e);
    }
    return null;
  }

  private DatabaseServer getLocalDbServer(int shard, int replica) {
    Map<Integer, Map<Integer, DatabaseServer>> dbServers = DatabaseServer.getServers();
    DatabaseServer dbserver = null;
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

  public void doCreateIndex(String dbName, CreateIndexStatementImpl statement) throws IOException {
    String command = "DatabaseServer:createIndex:1:" + common.getSchemaVersion() + ":" + dbName + ":master:" + statement.getTableName() + ":" + statement.getName();
    StringBuilder builder = new StringBuilder();
    boolean first = true;
    for (String field : statement.getColumns()) {
      if (!first) {
        builder.append(",");
      }
      first = false;
      builder.append(field);
    }
    command = command + ":" + builder.toString();

    byte[] ret = send(null, 0, rand.nextLong(), command, null, DatabaseClient.Replica.master);
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(ret));
    long serializationVersion = DataUtil.readVLong(in);
    common.deserializeSchema(common, in);
  }


  private static class StatementCacheEntry {
    private AtomicLong whenUsed = new AtomicLong();
    private Statement statement;

  }

  private ConcurrentHashMap<String, StatementCacheEntry> statementCache = new ConcurrentHashMap<>();

  public Object executeQuery(String dbName, QueryType queryType, String sql, ParameterHandler parms) throws SQLException {
    return executeQuery(dbName, queryType, sql, parms, false);
  }

  public Object executeQuery(String dbName, QueryType queryType, String sql, ParameterHandler parms, boolean debug) throws SQLException {
    while (true) {
      try {
        Statement statement;
        if (sql.toLowerCase().startsWith("describe")) {
          return doDescribe(dbName, sql);
        }
        else if (sql.toLowerCase().startsWith("explain")) {
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
          if (statement instanceof Select) {
            return doSelect(dbName, parms, (Select) statement, debug, null);
          }
          else if (statement instanceof Insert) {
            return doInsert(dbName, parms, (Insert) statement);
          }
          else if (statement instanceof Update) {
            return doUpdate(dbName, parms, (Update) statement);
          }
          else if (statement instanceof CreateTable) {
            return doCreateTable(dbName, (CreateTable) statement);
          }
          else if (statement instanceof CreateIndex) {
            return doCreateIndex(dbName, (CreateIndex) statement);
          }
          else if (statement instanceof Delete) {
            return doDelete(dbName, parms, (Delete) statement);
          }
          else if (statement instanceof Alter) {
            return doAlter(dbName, parms, (Alter) statement);
          }
          else if (statement instanceof Drop) {
            return doDrop(dbName, statement);
          }
          else if (statement instanceof Truncate) {
            return doTruncateTable(dbName, (Truncate) statement);
          }
        }
      }
      catch (SchemaOutOfSyncException e) {
        continue;
      }
      catch (Exception e) {
        throw new SQLException(e);
      }
    }
  }

  private Object doExplain(String dbName, String sql, ParameterHandler parms) {

    try {
      sql = sql.trim().substring("explain".length()).trim();
      String[] parts = sql.split(" ");
      if (!parts[0].trim().toLowerCase().equals("select")) {
        throw new DatabaseException("Verb not supported: verb=" + parts[0].trim());
      }

      CCJSqlParserManager parser = new CCJSqlParserManager();
      Statement statement = parser.parse(new StringReader(sql));
      SelectStatementImpl.Explain explain = new SelectStatementImpl.Explain();
      return (ResultSet) doSelect(dbName, parms, (Select) statement, false, explain);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private ResultSet doDescribe(String dbName, String sql) {
    String[] parts = sql.split(" ");
    if (parts[1].trim().equalsIgnoreCase("table")) {
      String table = parts[2].trim().toLowerCase();
      TableSchema tableSchema = common.getTables(dbName).get(table);
      if (tableSchema == null) {
        throw new DatabaseException("Table not defined: dbName=" + dbName + ", tableName=" + table);
      }
      List<FieldSchema> fields = tableSchema.getFields();
      int maxLen = 0;
      int maxTypeLen = 0;
      int maxWidthLen = 0;
      for (FieldSchema field : fields) {
        maxLen = Math.max("Name".length(), Math.max(field.getName().length(), maxLen));
        maxTypeLen = Math.max("Type".length(), Math.max(field.getType().name().length(), maxTypeLen));
        maxWidthLen = Math.max("Width".length(), Math.max(String.valueOf(field.getWidth()).length(), maxWidthLen));
      }

      int totalWidth = "| ".length() + maxLen + " | ".length() + maxTypeLen + " | ".length() + maxWidthLen + " |".length();

      StringBuilder builder = new StringBuilder();

      appendChars(builder, "-", totalWidth);
      builder.append("\n");

      builder.append("| Name");
      appendChars(builder, " ", maxLen - "Name".length());
      builder.append(" | Type");
      appendChars(builder, " ", maxTypeLen - "Type".length());
      builder.append(" | Width");
      appendChars(builder, " ", maxWidthLen - "Width".length());
      builder.append(" |\n");
      appendChars(builder, "-", totalWidth);
      builder.append("\n");
      for (FieldSchema field : fields) {
        if (field.getName().equals("_id")) {
          continue;
        }
        builder.append("| ");
        builder.append(field.getName());
        appendChars(builder, " ", maxLen - field.getName().length());
        builder.append(" | ");
        builder.append(field.getType().name());
        appendChars(builder, " ", maxTypeLen - field.getType().name().length());
        builder.append(" | ");
        builder.append(String.valueOf(field.getWidth()));
        appendChars(builder, " ", maxWidthLen - String.valueOf(field.getWidth()).length());
        builder.append(" |\n");
      }
      appendChars(builder, "-", totalWidth);
      builder.append("\n");

      for (IndexSchema indexSchema : tableSchema.getIndexes().values()) {
        builder.append("Index=").append(indexSchema.getName()).append("\n");
        doDescribeOneIndex(tableSchema, indexSchema, builder);
      }

      String ret = builder.toString();
      String[] lines = ret.split("\\n");
      return new ResultSetImpl(lines);
    }
    else if (parts[1].trim().equalsIgnoreCase("index")) {
      String str = parts[2].trim().toLowerCase();
      String[] innerParts = str.split("\\.");
      String table = innerParts[0].toLowerCase();
      if (innerParts.length == 1) {
        throw new DatabaseException("Must specify <table name>.<index name>");
      }
      String index = innerParts[1].toLowerCase();
      StringBuilder builder = new StringBuilder();
      doDescribeIndex(dbName, table, index, builder);

      String ret = builder.toString();
      String[] lines = ret.split("\\n");
      return new ResultSetImpl(lines);
    }
    else {
      throw new DatabaseException("Unknown target for describe: target=" + parts[1]);
    }

  }

  private StringBuilder doDescribeIndex(String dbName, String table, String index, StringBuilder builder) {
    TableSchema tableSchema = common.getTables(dbName).get(table);
    if (tableSchema == null) {
      throw new DatabaseException("Table not defined: dbName=" + dbName + ", tableName=" + table);
    }

    int countFound = 0;
    for (IndexSchema indexSchema : tableSchema.getIndices().values()) {
      if (!indexSchema.getName().contains(index)) {
        continue;
      }
      countFound++;
      doDescribeOneIndex(tableSchema, indexSchema, builder);

    }
    if (countFound == 0) {
      throw new DatabaseException("Index not defined: dbName=" + dbName + ", tableName=" + table + ", indexName=" + index);
    }
    return builder;
  }

  private void doDescribeOneIndex(TableSchema tableSchema, IndexSchema indexSchema, StringBuilder builder) {
    String[] fields = indexSchema.getFields();
    int maxLen = 0;
    int maxTypeLen = 0;
    int maxWidthLen = 0;
    for (String field : fields) {
      FieldSchema fieldSchema = tableSchema.getFields().get(tableSchema.getFieldOffset(field));
      maxLen = Math.max("Name".length(), Math.max(fieldSchema.getName().length(), maxLen));
      maxTypeLen = Math.max("Type".length(), Math.max(fieldSchema.getType().name().length(), maxTypeLen));
      maxWidthLen = Math.max("Width".length(), Math.max(String.valueOf(fieldSchema.getWidth()).length(), maxWidthLen));
    }

    int totalWidth = "| ".length() + maxLen + " | ".length() + maxTypeLen + " | ".length() + maxWidthLen + " |".length();

    appendChars(builder, "-", totalWidth);
    builder.append("\n");

    builder.append("| Name");
    appendChars(builder, " ", maxLen - "Name".length());
    builder.append(" | Type");
    appendChars(builder, " ", maxTypeLen - "Type".length());
    builder.append(" | Width");
    appendChars(builder, " ", maxWidthLen - "Width".length());
    builder.append(" |\n");
    appendChars(builder, "-", totalWidth);
    builder.append("\n");
    for (String field : fields) {
      FieldSchema fieldSchema = tableSchema.getFields().get(tableSchema.getFieldOffset(field));
      builder.append("| ");
      builder.append(fieldSchema.getName());
      appendChars(builder, " ", maxLen - fieldSchema.getName().length());
      builder.append(" | ");
      builder.append(fieldSchema.getType().name());
      appendChars(builder, " ", maxTypeLen - fieldSchema.getType().name().length());
      builder.append(" | ");
      builder.append(String.valueOf(fieldSchema.getWidth()));
      appendChars(builder, " ", maxWidthLen - String.valueOf(fieldSchema.getWidth()).length());
      builder.append(" |\n");
    }
    appendChars(builder, "-", totalWidth);
    builder.append("\n");
  }

  private void appendChars(StringBuilder builder, String character, int count) {
    for (int i = 0; i < count; i++) {
      builder.append(character);
    }
  }

  private Object doAlter(String dbName, ParameterHandler parms, Alter statement) throws IOException {
    String operation = statement.getOperation();
    String tableName = statement.getTable().getName();
    ColDataType type = statement.getDataType();
    String columnName = statement.getColumnName();

    if (operation.equalsIgnoreCase("add")) {
      doAddColumn(dbName, tableName, columnName, type);
    }
    else if (operation.equalsIgnoreCase("drop")) {
      doDropColumn(dbName, tableName, columnName);
    }
    return 1;
  }

  private void doDropColumn(String dbName, String tableName, String columnName) throws IOException {

    String command = "DatabaseServer:dropColumn:1:" + common.getSchemaVersion() + ":" + dbName + ":" + tableName + ":" + columnName + ":master";
    byte[] ret = send(null, 0, 0, command, null, DatabaseClient.Replica.master);
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(ret));
    long serializationVersion = DataUtil.readVLong(in);
    common.deserializeSchema(common, in);
  }

  private void doAddColumn(String dbName, String tableName, String columnName, ColDataType type) throws IOException {

    String command = "DatabaseServer:addColumn:1:" + common.getSchemaVersion() + ":" + dbName + ":" + tableName + ":" + columnName + ":" + type.getDataType() + ":master";
    byte[] ret = send(null, 0, 0, command, null, DatabaseClient.Replica.master);
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(ret));
    long serializationVersion = DataUtil.readVLong(in);
    common.deserializeSchema(common, in);
  }

  private Object doDrop(String dbName, Statement statement) throws IOException {
    Drop drop = (Drop) statement;
    if (drop.getType().equalsIgnoreCase("table")) {
      String table = drop.getName().getName().toLowerCase();
      doTruncateTable(dbName, table);

      String command = "DatabaseServer:dropTable:1:" + common.getSchemaVersion() + ":" + dbName + ":" + table + ":master";
      byte[] ret = send(null, 0, 0, command, null, DatabaseClient.Replica.master);
      DataInputStream in = new DataInputStream(new ByteArrayInputStream(ret));
      long serializationVersion = DataUtil.readVLong(in);
      common.deserializeSchema(common, in);
    }
    else if (drop.getType().equalsIgnoreCase("index")) {
      String indexName = drop.getName().getName().toLowerCase();
      String tableName = drop.getName().getSchemaName().toLowerCase();

      String command = "DatabaseServer:dropIndex:1:" + common.getSchemaVersion() + ":" + dbName + ":" + tableName + ":" + indexName + ":master";
      byte[] ret = send(null, 0, 0, command, null, DatabaseClient.Replica.master);
      DataInputStream in = new DataInputStream(new ByteArrayInputStream(ret));
      long serializationVersion = DataUtil.readVLong(in);
      common.deserializeSchema(common, in);
    }
    return 1;
  }

  private Object doTruncateTable(String dbName, Truncate statement) {
    String table = statement.getTable().getName();
    table = table.toLowerCase();

    doTruncateTable(dbName, table);

    return 1;
  }

  private void doTruncateTable(String dbName, String table) {

    String command = "DatabaseServer:truncateTable:1:" + common.getSchemaVersion() + ":" + dbName + ":" + table + ":secondary";

    Random rand = new Random(System.currentTimeMillis());
    sendToAllShards(null, rand.nextLong(), command, null, Replica.def);

    command = "DatabaseServer:truncateTable:1:" + common.getSchemaVersion() + ":" + dbName + ":" + table + ":primary";

    rand = new Random(System.currentTimeMillis());
    sendToAllShards(null, rand.nextLong(), command, null, Replica.def);
  }

  private Object doCreateIndex(String dbName, CreateIndex stmt) throws IOException {
    Index index = stmt.getIndex();
    String indexName = index.getName().toLowerCase();
    List<String> columnNames = index.getColumnsNames();
    Table table = stmt.getTable();
    String tableName = table.getName().toLowerCase();
    for (int i = 0; i < columnNames.size(); i++) {
      columnNames.set(i, columnNames.get(i).toLowerCase());
    }

    CreateIndexStatementImpl statement = new CreateIndexStatementImpl(this);
    statement.setName(indexName);
    statement.setTableName(tableName);
    statement.setColumns(columnNames);

    doCreateIndex(dbName, statement);

    return 1;
  }

  private Object doDelete(String dbName, ParameterHandler parms, Delete stmt) {
    DeleteStatementImpl deleteStatement = new DeleteStatementImpl(this);
    deleteStatement.setTableName(stmt.getTable().getName());

    Expression expression = stmt.getWhere();
    AtomicInteger currParmNum = new AtomicInteger();
    ExpressionImpl innerExpression = getExpression(currParmNum, expression, deleteStatement.getTableName(), parms);
    deleteStatement.setWhereClause(innerExpression);

    deleteStatement.setParms(parms);
    return deleteStatement.execute(dbName, null);
  }

  private int doCreateTable(String dbName, CreateTable stmt) {
    CreateTableStatementImpl createTableStatement = new CreateTableStatementImpl(this);
    createTableStatement.setTableName(stmt.getTable().getName());

    List<FieldSchema> fields = new ArrayList<>();
    List columnDefinitions = stmt.getColumnDefinitions();
    for (int i = 0; i < columnDefinitions.size(); i++) {
      ColumnDefinition columnDefinition = (ColumnDefinition) columnDefinitions.get(i);

      FieldSchema fieldSchema = new FieldSchema();
      fieldSchema.setName(columnDefinition.getColumnName().toLowerCase());
      fieldSchema.setType(DataType.Type.valueOf(columnDefinition.getColDataType().getDataType().toUpperCase()));
      if (columnDefinition.getColDataType().getArgumentsStringList() != null) {
        String width = columnDefinition.getColDataType().getArgumentsStringList().get(0);
        fieldSchema.setWidth(Integer.valueOf(width));
      }
      List specs = columnDefinition.getColumnSpecStrings();
      if (specs != null) {
        for (Object obj : specs) {
          if (obj instanceof String) {
            String spec = (String) obj;
            if (spec.toLowerCase().contains("auto_increment")) {
              fieldSchema.setAutoIncrement(true);
            }
            if (spec.toLowerCase().contains("array")) {
              fieldSchema.setArray(true);
            }
          }
        }
      }
      List argList = columnDefinition.getColDataType().getArgumentsStringList();
      if (argList != null) {
        int width = Integer.valueOf((String) argList.get(0));
        fieldSchema.setWidth(width);
      }
      //fieldSchema.setWidth(width);
      fields.add(fieldSchema);
    }

    List<String> primaryKey = new ArrayList<String>();
    List indexes = stmt.getIndexes();
    if (indexes == null) {
      primaryKey.add("_id");
    }
    else {
      for (int i = 0; i < indexes.size(); i++) {
        Index index = (Index) indexes.get(i);
        if (index.getType().equalsIgnoreCase("primary key")) {
          List columnNames = index.getColumnsNames();
          for (int j = 0; j < columnNames.size(); j++) {
            primaryKey.add((String) columnNames.get(j));
          }
        }
      }
    }

    createTableStatement.setFields(fields);
    createTableStatement.setPrimaryKey(primaryKey);

    return doCreateTable(dbName, createTableStatement);
  }

  public int doCreateTable(String dbName, CreateTableStatementImpl createTableStatement) {
    try {
      String command = "DatabaseServer:createTable:1:" + common.getSchemaVersion() + ":" + dbName + ":master:query0";
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytesOut);
      DataUtil.writeVLong(out, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
      createTableStatement.serialize(out);
      out.close();

      byte[] ret = send(null, 0, rand.nextLong(), command, bytesOut.toByteArray(), DatabaseClient.Replica.master);
      DataInputStream in = new DataInputStream(new ByteArrayInputStream(ret));
      long serializationVersion = DataUtil.readVLong(in);
      common.deserializeSchema(common, in);

      return 1;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }


  public Object doUpdate(String dbName, ParameterHandler parms, Update stmt) {
    UpdateStatementImpl updateStatement = new UpdateStatementImpl(this);
    AtomicInteger currParmNum = new AtomicInteger();
    //todo: support multiple tables?
    updateStatement.setTableName(stmt.getTables().get(0).getName());

    List<Column> columns = stmt.getColumns();
    for (Column column : columns) {
      updateStatement.addColumn(column);
    }
    List<Expression> expressions = stmt.getExpressions();
    for (Expression expression : expressions) {
      ExpressionImpl qExpression = getExpression(currParmNum, expression, updateStatement.getTableName(), parms);
      updateStatement.addSetExpression(qExpression);
    }

    ExpressionImpl whereExpression = getExpression(currParmNum, stmt.getWhere(), updateStatement.getTableName(), parms);
    updateStatement.setWhereClause(whereExpression);

    if (isExplicitTrans()) {
      List<TransactionOperation> ops = transactionOps.get();
      if (ops == null) {
        ops = new ArrayList<>();
        transactionOps.set(ops);
      }
      ops.add(new TransactionOperation(updateStatement, parms));
    }

    updateStatement.setParms(parms);
    return updateStatement.execute(dbName, null);
  }

  public void insertKey(String dbName, String tableName, KeyInfo keyInfo, String primaryKeyIndexName, Object[] primaryKey) {
    try {
      String command = "DatabaseServer:insertIndexEntryByKey:1:" + common.getSchemaVersion() + ":" + dbName;

      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytesOut);
      DataUtil.writeVLong(out, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
      out.writeUTF(tableName);
      out.writeUTF(keyInfo.indexSchema.getKey());
      out.writeBoolean(isExplicitTrans());
      out.writeBoolean(isCommitting());
      DataUtil.writeVLong(out, getTransactionId());
      byte[] keyBytes = DatabaseCommon.serializeKey(common.getTables(dbName).get(tableName), keyInfo.indexSchema.getKey(), keyInfo.key);
      DataUtil.ResultLength resultLength = new DataUtil.ResultLength();
      DataUtil.writeVLong(out, keyBytes.length, resultLength);
      out.write(keyBytes);
      byte[] primaryKeyBytes = DatabaseCommon.serializeKey(common.getTables(dbName).get(tableName), primaryKeyIndexName, primaryKey);
      DataUtil.writeVLong(out, primaryKeyBytes.length, resultLength);
      out.write(primaryKeyBytes);
      out.close();

      send("DatabaseServer:insertIndexEntryByKey", keyInfo.shard, rand.nextLong(), command, bytesOut.toByteArray(), DatabaseClient.Replica.all);
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  class FailedToInsertException extends RuntimeException {
    public FailedToInsertException(String msg) {
      super(msg);
    }
  }

  public void insertKeyWithRecord(String dbName, String tableName, KeyInfo keyInfo, Record record) {
    try {
      String command = "DatabaseServer:insertIndexEntryByKeyWithRecord:1:" + common.getSchemaVersion() + ":" + dbName;

      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytesOut);
      DataUtil.writeVLong(out, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
      out.writeUTF(tableName);
      out.writeUTF(keyInfo.indexSchema.getKey());
      DataUtil.writeVLong(out, record.getId());
      out.writeBoolean(isExplicitTrans());
      out.writeBoolean(isCommitting());
      DataUtil.writeVLong(out, getTransactionId());
      byte[] recordBytes = record.serialize(common);
      out.writeInt(recordBytes.length);
      out.write(recordBytes);
      out.write(DatabaseCommon.serializeKey(common.getTables(dbName).get(tableName), keyInfo.indexSchema.getKey(), keyInfo.key));
      out.close();

      int replicaCount = getReplicaCount();
      Exception lastException = null;
      //for (int i = 0; i < replicaCount; i++) {
        try {
          byte[] ret = send("DatabaseServer:insertIndexEntryByKeyWithRecord", keyInfo.shard, 0, command, bytesOut.toByteArray(), DatabaseClient.Replica.all);
          if (ret == null) {
            throw new FailedToInsertException("No response for key insert");
          }
          DataInputStream in = new DataInputStream(new ByteArrayInputStream(ret));
          long serializationVersion = DataUtil.readVLong(in);
          int retVal = in.readInt();
          if (retVal != 1) {
            throw new FailedToInsertException("Incorrect response from server: value=" + retVal);
          }
        }
        catch (Exception e) {
          lastException = e;
        }
      //}
      if (lastException != null) {
        if (lastException instanceof SchemaOutOfSyncException) {
          throw (SchemaOutOfSyncException)lastException;
        }
        throw new DatabaseException(lastException);
      }
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public void deleteKey(String dbName, String tableName, KeyInfo keyInfo, String primaryKeyIndexName, Object[] primaryKey) {
    try {
      String command = "DatabaseServer:deleteIndexEntryByKey:1:" + common.getSchemaVersion() + ":" + dbName + ":" + tableName + ":" + keyInfo.indexSchema.getKey() + ":" + primaryKeyIndexName
          + ":" + isExplicitTrans() + ":" + isCommitting() + ":" + getTransactionId();
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytesOut);
      DataUtil.writeVLong(out, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
      out.write(DatabaseCommon.serializeKey(common.getTables(dbName).get(tableName), keyInfo.indexSchema.getKey(), keyInfo.key));
      out.write(DatabaseCommon.serializeKey(common.getTables(dbName).get(tableName), primaryKeyIndexName, primaryKey));
      out.close();

      send("DatabaseServer:deleteIndexEntryByKey", keyInfo.shard, rand.nextLong(), command, bytesOut.toByteArray(), DatabaseClient.Replica.def);
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public void populateOrderedKeyInfo(
      Map<String, ConcurrentSkipListMap<Object[], KeyInfo>> orderedKeyInfos,
      List<KeyInfo> keys) {
    for (final KeyInfo keyInfo : keys) {
      ConcurrentSkipListMap<Object[], KeyInfo> indexMap = orderedKeyInfos.get(keyInfo.indexSchema.getKey());
      if (indexMap == null) {
        indexMap = new ConcurrentSkipListMap<>(new Comparator<Object[]>() {
          @Override
          public int compare(Object[] o1, Object[] o2) {
            for (int i = 0; i < o1.length; i++) {
              int value = keyInfo.indexSchema.getValue().getComparators()[i].compare(o1[i], o2[i]);
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
        orderedKeyInfos.put(keyInfo.indexSchema.getKey(), indexMap);
      }
      indexMap.put(keyInfo.key, keyInfo);
    }
  }

  private final Object idAllocatorLock = new Object();
  private final AtomicLong nextId = new AtomicLong(-1L);
  private final AtomicLong maxAllocatedId = new AtomicLong(-1L);

  static class TransactionOperation {
    private StatementImpl statement;
    private ParameterHandler parms;

    public TransactionOperation(StatementImpl statement, ParameterHandler parms) {
      this.statement = statement;
      this.parms = parms;
    }
  }

  public int doInsert(String dbName, ParameterHandler parms, Insert stmt) throws IOException, SQLException {
    final InsertStatementImpl insertStatement = new InsertStatementImpl(this);
    insertStatement.setTableName(stmt.getTable().getName());

    List<Object> values = new ArrayList<>();
    List<String> columnNames = new ArrayList<>();

    List srcColumns = stmt.getColumns();
    ExpressionList items = (ExpressionList) stmt.getItemsList();
    List srcExpressions = items.getExpressions();
    int parmOffset = 1;
    for (int i = 0; i < srcColumns.size(); i++) {
      Column column = (Column) srcColumns.get(i);
      columnNames.add(column.getColumnName().toLowerCase());
      Expression expression = (Expression) srcExpressions.get(i);
      //todo: this doesn't handle out of order fields
      if (expression instanceof JdbcParameter) {
        values.add(parms.getValue(parmOffset++));
      }
      else if (expression instanceof StringValue) {
        values.add(((StringValue) expression).getValue());
      }
      else if (expression instanceof LongValue) {
        values.add(((LongValue) expression).getValue());
      }
      else if (expression instanceof DoubleValue) {
        values.add(((DoubleValue) expression).getValue());
      }
      else {
        throw new DatabaseException("Unexpected column type: " + expression.getClass().getName());
      }

    }
    for (int i = 0; i < columnNames.size(); i++) {
      insertStatement.addValue(columnNames.get(i), values.get(i));
    }

    if (isExplicitTrans()) {
      List<TransactionOperation> ops = transactionOps.get();
      if (ops == null) {
        ops = new ArrayList<>();
        transactionOps.set(ops);
      }
      ops.add(new TransactionOperation(insertStatement, parms));
    }
    return doInsert(dbName, insertStatement, parms);

  }

  private ConcurrentHashMap<String, TableSchema> tableSchema = new ConcurrentHashMap<>();
  private long lastGotSchema = 0;

  private static ConcurrentHashMap<Long, Integer> addedRecords = new ConcurrentHashMap<>();

  public byte[] checkAddedRecords(String command, byte[] body) {
    logger.info("begin checkAddedRecords");
    for (int i = 0; i < 1000000; i++) {
      if (addedRecords.get((long)i) == null) {
        logger.error("missing record: id=" + i + ", count=0");
      }
    }
    logger.info("finished checkAddedRecords");
    return null;
  }

  public int doInsert(String dbName, InsertStatementImpl insertStatement, ParameterHandler parms) throws IOException, SQLException {
    int previousSchemaVersion = common.getSchemaVersion();

    List<String> columnNames;
    List<Object> values;

    long id = allocateId(dbName);

    String tableName = insertStatement.getTableName();

    TableSchema tableSchema = common.getTables(dbName).get(tableName);
    if (tableSchema == null) {
      throw new DatabaseException("Table does not exist: name=" + tableName);
    }

    long transId = 0;
    if (!isExplicitTrans.get()) {
      transId = allocateId(dbName);
    }
    else {
      transId = transactionId.get();
    }
    Record record = prepareRecordForInsert(insertStatement, tableSchema, id);
    record.setTransId(transId);
    record.setId(id);

    Object[] fields = record.getFields();
    columnNames = new ArrayList<>();
    values = new ArrayList<>();
    for (int i = 0; i < fields.length; i++) {
      values.add(fields[i]);
      columnNames.add(tableSchema.getFields().get(i).getName());
    }


    int primaryKeyCount = 0;
    List<KeyInfo> completed = new ArrayList<>();
    KeyInfo primaryKey = new KeyInfo();
    while (true) {
      try {
        tableSchema = common.getTables(dbName).get(tableName);

        List<KeyInfo> keys = getKeys(tableSchema, columnNames, values, id);
        if (keys.size() == 0) {
          throw new DatabaseException("key not generated for record to insert");
        }
        for (final KeyInfo keyInfo : keys) {
          if (keyInfo.indexSchema.getValue().isPrimaryKey()) {
            primaryKey.key = keyInfo.key;
            primaryKey.indexSchema = keyInfo.indexSchema;
            break;
          }
        }

//        if (keys.size() == 2 && tableName.equals("persons")) {
//          System.out.println("hey");
//        }
        outer:
        for (final KeyInfo keyInfo : keys) {
          previousSchemaVersion = common.getSchemaVersion();
          for (KeyInfo completedKey : completed) {
            Comparator[] comparators = keyInfo.indexSchema.getValue().getComparators();

            if (completedKey.indexSchema.getKey().equals(keyInfo.indexSchema.getKey()) &&
                DatabaseCommon.compareKey(comparators, completedKey.key, keyInfo.key) == 0
                &&
                completedKey.shard == keyInfo.shard
                ) {
              continue outer;
            }
          }

          if (keyInfo.indexSchema.getValue().isPrimaryKey()) {
            if (!keyInfo.currAndLastMatch) {
              if (keyInfo.isCurrPartition()) {
                record.setDbViewNumber(common.getSchemaVersion());
                record.setDbViewFlags(Record.DB_VIEW_FLAG_ADDING);
              }
              else {
                record.setDbViewFlags(Record.DB_VIEW_FLAG_DELETING);
                record.setDbViewNumber(common.getSchemaVersion() - 1);
              }
            }
            insertKeyWithRecord(dbName, insertStatement.getTableName(), keyInfo, record);

            primaryKeyCount++;
//            if (previousSchemaVersion != common.getSchemaVersion()) {
//              throw new SchemaOutOfSyncException();
//            }
            completed.add(keyInfo);
          }
          else {
            insertKey(dbName, insertStatement.getTableName(), keyInfo, primaryKey.indexSchema.getKey(), primaryKey.key);
            completed.add(keyInfo);
          }
          if (previousSchemaVersion != common.getSchemaVersion()) {
            throw new SchemaOutOfSyncException();
          }

        }
//        if (primaryKeyCount != 1) {
//          logger.info("Multiple primary keys: count=" + primaryKeyCount);
//        }
        break;
      }
      catch (SchemaOutOfSyncException e) {
        continue;
      }
      catch (FailedToInsertException e) {
        logger.error(e.getMessage());
        continue;
      }
    }

    if (primaryKeyCount == 0) {
      throw new DatabaseException("failed to insert");
    }
    //    for (Future future : futures) {
    //      future.get();
    //    }
//    if (previousSchemaVersion != common.getSchemaVersion()) {
//      throw new SchemaOutOfSyncException();
//    }

    return 1;
  }

  public long allocateId(String dbName) {
    try {
      long id = -1;
      synchronized (idAllocatorLock) {
        if (nextId.get() != -1 && nextId.get() <= maxAllocatedId.get()) {
          id = nextId.getAndIncrement();
        }
        else {
          String command = "DatabaseServer:allocateRecordIds:1:" + common.getSchemaVersion() + ":" + dbName;
          byte[] ret = send(null, 0, rand.nextLong(), command, null, Replica.master);
          ByteArrayInputStream bytesIn = new ByteArrayInputStream(ret);
          DataInputStream in = new DataInputStream(bytesIn);
          long serializationVersion = DataUtil.readVLong(in);
          nextId.set(in.readLong());
          maxAllocatedId.set(in.readLong());
          id = nextId.getAndIncrement();
        }
      }
      return id;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  private Record prepareRecordForInsert(
      InsertStatementImpl statement, TableSchema schema, long id) throws UnsupportedEncodingException, SQLException {
    Record record;
    FieldSchema fieldSchema;
    Object[] valuesToStore = new Object[schema.getFields().size()];

    List<String> columnNames = statement.getColumns();
    List<Object> values = statement.getValues();
    for (int i = 0; i < schema.getFields().size(); i++) {
      fieldSchema = schema.getFields().get(i);
      for (int j = 0; j < columnNames.size(); j++) {
        if (fieldSchema.getName().equals(columnNames.get(j))) {
          Object value = values.get(j);
//          //todo: this doesn't handle out of order fields
//          if (value instanceof com.foundationdb.sql.parser.ParameterNode) {
//            value = parms.getValue(parmNum);
//            parmNum++;
//          }
          //if (fieldSchema.getType().equals(DataType.Type.SMALLINT)) {

          value = fieldSchema.getType().getConverter().convert(value);

          if (fieldSchema.getWidth() != 0) {
            switch(fieldSchema.getType()) {
              case VARCHAR:
              case NVARCHAR:
              case LONGVARCHAR:
              case LONGNVARCHAR:
              case CLOB:
              case NCLOB:
                String str = new String((byte[])value, "utf-8");
                if (str.length() > fieldSchema.getWidth()) {
                  throw new SQLException("value too long: field=" + fieldSchema.getName() + ", width=" + fieldSchema.getWidth());
                }
                break;
              case VARBINARY:
              case LONGVARBINARY:
              case BLOB:
                if (((byte[])value).length > fieldSchema.getWidth()) {
                  throw new SQLException("value too long: field=" + fieldSchema.getName() + ", width=" + fieldSchema.getWidth());
                }
                break;
            }
          }
//          }
//          else if (value instanceof String) {
//            value = ((String)value).getBytes("utf-8");
//          }
//          else if (value instanceof byte[]) {
//            value = new Blob((byte[])value);
//          }
//          else if (value instanceof InputStream) {
//            ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
//            StreamUtils.copyStream((InputStream)value, bytesOut);
//            bytesOut.close();
//            value = new Blob(bytesOut.toByteArray());
//          }
          valuesToStore[i] = value;
          break;
        }
        else {
          if (fieldSchema.getName().equals("_id")) {
            valuesToStore[i] = id;
          }
        }
      }
      if (fieldSchema.isAutoIncrement()) {
//         String key = (tableName + "." + fieldSchema.getName());
//         SchemaManager.AutoIncrementValue value = autoIncrementValues.get(key);
//         if (value == null) {
//           value = new SchemaManager.AutoIncrementValue(fieldSchema.getType());
//           SchemaManager.AutoIncrementValue prevValue = autoIncrementValues.putIfAbsent(key, value);
//           if (prevValue != null) {
//             value = prevValue;
//           }
//         }
//         Object currValue = value.increment();
//         valuesToStore[i] = currValue;
//         if (fieldSchema.getName().equals("_id")) {
//           id = (long) currValue;
//         }
      }
    }
    record = new Record(schema);
    record.setFields(valuesToStore);

    return record;
  }

  public static class KeyInfo {
    private boolean currPartition;
    private Object[] key;
    private int shard;
    private Map.Entry<String, IndexSchema> indexSchema;
    public boolean currAndLastMatch;

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "EI_EXPOSE_REP", justification = "copying the returned data is too slow")
    public Object[] getKey() {
      return key;
    }

    public int getShard() {
      return shard;
    }

    public Map.Entry<String, IndexSchema> getIndexSchema() {
      return indexSchema;
    }

    public boolean isCurrPartition() {
      return currPartition;
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "EI_EXPOSE_REP2", justification = "copying the passed in data is too slow")
    @SuppressWarnings("PMD.ArrayIsStoredDirectly") //copying the passed in data is too slow
    public KeyInfo(int shard, Object[] key, Map.Entry<String, IndexSchema> indexSchema, boolean currPartition) {
      this.shard = shard;
      this.key = key;
      this.indexSchema = indexSchema;
      this.currPartition = currPartition;
    }

    public KeyInfo() {
    }

    public void setKey(Object[] key) {
      this.key = key;
    }

    public void setIndexSchema(Map.Entry<String, IndexSchema> indexSchema) {
      this.indexSchema = indexSchema;
    }
  }

  public List<KeyInfo> getKeys(TableSchema tableSchema, List<String> columnNames, List<Object> values, long id) {
    List<KeyInfo> ret = new ArrayList<>();
    for (Map.Entry<String, IndexSchema> indexSchema : tableSchema.getIndices().entrySet()) {
      String[] fields = indexSchema.getValue().getFields();
      boolean shouldIndex = true;
      for (int i = 0; i < fields.length; i++) {
        boolean found = false;
        for (int j = 0; j < columnNames.size(); j++) {
          if (fields[i].equals(columnNames.get(j))) {
            found = true;
            break;
          }
        }
        if (!found) {
          shouldIndex = false;
          break;
        }
      }
      if (shouldIndex) {
        String[] indexFields = indexSchema.getValue().getFields();
        int[] fieldOffsets = new int[indexFields.length];
        for (int i = 0; i < indexFields.length; i++) {
          fieldOffsets[i] = tableSchema.getFieldOffset(indexFields[i]);
        }
        TableSchema.Partition[] currPartitions = indexSchema.getValue().getCurrPartitions();
        TableSchema.Partition[] lastPartitions = indexSchema.getValue().getLastPartitions();

        Object[] key = new Object[indexFields.length];
        if (indexFields.length == 1 && indexFields[0].equals("_id")) {
          key[0] = id;
        }
        else {
          for (int i = 0; i < key.length; i++) {
            for (int j = 0; j < columnNames.size(); j++) {
              if (columnNames.get(j).equals(indexFields[i])) {
                key[i] = values.get(j);
              }
            }
          }
        }

//        if (//indexSchema.getValue().getLastPartitions() == null ||
//            this.tableSchema.get(tableSchema.getName()) == null ||
//            System.currentTimeMillis() - lastGotSchema > 2000) {
//          this.tableSchema.put(tableSchema.getName(), common.getTables().get(tableSchema.getName()));
//          lastGotSchema = System.currentTimeMillis();
//        }

        boolean keyIsNull = false;
        for (Object obj : key) {
          if (obj == null) {
            keyIsNull = true;
          }
        }

        if (!keyIsNull) {
          List<Integer> selectedShards = Repartitioner.findOrderedPartitionForRecord(true, false, fieldOffsets, common, tableSchema,
              indexSchema.getKey(), null, BinaryExpression.Operator.equal, null, key, null);
          //        List<Integer> selectedShards = new ArrayList<>();
          //        selectedShards.add(0);
          //        selectedShards.add(1);
          for (int partition : selectedShards) {
            int shard = currPartitions[partition].getShardOwning();
            ret.add(new KeyInfo(shard, key, indexSchema, true));
          }

          selectedShards = Repartitioner.findOrderedPartitionForRecord(false, true, fieldOffsets, common, tableSchema,
              indexSchema.getKey(), null, BinaryExpression.Operator.equal, null, key, null);
          //        List<Integer> selectedShards = new ArrayList<>();
          //        selectedShards.add(0);
          //        selectedShards.add(1);
          for (int partition : selectedShards) {
            boolean found = false;
            int shard = lastPartitions[partition].getShardOwning();
            for (KeyInfo keyInfo : ret) {
              if (keyInfo.shard == shard) {
                keyInfo.currAndLastMatch = true;
                found = true;
                break;
              }
            }
            if (!found) {
              ret.add(new KeyInfo(shard, key, indexSchema, false));
            }
          }
        }
//        for (int i = 0; i < 2; i++) {
//          int shard = partitions[i].getShardOwning();
//          ret.add(new KeyInfo(shard, key, indexSchema));
//        }
      }
    }
    return ret;
  }

  private Object doSelect(String dbName, ParameterHandler parms, Select selectNode, boolean debug, SelectStatementImpl.Explain explain) {
//    int currParmNum = 0;
//    List<String> columnNames = new ArrayList<>();
//    List<Object> values = new ArrayList<>();
    SelectBody selectBody = selectNode.getSelectBody();
    AtomicInteger currParmNum = new AtomicInteger();
    SelectStatementImpl selectStatement = new SelectStatementImpl(this);
    if (selectBody instanceof PlainSelect) {
      PlainSelect pselect = (PlainSelect) selectBody;
      selectStatement.setFromTable(((Table) pselect.getFromItem()).getName());
      Expression whereExpression = pselect.getWhere();
      ExpressionImpl expression = getExpression(currParmNum, whereExpression, selectStatement.getFromTable(), parms);
      if (expression == null) {
        expression = new AllRecordsExpressionImpl();
        ((AllRecordsExpressionImpl) expression).setFromTable(selectStatement.getFromTable());
      }
      expression.setDebug(debug);
      selectStatement.setWhereClause(expression);

      Limit limit = pselect.getLimit();
      selectStatement.setLimit(limit);
      Offset offset = pselect.getOffset();
      selectStatement.setOffset(offset);

      List<Join> joins = pselect.getJoins();
      if (joins != null) {
        for (Join join : joins) {
          FromItem rightFromItem = join.getRightItem();
          Expression onExpressionSrc = join.getOnExpression();
          ExpressionImpl onExpression = getExpression(currParmNum, onExpressionSrc, selectStatement.getFromTable(), parms);

          String rightFrom = rightFromItem.toString();
          SelectStatement.JoinType type = null;
          if (join.isInner()) {
            type = SelectStatement.JoinType.inner;
          }
          else if (join.isFull()) {
            type = SelectStatement.JoinType.full;
          }
          else if (join.isOuter() && join.isLeft()) {
            type = SelectStatement.JoinType.leftOuter;
          }
          else if (join.isOuter() && join.isRight()) {
            type = SelectStatement.JoinType.rightOuter;
          }
          selectStatement.addJoinExpression(type, rightFrom, onExpression);
        }
      }

      Distinct distinct = ((PlainSelect) selectBody).getDistinct();
      if (distinct != null) {
        //distinct.getOnSelectItems();
        selectStatement.setIsDistinct();
      }

      List<SelectItem> selectItems = ((PlainSelect) selectBody).getSelectItems();
      for (SelectItem selectItem : selectItems) {
        if (selectItem instanceof SelectExpressionItem) {
          SelectExpressionItem item = (SelectExpressionItem) selectItem;
          Alias alias = item.getAlias();
          String aliasName = null;
          if (alias != null) {
            aliasName = alias.getName();
          }

          if (item.getExpression() instanceof Column) {
            selectStatement.addSelectColumn(null, null, ((Column) item.getExpression()).getTable().getName(),
                ((Column) item.getExpression()).getColumnName(), aliasName);
          }
          else if (item.getExpression() instanceof Function) {
            Function function = (Function) item.getExpression();
            String name = function.getName();
            boolean groupCount = null != pselect.getGroupByColumnReferences() && pselect.getGroupByColumnReferences().size() != 0 && name.equalsIgnoreCase("count");
            if (groupCount || name.equalsIgnoreCase("min") || name.equalsIgnoreCase("max") || name.equalsIgnoreCase("sum") || name.equalsIgnoreCase("avg")) {
              Column parm = (Column) function.getParameters().getExpressions().get(0);
              selectStatement.addSelectColumn(name, function.getParameters(), parm.getTable().getName(), parm.getColumnName(), aliasName);
            }
            else if (name.equalsIgnoreCase("count")) {
              if (null == pselect.getGroupByColumnReferences() || pselect.getGroupByColumnReferences().size() == 0) {
                if (function.isAllColumns()) {
                  selectStatement.setCountFunction();
                }
                else {
                  ExpressionList list = function.getParameters();
                  Column column = (Column) list.getExpressions().get(0);
                  selectStatement.setCountFunction(column.getTable().getName(), column.getColumnName());
                }
                if (function.isDistinct()) {
                  selectStatement.setIsDistinct();

                  String currAlias = null;
                  for (SelectItem currItem : selectItems) {
                    if (((SelectExpressionItem) currItem).getExpression() == function) {
                      if (((SelectExpressionItem) currItem).getAlias() != null) {
                        currAlias = ((SelectExpressionItem) currItem).getAlias().getName();
                      }
                    }
                  }
                  selectStatement.addSelectColumn(null, null, ((Column) function.getParameters().getExpressions().get(0)).getTable().getName(),
                      ((Column) function.getParameters().getExpressions().get(0)).getColumnName(), currAlias);
                }
              }
            }
            else if (name.equalsIgnoreCase("upper") || name.equalsIgnoreCase("lower") ||
                name.equalsIgnoreCase("substring") || name.equalsIgnoreCase("length")) {
              Column parm = (Column) function.getParameters().getExpressions().get(0);
              selectStatement.addSelectColumn(name, function.getParameters(), parm.getTable().getName(), parm.getColumnName(), aliasName);
            }
          }
        }
      }

      List<Expression> groupColumns = pselect.getGroupByColumnReferences();
      if (groupColumns != null && groupColumns.size() != 0) {
        for (int i = 0; i < groupColumns.size(); i++) {
          Column column = (Column)groupColumns.get(i);
          selectStatement.addOrderBy(column.getTable().getName(), column.getColumnName(), true);
        }
        selectStatement.setGroupByColumns(groupColumns);
      }

      List<OrderByElement> orderByElements = pselect.getOrderByElements();
      if (orderByElements != null) {
        for (OrderByElement element : orderByElements) {
          selectStatement.addOrderBy(((Column) element.getExpression()).getTable().getName(), ((Column) element.getExpression()).getColumnName(), element.isAsc());
        }
      }
    }
    selectStatement.setPageSize(pageSize);
    selectStatement.setParms(parms);
    return selectStatement.execute(dbName, explain);
  }


  private ExpressionImpl getExpression(
      AtomicInteger currParmNum, Expression whereExpression, String tableName, ParameterHandler parms) {

    //todo: add math operators
    if (whereExpression instanceof Between) {
      Between between = (Between) whereExpression;
      Column column = (Column) between.getLeftExpression();

      BinaryExpressionImpl ret = new BinaryExpressionImpl();
      ret.setNot(between.isNot());
      ret.setOperator(BinaryExpression.Operator.and);

      BinaryExpressionImpl leftExpression = new BinaryExpressionImpl();
      ColumnImpl leftColumn = new ColumnImpl();
      if (column.getTable() != null) {
        leftColumn.setTableName(column.getTable().getName());
      }
      leftColumn.setColumnName(column.getColumnName());
      leftExpression.setLeftExpression(leftColumn);

      BinaryExpressionImpl rightExpression = new BinaryExpressionImpl();
      ColumnImpl rightColumn = new ColumnImpl();
      if (column.getTable() != null) {
        rightColumn.setTableName(column.getTable().getName());
      }
      rightColumn.setColumnName(column.getColumnName());
      rightExpression.setLeftExpression(rightColumn);

      leftExpression.setOperator(BinaryExpression.Operator.greaterEqual);
      rightExpression.setOperator(BinaryExpression.Operator.lessEqual);

      ret.setLeftExpression(leftExpression);
      ret.setRightExpression(rightExpression);

      ConstantImpl leftValue = new ConstantImpl();
      ConstantImpl rightValue = new ConstantImpl();
      if (between.getBetweenExpressionStart() instanceof LongValue) {
        long start = ((LongValue) between.getBetweenExpressionStart()).getValue();
        long end = ((LongValue) between.getBetweenExpressionEnd()).getValue();
        if (start > end) {
          long temp = start;
          start = end;
          end = temp;
        }
        leftValue.setValue(start);
        leftValue.setSqlType(Types.BIGINT);
        rightValue.setValue(end);
        rightValue.setSqlType(Types.BIGINT);
      }
      else if (between.getBetweenExpressionStart() instanceof StringValue) {
        String start = ((StringValue) between.getBetweenExpressionStart()).getValue();
        String end = ((StringValue) between.getBetweenExpressionEnd()).getValue();
        if (1 == start.compareTo(end)) {
          String temp = start;
          start = end;
          end = temp;
        }
        leftValue.setValue(start);
        leftValue.setSqlType(Types.VARCHAR);
        rightValue.setValue(end);
        rightValue.setSqlType(Types.VARCHAR);
      }

      leftExpression.setRightExpression(leftValue);
      rightExpression.setRightExpression(rightValue);

      return ret;
    }
    else if (whereExpression instanceof AndExpression) {
      BinaryExpressionImpl binaryOp = new BinaryExpressionImpl();
      binaryOp.setOperator(BinaryExpression.Operator.and);
      AndExpression andExpression = (AndExpression) whereExpression;
      Expression leftExpression = andExpression.getLeftExpression();
      binaryOp.setLeftExpression(getExpression(currParmNum, leftExpression, tableName, parms));
      Expression rightExpression = andExpression.getRightExpression();
      binaryOp.setRightExpression(getExpression(currParmNum, rightExpression, tableName, parms));
      return binaryOp;
    }
    else if (whereExpression instanceof OrExpression) {
      BinaryExpressionImpl binaryOp = new BinaryExpressionImpl();

      binaryOp.setOperator(BinaryExpression.Operator.or);
      OrExpression andExpression = (OrExpression) whereExpression;
      Expression leftExpression = andExpression.getLeftExpression();
      binaryOp.setLeftExpression(getExpression(currParmNum, leftExpression, tableName, parms));
      Expression rightExpression = andExpression.getRightExpression();
      binaryOp.setRightExpression(getExpression(currParmNum, rightExpression, tableName, parms));
      return binaryOp;
    }
    else if (whereExpression instanceof Parenthesis) {
      return getExpression(currParmNum, ((Parenthesis) whereExpression).getExpression(), tableName, parms);
    }
    else if (whereExpression instanceof net.sf.jsqlparser.expression.BinaryExpression) {
      BinaryExpressionImpl binaryOp = new BinaryExpressionImpl();

      if (whereExpression instanceof EqualsTo) {
        binaryOp.setOperator(BinaryExpression.Operator.equal);
      }
      else if (whereExpression instanceof LikeExpression) {
        binaryOp.setOperator(BinaryExpression.Operator.like);
      }
      else if (whereExpression instanceof NotEqualsTo) {
        binaryOp.setOperator(BinaryExpression.Operator.notEqual);
      }
      else if (whereExpression instanceof MinorThan) {
        binaryOp.setOperator(BinaryExpression.Operator.less);
      }
      else if (whereExpression instanceof MinorThanEquals) {
        binaryOp.setOperator(BinaryExpression.Operator.lessEqual);
      }
      else if (whereExpression instanceof GreaterThan) {
        binaryOp.setOperator(BinaryExpression.Operator.greater);
      }
      else if (whereExpression instanceof GreaterThanEquals) {
        binaryOp.setOperator(BinaryExpression.Operator.greaterEqual);
      }
      net.sf.jsqlparser.expression.BinaryExpression bexp = (net.sf.jsqlparser.expression.BinaryExpression) whereExpression;
      binaryOp.setNot(bexp.isNot());

      Expression left = bexp.getLeftExpression();
      binaryOp.setLeftExpression(getExpression(currParmNum, left, tableName, parms));

      Expression right = bexp.getRightExpression();
      binaryOp.setRightExpression(getExpression(currParmNum, right, tableName, parms));

      return binaryOp;
    }
//    else if (whereExpression instanceof ParenthesisImpl) {
//      Parenthesis retParenthesis = new Parenthesis();
//      Parenthesis parenthesis = (Parenthesis) whereExpression;
//      retParenthesis.setWhereClause(getExpression(currParmNum, parenthesis.getExpression()));
//      retParenthesis.setNot(parenthesis.isNot());
//      return retParenthesis;

//    }
    else if (whereExpression instanceof net.sf.jsqlparser.expression.operators.relational.InExpression) {
      InExpressionImpl retInExpression = new InExpressionImpl(this, parms, tableName);
      net.sf.jsqlparser.expression.operators.relational.InExpression inExpression = (net.sf.jsqlparser.expression.operators.relational.InExpression) whereExpression;
      retInExpression.setNot(inExpression.isNot());
      retInExpression.setLeftExpression(getExpression(currParmNum, inExpression.getLeftExpression(), tableName, parms));
      ItemsList items = inExpression.getRightItemsList();
      if (items instanceof ExpressionList) {
        ExpressionList expressionList = (ExpressionList) items;
        List expressions = expressionList.getExpressions();
        for (Object obj : expressions) {
          retInExpression.addExpression(getExpression(currParmNum, (Expression) obj, tableName, parms));
        }
      }
      else if (items instanceof SubSelect) {
        //todo: implement
      }
      return retInExpression;
    }
    else if (whereExpression instanceof Column) {
      Column column = (Column) whereExpression;
      ColumnImpl columnNode = new ColumnImpl();
      String colTableName = column.getTable().getName();
      if (colTableName != null) {
        columnNode.setTableName(colTableName.toLowerCase());
      }
      columnNode.setColumnName(column.getColumnName().toLowerCase());
      return columnNode;
    }
    else if (whereExpression instanceof StringValue) {
      StringValue string = (StringValue) whereExpression;
      ConstantImpl constant = new ConstantImpl();
      constant.setSqlType(Types.VARCHAR);
      try {
        constant.setValue(string.getValue().getBytes("utf-8"));
      }
      catch (UnsupportedEncodingException e) {
        throw new DatabaseException(e);
      }
      return constant;
    }
    else if (whereExpression instanceof DoubleValue) {
      DoubleValue doubleValue = (DoubleValue) whereExpression;
      ConstantImpl constant = new ConstantImpl();
      constant.setSqlType(Types.DOUBLE);
      constant.setValue(doubleValue.getValue());
      return constant;
    }
    else if (whereExpression instanceof LongValue) {
      LongValue longValue = (LongValue) whereExpression;
      ConstantImpl constant = new ConstantImpl();
      constant.setSqlType(Types.BIGINT);
      constant.setValue(longValue.getValue());
      return constant;
    }
    else if (whereExpression instanceof JdbcNamedParameter) {
      ParameterImpl parameter = new ParameterImpl();
      parameter.setParmName(((JdbcNamedParameter)whereExpression).getName());
      return parameter;
    }
    else if (whereExpression instanceof JdbcParameter) {
      ParameterImpl parameter = new ParameterImpl();
      parameter.setParmOffset(currParmNum.getAndIncrement());
      return parameter;
    }

    return null;
  }

  public boolean isRepartitioningComplete(String dbName) {
    try {
      String command = "DatabaseServer:isRepartitioningComplete:1:" + common.getSchemaVersion() + ":" + dbName;
      byte[] bytes = send(null, 0, rand.nextLong(), command, null, DatabaseClient.Replica.master);
      DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
      long serializationVersion = DataUtil.readVLong(in);
      return in.readBoolean();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public long getPartitionSize(String dbName, int shard, String tableName, String indexName) {
    return getPartitionSize(dbName, shard, 0, tableName, indexName);
  }

  public long getPartitionSize(String dbName, int shard, int replica, String tableName, String indexName) {
    try {
      String command = "DatabaseServer:getPartitionSize:1:" + common.getSchemaVersion() + ":" + dbName + ":" + tableName + ":" + indexName;
      byte[] bytes = send(null, shard, replica, command, null, DatabaseClient.Replica.specified);
      DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
      long serializationVersion = DataUtil.readVLong(in);
      return in.readLong();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }


//  public void addRecord(final long auth_user, String table, Object[] fields) throws Exception {
//    for (int i = 0; i < 2; i++) {
//      try {
//        byte[] body = null;
//        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
//        DataOutputStream out = new DataOutputStream(bytesOut);
//
//        TableSchema tableSchema = common.getTables().get(table.toLowerCase());
//        if (tableSchema == null) {
//          throw new Exception("Undefined table: name=" + table);
//        }
//
//        String command = "DatabaseServer:reserveNextId:1:" + common.getSchemaVersion() + ":" + auth_user;
//        AtomicReference<String> selectedHost = new AtomicReference<String>();
//        byte[] ret = send(selectShard(0), auth_user, command, null, DatabaseClient.Replica.def, 20000, selectedHost);
//        long id = Long.valueOf(new String(ret, "utf-8"));
//
//        DataUtil.writeVLong(out, 0, new DataUtil.ResultLength());
//        common.serializeFields(fields, out, tableSchema);
//        out.close();
//        body = bytesOut.toByteArray();
//
//        command = "DatabaseServer:addRecord:1:" + common.getSchemaVersion() + ":" + auth_user + ":" + table + ":" + id;
//        send(selectShard(id), auth_user, command, body, DatabaseClient.Replica.def, 20000, selectedHost);
//      }
//      catch (SchemaOutOfSyncException t) {
//        logger.error("Schema out of sync: currVer=" + common.getSchemaVersion());
//        syncSchema();
//      }
//    }
//  }

//  public void updateRecord(
//      final long auth_user, String table, long recordId, List<String> columns, List<Object> values) throws Exception {
//    for (int i = 0; i < 2; i++) {
//      try {
//        byte[] body = null;
//        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
//        DataOutputStream out = new DataOutputStream(bytesOut);
//
//        TableSchema tableSchema = common.getTables().get(table.toLowerCase());
//        if (tableSchema == null) {
//          throw new Exception("Undefined table: name=" + table);
//        }
//
//        common.serializeFields(columns, values, out, tableSchema);
//        out.close();
//        body = bytesOut.toByteArray();
//
//        String command = "DatabaseServer:updateRecord:1:" + common.getSchemaVersion() + ":" + auth_user + ":" + table + ":" + recordId;
//        AtomicReference<String> selectedHost = new AtomicReference<String>();
//        send(selectShard(recordId), auth_user, command, body, DatabaseClient.Replica.def, 20000, selectedHost);
//      }
//      catch (SchemaOutOfSyncException t) {
//        logger.error("Schema out of sync: currVer=" + common.getSchemaVersion());
//        syncSchema();
//      }
//    }
//  }

  public void syncSchema() {
//    try {
      long previousVersion = common.getSchemaVersion();
      //Thread.sleep(4);
      //synchronized (common.getSchema(dbName).getSchemaLock()) {
//        if (previousVersion < common.getSchemaVersion()) {
//          return;
//        }
        //logger.error("Schema out of sync: currVer=" + common.getSchemaVersion());

        String command = "DatabaseServer:getSchema:1:" + common.getSchemaVersion() + ":__none__";
        try {

          byte[] ret = send(null, 0, 0, command, null, Replica.master);
          if (ret != null) {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(ret));
            long serializationVersion = DataUtil.readVLong(in);
            common.deserializeSchema(common, in);

            logger.info("Schema received from server: currVer=" + common.getSchemaVersion());
          }
        }
        catch (Exception t) {
          throw new DatabaseException(t);
        }
      //}
//    }
//    catch (InterruptedException e) {
//      throw new DatabaseException(e);
//    }
  }


//  public static void serializeMap(Map<String, Object> fields, DataOutputStream out) throws IOException {
//    out.writeInt(fields.size());
//    for (Map.Entry<String, Object> field : fields.entrySet()) {
//      out.writeUTF(field.getKey());
//      Object value = field.getValue();
//      if (value instanceof Long) {
//        out.writeInt(DataType.Type.BIGINT.getValue());
//        out.writeLong((Long) value);
//      }
//    }
//  }

  public JsonDict getConfig(String dbName) {
    try {
      long auth_user = rand.nextLong();
      String command = "DatabaseServer:getConfig:1:" + common.getSchemaVersion() + ":" + dbName + ":" + auth_user;
      byte[] ret = send(null, selectShard(0), auth_user, command, null, DatabaseClient.Replica.def);
      return new JsonDict(new String(ret, "utf-8"));
    }
    catch (UnsupportedEncodingException e) {
      throw new DatabaseException(e);
    }
  }

  public void beginRebalance(String dbName, String tableName, String indexName) {
    String command = "DatabaseServer:beginRebalance:1:1:" + dbName + ":" + tableName + ":" + indexName;
    send(null, 0, rand.nextLong(), command, null, DatabaseClient.Replica.master);
  }
}
