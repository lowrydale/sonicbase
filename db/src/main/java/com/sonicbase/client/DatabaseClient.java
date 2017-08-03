package com.sonicbase.client;

import com.codahale.metrics.MetricRegistry;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.sonicbase.common.*;
import com.sonicbase.index.Repartitioner;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.jdbcdriver.QueryType;
import com.sonicbase.query.BinaryExpression;
import com.sonicbase.query.*;
import com.sonicbase.query.impl.*;
import com.sonicbase.schema.*;
import com.sonicbase.server.DatabaseServer;
import com.sonicbase.server.ReadManager;
import com.sonicbase.server.SnapshotManager;
import com.sonicbase.socket.DatabaseSocketClient;
import com.sonicbase.socket.DeadServerException;
import com.sonicbase.util.DataUtil;
import com.sonicbase.util.JsonArray;
import com.sonicbase.util.JsonDict;
import com.sonicbase.util.StreamUtils;
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

import java.io.*;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.singletonList;

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

  private static org.apache.log4j.Logger localLogger = org.apache.log4j.Logger.getLogger("com.sonicbase.logger");
  private Logger logger;

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
      "populateIndex",
      "insertIndexEntryByKey",
      "insertIndexEntryByKeyWithRecord",
      "removeRecord",
      "deleteMovedRecords",
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
      "testWrite"

  };

  private static Set<String> writeVerbs = new HashSet<String>();

  public DatabaseClient(String host, int port, int shard, int replica, boolean isClient) {
    this(new String[]{host + ":" + port}, shard, replica, isClient, null);
  }

  public DatabaseClient(String[] hosts, int shard, int replica, boolean isClient) {
    this(hosts, shard, replica, isClient, null);
  }

  public DatabaseClient(String host, int port, int shard, int replica, boolean isClient, DatabaseCommon common) {
    this(new String[]{host + ":" + port}, shard, replica, isClient, common);
  }
  public DatabaseClient(String[] hosts, int shard, int replica, boolean isClient, DatabaseCommon common) {
    servers = new Server[1][];
    servers[0] = new Server[hosts.length];
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

    ExpressionImpl.startPreparedReaper(this);

    configureServers();

    logger = new Logger(this);

    statsTimer = new java.util.Timer();
//    statsTimer.scheduleAtFixedRate(new TimerTask() {
//      @Override
//      public void run() {
//        System.out.println("IndexLookup stats: count=" + INDEX_LOOKUP_STATS.getCount() + ", rate=" + INDEX_LOOKUP_STATS.getFiveMinuteRate() +
//            ", durationAvg=" + INDEX_LOOKUP_STATS.getSnapshot().getMean() / 1000000d +
//            ", duration99.9=" + INDEX_LOOKUP_STATS.getSnapshot().get999thPercentile() / 1000000d);
//        System.out.println("BatchIndexLookup stats: count=" + BATCH_INDEX_LOOKUP_STATS.getCount() + ", rate=" + BATCH_INDEX_LOOKUP_STATS.getFiveMinuteRate() +
//            ", durationAvg=" + BATCH_INDEX_LOOKUP_STATS.getSnapshot().getMean() / 1000000d +
//            ", duration99.9=" + BATCH_INDEX_LOOKUP_STATS.getSnapshot().get999thPercentile() / 1000000d);
//        System.out.println("BatchIndexLookup stats: count=" + JOIN_EVALUATE.getCount() + ", rate=" + JOIN_EVALUATE.getFiveMinuteRate() +
//            ", durationAvg=" + JOIN_EVALUATE.getSnapshot().getMean() / 1000000d +
//            ", duration99.9=" + JOIN_EVALUATE.getSnapshot().get999thPercentile() / 1000000d);
//      }
//    }, 20 * 1000, 20 * 1000);

    for (String verb : write_verbs_array) {
      write_verbs.add(verb);
    }

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

  static {
    for (String verb : write_verbs_array) {
      writeVerbs.add(verb);
    }
  }

  public static ThreadLocal<List<InsertRequest>> batch = new ThreadLocal<>();

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

  private ThreadLocal<Boolean> isExplicitTrans = new ThreadLocal<>();
  private ThreadLocal<Boolean> isCommitting = new ThreadLocal<>();
  private ThreadLocal<Long> transactionId = new ThreadLocal<>();
  private ThreadLocal<List<TransactionOperation>> transactionOps = new ThreadLocal<>();
  Timer statsTimer;

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
     /*
    List<TransactionOperation> ops = transactionOps.get();
    for (TransactionOperation op : ops) {
      op.statement.setParms(op.parms);
      op.statement.execute(dbName, explain);
    }
    */
     while (true) {
      try {
        ComObject cobj = new ComObject();
        cobj.put(ComObject.Tag.dbName, dbName);
        cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
        cobj.put(ComObject.Tag.method, "commit");
        cobj.put(ComObject.Tag.transactionId, transactionId.get());
        String command = "DatabaseServer:ComObject:commit:";
        sendToAllShards(null, 0, command, cobj, DatabaseClient.Replica.def);

        isExplicitTrans.set(false);
        transactionOps.set(null);
        isCommitting.set(false);
        transactionId.set(null);

        break;
      }
      catch (Exception e) {
        int index = ExceptionUtils.indexOfThrowable(e, SchemaOutOfSyncException.class);
        if (-1 != index) {
          continue;
        }
        throw new DatabaseException(e);
      }
     }



  }

  public void rollback(String dbName) {

    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.dbName, dbName);
    cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
    cobj.put(ComObject.Tag.method, "rollback");
    cobj.put(ComObject.Tag.transactionId, transactionId.get());
    String command = "DatabaseServer:ComObject:rollback:";
    sendToAllShards(null, 0, command, cobj, DatabaseClient.Replica.def);

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
    String command = "DatabaseServer:ComObject:createDatabase:";

    sendToMaster(command, cobj);
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
            SelectContextImpl context = ExpressionImpl.lookupIds(dbName, common, this, replica, 1, tableSchema.getName(), indexSchema.getName(), forceSelectOnServer, BinaryExpression.Operator.equal, null, null, keyObj, parms,
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
      catch (Exception e) {
        int index = ExceptionUtils.indexOfThrowable(e, SchemaOutOfSyncException.class);
        if (-1 != index) {
          continue;
        }
        logger.error("Error debugging record", e);
        break;
      }
    }
    return "[not found]";
  }

  public void shutdown() {
    if (statsTimer != null) {
      statsTimer.cancel();
    }
    ExpressionImpl.stopPreparedReaper();
    executor.shutdownNow();
    for (Server[] shard : servers) {
      for (Server replica : shard) {
        replica.getSocketClient().shutdown();
      }
    }
  }

  public int[] executeBatch() throws UnsupportedEncodingException, SQLException {

    try {
      final Object mutex = new Object();
      final List<PreparedInsert> withRecordPrepared = new ArrayList<>();
      final List<PreparedInsert> prepared = new ArrayList<>();
      long nonTransId = 0;
      while (true) {
        try {
          if (!isExplicitTrans.get()) {
            nonTransId = allocateId(batch.get().get(0).dbName);
          }
          break;
        }
        catch (Exception e) {
          int index = ExceptionUtils.indexOfThrowable(e, SchemaOutOfSyncException.class);
          if (-1 != index) {
            continue;
          }
          throw new DatabaseException(e);
        }
      }

      for (InsertRequest request : batch.get()) {
        List<PreparedInsert> inserts = prepareInsert(request, nonTransId);
        for (PreparedInsert insert : inserts) {
          if (insert.keyInfo.indexSchema.getValue().isPrimaryKey()) {
            withRecordPrepared.add(insert);
          }
          else {
            prepared.add(insert);
          }
        }
      }
      while (true) {
        final AtomicInteger totalCount = new AtomicInteger();
        try {
          if (batch.get() == null) {
            throw new DatabaseException("No batch initiated");
          }

          String dbName = batch.get().get(0).dbName;
          final String command = "DatabaseServer:ComObject:batchInsertIndexEntryByKeyWithRecord:";

          final List<List<PreparedInsert>> withRecordProcessed = new ArrayList<>();
          final List<List<PreparedInsert>> processed = new ArrayList<>();
          final List<ByteArrayOutputStream> withRecordBytesOut = new ArrayList<>();
          final List<DataOutputStream> withRecordOut = new ArrayList<>();
          final List<ByteArrayOutputStream> bytesOut = new ArrayList<>();
          final List<DataOutputStream> out = new ArrayList<>();
          final List<ComObject> cobjs1 = new ArrayList<>();
          final List<ComObject> cobjs2 = new ArrayList<>();
          for (int i = 0; i < getShardCount(); i++) {
            ByteArrayOutputStream bOut = new ByteArrayOutputStream();
            withRecordBytesOut.add(bOut);
            withRecordOut.add(new DataOutputStream(bOut));
            bOut = new ByteArrayOutputStream();
            bytesOut.add(bOut);
            out.add(new DataOutputStream(bOut));
            withRecordProcessed.add(new ArrayList<PreparedInsert>());
            processed.add(new ArrayList<PreparedInsert>());

            final ComObject cobj1 = new ComObject();
            cobj1.put(ComObject.Tag.dbName, dbName);
            cobj1.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
            cobj1.put(ComObject.Tag.method, "batchInsertIndexEntryByKeyWithRecord");
            cobj1.putArray(ComObject.Tag.insertObjects, ComObject.Type.objectType);
            cobjs1.add(cobj1);

            final ComObject cobj2 = new ComObject();
            cobj2.put(ComObject.Tag.dbName, dbName);
            cobj2.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
            cobj2.put(ComObject.Tag.method, "batchInsertIndexEntryByKey");
            cobj2.putArray(ComObject.Tag.insertObjects, ComObject.Type.objectType);
            cobjs2.add(cobj2);

          }
          synchronized (mutex) {
            for (PreparedInsert insert : withRecordPrepared) {
              ComObject obj = serializeInsertKeyWithRecord(insert.dbName, insert.tableName, insert.keyInfo, insert.record);
              cobjs1.get(insert.keyInfo.shard).getArray(ComObject.Tag.insertObjects).getArray().add(obj);
              withRecordProcessed.get(insert.keyInfo.shard).add(insert);
            }
            for (PreparedInsert insert : prepared) {
              ComObject obj = serializeInsertKey(insert.dbName, insert.tableName, insert.keyInfo,
                  insert.primaryKeyIndexName, insert.primaryKey);
              cobjs2.get(insert.keyInfo.shard).getArray(ComObject.Tag.insertObjects).getArray().add(obj);
              processed.get(insert.keyInfo.shard).add(insert);
            }
          }


          List<Future> futures = new ArrayList<>();
          for (int i = 0; i < bytesOut.size(); i++) {
            final int offset = i;
            futures.add(executor.submit(new Callable() {
              @Override
              public Object call() throws Exception {
//                ByteArrayOutputStream currBytes = withRecordBytesOut.get(offset);
//                byte[] bytes = currBytes.toByteArray();
//                if (bytes == null || bytes.length == 0) {
//                  return null;
//                }
                if (cobjs1.get(offset).getArray(ComObject.Tag.insertObjects).getArray().size() == 0) {
                  return null;
                }
                byte[] ret = send(null, offset, 0, command, cobjs1.get(offset), DatabaseClient.Replica.def);
                if (ret == null) {
                  throw new FailedToInsertException("No response for key insert");
                }
                for (PreparedInsert insert : withRecordProcessed.get(offset)) {
                  synchronized (mutex) {
                    withRecordPrepared.remove(insert);
                  }
                }
                ComObject retObj = new ComObject(ret);
                int retVal = retObj.getInt(ComObject.Tag.count);
                totalCount.addAndGet(retVal);
                //if (retVal != 1) {
                //  throw new FailedToInsertException("Incorrect response from server: value=" + retVal);
                //}
                return null;
              }
            }));
          }
          for (Future future : futures) {
            future.get();
          }
          final String command2 = "DatabaseServer:ComObject:batchInsertIndexEntryByKey:";

          futures = new ArrayList<>();
          for (int i = 0; i < bytesOut.size(); i++) {
            final int offset = i;
            futures.add(executor.submit(new Callable() {
              @Override
              public Object call() throws Exception {
//                ByteArrayOutputStream currBytes = bytesOut.get(offset);
//                byte[] bytes = currBytes.toByteArray();
//                if (bytes == null || bytes.length == 0) {
//                  return null;
//                }
                if (cobjs2.get(offset).getArray(ComObject.Tag.insertObjects).getArray().size() == 0) {
                  return null;
                }
                send(null, offset, rand.nextLong(), command2, cobjs2.get(offset), DatabaseClient.Replica.def);

                for (PreparedInsert insert : processed.get(offset)) {
                  prepared.remove(insert);
                }

                return null;
              }
            }));
          }
          Exception firstException = null;
          for (Future future : futures) {
            try {
              future.get();
            }
            catch (Exception e) {
              firstException = e;
            }
          }
          if (firstException != null) {
            throw firstException;
          }

          int[] ret = new int[totalCount.get()];
          for (int i = 0; i < ret.length; i++) {
            ret[i] = 1;
          }
          return ret;
        }
        catch (Exception e) {
          if (e.getCause() instanceof SchemaOutOfSyncException) {
            synchronized (mutex) {
              for (PreparedInsert insert : withRecordPrepared) {
                List<KeyInfo> keys = getKeys(common.getTables(insert.dbName).get(insert.tableSchema.getName()), insert.columnNames, insert.values, insert.id);
                for (KeyInfo key : keys) {
                  if (key.indexSchema.getKey().equals(insert.indexName)) {
                    insert.keyInfo.shard = key.shard;
                    break;
                  }
                }
              }

              for (PreparedInsert insert : prepared) {
                List<KeyInfo> keys = getKeys(common.getTables(insert.dbName).get(insert.tableSchema.getName()), insert.columnNames, insert.values, insert.id);
                for (KeyInfo key : keys) {
                  if (key.indexSchema.getKey().equals(insert.indexName)) {
                    insert.keyInfo.shard = key.shard;
                    break;
                  }
                }
              }
            }
            continue;
          }
          throw new DatabaseException(e);
        }
      }
    }
    catch (Exception e) {
      if (!(e instanceof  SchemaOutOfSyncException)) {
        logger.sendErrorToServer("Error processing batch request", e);
      }
      throw new DatabaseException(e);
    }
  }

  public String getCluster() {
    getConfig();
    return common.getServersConfig().getCluster();
  }

  public ReconfigureResults reconfigureCluster() {
    try {
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.dbName, "__none__");
      cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
      cobj.put(ComObject.Tag.method, "healthCheck");
      String command = "DatabaseServer:ComObject:healthCheck:";

      try {
        byte[] bytes = sendToMaster(command, cobj);
        ComObject retObj = new ComObject(bytes);
        if (retObj.getString(ComObject.Tag.status).equals("{\"status\" : \"ok\"}")) {
          ComObject rcobj = new ComObject();
          rcobj.put(ComObject.Tag.dbName, "__none__");
          rcobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
          rcobj.put(ComObject.Tag.method, "reconfigureCluster");
          command = "DatabaseServer:ComObject:reconfigureCluster:";
          bytes = sendToMaster(command, null);
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

    public byte[] do_send(String batchKey, String command, ComObject body) {
      return socketClient.do_send(batchKey, command, body.serialize(), hostPort);
    }
    public byte[] do_send(String batchKey, String command, byte[] body) {
      return socketClient.do_send(batchKey, command, body, hostPort);
    }
  }

  public byte[] do_send(List<DatabaseSocketClient.Request> requests) {
    return DatabaseSocketClient.do_send(requests);
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
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.dbName, "__none__");
    cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
    cobj.put(ComObject.Tag.method, "getConfig");
    String command = "DatabaseServer:ComObject:getConfig:";
    try {
      byte[] ret = null;
      int receivedReplica = -1;
      try {
        ret = send(null, 0, 0, command, cobj, Replica.specified);
        receivedReplica = 0;
      }
      catch (Exception e) {
        localLogger.error("Error getting config from master", e);
      }
      if (ret == null) {
        for (int replica = 1; replica < getReplicaCount(); replica++) {
          try {
            ret = send(null, 0, replica, command, cobj, Replica.specified);
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
      final long auth_user, final String command, final ComObject body, final Replica replica) {
    return sendToAllShards(batchKey, auth_user, command, body, replica, false);
  }

  public byte[][] sendToAllShards(
      final String batchKey,
      final long auth_user, final String command, final ComObject body, final Replica replica, final boolean ignoreDeath) {
    List<Future<byte[]>> futures = new ArrayList<Future<byte[]>>();
    try {
      for (int i = 0; i < servers.length; i++) {
        final int shard = i;
        futures.add(executor.submit(new Callable<byte[]>() {
          @Override
          public byte[] call() {
            return send(batchKey, shard, auth_user, command, body, replica, ignoreDeath);
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
      handleSchemaOutOfSyncException(command, e);
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
                     int shard, long auth_user, String command, ComObject body, Replica replica) {
    return send(batchKey, shard, auth_user, command, body, replica, false);
  }

  public byte[] send(String batchKey,
                     int shard, long auth_user, String command, ComObject body, Replica replica, boolean ignoreDeath) {
//    DatabaseServer server = DatabaseServer.getServers().get(shard).get(0);
//    while (true) {
//      try {
//        if (server != null) {
//          return server.handleCommand(command, body, false);
//        }

    return send(batchKey, servers[shard], shard, auth_user, command, body, replica, ignoreDeath);
//      }
//      catch (Exception e) {
//        command = handleSchemaOutOfSyncException(command, e);
//      }
//    }
  }

  public byte[] sendToMaster(String command, ComObject body) {
    while (true) {
      int masterReplica = 0;
      if (common.getServersConfig() != null) {
        masterReplica = common.getServersConfig().getShards()[0].getMasterReplica();
      }
      try {
        return send(null, servers[0], 0, masterReplica, command, body, Replica.specified);
      }
      catch (DeadServerException e1) {
        throw e1;
      }
      catch (SchemaOutOfSyncException e) {
        throw e;
      }
      catch (Exception e) {
        for (int i = 0; i < getReplicaCount(); i++) {
          if (i == masterReplica) {
            continue;
          }
          ComObject cobj = new ComObject();
          cobj.put(ComObject.Tag.dbName, "__none__");
          cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
          cobj.put(ComObject.Tag.method, "getSchema");
          String getCommand = "DatabaseServer:ComObject:getSchema:";
          try {

            byte[] ret = send(null, 0, i, getCommand, cobj, Replica.specified);
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
    }
  }

  private String handleSchemaOutOfSyncException(String command, Exception e) {
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
        Long serverVersion = null;
        if (msg != null) {
          int pos = msg.indexOf("currVer:");
          if (pos != -1) {
            int pos2 = msg.indexOf(":", pos + "currVer:".length());
            serverVersion = Long.valueOf(msg.substring(pos + "currVer:".length(), pos2));
          }
        }

        if (serverVersion == null || serverVersion > common.getSchemaVersion()) {
          //logger.info("Schema out of sync: currVersion=" + common.getSchemaVersion());
          syncSchema(serverVersion);
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
      String command, ComObject body, Replica replica) {
    return send(batchKey, replicas, shard, auth_user, command, body, replica, false);
  }

  public byte[] send(
      String batchKey, Server[] replicas, int shard, long auth_user,
      String command, ComObject body, Replica replica, boolean ignoreDeath) {
    try {
      if (body == null) {
        body = new ComObject();
      }
      String localCommand = command;
      int pos = localCommand.indexOf(":");
      int pos2 = localCommand.indexOf(":", pos + 1);
      String verb = localCommand.substring(pos + 1, pos2);
      int pos3 = localCommand.indexOf(":", pos2 + 1);
      String verb2 = localCommand.substring(pos2 + 1, pos3);

      byte[] ret = null;
      for (int attempt = 0; attempt < 1; attempt++) {
        try {
//          if (!ignoreDeath) {
//            outer:
//            while (true) {
//              for (Server server : replicas) {
//                if (!server.dead) {
//                  break outer;
//                }
//                Thread.sleep(1000);
//              }
//            }
//          }

          if (replica == Replica.all) {
            try {
              boolean local = false;
              List<DatabaseSocketClient.Request> requests = new ArrayList<>();
              for (int i = 0; i < replicas.length; i++) {
                Server server = replicas[i];
                if (server.dead) {
                  throw new DeadServerException("Host=" + server.hostPort + ", command=" + localCommand);
                }
                DatabaseServer dbserver = getLocalDbServer(shard, i);
                if (dbserver != null) {
                  local = true;
                  ret = dbserver.handleCommand(localCommand, body.serialize(), false, true);
                }
                else {
                  DatabaseSocketClient.Request request = new DatabaseSocketClient.Request();
                  request.setBatchKey(batchKey);
                  request.setCommand(localCommand);
                  request.setBody(body.serialize());
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
            catch (DeadServerException e) {
              throw e;
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
            int masterReplica = - 1;
            while (true) {
              masterReplica = common.getServersConfig().getShards()[shard].getMasterReplica();
              if (masterReplica != -1) {
                break;
              }
              syncSchema();
            }

            Server currReplica = replicas[masterReplica];
            DatabaseServer dbserver = getLocalDbServer(shard, 0);
            try {
              if (!ignoreDeath && currReplica.dead) {
                throw new DeadServerException("Host=" + currReplica.hostPort + ", command=" + localCommand);
              }
              if (dbserver != null) {
                return dbserver.handleCommand(localCommand, body.serialize(), false, true);
              }
              return currReplica.do_send(batchKey, localCommand, body);
            }
            catch (Exception e) {
              syncSchema();

              masterReplica = common.getServersConfig().getShards()[shard].getMasterReplica();
              currReplica = replicas[masterReplica];
              dbserver = getLocalDbServer(shard, 0);
              try {
                if (!ignoreDeath && currReplica.dead) {
                  throw new DeadServerException("Host=" + currReplica.hostPort + ", command=" + localCommand);
                }
                if (dbserver != null) {
                  return dbserver.handleCommand(localCommand, body.serialize(), false, true);
                }
                return currReplica.do_send(batchKey, localCommand, body);
              }
              catch (DeadServerException e1) {
                throw e;
              }
              catch (Exception e1) {
                e = new DatabaseException("Host=" + currReplica.hostPort + ", command=" + localCommand, e1);
                handleDeadServer(e1, currReplica);
                localCommand = handleSchemaOutOfSyncException(localCommand, e1);
              }
            }
          }
          else if (replica == Replica.specified) {
            DatabaseServer dbserver = getLocalDbServer(shard, (int) auth_user);
            boolean skip = false;
            if (!ignoreDeath && replicas[(int)auth_user].dead) {
              if (writeVerbs.contains(verb) || writeVerbs.contains(verb2)) {
                body.put(ComObject.Tag.command, localCommand);
                String queueCommand = "DatabaseServer:queueForOtherServer:1:" + SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION + ":1:__none__:" + (int) auth_user;

                int masterReplica = common.getServersConfig().getShards()[shard].getMasterReplica();
                dbserver = getLocalDbServer(shard, masterReplica);
                if (dbserver != null) {
                  dbserver.handleCommand(queueCommand, body.serialize(), false, true);
                }
                else {
                  replicas[masterReplica].do_send(null, queueCommand, body);
                }
                skip = true;
              }
            }
            if (!skip) {
              try {
                if (dbserver != null) {
                  return dbserver.handleCommand(localCommand, body.serialize(), false, true);
                }
                return replicas[(int) auth_user].do_send(batchKey, localCommand, body);
              }
              catch (DeadServerException e) {
                throw e;
              }
              catch (Exception e) {
                e = new DatabaseException("Host=" + replicas[(int) auth_user].hostPort + ", command=" + localCommand, e);
                try {
                  handleDeadServer(e, replicas[(int) auth_user]);
                  localCommand = handleSchemaOutOfSyncException(localCommand, e);
                }
                catch (Exception t) {
                  throw t;
                }
              }
            }
          }
          else if (replica == Replica.def) {
            byte[] bytes = body.serialize();
            if (write_verbs.contains(verb) || write_verbs.contains(verb2)) {
              while (true) {
                int masterReplica = common.getServersConfig().getShards()[shard].getMasterReplica();
                Server currReplica = replicas[masterReplica];
                try {
                 //int successCount = 0;
                  if (!ignoreDeath && replicas[masterReplica].dead) {
                    logger.error("dead server: master=" + masterReplica);
                    throw new DeadServerException("Host=" + currReplica.hostPort + ", command=" + localCommand);
                  }
                  localCommand += ":xx_repl_xx" + masterReplica;
                  DatabaseServer dbserver = getLocalDbServer(shard, masterReplica);
                  if (dbserver != null) {
                    ret = dbserver.handleCommand(localCommand, bytes, false, true);
                  }
                  else {
                    ret = currReplica.do_send(batchKey, localCommand, bytes);
                  }

                  String newCommand = localCommand;
                  if (ret != null) {
                    ComObject retObj = new ComObject(ret);
                    final String newCommand0 = localCommand + ":xx_sn0_xx=" + retObj.getLong(ComObject.Tag.sequence0);
                    newCommand = newCommand0 + ":xx_sn1_xx=" + retObj.getLong(ComObject.Tag.sequence1);
                  }
                  for (int i = 0; i < getReplicaCount(); i++) {
                    if (i == masterReplica) {
                      continue;
                    }
                    boolean dead = currReplica.dead;
                    currReplica = replicas[i];
                    dbserver = getLocalDbServer(shard, i);
                    while (true) {
                      boolean skip = false;
                      if (!ignoreDeath && dead) {
                        try {
                          if (writeVerbs.contains(verb) || writeVerbs.contains(verb2)) {
                            body.put(ComObject.Tag.command, newCommand);
                            String queueCommand = "DatabaseServer:queueForOtherServer:1:" + SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION + ":1:__none__:" + (int) auth_user;

                            masterReplica = common.getServersConfig().getShards()[shard].getMasterReplica();
                            dbserver = getLocalDbServer(shard, masterReplica);
                            if (dbserver != null) {
                              dbserver.handleCommand(queueCommand, body.serialize(), false, true);
                            }
                            else {
                              replicas[masterReplica].do_send(null, queueCommand, body.serialize());
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
                          if (dbserver != null) {
                            ret = dbserver.handleCommand(newCommand, bytes, false, true);
                          }
                          else {
                            ret = currReplica.do_send(batchKey, newCommand, bytes);
                          }
                        }
                        catch (Exception e) {
                          if (e.getMessage().contains("SchemaOutOfSyncException")) {
                            syncSchema();
                            body.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
                            continue;
                          }
                          if (-1 != ExceptionUtils.indexOfThrowable(e, DeadServerException.class)) {
                            dead = true;
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
                  e = new DatabaseException("Host=" + currReplica.hostPort + ", command=" + localCommand, e);
                  localCommand = handleSchemaOutOfSyncException(localCommand, e);
                }
              }
            }
            else {
              Exception lastException = null;
              boolean success = false;
              int offset = ThreadLocalRandom.current().nextInt(replicas.length);
              for (long rand = offset; rand < offset + replicas.length; rand++) {
                int replicaOffset = Math.abs((int) (rand % replicas.length));
                if (!replicas[replicaOffset].dead) {
                  try {
                    DatabaseServer dbserver = getLocalDbServer(shard, replicaOffset);
                    if (dbserver != null) {
                      return dbserver.handleCommand(localCommand, body.serialize(), false, true);
                    }
                    else {
                      return replicas[replicaOffset].do_send(batchKey, localCommand, body);
                    }
                    //success = true;
                  }
                  catch (Exception e) {
                    Server currReplica = replicas[replicaOffset];
                    try {
                      handleDeadServer(e, replicas[replicaOffset]);
                      localCommand = handleSchemaOutOfSyncException(localCommand, e);
                      //rand--;
                      lastException = e;
                    }
                    catch (SchemaOutOfSyncException s) {
                      throw s;
                    }
                    catch (Exception t) {
                      localLogger.error("Error synching schema", t);
                      localLogger.error("Error sending request", e);
                      lastException = t;
                    }
                  }
                }
              }
              if (!success) {
                if (lastException != null) {
                  throw new DatabaseException("Failed to send to any replica: command=" + localCommand, lastException);
                }
                throw new DatabaseException("Failed to send to any replica: command=" + localCommand);
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

  private void handleDeadServer(Throwable t, Server replica) {
//    if (t instanceof DeadServerException) {
//      replica.dead = true;
//
//      addServerToDeadList(replica);
//    }
  }

//  private Map<String, Thread> deadList = new ConcurrentHashMap<>();

//  private void addServerToDeadList(final Server replica) {
//    Thread thread = new Thread(new Runnable(){
//      @Override
//      public void run() {
//        while (true) {
//          try {
//            Thread.sleep(10000);
//
//            String command = "DatabaseServer:healthCheck:1:" + SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION + ":1:__none__";
//
//            byte[] bytes = replica.do_send(null, command, null);
//            if (new String(bytes, "utf-8").equals("{\"status\" : \"ok\"}")) {
//              replica.dead = false;
//              deadList.remove(replica.hostPort);
//              break;
//            }
//          }
//          catch (Exception e) {
//            logger.error("Error in dead server thread", e);
//          }
//        }
//      }
//    });
//
//    if (deadList.put(replica.hostPort, thread) == null) {
//      thread.start();
//    }
//  }

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

  public boolean isBackupComplete() {
    try {
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.dbName, "__none__");
      cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
      cobj.put(ComObject.Tag.method, "isEntireBackupComplete");
      String command = "DatabaseServer:ComObject:isEntireBackupComplete:";
      byte[] ret = send(null, 0, 0, command, cobj, DatabaseClient.Replica.master);
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
      String command = "DatabaseServer:ComObject:isEntireRestoreComplete:";
      byte[] ret = send(null, 0, 0, command, cobj, DatabaseClient.Replica.master);
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
      String command = "DatabaseServer:ComObject:startRestore:";
      byte[] ret = send(null, 0, 0, command, cobj, DatabaseClient.Replica.master);
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
    String command = "DatabaseServer:ComObject:startBackup:";
    byte[] ret = send(null, 0, 0, command, cobj, DatabaseClient.Replica.master);
  }

  public void doCreateIndex(String dbName, CreateIndexStatementImpl statement) throws IOException {
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.dbName, dbName);
    cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
    cobj.put(ComObject.Tag.method, "createIndex");
    cobj.put(ComObject.Tag.masterSlave, "master");
    cobj.put(ComObject.Tag.tableName, statement.getTableName());
    cobj.put(ComObject.Tag.indexName, statement.getName());
    cobj.put(ComObject.Tag.isUnique, statement.isUnique());
    String command = "DatabaseServer:ComObject:createIndex:";
    StringBuilder builder = new StringBuilder();
    boolean first = true;
    for (String field : statement.getColumns()) {
      if (!first) {
        builder.append(",");
      }
      first = false;
      builder.append(field);
    }
    //command = command + ":" + builder.toString();

    cobj.put(ComObject.Tag.fieldsStr, builder.toString());

    byte[] ret = sendToMaster(command, cobj);
    ComObject retObj = new ComObject(ret);
    common.deserializeSchema(retObj.getByteArray(ComObject.Tag.schemaBytes));
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
      catch (Exception e) {
        int index = ExceptionUtils.indexOfThrowable(e, SchemaOutOfSyncException.class);
        if (-1 != index) {
          continue;
        }
        logger.sendErrorToServer("Error processing request", e);
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

  public static HttpResponse restGet(String url) throws IOException {
    NetHttpTransport.Builder builder = new NetHttpTransport.Builder();

    final HttpRequest request = builder.build().createRequestFactory().buildGetRequest(new GenericUrl(url));
    request.setReadTimeout(120000);
    final HttpHeaders headers = request.getHeaders();
    headers.put("Accept", singletonList("application/json"));
    headers.put("Content-Type", singletonList("application/json"));
    //headers.put("User-Agent", "TreeGraphBenchmarkClient");
    request.setHeaders(headers);

    return request.execute();
  }

  private ResultSet doDescribe(String dbName, String sql) throws InterruptedException, ExecutionException, IOException {
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
    else if (parts[1].trim().equalsIgnoreCase("tables")) {
      StringBuilder builder = new StringBuilder();
      for (TableSchema tableSchema : common.getTables(dbName).values()) {
        builder.append(tableSchema.getName() + "\n");
      }
      String ret = builder.toString();
      String[] lines = ret.split("\\n");
      return new ResultSetImpl(lines);
    }
    else if (parts[1].trim().equalsIgnoreCase("licenses")) {
      return describeLicenses();
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
    else if (parts[1].trim().equalsIgnoreCase("shards")) {
      return describeShards(dbName);
    }
    else if (parts[1].trim().equalsIgnoreCase("server") &&
        parts[2].trim().equalsIgnoreCase("stats")) {
      return describeServerStats(dbName);
    }
    else if (parts[1].trim().equalsIgnoreCase("server") &&
        parts[2].trim().equalsIgnoreCase("health")) {
      return describeServerHeath(dbName);
    }
    else if (parts[1].trim().equalsIgnoreCase("schema") &&
        parts[2].trim().equalsIgnoreCase("version")) {
      return describeSchemaVersion(dbName);
    }
    else {
      throw new DatabaseException("Unknown target for describe: target=" + parts[1]);
    }

  }

  public static ResultSet describeLicenses() {
    try {
      String json = StreamUtils.inputStreamToString(DatabaseClient.class.getResourceAsStream("/config-license-server.json"));
      JsonDict config = new JsonDict(json);

      HttpResponse response = restGet("https://" + config.getDict("server").getString("publicAddress") + ":" +
          config.getDict("server").getInt("port") + "/license/currUsage");
      JsonDict dict = new JsonDict(StreamUtils.inputStreamToString(response.getContent()));
      StringBuilder builder = new StringBuilder();
      builder.append("total cores in use=" + dict.getInt("totalCores") + "\n");
      builder.append("total allocated cores=" + dict.getInt("allocatedCores") + "\n");
      builder.append("in compliance=" + dict.getBoolean("inCompliance") + "\n");
      builder.append("disabling now=" + dict.getBoolean("disableNow") + "\n");
      JsonArray servers = dict.getArray("servers");
      for (int i = 0; i < servers.size(); i++) {
        builder.append(servers.getDict(i).getString("host") + "=" + servers.getDict(i).getInt("cores") + "\n");
      }

      String ret = builder.toString();
      String[] lines = ret.split("\\n");
      return new ResultSetImpl(lines);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private ResultSet describeServerHeath(String dbName) {
    try {
      syncSchema();

      if (!common.haveProLicense()) {
        throw new InsufficientLicense("You must have a pro license to describe server health");
      }

      List<Map<String, String>> serverStatsData = new ArrayList<>();

      DatabaseServer.Shard[] shards = common.getServersConfig().getShards();
      for (int j = 0; j < shards.length; j++) {
        DatabaseServer.Shard shard = shards[j];
        DatabaseServer.Host[] replicas = shard.getReplicas();
        for (int i = 0; i < replicas.length; i++) {
          DatabaseServer.Host replica = replicas[i];
          Map<String, String> line = new HashMap<>();
          line.put("host", replica.getPrivateAddress() + ":" + replica.getPort());
          line.put("shard", String.valueOf(j));
          line.put("replica", String.valueOf(i));
          line.put("dead", String.valueOf(replica.isDead()));
          line.put("master", String.valueOf(shard.getMasterReplica() == i));
          serverStatsData.add(line);
        }
      }
      return new ResultSetImpl(serverStatsData);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private ResultSet describeSchemaVersion(String dbName) {
    try {
      if (!common.haveProLicense()) {
        throw new InsufficientLicense("You must have a pro license to describe schema version");
      }

      List<Map<String, String>> serverStatsData = new ArrayList<>();

      DatabaseServer.Shard[] shards = common.getServersConfig().getShards();
      for (int j = 0; j < shards.length; j++) {
        DatabaseServer.Shard shard = shards[j];
        DatabaseServer.Host[] replicas = shard.getReplicas();
        for (int i = 0; i < replicas.length; i++) {
          DatabaseServer.Host replica = replicas[i];
          Map<String, String> line = new HashMap<>();
          line.put("host", replica.getPrivateAddress() + ":" + replica.getPort());
          line.put("shard", String.valueOf(j));
          line.put("replica", String.valueOf(i));


          ComObject cobj = new ComObject();
          cobj.put(ComObject.Tag.dbName, "__none__");
          cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
          cobj.put(ComObject.Tag.method, "getSchema");
          String command = "DatabaseServer:ComObject:getSchema:";
          byte[] ret = null;
          try {
            ret = send(null, j, i, command, cobj, Replica.specified);
            ComObject retObj = new ComObject(ret);
            DatabaseCommon tmpCommon = new DatabaseCommon();
            tmpCommon.deserializeSchema(retObj.getByteArray(ComObject.Tag.schemaBytes));
            line.put("version", String.valueOf(tmpCommon.getSchemaVersion()));
          }
          catch (Exception e) {
            logger.error("Error getting schema from server: shard=" + j + ", replica=" + i, e);
          }

          serverStatsData.add(line);
        }
      }
      return new ResultSetImpl(serverStatsData);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private ResultSetImpl describeServerStats(final String dbName) throws ExecutionException, InterruptedException {
    while (true) {
      try {
        syncSchema();

        if (!common.haveProLicense()) {
          throw new InsufficientLicense("You must have a pro license to describe server stats");
        }

        List<Map<String, String>> serverStatsData = new ArrayList<>();

        List<Future<Map<String, String>>> futures = new ArrayList<>();
        for (int i = 0; i < getShardCount(); i++) {
          for (int j = 0; j < getReplicaCount(); j++) {
            final int shard = i;
            final int replica = j;
            futures.add(executor.submit(new Callable<Map<String, String>>(){
              @Override
              public Map<String, String> call() throws Exception {
                ComObject cobj = new ComObject();
                cobj.put(ComObject.Tag.dbName, "__none__");
                cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
                cobj.put(ComObject.Tag.method, "getOSStats");
                String command = "DatabaseServer:ComObject:getOSStats:";
                byte[] ret = send(null, shard, replica, command, cobj, DatabaseClient.Replica.specified);
                ComObject retObj = new ComObject(ret);

                double resGig = retObj.getDouble(ComObject.Tag.resGig);
                double cpu = retObj.getDouble(ComObject.Tag.cpu);
                double javaMemMin = retObj.getDouble(ComObject.Tag.javaMemMin);
                double javaMemMax = retObj.getDouble(ComObject.Tag.javaMemMax);
                double recRate = retObj.getDouble(ComObject.Tag.avgRecRate) / 1000000000d;
                double transRate = retObj.getDouble(ComObject.Tag.avgTransRate) / 1000000000d;
                String diskAvail = retObj.getString(ComObject.Tag.diskAvail);
                String host = retObj.getString(ComObject.Tag.host);
                int port = retObj.getInt(ComObject.Tag.port);

                Map<String, String> line = new HashMap<>();

                line.put("host", host + ":" + port);
                line.put("cpu", String.format("%.0f", cpu));
                line.put("resGig", String.format("%.2f", resGig));
                line.put("javaMemMin", String.format("%.2f", javaMemMin));
                line.put("javaMemMax", String.format("%.2f", javaMemMax));
                line.put("receive", String.format("%.4f", recRate));
                line.put("transmit", String.format("%.4f", transRate));
                line.put("diskAvail", diskAvail);
                return line;
              }
            }));

          }
        }

        for (Future<Map<String, String>> future : futures) {
          try {
            serverStatsData.add(future.get());
          }
          catch (Exception e) {
            logger.error("Error getting stats", e);
          }
        }
        return new ResultSetImpl(serverStatsData);
      }
      catch (Exception e) {
        int index = ExceptionUtils.indexOfThrowable(e, SchemaOutOfSyncException.class);
        if (-1 != index) {
          continue;
        }
        throw new DatabaseException(e);
      }
    }

  }

  class Entry {
    public Entry(String table, String index, int shard, String result) {
      this.table = table;
      this.index = index;
      this.shard = shard;
      this.result = result;
    }

    private String getKey() {
      return table + ":" + index + ":" + shard;
    }
    private String table;
    private String index;
    private int shard;
    private String result;
  }

  private ResultSet describeShards(String dbName) throws IOException, ExecutionException, InterruptedException {
    while (true) {
      try {
        syncSchema();

//        if (!common.haveProLicense()) {
//          throw new InsufficientLicense("You must have a pro license to describe shards");
//        }

        StringBuilder ret = new StringBuilder();

        Map<String, Entry> entries = new HashMap<>();
        Repartitioner.GlobalIndexCounts counts = Repartitioner.getIndexCounts(dbName, this);
        for (Map.Entry<String, Repartitioner.TableIndexCounts> tableEntry : counts.getTables().entrySet()) {
          for (Map.Entry<String, Repartitioner.IndexCounts> indexEntry : tableEntry.getValue().getIndices().entrySet()) {
            ConcurrentHashMap<Integer, Long> currCounts = indexEntry.getValue().getCounts();
            for (Map.Entry<Integer, Long> countEntry : currCounts.entrySet()) {
              Entry entry = new Entry(tableEntry.getKey(), indexEntry.getKey(), countEntry.getKey(), "Table=" +
                  tableEntry.getKey() + ", Index=" + indexEntry.getKey() +
                  ", Shard=" + countEntry.getKey() + ", count=" + countEntry.getValue() + "\n");
              entries.put(entry.getKey(), entry);
            }
          }
        }

        //    List<Future<Entry>> futures = new ArrayList<>();
        //    for (final Map.Entry<String, TableSchema> table : getCommon().getTables(dbName).entrySet()) {
        //      for (final Map.Entry<String, IndexSchema> indexSchema : table.getValue().getIndexes().entrySet()) {
        //        final String command = "DatabaseServer:getPartitionSize:1:" + getCommon().getSchemaVersion() + ":" +
        //            dbName + ":" + table.getKey() + ":" + indexSchema.getKey();
        //        for (int i = 0; i < getShardCount(); i++) {
        //          final int currShard = i;
        //          futures.add(executor.submit(new Callable<Entry>(){
        //            @Override
        //            public Entry call() throws Exception {
        //              byte[] currRet = send(null, currShard, 0, command, null, DatabaseClient.Replica.master);
        //              DataInputStream in = new DataInputStream(new ByteArrayInputStream(currRet));
        //              long serializationVersion = DataUtil.readVLong(in);
        //              long count = in.readLong();
        //              return new Entry(table.getKey(), indexSchema.getKey(), currShard, "Table=" + table.getKey() + ", Index=" + indexSchema.getKey() +
        //                  ", Shard=" + currShard + ", count=" + count + "\n");
        //            }
        //          }));
        //        }
        //      }
        //    }
        //    Map<String, Entry> entries = new HashMap<>();
        //    for (Future<Entry> future : futures) {
        //      Entry entry = future.get();
        //      entries.put(entry.getKey(), entry);
        //    }

        for (final Map.Entry<String, TableSchema> table : getCommon().getTables(dbName).entrySet()) {
          for (final Map.Entry<String, IndexSchema> indexSchema : table.getValue().getIndexes().entrySet()) {
            int shard = 0;
            TableSchema.Partition[] partitions = indexSchema.getValue().getCurrPartitions();
            TableSchema.Partition[] lastPartitions = indexSchema.getValue().getLastPartitions();
            for (int i = 0; i < partitions.length; i++) {
              String key = "[null]";
              if (partitions[i].getUpperKey() != null) {
                key = DatabaseCommon.keyToString(partitions[i].getUpperKey());
              }
              String lastKey = "[null]";
              if (lastPartitions != null && lastPartitions[i].getUpperKey() != null) {
                lastKey = DatabaseCommon.keyToString(lastPartitions[i].getUpperKey());
              }
              ret.append("Table=" + table.getKey() + ", Index=" + indexSchema.getKey() + ", shard=" + shard + ", key=" +
                  key).append(", lastKey=").append(lastKey).append("\n");
              shard++;
            }
            for (int i = 0; i < getShardCount(); i++) {
              ret.append(entries.get(table.getKey() + ":" + indexSchema.getKey() + ":" + i).result);
            }
          }
        }

        String retStr = ret.toString();
        String[] lines = retStr.split("\\n");
        return new ResultSetImpl(lines);
      }
      catch (Exception e) {
        int index = ExceptionUtils.indexOfThrowable(e, SchemaOutOfSyncException.class);
        if (-1 != index) {
          continue;
        }
        throw new DatabaseException(e);
      }
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
    String tableName = statement.getTable().getName().toLowerCase();
    ColDataType type = statement.getDataType();
    String columnName = statement.getColumnName().toLowerCase();

    if (operation.equalsIgnoreCase("add")) {
      doAddColumn(dbName, tableName, columnName, type);
    }
    else if (operation.equalsIgnoreCase("drop")) {
      doDropColumn(dbName, tableName, columnName);
    }
    return 1;
  }

  private void doDropColumn(String dbName, String tableName, String columnName) throws IOException {

    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.dbName, dbName);
    cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
    cobj.put(ComObject.Tag.tableName, tableName);
    cobj.put(ComObject.Tag.columnName, columnName);
    cobj.put(ComObject.Tag.method, "dropColumn");
    cobj.put(ComObject.Tag.masterSlave, "master");
    String command = "DatabaseServer:ComObject:dropColumn:";
    byte[] ret = sendToMaster(command, cobj);
    ComObject retObj = new ComObject(ret);
    common.deserializeSchema(retObj.getByteArray(ComObject.Tag.schemaBytes));
  }

  private void doAddColumn(String dbName, String tableName, String columnName, ColDataType type) throws IOException {

    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.dbName, dbName);
    cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
    cobj.put(ComObject.Tag.method, "addColumn");
    cobj.put(ComObject.Tag.tableName, tableName);
    cobj.put(ComObject.Tag.columnName, columnName);
    cobj.put(ComObject.Tag.dataType, type.getDataType());
    cobj.put(ComObject.Tag.masterSlave, "master");
    String command = "DatabaseServer:ComObject:addColumn:";
    byte[] ret = sendToMaster(command, cobj);
    ComObject retObj = new ComObject(ret);
    common.deserializeSchema(retObj.getByteArray(ComObject.Tag.schemaBytes));
  }

  private Object doDrop(String dbName, Statement statement) throws IOException {
    Drop drop = (Drop) statement;
    if (drop.getType().equalsIgnoreCase("table")) {
      String table = drop.getName().getName().toLowerCase();
      doTruncateTable(dbName, table);

      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.dbName, dbName);
      cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
      cobj.put(ComObject.Tag.method, "dropTable");
      cobj.put(ComObject.Tag.masterSlave, "master");
      cobj.put(ComObject.Tag.tableName, table);
      String command = "DatabaseServer:ComObject:dropTable:";
      byte[] ret = sendToMaster(command, cobj);
      ComObject retObj = new ComObject(ret);
      byte[] bytes = retObj.getByteArray(ComObject.Tag.schemaBytes);
      common.deserializeSchema(bytes);
    }
    else if (drop.getType().equalsIgnoreCase("index")) {
      String indexName = drop.getName().getName().toLowerCase();
      String tableName = drop.getName().getSchemaName().toLowerCase();

      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.dbName, dbName);
      cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
      cobj.put(ComObject.Tag.method, "dropIndex");
      cobj.put(ComObject.Tag.tableName, tableName);
      cobj.put(ComObject.Tag.indexName, indexName);
      cobj.put(ComObject.Tag.masterSlave, "master");
      String command = "DatabaseServer:ComObject:dropIndex:";
      byte[] ret = send(null, 0, 0, command, cobj, DatabaseClient.Replica.master);
      ComObject retObj = new ComObject(ret);
      common.deserializeSchema(retObj.getByteArray(ComObject.Tag.schemaBytes));
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

    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.dbName, dbName);
    cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
    cobj.put(ComObject.Tag.method, "truncateTable");
    cobj.put(ComObject.Tag.tableName, table);
    cobj.put(ComObject.Tag.phase, "secondary");
    String command = "DatabaseServer:ComObject:truncateTable:";

    Random rand = new Random(System.currentTimeMillis());
    sendToAllShards(null, rand.nextLong(), command, cobj, Replica.def);

    cobj.put(ComObject.Tag.phase, "primary");
    command = "DatabaseServer:ComObject:truncateTable:";

    rand = new Random(System.currentTimeMillis());
    sendToAllShards(null, rand.nextLong(), command, cobj, Replica.def);
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
    if (index.getType() != null) {
      statement.setIsUnique(index.getType().equalsIgnoreCase("unique"));
    }
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
      String command = "DatabaseServer:ComObject:createTable:";

      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.dbName, dbName);
      cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
      cobj.put(ComObject.Tag.method, "createTable");
      cobj.put(ComObject.Tag.masterSlave, "master");
      cobj.put(ComObject.Tag.createTableStatement, createTableStatement.serialize());

      byte[] ret = sendToMaster(command, cobj);
      ComObject retObj = new ComObject(ret);
      common.deserializeSchema(retObj.getByteArray(ComObject.Tag.schemaBytes));

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
      String command = "DatabaseServer:ComObject:insertIndexEntryByKey:";

      ComObject cobj = serializeInsertKey(dbName, tableName, keyInfo, primaryKeyIndexName, primaryKey);

      cobj.put(ComObject.Tag.dbName, dbName);
      cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
      cobj.put(ComObject.Tag.method, "insertIndexEntryByKey");
      send("DatabaseServer:insertIndexEntryByKey", keyInfo.shard, rand.nextLong(), command, cobj, DatabaseClient.Replica.def);
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  private ComObject serializeInsertKey(String dbName, String tableName, KeyInfo keyInfo,
                                    String primaryKeyIndexName, Object[] primaryKey) throws IOException {
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.serializationVersion, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
    cobj.put(ComObject.Tag.dbName, dbName);
    cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
    cobj.put(ComObject.Tag.tableName, tableName);
    cobj.put(ComObject.Tag.indexName, keyInfo.indexSchema.getKey());
    cobj.put(ComObject.Tag.isExcpliciteTrans, isExplicitTrans());
    cobj.put(ComObject.Tag.isCommitting, isCommitting());
    cobj.put(ComObject.Tag.transactionId, getTransactionId());
    byte[] keyBytes = DatabaseCommon.serializeKey(common.getTables(dbName).get(tableName), keyInfo.indexSchema.getKey(), keyInfo.key);
    cobj.put(ComObject.Tag.keyBytes, keyBytes);
    byte[] primaryKeyBytes = DatabaseCommon.serializeKey(common.getTables(dbName).get(tableName), primaryKeyIndexName, primaryKey);
    cobj.put(ComObject.Tag.primaryKeyBytes, primaryKeyBytes);

    return cobj;
  }

  class FailedToInsertException extends RuntimeException {
    public FailedToInsertException(String msg) {
      super(msg);
    }
  }

  public void insertKeyWithRecord(String dbName, String tableName, KeyInfo keyInfo, Record record) {
    try {
      String command = "DatabaseServer:ComObject:insertIndexEntryByKeyWithRecord:";

      ComObject cobj = serializeInsertKeyWithRecord(dbName, tableName, keyInfo, record);
      cobj.put(ComObject.Tag.dbName, dbName);
      cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
      cobj.put(ComObject.Tag.method, "insertIndexEntryByKeyWithRecord");

      int replicaCount = getReplicaCount();
      Exception lastException = null;
      //for (int i = 0; i < replicaCount; i++) {
      try {
        byte[] ret = send(null, keyInfo.shard, 0, command, cobj, DatabaseClient.Replica.def);
        if (ret == null) {
          throw new FailedToInsertException("No response for key insert");
        }
        ComObject retObj = new ComObject(ret);
        int retVal = retObj.getInt(ComObject.Tag.count);
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
          throw (SchemaOutOfSyncException) lastException;
        }
        throw new DatabaseException(lastException);
      }
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  private ComObject serializeInsertKeyWithRecord(String dbName, String tableName,
                                            KeyInfo keyInfo, Record record) throws IOException {
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.serializationVersion, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
    cobj.put(ComObject.Tag.dbName, dbName);
    cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
    cobj.put(ComObject.Tag.tableName, tableName);
    cobj.put(ComObject.Tag.indexName, keyInfo.indexSchema.getKey());
    cobj.put(ComObject.Tag.id, record.getId());
    cobj.put(ComObject.Tag.isExcpliciteTrans, isExplicitTrans());
    cobj.put(ComObject.Tag.isCommitting, isCommitting());
    cobj.put(ComObject.Tag.transactionId, getTransactionId());
    byte[] recordBytes = record.serialize(common);
    cobj.put(ComObject.Tag.recordBytes, recordBytes);
    cobj.put(ComObject.Tag.keyBytes, DatabaseCommon.serializeKey(common.getTables(dbName).get(tableName), keyInfo.indexSchema.getKey(), keyInfo.key));

    return cobj;
  }

  public void deleteKey(String dbName, String tableName, KeyInfo keyInfo, String primaryKeyIndexName, Object[] primaryKey) {
    String command = "DatabaseServer:ComObject:deleteIndexEntryByKey:";
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.dbName, dbName);
    cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
    cobj.put(ComObject.Tag.method, "deleteIndexEntryByKey");
    cobj.put(ComObject.Tag.tableName, tableName);
    cobj.put(ComObject.Tag.indexName, keyInfo.indexSchema.getKey());
    cobj.put(ComObject.Tag.primaryKeyIndexName, primaryKeyIndexName);
    cobj.put(ComObject.Tag.isExcpliciteTrans, isExplicitTrans());
    cobj.put(ComObject.Tag.isCommitting, isCommitting());
    cobj.put(ComObject.Tag.transactionId, getTransactionId());

    cobj.put(ComObject.Tag.serializationVersion, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
    cobj.put(ComObject.Tag.keyBytes, DatabaseCommon.serializeKey(common.getTables(dbName).get(tableName), keyInfo.indexSchema.getKey(), keyInfo.key));
    cobj.put(ComObject.Tag.primaryKeyBytes, DatabaseCommon.serializeKey(common.getTables(dbName).get(tableName), primaryKeyIndexName, primaryKey));

    send("DatabaseServer:deleteIndexEntryByKey", keyInfo.shard, rand.nextLong(), command, cobj, DatabaseClient.Replica.def);
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
      if (addedRecords.get((long) i) == null) {
        logger.error("missing record: id=" + i + ", count=0");
      }
    }
    logger.info("finished checkAddedRecords");
    return null;
  }

  public class InsertRequest {
    private String dbName;
    private InsertStatementImpl insertStatement;
    private ParameterHandler parms;
  }

  class PreparedInsert {
    String dbName;
    String tableName;
    KeyInfo keyInfo;
    Record record;
    Object[] primaryKey;
    String primaryKeyIndexName;
    public TableSchema tableSchema;
    public List<String> columnNames;
    public List<Object> values;
    public long id;
    public String indexName;
  }

  public List<PreparedInsert> prepareInsert(InsertRequest request, long nonTransId) throws UnsupportedEncodingException, SQLException {
    List<PreparedInsert> ret = new ArrayList<>();

    String dbName = request.dbName;

    List<String> columnNames;
    List<Object> values;

    String tableName = request.insertStatement.getTableName();

    TableSchema tableSchema = common.getTables(dbName).get(tableName);
    if (tableSchema == null) {
      throw new DatabaseException("Table does not exist: name=" + tableName);
    }

    long id = 0;
    for (IndexSchema indexSchema : tableSchema.getIndexes().values()) {
      if (indexSchema.isPrimaryKey() && indexSchema.getFields()[0].equals("_id")) {
        id = allocateId(dbName);
      }
    }

    long transId = 0;
    if (!isExplicitTrans.get()) {
      transId = nonTransId;
    }
    else {
      transId = transactionId.get();
    }
    Record record = prepareRecordForInsert(request.insertStatement, tableSchema, id);
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
        }
        PreparedInsert insert = new PreparedInsert();
        insert.dbName = dbName;
        insert.keyInfo = keyInfo;
        insert.record = record;
        insert.tableName = tableName;
        insert.primaryKeyIndexName = primaryKey.indexSchema.getKey();
        insert.primaryKey = primaryKey.key;
        insert.tableSchema = tableSchema;
        insert.columnNames = columnNames;
        insert.values = values;
        insert.id = id;
        insert.indexName = keyInfo.indexSchema.getKey();
        ret.add(insert);
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
    return ret;
  }

  public int doInsert(String dbName, InsertStatementImpl insertStatement, ParameterHandler parms) throws IOException, SQLException {
    long previousSchemaVersion = common.getSchemaVersion();
    InsertRequest request = new InsertRequest();
    request.dbName = dbName;
    request.insertStatement = insertStatement;
    request.parms = parms;
    int insertCountCompleted = 0;
    while (true) {
      try {
        if (batch.get() != null) {
          batch.get().add(request);
        }
        else {
          long nonTransId = 0;
          if (!isExplicitTrans.get()) {
             nonTransId = allocateId(dbName);
          }

          List<PreparedInsert> inserts = prepareInsert(request, nonTransId);
          for (int i = 0; i < inserts.size(); i++) {
            if (i < insertCountCompleted) {
              continue;
            }
            PreparedInsert insert = inserts.get(i);
            if (insert.keyInfo.indexSchema.getValue().isPrimaryKey()) {
              insertKeyWithRecord(dbName, insertStatement.getTableName(), insert.keyInfo, insert.record);
            }
            else {
              insertKey(dbName, insertStatement.getTableName(), insert.keyInfo, insert.primaryKeyIndexName, insert.primaryKey);
            }
            insertCountCompleted++;
            if (previousSchemaVersion != common.getSchemaVersion()) {
              throw new SchemaOutOfSyncException();
            }
          }
        }
        break;
      }
      catch (FailedToInsertException e) {
        logger.error(e.getMessage());
        continue;
      }
      catch (Exception e) {
        int index = ExceptionUtils.indexOfThrowable(e, SchemaOutOfSyncException.class);
        if (-1 != index) {
          continue;
        }
        throw new DatabaseException(e);
      }
    }
    return 1;
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
        String command = "DatabaseServer:ComObject:allocateRecordIds:";
        byte[] ret = sendToMaster(command, cobj);
        ComObject retObj = new ComObject(ret);
        nextId.set(retObj.getLong(ComObject.Tag.nextId));
        maxAllocatedId.set(retObj.getLong(ComObject.Tag.maxId));
        id = nextId.getAndIncrement();
      }
    }
    return id;
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
            switch (fieldSchema.getType()) {
              case VARCHAR:
              case NVARCHAR:
              case LONGVARCHAR:
              case LONGNVARCHAR:
              case CLOB:
              case NCLOB:
                String str = new String((byte[]) value, "utf-8");
                if (str.length() > fieldSchema.getWidth()) {
                  throw new SQLException("value too long: field=" + fieldSchema.getName() + ", width=" + fieldSchema.getWidth());
                }
                break;
              case VARBINARY:
              case LONGVARBINARY:
              case BLOB:
                if (((byte[]) value).length > fieldSchema.getWidth()) {
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
        if (!common.haveProLicense()) {
          throw new InsufficientLicense("You must have a pro license to execute joins");
        }
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
          Column column = (Column) groupColumns.get(i);
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
      parameter.setParmName(((JdbcNamedParameter) whereExpression).getName());
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
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.dbName, dbName);
    cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
    cobj.put(ComObject.Tag.method, "isRepartitioningComplete");
    String command = "DatabaseServer:ComObject:isRepartitioningComplete:";
    byte[] bytes = sendToMaster(command, cobj);
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
    String command = "DatabaseServer:ComObject:getPartitionSize:";
    byte[] bytes = send(null, shard, replica, command, cobj, DatabaseClient.Replica.specified);
    ComObject retObj = new ComObject(bytes);
    return retObj.getLong(ComObject.Tag.size);
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

  private Object syncSchemaMutex = new Object();

  public void syncSchema(long serverVersion) {
    synchronized (syncSchemaMutex) {
      if (serverVersion > common.getSchemaVersion()) {
        syncSchema();
      }
    }
  }

  public void syncSchema() {
    synchronized (syncSchemaMutex) {
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.dbName, "__none__");
      cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
      cobj.put(ComObject.Tag.method, "getSchema");
      String command = "DatabaseServer:ComObject:getSchema:";
      try {

        byte[] ret = null;
        try {
          ret = sendToMaster(command, cobj);
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
              ret = send(null, 0, replica, command, cobj, Replica.specified);
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

          DatabaseServer.ServersConfig serversConfig = common.getServersConfig();
          for (int i = 0; i < serversConfig.getShards().length; i++) {
            for (int j = 0; j < serversConfig.getShards()[0].getReplicas().length; j++) {
              servers[i][j].dead = serversConfig.getShards()[i].getReplicas()[j].isDead();
            }
          }

          logger.info("Schema received from server: currVer=" + common.getSchemaVersion());
        }
      }
      catch (Exception t) {
        throw new DatabaseException(t);
      }
    }
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

  public void getConfig() {
    try {
      long auth_user = rand.nextLong();
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.dbName, "__none__");
      cobj.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
      cobj.put(ComObject.Tag.method, "getConfig");
      String command = "DatabaseServer:ComObject:getConfig:";

      byte[] ret = send(null, selectShard(0), auth_user, command, cobj, DatabaseClient.Replica.def);
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
    String command = "DatabaseServer:ComObject:beginRebalance:";
    sendToMaster(command, cobj);
  }
}
