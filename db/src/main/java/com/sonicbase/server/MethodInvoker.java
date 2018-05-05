package com.sonicbase.server;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.*;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.socket.DeadServerException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.exception.ExceptionUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by lowryda on 7/28/17.
 */
public class MethodInvoker {
  private Logger logger;

  private final BulkImportManager bulkImportManager;
  private final DeleteManager deleteManagerImpl;
  private final SnapshotManager deltaManager;
  private final UpdateManager updateManager;
  private final TransactionManager transactionManager;
  private final ReadManager readManager;
  private final LogManager logManager;
  private final SchemaManager schemaManager;
  private final DatabaseServer server;
  private final DatabaseCommon common;
  private final MonitorManager monitorManager;
  private final BackupManager backupManager;
  private final OSStatsManager osStatsManager;
  private final MasterManager masterManager;
  private boolean shutdown;
  private AtomicInteger testWriteCallCount = new AtomicInteger();
  private ConcurrentHashMap<String, Method> methodMap = new ConcurrentHashMap<>();

  public MethodInvoker(DatabaseServer server, BulkImportManager bulkImportManager, DeleteManager deleteManagerImpl,
                       SnapshotManager deltaManager, UpdateManager updateManager, TransactionManager transactionManager,
                       ReadManager readManager, LogManager logManager, SchemaManager schemaManager, MonitorManager monitorManager,
                       BackupManager backupManager, OSStatsManager osStatsManager, MasterManager masterManager) {
    this.server = server;
    this.common = server.getCommon();
    this.bulkImportManager = bulkImportManager;
    this.deleteManagerImpl = deleteManagerImpl;
    this.deltaManager = deltaManager;
    this.updateManager = updateManager;
    this.transactionManager = transactionManager;
    this.readManager = readManager;
    this.logManager = logManager;
    this.schemaManager = schemaManager;
    this.monitorManager = monitorManager;
    this.backupManager = backupManager;
    this.osStatsManager = osStatsManager;
    this.masterManager = masterManager;

    Method[] methods = MethodInvoker.class.getMethods();
    for (Method method : methods) {
      Class[] parms = method.getParameterTypes();
      if (parms.length == 2) {
        if (parms[0] == ComObject.class && parms[1] == boolean.class) {
          methodMap.put(method.getName(), method);
        }
      }
    }

    logger = new Logger(null/*server.getDatabaseClient()*/);
  }

  public void shutdown() {
    this.shutdown = true;
  }

  public ReadManager getReadManager() {
    return readManager;
  }

  class ReplicaFuture {
    private Future future;
    private int replica;
  }

  public int getTestWriteCallCount() {
    return testWriteCallCount.get();
  }

  private static Set<String> priorityCommands = new HashSet<>();

  static {
    priorityCommands.add("logError");
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
    priorityCommands.add("doGetBackupSizes");
    priorityCommands.add("doGetRestoreSizes");
    priorityCommands.add("getBackupStatus");
    priorityCommands.add("doBackupFileSystem");
    priorityCommands.add("isBackupComplete");
    priorityCommands.add("finishBackup");
    priorityCommands.add("sendLogsToPeer");
    priorityCommands.add("isEntireRestoreComplete");
    priorityCommands.add("isEntireBackupComplete");
    priorityCommands.add("isServerReloadFinished");
    priorityCommands.add("licenseCheckin");
    //priorityCommands.add("doPopulateIndex");
  }

  public byte[] invokeMethod(final byte[] requestBytes, long logSequence0, long logSequence1,
                             boolean replayedCommand, boolean enableQueuing, AtomicLong timeLogging, AtomicLong handlerTime) {
    try {
      if (shutdown) {
        throw new DatabaseException("Shutdown in progress");
      }
      ComObject request = new ComObject(requestBytes);
      String methodStr = request.getString(ComObject.Tag.method);

      if (server.isApplyingQueuesAndInteractive()) {
        replayedCommand = true;
      }

      if (server.isRecovered() && server.shouldDisableNow() && server.isUsingMultipleReplicas()) {
        if (!methodStr.equals("healthCheck") && !methodStr.equals("healthCheckPriority") &&
            !methodStr.equals("getConfig") &&
            !methodStr.equals("licenseCheckin") &&
            !methodStr.equals("getSchema") && !methodStr.equals("getDbNames")) {
          throw new LicenseOutOfComplianceException("Licenses out of compliance");
        }
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
//      String queueCommand = "DatabaseServer:queueForOtherServer:1:" + SnapshotManagerImpl.SERIALIZATION_VERSION + ":1:__none__:" + i;
//      replicas[common.getServersConfig().getShards()[shard].getMasterReplica()].do_send(
//          null, queueCommand, bytesOut.toByteArray())
//
      if (methodStr.equals("queueForOtherServer")) {
        try {
          ComObject header = request.getObject(ComObject.Tag.header);
          Integer replica = header.getInt(ComObject.Tag.replica);
          String innerMethod = header.getString(ComObject.Tag.method);
          request.put(ComObject.Tag.method, innerMethod);
          logManager.logRequestForPeer(requestBytes, innerMethod, System.currentTimeMillis(), logManager.getNextSequencenNum(), replica);
          return null;
        }
        catch (Exception e) {
          throw new DatabaseException(e);
        }
      }

      Long existingSequence0 = getExistingSequence0(request);
      Long existingSequence1 = getExistingSequence1(request);

      DatabaseServer.LogRequest logRequest = logManager.logRequest(requestBytes, enableQueuing, methodStr,
          existingSequence0, existingSequence1, timeLogging);
      ComObject ret = null;

      if (!replayedCommand && !server.isRecovered() && !priorityCommands.contains(methodStr)) {
        throw new DeadServerException("Server not running: method=" + methodStr);
      }

      List<ReplicaFuture> futures = new ArrayList<>();
      long sequence0 = logRequest == null ? logSequence0 : logRequest.getSequences0()[0];
      long sequence1 = logRequest == null ? logSequence1 : logRequest.getSequences1()[0];

      ComObject newMessage = new ComObject(requestBytes);
      if (logRequest != null) {
        newMessage.put(ComObject.Tag.sequence0, sequence0);
        newMessage.put(ComObject.Tag.sequence1, sequence1);
      }
      if (!server.onlyQueueCommands() || !enableQueuing) {
        ServersConfig.Shard currShard = common.getServersConfig().getShards()[server.getShard()];
        try {
          long handleBegin = System.nanoTime();
          request.put(ComObject.Tag.sequence0, sequence0);
          request.put(ComObject.Tag.sequence1, sequence1);
          if (existingSequence0 == null) {
            request.put(ComObject.Tag.currRequestIsMaster, true);
          }
          else {
            request.put(ComObject.Tag.currRequestIsMaster, false);
          }

          Method method = methodMap.get(methodStr);//getClass().getMethod(methodStr, ComObject.class, boolean.class);
          ret = (ComObject) method.invoke(this, request, replayedCommand);
          if (handlerTime != null) {
            handlerTime.addAndGet(System.nanoTime() - handleBegin);
          }
          if (ret == null) {
            ret = new ComObject();
            ret.put(ComObject.Tag.sequence0, sequence0);
            ret.put(ComObject.Tag.sequence1, sequence1);
          }
          else {
            ret.put(ComObject.Tag.sequence0, sequence0);
            ret.put(ComObject.Tag.sequence1, sequence1);
          }
        }
        catch (InvocationTargetException e) {
          if (e.getCause() instanceof SchemaOutOfSyncException) {
            throw (SchemaOutOfSyncException)e.getCause();
          }
          throw new DatabaseException(e);
        }
        catch (SchemaOutOfSyncException e) {
          throw e;
        }
        catch (Exception e) {
          if (e.getCause() instanceof SchemaOutOfSyncException) {
            throw (SchemaOutOfSyncException)e.getCause();
          }
          throw new DatabaseException(e);
        }

        for (ReplicaFuture future : futures) {
          try {
            future.future.get();
          }
          catch (Exception e) {
            int index = ExceptionUtils.indexOfThrowable(e, DeadServerException.class);
            if (-1 != index) {
              logManager.logRequestForPeer(newMessage.serialize(), methodStr, sequence0, sequence1, future.replica);
            }
            else {
              logger.error("Error sending command to slave", e);
            }
          }
        }
      }
      if (logRequest != null) {
        logRequest.getLatch().await();
      }
      if (ret == null) {
        ret = new ComObject();
      }

      return ret.serialize();
    }
    catch (InterruptedException e) {
      throw new DatabaseException(e);
    }
    catch (DeadServerException | SchemaOutOfSyncException e) {
      throw e; //don't log
    }
    catch (Exception e) {
      if (-1 != ExceptionUtils.indexOfThrowable(e, SchemaOutOfSyncException.class)) {
        throw new DatabaseException(e); //don't log
      }
      if (e.getCause() instanceof SchemaOutOfSyncException) {
        throw new DatabaseException(e);
      }
      if (-1 != ExceptionUtils.indexOfThrowable(e, UniqueConstraintViolationException.class)) {
        throw new DatabaseException(e); //don't log
      }
      if (e.getMessage().contains("Shutdown in progress")) {
        throw new DatabaseException(e);
      }
      logger.error("Error handling command: method=" + new ComObject(requestBytes).getString(ComObject.Tag.method), e);
      throw new DatabaseException(e);
    }
  }

  private Long getExistingSequence0(ComObject request) {
    return request.getLong(ComObject.Tag.sequence0);
  }

  private Long getExistingSequence1(ComObject request) {
    return request.getLong(ComObject.Tag.sequence1);
  }

  public ComObject startStreaming(final ComObject cobj, boolean replayedCommand) {
    return server.getStreamManager().startStreaming(cobj);
  }

  public ComObject stopStreaming(final ComObject cobj, boolean replayedCommand) {
    return server.getStreamManager().stopStreaming(cobj);
  }

  public ComObject isStreamingStarted(ComObject cobj, boolean replayedCommand) {
    boolean started = server.getStreamManager().isStreamingStarted();
    ComObject retObj = new ComObject();
    retObj.put(ComObject.Tag.isStarted, started);
    return retObj;
  }

  public ComObject processMessages(ComObject cobj, boolean replayedCommand) {
    return server.getStreamManager().processMessages(cobj);
  }

  public ComObject executeProcedurePrimary(final ComObject cobj, boolean replayedCommand) {
    return server.executeProcedurePrimary(cobj);
  }

  public ComObject executeProcedure(final ComObject cobj, boolean replayedCommand) {
    return server.executeProcedure(cobj);
  }

  public ComObject cancelBulkImport(final ComObject cobj, boolean replayedCommand) {
    return bulkImportManager.cancelBulkImport(cobj);
  }

  public ComObject getBulkImportProgress(final ComObject cobj, boolean replayedCommand) {
    return bulkImportManager.getBulkImportProgress(cobj);
  }

  public ComObject getBulkImportProgressOnServer(final ComObject cobj, boolean replayedCommand) {
    return bulkImportManager.getBulkImportProgressOnServer(cobj);
  }

  public ComObject startBulkImportOnServer(ComObject cobj, boolean replayedCommand) {
    return bulkImportManager.startBulkImportOnServer(cobj);
  }

  public ComObject coordinateBulkImportForTable(final ComObject cobj, boolean replayedCommand) {
    return bulkImportManager.coordinateBulkImportForTable(cobj);
  }

  public ComObject startBulkImport(ComObject cobj, boolean replayedCommand) {
    return bulkImportManager.startBulkImport(cobj);
  }

  public ComObject licenseCheckin(ComObject cobj, boolean replayedCommand) {
    return server.getLicenseManager().licenseCheckin(cobj);
  }

  public ComObject areAllLongRunningCommandsComplete(ComObject cobj, boolean replayedCommand) {
    return server.areAllLongRunningCommandsComplete(cobj);
  }

  public ComObject createTable(ComObject cobj, boolean replayedCommand) {
    return schemaManager.createTable(cobj, replayedCommand);
  }

  public ComObject createTableSlave(ComObject cobj, boolean replayedCommand) {
    return schemaManager.createTableSlave(cobj, replayedCommand);
  }

  public ComObject dropTable(ComObject cobj, boolean replayedCommand) {
    return schemaManager.dropTable(cobj, replayedCommand);
  }

  public ComObject createDatabaseSlave(ComObject cobj, boolean replayedCommand) {
    return schemaManager.createDatabaseSlave(cobj, replayedCommand);
  }

  public ComObject createDatabase(ComObject cobj, boolean replayedCommand) {
    return schemaManager.createDatabase(cobj, replayedCommand);
  }

  public ComObject registerQueryForStats(ComObject cobj, boolean replayedCommand) {
    return monitorManager.registerQueryForStats(cobj, replayedCommand);
  }

  public ComObject registerStats(ComObject cobj, boolean replayedCommand) {
    return monitorManager.registerStats(cobj, replayedCommand);
  }


  public ComObject addColumnSlave(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaWriteLock(dbName).lock();
    try {
      return schemaManager.addColumnSlave(cobj, replayedCommand);
    }
    finally {
      common.getSchemaWriteLock(dbName).unlock();
    }
  }

  public ComObject addColumn(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaWriteLock(dbName).lock();
    try {
      return schemaManager.addColumn(cobj, replayedCommand);
    }
    finally {
      common.getSchemaWriteLock(dbName).unlock();
    }
  }

  public ComObject dropColumnSlave(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaWriteLock(dbName).lock();
    try {
      return schemaManager.dropColumnSlave(cobj, replayedCommand);
    }
    finally {
      common.getSchemaWriteLock(dbName).unlock();
    }
  }

  public ComObject dropColumn(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaWriteLock(dbName).lock();
    try {
      return schemaManager.dropColumn(cobj, replayedCommand);
    }
    finally {
      common.getSchemaWriteLock(dbName).unlock();
    }
  }

  public ComObject dropIndexSlave(ComObject cobj, boolean replayedCommand) {

    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaWriteLock(dbName).lock();
    try {
      return schemaManager.dropIndexSlave(cobj);
    }
    finally {
      common.getSchemaWriteLock(dbName).unlock();
    }
  }

  public ComObject dropIndex(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaWriteLock(dbName).lock();
    try {
      return schemaManager.dropIndex(cobj, replayedCommand);
    }
    finally {
      common.getSchemaWriteLock(dbName).unlock();
    }
  }

  public ComObject createIndexSlave(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaWriteLock(dbName).lock();
    try {
      return schemaManager.createIndexSlave(cobj);
    }
    finally {
      common.getSchemaWriteLock(dbName).unlock();
    }
  }

  public ComObject promoteEntireReplicaToMaster(ComObject cobj, boolean replayedCommand) {
    return masterManager.promoteEntireReplicaToMaster(cobj);
  }

  public ComObject electNewMaster(ComObject cobj, boolean replayedCommand) throws InterruptedException, IOException {
    return masterManager.electNewMaster(cobj);
  }

  public ComObject promoteToMaster(ComObject cobj, boolean replayedCommand) {
    return masterManager.promoteToMaster(cobj);
  }

  public ComObject markReplicaDead(ComObject cobj, boolean replayedCommand) {
    int replicaToKill = cobj.getInt(ComObject.Tag.replica);
    logger.info("Marking replica dead: replica=" + replicaToKill);
    for (int shard = 0; shard < server.getShardCount(); shard++) {
      common.getServersConfig().getShards()[shard].getReplicas()[replicaToKill].setDead(true);
    }
    common.saveSchema(server.getClient(), server.getDataDir());
    server.pushSchema();

    server.setReplicaDeadForRestart(replicaToKill);
    return null;
  }

  public ComObject markReplicaAlive(ComObject cobj, boolean replayedCommand) {
    int replicaToMarkAlive = cobj.getInt(ComObject.Tag.replica);
    logger.info("Marking replica alive: replica=" + replicaToMarkAlive);
    for (int shard = 0; shard < server.getShardCount(); shard++) {
      common.getServersConfig().getShards()[shard].getReplicas()[replicaToMarkAlive].setDead(false);
    }
    common.saveSchema(server.getClient(), server.getDataDir());
    server.pushSchema();

    server.setReplicaDeadForRestart(-1);
    return null;
  }

  public ComObject promoteToMasterAndPushSchema(ComObject cobj, boolean replayedCommand) {
    int shard = cobj.getInt(ComObject.Tag.shard);
    int replica = cobj.getInt(ComObject.Tag.replica);

    logger.info("promoting to master: shard=" + shard + ", replica=" + replica);
    common.getServersConfig().getShards()[shard].setMasterReplica(replica);
    common.saveSchema(server.getClient(), server.getDataDir());
    server.pushSchema();
    return null;
  }

  public ComObject getRepartitionerState(ComObject cobj, boolean replayedCommand) {
    return server.getPartitionManager().getRepartitionerState(cobj);
  }

  public ComObject isShardRepartitioningComplete(ComObject cobj, boolean replayedCommand) {
    return server.getPartitionManager().isShardRepartitioningComplete(cobj, replayedCommand);
  }

  public ComObject prepareForBackup(ComObject cobj, boolean replayedCommand) {
    return backupManager.prepareForBackup(cobj);
  }

  public ComObject doBackupFileSystem(final ComObject cobj, boolean replayedCommand) {
    return backupManager.doBackupFileSystem(cobj);
  }


  public ComObject doGetBackupSizes(final ComObject obj, boolean replayedCommand) {
    return backupManager.doGetBackupSizes(obj);
  }

  public ComObject doGetRestoreSizes(final ComObject obj, boolean replayedCommand) {
    return backupManager.doGetRestoreSizes(obj);
  }

  public ComObject getBackupStatus(final ComObject obj, boolean replayedCommand) {
    return backupManager.getBackupStatus(obj);
  }

  public ComObject getRestoreStatus(final ComObject obj, boolean replayedCommand) {
    return backupManager.getRestoreStatus(obj);
  }

  public ComObject doBackupAWS(final ComObject cobj, boolean replayedCommand) {
    return backupManager.doBackupAWS(cobj);
  }

  public ComObject isBackupComplete(ComObject cobj, boolean replayedCommand) {
    return backupManager.isBackupComplete(cobj);
  }

  public ComObject finishBackup(ComObject cobj, boolean replayedCommand) {
    return backupManager.finishBackup(cobj);
  }

  public ComObject isEntireBackupComplete(ComObject cobj, boolean replayedCommand) {
    return backupManager.isEntireBackupComplete(cobj);
  }

  public byte[] startBackup(ComObject cobj, boolean replayedCommand) {
    return backupManager.startBackup(cobj);
  }

  public ComObject getLastBackupDir(ComObject cobj, boolean replayedCommand) {
    return backupManager.getLastBackupDir(cobj);
  }

  public ComObject prepareForRestore(ComObject cobj, boolean replayedCommand) {
    return backupManager.prepareForRestore(cobj);
  }

  public ComObject doRestoreFileSystem(final ComObject cobj, boolean replayedCommand) {
    return backupManager.doRestoreFileSystem(cobj);
  }

  public ComObject doRestoreAWS(final ComObject cobj, boolean replayedCommand) {
    return backupManager.doRestoreAWS(cobj);
  }

  public ComObject isRestoreComplete(ComObject cobj, boolean replayedCommand) {
    return backupManager.isRestoreComplete(cobj);
  }

  public ComObject finishRestore(ComObject cobj, boolean replayedCommand) {
    return backupManager.finishRestore(cobj);
  }

  public ComObject isEntireRestoreComplete(ComObject cobj, boolean replayedCommand) {
    return backupManager.isEntireRestoreComplete(cobj);
  }

  public ComObject startRestore(final ComObject cobj, boolean replayedCommand) {
    return backupManager.startRestore(cobj);
  }

  public ComObject getFile(ComObject cobj, boolean replayedCommand) {
    try {
      String filename = cobj.getString(ComObject.Tag.filename);
      File file = new File(server.getInstallDir(), filename);
      if (!file.exists()) {
        return null;
      }
      try (FileInputStream fileIn = new FileInputStream(file)) {
        String ret = IOUtils.toString(fileIn, "utf-8");
        ComObject retObj = new ComObject();
        retObj.put(ComObject.Tag.fileContent, ret);
        return retObj;
      }
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public ComObject logError(ComObject cobj, boolean replayedCommand) {
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
        server.getClientErrorLogger().error(actualMsg.toString());
      }
      else {
        server.getErrorLogger().error(actualMsg.toString());
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
    return null;
  }

  public ComObject getOSStats(ComObject cobj, boolean replayedCommand) {
    return osStatsManager.getOSStats(cobj);
  }

  public ComObject getDbNames(ComObject cobj, boolean replayedCommand) {

    try {
      ComObject retObj = new ComObject();
      List<String> dbNames = server.getDbNames(server.getDataDir());
      ComArray array = retObj.putArray(ComObject.Tag.dbNames, ComObject.Type.stringType);
      for (String dbName : dbNames) {
        array.add(dbName);
      }
      return retObj;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public ComObject updateSchema(ComObject cobj, boolean replayedCommand) throws IOException {
    if (replayedCommand) {
      return null;
    }
    DatabaseCommon tempCommon = new DatabaseCommon();
    tempCommon.deserializeSchema(cobj.getByteArray(ComObject.Tag.schemaBytes));

//    if (getShard() == 0 &&
//        tempCommon.getServersConfig().getShards()[0].getMasterReplica() == getReplica()) {
//      return null;
//    }

    synchronized (common) {
      if (tempCommon.getSchemaVersion() > common.getSchemaVersion()) {
        common.deserializeSchema(cobj.getByteArray(ComObject.Tag.schemaBytes));
        common.saveSchema(server.getClient(), server.getDataDir());
      }
    }
    return null;
  }

  public ComObject prepareSourceForServerReload(ComObject cobj, boolean replayedCommand) {
    return backupManager.prepareSourceForServerReload(cobj);
  }

  public ComObject finishServerReloadForSource(ComObject cobj, boolean replayedCommand) {

    deltaManager.enableSnapshot(true);

    return null;
  }

  public ComObject isServerReloadFinished(ComObject cobj, boolean replayedCommand) {
    return backupManager.isServerReloadFinished(cobj);
  }

  public ComObject reloadServer(ComObject cobj, boolean replayedCommand) {
    return backupManager.reloadServer(cobj);
  }

  public ComObject getDatabaseFile(ComObject cobj, boolean replayedCommand) {
    return server.getDatabaseFile(cobj);
  }

  public ComObject updateServersConfig(ComObject cobj, boolean replayedCommand) {
    return server.updateServersConfig(cobj);
  }

  public ComObject healthCheck(ComObject cobj, boolean replayedCommand) {
    ComObject retObj = new ComObject();
    retObj.put(ComObject.Tag.status, "{\"status\" : \"ok\"}");
    retObj.put(ComObject.Tag.haveProLicense, server.haveProLicense());
    return retObj;
  }

  public ComObject healthCheckPriority(ComObject cobj, boolean replayedCommand) {
    ComObject retObj = new ComObject();
    retObj.put(ComObject.Tag.status, "{\"status\" : \"ok\"}");
    retObj.put(ComObject.Tag.haveProLicense, server.haveProLicense());
    return retObj;
  }

  @ExcludeRename
  public ComObject setMaxSequenceNum(ComObject cobj, boolean replayedCommand) {
    return logManager.setMaxSequenceNum(cobj);
  }

  @ExcludeRename
  public ComObject getRecoverProgress(ComObject cobj, boolean replayedCommand) {
    return server.getRecoverProgress();
  }

  public ComObject pushMaxSequenceNum(ComObject cobj, boolean replayedCommand) {
    logManager.pushMaxSequenceNum();
    return null;
  }

  public ComObject prepareToComeAlive(ComObject cobj, boolean replayedCommand) {
    return server.prepareToComeAlive(cobj);
  }

  public ComObject reconfigureCluster(ComObject cobj, boolean replayedCommand) {
    return server.reconfigureCluster(cobj);
  }

  public ComObject getConfig(ComObject cobj, boolean replayedCommand) {
    short serializationVersionNumber = cobj.getShort(ComObject.Tag.serializationVersion);
    try {
      byte[] bytes = common.serializeConfig(serializationVersionNumber);
      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.configBytes, bytes);
      return retObj;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public ComObject getSchema(ComObject cobj, boolean replayedCommand) {
    short serializationVersionNumber = cobj.getShort(ComObject.Tag.serializationVersion);
    try {

      if (cobj.getBoolean(ComObject.Tag.force) != null && cobj.getBoolean(ComObject.Tag.force)) {
        final ComObject cobj2 = new ComObject();
        cobj2.put(ComObject.Tag.dbName, "__none__");
        cobj2.put(ComObject.Tag.schemaVersion, common.getSchemaVersion());
        cobj2.put(ComObject.Tag.method, "getSchema");

        int threadCount = server.getShardCount() * server.getReplicationFactor();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(threadCount, threadCount, 10_000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
        try {
          logger.info("forcing schema sync: version=" + common.getSchemaVersion());
          List<Future> futures = new ArrayList<>();
          for (int i = 0; i < server.getShardCount(); i++) {
            for (int j = 0; j < server.getReplicationFactor(); j++) {
              try {
                if (common.getServersConfig().getShards()[i].getReplicas()[j].isDead()) {
                  continue;
                }
                if (i == server.getShard() && j == server.getReplica()) {
                  continue;
                }

                final int shard = i;
                final int replica = j;

                futures.add(executor.submit(new Callable(){
                  @Override
                  public Object call() throws Exception {
                    byte[] bytes = server.getClient().send(null, shard, replica, cobj2, DatabaseClient.Replica.specified);
                    ComObject retObj = new ComObject(bytes);
                    DatabaseCommon tmpCommon = new DatabaseCommon();
                    tmpCommon.deserializeSchema(retObj.getByteArray(ComObject.Tag.schemaBytes));
                    synchronized (common) {
                      if (tmpCommon.getSchemaVersion() > common.getSchemaVersion()) {
                        logger.info("Found schema with higher version: version=" + tmpCommon.getSchemaVersion() +
                            ", currVersion=" + common.getSchemaVersion() + ", shard=" + shard + ", replica=" + replica);
                        common.deserializeSchema(retObj.getByteArray(ComObject.Tag.schemaBytes));
                      }
                    }
                    return null;
                  }
                }));
              }
              catch (Exception e) {
                logger.error("Error getting schema: shard=" + i + ", replica=" + j, e);
              }
            }
          }
          for (Future future : futures) {
            future.get();
          }
          server.pushSchema();
        }
        catch (Exception e) {
          logger.error("Error pushing schema", e);
        }
        finally {
          executor.shutdownNow();
        }
      }
      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.schemaBytes, common.serializeSchema(serializationVersionNumber));
      return retObj;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public static AtomicInteger blockCount = new AtomicInteger();

  public static AtomicInteger echoCount = new AtomicInteger(0);
  public static AtomicInteger echo2Count = new AtomicInteger(0);

  public ComObject echo(ComObject cobj, boolean replayedCommand) {
    logger.info("called echo");
    if (cobj.getInt(ComObject.Tag.count) != null) {
      echoCount.set(cobj.getInt(ComObject.Tag.count));
    }
    return cobj;
  }

  public ComObject echoWrite(ComObject cobj, boolean replayedCommand) {
    //logger.info("called echo");
    echoCount.set(cobj.getInt(ComObject.Tag.count));
    return cobj;
  }

  public ComObject echo2(ComObject cobj, boolean replayedCommand) {
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

  public ComObject block(ComObject cobj, boolean replayedCommand) {
    logger.info("called block");
    blockCount.incrementAndGet();

    try {
      Thread.sleep(1000000);
    }
    catch (InterruptedException e) {
      throw new DatabaseException(e);
    }
    return cobj;
  }

  public ComObject reserveNextIdFromReplica(ComObject cobj, boolean replayedCommand) {
    return server.reserveNextIdFromReplica(cobj);
  }

  public byte[] noOp(ComObject cobj, boolean replayedCommand) {
    return null;
  }

  public ComObject allocateRecordIds(ComObject cobj, boolean replayedCommand) {
    return server.allocateRecordIds(cobj);
  }

  public ComObject pushMaxRecordId(ComObject cobj, boolean replayedCommand) {
    return server.pushMaxRecordId(cobj);
  }

  public ComObject setMaxRecordId(ComObject cobj, boolean replayedCommand) {
    return server.setMaxRecordId(cobj);
  }

  public ComObject sendLogsToPeer(ComObject cobj, boolean replayedCommand) {
    int replicaNum = cobj.getInt(ComObject.Tag.replica);

    return logManager.sendLogsToPeer(replicaNum);
  }

  public ComObject getLogFile(ComObject cobj, boolean replayedCommand) {
    return logManager.getLogFile(cobj);
  }

  public ComObject deletePeerLogs(ComObject cobj, boolean replayedCommand) {
    return logManager.deletePeerLogs(cobj);
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

  public ComObject deleteIndexEntryByKey(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return updateManager.deleteIndexEntryByKey(cobj, replayedCommand);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public ComObject commit(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return updateManager.commit(cobj, replayedCommand);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public ComObject rollback(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return updateManager.rollback(cobj, replayedCommand);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public ComObject insertWithSelect(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return updateManager.insertWithSelect(cobj, replayedCommand);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public ComObject insertIndexEntryByKey(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return updateManager.insertIndexEntryByKey(cobj, replayedCommand);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public ComObject insertIndexEntryByKeyWithRecord(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return updateManager.insertIndexEntryByKeyWithRecord(cobj, replayedCommand);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public ComObject batchInsertIndexEntryByKey(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return updateManager.batchInsertIndexEntryByKey(cobj, replayedCommand);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public ComObject batchInsertIndexEntryByKeyWithRecord(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return updateManager.batchInsertIndexEntryByKeyWithRecord(cobj, replayedCommand);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public ComObject updateIndexSchema(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaWriteLock(dbName).lock();
    try {
      return schemaManager.updateIndexSchema(cobj, replayedCommand);
    }
    finally {
      common.getSchemaWriteLock(dbName).unlock();
    }
  }

  public ComObject updateTableSchema(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaWriteLock(dbName).lock();
    try {
      return schemaManager.updateTableSchema(cobj, replayedCommand);
    }
    finally {
      common.getSchemaWriteLock(dbName).unlock();
    }
  }

  public ComObject getSchemaVersions(ComObject cobj, boolean replayedCommand) {
    return schemaManager.getSchemaVersions(cobj, replayedCommand);
  }

  public ComObject getTableSchema(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return schemaManager.getTableSchema(cobj, replayedCommand);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public ComObject abortTransaction(String command, byte[] body, boolean replayedCommand) {
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

  public ComObject updateRecord(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return updateManager.updateRecord(cobj, replayedCommand);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public ComObject deleteRecord(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return updateManager.deleteRecord(cobj, replayedCommand);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public ComObject deleteIndexEntry(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return updateManager.deleteIndexEntry(cobj, replayedCommand);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public ComObject saveSchema(ComObject cobj, boolean replayedCommand) {
//    byte[] bytes = cobj.getByteArray(ComObject.Tag.schemaBytes);
//    server.getCommon().saveSchema(bytes, server.getDataDir());

    return null;
  }

  public ComObject truncateTable(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return updateManager.truncateTable(cobj, replayedCommand);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public ComObject countRecords(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return readManager.countRecords(cobj);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public ComObject batchIndexLookup(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return readManager.batchIndexLookup(cobj);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public ComObject indexLookup(ComObject cobj, boolean replayedCommand) {
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


  public ComObject closeResultSet(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return readManager.closeResultSet(cobj, replayedCommand);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public ComObject serverSelectDelete(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return readManager.serverSelectDelete(cobj, replayedCommand);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public ComObject serverSetSelect(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return readManager.serverSetSelect(cobj);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public ComObject serverSelect(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return readManager.serverSelect(cobj);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public ComObject indexLookupExpression(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return readManager.indexLookupExpression(cobj);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public ComObject evaluateCounterGetKeys(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return readManager.evaluateCounterGetKeys(cobj);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public ComObject evaluateCounterWithRecord(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return readManager.evaluateCounterWithRecord(cobj);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public ComObject getIndexSchema(ComObject cobj, boolean replayedCommand) {
    return schemaManager.getIndexSchema(cobj, replayedCommand);
  }

  public ComObject getIndexCounts(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return server.getPartitionManager().getIndexCounts(cobj);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public ComObject testWrite(ComObject cobj, boolean replayedCommand) {
    logger.info("Called testWrite");
    testWriteCallCount.incrementAndGet();
    return null;
  }

  public ComObject deleteMovedRecords(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return server.getPartitionManager().deleteMovedRecords(cobj, replayedCommand);
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

  public ComObject isRepartitioningComplete(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return server.getPartitionManager().isRepartitioningComplete(cobj);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public ComObject beginRebalance(ComObject cobj, boolean replayedCommand) {

    //schema lock below
    return server.getPartitionManager().beginRebalance(cobj);
  }

  public ComObject getKeyAtOffset(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return server.getPartitionManager().getKeyAtOffset(cobj);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public ComObject getPartitionSize(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return server.getPartitionManager().getPartitionSize(cobj);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public ComObject stopRepartitioning(ComObject cobj, boolean replayedCommand) {
    return server.getPartitionManager().stopRepartitioning(cobj);
  }

  public ComObject doRebalanceOrderedIndex(ComObject cobj, boolean replayedCommand) {
    return server.getPartitionManager().doRebalanceOrderedIndex(cobj);
  }

  public ComObject rebalanceOrderedIndex(ComObject cobj, boolean replayedCommand) {
    //schema lock below
    if (replayedCommand) {
      return null;
    }
    return server.getPartitionManager().rebalanceOrderedIndex(cobj);
  }

  public ComObject moveIndexEntries(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return server.getPartitionManager().moveIndexEntries(cobj, replayedCommand);
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

  public ComObject doPopulateIndex(ComObject cobj, boolean replayedCommand) {
    return updateManager.doPopulateIndex(cobj);
  }

  public ComObject populateIndex(ComObject cobj, boolean replayedCommand) {

//    common.getSchemaReadLock(dbName).lock();
//    try {
    return updateManager.populateIndex(cobj, replayedCommand);
//    }
//    finally {
//      common.getSchemaReadLock(dbName).unlock();
//    }
  }

  public ComObject forceDeletes(ComObject cobj, boolean replayedCommand) {
    deleteManagerImpl.forceDeletes();
    return null;
  }

  public ComObject createIndex(ComObject cobj, boolean replayedCommand) {
    return schemaManager.createIndex(cobj, replayedCommand);
  }

  public ComObject expirePreparedStatement(ComObject cobj, boolean replayedCommand) {
    long preparedId = cobj.getLong(ComObject.Tag.preparedId);
    return null;
  }
}