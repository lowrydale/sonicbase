package com.sonicbase.server;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.*;
import com.sonicbase.index.Repartitioner;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.socket.DeadServerException;
import com.sonicbase.util.StreamUtils;
import org.apache.commons.lang.exception.ExceptionUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by lowryda on 7/28/17.
 */
public class CommandHandler {
  private Logger logger;

  private final DeleteManager deleteManager;
  private final SnapshotManager snapshotManager;
  private final UpdateManager updateManager;
  private final TransactionManager transactionManager;
  private final ReadManager readManager;
  private final LogManager logManager;
  private final SchemaManager schemaManager;
  private final DatabaseServer server;
  private final DatabaseCommon common;
  private Repartitioner repartitioner;
  private boolean shutdown;
  private AtomicInteger testWriteCallCount = new AtomicInteger();


  public CommandHandler(DatabaseServer server, DeleteManager deleteManager, SnapshotManager snapshotManager, UpdateManager updateManager, TransactionManager transactionManager, ReadManager readManager, LogManager logManager, SchemaManager schemaManager) {
    this.server = server;
    this.common = server.getCommon();
    this.deleteManager = deleteManager;
    this.snapshotManager = snapshotManager;
    this.updateManager = updateManager;
    this.transactionManager = transactionManager;
    this.readManager = readManager;
    this.logManager = logManager;
    this.schemaManager = schemaManager;

    logger = new Logger(server.getDatabaseClient());
  }

  public void shutdown() {
    this.shutdown = true;
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
    priorityCommands.add("doBackupFileSystem");
    priorityCommands.add("isBackupComplete");
    priorityCommands.add("finishBackup");
    priorityCommands.add("sendLogsToPeer");
    priorityCommands.add("isEntireRestoreComplete");
    priorityCommands.add("isEntireBackupComplete");
    priorityCommands.add("isServerReloadFinished");
    //priorityCommands.add("doPopulateIndex");
  }

  public byte[] handleCommand(final String command, final byte[] body, boolean replayedCommand, boolean enableQueuing) {
    return handleCommand(command, body, -1L, -1L, replayedCommand, enableQueuing);
  }

  public byte[] handleCommand(final String command, final byte[] body, long logSequence0, long logSequence1,
                              boolean replayedCommand, boolean enableQueuing) {
    try {
      if (shutdown) {
        throw new DatabaseException("Shutdown in progress");
      }
      if (command == null) {
        logger.error("null command");
        throw new DatabaseException();
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

      if (server.isApplyingQueuesAndInteractive()) {
        replayedCommand = true;
      }

      ComObject cobj = null;
      if (methodStr.equals("ComObject")) {
        cobj = new ComObject(body);
        methodStr = cobj.getString(ComObject.Tag.method);
      }

      if (server.shouldDisableNow() && server.isUsingMultipleReplicas()) {
        if (!methodStr.equals("healthCheckPriority") && !methodStr.equals("getConfig") && !methodStr.equals("getSchema")) {
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
//      String queueCommand = "DatabaseServer:queueForOtherServer:1:" + SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION + ":1:__none__:" + i;
//      replicas[common.getServersConfig().getShards()[shard].getMasterReplica()].do_send(
//          null, queueCommand, bytesOut.toByteArray())
//
      if (methodStr.equals("queueForOtherServer")) {
        try {
          String[] parts = command.split(":");
          int replica = Integer.valueOf(parts[6]);
          cobj = new ComObject(body);
          logManager.logRequestForPeer(cobj.getString(ComObject.Tag.command), body, System.currentTimeMillis(), logManager.getNextSequencenNum(), replica);
          return null;
        }
        catch (Exception e) {
          throw new DatabaseException(e);
        }
      }

      Long existingSequence0 = getExistingSequence0(command);
      Long existingSequence1 = getExistingSequence1(command);

      DatabaseServer.LogRequest logRequest = logManager.logRequest(command, body, enableQueuing, methodStr, existingSequence0, existingSequence1);

      ComObject ret = null;

      if (!replayedCommand && !server.isRunning() && !priorityCommands.contains(methodStr)) {
        throw new DeadServerException("Server not running: command=" + command);
      }

      List<ReplicaFuture> futures = new ArrayList<>();
      long sequence0 = logRequest == null ? logSequence0 : logRequest.getSequences0()[0];
      long sequence1 = logRequest == null ? logSequence1 : logRequest.getSequences1()[0];
      final String newCommand0 = logRequest == null ? command : command + ":xx_sn0_xx=" + sequence0;
      final String newCommand = logRequest == null ? newCommand0 : newCommand0 + ":xx_sn1_xx=" + sequence1;
      if (!server.onlyQueueCommands() || !enableQueuing) {
        DatabaseServer.Shard currShard = common.getServersConfig().getShards()[server.getShard()];
        int replPos = command.indexOf("xx_repl_xx");
        try {
          if (cobj != null) {
            cobj.put(ComObject.Tag.sequence0, sequence0);
            cobj.put(ComObject.Tag.sequence1, sequence1);
            methodStr = cobj.getString(ComObject.Tag.method);
            Method method = getClass().getMethod(methodStr, ComObject.class, boolean.class);
            ret = (ComObject) method.invoke(this, cobj, replayedCommand);
          }
          else {
            Method method = getClass().getMethod(methodStr, String.class, byte[].class, boolean.class);
            ret = (ComObject) method.invoke(this, command, body, replayedCommand);
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
        catch (Exception e) {
//          boolean schemaOutOfSync = false;
//          int index = ExceptionUtils.indexOfThrowable(e, SchemaOutOfSyncException.class);
//          if (-1 != index) {
//            schemaOutOfSync = true;
//          }
//          else if (e.getMessage() != null && e.getMessage().contains("SchemaOutOfSyncException")) {
//            schemaOutOfSync = true;
//          }
//
//          if (!schemaOutOfSync) {
//            logger.error("Error processing request", e);
//          }
          throw new DatabaseException(e);
        }

        for (ReplicaFuture future : futures) {
          try {
            future.future.get();
          }
          catch (Exception e) {
            int index = ExceptionUtils.indexOfThrowable(e, DeadServerException.class);
            if (-1 != index) {
              logManager.logRequestForPeer(newCommand, body, sequence0, sequence1, future.replica);
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
    catch (DeadServerException e) {
      throw e; //don't log
    }
    catch (Exception e) {
      if (-1 != ExceptionUtils.indexOfThrowable(e, SchemaOutOfSyncException.class)) {
        throw new DatabaseException(e); //don't log
      }
      logger.error("Error handling command: command=" + command, e);
      throw new DatabaseException(e);
    }
  }

  private Long getExistingSequence0(String command) {
    Long existingSequenceNumber = null;
    int snPos = command.indexOf("xx_sn0_xx=");
    if (snPos != -1) {
      String sn = null;
      try {
        int endPos = command.indexOf(":", snPos);
        if (endPos == -1) {
          sn = command.substring(snPos + "xx_sn0_xx=".length());
        }
        else {
          sn = command.substring(snPos + "xx_sn0_xx=".length(), endPos);
        }
        existingSequenceNumber = Long.valueOf(sn);
      }
      catch (Exception e) {
        logger.error("Error getting sequenceNum: command=" + command + ", error=" + e.getMessage());
      }
    }
    return existingSequenceNumber;
  }

  private Long getExistingSequence1(String command) {
    Long existingSequenceNumber = null;
    int snPos = command.indexOf("xx_sn1_xx=");
    if (snPos != -1) {
      try {
        String sn = null;
        int endPos = command.indexOf(":", snPos);
        if (endPos == -1) {
          sn = command.substring(snPos + "xx_sn1_xx=".length());
        }
        else {
          sn = command.substring(snPos + "xx_sn1_xx=".length(), endPos);
        }
        existingSequenceNumber = Long.valueOf(sn);
      }
      catch (Exception e) {
        logger.error("Error getting sequenceNum: command=" + command + ", error=" + e.getMessage());
      }
    }
    return existingSequenceNumber;
  }

  public ComObject licenseCheckin(ComObject cobj, boolean replayedCommand) {
    return server.licenseCheckin(cobj);
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

  public ComObject addColumn(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaWriteLock(dbName).lock();
    try {
      return schemaManager.addColumn(cobj);
    }
    finally {
      common.getSchemaWriteLock(dbName).unlock();
    }
  }

  public ComObject dropColumn(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaWriteLock(dbName).lock();
    try {
      return schemaManager.dropColumn(cobj);
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
      return schemaManager.dropIndex(cobj);
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


  public void setRepartitioner(Repartitioner repartitioner) {
    this.repartitioner = repartitioner;
  }

  public ComObject promoteEntireReplicaToMaster(ComObject cobj, boolean replayedCommand) {
    return server.promoteEntireReplicaToMaster(cobj);
  }

  public ComObject electNewMaster(ComObject cobj, boolean replayedCommand) throws InterruptedException, IOException {
    return server.electNewMaster(cobj);
  }

  public ComObject promoteToMaster(ComObject cobj, boolean replayedCommand) {
    return server.promoteToMaster(cobj);
  }

  public ComObject markReplicaDead(ComObject cobj, boolean replayedCommand) {
    int replicaToKill = cobj.getInt(ComObject.Tag.replica);
    logger.info("Marking replica dead: replica=" + replicaToKill);
    for (int shard = 0; shard < server.getShardCount(); shard++) {
      common.getServersConfig().getShards()[shard].getReplicas()[replicaToKill].setDead(true);
    }
    common.saveSchema(server.getDataDir());
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
    common.saveSchema(server.getDataDir());
    server.pushSchema();

    server.setReplicaDeadForRestart(-1);
    return null;
  }

  public ComObject promoteToMasterAndPushSchema(ComObject cobj, boolean replayedCommand) {
    int shard = cobj.getInt(ComObject.Tag.shard);
    int replica = cobj.getInt(ComObject.Tag.replica);

    logger.info("promoting to master: shard=" + shard + ", replica=" + replica);
    common.getServersConfig().getShards()[shard].setMasterReplica(replica);
    common.saveSchema(server.getDataDir());
    server.pushSchema();
    return null;
  }

  public ComObject isShardRepartitioningComplete(ComObject cobj, boolean replayedCommand) {
    return repartitioner.isShardRepartitioningComplete(cobj, replayedCommand);
  }

  public ComObject prepareForBackup(ComObject cobj, boolean replayedCommand) {
    return server.prepareForBackup(cobj);
  }

  public ComObject doBackupFileSystem(final ComObject cobj, boolean replayedCommand) {
    return server.doBackupFileSystem(cobj);
  }

  public ComObject doBackupAWS(final ComObject cobj, boolean replayedCommand) {
    return server.doBackupAWS(cobj);
  }

  public ComObject isBackupComplete(ComObject cobj, boolean replayedCommand) {
    return server.isBackupComplete(cobj);
  }

  public ComObject finishBackup(ComObject cobj, boolean replayedCommand) {
    return server.finishBackup(cobj);
  }

  public ComObject isEntireBackupComplete(ComObject cobj, boolean replayedCommand) {
    return server.isEntireBackupComplete(cobj);
  }

  public byte[] startBackup(ComObject cobj, boolean replayedCommand) {
    return server.startBackup(cobj);
  }

  public ComObject getLastBackupDir(ComObject cobj, boolean replayedCommand) {
    return server.getLastBackupDir(cobj);
  }

  public ComObject prepareForRestore(ComObject cobj, boolean replayedCommand) {
    return server.prepareForRestore(cobj);
  }

  public ComObject doRestoreFileSystem(final ComObject cobj, boolean replayedCommand) {
    return server.doRestoreFileSystem(cobj);
  }

  public ComObject doRestoreAWS(final ComObject cobj, boolean replayedCommand) {
    return server.doRestoreAWS(cobj);
  }

  public ComObject isRestoreComplete(ComObject cobj, boolean replayedCommand) {
    return server.isRestoreComplete(cobj);
  }

  public ComObject finishRestore(ComObject cobj, boolean replayedCommand) {
    return server.finishRestore(cobj);
  }

  public ComObject isEntireRestoreComplete(ComObject cobj, boolean replayedCommand) {
    return server.isEntireRestoreComplete(cobj);
  }

  public ComObject startRestore(final ComObject cobj, boolean replayedCommand) {
    return server.startRestore(cobj);
  }

  public ComObject getFile(ComObject cobj, boolean replayedCommand) {
    try {
      String filename = cobj.getString(ComObject.Tag.filename);
      File file = new File(server.getInstallDir(), filename);
      if (!file.exists()) {
        return null;
      }
      try (FileInputStream fileIn = new FileInputStream(file)) {
        String ret = StreamUtils.inputStreamToString(fileIn);
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
    return server.getOSStats(cobj);
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
    DatabaseCommon tempCommon = new DatabaseCommon();
    tempCommon.deserializeSchema(cobj.getByteArray(ComObject.Tag.schemaBytes));

//    if (getShard() == 0 &&
//        tempCommon.getServersConfig().getShards()[0].getMasterReplica() == getReplica()) {
//      return null;
//    }

    synchronized (common) {
      if (tempCommon.getSchemaVersion() > common.getSchemaVersion()) {
        common.deserializeSchema(cobj.getByteArray(ComObject.Tag.schemaBytes));
        common.saveSchema(server.getDataDir());
      }
    }
    return null;
  }

  public ComObject prepareSourceForServerReload(ComObject cobj, boolean replayedCommand) {
    return server.prepareSourceForServerReload(cobj);
  }

  public ComObject finishServerReloadForSource(ComObject cobj, boolean replayedCommand) {

    snapshotManager.enableSnapshot(true);

    return null;
  }

  public ComObject isServerReloadFinished(ComObject cobj, boolean replayedCommand) {
    return server.isServerReloadFinished(cobj);
  }

  public ComObject reloadServer(ComObject cobj, boolean replayedCommand) {
    return server.reloadServer(cobj);
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

    ComObject retObj = new ComObject();
    if (snapshotManager.isRecovering()) {
      retObj.put(ComObject.Tag.percentComplete, snapshotManager.getPercentRecoverComplete());
      retObj.put(ComObject.Tag.stage, "recoveringSnapshot");
    }
    else {
      retObj.put(ComObject.Tag.percentComplete, logManager.getPercentApplyQueuesComplete());
      retObj.put(ComObject.Tag.stage, "applyingLogs");
    }
    Exception error = snapshotManager.getErrorRecovering();
    if (error != null) {
      retObj.put(ComObject.Tag.error, true);
    }
    return retObj;
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
    long serializationVersionNumber = cobj.getLong(ComObject.Tag.serializationVersion);
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
    long serializationVersionNumber = cobj.getLong(ComObject.Tag.serializationVersion);
    try {
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

  public ComObject evaluateCounter(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return readManager.evaluateCounter(cobj);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public ComObject getIndexCounts(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return repartitioner.getIndexCounts(cobj);
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

  public ComObject isRepartitioningComplete(ComObject cobj, boolean replayedCommand) {
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

  public ComObject notifyRepartitioningComplete(ComObject cobj, boolean replayedCommand) {
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

  public ComObject beginRebalance(ComObject cobj, boolean replayedCommand) {

    //schema lock below
    return repartitioner.beginRebalance(cobj);
  }

  public ComObject getKeyAtOffset(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return repartitioner.getKeyAtOffset(cobj);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public ComObject getPartitionSize(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    common.getSchemaReadLock(dbName).lock();
    try {
      return repartitioner.getPartitionSize(cobj);
    }
    finally {
      common.getSchemaReadLock(dbName).unlock();
    }
  }

  public ComObject stopRepartitioning(ComObject cobj, boolean replayedCommand) {
    return repartitioner.stopRepartitioning(cobj);
  }

  public ComObject doRebalanceOrderedIndex(ComObject cobj, boolean replayedCommand) {
    return repartitioner.doRebalanceOrderedIndex(cobj);
  }

  public ComObject rebalanceOrderedIndex(ComObject cobj, boolean replayedCommand) {
    //schema lock below
    return repartitioner.rebalanceOrderedIndex(cobj);
  }

  public ComObject moveIndexEntries(ComObject cobj, boolean replayedCommand) {
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

  public ComObject doPopulateIndex(ComObject cobj, boolean replayedCommand) {
    return updateManager.doPopulateIndex(cobj);
  }

  public ComObject populateIndex(ComObject cobj, boolean replayedCommand) {

//    common.getSchemaReadLock(dbName).lock();
//    try {
    return updateManager.populateIndex(cobj);
//    }
//    finally {
//      common.getSchemaReadLock(dbName).unlock();
//    }
  }

  public ComObject forceDeletes(ComObject cobj, boolean replayedCommand) {
    deleteManager.forceDeletes();
    return null;
  }

  public ComObject createIndex(ComObject cobj, boolean replayedCommand) {
    return schemaManager.createIndex(cobj, replayedCommand);
  }

  public ComObject expirePreparedStatement(ComObject cobj, boolean replayedCommand) {
    long preparedId = cobj.getLong(ComObject.Tag.preparedId);
    readManager.expirePreparedStatement(preparedId);
    return null;
  }





}