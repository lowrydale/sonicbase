package com.sonicbase.server;

import com.sonicbase.common.*;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.common.DeadServerException;
import org.apache.commons.lang.exception.ExceptionUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
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
  private final com.sonicbase.server.LogManager logManager;
  private final SchemaManager schemaManager;
  private final com.sonicbase.server.DatabaseServer server;
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
                       ReadManager readManager, com.sonicbase.server.LogManager logManager, SchemaManager schemaManager, MonitorManager monitorManager,
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
    priorityCommands.add("DatabaseServer.logError");
    priorityCommands.add("DatabaseServer:setMaxRecordId");
    priorityCommands.add("LogManager:setMaxSequenceNum");
    priorityCommands.add("LogManager:sendQueueFile");
    priorityCommands.add("LogManager:sendLogsToPeer");
    priorityCommands.add("DatabaseServer:pushMaxRecordId");
    priorityCommands.add("LogManager:pushMaxSequenceNum");
    priorityCommands.add("DatabaseServer:getSchema");
    priorityCommands.add("DatabaseServer:updateSchema");
    priorityCommands.add("DatabaseServer:getConfig");
    priorityCommands.add("DatabaseServer:getRecoverProgress");
    priorityCommands.add("DatabaseServer:healthCheckPriority");
    priorityCommands.add("DatabaseServer:getDbNames");
    priorityCommands.add("DatabaseServer:updateServersConfig");
    priorityCommands.add("BackupManager:prepareForRestore");
    priorityCommands.add("BackupManager:doRestoreAWS");
    priorityCommands.add("BackupManager:doRestoreFileSystem");
    priorityCommands.add("BackupManager:isRestoreComplete");
    priorityCommands.add("BackupManager:finishRestore");
    priorityCommands.add("BackupManager:prepareForBackup");
    priorityCommands.add("BackupManager:doBackupAWS");
    priorityCommands.add("BackupManager:doGetBackupSizes");
    priorityCommands.add("BackupManager:doGetRestoreSizes");
    priorityCommands.add("BackupManager:getBackupStatus");
    priorityCommands.add("BackupManager:doBackupFileSystem");
    priorityCommands.add("BackupManager:isBackupComplete");
    priorityCommands.add("BackupManager:finishBackup");
    priorityCommands.add("LogManager:sendLogsToPeer");
    priorityCommands.add("BackupManager:isEntireRestoreComplete");
    priorityCommands.add("BackupManager:isEntireBackupComplete");
    priorityCommands.add("BackupManager:isServerReloadFinished");
    priorityCommands.add("LicenseManager:licenseCheckin");
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
        if (!methodStr.equals("DatabaseServer:healthCheck") && !methodStr.equals("DatabaseServer:healthCheckPriority") &&
            !methodStr.equals("DatabaseServer:getConfig") &&
            !methodStr.equals("LicenseManager:licenseCheckin") &&
            !methodStr.equals("DatabaseServer:getSchema") && !methodStr.equals("DatabaseServer:getDbNames")) {
          throw new LicenseOutOfComplianceException("Licenses out of compliance");
        }
      }
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

      LogManager.LogRequest logRequest = logManager.logRequest(requestBytes, enableQueuing, methodStr,
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

          boolean readLock = false;
          boolean writeLock = false;
          String dbName = null;
          try {
            Object provider = this;
            Method method = null;
            String[] parts = methodStr.split(":");
            if (parts.length == 2) {
              Provider providerObj = providers.get(parts[0]);
              if (providerObj == null) {
                throw new DatabaseException("Inalid provider name: method=" + methodStr);
              }
              provider = providerObj.provider;
              method = providerObj.methodMap.get(parts[1]);
            }
            else {
              method = methodMap.get(methodStr);
            }
            if (method.isAnnotationPresent(SchemaReadLock.class)) {
              readLock = true;
              dbName = request.getString(ComObject.Tag.dbName);
              common.getSchemaReadLock(dbName).lock();
            }
            else if (method.isAnnotationPresent(SchemaWriteLock.class)) {
              writeLock = true;
              dbName = request.getString(ComObject.Tag.dbName);
              common.getSchemaWriteLock(dbName).lock();
            }
            ret = (ComObject) method.invoke(provider, request, replayedCommand);
            if (handlerTime != null) {
              handlerTime.addAndGet(System.nanoTime() - handleBegin);
            }
          }
          finally{
            if (readLock) {
              common.getSchemaReadLock(dbName).unlock();
            }
            else if (writeLock) {
              common.getSchemaWriteLock(dbName).unlock();
            }
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

  class Provider {
    private ConcurrentHashMap<String, Method> methodMap = new ConcurrentHashMap<>();
    private Object provider;
  }

  private ConcurrentHashMap<String, Provider> providers = new ConcurrentHashMap<>();

  public void registerMethodProvider(String providerName, Object provider) {
    Provider providerObj = new Provider();
    providerObj.provider = provider;

    Method[] methods = provider.getClass().getMethods();
    for (Method method : methods) {
      Class[] parms = method.getParameterTypes();
      if (parms.length == 2) {
        if (parms[0] == ComObject.class && parms[1] == boolean.class) {
          providerObj.methodMap.put(method.getName(), method);
        }
      }
    }
    providers.put(providerName, providerObj);
  }

  private Long getExistingSequence0(ComObject request) {
    return request.getLong(ComObject.Tag.sequence0);
  }

  private Long getExistingSequence1(ComObject request) {
    return request.getLong(ComObject.Tag.sequence1);
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

  public byte[] noOp(ComObject cobj, boolean replayedCommand) {
    return null;
  }

  public ComObject testWrite(ComObject cobj, boolean replayedCommand) {
    logger.info("Called testWrite");
    testWriteCallCount.incrementAndGet();
    return null;
  }

}