package com.sonicbase.server;

import com.sonicbase.common.*;
import com.sonicbase.query.DatabaseException;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@SuppressWarnings({"squid:S1172", "squid:S1168", "squid:S00107"})
// all methods called from method invoker must have cobj and replayed command parms
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class MethodInvoker {
  private static final Logger logger = LoggerFactory.getLogger(MethodInvoker.class);

  private final com.sonicbase.server.LogManager logManager;
  private final com.sonicbase.server.DatabaseServer server;
  private final DatabaseCommon common;
  private boolean shutdown;
  private final AtomicInteger testWriteCallCount = new AtomicInteger();
  private final ConcurrentHashMap<String, Method> methodMap = new ConcurrentHashMap<>();
  public static final AtomicInteger blockCount = new AtomicInteger();
  public static final AtomicInteger echoCount = new AtomicInteger(0);
  public static final AtomicInteger echo2Count = new AtomicInteger(0);

  public MethodInvoker(DatabaseServer server, LogManager logManager) {
    this.server = server;
    this.common = server.getCommon();
    this.logManager = logManager;

    Method[] methods = MethodInvoker.class.getMethods();
    for (Method method : methods) {
      Class[] parms = method.getParameterTypes();
      if (parms.length == 2 && parms[0] == ComObject.class && parms[1] == boolean.class) {
        methodMap.put(method.getName(), method);
      }
    }
  }

  public int getEchoCount() {
    return echoCount.get();
  }

  public void shutdown() {
    this.shutdown = true;
  }

  private static final Set<String> priorityCommands = new HashSet<>();

  static {



    priorityCommands.add("DatabaseServer:reloadServer");
    priorityCommands.add("DatabaseServer:finishServerReloadForSource");
    priorityCommands.add("DatabaseServer:isServerReloadFinished");
    priorityCommands.add("DatabaseServer:enableServer");
    priorityCommands.add("DatabaseServer:disableServer");
    priorityCommands.add("DatabaseServer:logError");
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
  }

  public byte[] invokeMethod(final byte[] requestBytes, long logSequence0, long logSequence1,
                             boolean replayedCommand, boolean enableQueuing, AtomicLong timeLogging, AtomicLong handlerTime) {
    try {
      if (shutdown) {
        throw new DatabaseException("Shutdown in progress");
      }
      ComObject request = new ComObject(requestBytes);
      String methodStr = request.getString(ComObject.Tag.METHOD);

      if (server.isApplyingQueuesAndInteractive()) {
        replayedCommand = true;
      }

      if (methodStr.equals("queueForOtherServer")) {
        return queueForOtherServer(requestBytes, request);
      }

      Long existingSequence0 = getExistingSequence0(request);
      Long existingSequence1 = getExistingSequence1(request);

      LogManager.LogRequest logRequest = logManager.logRequest(requestBytes, enableQueuing, methodStr,
          existingSequence0, existingSequence1, timeLogging);
      ComObject ret = null;

      if (!replayedCommand && (!server.isRecovered() || !server.isRunning())
            && !priorityCommands.contains(methodStr)) {
        throw new DeadServerException("Server not running: method=" + methodStr);
      }

      long sequence0 = logRequest == null ? logSequence0 : logRequest.getSequences0()[0];
      long sequence1 = logRequest == null ? logSequence1 : logRequest.getSequences1()[0];

      ComObject newMessage = new ComObject(requestBytes);
      if (logRequest != null) {
        newMessage.put(ComObject.Tag.SEQUENCE_0, sequence0);
        newMessage.put(ComObject.Tag.SEQUENCE_1, sequence1);
      }
      if (!server.onlyQueueCommands() || !enableQueuing) {
        ret = doInvokeMethod(replayedCommand, handlerTime, request, methodStr, existingSequence0, sequence0, sequence1);
      }
      if (logRequest != null) {
        logRequest.getLatch().await();
      }
      if (ret == null) {
        return null;
      }

      return ret.serialize();
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new DatabaseException(e);
    }
    catch (DeadServerException | SchemaOutOfSyncException e) {
      throw e; //don't log
    }
    catch (Exception e) {
      handleGenericException(requestBytes, e);
    }
    return null;
  }

  private void handleGenericException(byte[] requestBytes, Exception e) {

    if (-1 != ExceptionUtils.indexOfThrowable(e, SchemaOutOfSyncException.class)) {
      throw new DatabaseException(e); //don't log
    }
    if (e.getCause() instanceof SchemaOutOfSyncException) {
      throw new DatabaseException(e);
    }
    if (-1 != ExceptionUtils.indexOfThrowable(e, UniqueConstraintViolationException.class)) {
      throw new DatabaseException(e); //don't log
    }
    if (e.getMessage() != null && e.getMessage().contains("Shutdown in progress")) {
      throw new DatabaseException(e);
    }
    logger.error("Error handling command: method=" + new ComObject(requestBytes).getString(ComObject.Tag.METHOD), e);
    throw new DatabaseException(e);
  }

  private ComObject doInvokeMethod(boolean replayedCommand, AtomicLong handlerTime, ComObject request, String methodStr,
                                   Long existingSequence0, long sequence0, long sequence1) {
    ComObject ret;
    try {
      long handleBegin = System.nanoTime();
      request.put(ComObject.Tag.SEQUENCE_0, sequence0);
      request.put(ComObject.Tag.SEQUENCE_1, sequence1);
      if (existingSequence0 == null) {
        request.put(ComObject.Tag.CURR_REQUEST_IS_MASTER, true);
      }
      else {
        request.put(ComObject.Tag.CURR_REQUEST_IS_MASTER, false);
      }

      ret = doInvokeMethod(replayedCommand, handlerTime, request, methodStr, handleBegin);

      if (ret != null) {
        ret.put(ComObject.Tag.SEQUENCE_0, sequence0);
        ret.put(ComObject.Tag.SEQUENCE_1, sequence1);
      }
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
    return ret;
  }

  private ComObject doInvokeMethod(boolean replayedCommand, AtomicLong handlerTime, ComObject request, String methodStr,
                                   long handleBegin) throws IllegalAccessException, InvocationTargetException {
    boolean readLock = false;
    boolean writeLock = false;
    String dbName = null;

    ComObject ret;
    try {
      Object provider = this;
      Method method = null;
      String[] parts = methodStr.split(":");
      if (parts.length == 2) {
        MethodProvider providerObj = providers.get(parts[0]);
        if (providerObj == null) {
          throw new DatabaseException("Invalid provider name: method=" + methodStr);
        }
        if (providerObj instanceof NoOpMethodProvider) {
          return null;
        }
        provider = providerObj.provider;
        method = providerObj.methodMap.get(parts[1]);
      }
      else {
        method = methodMap.get(methodStr);
      }
      if (method.isAnnotationPresent(SchemaReadLock.class)) {
        readLock = true;
        dbName = request.getString(ComObject.Tag.DB_NAME);
        common.getSchemaReadLock(dbName).lock();
      }
      else if (method.isAnnotationPresent(SchemaWriteLock.class)) {
        writeLock = true;
        dbName = request.getString(ComObject.Tag.DB_NAME);
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
    return ret;
  }

  private byte[] queueForOtherServer(byte[] requestBytes, ComObject request) {
    try {
      ComObject header = request.getObject(ComObject.Tag.HEADER);
      Integer replica = header.getInt(ComObject.Tag.REPLICA);
      String innerMethod = header.getString(ComObject.Tag.METHOD);
      request.put(ComObject.Tag.METHOD, innerMethod);
      logManager.logRequestForPeer(requestBytes, innerMethod, System.currentTimeMillis(), logManager.getNextSequencenNum(), replica);
      return null;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public void registerNoOpMethodProvider(String providerName) {
    providers.put(providerName, new NoOpMethodProvider());
  }


  class MethodProvider {
    private final ConcurrentHashMap<String, Method> methodMap = new ConcurrentHashMap<>();
    private Object provider;
  }

  class NoOpMethodProvider extends MethodProvider {
  }

  private final ConcurrentHashMap<String, MethodProvider> providers = new ConcurrentHashMap<>();

  public void registerMethodProvider(String providerName, Object provider) {
    MethodProvider providerObj = new MethodProvider();
    providerObj.provider = provider;

    Method[] methods = provider.getClass().getMethods();
    for (Method method : methods) {
      Class[] parms = method.getParameterTypes();
      if (parms.length == 2 && parms[0] == ComObject.class && parms[1] == boolean.class) {
        providerObj.methodMap.put(method.getName(), method);
      }
    }
    providers.put(providerName, providerObj);
  }

  private Long getExistingSequence0(ComObject request) {
    return request.getLong(ComObject.Tag.SEQUENCE_0);
  }

  private Long getExistingSequence1(ComObject request) {
    return request.getLong(ComObject.Tag.SEQUENCE_1);
  }

  public ComObject echo(ComObject cobj, boolean replayedCommand) {
    logger.info("called echo");
    if (cobj.getInt(ComObject.Tag.COUNT) != null) {
      echoCount.set(cobj.getInt(ComObject.Tag.COUNT));
    }
    return cobj;
  }

  public ComObject echoWrite(ComObject cobj, boolean replayedCommand) {
    echoCount.set(cobj.getInt(ComObject.Tag.COUNT));
    return cobj;
  }

  public ComObject echo2(ComObject cobj, boolean replayedCommand) {
    logger.info("called echo2");
    throw new DatabaseException("not supported");
  }

  public ComObject block(ComObject cobj, boolean replayedCommand) {
    logger.info("called block");
    blockCount.incrementAndGet();

    try {
      Thread.sleep(1000000);
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
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