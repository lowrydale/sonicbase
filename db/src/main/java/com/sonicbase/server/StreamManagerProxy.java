package com.sonicbase.server;

import com.sonicbase.common.ComObject;
import com.sonicbase.query.DatabaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

@SuppressWarnings({"squid:S1172", "squid:S1168", "squid:S00107"})
// all methods called from method invoker must have cobj and replayed command parms
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
class StreamManagerProxy {

  private static final Logger logger = LoggerFactory.getLogger(StreamManagerProxy.class);

  private Object streamManager;
  private Method initPublisher;
  private Method initBatchInsert;
  private Method publishBatch;
  private Method batchInsertFinish;
  private Method publishInsertOrUpdate;
  private Method addToBatch;
  private Method startStreamsConsumerMasterMonitor;
  private Method stopStreamsConsumerMasterMonitor;

  StreamManagerProxy(Object proServer) {
    try {
      Class proClz = Class.forName("com.sonicbase.server.ProServer");
      Method method = proClz.getMethod("getStreamManager");
      streamManager = method.invoke(proServer);
      Class streamClz = Class.forName("com.sonicbase.server.StreamManager");
      initPublisher = streamClz.getMethod("initPublisher");
      initBatchInsert = streamClz.getMethod("initBatchInsert");
      publishBatch = streamClz.getMethod("publishBatch", ComObject.class);
      batchInsertFinish = streamClz.getMethod("batchInsertFinish");
      publishInsertOrUpdate = streamClz.getMethod("publishInsertOrUpdate", ComObject.class,
          String.class, String.class, byte[].class, byte[].class, UpdateManager.UpdateType.class);
      addToBatch = streamClz.getMethod("addToBatch",
          String.class, String.class, byte[].class, UpdateManager.UpdateType.class);
      startStreamsConsumerMasterMonitor = streamClz.getMethod("startStreamsConsumerMasterMonitor");
      stopStreamsConsumerMasterMonitor = streamClz.getMethod("stopStreamsConsumerMasterMonitor");
    }
    catch (Exception e) {
      logger.warn("Error initializing streamManager", e);
    }
  }

  void startStreamsConsumerMasterMonitor() {
    try {
      if (streamManager != null && startStreamsConsumerMasterMonitor != null) {
        startStreamsConsumerMasterMonitor.invoke(streamManager);
      }
    }
    catch (Exception e) {
      streamManager = null;
      throw new DatabaseException(e);
    }
  }

  void stopStreamsConsumerMasterMonitor() {
    try {
      if (streamManager != null && stopStreamsConsumerMasterMonitor != null) {
        stopStreamsConsumerMasterMonitor.invoke(streamManager);
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  void initPublisher() {
    try {
      if (streamManager != null && initPublisher != null) {
        initPublisher.invoke(streamManager);
      }
    }
    catch (Exception e) {
      streamManager = null;
      throw new DatabaseException(e);
    }
  }

  void initBatchInsert() {
    if (streamManager != null && initBatchInsert != null) {
      try {
        initBatchInsert.invoke(streamManager);
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }
  }

  void publishBatch(ComObject cobj) {
    if (streamManager != null && publishBatch != null) {
      try {
        publishBatch.invoke(streamManager, cobj);
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }

  }

  void batchInsertFinish() {
    if (streamManager != null && batchInsertFinish != null) {
      try {
        batchInsertFinish.invoke(streamManager);
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }
  }

  void publishInsertOrUpdate(ComObject cobj, String dbName, String tableName, byte[] bytes, byte[] existingBytes,
                             UpdateManager.UpdateType update) {
    if (streamManager != null && publishInsertOrUpdate != null) {
      try {
        publishInsertOrUpdate.invoke(streamManager, cobj, dbName, tableName, bytes, existingBytes, update);
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }
  }

  void addToBatch(String dbName, String tableName, byte[] recordBytes, UpdateManager.UpdateType insert) {
    if (streamManager != null && addToBatch != null
        ) {
      try {
        addToBatch.invoke(streamManager, dbName, tableName, recordBytes, insert);
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }

  }

}
