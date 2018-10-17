/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.stream;

import com.sonicbase.common.ComObject;
import com.sonicbase.server.DatabaseServer;
import com.sonicbase.server.UpdateManager;

public class StreamManager {

  private final StreamManagerImpl impl;

  public StreamManager(final DatabaseServer server) {
    impl = new StreamManagerImpl(server);
  }

  public StreamManagerImpl getImpl() {
    return impl;
  }

  public void initPublisher() {
    impl.initPublisher();
  }

  public void startStreamsConsumerMasterMonitor() {
    impl.startStreamsConsumerMasterMonitor();
  }

  public void stopStreamsConsumerMasterMonitor() {
    impl.stopStreamsConsumerMasterMonitor();
  }

  public void initBatchInsert() {
    impl.initBatchInsert();
  }

  public void publishBatch(ComObject cobj) {
    impl.publishBatch(cobj);
  }

  public void batchInsertFinish() {
    impl.batchInsertFinish();
  }

  public void publishInsertOrUpdate(ComObject cobj, String dbName, String tableName, byte[] recordBytes, byte[] existingBytes,
                                    UpdateManager.UpdateType updateType) {
    impl.publishInsertOrUpdate(cobj, dbName, tableName, recordBytes, existingBytes, updateType);
  }

  public void addToBatch(String dbName, String tableName, byte[] recordBytes, UpdateManager.UpdateType updateType) {
    impl.addToBatch(dbName, tableName, recordBytes, updateType);
  }

  public ComObject isStreamingStarted(ComObject cobj, boolean replayedCommand) {
    return impl.isStreamingStarted(cobj, replayedCommand);
  }

  public ComObject startStreaming(ComObject cobj, boolean replayedCommand) {
    return impl.startStreaming(cobj, replayedCommand);
  }

  public ComObject processMessages(ComObject cobj, boolean replayedCommand) {
    return impl.processMessages(cobj, replayedCommand);
  }

  public ComObject stopStreaming(ComObject cobj, boolean replayedCommand) {
    return impl.stopStreaming(cobj, replayedCommand);
  }

  public ComObject initConnection(ComObject cobj, boolean replayedCommand) {
    return impl.initConnection(cobj, replayedCommand);
  }

  public void shutdown() {
    impl.shutdown();
  }
}
