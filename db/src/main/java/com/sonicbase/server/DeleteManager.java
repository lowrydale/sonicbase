/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.server;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public interface DeleteManager {
  long getBackupLocalFileSystemSize();

  void delteTempDirs();

  void backupFileSystem(String directory, String subDirectory);

  void backupAWS(String bucket, String prefix, String subDirectory);

  void restoreFileSystem(String directory, String subDirectory);

  void restoreAWS(String bucket, String prefix, String subDirectory);

  void getFiles(List<String> files);

  void shutdown();

  boolean isForcingDeletes();

  double getPercentDeleteComplete();

  void saveDeletesForRecords(String dbName, String tableName, String indexName, long sequence0, long sequence1, ConcurrentLinkedQueue<DeleteManagerImpl.DeleteRequest> keysToDeleteExpanded);

  void saveDeletesForKeyRecords(String dbName, String tableName, String indexName, long sequence0, long sequence1, ConcurrentLinkedQueue<DeleteManagerImpl.DeleteRequest> keysToDeleteExpanded);

  void saveDeleteForRecord(String dbName, String tableName, String key, long sequence0, long sequence1, DeleteManagerImpl.DeleteRequestForRecord request);

  void saveDeleteForKeyRecord(String dbName, String tableName, String key, long sequence0, long sequence1, DeleteManagerImpl.DeleteRequestForKeyRecord request);

  void deleteOldLogs(long lastSnapshot);

  void buildDeletionsFiles(String dbName, AtomicReference<String> currStage, AtomicLong totalBytes, AtomicLong finishedBytes);

  void applyDeletesToSnapshot(String dbName, int currDeltaDirNum, AtomicLong finishedBytes);

  void forceDeletes();

  void start();
}
