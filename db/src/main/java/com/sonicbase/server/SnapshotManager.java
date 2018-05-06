/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.server;

import com.sonicbase.common.ComObject;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.List;

public interface SnapshotManager {
  void enableSnapshot(boolean enable);

  boolean isRecovering();

  ComObject finishServerReloadForSource(ComObject cobj, boolean replayedCommand);

  void getPercentRecoverComplete(ComObject retObj);

  Exception getErrorRecovering();

  long getBackupLocalFileSystemSize();

  void deleteTempDirs();

  void deleteDeletedDirs();

  void backupFileSystem(String directory, String subDirectory);

  void backupFileSystemSchema(String directory, String subDirectory);

  void backupAWS(String bucket, String prefix, String subDirectory);

  void backupAWSSchema(String bucket, String prefix, String subDirectory);

  void deleteSnapshots();

  void restoreFileSystem(String directory, String subDirectory);

  void restoreAWS(String bucket, String prefix, String subDirectory);

  void shutdown();

  void runSnapshotLoop();

  void runSnapshot(String dbName) throws IOException, InterruptedException, ParseException;

  void recoverFromSnapshot(String dbName) throws Exception;

  void getFilesForCurrentSnapshot(List<String> files);

  File getSortedDeltaFile(String dbName, String s, String key, String key1, int i);

  File getDeletedDeltaDir(String dbName, String s, String key, String key1);

  File getIndexSchemaDir(String dbName, String tableName, String indexName);

  File getTableSchemaDir(String dbName, String tableName);

  void saveIndexSchema(String dbName, int schemaVersion, TableSchema tableSchema, IndexSchema indexSchema);

  void saveTableSchema(String dbName, int schemaVersion, String tableName, TableSchema tableSchema);

  void deleteTableSchema(String dbName, int schemaVersion, String tableName);

  void deleteIndexSchema(String dbName, int schemaVersion, String table, String indexName);
}
