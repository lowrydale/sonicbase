package com.sonicbase.server;

import com.sonicbase.aws.AWSClient;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.ComObject;
import com.sonicbase.common.InsufficientLicense;
import com.sonicbase.common.ThreadUtil;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.util.DateUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.sonicbase.server.DatabaseServer.USE_SNAPSHOT_MGR_OLD;
import static com.sonicbase.server.SnapshotManager.SNAPSHOT_STR;
import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

public class BackupManagerImpl {

  private static Logger logger = LoggerFactory.getLogger(BackupManagerImpl.class);

  private final com.sonicbase.server.DatabaseServer server;
  private final SnapshotManager snapshotManager;
  private final DeleteManager deleteManager;
  private final DeleteHandler deleteHandler;
  private final LongRunningCalls longRunningCalls;
  private final LongRunningCallsHandler longRunningCallsHandler;
  private final SnapshotHandler snapshotManagerHandler;
  private final LogManager logManager;
  private final LogManagerHandler logManagerHandler;
  private final ProServer proServer;
  private String logSlicePoint;
  private boolean isBackupComplete;
  private boolean isRestoreComplete;
  private Exception backupException;
  private Exception restoreException;
  private Map<String, Object> backupConfig;
  private boolean finishedRestoreFileCopy;
  private Thread backupFileSystemThread;
  private Thread backupAWSThread;
  private Thread backupMainThread;
  private Thread restoreFileSystemThread;
  private Thread restoreAWSThread;
  private Thread restoreMainThread;
  private Thread reloadServerThread;

  private boolean doingBackup;
  private int cronIdentity = 0;
  private boolean doingRestore;
  private static Object restoreAwsMutex = new Object();
  private String lastBackupDir;
  private String backupFileSystemDir;
  private boolean startedPrepareData;


  public BackupManagerImpl(ProServer proServer, com.sonicbase.server.DatabaseServer server) {
    this.server = server;
    this.proServer = proServer;
    this.snapshotManager = server.getSnapshotManager();
    this.snapshotManagerHandler = new SnapshotHandler();
    this.deleteManager = server.getDeleteManager();
    this.deleteHandler = new DeleteHandler();
    this.longRunningCalls = server.getLongRunningCommands();
    this.longRunningCallsHandler = new LongRunningCallsHandler();
    this.logManager = server.getLogManager();
    this.logManagerHandler = new LogManagerHandler();
  }

  public void shutdown() {
    try {
      if (backupFileSystemThread != null) {
        backupFileSystemThread.interrupt();
        backupFileSystemThread.join();
      }
      if (backupAWSThread != null) {
        backupAWSThread.interrupt();
        backupAWSThread.join();
      }

      if (restoreMainThread != null) {
        restoreMainThread.interrupt();
        restoreMainThread.join();
      }
      if (restoreAWSThread != null) {
        restoreAWSThread.interrupt();
        restoreAWSThread.join();
      }

      if (restoreFileSystemThread != null) {
        restoreFileSystemThread.interrupt();
        restoreFileSystemThread.join();
      }

      if (reloadServerThread != null) {
        reloadServerThread.interrupt();
        reloadServerThread.join();
      }

    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  class SnapshotHandler {

    public long getBackupLocalFileSystemSize() {
      File dir = snapshotManager.getSnapshotReplicaDir();
      if (dir.exists()) {
        return com.sonicbase.common.FileUtils.sizeOfDirectory(dir);
      }
      return 0;
    }

    public void deleteSnapshots() {
      File dir = snapshotManager.getSnapshotReplicaDir();
      try {
        FileUtils.deleteDirectory(dir);
        dir.mkdirs();
      }
      catch (IOException e) {
        throw new DatabaseException(e);
      }
    }

    public void deleteTempDirs() {
      try {
        File dir = snapshotManager.getSnapshotReplicaDir();
        doDeleteTempDirs(dir);
      }
      catch (Exception e) {

      }
    }

    public void deleteDeletedDirs() {

    }

    private void doDeleteTempDirs(File dir) throws IOException {
      File[] files = dir.listFiles();
      if (files != null) {
        for (File file : files) {
          if (file.isDirectory()) {
            if (file.getName().contains("tmp")) {
              FileUtils.deleteDirectory(file);
            }
            else {
              doDeleteTempDirs(file);
            }
          }
        }
      }
    }

    public void backupFileSystemSchema(String directory, String subDirectory) {
      try {
        File file = new File(directory, subDirectory);
        file = new File(file, SNAPSHOT_STR + server.getShard() + "/0/schema.bin");
        file.getParentFile().mkdirs();

        File sourceFile = new File(snapshotManager.getSnapshotReplicaDir(), "schema.bin");
        FileUtils.copyFile(sourceFile, file);


        file = new File(directory, subDirectory);
        file = new File(file, SNAPSHOT_STR + server.getShard() + "/0/config.bin");
        file.getParentFile().mkdirs();

        sourceFile = new File(snapshotManager.getSnapshotReplicaDir(), "config.bin");
        if (sourceFile.exists()) {
          FileUtils.copyFile(sourceFile, file);
        }
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }

    public void backupFileSystem(String directory, String subDirectory) {
      try {
        File file = new File(directory, subDirectory);
        file = new File(file, SNAPSHOT_STR + server.getShard() + "/0");
        FileUtils.deleteDirectory(file);
        file.mkdirs();

        if (snapshotManager.getSnapshotReplicaDir().exists()) {
          FileUtils.copyDirectory(snapshotManager.getSnapshotReplicaDir(), file);
        }
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }

    public void restoreFileSystem(String directory, String subDirectory) {
      try {
        File file = new File(directory, subDirectory);
        file = new File(file, SNAPSHOT_STR + server.getShard() + "/0");

        if (snapshotManager.getSnapshotReplicaDir().exists()) {
          FileUtils.deleteDirectory(snapshotManager.getSnapshotReplicaDir());
        }
        snapshotManager.getSnapshotReplicaDir().mkdirs();

        if (file.exists()) {
          FileUtils.copyDirectory(file, snapshotManager.getSnapshotReplicaDir());
        }
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }

    public void backupAWSSchema(String bucket, String prefix, String subDirectory) {
      AWSClient awsClient = proServer.getAwsClient();
      File srcFile = new File(snapshotManager.getSnapshotReplicaDir(), "schema.bin");
      subDirectory += "/" + SNAPSHOT_STR + server.getShard() + "/0";

      awsClient.uploadFile(bucket, prefix, subDirectory, srcFile);

      srcFile = new File(snapshotManager.getSnapshotReplicaDir(), "config.bin");

      if (srcFile.exists()) {
        awsClient.uploadFile(bucket, prefix, subDirectory, srcFile);
      }
    }

    public void backupAWS(String bucket, String prefix, String subDirectory) {
      AWSClient awsClient = proServer.getAwsClient();
      File srcDir = snapshotManager.getSnapshotReplicaDir();
      subDirectory += "/" + SNAPSHOT_STR + server.getShard() + "/0";

      awsClient.uploadDirectory(bucket, prefix, subDirectory, srcDir);
    }

    public void restoreAWS(String bucket, String prefix, String subDirectory) {
      try {
        AWSClient awsClient = proServer.getAwsClient();
        File destDir = snapshotManager.getSnapshotReplicaDir();
        subDirectory += "/" + SNAPSHOT_STR + server.getShard() + "/0";

        FileUtils.deleteDirectory(snapshotManager.getSnapshotReplicaDir());
        snapshotManager.getSnapshotReplicaDir().mkdirs();

        awsClient.downloadDirectory(bucket, prefix, subDirectory, destDir);
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }

  }

  class LogManagerHandler {
    public long getBackupLocalFileSystemSize() {
      File srcDir = new File(logManager.getLogReplicaDir(), "self");
      long size = 0;
      if (srcDir.exists()) {
        size += FileUtils.sizeOfDirectory(srcDir);
      }

      for (int replica = 0; replica < server.getReplicationFactor(); replica++) {
        srcDir = new File(logManager.getLogReplicaDir(), "peer-" + replica);
        if (srcDir.exists()) {
          size += FileUtils.sizeOfDirectory(srcDir);
        }
      }
      return size;
    }

    public void backupFileSystem(String directory, String subDirectory, String logSlicePoint) {
      try {
        File srcDir = new File(logManager.getLogReplicaDir(), "self");
        File destDir = new File(directory, subDirectory + "/queue/" + server.getShard() + "/0/self");
        backupLogDir(logSlicePoint, destDir, srcDir);

        for (int replica = 0; replica < server.getReplicationFactor(); replica++) {
          srcDir = new File(logManager.getLogReplicaDir(), "peer-" + replica);
          destDir = new File(directory, subDirectory + "/queue/" + server.getShard() + "/0/peer-" + replica);
          backupLogDir(logSlicePoint, destDir, srcDir);
        }
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }

    private void backupLogDir(String logSlicePoint, File destDir, File srcDir) throws IOException {
      File[] files = srcDir.listFiles();
      if (files != null) {
        Set<String> sliceFiles = new HashSet<>();
        BufferedReader reader = new BufferedReader(new StringReader(logSlicePoint));
        while (true) {
          String line = reader.readLine();
          if (line == null) {
            break;
          }
          sliceFiles.add(line);
        }
        destDir.mkdirs();
        for (File file : files) {
          if (sliceFiles.contains(file.getAbsolutePath())) {
            File destFile = new File(destDir, file.getName());
            FileUtils.copyFile(file, destFile);
          }
        }
      }
    }

    private void backupLogDirToAWS(AWSClient awsClient, String logSlicePoint, String bucket, String prefix,
                                   String destDir, File srcDir) throws IOException {
      File[] files = srcDir.listFiles();
      if (files != null) {
        Set<String> sliceFiles = new HashSet<>();
        BufferedReader reader = new BufferedReader(new StringReader(logSlicePoint));
        while (true) {
          String line = reader.readLine();
          if (line == null) {
            break;
          }
          sliceFiles.add(line);
        }
        for (File file : files) {
          if (sliceFiles.contains(file.getAbsolutePath())) {
            awsClient.uploadFile(bucket, prefix, destDir, file);
          }
        }
      }
    }

    public void restoreFileSystem(String directory, String subDirectory) {
      try {
        File destDir = new File(logManager.getLogReplicaDir(), "self");
        File srcDir = new File(directory, subDirectory + "/queue/" + server.getShard() + "/0/self");
        restoreLogDir(srcDir, destDir);

        for (int replica = 0; replica < server.getReplicationFactor(); replica++) {
          destDir = new File(logManager.getLogReplicaDir(), "peer-" + replica);
          srcDir = new File(directory, subDirectory + "/queue/" + server.getShard() + "/0/peer-" + replica);
          restoreLogDir(srcDir, destDir);
        }

      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }

    private void restoreLogDir(File srcDir, File destDir) throws IOException {
      if (destDir.exists()) {
        FileUtils.deleteDirectory(destDir);
      }
      destDir.mkdirs();

      if (srcDir.exists()) {
        FileUtils.copyDirectory(srcDir, destDir);
      }
    }

    public void backupAWS(String bucket, String prefix, String subDirectory, String logSlicePoint) {
      AWSClient awsClient = proServer.getAwsClient();
      try {
        File srcDir = new File(logManager.getLogReplicaDir(), "self");
        String destDir = subDirectory + "/queue/" + server.getShard() + "/0/self";
        backupLogDirToAWS(awsClient, logSlicePoint, bucket, prefix, destDir, srcDir);

        for (int replica = 0; replica < server.getReplicationFactor(); replica++) {
          srcDir = new File(logManager.getLogReplicaDir(), "peer-" + replica);
          destDir = subDirectory + "/queue/" + server.getShard() + "/0/peer-" + replica;
          backupLogDirToAWS(awsClient, logSlicePoint, bucket, prefix, destDir, srcDir);
        }
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }

    public void restoreAWS(String bucket, String prefix, String subDirectory) {
      try {
        AWSClient awsClient = proServer.getAwsClient();
        File destDir = logManager.getLogReplicaDir();
        subDirectory += "/queue/" + server.getShard() + "/0";

        FileUtils.deleteDirectory(destDir);
        destDir.mkdirs();

        awsClient.downloadDirectory(bucket, prefix, subDirectory, destDir);
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }
  }

  class DeleteHandler {
    public void backupAWS(String bucket, String prefix, String subDirectory) {
      AWSClient awsClient = proServer.getAwsClient();
      File srcDir = deleteManager.getReplicaRoot();
      subDirectory += "/deletes/" + server.getShard() + "/0";

      if (srcDir.exists()) {
        awsClient.uploadDirectory(bucket, prefix, subDirectory, srcDir);
      }
    }

    public void restoreAWS(String bucket, String prefix, String subDirectory) {
      try {
        AWSClient awsClient = proServer.getAwsClient();
        File destDir = deleteManager.getReplicaRoot();
        subDirectory += "/deletes/" + server.getShard() + "/0";

        FileUtils.deleteDirectory(destDir);
        destDir.mkdirs();

        awsClient.downloadDirectory(bucket, prefix, subDirectory, destDir);
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }

    public long getBackupLocalFileSystemSize() {
      File dir = deleteManager.getReplicaRoot();
      return com.sonicbase.common.FileUtils.sizeOfDirectory(dir);
    }

    public void deleteTempDirs() {

    }

    public void backupFileSystem(String directory, String subDirectory) {
      try {
        File dir = deleteManager.getReplicaRoot();
        File destDir = new File(directory, subDirectory + "/deletes/" + server.getShard() + "/0");
        if (dir.exists()) {
          FileUtils.copyDirectory(dir, destDir);
        }
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }

    public void restoreFileSystem(String directory, String subDirectory) {
      try {
        File destDir = deleteManager.getReplicaRoot();
        if (destDir.exists()) {
          FileUtils.deleteDirectory(destDir);
        }
        destDir.mkdirs();
        File srcDir = new File(directory, subDirectory + "/deletes/" + server.getShard() + "/0");
        if (srcDir.exists()) {
          FileUtils.copyDirectory(srcDir, destDir);
        }
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }
  }

  public ComObject prepareForBackup(ComObject cobj, boolean replayedCommand) {

    Map<String, Object> backup = (Map<String, Object>) server.getConfig().getMap().get("backup");
    if (backupConfig == null) {
      backupConfig = backup;
    }

    server.getSnapshotManager().enableSnapshot(false);

    logSlicePoint = server.getLogManager().sliceLogs(true);

    isBackupComplete = false;

    backupException = null;

    ComObject ret = new ComObject(1);
    ret.put(ComObject.Tag.REPLICA, server.getReplica());
    return ret;
  }

  public long getBackupLocalFileSystemSize() {
    long size = 0;
    size += deleteHandler.getBackupLocalFileSystemSize();
    size += longRunningCallsHandler.getBackupLocalFileSystemSize();
    size += snapshotManagerHandler.getBackupLocalFileSystemSize();
    size += logManagerHandler.getBackupLocalFileSystemSize();
    return size;
  }

  public long getBackupS3Size(String bucket, String prefix, String subDirectory) {
    AWSClient client = proServer.getAwsClient();
    return client.getDirectorySize(bucket, prefix, subDirectory);
  }

  public ComObject getBackupStatus(final ComObject obj) {

    List<Future> futures = new ArrayList<>();
    long srcSize = 0;
    long destSize = 0;
    for (int i = 0; i < server.getShardCount(); i++) {
      for (int j = 0; j < server.getReplicationFactor(); j++) {

        final int finalI = i;
        final int finalJ = j;
        futures.add(server.getExecutor().submit((Callable) () -> {
          ComObject cobj = new ComObject(2);
          cobj.put(ComObject.Tag.DB_NAME, "__none__");
          cobj.put(ComObject.Tag.SCHEMA_VERSION, server.getCommon().getSchemaVersion());

          ComObject ret = new ComObject(server.getDatabaseClient().send("BackupManager:doGetBackupSizes",
              finalI, finalJ, cobj, DatabaseClient.Replica.SPECIFIED));
          return ret;
        }));
      }
    }

    String error = null;
    boolean first = true;
    for (Future future : futures) {
      try {
        ComObject ret = (ComObject) future.get();
        String type = (String) backupConfig.get("type");
        if (type.equals("AWS")) {
          srcSize += ret.getLong(ComObject.Tag.SOURCE_SIZE);
          if (first) {
            destSize += ret.getLong(ComObject.Tag.DEST_SIZE);
            first = false;
          }
        }
        else if (backupConfig.containsKey("sharedDirectory")) {
          srcSize += ret.getLong(ComObject.Tag.SOURCE_SIZE);
          if (first) {
            destSize += ret.getLong(ComObject.Tag.DEST_SIZE);
            first = false;
          }
        }
        else {
          srcSize += ret.getLong(ComObject.Tag.SOURCE_SIZE);
          destSize += ret.getLong(ComObject.Tag.DEST_SIZE);
        }
        String localError = ret.getString(ComObject.Tag.EXCEPTION);
        if (localError != null) {
          error = localError;
          logger.error("Error performing backup: " + error);
        }
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new DatabaseException(e);
      }
      catch (ExecutionException e) {
        logger.error("Error getting backup status", e);
        error = ExceptionUtils.getFullStackTrace(e);
      }
    }
    ComObject retObj = new ComObject(2);
    double percent = destSize == 0 || srcSize == 0 ? 0d : (double)destSize / (double)srcSize;
    percent = Math.min(percent, 01.0d);
    retObj.put(ComObject.Tag.PERCENT_COMPLETE, percent);
    if (error != null) {
      retObj.put(ComObject.Tag.EXCEPTION, error);
    }
    return retObj;
  }

  public ComObject doGetBackupSizes(final ComObject obj) {

    Map<String, Object> backup = (Map<String, Object>) server.getConfig().getMap().get("backup");
    if (backupConfig == null) {
      backupConfig = backup;
    }

    long begin = System.currentTimeMillis();
    long srcSize = 0;
    long end = 0;
    long destSize = 0;
    long beginDest = 0;
    boolean error = false;
    try {
      srcSize = getBackupLocalFileSystemSize();
      end = System.currentTimeMillis();
      destSize = 0;
      String type = (String) backupConfig.get("type");
      beginDest = System.currentTimeMillis();
      if (type.equals("AWS")) {
        String bucket = (String) backupConfig.get("bucket");
        String prefix = (String) backupConfig.get("prefix");
        destSize = getBackupS3Size(bucket, prefix, lastBackupDir);
      }
      else {
        destSize = FileUtils.sizeOfDirectory(new File(backupFileSystemDir, lastBackupDir));
      }
    }
    catch (Exception e) {
      logger.error("Error getting backup sizes", e);
      backupException = e;
    }
    long endDest = System.currentTimeMillis();
    ComObject retObj = new ComObject(3);
    retObj.put(ComObject.Tag.SOURCE_SIZE, srcSize);
    retObj.put(ComObject.Tag.DEST_SIZE, destSize);
    if (backupException != null) {
      retObj.put(ComObject.Tag.EXCEPTION, ExceptionUtils.getFullStackTrace(backupException));
    }

    logger.info("backup sizes: shard=" + server.getShard() + ", replica=" + server.getReplica() +
        ", srcTime=" + (end - begin) + ", destTime=" + (endDest - beginDest) + ", srcSize=" + srcSize + ", destSize=" + destSize);
    return retObj;
  }

  public ComObject getRestoreStatus(final ComObject obj) {

    if (startedPrepareData) {
      ComObject ret = server.getRecoverProgress(null, false);
      if (restoreException != null) {
        ret.put(ComObject.Tag.EXCEPTION, ExceptionUtils.getFullStackTrace(restoreException));
      }
      return ret;
    }
    else {
      List<Future> futures = new ArrayList<>();
      long srcSize = 0;
      long destSize = 0;
      for (int i = 0; i < server.getShardCount(); i++) {
        for (int j = 0; j < server.getReplicationFactor(); j++) {

          final int finalI = i;
          final int finalJ = j;
          futures.add(server.getExecutor().submit((Callable) () -> {
            ComObject cobj = new ComObject(2);
            cobj.put(ComObject.Tag.DB_NAME, "__none__");
            cobj.put(ComObject.Tag.SCHEMA_VERSION, server.getCommon().getSchemaVersion());

            ComObject ret = new ComObject(server.getDatabaseClient().send("BackupManager:doGetRestoreSizes",
                finalI, finalJ, cobj, DatabaseClient.Replica.SPECIFIED));
            return ret;
          }));
        }
      }

      String error = null;
      boolean first = true;
      for (Future future : futures) {
        try {
          ComObject ret = (ComObject) future.get();
          String type = (String) backupConfig.get("type");
          if (type.equals("AWS")) {
            if (first) {
              srcSize += ret.getLong(ComObject.Tag.SOURCE_SIZE);
              first = false;
            }
            destSize += ret.getLong(ComObject.Tag.DEST_SIZE);
          }
          else if (backupConfig.containsKey("sharedDirectory")) {
            if (first) {
              srcSize += ret.getLong(ComObject.Tag.SOURCE_SIZE);
              first = false;
            }
            destSize += ret.getLong(ComObject.Tag.DEST_SIZE);
          }
          else {
            srcSize += ret.getLong(ComObject.Tag.SOURCE_SIZE);
            destSize += ret.getLong(ComObject.Tag.DEST_SIZE);
          }
          String localError = ret.getString(ComObject.Tag.EXCEPTION);
          if (localError != null) {
            error = localError;
            logger.error("Error performing restore: " + error);
            restoreException = new Exception(error);
          }
        }
        catch (InterruptedException e) {
          throw new DatabaseException(e);
        }
        catch (ExecutionException e) {
          logger.error("Error getting restore status", e);
          error = ExceptionUtils.getFullStackTrace(e);
        }
      }
      srcSize *= (double)server.getReplicationFactor();

      ComObject retObj = new ComObject(3);
      double percent = destSize == 0 || srcSize == 0 ? 0d : (double)destSize / (double)srcSize;
      percent = Math.min(percent, 01.0d);
      retObj.put(ComObject.Tag.PERCENT_COMPLETE, percent);
      retObj.put(ComObject.Tag.STAGE, "copyingFiles");
      if (error != null) {
        retObj.put(ComObject.Tag.EXCEPTION, error);
      }
      return retObj;
    }
  }

  public ComObject doGetRestoreSizes(final ComObject obj) {
    Map<String, Object> backup = (Map<String, Object>) server.getConfig().getMap().get("backup");
    if (backupConfig == null) {
      backupConfig = backup;
    }

    long destSize = 0;
    long srcSize = 0;
    try {
      destSize = getBackupLocalFileSystemSize();
      srcSize = 0;
      String type = (String) backupConfig.get("type");
      if (type.equals("AWS")) {
        String bucket = (String) backupConfig.get("bucket");
        String prefix = (String) backupConfig.get("prefix");
        srcSize = getBackupS3Size(bucket, prefix, lastBackupDir);
      }
      else {
        srcSize = FileUtils.sizeOfDirectory(new File(backupFileSystemDir, lastBackupDir));
      }
    }
    catch (Exception e) {
      logger.error("Error getting restore sizes", e);
      restoreException = e;
    }
    ComObject retObj = new ComObject(3);
    retObj.put(ComObject.Tag.SOURCE_SIZE, srcSize);
    retObj.put(ComObject.Tag.DEST_SIZE, destSize);
    if (restoreException != null) {
      retObj.put(ComObject.Tag.EXCEPTION, ExceptionUtils.getFullStackTrace(restoreException));
    }
    logger.info("restore sizes: shard=" + server.getShard() + ", replica=" + server.getReplica() + ", srcSize=" +
        srcSize + ", destSize=" + destSize + ", backupDir=" + lastBackupDir);
    return retObj;
  }


  class LongRunningCallsHandler {
    public long getBackupLocalFileSystemSize() {
      File dir = longRunningCalls.getReplicaRoot();
      return com.sonicbase.common.FileUtils.sizeOfDirectory(dir);
    }

    public void backupFileSystem(String directory, String subDirectory) {
      try {
        File dir = longRunningCalls.getReplicaRoot();
        File destDir = new File(directory, subDirectory + "/lrc/" + server.getShard() + "/0");
        if (dir.exists()) {
          FileUtils.copyDirectory(dir, destDir);
        }
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }

    public void restoreFileSystem(String directory, String subDirectory) {
      try {
        File destDir = longRunningCalls.getReplicaRoot();
        if (destDir.exists()) {
          FileUtils.deleteDirectory(destDir);
        }
        destDir.mkdirs();
        File srcDir = new File(directory, subDirectory + "/lrc/" + server.getShard() + "/0");
        if (srcDir.exists()) {
          FileUtils.copyDirectory(srcDir, destDir);
        }
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }

    public void restoreAWS(String bucket, String prefix, String subDirectory) {
      try {
        AWSClient awsClient = proServer.getAwsClient();
        File destDir = longRunningCalls.getReplicaRoot();
        subDirectory += "/lrc/" + server.getShard() + "/0";

        FileUtils.deleteDirectory(destDir);
        destDir.mkdirs();

        awsClient.downloadDirectory(bucket, prefix, subDirectory, destDir);
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }

    public void backupAWS(String bucket, String prefix, String subDirectory) {
      AWSClient awsClient = proServer.getAwsClient();
      File srcDir = longRunningCalls.getReplicaRoot();
      subDirectory += "/lrc/" + server.getShard() + "/0";

      awsClient.uploadDirectory(bucket, prefix, subDirectory, srcDir);
    }
  }

  public ComObject doBackupFileSystem(final ComObject cobj, boolean replayedCommand) {
    backupFileSystemThread = ThreadUtil.createThread(() -> {
      try {
        String directory = cobj.getString(ComObject.Tag.DIRECTORY);
        String subDirectory = cobj.getString(ComObject.Tag.SUB_DIRECTORY);

        directory = directory.replace("$HOME", System.getProperty("user.home"));

        lastBackupDir = subDirectory;
        backupFileSystemDir = directory;

        backupFileSystemDir = directory;
        backupFileSystemSingleDir(directory, subDirectory, "logSequenceNum");
        backupFileSystemSingleDir(directory, subDirectory, "nextRecordId");
        deleteHandler.deleteTempDirs();
        deleteHandler.backupFileSystem(directory, subDirectory);
        longRunningCallsHandler.backupFileSystem(directory, subDirectory);
//          snapshotManager.deleteTempDirs();
//          snapshotManager.backupFileSystem(directory, subDirectory);
        server.getSnapshotManager().enableSnapshot(false);
        snapshotManagerHandler.deleteTempDirs();
        snapshotManagerHandler.deleteDeletedDirs();
        snapshotManagerHandler.backupFileSystem(directory, subDirectory);
        synchronized (server.getCommon()) {
          //snapshotManager.backupFileSystemSchema(directory, subDirectory);
          snapshotManagerHandler.backupFileSystemSchema(directory, subDirectory);
        }
        logManagerHandler.backupFileSystem(directory, subDirectory, logSlicePoint);

        server.getSnapshotManager().enableSnapshot(true);

        isBackupComplete = true;

      }
      catch (Exception e) {
        logger.error("Error backing up database", e);
        backupException = e;
      }
    }, "SonicBase Backup FileSystem Thread");
    backupFileSystemThread.start();
    return null;
  }

  private void backupFileSystemSingleDir(String directory, String subDirectory, String dirName) {
    try {
      File dir = new File(server.getDataDir(), dirName + "/" + server.getShard() + "/0");
      File destDir = new File(directory, subDirectory + "/" + dirName + "/" + server.getShard() + "/0");
      if (dir.exists()) {
        FileUtils.copyDirectory(dir, destDir);
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public ComObject doBackupAWS(final ComObject cobj, boolean replayedCommand) {
    backupAWSThread = ThreadUtil.createThread(() -> {
      try {
        String subDirectory = cobj.getString(ComObject.Tag.SUB_DIRECTORY);
        String bucket = cobj.getString(ComObject.Tag.BUCKET);
        String prefix = cobj.getString(ComObject.Tag.PREFIX);

        lastBackupDir = subDirectory;

        backupAWSSingleDir(bucket, prefix, subDirectory, "logSequenceNum");
        backupAWSSingleDir(bucket, prefix, subDirectory, "nextRecordId");
        deleteHandler.deleteTempDirs();
        deleteHandler.backupAWS(bucket, prefix, subDirectory);
        longRunningCallsHandler.backupAWS(bucket, prefix, subDirectory);
        //snapshotManager.deleteTempDirs();
        server.getSnapshotManager().enableSnapshot(false);
        snapshotManagerHandler.deleteDeletedDirs();
        snapshotManagerHandler.deleteTempDirs();
        //snapshotManager.backupAWS(bucket, prefix, subDirectory);
        snapshotManagerHandler.backupAWS(bucket, prefix, subDirectory);
        synchronized (server.getCommon()) {
          //snapshotManager.backupAWSSchema(bucket, prefix, subDirectory);
          snapshotManagerHandler.backupAWSSchema(bucket, prefix, subDirectory);
        }
        logManagerHandler.backupAWS(bucket, prefix, subDirectory, logSlicePoint);

        server.getSnapshotManager().enableSnapshot(true);

        isBackupComplete = true;
      }
      catch (Exception e) {
        logger.error("Error backing up database", e);
        backupException = e;
      }
    }, "SonicBase Backup AWS Thread");
    backupAWSThread.start();
    return null;
  }

  private void backupAWSSingleDir(String bucket, String prefix, String subDirectory, String dirName) {
    AWSClient awsClient = proServer.getAwsClient();
    File srcDir = new File(server.getDataDir(), dirName + "/" + server.getShard() + "/" + server.getReplica());
    if (srcDir.exists()) {
      subDirectory += "/" + dirName + "/" + server.getShard() + "/0";

      awsClient.uploadDirectory(bucket, prefix, subDirectory, srcDir);
    }
  }

  public ComObject isBackupComplete(ComObject cobj, boolean replayedCommand) {
    try {
      ComObject retObj = new ComObject(1);
      retObj.put(ComObject.Tag.IS_COMPLETE, isBackupComplete);
      return retObj;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public ComObject finishBackup(ComObject cobj, boolean replayedCommand) {
    try {
      boolean shared = cobj.getBoolean(ComObject.Tag.SHARED);
      String directory = cobj.getString(ComObject.Tag.DIRECTORY);
      int maxBackupCount = cobj.getInt(ComObject.Tag.MAX_BACKUP_COUNT);
      String type = cobj.getString(ComObject.Tag.TYPE);

      if (type.equals("fileSystem")) {
        if (!shared) {
          doDeleteFileSystemBackups(directory, maxBackupCount);
        }
      }
      server.getSnapshotManager().enableSnapshot(true);
      isBackupComplete = false;
      return null;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private void doDeleteFileSystemBackups(String directory, int maxBackupCount) {
    File file = new File(directory);
    File[] backups = file.listFiles();
    if (backups != null) {
      Arrays.sort(backups, Comparator.comparing(File::getAbsolutePath));
      for (int i = 0; i < backups.length; i++) {
        if (i > maxBackupCount) {
          try {
            FileUtils.deleteDirectory(backups[i]);
          }
          catch (Exception e) {
            logger.error("Error deleting backup: dir=" + backups[i].getAbsolutePath(), e);
          }
        }
      }
    }
  }

  public ComObject isEntireBackupComplete(ComObject cobj, boolean replayedCommand) {
    try {
      if (finalBackupException != null) {
        throw new DatabaseException("Error performing backup", finalBackupException);
      }
      ComObject retObj = new ComObject(1);
      retObj.put(ComObject.Tag.IS_COMPLETE, !doingBackup);

      return retObj;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public byte[] startBackup(ComObject cobj, boolean replayedCommand) {

    Map<String, Object> backup = (Map<String, Object>) server.getConfig().getMap().get("backup");
    if (backupConfig == null) {
      backupConfig = backup;
    }

    final boolean wasDoingBackup = doingBackup;
    doingBackup = true;
    backupMainThread = ThreadUtil.createThread(() -> {
      try {
        if (wasDoingBackup) {
          while (doingBackup) {
            Thread.sleep(2000);
          }
        }
        doingBackup = true;
        doBackup();
      }
      catch (Exception e) {
        logger.error("Error doing backup", e);
      }
    }, "SonicBase Backup Main Thread");
    backupMainThread.start();
    return null;
  }

  public void setBackupConfig(Map<String, Object> backupConfig) {
    this.backupConfig = backupConfig;
  }

  class BackupJob implements Job {

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
      JobDataMap map = jobExecutionContext.getMergedJobDataMap();
      com.sonicbase.server.DatabaseServer server = (com.sonicbase.server.DatabaseServer) map.get("server");
      doBackup();
    }
  }

  public void scheduleBackup() {
    try {
      Map<String, Object> backup = (Map<String, Object>) server.getConfig().getMap().get("backup");
      if (backupConfig == null) {
        backupConfig = backup;
      }
      if (backupConfig == null) {
        return;
      }
      String cronSchedule = (String) backupConfig.get("cronSchedule");
      if (cronSchedule == null) {
        return;
      }
      JobDataMap map = new JobDataMap();
      map.put("server", this);

      logger.info("Scheduling backup: cronSchedule=" + cronSchedule);

      JobDetail job = newJob(BackupJob.class)
          .withIdentity("job" + cronIdentity, "group1")
          .usingJobData(map)
          .build();
      Trigger trigger = newTrigger()
          .withIdentity("trigger" + cronIdentity, "group1")
          .withSchedule(cronSchedule(cronSchedule))
          .forJob("myJob" + cronIdentity, "group1")
          .build();
      cronIdentity++;

      SchedulerFactory sf = new StdSchedulerFactory();
      Scheduler sched = sf.getScheduler();
      sched.scheduleJob(job, trigger);
      sched.start();
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public ComObject getLastBackupDir(ComObject cobj, boolean replayedCommand) {
    ComObject retObj = new ComObject(1);
    if (lastBackupDir != null) {
      retObj.put(ComObject.Tag.DIRECTORY, lastBackupDir);
    }
    return retObj;
  }

  public void doBackup() {
    try {
      server.shutdownRepartitioner();
      finalBackupException = null;
      doingBackup = true;

      logger.info("Backup Master - begin");

      logger.info("Backup Master - prepareForBackup - begin");
      // tell all servers to pause snapshot and slice the queue
      ComObject cobj = new ComObject(2);
      cobj.put(ComObject.Tag.DB_NAME, "__none__");
      cobj.put(ComObject.Tag.SCHEMA_VERSION, server.getCommon().getSchemaVersion());
      byte[][] ret = server.getDatabaseClient().sendToAllShards("BackupManager:prepareForBackup",
          0, cobj, DatabaseClient.Replica.MASTER);
      int[] masters = new int[server.getShardCount()];
      for (int i = 0; i < ret.length; i++) {
        ComObject retObj = new ComObject(ret[i]);
        masters[i] = retObj.getInt(ComObject.Tag.REPLICA);
      }
      logger.info("Backup Master - prepareForBackup - finished");

      String subDirectory = DateUtils.toString(new Date(System.currentTimeMillis()));
      lastBackupDir = subDirectory;
      Map<String, Object> backup = (Map<String, Object>) server.getConfig().getMap().get("backup");
      if (backupConfig == null) {
        backupConfig = backup;
      }

      String type = (String) backupConfig.get("type");
      if (type.equals("AWS")) {
        doAWSBackup(masters, subDirectory);
      }
      else if (type.equals("fileSystem")) {
        doFieSystemBackup(masters, subDirectory);
      }

      waitForBackupToComplete(masters);

      logger.info("Backup Master - doBackup finished");

      logger.info("Backup Master - delete old backups - begin");

      Integer maxBackupCount = (Integer) backupConfig.get("maxBackupCount");
      String directory = null;
      if (backupConfig.containsKey("directory")) {
        directory = (String) backupConfig.get("directory");
      }
      Boolean shared = null;
      if (backupConfig.containsKey("sharedDirectory")) {
        shared = (Boolean) backupConfig.get("sharedDirectory");
      }
      if (shared == null) {
        shared = false;
      }

      shared = deleteOldBackups(type, maxBackupCount, directory, shared);

      logger.info("Backup Master - delete old backups - finished");

      logger.info("Backup Master - finishBackup - begin");

      ComObject fcobj = new ComObject(7);
      fcobj.put(ComObject.Tag.DB_NAME, "__none__");
      fcobj.put(ComObject.Tag.SCHEMA_VERSION, server.getCommon().getSchemaVersion());
      fcobj.put(ComObject.Tag.METHOD, "BackupManager:finishBackup");
      fcobj.put(ComObject.Tag.SHARED, shared);
      if (directory != null) {
        fcobj.put(ComObject.Tag.DIRECTORY, directory);
      }
      fcobj.put(ComObject.Tag.TYPE, type);
      fcobj.put(ComObject.Tag.MAX_BACKUP_COUNT, maxBackupCount);
      for (int i = 0; i < server.getShardCount(); i++) {
        server.getDatabaseClient().send(null, i, masters[i], fcobj, DatabaseClient.Replica.SPECIFIED);
      }

      logger.info("Backup Master - finishBackup - finished");
    }
    catch (Exception e) {
      finalBackupException = e;
      logger.error("Error performing backup", e);
      throw new DatabaseException(e);
    }
    finally {
      doingBackup = false;
      server.startRepartitioner();
      logger.info("Backup - finished");
    }
  }

  private Boolean deleteOldBackups(String type, Integer maxBackupCount, String directory, Boolean shared) {
    if (maxBackupCount != null) {
      try {
        // delete old backups
        if (type.equals("AWS")) {
          String bucket = (String) backupConfig.get("bucket");
          String prefix = (String) backupConfig.get("prefix");
          shared = true;
          doDeleteAWSBackups(bucket, prefix, maxBackupCount);
        }
        else if (type.equals("fileSystem")) {
          if (shared) {
            doDeleteFileSystemBackups(directory, maxBackupCount);
          }
        }
      }
      catch (Exception e) {
        logger.error("Error deleting old backups", e);
      }
    }
    return shared;
  }

  private void waitForBackupToComplete(int[] masters) throws InterruptedException {
    while (!server.getShutdown()) {
      ComObject iscobj = new ComObject(2);
      iscobj.put(ComObject.Tag.DB_NAME, "__none__");
      iscobj.put(ComObject.Tag.SCHEMA_VERSION, server.getCommon().getSchemaVersion());

      boolean finished = false;
      outer:
      for (int shard = 0; shard < server.getShardCount(); shard++) {
        try {
          byte[] currRet = server.getDatabaseClient().send("BackupManager:isBackupComplete", shard,
              masters[shard], iscobj, DatabaseClient.Replica.SPECIFIED);
          ComObject retObj = new ComObject(currRet);
          finished = retObj.getBoolean(ComObject.Tag.IS_COMPLETE);
          if (!finished) {
            break outer;
          }
        }
        catch (Exception e) {
          finished = false;
          logger.error("Error checking if backup complete: shard=" + shard);
          break outer;
        }
      }
      if (finished) {
        break;
      }
      Thread.sleep(1000);
    }
  }

  private void doFieSystemBackup(int[] masters, String subDirectory) {
    // if fileSystem
    //    tell all servers to copy files to backup directory with a specific root directory
    String directory = (String) backupConfig.get("directory");

    logger.info("Backup Master - doBackupFileSystem - begin");

    ComObject docobj = new ComObject(4);
    docobj.put(ComObject.Tag.DB_NAME, "__none__");
    docobj.put(ComObject.Tag.SCHEMA_VERSION, server.getCommon().getSchemaVersion());
    docobj.put(ComObject.Tag.DIRECTORY, directory);
    docobj.put(ComObject.Tag.SUB_DIRECTORY, subDirectory);
    for (int i = 0; i < server.getShardCount(); i++) {
      server.getDatabaseClient().send("BackupManager:doBackupFileSystem", i, masters[i], docobj,
          DatabaseClient.Replica.SPECIFIED);
    }

    logger.info("Backup Master - doBackupFileSystem - end");
  }

  private void doAWSBackup(int[] masters, String subDirectory) {
    String bucket = (String) backupConfig.get("bucket");
    String prefix = (String) backupConfig.get("prefix");

    // if aws
    //    tell all servers to upload with a specific root directory
    logger.info("Backup Master - doBackupAWS - begin");

    ComObject docobj = new ComObject(5);
    docobj.put(ComObject.Tag.DB_NAME, "__none__");
    docobj.put(ComObject.Tag.SCHEMA_VERSION, server.getCommon().getSchemaVersion());
    docobj.put(ComObject.Tag.SUB_DIRECTORY, subDirectory);
    docobj.put(ComObject.Tag.BUCKET, bucket);
    docobj.put(ComObject.Tag.PREFIX, prefix);

    for (int i = 0; i < server.getShardCount(); i++) {
      server.getDatabaseClient().send("BackupManager:doBackupAWS", i, masters[i], docobj, DatabaseClient.Replica.SPECIFIED);
    }

    logger.info("Backup Master - doBackupAWS - end");
  }

  private void doDeleteAWSBackups(String bucket, String prefix, Integer maxBackupCount) {
    List<String> dirs = proServer.getAwsClient().listDirectSubdirectories(bucket, prefix);
    Collections.sort(dirs, new Comparator<String>() {
      @Override
      public int compare(String o1, String o2) {
        return -1 * o1.compareTo(o2);
      }
    });
    for (int i = 0; i < dirs.size(); i++) {
      if (i >= maxBackupCount) {
        try {
          proServer.getAwsClient().deleteDirectory(bucket, prefix + "/" + dirs.get(i));
        }
        catch (Exception e) {
          logger.error("Error deleting backup from AWS: dir=" + prefix + "/" + dirs.get(i), e);
        }
      }
    }
  }

  public ComObject prepareForRestore(ComObject cobj, boolean replayedCommand) {
    try {
      Map<String, Object> backup = (Map<String, Object>) server.getConfig().getMap().get("backup");
      if (backupConfig == null) {
        backupConfig = backup;
      }

      isRestoreComplete = false;
      finishedRestoreFileCopy = false;
      startedPrepareData = false;

      server.getUpdateManager().truncateAllForSingleServerTruncate();

      proServer.getMonitorManager().getImpl().enableMonitor(false);
      server.getStreamManager().enable(false);

      //isRunning.set(false);
      server.getSnapshotManager().enableSnapshot(false);
      Thread.sleep(5000);
      //snapshotManager.deleteSnapshots();
      snapshotManagerHandler.deleteSnapshots();

      File file = new File(server.getDataDir(), "result-sets/" + server.getShard() + "/" + server.getReplica());
      FileUtils.deleteDirectory(file);

      server.getLogManager().deleteLogs();

      synchronized (server.getCommon()) {
        server.getClient().syncSchema();
        server.getCommon().saveSchema(server.getDataDir());
      }

      return null;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public ComObject doRestoreFileSystem(final ComObject cobj, boolean replayedCommand) {
    Thread thread = ThreadUtil.createThread(() -> {
      Map<String, Object> backup = (Map<String, Object>) server.getConfig().getMap().get("backup");
      if (backupConfig == null) {
        backupConfig = backup;
      }

      restoreFileSystemThread = ThreadUtil.createThread(new Runnable() {
        @Override
        public void run() {
          try {
            finishedRestoreFileCopy = false;
            restoreException = null;

            String directory = cobj.getString(ComObject.Tag.DIRECTORY);
            String subDirectory = cobj.getString(ComObject.Tag.SUB_DIRECTORY);

            backupFileSystemDir = directory;
            lastBackupDir = subDirectory;
            directory = directory.replace("$HOME", System.getProperty("user.home"));

            restoreFileSystemSingleDir(directory, subDirectory, "logSequenceNum");
            restoreFileSystemSingleDir(directory, subDirectory, "nextRecordId");
            deleteHandler.restoreFileSystem(directory, subDirectory);
            longRunningCallsHandler.restoreFileSystem(directory, subDirectory);
            snapshotManagerHandler.restoreFileSystem(directory, subDirectory);

            logManagerHandler.restoreFileSystem(directory, subDirectory);

            server.getCommon().loadSchema(server.getDataDir());

            finishedRestoreFileCopy = true;
          }
          catch (Exception e) {
            logger.error("Error restoring backup", e);
            restoreException = e;
          }
        }
      }, "SonicBase Restore FileSystem Thread");
      restoreFileSystemThread.start();
    }, "doRestoreFileSystem Thread");
    thread.start();

    return null;
  }

  public ComObject prepareDataFromRestore(ComObject cobj, boolean replayedCommand) {
    Thread thread = ThreadUtil.createThread(() -> {
      try {
        startedPrepareData = true;
        prepareDataFromRestore();
        isRestoreComplete = true;
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }, "SonicBase prepareDataFromRestore thread");
    thread.start();
    return null;
  }


  private void restoreFileSystemSingleDir(String directory, String subDirectory, String dirName) {
    try {
      File destDir = new File(server.getDataDir(), dirName + "/" + server.getShard() + "/" + server.getReplica());
      if (destDir.exists()) {
        FileUtils.deleteDirectory(destDir);
      }
      destDir.mkdirs();
      File srcDir = new File(directory, subDirectory + "/" + dirName + "/" + server.getShard() + "/0");
      if (srcDir.exists()) {
        FileUtils.copyDirectory(srcDir, destDir);
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }

  }

  public ComObject doRestoreAWS(final ComObject cobj, boolean replayedCommand) {
    Thread thread = ThreadUtil.createThread(() -> {
      logger.info("doRestoreAWS replica restore - begin");
      Map<String, Object> backup = (Map<String, Object>) server.getConfig().getMap().get("backup");
      if (backupConfig == null) {
        backupConfig = backup;
      }
      restoreException = null;
      finishedRestoreFileCopy = false;
      //synchronized (restoreAwsMutex) {
      String subDirectory = cobj.getString(ComObject.Tag.SUB_DIRECTORY);
      String bucket = cobj.getString(ComObject.Tag.BUCKET);
      String prefix = cobj.getString(ComObject.Tag.PREFIX);

      logger.info("start restore: dir=" + subDirectory);
      lastBackupDir = subDirectory;

      restoreAWSThread = ThreadUtil.createThread(() -> {
        try {
          restoreAWSSingleDir(bucket, prefix, subDirectory, "logSequenceNum");
          restoreAWSSingleDir(bucket, prefix, subDirectory, "nextRecordId");
          deleteHandler.restoreAWS(bucket, prefix, subDirectory);
          longRunningCallsHandler.restoreAWS(bucket, prefix, subDirectory);
          snapshotManagerHandler.restoreAWS(bucket, prefix, subDirectory);
          logManagerHandler.restoreAWS(bucket, prefix, subDirectory);

          server.getCommon().loadSchema(server.getDataDir());

          finishedRestoreFileCopy = true;
          //}
          logger.info("doRestoreAWS replica restore - end");
        }
        catch (Exception e) {
          logger.error("Error restoring backup", e);
          restoreException = e;
        }
      }, "SonicBase Restore AWS Thread");
      restoreAWSThread.start();
    }, "doRestoreAWS Thread");
    thread.start();

    return null;
  }

  private void restoreAWSSingleDir(String bucket, String prefix, String subDirectory, String dirName) {
    try {
      AWSClient awsClient = proServer.getAwsClient();
      File destDir = new File(server.getDataDir(), dirName + "/" + server.getShard() + "/" + server.getReplica());
      subDirectory += "/" + dirName + "/" + server.getShard() + "/0";

      FileUtils.deleteDirectory(destDir);
      destDir.mkdirs();

      awsClient.downloadDirectory(bucket, prefix, subDirectory, destDir);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public ComObject isRestoreComplete(ComObject cobj, boolean replayedCommand) {
    try {
      ComObject retObj = new ComObject(1);
      retObj.put(ComObject.Tag.IS_COMPLETE, isRestoreComplete);
      return retObj;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public ComObject isFileRestoreComplete(ComObject cobj, boolean replayedCommand) {
    try {
      ComObject retObj = new ComObject(1);
      retObj.put(ComObject.Tag.IS_COMPLETE, finishedRestoreFileCopy);
      return retObj;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public ComObject finishRestore(ComObject cobj, boolean replayedCommand) {
    try {
      server.getSnapshotManager().enableSnapshot(true);
      proServer.getMonitorManager().getImpl().enableMonitor(true);
      server.getStreamManager().enable(true);
      //isRunning.set(true);
      isRestoreComplete = false;
      return null;
    }
    catch (Exception e) {
      logger.error("Error finishing restore", e);
      throw new DatabaseException(e);
    }
  }

  private void prepareDataFromRestore() throws Exception {
    for (String dbName : server.getDbNames(server.getDataDir())) {
      server.getSnapshotManager().recoverFromSnapshot(dbName);
    }
    server.getLogManager().applyLogs();
  }

  public ComObject isEntireRestoreComplete(ComObject cobj, boolean replayedCommand) {
    try {
      if (finalRestoreException != null) {
        throw new DatabaseException("Error restoring backup", finalRestoreException);
      }
      ComObject retObj = new ComObject(1);
      retObj.put(ComObject.Tag.IS_COMPLETE, !doingRestore);

      return retObj;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public ComObject startRestore(final ComObject cobj, boolean replayedCommand) {

    doingRestore = true;
    restoreMainThread = ThreadUtil.createThread(new Runnable() {
      @Override
      public void run() {
        try {
          String directory = cobj.getString(ComObject.Tag.DIRECTORY);

          doRestore(directory);
        }
        catch (Exception e) {
          logger.error("Error restoring backup", e);
        }
      }
    }, "SonicBase Restore Main Thread");
    restoreMainThread.start();
    return null;
  }

  private Exception finalRestoreException;
  private Exception finalBackupException;

  private void doRestore(String subDirectory) {
    try {
      server.shutdownRepartitioner();
      server.getSnapshotManager().shutdown();

      finalRestoreException = null;
      doingRestore = true;

      Map<String, Object> backup = (Map<String, Object>) server.getConfig().getMap().get("backup");
      if (backupConfig == null) {
        backupConfig = backup;
      }
      String type = (String) backupConfig.get("type");

      if (type.equals("AWS")) {
        String bucket = (String) backupConfig.get("bucket");
        String prefix = (String) backupConfig.get("prefix");

        String key = null;
        if (USE_SNAPSHOT_MGR_OLD) {
          key = prefix + "/" + subDirectory + "/snapshot/" + server.getShard() + "/0/schema.bin";
        }
        else {
          key = prefix + "/" + subDirectory + "/delta/" + server.getShard() + "/0/schema.bin";
        }
        byte[] bytes = proServer.getAwsClient().downloadBytes(bucket, key);
        synchronized (server.getCommon()) {
          server.getCommon().deserializeSchema(bytes);
          server.getCommon().saveSchema(server.getDataDir());
        }
      }
      else if (type.equals("fileSystem")) {
        String currDirectory = (String) backupConfig.get("directory");
        currDirectory = currDirectory.replace("$HOME", System.getProperty("user.home"));

        File file = new File(currDirectory, subDirectory);
        if (USE_SNAPSHOT_MGR_OLD) {
          file = new File(file, "snapshot/" + server.getShard() + "/0/schema.bin");
        }
        else {
          file = new File(file, "delta/" + server.getShard() + "/0/schema.bin");
        }
        BufferedInputStream in = new BufferedInputStream(new FileInputStream(file));
        byte[] bytes = IOUtils.toByteArray(in);

        synchronized (server.getCommon()) {
          server.getCommon().deserializeSchema(bytes);
          server.getCommon().saveSchema(server.getDataDir());
        }
      }
      //pushSchema();

      //common.clearSchema();

      // delete snapshots and logs
      // enter recovery mode (block commands)
      ComObject cobj = new ComObject(2);
      cobj.put(ComObject.Tag.DB_NAME, "__none__");
      cobj.put(ComObject.Tag.SCHEMA_VERSION, server.getCommon().getSchemaVersion());
      byte[][] ret = server.getDatabaseClient().sendToAllShards("BackupManager:prepareForRestore", 0,
          cobj, DatabaseClient.Replica.ALL, true);

      if (type.equals("AWS")) {
        doAWSRestore(subDirectory);
      }
      else if (type.equals("fileSystem")) {
        doFileSystemRestore(subDirectory);
      }

      waitForRestoreToComplete("BackupManager:isFileRestoreComplete");

      cobj = new ComObject(2);
      cobj.put(ComObject.Tag.DB_NAME, "__none__");
      cobj.put(ComObject.Tag.SCHEMA_VERSION, server.getCommon().getSchemaVersion());
      server.getDatabaseClient().sendToAllShards("BackupManager:prepareDataFromRestore", 0, cobj,
          DatabaseClient.Replica.ALL, true);


      waitForRestoreToComplete("BackupManager:isRestoreComplete");

      synchronized (server.getCommon()) {
        if (server.getShard() != 0 || server.getCommon().getServersConfig().getShards()[0].getMasterReplica() != server.getReplica()) {
          int schemaVersion = server.getCommon().getSchemaVersion() + 100;
          server.getCommon().setSchemaVersion(schemaVersion);
        }
        server.getCommon().saveSchema(server.getDataDir());
      }
      server.pushSchema();

      cobj = new ComObject(2);
      cobj.put(ComObject.Tag.DB_NAME, "__none__");
      cobj.put(ComObject.Tag.SCHEMA_VERSION, server.getCommon().getSchemaVersion());

      ret = server.getDatabaseClient().sendToAllShards("BackupManager:finishRestore", 0, cobj,
          DatabaseClient.Replica.ALL, true);

      for (String dbName : server.getDbNames(server.getDataDir())) {
        server.getClient().beginRebalance(dbName);

        while (!server.getShutdown()) {
          if (server.getClient().isRepartitioningComplete(dbName)) {
            break;
          }
          Thread.sleep(1000);
        }
      }
      cobj = new ComObject(2);
      cobj.put(ComObject.Tag.DB_NAME, "test");
      cobj.put(ComObject.Tag.SCHEMA_VERSION, server.getClient().getCommon().getSchemaVersion());
      server.getClient().sendToAllShards("DeleteManager:forceDeletes", 0, cobj, DatabaseClient.Replica.ALL);

    }
    catch (Exception e) {
      finalRestoreException = e;
      logger.error("Error restoring backup", e);
      throw new DatabaseException(e);
    }
    finally {
      doingRestore = false;
      server.startRepartitioner();
      server.getSnapshotManager().runSnapshotLoop();
    }
  }

  private void waitForRestoreToComplete(String verb) throws InterruptedException {
    ComObject cobj;
    while (!server.getShutdown()) {
      cobj = new ComObject(2);
      cobj.put(ComObject.Tag.DB_NAME, "__none__");
      cobj.put(ComObject.Tag.SCHEMA_VERSION, server.getCommon().getSchemaVersion());

      int finishedCount = 0;
      outer:
      for (int shard = 0; shard < server.getShardCount(); shard++) {
        for (int replica = 0; replica < server.getReplicationFactor(); replica++) {
          try {
            byte[] currRet = server.getDatabaseClient().send(verb, shard, replica, cobj,
                DatabaseClient.Replica.SPECIFIED, true);
            ComObject retObj = new ComObject(currRet);
            boolean finished = retObj.getBoolean(ComObject.Tag.IS_COMPLETE);
            if (!finished) {
              break outer;
            }
            finishedCount++;
          }
          catch (Exception e) {
            logger.error("Error checking if restore is complete", e);
            throw e;
          }
        }
      }
      if (finishedCount == server.getShardCount() * server.getReplicationFactor()) {
        break;
      }
      Thread.sleep(2000);
    }
  }

  private void doFileSystemRestore(String subDirectory) {
    ComObject cobj;
    byte[][] ret;// if fileSystem
    //    tell all servers to copy files to backup directory with a specific root directory
    String directory = (String) backupConfig.get("directory");
    cobj = new ComObject(4);
    cobj.put(ComObject.Tag.DB_NAME, "__none__");
    cobj.put(ComObject.Tag.SCHEMA_VERSION, server.getCommon().getSchemaVersion());
    cobj.put(ComObject.Tag.DIRECTORY, directory);
    cobj.put(ComObject.Tag.SUB_DIRECTORY, subDirectory);
    ret = server.getDatabaseClient().sendToAllShards("BackupManager:doRestoreFileSystem", 0, cobj,
        DatabaseClient.Replica.ALL, true);
  }

  private void doAWSRestore(String subDirectory) {
    ComObject cobj;
    byte[][] ret;// if aws
    //    tell all servers to upload with a specific root directory

    String bucket = (String) backupConfig.get("bucket");
    String prefix = (String) backupConfig.get("prefix");
    cobj = new ComObject(5);
    cobj.put(ComObject.Tag.DB_NAME, "__none__");
    cobj.put(ComObject.Tag.SCHEMA_VERSION, server.getCommon().getSchemaVersion());
    cobj.put(ComObject.Tag.BUCKET, bucket);
    cobj.put(ComObject.Tag.PREFIX, prefix);
    cobj.put(ComObject.Tag.SUB_DIRECTORY, subDirectory);
    ret = server.getDatabaseClient().sendToAllShards("BackupManager:doRestoreAWS", 0, cobj,
        DatabaseClient.Replica.ALL, true);
  }
}
