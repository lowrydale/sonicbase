/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.*;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.util.DateUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.sonicbase.server.DatabaseServer.USE_SNAPSHOT_MGR_OLD;
import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

public class BackupManager {

  private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("com.sonicbase.logger");

  private final DatabaseServer server;
  private String logSlicePoint;
  private boolean isBackupComplete;
  private boolean isRestoreComplete;
  private Exception backupException;
  private Exception restoreException;
  private ObjectNode backupConfig;
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

  public BackupManager(DatabaseServer server) {
    this.server = server;
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

  public ComObject prepareForBackup(ComObject cobj, boolean replayedCommand) {

    server.getDeltaManager().enableSnapshot(false);

    logSlicePoint = server.getLogManager().sliceLogs(true);

    isBackupComplete = false;

    backupException = null;

    ComObject ret = new ComObject();
    ret.put(ComObject.Tag.replica, server.getReplica());
    return ret;
  }

  public long getBackupLocalFileSystemSize() {
    long size = 0;
    size += server.getDeleteManager().getBackupLocalFileSystemSize();
    size += server.getLongRunningCommands().getBackupLocalFileSystemSize();
    size += server.getDeltaManager().getBackupLocalFileSystemSize();
    //size += logManager.getBackupLocalFileSystemSize();
    return size;
  }

  public long getBackupS3Size(String bucket, String prefix, String subDirectory) {
    AWSClient client = new AWSClient(/*getDatabaseClient()*/ null);
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
        futures.add(server.getExecutor().submit(new Callable(){
          @Override
          public Object call() throws Exception {
            ComObject cobj = new ComObject();
            cobj.put(ComObject.Tag.dbName, "__none__");
            cobj.put(ComObject.Tag.schemaVersion, server.getCommon().getSchemaVersion());
            cobj.put(ComObject.Tag.method, "BackupManager:doGetBackupSizes");

            ComObject ret = new ComObject(server.getDatabaseClient().send(null, finalI, finalJ, cobj, DatabaseClient.Replica.specified));
            return ret;
          }
        }));
      }
    }

    boolean error = false;
    for (Future future : futures) {
      try {
        ComObject ret = (ComObject) future.get();
        srcSize += ret.getLong(ComObject.Tag.sourceSize);
        String type = backupConfig.get("type").asText();
        if (type.equals("AWS")) {
          destSize = ret.getLong(ComObject.Tag.destSize);
        }
        else if (backupConfig.has("sharedDirectory")) {
          destSize = ret.getLong(ComObject.Tag.destSize);
        }
        else {
          destSize += ret.getLong(ComObject.Tag.destSize);
        }
        boolean localError = ret.getBoolean(ComObject.Tag.error);
        if (localError) {
          error = true;
        }
      }
      catch (InterruptedException e) {
        throw new DatabaseException(e);
      }
      catch (ExecutionException e) {
        e.printStackTrace();
      }
    }
    ComObject retObj = new ComObject();
    double percent = destSize == 0 || srcSize == 0 ? 0d : (double)destSize / (double)srcSize;
    percent = Math.min(percent, 01.0d);
    retObj.put(ComObject.Tag.percentComplete, percent     );
    retObj.put(ComObject.Tag.error, error);
    return retObj;
  }

  public ComObject doGetBackupSizes(final ComObject obj) {

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
      String type = backupConfig.get("type").asText();
      beginDest = System.currentTimeMillis();
      if (type.equals("AWS")) {
        String bucket = backupConfig.get("bucket").asText();
        String prefix = backupConfig.get("prefix").asText();
        destSize = getBackupS3Size(bucket, prefix, lastBackupDir);
      }
      else {
        destSize = FileUtils.sizeOfDirectory(new File(backupFileSystemDir, lastBackupDir));
      }
    }
    catch (Exception e) {
      logger.error("Error getting backup sizes", e);
      error = true;
    }
    long endDest = System.currentTimeMillis();
    ComObject retObj = new ComObject();
    retObj.put(ComObject.Tag.sourceSize, srcSize);
    retObj.put(ComObject.Tag.destSize, destSize);
    retObj.put(ComObject.Tag.error, error || backupException != null);

    logger.info("backup sizes: shard=" + server.getShard() + ", replica=" + server.getReplica() +
        ", srcTime=" + (end - begin) + ", destTime=" + (endDest - beginDest) + ", srcSize=" + srcSize + ", destSize=" + destSize);
    return retObj;
  }

  public ComObject getRestoreStatus(final ComObject obj) {

    if (finishedRestoreFileCopy) {
      return server.getRecoverProgress(null, false);
    }
    else {
      List<Future> futures = new ArrayList<>();
      long srcSize = 0;
      long destSize = 0;
      for (int i = 0; i < server.getShardCount(); i++) {
        for (int j = 0; j < server.getReplicationFactor(); j++) {

          final int finalI = i;
          final int finalJ = j;
          futures.add(server.getExecutor().submit(new Callable(){
            @Override
            public Object call() throws Exception {
              ComObject cobj = new ComObject();
              cobj.put(ComObject.Tag.dbName, "__none__");
              cobj.put(ComObject.Tag.schemaVersion, server.getCommon().getSchemaVersion());
              cobj.put(ComObject.Tag.method, "BackupManager:doGetRestoreSizes");

              ComObject ret = new ComObject(server.getDatabaseClient().send(null, finalI, finalJ, cobj, DatabaseClient.Replica.specified));
              return ret;
            }
          }));
        }
      }

      boolean error = false;
      for (Future future : futures) {
        try {
          ComObject ret = (ComObject) future.get();
          String type = backupConfig.get("type").asText();
          if (type.equals("AWS")) {
            srcSize = ret.getLong(ComObject.Tag.sourceSize);
          }
          else if (backupConfig.has("sharedDirectory")) {
            srcSize = ret.getLong(ComObject.Tag.sourceSize);
          }
          else {
            srcSize += ret.getLong(ComObject.Tag.sourceSize);
          }
          boolean localError = ret.getBoolean(ComObject.Tag.error);
          if (localError) {
            error = true;
          }
          srcSize *= (double)server.getReplicationFactor();
          destSize += ret.getLong(ComObject.Tag.destSize);
        }
        catch (InterruptedException e) {
          throw new DatabaseException(e);
        }
        catch (ExecutionException e) {
          e.printStackTrace();
        }
      }
      ComObject retObj = new ComObject();
      double percent = destSize == 0 || srcSize == 0 ? 0d : (double)destSize / (double)srcSize;
      percent = Math.min(percent, 01.0d);
      retObj.put(ComObject.Tag.percentComplete, percent);
      retObj.put(ComObject.Tag.stage, "copyingFiles");
      retObj.put(ComObject.Tag.error, error);
      return retObj;
    }
  }

  public ComObject doGetRestoreSizes(final ComObject obj) {
    boolean error = false;
    long destSize = 0;
    long srcSize = 0;
    try {
      destSize = getBackupLocalFileSystemSize();
      srcSize = 0;
      String type = backupConfig.get("type").asText();
      if (type.equals("AWS")) {
        String bucket = backupConfig.get("bucket").asText();
        String prefix = backupConfig.get("prefix").asText();
        srcSize = getBackupS3Size(bucket, prefix, lastBackupDir);
      }
      else {
        srcSize = FileUtils.sizeOfDirectory(new File(backupFileSystemDir, lastBackupDir));
      }
    }
    catch (Exception e) {
      logger.error("Error getting restore sizes", e);
      error = true;
    }
    ComObject retObj = new ComObject();
    retObj.put(ComObject.Tag.sourceSize, srcSize);
    retObj.put(ComObject.Tag.destSize, destSize);
    retObj.put(ComObject.Tag.error, error || restoreException != null);
    logger.info("restore sizes: shard=" + server.getShard() + ", replica=" + server.getReplica() + ", srcSize=" + srcSize + ", destSize=" + destSize);
    return retObj;
  }

  public ComObject doBackupFileSystem(final ComObject cobj, boolean replayedCommand) {
    backupFileSystemThread = ThreadUtil.createThread(new Runnable() {

      @Override
      public void run() {
        try {
          String directory = cobj.getString(ComObject.Tag.directory);
          String subDirectory = cobj.getString(ComObject.Tag.subDirectory);

          directory = directory.replace("$HOME", System.getProperty("user.home"));

          lastBackupDir = subDirectory;
          backupFileSystemDir = directory;

          backupFileSystemDir = directory;
          backupFileSystemSingleDir(directory, subDirectory, "logSequenceNum");
          backupFileSystemSingleDir(directory, subDirectory, "nextRecordId");
          server.getDeleteManager().delteTempDirs();
          server.getDeleteManager().backupFileSystem(directory, subDirectory);
          server.getLongRunningCommands().backupFileSystem(directory, subDirectory);
//          snapshotManager.deleteTempDirs();
//          snapshotManager.backupFileSystem(directory, subDirectory);
          server.getDeltaManager().enableSnapshot(false);
          server.getDeltaManager().deleteTempDirs();
          server.getDeltaManager().deleteDeletedDirs();
          server.getDeltaManager().backupFileSystem(directory, subDirectory);
          synchronized (server.getCommon()) {
            //snapshotManager.backupFileSystemSchema(directory, subDirectory);
            server.getDeltaManager().backupFileSystemSchema(directory, subDirectory);
          }
          server.getLogManager().backupFileSystem(directory, subDirectory, logSlicePoint);

          server.getDeltaManager().enableSnapshot(true);

          isBackupComplete = true;

        }
        catch (Exception e) {
          logger.error("Error backing up database", e);
          backupException = e;
        }
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
    backupAWSThread = ThreadUtil.createThread(new Runnable() {
      @Override
      public void run() {
        try {
          String subDirectory = cobj.getString(ComObject.Tag.subDirectory);
          String bucket = cobj.getString(ComObject.Tag.bucket);
          String prefix = cobj.getString(ComObject.Tag.prefix);

          lastBackupDir = subDirectory;

          backupAWSSingleDir(bucket, prefix, subDirectory, "logSequenceNum");
          backupAWSSingleDir(bucket, prefix, subDirectory, "nextRecordId");
          server.getDeleteManager().delteTempDirs();
          server.getDeleteManager().backupAWS(bucket, prefix, subDirectory);
          server.getLongRunningCommands().backupAWS(bucket, prefix, subDirectory);
          //snapshotManager.deleteTempDirs();
          server.getDeltaManager().enableSnapshot(false);
          server.getDeltaManager().deleteDeletedDirs();
          server.getDeltaManager().deleteTempDirs();
          //snapshotManager.backupAWS(bucket, prefix, subDirectory);
          server.getDeltaManager().backupAWS(bucket, prefix, subDirectory);
          synchronized (server.getCommon()) {
            //snapshotManager.backupAWSSchema(bucket, prefix, subDirectory);
            server.getDeltaManager().backupAWSSchema(bucket, prefix, subDirectory);
          }
          server.getLogManager().backupAWS(bucket, prefix, subDirectory, logSlicePoint);

          server.getDeltaManager().enableSnapshot(true);

          isBackupComplete = true;
        }
        catch (Exception e) {
          logger.error("Error backing up database", e);
          backupException = e;
        }
      }
    }, "SonicBase Backup AWS Thread");
    backupAWSThread.start();
    return null;
  }

  private void backupAWSSingleDir(String bucket, String prefix, String subDirectory, String dirName) {
    AWSClient awsClient = server.getAWSClient();
    File srcDir = new File(server.getDataDir(), dirName + "/" + server.getShard() + "/" + server.getReplica());
    if (srcDir.exists()) {
      subDirectory += "/" + dirName + "/" + server.getShard() + "/0";

      awsClient.uploadDirectory(bucket, prefix, subDirectory, srcDir);
    }
  }

  public ComObject isBackupComplete(ComObject cobj, boolean replayedCommand) {
    try {
      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.isComplete, isBackupComplete);
      return retObj;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public ComObject finishBackup(ComObject cobj, boolean replayedCommand) {
    try {
      boolean shared = cobj.getBoolean(ComObject.Tag.shared);
      String directory = cobj.getString(ComObject.Tag.directory);
      int maxBackupCount = cobj.getInt(ComObject.Tag.maxBackupCount);
      String type = cobj.getString(ComObject.Tag.type);

      if (type.equals("fileSystem")) {
        if (!shared) {
          doDeleteFileSystemBackups(directory, maxBackupCount);
        }
      }
      server.getDeltaManager().enableSnapshot(true);
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
      Arrays.sort(backups, new Comparator<File>() {
        @Override
        public int compare(File o1, File o2) {
          return o1.getAbsolutePath().compareTo(o2.getAbsolutePath());
        }
      });
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
      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.isComplete, !doingBackup);

      return retObj;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public byte[] startBackup(ComObject cobj, boolean replayedCommand) {

    if (!server.getCommon().haveProLicense()) {
      throw new InsufficientLicense("You must have a pro license to start a backup");
    }

    final boolean wasDoingBackup = doingBackup;
    doingBackup = true;
    backupMainThread = ThreadUtil.createThread(new Runnable() {
      @Override
      public void run() {
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
      }
    }, "SonicBase Backup Main Thread");
    backupMainThread.start();
    return null;
  }

  public void setBackupConfig(ObjectNode backupConfig) {
    this.backupConfig = backupConfig;
  }

  public ComObject prepareSourceForServerReload(ComObject cobj, boolean replayedCommand) {
    try {
      List<String> files = new ArrayList<>();

      server.getDeltaManager().enableSnapshot(false);
      logSlicePoint = server.getLogManager().sliceLogs(true);

      BufferedReader reader = new BufferedReader(new StringReader(logSlicePoint));
      while (!server.getShutdown()) {
        String line = reader.readLine();
        if (line == null) {
          break;
        }
        files.add(line);
      }
      //snapshotManager.getFilesForCurrentSnapshot(files);
      server.getDeltaManager().getFilesForCurrentSnapshot(files);

      File file = new File(server.getDataDir(), "logSequenceNum/" + server.getShard() + "/" + server.getReplica() + "/logSequenceNum.txt");
      if (file.exists()) {
        files.add(file.getAbsolutePath());
      }
      file = new File(server.getDataDir(), "nextRecordId/" + server.getShard() + "/" + server.getReplica() + "/nextRecorId.txt");
      if (file.exists()) {
        files.add(file.getAbsolutePath());
      }

      server.getDeleteManager().getFiles(files);
      server.getLongRunningCommands().getFiles(files);

      ComObject retObj = new ComObject();
      ComArray array = retObj.putArray(ComObject.Tag.filenames, ComObject.Type.stringType);
      for (String filename : files) {
        array.add(filename);
      }
      return retObj;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public ComObject isServerReloadFinished(ComObject cobj, boolean replayedCommand) {
    ComObject retObj = new ComObject();
    retObj.put(ComObject.Tag.isComplete, !isServerRoloadRunning);

    return retObj;
  }

  private boolean isServerRoloadRunning = false;

  public ComObject reloadServer(ComObject cobj, boolean replayedCommand) {
    reloadServerThread = ThreadUtil.createThread(new Runnable() {
      @Override
      public void run() {
        try {
          isServerRoloadRunning = true;
          server.setIsRunning(false);
          server.getDeltaManager().enableSnapshot(false);
          Thread.sleep(5000);
          //snapshotManager.deleteSnapshots();
          server.getDeltaManager().deleteSnapshots();

          File file = new File(server.getDataDir(), "result-sets");
          FileUtils.deleteDirectory(file);

          server.getLogManager().deleteLogs();

          String command = "DatabaseServer:ComObject:prepareSourceForServerReload:";
          ComObject rcobj = new ComObject();
          rcobj.put(ComObject.Tag.dbName, "__none__");
          rcobj.put(ComObject.Tag.schemaVersion, server.getCommon().getSchemaVersion());
          byte[] bytes = server.getClient().send(null, server.getShard(), 0, rcobj, DatabaseClient.Replica.master);
          ComObject retObj = new ComObject(bytes);
          ComArray files = retObj.getArray(ComObject.Tag.filenames);

          downloadFilesForReload(files);

          server.getCommon().loadSchema(server.getDataDir());
          server.getClient().syncSchema();
          prepareDataFromRestore();
          server.getDeltaManager().enableSnapshot(true);
          server.setIsRunning(true);
        }
        catch (Exception e) {
          throw new DatabaseException(e);
        }
        finally {
          ComObject rcobj = new ComObject();
          rcobj.put(ComObject.Tag.dbName, "__none__");
          rcobj.put(ComObject.Tag.schemaVersion, server.getCommon().getSchemaVersion());
          rcobj.put(ComObject.Tag.method, "SnapshotManager:finishServerReloadForSource");
          byte[] bytes = server.getClient().send(null, server.getShard(), 0, rcobj, DatabaseClient.Replica.master);

          isServerRoloadRunning = false;
        }
      }
    }, "SonicBase Reload Server Thread");
    reloadServerThread.start();

    return null;
  }

  private void downloadFilesForReload(ComArray files) {
    for (Object obj : files.getArray()) {
      String filename = (String) obj;
      try {

        ComObject cobj = new ComObject();
        cobj.put(ComObject.Tag.dbName, "__none__");
        cobj.put(ComObject.Tag.schemaVersion, server.getCommon().getSchemaVersion());
        cobj.put(ComObject.Tag.method, "DatabaesServer:getDatabaseFile");
        cobj.put(ComObject.Tag.filename, filename);
        byte[] bytes = server.getClient().send(null, server.getShard(), 0, cobj, DatabaseClient.Replica.master);
        ComObject retObj = new ComObject(bytes);
        byte[] content = retObj.getByteArray(ComObject.Tag.binaryFileContent);

        filename = fixReplica("deletes", filename);
        filename = fixReplica("lrc", filename);
        if (USE_SNAPSHOT_MGR_OLD) {
          filename = fixReplica("snapshot", filename);
        }
        else {
          filename = fixReplica("delta", filename);
        }
        filename = fixReplica("log", filename);
        filename = fixReplica("nextRecordId", filename);
        filename = fixReplica("logSequenceNum", filename);

        File file = new File(filename);
        file.getParentFile().mkdirs();
        file.delete();
        try (BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(file))) {
          out.write(content);
        }
      }
      catch (Exception e) {
        logger.error("Error copying file from source server for reloadServer: file=" + filename, e);
      }
    }
  }

  private String fixReplica(String dir, String filename) {
    String prefix = dir + File.separator + server.getShard() + File.separator;
    int pos = filename.indexOf(prefix);
    if (pos != -1) {
      int pos2 = filename.indexOf(File.separator, pos + prefix.length());
      filename = filename.substring(0, pos + prefix.length()) + server.getReplica() + filename.substring(pos2);
    }
    return filename;
  }


  static class BackupJob implements Job {

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
      JobDataMap map = jobExecutionContext.getMergedJobDataMap();
      DatabaseServer server = (DatabaseServer) map.get("server");
      server.getBackupManager().doBackup();
    }
  }

  public void scheduleBackup() {
    try {
      ObjectNode backup = (ObjectNode) server.getConfig().get("backup");
      if (backupConfig == null) {
        backupConfig = backup;
      }
      if (backupConfig == null) {
        return;
      }
      JsonNode cronSchedule = backupConfig.get("cronSchedule");
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
          .withSchedule(cronSchedule(cronSchedule.asText()))
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

  private String lastBackupDir;
  private String backupFileSystemDir;


  public ComObject getLastBackupDir(ComObject cobj, boolean replayedCommand) {
    ComObject retObj = new ComObject();
    if (lastBackupDir != null) {
      retObj.put(ComObject.Tag.directory, lastBackupDir);
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
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.dbName, "__none__");
      cobj.put(ComObject.Tag.schemaVersion, server.getCommon().getSchemaVersion());
      cobj.put(ComObject.Tag.method, "BackupManager:prepareForBackup");
      byte[][] ret = server.getDatabaseClient().sendToAllShards(null, 0, cobj, DatabaseClient.Replica.master);
      int[] masters = new int[server.getShardCount()];
      for (int i = 0; i < ret.length; i++) {
        ComObject retObj = new ComObject(ret[i]);
        masters[i] = retObj.getInt(ComObject.Tag.replica);
      }
      logger.info("Backup Master - prepareForBackup - finished");

      String subDirectory = DateUtils.toString(new Date(System.currentTimeMillis()));
      lastBackupDir = subDirectory;
      ObjectNode backup = (ObjectNode) server.getConfig().get("backup");
      if (backupConfig == null) {
        backupConfig = backup;
      }

      String type = backupConfig.get("type").asText();
      if (type.equals("AWS")) {
        String bucket = backupConfig.get("bucket").asText();
        String prefix = backupConfig.get("prefix").asText();

        // if aws
        //    tell all servers to upload with a specific root directory
        logger.info("Backup Master - doBackupAWS - begin");

        ComObject docobj = new ComObject();
        docobj.put(ComObject.Tag.dbName, "__none__");
        docobj.put(ComObject.Tag.schemaVersion, server.getCommon().getSchemaVersion());
        docobj.put(ComObject.Tag.method, "BackupManager:doBackupAWS");
        docobj.put(ComObject.Tag.subDirectory, subDirectory);
        docobj.put(ComObject.Tag.bucket, bucket);
        docobj.put(ComObject.Tag.prefix, prefix);

        for (int i = 0; i < server.getShardCount(); i++) {
          server.getDatabaseClient().send(null, i, masters[i], docobj, DatabaseClient.Replica.specified);
        }

        logger.info("Backup Master - doBackupAWS - end");
      }
      else if (type.equals("fileSystem")) {
        // if fileSystem
        //    tell all servers to copy files to backup directory with a specific root directory
        String directory = backupConfig.get("directory").asText();

        logger.info("Backup Master - doBackupFileSystem - begin");

        ComObject docobj = new ComObject();
        docobj.put(ComObject.Tag.dbName, "__none__");
        docobj.put(ComObject.Tag.schemaVersion, server.getCommon().getSchemaVersion());
        docobj.put(ComObject.Tag.method, "BackupManager:doBackupFileSystem");
        docobj.put(ComObject.Tag.directory, directory);
        docobj.put(ComObject.Tag.subDirectory, subDirectory);
        for (int i = 0; i < server.getShardCount(); i++) {
          server.getDatabaseClient().send(null, i, masters[i], docobj, DatabaseClient.Replica.specified);
        }

        logger.info("Backup Master - doBackupFileSystem - end");
      }

      while (!server.getShutdown()) {
        ComObject iscobj = new ComObject();
        iscobj.put(ComObject.Tag.dbName, "__none__");
        iscobj.put(ComObject.Tag.schemaVersion, server.getCommon().getSchemaVersion());
        iscobj.put(ComObject.Tag.method, "BackupManager:isBackupComplete");

        boolean finished = false;
        outer:
        for (int shard = 0; shard < server.getShardCount(); shard++) {
          try {
            byte[] currRet = server.getDatabaseClient().send(null, shard, masters[shard], iscobj, DatabaseClient.Replica.specified);
            ComObject retObj = new ComObject(currRet);
            finished = retObj.getBoolean(ComObject.Tag.isComplete);
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
      logger.info("Backup Master - doBackup finished");

      logger.info("Backup Master - delete old backups - begin");

      Integer maxBackupCount = backupConfig.get("maxBackupCount").asInt();
      String directory = null;
      if (backupConfig.has("directory")) {
        directory = backupConfig.get("directory").asText();
      }
      Boolean shared = null;
      if (backupConfig.has("sharedDirectory")) {
        shared = backupConfig.get("sharedDirectory").asBoolean();
      }
      if (shared == null) {
        shared = false;
      }
      if (maxBackupCount != null) {
        try {
          // delete old backups
          if (type.equals("AWS")) {
            String bucket = backupConfig.get("bucket").asText();
            String prefix = backupConfig.get("prefix").asText();
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
      logger.info("Backup Master - delete old backups - finished");

      logger.info("Backup Master - finishBackup - begin");

      ComObject fcobj = new ComObject();
      fcobj.put(ComObject.Tag.dbName, "__none__");
      fcobj.put(ComObject.Tag.schemaVersion, server.getCommon().getSchemaVersion());
      fcobj.put(ComObject.Tag.method, "BackupManager:finishBackup");
      fcobj.put(ComObject.Tag.shared, shared);
      if (directory != null) {
        fcobj.put(ComObject.Tag.directory, directory);
      }
      fcobj.put(ComObject.Tag.type, type);
      fcobj.put(ComObject.Tag.maxBackupCount, maxBackupCount);
      for (int i = 0; i < server.getShardCount(); i++) {
        server.getDatabaseClient().send(null, i, masters[i], fcobj, DatabaseClient.Replica.specified);
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

  private void doDeleteAWSBackups(String bucket, String prefix, Integer maxBackupCount) {
    List<String> dirs = server.getAWSClient().listDirectSubdirectories(bucket, prefix);
    Collections.sort(dirs, new Comparator<String>() {
      @Override
      public int compare(String o1, String o2) {
        return -1 * o1.compareTo(o2);
      }
    });
    for (int i = 0; i < dirs.size(); i++) {
      if (i >= maxBackupCount) {
        try {
          server.getAWSClient().deleteDirectory(bucket, prefix + "/" + dirs.get(i));
        }
        catch (Exception e) {
          logger.error("Error deleting backup from AWS: dir=" + prefix + "/" + dirs.get(i), e);
        }
      }
    }
  }

  public ComObject prepareForRestore(ComObject cobj, boolean replayedCommand) {
    try {
      isRestoreComplete = false;

      server.purgeMemory();

      //isRunning.set(false);
      server.getDeltaManager().enableSnapshot(false);
      Thread.sleep(5000);
      //snapshotManager.deleteSnapshots();
      server.getDeltaManager().deleteSnapshots();

      File file = new File(server.getDataDir(), "result-sets/" + server.getShard() + "/" + server.getReplica());
      FileUtils.deleteDirectory(file);

      server.getLogManager().deleteLogs();

      synchronized (server.getCommon()) {
        server.getClient().syncSchema();
        server.getCommon().saveSchema(server.getClient(), server.getDataDir());
      }

      return null;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public ComObject doRestoreFileSystem(final ComObject cobj, boolean replayedCommand) {

    restoreFileSystemThread = ThreadUtil.createThread(new Runnable() {
      @Override
      public void run() {
        try {
          finishedRestoreFileCopy = false;
          restoreException = null;

          String directory = cobj.getString(ComObject.Tag.directory);
          String subDirectory = cobj.getString(ComObject.Tag.subDirectory);

          backupFileSystemDir = directory;
          lastBackupDir = subDirectory;
          directory = directory.replace("$HOME", System.getProperty("user.home"));

          restoreFileSystemSingleDir(directory, subDirectory, "logSequenceNum");
          restoreFileSystemSingleDir(directory, subDirectory, "nextRecordId");
          server.getDeleteManager().restoreFileSystem(directory, subDirectory);
          server.getLongRunningCommands().restoreFileSystem(directory, subDirectory);
          //snapshotManager.restoreFileSystem(directory, subDirectory);
          server.getDeltaManager().restoreFileSystem(directory, subDirectory);

//          File file = new File(directory, subDirectory);
//          file = new File(file, "snapshot/" + getShard() + "/0/schema.bin");
//          BufferedInputStream in = new BufferedInputStream(new FileInputStream(file));
//          ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
//          IOUtils.copy(in, bytesOut);
          // bytesOut.close();
//
//          synchronized (common) {
//            common.deserializeSchema(bytesOut.toByteArray());
//            common.saveSchema(getClient(), getDataDir());
//          }
          server.getLogManager().restoreFileSystem(directory, subDirectory);

          finishedRestoreFileCopy = true;

          prepareDataFromRestore();

          //deleteManager.forceDeletes();

//          synchronized (common) {
//            if (shard != 0 || common.getServersConfig().getShards()[0].masterReplica != replica) {
//              long schemaVersion = common.getSchemaVersion() + 100;
//              common.setSchemaVersion(schemaVersion);
//            }
//            common.saveSchema(getClient(), getDataDir());
//          }
//          pushSchema();

          isRestoreComplete = true;
        }
        catch (Exception e) {
          logger.error("Error restoring backup", e);
          restoreException = e;
        }
      }
    }, "SonicBase Restore FileSystem Thread");
    restoreFileSystemThread.start();

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
    restoreAWSThread = ThreadUtil.createThread(new Runnable() {
      @Override
      public void run() {
        try {
          restoreException = null;
          finishedRestoreFileCopy = false;
          //synchronized (restoreAwsMutex) {
          String subDirectory = cobj.getString(ComObject.Tag.subDirectory);
          String bucket = cobj.getString(ComObject.Tag.bucket);
          String prefix = cobj.getString(ComObject.Tag.prefix);

          lastBackupDir = subDirectory;
          restoreAWSSingleDir(bucket, prefix, subDirectory, "logSequenceNum");
          restoreAWSSingleDir(bucket, prefix, subDirectory, "nextRecordId");
          server.getDeleteManager().restoreAWS(bucket, prefix, subDirectory);
          server.getLongRunningCommands().restoreAWS(bucket, prefix, subDirectory);
          //snapshotManager.restoreAWS(bucket, prefix, subDirectory);
          server.getDeltaManager().restoreAWS(bucket, prefix, subDirectory);
          server.getLogManager().restoreAWS(bucket, prefix, subDirectory);

          finishedRestoreFileCopy = true;

          prepareDataFromRestore();

          isRestoreComplete = true;
          //}
        }
        catch (Exception e) {
          logger.error("Error restoring backup", e);
          restoreException = e;
        }
      }
    }, "SonicBase Restore AWS Thread");
    restoreAWSThread.start();

    return null;
  }

  private void restoreAWSSingleDir(String bucket, String prefix, String subDirectory, String dirName) {
    try {
      AWSClient awsClient = server.getAWSClient();
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
//      if (restoreException != null) {
//        throw new DatabaseException(restoreException);
//      }
      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.isComplete, isRestoreComplete);
      return retObj;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }


  public ComObject finishRestore(ComObject cobj, boolean replayedCommand) {
    try {
      server.getDeltaManager().enableSnapshot(true);
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
      server.getDeltaManager().recoverFromSnapshot(dbName);
    }
    server.getLogManager().applyLogs();
  }

  public ComObject isEntireRestoreComplete(ComObject cobj, boolean replayedCommand) {
    try {
      if (finalRestoreException != null) {
        throw new DatabaseException("Error restoring backup", finalRestoreException);
      }
      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.isComplete, !doingRestore);

      return retObj;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public ComObject startRestore(final ComObject cobj, boolean replayedCommand) {
    if (!server.getCommon().haveProLicense()) {
      throw new InsufficientLicense("You must have a pro license to start a restore");
    }
    doingRestore = true;
    restoreMainThread = ThreadUtil.createThread(new Runnable() {
      @Override
      public void run() {
        try {
          String directory = cobj.getString(ComObject.Tag.directory);

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
      server.getDeltaManager().shutdown();

      finalRestoreException = null;
      doingRestore = true;

      ObjectNode backup = (ObjectNode) server.getConfig().get("backup");
      if (backupConfig == null) {
        backupConfig = backup;
      }
      String type = backupConfig.get("type").asText();

      if (type.equals("AWS")) {
        String bucket = backupConfig.get("bucket").asText();
        String prefix = backupConfig.get("prefix").asText();

        String key = null;
        if (USE_SNAPSHOT_MGR_OLD) {
          key = prefix + "/" + subDirectory + "/snapshot/" + server.getShard() + "/0/schema.bin";
        }
        else {
          key = prefix + "/" + subDirectory + "/delta/" + server.getShard() + "/0/schema.bin";
        }
        byte[] bytes = server.getAWSClient().downloadBytes(bucket, key);
        synchronized (server.getCommon()) {
          server.getCommon().deserializeSchema(bytes);
          server.getCommon().saveSchema(server.getClient(), server.getDataDir());
        }
      }
      else if (type.equals("fileSystem")) {
        String currDirectory = backupConfig.get("directory").asText();
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
          server.getCommon().saveSchema(server.getClient(), server.getDataDir());
        }
      }
      //pushSchema();

      //common.clearSchema();

      // delete snapshots and logs
      // enter recovery mode (block commands)
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.dbName, "__none__");
      cobj.put(ComObject.Tag.schemaVersion, server.getCommon().getSchemaVersion());
      cobj.put(ComObject.Tag.method, "BackupManager:prepareForRestore");
      byte[][] ret = server.getDatabaseClient().sendToAllShards(null, 0, cobj, DatabaseClient.Replica.all, true);



      if (type.equals("AWS")) {
        // if aws
        //    tell all servers to upload with a specific root directory

        String bucket = backupConfig.get("bucket").asText();
        String prefix = backupConfig.get("prefix").asText();
        cobj = new ComObject();
        cobj.put(ComObject.Tag.dbName, "__none__");
        cobj.put(ComObject.Tag.schemaVersion, server.getCommon().getSchemaVersion());
        cobj.put(ComObject.Tag.method, "BackupManager:doRestoreAWS");
        cobj.put(ComObject.Tag.bucket, bucket);
        cobj.put(ComObject.Tag.prefix, prefix);
        cobj.put(ComObject.Tag.subDirectory, subDirectory);
        ret = server.getDatabaseClient().sendToAllShards(null, 0, cobj, DatabaseClient.Replica.all, true);
      }
      else if (type.equals("fileSystem")) {
        // if fileSystem
        //    tell all servers to copy files to backup directory with a specific root directory
        String directory = backupConfig.get("directory").asText();
        cobj = new ComObject();
        cobj.put(ComObject.Tag.dbName, "__none__");
        cobj.put(ComObject.Tag.schemaVersion, server.getCommon().getSchemaVersion());
        cobj.put(ComObject.Tag.method, "BackupManager:doRestoreFileSystem");
        cobj.put(ComObject.Tag.directory, directory);
        cobj.put(ComObject.Tag.subDirectory, subDirectory);
        ret = server.getDatabaseClient().sendToAllShards(null, 0, cobj, DatabaseClient.Replica.all, true);
      }

      while (!server.getShutdown()) {
        cobj = new ComObject();
        cobj.put(ComObject.Tag.dbName, "__none__");
        cobj.put(ComObject.Tag.schemaVersion, server.getCommon().getSchemaVersion());
        cobj.put(ComObject.Tag.method, "BackupManager:isRestoreComplete");

        boolean finished = false;
        outer:
        for (int shard = 0; shard < server.getShardCount(); shard++) {
          for (int replica = 0; replica < server.getReplicationFactor(); replica++) {
            try {
              byte[] currRet = server.getDatabaseClient().send(null, shard, replica, cobj, DatabaseClient.Replica.specified, true);
              ComObject retObj = new ComObject(currRet);
              finished = retObj.getBoolean(ComObject.Tag.isComplete);
              if (!finished) {
                break outer;
              }
            }
            catch (Exception e) {
              logger.error("Error checking if restore is complete", e);
              finished = true;
              throw e;
            }
          }
        }
        if (finished) {
          break;
        }
        Thread.sleep(2000);
      }

      synchronized (server.getCommon()) {
        if (server.getShard() != 0 || server.getCommon().getServersConfig().getShards()[0].getMasterReplica() != server.getReplica()) {
          int schemaVersion = server.getCommon().getSchemaVersion() + 100;
          server.getCommon().setSchemaVersion(schemaVersion);
        }
        server.getCommon().saveSchema(server.getClient(), server.getDataDir());
      }
      server.pushSchema();

      cobj = new ComObject();
      cobj.put(ComObject.Tag.dbName, "__none__");
      cobj.put(ComObject.Tag.schemaVersion, server.getCommon().getSchemaVersion());
      cobj.put(ComObject.Tag.method, "BackupManager:finishRestore");

      ret = server.getDatabaseClient().sendToAllShards(null, 0, cobj, DatabaseClient.Replica.all, true);

      for (String dbName : server.getDbNames(server.getDataDir())) {
        server.getClient().beginRebalance(dbName, "persons", "_1__primarykey");

        while (!server.getShutdown()) {
          if (server.getClient().isRepartitioningComplete(dbName)) {
            break;
          }
          Thread.sleep(1000);
        }
      }
      cobj = new ComObject();
      cobj.put(ComObject.Tag.dbName, "test");
      cobj.put(ComObject.Tag.schemaVersion, server.getClient().getCommon().getSchemaVersion());
      cobj.put(ComObject.Tag.method, "DeleteManager:forceDeletes");
      server.getClient().sendToAllShards(null, 0, cobj, DatabaseClient.Replica.all);

    }
    catch (Exception e) {
      finalRestoreException = e;
      logger.error("Error restoring backup", e);
      throw new DatabaseException(e);
    }
    finally {
      doingRestore = false;
      server.startRepartitioner();
      server.getDeltaManager().runSnapshotLoop();
    }
  }
}
