package com.sonicbase.server;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.AWSClient;
import com.sonicbase.common.Logger;
import com.sonicbase.query.DatabaseException;
import org.apache.commons.io.FileUtils;
import org.apache.giraph.utils.Varint;

import java.io.*;
import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Responsible for
 */
public class LongRunningCalls {

  private Logger logger;

  private final DatabaseServer server;
  private ConcurrentLinkedQueue<Thread> executionThreads = new ConcurrentLinkedQueue<>();

  public LongRunningCalls(DatabaseServer server) {
    this.server = server;
    this.logger = new Logger(null/*server.getDatabaseClient()*/);
  }

  public void shutdown() {
    for (Thread thread : executionThreads) {
      try {
        thread.interrupt();
        thread.join();
      }
      catch (Exception e) {
        logger.error("Error shutting down thread", e);
      }
    }
  }

  public void load() {
    try {
      synchronized (this) {
        File file = getReplicaRoot();
        file.mkdirs();
        int version = getHighestSafeSnapshotVersion(file);
        if (version == -1) {
          return;
        }
        file = new File(file, String.valueOf(version));

        try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(file)))) {
          deserialize(in);
        }
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public void save() {
    try {
      synchronized (this) {
        File file = getReplicaRoot();
        file.mkdirs();
        int version = getHighestSafeSnapshotVersion(file);
        version++;
        file = new File(file, String.valueOf(version) + ".in-process");
        file.delete();

        try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)))) {
          serialize(out);
        }

        File newFile = new File(server.getDataDir(), "lrc/" + server.getShard() + "/" + server.getReplica() + "/" + String.valueOf(version));
        file.renameTo(newFile);

        deleteOldFiles();
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private void deleteOldFiles() throws IOException, InterruptedException, ParseException {
    File dataRootDir = getReplicaRoot();
    dataRootDir.mkdirs();
    int highestSnapshot = getHighestSafeSnapshotVersion(dataRootDir);

    for (String fileStr : dataRootDir.list()) {
      int fileNum = -1;
      try {
        fileNum = Integer.valueOf(fileStr);
      }
      catch (Exception t) {
        //expected numeric format problems
      }
      if (fileStr.contains("in-process") || (fileNum != -1 && fileNum < (highestSnapshot - 1))) {
        File currFile = new File(dataRootDir, fileStr);
        logger.info("Deleting file: " + currFile.getAbsolutePath());
        currFile.delete();
      }
    }
  }

  private File getReplicaRoot() {
    return new File(server.getDataDir(), "lrc/" + server.getShard() + "/" + server.getReplica());
  }

  private int getHighestSafeSnapshotVersion(File dataRootDir) {
    int highestSnapshot = -1;
    try {
      String[] dirs = dataRootDir.list();
      if (dirs != null) {
        for (String dir : dirs) {
          int pos = dir.indexOf('.');
          if (pos == -1) {
            try {
              int value = Integer.valueOf(dir);
              if (value > highestSnapshot) {
                highestSnapshot = value;
              }
            }
            catch (Exception t) {
              logger.error("Error parsing dir: " + dir, t);
            }
          }
        }
      }
    }
    catch (Exception t) {
      logger.error("Error getting highest snapshot version");
    }
    return highestSnapshot;
  }

  public void execute() {
    for (SingleCommand command : commands) {
      command.execute(commands);
    }
  }

  public void addCommand(SingleCommand command) {
    synchronized (commands) {
      commands.add(command);
    }
    save();
    command.execute(commands);
  }

  public int getCommandCount() {
    synchronized (commands) {
      return commands.size();
    }
  }

  public SingleCommand createSingleCommand(byte[] body) {
    return new SingleCommand(this, body);
  }

  public long getBackupLocalFileSystemSize() {
    File dir = getReplicaRoot();
    return com.sonicbase.common.FileUtils.sizeOfDirectory(dir);
  }


  public void backupFileSystem(String directory, String subDirectory) {
    try {
      File dir = getReplicaRoot();
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
      File destDir = getReplicaRoot();
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
      AWSClient awsClient = server.getAWSClient();
      File destDir = getReplicaRoot();
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
    AWSClient awsClient = server.getAWSClient();
    File srcDir = getReplicaRoot();
    subDirectory += "/lrc/" + server.getShard() + "/0";

    awsClient.uploadDirectory(bucket, prefix, subDirectory, srcDir);
  }

  public void getFiles(List<String> files) {
    File dir = getReplicaRoot();
    File[] currFiles = dir.listFiles();
    if (currFiles != null) {
      for (File file : currFiles) {
        files.add(file.getAbsolutePath());
      }
    }
  }

  public class SingleCommand {
    final LongRunningCalls longRunningCommands;
    byte[] body;

    SingleCommand(LongRunningCalls longRunningCommands) {
      this.longRunningCommands = longRunningCommands;
    }

    public SingleCommand(LongRunningCalls longRunningCommands, byte[] body) {
      this.longRunningCommands = longRunningCommands;
      this.body = body;
    }

    public void serialize(DataOutputStream out) throws IOException {
      Varint.writeSignedVarLong(DatabaseClient.SERIALIZATION_VERSION, out);
      Varint.writeSignedVarLong(body.length, out);
      out.write(body);
    }

    public void deserialize(DataInputStream in) throws IOException {
      short serializationVersion = (short)Varint.readSignedVarLong(in);
      int len = (int)Varint.readSignedVarLong(in);
      body = new byte[len];
      in.readFully(body);
    }

    public void execute(final ConcurrentLinkedQueue<SingleCommand> parentList) {
      Thread thread = new Thread(new Runnable(){
        @Override
        public void run() {
          try {
            doExecute(parentList);
          }
          finally {
            executionThreads.remove(Thread.currentThread());
          }
        }
      });
      executionThreads.add(thread);
      thread.start();
    }

    private void doExecute(ConcurrentLinkedQueue<SingleCommand> parentList) {
      longRunningCommands.server.invokeMethod(body, false, false);
      synchronized (parentList) {
        parentList.remove(SingleCommand.this);
      }
      longRunningCommands.save();
    }
  }

  static Map<Integer, Type> lookupTypeById = new HashMap<>();
  enum Type {
    single(0),
    compound(1);

    private final int value;

    Type(int i) {
      this.value = i;
      lookupTypeById.put(i, this);
    }
  }

  private ConcurrentLinkedQueue<SingleCommand> commands = new ConcurrentLinkedQueue<>();

  public void serialize(DataOutputStream out) throws IOException {
    synchronized (commands) {
      Varint.writeSignedVarLong(DatabaseClient.SERIALIZATION_VERSION, out);
      Varint.writeSignedVarLong(commands.size(), out);
      for (SingleCommand command : commands) {
        command.serialize(out);
      }
    }
  }

  public void deserialize(DataInputStream in) throws IOException {
    synchronized (commands) {
      commands.clear();
      short serializationVersion = (short)Varint.readSignedVarLong(in);
      long count = Varint.readSignedVarLong(in);
      for (int i = 0; i < count; i++) {
        SingleCommand command = new SingleCommand(this);
        command.deserialize(in);
        commands.add(command);
      }
    }
  }

}
