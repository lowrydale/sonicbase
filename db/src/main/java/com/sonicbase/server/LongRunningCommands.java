package com.sonicbase.server;

import com.sonicbase.common.AWSClient;
import com.sonicbase.common.Logger;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.util.DataUtil;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Responsible for
 */
public class LongRunningCommands {

  private Logger logger;

  private final DatabaseServer server;

  public LongRunningCommands(DatabaseServer server) {
    this.server = server;
    this.logger = new Logger(server.getDatabaseClient());
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

  public SingleCommand createSingleCommand(String command, byte[] body) {
    return new SingleCommand(this, command, body);
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

  public static class SingleCommand {
    final LongRunningCommands longRunningCommands;
    String command;
    byte[] body;

    SingleCommand(LongRunningCommands longRunningCommands) {
      this.longRunningCommands = longRunningCommands;
    }

    public SingleCommand(LongRunningCommands longRunningCommands, String command, byte[] body) {
      this.longRunningCommands = longRunningCommands;
      this.command = command;
      this.body = body;
    }

    public void serialize(DataOutputStream out) throws IOException {
      DataUtil.writeVLong(out, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
      out.writeUTF(command);
      if (body == null) {
        DataUtil.writeVLong(out, 0);
      }
      else {
        DataUtil.writeVLong(out, body.length);
        out.write(body);
      }
    }

    public void deserialize(DataInputStream in) throws IOException {
      short serializationVersion = (short)DataUtil.readVLong(in);
      command = in.readUTF();
      int len = (int)DataUtil.readVLong(in);
      if (len != 0) {
        body = new byte[len];
        in.readFully(body);
      }
    }

    public void execute(final ConcurrentLinkedQueue<SingleCommand> parentList) {
      Thread thread = new Thread(new Runnable(){
        @Override
        public void run() {
          doExecute(parentList);
        }
      });
      thread.start();
    }

    private void doExecute(ConcurrentLinkedQueue<SingleCommand> parentList) {
      longRunningCommands.server.handleCommand(command, body, false, false);
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
      DataUtil.writeVLong(out, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
      DataUtil.writeVLong(out, commands.size());
      for (SingleCommand command : commands) {
        command.serialize(out);
      }
    }
  }

  public void deserialize(DataInputStream in) throws IOException {
    synchronized (commands) {
      commands.clear();
      short serializationVersion = (short)DataUtil.readVLong(in);
      long count = DataUtil.readVLong(in);
      for (int i = 0; i < count; i++) {
        SingleCommand command = new SingleCommand(this);
        command.deserialize(in);
        commands.add(command);
      }
    }
  }

}
