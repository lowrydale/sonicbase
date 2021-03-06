package com.sonicbase.server;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.util.Varint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

@SuppressWarnings({"squid:S1172", "squid:S1168", "squid:S00107"})
// all methods called from method invoker must have cobj and replayed command parms
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class LongRunningCalls {

  private static final Logger logger = LoggerFactory.getLogger(LongRunningCalls.class);

  private final com.sonicbase.server.DatabaseServer server;
  private final ConcurrentLinkedQueue<Thread> executionThreads = new ConcurrentLinkedQueue<>();
  private byte[] bytesForEmbedded;

  LongRunningCalls(DatabaseServer server) {
    this.server = server;
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
        if (server.isNotDurable()) {
          if (bytesForEmbedded == null) {
            return;
          }
          try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytesForEmbedded))) {
            deserialize(in);
          }
        }
        else {
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
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
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

  public void save() {
    try {
      synchronized (this) {
        if (server.isNotDurable()) {
          ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
          try (DataOutputStream out = new DataOutputStream(bytesOut)) {
            serialize(out);
          }
          bytesForEmbedded = bytesOut.toByteArray();
        }
        else {
          File file = getReplicaRoot();
          file.mkdirs();
          int version = getHighestSafeSnapshotVersion(file);
          version++;
          file = new File(file, String.valueOf(version) + ".in-process");
          if (file.exists()) {
            deleteFile(file);
          }

          try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)))) {
            serialize(out);
          }

          File newFile = new File(server.getDataDir(), "lrc" + File.separator + server.getShard() + File.separator +
              server.getReplica() + File.separator + version);
          if (!file.renameTo(newFile)) {
            logger.error("Error renaming file: oldPath={}, newPath={}", file.getAbsolutePath(), newFile.getAbsolutePath());
          }

          deleteOldFiles();
        }
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private void deleteFile(File file) {
    if (server.isNotDurable()) {
      return;
    }
    try {
      Files.delete(file.toPath());
    }
    catch (IOException e) {
      logger.error("Error deleting file: path={}", file.getAbsolutePath());
    }
  }

  private void deleteOldFiles() {
    if (server.isNotDurable()) {
      return;
    }
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
        logger.info("Deleting file: path={}", currFile.getAbsolutePath());
        if (currFile.exists()) {
          deleteFile(currFile);
        }
      }
    }
  }

  public File getReplicaRoot() {
    return new File(server.getDataDir(), "lrc" + File.separator + server.getShard() + File.separator + server.getReplica());
  }

  private int getHighestSafeSnapshotVersion(File dataRootDir) {
    int highestSnapshot = -1;
    try {
      String[] dirs = dataRootDir.list();
      if (dirs != null) {
        for (String dir : dirs) {
          int pos = dir.indexOf('.');
          if (pos == -1) {
            int value = Integer.parseInt(dir);
            if (value > highestSnapshot) {
              highestSnapshot = value;
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

  public class SingleCommand {
    final LongRunningCalls longRunningCommands;
    byte[] body;

    SingleCommand(LongRunningCalls longRunningCommands) {
      this.longRunningCommands = longRunningCommands;
    }

    SingleCommand(LongRunningCalls longRunningCommands, byte[] body) {
      this.longRunningCommands = longRunningCommands;
      this.body = body;
    }

    public void serialize(DataOutputStream out) throws IOException {
      Varint.writeSignedVarLong(DatabaseClient.SERIALIZATION_VERSION, out);
      Varint.writeSignedVarLong(body.length, out);
      out.write(body);
    }

    void deserialize(DataInputStream in) throws IOException {
      Varint.readSignedVarLong(in); //serialization version
      int len = (int)Varint.readSignedVarLong(in);
      body = new byte[len];
      in.readFully(body);
    }

    public void execute(final Queue<SingleCommand> parentList) {
      Thread thread = new Thread(() -> {
        try {
          doExecute(parentList);
        }
        finally {
          executionThreads.remove(Thread.currentThread());
        }
      });
      executionThreads.add(thread);
      thread.start();
    }

    private void doExecute(Queue<SingleCommand> parentList) {
      longRunningCommands.server.invokeMethod(null, body, false, false);
      synchronized (parentList) {
        parentList.remove(SingleCommand.this);
      }
      longRunningCommands.save();
    }
  }

  static final Map<Integer, Type> lookupTypeById = new HashMap<>();
  enum Type {
    SINGLE(0),
    COMPOUND(1);

    private final int value;

    Type(int i) {
      this.value = i;
      lookupTypeById.put(i, this);
    }
  }

  private final ConcurrentLinkedQueue<SingleCommand> commands = new ConcurrentLinkedQueue<>();

  public void serialize(DataOutputStream out) throws IOException {
    synchronized (commands) {
      Varint.writeSignedVarLong(DatabaseClient.SERIALIZATION_VERSION, out);
      Varint.writeSignedVarLong(commands.size(), out);
      for (SingleCommand command : commands) {
        command.serialize(out);
      }
    }
  }

  private void deserialize(DataInputStream in) throws IOException {
    synchronized (commands) {
      commands.clear();
      Varint.readSignedVarLong(in); //serialization version
      long count = Varint.readSignedVarLong(in);
      for (int i = 0; i < count; i++) {
        SingleCommand command = new SingleCommand(this);
        command.deserialize(in);
        commands.add(command);
      }
    }
  }

}
