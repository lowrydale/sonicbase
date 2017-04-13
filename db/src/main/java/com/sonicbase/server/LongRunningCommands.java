package com.sonicbase.server;

import com.sonicbase.common.Logger;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.util.DataUtil;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        File file = new File(server.getDataDir(), "lrc/" + server.getShard() + "/" + server.getReplica());
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
        File file = new File(server.getDataDir(), "lrc/" + server.getShard() + "/" + server.getReplica());
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
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
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
    synchronized (commands) {
      for (SingleCommand command : commands) {
        command.execute(commands);
      }
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
      command = in.readUTF();
      int len = (int)DataUtil.readVLong(in);
      if (len != 0) {
        body = new byte[len];
        in.readFully(body);
      }
    }

    public void execute(final List<SingleCommand> parentList) {
      Thread thread = new Thread(new Runnable(){
        @Override
        public void run() {
          doExecute(parentList);
        }
      });
      thread.start();
    }

    public void executeBlocking(final List<SingleCommand> parentList) {
      doExecute(parentList);
    }

    private void doExecute(List<SingleCommand> parentList) {
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

  private List<SingleCommand> commands = new ArrayList<>();

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
      long serializationVersion = DataUtil.readVLong(in);
      long count = DataUtil.readVLong(in);
      for (int i = 0; i < count; i++) {
        SingleCommand command = new SingleCommand(this);
        command.deserialize(in);
        commands.add(command);
      }
    }
  }

}
