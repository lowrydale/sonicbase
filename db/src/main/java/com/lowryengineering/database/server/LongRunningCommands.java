package com.lowryengineering.database.server;

import com.lowryengineering.database.query.DatabaseException;
import com.lowryengineering.database.util.DataUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Responsible for
 */
public class LongRunningCommands {

  private static Logger logger = LoggerFactory.getLogger(LongRunningCommands.class);

  private final DatabaseServer server;

  public LongRunningCommands(DatabaseServer server) {
    this.server = server;
    commands = new CompoundCommand(this);
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
    synchronized (commands.commands) {
      for (Command command : commands.commands) {
        command.execute(commands.commands);
      }
    }
  }

  public void addCommand(Command command) {
    synchronized (commands.commands) {
      commands.commands.add(command);
    }
    save();
    command.execute(commands.commands);
  }

  public int getCommandCount() {
    return commands.commands.size();
  }

  public interface Command {
    void serialize(DataOutputStream out) throws IOException;
    void deserialize(DataInputStream in) throws IOException;

    void execute(List<Command> parentList);
    void executeBlocking(List<Command> parentList);
  }

  public static class CompoundCommand implements Command {
    private final LongRunningCommands longRunningCommands;
    List<Command> commands = new ArrayList<>();
    CompoundCommand(LongRunningCommands longRunningCommands) {
      this.longRunningCommands = longRunningCommands;
    }
    public void serialize(DataOutputStream out) throws IOException {
      DataUtil.writeVLong(out, commands.size());
      for (Command command : commands) {
        serializeType(out, command);
        command.serialize(out);
      }
    }

    public void deserialize(DataInputStream in) throws IOException {
      commands.clear();
      int count = (int)DataUtil.readVLong(in);
      for (int i = 0; i < count; i++) {
        Command command = createType(longRunningCommands, in);
        command.deserialize(in);
        commands.add(command);
      }
    }

    @Override
    public void execute(final List<Command> parentList) {
      Thread thread = new Thread(new Runnable(){
        @Override
        public void run() {
          doExecute(parentList);
        }
      });
      thread.start();
    }

    private void doExecute(List<Command> parentList) {
      while (true) {
        Command command = null;
        synchronized (commands) {
          if (commands.size() == 0) {
            break;
          }
          command = commands.get(0);
        }
        command.executeBlocking(commands);
      }
      synchronized (parentList) {
        parentList.remove(CompoundCommand.this);
      }
      longRunningCommands.save();
    }

    @Override
    public void executeBlocking(List<Command> parentList) {
      doExecute(parentList);
    }

    public void addCommand(Command singleCommand) {
      synchronized (commands) {
        commands.add(singleCommand);
      }
    }
  }

  public static class SingleCommand implements Command {
    private final LongRunningCommands longRunningCommands;
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

    @Override
    public void execute(final List<Command> parentList) {
      Thread thread = new Thread(new Runnable(){
        @Override
        public void run() {
          doExecute(parentList);
        }
      });
      thread.start();
    }

    @Override
    public void executeBlocking(final List<Command> parentList) {
      doExecute(parentList);
    }

    private void doExecute(List<Command> parentList) {
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

  private CompoundCommand commands;

  public CompoundCommand createCompoundCommand() {
    return new CompoundCommand(this);
  }
  public SingleCommand createSingleCommand(String command, byte[] body) {
    return new SingleCommand(this, command, body);
  }

  public void serialize(DataOutputStream out) throws IOException {
    synchronized (commands) {
      commands.serialize(out);
    }
  }

  private static void serializeType(DataOutputStream out, Command command) throws IOException {
    if (command instanceof SingleCommand) {
      out.write(Type.single.value);
    }
    else {
      out.write(Type.compound.value);
    }
  }

  public void deserialize(DataInputStream in) throws IOException {
    synchronized (commands) {
      commands.deserialize(in);
    }
  }

  private static Command createType(LongRunningCommands longRunningCommands, DataInputStream in) throws IOException {
    Command command;Type type = lookupTypeById.get(in.read());
    if (type == Type.single) {
      command = new SingleCommand(longRunningCommands);
    }
    else {
      command = new CompoundCommand(longRunningCommands);
    }
    return command;
  }

}
