package com.lowryengineering.database.cli;

import com.lowryengineering.database.query.DatabaseException;
import com.lowryengineering.database.util.JsonArray;
import com.lowryengineering.database.util.JsonDict;
import com.lowryengineering.database.util.StreamUtils;

import java.io.*;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class Cli {

  static Thread thread = null;
  static String command = "";
  static File workingDir;

  public static void main(final String[] args) throws IOException, InterruptedException {
    workingDir = new File(System.getProperty("user.dir"));

    final AtomicBoolean resized = new AtomicBoolean();
    thread = new Thread(new Runnable(){
      @Override
      public void run() {
        List<String> capture = new ArrayList<>();
        outer:
        while (true) {
          final BufferedReader stdin = new BufferedReader(
              new InputStreamReader(Channels.newInputStream((
                  new FileInputStream(FileDescriptor.in)).getChannel())));
          try {
            File commandFile = new File("bin/command.txt");
            if (commandFile.exists()) {
              command = StreamUtils.inputStreamToString(new FileInputStream(commandFile));
            }

            ProcessBuilder builder = new ProcessBuilder().command("bin/terminal-size.py");
            //builder.directory(workingDir);
            Process p = builder.start();
            p.waitFor();
            File file = new File("bin/size.txt");
            String str = StreamUtils.inputStreamToString(new FileInputStream(file));
            String[] parts = str.split(",");
            String width = parts[0];
            String height = parts[1];

            //          if (command == null || command.length() == 0) {
                        for (int i = 0; i < Integer.valueOf(height) - 2; i++) {
                          System.out.println("");
                        }
          //            }

            if (command != null && command.length() > 0) {
              runCommand(command);
            }


//            System.out.println("\n" + command);
//            for (int i = 0; i < capture.size(); i++) {
//              System.out.println(capture.get(i));
//            }
            System.out.print(">>");

            command = "";
            while (true) {

              String s = String.valueOf((char) stdin.read());//stdin.read());
              command += s;
              if (command.endsWith("\n")) {
                break;
              }

            }

            file = new File("bin/command.txt");
            file.delete();
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)));
            writer.write(command);
            writer.close();

            capture.add(command);

          }
          catch (IOException | InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    });


    if (args != null && args.length != 0 && args[0].equals("child")) {
      thread.start();
    }
    else {
      File commandFile = new File("bin/command.txt");
      commandFile.delete();

      Process childCli = startChildCli();

      ProcessBuilder builder = new ProcessBuilder().command("bin/terminal-resize.py");
      //builder.directory(workingDir);
      Process p = builder.start();

      while (true) {
        File file = new File("bin/resize.txt");
        if (file.exists()) {
          //        String content = StreamUtils.inputStreamToString(new FileInputStream(file));
          //        if (content.equals("true")) {
          Thread.sleep(500);

          resized.set(true);

          childCli.destroyForcibly();

          commandFile = new File("bin/command.txt");
          if (commandFile.exists()) {
            command = StreamUtils.inputStreamToString(new FileInputStream(commandFile));
            commandFile.delete();

            runCommand(command);
          }

          childCli = startChildCli();
          file.delete();
        }
      }
    }
  }

  private static Process startChildCli() throws IOException {
    String javaHome = System.getProperty("java.home");
    String javaBin = javaHome +
            File.separator + "bin" +
            File.separator + "java";
    String classpath = System.getProperty("java.class.path");
    String className = Cli.class.getCanonicalName();

    ProcessBuilder builder = new ProcessBuilder(
            javaBin, "-cp", classpath, className, "child", command);
    //builder.directory(workingDir);
    final Process childCli = builder.inheritIO().start();

    Thread t1 = new Thread(new Runnable(){
      @Override
      public void run() {
        try {
          while (true) {
            int b = childCli.getInputStream().read();
            if (-1 == b) {
              try {
                Thread.sleep(10);
              }
              catch (InterruptedException e1) {
                break;
              }
              continue;
            }
            System.out.print((char) b);
          }
        }
        catch (Exception e) {
          e.printStackTrace();
        }
    }});
    t1.start();

    thread = new Thread(new Runnable(){
      @Override
      public void run() {
        try {
          InputStream in = childCli.getInputStream();
          command = "";
          while (true) {

            int b = in.read();
            if (-1 == b) {
              try {
                Thread.sleep(10);
              }
              catch (InterruptedException e1) {
                break;
              }
              continue;
            }

            String s = String.valueOf((char) b);//stdin.read());
            command += s; //String.valueOf(c);
            if (command.endsWith("\n")) {
              break;
            }
          }
          File file = new File("bin/command.txt");
          file.delete();
          BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)));
          writer.write(command);
          writer.close();
          //System.exit(0);
        }
        catch (IOException e1) {
          e1.printStackTrace();
        }
        catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
    thread.start();
    return childCli;
  }

  private static void runCommand(String command) throws IOException {
    int pos = command.indexOf(" ");
    String verb = command.substring(0, pos);
    if (verb.equals("deploy")) {
      String cluster = command.substring(pos);
      cluster = cluster.trim();
      deploy(cluster);
    }
    else if (verb.equals("start")) {
      String cluster = command.substring(pos);
      cluster = cluster.trim();
      startCluster(cluster);
    }
  }

  private static void startCluster(String cluster) throws IOException {
    String json = StreamUtils.inputStreamToString(Cli.class.getResourceAsStream("/config-" + cluster + ".json"));
    JsonDict config = new JsonDict(json);
    JsonDict databaseDict = config.getDict("database");
    String installDir = databaseDict.getString("installDirectory");
    int replicationFactor = databaseDict.getInt("replicationFactor");
    installDir = getInstallDir(installDir);
    JsonArray servers = databaseDict.getArray("servers");
    if (servers.size() % replicationFactor != 0) {
      throw new DatabaseException("Server count not divisible by replication factor");
    }
    int currShard = 0;
    int currReplica = 0;
    for (int i = 0; i < servers.size(); i++) {
      JsonDict server = servers.getDict(i);
      stopServer(databaseDict, server.getString("publicAddress"), installDir);
    }
    for (int i = 0; i < servers.size(); i++) {
      JsonDict server = servers.getDict(i);
      startServer(databaseDict, server.getString("publicAddress"), server.getString("port"), installDir, currShard, currReplica);
      currReplica = currReplica++ % replicationFactor;
      if (currReplica == 0) {
        currShard = currShard++;
      }
    }
  }

  private static void startServer(JsonDict databaseDict, String externalAddress, String port, String installDir, int currShard, int currReplica) throws IOException {
    String deployUser = databaseDict.getString("user");
    String maxHeap = databaseDict.getString("maxJavaHeap");
    if (port == null) {
      port = "9010";
    }
    String searchHome = installDir;
    if (!searchHome.startsWith("/")) {
      searchHome = "$HOME/" + searchHome;
    }
    ProcessBuilder builder = new ProcessBuilder().command("bin/do-start2.sh", deployUser + "@" + externalAddress, installDir,
        String.valueOf(currShard), String.valueOf(currReplica), port, maxHeap, searchHome);
    //builder.directory(workingDir);

    System.out.println("Started server: address=" + externalAddress + ", port=" + port);
//    ProcessBuilder builder = new ProcessBuilder().command("ssh", "-n", "-f", "-o",
//        "UserKnownHostsFile=/dev/null", "-o", "StrictHostKeyChecking=no", deployUser + "@" +
//            externalAddress, "\"sh -c \'nohup bash " + installDir + "/bin/start-db-server.sh " + String.valueOf(currShard) + " " +
//            String.valueOf(currReplica) + " " + port + " " + maxHeap + " " + searchHome);

    builder.start();
  }

  private static void stopServer(JsonDict databaseDict, String externalAddress, String installDir) throws IOException {
    String deployUser = databaseDict.getString("user");
    ProcessBuilder builder = new ProcessBuilder().command("ssh", "-n", "-f", "-o",
        "UserKnownHostsFile=/dev/null", "-o", "StrictHostKeyChecking=no", deployUser + "@" +
            externalAddress, "\"sh -c \'nohup bash " + installDir + "/bin/kill-server.sh", "NettyServer");
    //builder.directory(workingDir);
    builder.start();
  }

  private static void deploy(String cluster) throws IOException {
    System.out.println("running command: " + command + ", cluster=" + cluster);

    try {
      String json = StreamUtils.inputStreamToString(Cli.class.getResourceAsStream("/config-" + cluster + ".json"));
      JsonDict config = new JsonDict(json);
      JsonDict databaseDict = config.getDict("database");
      String deployUser = databaseDict.getString("user");
      String installDir = databaseDict.getString("installDirectory");
      installDir = getInstallDir(installDir);
      Set<String> installedAddresses = new HashSet<>();
      JsonArray servers = databaseDict.getArray("servers");
      for (int i = 0; i < servers.size(); i++) {
        JsonDict server = servers.getDict(i);
        String externalAddress = server.getString("publicAddress");
        if (installedAddresses.add(externalAddress)) {
          //ProcessBuilder builder = new ProcessBuilder().command("rsync", "-rvlLt", "--delete", "-e", "'ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no'", "*", deployUser + "@" + externalAddress + ":" + installDir);
          ProcessBuilder builder = new ProcessBuilder().command("bin/do-rsync.sh", deployUser + "@" + externalAddress + ":" + installDir);
          //builder.directory(workingDir);
          Process p = builder.start();
          InputStream in = p.getInputStream();
          while (true) {
            int b = in.read();
            if (b == -1) {
              break;
            }
            System.out.write(b);
          }
          p.waitFor();
        }
      }
    }
    catch (InterruptedException e) {
      throw new DatabaseException(e);
    }
  }

  private static String getInstallDir(String installDir) {
    if (installDir.startsWith("$HOME")) {
      installDir = installDir.substring("$HOME".length());
      if (installDir.startsWith("/")) {
        installDir = installDir.substring(1);
      }
    }
    return installDir;
  }
}
