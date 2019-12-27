/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.cli;

import com.sonicbase.common.ComObject;
import com.sonicbase.common.Config;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.schema.TableSchema;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

class MiscHandler {

  private static final Logger logger = LoggerFactory.getLogger(MiscHandler.class);
  private static final String CONFIG_LICENSE_SERVER_STR = "/config-license-server";
  private static final String USERS_LOWRYDA_SONICBASE_CONFIG_CONFIG_LICENSE_SERVER_STR = "/Users/lowryda/sonicbase/config/config-license-server";
  private static final String UTF_8_STR = "utf-8";
  private static final String SERVER_STR = "server";
  private static final String USER_DIR_STR = "user.dir";
  private static final String CONFIG_STR = "/config-";
  private static final String LOGS_STR = "/logs/";
  private static final String ADDRESS_STR = "address";
  private static final String JSON_STR = ".json";
  private static final String LOCALHOST_NUMS_STR = "127.0.0.1";
  private static final String LOCALHOST_STR = "localhost";
  private static final String USER_KNOWN_HOSTS_FILE_DEV_NULL_STR = "UserKnownHostsFile=/dev/null";
  private static final String LICENSE_STR = "license-";
  private static final String STRICT_HOST_KEY_CHECKING_NO_STR = "StrictHostKeyChecking=no";
  private static final String PORT_STR = ", port=";
  private final Cli cli;

  MiscHandler(Cli cli) {
    this.cli = cli;
  }

  void traverseIndex() {
    ComObject cobj = new ComObject(1);
    cli.getConn().sendToAllShards("ReadManager:traverseIndex", 0, cobj, ConnectionProxy.Replica.ALL);
  }

  void stopStreaming() {
    ComObject cobj = new ComObject(1);
    for (int shard = 0; shard < cli.getConn().getShardCount(); shard++) {
      cli.getConn().send("StreamManager:stopStreaming", shard, 0, cobj, ConnectionProxy.Replica.ALL);
    }
  }

  void startStreaming() {
    ComObject cobj = new ComObject(1);
    for (int shard = 0; shard < cli.getConn().getShardCount(); shard++) {
      cli.getConn().send("StreamManager:startStreaming", shard, 0, cobj, ConnectionProxy.Replica.ALL);
    }
  }

  void explain(String command) throws SQLException, ClassNotFoundException, IOException, InterruptedException {
    if (cli.getCurrDbName() == null) {
      cli.println("Error, not using a database");
      return;
    }

    cli.initConnection();

    cli.println("Executing explain request");

    PreparedStatement stmt = cli.getConn().prepareStatement(command);
    cli.setRet(stmt.executeQuery());

    String str = cli.getTerminalSize();
    String[] parts = str.split(",");
    String height = parts[1].trim();

    int currLine = 0;
    cli.println("");
    for (int i = 0; currLine < Integer.valueOf(height) - 2; i++, currLine++) {
      if (!cli.getRet().next()) {
        break;
      }
      cli.println(cli.getRet().getString(1));
    }
    cli.setLastCommand(command);
  }

  private Config getLicenseServerConfig() throws IOException {

    InputStream in = Cli.class.getResourceAsStream(CONFIG_LICENSE_SERVER_STR + ".yaml");
    if (in == null) {
      in = new FileInputStream(USERS_LOWRYDA_SONICBASE_CONFIG_CONFIG_LICENSE_SERVER_STR + ".yaml");
    }

    String json = IOUtils.toString(in, UTF_8_STR);
    return new Config(json);
  }


  void gatherDiagnostics() throws IOException, InterruptedException, SQLException, ClassNotFoundException, ExecutionException {
    if (cli.getCurrDbName() == null) {
      cli.println("Error, not using a database");
      return;
    }

    cli.initConnection();

    Config config = cli.getConfig();
    String dataDir = config.getString("dataDirectory");
    dataDir = cli.resolvePath(dataDir);

    final File dir = new File(System.getProperty(USER_DIR_STR), "tmp/diag");
    FileUtils.deleteDirectory(dir);
    dir.mkdirs();

    cli.println("Output dir=" + dir.getAbsolutePath());

    File srcConfig = new File("config", "config.yaml");
    File destConfig = new File(dir, "config.yaml");

    FileUtils.copyFile(srcConfig, destConfig);

    try {
      cli.doGetOSStats(cli.getConn());
      File file = new File(dir, "os-stats.csv");
      try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))) {
        writer.write("host,cpu,resGig,javaMemMin,javaMemMax,recRate,trasnRate\n");
        for (List<String> line : cli.getServerStatsData()) {
          boolean first = true;
          for (String item : line) {
            if (first) {
              first = false;
            }
            else {
              writer.write(",");
            }
            writer.write(item);
          }
          writer.write("\n");
        }
      }
    }
    catch (Exception e) {
      cli.println("Error gathering OS stats");
      cli.printException(e);
    }

    try {
      int masterReplica = cli.getConn().getMasterReplica(0);
      cli.getFile(config, dir, 0, masterReplica, LOGS_STR + "errors.log");
      for (int i = 1; i < 11; i++) {
        cli.getFile(config, dir, 0, masterReplica, LOGS_STR + "errors.log." + i);
      }

      cli.getFile(config, dir, 0, masterReplica, LOGS_STR + "client-errors.log");
      for (int i = 1; i < 11; i++) {
        cli.getFile(config, dir, 0, masterReplica, LOGS_STR + "client-errors.log." + i);
      }
    }
    catch (Exception e) {
      logger.error("Error getting error files from master", e);
    }

    try {

      Config lconfig = getLicenseServerConfig();

      String address = lconfig.getString(ADDRESS_STR);
      int port = lconfig.getInt("port");

      cli.getFile(lconfig, dir, address, LOGS_STR + LICENSE_STR + port + ".log");
      for (int i = 1; i < 11; i++) {
        cli.getFile(lconfig, dir, address, LOGS_STR + LICENSE_STR + port + ".log." + i);
      }

      cli.getFile(lconfig, dir, address, LOGS_STR + LICENSE_STR + port + ".sysout.log");
      for (int i = 1; i < 11; i++) {
        cli.getFile(lconfig, dir, address, LOGS_STR + LICENSE_STR + port + ".sysout.log." + i);
      }
    }
    catch (Exception e) {
      logger.error("Error getting log files from license server", e);
    }

    try {
      cli.getFile(config, dir, LOCALHOST_STR, LOGS_STR + "cli.out");
    }
    catch (Exception e) {
      logger.error("Error getting cli log file");
    }

    Set<String> machinesVisited = new HashSet<>();
    if (!cli.isWindows()) {
      List<Config.Shard> shards = config.getShards();
      List<Future> futures = new ArrayList<>();
      for (int i = 0; i < shards.size(); i++) {
        final List<Config.Replica> replicas = shards.get(i).getReplicas();
        for (int j = 0; j < replicas.size(); j++) {
          final boolean first;
          first = machinesVisited.add(replicas.get(j).getString("address"));
          try {
            gatherDiagnostics(config, dir, i, j, replicas.get(j), first);
          }
          catch (Exception e) {
            logger.error("Error gathering diagnostics from server: shard={}, replica={}", i, j);
          }
        }
      }
      for (Future future : futures) {
        future.get();
      }
    }

    File shardsFile = new File(dir, "shards.txt");
    try (final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(shardsFile)))) {
      PreparedStatement stmt = cli.getConn().prepareStatement("describe shards");
      cli.setRet(stmt.executeQuery());

      while (cli.getRet().next()) {
        writer.write(cli.getRet().getString(1) + "\n");
      }
    }

    File versionFile = new File(dir, "schemaVersion.txt");
    try (final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(versionFile)))) {
      PreparedStatement stmt = cli.getConn().prepareStatement("describe schema version");
      cli.setRet(stmt.executeQuery());

      writer.write("host,shard,replica,version\n");
      while (cli.getRet().next()) {

        String builder = cli.getRet().getString("host") +
            "," + cli.getRet().getString("shard") +
            "," + cli.getRet().getString("replica") +
            "," + cli.getRet().getString("version");
        writer.write(builder + "\n");
      }
    }

    File licensesFile = new File(dir, "licenses.txt");
    try (final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(licensesFile)))) {
      PreparedStatement stmt = cli.getConn().prepareStatement("describe licenses");
      cli.setRet(stmt.executeQuery());

      while (cli.getRet().next()) {
        writer.write(cli.getRet().getString(1) + "\n");
      }
    }

    File statsFile = new File(dir, "server-stats.txt");
    try (final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(statsFile)))) {
      PreparedStatement stmt = cli.getConn().prepareStatement("describe server stats");
      cli.setRet(stmt.executeQuery());

      writer.write("host,cpu,resGig,javaMemMin,javaMemMax,receive,transmit,diskAvail\n");
      while (cli.getRet().next()) {
        writer.write(cli.getRet().getString("host") + "," +
            cli.getRet().getString("cpu") + "," + cli.getRet().getString("resGig") + "," +
            cli.getRet().getString("javaMemMin") + "," + cli.getRet().getString("javaMemMax") + "," +
            cli.getRet().getString("receive") + "," + cli.getRet().getString("transmit") + "," +
            cli.getRet().getString("diskAvail") + "\n");
      }
    }

    File tablesDir = new File(dir, "tables");
    tablesDir.mkdirs();

    Map<String, TableSchema> tables = cli.getConn().getTables(cli.getCurrDbName());
    for (TableSchema table : tables.values()) {
      File tableFile = new File(tablesDir, table.getName() + ".txt");
      try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tableFile)))) {
        PreparedStatement stmt = cli.getConn().prepareStatement("describe table " + table.getName());
        cli.setRet(stmt.executeQuery());
        while (cli.getRet().next()) {
          writer.write(cli.getRet().getString(1) + "\n");
        }
      }
    }

    try (PreparedStatement stmt = cli.getConn().prepareStatement("describe server health")) {
      try (ResultSet rs = stmt.executeQuery()) {

        StringBuilder builder = new StringBuilder();
        builder.append("host,shard,replica,dead,master\n");
        while (rs.next()) {
          builder.append(rs.getString("host")).append(",");
          builder.append(rs.getString("shard")).append(",");
          builder.append(rs.getString("replica")).append(",");
          builder.append(rs.getString("dead")).append(",");
          builder.append(rs.getString("master")).append(",");
          builder.append("\n");
        }
        File healthFile = new File(dir, "health.csv");
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(healthFile)))) {
          writer.write(builder.toString());
        }
      }
    }

    if (cli.isWindows()) {
      cli.println("Diagnostics file can be found at: tmp/*. Zip the contents and send the zip file");
    }
    else {
      ProcessBuilder pbuilder = new ProcessBuilder().command("tar", "-czf", "tmp/diag.tgz", "tmp/diag");
      Process p = pbuilder.start();
      p.waitFor();
      cli.println("Diagnostics file can be found at: " + System.getProperty(USER_DIR_STR) + "/tmp/diag.tgz");
    }
  }

  private void gatherDiagnostics(Config config, File dir, int shard, int replica, Config.Replica replicaDict, boolean first) throws IOException, InterruptedException {
//    final String user = config.getString("user");
//    final String address = replicaDict.getString("address");
//    File machineDir = new File(dir, address);
//
//    machineDir.mkdirs();
//
//    final AtomicBoolean finished = new AtomicBoolean();
//    if (!cli.isWindows()) {
//      if (address.equals(LOCALHOST_STR) || address.equals(LOCALHOST_NUMS_STR)) {
//        if (first) {
//          ProcessBuilder builder = new ProcessBuilder().command("killall", "-QUIT", "java");
//          Process p = builder.start();
//          p.waitFor();
//
//          builder = new ProcessBuilder().command("bash", "bin/get-distribution", installDir);
//          p = builder.start();
//          p.waitFor();
//
//          builder = new ProcessBuilder().command("bash", "bin/get-df", installDir);
//          p = builder.start();
//          p.waitFor();
//
//          builder = new ProcessBuilder().command("bash", "bin/get-dir", installDir);
//          p = builder.start();
//          p.waitFor();
//
//          builder = new ProcessBuilder().command("bash", "bin/get-top", installDir);
//          p = builder.start();
//          p.waitFor();
//
//          builder = new ProcessBuilder().command("bash", "bin/get-jarlist", installDir);
//          p = builder.start();
//          p.waitFor();
//
//          finished.set(true);
//        }
//      }
//      else {
//        if (first) {
//          Thread thread = new Thread(() -> {
//            try {
//              ProcessBuilder builder = new ProcessBuilder().command("ssh", "-n", "-f", "-o",
//                  USER_KNOWN_HOSTS_FILE_DEV_NULL_STR, "-o", STRICT_HOST_KEY_CHECKING_NO_STR, user + "@" +
//                      address, "killall -QUIT java");
//              Process p = builder.start();
//              p.waitFor();
//
//              builder = new ProcessBuilder().command("ssh", "-n", "-f", "-o",
//                  USER_KNOWN_HOSTS_FILE_DEV_NULL_STR, "-o", STRICT_HOST_KEY_CHECKING_NO_STR, user + "@" +
//                      address, installDir + "/bin/get-distribution", installDir);
//              p = builder.start();
//              p.waitFor();
//
//              builder = new ProcessBuilder().command("ssh", "-n", "-f", "-o",
//                  USER_KNOWN_HOSTS_FILE_DEV_NULL_STR, "-o", STRICT_HOST_KEY_CHECKING_NO_STR, user + "@" +
//                      address, installDir + "/bin/get-df", installDir);
//              p = builder.start();
//              p.waitFor();
//
//              builder = new ProcessBuilder().command("ssh", "-n", "-f", "-o",
//                  USER_KNOWN_HOSTS_FILE_DEV_NULL_STR, "-o", STRICT_HOST_KEY_CHECKING_NO_STR, user + "@" +
//                      address, installDir + "/bin/get-dir", installDir);
//              p = builder.start();
//              p.waitFor();
//
//              builder = new ProcessBuilder().command("ssh", "-n", "-f", "-o",
//                  USER_KNOWN_HOSTS_FILE_DEV_NULL_STR, "-o", STRICT_HOST_KEY_CHECKING_NO_STR, user + "@" +
//                      address, installDir + "/bin/get-top", installDir);
//              p = builder.start();
//              p.waitFor();
//
//              builder = new ProcessBuilder().command("ssh", "-n", "-f", "-o",
//                  USER_KNOWN_HOSTS_FILE_DEV_NULL_STR, "-o", STRICT_HOST_KEY_CHECKING_NO_STR, user + "@" +
//                      address, installDir + "/bin/get-jarlist", installDir);
//              p = builder.start();
//              p.waitFor();
//            }
//            catch (Exception e) {
//              logger.error("Error gathering diagnostics: server={}", address);
//            }
//            finally {
//              finished.set(true);
//            }
//          });
//          thread.start();
//
//          thread.join(10000, 0);
//
//        }
//      }
//    }
//
//    if (finished.get()) {
//      if (!cli.isWindows() && first) {
//        cli.getFile(config, machineDir, shard, replica, "/tmp/distribution");
//        cli.getFile(config, machineDir, shard, replica, "/tmp/df");
//        cli.getFile(config, machineDir, shard, replica, "/tmp/dir");
//        cli.getFile(config, machineDir, shard, replica, "/tmp/top");
//        cli.getFile(config, machineDir, shard, replica, "/tmp/jars");
//      }
//      cli.getFile(config, machineDir, shard, replica, LOGS_STR + replicaDict.getInt("port") + ".log");
//      cli.getFile(config, machineDir, shard, replica, LOGS_STR + replicaDict.getInt("port") + ".sysout.log");
//      cli.getFile(config, machineDir, shard, replica, "/logs/gc-" + replicaDict.getInt("port") + ".log.0.current");
//    }
/*
    1.	Current log from each server
    3.	Current gc log from each server
    4.	Current sysout.log from each server
    5.	Results of top form each server
    6.	Version of software running on each server
    7.	Thread dump on each server
*/

  }
}
