/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.common.ComObject;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.query.DatabaseException;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

public class MiscHandler {

  private static Logger logger = LoggerFactory.getLogger(MiscHandler.class);
  private final Cli cli;

  public MiscHandler(Cli cli) {
    this.cli = cli;
  }

  public void stopStreaming() {
    ComObject cobj = new ComObject();
    for (int shard = 0; shard < cli.getConn().getShardCount(); shard++) {
      cli.getConn().send("StreamManager:stopStreaming", shard, 0, cobj, ConnectionProxy.Replica.ALL);
    }
  }

  public void startStreaming() {
    ComObject cobj = new ComObject();
    for (int shard = 0; shard < cli.getConn().getShardCount(); shard++) {
      cli.getConn().send("SreamManager:stopStreaming", shard, 0, cobj, ConnectionProxy.Replica.ALL);
    }
  }

  public void explain(String command) throws SQLException, ClassNotFoundException, IOException, InterruptedException {
    String cluster = cli.getCurrCluster();
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }

    if (cli.getCurrDbName() == null) {
      System.out.println("Error, not using a database");
      return;
    }

    cli.initConnection();

    System.out.println("Executing explain request");

    PreparedStatement stmt = cli.getConn().prepareStatement(command);
    cli.setRet(stmt.executeQuery());

    String str = cli.getTerminalSize();
    String[] parts = str.split(",");
    String height = parts[1];

    int currLine = 0;
    System.out.println();
    for (int i = 0; currLine < Integer.valueOf(height) - 2; i++) {
      if (!cli.getRet().next()) {
        break;
      }
      System.out.println(cli.getRet().getString(1));
      currLine++;
    }
    cli.setLastCommand(command);
  }


  public void deployLicenseServer() throws InterruptedException, IOException {
    if (cli.isWindows()) {
      cli.getCredentials("license-server");
    }

    InputStream in = Cli.class.getResourceAsStream("/config-license-server.json");
    if (in == null) {
      in = new FileInputStream("/Users/lowryda/sonicbase/config/config-license-server.json");
    }
    String json = IOUtils.toString(in, "utf-8");
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode config = (ObjectNode) mapper.readTree(json);
    String dir = config.get("installDirectory").asText();
    final String installDir = cli.resolvePath(dir);
    ObjectNode server = (ObjectNode) config.get("server");
    String address = server.get("publicAddress").asText();
    String user = config.get("user").asText();

    System.out.println("Deploying to a server: address=" + address + ", userDir=" + System.getProperty("user.dir") +
        ", command=" + "bin/do-rsync " + user + "@" + address + ":" + installDir);
    ProcessBuilder builder = new ProcessBuilder().command("bash", "bin/do-rsync", user + "@" + address, installDir);
    Process p = builder.start();
    InputStream pin = p.getInputStream();
    while (true) {
      int b = pin.read();
      if (b == -1) {
        break;
      }
      System.out.write(b);
    }
    p.waitFor();
  }


  public void gatherDiagnostics() throws IOException, InterruptedException, SQLException, ClassNotFoundException, ExecutionException {
    String cluster = cli.getCurrCluster();
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }

    if (cli.getCurrDbName() == null) {
      System.out.println("Error, not using a database");
      return;
    }

    cli.initConnection();

    String json = IOUtils.toString(Cli.class.getResourceAsStream("/config-" + cluster + ".json"), "utf-8");
    ObjectMapper mapper = new ObjectMapper();
    final ObjectNode config = (ObjectNode) mapper.readTree(json);
    ObjectNode databaseDict = config;
    String dataDir = databaseDict.get("dataDirectory").asText();
    dataDir = cli.resolvePath(dataDir);
    String installDir = databaseDict.get("installDirectory").asText();

    installDir = cli.resolvePath(installDir);

    final File dir = new File(System.getProperty("user.dir"), "tmp/diag");
    FileUtils.deleteDirectory(dir);
    dir.mkdirs();

    System.out.println("Output dir=" + dir.getAbsolutePath());

    File srcConfig = new File("config", "config-" + cluster + ".json");
    File destConfig = new File(dir, "config-" + cluster + ".json");

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
      System.out.println("Error gathering OS stats");
      e.printStackTrace();
    }

    try {
      int masterReplica = cli.getConn().getMasterReplica(0);
      cli.getFile(config, dir, 0, masterReplica, "/logs/" + "errors.log");
      for (int i = 1; i < 11; i++) {
        cli.getFile(config, dir, 0, masterReplica, "/logs/" + "errors.log." + i);
      }

      cli.getFile(config, dir, 0, masterReplica, "/logs/" + "client-errors.log");
      for (int i = 1; i < 11; i++) {
        cli.getFile(config, dir, 0, masterReplica, "/logs/" + "client-errors.log." + i);
      }
    }
    catch (Exception e) {
      logger.error("Error getting error files from master", e);
    }

    try {

      InputStream in = null;//cli.class.getResourceAsStream("/config-license-server.json");
      if (in == null) {
        in = new FileInputStream(new File(System.getProperty("user.dir"), "config/config-license-server.json"));
      }
      String ljson = IOUtils.toString(in, "utf-8");
      ObjectNode lconfig = (ObjectNode) mapper.readTree(ljson);

      ObjectNode server = (ObjectNode) lconfig.get("server");
      String address = server.get("privateAddress").asText();
      int port = server.get("port").asInt();

      cli.getFile(lconfig, dir, address, "/logs/" + "license-" + port + ".log");
      for (int i = 1; i < 11; i++) {
        cli.getFile(lconfig, dir, address, "/logs/" + "license-" + port + ".log." + i);
      }

      cli.getFile(lconfig, dir, address, "/logs/" + "license-" + port + ".sysout.log");
      for (int i = 1; i < 11; i++) {
        cli.getFile(lconfig, dir, address, "/logs/" + "license-" + port + ".sysout.log." + i);
      }
    }
    catch (Exception e) {
      logger.error("Error getting log files from license server", e);
    }

    try {
      cli.getFile(config, dir, "localhost", "/logs/" + "cli.out");
    }
    catch (Exception e) {
      logger.error("Error getting cli log file");
    }

    Set<String> machinesVisited = new HashSet<>();
    if (!cli.isWindows()) {
      ArrayNode shards = databaseDict.withArray("shards");
      List<Future> futures = new ArrayList<>();
      for (int i = 0; i < shards.size(); i++) {
        final ArrayNode replicas = (ArrayNode) shards.get(i).withArray("replicas");
        for (int j = 0; j < replicas.size(); j++) {
          final int replicaOffset = j;
          final boolean first;
          if (machinesVisited.add(replicas.get(replicaOffset).get("publicAddress").asText())) {
            first = true;
          }
          else {
            first = false;
          }
          try {
            gatherDiagnostics(config, dir, i, j, (ObjectNode) replicas.get(replicaOffset), first);
          }
          catch (Exception e) {
            logger.error("Error gathering diagnostics from server: shard=" + i + ", replica=" + j);
          }
        }
      }
      for (Future future : futures) {
        future.get();
      }
      ArrayNode clients = databaseDict.withArray("clients");
      for (int i = 0; i < clients.size(); i++) {
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
        StringBuilder builder = new StringBuilder();
        builder.append(cli.getRet().getString("host"));
        builder.append(",").append(cli.getRet().getString("shard"));
        builder.append(",").append(cli.getRet().getString("replica"));
        builder.append(",").append(cli.getRet().getString("version"));

        writer.write(builder.toString() + "\n");
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

    PreparedStatement stmt = cli.getConn().prepareStatement("describe server health");
    ResultSet rs = stmt.executeQuery();

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

    if (cli.isWindows()) {
      System.out.println("Diagnostics file can be found at: " + installDir + "/tmp/*. Zip the contents and send the zip file");
    }
    else {
      ProcessBuilder pbuilder = new ProcessBuilder().command("tar", "-czf", "tmp/diag.tgz", "tmp/diag");
      Process p = pbuilder.start();
      p.waitFor();
      System.out.println("Diagnostics file can be found at: " + System.getProperty("user.dir") + "/tmp/diag.tgz");
    }
  }

  private void gatherDiagnostics(ObjectNode config, File dir, int shard, int replica, ObjectNode replicaDict, boolean first) throws IOException, InterruptedException {
    final String user = config.get("user").asText();
    final String publicAddress = replicaDict.get("publicAddress").asText();
    File machineDir = new File(dir, publicAddress);
    final String installDir = cli.resolvePath(config.get("installDirectory").asText());

    machineDir.mkdirs();

    final AtomicBoolean finished = new AtomicBoolean();
    if (!cli.isWindows()) {
      if (publicAddress.equals("localhost") || publicAddress.equals("127.0.0.1")) {
        if (first) {
          ProcessBuilder builder = new ProcessBuilder().command("killall", "-QUIT", "java");
          Process p = builder.start();
          p.waitFor();

          builder = new ProcessBuilder().command("bash", "bin/get-distribution", installDir);
          p = builder.start();
          p.waitFor();

          builder = new ProcessBuilder().command("bash", "bin/get-df", installDir);
          p = builder.start();
          p.waitFor();

          builder = new ProcessBuilder().command("bash", "bin/get-dir", installDir);
          p = builder.start();
          p.waitFor();

          builder = new ProcessBuilder().command("bash", "bin/get-top", installDir);
          p = builder.start();
          p.waitFor();

          builder = new ProcessBuilder().command("bash", "bin/get-jarlist", installDir);
          p = builder.start();
          p.waitFor();

          finished.set(true);
        }
      }
      else {
        if (first) {
          Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
              try {
                ProcessBuilder builder = new ProcessBuilder().command("ssh", "-n", "-f", "-o",
                    "UserKnownHostsFile=/dev/null", "-o", "StrictHostKeyChecking=no", user + "@" +
                        publicAddress, "killall -QUIT java");
                Process p = builder.start();
                p.waitFor();

                builder = new ProcessBuilder().command("ssh", "-n", "-f", "-o",
                    "UserKnownHostsFile=/dev/null", "-o", "StrictHostKeyChecking=no", user + "@" +
                        publicAddress, installDir + "/bin/get-distribution", installDir);
                p = builder.start();
                p.waitFor();

                builder = new ProcessBuilder().command("ssh", "-n", "-f", "-o",
                    "UserKnownHostsFile=/dev/null", "-o", "StrictHostKeyChecking=no", user + "@" +
                        publicAddress, installDir + "/bin/get-df", installDir);
                p = builder.start();
                p.waitFor();

                builder = new ProcessBuilder().command("ssh", "-n", "-f", "-o",
                    "UserKnownHostsFile=/dev/null", "-o", "StrictHostKeyChecking=no", user + "@" +
                        publicAddress, installDir + "/bin/get-dir", installDir);
                p = builder.start();
                p.waitFor();

                builder = new ProcessBuilder().command("ssh", "-n", "-f", "-o",
                    "UserKnownHostsFile=/dev/null", "-o", "StrictHostKeyChecking=no", user + "@" +
                        publicAddress, installDir + "/bin/get-top", installDir);
                p = builder.start();
                p.waitFor();

                builder = new ProcessBuilder().command("ssh", "-n", "-f", "-o",
                    "UserKnownHostsFile=/dev/null", "-o", "StrictHostKeyChecking=no", user + "@" +
                        publicAddress, installDir + "/bin/get-jarlist", installDir);
                p = builder.start();
                p.waitFor();
              }
              catch (Exception e) {
                logger.error("Error gathering diagnostics: server=" + publicAddress);
              }
              finally {
                finished.set(true);
              }
            }
          });
          thread.start();

          thread.join(10000, 0);

        }
      }
    }

    if (finished.get()) {
      if (!cli.isWindows() && first) {
        cli.getFile(config, machineDir, shard, replica, "/tmp/distribution");
        cli.getFile(config, machineDir, shard, replica, "/tmp/df");
        cli.getFile(config, machineDir, shard, replica, "/tmp/dir");
        cli.getFile(config, machineDir, shard, replica, "/tmp/top");
        cli.getFile(config, machineDir, shard, replica, "/tmp/jars");
      }
      cli.getFile(config, machineDir, shard, replica, "/logs/" + replicaDict.get("port").asInt() + ".log");
      cli.getFile(config, machineDir, shard, replica, "/logs/" + replicaDict.get("port").asInt() + ".sysout.log");
      cli.getFile(config, machineDir, shard, replica, "/logs/gc-" + replicaDict.get("port").asInt() + ".log.0.current");
    }
/*
    1.	Current log from each server
    3.	Current gc log from each server
    4.	Current sysout.log from each server
    5.	Results of top form each server
    6.	Version of software running on each server
    7.	Thread dump on each server
*/

  }

  public void purgeInstall() throws IOException {
    String cluster = cli.getCurrCluster();
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }

    System.out.println("Starting purge install: cluster=" + cluster);
    String json = IOUtils.toString(Cli.class.getResourceAsStream("/config-" + cluster + ".json"), "utf-8");
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode config = (ObjectNode) mapper.readTree(json);
    ObjectNode databaseDict = config;
    String dataDir = databaseDict.get("dataDirectory").asText();
    dataDir = cli.resolvePath(dataDir);
    String installDir = databaseDict.get("installDirectory").asText();
    installDir = cli.resolvePath(installDir);
    ArrayNode shards = databaseDict.withArray("shards");
    for (int i = 0; i < shards.size(); i++) {
      ArrayNode replicas = (ArrayNode) shards.get(i).withArray("replicas");
      for (int j = 0; j < replicas.size(); j++) {
        purgeInstallServer(databaseDict, dataDir, installDir, replicas, j);
      }
    }
    ArrayNode clients = databaseDict.withArray("clients");
    for (int i = 0; i < clients.size(); i++) {
      purgeInstallServer(databaseDict, dataDir, installDir, clients, i);
    }
    System.out.println("Finished purging install: cluster=" + cluster);
  }

  private static void purgeInstallServer(ObjectNode databaseDict, String dataDir, String installDir, ArrayNode replicas, int j) throws IOException {
    ObjectNode replica = (ObjectNode) replicas.get(j);
    String deployUser = databaseDict.get("user").asText();
    String publicAddress = replica.get("publicAddress").asText();
    if (publicAddress.equals("127.0.0.1") || publicAddress.equals("localhost")) {
      File file = new File(installDir);
      if (!dataDir.startsWith("/")) {
        file = new File(System.getProperty("user.home"), installDir);
      }
      System.out.println("Deleting directory: dir=" + file.getAbsolutePath());
      FileUtils.deleteDirectory(file);
    }
    else {
      ProcessBuilder builder = new ProcessBuilder().command("ssh", "-n", "-f", "-o",
          "UserKnownHostsFile=/dev/null", "-o", "StrictHostKeyChecking=no", deployUser + "@" +
              replica.get("publicAddress").asText(), "rm", "-rf", installDir);
      System.out.println("purging: address=" + replica.get("publicAddress").asText() + ", dir=" + dataDir);
      //builder.directory(workingDir);
      builder.start();
    }
  }

  private void startLicenseServer(ObjectNode config, String externalAddress, String privateAddress, String port,
                                         String installDir) throws IOException, InterruptedException {
    if (cli.isWindows()) {
      cli.getCredentials("license-server");
    }

    String deployUser = config.get("user").asText();
    if (port == null) {
      port = "8443";
    }
    String searchHome = installDir;
    if (externalAddress.equals("127.0.0.1") || externalAddress.equals("localhost")) {

      if (!searchHome.startsWith("/")) {
        File file = new File(System.getProperty("user.home"), searchHome);
        searchHome = file.getAbsolutePath();
      }
      searchHome = new File(System.getProperty("user.dir")).getAbsolutePath();
      System.out.println("Starting license server: installDir=" + searchHome);
      ProcessBuilder builder = null;
      if (cli.isCygwin() || cli.isWindows()) {
        System.out.println("starting license server: userDir=" + System.getProperty("user.dir"));

        builder = new ProcessBuilder().command("bin/start-license-server-task.bat", port, searchHome);
        Process p = builder.start();
        p.waitFor();
      }
      else {
        builder = new ProcessBuilder().command("bash", "bin/start-license-server", privateAddress, port, searchHome);
        Process p = builder.start();
      }
      System.out.println("Started server: address=" + externalAddress + ", port=" + port);
      return;
    }

    System.out.println("Home=" + searchHome);

    if (cli.isWindows()) {
      System.out.println("starting license server: userDir=" + System.getProperty("user.dir"));

      File file = new File("bin/remote-start-license-server-server.ps1");
      String str = IOUtils.toString(new FileInputStream(file), "utf-8");
      str = str.replaceAll("\\$1", new File(System.getProperty("user.dir"), "credentials/license-server-" + cli.getUsername()).getAbsolutePath().replaceAll("\\\\", "/"));
      str = str.replaceAll("\\$2", cli.getUsername());
      str = str.replaceAll("\\$3", externalAddress);
      str = str.replaceAll("\\$4", installDir);
      str = str.replaceAll("\\$5", port);
      File outFile = new File("tmp/" + externalAddress + "-" + port + "-remote-start-license-server.ps1");
      outFile.getParentFile().mkdirs();
      outFile.delete();
      try {
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outFile)))) {
          writer.write(str);
        }

        ProcessBuilder builder = new ProcessBuilder().command("powershell", "-F", outFile.getAbsolutePath());
        Process p = builder.start();
      }
      finally {
        //outFile.delete();
      }
    }
    else {
      ProcessBuilder builder = new ProcessBuilder().command("bash", "bin/do-start-license-server", deployUser + "@" + externalAddress,
          installDir, privateAddress, port, searchHome);
      Process p = builder.start();
      StringBuilder sbuilder = new StringBuilder();
      InputStream in = p.getInputStream();
      while (true) {
        int b = in.read();
        if (b == -1) {
          break;
        }
        sbuilder.append(String.valueOf((char) b));
        //System.out.write(b);
      }
      System.out.println(sbuilder.toString());
      int ret = p.waitFor();
      if (0 == ret) {
        System.out.println("Started license server: address=" + privateAddress + ", port=" + port);
      }
      else {
        System.out.println("Failed to start license server: address=" + privateAddress + ", port=" + port);
      }
    }
  }


  public void startLicenseServer() throws IOException, InterruptedException, ExecutionException {

    InputStream in = Cli.class.getResourceAsStream("/config-license-server.json");
    if (in == null) {
      in = new FileInputStream("/Users/lowryda/sonicbase/config/config-license-server.json");
    }
    String json = IOUtils.toString(in, "utf-8");
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode config = (ObjectNode) mapper.readTree(json);
    String dir = config.get("installDirectory").asText();
    final String installDir = cli.resolvePath(dir);

    stopLicenseServer();

    ObjectNode server = (ObjectNode) config.get("server");
    startLicenseServer(config, server.get("publicAddress").asText(), server.get("privateAddress").asText(),
        server.get("port").asText(), installDir);
    System.out.println("Finished starting license server");
  }



  private void stopLicenseServer(ObjectNode databaseDict, String externalAddress, String privateAddress, String port, String installDir) throws IOException, InterruptedException {
    String deployUser = databaseDict.get("user").asText();
    if (externalAddress.equals("127.0.0.1") || externalAddress.equals("localhost")) {
      ProcessBuilder builder = null;
      if (cli.isCygwin() || cli.isWindows()) {
        builder = new ProcessBuilder().command("bin/kill-server.bat", port);
      }
      else {
        builder = new ProcessBuilder().command("bash", "bin/kill-server", "LicenseServer", port, port, port, port);
      }
      Process p = builder.start();
      p.waitFor();
    }
    else {
      ProcessBuilder builder = null;
      Process p = null;
      if (cli.isWindows()) {
        File file = new File("bin/remote-kill-server.ps1");
        String str = IOUtils.toString(new FileInputStream(file), "utf-8");
        str = str.replaceAll("\\$1", new File(System.getProperty("user.dir"), "credentials/" + cli.getCurrCluster() + "-" + cli.getUsername()).getAbsolutePath().replaceAll("\\\\", "/"));
        str = str.replaceAll("\\$2", cli.getUsername());
        str = str.replaceAll("\\$3", externalAddress);
        str = str.replaceAll("\\$4", installDir);
        str = str.replaceAll("\\$5", port);
        File outFile = new File("tmp/" + externalAddress + "-" + port + "-remote-kill-server.ps1");
        outFile.getParentFile().mkdirs();
        outFile.delete();
        try {
          try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outFile)))) {
            writer.write(str);
          }
          builder = new ProcessBuilder().command("powershell", "-F", outFile.getAbsolutePath());
          p = builder.start();
        }
        finally {
          //outFile.delete();
        }
      }
      else {
        builder = new ProcessBuilder().command("ssh", "-n", "-f", "-o",
            "UserKnownHostsFile=/dev/null", "-o", "StrictHostKeyChecking=no", deployUser + "@" +
                externalAddress, installDir + "/bin/kill-server", "LicenseServer", port, port, port, port);
        p = builder.start();
      }
      p.waitFor();
    }
  }

  public void stopLicenseServer() throws IOException, InterruptedException, ExecutionException {

    InputStream in = Cli.class.getResourceAsStream("/config-license-server.json");
    if (in == null) {
      in = new FileInputStream("/Users/lowryda/sonicbase/config/config-license-server.json");
    }
    String json = IOUtils.toString(in, "utf-8");
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode config = (ObjectNode) mapper.readTree(json);
    final String installDir = cli.resolvePath(config.get("installDirectory").asText());

    ObjectNode server = (ObjectNode) config.get("server");
    stopLicenseServer(config, server.get("publicAddress").asText(), server.get("privateAddress").asText(),
        server.get("port").asText(), installDir);

    System.out.println("Stopped license server");
  }

}
