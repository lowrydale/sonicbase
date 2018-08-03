/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.ReconfigureResults;
import com.sonicbase.common.ComObject;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.sonicbase.common.MemUtil.getMemValue;

public class ClusterHandler {

  private static Logger logger = LoggerFactory.getLogger(ClusterHandler.class);
  private final Cli cli;

  public ClusterHandler(Cli cli) {
    this.cli = cli;
  }

  public void deploy() throws IOException, ExecutionException, InterruptedException {
    String cluster = cli.getCurrCluster();
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }
    System.out.println("deploying: cluster=" + cluster);

    Deploy deploy = new Deploy();
    deploy.deploy(cli.getCurrCluster(), "0");

    System.out.println("Finished deploy: cluster=" + cluster);
  }

  private void startServer(ObjectNode databaseDict, String externalAddress, String privateAddress, String port, String installDir,
                                  String cluster, AtomicReference<Double> lastTotalGig) throws IOException, InterruptedException {
    try {
      String deployUser = databaseDict.get("user").asText();
      String maxHeap = databaseDict.get("maxJavaHeap").asText();
      if (port == null) {
        port = "9010";
      }
      String searchHome = installDir;
      if (externalAddress.equals("127.0.0.1") || externalAddress.equals("localhost")) {
        maxHeap = cli.getMaxHeap(databaseDict);
        if (cli.isWindows()) {
          if (!searchHome.substring(1, 2).equals(":")) {
            File file = new File(System.getProperty("user.home"), searchHome);
            searchHome = file.getAbsolutePath();
          }
        }
        else {
          if (!searchHome.startsWith("/")) {
            File file = new File(System.getProperty("user.home"), searchHome);
            searchHome = file.getAbsolutePath();
          }
        }
      }
      System.out.println("Home=" + searchHome);
      if (externalAddress.equals("127.0.0.1") || externalAddress.equals("localhost")) {
        searchHome = new File(System.getProperty("user.dir")).getAbsolutePath();
        maxHeap = cli.getMaxHeap(databaseDict);
        ProcessBuilder builder = null;
        if (cli.isCygwin() || cli.isWindows()) {
          builder = new ProcessBuilder().command("bin/start-db-server-task.bat", privateAddress, port, maxHeap, searchHome, cluster);
          Process p = builder.start();
          p.waitFor();
          System.out.println("Started server: address=" + externalAddress + ", port=" + port + ", maxJavaHeap=" + maxHeap);
        }
        else {
          builder = new ProcessBuilder().command("bash", "bin/start-db-server", privateAddress, port, maxHeap, searchHome, cluster);
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
          System.out.println("Started server - linux: address=" + externalAddress + ", port=" + port + ", maxJavaHeap=" + maxHeap + ", searchHome=" + searchHome + ", cluster=" + cluster);
        }
      }
      else {
        System.out.println("Current working directory=" + System.getProperty("user.dir"));
        String maxStr = databaseDict.get("maxJavaHeap").asText();
        if (maxStr != null && maxStr.contains("%")) {
          ProcessBuilder builder = null;
          Process p = null;
          if (cli.isWindows()) {
            File file = new File("bin/remote-get-mem-total.ps1");
            String str = IOUtils.toString(new FileInputStream(file), "utf-8");
            str = str.replaceAll("\\$1", new File(System.getProperty("user.dir"), "credentials/" + cluster + "-" + cli.getUsername()).getAbsolutePath().replaceAll("\\\\", "/"));
            str = str.replaceAll("\\$2", cli.getUsername());
            str = str.replaceAll("\\$3", externalAddress);
            str = str.replaceAll("\\$4", installDir);
            File outFile = new File("tmp/" + externalAddress + "-" + port + "-remote-get-mem-total.ps1");
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
            builder = new ProcessBuilder().command("bash", "bin/remote-get-mem-total", deployUser + "@" + externalAddress, installDir);
            p = builder.start();
          }
          BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
          String line = reader.readLine();
          double totalGig = 0;
          if (cli.isWindows()) {
            totalGig = Double.valueOf(line) / 1000d / 1000d / 1000d;
          }
          else {
            try {
              if (line.toLowerCase().startsWith("memtotal")) {
                line = line.substring("MemTotal:".length()).trim();
                totalGig = getMemValue(line);
              }
              else {
                String[] parts = line.split(" ");
                String memStr = parts[1];
                totalGig = getMemValue(memStr);
              }
              lastTotalGig.set(totalGig);
            }
            catch (Exception e) {
              logger.error("Error getting totalGib", e);
              totalGig = lastTotalGig.get();
            }
          }
          p.waitFor();
          maxStr = maxStr.substring(0, maxStr.indexOf("%"));
          double maxPercent = Double.valueOf(maxStr);
          double maxGig = totalGig * (maxPercent / 100);
          maxHeap = (int) Math.floor(maxGig * 1024d) + "m";
        }

        System.out.println("Started server: address=" + externalAddress + ", port=" + port + ", maxJavaHeap=" + maxHeap);
        if (cli.isWindows()) {
          File file = new File("bin/remote-start-db-server.ps1");
          String str = IOUtils.toString(new FileInputStream(file), "utf-8");
          str = str.replaceAll("\\$1", new File(System.getProperty("user.dir"), "credentials/" + cluster + "-" + cli.getUsername()).getAbsolutePath().replaceAll("\\\\", "/"));
          str = str.replaceAll("\\$2", cli.getUsername());
          str = str.replaceAll("\\$3", externalAddress);
          str = str.replaceAll("\\$4", installDir);
          str = str.replaceAll("\\$5", privateAddress);
          str = str.replaceAll("\\$6", port);
          str = str.replaceAll("\\$7", maxHeap);
          str = str.replaceAll("\\$8", cluster);
          File outFile = new File("tmp/" + externalAddress + "-" + port + "-remote-start-db-server.ps1");
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
          ProcessBuilder builder = new ProcessBuilder().command("bash", "bin/do-start", deployUser + "@" + externalAddress, installDir, privateAddress, port, maxHeap, searchHome, cluster);
          Process p = builder.start();
          p.waitFor();
        }
      }
    }
    catch (Exception e) {
      System.out.println("Error starting server: publicAddress=" + externalAddress + ", internalAddress=" + privateAddress);
      throw e;
    }
  }



  public void startCluster() throws IOException, InterruptedException, SQLException, ClassNotFoundException, ExecutionException {
    final String cluster = cli.getCurrCluster();
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }

    InputStream in = Cli.class.getResourceAsStream("/config-" + cluster + ".json");
    if (in == null) {
      in = new FileInputStream("/Users/lowryda/sonicbase/config/config-" + cluster + ".json");
    }
    String json = IOUtils.toString(in, "utf-8");
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode config = (ObjectNode) mapper.readTree(json);
    final ObjectNode databaseDict = config;
    final String installDir = cli.resolvePath(databaseDict.get("installDirectory").asText());
    ArrayNode shards = databaseDict.withArray("shards");

    stopCluster();

    //System.out.println("cluster config=" + json);
    Thread.sleep(2000);

    final ArrayNode masterReplica = (ArrayNode) shards.get(0).withArray("replicas");
    ObjectNode master = (ObjectNode) masterReplica.get(0);
    startServer(databaseDict, master.get("publicAddress").asText(), master.get("privateAddress").asText(),
        master.get("port").asText(), installDir, cluster, new AtomicReference<Double>(0d));
    final AtomicBoolean ok = new AtomicBoolean();
    Thread.sleep(5_000);
    cli.initConnection();

    final AtomicReference<Double> lastTotalGig = new AtomicReference<>(0d);
    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < shards.size(); i++) {
      final int shardOffset = i;
      final ArrayNode replicas = (ArrayNode) shards.get(i).withArray("replicas");
      for (int j = 0; j < replicas.size(); j++) {
        final int replicaOffset = j;
        if (shardOffset == 0 && replicaOffset == 0) {
          continue;
        }
        futures.add(cli.getExecutor().submit(new Callable() {
          @Override
          public Object call() throws Exception {
            ObjectNode replica = (ObjectNode) replicas.get(replicaOffset);
            startServer(databaseDict, replica.get("publicAddress").asText(),
                replica.get("privateAddress").asText(), replica.get("port").asText(), installDir, cluster,
                lastTotalGig);
            return null;
          }
        }));
      }
    }
    for (Future future : futures) {
      future.get();
    }

    for (int i = 0; i < shards.size(); i++) {
      final int shardOffset = i;
      ArrayNode replicas = (ArrayNode) shards.get(i).withArray("replicas");
      for (int j = 0; j < replicas.size(); j++) {
        final int replicaOffset = j;
        final ObjectNode replica = (ObjectNode) replicas.get(j);
        final AtomicBoolean ok2 = new AtomicBoolean();
        while (!ok2.get()) {
          final ComObject cobj = new ComObject();
          cobj.put(ComObject.Tag.DB_NAME, "__none__");
          cobj.put(ComObject.Tag.SCHEMA_VERSION, 1);
          cobj.put(ComObject.Tag.METHOD, "DatabaseServer:healthCheck");

          Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
              try {
                byte[] bytes = cli.getConn().send(null, shardOffset, replicaOffset, cobj, ConnectionProxy.Replica.SPECIFIED, true);
                ComObject retObj = new ComObject(bytes);
                String retStr = retObj.getString(ComObject.Tag.STATUS);
                if (retStr.equals("{\"status\" : \"ok\"}")) {
                  ok2.set(true);
                }
                else {
                  System.out.println("Server not healthy: shard=" + shardOffset + ", replica=" + replicaOffset +
                      ", privateAddress=" + replica.get("privateAddress").asText());
                }
              }
              catch (Exception e) {
                ComObject cobj = new ComObject();
                cobj.put(ComObject.Tag.DB_NAME, "__none__");
                cobj.put(ComObject.Tag.SCHEMA_VERSION, 1);
                cobj.put(ComObject.Tag.METHOD, "DatabaseServer:getRecoverProgress");

                try {
                  byte[] bytes = cli.getConn().send(null, shardOffset, replicaOffset, cobj, ConnectionProxy.Replica.SPECIFIED, true);
                  ComObject retObj = new ComObject(bytes);
                  double percentComplete = retObj.getDouble(ComObject.Tag.PERCENT_COMPLETE);
                  String stage = retObj.getString(ComObject.Tag.STAGE);
                  Boolean error = retObj.getBoolean(ComObject.Tag.ERROR);

                  percentComplete *= 100d;
                  System.out.println("Waiting for " + replica.get("privateAddress").asText() + " to start: stage=" +
                      stage + ", percentComplete=" + String.format("%.2f", percentComplete) + (error != null && error ? ", error=true" : ""));
                  try {
                    Thread.sleep(2000);
                  }
                  catch (InterruptedException e1) {
                    return;
                  }
                }
                catch (Exception e1) {
                  System.out.println("Waiting for " + replica.get("privateAddress").asText() + " to start: percentComplete=?");
                  try {
                    Thread.sleep(2000);
                  }
                  catch (InterruptedException e2) {
                    return;
                  }
                }
              }
            }
          });
          thread.start();
          thread.join(5000);
          thread.interrupt();
        }
      }
    }
    System.out.println("Finished starting servers");
  }

  public void startReplica(final int replica, final ObjectNode config, final ArrayNode shards,
                                  final String installDir, final String cluster) throws IOException, InterruptedException, ExecutionException {
    final ArrayNode masterReplica = (ArrayNode) shards.get(0).withArray("replicas");

    final AtomicReference<Double> lastTotalGig = new AtomicReference<>(0d);
    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < shards.size(); i++) {
      final int shardOffset = i;
      final ArrayNode replicas = (ArrayNode) shards.get(i).withArray("replicas");
      for (int j = 0; j < replicas.size(); j++) {
        final int replicaOffset = j;
        if (replicaOffset != replica) {
          continue;
        }
        futures.add(cli.getExecutor().submit((Callable) () -> {
          ObjectNode replica1 = (ObjectNode) replicas.get(replicaOffset);
          startServer(config, replica1.get("publicAddress").asText(), replica1.get("privateAddress").asText(),
              replica1.get("port").asText(), installDir, cluster, lastTotalGig);
          return null;
        }));
      }
    }
    for (Future future : futures) {
      future.get();
    }

    for (int i = 0; i < shards.size(); i++) {
      final int shardOffset = i;
      ArrayNode replicas = (ArrayNode) shards.get(i).withArray("replicas");
      for (int j = 0; j < replicas.size(); j++) {
        final int replicaOffset = j;
        if (replicaOffset != replica) {
          continue;
        }
        final ObjectNode replicaDict = (ObjectNode) replicas.get(j);
        final AtomicBoolean ok2 = new AtomicBoolean();
        while (!ok2.get()) {
          final ComObject cobj = new ComObject();
          cobj.put(ComObject.Tag.DB_NAME, "__none__");
          cobj.put(ComObject.Tag.SCHEMA_VERSION, 1);
          cobj.put(ComObject.Tag.METHOD, "DatabaseServer:healthCheck");

          Thread thread = new Thread(() -> {
            try {
              byte[] bytes = cli.getConn().send(null, shardOffset, replicaOffset, cobj, ConnectionProxy.Replica.SPECIFIED);
              ComObject retObj = new ComObject(bytes);
              String retStr = retObj.getString(ComObject.Tag.STATUS);
              if (retStr.equals("{\"status\" : \"ok\"}")) {
                ok2.set(true);
              }
              else {
                System.out.println("Server not healthy: shard=" + shardOffset + ", replica=" + replicaOffset +
                    ", privateAddress=" + replicaDict.get("privateAddress").asText());
              }
            }
            catch (Exception e) {
              ComObject pcobj = new ComObject();
              pcobj.put(ComObject.Tag.DB_NAME, "__none__");
              pcobj.put(ComObject.Tag.SCHEMA_VERSION, cli.getConn().getSchemaVersion());
              pcobj.put(ComObject.Tag.METHOD, "DatabaseServer:getRecoverProgress");
              try {
                byte[] bytes = cli.getConn().send(null, shardOffset, replicaOffset,
                    pcobj, ConnectionProxy.Replica.SPECIFIED, true);
                ComObject retObj = new ComObject(bytes);
                double percentComplete = retObj.getDouble(ComObject.Tag.PERCENT_COMPLETE);
                String stage = retObj.getString(ComObject.Tag.STAGE);
                Boolean error = retObj.getBoolean(ComObject.Tag.ERROR);

                percentComplete *= 100d;
                System.out.println("Waiting for servers to start... server=" + replicaDict.get("privateAddress").asText() +
                    ", stage=" + stage + ", percentComplete=" + String.format("%.2f", percentComplete) + (error != null && error ? ", error=true" : ""));
                try {
                  Thread.sleep(2000);
                }
                catch (InterruptedException e1) {
                  return;
                }
              }
              catch (Exception e1) {
                System.out.println("Waiting for servers to start... server=" + replicaDict.get("privateAddress").asText() + ", percentComplete=?");
                try {
                  Thread.sleep(2000);
                }
                catch (InterruptedException e2) {
                  return;
                }
              }
            }
          });
          thread.start();
          thread.join(5000);
          thread.interrupt();
        }
      }
    }
  }

  public void reloadServerStatus(String command) throws SQLException, ClassNotFoundException {
    String cluster = cli.getCurrCluster();
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }

    cli.initConnection();

    command = command.trim();
    String[] parts = command.split("\\s+");
    final int shard = Integer.valueOf(parts[3]);
    final int replica = Integer.valueOf(parts[4]);

    if (getReloadStatus(cli.getConn(), shard, replica)) {
      System.out.println("complete");
    }
    else {
      System.out.println("running");
    }
  }

  private static Boolean getReloadStatus(ConnectionProxy conn, int shard, int replica) {
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, "__none__");
    cobj.put(ComObject.Tag.SCHEMA_VERSION, conn.getSchemaVersion());
    byte[] bytes = conn.send("BackupManaer:isServerReloadFinished", shard, replica, cobj, ConnectionProxy.Replica.SPECIFIED);
    ComObject retObj = new ComObject(bytes);
    return retObj.getBoolean(ComObject.Tag.IS_COMPLETE);
  }

  public void reloadServer(String command) throws SQLException, ClassNotFoundException {
    String cluster = cli.getCurrCluster();
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }

    cli.initConnection();

    command = command.trim();
    String[] parts = command.split("\\s+");
    final int shard = Integer.valueOf(parts[2]);
    final int replica = Integer.valueOf(parts[3]);

    reloadServer(cli.getConn(), shard, replica);
  }

  public void reloadReplica(String command) throws SQLException, ClassNotFoundException {
    String cluster = cli.getCurrCluster();
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }

    cli.initConnection();

    command = command.trim();
    String[] parts = command.split("\\s+");
    final int replica = Integer.valueOf(parts[2]);

    for (int shard = 0; shard < cli.getConn().getShardCount(); shard++) {
      reloadServer(cli.getConn(), shard, replica);
    }
  }

  public void getReplicaReloadStatus(String command) throws SQLException, ClassNotFoundException {
    String cluster = cli.getCurrCluster();
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }

    cli.initConnection();

    command = command.trim();
    String[] parts = command.split("\\s+");
    final int replica = Integer.valueOf(parts[3]);

    for (int shard = 0; shard < cli.getConn().getShardCount(); shard++) {
      if (!getReloadStatus(cli.getConn(), shard, replica)) {
        System.out.println("running");
        return;
      }
    }
    System.out.println("complete");
  }


  private static void reloadServer(ConnectionProxy conn, int shard, int replica) {
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, "__none__");
    cobj.put(ComObject.Tag.SCHEMA_VERSION, conn.getSchemaVersion());
    conn.send("BackupManager:reloadServer", shard, replica, cobj, ConnectionProxy.Replica.SPECIFIED);
  }


  public void rollingRestart() throws IOException, InterruptedException, SQLException, ClassNotFoundException, ExecutionException {
    final String cluster = cli.getCurrCluster();
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }

    InputStream in = Cli.class.getResourceAsStream("/config-" + cluster + ".json");
    if (in == null) {
      in = new FileInputStream("/Users/lowryda/sonicbase/config/config-" + cluster + ".json");
    }
    String json = IOUtils.toString(in, "utf-8");
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode config = (ObjectNode) mapper.readTree(json);
    final ObjectNode databaseDict = config;
    final String installDir = cli.resolvePath(databaseDict.get("installDirectory").asText());
    ArrayNode shards = databaseDict.withArray("shards");

    if (config.withArray("shards").get(0).withArray("replicas").size() > 1) {
      rollingRestart(config, shards, installDir, cluster);
    }
    else {
      System.out.println("Cannot restart a cluster with one replica. Call 'start cluster'.");
    }
  }

  public void stopServer(String command) throws InterruptedException, ExecutionException, IOException {
    final String cluster = cli.getCurrCluster();
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }

    command = command.trim();
    String[] parts = command.split("\\s+");
    final int shard = Integer.valueOf(parts[2]);
    final int replica = Integer.valueOf(parts[3]);

    InputStream in = Cli.class.getResourceAsStream("/config-" + cluster + ".json");
    if (in == null) {
      in = new FileInputStream("/Users/lowryda/sonicbase/config/config-" + cluster + ".json");
    }
    String json = IOUtils.toString(in, "utf-8");
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode config = (ObjectNode) mapper.readTree(json);
    final ObjectNode databaseDict = config;
    final String installDir = cli.resolvePath(databaseDict.get("installDirectory").asText());
    ArrayNode shards = databaseDict.withArray("shards");
    final ArrayNode masterReplica = (ArrayNode) shards.get(shard).withArray("replicas");
    ObjectNode currReplica = (ObjectNode) masterReplica.get(replica);

    stopServer(databaseDict, currReplica.get("publicAddress").asText(), currReplica.get("privateAddress").asText(),
        String.valueOf(currReplica.get("port").asInt()), installDir);

  }

  private void stopServer(ObjectNode databaseDict, String externalAddress, String privateAddress, String port, String installDir) throws IOException, InterruptedException {
    String deployUser = databaseDict.get("user").asText();
    if (externalAddress.equals("127.0.0.1") || externalAddress.equals("localhost")) {
      ProcessBuilder builder = null;
      if (cli.isCygwin() || cli.isWindows()) {
        builder = new ProcessBuilder().command("bin/kill-server.bat", port);
      }
      else {
        builder = new ProcessBuilder().command("bash", "bin/kill-server", "NettyServer", "-host", privateAddress, "-port", port);
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
                externalAddress, installDir + "/bin/kill-server", "NettyServer", "-host", privateAddress, "-port", port);
        p = builder.start();
      }
      //builder.directory(workingDir);
//      InputStream in = p.getInputStream();
//      String str = StreamUtils.inputStreamToString(in);
//      System.out.println(str);
      p.waitFor();

    }
  }


  public void purgeCluster() throws IOException, InterruptedException, ExecutionException {
    final String cluster = cli.getCurrCluster();
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }

    System.out.println("Stopping cluster: cluster=" + cluster);
    stopCluster();
    System.out.println("Starting purge: cluster=" + cluster);
    String json = IOUtils.toString(Cli.class.getResourceAsStream("/config-" + cluster + ".json"), "utf-8");
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode config = (ObjectNode) mapper.readTree(json);
    final ObjectNode databaseDict = config;
    final String dataDir = cli.resolvePath(databaseDict.get("dataDirectory").asText());
    ArrayNode shards = databaseDict.withArray("shards");
    Set<String> addresses = new HashSet<>();
    for (int i = 0; i < shards.size(); i++) {
      final ArrayNode replicas = (ArrayNode) shards.get(i).withArray("replicas");
      for (int j = 0; j < replicas.size(); j++) {
        ObjectNode replica = (ObjectNode) replicas.get(j);
        addresses.add(replica.get("publicAddress").asText());
      }
    }
    List<Future> futures = new ArrayList<>();
    for (final String address : addresses) {
      futures.add(cli.getExecutor().submit(new Callable() {
        @Override
        public Object call() throws Exception {
          String deployUser = databaseDict.get("user").asText();
          if (address.equals("127.0.0.1") || address.equals("localhost")) {
            File file = new File(dataDir);
            if (!dataDir.startsWith("/")) {
              file = new File(System.getProperty("user.home"), dataDir);
            }
            File lastFile = new File(file.getAbsolutePath() + ".last");
            file.renameTo(lastFile);
            System.out.println("Deleting directory: dir=" + file.getAbsolutePath());
            FileUtils.deleteDirectory(lastFile);
          }
          else {
            if (cli.isWindows()) {
              final String installDir = cli.resolvePath(databaseDict.get("installDirectory").asText());

              ProcessBuilder builder = null;
              File file = new File("bin/remote-purge-data.ps1");
              String str = IOUtils.toString(new FileInputStream(file), "utf-8");
              str = str.replaceAll("\\$1", new File(System.getProperty("user.dir"), "credentials/" + cluster + "-" + cli.getUsername()).getAbsolutePath().replaceAll("\\\\", "/"));
              str = str.replaceAll("\\$2", cli.getUsername());
              str = str.replaceAll("\\$3", address);
              str = str.replaceAll("\\$4", installDir);
              str = str.replaceAll("\\$5", dataDir);
              File outFile = new File("tmp/" + address + "-remote-purge-data.ps1");
              outFile.getParentFile().mkdirs();
              outFile.delete();
              try {
                try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outFile)))) {
                  writer.write(str);
                }

                builder = new ProcessBuilder().command("powershell", "-F", outFile.getAbsolutePath());
                Process p = builder.start();
                p.waitFor();
              }
              finally {
                //outFile.delete();
              }
            }
            else {
              ProcessBuilder builder = new ProcessBuilder().command("ssh", "-n", "-f", "-o",
                  "UserKnownHostsFile=/dev/null", "-o", "StrictHostKeyChecking=no", deployUser + "@" +
                      address, "mv", dataDir, dataDir + ".last");
              //builder.directory(workingDir);
              Process p = builder.start();
              p.waitFor();

              builder = new ProcessBuilder().command("ssh", "-n", "-f", "-o",
                  "UserKnownHostsFile=/dev/null", "-o", "StrictHostKeyChecking=no", deployUser + "@" +
                      address, "rm", "-rf", dataDir + ".last");
              System.out.println("purging: address=" + address + ", dir=" + dataDir);
              p = builder.start();
              p.waitFor();

              //delete it twice to make sure
              builder = new ProcessBuilder().command("ssh", "-n", "-f", "-o",
                  "UserKnownHostsFile=/dev/null", "-o", "StrictHostKeyChecking=no", deployUser + "@" +
                      address, "rm", "-rf", dataDir + ".last");
              System.out.println("purging: address=" + address + ", dir=" + dataDir);
              p = builder.start();
              p.waitFor();
            }
          }
          return null;
        }
      }));
    }
    for (Future future : futures) {
      future.get();
    }
    System.out.println("Finished purging: cluster=" + cluster);
  }

  private void stopReplica(final int replica, final ObjectNode config, final ArrayNode shards,
                                  final String installDir, final String cluster) throws IOException, InterruptedException, ExecutionException {
    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < shards.size(); i++) {
      final int shardOffset = i;
      ObjectNode shard = (ObjectNode) shards.get(i);
      final ArrayNode replicas = shard.withArray("replicas");
      for (int j = 0; j < replicas.size(); j++) {
        final int replicaOffset = j;
        if (replicaOffset != replica) {
          continue;
        }
        futures.add(cli.getExecutor().submit((Callable) () -> {
          ObjectNode replica1 = (ObjectNode) replicas.get(replicaOffset);
          System.out.println("Stopping server: address=" + replica1.get("publicAddress").asText() +
              ", port=" + replica1.get("port").asText());
          stopServer(config, replica1.get("publicAddress").asText(), replica1.get("privateAddress").asText(),
              replica1.get("port").asText(), installDir);
          System.out.println("Stopped server: address=" + replica1.get("publicAddress").asText() +
              ", port=" + replica1.get("port").asText());
          return null;
        }));
      }
    }
    for (Future future : futures) {
      future.get();
    }

    System.out.println("Stopped replica: replica=" + replica);
  }

  public void stopShard(int shardOffset) throws IOException, InterruptedException {
    String cluster = cli.getCurrCluster();
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }
    String json = IOUtils.toString(Cli.class.getResourceAsStream("/config-" + cluster + ".json"), "utf-8");
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode config = (ObjectNode) mapper.readTree(json);
    ObjectNode databaseDict = config;
    String installDir = databaseDict.get("installDirectory").asText();
    installDir = cli.resolvePath(installDir);
    ArrayNode shards = databaseDict.withArray("shards");
    ObjectNode shard = (ObjectNode) shards.get(shardOffset);
    ArrayNode replicas = shard.withArray("replicas");
    for (int j = 0; j < replicas.size(); j++) {
      ObjectNode replica = (ObjectNode) replicas.get(j);
      stopServer(databaseDict, replica.get("publicAddress").asText(), replica.get("privateAddress").asText(),
          replica.get("port").asText(), installDir);
    }
  }

  public void stopCluster() throws IOException, InterruptedException, ExecutionException {
    String cluster = cli.getCurrCluster();
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }
    InputStream in = Cli.class.getResourceAsStream("/config-" + cluster + ".json");
    if (in == null) {
      in = new FileInputStream("/Users/lowryda/sonicbase/config/config-" + cluster + ".json");
    }
    String json = IOUtils.toString(in, "utf-8");
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode config = (ObjectNode) mapper.readTree(json);
    final ObjectNode databaseDict = config;
    final String installDir = cli.resolvePath(databaseDict.get("installDirectory").asText());
    ArrayNode shards = databaseDict.withArray("shards");
    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < shards.size(); i++) {
      final int shardOffset = i;
      ObjectNode shard = (ObjectNode) shards.get(i);
      final ArrayNode replicas = shard.withArray("replicas");
      for (int j = 0; j < replicas.size(); j++) {
        final int replicaOffset = j;
        futures.add(cli.getExecutor().submit((Callable) () -> {
          ObjectNode replica = (ObjectNode) replicas.get(replicaOffset);
          System.out.println("Stopping server: address=" + replica.get("publicAddress").asText() +
              ", port=" + replica.get("port").asText());
          stopServer(databaseDict, replica.get("publicAddress").asText(),
              replica.get("privateAddress").asText(), replica.get("port").asText(), installDir);
          System.out.println("Stopped server: address=" + replica.get("publicAddress").asText() +
              ", port=" + replica.get("port").asText());
          return null;
        }));
      }
    }
    for (Future future : futures) {
      future.get();
    }

    System.out.println("Stopped cluster");
  }


  public void startShard(int shardOffset) throws IOException, InterruptedException {
    String cluster = cli.getCurrCluster();
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }
    String json = IOUtils.toString(Cli.class.getResourceAsStream("/config-" + cluster + ".json"), "utf-8");
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode config = (ObjectNode) mapper.readTree(json);
    ObjectNode databaseDict = config;
    String installDir = databaseDict.get("installDirectory").asText();
    installDir = cli.resolvePath(installDir);
    ArrayNode shards = databaseDict.withArray("shards");
    ObjectNode shard = (ObjectNode) shards.get(shardOffset);
    ArrayNode replicas = shard.withArray("replicas");
    for (int j = 0; j < replicas.size(); j++) {
      ObjectNode replica = (ObjectNode) replicas.get(j);
      stopServer(databaseDict, replica.get("publicAddress").asText(), replica.get("privateAddress").asText(),
          replica.get("port").asText(), installDir);
    }
    Thread.sleep(2000);
    final AtomicReference<Double> lastTotalGig = new AtomicReference<>(0d);
    replicas = shard.withArray("replicas");
    for (int j = 0; j < replicas.size(); j++) {
      ObjectNode replica = (ObjectNode) replicas.get(j);
      startServer(databaseDict, replica.get("publicAddress").asText(), replica.get("privateAddress").asText(),
          replica.get("port").asText(), installDir, cluster, lastTotalGig);
    }
    System.out.println("Finished starting servers");
  }

  public void reconfigureCluster() throws SQLException, ClassNotFoundException, IOException, InterruptedException, ExecutionException {
    String cluster = cli.getCurrCluster();
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }
    cli.closeConnection();
    cli.initConnection();
    deploy();

    ReconfigureResults results = cli.getConn().reconfigureCluster();
    if (!results.isHandedOffToMaster()) {
      System.out.println("Must start servers to reconfigure the cluster");
    }
    else {
      int shardCount = results.getShardCount();
      if (shardCount > 0) {
        String json = IOUtils.toString(Cli.class.getResourceAsStream("/config-" + cluster + ".json"), "utf-8");
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode config = (ObjectNode) mapper.readTree(json);
        ObjectNode databaseDict = config;
        ArrayNode shards = databaseDict.withArray("shards");
        int startedCount = 0;
        for (int i = shards.size() - 1; i >= 0; i--) {
          startShard(i);
          if (++startedCount >= shardCount) {
            break;
          }
        }
      }
    }
    System.out.println("Finished reconfiguring cluster");
  }

  public void startServer(String command) throws InterruptedException, ExecutionException, IOException {
    final String cluster = cli.getCurrCluster();
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }

    command = command.trim();
    String[] parts = command.split("\\s+");
    final int shard = Integer.valueOf(parts[2]);
    final int replica = Integer.valueOf(parts[3]);

    InputStream in = Cli.class.getResourceAsStream("/config-" + cluster + ".json");
    if (in == null) {
      in = new FileInputStream("/Users/lowryda/sonicbase/config/config-" + cluster + ".json");
    }
    String json = IOUtils.toString(in, "utf-8");
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode config = (ObjectNode) mapper.readTree(json);
    final ObjectNode databaseDict = config;
    final String installDir = cli.resolvePath(databaseDict.get("installDirectory").asText());
    ArrayNode shards = databaseDict.withArray("shards");
    final ArrayNode masterReplica = (ArrayNode) shards.get(shard).withArray("replicas");
    final ObjectNode currReplica = (ObjectNode) masterReplica.get(replica);

    stopServer(databaseDict, currReplica.get("publicAddress").asText(), currReplica.get("privateAddress").asText(),
        String.valueOf(currReplica.get("port").asInt()), installDir);

    //System.out.println("cluster config=" + json);
    Thread.sleep(2000);

    startServer(databaseDict, currReplica.get("publicAddress").asText(), currReplica.get("privateAddress").asText(),
        currReplica.get("port").asText(), installDir, cluster, new AtomicReference<Double>(0d));
    final AtomicBoolean ok = new AtomicBoolean();
    while (!ok.get()) {
      final ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.DB_NAME, "__none__");
      cobj.put(ComObject.Tag.SCHEMA_VERSION, 1);
      cobj.put(ComObject.Tag.METHOD, "DatabaseServer:healthCheck");

      Thread thread = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            cli.initConnection();

            byte[] bytes = cli.getConn().send(null, shard, replica, cobj, ConnectionProxy.Replica.MASTER, true);
            ComObject retObj = new ComObject(bytes);
            String retStr = retObj.getString(ComObject.Tag.STATUS);
            if (retStr.equals("{\"status\" : \"ok\"}")) {
              ok.set(true);
              return;
            }
          }
          catch (Exception e) {
            ComObject cobj = new ComObject();
            cobj.put(ComObject.Tag.DB_NAME, "__none__");
            cobj.put(ComObject.Tag.SCHEMA_VERSION, 1);
            cobj.put(ComObject.Tag.METHOD, "DatabaseServer:getRecoverProgress");

            try {
              byte[] bytes = cli.getConn().send(null, shard, replica, cobj, ConnectionProxy.Replica.SPECIFIED, true);
              ComObject retObj = new ComObject(bytes);
              double percentComplete = retObj.getDouble(ComObject.Tag.PERCENT_COMPLETE);
              String stage = retObj.getString(ComObject.Tag.STAGE);
              Boolean error = retObj.getBoolean(ComObject.Tag.ERROR);

              percentComplete *= 100d;
              System.out.println("Waiting for " + currReplica.get("privateAddress").asText() + " to start: stage=" +
                  stage + ", " + String.format("%.2f", percentComplete) + "%" + (error != null && error ? ", error=true" : ""));
              try {
                Thread.sleep(2000);
              }
              catch (InterruptedException e1) {
                return;
              }
            }
            catch (Exception e1) {
              System.out.println("Waiting for " + currReplica.get("privateAddress").asText() + " to start: percentComplete=?");
              try {
                Thread.sleep(2000);
              }
              catch (InterruptedException e2) {
                return;
              }
            }
          }
        }
      });
      thread.start();
      thread.join(5000);
      thread.interrupt();
    }
  }

  private void rollingRestart(ObjectNode config, ArrayNode shards, String installDir, String cluster) throws InterruptedException, ExecutionException, IOException, SQLException, ClassNotFoundException {
    int replicaCount = cli.getConn().getReplicaCount();

    boolean allHealthy = cli.healthCheck();
    if (!allHealthy) {
      System.out.println("At least one server is not healthy. Cannot proceed with restart");
      return;
    }

    cli.initConnection();

    for (int i = 0; i < replicaCount; i++) {
      int replica = i;

      Thread.sleep(5000);
      changeMasters(replica, (replica + 1) % replicaCount, config, shards, installDir, cluster);
      Thread.sleep(5000);
      markReplicaDead(replica, config, shards, installDir, cluster);
      try {
        Thread.sleep(5000);
        stopReplica(replica, config, shards, installDir, cluster);
        Thread.sleep(5000);
        startReplica(replica, config, shards, installDir, cluster);
      }
      finally {
        markReplicaAlive(replica, config, shards, installDir, cluster);
      }
      System.out.println("Finished restarting replica: replica=" + replica);
    }
  }

  private void changeMasters(int replica, int newReplica, ObjectNode config, ArrayNode shards,
                                    String installDir, String cluster) throws SQLException, ClassNotFoundException {

    while (true) {
      try {
        cli.getConn().syncSchema();

        ComObject cobj = new ComObject();
        cobj.put(ComObject.Tag.DB_NAME, "__none__");
        cobj.put(ComObject.Tag.SCHEMA_VERSION, cli.getConn().getSchemaVersion());
        cobj.put(ComObject.Tag.METHOD, "DatabaseServer:promoteEntireReplicaToMaster");
        cobj.put(ComObject.Tag.REPLICA, newReplica);
        byte[] bytes = cli.getConn().sendToMaster(cobj);
        break;
      }
      catch (Exception e) {

      }
    }
  }

  private void markReplicaAlive(int replica, ObjectNode config, ArrayNode shards, String installDir, String cluster) {
    while (true) {
      try {
        cli.getConn().syncSchema();

        ComObject cobj = new ComObject();
        cobj.put(ComObject.Tag.DB_NAME, "__none__");
        cobj.put(ComObject.Tag.SCHEMA_VERSION, cli.getConn().getSchemaVersion());
        cobj.put(ComObject.Tag.METHOD, "DatabaseServer:markReplicaAlive");
        cobj.put(ComObject.Tag.REPLICA, replica);
        byte[] bytes = cli.getConn().sendToMaster(cobj);
        break;
      }
      catch (Exception e) {

      }
    }
  }

  private void markReplicaDead(int replica, ObjectNode config, ArrayNode shards, String installDir, String cluster) {
    while (true) {
      try {
        cli.getConn().syncSchema();

        ComObject cobj = new ComObject();
        cobj.put(ComObject.Tag.DB_NAME, "__none__");
        cobj.put(ComObject.Tag.SCHEMA_VERSION, cli.getConn().getSchemaVersion());
        cobj.put(ComObject.Tag.METHOD, "DatabaseServer:markReplicaDead");
        cobj.put(ComObject.Tag.REPLICA, replica);
        byte[] bytes = cli.getConn().sendToMaster(cobj);
        break;
      }
      catch (Exception e) {

      }
    }
  }

}
