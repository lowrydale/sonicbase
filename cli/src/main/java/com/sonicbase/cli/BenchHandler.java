/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.mashape.unirest.request.GetRequest;
import com.sonicbase.common.Config;
import com.sonicbase.query.DatabaseException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class BenchHandler {

  public static final String CONFIG_STR = "/config-";
  public static final String JSON_STR = ".json";
  public static final String UTF_8_STR = "utf-8";
  public static final String USER_DIR_STR = "user.dir";
  public static final String CLIENTS_STR = "clients";
  public static final String PUBLIC_ADDRESS_STR = "publicAddress";
  public static final String BENCH_START_STR = "/bench/start/";
  public static final String CLUSTER_STR = "?cluster=";
  public static final String COUNT_1000000000_OFFSET_STR = "&count=1000000000&offset=";
  public static final String COUNT_STR = "count";
  private static long benchStartTime;
  private static final List<String> benchUris = new ArrayList<>();
  private static ThreadPoolExecutor benchExecutor;
  private final Cli cli;

  BenchHandler(Cli cli) {
    this.cli = cli;
  }


  void initBench(String cluster) throws IOException {
    Config config = cli.getConfig(cluster);
    benchUris.clear();
    List<Config.Client> clients = config.getClients();
    if (clients != null) {
      if (benchExecutor != null) {
        benchExecutor.shutdownNow();
      }
      benchExecutor = new ThreadPoolExecutor(Math.max(1, clients.size()), Math.max(1, clients.size()), 10000L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
      for (int i = 0; i < clients.size(); i++) {
        Config.Client replica = clients.get(i);
        String externalAddress = replica.getString(PUBLIC_ADDRESS_STR);
        int port = replica.getInt("port");
        benchUris.add("http://" + externalAddress + ":" + port);
      }
    }
  }


  private void benchStopCluster() throws IOException, InterruptedException, ExecutionException {
    String cluster = cli.getCurrCluster();
    if (cluster == null) {
      cli.println("Error, not using a cluster");
      return;
    }
    Config config = cli.getConfig(cluster);
    final String installDir = cli.resolvePath(config.getString("installDirectory"));
    List<Config.Client> clients = config.getClients();
    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < clients.size(); i++) {
      final Config.Client client = clients.get(i);
      futures.add(cli.getExecutor().submit((Callable) () -> {
        stopBenchServer(config, client.getString(PUBLIC_ADDRESS_STR), client.getString("privateAddress"),
            client.getInt("port"), installDir);
        return null;
      }));
    }
    for (Future future : futures) {
      future.get();
    }
    cli.println("Stopped benchmark cluster");
  }

  private void stopBenchServer(Config config, String externalAddress, String privateAddress,
                                      int port, String installDir) throws IOException, InterruptedException {
    String deployUser = config.getString("user");
    if (externalAddress.equals("127.0.0.1") || externalAddress.equals("localhost")) {
      ProcessBuilder builder = null;
      if (cli.isCygwin() || cli.isWindows()) {
        cli.println("killing windows: port=" + port);
        builder = new ProcessBuilder().command("bin/kill-server.bat", String.valueOf(port));
      }
      else {
        builder = new ProcessBuilder().command("bash", "bin/kill-server", "BenchServer", String.valueOf(port), String.valueOf(port), String.valueOf(port), String.valueOf(port));
      }
      Process p = builder.start();
      p.waitFor();
    }
    else {
      ProcessBuilder builder = null;
      Process p = null;
      if (cli.isWindows()) {
        File file = new File("bin/remote-kill-server.ps1");
        String str = IOUtils.toString(new FileInputStream(file), UTF_8_STR);
        str = str.replaceAll("\\$1", new File(System.getProperty(USER_DIR_STR), "credentials/" +
            cli.getCurrCluster() + "-" + cli.getUsername()).getAbsolutePath().replaceAll("\\\\", "/"));
        str = str.replaceAll("\\$2", cli.getUsername());
        str = str.replaceAll("\\$3", externalAddress);
        str = str.replaceAll("\\$4", installDir);
        str = str.replaceAll("\\$5", String.valueOf(port));
        File outFile = new File("tmp/" + externalAddress + "-" + port + "-remote-kill-server.ps1");
        outFile.getParentFile().mkdirs();
        FileUtils.forceDelete(outFile);
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outFile)))) {
          writer.write(str);
        }
        builder = new ProcessBuilder().command("powershell", "-F", outFile.getAbsolutePath());
        p = builder.start();
      }
      else {
        builder = new ProcessBuilder().command("ssh", "-n", "-f", "-o",
            "UserKnownHostsFile=/dev/null", "-o", "StrictHostKeyChecking=no", deployUser + "@" +
                externalAddress, installDir + "/bin/kill-server", "BenchServer", String.valueOf(port), String.valueOf(port), String.valueOf(port), String.valueOf(port));
        p = builder.start();
      }
      p.waitFor();
    }
  }


  void benchStartTest(String command) throws InterruptedException, ExecutionException, IOException {
    String[] parts = command.split(" ");
    String test = parts[2];

    if (test.equals("cluster")) {
      benchStartCluster();
      return;
    }

    String queryType = null;
    if (parts.length > 3) {
      queryType = parts[3];
    }
    long offset = 0;
    if (test.equals("insert") || test.equals("aws-insert") || test.equals("aws-delete")) {
      offset = Long.valueOf(parts[3]);
    }

    StringBuilder failed = new StringBuilder();
    boolean anyFailed = false;
    benchStartTime = System.currentTimeMillis();
    List<Response> responses = null;
    if (test.equals("insert")) {
      responses = sendBenchRequest(cli,BENCH_START_STR + test + CLUSTER_STR + cli.getCurrCluster() +
          COUNT_1000000000_OFFSET_STR + offset);
    }
    else if (test.equals("delete")) {
      responses = sendBenchRequest(cli,BENCH_START_STR + test + CLUSTER_STR + cli.getCurrCluster() +
          COUNT_1000000000_OFFSET_STR + offset);
    }
    else if (test.equals("aws-insert")) {
      responses = sendBenchRequest(cli,BENCH_START_STR + test + CLUSTER_STR + cli.getCurrCluster() +
          COUNT_1000000000_OFFSET_STR + offset);
    }
    else if (test.equals("aws-delete")) {
      responses = sendBenchRequest(cli,BENCH_START_STR + test + CLUSTER_STR + cli.getCurrCluster() +
          COUNT_1000000000_OFFSET_STR + offset);
    }
    else if (test.equals("kafka-insert")) {
      responses = sendBenchRequest(cli,BENCH_START_STR + test + CLUSTER_STR + cli.getCurrCluster() +
          COUNT_1000000000_OFFSET_STR + offset);
    }
    else if (test.equals("kafka-delete")) {
      responses = sendBenchRequest(cli,BENCH_START_STR + test + CLUSTER_STR + cli.getCurrCluster() +
          COUNT_1000000000_OFFSET_STR + offset);
    }
    else if (test.equals("identity")) {
      responses = sendBenchRequest(cli,BENCH_START_STR + test + CLUSTER_STR + cli.getCurrCluster() +
          "&count=1000000000&queryType=" + queryType);
    }
    else if (test.equals("joins")) {
      responses = sendBenchRequest(cli,BENCH_START_STR + test + CLUSTER_STR + cli.getCurrCluster() +
          "&count=1000000000&queryType=" + queryType);
    }
    else if (test.equals("range")) {
      responses = sendBenchRequest(cli,BENCH_START_STR + test + CLUSTER_STR + cli.getCurrCluster() +
          "&count=1000000000");
    }
    else if (test.equals("check")) {
      responses = sendBenchRequest(cli,BENCH_START_STR + test + CLUSTER_STR + cli.getCurrCluster() +
          "&count=1000000000");
    }
    for (int i = 0; i < responses.size(); i++) {
      Response response = responses.get(i);
      if (response.status != 200) {
        failed.append(",").append(i);
        anyFailed = true;
      }
    }
    if (!anyFailed) {
      cli.println("Start test successed");
    }
    else {
      cli.println("Start test failed: failed=" + failed.toString());
    }
  }

  void benchStopTest(String command) throws InterruptedException, ExecutionException, IOException {
    String[] parts = command.split(" ");
    String test = parts[2];

    if (test.equals("cluster")) {
      benchStopCluster();
      return;
    }

    boolean anyFailed = false;
    StringBuilder failed = new StringBuilder();
    List<Response> responses = sendBenchRequest(cli,"/bench/stop/" + test);
    for (int i = 0; i < responses.size(); i++) {
      Response response = responses.get(i);
      if (response.status != 200) {
        failed.append(",").append(i);
        anyFailed = true;
      }
    }
    if (!anyFailed) {
      cli.println("Stop successed");
    }
    else {
      cli.println("Stop failed: failed=" + failed.toString());
    }
  }

  void benchstats(String command) throws IOException {
    String[] parts = command.split(" ");
    String test = parts[2];

    List<Response> responses = sendBenchRequest(cli,"/bench/stats/" + test);

    long totalCount = 0;
    long totalErrorCount = 0;
    long totalDuration = 0;
    double minRate = Double.MAX_VALUE;
    int minOffset = 0;
    double maxRate = Double.MIN_VALUE;
    int maxOffset = 0;
    int countReporting = 0;
    int activeThreads = 0;
    int countDead = 0;
    ObjectMapper mapper = new ObjectMapper();
    cli.println("Count returned=" + responses.size());
    for (int i = 0; i < responses.size(); i++) {
      Response response = responses.get(i);
      if (response.status == 200) {
        countReporting++;
        ObjectNode dict = (ObjectNode) mapper.readTree(response.response);
        benchStartTime = dict.get("begin").asLong();
        totalCount += dict.get(COUNT_STR).asLong();
        cli.println("count=" + dict.get(COUNT_STR).asLong() + ", total=" + totalCount);
        totalErrorCount += dict.get("errorCount").asLong();
        totalDuration += dict.get("totalDuration").asLong();
        Long count = dict.get("activeThreads").asLong();
        if (count != null) {
          activeThreads += count;
        }
        if (dict.has("countDead")) {
          Long currDead = dict.get("countDead").asLong();
          if (currDead != null) {
            countDead += currDead;
          }
        }
        double rate = (double)dict.get(COUNT_STR).asLong() / (System.currentTimeMillis() - benchStartTime) * 1000d;
        if (rate < minRate) {
          minRate = rate;
          minOffset = i;
        }
        if (rate > maxRate) {
          maxRate = rate;
          maxOffset = i;
        }
      }
    }
    cli.println("Stats: countReporting=" + countReporting + ", count=" + totalCount + ", errorCount=" + totalErrorCount +
        ", rate=" + String.format("%.2f", (double) totalCount / (double) (System.currentTimeMillis() - benchStartTime) * 1000d) +
        ", errorRate=" + String.format("%.2f", (double) totalErrorCount / (double) (System.currentTimeMillis() - benchStartTime) * 1000d) +
        ", avgDuration=" + String.format("%.2f", totalDuration / (double) totalCount) +
        ", minRate=" + String.format("%.2f", minRate) + ", minOffset=" + minOffset +
        ", maxRate=" + String.format("%.2f", maxRate) + ", maxOffset=" + maxOffset +
        ", activeThreads=" + activeThreads + ", threadsPer=" + (activeThreads / countReporting) +
        ", countDead=" + countDead);
  }

  private static List<Response> sendBenchRequest(Cli cli, final String url) {
    List<Response> responses = new ArrayList<>();
    List<Future<Response>> futures = new ArrayList<>();
    cli.println("bench server count=" + benchUris.size());
    for (int i = 0; i < benchUris.size(); i++) {
      final int offset = i;
      futures.add(benchExecutor.submit(() -> {
        try {
          String benchUri = benchUris.get(offset);

          String fullUri = benchUri + url;
          if (fullUri.contains("?")) {
            fullUri += "&shard=" + offset + "&shardCount=" + benchUris.size();
          }
          else {
            fullUri += "?shard=" + offset + "&shardCount=" + benchUris.size();
          }
          cli.println(fullUri);
          final GetRequest request = Unirest.get(fullUri);

          HttpResponse<String> response = null;
          try {
            response = request.asString();
            if (response.getStatus() != 200) {
              throw new DatabaseException("Error sending bench request: status=" + response.getStatus());
            }

            Response responseObj = new Response();
            responseObj.status = response.getStatus();
            responseObj.response = response.getBody();
            return responseObj;

          }
          catch (UnirestException e) {
            cli.printException(e);
            Response responseObj = new Response();
            responseObj.status = 500;
            return responseObj;
          }
        }
        catch (Exception e) {
          throw new DatabaseException(e);
        }
      }));

    }
    for (Future<Response> future : futures) {
      try {
        responses.add(future.get());
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }
    return responses;
  }

  public void shutdown() {
    if (benchExecutor != null) {
      benchExecutor.shutdownNow();
    }
  }

  static class Response {
    private int status;
    private String response;
  }

  void benchResetStats(String command) {
    String[] parts = command.split(" ");
    String test = parts[2];

    List<Response> responses = sendBenchRequest(cli,"/bench/resetStats/" + test);
    StringBuilder failedNodes = new StringBuilder();
    boolean haveFailed = false;
    for (int i = 0; i < responses.size(); i++) {
      Response response = responses.get(i);
      if (response.status != 200 || !response.response.equals("ok")) {
        failedNodes.append(",").append(i);
        haveFailed = true;
      }
    }
    if (!haveFailed) {
      cli.println("All success: count=" + responses.size());
    }
    else {
      cli.println("Some failed: failed=" + failedNodes.toString());
    }
  }

  void benchHealthcheck() {
    List<Response> responses = sendBenchRequest(cli,"/bench/healthcheck");
    StringBuilder failedNodes = new StringBuilder();
    boolean haveFailed = false;
    for (int i = 0; i < responses.size(); i++) {
      Response response = responses.get(i);
      if (response.status != 200 || !response.response.contains("ok")) {
        failedNodes.append(",").append(i);
        haveFailed = true;
      }
    }
    if (!haveFailed) {
      cli.println("All success: count=" + responses.size());
    }
    else {
      cli.println("Some failed: failed=" + failedNodes.toString());
    }
  }

  private void benchStartCluster() throws IOException, InterruptedException, ExecutionException {
    final String cluster = cli.getCurrCluster();
    if (cluster == null) {
      cli.println("Error, not using a cluster");
      return;
    }

    Config config = cli.getConfig(cluster);
    String dir = config.getString("installDirectory");
    final String installDir = cli.resolvePath(dir);

    benchStopCluster();

    List<Config.Client> clients = config.getClients();
    Thread.sleep(2000);
    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < clients.size(); i++) {
      final Config.Client client = clients.get(i);
      futures.add(benchExecutor.submit((Callable) () -> {
        startBenchServer(config, client.getString(PUBLIC_ADDRESS_STR),
            client.getString("privateAddress"), String.valueOf(client.getInt("port")), installDir, cluster);
        return null;
      }));
    }
    for (Future future : futures) {
      try {
        future.get();
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }
    cli.println("Finished starting servers");

  }

  private void startBenchServer(Config config, String externalAddress, String privateAddress, String port, String installDir,
                                       String cluster) throws IOException, InterruptedException {
    String deployUser = config.getString("user");
    String maxHeap = config.getString("maxJavaHeap");
    if (port == null) {
      port = "9010";
    }
    String searchHome = installDir;
    if (externalAddress.equals("127.0.0.1") || externalAddress.equals("localhost")) {
      maxHeap = cli.getMaxHeap(config);

      if (!searchHome.startsWith("/")) {
        File file = new File(System.getProperty("user.home"), searchHome);
        searchHome = file.getAbsolutePath();
      }
      searchHome = new File(System.getProperty(USER_DIR_STR)).getAbsolutePath();
      ProcessBuilder builder = null;
      if (cli.isCygwin() || cli.isWindows()) {
        cli.println("starting bench server: userDir=" + System.getProperty(USER_DIR_STR));

        builder = new ProcessBuilder().command("bin/start-bench-server-task.bat", port, searchHome);
        Process p = builder.start();
        p.waitFor();
      }
      else {
        builder = new ProcessBuilder().command("bash", "bin/start-bench-server", privateAddress, port, maxHeap, searchHome, cluster);
        builder.start();
      }
      cli.println("Started server: address=" + externalAddress + ", port=" + port + ", maxJavaHeap=" + maxHeap);
      return;
    }
    maxHeap = cli.getMaxHeap(config);
    cli.println("Home=" + searchHome);

    cli.println("1=" + deployUser + "@" + externalAddress + ", 2=" + installDir +
        ", 3=" + privateAddress + ", 4=" + port + ", 5=" + maxHeap + ", 6=" + searchHome +
        ", 7=" + cluster);
    if (cli.isWindows()) {
      cli.println("starting bench server: userDir=" + System.getProperty(USER_DIR_STR));

      File file = new File("bin/remote-start-bench-server.ps1");
      String str = IOUtils.toString(new FileInputStream(file), UTF_8_STR);
      str = str.replaceAll("\\$1", new File(System.getProperty(USER_DIR_STR), "credentials/" + cluster + "-" + cli.getUsername()).getAbsolutePath().replaceAll("\\\\", "/"));
      str = str.replaceAll("\\$2", cli.getUsername());
      str = str.replaceAll("\\$3", externalAddress);
      str = str.replaceAll("\\$4", installDir);
      str = str.replaceAll("\\$5", port);
      File outFile = new File("tmp/" + externalAddress + "-" + port + "-remote-start-bench-server.ps1");
      outFile.getParentFile().mkdirs();
      FileUtils.forceDelete(outFile);
      try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outFile)))) {
        writer.write(str);
      }

      ProcessBuilder builder = new ProcessBuilder().command("powershell", "-F", outFile.getAbsolutePath());
      builder.start();
    }
    else {
      ProcessBuilder builder = new ProcessBuilder().command("bash", "bin/do-start-bench", deployUser + "@" + externalAddress,
          installDir, privateAddress, port, maxHeap, searchHome, cluster);
      cli.println("Started server: address=" + externalAddress + ", port=" + port + ", maxJavaHeap=" + maxHeap);
      Process p = builder.start();
      p.waitFor();
    }
  }
}
