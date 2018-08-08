/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.mashape.unirest.request.GetRequest;
import com.sonicbase.query.DatabaseException;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class BenchHandler {

  private static long benchStartTime;
  private static List<String> benchUris = new ArrayList<>();
  private static ThreadPoolExecutor benchExecutor;
  private final Cli cli;

  public BenchHandler(Cli cli) {
    this.cli = cli;
  }


  public void initBench(String cluster) throws IOException {
    InputStream in = Cli.class.getResourceAsStream("/config-" + cluster + ".json");
    try {
      if (in == null) {
        in = new FileInputStream(new File(System.getProperty("user.dir"), "../config/config-" + cluster + ".json"));
      }
    }
    catch (Exception e) {
      in = new FileInputStream(new File(System.getProperty("user.dir"), "config/config-" + cluster + ".json"));
    }
    String json = IOUtils.toString(in, "utf-8");
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode config = (ObjectNode) mapper.readTree(json);
    ObjectNode databaseDict = config;

    benchUris.clear();
    ArrayNode clients = databaseDict.withArray("clients");
    if (clients != null) {
      if (benchExecutor != null) {
        benchExecutor.shutdownNow();
      }
      benchExecutor = new ThreadPoolExecutor(Math.max(1, clients.size()), Math.max(1, clients.size()), 10000L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
      for (int i = 0; i < clients.size(); i++) {
        ObjectNode replica = (ObjectNode) clients.get(i);
        String externalAddress = replica.get("publicAddress").asText();
        int port = replica.get("port").asInt();
        benchUris.add("http://" + externalAddress + ":" + port);
      }
    }
  }


  public void benchStopCluster() throws IOException, InterruptedException, ExecutionException {
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
    ArrayNode clients = databaseDict.withArray("clients");
    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < clients.size(); i++) {
      final ObjectNode replica = (ObjectNode) clients.get(i);
      futures.add(cli.getExecutor().submit(new Callable() {
        @Override
        public Object call() throws Exception {
          stopBenchServer(databaseDict, replica.get("publicAddress").asText(), replica.get("privateAddress").asText(),
              replica.get("port").asText(), installDir);
          return null;
        }
      }));
    }
    for (Future future : futures) {
      future.get();
    }
    System.out.println("Stopped benchmark cluster");
  }

  private void stopBenchServer(ObjectNode databaseDict, String externalAddress, String privateAddress,
                                      String port, String installDir) throws IOException, InterruptedException {
    String deployUser = databaseDict.get("user").asText();
    if (externalAddress.equals("127.0.0.1") || externalAddress.equals("localhost")) {
      ProcessBuilder builder = null;
      if (cli.isCygwin() || cli.isWindows()) {
        System.out.println("killing windows: port=" + port);
        builder = new ProcessBuilder().command("bin/kill-server.bat", port);
      }
      else {
        builder = new ProcessBuilder().command("bash", "bin/kill-server", "BenchServer", port, port, port, port);
      }
      //builder.directory(workingDir);
      Process p = builder.start();
      p.waitFor();
    }
    else {
      ProcessBuilder builder = null;
      Process p = null;
      if (cli.isWindows()) {
        File file = new File("bin/remote-kill-server.ps1");
        String str = IOUtils.toString(new FileInputStream(file), "utf-8");
        str = str.replaceAll("\\$1", new File(System.getProperty("user.dir"), "credentials/" +
            cli.getCurrCluster() + "-" + cli.getUsername()).getAbsolutePath().replaceAll("\\\\", "/"));
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
                externalAddress, installDir + "/bin/kill-server", "BenchServer", port, port, port, port);
        p = builder.start();
      }
      //builder.directory(workingDir);
      p.waitFor();
    }
  }


  public void benchStartTest(String command) throws InterruptedException, ExecutionException, IOException {
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
      responses = sendBenchRequest("/bench/start/" + test + "?cluster=" + cli.getCurrCluster() +
          "&count=1000000000&offset=" + offset);
    }
    else if (test.equals("delete")) {
      responses = sendBenchRequest("/bench/start/" + test + "?cluster=" + cli.getCurrCluster() +
          "&count=1000000000&offset=" + offset);
    }
    else if (test.equals("aws-insert")) {
      responses = sendBenchRequest("/bench/start/" + test + "?cluster=" + cli.getCurrCluster() +
          "&count=1000000000&offset=" + offset);
    }
    else if (test.equals("aws-delete")) {
      responses = sendBenchRequest("/bench/start/" + test + "?cluster=" + cli.getCurrCluster() +
          "&count=1000000000&offset=" + offset);
    }
    else if (test.equals("kafka-insert")) {
      responses = sendBenchRequest("/bench/start/" + test + "?cluster=" + cli.getCurrCluster() +
          "&count=1000000000&offset=" + offset);
    }
    else if (test.equals("kafka-delete")) {
      responses = sendBenchRequest("/bench/start/" + test + "?cluster=" + cli.getCurrCluster() +
          "&count=1000000000&offset=" + offset);
    }
    else if (test.equals("identity")) {
      responses = sendBenchRequest("/bench/start/" + test + "?cluster=" + cli.getCurrCluster() +
          "&count=1000000000&queryType=" + queryType);
    }
    else if (test.equals("joins")) {
      responses = sendBenchRequest("/bench/start/" + test + "?cluster=" + cli.getCurrCluster() +
          "&count=1000000000&queryType=" + queryType);
    }
    else if (test.equals("range")) {
      responses = sendBenchRequest("/bench/start/" + test + "?cluster=" + cli.getCurrCluster() +
          "&count=1000000000");
    }
    else if (test.equals("check")) {
      responses = sendBenchRequest("/bench/start/" + test + "?cluster=" + cli.getCurrCluster() +
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
      System.out.println("Start test successed");
    }
    else {
      System.out.println("Start test failed: failed=" + failed.toString());
    }
  }

  public void benchStopTest(String command) throws InterruptedException, ExecutionException, IOException {
    String[] parts = command.split(" ");
    String test = parts[2];

    if (test.equals("cluster")) {
      benchStopCluster();
      return;
    }

    boolean anyFailed = false;
    StringBuilder failed = new StringBuilder();
    List<Response> responses = sendBenchRequest("/bench/stop/" + test);
    for (int i = 0; i < responses.size(); i++) {
      Response response = responses.get(i);
      if (response.status != 200) {
        failed.append(",").append(i);
        anyFailed = true;
      }
    }
    if (!anyFailed) {
      System.out.println("Stop successed");
    }
    else {
      System.out.println("Stop failed: failed=" + failed.toString());
    }
  }

  public void benchstats(String command) throws IOException {
    String[] parts = command.split(" ");
    String test = parts[2];

    List<Response> responses = sendBenchRequest("/bench/stats/" + test);

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
    System.out.println("Count returned=" + responses.size());
    for (int i = 0; i < responses.size(); i++) {
      Response response = responses.get(i);
      if (response.status == 200) {
        countReporting++;
        ObjectNode dict = (ObjectNode) mapper.readTree(response.response);
        benchStartTime = dict.get("begin").asLong();
        totalCount += dict.get("count").asLong();
        System.out.println("count=" + dict.get("count").asLong() + ", total=" + totalCount);
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
        double rate = dict.get("count").asLong() / (System.currentTimeMillis() - benchStartTime) * 1000d;
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
    System.out.println("Stats: countReporting=" + countReporting + ", count=" + totalCount + ", errorCount=" + totalErrorCount +
        ", rate=" + String.format("%.2f", (double) totalCount / (double) (System.currentTimeMillis() - benchStartTime) * 1000d) +
        ", errorRate=" + String.format("%.2f", (double) totalErrorCount / (double) (System.currentTimeMillis() - benchStartTime) * 1000d) +
        ", avgDuration=" + String.format("%.2f", totalDuration / (double) totalCount) +
        ", minRate=" + String.format("%.2f", minRate) + ", minOffset=" + minOffset +
        ", maxRate=" + String.format("%.2f", maxRate) + ", maxOffset=" + maxOffset +
        ", activeThreads=" + activeThreads + ", threadsPer=" + (activeThreads / countReporting) +
        ", countDead=" + countDead);
  }

  private static List<Response> sendBenchRequest(final String url) {
    List<Response> responses = new ArrayList<>();
    List<Future<Response>> futures = new ArrayList<>();
    System.out.println("bench server count=" + benchUris.size());
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
          System.out.println(fullUri);
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
            e.printStackTrace();
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

  public void benchResetStats(String command) {
    String[] parts = command.split(" ");
    String test = parts[2];

    List<Response> responses = sendBenchRequest("/bench/resetStats/" + test);
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
      System.out.println("All success: count=" + responses.size());
    }
    else {
      System.out.println("Some failed: failed=" + failedNodes.toString());
    }
  }

  public void benchHealthcheck() {
    List<Response> responses = sendBenchRequest("/bench/healthcheck");
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
      System.out.println("All success: count=" + responses.size());
    }
    else {
      System.out.println("Some failed: failed=" + failedNodes.toString());
    }
  }

    public void benchStartCluster() throws IOException, InterruptedException, ExecutionException {
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
    String dir = databaseDict.get("installDirectory").asText();
    final String installDir = cli.resolvePath(dir);

    benchStopCluster();

    ArrayNode clients = databaseDict.withArray("clients");
    Thread.sleep(2000);
    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < clients.size(); i++) {
      final ObjectNode replica = (ObjectNode) clients.get(i);
      futures.add(benchExecutor.submit((Callable) () -> {
        startBenchServer(databaseDict, replica.get("publicAddress").asText(),
            replica.get("privateAddress").asText(), replica.get("port").asText(), installDir, cluster);
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
    System.out.println("Finished starting servers");

  }

  private void startBenchServer(ObjectNode databaseDict, String externalAddress, String privateAddress, String port, String installDir,
                                       String cluster) throws IOException, InterruptedException {
    String deployUser = databaseDict.get("user").asText();
    String maxHeap = databaseDict.get("maxJavaHeap").asText();
    if (port == null) {
      port = "9010";
    }
    String searchHome = installDir;
    if (externalAddress.equals("127.0.0.1") || externalAddress.equals("localhost")) {
      maxHeap = cli.getMaxHeap(databaseDict);

      if (!searchHome.startsWith("/")) {
        File file = new File(System.getProperty("user.home"), searchHome);
        searchHome = file.getAbsolutePath();
      }
      searchHome = new File(System.getProperty("user.dir")).getAbsolutePath();
      ProcessBuilder builder = null;
      if (cli.isCygwin() || cli.isWindows()) {
        System.out.println("starting bench server: userDir=" + System.getProperty("user.dir"));

        builder = new ProcessBuilder().command("bin/start-bench-server-task.bat", port, searchHome);
        Process p = builder.start();
        p.waitFor();
      }
      else {
        builder = new ProcessBuilder().command("bash", "bin/start-bench-server", privateAddress, port, maxHeap, searchHome, cluster);
        Process p = builder.start();
      }
      System.out.println("Started server: address=" + externalAddress + ", port=" + port + ", maxJavaHeap=" + maxHeap);
      return;
    }
    maxHeap = cli.getMaxHeap(databaseDict);
    System.out.println("Home=" + searchHome);

    System.out.println("1=" + deployUser + "@" + externalAddress + ", 2=" + installDir +
        ", 3=" + privateAddress + ", 4=" + port + ", 5=" + maxHeap + ", 6=" + searchHome +
        ", 7=" + cluster);
    if (cli.isWindows()) {
      System.out.println("starting bench server: userDir=" + System.getProperty("user.dir"));

      File file = new File("bin/remote-start-bench-server.ps1");
      String str = IOUtils.toString(new FileInputStream(file), "utf-8");
      str = str.replaceAll("\\$1", new File(System.getProperty("user.dir"), "credentials/" + cluster + "-" + cli.getUsername()).getAbsolutePath().replaceAll("\\\\", "/"));
      str = str.replaceAll("\\$2", cli.getUsername());
      str = str.replaceAll("\\$3", externalAddress);
      str = str.replaceAll("\\$4", installDir);
      str = str.replaceAll("\\$5", port);
      File outFile = new File("tmp/" + externalAddress + "-" + port + "-remote-start-bench-server.ps1");
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
      ProcessBuilder builder = new ProcessBuilder().command("bash", "bin/do-start-bench", deployUser + "@" + externalAddress,
          installDir, privateAddress, port, maxHeap, searchHome, cluster);
      System.out.println("Started server: address=" + externalAddress + ", port=" + port + ", maxJavaHeap=" + maxHeap);
      Process p = builder.start();
      p.waitFor();
    }
  }


}
