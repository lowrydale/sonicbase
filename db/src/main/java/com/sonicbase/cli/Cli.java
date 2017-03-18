package com.sonicbase.cli;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.mashape.unirest.request.GetRequest;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.DataType;
import com.sonicbase.schema.FieldSchema;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.DataUtil;
import com.sonicbase.util.JsonArray;
import com.sonicbase.util.JsonDict;
import com.sonicbase.util.StreamUtils;
import jline.ConsoleReader;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

import static com.sonicbase.server.DatabaseServer.getMemValue;

public class Cli {

  static String command = "";
  static File workingDir;
  private static String currCluster;
  private static Connection conn;
  private static ResultSet ret;
  private static String address;
  private static int port;
  private static String lastCommand;
  private static String currDbName;
  private static ConsoleReader reader;
  private static long benchStartTime;
  private static ThreadPoolExecutor benchExecutor;

  public static void main(final String[] args) throws IOException, InterruptedException {
    workingDir = new File(System.getProperty("user.dir"));
    try {
      reader = new ConsoleReader();
      if (args.length > 0) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < args.length; i++) {
          builder.append(args[i] + " ");
        }
        runCommand(builder.toString());
        System.exit(0);
      }

      System.out.print("\033[2J\033[;H");

      moveToBottom();

      while (true) {
        while (true) {


          //       int c = (char) reader.readCharacter(new char[]{65,'\n','a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z',' '});//System.in.read();

          //        if (c == 91) {
          //          previousCommand();
          //          break;
          //        }
          //        else {
          //String str = reader.readLine();
          //        int c = System.in.read();
          //        if (c == 65) {
          // //         previousCommand();
          //          continue;
          //        }
          //System.out.print((char)c);
          String str = reader.readLine();//String.valueOf((char)c);
          //System.out.print(str);//String.valueOf((char)c));
          //String s = String.valueOf(c);//stdin.read());
          command += str;
          break;
          //          if (command.endsWith("\n")) {
          //            break;
          //          }
          // }

        }
        //clear screen
        moveToBottom();
        if (command != null && command.length() > 0) {
          runCommand(command);
          command = "";
        }
        //clear screen
        moveToBottom();
      }
    }
    catch (ExitCliException e) {
      return;
    }
  }

  private static List<String> benchUris = new ArrayList<>();

  private static void initBench(String cluster) throws IOException {
    InputStream in = Cli.class.getResourceAsStream("/config-" + cluster + ".json");
    if (in == null) {
      in = new FileInputStream("/Users/lowryda/database/config/config-" + cluster + ".json");
    }
    String json = StreamUtils.inputStreamToString(in);
    JsonDict config = new JsonDict(json);
    JsonDict databaseDict = config.getDict("database");

    JsonArray clients = databaseDict.getArray("clients");
    if (clients != null) {
      System.out.println("Configuring " + clients.size() + " bench clients");
      if (benchExecutor != null) {
        benchExecutor.shutdownNow();
      }
      benchExecutor = new ThreadPoolExecutor(clients.size(), clients.size(), 10000L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
      for (int i = 0; i < clients.size(); i++) {
        JsonDict replica = clients.getDict(i);
        String externalAddress = replica.getString("publicAddress");
        String port = replica.getString("port");
        benchUris.add("http://" + externalAddress + ":" + port);
      }
    }
  }

  private static List<String> commandHistory = new ArrayList<>();

  private static void writeHeader(String width) {
    System.out.print("\033[" + 0 + ";0f");
    System.out.print("\033[38;5;195m");
    System.out.print("\033[48;5;33m");

    StringBuilder builder = new StringBuilder();
    String using = "Using: ";
    if (currCluster == null) {
      using += "<cluster=none>";
    }
    else {
      using += currCluster;
    }
    if (currDbName == null) {
      using += ".<db=none>";
    }
    else {
      using += "." + currDbName;
    }
    builder.append("SonicBase Client");
    for (int i = "SonicBase Client".length(); i < Integer.valueOf(width) - using.length(); i++) {
      builder.append(" ");
    }
    builder.append(using);
    System.out.print(builder.toString());
    System.out.print("\033[49;39m");
  }

  private static void moveToBottom() throws IOException, InterruptedException {

    String str = getTerminalSize();
    String[] parts = str.split(",");
    String width = parts[0];
    String height = parts[1];

    writeHeader(width);

//    for (int i = 0; i < Integer.valueOf(height) - 1; i++) {
//      System.out.println();
//    }
    int h = Integer.valueOf(height);
    System.out.print("\033[" + h + ";" + 0 + "f");
  }

  private static String getTerminalSize() throws IOException, InterruptedException {
    ProcessBuilder builder = new ProcessBuilder().command("bin/terminal-size.py");
    //builder.directory(workingDir);
    Process p = builder.start();
    p.waitFor();
    File file = new File("bin/size.txt");
    return StreamUtils.inputStreamToString(new FileInputStream(file));
  }


  private static void previousCommand() {
    String command = commandHistory.get(0);
    System.out.print(command);
  }

  private static void runCommand(String command) throws IOException, InterruptedException {
    try {
      command = command.trim();
      commandHistory.add(command);

      if (command.startsWith("deploy cluster")) {
        deploy();
      }
      else if (command.startsWith("start cluster")) {
        startCluster();
      }
      else if (command.startsWith("stop cluster")) {
        stopCluster();
      }
      else if (command.startsWith("start shard")) {
        int pos = command.lastIndexOf(" ");
        String shard = command.substring(pos + 1);
        startShard(Integer.valueOf(shard));
      }
      else if (command.startsWith("stop shard")) {
        int pos = command.lastIndexOf(" ");
        String shard = command.substring(pos + 1);
        stopShard(Integer.valueOf(shard));
      }
      else if (command.startsWith("purge cluster")) {
        purgeCluster();
      }
      else if (command.startsWith("quit") || command.startsWith("exit")) {
        exit();
      }
      else if (command.startsWith("echo")) {
        moveToBottom();
        System.out.print(command);
      }
      else if (command.startsWith("clear")) {
        System.out.print("\033[2J\033[;H");
        moveToBottom();
      }
      else if (command.startsWith("readdress servers")) {
        readdressServers();
      }
      else if (command.startsWith("use cluster")) {
        int pos = command.lastIndexOf(" ");
        String cluster = command.substring(pos + 1);
        useCluster(cluster);
      }
      else if (command.startsWith("use database")) {
        int pos = command.lastIndexOf(" ");
        String dbName = command.substring(pos + 1);
        useDatabase(dbName);
      }
      else if (command.startsWith("create database")) {
        int pos = command.lastIndexOf(" ");
        String dbName = command.substring(pos + 1);
        createDatabase(dbName);
      }
      else if (command.startsWith("select")) {
        System.out.print("\033[2J\033[;H");
        select(command);
      }
      else if (command.startsWith("next")) {
        System.out.print("\033[2J\033[;H");
        next();
      }
      else if (command.startsWith("force rebalance")) {
        forceRebalance();
      }
      else if (command.startsWith("debug record")) {
        debugRecord(command);
      }
      else if (command.startsWith("insertWithRecord")) {
        System.out.print("\033[2J\033[;H");
        insert(command);
      }
      else if (command.startsWith("update")) {
        System.out.print("\033[2J\033[;H");
        update(command);
      }
      else if (command.startsWith("delete")) {
        System.out.print("\033[2J\033[;H");
        delete(command);
      }
      else if (command.startsWith("truncate")) {
        System.out.print("\033[2J\033[;H");
        truncate(command);
      }
      else if (command.startsWith("drop")) {
        System.out.print("\033[2J\033[;H");
        drop(command);
      }
      else if (command.startsWith("create")) {
        System.out.print("\033[2J\033[;H");
        create(command);
      }
      else if (command.startsWith("alter")) {
        System.out.print("\033[2J\033[;H");
        alter(command);
      }
      else if (command.trim().equals("help")) {
        help();
      }
      else if (command.trim().startsWith("describe")) {
        System.out.print("\033[2J\033[;H");
        describe(command);
      }
      else if (command.trim().startsWith("explain")) {
        System.out.print("\033[2J\033[;H");
        explain(command);
      }
      else if (command.startsWith("reconfigure cluster")) {
        reconfigureCluster();
      }
      else if (command.startsWith("stop cluster")) {
        stopCluster();
      }
      else if (command.startsWith("bench start cluster")) {
        benchStartCluster();
      }
      else if (command.startsWith("bench stop cluster")) {
        benchStopCluster();
      }
      else if (command.startsWith("bench healthcheck")) {
        benchHealthcheck();
      }
      else if (command.startsWith("bench start")) {
        benchStartTest(command);
      }
      else if (command.startsWith("bench stop")) {
        benchStopTest(command);
      }
      else if (command.startsWith("bench stats")) {
        benchstats(command);
      }
      else if (command.startsWith("bench resetStats")) {
        benchResetStats(command);
      }
      else if (command.startsWith("partitioning")) {
        getPartitionSizes();
      }
      else {
        System.out.println("Error, unknown command");
      }
    }
    catch (ExitCliException e) {
      throw e;
    }
    catch (Exception e) {
      e.printStackTrace();
      System.out.println("Error executing command: msg=" + e.getMessage());
    }
  }

  private static void benchStopCluster() throws IOException, InterruptedException {
    String cluster = currCluster;
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }

    String json = StreamUtils.inputStreamToString(Cli.class.getResourceAsStream("/config-" + cluster + ".json"));
    JsonDict config = new JsonDict(json);
    JsonDict databaseDict = config.getDict("database");
    String installDir = databaseDict.getString("installDirectory");
    installDir = resolvePath(installDir);
    JsonArray clients = databaseDict.getArray("clients");
    for (int i = 0; i < clients.size(); i++) {
      JsonDict replica = clients.getDict(i);
      stopBenchServer(databaseDict, replica.getString("publicAddress"), replica.getString("privateAddress"), replica.getString("port"), installDir);
    }
    System.out.println("Stopped benchmark cluster");
  }

  private static void benchStartCluster() throws IOException, InterruptedException {
    final String cluster = currCluster;
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }

    String json = StreamUtils.inputStreamToString(Cli.class.getResourceAsStream("/config-" + cluster + ".json"));
    JsonDict config = new JsonDict(json);
    final JsonDict databaseDict = config.getDict("database");
    String dir = databaseDict.getString("installDirectory");
    final String installDir = resolvePath(dir);
    JsonArray clients = databaseDict.getArray("clients");
    for (int i = 0; i < clients.size(); i++) {
      JsonDict replica = clients.getDict(i);
      stopBenchServer(databaseDict, replica.getString("publicAddress"), replica.getString("privateAddress"), replica.getString("port"), installDir);
    }
    Thread.sleep(2000);
    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < clients.size(); i++) {
      final JsonDict replica = clients.getDict(i);
      futures.add(benchExecutor.submit(new Callable(){
        @Override
        public Object call() throws Exception {
          startBenchServer(databaseDict, replica.getString("publicAddress"), replica.getString("privateAddress"), replica.getString("port"), installDir, cluster);
          return null;
        }
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

  private static void benchResetStats(String command) {
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

  private static void benchHealthcheck() {
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

  private static void getPartitionSizes() throws IOException, SQLException, ClassNotFoundException {
    initConnection();
    DatabaseClient client = ((ConnectionProxy) conn).getDatabaseClient();
    for (Map.Entry<String, TableSchema> table : client.getCommon().getTables(currDbName).entrySet()) {
      for (Map.Entry<String, IndexSchema> indexSchema : table.getValue().getIndexes().entrySet()) {
        int shard = 0;
        for (TableSchema.Partition partition : indexSchema.getValue().getCurrPartitions()) {
          //if (table.getKey().equals("persons")) {
          StringBuilder builder = new StringBuilder("[");
          if (partition.getUpperKey() == null) {
            builder.append("null");
          }
          else {
            for (Object obj : partition.getUpperKey()) {
              builder.append(",").append(obj);
            }
          }
          builder.append("]");
          System.out.println("Table=" + table.getKey() + ", Index=" + indexSchema.getKey() + ", shard=" + shard + ", key=" +
                builder.toString());
          //}
          shard++;
        }

        String command = "DatabaseServer:getPartitionSize:1:" + client.getCommon().getSchemaVersion() + ":" +
            currDbName + ":" + table.getKey() + ":" + indexSchema.getKey();
        Random rand = new Random(System.currentTimeMillis());
        for (int i = 0; i < client.getShardCount(); i++) {
          shard = i;
          byte[] ret = client.send(null, shard, rand.nextLong(), command, null, DatabaseClient.Replica.master);
          DataInputStream in = new DataInputStream(new ByteArrayInputStream(ret));
          long serializationVersion = DataUtil.readVLong(in);
          long count = in.readLong();
          System.out.println("Table=" + table.getKey() + ", Index=" + indexSchema.getKey() + ", Shard=" + shard + ", count=" + count);
        }
      }
    }

  }

  private static void benchstats(String command) {
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
    for (int i = 0; i < responses.size(); i++) {
      Response response = responses.get(i);
      if (response.status == 200) {
        countReporting++;
        JsonDict dict = new JsonDict(response.response);
        totalCount += dict.getLong("count");
        totalErrorCount += dict.getLong("errorCount");
        totalDuration += dict.getLong("totalDuration");
        double rate = dict.getLong("count") / (System.currentTimeMillis() - benchStartTime) * 1000d;
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
      ", rate=" + (double)totalCount / (double)(System.currentTimeMillis() - benchStartTime) * 1000d +
        ", errorRate=" + (double)totalErrorCount / (double)(System.currentTimeMillis() - benchStartTime) * 1000d +
        ", avgDuration=" + totalDuration / (double)totalCount +
        ", minRate=" + minRate + ", minOffset=" + minOffset +
        ", maxRate=" + maxRate + ", maxOffset=" + maxOffset);
  }

  private static void benchStopTest(String command) {
    String[] parts = command.split(" ");
    String test = parts[2];

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

  private static void benchStartTest(String command) {
    String[] parts = command.split(" ");
    String test = parts[2];
    String queryType = null;
    if (parts.length > 3) {
      queryType = parts[3];
    }

    StringBuilder failed = new StringBuilder();
    boolean anyFailed = false;
    benchStartTime = System.currentTimeMillis();
    List<Response> responses = null;
    if (test.equals("insert")) {
      responses = sendBenchRequest("/bench/start/" + test + "?cluster=" + currCluster +
          "&count=1000000000");
    }
    else if (test.equals("identity")) {
      responses = sendBenchRequest("/bench/start/" + test + "?cluster=" + currCluster +
          "&count=1000000&queryType=" + queryType);
    }
    else if (test.equals("joins")) {
      responses = sendBenchRequest("/bench/start/" + test + "?cluster=" + currCluster +
          "&count=1000000&queryType=" + queryType);
    }
    else if (test.equals("range")) {
      responses = sendBenchRequest("/bench/start/" + test + "?cluster=" + currCluster +
          "&count=1000000");
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

  static class Response {
    private int status;
    private String response;
  }

  private static List<Response> sendBenchRequest(final String url) {
    List<Response> responses = new ArrayList<>();
    List<Future<Response>> futures = new ArrayList<>();
    System.out.println("bench server count=" + benchUris.size());
    for (int i = 0; i < benchUris.size(); i++) {
      final int offset = i;
      futures.add(benchExecutor.submit(new Callable<Response>(){
        @Override
        public Response call() throws Exception {
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

  private static void exit() throws SQLException {
    if (conn != null) {
      conn.close();
    }
    if (benchExecutor != null) {
      benchExecutor.shutdownNow();
    }
    throw new ExitCliException();
  }

  static class ExitCliException extends RuntimeException {

  }

  private static void describe(String command) throws SQLException, ClassNotFoundException, IOException, InterruptedException {
    String cluster = currCluster;
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }

    if (currDbName == null) {
      System.out.println("Error, not using a database");
      return;
    }

    initConnection();

    System.out.println("Executing select request");


    DatabaseClient client = ((ConnectionProxy) conn).getDatabaseClient();
    PreparedStatement stmt = conn.prepareStatement(command);
    ret = stmt.executeQuery();

    String str = getTerminalSize();
    String[] parts = str.split(",");
    String height = parts[1];

    int currLine = 0;
    System.out.println();
    for (int i = 0; currLine < Integer.valueOf(height) - 2; i++) {
      if (!ret.next()) {
        break;
      }
      System.out.println(ret.getString(1));
      currLine++;
    }
    lastCommand = command;
  }

  private static void explain(String command) throws SQLException, ClassNotFoundException, IOException, InterruptedException {
    String cluster = currCluster;
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }

    if (currDbName == null) {
      System.out.println("Error, not using a database");
      return;
    }

    initConnection();

    System.out.println("Executing explain request");


    DatabaseClient client = ((ConnectionProxy) conn).getDatabaseClient();
    PreparedStatement stmt = conn.prepareStatement(command);
    ret = stmt.executeQuery();

    String str = getTerminalSize();
    String[] parts = str.split(",");
    String height = parts[1];

    int currLine = 0;
    System.out.println();
    for (int i = 0; currLine < Integer.valueOf(height) - 2; i++) {
      if (!ret.next()) {
        break;
      }
      System.out.println(ret.getString(1));
      currLine++;
    }
    lastCommand = command;
  }


  private static void debugRecord(String command) {
    String cluster = currCluster;
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }
    if (currDbName == null) {
      System.out.println("Error, not using a database");
      return;
    }

    command = command.substring("debug record ".length());
    int pos = command.indexOf(" ");
    String tableName = command.substring(0, pos);
    int pos2 = command.indexOf(" ", pos + 1);
    String indexName = command.substring(pos + 1, pos2);
    String key = command.substring(pos2 + 1);
    String ret = ((ConnectionProxy) conn).getDatabaseClient().debugRecord(currDbName, tableName, indexName, key);
    System.out.println(ret);
  }

  private static void forceRebalance() {
    String cluster = currCluster;
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }
    if (currDbName == null) {
      System.out.println("Error, not using a database");
      return;
    }

    String command = "DatabaseServer:beginRebalance:1:1:" + currDbName + ":" + true;
    ((ConnectionProxy) conn).getDatabaseClient().send(null, 0, 0, command, null, DatabaseClient.Replica.master);
  }

  private static void stopShard(int shardOffset) throws IOException, InterruptedException {
    String cluster = currCluster;
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }
    String json = StreamUtils.inputStreamToString(Cli.class.getResourceAsStream("/config-" + cluster + ".json"));
    JsonDict config = new JsonDict(json);
    JsonDict databaseDict = config.getDict("database");
    String installDir = databaseDict.getString("installDirectory");
    installDir = resolvePath(installDir);
    JsonArray shards = databaseDict.getArray("shards");
    JsonDict shard = shards.getDict(shardOffset);
    JsonArray replicas = shard.getArray("replicas");
    for (int j = 0; j < replicas.size(); j++) {
      JsonDict replica = replicas.getDict(j);
      stopServer(databaseDict, replica.getString("publicAddress"), replica.getString("privateAddress"), replica.getString("port"), installDir);
    }
  }

  private static void stopCluster() throws IOException, InterruptedException {
    String cluster = currCluster;
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }
    String json = StreamUtils.inputStreamToString(Cli.class.getResourceAsStream("/config-" + cluster + ".json"));
    JsonDict config = new JsonDict(json);
    JsonDict databaseDict = config.getDict("database");
    String installDir = databaseDict.getString("installDirectory");
    installDir = resolvePath(installDir);
    JsonArray shards = databaseDict.getArray("shards");
    for (int i = 0; i < shards.size(); i++) {
      JsonDict shard = shards.getDict(i);
      JsonArray replicas = shard.getArray("replicas");
      for (int j = 0; j < replicas.size(); j++) {
        JsonDict replica = replicas.getDict(j);
        stopServer(databaseDict, replica.getString("publicAddress"), replica.getString("privateAddress"), replica.getString("port"), installDir);
        System.out.println("Stopped server: address=" + replica.getString("publicAddress") + ", port=" + replica.getString("port"));
      }
    }
    System.out.println("Stopped cluster");
  }

  private static void reconfigureCluster() throws SQLException, ClassNotFoundException, IOException, InterruptedException, ExecutionException {
    String cluster = currCluster;
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }
    closeConnection();
    initConnection();
    deploy();
    DatabaseClient client = ((ConnectionProxy) conn).getDatabaseClient();
    DatabaseClient.ReconfigureResults results = client.reconfigureCluster();
    if (!results.isHandedOffToMaster()) {
      System.out.println("Must start servers to reconfigure the cluster");
    }
    else {
      int shardCount = results.getShardCount();
      if (shardCount > 0) {
        String json = StreamUtils.inputStreamToString(Cli.class.getResourceAsStream("/config-" + cluster + ".json"));
        JsonDict config = new JsonDict(json);
        JsonDict databaseDict = config.getDict("database");
        JsonArray shards = databaseDict.getArray("shards");
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

  private static void startShard(int shardOffset) throws IOException, InterruptedException {
    String cluster = currCluster;
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }
    String json = StreamUtils.inputStreamToString(Cli.class.getResourceAsStream("/config-" + cluster + ".json"));
    JsonDict config = new JsonDict(json);
    JsonDict databaseDict = config.getDict("database");
    String installDir = databaseDict.getString("installDirectory");
    installDir = resolvePath(installDir);
    JsonArray shards = databaseDict.getArray("shards");
    JsonDict shard = shards.getDict(shardOffset);
    JsonArray replicas = shard.getArray("replicas");
    for (int j = 0; j < replicas.size(); j++) {
      JsonDict replica = replicas.getDict(j);
      stopServer(databaseDict, replica.getString("publicAddress"), replica.getString("privateAddress"), replica.getString("port"), installDir);
    }
    Thread.sleep(2000);
    replicas = shard.getArray("replicas");
    for (int j = 0; j < replicas.size(); j++) {
      JsonDict replica = replicas.getDict(j);
      startServer(databaseDict, replica.getString("publicAddress"), replica.getString("privateAddress"), replica.getString("port"), installDir, cluster);
    }
    System.out.println("Finished starting servers");
  }

  static class HelpItem {
    private String command;
    private String shortDescription;
    private String longDescription;

    public HelpItem(String command, String shortDescription, String longDescription) {
      this.command = command;
      this.shortDescription = shortDescription;
      this.longDescription = longDescription;
    }
  }

  private static List<HelpItem> helpItems = new ArrayList<>();

  static {
    helpItems.add(new HelpItem("deploy cluster", "deploys code and configuration to the servers", ""));
    helpItems.add(new HelpItem("start cluster", "stops and starts all the servers in the cluster", ""));
    helpItems.add(new HelpItem("use cluster", "marks the active cluster to use for subsequent commands", ""));
    helpItems.add(new HelpItem("use database", "marks the active database to use for subsequent commands", ""));
    helpItems.add(new HelpItem("create database", "creates a new database in the current cluster", ""));
    helpItems.add(new HelpItem("purge cluster", "removes all data associated with the cluster", ""));
    helpItems.add(new HelpItem("select", "executes a sql select statement", ""));
    helpItems.add(new HelpItem("insert", "insert a record into a table", ""));
    helpItems.add(new HelpItem("update", "update a record", ""));
    helpItems.add(new HelpItem("delete", "deletes the specified records", ""));
    helpItems.add(new HelpItem("describe", "describes a table", ""));
  }

  private static void help() {
    helpItems.sort(new Comparator<HelpItem>() {
      @Override
      public int compare(HelpItem o1, HelpItem o2) {
        return o1.command.compareTo(o2.command);
      }
    });
    for (HelpItem item : helpItems) {
      System.out.println(item.command + " - " + item.shortDescription);
    }
  }

  private static void useDatabase(String dbName) throws SQLException, ClassNotFoundException {
    String cluster = currCluster;
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }
    closeConnection();
    currDbName = dbName.trim().toLowerCase();
    //initConnection();
  }

  private static void closeConnection() throws SQLException {
    if (conn != null) {
      conn.close();
      conn = null;
    }
  }

  private static void createDatabase(String dbName) throws SQLException, ClassNotFoundException {
    String cluster = currCluster;
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }
    initConnection();
    dbName = dbName.trim().toLowerCase();
    ((ConnectionProxy) conn).createDatabase(dbName);
    System.out.println("Successfully created database: name=" + dbName);
    useDatabase(dbName);
  }

  static class SelectColumn {
    private Integer columnOffset;
    private String name;
    private DataType.Type type;

    public SelectColumn(String name, DataType.Type type) {
      this.name = name;
      this.type = type;
    }

    public SelectColumn(String name, DataType.Type type, int columnOffset) {
      this.name = name;
      this.type = type;
      this.columnOffset = columnOffset;
    }
  }

  private static void next() throws SQLException, JSQLParserException, IOException, InterruptedException {
    DatabaseClient client = ((ConnectionProxy) conn).getDatabaseClient();
    if (lastCommand.startsWith("describe")) {
      String str = getTerminalSize();
      String[] parts = str.split(",");
      String height = parts[1];

      int currLine = 0;
      System.out.println();
      for (int i = 0; currLine < Integer.valueOf(height) - 2; i++) {
        if (!ret.next()) {
          break;
        }
        System.out.println(ret.getString(1));
        currLine++;
      }
    }
    else {
      processResults(lastCommand, client);
    }
  }

  private static void update(String command) throws SQLException, ClassNotFoundException {
    String cluster = currCluster;
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }

    if (currDbName == null) {
      System.out.println("Error, not using a database");
      return;
    }

    initConnection();

    System.out.println("Executing update request");

    PreparedStatement stmt = conn.prepareStatement(command);
    int count = stmt.executeUpdate();
    System.out.println("Finished update: count=" + count);
  }

  private static void insert(String command) throws SQLException, ClassNotFoundException {
    String cluster = currCluster;
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }

    if (currDbName == null) {
      System.out.println("Error, not using a database");
      return;
    }

    initConnection();

    System.out.println("Executing insert request");

    PreparedStatement stmt = conn.prepareStatement(command);
    int count = stmt.executeUpdate();
    System.out.println("Finished insert: count=" + count);
  }

  private static void delete(String command) throws SQLException, ClassNotFoundException {
    String cluster = currCluster;
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }

    if (currDbName == null) {
      System.out.println("Error, not using a database");
      return;
    }

    initConnection();

    System.out.println("Executing delete request");

    PreparedStatement stmt = conn.prepareStatement(command);
    int count = stmt.executeUpdate();
    System.out.println("Finished delete: count=" + count);
  }

  private static void truncate(String command) throws SQLException, ClassNotFoundException {
    String cluster = currCluster;
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }

    if (currDbName == null) {
      System.out.println("Error, not using a database");
      return;
    }

    initConnection();

    System.out.println("Executing truncate request");

    PreparedStatement stmt = conn.prepareStatement(command);
    int count = stmt.executeUpdate();
    System.out.println("Finished truncate: count=" + count);
  }

  private static void drop(String command) throws SQLException, ClassNotFoundException {
    String cluster = currCluster;
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }

    if (currDbName == null) {
      System.out.println("Error, not using a database");
      return;
    }

    initConnection();

    System.out.println("Executing drop request");

    PreparedStatement stmt = conn.prepareStatement(command);
    int count = stmt.executeUpdate();
    System.out.println("Finished drop: count=" + count);
  }

  private static void create(String command) throws SQLException, ClassNotFoundException {
    String cluster = currCluster;
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }

    if (currDbName == null) {
      System.out.println("Error, not using a database");
      return;
    }

    initConnection();

    System.out.println("Executing create request");

    PreparedStatement stmt = conn.prepareStatement(command);
    int count = stmt.executeUpdate();
    System.out.println("Finished create: count=" + count);
  }

  private static void alter(String command) throws SQLException, ClassNotFoundException {
    String cluster = currCluster;
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }

    if (currDbName == null) {
      System.out.println("Error, not using a database");
      return;
    }

    initConnection();

    System.out.println("Executing alter request");

    PreparedStatement stmt = conn.prepareStatement(command);
    int count = stmt.executeUpdate();
    System.out.println("Finished alter: count=" + count);
  }

  private static void select(String command) throws SQLException, JSQLParserException, ClassNotFoundException, IOException, InterruptedException {
    String cluster = currCluster;
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }

    if (currDbName == null) {
      System.out.println("Error, not using a database");
      return;
    }

    initConnection();

    System.out.println("Executing select request");


    DatabaseClient client = ((ConnectionProxy) conn).getDatabaseClient();
    PreparedStatement stmt = conn.prepareStatement(command);
    ret = stmt.executeQuery();

    lastCommand = command;
    processResults(command, client);

  }

  private static void initConnection() throws ClassNotFoundException, SQLException {
    if (conn == null) {
      Class.forName("com.sonicbase.jdbcdriver.Driver");

      String db = "";
      if (currDbName != null) {
        db = "/" + currDbName;
      }
      conn = DriverManager.getConnection("jdbc:sonicbase:" + address + ":" + port + db);
    }
  }

  private static void processResults(String command, DatabaseClient client) throws JSQLParserException, SQLException, IOException, InterruptedException {
    List<SelectColumn> columns = new ArrayList<>();
    CCJSqlParserManager parser = new CCJSqlParserManager();
    net.sf.jsqlparser.statement.Statement statement = parser.parse(new StringReader(command));
    Select select = (Select) statement;
    SelectBody selectBody = select.getSelectBody();
    if (selectBody instanceof PlainSelect) {
      PlainSelect pselect = (PlainSelect) selectBody;
      String fromTable = ((Table) pselect.getFromItem()).getName();
      int columnOffset = 1;
      List<SelectItem> selectItemList = pselect.getSelectItems();
      for (SelectItem selectItem : selectItemList) {
        if (selectItem instanceof AllColumns) {
          for (FieldSchema field : client.getCommon().getTables(currDbName).get(fromTable).getFields()) {
            columns.add(new SelectColumn(field.getName(), field.getType()));
          }
        }
        else {
          SelectExpressionItem item = (SelectExpressionItem) selectItem;

          Alias alias = item.getAlias();
          String columnName = null;
          if (alias != null) {
            columnName = alias.getName();
          }
          Expression expression = item.getExpression();
          if (expression instanceof Function) {
            Function function = (Function) expression;
            columnName = function.getName();
            columns.add(new SelectColumn(columnName, DataType.Type.VARCHAR, columnOffset));
          }
          else if (item.getExpression() instanceof Column) {
            String tableName = ((Column) item.getExpression()).getTable().getName();
            if (tableName == null) {
              tableName = fromTable;
            }
            String actualColumnName = ((Column) item.getExpression()).getColumnName();
            if (columnName == null) {
              columnName = actualColumnName;
            }
            Integer offset = client.getCommon().getTables(currDbName).get(tableName).getFieldOffset(actualColumnName);
            if (offset != null) {
              int fieldOffset = offset;
              FieldSchema fieldSchema = client.getCommon().getTables(currDbName).get(tableName).getFields().get(fieldOffset);
              DataType.Type type = fieldSchema.getType();
              columns.add(new SelectColumn(columnName, type));
            }
          }
          columnOffset++;
        }
      }
    }

    String str = getTerminalSize();
    String[] parts = str.split(",");
    String width = parts[0];
    String height = parts[1];

    List<List<String>> data = new ArrayList<>();

    for (int i = 0; i < Integer.valueOf(height) - 6; i++) {
      List<String> line = new ArrayList<>();

      if (!ret.next()) {
        break;
      }
      for (SelectColumn column : columns) {
        if (column.columnOffset != null) {
          str = ret.getString(column.columnOffset);
        }
        else {
          str = ret.getString(column.name);
        }
        line.add(str == null ? "<null>" : str);
      }
      data.add(line);
    }

    StringBuilder builder = new StringBuilder();

    int totalWidth = 0;
    List<Integer> columnWidths = new ArrayList<>();
    for (int i = 0; i < columns.size(); i++) {
      int currWidth = 0;
      for (int j = 0; j < data.size(); j++) {
        currWidth = Math.min(33, Math.max(columns.get(i).name.length(), Math.max(currWidth, data.get(j).get(i).length())));
      }
      if (totalWidth + currWidth + 3 > Integer.valueOf(width)) {
        columnWidths.add(-1);
      }
      else {
        totalWidth += currWidth + 3;
        columnWidths.add(currWidth);
      }
    }

    totalWidth += 1;
    appendChar(builder, "-", totalWidth);
    builder.append("\n");
    for (int i = 0; i < columns.size(); i++) {
      SelectColumn column = columns.get(i);
      if (columnWidths.get(i) == -1) {
        continue;
      }
      builder.append("| ");
      String value = column.name;
      if (value.length() > 30) {
        value = value.substring(0, 30);
        value += "...";
      }
      builder.append(value);
      appendChar(builder, " ", columnWidths.get(i) - value.length() + 1);
    }
    builder.append("|\n");
    appendChar(builder, "-", totalWidth);
    builder.append("\n");

    for (int i = 0; i < data.size(); i++) {
      List<String> line = data.get(i);
      for (int j = 0; j < line.size(); j++) {
        if (columnWidths.get(j) == -1) {
          continue;
        }
        String value = line.get(j);
        if (line.get(j).length() > 30) {
          value = line.get(j).substring(0, 30);
          value += "...";
        }
        builder.append("| ");
        builder.append(value);
        appendChar(builder, " ", columnWidths.get(j) - value.length() + 1);
      }
      builder.append("|\n");
    }
    appendChar(builder, "-", totalWidth);
    System.out.println(builder.toString());
  }

  private static void appendChar(StringBuilder builder, String c, int count) {
    for (int i = 0; i < count; i++) {
      builder.append(c);
    }
  }

  private static void useCluster(String cluster) throws ClassNotFoundException, SQLException, IOException {
    cluster = cluster.trim();

    currCluster = cluster;

    initBench(cluster);


    closeConnection();
    currDbName = null;

    String json = null;
    try {
      json = StreamUtils.inputStreamToString(Cli.class.getResourceAsStream("/config-" + cluster + ".json"));
    }
    catch (Exception e) {
      json = StreamUtils.inputStreamToString(new FileInputStream(new File(System.getProperty("user.dir"), "config/config-" + cluster + ".json")));
    }
    JsonDict config = new JsonDict(json);
    JsonDict databaseDict = config.getDict("database");
    Boolean clientIsPrivate = config.getBoolean("clientIsPrivate");
    if (clientIsPrivate == null) {
      clientIsPrivate = false;
    }
    JsonArray shards = databaseDict.getArray("shards");
    JsonDict replica = shards.getDict(0).getArray("replicas").getDict(0);
    if (clientIsPrivate) {
      address = replica.getString("privateAddress");
    }
    else {
      address = replica.getString("publicAddress");
    }
    port = replica.getInt("port");
  }

  private static void purgeCluster() throws IOException, InterruptedException {
    String cluster = currCluster;
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }

    System.out.println("Starting purge: cluster=" + cluster);
    String json = StreamUtils.inputStreamToString(Cli.class.getResourceAsStream("/config-" + cluster + ".json"));
    JsonDict config = new JsonDict(json);
    JsonDict databaseDict = config.getDict("database");
    String dataDir = databaseDict.getString("dataDirectory");
    dataDir = resolvePath(dataDir);
    String installDir = databaseDict.getString("installDirectory");
    installDir = resolvePath(installDir);
    JsonArray shards = databaseDict.getArray("shards");
    for (int i = 0; i < shards.size(); i++) {
      JsonArray replicas = shards.getDict(i).getArray("replicas");
      for (int j = 0; j < replicas.size(); j++) {
        JsonDict replica = replicas.getDict(j);
        String deployUser = databaseDict.getString("user");
        String publicAddress = replica.getString("publicAddress");
        stopServer(databaseDict, publicAddress, replica.getString("privateAddress"), replica.getString("port"), installDir);
        if (publicAddress.equals("127.0.0.1") || publicAddress.equals("localhost")) {
          File file = new File(dataDir);
          if (!dataDir.startsWith("/")) {
            file = new File(System.getProperty("user.home"), dataDir);
          }
          System.out.println("Deleting directory: dir=" + file.getAbsolutePath());
          FileUtils.deleteDirectory(file);
        }
        else {
          ProcessBuilder builder = new ProcessBuilder().command("ssh", "-n", "-f", "-o",
              "UserKnownHostsFile=/dev/null", "-o", "StrictHostKeyChecking=no", deployUser + "@" +
                  replica.getString("publicAddress"), "rm", "-rf", dataDir);
          System.out.println("purging: address=" + replica.getString("publicAddress") + ", dir=" + dataDir);
          //builder.directory(workingDir);
          builder.start();
        }
      }
    }
    System.out.println("Finished purging: cluster=" + cluster);
  }

  private static void readdressServers() throws IOException, InterruptedException, SQLException, ClassNotFoundException, ExecutionException {
    deploy();
    startCluster();
  }

  private static void startCluster() throws IOException, InterruptedException, SQLException, ClassNotFoundException {
    String cluster = currCluster;
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }

    String json = StreamUtils.inputStreamToString(Cli.class.getResourceAsStream("/config-" + cluster + ".json"));
    JsonDict config = new JsonDict(json);
    JsonDict databaseDict = config.getDict("database");
    String installDir = databaseDict.getString("installDirectory");
    installDir = resolvePath(installDir);
    JsonArray shards = databaseDict.getArray("shards");
    for (int i = 0; i < shards.size(); i++) {
      JsonArray replicas = shards.getDict(i).getArray("replicas");
      for (int j = 0; j < replicas.size(); j++) {
        JsonDict replica = replicas.getDict(j);
        stopServer(databaseDict, replica.getString("publicAddress"), replica.getString("privateAddress"), replica.getString("port"), installDir);
      }
    }
    Thread.sleep(2000);
    for (int i = 0; i < shards.size(); i++) {
      JsonArray replicas = shards.getDict(i).getArray("replicas");
      for (int j = 0; j < replicas.size(); j++) {
        JsonDict replica = replicas.getDict(j);
        startServer(databaseDict, replica.getString("publicAddress"), replica.getString("privateAddress"), replica.getString("port"), installDir, cluster);
        if (i == 0 && j == 0) {

          while (true) {
            String command = "DatabaseServer:healthCheckPriority:1:1:__none__";

            try {
              initConnection();

              byte[] bytes = ((ConnectionProxy) conn).getDatabaseClient().send(null, 0, 0, command, null, DatabaseClient.Replica.master);
              if (new String(bytes, "utf-8").equals("{\"status\" : \"ok\"}")) {
                break;
              }
            }
            catch (Exception e) {
              System.out.println("Waiting for master to start...");
              Thread.sleep(2000);
            }
          }
        }
      }
    }

    for (int i = 0; i < shards.size(); i++) {
      JsonArray replicas = shards.getDict(i).getArray("replicas");
      for (int j = 0; j < replicas.size(); j++) {
        JsonDict replica = replicas.getDict(j);
        while (true) {
          String command = "DatabaseServer:healthCheck:1:1:__none__";

          try {
            byte[] bytes = ((ConnectionProxy) conn).getDatabaseClient().send(null, i, j, command, null, DatabaseClient.Replica.specified);
            if (new String(bytes, "utf-8").equals("{\"status\" : \"ok\"}")) {
              break;
            }
          }
          catch (Exception e) {
            System.out.println("Waiting for servers to start...");
            Thread.sleep(2000);
          }
        }
      }
    }

    System.out.println("Finished starting servers");
  }

  private static void startServer(JsonDict databaseDict, String externalAddress, String privateAddress, String port, String installDir,
                                  String cluster) throws IOException, InterruptedException {
    String deployUser = databaseDict.getString("user");
    String maxHeap = databaseDict.getString("maxJavaHeap");
    if (port == null) {
      port = "9010";
    }
    String searchHome = installDir;
//    if (!searchHome.startsWith("/")) {
//      searchHome = "$HOME/" + searchHome;
//    }
    if (externalAddress.equals("127.0.0.1") || externalAddress.equals("localhost")) {
      if (!searchHome.startsWith("/")) {
        File file = new File(System.getProperty("user.home"), searchHome);
        searchHome = file.getAbsolutePath();
      }
    }
    System.out.println("Home=" + searchHome);
    if (externalAddress.equals("127.0.0.1") || externalAddress.equals("localhost")) {
      String maxStr = databaseDict.getString("maxJavaHeap");
      if (maxStr != null && maxStr.contains("%")) {
        ProcessBuilder builder = new ProcessBuilder().command("bin/get-mem-total");
        Process p = builder.start();
        BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
        String line = reader.readLine();
        double totalGig = 0;
        if (line.toLowerCase().startsWith("memtotal")) {
          line = line.substring("MemTotal:".length()).trim();
          totalGig = getMemValue(line);
        }
        else {
          String[] parts = line.split(" ");
          String memStr = parts[1];
          totalGig = getMemValue(memStr);
        }
        p.waitFor();
        maxStr = maxStr.substring(0, maxStr.indexOf("%"));
        double maxPercent = Double.valueOf(maxStr);
        double maxGig = totalGig * (maxPercent / 100);
        maxHeap = (int)Math.floor(maxGig * 1024d) + "m";
      }
      ProcessBuilder builder = new ProcessBuilder().command("bin/start-db-server", privateAddress, port, maxHeap, searchHome, cluster);
      System.out.println("Started server: address=" + externalAddress + ", port=" + port + ", maxJavaHeap=" + maxHeap);
      Process p = builder.start();
      p.waitFor();
    }
    else {
      String maxStr = databaseDict.getString("maxJavaHeap");
      if (maxStr != null && maxStr.contains("%")) {
        ProcessBuilder builder = new ProcessBuilder().command("bin/remote-get-mem-total", deployUser + "@" + externalAddress, installDir);
        Process p = builder.start();
        BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
        String line = reader.readLine();
        double totalGig = 0;
        if (line.toLowerCase().startsWith("memtotal")) {
          line = line.substring("MemTotal:".length()).trim();
          totalGig = getMemValue(line);
        }
        else {
          String[] parts = line.split(" ");
          String memStr = parts[1];
          totalGig =  getMemValue(memStr);
        }
        p.waitFor();
        maxStr = maxStr.substring(0, maxStr.indexOf("%"));
        double maxPercent = Double.valueOf(maxStr);
        double maxGig = totalGig * (maxPercent / 100);
        maxHeap = (int)Math.floor(maxGig * 1024d) + "m";
      }

      ProcessBuilder builder = new ProcessBuilder().command("bin/do-start", deployUser + "@" + externalAddress, installDir, privateAddress, port, maxHeap, searchHome, cluster);
      System.out.println("Started server: address=" + externalAddress + ", port=" + port + ", maxJavaHeap=" + maxHeap);
      Process p = builder.start();
      p.waitFor();
    }
  }

  private static void startBenchServer(JsonDict databaseDict, String externalAddress, String privateAddress, String port, String installDir,
                                  String cluster) throws IOException, InterruptedException {
    String deployUser = databaseDict.getString("user");
    String maxHeap = databaseDict.getString("maxJavaHeap");
    if (port == null) {
      port = "9010";
    }
    String searchHome = installDir;
//    if (!searchHome.startsWith("/")) {
//      searchHome = "$HOME/" + searchHome;
//    }
    if (externalAddress.equals("127.0.0.1") || externalAddress.equals("localhost")) {
      if (!searchHome.startsWith("/")) {
        File file = new File(System.getProperty("user.home"), searchHome);
        searchHome = file.getAbsolutePath();
      }
      ProcessBuilder builder = new ProcessBuilder().command("bin/do-start-bench", "", installDir, privateAddress, port, maxHeap, searchHome, cluster);
      System.out.println("Started server: address=" + externalAddress + ", port=" + port + ", maxJavaHeap=" + maxHeap);
      Process p = builder.start();
      p.waitFor();
      System.out.println(StreamUtils.inputStreamToString(p.getErrorStream()));
      System.out.println(StreamUtils.inputStreamToString(p.getInputStream()));
      return;
    }
    System.out.println("Home=" + searchHome);

    System.out.println("1=" + deployUser + "@" + externalAddress + ", 2=" + installDir +
        ", 3=" + privateAddress + ", 4=" + port + ", 5=" + maxHeap + ", 6=" + searchHome +
        ", 7=" + cluster);
    ProcessBuilder builder = new ProcessBuilder().command("bin/do-start-bench", deployUser + "@" + externalAddress, installDir, privateAddress, port, maxHeap, searchHome, cluster);
    System.out.println("Started server: address=" + externalAddress + ", port=" + port + ", maxJavaHeap=" + maxHeap);
    Process p = builder.start();
    p.waitFor();
  }

  private static void stopServer(JsonDict databaseDict, String externalAddress, String privateAddress, String port, String installDir) throws IOException, InterruptedException {
    String deployUser = databaseDict.getString("user");
    if (externalAddress.equals("127.0.0.1") || externalAddress.equals("localhost")) {
      ProcessBuilder builder = new ProcessBuilder().command("bin/kill-server", "NettyServer", "-host", privateAddress, "-port", port);
      //builder.directory(workingDir);
      Process p = builder.start();
      p.waitFor();
    }
    else {
      ProcessBuilder builder = new ProcessBuilder().command("ssh", "-n", "-f", "-o",
          "UserKnownHostsFile=/dev/null", "-o", "StrictHostKeyChecking=no", deployUser + "@" +
              externalAddress, installDir + "/bin/kill-server", "NettyServer", "-host", privateAddress, "-port", port);
      //builder.directory(workingDir);
      Process p = builder.start();
      p.waitFor();
    }
//    while (true) {
//      builder = new ProcessBuilder().command("ssh", "-n", "-f", "-o",
//          "UserKnownHostsFile=/dev/null", "-o", "StrictHostKeyChecking=no", deployUser + "@" +
//              externalAddress, installDir + "/bin/is-server-running", privateAddress, port);
//      //builder.directory(workingDir);
//      p = builder.start();
//      InputStream in = p.getInputStream();
//      String str = StreamUtils.inputStreamToString(in);
//      if (str.length() == 0) {
//        break;
//      }
//      p.waitFor();
//
//      Thread.sleep(1000);
//      System.out.println("Waiting for server to stop...");
//    }
  }

  private static void stopBenchServer(JsonDict databaseDict, String externalAddress, String privateAddress, String port, String installDir) throws IOException, InterruptedException {
    String deployUser = databaseDict.getString("user");
    if (externalAddress.equals("127.0.0.1") || externalAddress.equals("localhost")) {
      ProcessBuilder builder = new ProcessBuilder().command("bin/kill-server", "BenchServer", port, port, port, port);
      //builder.directory(workingDir);
      Process p = builder.start();
      p.waitFor();
    }
    else {
      ProcessBuilder builder = new ProcessBuilder().command("ssh", "-n", "-f", "-o",
          "UserKnownHostsFile=/dev/null", "-o", "StrictHostKeyChecking=no", deployUser + "@" +
              externalAddress, installDir + "/bin/kill-server", "BenchServer", port, port, port, port);
      //builder.directory(workingDir);
      Process p = builder.start();
      p.waitFor();
    }
//    while (true) {
//      builder = new ProcessBuilder().command("ssh", "-n", "-f", "-o",
//          "UserKnownHostsFile=/dev/null", "-o", "StrictHostKeyChecking=no", deployUser + "@" +
//              externalAddress, installDir + "/bin/is-server-running", privateAddress, port);
//      //builder.directory(workingDir);
//      p = builder.start();
//      InputStream in = p.getInputStream();
//      String str = StreamUtils.inputStreamToString(in);
//      if (str.length() == 0) {
//        break;
//      }
//      p.waitFor();
//
//      Thread.sleep(1000);
//      System.out.println("Waiting for server to stop...");
//    }
  }

  private static void deploy() throws IOException, ExecutionException {
    String cluster = currCluster;
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }
    System.out.println("deploying: cluster=" + cluster);

    try {
      InputStream in = Cli.class.getResourceAsStream("/config-" + cluster + ".json");
      if (in == null) {
        in = new FileInputStream("/Users/lowryda/database/config/config-" + cluster + ".json");
      }
      String json = StreamUtils.inputStreamToString(in);
      JsonDict config = new JsonDict(json);
      JsonDict databaseDict = config.getDict("database");
      final String deployUser = databaseDict.getString("user");
      final String installDir = resolvePath(databaseDict.getString("installDirectory"));
      Set<String> installedAddresses = new HashSet<>();
      JsonArray shards = databaseDict.getArray("shards");
      List<Future> futures = new ArrayList<>();
      ThreadPoolExecutor executor = new ThreadPoolExecutor(4, 4, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
      try {
        for (int i = 0; i < shards.size(); i++) {
          JsonArray replicas = shards.getDict(i).getArray("replicas");
          for (int j = 0; j < replicas.size(); j++) {
            JsonDict replica = replicas.getDict(j);
            final String externalAddress = replica.getString("publicAddress");
            if (installedAddresses.add(externalAddress)) {
              if (externalAddress.equals("127.0.0.1") || externalAddress.equals("localhost")) {
                continue;
              }
              futures.add(executor.submit(new Callable(){
                @Override
                public Object call() throws Exception {
                  System.out.println("deploying to server: publicAddress=" + externalAddress);
                  //ProcessBuilder builder = new ProcessBuilder().command("rsync", "-rvlLt", "--delete", "-e", "'ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no'", "*", deployUser + "@" + externalAddress + ":" + installDir);
                  ProcessBuilder builder = new ProcessBuilder().command("bin/do-rsync", deployUser + "@" + externalAddress + ":" + installDir);
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
                  return null;
                }
              }));
            }
          }
        }
        JsonArray clients = databaseDict.getArray("clients");
        if (clients != null) {
          for (int i = 0; i < clients.size(); i++) {
            JsonDict replica = clients.getDict(i);
            final String externalAddress = replica.getString("publicAddress");
            if (installedAddresses.add(externalAddress)) {
              if (externalAddress.equals("127.0.0.1") || externalAddress.equals("localhost")) {
                continue;
              }
              futures.add(executor.submit(new Callable(){
                @Override
                public Object call() throws Exception {
                  System.out.println("deploying to client: publicAddress=" + externalAddress);
                  //ProcessBuilder builder = new ProcessBuilder().command("rsync", "-rvlLt", "--delete", "-e", "'ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no'", "*", deployUser + "@" + externalAddress + ":" + installDir);
                  ProcessBuilder builder = new ProcessBuilder().command("bin/do-rsync", deployUser + "@" + externalAddress + ":" + installDir);
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
                  return null;
                }
              }));
            }
          }
        }
        for (Future future : futures) {
          future.get();
        }
      }
      finally {
        executor.shutdownNow();
      }
    }
    catch (InterruptedException e) {
      throw new DatabaseException(e);
    }

    System.out.println("Finished deploy: cluster=" + cluster);
  }

  private static String resolvePath(String installDir) {
    if (installDir.startsWith("$HOME")) {
      installDir = installDir.substring("$HOME".length());
      if (installDir.startsWith("/")) {
        installDir = installDir.substring(1);
      }
    }
    return installDir;
  }
}
