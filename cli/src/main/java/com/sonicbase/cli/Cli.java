package com.sonicbase.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.mashape.unirest.request.GetRequest;
import com.sonicbase.aws.EC2ToConfig;
import com.sonicbase.common.ComArray;
import com.sonicbase.common.ComObject;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.jdbcdriver.ResultSetProxy;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.query.impl.ResultSetImpl;
import com.sonicbase.schema.DataType;
import com.sonicbase.schema.FieldSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.DateUtils;
import jline.ConsoleReader;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.net.ssl.*;
import java.io.*;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.NumberFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.sonicbase.common.MemUtil.getMemValue;

//import jline.ConsoleReader;

public class Cli {

  private static Logger logger = LoggerFactory.getLogger(Cli.class);

  private String[] args;
  private String command = "";
  private String currCluster;
  private ConnectionProxy conn;
  private ResultSet ret;
  private String lastCommand;
  private String currDbName;
  private ConsoleReader reader;
  private ThreadPoolExecutor executor;
  private ArrayList<List<String>> serverStatsData;
  private String OS = System.getProperty("os.name").toLowerCase();
  private boolean plainConsole = false;
  private String[] hosts;
  private String username;
  private Integer licenseDaysLeft = null;
  private BackupHandler backupHandler;
  private ClusterHandler clusterHandler;
  private BenchHandler benchHandler;
  private DescribeHandler describeHandler;
  private BulkImportHandler bulkImportHandler;
  private SQLHandler sqlHandler;
  private MiscHandler miscHandler;
  private Map<String, CommandInvoker> commands = new HashMap<>();

  public Cli(String[] args) {
    this.args = args;
  }

  public void start() {
    startCheckLicensesThread();

    initCommands();
    backupHandler = new BackupHandler(this);
    clusterHandler = new ClusterHandler(this);
    benchHandler = new BenchHandler(this);
    describeHandler = new DescribeHandler(this);
    bulkImportHandler = new BulkImportHandler(this);
    sqlHandler = new SQLHandler(this);
    miscHandler = new MiscHandler(this);

    try {
      ProcessBuilder builder = new ProcessBuilder().command("uname", "-o");
      Process p = builder.start();
      InputStream stream = p.getInputStream();
      String os = IOUtils.toString(stream, "utf-8");
      stream.close();
      if (os.equalsIgnoreCase("cygwin")) {
        OS = "cygwin";
      }
      p.waitFor();
    }
    catch (Exception e) {
      // not cygwin
    }


    executor = new ThreadPoolExecutor(128, 128, 10000, TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
    try {
      if (isWindows()) {
        com.sonicbase.common.WindowsTerminal term = new com.sonicbase.common.WindowsTerminal();
        term.enableAnsi();
      }

      if (!isWindows() && !isCygwin()) {
        reader = new ConsoleReader();
      }
      if (args.length > 0) {
        if (args[0].equals("--plain")) {
          plainConsole = true;
        }
        else {
          CommandLine commandLine= getCommandLineOptions(args);
          String localCluster = commandLine.getOptionValue("cluster");
          if (localCluster != null) {
            useCluster(localCluster);
          }
          String localDb = commandLine.getOptionValue("db");
          if (localDb != null) {
            useDatabase(localDb);
          }
          runCommand(commandLine.getOptionValue("command"));
          System.exit(0);
        }
      }

      disable();

      System.out.print("\033[2J\033[;H");

      moveToBottom();

      while (true) {
        while (true) {
          StringBuilder builder = new StringBuilder();
          while (true) {

            int ch = 0;
            if (isWindows() || isCygwin()) {
              ch = System.in.read();
              if ((char) ch == '\n') {
                break;
              }
              builder.append(String.valueOf((char) ch));
            }
            else {
              String line = reader.readLine();//String.valueOf((char)c);
              builder.append(line);
              break;
            }

          }
          String str = builder.toString();
          command += str;
          break;
        }

        if (!plainConsole) {
          //clear screen
          moveToBottom();
        }
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
    catch (Exception e) {
      e.printStackTrace();
    }

  }

  private CommandLine getCommandLineOptions(String[] args) throws ParseException {
    Options options = new Options();
    Option op = new Option("c", "cluster", true, "cluster");
    op.setRequired(false);
    options.addOption(op);
    op = new Option("d", "db", true, "db");
    op.setRequired(false);
    options.addOption(op);
    op = new Option("m", "command", true, "command");
    op.setRequired(false);
    options.addOption(op);

    CommandLineParser parser = new DefaultParser();
    return parser.parse(options, args);
  }


  public boolean isWindows() {
    return !OS.contains("cygwin") && OS.contains("win");
  }

  public boolean isCygwin() {
    return OS.contains("cygwin");
  }

  public boolean isMac() {
    return OS.contains("mac");
  }

  public boolean isUnix() {
    return OS.contains("nux");
  }


  public static void main(final String[] args) throws IOException, InterruptedException {
    Cli cli = new Cli(args);
    cli.start();
  }


   private List<String> commandHistory = new ArrayList<>();

  private void writeHeader(String width) throws IOException {
    width = width.trim();
    System.out.print("\033[" + 0 + ";0f");

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
    if (licenseDaysLeft != null) {
      builder.append(": License Days Left=" + licenseDaysLeft);
    }
    for (int i = builder.length(); i < Integer.valueOf(width) - using.length(); i++) {
      builder.append(" ");
    }
    builder.append(using);

    if (isWindows()) {
      System.out.print("\033[0;1;37;46m");
    }
    else {
      System.out.print("\033[0;1;38;46m");
      System.out.print("\033[38;5;195m");
    }

    System.out.print(builder.toString());
    System.out.print("\033[49;39m");
  }

  private void moveToBottom() throws IOException, InterruptedException {

    if (plainConsole) {
      System.out.print("sonicbase>>");
    }
    else {
      String str = getTerminalSize();
      String[] parts = str.split(",");
      String width = parts[0].trim();
      String height = parts[1].trim();

      writeHeader(width);

      int h = Integer.valueOf(height);
      System.out.print("\033[" + h + ";" + 0 + "f");

    }
  }

  public String getTerminalSize() throws IOException, InterruptedException {
    if (false && isCygwin()) {
      com.sonicbase.common.WindowsTerminal terminal = new com.sonicbase.common.WindowsTerminal();
      String values = terminal.getConsoleSize();
      System.out.println("stty=" + values);

      String[] parts = values.trim().split(" ");

      return parts[1] + "," + parts[0];
    }
    else if (isWindows()) {
      File file = new File("tmp", "width.txt");
      File file2 = new File("tmp", "height.txt");
      file.getParentFile().mkdirs();
      ProcessBuilder builder = new ProcessBuilder().command(new File("bin", "terminal-size.bat").getAbsolutePath());
      Process p = builder.start();
      p.waitFor();
      String width = IOUtils.toString(new FileInputStream(file), "utf-8");
      String height = IOUtils.toString(new FileInputStream(file2), "utf-8");
      file.delete();
      file2.delete();
      return width + "," + height;
    }
    else {
      String[] cmd = {"/bin/sh", "-c", "stty size </dev/tty"};
      Runtime.getRuntime().exec(cmd);
      ProcessBuilder builder = new ProcessBuilder().command(cmd);
      //ProcessBuilder builder = new ProcessBuilder().command(new File("bin", "terminal-size").getAbsolutePath());
      Process p = builder.start();
      String stty = IOUtils.toString(p.getInputStream());
      p.waitFor();
      String[] parts = stty.split(" ");
      if (parts.length == 2) {
        return parts[1].trim() + "," + parts[0].trim();
      }
      return 80 + "," + 80;
    }
  }


  private void previousCommand() {
    String command = commandHistory.get(0);
    System.out.print(command);
  }

  public void setCurrDbName(String dbName) {
    currDbName = dbName;
  }

  interface CommandInvoker {
    void invoke(String command) throws Exception;
  }

  public void initCommands() {

    commands.put("deploy cluster", (command)-> clusterHandler.deploy());
    commands.put("deploy license server", (command)->miscHandler.deployLicenseServer());
    commands.put("start cluster", (command)->clusterHandler.startCluster());
    commands.put("start server", (command)->clusterHandler.startServer(command));
    commands.put("restart cluster", (command)->clusterHandler.rollingRestart());
    commands.put("start license server", (command)->miscHandler.startLicenseServer());
    commands.put("stop cluster", (command)->clusterHandler.stopCluster());
    commands.put("stop server", (command)->clusterHandler.stopServer(command));
    commands.put("stop license server", (command)->miscHandler.stopLicenseServer());
    commands.put("start backup", (command)->backupHandler.startBackup());
    commands.put("backup status", (command)->backupHandler.backupStatus());
    commands.put("start restore", (command)->backupHandler.startRestore(command));
    commands.put("restore status", (command)->backupHandler.restoreStatus());
    commands.put("reload server", (command)->clusterHandler.reloadServer(command));
    commands.put("reload replica", (command)->clusterHandler.reloadReplica(command));
    commands.put("start shard", (command)-> {
        int pos = command.lastIndexOf(" ");
        String shard = command.substring(pos + 1);
        clusterHandler.startShard(Integer.valueOf(shard));
      });
    commands.put("stop shard", (command)-> {
        int pos = command.lastIndexOf(" ");
        String shard = command.substring(pos + 1);
        clusterHandler.stopShard(Integer.valueOf(shard));
      });
    commands.put("purge cluster", (command)->clusterHandler.purgeCluster());
    commands.put("purge install", (command)->miscHandler.purgeInstall());
    commands.put("start streams consumers", (command)->miscHandler.startStreaming());
    commands.put("stop streams consumers", (command)->miscHandler.stopStreaming());
    commands.put("describe licenses", (command)->describeHandler.describeLicenses());
    commands.put("quit", (command)->exit());
    commands.put("exit", (command)->exit());
    commands.put("healthcheck", (command)->healthCheck());
    commands.put("echo", (command)->{
        moveToBottom();
        System.out.print(command);
      });
    commands.put("clear", (command)->{
        System.out.print("\033[2J\033[;H");
        moveToBottom();
      });
    commands.put("gather diagnostics", (command)->miscHandler.gatherDiagnostics());
    commands.put("bulk import status", (command)->bulkImportHandler.bulkImportStatus(command));
    commands.put("start bulk import", (command)->bulkImportHandler.startBulkImport(command));
    commands.put("cancel bulk import", (command)->bulkImportHandler.cancelBulkImport(command));
    commands.put("use cluster", (command)->{
        int pos = command.lastIndexOf(" ");
        String cluster = command.substring(pos + 1);
        useCluster(cluster);
      });
    commands.put("use database", (command)->{
        int pos = command.lastIndexOf(" ");
        String dbName = command.substring(pos + 1);
        useDatabase(dbName);
      });
    commands.put("start range", (command)->startRange());
    commands.put("stop range", (command)->stopRange());
    commands.put("create database", (command)->{
        int pos = command.lastIndexOf(" ");
        String dbName = command.substring(pos + 1);
        createDatabase(dbName);
      });
    commands.put("select", (command)->{
        System.out.print("\033[2J\033[;H");
        sqlHandler.select(command);
      });
    commands.put("next", (command)->{
        System.out.print("\033[2J\033[;H");
        next();
      });
    commands.put("build config", (command)->buildConfig(command));
    commands.put("build config aws", (command)->buildConfigAws(command));
    commands.put("force rebalance", (command)->forceRebalance());
    commands.put("insert", (command)->{
        System.out.print("\033[2J\033[;H");
        sqlHandler.insert(command);
      });
    commands.put("update", (command)->{
        System.out.print("\033[2J\033[;H");
        update(command);
      });
    commands.put("delete", (command)->{
        System.out.print("\033[2J\033[;H");
        sqlHandler.delete(command);
      });
    commands.put("truncate", (command)->{
        System.out.print("\033[2J\033[;H");
        sqlHandler.truncate(command);
      });
    commands.put("drop", (command)->{
        System.out.print("\033[2J\033[;H");
        sqlHandler.drop(command);
      });
    commands.put("create", (command)->{
        System.out.print("\033[2J\033[;H");
        sqlHandler.create(command);
      });
    commands.put("alter", (command)->{
        System.out.print("\033[2J\033[;H");
        sqlHandler.alter(command);
      });
    commands.put("help", (command)->help());
    commands.put("describe", (command)->{
        System.out.print("\033[2J\033[;H");
        describeHandler.describe(command);
      });
    commands.put("explain", (command)->{
        System.out.print("\033[2J\033[;H");
        miscHandler.explain(command);
      });
    commands.put("reconfigure cluster", (command)->clusterHandler.reconfigureCluster());
    commands.put("stop cluster", (command)->clusterHandler.stopCluster());
    commands.put("bench healthcheck", (command)->benchHandler.benchHealthcheck());
    commands.put("bench start", (command)->benchHandler.benchStartTest(command));
    commands.put("bench stop", (command)->benchHandler.benchStopTest(command));
    commands.put("bench stats", (command)->benchHandler.benchstats(command));
    commands.put("bench resetStats", (command)->benchHandler.benchResetStats(command));
  }

  private void runCommand(String command) {
    try {
      command = command.trim();
      commandHistory.add(command);

      boolean found = false;
      for (Map.Entry<String, CommandInvoker> entry : commands.entrySet()) {
        if (command.startsWith(entry.getKey())) {
          entry.getValue().invoke(command);
          found = true;
          break;
        }
      }

      if (!found) {
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

  public void disable() {
    try {
      SSLContext sslc = SSLContext.getInstance("TLS");
      TrustManager[] trustManagerArray = {new NullX509TrustManager()};
      sslc.init(null, trustManagerArray, null);
      HttpsURLConnection.setDefaultSSLSocketFactory(sslc.getSocketFactory());
      HttpsURLConnection.setDefaultHostnameVerifier(new NullHostnameVerifier());
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }

  public String getCurrCluster() {
    return currCluster;
  }

  public ConnectionProxy getConn() {
    return conn;
  }

  public String getUsername() {
    return username;
  }

  public ThreadPoolExecutor getExecutor() {
    return executor;
  }

  public String getCurrDbName() {
    return currDbName;
  }

  public void setLastCommand(String command) {
    lastCommand = command;
  }

  public String getCommand() {
    return command;
  }

  public void setRet(ResultSet resultSet) {
    ret = resultSet;
  }

  public ResultSet getRet() {
    return ret;
  }

  public ArrayList<List<String>> getServerStatsData() {
    return serverStatsData;
  }

  private class NullX509TrustManager implements X509TrustManager {
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
      System.out.println();
    }

    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
      System.out.println();
    }

    public X509Certificate[] getAcceptedIssuers() {
      return new X509Certificate[0];
    }
  }

  private class NullHostnameVerifier implements HostnameVerifier {
    public boolean verify(String hostname, SSLSession session) {
      return true;
    }
  }

  public com.google.api.client.http.HttpResponse restGet(String url) throws IOException {
    NetHttpTransport.Builder builder = new NetHttpTransport.Builder();
    HttpRequest request = builder.build().createRequestFactory().buildGetRequest(new GenericUrl(url));
    request.setReadTimeout(120000);
    HttpHeaders headers = request.getHeaders();
    headers.put("Accept", Collections.singletonList("application/json"));
    headers.put("Content-Type", Collections.singletonList("application/json"));
    request.setHeaders(headers);
    return request.execute();
  }

  public void startCheckLicensesThread() {
    Thread thread = new Thread(() -> {
      while (true) {
        try {
          ret = new ResultSetProxy((ResultSetImpl) ConnectionProxy.describeLicenses());

          while (true) {
            if (!ret.next()) {
              break;
            }
            String line = ret.getString(1);
            if (line.startsWith("disabling date=")) {
              String[] parts = line.split("=");
              String date = parts[1].trim();

              Date d = DateUtils.fromString(date);
              long disableDateLong = d.getTime();

              licenseDaysLeft = (int) ((disableDateLong - System.currentTimeMillis()) / 24 / 60 / 60 / 1000);
              break;
            }
          }
        }
        catch (Exception e) {
          logger.error("Error getting license status", e);
        }
        try {
          Thread.sleep(60_000);
        }
        catch (Exception e) {
          break;
        }
      }
    });
    thread.start();
  }

  private Thread rangeThread;

  private void stopRange() {
    if (rangeThread != null) {
      rangeThread.interrupt();
      rangeThread = null;
    }
  }

  public boolean healthCheck() throws IOException, InterruptedException {
    final String cluster = getCurrCluster();
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return false;
    }

    final AtomicBoolean allHealthy = new AtomicBoolean(true);
    String json = IOUtils.toString(Cli.class.getResourceAsStream("/config-" + cluster + ".json"), "utf-8");
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode config = (ObjectNode) mapper.readTree(json);
    final ObjectNode databaseDict = config;
    final String installDir = resolvePath(databaseDict.get("installDirectory").asText());
    ArrayNode shards = databaseDict.withArray("shards");

    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < shards.size(); i++) {
      final int shard = i;
      final ArrayNode replicas = (ArrayNode) shards.get(i).withArray("replicas");
      for (int j = 0; j < replicas.size(); j++) {
        final int replica = j;
        getExecutor().submit((Callable) () -> {
          ObjectNode replicaObj = (ObjectNode) replicas.get(replica);
          ComObject cobj = new ComObject();
          cobj.put(ComObject.Tag.DB_NAME, "__none__");
          cobj.put(ComObject.Tag.SCHEMA_VERSION, 1);
          cobj.put(ComObject.Tag.METHOD, "DatabaseServer:healthCheck");

          try {
            byte[] bytes = getConn().send(null, shard, replica, cobj, ConnectionProxy.Replica.SPECIFIED);
            ComObject retObj = new ComObject(bytes);
            String retStr = retObj.getString(ComObject.Tag.STATUS);
            if (retStr.equals("{\"status\" : \"ok\"}")) {
              return null;
            }
            else {
              allHealthy.set(false);
              System.out.println("Server not healthy: shard=" + shard + ", replica=" + replica + ", privateAddress=" +
                  replicaObj.get("privateAddress").asText());
            }
          }
          catch (Exception e) {
            System.out.println("Server not healthy... server=" + replicaObj.get("privateAddress").asText());
            logger.error("Server not healthy... server=" + replicaObj.get("privateAddress").asText(), e);
          }
          return null;
        });

      }
    }
    for (Future future : futures) {
      try {
        future.get();
      }
      catch (ExecutionException e) {
        throw new DatabaseException(e);
      }
    }
    if (allHealthy.get()) {
      System.out.println("All servers healthy");
    }
    else {
      System.out.println("At least one server is not healthy");
    }
    return allHealthy.get();
  }

  private void startRange() throws SQLException, ClassNotFoundException {
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

    rangeThread = new Thread(() -> {
      while (true) {
        try {
          PreparedStatement stmt = conn.prepareStatement("select id1 from persons where id1 > 0");
          ResultSet rs = stmt.executeQuery();
          long currOffset = 1;
          while (rs.next()) {
            long curr = rs.getLong("id1");
            if (curr != currOffset) {
              System.out.println("expected " + currOffset + ", got " + curr);
              currOffset = curr;
            }
            currOffset++;
          }
          System.out.println("max=" + currOffset);
        }
        catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
    rangeThread.start();
  }

  public void doGetOSStats(final ConnectionProxy conn) throws InterruptedException, ExecutionException {
    serverStatsData = new ArrayList<>();

    List<Future<List<String>>> futures = new ArrayList<>();
    for (int i = 0; i < conn.getShardCount(); i++) {
      for (int j = 0; j < conn.getReplicaCount(); j++) {
        final int shard = i;
        final int replica = j;
        futures.add(executor.submit(() -> {
          try {
            ComObject cobj = new ComObject();
            cobj.put(ComObject.Tag.DB_NAME, currDbName);
            cobj.put(ComObject.Tag.SCHEMA_VERSION, conn.getSchemaVersion());
            byte[] ret = conn.send("MonitorManager:getOSStats", shard, replica, cobj, ConnectionProxy.Replica.SPECIFIED);
            ComObject retObj = new ComObject(ret);
            double resGig = retObj.getDouble(ComObject.Tag.RES_GIG);
            double cpu = retObj.getDouble(ComObject.Tag.CPU);
            double javaMemMin = retObj.getDouble(ComObject.Tag.JAVA_MEM_MIN);
            double javaMemMax = retObj.getDouble(ComObject.Tag.JAVA_MEM_MAX);
            double recRate = retObj.getDouble(ComObject.Tag.AVG_REC_RATE) / 1000000000d;
            double transRate = retObj.getDouble(ComObject.Tag.AVG_TRANS_RATE) / 1000000000d;
            String diskAvail = retObj.getString(ComObject.Tag.DISK_AVAIL);
            String host = retObj.getString(ComObject.Tag.HOST);
            int port = retObj.getInt(ComObject.Tag.PORT);

            List<String> line = new ArrayList<>();
            line.add(host + ":" + port);
            line.add(String.format("%.0f", cpu));
            line.add(String.format("%.2f", resGig));
            line.add(String.format("%.2f", javaMemMin));
            line.add(String.format("%.2f", javaMemMax));
            line.add(String.format("%.4f", recRate));
            line.add(String.format("%.4f", transRate));
            line.add(diskAvail);
            return line;
          }
          catch (Exception e) {
            logger.error("Error gathering OSStats: shard=" + shard + ", replica=" + replica, e);
            return null;
          }
        }));
      }
    }

    for (Future<List<String>> future : futures) {
      List<String> line = null;
      try {
        line = future.get(10000, TimeUnit.MILLISECONDS);
      }
      catch (TimeoutException e) {
        logger.error("Timed out gathering diagnostics");
      }
      if (line != null) {
        serverStatsData.add(line);
      }
    }
  }

  public void getFile(ObjectNode config, File dir, int shard, int replica, String filename) throws IOException, InterruptedException {
    try {
      String installDir = resolvePath(config.get("installDirectory").asText());
      String deployUser = config.get("user").asText();

      String address = config.withArray("shards").get(shard).withArray("replicas").get(replica).get("publicAddress").asText();
      //System.out.println("user.dir=" + System.getProperty("user.dir") + ", installDir=" + installDir);
      if (address.equals("localhost") || address.equals("127.0.0.1")) {
        File destFile = new File(dir, filename);
        destFile.getParentFile().mkdirs();
        destFile.delete();
        File srcFile = new File(System.getProperty("user.dir"), filename);
        FileUtils.copyFile(srcFile, destFile);
      }
      else {
        if (isWindows()) {
          System.out.println("getting file: " + installDir + filename + " " + dir.getAbsolutePath());
          ComObject cobj = new ComObject();
          cobj.put(ComObject.Tag.DB_NAME, currDbName);
          cobj.put(ComObject.Tag.SCHEMA_VERSION, 1);
          cobj.put(ComObject.Tag.FILENAME, filename);
          byte[] bytes = conn.send("SnapshotManager:getFile", shard, replica, cobj, ConnectionProxy.Replica.SPECIFIED);
          if (bytes != null) {
            File file = new File(dir, filename);
            file.getParentFile().mkdirs();
            file.delete();
            ComObject retObj = new ComObject(bytes);
            try (FileOutputStream fileOut = new FileOutputStream(file)) {
              fileOut.write(retObj.getByteArray(ComObject.Tag.BINARY_FILE_CONTENT));
            }
          }
        }
        else {
          System.out.println("getting file: " + "rsync -rvlLt -e " +
              "'ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no' " + deployUser + "@" + address + ":" +
              installDir + filename + " " + dir.getAbsolutePath());

          ProcessBuilder builder = new ProcessBuilder().command("bash", "bin/rsync-file", deployUser + "@" + address + ":" +
              installDir + filename, dir.getAbsolutePath());
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
    catch (Exception e) {
      System.out.println("Error getting file: " + filename);
    }
  }


  public void getFile(ObjectNode config, File dir, String address, String filename) throws IOException, InterruptedException {
    try {
      String installDir = resolvePath(config.get("installDirectory").asText());
      String deployUser = config.get("user").asText();

      //System.out.println("user.dir=" + System.getProperty("user.dir") + ", installDir=" + installDir);
      if (address.equals("localhost") || address.equals("127.0.0.1")) {
        File destFile = new File(dir, filename);
        destFile = new File(dir, destFile.getName());
        destFile.getParentFile().mkdirs();
        destFile.delete();
        File srcFile = new File(System.getProperty("user.dir"), filename);
        logger.info("copying local file: src=" + srcFile.getAbsolutePath() + ", dest=" + destFile.getAbsolutePath());
        FileUtils.copyFile(srcFile, destFile);
      }
      else {
        if (isWindows()) {
          logger.info("Get file by address not supported on Windows");
        }
        else {
          System.out.println("getting file: " + "rsync -rvlLt -e " +
              "'ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no' " + deployUser + "@" + address + ":" +
              installDir + filename + " " + dir.getAbsolutePath());

          ProcessBuilder builder = new ProcessBuilder().command("bash", "bin/rsync-file", deployUser + "@" + address + ":" +
              installDir + filename, dir.getAbsolutePath());
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
    catch (Exception e) {
      System.out.println("Error getting file: " + filename);
    }
  }


  private void buildConfig(String command) throws IOException {
    if (currCluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }
    String[] parts = command.split(" ");
    String filename = parts[parts.length - 1];
    BuildConfig build = new BuildConfig();
    build.buildConfig(currCluster, filename);
  }

  private void buildConfigAws(String command) throws IOException {
    if (currCluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }
    EC2ToConfig toConfig = new EC2ToConfig();
    toConfig.buildConfig(currCluster);
  }

  private void exit() throws SQLException {
    if (conn != null) {
      conn.close();
    }
    benchHandler.shutdown();
    System.exit(0);
  }

  class ExitCliException extends RuntimeException {

  }


  private void forceRebalance() {
    String cluster = currCluster;
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }
    if (currDbName == null) {
      System.out.println("Error, not using a database");
      return;
    }

    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, currDbName);
    cobj.put(ComObject.Tag.SCHEMA_VERSION, conn.getSchemaVersion());
    conn.send("PartitionManager:beginRebalance", 0, 0, cobj, ConnectionProxy.Replica.MASTER);
  }


  class HelpItem {
    private String command;
    private String shortDescription;
    private String longDescription;

    public HelpItem(String command, String shortDescription, String longDescription) {
      this.command = command;
      this.shortDescription = shortDescription;
      this.longDescription = longDescription;
    }
  }

  private List<HelpItem> helpItems = new ArrayList<>();

  {
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

  private void help() {
    helpItems.sort((o1, o2) -> o1.command.compareTo(o2.command));
    for (HelpItem item : helpItems) {
      System.out.println(item.command + " - " + item.shortDescription);
    }
  }

  public void useDatabase(String dbName) throws SQLException, ClassNotFoundException {
    String cluster = currCluster;
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }
    closeConnection();
    currDbName = dbName.trim().toLowerCase();
  }

  public void closeConnection() throws SQLException {
    if (conn != null) {
      conn.close();
      conn = null;
    }
  }

  private void createDatabase(String dbName) throws SQLException, ClassNotFoundException {
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
    private String columnName;
    private String metric;
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

    public SelectColumn(String name, String metric, String columnName, DataType.Type type) {
      this.name = name;
      this.metric = metric;
      this.columnName = columnName;
      this.type = type;
    }
  }

  private void next() throws SQLException, JSQLParserException, IOException, InterruptedException {
    if (lastCommand.startsWith("describe server stats")) {
      describeHandler.displayServerStatsPage(0);
    }
    else if (lastCommand.startsWith("describe server health")) {
      describeHandler.displayServerHealthPage(0);
    }
    else if (lastCommand.startsWith("describe shards")) {
      String str = getTerminalSize();
      String[] parts = str.split(",");
      String height = parts[1];

      int currLine = 0;
      System.out.println();
      for (int i = 0; currLine < Integer.valueOf(height) - 3; i++) {
        if (!ret.next()) {
          break;
        }
        System.out.println(ret.getString(1));
        currLine++;
      }
      if (!ret.isLast()) {
        System.out.println("next");
      }
    }
    else if (lastCommand.startsWith("describe")) {
      String str = getTerminalSize();
      String[] parts = str.split(",");
      String height = parts[1];

      int currLine = 0;
      System.out.println();
      for (int i = 0; currLine < Integer.valueOf(height) - 3; i++) {
        if (!ret.next()) {
          break;
        }
        System.out.println(ret.getString(1));
        currLine++;
      }
      System.out.println("next");
    }
    else {
      processResults(lastCommand, conn);
    }
  }

  private void update(String command) throws SQLException, ClassNotFoundException {
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

  public void initConnection() throws ClassNotFoundException, SQLException {
    if (conn == null) {
      Class.forName("com.sonicbase.jdbcdriver.Driver");

      String db = "";
      if (currDbName != null) {
        db = "/" + currDbName;
      }
      StringBuilder builder = new StringBuilder();
      for (int i = 0; i < hosts.length; i++) {
        if (i != 0) {
          builder.append(",");
        }
        builder.append(hosts[i]);
      }
      conn = (ConnectionProxy) DriverManager.getConnection("jdbc:sonicbase:" + builder.toString() + db);
    }
  }

  public void processResults(String command, ConnectionProxy conn) throws JSQLParserException, SQLException, IOException, InterruptedException {
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
          for (FieldSchema field : conn.getTables(currDbName).get(fromTable).getFields()) {
            columns.add(new SelectColumn(field.getName(), field.getType()));
          }
        }
        else {
          SelectExpressionItem item = (SelectExpressionItem) selectItem;

          Alias alias = item.getAlias();
          String columnName = null;
          if (alias != null) {
            columnName = alias.getName().toLowerCase();
            columns.add(new SelectColumn(columnName, DataType.Type.VARCHAR));
          }
          else {
            Expression expression = item.getExpression();
            if (expression instanceof Function) {
              Function function = (Function) expression;
              columnName = function.getName().toLowerCase();
              columns.add(new SelectColumn(columnName, DataType.Type.VARCHAR, columnOffset));
            }
            else if (item.getExpression() instanceof Column) {
              String tableName = ((Column) item.getExpression()).getTable().getName().toLowerCase();
              if (tableName == null) {
                tableName = fromTable;
              }
              String actualColumnName = ((Column) item.getExpression()).getColumnName().toLowerCase();
              if (columnName == null) {
                columnName = actualColumnName;
              }
              Integer offset = conn.getTables(currDbName).get(tableName).getFieldOffset(actualColumnName);
              if (offset != null) {
                int fieldOffset = offset;
                FieldSchema fieldSchema = conn.getTables(currDbName).get(tableName).getFields().get(fieldOffset);
                DataType.Type type = fieldSchema.getType();
                columns.add(new SelectColumn(columnName, type));
              }
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

    for (int i = 0; i < Integer.valueOf(height) - 7; i++) {
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

    displayPageOfData(columns, width, data, !ret.isLast());
  }

  public void displayPageOfData(List<SelectColumn> columns, String width, List<List<String>> data, boolean isLast) throws IOException, InterruptedException, SQLException {
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
    if (columns.get(0).metric != null) {
      for (int i = 0; i < columns.size(); i++) {
        SelectColumn column = columns.get(i);
        if (columnWidths.get(i) == -1) {
          continue;
        }
        builder.append("| ");
        String value = column.metric;
        if (value.length() > 30) {
          value = value.substring(0, 30);
          value += "...";
        }
        builder.append(value);
        appendChar(builder, " ", columnWidths.get(i) - value.length() + 1);
      }
      builder.append("|\n");
    }
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
    if (!isLast) {
      builder.append("\nnext");
    }
    System.out.println(builder.toString());
  }

  private void appendChar(StringBuilder builder, String c, int count) {
    for (int i = 0; i < count; i++) {
      builder.append(c);
    }
  }

  private void useCluster(String cluster) throws ClassNotFoundException, SQLException, IOException, NoSuchPaddingException, NoSuchAlgorithmException, BadPaddingException, InvalidKeyException, IllegalBlockSizeException, InterruptedException {
    cluster = cluster.trim();

    currCluster = cluster;

    benchHandler.initBench(cluster);


    closeConnection();
    currDbName = null;

    String json = null;
    try {
      json = IOUtils.toString(Cli.class.getResourceAsStream("/config-" + cluster + ".json"), "utf-8");
    }
    catch (Exception e) {
      json = IOUtils.toString(new FileInputStream(new File(System.getProperty("user.dir"), "config/config-" + cluster + ".json")), "utf-8");
    }

    ObjectMapper mapper = new ObjectMapper();
    ObjectNode config = (ObjectNode) mapper.readTree(json);

    ArrayNode shards = config.withArray("shards");
    ArrayNode replicas = (ArrayNode) shards.get(0).withArray("replicas");

    if (replicas.size() >= 2) {
      if (replicas.size() < 3) {
        if (shards.size() == 1) {
          throw new DatabaseException("If you have two or more replicas, you must have at least three total servers");
        }
      }
    }

    ObjectNode databaseDict = config;

    if (isWindows()) {
      boolean remote = false;
      shards = databaseDict.withArray("shards");
      for (int shard = 0; shard < shards.size(); shard++) {
        for (int i = 0; i < shards.get(shard).withArray("replicas").size(); i++) {
          ObjectNode replica = (ObjectNode) shards.get(shard).withArray("replicas").get(i);
          String privateAddress = replica.get("privateAddress").asText();
          String publicAddress = replica.get("publicAddress").asText();
          if (!(privateAddress.equalsIgnoreCase("127.0.0.1") || privateAddress.equalsIgnoreCase("localhost") ||
              publicAddress.equalsIgnoreCase("127.0.0.1") || publicAddress.equalsIgnoreCase("localhost"))) {
            remote = true;
            break;
          }
        }
      }
      if (remote) {
        getCredentials(cluster);
      }
    }

    Boolean clientIsPrivate = config.get("clientIsPrivate").asBoolean();
    if (clientIsPrivate == null) {
      clientIsPrivate = false;
    }
    shards = databaseDict.withArray("shards");
    hosts = new String[shards.get(0).withArray("replicas").size()];
    for (int i = 0; i < hosts.length; i++) {
      ObjectNode replica = (ObjectNode) shards.get(0).withArray("replicas").get(i);
      if (clientIsPrivate) {
        hosts[i] = replica.get("privateAddress").asText();
      }
      else {
        hosts[i] = replica.get("publicAddress").asText();
      }
      hosts[i] += ":" + replica.get("port").asInt();
    }
  }

  public void getCredentials(String cluster) throws IOException, InterruptedException {
    File dir = new File(System.getProperty("user.dir"), "credentials");
    File[] files = dir.listFiles();
    username = null;
    if (files != null) {
      for (File currFile : files) {
        if (currFile.getName().startsWith(cluster + "-")) {
          username = currFile.getName().substring(cluster.length() + "-".length());
        }
      }
    }
    if (username == null) {
      dir.mkdirs();
      System.out.println("Enter credentials for server");

      File script = new File("bin/get-credentials.ps1");
      String str = IOUtils.toString(new FileInputStream(script), "utf-8");
      str = str.replaceAll("\\$1", dir.getAbsolutePath().replaceAll("\\\\", "/"));
      str = str.replaceAll("\\$2", cluster);
      File outFile = new File("tmp/" + cluster + "-get-credentials.ps1");
      outFile.getParentFile().mkdirs();
      outFile.delete();
      try {
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outFile)))) {
          writer.write(str);
        }

        ProcessBuilder builder = new ProcessBuilder().command("powershell", "-F", outFile.getAbsolutePath());
        Process p = builder.start();
        p.waitFor();

        files = dir.listFiles();
        username = null;
        if (files != null) {
          for (File currFile : files) {
            if (currFile.getName().startsWith(cluster + "-")) {
              username = currFile.getName().substring(cluster.length() + "-".length());
            }
          }
        }

      }
      finally {
        //outFile.delete();
      }
    }
  }


  private boolean validateLicense(ArrayNode shards, String cluster) {
    int replicaCount = shards.get(0).withArray("replicas").size();
    if (replicaCount == 1) {
      return true;
    }
    String json = null;
    try {
      json = IOUtils.toString(Cli.class.getResourceAsStream("/config-license-server.json"), "utf-8");
    }
    catch (Exception e) {
      throw new DatabaseException("Error validating license", e);
    }
    try {
      ObjectMapper mapper = new ObjectMapper();
      ObjectNode config = (ObjectNode) mapper.readTree(json);
      int port = config.get("server").get("port").asInt();
      final String address = config.get("server").get("privateAddress").asText();

      String primaryAddress = shards.get(0).withArray("replicas").get(0).get("privateAddress").asText();
      int primaryPort = shards.get(0).withArray("replicas").get(0).get("port").asInt();

      com.google.api.client.http.HttpResponse response = restGet("https://" + address + ":" + port + "/license/checkIn?" +
          "primaryAddress=" + primaryAddress +
          "&primaryPort=" + primaryPort +
          "&cluster=" + cluster + "&cores=0");
      String responseStr = IOUtils.toString(response.getContent(), "utf-8");
      logger.info("CheckIn response: " + responseStr);

      ObjectNode dict = (ObjectNode) mapper.readTree(responseStr);

      return !dict.get("disableNow").asBoolean();
    }
    catch (Exception e) {
      throw new DatabaseException("Error validating license", e);
    }
  }



  public String getMaxHeap(ObjectNode databaseDict) throws IOException, InterruptedException {
    String maxStr = databaseDict.get("maxJavaHeap").asText();
    String maxHeap = maxStr;
    if (maxStr != null && maxStr.contains("%")) {
      String command = "bin/get-mem-total";
      if (isCygwin() || isWindows()) {
        command = "bin/get-mem-total.bat";
      }
      ProcessBuilder builder = new ProcessBuilder().command(command);
      Process p = builder.start();
      BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
      String line = reader.readLine();
      double totalGig = 0;
      if (isCygwin() || isWindows()) {
        totalGig = Long.valueOf(line.trim()) / 1024d / 1024d / 1024d;
      }
      else {
        if (line.toLowerCase().startsWith("memtotal")) {
          line = line.substring("MemTotal:".length()).trim();
          totalGig = getMemValue(line);
        }
        else {
          String[] parts = line.split(" ");
          String memStr = parts[1];
          totalGig = getMemValue(memStr);
        }
      }
      p.waitFor();
      maxStr = maxStr.substring(0, maxStr.indexOf("%"));
      double maxPercent = Double.valueOf(maxStr);
      double maxGig = totalGig * (maxPercent / 100);
      maxHeap = (int) Math.floor(maxGig * 1024d) + "m";
    }
    return maxHeap;
  }

  public String resolvePath(String installDir) {
    if (installDir.startsWith("$HOME")) {
      installDir = installDir.substring("$HOME".length());
      if (installDir.startsWith("/")) {
        installDir = installDir.substring(1);
      }
    }
    return installDir;
  }


}
