package com.sonicbase.cli;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sonicbase.aws.EC2ToConfig;
import com.sonicbase.common.ComObject;
import com.sonicbase.common.Config;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.FieldSchema;
import jline.console.ConsoleReader;
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
import org.yaml.snakeyaml.Yaml;

import javax.net.ssl.*;
import java.io.*;
import java.security.cert.X509Certificate;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.sonicbase.common.MemUtil.getMemValue;

public class Cli {

  private static final Logger logger = LoggerFactory.getLogger(Cli.class);
  private static final String UTF_8_STR = "utf-8";
  private static final String CYGWIN_STR = "cygwin";
  private static final String CLUSTER_STR = "cluster";
  private static final String COMMAND_STR = "command";
  private static final String DESCRIBE_STR = "describe";
  private static final String ERROR_NOT_USING_A_CLUSTER_STR = "Error, not using a cluster";
  private static final String JSON_STR = ".json";
  private static final String INSTALL_DIRECTORY_STR = "installDirectory";
  private static final String SHARDS_STR = "shards";
  private static final String REPLICAS_STR = "replicas";
  public static final String PRIVATE_ADDRESS_STR = "privateAddress";
  public static final String ERROR_NOT_USING_A_DATABASE_STR = "Error, not using a database";
  public static final String LOCALHOST_STR = "localhost";
  public static final String PUBLIC_ADDRESS_STR = "publicAddress";
  public static final String LOCAL_HOST_NUMS_STR = "127.0.0.1";
  public static final String USER_DIR_STR = "user.dir";
  public static final String GETTING_FILE_STR = "getting file: ";
  public static final String CONFIG_STR = "/config-";


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
  private static String OS = System.getProperty("os.name").toLowerCase();
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
  private final Map<String, CommandInvoker> commands = new HashMap<>();
  private final List<String> commandHistory = new ArrayList<>();
  private final List<HelpItem> helpItems = new ArrayList<>();

  public Cli(String[] args) {
    this.args = args;
  }

  public void start() {

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
      String os = IOUtils.toString(stream, UTF_8_STR);
      stream.close();
      if (os.equalsIgnoreCase(CYGWIN_STR)) {
        OS = CYGWIN_STR;
      }
      p.waitFor();
    }
    catch (Exception e) {
      // not cygwin
    }

    detectJsonFiles();

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
          String localCluster = commandLine.getOptionValue(CLUSTER_STR);
          if (localCluster != null) {
            useCluster(localCluster);
          }
          String localDb = commandLine.getOptionValue("db");
          if (localDb != null) {
            useDatabase(localDb);
          }

          String script = commandLine.getOptionValue("script");
          if (script != null) {
            runScript(script);
            System.exit(0);
          }
          runCommand(commandLine.getOptionValue(COMMAND_STR));
          System.exit(0);
        }
      }

      disable();

      print("\033[2J\033[;H");

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
              String line = reader.readLine();
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
      //do nothing
    }
    catch (Exception e) {
      printException(e);
    }
  }


  private void detectJsonFiles() {
    File dir = new File(System.getProperty(USER_DIR_STR), "config");
    File[] files = dir.listFiles();
    if (files == null) {
      return;
    }
    for (File file : files) {
      if (file.getName().endsWith(".json")) {
        println("It appears you have old json-based config files. If you want to convert them to the new yaml format, type toyaml.");
        return;
      }
    }
  }

  private void convertJsonFilesToYaml() throws IOException {
    File dir = new File(System.getProperty(USER_DIR_STR), "config");
    File[] files = dir.listFiles();
    if (files == null) {
      return;
    }
    for (File file : files) {
      if (file.getName().endsWith(".json")) {
        String json = IOUtils.toString(new FileInputStream(file), "utf-8");
        TypeReference<HashMap<String, Object>> typeRef
            = new TypeReference<HashMap<String, Object>>() {
        };

        File outFile = new File(System.getProperty(USER_DIR_STR),
            "config/" + file.getName().substring(0, file.getName().lastIndexOf('.')) + ".yaml");
        if (outFile.exists()) {
          println("Output file '" + outFile.getName() + "' already exists, skipping");
          continue;
        }
        HashMap<String, Object> root = new ObjectMapper().readValue(json, typeRef);
        if (file.getName().equals("config-license-server.json")) {
          Map<String, Object> server = (Map<String, Object>) root.get("server");
          root.put("publicAddress", server.get("publicAddress"));
          root.put("privateAddress", server.get("privateAddress"));
          root.put("port", server.get("port"));
          root.remove("server");
          String outStr = new Yaml().dumpAsMap(root);
          try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outFile)))) {
            writer.write(outStr);
          }
        }
        else {
          List<Map<String, Object>> newShards = new ArrayList<>();
          List<Map<String, Object>> shards = (List<Map<String, Object>>) root.get("shards");
          for (Map<String, Object> shard : shards) {
            Map<String, Object> newShard = new HashMap<>();
            newShard.put("shard", shard);
            List<Map<String, Object>> newReplicas = new ArrayList<>();
            newShards.add(newShard);
            List<Map<String, Object>> replicas = (List<Map<String, Object>>) shard.get("replicas");
            for (Map<String, Object> replica : replicas) {
              Map<String, Object> newReplica = new HashMap<>();
              newReplica.put("replica", replica);
              newReplicas.add(newReplica);
            }
            shard.put("replicas", newReplicas);
          }
          root.put("shards", newShards);

          List<Map<String, Object>> newClients = new ArrayList<>();
          List<Map<String, Object>> clients = (List<Map<String, Object>>) root.get("clients");
          for (Map<String, Object> client : clients) {
            Map<String, Object> newClient = new HashMap<>();
            newClient.put("client", client);
            newClients.add(newClient);
          }
          root.put("clients", newClients);

          Map<String, Object> streams = (Map<String, Object>) root.get("streams");
          if (streams != null) {
            List<Map<String, Object>> consumers = (List<Map<String, Object>>) streams.get("consumers");
            if (consumers != null) {
              List<Map<String, Object>> newConsumers = new ArrayList<>();
              for (Map<String, Object> consumer : consumers) {
                Map<String, Object> newConsumer = new HashMap<>();
                newConsumer.put("consumer", consumer);
                newConsumers.add(newConsumer);
              }
              streams.put("consumers", newConsumers);
            }
            List<Map<String, Object>> producers = (List<Map<String, Object>>) streams.get("producers");
            if (consumers != null) {
              List<Map<String, Object>> newProducers = new ArrayList<>();
              for (Map<String, Object> producer : producers) {
                Map<String, Object> newProducer = new HashMap<>();
                newProducer.put("producer", producer);
                newProducers.add(newProducer);
              }
              streams.put("producers", newProducers);
            }
          }
          String outStr = new Yaml().dumpAsMap(root);
          try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outFile)))) {
            writer.write(outStr);
          }
        }
        println("Converted file: " + file.getName());
      }
    }
  }



  private void runScript(String script) {
    File file = new File(script);
    if (!file.exists()) {
      file = new File(System.getProperty("user.dir"), "scripts/" + script);
    }
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)))) {
      while (true) {
        String command =  reader.readLine();
        if (command == null) {
          break;
        }
        println(command);
        runCommand(command);
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private CommandLine getCommandLineOptions(String[] args) throws ParseException {
    Options options = new Options();
    Option op = new Option("c", CLUSTER_STR, true, CLUSTER_STR);
    op.setRequired(false);
    options.addOption(op);
    op = new Option("d", "db", true, "db");
    op.setRequired(false);
    options.addOption(op);
    op = new Option("m", COMMAND_STR, true, COMMAND_STR);
    op.setRequired(false);
    options.addOption(op);
    op = new Option("s", "script", true, "script");
    op.setRequired(false);
    options.addOption(op);

    CommandLineParser parser = new DefaultParser();
    return parser.parse(options, args);
  }


  public boolean isWindows() {
    return !OS.contains(CYGWIN_STR) && OS.contains("win");
  }

  public boolean isCygwin() {
    return OS.contains(CYGWIN_STR);
  }

  public boolean isMac() {
    return OS.contains("mac");
  }

  public boolean isUnix() {
    return OS.contains("nux");
  }


  public static void main(final String[] args) {
    Cli cli = new Cli(args);
    cli.start();
  }

  private void writeHeader(String width) {
    width = width.trim();
    print("\033[" + 0 + ";0f");

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
//    if (licenseDaysLeft != null) {
//      builder.append(": License Days Left=").append(licenseDaysLeft);
//    }
    for (int i = builder.length(); i < Integer.valueOf(width) - using.length(); i++) {
      builder.append(" ");
    }
    builder.append(using);

    if (isWindows()) {
      print("\033[0;1;37;46m");
    }
    else {
      print("\033[0;1;38;46m");
      print("\033[38;5;195m");
    }

    print(builder.toString());
    print("\033[49;39m");
  }

  private void moveToBottom() throws IOException, InterruptedException {

    if (plainConsole) {
      print("sonicbase>>");
    }
    else {
      String str = getTerminalSize();
      String[] parts = str.split(",");
      String width = parts[0].trim();
      String height = parts[1].trim();

      writeHeader(width);

      int h = Integer.parseInt(height);
      print("\033[" + h + ";" + 0 + "f");

    }
  }

  public String getTerminalSize() throws IOException, InterruptedException {
    if (false && isCygwin()) {
      com.sonicbase.common.WindowsTerminal terminal = new com.sonicbase.common.WindowsTerminal();
      String values = terminal.getConsoleSize();
      println("stty=" + values);

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
      String width = IOUtils.toString(new FileInputStream(file), UTF_8_STR);
      String height = IOUtils.toString(new FileInputStream(file2), UTF_8_STR);
      FileUtils.deleteQuietly(file);
      FileUtils.deleteQuietly(file2);
      return width + "," + height;
    }
    else {
      String[] cmd = {"/bin/sh", "-c", "stty size </dev/tty"};
      Runtime.getRuntime().exec(cmd);
      ProcessBuilder builder = new ProcessBuilder().command(cmd);
      Process p = builder.start();
      String stty = IOUtils.toString(p.getInputStream(), UTF_8_STR);
      p.waitFor();
      String[] parts = stty.split(" ");
      if (parts.length == 2) {
        return parts[1].trim() + "," + parts[0].trim();
      }
      return 80 + "," + 80;
    }
  }

  public void print(String msg) {
    System.out.print(msg);
  }

  public void println(String msg) {
    System.out.println(msg);
  }

  public void write(int b) {
    System.out.write(b);
  }

  public void printException(Exception e) {
    e.printStackTrace();
  }

  public Config getConfig(String cluster) {
    try {
      InputStream in = Cli.class.getResourceAsStream(CONFIG_STR + cluster + ".yaml");
      if (in == null) {
        File file = new File(System.getProperty(USER_DIR_STR), "../config/config-" + cluster + ".yaml");
        if (file.exists()) {
          in = new FileInputStream(file);
        }
        else {
          file = new File(System.getProperty(USER_DIR_STR), "config/config-" + cluster + ".yaml");
          if (file.exists()) {
            in = new FileInputStream(file);
          }
        }
      }

      String json = IOUtils.toString(in, UTF_8_STR);
      return new Config(json);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  interface CommandInvoker {
    void invoke(String command) throws Exception;
  }

  public void initCommands() {

    commands.put("run script", currCommand->{
      command = command.trim();
      int pos = command.lastIndexOf(" script");
      command = command.substring(pos + " script".length());
      command = command.trim();
      runScript(command);
    });
    commands.put("toyaml", currCommand-> convertJsonFilesToYaml());
    commands.put("disable server", currCommand-> clusterHandler.disableServer(currCommand));
    commands.put("enable server", currCommand-> clusterHandler.enableServer(currCommand));
    commands.put("deploy cluster", currCommand-> clusterHandler.deploy());
    commands.put("deploy license server", currCommand->miscHandler.deployLicenseServer());
    commands.put("start cluster", currCommand->clusterHandler.startCluster());
    commands.put("start server", currCommand->clusterHandler.startServer(currCommand));
    commands.put("restart cluster", currCommand->clusterHandler.rollingRestart());
    commands.put("start license server", currCommand->miscHandler.startLicenseServer());
    commands.put("stop cluster", currCommand->clusterHandler.stopCluster());
    commands.put("stop server", currCommand->clusterHandler.stopServer(currCommand));
    commands.put("stop license server", currCommand->miscHandler.stopLicenseServer());
    commands.put("start backup", currCommand->backupHandler.startBackup());
    commands.put("backup status", currCommand->backupHandler.backupStatus());
    commands.put("start restore", currCommand->backupHandler.startRestore(currCommand));
    commands.put("restore status", currCommand->backupHandler.restoreStatus());
    commands.put("reload server", currCommand->clusterHandler.reloadServer(currCommand));
    commands.put("reload replica", currCommand->clusterHandler.reloadReplica(currCommand));
    commands.put("start shard", currCommand-> {
        int pos = currCommand.lastIndexOf(' ');
        String shard = currCommand.substring(pos + 1);
        clusterHandler.startShard(Integer.valueOf(shard));
      });
    commands.put("stop shard", currCommand-> {
        int pos = currCommand.lastIndexOf(' ');
        String shard = currCommand.substring(pos + 1);
        clusterHandler.stopShard(Integer.valueOf(shard));
      });
    commands.put("purge cluster", currCommand->clusterHandler.purgeCluster());
    commands.put("purge install", currCommand->miscHandler.purgeInstall());
    commands.put("start streams consumers", currCommand->miscHandler.startStreaming());
    commands.put("stop streams consumers", currCommand->miscHandler.stopStreaming());
    commands.put("describe licenses", currCommand->describeHandler.describeLicenses());
    commands.put("quit", currCommand->exit());
    commands.put("exit", currCommand->exit());
    commands.put("healthcheck", currCommand->healthCheck());
    commands.put("echo", currCommand->{
        moveToBottom();
        println(currCommand);
      });
    commands.put("clear", currCommand->{
        print("\033[2J\033[;H");
        moveToBottom();
      });
    commands.put("gather diagnostics", currCommand->miscHandler.gatherDiagnostics());
    commands.put("bulk import status", currCommand->bulkImportHandler.bulkImportStatus());
    commands.put("start bulk import", currCommand->bulkImportHandler.startBulkImport(currCommand));
    commands.put("cancel bulk import", currCommand->bulkImportHandler.cancelBulkImport());
    commands.put("use cluster", currCommand->{
        int pos = currCommand.lastIndexOf(' ');
        String cluster = currCommand.substring(pos + 1);
        useCluster(cluster);
      });
    commands.put("use database", currCommand->{
        int pos = currCommand.lastIndexOf(' ');
        String dbName = currCommand.substring(pos + 1);
        useDatabase(dbName);
      });
    commands.put("start range", currCommand->startRange());
    commands.put("stop range", currCommand->stopRange());
    commands.put("create database", currCommand->{
        int pos = currCommand.lastIndexOf(' ');
        String dbName = currCommand.substring(pos + 1);
        createDatabase(dbName);
      });
    commands.put("select", currCommand->{
        print("\033[2J\033[;H");
        sqlHandler.select(currCommand);
      });
    commands.put("next", currCommand->{
        print("\033[2J\033[;H");
        next();
      });
    commands.put("build config", this::buildConfig);
    commands.put("force rebalance", currCommand->forceRebalance());
    commands.put("insert", currCommand->{
        print("\033[2J\033[;H");
        sqlHandler.insert(currCommand);
      });
    commands.put("update", currCommand->{
        print("\033[2J\033[;H");
        update(currCommand);
      });
    commands.put("delete", currCommand->{
        print("\033[2J\033[;H");
        sqlHandler.delete(currCommand);
      });
    commands.put("truncate", currCommand->{
        print("\033[2J\033[;H");
        sqlHandler.truncate(currCommand);
      });
    commands.put("drop", currCommand->{
        print("\033[2J\033[;H");
        sqlHandler.drop(currCommand);
      });
    commands.put("create", currCommand->{
        print("\033[2J\033[;H");
        sqlHandler.create(currCommand);
      });
    commands.put("alter", currCommand->{
        print("\033[2J\033[;H");
        sqlHandler.alter(currCommand);
      });
    commands.put("help", currCommand->help());
    commands.put(DESCRIBE_STR, currCommand->{
        print("\033[2J\033[;H");
        describeHandler.describe(currCommand);
      });
    commands.put("explain", currCommand->{
        print("\033[2J\033[;H");
        miscHandler.explain(currCommand);
      });
    commands.put("reconfigure cluster", currCommand->clusterHandler.reconfigureCluster());
    commands.put("bench healthcheck", currCommand->benchHandler.benchHealthcheck());
    commands.put("bench start", currCommand->benchHandler.benchStartTest(currCommand));
    commands.put("bench stop", currCommand->benchHandler.benchStopTest(currCommand));
    commands.put("bench stats", currCommand->benchHandler.benchstats(currCommand));
    commands.put("bench resetStats", currCommand->benchHandler.benchResetStats(currCommand));
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
        println("Error, unknown command");
      }
    }
    catch (ExitCliException e) {
      throw e;
    }
    catch (Exception e) {
      printException(e);
      println("Error executing command: msg=" + e.getMessage());
    }
  }

  private void disable() {
    try {
      SSLContext sslc = SSLContext.getInstance("TLS");
      TrustManager[] trustManagerArray = {new NullX509TrustManager()};
      sslc.init(null, trustManagerArray, null);
      HttpsURLConnection.setDefaultSSLSocketFactory(sslc.getSocketFactory());
      HttpsURLConnection.setDefaultHostnameVerifier(new NullHostnameVerifier());
    }
    catch (Exception e) {
      printException(e);
    }
  }

  String getCurrCluster() {
    return currCluster;
  }

  public ConnectionProxy getConn() {
    return conn;
  }

  String getUsername() {
    return username;
  }

  ThreadPoolExecutor getExecutor() {
    return executor;
  }

  String getCurrDbName() {
    return currDbName;
  }

  void setLastCommand(String command) {
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

  ArrayList<List<String>> getServerStatsData() {
    return serverStatsData;
  }

  private class NullX509TrustManager implements X509TrustManager {
    public void checkClientTrusted(X509Certificate[] chain, String authType) {
      println("");
    }

    public void checkServerTrusted(X509Certificate[] chain, String authType) {
      println("");
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


  private Thread rangeThread;

  private void stopRange() {
    if (rangeThread != null) {
      rangeThread.interrupt();
      rangeThread = null;
    }
  }

  boolean healthCheck() throws IOException, InterruptedException {
    final String cluster = getCurrCluster();
    if (cluster == null) {
      println(ERROR_NOT_USING_A_CLUSTER_STR);
      return false;
    }

    final AtomicBoolean allHealthy = new AtomicBoolean(true);
    Config config = getConfig(cluster);
    List<Config.Shard> shards = config.getShards();

    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < shards.size(); i++) {
      final int shard = i;
      final List<Config.Replica> replicas = shards.get(i).getReplicas();
      for (int j = 0; j < replicas.size(); j++) {
        final int replica = j;
        getExecutor().submit((Callable) () -> {
          Config.Replica replicaObj = replicas.get(replica);
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
              println("Server not healthy: shard=" + shard + ", replica=" + replica + ", privateAddress=" +
                  replicaObj.getString(PRIVATE_ADDRESS_STR));
            }
          }
          catch (Exception e) {
            println("Server not healthy... server=" + replicaObj.getString(PRIVATE_ADDRESS_STR));
            logger.error("Server not healthy... server=" + replicaObj.getString(PRIVATE_ADDRESS_STR), e);
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
      println("All servers healthy");
    }
    else {
      println("At least one server is not healthy");
    }
    return allHealthy.get();
  }

  private void startRange() throws SQLException, ClassNotFoundException {
    String cluster = currCluster;
    if (cluster == null) {
      println(ERROR_NOT_USING_A_CLUSTER_STR);
      return;
    }

    if (currDbName == null) {
      println(ERROR_NOT_USING_A_DATABASE_STR);
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
              println("expected " + currOffset + ", got " + curr);
              currOffset = curr;
            }
            currOffset++;
          }
          println("max=" + currOffset);
        }
        catch (Exception e) {
          printException(e);
        }
      }
    });
    rangeThread.start();
  }

  void doGetOSStats(final ConnectionProxy conn) throws InterruptedException, ExecutionException {
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

  void getFile(Config config, File dir, int shard, int replica, String filename) {
    try {
      String installDir = resolvePath(config.getString(INSTALL_DIRECTORY_STR));
      String deployUser = config.getString("user");

      String address = config.getShards().get(shard).getReplicas().get(replica).getString(PUBLIC_ADDRESS_STR);
      if (address.equals(LOCALHOST_STR) || address.equals(LOCAL_HOST_NUMS_STR)) {
        File destFile = new File(dir, filename);
        destFile.getParentFile().mkdirs();
        FileUtils.deleteQuietly(destFile);
        File srcFile = new File(System.getProperty(USER_DIR_STR), filename);
        FileUtils.copyFile(srcFile, destFile);
      }
      else {
        if (isWindows()) {
          println(GETTING_FILE_STR + installDir + filename + " " + dir.getAbsolutePath());
          ComObject cobj = new ComObject();
          cobj.put(ComObject.Tag.DB_NAME, currDbName);
          cobj.put(ComObject.Tag.SCHEMA_VERSION, 1);
          cobj.put(ComObject.Tag.FILENAME, filename);
          byte[] bytes = conn.send("SnapshotManager:getFile", shard, replica, cobj, ConnectionProxy.Replica.SPECIFIED);
          if (bytes != null) {
            File file = new File(dir, filename);
            file.getParentFile().mkdirs();
            FileUtils.deleteQuietly(file);
            ComObject retObj = new ComObject(bytes);
            try (FileOutputStream fileOut = new FileOutputStream(file)) {
              fileOut.write(retObj.getByteArray(ComObject.Tag.BINARY_FILE_CONTENT));
            }
          }
        }
        else {
          println(GETTING_FILE_STR + "rsync -rvlLt -e " +
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
            write(b);
          }
          p.waitFor();
        }
      }
    }
    catch (Exception e) {
      println("Error getting file: " + filename);
    }
  }


  void getFile(Config config, File dir, String address, String filename) {
    try {
      String installDir = resolvePath(config.getString(INSTALL_DIRECTORY_STR));
      String deployUser = config.getString("user");

      if (address.equals(LOCALHOST_STR) || address.equals(LOCAL_HOST_NUMS_STR)) {
        File destFile = new File(dir, filename);
        destFile = new File(dir, destFile.getName());
        destFile.getParentFile().mkdirs();
        FileUtils.deleteQuietly(destFile);
        File srcFile = new File(System.getProperty(USER_DIR_STR), filename);
        logger.info("copying local file: src={}, dest={}", srcFile.getAbsolutePath(), destFile.getAbsolutePath());
        FileUtils.copyFile(srcFile, destFile);
      }
      else {
        if (isWindows()) {
          logger.info("Get file by address not supported on Windows");
        }
        else {
          println(GETTING_FILE_STR + "rsync -rvlLt -e " +
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
            write(b);
          }
          p.waitFor();
        }
      }
    }
    catch (Exception e) {
      println("Error getting file: " + filename);
    }
  }


  private void buildConfig(String command) throws IOException {
    if (currCluster == null) {
      println(ERROR_NOT_USING_A_CLUSTER_STR);
      return;
    }
    if (command.startsWith("build config aws")) {
      buildConfigAws();
      return;
    }

    String[] parts = command.split(" ");
    String filename = parts[parts.length - 1];
    BuildConfig build = new BuildConfig();
    build.buildConfig(currCluster, filename);
  }

  private void buildConfigAws() {
    if (currCluster == null) {
      println(ERROR_NOT_USING_A_CLUSTER_STR);
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
      println(ERROR_NOT_USING_A_CLUSTER_STR);
      return;
    }
    if (currDbName == null) {
      println(ERROR_NOT_USING_A_DATABASE_STR);
      return;
    }

    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, currDbName);
    cobj.put(ComObject.Tag.SCHEMA_VERSION, conn.getSchemaVersion());
    conn.send("PartitionManager:beginRebalance", 0, 0, cobj, ConnectionProxy.Replica.MASTER);
  }


  class HelpItem {
    private final String command;
    private final String shortDescription;

    HelpItem(String command, String shortDescription) {
      this.command = command;
      this.shortDescription = shortDescription;
    }
  }

  {
    helpItems.add(new HelpItem("deploy cluster", "deploys code and configuration to the servers"));
    helpItems.add(new HelpItem("start cluster", "stops and starts all the servers in the cluster"));
    helpItems.add(new HelpItem("use cluster", "marks the active cluster to use for subsequent commands"));
    helpItems.add(new HelpItem("use database", "marks the active database to use for subsequent commands"));
    helpItems.add(new HelpItem("create database", "creates a new database in the current cluster"));
    helpItems.add(new HelpItem("purge cluster", "removes all data associated with the cluster"));
    helpItems.add(new HelpItem("select", "executes a sql select statement"));
    helpItems.add(new HelpItem("insert", "insert a record into a table"));
    helpItems.add(new HelpItem("update", "update a record"));
    helpItems.add(new HelpItem("delete", "deletes the specified records"));
    helpItems.add(new HelpItem(DESCRIBE_STR, "describes a table"));
  }

  private void help() {
    helpItems.sort(Comparator.comparing(o -> o.command));
    for (HelpItem item : helpItems) {
      println(item.command + " - " + item.shortDescription);
    }
  }

  void useDatabase(String dbName) throws SQLException {
    String cluster = currCluster;
    if (cluster == null) {
      println(ERROR_NOT_USING_A_CLUSTER_STR);
      return;
    }
    closeConnection();
    currDbName = dbName.trim().toLowerCase();
  }

  void closeConnection() throws SQLException {
    if (conn != null) {
      conn.close();
      conn = null;
    }
  }

  private void createDatabase(String dbName) throws SQLException, ClassNotFoundException {
    String cluster = currCluster;
    if (cluster == null) {
      println(ERROR_NOT_USING_A_CLUSTER_STR);
      return;
    }
    initConnection();
    dbName = dbName.trim().toLowerCase();
    conn.createDatabase(dbName);
    println("Successfully created database: name=" + dbName);
    useDatabase(dbName);
  }

  static class SelectColumn {
    private String metric;
    private Integer columnOffset;
    private final String name;

    SelectColumn(String name) {
      this.name = name;
    }

    SelectColumn(String name, int columnOffset) {
      this.name = name;
      this.columnOffset = columnOffset;
    }

    SelectColumn(String name, String metric) {
      this.name = name;
      this.metric = metric;
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
      String height = parts[1].trim();

      int currLine = 0;
      println("");
      for (int i = 0; currLine < Integer.valueOf(height) - 3; i++, currLine++) {
        if (!ret.next()) {
          break;
        }
        println(ret.getString(1));
      }
      if (!ret.isLast()) {
        println("next");
      }
    }
    else if (lastCommand.startsWith(DESCRIBE_STR)) {
      String str = getTerminalSize();
      String[] parts = str.split(",");
      String height = parts[1].trim();

      int currLine = 0;
      println("");
      for (int i = 0; currLine < Integer.valueOf(height) - 3; i++, currLine++) {
        if (!ret.next()) {
          break;
        }
        println(ret.getString(1));
      }
      println("next");
    }
    else {
      processResults(lastCommand, conn);
    }
  }

  private void update(String command) throws SQLException, ClassNotFoundException {
    String cluster = currCluster;
    if (cluster == null) {
      println(ERROR_NOT_USING_A_CLUSTER_STR);
      return;
    }

    if (currDbName == null) {
      println(ERROR_NOT_USING_A_DATABASE_STR);
      return;
    }

    initConnection();

    println("Executing update request");

    PreparedStatement stmt = conn.prepareStatement(command);
    int count = stmt.executeUpdate();
    println("Finished update: count=" + count);
  }

  void initConnection() throws ClassNotFoundException, SQLException {
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

  void processResults(String command, ConnectionProxy conn) throws JSQLParserException, SQLException, IOException, InterruptedException {
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
            columns.add(new SelectColumn(field.getName()));
          }
        }
        else {
          SelectExpressionItem item = (SelectExpressionItem) selectItem;

          Alias alias = item.getAlias();
          String columnName = null;
          if (alias != null) {
            columnName = alias.getName().toLowerCase();
            columns.add(new SelectColumn(columnName));
          }
          else {
            Expression expression = item.getExpression();
            if (expression instanceof Function) {
              Function function = (Function) expression;
              columnName = function.getName().toLowerCase();
              columns.add(new SelectColumn(columnName, columnOffset));
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
                columns.add(new SelectColumn(columnName));
              }
            }
          }
          columnOffset++;
        }
      }
    }

    String str = getTerminalSize();
    String[] parts = str.split(",");
    String width = parts[0].trim();
    String height = parts[1].trim();

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

  void displayPageOfData(List<SelectColumn> columns, String width, List<List<String>> data, boolean isLast) {
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
    println(builder.toString());
  }

  private void appendChar(StringBuilder builder, String c, int count) {
    for (int i = 0; i < count; i++) {
      builder.append(c);
    }
  }

  private void useCluster(String cluster) throws SQLException, IOException, InterruptedException {
    cluster = cluster.trim();

    currCluster = cluster;

    benchHandler.initBench(cluster);


    closeConnection();
    currDbName = null;

    Config config = getConfig(cluster);
    List<Config.Shard> shards = config.getShards();
    List<Config.Replica> replicas = shards.get(0).getReplicas();

    if (replicas.size() >= 2 && replicas.size() < 3 && shards.size() == 1) {
      throw new DatabaseException("If you have two or more replicas, you must have at least three total servers");
    }

    if (isWindows()) {
      boolean remote = false;
      for (int shard = 0; shard < shards.size(); shard++) {
        for (int i = 0; i < shards.get(shard).getReplicas().size(); i++) {
          Config.Replica replica = shards.get(shard).getReplicas().get(i);
          String privateAddress = replica.getString(PRIVATE_ADDRESS_STR);
          String publicAddress = replica.getString(PUBLIC_ADDRESS_STR);
          if (!(privateAddress.equalsIgnoreCase(LOCAL_HOST_NUMS_STR) || privateAddress.equalsIgnoreCase(LOCALHOST_STR) ||
              publicAddress.equalsIgnoreCase(LOCAL_HOST_NUMS_STR) || publicAddress.equalsIgnoreCase(LOCALHOST_STR))) {
            remote = true;
            break;
          }
        }
      }
      if (remote) {
        getCredentials(cluster);
      }
    }

    Boolean clientIsPrivate = config.getBoolean("clientIsPrivate");
    if (clientIsPrivate == null) {
      clientIsPrivate = false;
    }
    hosts = new String[shards.get(0).getReplicas().size()];
    for (int i = 0; i < hosts.length; i++) {
      Config.Replica replica = shards.get(0).getReplicas().get(i);
      if (clientIsPrivate) {
        hosts[i] = replica.getString(PRIVATE_ADDRESS_STR);
      }
      else {
        hosts[i] = replica.getString(PUBLIC_ADDRESS_STR);
      }
      hosts[i] += ":" + replica.getInt("port");
    }
  }

  void getCredentials(String cluster) throws IOException, InterruptedException {
    File dir = new File(System.getProperty(USER_DIR_STR), "credentials");
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
      println("Enter credentials for server");

      File script = new File("bin/get-credentials.ps1");
      String str = IOUtils.toString(new FileInputStream(script), UTF_8_STR);
      str = str.replaceAll("\\$1", dir.getAbsolutePath().replaceAll("\\\\", "/"));
      str = str.replaceAll("\\$2", cluster);
      File outFile = new File("tmp/" + cluster + "-get-credentials.ps1");
      outFile.getParentFile().mkdirs();
      FileUtils.deleteQuietly(outFile);
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
  }

  String getMaxHeap(Config config) throws IOException, InterruptedException {
    String maxStr = config.getString("maxJavaHeap");
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
      maxStr = maxStr.substring(0, maxStr.indexOf('%'));
      double maxPercent = Double.parseDouble(maxStr);
      double maxGig = totalGig * (maxPercent / 100);
      maxHeap = (int) Math.floor(maxGig * 1024d) + "m";
    }
    return maxHeap;
  }

  String resolvePath(String installDir) {
    if (installDir.startsWith("$HOME")) {
      installDir = installDir.substring("$HOME".length());
      if (installDir.startsWith("/")) {
        installDir = installDir.substring(1);
      }
    }
    else if (installDir.startsWith("$WORKING_DIR")) {
      installDir = installDir.replace("$WORKING_DIR", System.getProperty(USER_DIR_STR));
    }
    return installDir;
  }


}
