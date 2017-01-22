package com.lowryengineering.database.cli;

import com.lowryengineering.database.client.DatabaseClient;
import com.lowryengineering.database.jdbcdriver.ConnectionProxy;
import com.lowryengineering.database.query.DatabaseException;
import com.lowryengineering.database.schema.DataType;
import com.lowryengineering.database.schema.FieldSchema;
import com.lowryengineering.database.server.DatabaseServer;
import com.lowryengineering.database.util.JsonArray;
import com.lowryengineering.database.util.JsonDict;
import com.lowryengineering.database.util.StreamUtils;
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

import static com.lowryengineering.database.server.DatabaseServer.getMemValue;

public class Cli2 {

  static Thread thread = null;
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

  public static void main(final String[] args) throws IOException, InterruptedException {
    workingDir = new File(System.getProperty("user.dir"));

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
      else if (command.startsWith("insert")) {
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
      else {
        System.out.println("Error, unknown command");
      }
    }
    catch (Exception e) {
      e.printStackTrace();
      System.out.println("Error executing command: msg=" + e.getMessage());
    }
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
    String json = StreamUtils.inputStreamToString(Cli2.class.getResourceAsStream("/config-" + cluster + ".json"));
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
    String json = StreamUtils.inputStreamToString(Cli2.class.getResourceAsStream("/config-" + cluster + ".json"));
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

  private static void reconfigureCluster() throws SQLException, ClassNotFoundException, IOException, InterruptedException {
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
        String json = StreamUtils.inputStreamToString(Cli2.class.getResourceAsStream("/config-" + cluster + ".json"));
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
    String json = StreamUtils.inputStreamToString(Cli2.class.getResourceAsStream("/config-" + cluster + ".json"));
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
      Class.forName("com.lowryengineering.database.jdbcdriver.DriverProxy");

      String db = "";
      if (currDbName != null) {
        db = "/" + currDbName;
      }
      conn = DriverManager.getConnection("jdbc:dbproxy:" + address + ":" + port + db);
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

    closeConnection();
    currDbName = null;

    String json = null;
    try {
      json = StreamUtils.inputStreamToString(Cli2.class.getResourceAsStream("/config-" + cluster + ".json"));
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
    String json = StreamUtils.inputStreamToString(Cli2.class.getResourceAsStream("/config-" + cluster + ".json"));
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

  private static void readdressServers() throws IOException, InterruptedException, SQLException, ClassNotFoundException {
    deploy();
    startCluster();
  }

  private static void startCluster() throws IOException, InterruptedException, SQLException, ClassNotFoundException {
    String cluster = currCluster;
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }

    String json = StreamUtils.inputStreamToString(Cli2.class.getResourceAsStream("/config-" + cluster + ".json"));
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
      ProcessBuilder builder = new ProcessBuilder().command("bin/start-db-server.sh", privateAddress, port, maxHeap, searchHome, cluster);
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

      ProcessBuilder builder = new ProcessBuilder().command("bin/do-start.sh", deployUser + "@" + externalAddress, installDir, privateAddress, port, maxHeap, searchHome, cluster);
      System.out.println("Started server: address=" + externalAddress + ", port=" + port + ", maxJavaHeap=" + maxHeap);
      Process p = builder.start();
      p.waitFor();
    }
  }

  private static void stopServer(JsonDict databaseDict, String externalAddress, String privateAddress, String port, String installDir) throws IOException, InterruptedException {
    String deployUser = databaseDict.getString("user");
    if (externalAddress.equals("127.0.0.1") || externalAddress.equals("localhost")) {
      ProcessBuilder builder = new ProcessBuilder().command("bin/kill-server.sh", "NettyServer", "-host", privateAddress, "-port", port);
      //builder.directory(workingDir);
      Process p = builder.start();
      p.waitFor();
    }
    else {
      ProcessBuilder builder = new ProcessBuilder().command("ssh", "-n", "-f", "-o",
          "UserKnownHostsFile=/dev/null", "-o", "StrictHostKeyChecking=no", deployUser + "@" +
              externalAddress, installDir + "/bin/kill-server.sh", "NettyServer", "-host", privateAddress, "-port", port);
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

  private static void deploy() throws IOException {
    String cluster = currCluster;
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }
    System.out.println("deploying: cluster=" + cluster);

    try {
      String json = StreamUtils.inputStreamToString(Cli2.class.getResourceAsStream("/config-" + cluster + ".json"));
      JsonDict config = new JsonDict(json);
      JsonDict databaseDict = config.getDict("database");
      String deployUser = databaseDict.getString("user");
      String installDir = databaseDict.getString("installDirectory");
      installDir = resolvePath(installDir);
      Set<String> installedAddresses = new HashSet<>();
      JsonArray shards = databaseDict.getArray("shards");
      for (int i = 0; i < shards.size(); i++) {
        JsonArray replicas = shards.getDict(i).getArray("replicas");
        for (int j = 0; j < replicas.size(); j++) {
          JsonDict replica = replicas.getDict(j);
          String externalAddress = replica.getString("publicAddress");
          if (installedAddresses.add(externalAddress)) {
            if (externalAddress.equals("127.0.0.1") || externalAddress.equals("localhost")) {
              continue;
            }
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
      JsonArray clients = databaseDict.getArray("clients");
      if (clients != null) {
        for (int i = 0; i < clients.size(); i++) {
          JsonDict replica = clients.getDict(i);
          String externalAddress = replica.getString("publicAddress");
          if (installedAddresses.add(externalAddress)) {
            if (externalAddress.equals("127.0.0.1") || externalAddress.equals("localhost")) {
              continue;
            }
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
