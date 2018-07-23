package com.sonicbase.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.common.*;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.query.ResultSet;
import com.sonicbase.query.impl.ResultSetImpl;
import com.sonicbase.schema.FieldSchema;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.PartitionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.exception.ExceptionUtils;

import javax.net.ssl.*;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;


public class DescribeStatementHandler {
  public static final String TABLE_NAME_STR = ", tableName=";
  public static final String WIDTH_STR = "Width";
  public static final String SERVER_STR = "server";
  private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("com.sonicbase.logger");

  private final DatabaseClient client;

  public DescribeStatementHandler(DatabaseClient client) {
    this.client = client;
  }

  public ResultSet doDescribe(String dbName, String sql) throws InterruptedException, ExecutionException, IOException {
    String[] parts = sql.split(" ");
    if (parts[1].trim().equalsIgnoreCase("table")) {
      String table = parts[2].trim().toLowerCase();
      TableSchema tableSchema = client.getCommon().getTables(dbName).get(table);
      if (tableSchema == null) {
        throw new DatabaseException("Table not defined: dbName=" + dbName + TABLE_NAME_STR + table);
      }
      List<FieldSchema> fields = tableSchema.getFields();
      int maxLen = 0;
      int maxTypeLen = 0;
      int maxWidthLen = 0;
      for (FieldSchema field : fields) {
        maxLen = Math.max("Name".length(), Math.max(field.getName().length(), maxLen));
        maxTypeLen = Math.max("Type".length(), Math.max(field.getType().name().length(), maxTypeLen));
        maxWidthLen = Math.max(WIDTH_STR.length(), Math.max(String.valueOf(field.getWidth()).length(), maxWidthLen));
      }

      int totalWidth = "| ".length() + maxLen + " | ".length() + maxTypeLen + " | ".length() + maxWidthLen + " |".length();

      StringBuilder builder = new StringBuilder();

      appendChars(builder, "-", totalWidth);
      builder.append("\n");

      builder.append("| Name");
      appendChars(builder, " ", maxLen - "Name".length());
      builder.append(" | Type");
      appendChars(builder, " ", maxTypeLen - "Type".length());
      builder.append(" | Width");
      appendChars(builder, " ", maxWidthLen - WIDTH_STR.length());
      builder.append(" |\n");
      appendChars(builder, "-", totalWidth);
      builder.append("\n");
      for (FieldSchema field : fields) {
        if (field.getName().equals("_sonicbase_id")) {
          continue;
        }
        builder.append("| ");
        builder.append(field.getName());
        appendChars(builder, " ", maxLen - field.getName().length());
        builder.append(" | ");
        builder.append(field.getType().name());
        appendChars(builder, " ", maxTypeLen - field.getType().name().length());
        builder.append(" | ");
        builder.append(String.valueOf(field.getWidth()));
        appendChars(builder, " ", maxWidthLen - String.valueOf(field.getWidth()).length());
        builder.append(" |\n");
      }
      appendChars(builder, "-", totalWidth);
      builder.append("\n");

      for (IndexSchema indexSchema : tableSchema.getIndices().values()) {
        builder.append("Index=").append(indexSchema.getName()).append("\n");
        doDescribeOneIndex(tableSchema, indexSchema, builder);
      }

      String ret = builder.toString();
      String[] lines = ret.split("\\n");
      return new ResultSetImpl(lines);
    }
    else if (parts[1].trim().equalsIgnoreCase("tables")) {
      StringBuilder builder = new StringBuilder();
      for (TableSchema tableSchema : client.getCommon().getTables(dbName).values()) {
        builder.append(tableSchema.getName() + "\n");
      }
      String ret = builder.toString();
      String[] lines = ret.split("\\n");
      return new ResultSetImpl(lines);
    }
    else if (parts[1].trim().equalsIgnoreCase("licenses")) {
      return describeLicenses();
    }
    else if (parts[1].trim().equalsIgnoreCase("index")) {
      String str = parts[2].trim().toLowerCase();
      String[] innerParts = str.split("\\.");
      String table = innerParts[0].toLowerCase();
      if (innerParts.length == 1) {
        throw new DatabaseException("Must specify <table name>.<index name>");
      }
      String index = innerParts[1].toLowerCase();
      StringBuilder builder = new StringBuilder();
      doDescribeIndex(dbName, table, index, builder);

      String ret = builder.toString();
      String[] lines = ret.split("\\n");
      return new ResultSetImpl(lines);
    }
    else if (parts[1].trim().equalsIgnoreCase("shards")) {
      return describeShards(dbName);
    }
    else if (parts[1].trim().equalsIgnoreCase("repartitioner")) {
      return describeRepartitioner(dbName);
    }
    else if (parts[1].trim().equalsIgnoreCase(SERVER_STR) &&
        parts[2].trim().equalsIgnoreCase("stats")) {
      return describeServerStats();
    }
    else if (parts[1].trim().equalsIgnoreCase(SERVER_STR) &&
        parts[2].trim().equalsIgnoreCase("health")) {
      return describeServerHeath();
    }
    else if (parts[1].trim().equalsIgnoreCase("schema") &&
        parts[2].trim().equalsIgnoreCase("version")) {
      return describeSchemaVersion();
    }
    else {
      throw new DatabaseException("Unknown target for describe: target=" + parts[1]);
    }

  }

  private void appendChars(StringBuilder builder, String character, int count) {
    for (int i = 0; i < count; i++) {
      builder.append(character);
    }
  }




  public static ResultSet describeLicenses() {
    try {
      TrustManager[] trustAllCerts = new TrustManager[]{
          new X509TrustManager() {
            public java.security.cert.X509Certificate[] getAcceptedIssuers() {
              return null;
            }

            public void checkClientTrusted(X509Certificate[] certs, String authType) {
            }

            public void checkServerTrusted(X509Certificate[] certs, String authType) {
            }

          }
      };

      SSLContext sc = SSLContext.getInstance("SSL");
      sc.init(null, trustAllCerts, new java.security.SecureRandom());
      HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());

      // Create all-trusting host name verifier
      HostnameVerifier allHostsValid = (hostname, session) -> true;
      // Install the all-trusting host verifier
      HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);
      /*
       * end of the fix
       */

      String json = IOUtils.toString(DatabaseClient.class.getResourceAsStream("/config-license-server.json"), "utf-8");
      ObjectMapper mapper = new ObjectMapper();
      ObjectNode config = (ObjectNode) mapper.readTree(json);

      URL url = new URL("https://" + config.get(SERVER_STR).get("publicAddress").asText() + ":" +
          config.get(SERVER_STR).get("port").asInt() + "/license/currUsage");
      URLConnection con = url.openConnection();
      InputStream in = new BufferedInputStream(con.getInputStream());

      ObjectNode dict = (ObjectNode) mapper.readTree(IOUtils.toString(in, "utf-8"));
      StringBuilder builder = new StringBuilder();
      builder.append("total cores in use=" + dict.get("totalCores").asInt() + "\n");
      builder.append("total allocated cores=" + dict.get("allocatedCores").asInt() + "\n");
      builder.append("in compliance=" + dict.get("inCompliance").asBoolean() + "\n");
      builder.append("disabling now=" + dict.get("disableNow").asBoolean() + "\n");
      builder.append("disabling date=" + dict.get("disableDate").asText() + "\n");
      builder.append("multiple license servers=" + dict.get("multipleLicenseServers").asBoolean() + "\n");
      ArrayNode servers = dict.withArray("clusters");
      for (int i = 0; i < servers.size(); i++) {
        builder.append(servers.get(i).get("cluster").asText() + "=" + servers.get(i).get("cores").asInt() + "\n");
      }

      String ret = builder.toString();
      String[] lines = ret.split("\\n");
      return new ResultSetImpl(lines);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private ResultSet describeServerHeath() {
    try {
      client.syncSchema();

      if (!client.getCommon().haveProLicense()) {
        throw new InsufficientLicense("You must have a pro license to describe server health");
      }

      List<Map<String, String>> serverStatsData = new ArrayList<>();

      ServersConfig.Shard[] shards = client.getCommon().getServersConfig().getShards();
      for (int j = 0; j < shards.length; j++) {
        ServersConfig.Shard shard = shards[j];
        ServersConfig.Host[] replicas = shard.getReplicas();
        for (int i = 0; i < replicas.length; i++) {
          ServersConfig.Host replica = replicas[i];
          Map<String, String> line = new HashMap<>();
          line.put("host", replica.getPrivateAddress() + ":" + replica.getPort());
          line.put("shard", String.valueOf(j));
          line.put("replica", String.valueOf(i));
          line.put("dead", String.valueOf(replica.isDead()));
          line.put("master", String.valueOf(shard.getMasterReplica() == i));
          serverStatsData.add(line);
        }
      }
      return new ResultSetImpl(serverStatsData);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private ResultSet describeSchemaVersion() {
    try {
      if (!client.getCommon().haveProLicense()) {
        throw new InsufficientLicense("You must have a pro license to describe schema version");
      }

      List<Map<String, String>> serverStatsData = new ArrayList<>();

      ServersConfig.Shard[] shards = client.getCommon().getServersConfig().getShards();
      for (int j = 0; j < shards.length; j++) {
        ServersConfig.Shard shard = shards[j];
        ServersConfig.Host[] replicas = shard.getReplicas();
        for (int i = 0; i < replicas.length; i++) {
          ServersConfig.Host replica = replicas[i];
          Map<String, String> line = new HashMap<>();
          line.put("host", replica.getPrivateAddress() + ":" + replica.getPort());
          line.put("shard", String.valueOf(j));
          line.put("replica", String.valueOf(i));


          ComObject cobj = new ComObject();
          cobj.put(ComObject.Tag.DB_NAME, "__none__");
          cobj.put(ComObject.Tag.SCHEMA_VERSION, client.getCommon().getSchemaVersion());
          byte[] ret = null;
          try {
            ret = client.send("DatabaseServer:getSchema", j, i, cobj, DatabaseClient.Replica.SPECIFIED);
            ComObject retObj = new ComObject(ret);
            DatabaseCommon tmpCommon = new DatabaseCommon();
            tmpCommon.deserializeSchema(retObj.getByteArray(ComObject.Tag.SCHEMA_BYTES));
            line.put("version", String.valueOf(tmpCommon.getSchemaVersion()));
          }
          catch (Exception e) {
            logger.error("Error getting schema from server: shard=" + j + ", replica=" + i, e);
          }

          serverStatsData.add(line);
        }
      }
      return new ResultSetImpl(serverStatsData);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private ResultSetImpl describeServerStats()  {
    while (true) {
      if (client.getShutdown()) {
        throw new DatabaseException("Shutting down");
      }

      try {
        client.syncSchema();

        if (!client.getCommon().haveProLicense()) {
          throw new InsufficientLicense("You must have a pro license to describe server stats");
        }

        List<Map<String, String>> serverStatsData = new ArrayList<>();

        List<Future<Map<String, String>>> futures = new ArrayList<>();
        for (int i = 0; i < client.getShardCount(); i++) {
          for (int j = 0; j < client.getReplicaCount(); j++) {
            final int shard = i;
            final int replica = j;
            boolean dead = client.getServersArray()[i][j].isDead();
            if (dead) {
              continue;
            }
            futures.add(client.getExecutor().submit(() -> {
              boolean dead1 = client.getCommon().getServersConfig().getShards()[shard].getReplicas()[replica].isDead();
              if (dead1) {
                Map<String, String> line = new HashMap<>();
                line.put("host", "shard=" + shard + ", replica=" + replica);
                line.put("cpu", "?");
                line.put("resGig", "?");
                line.put("javaMemMin", "?");
                line.put("javaMemMax", "?");
                line.put("receive", "?");
                line.put("transmit", "?");
                line.put("diskAvail", "?");
                return line;
              }
              ComObject cobj = new ComObject();
              cobj.put(ComObject.Tag.DB_NAME, "__none__");
              cobj.put(ComObject.Tag.SCHEMA_VERSION, client.getCommon().getSchemaVersion());
              cobj.put(ComObject.Tag.METHOD, "OSStatsManager:getOSStats");

              byte[] ret = client.send(null, shard, replica, cobj, DatabaseClient.Replica.SPECIFIED);
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

              Map<String, String> line = new HashMap<>();

              line.put("host", host + ":" + port);
              line.put("cpu", String.format("%.0f", cpu));
              line.put("resGig", String.format("%.2f", resGig));
              line.put("javaMemMin", String.format("%.2f", javaMemMin));
              line.put("javaMemMax", String.format("%.2f", javaMemMax));
              line.put("receive", String.format("%.4f", recRate));
              line.put("transmit", String.format("%.4f", transRate));
              line.put("diskAvail", diskAvail);
              return line;
            }));

          }
        }

        for (Future<Map<String, String>> future : futures) {
          try {
            serverStatsData.add(future.get());
          }
          catch (Exception e) {
            logger.error("Error getting stats", e);
          }
        }
        return new ResultSetImpl(serverStatsData);
      }
      catch (Exception e) {
        int index = ExceptionUtils.indexOfThrowable(e, SchemaOutOfSyncException.class);
        if (-1 != index) {
          continue;
        }
        throw new DatabaseException(e);
      }
    }

  }

  class Entry {
    public Entry(String table, String index, int shard, String result) {
      this.table = table;
      this.index = index;
      this.shard = shard;
      this.result = result;
    }

    private String getKey() {
      return table + ":" + index + ":" + shard;
    }

    private String table;
    private String index;
    private int shard;
    private String result;
  }



  private ResultSet describeShards(String dbName) {
    while (true) {
      if (client.getShutdown()) {
        throw new DatabaseException("Shutting down");
      }

      try {
        client.syncSchema();

        StringBuilder ret = new StringBuilder();

        Map<String, Entry> entries = new HashMap<>();
        PartitionUtils.GlobalIndexCounts counts = PartitionUtils.getIndexCounts(dbName, client);
        for (Map.Entry<String, PartitionUtils.TableIndexCounts> tableEntry : counts.getTables().entrySet()) {
          for (Map.Entry<String, PartitionUtils.IndexCounts> indexEntry : tableEntry.getValue().getIndices().entrySet()) {
            Map<Integer, Long> currCounts = indexEntry.getValue().getCounts();
            for (Map.Entry<Integer, Long> countEntry : currCounts.entrySet()) {
              Entry entry = new Entry(tableEntry.getKey(), indexEntry.getKey(), countEntry.getKey(), "Table=" +
                  tableEntry.getKey() + ", Index=" + indexEntry.getKey() +
                  ", Shard=" + countEntry.getKey() + ", count=" + countEntry.getValue() + "\n");
              entries.put(entry.getKey(), entry);
            }
          }
        }

        for (final Map.Entry<String, TableSchema> table : client.getCommon().getTables(dbName).entrySet()) {
          for (final Map.Entry<String, IndexSchema> indexSchema : table.getValue().getIndices().entrySet()) {
            int shard = 0;
            TableSchema.Partition[] partitions = indexSchema.getValue().getCurrPartitions();
            TableSchema.Partition[] lastPartitions = indexSchema.getValue().getLastPartitions();
            for (int i = 0; i < partitions.length; i++) {
              String key = "[null]";
              if (partitions[i].getUpperKey() != null) {
                key = DatabaseCommon.keyToString(partitions[i].getUpperKey());
              }
              String lastKey = "[null]";
              if (lastPartitions != null && lastPartitions[i].getUpperKey() != null) {
                lastKey = DatabaseCommon.keyToString(lastPartitions[i].getUpperKey());
              }
              ret.append("Table=" + table.getKey() + ", Index=" + indexSchema.getKey() + ", shard=" + shard + ", key=" +
                  key).append(", lastKey=").append(lastKey).append("\n");
              shard++;
            }
            for (int i = 0; i < client.getShardCount(); i++) {
              ret.append(entries.get(table.getKey() + ":" + indexSchema.getKey() + ":" + i).result);
            }
          }
        }

        String retStr = ret.toString();
        String[] lines = retStr.split("\\n");
        return new ResultSetImpl(lines);
      }
      catch (Exception e) {
        int index = ExceptionUtils.indexOfThrowable(e, SchemaOutOfSyncException.class);
        if (-1 != index) {
          continue;
        }
        throw new DatabaseException(e);
      }
    }
  }

  class ShardState {
    private int shard;
    private long count;
    private String exception;
  }

  public ResultSetImpl describeRepartitioner(String dbName) {
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, dbName);
    cobj.put(ComObject.Tag.SCHEMA_VERSION, client.getCommon().getSchemaVersion());
    byte[] ret = client.sendToMaster("PartitionManager:getRepartitionerState", cobj);
    ComObject retObj = new ComObject(ret);

    StringBuilder builder = new StringBuilder();
    String state = retObj.getString(ComObject.Tag.STATE);
    builder.append("state=" + state).append("\n");
    if (state.equals("rebalancing")) {
      builder.append("table=").append(retObj.getString(ComObject.Tag.TABLE_NAME)).append("\n");
      builder.append("index=").append(retObj.getString(ComObject.Tag.INDEX_NAME)).append("\n");
      builder.append("shards:\n");
      List<ShardState> shards = new ArrayList<>();
      ComArray array = retObj.getArray(ComObject.Tag.SHARDS);
      for (int i = 0; i < array.getArray().size(); i++) {
        ShardState shardState = new ShardState();
        shardState.shard = ((ComObject) array.getArray().get(i)).getInt(ComObject.Tag.SHARD);
        shardState.count = ((ComObject) array.getArray().get(i)).getLong(ComObject.Tag.COUNT_LONG);
        shardState.exception = ((ComObject) array.getArray().get(i)).getString(ComObject.Tag.EXCEPTION);

        shards.add(shardState);
      }
      Collections.sort(shards, Comparator.comparingInt(o -> o.shard));
      for (ShardState shardState : shards) {
        builder.append("shard " + shardState.shard + "=" + shardState.count).append("\n");
        if (shardState.exception != null) {
          builder.append(shardState.exception.substring(0, 300));
        }
      }
    }
    String retStr = builder.toString();
    String[] lines = retStr.split("\\n");
    return new ResultSetImpl(lines);
  }

  private StringBuilder doDescribeIndex(String dbName, String table, String index, StringBuilder builder) {
    TableSchema tableSchema = client.getCommon().getTables(dbName).get(table);
    if (tableSchema == null) {
      throw new DatabaseException("Table not defined: dbName=" + dbName + TABLE_NAME_STR + table);
    }

    int countFound = 0;
    for (IndexSchema indexSchema : tableSchema.getIndices().values()) {
      if (!indexSchema.getName().contains(index)) {
        continue;
      }
      countFound++;
      doDescribeOneIndex(tableSchema, indexSchema, builder);

    }
    if (countFound == 0) {
      throw new DatabaseException("Index not defined: dbName=" + dbName + TABLE_NAME_STR + table + ", indexName=" + index);
    }
    return builder;
  }

  private void doDescribeOneIndex(TableSchema tableSchema, IndexSchema indexSchema, StringBuilder builder) {
    String[] fields = indexSchema.getFields();
    int maxLen = 0;
    int maxTypeLen = 0;
    int maxWidthLen = 0;
    for (String field : fields) {
      FieldSchema fieldSchema = tableSchema.getFields().get(tableSchema.getFieldOffset(field));
      maxLen = Math.max("Name".length(), Math.max(fieldSchema.getName().length(), maxLen));
      maxTypeLen = Math.max("Type".length(), Math.max(fieldSchema.getType().name().length(), maxTypeLen));
      maxWidthLen = Math.max(WIDTH_STR.length(), Math.max(String.valueOf(fieldSchema.getWidth()).length(), maxWidthLen));
    }

    int totalWidth = "| ".length() + maxLen + " | ".length() + maxTypeLen + " | ".length() + maxWidthLen + " |".length();

    appendChars(builder, "-", totalWidth);
    builder.append("\n");

    builder.append("| Name");
    appendChars(builder, " ", maxLen - "Name".length());
    builder.append(" | Type");
    appendChars(builder, " ", maxTypeLen - "Type".length());
    builder.append(" | Width");
    appendChars(builder, " ", maxWidthLen - WIDTH_STR.length());
    builder.append(" |\n");
    appendChars(builder, "-", totalWidth);
    builder.append("\n");
    for (String field : fields) {
      FieldSchema fieldSchema = tableSchema.getFields().get(tableSchema.getFieldOffset(field));
      builder.append("| ");
      builder.append(fieldSchema.getName());
      appendChars(builder, " ", maxLen - fieldSchema.getName().length());
      builder.append(" | ");
      builder.append(fieldSchema.getType().name());
      appendChars(builder, " ", maxTypeLen - fieldSchema.getType().name().length());
      builder.append(" | ");
      builder.append(String.valueOf(fieldSchema.getWidth()));
      appendChars(builder, " ", maxWidthLen - String.valueOf(fieldSchema.getWidth()).length());
      builder.append(" |\n");
    }
    appendChars(builder, "-", totalWidth);
    builder.append("\n");
  }

}
