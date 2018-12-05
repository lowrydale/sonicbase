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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.concurrent.Future;


@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class DescribeStatementHandler {
  private static final String TABLE_NAME_STR = ", tableName=";
  private static final String WIDTH_STR = "Width";
  private static final String SERVER_STR = "server";
  private static final Logger logger = LoggerFactory.getLogger(DescribeStatementHandler.class);

  private final DatabaseClient client;

  public DescribeStatementHandler(DatabaseClient client) {
    this.client = client;
  }

  ResultSet doDescribe(String dbName, String sql) {
    String[] parts = sql.split(" ");
    if (parts[1].trim().equalsIgnoreCase("table")) {
      return doDescribeTable(dbName, parts[2]);
    }
    else if (parts[1].trim().equalsIgnoreCase("tables")) {
      return doDescribeTables(dbName);
    }
    else if (parts[1].trim().equalsIgnoreCase("licenses")) {
      return describeLicenses();
    }
    else if (parts[1].trim().equalsIgnoreCase("index")) {
      return doDescribeIndex(dbName, parts[2]);
    }
    else if (parts[1].trim().equalsIgnoreCase("shards")) {
      return describeShards(dbName);
    }
    else if (parts[1].trim().equalsIgnoreCase("repartitioner")) {
      return null;
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

  private ResultSet doDescribeIndex(String dbName, String part) {
    String str = part.trim().toLowerCase();
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

  private ResultSet doDescribeTables(String dbName) {
    StringBuilder builder = new StringBuilder();
    for (TableSchema tableSchema : client.getCommon().getTables(dbName).values()) {
      builder.append(tableSchema.getName()).append("\n");
    }
    String ret = builder.toString();
    String[] lines = ret.split("\\n");
    return new ResultSetImpl(lines);
  }

  private ResultSet doDescribeTable(String dbName, String part) {
    String table = part.trim().toLowerCase();
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

    doDescribeFields(fields, maxLen, maxTypeLen, maxWidthLen, builder);

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

  private void doDescribeFields(List<FieldSchema> fields, int maxLen, int maxTypeLen, int maxWidthLen, StringBuilder builder) {
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
  }

  private void appendChars(StringBuilder builder, String character, int count) {
    for (int i = 0; i < count; i++) {
      builder.append(character);
    }
  }

  public ResultSet describeLicenses() {
    try {
      TrustManager[] trustAllCerts = new TrustManager[]{
          new X509TrustManager() {
            public java.security.cert.X509Certificate[] getAcceptedIssuers() {
              return null;
            }

            public void checkClientTrusted(X509Certificate[] certs, String authType) {
              //nothing to implement
            }

            public void checkServerTrusted(X509Certificate[] certs, String authType) {
              //nothing to implement
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

      String json = getLicenseServerConfig();
      Config config = new Config(json);

      InputStream in = urlGet("https://" + config.getString("address") + ":" +
          config.getInt("port") + "/license/currUsage");

      ObjectMapper mapper = new ObjectMapper();
      ObjectNode dict = (ObjectNode) mapper.readTree(IOUtils.toString(in, "utf-8"));
      StringBuilder builder = new StringBuilder();
      builder.append("total cores in use=").append(dict.get("totalCores").asInt()).append("\n");
      builder.append("total allocated cores=").append(dict.get("allocatedCores").asInt()).append("\n");
      builder.append("in compliance=").append(dict.get("inCompliance").asBoolean()).append("\n");
      builder.append("disabling now=").append(dict.get("disableNow").asBoolean()).append("\n");
      builder.append("disabling date=").append(dict.get("disableDate").asText()).append("\n");
      builder.append("multiple license servers=").append(dict.get("multipleLicenseServers").asBoolean()).append("\n");
      ArrayNode servers = dict.withArray("clusters");
      for (int i = 0; i < servers.size(); i++) {
        builder.append(servers.get(i).get("cluster").asText()).append("=").append(servers.get(i).get("cores").asInt()).append("\n");
      }

      String ret = builder.toString();
      String[] lines = ret.split("\\n");
      return new ResultSetImpl(lines);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public String getLicenseServerConfig() throws IOException {
    return IOUtils.toString(DatabaseClient.class.getResourceAsStream("/config-license-server.yaml"), "utf-8");
  }

  public InputStream urlGet(String urlStr) throws IOException {
    URL url = new URL(urlStr);

    URLConnection con = url.openConnection();
    return new BufferedInputStream(con.getInputStream());
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


          ComObject cobj = new ComObject(2);
          cobj.put(ComObject.Tag.DB_NAME, "__none__");
          cobj.put(ComObject.Tag.SCHEMA_VERSION, client.getCommon().getSchemaVersion());

          deserializeSchema(j, i, line, cobj);

          serverStatsData.add(line);
        }
      }
      return new ResultSetImpl(serverStatsData);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private void deserializeSchema(int j, int i, Map<String, String> line, ComObject cobj) {
    byte[] ret;
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

        List<Future<Map<String, String>>> futures = describeServerStatsForEachServer();

        waitForFutures(serverStatsData, futures);

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

  private void waitForFutures(List<Map<String, String>> serverStatsData, List<Future<Map<String, String>>> futures) {
    for (Future<Map<String, String>> future : futures) {
      try {
        serverStatsData.add(future.get());
      }
      catch (Exception e) {
        logger.error("Error getting stats", e);
      }
    }
  }

  private List<Future<Map<String, String>>> describeServerStatsForEachServer() {
    List<Future<Map<String, String>>> futures = new ArrayList<>();
    for (int i = 0; i < client.getShardCount(); i++) {
      for (int j = 0; j < client.getReplicaCount(); j++) {
        final int shard = i;
        final int replica = j;
        futures.add(client.getExecutor().submit(() -> doDescribeServerStats(shard, replica)));
      }
    }
    return futures;
  }

  private Map<String, String> doDescribeServerStats(int shard, int replica) {
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
    ComObject cobj = new ComObject(2);
    cobj.put(ComObject.Tag.DB_NAME, "__none__");
    cobj.put(ComObject.Tag.SCHEMA_VERSION, client.getCommon().getSchemaVersion());

    byte[] ret = client.send("OSStatsManager:getOSStats", shard, replica, cobj, DatabaseClient.Replica.SPECIFIED);
    ComObject retObj = new ComObject(ret);

    double resGig = retObj.getDouble(ComObject.Tag.RES_GIG);
    double cpu = retObj.getDouble(ComObject.Tag.CPU);
    double javaMemMin = retObj.getDouble(ComObject.Tag.JAVA_MEM_MIN);
    double javaMemMax = retObj.getDouble(ComObject.Tag.JAVA_MEM_MAX);
    double recRate = retObj.getDouble(ComObject.Tag.AVG_REC_RATE) / 1073741824d;
    double transRate = retObj.getDouble(ComObject.Tag.AVG_TRANS_RATE) / 1073741824d;
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

    private final String table;
    private final String index;
    private final int shard;
    private final String result;
  }



  private ResultSet describeShards(String dbName) {
    while (true) {
      if (client.getShutdown()) {
        throw new DatabaseException("Shutting down");
      }

      try {
        client.syncSchema();

        StringBuilder ret = new StringBuilder();

        Map<String, Entry> entries = putEntries(dbName);

        describeShardsForEachIndex(dbName, ret, entries);

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

  private Map<String, Entry> putEntries(String dbName) {
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
    return entries;
  }

  private void describeShardsForEachIndex(String dbName, StringBuilder ret, Map<String, Entry> entries) {
    for (final Map.Entry<String, TableSchema> table : client.getCommon().getTables(dbName).entrySet()) {
      for (final Map.Entry<String, IndexSchema> indexSchema : table.getValue().getIndices().entrySet()) {
        int shard = 0;
        TableSchema.Partition[] partitions = indexSchema.getValue().getCurrPartitions();
        TableSchema.Partition[] lastPartitions = indexSchema.getValue().getLastPartitions();
        for (int i = 0; i < partitions.length; i++) {
          shard = describeShardsForEachPartitionOfEachIndex(ret, table, indexSchema, shard, partitions[i], lastPartitions, i);
        }
        for (int i = 0; i < client.getShardCount(); i++) {
          ret.append(entries.get(table.getKey() + ":" + indexSchema.getKey() + ":" + i).result);
        }
      }
    }
  }

  private int describeShardsForEachPartitionOfEachIndex(StringBuilder ret, Map.Entry<String, TableSchema> table,
                                                        Map.Entry<String, IndexSchema> indexSchema, int shard,
                                                        TableSchema.Partition partition,
                                                        TableSchema.Partition[] lastPartitions, int i) {
    String key = "[null]";
    if (partition.getUpperKey() != null) {
      key = DatabaseCommon.keyToString(partition.getUpperKey());
    }
    String lastKey = "[null]";
    if (lastPartitions != null && lastPartitions[i].getUpperKey() != null) {
      lastKey = DatabaseCommon.keyToString(lastPartitions[i].getUpperKey());
    }
    ret.append("Table=").append(table.getKey()).append(", Index=").append(indexSchema.getKey()).append(", shard=").append(shard).append(", key=").append(key).append(", lastKey=").append(lastKey).append("\n");
    shard++;
    return shard;
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
