package com.sonicbase.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.request.HttpRequestWithBody;
import com.sonicbase.common.Config;
import com.sonicbase.query.DatabaseException;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.StringReader;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class ElasticsearchProducer implements StreamsProducer {

  private static Logger logger = LoggerFactory.getLogger(ElasticsearchProducer.class);
  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private AtomicReference<Mapping> mapping = new AtomicReference<>();

  private class Mapping {
    private Map<String, Cluster> sbClusters = new HashMap<>();
    private Map<String, EsCluster> esClusters = new HashMap<>();
  }

  @Override
  public void init(Config config, String installDir, Map<String, Object> streamConfig) {
    try {
      logger.info("Elasticsearch producer init - begin");

      readMappings(streamConfig);

      Timer timer = new Timer("SonicBase ElasticsearchProducer Mapping Reader");
      timer.scheduleAtFixedRate(new TimerTask() {
        @Override
        public void run() {
          try {
            readMappings(streamConfig);
          }
          catch (IOException e) {
            logger.error("Error reading mapping file", e);
          }
        }
      }, 60_000, 60_000);

      logger.info("Elasticsearch producer init - end");
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private class EsCluster {
    private String name;
    private String[] hosts;
    private AtomicLong currOffset = new AtomicLong();
  }

  private class Cluster {
    private String name;
    private Map<String, Database> dbs = new HashMap<>();
  }

  private class Database {
    private String name;
    private Map<String, Table> tables = new HashMap<>();
  }

  private class Table {
    private String name;
    private String esClusterName;
    private String esIndex;
    private Map<String, String> fields = new HashMap<>();
  }

  private void readMappings(Map<String, Object> streamConfig) throws IOException {
    String mappingFilename = (String) streamConfig.get("mappingFile");

    String mappingStr = IOUtils.toString(ElasticsearchProducer.class.getResourceAsStream("/" + mappingFilename), "utf-8");
    Map mapping = new Yaml().loadAs(new StringReader(mappingStr), Map.class);

    Mapping newMapping = new Mapping();

    List esClusters = (List) mapping.get("esClusters");
    for (Object esClusterObj : esClusters) {
      Map esCluster = (Map) esClusterObj;
      String clusterName = (String) esCluster.get("name");
      String hosts = (String) esCluster.get("hosts");
      EsCluster cluster = new EsCluster();
      cluster.name = clusterName;
      cluster.hosts = hosts.split(",");
      newMapping.esClusters.put(clusterName, cluster);
    }

    List sbClusters = (List) mapping.get("sbClusters");
    for (Object sbClusterObj : sbClusters) {
      Map sbCluster = (Map) sbClusterObj;
      String sbClusaterName = (String) sbCluster.get("name");

      Cluster cluster = new Cluster();
      cluster.name = sbClusaterName;
      newMapping.sbClusters.put(sbClusaterName, cluster);

      List databases = (List) sbCluster.get("databases");
      for (Object dbObj : databases) {
        Map db = (Map) dbObj;
        String dbName = (String) db.get("name");
        Database database = new Database();
        database.name = dbName;
        cluster.dbs.put(dbName, database);

        List tables = (List) db.get("tables");
        for (Object tableObj : tables) {
          Map table = (Map) tableObj;
          String esClusterName = (String) table.get("esCluster");
          String tableName = (String) table.get("name");
          String esIndex = (String) table.get("esIndex");
          Map fields = (Map) table.get("fields");

          Table sbTable = new Table();
          sbTable.name = tableName;
          sbTable.esClusterName = esClusterName;
          sbTable.esIndex = esIndex;
          for (Map.Entry<String, Object> entry : ((Map<String, Object>)fields).entrySet()) {
            sbTable.fields.put(((String)entry.getKey()).toLowerCase(), (String)entry.getValue());
          }
          database.tables.put(sbTable.name, sbTable);
        }
      }
    }

    this.mapping.set(newMapping);
  }

  @Override
  public void publish(List<String> messages) {
    try {
      Map<String, StringBuilder> batches = new HashMap<>();

      for (String message : messages) {
        JsonNode node = OBJECT_MAPPER.readTree(message);
        JsonNode events = node.withArray("events");
        for (int i = 0; i < events.size(); i++) {
          JsonNode record = events.get(i);
          String clusterName = record.get("_sonicbase_clustername").asText();
          String dbname = record.get("_sonicbase_dbname").asText();
          String tablename = record.get("_sonicbase_tablename").asText();
          StringBuilder builder = batches.computeIfAbsent(clusterName + ":" + dbname + ":" + tablename, (k) -> new StringBuilder());
          if ("UPDATE".equals(record.get("_sonicbase_action").asText())) {
            record = record.get("after");

            long id = record.get("_sonicbase_id").asLong();
            JsonNode esNode = OBJECT_MAPPER.createObjectNode();
            ObjectNode idField = ((ObjectNode) esNode).putObject("index");

            Table table = mapping.get().sbClusters.get(clusterName).dbs.get(dbname).tables.get(tablename);

            idField.put("_index", table.esIndex);
            idField.put("_type", "_doc");
            idField.put("_id", "_sonicbase_" + id);
            builder.append(esNode.toString()).append("\n");

            esNode = OBJECT_MAPPER.createObjectNode();

            setFields(mapping.get().sbClusters.get(clusterName).dbs.get(dbname).tables.get(tablename), record, (ObjectNode) esNode);

            builder.append(esNode.toString()).append("\n");
          }
          else if ("INSERT".equals(record.get("_sonicbase_action").asText())) {
            record = record.get("record");

            long id = record.get("_sonicbase_id").asLong();
            JsonNode esNode = OBJECT_MAPPER.createObjectNode();
            ObjectNode idField = ((ObjectNode) esNode).putObject("index");

            Table table = mapping.get().sbClusters.get(clusterName).dbs.get(dbname).tables.get(tablename);

            idField.put("_index", table.esIndex);
            idField.put("_type", "_doc");
            idField.put("_id", "_sonicbase_" + id);
            builder.append(esNode.toString()).append("\n");

            esNode = OBJECT_MAPPER.createObjectNode();

            setFields(mapping.get().sbClusters.get(clusterName).dbs.get(dbname).tables.get(tablename), record, (ObjectNode) esNode);
            builder.append(esNode.toString()).append("\n");

          }
          else if ("DELETE".equals(record.get("_sonicbase_action"))) {
            record = record.get("record");

            long id = record.get("_sonicbase_id").asLong();

            JsonNode esNode = OBJECT_MAPPER.createObjectNode();
            ObjectNode idField = ((ObjectNode) esNode).putObject("delete");

            Table table = mapping.get().sbClusters.get(clusterName).dbs.get(dbname).tables.get(tablename);

            idField.put("_index", table.esIndex);
            idField.put("_type", "_doc");
            idField.put("_id", "_sonicbase_" + id);
            builder.append(esNode.toString()).append("\n");
          }
        }
      }

      for (Map.Entry<String, StringBuilder> tableEntry : batches.entrySet()) {
        String[] parts = tableEntry.getKey().split(":");
        Table table = mapping.get().sbClusters.get(parts[0]).dbs.get(parts[1]).tables.get(parts[2]);
        EsCluster esCluster = mapping.get().esClusters.get(table.esClusterName);

        final HttpRequestWithBody request = Unirest.post("http://" +
            esCluster.hosts[(int) (esCluster.currOffset.incrementAndGet() % esCluster.hosts.length)] + "/_bulk");
        request.header("Content-Type", "Application/Json");
        request.body(tableEntry.getValue().toString());
        try {
          HttpResponse<String> response = request.asString();
          if (response.getStatus() != 200) {
            throw new DatabaseException("Error sending Elasticsearch request: status=" + response.getStatus() + ", response=" + response.getBody());
          }
        }
        catch (Exception e) {
          logger.error("Error inserting record: table={}", tableEntry.getKey(), e);
        }
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private void setFields(Table table, JsonNode record, ObjectNode esNode) {
    Iterator<Map.Entry<String, JsonNode>> iterator = record.fields();
    while (iterator.hasNext()) {
      Map.Entry<String, JsonNode> field = iterator.next();

      String esField = table.fields.get(field.getKey().toLowerCase());
      if (esField != null) {
        esNode.set(esField, field.getValue());
      }
    }
  }

  @Override
  public void shutdown() {
  }
}
