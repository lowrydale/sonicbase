package com.sonicbase.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.common.ComObject;
import com.sonicbase.common.InsufficientLicense;
import com.sonicbase.common.Logger;
import com.sonicbase.common.ServersConfig;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.queue.MessageQueueConsumer;
import com.sonicbase.schema.FieldSchema;
import com.sonicbase.schema.TableSchema;
import sun.misc.BASE64Decoder;

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class StreamManager {

  private Logger logger;
  private final DatabaseServer server;
  private ConcurrentHashMap<String, Connection> connections = new ConcurrentHashMap<>();
  private List<Thread> threads = new ArrayList<>();
  private boolean shutdown = false;
  private boolean pauseStreaming = false;

  public StreamManager(final DatabaseServer server) {
    this.server = server;
    logger = new Logger(server.getDatabaseClient(), server.getShard(), server.getReplica());

    logger.info("initializing StreamManager");

    Thread thread = new Thread(new Runnable(){
      @Override
      public void run() {
        while (true) {
          try {
            ServersConfig servers = server.getCommon().getServersConfig();
            ServersConfig.Shard[] shards = servers.getShards();
            boolean allShardsAlive = true;
            outer:
            for (ServersConfig.Shard shard : shards) {
              ServersConfig.Host[] hosts = shard.getReplicas();
              boolean alive = false;
              for (ServersConfig.Host host : hosts) {
                if (!host.isDead()) {
                  alive = true;
                  break;
                }
              }
              if (!alive) {
                pauseStreaming = true;
                allShardsAlive = false;
                break outer;
              }
            }
            if (allShardsAlive) {
              pauseStreaming = false;
            }
            Thread.sleep(1000);
          }
          catch (InterruptedException e) {
            break;
          }
          catch (Exception e) {
            logger.error("Error in Queue Health Detector", e);
          }
        }
      }
    });
    thread.start();
  }

  public ComObject startStreaming(ComObject cobj) {
    stopStreaming(cobj);
    shutdown = false;

    final ObjectNode config = server.getConfig();
    ObjectNode queueDict = (ObjectNode) config.get("queue");
    logger.info("Starting queue consumers: queue notNull=" + (queueDict != null));
    if (queueDict != null) {
      if (!server.haveProLicense()) {
        throw new InsufficientLicense("You must have a pro license to use message queue integration");
      }
      logger.info("Starting queues. Have license");

      ArrayNode streams = queueDict.withArray("consumers");
      if (streams != null) {
        for (int i = 0; i < streams.size(); i++) {
          try {
            final ObjectNode stream = (ObjectNode) streams.get(i);
            final String className = stream.get("className").asText();
            Integer threadCount = stream.get("threadCount").asInt();
            if (threadCount == null) {
              threadCount = 1;
            }
            logger.info("starting queue consumer: config=" + stream.toString());
            for (int j = 0; j < threadCount; j++) {
              Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                  try {
                    MessageQueueConsumer consumer = (MessageQueueConsumer) Class.forName(className).newInstance();

                    consumer.init(server.getCluster(), config.toString(), stream.toString());

                    int errorCountInARow = 0;
                    while (true) {
                      while (pauseStreaming) {
                        Thread.sleep(1000);
                      }
                      try {
                        List<com.sonicbase.queue.Message> messages = consumer.receive();
                        processMessages(consumer, messages);
                        errorCountInARow = 0;
                      }
                      catch (Exception e) {
                        logger.error("Error getting messages: config=" + stream.toString(), e);
                        errorCountInARow++;
                        Thread.sleep(100 * Math.min(1000, errorCountInARow));
                      }
                    }
                  }
                  catch (Exception e) {
                    logger.error("Error starting message queue consumer: config=" + stream.toString(), e);
                  }
                }
              });
              threads.add(thread);
              thread.start();
            }
          }
          catch (Exception e) {
            logger.error("Error starting message queue consumer", e);
          }
        }
      }
    }
    return null;
  }

  private void processMessages(MessageQueueConsumer consumer, List<com.sonicbase.queue.Message> messages) {
    for (com.sonicbase.queue.Message messageObj : messages) {
      try {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode message = (ObjectNode) mapper.readTree(messageObj.getBody());
        String dbName = message.get("database").asText();
        initConnection(dbName);
        String tableName = message.get("table").asText();
        String action = message.get("action").asText();
        final TableSchema tableSchema = server.getCommon().getTables(dbName).get(tableName);
        final List<FieldSchema> fields = tableSchema.getFields();
        if (action.equals("upsert")) {

          final StringBuilder fieldsStr = new StringBuilder();
          final StringBuilder parmsStr = new StringBuilder();
          boolean first = true;
          for (FieldSchema field : fields) {
            if (field.getName().equals("_id")) {
              continue;
            }
            if (first) {
              first = false;
            }
            else {
              fieldsStr.append(",");
              parmsStr.append(",");
            }
            fieldsStr.append(field.getName());
            parmsStr.append("?");
          }

          PreparedStatement stmt = connections.get(dbName).prepareStatement("insert into " + tableName + " (" + fieldsStr.toString() +
              ") VALUES (" + parmsStr.toString() + ")");
          try {

            ArrayNode records = message.withArray("records");
            for (int i = 0; i < records.size(); i++) {
              ObjectNode json = (ObjectNode) records.get(i);
              Object[] record = getCurrRecordFromJson(json, fields);
              BulkImportManager.setFieldsInInsertStatement(stmt, record, fields);

              stmt.addBatch();
            }
            stmt.executeBatch();
          }
          finally {
            stmt.close();
          }
        }
        else if (action.equals("delete")) {
          ArrayNode records = message.withArray("records");
          for (int i = 0; i < records.size(); i++) {
            ObjectNode json = (ObjectNode) records.get(i);

            List<FieldSchema> specifiedFields = new ArrayList<>();
            String str = "delete from " + tableName + " where ";
            int offset = 0;
            Iterator<Map.Entry<String, JsonNode>> iterator = json.fields();
            while (iterator.hasNext()) {
              Map.Entry<String, JsonNode> entry = iterator.next();
              if (offset != 0) {
                str += " AND ";
              }
              str += " " + entry.getKey().toLowerCase() + "=? ";
              for (FieldSchema fieldSchema : fields) {
                if (fieldSchema.getName().equals(entry.getKey().toLowerCase())) {
                  specifiedFields.add(fieldSchema);
                }
              }
              offset++;
            }
            Object[] record = getCurrRecordFromJson(json, specifiedFields);

            PreparedStatement stmt = connections.get(dbName).prepareStatement(str);
            BulkImportManager.setFieldsInInsertStatement(stmt, record, specifiedFields);
            stmt.execute();
          }
        }
        consumer.acknowledgeMessage(messageObj);
      }
      catch (Exception e) {
      }
    }
  }

  public ComObject stopStreaming(ComObject cobj) {
    shutdown = true;
    try {
      for (Thread thread : threads) {
        thread.join();
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
    threads.clear();
    return null;
  }

  private void initConnection(String dbName) {
    synchronized (connections) {
      Connection connection = connections.get(dbName);
      if (connection == null) {
        connection = doInitConnection(dbName);
        connections.put(dbName, connection);
      }
    }
  }

  private Connection doInitConnection(String dbName) {
    try {
      ObjectNode dict = server.getConfig();
      ObjectNode databaseDict = dict;
      ArrayNode array = databaseDict.withArray("shards");
      ObjectNode replicaDict = (ObjectNode) array.get(0);
      ArrayNode replicasArray = replicaDict.withArray("replicas");
      final String address = databaseDict.get("clientIsPrivate").asBoolean() ?
          replicasArray.get(0).get("privateAddress").asText() :
          replicasArray.get(0).get("publicAddress").asText();
      final int port = replicasArray.get(0).get("port").asInt();

      Class.forName("com.sonicbase.jdbcdriver.Driver");
      return DriverManager.getConnection("jdbc:sonicbase:" + address + ":" + port + "/" + dbName);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public static Object[] getCurrRecordFromJson(ObjectNode json, List<FieldSchema> fields) throws SQLException {

    ObjectNode newJson = new ObjectNode(JsonNodeFactory.instance);
    Iterator<Map.Entry<String, JsonNode>> iterator = json.fields();
    while (iterator.hasNext()) {
      Map.Entry<String, JsonNode> entry = iterator.next();
      newJson.put(entry.getKey().toLowerCase(), entry.getValue());
    }
    json = newJson;

    final Object[] currRecord = new Object[fields.size()];
    try {
      int offset = 0;
      for (FieldSchema field : fields) {
        String fieldName = field.getName().toLowerCase();
        if (fieldName.equals("_id")) {
          continue;
        }
        switch (field.getType()) {
          case BIT: {
            JsonNode node = json.get(fieldName);
            if (node != null) {
              Boolean value = node.asBoolean();
              if (value != null) {
                currRecord[offset] = value;
              }
            }
          }
          break;
          case TINYINT: {
            JsonNode node = json.get(fieldName);
            if (node != null) {
              Integer value = node.asInt();
              if (value != null) {
                currRecord[offset] = (byte) (int) value;
              }
            }
          }
          break;
          case SMALLINT: {
            JsonNode node = json.get(fieldName);
            if (node != null) {
              Integer value = node.asInt();
              if (value != null) {
                currRecord[offset] = (short) (int) value;
              }
            }
          }
          break;
          case INTEGER: {
            JsonNode node = json.get(fieldName);
            if (node != null) {
              Integer value = node.asInt();
              if (value != null) {
                currRecord[offset] = value;
              }
            }
          }
          break;
          case BIGINT: {
            JsonNode node = json.get(fieldName);
            if (node != null) {
              Long value = node.asLong();
              if (value != null) {
                currRecord[offset] = value;
              }
            }
          }
          break;
          case FLOAT: {
            JsonNode node = json.get(fieldName);
            if (node != null) {
              Double value = node.asDouble();
              if (value != null) {
                currRecord[offset] = value;
              }
            }
          }
          break;
          case REAL: {
            JsonNode node = json.get(fieldName);
            if (node != null) {
              Double value = node.asDouble();
              if (value != null) {
                currRecord[offset] = (float) (double) value;
              }
            }
          }
          break;
          case DOUBLE: {
            JsonNode node = json.get(fieldName);
            if (node != null) {
              Double value = node.asDouble();
              if (value != null) {
                currRecord[offset] = value;
              }
            }
          }
          break;
          case NUMERIC: {
            JsonNode node = json.get(fieldName);
            if (node != null) {
              String value = node.asText();
              if (value != null) {
                currRecord[offset] = new BigDecimal(value);
              }
            }
          }
          break;
          case DECIMAL: {
            JsonNode node = json.get(fieldName);
            if (node != null) {
              String value = node.asText();
              if (value != null) {
                currRecord[offset] = new BigDecimal(value);
              }
            }
          }
          break;
          case CHAR: {
            JsonNode node = json.get(fieldName);
            if (node != null) {
              String value = node.asText();
              if (value != null) {
                currRecord[offset] = value;
              }
            }
          }
          break;
          case VARCHAR: {
            JsonNode node = json.get(fieldName);
            if (node != null) {
              String value = node.asText();
              if (value != null) {
                currRecord[offset] = value;
              }
            }
          }
          break;
          case LONGVARCHAR: {
            JsonNode node = json.get(fieldName);
            if (node != null) {
              String value = node.asText();
              if (value != null) {
                currRecord[offset] = value;
              }
            }
          }
          break;
          case DATE: {
            JsonNode node = json.get(fieldName);
            if (node != null) {
              String value = node.asText();
              if (value != null) {
                currRecord[offset] = Date.valueOf(value);
              }
            }
          }
          break;
          case TIME: {
            JsonNode node = json.get(fieldName);
            if (node != null) {
              String value = node.asText();
              if (value != null) {
                currRecord[offset] = Time.valueOf(value);
              }
            }
          }
          break;
          case TIMESTAMP: {
            JsonNode node = json.get(fieldName);
            if (node != null) {
              String value = node.asText();
              if (value != null) {
                currRecord[offset] = Timestamp.valueOf(value);
              }
            }
          }
          break;
          case BINARY: {
            JsonNode node = json.get(fieldName);
            if (node != null) {
              String value = node.asText();
              if (value != null) {
                currRecord[offset] = new BASE64Decoder().decodeBuffer(value);
              }
            }
          }
          break;
          case VARBINARY: {
            JsonNode node = json.get(fieldName);
            if (node != null) {
              String value = node.asText();
              if (value != null) {
                currRecord[offset] = new BASE64Decoder().decodeBuffer(value);
              }
            }
          }
          break;
          case LONGVARBINARY: {
            JsonNode node = json.get(fieldName);
            if (node != null) {
              String value = node.asText();
              if (value != null) {
                currRecord[offset] = new BASE64Decoder().decodeBuffer(value);
              }
            }
          }
          break;
          case BLOB: {
            JsonNode node = json.get(fieldName);
            if (node != null) {
              String value = node.asText();
              if (value != null) {
                currRecord[offset] = new BASE64Decoder().decodeBuffer(value);
              }
            }
          }
          break;
          case CLOB: {
            JsonNode node = json.get(fieldName);
            if (node != null) {
              String value = node.asText();
              if (value != null) {
                currRecord[offset] = value;
              }
            }
          }
          break;
          case BOOLEAN: {
            JsonNode node = json.get(fieldName);
            if (node != null) {
              Boolean value = node.asBoolean();
              if (value != null) {
                currRecord[offset] = value;
              }
            }
          }
          break;
          case ROWID: {
            JsonNode node = json.get(fieldName);
            if (node != null) {
              Long value = node.asLong();
              if (value != null) {
                currRecord[offset] = value;
              }
            }
          }
          break;
          case NCHAR: {
            JsonNode node = json.get(fieldName);
            if (node != null) {
              String value = node.asText();
              if (value != null) {
                currRecord[offset] = value;
              }
            }
          }
          break;
          case NVARCHAR: {
            JsonNode node = json.get(fieldName);
            if (node != null) {
              String value = node.asText();
              if (value != null) {
                currRecord[offset] = value;
              }
            }
          }
          break;
          case LONGNVARCHAR: {
            JsonNode node = json.get(fieldName);
            if (node != null) {
              String value = node.asText();
              if (value != null) {
                currRecord[offset] = value;
              }
            }
          }
          break;
          case NCLOB: {
            JsonNode node = json.get(fieldName);
            if (node != null) {
              String value = node.asText();
              if (value != null) {
                currRecord[offset] = value;
              }
            }
          }
          break;
        }

        offset++;
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
    return currRecord;
  }

}
