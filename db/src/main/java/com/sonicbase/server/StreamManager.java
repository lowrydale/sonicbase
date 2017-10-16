package com.sonicbase.server;

import com.sonicbase.common.ComObject;
import com.sonicbase.common.InsufficientLicense;
import com.sonicbase.common.Logger;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.queue.MessageQueueConsumer;
import com.sonicbase.schema.FieldSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.JsonArray;
import com.sonicbase.util.JsonDict;
import sun.misc.BASE64Decoder;

import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.util.*;
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
            DatabaseServer.ServersConfig servers = server.getCommon().getServersConfig();
            DatabaseServer.Shard[] shards = servers.getShards();
            boolean allShardsAlive = true;
            outer:
            for (DatabaseServer.Shard shard : shards) {
              DatabaseServer.Host[] hosts = shard.getReplicas();
              boolean alive = false;
              for (DatabaseServer.Host host : hosts) {
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

    final JsonDict config = server.getConfig();
    JsonDict queueDict = config.getDict("queue");
    logger.info("Starting queue consumers: queue notNull=" + (queueDict != null));
    if (queueDict != null) {
      if (!server.haveProLicense()) {
        throw new InsufficientLicense("You must have a pro license to use message queue integration");
      }
      logger.info("Starting queues. Have license");

      JsonArray streams = queueDict.getArray("consumers");
      if (streams != null) {
        for (int i = 0; i < streams.size(); i++) {
          try {
            final JsonDict stream = streams.getDict(i);
            final String className = stream.getString("className");
            Integer threadCount = stream.getInt("threadCount");
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
        JsonDict message = new JsonDict(messageObj.getBody());
        String dbName = message.getString("database");
        initConnection(dbName);
        String tableName = message.getString("table");
        String action = message.getString("action");
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

            JsonArray records = message.getArray("records");
            for (int i = 0; i < records.size(); i++) {
              JsonDict json = records.getDict(i);
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
          JsonArray records = message.getArray("records");
          for (int i = 0; i < records.size(); i++) {
            JsonDict json = records.getDict(i);

            List<FieldSchema> specifiedFields = new ArrayList<>();
            String str = "delete from " + tableName + " where ";
            int offset = 0;
            for (Map.Entry<String, Object> entry : json.getEntrySet()) {
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
      JsonDict dict = server.getConfig();
      JsonDict databaseDict = dict;
      JsonArray array = databaseDict.getArray("shards");
      JsonDict replicaDict = array.getDict(0);
      JsonArray replicasArray = replicaDict.getArray("replicas");
      final String address = databaseDict.getBoolean("clientIsPrivate") ?
          replicasArray.getDict(0).getString("privateAddress") :
          replicasArray.getDict(0).getString("publicAddress");
      final int port = replicasArray.getDict(0).getInt("port");

      Class.forName("com.sonicbase.jdbcdriver.Driver");
      return DriverManager.getConnection("jdbc:sonicbase:" + address + ":" + port + "/" + dbName);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public static Object[] getCurrRecordFromJson(JsonDict json, List<FieldSchema> fields) throws SQLException {

    JsonDict newJson = new JsonDict();
    for (Map.Entry<String, Object> entry : json.getEntrySet()) {
      newJson.putObject(entry.getKey().toLowerCase(), entry.getValue());
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
            Boolean value = json.getBoolean(fieldName);
            if (value != null) {
              currRecord[offset] = value;
            }
          }
          break;
          case TINYINT: {
            Integer value = json.getInt(fieldName);
            if (value != null) {
              currRecord[offset] = (byte) (int) value;
            }
          }
          break;
          case SMALLINT: {
            Integer value = json.getInt(fieldName);
            if (value != null) {
              currRecord[offset] = (short) (int) value;
            }
          }
          break;
          case INTEGER: {
            Integer value = json.getInt(fieldName);
            if (value != null) {
              currRecord[offset] = value;
            }
          }
          break;
          case BIGINT: {
            Long value = json.getLong(fieldName);
            if (value != null) {
              currRecord[offset] = value;
            }
          }
          break;
          case FLOAT: {
            Double value = json.getDouble(fieldName);
            if (value != null) {
              currRecord[offset] = value;
            }
          }
          break;
          case REAL: {
            Double value = json.getDouble(fieldName);
            if (value != null) {
              currRecord[offset] = (float) (double) value;
            }
          }
          break;
          case DOUBLE: {
            Double value = json.getDouble(fieldName);
            if (value != null) {
              currRecord[offset] = value;
            }
          }
          break;
          case NUMERIC: {
            String value = json.getString(fieldName);
            if (value != null) {
              currRecord[offset] = new BigDecimal(value);
            }
          }
          break;
          case DECIMAL: {
            String value = json.getString(fieldName);
            if (value != null) {
              currRecord[offset] = new BigDecimal(value);
            }
          }
          break;
          case CHAR: {
            String value = json.getString(fieldName);
            if (value != null) {
              currRecord[offset] = value;
            }
          }
          break;
          case VARCHAR: {
            String value = json.getString(fieldName);
            if (value != null) {
              currRecord[offset] = value;
            }
          }
          break;
          case LONGVARCHAR: {
            String value = json.getString(fieldName);
            if (value != null) {
              currRecord[offset] = value;
            }
          }
          break;
          case DATE: {
            String value = json.getString(fieldName);
            if (value != null) {
              currRecord[offset] = Date.valueOf(value);
            }
          }
          break;
          case TIME: {
            String value = json.getString(fieldName);
            if (value != null) {
              currRecord[offset] = Time.valueOf(value);
            }
          }
          break;
          case TIMESTAMP: {
            String value = json.getString(fieldName);
            if (value != null) {
              currRecord[offset] = Timestamp.valueOf(value);
            }
          }
          break;
          case BINARY: {
            String value = json.getString(fieldName);
            if (value != null) {
              currRecord[offset] = new BASE64Decoder().decodeBuffer(value);
            }
          }
          break;
          case VARBINARY: {
            String value = json.getString(fieldName);
            if (value != null) {
              currRecord[offset] = new BASE64Decoder().decodeBuffer(value);
            }
          }
          break;
          case LONGVARBINARY: {
            String value = json.getString(fieldName);
            if (value != null) {
              currRecord[offset] = new BASE64Decoder().decodeBuffer(value);
            }
          }
          break;
          case BLOB: {
            String value = json.getString(fieldName);
            if (value != null) {
              currRecord[offset] = new BASE64Decoder().decodeBuffer(value);
            }
          }
          break;
          case CLOB: {
            String value = json.getString(fieldName);
            if (value != null) {
              currRecord[offset] = value;
            }
          }
          break;
          case BOOLEAN: {
            Boolean value = json.getBoolean(fieldName);
            if (value != null) {
              currRecord[offset] = value;
            }
          }
          break;
          case ROWID: {
            Long value = json.getLong(fieldName);
            if (value != null) {
              currRecord[offset] = value;
            }
          }
          break;
          case NCHAR: {
            String value = json.getString(fieldName);
            if (value != null) {
              currRecord[offset] = value;
            }
          }
          break;
          case NVARCHAR: {
            String value = json.getString(fieldName);
            if (value != null) {
              currRecord[offset] = value;
            }
          }
          break;
          case LONGNVARCHAR: {
            String value = json.getString(fieldName);
            if (value != null) {
              currRecord[offset] = value;
            }
          }
          break;
          case NCLOB: {
            String value = json.getString(fieldName);
            if (value != null) {
              currRecord[offset] = value;
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
