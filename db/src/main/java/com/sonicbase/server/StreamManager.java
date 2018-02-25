package com.sonicbase.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.*;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.jdbcdriver.StatementProxy;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.streams.Message;
import com.sonicbase.streams.StreamsConsumer;
import com.sonicbase.schema.FieldSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.DateUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import sun.misc.BASE64Decoder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

public class StreamManager {

  private Logger logger;
  private final DatabaseServer server;
  private ConcurrentHashMap<String, Connection> connections = new ConcurrentHashMap<>();
  private boolean shutdown = false;
  private boolean pauseStreaming = false;
  private Thread[] processingThreads;
  private Thread activeCheckThread;

  public StreamManager(final DatabaseServer server) {
    this.server = server;
    logger = new Logger(server.getDatabaseClient(), server.getShard(), server.getReplica());

    logger.info("initializing StreamManager");

//    Thread thread = new Thread(new Runnable(){
//      @Override
//      public void run() {
//        while (true) {
//          try {
//            ServersConfig servers = server.getCommon().getServersConfig();
//            ServersConfig.Shard[] shards = servers.getShards();
//            boolean allShardsAlive = true;
//            outer:
//            for (ServersConfig.Shard shard : shards) {
//              ServersConfig.Host[] hosts = shard.getReplicas();
//              boolean alive = false;
//              for (ServersConfig.Host host : hosts) {
//                if (!host.isDead()) {
//                  alive = true;
//                  break;
//                }
//              }
//              if (!alive) {
//                pauseStreaming = true;
//                allShardsAlive = false;
//                break outer;
//              }
//            }
//            if (allShardsAlive) {
//              pauseStreaming = false;
//            }
//            Thread.sleep(1000);
//          }
//          catch (InterruptedException e) {
//            break;
//          }
//          catch (Exception e) {
//            logger.error("Error in Queue Health Detector", e);
//          }
//        }
//      }
//    });
//    thread.start();
  }

  class ProcessingRequest {
    private List<Message> messages;
    private StreamsConsumer consumer;
  }

  ArrayBlockingQueue<ProcessingRequest> processingQueue = new ArrayBlockingQueue<>(100);
  Connection sysConnection = null;

  public static Connection initSysConnection(ObjectNode config) {
    try {
      ArrayNode array = config.withArray("shards");
      ObjectNode replicaDict = (ObjectNode) array.get(0);
      ArrayNode replicasArray = replicaDict.withArray("replicas");
      final String address = config.get("clientIsPrivate").asBoolean() ?
          replicasArray.get(0).get("privateAddress").asText() :
          replicasArray.get(0).get("publicAddress").asText();
      final int port = replicasArray.get(0).get("port").asInt();

      Class.forName("com.sonicbase.jdbcdriver.Driver");

      Connection conn = DriverManager.getConnection("jdbc:sonicbase:" + address + ":" + port);

      try {
        if (!((ConnectionProxy) conn).databaseExists("_sonicbase_sys")) {
          ((ConnectionProxy) conn).createDatabase("_sonicbase_sys");
        }
      }
      catch (Exception e) {
        if (!ExceptionUtils.getFullStackTrace(e).toLowerCase().contains("database already exists")) {
          throw new DatabaseException(e);
        }
      }

      conn.close();

      conn = DriverManager.getConnection("jdbc:sonicbase:" + address + ":" + port + "/_sonicbase_sys");
      return conn;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public static void initStreamsConsumerTable(Connection conn) {
    PreparedStatement stmt = null;
    try {
      try {
        stmt = conn.prepareStatement("describe table streams_consumer_state");
        stmt.executeQuery();
      }
      catch (Exception e) {
        stmt = conn.prepareStatement("create table streams_consumer_state (shard INTEGER, replica INTEGER, active BOOLEAN, PRIMARY KEY (shard, replica))");
        stmt.executeUpdate();
      }
    }
    catch (Exception e) {
      if (!ExceptionUtils.getFullStackTrace(e).toLowerCase().contains("table already exists")) {
        throw new DatabaseException(e);
      }
    }
    finally {
      if (stmt != null) {
        try {
          stmt.close();
        }
        catch (SQLException e) {
          throw new DatabaseException(e);
        }
      }
    }
  }

  private boolean streamingHasBeenStarted = false;

  public boolean isStreamingStarted() {
    return streamingHasBeenStarted;
  }

  private class ConsumerContext {
    private StreamsConsumer consumer;
    private List<Thread> threads = new ArrayList<>();
  }

  private ConcurrentLinkedQueue<ConsumerContext> consumers = new ConcurrentLinkedQueue<>();

  public ComObject startStreaming(ComObject cobj) {

    stopStreaming(cobj);

    if (!server.haveProLicense()) {
      throw new InsufficientLicense("You must have a pro license to use streams integration");
    }

    shutdown = false;

    pauseStreaming = true;

    streamingHasBeenStarted = true;
    final ObjectNode config = server.getConfig();
    final ObjectNode queueDict = (ObjectNode) config.get("streams");

    logger.info("Starting streams consumers: queue notNull=" + (queueDict != null));
    if (queueDict != null) {
      ArrayNode streams = queueDict.withArray("consumers");
      if (streams != null) {

        if (sysConnection == null) {
          sysConnection = initSysConnection(config);
          initStreamsConsumerTable(sysConnection);
        }

        activeCheckThread = new Thread(new Runnable() {
          @Override
          public void run() {
            while (!shutdown) {
              try {
                boolean active = readActiveStatus();
                boolean wasPaused = pauseStreaming;
                pauseStreaming = !active;
                if (!wasPaused && pauseStreaming) {
                  logger.info("pausing stream consumers");
                  shutdownConsumers();
                }
                if (!pauseStreaming && wasPaused) {
                  logger.info("unpausing stream consumers");
                  initConsumers(config, queueDict);
                }
                logger.info("streams consumer state: paused=" + pauseStreaming);
                Thread.sleep(10_000);
              }
              catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
              }
              catch (Exception e) {
                logger.error("Error checking active state", e);
              }
            }
          }
        }, "Streams Consumer Active Check Thread");
        activeCheckThread.start();

        int processorThreadCount = 8;
        if (queueDict.has("processorThreadCount")) {
          processorThreadCount = queueDict.get("processorThreadCount").asInt();
        }

        processingThreads = new Thread[processorThreadCount];
        for (int i = 0; i < processingThreads.length; i++) {
          processingThreads[i] = new Thread(new Runnable() {
            @Override
            public void run() {
              while (!shutdown) {
                try {
                  ProcessingRequest request = processingQueue.poll(2_000, TimeUnit.MILLISECONDS);
                  if (request == null) {
                    continue;
                  }
                  processMessages(request.consumer, request.messages);
                }
                catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  break;
                }
                catch (Exception e) {
                  logger.error("Error processing messages", e);
                }
              }
            }
          }, "Stream processing thread - " + i);
          processingThreads[i].start();
        }

        logger.info("Starting streams. Have license: processorThreadCount=" + processorThreadCount);

      }
    }
    return null;
  }

  private void shutdownConsumers() {
    while (true) {
      try {
        ConsumerContext context = consumers.poll();
        if (context == null) {
          break;
        }
        for (Thread thread : context.threads) {
          thread.interrupt();
          thread.join();
        }
        context.consumer.shutdown();
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }
  }

  private void initConsumers(final ObjectNode config, ObjectNode queueDict) {
    ArrayNode streams = queueDict.withArray("consumers");
    if (streams != null) {
      for (int i = 0; i < streams.size(); i++) {
        try {
          final ObjectNode stream = (ObjectNode) streams.get(i);
          final String className = stream.get("className").asText();
          logger.info("starting queue consumer: config=" + stream.toString());

          final StreamsConsumer consumer = (StreamsConsumer) Class.forName(className).newInstance();

          ConsumerContext consumerContext = new ConsumerContext();
          consumerContext.consumer = consumer;

          config.put("shard", server.getShard());
          config.put("replica", server.getReplica());

          boolean isMaster = false;
          if (server.getCommon().getServersConfig() != null) {
            if (server.getShard() == 0) {
              int masterReplica = server.getCommon().getServersConfig().getShards()[0].getMasterReplica();
              if (masterReplica == server.getReplica()) {
                isMaster = true;
              }
            }
          }
          config.put("isMaster", isMaster);

          int threadCount = consumer.init(server.getCluster(), config.toString(), stream.toString());

          for (int j = 0; j < threadCount; j++) {
            Thread thread = new Thread(new Runnable() {
              @Override
              public void run() {
                try {
                  consumer.initThread();

                  int errorCountInARow = 0;
                  while (!shutdown) {
                    boolean wasPaused = pauseStreaming;
                    while (!shutdown && pauseStreaming) {
                      Thread.sleep(1000);
                    }
                    if (wasPaused) {
                      consumer.initThread();
                    }
                    try {
                      List<Message> messages = consumer.receive();
                      if (messages != null) {
                        List<Message> currMessages = new ArrayList<>();
                        for (Message message : messages) {
                          currMessages.add(message);
                          if (currMessages.size() >= 10) {
                            ProcessingRequest request = new ProcessingRequest();
                            request.consumer = consumer;
                            request.messages = currMessages;
                            processingQueue.put(request);
                            currMessages = new ArrayList<>();
                          }
                        }
                        if (currMessages.size() != 0) {
                          ProcessingRequest request = new ProcessingRequest();
                          request.consumer = consumer;
                          request.messages = currMessages;
                          processingQueue.put(request);
                        }
                        errorCountInARow = 0;
                      }
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
            }, "Consumer thread - " + j);
            consumerContext.threads.add(thread);
            thread.start();
          }
          consumers.add(consumerContext);
        }
        catch (Exception e) {
          logger.error("Error starting message queue consumer", e);
        }
      }
    }
  }

  private boolean readActiveStatus() {
    PreparedStatement stmt = null;
    try {
      stmt = sysConnection.prepareStatement("select * from streams_consumer_state where shard=? and replica=?");
      stmt.setInt(1, server.getShard());
      stmt.setInt(2, server.getReplica());
      ResultSet rs = stmt.executeQuery();
      if (rs.next()) {
        return rs.getBoolean("active");
      }
      return false;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
    finally {
      if (stmt != null) {
        try {
          stmt.close();
        }
        catch (SQLException e) {
          throw new DatabaseException(e);
        }
      }
    }
  }

  private Object errorLogMutex = new Object();
  private BufferedWriter errorLogWriter = null;
  private long errorLogLastCreated = System.currentTimeMillis();

  private void processMessages(StreamsConsumer consumer, List<Message> messages) {
    if (messages == null || messages.size() == 0) {
      return;
    }
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.method, "processMessages");
    ComArray array = cobj.putArray(ComObject.Tag.messages, ComObject.Type.stringType);
    for (Message msg : messages) {
      array.add(msg.getBody());
    }

    try {
      byte[] ret = server.getDatabaseClient().send(null, server.getShard(), 0, cobj, DatabaseClient.Replica.def);
      //processMessages(cobj);

      consumer.acknowledgeMessages(messages);
    }
    catch (Exception e) {
      try {
        logger.error("Error processing messages", e);
        synchronized (errorLogMutex) {
          if (errorLogWriter != null && (System.currentTimeMillis() - errorLogLastCreated) > 6 * 60 * 60 * 1000) {
            errorLogWriter.close();
            errorLogWriter = null;
          }
          if (errorLogWriter == null) {
            File file = new File(server.getDataDir(), "stream_errors/" + server.getShard() + "/" +
                server.getReplica() + "/" + DateUtils.toString(new Date(System.currentTimeMillis())) + ".error");
            file.getParentFile().mkdirs();
            errorLogWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)));
            errorLogLastCreated = System.currentTimeMillis();
          }
          for (Message msg : messages) {
            errorLogWriter.write(msg.getBody() + "\n");
          }
          errorLogWriter.flush();
        }
        consumer.handleError(messages, e);
      }
      catch (Exception e1) {
        throw new DatabaseException(e1);
      }
    }
  }

  public ComObject processMessages(ComObject cobj) {
    Map<String, List<JsonNode>> groupedMessages = new HashMap<>();
    ComArray messages = cobj.getArray(ComObject.Tag.messages);
    for (int i = 0; i < messages.getArray().size(); i++) {
      try {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode message = (ObjectNode) mapper.readTree((String) messages.getArray().get(i));
        ArrayNode array = message.withArray("actions");
        for (int j = 0; j < array.size(); j++) {
          JsonNode entry = array.get(j);

          String dbName = entry.get("_sonicbase_dbname").asText();
          initConnection(dbName);
          String tableName = entry.get("_sonicbase_tablename").asText();
          String action = entry.get("_sonicbase_action").asText();
          String key = dbName + ":" + tableName + ":" + action;
          List<JsonNode> currMsgs = groupedMessages.get(key);
          if (currMsgs == null) {
            currMsgs = new ArrayList<>();
            groupedMessages.put(key, currMsgs);
          }
          currMsgs.add(entry);
        }
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }
    for (Map.Entry<String, List<JsonNode>> entry : groupedMessages.entrySet()) {
      try {
        String[] parts = entry.getKey().split(":");
        String dbName = parts[0];
        String tableName = parts[1];
        String action = parts[2];
        final TableSchema tableSchema = server.getCommon().getTables(dbName).get(tableName);
        final List<FieldSchema> fields = tableSchema.getFields();
        if (action.equals("insert")) {

          final StringBuilder fieldsStr = new StringBuilder();
          final StringBuilder parmsStr = new StringBuilder();
          boolean first = true;
          for (FieldSchema field : fields) {
            if (field.getName().equals("_sonicbase_id")) {
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

          List<JsonNode> msgs = entry.getValue();
          while (msgs.size() != 0) {
            PreparedStatement stmt = connections.get(dbName).prepareStatement("insert ignore into " + tableName + " (" + fieldsStr.toString() +
                ") VALUES (" + parmsStr.toString() + ")");
            try {
              for (int i = 0; i < 200 && msgs.size() != 0; i++) {
                JsonNode message = msgs.remove(0);
                Object[] record = getCurrRecordFromJson(message, fields);
                BulkImportManager.setFieldsInInsertStatement(stmt, 1, record, fields);

                stmt.addBatch();
              }
              stmt.executeBatch();
            }
            finally {
              stmt.close();
            }
          }
        }
        else if (action.equals("update")) {
          for (JsonNode message : entry.getValue()) {
            ObjectNode json = (ObjectNode) message;
            JsonNode after =json.get("after");
            JsonNode before =json.get("before");

            List<FieldSchema> specifiedFields = new ArrayList<>();
            String str = "update " + tableName + " set ";
            int offset = 0;
            int parmOffset = 1;
            Iterator<Map.Entry<String, JsonNode>> iterator = after.fields();
            while (iterator.hasNext()) {
              Map.Entry<String, JsonNode> jsonEntry = iterator.next();
              if (jsonEntry.getKey().toLowerCase().startsWith("_sonicbase_")) {
                continue;
              }
              if (offset != 0) {
                str += ", ";
              }
              str += " " + jsonEntry.getKey().toLowerCase() + "=? ";
              for (FieldSchema fieldSchema : fields) {
                if (fieldSchema.getName().equals(jsonEntry.getKey().toLowerCase())) {
                  specifiedFields.add(fieldSchema);
                }
              }
              offset++;
              parmOffset++;
            }
            Object[] record = getCurrRecordFromJson(after, specifiedFields);

            str += " where ";

            List<FieldSchema> specifiedWhereFields = new ArrayList<>();
            offset = 0;
            iterator = before.fields();
            while (iterator.hasNext()) {
              Map.Entry<String, JsonNode> jsonEntry = iterator.next();
              if (jsonEntry.getKey().toLowerCase().startsWith("_sonicbase_")) {
                continue;
              }
              if (offset != 0) {
                str += " AND ";
              }
              str += " " + jsonEntry.getKey().toLowerCase() + "=? ";
              for (FieldSchema fieldSchema : fields) {
                if (fieldSchema.getName().equals(jsonEntry.getKey().toLowerCase())) {
                  specifiedWhereFields.add(fieldSchema);
                }
              }
              offset++;
            }
            Object[] whereRecord = getCurrRecordFromJson(before, specifiedFields);

            PreparedStatement stmt = connections.get(dbName).prepareStatement(str);
            BulkImportManager.setFieldsInInsertStatement(stmt, 1, record, specifiedFields);
            BulkImportManager.setFieldsInInsertStatement(stmt, parmOffset, whereRecord, specifiedWhereFields);

            Long sequence0 = null;
            Long sequence1 = null;
            Short sequence2 = null;
            if (before.has("_sonicbase_sequence0") &&
                before.has("_sonicbase_sequence1") &&
                before.has("_sonicbase_sequence2")) {
              sequence0 = before.get("_sonicbase_sequence0").asLong();
              sequence1 = before.get("_sonicbase_sequence1").asLong();
              sequence2 = (short)before.get("_sonicbase_sequence2").asInt();
            }
            ((StatementProxy)stmt).doUpdate(sequence0, sequence1, sequence2);
          }
        }
        else if (action.equals("delete")) {
          for (JsonNode message : entry.getValue()) {
            ObjectNode json = (ObjectNode) message;

            List<FieldSchema> specifiedFields = new ArrayList<>();
            String str = "delete from " + tableName + " where ";
            int offset = 0;
            Iterator<Map.Entry<String, JsonNode>> iterator = json.fields();
            while (iterator.hasNext()) {
              Map.Entry<String, JsonNode> jsonEntry = iterator.next();
              if (jsonEntry.getKey().toLowerCase().startsWith("_sonicbase_")) {
                continue;
              }
              if (offset != 0) {
                str += " AND ";
              }
              str += " " + jsonEntry.getKey().toLowerCase() + "=? ";
              for (FieldSchema fieldSchema : fields) {
                if (fieldSchema.getName().equals(jsonEntry.getKey().toLowerCase())) {
                  specifiedFields.add(fieldSchema);
                }
              }
              offset++;
            }
            Object[] record = getCurrRecordFromJson(json, specifiedFields);

            PreparedStatement stmt = connections.get(dbName).prepareStatement(str);
            BulkImportManager.setFieldsInInsertStatement(stmt, 1, record, specifiedFields);

            Long sequence0 = null;
            Long sequence1 = null;
            Short sequence2 = null;
            if (json.has("_sonicbase_sequence0") &&
                json.has("_sonicbase_sequence1") &&
                json.has("_sonicbase_sequence2")) {
              sequence0 = json.get("_sonicbase_sequence0").asLong();
              sequence1 = json.get("_sonicbase_sequence1").asLong();
              sequence2 = (short)json.get("_sonicbase_sequence2").asInt();
            }

            ((StatementProxy)stmt).doDelete(sequence0, sequence1, sequence2);
          }
        }
        else {
          throw new DatabaseException("Unknown publish action: action=" + action);
        }
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }
    return null;
  }

  public ComObject stopStreaming(ComObject cobj) {
    shutdown = true;
    streamingHasBeenStarted = false;
    try {
      if (activeCheckThread != null) {
        activeCheckThread.interrupt();
        activeCheckThread.join();
        activeCheckThread = null;
      }
      shutdownConsumers();
      if (processingThreads != null) {
        for (Thread thread : processingThreads) {
          thread.interrupt();
          thread.join();
        }
        processingThreads = null;
      }
      if (sysConnection != null) {
        sysConnection.close();
        sysConnection = null;
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
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

  public static Object[] getCurrRecordFromJson(JsonNode json, List<FieldSchema> fields) throws SQLException {

//    ObjectNode newJson = new ObjectNode(JsonNodeFactory.instance);
//    Iterator<Map.Entry<String, JsonNode>> iterator = json.fields();
//    while (iterator.hasNext()) {
//      Map.Entry<String, JsonNode> entry = iterator.next();
//      newJson.put(entry.getKey().toLowerCase(), entry.getValue());
//    }
//    json = newJson;
    Map<String, JsonNode> lowerJsonFields = new HashMap<>();
    Iterator<Map.Entry<String, JsonNode>> jsonFields = json.fields();
    while (jsonFields.hasNext()) {
      Map.Entry<String, JsonNode> entry = jsonFields.next();
      lowerJsonFields.put(entry.getKey().toLowerCase(), entry.getValue());
    }

    final Object[] currRecord = new Object[fields.size()];
    try {
      int offset = 0;
      for (FieldSchema field : fields) {
        String fieldName = DatabaseClient.toLower(field.getName());
        if (fieldName.equals("_sonicbase_id")) {
          continue;
        }
        switch (field.getType()) {
          case BIT: {
            JsonNode node = lowerJsonFields.get(fieldName);
            if (node != null) {
              Boolean value = node.asBoolean();
              if (value != null) {
                currRecord[offset] = value;
              }
            }
          }
          break;
          case TINYINT: {
            JsonNode node = lowerJsonFields.get(fieldName);
            if (node != null) {
              Integer value = node.asInt();
              if (value != null) {
                currRecord[offset] = (byte) (int) value;
              }
            }
          }
          break;
          case SMALLINT: {
            JsonNode node = lowerJsonFields.get(fieldName);
            if (node != null) {
              Integer value = node.asInt();
              if (value != null) {
                currRecord[offset] = (short) (int) value;
              }
            }
          }
          break;
          case INTEGER: {
            JsonNode node = lowerJsonFields.get(fieldName);
            if (node != null) {
              Integer value = node.asInt();
              if (value != null) {
                currRecord[offset] = value;
              }
            }
          }
          break;
          case BIGINT: {
            JsonNode node = lowerJsonFields.get(fieldName);
            if (node != null) {
              Long value = node.asLong();
              if (value != null) {
                currRecord[offset] = value;
              }
            }
          }
          break;
          case FLOAT: {
            JsonNode node = lowerJsonFields.get(fieldName);
            if (node != null) {
              Double value = node.asDouble();
              if (value != null) {
                currRecord[offset] = value;
              }
            }
          }
          break;
          case REAL: {
            JsonNode node = lowerJsonFields.get(fieldName);
            if (node != null) {
              Double value = node.asDouble();
              if (value != null) {
                currRecord[offset] = (float) (double) value;
              }
            }
          }
          break;
          case DOUBLE: {
            JsonNode node = lowerJsonFields.get(fieldName);
            if (node != null) {
              Double value = node.asDouble();
              if (value != null) {
                currRecord[offset] = value;
              }
            }
          }
          break;
          case NUMERIC: {
            JsonNode node = lowerJsonFields.get(fieldName);
            if (node != null) {
              String value = node.asText();
              if (value != null) {
                currRecord[offset] = new BigDecimal(value);
              }
            }
          }
          break;
          case DECIMAL: {
            JsonNode node = lowerJsonFields.get(fieldName);
            if (node != null) {
              String value = node.asText();
              if (value != null) {
                currRecord[offset] = new BigDecimal(value);
              }
            }
          }
          break;
          case CHAR: {
            JsonNode node = lowerJsonFields.get(fieldName);
            if (node != null) {
              String value = node.asText();
              if (value != null) {
                currRecord[offset] = value;
              }
            }
          }
          break;
          case VARCHAR: {
            JsonNode node = lowerJsonFields.get(fieldName);
            if (node != null) {
              String value = node.asText();
              if (value != null) {
                currRecord[offset] = value;
              }
            }
          }
          break;
          case LONGVARCHAR: {
            JsonNode node = lowerJsonFields.get(fieldName);
            if (node != null) {
              String value = node.asText();
              if (value != null) {
                currRecord[offset] = value;
              }
            }
          }
          break;
          case DATE: {
            JsonNode node = lowerJsonFields.get(fieldName);
            if (node != null) {
              String value = node.asText();
              if (value != null) {
                currRecord[offset] = Date.valueOf(value);
              }
            }
          }
          break;
          case TIME: {
            JsonNode node = lowerJsonFields.get(fieldName);
            if (node != null) {
              String value = node.asText();
              if (value != null) {
                currRecord[offset] = Time.valueOf(value);
              }
            }
          }
          break;
          case TIMESTAMP: {
            JsonNode node = lowerJsonFields.get(fieldName);
            if (node != null) {
              String value = node.asText();
              if (value != null) {
                currRecord[offset] = Timestamp.valueOf(value);
              }
            }
          }
          break;
          case BINARY: {
            JsonNode node = lowerJsonFields.get(fieldName);
            if (node != null) {
              String value = node.asText();
              if (value != null) {
                currRecord[offset] = new BASE64Decoder().decodeBuffer(value);
              }
            }
          }
          break;
          case VARBINARY: {
            JsonNode node = lowerJsonFields.get(fieldName);
            if (node != null) {
              String value = node.asText();
              if (value != null) {
                currRecord[offset] = new BASE64Decoder().decodeBuffer(value);
              }
            }
          }
          break;
          case LONGVARBINARY: {
            JsonNode node = lowerJsonFields.get(fieldName);
            if (node != null) {
              String value = node.asText();
              if (value != null) {
                currRecord[offset] = new BASE64Decoder().decodeBuffer(value);
              }
            }
          }
          break;
          case BLOB: {
            JsonNode node = lowerJsonFields.get(fieldName);
            if (node != null) {
              String value = node.asText();
              if (value != null) {
                currRecord[offset] = new BASE64Decoder().decodeBuffer(value);
              }
            }
          }
          break;
          case CLOB: {
            JsonNode node = lowerJsonFields.get(fieldName);
            if (node != null) {
              String value = node.asText();
              if (value != null) {
                currRecord[offset] = value;
              }
            }
          }
          break;
          case BOOLEAN: {
            JsonNode node = lowerJsonFields.get(fieldName);
            if (node != null) {
              Boolean value = node.asBoolean();
              if (value != null) {
                currRecord[offset] = value;
              }
            }
          }
          break;
          case ROWID: {
            JsonNode node = lowerJsonFields.get(fieldName);
            if (node != null) {
              Long value = node.asLong();
              if (value != null) {
                currRecord[offset] = value;
              }
            }
          }
          break;
          case NCHAR: {
            JsonNode node = lowerJsonFields.get(fieldName);
            if (node != null) {
              String value = node.asText();
              if (value != null) {
                currRecord[offset] = value;
              }
            }
          }
          break;
          case NVARCHAR: {
            JsonNode node = lowerJsonFields.get(fieldName);
            if (node != null) {
              String value = node.asText();
              if (value != null) {
                currRecord[offset] = value;
              }
            }
          }
          break;
          case LONGNVARCHAR: {
            JsonNode node = lowerJsonFields.get(fieldName);
            if (node != null) {
              String value = node.asText();
              if (value != null) {
                currRecord[offset] = value;
              }
            }
          }
          break;
          case NCLOB: {
            JsonNode node = lowerJsonFields.get(fieldName);
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
