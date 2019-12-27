/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.*;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.jdbcdriver.StatementProxy;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.FieldSchema;
import com.sonicbase.schema.Schema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.server.BulkImportManager;
import com.sonicbase.server.DatabaseServer;
import com.sonicbase.server.UpdateManager;
import com.sonicbase.util.DateUtils;
import com.sun.jersey.json.impl.writer.JsonEncoder;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static Logger logger = LoggerFactory.getLogger(StreamManager.class);

  private final com.sonicbase.server.DatabaseServer server;
  private final Config config;
  private ConcurrentHashMap<String, Connection> connections = new ConcurrentHashMap<>();
  private boolean shutdown = false;
  private boolean pauseStreaming = false;
  private Thread[] processingThreads;
  private Thread activeCheckThread;
  private ThreadLocal<List<MessageRequest>> threadLocalMessageRequests = new ThreadLocal<>();
  private ArrayBlockingQueue<MessageRequest> publishQueue = new ArrayBlockingQueue<>(30_000);
  private int publisherThreadCount;
  private int maxPublishBatchSize;
  private List<StreamsProducer> producers = new ArrayList<>();
  private Thread[] publisherThreads;
  private ThreadLocal<Boolean> threadLocalIsBatchRequest = new ThreadLocal<>();
  private Thread streamsConsumerMonitorthread;
  private boolean enable = true;
  private boolean isDatabaseInitialized;
  private Connection sysConn;

  public StreamManager(final DatabaseServer server) {
    this.server = server;
    this.config = server.getConfig();
    logger.info("initializing StreamManager");
  }

  public void enable(boolean enable) {
    this.enable = enable;
  }

  public void stopStreamsConsumerMasterMonitor() {
    streamsConsumerMonitorthread.interrupt();
    try {
      streamsConsumerMonitorthread.join();
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  public void startStreamsConsumerMasterMonitor() {
    if (streamsConsumerMonitorthread == null) {
      streamsConsumerMonitorthread = ThreadUtil.createThread(() -> {
        try {
          while (!streamingHasBeenStarted || sysConn == null) {
            try {
              Thread.sleep(2_000);
            }
            catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              break;
            }
          }

          StreamManager.initStreamsConsumerTable(sysConn);

          while (!shutdown && !Thread.interrupted()) {
            try {
              if (!enable) {
                Thread.sleep(2_000);
                continue;
              }
              StringBuilder builder = new StringBuilder();
              Map<String, Boolean> currState = null;
              try {
                currState = readStreamConsumerState();
              }
              catch (Exception e) {
                logger.error("Error reading stream consumer state - will retry: msg=" + e.getMessage());
                Thread.sleep(2_000);
                continue;
              }
              for (int shard = 0; shard < server.getShardCount(); shard++) {
                int aliveReplica = -1;
                for (int replica = 0; replica < server.getReplicationFactor(); replica++) {
                  boolean dead = server.getCommon().getServersConfig().getShards()[shard].getReplicas()[replica].isDead();
                  if (!dead) {
                    aliveReplica = replica;
                  }
                }
                builder.append(",").append(shard).append(":").append(aliveReplica);
                Boolean currActive = currState.get(shard + ":" + aliveReplica);
                if (currActive == null || !currActive) {
                  setStreamConsumerState(shard, aliveReplica);
                }
              }
              logger.info("active streams consumers: " + builder.toString());
              Thread.sleep(10_000);
            }
            catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              break;
            }
            catch (Exception e) {
              logger.error("Error in stream consumer monitor", e);
            }
          }
        }
        finally {
          streamsConsumerMonitorthread = null;
        }
      }, "SonicBase Streams Consumer Monitor Thread");
      streamsConsumerMonitorthread.start();
    }
  }


  private void setStreamConsumerState(int shard, int replica) {
    try {
      for (int i = 0; i < server.getReplicationFactor(); i++) {
        try (PreparedStatement stmt = sysConn.prepareStatement("insert ignore into streams_consumer_state (shard, replica, active) VALUES (?, ?, ?)")) {
          stmt.setInt(1, shard);
          stmt.setInt(2, i);
          stmt.setBoolean(3, replica == i);
          int count = stmt.executeUpdate();
        }
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private Map<String, Boolean> readStreamConsumerState() {
    Map<String, Boolean> ret = new HashMap<>();
    PreparedStatement stmt = null;
    try {
      stmt = sysConn.prepareStatement("select * from streams_consumer_state");
      ResultSet rs = stmt.executeQuery();
      while (rs.next()) {
        int shard = rs.getInt("shard");
        int replica = rs.getInt("replica");
        boolean isActive = rs.getBoolean("active");
        ret.put(shard + ":" + replica, isActive);
      }
      return ret;
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

  public void shutdown() {
    this.shutdown = true;
    for (Connection conn : connections.values()) {
      try {
        conn.close();
      }
      catch (SQLException e) {
        logger.error("Error closing stream connection", e);
      }
    }
  }

  public void addToBatch(String dbName, String tableName, byte[] recordBytes, UpdateManager.UpdateType updateType) {
    if (producers.isEmpty()) {
      return;
    }
    MessageRequest request = new MessageRequest();
    request.dbName = dbName;
    request.tableName = tableName;
    request.recordBytes = recordBytes;
    request.updateType = updateType;
    threadLocalMessageRequests.get().add(request);
  }

  public void batchInsertFinish() {
    threadLocalMessageRequests.set(null);
  }

  public void initBatchInsert() {
    if (threadLocalMessageRequests.get() != null) {
      logger.warn("Left over batch messages: count=" + threadLocalMessageRequests.get().size());
    }
    threadLocalMessageRequests.set(new ArrayList<>());
  }

  class ProcessingRequest {
    private List<Message> messages;
    private StreamsConsumer consumer;
  }

  ArrayBlockingQueue<ProcessingRequest> processingQueue = new ArrayBlockingQueue<>(100);

  public ComObject initConnection(ComObject cobj, boolean replayedCommand) {
    logger.info("initConnection - begin");

    initConnection();

    logger.info("initConnection - finished");
    return null;
  }

  private void initConnection() {
    if (sysConn == null) {
      sysConn = server.getSysConnection();
    }
  }

  private boolean isDatabaseInitialized() {
    if (isDatabaseInitialized) {
      return true;
    }

    if (sysConn == null) {
      return false;
    }
    DatabaseClient client = ((ConnectionProxy)sysConn).getDatabaseClient();
    Schema schema = client.getCommon().getDatabases().get("_sonicbase_sys");
    if (schema == null) {
      return false;
    }
    TableSchema tableSchema = schema.getTables().get("streams_consumer_state");
    if (tableSchema == null) {
      return false;
    }
    isDatabaseInitialized = true;
    return true;
  }

  public static void initStreamsConsumerTable(Connection conn) {
    PreparedStatement stmt = null;
    try {
      DatabaseClient client = ((ConnectionProxy)conn).getDatabaseClient();
      client.syncSchema();

      TableSchema tableSchema = client.getCommon().getTables("_sonicbase_sys").get("streams_consumer_state");
      if (tableSchema == null) {
        stmt = conn.prepareStatement("create table streams_consumer_state (shard INTEGER, replica INTEGER, active BOOLEAN, PRIMARY KEY (shard, replica))");
        ((StatementProxy) stmt).disableStats(true);
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

  public ComObject isStreamingStarted(ComObject cobj, boolean replayedCommand) {
    boolean started = isStreamingStarted();
    ComObject retObj = new ComObject(1);
    retObj.put(ComObject.Tag.IS_STARTED, started);
    return retObj;
  }

  public boolean isStreamingStarted() {
    return streamingHasBeenStarted;
  }

  private class ConsumerContext {
    private StreamsConsumer consumer;
    private List<Thread> threads = new ArrayList<>();
  }

  private ConcurrentLinkedQueue<ConsumerContext> consumers = new ConcurrentLinkedQueue<>();

  public ComObject startStreaming(ComObject cobj, boolean replayedCommand) {

    stopStreaming(cobj, false);

    shutdown = false;

    pauseStreaming = true;

    streamingHasBeenStarted = true;
    final Config config = server.getConfig();
    final Map<String, Object> queueDict = (Map<String, Object>) config.getMap().get("streams");

    logger.info("Starting streams consumers: queue notNull=" + (queueDict != null));
    if (queueDict != null) {
      List<Map<String, Object>> streams = (List<Map<String, Object>>) queueDict.get("consumers");
      if (streams != null) {

        activeCheckThread = new Thread(() -> {
          while (!shutdown) {
            try {
              if (!enable) {
                Thread.sleep(2_000);
                continue;
              }
              while (!isDatabaseInitialized()) {
                Thread.sleep(2_000);
              }
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
        }, "Streams Consumer Active Check Thread");
        activeCheckThread.start();

        int processorThreadCount = 8;
        if (queueDict.containsKey("processorThreadCount")) {
          processorThreadCount = (int) queueDict.get("processorThreadCount");
        }

        processingThreads = new Thread[processorThreadCount];
        for (int i = 0; i < processingThreads.length; i++) {
          processingThreads[i] = new Thread(new Runnable() {
            @Override
            public void run() {
              while (!shutdown) {
                try {
                  while (!isDatabaseInitialized()) {
                    Thread.sleep(2_000);
                  }
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

  private void initConsumers(final Config config, Map<String, Object> queueDict) {
    List<Map<String, Object>> streams = (List<Map<String, Object>>) queueDict.get("consumers");
    if (streams != null) {
      for (int i = 0; i < streams.size(); i++) {
        try {
          final Map<String, Object> stream = (Map<String, Object>) streams.get(i).get("consumer");
          if (stream == null) {
            continue;
          }
          final String className = (String) stream.get("className");
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

          int threadCount = consumer.init(config, server.getInstallDir(), stream);

          for (int j = 0; j < threadCount; j++) {
            Thread thread = new Thread(() -> {
              try {
                consumer.initThread();

                int errorCountInARow = 0;
                while (!shutdown) {
                  if (!enable) {
                    Thread.sleep(2_000);
                    continue;
                  }
                  boolean wasPaused = pauseStreaming;
                  while (!shutdown && pauseStreaming) {
                    Thread.sleep(1000);
                  }
                  if (wasPaused) {
                    consumer.initThread();
                  }
                  try {
                    List<Message> messages = consumer.receive();
                    if (messages != null && messages.size() != 0) {
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
      stmt = sysConn.prepareStatement("select * from streams_consumer_state where shard=? and replica=?");
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

    ComObject cobj = new ComObject(2);
    ComArray array = cobj.putArray(ComObject.Tag.MESSAGES, ComObject.Type.STRING_TYPE, messages.size());
    for (Message msg : messages) {
      array.add(msg.getBody());
    }

    try {
      byte[] ret = server.getDatabaseClient().send("StreamManager:processMessages", server.getShard(),
          0, cobj, DatabaseClient.Replica.DEF);
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

  public ComObject processMessages(ComObject cobj, boolean replayedCommand) {
    Map<String, List<JsonNode>> groupedMessages = new HashMap<>();
    ComArray messages = cobj.getArray(ComObject.Tag.MESSAGES);
    for (int i = 0; i < messages.getArray().size(); i++) {
      try {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode message = (ObjectNode) mapper.readTree((String) messages.getArray().get(i));
        ArrayNode array = message.withArray("events");
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
        if (action.equals(UpdateManager.UpdateType.INSERT.name())) {
          handleInsert(entry, dbName, tableName, fields);
        }
        else if (action.equals(UpdateManager.UpdateType.UPDATE.name())) {
          handleUpdate(entry, dbName, tableName, fields);
        }
        else if (action.equals(UpdateManager.UpdateType.DELETE.name())) {
          handleDelete(entry, dbName, tableName, fields);
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

  private void handleDelete(Map.Entry<String, List<JsonNode>> entry, String dbName, String tableName, List<FieldSchema> fields) throws SQLException {
    for (JsonNode message : entry.getValue()) {
      ObjectNode json = (ObjectNode) message;
      json = (ObjectNode) json.get("record");
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

      ((StatementProxy)stmt).doDelete(sequence0, sequence1, sequence2, false);
    }
  }

  private void handleInsert(Map.Entry<String, List<JsonNode>> entry, String dbName, String tableName, List<FieldSchema> fields) throws SQLException {
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
          message = message.get("record");
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

  private void handleUpdate(Map.Entry<String, List<JsonNode>> entry, String dbName, String tableName, List<FieldSchema> fields) throws SQLException {
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
      if (after.has("_sonicbase_sequence0") &&
          after.has("_sonicbase_sequence1") &&
          after.has("_sonicbase_sequence2")) {
        sequence0 = after.get("_sonicbase_sequence0").asLong();
        sequence1 = after.get("_sonicbase_sequence1").asLong();
        sequence2 = (short)after.get("_sonicbase_sequence2").asInt();
      }
      ((StatementProxy)stmt).doUpdate(sequence0, sequence1, sequence2, false);
    }
  }

  public ComObject stopStreaming(ComObject cobj, boolean replayedCommand) {
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
      Config config = server.getConfig();
      List<Config.Shard> array = config.getShards();
      Config.Shard shard = array.get(0);
      List<Config.Replica> replicasArray = shard.getReplicas();
      final String address = replicasArray.get(0).getString("address");
      final int port = replicasArray.get(0).getInt("port");

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
          offset++;
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
                currRecord[offset] = new Date(DateUtils.fromDbCalString(value).getTimeInMillis());
              }
            }
          }
          break;
          case TIME: {
            JsonNode node = lowerJsonFields.get(fieldName);
            if (node != null) {
              String value = node.asText();
              if (value != null) {
                currRecord[offset] = new Time(DateUtils.fromDbTimeString(value).getTimeInMillis());
              }
            }
          }
          break;
          case TIMESTAMP: {
            JsonNode node = lowerJsonFields.get(fieldName);
            if (node != null) {
              String value = node.asText();
              if (value != null) {
                currRecord[offset] = new Timestamp(DateUtils.fromDbCalString(value).getTimeInMillis());
              }
            }
          }
          break;
          case BINARY: {
            JsonNode node = lowerJsonFields.get(fieldName);
            if (node != null) {
              String value = node.asText();
              if (value != null) {
                currRecord[offset] = new Base64().decode(value);
              }
            }
          }
          break;
          case VARBINARY: {
            JsonNode node = lowerJsonFields.get(fieldName);
            if (node != null) {
              String value = node.asText();
              if (value != null) {
                currRecord[offset] = new Base64().decode(value);
              }
            }
          }
          break;
          case LONGVARBINARY: {
            JsonNode node = lowerJsonFields.get(fieldName);
            if (node != null) {
              String value = node.asText();
              if (value != null) {
                currRecord[offset] = new Base64().decode(value);
              }
            }
          }
          break;
          case BLOB: {
            JsonNode node = lowerJsonFields.get(fieldName);
            if (node != null) {
              String value = node.asText();
              if (value != null) {
                currRecord[offset] = new Base64().decode(value);
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

  class MessageRequest {
    private String dbName;
    private String tableName;
    private byte[] recordBytes;
    private UpdateManager.UpdateType updateType;
    private byte[] existingBytes;
  }

  private void initStreamProducers() {
    final Config config = server.getConfig();
    Map<String, Object> queueDict = (Map<String, Object>) config.getMap().get("streams");
    logger.info("Starting stream producers: streams notNull=" + (queueDict != null));
    if (queueDict != null) {
      if (queueDict.containsKey("publisherThreadCount")) {
        publisherThreadCount = (int) queueDict.get("publisherThreadCount");
      }
      else {
        publisherThreadCount = 8;
      }

      logger.info("Starting streams. Have license: publisherThreadCount=" + publisherThreadCount);

      List<Map<String, Object>> streams = (List<Map<String, Object>>) queueDict.get("producers");
      if (streams != null) {
        for (int i = 0; i < streams.size(); i++) {
          try {
            final Map<String, Object> stream = (Map<String, Object>) streams.get(i).get("producer");
            final String className = (String) stream.get("className");
            Integer maxBatchSize = (Integer) stream.get("maxBatchSize");
            if (maxBatchSize == null) {
              maxBatchSize = 10;
            }
            this.maxPublishBatchSize = maxBatchSize;

            logger.info("starting stream producer: config=" + stream.toString());
            StreamsProducer producer = (StreamsProducer) Class.forName(className).newInstance();

            producer.init(config, server.getInstallDir(), stream);

            producers.add(producer);
          }
          catch (Exception e) {
            logger.error("Error initializing stream producer: config=" + streams.toString(), e);
          }
        }
      }
    }
  }


  public void initPublisher() {
    initStreamProducers();

    final Config config = server.getConfig();
    Map<String, Object> queueDict = (Map<String, Object>) config.getMap().get("streams");
    if (queueDict != null) {
      publisherThreads = new Thread[publisherThreadCount];
      for (int i = 0; i < publisherThreads.length; i++) {
        publisherThreads[i] = new Thread(() -> {
          List<MessageRequest> toProcess = new ArrayList<>();
          long lastTimePublished = System.currentTimeMillis();
          while (!shutdown) {
            try {
              if (!enable) {
                Thread.sleep(2_000);
                continue;
              }
              for (int i1 = 0; i1 < maxPublishBatchSize * 4; i1++) {
                MessageRequest initialRequest = publishQueue.poll(100, TimeUnit.MILLISECONDS);
                if (initialRequest != null) {
                  toProcess.add(initialRequest);
                }
                if (System.currentTimeMillis() - lastTimePublished > 2000) {
                  break;
                }
              }

              if (toProcess.size() != 0) {
                publishMessages(toProcess);
                toProcess.clear();
              }
              lastTimePublished = System.currentTimeMillis();
            }
            catch (Exception e) {
              logger.error("error in message publisher", e);
            }
          }
        }, "SonicBase Publisher Thread");
        publisherThreads[i].start();
      }
    }
  }

  private void publishMessages(List<MessageRequest> toProcess) {
    List<MessageRequest> batch = new ArrayList<>();
    List<String> messages = new ArrayList<>();
    try {
      for (MessageRequest request : toProcess) {
        batch.add(request);
        if (batch.size() >= maxPublishBatchSize) {
          buildBatchMessage(batch, messages);
          batch = new ArrayList<>();
        }
      }
      if (batch.size() != 0) {
        buildBatchMessage(batch, messages);
      }

      for (StreamsProducer producer : producers) {
        producer.publish(messages);
      }
    }
    catch (Exception e) {
      logger.error("error publishing message", e);
    }
  }

  private void buildBatchMessage(List<MessageRequest> batch, List<String> messages) {
    StringBuilder builder = new StringBuilder();
    builder.append("{");
    builder.append("\"events\":[");

    int offset = 0;
    for (MessageRequest currRequest : batch) {
      if (offset != 0) {
        builder.append(",");
      }
      builder.append("{");

      if (currRequest.updateType == UpdateManager.UpdateType.UPDATE) {
        builder.append("\"_sonicbase_dbname\": \"").append(currRequest.dbName).append("\",");
        builder.append("\"_sonicbase_tablename\": \"").append(currRequest.tableName).append("\",");
        builder.append("\"_sonicbase_action\": \"").append(currRequest.updateType).append("\",");
        builder.append("\"before\" : {");

        TableSchema tableSchema = server.getTableSchema(currRequest.dbName, currRequest.tableName, server.getDataDir());
        Record record = new Record(currRequest.dbName, server.getCommon(), currRequest.existingBytes);
        getJsonFromRecord(builder, tableSchema, record);
        builder.append("},");
        builder.append("\"after\": {");
        record = new Record(currRequest.dbName, server.getCommon(), currRequest.recordBytes);
        getJsonFromRecord(builder, tableSchema, record);
        builder.append("}");

      }
      else {
        TableSchema tableSchema = server.getTableSchema(currRequest.dbName, currRequest.tableName, server.getDataDir());
        Record record = new Record(currRequest.dbName, server.getCommon(), currRequest.recordBytes);
        builder.append("\"_sonicbase_dbname\": \"").append(currRequest.dbName).append("\",");
        builder.append("\"_sonicbase_tablename\": \"").append(currRequest.tableName).append("\",");
        builder.append("\"_sonicbase_action\": \"").append(currRequest.updateType).append("\",");

        builder.append("\"record\": {");
        getJsonFromRecord(builder, tableSchema, record);
        builder.append("}");
      }
      builder.append("}");
      offset++;
    }

    builder.append("]}");
    messages.add(builder.toString());
  }

  public void publishBatch(ComObject cobj) {
    if (producers.isEmpty()) {
      return;
    }

    if (!producers.isEmpty() && threadLocalMessageRequests.get() != null && threadLocalMessageRequests.get().size() != 0) {
      try {
        if (!cobj.getBoolean(ComObject.Tag.CURR_REQUEST_IS_MASTER)) {
          return;
        }

        List<MessageRequest> toPublish = new ArrayList<>();
        while(true) {
          for (int i = 0; threadLocalMessageRequests.get().size() != 0; i++) {
            toPublish.add(threadLocalMessageRequests.get().remove(0));
          }
          if (threadLocalMessageRequests.get().size() == 0) {
            break;
          }
        }
        for (MessageRequest msg : toPublish) {
          if (!msg.dbName.equals("_sonicbase_sys")) {
            publishQueue.put(msg);
          }
        }
      }
      catch (Exception e) {
        logger.error("Error publishing messages", e);
      }
      finally {
        threadLocalMessageRequests.set(null);
        threadLocalIsBatchRequest.set(false);
      }
    }
  }

  public void publishInsertOrUpdate(ComObject cobj, String dbName, String tableName, byte[] recordBytes, byte[] existingBytes,
                                    UpdateManager.UpdateType updateType) {
    if (dbName.equals("_sonicbase_sys")) {
      return;
    }
    if (producers.isEmpty()) {
      return;
    }

    try {
      if (!cobj.getBoolean(ComObject.Tag.CURR_REQUEST_IS_MASTER)) {
        return;
      }

      MessageRequest request = new MessageRequest();
      request.dbName = dbName;
      request.tableName = tableName;
      request.recordBytes = recordBytes;
      request.existingBytes = existingBytes;
      request.updateType = updateType;
      publishQueue.put(request);
    }
    catch (Exception e) {
      logger.error("Error publishing message", e);
    }
  }

  public static void getJsonFromRecord(StringBuilder builder, TableSchema tableSchema, Record record) {
    String fieldName = null;
    try {

      builder.append("\"_sonicbase_sequence0\": ").append(record.getSequence0()).append(",");
      builder.append("\"_sonicbase_sequence1\": ").append(record.getSequence1()).append(",");
      builder.append("\"_sonicbase_sequence2\": ").append(record.getSequence2());

      List<FieldSchema> fields = tableSchema.getFields();
      for (FieldSchema fieldSchema : fields) {
        fieldName = fieldSchema.getName();
        int offset = tableSchema.getFieldOffset(fieldName);
        Object[] recordFields = record.getFields();
        if (recordFields[offset] == null) {
          continue;
        }

        builder.append(",");
        switch (fieldSchema.getType()) {
          case VARCHAR:
          case CHAR:
          case LONGVARCHAR:
          case CLOB:
          case NCHAR:
          case NVARCHAR:
          case LONGNVARCHAR:
          case NCLOB: {
            String value = new String((char[]) recordFields[offset]);
            value = JsonEncoder.encode(value);
            builder.append("\"").append(fieldName).append("\": \"").append(value).append("\"");
          }
          break;
          case BIT:
          case TINYINT:
          case SMALLINT:
          case INTEGER:
          case BIGINT:
          case FLOAT:
          case REAL:
          case DOUBLE:
          case NUMERIC:
          case DECIMAL:
          case BOOLEAN:
          case ROWID:
            builder.append("\"").append(fieldName).append("\": ").append(String.valueOf(recordFields[offset]));
            break;
          case DATE:{
            Calendar cal = Calendar.getInstance();
            cal.setTime((java.util.Date)recordFields[offset]);

            String value = DateUtils.toDbString(cal);
            value = JsonEncoder.encode(value);
            builder.append("\"").append(fieldName).append("\": ").append("\"").append(value).append("\"");
          }
          break;
          case TIME: {
            String value = DateUtils.toDbTimeString((Time)recordFields[offset]);
            value = JsonEncoder.encode(value);
            builder.append("\"").append(fieldName).append("\": ").append("\"").append(value).append("\"");
          }
          break;
          case TIMESTAMP: {
            String value = DateUtils.toDbTimestampString((Timestamp)recordFields[offset]);
            value = JsonEncoder.encode(value);
            builder.append("\"").append(fieldName).append("\": \"").append(value).append("\"");
          }
          break;
          case BINARY:
          case VARBINARY:
          case LONGVARBINARY:
          case BLOB:
            builder.append("\"").append(fieldName).append("\": \"").append(
                new Base64().encode((byte[]) recordFields[offset])).append("\"");
            break;
        }
      }
    }
    catch (Exception e) {
      throw new DatabaseException("Error converting record: field=" + fieldName, e);
    }
  }
}
