package com.sonicbase.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sonicbase.common.Config;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.query.DatabaseException;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;

public class KafkaConsumer implements StreamsConsumer {

  private static Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

  private ThreadLocal<org.apache.kafka.clients.consumer.KafkaConsumer> consumer = new ThreadLocal<>();
  private boolean shutdown;
  private Connection conn;
  private AtomicInteger messageCountSinceSavedSequence = new AtomicInteger();
  private String topic;
  private List<TopicPartition> ownedPartitions;
  private int sonicBaseShardCount;
  private List<org.apache.kafka.clients.consumer.KafkaConsumer> consumers = new ArrayList<>();
  private String cluster;
  private Config config;
  private Map<String, Object> streamConfig;

  class KafkaMessage extends com.sonicbase.streams.Message {
    private final int partition;
    private final long offset;

    KafkaMessage(String body, int partition, long offset) {
      super(body);
      this.partition = partition;
      this.offset = offset;
    }
  }

  public void shutdown() {
    this.shutdown = true;
    try {
      conn.close();
      for (org.apache.kafka.clients.consumer.KafkaConsumer consumer : consumers) {
        consumer.close();
      }
      consumers.clear();
    }
    catch (SQLException e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public int init(String cluster, Config config, Map<String, Object> streamConfig) {

    try {
      this.cluster = cluster;
      this.config = config;
      this.streamConfig = streamConfig;
      topic = (String) streamConfig.get("topic");

      initConnection(config);
      initTable();

      return 1;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private void initConnection(Config config) {
    try {
      List<Config.Shard> array = config.getShards();
      Config.Shard shard = array.get(0);
      List<Config.Replica> replicasArray = shard.getReplicas();
      final String address = config.getBoolean("clientIsPrivate") ?
          replicasArray.get(0).getString("privateAddress") :
          replicasArray.get(0).getString("publicAddress");
      final int port = replicasArray.get(0).getInt("port");

      Class.forName("com.sonicbase.jdbcdriver.Driver");

      conn = DriverManager.getConnection("jdbc:sonicbase:" + address + ":" + port);

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
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private void initTable() {
    PreparedStatement stmt = null;
    try {
      try {
        stmt = conn.prepareStatement("describe table kafka_stream_state");
        stmt.executeQuery();
      }
      catch (Exception e) {
        stmt = conn.prepareStatement("create table kafka_stream_state (topic VARCHAR, _partition BIGINT, _offset BIGINT, PRIMARY KEY (topic, _partition))");
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

  @Override
  public void initThread() {
    try {
      logger.info("kafka consumer initThread - begin");
      ObjectMapper mapper = new ObjectMapper();
      String servers = (String) streamConfig.get("servers");
      String topic = (String) streamConfig.get("topic");
      Properties props = new Properties();
      props.put("bootstrap.servers", servers);
      props.put("group.id", "sonicbase-queue");
      props.put("enable.auto.commit", "true");
      props.put("auto.commit.interval.ms", "1000");
      props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
      props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
      final org.apache.kafka.clients.consumer.KafkaConsumer consumer = new org.apache.kafka.clients.consumer.KafkaConsumer(props);

      readState();

      List<PartitionInfo> partitions = new ArrayList<>();
      partitions.addAll(consumer.partitionsFor(topic));
      logger.info("kafka consumer partition count=" + partitions.size());

      Collections.sort(partitions, new Comparator<PartitionInfo>() {
        @Override
        public int compare(PartitionInfo o1, PartitionInfo o2) {
          return Integer.compare(o1.partition(), o2.partition());
        }
      });

      int thisServerShard = config.getInt("shard");
      int thisServerReplica = config.getInt("replica");

      List<Config.Shard> array = config.getShards();
      sonicBaseShardCount = array.size();
      List<Config.Replica> replicaArray = array.get(0).getReplicas();
      int replicaCount = replicaArray.size();

      String ownedStr = new String();
      ownedPartitions = new ArrayList<>();
      int partitionOffset = 0;
      while (true) {
        outer:
        for (int i = 0; i < sonicBaseShardCount; i++) {
          for (int j = 0; j < replicaCount; j++) {
            if (i == thisServerShard) {
              ownedPartitions.add(new TopicPartition(topic, partitions.get(partitionOffset).partition()));
              ownedStr += "," + partitions.get(partitionOffset).partition();
            }
            partitionOffset++;
            if (partitionOffset >= partitions.size()) {
              break outer;
            }
          }
        }
        if (partitionOffset >= partitions.size()) {
          break;
        }
      }

      logger.info("kafka consumer owned partitions: " + ownedStr);
      if (ownedPartitions.size() != 0) {
        //consumer.subscribe(asList(topic));
        consumer.assign(ownedPartitions);

        for (TopicPartition topicPartition : ownedPartitions) {
          Long offset = partitionOffsets.get(topicPartition.partition());
          if (offset == null) {
            logger.info("kafka consumer seekToBeginning: partition=" + topicPartition.partition());
            consumer.seekToBeginning(asList(topicPartition));
          }
          else {
            logger.info("kafka consumer seek: partition=" + topicPartition.partition() + ", offset=" + offset);
            consumer.seek(topicPartition, offset);
          }
        }
      }

      this.consumers.add(consumer);
      this.consumer.set(consumer);
      logger.info("kafka consumer initThread - end");
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private void readState() {
    PreparedStatement stmt = null;
    try {
      partitionOffsets.clear();

      stmt = conn.prepareStatement("select * from kafka_stream_state where topic='" + topic + "'");
      ResultSet rs = stmt.executeQuery();
      while (rs.next()) {
        partitionOffsets.put(rs.getInt("_partition"), rs.getLong("_offset"));
      }
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

  private void saveState() {
    PreparedStatement stmt = null;
    try {
      for (Map.Entry<Integer, Long> entry : partitionOffsets.entrySet()) {
        try {
          stmt = conn.prepareStatement("insert ignore into kafka_stream_state (topic, _partition, _offset) VALUES (?, ?, ?)");
          stmt.setString(1, topic);
          stmt.setLong(2, entry.getKey());
          stmt.setLong(3, entry.getValue());
          stmt.executeUpdate();
          stmt.close();
        }
        finally {
          stmt.close();
        }
      }
    }
    catch (SQLException e) {
      throw new DatabaseException(e);
    }
  }

  ConcurrentHashMap<Integer, Long> partitionOffsets = new ConcurrentHashMap<>();

  @Override
  public List<Message> receive() {
    try {
      if (ownedPartitions.size() == 0) {
        Thread.sleep(1_000);
        return null;
      }

      ConsumerRecords<String, String> records = consumer.get().poll(100);
      List<Message> resultMessages = new ArrayList<>();
      for (ConsumerRecord<String, String> record : records) {
        resultMessages.add(new KafkaMessage(record.value(), record.partition(), record.offset()));
      }
      return resultMessages;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public void acknowledgeMessages(List<Message> messages) {
    for (com.sonicbase.streams.Message message : messages) {
      partitionOffsets.put(((KafkaMessage) message).partition, ((KafkaMessage) message).offset);

      if (messageCountSinceSavedSequence.incrementAndGet() % 1000 == 0) {
        saveState();
      }
    }
  }

  @Override
  public void handleError(List<Message> messages, Exception e) {

  }

}
