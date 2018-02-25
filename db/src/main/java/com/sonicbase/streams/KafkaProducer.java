package com.sonicbase.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.query.DatabaseException;
import org.apache.kafka.clients.producer.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

public class KafkaProducer implements StreamsProducer {

  private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(KafkaProducer.class);

  private String topic;
  private Producer<String, String> producer;

  @Override
  public void init(String cluster, String jsonConfig, String jsonQueueConfig) {

    try {
      logger.info("kafka producer init - begin");
      ObjectMapper mapper = new ObjectMapper();
      ObjectNode queueConfig = (ObjectNode) mapper.readTree(jsonQueueConfig);
      String servers = queueConfig.get("servers").asText();
      this.topic = queueConfig.get("topic").asText();

      Properties props = new Properties();
      props.put("compression.type", "gzip");
      //props.put("compressed.topics", topic);

      props.put("bootstrap.servers", servers);
      if (queueConfig.has("kafka.acks")) {
        props.put("acks", queueConfig.get("kafka.acks").asText());
      }
      else {
        props.put("acks", "1");
      }
      props.put("retries", 0);
      if (queueConfig.has("kafka.batch.size")) {
        props.put("batch.size", queueConfig.get("kafka.batch.size").asInt());
      }
      else {
        props.put("batch.size", 1_638_400);
      }
      if (queueConfig.has("kafka.linger.ms")) {
        props.put("linger.ms", queueConfig.get("kafka.linger.ms").asInt());
      }
      else {
        props.put("linger.ms", 1000);
      }
      props.put("buffer.memory", 33554432);
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
      props.put("topic.metadata.refresh.interval.ms", 5_000);

      producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);
      logger.info("kafka producer init - end");
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public void publish(List<String> messages) {
    try {
      List<Future<RecordMetadata>> futures = new ArrayList<>();
      for (String message : messages) {
        int key = ThreadLocalRandom.current().nextInt();
        Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, String.valueOf(key), message));
        futures.add(future);
      }
      for (Future<RecordMetadata> future : futures) {
        RecordMetadata meta = future.get();
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public void shutdown() {

  }
}
