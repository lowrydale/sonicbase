package com.sonicbase.streams;

import com.sonicbase.common.Config;
import com.sonicbase.query.DatabaseException;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

public class KafkaProducer implements StreamsProducer {

  private static Logger logger = LoggerFactory.getLogger(KafkaProducer.class);

  private String topic;
  private Producer<String, String> producer;

  @Override
  public void init(Config config, String installDir, Map<String, Object> streamConfig) {

    try {
      logger.info("kafka producer init - begin");
      String servers = (String) streamConfig.get("servers");
      this.topic = (String) streamConfig.get("topic");

      Properties props = new Properties();
      props.put("compression.type", "gzip");
      //props.put("compressed.topics", topic);

      props.put("bootstrap.servers", servers);
      if (streamConfig.containsKey("kafka.acks")) {
        props.put("acks", streamConfig.get("kafka.acks"));
      }
      else {
        props.put("acks", "1");
      }
      props.put("retries", 0);
      if (streamConfig.containsKey("kafka.batch.size")) {
        props.put("batch.size", streamConfig.get("kafka.batch.size"));
      }
      else {
        props.put("batch.size", 1_638_400);
      }
      if (streamConfig.containsKey("kafka.linger.ms")) {
        props.put("linger.ms", streamConfig.get("kafka.linger.ms"));
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
