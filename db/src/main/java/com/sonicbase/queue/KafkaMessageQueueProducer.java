package com.sonicbase.queue;

import com.sonicbase.query.DatabaseException;
import com.sonicbase.util.JsonDict;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaMessageQueueProducer implements MessageQueueProducer {
  private String topic;
  private Producer<String, String> producer;

  @Override
  public void init(String cluster, String jsonConfig, String jsonQueueConfig) {

    JsonDict queueConfig = new JsonDict(jsonQueueConfig);
    String servers = queueConfig.getString("servers");
    this.topic = queueConfig.getString("topic");

    Properties props = new Properties();
    props.put("bootstrap.servers", servers);
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    producer = new KafkaProducer<>(props);
  }

  @Override
  public void publish(String message) {
    try {
      Future<RecordMetadata> response = producer.send(new ProducerRecord<>(topic, "message", message));
      response.get();
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public void shutdown() {

  }
}
