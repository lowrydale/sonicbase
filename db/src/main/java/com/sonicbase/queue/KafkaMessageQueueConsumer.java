package com.sonicbase.queue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.query.DatabaseException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class KafkaMessageQueueConsumer implements MessageQueueConsumer {

  private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(MessageQueueConsumer.class);

  private KafkaConsumer<String, String> consumer;
  private boolean shutdown;

  public void shutdown() {
    this.shutdown = true;
  }

  @Override
  public void init(String cluster, String jsonConfig, String jsonQueueConfig) {
    try {
      ObjectMapper mapper = new ObjectMapper();
      ObjectNode queueConfig = (ObjectNode) mapper.readTree(jsonQueueConfig);
      String servers = queueConfig.get("servers").asText();
      String topic = queueConfig.get("topic").asText();
      Properties props = new Properties();
      props.put("bootstrap.servers", servers);
      props.put("group.id", "sonicbase-queue");
      props.put("enable.auto.commit", "true");
      props.put("auto.commit.interval.ms", "1000");
      props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
      props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
      consumer = new KafkaConsumer<>(props);
      consumer.subscribe(Arrays.asList(topic));
      consumer.seekToBeginning(new ArrayList<TopicPartition>());
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public List<Message> receive() {
    ConsumerRecords<String, String> records = consumer.poll(100);
    List<com.sonicbase.queue.Message> resultMessages = new ArrayList<>();
    for (ConsumerRecord<String, String> record : records) {
      resultMessages.add(new Message(record.value()));
      logger.info("received message: key=" + record.key() + ", value=" + record.value());
    }
    return resultMessages;
  }

  @Override
  public void acknowledgeMessage(com.sonicbase.queue.Message message) {
  }

}
