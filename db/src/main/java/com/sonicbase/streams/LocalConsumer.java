package com.sonicbase.streams;

import com.sonicbase.query.DatabaseException;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class LocalConsumer implements StreamsConsumer {

  private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(StreamsConsumer.class);

  private KafkaConsumer<String, String> consumer;
  private boolean shutdown;

  public void shutdown() {
    this.shutdown = true;
  }

  @Override
  public int init(String cluster, String jsonConfig, String jsonQueueConfig) {
    return 1;
  }

  @Override
  public void initThread() {

  }

  @Override
  public List<com.sonicbase.streams.Message> receive() {
    try {
      List<com.sonicbase.streams.Message> ret = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        com.sonicbase.streams.Message message = new com.sonicbase.streams.Message();
        String msg = LocalProducer.queue.poll(100, TimeUnit.MILLISECONDS);
        if (msg != null) {
          message.setBody(msg);
          ret.add(message);
        }
        else {
          break;
        }
      }
      return ret;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public void acknowledgeMessages(List<com.sonicbase.streams.Message> messages) {
  }

  @Override
  public void handleError(List<Message> messages, Exception e) {

  }

}

