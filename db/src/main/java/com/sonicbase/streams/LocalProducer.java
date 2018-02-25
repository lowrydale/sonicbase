package com.sonicbase.streams;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public class LocalProducer implements StreamsProducer {
  public static ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<>(1000);

  @Override
  public void init(String cluster, String jsonConfig, String jsonQueueConfig) {
  }

  @Override
  public void publish(List<String> messages) {
    queue.addAll(messages);
  }

  @Override
  public void shutdown() {

  }
}
