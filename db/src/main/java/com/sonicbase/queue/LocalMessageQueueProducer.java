package com.sonicbase.queue;

import java.util.ArrayList;
import java.util.List;

public class LocalMessageQueueProducer implements MessageQueueProducer {
  public static List<String> queue = new ArrayList<>();

  @Override
  public void init(String cluster, String jsonConfig, String jsonQueueConfig) {
  }

  @Override
  public void publish(String message) {
    queue.add(message);
  }

  @Override
  public void shutdown() {

  }
}
