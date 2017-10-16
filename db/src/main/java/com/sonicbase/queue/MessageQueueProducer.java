package com.sonicbase.queue;

public interface MessageQueueProducer {

  void init(String cluster, String jsonConfig, String jsonQueueConfig);

  void publish(String message);

  void shutdown();
}
