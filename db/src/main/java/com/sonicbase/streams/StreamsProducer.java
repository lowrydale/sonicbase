package com.sonicbase.streams;

import java.util.List;

public interface StreamsProducer {

  void init(String cluster, String jsonConfig, String jsonQueueConfig);

  void publish(List<String> messages);

  void shutdown();
}
