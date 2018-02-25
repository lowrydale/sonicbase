package com.sonicbase.streams;

import java.util.List;

public interface StreamsConsumer {

  int init(String cluster, String jsonConfig, String jsonQueueConfig);

  void initThread();

  List<Message> receive();

  void acknowledgeMessages(List<Message> messages);

  void handleError(List<Message> messages, Exception e);

  void shutdown();

}
