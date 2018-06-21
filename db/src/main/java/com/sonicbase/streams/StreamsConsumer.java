package com.sonicbase.streams;

import com.sonicbase.streams.Message;

import java.util.List;

public interface StreamsConsumer {

  int init(String cluster, String jsonConfig, String jsonQueueConfig);

  void initThread();

  List<com.sonicbase.streams.Message> receive();

  void acknowledgeMessages(List<com.sonicbase.streams.Message> messages);

  void handleError(List<Message> messages, Exception e);

  void shutdown();

}
