package com.sonicbase.queue;

import java.util.List;

public interface MessageQueueConsumer {

  void init(String cluster, String jsonConfig, String jsonQueueConfig);

  List<Message> receive();

  void acknowledgeMessage(Message message);

  void shutdown();
}
