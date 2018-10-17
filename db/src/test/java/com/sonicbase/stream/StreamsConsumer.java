/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.stream;

import com.sonicbase.common.Config;

import java.util.List;
import java.util.Map;

public interface StreamsConsumer {

  int init(String cluster, Config config, Map<String, Object> streamConfig);

  void initThread();

  List<Message> receive();

  void acknowledgeMessages(List<Message> messages);

  void handleError(List<Message> messages, Exception e);

  void shutdown();

}
