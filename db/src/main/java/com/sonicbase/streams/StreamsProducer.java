/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.streams;

import com.sonicbase.common.Config;

import java.util.List;
import java.util.Map;

public interface StreamsProducer {

  void init(Config config, String installDir, Map<String, Object> streamCOnfig);

  void publish(List<String> messages);

  void shutdown();
}
