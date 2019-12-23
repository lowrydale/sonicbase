package com.sonicbase.streams;

import com.sonicbase.common.Config;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

public class LocalProducer implements StreamsProducer {
  public static ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<>(1000);

  public static ArrayBlockingQueue<String> getQueue() {
    return queue;
  }

  @Override
  public void init(Config config, String installDir, Map<String, Object> streamConfig) {
  }

  @Override
  public void publish(List<String> messages) {
    queue.addAll(messages);
  }

  @Override
  public void shutdown() {

  }
}
