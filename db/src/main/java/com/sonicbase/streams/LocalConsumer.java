package com.sonicbase.streams;

import com.sonicbase.common.Config;
import com.sonicbase.query.DatabaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class LocalConsumer implements StreamsConsumer {

  private static Logger logger = LoggerFactory.getLogger(LocalConsumer.class);

  private boolean shutdown;

  public void shutdown() {
    this.shutdown = true;
  }

  @Override
  public int init(Config config, String installDir, Map<String, Object> streamConfig) {
    return 1;
  }

  @Override
  public void initThread() {

  }

  @Override
  public List<Message> receive() {
    try {
      List<Message> ret = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        com.sonicbase.streams.Message message = new com.sonicbase.streams.Message();
        String msg = LocalProducer.queue.poll(100, TimeUnit.MILLISECONDS);
        if (msg != null) {
          message.setBody(msg);
          ret.add(message);
        }
        else {
          break;
        }
      }
      return ret;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public void acknowledgeMessages(List<Message> messages) {
  }

  @Override
  public void handleError(List<Message> messages, Exception e) {

  }

}

