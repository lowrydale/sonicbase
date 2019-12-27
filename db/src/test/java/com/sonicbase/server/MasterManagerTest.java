package com.sonicbase.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.Config;
import org.apache.commons.io.IOUtils;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class MasterManagerTest {

  @Test
  public void test() throws IOException, InterruptedException {
    final AtomicBoolean calledErrorLogger = new AtomicBoolean();
    String configStr = IOUtils.toString(DatabaseServerTest.class.getResourceAsStream("/config/config-1-local.yaml"), "utf-8");
    Config config = new Config(configStr);
    com.sonicbase.server.DatabaseServer server = new DatabaseServer() {
      @Override
      public Logger getErrorLogger() {
        Logger mockLogger = mock(Logger.class);
        doAnswer((Answer<Object>) invocationOnMock -> {
          calledErrorLogger.set(true);
          return null;
        }).when(mockLogger).error(anyObject());
        return mockLogger;
      }
    };
    Config.copyConfig("test");
    server.setConfig(config, "localhost", 9010, true, new AtomicBoolean(), new AtomicBoolean(), "gc.log", false);
    DatabaseClient client = mock(DatabaseClient.class);
    server.setDatabaseClient(client);
    server.setReplicationFactor(2);

    MasterManager masterManager = server.getMasterManager();
    masterManager.startMasterMonitor(5L);
    Thread.sleep(500L);
  }
}
