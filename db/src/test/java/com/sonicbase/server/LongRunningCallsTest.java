/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.ComObject;
import com.sonicbase.common.FileUtils;
import org.apache.commons.io.IOUtils;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;

public class LongRunningCallsTest {

  @Test
  public void test() throws IOException, InterruptedException {
    FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "/db-data"));

    String configStr = IOUtils.toString(DatabaseServerTest.class.getResourceAsStream("/config/config-1-local.json"), "utf-8");
    ObjectNode config = (ObjectNode) new ObjectMapper().readTree(configStr);
    DatabaseServer server = new DatabaseServer();
    server.setConfig(config, "test", "localhost", 9010, true, new AtomicBoolean(), new AtomicBoolean(), "gc.log", true);
    server.setRecovered(true);

    DatabaseClient client = mock(DatabaseClient.class);
    server.setDatabaseClient(client);

    LongRunningCalls calls = server.getLongRunningCommands();
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.method, "echo");
    cobj.put(ComObject.Tag.count, 1);
    calls.addCommand(calls.createSingleCommand(cobj.serialize()));

    assertEquals(calls.getCommandCount(), 1);
    calls.execute();

    Thread.sleep(1_000);
    assertEquals(server.getMethodInvoker().getEchoCount(), 1);

    server.shutdown();
  }
}
