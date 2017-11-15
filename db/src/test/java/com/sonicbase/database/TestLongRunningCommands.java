package com.sonicbase.database;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.ComObject;
import com.sonicbase.server.MethodInvoker;
import com.sonicbase.server.DatabaseServer;
import com.sonicbase.server.LongRunningCalls;
import org.apache.commons.io.IOUtils;
import org.codehaus.plexus.util.FileUtils;
import org.testng.annotations.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.testng.Assert.assertEquals;

/**
 * Responsible for
 */
public class TestLongRunningCommands {

  @Test
  public void test() throws IOException, InterruptedException {
    String configStr = IOUtils.toString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-4-servers.json")), "utf-8");
    ObjectMapper mapper = new ObjectMapper();
    final ObjectNode config = (ObjectNode) mapper.readTree(configStr);

    FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db"));

    ArrayNode array = new ArrayNode(JsonNodeFactory.instance);
    array.add(DatabaseServer.FOUR_SERVER_LICENSE);
    config.put("licenseKeys", array);

    DatabaseClient.getServers().clear();

    DatabaseServer server = new DatabaseServer();
    server.setConfig(config, "4-servers", "localhost", 9010, true, new AtomicBoolean(true), null, true);
    server.disableLogProcessor();
    server.shutdownRepartitioner();

    ComObject cobj  = new ComObject();
    cobj.put(ComObject.Tag.method, "echo");
    LongRunningCalls.SingleCommand command = server.getLongRunningCommands().createSingleCommand(cobj.serialize());
    //LongRunningCalls.SingleCommand command2 = server.getLongRunningCommands().createSingleCommand("DatabaseServer:echo2:1:1:test:10", null);
    server.getLongRunningCommands().addCommand(command);
    //server.getLongRunningCommands().addCommand(command2);

//    while (true) {
//      if (DatabaseServer.echoCount.get() == 10) {
//        System.out.println("Echoed");
//        break;
//      }
//      Thread.sleep(1000);
//    }
    Thread.sleep(1000);

    server.getLongRunningCommands().addCommand(server.getLongRunningCommands().createSingleCommand(cobj.serialize()));

    while (true) {
      if (MethodInvoker.echoCount.get() == 11) {
        System.out.println("Echoed");
        break;
      }
      Thread.sleep(1000);
    }

    Thread.sleep(1000);

//    while (true) {
//      if (DatabaseServer.echo2Count.get() == 10) {
//        System.out.println("Echoed");
//        break;
//      }
//      Thread.sleep(1000);
//    }
    assertEquals(server.getLongRunningCommands().getCommandCount(), 0);

    server.getLongRunningCommands().load();

    assertEquals(server.getLongRunningCommands().getCommandCount(), 0);

    cobj = new ComObject();
    cobj.put(ComObject.Tag.dbName, "__none__");
    cobj.put(ComObject.Tag.schemaVersion, 1L);
    cobj.put(ComObject.Tag.method, "block");
    server.getLongRunningCommands().addCommand(server.getLongRunningCommands().createSingleCommand(cobj.serialize()));

    Thread.sleep(1000);

    int count = MethodInvoker.blockCount.get();

    server = new DatabaseServer();
    server.setConfig(config, "4-servers", "localhost", 9010, true, new AtomicBoolean(true), null, true);
    server.disableLogProcessor();
    server.shutdownRepartitioner();

    Thread.sleep(1000);

    assertEquals(MethodInvoker.blockCount.get(), count + 1);

  }
}
