package com.sonicbase.database;

import com.sonicbase.server.DatabaseServer;
import com.sonicbase.server.LongRunningCommands;
import com.sonicbase.util.JsonArray;
import com.sonicbase.util.JsonDict;
import com.sonicbase.util.StreamUtils;
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
    String configStr = StreamUtils.inputStreamToString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-4-servers.json")));
    final JsonDict config = new JsonDict(configStr);

    JsonArray array = config.getDict("database").putArray("licenseKeys");
    array.add(DatabaseServer.FOUR_SERVER_LICENSE);

    FileUtils.deleteDirectory(new File("/data/database"));

    DatabaseServer.getServers().clear();

    DatabaseServer server = new DatabaseServer();
    server.setConfig(config, "4-servers", "localhost", 9010, true, new AtomicBoolean(true), null);
    server.disableLogProcessor();
    server.disableRepartitioner();

    LongRunningCommands.SingleCommand command = server.getLongRunningCommands().createSingleCommand("DatabaseServer:echo:1:1:test:10", null);
    //LongRunningCommands.SingleCommand command2 = server.getLongRunningCommands().createSingleCommand("DatabaseServer:echo2:1:1:test:10", null);
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

    server.getLongRunningCommands().addCommand(server.getLongRunningCommands().createSingleCommand("DatabaseServer:echo:1:1:test:11", null));

    while (true) {
      if (DatabaseServer.echoCount.get() == 11) {
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

    server.getLongRunningCommands().addCommand(server.getLongRunningCommands().createSingleCommand("DatabaseServer:block:1:1:test:20", null));

    Thread.sleep(1000);

    int count = DatabaseServer.blockCount.get();

    server = new DatabaseServer();
    server.setConfig(config, "4-servers", "localhost", 9010, true, new AtomicBoolean(true), null);
    server.disableLogProcessor();
    server.disableRepartitioner();

    Thread.sleep(1000);

    assertEquals(DatabaseServer.blockCount.get(), count + 1);

  }
}
