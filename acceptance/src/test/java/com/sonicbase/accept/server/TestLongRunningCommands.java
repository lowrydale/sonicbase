package com.sonicbase.accept.server;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.ComObject;
import com.sonicbase.common.Config;
import com.sonicbase.server.DatabaseServer;
import com.sonicbase.server.LongRunningCalls;
import com.sonicbase.server.MethodInvoker;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.testng.Assert.assertEquals;

public class TestLongRunningCommands {

  @Test
  public void test() throws IOException, InterruptedException {
    System.setProperty("log4j.configuration", "test-log4j.xml");

    String configStr = IOUtils.toString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-4-servers.yaml")), "utf-8");
    Config config = new Config(configStr);

    FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db-data"));

    DatabaseClient.getServers().clear();

    com.sonicbase.server.DatabaseServer server = new com.sonicbase.server.DatabaseServer();
    try {
      server.setConfig(config, "4-servers", "localhost", 9010, true, new AtomicBoolean(true), new AtomicBoolean(true), null, false);
      server.shutdownRepartitioner();

      ComObject cobj = new ComObject(2);
      cobj.put(ComObject.Tag.METHOD, "echo");
      cobj.put(ComObject.Tag.COUNT, 10);
      LongRunningCalls.SingleCommand command = server.getLongRunningCommands().createSingleCommand(cobj.serialize());
      //LongRunningCalls.SingleCommand command2 = server.getLongRunningCommands().createSingleCommand("DatabaseServer:echo2:1:1:test:10", null);
      server.getLongRunningCommands().addCommand(command);
      //server.getLongRunningCommands().addCommand(command2);

      while (true) {
        if (MethodInvoker.echoCount.get() == 10) {
          System.out.println("Echoed");
          break;
        }
        Thread.sleep(1000);
      }
      Thread.sleep(1000);

      cobj.put(ComObject.Tag.COUNT, 11);
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

      cobj = new ComObject(3);
      cobj.put(ComObject.Tag.DB_NAME, "__none__");
      cobj.put(ComObject.Tag.SCHEMA_VERSION, 1L);
      cobj.put(ComObject.Tag.METHOD, "block");
      server.getLongRunningCommands().addCommand(server.getLongRunningCommands().createSingleCommand(cobj.serialize()));

      Thread.sleep(1000);

    }
    finally {
      server.shutdown();
    }

    int count = MethodInvoker.blockCount.get();

    try {
      server = new DatabaseServer();
      server.setConfig(config, "4-servers", "localhost", 9010, true, new AtomicBoolean(true), new AtomicBoolean(true), null, false);
      server.shutdownRepartitioner();

      Thread.sleep(1000);

      assertEquals(MethodInvoker.blockCount.get(), count + 1);
    }
    finally {
      server.shutdown();

      System.out.println("client refCount=" + DatabaseClient.clientRefCount.get() + ", sharedClients=" + DatabaseClient.sharedClients.size() + ", class=TestLongRunningCommands");
      for (DatabaseClient client : DatabaseClient.allClients) {
        System.out.println("Stack:\n" + client.getAllocatedStack());
      }
    }


  }
}
