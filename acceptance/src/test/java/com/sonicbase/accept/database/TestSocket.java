package com.sonicbase.accept.database;

import com.sonicbase.common.ComObject;
import com.sonicbase.server.NettyServer;
import com.sonicbase.socket.DatabaseSocketClient;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertEquals;

public class TestSocket {

  @Test(enabled=false)
  public void test() throws InterruptedException {
    Thread thread = new Thread(() -> {
      NettyServer server = new NettyServer();
      String argsStr = "-host localhost -port 9010 -gclog gc.log -xmx 2G";
      String[] args = argsStr.split(" ");
      server.startServer(args);
    });
    thread.start();
    Thread.sleep(5000);

    for (int i = 0; i < 20; i++) {
      List<DatabaseSocketClient.Request> requests = new ArrayList<>();
      DatabaseSocketClient.Request request = new DatabaseSocketClient.Request();
      ComObject cobj = new ComObject(2);
      cobj.put(ComObject.Tag.COUNT, 10);
      cobj.put(ComObject.Tag.METHOD, "echo");
      request.setBody(cobj.serialize());
      requests.add(request);
      DatabaseSocketClient.sendBatch("localhost", 9010, requests);
      byte[] response = requests.get(0).getResponse();
      ComObject retObj = new ComObject(response);
      assertEquals((int) retObj.getInt(ComObject.Tag.COUNT), 10);
    }
  }

}
