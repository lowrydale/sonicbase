package com.sonicbase.server;

import com.sonicbase.common.ComObject;
import com.sonicbase.common.FileUtils;
import com.sonicbase.socket.DatabaseSocketClient;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertEquals;

public class NettyServerTest {

  @Test
  public void test() throws InterruptedException, IOException {
    FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "/db-data"));

    final NettyServer server = new NettyServer();
    Thread thread = new Thread(() -> {
      String argsStr = "-host localhost -port 9010 -gclog gc.log -xmx 2G";
      String[] args = argsStr.split(" ");
      server.startServer(args);
    });
    thread.start();
    Thread.sleep(10000);

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

    requests = new ArrayList<>();
    request = new DatabaseSocketClient.Request();
    cobj = new ComObject(2);
    cobj.put(ComObject.Tag.COUNT, 10);
    cobj.put(ComObject.Tag.METHOD, "echo");
    request.setBody(cobj.serialize());
    request.setHostPort("localhost:9010");
    requests.add(request);
    DatabaseSocketClient.doSend(requests);
    response = requests.get(0).getResponse();
    retObj = new ComObject(response);
    assertEquals((int) retObj.getInt(ComObject.Tag.COUNT), 10);

    cobj = new ComObject(2);
    cobj.put(ComObject.Tag.COUNT, 10);
    cobj.put(ComObject.Tag.METHOD, "echo");
    DatabaseSocketClient client = new DatabaseSocketClient();
    response = client.doSend(null, cobj.serialize(), "localhost:9010");
    retObj = new ComObject(response);
    assertEquals((int) retObj.getInt(ComObject.Tag.COUNT), 10);

    DatabaseSocketClient.shutdown();

    thread.interrupt();
    server.shutdown();
  }

}
