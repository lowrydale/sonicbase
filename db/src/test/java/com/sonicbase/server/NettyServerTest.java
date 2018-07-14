/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
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
    Thread thread = new Thread(new Runnable(){
      @Override
      public void run() {
        String argsStr = "-host localhost -port 9010 -cluster 1-local -gclog gc.log -xmx 2G";
        String[] args = argsStr.split(" ");
        server.startServer(args, null, true);
      }
    });
    thread.start();
    Thread.sleep(10000);

    List<DatabaseSocketClient.Request> requests = new ArrayList<>();
    DatabaseSocketClient.Request request = new DatabaseSocketClient.Request();
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.count, 10);
    cobj.put(ComObject.Tag.method, "echo");
    request.setBody(cobj.serialize());
    requests.add(request);
    DatabaseSocketClient.sendBatch("localhost", 9010, requests);
    byte[] response = requests.get(0).getResponse();
    ComObject retObj = new ComObject(response);
    assertEquals((int) retObj.getInt(ComObject.Tag.count), 10);

    requests = new ArrayList<>();
    request = new DatabaseSocketClient.Request();
    cobj = new ComObject();
    cobj.put(ComObject.Tag.count, 10);
    cobj.put(ComObject.Tag.method, "echo");
    request.setBody(cobj.serialize());
    request.setHostPort("localhost:9010");
    requests.add(request);
    DatabaseSocketClient.do_send(requests);
    response = requests.get(0).getResponse();
    retObj = new ComObject(response);
    assertEquals((int) retObj.getInt(ComObject.Tag.count), 10);

    cobj = new ComObject();
    cobj.put(ComObject.Tag.count, 10);
    cobj.put(ComObject.Tag.method, "echo");
    DatabaseSocketClient client = new DatabaseSocketClient();
    response = client.do_send(null, cobj.serialize(), "localhost:9010");
    retObj = new ComObject(response);
    assertEquals((int) retObj.getInt(ComObject.Tag.count), 10);

    DatabaseSocketClient.shutdown();

    thread.interrupt();
    server.shutdown();
  }

}
