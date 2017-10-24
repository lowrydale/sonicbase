package com.sonicbase.database;

import com.sonicbase.common.ComObject;
import com.sonicbase.research.socket.NettyServer;
import com.sonicbase.socket.DatabaseSocketClient;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertEquals;

/**
 * Created by lowryda on 8/14/17.
 */
public class TestSocket {

  @Test
  public void test() throws InterruptedException {
    Thread thread = new Thread(new Runnable(){
      @Override
      public void run() {
        NettyServer server = new NettyServer();
        String argsStr = "-host localhost -port 9010 -cluster 1-local -gclog gc.log -xmx 2G";
        String[] args = argsStr.split(" ");
        server.startServer(args, null, true);
      }
    });
    thread.start();
    Thread.sleep(5000);

    for (int i = 0; i < 20; i++) {
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
    }
  }

}
