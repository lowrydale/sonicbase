package com.sonicbase.misc;

import com.sonicbase.common.ComObject;
import com.sonicbase.research.socket.NettyServer;
import com.sonicbase.socket.DatabaseSocketClient;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.assertEquals;

public class TestSocket {


  public static void main(String[] args) throws InterruptedException {
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

    final AtomicInteger threadsActive = new AtomicInteger();
    final AtomicInteger count = new AtomicInteger();
    for (int i = 0; i < 2000; i++) {
      Thread clientThread = new Thread(new Runnable(){
        @Override
        public void run() {
          threadsActive.incrementAndGet();
          while (true) {
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
            if (count.incrementAndGet() % 100_000 == 0) {
              System.out.println("progress: count=" + count.get() + ", threadsActive=" + threadsActive.get() + ", socketCount=" + DatabaseSocketClient.getConnectionCount());
            }
          }
        }
      });
      clientThread.start();
    }
    Thread.sleep(2_000_000);
  }
}
