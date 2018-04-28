/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.server;

import com.sonicbase.common.ThreadUtil;
import com.sonicbase.query.DatabaseException;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.nio.SelectChannelConnector;

public class HttpServer {
  private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("com.sonicbase.logger");

  public static final String DAY_FORMAT_STR = "yyyy-MM-dd";
  public static final String TIME_FORMAT_STR = "yyyy-MM-dd'T'HH:mm:ss";

  private static Thread mainThread;
  private final DatabaseServer server;
  private boolean shutdown;
  private MonitorHandler handler;


  public HttpServer(DatabaseServer server) {

    this.server = server;
    handler = new MonitorHandler(server);
  }

  public void shutdown() {
    this.shutdown = true;

    try {
      if (mainThread != null) {
        mainThread.interrupt();
        mainThread.join();
        mainThread = null;
      }

      handler.shutdown();
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public void startMonitorServer(final String host, final int port) {
    mainThread = ThreadUtil.createThread(new Runnable(){
      @Override
      public void run() {
        try {
          Server server = new Server();

          SelectChannelConnector connector0 = new SelectChannelConnector();
          connector0.setPort(port);
          connector0.setHost(host);
          connector0.setMaxIdleTime(30000);
          connector0.setRequestHeaderSize(8192);

          server.setConnectors(new Connector[]{connector0});

          server.setHandler(handler);
          server.start();
          server.join();
        }
        catch (Exception e) {
          logger.error("Error in HttpServer thread", e);
        }
      }
    }, "SonicBase HttpServer Thread");
    mainThread.start();
  }



}
