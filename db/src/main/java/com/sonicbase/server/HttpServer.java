package com.sonicbase.server;

import com.sonicbase.common.ThreadUtil;
import com.sonicbase.query.DatabaseException;
import org.eclipse.jetty.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpServer {
  private static Logger logger = LoggerFactory.getLogger(HttpServer.class);

  public static final String DAY_FORMAT_STR = "yyyy-MM-dd";
  public static final String TIME_FORMAT_STR = "yyyy-MM-dd'T'HH:mm:ss";

  private static Thread mainThread;
  private final com.sonicbase.server.DatabaseServer server;
  private final ProServer proServer;
  private boolean shutdown;
  private MonitorHandler handler;


  public HttpServer(ProServer proServer, DatabaseServer server) {
    this.proServer = proServer;
    this.server = server;
    handler = new MonitorHandler(proServer, server);
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
    mainThread = ThreadUtil.createThread(() -> {
      try {
        Server server = new Server(port);
        server.setHandler(handler);

        server.start();
        server.join();
      }
      catch (Exception e) {
        logger.error("Error in HttpServer thread", e);
      }
    }, "SonicBase HttpServer Thread");
    mainThread.start();
  }



}
