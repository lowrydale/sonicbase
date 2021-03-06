/* © 2019 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.controller;

import com.sonicbase.common.Config;
import com.sonicbase.common.ThreadUtil;
import com.sonicbase.query.DatabaseException;
import org.apache.commons.io.IOUtils;
import org.eclipse.jetty.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

public class HttpServer {
  private static Logger logger = LoggerFactory.getLogger(HttpServer.class);

  public static final String DAY_FORMAT_STR = "yyyy-MM-dd";
  public static final String TIME_FORMAT_STR = "yyyy-MM-dd'T'HH:mm:ss";

  private static Thread mainThread;
  private boolean shutdown;
  private ControllerHandler handler;


  public HttpServer() {
    handler = new ControllerHandler();
  }

  public static void main(String[] args) {
    HttpServer server = new HttpServer();
    String installDir = args[0];

    Config config = getConfig();
    Integer port = config.getInt("defaultControllerPort");
    if (port == null) {
      port = 8081;
    }

    server.startControllerServer(port, installDir);
  }

  public static Config getConfig() {
    try {
      InputStream in = Config.getConfigStream();

      String json = IOUtils.toString(in, "utf-8");
      return new Config(json);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
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

  public void startControllerServer(final int port, String installDir) {
    mainThread = ThreadUtil.createThread(() -> {
      try {
        Server server = new Server(port);
        handler.setInstallDir(installDir);
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

