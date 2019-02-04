/* © 2019 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.controller;

import com.sonicbase.cli.Cli;
import com.sonicbase.common.Config;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.net.URLDecoder;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.sonicbase.cli.BenchHandler.USER_DIR_STR;
import static com.sonicbase.cli.ClusterHandler.INSTALL_DIRECTORY_STR;

public class ControllerHandler extends AbstractHandler {

  private static Logger logger = LoggerFactory.getLogger(ControllerHandler.class);


  private AtomicInteger count = new AtomicInteger();

  private ConcurrentMap<String, Config> configs = new ConcurrentHashMap<>();

  public ControllerHandler() {
  }

  public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    //new Exception().printStackTrace();
    String uri = request.getRequestURI();
    logger.info("http request: uri=" + uri + "?" + request.getQueryString());
    //InputStream in = HttpServer.class.getResourceAsStream(uri);
    try {
      if (uri.startsWith("/start-server")) {
        String cluster = request.getParameter("cluster");
        int shard = Integer.parseInt(request.getParameter("shard"));
        int replica = Integer.parseInt(request.getParameter("replica"));

        Config config = configs.computeIfAbsent(cluster, k -> Cli.getConfig(cluster));

        final String installDir = Cli.resolvePath(config.getString(INSTALL_DIRECTORY_STR));
        List<Config.Shard> shards = config.getShards();

        final List<Config.Replica> replicaObj = shards.get(shard).getReplicas();
        Config.Replica currReplica = replicaObj.get(replica);

        String maxHeap = request.getParameter("maxHeap");
        Integer port = currReplica.getInt("port");
        if (port == null) {
          port = 9010;
        }
        String address = currReplica.getString("privateAddress");

        String searchHome = new File(System.getProperty(USER_DIR_STR)).getAbsolutePath();

        System.out.println("Server start command: bin/start-db-server-task.bat " + address + " " + port + " " + maxHeap + " " + searchHome + " " + cluster + " enable");
        ProcessBuilder builder = new ProcessBuilder().command("bin/start-db-server-task.bat", address, String.valueOf(port),
            maxHeap, searchHome, cluster, "enable");
        Process p = builder.start();
        StringBuilder sbuilder = new StringBuilder();
        InputStream procIn = p.getInputStream();
        while (true) {
          int b = procIn.read();
          if (b == -1) {
            break;
          }
          sbuilder.append(String.valueOf((char) b));
        }
        p.waitFor();

        if (p.exitValue() != 0) {
          System.out.println(sbuilder.toString());
        }

        response.setContentType("application/json; charset=utf-8");
        response.setStatus(HttpServletResponse.SC_OK);
        response.getOutputStream().flush();
        System.out.println("Started server: shard=" + shard + ", replica=" + replica);
      }
      else if (uri.startsWith("/start-bench-server")) {
        int port = Integer.parseInt(request.getParameter("port"));

        String searchHome = new File(System.getProperty(USER_DIR_STR)).getAbsolutePath();

        System.out.println("start-bench-server.bat " + port + " " + searchHome);
        ProcessBuilder builder = new ProcessBuilder().command("bin/start-bench-server-task.bat", String.valueOf(port), searchHome);
        Process p = builder.start();
        StringBuilder sbuilder = new StringBuilder();
        InputStream procIn = p.getInputStream();
        while (true) {
          int b = procIn.read();
          if (b == -1) {
            break;
          }
          sbuilder.append(String.valueOf((char) b));
        }
        p.waitFor();

        if (p.exitValue() != 0) {
          System.out.println(sbuilder.toString());
        }

        response.setContentType("application/json; charset=utf-8");
        response.setStatus(HttpServletResponse.SC_OK);
        response.getOutputStream().flush();
        System.out.println("Started bench server: port=" + port);
      }
      else if (uri.startsWith("/get-mem-total")) {
        ProcessBuilder builder = new ProcessBuilder().command("bin/get-mem-total.bat");
        Process p = builder.start();
        StringBuilder sbuilder = new StringBuilder();
        InputStream procIn = p.getInputStream();
        while (true) {
          int b = procIn.read();
          if (b == -1) {
            break;
          }
          sbuilder.append(String.valueOf((char) b));
        }
        response.setContentType("application/json; charset=utf-8");
        response.setStatus(HttpServletResponse.SC_OK);
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(response.getOutputStream()))) {
          writer.write(sbuilder.toString());
        }
        System.out.println("Got total memory: total=" + sbuilder.toString());
      }
      else if (uri.startsWith("/stop-server")) {
        System.out.println("pwd=" + System.getProperty("user.dir"));
        int port = Integer.parseInt(request.getParameter("port"));
        ProcessBuilder builder = new ProcessBuilder().command("bin/kill-server.bat ", String.valueOf(port));
        Process p = builder.start();
        StringBuilder sbuilder = new StringBuilder();
        InputStream procIn = p.getInputStream();
        while (true) {
          int b = procIn.read();
          if (b == -1) {
            break;
          }
          sbuilder.append(String.valueOf((char) b));
        }
        response.setContentType("application/json; charset=utf-8");
        response.setStatus(HttpServletResponse.SC_OK);
        response.getOutputStream().flush();
        System.out.println("Stopped server");
      }
      else if (uri.startsWith("/purge-server")) {
        String dataDir = URLDecoder.decode(request.getParameter("dataDir"));
        if (dataDir == null || dataDir.isEmpty()) {
          System.out.println("Invalid dataDir: isEmpty");
          response.setContentType("application/json; charset=utf-8");
          response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
          response.getOutputStream().flush();
          return;
        }

        dataDir = dataDir.replaceAll("/", "\\\\");

        ProcessBuilder builder = new ProcessBuilder().command("bin/purge-data.bat ", dataDir);
        Process p = builder.start();
        StringBuilder sbuilder = new StringBuilder();
        InputStream procIn = p.getInputStream();
        while (true) {
          int b = procIn.read();
          if (b == -1) {
            break;
          }
          sbuilder.append(String.valueOf((char) b));
        }
        response.setContentType("application/json; charset=utf-8");
        response.setStatus(HttpServletResponse.SC_OK);
        response.getOutputStream().flush();
        System.out.println("Purged server: dataDir=" + dataDir);
      }
    }
    catch (Exception e) {
      e.printStackTrace();
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      IOUtils.write(ExceptionUtils.getStackTrace(e), response.getOutputStream(), "utf-8");

    }
  }

  public void shutdown() {

  }
}
