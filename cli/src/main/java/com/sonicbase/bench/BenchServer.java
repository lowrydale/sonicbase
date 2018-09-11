package com.sonicbase.bench;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.common.Config;
import org.apache.commons.cli.*;
import org.apache.commons.io.IOUtils;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class BenchServer {
  public static final Logger logger = LoggerFactory.getLogger(BenchServer.class);

  static final BenchmarkCheck benchCheck = new BenchmarkCheck();
  static final BenchmarkInsert benchInsert = new BenchmarkInsert();
  static final BenchmarkDelete benchDelete = new BenchmarkDelete();
  static final BenchmarkIdentityQuery benchIdentity = new BenchmarkIdentityQuery();
  static final BenchmarkRangeQuery benchRange = new BenchmarkRangeQuery();
  static final BenchmarkJoins benchJoins = new BenchmarkJoins();
  static final AtomicLong insertBegin = new AtomicLong();
  static final AtomicLong insertHighest = new AtomicLong();
  public static final String CLUSTER_STR = "cluster";
  public static final String SHARD_STR = "shard";
  public static final String COUNT_STR = "count";
  public static final String SHARD_COUNT_STR = "shardCount";
  public static final String USER_DIR_STR = "user.dir";

  private static String getAddress(HttpServletRequest request) throws IOException {
    String cluster = request.getParameter(CLUSTER_STR);
    if (cluster == null) {
      return null;
    }
    logger.info("userDir={}", System.getProperty(USER_DIR_STR));
    File file = new File(System.getProperty(USER_DIR_STR), "config/config-" + cluster + ".yaml");
    if (!file.exists()) {
      file = new File(System.getProperty(USER_DIR_STR), "db/src/main/resources/config/config-" + cluster + ".yaml");
    }
    String configStr = IOUtils.toString(new BufferedInputStream(new FileInputStream(file)), "utf-8");
    Config config = new Config(configStr);
    List<Config.Shard> array = config.getShards();
    Config.Shard shard = array.get(0);
    List<Config.Replica> replicasArray = shard.getReplicas();
    String address = replicasArray.get(0).getString("publicAddress");
    if (config.getBoolean("clientIsPrivate")) {
      address = replicasArray.get(0).getString("privateAddress");
    }
    return address;
  }
  public static class HelloHandler extends AbstractHandler {
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
        throws IOException {
      logger.info("target={}", target);
      logger.info("baseRequest, contextPath={}, queryString={}", baseRequest.getContextPath(),
          baseRequest.getQueryString());
      logger.info("request, queryString={}, servletPath={}, requestURL={}, contextPath={}, requestUri={}",
          request.getQueryString(), request.getServletPath(), request.getRequestURL(),
          request.getContextPath(), request.getRequestURI());
      String uri = request.getRequestURI();
      String ret = "";
      if (uri.startsWith("/bench/start/insert")) {
        benchInsert.start(getAddress(request), insertBegin, insertHighest, request.getParameter(CLUSTER_STR),
            Integer.valueOf(request.getParameter(SHARD_STR)),
            Long.valueOf(request.getParameter("offset")),
            Long.valueOf(request.getParameter(COUNT_STR)), false);
      }
      else if (uri.startsWith("/bench/start/delete")) {
        benchDelete.start(getAddress(request), insertBegin, insertHighest, request.getParameter(CLUSTER_STR), Integer.valueOf(request.getParameter(SHARD_COUNT_STR)),
            Integer.valueOf(request.getParameter(SHARD_STR)),
            Long.valueOf(request.getParameter("offset")),
            Long.valueOf(request.getParameter(COUNT_STR)), false);
      }
      else if (uri.startsWith("/bench/stop/insert")) {
        benchInsert.stop();
      }
      else if (uri.startsWith("/bench/stop/Delete")) {
        benchDelete.stop();
      }
      else if (uri.startsWith("/bench/start/check")) {
        benchCheck.start(getAddress(request), insertBegin, insertHighest, request.getParameter(CLUSTER_STR), Integer.valueOf(request.getParameter(SHARD_COUNT_STR)),
            Integer.valueOf(request.getParameter(SHARD_STR)),
            Long.valueOf(request.getParameter(COUNT_STR)));
      }
      else if (uri.startsWith("/bench/stop/check")) {
        benchCheck.stop();
      }
      else if (uri.startsWith("/bench/start/identity")) {
        benchIdentity.start(getAddress(request), request.getParameter(CLUSTER_STR), Integer.valueOf(request.getParameter(SHARD_COUNT_STR)),
            Integer.valueOf(request.getParameter(SHARD_STR)),
            Long.valueOf(request.getParameter(COUNT_STR)), request.getParameter("queryType"));
      }
      else if (uri.startsWith("/bench/stop/identity")) {
        benchIdentity.stop();
      }
      else if (uri.startsWith("/bench/start/range")) {
        benchRange.start(getAddress(request), request.getParameter(CLUSTER_STR), Integer.valueOf(request.getParameter(SHARD_COUNT_STR)),
            Integer.valueOf(request.getParameter(SHARD_STR)),
            Long.valueOf(request.getParameter(COUNT_STR)));
      }
      else if (uri.startsWith("/bench/stop/range")) {
        benchRange.stop();
      }
      else if (uri.startsWith("/bench/start/joins")) {
        benchJoins.start(getAddress(request), request.getParameter(CLUSTER_STR), Integer.valueOf(request.getParameter(SHARD_COUNT_STR)),
            Integer.valueOf(request.getParameter(SHARD_STR)),
            Long.valueOf(request.getParameter(COUNT_STR)), request.getParameter("queryType"));
      }
      else if (uri.startsWith("/bench/stop/joins")) {
        benchJoins.stop();
      }
      else if (uri.startsWith("/bench/stats/check")) {
        ret = benchCheck.stats();
      }
      else if (uri.startsWith("/bench/stats/insert")) {
        ret = benchInsert.stats();
      }
      else if (uri.startsWith("/bench/stats/delete")) {
        ret = benchDelete.stats();
      }
      else if (uri.startsWith("/bench/stats/identity")) {
        ret = benchIdentity.stats();
      }
      else if (uri.startsWith("/bench/stats/range")) {
        ret = benchRange.stats();
      }
      else if (uri.startsWith("/bench/stats/joins")) {
        ret = benchJoins.stats();
      }
      else if (uri.startsWith("/bench/resetStats/insert")) {
        benchInsert.resetStats();
      }
      else if (uri.startsWith("/bench/resetStats/delete")) {
        benchDelete.resetStats();
      }
      else if (uri.startsWith("/bench/resetStats/identity")) {
        benchIdentity.resetStats();
      }
      else if (uri.startsWith("/bench/resetStats/range")) {
        benchRange.resetStats();
      }
      else if (uri.startsWith("/bench/resetStats/joins")) {
        benchJoins.resetStats();
      }
      else if (uri.startsWith("/bench/healthcheck")) {
        ret = "ok";
      }
      response.setContentType("text/html;charset=utf-8");
      response.setStatus(HttpServletResponse.SC_OK);
      baseRequest.setHandled(true);
      response.getWriter().println(ret);
    }
  }

  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("port").hasArg().create("port"));

    CommandLineParser parser = new DefaultParser();
    CommandLine line = parser.parse(options, args);

    String portStr = line.getOptionValue("port");

    Server server = new Server(Integer.valueOf(portStr));
    server.setHandler(new HelloHandler());

    server.start();
    server.join();
  }
}
