package com.sonicbase.bench;

import org.apache.commons.cli.*;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by lowryda on 3/11/17.
 */
public class BenchServer {

  static BenchmarkCheck benchCheck = new BenchmarkCheck();
  static BenchmarkInsert benchInsert = new BenchmarkInsert();
  static BenchmarkDelete benchDelete = new BenchmarkDelete();
  static BenchmarkAWSInsertPublish benchAWSInsert = new BenchmarkAWSInsertPublish();
  static BenchmarkAWSDeletePublish benchAWSDelete = new BenchmarkAWSDeletePublish();
  static BenchmarkIdentityQuery benchIdentity = new BenchmarkIdentityQuery();
  static BenchmarkRangeQuery benchRange = new BenchmarkRangeQuery();
  static BenchmarkJoins benchJoins = new BenchmarkJoins();
  static AtomicLong insertBegin = new AtomicLong();
  static AtomicLong insertHighest = new AtomicLong();

  public static class HelloHandler extends AbstractHandler
  {
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
        throws IOException, ServletException
    {
      System.out.println("target=" + target);
      System.out.println("baseRequest, contextPath=" + baseRequest.getContextPath() +
          ", queryString=" + baseRequest.getQueryString());
      System.out.println("request, queryString=" + request.getQueryString() +
          ", servletPath=" + request.getServletPath() +
          ", requestURL=" + request.getRequestURL() +
          ", contextPath=" + request.getContextPath() +
          ", requestUri=" + request.getRequestURI());
      String uri = request.getRequestURI();
      String ret = "";
      if (uri.startsWith("/bench/start/insert")) {
        benchInsert.start(insertBegin, insertHighest, request.getParameter("cluster"),
            Integer.valueOf(request.getParameter("shard")),
            Long.valueOf(request.getParameter("offset")),
            Long.valueOf(request.getParameter("count")), false);
      }
      else if (uri.startsWith("/bench/start/delete")) {
        benchDelete.start(insertBegin, insertHighest, request.getParameter("cluster"), Integer.valueOf(request.getParameter("shardCount")),
            Integer.valueOf(request.getParameter("shard")),
            Long.valueOf(request.getParameter("offset")),
            Long.valueOf(request.getParameter("count")), false);
      }
      else if (uri.startsWith("/bench/stop/insert")) {
        benchInsert.stop();
      }
      else if (uri.startsWith("/bench/stop/Delete")) {
        benchDelete.stop();
      }
      else if (uri.startsWith("/bench/start/aws-insert")) {
        benchAWSInsert.start(insertBegin, insertHighest, request.getParameter("cluster"), Integer.valueOf(request.getParameter("shardCount")),
            Integer.valueOf(request.getParameter("shard")),
            Long.valueOf(request.getParameter("offset")),
            Long.valueOf(request.getParameter("count")), false);
      }
      else if (uri.startsWith("/bench/start/aws-delete")) {
        benchAWSDelete.start(insertBegin, insertHighest, request.getParameter("cluster"), Integer.valueOf(request.getParameter("shardCount")),
            Integer.valueOf(request.getParameter("shard")),
            Long.valueOf(request.getParameter("offset")),
            Long.valueOf(request.getParameter("count")), false);
      }
      else if (uri.startsWith("/bench/stop/aws-insert")) {
        benchAWSInsert.stop();
      }
      else if (uri.startsWith("/bench/stop/aws-delete")) {
        benchAWSDelete.stop();
      }
//      else if (uri.startsWith("/bench/start/kafka-insert")) {
//        benchKafkaInsert.start(insertBegin, insertHighest, request.getParameter("cluster"), Integer.valueOf(request.getParameter("shardCount")),
//            Integer.valueOf(request.getParameter("shard")),
//            Long.valueOf(request.getParameter("offset")),
//            Long.valueOf(request.getParameter("count")), false);
//      }
//      else if (uri.startsWith("/bench/start/kafka-delete")) {
//        benchKafkaDelete.start(insertBegin, insertHighest, request.getParameter("cluster"), Integer.valueOf(request.getParameter("shardCount")),
//            Integer.valueOf(request.getParameter("shard")),
//            Long.valueOf(request.getParameter("offset")),
//            Long.valueOf(request.getParameter("count")), false);
//      }
//      else if (uri.startsWith("/bench/stop/kafka-insert")) {
//        benchKafkaInsert.stop();
//      }
//      else if (uri.startsWith("/bench/stop/aws-delete")) {
//        benchKafkaDelete.stop();
//      }
      else if (uri.startsWith("/bench/start/check")) {
        benchCheck.start(insertBegin, insertHighest, request.getParameter("cluster"), Integer.valueOf(request.getParameter("shardCount")),
            Integer.valueOf(request.getParameter("shard")),
            Long.valueOf(request.getParameter("count")));
      }
      else if (uri.startsWith("/bench/stop/check")) {
        benchCheck.stop();
      }
      else if (uri.startsWith("/bench/start/identity")) {
        benchIdentity.start(request.getParameter("cluster"), Integer.valueOf(request.getParameter("shardCount")),
            Integer.valueOf(request.getParameter("shard")),
            Long.valueOf(request.getParameter("count")), request.getParameter("queryType"));
      }
      else if (uri.startsWith("/bench/stop/identity")) {
        benchIdentity.stop();
      }
      else if (uri.startsWith("/bench/start/range")) {
        benchRange.start(request.getParameter("cluster"), Integer.valueOf(request.getParameter("shardCount")),
            Integer.valueOf(request.getParameter("shard")),
            Long.valueOf(request.getParameter("count")));
      }
      else if (uri.startsWith("/bench/stop/range")) {
        benchRange.stop();
      }
      else if (uri.startsWith("/bench/start/joins")) {
        benchJoins.start(request.getParameter("cluster"), Integer.valueOf(request.getParameter("shardCount")),
            Integer.valueOf(request.getParameter("shard")),
            Long.valueOf(request.getParameter("count")), request.getParameter("queryType"));
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
      else if (uri.startsWith("/bench/stats/aws-insert")) {
        ret = benchAWSInsert.stats();
      }
      else if (uri.startsWith("/bench/stats/aws-delete")) {
        ret = benchAWSDelete.stats();
      }
//      else if (uri.startsWith("/bench/stats/kafka-insert")) {
//        ret = benchKafkaInsert.stats();
//      }
//      else if (uri.startsWith("/bench/stats/kafka-delete")) {
//        ret = benchKafkaDelete.stats();
//      }
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
      else if (uri.startsWith("/bench/resetStats/aws-insert")) {
        benchAWSInsert.resetStats();
      }
      else if (uri.startsWith("/bench/resetStats/aws-delete")) {
        benchAWSDelete.resetStats();
      }
//      else if (uri.startsWith("/bench/resetStats/kafka-insert")) {
//        benchKafkaInsert.resetStats();
//      }
//      else if (uri.startsWith("/bench/resetStats/kafka-delete")) {
//        benchKafkaDelete.resetStats();
//      }
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
