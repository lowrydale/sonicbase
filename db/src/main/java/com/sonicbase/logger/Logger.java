package com.sonicbase.logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.common.ThreadUtil;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.util.DateUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.spi.ThrowableInformation;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class Logger extends AppenderSkeleton {
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(Logger.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final int CONNECTION_COUNT = 10;
  private static final int THREAD_COUNT = 8;
  private static String[] servers;
  private static ThreadPoolExecutor executor;
  private static ArrayBlockingQueue<LoggingEvent> queue = new ArrayBlockingQueue<>(100_000);
  private static AtomicLong callCount = new AtomicLong();
  private static boolean shutdown;
  private static ConcurrentHashMap<String, Server> pools = new ConcurrentHashMap<>();
  private static ConcurrentLinkedQueue<Server> deadServers = new ConcurrentLinkedQueue<>();
  private static Thread fixerThread;
  private static int shard;
  private static int replica;
  private static ConcurrentHashMap<String, Type> types = new ConcurrentHashMap<>();
  private static AtomicLong countLogged;
  private static String cluster;

  public Logger() {
  }

  private static class Connection {
    private Socket socket;
    private DataOutputStream out;
  }

  private static class Server {
    public String host;
    private boolean dead;
    private ArrayBlockingQueue<Connection> pool;
  }

  private static class Type {
    private boolean isNumeric;
    private String originalMessage;
  }

  private static class ConnectionFixer implements Runnable {

    @Override
    public void run() {
      while (!shutdown) {
        try {
          Server server = deadServers.poll();
          if (server == null) {
            Thread.sleep(100);
            continue;
          }
          fixConnection(server);
          Thread.sleep(1_000);
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
        catch (Exception e) {
          logger.error("Error fixing connections", e);
        }
      }
    }

    private void fixConnection(Server server) {
      List<Connection> connections = new ArrayList<>();
      server.pool.drainTo(connections);
      for (Connection connection : connections) {
        try {
          connection.out.close();
          connection.socket.close();
        }
        catch (Exception e) {
        }
      }
      try {
        final CountDownLatch latch = new CountDownLatch(1);

        Thread thread = new Thread(() -> {
          try {
            for (int i = 0; i < CONNECTION_COUNT; i++) {
              Connection connection = new Connection();
              String[] parts = server.host.split(":");
              connection.socket = new Socket(parts[0], Integer.parseInt(parts[1]));
              connection.out = new DataOutputStream(new BufferedOutputStream(connection.socket.getOutputStream()));
              server.pool.put(connection);
              latch.countDown();
            }
          }
          catch (Exception e) {
            logger.error("Error fixing connection: host={}", server.host);
          }
        }, "SonicBase Logger Reconnect Thread");
        thread.start();
        if (latch.await(20_000, TimeUnit.MILLISECONDS)) {
          server.dead = false;
        }
        else {
          deadServers.add(server);
        }
      }
      catch (Exception e) {
        logger.error("Error fixing connection: host={}", server.host, e);
      }
    }
  }

  public static void init(String cluster, int shard, int replica, AtomicLong count, String serversStr) {
    try {
      Logger.countLogged = count;
      Logger.cluster = cluster;
      Logger.shard = shard;
      Logger.replica = replica;
      if (serversStr == null) {
        return;
      }
      if (servers != null) {
        return;
      }
      shutdown = false;
      executor = ThreadUtil.createExecutor(THREAD_COUNT, "SonicBase Logger Thread");
      servers = serversStr.split(",");

      fixerThread = ThreadUtil.createThread(new ConnectionFixer(), "SonicBase Logger Connection Fixer");
      fixerThread.start();

      for (int i = 0; i < servers.length; i++) {
        String[] parts = servers[i].split(":");

        ArrayBlockingQueue<Connection> pool = new ArrayBlockingQueue<>(CONNECTION_COUNT * 10);
        Server server = new Server();
        server.host = servers[i];
        server.pool = pool;
        pools.put(servers[i], server);

        for (int j = 0; j < CONNECTION_COUNT; j++) {
          Connection connection = new Connection();
          connection.socket = new Socket(parts[0], Integer.parseInt(parts[1]));
          connection.out = new DataOutputStream(new BufferedOutputStream(connection.socket.getOutputStream()));
          pool.put(connection);
        }
      }
      for (int i = 0; i < THREAD_COUNT; i++) {
        executor.submit(() -> {
          while (!shutdown) {
            try {
              List<LoggingEvent> events = new ArrayList<>();
              if (0 == queue.drainTo(events, 100)) {
                Thread.sleep(50);
              }
              else {
                int deadCount = 0;
                while (true) {
                  String serverStr = servers[(int) (callCount.incrementAndGet() % servers.length)];
                  Server server = pools.get(serverStr);
                  if (server.dead) {
                    deadCount++;
                    if (deadCount >= servers.length) {
                      Thread.sleep(1_000);
                    }
                    continue;
                  }
                  Connection connection = server.pool.poll();
                  try {
                    for (LoggingEvent event : events) {
                      processEvent(connection, event);
                    }
                    connection.out.flush();
                    server.pool.put(connection);
                    break;
                  }
                  catch (Exception e) {
                    e.printStackTrace();
                    try {
                      connection.out.close();
                      connection.socket.close();
                    }
                    catch (Exception e1) {
                    }

                    server.dead = true;
                    deadServers.add(server);
                  }
                }
              }
            }
            catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              break;
            }
            catch (Exception e) {
              logger.error("Error logging events", e);
            }
          }
        });
      }
    }
    catch (Exception e) {
      logger.error("Error connecting to logstash servers");
    }
  }

  @Override
  protected void append(LoggingEvent loggingEvent) {

    if (loggingEvent.getMessage().equals("_sonicbase_shutdown_")) {
      close();
      return;
    }

    if (servers == null) {
      return;
    }

    try {
      queue.put(loggingEvent);
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new DatabaseException(e);
    }
  }

  private static void processEvent(Connection connection, LoggingEvent loggingEvent) {
    ObjectNode json = OBJECT_MAPPER.createObjectNode();

    String msg = loggingEvent.getRenderedMessage();
    Level logLevel = loggingEvent.getLevel();
    String loggerName = loggingEvent.getLoggerName();
    String threadName = loggingEvent.getThreadName();
    long timeStamp = loggingEvent.getTimeStamp();


    ThrowableInformation throwableInfo = loggingEvent.getThrowableInformation();
    if (throwableInfo != null) {
      Throwable throwable = throwableInfo.getThrowable();
      String stackTrace = ExceptionUtils.getFullStackTrace(throwable);
      json.put("stackTrace", stackTrace);
    }

    json.put("class", loggerName);
    json.put("message", msg);
    json.put("logLevel", logLevel.toString());
    json.put("threadName", threadName);
    Calendar cal = Calendar.getInstance();
    cal.setTime(new java.sql.Date(timeStamp));

    json.put("timeStamp", DateUtils.toDbString(cal));
    json.put("this.cluster", cluster);
    json.put("this.shard", shard);
    json.put("this.replica", replica);
    try {
      parseMessage(connection, msg, json, msg);
      try {
        String body = json.toString();
        connection.out.writeBytes( body + "\n");
        countLogged.incrementAndGet();
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }

  protected static void parseMessage(Connection connection, String origMsg, ObjectNode json, String msg) throws IOException {
    int pos = msg.indexOf("=");
    while (pos != -1) {
      int pos2 = msg.lastIndexOf(' ', pos);
      String key = msg.substring(pos2 + 1, pos);

      boolean quote = msg.charAt(pos + 1) == '\"';
      boolean brace = msg.charAt(pos + 1) == '{';
      boolean bracket = msg.charAt(pos + 1) == '[';
      if (quote || brace || bracket) {
        int pos3 = msg.indexOf(quote ? '\"' : brace ? '}' : ']', pos + 2);
        if (pos3 != -1) {
          if (msg.charAt(pos3 - 1) == ',') {
            putValue(connection, origMsg, json, key, msg.substring(pos + 1, pos3));
          }
          else {
            putValue(connection, origMsg, json, key, msg.substring(pos + 1, pos3 + 1));
          }
          pos = msg.indexOf("=", pos3 + 1);
        }
        else {
          putValue(connection, origMsg, json, key, msg.substring(pos));
          pos = -1;
        }
      }
      else {
        int pos3 = msg.indexOf("=", pos + 1);
        if (pos3 != -1) {
          int pos4 = msg.lastIndexOf(' ', pos3);
          if (msg.charAt(pos4 - 1) == ',') {
            putValue(connection, origMsg, json, key, msg.substring(pos + 1, pos4 - 1));
          }
          else {
            putValue(connection, origMsg, json, key, msg.substring(pos + 1, pos4));
          }
          pos = pos3;
        }
        else {
          putValue(connection, origMsg, json, key, msg.substring(pos + 1));
          pos = -1;
        }
      }
    }
  }

  private static void putValue(Connection connection, String origMsg, ObjectNode json, String key, String value) throws IOException {
    Type type = types.computeIfAbsent(key, (k) -> {
      ObjectNode node = OBJECT_MAPPER.createObjectNode();

      try {
        Double.parseDouble(value);
        Type ret = new Type();
        ret.isNumeric = true;
        ret.originalMessage = origMsg;

        node.put("message", "registering field for first time: field=" + key + ", isNumeric=true");
        node.put("isNumeric", true);
        node.put("field", key);
        node.put("currentMessage", origMsg);
        node.put("logLevel", "INFO");
        node.put("this.shard", shard);
        node.put("this.replica", replica);
        node.put("value", value);

        if (connection != null) {
          connection.out.writeBytes(node.toString() + "\n");
        }
        return ret;
      }
      catch (Exception e) {
        Type ret = new Type();
        ret.isNumeric = false;
        ret.originalMessage = origMsg;

        node.put("message", "registering field for first time: field=" + key + ", isNumeric=false");
        node.put("isNumeric", false);
        node.put("field", key);
        node.put("currentMessage", origMsg);
        node.put("logLevel", "INFO");
        node.put("this.shard", shard);
        node.put("this.replica", replica);
        node.put("value", value);

        try {
          if (connection != null) {
            connection.out.writeBytes(node.toString() + "\n");
          }
        }
        catch (IOException e1) {
        }
        return ret;
      }
    });
    try {
      if (type.isNumeric) {
        json.put(key, Double.parseDouble(value));
      }
      else {
        json.put(key, value);
      }
    }
    catch (Exception e) {
      ObjectNode node = OBJECT_MAPPER.createObjectNode();
      node.put("message", "Logger dataType mismatch: expected=" + (type.isNumeric ? "number" : "string"));
      node.put("expected", (type.isNumeric ? "number" : "string"));
      node.put("field", key);
      node.put("originalMessage", type.originalMessage);
      node.put("currentMessage", origMsg);
      node.put("class", "Logger");
      node.put("logLevel", "ERROR");
      node.put("this.shard", shard);
      node.put("this.replica", replica);
      node.put("value", value);

      connection.out.writeBytes(node.toString() + "\n");
    }
  }

  @Override
  public void close() {
    shutdown = true;

    if (pools != null) {
      try {
        for (Server server : pools.values()) {
          List<Connection> connections = new ArrayList<>();
          ArrayBlockingQueue<Connection> pool = server.pool;
          pool.drainTo(connections);
          for (Connection connection : connections) {
            connection.out.flush();
            connection.out.close();
            connection.socket.close();
          }
        }
      }
      catch (IOException e) {
        logger.error("Error closing logstash socket");
      }
    }
    if (executor != null) {
      executor.shutdownNow();
    }
    pools.clear();
    servers = null;
    executor = null;

    logger.info("closing logstash logger");
  }


  @Override
  public boolean requiresLayout() {
    return false;
  }
}
