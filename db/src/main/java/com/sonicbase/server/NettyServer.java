package com.sonicbase.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.ComObject;
import com.sonicbase.common.Config;
import com.sonicbase.common.ThreadUtil;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.socket.DatabaseSocketClient;
import com.sonicbase.socket.Util;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.apache.commons.cli.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

@SuppressWarnings({"squid:S1172", "squid:S1168", "squid:S00107"})
// all methods called from method invoker must have cobj and replayed command parms
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class NettyServer {

  private static final String USER_HOME = System.getProperty("user.home");
  private static final String EXCEPTION_STR = "exception: ";
  private static final String MPORT_STR = "mport";
  private static final String CLUSTER_STR = "cluster";
  private static final String GCLOG_STR = "gclog";
  private static final String SHARD_STR = "shard";
  private static final String MHOST_STR = "mhost";
  private static final String REPLICA_STR = "replica";
  private static final String XMX_STR = "xmx";
  private static final String JSON_STR = ".json";
  private static final String STARTUP_ERROR_TXT_STR = "startupError.txt";
  private static final String ERROR_STARTING_SERVER_STR = "Error starting server";
  private static final String USER_DIR = System.getProperty("user.dir");
  private static final Logger logger = LoggerFactory.getLogger(NettyServer.class);

  public static final boolean ENABLE_COMPRESSION = false;
  private static final String UTF8_STR = "utf-8";
  private static final String PORT_STR = "port";
  private static final String HOST_STR = "host";
  public static final String DISABLE_STR = "disable";
  private final int threadCount;

  private final AtomicBoolean isRunning = new AtomicBoolean(false);
  private final AtomicBoolean isRecovered = new AtomicBoolean(false);
  private int port;
  private DatabaseServer databaseServer = null;
  private ChannelFuture f;
  private EventLoopGroup bossGroup;
  private EventLoopGroup workerGroup;
  private final AtomicLong totalRequestSize = new AtomicLong();
  private final AtomicLong totalResponseSize = new AtomicLong();
  private final AtomicLong totalTimeProcessing = new AtomicLong();

  private Thread nettyThread = null;
  private Thread serverThread = null;
  private boolean shutdown;

  public void shutdown() {
    try {
      this.shutdown = true;
      serverThread.interrupt();
      shutdownNetty();
      nettyThread.interrupt();

      serverThread.join();
      nettyThread.join();
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private void shutdownNetty() throws ExecutionException, InterruptedException {
    f.channel().close();
    if (f.channel().parent() != null) {
      f.channel().parent().close();
    }

    Future future1 = bossGroup.shutdownGracefully();
    Future future2 = workerGroup.shutdownGracefully();
    future1.get();
    future2.get();
  }

  public static class Request {
    private byte[] body;
    private long sequence0;
    private long sequence1;

    public byte[] getBody() {
      return body;
    }

    public void setBody(byte[] body) {
      this.body = body;
    }

    public long getSequence0() {
      return sequence0;
    }

    public long getSequence1() {
      return sequence1;
    }

    public void setSequence0(long sequence0) {
      this.sequence0 = sequence0;
    }

    public void setSequence1(long sequence1) {
      this.sequence1 = sequence1;
    }
  }

  public NettyServer() {
    this(Runtime.getRuntime().availableProcessors() * 128);
  }

  public NettyServer(int threadCount) {
    this.threadCount = threadCount;
  }

  public boolean isRunning() {
    return isRunning.get();
  }

  public boolean isRecovered() {
    return isRecovered.get();
  }

  public static class Response {
    private byte[] bytes;
    private Exception exception;

    public Response(Exception e) {
      this.exception = e;
    }

    public Response(byte[] bytes) {
      this.bytes = bytes;
    }

    public Exception getException() {
      return exception;
    }

    public byte[] getBytes() {
      return bytes;
    }
  }

  public enum ReadState {
    SIZE,
    BYTES,
    DLQ_SIZE,
    DLQ_BYTES
  }

  private static byte[] writeResponse(byte[] intBuff, OutputStream to, int requestCount, List<byte[]> retBytes,
                                      ByteArrayOutputStream out) throws IOException {
    long checksumValue;
    Util.writeRawLittleEndian32(requestCount, intBuff);
    out.write(intBuff);
    for (byte[] bytes : retBytes) {
      out.write(bytes);
    }
    out.close();
    byte[] body = out.toByteArray();
    int uncompressedSize = body.length;
    if (DatabaseSocketClient.COMPRESS) {
      if (DatabaseSocketClient.LZO_COMPRESSION) {
        LZ4Factory factory = LZ4Factory.fastestInstance();

        LZ4Compressor compressor = factory.fastCompressor();
        int maxCompressedLength = compressor.maxCompressedLength(body.length);
        byte[] compressed = new byte[maxCompressedLength];
        int compressedLength = compressor.compress(body, 0, body.length, compressed, 0, maxCompressedLength);
        body = new byte[compressedLength];
        System.arraycopy(compressed, 0, body, 0, compressedLength);
      }
      else {
        ByteArrayOutputStream bodyBytesOut = new ByteArrayOutputStream();
        GZIPOutputStream bodyOut = new GZIPOutputStream(bodyBytesOut);
        bodyOut.write(body);
        bodyOut.close();
        body = bodyBytesOut.toByteArray();
      }
      Util.writeRawLittleEndian32(body.length + 12, intBuff);
      to.write(intBuff);
    }
    else {
      Util.writeRawLittleEndian32(body.length + 8, intBuff);
      to.write(intBuff);
    }

    if (DatabaseSocketClient.COMPRESS) {
      Util.writeRawLittleEndian32(uncompressedSize, intBuff);
      to.write(intBuff);
    }

    checksumValue = 0;

    byte[] longBuff = new byte[8];
    Util.writeRawLittleEndian64(checksumValue, longBuff);
    to.write(longBuff);

    to.write(body);

    to.flush();
    return body;
  }

  private static Request deserializeRequest(InputStream from, byte[] intBuff) {
    try {
      from.read(intBuff);
      int bodyLen = Util.readRawLittleEndian32(intBuff);

      if (bodyLen > 1024 * 1024 * 1024) {
        throw new DatabaseException("Invalid inner body length: " + bodyLen);
      }
      byte[] body = new byte[bodyLen];
      if (bodyLen != 0) {
        from.read(body);
      }
      else {
        body = null;
      }

      Request request = new Request();
      request.body = body;
      return request;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  private final AtomicLong totalCallCount = new AtomicLong();
  private final AtomicLong callCount = new AtomicLong();
  private final AtomicLong lastLoggedSocketServerStats = new AtomicLong(System.currentTimeMillis());
  private final AtomicLong requestDuration = new AtomicLong();
  private final AtomicLong responseDuration = new AtomicLong();
  private final AtomicLong lastLogReset = new AtomicLong();
  private final AtomicLong timeLogging = new AtomicLong();
  private final AtomicLong handlerTime = new AtomicLong();

  class ServerHandler extends ChannelInboundHandlerAdapter {
    private final ByteBufAllocator alloc = PooledByteBufAllocator.DEFAULT;
    private int len;
    private int bodyLen;
    private ReadState readState = ReadState.SIZE;
    private ByteBuf destBuff = alloc.directBuffer(1024);
    private ByteBuf respBuffer = alloc.directBuffer(1024);
    private final byte[] intBuff = new byte[4];
    private final List<ByteBuf> buffers = new ArrayList<>();

    public void handlerAdded(ChannelHandlerContext ctx) {
      destBuff.clear();
      destBuff.retain();
      readState = ReadState.SIZE;
    }

    public void handlerRemoved(ChannelHandlerContext ctx) {
      destBuff.release();
      destBuff = null;
      respBuffer.release();
      respBuffer = null;
    }

    boolean oldWay = false;

    private byte[] readRequest(ByteBuf m) throws IOException {
      buffers.add(m);

      int readable = m.readableBytes();

      if (readState == ReadState.SIZE && readable >= 4) {
        readState = ReadState.BYTES;

        byte[] localIntBuff = new byte[4];
        m.resetReaderIndex();
        m.readBytes(localIntBuff);
        bodyLen = ((int) localIntBuff[0] & 0xff) |
            ((int) localIntBuff[1] & 0xff) << 8 |
            ((int) localIntBuff[2] & 0xff) << 16 |
            ((int) localIntBuff[3] & 0xff) << 24;
      }

      if (readState == ReadState.BYTES) {
        readable = 0;
        for (ByteBuf currBuf : buffers) {
          readable += currBuf.readableBytes();
        }
        if (readable >= len + bodyLen) {

          ReadBody readBody = new ReadBody().invoke();
          byte[] body = readBody.getBody();
          int origBodyLen = readBody.getOrigBodyLen();
          int offset = readBody.getOffset();

          return decompressReadAsNeeded(body, origBodyLen, offset);
        }
      }
      return null;
    }

    private byte[] decompressReadAsNeeded(byte[] body, int origBodyLen, int offset) throws IOException {
      if (DatabaseSocketClient.COMPRESS) {
        if (DatabaseSocketClient.LZO_COMPRESSION) {
          LZ4Factory factory = LZ4Factory.fastestInstance();

          LZ4FastDecompressor decompressor = factory.fastDecompressor();
          byte[] restored = new byte[origBodyLen];
          decompressor.decompress(body, offset, restored, 0, origBodyLen);
          body = restored;
        }
        else {
          GZIPInputStream bodyIn = new GZIPInputStream(new ByteArrayInputStream(body));
          body = new byte[origBodyLen];
          bodyIn.read(body);
        }
      }
      return body;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
      totalCallCount.incrementAndGet();

      int requestSize = 0;
      int responseSize = 0;
      ByteBuf m = null;
      try {
        m = (ByteBuf) msg;

        long requestBegin = System.nanoTime();
        byte[] body = readRequest(m);
        requestDuration.addAndGet(System.nanoTime() - requestBegin);
        if (body != null) {
          DoRead doRead = new DoRead(ctx, requestSize, responseSize, body).invoke();
          requestSize = doRead.getRequestSize();
          responseSize = doRead.getResponseSize();

          callCount.incrementAndGet();
          boolean shouldLog = false;
          synchronized (lastLoggedSocketServerStats) {
            if (System.currentTimeMillis() - lastLoggedSocketServerStats.get() > 5_000) {
              shouldLog = true;
              lastLoggedSocketServerStats.set(System.currentTimeMillis());
            }
          }
          if (shouldLog) {
            logger.info("SocketServer stats: callCount=" + totalCallCount.get() + ", requestDuration=" +
                ((double)requestDuration.get() / callCount.get() / 1000000d) + ", responseDuration=" +
                ((double)responseDuration.get() / callCount.get() / 1000000d) + ", avgRequestSize=" +
                (totalRequestSize.get() / callCount.get()) +
                ", avgResponseSize=" + (totalResponseSize.get() / callCount.get()) + ", avgTimeProcessing=" +
                ((double)totalTimeProcessing.get() / callCount.get() / 1000000d) +
                ", avgTimeLogging=" + ((double)timeLogging.get() / callCount.get() / 1000000d) +
                ", avgTimeHandling=" + ((double)handlerTime.get() / callCount.get() / 1000000d));
            synchronized (lastLogReset) {
              if (System.currentTimeMillis() - lastLogReset.get() > 4 * 60 * 1000) {
                requestDuration.set(0);
                responseDuration.set(0);
                callCount.set(0);
                totalRequestSize.set(0);
                totalResponseSize.set(0);
                totalTimeProcessing.set(0);
                timeLogging.set(0);
                handlerTime.set(0);
                lastLogReset.set(System.currentTimeMillis());
              }
            }
          }

          totalRequestSize.addAndGet(requestSize);
          totalResponseSize.addAndGet(responseSize);
        }
      }
      catch (Exception e) {
        logger.error("Error: " + e.getMessage(), e);
        readState = ReadState.SIZE;
      }
    }

    List<byte[]> doProcessRequests(List<Request> requests, AtomicLong timeLogging, AtomicLong handlerTime) {
      List<byte[]> finalRet = new ArrayList<>();
      try {
        List<Response> ret = processRequests(requests, timeLogging, handlerTime);
        for (Response response : ret) {
          if (response.getException() != null) {
            finalRet.add(returnException(EXCEPTION_STR + response.getException().getMessage(), response.getException()));
          }
          else {
            byte[] bytes = response.getBytes();
            int size = 0;
            if (bytes != null) {
              size = bytes.length;
            }
            byte[] localIntBuff = new byte[4];
            Util.writeRawLittleEndian32(size, localIntBuff);
            ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
            bytesOut.write(1);
            bytesOut.write(localIntBuff);
            if (bytes != null) {
              bytesOut.write(bytes);
            }
            bytesOut.close();
            finalRet.add(bytesOut.toByteArray());
          }
        }
      }
      catch (Exception e) {
        finalRet.clear();
        for (int i = 0; i < requests.size(); i++) {
          finalRet.add(returnException(EXCEPTION_STR + e.getMessage(), e));
        }
      }
      return finalRet;
    }

    private List<Response> processRequests(List<Request> requests, AtomicLong timeLogging, AtomicLong handlerTime) {
      List<Response> ret = new ArrayList<>();
      for (Request request : requests) {
        byte[] retBody = getDatabaseServer().invokeMethod(request.body, -1L, (short) -1L,
            false, true, timeLogging, handlerTime);
        Response response = new Response(retBody);
        ret.add(response);
      }
      return ret;
    }

    private byte[] returnException(String respStr, Throwable t) {
      try {
        StringBuilder errorResponse = new StringBuilder();
        errorResponse.append("Response: ").append(respStr).append("\n");
        StringWriter sWriter = new StringWriter();
        PrintWriter writer = new PrintWriter(sWriter);
        t.printStackTrace(writer);
        writer.close();
        errorResponse.append(sWriter.toString());
        byte[] retBytes = errorResponse.toString().getBytes(UTF8_STR);
        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        bytesOut.write(0);
        byte[] localIntBuff = new byte[4];
        Util.writeRawLittleEndian32(retBytes.length, localIntBuff);
        bytesOut.write(localIntBuff);
        bytesOut.write(retBytes);
        bytesOut.close();
        return bytesOut.toByteArray();
      }
      catch (IOException e) {
        throw new DatabaseException(e);
      }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
      //nothing to do
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) { // (4)
      // Close the connection when an exception is raised.
      logger.error("Netty Exception Caught", cause);

      ctx.close();
    }

    private class ReadBody {
      private byte[] body;
      private int origBodyLen;
      private int offset;

      public byte[] getBody() {
        return body;
      }

      int getOrigBodyLen() {
        return origBodyLen;
      }

      public int getOffset() {
        return offset;
      }

      public ReadBody invoke() {
        readState = ReadState.SIZE;

        body = new byte[bodyLen];
        int bodyOffset = 0;
        for (ByteBuf currBuf : buffers) {
          int localLen = currBuf.readableBytes();
          if (localLen > 0) {
            currBuf.readBytes(body, bodyOffset, localLen);
            bodyOffset += localLen;
          }
          currBuf.release();
        }
        buffers.clear();

        origBodyLen = -1;
        offset = 0;
        if (DatabaseSocketClient.COMPRESS) {
          origBodyLen = Util.readRawLittleEndian32(body);
          offset += 4;
        }

        Util.readRawLittleEndian64(body, offset); //sentChecksum
        if (DatabaseSocketClient.COMPRESS) {
          offset = 12;
        }
        else {
          offset = 8;
        }
        return this;
      }
    }

    private class DoRead {
      private ChannelHandlerContext ctx;
      private int requestSize;
      private int responseSize;
      private byte[] body;

      public DoRead(ChannelHandlerContext ctx, int requestSize, int responseSize, byte... body) {
        this.ctx = ctx;
        this.requestSize = requestSize;
        this.responseSize = responseSize;
        this.body = body;
      }

      public int getRequestSize() {
        return requestSize;
      }

      public int getResponseSize() {
        return responseSize;
      }

      public DoRead invoke() {
        try {
          requestSize = body.length;
          ByteArrayInputStream bytesIn = new ByteArrayInputStream(body);

          List<Request> requests = new ArrayList<>();
          int batchSize = getRequests(bytesIn, requests);

          ArrayList<byte[]> retBytes = new ArrayList<>();
          responseSize = doRead(ctx, batchSize, requests, retBytes);
        }
        catch (Exception t) {
          logger.error("Error processing request", t);
          readState = ReadState.SIZE;
          buffers.clear();
        }
        return this;
      }

      private int getRequests(ByteArrayInputStream bytesIn, List<Request> requests) throws IOException {
        int batchSize = 1;
        if (DatabaseSocketClient.ENABLE_BATCH) {
          bytesIn.read(intBuff);
          batchSize = Util.readRawLittleEndian32(intBuff);
        }

        for (int i = 0; i < batchSize; i++) {
          Request request = deserializeRequest(bytesIn, intBuff);
          requests.add(request);
        }
        return batchSize;
      }

      private int doRead(ChannelHandlerContext ctx, int batchSize, List<Request> requests,
                         ArrayList<byte[]> retBytes) throws IOException, InterruptedException {
        try {
          List<LogManager.LogRequest> logRequests = new ArrayList<>();

          long beginProcess = System.nanoTime();
          List<byte[]> ret = doProcessRequests(requests, timeLogging, handlerTime);
          retBytes.addAll(ret);
          totalTimeProcessing.addAndGet(System.nanoTime() - beginProcess);

          ByteArrayOutputStream out = new ByteArrayOutputStream();
          ByteArrayOutputStream to = new ByteArrayOutputStream();

          long responseBegin = System.nanoTime();
          writeResponse(intBuff, to, batchSize, retBytes, out);
          responseDuration.addAndGet(System.nanoTime() - responseBegin);

          respBuffer.clear();
          respBuffer.retain();

          byte[] bytes = to.toByteArray();
          respBuffer.writeBytes(bytes);
          respBuffer.retain();


          for (LogManager.LogRequest logRequest : logRequests) {
            logRequest.getLatch().await();
          }

          ctx.writeAndFlush(respBuffer);
          return bytes.length;
        }
        catch (Exception t) {
          handleReadError(ctx, batchSize, t);
        }
        return 0;
      }

      private void handleReadError(ChannelHandlerContext ctx, int batchSize, Exception t) throws IOException {
        ArrayList<byte[]> retBytes;
        logger.error("Transport error", t);

        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        retBytes = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
          byte[] currRetBytes = returnException(EXCEPTION_STR + t.getMessage(), t);
          retBytes.add(currRetBytes);
        }
        ByteArrayOutputStream to = new ByteArrayOutputStream();
        writeResponse(intBuff, to, batchSize, retBytes, bytesOut);

        respBuffer.clear();
        respBuffer.retain();

        respBuffer.writeBytes(to.toByteArray());
        respBuffer.retain();
        ctx.writeAndFlush(respBuffer);
      }
    }
  }

  public void setDatabaseServer(DatabaseServer databaseServer) {
    this.databaseServer = databaseServer;
  }

  public DatabaseServer getDatabaseServer() {
    return this.databaseServer;
  }

  class MyChannelInitializer extends ChannelInitializer<SocketChannel> { // (4)

    @Override
    protected void initChannel(SocketChannel socketChannel) {
      socketChannel.pipeline().addLast(new ServerHandler());
    }
  }

  public void run() {
    bossGroup = new NioEventLoopGroup(); // (1)
    workerGroup = new NioEventLoopGroup(threadCount);
    try {

      ServerBootstrap bootstrap = new ServerBootstrap(); // (2)
      logger.info("creating group");
      bootstrap.group(bossGroup, workerGroup)
          .channel(NioServerSocketChannel.class) // (3)
          .childHandler(new MyChannelInitializer())
          .option(ChannelOption.SO_BACKLOG, 128)          // (5)
          .option(ChannelOption.TCP_NODELAY, true)
          .option(ChannelOption.SO_LINGER, 1000)
          .childOption(ChannelOption.SO_KEEPALIVE, true); // (6)

      // Bind and start to accept incoming connections.
      logger.info("binding port");
      f = bootstrap.bind(port).sync(); // (7)

      // Wait until the server socket is closed.
      // In this example, this does not happen, but you can do that to gracefully
      // shut down your server.
      logger.info("bound port");
      f.channel().closeFuture().sync();
      logger.info("exiting netty server");
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new DatabaseException(e);
    }
  }

  public static void main(String[] args) {
    logger.info("Starting server: workingDir={}", USER_DIR);
    NettyServer server = new NettyServer();
    server.startServer(args);
  }

  public void startServer(String[] args) {
    for (String arg : args) {
      logger.info("arg: {}", arg);
    }

    try {
      CommandLine line = getCommandLineOptions(args);

      String portStr = line.getOptionValue(PORT_STR);
      String host = line.getOptionValue(HOST_STR);
      String cluster = line.getOptionValue(CLUSTER_STR);
      this.port = Integer.valueOf(portStr);
      String gclog = line.getOptionValue(GCLOG_STR);
      String xmx = line.getOptionValue(XMX_STR);
      String disableStr = line.getOptionValue(DISABLE_STR);
      boolean disable = DISABLE_STR.equals(disableStr);
      String configStr;

      InputStream in = null;
      File file = new File(USER_DIR, "config/config-" + cluster + ".yaml");
      if (file.exists()) {
        in = new FileInputStream(file);
      }

      if (in == null) {
        in = NettyServer.class.getResourceAsStream("/config/config-" + cluster + ".yaml");
      }
      try {
        configStr = IOUtils.toString(new BufferedInputStream(in), UTF8_STR);

        Config config = new Config(configStr);
        String role = line.getOptionValue("role");


        int localPort = Integer.parseInt(portStr);

        doStartServer(host, cluster, gclog, xmx, config, role, localPort, disable);

        nettyThread.join();
        logger.info("joined netty thread");
      }
      finally {
        in.close();
      }
    }
    catch (Exception e) {
      handleStartupException(e, ERROR_STARTING_SERVER_STR);

      throw new DatabaseException(e);
    }
    logger.info("exiting netty server");
  }

  private void doStartServer(String host, String cluster, String gclog, String xmx, Config config, String role, int localPort, boolean disable) {
    try {
      final DatabaseServer localDatabaseServer = new DatabaseServer();

      localDatabaseServer.setConfig(config, cluster, host, localPort, isRunning, isRecovered, gclog, xmx);
      localDatabaseServer.setRole(role);

      setDatabaseServer(localDatabaseServer);

      nettyThread = new Thread(() -> {
        try {
          logger.info("starting netty server");
          NettyServer.this.run();
        }
        catch (Exception e) {
          handleStartupException(e, "Error starting netty server");
        }
      });
      nettyThread.start();

      serverThread = new Thread(() -> {
        try {
          if (disable) {
            isRunning.set(false);
          }
          else {
            isRunning.set(true);
          }

          Thread thread = ThreadUtil.createThread(() -> {
            waitForServersToStart();

            localDatabaseServer.getSchemaManager().reconcileSchema();

          }, "SonicBase Reconcile Thread");
          thread.start();

          localDatabaseServer.recoverFromSnapshot();

          logger.info("applying queues");
          localDatabaseServer.getLogManager().applyLogs();

          localDatabaseServer.getDeleteManager().forceDeletes(null,  false);
          localDatabaseServer.getDeleteManager().start();

          localDatabaseServer.getMasterManager().startMasterMonitor();

          logger.info("running snapshot loop");
          localDatabaseServer.getSnapshotManager().runSnapshotLoop();

          isRecovered.set(true);
        }
        catch (Exception e) {
          logger.error(ERROR_STARTING_SERVER_STR, e);
          writeError(e);
        }
      });
      serverThread.start();
    }
    catch (Exception e) {
      writeError(e);
      logger.error("Error recovering snapshot", e);
      System.exit(1);
    }
  }

  private void handleStartupException(Exception e, String s) {
    File file = new File(USER_HOME, STARTUP_ERROR_TXT_STR);
    try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))) {
      writer.write(ExceptionUtils.getFullStackTrace(e));
    }
    catch (IOException e1) {
      logger.error(s, e1);
    }
    logger.error(s, e);
  }

  private CommandLine getCommandLineOptions(String[] args) throws ParseException {
    Options options = new Options();
    options.addOption("p", PORT_STR, true, PORT_STR);
    Option op = new Option("s", SHARD_STR, true, SHARD_STR);
    op.setRequired(false);
    options.addOption(op);
    options.addOption("h", HOST_STR, true, HOST_STR);
    options.addOption("m", MHOST_STR, true, MHOST_STR);
    options.addOption("n", MPORT_STR, true, MPORT_STR);
    op = new Option("r", REPLICA_STR, true, REPLICA_STR);
    op.setRequired(false);
    options.addOption(op);
    options.addOption("c", CLUSTER_STR, true, CLUSTER_STR);
    op = new Option("g", GCLOG_STR, true, GCLOG_STR);
    op.setRequired(false);
    options.addOption(op);
    op = new Option("x", XMX_STR, true, XMX_STR);
    op.setRequired(false);
    options.addOption(op);
    op = new Option("d", DISABLE_STR, true, DISABLE_STR);
    op.setRequired(false);
    options.addOption(op);

    CommandLineParser parser = new DefaultParser();
    // parse the command line arguments
    return parser.parse(options, args);
  }

  private void writeError(Exception e) {
    File file = new File(USER_HOME, STARTUP_ERROR_TXT_STR);
    try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))) {
      logger.error("Error", e);
      writer.write(ExceptionUtils.getFullStackTrace(e));
    }
    catch (IOException e1) {
      logger.error("Error", e1);
    }
  }

  private void waitForServersToStart() {
    int localThreadCount = databaseServer.getShardCount() * databaseServer.getReplicationFactor();
    ThreadPoolExecutor executor = new ThreadPoolExecutor(localThreadCount, localThreadCount, 10_000,
        TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
    try {
      List<Future> futures = new ArrayList<>();
      for (int i = 0; i < databaseServer.getShardCount(); i++) {
        for (int j = 0; j < databaseServer.getReplicationFactor(); j++) {
          final int shard = i;
          final int replica = j;
          futures.add(executor.submit((Callable) () -> {
            waitForServerToStart(shard, replica);
            return null;
          }));

        }
      }
      try {
        for (Future future : futures) {
          future.get();
        }

        if (databaseServer.getShard() == 0 && databaseServer.getReplica() == 0) {
          databaseServer.pushSchema();
        }
      }
      catch (Exception e) {
        logger.error("Error pushing schema", e);
      }
    }
    finally {
      executor.shutdownNow();
    }
  }

  private void waitForServerToStart(int shard, int replica) {
    long beginTime = System.currentTimeMillis();
    while (!shutdown) {
      try {
        if (databaseServer.getShard() == shard && databaseServer.getReplica() == replica) {
          break;
        }
        ComObject cobj = new ComObject(1);
        cobj.put(ComObject.Tag.METHOD, "DatabaseServer:healthCheckPriority");
        byte[] ret = databaseServer.getDatabaseClient().send(null, shard, replica, cobj,
            DatabaseClient.Replica.SPECIFIED);
        if (ret != null) {
          ComObject retObj = new ComObject(ret);
          String status = retObj.getString(ComObject.Tag.STATUS);
          if (status.equals("{\"status\" : \"ok\"}")) {
            break;
          }
        }
        Thread.sleep(1_000);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
      catch (Exception e) {
        logger.warn("Error checking if server is healthy: shard={}, replica={}", shard, replica, e);
      }
      if (System.currentTimeMillis() - beginTime > 2 * 60 * 1000) {
        logger.error("Server appears to be dead, skipping: shard={}, replica={}", shard, replica);
        break;
      }
    }
  }
}
