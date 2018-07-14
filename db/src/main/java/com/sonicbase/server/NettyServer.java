package com.sonicbase.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.ComObject;
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
import java.util.zip.CRC32;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;


/**
 * User: lowryda
 * Date: 12/25/13
 * Time: 4:55 PM
 */
public class NettyServer {

  private static Logger logger = LoggerFactory.getLogger(NettyServer.class);

  public static final boolean ENABLE_COMPRESSION = false;
  private static final String UTF8_STR = "utf-8";
  private static final String PORT_STR = "port";
  private static final String HOST_STR = "host";
  private final int threadCount;

  final AtomicBoolean isRunning = new AtomicBoolean(false);
  final AtomicBoolean isRecovered = new AtomicBoolean(false);
  private int port;
  private String cluster;
  private DatabaseServer databaseServer = null;
  private ChannelFuture f;
  private EventLoopGroup bossGroup;
  private EventLoopGroup workerGroup;
  private ServerBootstrap bootstrap;
  private AtomicLong totalRequestSize = new AtomicLong();
  private AtomicLong totalResponseSize = new AtomicLong();
  private AtomicLong totalTimeProcessing = new AtomicLong();

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
    public CountDownLatch latch = new CountDownLatch(1);
    private byte[] response;
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
    size,
    bytes,
    dlqSize,
    dlqBytes
  }

  public static byte[] writeResponse(byte[] intBuff, OutputStream to, byte[] body, int requestCount, ArrayList<byte[]> retBytes, ByteArrayOutputStream out) throws IOException {
    CRC32 checksum;
    long checksumValue;
    Util.writeRawLittleEndian32(requestCount, intBuff);
    out.write(intBuff);
    for (byte[] bytes : retBytes) {
      out.write(bytes);
    }
    out.close();
    body = out.toByteArray();
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

//        checksum = new CRC32();
//        checksum.update(body, 0, body.length);
    checksumValue = 0;//checksum.getValue();

    byte[] longBuff = new byte[8];
    Util.writeRawLittleEndian64(checksumValue, longBuff);
    to.write(longBuff);

    to.write(body);

    to.flush();
    return body;
  }

  public static Request deserializeRequest(InputStream from, byte[] intBuff) {
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

  private AtomicLong totalCallCount = new AtomicLong();
  private AtomicLong callCount = new AtomicLong();
  private AtomicLong lastLoggedSocketServerStats = new AtomicLong(System.currentTimeMillis());
  private AtomicLong requestDuration = new AtomicLong();
  private AtomicLong responseDuration = new AtomicLong();
  private AtomicLong lastLogReset = new AtomicLong();
  private AtomicLong timeLogging = new AtomicLong();
  private AtomicLong handlerTime = new AtomicLong();

  class ServerHandler extends ChannelInboundHandlerAdapter {
    private ByteBufAllocator alloc = PooledByteBufAllocator.DEFAULT;
    private int len;
    private int bodyLen;

    private ReadState readState = ReadState.size;
    private ByteBuf destBuff = alloc.directBuffer(1024);
    private ByteBuf respBuffer = alloc.directBuffer(1024);
    private byte[] intBuff = new byte[4];
    private List<ByteBuf> buffers = new ArrayList<>();

    public ServerHandler() {
    }

    public void handlerAdded(ChannelHandlerContext ctx) {

      destBuff.clear();
      destBuff.retain();
      readState = ReadState.size;
    }

    public void handlerRemoved(ChannelHandlerContext ctx) {
      destBuff.release();
      destBuff = null;
      respBuffer.release();
      respBuffer = null;
    }

    boolean oldWay = false;

    private byte[] readRequest(ByteBuf m) throws IOException {
      if (oldWay) {
        destBuff.writeBytes(m);
      }
      else {
        buffers.add(m);
      }
      int readable = 0;
      if (oldWay) {
        readable = destBuff.readableBytes();
      }
      else {
        readable = m.readableBytes();
      }
      if (readState == ReadState.size) {
        if (readable >= 4) {
          readState = ReadState.bytes;

          if (oldWay) {
            byte[] intBuff = new byte[4];
            destBuff.resetReaderIndex();
            destBuff.readBytes(intBuff);
            bodyLen = (((int) intBuff[0] & 0xff)) |
                (((int) intBuff[1] & 0xff) << 8) |
                (((int) intBuff[2] & 0xff) << 16) |
                (((int) intBuff[3] & 0xff) << 24);
          }
          else {
            byte[] intBuff = new byte[4];
            m.resetReaderIndex();
            m.readBytes(intBuff);
            bodyLen = (((int) intBuff[0] & 0xff)) |
                (((int) intBuff[1] & 0xff) << 8) |
                (((int) intBuff[2] & 0xff) << 16) |
                (((int) intBuff[3] & 0xff) << 24);
          }
        }
      }

      if (readState == ReadState.bytes) {
        if (oldWay) {
          readable = destBuff.readableBytes();
        }
        else {

          readable = 0;
          for (ByteBuf currBuf : buffers) {
            readable += currBuf.readableBytes();
          }
        }
        if (readable >= len + bodyLen) {
          readState = ReadState.size;

          byte[] body = new byte[bodyLen];
          if (oldWay) {
            destBuff.readBytes(body);
          }
          else {
            int bodyOffset = 0;
            for (ByteBuf currBuf : buffers) {
              int len = currBuf.readableBytes();
              if (len > 0) {
                currBuf.readBytes(body, bodyOffset, len);
                bodyOffset += len;
              }
              currBuf.release();
            }
            buffers.clear();
          }
          int origBodyLen = -1;
          int offset = 0;
          if (DatabaseSocketClient.COMPRESS) {
            origBodyLen = Util.readRawLittleEndian32(body);
            offset += 4;
          }

          long sentChecksum = Util.readRawLittleEndian64(body, offset);
          offset += 8;
          offset = 0;
          if (DatabaseSocketClient.COMPRESS) {
            offset = 12;
          }
          else {
            offset = 8;
          }
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
      }
      return null;
    }

    public void channelRead(ChannelHandlerContext ctx, Object msg) { // (2)

      long begin = System.currentTimeMillis();

      totalCallCount.incrementAndGet();

      int requestSize = 0;
      int responseSize = 0;

      ByteBuf m = null;
      String respStr = "";
      try {
        m = (ByteBuf) msg;

        long requestBegin = System.nanoTime();
        byte[] body = readRequest(m);
        requestDuration.addAndGet(System.nanoTime() - requestBegin);
        if (body != null) {
          try {

            requestSize = body.length;
            ByteArrayInputStream bytesIn = new ByteArrayInputStream(body);

            int batchSize = 1;
            if (DatabaseSocketClient.ENABLE_BATCH) {
              bytesIn.read(intBuff);
              batchSize = Util.readRawLittleEndian32(intBuff);
            }

            List<Request> requests = new ArrayList<Request>();
            for (int i = 0; i < batchSize; i++) {
              Request request = deserializeRequest(bytesIn, intBuff);
              requests.add(request);
            }

            ArrayList<byte[]> retBytes = new ArrayList<byte[]>();
            try {
              List<LogManager.LogRequest> logRequests = new ArrayList<>();

              long beginProcess = System.nanoTime();
              List<byte[]> ret = doProcessRequests(requests, timeLogging, handlerTime);
              for (byte[] bytes : ret) {
                retBytes.add(bytes);
              }
              totalTimeProcessing.addAndGet(System.nanoTime() - beginProcess);

              ByteArrayOutputStream out = new ByteArrayOutputStream();
              ByteArrayOutputStream to = new ByteArrayOutputStream();

              long responseBegin = System.nanoTime();
              writeResponse(intBuff, to, body, batchSize, retBytes, out);
              responseDuration.addAndGet(System.nanoTime() - responseBegin);

              respBuffer.clear();
              respBuffer.retain();

              byte[] bytes = to.toByteArray();
              respBuffer.writeBytes(bytes);
              respBuffer.retain();


              for (LogManager.LogRequest logRequest : logRequests) {
                logRequest.getLatch().await();
              }

              responseSize = bytes.length;
              ctx.writeAndFlush(respBuffer);
            }
            catch (Exception t) {
              logger.error("Transport error", t);

              ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
              retBytes = new ArrayList<byte[]>();
              for (int i = 0; i < batchSize; i++) {
                byte[] currRetBytes = returnException("exception: " + t.getMessage(), t);
                retBytes.add(currRetBytes);
              }
              ByteArrayOutputStream to = new ByteArrayOutputStream();
              writeResponse(intBuff, to, body, batchSize, retBytes, bytesOut);

              respBuffer.clear();
              respBuffer.retain();

              respBuffer.writeBytes(to.toByteArray());
              respBuffer.retain();
              ctx.writeAndFlush(respBuffer);
            }
          }
          catch (Exception t) {
            logger.error("Error processing request", t);
            t.printStackTrace();
            readState = ReadState.size;
            buffers.clear();
            if (oldWay) {
              destBuff.clear();
            }
          }

          if (oldWay) {
            destBuff.clear();
          }

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
                (requestDuration.get() / callCount.get() / 1000000d) + ", responseDuration=" +
                (responseDuration.get() / callCount.get() / 1000000d) + ", avgRequestSize=" + (totalRequestSize.get() / callCount.get()) +
                ", avgResponseSize=" + (totalResponseSize.get() / callCount.get()) + ", avgTimeProcessing=" +
                (totalTimeProcessing.get() / callCount.get() / 1000000d) +
                ", avgTimeLogging=" + (timeLogging.get() / callCount.get() / 1000000d) +
                ", avgTimeHandling=" + (handlerTime.get() / callCount.get() / 1000000d));
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
        readState = ReadState.size;
        e.printStackTrace();
        if (oldWay) {
          destBuff.clear();
          m.clear();
        }
      }
      finally {
        if (oldWay) {
          if (m != null) {
            m.release();
          }
        }
      }
      //       ((ByteBuf)msg).release(); // (3)
    }

    List<byte[]> doProcessRequests(List<Request> requests, AtomicLong timeLogging, AtomicLong handlerTime) {
      List<byte[]> finalRet = new ArrayList<>();
      try {
        List<Response> ret = processRequests(requests, timeLogging, handlerTime);
        for (Response response : ret) {
          if (response.getException() != null) {
            finalRet.add(returnException("exception: " + response.getException().getMessage(), response.getException()));
          }
          else {
            byte[] bytes = response.getBytes();
            int size = 0;
            if (bytes != null) {
              size = bytes.length;
            }
            byte[] intBuff = new byte[4];
            Util.writeRawLittleEndian32(size, intBuff);
            ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
            bytesOut.write(1);
            bytesOut.write(intBuff);
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
        for (Request request : requests) {
          finalRet.add(returnException("exception: " + e.getMessage(), e));
        }
      }
      return finalRet;
    }

    private List<Response> processRequests(List<Request> requests, AtomicLong timeLogging, AtomicLong handlerTime) throws IOException {
      List<Response> ret = new ArrayList<>();
      if (requests.size() > 1) {
        ;
      }
      for (Request request : requests) {
        byte[] retBody = getDatabaseServer().invokeMethod(request.body, -1L, (short) -1L, false, true, timeLogging, handlerTime);
        Response response = new Response(retBody);
        ret.add(response);
      }
      return ret;
    }

    private byte[] processRequest(byte[] body) {
      return getDatabaseServer().invokeMethod(body, -1L, (short) -1L, false, true, timeLogging, handlerTime);
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
        byte[] intBuff = new byte[4];
        Util.writeRawLittleEndian32(retBytes.length, intBuff);
        bytesOut.write(intBuff);
        bytesOut.write(retBytes);
        bytesOut.close();
        return bytesOut.toByteArray();
      }
      catch (IOException e) {
        throw new DatabaseException(e);
      }
    }

    public void channelReadComplete(ChannelHandlerContext ctx) {
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) { // (4)
      // Close the connection when an exception is raised.
      logger.error("Netty Exception Caught", cause);

      ctx.close();
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
    protected void initChannel(SocketChannel socketChannel) throws Exception {
      socketChannel.pipeline().addLast(new ServerHandler());
    }
  }

  public void run() {
    bossGroup = new NioEventLoopGroup(); // (1)
    workerGroup = new NioEventLoopGroup(threadCount);
    try {

      bootstrap = new ServerBootstrap(); // (2)
      logger.info("creating group");
      bootstrap.group(bossGroup, workerGroup)
          .channel(NioServerSocketChannel.class) // (3)
          .childHandler(new MyChannelInitializer())
          .option(ChannelOption.SO_BACKLOG, 128)          // (5)
          .option(ChannelOption.TCP_NODELAY, true)
          .option(ChannelOption.SO_LINGER, 1000)
          .childOption(ChannelOption.SO_KEEPALIVE, true); // (6)
      ;
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
      throw new DatabaseException(e);
    }
  }

  public static void main(String[] args) {
    System.out.println("Starting server: workingDir=" + System.getProperty("user.dir"));
    NettyServer server = new NettyServer();
    server.startServer(args, null, false);
  }

  public void startServer(String[] args, String configPathPre, boolean skipLicense) {
    for (String arg : args) {
      System.out.println("arg: " + arg);
    }

    try {
      Options options = new Options();
      options.addOption(OptionBuilder.withArgName(PORT_STR).hasArg().create(PORT_STR));
      options.addOption(OptionBuilder.withArgName("shard").hasArg().create("shard"));
      options.addOption(OptionBuilder.withArgName(HOST_STR).hasArg().create(HOST_STR));
      options.addOption(OptionBuilder.withArgName("mhost").hasArg().create("mhost"));
      options.addOption(OptionBuilder.withArgName("mport").hasArg().create("mport"));
      options.addOption(OptionBuilder.withArgName("replica").hasArg().create("replica"));
      options.addOption(OptionBuilder.withArgName("cluster").hasArg().create("cluster"));
      options.addOption(OptionBuilder.withArgName("gclog").hasArg().create("gclog"));
      options.addOption(OptionBuilder.withArgName("xmx").hasArg().create("xmx"));

      CommandLineParser parser = new DefaultParser();
      // parse the command line arguments
      CommandLine line = parser.parse(options, args);


      String portStr = line.getOptionValue(PORT_STR);
      String host = line.getOptionValue(HOST_STR);
      String masterPort = line.getOptionValue("mport");
      String masterHost = line.getOptionValue("mhost");
      this.cluster = line.getOptionValue("cluster");
      this.port = Integer.valueOf(portStr);
      String gclog = line.getOptionValue("gclog");
      String xmx = line.getOptionValue("xmx");

      String configStr = null;
      InputStream in = NettyServer.class.getResourceAsStream("/config/config-" + cluster + ".json");
      if (in != null) {
        configStr = IOUtils.toString(new BufferedInputStream(in), "utf-8");
      }
      else {
        File file = new File(System.getProperty("user.dir"), "config/config-" + cluster + ".json");
        if (!file.exists()) {
          file = new File(System.getProperty("user.dir"), "db/src/main/resources/config/config-" + cluster + ".json");
        }
        configStr = IOUtils.toString(new BufferedInputStream(new FileInputStream(file)), "utf-8");
      }
      ObjectMapper mapper = new ObjectMapper();
      ObjectNode config = (ObjectNode) mapper.readTree(configStr);
      String role = line.getOptionValue("role");


      int port = Integer.valueOf(portStr);
      try {

        final DatabaseServer databaseServer = new DatabaseServer();

        databaseServer.setConfig(config, cluster, host, port, isRunning, isRecovered, gclog, xmx, skipLicense);
        databaseServer.setRole(role);

        setDatabaseServer(databaseServer);

        nettyThread = new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              logger.info("starting netty server");
              NettyServer.this.run();
            }
            catch (Exception e) {
              File file = new File(System.getProperty("user.home"), "startupError.txt");
              try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))) {
                e.printStackTrace();
                writer.write(ExceptionUtils.getFullStackTrace(e));
              }
              catch (IOException e1) {
                e1.printStackTrace();
              }
              logger.error("Error starting netty server", e);
            }
          }
        });
        nettyThread.start();

        serverThread = new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              isRunning.set(true);

              Thread thread = ThreadUtil.createThread(new Runnable(){
                @Override
                public void run() {
                  //databaseServer.setWaitingForServersToStart(true);
                  waitForServersToStart();
                  //databaseServer.setWaitingForServersToStart(false);

                  databaseServer.getSchemaManager().reconcileSchema();

                }
              }, "SonicBase Reconcile Thread");
              thread.start();

              databaseServer.recoverFromSnapshot();

              logger.info("applying queues");
              databaseServer.getLogManager().applyLogs();

              databaseServer.getDeleteManager().forceDeletes(null,  false);
              databaseServer.getDeleteManager().start();

              databaseServer.getMasterManager().startMasterMonitor();

              logger.info("running snapshot loop");
              databaseServer.getSnapshotManager().runSnapshotLoop();
//              databaseServer.getSnapshotManager().runSnapshotLoop();

              isRecovered.set(true);
            }
            catch (Exception e) {
              logger.error("Error starting server", e);
              File file = new File(System.getProperty("user.home"), "startupError.txt");
              try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))) {
                e.printStackTrace();
                writer.write(ExceptionUtils.getFullStackTrace(e));
              }
              catch (IOException e1) {
                e1.printStackTrace();
              }
            }
          }
        });
        serverThread.start();
      }
      catch (Exception e) {
        File file = new File(System.getProperty("user.home"), "startupError.txt");
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))) {
          e.printStackTrace();
          writer.write(ExceptionUtils.getFullStackTrace(e));
        }
        catch (IOException e1) {
          e1.printStackTrace();
        }
        if (logger == null) {
          System.out.println("Error starting server");
          e.printStackTrace();
        }
        else {
          logger.error("Error recovering snapshot", e);
        }
        System.exit(1);
      }


      nettyThread.join();
      logger.info("joined netty thread");
    }
    catch (Exception e) {
      File file = new File(System.getProperty("user.home"), "startupError.txt");
      try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))) {
        e.printStackTrace();
        writer.write(ExceptionUtils.getFullStackTrace(e));
      }
      catch (IOException e1) {
        e1.printStackTrace();
      }
      if (logger == null) {
        System.out.println("Error starting server");
        e.printStackTrace();
      }
      else {
        logger.error("Error starting server", e);
      }
      throw new DatabaseException(e);
    }
    logger.info("exiting netty server");
  }

  private void waitForServersToStart() {
    int threadCount = databaseServer.getShardCount() * databaseServer.getReplicationFactor();
    ThreadPoolExecutor executor = new ThreadPoolExecutor(threadCount, threadCount, 10_000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
    try {
      List<Future> futures = new ArrayList<>();
      for (int i = 0; i < databaseServer.getShardCount(); i++) {
        for (int j = 0; j < databaseServer.getReplicationFactor(); j++) {
          final int shard = i;
          final int replica = j;
          futures.add(executor.submit(new Callable() {
            @Override
            public Object call() throws Exception {
              long beginTime = System.currentTimeMillis();
              while (!shutdown) {
                try {
                  if (databaseServer.getShard() == shard && databaseServer.getReplica() == replica) {
                    break;
                  }
                  ComObject cobj = new ComObject();
                  cobj.put(ComObject.Tag.method, "DatabaseServer:healthCheckPriority");
                  byte[] ret = databaseServer.getDatabaseClient().send(null, shard, replica, cobj, DatabaseClient.Replica.specified);
                  if (ret != null) {
                    ComObject retObj = new ComObject(ret);
                    String status = retObj.getString(ComObject.Tag.status);
                    if (status.equals("{\"status\" : \"ok\"}")) {
                      break;
                    }
                  }
                  Thread.sleep(1_000);
                }
                catch (InterruptedException e) {
                  return null;
                }
                catch (Exception e) {
                  logger.error("Error checking if server is healthy: shard=" + shard + ", replica=" + replica);
                }
                if (System.currentTimeMillis() - beginTime > 2 * 60 * 1000) {
                  logger.error("Server appears to be dead, skipping: shard=" + shard + ", replica=" + replica);
                  break;
                }
              }
              return null;
            }
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
}
