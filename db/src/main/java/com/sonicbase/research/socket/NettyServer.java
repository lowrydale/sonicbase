package com.sonicbase.research.socket;

import com.sonicbase.common.Logger;
import com.sonicbase.server.DatabaseServer;
import com.sonicbase.socket.DatabaseSocketClient;
import com.sonicbase.socket.Util;
import com.sonicbase.util.JsonDict;
import com.sonicbase.util.StreamUtils;
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

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.CRC32;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;


/**
 * User: lowryda
 * Date: 12/25/13
 * Time: 4:55 PM
 */
public class NettyServer {

  private static Logger logger;

  public static final boolean ENABLE_COMPRESSION = false;
  private static final String UTF8_STR = "utf-8";
  private static final String PORT_STR = "port";
  private static final String HOST_STR = "host";

  private boolean isRunning;
  private int port;
  private static String cluster;
  private DatabaseServer databaseServer = null;
  private RequestHandler dlqServer;
  private ChannelFuture f;
  private EventLoopGroup bossGroup;
  private EventLoopGroup workerGroup;

  interface RequestHandler {
    String handleCommand(String command, String jsonStr) throws InterruptedException;

    String handleCommandOld(String command, String jsonStr) throws InterruptedException;
  }

  public static class Request {
    private String command;
    private byte[] body;
    public CountDownLatch latch = new CountDownLatch(1);
    private byte[] response;

    public String getCommand() {
      return command;
    }

    public byte[] getBody() {
      return body;
    }

    public void setCommand(String command) {
      this.command = command;
    }

    public void setBody(byte[] body) {
      this.body = body;
    }
  }

  public NettyServer() {

  }

  public boolean isRunning() {
    return isRunning;
  }

  public enum ReadState {
    size,
    bytes,
    dlqSize,
    dlqBytes
  }

  public static byte[] sendResponse(byte[] intBuff, OutputStream to, byte[] body, int requestCount, ArrayList<byte[]> retBytes, ByteArrayOutputStream out) throws IOException {
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

        checksum = new CRC32();
        checksum.update(body, 0, body.length);
        checksumValue = checksum.getValue();

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
        int len = Util.readRawLittleEndian32(intBuff);
        from.read(intBuff);
        int bodyLen = Util.readRawLittleEndian32(intBuff);

        if (bodyLen > 1024 * 1024 * 1024) {
          throw new com.sonicbase.query.DatabaseException("Invalid inner body length: " + bodyLen);
        }
        if (len > 1024 * 1024 * 1024) {
          throw new com.sonicbase.query.DatabaseException("Invalid command length: " + len);
        }
        byte[] b = new byte[len];
        from.read(b);
        byte[] body = new byte[bodyLen];
        if (bodyLen != 0) {
          from.read(body);
        }
        else {
          body = null;
        }

        final String command = new String(b, 0, len, UTF8_STR);
        Request request = new Request();
        request.command = command;
        request.body = body;
        return request;
      }
      catch (IOException e) {
        throw new com.sonicbase.query.DatabaseException(e);
      }
   }

  class ServerHandler extends ChannelInboundHandlerAdapter {
    private ByteBufAllocator alloc = PooledByteBufAllocator.DEFAULT;
    private int len;
    private int bodyLen;

    private ReadState readState = ReadState.size;
    private ByteBuf destBuff = alloc.directBuffer(1024);
    private ByteBuf respBuffer = alloc.directBuffer(1024);
    private byte[] intBuff = new byte[4];
    private String command;

    public ServerHandler() {
    }

    public void handlerAdded(ChannelHandlerContext ctx) {

      destBuff.clear();
      destBuff.retain();

//      ByteBufAllocator alloc = PooledByteBufAllocator.DEFAULT;
//      destBuff = alloc.directBuffer(1024);
//      //destBuff = ctx.alloc().buffer(100, 1000000); // (1)
//      //respBuffer = ctx.alloc().buffer(100, 1000000);
//      respBuffer = alloc.directBuffer(1024);
      readState = ReadState.size;
    }

    public void handlerRemoved(ChannelHandlerContext ctx) {
      destBuff.release(); // (1)
      destBuff = null;
      respBuffer.release(); // (1)
      respBuffer = null;
    }

    //public void channelReadCompleted(ChannelHandlerContext ctx) { // (2)
    //  ctx.flush();
    //}



    public void channelRead(ChannelHandlerContext ctx, Object msg) { // (2)
      ByteBuf m = null;
      String respStr = "";
      try {
        if (!NettyServer.this.isRunning) {
          return;
        }
        m = (ByteBuf) msg;

        destBuff.writeBytes(m);
        int readable = destBuff.readableBytes();
        if (readState == ReadState.size) {
          //logger.info("Reading size");
          if (readable >= 4) {
            readState = ReadState.bytes;

            byte[] intBuff = new byte[4];
            destBuff.resetReaderIndex();
            destBuff.readBytes(intBuff);
            bodyLen = (((int) intBuff[0] & 0xff)) |
                (((int) intBuff[1] & 0xff) << 8) |
                (((int) intBuff[2] & 0xff) << 16) |
                (((int) intBuff[3] & 0xff) << 24);

//            if (len > 100000) {
//              byte[] other = new byte[readable - 4];
//              destBuff.getBytes(4, other);
//              logger.error("Invalid length: len=" + len + ", Other bytes=" + new String(other));
//              returnException(ctx, "Error", new Throwable("Invalid len=" + len));
//              readState = ReadState.size;
//              destBuff.clear();
//              return;
//  //            int index = destBuff.readerIndex();
//  //            destBuff.readerIndex(index - 4);
//            }
            //destBuff = ctx.alloc().buffer(len);
            //logger.info("Read size: len=" + len);

          }
        }

        if (readState == ReadState.bytes) {
          //logger.info("Reading bytes");
//          if (len > 1024 * 1024 * 1024) {
//            returnException(ctx, "Payload too big: len=" + len, new Throwable("Payload too big: len=" + len));
//            readState = ReadState.size;
//            destBuff.clear();
//          }
//          else {
          readable = destBuff.readableBytes();
          if (readable >= len + bodyLen) {
            readState = ReadState.size;

            try {
              byte[] body = new byte[bodyLen];
              destBuff.readBytes(body);

              int origBodyLen = -1;
              int offset = 0;
              if (DatabaseSocketClient.COMPRESS) {
                origBodyLen = Util.readRawLittleEndian32(body);
                offset += 4;
              }
              if (origBodyLen > 1024 * 1024 * 1024) {
                throw new Exception("Invalid orig body length: " + origBodyLen);
              }
              long sentChecksum = Util.readRawLittleEndian64(body, offset);
              offset += 8;

              byte[] newBody = null;
              if (DatabaseSocketClient.COMPRESS) {
                newBody = new byte[bodyLen - 12];
              }
              else {
                newBody = new byte[bodyLen - 8];
              }
              System.arraycopy(body, offset, newBody, 0, newBody.length);
              body = newBody;

              CRC32 checksum = new CRC32();
              checksum.update(body, 0, body.length);
              long checksumValue = checksum.getValue();
              //checksumValue = Arrays.hashCode(body);

              if (sentChecksum != checksumValue) {
                throw new Exception("Checksum mismatch");
              }
              //System.out.println("origBodyLen=" + origBodyLen + ", len=" + bodyLen);
              if (DatabaseSocketClient.COMPRESS) {
                if (DatabaseSocketClient.LZO_COMPRESSION) {
                  LZ4Factory factory = LZ4Factory.fastestInstance();

                  LZ4FastDecompressor decompressor = factory.fastDecompressor();
                  byte[] restored = new byte[origBodyLen];
                  decompressor.decompress(body, 0, restored, 0, origBodyLen);
                  body = restored;
                }
                else {
                  GZIPInputStream bodyIn = new GZIPInputStream(new ByteArrayInputStream(body));
                  body = new byte[origBodyLen];
                  bodyIn.read(body);
                }
              }

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
                List<DatabaseServer.LogRequest> logRequests = new ArrayList<>();
//                  for (BlockingServer.Request request : requests) {
//                    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
//                    DataOutputStream out = new DataOutputStream(bytesOut);
//                    byte[] buffer = request.command.getBytes("utf-8");
//
//                    out.writeInt(buffer.length);
//                    out.write(buffer);
//                    out.writeInt(request.body == null ? 0 : request.body.length);
//                    if (request.body != null && request.body.length != 0) {
//                      out.write(request.body);
//                    }
//
//                    out.close();
//
//                    DatabaseServer.LogRequest logRequest = new DatabaseServer.LogRequest();
//                    logRequest.setBuffer(bytesOut.toByteArray());
//                    NettyServer.this.logRequests[ ThreadLocalRandom.current().nextInt(0, NettyServer.this.logRequests.length)].put(logRequest);
//                    logRequests.add(logRequest);
//
////                    Queue.Request queueRequest = new Queue.Request(bytesOut.toByteArray());
////                    queue.push(queueRequest);
////                    queueRequests.add(queueRequest);
//                  }
//                {
//                  DatabaseServer.LogRequest logRequest = new DatabaseServer.LogRequest();
//                  logRequest.setBuffer(compressedBatch);
//                  NettyServer.this.logRequests[ThreadLocalRandom.current().nextInt(0, NettyServer.this.logRequests.length)].put(logRequest);
//                  logRequests.add(logRequest);
//                }


                if (true) {

                  List<byte[]> ret = doProcessRequests(requests);
                  for (byte[] bytes : ret) {
                    retBytes.add(bytes);
                  }
                }
                else {
                  for (Request request : requests) {
                    byte[] bytes = (byte[]) doProcessRequest(request);
                    retBytes.add(bytes);
                    //                    Future future = executor.submit(processRequest(request));
                    //                    futures.add(future);
                  }
                }


//                  for (Future future : futures) {
//                    byte[] currRetBytes = (byte[]) future.get();
//                    retBytes.add(currRetBytes);
//                  }


                ByteArrayOutputStream out = new ByteArrayOutputStream();
                ByteArrayOutputStream to = new ByteArrayOutputStream();

                sendResponse(intBuff, to, body, batchSize, retBytes, out);

                respBuffer.clear();
                respBuffer.retain();

                byte[] bytes = to.toByteArray();
                respBuffer.writeBytes(bytes);
                respBuffer.retain();


                for (DatabaseServer.LogRequest logRequest : logRequests) {
                  logRequest.getLatch().await();
                }

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
                sendResponse(intBuff, to, body, batchSize, retBytes, bytesOut);

                respBuffer.clear();
                respBuffer.retain();

                respBuffer.writeBytes(to.toByteArray());
                respBuffer.retain();
                ctx.writeAndFlush(respBuffer);
              }


              //logger.info("NettyServer handling command: " + command);
              //          System.out.println(command);
            }
            catch (Exception t) {
              logger.error("Error processing request", t);
              t.printStackTrace();
              readState = ReadState.size;
              destBuff.clear();
            }
            if (readState != ReadState.dlqSize) {
              destBuff.clear();
            }
          }
//          }
        }

        if (readState == ReadState.dlqSize) {
          logger.info("Reading dlq size");
          readable = destBuff.readableBytes();
          if (readable >= 4) {
            readState = ReadState.dlqBytes;
            final byte b1 = destBuff.readByte();
            final byte b2 = destBuff.readByte();
            final byte b3 = destBuff.readByte();
            final byte b4 = destBuff.readByte();
            len = (((int) b1 & 0xff)) |
                (((int) b2 & 0xff) << 8) |
                (((int) b3 & 0xff) << 16) |
                (((int) b4 & 0xff) << 24);

//            if (len > 100000) {
//              byte[] other = new byte[readable - 4];
//              destBuff.getBytes(4, other);
//              logger.error("Invalid length: len=" + len + ", Other bytes=" + new String(other));
//              readState = ReadState.size;
//              destBuff.clear();
//              return;
//  //            int index = destBuff.readerIndex();
//  //            destBuff.readerIndex(index - 4);
//            }
//            //destBuff = ctx.alloc().buffer(len);
            logger.info("Read dlq size: len=" + len);

          }
        }

        if (readState == ReadState.dlqBytes) {
          logger.info("Reading dlq bytes");
          if (len > 1024 * 1024 * 1024) {
            logger.error("DLQ payload too big: len=" + len);
            readState = ReadState.size;
            destBuff.clear();
          }
          else {
            readable = destBuff.readableBytes();
            if (readable >= len) {
              readState = ReadState.size;
              try {
                //          System.out.println("Read readable=" + readable + ", len=" + len);
                byte[] b = new byte[len];
                destBuff.readBytes(b);

                while (destBuff.isReadable()) {
                  destBuff.readByte();
                }

                String dlqMsg = new String(b, UTF8_STR);
                logger.info("Request dlq msg=" + dlqMsg);
                respStr = getDlqServer().handleCommandOld(command, dlqMsg);

                byte[] retBytes = respStr.getBytes(UTF8_STR);
                respBuffer.clear();
                respBuffer.retain();
                Util.writeRawLittleEndian32(retBytes.length, intBuff);
                respBuffer.writeBytes(intBuff);
                respBuffer.writeBytes(retBytes);
                ctx.writeAndFlush(respBuffer);
              }
              catch (Exception t) {
                logger.error("Error handling old request", t);
                readState = ReadState.size;
                destBuff.clear();
              }
              destBuff.clear();
            }
          }
        }

        //      ctx.flush(); // (2)
        //      Systeuuu.out.println("Wrote to client");
        //ctx.close();
      }
      catch (Exception e) {
        logger.error("Error: " + e.getMessage(), e);
        readState = ReadState.size;
        e.printStackTrace();
        destBuff.clear();
      }
      finally {
        if (m != null) {
          m.release();
        }
      }
      //       ((ByteBuf)msg).release(); // (3)
    }

    private void processError(String respStr, List<byte[]> buffers, Throwable t) throws IOException {
      logger.error("Error processing command", t);
      StringBuilder errorResponse = new StringBuilder();
      errorResponse.append("Response: ").append(respStr).append("\n");
      t.fillInStackTrace();
      StringWriter sWriter = new StringWriter();
      PrintWriter writer = new PrintWriter(sWriter);
      t.printStackTrace(writer);
      writer.close();
      errorResponse.append(sWriter.toString());
      //    for (StackTraceElement elem : t.getStackTrace()) {
      //      errorResponse.append(elem.toString()).append("\n");
      //    }
      byte[] retBytes = errorResponse.toString().getBytes(UTF8_STR);
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      bytesOut.write(0);

      //                  respBuffer.writeBoolean(false);
      Util.writeRawLittleEndian32(retBytes.length, intBuff);
      //                  respBuffer.writeBytes(intBuff);
      //                  respBuffer.writeBytes(retBytes);
      bytesOut.write(intBuff);
      bytesOut.write(retBytes);
      buffers.add(bytesOut.toByteArray());
    }

    byte[] doProcessRequest(Request request) {
      try {
        byte[] ret = processRequest(request.command, request.body);

        int size = 0;
        if (ret != null) {
          size = ret.length;
        }
        byte[] intBuff = new byte[4];
        Util.writeRawLittleEndian32(size, intBuff);
        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        bytesOut.write(1);
        bytesOut.write(intBuff);
        if (ret != null) {
          bytesOut.write(ret);
        }
        bytesOut.close();
        return bytesOut.toByteArray();
      }
      catch (Exception t) {
      //  System.out.println("Error processing request: error=" + t.getMessage());
      //  t.printStackTrace();
        return returnException("exception: " + t.getMessage(), t);
      }
    }

    List<byte[]> doProcessRequests(List<Request> requests) {
      List<byte[]> finalRet = new ArrayList<>();
      try {
        List<DatabaseServer.Response> ret = processRequests(requests);
        for (DatabaseServer.Response response : ret) {
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

    private List<DatabaseServer.Response> processRequests(List<Request> requests) throws IOException {
      List<DatabaseServer.Response> ret = new ArrayList<>();
      for (Request request : requests) {
        byte[] retBody = getDatabaseServer().handleCommand(request.command, request.body, false, true);
        DatabaseServer.Response response = new DatabaseServer.Response(retBody);
        ret.add(response);
      }
      return ret;
    }

    private byte[] processRequest(String command, byte[] body) {
      int pos = command.indexOf(':');
      String subSystem = "";
      String actualCommand = command;
      if (pos != -1) {
        subSystem = command.substring(0, pos);
        int jsonPos = command.indexOf("{");
        if (jsonPos != -1) {
          actualCommand = command.substring(0, jsonPos - 1);
        }
      }

      byte[] ret = null;

      if (subSystem.equals("DatabaseServer")) {
        ret = getDatabaseServer().handleCommand(actualCommand, body, false, true);
      }
      //              else if (subSystem.equals("IdMapServer")) {
      //                respStr = NettyServer.getIdMapServer().handleCommand(command, jsonStr);
      //              }


      return ret;
    }

    private byte[] returnException(String respStr, Throwable t) {
      try {
        StringBuilder errorResponse = new StringBuilder();
        errorResponse.append("Response: ").append(respStr).append("\n");
        //t.fillInStackTrace();
        StringWriter sWriter = new StringWriter();
        PrintWriter writer = new PrintWriter(sWriter);
        t.printStackTrace(writer);
        writer.close();
        errorResponse.append(sWriter.toString());
        //    for (StackTraceElement elem : t.getStackTrace()) {
        //      errorResponse.append(elem.toString()).append("\n");
        //    }
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
        throw new com.sonicbase.query.DatabaseException(e);
      }
    }

    public void channelReadComplete(ChannelHandlerContext ctx) {
      //ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) { // (4)
      // Close the connection when an exception is raised.
      if (cause.getMessage().contains("Connection reset")) {
        logger.errorLocalOnly("Netty Exception Caught", cause);
      }
      else {
        logger.error("Netty Exception Caught", cause);
      }
      ctx.close();
    }
  }

  public static byte[] compress(byte[] retBytes) throws IOException {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    GZIPOutputStream out = new GZIPOutputStream(bytesOut);
    out.write(retBytes);
    out.close();

    return bytesOut.toByteArray();
  }

  public static byte[] uncompress(byte[] b) throws IOException {
    GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(b));
    return StreamUtils.inputStreamToBytes(in);
  }

  public void setDatabaseServer(DatabaseServer databaseServer)  {
    this.databaseServer = databaseServer;
    logger = new Logger(databaseServer.getDatabaseClient());
    Logger.setIsClient(false);
//    File file = new File(databaseServer.getDataDir(), "queue/" + databaseServer.getShard() + "/" + databaseServer.getReplica());
//    queue = new Queue(file.getAbsolutePath(), "request-log", 0);
//    Thread logThread = new Thread(queue);
//    logThread.start();
//
//    for (int i = 0; i < logRequests.length; i++) {
//      logRequests[i] = new ArrayBlockingQueue<>(2000);
//      Thread thread = new Thread(new LogProcessor(i, logRequests[i], databaseServer.getDataDir(), databaseServer.getShard(), databaseServer.getReplica()));
//      thread.start();
//    }
//        File file = new File(databaseServer.getDataDir(), "queue/" + databaseServer.getShard()+ "/" + databaseServer.getReplica());
//        queue = new Queue(file.getAbsolutePath(), "request-log", 0);
//        Thread logThread = new Thread(queue);
//        logThread.start();
  }

  public DatabaseServer getDatabaseServer() {
    return this.databaseServer;
  }

  public RequestHandler getDlqServer() {
    return this.dlqServer;
  }

  public void setDlqServer(RequestHandler server) {
    this.dlqServer = server;
  }

  public void run() {
    EventLoopGroup bossGroup = new NioEventLoopGroup(); // (1)
    EventLoopGroup workerGroup = new NioEventLoopGroup(1024);
    try {

      ServerBootstrap b = new ServerBootstrap(); // (2)
      logger.info("creating group");
      b.group(bossGroup, workerGroup)
          .channel(NioServerSocketChannel.class) // (3)
          .childHandler(new ChannelInitializer<SocketChannel>() { // (4)
            @Override
            public void initChannel(SocketChannel ch) {
              ch.pipeline().addLast(new ServerHandler());
            }
          })
          .option(ChannelOption.SO_BACKLOG, 128)          // (5)
          .option(ChannelOption.TCP_NODELAY, true)
          .option(ChannelOption.SO_LINGER, 1000)
          .childOption(ChannelOption.SO_KEEPALIVE, true); // (6)
      ;
      // Bind and start to accept incoming connections.
      logger.info("binding port");
      ChannelFuture f = b.bind(port).sync(); // (7)

      // Wait until the server socket is closed.
      // In this example, this does not happen, but you can do that to gracefully
      // shut down your server.
      logger.info("bound port");
      f.channel().closeFuture().sync();
      logger.info("exiting netty server");
    }
    catch (InterruptedException e) {
      throw new com.sonicbase.query.DatabaseException(e);
    }
    finally {
      workerGroup.shutdownGracefully();
      bossGroup.shutdownGracefully();
    }
  }

  public static void main(String[] args) {
    System.out.println("Starting server: workingDir=" + System.getProperty("user.dir"));
    NettyServer server = new NettyServer();
    server.startServer(args, null, false);
  }

  public void startServer(String[] args, String configPathPre, boolean skipLicense) {
    for (String arg: args) {
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
        configStr = StreamUtils.inputStreamToString(new BufferedInputStream(in));
      }
      else {
        File file = new File(System.getProperty("user.dir"), "config/config-" + cluster + ".json");
        if (!file.exists()) {
          file = new File(System.getProperty("user.dir"), "db/src/main/resources/config/config-" + cluster + ".json");
        }
        configStr = StreamUtils.inputStreamToString(new BufferedInputStream(new FileInputStream(file)));
      }
      JsonDict config = new JsonDict(configStr);
      String role = line.getOptionValue("role");


      int port = Integer.valueOf(portStr);
      try {

        final DatabaseServer databaseServer = new DatabaseServer();
        final AtomicBoolean isRunning = new AtomicBoolean(false);
        databaseServer.setConfig(config, cluster, host, port, isRunning, gclog, xmx, false);
        databaseServer.setRole(role);

        setDatabaseServer(databaseServer);

        Thread thread = new Thread(new Runnable(){
          @Override
          public void run() {
            try {
              //for (int i = 0; i < 10000000; i++) {
                for (String dbName : databaseServer.getDbNames(databaseServer.getDataDir())) {
                  databaseServer.getSnapshotManager().recoverFromSnapshot(dbName);
                }
              //}
              logger.info("applying queues");
              databaseServer.getLogManager().applyQueues();

              logger.info("starting repartitioner");
              databaseServer.startRepartitioner();

              logger.info("running snapshot loop");
              databaseServer.getSnapshotManager().runSnapshotLoop();

              isRunning.set(true);
            }
            catch (Exception e) {
              logger.error("Error starting server", e);
            }
          }
        });
        thread.start();
      }
      catch (Exception e) {
        logger.error("Error recovering snapshot", e);
        System.exit(1);
      }


      Thread nettyThread = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            logger.info("starting netty server");
            NettyServer.this.run();
          }
          catch (Exception e) {
            logger.error("Error starting netty server", e);
          }
        }
      });
      nettyThread.start();

      Logger.setReady();

      this.isRunning = true;
      nettyThread.join();
      logger.info("joined netty thread");
    }
    catch (Exception e) {
      logger.error("Error starting server", e);
      throw new com.sonicbase.query.DatabaseException(e);
    }
    logger.info("exiting netty server");
  }

  public static String getHelpPage(NettyServer server) throws IOException {
    InputStream in = server.getClass().getResourceAsStream("rest-help.html");
    return StreamUtils.inputStreamToString(in);
  }
}
