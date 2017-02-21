package com.lowryengineering.database.socket;

import com.lowryengineering.database.query.DatabaseException;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * User: lowryda
 * Date: 11/7/14
 * Time: 5:20 PM
 */
public class DatabaseSocketClient {

  private static int CONNECTION_COUNT = 10000;

  private static Logger logger = LoggerFactory.getLogger(DatabaseSocketClient.class);


  private static ConcurrentHashMap<String, ArrayBlockingQueue<Connection>> pools = new ConcurrentHashMap<>();

  private static AtomicInteger connectionCount = new AtomicInteger();
  private List<Thread> batchThreads = new ArrayList<>();

  private static Connection borrow_connection(String host, int port) {
    for (int i = 0; i < 1; i++) {
      try {
        Connection sock = null;
        ArrayBlockingQueue<Connection> pool = pools.get(host + ":" + port);
        if (pool == null) {
          pool = new ArrayBlockingQueue<>(CONNECTION_COUNT);
          pools.put(host + ":" + port, pool);
        }
        if (connectionCount.get() >= CONNECTION_COUNT) {
          sock = pool.poll(20000, TimeUnit.MILLISECONDS);
        }
        else {
          sock = pool.poll(0, TimeUnit.MILLISECONDS);
          if (sock == null) {
            try {
              connectionCount.incrementAndGet();
              logger.info("Adding connection: count=" + connectionCount.get());
              sock = new Connection(host, port);//new NioClient(host, port);
            }
            catch (Exception t) {
              throw new Exception("Error creating connection: host=" + host + ", port=" + port, t);
            }
          }
        }
        return sock;
      }
      catch (Exception t) {
        logger.error("Error borrowing connection: host=" + host + ", port=" + port);
        //   t.printStackTrace();
        try {
          Thread.sleep(100);
        }
        catch (InterruptedException e) {
          throw new DatabaseException(e);
        }
        throw new DatabaseException(t);
      }
    }
    throw new DatabaseException("Error borrowing connection");
  }

  public static void return_connection(
      Connection sock, String host, int port) {
//      synchronized (borrowLock) {
    if (sock != null) {
      try {
        pools.get(host + ":" + port).put(sock);
      }
      catch (InterruptedException e) {
        throw new DatabaseException(e);
      }
    }
//      }
  }

  private static EventLoopGroup clientGroup = new NioEventLoopGroup(); // NIO event loops are also OK

  public List<Thread> getBatchThreads() {
    return batchThreads;
  }

  public static class NioClient {
    private ClientNioHandler clientHandler;
    //private io.netty.channel.Channel channel;

    public NioClient(String address, int port) {

      clientHandler = new ClientNioHandler();
      Bootstrap cb = new Bootstrap();
      cb.group(clientGroup)
          .channel(NioSocketChannel.class)
          .option(ChannelOption.SO_KEEPALIVE, true)
          .option(ChannelOption.SO_REUSEADDR, true)
          .option(ChannelOption.TCP_NODELAY, true)
          .handler(new ChannelInitializer<io.netty.channel.socket.SocketChannel>() {
            @Override
            public void initChannel(io.netty.channel.socket.SocketChannel ch) {
              ch.pipeline().addLast(
                  clientHandler);
            }
          });
      // Start the client.
      try {
        cb.connect(address, port).sync().channel();
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }


//      NioEventLoopGroup workerGroup = new NioEventLoopGroup();
//      Bootstrap b = new Bootstrap();
//      b.group(workerGroup);
//      b.channel(NioSocketChannel.class);
//
//      this.clientHandler = new ClientNioHandler();
//      b.handler(new ChannelInitializer<io.netty.channel.socket.SocketChannel>() {
//
//        @Override
//        public void initChannel(io.netty.channel.socket.SocketChannel ch) throws Exception {
//          ch.pipeline().addLast(clientHandler);
//          NioClient.this.channel = ch;
//        }
//      });
//
//      b.connect(address, port);
      while (clientHandler.channel == null) {
        try {
          Thread.sleep(1);
        }
        catch (InterruptedException e) {
          throw new DatabaseException(e);
        }
      }
    }
  }

  public static class ClientNioHandler extends ChannelInboundHandlerAdapter {
    private byte[] body;
    private byte[] lenBytes = new byte[4];
    private int lenPos;
    private int bodyPos;
    private io.netty.channel.socket.SocketChannel channel;
    private CountDownLatch latch;

    public void await() throws InterruptedException {
      this.latch.await();
    }

    public void write(byte[] bytes) {
      this.latch = new CountDownLatch(1);
      channel.writeAndFlush(bytes);
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) {
      channel = (io.netty.channel.socket.SocketChannel) ctx.channel();
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
      //this.channel = ctx.channel();
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
      try {
        if (body == null) {
          int bytesToRead = Math.min(4 - lenPos, ((ByteBuf) msg).readableBytes());
          ((ByteBuf) msg).readBytes(lenBytes, lenPos, bytesToRead);
          //System.arraycopy(((ByteBuf) msg).array(), 0, lenBytes, lenPos, bytesToRead);
          lenPos += bytesToRead;
          if (lenPos == 4) {
            body = new byte[Util.readRawLittleEndian32(lenBytes)];
            bodyPos = 0;

            if (bytesToRead < ((ByteBuf) msg).readableBytes()) {
              bytesToRead = ((ByteBuf) msg).readableBytes();
              ((ByteBuf) msg).readBytes(body, bodyPos, bytesToRead);
              //  System.arraycopy(((ByteBuf) msg).array(), 0, body, bodyPos, bytesToRead);
              bodyPos += bytesToRead;
              if (bodyPos == body.length) {
                //ctx.close();
                bodyPos = lenPos = 0;
                latch.countDown();
              }
            }
          }
        }
        else if (body != null) {
          int numBytesToRead = ((ByteBuf) msg).readableBytes();
          ((ByteBuf) msg).readBytes(body, bodyPos, numBytesToRead);
          //          System.arraycopy(((ByteBuf) msg).array(), 0, body, bodyPos, ((ByteBuf) msg).readableBytes());
          bodyPos += numBytesToRead;
          if (bodyPos == body.length) {
            //ctx.close();
            bodyPos = lenPos = 0;
            latch.countDown();
          }
        }
      }
      finally {
        ((ByteBuf) msg).release();
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      cause.printStackTrace();
      ctx.close();
    }
  }

  static class Connection {
    private int count_called;
    private SocketChannel sock;

    public Connection(String host, int port) throws IOException {
      this.count_called = 0;
      this.sock = SocketChannel.open();
      InetSocketAddress address = new InetSocketAddress(host, port);
      this.sock.connect(address);
      this.sock.configureBlocking(true);

//      this.sock = new Socket(host, port);
//      sock.setSoLinger(true, 120);
//
      //sock.setSoLinger(false, 0);
      sock.socket().setKeepAlive(true);
      sock.socket().setReuseAddress(true);
      sock.socket().setSoTimeout(100000000);
      sock.socket().setTcpNoDelay(true);
      sock.socket().setPerformancePreferences(0, 1, 0);
    }
  }

  public static final boolean ENABLE_BATCH = true;
  private static final int BATCH_SIZE = 80;

  private Map<String, ConcurrentLinkedQueue<Request>> requestQueues = new HashMap<>();

  public static class Request {
    private String command;
    private byte[] body;
    private byte[] response;
    private CountDownLatch latch = new CountDownLatch(1);
    private boolean success;

    private Exception exception;
    private String batchKey;
    public String hostPort;
    public DatabaseSocketClient socketClient;

    public void setCommand(String command) {
      this.command = command;
    }

    public void setBody(byte[] body) {
      this.body = body;
    }

    public void setResponse(byte[] response) {
      this.response = response;
    }

    public void setLatch(CountDownLatch latch) {
      this.latch = latch;
    }

    public void setSuccess(boolean success) {
      this.success = success;
    }

    public void setException(Exception exception) {
      this.exception = exception;
    }

    public void setBatchKey(String batchKey) {
      this.batchKey = batchKey;
    }

    public void setHostPort(String hostPort) {
      this.hostPort = hostPort;
    }

    public void setSocketClient(DatabaseSocketClient socketClient) {
      this.socketClient = socketClient;
    }
  }

  private static AtomicInteger batchCount = new AtomicInteger();
  private static AtomicLong batchTotalEntryCount = new AtomicLong();

  static class BatchSender implements Runnable {

    private final ConcurrentLinkedQueue<Request> queue;
    private final String host;
    private final int port;

    public BatchSender(String host, int port, ConcurrentLinkedQueue<Request> requests) {
      this.queue = requests;
      this.host = host;
      this.port = port;
    }

    @Override
    public void run() {

      while (true) {
        List<Request> requests = new ArrayList<>();
        try {

          Request request = queue.poll();//30000, TimeUnit.MILLISECONDS);
          if (request == null) {
            Thread.sleep(0, 5000);
            continue;
          }
          requests.add(request);
          String batchKey = request.batchKey;
          Thread.sleep(0, 100);
          for (int i = 0; i < BATCH_SIZE; i++) {
            request = queue.poll();
            if (request == null) {
              break;
            }
            if (batchKey == null || request.batchKey == null || !batchKey.equals(request.batchKey)) {
              try {
                List<Request> nonMatchingRequets = new ArrayList<>();
                nonMatchingRequets.add(request);
                sendBatch(host, port, nonMatchingRequets);
              }
              catch (Exception t) {
                request.exception = t;
                request.latch.countDown();
              }
              break;
            }
            requests.add(request);
          }
          //queue.drainTo(requests, BATCH_SIZE);

//            for (int i = 0; i < BATCH_SIZE; i++) {
//              request = queue.poll(0, TimeUnit.MILLISECONDS);
//              if (request != null) {
//                requests.add(request);
//              }
//            }
          if (requests.size() == 0) {
            continue;
          }
          sendBatch(host, port, requests);

          batchTotalEntryCount.addAndGet(requests.size());
          if (batchCount.incrementAndGet() % 100000 == 0) {
            logger.info("Batch stats: count=" + batchCount.get() + ", avgSize=" + (batchTotalEntryCount.get() / batchCount.get()));
          }
//          requests.add(request);
//
//          for (int i = 0; i < BATCH_SIZE; i++) {
//            request = queue.poll(0, TimeUnit.MILLISECONDS);
//            if (request != null) {
//              requests.add(request);
//            }
//          }
//          sendBatch(host, port, requests);

        }
        catch (InterruptedException e) {
          break;
        }
        catch (Exception t) {
          for (Request request : requests) {
            request.exception = t;
            request.latch.countDown();
          }
          //t.printStackTrace();
        }
      }
    }
  }

  private static void sendBatch(String host, int port, List<Request> requests) {
    try {
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      byte[] intBuff = new byte[4];
      int requestCount = requests.size();
      Util.writeRawLittleEndian32(requestCount, intBuff);
      bytesOut.write(intBuff);
      for (Request currRequest : requests) {
        serializeSingleRequest(bytesOut, currRequest.command, currRequest.body);
      }
      bytesOut.close();
      byte[] body = bytesOut.toByteArray();
      int originalBodyLen = body.length;
      if (COMPRESS) {
        if (LZO_COMPRESSION) {
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
      }
      else {
        Util.writeRawLittleEndian32(body.length + 8, intBuff);
      }


      boolean shouldReturn = true;
      Connection sock = borrow_connection(host, port);
      try {
        //System.out.println("borrow: " + (end - begin) / 1000000f);

        //OutputStream out = new BufferedOutputStream(sock.sock.getOutputStream());
        ByteBuffer buf = ByteBuffer.wrap(intBuff);
        sock.sock.write(buf);
        //buf.release();
        if (COMPRESS) {
          byte[] originalLenBuff = new byte[4];
          Util.writeRawLittleEndian32(originalBodyLen, originalLenBuff);
          buf = ByteBuffer.wrap(originalLenBuff);
          sock.sock.write(buf);
          //buf.release();
        }
        CRC32 checksum = new CRC32();
        checksum.update(body, 0, body.length);
        long checksumValue = checksum.getValue();
        //checksumValue = Arrays.hashCode(body);
        byte[] longBuff = new byte[8];

        Util.writeRawLittleEndian64(checksumValue, longBuff);
        buf = ByteBuffer.wrap(longBuff);
        //buf.release();
        sock.sock.write(buf);

        //sock.latch = new CountDownLatch(1);
        //buf = Unpooled.wrappedBuffer(body);
        //sock.sock.body = null;
        //ChannelFuture future = sock.sock.write(ByteBuffer.wrap(body));
        sock.sock.write(ByteBuffer.wrap(body));
        //      //buf.release();
        //      future.addListener(new GenericFutureListener<ChannelFuture>() {
        //        @Override
        //        public void operationComplete(ChannelFuture future) throws Exception {
        //          if (!future.isSuccess()) {
        //            System.err.print("write failed: ");
        //            future.cause().printStackTrace(System.err);
        //          }
        //        }
        //      });
        //sock.channel.flush();
        //InputStream in = new BufferedInputStream(sock.sock.getInputStream());
        int totalRead = 0;

        int bodyLen = 0;
        //while (true) {
        int nBytes = 0;
        //sock.clientHandler.await();
        //
        //     while (true) {
        buf = ByteBuffer.allocate(intBuff.length - totalRead);
        while ((nBytes = nBytes = sock.sock.read(buf)) > 0) {
          buf.flip();
          System.arraycopy(buf.array(), 0, intBuff, totalRead, nBytes);
          buf.clear();

          totalRead += nBytes;
          if (totalRead == intBuff.length) {
            bodyLen = Util.readRawLittleEndian32(intBuff);
            break;
          }
        }
        //              int lenRead = sock.sock.read(intBuff, totalRead, intBuff.length - totalRead);
        //              if (lenRead == -1) {
        //                throw new Exception("EOF");
        //}
        //        totalRead += nBytes;
        //        if (totalRead == intBuff.length) {
        //          bodyLen = Util.readRawLittleEndian32(intBuff);
        //          break;
        //        }

        totalRead = 0;
        byte[] responseBody = new byte[bodyLen];
        //while (true) {
        nBytes = 0;
        buf = ByteBuffer.allocate(responseBody.length - totalRead);
        while ((nBytes = nBytes = sock.sock.read(buf)) > 0) {
          buf.flip();
          System.arraycopy(buf.array(), 0, responseBody, totalRead, nBytes);
          buf.clear();

          totalRead += nBytes;
          if (totalRead == responseBody.length) {
            break;
          }
        }
        //
        //                    int lenRead = in.read(responseBody, totalRead, responseBody.length - totalRead);
        //                    if (lenRead == -1) {
        //                      throw new Exception("EOF");
        //                    }
        //

        int offset = 0;
        if (COMPRESS) {
          originalBodyLen = Util.readRawLittleEndian32(responseBody, 0);
          offset += 4;
        }
        long responseChecksum = Util.readRawLittleEndian64(responseBody, offset);
        offset += 8;
        body = new byte[responseBody.length - offset];
        System.arraycopy(responseBody, offset, body, 0, body.length);

        checksum = new CRC32();
        checksum.update(body, 0, body.length);
        checksumValue = checksum.getValue();
        if (checksumValue != responseChecksum) {
          throw new DatabaseException("Checksum mismatch");
        }

        if (DatabaseSocketClient.COMPRESS) {
          if (DatabaseSocketClient.LZO_COMPRESSION) {
            LZ4Factory factory = LZ4Factory.fastestInstance();

            LZ4FastDecompressor decompressor = factory.fastDecompressor();
            byte[] restored = new byte[originalBodyLen];
            decompressor.decompress(body, 0, restored, 0, originalBodyLen);
            body = restored;
          }
          else {
            GZIPInputStream bodyIn = new GZIPInputStream(new ByteArrayInputStream(body));
            body = new byte[originalBodyLen];
            bodyIn.read(body);
          }
        }

        ByteArrayInputStream bytesIn = new ByteArrayInputStream(body);
        bytesIn.read(intBuff); //response count

        for (Request currRequest : requests) {
          try {
            processResponse(bytesIn, currRequest);
          }
          catch (Exception t) {
            System.out.println("Error processing response: command=" + currRequest.command);
            throw new DatabaseException(t);
          }
        }

      }
      catch (IOException e) {
        sock.sock.close();//clientHandler.channel.close();
        connectionCount.decrementAndGet();
        shouldReturn = false;
        throw e;
      }
      finally {
        if (shouldReturn) {
          return_connection(sock, host, port);
        }
      }
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  private static void processResponse(InputStream in, Request request) {
    try {
      int totalRead;
      int responseLen;
      byte[] headerBuff = new byte[1 + 4];
      totalRead = 0;
      while (true) {
        int lenRead = in.read(headerBuff, totalRead, headerBuff.length - totalRead);
        if (lenRead == -1) {
          throw new DatabaseException("EOF");
        }
        totalRead += lenRead;
        if (totalRead == headerBuff.length) {
          request.success = headerBuff[0] == 1;
          responseLen = Util.readRawLittleEndian32(headerBuff, 1);
          break;
        }
      }

      request.response = null;
      if (responseLen > 0) {
        request.response = new byte[responseLen];
        totalRead = 0;
        while (true) {
          int lenRead = in.read(request.response, totalRead, request.response.length - totalRead);
          if (lenRead == -1) {
            throw new DatabaseException("EOF");
          }
          totalRead += lenRead;
          if (totalRead == request.response.length) {
            break;
          }
        }
      }
      request.latch.countDown();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  private static void serializeSingleRequest(ByteArrayOutputStream bytesOut, String command, byte[] body) throws IOException {
    byte[] intBuff = new byte[4];
    byte[] commandBytes = command.getBytes("utf-8");
    Util.writeRawLittleEndian32(commandBytes.length, intBuff);
    bytesOut.write(intBuff);
    if (body == null) {
      Util.writeRawLittleEndian32(0, intBuff);
    }
    else {
      Util.writeRawLittleEndian32(body.length, intBuff);
    }
    bytesOut.write(intBuff);
    bytesOut.write(commandBytes);
    if (body != null) {
      bytesOut.write(body);
    }
  }

  private static final int BATCH_THREAD_COUNT = 2;

  private static void initBatchSender(String host, int port, DatabaseSocketClient socketClient) {
    socketClient.requestQueues.put(host + ":" + port, new ConcurrentLinkedQueue<Request>());


    for (int i = 0; i < BATCH_THREAD_COUNT; i++) {
      Thread thread = new Thread(new BatchSender(host, port, socketClient.requestQueues.get(host + ":" + port)), "BatchSender: host=" + host + ", port=" + port + ", offset=" + i);
      thread.start();
      socketClient.batchThreads.add(thread);
    }
  }


  public static final boolean COMPRESS = true;
  public static final boolean LZO_COMPRESSION = true;

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="EI_EXPOSE_REP2", justification="copying the passed in data is too slow")
  @SuppressWarnings("PMD.ArrayIsStoredDirectly") //copying the passed in data is too slow
  public byte[] do_send(String batchKey, String command, byte[] body, String hostPort) {
    try {
      if (ENABLE_BATCH) {
        ConcurrentLinkedQueue<Request> queue = null;
        synchronized (this) {
          queue = requestQueues.get(hostPort);
          if (queue == null) {
            String[] parts = hostPort.split(":");
            initBatchSender(parts[0], Integer.valueOf(parts[1]), this);
            queue = requestQueues.get(hostPort);
          }
        }
        Request request = new Request();
        request.batchKey = batchKey;
        request.command = command;
        request.body = body;

        queue.add(request);

        if (!request.latch.await(365, TimeUnit.DAYS)) {
          throw new Exception("Request timeout");
        }

        if (request.exception != null) {
          throw request.exception;
        }
        if (!request.success) {
          throw new Exception(new String(request.response, "utf-8"));
        }
        return request.response;
      }
      return null;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public static byte[] do_send(List<Request> requests) {
    try {

      if (ENABLE_BATCH) {
        ConcurrentLinkedQueue<Request> queue = null;
        for (Request request : requests) {
          synchronized (request.socketClient) {
            queue = request.socketClient.requestQueues.get(request.hostPort);
            if (queue == null) {
              String[] parts = request.hostPort.split(":");
              initBatchSender(parts[0], Integer.valueOf(parts[1]), request.socketClient);
              queue = request.socketClient.requestQueues.get(request.hostPort);
            }
          }
          queue.add(request);
        }

        for (Request request : requests) {
          if (!request.latch.await(365, TimeUnit.DAYS)) {
            throw new Exception("Request timeout");
          }

          if (request.exception != null) {
            throw request.exception;
          }
          if (!request.success) {
            throw new Exception(new String(request.response, "utf-8"));
          }
        }
        return requests.get(0).response;
      }
      return null;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
//    long begin = System.nanoTime();
//    NioClient sock = borrow_connection(host, port);
//    try {
//      long end = System.nanoTime();
//      //System.out.println("borrow: " + (end - begin) / 1000000f);
//      //sock.sock.setSoTimeout(timeout);
//      //OutputStream out = new BufferedOutputStream(sock.sock.getOutputStream());
//
//      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
//      byte[] intBuff = new byte[4];
//      byte[] commandBytes = command.getBytes("utf-8");
//      Util.writeRawLittleEndian32(commandBytes.length, intBuff);
//      bytesOut.write(intBuff);
//      int origBodyLen = body == null ? 0 : body.length;
//      if (body == null) {
//        Util.writeRawLittleEndian32(0, intBuff);
//        bytesOut.write(intBuff);
//      }
//      else {
//        if (COMPRESS) {
//          if (LZO_COMPRESSION) {
//            LZ4Factory factory = LZ4Factory.fastestInstance();
//
//            LZ4Compressor compressor = factory.fastCompressor();
//            int maxCompressedLength = compressor.maxCompressedLength(body.length);
//            byte[] compressed = new byte[maxCompressedLength];
//            int compressedLength = compressor.compress(body, 0, body.length, compressed, 0, maxCompressedLength);
//            body = new byte[compressedLength];
//            System.arraycopy(compressed, 0, body, 0, compressedLength);
//          }
//          else {
//            ByteArrayOutputStream bodyBytesOut = new ByteArrayOutputStream();
//            GZIPOutputStream bodyOut = new GZIPOutputStream(bodyBytesOut);
//            bodyOut.write(body);
//            bodyOut.close();
//            body = bodyBytesOut.toByteArray();
//          }
//          Util.writeRawLittleEndian32(body.length + 12, intBuff);
//          bytesOut.write(intBuff);
//        }
//        else {
//          Util.writeRawLittleEndian32(body.length + 8, intBuff);
//          bytesOut.write(intBuff);
//        }
//      }
//      bytesOut.write(commandBytes);
//      if (body != null) {
//        if (COMPRESS) {
//          Util.writeRawLittleEndian32(origBodyLen, intBuff);
//          bytesOut.write(intBuff);
//        }
//        CRC32 checksum = new CRC32();
//        checksum.update(body, 0, body.length);
//        long checksumValue = checksum.getValue();
//        checksumValue = Arrays.hashCode(body);
//        byte[] longBuff = new byte[8];
//        Util.writeRawLittleEndian64(checksumValue, longBuff);
//        bytesOut.write(longBuff);
//
//        bytesOut.write(body);
//      }
//      bytesOut.close();
//      sock.clientHandler.latch = new CountDownLatch(1);
//      sock.clientHandler.channel.write(ByteBuffer.wrap(bytesOut.toByteArray()));
//      sock.clientHandler.channel.flush();
//      //out.flush();
//      //InputStream in = new BufferedInputStream(sock.sock.getInputStream());
//      int totalRead = 0;
//
//      boolean success = false;
//      int responseLen = 0;
//      byte[] headerBuff = new byte[1 + 4];
//
//      sock.clientHandler.await();
//
////      int bodyLen = 0;
////      while (true) {
////        int nBytes = 0;
////        ByteBuffer buf = ByteBuffer.allocate(intBuff.length - totalRead);
////        while ((nBytes = nBytes = sock.sock.read(buf)) > 0) {
////          buf.flip();
////          System.arraycopy(buf.array(), 0, intBuff, totalRead, nBytes);
////          buf.flip();
////        }
////              int lenRead = sock.sock.read(intBuff, totalRead, intBuff.length - totalRead);
////              if (lenRead == -1) {
////                throw new Exception("EOF");
//        //}
////        totalRead += nBytes;
////        if (totalRead == intBuff.length) {
////          bodyLen = Util.readRawLittleEndian32(intBuff);
////          break;
////        }
////      }
//
////      totalRead = 0;
////      byte[] responseBody = new byte[bodyLen];
////      while (true) {
////        int nBytes = 0;
////        ByteBuffer buf = ByteBuffer.allocate(responseBody.length - totalRead);
////        while ((nBytes = nBytes = sock.sock.read(buf)) > 0) {
////          buf.flip();
////          System.arraycopy(buf.array(), 0, responseBody, totalRead, nBytes);
////          buf.flip();
////        }
////
//////              int lenRead = in.read(responseBody, totalRead, responseBody.length - totalRead);
//////              if (lenRead == -1) {
//////                throw new Exception("EOF");
//////              }
////        totalRead += nBytes;
////        if (totalRead == responseBody.length) {
////          break;
////        }
////      }
//
//
////      while (true) {
////        int nBytes = 0;
////        ByteBuffer buf = ByteBuffer.allocate(headerBuff.length - totalRead);
////        while ((nBytes = nBytes = sock.sock.read(buf)) > 0) {
////          buf.flip();
////          System.arraycopy(buf.array(), 0, headerBuff, totalRead, nBytes);
////          buf.flip();
////        }
////        int lenRead = in.read(headerBuff, totalRead, headerBuff.length - totalRead);
////        if (lenRead == -1) {
////          throw new Exception("EOF");
////        }
////        totalRead += nBytes;
////        if (totalRead == headerBuff.length) {
////          success = headerBuff[0] == 1;
////          responseLen = Util.readRawLittleEndian32(headerBuff, 1);
////          break;
////        }
////      }
//
////      totalRead = 0;
////      byte[] retBytes = new byte[responseLen];
////      while (true) {
////        int nBytes = 0;
////        ByteBuffer buf = ByteBuffer.allocate(retBytes.length - totalRead);
////        while ((nBytes = nBytes = sock.sock.read(buf)) > 0) {
////          buf.flip();
////          System.arraycopy(buf.array(), 0, retBytes, totalRead, nBytes);
////          buf.flip();
////        }
////        int lenRead = in.read(retBytes, totalRead, retBytes.length - totalRead);
////        if (lenRead == -1) {
////          throw new Exception("EOF");
////        }
////        totalRead += nBytes;
////        if (totalRead == retBytes.length) {
////          if (!success) {
////            throw new Exception(new String(retBytes, "utf-8"));
////          }
////          break;
////        }
////      }
//      return_connection(sock, host, port);
//
//      return sock.clientHandler.body;
//    }
//    catch (SocketException e) {
//      sock.clientHandler.channel.close();
//      connectionCount.decrementAndGet();
//      throw e;
//    }
  }


}
