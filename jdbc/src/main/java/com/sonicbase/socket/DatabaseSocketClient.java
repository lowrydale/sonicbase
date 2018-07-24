package com.sonicbase.socket;

import com.sonicbase.common.ComObject;
import com.sonicbase.common.DeadServerException;
import com.sonicbase.query.DatabaseException;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * User: lowryda
 * Date: 11/7/14
 * Time: 5:20 PM
 */
public class DatabaseSocketClient {

  private static final int CONNECTION_COUNT = 10000;
  private static final String PORT_STR = ", port=";
  private static Logger logger = LoggerFactory.getLogger(DatabaseSocketClient.class);
  private static ConcurrentHashMap<String, ArrayBlockingQueue<Connection>> pools = new ConcurrentHashMap<>();
  private static AtomicInteger connectionCount = new AtomicInteger();
  private static ConcurrentLinkedQueue<Connection> openConnections = new ConcurrentLinkedQueue<>();

  private static Connection borrowConnection(final String host, final int port) {
    for (int i = 0; i < 1; i++) {
      try {
        Connection sock = null;
        ArrayBlockingQueue<Connection> pool = pools.get(host + ":" + port);
        if (pool == null) {
          pool = new ArrayBlockingQueue<>(CONNECTION_COUNT);
          pools.put(host + ":" + port, pool);
        }
        sock = pool.poll(0, TimeUnit.MILLISECONDS);
        if (sock == null) {
          try {
            final CountDownLatch latch = new CountDownLatch(1);
            final AtomicReference<Connection> newSock = new AtomicReference<>();
            connectionCount.incrementAndGet();
            Thread thread = new Thread(() -> {
              try {
                newSock.set(new Connection(host, port));
                openConnections.add(newSock.get());
                latch.countDown();
              }
              catch (Exception e) {
                logger.error("Error connecting to server: host={}, port={}", host, port);
              }
            });
            thread.start();
            if (latch.await(20_000, TimeUnit.MILLISECONDS)) {
              sock = newSock.get();
            }
            else {
              thread.interrupt();
              thread.join();
              throw new DatabaseException("Error connecting to server: host=" + host + PORT_STR + port);
            }
          }
          catch (Exception t) {
            throw new DatabaseException("Error creating connection: host=" + host + PORT_STR + port, t);
          }
        }
        sock.countCalled++;
        return sock;
      }
      catch (Exception t) {
        try {
          Thread.sleep(100);
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new DatabaseException(e);
        }
        throw new DatabaseException(t);
      }
    }
    throw new DatabaseException("Error borrowing connection");
  }

  public static void returnConnection(
      Connection sock, String host, int port) {
    if (sock != null) {
      try {
        pools.get(host + ":" + port).put(sock);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new DatabaseException(e);
      }
    }
  }

  public static void shutdown() {
    synchronized (DatabaseSocketClient.class) {
      for (ArrayBlockingQueue<Connection> pool : pools.values()) {
        while (true) {
          try {
            Connection conn = pool.poll(1, TimeUnit.MILLISECONDS);
            if (conn == null) {
              break;
            }
          }
          catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Error closing connection pool");
          }
        }
      }
      List<Connection> toRemove = new ArrayList<>();
      for (Connection conn : openConnections) {
        try {
          conn.sock.shutdownInput();
          conn.sock.shutdownOutput();
          conn.sock.close();
          toRemove.add(conn);
        }
        catch (IOException e) {
          logger.info("Error closing connection");
        }
      }
      for (Connection curr : toRemove) {
        openConnections.remove(curr);
      }
    }
  }

  static class Connection {
    private int countCalled;
    private SocketChannel sock;

    public Connection(String host, int port) throws IOException {
      for (int i = 0; i < 3; i++) {
        try {
          this.countCalled = 0;
          this.sock = SocketChannel.open();
          InetSocketAddress address = new InetSocketAddress(host, port);
          this.sock.connect(address);
          this.sock.configureBlocking(true);

          sock.socket().setSoLinger(true, 120);
          sock.socket().setKeepAlive(true);
          sock.socket().setReuseAddress(true);
          sock.socket().setSoTimeout(100000000);
          sock.socket().setTcpNoDelay(true);
          sock.socket().setPerformancePreferences(0, 1, 0);
        }
        catch (ConnectException e) {
          if (i == 2) {
            throw new DatabaseException(e);
          }
          try {
            Thread.sleep(100);
          }
          catch (InterruptedException e1) {
            Thread.currentThread().interrupt();
            throw new DatabaseException(e1);
          }
        }
      }
    }
  }

  public static final boolean ENABLE_BATCH = true;

  public static class Request {
    private byte[] body;
    private byte[] response;
    private CountDownLatch latch = new CountDownLatch(1);
    private boolean success;

    private Exception exception;
    private String hostPort;

    public byte[] getResponse() {
      return response;
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

    public void setHostPort(String hostPort) {
      this.hostPort = hostPort;
    }

    public String getHostPort() {
      return hostPort;
    }
  }

  private static AtomicLong totalCallCount = new AtomicLong();
  private static AtomicLong callCount = new AtomicLong();
  private static AtomicLong callDuration = new AtomicLong();
  private static AtomicLong requestDuration = new AtomicLong();
  private static AtomicLong processingDuration = new AtomicLong();
  private static AtomicLong responseDuration = new AtomicLong();
  private static AtomicLong lastLogReset = new AtomicLong(System.currentTimeMillis());

  public static void sendBatch(String host, int port, List<Request> requests) {
    try {
      long begin = System.nanoTime();
      totalCallCount.incrementAndGet();
      if (callCount.incrementAndGet() % 10000 == 0) {
        int connectionCount = 0;
        int maxConnectionCount = 0;
        for (ArrayBlockingQueue<Connection> value : pools.values()) {
          connectionCount += value.size();
          maxConnectionCount = Math.max(value.size(), maxConnectionCount);
        }

        logger.info("SocketClient stats: callCount={}, avgDuration={}, processingDuration={}, avgRequestDuration={}, avgResponseDuration={}, avgConnectionCount={}, maxConnectionCount={}",
            totalCallCount.get(), ((double)callDuration.get() / callCount.get() / 1000000d),
            ((double)processingDuration.get() / callCount.get() / 1000000d),
            ((double)requestDuration.get() / callCount.get() / 1000000d),
            ((double)responseDuration.get() / callCount.get() / 1000000d), (connectionCount / pools.size()), maxConnectionCount);
        synchronized (lastLogReset) {
          if (System.currentTimeMillis() - lastLogReset.get() > 4 * 60 * 1000) {
            callDuration.set(0);
            callCount.set(0);
            requestDuration.set(0);
            responseDuration.set(0);
            processingDuration.set(0);
            lastLogReset.set(System.currentTimeMillis());
          }
        }
      }
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      byte[] intBuff = new byte[4];
      int requestCount = requests.size();
      Util.writeRawLittleEndian32(requestCount, intBuff);
      bytesOut.write(intBuff);
      for (Request currRequest : requests) {
        serializeSingleRequest(bytesOut, currRequest.body);
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
      Connection sock = borrowConnection(host, port);
      try {
        ByteArrayOutputStream sockBytes = new ByteArrayOutputStream();

        sockBytes.write(intBuff);
        if (COMPRESS) {
          byte[] originalLenBuff = new byte[4];
          Util.writeRawLittleEndian32(originalBodyLen, originalLenBuff);
          sockBytes.write(originalLenBuff);

        }
        long checksumValue = 0;
        byte[] longBuff = new byte[8];

        Util.writeRawLittleEndian64(checksumValue, longBuff);
        sockBytes.write(longBuff);

        sockBytes.write(body);

        long beginRequest = System.nanoTime();
        writeRequest(sock, ByteBuffer.wrap(sockBytes.toByteArray()));
        long processingBegin = System.nanoTime();
        requestDuration.addAndGet(System.nanoTime() - beginRequest);

        int totalRead = 0;

        int bodyLen = 0;

        byte[] responseBody = readResponse(intBuff, sock, totalRead, bodyLen, processingBegin);

        int offset = 0;
        if (COMPRESS) {
          originalBodyLen = Util.readRawLittleEndian32(responseBody, 0);
          offset += 4;
        }
        Util.readRawLittleEndian64(responseBody, offset); // responseChecksum
        offset += 8;

        if (DatabaseSocketClient.COMPRESS) {
          if (DatabaseSocketClient.LZO_COMPRESSION) {
            LZ4Factory factory = LZ4Factory.fastestInstance();

            LZ4FastDecompressor decompressor = factory.fastDecompressor();
            byte[] restored = new byte[originalBodyLen];
            decompressor.decompress(responseBody, offset, restored, 0, originalBodyLen);
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
            logger.error("Error processing response: method={}",
                new ComObject(currRequest.body).getString(ComObject.Tag.METHOD));
            throw new DeadServerException(t);
          }
        }
      }
      catch (Exception e) {
        if (sock != null && sock.sock != null) {
          sock.sock.close();
          openConnections.remove(sock);
        }
        connectionCount.decrementAndGet();
        shouldReturn = false;
        throw new DeadServerException(e);
      }
      finally {
        if (shouldReturn) {
          if (sock.countCalled > 100000) {
            if (sock != null && sock.sock != null) {
              sock.sock.close();
              openConnections.remove(sock);
            }
          }
          else {
            returnConnection(sock, host, port);
          }
        }
      }
      callDuration.addAndGet(System.nanoTime() - begin);
    }
    catch (IOException e) {
      throw new DeadServerException(e);
    }
  }

  private static byte[] readResponse(byte[] intBuff, Connection sock, int totalRead, int bodyLen, long processingBegin) throws IOException {
    int nBytes;
    long beginResponse = 0;
    boolean first = true;
    ByteBuffer buf = ByteBuffer.allocateDirect(intBuff.length - totalRead);
    while (!Thread.interrupted() && (nBytes = sock.sock.read(buf)) > 0) {
      if (first) {
        processingDuration.addAndGet(System.nanoTime() - processingBegin);
        beginResponse = System.nanoTime();
        first = false;
      }
      buf.flip();
      buf.get(intBuff, totalRead, nBytes);
      buf.clear();

      totalRead += nBytes;
      if (totalRead == intBuff.length) {
        bodyLen = Util.readRawLittleEndian32(intBuff);
        break;
      }
    }

    totalRead = 0;
    byte[] responseBody = new byte[bodyLen];
    buf = ByteBuffer.allocateDirect(responseBody.length - totalRead);
    while ((nBytes = sock.sock.read(buf)) > 0) {
      buf.flip();
      buf.get(responseBody, totalRead, nBytes);
      buf.clear();

      totalRead += nBytes;
      if (totalRead == responseBody.length) {
        break;
      }
    }
    responseDuration.addAndGet(System.nanoTime() - beginResponse);

    return responseBody;
  }

  private static void writeRequest(Connection sock, ByteBuffer sockBytes) throws IOException {
    sock.sock.write(sockBytes);
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

  private static void serializeSingleRequest(ByteArrayOutputStream bytesOut, byte[] body) throws IOException {
    byte[] intBuff = new byte[4];
    Util.writeRawLittleEndian32(body.length, intBuff);
    bytesOut.write(intBuff);
    bytesOut.write(body);
  }

  public static final boolean COMPRESS = true;
  public static final boolean LZO_COMPRESSION = true;

  public byte[] doSend(String batchKey, byte[] body, String hostPort) {
    try {
      Request request = new Request();
      request.body = body;

      try {
        List<Request> nonMatchingRequets = new ArrayList<>();
        nonMatchingRequets.add(request);
        String[] parts = hostPort.split(":");
        sendBatch(parts[0], Integer.valueOf(parts[1]), nonMatchingRequets);
      }
      catch (Exception t) {
        request.exception = t;
        request.latch.countDown();
      }
      if (request.exception != null) {
        throw new DeadServerException(request.exception);
      }
      if (!request.success) {
        String exceptionStr = new String(request.response, "utf-8");
        if (exceptionStr.contains("Server not running")) {
          throw new DeadServerException();
        }
        throw new DatabaseException(exceptionStr);
      }
      return request.response;
    }
    catch (DeadServerException e) {
      String[] parts = hostPort.split(":");
      throw new DeadServerException("Server error: host=" + parts[0] + PORT_STR + parts[1] +
          ", method=" + new ComObject(body).getString(ComObject.Tag.METHOD), e);
    }
    catch (Exception e) {
      String[] parts = hostPort.split(":");
      throw new DatabaseException("Server error: host=" + parts[0] + PORT_STR + parts[1] +
          ", method=" + new ComObject(body).getString(ComObject.Tag.METHOD), e);
    }
  }

  public static byte[] doSend(List<Request> requests) {
    try {

      String[] parts = requests.get(0).hostPort.split(":");
      sendBatch(parts[0], Integer.valueOf(parts[1]), requests);

      for (Request request : requests) {
        if (request.exception != null) {
          throw request.exception;
        }
        if (!request.success) {
          String exceptionStr = new String(request.response, "utf-8");
          if (exceptionStr.contains("Server not running")) {
            throw new DeadServerException();
          }
          throw new DatabaseException(exceptionStr);
        }
      }
      return requests.get(0).response;

    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }
}
