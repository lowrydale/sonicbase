package com.sonicbase.server;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.util.DataUtil;
import com.sonicbase.util.ISO8601;
import com.sonicbase.util.StreamUtils;
import com.sonicbase.research.socket.NettyServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

/**
 * Responsible for
 */
public class LogManager {

  private static final String DATABASE_STR = "database";
  private static final String UTF8_STR = "utf-8";
  private static Logger logger = LoggerFactory.getLogger(LogManager.class);
  private final DatabaseServer databaseServer;
  private final ThreadPoolExecutor executor;

  private AtomicLong countLogged = new AtomicLong();
  private final DatabaseServer server;
  private ArrayBlockingQueue<DatabaseServer.LogRequest> logRequests = new ArrayBlockingQueue<>(1000);
  private AtomicBoolean unbindQueues = new AtomicBoolean();
  private final Object logLock = new Object();
  private AtomicLong logSequenceNumber = new AtomicLong();
  private AtomicLong maxAllocatedLogSequenceNumber = new AtomicLong();
  private static final int SEQUENCE_NUM_ALLOC_COUNT = 100000;

  public LogManager(DatabaseServer databaseServer) {
    this.databaseServer = databaseServer;
    executor = new ThreadPoolExecutor(64, 64, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

    synchronized (this) {
      try {
        File file = new File(databaseServer.getDataDir(), "logSequenceNum/" + databaseServer.getShard() + "/" + databaseServer.getReplica() + "/logSequenceNum.txt");
        file.getParentFile().mkdirs();
        if (!file.exists() || file.length() == 0) {
          logSequenceNumber.set(0);
          maxAllocatedLogSequenceNumber.set(SEQUENCE_NUM_ALLOC_COUNT);
          try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))) {
            writer.write(String.valueOf(maxAllocatedLogSequenceNumber.get()));
          }
        }
        else {
          try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)))) {
            String seq = null;
            try {
              seq = StreamUtils.readerToString(reader);
              seq = seq.trim();
              logSequenceNumber.set(Long.valueOf(seq));
            }
            catch (Exception e) {
              logSequenceNumber.set(0);
              logger.error("Error reading log sequence number: value=" + seq, e);
            }
            maxAllocatedLogSequenceNumber.set(logSequenceNumber.get() + SEQUENCE_NUM_ALLOC_COUNT);
            try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))) {
              writer.write(String.valueOf(maxAllocatedLogSequenceNumber.get()));
            }
          }
        }
      }
      catch (IOException e) {
        throw new DatabaseException(e);
      }
    }
    this.server = databaseServer;
    int logThreadCount = 1;//64;
    for (int i = 0; i < logThreadCount; i++) {
      Thread thread = new Thread(new LogProcessor(i, logRequests, server.getDataDir(), server.getShard(), server.getReplica()));
      thread.start();
    }
  }

  public long getNextSequencenNum() throws IOException {
    synchronized (this) {
      if (logSequenceNumber.get() == maxAllocatedLogSequenceNumber.get()) {
        maxAllocatedLogSequenceNumber.set(logSequenceNumber.get() + SEQUENCE_NUM_ALLOC_COUNT);
        File file = new File(databaseServer.getDataDir(), "logSequenceNum/" + databaseServer.getShard() + "/" + databaseServer.getReplica() + "/logSequenceNum.txt");
        file.getParentFile().mkdirs();
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))) {
          writer.write(String.valueOf(maxAllocatedLogSequenceNumber.get()));
        }
      }
      return logSequenceNumber.incrementAndGet();
    }
  }

  public void enableLogProcessor(boolean enable) {
  }

  public void replayLogs() {
    applyQueues();
  }

  public long getCountLogged() {
    return countLogged.get();
  }

  private static class QueueEntry {
    private String command;
    private byte[] body;

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "EI_EXPOSE_REP2", justification = "copying the passed in data is too slow")
    @SuppressWarnings("PMD.ArrayIsStoredDirectly") //copying the passed in data is too slow
    public QueueEntry(String command, byte[] body) {
      this.command = command;
      this.body = body;
    }
  }

  public class LogProcessor implements Runnable {
    private final int offset;
    private final ArrayBlockingQueue<DatabaseServer.LogRequest> logRequests;
    private final String dataDir;
    private final int shard;
    private final int replica;
    private long currQueueTime = 0;
    private DataOutputStream writer = null;

    public LogProcessor(
        int offset, ArrayBlockingQueue<DatabaseServer.LogRequest> logRequests, String dataDir, int shard, int replica) {
      this.offset = offset;
      this.logRequests = logRequests;
      this.dataDir = dataDir;
      this.shard = shard;
      this.replica = replica;
    }

    @Override
    public void run() {
      while (true) {
        try {
          List<DatabaseServer.LogRequest> requests = new ArrayList<>();
          while (true) {
            DatabaseServer.LogRequest request = logRequests.poll(30000, TimeUnit.MILLISECONDS);
            if (request == null) {
              Thread.sleep(0, 50000);
            }
            else {
              requests.add(request);
              break;
////              if (requests.size() > 20) {
////                break;
////              }
//            }
            }
//            logRequests.drainTo(requests, 200);
//            if (requests.size() == 0) {
//              Thread.sleep(0, 50000);
//            }
//            else {
//              break;
//            }
          }
          if (writer == null || System.currentTimeMillis() - 2 * 60 * 100 > currQueueTime) {
            if (writer != null) {
              writer.close();
            }
            String directory = getLogRoot();
            currQueueTime = System.currentTimeMillis();
            File dataRootDir = new File(directory);
            dataRootDir.mkdirs();
            String dt = DatabaseServer.format8601(new Date(System.currentTimeMillis()));
            File newFile = new File(dataRootDir, offset + "-" + dt + ".bin");
            writer = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(newFile), 102400));
          }

          for (DatabaseServer.LogRequest request : requests) {
            if (request.getBuffers() != null) {
              writer.writeInt(request.getBuffers().size());
              for (byte[] buffer : request.getBuffers()) {
                countLogged.incrementAndGet();
                writer.write(buffer);
              }
            }
            else {
              writer.writeInt(1);
              writer.write(request.getBuffer());
              countLogged.incrementAndGet();
            }
          }
          writer.flush();

          for (DatabaseServer.LogRequest request : requests) {
            request.getLatch().countDown();
          }
        }
        catch (Exception t) {
          logger.error("Error processing log requests", t);
        }
      }
    }

  }

  private String getLogRoot() {
    return new File(server.getDataDir(), "queue/" + server.getShard() + "/" + server.getReplica()).getAbsolutePath();
  }

  public void bindQueues() {
    unbindQueues.set(false);
  }

  public void unbindQueues() {
    unbindQueues.set(true);
  }

  public void applyQueues() {

    unbindQueues();
    try {

      String dataRoot = new File(server.getDataDir(), "queue/" + server.getShard() + "/" + server.getReplica()).getAbsolutePath();
      File dataRootDir = new File(dataRoot);
      dataRootDir.mkdirs();

      long begin = System.currentTimeMillis();
      replayQueues(dataRootDir);
      long end = System.currentTimeMillis();

      logger.info("Finished replaying queue: duration=" + (end - begin));
    }
    catch (IOException e) {
      logger.error("Error", e);
    }
    finally {
      bindQueues();
    }
  }

  class LogSource {
    DataInputStream in;
    long sequenceNumber;
    String command;
    byte[] buffer;
    List<NettyServer.Request> requests;
    DataUtil.ResultLength resultLength = new DataUtil.ResultLength();

    LogSource(File file) throws IOException {
      InputStream inputStream = null;
      if (file.getName().contains(".gz")) {
        inputStream = new GZIPInputStream(new BufferedInputStream(new FileInputStream(file)));
      }
      else {
        inputStream = new BufferedInputStream(new FileInputStream(file));
      }
      in = new DataInputStream(inputStream);

      readNext();
    }

    boolean take() {
      readNext();
      return command != null || requests != null;
    }

    void readNext() {
      try {
        int count = in.readInt();
        if (count == 1) {
          NettyServer.Request request = readRequest();
          command = request.getCommand();
          buffer = request.getBody();
          requests = null;
        }
        else {
          requests = new ArrayList<>();
          for (int i = 0; i < count; i++) {
            requests.add(readRequest());
          }
          command = null;
          buffer = null;
        }
      }
      catch (IOException e) {
        command = null;
        buffer = null;
        sequenceNumber = -1;
        requests = null;
        try {
          in.close();
        }
        catch (IOException e1) {
          logger.error("Error closing stream", e1);
        }
      }
    }

    private NettyServer.Request readRequest() throws IOException {
      sequenceNumber = DataUtil.readVLong(in, resultLength);
      int size = in.readInt();
      byte[] commandBuffer = new byte[size];
      in.readFully(commandBuffer);
      String command = new String(commandBuffer, UTF8_STR);
      String[] parts = command.split(":");
      String dbName = parts[4];
      StringBuilder builder = new StringBuilder();
      for (int i = 0; i < parts.length; i++) {
        if (i != 0) {
          builder.append(":");
        }
        if (i == 3) {
          builder.append(server.getCommon().getSchemaVersion());
        }
        else {
          builder.append(parts[i]);
        }
      }
      command = builder.toString();
      size = in.readInt();
      byte[] buffer = size == 0 ? null : new byte[size];
      if (buffer != null) {
        in.readFully(buffer);
      }
      NettyServer.Request ret = new NettyServer.Request();
      ret.setCommand(command);
      ret.setBody(buffer);
      return ret;
    }

    public void close() throws IOException {
      in.close();
    }
  }

  private void replayQueues(File dataRootDir) throws IOException {
    synchronized (logLock) {
      File[] files = dataRootDir.listFiles();
      if (files != null) {
        final AtomicInteger countProcessed = new AtomicInteger();
        logger.info("applyQueues - begin: fileCount=" + files.length);
        List<LogSource> sources = new ArrayList<>();
        for (final File file : files) {
          try {
            sources.add(new LogSource(file));
          }
          catch (IOException e) {
            logger.error("Error opening log source: name=" + file.getName());
          }
        }
        final long begin = System.currentTimeMillis();
        final AtomicLong lastLogged = new AtomicLong(System.currentTimeMillis());
        try {
          while (true) {
            long minSeqenceNum = Long.MAX_VALUE;
            int minOffset = -1;
            for (int i = 0; i < sources.size(); i++) {
              long seq = sources.get(i).sequenceNumber;
              if (seq < minSeqenceNum) {
                minSeqenceNum = seq;
                minOffset = i;
              }
            }
            if (minOffset == -1) {
              break;
            }
            final LogSource minSource = sources.get(minOffset);
            try {
              if (minSource.requests != null) {
                List<Future> futures = new ArrayList<>();
                for (final NettyServer.Request request : minSource.requests) {
                  futures.add(executor.submit(new Callable() {
                    @Override
                    public Object call() throws Exception {
                      try {
                        server.handleCommand(request.getCommand(), request.getBody(), true, false);
                        countProcessed.incrementAndGet();
                        if (System.currentTimeMillis() - lastLogged.get() > 2000) {
                          lastLogged.set(System.currentTimeMillis());
                          logger.info("applyQueues - progress: count=" + countProcessed.get() +
                              ", rate=" + (double) countProcessed.get() / (double) (System.currentTimeMillis() - begin) * 1000d);
                        }
                      }
                      catch (Exception e) {
                        logger.error("Error replaying command: command=" + minSource.command, e);
                      }
                      return null;
                    }
                  }));
                }
                for (Future future : futures) {
                  future.get();
                }
              }
              else {
                server.handleCommand(minSource.command, minSource.buffer, true, false);
                countProcessed.incrementAndGet();
                if (System.currentTimeMillis() - lastLogged.get() > 2000) {
                  lastLogged.set(System.currentTimeMillis());
                  logger.info("applyQueues - progress: count=" + countProcessed.get() +
                      ", rate=" + (double) countProcessed.get() / (double) (System.currentTimeMillis() - begin) * 1000d);
                }
              }
            }
            catch (Exception t) {
              logger.error("Error replaying command: command=" + minSource.command, t);
            }
            finally {
              if (!minSource.take()) {
                sources.remove(minOffset);
              }
            }
          }

        }
        finally {
          logger.info("applyQueues - finished: count=" + countProcessed.get() +
              ", rate=" + (double) countProcessed.get() / (double) (System.currentTimeMillis() - begin) * 1000d);

          for (LogSource source : sources) {
            source.close();
          }
        }
        logger.info("applyQueue commandCount=" + countProcessed.get());
      }
    }
  }

  public DatabaseServer.LogRequest logRequests(List<NettyServer.Request> requests, boolean enableQueuing) throws IOException, InterruptedException {
    if (enableQueuing) {
      DatabaseServer.LogRequest logRequest = null;
      List<byte[]> serializedCommands = new ArrayList<>();
      for (NettyServer.Request request : requests) {
        String command = request.getCommand();
        byte[] body = request.getBody();
        int pos = command.indexOf(':');
        int pos2 = command.indexOf(':', pos + 1);
        String methodStr = null;
        if (pos2 == -1) {
          methodStr = command.substring(pos + 1);
        }
        else {
          methodStr = command.substring(pos + 1, pos2);
        }
        if (DatabaseClient.getWriteVerbs().contains(methodStr)) {

          ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
          DataOutputStream out = new DataOutputStream(bytesOut);
          byte[] buffer = command.getBytes(UTF8_STR);

          DataUtil.ResultLength resultLength = new DataUtil.ResultLength();
          DataUtil.writeVLong(out, getNextSequencenNum(), resultLength);
          out.writeInt(buffer.length);
          out.write(buffer);
          out.writeInt(body == null ? 0 : body.length);
          if (body != null && body.length != 0) {
            out.write(body);
          }
          serializedCommands.add(bytesOut.toByteArray());

//          logRequest = new DatabaseServer.LogRequest();
//          logRequest.setBuffer(bytesOut.toByteArray());//setBuffers(serializedCommands);
//          logRequests.put(logRequest);
        }
      }
      if (serializedCommands.size() != 0) {
        DatabaseServer.LogRequest request = new DatabaseServer.LogRequest();
        request.setBuffers(serializedCommands);
        logRequests.put(request);
        return request;
      }
    }
    return null;
  }


  public DatabaseServer.LogRequest logRequest(String command, byte[] body, boolean enableQueuing, String methodStr, DatabaseServer.LogRequest request) {
    try {
      if (enableQueuing && DatabaseClient.getWriteVerbs().contains(methodStr)) {

        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytesOut);
        byte[] buffer = command.getBytes(UTF8_STR);

        DataUtil.ResultLength resultLength = new DataUtil.ResultLength();
        DataUtil.writeVLong(out, getNextSequencenNum(), resultLength);
        out.writeInt(buffer.length);
        out.write(buffer);
        out.writeInt(body == null ? 0 : body.length);
        if (body != null && body.length != 0) {
          out.write(body);
        }
        request = new DatabaseServer.LogRequest();
        request.setBuffer(bytesOut.toByteArray());
        logRequests.put(request);
      }
      return request;
    }
    catch (InterruptedException | IOException e) {
      throw new DatabaseException(e);
    }
  }

  public void deleteOldLogs(long lastSnapshot) {
    try {
      synchronized (logLock) {
        if (lastSnapshot != -1) {
          File[] files = new File(getLogRoot()).listFiles();
          if (files != null) {
            for (File file : files) {
              String name = file.getName();
              if (name.contains("in-process")) {
                continue;
              }
              int pos = name.indexOf('-');
              int pos2 = name.lastIndexOf('.');
              String dateStr = name.substring(pos + 1, pos2);
              Date fileDate = ISO8601.from8601String(dateStr).getTime();
              long fileTime = fileDate.getTime();
              if (fileTime < lastSnapshot - (30 * 1000) && file.exists() && !file.delete()) {
                throw new DatabaseException("Error deleting file: file=" + file.getAbsolutePath());
              }
            }
          }
        }
      }
    }
    catch (ParseException e) {
      throw new DatabaseException(e);
    }
  }
}
