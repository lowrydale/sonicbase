package com.sonicbase.server;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.*;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.util.DateUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.giraph.utils.Varint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPInputStream;

import static com.sonicbase.client.DatabaseClient.SERIALIZATION_VERSION_26;

@SuppressWarnings({"squid:S1172", "squid:S1168", "squid:S00107"})
// all methods called from method invoker must have cobj and replayed command parms
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class LogManager {

  private static final String ERROR_REPLAYING_REQUEST_STR = "Error replaying request";
  private static final String PEER_STR = "peer-";
  private static final String NONE_STR = "__none__";
  private final List<LogWriter> logWriters = new ArrayList<>();
  private final List<LogWriter> peerLogWriters = new ArrayList<>();
  private static Logger logger = LoggerFactory.getLogger(LogManager.class);

  private final com.sonicbase.server.DatabaseServer databaseServer;
  private final ThreadPoolExecutor executor;
  private final File rootDir;
  private final List<Thread> logwWriterThreads = new ArrayList<>();

  private long cycleLogsMillis = 60_000;
  private AtomicLong countLogged = new AtomicLong();
  private final com.sonicbase.server.DatabaseServer server;
  private ArrayBlockingQueue<LogRequest> logRequests = new ArrayBlockingQueue<>(2_000);
  private Map<Integer, ArrayBlockingQueue<LogRequest>> peerLogRequests = new ConcurrentHashMap<>();
  private AtomicBoolean unbindQueues = new AtomicBoolean();
  private final Object logLock = new Object();
  private AtomicLong logSequenceNumber = new AtomicLong();
  private AtomicLong maxAllocatedLogSequenceNumber = new AtomicLong();
  private static final int SEQUENCE_NUM_ALLOC_COUNT = 100000;
  private boolean shouldSlice = false;
  private boolean shutdown;
  final AtomicInteger countReplayed = new AtomicInteger();
  private AtomicLong countRead = new AtomicLong();

  public LogManager(com.sonicbase.server.DatabaseServer databaseServer, File rootDir) {
    this.databaseServer = databaseServer;
    this.server = databaseServer;
    this.rootDir = rootDir;
    executor = ThreadUtil.createExecutor(Runtime.getRuntime().availableProcessors() * 8,
        "SonicBase LogManager Thread");

    synchronized (this) {
      try {
        skipToMaxSequenceNumber();
      }
      catch (IOException e) {
        throw new DatabaseException(e);
      }
    }
    int logThreadCount = 4;
    for (int i = 0; i < logThreadCount; i++) {
      LogWriter logWriter = new LogWriter(i, -1, logRequests);
      logWriters.add(logWriter);
      Thread thread = ThreadUtil.createThread(logWriter, "SonicBase Log Writer Thread");
      logwWriterThreads.add(thread);
      thread.start();
    }
  }

  public static class LogRequest {
    private byte[] buffer;
    private CountDownLatch latch = new CountDownLatch(1);
    private List<byte[]> buffers;
    private long[] sequenceNumbers;
    private long[] times;
    private long begin;
    private AtomicLong timeLogging;

    LogRequest(int size) {
      this.sequenceNumbers = new long[size];
      this.times = new long[size];
    }

    byte[] getBuffer() {
      return buffer;
    }

    void setBuffer(byte[] buffer) {
      this.buffer = buffer;
    }

    public CountDownLatch getLatch() {
      return latch;
    }

    public List<byte[]> getBuffers() {
      return buffers;
    }

    long[] getSequences1() {
      return sequenceNumbers;
    }

    long[] getSequences0() {
      return times;
    }

    public void setBegin(long begin) {
      this.begin = begin;
    }

    void setTimeLogging(AtomicLong timeLogging) {
      this.timeLogging = timeLogging;
    }

    AtomicLong getTimeLogging() {
      return timeLogging;
    }

    public long getBegin() {
      return begin;
    }
  }

  public void setCycleLogsMillis(long newMillis) {
    this.cycleLogsMillis = newMillis;
  }

  public void shutdown() {
    this.shutdown = true;
    executor.shutdownNow();
    for (Thread thread : logwWriterThreads) {
      thread.interrupt();
      try {
        thread.join();
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new DatabaseException(e);
      }
    }
    for (LogWriter writer : logWriters) {
      writer.shutdown();
    }
  }


  public void startLoggingForPeer(int replicaNum) {
    synchronized (peerLogRequests) {
      if (!peerLogRequests.containsKey(replicaNum)) {
        peerLogRequests.put(replicaNum, new ArrayBlockingQueue<LogRequest>(1000));
        LogWriter logWriter = new LogWriter(0, replicaNum, peerLogRequests.get(replicaNum));
        peerLogWriters.add(logWriter);
        Thread thread = new Thread(logWriter);
        thread.start();
      }
    }
  }

  void skipToMaxSequenceNumber() throws IOException {
    File file = new File(rootDir, "logSequenceNum" + File.separator + databaseServer.getShard() + File.separator +
        databaseServer.getReplica() + File.separator + "logSequenceNum.txt");
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
          seq = IOUtils.toString(reader);
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
      pushMaxSequenceNum(null, false);
    }
  }

  public ComObject setMaxSequenceNum(ComObject cobj, boolean replayedCommand) {
    try {
      long sequenceNum = cobj.getLong(ComObject.Tag.SEQUENCE_NUMBER);

      maxAllocatedLogSequenceNumber.set(sequenceNum);
      File file = new File(rootDir, "logSequenceNum" + File.separator + databaseServer.getShard() + File.separator +
          databaseServer.getReplica() + File.separator + "logSequenceNum.txt");
      file.getParentFile().mkdirs();
      try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))) {
        writer.write(String.valueOf(maxAllocatedLogSequenceNumber.get()));
      }
      return null;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  long getNextSequencenNum() {
    return System.nanoTime();
  }

  public void pushMaxSequenceNum(ComObject cobj, boolean replayedCommand) {
    for (int replica = 0; replica < server.getReplicationFactor(); replica++) {
      if (replica != server.getReplica()) {
        try {
          ComObject localCobj = new ComObject();
          localCobj.put(ComObject.Tag.DB_NAME, NONE_STR);
          localCobj.put(ComObject.Tag.SCHEMA_VERSION, server.getCommon().getSchemaVersion());
          localCobj.put(ComObject.Tag.SEQUENCE_NUMBER, maxAllocatedLogSequenceNumber.get());
          server.getClient().send("LogManager:setMaxSequenceNum", server.getShard(), replica, localCobj,
              DatabaseClient.Replica.SPECIFIED, true);
        }
        catch (Exception e) {
          logger.error("Error setting maxSequenceNum: shard={}, replica={}", server.getShard(), replica);
        }
      }
    }
  }

  void replayLogs() {
    applyLogs();
  }

  String sliceLogs(boolean includePeers) {
    try {
      File dataRootDir = getLogReplicaDir();
      dataRootDir = new File(dataRootDir, "self");

      StringBuilder sliceFiles = new StringBuilder();
      File[] files = dataRootDir.listFiles();
      if (files != null) {
        for (File file : files) {
          sliceFiles.append(file.getAbsolutePath()).append("\n");
        }
      }
      sliceLogsForPeers(includePeers, sliceFiles);

      for (LogWriter logWriter : logWriters) {
        logWriter.closeAndCreateLog();
      }

      for (LogWriter logWriter : peerLogWriters) {
        logWriter.closeAndCreateLog();
      }

      return sliceFiles.toString();
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private void sliceLogsForPeers(boolean includePeers, StringBuilder sliceFiles) {
    File[] files;
    if (includePeers) {
      for (int replica = 0; replica < server.getReplicationFactor(); replica++) {
        files = new File(getLogRoot() + File.separator + PEER_STR + replica).listFiles();
        if (files != null) {
          for (File file : files) {
            sliceFiles.append(file.getAbsolutePath()).append("\n");
          }
        }
      }
    }
  }

  public void deleteLogs() {
    File dir = getLogReplicaDir();
    try {
      FileUtils.deleteDirectory(dir);
      dir.mkdirs();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }

  }

  public ComObject getLogFile(ComObject cobj, boolean replayedCommand) {
    try {
      ComObject retObj = new ComObject();
      int replica = cobj.getInt(ComObject.Tag.REPLICA);
      String filename = cobj.getString(ComObject.Tag.FILENAME);
      File file = new File(getLogRoot() + File.separator + PEER_STR + replica + File.separator + filename);
      try (InputStream in = new BufferedInputStream(new FileInputStream(file));
           ByteArrayOutputStream out = new ByteArrayOutputStream()) {
        IOUtils.copy(in, out);
        retObj.put(ComObject.Tag.BINARY_FILE_CONTENT, out.toByteArray());
      }

      return retObj;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public ComObject deletePeerLogs(ComObject cobj, boolean replayedCommand) {
    deletePeerLogs(cobj.getInt(ComObject.Tag.REPLICA));
    return null;
  }

  public ComObject sendLogsToPeer(ComObject cobj, boolean replayedCommand) {
    int replicaNum = cobj.getInt(ComObject.Tag.REPLICA);

    try {
      ComObject retObj = new ComObject();
      File[] files = new File(getLogRoot() + File.separator + PEER_STR + replicaNum).listFiles();
      if (files != null) {
        ComArray fileNameArray = retObj.putArray(ComObject.Tag.FILENAMES, ComObject.Type.STRING_TYPE);
        for (File file : files) {
          fileNameArray.add(file.getName());
        }
      }
      return retObj;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private void deletePeerLogs(int replicaNum) {
    File dir = new File(getLogRoot() + File.separator + PEER_STR + replicaNum);
    logger.info("Deleting peer logs: dir={}", dir.getAbsolutePath());
    File[] files = dir.listFiles();
    int count = 0;
    if (files != null) {
      for (File file : files) {
        try {
          if (file.exists()) {
            Files.delete(file.toPath());
          }
        }
        catch (IOException e) {
          logger.error("error deleting peer log file", e);
        }
        count++;
      }
    }
    logger.info("Deleted peer logs: count={}, dir={}", count, dir.getAbsolutePath());
  }

  void logRequestForPeer(byte[] request, String methodStr, long sequence0, long sequence1, int deadReplica) {
    startLoggingForPeer(deadReplica);

    try {
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytesOut);

      Varint.writeSignedVarLong(DatabaseClient.SERIALIZATION_VERSION, out);
      out.writeUTF(methodStr);
      Varint.writeSignedVarLong(sequence0, out);
      Varint.writeSignedVarLong(sequence1, out);
      out.writeInt(request.length);
      out.write(request);

      LogRequest logRequest = new LogRequest(1);
      logRequest.setBuffer(bytesOut.toByteArray());
      peerLogRequests.get(deadReplica).put(logRequest);
      logRequest.getLatch().await();
    }
    catch (InterruptedException | IOException e) {
      throw new DatabaseException(e);
    }
  }

  public ComObject sendQueueFile(ComObject cobj, boolean replayedCommand) {
    int peerReplica = cobj.getInt(ComObject.Tag.REPLICA);
    String filename = cobj.getString(ComObject.Tag.FILENAME);
    byte[] bytes = cobj.getByteArray(ComObject.Tag.BINARY_FILE_CONTENT);
    return sendQueueFile(peerReplica, filename, bytes);
  }

  private ComObject sendQueueFile(int peerReplica, String filename, byte[] bytes) {
    try {
      String directory = getLogRoot();
      File dataRootDir = new File(directory);
      dataRootDir.mkdirs();
      File file = new File(dataRootDir, "self/peer-" + peerReplica + "-" + filename);
      try (BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(file))) {
        out.write(bytes);
      }
      return null;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public boolean hasLogsForPeer(int replica) {
    File[] files = new File(getLogRoot() + "/peer-" + replica).listFiles();
    if (files != null) {
      return files.length > 0;
    }
    return false;
  }

  double getPercentApplyQueuesComplete() {
    long totalBytes = 0;
    long readBytes = countRead.get();
    for (LogSource source : allCurrentSources) {
      totalBytes += source.getTotalBytes();
    }
    if (totalBytes == 0) {
      return 0;
    }
    return (double) readBytes / (double) totalBytes;
  }

  public class LogWriter implements Runnable {
    private final int offset;
    private final ArrayBlockingQueue<LogRequest> currLogRequests;
    private final int peerReplicaNum;
    private long currQueueTime = 0;
    private DataOutputStream writer = null;
    private boolean shutdown;
    private AtomicReference<String> currFilename = new AtomicReference<>();


    public LogWriter(int offset, int peerReplicaNum, ArrayBlockingQueue<LogRequest> logRequests) {
      this.offset = offset;
      this.peerReplicaNum = peerReplicaNum;
      this.currLogRequests = logRequests;
    }

    @Override
    public void run() {
      while (!shutdown && !Thread.interrupted()) {
        try {
          List<LogRequest> requests = new ArrayList<>();
          requests.add(currLogRequests.take());
          currLogRequests.drainTo(requests, 100);

          logRequests(requests);

          for (LogRequest request : requests) {
            if (request.getTimeLogging() != null) {
              request.getTimeLogging().addAndGet(System.nanoTime() - request.getBegin());
            }
            request.getLatch().countDown();
          }
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
        catch (Exception t) {
          logger.error("Error processing log requests", t);
        }
      }
    }

    void logRequests(List<LogRequest> requests) throws IOException {
      synchronized (this) {
        if (shouldSlice || writer == null || System.currentTimeMillis() - cycleLogsMillis > currQueueTime) {
          closeAndCreateLog();
        }

        for (LogRequest request : requests) {
          writer.writeInt(1);
          writer.write(request.getBuffer());
          countLogged.incrementAndGet();
        }
        writer.flush();
      }
    }

    private void closeAndCreateLog() throws IOException {
      synchronized (this) {
        if (writer != null) {
          writer.close();
        }
        String directory = getLogRoot();
        currQueueTime = System.currentTimeMillis();
        File dataRootDir = new File(directory);
        dataRootDir.mkdirs();
        String dt = DateUtils.toString(new Date(System.currentTimeMillis()));
        long nano = System.nanoTime();
        File newFile = null;
        if (peerReplicaNum == -1) {
          newFile = new File(dataRootDir, "/self/" + offset + "-" + dt + "-" + nano + ".bin");
          if (newFile.getAbsolutePath().equals(currFilename.get()) || newFile.exists()) {
            throw new DatabaseException("Reusing same file: filename=" + newFile.getAbsolutePath());
          }
        }
        else {
          newFile = new File(dataRootDir.getAbsolutePath() + "/peer-" + peerReplicaNum, offset +
              "-" + dt + "-" + nano + ".bin");
        }
        newFile.getParentFile().mkdirs();
        currFilename.set(newFile.getAbsolutePath());
        writer = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(newFile), 102400));
        shouldSlice = false;
      }
    }

    public void shutdown() {
      this.shutdown = true;
      if (writer != null) {
        try {
          writer.close();
        }
        catch (IOException e) {
          throw new DatabaseException(e);
        }
      }

    }
  }

  private String getLogRoot() {
    return getLogReplicaDir().getAbsolutePath();
  }

  private void bindQueues() {
    unbindQueues.set(false);
  }

  private void unbindQueues() {
    unbindQueues.set(true);
  }

  public void applyLogs() {
    unbindQueues();
    try {

      String dataRoot = getLogReplicaDir().getAbsolutePath();
      File dataRootDir = new File(dataRoot, "self");
      dataRootDir.mkdirs();

      long begin = System.currentTimeMillis();
      for (int replica = 0; replica < server.getReplicationFactor(); replica++) {
        if (replica != server.getReplica()) {
          try {
            getLogsFromPeer(replica);
          }
          catch (Exception e) {
            logger.error("Error getting logs from peer: replica=" + replica, e);
          }
        }
      }

      replayLogs(dataRootDir, null, false, false);

      long end = System.currentTimeMillis();

      logger.info("Finished replaying queue: duration={}", (end - begin));
    }
    catch (IOException e) {
      logger.error("Error", e);
    }
    finally {
      bindQueues();
    }
  }

  void getLogsFromPeer(int replica) {
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, NONE_STR);
    cobj.put(ComObject.Tag.SCHEMA_VERSION, server.getCommon().getSchemaVersion());
    cobj.put(ComObject.Tag.METHOD, "LogManager:sendLogsToPeer");
    cobj.put(ComObject.Tag.REPLICA, server.getReplica());
    AtomicBoolean isHealthy = new AtomicBoolean();
    try {
      server.checkHealthOfServer(server.getShard(), replica, isHealthy);
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return;
    }
    if (isHealthy.get()) {
      byte[] ret = server.getClient().send(null, server.getShard(), replica, cobj, DatabaseClient.Replica.SPECIFIED);
      ComObject retObj = new ComObject(ret);
      ComArray filenames = retObj.getArray(ComObject.Tag.FILENAMES);
      if (filenames != null) {
        for (int i = 0; i < filenames.getArray().size(); i++) {
          String filename = (String) filenames.getArray().get(i);
          cobj = new ComObject();
          cobj.put(ComObject.Tag.DB_NAME, NONE_STR);
          cobj.put(ComObject.Tag.SCHEMA_VERSION, server.getCommon().getSchemaVersion());
          cobj.put(ComObject.Tag.METHOD, "LogManager:getLogFile");
          cobj.put(ComObject.Tag.REPLICA, server.getReplica());
          cobj.put(ComObject.Tag.FILENAME, filename);

          ret = server.getClient().send(null, server.getShard(), replica, cobj, DatabaseClient.Replica.SPECIFIED);
          retObj = new ComObject(ret);
          byte[] bytes = retObj.getByteArray(ComObject.Tag.BINARY_FILE_CONTENT);

          sendQueueFile(replica, filename, bytes);
          logger.info("Received log file: filename={}, replica={}", filename, replica);
        }
        cobj = new ComObject();
        cobj.put(ComObject.Tag.DB_NAME, NONE_STR);
        cobj.put(ComObject.Tag.SCHEMA_VERSION, server.getCommon().getSchemaVersion());
        cobj.put(ComObject.Tag.METHOD, "LogManager:deletePeerLogs");
        cobj.put(ComObject.Tag.REPLICA, server.getReplica());
        server.getClient().send(null, server.getShard(), replica, cobj, DatabaseClient.Replica.SPECIFIED);
      }
    }
  }

  private File getLogReplicaDir() {
    return new File(rootDir, server.getShard() + File.separator + server.getReplica());
  }

  public static class ByteCounterStream extends InputStream {
    private final AtomicLong countRead;
    private final InputStream in;

    ByteCounterStream(InputStream in, AtomicLong countRead) {
      this.in = in;
      this.countRead = countRead;
    }

    @Override
    public int read() throws IOException {
      countRead.incrementAndGet();
      return in.read();
    }

    public long getCount() {
      return countRead.get();
    }
  }

  public static class LogSource {
    private long totalBytes;
    private ByteCounterStream counterStream;
    DataInputStream in;
    long sequence1;
    long sequence0;
    byte[] buffer;
    List<NettyServer.Request> requests;
    private String methodStr;

    LogSource(File file, com.sonicbase.server.DatabaseServer server, Logger logger, AtomicLong countRead) throws IOException {
      InputStream inputStream = null;
      if (file.getName().contains(".gz")) {
        inputStream = new GZIPInputStream(new BufferedInputStream(new FileInputStream(file)));
      }
      else {
        inputStream = new BufferedInputStream(new FileInputStream(file));
      }
      counterStream = new ByteCounterStream(inputStream, countRead);
      in = new DataInputStream(counterStream);
      totalBytes = file.length();
      readNext(server, logger);
    }

    long getTotalBytes() {
      return this.totalBytes;
    }

    boolean take(com.sonicbase.server.DatabaseServer server, Logger logger) {
      readNext(server, logger);
      return buffer != null;
    }

    void readNext(com.sonicbase.server.DatabaseServer server, Logger logger) {
      try {
        int count = in.readInt();
        if (count == 1) {
          readRequest(server);
          requests = null;
        }
        else {
          throw new DatabaseException("Processing batch");
        }
      }
      catch (EOFException e) {
        buffer = null;
        sequence1 = -1;
        sequence0 = -1;
        methodStr = null;
        requests = null;
        try {
          in.close();
        }
        catch (IOException e1) {
          logger.error("Error closing stream", e1);
        }
      }
      catch (Exception e) {
        logger.error("Error reading log entry", e);
        buffer = null;
        sequence1 = -1;
        sequence0 = -1;
        methodStr = null;
        requests = null;
        try {
          in.close();
        }
        catch (IOException e1) {
          logger.error("Error closing stream", e1);
        }
      }
    }

    private void readRequest(DatabaseServer server) throws IOException {
      boolean readAll = false;
      try {
        short serializationVersion = (short) Varint.readSignedVarLong(in);
        if (serializationVersion >= SERIALIZATION_VERSION_26) {
          methodStr = in.readUTF();
        }
        else {
          methodStr = "";
        }
        sequence0 = Varint.readSignedVarLong(in);
        sequence1 = Varint.readSignedVarLong(in);
        int size = in.readInt();
        if (size == 0) {
          throw new DatabaseException("Invalid size: size=0");
        }
        buffer = new byte[size];
        in.readFully(buffer);
        ComObject cobj = new ComObject(buffer);
        cobj.put(ComObject.Tag.SCHEMA_VERSION, server.getCommon().getSchemaVersion());
        buffer = cobj.serialize();

        readAll = true;
      }
      finally {
        if (!readAll) {
          throw new DatabaseException("Didn't read entire record");
        }
      }
    }

    public void close() throws IOException {
      in.close();
    }

    public byte[] getBuffer() {
      return buffer;
    }
  }

  void applyLogsFromPeers(String slicePoint) {
    try {
      String dataRoot = getLogReplicaDir().getAbsolutePath();
      File dataRootDir = new File(dataRoot, "self");
      dataRootDir.mkdirs();

      replayLogs(dataRootDir, null, false, true);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  void applyLogsAfterSlice(String slicePoint) {
    try {
      String dataRoot = getLogReplicaDir().getAbsolutePath();
      File dataRootDir = new File(dataRoot, "self");
      dataRootDir.mkdirs();

      replayLogs(dataRootDir, slicePoint, false, false);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private List<LogSource> allCurrentSources = new ArrayList<>();

  public interface LogVisitor {
    boolean visit(byte[] buffer);
  }

  private void replayLogs(File dataRootDir, final String slicePoint, final boolean beforeSlice, boolean peerFiles) throws IOException {
    ThreadPoolExecutor localExecutor = ThreadUtil.createExecutor(32, "SonicBase LogManager replayLogs Thread");
    try {
      countReplayed.set(0);
      allCurrentSources.clear();
      synchronized (logLock) {
        File[] files = dataRootDir.listFiles();
        if (files == null) {
          logger.warn("No files to restore: shard={}, replica={}", server.getShard(), server.getReplica());
        }
        else {
          logger.info("applyLogs - begin: fileCount={}", files.length);
          List<LogSource> sources = new ArrayList<>();
          Set<String> sliceFiles = new HashSet<>();

          getSliceFiles(slicePoint, sliceFiles);

          addSources(slicePoint, beforeSlice, peerFiles, files, sources, sliceFiles);

          final long begin = System.currentTimeMillis();
          final AtomicLong lastLogged = new AtomicLong(System.currentTimeMillis());
          try {
            List<NettyServer.Request> batch = new ArrayList<>();
            while (!shutdown) {
              ProcessSource processSource = new ProcessSource(sources, begin, lastLogged, batch).invoke();
              batch = processSource.getBatch();
              if (processSource.is()) {
                break;
              }
            }

            flushBatch(begin, lastLogged, batch);
          }
          finally {
            logger.info("applyLogs - finished: count={}, rate={}", countReplayed.get(),
                (double) countReplayed.get() / (double) (System.currentTimeMillis() - begin) * 1000d);

            for (LogSource source : allCurrentSources) {
              source.close();
            }
            allCurrentSources.clear();
          }
          logger.info("applyQueue requestCount={}", countReplayed.get());
        }
      }
    }
    finally {
      localExecutor.shutdownNow();
    }
  }

  private void getSliceFiles(String slicePoint, Set<String> sliceFiles) throws IOException {
    if (slicePoint != null) {
      BufferedReader reader = new BufferedReader(new StringReader(slicePoint));
      while (true) {
        String line = reader.readLine();
        if (line == null) {
          break;
        }
        sliceFiles.add(line);
      }
    }
  }


  private void addSources(String slicePoint, boolean beforeSlice, boolean peerFiles, File[] files,
                          List<LogSource> sources, Set<String> sliceFiles) throws IOException {
    for (File file : files) {
      if (peerFiles && !file.getName().startsWith("peer")) {
        continue;
      }
      if (slicePoint == null) {
        LogSource src = new LogSource(file, server, logger, countRead);
        sources.add(src);
        allCurrentSources.add(src);
        continue;
      }
      if (beforeSlice && sliceFiles.contains(file.getAbsolutePath())) {
        LogSource src = new LogSource(file, server, logger, countRead);
        sources.add(src);
        allCurrentSources.add(src);
      }

      if (!beforeSlice && !sliceFiles.contains(file.getAbsolutePath())) {
        LogSource src = new LogSource(file, server, logger, countRead);
        sources.add(src);
        allCurrentSources.add(src);
      }
    }
  }

  private void flushBatch(final long begin, final AtomicLong lastLogged,
                          final List<NettyServer.Request> finalBatch) {
    List<Future> futures = new ArrayList<>();
    for (final NettyServer.Request currRequest : finalBatch) {
      futures.add(executor.submit((Callable) () -> {
        try {
          server.invokeMethod(currRequest.getBody(), currRequest.getSequence0(), currRequest.getSequence1(),
              true, false, null, null);
          countReplayed.incrementAndGet();
          if (System.currentTimeMillis() - lastLogged.get() > 2000) {
            lastLogged.set(System.currentTimeMillis());
            logger.info("applyLogs - SINGLE request - progress: count=" + countReplayed.get() +
                ", rate=" + (double) countReplayed.get() / (double) (System.currentTimeMillis() - begin) * 1000d);
          }
        }
        catch (Exception e) {
          logger.error(ERROR_REPLAYING_REQUEST_STR, e);
        }
        return null;
      }));
    }
    for (Future future : futures) {
      try {
        future.get();
      }
      catch (Exception e) {
        logger.error(ERROR_REPLAYING_REQUEST_STR, e);
      }
    }
  }

  public LogRequest logRequest(byte[] body, boolean enableQueuing, String methodStr,
                               Long existingSequence0, Long existingSequence1, AtomicLong timeLogging) {
    LogRequest request = null;
    try {
      if (enableQueuing && DatabaseClient.getWriteVerbs().contains(methodStr)) {
        request = new LogRequest(1);
        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytesOut);

        long sequence1 = 0;
        long sequence0 = 0;
        if (existingSequence0 != null) {
          sequence0 = existingSequence0;
        }
        else {
          sequence0 = System.currentTimeMillis();
        }
        if (existingSequence1 != null) {
          sequence1 = existingSequence1;
        }
        else {
          sequence1 = getNextSequencenNum();
        }

        Varint.writeSignedVarLong(DatabaseClient.SERIALIZATION_VERSION, out);
        out.writeUTF(methodStr);
        Varint.writeSignedVarLong(sequence0, out);
        Varint.writeSignedVarLong(sequence1, out);
        out.writeInt(body.length);
        out.write(body);

        request.getSequences0()[0] = sequence0;
        request.getSequences1()[0] = sequence1;
        request.setBuffer(bytesOut.toByteArray());
        request.setBegin(System.nanoTime());
        request.setTimeLogging(timeLogging);

        logRequests.put(request);
      }
      return request;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  void deleteOldLogs(long lastSnapshot, boolean exactDate) {
    synchronized (logLock) {
      if (lastSnapshot != -1) {
        File[] files = new File(getLogRoot(), "self").listFiles();
        if (files != null) {
          outer:
          for (File file : files) {
            deleteOldLogsProcessFile(lastSnapshot, exactDate, file);
          }
        }
      }
    }
  }

  private void deleteOldLogsProcessFile(long lastSnapshot, boolean exactDate, File file) {
    try {
      for (LogWriter logWriter : logWriters) {
        if (file.getAbsolutePath().equals(logWriter.currFilename.get())) {
          return;
        }
      }

      long fileTime = file.lastModified();
      if (exactDate) {
        if (fileTime < lastSnapshot && file.exists()) {
          Files.delete(file.toPath());
        }
      }
      else {
        if (fileTime < lastSnapshot - (10 * 1_000) && file.exists()) {
          Files.delete(file.toPath());
        }
      }
    }
    catch (Exception e) {
      logger.error("Error deleting log file: filename={}", file.getAbsolutePath());
    }
  }

  private class ProcessSource {
    private boolean myResult;
    private List<LogSource> sources;
    private long begin;
    private AtomicLong lastLogged;
    private List<NettyServer.Request> batch;

    public ProcessSource(List<LogSource> sources, long begin, AtomicLong lastLogged, List<NettyServer.Request> batch) {
      this.sources = sources;
      this.begin = begin;
      this.lastLogged = lastLogged;
      this.batch = batch;
    }

    boolean is() {
      return myResult;
    }

    public List<NettyServer.Request> getBatch() {
      return batch;
    }

    public ProcessSource invoke() {
      if (sources.isEmpty()) {
        myResult = true;
        return this;
      }
      LogSource minSource = sources.get(0);
      for (int i = 0; i < sources.size(); i++) {
        LogSource currSource = sources.get(i);
        if (currSource.buffer == null) {
          continue;
        }
        if (minSource.buffer == null || currSource.sequence0 < minSource.sequence0 ||
            currSource.sequence0 == minSource.sequence0 && currSource.sequence1 < minSource.sequence1) {
          minSource = currSource;
        }
      }
      if (minSource.buffer == null) {
        myResult = true;
        return this;
      }

      batch = addRequestToBatch(begin, lastLogged, batch, minSource);

      minSource.take(server, logger);
      myResult = false;
      return this;
    }

    private List<NettyServer.Request> addRequestToBatch(long begin, AtomicLong lastLogged,
                                                        List<NettyServer.Request> batch, LogSource minSource) {
      try {
        final byte[] buffer = minSource.buffer;
        final long sequence0 = minSource.sequence0;
        final long sequence1 = minSource.sequence1;
        final String methodStr = minSource.methodStr;

        NettyServer.Request request = new NettyServer.Request();
        request.setBody(buffer);
        request.setSequence0(sequence0);
        request.setSequence1(sequence1);

        if (!DatabaseClient.getParallelVerbs().contains(methodStr)) {
          flushBatch(begin, lastLogged, batch);
          batch = new ArrayList<>();
          batch.add(request);
          flushBatch(begin, lastLogged, batch);
          batch = new ArrayList<>();
        }
        else {
          batch.add(request);
          if (batch.size() > 64) {
            flushBatch(begin, lastLogged, batch);
            batch = new ArrayList<>();
          }
        }
      }
      catch (Exception t) {
        logger.error(ERROR_REPLAYING_REQUEST_STR, t);
      }
      return batch;
    }
  }
}
