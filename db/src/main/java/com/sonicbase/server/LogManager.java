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
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPInputStream;

import static com.sonicbase.client.DatabaseClient.SERIALIZATION_VERSION_26;

/**
 * Responsible for
 */
public class LogManager {

  private static final String UTF8_STR = "utf-8";
  private static boolean PARALLEL_APPLY_LOGS = true;
  private final List<LogWriter> logWriters = new ArrayList<>();
  private final List<LogWriter> peerLogWriters = new ArrayList<>();
  private static Logger logger = LoggerFactory.getLogger(ReadManager.class);

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
  private String sliceFilename;
  private boolean shouldSlice = false;
  private boolean didSlice = false;
  private boolean shutdown;
  final AtomicInteger countReplayed = new AtomicInteger();
  private AtomicLong countRead = new AtomicLong();

  public LogManager(com.sonicbase.server.DatabaseServer databaseServer, File rootDir) {
    this.databaseServer = databaseServer;
    this.server = databaseServer;
    this.rootDir = rootDir;
    executor = ThreadUtil.createExecutor(Runtime.getRuntime().availableProcessors() * 8, "SonicBase LogManager Thread");

    synchronized (this) {
      try {
        skipToMaxSequenceNumber();
      }
      catch (IOException e) {
        throw new DatabaseException(e);
      }
    }
    int logThreadCount = 4;//64;
    for (int i = 0; i < logThreadCount; i++) {
      LogWriter logWriter = new LogWriter(i, -1, logRequests, rootDir, server.getShard(), server.getReplica());
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

    public LogRequest(int size) {
      this.sequenceNumbers = new long[size];
      this.times = new long[size];
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "EI_EXPOSE_REP", justification = "copying the returned data is too slow")
    public byte[] getBuffer() {
      return buffer;
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "EI_EXPOSE_REP2", justification = "copying the passed in data is too slow")
    @SuppressWarnings("PMD.ArrayIsStoredDirectly") //copying the passed in data is too slow
    public void setBuffer(byte[] buffer) {
      this.buffer = buffer;
    }

    public CountDownLatch getLatch() {
      return latch;
    }

    public List<byte[]> getBuffers() {
      return buffers;
    }

    public long[] getSequences1() {
      return sequenceNumbers;
    }

    public long[] getSequences0() {
      return times;
    }

    public void setBegin(long begin) {
      this.begin = begin;
    }

    public void setTimeLogging(AtomicLong timeLogging) {
      this.timeLogging = timeLogging;
    }

    public AtomicLong getTimeLogging() {
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
        LogWriter logWriter = new LogWriter(0, replicaNum, peerLogRequests.get(replicaNum), rootDir, server.getShard(), server.getReplica());
        peerLogWriters.add(logWriter);
        Thread thread = new Thread(logWriter);
        thread.start();
      }
    }
  }

  public void skipToMaxSequenceNumber() throws IOException {
    File file = new File(rootDir, "logSequenceNum/" + databaseServer.getShard() + "/" + databaseServer.getReplica() + "/logSequenceNum.txt");
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
      long sequenceNum = cobj.getLong(ComObject.Tag.sequenceNumber);

      maxAllocatedLogSequenceNumber.set(sequenceNum);
      File file = new File(rootDir, "logSequenceNum/" + databaseServer.getShard() + "/" + databaseServer.getReplica() + "/logSequenceNum.txt");
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

  public long getNextSequencenNum() throws IOException {
    return System.nanoTime();
  }

  void pushMaxSequenceNum(ComObject cobj, boolean replayedCommand) {
    for (int replica = 0; replica < server.getReplicationFactor(); replica++) {
      if (replica != server.getReplica()) {
        try {
          cobj = new ComObject();
          cobj.put(ComObject.Tag.dbName, "__none__");
          cobj.put(ComObject.Tag.schemaVersion, server.getCommon().getSchemaVersion());
          cobj.put(ComObject.Tag.method, "LogManager:setMaxSequenceNum");
          cobj.put(ComObject.Tag.sequenceNumber, maxAllocatedLogSequenceNumber.get());
          server.getClient().send(null, server.getShard(), replica, cobj, DatabaseClient.Replica.specified, true);
        }
        catch (Exception e) {
          logger.error("Error setting maxSequenceNum: shard=" + server.getShard() + ", replica=" + replica);
        }
      }
    }
  }

  public void enableLogWriter(boolean enable) {
  }

  public void replayLogs() {
    applyLogs();
  }

  public long getCountLogged() {
    return countLogged.get();
  }

  public String sliceLogs(boolean includePeers) {
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
      if (includePeers) {
        for (int replica = 0; replica < server.getReplicationFactor(); replica++) {
          files = new File(getLogRoot() + "/peer-" + replica).listFiles();
          if (files != null) {
            for (File file : files) {
              sliceFiles.append(file.getAbsolutePath()).append("\n");
            }
          }
        }
      }

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
      int replica = cobj.getInt(ComObject.Tag.replica);
      String filename = cobj.getString(ComObject.Tag.filename);
      File file = new File(getLogRoot() + "/peer-" + replica + "/" + filename);
      InputStream in = new BufferedInputStream(new FileInputStream(file));
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      IOUtils.copy(in, out);
      out.close();

      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.binaryFileContent, out.toByteArray());

      return retObj;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public ComObject deletePeerLogs(ComObject cobj, boolean replayedCommand) {
    deletePeerLogs(cobj.getInt(ComObject.Tag.replica));
    return null;
  }

  public ComObject sendLogsToPeer(ComObject cobj, boolean replayedCommand) {
    int replicaNum = cobj.getInt(ComObject.Tag.replica);

    try {
      ComObject retObj = new ComObject();
      File[] files = new File(getLogRoot() + "/peer-" + replicaNum).listFiles();
      if (files != null) {
        ComArray fileNameArray = retObj.putArray(ComObject.Tag.filenames, ComObject.Type.stringType);
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

  public void deletePeerLogs(int replicaNum) {
    File dir = new File(getLogRoot() + "/peer-" + replicaNum);
    logger.info("Deleting peer logs: dir=" + dir.getAbsolutePath());
    File[] files = dir.listFiles();
    int count = 0;
    if (files != null) {
      for (File file : files) {
        file.delete();
        count++;
      }
    }
    logger.info("Deleted peer logs: count=" + count + ", dir=" + dir.getAbsolutePath());
  }

  public void logRequestForPeer(byte[] request, String methodStr, long sequence0, long sequence1, int deadReplica) {
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
    int peerReplica = cobj.getInt(ComObject.Tag.replica);
    String filename = cobj.getString(ComObject.Tag.filename);
    byte[] bytes = cobj.getByteArray(ComObject.Tag.binaryFileContent);
    return sendQueueFile(peerReplica, filename, bytes);
  }

  public ComObject sendQueueFile(int peerReplica, String filename, byte[] bytes) {
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

  public double getPercentApplyQueuesComplete() {
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

  public int getCountReplayed() {
    return countReplayed.get();
  }

  private static class QueueEntry {
    private byte[] request;

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "EI_EXPOSE_REP2", justification = "copying the passed in data is too slow")
    @SuppressWarnings("PMD.ArrayIsStoredDirectly") //copying the passed in data is too slow
    public QueueEntry(byte[] request) {
      this.request = request;
    }
  }

  public class LogWriter implements Runnable {
    private final int offset;
    private final ArrayBlockingQueue<LogRequest> currLogRequests;
    private final File dataDir;
    private final int shard;
    private final int replica;
    private final int peerReplicaNum;
    private long currQueueTime = 0;
    private DataOutputStream writer = null;
    private boolean shutdown;
    private boolean wroteData;
    private AtomicReference<String> currFilename = new AtomicReference<>();


    public LogWriter(
        int offset, int peerReplicaNum, ArrayBlockingQueue<LogRequest> logRequests,
        File rootDir, int shard, int replica) {
      this.offset = offset;
      this.peerReplicaNum = peerReplicaNum;
      this.currLogRequests = logRequests;
      this.dataDir = rootDir;
      this.shard = shard;
      this.replica = replica;
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
          break;
        }
        catch (Exception t) {
          logger.error("Error processing log requests", t);
        }
      }
    }

    public void logRequests(List<LogRequest> requests) throws IOException, ParseException {
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
        wroteData = true;
      }
    }

    private void closeAndCreateLog() throws IOException, ParseException {
      synchronized (this) {
        if (writer != null) {
          writer.close();
        }
        sliceFilename = currFilename.get();
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
          newFile = new File(dataRootDir.getAbsolutePath() + "/peer-" + peerReplicaNum, offset + "-" + dt + "-" + nano + ".bin");
        }
        newFile.getParentFile().mkdirs();
        wroteData = false;
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

  public void bindQueues() {
    unbindQueues.set(false);
  }

  public void unbindQueues() {
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

      logger.info("Finished replaying queue: duration=" + (end - begin));
    }
    catch (IOException e) {
      logger.error("Error", e);
    }
    finally {
      bindQueues();
    }
  }

  public void getLogsFromPeer(int replica) {
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.dbName, "__none__");
    cobj.put(ComObject.Tag.schemaVersion, server.getCommon().getSchemaVersion());
    cobj.put(ComObject.Tag.method, "LogManager:sendLogsToPeer");
    cobj.put(ComObject.Tag.replica, server.getReplica());
    AtomicBoolean isHealthy = new AtomicBoolean();
    try {
      server.checkHealthOfServer(server.getShard(), replica, isHealthy, false);
    }
    catch (InterruptedException e) {
      return;
    }
    if (isHealthy.get()) {
      byte[] ret = server.getClient().send(null, server.getShard(), replica, cobj, DatabaseClient.Replica.specified);
      ComObject retObj = new ComObject(ret);
      ComArray filenames = retObj.getArray(ComObject.Tag.filenames);
      if (filenames != null) {
        for (int i = 0; i < filenames.getArray().size(); i++) {
          String filename = (String) filenames.getArray().get(i);
          cobj = new ComObject();
          cobj.put(ComObject.Tag.dbName, "__none__");
          cobj.put(ComObject.Tag.schemaVersion, server.getCommon().getSchemaVersion());
          cobj.put(ComObject.Tag.method, "LogManager:getLogFile");
          cobj.put(ComObject.Tag.replica, server.getReplica());
          cobj.put(ComObject.Tag.filename, filename);

          ret = server.getClient().send(null, server.getShard(), replica, cobj, DatabaseClient.Replica.specified);
          retObj = new ComObject(ret);
          byte[] bytes = retObj.getByteArray(ComObject.Tag.binaryFileContent);

          sendQueueFile(replica, filename, bytes);
          logger.info("Received log file: filename=" + filename + ", replica=" + replica);
        }
        cobj = new ComObject();
        cobj.put(ComObject.Tag.dbName, "__none__");
        cobj.put(ComObject.Tag.schemaVersion, server.getCommon().getSchemaVersion());
        cobj.put(ComObject.Tag.method, "LogManager:deletePeerLogs");
        cobj.put(ComObject.Tag.replica, server.getReplica());
        ret = server.getClient().send(null, server.getShard(), replica, cobj, DatabaseClient.Replica.specified);

      }
    }
  }

  public File getLogReplicaDir() {
    return new File(rootDir, server.getShard() + "/" + server.getReplica());
  }

  public static class ByteCounterStream extends InputStream {
    private final AtomicLong countRead;
    private final InputStream in;

    public ByteCounterStream(InputStream in, AtomicLong countRead) {
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
    private String filename;
    private ByteCounterStream counterStream;
    DataInputStream in;
    long sequence1;
    long sequence0;
    byte[] buffer;
    List<NettyServer.Request> requests;
    private String methodStr;

    public LogSource(File file, com.sonicbase.server.DatabaseServer server, Logger logger, AtomicLong countRead) throws IOException {
      InputStream inputStream = null;
      if (file.getName().contains(".gz")) {
        inputStream = new GZIPInputStream(new BufferedInputStream(new FileInputStream(file)));
      }
      else {
        inputStream = new BufferedInputStream(new FileInputStream(file));
      }
      counterStream = new ByteCounterStream(inputStream, countRead);
      in = new DataInputStream(counterStream);
      filename = file.getAbsolutePath();
      totalBytes = file.length();
      readNext(server, logger);
    }

    public long getTotalBytes() {
      return this.totalBytes;
    }

    public boolean take(com.sonicbase.server.DatabaseServer server, Logger logger) {
      readNext(server, logger);
      return buffer != null;
    }

    public void readNext(com.sonicbase.server.DatabaseServer server, Logger logger) {
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
        buffer = size == 0 ? null : new byte[size];
        if (buffer != null) {
          in.readFully(buffer);
          ComObject cobj = new ComObject(buffer);
          cobj.put(ComObject.Tag.schemaVersion, server.getCommon().getSchemaVersion());
          buffer = cobj.serialize();
        }

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

  public void applyLogsFromPeers(String slicePoint) {
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

  public void applyLogsAfterSlice(String slicePoint) {
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
    ThreadPoolExecutor executor = ThreadUtil.createExecutor(32, "SonicBase LogManager replayLogs Thread");
    try {
      countReplayed.set(0);
      allCurrentSources.clear();
      synchronized (logLock) {
        File[] files = dataRootDir.listFiles();
        if (files == null) {
          logger.warn("No files to restore: shard=" + server.getShard() + ", replica=" + server.getReplica());
        }
        else {
          logger.info("applyLogs - begin: fileCount=" + files.length);
          List<LogSource> sources = new ArrayList<>();
          Set<String> sliceFiles = new HashSet<>();
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
          for (File file : files) {
            if (peerFiles) {
              if (!file.getName().startsWith("peer")) {
                continue;
              }
            }
            if (slicePoint == null) {
              LogSource src = new LogSource(file, server, logger, countRead);
              sources.add(src);
              allCurrentSources.add(src);
              continue;
            }
            if (beforeSlice) {
              if (sliceFiles.contains(file.getAbsolutePath())) {
                LogSource src = new LogSource(file, server, logger, countRead);
                sources.add(src);
                allCurrentSources.add(src);
              }
            }

            if (!beforeSlice) {
              if (!sliceFiles.contains(file.getAbsolutePath())) {
                LogSource src = new LogSource(file, server, logger, countRead);
                sources.add(src);
                allCurrentSources.add(src);
              }
            }
          }
          final long begin = System.currentTimeMillis();
          final AtomicLong lastLogged = new AtomicLong(System.currentTimeMillis());
          try {
            List<NettyServer.Request> batch = new ArrayList<>();
            while (!shutdown) {
              if (sources.size() == 0) {
                break;
              }
              LogSource minSource = sources.get(0);
              for (int i = 0; i < sources.size(); i++) {
                LogSource currSource = sources.get(i);
                if (currSource.buffer == null) {
                  continue;
                }
                if (minSource.buffer == null || currSource.sequence0 < minSource.sequence0) {
                  minSource = currSource;
                }
                else if (currSource.sequence0 == minSource.sequence0) {
                  if (currSource.sequence1 < minSource.sequence1) {
                    minSource = currSource;
                  }
                }
              }
              if (minSource.buffer == null) {
                break;
              }

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
                logger.error("Error replaying request", t);
              }
              minSource.take(server, logger);
            }

            flushBatch(begin, lastLogged, batch);
          }
          finally {
            logger.info("applyLogs - finished: count=" + countReplayed.get() +
                ", rate=" + (double) countReplayed.get() / (double) (System.currentTimeMillis() - begin) * 1000d);

            for (LogSource source : allCurrentSources) {
              source.close();
            }
            allCurrentSources.clear();
          }
          logger.info("applyQueue requestCount=" + countReplayed.get());
        }
      }
    }
    finally {
      executor.shutdownNow();
    }
  }

  private void flushBatch(final long begin, final AtomicLong lastLogged,
                          final List<NettyServer.Request> finalBatch) {
    List<Future> futures = new ArrayList<>();
    for (final NettyServer.Request currRequest : finalBatch) {
      futures.add(executor.submit(new Callable(){
        @Override
        public Object call() {
          try {
            server.invokeMethod(currRequest.getBody(), currRequest.getSequence0(), currRequest.getSequence1(), true, false, null, null);
            countReplayed.incrementAndGet();
            if (System.currentTimeMillis() - lastLogged.get() > 2000) {
              lastLogged.set(System.currentTimeMillis());
              logger.info("applyLogs - single request - progress: count=" + countReplayed.get() +
                  ", rate=" + (double) countReplayed.get() / (double) (System.currentTimeMillis() - begin) * 1000d);
            }
          }
          catch (Exception e) {
            logger.error("Error replaying request", e);
          }
          return null;
        }
      }));
    }
    for (Future future : futures) {
      try {
        future.get();
      }
      catch (Exception e) {
        logger.error("Error replaying request", e);
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

  public void deleteOldLogs(long lastSnapshot, boolean exactDate) {
    synchronized (logLock) {
      if (lastSnapshot != -1) {
        File[] files = new File(getLogRoot(), "self").listFiles();
        if (files != null) {
          outer:
          for (File file : files) {
            try {
              String name = file.getName();
              for (LogWriter logWriter : logWriters) {
                if (file.getAbsolutePath().equals(logWriter.currFilename.get())) {
                  continue outer;
                }
              }

              long fileTime = file.lastModified();
              if (exactDate) {
                if (fileTime < lastSnapshot && file.exists() && !file.delete()) {
                  throw new DatabaseException("Error deleting file: file=" + file.getAbsolutePath());
                }
              }
              else {
                if (fileTime < lastSnapshot - (10 * 1_000) && file.exists() && !file.delete()) {
                  throw new DatabaseException("Error deleting file: file=" + file.getAbsolutePath());
                }
              }
            }
            catch (Exception e) {
              logger.error("Error deleting log file: filename=" + file.getAbsolutePath());
            }
          }
        }
      }
    }

  }
}
