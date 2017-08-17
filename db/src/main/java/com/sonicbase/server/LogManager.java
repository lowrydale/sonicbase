package com.sonicbase.server;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.AWSClient;
import com.sonicbase.common.ComArray;
import com.sonicbase.common.ComObject;
import com.sonicbase.common.Logger;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.util.DataUtil;
import com.sonicbase.util.ISO8601;
import com.sonicbase.util.StreamUtils;
import com.sonicbase.research.socket.NettyServer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

/**
 * Responsible for
 */
public class LogManager {

  private static final String UTF8_STR = "utf-8";
  private final List<LogProcessor> logProcessors = new ArrayList<>();
  private final List<LogProcessor> peerLogProcessors = new ArrayList<>();
  private Logger logger;
  private final DatabaseServer databaseServer;
  private final ThreadPoolExecutor executor;

  private AtomicLong countLogged = new AtomicLong();
  private final DatabaseServer server;
  private ArrayBlockingQueue<DatabaseServer.LogRequest> logRequests = new ArrayBlockingQueue<>(1000);
  private Map<Integer, ArrayBlockingQueue<DatabaseServer.LogRequest>> peerLogRequests = new ConcurrentHashMap<>();
  private AtomicBoolean unbindQueues = new AtomicBoolean();
  private final Object logLock = new Object();
  private AtomicLong logSequenceNumber = new AtomicLong();
  private AtomicLong maxAllocatedLogSequenceNumber = new AtomicLong();
  private static final int SEQUENCE_NUM_ALLOC_COUNT = 100000;
  private String currFilename;
  private String sliceFilename;
  private boolean shouldSlice = false;
  private boolean didSlice = false;

  public LogManager(DatabaseServer databaseServer) {
    this.databaseServer = databaseServer;
    this.server = databaseServer;
    logger = new Logger(databaseServer.getDatabaseClient());
    executor = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors() * 8,
        Runtime.getRuntime().availableProcessors() * 8, 10000, TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

    synchronized (this) {
      try {
        skipToMaxSequenceNumber();
      }
      catch (IOException e) {
        throw new DatabaseException(e);
      }
    }
    int logThreadCount = 1;//64;
    for (int i = 0; i < logThreadCount; i++) {
      LogProcessor logProcessor = new LogProcessor(i, -1, logRequests, server.getDataDir(), server.getShard(), server.getReplica());
      logProcessors.add(logProcessor);
      Thread thread = new Thread(logProcessor);
      thread.start();
    }
  }

  public void startLoggingForPeer(int replicaNum) {
    synchronized (peerLogRequests) {
      if (!peerLogRequests.containsKey(replicaNum)) {
        peerLogRequests.put(replicaNum, new ArrayBlockingQueue<DatabaseServer.LogRequest>(1000));
        LogProcessor logProcessor = new LogProcessor(0, replicaNum, peerLogRequests.get(replicaNum), server.getDataDir(), server.getShard(), server.getReplica());
        peerLogProcessors.add(logProcessor);
        Thread thread = new Thread(logProcessor);
        thread.start();
      }
    }
  }

  public void skipToMaxSequenceNumber() throws IOException {
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
      pushMaxSequenceNum();
    }
  }

  public ComObject setMaxSequenceNum(ComObject cobj) {
    try {
      long sequenceNum = cobj.getLong(ComObject.Tag.sequenceNumber);

      maxAllocatedLogSequenceNumber.set(sequenceNum);
      File file = new File(databaseServer.getDataDir(), "logSequenceNum/" + databaseServer.getShard() + "/" + databaseServer.getReplica() + "/logSequenceNum.txt");
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
    synchronized (this) {
      if (logSequenceNumber.get() == maxAllocatedLogSequenceNumber.get()) {
        maxAllocatedLogSequenceNumber.set(logSequenceNumber.get() + SEQUENCE_NUM_ALLOC_COUNT);
        File file = new File(databaseServer.getDataDir(), "logSequenceNum/" + databaseServer.getShard() + "/" + databaseServer.getReplica() + "/logSequenceNum.txt");
        file.getParentFile().mkdirs();
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))) {
          writer.write(String.valueOf(maxAllocatedLogSequenceNumber.get()));
        }
        pushMaxSequenceNum();
      }
      return logSequenceNumber.incrementAndGet();
    }
  }

  void pushMaxSequenceNum() {
    for (int replica = 0; replica < server.getReplicationFactor(); replica++) {
      if (replica != server.getReplica()) {
        try {
          ComObject cobj = new ComObject();
          cobj.put(ComObject.Tag.dbName, "__none__");
          cobj.put(ComObject.Tag.schemaVersion, server.getCommon().getSchemaVersion());
          cobj.put(ComObject.Tag.method, "setMaxSequenceNum");
          cobj.put(ComObject.Tag.sequenceNumber, maxAllocatedLogSequenceNumber.get());
          String command = "DatabaseServer:ComObject:setMaxSequenceNum:";
          server.getClient().send(null, server.getShard(), replica, command, cobj, DatabaseClient.Replica.specified, true);
        }
        catch (Exception e) {
          logger.error("Error setting maxSequenceNum: shard=" + server.getShard() + ", replica=" + replica);
        }
      }
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

      for (LogProcessor logProcessor : logProcessors) {
        logProcessor.closeAndCreateLog();
      }

      for (LogProcessor logProcessor : peerLogProcessors) {
        logProcessor.closeAndCreateLog();
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

  public void backupFileSystem(String directory, String subDirectory, String logSlicePoint) {
    try {
      File srcDir = new File(getLogReplicaDir(), "self");
      File destDir = new File(directory, subDirectory + "/queue/" + server.getShard() + "/0/self");
      backupLogDir(logSlicePoint, destDir, srcDir);

      for (int replica = 0; replica < server.getReplicationFactor(); replica++) {
        srcDir = new File(getLogReplicaDir(), "peer-" + replica);
        destDir = new File(directory, subDirectory + "/queue/" + server.getShard() + "/0/peer-" + replica);
        backupLogDir(logSlicePoint, destDir, srcDir);
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private void backupLogDir(String logSlicePoint, File destDir, File srcDir) throws IOException {
    File[] files = srcDir.listFiles();
    if (files != null) {
      Set<String> sliceFiles = new HashSet<>();
      BufferedReader reader = new BufferedReader(new StringReader(logSlicePoint));
      while (true) {
        String line = reader.readLine();
        if (line == null) {
          break;
        }
        sliceFiles.add(line);
      }
      destDir.mkdirs();
      for (File file : files) {
        if (sliceFiles.contains(file.getAbsolutePath())) {
          File destFile = new File(destDir, file.getName());
          FileUtils.copyFile(file, destFile);
        }
      }
    }
  }

  private void backupLogDirToAWS(AWSClient awsClient, String logSlicePoint, String bucket, String prefix, String destDir, File srcDir) throws IOException {
    File[] files = srcDir.listFiles();
    if (files != null) {
      Set<String> sliceFiles = new HashSet<>();
      BufferedReader reader = new BufferedReader(new StringReader(logSlicePoint));
      while (true) {
        String line = reader.readLine();
        if (line == null) {
          break;
        }
        sliceFiles.add(line);
      }
      for (File file : files) {
        if (sliceFiles.contains(file.getAbsolutePath())) {
          awsClient.uploadFile(bucket, prefix, destDir, file);
        }
      }
    }
  }

  public void restoreFileSystem(String directory, String subDirectory) {
    try {
      File destDir = new File(getLogReplicaDir(), "self");
      File srcDir = new File(directory, subDirectory + "/queue/" + server.getShard() + "/0/self");
      restoreLogDir(srcDir, destDir);

      for (int replica = 0; replica < server.getReplicationFactor(); replica++) {
        destDir = new File(getLogReplicaDir(), "peer-" + replica);
        srcDir = new File(directory, subDirectory + "/queue/" + server.getShard() + "/0/peer-" + replica);
        restoreLogDir(srcDir, destDir);
      }

    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private void restoreLogDir(File srcDir, File destDir) throws IOException {
    if (destDir.exists()) {
      FileUtils.deleteDirectory(destDir);
    }
    destDir.mkdirs();

    if (srcDir.exists()) {
      FileUtils.copyDirectory(srcDir, destDir);
    }
  }

  public void backupAWS(String bucket, String prefix, String subDirectory, String logSlicePoint) {
    AWSClient awsClient = server.getAWSClient();
    try {
      File srcDir = new File(getLogReplicaDir(), "self");
      String destDir = subDirectory + "/queue/" + server.getShard() + "/0/self";
      backupLogDirToAWS(awsClient, logSlicePoint, bucket, prefix, destDir, srcDir);

      for (int replica = 0; replica < server.getReplicationFactor(); replica++) {
        srcDir = new File(getLogReplicaDir(), "peer-" + replica);
        destDir = subDirectory + "/queue/" + server.getShard() + "/0/peer-" + replica;
        backupLogDirToAWS(awsClient, logSlicePoint, bucket, prefix, destDir, srcDir);
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public void restoreAWS(String bucket, String prefix, String subDirectory) {
    try {
      AWSClient awsClient = server.getAWSClient();
      File destDir = getLogReplicaDir();
      subDirectory += "/queue/" + server.getShard() + "/0";

      FileUtils.deleteDirectory(destDir);
      destDir.mkdirs();

      awsClient.downloadDirectory(bucket, prefix, subDirectory, destDir);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public ComObject getLogFile(ComObject cobj) {
    try {
      int replica = cobj.getInt(ComObject.Tag.replica);
      String filename = cobj.getString(ComObject.Tag.filename);
      File file = new File(getLogRoot() + "/peer-" + replica + "/" + filename);
      InputStream in = new BufferedInputStream(new FileInputStream(file));
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      IOUtils.copy(in, out);

      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.binaryFileContent, out.toByteArray());

      return retObj;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public ComObject deletePeerLogs(ComObject cobj) {
    deletePeerLogs(cobj.getInt(ComObject.Tag.replica));
    return null;
  }

  public ComObject sendLogsToPeer(int replicaNum) {
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

  public void logRequestForPeer(String command, byte[] body, long sequence0, long sequence1, int deadReplica) {
    startLoggingForPeer(deadReplica);

    try {
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytesOut);
      byte[] buffer = command.getBytes(UTF8_STR);

      DataUtil.ResultLength resultLength = new DataUtil.ResultLength();
      DataUtil.writeVLong(out, sequence0, resultLength);
      DataUtil.writeVLong(out, sequence1, resultLength);
      out.writeInt(buffer.length);
      out.write(buffer);
      out.writeInt(body == null ? 0 : body.length);
      if (body != null && body.length != 0) {
        out.write(body);
      }
      DatabaseServer.LogRequest request = new DatabaseServer.LogRequest(1);
      request.setBuffer(bytesOut.toByteArray());
      peerLogRequests.get(deadReplica).put(request);
      request.getLatch().await();
    }
    catch (InterruptedException | IOException e) {
      throw new DatabaseException(e);
    }
  }

  public void receiveExternalLog(int peerReplica, String filename, byte[] bytes) {
    try {
      String directory = getLogRoot();
      File dataRootDir = new File(directory);
      dataRootDir.mkdirs();
      File file = new File(dataRootDir, "self/peer-" + peerReplica + "-" + filename);
      try (BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(file))) {
        out.write(bytes);
      }
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
    long readBytes = 0;
    for (LogSource source : allCurrentSources) {
      totalBytes += source.getTotalBytes();
      readBytes += source.getBytesRead();
    }
    if (totalBytes == 0) {
      return 0;
    }
    return (double)readBytes / (double)totalBytes;
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
    private final ArrayBlockingQueue<DatabaseServer.LogRequest> currLogRequests;
    private final String dataDir;
    private final int shard;
    private final int replica;
    private final int peerReplicaNum;
    private long currQueueTime = 0;
    private DataOutputStream writer = null;

    public LogProcessor(
        int offset, int peerReplicaNum, ArrayBlockingQueue<DatabaseServer.LogRequest> logRequests,
        String dataDir, int shard, int replica) {
      this.offset = offset;
      this.peerReplicaNum = peerReplicaNum;
      this.currLogRequests = logRequests;
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
            DatabaseServer.LogRequest request = currLogRequests.poll(30000, TimeUnit.MILLISECONDS);
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
          synchronized (this) {
            if (shouldSlice || writer == null || System.currentTimeMillis() - 2 * 60 * 100 > currQueueTime) {
              closeAndCreateLog();
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
          }

          for (DatabaseServer.LogRequest request : requests) {
            request.getLatch().countDown();
          }
        }
        catch (Exception t) {
          logger.error("Error processing log requests", t);
        }
      }
    }

    private void closeAndCreateLog() throws IOException, ParseException {
      synchronized (this) {
        if (writer != null) {
          writer.close();
        }
        if (currFilename != null) {
          File currFile = new File(currFilename);
          if (currFile.length() == 0) {
            currFile.delete();
          }
        }
        sliceFilename = currFilename;
        String directory = getLogRoot();
        currQueueTime = System.currentTimeMillis();
        File dataRootDir = new File(directory);
        dataRootDir.mkdirs();
        String dt = DatabaseServer.format8601(new Date(System.currentTimeMillis()));
        dt = dt.replace(':', '_');
        File newFile = null;
        if (peerReplicaNum == -1) {
          newFile = new File(dataRootDir, "/self/" + offset + "-" + dt + ".bin");
        }
        else {
          newFile = new File(dataRootDir.getAbsolutePath() + "/peer-" + peerReplicaNum, offset + "-" + dt + ".bin");
        }
        newFile.getParentFile().mkdirs();
        currFilename = newFile.getAbsolutePath();
        writer = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(newFile), 102400));
        shouldSlice = false;
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

  public void applyQueues() {

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

      replayQueues(dataRootDir, null, false, false);

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
    cobj.put(ComObject.Tag.method, "sendLogsToPeer");
    cobj.put(ComObject.Tag.replica, server.getReplica());
    String command = "DatabaseServer:ComObject:sendLogsToPeer:";
    AtomicBoolean isHealthy = new AtomicBoolean();
    try {
      server.checkHealthOfServer(server.getShard(), replica, isHealthy, false);
    }
    catch (InterruptedException e) {
      return;
    }
    if (isHealthy.get()) {
      byte[] ret = server.getClient().send(null, server.getShard(), replica, command, cobj, DatabaseClient.Replica.specified);
      ComObject retObj = new ComObject(ret);
      ComArray filenames = retObj.getArray(ComObject.Tag.filenames);
      if (filenames != null) {
        for (int i = 0; i < filenames.getArray().size(); i++) {
          String filename = (String) filenames.getArray().get(i);
          cobj = new ComObject();
          cobj.put(ComObject.Tag.dbName, "__none__");
          cobj.put(ComObject.Tag.schemaVersion, server.getCommon().getSchemaVersion());
          cobj.put(ComObject.Tag.method, "getLogFile");
          cobj.put(ComObject.Tag.replica, server.getReplica());
          cobj.put(ComObject.Tag.filename, filename);
          command = "DatabaseServer:ComObject:getLogFile:";
          ret = server.getClient().send(null, server.getShard(), replica, command, cobj, DatabaseClient.Replica.specified);
          retObj = new ComObject(ret);
          byte[] bytes = retObj.getByteArray(ComObject.Tag.binaryFileContent);

          receiveExternalLog(replica, filename, bytes);
          logger.info("Received log file: filename=" + filename + ", replica=" + replica);
        }
        cobj = new ComObject();
        cobj.put(ComObject.Tag.dbName, "__none__");
        cobj.put(ComObject.Tag.schemaVersion, server.getCommon().getSchemaVersion());
        cobj.put(ComObject.Tag.method, "deletePeerLogs");
        cobj.put(ComObject.Tag.replica, server.getReplica());
        command = "DatabaseServer:ComObject:deletePeerLogs:";
        ret = server.getClient().send(null, server.getShard(), replica, command, cobj, DatabaseClient.Replica.specified);

      }
    }
  }

  private File getLogReplicaDir() {
    return new File(server.getDataDir(), "queue/" + server.getShard() + "/" + server.getReplica());
  }

  class ByteCounterStream extends InputStream {
    long count;
    private final InputStream in;

    public ByteCounterStream(InputStream in) {
      this.in = in;
    }
    @Override
    public int read() throws IOException {
      count++;
      return in.read();
    }
  }

  class LogSource {
    private long totalBytes;
    private String filename;
    private ByteCounterStream counterStream;
    DataInputStream in;
    long sequence1;
    long sequence0;
    String command;
    byte[] buffer;
    List<NettyServer.Request> requests;
    DataUtil.ResultLength resultLength = new DataUtil.ResultLength();
    public int offset;

    LogSource(File file) throws IOException {
      InputStream inputStream = null;
      if (file.getName().contains(".gz")) {
        inputStream = new GZIPInputStream(new BufferedInputStream(new FileInputStream(file)));
      }
      else {
        inputStream = new BufferedInputStream(new FileInputStream(file));
      }
      counterStream = new ByteCounterStream(inputStream);
      in = new DataInputStream(counterStream);
      filename = file.getAbsolutePath();
      totalBytes = file.length();
      readNext();
    }

    public long getTotalBytes() {
      return this.totalBytes;
    }

    public long getBytesRead() {
      return counterStream.count;
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
          sequence0 = request.getSequence0();
          sequence1 = request.getSequence1();
          requests = null;
        }
        else {
          requests = new ArrayList<>();
          long lowestSequence0 = Long.MAX_VALUE;
          long lowestSequence1 = Long.MAX_VALUE;
          for (int i = 0; i < count; i++) {
            NettyServer.Request request = readRequest();
            if (sequence0 < lowestSequence1 && sequence1 < lowestSequence1) {
              lowestSequence0 = sequence0;
              lowestSequence1 = sequence1;
            }
            requests.add(request);
          }
          sequence0 = lowestSequence0;
          sequence1 = lowestSequence1;
          command = null;
          buffer = null;
        }
      }
      catch (IOException e) {
        command = null;
        buffer = null;
        sequence1 = -1;
        sequence0 = -1;
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
      sequence0 = DataUtil.readVLong(in, resultLength);
      sequence1 = DataUtil.readVLong(in, resultLength);
      int size = in.readInt();
      byte[] commandBuffer = new byte[size];
      in.readFully(commandBuffer);
      String command = new String(commandBuffer, UTF8_STR);
     // String[] parts = command.split(":");
     // StringBuilder builder = new StringBuilder();
//      for (int i = 0; i < parts.length; i++) {
//        if (i != 0) {
//          builder.append(":");
//        }
//        if (i == 3) {
//          builder.append(server.getCommon().getSchemaVersion());
//        }
//        else {
//          builder.append(parts[i]);
//        }
//      }
//      command = builder.toString();
      size = in.readInt();
      byte[] buffer = size == 0 ? null : new byte[size];
      if (buffer != null) {
        in.readFully(buffer);
        ComObject cobj = new ComObject(buffer);
        cobj.put(ComObject.Tag.schemaVersion, server.getCommon().getSchemaVersion());
        buffer = cobj.serialize();
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

  public void applyLogsFromPeers(String slicePoint) {
    try {
      String dataRoot = getLogReplicaDir().getAbsolutePath();
      File dataRootDir = new File(dataRoot, "self");
      dataRootDir.mkdirs();

      replayQueues(dataRootDir, null, false, true);
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

      replayQueues(dataRootDir, slicePoint, false, false);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private List<LogSource> allCurrentSources = new ArrayList<>();

  private void replayQueues(File dataRootDir, final String slicePoint, final boolean beforeSlice, boolean peerFiles) throws IOException {
    allCurrentSources.clear();
    synchronized (logLock) {
      File[] files = dataRootDir.listFiles();
      if (files != null) {
        final AtomicInteger countProcessed = new AtomicInteger();
        logger.info("applyQueues - begin: fileCount=" + files.length);
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
            LogSource src = new LogSource(file);
            sources.add(src);
            allCurrentSources.add(src);
            continue;
          }
          if (beforeSlice) {
            if (sliceFiles.contains(file.getAbsolutePath())) {
              LogSource src = new LogSource(file);
              sources.add(src);
              allCurrentSources.add(src);
            }
          }

          if (!beforeSlice) {
            if (!sliceFiles.contains(file.getAbsolutePath())) {
              LogSource src = new LogSource(file);
              sources.add(src);
              allCurrentSources.add(src);
            }
          }
        }
        final long begin = System.currentTimeMillis();
        final AtomicLong lastLogged = new AtomicLong(System.currentTimeMillis());
        final AtomicLong countBatched = new AtomicLong();
        final AtomicLong batchCount = new AtomicLong();
        final AtomicLong countSubmitted = new AtomicLong();
        final AtomicLong countFinished = new AtomicLong();
        try {
          while (true) {
            long minSequence0 = Long.MAX_VALUE;
            long minSequence1 = Long.MAX_VALUE;
            int minOffset = -1;
            for (int i = 0; i < sources.size(); i++) {
              if (sources.get(i).requests != null) {
                if (sources.get(i).offset < sources.get(i).requests.size()) {
                  long sequence0 = sources.get(i).requests.get(sources.get(i).offset).getSequence0();
                  long sequence1 = sources.get(i).requests.get(sources.get(i).offset).getSequence1();
                  if (sequence0 < minSequence0 && sequence1 < minSequence1) {
                    minSequence0 = sequence0;
                    minSequence1 = sequence1;
                    minOffset = i;
                  }
                }
              }
              else {
                long sequence0 = sources.get(i).sequence0;
                long sequence1 = sources.get(i).sequence1;
                if (sequence0 < minSequence0 && sequence1 < minSequence1) {
                  minSequence0 = sequence0;
                  minSequence1 = sequence1;
                  minOffset = i;
                }
              }
            }
            if (minOffset == -1) {
              break;
            }
            final LogSource minSource = sources.get(minOffset);
            try {
              batchCount.incrementAndGet();
              if (minSource.requests != null) {
                final NettyServer.Request request = minSource.requests.get(minSource.offset++);
                if (minSource.requests.size() <= minSource.offset) {
                  minSource.offset = 0;
                  if (!minSource.take()) {
                    sources.remove(minOffset);
                  }
                }
                  countBatched.incrementAndGet();
                  countSubmitted.incrementAndGet();
                  executor.submit(new Runnable() {
                    public void run() {
                      try {
                        server.handleCommand(request.getCommand(), request.getBody(), request.getSequence0(),
                            request.getSequence1(), true, false);
                        countProcessed.incrementAndGet();
                        if (System.currentTimeMillis() - lastLogged.get() > 2000) {
                          lastLogged.set(System.currentTimeMillis());
                          logger.info("applyQueues - progress: count=" + countProcessed.get() +
                              ", countBatched=" + countBatched.get() +
                              ", avgBatchSize=" + (countProcessed.get() / batchCount.get()) +
                              ", rate=" + (double) countProcessed.get() / (double) (System.currentTimeMillis() - begin) * 1000d);
                        }
                      }
                      catch (Exception e) {
                        logger.error("Error replaying command: command=" + minSource.command, e);
                      }
                      finally {
                        countFinished.incrementAndGet();
                      }
                    }
                  });

              }
              else {
                if (minSource.command == null) {
                  sources.remove(minOffset);
                  continue;
                }

                countSubmitted.incrementAndGet();
                final String command = minSource.command;
                final byte[] buffer = minSource.buffer;
                final long sequence0 = minSource.sequence0;
                final long sequence1 = minSource.sequence1;
                executor.submit(new Runnable(){
                  public void run () {
                    try {
                      server.handleCommand(command, buffer, sequence0, sequence1,true, false);
                      countProcessed.incrementAndGet();
                      if (System.currentTimeMillis() - lastLogged.get() > 2000) {
                        lastLogged.set(System.currentTimeMillis());
                        logger.info("applyQueues - progress: count=" + countProcessed.get() +
                                 ", countBatched=" + countBatched.get() +
                            ", avgBatchSize=" + (countProcessed.get() / batchCount.get()) +
                            ", rate=" + (double) countProcessed.get() / (double) (System.currentTimeMillis() - begin) * 1000d);
                      }
                    }
                    catch (Exception e) {
                      logger.error("Error replaying command", e);
                    }
                    finally {
                      countFinished.incrementAndGet();
                    }
                  }
                });

                if (!minSource.take()) {
                  sources.remove(minOffset);
                }
              }
            }
            catch (Exception t) {
              logger.error("Error replaying command: command=" + minSource.command, t);
            }
          }
          while (countSubmitted.get() > countFinished.get()) {
            try {
              Thread.sleep(100);
            }
            catch (InterruptedException e) {
              throw new DatabaseException(e);
            }
          }
        }
        finally {
          logger.info("applyQueues - finished: count=" + countProcessed.get() +
              ", rate=" + (double) countProcessed.get() / (double) (System.currentTimeMillis() - begin) * 1000d);

          allCurrentSources.clear();
          for (LogSource source : sources) {
            source.close();
          }
        }
        logger.info("applyQueue commandCount=" + countProcessed.get());
      }
    }
  }

  public DatabaseServer.LogRequest dont_use_logRequests(List<NettyServer.Request> requests, boolean enableQueuing) throws IOException, InterruptedException {
    if (enableQueuing) {
      DatabaseServer.LogRequest logRequest = new DatabaseServer.LogRequest(requests.size());
      List<byte[]> serializedCommands = new ArrayList<>();
      for (int i = 0; i < requests.size(); i++) {
        NettyServer.Request  request = requests.get(i);
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
          long sequenceNumber = getNextSequencenNum();
          logRequest.getSequences1()[i] = sequenceNumber;
          DataUtil.writeVLong(out, System.currentTimeMillis(), resultLength);
          DataUtil.writeVLong(out, sequenceNumber, resultLength);
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
        logRequest.setBuffers(serializedCommands);
        logRequests.put(logRequest);
        return logRequest;
      }
    }
    return null;
  }


  public DatabaseServer.LogRequest logRequest(String command, byte[] body, boolean enableQueuing, String methodStr,
                                              Long existingSequence0, Long existingSequence1) {
    DatabaseServer.LogRequest request = null;
    try {
      if (enableQueuing && DatabaseClient.getWriteVerbs().contains(methodStr)) {
        request = new DatabaseServer.LogRequest(1);
        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytesOut);
        byte[] buffer = command.getBytes(UTF8_STR);

        DataUtil.ResultLength resultLength = new DataUtil.ResultLength();
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

        DataUtil.writeVLong(out, sequence0, resultLength);
        DataUtil.writeVLong(out, sequence1, resultLength);
        out.writeInt(buffer.length);
        out.write(buffer);
        out.writeInt(body == null ? 0 : body.length);
        if (body != null && body.length != 0) {
          out.write(body);
        }
        request.getSequences0()[0] = sequence0;
        request.getSequences1()[0] = sequence1;
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
    synchronized (logLock) {
      if (lastSnapshot != -1) {
        File[] files = new File(getLogRoot(), "self").listFiles();
        if (files != null) {
          for (File file : files) {
            try {
              String name = file.getName();
              if (name.contains("in-process")) {
                continue;
              }

              int pos = 0;
              if (name.startsWith("peer-")) {
                pos = name.indexOf('-', "peer-".length());  //skip 'peer'
                pos = name.indexOf('-', pos + 1); //skip replica
              }
              else {
                pos = name.indexOf('-');
              }

              int pos2 = name.lastIndexOf('.');
              String dateStr = name.substring(pos + 1, pos2);
              dateStr = dateStr.replace('_', ':');
              Date fileDate = ISO8601.from8601String(dateStr).getTime();
              long fileTime = fileDate.getTime();
              if (fileTime < lastSnapshot - (60 * 1000) && file.exists() && !file.delete()) {
                throw new DatabaseException("Error deleting file: file=" + file.getAbsolutePath());
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
