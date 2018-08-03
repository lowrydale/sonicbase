package com.sonicbase.bench;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.query.DatabaseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class BenchmarkAWSDeletePublish {

  public static Logger logger = LoggerFactory.getLogger(BenchmarkAWSDeletePublish.class);


  private static final MetricRegistry METRICS = new MetricRegistry();

  public static final Timer INSERT_STATS = METRICS.timer("insert");

  private Thread mainThread;
  private boolean shutdown;

  private AtomicInteger countInserted = new AtomicInteger();
  private AtomicLong insertErrorCount = new AtomicLong();
  private long begin;
  private AtomicLong totalDuration = new AtomicLong();
  private AtomicLong insertBegin;
  private AtomicLong insertHighest;

  public static void main(String[] args) {
    Thread[] threads = new Thread[4];
    final BenchmarkAWSDeletePublish insert = new BenchmarkAWSDeletePublish();
    for (int i = 0; i < 4; i++) {
      final int shard = i;
      threads[i] = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            insert.start(new AtomicLong(), new AtomicLong(), "1-local", 4, shard, 0, 1000000000, true);
          }
          catch (IOException e) {
            e.printStackTrace();
          }
        }
      });
      threads[i].start();
    }
  }

  private AtomicInteger activeThreads = new AtomicInteger();
  private ConcurrentHashMap<Integer, Long> threadLiveliness = new ConcurrentHashMap<>();
  private int countDead = 0;

  public void start(final AtomicLong insertBegin, AtomicLong insertHighest, final String cluster, final int shardCount, final int shard, final long offset,
                    final long count, final boolean simulate) throws IOException {
    shutdown = false;
    doResetStats();
    this.insertBegin = insertBegin;
    this.insertHighest = insertHighest;
    begin = System.currentTimeMillis();

    final ClientConfiguration config = new ClientConfiguration();
    config.setMaxConnections(10);
    config.setRequestTimeout(20_000);
    config.setConnectionTimeout(60_000);

    File file = new File(System.getProperty("user.dir"), "config/config-" + cluster + ".json");
    if (!file.exists()) {
      file = new File(System.getProperty("user.dir"), "db/src/main/resources/config/config-" + cluster + ".json");
      System.out.println("Loaded config resource dir");
    }
    else {
      System.out.println("Loaded config default dir");
    }
    String configStr = IOUtils.toString(new BufferedInputStream(new FileInputStream(file)), "utf-8");
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode clusterConfig = (ObjectNode) mapper.readTree(configStr);

    File installDir = new File(clusterConfig.get("installDirectory").asText().replaceAll("\\$HOME", System.getProperty("user.home")));
    File keysFile = new File(installDir, "/keys/" + cluster + "-awskeys");
    if (!keysFile.exists()) {
      throw new DatabaseException(cluster + "-awskeys file not found");
    }
    final AmazonSQSClient sqsClient;
    final String queueUrl;
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(keysFile)))) {
      String accessKey = reader.readLine();
      String secretKey = reader.readLine();

      BasicAWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
      sqsClient = new AmazonSQSClient(awsCredentials);
      sqsClient.setEndpoint("https://sqs.us-east-1.amazonaws.com");

      queueUrl = sqsClient.getQueueUrl("benchmark-queue").getQueueUrl();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }

    mainThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          final ThreadPoolExecutor executor = new ThreadPoolExecutor(256, 256, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
          final ThreadPoolExecutor selectExecutor = new ThreadPoolExecutor(256, 256, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new RejectedExecutionHandler() {
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
              // This will block if the queue is full
              try {
                executor.getQueue().put(r);
              }
              catch (InterruptedException e) {
                System.err.println(e.getMessage());
              }

            }
          });

          final boolean batch = offset != 1;

          final AtomicLong countFinished = new AtomicLong();

          final AtomicInteger errorCountInARow = new AtomicInteger();
          final int batchSize = 100;
          while (!shutdown) {
            final long startId = offset + (shard * count);
            insertBegin.set(startId);
            List<Thread> threads = new ArrayList<>();
            final AtomicLong currOffset = new AtomicLong(startId);
            final int threadCount = (batch ? 32 : 256);
            for (int i = 0; i < threadCount; i++) {
              final int threadOffset = i;
              final AtomicLong lastLogged = new AtomicLong(System.currentTimeMillis());
              Thread insertThread = new Thread(new Runnable() {
                @Override
                public void run() {
                  try {
                    threadLiveliness.put(threadOffset, System.currentTimeMillis());
                    activeThreads.incrementAndGet();
                    while (!shutdown) {

                      long offset = 0;
                      synchronized (currOffset) {
                        offset = currOffset.getAndAdd(batchSize);
                      }
                      BenchmarkAWSDeletePublish.this.insertHighest.set(offset - (threadCount * batchSize * 2));
                      try {
                        if (batch) {
                          long thisDuration = 0;
                          if (true) {
                            for (int attempt = 0; attempt < 4; attempt++) {
                              try {
                                ObjectNode request = new ObjectNode(JsonNodeFactory.instance);
                                request.put("database", "db");
                                request.put("table", "persons");
                                request.put("action", "delete");
                                ArrayNode records = new ArrayNode(JsonNodeFactory.instance);
                                request.put("records", records);
                                for (int i = 0; i < batchSize; i++) {
                                  ObjectNode record = records.addObject();
                                  record.put("id1", offset + i);
                                }
                                long currBegin = System.nanoTime();
                                sendRequest(sqsClient, queueUrl, request);
                                thisDuration += System.nanoTime() - currBegin;
                                break;
                              }
                              catch (Exception e) {
                                if (attempt == 3) {
                                  throw e;
                                }
                                e.printStackTrace();
                              }
                            }

                            threadLiveliness.put(threadOffset, System.currentTimeMillis());
                            //conn.commit();
                            totalDuration.addAndGet(thisDuration);
                            countInserted.addAndGet(batchSize);
                            logProgress(offset, threadOffset, countInserted, lastLogged, begin, totalDuration, insertErrorCount);
                          }

                          thisDuration = 0;
                          if (true) {
                            for (int attempt = 0; attempt < 4; attempt++) {
                              try {
                                ObjectNode request = new ObjectNode(JsonNodeFactory.instance);
                                request.put("database", "test");
                                request.put("table", "memberships");
                                request.put("action", "upsert");
                                ArrayNode records = new ArrayNode(JsonNodeFactory.instance);
                                request.put("records", records);
                                for (int i = 0; i < batchSize; i++) {
                                  for (int j = 0; j < 1; j++) {
                                    ObjectNode record = records.addObject();
                                    record.put("personId", offset + i);
                                    record.put("personId2", j);
                                    record.put("membershipName", "membership-" + j);
                                    record.put("resortId", new long[]{1000, 2000}[j % 2]);
                                  }
                                }
                                long currBegin = System.nanoTime();
                                sendRequest(sqsClient, queueUrl, request);
                                thisDuration += System.nanoTime() - currBegin;
                                break;
                              }
                              catch (Exception e) {
                                if (attempt == 3) {
                                  throw e;
                                }
                                e.printStackTrace();
                              }
                            }
                            totalDuration.addAndGet(thisDuration);
                            countInserted.addAndGet(2 * batchSize);
                            logProgress(offset, threadOffset, countInserted, lastLogged, begin, totalDuration, insertErrorCount);
                          }
                        }
                        errorCountInARow.set(0);
                      }
                      catch (Exception e) {
                        if (errorCountInARow.incrementAndGet() > 2000) {
                          System.out.println("Too many errors, aborting");
                          break;
                        }
                        insertErrorCount.incrementAndGet();
                        if (e.getMessage() != null && e.getMessage().contains("Unique constraint violated")) {
                          System.out.println("Unique constraint violation");
                        }
                        else {
                          System.out.println("Error inserting");
                          e.printStackTrace();
                        }
                      }
                      finally {
                        countFinished.incrementAndGet();
                        logProgress(offset, threadOffset, countInserted, lastLogged, begin, totalDuration, insertErrorCount);
                      }
                    }
                  }
                  finally {
                    activeThreads.decrementAndGet();
                  }
                }
              });
              insertThread.start();
              threads.add(insertThread);
            }

            while (true) {
              int countDead = 0;
              for (Map.Entry<Integer, Long> entry : threadLiveliness.entrySet()) {
                if (System.currentTimeMillis() - entry.getValue() > 4 * 60 * 1000) {
                  countDead++;
                }
              }
              BenchmarkAWSDeletePublish.this.countDead = countDead;
              Thread.sleep(1000);
            }
//            for (Thread thread : threads) {
//              thread.join();
//            }
          }

          selectExecutor.shutdownNow();
          executor.shutdownNow();
        }
        catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
    mainThread.start();
  }

  private void sendRequest(AmazonSQSClient sqsClient, String queueUrl, ObjectNode requestJson) {
//    SendMessageRequest request = new SendMessageRequest();
//    request.withMessageBody(requestJson.toString());
//    request.withQueueUrl("https://sqs.us-east-1.amazonaws.com/892217711366/benchmark-queue");
    String url = queueUrl;//"https://sqs.us-east-1.amazonaws.com/892217711366/benchmark-queue";
    SendMessageResult result = sqsClient.sendMessage(url, requestJson.toString());//.sendMessage(request);
    if (result == null || StringUtils.isEmpty(result.getMessageId())) {
      System.out.println("SendMessage results: null result or null message id result=" + result.toString());
      throw new DatabaseException("Error sending message");
    }
  }

  private void doResetStats() {
    countInserted.set(0);
    insertErrorCount.set(0);
    begin = System.currentTimeMillis();
    totalDuration.set(0);
  }

  private static void logProgress(long offset, int threadOffset, AtomicInteger countInserted, AtomicLong lastLogged, long begin, AtomicLong totalDuration, AtomicLong insertErrorCount) {
    if (threadOffset == 0) {
      if (System.currentTimeMillis() - lastLogged.get() > 2000) {
        lastLogged.set(System.currentTimeMillis());
        StringBuilder builder = new StringBuilder();
        builder.append("count=").append(countInserted.get());
        Snapshot snapshot = INSERT_STATS.getSnapshot();
        builder.append(String.format(", rate=%.2f", countInserted.get() / (double) (System.currentTimeMillis() - begin) * 1000f)); //INSERT_STATS.getFiveMinuteRate()));
        builder.append(String.format(", avg=%.2f", totalDuration.get() / (countInserted.get()) / 1000000d));//snapshot.getMean() / 1000000d));
        builder.append(String.format(", 99th=%.2f", snapshot.get99thPercentile() / 1000000d));
        builder.append(String.format(", max=%.2f", (double) snapshot.getMax() / 1000000d));
        builder.append(", errorCount=" + insertErrorCount.get());
        System.out.println(builder.toString());
      }
    }
  }

  public void stop() {
    shutdown = true;
    mainThread.interrupt();
  }

  public String stats() {
    ObjectNode dict = new ObjectNode(JsonNodeFactory.instance);
    dict.put("begin", begin);
    dict.put("count", countInserted.get());
    dict.put("errorCount", insertErrorCount.get());
    dict.put("totalDuration", totalDuration.get());
    dict.put("activeThreads", activeThreads.get());
    dict.put("countDead", countDead);
    return dict.toString();
  }

  public void resetStats() {
    doResetStats();
  }
}
