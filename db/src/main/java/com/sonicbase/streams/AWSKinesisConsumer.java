package com.sonicbase.streams;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.*;
import com.sonicbase.common.Config;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.query.DatabaseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;

public class AWSKinesisConsumer implements StreamsConsumer {

  private static Logger logger = LoggerFactory.getLogger(AWSKinesisConsumer.class);

  private boolean shutdown;
  private AmazonKinesis kinesisClient;
  private String streamName;
  private ThreadLocal<String> shardIterator = new ThreadLocal<>();
  private ThreadLocal<String> initialShardIterator = new ThreadLocal<>();
  private ThreadLocal<Shard> shardsByThread = new ThreadLocal<>();
  private ThreadLocal<String> lastSequence = new ThreadLocal<>();
  private ConcurrentLinkedQueue<Shard> ownedShards;
  private int streamShardCount;
  private int sonicBaseShardCount;
  private AtomicInteger messageCountSinceSavedSequence = new AtomicInteger();
  private Connection conn;
  private int getRecordsSleepMillis;
  private int getRecordsRequestCount;

  class KinesisMessage extends Message {
    private final String sequenceNum;
    private final String shardId;

    KinesisMessage(String body, String shardId, String sequenceNum) {
      super(body);
      this.shardId = shardId;
      this.sequenceNum = sequenceNum;
    }
  }

  public void shutdown() {
    this.shutdown = true;
    try {
      kinesisClient.shutdown();
      conn.close();
    }
    catch (SQLException e) {
      throw new DatabaseException(e);
    }
  }

  private void initConnection(Config config) {
    try {
      List<Config.Shard> array = config.getShards();
      com.sonicbase.common.Config.Shard shard =  array.get(0);
      List<Config.Replica> replicasArray = shard.getReplicas();
      final String address = replicasArray.get(0).getString("address");
      final int port = replicasArray.get(0).getInt("port");

      Class.forName("com.sonicbase.jdbcdriver.Driver");

      conn = DriverManager.getConnection("jdbc:sonicbase:" + address + ":" + port);

      try {
        if (!((ConnectionProxy) conn).databaseExists("_sonicbase_sys")) {
          ((ConnectionProxy) conn).createDatabase("_sonicbase_sys");
        }
      }
      catch (Exception e) {
        if (!ExceptionUtils.getFullStackTrace(e).toLowerCase().contains("database already exists")) {
          throw new DatabaseException(e);
        }
      }

      conn.close();

      conn = DriverManager.getConnection("jdbc:sonicbase:" + address + ":" + port + "/_sonicbase_sys");
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private void initTable() {
    PreparedStatement stmt = null;
    try {
      try {
        stmt = conn.prepareStatement("describe table kinesis_stream_state");
        stmt.executeQuery();
      }
      catch (Exception e) {
        stmt = conn.prepareStatement("create table kinesis_stream_state (stream_name VARCHAR, shard_id VARCHAR, sequence_num VARCHAR, PRIMARY KEY (stream_name, shard_id))");
        stmt.executeUpdate();
      }
    }
    catch (Exception e) {
      if (!ExceptionUtils.getFullStackTrace(e).toLowerCase().contains("table already exists")) {
        throw new DatabaseException(e);
      }
    }
    finally {
      if (stmt != null) {
        try {
          stmt.close();
        }
        catch (SQLException e) {
          throw new DatabaseException(e);
        }
      }
    }
  }

  @Override
  public int init(Config config, String installDir, Map<String, Object> streamConfig) {
    try {
      logger.info("aws kinesis consumer init - begin");

      initConnection(config);
      initTable();

      final ClientConfiguration clientConfig = new ClientConfiguration();
      clientConfig.setMaxConnections(100);
      clientConfig.setRequestTimeout(20_000);
      clientConfig.setConnectionTimeout(60_000);

      AmazonKinesisClientBuilder clientBuilder = AmazonKinesisClientBuilder.standard();

      File keysFile = new File(installDir, "/keys/sonicbase-awskeys");
      if (!keysFile.exists()) {
        clientBuilder.setCredentials(new InstanceProfileCredentialsProvider(true));
      }
      else {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(keysFile)))) {
          String accessKey = reader.readLine();
          String secretKey = reader.readLine();

          BasicAWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
          clientBuilder.setCredentials(new AWSStaticCredentialsProvider(awsCredentials));
        }
        catch (IOException e) {
          throw new DatabaseException(e);
        }
      }
      clientBuilder.setRegion((String) streamConfig.get("region"));
      clientBuilder.setClientConfiguration(clientConfig);

      kinesisClient = clientBuilder.build();

      streamName = (String) streamConfig.get("streamName");
      getRecordsSleepMillis = (int) streamConfig.get("getRecordsSleepMillis");
      getRecordsRequestCount = (int) streamConfig.get("getRecordsRequestCount");

      DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
      describeStreamRequest.setStreamName(streamName);
      List<Shard> shards = new ArrayList<>();
      String exclusiveStartShardId = null;
      do {
        describeStreamRequest.setExclusiveStartShardId( exclusiveStartShardId );
        DescribeStreamResult describeStreamResult = kinesisClient.describeStream( describeStreamRequest );
        shards.addAll( describeStreamResult.getStreamDescription().getShards() );
        if (describeStreamResult.getStreamDescription().getHasMoreShards() && shards.size() > 0) {
          exclusiveStartShardId = shards.get(shards.size() - 1).getShardId();
        }
        else {
          exclusiveStartShardId = null;
        }
      }
      while ( exclusiveStartShardId != null );

      Collections.sort(shards, (o1, o2) -> o1.getShardId().compareTo(o2.getShardId()));

      streamShardCount = shards.size();

      int thisServerShard = config.getInt("shard");
      int thisServerReplica = config.getInt("replica");

      List<Config.Shard> array = config.getShards();
      sonicBaseShardCount = array.size();
      List<Config.Replica> replicaArray = array.get(0).getReplicas();
      int replicaCount = replicaArray.size();

      ownedShards = new ConcurrentLinkedQueue<>();
      int shardOffset = 0;
      while (true) {
        outer:
        for (int i = 0; i < sonicBaseShardCount; i++) {
          for (int j = 0; j < replicaCount; j++) {
            if (i == thisServerShard) {
              ownedShards.add(shards.get(shardOffset));
            }
            shardOffset++;
            if (shardOffset >= shards.size()) {
              break outer;
            }
          }
        }
        if (shardOffset >= shards.size()) {
          break;
        }
      }

      logger.info("aws kinesis consumer init - end: streamShardCount=" + streamShardCount + ", threadCount=" + ownedShards.size());

      return ownedShards.size();
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }

  }

  @Override
  public void initThread() {
    Shard shard = ownedShards.poll();
    while (true) {
      try {
        GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
        getShardIteratorRequest.setStreamName(streamName);
        getShardIteratorRequest.setShardId(shard.getShardId());
        String sequenceNum = getSequenceNumber(shard.getShardId());
        if (sequenceNum == null) {
          getShardIteratorRequest.setShardIteratorType("TRIM_HORIZON");
        }
        else {
          logger.info("initializing shardIterator with sequenceNumber: shardId=" + shard.getShardId() + ", sequenceNum=" + sequenceNum);
          getShardIteratorRequest.setShardIteratorType("AFTER_SEQUENCE_NUMBER");
          getShardIteratorRequest.setStartingSequenceNumber(sequenceNum);
        }

        GetShardIteratorResult getShardIteratorResult = kinesisClient.getShardIterator(getShardIteratorRequest);
        initialShardIterator.set(getShardIteratorResult.getShardIterator());
        shardsByThread.set(shard);
        Thread.currentThread().setName("Consumer: shard=" + shard.getShardId());
        break;
      }
      catch (Exception e) {
        logger.error("Error initializing thread: shard=" + shard.getShardId(), e);
        try {
          Thread.sleep(1_000);
        }
        catch (InterruptedException e1) {
          throw new DatabaseException(e1);
        }
      }
    }
  }

  @Override
  public List<Message> receive() {
    try {
      Thread.sleep(getRecordsSleepMillis);

      GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
      if (shardIterator.get() == null) {
        getRecordsRequest.setShardIterator(initialShardIterator.get());
      }
      else {
        getRecordsRequest.setShardIterator(shardIterator.get());
      }
      getRecordsRequest.setLimit(getRecordsRequestCount);

      try {
        GetRecordsResult getRecordsResult = kinesisClient.getRecords(getRecordsRequest);
        List<Record> records = getRecordsResult.getRecords();
        if (records.size() != 0) {
          lastSequence.set(records.get(records.size() - 1).getSequenceNumber());
        }
        if (shardIterator.get() == null) {
          getNewShardIterator();
        }
        else {
          shardIterator.set(getRecordsResult.getNextShardIterator());
          if (shardIterator.get() == null) {
            getNewShardIterator();
          }
        }
        if (records != null) {
          List<Message> ret = new ArrayList<>();
          for (Record record : records) {
            if (record.getData().remaining() == 0) {
              continue;
            }
            byte[] bytes = new byte[record.getData().remaining()];
            record.getData().get(bytes);
            GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(bytes));
            bytes = IOUtils.toByteArray(in);
            String message = new String(bytes, "utf-8");
            ret.add(new KinesisMessage(message, shardsByThread.get().getShardId(), record.getSequenceNumber()));
          }
          return ret;
        }
      }
      catch (ExpiredIteratorException e) {
        getNewShardIterator();
      }
      catch (Exception e) {
        logger.error("Error getting records", e);
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
    return null;
  }

  private void saveState(String shardId, String sequenceNumber) {
    PreparedStatement stmt = null;
    try {
      stmt = conn.prepareStatement("insert ignore into kinesis_stream_state (stream_name, shard_id, sequence_num) VALUES (?, ?, ?)");
      stmt.setString(1, streamName);
      stmt.setString(2, shardId);
      stmt.setString(3, sequenceNumber);
      stmt.executeUpdate();
    }
    catch (SQLException e) {
      throw new DatabaseException(e);
    }
    finally {
      if (stmt != null) {
        try {
          stmt.close();
        }
        catch (SQLException e) {
          throw new DatabaseException(e);
        }
      }
    }
  }

  private String getSequenceNumber(String shardId) {
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try {
      stmt = conn.prepareStatement("select * from kinesis_stream_state where stream_name = ? and shard_id = ?");
      stmt.setString(1, streamName);
      stmt.setString(2, shardId);
      rs = stmt.executeQuery();
      if (!rs.next()) {
        return null;
      }
      return rs.getString("sequence_num");

    }
    catch (SQLException e) {
      throw new DatabaseException(e);
    }
    finally {
      try {
        if (rs != null) {
          rs.close();
        }
        if (stmt != null) {
          stmt.close();
        }
      }
      catch (SQLException e) {
        throw new DatabaseException(e);
      }
    }
  }

  private void getNewShardIterator() {
    if (lastSequence.get() != null) {
      GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
      getShardIteratorRequest.setStreamName(streamName);
      getShardIteratorRequest.setShardId(shardsByThread.get().getShardId());
      getShardIteratorRequest.setShardIteratorType("AFTER_SEQUENCE_NUMBER");
      getShardIteratorRequest.setStartingSequenceNumber(lastSequence.get());

      GetShardIteratorResult getShardIteratorResult = kinesisClient.getShardIterator(getShardIteratorRequest);
      shardIterator.set(getShardIteratorResult.getShardIterator());
    }
  }

  @Override
  public void acknowledgeMessages(List<Message> messages) {
    for (Message message : messages) {
      if (messageCountSinceSavedSequence.incrementAndGet() % 1000 == 0) {
        saveState(((KinesisMessage) message).shardId, ((KinesisMessage) message).sequenceNum);
      }
    }
  }

  @Override
  public void handleError(List<Message> messages, Exception e) {

  }

}
