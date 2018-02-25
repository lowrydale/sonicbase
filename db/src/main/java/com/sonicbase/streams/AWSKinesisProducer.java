package com.sonicbase.streams;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.query.DatabaseException;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPOutputStream;

public class AWSKinesisProducer implements StreamsProducer {

  private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(AWSKinesisProducer.class);

  private AmazonKinesis kinesisClient;
  private String streamName;

  public File getInstallDir(ObjectNode config) {
    String dir = config.get("installDirectory").asText();
    return new File(dir.replace("$HOME", System.getProperty("user.home")));
  }

  @Override
  public void init(String cluster, String jsonConfig, String jsonQueueConfig) {
    try {

      logger.info("aws kinesis producer init - begin");
      ObjectMapper mapper = new ObjectMapper();
      ObjectNode queueConfig = (ObjectNode) mapper.readTree(jsonQueueConfig);
      ObjectNode config = (ObjectNode) mapper.readTree(jsonConfig);
      File installDir = getInstallDir(config);
      File keysFile = new File(installDir, "/keys/" + cluster + "-awskeys");
      if (!keysFile.exists()) {
        throw new DatabaseException(cluster + "-awskeys file not found");
      }

      final ClientConfiguration clientConfig = new ClientConfiguration();
      clientConfig.setMaxConnections(1000);
      clientConfig.setRequestTimeout(20_000);
      clientConfig.setConnectionTimeout(60_000);

      AmazonKinesisClientBuilder clientBuilder = AmazonKinesisClientBuilder.standard();

      try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(keysFile)))) {
        String accessKey = reader.readLine();
        String secretKey = reader.readLine();

        BasicAWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
        clientBuilder.setCredentials(new AWSStaticCredentialsProvider(awsCredentials));
      }
      catch (IOException e) {
        throw new DatabaseException(e);
      }

      clientBuilder.setRegion(queueConfig.get("region").asText());
      clientBuilder.setClientConfiguration(clientConfig);

      kinesisClient = clientBuilder.build();

      streamName = queueConfig.get("streamName").asText();

      logger.info("aws kinesis producer init - end");
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public void publish(List<String> messages) {

    try {
      PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
      putRecordsRequest.setStreamName(streamName);
      List<PutRecordsRequestEntry> putRecordsRequestEntryList = new ArrayList<>();

      for (String message : messages) {
        PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();
        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        GZIPOutputStream out = new GZIPOutputStream(bytesOut);
        out.write(message.getBytes("utf-8"));
        out.close();
        putRecordsRequestEntry.setData(ByteBuffer.wrap(bytesOut.toByteArray()));
        putRecordsRequestEntry.setPartitionKey(String.format("partitionKey-%d", message.hashCode() % 5000));
        putRecordsRequestEntryList.add(putRecordsRequestEntry);
      }

      putRecordsRequest.setRecords(putRecordsRequestEntryList);
      PutRecordsResult putRecordsResult = kinesisClient.putRecords(putRecordsRequest);

      boolean exceeded = false;
      while (putRecordsResult.getFailedRecordCount() > 0) {
        final List<PutRecordsRequestEntry> failedRecordsList = new ArrayList<>();
        final List<PutRecordsResultEntry> putRecordsResultEntryList = putRecordsResult.getRecords();
        for (int i = 0; i < putRecordsResultEntryList.size(); i++) {
          final PutRecordsRequestEntry putRecordRequestEntry = putRecordsRequestEntryList.get(i);
          final PutRecordsResultEntry putRecordsResultEntry = putRecordsResultEntryList.get(i);
          if (putRecordsResultEntry.getErrorCode() != null) {
            if ("ProvisionedThroughputExceededException".equals(putRecordsResultEntry.getErrorCode())) {
              exceeded =true;
            }
            logger.error("put record error: code=" + putRecordsResultEntry.getErrorCode() + ", msg=" + putRecordsResultEntry.getErrorMessage());
            failedRecordsList.add(putRecordRequestEntry);
          }
        }
        putRecordsRequestEntryList = failedRecordsList;
        putRecordsRequest.setRecords(putRecordsRequestEntryList);
        putRecordsResult = kinesisClient.putRecords(putRecordsRequest);
      }
      if (exceeded) {
        Thread.sleep(100);
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public void shutdown() {
    kinesisClient.shutdown();
  }
}
