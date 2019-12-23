package com.sonicbase.streams;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import com.sonicbase.common.Config;
import com.sonicbase.query.DatabaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

public class AWSKinesisProducer implements StreamsProducer {

  private static Logger logger = LoggerFactory.getLogger(AWSKinesisProducer.class);

  private AmazonKinesis kinesisClient;
  private String streamName;

  @Override
  public void init(Config config, String installDir, Map<String, Object> queueConfig) {
    try {
      logger.info("aws kinesis producer init - begin");

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

      clientBuilder.setRegion((String) queueConfig.get("region"));
      clientBuilder.setClientConfiguration(clientConfig);

      kinesisClient = clientBuilder.build();

      streamName = (String) queueConfig.get("streamName");

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
