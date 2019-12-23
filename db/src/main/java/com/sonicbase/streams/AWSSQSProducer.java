package com.sonicbase.streams;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.BatchResultErrorEntry;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageBatchResult;
import com.sonicbase.common.Config;
import com.sonicbase.query.DatabaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

public class AWSSQSProducer implements StreamsProducer {

  private static Logger logger = LoggerFactory.getLogger(AWSSQSProducer.class);

  private String url;
  private AmazonSQS sqsClient;

  public File getInstallDir(Config config) {
    String dir = config.getString("installDirectory");
    return new File(dir.replace("$HOME", System.getProperty("user.home")));
  }

  @Override
  public void init(Config config, String installDir, Map<String, Object> streamConfig) {
    try {
      logger.info("aws sqs producer init - begin");
      final ClientConfiguration clientConfig = new ClientConfiguration();
      clientConfig.setMaxConnections(100);
      clientConfig.setRequestTimeout(20_000);
      clientConfig.setConnectionTimeout(60_000);

      AmazonSQSClientBuilder builder = AmazonSQSClient.builder();

      File keysFile = new File(installDir, "/keys/sonicbase-awskeys");
      if (!keysFile.exists()) {
        builder.setCredentials(new InstanceProfileCredentialsProvider(true));
      }
      else {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(keysFile)))) {
          String accessKey = reader.readLine();
          String secretKey = reader.readLine();

          BasicAWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
          builder.setCredentials(new AWSStaticCredentialsProvider(awsCredentials));
        }
        catch (IOException e) {
          throw new DatabaseException(e);
        }
      }
      builder.setClientConfiguration(clientConfig);
      sqsClient = builder.build();

      url = (String) streamConfig.get("url");

      logger.info("aws sqs producer init - end");
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public void publish(List<String> messages) {
    try {
      Map<String, MessageAttributeValue> attributes = new HashMap<>();
      List<SendMessageBatchRequestEntry> entries = new ArrayList<>();
      int offset = 0;
      for (String message : messages) {
        SendMessageBatchRequestEntry entry = new SendMessageBatchRequestEntry();

        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        GZIPOutputStream out = new GZIPOutputStream(bytesOut);
        out.write(message.getBytes("utf-8"));
        out.close();

        entry.setId(String.valueOf(offset));
        entry.setMessageBody("SonicBase Publish");
        MessageAttributeValue attrib = new MessageAttributeValue().withDataType("Binary").withBinaryValue(ByteBuffer.wrap(bytesOut.toByteArray()));
        attributes.put(String.valueOf(offset), attrib);
        entry.addMessageAttributesEntry("message", attrib);
        entries.add(entry);
        offset++;
      }
      SendMessageBatchResult result = sqsClient.sendMessageBatch(url, entries);

      List<BatchResultErrorEntry> failed = result.getFailed();
      for (int i = 0; i < 10 && !failed.isEmpty(); i++) {
        entries.clear();

        logger.error("Error publishing message: count=" + failed.size());

        for (BatchResultErrorEntry curr : failed) {
          SendMessageBatchRequestEntry entry = new SendMessageBatchRequestEntry();
          entry.setMessageBody(curr.getMessage());
          String id = curr.getId();
          entry.addMessageAttributesEntry("message", attributes.get(id));
          entries.add(entry);
        }

        result = sqsClient.sendMessageBatch(url, entries);
        failed = result.getFailed();

        if (i == 9 && !failed.isEmpty()) {
          throw new DatabaseException("Error publishing message: count=" + failed.size());
        }
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public void shutdown() {
    sqsClient.shutdown();
  }
}
