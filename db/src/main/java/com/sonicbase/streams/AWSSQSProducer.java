package com.sonicbase.streams;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.query.DatabaseException;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

public class AWSSQSProducer implements StreamsProducer {

  private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(AWSSQSProducer.class);

  private String url;
  private AmazonSQSClient sqsClient;

  public File getInstallDir(ObjectNode config) {
    String dir = config.get("installDirectory").asText();
    return new File(dir.replace("$HOME", System.getProperty("user.home")));
  }

  @Override
  public void init(String cluster, String jsonConfig, String jsonQueueConfig) {
    try {
      logger.info("aws sqs producer init - begin");
      final ClientConfiguration clientConfig = new ClientConfiguration();
      clientConfig.setMaxConnections(1000);
      clientConfig.setRequestTimeout(20_000);
      clientConfig.setConnectionTimeout(60_000);

      ObjectMapper mapper = new ObjectMapper();
      ObjectNode config = (ObjectNode) mapper.readTree(jsonConfig);
      File installDir = getInstallDir(config);
      File keysFile = new File(installDir, "/keys/" + cluster + "-awskeys");
      if (!keysFile.exists()) {
        throw new DatabaseException(cluster + "-awskeys file not found");
      }
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(keysFile)))) {
        String accessKey = reader.readLine();
        String secretKey = reader.readLine();

        BasicAWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
        sqsClient = new AmazonSQSClient(awsCredentials, clientConfig);
      }
      catch (IOException e) {
        throw new DatabaseException(e);
      }
      ObjectNode queueConfig = (ObjectNode) mapper.readTree(jsonQueueConfig);
      url = queueConfig.get("url").asText();

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
