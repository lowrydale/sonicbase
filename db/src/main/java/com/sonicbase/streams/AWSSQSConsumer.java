package com.sonicbase.streams;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.query.DatabaseException;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class AWSSQSConsumer implements StreamsConsumer {

  private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(AWSSQSConsumer.class);

  private String url;
  private AmazonSQSClient sqsClient;
  private boolean shutdown;

  class AWSMessage extends Message {
    private final com.amazonaws.services.sqs.model.Message message;

    public AWSMessage(com.amazonaws.services.sqs.model.Message message, String body) {
      super(body);
      this.message = message;
    }
  }

  public File getInstallDir(ObjectNode config) {
    String dir = config.get("installDirectory").asText();
    return new File(dir.replace("$HOME", System.getProperty("user.home")));
  }

  public void shutdown() {
    this.shutdown = true;
    sqsClient.shutdown();
  }

  @Override
  public int init(String cluster, String jsonConfig, String jsonQueueConfig) {
    try {
      logger.info("aws sqs init - begin");
      final ClientConfiguration clientConfig = new ClientConfiguration();
      clientConfig.setMaxConnections(10);
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

      logger.info("aws sqs init - end: url=" + url);
      return queueConfig.get("threadCount").asInt();
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public void initThread() {

  }

  @Override
  public List<Message> receive() {
    try {
      ReceiveMessageRequest request = new ReceiveMessageRequest(url);
      request.setMaxNumberOfMessages(10);
      request.setWaitTimeSeconds(10);
      ReceiveMessageResult receivedMessages = sqsClient.receiveMessage(request.withMessageAttributeNames("All"));

      List<com.amazonaws.services.sqs.model.Message> innerMessages = receivedMessages.getMessages();
      List<Message> resultMessages = new ArrayList<>();
      for (com.amazonaws.services.sqs.model.Message message : innerMessages) {
        ByteBuffer buffer = message.getMessageAttributes().get("message").getBinaryValue();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);

        GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(bytes));
        bytes = IOUtils.toByteArray(in);

        resultMessages.add(new AWSMessage(message, new String(bytes, "utf-8")));
      }
      return resultMessages;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public void acknowledgeMessage(Message message) {
    sqsClient.deleteMessage(url, ((AWSMessage)message).message.getReceiptHandle());
  }
}
