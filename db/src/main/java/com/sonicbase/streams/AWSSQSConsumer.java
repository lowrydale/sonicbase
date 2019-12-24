package com.sonicbase.streams;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.sonicbase.common.Config;
import com.sonicbase.query.DatabaseException;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public class AWSSQSConsumer implements StreamsConsumer {

  private static Logger logger = LoggerFactory.getLogger(AWSSQSConsumer.class);

  private String url;
  private AmazonSQS sqsClient;
  private boolean shutdown;

  class AWSMessage extends com.sonicbase.streams.Message {
    private final com.amazonaws.services.sqs.model.Message message;

    public AWSMessage(com.amazonaws.services.sqs.model.Message message, String body) {
      super(body);
      this.message = message;
    }
  }

  public void shutdown() {
    this.shutdown = true;
    sqsClient.shutdown();
  }

  @Override
  public int init(Config config, String installDir, Map<String, Object> streamConfig) {
    try {
      logger.info("aws sqs init - begin");
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

      logger.info("aws sqs init - end: url=" + url);
      return (int) streamConfig.get("threadCount");
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
  public void acknowledgeMessages(List<Message> messages) {
    for (com.sonicbase.streams.Message message : messages) {
      sqsClient.deleteMessage(url, ((AWSMessage) message).message.getReceiptHandle());
    }
  }

  @Override
  public void handleError(List<Message> messages, Exception e) {

  }

}
