package com.sonicbase.queue;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.util.JsonDict;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class AWSSQSMessageQueueConsumer implements MessageQueueConsumer {

  private String url;
  private AmazonSQSClient sqsClient;
  private boolean shutdown;

  class AWSMessage extends com.sonicbase.queue.Message {
    private final com.amazonaws.services.sqs.model.Message message;
    private com.amazonaws.services.sqs.model.Message awsMessage;

    public AWSMessage(com.amazonaws.services.sqs.model.Message message, String body) {
      super(body);
      this.message = message;
    }
  }

  public File getInstallDir(JsonDict config) {
    String dir = config.getString("installDirectory");
    return new File(dir.replace("$HOME", System.getProperty("user.home")));
  }

  public void shutdown() {
    this.shutdown = true;
  }

  @Override
  public void init(String cluster, String jsonConfig, String jsonQueueConfig) {
    final ClientConfiguration clientConfig = new ClientConfiguration();
    clientConfig.setMaxConnections(10);
    clientConfig.setRequestTimeout(20_000);
    clientConfig.setConnectionTimeout(60_000);

    JsonDict config = new JsonDict(jsonConfig);
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
    JsonDict queueConfig = new JsonDict(jsonQueueConfig);
    url = queueConfig.getString("url");
  }

  @Override
  public List<Message> getMessages() {
    ReceiveMessageRequest request = new ReceiveMessageRequest(url);
    request.setMaxNumberOfMessages(10);
    request.setWaitTimeSeconds(10);
    ReceiveMessageResult receivedMessages = sqsClient.receiveMessage(request.withMessageAttributeNames("All"));

    List<com.amazonaws.services.sqs.model.Message> innerMessages = receivedMessages.getMessages();
    List<com.sonicbase.queue.Message> resultMessages = new ArrayList<>();
    for (com.amazonaws.services.sqs.model.Message message : innerMessages) {
      resultMessages.add(new AWSMessage(message, message.getBody()));
    }
    return resultMessages;
  }

  @Override
  public void acknowledgeMessage(com.sonicbase.queue.Message message) {
    sqsClient.deleteMessage(url, ((AWSMessage)message).message.getReceiptHandle());
  }

}
