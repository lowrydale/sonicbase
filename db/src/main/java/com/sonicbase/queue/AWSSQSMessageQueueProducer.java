package com.sonicbase.queue;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.query.DatabaseException;
import org.apache.commons.lang.StringUtils;

import java.io.*;

public class AWSSQSMessageQueueProducer implements MessageQueueProducer {

  private String url;
  private AmazonSQSClient sqsClient;

  public File getInstallDir(ObjectNode config) {
    String dir = config.get("installDirectory").asText();
    return new File(dir.replace("$HOME", System.getProperty("user.home")));
  }

  @Override
  public void init(String cluster, String jsonConfig, String jsonQueueConfig) {
    try {
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
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public void publish(String message) {
    SendMessageRequest request = new SendMessageRequest();
    request.setMessageBody(message);
    request.setQueueUrl(url);
    SendMessageResult result = sqsClient.sendMessage(request);

    if (result == null || StringUtils.isEmpty(result.getMessageId())) {
      throw new DatabaseException("Error sending message");
    }

  }

  @Override
  public void shutdown() {

  }
}
