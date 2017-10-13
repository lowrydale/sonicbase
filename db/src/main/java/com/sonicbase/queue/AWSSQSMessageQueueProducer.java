/* © 2017 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.queue;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.util.JsonDict;
import org.apache.commons.lang.StringUtils;

import java.io.*;

public class AWSSQSMessageQueueProducer implements MessageQueueProducer {

  private String url;
  private AmazonSQSClient sqsClient;

  public File getInstallDir(JsonDict config) {
    String dir = config.getString("installDirectory");
    return new File(dir.replace("$HOME", System.getProperty("user.home")));
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
  public void publish(String message) {
    SendMessageResult result = sqsClient.sendMessage(url, message);
    if (result == null || StringUtils.isEmpty(result.getMessageId())) {
      throw new DatabaseException("Error sending message");
    }

  }
}
