/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.cli;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.aws.EC2ToConfig;
import com.sonicbase.query.DatabaseException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class BuildConfig {

  static class Node {
    public String privateAddress;
    public String publicAddress;

    public Node(String publicIpAddress, String privateIpAddress) {
      this.publicAddress = publicIpAddress;
      this.privateAddress = privateIpAddress;
    }
  }

  public void buildConfig(String cluster, String filename) throws IOException {
    File file = new File(System.getProperty("user.dir"), "config/config-" + cluster + ".json");
    InputStream in = new FileInputStream(file);

    String json = IOUtils.toString(in, "utf-8");
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode config = (ObjectNode) mapper.readTree(json);

    buildConfig(config, filename);

    file.delete();
    try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))) {
      Object prettyJson = mapper.readValue(config.toString(), Object.class);
      String ret = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(prettyJson);
      writer.write(ret);
    }
    System.out.println("Finished building config");

  }

  public void buildConfig(ObjectNode config, String filename) throws IOException {
    List<Node> servers = new ArrayList<>();

    try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(filename)))) {
      String line = null;
      while ((line = reader.readLine()) != null) {
        servers.add(0, new Node(line, line));
      }
    }
    int replicationFactor = config.get("replicationFactor").asInt();

    if (servers.size() % replicationFactor != 0) {
      throw new DatabaseException("Server count not divisible by replication factor: serverCount=" + servers.size() + ", replicationFactor=" + replicationFactor);
    }
    int shardCount = servers.size() / replicationFactor;
    ArrayNode shards = new ArrayNode(JsonNodeFactory.instance);
    int nodeOffset = 0;
    for (int i = 0; i < shardCount; i++) {
      ObjectNode shard = shards.addObject();
      ArrayNode replicas = new ArrayNode(JsonNodeFactory.instance);
      shard.put("replicas", replicas);
      for (int j = 0; j < replicationFactor; j++) {
        ObjectNode replica = replicas.addObject();
        replica.put("publicAddress", servers.get(nodeOffset).publicAddress);
        replica.put("privateAddress", servers.get(nodeOffset).privateAddress);
        replica.put("port", 9010);
        replica.put("httpPort", 8080);
        nodeOffset++;
      }
    }
    config.put("shards", shards);
  }
}
