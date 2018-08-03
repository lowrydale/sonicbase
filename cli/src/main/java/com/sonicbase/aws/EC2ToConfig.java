package com.sonicbase.aws;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.query.DatabaseException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class EC2ToConfig {

  public static void main(String[] args) throws IOException {
    final String cluster = args[0];
    EC2ToConfig toConfig = new EC2ToConfig();
    toConfig.buildConfig(cluster);
  }

  public void buildConfig(String cluster) {
    try {
      File file = new File(System.getProperty("user.dir"), "../config/config-" + cluster + ".json");
      InputStream in = new FileInputStream(file);

      String json = IOUtils.toString(in, "utf-8");
      ObjectMapper mapper = new ObjectMapper();
      ObjectNode config = (ObjectNode) mapper.readTree(json);
      ObjectNode databaseDict = config;

      File keysFile = new File(System.getProperty("user.home"), ".awskeys");
      if (!keysFile.exists()) {
        throw new DatabaseException(".awskeys file not found");
      }
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(keysFile)))) {
        String accessKey = reader.readLine();
        String secretKey = reader.readLine();

        BasicAWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
        AmazonEC2 ec2 = AmazonEC2ClientBuilder.standard()
            .withRegion(Regions.US_EAST_1)
            .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
            .build();

        configureServers(ec2, databaseDict, cluster);
        configureClients(ec2, databaseDict, cluster);
      }

      file.delete();
      try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))) {
        Object prettyJson = mapper.readValue(config.toString(), Object.class);
        String ret = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(prettyJson);
        writer.write(ret);
      }
      File secondaryConfigDir = new File(System.getProperty("user.home"), "/Dropbox/sonicbase-config");
      if (secondaryConfigDir.exists()) {
        File destFile = new File(secondaryConfigDir, "config-" + cluster + ".json");
        FileUtils.copyFile(file, destFile);
      }
      System.out.println("Finished building config");
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void configureServers(AmazonEC2 ec2Client, ObjectNode databaseDict, String cluster) {
    DescribeInstancesRequest request = new DescribeInstancesRequest();

    List<String> valuesT1 = new ArrayList<>();
    valuesT1.add(cluster);
    Filter filter1 = new Filter("tag:sonicbase-server", valuesT1);
//    List<String> valuesSomeprop = new ArrayList<>();
//    valuesSomeprop.add("somevalue");
//    Filter filter2 = new Filter("tag:someprop", valuesSomeprop);

    DescribeInstancesResult result = ec2Client.describeInstances(request.withFilters(filter1));
    List<Reservation> reservations = result.getReservations();

    int replicationFactor = databaseDict.get("replicationFactor").asInt();

    boolean haveFirst = false;
    List<Node> servers = new ArrayList<>();
    for (Reservation reservation : reservations) {
      List<Instance> instances = reservation.getInstances();
      for (Instance instance : instances) {
        if (instance.getState().getName().equals("running")) {
          String publicAddress = instance.getPublicIpAddress();
          if (publicAddress == null || publicAddress.length() == 0) {
            publicAddress = instance.getPrivateIpAddress();
          }
          if (!haveFirst && !publicAddress.equals(instance.getPrivateIpAddress())) {
            haveFirst = true;
            servers.add(0, new Node(publicAddress, instance.getPrivateIpAddress()));
          }
          else {
            servers.add(new Node(publicAddress, instance.getPrivateIpAddress()));
          }
          System.out.println("publicIp=" + publicAddress + ", privateIp=" + instance.getPrivateIpAddress());
        }
      }
    }

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

    databaseDict.put("shards", shards);
  }

  public static void configureClients(AmazonEC2 ec2Client, ObjectNode databaseDict, String cluster) {
    DescribeInstancesRequest request = new DescribeInstancesRequest();

    List<String> valuesT1 = new ArrayList<>();
    valuesT1.add(cluster);
    Filter filter1 = new Filter("tag:sonicbase-client", valuesT1);

    DescribeInstancesResult result = ec2Client.describeInstances(request.withFilters(filter1));
    List<Reservation> reservations = result.getReservations();

    List<Node> clients = new ArrayList<>();
    for (Reservation reservation : reservations) {
      List<Instance> instances = reservation.getInstances();
      for (Instance instance : instances) {
        if (instance.getState().getName().equals("running")) {
          String publicAddress = instance.getPublicIpAddress();
          if (publicAddress == null || publicAddress.length() == 0) {
            publicAddress = instance.getPrivateIpAddress();
          }
          clients.add(new Node(publicAddress, instance.getPrivateIpAddress()));
          System.out.println("publicIp=" + publicAddress + ", privateIp=" + instance.getPrivateIpAddress());
        }
      }
    }

    ArrayNode clientArray = new ArrayNode(JsonNodeFactory.instance);
    int nodeOffset = 0;
    for (int i = 0; i < clients.size(); i++) {
      ObjectNode client = clientArray.addObject();
      client.put("publicAddress", clients.get(nodeOffset).publicAddress);
      client.put("privateAddress", clients.get(nodeOffset).privateAddress);
      client.put("port", 8080);
      nodeOffset++;
    }
    databaseDict.put("clients", clientArray);

  }

  static class Node {
    public String privateAddress;
    public String publicAddress;

    public Node(String publicIpAddress, String privateIpAddress) {
      this.publicAddress = publicIpAddress;
      this.privateAddress = privateIpAddress;
    }
  }
}
