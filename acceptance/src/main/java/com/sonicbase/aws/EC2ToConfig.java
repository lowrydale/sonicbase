package com.sonicbase.aws;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import com.sonicbase.cli.BuildConfig;
import com.sonicbase.common.Config;
import com.sonicbase.query.DatabaseException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class EC2ToConfig {

  public static void main(String[] args) {
    final String cluster = args[0];
    EC2ToConfig toConfig = new EC2ToConfig();
    toConfig.buildConfig(cluster);
  }

  public void buildConfig(String cluster) {
    try {
      File file = new File(System.getProperty("user.dir"), "../config/config-" + cluster + ".yaml");
      InputStream in = new FileInputStream(file);

      String json = IOUtils.toString(in, "utf-8");
      Config config = new Config(json);

      File keysFile = new File(System.getProperty("user.home"), ".awskeys");
      if (!keysFile.exists()) {
        throw new DatabaseException(".awskeys file not found");
      }
      String accessKey;
      String secretKey;
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(keysFile)))) {
        accessKey = reader.readLine();
        secretKey = reader.readLine();

      }
      BasicAWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
      AmazonEC2 ec2 = AmazonEC2ClientBuilder.standard()
          .withRegion(Regions.US_EAST_1)
          .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
          .build();

      List<Config.Shard> shards = configureServers(ec2, config, cluster);
      List<Config.Client> clients = configureClients(ec2, config, cluster);

      String newYaml = BuildConfig.dumpYaml(config, shards, clients);

      FileUtils.forceDelete(file);
      try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))) {
        writer.write(newYaml);
      }
      File secondaryConfigDir = new File(System.getProperty("user.home"), "/Dropbox/sonicbase-config");
      if (secondaryConfigDir.exists()) {
        File destFile = new File(secondaryConfigDir, "config-" + cluster + ".yaml");
        FileUtils.copyFile(file, destFile);
      }
      System.out.println("Finished building config");
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static List<Config.Shard> configureServers(AmazonEC2 ec2Client, Config config, String cluster) throws Exception {
    DescribeInstancesRequest request = new DescribeInstancesRequest();

    List<String> valuesT1 = new ArrayList<>();
    valuesT1.add(cluster);
    Filter filter1 = new Filter("tag:sonicbase-server", valuesT1);

    DescribeInstancesResult result = ec2Client.describeInstances(request.withFilters(filter1));
    List<Reservation> reservations = result.getReservations();

    int replicationFactor = config.getInt("replicationFactor");

    List<Node> servers = new ArrayList<>();
    for (Reservation reservation : reservations) {
      List<Instance> instances = reservation.getInstances();
      for (Instance instance : instances) {
        if (instance.getState().getName().equals("running")) {
          servers.add(new Node(instance.getPrivateIpAddress()));
          System.out.println("privateIp=" + instance.getPrivateIpAddress());
        }
      }
    }

    if (servers.size() % replicationFactor != 0) {
      throw new DatabaseException("Server count not divisible by replication factor: serverCount=" + servers.size() + ", replicationFactor=" + replicationFactor);
    }
    int shardCount = servers.size() / replicationFactor;
    List<Config.Shard> shards = new ArrayList<>();
    int nodeOffset = 0;
    for (int i = 0; i < shardCount; i++) {
      Config.Shard shard = new Config.Shard();
      shards.add(shard);
      List<Config.Replica> replicas = shard.getReplicas();
      for (int j = 0; j < replicationFactor; j++) {
        Config.Replica replica = new Config.Replica(new HashMap<>());
        replicas.add(replica);

        replica.put("address", servers.get(nodeOffset).address);
        replica.put("port", 9010);
        replica.put("httpPort", 8080);
        nodeOffset++;
      }
    }

    return shards;
  }

  private static List<Config.Client> configureClients(AmazonEC2 ec2Client, Config config, String cluster) {
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
          clients.add(new Node(instance.getPrivateIpAddress()));
          System.out.println("privateIp=" + instance.getPrivateIpAddress());
        }
      }
    }

    List<Config.Client> clientArray = new ArrayList<>();
    int nodeOffset = 0;
    for (int i = 0; i < clients.size(); i++) {
      Config.Client client = new Config.Client(new HashMap<>());
      clientArray.add(client);
      client.put("address", clients.get(nodeOffset).address);
      client.put("port", 8080);
      nodeOffset++;
    }
    return clientArray;
  }

  static class Node {
    public final String address;

    Node(String privateIpAddress) {
      this.address = privateIpAddress;
    }
  }
}
