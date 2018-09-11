/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.cli;

import com.sonicbase.common.Config;
import com.sonicbase.query.DatabaseException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.util.*;

public class BuildConfig {

  static class Node {
    public final String privateAddress;
    public final String publicAddress;

    Node(String publicIpAddress, String privateIpAddress) {
      this.publicAddress = publicIpAddress;
      this.privateAddress = privateIpAddress;
    }
  }

  void buildConfig(String cluster, String filename) throws IOException {
    File file = new File(System.getProperty("user.dir"), "config/config-" + cluster + ".yaml");
    InputStream in = new FileInputStream(file);
    String json = IOUtils.toString(in, "utf-8");
    Config config = new Config(json);

    List<Config.Shard> shards = buildConfig(config, filename);

    String newYaml = dumpYaml(config, shards, null);

    FileUtils.forceDelete(file);
    try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))) {
      writer.write(newYaml);
    }
    System.out.println("Finished building config");

  }

  public static String dumpYaml(Config config, List<Config.Shard> shards, List<Config.Client> clients) {
    Map<String, Object> configMap = config.getMap();
    List<Map<String, Object>> shardsList = new ArrayList<>();
    for (Config.Shard shard : shards) {
      Map<String, Object> shardMap = new LinkedHashMap<>();
      shardsList.add(shardMap);
      List<Map<String, Object>> replicasList = new ArrayList<>();
      Map<String, Object> replicasMap = new LinkedHashMap<>();
      replicasMap.put("replicas", replicasList);
      shardMap.put("shard", replicasMap);
      List<Config.Replica> replicas = shard.getReplicas();
      for (Config.Replica replica : replicas) {
        Map<String, Object> replicaMap = new LinkedHashMap<>();
        replicaMap.put("replica", replica.getMap());
        replicasList.add(replicaMap);
      }
    }
    List<Map<String, Object>> clientList = new ArrayList<>();
    if (clients != null) {
      for (Config.Client client : clients) {
        Map<String, Object> clientMap = new LinkedHashMap<>();
        clientMap.put("client", client.getMap());
        clientList.add(clientMap);
      }
    }
    if (!clientList.isEmpty()) {
      configMap.put("clients", clientList);
    }
    configMap.put("shards", shardsList);

    return new Yaml().dumpAsMap(configMap);
  }

  private List<Config.Shard> buildConfig(Config config, String filename) throws IOException {
    List<Node> servers = new ArrayList<>();

    try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(filename)))) {
      String line = null;
      while ((line = reader.readLine()) != null) {
        servers.add(new Node(line, line));
      }
    }
    int replicationFactor = config.getInt("replicationFactor");

    if (servers.size() % replicationFactor != 0) {
      throw new DatabaseException("Server count not divisible by replication factor: serverCount=" + servers.size() + ", replicationFactor=" + replicationFactor);
    }
    int shardCount = servers.size() / replicationFactor;
    List<Config.Shard> shards = config.getShards();
    shards.clear();
    int nodeOffset = 0;
    for (int i = 0; i < shardCount; i++) {
      Config.Shard shard = new Config.Shard();
      shards.add(shard);
      List<Config.Replica> replicas = shard.getReplicas();
      for (int j = 0; j < replicationFactor; j++) {
        Config.Replica replica = new Config.Replica(new LinkedHashMap<>());
        replicas.add(replica);
        replica.put("publicAddress", servers.get(nodeOffset).publicAddress);
        replica.put("privateAddress", servers.get(nodeOffset).privateAddress);
        replica.put("port", 9010);
        replica.put("httpPort", 8080);
        nodeOffset++;
      }
    }
    return shards;
  }
}
