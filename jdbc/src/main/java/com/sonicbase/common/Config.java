/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.common;

import com.sonicbase.query.DatabaseException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Config extends Properties {

  public Config(Map<String, Object> map) {
    super(map);
  }

  public Config(String rawConfig) {
    super(rawConfig);
  }

  public List<Client> getClients() {
    Integer defaultHttpPort = (Integer) getMap().get("defaultHttpPort");
    if (defaultHttpPort == null) {
      defaultHttpPort = 8080;
    }
    List<Client> ret = new ArrayList<>();
    List<Map<String, Object>> array = (List<Map<String, Object>>) getMap().get("clients");
    if (array != null){
      for (Map<String, Object> clientObj : array) {
        Map<String, Object> cli = (Map<String, Object>) clientObj.get("client");
        if (cli.get("port") == null) {
          cli.put("port", defaultHttpPort);
        }
        String address = (String) cli.get("address");
        if (address != null) {
          cli.put("privateAddress", address);
          cli.put("publicAddress", address);
        }

        Client client = new Client(cli);
        ret.add(client);
      }
    }
    return ret;
  }

  public List<Shard> getShards() {
    List<Shard> ret1 = convertServersToShards();
    if (ret1 != null) {
      return ret1;
    }
    List<Shard> ret = new ArrayList<>();
    List<Map<String, Object>> array = (List<Map<String, Object>>) getMap().get("shards");
    if (array != null) {
      for (Map<String, Object> yamlShard : array) {
        Shard shard = new Shard();
        yamlShard = (Map<String, Object>) yamlShard.get("shard");
        List<Map<String, Object>> replicasArray = (List<Map<String, Object>>) yamlShard.get("replicas");
        for (Map<String, Object> yamlReplica : replicasArray) {
          Replica replica = new Replica((Map<String, Object>) yamlReplica.get("replica"));
          shard.replicas.add(replica);
        }
        ret.add(shard);
      }
    }
    return ret;
  }

  private List<Shard> convertServersToShards() {
    List<Map<String, Object>> servers = (List<Map<String, Object>>) getMap().get("servers");
    if (servers != null) {
      Integer defaultPort = (Integer) getMap().get("defaultPort");
      if (defaultPort == null) {
        defaultPort = 9010;
      }
      Integer defaultHttpPort = (Integer) getMap().get("defaultHttpPort");
      if (defaultHttpPort == null) {
        defaultHttpPort = 8080;
      }
      List<Shard> ret = new ArrayList<>();
      Integer replicationFactor = (Integer) getMap().get("replicationFactor");
      if (replicationFactor == null) {
        throw new DatabaseException("replicationFactor must be specified if using 'servers'");
      }
      int offset = 0;
      while (offset < servers.size()) {
        Shard shard = new Shard();
        for (int i = 0; i < replicationFactor; i++) {
          if (offset >= servers.size()) {
            throw new DatabaseException("Number of servers is not divisible by replicationFactor");
          }
          Map<String, Object> server = servers.get(offset);
          server = (Map<String, Object>) server.get("server");
          if (server.get("port") == null) {
            server.put("port", defaultPort);
          }
          if (server.get("httpPort") == null) {
            server.put("httpPort", defaultHttpPort);
          }
          String address = (String) server.get("address");
          if (address != null) {
            server.put("privateAddress", address);
            server.put("publicAddress", address);
          }
          Replica replica = new Replica(server);
          shard.replicas.add(replica);
          offset++;
        }
        ret.add(shard);
      }
      return ret;
    }
    return null;
  }

  public static class Client extends Properties {
    public Client(Map<String, Object> clientObj) {
      super(clientObj);
    }
  }

  public static class Shard {
    private List<Replica> replicas = new ArrayList<>();

    public List<Replica> getReplicas() {
      return replicas;
    }
  }

  public static class Replica extends Properties {
    public Replica(Map<String, Object> yaml) {
      super(yaml);
    }
  }

}
