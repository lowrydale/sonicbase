/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.common;

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
    List<Client> ret = new ArrayList<>();
    List<Map<String, Object>> array = (List<Map<String, Object>>) getMap().get("clients");
    if (array != null){
      for (Map<String, Object> clientObj : array) {
        Client client = new Client((Map<String, Object>) clientObj.get("client"));
        ret.add(client);
      }
    }
    return ret;
  }

  public List<Shard> getShards() {
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
