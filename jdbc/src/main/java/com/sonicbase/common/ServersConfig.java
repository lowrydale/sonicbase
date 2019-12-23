package com.sonicbase.common;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.util.Varint;

import java.io.*;
import java.util.List;

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class ServersConfig {
  private boolean optimizeForThroughput;
  private Shard[] shards;
  private boolean clientIsInternal;

  public ServersConfig() {
  }

  public boolean shouldOptimizeForThroughput() {
    return optimizeForThroughput;
  }

  public void setShards(Shard[] shards) {
    this.shards = shards;
  }

  public static class Host {
    private String address;
    private int port;
    private boolean dead;

    private Host(String address, int port) {
      this.address = address;
      this.port = port;
    }

    public String getaddress() {
      return address;
    }

    public int getPort() {
      return port;
    }

    public Host(String address, int port, boolean dead) {
      this.address = address;
      this.port = port;
      this.dead = dead;
    }

    public Host(DataInputStream in, short serializationVersionNumber) throws IOException {
      address = in.readUTF();
      port = in.readInt();
      if (serializationVersionNumber >= DatabaseClient.SERIALIZATION_VERSION_21) {
        dead = in.readBoolean();
      }
    }

    public void serialize(DataOutputStream out, short serializationVersionNumber) throws IOException {
      out.writeUTF(address);
      out.writeInt(port);
      if (serializationVersionNumber >= DatabaseClient.SERIALIZATION_VERSION_21) {
        out.writeBoolean(dead);
      }
    }

    public boolean isDead() {
      return dead;
    }

    public void setDead(boolean dead) {
      this.dead = dead;
    }
  }

  public static class Shard {
    private Host[] replicas;
    private int masterReplica;

    public Shard(Host[] hosts) {
      this.replicas = hosts;
    }

    public Shard(DataInputStream in, short serializationVersionNumber) throws IOException {
      int count = in.readInt();
      replicas = new Host[count];
      for (int i = 0; i < replicas.length; i++) {
        replicas[i] = new Host(in, serializationVersionNumber);
      }
      if (serializationVersionNumber >= DatabaseClient.SERIALIZATION_VERSION_21) {
        masterReplica = (int) Varint.readSignedVarLong(in);
      }
    }

    public void serialize(DataOutputStream out, short serializationVersionNumber) throws IOException {
      out.writeInt(replicas.length);
      for (Host host : replicas) {
        host.serialize(out, serializationVersionNumber);
      }
      if (serializationVersionNumber >= DatabaseClient.SERIALIZATION_VERSION_21) {
        Varint.writeSignedVarLong(masterReplica, out);
      }
    }

    public void setMasterReplica(int masterReplica) {
      this.masterReplica = masterReplica;
    }

    public int getMasterReplica() {
      return this.masterReplica;
    }

    public boolean contains(String host, int port) {
      for (int i = 0; i < replicas.length; i++) {
        if (replicas[i].address.equals(host) && replicas[i].port == port) {
          return true;
        }
      }
      return false;
    }

    public Host[] getReplicas() {
      return replicas;
    }

  }

  public ServersConfig(byte[] bytes, short serializationVersion) throws IOException {
    this(new DataInputStream(new ByteArrayInputStream(bytes)), serializationVersion);
  }

  public ServersConfig(DataInputStream in, short serializationVersion) throws IOException {
    if (serializationVersion >= DatabaseClient.SERIALIZATION_VERSION_21 &&
        serializationVersion < DatabaseClient.SERIALIZATION_VERSION_31) {
      in.readUTF();
    }
    int count = in.readInt();
    shards = new Shard[count];
    for (int i = 0; i < count; i++) {
      shards[i] = new Shard(in, serializationVersion);
    }
    clientIsInternal = in.readBoolean();
    if (serializationVersion >= DatabaseClient.SERIALIZATION_VERSION_25) {
      optimizeForThroughput = in.readBoolean();
    }
  }

  public byte[] serialize(short serializationVersionNumber) throws IOException {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytesOut);
    serialize(out, serializationVersionNumber);
    out.close();
    return bytesOut.toByteArray();
  }

  public void serialize(DataOutputStream out, short serializationVersionNumber) throws IOException {
    out.writeInt(shards.length);
    for (Shard shard : shards) {
      shard.serialize(out, serializationVersionNumber);
    }
    out.writeBoolean(clientIsInternal);
    out.writeBoolean(optimizeForThroughput);
  }

  public Shard[] getShards() {
    return shards;
  }

  public int getShardCount() {
    return shards.length;
  }

  public ServersConfig(ArrayNode inShards, boolean clientIsInternal, boolean optimizedForThroughput) {
    int shardCount = inShards.size();
    shards = new Shard[shardCount];
    for (int i = 0; i < shardCount; i++) {
      ArrayNode replicas = (ArrayNode) inShards.get(i).withArray("replicas");
      Host[] hosts = new Host[replicas.size()];
      for (int j = 0; j < hosts.length; j++) {
        hosts[j] = new Host(replicas.get(j).get("address").asText(),
            (int) replicas.get(j).get("port").asLong());
      }
      shards[i] = new Shard(hosts);

    }
    this.clientIsInternal = clientIsInternal;
    this.optimizeForThroughput = optimizedForThroughput;
  }


  public ServersConfig(List<Config.Shard> inShards, boolean clientIsInternal, boolean optimizedForThroughput) {
    int shardCount = inShards.size();
    shards = new Shard[shardCount];
    for (int i = 0; i < shardCount; i++) {
      List<Config.Replica> replicas = inShards.get(i).getReplicas();
      Host[] hosts = new Host[replicas.size()];
      for (int j = 0; j < hosts.length; j++) {
        hosts[j] = new Host(replicas.get(j).getString("address"),
            replicas.get(j).getInt("port"));
      }
      shards[i] = new Shard(hosts);

    }
    this.clientIsInternal = clientIsInternal;
    this.optimizeForThroughput = optimizedForThroughput;
  }

  public int getThisReplica(String host, int port) {
    for (int i = 0; i < shards.length; i++) {
      for (int j = 0; j < shards[i].replicas.length; j++) {
        Host currHost = shards[i].replicas[j];
        if (currHost.address.equals(host) && currHost.port == port) {
          return j;
        }
      }
    }
    return -1;
  }

  public int getThisShard(String host, int port) {
    for (int i = 0; i < shards.length; i++) {
      if (shards[i].contains(host, port)) {
        return i;
      }
    }
    return -1;
  }

  public boolean clientIsInternal() {
    return clientIsInternal;
  }
}

