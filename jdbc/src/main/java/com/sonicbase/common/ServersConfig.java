package com.sonicbase.common;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.sonicbase.client.DatabaseClient;
import org.apache.giraph.utils.Varint;

import java.io.*;

public class ServersConfig {
  private boolean optimizeForThroughput;
  private String cluster;
  private Shard[] shards;
  private boolean clientIsInternal;

  public boolean shouldOptimizeForThroughput() {
    return optimizeForThroughput;
  }


  public static class Host {
    private String publicAddress;
    private String privateAddress;
    private int port;
    private boolean dead;

    public Host(String publicAddress, String privateAddress, int port) {
      this.publicAddress = publicAddress;
      this.privateAddress = privateAddress;
      this.port = port;
    }

    public String getPublicAddress() {
      return publicAddress;
    }

    public String getPrivateAddress() {
      return privateAddress;
    }

    public int getPort() {
      return port;
    }

    public Host(DataInputStream in, short serializationVersionNumber) throws IOException {
      publicAddress = in.readUTF();
      privateAddress = in.readUTF();
      port = in.readInt();
      if (serializationVersionNumber >= DatabaseClient.SERIALIZATION_VERSION_21) {
        dead = in.readBoolean();
      }
    }

    public void serialize(DataOutputStream out, short serializationVersionNumber) throws IOException {
      out.writeUTF(publicAddress);
      out.writeUTF(privateAddress);
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
        if (replicas[i].privateAddress.equals(host) && replicas[i].port == port) {
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

  /**
   * ###############################
   * DON"T MODIFY THIS SERIALIZATION
   * ###############################
   */
  public ServersConfig(DataInputStream in, short serializationVersion) throws IOException {
    if (serializationVersion >= DatabaseClient.SERIALIZATION_VERSION_21) {
      cluster = in.readUTF();
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

  /**
   * ###############################
   * DON"T MODIFY THIS SERIALIZATION
   * ###############################
   */
  public byte[] serialize(short serializationVersionNumber) throws IOException {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytesOut);
    serialize(out, serializationVersionNumber);
    out.close();
    return bytesOut.toByteArray();
  }

  public void serialize(DataOutputStream out, short serializationVersionNumber) throws IOException {
    out.writeUTF(cluster);
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

  public String getCluster() {
    return cluster;
  }

  public ServersConfig(String cluster, ArrayNode inShards, boolean clientIsInternal, boolean optimizedForThroughput) {
    this.cluster = cluster;
    int shardCount = inShards.size();
    shards = new Shard[shardCount];
    for (int i = 0; i < shardCount; i++) {
      ArrayNode replicas = (ArrayNode) inShards.get(i).withArray("replicas");
      Host[] hosts = new Host[replicas.size()];
      for (int j = 0; j < hosts.length; j++) {
        hosts[j] = new Host(replicas.get(j).get("publicAddress").asText(), replicas.get(j).get("privateAddress").asText(),
            (int) replicas.get(j).get("port").asLong());
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
        if (currHost.privateAddress.equals(host) && currHost.port == port) {
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

