package com.sonicbase.client;


@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class ReconfigureResults {
  private boolean handedOffToMaster;
  private int shardCount;

  public ReconfigureResults(boolean handedOffToMaster, int shardCount) {
    this.handedOffToMaster = handedOffToMaster;
    this.shardCount = shardCount;
  }

  public boolean isHandedOffToMaster() {
    return handedOffToMaster;
  }

  public int getShardCount() {
    return shardCount;
  }
}