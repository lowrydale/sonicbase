package com.sonicbase.client;


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