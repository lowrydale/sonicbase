package com.sonicbase.client;

/**
 * Created by lowryda on 7/28/17.
 */
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