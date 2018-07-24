package com.sonicbase.schema;

public class RecordIndexPartition {

  private int shardOwning;

  RecordIndexPartition() {
  }

  int getShardOwning() {
    return shardOwning;
  }

  void setShardOwning(int shardOwning) {
    this.shardOwning = shardOwning;
  }
}
