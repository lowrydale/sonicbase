package com.sonicbase.schema;

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
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
