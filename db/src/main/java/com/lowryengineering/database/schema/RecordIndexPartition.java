package com.lowryengineering.database.schema;

/**
 * Responsible for
 */
public class RecordIndexPartition {

  private int shardOwning;

  public RecordIndexPartition() {
  }

  public RecordIndexPartition(int shardOwning) {
    this.shardOwning = shardOwning;
  }

  public int getShardOwning() {
    return shardOwning;
  }

  public void setShardOwning(int shardOwning) {
    this.shardOwning = shardOwning;
  }
}
