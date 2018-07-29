package com.sonicbase.schema;

import java.util.Comparator;
import java.util.List;

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class IndexSchema {

  private boolean isUnique;
  private boolean isPrimaryKeyGroup;
  private boolean isPrimaryKey;
  private String name;
  private int indexId;
  String[] fields;
  private Comparator[] comparators;
  private TableSchema.Partition[] lastPartitions;
  private TableSchema.Partition[] currPartitions;
  private int[] fieldOffsets;

  public IndexSchema(String name, int indexId, boolean isUnique, String[] fields, Comparator[] comparators,
                     TableSchema.Partition[] partitions, boolean isPrimaryKey, boolean isPrimaryKeyGroup, TableSchema tableSchema) {
    this.name = name;
    this.indexId = indexId;
    this.isUnique = isUnique;
    this.fields = fields;
    for (int i = 0; i < fields.length; i++) {
      this.fields[i] = fields[i].toLowerCase();
    }
    calculateFieldOffsets(tableSchema);
    this.comparators = comparators;
    this.currPartitions = partitions;
    this.isPrimaryKey = isPrimaryKey;
    this.isPrimaryKeyGroup = isPrimaryKeyGroup;
  }

  public IndexSchema() {

  }

  public boolean isPrimaryKeyGroup() {
    return isPrimaryKeyGroup;
  }

  public boolean isPrimaryKey() {
    return isPrimaryKey;
  }

  public String getName() {
    return name;
  }

  public void setFields(String[] fields, TableSchema tableSchema) {
    this.fields = fields;
    calculateFieldOffsets(tableSchema);
  }

  private void calculateFieldOffsets(TableSchema tableSchema) {
    this.fieldOffsets = new int[fields.length];
    for (int i = 0; i < fieldOffsets.length; i++) {
      fieldOffsets[i] = tableSchema.getFieldOffset(fields[i]);
    }
  }

  public void setCurrPartitions(TableSchema.Partition[] partitions) {
    this.currPartitions = partitions;
  }

  public void setLastPartitions(TableSchema.Partition[] partitions) {
    this.lastPartitions = partitions;
  }

  public String[] getFields() {
    return fields;
  }

  public TableSchema.Partition[] getCurrPartitions() {
    return currPartitions;
  }

  public TableSchema.Partition[] getLastPartitions() {
    return lastPartitions;
  }

  public void setComparators(Comparator[] comparators) {
    this.comparators = comparators;
  }

  public int[] getFieldOffsets() {
    return fieldOffsets;
  }

  public Comparator[] getComparators() {
    return comparators;
  }

  public void reshardPartitions(List<TableSchema.Partition> newPartitions) {
    this.lastPartitions = currPartitions;
    this.currPartitions = new TableSchema.Partition[newPartitions.size()];
    for (int i = 0; i < currPartitions.length; i++) {
      currPartitions[i] = newPartitions.get(i);
    }
  }

  public void deleteLastPartitions() {
    lastPartitions = null;
  }

  public int getIndexId() {
    return indexId;
  }

  public void setIndexId(int indexId) {
    this.indexId = indexId;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setIsPrimaryKey(boolean isPrimaryKey) {
    this.isPrimaryKey = isPrimaryKey;
  }

  public void setIsPrimaryKeyGroup(boolean isPrimaryKeyGroup) {
    this.isPrimaryKeyGroup = isPrimaryKeyGroup;
  }

  public boolean isUnique() {
    return isUnique;
  }

  public void setIsUnique(boolean isUnique) {
    this.isUnique = isUnique;
  }
}
