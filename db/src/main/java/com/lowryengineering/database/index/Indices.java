package com.lowryengineering.database.index;

import com.lowryengineering.database.schema.TableSchema;

import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;

public class Indices {
  private ConcurrentHashMap<String, ConcurrentHashMap<String, Index>> indexes = new ConcurrentHashMap<String, ConcurrentHashMap<String, Index>>();

  public ConcurrentHashMap<String, ConcurrentHashMap<String, Index>> getIndices() {
    return indexes;
  }

  public void addIndex(TableSchema tableSchema, String indexName, Comparator[] comparators) {
    Index index = new Index(tableSchema, indexName, comparators);
    ConcurrentHashMap<String, Index> tableIndexes = indexes.get(tableSchema.getName());
    if (tableIndexes == null) {
      tableIndexes = new ConcurrentHashMap<String, Index>();
      indexes.put(tableSchema.getName(), tableIndexes);
    }
    tableIndexes.put(indexName, index);
  }
}
