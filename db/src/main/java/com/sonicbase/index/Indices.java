package com.sonicbase.index;

import com.sonicbase.schema.TableSchema;

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Indices {
  private ConcurrentHashMap<String, ConcurrentHashMap<String, Index>> indexes = new ConcurrentHashMap<>();

  public Map<String, ConcurrentHashMap<String, Index>> getIndices() {
    return indexes;
  }

  public void addIndex(TableSchema tableSchema, String indexName, Comparator[] comparators) {
    Index index = new Index(tableSchema, indexName, comparators);
    ConcurrentHashMap<String, Index> tableIndexes = indexes.get(tableSchema.getName());
    if (tableIndexes == null) {
      tableIndexes = new ConcurrentHashMap<>();
      indexes.put(tableSchema.getName(), tableIndexes);
    }
    tableIndexes.put(indexName, index);
  }
}
