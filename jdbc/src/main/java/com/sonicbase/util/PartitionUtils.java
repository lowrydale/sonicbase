package com.sonicbase.util;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.ComArray;
import com.sonicbase.common.ComObject;
import com.sonicbase.query.BinaryExpression;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.query.impl.OrderByExpressionImpl;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

public class PartitionUtils {
  private PartitionUtils() {
  }

  public static class IndexCounts {
    private Map<Integer, Long> counts = new ConcurrentHashMap<>();

    public Map<Integer, Long> getCounts() {
      return counts;
    }
  }

  public static class TableIndexCounts {
    private Map<String, IndexCounts> indices = new ConcurrentHashMap<>();

    public Map<String, IndexCounts> getIndices() {
      return indices;
    }
  }

  public static class GlobalIndexCounts {
    private Map<String, TableIndexCounts> tables = new ConcurrentHashMap<>();

    public Map<String, TableIndexCounts> getTables() {
      return tables;
    }

  }
  public static GlobalIndexCounts getIndexCounts(final String dbName, final DatabaseClient client) {
    try {
      final GlobalIndexCounts ret = new GlobalIndexCounts();
      List<Future> futures = new ArrayList<>();
      for (int i = 0; i < client.getShardCount(); i++) {
        final int shard = i;
        futures.add(client.getExecutor().submit((Callable) () -> {
          ComObject cobj = new ComObject();
          cobj.put(ComObject.Tag.DB_NAME, dbName);
          cobj.put(ComObject.Tag.SCHEMA_VERSION, client.getCommon().getSchemaVersion());
          byte[] response = client.send("PartitionManager:getIndexCounts", shard, 0, cobj, DatabaseClient.Replica.MASTER);
          synchronized (ret) {
            ComObject retObj = new ComObject(response);
            ComArray tables = retObj.getArray(ComObject.Tag.TABLES);
            if (tables != null) {
              for (int i1 = 0; i1 < tables.getArray().size(); i1++) {
                ComObject tableObj = (ComObject) tables.getArray().get(i1);
                String tableName = tableObj.getString(ComObject.Tag.TABLE_NAME);

                TableIndexCounts tableIndexCounts = ret.tables.get(tableName);
                if (tableIndexCounts == null) {
                  tableIndexCounts = new TableIndexCounts();
                  ret.tables.put(tableName, tableIndexCounts);
                }
                ComArray indices = tableObj.getArray(ComObject.Tag.INDICES);
                if (indices != null) {
                  for (int j = 0; j < indices.getArray().size(); j++) {
                    ComObject indexObj = (ComObject) indices.getArray().get(j);
                    String indexName = indexObj.getString(ComObject.Tag.INDEX_NAME);
                    long size = indexObj.getLong(ComObject.Tag.SIZE);
                    IndexCounts indexCounts = tableIndexCounts.indices.get(indexName);
                    if (indexCounts == null) {
                      indexCounts = new IndexCounts();
                      tableIndexCounts.indices.put(indexName, indexCounts);
                    }
                    indexCounts.counts.put(shard, size);
                  }
                }
              }
            }
            return null;
          }
        }));

      }
      for (Future future : futures) {
        future.get();
      }
      for (Map.Entry<String, TableIndexCounts> entry : ret.tables.entrySet()) {
        for (Map.Entry<String, IndexCounts> indexEntry : entry.getValue().indices.entrySet()) {
          for (int i = 0; i < client.getShardCount(); i++) {
            Long count = indexEntry.getValue().counts.get(i);
            if (count == null) {
              indexEntry.getValue().counts.put(i, 0L);
            }
          }
        }
      }
      return ret;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }



  public static List<Integer> findOrderedPartitionForRecord(
      boolean includeCurrPartitions, boolean includeLastPartitions,
      TableSchema tableSchema, String indexName,
      List<OrderByExpressionImpl> orderByExpressions,
      BinaryExpression.Operator leftOperator,
      BinaryExpression.Operator rightOperator,
      Object[] leftKey, Object[] rightKey) {
    boolean ascending = true;
    if (orderByExpressions != null && !orderByExpressions.isEmpty()) {
      OrderByExpressionImpl expression = orderByExpressions.get(0);
      String columnName = expression.getColumnName();
      if (expression.getTableName() == null || !expression.getTableName().equals(tableSchema.getName()) ||
          columnName.equals(tableSchema.getIndices().get(indexName).getFields()[0])) {
        ascending = expression.isAscending();
      }
    }

    IndexSchema specifiedIndexSchema = tableSchema.getIndices().get(indexName);
    Comparator[] comparators = specifiedIndexSchema.getComparators();

    List<Integer> ret = new ArrayList<>();

    List<Integer> selectedPartitions = new ArrayList<>();
    if (includeCurrPartitions) {
      TableSchema.Partition[] partitions = tableSchema.getIndices().get(indexName).getCurrPartitions();
      if (rightOperator == null) {
        doSelectPartitions(partitions, tableSchema, indexName, leftOperator, comparators, leftKey,
            ascending, ret);
      }
      else {
        doSelectPartitions(partitions, tableSchema, indexName, leftOperator, comparators, leftKey,
            rightKey, ascending, ret);
      }
    }

    if (includeLastPartitions) {
      List<Integer> selectedLastPartitions = new ArrayList<>();
      TableSchema.Partition[] lastPartitions = tableSchema.getIndices().get(indexName).getLastPartitions();
      if (lastPartitions != null) {
        if (rightOperator == null) {
          doSelectPartitions(lastPartitions, tableSchema, indexName, leftOperator, comparators, leftKey,
              ascending, selectedLastPartitions);
        }
        else {
          doSelectPartitions(lastPartitions, tableSchema, indexName, leftOperator, comparators, leftKey,
              rightKey, ascending, selectedLastPartitions);
        }
        for (int partitionOffset : selectedLastPartitions) {
          selectedPartitions.add(lastPartitions[partitionOffset].getShardOwning());
        }
        for (int partitionOffset : selectedLastPartitions) {
          int shard = lastPartitions[partitionOffset].getShardOwning();
          boolean found = false;
          for (int currShard : ret) {
            if (currShard == shard) {
              found = true;
              break;
            }
          }
          if (!found) {
            ret.add(shard);
          }
        }
      }
    }
    return ret;
  }

  private static void doSelectPartitions(
      TableSchema.Partition[] partitions, TableSchema tableSchema, String indexName,
      BinaryExpression.Operator leftOperator,
      Comparator[] comparators, Object[] leftKey,
      Object[] rightKey, boolean ascending, List<Integer> selectedPartitions) {

    BinaryExpression.Operator greaterOp = leftOperator;
    Object[] greaterKey = leftKey;
    Object[] lessKey = rightKey;
    if (greaterOp == com.sonicbase.query.BinaryExpression.Operator.LESS ||
        greaterOp == com.sonicbase.query.BinaryExpression.Operator.LESS_EQUAL) {
      greaterKey = rightKey;
      lessKey = leftKey;
    }

    for (int i = !ascending ? partitions.length - 1 : 0; (!ascending ? i >= 0 : i < partitions.length); i += (!ascending ? -1 : 1)) {
      if (partitions[i].isUnboundUpper()) {
        selectedPartitions.add(i);
        if (ascending) {
          break;
        }
      }
      Object[] lowerKey = partitions[i].getUpperKey();
      if (lowerKey == null) {
        continue;
      }
      String[] indexFields = tableSchema.getIndices().get(indexName).getFields();
      Object[] tempLowerKey = new Object[indexFields.length];
      for (int j = 0; j < indexFields.length; j++) {
        tempLowerKey[j] = lowerKey[j];
      }

      int greaterCompareValue = getCompareValue(comparators, greaterKey, tempLowerKey);

      if (greaterCompareValue == -1 || greaterCompareValue == 0) {
        if (i == 0) {
          selectedPartitions.add(i);
        }
        else {
          int lessCompareValue2 = getCompareValue(comparators, lessKey, partitions[i - 1].getUpperKey());
          if (lessCompareValue2 == 1) {
            selectedPartitions.add(i);
          }
        }
      }
    }
  }

  private static int getCompareValue(
      Comparator[] comparators, Object[] leftKey, Object[] tempLowerKey) {
    int compareValue = 0;
    for (int k = 0; k < leftKey.length; k++) {
      int value = comparators[k].compare(leftKey[k], tempLowerKey[k]);
      if (value < 0) {
        compareValue = -1;
        break;
      }
      if (value > 0) {
        compareValue = 1;
        break;
      }
    }
    return compareValue;
  }

  private static void doSelectPartitions(
      TableSchema.Partition[] partitions, TableSchema tableSchema, String indexName,
      BinaryExpression.Operator operator, Comparator[] comparators, Object[] key,
      boolean ascending, List<Integer> selectedPartitions) {

    if (key == null) {
      if (ascending) {
        for (int i = 0; i < partitions.length; i++) {
          selectedPartitions.add(i);
        }
      }
      else {
        for (int i = partitions.length - 1; i >= 0; i--) {
          selectedPartitions.add(i);
        }
      }
      return;
    }

    if (operator == com.sonicbase.query.BinaryExpression.Operator.EQUAL) {

      TableSchema.Partition partitionZero = partitions[0];
      if (partitionZero.getUpperKey() == null) {
        selectedPartitions.add(0);
        return;
      }

      for (int i = 0; i < partitions.length - 1; i++) {
        int compareValue = 0;

        for (int k = 0; k < key.length; k++) {
          if (key[k] == null || partitions[0].getUpperKey()[k] == null) {
            continue;
          }
          int value = comparators[k].compare(key[k], partitions[i].getUpperKey()[k]);
          if (value < 0) {
            compareValue = -1;
            break;
          }
          if (value > 0) {
            compareValue = 1;
            break;
          }
        }

        if (i == 0 && compareValue == -1 || compareValue == 0) {
          selectedPartitions.add(i);
        }

        int compareValue2 = 0;
        if (partitions[i + 1].getUpperKey() == null) {
          if (compareValue == 1 || compareValue == 0) {
            selectedPartitions.add(i + 1);
          }
        }
        else {
          for (int k = 0; k < key.length; k++) {
            if (key[k] == null || partitions[0].getUpperKey()[k] == null) {
              continue;
            }
            int value = comparators[k].compare(key[k], partitions[i + 1].getUpperKey()[k]);
            if (value < 0) {
              compareValue2 = -1;
              break;
            }
            if (value > 0) {
              compareValue2 = 1;
              break;
            }
          }
          if ((compareValue == 1 || compareValue == 0) && compareValue2 == -1) {
            selectedPartitions.add(i + 1);
          }
        }
      }
      return;
    }

    outer:
    for (int i = !ascending ? partitions.length - 1 : 0; (!ascending ? i >= 0 : i < partitions.length); i += (!ascending ? -1 : 1)) {
      Object[] lowerKey = partitions[i].getUpperKey();
      if (lowerKey == null) {


        if (i == 0 || (!ascending ? i == 0 : i == partitions.length - 1)) {
          selectedPartitions.add(i);
          break;
        }
        Object[] lowerLowerKey = partitions[i - 1].getUpperKey();
        if (lowerLowerKey == null) {
          continue;
        }
        String[] indexFields = tableSchema.getIndices().get(indexName).getFields();
        Object[] tempLowerKey = new Object[indexFields.length];
        for (int j = 0; j < indexFields.length; j++) {
          tempLowerKey[j] = lowerLowerKey[j];
        }
        int compareValue = 0;

        for (int k = 0; k < key.length; k++) {
          int value = comparators[k].compare(key[k], tempLowerKey[k]);
          if (value < 0) {
            compareValue = -1;
            break;
          }
          if (value > 0) {
            compareValue = 1;
            break;
          }
        }
        if (compareValue == 0 && operator == BinaryExpression.Operator.GREATER) {
          continue outer;
        }
        if (compareValue == 1) {
          selectedPartitions.add(i);
        }
        if (compareValue == -1 && (operator == com.sonicbase.query.BinaryExpression.Operator.GREATER || operator == com.sonicbase.query.BinaryExpression.Operator.GREATER_EQUAL)) {
          selectedPartitions.add(i);
        }
        if (ascending) {
          break;
        }
        continue;
      }

      String[] indexFields = tableSchema.getIndices().get(indexName).getFields();
      Object[] tempLowerKey = new Object[indexFields.length];
      for (int j = 0; j < indexFields.length; j++) {
        tempLowerKey[j] = lowerKey[j];
      }

      int compareValue = 0;

      for (int k = 0; k < comparators.length; k++) {
        int value = comparators[k].compare(key[k], tempLowerKey[k]);
        if (value < 0) {
          compareValue = -1;
          break;
        }
        if (value > 0) {
          compareValue = 1;
          break;
        }
      }
      if (compareValue == 0 && operator == BinaryExpression.Operator.GREATER) {
        continue outer;
      }
      if (compareValue == 1 &&
          (operator == com.sonicbase.query.BinaryExpression.Operator.LESS ||
              operator == com.sonicbase.query.BinaryExpression.Operator.LESS_EQUAL)) {
        selectedPartitions.add(i);
      }
      if (compareValue == -1 || compareValue == 0 || i == partitions.length - 1) {
        selectedPartitions.add(i);
        if (operator == com.sonicbase.query.BinaryExpression.Operator.EQUAL) {
          return;
        }
      }
    }
  }


}
