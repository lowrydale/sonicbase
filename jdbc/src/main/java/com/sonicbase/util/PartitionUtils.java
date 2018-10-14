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

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class PartitionUtils {
  private PartitionUtils() {
  }

  public static class IndexCounts {
    private final Map<Integer, Long> counts = new ConcurrentHashMap<>();

    public Map<Integer, Long> getCounts() {
      return counts;
    }
  }

  public static class TableIndexCounts {
    private final Map<String, IndexCounts> indices = new ConcurrentHashMap<>();

    public Map<String, IndexCounts> getIndices() {
      return indices;
    }
  }

  public static class GlobalIndexCounts {
    private final Map<String, TableIndexCounts> tables = new ConcurrentHashMap<>();

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
            processResponseForGetIndexCounts(ret, shard, response);
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

  private static void processResponseForGetIndexCounts(GlobalIndexCounts ret, int shard, byte[] response) {
    ComObject retObj = new ComObject(response);
    ComArray tables = retObj.getArray(ComObject.Tag.TABLES);
    if (tables != null) {
      for (int i1 = 0; i1 < tables.getArray().size(); i1++) {
        ComObject tableObj = (ComObject) tables.getArray().get(i1);
        doProcessResponseForGetIndexCountsHandleTable(ret, shard, tableObj);
      }
    }
  }

  private static void doProcessResponseForGetIndexCountsHandleTable(GlobalIndexCounts ret, int shard, ComObject tableObj) {
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


  public static List<Integer> findOrderedPartitionForRecord(
      boolean includeCurrPartitions, boolean includeLastPartitions,
      TableSchema tableSchema, IndexSchema indexSchema,
      List<OrderByExpressionImpl> orderByExpressions,
      BinaryExpression.Operator leftOperator,
      BinaryExpression.Operator rightOperator,
      Object[] leftKey, Object[] rightKey) {
    boolean ascending = true;
    if (orderByExpressions != null && !orderByExpressions.isEmpty()) {
      OrderByExpressionImpl expression = orderByExpressions.get(0);
      String columnName = expression.getColumnName();
      if (expression.getTableName() == null || !expression.getTableName().equals(tableSchema.getName()) ||
          columnName.equals(indexSchema.getFields()[0])) {
        ascending = expression.isAscending();
      }
    }

    Comparator[] comparators = indexSchema.getComparators();

    List<Integer> ret = new ArrayList<>();

    List<Integer> selectedPartitions = new ArrayList<>();
    if (includeCurrPartitions) {
      TableSchema.Partition[] partitions = indexSchema.getCurrPartitions();
      if (rightOperator == null) {
        doSelectPartitions(partitions, tableSchema, indexSchema, leftOperator, comparators, leftKey,
            ascending, ret);
      }
      else {
        doSelectPartitions(partitions, tableSchema, indexSchema, leftOperator, comparators, leftKey,
            rightKey, ascending, ret);
      }
    }

    if (includeLastPartitions) {
      findOrderedPartitionsForRecordLastPartitions(tableSchema, indexSchema, leftOperator, rightOperator, leftKey,
          rightKey, ascending, comparators, ret, selectedPartitions);
    }
    return ret;
  }

  private static void findOrderedPartitionsForRecordLastPartitions(TableSchema tableSchema, IndexSchema indexSchema,
                                                                   BinaryExpression.Operator leftOperator,
                                                                   BinaryExpression.Operator rightOperator, Object[] leftKey,
                                                                   Object[] rightKey, boolean ascending, Comparator[] comparators,
                                                                   List<Integer> ret, List<Integer> selectedPartitions) {
    List<Integer> selectedLastPartitions = new ArrayList<>();
    TableSchema.Partition[] lastPartitions = indexSchema.getLastPartitions();
    if (lastPartitions != null) {
      if (rightOperator == null) {
        doSelectPartitions(lastPartitions, tableSchema, indexSchema, leftOperator, comparators, leftKey,
            ascending, selectedLastPartitions);
      }
      else {
        doSelectPartitions(lastPartitions, tableSchema, indexSchema, leftOperator, comparators, leftKey,
            rightKey, ascending, selectedLastPartitions);
      }
      for (int partitionOffset : selectedLastPartitions) {
        selectedPartitions.add(lastPartitions[partitionOffset].getShardOwning());
      }
      doAddShardToResults(ret, selectedLastPartitions, lastPartitions);
    }
  }

  private static void doAddShardToResults(List<Integer> ret, List<Integer> selectedLastPartitions,
                                          TableSchema.Partition[] lastPartitions) {
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

  private static void doSelectPartitions(
      TableSchema.Partition[] partitions, TableSchema tableSchema, IndexSchema indexSchema,
      BinaryExpression.Operator leftOperator,
      Comparator[] comparators, Object[] leftKey,
      Object[] rightKey, boolean ascending, List<Integer> selectedPartitions) {

    BinaryExpression.Operator greaterOp = leftOperator;
    GetKeys getKeys = new GetKeys(leftKey, rightKey, greaterOp).invoke();
    Object[] greaterKey = getKeys.getGreaterKey();
    Object[] lessKey = getKeys.getLessKey();

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
      doSelectPartitions(partitions, tableSchema, indexSchema, comparators, selectedPartitions, greaterKey, lessKey, i, lowerKey);
    }
  }

  private static void doSelectPartitions(TableSchema.Partition[] partitions, TableSchema tableSchema, IndexSchema indexSchema,
                                         Comparator[] comparators, List<Integer> selectedPartitions, Object[] greaterKey,
                                         Object[] lessKey, int i, Object[] lowerKey) {
    String[] indexFields = indexSchema.getFields();
    Object[] tempLowerKey = new Object[indexFields.length];
    System.arraycopy(lowerKey, 0, tempLowerKey, 0, indexFields.length);

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
      TableSchema.Partition[] partitions, TableSchema tableSchema, IndexSchema indexSchema,
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
      doSelectPartitionsForOperatorEquals(partitions, comparators, key, selectedPartitions);
      return;
    }

    doChoosePartition(partitions, tableSchema, indexSchema, operator, comparators, key, ascending, selectedPartitions);
  }

  private static void doChoosePartition(TableSchema.Partition[] partitions, TableSchema tableSchema, IndexSchema indexSchema,
                                        BinaryExpression.Operator operator, Comparator[] comparators, Object[] key,
                                        boolean ascending, List<Integer> selectedPartitions) {
    for (int i = !ascending ? partitions.length - 1 : 0; (!ascending ? i >= 0 : i < partitions.length); i += (!ascending ? -1 : 1)) {
      Object[] lowerKey = partitions[i].getUpperKey();

      HandleLowerKeyIsNull isNull = new HandleLowerKeyIsNull(ascending, tableSchema, indexSchema, selectedPartitions,
          partitions, i, lowerKey, key, operator, comparators).invoke();
      if (isNull.shouldContinue) {
        continue;
      }
      if (isNull.shouldBreak) {
        break;
      }
      String[] indexFields = indexSchema.getFields();
      CompareLowerKeyWithKey compare = new CompareLowerKeyWithKey(selectedPartitions, partitions, i, indexFields,
          lowerKey, key, operator, comparators).invoke();
      if (compare.shouldContinue) {
        continue;
      }
      if (compare.shouldReturn) {
        return;
      }
    }
  }
  private static class HandleLowerKeyIsNull {

    private final List<Integer> selectedPartitions;
    private final TableSchema.Partition[] partitions;
    private final int i;
    private final Object[] lowerKey;
    private final Object[] key;
    private final BinaryExpression.Operator operator;
    private final Comparator[] comparators;
    private final boolean ascending;
    private final TableSchema tableSchema;
    private final IndexSchema indexSchema;
    private boolean shouldContinue;
    private boolean shouldBreak;

    HandleLowerKeyIsNull(boolean ascending, TableSchema tableSchema, IndexSchema indexSchema,
                         List<Integer> selectedPartitions, TableSchema.Partition[] partitions, int i,
                         Object[] lowerKey, Object[] key, BinaryExpression.Operator operator, Comparator[] comparators) {
      this.ascending = ascending;
      this.tableSchema = tableSchema;
      this.indexSchema = indexSchema;
      this.selectedPartitions = selectedPartitions;
      this.partitions = partitions;
      this.i = i;
      this.lowerKey = lowerKey;
      this.key = key;
      this.operator = operator;
      this.comparators = comparators;
    }

    private HandleLowerKeyIsNull invoke() {
      if (lowerKey == null) {
        if (i == 0 || (!ascending ? i == 0 : i == partitions.length - 1)) {
          selectedPartitions.add(i);
          this.shouldBreak = true;
          return this;
        }
        Object[] lowerLowerKey = partitions[i - 1].getUpperKey();
        if (lowerLowerKey == null) {
          this.shouldContinue = true;
          return this;
        }

        if (compareUpperKeyWithKey(tableSchema, indexSchema, operator, comparators, key, selectedPartitions, i, lowerLowerKey)) {
          this.shouldContinue = true;
          return this;
        }
        if (ascending) {
          this.shouldBreak = true;
          return this;
        }
        this.shouldContinue = true;
        return this;
      }
      return this;
    }

    private static boolean compareUpperKeyWithKey(TableSchema tableSchema, IndexSchema indexSchema,
                                                  BinaryExpression.Operator operator, Comparator[] comparators,
                                                  Object[] key, List<Integer> selectedPartitions, int i, Object[] lowerLowerKey) {
      int compareValue = doCompareLowerKeyWithKey(tableSchema, indexSchema, comparators, key, lowerLowerKey);

      if (compareValue == 0 && operator == BinaryExpression.Operator.GREATER) {
        return true;
      }
      if (compareValue == 1) {
        selectedPartitions.add(i);
      }
      if (compareValue == -1 && (operator == BinaryExpression.Operator.GREATER ||
          operator == BinaryExpression.Operator.GREATER_EQUAL)) {
        selectedPartitions.add(i);
      }
      return false;
    }
  }

  private static class CompareLowerKeyWithKey {
    private final String[] indexFields;
    private final Object[] lowerKey;
    private final Comparator[] comparators;
    private final Object[] key;
    private final BinaryExpression.Operator operator;
    private final List<Integer> selectedPartitions;
    private final TableSchema.Partition[] partitions;
    private final int i;
    private boolean shouldReturn;
    private boolean shouldContinue;

    CompareLowerKeyWithKey(List<Integer> selectedPartitions, TableSchema.Partition[] partitions, int i,
                           String[] indexFields, Object[] lowerKey, Object[] key,
                           BinaryExpression.Operator operator, Comparator[] comparators) {
      this.indexFields = indexFields;
      this.lowerKey = lowerKey;
      this.comparators = comparators;
      this.key = key;
      this.operator = operator;
      this.selectedPartitions = selectedPartitions;
      this.partitions = partitions;
      this.i = i;
    }

    private CompareLowerKeyWithKey invoke() {
      Object[] tempLowerKey = new Object[indexFields.length];
      System.arraycopy(lowerKey, 0, tempLowerKey, 0, indexFields.length);

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
        shouldContinue = true;
        return this;
      }
      if (compareValue == 1 &&
          (operator == BinaryExpression.Operator.LESS ||
              operator == BinaryExpression.Operator.LESS_EQUAL)) {
        selectedPartitions.add(i);
      }
      if (compareValue == -1 || compareValue == 0 || i == partitions.length - 1) {
        selectedPartitions.add(i);
        if (operator == BinaryExpression.Operator.EQUAL) {
          shouldReturn = true;
          return this;
        }
      }
      return this;
    }
  }


  private static int doCompareLowerKeyWithKey(TableSchema tableSchema, IndexSchema indexSchema, Comparator[] comparators,
                                              Object[] key, Object[] lowerLowerKey) {
    String[] indexFields = indexSchema.getFields();
    Object[] tempLowerKey = new Object[indexFields.length];
    System.arraycopy(lowerLowerKey, 0, tempLowerKey, 0, indexFields.length);
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
    return compareValue;
  }

  private static void doSelectPartitionsForOperatorEquals(TableSchema.Partition[] partitions, Comparator[] comparators,
                                                          Object[] key, List<Integer> selectedPartitions) {
    TableSchema.Partition partitionZero = partitions[0];
    if (partitionZero.getUpperKey() == null) {
      selectedPartitions.add(0);
      return;
    }

    for (int i = 0; i < partitions.length - 1; i++) {
      int compareValue = 0;

      compareValue = doSelectFirstPartitionForEquals(partitions, comparators, key, selectedPartitions, i, compareValue);

      if (!selectedPartitions.isEmpty()) {
        break;
      }
      int compareValue2 = 0;
      if (partitions[i + 1].getUpperKey() == null) {
        if (compareValue == 1 || compareValue == 0) {
          selectedPartitions.add(i + 1);
        }
      }
      else {
        doSelectSecondPartitionForEquals(partitions, comparators, key, selectedPartitions, i, compareValue, compareValue2);
      }
      if (!selectedPartitions.isEmpty()) {
        break;
      }
    }
  }

  private static int doSelectFirstPartitionForEquals(TableSchema.Partition[] partitions, Comparator[] comparators,
                                                     Object[] key, List<Integer> selectedPartitions, int i, int compareValue) {
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
    return compareValue;
  }

  private static void doSelectSecondPartitionForEquals(TableSchema.Partition[] partitions, Comparator[] comparators,
                                                       Object[] key, List<Integer> selectedPartitions, int i, int compareValue,
                                                       int compareValue2) {
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


  private static class GetKeys {
    private final Object[] leftKey;
    private final Object[] rightKey;
    private final BinaryExpression.Operator greaterOp;
    private Object[] greaterKey;
    private Object[] lessKey;

    GetKeys(Object[] leftKey, Object[] rightKey, BinaryExpression.Operator greaterOp) {
      this.leftKey = leftKey;
      this.rightKey = rightKey;
      this.greaterOp = greaterOp;
    }

    Object[] getGreaterKey() {
      return greaterKey;
    }

    Object[] getLessKey() {
      return lessKey;
    }

    public GetKeys invoke() {
      greaterKey = leftKey;
      lessKey = rightKey;
      if (greaterOp == BinaryExpression.Operator.LESS ||
          greaterOp == BinaryExpression.Operator.LESS_EQUAL) {
        greaterKey = rightKey;
        lessKey = leftKey;
      }
      return this;
    }
  }
}
