/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.util;

import com.sonicbase.query.BinaryExpression;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;

public class PartitionUtilsTest {

  @Test
  public void test() {
    TableSchema tableSchema = ClientTestUtils.createTable();
    IndexSchema indexSchema = ClientTestUtils.createIndexSchema(tableSchema, 2);
    TableSchema.Partition[] currPartitions = new TableSchema.Partition[2];
    currPartitions[0] = new TableSchema.Partition();
    currPartitions[0].setShardOwning(0);
    currPartitions[0].setUpperKey(new Object[]{5});
    currPartitions[1] = new TableSchema.Partition();
    currPartitions[1].setShardOwning(1);
    currPartitions[1].setUnboundUpper(true);
    TableSchema.Partition[] lastPartitions = new TableSchema.Partition[2];
    lastPartitions[0] = new TableSchema.Partition();
    lastPartitions[0].setShardOwning(0);
    lastPartitions[0].setUpperKey(new Object[]{2});
    lastPartitions[1] = new TableSchema.Partition();
    lastPartitions[1].setShardOwning(1);
    lastPartitions[1].setUnboundUpper(true);
    indexSchema.setCurrPartitions(currPartitions);
    indexSchema.setLastPartitions(lastPartitions);
    List<Integer> selectedShards = PartitionUtils.findOrderedPartitionForRecord(true, false, tableSchema,
        indexSchema, null, BinaryExpression.Operator.EQUAL, null, new Object[]{5},
        null);

    assertEquals(selectedShards.size(), 1);
    assertEquals((int)selectedShards.get(0), 0);
    selectedShards = PartitionUtils.findOrderedPartitionForRecord(false, true, tableSchema,
        indexSchema, null, BinaryExpression.Operator.EQUAL, null, new Object[]{5}, null);
    assertEquals(selectedShards.size(), 1);
    assertEquals((int)selectedShards.get(0), 1);

  }

  @Test
  public void testGreater() {
    TableSchema tableSchema = ClientTestUtils.createTable();
    IndexSchema indexSchema = ClientTestUtils.createIndexSchema(tableSchema, 2);
    TableSchema.Partition[] currPartitions = new TableSchema.Partition[2];
    currPartitions[0] = new TableSchema.Partition();
    currPartitions[0].setShardOwning(0);
    currPartitions[0].setUpperKey(new Object[]{5});
    currPartitions[1] = new TableSchema.Partition();
    currPartitions[1].setShardOwning(1);
    currPartitions[1].setUnboundUpper(true);
    TableSchema.Partition[] lastPartitions = new TableSchema.Partition[2];
    lastPartitions[0] = new TableSchema.Partition();
    lastPartitions[0].setShardOwning(0);
    lastPartitions[0].setUpperKey(new Object[]{2});
    lastPartitions[1] = new TableSchema.Partition();
    lastPartitions[1].setShardOwning(1);
    lastPartitions[1].setUnboundUpper(true);
    indexSchema.setCurrPartitions(currPartitions);
    indexSchema.setLastPartitions(lastPartitions);
    List<Integer> selectedShards = PartitionUtils.findOrderedPartitionForRecord(true, false, tableSchema,
        indexSchema, null, BinaryExpression.Operator.GREATER, null, new Object[]{4},
        null);

    assertEquals(selectedShards.size(), 2);
    assertEquals((int)selectedShards.get(0), 0);
    selectedShards = PartitionUtils.findOrderedPartitionForRecord(false, true, tableSchema,
        indexSchema, null, BinaryExpression.Operator.GREATER, null, new Object[]{4}, null);
    assertEquals(selectedShards.size(), 1);
    assertEquals((int)selectedShards.get(0), 1);

  }

  @Test
  public void testTwoKey() {
    TableSchema tableSchema = ClientTestUtils.createTable();
    IndexSchema indexSchema = ClientTestUtils.createIndexSchema(tableSchema, 2);
    TableSchema.Partition[] currPartitions = new TableSchema.Partition[2];
    currPartitions[0] = new TableSchema.Partition();
    currPartitions[0].setShardOwning(0);
    currPartitions[0].setUpperKey(new Object[]{5});
    currPartitions[1] = new TableSchema.Partition();
    currPartitions[1].setShardOwning(1);
    currPartitions[1].setUnboundUpper(true);
    TableSchema.Partition[] lastPartitions = new TableSchema.Partition[2];
    lastPartitions[0] = new TableSchema.Partition();
    lastPartitions[0].setShardOwning(0);
    lastPartitions[0].setUpperKey(new Object[]{2});
    lastPartitions[1] = new TableSchema.Partition();
    lastPartitions[1].setShardOwning(1);
    lastPartitions[1].setUnboundUpper(true);
    indexSchema.setCurrPartitions(currPartitions);
    indexSchema.setLastPartitions(lastPartitions);
    List<Integer> selectedShards = PartitionUtils.findOrderedPartitionForRecord(true, false, tableSchema,
        indexSchema, null, BinaryExpression.Operator.GREATER_EQUAL, BinaryExpression.Operator.LESS, new Object[]{5},
        new Object[]{7});

    assertEquals(selectedShards.size(), 2);
    assertEquals((int)selectedShards.get(0), 0);
    selectedShards = PartitionUtils.findOrderedPartitionForRecord(false, true, tableSchema,
        indexSchema, null, BinaryExpression.Operator.GREATER_EQUAL, BinaryExpression.Operator.LESS, new Object[]{5}, new Object[]{8});
    assertEquals(selectedShards.size(), 1);
    assertEquals((int)selectedShards.get(0), 1);

  }
}
