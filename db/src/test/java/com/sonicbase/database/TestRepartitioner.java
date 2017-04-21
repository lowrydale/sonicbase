package com.sonicbase.database;

import com.sonicbase.index.Repartitioner;
import com.sonicbase.schema.TableSchema;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

/**
 * Created by lowryda on 3/18/17.
 */
public class TestRepartitioner {

  @Test
  public void test() throws IOException {
    List<TableSchema.Partition> newPartitions = new ArrayList<>();

    final long[][] lists = new long[][]{new long[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, new long[]{}, new long[]{}, new long[]{}};
    long[] currPartitionSizes = new long[]{lists[0].length, lists[1].length, lists[2].length, lists[3].length};
    long newPartitionSize = lists[0].length / 4;
    Repartitioner.calculatePartitions("db", 4, newPartitions,
        "index", "table", currPartitionSizes,
        newPartitionSize, new Repartitioner.GetKeyAtOffset() {
          @Override
          public List<Object[]> getKeyAtOffset(String dbName, int shard, String tableName, String indexName,
                                         List<Repartitioner.OffsetEntry> offsets) {
            List<Object[]> ret = new ArrayList<>();
            for (Repartitioner.OffsetEntry entry : offsets) {
              ret.add(new Object[]{lists[shard][(int)entry.getOffset()]});
            }
            return ret;
          }
        });

    System.out.println(newPartitions.get(0).getUpperKey()[0]);
     System.out.println(newPartitions.get(1).getUpperKey()[0]);
    System.out.println(newPartitions.get(2).getUpperKey()[0]);
    //System.out.println(newPartitions.get(3).getUpperKey()[0]);
  }

  @Test
  public void test2() throws IOException {
    List<TableSchema.Partition> newPartitions = new ArrayList<>();

    final long[][] lists = new long[][]{new long[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
        new long[]{}, new long[]{}, new long[]{11, 12, 13, 14, 15, 16, 17, 18, 19, 20}};
    long[] currPartitionSizes = new long[]{lists[0].length, lists[1].length, lists[2].length, lists[3].length};
    long newPartitionSize = (lists[0].length + lists[3].length) / 4;
    Repartitioner.calculatePartitions("db", 4, newPartitions,
        "index", "table", currPartitionSizes,
        newPartitionSize, new Repartitioner.GetKeyAtOffset() {
          @Override
          public List<Object[]> getKeyAtOffset(String dbName, int shard, String tableName, String indexName, List<Repartitioner.OffsetEntry> offsets) {
            List<Object[]> ret = new ArrayList<>();
            for (Repartitioner.OffsetEntry entry : offsets) {
              ret.add(new Object[]{lists[shard][(int)entry.getOffset()]});
            }
            return ret;
          }
        });

    System.out.println(newPartitions.get(0).getUpperKey()[0]);
    System.out.println(newPartitions.get(1).getUpperKey()[0]);
    System.out.println(newPartitions.get(2).getUpperKey()[0]);
    //System.out.println(newPartitions.get(3).getUpperKey()[0]);
  }

  @Test
  public void test3() throws IOException {
    List<TableSchema.Partition> newPartitions = new ArrayList<>();

    final long[][] lists = new long[][]{new long[]{1, 2, 3, 4},
        new long[]{5, 6 }, new long[]{7, 8}, new long[]{9, 10, 11, 12}};
    long[] currPartitionSizes = new long[]{lists[0].length, lists[1].length, lists[2].length, lists[3].length};
    long newPartitionSize = (lists[0].length + lists[1].length + lists[2].length + lists[3].length) / 4;
    Repartitioner.calculatePartitions("db", 4, newPartitions,
        "index", "table", currPartitionSizes,
        newPartitionSize, new Repartitioner.GetKeyAtOffset() {
          @Override
          public List<Object[]> getKeyAtOffset(String dbName, int shard, String tableName, String indexName, List<Repartitioner.OffsetEntry> offsets) {
            List<Object[]> ret = new ArrayList<>();
            for (Repartitioner.OffsetEntry entry : offsets) {
              ret.add(new Object[]{lists[shard][(int)entry.getOffset()]});
            }
            return ret;
          }
        });

    System.out.println(newPartitions.get(0).getUpperKey()[0]);
    System.out.println(newPartitions.get(1).getUpperKey()[0]);
    System.out.println(newPartitions.get(2).getUpperKey()[0]);
    //System.out.println(newPartitions.get(3).getUpperKey()[0]);
  }

  @Test
  public void test4() throws IOException {
    List<TableSchema.Partition> newPartitions = new ArrayList<>();

    final long[][] lists = new long[][]{new long[]{},
        new long[]{}, new long[]{}, new long[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}};
    long[] currPartitionSizes = new long[]{lists[0].length, lists[1].length, lists[2].length, lists[3].length};
    long newPartitionSize = (lists[0].length + lists[1].length + lists[2].length + lists[3].length) / 4;
    Repartitioner.calculatePartitions("db", 4, newPartitions,
        "index", "table", currPartitionSizes,
        newPartitionSize, new Repartitioner.GetKeyAtOffset() {
          @Override
          public List<Object[]> getKeyAtOffset(String dbName, int shard, String tableName, String indexName, List<Repartitioner.OffsetEntry> offsets) {
            List<Object[]> ret = new ArrayList<>();
            for (Repartitioner.OffsetEntry entry : offsets) {
              ret.add(new Object[]{lists[shard][(int)entry.getOffset()]});
            }
            return ret;
          }
        });

    System.out.println(newPartitions.get(0).getUpperKey()[0]);
    System.out.println(newPartitions.get(1).getUpperKey()[0]);
    System.out.println(newPartitions.get(2).getUpperKey()[0]);
    //System.out.println(newPartitions.get(3).getUpperKey()[0]);
  }

  @Test
  public void testLarge() throws IOException {
    List<TableSchema.Partition> newPartitions = new ArrayList<>();

    final long[][] lists = new long[][]{new long[]{},
        new long[]{}, new long[]{}, new long[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}};
    lists[0] = new long[500000];
    for (int i = 0; i < lists[0].length; i++) {
      lists[0][i] = i;
    }
    lists[3] = new long[50];
    for (int i = 0; i < lists[3].length; i++) {
      lists[3][i] = i + 50;
    }
    long[] currPartitionSizes = new long[]{lists[0].length, lists[1].length, lists[2].length, lists[3].length};
    long newPartitionSize = (lists[0].length + lists[1].length + lists[2].length + lists[3].length) / 4;
    Repartitioner.calculatePartitions("db", 4, newPartitions,
        "index", "table", currPartitionSizes,
        newPartitionSize, new Repartitioner.GetKeyAtOffset() {
          @Override
          public List<Object[]> getKeyAtOffset(String dbName, int shard, String tableName, String indexName, List<Repartitioner.OffsetEntry> offsets) {
            List<Object[]> ret = new ArrayList<>();
            for (Repartitioner.OffsetEntry entry : offsets) {
              ret.add(new Object[]{lists[shard][(int)entry.getOffset()]});
            }
            return ret;
          }
        });

    System.out.println(newPartitions.get(0).getUpperKey()[0]);
    System.out.println(newPartitions.get(1).getUpperKey()[0]);
    System.out.println(newPartitions.get(2).getUpperKey()[0]);
    //System.out.println(newPartitions.get(3).getUpperKey()[0]);
  }

  @Test
  public void testLarge2() throws IOException {
    List<TableSchema.Partition> newPartitions = new ArrayList<>();

    final long[][] lists = new long[][]{new long[]{},
        new long[]{}};
    lists[0] = new long[5961917];
    for (int i = 0; i < lists[0].length; i++) {
      lists[0][i] = i;
    }
    lists[1] = new long[6274258];
    for (int i = 0; i < lists[1].length; i++) {
      lists[1][i] = i + lists[0].length;
    }
    long[] currPartitionSizes = new long[]{lists[0].length, lists[1].length};
    long newPartitionSize = (lists[0].length + lists[1].length) / 2;
    Repartitioner.calculatePartitions("db", 2, newPartitions,
        "index", "table", currPartitionSizes,
        newPartitionSize, new Repartitioner.GetKeyAtOffset() {
          @Override
          public List<Object[]> getKeyAtOffset(String dbName, int shard, String tableName, String indexName, List<Repartitioner.OffsetEntry> offsets) {
            List<Object[]> ret = new ArrayList<>();
            for (Repartitioner.OffsetEntry entry : offsets) {
              ret.add(new Object[]{lists[shard][(int)entry.getOffset()]});
            }
            return ret;
          }
        });

    System.out.println(newPartitions.get(0).getUpperKey()[0]);

  }

  @Test
  public void testLarge3() throws IOException {
    List<TableSchema.Partition> newPartitions = new ArrayList<>();

    final long[][] lists = new long[][]{new long[]{},
        new long[]{}};
    lists[0] = new long[1136699];
    for (int i = 0; i < lists[0].length; i++) {
      lists[0][i] = i;
    }
    lists[1] = new long[1749256];
    for (int i = 0; i < lists[1].length; i++) {
      lists[1][i] = i + lists[0].length;
    }
    long[] currPartitionSizes = new long[]{lists[0].length, lists[1].length};
    long newPartitionSize = (lists[0].length + lists[1].length) / 2;
    Repartitioner.calculatePartitions("db", 2, newPartitions,
        "index", "table", currPartitionSizes,
        newPartitionSize, new Repartitioner.GetKeyAtOffset() {
          @Override
          public List<Object[]> getKeyAtOffset(String dbName, int shard, String tableName, String indexName, List<Repartitioner.OffsetEntry> offsets) {
            List<Object[]> ret = new ArrayList<>();
            for (Repartitioner.OffsetEntry entry : offsets) {
              ret.add(new Object[]{lists[shard][(int)entry.getOffset()]});
            }
            return ret;
          }
        });

    System.out.println(newPartitions.get(0).getUpperKey()[0]);

  }

  @Test
  public void testHeadMap() {
    ConcurrentSkipListMap<Integer, Integer> map = new ConcurrentSkipListMap<>();
    map.put(1, 1); map.put(2, 2); map.put(3, 3); map.put(4, 4); map.put(5, 5); map.put(6, 6);

    ConcurrentNavigableMap<Integer, Integer> headMap = map.headMap(4);
    Set<Map.Entry<Integer, Integer>> set =headMap.descendingMap().entrySet();
    Iterator<Map.Entry<Integer, Integer>> iter = set.iterator();
    assertEquals((int)iter.next().getKey(), 3);
    assertEquals((int)iter.next().getKey(), 2);
    assertEquals((int)iter.next().getKey(), 1);
    assertFalse(iter.hasNext());
  }

  @Test
  public void testTailMap() {
    ConcurrentSkipListMap<Integer, Integer> map = new ConcurrentSkipListMap<>();
    map.put(1, 1); map.put(2, 2); map.put(3, 3); map.put(4, 4); map.put(5, 5); map.put(6, 6);

    ConcurrentNavigableMap<Integer, Integer> headMap = map.tailMap(3);
    Set<Map.Entry<Integer, Integer>> set =headMap.entrySet();
    Iterator<Map.Entry<Integer, Integer>> iter = set.iterator();
    assertEquals((int)iter.next().getKey(), 3);
    assertEquals((int)iter.next().getKey(), 4);
    assertEquals((int)iter.next().getKey(), 5);
    assertEquals((int)iter.next().getKey(), 6);
    assertFalse(iter.hasNext());
  }
}
