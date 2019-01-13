/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.index;

import it.unimi.dsi.fastutil.objects.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class PartitionedTree {

  public static final int NUM_RECORDS_PER_PARTITION = 8;
  private AvlTree[] avlMaps;
  public static int partitionCount = 256;
  public static int BLOCK_SIZE = NUM_RECORDS_PER_PARTITION * partitionCount;

  public PartitionedTree() {
    avlMaps = new AvlTree[partitionCount];
    for (int i = 0; i < avlMaps.length; i++) {
      avlMaps[i] = new AvlTree<Object[]>(new Comparator<Object[]>() {
        @Override
        public int compare(Object[] o1, Object[] o2) {
          return Long.compare((long)o1[0], (long)o2[0]);
        }
      });
    }
  }

  public void put(Object[] key, Long value) {
    int partition = (int) ((long)key[0] % partitionCount);
    synchronized (avlMaps[partition]) {
      avlMaps[partition].put(key, value);
    }
  }

  public Long get(Object[] key) {
    int partition = (int) ((long)key[0] % partitionCount);
    synchronized (avlMaps[partition]) {
      return avlMaps[partition].get(key);
    }
  }

  public void tailBlock(Object[] startKey, AtomicReference lastKey, Object[][] keys, long[] values, int count, boolean first) {
    PartitionResults[] currKeys = new PartitionResults[partitionCount];
    AtomicInteger posInTopLevelArray = new AtomicInteger();
    for (int partition = 0; partition < partitionCount; partition++) {
      currKeys[partition] = new PartitionResults();
      getNextEntryFromPartition(currKeys, currKeys[partition], partition, startKey, first);
    }
    Arrays.sort(currKeys, Comparator.comparingLong(o -> (long) o.getKey()[0]));

    for (int i = 0; i < count; i++) {
      if (!next(currKeys, keys, values, i, posInTopLevelArray)) {
        lastKey.set(keys[i - 1]);
      }
    }
    if ((Object[])lastKey.get() == null) {
      lastKey.set(keys[keys.length - 1]);
    }
  }

  boolean next(PartitionResults[] currKeys, Object[][] keys, long[] values, int offset, AtomicInteger posInTopLevelArray) {
    Object[] currKey = currKeys[posInTopLevelArray.get()].entry.getKey();
    Long currValue = currKeys[posInTopLevelArray.get()].entry.getLongValue();
    getNextEntryFromPartition(currKeys, currKeys[posInTopLevelArray.get()], currKeys[posInTopLevelArray.get()].getPartition(), currKeys[posInTopLevelArray.get()].getKey(), false);
    if (posInTopLevelArray.get() == partitionCount - 1) {
      posInTopLevelArray.set(0);
    }
    else if (keyCompare(currKeys[posInTopLevelArray.get()].getKey(), currKeys[partitionCount - 1].getKey()) > 0) {
      posInTopLevelArray.incrementAndGet();
    }
    else {
      Arrays.sort(currKeys, Comparator.comparingLong(o -> (long) o.getKey()[0]));
      posInTopLevelArray.set(0);
    }

    keys[offset] = currKey;
    values[offset] = currValue;
    return currKey != null;
  }


  int keyCompare(Object[] o1, Object[] o2) {
    return Long.compare((long)o1[0], (long)o2[0]);
  }

  public class PartitionResults {
    public AvlTree.Entry[] entries;
    public int posWithinPartition;
    private AvlTree.Entry<Object[]> entry;
    private int partition;

    public Object[] getKey() {
      Object[] ret = entry == null ? null : (Object[])entry.getKey();
      return ret;
    }

    public Long getValue() {
      return entry.getValue();
    }

    public int getPartition() {
      return partition;
    }
  }

  private void getNextEntryFromPartition(PartitionResults[] currKeys, PartitionResults entry, int partition, Object[] currKey, boolean first) {
    synchronized (avlMaps[partition]) {
      if (entry.entries == null || entry.posWithinPartition == NUM_RECORDS_PER_PARTITION) {
        entry.posWithinPartition = 0;
        if (entry.entries == null) {
          entry.entries = new AvlTree.Entry[NUM_RECORDS_PER_PARTITION];
        }
        avlMaps[partition].nextEntries(entry.entries, currKey);
        if (!first) {
          if (keyCompare((Object[])entry.entries[0].getKey(), currKey) == 0) {
            entry.posWithinPartition = 1;
          }
        }
      }
      entry.entry = entry.entries[entry.posWithinPartition++];
      entry.partition = partition;
    }
  }
}
