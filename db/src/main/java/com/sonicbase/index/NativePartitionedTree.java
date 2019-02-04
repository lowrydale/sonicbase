package com.sonicbase.index;

public class NativePartitionedTree {

  public void logError(String msg) {
  }

  public native long initIndex(int[] dataTypes);

  public native long put(long indexId, Object[] startKey, long value);

  public native long get(long indexId, Object[] key);

  public native long remove(long indexId, Object[] key);

  public native void clear(long indexId);

  public native int tailBlockArray(long indexId, Object[] startKey, int count, boolean first, Object[][] keys, long[] values);

  public native int headBlockArray(long indexId, Object[] startKey, int count, boolean first, Object[][] keys, long[] values);

  public native boolean higherEntry(long indexId, Object[] key, Object[][] retKey, long[] retValue);

  public native boolean lowerEntry(long indexId, Object[] key, Object[][] retKey, long[] retValue);

  public native boolean floorEntry(long indexId, Object[] key, Object[][] retKey, long[] retValue);

  public native boolean ceilingEntry(long indexId, Object[] key, Object[][] retKey, long[] retValue);

  public native boolean lastEntry2(long indexId, Object[][] retKey, long[] retValue);

  public native boolean firstEntry2(long indexId, Object[][] retKey, long[] retValue);

  public native void delete(long indexId);

  public native void sortKeys(long indexId, Object[][] keys, boolean ascend);
}
