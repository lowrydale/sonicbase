package com.sonicbase.index;

public class NativePartitionedTree {

  public native long initIndex();

  public native long put(long indexId, byte[] key, long value);

  public native byte[] putBytes(long indexId, byte[] key, byte[] value);

  public native long get(long indexId, byte[] startKey);

  public native long remove(long indexId, byte[] key);

  public native void clear(long indexId);

  public native byte[] tailBlock(long indexId, byte[] startKey, int count, boolean first);

  public native byte[] tailBlockBytes(long indexId, byte[] startKey, int count, boolean first);

  public native byte[] headBlock(long indexId, byte[] startKey, int count, boolean first);

  public native byte[] higherEntry(long indexId, byte[] key);

  public native byte[] lowerEntry(long indexId, byte[] key);

  public native byte[] floorEntry(long indexId, byte[] key);

  public native byte[] ceilingEntry(long indexId, byte[] key);

  public native byte[] lastEntry2(long indexId);

  public native byte[] firstEntry2(long indexId);
}
