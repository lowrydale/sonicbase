package com.sonicbase.server;

import com.sonicbase.query.DatabaseException;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class AddressMap {

  private org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("com.sonicbase.logger");

  private LongList[] map = new LongArrayList[10_000];
  private AtomicLong currOuterAddress = new AtomicLong();
  final ConcurrentLinkedQueue<Long> freeList = new ConcurrentLinkedQueue<>();
  private ReentrantReadWriteLock[] readWriteLocks = new ReentrantReadWriteLock[10_000];
  private ReentrantReadWriteLock.ReadLock[] readLocks = new ReentrantReadWriteLock.ReadLock[10_000];
  private ReentrantReadWriteLock.WriteLock[] writeLocks = new ReentrantReadWriteLock.WriteLock[10_000];

  private Long2LongOpenHashMap[] addressMap = new Long2LongOpenHashMap[10_000];

  private static Unsafe getUnsafe() {
    try {

      Field singleoneInstanceField = Unsafe.class.getDeclaredField("theUnsafe");
      singleoneInstanceField.setAccessible(true);
      return (Unsafe) singleoneInstanceField.get(null);

    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }


  private Unsafe unsafe = getUnsafe();



  public AddressMap() {
    for (int i = 0; i < readWriteLocks.length; i++) {
      readWriteLocks[i] = new ReentrantReadWriteLock();
      readLocks[i] = readWriteLocks[i].readLock();
      writeLocks[i] = readWriteLocks[i].writeLock();
    }
    for (int i = 0; i < addressMap.length; i++) {
      addressMap[i] = new Long2LongOpenHashMap();
      addressMap[i].defaultReturnValue(-1L);
    }
    for (int i = 0; i < map.length; i++) {
      map[i] = new LongArrayList();
      //map[i].defaultReturnValue(-1);
    }
  }

  public void clear() {
    freeList.clear();
    for (int i = 0; i < addressMap.length; i++) {
      for (long innerAddress : addressMap[i].values()) {
        unsafe.freeMemory(innerAddress);
      }
    }
    addressMap = new Long2LongOpenHashMap[10_000];
    for (int i = 0; i < addressMap.length; i++) {
      addressMap[i] = new Long2LongOpenHashMap();
      addressMap[i].defaultReturnValue(-1L);
    }

    //currOuterAddress.set(0);
    map = new LongArrayList[10_000];
    for (int i = 0; i < map.length; i++) {
      map[i] = new LongArrayList();
      //map[i].defaultReturnValue(-1);
    }
  }

  public Object getMutex(long outerAddress) {
    int slot = (int) (outerAddress % map.length);
    return map[slot];
  }

  public ReentrantReadWriteLock.ReadLock getReadLock(long outerAddress) {
    int slot = (int) (outerAddress % map.length);
    return readLocks[slot];
  }

  public ReentrantReadWriteLock.WriteLock getWriteLock(long outerAddress) {
    int slot = (int) (outerAddress % map.length);
    return writeLocks[slot];
  }

  //  public Object getMutex(long outerAddress) {
//    int slot = (int) (outerAddress % map.length);
//    return map[slot];
//  }

  public long getUpdateTime(Long outerAddress) {
    if (outerAddress != null) {
    ReentrantReadWriteLock.ReadLock readLock = getReadLock(outerAddress);
    readLock.lock();
      try {
        return 999999999999999999L;
//      long innerAddress = getAddress(outerAddress);
//      long value = 0;
//      for (int i = 0; i < 8; i++) {
//        value <<= 8;
//        value |= (unsafe.getByte(innerAddress + i) & 0xFF);
//      }
//      return value;
      }
      finally {
        readLock.unlock();
      }

//      int offset = (int) Math.floor(outerAddress / (float) map.length);

//      long value = -1;
//      synchronized (getMutex(outerAddress)) {
//        long innerAddress = map[(int) (outerAddress % map.length)].get(offset);
//        for (int i = 0; i < 8; i++) {
//          value <<= 8;
//          value |= (unsafe.getByte(innerAddress + i) & 0xFF);
//        }
//      }

    }
    return 0;
  }

  public long addAddress(long innerAddress, long updateTime) {
    long outerAddress = currOuterAddress.incrementAndGet();
//    return innerAddress;

    ReentrantReadWriteLock.WriteLock lock = getWriteLock(outerAddress);
    lock.lock();
    try {
      addressMap[(int) (outerAddress % map.length)].put(outerAddress, innerAddress);
    }
    finally {
      lock.unlock();
    }


    return outerAddress;


//    try {
//      Long outerAddress = freeList.poll();
//      if (outerAddress != null) {
//        int offset = (int) Math.floor(outerAddress / (float) map.length);
//        synchronized (getMutex(outerAddress)) {
//          map[(int) (outerAddress % map.length)].set(offset, innerAddress);
//        }
//        return outerAddress;
//      }
//      outerAddress = currOuterAddress.incrementAndGet();
//      int offset = (int) Math.floor(outerAddress / (float) map.length);
//
//      //long[] value = new long[]{innerAddress, updateTime};
//
//      synchronized (getMutex(outerAddress)) {
//        int size = map[(int) (outerAddress % map.length)].size();
//        if (size <= offset) {
//          while (size < offset) {
//            map[(int) (outerAddress % map.length)].add(-1);
//            size++;
//          }
//          map[(int) (outerAddress % map.length)].add(innerAddress);
//        }
//        else {
//          map[(int) (outerAddress % map.length)].set(offset, innerAddress);
//        }
//      }
//      if (innerAddress == 0 || innerAddress == -1) {
//        throw new DatabaseException("Adding invalid address");
//      }
//      return outerAddress;
//    }
//    catch (Exception e) {
//      throw new DatabaseException(e);
//    }
  }

  public Long getAddress(long outerAddress) {
//    ReentrantReadWriteLock.ReadLock readLock = getReadLock(outerAddress);
//    try {
      long ret = addressMap[(int) (outerAddress % map.length)].get(outerAddress);
      if (ret == -1) {
        return null;
      }
      return ret;
//    }
//    finally {
//      readLock.unlock();
//    }
//    return outerAddress;

//    try {
//      int offset = (int) Math.floor(outerAddress / (float) map.length);
//      long value = -1;
//      synchronized (getMutex(outerAddress)) {
//        value = map[(int) (outerAddress % map.length)].get(offset);
//      }
//      if (value == -1) {
//        return null;
//      }
//      return value;
//    }
//    catch (Exception e) {
//      throw new DatabaseException(e);
//    }
  }

  public void removeAddress(long outerAddress, Unsafe unsafe) {
    ReentrantReadWriteLock.WriteLock writeLock = getWriteLock(outerAddress);
    writeLock.lock();
    try {
      long innerAddress = addressMap[(int) (outerAddress % map.length)].remove(outerAddress);
      if (innerAddress != -1) {
        unsafe.freeMemory(innerAddress);
      }
    }
    finally {
      writeLock.unlock();
    }

//      unsafe.freeMemory(outerAddress);


//    try {
//      int offset = (int) Math.floor(outerAddress / (float)map.length);
//      long value = -1;
//      synchronized (getMutex(outerAddress)) {
//        value = map[(int) (outerAddress % map.length)].set(offset, -1);
//      }
//      freeList.add(outerAddress);
//
//      long innerAddress = value;
//      if (innerAddress == 0) {
//        return;
//      }
//      unsafe.freeMemory(innerAddress);
//    }
//    catch (Exception e) {
//      throw new DatabaseException(e);
//    }
  }
}

