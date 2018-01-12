/* © 2017 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.server;

import com.sonicbase.query.DatabaseException;
import org.apache.giraph.utils.Varint;
import sun.misc.Unsafe;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

public class AddressMap {

  private org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("com.sonicbase.logger");

  private List<long[]>[] map = new List[10_000];
  private AtomicLong currOuterAddress = new AtomicLong();
  final ConcurrentLinkedQueue<Long> freeList = new ConcurrentLinkedQueue<>();

  public AddressMap() {
    for (int i = 0; i < map.length; i++) {
      map[i] = new ArrayList<>();
      //map[i].defaultReturnValue(-1);
    }
  }

  public void clear() {
    freeList.clear();
    currOuterAddress.set(0);
    map = new List[10_000];
    for (int i = 0; i < map.length; i++) {
      map[i] = new ArrayList<>();
      //map[i].defaultReturnValue(-1);
    }
  }

  public Object getMutex(long outerAddress) {
    return map[(int) (outerAddress % map.length)];
  }

  public long getUpdateTime(Long outerAddress) {
    if (outerAddress != null) {
      int offset = (int) Math.floor(outerAddress / (float) map.length);

      long[] value = null;
      synchronized (getMutex(outerAddress)) {
        value = map[(int) (outerAddress % map.length)].get(offset);
      }
      if (value.length < 2) {
        logger.error("Invalid address: address=" + outerAddress);
        return 0;
      }
      return value[1];
    }
    return 0;
  }

  public long addAddress(long innerAddress, long updateTime) {

    try {
      Long outerAddress = freeList.poll();
      if (outerAddress != null) {
        int offset = (int) Math.floor(outerAddress / (float) map.length);
        long[] prev = null;

        long[] value = new long[]{innerAddress, updateTime};
        synchronized (getMutex(outerAddress)) {
          prev = map[(int) (outerAddress % map.length)].set(offset, value);
        }
        if (prev != null && prev.length != 0) {
          throw new DatabaseException("Invalid pointer");
        }
        return outerAddress;
      }
      outerAddress = currOuterAddress.incrementAndGet();
      int offset = (int) Math.floor(outerAddress / (float) map.length);

      long[] value = new long[]{innerAddress, updateTime};

      synchronized (getMutex(outerAddress)) {
        int size = map[(int) (outerAddress % map.length)].size();
        if (size <= offset) {
          while (size < offset) {
            map[(int) (outerAddress % map.length)].add(new long[0]);
            size++;
          }
          map[(int) (outerAddress % map.length)].add(value);
        }
        else {
          map[(int) (outerAddress % map.length)].set(offset, value);
        }
      }
      if (innerAddress == 0 || innerAddress == -1) {
        throw new DatabaseException("Adding invalid address");
      }
      return outerAddress;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public Long getAddress(long outerAddress) {
    try {
      int offset = (int) Math.floor(outerAddress / (float) map.length);
      long[] value = null;
      synchronized (getMutex(outerAddress)) {
        value = map[(int) (outerAddress % map.length)].get(offset);
      }
      if (value == null || value.length == 0) {
        return null;
      }
      return value[0];
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public void removeAddress(long outerAddress, Unsafe unsafe) {
    try {
      int offset = (int) Math.floor(outerAddress / (float)map.length);
      long[] value = null;
      synchronized (getMutex(outerAddress)) {
        value = map[(int) (outerAddress % map.length)].set(offset, new long[0]);
      }
      freeList.add(outerAddress);

      long innerAddress = value[0];
      if (innerAddress == 0) {
        return;
      }
      unsafe.freeMemory(innerAddress);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }
}

