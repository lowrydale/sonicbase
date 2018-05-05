package com.sonicbase.index;

import com.sonicbase.common.DataUtils;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.server.DatabaseServer;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.apache.giraph.utils.Varint;
import sun.misc.Unsafe;

import java.io.*;
import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.sonicbase.server.DatabaseServer.TIME_2017;

public class AddressMap {

  private org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("com.sonicbase.logger");

  private LongList[] map = new LongArrayList[10_000];
  private AtomicLong currOuterAddress = new AtomicLong();
  final ConcurrentLinkedQueue<Long> freeList = new ConcurrentLinkedQueue<>();
  private ReentrantReadWriteLock[] readWriteLocks = new ReentrantReadWriteLock[10_000];
  private ReentrantReadWriteLock.ReadLock[] readLocks = new ReentrantReadWriteLock.ReadLock[10_000];
  private ReentrantReadWriteLock.WriteLock[] writeLocks = new ReentrantReadWriteLock.WriteLock[10_000];
  private LZ4Factory factory = LZ4Factory.fastestInstance();


  private Long2LongOpenHashMap[] addressMap = new Long2LongOpenHashMap[10_000];
  private final DatabaseServer server;

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



  public AddressMap(DatabaseServer server) {
    this.server = server;

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

  public class IndexValue {
    long updateTime;
    byte[][] records;
    byte[] bytes;

    public IndexValue(long updateTime, byte[][] records) {
      this.updateTime = updateTime;
      this.records = records;
    }

    public IndexValue(long updateTime, byte[] bytes) {
      this.updateTime = updateTime;
      this.bytes = bytes;
    }

    public long getUpdateTime() {
      return updateTime;
    }
  }



  public Object toUnsafeFromRecords(byte[][] records) {
    long seconds = (System.currentTimeMillis() - TIME_2017) / 1000;
    return toUnsafeFromRecords(seconds, records);
  }

  public Object toUnsafeFromRecords(long updateTime, byte[][] records) {
    if (!server.useUnsafe()) {
      try {
        if (!server.compressRecords()) {
          return new IndexValue(updateTime, records);
        }
        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytesOut);
        AtomicInteger AtomicInteger = new AtomicInteger();
        //out.writeInt(0);
        Varint.writeUnsignedVarInt(records.length, out);
        for (byte[] record : records) {
          Varint.writeUnsignedVarInt(record.length, out);
          out.write(record);
        }
        out.close();
        byte[] bytes = bytesOut.toByteArray();
        int origLen = -1;

        if (server.compressRecords()) {
          origLen = bytes.length;


          LZ4Compressor compressor = factory.highCompressor();//fastCompressor();
          int maxCompressedLength = compressor.maxCompressedLength(bytes.length);
          byte[] compressed = new byte[maxCompressedLength];
          int compressedLength = compressor.compress(bytes, 0, bytes.length, compressed, 0, maxCompressedLength);
          bytes = new byte[compressedLength + 8];
          System.arraycopy(compressed, 0, bytes, 8, compressedLength);
        }

        bytesOut = new ByteArrayOutputStream();
        out = new DataOutputStream(bytesOut);
        out.writeInt(bytes.length);
        out.writeInt(origLen);
        out.close();
        byte[] lenBuffer = bytesOut.toByteArray();

        System.arraycopy(lenBuffer, 0, bytes, 0, lenBuffer.length);

        return new IndexValue(updateTime, bytes);
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }
    else {
      int recordsLen = 0;
      for (byte[] record : records) {
        recordsLen += record.length;
      }

      byte[] bytes = new byte[4 + (4 * records.length) + recordsLen];
      //out.writeInt(0);
      int actualLen = 0;
      int offset = 0; //update time
      DataUtils.intToBytes(records.length, bytes, offset);
      offset += 4;
      for (byte[] record : records) {
        DataUtils.intToBytes(record.length, bytes, offset);
        offset += 4;
        System.arraycopy(record, 0, bytes, offset, record.length);
        offset += record.length;
      }
      actualLen = bytes.length;

      int origLen = -1;
      if (server.compressRecords()) {
        origLen = bytes.length;

        LZ4Factory factory = LZ4Factory.fastestInstance();

        LZ4Compressor compressor = factory.fastCompressor();
        int maxCompressedLength = compressor.maxCompressedLength(bytes.length);
        byte[] compressed = new byte[maxCompressedLength];
        int compressedLength = compressor.compress(bytes, 0, bytes.length, compressed, 0, maxCompressedLength);
        bytes = new byte[compressedLength];
        System.arraycopy(compressed, 0, bytes, 0, compressedLength);
        actualLen = bytes.length;
      }


      //System.arraycopy(lenBuffer, 0, bytes, 0, lenBuffer.length);

      if (bytes.length > 1000000000) {
        throw new DatabaseException("Invalid allocation: size=" + bytes.length);
      }

      long address = unsafe.allocateMemory(bytes.length + 16);

      DataUtils.intToAddress(origLen, address + 8, unsafe);
      DataUtils.intToAddress(actualLen, address + 8 + 4, unsafe);

      for (int i = 7; i >= 0; i--) {
        unsafe.putByte(address + 16 + i, (byte)(updateTime & 0xFF));
        updateTime >>= 8;
      }

      for (int i = 0; i < bytes.length; i++) {
        unsafe.putByte(address + 16 + i, bytes[i]);
      }

      if (address == 0 || address == -1L) {
        throw new DatabaseException("Inserted null address *****************");
      }

      return addAddress(address, updateTime);
    }
  }
  public Object toUnsafeFromKeys(byte[][] records) {
    long seconds = (System.currentTimeMillis() - TIME_2017) / 1000;
    return toUnsafeFromKeys(seconds, records);
  }


  public Object toUnsafeFromKeys(long updateTime, byte[][] records) {
    return toUnsafeFromRecords(updateTime, records);
  }


  public byte[][] fromUnsafeToRecords(Object obj) {
    try {
      if (obj instanceof Long) {

//        try read-write lock
//            try lock for outer and lock for map
        ReentrantReadWriteLock.ReadLock readLock = getReadLock((Long)obj);
        readLock.lock();
        try {
          Long innerAddress = getAddress((Long) obj);
          if (innerAddress == null) {
//              System.out.println("null address ******************* outerAddress=" + (long) obj);
//              new Exception().printStackTrace();
            return null;
          }

          //          long updateTime = 0;
          //          for (int i = 0; i < 8; i++) {
          //            updateTime <<= 8;
          //            updateTime |= (unsafe.getByte(innerAddress + i) & 0xFF);
          //          }

          //          int count = ((unsafe.getByte(innerAddress + 8 + 0)   & 0xff) << 24) |
          //              ((unsafe.getByte(innerAddress + 8 + 1) & 0xff) << 16) |
          //              ((unsafe.getByte(innerAddress + 8 + 2) & 0xff) << 8) |
          //              (unsafe.getByte(innerAddress + 8 + 3) & 0xff);

          int origLen = ((unsafe.getByte(innerAddress + 8 + 0) & 0xff) << 24) |
              ((unsafe.getByte(innerAddress + 8 + 1) & 0xff) << 16) |
              ((unsafe.getByte(innerAddress + 8 + 2) & 0xff) << 8) |
              (unsafe.getByte(innerAddress + 8 + 3) & 0xff);

          int actualLen = ((unsafe.getByte(innerAddress + 12 + 0) & 0xff) << 24) |
              ((unsafe.getByte(innerAddress + 12 + 1) & 0xff) << 16) |
              ((unsafe.getByte(innerAddress + 12 + 2) & 0xff) << 8) |
              (unsafe.getByte(innerAddress + 12 + 3) & 0xff);
          //          byte[] bytes = new byte[count];
          //          for (int i = 0; i < count; i++) {
          //            bytes[i] = unsafe.getByte(innerAddress + i + 8 + 8);
          //          }
          if (origLen == -1) {
            int offset = 0;
            int recCount = DataUtils.addressToInt(innerAddress + offset + 16, unsafe);
            offset += 4;
            byte[][] ret = new byte[recCount][];
            for (int i = 0; i < ret.length; i++) {
              int len = DataUtils.addressToInt(innerAddress + offset + 16, unsafe);
              offset += 4;
              byte[] record = new byte[len];
              for (int j = 0; j < len; j++) {
                record[j] = unsafe.getByte(innerAddress + offset + 16);
                offset++;
              }
              ret[i] = record;
            }
            return ret;
          }

          if (origLen != -1) {
            LZ4Factory factory = LZ4Factory.fastestInstance();

            byte[] bytes = new byte[actualLen];
            for (int j = 0; j < actualLen; j++) {
              bytes[j] = unsafe.getByte(innerAddress + 16 + j);
            }

            LZ4FastDecompressor decompressor = factory.fastDecompressor();
            byte[] restored = new byte[origLen];
            decompressor.decompress(bytes, 0, restored, 0, origLen);
            bytes = restored;

            DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
            //in.readInt(); //byte count
            //in.readInt(); //orig len
            byte[][] ret = new byte[(int) in.readInt()][];
            for (int i = 0; i < ret.length; i++) {
              int len = (int) in.readInt();
              byte[] record = new byte[len];
              in.readFully(record);
              ret[i] = record;
            }
            return ret;
          }
        }
        finally {
          readLock.unlock();
        }
      }
      else {
        if (!server.compressRecords()) {
          return ((IndexValue)obj).records;
        }
        byte[] bytes = ((IndexValue)obj).bytes;
        byte[] lenBuffer = new byte[8];
        System.arraycopy(bytes, 0, lenBuffer, 0, lenBuffer.length);
        ByteArrayInputStream bytesIn = new ByteArrayInputStream(lenBuffer);
        DataInputStream in = new DataInputStream(bytesIn);
        int count = in.readInt();
        int origLen = in.readInt();

        if (origLen != -1) {
          LZ4FastDecompressor decompressor = factory.fastDecompressor();
          byte[] restored = new byte[origLen];
          decompressor.decompress(bytes, lenBuffer.length, restored, 0, origLen);
          bytes = restored;
        }

        in = new DataInputStream(new ByteArrayInputStream(bytes));
        byte[][] ret = new byte[(int) Varint.readSignedVarLong(in)][];
        for (int i = 0; i < ret.length; i++) {
          int len = (int) Varint.readSignedVarLong(in);
          byte[] record = new byte[len];
          in.readFully(record);
          ret[i] = record;
        }
        return ret;
      }
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
    return null;
  }

  public byte[][] fromUnsafeToKeys(Object obj) {
    return fromUnsafeToRecords(obj);
  }

  public void freeUnsafeIds(Object obj) {
    try {
      if (obj instanceof Long) {
        removeAddress((Long)obj, unsafe);
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

}

