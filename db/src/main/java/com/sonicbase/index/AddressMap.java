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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.sonicbase.server.DatabaseServer.TIME_2017;

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class AddressMap {

  private LongList[] map = new LongArrayList[10_000];
  private AtomicLong currOuterAddress = new AtomicLong();
  private final ConcurrentLinkedQueue<Long> freeList = new ConcurrentLinkedQueue<>();
  private ReentrantReadWriteLock[] readWriteLocks = new ReentrantReadWriteLock[10_000];
  private ReentrantReadWriteLock.ReadLock[] readLocks = new ReentrantReadWriteLock.ReadLock[10_000];
  private ReentrantReadWriteLock.WriteLock[] writeLocks = new ReentrantReadWriteLock.WriteLock[10_000];
  private LZ4Factory factory = LZ4Factory.fastestInstance();
  private Unsafe unsafe = getUnsafe();

  private Long2LongOpenHashMap[] addressMaps = new Long2LongOpenHashMap[10_000];
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

  public AddressMap(DatabaseServer server) {
    this.server = server;

    for (int i = 0; i < readWriteLocks.length; i++) {
      readWriteLocks[i] = new ReentrantReadWriteLock();
      readLocks[i] = readWriteLocks[i].readLock();
      writeLocks[i] = readWriteLocks[i].writeLock();
    }
    for (int i = 0; i < addressMaps.length; i++) {
      addressMaps[i] = new Long2LongOpenHashMap();
      addressMaps[i].defaultReturnValue(-1L);
    }
    for (int i = 0; i < map.length; i++) {
      map[i] = new LongArrayList();
    }
  }

  public void clear() {
    freeList.clear();
    for (int i = 0; i < addressMaps.length; i++) {
      for (long innerAddress : addressMaps[i].values()) {
        unsafe.freeMemory(innerAddress);
      }
    }
    addressMaps = new Long2LongOpenHashMap[10_000];
    for (int i = 0; i < addressMaps.length; i++) {
      addressMaps[i] = new Long2LongOpenHashMap();
      addressMaps[i].defaultReturnValue(-1L);
    }

    map = new LongArrayList[10_000];
    for (int i = 0; i < map.length; i++) {
      map[i] = new LongArrayList();
    }
  }

  private ReentrantReadWriteLock.ReadLock getReadLock(long outerAddress) {
    int slot = (int) (outerAddress % map.length);
    return readLocks[slot];
  }

  private ReentrantReadWriteLock.WriteLock getWriteLock(long outerAddress) {
    int slot = (int) (outerAddress % map.length);
    return writeLocks[slot];
  }

  public long getUpdateTime(Long outerAddress) {
    if (outerAddress != null) {
    ReentrantReadWriteLock.ReadLock readLock = getReadLock(outerAddress);
    readLock.lock();
      try {
        return 999999999999999999L;
      }
      finally {
        readLock.unlock();
      }
    }
    return 0;
  }

  private long addAddress(long innerAddress) {
    long outerAddress = currOuterAddress.incrementAndGet();
    ReentrantReadWriteLock.WriteLock lock = getWriteLock(outerAddress);
    lock.lock();
    try {
      addressMaps[(int) (outerAddress % map.length)].put(outerAddress, innerAddress);
    }
    finally {
      lock.unlock();
    }
    return outerAddress;
  }

  private Long getAddress(long outerAddress) {
    long ret = addressMaps[(int) (outerAddress % map.length)].get(outerAddress);
    if (ret == -1) {
      return null;
    }
    return ret;
  }

  private void removeAddress(long outerAddress, Unsafe unsafe) {
    ReentrantReadWriteLock.WriteLock writeLock = getWriteLock(outerAddress);
    writeLock.lock();
    try {
      long innerAddress = addressMaps[(int) (outerAddress % map.length)].remove(outerAddress);
      if (innerAddress != -1) {
        unsafe.freeMemory(innerAddress);
      }
    }
    finally {
      writeLock.unlock();
    }
  }

  public class IndexValue {
    long updateTime;
    byte[][] records;
    byte[] bytes;

    IndexValue(long updateTime, byte[][] records) {
      this.updateTime = updateTime;
      this.records = records;
    }

    IndexValue(long updateTime, byte[] bytes) {
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
      return toUnsafeForNonUnsafe(updateTime, records);
    }
    else {
      return toUnsafeForUnsafe(updateTime, records);
    }
  }

  private Object toUnsafeForUnsafe(long updateTime, byte[][] records) {
    int recordsLen = 0;
    for (byte[] record : records) {
      recordsLen += record.length;
    }

    byte[] bytes = new byte[4 + (4 * records.length) + recordsLen];
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

      LZ4Factory localFactory = LZ4Factory.fastestInstance();

      LZ4Compressor compressor = localFactory.fastCompressor();
      int maxCompressedLength = compressor.maxCompressedLength(bytes.length);
      byte[] compressed = new byte[maxCompressedLength];
      int compressedLength = compressor.compress(bytes, 0, bytes.length, compressed, 0, maxCompressedLength);
      bytes = new byte[compressedLength];
      System.arraycopy(compressed, 0, bytes, 0, compressedLength);
      actualLen = bytes.length;
    }

    if (bytes.length > 1000000000) {
      throw new DatabaseException("Invalid allocation: size=" + bytes.length);
    }

    long address = unsafe.allocateMemory(bytes.length + 16L);

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

    return addAddress(address);
  }

  private Object toUnsafeForNonUnsafe(long updateTime, byte[][] records) {
    try {
      if (!server.compressRecords()) {
        return new IndexValue(updateTime, records);
      }
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytesOut);
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

        LZ4Compressor compressor = factory.highCompressor();
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
        return fromUnsafeForUnsafe((Long) obj);
      }
      else {
        return fromUnsafeForNonUnsafe((IndexValue) obj);
      }
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  private byte[][] fromUnsafeForNonUnsafe(IndexValue obj) throws IOException {
    if (!server.compressRecords()) {
      return obj.records;
    }
    byte[] bytes = obj.bytes;
    byte[] lenBuffer = new byte[8];
    System.arraycopy(bytes, 0, lenBuffer, 0, lenBuffer.length);
    ByteArrayInputStream bytesIn = new ByteArrayInputStream(lenBuffer);
    DataInputStream in = new DataInputStream(bytesIn);
    in.readInt(); // count
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

  private byte[][] fromUnsafeForUnsafe(Long obj) throws IOException {
    ReentrantReadWriteLock.ReadLock readLock = getReadLock(obj);
    readLock.lock();
    try {
      Long innerAddress = getAddress(obj);
      if (innerAddress == null) {
        return null;
      }
      int origLen = ((unsafe.getByte(innerAddress + 8 + 0) & 0xff) << 24) |
          ((unsafe.getByte(innerAddress + 8 + 1) & 0xff) << 16) |
          ((unsafe.getByte(innerAddress + 8 + 2) & 0xff) << 8) |
          (unsafe.getByte(innerAddress + 8 + 3) & 0xff);

      int actualLen = ((unsafe.getByte(innerAddress + 12 + 0) & 0xff) << 24) |
          ((unsafe.getByte(innerAddress + 12 + 1) & 0xff) << 16) |
          ((unsafe.getByte(innerAddress + 12 + 2) & 0xff) << 8) |
          (unsafe.getByte(innerAddress + 12 + 3) & 0xff);
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
        LZ4Factory localFactory = LZ4Factory.fastestInstance();

        byte[] bytes = new byte[actualLen];
        for (int j = 0; j < actualLen; j++) {
          bytes[j] = unsafe.getByte(innerAddress + 16 + j);
        }

        LZ4FastDecompressor decompressor = localFactory.fastDecompressor();
        byte[] restored = new byte[origLen];
        decompressor.decompress(bytes, 0, restored, 0, origLen);
        bytes = restored;

        DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
        byte[][] ret = new byte[in.readInt()][];
        for (int i = 0; i < ret.length; i++) {
          int len = in.readInt();
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

