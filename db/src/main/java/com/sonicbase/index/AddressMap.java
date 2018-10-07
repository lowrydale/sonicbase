package com.sonicbase.index;

import com.sonicbase.common.DataUtils;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.server.DatabaseServer;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.sonicbase.server.DatabaseServer.TIME_2017;

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class AddressMap {
  private static final Logger logger = LoggerFactory.getLogger(AddressMap.class);

  public static int MAX_PAGE_SIZE = 256_000;
  public static boolean MEM_OP = false;
  private static boolean USE_FAST_UTIL = true;

  private final AtomicLong currOuterAddress = new AtomicLong();
  private final AtomicLong currPageId = new AtomicLong();
  private final ReentrantReadWriteLock[] readWriteLocks = new ReentrantReadWriteLock[10_000];
  private final ReentrantReadWriteLock.ReadLock[] readLocks = new ReentrantReadWriteLock.ReadLock[10_000];
  private final ReentrantReadWriteLock.WriteLock[] writeLocks = new ReentrantReadWriteLock.WriteLock[10_000];
  private final Unsafe unsafe = getUnsafe();
  private final boolean useUnsafe;
  private Thread compactionThread;
  private final DatabaseServer server;
  private Long2LongOpenHashMap[] addressMaps = new Long2LongOpenHashMap[10_000];
  private ConcurrentHashMap<Long, Page> pages = new ConcurrentHashMap<>();
  private ConcurrentHashMap<Long, Long> addressMap = new ConcurrentHashMap<>();

  public static Unsafe getUnsafe() {
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
    this.useUnsafe = server.shouldUseUnsafe();
    for (int i = 0; i < readWriteLocks.length; i++) {
      readWriteLocks[i] = new ReentrantReadWriteLock();
      readLocks[i] = readWriteLocks[i].readLock();
      writeLocks[i] = readWriteLocks[i].writeLock();
    }
    for (int i = 0; i < addressMaps.length; i++) {
      addressMaps[i] = new Long2LongOpenHashMap();
      addressMaps[i].defaultReturnValue(-1L);
    }

    if (MEM_OP) {
      compactionThread = new Thread(new Compactor(), "SonicBase Memory Compactor");
      compactionThread.start();
    }
  }

  public void stopCompactor() {
    try {
      if (compactionThread != null) {
        compactionThread.interrupt();
        compactionThread.join();
        compactionThread = null;
      }
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  public ConcurrentHashMap<Long, Page> getPages() {
    return pages;
  }

  public void shutdown() {
    stopCompactor();
  }

  class Compactor implements Runnable {

    @Override
    public void run() {
      while (!server.getShutdown()) {
        try {
          Thread.sleep(100);
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }

        doCompact();
      }
    }

  }

  public void doCompact() {
    int currPagesSize = 0;
    List<Page> currPages = new ArrayList<>();
    LongSet allocationsToKeep = new LongOpenHashSet();
    for (Map.Entry<Long, Page> entry : pages.entrySet()) {
      Page currPage = entry.getValue();
      if ((double)currPage.totalFreeSize.get() / (double)currPage.totalSize > 0.05d) {
        if (currPage.totalFreeSize.get() == currPage.totalSize) {
          unsafe.freeMemory(currPage.baseAddress);
          pages.remove(currPage.id);
        }
        else {
          currPages.add(currPage);
          int pageSize = getPageCurrUsedSize(currPage, allocationsToKeep);
          currPagesSize += pageSize;
          if (currPagesSize > MAX_PAGE_SIZE) {
            compactPages(currPagesSize, currPages, allocationsToKeep);
            currPages.clear();
            allocationsToKeep.clear();
            currPagesSize = 0;
          }
        }
      }
    }
    if (!currPages.isEmpty()) {
      compactPages(currPagesSize, currPages, allocationsToKeep);
    }
  }

  private void compactPages(int currPagesSize, List<Page> currPages, LongSet allocationsToKeep) {
    currPagesSize += 12;

    long newAddress = unsafe.allocateMemory(currPagesSize);
    AtomicInteger offset = new AtomicInteger();
    offset.addAndGet(8);
    offset.addAndGet(4);
    AtomicInteger missSize = new AtomicInteger();
    AtomicInteger missCount = new AtomicInteger();
    for (Page pageToMove : currPages) {
      movePage(newAddress, offset, pageToMove, missSize, missCount, allocationsToKeep);
    }
    Page newPage = new Page();
    newPage.totalSize = currPagesSize;
    newPage.totalFreeSize.set(missSize.get());
    newPage.baseAddress = newAddress;
    long newPageId = currPageId.incrementAndGet();
    newPage.id = newPageId;
    DataUtils.longToAddress(newPage.id, newAddress, unsafe);
    DataUtils.intToAddress(allocationsToKeep.size() - missCount.get(), newAddress + 8, unsafe);
    pages.put(newPageId, newPage);

    for (Page movedPage : currPages) {
      unsafe.freeMemory(movedPage.baseAddress);
      pages.remove(movedPage.id);
    }
  }

  private void movePage(long newAddress, AtomicInteger newPageOffset, Page pageToMove, AtomicInteger missSize, AtomicInteger missCount, LongSet allocationsToKeep) {
    long offset = pageToMove.baseAddress;
    offset += 8;
    int allocationCount = DataUtils.addressToInt(offset, unsafe);
    offset += 4;
    for (int i = 0; i < allocationCount; i++) {
      long startOffset = offset;
      offset += 1; //single allocation
      offset += 1; //freed
      long prevOuterAddress = DataUtils.addressToLong(offset, unsafe);
      offset += 8; //outerAddress
      ReentrantReadWriteLock.WriteLock lock = getWriteLock(prevOuterAddress);
      lock.lock();
      try {
        boolean active = unsafe.getByte(startOffset + 1) != 1;
        offset += 4; //offset from top of page allocation
        offset += 8; //update time
        offset += DataUtils.addressToInt(offset, unsafe);
        offset += 4; //actualSize
        if (active) {
          int size = (int)(offset - startOffset);
          unsafe.copyMemory(startOffset, newAddress + newPageOffset.get(), size);
          if (USE_FAST_UTIL) {
            addressMaps[(int) (prevOuterAddress % addressMaps.length)].put(prevOuterAddress, newAddress + newPageOffset.get());
          }
          else {
            addressMap.put(prevOuterAddress, newAddress + newPageOffset.get());
          }
          newPageOffset.addAndGet(size);
        }
        else {
          if (allocationsToKeep.contains(startOffset)) {
            missSize.addAndGet((int)(offset - startOffset));
            missCount.incrementAndGet();
          }
        }
      }
      finally {
        lock.unlock();
      }
    }
  }

  private int getPageCurrUsedSize(Page currPage, LongSet allocationsToKeep) {
    int size = 0;
    long offset = currPage.baseAddress;
    offset += 8;
    int allocationCount = DataUtils.addressToInt(offset, unsafe);
    offset += 4;
    for (int i = 0; i < allocationCount; i++) {
      long startOffset = offset;
      offset += 1; //single allocation
      boolean active = unsafe.getByte(offset) != 1;
      offset += 1; //freed
      offset += 8; //outerAddress
      offset += 4; //offset from top of page allocation
      offset += 8; //update time
      offset += DataUtils.addressToInt(offset, unsafe);
      offset += 4; //actualSize
      if (active) {
        allocationsToKeep.add(startOffset);
        size += offset - startOffset;
      }
    }
    return size;
  }

  public long addPage(long address, int totalSize) {
    Page page = new Page();
    long pageId = currPageId.incrementAndGet();
    page.id = pageId;
    page.baseAddress = address;
    page.totalFreeSize.set(0);
    page.totalSize = totalSize;
    pages.put(pageId, page);
    return pageId;
  }

  public class Page {
    private long id;
    private long baseAddress;
    private int totalSize;
    private AtomicInteger totalFreeSize = new AtomicInteger();

    public long getId() {
      return id;
    }

    public long getBaseAddress() {
      return baseAddress;
    }

    public int getTotalSize() {
      return totalSize;
    }

    public int getTotalFreeSize() {
      return totalFreeSize.get();
    }
  }

  public void clear() {
//    for (int i = 0; i < addressMaps.length; i++) {
//      for (long innerAddress : addressMaps[i].values()) {
//        unsafe.freeMemory(innerAddress);
//      }
//    }
//    addressMaps = new Long2LongOpenHashMap[10_000];
//    for (int i = 0; i < addressMaps.length; i++) {
//      addressMaps[i] = new Long2LongOpenHashMap();
//      addressMaps[i].defaultReturnValue(-1L);
//    }
//
//    map = new LongArrayList[10_000];
//    for (int i = 0; i < map.length; i++) {
//      map[i] = new LongArrayList();
//    }
  }

  private ReentrantReadWriteLock.ReadLock getReadLock(long outerAddress) {
    int slot = (int) (outerAddress % addressMaps.length);
    return readLocks[slot];
  }

  private ReentrantReadWriteLock.WriteLock getWriteLock(long outerAddress) {
    int slot = (int) (outerAddress % addressMaps.length);
    return writeLocks[slot];
  }

  public long getUpdateTime(Object outerAddress) {
    if (outerAddress instanceof AddressEntry) {
      return ((AddressEntry)outerAddress).updateTime;
    }
    if (outerAddress != null) {
    ReentrantReadWriteLock.ReadLock readLock = getReadLock((long)outerAddress);
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

  public long addAddress(long innerAddress) {
    if (!MEM_OP) {
      return innerAddress;
    }
    else {
      long outerAddress = currOuterAddress.incrementAndGet();
          ReentrantReadWriteLock.WriteLock lock = getWriteLock(outerAddress);
          lock.lock();
      try {
        if (USE_FAST_UTIL) {
          addressMaps[(int) (outerAddress % addressMaps.length)].put(outerAddress, innerAddress);
        }
        else {
          addressMap.put(outerAddress, innerAddress);
        }
      }
      finally {
        lock.unlock();
      }
      return outerAddress;
    }
  }

  private Long getAddress(long outerAddress) {
    if (USE_FAST_UTIL) {
      long ret = addressMaps[(int) (outerAddress % addressMaps.length)].get(outerAddress);
      if (ret == -1) {
        return null;
      }
      return ret;
    }

    return addressMap.get(outerAddress);
  }

  private void removeAddress(long outerAddress, Unsafe unsafe) {
    if (!MEM_OP) {
      unsafe.freeMemory(outerAddress);
    }
    else {
      ReentrantReadWriteLock.WriteLock writeLock = getWriteLock(outerAddress);
      writeLock.lock();
      try {
        Long innerAddress = null;
        if (USE_FAST_UTIL) {
          innerAddress = addressMaps[(int) (outerAddress % addressMaps.length)].remove(outerAddress);
        }
        else {
          innerAddress = addressMap.remove(outerAddress);
        }
        if (innerAddress != null && innerAddress != -1) {
          if (1 == unsafe.getByte(innerAddress)) {
            freeSingleAllocation(unsafe, innerAddress);
          }
          else {
            freeMultipleAllocations(unsafe, innerAddress);
          }
        }
      }
      finally {
        writeLock.unlock();
      }
    }
  }

  private void freeMultipleAllocations(Unsafe unsafe, Long innerAddress) {
    unsafe.putByte(innerAddress + 1, (byte)1);

    int offset = 0;
    offset += 1; //single allocation
    offset += 1; //freed
    offset += 8; //outerAddress
    int offsetFromTop = DataUtils.addressToInt(innerAddress + offset, unsafe);
    offset += 4; //offset from top of page allocation
    offset += 8; //update time
    offset += DataUtils.addressToInt(innerAddress + offset, unsafe);
    offset += 4; //actualSize

    long pageId = DataUtils.addressToLong(innerAddress - offsetFromTop, unsafe);

    Page page = pages.get(pageId);
    if (page == null) {
      logger.error("Page not found");
    }
    else {
      page.totalFreeSize.addAndGet(offset);
    }
  }

  private void freeSingleAllocation(Unsafe unsafe, Long innerAddress) {
//          int offset = 0;
//          offset += 1; //single allocation
//          offset += 1; //freed
//          offset += 8; //outerAddress
//          int offsetFromTop = DataUtils.addressToInt(innerAddress + offset, unsafe);
    unsafe.freeMemory(innerAddress /*- offsetFromTop*/);
  }

  public Object toUnsafeFromRecords(byte[][] records) {
    long seconds = (System.currentTimeMillis() - TIME_2017) / 1000;
    return toUnsafeFromRecords(seconds, records);
  }

  public static class AddressEntry {
    private byte[][] records;
    private long updateTime;

    public AddressEntry(byte[][] records, long updateTime) {
      this.records = records;
      this.updateTime = updateTime;
    }
  }

  public Object toUnsafeFromRecords(long updateTime, byte[][] records) {
    if (!useUnsafe) {
      return new AddressEntry(records, updateTime);
    }
    return toUnsafeForUnsafe(updateTime, records);
  }

  private Object toUnsafeForUnsafe(long updateTime, byte[][] records) {
    if (MEM_OP) {
      int recordsLen = 0;
      for (byte[] record : records) {
        recordsLen += record.length;
      }

      byte[] bytes = new byte[1 + 1 + 8 + 4 + 8 + 4 + 4 + (4 * records.length) + recordsLen];
      int offset = 0;
      bytes[offset] = 1; //single allocation
      offset += 1;
      bytes[offset] = 0; //freed
      offset += 1;
      offset += 8; //outerAddress
      offset += 4; //offset from top of page allocation
      DataUtils.longToBytes(updateTime, bytes, offset); //update time
      offset += 8;
      DataUtils.intToBytes(4 + (4 * records.length) + recordsLen, bytes, offset); //actual size
      offset += 4;
      DataUtils.intToBytes(records.length, bytes, offset);
      offset += 4;
      for (byte[] record : records) {
        DataUtils.intToBytes(record.length, bytes, offset);
        offset += 4;
        System.arraycopy(record, 0, bytes, offset, record.length);
        offset += record.length;
      }

      if (bytes.length > 1000000000) {
        throw new DatabaseException("Invalid allocation: size=" + bytes.length);
      }

      long address = unsafe.allocateMemory(bytes.length);

      for (int i = 0; i < bytes.length; i++) {
        unsafe.putByte(address + i, bytes[i]);
      }

      long outerAddress = addAddress(address);
      DataUtils.longToAddress(outerAddress, address + 2, unsafe);
      return outerAddress;
    }
    else {
      int recordsLen = 0;
      for (byte[] record : records) {
        recordsLen += record.length;
      }

      byte[] bytes = new byte[8 + 4 + (4 * records.length) + recordsLen];
      int offset = 0;
      DataUtils.longToBytes(updateTime, bytes, offset); //update time
      offset += 8;
      DataUtils.intToBytes(records.length, bytes, offset);
      offset += 4;
      for (byte[] record : records) {
        DataUtils.intToBytes(record.length, bytes, offset);
        offset += 4;
        System.arraycopy(record, 0, bytes, offset, record.length);
        offset += record.length;
      }

      if (bytes.length > 1000000000) {
        throw new DatabaseException("Invalid allocation: size=" + bytes.length);
      }

      long address = unsafe.allocateMemory(bytes.length);

      for (int i = 0; i < bytes.length; i++) {
        unsafe.putByte(address + i, bytes[i]);
      }
      return address;
    }
  }

  public void writeRecordstoExistingAddress(Object address, byte[][] records) {
    if (address instanceof AddressEntry) {
      ((AddressEntry)address).records = records;
      return;
    }
    int recordsLen = 0;
    for (byte[] record : records) {
      recordsLen += record.length;
    }

    if (MEM_OP) {
      ReentrantReadWriteLock.WriteLock lock = getWriteLock((long)address);
      lock.lock();
      try {

        long innerAddress = getAddress((long)address);
        byte[] bytes = new byte[4 + (4 * records.length) + recordsLen];
        int offset = 0; //update time
        DataUtils.intToBytes(records.length, bytes, offset);
        offset += 4;
        for (byte[] record : records) {
          DataUtils.intToBytes(record.length, bytes, offset);
          offset += 4;
          System.arraycopy(record, 0, bytes, offset, record.length);
          offset += record.length;
        }

        long seconds = (System.currentTimeMillis() - TIME_2017) / 1000;

        offset = 0;
        offset += 1;//single allocation
        offset += 1; //freed
        offset += 8; //outerAddress
        offset += 4; //offset from top of page allocation
        DataUtils.longToAddress(seconds, innerAddress + offset, unsafe);
        offset += 8; //update time
        offset += 4; //actual size

        for (int i = 0; i < bytes.length; i++) {
          unsafe.putByte(innerAddress + offset + i, bytes[i]);
        }

        if ((long)address == 0 || (long)address == -1L) {
          throw new DatabaseException("Inserted null address *****************");
        }
      }
      finally {
        lock.unlock();
      }
    }
    else {
      long innerAddress = (long)address;
      byte[] bytes = new byte[4 + (4 * records.length) + recordsLen];
      int offset = 0; //update time
      DataUtils.intToBytes(records.length, bytes, offset);
      offset += 4;
      for (byte[] record : records) {
        DataUtils.intToBytes(record.length, bytes, offset);
        offset += 4;
        System.arraycopy(record, 0, bytes, offset, record.length);
        offset += record.length;
      }

      long seconds = (System.currentTimeMillis() - TIME_2017) / 1000;

      offset = 0;
      DataUtils.longToAddress(seconds, innerAddress + offset, unsafe);
      offset += 8; //update time

      for (int i = 0; i < bytes.length; i++) {
        unsafe.putByte(innerAddress + offset + i, bytes[i]);
      }
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
    if (!useUnsafe) {
      return ((AddressEntry)obj).records;
    }
    try {
      return fromUnsafeForUnsafe((Long) obj);
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  private byte[][] fromUnsafeForUnsafe(Long obj) throws IOException {
    if (MEM_OP) {
      ReentrantReadWriteLock.ReadLock readLock = getReadLock(obj);
      readLock.lock();
      try {
        Long innerAddress = getAddress(obj);
        if (innerAddress == null || (long) innerAddress == -1L) {
          return null;
        }

        int offset = 0;
        offset += 1;//single allocation
        offset += 1; //freed
        offset += 8; //outerAddress
        offset += 4; //offset from top of page allocation
        offset += 8; //update time
        offset += 4; //actual size
        int recCount = DataUtils.addressToInt(innerAddress + offset, unsafe);
        offset += 4;
        byte[][] ret = new byte[recCount][];
        for (int i = 0; i < ret.length; i++) {
          int len = DataUtils.addressToInt(innerAddress + offset, unsafe);
          offset += 4;
          byte[] record = new byte[len];
          for (int j = 0; j < len; j++) {
            record[j] = unsafe.getByte(innerAddress + offset);
            offset++;
          }
          ret[i] = record;
        }
        return ret;
      }
      finally {
        readLock.unlock();
      }
    }
    else {
      Long innerAddress = obj;
      if (innerAddress == null || (long) innerAddress == -1L) {
        return null;
      }

      int offset = 0;
      offset += 8; //update time
      int recCount = DataUtils.addressToInt(innerAddress + offset, unsafe);
      offset += 4;
      byte[][] ret = new byte[recCount][];
      for (int i = 0; i < ret.length; i++) {
        int len = DataUtils.addressToInt(innerAddress + offset, unsafe);
        offset += 4;
        byte[] record = new byte[len];
        for (int j = 0; j < len; j++) {
          record[j] = unsafe.getByte(innerAddress + offset);
          offset++;
        }
        ret[i] = record;
      }
      return ret;
    }
  }

  public byte[][] fromUnsafeToKeys(Object obj) {
    return fromUnsafeToRecords(obj);
  }

  public void freeUnsafeIds(Object obj) {
    try {
      if (obj instanceof AddressEntry) {
        return;
      }
      removeAddress((Long)obj, unsafe);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

}

