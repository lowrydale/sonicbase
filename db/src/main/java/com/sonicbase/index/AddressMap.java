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
import java.util.concurrent.ConcurrentLinkedQueue;
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
  private final Object[] mutexes = new Object[10_000];
  private final Unsafe unsafe = getUnsafe();
  private final boolean useUnsafe;
  private Thread compactionThread;
  private final DatabaseServer server;
  private Long2LongOpenHashMap[] addressMaps = new Long2LongOpenHashMap[10_000];
  private ConcurrentHashMap<Long, Page> pages = new ConcurrentHashMap<>();
  private ConcurrentHashMap<Long, Long> addressMap = new ConcurrentHashMap<>();
  private boolean shutdown;
  private Thread freeQueueThread;

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
    this.useUnsafe = true;//server.shouldUseUnsafe();
    for (int i = 0; i < mutexes.length; i++) {
      mutexes[i] = new Object();
    }
    for (int i = 0; i < addressMaps.length; i++) {
      addressMaps[i] = new Long2LongOpenHashMap();
      addressMaps[i].defaultReturnValue(-1L);
    }

    if (MEM_OP) {
      compactionThread = new Thread(new Compactor(), "SonicBase Memory Compactor");
      compactionThread.start();
    }
    startFreeThread();
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
    this.shutdown = true;
    if (freeQueueThread != null) {
      freeQueueThread.interrupt();
    }
    stopCompactor();
  }

  private static class FreeRequest {
    private long address;
    private long timeAdded;
  }

  private ConcurrentLinkedQueue<FreeRequest> freeRequests = new ConcurrentLinkedQueue<>();
  private long lastLogged = System.currentTimeMillis();

  private void startFreeThread() {
    freeQueueThread = new Thread(new Runnable(){
      @Override
      public void run() {
        while (!shutdown) {
          try {
            FreeRequest request = freeRequests.peek();
            if (request == null) {
              Thread.sleep(1_000);
              continue;
            }
            if (System.currentTimeMillis() - request.timeAdded < 10_000) {
              Thread.sleep(1_000);
              continue;
            }
            request = freeRequests.poll();
            if (request == null) {
              Thread.sleep(1_000);
              continue;
            }
            removeAddress(request.address, unsafe);

            if (System.currentTimeMillis() - lastLogged > 10_000) {
              logger.info("Free queue status: size={}", freeRequests.size());
              lastLogged = System.currentTimeMillis();
            }
          }
          catch (Exception e) {
            logger.error("error freeing address", e);
          }
        }
      }
    });
    freeQueueThread.start();
  }

  public void delayedFreeUnsafeIds(Object value) {
    FreeRequest request = new FreeRequest();
    request.address = (long)value;
    request.timeAdded = System.currentTimeMillis();
    freeRequests.add(request);
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
      Object mutex = getMutex(prevOuterAddress);
      synchronized (mutex) {
        boolean active = unsafe.getByte(startOffset + 1) != 1;
        offset += 4; //offset from top of page allocation
        offset += 8; //update time
        offset += DataUtils.addressToInt(offset, unsafe);
        offset += 4; //actualSize
        if (active) {
          int size = (int) (offset - startOffset);
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
            missSize.addAndGet((int) (offset - startOffset));
            missCount.incrementAndGet();
          }
        }
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

  private Object getMutex(long outerAddress) {
    int slot = (int) (outerAddress % addressMaps.length);
    return mutexes[slot];
  }

  public long getUpdateTime(Object outerAddress) {
    if (outerAddress instanceof AddressEntry) {
      return ((AddressEntry)outerAddress).updateTime;
    }
    if (outerAddress != null) {
      Object mutex = getMutex((long)outerAddress);
      synchronized (mutex) {
        return 999999999999999999L;
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
      Object mutex = getMutex(outerAddress);
      synchronized (mutex) {
        if (USE_FAST_UTIL) {
          addressMaps[(int) (outerAddress % addressMaps.length)].put(outerAddress, innerAddress);
        }
        else {
          addressMap.put(outerAddress, innerAddress);
        }
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
      Object mutex = getMutex(outerAddress);
      synchronized (mutex) {
        Long innerAddress = null;
        if (USE_FAST_UTIL) {
          innerAddress = addressMaps[(int) (outerAddress % addressMaps.length)].remove(outerAddress);
        }
        else {
          innerAddress = addressMap.remove(outerAddress);
        }
        //        if (innerAddress != null && innerAddress != -1) {
        //          if (1 == unsafe.getByte(innerAddress)) {
        //            freeSingleAllocation(unsafe, innerAddress);
        //          }
        //          else {
        //            freeMultipleAllocations(unsafe, innerAddress);
        //          }
        //        }
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
      //logger.error("Page not found");
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
    return toUnsafeFromRecords(seconds, records, null);
  }

  public Object toUnsafeFromRecords(byte[] records) {
    long seconds = (System.currentTimeMillis() - TIME_2017) / 1000;
    return toUnsafeFromRecords(seconds, null, records);
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
    return toUnsafeFromRecords(updateTime, records, null);
  }

  public Object toUnsafeFromRecords(long updateTime, byte[][] records, byte[] record) {
    if (MEM_OP) {
      return toUnsafeForUnsafe(updateTime, records, record);
    }
    if (!useUnsafe) {
      return new AddressEntry(record != null ? new byte[][]{record} : records, updateTime);
    }
    return toUnsafeForUnsafe(updateTime, records, record);
  }

  private Object toUnsafeForUnsafe(long updateTime, byte[][] records, byte[] record) {
    if (MEM_OP) {
      int recordsLen = 0;
      if (record != null) {
        recordsLen = record.length;
      }
      else {
        for (byte[] rec : records) {
          recordsLen += rec.length;
        }
      }

      int recordCount = 1;
      if (records != null) {
        recordCount = records.length;
      }
      byte[] bytes = new byte[1 + 1 + 8 + 4 + 8 + 4 + 4 + (4 * recordCount) + recordsLen];
      int offset = 0;
      bytes[offset] = 1; //single allocation
      offset += 1;
      bytes[offset] = 0; //freed
      offset += 1;
      offset += 8; //outerAddress
      offset += 4; //offset from top of page allocation
      DataUtils.longToBytes(updateTime, bytes, offset); //update time
      offset += 8;
      DataUtils.intToBytes(4 + (4 * recordCount) + recordsLen, bytes, offset); //actual size
      offset += 4;
      DataUtils.intToBytes(recordCount, bytes, offset);
      offset += 4;
      if (record != null) {
        DataUtils.intToBytes(record.length, bytes, offset);
        offset += 4;
        System.arraycopy(record, 0, bytes, offset, record.length);
        offset += record.length;
      }
      else {
        for (byte[] rec : records) {
          DataUtils.intToBytes(rec.length, bytes, offset);
          offset += 4;
          System.arraycopy(rec, 0, bytes, offset, rec.length);
          offset += record.length;
        }
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
      if (records != null) {
        for (byte[] rec : records) {
          recordsLen += rec.length;
        }
      }
      else {
        recordsLen = record.length;
      }
      int recordCount = 1;
      if (records != null) {
        recordCount = records.length;
      }
      int len = 8 + 4 + (4 * recordCount) + recordsLen;

      if (len > 1000000000) {
        throw new DatabaseException("Invalid allocation: size=" + len);
      }

      long address = unsafe.allocateMemory(len);


      int offset = 0;
      DataUtils.longToAddress(updateTime, address + offset, unsafe); //update time
      offset += 8;
      DataUtils.intToAddress(recordCount, address + offset, unsafe);
      offset += 4;
      if (records != null) {
        for (byte[] rec : records) {
          DataUtils.intToAddress(rec.length, address + offset, unsafe);
          offset += 4;

          for (int i = 0; i < rec.length; i++) {
            unsafe.putByte(address + offset + i, rec[i]);
          }
          offset += rec.length;
        }
      }
      else {
        DataUtils.intToAddress(record.length, address + offset, unsafe);
        offset += 4;

        for (int i = 0; i < record.length; i++) {
          unsafe.putByte(address + offset + i, record[i]);
        }
        offset += record.length;
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
      Object mutex = ((long)address);
      synchronized (mutex) {
        long innerAddress = getAddress((long) address);
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

        if ((long) address == 0 || (long) address == -1L) {
          throw new DatabaseException("Inserted null address *****************");
        }
      }
    }
    else {
      long innerAddress = (long)address;
      int len = 8 + 4 + (4 * records.length) + recordsLen;

      if (len > 1000000000) {
        throw new DatabaseException("Invalid allocation: size=" + len);
      }

      long seconds = (System.currentTimeMillis() - TIME_2017) / 1000;

      int offset = 0;
      DataUtils.longToAddress(seconds, innerAddress + offset, unsafe); //update time
      offset += 8;
      DataUtils.intToAddress(records.length, innerAddress + offset, unsafe);
      offset += 4;
      for (byte[] record : records) {
        DataUtils.intToAddress(record.length, innerAddress + offset, unsafe);
        offset += 4;

        for (int i = 0; i < record.length; i++) {
          unsafe.putByte(innerAddress + offset + i, record[i]);
        }
        offset += record.length;
      }


//      long innerAddress = (long)address;
//      byte[] bytes = new byte[4 + (4 * records.length) + recordsLen];
//      int offset = 0; //update time
//      DataUtils.intToBytes(records.length, bytes, offset);
//      offset += 4;
//      for (byte[] record : records) {
//        DataUtils.intToBytes(record.length, bytes, offset);
//        offset += 4;
//        System.arraycopy(record, 0, bytes, offset, record.length);
//        offset += record.length;
//      }
//
//      long seconds = (System.currentTimeMillis() - TIME_2017) / 1000;
//
//      offset = 0;
//      DataUtils.longToAddress(seconds, innerAddress + offset, unsafe);
//      offset += 8; //update time
//
//      for (int i = 0; i < bytes.length; i++) {
//        unsafe.putByte(innerAddress + offset + i, bytes[i]);
//      }
    }
  }

  public Object toUnsafeFromKeys(byte[][] records) {
    long seconds = (System.currentTimeMillis() - TIME_2017) / 1000;
    return toUnsafeFromKeys(seconds, records);
  }


  public Object toUnsafeFromKeys(long updateTime, byte[][] records) {
    return toUnsafeFromRecords(updateTime, records, null);
  }


  public byte[][] fromUnsafeToRecords(Object obj) {
    try {
      if (MEM_OP) {
        return fromUnsafeForUnsafe((Long) obj);
      }
      if (!useUnsafe) {
        return ((AddressEntry)obj).records;
      }
      return fromUnsafeForUnsafe((Long) obj);
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  private byte[][] fromUnsafeForUnsafe(Long obj) throws IOException {
    if (MEM_OP) {
      Object mutex = getMutex(obj);
      synchronized (mutex) {
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

//  public void freeUnsafeIds(Object obj) {
//    try {
//      if (obj instanceof AddressEntry) {
//        return;
//      }
//      removeAddress((Long)obj, unsafe);
//    }
//    catch (Exception e) {
//      throw new DatabaseException(e);
//    }
//  }

}

