package com.sonicbase.index;

import com.sonicbase.client.DatabaseClient;
import sun.misc.Unsafe;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class NativePartitionedTreeImpl extends NativePartitionedTree implements IndexImpl {

  private final Object mutex = new Object();

  static {

    //System.load("C:/Users/lowryda/source/repos/SonicBase/x64/Release/PartitionedAvlTree.dll");

    try {
      System.load(new File(System.getProperty("user.home"), "SonicBase.so").getAbsolutePath());
    }
    catch (UnsatisfiedLinkError e1) {
      try {
        System.load("/Users/lowryda/Dropbox/git/sonicbase/native/PartitionedAvlTree/mac/SonicBase.so");
      }
      catch (UnsatisfiedLinkError e) {
        try {
          System.load("../lib/linux/SonicBase.so");
        }
        catch (UnsatisfiedLinkError e2) {
          e2.printStackTrace();
        }
      }
    }
  }

  private final long indexId;
  private final Index index;


  public NativePartitionedTreeImpl(Index index) {
    this.index = index;
    this.indexId = initIndex();
  }

  @Override
  public Object put(Object[] key, Object value) {
    byte[] keyBytes = new byte[8];
    writeLong(keyBytes, (long)key[0]);

    long ret = put(indexId, keyBytes, (long)value);
    if (ret == -1) {
      index.getSizeObj().incrementAndGet();
      return null;
    }
    return ret;
  }

  public byte[] putBytes(Object[] key, byte[] value) {
    byte[] keyBytes = new byte[8];
    writeLong(keyBytes, (long)key[0]);

    byte[] ret = putBytes(indexId, keyBytes, value);
    if (ret == null) {
      index.getSizeObj().incrementAndGet();
      return null;
    }
    return ret;
  }

  @Override
  public Object get(Object[] key) {
    byte[] keyBytes = new byte[8];
    writeLong(keyBytes, (long)key[0]);

    long ret = get(indexId, keyBytes);
    if (-1 == ret) {
      return null;
    }
    return ret;
  }

  @Override
  public Object remove(Object[] key) {
    byte[] keyBytes = new byte[8];
    writeLong(keyBytes, (long)key[0]);

    long ret = remove(indexId, keyBytes);
    if (ret != -1) {
      index.getSizeObj().decrementAndGet();
      return ret;
    }
    return null;
  }

  @Override
  public int tailBlock(Object[] startKey, int count, boolean first, Object[][] keys, Object[] values) {
    try {
      byte[] keyBytes = new byte[8];
      writeLong(keyBytes, (long)startKey[0]);

      byte[] ret = tailBlock(indexId, keyBytes, count, first);
      return parseResults(ret, keys, values);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public int tailBlockBytes(Object[] startKey, int count, boolean first, Object[][] keys, byte[][] values) {
    try {
      byte[] keyBytes = new byte[8];
      writeLong(keyBytes, (long)startKey[0]);

      byte[] ret = tailBlockBytes(indexId, keyBytes, count, first);
      return parseResultsBytes(ret, keys, values);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int headBlock(Object[] startKey, int count, boolean first, Object[][] keys, Object[] values) {
    try {
      byte[] keyBytes = new byte[8];
      writeLong(keyBytes, (long)startKey[0]);

      byte[] ret = headBlock(indexId, keyBytes, count, first);
      return parseResults(ret, keys, values);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Map.Entry<Object[], Object> higherEntry(Object[] key) {
    byte[] keyBytes = new byte[8];
    writeLong(keyBytes, (long)key[0]);

    byte[] ret = higherEntry(indexId, keyBytes);
    return parseResults(ret);
  }

  @Override
  public Map.Entry<Object[], Object> lowerEntry(Object[] key) {
    byte[] keyBytes = new byte[8];
    writeLong(keyBytes, (long)key[0]);

    byte[] ret = lowerEntry(indexId, keyBytes);
    return parseResults(ret);
  }

  @Override
  public Map.Entry<Object[], Object> floorEntry(Object[] key) {
    byte[] keyBytes = new byte[8];
    writeLong(keyBytes, (long)key[0]);

    byte[] ret = floorEntry(indexId, keyBytes);
    return parseResults(ret);
  }

  @Override
  public List<Map.Entry<Object[], Object>> equalsEntries(Object[] key) {
    List<Map.Entry<Object[], Object>> ret = new ArrayList<>();

    synchronized (this) {
      Map.Entry<Object[], Object> entry = floorEntry(key);
      if (entry == null) {
        return null;
      }
      ret.add(entry);
      while (true) {
        entry = higherEntry(entry.getKey());
        if (entry == null) {
          break;
        }
        if ((long) entry.getKey()[0] != (long) key[0]) {
          return ret;
        }
        ret.add(entry);
      }
      return ret;
    }
  }


  @Override
  public Map.Entry<Object[], Object> ceilingEntry(Object[] key) {
    byte[] keyBytes = new byte[8];
    writeLong(keyBytes, (long)key[0]);

    byte[] ret = ceilingEntry(indexId, keyBytes);
    return parseResults(ret);
  }

  @Override
  public Map.Entry<Object[], Object> lastEntry() {
    byte[] ret = lastEntry2(indexId);
    return parseResults(ret);
  }

  @Override
  public Map.Entry<Object[], Object> firstEntry() {
    byte[] ret = firstEntry2(indexId);
    return parseResults(ret);
  }

  @Override
  public void clear() {
    clear(indexId);
  }

  private Map.Entry<Object[],Object> parseResults(byte[] ret) {
    try {
      if (ret == null) {
        return null;
      }
      AtomicInteger offset = new AtomicInteger();
      int len = readInt(ret, offset);
      if (len == 0) {
        return null;
      }
      long key = readLong(ret, offset);
      long value = readLong(ret, offset);
      return new Index.MyEntry(new Object[]{key}, value);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


  private int parseResults(byte[] results, Object[][] keys, Object[] values) throws IOException {
    AtomicInteger offset = new AtomicInteger();
    int count = readInt(results, offset);
    long[] ret = new long[count];
    for (int i = 0; i < count; i++) {
      int len = readInt(results, offset);
      long key = readLong(results, offset);
      long value = readLong(results, offset);
      keys[i] = new Object[]{key};
      values[i] = value;
      ret[i] = key;
    }
    return count;
  }

  private int parseResultsBytes(byte[] results, Object[][] keys, byte[][] values) throws IOException {
    AtomicInteger offset = new AtomicInteger();
    int count = readInt(results, offset);
    long[] ret = new long[count];
    for (int i = 0; i < count; i++) {
      int len = readInt(results, offset);
      long key = readLong(results, offset);
      len = readInt(results, offset);
      byte[] bytes = new byte[len];
      System.arraycopy(results, offset.get(), bytes, 0, len);
      offset.addAndGet(len);
      keys[i] = new Object[]{key};
      values[i] = bytes;
      ret[i] = key;
    }
    return count;
  }

  private List<Map.Entry<Object[], Object>> parseResultsList(byte[] results) throws IOException {
    AtomicInteger offset = new AtomicInteger();
    int count = readInt(results, offset);
    List<Map.Entry<Object[], Object>> retList = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      int len = readInt(results, offset);
      long key = readLong(results, offset);
      long value = readLong(results, offset);
      Index.MyEntry<Object[], Object> entry = new Index.MyEntry<>(new Object[]{key}, value);
      retList.add(entry);
    }
    return retList;
  }

  public static void main(String[] args) throws InterruptedException, ExecutionException {
    //Thread.sleep(10000000);
    Index index = new Index();
    new NativePartitionedTreeImpl(index).bench();
  }

  private static int BLOCK_SIZE = DatabaseClient.SELECT_PAGE_SIZE;

  private final Unsafe unsafe = getUnsafe();
  public static Unsafe getUnsafe() {
    try {
      Field singleoneInstanceField = Unsafe.class.getDeclaredField("theUnsafe");
      singleoneInstanceField.setAccessible(true);
      return (Unsafe) singleoneInstanceField.get(null);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private final Object[] mutexes = new Object[100_000];

  private void bench() throws ExecutionException, InterruptedException {
    boolean mixg = false;
    getUnsafe();
    for (int i = 0; i < mutexes.length; i++) {
      mutexes[i] = new Object();
    }

    long[] indices = new long[32];
    for (int i = 0; i < 32; i++) {
      indices[i] = initIndex();
    }
    ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 10_000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
    ThreadPoolExecutor executor2 = new ThreadPoolExecutor(128, 128, 10_000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
    ThreadPoolExecutor executor3 = new ThreadPoolExecutor(24, 24, 10_000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
    final AtomicInteger countInserted = new AtomicInteger();
    final AtomicLong countRead = new AtomicLong();
    final List<Future> futures = new ArrayList<>();
    final AtomicLong last = new AtomicLong(System.currentTimeMillis());
    final long begin = System.currentTimeMillis();

    final AtomicLong localCountInserted = new AtomicLong();
    List<Future> futures2 = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      final int offset = i;
      futures2.add(executor3.submit((Callable) () -> {
        for (int i1 = 0; i1 < 100_000_000; i1++) {
          //            Object[] key = new Object[2];
          //            key[0] = (long)i1;
          //            key[1] = String.valueOf(i1).getBytes("utf-8");

          if (i1 % 4 == offset) {
            byte[] keyBytes = new byte[8];
            writeLong(keyBytes, i1);

            if (true) {
              long address = unsafe.allocateMemory(75);
              for (int l = 0; l < 75; l++) {
                unsafe.putByte(address + l, (byte)0);
              }

              //synchronized (mutexes[(int) i1 % mutexes.length]) {
                put(indices[0 /*currIndex*/], keyBytes, address);
              //}
            }
            else {
              putBytes(new Object[]{(long)i1}, new byte[75]);
            }
            //                          if (i1 % 150 == 0) {
            //                Thread.sleep(5);
            //              }
            localCountInserted.incrementAndGet();
            if (countInserted.incrementAndGet() % 1_000_000 == 0) {
              System.out.println("insert progress: count=" + countInserted.get() + ", rate=" + ((float) countInserted.get() / (System.currentTimeMillis() - begin) * 1000f));
            }
          }
        }
        return null;
      }));
    }

    for (int i = 0; i < 32; i++) {
      final int currIndex = i;
      futures.add(executor.submit((Callable) () -> {

        if (true  ) {
          futures2.add(executor2.submit((Callable) () -> {
            try {
              Thread.sleep(5_000);
//          (NativePartitionedTree.this) {
              for (int j = 0; j < (mixg ? 1000 : 100_000); j++) {
                if (mixg) {
                  for (int i1 = 0; i1 < localCountInserted.get() - 1000; i1++) {
                    byte[] keyBytes = new byte[8];
                    writeLong(keyBytes, i1);
                    long value = get(indices[0 /*currIndex*/], keyBytes);
                    if (value != i1) {
                      System.out.println("key mismatch: expected=" + i1 + ", actual=" + value);
                    }
                    long count = countRead.addAndGet(1);
                    //(mutex) {
                    if (System.currentTimeMillis() - last.get() > 1_000) {
                      last.set(System.currentTimeMillis());
                      System.out.println("read progress: count=" + countRead.get() + ", rate=" + ((float) countRead.get() / (System.currentTimeMillis() - begin) * 1000f));
                      System.out.flush();
                    }
                    //}
                  }
                }
                else {

                  boolean first = true;
                  for (int i1 = 0; i1 < localCountInserted.get() - 1000000; i1 += BLOCK_SIZE) {
                    byte[] keyBytes = new byte[8];
                    writeLong(keyBytes, i1 - 1);

                    Object[][] keys = new Object[BLOCK_SIZE + 200][];
                    int retCount = 0;
                    if (true) {
                      byte[] results = tailBlock(indices[0], keyBytes, BLOCK_SIZE, first);
                      long count = countRead.addAndGet(BLOCK_SIZE);
                      Object[] values = new Object[BLOCK_SIZE + 200];
                      retCount = parseResults(results, keys, values);
                      //                    if (i1 < 1024) {
                      for (int k = 0; k < retCount; k++) {
//                        synchronized (mutexes[(int) ((long) keys[k][0] % mutexes.length)]) {
//                          get(keys[k]);
                          long address = (long)values[k];
                          byte[] bytes = new byte[75];
                          for (int l = 0; l < 75; l++) {
                            bytes[l] = unsafe.getByte(address + l);
                          }
//                        }
                      }

                    }
                    else {
                      byte[][] values = new byte[BLOCK_SIZE + 200][];
                      retCount = tailBlockBytes(new Object[]{((long)i1) - 1}, BLOCK_SIZE, first, keys, values);
                      for (int k = 0; k < retCount; k++) {
                        if (values[k].length != 75) {
                          System.out.println("size mismatch");
                        }
                      }

                    }
                    countRead.addAndGet(retCount);
                    for (int keyOffset = 0; keyOffset < retCount; keyOffset++) {
                      if ((long)keys[keyOffset][0] != i1 + keyOffset) {
                        System.out.println("key mismatch: expected=" + (i1 + keyOffset) + ", actual=" + keys[keyOffset][0]);
                      }
                    }
//                    }
                    //(mutex) {
                    if (System.currentTimeMillis() - last.get() > 1_000) {
                      last.set(System.currentTimeMillis());
                      System.out.println("read progress: count=" + countRead.get() + ", rate=" + ((float) countRead.get() / (System.currentTimeMillis() - begin) * 1000f));
                      System.out.flush();
                    }
                    first = false;
                    //}
                  }
                }
              }
              for (Future future : futures2) {
                future.get();
              }
              System.out.println("read progress: count=" + countRead.get() + ", rate=" + ((float) countRead.get() / (System.currentTimeMillis() - begin) * 1000f));
              System.out.flush();
//          }
            }
            catch (Exception e) {
              e.printStackTrace();
            }
            return null;
          }));
        }
        return null;
      }));
    }

    for (Future future : futures) {
      future.get();
    }

    Thread.sleep(10000000);
//    final AtomicLong last = new AtomicLong(System.currentTimeMillis());
//    final long begin = System.currentTimeMillis();
//    futures = new ArrayList<>();
//    for (int i = 0; i < 32; i++) {
//      final int currOffset = i;
//      futures.add(executor.submit(new Callable(){
//        @Override
//        public Object call() throws Exception {
//
////          (NativePartitionedTree.this) {
//            for (int j = 0; j < 100; j++) {
//              for (int i = 0; i < 3125000; i += 1000) {
//                long value = get(indices[currOffset], i);
//                if (value != i) {
//                  System.out.println("key mismatch: expected=" + i + ", actual=" + value);
//                }
//                long count = countRead.addAndGet(1000);
//                //(mutex) {
//                  if ( System.currentTimeMillis() - last.get() > 1_000) {
//                    last.set(System.currentTimeMillis());
//                    System.out.println("read progress: count=" + countRead.get() + ", rate=" + ((float) countRead.get() / (System.currentTimeMillis() - begin) * 1000f));
//                    System.out.flush();
//                  }
//                //}
//              }
//            }
//          System.out.println("read progress: count=" + countRead.get() + ", rate=" + ((float) countRead.get() / (System.currentTimeMillis() - begin) * 1000f));
//          System.out.flush();
////          }
//          return null;
//        }
//      }));
//
//    }
//    for (Future future : futures) {
//      future.get();
//    }
    System.out.println("read progress - finished: count=" + countRead.get() + ", rate=" + ((float) countRead.get() / (System.currentTimeMillis() - begin) * 1000f));
    System.out.flush();

    executor.shutdownNow();
    executor2.shutdownNow();
  }

  private long[] parseResultsOld(byte[] results) throws IOException {
    AtomicInteger offset = new AtomicInteger();
    int count = readInt(results, offset);
    long[] ret = new long[count];
    for (int i = 0; i < count; i++) {
      int len = readInt(results, offset);
      long key = readLong(results, offset);
      ret[i] = key;
    }
    return ret;
  }

  public final long readLong(byte[] bytes, AtomicInteger offset) throws IOException {
    long ret = bytesToLong(bytes, offset.get());
    offset.addAndGet(8);
    return ret;
//    long ret = (((long)bytes[offset.get() + 0] << 56) +
//        ((long)(bytes[offset.get() + 1] & 255) << 48) +
//        ((long)(bytes[offset.get() + 2] & 255) << 40) +
//        ((long)(bytes[offset.get() + 3] & 255) << 32) +
//        ((long)(bytes[offset.get() + 4] & 255) << 24) +
//        ((bytes[offset.get() + 5] & 255) << 16) +
//        ((bytes[offset.get() + 6] & 255) <<  8) +
//        ((bytes[offset.get() + 7] & 255) <<  0));
//    offset.addAndGet(8);
//    return ret;
  }

  public final void writeLong(byte[] bytes, long v) {
    bytes[0] = (byte)(v >>> 56);
    bytes[1] = (byte)(v >>> 48);
    bytes[2] = (byte)(v >>> 40);
    bytes[3] = (byte)(v >>> 32);
    bytes[4] = (byte)(v >>> 24);
    bytes[5] = (byte)(v >>> 16);
    bytes[6] = (byte)(v >>>  8);
    bytes[7] = (byte)(v >>>  0);
  }

  public final int readInt(byte[] bytes, AtomicInteger offset) throws IOException {
    int ret = bytesToInt(bytes, offset.get());
    offset.addAndGet(4);
    return ret;
//    int ch1 = bytes[offset.get()];
//    int ch2 = bytes[offset.get() + 1];
//    int ch3 = bytes[offset.get() + 1];
//    int ch4 = bytes[offset.get() + 1];
//    if ((ch1 | ch2 | ch3 | ch4) < 0)
//      throw new EOFException();
//    offset.addAndGet(4);
//    return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
  }

  public static int bytesToInt(byte[] bytes, int offset) {
    return bytes[offset] << 24 |
        (bytes[1 + offset] & 0xFF) << 16 |
        (bytes[2 + offset] & 0xFF) << 8 |
        (bytes[3 + offset] & 0xFF);
  }

  public static long bytesToLong(byte[] b, int offset) {
    long result = 0;
    for (int i = 0; i < 8; i++) {
      result <<= 8;
      result |= (b[i + offset] & 0xFF);
    }
    return result;
  }


}
