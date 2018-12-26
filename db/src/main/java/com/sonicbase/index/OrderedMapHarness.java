/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.index;

import com.sonicbase.query.DatabaseException;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class OrderedMapHarness {

  private static boolean mix;
  private static boolean avl;
  private PartitionedTree avlMap;
  private ConcurrentSkipListMap<Object[], Long> skipMap;
  private int loadThreadCount = 8;
  private int iterThreadCount = 28;
  private static int recordCount = 300_000_000;
  private AtomicInteger maxRecord = new AtomicInteger();

  public OrderedMapHarness() {
    avlMap = new PartitionedTree();
    skipMap = new ConcurrentSkipListMap<>((o1, o2) -> Long.compare((long)o1[0], (long)o2[0]));
    loadThreadCount = mix ? 32 : 32;
    iterThreadCount = mix ? 28 : 32;
    for (int i = 0; i < mutexes.length; i++) {
      mutexes[i] = new Object();
    }
  }

  public static void main(String[] args) throws Exception {
    mix = args[1].equalsIgnoreCase("mix");
    boolean mixg = args[1].equalsIgnoreCase("mixg");
    avl = args[0].equalsIgnoreCase("avl");
    OrderedMapHarness tree = new OrderedMapHarness();
    if (mix || mixg) {
      if (avl) {
        Thread loadThread = new Thread(() -> {
          try {
            tree.load(false, -1);
          }
          catch (InterruptedException e) {
            e.printStackTrace();
          }
        });
        Thread iterThread = new Thread(() -> {
          try {
            Thread.sleep(5_000);
            if (mixg) {
              tree.identityAvl();
            }
            else {
              tree.iterateAvl();
            }
          }
          catch (Exception e) {
            e.printStackTrace();
          }
        });
        loadThread.start();
        iterThread.start();
        loadThread.join();
        iterThread.join();
      }
      else {
        Thread loadThread = new Thread(() -> {
          try {
            tree.load(true, -1);
          }
          catch (InterruptedException e) {
            e.printStackTrace();
          }
        });
        Thread iterThread = new Thread(() -> {
          try {
            Thread.sleep(5_000);
            if (mixg) {
              tree.identitySkip();
            }
            else {
              tree.iterateSkip();
            }
          }
          catch (Exception e) {
            e.printStackTrace();
          }
        });
        loadThread.start();
        iterThread.start();
        loadThread.join();
        iterThread.join();
      }
    }
    else {
      if (args[0].equalsIgnoreCase("avl")) {
        tree.load(false, recordCount);
        tree.iterateAvl();
      }
      else {
        tree.load(true, recordCount);
        tree.iterateSkip();
      }
    }
  }

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

  private void load(boolean isSkip, int max) throws InterruptedException {
    getUnsafe();

    ThreadPoolExecutor executor = new ThreadPoolExecutor(loadThreadCount, loadThreadCount, 10_000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
    try {
      long begin = System.currentTimeMillis();
      final AtomicInteger countSubmitted = new AtomicInteger();
      final AtomicInteger countFinished = new AtomicInteger();
      final AtomicInteger countInserted = new AtomicInteger();
      for (int j = 0; j < loadThreadCount; j++) {
        final int currThread = j;
        countSubmitted.incrementAndGet();
        executor.submit(() -> {
          try {
            for (int i = 0; i < recordCount; i++) {
              if (i % loadThreadCount == currThread) {
                long address = unsafe.allocateMemory(75);
                for (int l = 0; l < 75; l++) {
                  unsafe.putByte(address + l, (byte)0);
                }

                if (isSkip) {
                  Object[] newKey = new Object[1];
                  newKey[0] = (long) i;
                  //newKey[1] = String.valueOf(i);
//                  synchronized (mutexes[(int)(i % mutexes.length)]) {
                    skipMap.put(newKey, (long) address);
//                  }
                }
                else {
                  avlMap.put(new Object[]{(long)i}, (long) address);
                }
                if (!isSkip && !mix) {
                  if (i % 1000 == 0) {
                    try {
                      Thread.sleep(1);
                    }
                    catch (InterruptedException e) {
                      e.printStackTrace();
                    }
                  }
                }
                maxRecord.set(Math.max(i, maxRecord.get()));
                if (countInserted.incrementAndGet() % 1_000_000 == 0) {
                  System.out.println("load progress: count=" + countInserted.get() + "rate=" + String.format("%.2f", (double) countInserted.get() / (double) (System.currentTimeMillis() - begin) * 1000f));
                }
              }
            }
            countFinished.incrementAndGet();
          }
          catch (Exception e) {
            e.printStackTrace();
          }
        });
      }
      while (countSubmitted.get() > countFinished.get()) {
        Thread.sleep(100);
      }
    }
    finally {
      executor.shutdownNow();
    }
  }

  private void loadAvl() {
    long begin = System.currentTimeMillis();
    for (int i = 0; i < recordCount; i++) {
      avlMap.put(new Object[]{i}, (long)i);
      if (i % 1_000_000 == 0) {
        System.out.println("load progress: count=" + i + "rate=" + ((double)i / (double)(System.currentTimeMillis() - begin)*1000f));
      }
    }
  }

  private final Object[] mutexes = new Object[100_000];

  private void identityAvl() throws InterruptedException {
    final long begin = System.currentTimeMillis();
    Thread[] threads = new Thread[iterThreadCount];
    AtomicLong totalCount = new AtomicLong();
    for (int j = 0; j < threads.length; j++) {
      threads[j] = new Thread(() -> {
        for (int i = 0; i < 100_000; i++) {
          long count = 0;
          while (count < maxRecord.get() - 10000) {
            try {
              Long value = avlMap.get(new Object[]{count});
              if (value != count) {
                System.out.println("invalid key: value=" + value + ", count=" + count);
                //return;
              }
              count = value + 1;
              if (totalCount.incrementAndGet() % 100_000 == 0) {
                System.out.println("iter progress: count=" + totalCount.get() + ", rate=" + String.format("%.2f", (double) totalCount.get() / (double) (System.currentTimeMillis() - begin) * 1000f));
              }
            }
            catch (Exception e) {
              e.printStackTrace();
              break;
            }
          }
        }
      });
      threads[j].start();
    }
    for (int j = 0; j < threads.length; j++) {
      threads[j].join();
    }
  }

  private void iterateAvl() throws Exception {
    final long begin = System.currentTimeMillis();
    Thread[] threads = new Thread[iterThreadCount];
    AtomicLong totalCount = new AtomicLong();
    for (int j = 0; j < threads.length; j++) {
      threads[j] = new Thread(() -> {
        for (int i = 0; i < 100_000; i++) {
          long count = 0;
          Object[] currKey = new Object[]{0L};
          boolean first = true;
          while (count < maxRecord.get() - 10000) {
            try {
              AtomicReference lastKey = new AtomicReference<>();
              final Object[][] keys = new Object[PartitionedTree.BLOCK_SIZE][];
              final long[] values = new long[PartitionedTree.BLOCK_SIZE];
              avlMap.tailBlock(currKey, lastKey, keys, values, PartitionedTree.BLOCK_SIZE, first);
              first = false;
              currKey = (Object[])lastKey.get();
              for (int k = 0; k < keys.length; k++) {
                final int offset = k;
                Map.Entry<Object[], Long> entry = new Map.Entry<Object[], Long>() {
                  @Override
                  public Object[] getKey() {
                    return keys[offset];
                  }

                  @Override
                  public Long getValue() {
                    return values[offset];
                  }

                  @Override
                  public Long setValue(Long value) {
                    return null;
                  }

                  @Override
                  public boolean equals(Object o) {
                    return false;
                  }

                  @Override
                  public int hashCode() {
                    return 0;
                  }
                };

                if (entry == null) {
                  break;
                }
                if ((long)entry.getKey()[0] != count) {
                  System.out.println("invalid key: key=" + entry.getKey() + ", count=" + count);
                }
                count = (long)entry.getKey()[0] + 1;
                if (totalCount.incrementAndGet() % 100_000 == 0) {
                  System.out.println("iter progress: count=" + totalCount.get() + ", rate=" + String.format("%.2f", (double) totalCount.get() / (double) (System.currentTimeMillis() - begin) * 1000f));
                }
              }
            }
            catch (Exception e) {
              e.printStackTrace();
              break;
            }
          }
        }
      });
      threads[j].start();
    }
    for (int j = 0; j < threads.length; j++) {
      threads[j].join();
    }
  }

  private void identitySkip() throws Exception {
    final long begin = System.currentTimeMillis();
    Thread[] threads = new Thread[iterThreadCount];
    AtomicLong totalCount = new AtomicLong();
    for (int j = 0; j < threads.length; j++) {
      threads[j] = new Thread(() -> {
        for (int i = 0; i < 100_000; i++) {
          long count = 0;
          while (count < maxRecord.get() - 10000) {
            Object[] newKey = new Object[2];
            newKey[0] = (long)count;
            newKey[1] = String.valueOf(count);
            Long value = skipMap.get(newKey);
            if (value != count) {
              System.out.println("invalid key: value=" + value + ", count=" + count);
              //return;
            }
            count = value + 1;
            if (totalCount.incrementAndGet() % 1_000_000 == 0) {
              System.out.println("iter progress: count=" + totalCount.get() + ", rate=" + String.format("%.2f", (double) totalCount.get() / (double) (System.currentTimeMillis() - begin) * 1000f));
            }
          }
        }
      });
      threads[j].start();
    }
    for (int j = 0; j < threads.length; j++) {
      threads[j].join();
    }
  }

  private void iterateSkip() throws Exception {
    final long begin = System.currentTimeMillis();
    Thread[] threads = new Thread[iterThreadCount];
    AtomicLong totalCount = new AtomicLong();
    for (int j = 0; j < threads.length; j++) {
      threads[j] = new Thread(() -> {
        for (int i = 0; i < 100_000; i++) {
          long count = 0;
          Object[] newKey = new Object[1];
          newKey[0] = (long)0;
          //newKey[1] = String.valueOf(count);
          ConcurrentNavigableMap<Object[], Long> map = skipMap.tailMap(newKey);
          Iterator<Map.Entry<Object[], Long>> iter = map.entrySet().iterator();
          while (count < maxRecord.get() - 100000) {
            Map.Entry<Object[], Long> entry = iter.next();

//            synchronized (mutexes[(int)((long)entry.getKey()[0] % mutexes.length)]) {
//              skipMap.get(entry.getKey());
              long address = entry.getValue();
              byte[] bytes = new byte[75];
              for (int k = 0; k < 75; k++) {
                bytes[i] = unsafe.getByte(address + k);
              }
//            }


//            if ((long)entry.getKey()[0] != count) {
//              System.out.println("invalid key: key=" + entry.getKey()[0] + ", count=" + count);
//              //return;
//            }
            count = (long)entry.getKey()[0] + 1;
            if (totalCount.incrementAndGet() % 1_000_000 == 0) {
              System.out.println("iter progress: count=" + totalCount.get() + ", rate=" + String.format("%.2f", (double) totalCount.get() / (double) (System.currentTimeMillis() - begin) * 1000f));
            }
          }
        }
      });
      threads[j].start();
    }
    for (int j = 0; j < threads.length; j++) {
      threads[j].join();
    }
  }

}
