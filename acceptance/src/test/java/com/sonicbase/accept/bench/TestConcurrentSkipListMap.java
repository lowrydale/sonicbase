package com.sonicbase.accept.bench;

import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class TestConcurrentSkipListMap {
  public static void main(String[] args) {
    final ConcurrentSkipListMap<Object[], String> map = new ConcurrentSkipListMap<Object[], String>(new Comparator<Object[]>() {
          @Override
          public int compare(Object[] o1, Object[] o2) {
            for (int i = 0; i < o1.length; i++) {
              int value = ((Long)o1[i]).compareTo((Long)o2[i]);//comparators[i].compare(o1[i], o2[i]);
              if (value < 0) {
                return -1;
              }
              if (value > 0)
                return 1;
              }
            }
            return 0;
          }
        });

    final Random rand = new Random(System.currentTimeMillis());

    ThreadPoolExecutor executor = new ThreadPoolExecutor(16, 16, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
    final long begin = System.currentTimeMillis();
    for (int i = 0; i < 1000000000; i++) {

      final int offset = i;
      executor.submit(() -> {
        Object[] key = new Long[]{(long) rand.nextLong()};
        map.get(key);
        map.put(key, "9");
        if (offset % 100000 == 0) {
          System.out.println("count=" + offset + ", rate=" + ((float)offset / (float)(System.currentTimeMillis() - begin) * 1000f));
        }
      });

    }

  }
}
