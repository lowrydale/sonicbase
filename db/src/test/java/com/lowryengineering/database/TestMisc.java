package com.lowryengineering.database;

import org.testng.annotations.Test;

import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListMap;

import static org.testng.Assert.assertEquals;

/**
 * Responsible for
 */
public class TestMisc {

  @Test
  public void testWhite() {
    String str = "s   a b  c ";
    String[] parts = str.split("\\s+");
    System.out.println("test");
  }

  @Test
  public void testMath() {
    double value = 17179869184d;
    value = value / 1024d / 1024d / 1024d;
    System.out.println(value);
  }

  @Test
  public void testIndex() {
    ConcurrentSkipListMap<Object[], Integer> index = new ConcurrentSkipListMap<>(new Comparator<Object[]>() {
      @Override
      public int compare(Object[] o1, Object[] o2) {
        for (int i = 0; i < o1.length; i++) {
           if (o1[i] == null) {
            continue;
          }
           if (o2[i] == null) {
            continue;
          }
          if ((int)o1[i] > (int)o2[i]) {
            return 1;
          }
          if ((int)o1[i] < (int)o2[i]) {
            return -1;
          }
        }
        return 0;
      }
    });

    index.put(new Object[]{1, 100}, 1);
    index.put(new Object[]{1, 101}, 1);
    index.put(new Object[]{1, 102}, 1);
    index.put(new Object[]{1, 103}, 1);
    index.put(new Object[]{1, 104}, 1);
    index.put(new Object[]{1, 105}, 1);

    index.put(new Object[]{2, 100}, 1);
    index.put(new Object[]{2, 101}, 1);
    index.put(new Object[]{2, 102}, 1);
    index.put(new Object[]{2, 103}, 1);
    index.put(new Object[]{2, 104}, 1);
    index.put(new Object[]{2, 105}, 1);

    index.put(new Object[]{3, 100}, 1);
    index.put(new Object[]{3, 101}, 1);
    index.put(new Object[]{3, 102}, 1);
    index.put(new Object[]{3, 103}, 1);
    index.put(new Object[]{3, 104}, 1);
    index.put(new Object[]{3, 105}, 1);

    Object[] key = index.firstKey();
    key = index.higherKey(new Object[]{1, null});
    assertEquals((int)key[0], 2);
    key = index.higherKey(new Object[]{2, null});
    assertEquals((int)key[0], 3);
    key = index.higherKey(new Object[]{3, 100});
    assertEquals((int)key[0], 3);
    assertEquals((int)key[1], 101);
  }
}
