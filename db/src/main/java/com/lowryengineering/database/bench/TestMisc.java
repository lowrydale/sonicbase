package com.lowryengineering.database.bench;

import org.testng.annotations.Test;

import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Responsible for
 */
public class TestMisc {

  @Test
  public void test() {
    ConcurrentSkipListMap<Long, Integer> map = new ConcurrentSkipListMap<>();
    map.put(100L, 100);
    map.put(1L, 100);
    map.put(101L, 100);
    Long key = map.floorKey(102L);
    System.out.println(key);
  }
}
