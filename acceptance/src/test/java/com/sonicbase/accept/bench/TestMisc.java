package com.sonicbase.accept.bench;

import org.testng.annotations.Test;

import java.util.concurrent.ConcurrentSkipListMap;

public class TestMisc {

  @Test(enabled=false)
  public void test() {
    ConcurrentSkipListMap<Long, Integer> map = new ConcurrentSkipListMap<>();
    map.put(100L, 100);
    map.put(1L, 100);
    map.put(101L, 100);
    Long key = map.floorKey(102L);
    System.out.println(key);
  }
}
