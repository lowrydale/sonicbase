/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.util;

import org.testng.annotations.Test;

import java.util.concurrent.ConcurrentHashMap;

public class ThreadUtilsTest {

  @Test
  public void test() {

    ConcurrentHashMap<Long, byte[]> tl = new ConcurrentHashMap<>();

    byte[] bytes = tl.computeIfAbsent(Thread.currentThread().getId(), (k) -> new byte[3000]);
  }
}
