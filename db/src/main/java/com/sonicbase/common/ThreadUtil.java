/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.common;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadUtil {

  public static ThreadPoolExecutor createExecutor(int threadCount, final String threadName) {
    return new ThreadPoolExecutor(threadCount, threadCount, 10000,
        TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadFactory(){
      @Override
      public Thread newThread(@NotNull Runnable r) {
        return new Thread(r, threadName);
      }
    }, new ThreadPoolExecutor.CallerRunsPolicy());
  }

  public static Thread createThread(Runnable runnable, String name) {
    return new Thread(runnable, name);
  }
}
