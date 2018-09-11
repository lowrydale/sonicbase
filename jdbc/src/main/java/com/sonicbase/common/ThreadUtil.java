package com.sonicbase.common;

import com.sonicbase.query.DatabaseException;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadUtil {

  private ThreadUtil() {
  }
  public static ThreadPoolExecutor createExecutor(int threadCount, final String threadName) {
    return new ThreadPoolExecutor(threadCount, threadCount, 10000,
        TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1000), r -> new Thread(r, threadName),
        new ThreadPoolExecutor.CallerRunsPolicy());
  }

  public static Thread createThread(Runnable runnable, String name) {
    return new Thread(runnable, name);
  }

  public static void sleep(int millis) {
    try {
      Thread.sleep(millis);
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new DatabaseException(e);
    }
  }
}
