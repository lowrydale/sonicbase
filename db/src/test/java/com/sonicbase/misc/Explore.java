package com.sonicbase.misc;

import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static java.util.concurrent.CompletableFuture.allOf;

public class Explore {

  @Test
  public void testCollections() {
    List<Integer> list = new ArrayList<>();
    for (int i = 0; i < 10_000_000; i++) {
      list.add(i);
    }

    final AtomicInteger count = new AtomicInteger();

    long begin = System.nanoTime();
    list.stream().forEach((Consumer) o -> {
      count.incrementAndGet();
    });
    long end = System.nanoTime();
    System.out.println("duration=" + (end - begin) + ", count=" + count.get());

    begin = System.nanoTime();
    list.parallelStream().forEach((Consumer) o -> {
      count.incrementAndGet();
    });
    end = System.nanoTime();
    System.out.println("duration=" + (end - begin) + ", count=" + count.get());



  }

  @Test
  public void testCompletable() throws ExecutionException, InterruptedException {
    ForkJoinPool pool = new ForkJoinPool(16);
    final AtomicInteger count = new AtomicInteger();
    List<CompletableFuture<Integer>> futures = new ArrayList<>();
    long begin = System.nanoTime();
    for (int i = 0; i < 1_000_000; i++) {
      futures.add(CompletableFuture.supplyAsync(() -> count.incrementAndGet(), pool));
    }

    CompletableFuture<Void> all = allOf(futures.toArray(new CompletableFuture[futures.size()]));
    all.get();
//    for (CompletableFuture<Integer> future : futures) {
//      future.get();
//    }
    long end = System.nanoTime();
    System.out.println("duration=" + (end - begin));

    begin = System.nanoTime();
    List<Integer> batch = new ArrayList<>();
    ThreadPoolExecutor executor = new ThreadPoolExecutor(16, 16, 10_000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
    List<Future> simpleFutures = new ArrayList<>();
    for (int i = 0; i < 1_000_000; i++) {
      batch.add(i);
      if (batch.size() >= 2_000) {
        final List<Integer> finalBatch = batch;
        batch = new ArrayList<>();
        simpleFutures.add(executor.submit((Callable<Object>) () -> {
          for (int i1 : finalBatch) {
            count.incrementAndGet();
          }
          return 1;
        }));

      }
    }
    for (Future future : simpleFutures) {
      future.get();
    }
    end = System.nanoTime();
    System.out.println("duration=" + (end - begin));



    futures = new ArrayList<>();
    begin = System.nanoTime();
    batch = new ArrayList<>();
    for (int i = 0; i < 1_000_000; i++) {
      batch.add(i);
      if (batch.size() >= 2_000) {
        final List<Integer> finalBatch = batch;
        batch = new ArrayList<>();
        futures.add(CompletableFuture.supplyAsync(() -> {
          for (int j : finalBatch) {
            count.incrementAndGet();
          }
          return null;
        }, pool));
      }
    }
    all = allOf(futures.toArray(new CompletableFuture[futures.size()]));
    all.get();
//    for (CompletableFuture<Integer> future : futures) {
//      future.get();
//    }
    end = System.nanoTime();
    System.out.println("duration=" + (end - begin));

  }


}
