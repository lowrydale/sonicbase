/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.database;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.sonicbase.server.MonitorManager;
import org.testng.annotations.Test;

import static com.codahale.metrics.MetricRegistry.name;

public class TestMetrics {

  final static MetricRegistry METRICS = new MetricRegistry();

  @Test
  public void testSecond() {
    Histogram h = METRICS.histogram(name(MonitorManager.class, "xxx"));
    h.update(100);
    h.update(200);
    h.update(300);
    Snapshot s = h.getSnapshot();
    System.out.println(h.getCount() + ", 95=" + s.get95thPercentile());
    s = h.getSnapshot();
    System.out.println(h.getCount() + ", 95=" + s.get95thPercentile());
  }
  @Test
  public void testAggregation() {
    Histogram histogram1 = METRICS.histogram(name(MonitorManager.class, "query-1"));
    Histogram histogram2 = METRICS.histogram(name(MonitorManager.class, "query-2"));
    Histogram histogramCorrect = METRICS.histogram(name(MonitorManager.class, "query-correct"));
    Histogram histogramAggregate = METRICS.histogram(name(MonitorManager.class, "query-aggregate"));
    int count1 = 0;
    int count2 = 0;

    for (int i = 0; i < 100_000; i++) {
      histogram1.update((long)1_000);
      histogram1.update((long)2_000);

      histogramCorrect.update(1_000);
      histogramCorrect.update(2_000);
      count1 += 2;
    }

    for (int i = 0; i < 50_000; i++) {
      histogram1.update((long)10_000);
      histogram1.update((long)20_000);

      histogramCorrect.update(10_000);
      histogramCorrect.update(20_000);
      count1 += 2;
    }

    for (int i = 0; i < 10_000; i++) {
      histogram1.update((long)90_000);

      histogramCorrect.update(90_000);
      count1 += 1;
    }

    for (int i = 0; i < 1000; i++) {
      histogram1.update((long)100_000);

      histogramCorrect.update(100_000);
      count1 += 1;
    }

    for (int i = 0; i < 50_000; i++) {
      histogram2.update(25000);
      histogram2.update(40000);
      histogramCorrect.update(25000);
      histogramCorrect.update(40000);
      count2 += 2;
    }


    Snapshot snapshot2 = histogram2.getSnapshot();
    Snapshot snapshot1 = histogram1.getSnapshot();
    Snapshot snapshotc = histogramCorrect.getSnapshot();

    double latc_75 = snapshotc.get75thPercentile();
    double latc_95 = snapshotc.get95thPercentile();
    double latc_99 = snapshotc.get99thPercentile();
    double latc_999 = snapshotc.get999thPercentile();

    double lat_avg = snapshot2.getMean();
    double lat_75 = snapshot2.get75thPercentile();
    double lat_95 = snapshot2.get95thPercentile();
    double lat_99 = snapshot2.get99thPercentile();
    double lat_999 = snapshot2.get999thPercentile();
    int totalCount = 0;
    int perc = count2 / (count1 + count2);

//
//    int count = (int) (((1 - 0.999)) * count2);// * count2 / (count1 + count2));
//    for (int i = 0; i < count; i++) {
//      histogram1.update((long)lat_avg);
//    }
//    totalCount += count;
//    count = (int) (((0.999 - 0.99)) * count2);// * count2 / (count1 + count2));
//    for (int i = 0; i < count; i++) {
//      histogram1.update((long)lat_75);
//    }
//    totalCount += count;
//    count = (int) (((0.99 - 0.95)) * count2);// * count2 /  (count1 + count2));
//    for (int i = 0; i < count; i++) {
//      histogram1.update((long)lat_95);
//    }
//    totalCount += count;
//    count = (int) (((0.95 - 0.75)) * count2);// * count2 / (count1 + count2));
//    for (int i = 0; i < count; i++) {
//      histogram1.update((long)lat_99);
//    }
//    totalCount += count;
//    count = (int) ((0.75 - 0.5) * count2);// * count2 / (count1 + count2));
//    for (int i = 0; i < count; i++) {
//      histogram1.update((long)lat_999);
//    }
//
//

    MonitorManager.updateStats(histogramAggregate, null, count1, snapshot1.getMean(), snapshot1.get75thPercentile(),
        snapshot1.get95thPercentile(), snapshot1.get99thPercentile(), snapshot1.get999thPercentile(),
        snapshot1.getMax());
    MonitorManager.updateStats(histogramAggregate, histogram1, count2, snapshot2.getMean(), snapshot2.get75thPercentile(),
        snapshot2.get95thPercentile(), snapshot2.get99thPercentile(), snapshot2.get999thPercentile(),
        snapshot2.getMax());

    System.out.println("totalcount=" + totalCount);
    Snapshot snapshotCorrect = histogramCorrect.getSnapshot();
    Snapshot snapshotAggregate = histogramAggregate.getSnapshot();
    snapshot1 = histogram1.getSnapshot();
    System.out.println("calculated-cnt=" + histogram1.getCount() + ", aggr=" + histogramAggregate.getCount() + ", correct=" + histogramCorrect.getCount());
    System.out.println("calculated-avg=" + snapshot1.getMean() + ", aggr=" + snapshotAggregate.getMean() + ", correct=" + snapshotCorrect.getMean());
    System.out.println("calculated-75=" + snapshot1.get75thPercentile() + ", aggr=" + snapshotAggregate.get75thPercentile() + ", correct=" + snapshotCorrect.get75thPercentile());
    System.out.println("calculated-95=" + snapshot1.get95thPercentile() + ", aggr=" + snapshotAggregate.get95thPercentile() + ", correct=" + snapshotCorrect.get95thPercentile());
    System.out.println("calculated-99=" + snapshot1.get99thPercentile() + ", aggr=" + snapshotAggregate.get99thPercentile() + ", correct=" + snapshotCorrect.get99thPercentile());
    System.out.println("calculated-999=" + snapshot1.get999thPercentile() + ", aggr=" + snapshotAggregate.get999thPercentile() + ", correct=" + snapshotCorrect.get999thPercentile());
    System.out.println("calculated-max=" + snapshot1.getMax() + "aggr=" + snapshotAggregate.getMax() + ", correct=" + snapshotCorrect.getMax());
  }

  @Test
  public void testAggregation2() {
    Histogram histogram1 = METRICS.histogram(name(MonitorManager.class, "query-1"));
    Histogram histogram2 = METRICS.histogram(name(MonitorManager.class, "query-2"));
    Histogram histogramCorrect = METRICS.histogram(name(MonitorManager.class, "query-correct"));
    Histogram histogramAggregate = METRICS.histogram(name(MonitorManager.class, "query-aggregate"));
    int count1 = 0;
    int count2 = 0;

    for (int i = 0; i < 50_000; i++) {
      histogram1.update((long)i);
      histogramCorrect.update(i);
      count1 += 1;
    }
    for (int i = 50_000; i < 100_000; i++) {
      histogram2.update((long)i);
      histogramCorrect.update(i);
      count2 += 1;
    }

    Snapshot snapshot2 = histogram2.getSnapshot();
    Snapshot snapshot1 = histogram1.getSnapshot();
    int totalCount = 0;

    MonitorManager.updateStats(histogramAggregate, null, count1, snapshot1.getMean(), snapshot1.get75thPercentile(),
        snapshot1.get95thPercentile(), snapshot1.get99thPercentile(), snapshot1.get999thPercentile(),
        snapshot1.getMax());
    MonitorManager.updateStats(histogramAggregate, histogram1, count2, snapshot2.getMean(), snapshot2.get75thPercentile(),
        snapshot2.get95thPercentile(), snapshot2.get99thPercentile(), snapshot2.get999thPercentile(),
        snapshot2.getMax());

    System.out.println("totalcount=" + totalCount);
    Snapshot snapshotCorrect = histogramCorrect.getSnapshot();
    Snapshot snapshotAggregate = histogramAggregate.getSnapshot();
    snapshot1 = histogram1.getSnapshot();
    System.out.println("calculated-cnt=" + histogram1.getCount() + ", aggr=" + histogramAggregate.getCount() + ", correct=" + histogramCorrect.getCount());
    System.out.println("calculated-avg=" + snapshot1.getMean() + ", aggr=" + snapshotAggregate.getMean() + ", correct=" + snapshotCorrect.getMean());
    System.out.println("calculated-75=" + snapshot1.get75thPercentile() + ", aggr=" + snapshotAggregate.get75thPercentile() + ", correct=" + snapshotCorrect.get75thPercentile());
    System.out.println("calculated-95=" + snapshot1.get95thPercentile() + ", aggr=" + snapshotAggregate.get95thPercentile() + ", correct=" + snapshotCorrect.get95thPercentile());
    System.out.println("calculated-99=" + snapshot1.get99thPercentile() + ", aggr=" + snapshotAggregate.get99thPercentile() + ", correct=" + snapshotCorrect.get99thPercentile());
    System.out.println("calculated-999=" + snapshot1.get999thPercentile() + ", aggr=" + snapshotAggregate.get999thPercentile() + ", correct=" + snapshotCorrect.get999thPercentile());
    System.out.println("calculated-max=" + snapshot1.getMax() + "aggr=" + snapshotAggregate.getMax() + ", correct=" + snapshotCorrect.getMax());
  }

}
