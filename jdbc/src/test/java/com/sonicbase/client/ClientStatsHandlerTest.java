package com.sonicbase.client;

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import com.sonicbase.common.ComObject;
import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.common.ThreadUtil;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertTrue;

public class ClientStatsHandlerTest {

  @Test
  public void test() throws InterruptedException {
    DatabaseClient client = mock(DatabaseClient.class);
    DatabaseCommon common = new DatabaseCommon();
    when(client.getCommon()).thenReturn(common);

    final AtomicBoolean called = new AtomicBoolean();
    when(client.send(eq("MonitorManager:registerStats"), anyInt(), anyInt(), anyObject(), anyObject())).thenAnswer(
        (Answer) invocationOnMock -> {called.set(true); return null;});

    ClientStatsHandler handler = new ClientStatsHandler() {
      public ComObject sendToMasterOnSharedClient(ComObject cobj, DatabaseClient sharedClient) {
        called.set(true);
        ComObject retObj = new ComObject(1);
        retObj.put(ComObject.Tag.ID, 100_000L);
        return retObj;
      }
    };
    ClientStatsHandler.HistogramEntry histogramEntry = new ClientStatsHandler.HistogramEntry();
    ConcurrentLinkedQueue<Long> latencies = new ConcurrentLinkedQueue<>();
    latencies.add(100L);
    latencies.add(200L);
    histogramEntry.setHistogram(new Histogram(new ExponentiallyDecayingReservoir()));
    histogramEntry.setLatencies(latencies);
    histogramEntry.setDbName("test");
    histogramEntry.setQuery("select * from persons");
    Thread statsRecorderThread = ThreadUtil.createThread(new ClientStatsHandler.QueryStatsRecorder(client, 5), "SonicBase Stats Recorder");
    statsRecorderThread.start();
    handler.registerCompletedQueryForStats(histogramEntry, System.nanoTime());
    //Thread.sleep(500);

    handler.registerQueryForStats("test", "select * from persons");
    handler.registerQueryForStats("test", "select * from persons");
    assertTrue(called.get());

    statsRecorderThread.interrupt();
    statsRecorderThread.join();
  }
}
