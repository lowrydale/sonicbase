package com.sonicbase.client;

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.sonicbase.common.ComArray;
import com.sonicbase.common.ComObject;
import com.sonicbase.common.ServersConfig;
import org.apache.giraph.utils.Varint;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class ClientStatsHandler {
  private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("com.sonicbase.logger");

  private final DatabaseClient client;

  public ClientStatsHandler(DatabaseClient client) {
    this.client = client;
  }

  public void registerCompletedQueryForStats(String dbName, HistogramEntry histogramEntry, long beginMillis, long beginNanos) {
    long latency = System.nanoTime() - beginNanos;
    histogramEntry.histogram.update(latency);
    if (histogramEntry.maxedLatencies.get()) {
      return;
    }
    histogramEntry.latencies.add(latency);
    if (histogramEntry.latencies.size() > 1_000) {
      histogramEntry.maxedLatencies.set(true);
      histogramEntry.latencies.clear();
    }
  }

  public static class QueryStatsRecorder implements Runnable {

    private final String cluster;
    private final DatabaseClient client;

    public QueryStatsRecorder(DatabaseClient client, String cluster) {
      this.client = client;
      this.cluster = cluster;
    }

    @Override
    public void run() {
      while (!client.getShutdown()) {
        try {
          Thread.sleep(15_000);

          boolean oneIsAlive = false;
          DatabaseClient sharedClient = client.getSharedClients().get(cluster);
          ServersConfig.Host[] replicas = sharedClient.getCommon().getServersConfig().getShards()[0].getReplicas();
          for (ServersConfig.Host host : replicas) {
            if (!host.isDead()) {
              oneIsAlive = true;
              break;
            }
          }

          if (!oneIsAlive) {
            sharedClient.syncSchema();
            continue;
          }

          ComObject cobj = new ComObject();
          cobj.put(ComObject.Tag.method, "registerStats");
          ComArray array = cobj.putArray(ComObject.Tag.histogramSnapshot, ComObject.Type.objectType);

          if (registeredQueries.get(cluster) == null) {
            continue;
          }
          for (HistogramEntry entry : registeredQueries.get(cluster).values()) {
            if (entry.histogram == null || entry.histogram.getCount() == 0) {
              continue;
            }

            ComObject snapshotObj = new ComObject();
            snapshotObj.put(ComObject.Tag.dbName, entry.dbName);
            snapshotObj.put(ComObject.Tag.id, entry.queryId);
            if (!entry.maxedLatencies.get()) {
              ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
              DataOutputStream out = new DataOutputStream(bytesOut);
              for (Long latency : entry.latencies) {
                Varint.writeUnsignedVarLong(latency, out);
              }
              snapshotObj.put(ComObject.Tag.latenciesBytes, bytesOut.toByteArray());
              array.add(snapshotObj);
            }
            else {
              Snapshot snapshot = entry.histogram.getSnapshot();
              snapshotObj.put(ComObject.Tag.count, (int) entry.histogram.getCount());
              snapshotObj.put(ComObject.Tag.lat_avg, (double) snapshot.getMean());
              snapshotObj.put(ComObject.Tag.lat_75, (double) snapshot.get75thPercentile());
              snapshotObj.put(ComObject.Tag.lat_95, (double) snapshot.get95thPercentile());
              snapshotObj.put(ComObject.Tag.lat_99, (double) snapshot.get99thPercentile());
              snapshotObj.put(ComObject.Tag.lat_999, (double) snapshot.get999thPercentile());
              snapshotObj.put(ComObject.Tag.lat_max, (double) snapshot.getMax());
              array.add(snapshotObj);
            }

            entry.latencies.clear();
            entry.maxedLatencies.set(false);
            entry.histogram = null;
          }

          byte[] ret = sharedClient.send(null, 0, 0, cobj, DatabaseClient.Replica.def);
        }
        catch (InterruptedException e) {
          break;
        }
        catch (Exception e) {
          logger.error("Error in stats reporting thread", e);
        }
      }
    }
  }

  private class QueryStats {
    private long begin;
    private long duration;
    private int queryId;
  }

  private static ConcurrentHashMap<String, ConcurrentHashMap<String, HistogramEntry>> registeredQueries = new ConcurrentHashMap<>();

  public static class HistogramEntry {
    private Histogram histogram;
    private long queryId;
    public String dbName;
    public String query;
    public AtomicBoolean maxedLatencies = new AtomicBoolean();
    public ConcurrentLinkedQueue<Long> latencies;
  }

  public HistogramEntry registerQueryForStats(String cluster, String dbName, String sql) {
    try {
      ConcurrentHashMap<String, HistogramEntry> clusterEntry = registeredQueries.get(cluster);
      if (clusterEntry == null) {
        clusterEntry = new ConcurrentHashMap<>();
        registeredQueries.put(cluster, clusterEntry);
      }
      HistogramEntry entry = clusterEntry.get(sql);
      if (entry != null) {
        //make a copy because background thread may wipe out the histogram
        HistogramEntry retEntry = new HistogramEntry();
        retEntry.queryId = entry.queryId;
        retEntry.query = entry.query;
        retEntry.dbName = entry.dbName;
        retEntry.histogram = entry.histogram;
        retEntry.latencies = entry.latencies;
        retEntry.maxedLatencies = entry.maxedLatencies;
        if (retEntry.histogram == null) {
          retEntry.histogram = entry.histogram = new Histogram(new ExponentiallyDecayingReservoir());
        }
        return retEntry;
      }

      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.method, "registerQueryForStats");
      cobj.put(ComObject.Tag.dbName, dbName);
      cobj.put(ComObject.Tag.sql, sql);

      DatabaseClient sharedClient = client.getSharedClients().get(cluster);
      byte[] ret = sharedClient.sendToMaster(cobj);
      if (ret != null) {
        ComObject retObj = new ComObject(ret);
        entry = new HistogramEntry();
        HistogramEntry retEntry = new HistogramEntry();
        retEntry.queryId = entry.queryId = retObj.getLong(ComObject.Tag.id);
        retEntry.dbName = entry.dbName = dbName;
        retEntry.histogram = entry.histogram = new Histogram(new ExponentiallyDecayingReservoir());
        retEntry.latencies = entry.latencies = new ConcurrentLinkedQueue<>();
        retEntry.maxedLatencies = entry.maxedLatencies;
        retEntry.query = entry.query = sql;
        clusterEntry.put(sql, entry);
        return retEntry;
      }
    }
    catch (Exception e) {
      logger.error("Error registering completed query", e);
    }
    return null;
  }

}
