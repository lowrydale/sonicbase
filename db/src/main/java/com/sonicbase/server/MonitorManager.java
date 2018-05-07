/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.server;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformReservoir;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.sonicbase.common.*;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.jdbcdriver.StatementProxy;
import com.sonicbase.query.DatabaseException;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.giraph.utils.Varint;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.sonicbase.server.DatabaseServer.SONICBASE_SYS_DB_STR;

public class MonitorManager {
  private static final int MAX_QUERY_COUNT = 10_000;
  public static final String DAY_FORMAT_STR = "yyyy-MM-dd";

  private final DatabaseServer server;
  private final Logger logger;
  private Connection conn;
  private AtomicBoolean initialized = new AtomicBoolean();
  final static  MetricRegistry METRICS = new MetricRegistry();
  private String currDay = "";
  private Cache<Long, HistogramEntry> cache = CacheBuilder.newBuilder().maximumSize(MAX_QUERY_COUNT).build();

  public static final com.codahale.metrics.Timer INDEX_LOOKUP_STATS = METRICS.timer("indexLookup");
  private Thread persisterThread;
  private boolean shutdown;


  public MonitorManager(DatabaseServer databaseServer) {
    this.server = databaseServer;
    this.logger = new Logger(null/*databaseServer.getDatabaseClient()*/);
  }

  public void startMasterMonitor() {
    shutdown = false;
    if (persisterThread != null) {
      persisterThread.interrupt();
      try {
        persisterThread.join();
      }
      catch (InterruptedException e) {
        throw new DatabaseException(e);
      }
    }

    cache = CacheBuilder.newBuilder().maximumSize(MAX_QUERY_COUNT).build();

    Date date = new Date(System.currentTimeMillis());
    currDay = new SimpleDateFormat(DAY_FORMAT_STR).format(date);

    persisterThread = ThreadUtil.createThread(new StatsPersister(), "SonicBase Stats Persister");
    persisterThread.start();
  }

  public void initMonitoringTables() {
    if (initialized.get()) {
      return;
    }
    Connection connection = server.getSysConnection();
    synchronized (this) {
      if (conn == null) {
        conn = connection;
      }
      else {
        return;
      }
      PreparedStatement stmt = null;
      try {
        try {
          if (!((ConnectionProxy) conn).databaseExists(SONICBASE_SYS_DB_STR)) {
            ((ConnectionProxy) conn).createDatabase(SONICBASE_SYS_DB_STR);
          }
        }
        catch (Exception e) {
          if (!ExceptionUtils.getFullStackTrace(e).toLowerCase().contains("database already exists")) {
            throw new DatabaseException(e);
          }
        }
        ((ConnectionProxy)conn).getDatabaseClient().syncSchema();
        if (null == ((ConnectionProxy)conn).getDatabaseClient().getCommon().getTables("_sonicbase_sys").get("query_ids")) {
          stmt = conn.prepareStatement("create table query_ids(db_name VARCHAR, query VARCHAR, id BIGINT, PRIMARY KEY (db_name, query))");
          ((StatementProxy) stmt).disableStats();
          stmt.executeUpdate();
          stmt.close();
        }
        stmt = conn.prepareStatement("describe table query_ids");
        ((StatementProxy) stmt).disableStats();
        stmt.executeQuery();
        stmt.close();

        StringBuilder builder = new StringBuilder();
        if (null == ((ConnectionProxy)conn).getDatabaseClient().getCommon().getTables("_sonicbase_sys").get("query_stats")) {
          stmt = conn.prepareStatement("create table query_stats(db_name VARCHAR, id BIGINT, date_val VARCHAR, date_modified BIGINT, query VARCHAR, cnt BIGINT, lat_avg DOUBLE, lat_75 DOUBLE, lat_95 DOUBLE, lat_99 DOUBLE, lat_999 DOUBLE, lat_max DOUBLE, PRIMARY KEY (id, date_val))");
          ((StatementProxy) stmt).disableStats();
          stmt.executeUpdate();
          stmt.close();
        }
        stmt = conn.prepareStatement("describe table query_stats");
        ((StatementProxy) stmt).disableStats();
        ResultSet rs = stmt.executeQuery();
        while (rs.next()) {
          builder.append(rs.getString(1) + "\n");
        }
        rs.close();
        stmt.close();

        String tableDescription = builder.toString();
        if (!tableDescription.contains("Index=_1_date_updated")) {
          stmt = conn.prepareStatement("create index date_updated on query_stats(date_modified)");
          ((StatementProxy) stmt).disableStats();
          stmt.executeUpdate();
          stmt.close();
        }
        if (!tableDescription.contains("Index=_1_cnt")) {
          stmt = conn.prepareStatement("create index cnt on query_stats(cnt)");
          ((StatementProxy) stmt).disableStats();
          stmt.executeUpdate();
          stmt.close();
        }
        if (!tableDescription.contains("Index=_1_lat_avg")) {
          stmt = conn.prepareStatement("create index lat_avg on query_stats(lat_avg)");
          ((StatementProxy) stmt).disableStats();
          stmt.executeUpdate();
          stmt.close();
        }
        if (!tableDescription.contains("Index=_1_lat_75")) {
          stmt = conn.prepareStatement("create index lat_75 on query_stats(lat_75)");
          ((StatementProxy) stmt).disableStats();
          stmt.executeUpdate();
          stmt.close();
        }
        if (!tableDescription.contains("Index=_1_lat_95")) {
          stmt = conn.prepareStatement("create index lat_95 on query_stats(lat_95)");
          ((StatementProxy) stmt).disableStats();
          stmt.executeUpdate();
          stmt.close();
        }
        if (!tableDescription.contains("Index=_1_lat_99")) {
          stmt = conn.prepareStatement("create index lat_99 on query_stats(lat_99)");
          ((StatementProxy) stmt).disableStats();
          stmt.executeUpdate();
          stmt.close();
        }
        if (!tableDescription.contains("Index=_1_lat_999")) {
          stmt = conn.prepareStatement("create index lat_999 on query_stats(lat_999)");
          ((StatementProxy) stmt).disableStats();
          stmt.executeUpdate();
          stmt.close();
        }
        if (!tableDescription.contains("Index=_1_lat_max")) {
          stmt = conn.prepareStatement("create index lat_max on query_stats(lat_max)");
          ((StatementProxy) stmt).disableStats();
          stmt.executeUpdate();
          stmt.close();
        }
      }
      catch (Exception e) {
        if (!ExceptionUtils.getFullStackTrace(e).toLowerCase().contains("table already exists")) {
          throw new DatabaseException(e);
        }
      }
      finally {
        initialized.set(true);
        if (stmt != null) {
          try {
            stmt.close();
          }
          catch (SQLException e) {
            throw new DatabaseException(e);
          }
        }
      }
    }
  }


  public ComObject registerQueryForStats(ComObject cobj, boolean replayedCommand) {
    try {
      initMonitoringTables();
    }
    catch (Exception e) {
      logger.error("Error initializing query stats tables: " + e.getMessage());
      return null;
    }

    try {
      String dbName = cobj.getString(ComObject.Tag.dbName);
      String sql = cobj.getString(ComObject.Tag.sql);

      long retId = -1;

      PreparedStatement stmt = conn.prepareStatement("select * from query_ids where query=? and db_name=?");
      stmt.setString(1, sql);
      stmt.setString(2, dbName);
      ((StatementProxy)stmt).disableStats();
      ResultSet rs = stmt.executeQuery();
      if (rs.next()) {
        retId = rs.getLong("id");
      }
      else {
        try {
          retId = server.getClient().allocateId(SONICBASE_SYS_DB_STR);
          stmt = conn.prepareStatement("insert into query_ids (db_name, query, id) values (?, ?, ?)");
          stmt.setString(1, dbName);
          stmt.setString(2, sql);
          stmt.setLong(3, retId);
          ((StatementProxy)stmt).disableStats();
          int count = stmt.executeUpdate();
          if (count == 0) {
            retId = -1;
          }
        }
        catch (Exception e) {
          stmt = conn.prepareStatement("select * from query_ids where query=? and db_name=?");
          stmt.setString(1, sql);
          stmt.setString(2, dbName);
          ((StatementProxy)stmt).disableStats();
          rs = stmt.executeQuery();
          if (rs.next()) {
            retId = rs.getLong("id");
          }
        }
      }
      queryIdToStr.put(retId, sql);
      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.id, retId);
      return retObj;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private ConcurrentHashMap<Long, String> queryIdToStr = new ConcurrentHashMap<>();

  public void shutdown() {
    this.shutdown = true;
    if (persisterThread != null) {
      persisterThread.interrupt();
      try {
        persisterThread.join();
      }
      catch (InterruptedException e) {
        throw new DatabaseException(e);
      }
    }
  }

  class HistogramEntry {
    private String dbName;
    private Histogram histogram;
    private long lastCount;
    private AtomicLong totalCount = new AtomicLong();

    public HistogramEntry(String dbName, Histogram histogram) {
      this.dbName = dbName;
      this.histogram = histogram;
    }

    public HistogramEntry() {
    }
  }

  class StatsPersister implements Runnable {
    public void run() {
      while (!shutdown) {
        try {
          Thread.sleep(10_000);

          initMonitoringTables();
          break;
        }
        catch (InterruptedException e) {
          break;
        }
        catch (Exception e) {
          logger.error("Error initializing query stats tables" + e.getMessage());
        }
      }
      outer:
      while (!shutdown) {
        try {
          ServersConfig.Shard[] shards = server.getCommon().getServersConfig().getShards();
          for (ServersConfig.Shard shard : shards) {
            boolean haveReplica = false;
            for (ServersConfig.Host host : shard.getReplicas()) {
              if (!host.isDead()) {
                haveReplica = true;
                break;
              }
            }
            if (!haveReplica) {
              Thread.sleep(10_000);
              continue outer;
            }
          }

          Thread.sleep(20_000);
          for (Map.Entry<Long, HistogramEntry> entry : cache.asMap().entrySet()) {
            if (shutdown) {
              break;
            }
            try {
              if (entry.getValue().lastCount == entry.getValue().histogram.getCount()) {
                continue;
              }
              entry.getValue().lastCount = entry.getValue().histogram.getCount();

              String query = queryIdToStr.get(entry.getKey());
              if (query == null) {
                PreparedStatement stmt = conn.prepareStatement("select * from query_ids where id=? and db_name=?");
                stmt.setLong(1, entry.getKey());
                stmt.setString(2, entry.getValue().dbName);
                ((StatementProxy) stmt).disableStats();
                ResultSet rs = stmt.executeQuery();
                if (rs.next()) {
                  query = rs.getString("query");
                  queryIdToStr.put(entry.getKey(), query);
                }
              }

              Histogram histogram = entry.getValue().histogram;
              AtomicLong totalCount = entry.getValue().totalCount;
              Snapshot snapshot = histogram.getSnapshot();

              long id = entry.getKey();
              long count = totalCount.get();
              double lat_mean = snapshot.getMean();
              double lat_75 = snapshot.get75thPercentile();
              if (lat_75 < lat_mean) {
                lat_75 = lat_mean;
              }
              double lat_95 = snapshot.get95thPercentile();
              if (lat_95 < lat_75) {
                lat_95 = lat_75;
              }
              double lat_99 = snapshot.get99thPercentile();
              if (lat_99 < lat_95) {
                lat_99 = lat_95;
              }
              double lat_999 = snapshot.get999thPercentile();
              if (lat_999 < lat_99) {
                lat_999 = lat_99;
              }
              double lat_max = snapshot.getMax();
              if (lat_max < lat_999) {
                lat_max = lat_999;
              }

              Date date = new Date(System.currentTimeMillis());
              SimpleDateFormat df = new SimpleDateFormat(DAY_FORMAT_STR);
              df.setTimeZone(TimeZone.getTimeZone("UTC"));
              String day = df.format(date);

              PreparedStatement stmt = conn.prepareStatement("select * from query_stats where id=? and date_val=?");
              stmt.setLong(1, entry.getKey());
              stmt.setString(2, day);
              ResultSet rs = stmt.executeQuery();
              updateStats(entry, query, id, count, lat_mean, lat_75, lat_95, lat_99, lat_999, lat_max, day, rs);
            }
            catch (Exception e) {
              logger.error("Error persisting stats", e);
            }
          }
          evictOldQueries();
        }
        catch (InterruptedException e) {
          break;
        }
        catch (Exception e) {
          logger.error("Error in stats persister thread", e);
        }
      }
    }
  }

  private void updateStats(Map.Entry<Long, HistogramEntry> entry, String query, long id, long count, double lat_mean, double lat_75, double lat_95, double lat_99, double lat_999, double lat_max, String day, ResultSet rs) throws SQLException {
    PreparedStatement stmt;
    if (rs.next()) {
      stmt = conn.prepareStatement("update query_stats set db_name=?, id=?, date_val=?, date_modified=?, query=?, cnt=?, lat_avg=?, lat_75=?, lat_95=?, lat_99=?, lat_999=?, lat_max=? where id=?");
      stmt.setString(1, entry.getValue().dbName);
      stmt.setLong(2, id);
      stmt.setString(3, day);
      stmt.setLong(4, System.currentTimeMillis());
      stmt.setString(5, query);
      stmt.setLong(6, count);
      stmt.setDouble(7, lat_mean);
      stmt.setDouble(8, lat_75);
      stmt.setDouble(9, lat_95);
      stmt.setDouble(10, lat_99);
      stmt.setDouble(11, lat_999);
      stmt.setDouble(12, lat_max);
      stmt.setLong(13, id);
      ((StatementProxy) stmt).disableStats();
      stmt.executeUpdate();
    }
    else {
      stmt = conn.prepareStatement("insert into query_stats (db_name, id, date_val, date_modified, query, cnt, lat_avg, lat_75, lat_95, lat_99, lat_999, lat_max) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
      stmt.setString(1, entry.getValue().dbName);
      stmt.setLong(2, id);
      stmt.setString(3, day);
      stmt.setLong(4, System.currentTimeMillis());
      stmt.setString(5, query);
      stmt.setLong(6, count);
      stmt.setDouble(7, lat_mean);
      stmt.setDouble(8, lat_75);
      stmt.setDouble(9, lat_95);
      stmt.setDouble(10, lat_99);
      stmt.setDouble(11, lat_999);
      stmt.setDouble(12, lat_max);
      ((StatementProxy) stmt).disableStats();
      stmt.executeUpdate();
    }
  }

  private void evictOldQueries() {
    try {
      PreparedStatement stmt = conn.prepareStatement("select count(*) from query_stats");
      ResultSet rs = stmt.executeQuery();
      rs.next();
      long count = rs.getLong(1);
      int countToRemove = (int) (count - MAX_QUERY_COUNT);
      if (countToRemove > 0) {
        logger.info("deleting queries from query_stats: count=" + countToRemove);
        stmt = conn.prepareStatement("select * from query_stats order by date_modified asc");
        rs = stmt.executeQuery();
        for (int i = 0; i < countToRemove && rs.next(); i++) {
          PreparedStatement delStmt = conn.prepareStatement("delete from query_stats where id=?");
          delStmt.setLong(1, rs.getLong("id"));
          int countDeleted = delStmt.executeUpdate();
          if (countDeleted != 1) {
            logger.error("Error deleting query from query_stats");
          }
          delStmt = conn.prepareStatement("delete from query_ids where id=?");
          delStmt.setLong(1, rs.getLong("id"));
          countDeleted = delStmt.executeUpdate();
          if (countDeleted != 1) {
            logger.error("Error deleting query from query_ids");
          }
        }
      }

      Date referenceDate = new Date(System.currentTimeMillis());
      Calendar c = Calendar.getInstance();
      c.setTime(referenceDate);
      c.add(Calendar.MONTH, -1);

      String dateStr = new SimpleDateFormat(DAY_FORMAT_STR).format(new Date(c.getTimeInMillis()));


      PreparedStatement delStmt = conn.prepareStatement("delete from query_stats where date_val<?");
      delStmt.setString(1, dateStr);
      delStmt.executeUpdate();
    }
    catch (Exception e) {
      logger.error("Error evicting old queries", e);
    }
  }

  public ComObject registerStats(ComObject cobj, boolean replayedCommand) {

    if (replayedCommand) {
      return null;
    }

//    try {
//      initMonitoringTables();
//    }
//    catch (Exception e) {
//      logger.error("Error initializing query stats tables");
//      return null;
//    }

    try {

      Date date = new Date(System.currentTimeMillis());
      String day = new SimpleDateFormat(DAY_FORMAT_STR).format(date);

      if (!day.equals(currDay)) {
        cache = CacheBuilder.newBuilder().maximumSize(MAX_QUERY_COUNT).build();
        currDay = day;
      }

      ComArray array = cobj.getArray(ComObject.Tag.histogramSnapshot);
      for (int i = 0; i < array.getArray().size(); i++) {
        ComObject snapshotObj = (ComObject) array.getArray().get(i);
        String dbName = snapshotObj.getString(ComObject.Tag.dbName);
        long id = snapshotObj.getLong(ComObject.Tag.id);

        Histogram histogram = null;

        AtomicLong totalCount;
        HistogramEntry histogramEntry = cache.getIfPresent(id);
        if (histogramEntry == null) {
          histogramEntry = new HistogramEntry();
          histogramEntry.dbName = dbName;

          date = new Date(System.currentTimeMillis());
          String dateStr = new SimpleDateFormat(DAY_FORMAT_STR).format(date);

          histogram = histogramEntry.histogram = new Histogram(new UniformReservoir());
          totalCount = histogramEntry.totalCount;

          try (PreparedStatement stmt = conn.prepareStatement("select * from query_stats where id=? and date_val=?")) {
            stmt.setLong(1, id);
            stmt.setString(2, dateStr);
            try (ResultSet rs = stmt.executeQuery()) {
              if (rs.next()) {
                long count = rs.getLong("cnt");
                double lat_avg = rs.getDouble("lat_avg");
                double lat_75 = rs.getDouble("lat_75");
                double lat_95 = rs.getDouble("lat_95");
                double lat_99 = rs.getDouble("lat_99");
                double lat_999 = rs.getDouble("lat_999");
                double lat_max = rs.getDouble("lat_max");

                updateStats(totalCount, histogram, null, count, lat_avg, lat_75, lat_95, lat_99, lat_999, lat_max);
              }
            }
          }
          catch (Exception e) {
            logger.error("Error getting query_stats: " + e.getMessage());
          }
          cache.put(id, histogramEntry);
        }
        else {
          histogram = histogramEntry.histogram;
          totalCount = histogramEntry.totalCount;
        }

        byte[] latenciesBytes = snapshotObj.getByteArray(ComObject.Tag.latenciesBytes);
        if (latenciesBytes != null) {
          try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(latenciesBytes));
            while (true) {
              long latency = Varint.readUnsignedVarLong(in);
              histogram.update(latency);
              totalCount.incrementAndGet();
            }
          }
          catch (EOFException e) {
            //expected
          }
        }
        else {
          updateStats(totalCount, histogram, null,
              snapshotObj.getInt(ComObject.Tag.count),
              snapshotObj.getDouble(ComObject.Tag.lat_avg),
              snapshotObj.getDouble(ComObject.Tag.lat_75),
              snapshotObj.getDouble(ComObject.Tag.lat_95),
              snapshotObj.getDouble(ComObject.Tag.lat_99),
              snapshotObj.getDouble(ComObject.Tag.lat_999),
              snapshotObj.getDouble(ComObject.Tag.lat_max));
        }
      }
      return null;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public static void updateStats(AtomicLong totalCountRet, Histogram histogram, Histogram histogram2, double countRegistered,
                                 double lat_avg, double lat_75, double lat_95, double lat_99, double lat_999, double lat_max) {
    int totalCount = 0;
    int count = 0;

    totalCountRet.addAndGet((long)countRegistered);

    if (countRegistered >= 1000) {
      countRegistered /= 100;
    }

    double updateCount = countRegistered;//count * ((count + countRegistered) / countRegistered);
    count += (int) (((0.5)) * updateCount);// * count2 / (count1 + count2));
    count += (int) (((0.75 - 0.5)) * updateCount);// * count2 / (count1 + count2));
    count += (int) (((0.95 - 0.75)) * updateCount);// * count2 / (count1 + count2));
    count += (int) (((0.99 - 0.95)) * updateCount);// * count2 /  (count1 + count2));
    count += (int) (((0.999 - 0.99)) * updateCount);// * count2 / (count1 + count2));
    count += (int) ((1 - 0.999) * updateCount);// * count2 / (count1 + count2));

    totalCount = count;

    count = (int) (((0.5)) * updateCount);// * count2 / (count1 + count2));
    count -= totalCount - countRegistered;
    count -= 1; //for max
    for (int i = 0; i < count; i++) {
      histogram.update((long)(lat_avg * 0.50d));
      if (histogram2 != null) {
        histogram2.update((long)(lat_avg * 0.50d));
      }
    }
    count = (int) (((0.75 - 0.5)) * updateCount);// * count2 / (count1 + count2));
    for (int i = 0; i < count; i++) {
      histogram.update((long)(lat_75 * .75d));
      if (histogram2 != null) {
        histogram2.update((long)(lat_75 * .75d));
      }
    }
    count = (int) (((0.95 - 0.75)) * updateCount);// * count2 / (count1 + count2));
    for (int i = 0; i < count; i++) {
      histogram.update((long)(lat_95 * 0.95d));
      if (histogram2 != null) {
        histogram2.update((long)(lat_95 * 0.95d));
      }
    }
    count = (int) (((0.99 - 0.95)) * updateCount);// * count2 /  (count1 + count2));
    for (int i = 0; i < count; i++) {
      histogram.update((long)(lat_99 * 0.99d));
      if (histogram2 != null) {
        histogram2.update((long)lat_99);
      }
    }
    count = (int) (((0.999 - 0.99)) * updateCount);// * count2 / (count1 + count2));
    for (int i = 0; i < count; i++) {
      histogram.update((long)(lat_999 * 0.999d));
      if (histogram2 != null) {
        histogram2.update((long)(lat_999 * 0.999d));
      }
    }
    count = (int) ((1 - 0.999) * updateCount);// * count2 / (count1 + count2));
    for (int i = 0; i < count; i++) {
      histogram.update((long)lat_999);
      if (histogram2 != null) {
        histogram2.update((long)lat_999);
      }
    }
//    for (int i = 0; i < countRegistered - totalCount -  1; i++) {
//      histogram.update((long)lat_max);
//      if (histogram2 != null) {
//        histogram2.update((long)lat_max);
//      }
//    }
    histogram.update((long)lat_max);
    if (histogram2 != null) {
      histogram2.update((long)lat_max);
    }
    totalCount += count;
    System.out.println("totalcount=" + totalCount);
  }

}
