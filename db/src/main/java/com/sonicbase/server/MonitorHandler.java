/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.server;

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.InsufficientLicense;
import com.sonicbase.common.ServersConfig;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.query.DatabaseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static com.sonicbase.server.HttpServer.DAY_FORMAT_STR;
import static com.sonicbase.server.HttpServer.TIME_FORMAT_STR;
import static java.util.Calendar.*;
import static java.util.Calendar.DAY_OF_MONTH;
import static java.util.Calendar.MONTH;

public class MonitorHandler extends AbstractHandler {

  private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("com.sonicbase.logger");
  private final DatabaseServer server;
  private static ConcurrentHashMap<String, Connection> connections = new ConcurrentHashMap<>();


  public MonitorHandler(DatabaseServer server) {
    this.server = server;
  }

  public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    //new Exception().printStackTrace();
    String uri = request.getRequestURI();
    logger.info("http request: uri=" + uri + "?" + request.getQueryString());
    InputStream in = HttpServer.class.getResourceAsStream(uri);
    try {
      if (uri.equals("") || uri.equals("/") || uri.endsWith("index.html")) {
        response.setContentType("text/html;charset=utf-8");
        in = HttpServer.class.getResourceAsStream("/health.html");
        String data = IOUtils.toString(in, "utf-8");
        IOUtils.write(data, response.getOutputStream(), "utf-8");
        response.setStatus(HttpServletResponse.SC_OK);
        response.getOutputStream().flush();
        return;
      }
      if (in != null) {
        if (uri.endsWith(".html") || uri.endsWith(".js")) {
          response.setContentType("text/html;charset=utf-8");
          String data = IOUtils.toString(in, "utf-8");
          IOUtils.write(data, response.getOutputStream(), "utf-8");
        }
        else if (uri.endsWith(".png")) {
          response.setContentType("image/png");
          IOUtils.write(IOUtils.toByteArray(in), response.getOutputStream());
        }
        else if (uri.endsWith(".jpeg")) {
          response.setContentType("image/jpeg");
          IOUtils.write(IOUtils.toByteArray(in), response.getOutputStream());
        }
        else if (uri.endsWith(".gif")) {
          response.setContentType("image/gif");
          IOUtils.write(IOUtils.toByteArray(in), response.getOutputStream());
        }
        else if (uri.endsWith(".css")) {
          response.setContentType("text/css;charset=utf-8");
          IOUtils.write(IOUtils.toByteArray(in), response.getOutputStream());
        }
        response.setStatus(HttpServletResponse.SC_OK);
        response.getOutputStream().flush();
        return;
      }

      if (uri.startsWith("/query-stats")) {
        String cluster = request.getParameter("cluster");
        String orderBy = request.getParameter("order_by");
        String pageSize = request.getParameter("page_size");
        String offset = request.getParameter("offset");
        String asc = request.getParameter("asc");
        String date = request.getParameter("date");
        String currentDate = request.getParameter("currDate");
        String timezone = request.getParameter("tz");
        String searchStr = request.getParameter("search");
        String data = getQueryStats(cluster, offset, pageSize, orderBy, asc, date, currentDate, timezone, searchStr);
        response.setContentType("application/json; charset=utf-8");
        response.setStatus(HttpServletResponse.SC_OK);
        IOUtils.write(data, response.getOutputStream(), "utf-8");
        response.getOutputStream().flush();
      }
      else if (uri.startsWith("/os-stats")) {
        String cluster = request.getParameter("cluster");
        String time = request.getParameter("time");
        String timezone = request.getParameter("tz");
        String data = getOSStats(cluster, time, timezone);
        response.setContentType("application/json; charset=utf-8");
        response.setStatus(HttpServletResponse.SC_OK);
        IOUtils.write(data, response.getOutputStream(), "utf-8");
        response.getOutputStream().flush();
      }
      else if (uri.startsWith("/health")) {
        String cluster = request.getParameter("cluster");
        String data = getServerHealth(cluster);
        response.setContentType("application/json; charset=utf-8");
        response.setStatus(HttpServletResponse.SC_OK);
        IOUtils.write(data, response.getOutputStream(), "utf-8");
        response.getOutputStream().flush();
      }
      else if (uri.startsWith("/get-date")) {
        TimeZone utcTimezone = TimeZone.getTimeZone("UTC");
        SimpleDateFormat utfDf = new SimpleDateFormat(DAY_FORMAT_STR);
        utfDf.setTimeZone(utcTimezone);
        String date = utfDf.format(new Date(System.currentTimeMillis()));//new Date(now.getTimeInMillis()));
        response.setStatus(HttpServletResponse.SC_OK);
        IOUtils.write("{\"date\": \"" + date + "\"}", response.getOutputStream(), "utf-8");
        response.getOutputStream().flush();
      }
    }
    catch (InsufficientLicense e) {
      response.setContentType("application/json; charset=utf-8");
      String data = "{\"error\": \"insuficient license\"}";
      response.setStatus(HttpServletResponse.SC_OK);
      IOUtils.write(data, response.getOutputStream(), "utf-8");
      response.getOutputStream().flush();
    }
    catch (Exception e) {
      e.printStackTrace();
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      IOUtils.write(ExceptionUtils.getStackTrace(e), response.getOutputStream(), "utf-8");

    }
  }

  private static void checkLicense(ConnectionProxy conn) {
    if (!conn.getDatabaseClient().getCommon().haveProLicense()) {
      conn.getDatabaseClient().syncSchema();
      if (!conn.getDatabaseClient().getCommon().haveProLicense()) {
        throw new InsufficientLicense("You must have a pro license to use monitor feature");
      }
    }
  }

  private static ArrayNode getClusters(ObjectNode node) {
    ArrayNode clusters = node.putArray("clusters");

    File binDir = new File(System.getProperty("user.dir"));

    File dir = new File(binDir, "config");
    if (!dir.exists()) {
      dir = new File(binDir.getParentFile(), "config");
    }
    //System.out.println("Checking dir for clusters: dir=" + dir.getAbsolutePath());
    File[] files = dir.listFiles();
    if (files != null) {
      for (File file : files) {
        String name = file.getName();
        if (name.contains("config-license-server")) {
          continue;
        }
        if (name.contains("config-mgmt-server")) {
          continue;
        }
        if (name.startsWith("config-")) {
          String clusterName = name.substring("config-".length(), name.lastIndexOf('.'));
          clusters.add(clusterName);
        }
      }
    }
    return clusters;
  }

  public String getOSStats(String cluster, String time, String timezone) {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode node = mapper.createObjectNode();
    try {
      getClusters(node);

      //if (cluster == null || cluster.length() == 0 || cluster.equals("_unknown_")) {
      cluster = server.getCluster();
      //}

      Connection conn = getDbConnection(cluster);

      checkLicense((ConnectionProxy) conn);

      double maxMem = 0;
      double maxDisk = 0;
      int totalsCount = 0;
      try (PreparedStatement stmt = conn.prepareStatement("select * from os_totals")) {
        try (ResultSet rs = stmt.executeQuery()) {
          while (rs.next()) {
            maxMem = Math.max(maxMem, rs.getDouble("mem"));
            maxDisk = Math.max(maxDisk, rs.getDouble("disk"));
            totalsCount++;
          }
        }
      }
      logger.info("os_totals count: " + totalsCount);
      node.put("maxMem", maxMem);
      node.put("maxDisk", maxDisk);


      Date date = new Date(System.currentTimeMillis());
      Calendar cal = new GregorianCalendar();
      cal.setTime(date);

      String timeStr = null;
      String timeStr2 = null;
      if (time.equals("min15")) {
        cal.add(MINUTE, -15);
      }
      else if (time.equals("hour1")) {
        cal.add(Calendar.HOUR, -1);
      }
      else if (time.equals("hour6")) {
        cal.add(Calendar.HOUR, -6);
      }
      else if (time.equals("hour24")) {
        cal.add(Calendar.HOUR, -24);
      }
      else if (time.equals("day7")) {
        cal.add(HOUR, -24 * 7);
      }
      else if (time.equals("day30")) {
        cal.add(HOUR, -24 * 30);
      }
      else if (time.equals("today")) {
        Calendar cal2 = new GregorianCalendar();
        cal2.set(YEAR, cal.get(YEAR));
        cal2.set(MONTH, cal.get(MONTH));
        cal2.set(DAY_OF_MONTH, cal.get(DAY_OF_MONTH));
        cal = cal2;
      }
      else if (time.equals("yesterday")) {
        Calendar cal2 = new GregorianCalendar();
        cal2.set(YEAR, cal.get(YEAR));
        cal2.set(MONTH, cal.get(MONTH));
        cal2.set(DAY_OF_MONTH, cal.get(DAY_OF_MONTH));

        SimpleDateFormat df = new SimpleDateFormat(TIME_FORMAT_STR);
        df.setTimeZone(TimeZone.getTimeZone("UTC"));
        timeStr2 = df.format(new Date(cal2.getTimeInMillis()));

        cal2.set(YEAR, cal.get(YEAR));
        cal2.set(MONTH, cal.get(MONTH));
        cal2.set(DAY_OF_MONTH, cal.get(DAY_OF_MONTH) - 1);
        cal = cal2;
      }

      SimpleDateFormat df = new SimpleDateFormat(TIME_FORMAT_STR);
      df.setTimeZone(TimeZone.getTimeZone("UTC"));
      timeStr = df.format(new Date(cal.getTimeInMillis()));
      String query = null;
      if (timeStr2 == null) {
        query = "select * from os_stats where time_val > ? order by time_val asc";
      }
      else {
        query = "select * from os_stats where time_val > ? and time_val < ? order by time_val asc";
      }
      try (PreparedStatement stmt = conn.prepareStatement(query)) {
        stmt.setString(1, timeStr);
        if (timeStr2 != null) {
          stmt.setString(2, timeStr2);
        }
        try (ResultSet rs = stmt.executeQuery()) {
          ConcurrentSkipListMap<String, List<OSStats>> stats = new ConcurrentSkipListMap<>();
          while (rs.next()) {
            OSStats currStats = new OSStats();
            currStats.host = rs.getString("host");
            currStats.time = rs.getString("time_val");
            currStats.cpu = rs.getDouble("cpu");
            currStats.resGig = rs.getDouble("res_mem");
            currStats.javaMemMin = rs.getDouble("jmem_min");
            currStats.javaMemMax = rs.getDouble("jmem_max");
            currStats.netIn = rs.getDouble("net_bytes_in");
            currStats.netOut = rs.getDouble("net_bytes_out");
            currStats.diskAvail = rs.getDouble("disk_avail");

            List<OSStats> statsList = stats.get(currStats.time);
            if (statsList == null) {
              statsList = new ArrayList<>();
              stats.put(currStats.time, statsList);
            }
            statsList.add(currStats);
          }

          String minTime = stats.firstKey();
          String maxTime = stats.firstKey();
          for (String currTime : stats.keySet()) {
            if (time.compareTo(minTime) < 0) {
              minTime = currTime;
            }
            if (time.compareTo(maxTime) > 0) {
              maxTime = currTime;
            }
          }

          java.util.Date minDate = cal.getTime();//new SimpleDateFormat(TIME_FORMAT_STR).parse(minTime);
          long minMillis = minDate.getTime();
          Calendar currCal = new GregorianCalendar();
          currCal.setTimeInMillis(System.currentTimeMillis());
          java.util.Date maxDate = currCal.getTime();//new SimpleDateFormat(TIME_FORMAT_STR).parse(maxTime);
          long maxMillis = maxDate.getTime();
          long range = maxMillis - minMillis;
          int numPoints = 15;
          if (time.equals("hour1")) {
            numPoints = 50;
          }
          if (time.equals("hour6")) {
            numPoints = 75;
          }
          else if (time.equals("hour24") || time.equals("day7") || time.equals("day30")) {
            numPoints = 100;
          }
          double intervalSize = range / numPoints;
          ConcurrentSkipListMap<String, Histograms> buckets = new ConcurrentSkipListMap<>();
          long currMillis = minMillis;
          for (int i = 0; i < numPoints; i++) {
            df = new SimpleDateFormat(TIME_FORMAT_STR);
            df.setTimeZone(TimeZone.getTimeZone("UTC"));
            String formatted = df.format(new Date(currMillis));
            buckets.put(formatted, new Histograms());
            currMillis += intervalSize;
          }

          System.out.println("min=" + minDate + ", max=" + maxDate);
          intervalSize = range / 10;
          List<String> times = new ArrayList<>();
          currMillis = minMillis;
          for (int i = 0; i < 11; i++) {
            df = new SimpleDateFormat(TIME_FORMAT_STR);
            df.setTimeZone(TimeZone.getTimeZone(timezone));
            String formatted = df.format(new Date(currMillis));
            times.add(formatted);
            currMillis += intervalSize;
            System.out.println(formatted);
          }

          Iterator<Map.Entry<String, List<OSStats>>> iterator = stats.entrySet().iterator();
          String key = buckets.firstKey();
          Map.Entry<String, List<OSStats>> currEntry = null;
          while (key != null) {
            if (!iterator.hasNext()) {
              break;
            }
            Histograms historgrams = buckets.get(key);
            while (iterator.hasNext()) {
              Map.Entry<String, List<OSStats>> entry = currEntry;
              currEntry = null;
              if (entry == null) {
                entry = iterator.next();
              }
              if (entry.getKey().compareTo(key) < 0) {
                for (OSStats currStats : entry.getValue()) {
                  historgrams.cpuHistogram.update((long)currStats.cpu);
                  historgrams.resGigHistogram.update((long)currStats.resGig);
                  historgrams.javaMemMinHistogram.update((long)currStats.javaMemMin);
                  historgrams.javaMemMaxHistogram.update((long)currStats.javaMemMax);
                  historgrams.netInHistogram.update((long)currStats.netIn);
                  historgrams.netOutHistogram.update((long)currStats.netOut);
                  historgrams.diskAvailHistogram.update((long)currStats.diskAvail);
                }
              }
              else {
                currEntry = entry;
                break;
              }
            }
            key = buckets.higherKey(key);
          }

          for (Histograms h : buckets.values()) {
            h.cpuSnapshot = h.cpuHistogram.getSnapshot();
            h.diskAvailSnapshot = h.diskAvailHistogram.getSnapshot();
            h.netOutSnapshot = h.netOutHistogram.getSnapshot();
            h.netInSnapshot = h.netInHistogram.getSnapshot();
            h.javaMemMinSnapshot = h.javaMemMinHistogram.getSnapshot();
            h.javaMemMaxSnapshot = h.javaMemMaxHistogram.getSnapshot();
            h.resGigSnapshot = h.resGigHistogram.getSnapshot();
          }

          ArrayNode timesArray = node.putArray("times");
          for (String currTime : times) {
            timesArray.add(currTime);
          }
          ArrayNode max = node.putArray("max");
          for (Map.Entry<String, Histograms> statsList : buckets.entrySet()) {
            ObjectNode obj = max.addObject();
            Histograms h = statsList.getValue();

            obj.put("time", statsList.getKey());
            obj.put("cpu", h.cpuSnapshot.getMax());
            obj.put("net_in", h.netInSnapshot.getMax());
            obj.put("net_out", h.netOutSnapshot.getMax());
            obj.put("r_mem", h.resGigSnapshot.getMax());
            obj.put("j_min", h.javaMemMinSnapshot.getMax());
            obj.put("j_max", h.javaMemMaxSnapshot.getMax());
            obj.put("d_avail", h.diskAvailSnapshot.getMax());
          }
          ArrayNode p75 = node.putArray("p75");
          for (Map.Entry<String, Histograms> statsList : buckets.entrySet()) {
            ObjectNode obj = p75.addObject();
            Histograms h = statsList.getValue();

            obj.put("time", statsList.getKey());
            obj.put("cpu", h.cpuSnapshot.get75thPercentile());
            obj.put("net_in", h.netInSnapshot.get75thPercentile());
            obj.put("net_out", h.netOutSnapshot.get75thPercentile());
            obj.put("r_mem", h.resGigSnapshot.get75thPercentile());
            obj.put("j_min", h.javaMemMinSnapshot.get75thPercentile());
            obj.put("j_max", h.javaMemMaxSnapshot.get75thPercentile());
            obj.put("d_avail", h.diskAvailSnapshot.get75thPercentile());
          }
          ArrayNode p95 = node.putArray("p95");
          for (Map.Entry<String, Histograms> statsList : buckets.entrySet()) {
            ObjectNode obj = p95.addObject();
            Histograms h = statsList.getValue();

            obj.put("time", statsList.getKey());
            obj.put("cpu", h.cpuSnapshot.get95thPercentile());
            obj.put("net_in", h.netInSnapshot.get95thPercentile());
            obj.put("net_out", h.netOutSnapshot.get95thPercentile());
            obj.put("r_mem", h.resGigSnapshot.get95thPercentile());
            obj.put("j_min", h.javaMemMinSnapshot.get95thPercentile());
            obj.put("j_max", h.javaMemMaxSnapshot.get95thPercentile());
            obj.put("d_avail", h.diskAvailSnapshot.get95thPercentile());
          }
          ArrayNode p99 = node.putArray("p99");
          for (Map.Entry<String, Histograms> statsList : buckets.entrySet()) {
            ObjectNode obj = p99.addObject();
            Histograms h = statsList.getValue();

            obj.put("time", statsList.getKey());
            obj.put("cpu", h.cpuSnapshot.get99thPercentile());
            obj.put("net_in", h.netInSnapshot.get99thPercentile());
            obj.put("net_out", h.netOutSnapshot.get99thPercentile());
            obj.put("r_mem", h.resGigSnapshot.get99thPercentile());
            obj.put("j_min", h.javaMemMinSnapshot.get99thPercentile());
            obj.put("j_max", h.javaMemMaxSnapshot.get99thPercentile());
            obj.put("d_avail", h.diskAvailSnapshot.get99thPercentile());
          }
          ArrayNode p999 = node.putArray("p999");
          for (Map.Entry<String, Histograms> statsList : buckets.entrySet()) {
            ObjectNode obj = p999.addObject();
            Histograms h = statsList.getValue();

            obj.put("time", statsList.getKey());
            obj.put("cpu", h.cpuSnapshot.get999thPercentile());
            obj.put("net_in", h.netInSnapshot.get999thPercentile());
            obj.put("net_out", h.netOutSnapshot.get999thPercentile());
            obj.put("r_mem", h.resGigSnapshot.get999thPercentile());
            obj.put("j_min", h.javaMemMinSnapshot.get999thPercentile());
            obj.put("j_max", h.javaMemMaxSnapshot.get999thPercentile());
            obj.put("d_avail", h.diskAvailSnapshot.get999thPercentile());
          }
          ArrayNode avg = node.putArray("avg");
          for (Map.Entry<String, Histograms> statsList : buckets.entrySet()) {
            ObjectNode obj = avg.addObject();
            Histograms h = statsList.getValue();

            obj.put("time", statsList.getKey());
            obj.put("cpu", h.cpuSnapshot.getMean());
            obj.put("net_in", h.netInSnapshot.getMean());
            obj.put("net_out", h.netOutSnapshot.getMean());
            obj.put("r_mem", h.resGigSnapshot.getMean());
            obj.put("j_min", h.javaMemMinSnapshot.getMean());
            obj.put("j_max", h.javaMemMaxSnapshot.getMean());
            obj.put("d_avail", h.diskAvailSnapshot.getMean());
          }
        }
      }
    }
    catch (InsufficientLicense e) {
      throw e;
    }
    catch (Exception e) {
      logger.error("Error processing request", e);
    }
    return node.toString();
  }

  public void shutdown() {
    for (Connection conn : connections.values()) {
      try {
        conn.close();
      }
      catch (SQLException e) {
        logger.error("Error shutting down monitor handler", e);
      }
    }
  }

  class OSStats {
    String host;
    String time;
    double resGig;
    double cpu;
    double javaMemMin;
    double javaMemMax;
    double diskAvail;
    double netIn;
    double netOut;
  }

  class Histograms {
    String host;
    String time;
    Histogram resGigHistogram = new Histogram(new ExponentiallyDecayingReservoir());
    Histogram cpuHistogram = new Histogram(new ExponentiallyDecayingReservoir());
    Histogram javaMemMinHistogram = new Histogram(new ExponentiallyDecayingReservoir());
    Histogram javaMemMaxHistogram = new Histogram(new ExponentiallyDecayingReservoir());
    Histogram diskAvailHistogram = new Histogram(new ExponentiallyDecayingReservoir());
    Histogram netInHistogram = new Histogram(new ExponentiallyDecayingReservoir());
    Histogram netOutHistogram = new Histogram(new ExponentiallyDecayingReservoir());
    Snapshot resGigSnapshot;
    Snapshot cpuSnapshot;
    Snapshot javaMemMinSnapshot;
    Snapshot javaMemMaxSnapshot;
    Snapshot diskAvailSnapshot;
    Snapshot netInSnapshot;
    Snapshot netOutSnapshot;
  }


  public String getQueryStats(String cluster, String offset, String page_size, String orderBy,
                              String asc, String date, String currentDate, String timezone, String searchStr) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode node = mapper.createObjectNode();
    try {
      getClusters(node);

      //if (cluster == null || cluster.length() == 0 || cluster.equals("_unknown_")) {
      cluster = server.getCluster();
      //}

      Connection conn = getDbConnection(cluster);

      SimpleDateFormat df = new SimpleDateFormat(DAY_FORMAT_STR);
      TimeZone userTimezone = TimeZone.getTimeZone(timezone);
      //df.setTimeZone();

      //userTimezone.getOffset(

      String originalDate = date;
      Calendar dateObj = new GregorianCalendar();
      dateObj.setTime(df.parse(date));
//        Calendar currentDateObj = new GregorianCalendar();
//        currentDateObj.setTime(df.parse(currentDate));
//
//        long millisDiff = currentDateObj.getTimeInMillis() - dateObj.getTimeInMillis();
//        int daysDiff = (int) (millisDiff / 1000 / 60 / 60 / 24);

//
//        SimpleDateFormat dateFormatUtc = new SimpleDateFormat("yyyy-MMM-dd HH:mm:ss");
//        dateFormatUtc.setTimeZone(TimeZone.getTimeZone("UTC"));
//        SimpleDateFormat dateFormatLocal = new SimpleDateFormat("yyyy-MMM-dd HH:mm:ss");

//        Calendar now = new GregorianCalendar();
//        now.setTime(new Date());
//        now.
//        now.setTimeInMillis(System.currentTimeMillis());
      TimeZone utcTimezone = TimeZone.getTimeZone("UTC");
//        df.setTimeZone(utcTimezone);
//        now.set(HOUR, 0);
//        now.set(MINUTE, 0);
//        now.set(SECOND, 0);
//        now.set(MILLISECOND, 0);

//        now.add(DAY_OF_YEAR, -1 * daysDiff);

//        Date nowDate = dateFormatUtc.parse( dateFormatLocal.format(now.getTime()) );
//

      SimpleDateFormat utfDf = new SimpleDateFormat(DAY_FORMAT_STR);
      utfDf.setTimeZone(utcTimezone);
      date = utfDf.format(dateObj.getTime());//new Date(now.getTimeInMillis()));

      System.out.println("originalDate=" + originalDate + ", adjustedDate=" + date);
      //
//        currentDateObj.di
//
//        Calendar cal = new GregorianCalendar();
//        cal.setTimeInMillis(dateObj.getTime());
//        int timezoneOffset = userTimezone.getOffset(cal.getTimeInMillis());
//        cal.add(MILLISECOND, timezoneOffset);
//        TimeZone utcTimezone = TimeZone.getTimeZone("UTC");
//        int utcOffset = utcTimezone.getOffset(cal.getTimeInMillis());
//        cal.add(MILLISECOND, utcOffset);
//
//        Calendar now = new GregorianCalendar();
//        now.setTimeInMillis(System.currentTimeMillis());
//        int hour = now.get(HOUR_OF_DAY);
//        int minute = now.get(MINUTE);
//        cal.set(HOUR_OF_DAY, hour);
//        cal.set(MINUTE, minute);

//        Date adjustedateObj = cal.getTime();
//
//        df = new SimpleDateFormat(DAY_FORMAT_STR);
//        df.setTimeZone(TimeZone.getTimeZone("UTC"));
//        date = df.format(adjustedateObj.getTime());
//
//        SimpleDateFormat df2 = new SimpleDateFormat(TIME_FORMAT_STR);
//        df2.setTimeZone(TimeZone.getTimeZone("UTC"));
//        String time = df2.format(dateObj.getTime());
//        System.out.println("date_prev=" + dateObj + ", adjustedate=" + adjustedateObj + ", dateStr=" + date + ", time=" + time);
      checkLicense((ConnectionProxy) conn);


      ArrayNode queries = node.putArray("queries");

      if (orderBy.equals("count")) {
        orderBy = "cnt";
      }

      String likeStr = "";
      if (searchStr != null && searchStr.length() != 0) {
        likeStr = " and query like ?";
      }

      int lastOffset = 0;
      if (Integer.valueOf(offset) == -1) {// indicates last page
        int currOffset = 0;
        while (true) {
          try (PreparedStatement stmt = conn.prepareStatement("select * from query_stats where date_val=? " + likeStr + " order by " + orderBy + " " + asc + " limit " + page_size + " offset " + currOffset)) {
            stmt.setString(1, date);
            if (likeStr.length() != 0) {
              stmt.setString(2, searchStr);
            }
            ResultSet rs = stmt.executeQuery();
            int pos = 1;
            while (rs.next()) {
              pos++;
            }
            if (pos == 1) {
              lastOffset = currOffset - Integer.valueOf(page_size);
              break;
            }
            if (pos < Integer.valueOf(page_size)) {
              lastOffset = currOffset;
              break;
            }
            rs.close();
          }
          currOffset += Integer.valueOf(page_size);
        }
        offset = String.valueOf(lastOffset);
      }
      node.put("offset", offset);
      try (PreparedStatement stmt = conn.prepareStatement("select * from query_stats where date_val=? " + likeStr + " order by " + orderBy + " " + asc +  " limit " + page_size +" offset " + offset)) {
        stmt.setString(1, date);
        if (likeStr.length() != 0) {
          stmt.setString(2, searchStr);
        }
        ResultSet rs = stmt.executeQuery();
        while (rs.next()) {
          String dbName = rs.getString("db_name");
          String query = rs.getString("query");
          long count = rs.getLong("cnt");
          double lat_avg = rs.getDouble("lat_avg");
          double lat_75 = rs.getDouble("lat_75");
          double lat_95 = rs.getDouble("lat_95");
          double lat_99 = rs.getDouble("lat_99");
          double lat_999 = rs.getDouble("lat_999");
          double lat_max = rs.getDouble("lat_max");
          ObjectNode obj = queries.addObject();
          obj.put("db_name", dbName);
          obj.put("query", query);
          obj.put("lat_avg", lat_avg);
          obj.put("count", count);
          obj.put("lat_75", lat_75);
          obj.put("lat_95", lat_95);
          obj.put("lat_99", lat_99);
          obj.put("lat_999", lat_999);
          obj.put("lat_max", lat_max);
        }
        rs.close();
      }
    }
    catch (InsufficientLicense e) {
      throw e;
    }
    catch (Exception e) {
      logger.error("Error processing request", e);
    }
    return node.toString();
  }

  private Connection getDbConnection(String cluster) {
    try {
      Connection conn = null;
      synchronized (this) {
        conn = connections.get(cluster);
        if (conn != null) {
          return conn;
        }
        String json = IOUtils.toString(HttpServer.class.getResourceAsStream("/config-" + cluster + ".json"), "utf-8");
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode config = (ObjectNode) mapper.readTree(json);

        ArrayNode array = config.withArray("shards");
        ObjectNode replicaDict = (ObjectNode) array.get(0);
        ArrayNode replicasArray = replicaDict.withArray("replicas");
        JsonNode node = config.get("clientIsPrivate");
        final String address = node != null && node.asBoolean() ?
            replicasArray.get(0).get("privateAddress").asText() :
            replicasArray.get(0).get("publicAddress").asText();
        final int port = replicasArray.get(0).get("port").asInt();

        Class.forName("com.sonicbase.jdbcdriver.Driver");
        conn = DriverManager.getConnection("jdbc:sonicbase:" + address + ":" + port + "/_sonicbase_sys");

        connections.put(cluster, conn);
        return conn;
      }
    }
    catch (Exception e) {
      e.printStackTrace();
      throw new DatabaseException(e);
    }

  }


  private String getServerHealth(String cluster) {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode node = mapper.createObjectNode();
    try {
      getClusters(node);

      //if (cluster == null || cluster.length() == 0 || cluster.equals("_unknown_")) {
      cluster = server.getCluster();
      //}

      Connection conn = getDbConnection(cluster);

      checkLicense((ConnectionProxy) conn);

      DatabaseClient client = ((ConnectionProxy)conn).getDatabaseClient();
      client.syncSchema();

      ArrayNode shardsArray = node.putArray("shards");
      ServersConfig.Shard[] shards = client.getCommon().getServersConfig().getShards();
      for (int j = 0; j < shards.length; j++) {
        ServersConfig.Shard shard = shards[j];
        ServersConfig.Host[] replicas = shard.getReplicas();
        ArrayNode replicasArray = shardsArray.addArray();
        for (int i = 0; i < replicas.length; i++) {
          ObjectNode replicaNode = replicasArray.addObject();
          ServersConfig.Host replica = replicas[i];
          replicaNode.put("host", replica.getPrivateAddress() + ":" + replica.getPort());
          replicaNode.put("shard", String.valueOf(j));
          replicaNode.put("replica", String.valueOf(i));
          replicaNode.put("dead", String.valueOf(replica.isDead()));
          replicaNode.put("master", String.valueOf(shard.getMasterReplica() == i));
        }
      }
    }
    catch (InsufficientLicense e) {
      throw e;
    }
    catch (Exception e) {
      logger.error("Error processing request", e);
    }
    return node.toString();
  }
}