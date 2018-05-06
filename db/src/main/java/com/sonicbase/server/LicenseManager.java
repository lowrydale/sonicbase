/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.common.ComObject;
import com.sonicbase.common.ThreadUtil;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.util.DateUtils;
import org.apache.commons.io.IOUtils;

import javax.net.ssl.*;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.security.cert.X509Certificate;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Calendar.HOUR;
import static jdk.nashorn.internal.parser.DateParser.DAY;

public class LicenseManager {
  private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("com.sonicbase.logger");

  private final DatabaseServer server;
  private boolean shutdownMasterValidatorThread = false;
  private Thread masterLicenseValidatorThread;
  private String disableDate;
  private Boolean multipleLicenseServers;
  private boolean overrideProLicense;
  private boolean haveProLicense;
  private Boolean disableNow = false;
  private Map<Integer, Map<Integer, Integer>> numberOfCoresPerServer = new HashMap<>();
  private Thread licenseValidatorThread;


  public LicenseManager(DatabaseServer server, boolean overrideProLicense) {
    this.server = server;
    this.haveProLicense = true;
    this.disableNow = false;
    this.overrideProLicense = overrideProLicense;
    haveProLicense = true;
    disableNow = false;
    this.disableNow = false;
    this.haveProLicense = true;
    //startMasterLicenseValidator();

    try {
      checkLicense(new AtomicBoolean(false), new AtomicBoolean(false), new AtomicBoolean(true));
    }
    catch (Exception e) {
      logger.error("Error checking license", e);
    }
  }

  public void shutdown() {
    try {
      if (licenseValidatorThread != null) {
        licenseValidatorThread.interrupt();
        licenseValidatorThread.join();
      }
      shutdownMasterLicenseValidator();
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public void startMasterLicenseValidator() {

    shutdownMasterValidatorThread = false;

    if (overrideProLicense) {
      logger.info("Overriding pro license");
      haveProLicense = true;
      server.getCommon().setHaveProLicense(haveProLicense);
      server.getCommon().saveSchema(server.getClient(), server.getDataDir());
      return;
    }
    haveProLicense = true;

    final AtomicBoolean haventSet = new AtomicBoolean(true);
    //    if (usingMultipleReplicas) {
    //      throw new InsufficientLicense("You must have a pro license to use multiple replicas");
    //    }

    final AtomicInteger licensePort = new AtomicInteger();
    String json = null;
    try {
      json = IOUtils.toString(DatabaseServer.class.getResourceAsStream("/config-license-server.json"), "utf-8");
    }
    catch (Exception e) {
      logger.error("Error initializing license validator", e);
//      common.setHaveProLicense(false);
//      common.saveSchema(getClient(), dataDir);
      haveProLicense = false;
      disableNow = true;
//      this.haveProLicense = true;
//      this.disableNow = false;
//      this.disableDate = null;
//      this.multipleLicenseServers = false;

      haventSet.set(false);
      return;
    }
    try {
      ObjectMapper mapper = new ObjectMapper();
      ObjectNode config = (ObjectNode) mapper.readTree(json);
      licensePort.set(config.get("server").get("port").asInt());
      final String address = config.get("server").get("privateAddress").asText();

      final AtomicBoolean lastHaveProLicense = new AtomicBoolean(false);
      final AtomicBoolean haveHadProLicense = new AtomicBoolean(false);

      doValidateLicense(address, licensePort, lastHaveProLicense, haveHadProLicense, haventSet, false);
      masterLicenseValidatorThread = ThreadUtil.createThread(new Runnable() {
        @Override
        public void run() {
          while (!shutdownMasterValidatorThread) {
            try {
//            DatabaseServer.this.haveProLicense = true;
//            DatabaseServer.this.disableNow = false;
//            DatabaseServer.this.disableDate = null;
//            DatabaseServer.this.multipleLicenseServers = false;

              doValidateLicense(address, licensePort, lastHaveProLicense, haveHadProLicense, haventSet, false);
            }
            catch (Exception e) {
              logger.error("license server not found", e);
              if (haveHadProLicense.get()) {
                haveProLicense = true;
                lastHaveProLicense.set(true);
                haventSet.set(false);
                disableNow = false;
                disableDate = DateUtils.fromDate(new Date(System.currentTimeMillis() + 7 * 24 * 60 * 60 * 1000));
              }
              else {
                //  if (haventSet.get() || lastHaveProLicense.get() != false) {
//              common.setHaveProLicense(false);
//              common.saveSchema(getClient(), dataDir);
                haveProLicense = false;
                lastHaveProLicense.set(false);
                haventSet.set(false);
                disableNow = true;
                disableDate = DateUtils.fromDate(new Date(System.currentTimeMillis()));
//              DatabaseServer.this.haveProLicense = true;
//              DatabaseServer.this.disableNow = false;
//              DatabaseServer.this.disableDate = null;
//              DatabaseServer.this.multipleLicenseServers = false;

              }
              logger.debug("License server not found", e);
            }
            try {
              Thread.sleep(60 * 1000);
            }
            catch (InterruptedException e) {
              logger.error("Error checking licenses", e);
            }
          }
        }
      }, "SonicBase Master License Validator Thread");
      masterLicenseValidatorThread.start();
    }
    catch (Exception e) {
      logger.error("Error validating licenses", e);
    }
  }

  private void doValidateLicense(String address, AtomicInteger licensePort, AtomicBoolean lastHaveProLicense,
                                 AtomicBoolean haveHadProLicense, AtomicBoolean haventSet, boolean standalone) throws IOException {
    int cores = 0;
    if (!standalone) {
      synchronized (numberOfCoresPerServer) {
        for (Map.Entry<Integer, Map<Integer, Integer>> shardEntry : numberOfCoresPerServer.entrySet()) {
          for (Map.Entry<Integer, Integer> replicaEntry : shardEntry.getValue().entrySet()) {
            cores += replicaEntry.getValue();
          }
        }
      }
    }

    try {
      TrustManager[] trustAllCerts = new TrustManager[]{
          new X509TrustManager() {
            public java.security.cert.X509Certificate[] getAcceptedIssuers() {
              return null;
            }

            public void checkClientTrusted(X509Certificate[] certs, String authType) {
            }

            public void checkServerTrusted(X509Certificate[] certs, String authType) {
            }

          }
      };

      SSLContext sc = SSLContext.getInstance("SSL");
      sc.init(null, trustAllCerts, new java.security.SecureRandom());
      HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());

      // Create all-trusting host name verifier
      HostnameVerifier allHostsValid = new HostnameVerifier() {

        public boolean verify(String hostname, SSLSession session) {
          return true;
        }
      };
      // Install the all-trusting host verifier
      HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);
      /*
       * end of the fix
       */

      URL url = new URL("https://" + address + ":" + licensePort.get() + "/license/checkIn?" +
          "primaryAddress=" + server.getCommon().getServersConfig().getShards()[0].getReplicas()[0].getPrivateAddress() +
          "&primaryPort=" + server.getCommon().getServersConfig().getShards()[0].getReplicas()[0].getPort() +
          "&cluster=" + server.getCluster() + "&cores=" + cores);
      URLConnection con = url.openConnection();
      InputStream in = new BufferedInputStream(con.getInputStream());

//      HttpResponse response = restGet("https://" + config.getDict("server").getString("publicAddress") + ":" +
//          config.getDict("server").getInt("port") + "/license/currUsage");
      ObjectMapper mapper = new ObjectMapper();
      ObjectNode dict = (ObjectNode) mapper.readTree(IOUtils.toString(in, "utf-8"));

//      HttpResponse response = DatabaseClient.restGet("https://" + address + ":" + licensePort.get() + "/license/checkIn?" +
//        "primaryAddress=" + common.getServersConfig().getShards()[0].getReplicas()[0].getPrivateAddress() +
//        "&primaryPort=" + common.getServersConfig().getShards()[0].getReplicas()[0].getPort() +
//        "&cluster=" + cluster + "&cores=" + cores);
//    String responseStr = StreamUtils.inputStreamToString(response.getContent());
//    logger.info("CheckIn response: " + responseStr);

      this.haveProLicense = dict.get("inCompliance").asBoolean();
      this.disableNow = dict.get("disableNow").asBoolean();
      this.disableDate = dict.get("disableDate").asText();
      this.multipleLicenseServers = dict.get("multipleLicenseServers").asBoolean();
      if (haveProLicense) {
        haveHadProLicense.set(true);
      }
//      this.haveProLicense = true;
//      this.disableNow = false;
//      this.multipleLicenseServers = false;
      logger.info("licenseValidator: cores=" + cores + ", lastHaveProLicense=" + lastHaveProLicense.get() +
          ", haveProLicense=" + haveProLicense + ",  disableNow=" + disableNow);
      if (haventSet.get() || lastHaveProLicense.get() != haveProLicense) {
//        common.setHaveProLicense(haveProLicense);
//        if (!standalone) {
//          common.saveSchema(getClient(), dataDir);
//        }
        lastHaveProLicense.set(haveProLicense);
        haventSet.set(true);
        logger.info("Saving schema with haveProLicense=" + haveProLicense);
      }
    }
    catch (Exception e) {
//      this.haveProLicense = true;
//      this.disableNow = false;
//      this.disableDate = null;
//      this.multipleLicenseServers = false;

      if (haveHadProLicense.get()) {
        Date date = new Date(System.currentTimeMillis());
        Calendar cal = new GregorianCalendar();
        cal.setTime(date);
        cal.add(DAY, 7);
        disableDate = DateUtils.fromDate(cal.getTime());

        this.haveProLicense = true;
        this.disableNow = false;
        this.multipleLicenseServers = false;
      }
      else {
        this.haveProLicense = false;
        this.disableNow = true;
        disableDate = DateUtils.fromDate(new Date(System.currentTimeMillis()));
        this.multipleLicenseServers = false;
      }
//      common.setHaveProLicense(haveProLicense);
//      if (!standalone) {
//        common.saveSchema(getClient(), dataDir);
//        logger.error("Error validating license", e);
//      }
      lastHaveProLicense.set(haveProLicense);
      haventSet.set(true);
      logger.error("MasterLicenseValidator error checking licenses", e);
    }
  }

  public void shutdownMasterLicenseValidator() {
    shutdownMasterValidatorThread = true;
    if (masterLicenseValidatorThread != null) {
      masterLicenseValidatorThread.interrupt();

      try {
        masterLicenseValidatorThread.join();
        masterLicenseValidatorThread = null;
      }
      catch (InterruptedException e) {
        throw new DatabaseException(e);
      }
    }
  }

  public void startLicenseValidator() {
    if (overrideProLicense) {
      logger.info("Overriding pro license");
      haveProLicense = true;
      disableNow = false;
//      common.setHaveProLicense(haveProLicense);
//      common.saveSchema(getClient(), dataDir);
      return;
    }
    haveProLicense = true;

    final AtomicBoolean haventSet = new AtomicBoolean(true);
//    if (usingMultipleReplicas) {
//      throw new InsufficientLicense("You must have a pro license to use multiple replicas");
//    }

    final AtomicInteger licensePort = new AtomicInteger();
    String json = null;
    try {
      json = IOUtils.toString(DatabaseServer.class.getResourceAsStream("/config-license-server.json"), "utf-8");
    }
    catch (Exception e) {
      logger.error("Error initializing license validator", e);

//      this.haveProLicense = true;
//      this.disableNow = false;
//      this.disableDate = null;
//      this.multipleLicenseServers = false;

//      common.setHaveProLicense(false);
//      common.saveSchema(getClient(), dataDir);
      haveProLicense = false;
      disableNow = true;
      haventSet.set(false);
      return;
    }
    try {
      ObjectMapper mapper = new ObjectMapper();
      ObjectNode config = (ObjectNode) mapper.readTree(json);
      licensePort.set(config.get("server").get("port").asInt());
      final String address = config.get("server").get("privateAddress").asText();

      final AtomicBoolean lastHaveProLicense = new AtomicBoolean(haveProLicense);
      final AtomicBoolean haveHadProLicense = new AtomicBoolean();

      licenseValidatorThread = ThreadUtil.createThread(new Runnable() {
        @Override
        public void run() {
          while (!server.getShutdown()) {
            boolean hadError = false;
            try {
              if (!(server.getShard() == 0 && server.getCommon().getServersConfig().getShards()[0].getMasterReplica() == server.getReplica())) {
                checkLicense(lastHaveProLicense, haveHadProLicense, haventSet);
              }
              else {
                if (haveProLicense) {
                  Date date = new Date(System.currentTimeMillis());
                  Calendar cal = new GregorianCalendar();
                  cal.setTime(date);
                  cal.add(HOUR, 1);
                  disableDate = DateUtils.fromDate(cal.getTime());

                }
                else {
                  Date date = new Date(System.currentTimeMillis());
                  disableDate = DateUtils.fromDate(date);
                }
              }
            }
            catch (Exception e) {
              try {
                Date date = disableDate == null ? null : DateUtils.fromString(disableDate);
                if (date == null || date.getTime() < System.currentTimeMillis()) {
                  disableNow = true;
                  haveProLicense = false;
                }
              }
              catch (ParseException e1) {
                logger.error("Error validating license", e);
              }

              hadError = true;
              logger.error("license server not found", e);
              if (haventSet.get() || lastHaveProLicense.get() != false) {
//                common.setHaveProLicense(false);
                //common.saveSchema(getClient(), dataDir);
                haveProLicense = false;
                lastHaveProLicense.set(false);
                haventSet.set(false);
                disableNow = true;
              }
              logger.debug("License server not found", e);
            }
            try {
              if (hadError) {
                Thread.sleep(1000);
              }
              else {
                Thread.sleep(60 * 1000);
              }
            }
            catch (InterruptedException e) {
              logger.error("Error checking licenses", e);
            }
          }
        }
      }, "SonicBase License Validator Thread");
      licenseValidatorThread.start();
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private void checkLicense(AtomicBoolean lastHaveProLicense, AtomicBoolean haveHadProLicense, AtomicBoolean haventSet) {
    int cores = Runtime.getRuntime().availableProcessors();

    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.dbName, "__none__");
    cobj.put(ComObject.Tag.schemaVersion, server.getCommon().getSchemaVersion());
    cobj.put(ComObject.Tag.method, "LicenseManager:licenseCheckin");
    cobj.put(ComObject.Tag.shard, server.getShard());
    cobj.put(ComObject.Tag.replica, server.getReplica());
    cobj.put(ComObject.Tag.coreCount, cores);
    byte[] ret = null;
    if (0 == server.getShard() && server.getCommon().getServersConfig().getShards()[0].getMasterReplica() == server.getReplica()) {
      ret = licenseCheckin(cobj, false).serialize();
    }
    else {
      ret = server.getClient().sendToMaster(cobj);
    }
    ComObject retObj = new ComObject(ret);

    haveProLicense = retObj.getBoolean(ComObject.Tag.inCompliance);
    disableNow = retObj.getBoolean(ComObject.Tag.disableNow);
    //DatabaseServer.this.disableDate = retObj.getString(ComObject.Tag.disableDate);

    if (haveProLicense) {
      Date date = new Date(System.currentTimeMillis());
      Calendar cal = new GregorianCalendar();
      cal.setTime(date);
      cal.add(HOUR, 1);
      disableDate = DateUtils.fromDate(cal.getTime());
    }
    else {
      Date date = new Date(System.currentTimeMillis());
      disableDate = DateUtils.fromDate(date);
    }

    haveHadProLicense.set(haveProLicense);

    multipleLicenseServers = retObj.getBoolean(ComObject.Tag.multipleLicenseServers);

//    DatabaseServer.this.haveProLicense = true;
//    DatabaseServer.this.disableNow = false;
//    DatabaseServer.this.disableDate = null;
//    DatabaseServer.this.multipleLicenseServers = false;

    logger.info("licenseCheckin: lastHaveProLicense=" + lastHaveProLicense.get() + ", haveProLicense=" + haveProLicense +
        ", disableNow=" + disableNow + ", disableDate=" + disableDate + ", multipleLicenseServers=" + multipleLicenseServers);
    if (haventSet.get() || lastHaveProLicense.get() != haveProLicense) {
//      common.setHaveProLicense(haveProLicense);
      //common.saveSchema(getClient(), dataDir);
      lastHaveProLicense.set(haveProLicense);
      haventSet.set(false);
      logger.info("Saving schema with haveProLicense=" + haveProLicense);
    }
  }

  public ComObject licenseCheckin(ComObject cobj, boolean replayedCommand) {
    int shard = cobj.getInt(ComObject.Tag.shard);
    int replica = cobj.getInt(ComObject.Tag.replica);
    int cores = cobj.getInt(ComObject.Tag.coreCount);
    synchronized (numberOfCoresPerServer) {
      Map<Integer, Integer> replicaMap = numberOfCoresPerServer.get(shard);
      if (replicaMap == null) {
        replicaMap = new HashMap<>();
        numberOfCoresPerServer.put(shard, replicaMap);
      }
      replicaMap.put(replica, cores);
    }

    ComObject retObj = new ComObject();
    retObj.put(ComObject.Tag.inCompliance, this.haveProLicense);
    retObj.put(ComObject.Tag.disableNow, this.disableNow);
    if (disableDate != null) {
      retObj.put(ComObject.Tag.disableDate, this.disableDate);
    }
    if (multipleLicenseServers == null) {
      retObj.put(ComObject.Tag.multipleLicenseServers, false);
    }
    else {
      retObj.put(ComObject.Tag.multipleLicenseServers, this.multipleLicenseServers);
    }
    return retObj;
  }

  public boolean disableNow() {
    return disableNow;
  }

  public boolean haveProLicense() {
    return haveProLicense;
  }

  public void setOverrideProLicense(boolean overridProLicense) {
    this.overrideProLicense = overridProLicense;
  }
}
