package com.sonicbase.server;

import com.sonicbase.query.DatabaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

@SuppressWarnings({"squid:S1172", "squid:S1168", "squid:S00107"})
// all methods called from method invoker must have cobj and replayed command parms
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
class LicenseManagerProxy {

  private static final Logger logger = LoggerFactory.getLogger(LicenseManagerProxy.class);

  private Object licenseManager;
  private Method startMasterLicenseValidator;
  private Method shutdownMasterLicenseValidator;

  LicenseManagerProxy(Object proServer) {
    try {
      Class proClz = Class.forName("com.sonicbase.server.ProServer");
      Method method = proClz.getMethod("getLicenseManager");
      licenseManager = method.invoke(proServer);
      Class streamClz = Class.forName("com.sonicbase.server.LicenseManager");
      startMasterLicenseValidator = streamClz.getMethod("startMasterLicenseValidator");
      shutdownMasterLicenseValidator = streamClz.getMethod("shutdownMasterLicenseValidator");
    }
    catch (Exception e) {
      logger.warn("Error initializing LicenseManager", e);
    }
  }

  void startMasterLicenseValidator() {
    try {
      if (startMasterLicenseValidator != null) {
        startMasterLicenseValidator.invoke(licenseManager);
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  void shutdownMasterLicenseValidator() {
    try {
      if (shutdownMasterLicenseValidator != null) {
        shutdownMasterLicenseValidator.invoke(licenseManager);
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }
}
