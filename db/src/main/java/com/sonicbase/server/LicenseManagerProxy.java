/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.server;

import com.sonicbase.common.ComObject;
import com.sonicbase.query.DatabaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

public class LicenseManagerProxy {

  private static Logger logger = LoggerFactory.getLogger(LicenseManagerProxy.class);

  private Object licenseManager;
  private Method startMasterLicenseValidator;
  private Method shutdownMasterLicenseValidator;

  public LicenseManagerProxy(Object proServer) {
    try {
      Class proClz = Class.forName("com.sonicbase.server.ProServer");
      Method method = proClz.getMethod("getLicenseManager");
      licenseManager = method.invoke(proServer);
      Class streamClz = Class.forName("com.sonicbase.server.LicenseManager");
      startMasterLicenseValidator = streamClz.getMethod("startMasterLicenseValidator");
      shutdownMasterLicenseValidator = streamClz.getMethod("shutdownMasterLicenseValidator");
    }
    catch (Exception e) {
      logger.error("Error initializing LicenseManager", e);
    }
  }

  public void startMasterLicenseValidator() {
    try {
      if (startMasterLicenseValidator != null) {
        startMasterLicenseValidator.invoke(licenseManager);
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public void shutdownMasterLicenseValidator() {
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
