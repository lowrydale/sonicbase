package com.sonicbase.common;

/**
 * Created by lowryda on 6/9/17.
 */
public class LicenseOutOfComplianceException extends RuntimeException {

  public LicenseOutOfComplianceException() {

  }

  public LicenseOutOfComplianceException(String msg) {
    super(msg);
  }
}
