package com.sonicbase.common;

/**
 * Created by lowryda on 6/12/17.
 */
public class InsufficientLicense extends RuntimeException {

  public InsufficientLicense() {
  }

  public InsufficientLicense(String msg) {
    super(msg);
  }
}
