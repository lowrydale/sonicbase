package com.sonicbase.common;

public class InsufficientLicense extends RuntimeException {

  public InsufficientLicense() {
  }

  public InsufficientLicense(String msg) {
    super(msg);
  }
}
