package com.sonicbase.common;

public class DeadServerException extends RuntimeException {

  public DeadServerException() {

  }

  public DeadServerException(String msg) {
    super(msg);
  }

  public DeadServerException(Throwable t) {
    super(t);
  }

  public DeadServerException(String msg, Throwable throwable) {
    super(msg, throwable);
  }
}
