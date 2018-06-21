package com.sonicbase.common;

/**
 * Created by lowryda on 6/17/17.
 */
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
