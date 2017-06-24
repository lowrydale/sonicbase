package com.sonicbase.socket;

/**
 * Created by lowryda on 6/17/17.
 */
public class DeadServerException extends RuntimeException {

  public DeadServerException() {

  }

  public DeadServerException(Throwable t) {
    super(t);
  }

  public DeadServerException(String msg, Throwable throwable) {
    super(msg, throwable);
  }
}
