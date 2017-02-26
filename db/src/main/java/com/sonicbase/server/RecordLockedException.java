package com.sonicbase.server;

/**
 * Responsible for
 */
public class RecordLockedException extends RuntimeException {
  public RecordLockedException() {
  }

  public RecordLockedException(String msg) {
    super(msg);
  }

  public RecordLockedException(String msg, Exception e) {
    super(msg, e);
  }

  public RecordLockedException(Exception e) {
    super(e);
  }
}
