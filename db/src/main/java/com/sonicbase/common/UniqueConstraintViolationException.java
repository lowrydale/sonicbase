package com.sonicbase.common;

public class UniqueConstraintViolationException extends RuntimeException {

  public UniqueConstraintViolationException(String msg) {
    super(msg);
  }
}
