/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.common;

public class NotFoundException extends RuntimeException {
  public NotFoundException() {
    super();
  }

  public NotFoundException(String msg) {
    super(msg);
  }

  public NotFoundException(String msg, Exception e) {
    super(msg, e);
  }
}
