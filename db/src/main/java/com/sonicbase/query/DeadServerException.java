/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.query;

public class DeadServerException extends RuntimeException {
  public DeadServerException(Exception e) {
    super(e);
  }
  public DeadServerException(String msg, Exception e) {
    super(msg, e);
  }
}
