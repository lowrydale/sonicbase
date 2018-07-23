package com.sonicbase.jdbcdriver;

import java.sql.SQLException;

public class NotSupportedException extends SQLException {
  public NotSupportedException(String s) {
    super(s);
  }

  public NotSupportedException() {
  }
}
