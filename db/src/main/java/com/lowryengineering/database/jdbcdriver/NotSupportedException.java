package com.lowryengineering.database.jdbcdriver;

import java.sql.SQLException;

/**
 * Created by IntelliJ IDEA.
 * User: lowryda
 * Date: Oct 8, 2011
 * Time: 10:45:20 AM
 */
public class NotSupportedException extends SQLException {
  public NotSupportedException(String s) {
    super(s);
  }

  public NotSupportedException() {
  }
}
