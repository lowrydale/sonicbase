package com.sonicbase.jdbcdriver;


import com.sonicbase.query.DatabaseException;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

public class Driver implements java.sql.Driver {


  private static final String URL_PREFIX =  "jdbc:sonicbase";

  private static Driver driver;

  private static final int MAJOR_VERSION = 0;
  private static final int MINOR_VERSION = 502;

  static {
    try {
      driver = new Driver();
      DriverManager.registerDriver(driver);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public Connection connect(String url, Properties info) throws SQLException {
    if (!acceptsURL(url)) {
      return null;
    }
    return new ConnectionProxy(url, info);
  }

  public boolean acceptsURL(String url) throws SQLException {
    return url.startsWith(URL_PREFIX);
  }

  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    return new DriverPropertyInfo[0];  //To change body of implemented methods use File | Settings | File Templates.
  }

  public int getMajorVersion() {
    return MAJOR_VERSION;
  }

  public int getMinorVersion() {
    return MINOR_VERSION;
  }

  public boolean jdbcCompliant() {
    return true;
  }

  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return null;
  }
}
