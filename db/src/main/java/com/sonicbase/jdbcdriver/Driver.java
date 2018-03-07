package com.sonicbase.jdbcdriver;


import com.sonicbase.query.DatabaseException;

import java.io.IOException;
import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: lowryda
 * Date: Oct 7, 2011
 * Time: 3:11:09 PM
 */
public class Driver implements java.sql.Driver {


  public static final String URL_PREFIX =  "jdbc:sonicbase";

  private static Driver driver;

  public static final int MAJOR_VERSION = 0;
  public static final int MINOR_VERSION = 502;

  static {
    try {
      driver = new Driver();
      DriverManager.registerDriver(driver);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public Driver() throws IOException {

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
    // todo: implement for JDK 1.7
    return null;
  }
}
