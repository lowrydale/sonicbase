package com.lowryengineering.database.jdbcdriver;

/**
 * User: lowryda
 * Date: 10/25/14
 * Time: 9:42 AM
 */

import com.lowryengineering.database.client.DatabaseClient;
import com.lowryengineering.database.query.DatabaseException;

import java.sql.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by IntelliJ IDEA.
 * User: lowryda
 * Date: Oct 7, 2011
 * Time: 3:09:46 PM
 */
public class ConnectionProxy implements Connection {

  private static final Object clientMutex = new Object();
  private static DatabaseClient databaseClient;
  private final String dbName;
  private boolean autoCommit;
  private java.util.Map<String, Class<?>> typemap;
  private int rsHoldability;
  private Properties _clientInfo;
  private Properties properties;
  private boolean closed = false;
  private int shard;


  public ConnectionProxy(String url, Properties properties) throws SQLException {

    this.properties = properties;
    initGlobalContext();
    try {
      String[] outerParts = url.split("/");
      String[] parts = outerParts[0].split(":");
      String host = parts[2];
      int port = Integer.valueOf(parts[3]);
      String db = null;
      if (outerParts.length > 1) {
        db = outerParts[1];
        db = db.toLowerCase();
      }
      synchronized (clientMutex) {
        if (databaseClient == null) {
          databaseClient = new DatabaseClient(host, port, true);
        }
      }
      if (db != null) {
        databaseClient.initDb(db);
      }
      this.dbName = db;
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public String getDbName() {
    return dbName;
  }
  private static final AtomicInteger globalContextRefCount = new AtomicInteger();

  public DatabaseClient getDatabaseClient() {
    return databaseClient;
  }

  private void initGlobalContext() {
    globalContextRefCount.incrementAndGet();
  }

  protected void checkClosed() throws SQLException {
      if (isClosed()) {
        throw new SQLException("This connection has been closed.");
      }
  }

  public Statement createStatement() throws SQLException {
    return new StatementProxy(this, databaseClient, null);
  }

  public void beginExplicitTransaction(String dbName) throws SQLException {
    databaseClient.beginExplicitTransaction(dbName);
  }

  public boolean getAutoCommit() throws SQLException {
    return autoCommit;
  }

  public void commit() throws SQLException {
    try {
      databaseClient.commit(dbName, null);
    }
    catch (DatabaseException e) {
      throw new SQLException(e);
    }
  }

  public void rollback() throws SQLException {
    databaseClient.rollback();
  }

  public String nativeSQL(String sql) throws SQLException {
    throw new NotImplementedException();
  }

  public void setAutoCommit(boolean autoCommit) throws SQLException {
    this.autoCommit = autoCommit;
    if (!autoCommit) {
      beginExplicitTransaction(dbName);
    }
  }

  public void close() throws SQLException {
    if (closed) {
      throw new SQLException("Attempting to close a connection that is already closed");
    }
    closed = true;
  }

  public boolean isClosed() throws SQLException {
    return closed;
  }

  public DatabaseMetaData getMetaData() throws SQLException {
    return null;
  }

  public void setReadOnly(boolean readOnly) throws SQLException {
    throw new NotImplementedException();
  }

  public boolean isReadOnly() throws SQLException {
    return false;
  }

  public void setCatalog(String catalog) throws SQLException {
    //todo: implement
    //To change body of implemented methods use File | Settings | File Templates.
  }

  public String getCatalog() throws SQLException {
    //todo: implement
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  public void setTransactionIsolation(int level) throws SQLException {
    //todo: implement
    //To change body of implemented methods use File | Settings | File Templates.
  }

  public int getTransactionIsolation() throws SQLException {
    //todo: implement
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  public SQLWarning getWarnings() throws SQLException {
    checkClosed();
    StringBuilder ret = new StringBuilder();
    if (ret.length() == 0) {
      return null;
    }
    return new SQLWarning(ret.toString());
  }

  public void clearWarnings() throws SQLException {
    checkClosed();
  }

  public PreparedStatement prepareStatement(String sql) throws SQLException {
    return new StatementProxy(this, databaseClient, sql);
  }

  public CallableStatement prepareCall(String sql) throws SQLException {
    throw new SQLException("not supported");
  }

  public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
    return new StatementProxy(this, databaseClient, null);
  }

  public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    return new StatementProxy(this, databaseClient, null);
  }

  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    return new StatementProxy(this, databaseClient, sql);
  }

  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    throw new SQLException("not supported");
  }

  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    return new StatementProxy(this, databaseClient, sql);
  }

  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    return new StatementProxy(this, databaseClient, sql);
  }

  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    return new StatementProxy(this, databaseClient, sql);
  }

  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
    return new StatementProxy(this, databaseClient, sql);
  }

  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
    throw new SQLException("not supported");
  }

  public Map<String, Class<?>> getTypeMap() throws SQLException {
    checkClosed();
    return typemap;
  }

  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    typemap = map;
  }

  public void setHoldability(int holdability) throws SQLException
  {
      checkClosed();

      switch (holdability)
      {
      case ResultSet.CLOSE_CURSORS_AT_COMMIT:
          rsHoldability = holdability;
          break;
      case ResultSet.HOLD_CURSORS_OVER_COMMIT:
          rsHoldability = holdability;
          break;
      default:
          throw new SQLException("Unknown ResultSet holdability setting: " + holdability);
      }
  }

  public int getHoldability() throws SQLException {
    return rsHoldability;
  }

  public Savepoint setSavepoint() throws SQLException {
    throw new NotImplementedException();
  }

  public Savepoint setSavepoint(String name) throws SQLException {
    throw new NotImplementedException();
  }

  public void rollback(Savepoint savepoint) throws SQLException {
    throw new NotImplementedException();
  }

  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    throw new NotImplementedException();
  }

  public Clob createClob() throws SQLException {
    return new com.lowryengineering.database.query.impl.Clob();
  }

  public Blob createBlob() throws SQLException {
    return new com.lowryengineering.database.query.impl.Blob();
  }

  public NClob createNClob() throws SQLException {
    return new com.lowryengineering.database.query.impl.NClob();
  }

  public SQLXML createSQLXML() throws SQLException {
    throw new NotImplementedException();
  }

  public boolean isValid(int timeout) throws SQLException {
    //todo: implement
    return false;
  }

  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    Map<String, ClientInfoStatus> failures = new HashMap<String, ClientInfoStatus>();
    failures.put(name, ClientInfoStatus.REASON_UNKNOWN_PROPERTY);
    throw new SQLClientInfoException(failures);
  }

  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    if (properties == null || properties.size() == 0)
        return;

    Map<String, ClientInfoStatus> failures = new HashMap<String, ClientInfoStatus>();

    Iterator<String> i = properties.stringPropertyNames().iterator();
    while (i.hasNext()) {
        failures.put(i.next(), ClientInfoStatus.REASON_UNKNOWN_PROPERTY);
    }
    throw new SQLClientInfoException(failures);
  }

  public String getClientInfo(String name) throws SQLException {
    checkClosed();
    return null;
  }

  public Properties getClientInfo() throws SQLException {
    checkClosed();
    if (_clientInfo == null) {
        _clientInfo = new Properties();
    }
    return _clientInfo;
  }

  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    //todo: implement
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    //todo: implement
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  public void setSchema(String schema) throws SQLException {
    // todo: implement for JDK 1.7
  }

  public String getSchema() throws SQLException {
    // todo: implement for JDK 1.7
    return null;
  }

  public void abort(Executor executor) throws SQLException {
    // todo: implement for JDK 1.7
  }

  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    // todo: implement for JDK 1.7
  }

  public int getNetworkTimeout() throws SQLException {
    // todo: implement for JDK 1.7
    return 0;
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
    //todo: validate cast
    return (T) this;
  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    //todo: validate iface
    return true;
  }

  public void createDatabase(String dbName) {
    databaseClient.createDatabase(dbName);
  }
}
