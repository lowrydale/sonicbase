package com.sonicbase.jdbcdriver;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.client.InsertStatementHandler;
import com.sonicbase.procedure.StoredProcedureContextImpl;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.query.impl.ResultSetImpl;
import net.sf.jsqlparser.statement.insert.Insert;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;

public class StatementProxy extends ParameterHandler implements java.sql.Statement, PreparedStatement {

  public static final String NOT_SUPPORTED_STR = "not supported";
  private final String dbName;

  private String sql;
  private DatabaseClient databaseClient;
  private ConnectionProxy connectionProxy;

  private Integer maxFieldSize;
  private Integer maxRows;
  private Integer fetchDirection;
  private Integer fetchSize;
  private ParameterHandler parms = new ParameterHandler();
  private boolean restrictToThisServer = false;
  private StoredProcedureContextImpl procedureContext;
  private boolean disableStats;

  public StatementProxy(ConnectionProxy connectionProxy, DatabaseClient databaseClient, String sql) {
    this.connectionProxy = connectionProxy;
    this.databaseClient = databaseClient;
    this.sql = sql;
    this.dbName = connectionProxy.getDbName();

    InsertStatementHandler.getBatch().set(null);
  }

  public void close() {
    clearBatch();
  }

  public int getMaxFieldSize() {
    if (maxFieldSize != null) {
      return maxFieldSize;
    }
    return Integer.MAX_VALUE;
  }

  public void setMaxFieldSize(int max) {
    maxFieldSize = max;
  }

  public int getMaxRows() {
    if (maxRows != null) {
      return maxRows;
    }
    return Integer.MAX_VALUE;
  }

  public void setMaxRows(int max) {
    maxRows = max;
  }

  public void setEscapeProcessing(boolean enable) throws SQLException {
    throw new NotImplementedException();
  }

  public int getQueryTimeout() {
    return 0;
  }

  public void setQueryTimeout(int seconds) throws SQLException {
    throw new NotImplementedException();
  }

  public void cancel() throws SQLException {
    throw new NotImplementedException();
  }

  public SQLWarning getWarnings() throws SQLException {
    throw new NotImplementedException();
  }

  public void clearWarnings() throws SQLException {
    throw new NotImplementedException();
  }

  public void setCursorName(String name) throws SQLException {
    throw new NotImplementedException();
  }

  public ResultSet getResultSet() throws SQLException {
    throw new NotImplementedException();
  }

  public int getUpdateCount() throws SQLException {
    throw new NotImplementedException();
  }

  public boolean getMoreResults() throws SQLException {
    throw new NotImplementedException();
  }

  public void setFetchDirection(int direction) {
    fetchDirection = direction;
  }

  public int getFetchDirection() {
    if (fetchDirection != null) {
      return fetchDirection;
    }
    return ResultSet.FETCH_FORWARD;
  }

  public void setFetchSize(int rows) {
    fetchSize = rows;
  }

  public int getFetchSize() {
    if (fetchSize != null) {
      return fetchSize;
    }
    return 0;
  }

  public int getResultSetConcurrency() throws SQLException {
    throw new NotImplementedException();
  }

  public int getResultSetType() throws SQLException {
    throw new NotImplementedException();
  }

  public void addBatch(String sql) throws SQLException {
    throw new NotImplementedException();
  }

  public void addBatch() throws SQLException {
    try {
      if (InsertStatementHandler.getBatch().get() == null) {
        InsertStatementHandler.getBatch().set(new ArrayList<>());
      }
      String normalizedSQL = sql.trim().toLowerCase();
      if (!normalizedSQL.startsWith("insert")) {
        int pos = normalizedSQL.indexOf(' ');
        String verb = normalizedSQL.substring(0, pos);
        throw new DatabaseException("Verb not supported by batch operations: verb=" + verb);
      }
      databaseClient.executeQuery(dbName, sql, parms, restrictToThisServer, procedureContext, disableStats);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public void clearBatch() {
    InsertStatementHandler.getBatch().set(null);
  }

  public int[] executeBatch() throws SQLException {
    try {
      InsertStatementHandler handler = (InsertStatementHandler) databaseClient.getStatementHandlerFactory().getHandler(new Insert());
      return handler.executeBatch();
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public Connection getConnection() {
    return connectionProxy;
  }

  public boolean getMoreResults(int current) throws SQLException {
    throw new NotImplementedException();
  }

  public ResultSet getGeneratedKeys() throws SQLException {
    throw new NotImplementedException();
  }

  public ResultSet executeQuery() throws SQLException {
    try {
      ResultSetImpl ret = (ResultSetImpl) databaseClient.executeQuery(dbName, sql, parms, restrictToThisServer, procedureContext, disableStats);
      return new ResultSetProxy(ret);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public ResultSet executeQuery(String sql) throws SQLException {
    try {
      ResultSetImpl ret = (ResultSetImpl)databaseClient.executeQuery(dbName, sql, parms, restrictToThisServer, procedureContext, disableStats);
      return new ResultSetProxy(ret);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public int executeUpdate() throws SQLException {
    try {
      return (Integer) databaseClient.executeQuery(dbName,  sql, parms, restrictToThisServer, procedureContext, disableStats);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public int executeUpdate(String sql) throws SQLException {
    try {
      return (Integer) databaseClient.executeQuery(dbName, sql, parms, restrictToThisServer, procedureContext, disableStats);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    throw new NotImplementedException();
  }

  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    throw new NotImplementedException();
  }

  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    throw new NotImplementedException();
  }

  public void doUpdate(Long sequence0, Long sequence1, Short sequence2, boolean disableStats) throws SQLException {
    databaseClient.executeQuery(dbName, sql, parms, sequence0, sequence1, sequence2, restrictToThisServer, procedureContext, disableStats);
  }

  public void doDelete(Long sequence0, Long sequence1, Short sequence2, boolean disableStats) throws SQLException {
    databaseClient.executeQuery(dbName, sql, parms, sequence0, sequence1, sequence2, restrictToThisServer, procedureContext, disableStats);
  }

  public boolean execute() throws SQLException {
    try {
      int ret = (int)(Integer) databaseClient.executeQuery(dbName, sql, parms, restrictToThisServer, procedureContext, disableStats);
      return ret > 0;
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public boolean execute(String sql) throws SQLException {
    try {
      int ret = (Integer) databaseClient.executeQuery(dbName, sql, parms, restrictToThisServer, procedureContext, disableStats);
      return ret > 0;
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    try {
      int ret = (Integer) databaseClient.executeQuery(dbName, sql, parms, restrictToThisServer, procedureContext, disableStats);
      return ret > 0;
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    try {
      int ret = (Integer) databaseClient.executeQuery(dbName, sql, parms, restrictToThisServer, procedureContext, disableStats);
      return ret > 0;
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public boolean execute(String sql, String[] columnNames) throws SQLException {
    try {
      int ret = (Integer) databaseClient.executeQuery(dbName, sql, parms, restrictToThisServer, procedureContext, disableStats);
      return ret > 0;
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }


  public int getResultSetHoldability() throws SQLException {
    throw new NotImplementedException();
  }

  public boolean isClosed() throws SQLException {
    throw new NotImplementedException();
  }

  public void setPoolable(boolean poolable) throws SQLException {
    throw new NotImplementedException();
  }

  public boolean isPoolable() throws SQLException {
    throw new NotImplementedException();
  }

  public void closeOnCompletion() throws SQLException {
    throw new NotImplementedException();
  }

  public boolean isCloseOnCompletion() {
    return false;
  }

  public <T> T unwrap(Class<T> iface) {
    return (T) this;
  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new NotImplementedException();
  }

  public void setNull(int parameterIndex, int sqlType) throws SQLException {
    try {
      parms.setNull(parameterIndex, sqlType);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public void setBoolean(int parameterIndex, boolean x) throws SQLException {
    try {
      parms.setBoolean(parameterIndex, x);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public void setByte(int parameterIndex, byte x) throws SQLException {
    try {
      parms.setByte(parameterIndex, x);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public void setShort(int parameterIndex, short x) throws SQLException {
    try {
      parms.setShort(parameterIndex, x);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public void setInt(int parameterIndex, int x) throws SQLException {
    try {
      parms.setInt(parameterIndex, x);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public void setLong(int parameterIndex, long x) throws SQLException {
    try {
      parms.setLong(parameterIndex, x);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public void setFloat(int parameterIndex, float x) throws SQLException {
    try {
      parms.setFloat(parameterIndex, x);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public void setDouble(int parameterIndex, double x) throws SQLException {
    try {
      parms.setDouble(parameterIndex, x);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
    try {
      parms.setBigDecimal(parameterIndex, x);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public void setString(int parameterIndex, String x) throws SQLException {
    try {
      parms.setString(parameterIndex, x);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public void setBytes(int parameterIndex, byte[] x) throws SQLException {
    try {
      parms.setBytes(parameterIndex, x);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public void setDate(int parameterIndex, Date x) throws SQLException {
    try {
      parms.setDate(parameterIndex, x);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public void setTime(int parameterIndex, Time x) throws SQLException {
    try {
      parms.setTime(parameterIndex, x);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
    try {
      parms.setTimestamp(parameterIndex, x);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
    try {
      parms.setAsciiStream(parameterIndex, x, length);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
    try {
      parms.setUnicodeStream(parameterIndex, x, length);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
    try {
      parms.setBinaryStream(parameterIndex, x, length);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
    throw new SQLException(NOT_SUPPORTED_STR);
  }

  public void setObject(int parameterIndex, Object x) throws SQLException {
    throw new SQLException(NOT_SUPPORTED_STR);
  }

  public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
    try {
      parms.setCharacterStream(parameterIndex, reader, length);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public void setRef(int parameterIndex, Ref x) throws SQLException {
    throw new SQLException(NOT_SUPPORTED_STR);
  }

  public void setBlob(int parameterIndex, Blob x) throws SQLException {
    try {
      parms.setBlob(parameterIndex, x);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public void setClob(int parameterIndex, Clob x) throws SQLException {
    try {
      parms.setClob(parameterIndex, x);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public void setArray(int parameterIndex, Array x) throws SQLException {
    throw new SQLException(NOT_SUPPORTED_STR);
  }

  public ResultSetMetaData getMetaData() throws SQLException {
    throw new NotImplementedException();
  }

  public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
    throw new SQLException(NOT_SUPPORTED_STR);
  }

  public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
    throw new SQLException(NOT_SUPPORTED_STR);
  }

  public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
    throw new SQLException(NOT_SUPPORTED_STR);
  }

  public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
    try {
      parms.setNull(parameterIndex, sqlType, typeName);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public void setURL(int parameterIndex, URL x) throws SQLException {
    throw new SQLException(NOT_SUPPORTED_STR);
  }

  public ParameterMetaData getParameterMetaData() throws SQLException {
    throw new NotImplementedException();
  }

  public void setRowId(int parameterIndex, RowId x) throws SQLException {
    throw new SQLException(NOT_SUPPORTED_STR);
  }

  public void setNString(int parameterIndex, String value) throws SQLException {
    try {
      parms.setNString(parameterIndex, value);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
    try {
      parms.setNCharacterStream(parameterIndex, value, length);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public void setNClob(int parameterIndex, NClob value) throws SQLException {
    try {
      parms.setNClob(parameterIndex, value);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
    try {
      parms.setClob(parameterIndex, reader, length);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
    try {
      parms.setBlob(parameterIndex, inputStream, length);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
    try {
      parms.setNClob(parameterIndex, reader, length);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
    throw new SQLException(NOT_SUPPORTED_STR);
  }

  public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
    throw new SQLException(NOT_SUPPORTED_STR);
  }

  public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
    try {
      parms.setAsciiStream(parameterIndex, x, (int)length);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
    try {
      parms.setBinaryStream(parameterIndex, x, (int)length);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
    try {
      parms.setCharacterStream(parameterIndex, reader, (int)length);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
    try {
      parms.setAsciiStream(parameterIndex, x);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
    try {
      parms.setBinaryStream(parameterIndex, x);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
    try {
      parms.setCharacterStream(parameterIndex, reader);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
    try {
      parms.setNCharacterStream(parameterIndex, value);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public void setClob(int parameterIndex, Reader reader) throws SQLException {
    try {
      parms.setClob(parameterIndex, reader);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
    try {
      parms.setBlob(parameterIndex, inputStream);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public void setNClob(int parameterIndex, Reader reader) throws SQLException {
    try {
      parms.setNClob(parameterIndex, reader);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public ParameterHandler getParms() {
    return parms;
  }

  public void restrictToThisServer(boolean restrict) {
    this.restrictToThisServer = restrict;
  }

  public void setProcedureContext(StoredProcedureContextImpl procedureContext) {
    this.procedureContext = procedureContext;
  }

  public void setParms(ParameterHandler parms) {
    this.parms = parms;
  }

  public String getSql() {
    return sql;
  }
}


