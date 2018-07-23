package com.sonicbase.procedure;

import com.sonicbase.jdbcdriver.StatementProxy;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;

public class SonicBasePreparedStatementImpl implements SonicBasePreparedStatement {

  private final PreparedStatement proxy;
  private final StoredProcedureContextImpl context;
  private boolean restrictToThisServer;


  public SonicBasePreparedStatementImpl(StoredProcedureContext context, PreparedStatement preparedStatement) {
    this.proxy = preparedStatement;
    this.context = (StoredProcedureContextImpl) context;
  }

  @Override
  public void restrictToThisServer(boolean restrict) {
    this.restrictToThisServer = restrict;
    ((StatementProxy)proxy).restrictToThisServer(restrict);
  }

  @Override
  public boolean isRestrictedToThisServer() {
    return restrictToThisServer;
  }

  @Override
  public ResultSet executeQueryWithEvaluator(RecordEvaluator evaluator) throws SQLException {
    context.setRecordEvaluator(evaluator);
    ((StatementProxy)proxy).setProcedureContext(context);
    return proxy.executeQuery();
  }

  @Override
  public ResultSet executeQueryWithEvaluator(String evaluatorClassName) throws SQLException {
    return proxy.executeQuery();
  }


  @Override
  public ResultSet executeQuery() throws SQLException {
    return proxy.executeQuery();
  }

  @Override
  public int executeUpdate() throws SQLException {
    return proxy.executeUpdate();
  }

  @Override
  public void setNull(int parameterIndex, int sqlType) throws SQLException {
    proxy.setNull(parameterIndex, sqlType);
  }

  @Override
  public void setBoolean(int parameterIndex, boolean x) throws SQLException {
    proxy.setBoolean(parameterIndex, x);
  }

  @Override
  public void setByte(int parameterIndex, byte x) throws SQLException {
    proxy.setByte(parameterIndex, x);
  }

  @Override
  public void setShort(int parameterIndex, short x) throws SQLException {
    proxy.setShort(parameterIndex, x);
  }

  @Override
  public void setInt(int parameterIndex, int x) throws SQLException {
    proxy.setInt(parameterIndex, x);
  }

  @Override
  public void setLong(int parameterIndex, long x) throws SQLException {
    proxy.setLong(parameterIndex, x);
  }

  @Override
  public void setFloat(int parameterIndex, float x) throws SQLException {
    proxy.setFloat(parameterIndex, x);
  }

  @Override
  public void setDouble(int parameterIndex, double x) throws SQLException {
    proxy.setDouble(parameterIndex, x);
  }

  @Override
  public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
    proxy.setBigDecimal(parameterIndex, x);
  }

  @Override
  public void setString(int parameterIndex, String x) throws SQLException {
    proxy.setString(parameterIndex, x);
  }

  @Override
  public void setBytes(int parameterIndex, byte[] x) throws SQLException {
    proxy.setBytes(parameterIndex, x);
  }

  @Override
  public void setDate(int parameterIndex, Date x) throws SQLException {
    proxy.setDate(parameterIndex, x);
  }

  @Override
  public void setTime(int parameterIndex, Time x) throws SQLException {
    proxy.setTime(parameterIndex, x);
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
    proxy.setTimestamp(parameterIndex, x);
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
    proxy.setAsciiStream(parameterIndex, x, length);
  }

  @Override
  public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
    proxy.setCharacterStream(parameterIndex, new InputStreamReader(x), length);
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
    proxy.setBinaryStream(parameterIndex, x, length);
  }

  @Override
  public void clearParameters() throws SQLException {
    proxy.clearParameters();
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
    proxy.setObject(parameterIndex, x, targetSqlType);
  }

  @Override
  public void setObject(int parameterIndex, Object x) throws SQLException {
    proxy.setObject(parameterIndex, x);
  }

  @Override
  public boolean execute() throws SQLException {
    return proxy.execute();
  }

  @Override
  public void addBatch() throws SQLException {
    proxy.addBatch();
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
    proxy.setCharacterStream(parameterIndex, reader, length);
  }

  @Override
  public void setRef(int parameterIndex, Ref x) throws SQLException {
    proxy.setRef(parameterIndex, x);
  }

  @Override
  public void setBlob(int parameterIndex, Blob x) throws SQLException {
    proxy.setBlob(parameterIndex, x);
  }

  @Override
  public void setClob(int parameterIndex, Clob x) throws SQLException {
    proxy.setClob(parameterIndex, x);
  }

  @Override
  public void setArray(int parameterIndex, Array x) throws SQLException {
    proxy.setArray(parameterIndex, x);
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return proxy.getMetaData();
  }

  @Override
  public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
    proxy.setDate(parameterIndex, x, cal);
  }

  @Override
  public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
    proxy.setTime(parameterIndex, x, cal);
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
    proxy.setTimestamp(parameterIndex, x, cal);
  }

  @Override
  public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
    proxy.setNull(parameterIndex, sqlType, typeName);
  }

  @Override
  public void setURL(int parameterIndex, URL x) throws SQLException {
    proxy.setURL(parameterIndex, x);
  }

  @Override
  public ParameterMetaData getParameterMetaData() throws SQLException {
    return proxy.getParameterMetaData();
  }

  @Override
  public void setRowId(int parameterIndex, RowId x) throws SQLException {
    proxy.setRowId(parameterIndex, x);
  }

  @Override
  public void setNString(int parameterIndex, String value) throws SQLException {
    proxy.setNString(parameterIndex, value);
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
    proxy.setNCharacterStream(parameterIndex, value, length);
  }

  @Override
  public void setNClob(int parameterIndex, NClob value) throws SQLException {
    proxy.setNClob(parameterIndex, value);
  }

  @Override
  public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
    proxy.setClob(parameterIndex, reader, length);
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
    proxy.setBlob(parameterIndex, inputStream, length);
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
    proxy.setNClob(parameterIndex, reader, length);
  }

  @Override
  public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
    proxy.setSQLXML(parameterIndex, xmlObject);
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
    proxy.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
    proxy.setAsciiStream(parameterIndex, x, length);
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
    proxy.setBinaryStream(parameterIndex, x, length);
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
    proxy.setCharacterStream(parameterIndex, reader, length);
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
    proxy.setAsciiStream(parameterIndex, x);
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
    proxy.setBinaryStream(parameterIndex, x);
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
    proxy.setCharacterStream(parameterIndex, reader);
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
    proxy.setNCharacterStream(parameterIndex, value);
  }

  @Override
  public void setClob(int parameterIndex, Reader reader) throws SQLException {
    proxy.setClob(parameterIndex, reader);
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
    proxy.setBlob(parameterIndex, inputStream);
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader) throws SQLException {
    proxy.setNClob(parameterIndex, reader);
  }

  @Override
  public void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
    proxy.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
  }

  @Override
  public void setObject(int parameterIndex, Object x, SQLType targetSqlType) throws SQLException {
    proxy.setObject(parameterIndex, x, targetSqlType);
  }

  @Override
  public long executeLargeUpdate() throws SQLException {
    return proxy.executeUpdate();
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    return proxy.executeQuery(sql);
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    return proxy.executeUpdate(sql);
  }

  @Override
  public void close() throws SQLException {
    proxy.close();
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    return proxy.getMaxFieldSize();
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException {
    proxy.setMaxFieldSize(max);
  }

  @Override
  public int getMaxRows() throws SQLException {
    return proxy.getMaxRows();
  }

  @Override
  public void setMaxRows(int max) throws SQLException {
    proxy.setMaxRows(max);
  }

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {
    proxy.setEscapeProcessing(enable);
  }

  @Override
  public int getQueryTimeout() throws SQLException {
    return proxy.getQueryTimeout();
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {
    proxy.setQueryTimeout(seconds);
  }

  @Override
  public void cancel() throws SQLException {
    proxy.cancel();
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return proxy.getWarnings();
  }

  @Override
  public void clearWarnings() throws SQLException {
    proxy.clearWarnings();
  }

  @Override
  public void setCursorName(String name) throws SQLException {
    proxy.setCursorName(name);
  }

  @Override
  public boolean execute(String sql) throws SQLException {
    return proxy.execute(sql);
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    return proxy.getResultSet();
  }

  @Override
  public int getUpdateCount() throws SQLException {
    return proxy.getUpdateCount();
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    return proxy.getMoreResults();
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    proxy.setFetchDirection(direction);
  }

  @Override
  public int getFetchDirection() throws SQLException {
    return proxy.getFetchDirection();
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    proxy.setFetchSize(rows);
  }

  @Override
  public int getFetchSize() throws SQLException {
    return proxy.getFetchSize();
  }

  @Override
  public int getResultSetConcurrency() throws SQLException {
    return proxy.getResultSetConcurrency();
  }

  @Override
  public int getResultSetType() throws SQLException {
    return proxy.getResultSetType();
  }

  @Override
  public void addBatch(String sql) throws SQLException {
    proxy.addBatch(sql);
  }

  @Override
  public void clearBatch() throws SQLException {
    proxy.clearBatch();
  }

  @Override
  public int[] executeBatch() throws SQLException {
    return proxy.executeBatch();
  }

  @Override
  public Connection getConnection() throws SQLException {
    return proxy.getConnection();
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException {
    return proxy.getMoreResults(current);
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    return proxy.getGeneratedKeys();
  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    return proxy.executeUpdate(sql, autoGeneratedKeys);
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    return proxy.executeUpdate(sql, columnIndexes);
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    return proxy.executeUpdate(sql, columnNames);
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    return proxy.execute(sql, autoGeneratedKeys);
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    return proxy.execute(sql, columnIndexes);
  }

  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException {
    return proxy.execute(sql, columnNames);
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    return proxy.getResultSetHoldability();
  }

  @Override
  public boolean isClosed() throws SQLException {
    return proxy.isClosed();
  }

  @Override
  public void setPoolable(boolean poolable) throws SQLException {
    proxy.setPoolable(poolable);
  }

  @Override
  public boolean isPoolable() throws SQLException {
    return proxy.isPoolable();
  }

  @Override
  public void closeOnCompletion() throws SQLException {
    proxy.closeOnCompletion();
  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    return proxy.isCloseOnCompletion();
  }

  @Override
  public long getLargeUpdateCount() throws SQLException {
    return proxy.getLargeUpdateCount();
  }

  @Override
  public void setLargeMaxRows(long max) throws SQLException {
    proxy.setLargeMaxRows(max);
  }

  @Override
  public long getLargeMaxRows() throws SQLException {
    return proxy.getLargeMaxRows();
  }

  @Override
  public long[] executeLargeBatch() throws SQLException {
    return proxy.executeLargeBatch();
  }

  @Override
  public long executeLargeUpdate(String sql) throws SQLException {
    return proxy.executeLargeUpdate(sql);
  }

  @Override
  public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    return proxy.executeLargeUpdate(sql, autoGeneratedKeys);
  }

  @Override
  public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
    return proxy.executeLargeUpdate(sql, columnIndexes);
  }

  @Override
  public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
    return proxy.executeLargeUpdate(sql, columnNames);
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return proxy.unwrap(iface);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return proxy.isWrapperFor(iface);
  }
}
