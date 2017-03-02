package com.sonicbase.jdbcdriver;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.query.impl.ResultSetImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: lowryda
 * Date: Oct 7, 2011
 * Time: 3:18:32 PM
 */
public class StatementProxy extends ParameterHandler implements java.sql.Statement, PreparedStatement {

  public static Logger LOGGER = LoggerFactory.getLogger(StatementProxy.class);
  private final String dbName;

  private String sql;
  private DatabaseClient databaseClient;
  private ConnectionProxy connectionProxy;

  private Integer maxFieldSize;
  private Integer maxRows;
  private Integer fetchDirection;
  private Integer fetchSize;
  private ParameterHandler parms = new ParameterHandler();

  public StatementProxy(ConnectionProxy connectionProxy, DatabaseClient databaseClient, String sql) throws SQLException {
    this.connectionProxy = connectionProxy;
    this.databaseClient = databaseClient;
    this.sql = sql;
    this.dbName = connectionProxy.getDbName();

    DatabaseClient.batchWithRecordOutputStreams.set(null);
    DatabaseClient.batchWithoutRecordOutputStreams.set(null);
    DatabaseClient.batchWithRecordByteOutputStreams.set(null);
    DatabaseClient.batchWithoutRecordByteOutputStreams.set(null);
  }

  public void close() throws SQLException {
  }

  public int getMaxFieldSize() throws SQLException {
    if (maxFieldSize != null) {
      return maxFieldSize;
    }
    return Integer.MAX_VALUE;
  }

  public void setMaxFieldSize(int max) throws SQLException {
    maxFieldSize = max;
  }

  public int getMaxRows() throws SQLException {
    if (maxRows != null) {
      return maxRows;
    }
    return Integer.MAX_VALUE;
  }

  public void setMaxRows(int max) throws SQLException {
    maxRows = max;
  }

  public void setEscapeProcessing(boolean enable) throws SQLException {
  }

  public int getQueryTimeout() throws SQLException {
    return 0;
  }

  public void setQueryTimeout(int seconds) throws SQLException {
  }

  public void cancel() throws SQLException {
    //todo: implement
    throw new NotImplementedException();
  }

  public SQLWarning getWarnings() throws SQLException {
    return null;
  }

  public void clearWarnings() throws SQLException {
    throw new NotImplementedException();
    //todo: implement
  }

  public void setCursorName(String name) throws SQLException {
    throw new NotImplementedException();
    //todo: implement
  }

  public ResultSet getResultSet() throws SQLException {
    throw new NotImplementedException();
    //todo: implement
  }

  public int getUpdateCount() throws SQLException {
    //todo: implement
    throw new NotImplementedException();
  }

  public boolean getMoreResults() throws SQLException {
    //todo: implement
    throw new NotImplementedException();
  }

  public void setFetchDirection(int direction) throws SQLException {
    fetchDirection = direction;
  }

  public int getFetchDirection() throws SQLException {
    if (fetchDirection != null) {
      return fetchDirection;
    }
    return ResultSet.FETCH_FORWARD;
  }

  public void setFetchSize(int rows) throws SQLException {
    fetchSize = rows;
  }

  public int getFetchSize() throws SQLException {
    if (fetchSize != null) {
      return fetchSize;
    }
    return 0;
  }

  public int getResultSetConcurrency() throws SQLException {
    //todo: implement
    throw new NotImplementedException();
  }

  public int getResultSetType() throws SQLException {
    //todo: implement
    throw new NotImplementedException();
  }

  public void addBatch(String sql) throws SQLException {
  }

  public void addBatch() throws SQLException {
    if (DatabaseClient.batchWithRecordOutputStreams.get() == null) {
      DatabaseClient.batchWithRecordOutputStreams.set(new ArrayList<DataOutputStream>());
      DatabaseClient.batchWithoutRecordOutputStreams.set(new ArrayList<DataOutputStream>());
      DatabaseClient.batchWithRecordByteOutputStreams.set(new ArrayList<ByteArrayOutputStream>());
      DatabaseClient.batchWithoutRecordByteOutputStreams.set(new ArrayList<ByteArrayOutputStream>());
      for (int i = 0; i < databaseClient.getShardCount(); i++) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DatabaseClient.batchWithRecordByteOutputStreams.get().add(bytes);
        DatabaseClient.batchWithRecordOutputStreams.get().add(new DataOutputStream(bytes));

        bytes = new ByteArrayOutputStream();
        DatabaseClient.batchWithoutRecordByteOutputStreams.get().add(bytes);
        DatabaseClient.batchWithoutRecordOutputStreams.get().add(new DataOutputStream(bytes));
      }
    }
    databaseClient.executeQuery(dbName, QueryType.execute0, sql, parms);
  }

  public void clearBatch() throws SQLException {
    DatabaseClient.batchWithRecordOutputStreams.set(null);
    DatabaseClient.batchWithRecordByteOutputStreams.set(null);
    DatabaseClient.batchWithoutRecordOutputStreams.set(null);
    DatabaseClient.batchWithoutRecordOutputStreams.set(null);
  }

  public int[] executeBatch() throws SQLException {
    return databaseClient.executeBatch();
  }

  public Connection getConnection() throws SQLException {
    return connectionProxy;
  }

  public boolean getMoreResults(int current) throws SQLException {
    //todo: implement
    throw new NotImplementedException();
  }

  public ResultSet getGeneratedKeys() throws SQLException {
    //todo: implement
    throw new NotImplementedException();
  }

  public ResultSet executeQuery() throws SQLException {
    ResultSetImpl ret = (ResultSetImpl)databaseClient.executeQuery(dbName, QueryType.query1, sql, parms);
    return new ResultSetProxy(connectionProxy, ret);
  }

  public ResultSet executeQuery(String sql) throws SQLException {
    //return new ResultSetProxy((Record[])databaseClient.executeQuery(QueryType.query1, sql, parms));
    return null;
  }

  public int executeUpdate() throws SQLException {
    return (Integer)databaseClient.executeQuery(dbName, QueryType.update0, sql, parms);
  }

  public int executeUpdate(String sql) throws SQLException {
    //return (Integer)databaseClient.executeQuery(QueryType.update1, sql, parms);
    return 0;
  }

  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    //return (Integer)databaseClient.executeQuery(QueryType.update2, sql, parms);
    return 0;
  }

  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    //return (Integer)databaseClient.executeQuery(QueryType.update3, sql, parms);
    return 0;
  }

  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    //return (Integer)databaseClient.executeQuery(QueryType.update4, sql, parms);
    return 0;
  }

  public boolean execute() throws SQLException {
    return (Integer)databaseClient.executeQuery(dbName, QueryType.execute0, sql, parms) > 0;
  }

  public boolean execute(String sql) throws SQLException {
    return (Integer)databaseClient.executeQuery(dbName, QueryType.execute0, sql, parms) > 0;
  }

  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    return (Integer)databaseClient.executeQuery(dbName, QueryType.execute1, sql, parms) > 0;
  }

  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    return (Integer)databaseClient.executeQuery(dbName, QueryType.execute2, sql, parms) > 0;
  }

  public boolean execute(String sql, String[] columnNames) throws SQLException {
    return (Integer)databaseClient.executeQuery(dbName, QueryType.execute3, sql, parms) > 0;
  }


  public int getResultSetHoldability() throws SQLException {
    //todo: implement
    throw new NotImplementedException();
  }

  public boolean isClosed() throws SQLException {
    //todo: implement
    throw new NotImplementedException();
  }

  public void setPoolable(boolean poolable) throws SQLException {
    //todo: implement
    throw new NotImplementedException();
  }

  public boolean isPoolable() throws SQLException {
    //todo: implement
    throw new NotImplementedException();
  }

  public void closeOnCompletion() throws SQLException {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  public boolean isCloseOnCompletion() throws SQLException {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
    return (T) this;
  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    //todo: implement
    throw new NotImplementedException();
  }

  //######################################################## begin delegated setters and getters;

  public void setNull(int parameterIndex, int sqlType) throws SQLException {
    parms.setNull(parameterIndex, sqlType);
  }

  public void setBoolean(int parameterIndex, boolean x) throws SQLException {
    parms.setBoolean(parameterIndex, x);
  }

  public void setByte(int parameterIndex, byte x) throws SQLException {
    parms.setByte(parameterIndex, x);
  }

  public void setShort(int parameterIndex, short x) throws SQLException {
    parms.setShort(parameterIndex, x);
  }

  public void setInt(int parameterIndex, int x) throws SQLException {
    parms.setInt(parameterIndex, x);
  }

  public void setLong(int parameterIndex, long x) throws SQLException {
    parms.setLong(parameterIndex, x);
  }

  public void setFloat(int parameterIndex, float x) throws SQLException {
    parms.setFloat(parameterIndex, x);
  }

  public void setDouble(int parameterIndex, double x) throws SQLException {
    parms.setDouble(parameterIndex, x);
  }

  public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
    parms.setBigDecimal(parameterIndex, x);
  }

  public void setString(int parameterIndex, String x) throws SQLException {
    parms.setString(parameterIndex, x);
  }

  public void setBytes(int parameterIndex, byte[] x) throws SQLException {
    parms.setBytes(parameterIndex, x);
  }

  public void setDate(int parameterIndex, Date x) throws SQLException {
    parms.setDate(parameterIndex, x);
  }

  public void setTime(int parameterIndex, Time x) throws SQLException {
    parms.setTime(parameterIndex, x);
  }

  public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
    parms.setTimestamp(parameterIndex, x);
  }

  public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
    parms.setAsciiStream(parameterIndex, x, length);
  }

  public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
    parms.setUnicodeStream(parameterIndex, x, length);
  }

  public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
    parms.setBinaryStream(parameterIndex, x, length);
  }

  public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
    parms.setObject(parameterIndex, x, targetSqlType);
  }

  public void setObject(int parameterIndex, Object x) throws SQLException {
    parms.setObject(parameterIndex, x);
  }

  public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
    parms.setCharacterStream(parameterIndex, reader, length);
  }

  public void setRef(int parameterIndex, Ref x) throws SQLException {
    parms.setRef(parameterIndex, x);
  }

  public void setBlob(int parameterIndex, Blob x) throws SQLException {
    parms.setBlob(parameterIndex, x);
  }

  public void setClob(int parameterIndex, Clob x) throws SQLException {
    parms.setClob(parameterIndex, x);
  }

  public void setArray(int parameterIndex, Array x) throws SQLException {
    parms.setArray(parameterIndex, x);
  }

  public ResultSetMetaData getMetaData() throws SQLException {
    return null;
  }

  public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
    parms.setDate(parameterIndex, x, cal);
  }

  public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
    parms.setTime(parameterIndex, x, cal);
  }

  public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
    parms.setTimestamp(parameterIndex, x, cal);
  }

  public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
    parms.setNull(parameterIndex, sqlType, typeName);
  }

  public void setURL(int parameterIndex, URL x) throws SQLException {
    parms.setURL(parameterIndex, x);
  }

  public ParameterMetaData getParameterMetaData() throws SQLException {
    return null;
  }

  public void setRowId(int parameterIndex, RowId x) throws SQLException {
    parms.setRowId(parameterIndex, x);
  }

  public void setNString(int parameterIndex, String value) throws SQLException {
    parms.setNString(parameterIndex, value);
  }

  public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
    parms.setNCharacterStream(parameterIndex, value, length);
  }

  public void setNClob(int parameterIndex, NClob value) throws SQLException {
    parms.setNClob(parameterIndex, value);
  }

  public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
    parms.setClob(parameterIndex, reader, length);
  }

  public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
    parms.setBlob(parameterIndex, inputStream, length);
  }

  public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
    parms.setNClob(parameterIndex, reader, length);
  }

  public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
    parms.setSQLXML(parameterIndex, xmlObject);
  }

  public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
    parms.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
  }

  public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
    parms.setAsciiStream(parameterIndex, x, length);
  }

  public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
    parms.setBinaryStream(parameterIndex, x, length);
  }

  public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
    parms.setCharacterStream(parameterIndex, reader, length);
  }

  public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
    parms.setAsciiStream(parameterIndex, x);
  }

  public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
    parms.setBinaryStream(parameterIndex, x);
  }

  public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
    parms.setCharacterStream(parameterIndex, reader);
  }

  public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
    parms.setNCharacterStream(parameterIndex, value);
  }

  public void setClob(int parameterIndex, Reader reader) throws SQLException {
    parms.setClob(parameterIndex, reader);
  }

  public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
    parms.setBlob(parameterIndex, inputStream);
  }

  public void setNClob(int parameterIndex, Reader reader) throws SQLException {
    parms.setNClob(parameterIndex, reader);
  }

  public ParameterHandler getParms() {
    return parms;
  }
}


