/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.jdbc;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.TestUtils;
import org.testng.annotations.Test;

import java.sql.*;
import java.util.Properties;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class ConnectionProxyTest {

  @Test
  public void test() throws SQLException {
    ConnectionProxy connWithClient = new ConnectionProxy("mock", (Properties)null);
    DatabaseClient standaloneClient = mock(DatabaseClient.class);
    connWithClient.setClient(standaloneClient);
    final DatabaseCommon common = new DatabaseCommon();

    TableSchema tableSchema = TestUtils.createTable();
    common.getTables("test").put(tableSchema.getName(), tableSchema);
    common.setSchemaVersion(100);
    when(standaloneClient.getCommon()).thenReturn(common);
    doAnswer(invocationOnMock -> {
      common.addDatabase("db");
      return null;
    }).when(standaloneClient).createDatabase(anyString());
    when(standaloneClient.isRestoreComplete()).thenReturn(true);
    when(standaloneClient.isBackupComplete()).thenReturn(true);
    when(standaloneClient.getReplicaCount()).thenReturn(1);
    when(standaloneClient.getShardCount()).thenReturn(1);
    when(standaloneClient.send(anyString(), anyInt(), anyInt(), anyObject(), anyObject())).
        thenReturn(new byte[]{123});
    when(standaloneClient.send(anyString(), anyInt(), anyInt(), anyObject(), anyObject(), anyBoolean())).
        thenReturn(new byte[]{125});
    when(standaloneClient.sendToMaster(anyObject())).
        thenReturn(new byte[]{11});
    connWithClient.createDatabase("db");

    assertTrue(connWithClient.databaseExists("db"));


    ConnectionProxy conn = new ConnectionProxy("mock", (Properties)null);
    DatabaseClient client = mock(DatabaseClient.class);
    conn.addClient("mock", client);
    conn.setClient(client);
    when(client.getCommon()).thenReturn(common);
    doAnswer(invocationOnMock -> {
      common.addDatabase("db");
      return null;
    }).when(client).createDatabase(anyString());
    when(client.isRestoreComplete()).thenReturn(true);
    when(client.isBackupComplete()).thenReturn(true);
    when(client.getReplicaCount()).thenReturn(1);
    when(client.getShardCount()).thenReturn(1);
    when(client.send(anyString(), anyInt(), anyInt(), anyObject(), anyObject())).
      thenReturn(new byte[]{124});
    when(client.send(anyString(), anyInt(), anyInt(), anyObject(), anyObject(), anyBoolean())).
        thenReturn(new byte[]{126});
    when(client.sendToMaster(anyObject())).
        thenReturn(new byte[]{12});

    conn.createDatabase("db");

    assertTrue(connWithClient.databaseExists("db"));

    assertEquals(connWithClient.getDatabaseClient(), standaloneClient);
    assertEquals(conn.getDatabaseClient(), client);

    assertEquals(connWithClient.getReplicaCount(), 1);
    assertEquals(conn.getReplicaCount(), 1);
    assertEquals(connWithClient.getShardCount(), 1);
    assertEquals(conn.getShardCount(), 1);

    assertEquals(connWithClient.getSchemaVersion(), 100);
    assertEquals(conn.getSchemaVersion(), 100);


    assertEquals(connWithClient.send(null, 0, 0, null, ConnectionProxy.Replica.def), new byte[]{123});
    assertEquals(conn.send(null, 0, 0, null, ConnectionProxy.Replica.def), new byte[]{124});

    assertEquals(connWithClient.send(null, 0, 0, null, ConnectionProxy.Replica.def, true), new byte[]{125});
    assertEquals(conn.send(null, 0, 0, null, ConnectionProxy.Replica.def, true), new byte[]{126});

    assertEquals(connWithClient.getTables("test").get("table1").getName(), "table1");
    assertEquals(conn.getTables("test").get("table1").getName(), "table1");

    assertEquals(connWithClient.sendToMaster(null), new byte[]{11});
    assertEquals(conn.sendToMaster(null), new byte[]{12});

    connWithClient.checkClosed();
    conn.checkClosed();

    try {
      connWithClient.nativeSQL("select * from persons");
      fail();
    }
    catch (SQLException e) {
    }
//    public Statement createStatement() throws SQLException {
//      try {
//        return new StatementProxy(this, clients.get(url).client, null);
//      }
//      catch (Exception e) {
//        throw new SQLException(e);
//      }
//    }
//
//    public void beginExplicitTransaction(String dbName) throws SQLException {
//      try {
//        if (client != null) {
//          client.beginExplicitTransaction(dbName);
//        }
//        else {
//          clients.get(url).client.beginExplicitTransaction(dbName);
//        }
//      }
//      catch (Exception e) {
//        throw new SQLException(e);
//      }
//    }
//
//    public boolean getAutoCommit() throws SQLException {
//      return autoCommit;
//    }
//
//    public void commit() throws SQLException {
//      try {
//        if (client != null) {
//          client.commit(dbName, null);
//        }
//        else {
//          clients.get(url).client.commit(dbName, null);
//        }
//        autoCommit = true;
//      }
//      catch (Exception e) {
//        throw new SQLException(e);
//      }
//    }
//
//    public void rollback() throws SQLException {
//      try {
//        if (client != null) {
//          client.rollback(dbName);
//        }
//        else {
//          clients.get(url).client.rollback(dbName);
//        }
//        autoCommit = true;
//      }
//      catch (Exception e) {
//        throw new SQLException(e);
//      }
//    }
//
//    public String nativeSQL(String sql) throws SQLException {
//      throw new NotImplementedException();
//    }
//
//    public void setAutoCommit(boolean autoCommit) throws SQLException {
//      try {
//        this.autoCommit = autoCommit;
//        if (!autoCommit) {
//          beginExplicitTransaction(dbName);
//        }
//      }
//      catch (Exception e) {
//        throw new SQLException(e);
//      }
//    }
//
//    public void close() throws SQLException {
//      try {
//        if (closed) {
//          throw new SQLException("Attempting to close a connection that is already closed");
//        }
//
//        closed = true;
//        ConnectionProxy.ClientEntry entry = clients.get(url);
//        if (entry != null && entry.refCount.decrementAndGet() == 0) {
//          entry.client.shutdown();
//          clients.remove(url);
//        }
//        if (client != null) {
//          client.shutdown();
//        }
//      }
//      catch (Exception e) {
//        throw new SQLException(e);
//      }
//    }
//
//    public boolean isClosed() throws SQLException {
//      return closed;
//    }
//
//    public DatabaseMetaData getMetaData() throws SQLException {
//      throw new NotImplementedException();
//    }
//
//    public void setReadOnly(boolean readOnly) throws SQLException {
//      throw new NotImplementedException();
//    }
//
//    public boolean isReadOnly() throws SQLException {
//      return false;
//    }
//
//    public void setCatalog(String catalog) throws SQLException {
//      throw new NotImplementedException();
//    }
//
//    public String getCatalog() throws SQLException {
//      throw new NotImplementedException();
//    }
//
//    public void setTransactionIsolation(int level) throws SQLException {
//      throw new NotImplementedException();
//    }
//
//    public int getTransactionIsolation() throws SQLException {
//      return TRANSACTION_READ_COMMITTED;
//    }
//
//    public SQLWarning getWarnings() throws SQLException {
//      try {
//        checkClosed();
//        StringBuilder ret = new StringBuilder();
//        if (ret.length() == 0) {
//          return null;
//        }
//        return new SQLWarning(ret.toString());
//      }
//      catch (Exception e) {
//        throw new SQLException(e);
//      }
//    }
//
//    public void clearWarnings() throws SQLException {
//      try {
//        checkClosed();
//      }
//      catch (Exception e) {
//        throw new SQLException(e);
//      }
//    }
//
//    public PreparedStatement prepareStatement(String sql) throws SQLException {
//      try {
//        if (client != null) {
//          return new StatementProxy(this, client, sql);
//        }
//        return new StatementProxy(this, clients.get(url).client, sql);
//      }
//      catch (Exception e) {
//        throw new SQLException(e);
//      }
//    }
//
//    public CallableStatement prepareCall(String sql) throws SQLException {
//      throw new SQLException("not supported");
//    }
//
//    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
//      try {
//        if (client != null) {
//          return new StatementProxy(this, client, null);
//        }
//        return new StatementProxy(this, clients.get(url).client, null);
//      }
//      catch (Exception e) {
//        throw new SQLException(e);
//      }
//    }
//
//    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
//      throw new NotImplementedException();
//    }
//
//    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
//      throw new NotImplementedException();
//    }
//
//    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
//      throw new SQLException("not supported");
//    }
//
//    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
//      throw new NotImplementedException();
//    }
//
//    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
//      throw new NotImplementedException();
//    }
//
//    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
//      throw new NotImplementedException();
//    }
//
//    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
//      try {
//        if (client != null) {
//          return new StatementProxy(this, client, sql);
//        }
//        return new StatementProxy(this, clients.get(url).client, sql);
//      }
//      catch (Exception e) {
//        throw new SQLException(e);
//      }
//    }
//
//    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
//      throw new SQLException("not supported");
//    }
//
//    public Map<String, Class<?>> getTypeMap() throws SQLException {
//      try {
//        checkClosed();
//        return typemap;
//      }
//      catch (Exception e) {
//        throw new SQLException(e);
//      }
//    }
//
//    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
//      typemap = map;
//    }
//
//    public void setHoldability(int holdability) throws SQLException {
//      throw new NotImplementedException();
//    }
//
//    public int getHoldability() throws SQLException {
//      throw new NotImplementedException();
//    }
//
//    public Savepoint setSavepoint() throws SQLException {
//      throw new NotImplementedException();
//    }
//
//    public Savepoint setSavepoint(String name) throws SQLException {
//      throw new NotImplementedException();
//    }
//
//    public void rollback(Savepoint savepoint) throws SQLException {
//      throw new NotImplementedException();
//    }
//
//    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
//      throw new NotImplementedException();
//    }
//
//    public Clob createClob() throws SQLException {
//      return new Clob();
//    }
//
//    public Blob createBlob() throws SQLException {
//      return new Blob();
//    }
//
//    public NClob createNClob() throws SQLException {
//      return new NClob();
//    }
//
//    public SQLXML createSQLXML() throws SQLException {
//      throw new NotImplementedException();
//    }
//
//    public boolean isValid(int timeout) throws SQLException {
//      throw new NotImplementedException();
//    }
//
//    public void setClientInfo(String name, String value) throws SQLClientInfoException {
//      throw new SQLClientInfoException();
//    }
//
//    public void setClientInfo(Properties properties) throws SQLClientInfoException {
//      throw new SQLClientInfoException();
//    }
//
//    public String getClientInfo(String name) throws SQLException {
//      throw new SQLClientInfoException();
//    }
//
//    public Properties getClientInfo() throws SQLException {
//      throw new SQLClientInfoException();
//    }
//
//    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
//      throw new NotImplementedException();
//    }
//
//    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
//      throw new NotImplementedException();
//    }
//
//    public void setSchema(String schema) throws SQLException {
//      throw new NotImplementedException();
//    }
//
//    public String getSchema() throws SQLException {
//      throw new NotImplementedException();
//    }
//
//    public void abort(Executor executor) throws SQLException {
//      throw new NotImplementedException();
//    }
//
//    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
//      throw new NotImplementedException();
//    }
//
//    public int getNetworkTimeout() throws SQLException {
//      // todo: implement for JDK 1.7
//      throw new NotImplementedException();
//    }
//
//    public <T> T unwrap(Class<T> iface) throws SQLException {
//      throw new NotImplementedException();
//    }
//
//    public boolean isWrapperFor(Class<?> iface) throws SQLException {
//      throw new NotImplementedException();
//    }
//
//    public void createDatabase(String dbName) {
//      try {
//        if (client != null) {
//          client.createDatabase(dbName);
//        }
//        else {
//          clients.get(url).client.createDatabase(dbName);
//        }
//      }
//      catch (Exception e) {
//        throw new DatabaseException(e);
//      }
//    }
//
//    public boolean databaseExists(String dbName) {
//      try {
//        if (client != null) {
//          client.syncSchema();
//          return client.getCommon().getDatabases().containsKey(dbName);
//        }
//        clients.get(url).client.syncSchema();
//        return clients.get(url).client.getCommon().getDatabases().containsKey(dbName);
//      }
//      catch (Exception e) {
//        throw new DatabaseException(e);
//      }
//    }
//
  }
}
