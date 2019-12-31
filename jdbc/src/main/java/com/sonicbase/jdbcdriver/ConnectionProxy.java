package com.sonicbase.jdbcdriver;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.client.DatabaseServerProxy;
import com.sonicbase.client.DescribeStatementHandler;
import com.sonicbase.client.ReconfigureResults;
import com.sonicbase.common.ComObject;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.TableSchema;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class ConnectionProxy implements Connection {

  private static final Object clientMutex = new Object();
  private static final String NOT_SUPPORTED_STR = "not supported";
  private static final Map<String, ClientEntry> clients = new ConcurrentHashMap<>();
  private final String dbName;
  private final String url;
  private DatabaseClient client;
  private boolean autoCommit;
  private java.util.Map<String, Class<?>> typemap;
  private boolean closed = false;

  public void addClient(String url, DatabaseClient client) {
    clients.computeIfAbsent(url, k -> new ClientEntry(client));
  }

  public void setClient(DatabaseClient client) {
    this.client = client;
  }


  private class ClientEntry {
    private final DatabaseClient client;
    private final AtomicInteger refCount = new AtomicInteger();

    ClientEntry(DatabaseClient client) {
      this.client = client;
    }
  }

  public ConnectionProxy(String url, Object server) throws SQLException {

    initGlobalContext();
    try {
      String[] outerParts = url.split("/");
      String hostsStr = outerParts[0].substring("jdbc:sonicbase:".length());
      String[] hosts = hostsStr.split(",");
      String db = null;
      if (outerParts.length > 1) {
        db = outerParts[1];
        db = db.toLowerCase();
      }
      client = new DatabaseClient(hosts, DatabaseServerProxy.getShard(server), DatabaseServerProxy.getReplica(server),
          true, DatabaseServerProxy.getCommon(server), server, false);
      if (db != null) {
        client.initDb();
      }
      this.dbName = db;
      this.url = url;
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public ConnectionProxy(String url, Properties properties) throws SQLException {

    initGlobalContext();
    try {
      if (!url.equalsIgnoreCase("mock")) {
        String[] outerParts = url.split("/");
        String hostsStr = outerParts[0].substring("jdbc:sonicbase:".length());
        String[] hosts = hostsStr.split(",");
        String db = null;
        if (outerParts.length > 1) {
          db = outerParts[1];
          db = db.toLowerCase();
        }
        synchronized (clientMutex) {
          ClientEntry clientEntry = clients.get(url);
          if (clientEntry == null) {
            DatabaseClient localClient = new DatabaseClient(hosts, -1, -1, false);
            clientEntry = new ClientEntry(localClient);
            clients.put(url, clientEntry);
            if (db != null) {
              localClient.initDb();
            }
          }
          clientEntry.refCount.incrementAndGet();
        }
        this.dbName = db;
        this.url = url;
      }
      else {
        this.dbName = null;
        this.url = url;
      }
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
    if (client != null) {
      return client;
    }
    return clients.get(url).client;
  }

  private void initGlobalContext() {
    globalContextRefCount.incrementAndGet();
  }

  public boolean isBackupComplete() {
    if (client != null) {
      return client.isBackupComplete();
    }
    return clients.get(url).client.isBackupComplete();
  }

  public boolean isRestoreComplete() {
    if (client != null) {
      return client.isRestoreComplete();
    }
    return clients.get(url).client.isRestoreComplete();
  }

  public void startBackup() {
    if (client != null) {
      client.startBackup();
    }
    else {
      clients.get(url).client.startBackup();
    }
  }

  public void startRestore(String subDir) {
    if (client != null) {
      client.startRestore(subDir);
    }
    else {
      clients.get(url).client.startRestore(subDir);
    }
  }

  public int getReplicaCount() {
    if (client != null) {
      return client.getReplicaCount();
    }
    return clients.get(url).client.getReplicaCount();
  }

  public int getShardCount() {
    if (client != null) {
      return client.getShardCount();
    }
    return clients.get(url).client.getShardCount();
  }

  public int getSchemaVersion() {
    if (client != null) {
      return client.getCommon().getSchemaVersion();
    }
    return clients.get(url).client.getCommon().getSchemaVersion();
  }

  public com.sonicbase.query.ResultSet describeLicenses() {
    DescribeStatementHandler handler = new DescribeStatementHandler(client);
    return handler.describeLicenses();
  }

  public enum Replica {
    PRIMARY(DatabaseClient.Replica.PRIMARY),
    SECONDARY(DatabaseClient.Replica.SECONDARY),
    ALL(DatabaseClient.Replica.ALL),
    DEF(DatabaseClient.Replica.DEF),
    SPECIFIED(DatabaseClient.Replica.SPECIFIED),
    MASTER(DatabaseClient.Replica.MASTER);

    private final DatabaseClient.Replica cliReplica;

    Replica(DatabaseClient.Replica cliReplica) {
      this.cliReplica = cliReplica;
    }
  }

  public ComObject send(String method,
                        int shard, long authUser, ComObject body, Replica replica) {
    if (client != null) {
      return client.send(method, shard, authUser, body, replica.cliReplica);
    }
    return clients.get(url).client.send(method, shard, authUser, body, replica.cliReplica);
  }

  public ComObject[] sendToAllShards(String method,
                                     long authUser, ComObject body, Replica replica) {
    if (client != null) {
      return client.sendToAllShards(method, authUser, body, replica.cliReplica);
    }
    return clients.get(url).client.sendToAllShards(method, authUser, body, replica.cliReplica);
  }

  public ComObject send(String batchKey,
                        int shard, long authUser, ComObject body, Replica replica, boolean ignoreDeath) {
    if (client != null) {
      return client.send(batchKey, shard, authUser,  body, replica.cliReplica, ignoreDeath);
    }
    return clients.get(url).client.send(batchKey, shard, authUser,  body, replica.cliReplica, ignoreDeath);
  }

  public int getMasterReplica(int shard) {
    if (client != null) {
      return client.getCommon().getServersConfig().getShards()[shard].getMasterReplica();
    }
    return clients.get(url).client.getCommon().getServersConfig().getShards()[shard].getMasterReplica();
  }

  public Map<String, TableSchema> getTables(String dbName) {
    if (client != null) {
      return client.getCommon().getTables(dbName);
    }
    return clients.get(url).client.getCommon().getTables(dbName);
  }

  public ReconfigureResults reconfigureCluster() {
    if (client != null) {
      return client.reconfigureCluster();
    }
    return clients.get(url).client.reconfigureCluster();
  }

  public ComObject sendToMaster(ComObject body) {
    if (client != null) {
      return client.sendToMaster(body);
    }
    return clients.get(url).client.sendToMaster(body);
  }

  public ComObject sendToMaster(String method, ComObject body) {
    if (client != null) {
      return client.sendToMaster(method, body);
    }
    return clients.get(url).client.sendToMaster(method, body);
  }

  public void syncSchema() {
    if (client != null) {
      client.syncSchema();
    }
    else {
      clients.get(url).client.syncSchema();
    }
  }

    public void checkClosed() throws SQLException {
      if (isClosed()) {
        throw new SQLException("This connection has been closed.");
      }
  }

  public Statement createStatement() throws SQLException {
    try {
      return new StatementProxy(this, clients.get(url).client, null);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  private void beginExplicitTransaction(String dbName) throws SQLException {
    try {
      if (client != null) {
        client.beginExplicitTransaction(dbName);
      }
      else {
        clients.get(url).client.beginExplicitTransaction(dbName);
      }
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public boolean getAutoCommit() {
    return autoCommit;
  }

  public void commit() throws SQLException {
    try {
      if (client != null) {
        client.commit(dbName);
      }
      else {
        clients.get(url).client.commit(dbName);
      }
      autoCommit = true;
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public void rollback() throws SQLException {
    try {
      if (client != null) {
        client.rollback(dbName);
      }
      else {
        clients.get(url).client.rollback(dbName);
      }
      autoCommit = true;
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public String nativeSQL(String sql) throws SQLException {
    throw new NotImplementedException();
  }

  public void setAutoCommit(boolean autoCommit) throws SQLException {
    try {
      this.autoCommit = autoCommit;
      if (!autoCommit) {
        beginExplicitTransaction(dbName);
      }
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public void close() throws SQLException {
    try {
      if (closed) {
        throw new SQLException("Attempting to close a connection that is already closed");
      }

      closed = true;
      ClientEntry entry = clients.get(url);
      if (entry != null && entry.refCount.decrementAndGet() == 0) {
        entry.client.shutdown();
        clients.remove(url);
      }
      if (client != null) {
        client.shutdown();
      }
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public boolean isClosed() {
    return closed;
  }

  public DatabaseMetaData getMetaData() throws SQLException {
    throw new NotImplementedException();
  }

  public void setReadOnly(boolean readOnly) throws SQLException {
    throw new NotImplementedException();
  }

  public boolean isReadOnly() {
    return false;
  }

  public void setCatalog(String catalog) throws SQLException {
    throw new NotImplementedException();
  }

  public String getCatalog() {
    return null;
  }

  public void setTransactionIsolation(int level) throws SQLException {
    throw new NotImplementedException();
  }

  public int getTransactionIsolation() {
    return TRANSACTION_READ_COMMITTED;
  }

  public SQLWarning getWarnings() throws SQLException {
    try {
      checkClosed();
      StringBuilder ret = new StringBuilder();
      if (ret.length() == 0) {
        return null;
      }
      return new SQLWarning(ret.toString());
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public void clearWarnings() throws SQLException {
    try {
      checkClosed();
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public PreparedStatement prepareStatement(String sql) throws SQLException {
    try {
      if (client != null) {
        return new StatementProxy(this, client, sql);
      }
      return new StatementProxy(this, clients.get(url).client, sql);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public CallableStatement prepareCall(String sql) throws SQLException {
    throw new SQLException(NOT_SUPPORTED_STR);
  }

  public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
    throw new NotImplementedException();
  }

  public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    throw new NotImplementedException();
  }

  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                            int resultSetHoldability) throws SQLException {
    throw new NotImplementedException();
  }

  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                       int resultSetHoldability) throws SQLException {
    throw new SQLException(NOT_SUPPORTED_STR);
  }

  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    throw new NotImplementedException();
  }

  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    throw new NotImplementedException();
  }

  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    throw new NotImplementedException();
  }

  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
    throw new NotImplementedException();
  }

  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
    throw new SQLException(NOT_SUPPORTED_STR);
  }

  public Map<String, Class<?>> getTypeMap() throws SQLException {
    try {
      checkClosed();
      return typemap;
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public void setTypeMap(Map<String, Class<?>> map) {
    typemap = map;
  }

  public void setHoldability(int holdability) throws SQLException {
    throw new NotImplementedException();
  }

  public int getHoldability() throws SQLException {
    return ResultSet.HOLD_CURSORS_OVER_COMMIT;
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

  public Clob createClob() {
    return new com.sonicbase.query.impl.Clob();
  }

  public Blob createBlob() {
    return new com.sonicbase.query.impl.Blob();
  }

  public NClob createNClob() {
    return new com.sonicbase.query.impl.NClob();
  }

  public SQLXML createSQLXML() throws SQLException {
    throw new NotImplementedException();
  }

  public boolean isValid(int timeout) throws SQLException {
    throw new NotImplementedException();
  }

  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    throw new SQLClientInfoException();
  }

  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    throw new SQLClientInfoException();
  }

  public String getClientInfo(String name) throws SQLException {
    throw new SQLClientInfoException();
  }

  public Properties getClientInfo() throws SQLException {
    throw new SQLClientInfoException();
  }

  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    throw new NotImplementedException();
  }

  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    throw new NotImplementedException();
  }

  public void setSchema(String schema) throws SQLException {
    throw new NotImplementedException();
  }

  public String getSchema() throws SQLException {
    throw new NotImplementedException();
  }

  public void abort(Executor executor) throws SQLException {
    throw new NotImplementedException();
  }

  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    throw new NotImplementedException();
  }

  public int getNetworkTimeout() throws SQLException {
    throw new NotImplementedException();
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new NotImplementedException();
  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new NotImplementedException();
  }

  public void createDatabase(String dbName) {
    try {
      if (client != null) {
        client.createDatabase(dbName);
      }
      else {
        clients.get(url).client.createDatabase(dbName);
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public boolean databaseExists(String dbName) {
    try {
      if (client != null) {
        client.syncSchema();
        return client.getCommon().getDatabases().containsKey(dbName);
      }
      clients.get(url).client.syncSchema();
      return clients.get(url).client.getCommon().getDatabases().containsKey(dbName);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }
}
