package com.sonicbase.jdbcdriver;

/**
 * User: lowryda
 * Date: 10/25/14
 * Time: 9:42 AM
 */

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.client.ReconfigureResults;
import com.sonicbase.common.ComObject;
import com.sonicbase.common.Logger;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.server.DatabaseServer;

import java.sql.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
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
  private static Map<String, ClientEntry> clients = new ConcurrentHashMap<>();
  private final String dbName;
  private final String url;
  private DatabaseClient client;
  private boolean autoCommit;
  private java.util.Map<String, Class<?>> typemap;
  private int rsHoldability;
  private Properties _clientInfo;
  private Properties properties;
  private boolean closed = false;
  private int shard;


  private class ClientEntry {
    private DatabaseClient client;
    private AtomicInteger refCount = new AtomicInteger();

    public ClientEntry(DatabaseClient client) {
      this.client = client;
    }
  }

  public ConnectionProxy(String url, DatabaseServer server) throws SQLException {

    this.properties = properties;
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
      client = new DatabaseClient(hosts, -1, -1, true, server.getCommon(), server);
      if (db != null) {
        client.initDb(db);
      }
      Logger.setIsClient(true);
      Logger.setReady();
      this.dbName = db;
      this.url = url;
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public ConnectionProxy(String url, Properties properties) throws SQLException {

    this.properties = properties;
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
      synchronized (clientMutex) {
        ClientEntry clientEntry = clients.get(url);
        if (clientEntry == null) {
          DatabaseClient client = new DatabaseClient(hosts, -1, -1, false);
          clientEntry = new ClientEntry(client);
          clients.put(url, clientEntry);
          if (db != null) {
            client.initDb(db);
          }
        }
        clientEntry.refCount.incrementAndGet();
        Logger.setIsClient(true);
        Logger.setReady();
      }
      this.dbName = db;
      this.url = url;
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

  public static com.sonicbase.query.ResultSet describeLicenses() {
    return DatabaseClient.describeLicenses();
  }

    public enum Replica {
    primary(DatabaseClient.Replica.primary),
    secondary(DatabaseClient.Replica.secondary),
    all(DatabaseClient.Replica.all),
    def(DatabaseClient.Replica.def),
    specified(DatabaseClient.Replica.specified),
    master(DatabaseClient.Replica.master);

    private final DatabaseClient.Replica cliReplica;

    Replica(DatabaseClient.Replica cliReplica) {
      this.cliReplica = cliReplica;
    }
  }

  public byte[] send(String batchKey,
                     int shard, long auth_user, ComObject body, Replica replica) {
    if (client != null) {
      return client.send(batchKey, shard, auth_user, body, replica.cliReplica);
    }
    return clients.get(url).client.send(batchKey, shard, auth_user, body, replica.cliReplica);
  }

  public byte[] send(String batchKey,
                     int shard, long auth_user, ComObject body, Replica replica, boolean ignoreDeath) {
    if (client != null) {
      return client.send(batchKey, shard, auth_user,  body, replica.cliReplica, ignoreDeath);
    }
    return clients.get(url).client.send(batchKey, shard, auth_user,  body, replica.cliReplica, ignoreDeath);
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

  public String debugRecord(String dbName, String tableName, String indexName, String key) {
    if (client != null) {
      return client.debugRecord(dbName, tableName, indexName, key);
    }
    return clients.get(url).client.debugRecord(dbName, tableName, indexName, key);
  }

  public ReconfigureResults reconfigureCluster() {
    if (client != null) {
      return client.reconfigureCluster();
    }
    return clients.get(url).client.reconfigureCluster();
  }

  public byte[] sendToMaster(ComObject body) {
    if (client != null) {
      return client.sendToMaster(body);
    }
    return clients.get(url).client.sendToMaster(body);
  }

  public void syncSchema() {
    if (client != null) {
      client.syncSchema();
    }
    else {
      clients.get(url).client.syncSchema();
    }
  }

    protected void checkClosed() throws SQLException {
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

  public void beginExplicitTransaction(String dbName) throws SQLException {
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

  public boolean getAutoCommit() throws SQLException {
    return autoCommit;
  }

  public void commit() throws SQLException {
    try {
      if (client != null) {
        client.commit(dbName, null);
      }
      else {
        clients.get(url).client.commit(dbName, null);
      }
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
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public boolean isClosed() throws SQLException {
    return closed;
  }

  public DatabaseMetaData getMetaData() throws SQLException {
    throw new NotImplementedException();
  }

  public void setReadOnly(boolean readOnly) throws SQLException {
    throw new NotImplementedException();
  }

  public boolean isReadOnly() throws SQLException {
    return false;
  }

  public void setCatalog(String catalog) throws SQLException {
  }

  public String getCatalog() throws SQLException {
    return null;
  }

  public void setTransactionIsolation(int level) throws SQLException {
  }

  public int getTransactionIsolation() throws SQLException {
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
    throw new SQLException("not supported");
  }

  public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
    try {
      if (client != null) {
        return new StatementProxy(this, client, null);
      }
      return new StatementProxy(this, clients.get(url).client, null);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    try {
      if (client != null) {
        return new StatementProxy(this, client, null);
      }
      return new StatementProxy(this, clients.get(url).client, null);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
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

  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    throw new SQLException("not supported");
  }

  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
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

  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
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

  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
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

  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
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

  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
    throw new SQLException("not supported");
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

  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    typemap = map;
  }

  public void setHoldability(int holdability) throws SQLException
  {
    try {
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
    catch (Exception e) {
      throw new SQLException(e);
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
    return new com.sonicbase.query.impl.Clob();
  }

  public Blob createBlob() throws SQLException {
    return new com.sonicbase.query.impl.Blob();
  }

  public NClob createNClob() throws SQLException {
    return new com.sonicbase.query.impl.NClob();
  }

  public SQLXML createSQLXML() throws SQLException {
    throw new NotImplementedException();
  }

  public boolean isValid(int timeout) throws SQLException {
    //todo: implement
    throw new NotImplementedException();
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
    try {
      checkClosed();
      return null;
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public Properties getClientInfo() throws SQLException {
    try {
      checkClosed();
      if (_clientInfo == null) {
          _clientInfo = new Properties();
      }
      return _clientInfo;
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    //todo: implement
    throw new NotImplementedException();
  }

  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    //todo: implement
    throw new NotImplementedException();
  }

  public void setSchema(String schema) throws SQLException {
    // todo: implement for JDK 1.7
    throw new NotImplementedException();
  }

  public String getSchema() throws SQLException {
    // todo: implement for JDK 1.7
    throw new NotImplementedException();
  }

  public void abort(Executor executor) throws SQLException {
    // todo: implement for JDK 1.7
    throw new NotImplementedException();
  }

  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    // todo: implement for JDK 1.7
    throw new NotImplementedException();
  }

  public int getNetworkTimeout() throws SQLException {
    // todo: implement for JDK 1.7
    throw new NotImplementedException();
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
    //todo: validate cast
    return (T) this;
  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    //todo: validate iface
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
