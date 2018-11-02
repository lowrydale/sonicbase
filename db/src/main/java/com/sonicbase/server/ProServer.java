package com.sonicbase.server;

import com.sonicbase.aws.AWSClient;
import com.sonicbase.common.Config;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.streams.StreamManager;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class ProServer {

  private static Logger logger = LoggerFactory.getLogger(ProServer.class);

  static final String SONICBASE_SYS_DB_STR = "_sonicbase_sys";

  private final DatabaseServer server;
  private final BackupManager backupManager;
  private final MonitorManager monitorManager;
  private final OSStatsManager osStatasManager;
  private final Config config;
  private AWSClient awsClient;
  private HttpServer httpServer;
  private int httpPort;
  private ConnectionProxy sysConnection;
  private final Object connMutex = new Object();

  public ProServer(DatabaseServer server) {
    this.server = server;
    this.config = server.getConfig();
    this.backupManager = new BackupManager(this, server);
    this.monitorManager = new MonitorManager(this, server);
    this.osStatasManager = new OSStatsManager(this, server);

    server.getMethodInvoker().registerMethodProvider("BackupManager", backupManager);
    server.getMethodInvoker().registerMethodProvider("MonitorManager", monitorManager);
    server.getMethodInvoker().registerMethodProvider("OSStatsManager", osStatasManager);

    List<Config.Shard> shards = config.getShards();

    outer:
    for (int i = 0; i < shards.size(); i++) {
      List<Config.Replica> replicas = shards.get(i).getReplicas();
      for (int j = 0; j < replicas.size(); j++) {
        Config.Replica replica = replicas.get(j);
        if (replica.getString("privateAddress").equals(server.getHost()) &&
            replica.getInt("port") == server.getPort()) {
          if (replica.getInt("httpPort") != null) {
            httpPort = replica.getInt("httpPort");

            this.httpServer = new HttpServer(this, server);
            this.httpServer.startMonitorServer(server.getHost(), httpPort);
            break outer;
          }
        }
      }
    }
  }

  public Config getConfig() {
    return config;
  }

  public void shutdown() {
    if (sysConnection != null) {
      try {
        sysConnection.close();
      }
      catch (SQLException e) {
        logger.error("Error closing connecion", e);
      }
      sysConnection = null;
    }
    httpServer.shutdown();
    osStatasManager.shutdown();
    monitorManager.shutdown();
    backupManager.shutdown();
  }

  public AWSClient getAwsClient() {
    if (awsClient == null) {
      this.awsClient = new AWSClient(this, server);
    }
    return awsClient;
  }

  public DatabaseServer getServer() {
    return server;
  }

  public BackupManager getBackupManager() {
    return backupManager;
  }

  public MonitorManager getMonitorManager() {
    return monitorManager;
  }

  public OSStatsManager getOsStatasManager() {
    return osStatasManager;
  }

  public Connection getSysConnection() {
    try {
      ConnectionProxy conn = null;
      try {
        synchronized (connMutex) {
          if (sysConnection != null) {
            return sysConnection;
          }
          List<Config.Shard> array = config.getShards();
          Config.Shard shard = array.get(0);
          List<Config.Replica> replicasArray = shard.getReplicas();
          Boolean priv = config.getBoolean("clientIsPrivate");
          final String address = priv != null && priv ?
              replicasArray.get(0).getString("privateAddress") :
              replicasArray.get(0).getString("publicAddress");
          final int port = replicasArray.get(0).getInt("port");

          Class.forName("com.sonicbase.jdbcdriver.Driver");
          conn = new ConnectionProxy("jdbc:sonicbase:" + address + ":" + port, server);
          try {
            if (!((ConnectionProxy) conn).databaseExists(SONICBASE_SYS_DB_STR)) {
              ((ConnectionProxy) conn).createDatabase(SONICBASE_SYS_DB_STR);
            }
          }
          catch (Exception e) {
            if (!ExceptionUtils.getFullStackTrace(e).toLowerCase().contains("database already exists")) {
              throw new DatabaseException(e);
            }
          }

          sysConnection = new ConnectionProxy("jdbc:sonicbase:" + address + ":" + port + "/_sonicbase_sys", server);
        }
      }
      finally {
        if (conn != null) {
          conn.close();
        }
      }

      return sysConnection;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

}
