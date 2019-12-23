/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.embedded;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.Config;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.server.DatabaseServer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

public class EmbeddedDatabase {
  private DatabaseServer server;
  private boolean notDurable = false;
  private String dataDir;
  private boolean useUnsafe = false;

  public EmbeddedDatabase() {
  }

  public void enableDurability(String path) {
    notDurable = false;
    dataDir = path;
  }

  public void disableDurability() {
    notDurable = true;
  }

  public void setUseUnsafe(boolean useUnsafe) {
    this.useUnsafe = useUnsafe;
  }

  public void start() throws SQLException {
    try {
      String configStr = IOUtils.toString(new BufferedInputStream(getClass().getResourceAsStream("/config/config-embedded.yaml")), "utf-8");
      Config config = new Config(configStr);

      if (dataDir != null) {
        config.put("dataDirectory", dataDir);
      }
      config.put("useUnsafe", useUnsafe);

      server = new DatabaseServer();
      String installDir = new File(System.getProperty("user.dir")).getAbsolutePath();
      server.setConfig(config, "localhost", 8999, true,
          new AtomicBoolean(true), new AtomicBoolean(true), "", "", notDurable, true, installDir);

      if (!notDurable) {
        server.recoverFromSnapshot();

        server.getLogManager().applyLogs();

        server.getDeleteManager().forceDeletes(null,  false);
        server.getDeleteManager().start();

        server.getSnapshotManager().runSnapshotLoop();
      }


      DatabaseServer.initDeathOverride(1, 1);
      DatabaseServer.getDeathOverride()[0][0] = false;

      Class.forName("com.sonicbase.jdbcdriver.Driver");

    }
    catch (Exception e) {
      throw new SQLException(e);
    }

  }

  public void createDatabaseIfNeeded(String dbName) throws SQLException {
    try (Connection conn = DriverManager.getConnection("jdbc:sonicbase:localhost:8999")) {
      DatabaseClient client = ((ConnectionProxy) conn).getDatabaseClient();
      client.syncSchema();
      if (!client.getCommon().getDatabases().containsKey(dbName)) {
        client.createDatabase(dbName);
      }
    }
  }

  public Connection getConnection(String db) throws SQLException {
    Connection conn = DriverManager.getConnection("jdbc:sonicbase:localhost:8999/" + db);
    DatabaseClient client = ((ConnectionProxy) conn).getDatabaseClient();
    client.syncSchema();
    return conn;
  }

  public void shutdown() {
    if (server != null) {
      server.shutdown();
    }
  }

  public void purge() throws SQLException {
    if (dataDir != null) {
      File dir = new File(dataDir);
      if (dir.exists()) {
        try {
          FileUtils.deleteDirectory(dir);
        }
        catch (IOException e) {
          throw new SQLException(e);
        }
      }
    }
  }
}
