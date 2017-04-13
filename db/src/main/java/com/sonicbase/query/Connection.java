package com.sonicbase.query;

import com.sonicbase.client.DatabaseClient;

public class Connection {

  private final DatabaseClient client;

  public Connection(String hosts) throws DatabaseException {
    try {
      String[] parts = hosts.split(":");
      this.client = new DatabaseClient(parts[0], Integer.valueOf(parts[1]), -1, -1, true);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }


  public SelectStatement createSelectStatement() {
    return client.createSelectStatement();
  }

  public InsertStatement createInsertStatement() {
    return client.createInsertStatement();
  }

  public UpdateStatement createUpdateStatement() {
    return client.createUpdateStatement();
  }

  public CreateTableStatement createCreateTableStatement() {
    return client.createCreateTableStatement();
  }

  public CreateIndexStatement createCreateIndexStatement() {
    return client.createCreateIndexStatement();
  }
}
