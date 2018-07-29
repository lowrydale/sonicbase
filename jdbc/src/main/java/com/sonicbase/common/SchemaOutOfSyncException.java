package com.sonicbase.common;

import com.sonicbase.query.DatabaseException;

public class SchemaOutOfSyncException extends DatabaseException {

  public SchemaOutOfSyncException() {
  }

  public SchemaOutOfSyncException(String msg) {
    super(msg);
  }
}
