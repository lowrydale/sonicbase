package com.lowryengineering.database.common;

import com.lowryengineering.database.query.DatabaseException;

/**
 * User: lowryda
 * Date: 10/30/14
 * Time: 8:06 PM
 */
public class SchemaOutOfSyncException extends DatabaseException {

  public SchemaOutOfSyncException() {
  }

  public SchemaOutOfSyncException(String msg) {
    super(msg);
  }
}
