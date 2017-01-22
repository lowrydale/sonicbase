package com.lowryengineering.database.query;

import com.lowryengineering.database.query.impl.SelectStatementImpl;

/**
 * Responsible for
 */
public interface InsertStatement extends Statement {
  void addValue(String columnName, Object value);

  void setTableName(String persons);

  Object execute(String dbName, SelectStatementImpl.Explain explain) throws DatabaseException;
}
