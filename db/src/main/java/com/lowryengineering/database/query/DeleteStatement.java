package com.lowryengineering.database.query;

import com.lowryengineering.database.query.impl.SelectStatementImpl;

/**
 * Responsible for
 */
public interface DeleteStatement extends Statement {

  void setTableName(String tableName);

  void setWhereClause(Expression expression);

  Object execute(String dbName, SelectStatementImpl.Explain explain) throws DatabaseException;
}
