package com.lowryengineering.database.query;


public interface UpdateStatement extends Statement {

  void setTableName(String tableName);

  void setWhereClause(Expression expression);

  void addSetExpression(Expression expression);

  Object execute(String dbName) throws DatabaseException;

}
