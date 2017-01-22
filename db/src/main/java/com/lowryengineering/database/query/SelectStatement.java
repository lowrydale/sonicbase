package com.lowryengineering.database.query;

import com.lowryengineering.database.query.impl.SelectStatementImpl;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;

/**
 * Responsible for
 */
public interface SelectStatement extends Statement {

  void setFromTable(String tableName);

  void addSelectColumn(String function, ExpressionList parameters, String table, String column, String alias);

  void setWhereClause(Expression expression);

  Object execute(String dbName, SelectStatementImpl.Explain explain) throws DatabaseException;

  void addOrderByExpression(String tableName, String columnName, boolean ascending);

  enum JoinType {
    inner,
    full,
    leftOuter,
    rightOuter
  }
  void addJoinExpression(JoinType type, String rightFrom, Expression joinExpression);

}
