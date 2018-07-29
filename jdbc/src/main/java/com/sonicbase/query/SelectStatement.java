package com.sonicbase.query;

import com.sonicbase.procedure.StoredProcedureContextImpl;
import com.sonicbase.query.impl.SelectStatementImpl;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public interface SelectStatement extends Statement {

  void setFromTable(String tableName);

  void addSelectColumn(String function, ExpressionList parameters, String table, String column, String alias);

  void setWhereClause(Expression expression);

  Object execute(String dbName, String sqlToUse, SelectStatementImpl.Explain explain, Long sequence0, Long sequence1, Short sequence2,
                 boolean restrictToThisServer, StoredProcedureContextImpl procedureContex, int schemaRetryCount) throws DatabaseException;

  void addOrderByExpression(String tableName, String columnName, boolean ascending);

  enum JoinType {
    INNER,
    FULL,
    LEFT_OUTER,
    RIGHT_OUTER
  }
  void addJoinExpression(JoinType type, String rightFrom, Expression joinExpression);

}
