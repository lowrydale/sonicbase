package com.sonicbase.query;


import com.sonicbase.procedure.StoredProcedureContextImpl;
import com.sonicbase.query.impl.SelectStatementImpl;

public interface UpdateStatement extends Statement {

  void setTableName(String tableName);

  void setWhereClause(Expression expression);

  void addSetExpression(Expression expression);

  Object execute(String dbName, String sqlToUse, SelectStatementImpl.Explain explain, Long sequence0, Long sequence1,
                 Short sequence2, boolean restrictToThisServer, StoredProcedureContextImpl procedureContext, int schemaRetryCount);

}
