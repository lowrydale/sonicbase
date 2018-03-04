package com.sonicbase.query;

import com.sonicbase.procedure.RecordEvaluator;
import com.sonicbase.procedure.StoredProcedureContextImpl;
import com.sonicbase.query.impl.SelectStatementImpl;

/**
 * Responsible for
 */
public interface DeleteStatement extends Statement {

  void setTableName(String tableName);

  void setWhereClause(Expression expression);

  Object execute(String dbName, SelectStatementImpl.Explain explain, Long sequence0, Long sequence1, Short sequence2,
                 boolean restrictToThisServer, StoredProcedureContextImpl procedureContext) throws DatabaseException;
}
