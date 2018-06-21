package com.sonicbase.client;

import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.procedure.StoredProcedureContextImpl;
import com.sonicbase.query.impl.SelectStatementImpl;
import net.sf.jsqlparser.statement.Statement;

import java.sql.SQLException;

public abstract class StatementHandler {


  public abstract Object execute(String dbName, ParameterHandler parms, String sqlToUse, Statement statement,
                                 SelectStatementImpl.Explain explain, Long sequence0, Long sequence1, Short sequence2, boolean restrictToThisServer,
                                 StoredProcedureContextImpl procedureContext, int schemaRetryCount) throws SQLException;
}
