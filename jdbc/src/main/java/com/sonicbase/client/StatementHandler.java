package com.sonicbase.client;

import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.procedure.StoredProcedureContextImpl;
import com.sonicbase.query.impl.SelectStatementImpl;
import net.sf.jsqlparser.statement.Statement;

import java.sql.SQLException;

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public interface StatementHandler {

  Object execute(String dbName, ParameterHandler parms, String sqlToUse, Statement statement,
                 SelectStatementImpl.Explain explain, Long sequence0, Long sequence1, Short sequence2, boolean restrictToThisServer,
                 StoredProcedureContextImpl procedureContext, int schemaRetryCount) throws SQLException;
}
