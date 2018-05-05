package com.sonicbase.client;

import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.procedure.StoredProcedureContextImpl;
import com.sonicbase.query.impl.DeleteStatementImpl;
import com.sonicbase.query.impl.ExpressionImpl;
import com.sonicbase.query.impl.SelectStatementImpl;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

public class DeleteStatementHandler extends StatementHandler {
  private final DatabaseClient client;

  public DeleteStatementHandler(DatabaseClient client) {
    this.client = client;
  }

  @Override
  public Object execute(String dbName, ParameterHandler parms, String sqlToUse, Statement statement,
                        SelectStatementImpl.Explain explain, Long sequence0, Long sequence1, Short sequence2,
                        boolean restrictToThisServer, StoredProcedureContextImpl procedureContext, int schemaRetryCount) throws SQLException {
    Delete delete = (Delete) statement;
    DeleteStatementImpl deleteStatement = new DeleteStatementImpl(client);
    deleteStatement.setTableName(delete.getTable().getName());

    Expression expression = delete.getWhere();
    AtomicInteger currParmNum = new AtomicInteger();
    ExpressionImpl innerExpression = SelectStatementHandler.getExpression(client, currParmNum, expression, deleteStatement.getTableName(), parms);
    deleteStatement.setWhereClause(innerExpression);

    deleteStatement.setParms(parms);
    return deleteStatement.execute(dbName, null, null, sequence0, sequence1, sequence2, restrictToThisServer, procedureContext, schemaRetryCount);
  }


}
