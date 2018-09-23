package com.sonicbase.client;

import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.procedure.StoredProcedureContextImpl;
import com.sonicbase.query.impl.AllRecordsExpressionImpl;
import com.sonicbase.query.impl.ExpressionImpl;
import com.sonicbase.query.impl.SelectStatementImpl;
import com.sonicbase.query.impl.UpdateStatementImpl;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.update.Update;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class UpdateStatementHandler implements StatementHandler {
  private final DatabaseClient client;

  public UpdateStatementHandler(DatabaseClient client) {
    this.client = client;
  }

  @Override
  public Object execute(String dbName, ParameterHandler parms, String sqlToUse, Statement statement,
                        SelectStatementImpl.Explain explain, Long sequence0, Long sequence1, Short sequence2,
                        boolean restrictToThisServer, StoredProcedureContextImpl procedureContext,
                        int schemaRetryCount) {
    UpdateStatementImpl updateStatement = new UpdateStatementImpl(client);
    return execute(dbName, parms, sqlToUse, statement, explain, sequence0, sequence1, sequence2, restrictToThisServer,
        procedureContext, schemaRetryCount, updateStatement);
  }

  public Object execute(String dbName, ParameterHandler parms, String sqlToUse, Statement statement,
                        SelectStatementImpl.Explain explain, Long sequence0, Long sequence1, Short sequence2,
                        boolean restrictToThisServer, StoredProcedureContextImpl procedureContext,
                        int schemaRetryCount, UpdateStatementImpl updateStatement) {
    Update update = (Update) statement;
    AtomicInteger currParmNum = new AtomicInteger();
    updateStatement.setTableName(update.getTables().get(0).getName());

    List<Column> columns = update.getColumns();
    for (Column column : columns) {
      updateStatement.addColumn(column);
    }
    List<Expression> expressions = update.getExpressions();
    for (Expression expression : expressions) {
      ExpressionImpl qExpression = SelectStatementHandler.getExpression(client, currParmNum, expression,
          updateStatement.getTableName(), parms);
      updateStatement.addSetExpression(qExpression);
    }

    ExpressionImpl whereExpression = SelectStatementHandler.getExpression(client, currParmNum, update.getWhere(),
        updateStatement.getTableName(), parms);
    if (whereExpression == null) {
      whereExpression = new AllRecordsExpressionImpl();
      ((AllRecordsExpressionImpl) whereExpression).setFromTable(updateStatement.getTableName());
    }
    updateStatement.setWhereClause(whereExpression);

    updateStatement.setParms(parms);
    return updateStatement.execute(dbName, null, null, sequence0, sequence1, sequence2,
        restrictToThisServer, procedureContext, schemaRetryCount);
  }
}
