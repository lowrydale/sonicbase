package com.sonicbase.client;

import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.procedure.StoredProcedureContextImpl;
import com.sonicbase.query.impl.ExpressionImpl;
import com.sonicbase.query.impl.SelectStatementImpl;
import com.sonicbase.query.impl.UpdateStatementImpl;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.update.Update;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class UpdateStatementHandler extends StatementHandler {
  private final DatabaseClient client;

  public UpdateStatementHandler(DatabaseClient client) {
    this.client = client;
  }

  @Override
  public Object execute(String dbName, ParameterHandler parms, String sqlToUse, Statement statement, SelectStatementImpl.Explain explain, Long sequence0, Long sequence1, Short sequence2, boolean restrictToThisServer, StoredProcedureContextImpl procedureContext, int schemaRetryCount) throws SQLException {
    Update update = (Update) statement;
    UpdateStatementImpl updateStatement = new UpdateStatementImpl(client);
    AtomicInteger currParmNum = new AtomicInteger();
    //todo: support multiple tables?
    updateStatement.setTableName(update.getTables().get(0).getName());

    List<Column> columns = update.getColumns();
    for (Column column : columns) {
      updateStatement.addColumn(column);
    }
    List<Expression> expressions = update.getExpressions();
    for (Expression expression : expressions) {
      ExpressionImpl qExpression = SelectStatementHandler.getExpression(client, currParmNum, expression, updateStatement.getTableName(), parms);
      updateStatement.addSetExpression(qExpression);
    }

    ExpressionImpl whereExpression = SelectStatementHandler.getExpression(client, currParmNum, update.getWhere(), updateStatement.getTableName(), parms);
    updateStatement.setWhereClause(whereExpression);

    if (client.isExplicitTrans()) {
      List<DatabaseClient.TransactionOperation> ops = client.getTransactionOps().get();
      if (ops == null) {
        ops = new ArrayList<>();
        client.getTransactionOps().set(ops);
      }
      ops.add(new DatabaseClient.TransactionOperation(updateStatement, parms));
    }
    updateStatement.setParms(parms);
    return updateStatement.execute(dbName, null, null, sequence0, sequence1, sequence2, restrictToThisServer, procedureContext, schemaRetryCount);
  }
}
