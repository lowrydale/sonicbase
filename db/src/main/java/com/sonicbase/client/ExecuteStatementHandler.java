package com.sonicbase.client;

import com.sonicbase.common.ComArray;
import com.sonicbase.common.ComObject;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.procedure.StoredProcedureContextImpl;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.query.impl.ResultSetImpl;
import com.sonicbase.query.impl.SelectStatementImpl;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.execute.Execute;

import java.sql.SQLException;
import java.util.concurrent.ThreadLocalRandom;

public class ExecuteStatementHandler extends StatementHandler {
  private final DatabaseClient client;

  public ExecuteStatementHandler(DatabaseClient client) {
    this.client = client;
  }

  @Override
  public Object execute(String dbName, ParameterHandler parms, String sqlToUse, Statement statement,
                        SelectStatementImpl.Explain explain, Long sequence0, Long sequence1, Short sequence2,
                        boolean restrictToThisServer, StoredProcedureContextImpl procedureContext, int schemaRetryCount) throws SQLException {
    Execute execute = (Execute) statement;
    ExpressionList expressions = execute.getExprList();
    if (!"procedure".equalsIgnoreCase((execute.getName()))) {
      throw new DatabaseException("invalid execute parameter: parm=" + execute.getName());
    }

    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.method, "executeProcedurePrimary");
    cobj.put(ComObject.Tag.sql, sqlToUse);
    cobj.put(ComObject.Tag.dbName, dbName);

    byte[] ret = client.send(null, ThreadLocalRandom.current().nextInt(0, client.getShardCount()), 0, cobj, DatabaseClient.Replica.def);
    if (ret != null) {

      ComObject retObj = new ComObject(ret);
      ResultSetImpl rs = new ResultSetImpl(client, retObj.getArray(ComObject.Tag.records));
      return rs;
    }
    return new ResultSetImpl(client, (ComArray) null);

  }
}
