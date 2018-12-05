package com.sonicbase.client;

import com.sonicbase.common.ComArray;
import com.sonicbase.common.ComObject;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.procedure.StoredProcedureContextImpl;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.query.impl.ResultSetImpl;
import com.sonicbase.query.impl.SelectStatementImpl;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.execute.Execute;

import java.util.concurrent.ThreadLocalRandom;

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class ExecuteStatementHandler implements StatementHandler {
  private final DatabaseClient client;

  public ExecuteStatementHandler(DatabaseClient client) {
    this.client = client;
  }

  @Override
  public Object execute(String dbName, ParameterHandler parms, String sqlToUse, Statement statement,
                        SelectStatementImpl.Explain explain, Long sequence0, Long sequence1, Short sequence2,
                        boolean restrictToThisServer, StoredProcedureContextImpl procedureContext,
                        int schemaRetryCount) {
    Execute execute = (Execute) statement;
    if (!"procedure".equalsIgnoreCase((execute.getName()))) {
      throw new DatabaseException("invalid execute parameter: parm=" + execute.getName());
    }

    ComObject cobj = new ComObject(2);
    cobj.put(ComObject.Tag.SQL, sqlToUse);
    cobj.put(ComObject.Tag.DB_NAME, dbName);

    byte[] ret = client.send("DatabaseServer:executeProcedurePrimary",
        ThreadLocalRandom.current().nextInt(0, client.getShardCount()), 0, cobj, DatabaseClient.Replica.DEF);
    if (ret != null) {

      ComObject retObj = new ComObject(ret);
      return new ResultSetImpl(client, retObj.getArray(ComObject.Tag.RECORDS));
    }
    return new ResultSetImpl(client, (ComArray) null);

  }
}
