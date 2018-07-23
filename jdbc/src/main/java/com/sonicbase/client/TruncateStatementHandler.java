package com.sonicbase.client;

import com.sonicbase.common.ComObject;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.procedure.StoredProcedureContextImpl;
import com.sonicbase.query.impl.SelectStatementImpl;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.truncate.Truncate;

import java.sql.SQLException;
import java.util.Random;

public class TruncateStatementHandler implements StatementHandler {
  private final DatabaseClient client;

  public TruncateStatementHandler(DatabaseClient client) {
    this.client = client;
  }

  @Override
  public Object execute(String dbName, ParameterHandler parms, String sqlToUse, Statement statement,
                        SelectStatementImpl.Explain explain, Long sequence0, Long sequence1, Short sequence2,
                        boolean restrictToThisServer, StoredProcedureContextImpl procedureContext, int schemaRetryCount) throws SQLException {
    Truncate truncate = (Truncate) statement;
    String table = truncate.getTable().getName();
    table = table.toLowerCase();

    doTruncateTable(client, dbName, table, schemaRetryCount);

    return 1;
  }

  public static void doTruncateTable(DatabaseClient client, String dbName, String table, int schemaRetryCount) {

    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, dbName);
    if (schemaRetryCount < 2) {
      cobj.put(ComObject.Tag.SCHEMA_VERSION, client.getCommon().getSchemaVersion());
    }
    cobj.put(ComObject.Tag.METHOD, "UpdateManager:truncateTable");
    cobj.put(ComObject.Tag.TABLE_NAME, table);
    cobj.put(ComObject.Tag.PHASE, "secondary");

    Random rand = new Random(System.currentTimeMillis());
    client.sendToAllShards(null, rand.nextLong(), cobj, DatabaseClient.Replica.DEF);

    cobj.put(ComObject.Tag.PHASE, "primary");

    rand = new Random(System.currentTimeMillis());
    client.sendToAllShards(null, rand.nextLong(), cobj, DatabaseClient.Replica.DEF);
  }

}
