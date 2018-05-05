package com.sonicbase.client;

import com.sonicbase.common.ComObject;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.procedure.StoredProcedureContextImpl;
import com.sonicbase.query.impl.SelectStatementImpl;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.drop.Drop;

import java.sql.SQLException;

public class DropStatementHandler extends StatementHandler {
  private final DatabaseClient client;

  public DropStatementHandler(DatabaseClient client) {
    this.client = client;
  }

  @Override
  public Object execute(String dbName, ParameterHandler parms, String sqlToUse, Statement statement,
                        SelectStatementImpl.Explain explain, Long sequence0, Long sequence1, Short sequence2,
                        boolean restrictToThisServer, StoredProcedureContextImpl procedureContext, int schemaRetryCount) throws SQLException {
    Drop drop = (Drop) statement;
    if (drop.getType().equalsIgnoreCase("table")) {
      String table = drop.getName().getName().toLowerCase();
      TruncateStatementHandler.doTruncateTable(client, dbName, table, schemaRetryCount);

      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.dbName, dbName);
      cobj.put(ComObject.Tag.schemaVersion, client.getCommon().getSchemaVersion());
      cobj.put(ComObject.Tag.method, "dropTable");
      cobj.put(ComObject.Tag.masterSlave, "master");
      cobj.put(ComObject.Tag.tableName, table);
      byte[] ret = client.sendToMaster(cobj);
      ComObject retObj = new ComObject(ret);
      byte[] bytes = retObj.getByteArray(ComObject.Tag.schemaBytes);
      client.getCommon().deserializeSchema(bytes);
    }
    else if (drop.getType().equalsIgnoreCase("index")) {
      String indexName = drop.getName().getName().toLowerCase();
      String tableName = drop.getName().getSchemaName().toLowerCase();

      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.dbName, dbName);
      cobj.put(ComObject.Tag.schemaVersion, client.getCommon().getSchemaVersion());
      cobj.put(ComObject.Tag.method, "dropIndex");
      cobj.put(ComObject.Tag.tableName, tableName);
      cobj.put(ComObject.Tag.indexName, indexName);
      cobj.put(ComObject.Tag.masterSlave, "master");
      byte[] ret = client.send(null, 0, 0, cobj, DatabaseClient.Replica.master);
      ComObject retObj = new ComObject(ret);
      client.getCommon().deserializeSchema(retObj.getByteArray(ComObject.Tag.schemaBytes));
    }
    return 1;

  }
}
