package com.sonicbase.client;

import com.sonicbase.common.ComObject;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.procedure.StoredProcedureContextImpl;
import com.sonicbase.query.impl.SelectStatementImpl;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.drop.Drop;

public class DropStatementHandler implements StatementHandler {
  private final DatabaseClient client;

  public DropStatementHandler(DatabaseClient client) {
    this.client = client;
  }

  @Override
  public Object execute(String dbName, ParameterHandler parms, String sqlToUse, Statement statement,
                        SelectStatementImpl.Explain explain, Long sequence0, Long sequence1, Short sequence2,
                        boolean restrictToThisServer, StoredProcedureContextImpl procedureContext, int schemaRetryCount) {
    Drop drop = (Drop) statement;
    if (drop.getType().equalsIgnoreCase("table")) {
      String table = drop.getName().getName().toLowerCase();
      TruncateStatementHandler.doTruncateTable(client, dbName, table, schemaRetryCount);

      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.DB_NAME, dbName);
      cobj.put(ComObject.Tag.SCHEMA_VERSION, client.getCommon().getSchemaVersion());
      cobj.put(ComObject.Tag.METHOD, "SchemaManager:dropTable");
      cobj.put(ComObject.Tag.MASTER_SLAVE, "master");
      cobj.put(ComObject.Tag.TABLE_NAME, table);
      byte[] ret = client.sendToMaster(cobj);
      ComObject retObj = new ComObject(ret);
      byte[] bytes = retObj.getByteArray(ComObject.Tag.SCHEMA_BYTES);
      client.getCommon().deserializeSchema(bytes);
    }
    else if (drop.getType().equalsIgnoreCase("index")) {
      String indexName = drop.getName().getName().toLowerCase();
      String tableName = drop.getName().getSchemaName().toLowerCase();

      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.DB_NAME, dbName);
      cobj.put(ComObject.Tag.SCHEMA_VERSION, client.getCommon().getSchemaVersion());
      cobj.put(ComObject.Tag.METHOD, "SchemaManager:dropIndex");
      cobj.put(ComObject.Tag.TABLE_NAME, tableName);
      cobj.put(ComObject.Tag.INDEX_NAME, indexName);
      cobj.put(ComObject.Tag.MASTER_SLAVE, "master");
      byte[] ret = client.send(null, 0, 0, cobj, DatabaseClient.Replica.MASTER);
      ComObject retObj = new ComObject(ret);
      client.getCommon().deserializeSchema(retObj.getByteArray(ComObject.Tag.SCHEMA_BYTES));
    }
    return 1;

  }
}
