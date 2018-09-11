package com.sonicbase.client;

import com.sonicbase.common.ComObject;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.procedure.StoredProcedureContextImpl;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.query.impl.SelectStatementImpl;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.drop.Drop;

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class DropStatementHandler implements StatementHandler {
  public static final String MASTER_STR = "master";
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
      cobj.put(ComObject.Tag.MASTER_SLAVE, MASTER_STR);
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
      cobj.put(ComObject.Tag.MASTER_SLAVE, MASTER_STR);
      byte[] ret = client.send(null, 0, 0, cobj, DatabaseClient.Replica.MASTER);
      ComObject retObj = new ComObject(ret);
      client.getCommon().deserializeSchema(retObj.getByteArray(ComObject.Tag.SCHEMA_BYTES));
    }
    else if (drop.getType().equalsIgnoreCase("database")) {
      for (String tableName : client.getCommon().getTables(dbName).keySet()) {
        TruncateStatementHandler.doTruncateTable(client, dbName, tableName, schemaRetryCount);
      }

      String localDbName = drop.getName().getName().toLowerCase();
      if (!localDbName.equals(dbName)) {
        throw new DatabaseException("must be using same db as dropping: usingName=" + dbName + ", droppingName=" + localDbName);
      }
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.DB_NAME, dbName);
      cobj.put(ComObject.Tag.SCHEMA_VERSION, client.getCommon().getSchemaVersion());
      cobj.put(ComObject.Tag.METHOD, "SchemaManager:dropDatabase");
      cobj.put(ComObject.Tag.MASTER_SLAVE, MASTER_STR);
      byte[] ret = client.send(null, 0, 0, cobj, DatabaseClient.Replica.MASTER);
      ComObject retObj = new ComObject(ret);
      client.getCommon().deserializeSchema(retObj.getByteArray(ComObject.Tag.SCHEMA_BYTES));
    }
    return 1;

  }
}
