package com.sonicbase.client;

import com.sonicbase.common.ComObject;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.procedure.StoredProcedureContextImpl;
import com.sonicbase.query.impl.SelectStatementImpl;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.create.table.ColDataType;

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class AlterStatementHandler implements StatementHandler {
  private final DatabaseClient client;

  public AlterStatementHandler(DatabaseClient client) {
    this.client = client;
  }

  @Override
  public Object execute(String dbName, ParameterHandler parms, String sqlToUse, Statement statement,
                        SelectStatementImpl.Explain explain, Long sequence0, Long sequence1, Short sequence2,
                        boolean restrictToThisServer, StoredProcedureContextImpl procedureContext, int schemaRetryCount) {
    Alter alter = (Alter) statement;
    String operation = alter.getOperation();
    String tableName = alter.getTable().getName().toLowerCase();
    ColDataType type = alter.getDataType();
    String columnName = alter.getColumnName().toLowerCase();

    if (operation.equalsIgnoreCase("add")) {
      doAddColumn(dbName, tableName, columnName, type);
    }
    else if (operation.equalsIgnoreCase("drop")) {
      doDropColumn(dbName, tableName, columnName);
    }
    return 1;
  }

  private void doDropColumn(String dbName, String tableName, String columnName) {

    ComObject cobj = new ComObject(5);
    cobj.put(ComObject.Tag.DB_NAME, dbName);
    cobj.put(ComObject.Tag.SCHEMA_VERSION, client.getCommon().getSchemaVersion());
    cobj.put(ComObject.Tag.TABLE_NAME, tableName);
    cobj.put(ComObject.Tag.COLUMN_NAME, columnName);
    cobj.put(ComObject.Tag.MASTER_SLAVE, "master");
    byte[] ret = client.sendToMaster("SchemaManager:dropColumn", cobj);
    ComObject retObj = new ComObject(ret);
    client.getCommon().deserializeSchema(retObj.getByteArray(ComObject.Tag.SCHEMA_BYTES));
  }

  private void doAddColumn(String dbName, String tableName, String columnName, ColDataType type) {

    ComObject cobj = new ComObject(6);
    cobj.put(ComObject.Tag.DB_NAME, dbName);
    cobj.put(ComObject.Tag.SCHEMA_VERSION, client.getCommon().getSchemaVersion());
    cobj.put(ComObject.Tag.TABLE_NAME, tableName);
    cobj.put(ComObject.Tag.COLUMN_NAME, columnName);
    cobj.put(ComObject.Tag.DATA_TYPE, type.getDataType());
    cobj.put(ComObject.Tag.MASTER_SLAVE, "master");
    byte[] ret = client.sendToMaster("SchemaManager:addColumn", cobj);
    ComObject retObj = new ComObject(ret);
    client.getCommon().deserializeSchema(retObj.getByteArray(ComObject.Tag.SCHEMA_BYTES));
  }

}
