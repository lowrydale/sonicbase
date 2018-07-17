package com.sonicbase.client;

import com.sonicbase.common.ComObject;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.procedure.StoredProcedureContextImpl;
import com.sonicbase.query.impl.SelectStatementImpl;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.create.table.ColDataType;

import java.sql.SQLException;

public class AlterStatementHandler extends StatementHandler {
  private final DatabaseClient client;

  public AlterStatementHandler(DatabaseClient client) {
    this.client = client;
  }

  @Override
  public Object execute(String dbName, ParameterHandler parms, String sqlToUse, Statement statement,
                        SelectStatementImpl.Explain explain, Long sequence0, Long sequence1, Short sequence2,
                        boolean restrictToThisServer, StoredProcedureContextImpl procedureContext, int schemaRetryCount) throws SQLException {
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

    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.dbName, dbName);
    cobj.put(ComObject.Tag.schemaVersion, client.getCommon().getSchemaVersion());
    cobj.put(ComObject.Tag.tableName, tableName);
    cobj.put(ComObject.Tag.columnName, columnName);
    cobj.put(ComObject.Tag.masterSlave, "master");
    byte[] ret = client.sendToMaster("SchemaManager:dropColumn", cobj);
    ComObject retObj = new ComObject(ret);
    client.getCommon().deserializeSchema(retObj.getByteArray(ComObject.Tag.schemaBytes));
  }

  private void doAddColumn(String dbName, String tableName, String columnName, ColDataType type) {

    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.dbName, dbName);
    cobj.put(ComObject.Tag.schemaVersion, client.getCommon().getSchemaVersion());
    cobj.put(ComObject.Tag.tableName, tableName);
    cobj.put(ComObject.Tag.columnName, columnName);
    cobj.put(ComObject.Tag.dataType, type.getDataType());
    cobj.put(ComObject.Tag.masterSlave, "master");
    byte[] ret = client.sendToMaster("SchemaManager:addColumn", cobj);
    ComObject retObj = new ComObject(ret);
    client.getCommon().deserializeSchema(retObj.getByteArray(ComObject.Tag.schemaBytes));
  }

}
