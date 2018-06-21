package com.sonicbase.client;

import com.sonicbase.common.ComObject;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.procedure.StoredProcedureContextImpl;
import com.sonicbase.query.impl.CreateIndexStatementImpl;
import com.sonicbase.query.impl.SelectStatementImpl;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.create.table.Index;

import java.sql.SQLException;
import java.util.List;

public class CreateIndexStatementHandler extends StatementHandler {
  private final DatabaseClient client;

  public CreateIndexStatementHandler(DatabaseClient client) {
    this.client = client;
  }

  @Override
  public Object execute(String dbName, ParameterHandler parms, String sqlToUse, Statement statement,
                        SelectStatementImpl.Explain explain, Long sequence0, Long sequence1, Short sequence2,
                        boolean restrictToThisServer, StoredProcedureContextImpl procedureContext, int schemaRetryCount) throws SQLException {
    CreateIndex createIndex = (CreateIndex) statement;
    Index index = createIndex.getIndex();
    String indexName = index.getName().toLowerCase();
    List<String> columnNames = index.getColumnsNames();
    Table table = createIndex.getTable();
    String tableName = table.getName().toLowerCase();
    for (int i = 0; i < columnNames.size(); i++) {
      columnNames.set(i, columnNames.get(i).toLowerCase());
    }

    CreateIndexStatementImpl createStatement = new CreateIndexStatementImpl(client);
    if (index.getType() != null) {
      createStatement.setIsUnique(index.getType().equalsIgnoreCase("unique"));
    }
    createStatement.setName(indexName);
    createStatement.setTableName(tableName);
    createStatement.setColumns(columnNames);

    doCreateIndex(dbName, createStatement);

    return 1;

  }

  public void doCreateIndex(String dbName, CreateIndexStatementImpl statement) {
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.dbName, dbName);
    cobj.put(ComObject.Tag.schemaVersion, client.getCommon().getSchemaVersion());
    cobj.put(ComObject.Tag.method, "SchemaManager:createIndex");
    cobj.put(ComObject.Tag.masterSlave, "master");
    cobj.put(ComObject.Tag.tableName, statement.getTableName());
    cobj.put(ComObject.Tag.indexName, statement.getName());
    cobj.put(ComObject.Tag.isUnique, statement.isUnique());
    StringBuilder builder = new StringBuilder();
    boolean first = true;
    for (String field : statement.getColumns()) {
      if (!first) {
        builder.append(",");
      }
      first = false;
      builder.append(field);
    }
    //command = command + ":" + builder.toString();

    cobj.put(ComObject.Tag.fieldsStr, builder.toString());

    byte[] ret = client.sendToMaster(cobj);
    ComObject retObj = new ComObject(ret);
    client.getCommon().deserializeSchema(retObj.getByteArray(ComObject.Tag.schemaBytes));
  }


}
