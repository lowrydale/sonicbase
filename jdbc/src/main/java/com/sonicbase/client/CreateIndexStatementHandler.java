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

public class CreateIndexStatementHandler implements StatementHandler {
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
    cobj.put(ComObject.Tag.DB_NAME, dbName);
    cobj.put(ComObject.Tag.SCHEMA_VERSION, client.getCommon().getSchemaVersion());
    cobj.put(ComObject.Tag.MASTER_SLAVE, "master");
    cobj.put(ComObject.Tag.TABLE_NAME, statement.getTableName());
    cobj.put(ComObject.Tag.INDEX_NAME, statement.getName());
    cobj.put(ComObject.Tag.IS_UNIQUE, statement.isUnique());
    StringBuilder builder = new StringBuilder();
    boolean first = true;
    for (String field : statement.getColumns()) {
      if (!first) {
        builder.append(",");
      }
      first = false;
      builder.append(field);
    }

    cobj.put(ComObject.Tag.FIELDS_STR, builder.toString());

    byte[] ret = client.sendToMaster("SchemaManager:createIndex", cobj);
    ComObject retObj = new ComObject(ret);
    client.getCommon().deserializeSchema(retObj.getByteArray(ComObject.Tag.SCHEMA_BYTES));
  }


}
