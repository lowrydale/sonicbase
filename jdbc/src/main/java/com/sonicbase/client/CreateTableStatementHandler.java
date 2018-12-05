package com.sonicbase.client;

import com.sonicbase.common.ComObject;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.procedure.StoredProcedureContextImpl;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.query.impl.CreateTableStatementImpl;
import com.sonicbase.query.impl.SelectStatementImpl;
import com.sonicbase.schema.DataType;
import com.sonicbase.schema.FieldSchema;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class CreateTableStatementHandler implements StatementHandler {
  private final DatabaseClient client;

  public CreateTableStatementHandler(DatabaseClient client) {
    this.client = client;
  }

  @Override
  public Object execute(String dbName, ParameterHandler parms, String sqlToUse, Statement statement,
                        SelectStatementImpl.Explain explain, Long sequence0, Long sequence1, Short sequence2,
                        boolean restrictToThisServer, StoredProcedureContextImpl procedureContext,
                        int schemaRetryCount) {
    CreateTable createTable = (CreateTable) statement;
    CreateTableStatementImpl createTableStatement = new CreateTableStatementImpl(client);
    createTableStatement.setTableName(createTable.getTable().getName());

    List<FieldSchema> fields = new ArrayList<>();
    List columnDefinitions = createTable.getColumnDefinitions();
    for (int i = 0; i < columnDefinitions.size(); i++) {
      ColumnDefinition columnDefinition = (ColumnDefinition) columnDefinitions.get(i);

      FieldSchema fieldSchema = new FieldSchema();
      fieldSchema.setName(columnDefinition.getColumnName().toLowerCase());
      fieldSchema.setType(DataType.Type.valueOf(columnDefinition.getColDataType().getDataType().toUpperCase()));
      if (columnDefinition.getColDataType().getArgumentsStringList() != null) {
        String width = columnDefinition.getColDataType().getArgumentsStringList().get(0);
        fieldSchema.setWidth(Integer.valueOf(width));
      }
      applyColumnSpecs(columnDefinition, fieldSchema);

      applyWidth(columnDefinition, fieldSchema);

      fields.add(fieldSchema);
    }

    List<String> primaryKey = new ArrayList<>();
    List indexes = createTable.getIndexes();
    if (indexes == null) {
      primaryKey.add("_sonicbase_id");
    }
    else {
      for (int i = 0; i < indexes.size(); i++) {
        Index index = (Index) indexes.get(i);
        if (index.getType().equalsIgnoreCase("primary key")) {
          List columnNames = index.getColumnsNames();
          for (int j = 0; j < columnNames.size(); j++) {
            primaryKey.add((String) columnNames.get(j));
          }
        }
      }
    }

    createTableStatement.setFields(fields);
    createTableStatement.setPrimaryKey(primaryKey);

    return doCreateTable(dbName, createTableStatement);
  }

  private void applyWidth(ColumnDefinition columnDefinition, FieldSchema fieldSchema) {
    List argList = columnDefinition.getColDataType().getArgumentsStringList();
    if (argList != null) {
      int width = Integer.parseInt((String) argList.get(0));
      fieldSchema.setWidth(width);
    }
  }

  private void applyColumnSpecs(ColumnDefinition columnDefinition, FieldSchema fieldSchema) {
    List specs = columnDefinition.getColumnSpecStrings();
    if (specs != null) {
      for (Object obj : specs) {
        if (obj instanceof String) {
          String spec = (String) obj;
          if (spec.toLowerCase().contains("auto_increment")) {
            fieldSchema.setAutoIncrement(true);
          }
          if (spec.toLowerCase().contains("array")) {
            fieldSchema.setArray(true);
          }
        }
      }
    }
  }

  private int doCreateTable(String dbName, CreateTableStatementImpl createTableStatement) {
    try {
      ComObject cobj = new ComObject(4);
      cobj.put(ComObject.Tag.DB_NAME, dbName);
      cobj.put(ComObject.Tag.SCHEMA_VERSION, client.getCommon().getSchemaVersion());
      cobj.put(ComObject.Tag.MASTER_SLAVE, "master");
      cobj.put(ComObject.Tag.CREATE_TABLE_STATEMENT, createTableStatement.serialize());

      byte[] ret = client.sendToMaster("SchemaManager:createTable", cobj);
      ComObject retObj = new ComObject(ret);
      client.getCommon().deserializeSchema(retObj.getByteArray(ComObject.Tag.SCHEMA_BYTES));

      return 1;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }
}
