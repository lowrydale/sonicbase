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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class CreateTableStatementHandler extends StatementHandler {
  private final DatabaseClient client;

  public CreateTableStatementHandler(DatabaseClient client) {
    this.client = client;
  }

  @Override
  public Object execute(String dbName, ParameterHandler parms, String sqlToUse, Statement statement, SelectStatementImpl.Explain explain, Long sequence0, Long sequence1, Short sequence2, boolean restrictToThisServer, StoredProcedureContextImpl procedureContext, int schemaRetryCount) throws SQLException {
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
      List argList = columnDefinition.getColDataType().getArgumentsStringList();
      if (argList != null) {
        int width = Integer.valueOf((String) argList.get(0));
        fieldSchema.setWidth(width);
      }
      //fieldSchema.setWidth(width);
      fields.add(fieldSchema);
    }

    List<String> primaryKey = new ArrayList<String>();
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

  public int doCreateTable(String dbName, CreateTableStatementImpl createTableStatement) {
    try {
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.dbName, dbName);
      cobj.put(ComObject.Tag.schemaVersion, client.getCommon().getSchemaVersion());
      cobj.put(ComObject.Tag.method, "SchemaManager:createTable");
      cobj.put(ComObject.Tag.masterSlave, "master");
      cobj.put(ComObject.Tag.createTableStatement, createTableStatement.serialize());

      byte[] ret = client.sendToMaster(cobj);
      ComObject retObj = new ComObject(ret);
      client.getCommon().deserializeSchema(retObj.getByteArray(ComObject.Tag.schemaBytes));

      return 1;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }
}
