package com.lowryengineering.database.query;

import java.util.List;


public interface CreateIndexStatement {

  void setName(String name);

  void setTableName(String tableName);

  void setColumns(List<String> columnNames);

  int execute(String dbName) throws DatabaseException;
}
