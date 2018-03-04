package com.sonicbase.query;

import java.util.List;

/**
 * Responsible for
 */
public interface CreateTableStatement {

  void setTableName(String tableName);

  void addField(String name, int sqlType);

  void setPrimaryKey(List<String> columnNames);

  int execute(String dbName) throws DatabaseException;
}
