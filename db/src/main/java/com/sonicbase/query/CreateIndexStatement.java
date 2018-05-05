package com.sonicbase.query;

import java.util.List;


public interface CreateIndexStatement {

  void setName(String name);

  void setTableName(String tableName);

  void setColumns(List<String> columnNames);
}
