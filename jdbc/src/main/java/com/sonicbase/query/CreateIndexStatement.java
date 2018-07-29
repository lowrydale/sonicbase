package com.sonicbase.query;

import java.util.List;


@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public interface CreateIndexStatement {

  void setName(String name);

  void setTableName(String tableName);

  void setColumns(List<String> columnNames);
}
