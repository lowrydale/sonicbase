package com.sonicbase.query;

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public interface InsertStatement extends Statement {
  void addValue(String columnName, Object value);

  void setTableName(String persons);
}
