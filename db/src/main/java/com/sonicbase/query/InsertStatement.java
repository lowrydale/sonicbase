package com.sonicbase.query;

/**
 * Responsible for
 */
public interface InsertStatement extends Statement {
  void addValue(String columnName, Object value);

  void setTableName(String persons);
}
