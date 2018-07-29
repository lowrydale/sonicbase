package com.sonicbase.query.impl;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.query.CreateIndexStatement;

import java.util.List;

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class CreateIndexStatementImpl implements CreateIndexStatement {

  private String name;
  private String tableName;
  private List<String> columns;
  private boolean isUnique;

  public CreateIndexStatementImpl(DatabaseClient client) {

  }

  public String getName() {
    return name;
  }

  public String getTableName() {
    return tableName;
  }

  public List<String> getColumns() {
    return columns;
  }

  @Override
  public void setName(String name) {
    this.name = name.toLowerCase();
  }

  @Override
  public void setTableName(String tableName) {
    this.tableName = tableName.toLowerCase();
  }

  @Override
  public void setColumns(List<String> columnNames) {
    this.columns = columnNames;
    for (int i = 0; i < columns.size(); i++) {
      columns.set(i, columns.get(i).toLowerCase());
    }
  }

  public void setIsUnique(boolean isUnique) {
    this.isUnique = isUnique;
  }

  public boolean isUnique() {
    return isUnique;
  }
}
