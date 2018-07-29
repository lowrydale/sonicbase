package com.sonicbase.query.impl;


import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.ComArray;
import com.sonicbase.common.ComObject;
import com.sonicbase.query.InsertStatement;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class InsertStatementImpl extends StatementImpl implements InsertStatement {
  private String tableName;
  private List<Object> values = new ArrayList<>();
  private List<String> columnNames = new ArrayList<>();
  private boolean ignore;
  private SelectStatementImpl select;

  public InsertStatementImpl(DatabaseClient client) {
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName.toLowerCase();
  }

  public List<Object> getValues() {
    return values;
  }

  public void setValues(List<Object> values) {
    this.values = values;
  }

  public List<String> getColumns() {
    return columnNames;
  }

  @Override
  public void addValue(String columnName, Object value) {
    columnNames.add(DatabaseClient.toLower(columnName));
    values.add(value);
  }

  public void setIgnore(boolean ignore) {
    this.ignore = ignore;
  }

  public boolean isIgnore() {
    return ignore;
  }

  public void setSelect(SelectStatementImpl select) {
    this.select = select;
  }

  public SelectStatementImpl getSelect() {
    return select;
  }

  public void serialize(ComObject cobj) {

    cobj.put(ComObject.Tag.TABLE_NAME, tableName);

    ComArray columnsArray = cobj.putArray(ComObject.Tag.COLUMNS, ComObject.Type.STRING_TYPE);
    for (String column : columnNames) {
      columnsArray.add(column);
    }

    cobj.put(ComObject.Tag.IGNORE, ignore);
    if (select != null) {
      cobj.put(ComObject.Tag.SELECT, select.serialize());
    }

  }

  public void setColumns(List<String> columns) {
    this.columnNames = columns;
  }
}
