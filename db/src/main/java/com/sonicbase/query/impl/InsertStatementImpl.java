package com.sonicbase.query.impl;


import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.ComArray;
import com.sonicbase.common.ComObject;
import com.sonicbase.query.InsertStatement;

import java.util.ArrayList;
import java.util.List;

public class InsertStatementImpl extends StatementImpl implements InsertStatement {
  private final DatabaseClient client;
  private String tableName;
  private List<Object> values = new ArrayList<Object>();
  private List<String> columnNames = new ArrayList<String>();
  private boolean ignore;
  private SelectStatementImpl select;

  public InsertStatementImpl(DatabaseClient client) {
    this.client = client;
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

//  List srcColumns = stmt.getColumns();
//    ExpressionList items = (ExpressionList) stmt.getItemsList();
//    List srcExpressions = items.getExpressions();
//    for (int i = 0; i < srcColumns.size(); i++) {
//      Column column = (Column) srcColumns.get(i);
//      columnNames.add(column.getColumnName().toLowerCase());
//      Expression expression = (Expression) srcExpressions.get(i);
//      //todo: this doesn't handle out of order fields
//      if (expression instanceof JdbcParameter) {
//        values.add(new ParameterNode());
//      }
//      else if (expression instanceof StringValue) {
//        values.add(((StringValue) expression).getValue());
//      }
//      else if (expression instanceof LongValue) {
//        values.add(((LongValue) expression).getValue());
//      }
//      else if (expression instanceof DoubleValue) {
//        values.add(((DoubleValue) expression).getValue());
//      }
//      else {
//        throw new Exception("Unexpected column type: " + expression.getClass().getName());
//      }



  @Override
  public void addValue(String columnName, Object value) {
    columnNames.add(client.toLower(columnName));
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

    cobj.put(ComObject.Tag.tableName, tableName);

    //todo: add support for values when needed
    //cobj.putArray(ComObject.Tag.insearverValues, ComObject.Type.objectType);

    ComArray columnsArray = cobj.putArray(ComObject.Tag.columns, ComObject.Type.stringType);
    for (String column : columnNames) {
      columnsArray.add(column);
    }

    cobj.put(ComObject.Tag.ignore, ignore);
    if (select != null) {
      cobj.put(ComObject.Tag.select, select.serialize());
    }

  }

  public void setColumns(List<String> columns) {
    this.columnNames = columns;
  }
}
