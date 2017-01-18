package com.lowryengineering.database.query.impl;


import com.lowryengineering.database.client.DatabaseClient;
import com.lowryengineering.database.query.DatabaseException;
import com.lowryengineering.database.query.InsertStatement;

import java.util.ArrayList;
import java.util.List;

public class InsertStatementImpl extends StatementImpl implements InsertStatement {
  private final DatabaseClient client;
  private String tableName;
  private List<Object> values = new ArrayList<Object>();
  private List<String> columnNames = new ArrayList<String>();

  public InsertStatementImpl(DatabaseClient client) {
    this.client = client;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName.toLowerCase();
  }

  @Override
  public Object execute(String dbName) throws DatabaseException {
    try {
      return client.doInsert(dbName, this, getParms());
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
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
    columnNames.add(columnName.toLowerCase());
    values.add(value);
  }

}
