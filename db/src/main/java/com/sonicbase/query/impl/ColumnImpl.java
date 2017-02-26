package com.sonicbase.query.impl;

import com.sonicbase.common.Record;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.TableSchema;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Set;

public class ColumnImpl extends ExpressionImpl {
  private ExpressionList parameters;
  private String function;
  private String tableName;
  private String columnName;
  private String alias;

  public ColumnImpl(String function, ExpressionList parameters, String table, String column, String alias) {
    this.function = function;
    this.parameters = parameters;
    this.tableName = table;
    this.columnName = column;
    this.alias = alias;
  }

  public ColumnImpl() {
  }

  public String toString() {
    if (tableName != null) {
      return tableName + "." + columnName;
    }
    return columnName;
  }

  public void getColumnsInExpression(List<ColumnImpl> columns) {
    super.getColumnsInExpression(columns);
    boolean found = false;
    for (ColumnImpl currColumn : columns) {
      if (((currColumn.getTableName() == null || getTableName() == null) || (currColumn.getTableName() == null || currColumn.getTableName().equals(getTableName()))) &&
          currColumn.getColumnName().equals(getColumnName())) {
        found = true;
        break;
      }
    }

    if (!found) {
      columns.add(this);
    }

  }

  public String getColumnName() {
    return columnName;
  }

  public void setColumnName(String columnName) {
    this.columnName = columnName.toLowerCase();
  }

  public ExpressionList getParameters() {
    return parameters;
  }

  public String getFunction() {
    return function;
  }

  public String getAlias() {
    return alias;
  }

  @Override
  public void serialize(DataOutputStream out) {
    try {
      super.serialize(out);
      out.writeUTF(columnName);
      if (tableName == null) {
        out.writeByte(0);
      }
      else {
        out.writeByte(1);
        out.writeUTF(tableName);
      }
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public ExpressionImpl.Type getType() {
    return ExpressionImpl.Type.column;
  }

  @Override
  public void deserialize(DataInputStream in) {
    try {
      super.deserialize(in);
      columnName = in.readUTF();
      if (1 == in.readByte()) {
        tableName = in.readUTF();
      }
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public Object evaluateSingleRecord(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms) {
    for (int i = 0; i < tableSchemas.length; i++) {
      if (tableSchemas[i].getName().equals(tableName)) {
        int offset = tableSchemas[i].getFieldOffset(columnName);
        if (records[i] == null) {
          return null;
        }
        return records[i].getFields()[offset];
      }
    }
    return null;
//
//    if (tableName != null && !tableName.equals(record.getTableSchema().getName())) {
//      throw new WrongTableException();
//    }
//    int offset = record.getTableSchema().getFieldOffset(columnName);
//    return record.getFields()[offset];
  }

  public String getTableName() {
    return tableName;
  }

  @Override
  public void getColumns(Set<ColumnImpl> columns) {
    columns.add(this);
  }

  public void setTableName(String tableName) {
    if (this.tableName != null) {
      return;
    }

    if (tableName == null) {
      return;
    }

    this.tableName = tableName;
  }

  @Override
  public NextReturn next(SelectStatementImpl.Explain explainBuilder) {
    return null;
  }

  @Override
  public NextReturn next(int count, SelectStatementImpl.Explain explain) {
    return null;
  }

  @Override
  public boolean canUseIndex() {
    return false;
  }

  @Override
  public boolean canSortWithIndex() {
    return false;
  }

  @Override
  public void queryRewrite() {

  }

  @Override
  public ColumnImpl getPrimaryColumn() {
    return this;
  }

  public int hashCode() {
    int hashCode = 0;
    if (tableName != null) {
      hashCode += tableName.hashCode();
    }
    hashCode += columnName.hashCode();
    return hashCode;
  }

  public boolean equals(Object rhsObj) {
    ColumnImpl rhs = ((ColumnImpl)rhsObj);
    if (tableName == null) {
      if (rhs.getTableName() != null) {
        return false;
      }
    }
    if (rhs.getTableName() == null) {
      return false;
    }
    if (!tableName.equals(rhs.getTableName())) {
      return false;
    }
    if (!columnName.equals(rhs.getColumnName())) {
      return false;
    }
    return true;
  }

}
