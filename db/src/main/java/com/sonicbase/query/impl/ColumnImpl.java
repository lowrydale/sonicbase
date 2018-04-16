package com.sonicbase.query.impl;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.ComObject;
import com.sonicbase.common.Record;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.TableSchema;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.Offset;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

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

  /**
   * ###############################
   * DON"T MODIFY THIS SERIALIZATION
   * ###############################
   */
  @Override
  public void serialize(short serializationVersion, DataOutputStream out) {
    try {
      super.serialize(serializationVersion, out);
      if (serializationVersion <= DatabaseClient.SERIALIZATION_VERSION_23) {
        out.writeUTF(columnName);
        if (tableName == null) {
          out.writeByte(0);
        }
        else {
          out.writeByte(1);
          out.writeUTF(tableName);
        }
      }
      else {
        ComObject cobj = new ComObject();
        if (columnName != null) {
          cobj.put(ComObject.Tag.columnName, columnName);
        }
        if (tableName != null) {
          cobj.put(ComObject.Tag.tableName, tableName);
        }
        if (alias != null) {
          cobj.put(ComObject.Tag.alias, alias);
        }
        if (function != null) {
          cobj.put(ComObject.Tag.function, function);
        }
        byte[] bytes = cobj.serialize();
        out.writeInt(bytes.length);
        out.write(bytes);
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

  /**
   * ###############################
   * DON"T MODIFY THIS SERIALIZATION
   * ###############################
   */
  @Override
  public void deserialize(short serializationVersion, DataInputStream in) {
    try {
      super.deserialize(serializationVersion, in);
      if (serializationVersion <= DatabaseClient.SERIALIZATION_VERSION_23) {
        columnName = in.readUTF();
        if (1 == in.readByte()) {
          tableName = in.readUTF();
        }
      }
      else {
        int len = in.readInt();
        byte[] buffer = new byte[len];
        in.readFully(buffer);
        ComObject cobj = new ComObject(buffer);
        columnName = cobj.getString(ComObject.Tag.columnName);
        tableName = cobj.getString(ComObject.Tag.tableName);
        alias = cobj.getString(ComObject.Tag.alias);
        function = cobj.getString(ComObject.Tag.function);
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
        Integer offset = tableSchemas[i].getFieldOffset(columnName);
        if (offset == null) {
          throw new DatabaseException("Invalid column name: table=" + tableName + ", name=" + columnName);
        }
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
  public NextReturn next(SelectStatementImpl.Explain explainBuilder, AtomicLong currOffset, AtomicLong countReturned, Limit limit, Offset offset, int schemaRetryCount) {
    return null;
  }

  @Override
  public NextReturn next(int count, SelectStatementImpl.Explain explain, AtomicLong currOffset, AtomicLong countReturned,
                         Limit limit, Offset offset, boolean b, boolean analyze, int schemaRetryCount) {
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
