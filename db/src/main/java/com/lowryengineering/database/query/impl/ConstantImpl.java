package com.lowryengineering.database.query.impl;

import com.lowryengineering.database.common.Record;
import com.lowryengineering.database.jdbcdriver.ParameterHandler;
import com.lowryengineering.database.query.DatabaseException;
import com.lowryengineering.database.schema.DataType;
import com.lowryengineering.database.schema.TableSchema;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Types;
import java.util.Set;


public class ConstantImpl extends ExpressionImpl {
  private Object value;
  private int sqlType;

  public ConstantImpl() {

  }

  public ConstantImpl(Object value, int sqlType) {
    this.value = value;
    this.sqlType = sqlType;
  }

  public String toString() {
    if (sqlType == DataType.Type.VARCHAR.getValue() ||
        sqlType == DataType.Type.NVARCHAR.getValue() ||
        sqlType == DataType.Type.LONGVARCHAR.getValue() ||
        sqlType == DataType.Type.LONGNVARCHAR.getValue() ||
        sqlType == DataType.Type.NCLOB.getValue() ||
        sqlType == DataType.Type.CLOB.getValue()) {
      try {
        return new String((byte[])value, "utf-8");
      }
      catch (UnsupportedEncodingException e) {
        throw new DatabaseException(e);
      }
    }
    else {
      return String.valueOf(value);
    }
  }

  public Object getValue() {
    return value;
  }

  public void setValue(Object value) {
    this.value = value;
  }

  public int getSqlType() {
    return sqlType;
  }

  public void setSqlType(int sqlType) {
    this.sqlType = sqlType;
  }

  public void deserialize(DataInputStream in) {
    try {
      super.deserialize(in);
      sqlType = in.readInt();
      switch (sqlType) {
        case Types.CLOB:
          value = com.lowryengineering.database.jdbcdriver.Parameter.Clob.deserialize(in).getValue();
          break;
        case Types.VARCHAR:
          value = com.lowryengineering.database.jdbcdriver.Parameter.String.deserialize(in).getValue();
          break;
        case Types.INTEGER:
        case Types.NUMERIC:
        case Types.DECIMAL:
          value = com.lowryengineering.database.jdbcdriver.Parameter.Int.deserialize(in).getValue();
          break;
        case Types.BIGINT:
          value = com.lowryengineering.database.jdbcdriver.Parameter.Long.deserialize(in).getValue();
          break;
        case Types.SMALLINT:
        case Types.TINYINT:
        case Types.CHAR:
          value = com.lowryengineering.database.jdbcdriver.Parameter.Short.deserialize(in).getValue();
          break;
        case Types.REAL:
        case Types.FLOAT:
          value = com.lowryengineering.database.jdbcdriver.Parameter.Float.deserialize(in).getValue();
          break;
        case Types.DOUBLE:
          value = com.lowryengineering.database.jdbcdriver.Parameter.Double.deserialize(in).getValue();
          break;
      }
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public Object evaluateSingleRecord(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms) {
    return value;
  }

  @Override
  public void getColumns(Set<ColumnImpl> columns) {

  }

  @Override
  public void serialize(DataOutputStream out) {
    try {
      super.serialize(out);
      out.writeInt(sqlType);
      switch (sqlType) {
        case Types.CLOB:
          new com.lowryengineering.database.jdbcdriver.Parameter.Clob((byte[]) value).serialize(out, false);
          break;
        case Types.VARCHAR:
          new com.lowryengineering.database.jdbcdriver.Parameter.String((byte[]) value).serialize(out, false);
          break;
        case Types.INTEGER:
        case Types.NUMERIC:
        case Types.DECIMAL:
          new com.lowryengineering.database.jdbcdriver.Parameter.Int((Integer) value).serialize(out, false);
          break;
        case Types.BIGINT:
          new com.lowryengineering.database.jdbcdriver.Parameter.Long((Long) value).serialize(out, false);
          break;
        case Types.SMALLINT:
        case Types.TINYINT:
        case Types.CHAR:
          new com.lowryengineering.database.jdbcdriver.Parameter.Short((Short) value).serialize(out, false);
          break;
        case Types.REAL:
        case Types.FLOAT:
          new com.lowryengineering.database.jdbcdriver.Parameter.Float((Float) value).serialize(out, false);
          break;
        case Types.DOUBLE:
          new com.lowryengineering.database.jdbcdriver.Parameter.Double((Double) value).serialize(out, false);
          break;
      }
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public ExpressionImpl.Type getType() {
    return ExpressionImpl.Type.constant;
  }

  public NextReturn next(SelectStatementImpl.Explain explain) {
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
    return null;
  }
}
