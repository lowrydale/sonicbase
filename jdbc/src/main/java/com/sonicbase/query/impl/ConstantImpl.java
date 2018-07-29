package com.sonicbase.query.impl;

import com.sonicbase.common.Record;
import com.sonicbase.jdbcdriver.Parameter;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.DataType;
import com.sonicbase.schema.TableSchema;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.Offset;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.Types;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;


@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
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
    if (sqlType == DataType.Type.CHAR.getValue() ||
        sqlType == DataType.Type.VARCHAR.getValue() ||
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

  /**
   * ###############################
   * DON"T MODIFY THIS SERIALIZATION
   * ###############################
   */
  @Override
  public void deserialize(short serializationVersion, DataInputStream in) {
    try {
      super.deserialize(serializationVersion, in);
      sqlType = in.readInt();
      switch (sqlType) {
        case Types.CLOB:
          value = Parameter.Clob.deserialize(in).getValue();
          break;
        case Types.VARCHAR:
        case Types.CHAR:
          value = Parameter.String.deserialize(in).getValue();
          break;
        case Types.INTEGER:
          value = Parameter.Int.deserialize(in).getValue();
          break;
        case Types.NUMERIC:
        case Types.DECIMAL:
          value = Parameter.BigDecimal.deserialize(in).getValue();
          break;
        case Types.BIGINT:
          value = Parameter.Long.deserialize(in).getValue();
          break;
        case Types.SMALLINT:
          value = Parameter.Short.deserialize(in).getValue();
          break;
        case Types.TINYINT:
          value = Parameter.Byte.deserialize(in).getValue();
          break;
        case Types.REAL:
        case Types.FLOAT:
          value = Parameter.Float.deserialize(in).getValue();
          break;
        case Types.DOUBLE:
          value = Parameter.Double.deserialize(in).getValue();
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
    //nothing to implement
  }

  public void negate() {
    if (value instanceof Long) {
      value = -1 * (Long)value;
    }
    else if (value instanceof Integer) {
      value = -1 * (Integer)value;
    }
    else if (value instanceof Short) {
      value = -1 * (Short)value;
    }
    else if (value instanceof Byte) {
      value = -1 * (Byte)value;
    }
    else if (value instanceof Double) {
      value = -1 * (Double)value;
    }
    else if (value instanceof Float) {
      value = -1 * (Float)value;
    }
    else if (value instanceof BigDecimal) {
      BigDecimal bd = (BigDecimal)value;
      value = bd.negate();
    }
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
      out.writeInt(sqlType);
      switch (sqlType) {
        case Types.CLOB:
          new Parameter.Clob((byte[]) value).serialize(out, false);
          break;
        case Types.VARCHAR:
        case Types.CHAR:
          new Parameter.String((byte[]) value).serialize(out, false);
          break;
        case Types.INTEGER:
          new Parameter.Int((Integer) value).serialize(out, false);
          break;
        case Types.NUMERIC:
        case Types.DECIMAL:
          new Parameter.BigDecimal((BigDecimal)value).serialize(out, false);
          break;
        case Types.BIGINT:
          new Parameter.Long((Long) value).serialize(out, false);
          break;
        case Types.SMALLINT:
          new Parameter.Short((Short) value).serialize(out, false);
          break;
        case Types.TINYINT:
          new Parameter.Byte((Byte) value).serialize(out, false);
          break;
        case Types.REAL:
        case Types.FLOAT:
          new Parameter.Float((Float) value).serialize(out, false);
          break;
        case Types.DOUBLE:
          new Parameter.Double((Double) value).serialize(out, false);
          break;
      }
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public ExpressionImpl.Type getType() {
    return ExpressionImpl.Type.CONSTANT;
  }

  @Override
  public NextReturn next(SelectStatementImpl.Explain explain, AtomicLong currOffset, AtomicLong countReturned,
                         Limit limit, Offset offset, int schemaRetryCount) {
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
    //nothing to implement
  }

  @Override
  public ColumnImpl getPrimaryColumn() {
    return null;
  }

}
