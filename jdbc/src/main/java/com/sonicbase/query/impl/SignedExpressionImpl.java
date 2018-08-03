package com.sonicbase.query.impl;

import com.sonicbase.common.Record;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.TableSchema;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.Offset;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.concurrent.atomic.AtomicLong;

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class SignedExpressionImpl extends ExpressionImpl {
  private ExpressionImpl expression;
  private boolean isNegative;

  public ExpressionImpl getExpression() {
    return expression;
  }

  public void setExpression(ExpressionImpl expression) {
    this.expression = expression;
  }

  public boolean isNegative() {
    return isNegative;
  }

  public void setNegative(boolean negative) {
    this.isNegative = negative;
  }

  @Override
  public void serialize(short serializationVersion, DataOutputStream out) {
    try {
      super.serialize(serializationVersion, out);
      serializeExpression(expression, out);
      out.writeBoolean(isNegative);
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public Type getType() {
    return Type.SIGNED_EXPRESSION;
  }

  @Override
  public void deserialize(short serializationVersion, DataInputStream in) {
    try {
      super.deserialize(serializationVersion, in);
      expression = deserializeExpression(in);
      isNegative = in.readBoolean();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public Object evaluateSingleRecord(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms)  {
    Object ret = expression.evaluateSingleRecord(tableSchemas, records, parms);
    if (isNegative) {
      if (ret instanceof Long) {
        return -1 * (Long)ret;
      }
      if (ret instanceof Integer) {
        return -1 * (Integer)ret;
      }
      if (ret instanceof Short) {
        return -1 * (Short)ret;
      }
      if (ret instanceof Byte) {
        return -1 * (Byte)ret;
      }
      if (ret instanceof Double) {
        return -1 * (Double)ret;
      }
      if (ret instanceof Float) {
        return -1 * (Float)ret;
      }
      if (ret instanceof BigDecimal) {
        return ((BigDecimal)ret).negate();
      }
    }
    return ret;
  }

  @Override
  public NextReturn next(int count, SelectStatementImpl.Explain eplain, AtomicLong currOffset, AtomicLong countReturned,
                         Limit limit, Offset offset, boolean b, boolean analyze, int schemaRetryCount) {
    return null;
  }


  @Override
  public NextReturn next(SelectStatementImpl.Explain explainBuilder, AtomicLong currOffset, AtomicLong countReturned,
                         Limit limit, Offset offset, int schemaRetryCount) {
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
  public ColumnImpl getPrimaryColumn() {
    return null;
  }
}
