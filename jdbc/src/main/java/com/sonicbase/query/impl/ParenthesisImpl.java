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
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Responsible for
 */
public class ParenthesisImpl extends ExpressionImpl {
  private ExpressionImpl expression;
  private boolean isNot;

  public ExpressionImpl getExpression() {
    return expression;
  }

  public void setExpression(ExpressionImpl expression) {
    this.expression = expression;
  }

  public boolean isNot() {
    return isNot;
  }

  public void setNot(boolean not) {
    isNot = not;
  }

  @Override
  public void getColumns(Set<ColumnImpl> columns) {

  }

  /**
   * ###############################
   * DON"T MODIFY THIS SERIALIZATION
   * ###############################
   */
  @Override
  public void serialize(DataOutputStream out) {
    try {
      super.serialize(out);
      serializeExpression(expression, out);
//      out.writeInt(expression.getType().getId());
//      expression.serialize(out);
      out.writeBoolean(isNot);
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public ExpressionImpl.Type getType() {
    return ExpressionImpl.Type.parenthesis;
  }

  /**
   * ###############################
   * DON"T MODIFY THIS SERIALIZATION
   * ###############################
   */
  @Override
  public void deserialize(DataInputStream in) {
    try {
      expression = deserializeExpression(in);
      isNot = in.readBoolean();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public Object evaluateSingleRecord(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms)  {
    Object ret = expression.evaluateSingleRecord(tableSchemas, records, parms);
    if (isNot) {
      return !(Boolean) ret;
    }
    return ret;
  }

  @Override
  public NextReturn next(int count, SelectStatementImpl.Explain eplain, AtomicLong currOffset, Limit limit, Offset offset, boolean b) {
    return null;
  }


  public NextReturn next(SelectStatementImpl.Explain explainBuilder, AtomicLong currOffset, Limit limit, Offset offset) {
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
