package com.sonicbase.query.impl;

import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.procedure.RecordEvaluator;
import com.sonicbase.procedure.StoredProcedureContextImpl;
import com.sonicbase.query.*;
import com.sonicbase.schema.DataType;

/**
 * Responsible for
 */
public abstract class StatementImpl implements Statement {

  private ParameterHandler parms = null;

  public StatementImpl() {
    try {
      parms = new ParameterHandler();
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public ParameterHandler getParms() {
    return parms;
  }

  public abstract Object execute(String dbName, SelectStatementImpl.Explain explain, Long sequence0, Long sequence1,
                                 Short sequence2, boolean restrictToThisServer, StoredProcedureContextImpl procedureContext) throws DatabaseException;

  @Override
  public BinaryExpression createBinaryExpression(String id, BinaryExpression.Operator op, long value) {
    return new BinaryExpressionImpl(id.toLowerCase(), op, DataType.Type.BIGINT, value);
  }

  @Override
  public BinaryExpression createBinaryExpression(String id, BinaryExpression.Operator op, String value) {
    return new BinaryExpressionImpl(id.toLowerCase(), op, DataType.Type.VARCHAR, value);
  }

  @Override
  public BinaryExpression createBinaryExpression(Expression leftExpression, BinaryExpression.Operator op, Expression rightExpression) {
    BinaryExpressionImpl ret = new BinaryExpressionImpl();
    ret.setOperator(op);
    ret.setLeftExpression(leftExpression);
    ret.setRightExpression(rightExpression);
    return ret;
  }

  @Override
  public InExpression createInExpression() {
    return new InExpressionImpl();
  }


  public void setParms(ParameterHandler parms) {
    this.parms = parms;
  }


}
