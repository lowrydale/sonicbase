package com.sonicbase.query.impl;

import com.sonicbase.common.Record;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.TableSchema;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Set;

/**
 * Responsible for
 */
public class ParameterImpl extends ExpressionImpl {
  private int parmOffset;
  private String parmName;

  public int getParmOffset() {
    return parmOffset;
  }

  public void setParmOffset(int parmOffset) {
    this.parmOffset = parmOffset;
  }

  public String getParmName() {
    return parmName;
  }

  public void setParmName(String parmName) {
    this.parmName = parmName;
  }

  @Override
  public void getColumns(Set<ColumnImpl> columns) {

  }

  public String toString() {
    return "parm(" + String.valueOf(parmOffset) + ")";
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
      out.writeInt(parmOffset);
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  /**
   * ###############################
   * DON"T MODIFY THIS SERIALIZATION
   * ###############################
   */
  public void deserialize(DataInputStream in) {
    try {
      super.deserialize(in);
      parmOffset = in.readInt();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public Object evaluateSingleRecord(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms) {
    return parms.getValue(parmOffset + 1);
  }

  @Override
  public ExpressionImpl.Type getType() {
    return ExpressionImpl.Type.parameter;
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
