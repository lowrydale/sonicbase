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
    return "parm(" + parmOffset + ")";
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
  @Override
  public void deserialize(short serializationVersion, DataInputStream in) {
    try {
      super.deserialize(serializationVersion, in);
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
    return ExpressionImpl.Type.PARAMETER;
  }

  @Override
  public NextReturn next(SelectStatementImpl.Explain explain, AtomicLong currOffset, AtomicLong countReturned, Limit limit, Offset offset, int schemaRetryCount) {
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
    return null;
  }
}
