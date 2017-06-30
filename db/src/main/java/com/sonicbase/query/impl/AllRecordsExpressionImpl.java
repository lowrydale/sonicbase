package com.sonicbase.query.impl;


import com.sonicbase.common.Record;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.query.BinaryExpression;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.server.ReadManager;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class AllRecordsExpressionImpl extends ExpressionImpl {
  private String fromTable;

  public void setFromTable(String fromTable) {
    this.fromTable = fromTable;
  }

  @Override
  public Type getType() {
    return Type.allExpression;
  }

  @Override
  public void getColumns(Set<ColumnImpl> columns) {

  }

  public String toString() {
    return "<all>";
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
      out.writeUTF(fromTable);
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
  public void deserialize(DataInputStream in) {
    try {
      super.deserialize(in);
      fromTable = in.readUTF();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public Object evaluateSingleRecord(
      TableSchema[] tableSchemas, Record[] records, ParameterHandler parms) {
    for (int i = 0; i < tableSchemas.length; i++) {
      if (tableSchemas[i].getName().equals(fromTable)) {
        return records[i];
      }
    }
    return null;
  }

  public String getFromTable() {
    return fromTable;
  }

  @Override
  public NextReturn next(int count, SelectStatementImpl.Explain explain) {
    TableSchema tableSchema = getClient().getCommon().getTables(dbName).get(getFromTable());
    IndexSchema indexSchema = null;
    for (Map.Entry<String, IndexSchema> entry : tableSchema.getIndexes().entrySet()) {
      if (entry.getValue().isPrimaryKey()) {
        indexSchema = entry.getValue();
      }
    }
    boolean ascending = true;
    List<OrderByExpressionImpl> orderByExpressions = getOrderByExpressions();
    if (orderByExpressions != null && orderByExpressions.size() != 0) {
      OrderByExpressionImpl expression = orderByExpressions.get(0);
      String columnName = expression.getColumnName();
      //if (columnName.equals(tableSchema.getIndices().get(indexSchema.getName()).getFields()[0])) {
        ascending = expression.isAscending();
      //}
    }
    BinaryExpression.Operator op = ascending ? BinaryExpression.Operator.greater : BinaryExpression.Operator.less;
    AtomicReference<String> usedIndex = new AtomicReference<>();
    SelectContextImpl context = ExpressionImpl.lookupIds(dbName, getClient().getCommon(), getClient(), getReplica(), count, tableSchema.getName(), indexSchema.getName(), isForceSelectOnServer(),
        op, null, getOrderByExpressions(), getNextKey(), getParms(), this, null, getNextKey(), null,
        getColumns(), indexSchema.getFields()[0], getNextShard(), getRecordCache(), usedIndex, false, getViewVersion(), getCounters(), getGroupByContext(), debug);
    setNextShard(context.getNextShard());
    setNextKey(context.getNextKey());
    NextReturn ret = new NextReturn();
    ret.setTableNames(context.getTableNames());
    ret.setIds(context.getCurrKeys());
    return ret;
  }


  @Override
  public NextReturn next(SelectStatementImpl.Explain explain) {
    return next(ReadManager.SELECT_PAGE_SIZE, explain);
  }

  @Override
  public boolean canUseIndex() {
    return true;
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
