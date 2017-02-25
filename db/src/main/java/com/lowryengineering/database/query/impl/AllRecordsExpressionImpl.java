package com.lowryengineering.database.query.impl;


import com.lowryengineering.database.common.Record;
import com.lowryengineering.database.jdbcdriver.ParameterHandler;
import com.lowryengineering.database.query.BinaryExpression;
import com.lowryengineering.database.query.DatabaseException;
import com.lowryengineering.database.schema.IndexSchema;
import com.lowryengineering.database.schema.TableSchema;
import com.lowryengineering.database.server.ReadManager;

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
    SelectContextImpl context = ExpressionImpl.lookupIds(dbName, getClient().getCommon(), getClient(), getReplica(), count, tableSchema, indexSchema, isForceSelectOnServer(),
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
