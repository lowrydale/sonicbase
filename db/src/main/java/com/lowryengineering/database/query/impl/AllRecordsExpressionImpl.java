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
  public NextReturn next(int count) {
    TableSchema tableSchema = getClient().getCommon().getTables(dbName).get(getFromTable());
    IndexSchema indexSchema = tableSchema.getIndices().get("_1__primarykey");

    AtomicReference<String> usedIndex = new AtomicReference<>();
    SelectContextImpl context = ExpressionImpl.lookupIds(dbName, getClient().getCommon(), getClient(), getReplica(), count, tableSchema, indexSchema,
        BinaryExpression.Operator.greater, null, getOrderByExpressions(), getNextKey(), getParms(), this, null, getNextKey(), null,
        getColumns(), indexSchema.getFields()[0], getNextShard(), getRecordCache(), usedIndex, false, getViewVersion(), getCounters(), getGroupByContext(), debug);
    setNextShard(context.getNextShard());
    setNextKey(context.getNextKey());
    NextReturn ret = new NextReturn();
    ret.setTableNames(context.getTableNames());
    ret.setIds(context.getCurrKeys());
    return ret;
  }


  @Override
  public NextReturn next() {
    return next(ReadManager.SELECT_PAGE_SIZE);
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
