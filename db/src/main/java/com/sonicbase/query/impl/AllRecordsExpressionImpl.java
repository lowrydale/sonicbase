package com.sonicbase.query.impl;


import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.Record;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.query.BinaryExpression;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.Offset;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
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
  public void serialize(short serializationVersion, DataOutputStream out) {
    try {
      super.serialize(serializationVersion, out);
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
  public void deserialize(short serilizationVersion, DataInputStream in) {
    try {
      super.deserialize(serializationVersion, in);
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
  public NextReturn next(int count, SelectStatementImpl.Explain explain, AtomicLong currOffset, AtomicLong countReturned, Limit limit, Offset offset,
                         boolean b, boolean analyze, int schemaRetryCount) {
    List<OrderByExpressionImpl> orderByExpressions = getOrderByExpressions();
    String orderByColumn = null;
    if (orderByExpressions != null && orderByExpressions.size() != 0) {
      orderByColumn = orderByExpressions.get(0).getColumnName();
    }
    TableSchema tableSchema = getClient().getCommon().getTables(dbName).get(getFromTable());
    IndexSchema indexSchema = null;
    IndexSchema primaryIndex = null;
    for (Map.Entry<String, IndexSchema> entry : tableSchema.getIndexes().entrySet()) {
      if (entry.getValue().isPrimaryKey()) {
        primaryIndex = entry.getValue();
      }
      if (orderByColumn == null || getGroupByContext() != null) {
        if (entry.getValue().isPrimaryKey()) {
          indexSchema = entry.getValue();
          break;
        }
      }
      else {
        //todo: check for compound fields
        if (entry.getValue().getFields()[0].equals(orderByColumn)) {
          indexSchema = entry.getValue();
          break;
        }
      }
    }
    if (indexSchema == null) {
      indexSchema = primaryIndex;
    }
//    TableSchema tableSchema = getClient().getCommon().getTables(dbName).get(getFromTable());
//    IndexSchema indexSchema = null;
//    for (Map.Entry<String, IndexSchema> entry : tableSchema.getIndexes().entrySet()) {
//      if (entry.getValue().isPrimaryKey()) {
//        indexSchema = entry.getValue();
//      }
//    }
    boolean ascending = true;
//    List<OrderByExpressionImpl> orderByExpressions = getOrderByExpressions();

    if (orderByExpressions != null && orderByExpressions.size() != 0) {
      OrderByExpressionImpl expression = orderByExpressions.get(0);
      String columnName = expression.getColumnName();
      //if (columnName.equals(tableSchema.getIndices().get(indexSchema.getName()).getFields()[0])) {
        ascending = expression.isAscending();
      //}
    }
    if (analyze) {
      return null;
    }
    else {

      setTableName(getFromTable());

      BinaryExpression.Operator op = ascending ? BinaryExpression.Operator.greater : BinaryExpression.Operator.less;
      AtomicReference<String> usedIndex = new AtomicReference<>();

      IndexLookup indexLookup = new IndexLookup();
      indexLookup.setCount(count);
      indexLookup.setIndexName(indexSchema.getName());
      indexLookup.setLeftOp(op);
      indexLookup.setLeftKey(getNextKey());
      indexLookup.setLeftOriginalKey(getNextKey());
      indexLookup.setColumnName(indexSchema.getFields()[0]);
      indexLookup.setCurrOffset(currOffset);
      indexLookup.setCountReturned(countReturned);
      indexLookup.setLimit(limit);
      indexLookup.setOffset(offset);
      indexLookup.setSchemaRetryCount(schemaRetryCount);
      indexLookup.setUsedIndex(usedIndex);
      indexLookup.setEvaluateExpression(false);

      SelectContextImpl context = indexLookup.lookup(this, getTopLevelExpression());
      setNextShard(context.getNextShard());
      setNextKey(context.getNextKey());
      NextReturn ret = new NextReturn();
      ret.setTableNames(context.getTableNames());
      ret.setIds(context.getCurrKeys());
      return ret;
    }
  }


  @Override
  public NextReturn next(SelectStatementImpl.Explain explain, AtomicLong currOffset, AtomicLong countReturned, Limit limit, Offset offset, int schemaRetryCount) {
    return next(DatabaseClient.SELECT_PAGE_SIZE, explain, currOffset, countReturned, limit, offset, false, false, schemaRetryCount);
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
