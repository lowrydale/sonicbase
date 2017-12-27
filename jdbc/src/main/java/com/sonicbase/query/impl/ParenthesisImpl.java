package com.sonicbase.query.impl;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.Record;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.query.BinaryExpression;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.query.Expression;
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

  /**
   * ###############################
   * DON"T MODIFY THIS SERIALIZATION
   * ###############################
   */
  @Override
  public void serialize(short serializationVersion, DataOutputStream out) {
    try {
      super.serialize(serializationVersion, out);
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
  public void deserialize(short serializationVersion, DataInputStream in) {
    try {
      super.deserialize(serializationVersion, in);
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
  public NextReturn next(int count, SelectStatementImpl.Explain explain, AtomicLong currOffset, Limit limit, Offset offset, boolean b) {
    NextReturn ret = doNext(explain, count, currOffset, limit, offset);
    return ret;
//    return expression.next(count, eplain, currOffset, limit, offset, b);
  }

  private NextReturn doNext(SelectStatementImpl.Explain explain, int count, AtomicLong currOffset, Limit limit, Offset offset) {
    if (isNot) {
      TableSchema tableSchema = getClient().getCommon().getTables(dbName).get(getTableName());
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
      SelectContextImpl context = lookupIds(dbName, getClient().getCommon(), getClient(), getReplica(), count, tableSchema.getName(), indexSchema.getName(), isForceSelectOnServer(),
          op, null, getOrderByExpressions(), getNextKey(), getParms(), this, null, getNextKey(), null,
          getColumns(), indexSchema.getFields()[0], getNextShard(), getRecordCache(), usedIndex, isNot, getViewVersion(), getCounters(), getGroupByContext(), debug, currOffset, limit, offset);
      setNextShard(context.getNextShard());
      setNextKey(context.getNextKey());
      NextReturn ret = new NextReturn();
      ret.setTableNames(context.getTableNames());
      ret.setIds(context.getCurrKeys());
      return ret;
    }
    else {
      return null;//return expression.next(explain, currOffset, limit, offset);
    }
  }


  public NextReturn next(SelectStatementImpl.Explain explainBuilder, AtomicLong currOffset, Limit limit, Offset offset) {
    NextReturn ret = doNext(null,1000, currOffset, limit, offset);
    return ret;
  }

  public void setTableName(String tableName) {
    super.setTableName(tableName);
    expression.setTableName(tableName);
  }

  public void setClient(DatabaseClient client) {
    super.setClient(client);
    expression.setClient(client);
  }

  public void setParms(ParameterHandler parms) {
    super.setParms(parms);
    expression.setParms(parms);
  }

  public void getColumns(Set<ColumnImpl> columns) {
    expression.getColumns(columns);
  }

  public void setColumns(List<ColumnImpl> columns) {
    super.setColumns(columns);
    expression.setColumns(columns);
  }

  public void setTopLevelExpression(Expression topLevelExpression) {
    super.setTopLevelExpression(topLevelExpression);
    expression.setTopLevelExpression(topLevelExpression);
  }

  public void setOrderByExpressions(List<OrderByExpressionImpl> orderByExpressions) {
    super.setOrderByExpressions(orderByExpressions);
    expression.setOrderByExpressions(orderByExpressions);
  }

  public void setDebug(boolean debug) {
    super.setDebug(debug);
    expression.setDebug(debug);
  }

  public void setViewVersion(int viewVersion) {
    super.setViewVersion(viewVersion);
    expression.setViewVersion(viewVersion);
  }

  public void setCounters(Counter[] counters) {
    super.setCounters(counters);
    expression.setCounters(counters);
  }

  public void setLimit(Limit limit) {
    super.setLimit(limit);
    expression.setLimit(limit);
  }

  public void setGroupByContext(GroupByContext groupByContext) {
    super.setGroupByContext(groupByContext);
    expression.setGroupByContext(groupByContext);
  }

  public void setDbName(String dbName) {
    super.setDbName(dbName);
    expression.setDbName(dbName);
  }

  public void forceSelectOnServer(boolean forceSelectOnServer) {
    super.forceSelectOnServer(forceSelectOnServer);
    expression.forceSelectOnServer(forceSelectOnServer);
  }

  public void reset() {
    setNextShard(-1);
    setNextKey(null);
    expression.reset();
  }

  public void setRecordCache(RecordCache recordCache) {
    super.setRecordCache(recordCache);
    expression.setRecordCache(recordCache);
  }

  public void setReplica(Integer replica) {
    super.setReplica(replica);
    expression.setReplica(replica);
  }

  @Override
  public boolean canUseIndex() {
    return false;
//    if (isNot) {
//      return false;
//    }
//    return expression.canUseIndex();
  }

  @Override
  public boolean canSortWithIndex() {
    return false;
  }

  @Override
  public void queryRewrite() {
    //expression.queryRewrite();
  }

  @Override
  public ColumnImpl getPrimaryColumn() {
    return null;
  }
}
