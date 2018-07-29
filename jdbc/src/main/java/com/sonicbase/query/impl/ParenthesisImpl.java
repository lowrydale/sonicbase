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

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
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
      out.writeBoolean(isNot);
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public ExpressionImpl.Type getType() {
    return ExpressionImpl.Type.PARENTHESIS;
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
  public NextReturn next(int count, SelectStatementImpl.Explain explain, AtomicLong currOffset, AtomicLong countReturned,
                         Limit limit, Offset offset, boolean b, boolean analyze, int schemaRetryCount) {
    return doNext(count, currOffset, countReturned, limit, offset, schemaRetryCount);
  }

  private NextReturn doNext(int count, AtomicLong currOffset,
                            AtomicLong countReturned, Limit limit, Offset offset, int schemaRetryCount) {
    if (isNot) {
      TableSchema tableSchema = getClient().getCommon().getTables(dbName).get(getTableName());
      IndexSchema indexSchema = null;
      for (Map.Entry<String, IndexSchema> entry : tableSchema.getIndices().entrySet()) {
        if (entry.getValue().isPrimaryKey()) {
          indexSchema = entry.getValue();
        }
      }
      if (indexSchema == null) {
        throw new DatabaseException("primary index not found: table=" + getTableName());
      }
      boolean ascending = true;
      List<OrderByExpressionImpl> orderByExpressions = getOrderByExpressions();
      if (orderByExpressions != null && !orderByExpressions.isEmpty()) {
        OrderByExpressionImpl localExpression = orderByExpressions.get(0);
        ascending = localExpression.isAscending();
      }
      BinaryExpression.Operator op = ascending ? BinaryExpression.Operator.GREATER : BinaryExpression.Operator.LESS;
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
      indexLookup.setEvaluateExpression(isNot);

      SelectContextImpl context = indexLookup.lookup(this, getTopLevelExpression());
      setNextShard(context.getNextShard());
      setNextKey(context.getNextKey());
      NextReturn ret = new NextReturn();
      ret.setTableNames(context.getTableNames());
      ret.setIds(context.getCurrKeys());
      return ret;
    }
    else {
      return null;
    }
  }

  @Override
  public NextReturn next(SelectStatementImpl.Explain explainBuilder, AtomicLong currOffset, AtomicLong countReturned, Limit limit, Offset offset, int schemaRetryCount) {
    return doNext(1000, currOffset, countReturned, limit, offset, schemaRetryCount);
  }

  @Override
  public void setTableName(String tableName) {
    super.setTableName(tableName);
    expression.setTableName(tableName);
  }

  @Override
  public void setClient(DatabaseClient client) {
    super.setClient(client);
    expression.setClient(client);
  }

  @Override
  public void setParms(ParameterHandler parms) {
    super.setParms(parms);
    expression.setParms(parms);
  }

  @Override
  public void getColumns(Set<ColumnImpl> columns) {
    expression.getColumns(columns);
  }

  @Override
  public void setColumns(List<ColumnImpl> columns) {
    super.setColumns(columns);
    expression.setColumns(columns);
  }

  @Override
  public void setTopLevelExpression(Expression topLevelExpression) {
    super.setTopLevelExpression(topLevelExpression);
    expression.setTopLevelExpression(topLevelExpression);
  }

  @Override
  public void setOrderByExpressions(List<OrderByExpressionImpl> orderByExpressions) {
    super.setOrderByExpressions(orderByExpressions);
    expression.setOrderByExpressions(orderByExpressions);
  }

  @Override
  public void setDebug(boolean debug) {
    super.setDebug(debug);
    expression.setDebug(debug);
  }

  @Override
  public void setViewVersion(int viewVersion) {
    super.setViewVersion(viewVersion);
    expression.setViewVersion(viewVersion);
  }

  @Override
  public void setCounters(Counter[] counters) {
    super.setCounters(counters);
    expression.setCounters(counters);
  }

  @Override
  public void setProbe(boolean probe) {
    super.setProbe(probe);
    expression.setProbe(probe);
  }

  @Override
  public void setGroupByContext(GroupByContext groupByContext) {
    super.setGroupByContext(groupByContext);
    expression.setGroupByContext(groupByContext);
  }

  @Override
  public void setDbName(String dbName) {
    super.setDbName(dbName);
    expression.setDbName(dbName);
  }

  @Override
  public void forceSelectOnServer(boolean forceSelectOnServer) {
    super.forceSelectOnServer(forceSelectOnServer);
    expression.forceSelectOnServer(forceSelectOnServer);
  }

  @Override
  public void reset() {
    setNextShard(-1);
    setNextKey(null);
    expression.reset();
  }

  @Override
  public void setRecordCache(RecordCache recordCache) {
    super.setRecordCache(recordCache);
    expression.setRecordCache(recordCache);
  }

  @Override
  public void setReplica(Integer replica) {
    super.setReplica(replica);
    expression.setReplica(replica);
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
