package com.sonicbase.query.impl;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.Record;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.query.BinaryExpression;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.query.Expression;
import com.sonicbase.query.InExpression;
import com.sonicbase.schema.DataType;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.Offset;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class InExpressionImpl extends ExpressionImpl implements InExpression {
  private ParameterHandler parms;
  private String tableName;
  private List<ExpressionImpl> expressionList = new ArrayList<>();
  private ExpressionImpl leftExpression;
  private boolean isNot;

  public InExpressionImpl(DatabaseClient client, ParameterHandler parms, String tableName) {
    super.setClient(client);
    this.parms = parms;
    this.tableName = tableName;
  }

  public InExpressionImpl() {
  }


  public String toString() {
    String ret = "";
    ret += leftExpression.toString();
    if (isNot) {
      ret += " not";
    }
    ret += " in (";
    boolean first = true;
    for (ExpressionImpl item : expressionList) {
      if (first) {
        first = false;
      }
      else {
        ret += ", ";
      }
      ret += item.toString();
    }
    ret += ")";
    return ret;
  }

  public List<ExpressionImpl> getExpressionList() {
    return expressionList;
  }

  @Override
  public void setTableName(String tableName) {
    super.setTableName(tableName);
    leftExpression.setTableName(tableName);
    if (this.expressionList != null) {
      for (ExpressionImpl expression : expressionList) {
        expression.setTableName(tableName);
      }
    }
  }

  public ExpressionImpl getLeftExpression() {
    return leftExpression;
  }


  public void setColumn(String tableName, String columnName, String alias) {
    this.leftExpression = new ColumnImpl(null, null, tableName.toLowerCase(), columnName.toLowerCase(), alias.toLowerCase());
  }

  public void addValue(String value) throws UnsupportedEncodingException {
    expressionList.add(new ConstantImpl(value.getBytes("utf-8"), DataType.Type.VARCHAR.getValue()));
  }

  public void addValue(long value) {
    expressionList.add(new ConstantImpl(value, DataType.Type.BIGINT.getValue()));
  }

  public void setLeftExpression(Expression leftExpression) {
    this.leftExpression = (ExpressionImpl)leftExpression;
  }

  @Override
  public void getColumns(Set<ColumnImpl> columns) {
    leftExpression.getColumns(columns);
  }

  @Override
  public void setColumns(List<ColumnImpl> columns) {
    super.setColumns(columns);
    leftExpression.setColumns(columns);
  }

  @Override
  public void setProbe(boolean probe) {
    super.setProbe(probe);
    leftExpression.setProbe(probe);
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
      out.writeInt(expressionList.size());
      for (ExpressionImpl expression : expressionList) {
        serializeExpression(expression, out);
      }
      serializeExpression(leftExpression, out);
      out.writeBoolean(isNot);
      if (tableName == null) {
        out.writeByte(0);
      }
      else {
        out.writeByte(1);
        out.writeUTF(tableName);
      }
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public ExpressionImpl.Type getType() {
    return ExpressionImpl.Type.IN_EXPRESSION;
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
      int count = in.readInt();
      for (int i = 0; i < count; i++) {
        ExpressionImpl expression = deserializeExpression(in);
        expressionList.add(expression);
      }
      leftExpression = deserializeExpression(in);
      isNot = in.readBoolean();

      if (1 == in.readByte()) {
        tableName = in.readUTF();
      }
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public Object evaluateSingleRecord(TableSchema[] tableSchemas, Record[] records, ParameterHandler parms) {
    Object lhsValue = leftExpression.evaluateSingleRecord(tableSchemas, records, parms);
    if (lhsValue == null) {
      return false;
    }
    Comparator comparator = DataType.Type.getComparatorForValue(lhsValue);
    for (ExpressionImpl expression : expressionList) {
      if (comparator.compare(lhsValue, expression.evaluateSingleRecord(tableSchemas, records, parms)) == 0) {
        return !isNot;
      }
    }
    return isNot;
  }

  @Override
  public NextReturn next(int count, SelectStatementImpl.Explain explain, AtomicLong currOffset, AtomicLong countReturned,
                         Limit limit, Offset offset, boolean b, boolean analyze, int schemaRetryCount) {
    if (getNextShard() == -2) {
      return new NextReturn(new String[]{getTableName()}, null);
    }
    if (isNot()) {
      SelectContextImpl context = tableScan(dbName, getViewVersion(), getClient(), count,
          getClient().getCommon().getTables(dbName).get(getTableName()),
           getOrderByExpressions(), this, getParms(), getColumns(), getNextShard(), getNextKey(),
          getRecordCache(), getCounters(), getGroupByContext(), currOffset, limit, offset, isProbe(), isRestrictToThisServer(), getProcedureContext());
       if (context != null) {
         setNextShard(context.getNextShard());
         setNextKey(context.getNextKey());
         return new NextReturn(context.getTableNames(), context.getCurrKeys());
       }
    }
    Object[][][] ret = null;
    IndexSchema indexSchema = null;
    List<ExpressionImpl> localExpressionList = getExpressionList();
    ColumnImpl cNode = (ColumnImpl) getLeftExpression();
    String[] preferredIndexColumns = null;

    for (Map.Entry<String, IndexSchema> currIndexSchema : getClient().getCommon().getTables(dbName).get(getTableName()).getIndices().entrySet()) {
      String[] fields = currIndexSchema.getValue().getFields();
      if (fields[0].equals(cNode.getColumnName()) && (preferredIndexColumns == null || preferredIndexColumns.length > fields.length)) {
        preferredIndexColumns = fields;
        indexSchema = currIndexSchema.getValue();
      }
    }

    if (indexSchema == null) {
      SelectContextImpl context = tableScan(dbName, getViewVersion(), getClient(), count,
          getClient().getCommon().getTables(dbName).get(getTableName()),
          getOrderByExpressions(), this, getParms(), getColumns(), getNextShard(), getNextKey(),
          getRecordCache(), getCounters(), getGroupByContext(), currOffset, limit, offset, isProbe(), isRestrictToThisServer(),
          getProcedureContext());
      if (context != null) {
        setNextShard(context.getNextShard());
        setNextKey(context.getNextKey());
        return new NextReturn(context.getTableNames(), context.getCurrKeys());
      }
    }

    for (ExpressionImpl inValue : localExpressionList) {

      Object value = getValueFromExpression(parms, inValue);

      Object[] key = new Object[]{value};
      AtomicReference<String> usedIndex = new AtomicReference<>();

      IndexLookup indexLookup = new IndexLookup();
      indexLookup.setCount(count);
      indexLookup.setIndexName(indexSchema.getName());
      indexLookup.setLeftOp(BinaryExpression.Operator.EQUAL);
      indexLookup.setLeftKey(key);
      indexLookup.setLeftOriginalKey(key);
      indexLookup.setColumnName(cNode.getColumnName());
      indexLookup.setCurrOffset(currOffset);
      indexLookup.setCountReturned(countReturned);
      indexLookup.setLimit(limit);
      indexLookup.setOffset(offset);
      indexLookup.setSchemaRetryCount(schemaRetryCount);
      indexLookup.setUsedIndex(usedIndex);
      indexLookup.setEvaluateExpression(false);

      SelectContextImpl currRet = indexLookup.lookup(this, getTopLevelExpression());
      ret = aggregateResults(ret, currRet.getCurrKeys());
    }
    setNextShard(-2);
    return new NextReturn(new String[]{getTableName()}, ret);
  }


  @Override
  public NextReturn next(SelectStatementImpl.Explain explain, AtomicLong currOffset, AtomicLong countReturned, Limit limit, Offset offset, int schemaRetryCount) {
    return next(DatabaseClient.SELECT_PAGE_SIZE, explain, currOffset, countReturned, limit, offset, false, false, schemaRetryCount);
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

  public void addExpression(ExpressionImpl expression) {
    expressionList.add(expression);
  }

  public void setNot(boolean not) {
    this.isNot = not;
  }

  public boolean isNot() {
    return isNot;
  }
}
