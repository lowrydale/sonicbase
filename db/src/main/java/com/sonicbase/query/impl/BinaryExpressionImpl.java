package com.sonicbase.query.impl;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.Record;
import com.sonicbase.common.SchemaOutOfSyncException;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.query.BinaryExpression;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.query.Expression;
import com.sonicbase.query.InExpression;
import com.sonicbase.schema.DataType;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.server.ReadManager;
import net.sf.jsqlparser.statement.select.Limit;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Responsible for
 */
public class BinaryExpressionImpl extends ExpressionImpl implements BinaryExpression {

  private Object originalLeftValue;
  private Object originalRightValue;
  private String indexName;
  private String columnName;

  private ExpressionImpl leftExpression;
  private ExpressionImpl rightExpression;
  private BinaryExpression.Operator operator;
  private boolean isNot;
  private boolean exhausted;
  private boolean rewroteQuery;

  public BinaryExpressionImpl(
      String columnName, BinaryExpression.Operator operator, DataType.Type type, Object value) {
    setOperator(operator);
    ColumnImpl columnNode = new ColumnImpl();
    columnNode.setColumnName(columnName);
    setLeftExpression(columnNode);

    ConstantImpl constant = new ConstantImpl();
    constant.setSqlType(type.getValue());
    constant.setValue(value);
    setRightExpression(constant);
  }

  public BinaryExpressionImpl() {
  }

  public String toString() {
    return leftExpression.toString() + " " + operator.getSymbol() + " " + rightExpression.toString();
  }

  public void setRecordCache(RecordCache recordCache) {
    super.setRecordCache(recordCache);
    leftExpression.setRecordCache(recordCache);
    rightExpression.setRecordCache(recordCache);
  }

  public BinaryExpressionImpl(
      BinaryExpression.Operator operator) {
    this.operator = operator;
  }

  public void setReplica(Integer replica) {
    super.setReplica(replica);
    leftExpression.setReplica(replica);
    rightExpression.setReplica(replica);
  }

  public void reset() {
    setNextShard(-1);
    setNextKey(null);
    exhausted = false;
    leftExpression.reset();
    rightExpression.reset();
  }

  public void setDebug(boolean debug) {
    super.setDebug(debug);
    leftExpression.setDebug(debug);
    rightExpression.setDebug(debug);
  }

  public void setViewVersion(long viewVersion) {
    super.setViewVersion(viewVersion);
    leftExpression.setViewVersion(viewVersion);
    rightExpression.setViewVersion(viewVersion);
  }

  public void setCounters(Counter[] counters) {
    super.setCounters(counters);
    leftExpression.setCounters(counters);
    rightExpression.setCounters(counters);
  }

  public void setLimit(Limit limit) {
    super.setLimit(limit);
    leftExpression.setLimit(limit);
    rightExpression.setLimit(limit);
  }

  public void setGroupByContext(GroupByContext groupByContext) {
    super.setGroupByContext(groupByContext);
    leftExpression.setGroupByContext(groupByContext);
    rightExpression.setGroupByContext(groupByContext);
  }

  public void setDbName(String dbName) {
    super.setDbName(dbName);
    leftExpression.setDbName(dbName);
    rightExpression.setDbName(dbName);
  }

  public void forceSelectOnServer(boolean forceSelectOnServer) {
    super.forceSelectOnServer(forceSelectOnServer);
    leftExpression.forceSelectOnServer(forceSelectOnServer);
    rightExpression.forceSelectOnServer(forceSelectOnServer);
  }


  @Override
  public NextReturn next(int count, SelectStatementImpl.Explain explain) {

    if (exhausted) {
      return null;
    }

    AtomicReference<String> usedIndex = new AtomicReference<>();

    if (BinaryExpression.Operator.or == operator) {
      return evaluateOrExpression(count, explain);
    }
    else if (BinaryExpression.Operator.and == operator) {
      NextReturn ret = evaluateAndExpression(count, usedIndex, explain);
//       if (leftExpression.nextShard == -2 ||| rightExpression.nextShard == -2) {
//         nextShard
//       }
      return ret;
    }
    else if (BinaryExpression.Operator.less == operator ||
        BinaryExpression.Operator.lessEqual == operator ||
        BinaryExpression.Operator.equal == operator ||
        BinaryExpression.Operator.notEqual == operator ||
        BinaryExpression.Operator.greater == operator ||
        BinaryExpression.Operator.greaterEqual == operator ||
        BinaryExpression.Operator.like == operator) {

      boolean canUseIndex = canUseIndex();
      Counter[] counters = getCounters();
      if (counters != null) {
        outer:
        for (Counter counter : counters) {
          TableSchema tableSchema = getClient().getCommon().getTables(dbName).get(counter.getTableName());
          for (Map.Entry<String, IndexSchema> entry : tableSchema.getIndexes().entrySet()) {
            if (entry.getValue().isPrimaryKey()) {
              if (!entry.getValue().getFields()[0].equals(counter.getColumnName())) {
                canUseIndex = false;
                break outer;
              }
            }
          }
        }
      }

      if (!canUseIndex || operator == Operator.like || (leftExpression instanceof ColumnImpl && rightExpression instanceof ColumnImpl)) {
        if (explain != null) {
          explain.appendSpaces();
          explain.getBuilder().append("Table scan: " + getTopLevelExpression().toString() + "\n");
        }

        SelectContextImpl context = ExpressionImpl.tableScan(dbName, getViewVersion(), getClient(), count, getClient().getCommon().getTables(dbName).get(getTableName()),
            getOrderByExpressions(), this, getParms(), getColumns(), getNextShard(), getNextKey(), getRecordCache(), getCounters(), getGroupByContext());
        if (context != null) {
          setNextShard(context.getNextShard());
          setNextKey(context.getNextKey());
          if (getNextShard() == -1 || getNextShard() == -2) {
            exhausted = true;
          }
          return new NextReturn(context.getTableNames(), context.getCurrKeys());
        }
//         return tableScan(count, getClient(), (ExpressionImpl) getTopLevelExpression(), getParms(), getTableName());
      }

      return evaluateRelationalOp(count, usedIndex, explain);
    }
    return null;
  }

  public void getColumnsInExpression(List<ColumnImpl> columns) {
    super.getColumnsInExpression(columns);
    leftExpression.getColumnsInExpression(columns);
    rightExpression.getColumnsInExpression(columns);
  }

  public NextReturn next(SelectStatementImpl.Explain explain) {
    return next(ReadManager.SELECT_PAGE_SIZE, explain);
  }

  private NextReturn evaluateRelationalOp(int count, AtomicReference<String> usedIndex, SelectStatementImpl.Explain explain) {
    try {
      ExpressionImpl rightExpression = getRightExpression();
      ExpressionImpl leftExpression = getLeftExpression();
      if (leftExpression instanceof ColumnImpl) {
        columnName = ((ColumnImpl) leftExpression).getColumnName();
        Object[] leftValue = null;
        if (getNextKey() != null) {
          leftValue = getNextKey();
        }
        else {
          Object currLeftValue = ExpressionImpl.getValueFromExpression(getParms(), rightExpression);
          TableSchema tableSchema = getClient().getCommon().getTables(dbName).get(getTableName());
          int offset = tableSchema.getFieldOffset(columnName);
          currLeftValue = tableSchema.getFields().get(offset).getType().getConverter().convert(currLeftValue);
          originalLeftValue = currLeftValue;
        }
        IndexSchema indexSchema = null;
        String[] preferredIndexColumns = null;
        for (Map.Entry<String, IndexSchema> entry : getClient().getCommon().getTables(dbName).get(getTableName()).getIndices().entrySet()) {
          String[] fields = entry.getValue().getFields();
          if (fields[0].equals(columnName)) {
            if (preferredIndexColumns == null || preferredIndexColumns.length > fields.length) {
              preferredIndexColumns = fields;
              indexName = entry.getKey();
              indexSchema = entry.getValue();
            }
          }
        }
        Object[] leftKey = null;
        if (leftValue != null) {
          List<Object[]> leftValues = new ArrayList<>();
          leftValues.add(leftValue);

          leftKey = leftValue;//ExpressionImpl.buildKey(leftValues, preferredIndexColumns);
        }

        if (explain != null) {
          explain.appendSpaces();
          explain.getBuilder().append("Index lookup for relational op: " + indexName +
              ", " + toString() + "\n");
        }

        GroupByContext groupByContext = getGroupByContext();

        if (groupByContext != null && !indexSchema.isPrimaryKey()) {
          SelectContextImpl context = ExpressionImpl.tableScan(dbName, getViewVersion(), getClient(), count, getClient().getCommon().getTables(dbName).get(getTableName()),
              getOrderByExpressions(), this, getParms(), getColumns(), getNextShard(), getNextKey(), getRecordCache(), getCounters(), getGroupByContext());
          if (context != null) {
            setNextShard(context.getNextShard());
            setNextKey(context.getNextKey());
            if (getNextShard() == -1 || getNextShard() == -2) {
              exhausted = true;
            }
            return new NextReturn(context.getTableNames(), context.getCurrKeys());
          }
        }
        else {
          SelectContextImpl context = ExpressionImpl.lookupIds(dbName, getClient().getCommon(), getClient(), getReplica(), count,
              getTableName(),
              indexName, isForceSelectOnServer(),
              operator, null, getOrderByExpressions(), leftKey, getParms(), getTopLevelExpression(), null, new Object[]{originalLeftValue}, null, getColumns(), columnName,
              getNextShard(), getRecordCache(), usedIndex, false, getViewVersion(), getCounters(), getGroupByContext(), debug);
          setNextShard(context.getNextShard());
          setNextKey(context.getNextKey());
          if (getNextShard() == -1 || getNextShard() == -2) {
            exhausted = true;
          }
          return new NextReturn(context.getTableNames(), context.getCurrKeys());
        }
      }
    }
    catch (SchemaOutOfSyncException e) {
      setNextShard(-1);
      throw e;
    }
    return null;
  }

  private void doQueryRewrite() {
    if (rewroteQuery) {
      return;
    }
    Map<String, Integer> mostUsed = new HashMap<>();
    getMostUsedIndex(mostUsed, leftExpression);
    getMostUsedIndex(mostUsed, rightExpression);

    String mostUsedColumn = null;
    String secondMostUsedColumn = null;
    int usedCount = 0;
    int secondMostUsedCount = 0;
    for (Map.Entry<String, Integer> entry : mostUsed.entrySet()) {
      if (entry.getValue() > usedCount) {
        usedCount = entry.getValue();
        mostUsedColumn = entry.getKey();
      }
      else if (entry.getValue() > secondMostUsedCount) {
        secondMostUsedCount = entry.getValue();
        secondMostUsedColumn = entry.getKey();
      }
    }

    if (usedCount == secondMostUsedCount) {
      for (Map.Entry<String, IndexSchema> indexSchema : getClient().getCommon().getTables(dbName).get(getTableName()).getIndices().entrySet()) {
        String[] fields = indexSchema.getValue().getFields();
        if (indexSchema.getValue().isPrimaryKey()) {
          if (fields[0].equals(mostUsedColumn)) {
            break;
          }
          if (fields[0].equals(secondMostUsedColumn)) {
            mostUsedColumn = secondMostUsedColumn;
            break;
          }
        }
      }
    }

    List<ExpressionImpl> andExpressions = new ArrayList<>();
    List<ExpressionImpl> otherExpressions = new ArrayList<>();
    extractAndExpressions(mostUsedColumn, andExpressions, otherExpressions, leftExpression);
    extractAndExpressions(mostUsedColumn, andExpressions, otherExpressions, rightExpression);

    if (otherExpressions.size() == 0 || andExpressions.size() == 0) {
      return;
    }
    leftExpression = buildQueryTree(andExpressions);
    rightExpression = buildQueryTree(otherExpressions);


    this.rewroteQuery = true;
  }

  private ExpressionImpl buildQueryTree(List<ExpressionImpl> expressions) {
    if (expressions.size() == 1) {
      return expressions.remove(0);
    }

    BinaryExpressionImpl ret = new BinaryExpressionImpl();

    ret.operator = Operator.and;
    ret.rewroteQuery = true;
    ret.rightExpression = expressions.remove(0);
    if (ret.rightExpression instanceof BinaryExpressionImpl) {
      ((BinaryExpressionImpl) ret.rightExpression).rewroteQuery = true;
    }
    if (expressions.size() == 1) {
      ret.leftExpression = expressions.remove(0);
      if (ret.leftExpression instanceof BinaryExpressionImpl) {
        ((BinaryExpressionImpl) ret.leftExpression).rewroteQuery = true;
      }
    }
    else {
      ret.leftExpression = buildQueryTree(expressions);
      if (ret.leftExpression instanceof BinaryExpressionImpl) {
        ((BinaryExpressionImpl) ret.leftExpression).rewroteQuery = true;
      }
    }
    ret.setReplica(getReplica());
    ret.setTableName(getTableName());
    ret.setClient(getClient());
    ret.setParms(getParms());
    ret.setTopLevelExpression(getTopLevelExpression());
    ret.setOrderByExpressions(getOrderByExpressions());
    ret.setRecordCache(getRecordCache());
    return ret;
  }

  private void getMostUsedIndex(Map<String, Integer> mostUsed, ExpressionImpl expression) {
    if (expression instanceof BinaryExpressionImpl) {
      BinaryExpressionImpl binary = ((BinaryExpressionImpl) expression);
      if (binary.getOperator() == Operator.and) {
        //andExpressions.add(this);
        getMostUsedIndex(mostUsed, binary.getLeftExpression());
        getMostUsedIndex(mostUsed, binary.getRightExpression());
      }
      else {
        AtomicBoolean leftIsColumn = new AtomicBoolean();
        String columnName = isIndexed(binary.leftExpression, leftIsColumn);
        if (columnName != null) {
          Integer value = mostUsed.get(columnName);
          if (value == null) {
            mostUsed.put(columnName, 1);
          }
          else {
            mostUsed.put(columnName, value + 1);
          }
        }
        columnName = isIndexed(binary.rightExpression, leftIsColumn);
        if (columnName != null) {
          Integer value = mostUsed.get(columnName);
          if (value == null) {
            mostUsed.put(columnName, 1);
          }
          else {
            mostUsed.put(columnName, value + 1);
          }
        }

      }
    }
  }

  private void extractAndExpressions(String mostUsedColumn,
                                     List<ExpressionImpl> andExpressions, List<ExpressionImpl> otherExpressions, ExpressionImpl expression) {
    if (expression instanceof BinaryExpressionImpl) {
      BinaryExpressionImpl binary = ((BinaryExpressionImpl) expression);
      if (binary.getOperator() == Operator.and) {
        //andExpressions.add(this);
        extractAndExpressions(mostUsedColumn, andExpressions, otherExpressions, binary.getLeftExpression());
        extractAndExpressions(mostUsedColumn, andExpressions, otherExpressions, binary.getRightExpression());
      }
      else {
        if (binary.getOperator().isRelationalOp()) {
          if (binary.leftExpression instanceof ColumnImpl) {
            if (binary.rightExpression instanceof ColumnImpl) {
              otherExpressions.add(binary);
            }
            else {
              if (((ColumnImpl) binary.leftExpression).getColumnName().equals(mostUsedColumn)) {
                andExpressions.add(binary);
              }
              else {
                otherExpressions.add(binary);
              }
            }
          }
          else if (binary.rightExpression instanceof ColumnImpl) {
            if (((ColumnImpl) binary.rightExpression).getColumnName().equals(mostUsedColumn)) {
              andExpressions.add(binary);
            }
            else {
              otherExpressions.add(binary);
            }
          }
        }
        else {
          otherExpressions.add(binary);
        }
      }
    }
  }

  private String isIndexed(ExpressionImpl expression, AtomicBoolean isColumn) {
    String rightColumn = null;
    if (expression instanceof ColumnImpl) {
      isColumn.set(true);
      rightColumn = ((ColumnImpl) expression).getColumnName();
      for (Map.Entry<String, IndexSchema> indexSchema : getClient().getCommon().getTables(dbName).get(getTableName()).getIndices().entrySet()) {
        String[] fields = indexSchema.getValue().getFields();
        if (//indexSchema.getValue().isPrimaryKey() &&
            fields[0].equals(rightColumn)) {
          return rightColumn;
        }
      }
    }
    return null;
  }

  public void queryRewrite() {
    if (BinaryExpression.Operator.and == operator) {
      doQueryRewrite();
    }
  }

  @Override
  public ColumnImpl getPrimaryColumn() {
    if (BinaryExpression.Operator.and == operator) {
      return leftExpression.getPrimaryColumn();
    }
    else if (getOperator().isRelationalOp()) {
      if (leftExpression instanceof ColumnImpl) {
        return (ColumnImpl) leftExpression;
      }
      if (rightExpression instanceof ColumnImpl) {
        return (ColumnImpl) rightExpression;
      }
    }
    return null;
  }

  private NextReturn evaluateAndExpression(int count, AtomicReference<String> usedIndex, SelectStatementImpl.Explain explain) {
    String rightColumn = null;
    String leftColumn = null;
    BinaryExpression.Operator leftOp = null;
    BinaryExpression.Operator rightOp = null;
    Object leftValue = null;
    Object rightValue = null;

    ExpressionImpl leftExpression = getLeftExpression();
    ExpressionImpl rightExpression = getRightExpression();

    boolean isRightColumnCompare = isColumnCompare(leftExpression);
    ExpressionImpl tmp = leftExpression;
    leftExpression = rightExpression;
    rightExpression = tmp;
    boolean isLeftColumnCompare = isColumnCompare(leftExpression);

    if (leftExpression instanceof BinaryExpressionImpl) {
      BinaryExpressionImpl leftOpExpr = (BinaryExpressionImpl) leftExpression;
      leftOp = leftOpExpr.getOperator();
      if (leftOp.isRelationalOp()) {
        if (leftOpExpr.getLeftExpression() instanceof ColumnImpl) {
          leftColumn = ((ColumnImpl) leftOpExpr.getLeftExpression()).getColumnName();
        }
        originalLeftValue = ExpressionImpl.getValueFromExpression(getParms(), leftOpExpr.getRightExpression());
      }
    }
    if (rightExpression instanceof BinaryExpressionImpl) {
      BinaryExpressionImpl rightOpExpr = (BinaryExpressionImpl) rightExpression;
      rightOp = rightOpExpr.getOperator();
      if (rightOp.isRelationalOp()) {
        if (rightOpExpr.getLeftExpression() instanceof ColumnImpl) {
          rightColumn = ((ColumnImpl) rightOpExpr.getLeftExpression()).getColumnName();
        }
        originalRightValue = ExpressionImpl.getValueFromExpression(getParms(), rightOpExpr.getRightExpression());
      }
    }

    if (getNextKey() != null) {
      leftValue = getNextKey()[0];
    }

    List<Object> leftValues = new ArrayList<>();
    if (leftValue != null) {
      leftValues.add(leftValue);
    }

    List<Object> rightValues = new ArrayList<>();
    if (rightValue != null) {
      rightValues.add(rightValue);
    }

    List<Object> originalLeftValues = new ArrayList<>();
    if (originalLeftValue != null) {
      originalLeftValues.add(originalLeftValue);
    }

    List<Object> originalRightValues = new ArrayList<>();
    if (originalRightValue != null) {
      originalRightValues.add(originalRightValue);
    }

    String[] preferredIndexColumns = null;
    for (Map.Entry<String, IndexSchema> indexSchema : getClient().getCommon().getTables(dbName).get(getTableName()).getIndices().entrySet()) {
      String[] fields = indexSchema.getValue().getFields();
      //if (fields[0].equals(leftColumn == null ? rightColumn : leftColumn)) {
      if ((leftColumn != null && fields[0].equals(leftColumn)) || (rightColumn != null && fields[0].equals(rightColumn))) {
        if (preferredIndexColumns == null || preferredIndexColumns.length > fields.length) {
          preferredIndexColumns = fields;
          indexName = indexSchema.getKey();
        }
      }
    }

    Operator leftEffectiveOp = leftOp;
    if (leftOp == Operator.lessEqual) {
      leftEffectiveOp = Operator.less;
    }
    else if (leftOp == Operator.greaterEqual) {
      leftEffectiveOp = Operator.greater;
    }
    Operator rightEffectiveOp = rightOp;
    if (rightOp == Operator.lessEqual) {
      rightEffectiveOp = Operator.less;
    }
    else if (rightOp == Operator.greaterEqual) {
      rightEffectiveOp = Operator.greater;
    }
    if (leftOp == Operator.equal) {
      leftEffectiveOp = rightEffectiveOp;
    }
    if (rightOp == Operator.equal) {
      rightEffectiveOp = leftEffectiveOp;
    }

    Object[] singleKey = null;
    if (leftValues.size() != 0) {
      singleKey = makeSingleKeyExpression(indexName, leftColumn, leftValues, leftOp, rightColumn, rightValues, rightOp);
    }
    Object[] originalSingleKey = makeSingleKeyExpression(indexName, leftColumn, originalLeftValues, leftOp, rightColumn, originalRightValues, rightOp);
    if (originalSingleKey != null) {
      if (explain != null) {
        explain.appendSpaces();
        explain.getBuilder().append("Merged key index lookup: index=" + indexName +
            ", " + leftColumn + " " + leftOp.getSymbol() + " " + leftValue + " and " + rightColumn + " " + rightOp.getSymbol() + " " + rightValue + "\n");
      }
      else {
        SelectContextImpl context = ExpressionImpl.lookupIds(dbName, getClient().getCommon(), getClient(), getReplica(), count,
            getTableName(),
            indexName, isForceSelectOnServer(),
            leftOp, null, getOrderByExpressions(), singleKey, getParms(), getTopLevelExpression(), null,
            originalSingleKey, null, getColumns(), leftColumn, getNextShard(),
            getRecordCache(), usedIndex, false, getViewVersion(), getCounters(), getGroupByContext(), debug);
        if (context != null) {
          setNextShard(context.getNextShard());
          setNextKey(context.getNextKey());
          if (getNextShard() == -1 || getNextShard() == -2) {
            exhausted = true;
          }
          return new NextReturn(context.getTableNames(), context.getCurrKeys());
        }
      }
    }
    else if (leftExpression instanceof ColumnImpl && rightExpression instanceof ColumnImpl) {
      if (explain != null) {
        explain.appendSpaces();
        explain.getBuilder().append("Table scan: " + getTopLevelExpression().toString() + "\n");
      }
      else {
        SelectContextImpl context = ExpressionImpl.tableScan(dbName, getViewVersion(), getClient(), count, getClient().getCommon().getTables(dbName).get(getTableName()),
            getOrderByExpressions(), this, getParms(), getColumns(), getNextShard(), getNextKey(), getRecordCache(), getCounters(), getGroupByContext());
        if (context != null) {
          setNextShard(context.getNextShard());
          setNextKey(context.getNextKey());
          if (getNextShard() == -1 || getNextShard() == -2) {
            exhausted = true;
          }
          return new NextReturn(context.getTableNames(), context.getCurrKeys());
        }
      }
      //return tableScan(count, getClient(), (ExpressionImpl) getTopLevelExpression(), getParms(), getTableName());
    }
    else if (leftEffectiveOp != rightEffectiveOp && !isLeftColumnCompare && !isRightColumnCompare && leftColumn != null && rightColumn != null && leftColumn.equals(rightColumn)) {
      if (indexName == null) {
        SelectContextImpl context = ExpressionImpl.tableScan(dbName, getViewVersion(), getClient(), count, getClient().getCommon().getTables(dbName).get(getTableName()),
            getOrderByExpressions(), this, getParms(), getColumns(), getNextShard(), getNextKey(), getRecordCache(), getCounters(), getGroupByContext());
        if (context != null) {
          setNextShard(context.getNextShard());
          setNextKey(context.getNextKey());
          if (getNextShard() == -1 || getNextShard() == -2) {
            exhausted = true;
          }
          return new NextReturn(context.getTableNames(), context.getCurrKeys());
        }
      }
      else {
        String[] indexFields = getClient().getCommon().getTables(dbName).get(getTableName()).getIndices().get(indexName).getFields();
        Object[] leftKey = null;
        if (leftValues.size() != 0) {
          leftKey = ExpressionImpl.buildKey(leftValues, indexFields);
        }
        Object[] rightKey = null;
        if (rightValues.size() != 0) {
          rightKey = ExpressionImpl.buildKey(rightValues, indexFields);
        }

        if (explain != null) {
          explain.appendSpaces();
          explain.getBuilder().append("Two-sided index lookup: index=" + indexName +
              ", " + leftColumn + " " + leftOp.getSymbol() + " " + leftValue + " and " + rightColumn + " " + rightOp.getSymbol() + " " + rightValue + "\n");
        }
        else {
          SelectContextImpl context = ExpressionImpl.lookupIds(dbName, getClient().getCommon(), getClient(), getReplica(), count,
              getTableName(),
              indexName, isForceSelectOnServer(),
              leftOp, rightOp, getOrderByExpressions(), leftKey, getParms(), getTopLevelExpression(), rightKey,
              new Object[]{originalLeftValue}, new Object[]{originalRightValue}, getColumns(), leftColumn, getNextShard(),
              getRecordCache(), usedIndex, false, getViewVersion(), getCounters(), getGroupByContext(), debug);
          if (context != null) {
            setNextShard(context.getNextShard());
            setNextKey(context.getNextKey());
            if (getNextShard() == -1 || getNextShard() == -2) {
              exhausted = true;
            }
            return new NextReturn(context.getTableNames(), context.getCurrKeys());
          }
        }
      }
    }
    else {
//      int andOrCount = getAndOrCount(leftExpression);
//      andOrCount += getAndOrCount(rightExpression);

      if (isLeftColumnCompare || (!leftExpression.canUseIndex() && !rightExpression.canUseIndex())) {
        if (explain != null) {
          explain.appendSpaces();
          explain.getBuilder().append("Table scan: " + getTopLevelExpression().toString() + "\n");
        }
        SelectContextImpl context = ExpressionImpl.tableScan(dbName, getViewVersion(), getClient(), count, getClient().getCommon().getTables(dbName).get(getTableName()),
            getOrderByExpressions(), this, getParms(), getColumns(), getNextShard(), getNextKey(), getRecordCache(), getCounters(), getGroupByContext());
        if (context != null) {
          setNextShard(context.getNextShard());
          setNextKey(context.getNextKey());
          if (getNextShard() == -1 || getNextShard() == -2) {
            exhausted = true;
          }
          return new NextReturn(context.getTableNames(), context.getCurrKeys());
        }
//        return tableScan(count, getClient(), (ExpressionImpl) getTopLevelExpression(), getParms(), getTableName());
      }
      return evaluateOneSidedIndex(new String[]{getTableName()}, count, leftExpression, rightExpression, leftColumn, leftOp,
          leftValue, rightColumn, rightOp, rightValue, explain);
    }
    return null;
  }

  private Object[] makeSingleKeyExpression(String indexName, String leftColumn, List<Object> leftValues, Operator leftOp, String rightColumn, List<Object> rightValues,
                                           Operator rightOp) {
    if (indexName == null) {
      return null;
    }
    String[] indexFields = getClient().getCommon().getTables(dbName).get(getTableName()).getIndices().get(indexName).getFields();
    if (indexFields.length < 2) {
      return null;
    }

    Object[] key = new Object[indexFields.length];
    if (leftOp == Operator.equal && rightOp == Operator.equal) {
      if (indexFields[0].equals(leftColumn)) {
        key[0] = leftValues.get(0);
        if (indexFields[1].equals(rightColumn)) {
          key[1] = rightValues.get(0);
        }
      }
      else if (indexFields[0].equals(rightColumn)) {
        key[0] = rightValues.get(0);
        if (indexFields[1].equals(leftColumn)) {
          key[1] = leftValues.get(0);
        }
      }
    }
    if (key[0] == null) {
      return null;
    }
    return key;
  }

  private boolean isColumnCompare(ExpressionImpl expression) {
    if (!(expression instanceof BinaryExpressionImpl)) {
      return false;
    }
    Expression leftExpression = ((BinaryExpressionImpl) expression).getLeftExpression();
    Expression rightExpression = ((BinaryExpressionImpl) expression).getRightExpression();
    if (leftExpression instanceof ColumnImpl && rightExpression instanceof ColumnImpl) {
      return true;
    }
    return false;
  }

  private int getAndOrCount(ExpressionImpl expression) {
    int count = 0;
    if (expression instanceof BinaryExpressionImpl) {
      if (((BinaryExpressionImpl) expression).getOperator() == Operator.and) {
        count++;
      }
      else if (((BinaryExpressionImpl) expression).getOperator() == Operator.or) {
        count++;
      }
      count += getAndOrCount(((BinaryExpressionImpl) expression).getLeftExpression());
      count += getAndOrCount(((BinaryExpressionImpl) expression).getRightExpression());
    }
    return count;
  }

  private NextReturn evaluateOneSidedIndex(
      final String[] tableNames, int count, ExpressionImpl leftExpression, ExpressionImpl rightExpression, String leftColumn, Operator leftOp,
      Object leftValue, String rightColumn, Operator rightOp, Object rightValue, SelectStatementImpl.Explain explain) {
    if (getNextShard() == -2) {
      return null;
    }
    final List<Object[]> retIds = new ArrayList<>();

    ExpressionImpl tmpExpression = null;
    Operator tmpOp = null;
    String tmpColumn = null;
    Object tmpValue = null;
    if (!leftExpression.canUseIndex() && rightExpression.canUseIndex()) {
      tmpExpression = leftExpression;
      leftExpression = rightExpression;
      rightExpression = tmpExpression;

      tmpOp = leftOp;
      leftOp = rightOp;
      rightOp = tmpOp;
      tmpColumn = leftColumn;
      leftColumn = rightColumn;
      rightColumn = tmpColumn;
      tmpValue = leftValue;
      leftValue = rightValue;
      rightValue = tmpValue;
    }

//    ExpressionImpl clauses = extractClausesForIndex();
    NextReturn leftIds = null;
//    }
//    else {
    if (leftExpression instanceof BinaryExpressionImpl && ((BinaryExpressionImpl) leftExpression).isNot()) {
      ExpressionImpl tmp = leftExpression;
      leftExpression = rightExpression;
      rightExpression = tmp;

      tmpOp = leftOp;
      leftOp = rightOp;
      rightOp = tmpOp;
      tmpColumn = leftColumn;
      leftColumn = rightColumn;
      rightColumn = tmpColumn;
      tmpValue = leftValue;
      leftValue = rightValue;
      rightValue = tmpValue;
    }
    if (explain != null) {
      explain.appendSpaces();
      if (!(leftExpression instanceof BinaryExpressionImpl)) {
        explain.getBuilder().append("One sided index lookup: index=" + indexName +
            ", indexedExpression=[" + leftColumn +  " " + leftOp.getSymbol() + " " + leftValue + "] otherExpression=[" + rightColumn + " " + rightOp.getSymbol() + " " + rightValue + "]\n");
      }
      explain.indent();
    }
    if (leftExpression instanceof InExpressionImpl) {
      ExpressionImpl tmp = leftExpression;
      leftExpression = rightExpression;
      rightExpression = leftExpression;
    }
    leftIds = leftExpression.next(count, explain);
    if (explain != null) {
      explain.appendSpaces();
      explain.getBuilder().append(" AND \n");
    }
//    }
//    if (leftIds == null || leftIds.getIds() == null) {
//      return null;
//    }
    //todo: loop while less than 200
    final TableSchema tableSchema = getClient().getCommon().getTables(dbName).get(tableNames[0]);
    if (true || !rightExpression.canUseIndex()) {
      if (explain != null) {
        explain.appendSpaces();
        explain.getBuilder().append("Read record evaluation: " + rightExpression.toString() + "\n");
      }
      try {
        if (leftIds != null && leftIds.getIds() != null) {
          ThreadPoolExecutor executor = getClient().getExecutor();
          List<Object[][]> batch = new ArrayList<>();
          List<Future> futures = new ArrayList<>();
          for (Object[][] id : leftIds.getKeys()) {
            batch.add(id);
            if (batch.size() >= 250) {
              final List<Object[][]> currBatch = batch;
              batch = new ArrayList<>();

              futures.add(executor.submit(new Callable() {
                @Override
                public Object call() throws Exception {
                  processBatch(currBatch, tableNames[0], tableSchema, retIds);
                  return null;
                }
              }));
            }
          }
          processBatch(batch, tableNames[0], tableSchema, retIds);

          for (Future future : futures) {
            try {
              future.get();
            }
            catch (Exception e) {
              throw new DatabaseException(e);
            }
          }
          Object[][][] ids = new Object[retIds.size()][][];
          int i = 0;
          for (Object[] id : retIds) {
            ids[i++] = new Object[][]{id};
          }

          return new NextReturn(tableNames, ids);
        }
      }
      finally {
        if (explain != null) {
          explain.outdent();
        }
      }
    }
    else {
      //todo: loop while less than 200
      NextReturn rightIds = rightExpression.next(explain);

      if (rightIds != null && rightIds.getIds() != null) {
        String[] columns = null;
        for (Map.Entry<String, IndexSchema> entry : tableSchema.getIndices().entrySet()) {
          if (entry.getValue().isPrimaryKey()) {
            columns = entry.getValue().getFields();
            break;
          }
        }

        List<IdEntry> keysToRead = new ArrayList<>();
        for (int i = 0; i < rightIds.getIds().length; i++) {
          Object[][] id = rightIds.getIds()[i];

          if (!getRecordCache().containsKey(tableSchema.getName(), id[0])) {
            keysToRead.add(new ExpressionImpl.IdEntry(i, id[0]));
          }
        }
        ExpressionImpl.doReadRecords(dbName, getClient(), count, isForceSelectOnServer(), tableSchema, keysToRead, columns, this.getColumns(), getRecordCache(), getViewVersion());
      }

//      for (Object[][] id : rightIds.getKeys()) {
//        Record record = ExpressionImpl.doReadRecord(getClient(), recordCache, id[0], getTableName(), selectColumns, getTopLevelExpression(), getParms());
//        if (record != null) {
//          retIds.add(id[0]);
//        }
//      }
//      Object[][][] ids = new Object[retIds.size()][][];
//      int i = 0;
//      for (Object[] id : retIds) {
//        ids[i++] = new Object[][]{id};
//      }
//
//      return new NextReturn(tableNames, ids);
      return rightIds;
    }
    return null;
  }


  private void processBatch(List<Object[][]> currBatch, String tableName, TableSchema tableSchema, List<Object[]> retIds) {
    for (Object[][] id : currBatch) {
      CachedRecord cachedRecord = getRecordCache().get(tableName, id[0]);
      Record record = cachedRecord == null ? null : cachedRecord.getRecord();
      if (record != null) {
        boolean pass = (Boolean) ((ExpressionImpl) getTopLevelExpression()).evaluateSingleRecord(new TableSchema[]{tableSchema}, new Record[]{record}, getParms());
        if (pass) {
          synchronized (retIds) {
            retIds.add(id[0]);
          }
        }
      }
      else {
        record = ExpressionImpl.doReadRecord(dbName, getClient(), isForceSelectOnServer(), getRecordCache(), id[0],
            getTableName(), getColumns(), getTopLevelExpression(), getParms(), getViewVersion(), debug);
        if (record != null) {
          retIds.add(id[0]);
        }
      }
    }
  }

  private NextReturn evaluateOrExpression(int count, SelectStatementImpl.Explain explain) {
    if (!leftExpression.canUseIndex() || !rightExpression.canUseIndex()) {
      SelectContextImpl context = ExpressionImpl.tableScan(dbName, getViewVersion(), getClient(), count, getClient().getCommon().getTables(dbName).get(getTableName()),
          getOrderByExpressions(), this, getParms(), getColumns(), getNextShard(), getNextKey(), getRecordCache(), getCounters(), getGroupByContext());
      if (context != null) {
        setNextShard(context.getNextShard());
        setNextKey(context.getNextKey());
        if (getNextShard() == -1 || getNextShard() == -2) {
          exhausted = true;
        }
        return new NextReturn(context.getTableNames(), context.getCurrKeys());
      }
      //return tableScan(count, getClient(), (ExpressionImpl) getTopLevelExpression(), getParms(), getTableName());
    }
    if (explain != null) {
      explain.indent();
    }
    NextReturn leftIds = leftExpression.next(explain);
    if (explain != null) {
      explain.outdent();
      explain.getBuilder().append(" OR \n");
      explain.indent();
    }
    NextReturn rightIds = rightExpression.next(explain);
    if (explain != null) {
      explain.outdent();
    }
    if (leftIds == null) {
      return rightIds;
    }
    if (rightIds == null) {
      return leftIds;
    }
    Object[][][] ids = ExpressionImpl.aggregateResults(leftIds.getKeys(), rightIds.getKeys());
    return new NextReturn(new String[]{getTableName()}, ids);
  }


  @Override
  public boolean canUseIndex() {
    boolean leftCanUse = false;
    boolean rightCanUse = false;
    if (leftExpression instanceof BinaryExpressionImpl) {
      leftCanUse = leftExpression.canUseIndex();
    }
    if (rightExpression instanceof BinaryExpressionImpl) {
      rightCanUse = rightExpression.canUseIndex();
    }
    if (leftCanUse || rightCanUse) {
      return true;
    }
    if (leftExpression instanceof ColumnImpl) {
      if (operator == Operator.notEqual) {
        return false;
      }
      String columnName = ((ColumnImpl) leftExpression).getColumnName();

//      if (operator == Operator.notEqual) {
//        return false;
//      }
      for (Map.Entry<String, IndexSchema> entry : getClient().getCommon().getTables(dbName).get(getTableName()).getIndices().entrySet()) {
        if (entry.getValue().getFields()[0].equals(columnName)) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public boolean canSortWithIndex() {
    return false;
  }

  public void getColumns(Set<ColumnImpl> columns) {
    leftExpression.getColumns(columns);
    rightExpression.getColumns(columns);
  }

  public void setColumns(List<ColumnImpl> columns) {
    super.setColumns(columns);
    leftExpression.setColumns(columns);
    rightExpression.setColumns(columns);
  }

  public void setTopLevelExpression(Expression topLevelExpression) {
    super.setTopLevelExpression(topLevelExpression);
    leftExpression.setTopLevelExpression(topLevelExpression);
    rightExpression.setTopLevelExpression(topLevelExpression);
  }

  public void setOrderByExpressions(List<OrderByExpressionImpl> orderByExpressions) {
    super.setOrderByExpressions(orderByExpressions);
    leftExpression.setOrderByExpressions(orderByExpressions);
    rightExpression.setOrderByExpressions(orderByExpressions);
  }

  public void setTableName(String tableName) {
    super.setTableName(tableName);
    leftExpression.setTableName(tableName);
    rightExpression.setTableName(tableName);
  }

  public void setClient(DatabaseClient client) {
    super.setClient(client);
    leftExpression.setClient(client);
    rightExpression.setClient(client);
  }

  public void setParms(ParameterHandler parms) {
    super.setParms(parms);
    leftExpression.setParms(parms);
    rightExpression.setParms(parms);
  }

  public boolean isNot() {
    return isNot;
  }

  public void setNot(boolean not) {
    isNot = not;
  }

  public void setLeftExpression(Expression leftExpression) {
    this.leftExpression = (ExpressionImpl) leftExpression;
  }

  public ExpressionImpl getLeftExpression() {
    return leftExpression;
  }

  public void setRightExpression(Expression rightExpression) {
    this.rightExpression = (ExpressionImpl) rightExpression;
  }

  public ExpressionImpl getRightExpression() {
    return rightExpression;
  }

  /**
   * ###############################
   * DON"T MODIFY THIS SERIALIZATION
   * ###############################
   */
  public void deserialize(DataInputStream in) {
    try {
      super.deserialize(in);
      int id = in.readInt();
      operator = BinaryExpression.Operator.getOperator(id);
      ExpressionImpl expression = ExpressionImpl.deserializeExpression(in);
      setLeftExpression(expression);
      expression = ExpressionImpl.deserializeExpression(in);
      setRightExpression(expression);
      isNot = in.readBoolean();
      exhausted = in.readBoolean();
      rewroteQuery = in.readBoolean();
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
  public void serialize(DataOutputStream out) {
    try {
      super.serialize(out);
      out.writeInt(operator.getId());
      ExpressionImpl value = getLeftExpression();
      ExpressionImpl.serializeExpression(value, out);
      value = getRightExpression();
      ExpressionImpl.serializeExpression(value, out);
      out.writeBoolean(isNot);
      out.writeBoolean(exhausted);
      out.writeBoolean(rewroteQuery);
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public boolean like(final String str, String expr) {
    String localExpr = expr;
    final String[] parts = localExpr.split("%");
    final boolean traillingOp = localExpr.endsWith("%");
    localExpr = "";
    for (int i = 0, l = parts.length; i < l; ++i) {
      final String[] p = parts[i].split("\\\\\\?");
      if (p.length > 1) {
        for (int y = 0, l2 = p.length; y < l2; ++y) {
          localExpr += p[y];
          if (i + 1 < l2) {
            localExpr += ".";
          }
        }
      }
      else {
        localExpr += parts[i];
      }
      if (i + 1 < l) {
        localExpr += "%";
      }
    }
    if (traillingOp) {
      localExpr += "%";
    }
    localExpr = localExpr.replace("?", ".");
    localExpr = localExpr.replace("%", ".*");
    return str.matches(localExpr);
  }

  @Override
  public Object evaluateSingleRecord(
      TableSchema[] tableSchemas, Record[] records,
      ParameterHandler parms) {
    try {
      BinaryExpression.Operator operator = getOperator();
      Object lhsValue = leftExpression.evaluateSingleRecord(tableSchemas, records, parms);
      Object rhsValue = rightExpression.evaluateSingleRecord(tableSchemas, records, parms);
      Comparator comparator = DataType.Type.getComparatorForValue(lhsValue);
      if (operator == BinaryExpression.Operator.equal) {
        if (lhsValue == null && rhsValue == null) {
          return true;
        }
        if (lhsValue == null || rhsValue == null) {
          return false;
        }
        if (comparator.compare(lhsValue, rhsValue) == 0) {
          if (isNot) {
            return false;
          }
          return true;
        }
        if (isNot) {
          return true;
        }
        return false;
      }
      if (operator == BinaryExpression.Operator.like) {
        if (lhsValue == null && rhsValue == null) {
          return true;
        }
        if (lhsValue == null || rhsValue == null) {
          return false;
        }
        String lhsStr = new String((byte[]) lhsValue, "utf-8");
        String rhsStr = new String((byte[]) rhsValue, "utf-8");
        if (like(lhsStr, rhsStr)) {
          if (isNot) {
            return false;
          }
          return true;
        }
        if (isNot) {
          return true;
        }
        return false;
      }
      else if (operator == BinaryExpression.Operator.notEqual) {
        if (lhsValue == null && rhsValue == null) {
          return false;
        }
        if (lhsValue == null || rhsValue == null) {
          return true;
        }
        if (comparator.compare(lhsValue, rhsValue) == 0) {
          if (isNot) {
            return true;
          }
          return false;
        }
        if (isNot) {
          return false;
        }
        return true;
      }
      else if (operator == BinaryExpression.Operator.less) {
        if (lhsValue == null || rhsValue == null) {
          return false;
        }
        if (comparator.compare(lhsValue, rhsValue) < 0) {
          if (isNot) {
            return false;
          }
          return true;
        }
        if (isNot) {
          return true;
        }
        return false;
      }
      else if (operator == BinaryExpression.Operator.lessEqual) {
        if (lhsValue == null || rhsValue == null) {
          return false;
        }
        if (comparator.compare(lhsValue, rhsValue) <= 0) {
          if (isNot) {
            return false;
          }
          return true;
        }
        if (isNot) {
          return true;
        }
        return false;
      }
      else if (operator == BinaryExpression.Operator.greater) {
        if (lhsValue == null || rhsValue == null) {
          return false;
        }
        if (comparator.compare(lhsValue, rhsValue) > 0) {
          if (isNot) {
            return false;
          }
          return true;
        }
        if (isNot) {
          return true;
        }
        return false;
      }
      else if (operator == BinaryExpression.Operator.greaterEqual) {
        if (lhsValue == null || rhsValue == null) {
          return false;
        }
        if (comparator.compare(lhsValue, rhsValue) >= 0) {
          if (isNot) {
            return false;
          }
          return true;
        }
        if (isNot) {
          return true;
        }
        return false;
      }
      else if (operator == BinaryExpression.Operator.and) {
        if (lhsValue == null || rhsValue == null) {
          return false;
        }
        if (isNot) {
          return !((Boolean) lhsValue && (Boolean) rhsValue);
        }
        return (Boolean) lhsValue && (Boolean) rhsValue;
      }
      else if (operator == BinaryExpression.Operator.or) {
        if (lhsValue == null || rhsValue == null) {
          return false;
        }
        if (isNot) {
          return !((Boolean) lhsValue || (Boolean) rhsValue);
        }
        return (Boolean) lhsValue || (Boolean) rhsValue;
      }
      if (isNot) {
        return true;
      }
    }
    catch (UnsupportedEncodingException | WrongTableException e) {
      return true;
    }
    return false;
  }


  @Override
  public ExpressionImpl.Type getType() {
    return ExpressionImpl.Type.binaryOp;
  }

  public void setOperator(BinaryExpression.Operator operator) {
    this.operator = operator;
  }

  public BinaryExpression.Operator getOperator() {
    return operator;
  }
}
