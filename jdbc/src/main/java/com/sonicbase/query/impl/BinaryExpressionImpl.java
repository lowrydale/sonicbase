package com.sonicbase.query.impl;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.Record;
import com.sonicbase.common.SchemaOutOfSyncException;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.procedure.StoredProcedureContextImpl;
import com.sonicbase.query.BinaryExpression;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.query.Expression;
import com.sonicbase.schema.DataType;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.Offset;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.sonicbase.query.BinaryExpression.Operator.*;

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class BinaryExpressionImpl extends ExpressionImpl implements BinaryExpression {

  private static final String TABLE_SCAN_STR = "Table scan: ";
  private static final String INVALID_OPERATOR_STR = "Invalid operator";
  private Object originalLeftValue;
  private Object originalRightValue;
  private String indexName;

  private ExpressionImpl leftExpression;
  private ExpressionImpl rightExpression;
  private Operator operator;
  private boolean isNot;
  private boolean exhausted;
  private boolean rewroteQuery;
  private boolean twoKeyLookup;
  private boolean oneKeyLookup;
  private boolean isTableScan;
  private boolean isRightKey;
  private static Map<Operator, SimpleComparator> simpleComparators = new EnumMap<>(Operator.class);

  public BinaryExpressionImpl(
      String columnName, Operator operator, DataType.Type type, Object value) {
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

  @Override
  public void setRestrictToThisServer(boolean restrictToThisServer) {
    super.setRestrictToThisServer(restrictToThisServer);
    leftExpression.setRestrictToThisServer(restrictToThisServer);
    rightExpression.setRestrictToThisServer(restrictToThisServer);
  }

  @Override
  public void setProcedureContext(StoredProcedureContextImpl procedureContext) {
    super.setProcedureContext(procedureContext);
    leftExpression.setProcedureContext(procedureContext);
    rightExpression.setProcedureContext(procedureContext);
  }

  public String toString() {
    return leftExpression.toString() + " " + operator.getSymbol() + " " + rightExpression.toString();
  }

  @Override
  public void setRecordCache(RecordCache recordCache) {
    super.setRecordCache(recordCache);
    leftExpression.setRecordCache(recordCache);
    rightExpression.setRecordCache(recordCache);
  }

  public BinaryExpressionImpl(
      Operator operator) {
    this.operator = operator;
  }

  @Override
  public void setReplica(Integer replica) {
    super.setReplica(replica);
    leftExpression.setReplica(replica);
    rightExpression.setReplica(replica);
  }

  @Override
  public void reset() {
    setNextShard(-1);
    setNextKey(null);
    exhausted = false;
    leftExpression.reset();
    rightExpression.reset();
  }

  @Override
  public void setDebug(boolean debug) {
    super.setDebug(debug);
    leftExpression.setDebug(debug);
    rightExpression.setDebug(debug);
  }

  @Override
  public void setViewVersion(int viewVersion) {
    super.setViewVersion(viewVersion);
    leftExpression.setViewVersion(viewVersion);
    rightExpression.setViewVersion(viewVersion);
  }

  @Override
  public void setCounters(Counter[] counters) {
    super.setCounters(counters);
    leftExpression.setCounters(counters);
    rightExpression.setCounters(counters);
  }

  @Override
  public void setGroupByContext(GroupByContext groupByContext) {
    super.setGroupByContext(groupByContext);
    leftExpression.setGroupByContext(groupByContext);
    rightExpression.setGroupByContext(groupByContext);
  }

  @Override
  public void setDbName(String dbName) {
    super.setDbName(dbName);
    leftExpression.setDbName(dbName);
    rightExpression.setDbName(dbName);
  }

  @Override
  public void forceSelectOnServer(boolean forceSelectOnServer) {
    super.forceSelectOnServer(forceSelectOnServer);
    leftExpression.forceSelectOnServer(forceSelectOnServer);
    rightExpression.forceSelectOnServer(forceSelectOnServer);
  }


  @Override
  public NextReturn next(int count, SelectStatementImpl.Explain explain, AtomicLong currOffset,
                         AtomicLong countReturned, Limit limit,
                         Offset offset, boolean evaluateExpression, boolean analyze, int schemaRetryCount) {

    if (exhausted) {
      return null;
    }

    AtomicReference<String> usedIndex = new AtomicReference<>();

    if (OR == operator) {
      return evaluateOrExpression(count, explain, currOffset, countReturned, limit, offset, analyze, schemaRetryCount);
    }
    else if (AND == operator) {
      return evaluateAndExpression(count, usedIndex, explain, currOffset, countReturned, limit, offset, analyze,
          evaluateExpression, schemaRetryCount);
    }
    else if (LESS == operator ||
        LESS_EQUAL == operator ||
        EQUAL == operator ||
        NOT_EQUAL == operator ||
        GREATER == operator ||
        GREATER_EQUAL == operator ||
        LIKE == operator) {

      boolean canUseIndex = canUseIndex();
      canUseIndex = checkIfCanUseIndexForCounters(canUseIndex);

      if (!canUseIndex || operator == LIKE ||
          (leftExpression instanceof ColumnImpl && rightExpression instanceof ColumnImpl)) {
        if (explain != null) {
          explain.appendSpaces();
          explain.getBuilder().append(TABLE_SCAN_STR + getTopLevelExpression().toString() + "\n");
        }

        return doTableScan(count, currOffset, limit, offset, analyze);
      }
      return evaluateRelationalOp(count, usedIndex, explain, currOffset, countReturned, limit, offset,
          evaluateExpression, analyze, schemaRetryCount);
    }
    return null;
  }

  private boolean checkIfCanUseIndexForCounters(boolean canUseIndex) {
    Counter[] counters = getCounters();
    if (counters != null) {
      outer:
      for (Counter counter : counters) {
        TableSchema tableSchema = getClient().getCommon().getTables(dbName).get(counter.getTableName());
        for (Map.Entry<String, IndexSchema> entry : tableSchema.getIndices().entrySet()) {
          if (entry.getValue().isPrimaryKey() && !entry.getValue().getFields()[0].equals(counter.getColumnName())) {
            canUseIndex = false;
            break outer;
          }
        }
      }
    }
    return canUseIndex;
  }

  private boolean expressionContainsMath(ExpressionImpl expression) {
    if (expression instanceof BinaryExpression) {
      BinaryExpressionImpl binaryExpression = (BinaryExpressionImpl) expression;
      if (binaryExpression.operator == AND || binaryExpression.operator == OR) {
        return expressionContainsMath(leftExpression) || expressionContainsMath(rightExpression);
      }
      else if (binaryExpression.operator == PLUS ||
            binaryExpression.operator == MINUS ||
            binaryExpression.operator == TIMES ||
            binaryExpression.operator == DIVIDE ||
            binaryExpression.operator == BITWISE_AND ||
            binaryExpression.operator == BITWISE_OR ||
            binaryExpression.operator == BITWISE_X_OR ||
            binaryExpression.operator == MODULO) {
        return true;
      }
    }

    return false;
  }

  @Override
  public void getColumnsInExpression(List<ColumnImpl> columns) {
    super.getColumnsInExpression(columns);
    leftExpression.getColumnsInExpression(columns);
    rightExpression.getColumnsInExpression(columns);
  }

  @Override
  public NextReturn next(SelectStatementImpl.Explain explain, AtomicLong currOffset, AtomicLong countReturned,
                         Limit limit, Offset offset, int schemaRetryCount) {
    return next(DatabaseClient.SELECT_PAGE_SIZE, explain, currOffset, countReturned, limit, offset,
        false, false, schemaRetryCount);
  }

  private NextReturn evaluateRelationalOp(int count, AtomicReference<String> usedIndex, SelectStatementImpl.Explain explain,
                                          AtomicLong currOffset, AtomicLong countReturned, Limit limit, Offset offset,
                                          boolean evaluateExpression, boolean analyze, int schemaRetryCount) {
    try {
      ExpressionImpl localRightExpression = getRightExpression();
      ExpressionImpl localLeftExpression = getLeftExpression();
      if (localLeftExpression instanceof ColumnImpl) {
        String columnName = ((ColumnImpl) localLeftExpression).getColumnName();
        GetLeftValue getLeftValue = new GetLeftValue(localRightExpression, localLeftExpression, columnName).invoke();
        columnName = getLeftValue.getColumnName();
        Object[] leftValue = getLeftValue.getLeftValue();

        IndexSchema indexSchema = getIndexSchema(columnName);
        Object[] leftKey = getLeftKey(leftValue);

        if (explain != null) {
          explain.appendSpaces();
          explain.getBuilder().append("Index lookup for relational op: " + indexName +
              ", " + toString() + "\n");
        }

        GroupByContext groupByContext = getGroupByContext();

        if (groupByContext != null && !indexSchema.isPrimaryKey() ||
            expressionContainsMath(localLeftExpression) || expressionContainsMath(localRightExpression)) {
          return doTableScan(count, currOffset, limit, offset, analyze);
        }
        else {
          return doSingleKeyIndexLookup(count, usedIndex, currOffset, countReturned, limit, offset, evaluateExpression,
              analyze, schemaRetryCount, columnName, indexSchema, leftKey);
        }
      }
    }
    catch (SchemaOutOfSyncException e) {
      setNextShard(-1);
      throw e;
    }
    return null;
  }

  private Object[] getLeftKey(Object[] leftValue) {
    Object[] leftKey = null;
    if (leftValue != null) {
      List<Object[]> leftValues = new ArrayList<>();
      leftValues.add(leftValue);

      leftKey = leftValue;
    }
    return leftKey;
  }

  private IndexSchema getIndexSchema(String columnName) {
    IndexSchema indexSchema = null;
    String[] preferredIndexColumns = null;
    for (Map.Entry<String, IndexSchema> entry :
        getClient().getCommon().getTables(dbName).get(getTableName()).getIndices().entrySet()) {
      String[] fields = entry.getValue().getFields();
      if (fields[0].equals(columnName) && (preferredIndexColumns == null || preferredIndexColumns.length > fields.length)) {
        preferredIndexColumns = fields;
        indexName = entry.getKey();
        indexSchema = entry.getValue();
      }
    }
    return indexSchema;
  }

  private NextReturn doSingleKeyIndexLookup(int count, AtomicReference<String> usedIndex, AtomicLong currOffset,
                                            AtomicLong countReturned, Limit limit, Offset offset, boolean evaluateExpression,
                                            boolean analyze, int schemaRetryCount, String columnName, IndexSchema indexSchema,
                                            Object[] leftKey) {
    if (analyze) {
      oneKeyLookup = true;
    }
    else {
      int fieldCount = indexSchema.getFields().length;
      Object[] leftOriginalKey = new Object[fieldCount];
      leftOriginalKey[0] = originalLeftValue;

      IndexLookup indexLookup = createIndexLookup();
      indexLookup.setCount(count);
      indexLookup.setIndexName(indexName);
      indexLookup.setLeftOp(operator);
      indexLookup.setLeftKey(leftKey);
      indexLookup.setLeftOriginalKey(leftOriginalKey);
      indexLookup.setColumnName(columnName);
      indexLookup.setCurrOffset(currOffset);
      indexLookup.setCountReturned(countReturned);
      indexLookup.setLimit(limit);
      indexLookup.setOffset(offset);
      indexLookup.setSchemaRetryCount(schemaRetryCount);
      indexLookup.setUsedIndex(usedIndex);
      indexLookup.setEvaluateExpression(evaluateExpression);


      SelectContextImpl context = indexLookup.lookup(this, getTopLevelExpression());
      setNextShard(context.getNextShard());
      setNextKey(context.getNextKey());
      if (getNextShard() == -1 || getNextShard() == -2) {
        exhausted = true;
      }
      return new NextReturn(context.getTableNames(), context.getCurrKeys());
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

    mostUsedColumn = getSecondMostUsedColumn(mostUsedColumn, secondMostUsedColumn, usedCount, secondMostUsedCount);

    List<ExpressionImpl> andExpressions = new ArrayList<>();
    List<ExpressionImpl> otherExpressions = new ArrayList<>();
    extractAndExpressions(mostUsedColumn, andExpressions, otherExpressions, leftExpression);
    extractAndExpressions(mostUsedColumn, andExpressions, otherExpressions, rightExpression);

    if (otherExpressions.isEmpty() || andExpressions.isEmpty()) {
      return;
    }
    leftExpression = buildQueryTree(andExpressions);
    rightExpression = buildQueryTree(otherExpressions);


    this.rewroteQuery = true;
  }

  private String getSecondMostUsedColumn(String mostUsedColumn, String secondMostUsedColumn, int usedCount,
                                         int secondMostUsedCount) {
    if (usedCount == secondMostUsedCount) {
      for (Map.Entry<String, IndexSchema> indexSchema :
          getClient().getCommon().getTables(dbName).get(getTableName()).getIndices().entrySet()) {
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
    return mostUsedColumn;
  }

  private ExpressionImpl buildQueryTree(List<ExpressionImpl> expressions) {
    if (expressions.size() == 1) {
      return expressions.remove(0);
    }

    BinaryExpressionImpl ret = new BinaryExpressionImpl();

    ret.operator = AND;
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
      if (binary.getOperator() == AND) {
        getMostUsedIndex(mostUsed, binary.getLeftExpression());
        getMostUsedIndex(mostUsed, binary.getRightExpression());
      }
      else {
        doGetMostUsedIndex(mostUsed, binary);
      }
    }
  }

  private void doGetMostUsedIndex(Map<String, Integer> mostUsed, BinaryExpressionImpl binary) {
    AtomicBoolean leftIsColumn = new AtomicBoolean();
    String localColumnName = isIndexed(binary.leftExpression, leftIsColumn);
    if (localColumnName != null) {
      Integer value = mostUsed.get(localColumnName);
      if (value == null) {
        mostUsed.put(localColumnName, 1);
      }
      else {
        mostUsed.put(localColumnName, value + 1);
      }
    }
    localColumnName = isIndexed(binary.rightExpression, leftIsColumn);
    if (localColumnName != null) {
      Integer value = mostUsed.get(localColumnName);
      if (value == null) {
        mostUsed.put(localColumnName, 1);
      }
      else {
        mostUsed.put(localColumnName, value + 1);
      }
    }
  }

  private void extractAndExpressions(String mostUsedColumn,
                                     List<ExpressionImpl> andExpressions, List<ExpressionImpl> otherExpressions,
                                     ExpressionImpl expression) {
    if (expression instanceof BinaryExpressionImpl) {
      BinaryExpressionImpl binary = ((BinaryExpressionImpl) expression);
      if (binary.getOperator() == AND) {
        extractAndExpressions(mostUsedColumn, andExpressions, otherExpressions, binary.getLeftExpression());
        extractAndExpressions(mostUsedColumn, andExpressions, otherExpressions, binary.getRightExpression());
      }
      else {
        if (binary.getOperator().isRelationalOp()) {
          getRelationalOp(mostUsedColumn, andExpressions, otherExpressions, binary);
        }
        else {
          otherExpressions.add(binary);
        }
      }
    }
  }

  private void getRelationalOp(String mostUsedColumn, List<ExpressionImpl> andExpressions,
                               List<ExpressionImpl> otherExpressions, BinaryExpressionImpl binary) {
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

  String isIndexed(ExpressionImpl expression, AtomicBoolean isColumn) {
    String rightColumn = null;
    if (expression instanceof ColumnImpl) {
      isColumn.set(true);
      rightColumn = ((ColumnImpl) expression).getColumnName();
      for (Map.Entry<String, IndexSchema> indexSchema :
          getClient().getCommon().getTables(dbName).get(getTableName()).getIndices().entrySet()) {
        String[] fields = indexSchema.getValue().getFields();
        if (//indexSchema.getValue().isPrimaryKey() &&
            fields[0].equals(rightColumn)) {
          return rightColumn;
        }
      }
    }
    return null;
  }

  @Override
  public void queryRewrite() {
    if (AND == operator) {
      doQueryRewrite();
    }
  }

  @Override
  public ColumnImpl getPrimaryColumn() {
    if (AND == operator) {
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

  protected NextReturn evaluateAndExpression(int count, AtomicReference<String> usedIndex, SelectStatementImpl.Explain explain,
                                           AtomicLong currOffset, AtomicLong countReturned, Limit limit, Offset offset,
                                           boolean analyze, boolean evaluateExpression, int schemaRetryCount) {
    Object rightValue = null;

    ExpressionImpl localLeftExpression = getLeftExpression();
    ExpressionImpl localRightExpression = getRightExpression();

    boolean isRightColumnCompare = isColumnCompare(localLeftExpression);
    ExpressionImpl tmp = localLeftExpression;
    localLeftExpression = localRightExpression;
    localRightExpression = tmp;
    boolean isLeftColumnCompare = isColumnCompare(localLeftExpression);

    GetLeftAndRightValues getLeftAndRightValues = new GetLeftAndRightValues(localLeftExpression, localRightExpression).invoke();
    String rightColumn = getLeftAndRightValues.getRightColumn();
    String leftColumn = getLeftAndRightValues.getLeftColumn();
    Operator leftOp = getLeftAndRightValues.getLeftOp();
    Operator rightOp = getLeftAndRightValues.getRightOp();
    Object leftValue = getLeftAndRightValues.getLeftValue();
    List<Object> leftValues = getLeftAndRightValues.getLeftValues();
    List<Object> rightValues = getLeftAndRightValues.getRightValues();
    List<Object> originalLeftValues = getLeftAndRightValues.getOriginalLeftValues();
    List<Object> originalRightValues = getLeftAndRightValues.getOriginalRightValues();

    String[] preferredIndexColumns = null;

    getIndexName(rightColumn, leftColumn, preferredIndexColumns);

    GetEffectiveOps getEffectiveOps = new GetEffectiveOps(leftOp, rightOp).invoke();
    Operator leftEffectiveOp = getEffectiveOps.getLeftEffectiveOp();
    Operator rightEffectiveOp = getEffectiveOps.getRightEffectiveOp();

    Object[] singleKey = null;
    if (!leftValues.isEmpty()) {
      singleKey = makeSingleKeyExpression(indexName, leftColumn, leftValues, leftOp, rightColumn, rightValues, rightOp);
    }
    Object[] originalSingleKey = makeSingleKeyExpression(indexName, leftColumn, originalLeftValues, leftOp, rightColumn,
        originalRightValues, rightOp);
    if (originalSingleKey != null) {
      return doStandardSingleKeyLookup(count, usedIndex, explain, currOffset, countReturned, limit, offset, analyze,
          evaluateExpression, schemaRetryCount, rightColumn, leftColumn, leftOp, rightOp, leftValue, rightValue,
          singleKey, originalSingleKey);
    }
    else if (localLeftExpression instanceof ColumnImpl && localRightExpression instanceof ColumnImpl) {
      if (columnCompareTableScan(explain)) {
        return doTableScan(count, currOffset, limit, offset, analyze);
      }
    }
    else if (leftEffectiveOp != rightEffectiveOp && !isLeftColumnCompare && !isRightColumnCompare && leftColumn != null &&
        rightColumn != null && leftColumn.equals(rightColumn)) {
      if (indexName == null) {
        return doTableScan(count, currOffset, limit, offset, analyze);
      }
      else {
        return doStandardTwoKeyIndexLookup(count, usedIndex, explain, currOffset, countReturned, limit, offset, analyze,
            evaluateExpression, schemaRetryCount, rightColumn, leftColumn, leftOp, rightOp, leftValue, rightValue,
            leftValues, rightValues);
      }
    }
    else {
      return doTableScanOrOneSidedIndexLookup(count, explain, currOffset, countReturned, limit, offset, analyze,
          schemaRetryCount, rightValue, localLeftExpression, localRightExpression, isLeftColumnCompare, rightColumn,
          leftColumn, leftOp, rightOp, leftValue);
    }
    return null;
  }

  private NextReturn doTableScanOrOneSidedIndexLookup(int count, SelectStatementImpl.Explain explain,
                                                      AtomicLong currOffset, AtomicLong countReturned, Limit limit,
                                                      Offset offset, boolean analyze, int schemaRetryCount, Object rightValue,
                                                      ExpressionImpl localLeftExpression, ExpressionImpl localRightExpression,
                                                      boolean isLeftColumnCompare, String rightColumn, String leftColumn,
                                                      Operator leftOp, Operator rightOp, Object leftValue) {
    if (isLeftColumnCompare || (!localLeftExpression.canUseIndex() && !localRightExpression.canUseIndex())) {
      if (explain != null) {
        explain.appendSpaces();
        explain.getBuilder().append(TABLE_SCAN_STR + getTopLevelExpression().toString() + "\n");
      }
      return doTableScan(count, currOffset, limit, offset, analyze);
    }
    return evaluateOneSidedIndex(new String[]{getTableName()}, count, localLeftExpression, localRightExpression, leftColumn, leftOp,
        leftValue, rightColumn, rightOp, rightValue, explain, currOffset, countReturned, limit, offset, analyze, schemaRetryCount);
  }

  private NextReturn doTableScan(int count, AtomicLong currOffset, Limit limit, Offset offset, boolean analyze) {
    if (analyze) {
      isTableScan = true;
      return null;
    }
    else {
      SelectContextImpl context = tableScan(dbName, getViewVersion(), getClient(), count,
          getClient().getCommon().getTables(dbName).get(getTableName()),
          getOrderByExpressions(), this, getParms(), getColumns(), getNextShard(), getNextKey(),
          getRecordCache(), getCounters(), getGroupByContext(), currOffset, limit, offset, isProbe(),
          isRestrictToThisServer(), getProcedureContext());
      if (context != null) {
        setNextShard(context.getNextShard());
        setNextKey(context.getNextKey());
        if (getNextShard() == -1 || getNextShard() == -2) {
          exhausted = true;
        }
        return new NextReturn(context.getTableNames(), context.getCurrKeys());
      }
    }
    return null;
  }


  private void getIndexName(String rightColumn, String leftColumn, String[] preferredIndexColumns) {
    for (Map.Entry<String, IndexSchema> indexSchema :
        getClient().getCommon().getTables(dbName).get(getTableName()).getIndices().entrySet()) {
      String[] fields = indexSchema.getValue().getFields();
      if (((leftColumn != null && fields[0].equals(leftColumn)) || (rightColumn != null &&
          fields[0].equals(rightColumn))) && (preferredIndexColumns == null || preferredIndexColumns.length > fields.length)) {
        preferredIndexColumns = fields;
        indexName = indexSchema.getKey();
      }
    }
  }

  private boolean columnCompareTableScan(SelectStatementImpl.Explain explain) {
    if (explain != null) {
      explain.appendSpaces();
      explain.getBuilder().append(TABLE_SCAN_STR + getTopLevelExpression().toString() + "\n");
    }
    else {
      return true;
    }
    return false;
  }

  private NextReturn doStandardSingleKeyLookup(int count, AtomicReference<String> usedIndex,
                                               SelectStatementImpl.Explain explain, AtomicLong currOffset,
                                               AtomicLong countReturned, Limit limit, Offset offset, boolean analyze,
                                               boolean evaluateExpression, int schemaRetryCount, String rightColumn,
                                               String leftColumn, Operator leftOp, Operator rightOp, Object leftValue,
                                               Object rightValue, Object[] singleKey, Object[] originalSingleKey) {
    if (explain != null) {
      explain.appendSpaces();
      explain.getBuilder().append("Merged key index lookup: index=" + indexName +
          ", " + leftColumn + " " + leftOp.getSymbol() + " " + leftValue + " and " + rightColumn + " " +
          rightOp.getSymbol() + " " + rightValue + "\n");
    }
    else {
      if (analyze) {
        return null;
      }
      else {
        IndexLookup indexLookup = createIndexLookup();
        indexLookup.setCount(count);
        indexLookup.setIndexName(indexName);
        indexLookup.setLeftOp(leftOp);
        indexLookup.setLeftKey(singleKey);
        indexLookup.setLeftOriginalKey(originalSingleKey);
        indexLookup.setColumnName(leftColumn);
        indexLookup.setCurrOffset(currOffset);
        indexLookup.setCountReturned(countReturned);
        indexLookup.setLimit(limit);
        indexLookup.setOffset(offset);
        indexLookup.setSchemaRetryCount(schemaRetryCount);
        indexLookup.setUsedIndex(usedIndex);
        indexLookup.setEvaluateExpression(evaluateExpression);

        SelectContextImpl context = indexLookup.lookup(this, getTopLevelExpression());
        if (context != null) {
          setLastShard(context.getLastShard());
          setIsCurrPartitions(context.isCurrPartitions());
          setNextShard(context.getNextShard());
          setNextKey(context.getNextKey());
          if (getNextShard() == -1 || getNextShard() == -2) {
            exhausted = true;
          }
          return new NextReturn(context.getTableNames(), context.getCurrKeys());
        }
      }
    }
    return null;
  }

  private NextReturn doStandardTwoKeyIndexLookup(int count, AtomicReference<String> usedIndex,
                                                 SelectStatementImpl.Explain explain, AtomicLong currOffset,
                                                 AtomicLong countReturned, Limit limit, Offset offset, boolean analyze,
                                                 boolean evaluateExpression, int schemaRetryCount, String rightColumn,
                                                 String leftColumn, Operator leftOp, Operator rightOp, Object leftValue,
                                                 Object rightValue, List<Object> leftValues, List<Object> rightValues) {
    String[] indexFields = getClient().getCommon().getTables(dbName).get(getTableName()).getIndices().get(indexName).getFields();
    Object[] leftKey = null;
    GetLeftAndRightKey getLeftAndRightKey = new GetLeftAndRightKey(leftValues, rightValues, indexFields, leftKey).invoke();
    leftKey = getLeftAndRightKey.getLeftKey();
    Object[] rightKey = getLeftAndRightKey.getRightKey();

    if (explain != null) {
      explain.appendSpaces();
      explain.getBuilder().append("Two-sided index lookup: index=" + indexName +
          ", " + leftColumn + " " + leftOp.getSymbol() + " " + leftValue + " and " + rightColumn + " " +
          rightOp.getSymbol() + " " + rightValue + "\n");
    }
    else {
      if (originalLeftValue == null || originalRightValue == null) {
        return doTableScan(count, currOffset, limit, offset, analyze);
      }
      else {
        return doTwoKeyIndexLookup(count, usedIndex, currOffset, countReturned, limit, offset, analyze, evaluateExpression,
            schemaRetryCount, leftColumn, leftOp, rightOp, leftKey, rightKey);
      }
    }
    return null;
  }

  private NextReturn doTwoKeyIndexLookup(int count, AtomicReference<String> usedIndex, AtomicLong currOffset,
                                         AtomicLong countReturned, Limit limit, Offset offset, boolean analyze,
                                         boolean evaluateExpression, int schemaRetryCount, String leftColumn,
                                         Operator leftOp, Operator rightOp, Object[] leftKey, Object[] rightKey) {
    if (analyze) {
      twoKeyLookup = true;
      return null;
    }
    else {
      TableSchema tableSchema = getClient().getCommon().getTables(dbName).get(getTableName());
      IndexSchema indexSchema = tableSchema.getIndices().get(indexName);
      int fieldCount = indexSchema.getFields().length;
      Object[] leftOriginalKey = new Object[fieldCount];
      leftOriginalKey[0] = originalLeftValue;
      Object[] rightOriginalKey = new Object[fieldCount];
      rightOriginalKey[0] = originalRightValue;

      IndexLookup indexLookup = createIndexLookup();
      indexLookup.setCount(count);
      indexLookup.setIndexName(indexName);
      indexLookup.setLeftOp(leftOp);
      indexLookup.setRightOp(rightOp);
      indexLookup.setLeftKey(leftKey);
      indexLookup.setRightKey(rightKey);
      indexLookup.setLeftOriginalKey(leftOriginalKey);
      indexLookup.setRightOriginalKey(rightOriginalKey);
      indexLookup.setColumnName(leftColumn);
      indexLookup.setCurrOffset(currOffset);
      indexLookup.setCountReturned(countReturned);
      indexLookup.setLimit(limit);
      indexLookup.setOffset(offset);
      indexLookup.setSchemaRetryCount(schemaRetryCount);
      indexLookup.setUsedIndex(usedIndex);
      indexLookup.setEvaluateExpression(evaluateExpression);


      SelectContextImpl context = indexLookup.lookup(this, getTopLevelExpression());
      if (context != null) {
        setNextShard(context.getNextShard());
        setNextKey(context.getNextKey());
        if (getNextShard() == -1 || getNextShard() == -2) {
          exhausted = true;
        }
        return new NextReturn(context.getTableNames(), context.getCurrKeys());
      }
    }
    return null;
  }

  protected IndexLookup createIndexLookup() {
    return new IndexLookup();
  }

  private Object[] makeSingleKeyExpression(String indexName, String leftColumn, List<Object> leftValues, Operator leftOp,
                                           String rightColumn, List<Object> rightValues,
                                           Operator rightOp) {
    if (indexName == null) {
      return null;
    }
    String[] indexFields = getClient().getCommon().getTables(dbName).get(getTableName()).getIndices().get(indexName).getFields();
    if (indexFields.length < 2) {
      return null;
    }

    Object[] key = new Object[indexFields.length];
    if (leftOp == EQUAL && rightOp == EQUAL) {
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
    Expression localLeftExpression = ((BinaryExpressionImpl) expression).getLeftExpression();
    Expression localRightExpression = ((BinaryExpressionImpl) expression).getRightExpression();
    return localLeftExpression instanceof ColumnImpl && localRightExpression instanceof ColumnImpl;
  }

  protected NextReturn evaluateOneSidedIndex(
      final String[] tableNames, int count, ExpressionImpl leftExpression, ExpressionImpl rightExpression,
      String leftColumn, Operator leftOp,
      Object leftValue, String rightColumn, Operator rightOp, Object rightValue, SelectStatementImpl.Explain explain,
      AtomicLong currOffset, AtomicLong countReturned, Limit limit, Offset offset, boolean analyze, int schemaRetryCount) {
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

    NextReturn leftIds = null;
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
            ", indexedExpression=[" + leftColumn +  " " + leftOp.getSymbol() + " " + leftValue + "] otherExpression=[" +
            rightColumn + " " + rightOp.getSymbol() + " " + rightValue + "]\n");
      }
      explain.indent();
    }
    if (leftExpression instanceof InExpressionImpl) {
      leftExpression = rightExpression;
      rightExpression = leftExpression;
    }
    leftIds = leftExpression.next(count, explain, currOffset, countReturned, limit, offset, true,
        analyze, schemaRetryCount);
    if (explain != null) {
      explain.appendSpaces();
      explain.getBuilder().append(" AND \n");
    }

    final TableSchema tableSchema = getClient().getCommon().getTables(dbName).get(tableNames[0]);

    if (explain != null) {
      explain.appendSpaces();
      explain.getBuilder().append("Read record evaluation: " + rightExpression.toString() + "\n");
    }
    try {
      return processBatchForOneSidedLookup(tableNames, schemaRetryCount, retIds, leftIds, tableSchema);
    }
    finally {
      if (explain != null) {
        explain.outdent();
      }
    }
  }

  private NextReturn processBatchForOneSidedLookup(String[] tableNames, int schemaRetryCount, List<Object[]> retIds,
                                                   NextReturn leftIds, TableSchema tableSchema) {
    if (leftIds != null && leftIds.getIds() != null) {
      List<Object[][]> batch = new ArrayList<>();
      List<Future> futures = new ArrayList<>();
      for (Object[][] id : leftIds.getKeys()) {
        batch.add(id);
        if (batch.size() >= 250) {
          final List<Object[][]> currBatch = batch;
          batch = new ArrayList<>();

          processBatch(currBatch, tableNames[0], tableSchema, retIds, schemaRetryCount);
        }
      }
      processBatch(batch, tableNames[0], tableSchema, retIds, schemaRetryCount);

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
    return null;
  }


  private void processBatch(List<Object[][]> currBatch, String tableName, TableSchema tableSchema, List<Object[]> retIds,
                            int schemaRetryCount) {
    for (Object[][] id : currBatch) {
      CachedRecord cachedRecord = getRecordCache().get(tableName, id[0]);
      Record record = cachedRecord == null ? null : cachedRecord.getRecord();
      if (record != null) {
        boolean pass = (Boolean) ((ExpressionImpl) getTopLevelExpression()).evaluateSingleRecord(
            new TableSchema[]{tableSchema}, new Record[]{record}, getParms());
        if (pass) {
          synchronized (retIds) {
            retIds.add(id[0]);
          }
        }
      }
      else {
        record = doReadRecord(dbName, getClient(), isForceSelectOnServer(), getRecordCache(), id[0],
            getTableName(), getColumns(), getTopLevelExpression(), getParms(), getViewVersion(), isRestrictToThisServer(),
            getProcedureContext(), schemaRetryCount);
        if (record != null) {
          retIds.add(id[0]);
        }
      }
    }
  }

  private NextReturn evaluateOrExpression(int count, SelectStatementImpl.Explain explain, AtomicLong currOffset,
                                          AtomicLong countReturned,
                                          Limit limit, Offset offset, boolean analyze, int schemaRetryCount) {
    if (!leftExpression.canUseIndex() || !rightExpression.canUseIndex()) {
      return doTableScan(count, currOffset, limit, offset, analyze);
    }
    if (explain != null) {
      explain.indent();
    }
    NextReturn leftIds = leftExpression.next(explain, currOffset, countReturned, limit, offset, schemaRetryCount);
    if (explain != null) {
      explain.outdent();
      explain.getBuilder().append(" OR \n");
      explain.indent();
    }
    NextReturn rightIds = rightExpression.next(explain, currOffset, countReturned, limit, offset, schemaRetryCount);
    if (explain != null) {
      explain.outdent();
    }
    if (leftIds == null) {
      return rightIds;
    }
    if (rightIds == null) {
      return leftIds;
    }
    Object[][][] ids = aggregateResults(leftIds.getKeys(), rightIds.getKeys());
    return new NextReturn(new String[]{getTableName()}, ids);
  }


  @Override
  public boolean canUseIndex() {
    boolean leftCanUse = false;
    boolean rightCanUse = false;
    if (leftExpression instanceof FunctionImpl) {
      return false;
    }
    if (rightExpression instanceof FunctionImpl) {
      return false;
    }
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
      if (operator == NOT_EQUAL) {
        return false;
      }
      String localColumnName = ((ColumnImpl) leftExpression).getColumnName();

      for (Map.Entry<String, IndexSchema> entry :
          getClient().getCommon().getTables(dbName).get(getTableName()).getIndices().entrySet()) {
        if (entry.getValue().getFields()[0].equals(localColumnName)) {
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

  @Override
  public void getColumns(Set<ColumnImpl> columns) {
    leftExpression.getColumns(columns);
    rightExpression.getColumns(columns);
  }

  @Override
  public void setColumns(List<ColumnImpl> columns) {
    super.setColumns(columns);
    leftExpression.setColumns(columns);
    rightExpression.setColumns(columns);
  }

  @Override
  public void setProbe(boolean probe) {
    super.setProbe(probe);
    leftExpression.setProbe(probe);
    rightExpression.setProbe(probe);
  }


  @Override
  public void setTopLevelExpression(Expression topLevelExpression) {
    super.setTopLevelExpression(topLevelExpression);
    leftExpression.setTopLevelExpression(topLevelExpression);
    rightExpression.setTopLevelExpression(topLevelExpression);
  }

  @Override
  public void setOrderByExpressions(List<OrderByExpressionImpl> orderByExpressions) {
    super.setOrderByExpressions(orderByExpressions);
    leftExpression.setOrderByExpressions(orderByExpressions);
    rightExpression.setOrderByExpressions(orderByExpressions);
  }

  @Override
  public void setTableName(String tableName) {
    super.setTableName(tableName);
    leftExpression.setTableName(tableName);
    rightExpression.setTableName(tableName);
  }

  @Override
  public void setClient(DatabaseClient client) {
    super.setClient(client);
    leftExpression.setClient(client);
    rightExpression.setClient(client);
  }

  @Override
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

  @Override
  public void deserialize(short serializationVersion, DataInputStream in) {
    try {
      super.deserialize(serializationVersion, in);
      int id = in.readInt();
      operator = Operator.getOperator(id);
      ExpressionImpl expression = deserializeExpression(in);
      setLeftExpression(expression);
      expression = deserializeExpression(in);
      setRightExpression(expression);
      isNot = in.readBoolean();
      exhausted = in.readBoolean();
      rewroteQuery = in.readBoolean();
      if (serializationVersion >= DatabaseClient.SERIALIZATION_VERSION_24) {
        oneKeyLookup = in.readBoolean();
        twoKeyLookup = in.readBoolean();
        isTableScan = in.readBoolean();
      }
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public void serialize(short serializationVersion, DataOutputStream out) {
    try {
      super.serialize(serializationVersion, out);
      out.writeInt(operator.getId());
      ExpressionImpl value = getLeftExpression();
      serializeExpression(value, out);
      value = getRightExpression();
      serializeExpression(value, out);
      out.writeBoolean(isNot);
      out.writeBoolean(exhausted);
      out.writeBoolean(rewroteQuery);
      if (serializationVersion >= DatabaseClient.SERIALIZATION_VERSION_24) {
        out.writeBoolean(oneKeyLookup);
        out.writeBoolean(twoKeyLookup);
        out.writeBoolean(isTableScan);
      }
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public boolean like(final String str, String expr) {
    final String[] parts = expr.split("%");
    final boolean traillingOp = expr.endsWith("%");
    StringBuilder localExpr = new StringBuilder();
    for (int i = 0, l = parts.length; i < l; ++i) {
      final String[] p = parts[i].split("\\\\\\?");
      if (p.length > 1) {
        for (int y = 0, l2 = p.length; y < l2; ++y) {
          localExpr.append(p[y]);
          if (i + 1 < l2) {
            localExpr.append(".");
          }
        }
      }
      else {
        localExpr.append(parts[i]);
      }
      if (i + 1 < l) {
        localExpr.append("%");
      }
    }
    if (traillingOp) {
      localExpr.append("%");
    }
    String exprStr = localExpr.toString();
    exprStr = exprStr.replace("?", ".");
    exprStr = exprStr.replace("%", ".*");
    return str.matches(exprStr);
  }

  private interface SimpleComparator {
    boolean compare(int value);
  }

  static {
    simpleComparators.put(EQUAL, k -> k == 0);
    simpleComparators.put(NOT_EQUAL, k -> k != 0);
    simpleComparators.put(LESS, k -> k < 0);
    simpleComparators.put(LESS_EQUAL, k -> k <= 0);
    simpleComparators.put(GREATER, k -> k > 0);
    simpleComparators.put(GREATER_EQUAL, k -> k >= 0);
  }

  @Override
  public Object evaluateSingleRecord(
      TableSchema[] tableSchemas, Record[] records,
      ParameterHandler parms) {
    try {
      Operator localOperator = getOperator();
      Object lhsValue = leftExpression.evaluateSingleRecord(tableSchemas, records, parms);
      Object rhsValue = rightExpression.evaluateSingleRecord(tableSchemas, records, parms);
      Comparator comparator = DataType.Type.getComparatorForValue(lhsValue);
      if (lhsValue instanceof BigDecimal || rhsValue instanceof BigDecimal) {
        comparator = DataType.getBigDecimalComparator();
      }
      else if (lhsValue instanceof Double || rhsValue instanceof Double ||
          lhsValue instanceof Float || rhsValue instanceof Float) {
        comparator = DataType.getDoubleComparator();
      }
      switch (localOperator) {
        case LIKE:
          return evaluateLikeOperator(lhsValue, rhsValue);
        case AND:
          return evaluateAndOperator(lhsValue, rhsValue);
        case OR:
          return evaluateOrOperator(lhsValue, rhsValue);
        case PLUS:
        case MINUS:
        case TIMES:
        case DIVIDE:
        case BITWISE_AND:
        case BITWISE_OR:
        case BITWISE_X_OR:
        case MODULO:
          return evaluateMathOperator(localOperator, lhsValue, rhsValue);
        default:
          Boolean ret = evaluateSimpleOperator(localOperator, lhsValue, rhsValue, comparator);
          if (ret != null) {
            return ret;
          }
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

  private Object evaluateAndOperator(Object lhsValue, Object rhsValue) {
    if (lhsValue == null || rhsValue == null) {
      return false;
    }
    if (isNot) {
      return !((Boolean) lhsValue && (Boolean) rhsValue);
    }
    return (Boolean) lhsValue && (Boolean) rhsValue;
  }

  private Object evaluateOrOperator(Object lhsValue, Object rhsValue) {
    if (lhsValue == null || rhsValue == null) {
      return false;
    }
    if (isNot) {
      return !((Boolean) lhsValue || (Boolean) rhsValue);
    }
    return (Boolean) lhsValue || (Boolean) rhsValue;
  }

  private Boolean evaluateSimpleOperator(Operator localOperator, Object lhsValue, Object rhsValue, Comparator comparator) {
    if (simpleComparators.containsKey(localOperator)) {
      if (lhsValue == null && rhsValue == null) {
        return true;
      }
      if (lhsValue == null || rhsValue == null) {
        return false;
      }
      if (simpleComparators.get(localOperator).compare(comparator.compare(lhsValue, rhsValue))) {
        return !isNot;
      }
      return isNot;
    }
    return null;
  }

  private Object evaluateMathOperator(Operator localOperator, Object lhsValue, Object rhsValue) {
    if (lhsValue == null || rhsValue == null) {
      return null;
    }
    if (lhsValue instanceof BigDecimal || rhsValue instanceof BigDecimal) {
      return evaluateBigDecimalMath(localOperator, lhsValue, rhsValue);
    }
    else if (lhsValue instanceof Double || rhsValue instanceof Double ||
        lhsValue instanceof Float || rhsValue instanceof Float) {
      return evaluateDoubleMath(localOperator, lhsValue, rhsValue);
    }
    else if (lhsValue instanceof Long || rhsValue instanceof Long ||
        lhsValue instanceof Integer || rhsValue instanceof Integer ||
        lhsValue instanceof Short || rhsValue instanceof Short ||
        lhsValue instanceof Byte || rhsValue instanceof Byte) {
      return evaluateLongMath(localOperator, lhsValue, rhsValue);
    }
    else {
      throw new DatabaseException("Operator not supported for this datatype");
    }
  }

  private Object evaluateLikeOperator(Object lhsValue, Object rhsValue) throws UnsupportedEncodingException {
    if (lhsValue == null && rhsValue == null) {
      return true;
    }
    if (lhsValue == null || rhsValue == null) {
      return false;
    }
    String lhsStr = new String((byte[]) lhsValue, "utf-8");
    String rhsStr = new String((byte[]) rhsValue, "utf-8");
    if (like(lhsStr, rhsStr)) {
      return !isNot;
    }
    return isNot;
  }

  private Object evaluateLongMath(Operator localOperator, Object lhsValue, Object rhsValue) {
    Long lhs = (Long) DataType.getLongConverter().convert(lhsValue);
    Long rhs = (Long) DataType.getLongConverter().convert(rhsValue);
    if (localOperator == PLUS) {
      return lhs + rhs;
    }
    else if (localOperator == MINUS) {
      return lhs - rhs;
    }
    else if (localOperator == TIMES) {
      return lhs * rhs;
    }
    else if (localOperator == DIVIDE) {
      return lhs / rhs;
    }
    else if (localOperator == BITWISE_AND) {
      return lhs & rhs;
    }
    else if (localOperator == BITWISE_OR) {
      return lhs | rhs;
    }
    else if (localOperator == BITWISE_X_OR) {
      return lhs ^ rhs;
    }
    else if (localOperator == MODULO) {
      return lhs % rhs;
    }
    else {
      throw new DatabaseException(INVALID_OPERATOR_STR);
    }
  }

  private Object evaluateDoubleMath(Operator localOperator, Object lhsValue, Object rhsValue) {
    Double lhs = (Double) DataType.getDoubleConverter().convert(lhsValue);
    Double rhs = (Double) DataType.getDoubleConverter().convert(rhsValue);
    if (localOperator == PLUS) {
      return lhs + rhs;
    }
    else if (localOperator == MINUS) {
      return lhs - rhs;
    }
    else if (localOperator == TIMES) {
      return lhs * rhs;
    }
    else if (localOperator == DIVIDE) {
      return lhs / rhs;
    }
    else if (localOperator == BITWISE_AND ||
        localOperator == BITWISE_OR ||
        localOperator == BITWISE_X_OR ||
        localOperator == MODULO) {
      throw new DatabaseException(INVALID_OPERATOR_STR);
    }
    else {
      throw new DatabaseException(INVALID_OPERATOR_STR);
    }
  }

  private Object evaluateBigDecimalMath(Operator localOperator, Object lhsValue, Object rhsValue) {
    BigDecimal lhs = (BigDecimal) DataType.getBigDecimalConverter().convert(lhsValue);
    BigDecimal rhs = (BigDecimal) DataType.getBigDecimalConverter().convert(rhsValue);
    if (localOperator == PLUS) {
      return lhs.add(rhs);
    }
    else if (localOperator == MINUS) {
      return lhs.subtract(rhs);
    }
    else if (localOperator == TIMES) {
      return lhs.multiply(rhs);
    }
    else if (localOperator == DIVIDE) {
      return lhs.divide(rhs);
    }
    else if (localOperator == BITWISE_AND ||
        localOperator == BITWISE_OR ||
        localOperator == BITWISE_X_OR ||
        localOperator == MODULO) {
      throw new DatabaseException(INVALID_OPERATOR_STR);
    }
    else {
      throw new DatabaseException(INVALID_OPERATOR_STR);
    }
  }


  @Override
  public ExpressionImpl.Type getType() {
    return ExpressionImpl.Type.BINARY_OP;
  }

  public void setOperator(Operator operator) {
    this.operator = operator;
  }

  public Operator getOperator() {
    return operator;
  }

  @Override
  public void setLastShard(int lastShard) {
    super.setLastShard(lastShard);
    leftExpression.setLastShard(lastShard);
    rightExpression.setLastShard(lastShard);
  }

  @Override
  public void setIsCurrPartitions(boolean isCurrPartitions) {
    super.setIsCurrPartitions(isCurrPartitions);
    leftExpression.setIsCurrPartitions(isCurrPartitions);
    rightExpression.setIsCurrPartitions(isCurrPartitions);
  }

  public boolean isTableScan() {
    return isTableScan;
  }

  public void setIsRightKey(boolean isRightKey) {
    this.isRightKey = isRightKey;
  }


  public boolean isRighKey() {
    return isRightKey;
  }

  private class GetEffectiveOps {
    private Operator leftOp;
    private Operator rightOp;
    private Operator leftEffectiveOp;
    private Operator rightEffectiveOp;

    public GetEffectiveOps(Operator leftOp, Operator rightOp) {
      this.leftOp = leftOp;
      this.rightOp = rightOp;
    }

    public Operator getLeftEffectiveOp() {
      return leftEffectiveOp;
    }

    public Operator getRightEffectiveOp() {
      return rightEffectiveOp;
    }

    public GetEffectiveOps invoke() {
      leftEffectiveOp = leftOp;
      if (leftOp == LESS_EQUAL) {
        leftEffectiveOp = LESS;
      }
      else if (leftOp == GREATER_EQUAL) {
        leftEffectiveOp = GREATER;
      }
      rightEffectiveOp = rightOp;
      if (rightOp == LESS_EQUAL) {
        rightEffectiveOp = LESS;
      }
      else if (rightOp == GREATER_EQUAL) {
        rightEffectiveOp = GREATER;
      }
      if (leftOp == EQUAL) {
        leftEffectiveOp = rightEffectiveOp;
      }
      if (rightOp == EQUAL) {
        rightEffectiveOp = leftEffectiveOp;
      }
      return this;
    }
  }

  private class GetLeftAndRightValues {
    private String rightColumn;
    private String leftColumn;
    private Operator leftOp;
    private Operator rightOp;
    private Object leftValue;
    private Object rightValue;
    private ExpressionImpl localLeftExpression;
    private ExpressionImpl localRightExpression;
    private List<Object> leftValues;
    private List<Object> rightValues;
    private List<Object> originalLeftValues;
    private List<Object> originalRightValues;

    public GetLeftAndRightValues(ExpressionImpl localLeftExpression, ExpressionImpl localRightExpression) {
      this.localLeftExpression = localLeftExpression;
      this.localRightExpression = localRightExpression;
    }

    public String getRightColumn() {
      return rightColumn;
    }

    public String getLeftColumn() {
      return leftColumn;
    }

    public Operator getLeftOp() {
      return leftOp;
    }

    public Operator getRightOp() {
      return rightOp;
    }

    public Object getLeftValue() {
      return leftValue;
    }

    public List<Object> getLeftValues() {
      return leftValues;
    }

    public List<Object> getRightValues() {
      return rightValues;
    }

    public List<Object> getOriginalLeftValues() {
      return originalLeftValues;
    }

    public List<Object> getOriginalRightValues() {
      return originalRightValues;
    }

    public GetLeftAndRightValues invoke() {
      processLeftExpression();
      processRightExpression();

      if (getNextKey() != null) {
        leftValue = getNextKey()[0];
      }

      leftValues = new ArrayList<>();
      if (leftValue != null) {
        leftValues.add(leftValue);
      }

      rightValues = new ArrayList<>();
      if (rightValue != null) {
        rightValues.add(rightValue);
      }

      originalLeftValues = new ArrayList<>();
      if (originalLeftValue != null) {
        originalLeftValues.add(originalLeftValue);
      }

      originalRightValues = new ArrayList<>();
      if (originalRightValue != null) {
        originalRightValues.add(originalRightValue);
      }
      return this;
    }

    private void processRightExpression() {
      if (localRightExpression instanceof BinaryExpressionImpl) {
        BinaryExpressionImpl rightOpExpr = (BinaryExpressionImpl) localRightExpression;
        rightOp = rightOpExpr.getOperator();
        if (rightOp.isRelationalOp()) {
          if (rightOpExpr.getLeftExpression() instanceof ColumnImpl) {
            rightColumn = ((ColumnImpl) rightOpExpr.getLeftExpression()).getColumnName();
          }
          originalRightValue = getValueFromExpression(getParms(), rightOpExpr.getRightExpression());
        }
      }
    }

    private void processLeftExpression() {
      if (localLeftExpression instanceof BinaryExpressionImpl) {
        BinaryExpressionImpl leftOpExpr = (BinaryExpressionImpl) localLeftExpression;
        leftOp = leftOpExpr.getOperator();
        if (leftOp.isRelationalOp()) {
          if (leftOpExpr.getLeftExpression() instanceof ColumnImpl) {
            leftColumn = ((ColumnImpl) leftOpExpr.getLeftExpression()).getColumnName();
          }
          originalLeftValue = getValueFromExpression(getParms(), leftOpExpr.getRightExpression());
        }
      }
    }
  }

  private class GetLeftAndRightKey {
    private List<Object> leftValues;
    private List<Object> rightValues;
    private String[] indexFields;
    private Object[] leftKey;
    private Object[] rightKey;

    public GetLeftAndRightKey(List<Object> leftValues, List<Object> rightValues, String[] indexFields, Object... leftKey) {
      this.leftValues = leftValues;
      this.rightValues = rightValues;
      this.indexFields = indexFields;
      this.leftKey = leftKey;
    }

    public Object[] getLeftKey() {
      return leftKey;
    }

    public Object[] getRightKey() {
      return rightKey;
    }

    public GetLeftAndRightKey invoke() {
      if (!leftValues.isEmpty()) {
        leftKey = buildKey(leftValues, indexFields);
      }
      rightKey = null;
      if (!rightValues.isEmpty()) {
        rightKey = buildKey(rightValues, indexFields);
      }
      return this;
    }
  }

  private class GetLeftValue {
    private ExpressionImpl localRightExpression;
    private ExpressionImpl localLeftExpression;
    private String columnName;
    private Object[] leftValue;

    public GetLeftValue(ExpressionImpl localRightExpression, ExpressionImpl localLeftExpression, String columnName) {
      this.localRightExpression = localRightExpression;
      this.localLeftExpression = localLeftExpression;
      this.columnName = columnName;
    }

    public String getColumnName() {
      return columnName;
    }

    public Object[] getLeftValue() {
      return leftValue;
    }

    public GetLeftValue invoke() {
      leftValue = null;
      if (getNextKey() != null) {
        leftValue = getNextKey();
      }
      else {
        Object currLeftValue = getValueFromExpression(getParms(), localRightExpression);
        if (currLeftValue == null) {
          currLeftValue = getValueFromExpression(getParms(), localLeftExpression);
          columnName = ((ColumnImpl) localLeftExpression).getColumnName();
        }
        if (currLeftValue != null) {
          originalLeftValue = currLeftValue;
        }
      }
      return this;
    }
  }
}
