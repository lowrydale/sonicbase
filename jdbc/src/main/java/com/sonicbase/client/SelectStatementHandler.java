package com.sonicbase.client;

import com.sonicbase.common.*;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.procedure.StoredProcedureContextImpl;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.query.ResultSet;
import com.sonicbase.query.SelectStatement;
import com.sonicbase.query.impl.*;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import org.apache.commons.lang.exception.ExceptionUtils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static com.sonicbase.client.DatabaseClient.toLower;

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class SelectStatementHandler implements StatementHandler {

  private final DatabaseClient client;

  public SelectStatementHandler(DatabaseClient client) {
    this.client = client;
  }

  public Object execute(String dbName, ParameterHandler parms, String sqlToUse, Statement statement,
                        SelectStatementImpl.Explain explain, Long sequence0, Long sequence1, Short sequence2,
                        boolean restrictToThisServer, StoredProcedureContextImpl procedureContext, int schemaRetryCount) {
    Select selectNode = (Select) statement;
    SelectBody selectBody = selectNode.getSelectBody();
    AtomicInteger currParmNum = new AtomicInteger();
    if (selectBody instanceof PlainSelect) {
      SelectStatementImpl selectStatement = parseSelectStatement(client, parms, (PlainSelect) selectBody, currParmNum);
      return selectStatement.execute(dbName, sqlToUse, explain, null, null, null,
          restrictToThisServer, procedureContext, schemaRetryCount);
    }
    else if (selectBody instanceof SetOperationList) {
      return executeSetOperation(dbName, parms, restrictToThisServer, procedureContext,
          (SetOperationList) selectBody, currParmNum);
    }
    return null;
  }

  private Object executeSetOperation(String dbName, ParameterHandler parms, boolean restrictToThisServer,
                                     StoredProcedureContextImpl procedureContext, SetOperationList selectBody,
                                     AtomicInteger currParmNum) {
    String[] tableNames = new String[selectBody.getSelects().size()];
    SelectStatementImpl[] statements = new SelectStatementImpl[selectBody.getSelects().size()];
    for (int i = 0; i < selectBody.getSelects().size(); i++) {
      SelectBody innerBody = selectBody.getSelects().get(i);
      SelectStatementImpl selectStatement = parseSelectStatement(client, parms, (PlainSelect) innerBody, currParmNum);
      tableNames[i] = selectStatement.getFromTable();
      statements[i] = selectStatement;
    }
    String[] operations = new String[selectBody.getOperations().size()];
    for (int i = 0; i < operations.length; i++) {
      operations[i] = selectBody.getOperations().get(i).toString();
    }
    List<OrderByElement> orderByElements = selectBody.getOrderByElements();
    OrderByExpressionImpl[] orderBy = null;
    if (orderByElements != null) {
      orderBy = new OrderByExpressionImpl[orderByElements.size()];
      for (int i = 0; i < orderBy.length; i++) {
        OrderByElement element = orderByElements.get(i);
        String tableName = ((Column) element.getExpression()).getTable().getName();
        String columnName = ((Column) element.getExpression()).getColumnName();
        orderBy[i] = new OrderByExpressionImpl(tableName == null ? null : tableName.toLowerCase(),
            columnName.toLowerCase(), element.isAsc());
      }
    }
    SetOperation setOperation = new SetOperation();
    setOperation.setSelectStatements(statements);
    setOperation.setOperations(operations);
    setOperation.setOrderBy(orderBy);
    try {
      return serverSetSelect(dbName, tableNames, setOperation, restrictToThisServer, procedureContext);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public static class SetOperation {
    private SelectStatementImpl[] selectStatements;
    private String[] operations;
    private OrderByExpressionImpl[] orderBy;
    private long serverSelectPageNumber;
    private long resultSetId;
    private Integer shard;
    private Integer replica;


    public SelectStatementImpl[] getSelectStatements() {
      return selectStatements;
    }

    public void setSelectStatements(SelectStatementImpl[] selectStatements) {
      this.selectStatements = selectStatements;
    }

    public String[] getOperations() {
      return operations;
    }

    public void setOperations(String[] operations) {
      this.operations = operations;
    }

    public OrderByExpressionImpl[] getOrderBy() {
      return orderBy;
    }

    public void setOrderBy(OrderByExpressionImpl[] orderBy) {
      this.orderBy = orderBy;
    }

    public long getServerSelectPageNumber() {
      return serverSelectPageNumber;
    }

    void setServerSelectPageNumber(long serverSelectPageNumber) {
      this.serverSelectPageNumber = serverSelectPageNumber;
    }

    public long getResultSetId() {
      return resultSetId;
    }

    void setResultSetId(long resultSetId) {
      this.resultSetId = resultSetId;
    }

    public Integer getShard() {
      return shard;
    }

    public void setShard(Integer shard) {
      this.shard = shard;
    }

    public Integer getReplica() {
      return replica;
    }

    public void setReplica(Integer replica) {
      this.replica = replica;
    }
  }

  private ResultSet serverSetSelect(String dbName, String[] tableNames, SetOperation setOperation,
                                    boolean restrictToThisServer, StoredProcedureContextImpl procedureContext) {
    while (true) {
      if (client.getShutdown()) {
        throw new DatabaseException("Shutting down");
      }

      try {
        Map<String, SelectFunctionImpl> functionAliases = new HashMap<>();
        Map<String, ColumnImpl> aliases = new HashMap<>();
        for (SelectStatementImpl select : setOperation.getSelectStatements()) {
          aliases.putAll(select.getAliases());
          functionAliases.putAll(select.getFunctionAliases());
        }

        ResultSetImpl ret = new ResultSetImpl(dbName, client, tableNames, setOperation, aliases, functionAliases,
            restrictToThisServer, procedureContext);
        doServerSetSelect(dbName, tableNames, setOperation, ret, restrictToThisServer, procedureContext);
        return ret;
      }
      catch (Exception e) {
        if (e.getMessage() != null && e.getMessage().contains("SchemaOutOfSyncException") ||
            -1 != ExceptionUtils.indexOfThrowable(e, SchemaOutOfSyncException.class)) {
          continue;
        }
        throw new DatabaseException(e);
      }
    }

  }

  public void doServerSetSelect(String dbName, String[] tableNames, SetOperation setOperation, ResultSetImpl ret,
                                boolean restrictToThisServer, StoredProcedureContextImpl procedureContext) throws IOException {
    ComObject cobj = new ComObject(9);
    ComArray array = cobj.putArray(ComObject.Tag.SELECT_STATEMENTS, ComObject.Type.BYTE_ARRAY_TYPE, setOperation.getSelectStatements().length);
    for (int i = 0; i < setOperation.getSelectStatements().length; i++) {
      setOperation.getSelectStatements()[i].setTableNames(new String[]{setOperation.getSelectStatements()[i].getFromTable()});
      array.add(setOperation.getSelectStatements()[i].serialize());
    }
    if (setOperation.getOrderBy() != null) {
      ComArray orderByArray = cobj.putArray(ComObject.Tag.ORDER_BY_EXPRESSIONS, ComObject.Type.BYTE_ARRAY_TYPE, setOperation.getOrderBy().length);
      for (int i = 0; i < setOperation.getOrderBy().length; i++) {
        orderByArray.add(setOperation.getOrderBy()[i].serialize());
      }
    }
    ComArray tablesArray = cobj.putArray(ComObject.Tag.TABLES, ComObject.Type.STRING_TYPE, tableNames.length);
    for (int i = 0; i < tableNames.length; i++) {
      tablesArray.add(tableNames[i]);
    }
    ComArray strArray = cobj.putArray(ComObject.Tag.OPERATIONS, ComObject.Type.STRING_TYPE, setOperation.getOperations().length);
    for (int i = 0; i < setOperation.getOperations().length; i++) {
      strArray.add(setOperation.getOperations()[i]);
    }
    cobj.put(ComObject.Tag.SCHEMA_VERSION, client.getCommon().getSchemaVersion());
    cobj.put(ComObject.Tag.COUNT, DatabaseClient.SELECT_PAGE_SIZE);
    cobj.put(ComObject.Tag.DB_NAME, dbName);
    cobj.put(ComObject.Tag.SERVER_SELECT_PAGE_NUMBER, setOperation.getServerSelectPageNumber());
    cobj.put(ComObject.Tag.RESULT_SET_ID, setOperation.getResultSetId());

    ComObject retObj = null;
    if (restrictToThisServer) {
      retObj = DatabaseServerProxy.serverSetSelect(client.getDatabaseServer(), cobj, restrictToThisServer, procedureContext);
    }
    else {
      byte[] recordRet = null;
      if (setOperation.getShard() == null) {
        recordRet = client.send("ReadManager:serverSetSelect", Math.abs(ThreadLocalRandom.current().nextInt() % client.getShardCount()),
            Math.abs(ThreadLocalRandom.current().nextLong()), cobj, DatabaseClient.Replica.DEF);
      }
      else {
        recordRet = client.send("ReadManager:serverSetSelect", setOperation.getShard(), setOperation.getReplica(), cobj,
            DatabaseClient.Replica.SPECIFIED);
      }
      retObj = new ComObject(recordRet);
    }

    handleResponseForServerSetSelect(dbName, tableNames, setOperation, ret, retObj);
  }

  private void handleResponseForServerSetSelect(String dbName, String[] tableNames, SetOperation setOperation,
                                                ResultSetImpl ret, ComObject retObj) {
    TableSchema[] tableSchemas = new TableSchema[tableNames.length];
    for (int i = 0; i < tableNames.length; i++) {
      tableSchemas[i] = client.getCommon().getTables(dbName).get(tableNames[i]);
    }

    String[][] primaryKeyFields = new String[tableNames.length][];
    for (int i = 0; i < tableNames.length; i++) {
      for (Map.Entry<String, IndexSchema> entry : tableSchemas[i].getIndices().entrySet()) {
        if (entry.getValue().isPrimaryKey()) {
          primaryKeyFields[i] = entry.getValue().getFields();
          break;
        }
      }
    }
    setOperation.setServerSelectPageNumber(retObj.getLong(ComObject.Tag.SERVER_SELECT_PAGE_NUMBER));
    setOperation.setResultSetId(retObj.getLong(ComObject.Tag.RESULT_SET_ID));
    setOperation.setShard(retObj.getInt(ComObject.Tag.SHARD));
    setOperation.setReplica(retObj.getInt(ComObject.Tag.REPLICA));

    ret.getRecordCache().getRecordsForTable().clear();

    ComArray tableRecords = retObj.getArray(ComObject.Tag.TABLE_RECORDS);
    Object[][][] retKeys = new Object[tableRecords == null ? 0 : tableRecords.getArray().size()][][];
    Record[][] currRetRecords = new Record[tableRecords == null ? 0 : tableRecords.getArray().size()][];
    ExpressionImpl.CachedRecord[][] retRecords = new ExpressionImpl.CachedRecord[tableRecords == null ? 0 :
        tableRecords.getArray().size()][];

    processResponseRecordsForServerSetSelect(dbName, tableNames, ret, tableSchemas, primaryKeyFields, tableRecords,
        retKeys, currRetRecords, retRecords);

    ret.setRetKeys(retKeys);
    ret.setRecords(retRecords);
  }

  private void processResponseRecordsForServerSetSelect(String dbName, String[] tableNames, ResultSetImpl ret,
                                                        TableSchema[] tableSchemas, String[][] primaryKeyFields,
                                                        ComArray tableRecords, Object[][][] retKeys, Record[][] currRetRecords,
                                                        ExpressionImpl.CachedRecord[][] retRecords) {
    for (int k = 0; k < currRetRecords.length; k++) {
      currRetRecords[k] = new Record[tableNames.length];
      retRecords[k] = new ExpressionImpl.CachedRecord[tableNames.length];
      retKeys[k] = new Object[tableNames.length][];
      ComArray records = (ComArray) tableRecords.getArray().get(k);
      for (int j = 0; j < tableNames.length; j++) {
        byte[] recordBytes = (byte[]) records.getArray().get(j);
        if (recordBytes != null && recordBytes.length > 0) {
          Record record = new Record(tableSchemas[j]);
          record.deserialize(dbName, client.getCommon(), recordBytes, null, true);
          currRetRecords[k][j] = record;

          Object[] key = new Object[primaryKeyFields[j].length];
          for (int i = 0; i < primaryKeyFields[j].length; i++) {
            key[i] = record.getFields()[tableSchemas[j].getFieldOffset(primaryKeyFields[j][i])];
          }

          if (retKeys[k][j] == null) {
            retKeys[k][j] = key;
          }

          retRecords[k][j] = new ExpressionImpl.CachedRecord(record, recordBytes);
          ret.getRecordCache().put(tableNames[j], key, retRecords[k][j]);
        }
      }
    }
  }


  public static ExpressionImpl getExpression(
      DatabaseClient client, AtomicInteger currParmNum, Expression whereExpression, String tableName, ParameterHandler parms) {

    ExpressionImpl retExpression = null;
    if (whereExpression instanceof Between) {
      retExpression = getBetweenExpression((Between) whereExpression);
    }
    else if (whereExpression instanceof AndExpression) {
      retExpression = getAndExpression(client, currParmNum, (AndExpression) whereExpression, tableName, parms);
    }
    else if (whereExpression instanceof OrExpression) {
      retExpression = getOrExpression(client, currParmNum, (OrExpression) whereExpression, tableName, parms);
    }
    else if (whereExpression instanceof Parenthesis) {
      retExpression = getParenthesisExpression(client, currParmNum, (Parenthesis) whereExpression, tableName, parms);
    }
    else if (whereExpression instanceof net.sf.jsqlparser.expression.BinaryExpression) {
      retExpression = getBinaryExpression(client, currParmNum, whereExpression, tableName, parms);
    }
    else if (whereExpression instanceof net.sf.jsqlparser.expression.operators.relational.InExpression) {
      retExpression = getInExpression(client, currParmNum, (InExpression) whereExpression, tableName, parms);
    }
    else if (whereExpression instanceof Column) {
      retExpression = getColumnExpression((Column) whereExpression, tableName);
    }
    else if (whereExpression instanceof StringValue) {
      retExpression = getStringValueExpression((StringValue) whereExpression);
    }
    else if (whereExpression instanceof DoubleValue) {
      retExpression = getDoubleValueExpression((DoubleValue) whereExpression);
    }
    else if (whereExpression instanceof LongValue) {
      retExpression = getLongValueExpression((LongValue) whereExpression);
    }
    else if (whereExpression instanceof NullValue) {
      retExpression = getNullValueExpression((NullValue) whereExpression);
    }
    else if (whereExpression instanceof JdbcNamedParameter) {
      ParameterImpl parameter = new ParameterImpl();
      parameter.setParmName(((JdbcNamedParameter) whereExpression).getName());
      retExpression = parameter;
    }
    else if (whereExpression instanceof JdbcParameter) {
      ParameterImpl parameter = new ParameterImpl();
      parameter.setParmOffset(currParmNum.getAndIncrement());
      retExpression = parameter;
    }
    else if (whereExpression instanceof Function) {
      retExpression = getFunctionExpression(client, currParmNum, (Function) whereExpression, tableName, parms);
    }
    else if (whereExpression instanceof SignedExpression) {
      SignedExpression expression = (SignedExpression) whereExpression;
      Expression innerExpression = expression.getExpression();
      ExpressionImpl inner = getExpression(client, currParmNum, innerExpression, tableName, parms);
      if (inner instanceof ConstantImpl) {
        ConstantImpl constant = (ConstantImpl) inner;
        if ('-' == expression.getSign()) {
          constant.negate();
        }
        return constant;
      }
      SignedExpressionImpl ret = new SignedExpressionImpl();
      ret.setExpression(inner);
      if ('-' == expression.getSign()) {
        ret.setNegative(true);
      }
      retExpression = ret;
    }

    return retExpression;
  }

  private static ExpressionImpl getFunctionExpression(DatabaseClient client, AtomicInteger currParmNum,
                                                      Function whereExpression, String tableName, ParameterHandler parms) {
    ExpressionList sourceParms = whereExpression.getParameters();
    List<ExpressionImpl> expressions = new ArrayList<>();
    if (sourceParms != null) {
      for (Expression expression : sourceParms.getExpressions()) {
        ExpressionImpl expressionImpl = getExpression(client, currParmNum, expression, tableName, parms);
        expressions.add(expressionImpl);
      }
    }
    return new FunctionImpl(whereExpression.getName(), expressions);
  }

  private static ExpressionImpl getLongValueExpression(LongValue whereExpression) {
    ExpressionImpl retExpression;
    ConstantImpl constant = new ConstantImpl();
    constant.setSqlType(Types.BIGINT);
    constant.setValue(whereExpression.getValue());
    retExpression = constant;
    return retExpression;
  }

  private static ExpressionImpl getNullValueExpression(NullValue whereExpression) {
    ExpressionImpl retExpression;
    ConstantImpl constant = new ConstantImpl();
    constant.setSqlType(Types.NULL);
    constant.setValue(null);
    retExpression = constant;
    return retExpression;
  }

  private static ExpressionImpl getDoubleValueExpression(DoubleValue whereExpression) {
    ExpressionImpl retExpression;
    ConstantImpl constant = new ConstantImpl();
    constant.setSqlType(Types.DOUBLE);
    constant.setValue(whereExpression.getValue());
    retExpression = constant;
    return retExpression;
  }

  private static ExpressionImpl getStringValueExpression(StringValue whereExpression) {
    ExpressionImpl retExpression;
    ConstantImpl constant = new ConstantImpl();
    constant.setSqlType(Types.VARCHAR);
    try {
      constant.setValue(whereExpression.getValue().getBytes("utf-8"));
    }
    catch (UnsupportedEncodingException e) {
      throw new DatabaseException(e);
    }
    retExpression = constant;
    return retExpression;
  }

  private static ExpressionImpl getColumnExpression(Column whereExpression, String tableName) {
    ExpressionImpl retExpression;
    ColumnImpl columnNode = new ColumnImpl();
    String colTableName = whereExpression.getTable().getName();
    if (colTableName != null) {
      columnNode.setTableName(toLower(colTableName));
    }
    else {
      columnNode.setTableName(tableName);
    }
    columnNode.setColumnName(toLower(whereExpression.getColumnName()));
    retExpression = columnNode;
    return retExpression;
  }

  private static ExpressionImpl getInExpression(DatabaseClient client, AtomicInteger currParmNum,
                                                InExpression whereExpression, String tableName, ParameterHandler parms) {
    ExpressionImpl retExpression;
    InExpressionImpl retInExpression = new InExpressionImpl(client, parms, tableName);
    retInExpression.setNot(whereExpression.isNot());
    retInExpression.setLeftExpression(getExpression(client, currParmNum, whereExpression.getLeftExpression(), tableName, parms));
    ItemsList items = whereExpression.getRightItemsList();
    if (items instanceof ExpressionList) {
      ExpressionList expressionList = (ExpressionList) items;
      List expressions = expressionList.getExpressions();
      for (Object obj : expressions) {
        retInExpression.addExpression(getExpression(client, currParmNum, (Expression) obj, tableName, parms));
      }
    }

    retExpression = retInExpression;
    return retExpression;
  }

  private static ExpressionImpl getBinaryExpression(DatabaseClient client, AtomicInteger currParmNum,
                                                    Expression whereExpression, String tableName, ParameterHandler parms) {
    ExpressionImpl retExpression;
    BinaryExpressionImpl binaryOp = new  BinaryExpressionImpl();

    if (whereExpression instanceof EqualsTo) {
      binaryOp.setOperator(com.sonicbase.query.BinaryExpression.Operator.EQUAL);
    }
    else if (whereExpression instanceof LikeExpression) {
      binaryOp.setOperator(com.sonicbase.query.BinaryExpression.Operator.LIKE);
    }
    else if (whereExpression instanceof NotEqualsTo) {
      binaryOp.setOperator(com.sonicbase.query.BinaryExpression.Operator.NOT_EQUAL);
    }
    else if (whereExpression instanceof MinorThan) {
      binaryOp.setOperator(com.sonicbase.query.BinaryExpression.Operator.LESS);
    }
    else if (whereExpression instanceof MinorThanEquals) {
      binaryOp.setOperator(com.sonicbase.query.BinaryExpression.Operator.LESS_EQUAL);
    }
    else if (whereExpression instanceof GreaterThan) {
      binaryOp.setOperator(com.sonicbase.query.BinaryExpression.Operator.GREATER);
    }
    else if (whereExpression instanceof GreaterThanEquals) {
      binaryOp.setOperator(com.sonicbase.query.BinaryExpression.Operator.GREATER_EQUAL);
    }
    else if (whereExpression instanceof Addition) {
      binaryOp.setOperator(com.sonicbase.query.BinaryExpression.Operator.PLUS);
    }
    else if (whereExpression instanceof Subtraction) {
      binaryOp.setOperator(com.sonicbase.query.BinaryExpression.Operator.MINUS);
    }
    else if (whereExpression instanceof Multiplication) {
      binaryOp.setOperator(com.sonicbase.query.BinaryExpression.Operator.TIMES);
    }
    else if (whereExpression instanceof Division) {
      binaryOp.setOperator(com.sonicbase.query.BinaryExpression.Operator.DIVIDE);
    }
    else if (whereExpression instanceof BitwiseAnd) {
      binaryOp.setOperator(com.sonicbase.query.BinaryExpression.Operator.BITWISE_AND);
    }
    else if (whereExpression instanceof BitwiseOr) {
      binaryOp.setOperator(com.sonicbase.query.BinaryExpression.Operator.BITWISE_OR);
    }
    else if (whereExpression instanceof BitwiseXor) {
      binaryOp.setOperator(com.sonicbase.query.BinaryExpression.Operator.BITWISE_X_OR);
    }
    else if (whereExpression instanceof Modulo) {
      binaryOp.setOperator(com.sonicbase.query.BinaryExpression.Operator.MODULO);
    }
    BinaryExpression bexp = (BinaryExpression) whereExpression;
    binaryOp.setNot(bexp.isNot());

    Expression left = bexp.getLeftExpression();
    binaryOp.setLeftExpression(getExpression(client, currParmNum, left, tableName, parms));

    Expression right = bexp.getRightExpression();
    binaryOp.setRightExpression(getExpression(client, currParmNum, right, tableName, parms));

    retExpression = binaryOp;
    return retExpression;
  }

  private static ExpressionImpl getParenthesisExpression(DatabaseClient client, AtomicInteger currParmNum,
                                                         Parenthesis whereExpression, String tableName, ParameterHandler parms) {
    ExpressionImpl retExpression;
    retExpression = getExpression(client, currParmNum, whereExpression.getExpression(), tableName, parms);
    if (whereExpression.isNot()) {
      ParenthesisImpl parens = new ParenthesisImpl();
      parens.setExpression(retExpression);
      parens.setNot(true);
      retExpression = parens;
    }
    return retExpression;
  }

  private static ExpressionImpl getOrExpression(DatabaseClient client, AtomicInteger currParmNum,
                                                OrExpression whereExpression, String tableName, ParameterHandler parms) {
    ExpressionImpl retExpression;
    BinaryExpressionImpl binaryOp = new BinaryExpressionImpl();

    binaryOp.setOperator(com.sonicbase.query.BinaryExpression.Operator.OR);
    Expression leftExpression = whereExpression.getLeftExpression();
    binaryOp.setLeftExpression(getExpression(client, currParmNum, leftExpression, tableName, parms));
    Expression rightExpression = whereExpression.getRightExpression();
    binaryOp.setRightExpression(getExpression(client, currParmNum, rightExpression, tableName, parms));
    retExpression = binaryOp;
    return retExpression;
  }

  private static ExpressionImpl getAndExpression(DatabaseClient client, AtomicInteger currParmNum,
                                                 AndExpression whereExpression, String tableName, ParameterHandler parms) {
    ExpressionImpl retExpression;
    BinaryExpressionImpl binaryOp = new BinaryExpressionImpl();
    binaryOp.setOperator(com.sonicbase.query.BinaryExpression.Operator.AND);
    Expression leftExpression = whereExpression.getLeftExpression();
    binaryOp.setLeftExpression(getExpression(client, currParmNum, leftExpression, tableName, parms));
    Expression rightExpression = whereExpression.getRightExpression();
    binaryOp.setRightExpression(getExpression(client, currParmNum, rightExpression, tableName, parms));
    retExpression = binaryOp;
    return retExpression;
  }

  private static ExpressionImpl getBetweenExpression(Between whereExpression) {
    ExpressionImpl retExpression;
    Column column = (Column) whereExpression.getLeftExpression();

    BinaryExpressionImpl ret = new BinaryExpressionImpl();
    ret.setNot(whereExpression.isNot());
    ret.setOperator(com.sonicbase.query.BinaryExpression.Operator.AND);

    BinaryExpressionImpl leftExpression = new BinaryExpressionImpl();
    ColumnImpl leftColumn = new ColumnImpl();
    if (column.getTable() != null) {
      leftColumn.setTableName(column.getTable().getName());
    }
    leftColumn.setColumnName(column.getColumnName());
    leftExpression.setLeftExpression(leftColumn);

    BinaryExpressionImpl rightExpression = new BinaryExpressionImpl();
    ColumnImpl rightColumn = new ColumnImpl();
    if (column.getTable() != null) {
      rightColumn.setTableName(column.getTable().getName());
    }
    rightColumn.setColumnName(column.getColumnName());
    rightExpression.setLeftExpression(rightColumn);

    leftExpression.setOperator(com.sonicbase.query.BinaryExpression.Operator.GREATER_EQUAL);
    rightExpression.setOperator(com.sonicbase.query.BinaryExpression.Operator.LESS_EQUAL);

    ret.setLeftExpression(leftExpression);
    ret.setRightExpression(rightExpression);

    ConstantImpl leftValue = new ConstantImpl();
    ConstantImpl rightValue = new ConstantImpl();
    if (whereExpression.getBetweenExpressionStart() instanceof LongValue) {
      long start = ((LongValue) whereExpression.getBetweenExpressionStart()).getValue();
      long end = ((LongValue) whereExpression.getBetweenExpressionEnd()).getValue();
      if (start > end) {
        long temp = start;
        start = end;
        end = temp;
      }
      leftValue.setValue(start);
      leftValue.setSqlType(Types.BIGINT);
      rightValue.setValue(end);
      rightValue.setSqlType(Types.BIGINT);
    }
    else if (whereExpression.getBetweenExpressionStart() instanceof StringValue) {
      String start = ((StringValue) whereExpression.getBetweenExpressionStart()).getValue();
      String end = ((StringValue) whereExpression.getBetweenExpressionEnd()).getValue();
      if (0 < start.compareTo(end)) {
        String temp = start;
        start = end;
        end = temp;
      }
      leftValue.setValue(start);
      leftValue.setSqlType(Types.VARCHAR);
      rightValue.setValue(end);
      rightValue.setSqlType(Types.VARCHAR);
    }

    leftExpression.setRightExpression(leftValue);
    rightExpression.setRightExpression(rightValue);

    retExpression = ret;
    return retExpression;
  }

  public static SelectStatementImpl parseSelectStatement(DatabaseClient client, ParameterHandler parms,
                                                         PlainSelect selectBody, AtomicInteger currParmNum) {
    SelectStatementImpl selectStatement = new SelectStatementImpl(client);

    selectStatement.setFromTable(((Table) selectBody.getFromItem()).getName());
    Expression whereExpression = selectBody.getWhere();
    ExpressionImpl expression = getExpression(client, currParmNum, whereExpression, selectStatement.getFromTable(), parms);
    if (expression == null) {
      expression = new AllRecordsExpressionImpl();
      ((AllRecordsExpressionImpl) expression).setFromTable(selectStatement.getFromTable());
    }
    selectStatement.setWhereClause(expression);

    Limit limit = selectBody.getLimit();
    selectStatement.setLimit(limit);
    Offset offset = selectBody.getOffset();
    selectStatement.setOffset(offset);

    parseJoins(client, parms, currParmNum, selectStatement, selectBody);

    Distinct distinct = selectBody.getDistinct();
    if (distinct != null) {
      selectStatement.setIsDistinct();
    }

    List<SelectItem> selectItems = selectBody.getSelectItems();
    for (SelectItem selectItem : selectItems) {
      if (selectItem instanceof SelectExpressionItem) {
        parseSelectExpression(selectStatement, selectBody, expression, selectItems, (SelectExpressionItem) selectItem);
      }
    }

    List<Expression> groupColumns = selectBody.getGroupByColumnReferences();
    if (groupColumns != null && !groupColumns.isEmpty()) {
      for (int i = 0; i < groupColumns.size(); i++) {
        Column column = (Column) groupColumns.get(i);
        selectStatement.addOrderBy(column.getTable().getName(), column.getColumnName(), true);
      }
      selectStatement.setGroupByColumns(groupColumns);
    }

    List<OrderByElement> orderByElements = selectBody.getOrderByElements();
    if (orderByElements != null) {
      for (OrderByElement element : orderByElements) {
        selectStatement.addOrderBy(((Column) element.getExpression()).getTable().getName(),
            ((Column) element.getExpression()).getColumnName(), element.isAsc());
      }
    }
    selectStatement.setPageSize(client.getPageSize());
    selectStatement.setParms(parms);
    return selectStatement;
  }

  private static void parseSelectExpression(SelectStatementImpl selectStatement, PlainSelect pselect,
                                            ExpressionImpl expression, List<SelectItem> selectItems,
                                            SelectExpressionItem selectItem) {
    Alias alias = selectItem.getAlias();
    String aliasName = null;
    if (alias != null) {
      aliasName = alias.getName();
    }

    if (selectItem.getExpression() instanceof Column) {
      selectStatement.addSelectColumn(null, null, ((Column) selectItem.getExpression()).getTable().getName(),
          ((Column) selectItem.getExpression()).getColumnName(), aliasName);
    }
    else if (selectItem.getExpression() instanceof Function) {
      parseFunction(selectStatement, pselect, expression, selectItems, selectItem, aliasName);
    }
  }

  private static void parseFunction(SelectStatementImpl selectStatement, PlainSelect pselect, ExpressionImpl expression,
                                    List<SelectItem> selectItems, SelectExpressionItem item, String aliasName) {
    Function function = (Function) item.getExpression();
    String name = function.getName();
    boolean groupCount = null != pselect.getGroupByColumnReferences() &&
        !pselect.getGroupByColumnReferences().isEmpty() &&
        name.equalsIgnoreCase("count");
    if (groupCount || name.equalsIgnoreCase("min") || name.equalsIgnoreCase("max") ||
        name.equalsIgnoreCase("sum") || name.equalsIgnoreCase("avg")) {
      Column parm = (Column) function.getParameters().getExpressions().get(0);
      selectStatement.addSelectColumn(name, function.getParameters(), parm.getTable().getName(),
          parm.getColumnName(), aliasName);
    }
    else if (name.equalsIgnoreCase("count")) {
      parseCountFunction(selectStatement, pselect, expression, selectItems, function);
    }
    else if (name.equalsIgnoreCase("upper") || name.equalsIgnoreCase("lower") ||
        name.equalsIgnoreCase("substring") || name.equalsIgnoreCase("length")) {
      Column parm = (Column) function.getParameters().getExpressions().get(0);
      selectStatement.addSelectColumn(name, function.getParameters(), parm.getTable().getName(),
          parm.getColumnName(), aliasName);
    }
  }

  private static void parseCountFunction(SelectStatementImpl selectStatement, PlainSelect pselect,
                                         ExpressionImpl expression, List<SelectItem> selectItems, Function function) {
    if (null == pselect.getGroupByColumnReferences() || pselect.getGroupByColumnReferences().isEmpty()) {
      if (function.isAllColumns()) {
        selectStatement.setCountFunction();
      }
      else {
        ExpressionList list = function.getParameters();
        Column column = (Column) list.getExpressions().get(0);
        selectStatement.setCountFunction(column.getTable().getName(), column.getColumnName());
      }
      if (function.isDistinct()) {
        selectStatement.setIsDistinct();
      }

      String currAlias = null;
      for (SelectItem currItem : selectItems) {
        if (((SelectExpressionItem) currItem).getExpression() == function && ((SelectExpressionItem) currItem).getAlias() != null) {
          currAlias = ((SelectExpressionItem) currItem).getAlias().getName();
        }
      }
      parseCountFunctionForNonAllRecordsExpression(selectStatement, pselect, expression, function, currAlias);
    }
  }

  private static void parseCountFunctionForNonAllRecordsExpression(SelectStatementImpl selectStatement,
                                                                   PlainSelect pselect, ExpressionImpl expression,
                                                                   Function function, String currAlias) {
    if (!(expression instanceof AllRecordsExpressionImpl)) {
      String columnName = "__all__";
      if (!function.isAllColumns()) {
        ExpressionList list = function.getParameters();
        Column column = (Column) list.getExpressions().get(0);
        columnName = column.getColumnName();
      }
      selectStatement.addSelectColumn(function.getName(), null, ((Table) pselect.getFromItem()).getName(),
          columnName, currAlias);
    }
  }

  private static void parseJoins(DatabaseClient client, ParameterHandler parms, AtomicInteger currParmNum,
                                 SelectStatementImpl selectStatement, PlainSelect pselect) {
    List<Join> joins = pselect.getJoins();
    if (joins != null) {
      if (!client.getCommon().haveProLicense()) {
        throw new InsufficientLicense("You must have a pro license to execute joins");
      }
      for (Join join : joins) {
        FromItem rightFromItem = join.getRightItem();
        Expression onExpressionSrc = join.getOnExpression();
        ExpressionImpl onExpression = getExpression(client, currParmNum, onExpressionSrc, selectStatement.getFromTable(), parms);

        String rightFrom = rightFromItem.toString();
        SelectStatement.JoinType type = null;
        if (join.isInner()) {
          type = SelectStatement.JoinType.INNER;
        }
        else if (join.isFull()) {
          type = SelectStatement.JoinType.FULL;
        }
        else if (join.isOuter() && join.isLeft()) {
          type = SelectStatement.JoinType.LEFT_OUTER;
        }
        else if (join.isOuter() && join.isRight()) {
          type = SelectStatement.JoinType.RIGHT_OUTER;
        }
        selectStatement.addJoinExpression(type, rightFrom, onExpression);
      }
    }
  }
}
