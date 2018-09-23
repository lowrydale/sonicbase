package com.sonicbase.query.impl;

import com.codahale.metrics.Timer;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.client.DatabaseServerProxy;
import com.sonicbase.common.ComArray;
import com.sonicbase.common.ComObject;
import com.sonicbase.common.Record;
import com.sonicbase.common.SchemaOutOfSyncException;
import com.sonicbase.procedure.StoredProcedureContextImpl;
import com.sonicbase.query.*;
import com.sonicbase.schema.DataType;
import com.sonicbase.schema.FieldSchema;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.Varint;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.Offset;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.sonicbase.client.DatabaseClient.SELECT_PAGE_SIZE;
import static com.sonicbase.client.DatabaseClient.SERIALIZATION_VERSION_27;
import static com.sonicbase.schema.DataType.Type.*;

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class SelectStatementImpl extends StatementImpl implements SelectStatement {

  private static final Logger logger = LoggerFactory.getLogger(SelectStatementImpl.class);
  private ExpressionImpl.RecordCache recordCache;

  private final DatabaseClient client;
  private String fromTable;
  private ExpressionImpl expression;
  private List<OrderByExpressionImpl> orderByExpressions = new ArrayList<>();
  private final List<ColumnImpl> selectColumns = new ArrayList<>();
  private final List<Join> joins = new ArrayList<>();
  private boolean isCountFunction;
  private String countTable;
  private String countColumn;
  private Map<String, ColumnImpl> aliases = new HashMap<>();
  private boolean isDistinct;

  private static final AtomicLong expressionCount = new AtomicLong();
  private static final AtomicLong expressionDuration = new AtomicLong();

  private Map<String, SelectFunctionImpl> functionAliases = new HashMap<>();
  private List<ColumnImpl> columns;
  private boolean isOnServer;
  private boolean serverSelect;
  private boolean serverSort;
  private String[] tableNames;
  private int serverSelectPageNumber = -1;
  private int serverSelectShardNumber = -1;
  private int serverSelectReplicaNumber = -1;
  private long serverSelectResultSetId = -1;
  private Counter[] counters;
  private Limit limit;
  private Offset offset;
  private List<net.sf.jsqlparser.expression.Expression> groupByColumns;
  private GroupByContext groupByContext;
  private Long pageSize = (long) SELECT_PAGE_SIZE;
  private boolean forceSelectOnServer;
  private final AtomicLong currOffset = new AtomicLong();
  private final AtomicLong countReturned = new AtomicLong();
  private short serializationVersion = DatabaseClient.SERIALIZATION_VERSION;

  public SelectStatementImpl(DatabaseClient client) {
    this.client = client;
    this.recordCache = new ExpressionImpl.RecordCache();
  }

  public String getFromTable() {
    return fromTable;
  }

  public void setFromTable(String fromTable) {
    this.fromTable = DatabaseClient.toLower(fromTable);
  }

  public ExpressionImpl getExpression() {
    return expression;
  }

  public void setWhereClause(Expression expression) {
    this.expression = (ExpressionImpl) expression;
  }

  public void setOrderByExpressions(List<OrderByExpressionImpl> list) {
    this.orderByExpressions = list;
  }

  public byte[] serialize() {
    try {
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytesOut);
      serialize(out);
      out.close();
      return bytesOut.toByteArray();
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private void serialize(DataOutputStream out) {
    try {
      Varint.writeSignedVarLong(serializationVersion, out);
      out.writeUTF(fromTable);
      ExpressionImpl.serializeExpression(expression, out);
      out.writeInt(orderByExpressions.size());
      for (OrderByExpressionImpl orderByExpression : orderByExpressions) {
        orderByExpression.serialize(out);
      }
      out.writeInt(selectColumns.size());
      for (ColumnImpl column : selectColumns) {
        column.serialize(serializationVersion, out);
      }
      out.writeBoolean(serverSelect);
      out.writeBoolean(serverSort);

      Varint.writeSignedVarLong(serverSelectPageNumber, out);
      Varint.writeSignedVarLong(serverSelectShardNumber, out);
      Varint.writeSignedVarLong(serverSelectReplicaNumber, out);
      Varint.writeSignedVarLong(serverSelectResultSetId, out);

      Varint.writeSignedVarLong(tableNames.length, out);
      for (int i = 0; i < tableNames.length; i++) {
        out.writeUTF(tableNames[i]);
      }

      Varint.writeSignedVarLong(joins.size(), out);
      for (Join join : joins) {
        join.serialize(out);
      }
      if (groupByContext == null) {
        out.writeBoolean(false);
      }
      else {
        out.writeBoolean(true);
        out.write(groupByContext.serialize(client.getCommon()));
      }

      if (offset == null) {
        out.writeBoolean(false);
      }
      else {
        out.writeBoolean(true);
        out.writeLong(offset.getOffset());
      }
      if (limit == null) {
        out.writeBoolean(false);
      }
      else {
        out.writeBoolean(true);
        out.writeLong(limit.getRowCount());
      }

      getParms().serialize(out);
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public void deserialize(byte[] bytes) {
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
    deserialize(in);
  }

  public void deserialize(DataInputStream in) {
    try {
      serializationVersion = (short) Varint.readSignedVarLong(in);
      fromTable = in.readUTF();
      expression = ExpressionImpl.deserializeExpression(in);
      int count = in.readInt();
      orderByExpressions.clear();
      for (int i = 0; i < count; i++) {
        OrderByExpressionImpl orderByExpression = new OrderByExpressionImpl();
        orderByExpression.deserialize(in);
        orderByExpressions.add(orderByExpression);
      }
      selectColumns.clear();
      count = in.readInt();
      for (int i = 0; i < count; i++) {
        ColumnImpl column = new ColumnImpl();
        column.deserialize(serializationVersion, in);
        selectColumns.add(column);
      }
      for (ColumnImpl column : selectColumns) {
        if (column.getAlias() != null) {
          aliases.put(column.getAlias(), column);
        }
      }
      serverSelect = in.readBoolean();
      serverSort = in.readBoolean();

      serverSelectPageNumber = (int) Varint.readSignedVarLong(in);
      serverSelectShardNumber = (int) Varint.readSignedVarLong(in);
      serverSelectReplicaNumber = (int) Varint.readSignedVarLong(in);
      serverSelectResultSetId = Varint.readSignedVarLong(in);

      int tableCount = (int) Varint.readSignedVarLong(in);
      tableNames = new String[tableCount];
      for (int i = 0; i < tableNames.length; i++) {
        tableNames[i] = in.readUTF();
      }

      expression.setTableName(fromTable);
      expression.setClient(client);
      expression.setParms(getParms());
      expression.setTopLevelExpression(getWhereClause());
      expression.setOrderByExpressions(orderByExpressions);

      joins.clear();
      int joinCount = (int) Varint.readSignedVarLong(in);
      for (int i = 0; i < joinCount; i++) {
        Join join = new Join();
        join.deserialize(in);
        joins.add(join);
      }

      if (in.readBoolean()) {
        groupByContext = new GroupByContext();
        groupByContext.deserialize(in, client.getCommon());
      }
      expression.setGroupByContext(groupByContext);


      if (serializationVersion >= SERIALIZATION_VERSION_27) {
        if (in.readBoolean()) {
          Offset localOffset = new Offset();
          localOffset.setOffset(in.readLong());
          this.offset = localOffset;
        }
        if (in.readBoolean()) {
          Limit localLimit = new Limit();
          localLimit.setRowCount(in.readLong());
          this.limit = localLimit;
        }
      }

      getParms().deserialize(in);
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public void addOrderBy(String tableName, String columnName, boolean isAsc) {
    orderByExpressions.add(new OrderByExpressionImpl(tableName == null ? null : tableName.toLowerCase(),
        columnName.toLowerCase(), isAsc));
  }

  public List<OrderByExpressionImpl> getOrderByExpressions() {
    return orderByExpressions;
  }

  public void addOrderByExpression(String tableName, String columnName, boolean ascending) {
    OrderByExpressionImpl ret = new OrderByExpressionImpl();
    ret.setTableName(tableName.toLowerCase());
    ret.setColumnName(columnName.toLowerCase());
    ret.setAscending(ascending);
    orderByExpressions.add(ret);
  }

  public void setIsOnServer(boolean isOnServer) {
    this.isOnServer = isOnServer;
  }

  public int getServerSelectPageNumber() {
    return serverSelectPageNumber;
  }

  public void setServerSelectPageNumber(int serverSelectPageNumber) {
    this.serverSelectPageNumber = serverSelectPageNumber;
  }

  public void setServerSelectShardNumber(int serverSelectShardNumber) {
    this.serverSelectShardNumber = serverSelectShardNumber;
  }

  public void setServerSelectReplicaNumber(int serverSelectReplicaNumber) {
    this.serverSelectReplicaNumber = serverSelectReplicaNumber;
  }

  public long getServerSelectResultSetId() {
    return serverSelectResultSetId;
  }

  public void setServerSelectResultSetId(long serverSelectResultSetId) {
    this.serverSelectResultSetId = serverSelectResultSetId;
  }

  public String[] getTableNames() {
    return tableNames;
  }

  boolean isServerSelect() {
    return serverSelect;
  }

  int getServerSelectShardNumber() {
    return serverSelectShardNumber;
  }

  int getServerSelectReplicaNumber() {
    return serverSelectReplicaNumber;
  }

  boolean isOnServer() {
    return isOnServer;
  }

  public void setLimit(Limit limit) {
    this.limit = limit;
  }

  public void setOffset(Offset offset) {
    this.offset = offset;
  }

  public void setGroupByColumns(List<net.sf.jsqlparser.expression.Expression> groupByColumns) {
    this.groupByColumns = groupByColumns;
  }

  public void setPageSize(long pageSize) {
    this.pageSize = pageSize;
  }

  void forceSelectOnServer() {
    this.forceSelectOnServer = true;
  }

  boolean isForceSelectOnServer() {
    return forceSelectOnServer;
  }

  Long getPageSize() {
    return pageSize;
  }

  public long getViewVersion() {
    return expression.getViewVersion();
  }

  int getCurrShard() {
    return expression.getNextShard();
  }

  int getLastShard() {
    return expression.getLastShard();
  }

  public boolean isCurrPartitions() {
    return expression.isCurrPartitions();
  }

  public boolean isDistinct() {
    return isDistinct;
  }


  public void addSelectColumn(String function, ExpressionList parameters, String table, String column, String alias) {
    String localFunction = function;
    String localTable = table;
    String localColumn = column;
    String localAlias = alias;
    if (localFunction != null) {
      localFunction = localFunction.toLowerCase();
    }
    if (localTable != null) {
      localTable = localTable.toLowerCase();
    }
    if (localColumn != null) {
      localColumn = localColumn.toLowerCase();
    }
    if (localAlias != null) {
      localAlias = localAlias.toLowerCase();
    }
    else {
      localAlias = "__alias__";
    }
    ColumnImpl columnImpl = new ColumnImpl(localFunction, parameters, localTable, localColumn, localAlias);
    if (localAlias != null) {
      aliases.put(localAlias, columnImpl);
    }
    if (localFunction != null) {
      functionAliases.put(localAlias, new SelectFunctionImpl(localFunction, parameters));
    }
    this.selectColumns.add(columnImpl);
  }

  public Map<String, SelectFunctionImpl> getFunctionAliases() {
    return functionAliases;
  }

  public void setTableNames(String[] tableNames) {
    this.tableNames = tableNames;
  }

  public void setExpression(ExpressionImpl expression) {
    this.expression = expression;
  }

  public void setRecordCache(ExpressionImpl.RecordCache recordCache) {
    this.recordCache = recordCache;
  }

  public void setColumns(List<ColumnImpl> columns) {
    this.columns = columns;
  }

  public List<Join> getJoins() {
    return joins;
  }

  public void forceSelectOnServer(boolean force) {
    this.forceSelectOnServer = force;
  }

  public Offset getOffset() {
    return offset;
  }

  public Limit getLimit() {
    return limit;
  }

  void setAliases(Map<String, ColumnImpl> aliases) {
    this.aliases = aliases;
  }

  public void setFunctionAliases(Map<String, SelectFunctionImpl> functionAliases) {
    this.functionAliases = functionAliases;
  }

  void setServerSelect(boolean serverSelect) {
    this.serverSelect = serverSelect;
  }

  public class DistinctRecord {
    private final Record record;
    private final Comparator[] comparators;
    private final int[] distinctFields;
    private final boolean[] isArray;

    DistinctRecord(
        Record record, Comparator[] comparators, boolean[] isArray, int[] distinctFields) {
      this.record = record;
      this.comparators = comparators;
      this.distinctFields = distinctFields;
      this.isArray = isArray;
    }

    public boolean equals(Object rhsObj) {
      if (!(rhsObj instanceof DistinctRecord)) {
        return false;
      }
      DistinctRecord rhs = (DistinctRecord) rhsObj;
      boolean equals = true;
      for (int i = 0; i < distinctFields.length; i++) {
        int compare = comparators[distinctFields[i]].compare(record.getFields()[distinctFields[i]],
            rhs.record.getFields()[distinctFields[i]]);
        if (compare != 0) {
          equals = false;
          break;
        }
      }
      return equals;
    }

    public int hashCode() {
      int hash = 0;
      for (int i = 0; i < distinctFields.length; i++) {
        if (isArray[distinctFields[i]]) {
          hash += Arrays.hashCode((byte[]) record.getFields()[distinctFields[i]]);
        }
        else {
          hash += record.getFields()[distinctFields[i]].hashCode();
        }
      }
      return hash;
    }
  }

  public static class Explain {
    private StringBuilder builder = new StringBuilder();
    private int indent;

    public StringBuilder getBuilder() {
      return builder;
    }

    public void setBuilder(StringBuilder builder) {
      this.builder = builder;
    }

    public int getIndent() {
      return indent;
    }

    public void setIndent(int indent) {
      this.indent = indent;
    }

    void indent() {
      indent++;
    }

    void outdent() {
      indent--;
    }
  }

  @Override
  public Object execute(String dbName, String sqlToUse, Explain explain, Long sequence0, Long sequence1, Short sequence2,
                        boolean restrictToThisServer, StoredProcedureContextImpl procedureContext, int schemaRetryCount) {
    try {
      expression.setViewVersion(client.getCommon().getSchemaVersion());
      expression.setDbName(dbName);
      expression.setTableName(fromTable);
      expression.setClient(client);
      expression.setParms(getParms());
      expression.setTopLevelExpression(getWhereClause());
      expression.setOrderByExpressions(orderByExpressions);
      expression.setRecordCache(recordCache);

      boolean haveCounters = false;
      boolean needToEvaluate = false;
      List<Counter> countersList = new ArrayList<>();
      GroupByContext groupContext = this.groupByContext;
      if (groupContext == null && groupByColumns != null && !groupByColumns.isEmpty()) {
        processGroupByContext(dbName, groupContext);
      }
      else {
        CheckIfNeedToEvaluate checkIfNeedToEvaluate = new CheckIfNeedToEvaluate(dbName, restrictToThisServer,
            procedureContext, schemaRetryCount, haveCounters, needToEvaluate, countersList).invoke();
        haveCounters = checkIfNeedToEvaluate.isHaveCounters();
        needToEvaluate = checkIfNeedToEvaluate.isNeedToEvaluate();
      }
      if (!haveCounters) {
        needToEvaluate = true;
      }
      else if (explain != null) {
        explain.getBuilder().append("Evaluate counters\n");
      }

      if (!countersList.isEmpty()) {
        Counter[] localCounters = countersList.toArray(new Counter[countersList.size()]);
        this.counters = localCounters;
        expression.setCounters(localCounters);
      }

      getReplica();

      boolean sortWithIndex = expression.canSortWithIndex();
      tableNames = new String[]{fromTable};
      getTableNamesForJoins();

      boolean countDistinct = checkIfCountDistinct();

      if (!countDistinct && this.isCountFunction && expression instanceof AllRecordsExpressionImpl) {
        return countRecords(dbName, explain, tableNames, restrictToThisServer, procedureContext, schemaRetryCount);
      }
      else {
        return executeNonCountSelectStatement(dbName, sqlToUse, explain, restrictToThisServer, procedureContext,
            schemaRetryCount, needToEvaluate, sortWithIndex, countDistinct, haveCounters);
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private Object executeNonCountSelectStatement(String dbName, String sqlToUse, Explain explain,
                                                boolean restrictToThisServer, StoredProcedureContextImpl procedureContext,
                                                int schemaRetryCount, boolean needToEvaluate, boolean sortWithIndex,
                                                boolean countDistinct, boolean haveCounters) {
    Set<ColumnImpl> localColumns = prepareColumns(dbName);

    if (orderByExpressions != null) {
      for (OrderByExpressionImpl localExpression : orderByExpressions) {
        ColumnImpl column = new ColumnImpl();
        column.setTableName(localExpression.getTableName());
        column.setColumnName(localExpression.getColumnName());
        localColumns.add(column);
      }
    }
    List<ColumnImpl> list = new ArrayList<>(localColumns);
    expression.setColumns(list);
    this.columns = list;

    expression.queryRewrite();
    ColumnImpl primaryColumn = expression.getPrimaryColumn();

    boolean isIndexed = checkIfIsIndexed(dbName, primaryColumn);

    checkIfServerSort(primaryColumn, isIndexed);

    serverSelect = false;

    if (serverSort) {
      if (!isOnServer) {
        if (explain != null) {
          explain.getBuilder().append("Server select due to server sort\n");
        }
      }
      serverSelect = true;
      serverSelectResultSetId = -1;
    }

    Set<DistinctRecord> uniqueRecords = new HashSet<>();
    ExpressionImpl.NextReturn ids = null;
    if (needToEvaluate || (haveCounters && explain != null)) {
      ids = next(dbName, explain, restrictToThisServer, procedureContext, schemaRetryCount);
      if (!serverSelect && explain == null) {
        applyDistinct(dbName, tableNames, ids, uniqueRecords);
      }
    }

    return returnResultsForExecute(dbName, sqlToUse, explain, restrictToThisServer, procedureContext,
        sortWithIndex, countDistinct, list, uniqueRecords, ids);
  }

  private boolean checkIfCountDistinct() {
    boolean countDistinct = false;
    if (this.isCountFunction && this.isDistinct) {
      countDistinct = true;
    }
    return countDistinct;
  }

  private void getReplica() {
    Integer replica = expression.getReplica();
    if (replica == null) {
      int replicaCount = client.getCommon().getServersConfig().getShards()[0].getReplicas().length;
      replica = ThreadLocalRandom.current().nextInt(0, replicaCount);
      expression.setReplica(replica);
    }
  }

  private void getTableNamesForJoins() {
    if (!joins.isEmpty()) {
      tableNames = new String[joins.size() + 1];
      tableNames[0] = fromTable;
      for (int i = 0; i < tableNames.length - 1; i++) {
        tableNames[i + 1] = joins.get(i).rightFrom;
      }
    }
  }

  private boolean checkIfIsIndexed(String dbName, ColumnImpl primaryColumn) {
    boolean isIndexed = false;
    if (primaryColumn != null) {
      TableSchema tableSchema = client.getCommon().getTables(dbName).get(primaryColumn.getTableName());
      for (Map.Entry<String, IndexSchema> entry : tableSchema.getIndices().entrySet()) {
        if (entry.getValue().getFields()[0].equals(primaryColumn.getColumnName())) {
          isIndexed = true;
        }
      }
    }
    else if (orderByExpressions != null && !orderByExpressions.isEmpty()) {
      String columnName = orderByExpressions.get(0).getColumnName();
      String tableName = orderByExpressions.get(0).getTableName();
      if (tableName == null) {
        tableName = fromTable;
      }
      TableSchema tableSchema = client.getCommon().getTables(dbName).get(tableName);
      for (Map.Entry<String, IndexSchema> entry : tableSchema.getIndices().entrySet()) {
        if (entry.getValue().getFields()[0].equals(columnName)) {
          isIndexed = true;
        }
      }
    }
    return isIndexed;
  }

  private void checkIfServerSort(ColumnImpl primaryColumn, boolean isIndexed) {
    serverSort = true;
    if (orderByExpressions == null || orderByExpressions.isEmpty()) {
      serverSort = false;
    }
    else {
      String columnName = orderByExpressions.get(0).getColumnName();
      String tableName = orderByExpressions.get(0).getTableName();
      if (primaryColumn == null) {
        serverSort = !isIndexed;
      }
      else {
        if (isIndexed && primaryColumn.getColumnName().equals(columnName) &&
            (tableName == null ||
                primaryColumn.getTableName().equals(tableName))) {
          serverSort = false;
        }
      }
    }
  }

  private Object returnResultsForExecute(String dbName, String sqlToUse, Explain explain, boolean restrictToThisServer,
                                         StoredProcedureContextImpl procedureContext, boolean sortWithIndex,
                                         boolean countDistinct, List<ColumnImpl> list, Set<DistinctRecord> uniqueRecords,
                                         ExpressionImpl.NextReturn ids) {
    if (explain != null) {
      return new ResultSetImpl(explain.getBuilder().toString().split("\\n"));
    }

    ResultSet ret = new ResultSetImpl(dbName, sqlToUse, client, this, getParms(), uniqueRecords,
        new SelectContextImpl(ids, sortWithIndex, tableNames, expression.getNextShard(), expression.getNextKey(),
            this, recordCache, restrictToThisServer, procedureContext), null, list,
        null, counters, limit, offset, currOffset, countReturned, groupByColumns, this.groupByContext,
        restrictToThisServer, procedureContext);
    if (isCountFunction) {
      ret.setIsCount();
    }
    if (countDistinct) {
      long count = ret.getUniqueRecordCount();
      return new ResultSetImpl(dbName, client, this, count, restrictToThisServer, procedureContext);
    }
    else {
      return ret;
    }
  }

  private Set<ColumnImpl> prepareColumns(String dbName) {
    Set<ColumnImpl> localColumns = new HashSet<>();
    expression.getColumns(localColumns);
    if (selectColumns != null && !selectColumns.isEmpty()) {
      prepareColumnsWhereColumnsAreSpecified(dbName, localColumns);
    }
    else {
      for (String tableName : tableNames) {
        TableSchema tableSchema = client.getCommon().getTables(dbName).get(tableName);
        if (tableSchema == null) {
          client.syncSchema();
          tableSchema = client.getCommon().getTables(dbName).get(tableName);
          if (tableSchema == null) {
            throw new DatabaseException("Table does not exist: name=" + tableName);
          }
        }
        for (FieldSchema field : tableSchema.getFields()) {
          ColumnImpl column = new ColumnImpl();
          column.setTableName(tableName);
          column.setColumnName(field.getName());
          localColumns.add(column);
        }
      }
    }
    return localColumns;
  }

  private void prepareColumnsWhereColumnsAreSpecified(String dbName, Set<ColumnImpl> localColumns) {
    for (String tableName : tableNames) {
      for (IndexSchema indexSchema : client.getCommon().getTables(dbName).get(tableName).getIndices().values()) {
        if (indexSchema.isPrimaryKey()) {
          prepareColumnsForPrimaryKey(localColumns, tableName, indexSchema);
          break;
        }
      }
    }
    localColumns.addAll(selectColumns);
  }

  private void prepareColumnsForPrimaryKey(Set<ColumnImpl> localColumns, String tableName, IndexSchema indexSchema) {
    for (String field : indexSchema.getFields()) {
      for (ColumnImpl selectColumn : selectColumns) {
        if ((selectColumn.getTableName() == null || selectColumn.getTableName().equalsIgnoreCase(tableName)) &&
            selectColumn.getColumnName().equalsIgnoreCase(field)) {
          continue;
        }
        ColumnImpl column = new ColumnImpl();
        column.setTableName(tableName);
        column.setColumnName(field);
        localColumns.add(column);
      }
    }
  }

  private void processGroupByContext(String dbName, GroupByContext groupContext) {
    List<GroupByContext.FieldContext> fieldContexts = new ArrayList<>();
    for (int j = 0; j < groupByColumns.size(); j++) {
      GroupByContext.FieldContext fieldContext = new GroupByContext.FieldContext();

      Column column = (Column) groupByColumns.get(j);
      String tableName = column.getTable().getName();
      if (tableName == null) {
        tableName = fromTable;
      }
      int fieldOffset = client.getCommon().getTables(dbName).get(tableName).getFieldOffset(column.getColumnName());
      FieldSchema fieldSchema = client.getCommon().getTables(dbName).get(tableName).getFields().get(fieldOffset);
      fieldContext.setFieldName(column.getColumnName());
      fieldContext.setDataType(fieldSchema.getType());
      fieldContext.setComparator(fieldSchema.getType().getComparator());
      fieldContext.setFieldOffset(fieldOffset);
      fieldContexts.add(fieldContext);
    }


    Set<String> columnsHandled = new HashSet<>();
    Map<String, SelectFunctionImpl> localAliases = getFunctionAliases();

    processFunctionsForGroupByContext(dbName, groupContext, fieldContexts, columnsHandled, localAliases);
  }

  private void processFunctionsForGroupByContext(String dbName, GroupByContext groupContext,
                                                 List<GroupByContext.FieldContext> fieldContexts, Set<String> columnsHandled,
                                                 Map<String, SelectFunctionImpl> localAliases) {
    for (SelectFunctionImpl function : localAliases.values()) {
      if (function.getName().equalsIgnoreCase("count") || function.getName().equalsIgnoreCase("min") ||
          function.getName().equalsIgnoreCase("max") ||
          function.getName().equalsIgnoreCase("avg") || function.getName().equalsIgnoreCase("sum")) {

        groupContext = initGroupByContext(groupContext, fieldContexts);

        String columnName = ((Column) function.getParms().getExpressions().get(0)).getColumnName();
        Counter counter = new Counter();

        String table = ((Column) function.getParms().getExpressions().get(0)).getTable().getName();
        if (table == null) {
          table = getFromTable();
        }

        String k = table + ":" + columnName;
        if (columnsHandled.contains(k)) {
          continue;
        }
        columnsHandled.add(k);

        counter.setTableName(table);
        counter.setColumnName(columnName);

        groupContext.addCounterTemplate(counter);

        int columnOffset = client.getCommon().getTables(dbName).get(table).getFieldOffset(columnName);
        counter.setColumn(columnOffset);
        FieldSchema fieldSchema = client.getCommon().getTables(dbName).get(table).getFields().get(columnOffset);
        counter.setDataType(fieldSchema.getType());
        switch (fieldSchema.getType()) {
          case INTEGER:
          case BIGINT:
          case SMALLINT:
          case TINYINT:
            counter.setDestTypeToLong();
            break;
          case FLOAT:
          case DOUBLE:
          case NUMERIC:
          case DECIMAL:
          case REAL:
            counter.setDestTypeToDouble();
            break;
        }
      }
    }
  }

  private GroupByContext initGroupByContext(GroupByContext groupContext, List<GroupByContext.FieldContext> fieldContexts) {
    if (groupContext == null) {
      groupContext = new GroupByContext(fieldContexts);
      this.groupByContext = groupContext;
      expression.setGroupByContext(groupContext);
    }
    return groupContext;
  }

  public ExpressionImpl.NextReturn serverSelect(String dbName, Explain explain, String[] tableNames,
                                                boolean restrictToThisServer, StoredProcedureContextImpl procedureContext) {
    while (true) {
      try {
        if (explain != null) {
          if (!explain.getBuilder().toString().contains("Server select")) {
            explain.getBuilder().append("Server select\n");
          }
        }
        ComObject cobj = new ComObject();
        cobj.put(ComObject.Tag.LEGACY_SELECT_STATEMENT, serialize());
        cobj.put(ComObject.Tag.SCHEMA_VERSION, client.getCommon().getSchemaVersion());
        cobj.put(ComObject.Tag.COUNT, pageSize);
        cobj.put(ComObject.Tag.DB_NAME, dbName);
        cobj.put(ComObject.Tag.CURR_OFFSET, currOffset.get());
        cobj.put(ComObject.Tag.COUNT_RETURNED, countReturned.get());
        cobj.put(ComObject.Tag.SHOULD_EXPLAIN, explain != null);

        ComObject retObj = null;
        if (restrictToThisServer) {
          retObj = DatabaseServerProxy.serverSelect(client.getDatabaseServer(), cobj, restrictToThisServer, procedureContext);
        }
        else {
          byte[] recordRet = client.send("ReadManager:serverSelect",
              Math.abs(ThreadLocalRandom.current().nextInt() % client.getShardCount()),
              Math.abs(ThreadLocalRandom.current().nextLong()), cobj, DatabaseClient.Replica.DEF);
          retObj = new ComObject(recordRet);
        }

        if (explain != null) {
          explain.getBuilder().append(retObj.getString(ComObject.Tag.EXPLAIN));
        }

        return processResponseForServerSelect(dbName, tableNames, retObj);
      }
      catch (SchemaOutOfSyncException e) {
        //try again
      }
    }

  }

  private ExpressionImpl.NextReturn processResponseForServerSelect(String dbName, String[] tableNames, ComObject retObj) {
    byte[] selectBytes = retObj.getByteArray(ComObject.Tag.LEGACY_SELECT_STATEMENT);
    deserialize(selectBytes);

    setCurrOffsetAndCountReturned(retObj);

    TableSchema[] tableSchemas = new TableSchema[tableNames.length];
    for (int i = 0; i < tableNames.length; i++) {
      tableSchemas[i] = client.getCommon().getTables(dbName).get(tableNames[i]);
    }

    String[][] primaryKeyFields = getPrimaryKeyFields(tableNames, tableSchemas);

    ComArray tableRecords = retObj.getArray(ComObject.Tag.TABLE_RECORDS);
    Object[][][] retKeys = new Object[tableRecords == null ? 0 : tableRecords.getArray().size()][][];
    Record[][] currRetRecords = new Record[tableRecords == null ? 0 : tableRecords.getArray().size()][];
    for (int k = 0; k < currRetRecords.length; k++) {
      currRetRecords[k] = new Record[tableNames.length];
      retKeys[k] = new Object[tableNames.length][];
      ComArray records = (ComArray) tableRecords.getArray().get(k);

      doProcessResponseForServerSelect(dbName, tableNames, tableSchemas, primaryKeyFields, retKeys, currRetRecords, k, records);
    }
    return new ExpressionImpl.NextReturn(tableNames, retKeys);
  }

  private void doProcessResponseForServerSelect(String dbName, String[] tableNames, TableSchema[] tableSchemas,
                                                String[][] primaryKeyFields, Object[][][] retKeys, Record[][] currRetRecords,
                                                int k, ComArray records) {
    for (int j = 0; j < tableNames.length; j++) {
      if (records.getArray().size() <= j) {
        continue;
      }
      byte[] recordBytes = (byte[]) records.getArray().get(j);
      if (recordBytes != null) {
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

        recordCache.put(tableNames[j], key, new ExpressionImpl.CachedRecord(record, recordBytes));
      }
    }
  }

  private void setCurrOffsetAndCountReturned(ComObject retObj) {
    if (retObj.getLong(ComObject.Tag.CURR_OFFSET) != null) {
      currOffset.set(retObj.getLong(ComObject.Tag.CURR_OFFSET));
    }
    if (retObj.getLong(ComObject.Tag.COUNT_RETURNED) != null) {
      countReturned.set(retObj.getLong(ComObject.Tag.COUNT_RETURNED));
    }
  }

  private String[][] getPrimaryKeyFields(String[] tableNames, TableSchema[] tableSchemas) {
    String[][] primaryKeyFields = new String[tableNames.length][];
    for (int i = 0; i < tableNames.length; i++) {
      for (Map.Entry<String, IndexSchema> entry : tableSchemas[i].getIndices().entrySet()) {
        if (entry.getValue().isPrimaryKey()) {
          primaryKeyFields[i] = entry.getValue().getFields();
          break;
        }
      }
    }
    return primaryKeyFields;
  }

  public void applyDistinct(String dbName, String[] tableNames, ExpressionImpl.NextReturn ids, Set<DistinctRecord> uniqueRecords) {
    if (isDistinct) {
      int tableIndex = 0;
      for (int i = 0; i < tableNames.length; i++) {
        if (tableNames[i].equals(fromTable)) {
          tableIndex = i;
        }
      }

      TableSchema tableSchema = client.getCommon().getTables(dbName).get(fromTable);
      String[] fields = new String[tableSchema.getFields().size()];
      boolean[] isArray = new boolean[fields.length];
      for (int i = 0; i < fields.length; i++) {
        fields[i] = tableSchema.getFields().get(i).getName();
        if (VARCHAR == tableSchema.getFields().get(i).getType()) {
          isArray[i] = true;
        }
      }
      Comparator[] comparators = tableSchema.getComparators(fields);
      int[] distinctFields = new int[selectColumns.size()];
      for (int i = 0; i < selectColumns.size(); i++) {
        distinctFields[i] = tableSchema.getFieldOffset(selectColumns.get(i).getColumnName());
      }
      List<Object[][]> actualIds = doAddDistinctRecord(ids, uniqueRecords, tableIndex, isArray, comparators, distinctFields);
      ids.setIds(new Object[actualIds.size()][][]);
      for (int i = 0; i < actualIds.size(); i++) {
        ids.getIds()[i] = actualIds.get(i);
      }
    }
  }

  private List<Object[][]> doAddDistinctRecord(ExpressionImpl.NextReturn ids, Set<DistinctRecord> uniqueRecords,
                                               int tableIndex, boolean[] isArray, Comparator[] comparators, int[] distinctFields) {
    List<Object[][]> actualIds = new ArrayList<>();
    for (int i = 0; i < ids.getIds().length; i++) {
      Record record = recordCache.get(fromTable, ids.getIds()[i][tableIndex]).getRecord();
      if (!uniqueRecords.contains(new DistinctRecord(record, comparators, isArray, distinctFields))) {
        actualIds.add(ids.getIds()[i]);
        uniqueRecords.add(new DistinctRecord(record, comparators, isArray, distinctFields));
      }
    }
    return actualIds;
  }

  private ResultSet countRecords(final String dbName, Explain explain, String[] tableNames, boolean restrictToThisServer,
                                 StoredProcedureContextImpl procedureContext, int schemaRetryCount) {
    int tableIndex = 0;
    if (this.countTable != null) {
      for (int i = 0; i < tableNames.length; i++) {
        if (tableNames[i].equals(this.countTable)) {
          tableIndex = i;
          break;
        }
      }
    }
    else {
      this.countTable = tableNames[0];
    }

    if (explain != null) {
      explain.getBuilder().append("Count records, all shards: table=" + countTable + ", column=" + countColumn +
          ", expression=" + expression.toString());
      return new ResultSetImpl(explain.getBuilder().toString().split("\\n"));
    }

    int columnIndex = getColumnIndex(dbName);
    if (!joins.isEmpty()) {
      return doCountRecordsForJoins(dbName, explain, restrictToThisServer, procedureContext, schemaRetryCount, tableIndex, columnIndex);
    }
    else {
      while (true) {
        ResultSet count = doCountRecords(dbName, explain, restrictToThisServer, procedureContext);
        if (count != null) {
          return count;
        }
      }
    }
  }

  private int getColumnIndex(String dbName) {
    int columnIndex = 0;
    if (this.countColumn != null) {
      TableSchema tableSchema = client.getCommon().getTables(dbName).get(this.countTable);
      for (int i = 0; i < tableSchema.getFields().size(); i++) {
        if (this.countColumn.equals(tableSchema.getFields().get(i).getName())) {
          columnIndex = i;
          break;
        }
      }
    }
    return columnIndex;
  }

  private ResultSet doCountRecordsForJoins(String dbName, Explain explain, boolean restrictToThisServer,
                                           StoredProcedureContextImpl procedureContext, int schemaRetryCount, int tableIndex,
                                           int columnIndex) {
    long count = 0;
    while (true) {
      ExpressionImpl.NextReturn ids = next(dbName, null, restrictToThisServer, procedureContext, schemaRetryCount);
      if (ids == null || ids.getIds() == null) {
        break;
      }
      if (this.countColumn != null) {
        for (Object[][] key : ids.getIds()) {
          Record record = recordCache.get(this.countTable, key[tableIndex]).getRecord();
          if (record.getFields()[columnIndex] != null) {
            count++;
          }
        }
      }
      else {
        count += ids.getIds().length;
      }
    }
    return new ResultSetImpl(dbName, client, this, count, restrictToThisServer, procedureContext);
  }

  private ResultSet doCountRecords(String dbName, Explain explain, boolean restrictToThisServer, StoredProcedureContextImpl procedureContext) {
    long count = 0;
    try {
      int shardCount = client.getShardCount();
      List<Future> futures = new ArrayList<>();
      for (int i = 0; i < shardCount; i++) {
        final int shard = i;
        futures.add(client.getExecutor().submit((Callable) () -> {
          ComObject cobj = new ComObject();

          prepareComObjectForCountRecords(dbName, cobj);

          byte[] lookupRet = client.send("ReadManager:countRecords", shard, 0, cobj, DatabaseClient.Replica.MASTER);
          ComObject retObj = new ComObject(lookupRet);
          return (long) retObj.getLong(ComObject.Tag.COUNT_LONG);
        }));
      }
      for (Future future : futures) {
        count += (long) future.get();
      }
      return new ResultSetImpl(dbName, client, this, count, restrictToThisServer, procedureContext);
    }
    catch (SchemaOutOfSyncException e) {
      try {
        Thread.sleep(200);
      }
      catch (InterruptedException e1) {
        Thread.currentThread().interrupt();
        throw new DatabaseException(e1);
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
    return null;
  }

  private void prepareComObjectForCountRecords(String dbName, ComObject cobj) throws IOException {
    cobj.put(ComObject.Tag.SERIALIZATION_VERSION, DatabaseClient.SERIALIZATION_VERSION);
    if (expression instanceof AllRecordsExpressionImpl) {
      expression = null;
    }
    if (expression != null) {
      cobj.put(ComObject.Tag.LEGACY_EXPRESSION, ExpressionImpl.serializeExpression(expression));
    }

    if (getParms() != null) {
      cobj.put(ComObject.Tag.PARMS, getParms().serialize());
    }

    if (SelectStatementImpl.this.countTable != null) {
      cobj.put(ComObject.Tag.COUNT_TABLE_NAME, SelectStatementImpl.this.countTable);
    }

    if (SelectStatementImpl.this.countColumn != null) {
      cobj.put(ComObject.Tag.COUNT_COLUMN, SelectStatementImpl.this.countColumn);
    }

    cobj.put(ComObject.Tag.DB_NAME, dbName);
    cobj.put(ComObject.Tag.SCHEMA_VERSION, client.getCommon().getSchemaVersion());
    cobj.put(ComObject.Tag.TABLE_NAME, fromTable);
  }

  public ExpressionImpl.NextReturn next(String dbName, Explain explain, boolean restrictToThisServer,
                                        StoredProcedureContextImpl procedureContext, int schemaRetryCount) {
    while (true) {
      try {
        AtomicBoolean didTableScan = new AtomicBoolean();
        return doNext(dbName, explain, restrictToThisServer, procedureContext, schemaRetryCount, didTableScan);
      }
      catch (SchemaOutOfSyncException e) {
        handleSchemaOutOfSyncException();
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }

  }

  private ExpressionImpl.NextReturn doNext(String dbName, Explain explain, boolean restrictToThisServer,
                                           StoredProcedureContextImpl procedureContext, int schemaRetryCount, AtomicBoolean didTableScan) {
    ExpressionImpl localExpression = getExpression();

    int count = prepareNext(localExpression);

    localExpression.setRecordCache(recordCache);
    if (!joins.isEmpty()) {
      return handleJoins(count, dbName, explain, restrictToThisServer, procedureContext, schemaRetryCount);
    }
    ExpressionImpl.NextReturn ret;
    if (!isOnServer && serverSelect) {
      if (currOffset.get() != 0 && countReturned.get() < count) {
        return null;
      }
      ret = serverSelect(dbName, explain, tableNames, restrictToThisServer, procedureContext);
    }
    else {
      localExpression.forceSelectOnServer(forceSelectOnServer);
      localExpression.setDbName(dbName);
      localExpression.setRestrictToThisServer(restrictToThisServer);
      localExpression.setProcedureContext(procedureContext);
      ret = localExpression.next(this, count, explain, currOffset, countReturned, limit, offset,
          false, false, schemaRetryCount, didTableScan);
    }
    if (ret == null) {
      return null;
    }
    dedupIds(dbName, ret.getTableNames(), ret);
    return ret;
  }

  private void handleSchemaOutOfSyncException() {
    expression.setViewVersion(client.getCommon().getSchemaVersion());
    try {
      Thread.sleep(200);
    }
    catch (InterruptedException e1) {
      Thread.currentThread().interrupt();
      throw new DatabaseException(e1);
    }
  }

  private int prepareNext(ExpressionImpl localExpression) {
    Integer replica = localExpression.getReplica();
    if (replica == null) {
      int replicaCount = client.getCommon().getServersConfig().getShards()[0].getReplicas().length;
      replica = ThreadLocalRandom.current().nextInt(0, replicaCount);
      localExpression.setReplica(replica);
    }
    int count = DatabaseClient.SELECT_PAGE_SIZE;
    if (this.pageSize != null) {
      count = (int) (long) this.pageSize;
    }
    return count;
  }

  public void setCountFunction() {
    this.isCountFunction = true;
  }

  public void setCountFunction(String table, String columnName) {
    this.isCountFunction = true;
    this.countTable = table;
    this.countColumn = columnName;
  }

  public Map<String, ColumnImpl> getAliases() {
    return aliases;
  }

  public void setIsDistinct() {
    this.isDistinct = true;
  }

  class DedupComparator implements Comparator<Object[][]> {
    private final Comparator[][] comparators;

    public DedupComparator(Comparator[][] comparators) {
      this.comparators = comparators;
    }

    @Override
    public int compare(Object[][] o1, Object[][] o2) {
      for (int j = 0; j < Math.min(o1.length, o2.length); j++) {
        if (checkForNulls(o1, o2, j)) {
          continue;
        }
        for (int i = 0; i < Math.min(o1[j].length, o2[j].length); i++) {
          if (o1[j][i] == null || o2[j][i] == null) {
            continue;
          }
          int value = comparators[j][i].compare(o1[j][i], o2[j][i]);
          if (value < 0) {
            return -1;
          }
          if (value > 0) {
            return 1;
          }
        }
      }
      return 0;
    }

    private boolean checkForNulls(Object[][] o1, Object[][] o2, int j) {
      return o1[j] == null || o2[j] == null;
    }
  }


  private void dedupIds(String dbName, String[] tableNames, ExpressionImpl.NextReturn ids) {
    if (ids.getIds() == null) {
      return;
    }
    if (ids.getIds().length == 1) {
      return;
    }
    final Comparator[][] comparators = new Comparator[tableNames.length][];
    for (int i = 0; i < tableNames.length; i++) {
      TableSchema schema = client.getCommon().getTables(dbName).get(tableNames[i]);
      for (Map.Entry<String, IndexSchema> indexSchema : schema.getIndices().entrySet()) {
        if (indexSchema.getValue().isPrimaryKey()) {
          comparators[i] = indexSchema.getValue().getComparators();
          break;
        }
      }
    }

    DedupComparator comparator = new DedupComparator(comparators);

    Object[][][] actualIds = ids.getIds();
    ConcurrentSkipListSet<Object[][]> map = new ConcurrentSkipListSet<>(comparator);
    map.addAll(Arrays.asList(actualIds));

    Object[][][] retIds = new Object[map.size()][][];
    int localOffset = 0;
    for (int i = 0; i < actualIds.length; i++) {
      if (map.contains(actualIds[i])) {
        retIds[localOffset] = actualIds[i];
        map.remove(actualIds[i]);
        localOffset++;
      }
    }
    ids.setIds(retIds);
  }

  private ExpressionImpl.NextReturn handleJoins(int pageSize, String dbName, Explain explain, boolean restrictToThisServer,
                                                StoredProcedureContextImpl procedureContext, int schemaRetryCount) {
    Timer.Context ctx = DatabaseClient.JOIN_EVALUATE.time();
    try {
      final AtomicReference<List<Object[][]>> multiTableIds = new AtomicReference<>();
      String[] localTableNames = new String[joins.size() + 1];
      localTableNames[0] = fromTable;
      for (int i = 0; i < localTableNames.length - 1; i++) {
        localTableNames[i + 1] = joins.get(i).rightFrom;
      }

      for (Join join : joins) {
        ((ExpressionImpl) join.expression).getColumnsInExpression(columns);
      }

      final ExpressionImpl.NextReturn ret = new ExpressionImpl.NextReturn();

      handleEachJoinInStatement(pageSize, dbName, explain, restrictToThisServer, procedureContext, schemaRetryCount,
          multiTableIds, localTableNames);

      if (multiTableIds == null || multiTableIds.get() == null) {
        return null;
      }
      Object[][][] tableIds = new Object[multiTableIds.get().size()][][];
      for (int i = 0; i < tableIds.length; i++) {
        tableIds[i] = multiTableIds.get().get(i);
      }

      ret.setTableNames(localTableNames);
      for (int i = 0; i < localTableNames.length; i++) {
        for (Map.Entry<String, IndexSchema> entry :
            client.getCommon().getTables(dbName).get(localTableNames[i]).getIndices().entrySet()) {
          if (entry.getValue().isPrimaryKey()) {
            ret.setFields(localTableNames[i], entry.getValue().getFields());
          }
        }
      }
      ret.setIds(tableIds);
      return ret;
    }
    finally {
      ctx.stop();
    }
  }

  private void handleEachJoinInStatement(int pageSize, String dbName, Explain explain, boolean restrictToThisServer,
                                         StoredProcedureContextImpl procedureContext, int schemaRetryCount,
                                         AtomicReference<List<Object[][]>> multiTableIds, String[] localTableNames) {
    while (true) {
      try {
        AtomicBoolean hadSelectRet = new AtomicBoolean();
        for (int k = 0; k < joins.size(); k++) {
          Join join = joins.get(k);

          String joinRightFrom = join.rightFrom;
          Expression joinExpression = join.expression;
          final JoinType joinType = join.type;

          Object[] lastKey = expression.getNextKey();

          doHandleJoin(pageSize, dbName, explain, restrictToThisServer, procedureContext, schemaRetryCount,
              multiTableIds, localTableNames, hadSelectRet, k, joinRightFrom, joinExpression, joinType, lastKey);
        }
        if (hadSelectRet.get() && multiTableIds.get().isEmpty()) {
          multiTableIds.set(null);
          continue;
        }
        break;
      }
      catch (Exception e) {
        handleJoinException(e);
      }
    }
  }

  private void doHandleJoin(int pageSize, String dbName, Explain explain, boolean restrictToThisServer,
                            StoredProcedureContextImpl procedureContext, int schemaRetryCount,
                            AtomicReference<List<Object[][]>> multiTableIds, String[] localTableNames,
                            AtomicBoolean hadSelectRet, int k, String joinRightFrom, Expression joinExpression,
                            JoinType joinType, Object[] lastKey) {
    if (!(joinExpression instanceof BinaryExpressionImpl)) {
      throw new DatabaseException("Join expression type not supported");
    }
    BinaryExpressionImpl joinBinaryExpression = (BinaryExpressionImpl) joinExpression;
    ExpressionImpl leftExpression = joinBinaryExpression.getLeftExpression();
    ExpressionImpl rightExpression = joinBinaryExpression.getRightExpression();
    ExpressionImpl additionalJoinExpression = null;
    if (leftExpression instanceof BinaryExpressionImpl && rightExpression instanceof BinaryExpressionImpl) {
      PrepareJoinExpression prepareJoinExpression = new PrepareJoinExpression(joinBinaryExpression).invoke();
      leftExpression = prepareJoinExpression.getLeftExpression();
      rightExpression = prepareJoinExpression.getRightExpression();
      additionalJoinExpression = prepareJoinExpression.getAdditionalJoinExpression();
    }
    if (leftExpression instanceof ColumnImpl && rightExpression instanceof ColumnImpl) {
      doHandleJoinForColumnCompare(pageSize, dbName, explain, restrictToThisServer, procedureContext,
          schemaRetryCount, multiTableIds, localTableNames, hadSelectRet, k, joinRightFrom, joinType, lastKey,
          (ColumnImpl) leftExpression, (ColumnImpl) rightExpression, additionalJoinExpression);
    }
  }

  private void handleJoinException(Exception e) {
    if (-1 != ExceptionUtils.indexOfThrowable(e, SchemaOutOfSyncException.class)) {
      expression.setViewVersion(client.getCommon().getSchemaVersion());
      logger.error("SchemaOutOfSyncException");
      try {
        Thread.sleep(200);
      }
      catch (InterruptedException e1) {
        Thread.currentThread().interrupt();
        throw new DatabaseException(e);
      }
    }
    else {
      throw new DatabaseException(e);
    }
  }

  private void doHandleJoinForColumnCompare(int pageSize, String dbName, Explain explain, boolean restrictToThisServer, StoredProcedureContextImpl procedureContext, int schemaRetryCount, AtomicReference<List<Object[][]>> multiTableIds, String[] localTableNames, AtomicBoolean hadSelectRet, int k, String joinRightFrom, JoinType joinType, Object[] lastKey, ColumnImpl leftExpression, ColumnImpl rightExpression, ExpressionImpl additionalJoinExpression) {
    final AtomicReference<ColumnImpl> leftColumn = new AtomicReference<>(leftExpression);
    final AtomicReference<ColumnImpl> rightColumn = new AtomicReference<>(rightExpression);

    if (!leftColumn.get().getTableName().equals(localTableNames[k])) {
      ColumnImpl tmp = leftColumn.get();
      leftColumn.set(rightColumn.get());
      rightColumn.set(tmp);
    }
    int threadCount = 1;
    if (multiTableIds.get() != null) {
      threadCount = 1;
    }
    final AtomicReference<JoinReturn> joinRet = new AtomicReference<>();
    for (int x = 0; x < threadCount; x++) {
      doHandleJoins(pageSize, dbName, explain, restrictToThisServer, procedureContext, schemaRetryCount,
          multiTableIds, localTableNames, k, joinRightFrom, joinType, lastKey, additionalJoinExpression,
          leftColumn, rightColumn, threadCount, joinRet, hadSelectRet);
    }
    if (joinRet.get() != null) {
      multiTableIds.set(joinRet.get().keys);
    }
  }

  private void doHandleJoins(int pageSize, String dbName, Explain explain, boolean restrictToThisServer,
                             StoredProcedureContextImpl procedureContext, int schemaRetryCount,
                             AtomicReference<List<Object[][]>> multiTableIds, String[] localTableNames, int k,
                             String joinRightFrom, JoinType joinType, Object[] lastKey, ExpressionImpl additionalJoinExpression,
                             AtomicReference<ColumnImpl> leftColumn, AtomicReference<ColumnImpl> rightColumn, int threadCount,
                             AtomicReference<JoinReturn> joinRet, AtomicBoolean hadSelectRet) {
    final AtomicReference<String> leftFrom = new AtomicReference<>(k == 0 ? fromTable : localTableNames[k]);
    final AtomicReference<String> rightFrom = new AtomicReference<>(localTableNames[k + 1]);
    ExpressionImpl.NextReturn ids = null;
    if (joinType == JoinType.INNER) {
      ids = prepareInnerJoin(pageSize, explain, schemaRetryCount, multiTableIds, threadCount, hadSelectRet);
    }
    else if (joinType == JoinType.LEFT_OUTER || joinType == JoinType.FULL) {
      ids = prepareLeftOuterAndFullJoin(pageSize, dbName, explain, schemaRetryCount, multiTableIds,
          joinType, threadCount, hadSelectRet);
    }
    else if (joinType == JoinType.RIGHT_OUTER) {
      ids = prepareRightOuterJoin(pageSize, dbName, explain, schemaRetryCount, multiTableIds, joinRightFrom,
          leftColumn, rightColumn, threadCount, leftFrom, rightFrom, hadSelectRet);
    }
    final List<String> joinColumns = new ArrayList<>();
    joinColumns.add(leftColumn.get().getColumnName());

    final TableSchema leftTable = client.getCommon().getTables(dbName).get(leftFrom.get());
    final TableSchema rightTable = client.getCommon().getTables(dbName).get(rightFrom.get());

    final AtomicInteger rightTableIndex = new AtomicInteger();
    final AtomicInteger leftTableIndex = new AtomicInteger();

    getLeftAndRightTableIndices(localTableNames, leftTable, rightTable, rightTableIndex, leftTableIndex);

    final AtomicReference<List<Object[][]>> idsToProcess = prepareIdsToProcess(multiTableIds, localTableNames,
        ids, leftTableIndex);

    joinRet.set(evaluateJoin(pageSize, dbName, idsToProcess, joinType,
        leftColumn, rightColumn, leftTable, rightTable, rightTableIndex, leftTableIndex, explain,
        restrictToThisServer, procedureContext, schemaRetryCount));

    handleNonFullJoin(dbName, localTableNames, joinType, lastKey, joinRet, leftTableIndex);

    processAdditionalJoinExpression(dbName, explain, localTableNames, additionalJoinExpression, joinRet);
  }

  private void handleNonFullJoin(String dbName, String[] localTableNames, JoinType joinType, Object[] lastKey,
                                 AtomicReference<JoinReturn> joinRet, AtomicInteger leftTableIndex) {
    if (joinType != JoinType.FULL && joinRet.get() != null) {
      AtomicReference<JoinReturn> finalJoinRet = new AtomicReference<>();
      finalJoinRet.set(new JoinReturn());
      TableSchema[] tables = new TableSchema[localTableNames.length];
      for (int i = 0; i < tables.length; i++) {
        tables[i] = client.getCommon().getTables(dbName).get(localTableNames[i]);
      }
      Object[][] previousKey = null;
      if (lastKey != null) {
        previousKey = new Object[tables.length][];
        previousKey[leftTableIndex.get()] = lastKey;
      }
      for (Object[][] key : joinRet.get().keys) {
        Record[] records = new Record[localTableNames.length];
        for (int i = 0; i < records.length; i++) {
          if (key[i] != null) {
            records[i] = recordCache.get(localTableNames[i], key[i]).getRecord();
          }
        }
        boolean passes = (boolean) expression.evaluateSingleRecord(tables, records, getParms());

        postProcessSingleRecord(leftTableIndex, finalJoinRet, tables, previousKey, key, passes);

        previousKey = key;
      }
      joinRet.set(finalJoinRet.get());
    }
  }

  private void getLeftAndRightTableIndices(String[] localTableNames, TableSchema leftTable, TableSchema rightTable,
                                           AtomicInteger rightTableIndex, AtomicInteger leftTableIndex) {
    for (int j = 0; j < localTableNames.length; j++) {
      if (localTableNames[j].equals(rightTable.getName())) {
        rightTableIndex.set(j);
        break;
      }
    }
    for (int j = 0; j < localTableNames.length; j++) {
      if (localTableNames[j].equals(leftTable.getName())) {
        leftTableIndex.set(j);
        break;
      }
    }
  }

  private AtomicReference<List<Object[][]>> prepareIdsToProcess(AtomicReference<List<Object[][]>> multiTableIds,
                                                                String[] localTableNames, ExpressionImpl.NextReturn ids,
                                                                AtomicInteger leftTableIndex) {
    final AtomicReference<List<Object[][]>> idsToProcess = new AtomicReference<>();
    idsToProcess.set(new ArrayList<>());
    if (ids != null && ids.getKeys() != null) {
      for (Object[][] id : ids.getKeys()) {
        Object[][] newId = new Object[localTableNames.length][];
        newId[leftTableIndex.get()] = id[0];
        idsToProcess.get().add(newId);
      }
    }
    else {
      idsToProcess.set(multiTableIds.get());
    }
    return idsToProcess;
  }

  private void postProcessSingleRecord(AtomicInteger leftTableIndex, AtomicReference<JoinReturn> finalJoinRet,
                                       TableSchema[] tables, Object[][] previousKey, Object[][] key, boolean passes) {
    if (!passes) {
      boolean equals = true;
      for (int i = 0; i < tables.length; i++) {
        if (leftTableIndex.get() != i) {
          key[i] = null;
        }
        else {
          equals = checkIfKeyEquals(previousKey, key[i], equals, i);
        }
      }
      if (previousKey == null || !equals || key.length == 1) {
        finalJoinRet.get().keys.add(key);
      }
    }
    else {
      finalJoinRet.get().keys.add(key);
    }
  }

  private boolean checkIfKeyEquals(Object[][] previousKey, Object[] objects, boolean equals, int i) {
    for (int j = 0; j < objects.length; j++) {
      if (previousKey != null) {
        Object lhsValue = objects[j];
        Object rhsValue = previousKey[i][j];
        Comparator comparator = getComparatorForKeyEquals(lhsValue, rhsValue);

        if (lhsValue == null || rhsValue == null) {
          equals = false;
          break;
        }
        else {
          if (comparator.compare(lhsValue, rhsValue) != 0) {
            equals = false;
            break;
          }
        }
      }
    }
    return equals;
  }

  private Comparator getComparatorForKeyEquals(Object lhsValue, Object rhsValue) {
    Comparator comparator = getComparatorForValue(lhsValue);
    if (lhsValue instanceof BigDecimal || rhsValue instanceof BigDecimal) {
      comparator = DataType.getBigDecimalComparator();
    }
    else if (lhsValue instanceof Double || rhsValue instanceof Double ||
        lhsValue instanceof Float || rhsValue instanceof Float) {
      comparator = DataType.getDoubleComparator();
    }
    return comparator;
  }

  private void processAdditionalJoinExpression(String dbName, Explain explain, String[] localTableNames,
                                               ExpressionImpl additionalJoinExpression, AtomicReference<JoinReturn> joinRet) {
    if (additionalJoinExpression != null) {
      if (explain != null) {
        explain.getBuilder().append("Evaluating join expression: expression=").append(additionalJoinExpression.toString()).append("\n");
      }
      if (joinRet.get() != null) {
        List<Object[][]> retKeys = new ArrayList<>();
        TableSchema[] tables = new TableSchema[localTableNames.length];
        for (int i = 0; i < tables.length; i++) {
          tables[i] = client.getCommon().getTables(dbName).get(localTableNames[i]);
        }
        for (Object[][] key : joinRet.get().keys) {
          checkIfRecordPasses(localTableNames, additionalJoinExpression, retKeys, tables, key);
        }
        joinRet.get().keys = retKeys;
      }
    }
  }

  private void checkIfRecordPasses(String[] localTableNames, ExpressionImpl additionalJoinExpression,
                                   List<Object[][]> retKeys, TableSchema[] tables, Object[][] key) {
    Record[] records = new Record[localTableNames.length];
    for (int i = 0; i < records.length; i++) {
      if (key[i] != null) {
        records[i] = recordCache.get(localTableNames[i], key[i]).getRecord();
      }
    }
    boolean passes = (boolean) additionalJoinExpression.evaluateSingleRecord(tables, records, getParms());
    if (passes) {
      retKeys.add(key);
    }
  }

  private ExpressionImpl.NextReturn prepareInnerJoin(int pageSize, Explain explain, int schemaRetryCount,
                                                     AtomicReference<List<Object[][]>> multiTableIds, int threadCount,
                                                     AtomicBoolean hadSelectRet) {
    if (multiTableIds.get() == null) {
      if (explain != null) {
        explain.getBuilder().append("inner join based on expression: table=").append(fromTable).append(", expression=").append(expression.toString()).append("\n");
      }
      long begin = System.nanoTime();
      AtomicBoolean didTableScan = new AtomicBoolean();
      ExpressionImpl.NextReturn ids = expression.next(this, pageSize / threadCount, explain,
          currOffset, countReturned, limit, offset, false, false, schemaRetryCount, didTableScan);
      if (ids != null && ids.getIds() != null && ids.getIds().length != 0) {
        hadSelectRet.set(true);
      }
      expressionCount.incrementAndGet();
      expressionDuration.set(System.nanoTime() - begin);
      return ids;
    }
    return null;
  }

  private ExpressionImpl.NextReturn prepareRightOuterJoin(int pageSize, String dbName, Explain explain, int schemaRetryCount,
                                                          AtomicReference<List<Object[][]>> multiTableIds, String joinRightFrom,
                                                          AtomicReference<ColumnImpl> leftColumn,
                                                          AtomicReference<ColumnImpl> rightColumn, int threadCount,
                                                          AtomicReference<String> leftFrom, AtomicReference<String> rightFrom,
                                                          AtomicBoolean hadSelectRet) {
    ExpressionImpl.NextReturn ids = null;
    if (multiTableIds.get() == null) {
      if (explain != null) {
        explain.getBuilder().append("Right outer join. Retrieving all records from table: table=").append(fromTable).append("\n");
      }
      AllRecordsExpressionImpl allExpression = new AllRecordsExpressionImpl();
      allExpression.setNextShard(expression.getNextShard());
      allExpression.setNextKey(expression.getNextKey());
      allExpression.setReplica(expression.getReplica());
      allExpression.setViewVersion(expression.getViewVersion());
      allExpression.setFromTable(joinRightFrom);
      allExpression.setClient(client);
      allExpression.setRecordCache(recordCache);
      allExpression.setDbName(dbName);
      allExpression.setColumns(getSelectColumns());
      allExpression.setOrderByExpressions(expression.getOrderByExpressions());
      AtomicBoolean didTableScan = new AtomicBoolean();
      ids = allExpression.next(this, pageSize / threadCount, explain, currOffset, countReturned, limit,
          offset, false, false, schemaRetryCount, didTableScan);
      if (ids != null && ids.getIds() != null && ids.getIds().length != 0) {
        hadSelectRet.set(true);
      }
      expression.setNextShard(allExpression.getNextShard());
      expression.setNextKey(allExpression.getNextKey());
    }
    leftFrom.set(joinRightFrom);
    rightFrom.set(fromTable);
    ColumnImpl column = leftColumn.get();
    leftColumn.set(rightColumn.get());
    rightColumn.set(column);
    return ids;
  }

  private ExpressionImpl.NextReturn prepareLeftOuterAndFullJoin(int pageSize, String dbName, Explain explain,
                                                                int schemaRetryCount, AtomicReference<List<Object[][]>> multiTableIds,
                                                                JoinType joinType, int threadCount, AtomicBoolean hadSelectRet) {
    ExpressionImpl.NextReturn ids = null;
    if (multiTableIds.get() == null) {
      if (explain != null) {
        if (joinType == JoinType.LEFT_OUTER) {
          explain.getBuilder().append("left outer join. Retrieving all records from table: table=").append(fromTable).append("\n");
        }
        else {
          explain.getBuilder().append("Full outer join. Retrieving all records from table: table=").append(fromTable).append("\n");
        }
      }
      AllRecordsExpressionImpl allExpression = new AllRecordsExpressionImpl();
      allExpression.setNextShard(expression.getNextShard());
      allExpression.setNextKey(expression.getNextKey());
      allExpression.setReplica(expression.getReplica());
      allExpression.setViewVersion(expression.getViewVersion());
      allExpression.setFromTable(fromTable);
      allExpression.setClient(client);
      allExpression.setRecordCache(recordCache);
      allExpression.setDbName(dbName);
      allExpression.setColumns(getSelectColumns());
      allExpression.setOrderByExpressions(expression.getOrderByExpressions());
      AtomicBoolean didTableScan = new AtomicBoolean();
      ids = allExpression.next(this, pageSize / threadCount, explain, currOffset, countReturned, limit,
          offset, false, false, schemaRetryCount, didTableScan);
      if (ids != null && ids.getIds() != null && ids.getIds().length != 0) {
        hadSelectRet.set(true);
      }
      expression.setNextShard(allExpression.getNextShard());
      expression.setNextKey(allExpression.getNextKey());
    }
    return ids;
  }

  private void getActualJoinExpression(ExpressionImpl joinExpression, AtomicReference<BinaryExpressionImpl> actualJoinExpression,
                                       List<ExpressionImpl> otherJoinExpressions) {
    if (!(joinExpression instanceof BinaryExpressionImpl)) {
      otherJoinExpressions.add(joinExpression);
      return;
    }
    BinaryExpressionImpl joinBinaryExpression = (BinaryExpressionImpl) joinExpression;

    if (joinBinaryExpression.getLeftExpression() instanceof ColumnImpl &&
        joinBinaryExpression.getRightExpression() instanceof ColumnImpl) {
      if (actualJoinExpression.get() == null) {
        actualJoinExpression.set(joinBinaryExpression);
      }
      else {
        otherJoinExpressions.add(joinExpression);
      }
      return;
    }

    if (joinBinaryExpression.getOperator() == BinaryExpression.Operator.AND) {
      getActualJoinExpression(joinBinaryExpression.getLeftExpression(), actualJoinExpression, otherJoinExpressions);
      getActualJoinExpression(joinBinaryExpression.getRightExpression(), actualJoinExpression, otherJoinExpressions);
    }
    else {
      otherJoinExpressions.add(joinExpression);
    }
  }

  class JoinReturn {
    private List<Object[][]> keys = new ArrayList<>();
  }

  private JoinReturn evaluateJoin(
      int pageSize,
      String dbName,
      AtomicReference<List<Object[][]>> multiTableIds,
      JoinType joinType,
      AtomicReference<ColumnImpl> leftColumn,
      AtomicReference<ColumnImpl> rightColumn,
      TableSchema leftTable, final TableSchema rightTable, AtomicInteger rightTableIndex,
      AtomicInteger leftTableIndex, Explain explain, boolean restrictToThisServer, StoredProcedureContextImpl procedureContext,
      int schemaRetryCount) {

    if (multiTableIds.get() != null) {
      Map<Integer, Object[][]> keys;
      List<Object[][]> newMultiTableIds = new ArrayList<>();

      final AtomicReference<Map.Entry<String, IndexSchema>> indexSchema = new AtomicReference<>();
      final TableSchema tableSchema = client.getCommon().getTables(dbName).get(rightTable.getName());

      getIndexSchema(rightColumn, indexSchema, tableSchema);

      int leftColumnIndex = leftTable.getFieldOffset(leftColumn.get().getColumnName());

      final List<ExpressionImpl.IdEntry> keysToRead = getKeysToRead(multiTableIds, leftTable, leftTableIndex, leftColumnIndex);
      if (explain != null) {
        explain.getBuilder().append("Evaluate join expression. Read join records: joinTable=").append(rightTable.getName()).append(", expression=").append(expression.toString()).append("\n");
      }

      keys = ExpressionImpl.readRecords(dbName, client, pageSize, forceSelectOnServer, tableSchema, keysToRead,
          new String[]{rightColumn.get().getColumnName()}, columns, recordCache, expression.getViewVersion(),
          restrictToThisServer, procedureContext, schemaRetryCount);
      if (!indexSchema.get().getValue().isPrimaryKeyGroup()) {
        keys = readRecordsForNonPrimaryIndex(pageSize, dbName, restrictToThisServer, procedureContext, schemaRetryCount,
            keys, tableSchema);
      }

      processReadRecordResutlsForJoin(multiTableIds, joinType, rightTableIndex, keys, newMultiTableIds);

      JoinReturn joinRet = new JoinReturn();
      joinRet.keys = newMultiTableIds;
      return joinRet;
    }

    return null;
  }

  private void processReadRecordResutlsForJoin(AtomicReference<List<Object[][]>> multiTableIds, JoinType joinType,
                                               AtomicInteger rightTableIndex, Map<Integer, Object[][]> keys,
                                               List<Object[][]> newMultiTableIds) {
    for (int i = 0; i < multiTableIds.get().size(); i++) {
      Object[][] id = multiTableIds.get().get(i);
      Object[][] rightIds = keys.get(i);

      doProcessReadRecordResultsForJoin(joinType, rightTableIndex, newMultiTableIds, id, rightIds);
    }
  }

  private void doProcessReadRecordResultsForJoin(JoinType joinType, AtomicInteger rightTableIndex,
                                                 List<Object[][]> newMultiTableIds, Object[][] id, Object[][] rightIds) {
    int sizeBefore = newMultiTableIds.size();
    if (rightIds == null) {
      if (joinType == JoinType.FULL || joinType == JoinType.LEFT_OUTER || joinType == JoinType.RIGHT_OUTER) {
        Object[][] newId = Arrays.copyOf(id, id.length);
        newId[rightTableIndex.get()] = null;
        newMultiTableIds.add(newId);
      }
    }
    else {
      for (Object[] rightId : rightIds) {
        Object[][] newId = Arrays.copyOf(id, id.length);
        newId[rightTableIndex.get()] = rightId;
        newMultiTableIds.add(newId);
      }
      if (sizeBefore == newMultiTableIds.size() && (joinType == JoinType.FULL || joinType == JoinType.LEFT_OUTER ||
          joinType == JoinType.RIGHT_OUTER)) {
        Object[][] newId = Arrays.copyOf(id, id.length);
        newId[rightTableIndex.get()] = null;
        newMultiTableIds.add(newId);
      }
    }
  }

  private Map<Integer, Object[][]> readRecordsForNonPrimaryIndex(int pageSize, String dbName, boolean restrictToThisServer,
                                                                 StoredProcedureContextImpl procedureContext, int schemaRetryCount,
                                                                 Map<Integer, Object[][]> keys, TableSchema tableSchema) {
    List<ExpressionImpl.IdEntry> keysToRead2 = new ArrayList<>();
    for (int i = 0; i < keys.size(); i++) {
      Object[][] key = keys.get(i);
      for (int j = 0; j < key.length; j++) {
        keysToRead2.add(new ExpressionImpl.IdEntry(i, key[j]));
      }
    }

    keys = ExpressionImpl.readRecords(dbName, client, pageSize, forceSelectOnServer, tableSchema, keysToRead2,
        tableSchema.getPrimaryKey(), columns, recordCache, expression.getViewVersion(), restrictToThisServer,
        procedureContext, schemaRetryCount);
    return keys;
  }

  private int[] getIndexSchema(AtomicReference<ColumnImpl> rightColumn,
                               AtomicReference<Map.Entry<String, IndexSchema>> indexSchema, TableSchema tableSchema) {
    int[] fieldOffsets = null;
    for (Map.Entry<String, IndexSchema> entry : tableSchema.getIndices().entrySet()) {
      String[] fields = entry.getValue().getFields();
      boolean shouldIndex = true;
      if (fields.length == 1 && !fields[0].equals(rightColumn.get().getColumnName())) {
        shouldIndex = false;
      }
      if (shouldIndex) {
        indexSchema.set(entry);
        String[] indexFields = indexSchema.get().getValue().getFields();
        fieldOffsets = new int[indexFields.length];
        for (int l = 0; l < indexFields.length; l++) {
          fieldOffsets[l] = tableSchema.getFieldOffset(indexFields[l]);
        }
        break;
      }
    }
    return fieldOffsets;
  }

  private List<ExpressionImpl.IdEntry> getKeysToRead(AtomicReference<List<Object[][]>> multiTableIds,
                                                     TableSchema leftTable, AtomicInteger leftTableIndex, int leftColumnIndex) {
    final List<ExpressionImpl.IdEntry> keysToRead = new ArrayList<>();
    for (int i = 0; i < multiTableIds.get().size(); i++) {
      Object[][] id = multiTableIds.get().get(i);
      if (id[leftTableIndex.get()] != null) {
        ExpressionImpl.CachedRecord cachedRecord = recordCache.get(leftTable.getName(), id[leftTableIndex.get()]);
        Record leftRecord = cachedRecord == null ? null : cachedRecord.getRecord();
        if (leftRecord != null) {
          keysToRead.add(new ExpressionImpl.IdEntry(i, new Object[]{leftRecord.getFields()[leftColumnIndex]}));
        }
      }
    }
    return keysToRead;
  }

  public List<ColumnImpl> getSelectColumns() {
    return selectColumns;
  }

  public Expression getWhereClause() {
    return expression;
  }

  static class Join {
    private JoinType type;
    private String rightFrom;
    private Expression expression;

    public Join(JoinType type, String rightFrom, Expression expression) {
      this.type = type;
      this.rightFrom = rightFrom;
      this.expression = expression;
    }

    public Join() {

    }

    public void serialize(DataOutputStream out) {
      try {
        out.writeInt(type.ordinal());
        out.writeUTF(rightFrom);
        ExpressionImpl.serializeExpression((ExpressionImpl) expression, out);
      }
      catch (IOException e) {
        throw new DatabaseException(e);
      }
    }

    public void deserialize(DataInputStream in) {
      try {
        int typeOrd = in.readInt();
        type = JoinType.values()[typeOrd];
        rightFrom = in.readUTF();
        expression = ExpressionImpl.deserializeExpression(in);
      }
      catch (IOException e) {
        throw new DatabaseException(e);
      }
    }
  }


  public void addJoinExpression(JoinType type, String rightFrom, Expression joinExpression) {
    Join join = new Join(type, rightFrom.toLowerCase(), joinExpression);
    this.joins.add(join);
  }

  private class ProcessFunction {
    private final String dbName;
    private final boolean restrictToThisServer;
    private final StoredProcedureContextImpl procedureContext;
    private final int schemaRetryCount;
    private boolean haveCounters;
    private boolean needToEvaluate;
    private final List<Counter> countersList;
    private final Map<String, SelectFunctionImpl> localAliases;

    public ProcessFunction(String dbName, boolean restrictToThisServer, StoredProcedureContextImpl procedureContext,
                           int schemaRetryCount, boolean haveCounters, boolean needToEvaluate, List<Counter> countersList,
                           Map<String, SelectFunctionImpl> localAliases) {
      this.dbName = dbName;
      this.restrictToThisServer = restrictToThisServer;
      this.procedureContext = procedureContext;
      this.schemaRetryCount = schemaRetryCount;
      this.haveCounters = haveCounters;
      this.needToEvaluate = needToEvaluate;
      this.countersList = countersList;
      this.localAliases = localAliases;
    }

    public boolean isHaveCounters() {
      return haveCounters;
    }

    public boolean isNeedToEvaluate() {
      return needToEvaluate;
    }

    public ProcessFunction invoke() throws IOException {
      for (SelectFunctionImpl function : localAliases.values()) {
        if (function.getName().equalsIgnoreCase("min") || function.getName().equalsIgnoreCase("max") ||
            function.getName().equalsIgnoreCase("avg") || function.getName().equalsIgnoreCase("sum")) {
          String columnName = ((Column) function.getParms().getExpressions().get(0)).getColumnName();
          Counter counter = new Counter();
          countersList.add(counter);
          String table = ((Column) function.getParms().getExpressions().get(0)).getTable().getName();
          if (table == null) {
            table = getFromTable();
          }
          counter.setTableName(table);
          counter.setColumnName(columnName);
          int columnOffset = client.getCommon().getTables(dbName).get(table).getFieldOffset(columnName);
          counter.setColumn(columnOffset);
          FieldSchema fieldSchema = client.getCommon().getTables(dbName).get(table).getFields().get(columnOffset);
          counter.setDataType(fieldSchema.getType());
          switch (fieldSchema.getType()) {
            case INTEGER:
            case BIGINT:
            case SMALLINT:
            case TINYINT:
              counter.setDestTypeToLong();
              break;
            case FLOAT:
            case DOUBLE:
            case NUMERIC:
            case DECIMAL:
            case REAL:
              counter.setDestTypeToDouble();
              break;
          }
          boolean indexed = false;
          SelectIndexSchema selectIndexSchema = new SelectIndexSchema(columnName, table, indexed).invoke();
          indexed = selectIndexSchema.isIndexed();
          IndexSchema selectedSchema = selectIndexSchema.getSelectedSchema();

          haveCounters = true;
          if (indexed && expression instanceof AllRecordsExpressionImpl) {
            ExpressionImpl.evaluateCounter(client.getCommon(), client, dbName, expression, selectedSchema, counter,
                function, restrictToThisServer, procedureContext, schemaRetryCount);
          }
          else {
            needToEvaluate = true;
          }
        }
      }
      return this;
    }

    private class SelectIndexSchema {
      private final String columnName;
      private final String table;
      private boolean indexed;
      private IndexSchema selectedSchema;

      public SelectIndexSchema(String columnName, String table, boolean indexed) {
        this.columnName = columnName;
        this.table = table;
        this.indexed = indexed;
      }

      public boolean isIndexed() {
        return indexed;
      }

      public IndexSchema getSelectedSchema() {
        return selectedSchema;
      }

      public SelectIndexSchema invoke() {
        selectedSchema = null;
        for (IndexSchema indexSchema : client.getCommon().getTables(dbName).get(table).getIndices().values()) {
          if (indexSchema.getFields()[0].equals(columnName)) {
            indexed = true;
            selectedSchema = indexSchema;
            break;
          }
        }
        return this;
      }
    }
  }

  private class PrepareJoinExpression {
    private final BinaryExpressionImpl joinBinaryExpression;
    private ExpressionImpl leftExpression;
    private ExpressionImpl rightExpression;
    private ExpressionImpl additionalJoinExpression;

    public PrepareJoinExpression(BinaryExpressionImpl joinBinaryExpression) {
      this.joinBinaryExpression = joinBinaryExpression;
    }

    public ExpressionImpl getLeftExpression() {
      return leftExpression;
    }

    public ExpressionImpl getRightExpression() {
      return rightExpression;
    }

    public ExpressionImpl getAdditionalJoinExpression() {
      return additionalJoinExpression;
    }

    public PrepareJoinExpression invoke() {
      if (joinBinaryExpression.getOperator() != BinaryExpression.Operator.AND) {
        throw new DatabaseException("Only 'and' operators are supported in join expression");
      }
      List<ExpressionImpl> otherJoinExpressions = new ArrayList<>();
      AtomicReference<BinaryExpressionImpl> actualJoinExpression = new AtomicReference<>();
      getActualJoinExpression(joinBinaryExpression, actualJoinExpression, otherJoinExpressions);
      leftExpression = actualJoinExpression.get().getLeftExpression();
      rightExpression = actualJoinExpression.get().getRightExpression();

      if (otherJoinExpressions.isEmpty()) {
        additionalJoinExpression = null;
      }
      else if (otherJoinExpressions.size() == 1) {
        additionalJoinExpression = otherJoinExpressions.get(0);
      }
      else {
        BinaryExpressionImpl andExpression = new BinaryExpressionImpl();
        BinaryExpressionImpl firstAndExpression = andExpression;
        andExpression.setOperator(BinaryExpression.Operator.AND);
        for (int i = 0; i < otherJoinExpressions.size(); i++) {
          ExpressionImpl localExpression = otherJoinExpressions.get(i);
          if (andExpression.getLeftExpression() == null) {
            andExpression.setLeftExpression(localExpression);
          }
          else {
            if (i < otherJoinExpressions.size() - 2) {
              BinaryExpressionImpl newAndExpression = new BinaryExpressionImpl();
              newAndExpression.setOperator(BinaryExpression.Operator.AND);
              newAndExpression.setLeftExpression(localExpression);
              andExpression.setRightExpression(newAndExpression);
              andExpression = newAndExpression;
            }
            else {
              andExpression.setRightExpression(localExpression);
            }
          }
        }
        additionalJoinExpression = firstAndExpression;
      }
      return this;
    }
  }

  private class CheckIfNeedToEvaluate {
    private final String dbName;
    private final boolean restrictToThisServer;
    private final StoredProcedureContextImpl procedureContext;
    private final int schemaRetryCount;
    private boolean haveCounters;
    private boolean needToEvaluate;
    private final List<Counter> countersList;

    public CheckIfNeedToEvaluate(String dbName, boolean restrictToThisServer, StoredProcedureContextImpl procedureContext,
                                 int schemaRetryCount, boolean haveCounters, boolean needToEvaluate, List<Counter> countersList) {
      this.dbName = dbName;
      this.restrictToThisServer = restrictToThisServer;
      this.procedureContext = procedureContext;
      this.schemaRetryCount = schemaRetryCount;
      this.haveCounters = haveCounters;
      this.needToEvaluate = needToEvaluate;
      this.countersList = countersList;
    }

    public boolean isHaveCounters() {
      return haveCounters;
    }

    public boolean isNeedToEvaluate() {
      return needToEvaluate;
    }

    public CheckIfNeedToEvaluate invoke() throws IOException {
      Map<String, SelectFunctionImpl> localAliases = getFunctionAliases();
      if (!isDistinct && isCountFunction && !(expression instanceof AllRecordsExpressionImpl)) {
        Counter counter = new Counter();
        countersList.add(counter);
        String table = getFromTable();

        counter.setTableName(table);
        counter.setColumnName("__all__");
        counter.setColumn(0);
        counter.setDataType(BIGINT);
        counter.setDestTypeToLong();
        needToEvaluate = true;
      }
      else {
        ProcessFunction processFunction = new ProcessFunction(dbName, restrictToThisServer, procedureContext,
            schemaRetryCount, haveCounters, needToEvaluate, countersList, localAliases).invoke();
        haveCounters = processFunction.isHaveCounters();
        needToEvaluate = processFunction.isNeedToEvaluate();
      }
      return this;
    }
  }
}
