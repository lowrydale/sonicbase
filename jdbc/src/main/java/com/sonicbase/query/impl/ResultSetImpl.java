package com.sonicbase.query.impl;

import com.sonicbase.client.ClientStatsHandler;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.client.SelectStatementHandler;
import com.sonicbase.common.*;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.procedure.RecordImpl;
import com.sonicbase.procedure.StoredProcedureContextImpl;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.query.ResultSet;
import com.sonicbase.schema.DataType;
import com.sonicbase.schema.FieldSchema;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.Offset;
import net.sf.jsqlparser.statement.select.Select;
import org.apache.commons.lang.exception.ExceptionUtils;

import java.io.*;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class ResultSetImpl implements ResultSet {
  private static final String UTF8_STR = "utf-8";
  private static final String LENGTH_STR = "length";
  private static final String COUNT_STR = "count";
  private String sqlToUse;
  private AtomicLong currOffset = new AtomicLong();
  private AtomicLong countReturned = new AtomicLong();
  private ComArray recordsResults;
  private StoredProcedureContextImpl procedureContext;
  private boolean restrictToThisServer;
  private Map<String, SelectFunctionImpl> functionAliases;
  private Map<String, ColumnImpl> aliases;
  private String[] tableNames;
  private Object[][][] retKeys;
  private SelectStatementHandler.SetOperation setOperation;
  private List<Map<String, String>> mapResults;
  private String[] describeStrs;
  private String dbName;
  private GroupByContext groupByContext;
  private List<Expression> groupByColumns;
  private Offset offset;
  private List<ColumnImpl> columns;
  private Set<SelectStatementImpl.DistinctRecord> uniqueRecords;
  private boolean isCount;
  private long count;
  private ExpressionImpl.RecordCache recordCache;
  private ParameterHandler parms;
  private ExpressionImpl.CachedRecord[][] readRecords;
  private ExpressionImpl.CachedRecord[][] lastReadRecords;
  private SelectStatementImpl selectStatement;
  private SelectContextImpl selectContext;
  private DatabaseClient databaseClient;
  private int currPos = -1;
  private long currTotalPos = -1;
  private Record[] currRecord;
  private Counter[] counters;
  private long pageSize = DatabaseClient.SELECT_PAGE_SIZE;
  private RecordImpl cachedRecordResultAsRecord;

  public ResultSetImpl(String[] describeStrs) {
    this.describeStrs = describeStrs;
  }

  public ResultSetImpl(List<Map<String, String>> mapResults) {
    this.mapResults = mapResults;
  }

  public ResultSetImpl(String dbName, DatabaseClient client, String[] tableNames, SelectStatementHandler.SetOperation setOperation,
                       Map<String, ColumnImpl> aliases, Map<String, SelectFunctionImpl> functionAliases,
                       boolean restrictToThisServer, StoredProcedureContextImpl procedureContext) {

    this.dbName = dbName;
    this.databaseClient = client;
    this.setOperation = setOperation;
    this.tableNames = tableNames;
    this.recordCache = new ExpressionImpl.RecordCache();
    this.aliases = aliases;
    this.functionAliases = functionAliases;
    this.restrictToThisServer = restrictToThisServer;
    this.procedureContext = procedureContext;
  }

  public ResultSetImpl(DatabaseClient databaseClient, ComArray records) {
    this.databaseClient = databaseClient;
    this.recordsResults = records;
  }

  public ExpressionImpl.CachedRecord[][] getReadRecordsAndSerializedRecords() {
    return readRecords;
  }

  public ExpressionImpl.RecordCache getRecordCache() {
    return recordCache;
  }

  public void setCount(int count) {
    this.count = count;
  }

  public Object getGroupByFunctionResults(String columnLabel, DataType.Type type) {
    if (groupByContext == null) {
      return null;
    }

    for (Map.Entry<String, ColumnImpl> alias : aliases.entrySet()) {
      if (columnLabel.equalsIgnoreCase(alias.getValue().getAlias())) {
        String function = alias.getValue().getFunction();
        if (function.equals(COUNT_STR) || function.equals("min") || function.equals("max") || function.equals("sum") ||
            function.equals("avg")) {
          GetGroupByFunctionResutsForSimpleMathFunction getGroupByFunctionResutsForSimpleMathFunction =
              new GetGroupByFunctionResutsForSimpleMathFunction(columnLabel, alias).invoke();
          if (getGroupByFunctionResutsForSimpleMathFunction.is()) {
            return convertCounterValueToType(type, function, getGroupByFunctionResutsForSimpleMathFunction.getCounter());
          }
        }
      }
    }
    return null;
  }

  private Object convertCounterValueToType(DataType.Type type, String function, GroupByContext.GroupCounter counter) {
    if (counter.getCounter().getLongCount() != null) {
      return convertLongCounterValueToType(type, function, counter);
    }
    else if (counter.getCounter().getDoubleCount() != null) {
      return convertDoubleCounterValueToType(type, function, counter);
    }
    return null;
  }

  private Object convertDoubleCounterValueToType(DataType.Type type, String function, GroupByContext.GroupCounter counter) {
    if (function.equals("sum")) {
      return type.getConverter().convert((double) counter.getCounter().getDoubleCount());
    }
    if (function.equals("max")) {
      return type.getConverter().convert((double) counter.getCounter().getMaxDouble());
    }
    if (function.equals("min")) {
      return type.getConverter().convert((double) counter.getCounter().getMinDouble());
    }
    if (function.equals("avg")) {
      return type.getConverter().convert((double) counter.getCounter().getAvgDouble());
    }
    if (function.equals(COUNT_STR)) {
      return type.getConverter().convert((long) counter.getCounter().getCount());
    }
    return null;
  }

  private Object convertLongCounterValueToType(DataType.Type type, String function, GroupByContext.GroupCounter counter) {
    if (function.equals("sum")) {
      return type.getConverter().convert((long) counter.getCounter().getLongCount());
    }
    if (function.equals("max")) {
      return type.getConverter().convert((long) counter.getCounter().getMaxLong());
    }
    if (function.equals("min")) {
      return type.getConverter().convert((long) counter.getCounter().getMinLong());
    }
    if (function.equals("avg")) {
      return type.getConverter().convert((double) counter.getCounter().getAvgLong());
    }
    if (function.equals(COUNT_STR)) {
      return type.getConverter().convert((long) counter.getCounter().getCount());
    }
    return null;
  }

  public void setPageSize(int pageSize) {
    this.pageSize = pageSize;
  }

  public void forceSelectOnServer() {
    selectStatement.forceSelectOnServer();
  }

  public String[] getDescribeStrs() {
    return describeStrs;
  }

  public long getViewVersion() {
    return selectStatement.getViewVersion();
  }

  public int getCurrShard() {
    return selectStatement.getCurrShard();
  }

  public int getLastShard() {
    return selectStatement.getLastShard();
  }

  public boolean isCurrPartitions() {
    return selectStatement.isCurrPartitions();
  }

  public void setRetKeys(Object[][][] retKeys) {
    this.retKeys = retKeys;
  }

  public void setRecords(ExpressionImpl.CachedRecord[][] records) {
    this.readRecords = records;
  }

  public String[] getTableNames() {
    return tableNames;
  }

  public static class MultiTableRecordList {
    private String[] tableNames;
    private long[][] ids;

    public String[] getTableNames() {
      return tableNames;
    }

    public void setTableNames(String[] tableNames) {
      this.tableNames = tableNames;
    }

    public long[][] getIds() {
      return ids;
    }

    public void setIds(long[][] ids) {
      this.ids = ids;
    }
  }

  public ResultSetImpl(
      String dbName,
      DatabaseClient client, SelectStatementImpl selectStatement, long count, boolean restrictToThisServer,
      StoredProcedureContextImpl procedureContext) {
    this.dbName = dbName;
    this.databaseClient = client;
    this.selectStatement = selectStatement;
    this.count = count;
    this.isCount = true;
    this.aliases = selectStatement.getAliases();
    this.functionAliases = selectStatement.getFunctionAliases();
    this.tableNames = selectStatement.getTableNames();
    this.restrictToThisServer = restrictToThisServer;
    this.procedureContext = procedureContext;
  }

  public ResultSetImpl(
      String dbName,
      String sqlToUse,
      DatabaseClient databaseClient, SelectStatementImpl selectStatement,
      ParameterHandler parms, Set<SelectStatementImpl.DistinctRecord> uniqueRecords, SelectContextImpl selectContext,
      Record[] retRecords,
      List<ColumnImpl> columns,
      String indexUsed, Counter[] counters, Limit limit, Offset offset, AtomicLong currOffset, AtomicLong countReturned,
      List<Expression> groupByColumns, GroupByContext groupContext, boolean restrictToThisServer,
      StoredProcedureContextImpl procedureContext) {
    this.dbName = dbName;
    this.sqlToUse = sqlToUse;
    this.databaseClient = databaseClient;
    this.selectStatement = selectStatement;
    this.aliases = selectStatement.getAliases();
    this.functionAliases = selectStatement.getFunctionAliases();
    this.tableNames = selectStatement.getTableNames();
    this.parms = parms;
    this.uniqueRecords = uniqueRecords;
    this.selectContext = selectContext;
    this.columns = columns;
    this.recordCache = selectContext.getRecordCache();
    int schemaRetryCount = 0;
    this.readRecords = readRecords(new ExpressionImpl.NextReturn(selectContext.getTableNames(),
        selectContext.getCurrKeys()), schemaRetryCount);
    this.counters = counters;
    this.offset = offset;
    this.currOffset = currOffset;
    this.countReturned = countReturned;
    this.groupByColumns = groupByColumns;
    this.groupByContext = groupContext;
    this.pageSize = selectStatement.getPageSize();
    this.restrictToThisServer = restrictToThisServer;
    this.procedureContext = procedureContext;

    List<OrderByExpressionImpl> orderByExpressions = selectStatement.getOrderByExpressions();
    if (!orderByExpressions.isEmpty() &&
        (orderByExpressions.size() > 1 || (selectContext.getSortWithIndex() != null && !selectContext.getSortWithIndex())) &&
        selectContext.getCurrKeys() != null) {
      sortResults(dbName, databaseClient.getCommon(), readRecords, selectContext.getTableNames(),
          selectStatement.getOrderByExpressions());
    }
  }

  public static void sortResults(
      String dbName,
      DatabaseCommon common,
      ExpressionImpl.CachedRecord[][] records,
      final String[] tableNames, List<OrderByExpressionImpl> orderByExpressions) {
    if (!orderByExpressions.isEmpty()) {
      final int[] fieldOffsets = new int[orderByExpressions.size()];
      final boolean[] ascendingFlags = new boolean[orderByExpressions.size()];
      final Comparator[] comparators = new Comparator[orderByExpressions.size()];
      final int[] tableOffsets = new int[orderByExpressions.size()];
      for (int i = 0; i < orderByExpressions.size(); i++) {
        String tableName = orderByExpressions.get(i).getTableName();

        getTableOffsets(tableNames, tableOffsets, i, tableName);

        if (tableName == null) {
          tableName = tableNames[0];
        }
        TableSchema tableSchema = common.getTables(dbName).get(tableName);
        fieldOffsets[i] = tableSchema.getFieldOffset(orderByExpressions.get(i).getColumnName());
        ascendingFlags[i] = orderByExpressions.get(i).isAscending();
        FieldSchema fieldSchema = tableSchema.getFields().get(fieldOffsets[i]);
        comparators[i] = fieldSchema.getType().getComparator();
      }

      Arrays.sort(records, (o1, o2) -> getSortComparator(fieldOffsets, ascendingFlags, comparators, tableOffsets, o1, o2));
    }
  }

  private static void getTableOffsets(String[] tableNames, int[] tableOffsets, int i, String tableName) {
    for (int j = 0; j < tableNames.length; j++) {
      if (tableName == null) {
        tableOffsets[i] = 0;
      }
      else {
        if (tableName.equals(tableNames[j])) {
          tableOffsets[i] = j;
          break;
        }
      }
    }
  }

  private static int getSortComparator(int[] fieldOffsets, boolean[] ascendingFlags, Comparator[] comparators,
                                       int[] tableOffsets, ExpressionImpl.CachedRecord[] o1, ExpressionImpl.CachedRecord[] o2) {
    for (int i = 0; i < fieldOffsets.length; i++) {
      if (handleNullsIsSortCompare(fieldOffsets[i], o1[tableOffsets[i]], o2[tableOffsets[i]])) {
        continue;
      }
      Integer x = handleNullsInSortComparator(ascendingFlags, fieldOffsets[i], o1[tableOffsets[i]], o2[tableOffsets[i]], i);
      if (x != null) {
        return x;
      }
      int value = comparators[i].compare(o1[tableOffsets[i]].getRecord().getFields()[fieldOffsets[i]],
          o2[tableOffsets[i]].getRecord().getFields()[fieldOffsets[i]]);
      if (value < 0) {
        return -1 * (ascendingFlags[i] ? 1 : -1);
      }
      if (value > 0) {
        return 1 * (ascendingFlags[i] ? 1 : -1);
      }
    }
    return 0;
  }

  private static boolean handleNullsIsSortCompare(int fieldOffset, ExpressionImpl.CachedRecord cachedRecord,
                                                  ExpressionImpl.CachedRecord cachedRecord1) {
    if (cachedRecord == null && cachedRecord1 == null) {
      return true;
    }
    return (cachedRecord == null || cachedRecord.getRecord().getFields()[fieldOffset] == null) &&
        (cachedRecord1 == null || cachedRecord1.getRecord().getFields()[fieldOffset] == null);
  }

  private static Integer handleNullsInSortComparator(boolean[] ascendingFlags, int fieldOffset,
                                                     ExpressionImpl.CachedRecord cachedRecord,
                                                     ExpressionImpl.CachedRecord cachedRecord1, int i) {
    if (cachedRecord == null) {
      return -1 * (ascendingFlags[i] ? 1 : -1);
    }
    if (cachedRecord1 == null) {
      return 1 * (ascendingFlags[i] ? 1 : -1);
    }
    if (cachedRecord.getRecord().getFields()[fieldOffset] == null) {
      return 1 * (ascendingFlags[i] ? 1 : -1);
    }
    if (cachedRecord1.getRecord().getFields()[fieldOffset] == null) {
      return -1 * (ascendingFlags[i] ? 1 : -1);
    }
    return null;
  }

  public boolean isAfterLast() {
    return currRecord == null;
  }

  public boolean next() {
    currPos++;

    Boolean x1 = abortEarlyAsNeeded();
    if (x1 != null) {
      return x1;
    }

    currTotalPos++;
    if (setOperation == null && selectContext == null) {
      return false;
    }

    Boolean first = getResultsForCounters();
    if (first != null) {
      return first;
    }

    if (abortOnEmptyRecords()) {
      return false;
    }

    if (selectStatement != null && groupByColumns != null) {
      Object[] lastFields = new Object[groupByColumns.size()];
      Comparator[] comparators = new Comparator[groupByColumns.size()];
      String[][] actualColumns = new String[groupByColumns.size()][];
      for (int i = 0; i < lastFields.length; i++) {
        String column = ((Column) groupByColumns.get(i)).getColumnName();
        String fromTable = selectStatement.getFromTable();
        TableSchema tableSchema = databaseClient.getCommon().getTables(dbName).get(fromTable);
        DataType.Type type = tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType();
        comparators[i] = type.getComparator();

        actualColumns[i] = getActualColumn(column);
        lastFields[i] = getField(actualColumns[i], column);
      }

      Boolean x = getMoreResultsAsNeededForGroupBy(lastFields, comparators, actualColumns);
      if (x != null) {
        return x;
      }

      skipResultsUpToOffsetForGroupBy();
    }

    getMoreResultsAsNeeded();

    return (setOperation == null || (retKeys != null && retKeys.length != 0)) &&
        (setOperation != null || selectContext.getCurrKeys() != null);
  }

  private boolean abortOnEmptyRecords() {
    return ((setOperation != null && retKeys == null) || (setOperation == null && selectContext.getCurrKeys() == null))
        && (readRecords == null || readRecords.length == 0);
  }

  private Boolean abortEarlyAsNeeded() {
    if (describeStrs != null) {
      return currPos <= describeStrs.length - 1;
    }
    if (recordsResults != null) {
      if (currPos > recordsResults.getArray().size() - 1) {
        cachedRecordResultAsRecord = null;
        return false;
      }
      ComObject cobj = ((ComObject)recordsResults.getArray().get(currPos));
      cachedRecordResultAsRecord = new RecordImpl(databaseClient.getCommon(), cobj);
      return true;
    }
    if (mapResults != null) {
      return currPos <= mapResults.size() - 1;
    }
    if (isCount) {
      return currPos == 0;
    }
    return null;
  }

  private void getMoreResultsAsNeeded() {
    if ((setOperation != null && (retKeys.length == 0 || currPos >= retKeys.length)) ||
        (setOperation == null && (selectContext.getCurrKeys().length == 0 || currPos >= selectContext.getCurrKeys().length))) {
      int schemaRetryCount = 0;
      while (true) {
        try {
          getMoreResults(schemaRetryCount);
          break;
        }
        catch (SchemaOutOfSyncException e) {
          schemaRetryCount++;
        }
        catch (Exception e) {
          throw new DatabaseException(e);
        }
      }
    }
  }

  private Boolean getResultsForCounters() {
    if (counters != null || (selectStatement != null && (isCount && selectStatement.isDistinct()))) {

      boolean requireEvaluation = false;
      if (selectStatement != null) {
        for (SelectFunctionImpl function : selectStatement.getFunctionAliases().values()) {
          if (!(function.getName().equalsIgnoreCase("min") || function.getName().equalsIgnoreCase("max"))) {
            requireEvaluation = true;
          }
        }
      }
      return doGetMoreResultsForCounters(requireEvaluation);
    }
    return null;
  }

  private Boolean doGetMoreResultsForCounters(boolean requireEvaluation) {
    if (requireEvaluation || (counters == null || !(selectStatement.getExpression() instanceof AllRecordsExpressionImpl))) {
      boolean first = false;
      if (currPos == 0) {
        first = true;
        int schemaRetryCount = 0;
        doGetMoreResultsForCounters(schemaRetryCount);
      }
      return first;
    }
    else {
      return currPos == 0;
    }
  }

  private void doGetMoreResultsForCounters(int schemaRetryCount) {
    while (true) {
      try {
        getMoreResults(schemaRetryCount);
        if (selectContext.getCurrKeys() == null) {
          break;
        }
      }
      catch (SchemaOutOfSyncException e) {
        schemaRetryCount++;
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }
  }

  private void skipResultsUpToOffsetForGroupBy() {
    if (offset != null) {
      while (currTotalPos < offset.getOffset() - 1) {
        if ((selectContext.getCurrKeys().length == 0 || currPos >= selectContext.getCurrKeys().length)) {
          doSkipResultsUpToOffsetForGroupBy();
        }
        currPos++;
        currTotalPos++;
      }
    }
  }

  private void doSkipResultsUpToOffsetForGroupBy() {
    int schemaRetryCount = 0;
    while (true) {
      try {
        getMoreResults(schemaRetryCount);
        break;
      }
      catch (SchemaOutOfSyncException e) {
        schemaRetryCount++;
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }
  }

  private Boolean getMoreResultsAsNeededForGroupBy(Object[] lastFields, Comparator[] comparators, String[][] actualColumns) {
    while (true) {
      currPos++;
      if ((selectContext.getCurrKeys().length == 0 || currPos >= selectContext.getCurrKeys().length)) {
        int schemaRetryCount = 0;
        doGetMoreResultsAsNeededForGroupBy(schemaRetryCount);
      }
      if (decrementCurrPosAsNeeded()) {
        return true;
      }
      boolean nonNull = true;
      for (int i = 0; i < lastFields.length; i++) {
        if (lastFields[i] == null) {
          nonNull = false;
        }
      }
      Boolean x = processResultOfGetMoreResultsForGroupBy(lastFields, comparators, actualColumns, nonNull);
      if (x != null) {
        return x;
      }
    }
  }

  private void doGetMoreResultsAsNeededForGroupBy(int schemaRetryCount) {
    while (true) {
      try {
        getMoreResults(schemaRetryCount);
        break;
      }
      catch (SchemaOutOfSyncException e) {
        schemaRetryCount++;
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }
  }

  private boolean decrementCurrPosAsNeeded() {
    if (selectContext.getCurrKeys() == null && (readRecords == null || readRecords.length == 0)) {
      currPos--;
      return true;
    }
    if ((selectContext.getCurrKeys().length == 0 || currPos >= selectContext.getCurrKeys().length)) {
      currPos--;
      return true;
    }
    return false;
  }

  private Boolean processResultOfGetMoreResultsForGroupBy(Object[] lastFields, Comparator[] comparators, String[][] actualColumns, boolean nonNull) {
    boolean hasNull = false;
    Object[] lastTmp = new Object[lastFields.length];
    for (int i = 0; i < lastFields.length; i++) {
      if (lastFields[i] == null) {
        return false;
      }
      Object field = getField(actualColumns[i], actualColumns[i][1]);
      if (decrementCurrPosAsNeeded(lastFields, i, nonNull, lastFields[i], comparators, field)) {
        return false;
      }
      lastFields[i] = field;
      lastTmp[i] = field;
      if (field == null) {
        hasNull = true;
      }
    }
    if (!hasNull) {
      boolean notNull = false;
      for (int i = 0; i < lastFields.length; i++) {
        if (lastFields[i] != null) {
          notNull = true;
        }
      }
      if (!notNull) {
        currPos--;
        return false;
      }
    }
    return null;
  }

  private boolean decrementCurrPosAsNeeded(Object[] lastFields, int i, boolean nonNull, Object lastField,
                                           Comparator[] comparators, Object field) {
    if (nonNull && 0 != comparators[i].compare(field, lastFields[i]) && field != null && lastFields[i] != null) {
      currPos--;
      return true;
    }
    if (lastField != null && field == null) {
      currPos--;
      return true;
    }
    return false;
  }


  private Record doReadRecord(Object[] key, String tableName, int schemaRetryCount)  {

    return ExpressionImpl.doReadRecord(dbName, databaseClient, selectStatement.isForceSelectOnServer(), parms,
        selectStatement.getWhereClause(), recordCache, key, tableName, columns,
        ((ExpressionImpl) selectStatement.getWhereClause()).getViewVersion(),
        selectContext.isRestrictToThisServer(), selectContext.getProcedureContext(), schemaRetryCount);
  }


  public boolean isBeforeFirst() {
    return currPos == -1;
  }

  public boolean isFirst() {

    if (selectContext == null) {
      return false;
    }
    if (selectContext.getCurrKeys() == null) {
      return false;
    }

    return currPos == 0 && selectContext.getCurrKeys().length > 0;
  }

  public boolean isLast() {
    if (describeStrs != null) {
      return currPos >= describeStrs.length - 1;
    }
    if (mapResults != null) {
      return currPos >= mapResults.size() - 1;
    }
    if (selectContext == null) {
      return true;
    }
    if (selectContext.getCurrKeys() == null) {
      return true;
    }
    return currPos == selectContext.getCurrKeys().length - 1 && !isBeforeFirst();
  }

  public boolean last() {
    if (selectContext.getNextKey() != null) {
      return false;
    }
    currPos = selectContext.getCurrKeys().length - 1;
    return !isBeforeFirst();
  }

  public void close() {

    if (selectStatement != null && selectStatement.isServerSelect()) {
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.DB_NAME, dbName);
      cobj.put(ComObject.Tag.SCHEMA_VERSION, databaseClient.getCommon().getSchemaVersion());
      cobj.put(ComObject.Tag.METHOD, "ReadManager:serverSelectDelete");
      cobj.put(ComObject.Tag.ID, selectStatement.getServerSelectResultSetId());

      databaseClient.send(null, selectStatement.getServerSelectShardNumber(),
          selectStatement.getServerSelectReplicaNumber(), cobj, DatabaseClient.Replica.SPECIFIED);
    }
  }

  private Object doGetField(int i, String[] label, Integer offset, String columnLabel) {
    if (currPos < 0) {
      if (lastReadRecords == null) {
        return null;
      }
      if (lastReadRecords[lastReadRecords.length + currPos][i] == null) {
        return null;
      }
      return lastReadRecords[lastReadRecords.length + currPos][i].getRecord().getFields()[offset];
    }
    if (readRecords[currPos][i] == null) {
      return null;
    }
    FieldInfo fieldInfo = new FieldInfo();
    fieldInfo.labelName = label[1];
    fieldInfo.tableOffset = i;
    fieldInfo.fieldOffset = offset;
    fieldInfos.put(columnLabel, fieldInfo);
    return readRecords[currPos][i].getRecord().getFields()[offset];
  }

  private Object getField(String[] label, String columnLabel) {
    label[1] = DatabaseClient.toLower(label[1]);
    if (label[0] != null) {
      label[0] = DatabaseClient.toLower(label[0]);
    }
    if (label[0] != null) {
      Object ret = doGetFieldFromSpecifiedTable(label, columnLabel);
      if (ret != null) {
        return ret;
      }
    }
    Object ret = getGetFieldFromNonSpecifiedTable(label, columnLabel);
    if (ret != null) {
      return ret;
    }
    return null;
  }

  private Object getGetFieldFromNonSpecifiedTable(String[] label, String columnLabel) {
    for (int i = 0; i < tableNames.length; i++) {
      TableSchema tableSchema = databaseClient.getCommon().getTables(dbName).get(tableNames[i]);
      Integer localOffset = tableSchema.getFieldOffset(label[1]);
      if (localOffset != null) {
        Object ret = doGetField(i, label, localOffset, columnLabel);
        if (ret != null) {
          return ret;
        }
      }
    }
    return null;
  }

  private Object doGetFieldFromSpecifiedTable(String[] label, String columnLabel) {
    for (int i = 0; i < selectContext.getTableNames().length; i++) {
      if (label[0].equals(selectContext.getTableNames()[i])) {
        TableSchema tableSchema = databaseClient.getCommon().getTables(dbName).get(label[0]);
        Integer localOffset = tableSchema.getFieldOffset(label[1]);
        if (localOffset != null) {
          Object ret = doGetField(i, label, localOffset, columnLabel);
          if (ret != null) {
            return ret;
          }
        }
      }
    }
    return null;
  }

  public String getString(String columnLabel) {
    if (recordsResults != null) {
      return cachedRecordResultAsRecord.getString(columnLabel);
    }
    if (mapResults != null) {
      return mapResults.get(currPos).get(columnLabel);
    }
    String[] actualColumn;
    SelectFunctionImpl function = functionAliases.get(DatabaseClient.toLower(columnLabel));
    if (function != null) {
      if (this.groupByContext == null && (function.getName().equalsIgnoreCase(COUNT_STR) ||
          function.getName().equalsIgnoreCase("min") ||
          function.getName().equalsIgnoreCase("max") ||
          function.getName().equalsIgnoreCase("sum") ||
          function.getName().equalsIgnoreCase("avg"))) {
        Object obj = this.getCounterValue(function);
        if (obj instanceof Long) {
          return String.valueOf((Long) obj);
        }

        if (obj instanceof Double) {
          return String.valueOf((Double) obj);
        }
      }

      actualColumn = getActualColumn(columnLabel);
    }
    else {
      actualColumn = getActualColumn(columnLabel);
    }
    Object ret = getField(actualColumn, columnLabel);

    String retString = getString(ret);

    Object obj = getFunctionValue(function, retString);
    if (obj != null) {
      return (String) DataType.getStringConverter().convert(obj);
    }

    retString = applyFunctionToGetString(function, retString);

    return retString;
  }

  private String applyFunctionToGetString(SelectFunctionImpl function, String retString) {
    if (function != null) {
      if (function.getName().equals("upper")) {
        retString = retString.toUpperCase();
      }
      else if (function.getName().equals("lower")) {
        retString = retString.toLowerCase();
      }
      else if (function.getName().equals("substring")) {
        ExpressionList list = function.getParms();
        int pos1 = (int) ((LongValue) list.getExpressions().get(1)).getValue();
        if (list.getExpressions().size() > 2) {
          int pos2 = (int) ((LongValue) list.getExpressions().get(2)).getValue();
          retString = retString.substring(pos1, pos2);
        }
        else {
          retString = retString.substring(pos1);
        }
      }
    }
    return retString;
  }

  private String getString(Object ret) {
    return (String) DataType.getStringConverter().convert(ret);
  }

  private String[] getActualColumn(String columnLabel) {
    ColumnImpl column = aliases.get(columnLabel);
    if (column != null) {
      return new String[]{column.getTableName(), column.getColumnName()};
    }
    return new String[]{null, columnLabel};
  }

  private String[] getActualColumn(int columnIndex) {
    List<ColumnImpl> loalColumns = selectStatement.getSelectColumns();
    ColumnImpl columnObj = loalColumns.get(columnIndex - 1);
    String columnName = columnObj.getColumnName();
    ColumnImpl column = aliases.get(columnName);
    if (column != null) {
      return new String[]{column.getTableName(), column.getColumnName()};
    }
    return new String[]{null, columnName};
  }

  public Boolean getBoolean(String columnLabel) {
    if (recordsResults != null) {
      return cachedRecordResultAsRecord.getBoolean(columnLabel);
    }
    FieldInfo fieldInfo = fieldInfos.get(columnLabel);
    ExpressionImpl.CachedRecord cachedRecord = getDirectFieldValue(columnLabel);
    if (cachedRecord != null) {
      Object obj = cachedRecord.getRecord().getFields()[fieldInfo.fieldOffset];
      return getBoolean(obj);
    }

    String[] actualColumn = getActualColumn(columnLabel);
    Object ret = getField(actualColumn, columnLabel);

    return getBoolean(ret);
  }

  private Boolean getBoolean(Object ret) {
    return (Boolean) DataType.getBooleanConverter().convert(ret);
  }

  public Byte getByte(String columnLabel) {
    if (recordsResults != null) {
      return cachedRecordResultAsRecord.getByte(columnLabel);
    }

    FieldInfo fieldInfo = fieldInfos.get(columnLabel);
    ExpressionImpl.CachedRecord cachedRecord = getDirectFieldValue(columnLabel);
    if (cachedRecord != null) {
      Object obj = cachedRecord.getRecord().getFields()[fieldInfo.fieldOffset];
      return getByte(obj, fieldInfo.function);
    }

    String[] actualColumn = null;
    SelectFunctionImpl function = functionAliases.get(DatabaseClient.toLower(columnLabel));
    actualColumn = getActualColumn(columnLabel, function);
    Object ret = getField(actualColumn, columnLabel);

    fieldInfo = fieldInfos.get(columnLabel);
    if (fieldInfo != null) {
      fieldInfo.function = function;
    }

    return getByte(ret, function);
  }

  private Byte getByte(Object ret, SelectFunctionImpl function) {
    if (function != null) {
      String retString = getRetString(ret);
      String functionName = function.getName();
      if (functionName.equals(LENGTH_STR)) {
        if (retString == null) {
          return 0;
        }
        return (byte) retString.length();
      }
    }
    return (Byte) DataType.getByteConverter().convert(ret);
  }

  private String getRetString(Object ret) {
    String retString = null;
    if (ret instanceof byte[]) {
      try {
        retString = new String((byte[]) ret, UTF8_STR);
      }
      catch (UnsupportedEncodingException e) {
        throw new DatabaseException(e);
      }
    }
    return retString;
  }

  public Short getShort(String columnLabel) {
    if (recordsResults != null) {
      return cachedRecordResultAsRecord.getShort(columnLabel);
    }

    FieldInfo fieldInfo = fieldInfos.get(columnLabel);
    ExpressionImpl.CachedRecord cachedRecord = getDirectFieldValue(columnLabel);
    if (cachedRecord != null) {
      Object obj = cachedRecord.getRecord().getFields()[fieldInfo.fieldOffset];
      Short retString1 = getShort(obj, fieldInfo.function);
      if (retString1 != null) {
        return retString1;
      }
      return (Short) obj;
    }

    String[] actualColumn = null;
    SelectFunctionImpl function = functionAliases.get(DatabaseClient.toLower(columnLabel));
    actualColumn = getActualColumn(columnLabel, function);

    Object ret = getField(actualColumn, columnLabel);

    fieldInfo = fieldInfos.get(columnLabel);
    if (fieldInfo != null) {
      fieldInfo.function = function;
    }

    Short retString1 = getShort(ret, function);
    if (retString1 != null) {
      return retString1;
    }
    return (Short) ret;
  }

  private String[] getActualColumn(String columnLabel, SelectFunctionImpl function) {
    String[] actualColumn;
    if (function != null) {
      if (function.getName().equalsIgnoreCase(LENGTH_STR)) {
        actualColumn = getActualColumn(columnLabel);
      }
      else {
        actualColumn = new String[]{null, DatabaseClient.toLower(columnLabel)};
      }
    }
    else {
      actualColumn = getActualColumn(columnLabel);
    }
    return actualColumn;
  }

  private Short getShort(Object ret, SelectFunctionImpl function) {
    String retString = getRetString(ret);
    if (function != null) {
      String functionName = function.getName();
      if (functionName.equals(LENGTH_STR)) {
        if (retString == null) {
          return 0;
        }
        return (short) retString.length();
      }
    }
    return (Short) DataType.getShortConverter().convert(ret);
  }

  public Integer getInt(String columnLabel) {
    if (recordsResults != null) {
      return cachedRecordResultAsRecord.getInt(columnLabel);
    }

    FieldInfo fieldInfo = fieldInfos.get(columnLabel);
    ExpressionImpl.CachedRecord cachedRecord = getDirectFieldValue(columnLabel);
    if (cachedRecord != null) {
      Object obj = cachedRecord.getRecord().getFields()[fieldInfo.fieldOffset];
      return getInt(obj, fieldInfo.function);
    }

    if (isMatchingAlias(columnLabel) && isCount) {
      return (int) count;
    }
    Object ret = getGroupByFunctionResults(columnLabel, DataType.Type.INTEGER);
    if (ret != null) {
      return (Integer) ret;
    }

    String[] actualColumn = null;
    SelectFunctionImpl function = functionAliases.get(DatabaseClient.toLower(columnLabel));
    actualColumn = getActualColumn(columnLabel, function);

    Object retObj = getField(actualColumn, columnLabel);

    fieldInfo = fieldInfos.get(columnLabel);
    if (fieldInfo != null) {
      fieldInfo.function = function;
    }

    return getInt(retObj, function);
  }

  private boolean isMatchingAlias(String columnLabel) {
    boolean matchingAlias = false;
    if (aliases != null) {
      for (String alias : aliases.keySet()) {
        if (alias.equals(columnLabel)) {
          matchingAlias = true;
        }
      }
    }
    return matchingAlias;
  }

  private Integer getInt(Object ret, SelectFunctionImpl function) {
    String retString = getRetString(ret);
    Object obj = getFunctionValue(function, retString);
    if (obj != null) {
      return (Integer) DataType.getIntConverter().convert(obj);
    }
    return (Integer) DataType.getIntConverter().convert(ret);
  }

  private Object getFunctionValue(SelectFunctionImpl function, String retString) {
    if (function != null) {
      if (retString != null && function.getName().equals(LENGTH_STR)) {
        return retString.length();
      }
      else if (function.getName().equalsIgnoreCase(COUNT_STR) ||
          function.getName().equalsIgnoreCase("min") ||
          function.getName().equalsIgnoreCase("max") ||
          function.getName().equalsIgnoreCase("sum") ||
          function.getName().equalsIgnoreCase("avg")) {
        return getCounterValue(function);
      }
    }
    return null;
  }

  private class FieldInfo {
    private String labelName;
    private int tableOffset;
    private int fieldOffset;
    private SelectFunctionImpl function;
  }

  private Object2ObjectOpenHashMap<String, FieldInfo> fieldInfos = new Object2ObjectOpenHashMap<>();

  private boolean canShortCircuitFieldLookup(FieldInfo fieldInfo) {
    return fieldInfo != null && currPos >= 0 && !isCount && groupByContext == null && !functionAliases.containsKey(fieldInfo.labelName);
  }

  public Long getLong(String columnLabel) {
    if (recordsResults != null) {
      return cachedRecordResultAsRecord.getLong(columnLabel);
    }

    FieldInfo fieldInfo = fieldInfos.get(columnLabel);
    ExpressionImpl.CachedRecord cachedRecord = getDirectFieldValue(columnLabel);
    if (cachedRecord != null) {
      Object obj = cachedRecord.getRecord().getFields()[fieldInfo.fieldOffset];
      return getLong(obj, fieldInfo.function);
    }
    if (isMatchingAlias(columnLabel) && isCount) {
      SelectFunctionImpl function = functionAliases.get(DatabaseClient.toLower(columnLabel));
      if (function == null) {
        return count;
      }

      String[] actualColumn = getActualColumn("__all__");
      Object ret = getField(actualColumn, columnLabel);
      return getLong(ret, function);
    }
    Object ret = getGroupByFunctionResults(columnLabel, DataType.Type.BIGINT);
    if (ret != null) {
      return (Long) ret;
    }

    String[] actualColumn = null;
    SelectFunctionImpl function = functionAliases.get(DatabaseClient.toLower(columnLabel));
    actualColumn = getActualColumn(columnLabel, function);
    Object retObj = getField(actualColumn, columnLabel);

    fieldInfo = fieldInfos.get(columnLabel);
    if (fieldInfo != null) {
      fieldInfo.function = function;
    }

    return getLong(retObj, function);
  }

  private Long getLong(Object ret, SelectFunctionImpl function) {
    String retString = getRetString(ret);
    Object obj = getFunctionValue(function, retString);
    if (obj != null) {
      return (Long) DataType.getLongConverter().convert(obj);
    }

    return (Long) DataType.getLongConverter().convert(ret);
  }

  private Object getCounterValue(SelectFunctionImpl function) {
    if (function.getName().equalsIgnoreCase(COUNT_STR)) {
      if (counters != null) {
        for (Counter counter : counters) {
          if (counter.getLongCount() != null) {
            return counter.getCount();
          }
        }
      }
    }
    else if (function.getName().equalsIgnoreCase("min")) {
      return getCounterValueMin(function);
    }
    else if (function.getName().equalsIgnoreCase("max")) {
      return getCounterValueMax(function);
    }
    else if (function.getName().equalsIgnoreCase("avg")) {
      return getCounterValueAvg(function);
    }
    else if (function.getName().equalsIgnoreCase("sum")) {
      return getCounterValueSum(function);
    }
    return 0L;
  }

  private Object getCounterValueSum(SelectFunctionImpl function) {
    String columnName = ((Column) function.getParms().getExpressions().get(0)).getColumnName();
    if (counters != null) {
      for (Counter counter : counters) {
        if ((counter.getColumnName().equals(columnName))) {
          if (counter.getLongCount() != null) {
            return counter.getLongCount();
          }
          if (counter.getDoubleCount() != null) {
            return counter.getDoubleCount();
          }
        }
      }
    }
    return 0L;
  }

  private Object getCounterValueAvg(SelectFunctionImpl function) {
    String columnName = ((Column) function.getParms().getExpressions().get(0)).getColumnName();
    if (counters != null) {
      for (Counter counter : counters) {
        if ((counter.getColumnName().equals(columnName))) {
          if (counter.getLongCount() != null) {
            return counter.getAvgLong();
          }
          if (counter.getDoubleCount() != null) {
            return counter.getAvgDouble();
          }
        }
      }
    }
    return 0L;
  }

  private Object getCounterValueMax(SelectFunctionImpl function) {
    String columnName = ((Column) function.getParms().getExpressions().get(0)).getColumnName();
    if (counters != null) {
      for (Counter counter : counters) {
        if ((counter.getColumnName().equals(columnName))) {
          if (counter.getLongCount() != null) {
            return counter.getMaxLong();
          }
          if (counter.getDoubleCount() != null) {
            return counter.getMaxDouble();
          }
        }
      }
    }
    return 0L;
  }

  private Object getCounterValueMin(SelectFunctionImpl function) {
    String columnName = ((Column) function.getParms().getExpressions().get(0)).getColumnName();
    if (counters != null) {
      for (Counter counter : counters) {
        if ((counter.getColumnName().equals(columnName))) {
          if (counter.getLongCount() != null) {
            return counter.getMinLong();
          }
          if (counter.getDoubleCount() != null) {
            return counter.getMinDouble();
          }
        }
      }
    }
    return 0L;
  }

  public Float getFloat(String columnLabel) {
    if (recordsResults != null) {
      return cachedRecordResultAsRecord.getFloat(columnLabel);
    }

    FieldInfo fieldInfo = fieldInfos.get(columnLabel);
    ExpressionImpl.CachedRecord cachedRecord = getDirectFieldValue(columnLabel);
    if (cachedRecord != null) {
      Object obj = cachedRecord.getRecord().getFields()[fieldInfo.fieldOffset];
      return getFloat(obj, fieldInfo.function);
    }

    if (isMatchingAlias(columnLabel) && isCount) {
      return (float) count;
    }

    Object ret = getGroupByFunctionResults(columnLabel, DataType.Type.FLOAT);
    if (ret != null) {
      return (Float) ret;
    }

    String[] actualColumn = null;
    SelectFunctionImpl function = functionAliases.get(DatabaseClient.toLower(columnLabel));
    actualColumn = getActualColumn(columnLabel, function);

    Object retObj = getField(actualColumn, columnLabel);

    fieldInfo = fieldInfos.get(columnLabel);
    if (fieldInfo != null) {
      fieldInfo.function = function;
    }

    return getFloat(retObj, function);
  }

  private Float getFloat(Object ret, SelectFunctionImpl function) {
    String retString = getRetString(ret);
    if (function != null) {
      if (retString != null && function.getName().equalsIgnoreCase(LENGTH_STR)) {
        return (float) retString.length();
      }
      else if (function.getName().equalsIgnoreCase(COUNT_STR) ||
          function.getName().equalsIgnoreCase("min") ||
          function.getName().equalsIgnoreCase("max") ||
          function.getName().equalsIgnoreCase("sum") ||
          function.getName().equalsIgnoreCase("avg")) {
        Object obj = getCounterValue(function);
        return (Float) DataType.getFloatConverter().convert(obj);
      }
    }

    return (Float) DataType.getFloatConverter().convert(ret);
  }

  public Double getDouble(String columnLabel) {
    if (recordsResults != null) {
      return cachedRecordResultAsRecord.getDouble(columnLabel);
    }

    FieldInfo fieldInfo = fieldInfos.get(columnLabel);
    ExpressionImpl.CachedRecord cachedRecord = getDirectFieldValue(columnLabel);
    if (cachedRecord != null) {
      Object obj = cachedRecord.getRecord().getFields()[fieldInfo.fieldOffset];
      return getDouble(obj, fieldInfo.function);
    }

    if (isMatchingAlias(columnLabel) && isCount) {
      return (double) count;
    }

    Object ret = getGroupByFunctionResults(columnLabel, DataType.Type.DOUBLE);
    if (ret != null) {
      return (Double) ret;
    }

    String[] actualColumn = null;
    SelectFunctionImpl function = functionAliases.get(DatabaseClient.toLower(columnLabel));
    actualColumn = getActualColumn(columnLabel, function);

    Object retObj = getField(actualColumn, columnLabel);

    fieldInfo = fieldInfos.get(columnLabel);
    if (fieldInfo != null) {
      fieldInfo.function = function;
    }

    return getDouble(retObj, function);
  }

  private Double getDouble(Object ret, SelectFunctionImpl function) {
    String retString = getRetString(ret);

    if (function != null) {
      if (function.getName().equalsIgnoreCase(LENGTH_STR)) {
        if (retString == null) {
          return 0d;
        }
        return (double) retString.length();
      }
      else if (function.getName().equalsIgnoreCase(COUNT_STR) ||
          function.getName().equalsIgnoreCase("min") ||
          function.getName().equalsIgnoreCase("max") ||
          function.getName().equalsIgnoreCase("sum") ||
          function.getName().equalsIgnoreCase("avg")) {
        Object obj = getCounterValue(function);
        return (Double) DataType.getDoubleConverter().convert(obj);
      }
    }
    return (Double) DataType.getDoubleConverter().convert(ret);
  }

  public BigDecimal getBigDecimal(String columnLabel, int scale) {
    throw new DatabaseException("not supported");
  }

  public byte[] getBytes(String columnLabel) {
    if (recordsResults != null) {
      return cachedRecordResultAsRecord.getBytes(columnLabel);
    }

    FieldInfo fieldInfo = fieldInfos.get(columnLabel);
    ExpressionImpl.CachedRecord cachedRecord = getDirectFieldValue(columnLabel);
    if (cachedRecord != null) {
      Object obj = cachedRecord.getRecord().getFields()[fieldInfo.fieldOffset];
      if (obj instanceof Blob) {
        return ((Blob) obj).getData();
      }
      return (byte[]) obj;
    }

    String[] actualColumn = getActualColumn(columnLabel);
    Object ret = getField(actualColumn, columnLabel);
    if (ret instanceof Blob) {
      return ((Blob) ret).getData();
    }
    return (byte[]) ret;
  }

  public Date getDate(String columnLabel) {
    if (recordsResults != null) {
      return cachedRecordResultAsRecord.getDate(columnLabel);
    }

    FieldInfo fieldInfo = fieldInfos.get(columnLabel);
    ExpressionImpl.CachedRecord cachedRecord = getDirectFieldValue(columnLabel);
    if (cachedRecord != null) {
      Object obj = cachedRecord.getRecord().getFields()[fieldInfo.fieldOffset];
      return (Date) DataType.getDateConverter().convert(obj);
    }

    String[] actualColumn = getActualColumn(columnLabel);
    return (Date) getField(actualColumn, columnLabel);
  }

  public Time getTime(String columnLabel) {
    if (recordsResults != null) {
      return cachedRecordResultAsRecord.getTime(columnLabel);
    }

    FieldInfo fieldInfo = fieldInfos.get(columnLabel);
    ExpressionImpl.CachedRecord cachedRecord = getDirectFieldValue(columnLabel);
    if (cachedRecord != null) {
      Object obj = cachedRecord.getRecord().getFields()[fieldInfo.fieldOffset];
      return (Time) DataType.getTimeConverter().convert(obj);
    }

    String[] actualColumn = getActualColumn(columnLabel);
    return (Time) getField(actualColumn, columnLabel);
  }

  public Timestamp getTimestamp(String columnLabel) {
    if (recordsResults != null) {
      return cachedRecordResultAsRecord.getTimestamp(columnLabel);
    }

    FieldInfo fieldInfo = fieldInfos.get(columnLabel);
    ExpressionImpl.CachedRecord cachedRecord = getDirectFieldValue(columnLabel);
    if (cachedRecord != null) {
      Object obj = cachedRecord.getRecord().getFields()[fieldInfo.fieldOffset];
      return (Timestamp) obj;
    }

    String[] actualColumn = getActualColumn(columnLabel);
    return (Timestamp) getField(actualColumn, columnLabel);
  }

  private ExpressionImpl.CachedRecord getDirectFieldValue(String columnLabel) {
    FieldInfo fieldInfo = fieldInfos.get(columnLabel);
    if (canShortCircuitFieldLookup(fieldInfo)) {
      return readRecords[currPos][fieldInfo.tableOffset];
    }
    return null;
  }

  public InputStream getAsciiStream(String columnLabel) {
    FieldInfo fieldInfo = fieldInfos.get(columnLabel);
    ExpressionImpl.CachedRecord cachedRecord = getDirectFieldValue(columnLabel);
    if (cachedRecord != null) {
      Object obj = cachedRecord.getRecord().getFields()[fieldInfo.fieldOffset];
      if (obj instanceof byte[]) {
        return new ByteArrayInputStream((byte[])obj);
      }
      return (InputStream) obj;
    }

    String[] actualColumn = getActualColumn(columnLabel);
    return (InputStream) getField(actualColumn, columnLabel);
  }

  public InputStream getUnicodeStream(String columnLabel) {
    FieldInfo fieldInfo = fieldInfos.get(columnLabel);
    ExpressionImpl.CachedRecord cachedRecord = getDirectFieldValue(columnLabel);
    if (cachedRecord != null) {
      Object obj = cachedRecord.getRecord().getFields()[fieldInfo.fieldOffset];
      if (obj instanceof byte[]) {
        return new ByteArrayInputStream((byte[]) obj);
      }
      return (InputStream) obj;
    }

    String[] actualColumn = getActualColumn(columnLabel);
    return (InputStream) getField(actualColumn, columnLabel);
  }

  public InputStream getBinaryStream(String columnLabel) {
    if (recordsResults != null) {
      return new ByteArrayInputStream(cachedRecordResultAsRecord.getBytes(columnLabel));
    }

    FieldInfo fieldInfo = fieldInfos.get(columnLabel);
    ExpressionImpl.CachedRecord cachedRecord = getDirectFieldValue(columnLabel);
    if (cachedRecord != null) {
      Object obj = cachedRecord.getRecord().getFields()[fieldInfo.fieldOffset];
      if (obj != null) {
        if (obj instanceof byte[]) {
          return new ByteArrayInputStream((byte[]) obj);
        }
        Blob blob = (Blob) obj;
        return new ByteArrayInputStream(blob.getData());
      }
    }

    String[] actualColumn = getActualColumn(columnLabel);
    Object ret = getField(actualColumn, columnLabel);
    if (ret == null) {
      return null;
    }
    if (ret instanceof byte[]) {
      return new ByteArrayInputStream((byte[]) ret);
    }
    Blob blob = (Blob) ret;
    return new ByteArrayInputStream(blob.getData());

  }

  public Reader getCharacterStream(String columnLabel) {
    FieldInfo fieldInfo = fieldInfos.get(columnLabel);
    ExpressionImpl.CachedRecord cachedRecord = getDirectFieldValue(columnLabel);
    if (cachedRecord != null) {
      Object obj = cachedRecord.getRecord().getFields()[fieldInfo.fieldOffset];
      if (obj == null) {
        return null;
      }
      return new InputStreamReader(new ByteArrayInputStream((byte[]) obj));
    }

    String[] actualColumn = getActualColumn(columnLabel);
    byte[] bytes = (byte[]) getField(actualColumn, columnLabel);
    if (bytes == null) {
      return null;
    }
    return new InputStreamReader(new ByteArrayInputStream(bytes));
  }

  public Reader getCharacterStream(int columnIndex) {
    byte[] bytes = (byte[]) getField(columnIndex);
    if (bytes == null) {
      return null;
    }
    return new InputStreamReader(new ByteArrayInputStream(bytes));
  }

  public BigDecimal getBigDecimal(String columnLabel) {
    if (recordsResults != null) {
      return cachedRecordResultAsRecord.getBigDecimal(columnLabel, -1);
    }

    FieldInfo fieldInfo = fieldInfos.get(columnLabel);
    ExpressionImpl.CachedRecord cachedRecord = getDirectFieldValue(columnLabel);
    if (cachedRecord != null) {
      Object obj = cachedRecord.getRecord().getFields()[fieldInfo.fieldOffset];
      return (BigDecimal) DataType.getBigDecimalConverter().convert(obj);
    }

    String[] actualColumn = getActualColumn(columnLabel);
    return (BigDecimal) getField(actualColumn, columnLabel);
  }

  @Override
  public Integer getInt(int columnIndex) {
    if (columnIndex == 1 && isCount) {
      return (int) count;
    }

    Object obj = getFunctionValue(columnIndex);
    if (obj != null) {
      if (obj instanceof Long) {
        return (int) (long) obj;
      }
      if (obj instanceof Double) {
        return (int) (double) obj;
      }
    }

    List<ColumnImpl> localColumns = selectStatement.getSelectColumns();

    ColumnImpl column = localColumns.get(columnIndex - 1);
    SelectFunctionImpl function = functionAliases.get(DatabaseClient.toLower(column.getColumnName()));

    Object ret = getField(columnIndex);

    return getInt(ret, function);
  }

  public Object getField(int columnIndex) {
    List<ColumnImpl> localColumns = selectStatement.getSelectColumns();
    ColumnImpl column = localColumns.get(columnIndex - 1);
    String function = column.getFunction();
    if (function == null) {
      String columnName = column.getColumnName();
      String[] actualColumn = getActualColumn(columnName);
      return getField(actualColumn, actualColumn[1]);
    }
    return null;
  }

  @Override
  public Long getLong(int columnIndex) {
    if (columnIndex == 1 && isCount) {
      SelectFunctionImpl function = functionAliases.get(DatabaseClient.toLower("__alias__"));
      if (function == null) {
        return count;
      }

      String[] actualColumn = getActualColumn("__all__");
      Object ret = getField(actualColumn, actualColumn[1]);
      return getLong(ret, function);
    }

    Object obj = getFunctionValue(columnIndex);
    if (obj != null) {
      if (obj instanceof Long) {
        return (Long) obj;
      }
      if (obj instanceof Double) {
        return (long) (double) obj;
      }
    }

    List<ColumnImpl> localColumns = selectStatement.getSelectColumns();
    ColumnImpl column = localColumns.get(columnIndex - 1);
    String columnName = column.getColumnName();
    SelectFunctionImpl function = functionAliases.get(DatabaseClient.toLower(column.getColumnName()));

    String[] actualColumn = getActualColumn(columnName);
    Object ret = getField(actualColumn, actualColumn[1]);
    return getLong(ret, function);
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) {
    String[] actualColumn = getActualColumn(columnIndex);
    Object ret = getField(actualColumn, actualColumn[1]);
    return (BigDecimal) DataType.getBigDecimalConverter().convert(ret);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) {
    String[] actualColumn = getActualColumn(columnIndex);
    return (Timestamp) DataType.getTimestampConverter().convert(getField(actualColumn, actualColumn[1]));
  }

  @Override
  public Time getTime(int columnIndex) {
    String[] actualColumn = getActualColumn(columnIndex);
    return (Time) DataType.getTimeConverter().convert(getField(actualColumn, actualColumn[1]));
  }

  @Override
  public Date getDate(int columnIndex) {
    String[] actualColumn = getActualColumn(columnIndex);
    return (Date) DataType.getDateConverter().convert(getField(actualColumn, actualColumn[1]));
  }

  @Override
  public byte[] getBytes(int columnIndex) {
    String[] actualColumn = getActualColumn(columnIndex);
    Object ret = getField(actualColumn, actualColumn[1]);
    if (ret instanceof Blob) {
      return ((Blob) ret).getData();
    }
    return (byte[]) ret;
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale) {
    throw new DatabaseException("Not supported");
  }

  @Override
  public Double getDouble(int columnIndex) {
    if (columnIndex == 1 && this.isCount) {
      return (double) this.count;
    }

    Object obj = getFunctionValue(columnIndex);
    if (obj != null) {
      if (obj instanceof Long) {
        return (double) (long) obj;
      }
      if (obj instanceof Double) {
        return (double) obj;
      }
    }

    String[] actualColumn = getActualColumn(columnIndex);

    if (isMatchingAlias(actualColumn[1]) && isCount) {
      return (double) count;
    }

    Object ret = getField(actualColumn, actualColumn[1]);

    SelectFunctionImpl function = functionAliases.get(DatabaseClient.toLower(actualColumn[1]));
    return getDouble(ret, function);

  }

  @Override
  public Float getFloat(int columnIndex) {
    if (columnIndex == 1 && this.isCount) {
      return (float) this.count;
    }

    Object obj = getFunctionValue(columnIndex);
    if (obj != null) {
      if (obj instanceof Long) {
        return (float) (long) obj;
      }
      if (obj instanceof Double) {
        return (float) (double) obj;
      }
    }

    String[] actualColumn = getActualColumn(columnIndex);
    if (isMatchingAlias(actualColumn[1]) && isCount) {
      return (float) count;
    }
    Object ret = getField(actualColumn, actualColumn[1]);

    SelectFunctionImpl function = functionAliases.get(DatabaseClient.toLower(actualColumn[1]));
    return getFloat(ret, function);
  }

  private Object getFunctionValue(int columnIndex) {
    if (columnIndex == 1 && functionAliases != null && functionAliases.values() != null && !functionAliases.values().isEmpty()) {
      SelectFunctionImpl functionObj = functionAliases.values().iterator().next();
      if (functionObj != null && (functionObj.getName().equalsIgnoreCase(COUNT_STR) ||
          functionObj.getName().equalsIgnoreCase("min") ||
          functionObj.getName().equalsIgnoreCase("max") ||
          functionObj.getName().equalsIgnoreCase("sum") ||
          functionObj.getName().equalsIgnoreCase("avg"))) {
        return getCounterValue(functionObj);
      }
    }
    return null;
  }

  @Override
  public Short getShort(int columnIndex) {
    String[] actualColumn = getActualColumn(columnIndex);
    Object ret = getField(actualColumn, actualColumn[1]);
    SelectFunctionImpl function = functionAliases.get(DatabaseClient.toLower(actualColumn[1]));
    return getShort(ret, function);
  }

  @Override
  public Byte getByte(int columnIndex) {
    String[] actualColumn = getActualColumn(columnIndex);
    Object ret = getField(actualColumn, actualColumn[1]);
    SelectFunctionImpl function = functionAliases.get(DatabaseClient.toLower(actualColumn[1]));
    return getByte(ret, function);
  }

  @Override
  public Boolean getBoolean(int columnIndex) {
    String[] actualColumn = getActualColumn(columnIndex);
    Object ret = getField(actualColumn, actualColumn[1]);
    return getBoolean(ret);
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) {
    String[] actualColumn = getActualColumn(columnIndex);
    Object ret = getField(actualColumn, actualColumn[1]);
    if (ret == null) {
      return null;
    }
    if (ret instanceof byte[]) {
      return new ByteArrayInputStream((byte[]) ret);
    }
    Blob blob = (Blob) ret;
    return new ByteArrayInputStream(blob.getData());
  }

  public String getString(int columnIndex) {
    if (columnIndex == 1 && isCount) {
      return String.valueOf((int) count);
    }

    Object obj = getFunctionValue(columnIndex);
    if (obj != null) {
      return (String) DataType.getStringConverter().convert(obj);
    }
    if (describeStrs != null) {
      if (columnIndex == 1) {
        return describeStrs[(int) currPos];
      }
      return null;
    }
    List<ColumnImpl> localColumns = selectStatement.getSelectColumns();
    ColumnImpl column = localColumns.get(columnIndex - 1);
    String columnName = column.getColumnName();
    String function = column.getFunction();

    String[] actualColumn = getActualColumn(columnName);
    Object ret = getField(actualColumn, actualColumn[1]);

    if (function != null) {
      ExpressionList localParms = column.getParameters();
      String retString = (String) ret;
      if (function.equals("upper")) {
        retString = retString.toUpperCase();
      }
      else if (function.equals("lower")) {
        retString = retString.toLowerCase();
      }
      else if (function.equals("substring")) {
        ExpressionList list = localParms;
        int pos1 = (int) ((LongValue) list.getExpressions().get(1)).getValue();
        if (list.getExpressions().size() > 2) {
          int pos2 = (int) ((LongValue) list.getExpressions().get(2)).getValue();
          retString = retString.substring(pos1, pos2);
        }
        else {
          retString = retString.substring(pos1);
        }
      }
      return retString;
    }

    return getString(ret);
  }

  public long getUniqueRecordCount() {
    return uniqueRecords.size();
  }

  public void setIsCount() {
    this.isCount = true;
  }

  public void getMoreResults(final int schemaRetryCount) {
    ClientStatsHandler.HistogramEntry histogramEntry = null;
    if (sqlToUse != null) {
      histogramEntry = databaseClient.getClientStatsHandler().registerQueryForStats(databaseClient.getCluster(), dbName, "(next page) " + sqlToUse);
    }
    long beginNanos = System.nanoTime();
    try {
      if (setOperation != null) {
        getMoreServerSetResults();
        return;
      }

      if (selectContext.getSelectStatement() != null) {
        if (!selectStatement.isOnServer() && selectStatement.isServerSelect()) {
          getMoreServerResults(selectStatement);
        }
        else {
          lastReadRecords = readRecords;
          readRecords = null;

          selectStatement.setPageSize(pageSize);
          ExpressionImpl.NextReturn ids = selectStatement.next(dbName, null,
              selectContext.isRestrictToThisServer(), selectContext.getProcedureContext(), schemaRetryCount);
          processResultsFromNextCall(schemaRetryCount, ids);
          currPos = 0;
        }
        return;
      }

      if (selectContext.getNextShard() == -1) {
        selectContext.setCurrKeys((Object[][][]) null);
        currPos = 0;
      }
    }
    finally {
      if (histogramEntry != null) {
        databaseClient.getClientStatsHandler().registerCompletedQueryForStats(histogramEntry, beginNanos);
      }
    }
  }

  private void processResultsFromNextCall(int schemaRetryCount, ExpressionImpl.NextReturn ids) {
    if (ids != null && ids.getIds() != null) {
      selectStatement.applyDistinct(dbName, selectContext.getTableNames(), ids, uniqueRecords);
    }

    readRecords = readRecords(ids, schemaRetryCount);
    if (ids != null && ids.getIds() != null && ids.getIds().length != 0) {
      selectContext.setCurrKeys(ids.getKeys());
    }
    else {
      selectContext.setCurrKeys((Object[][][]) null);
    }
  }

  private void getMoreServerSetResults() {
    while (true) {
      try {
        lastReadRecords = readRecords;

        SelectStatementHandler handler = (SelectStatementHandler) databaseClient.getStatementHandlerFactory().getHandler(new Select());
        handler.doServerSetSelect(dbName, tableNames, setOperation, this, restrictToThisServer, procedureContext);
        currPos = 0;
        break;
      }
      catch (SchemaOutOfSyncException e) {
        // try again
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }
  }

  private void getMoreServerResults(SelectStatementImpl selectStatement) {
    int schemaRetryCount = 0;
    while (true) {
      try {
        ComObject cobj = new ComObject();
        cobj.put(ComObject.Tag.LEGACY_SELECT_STATEMENT, selectStatement.serialize());
        cobj.put(ComObject.Tag.SCHEMA_VERSION, databaseClient.getCommon().getSchemaVersion());
        cobj.put(ComObject.Tag.DB_NAME, dbName);
        cobj.put(ComObject.Tag.COUNT, DatabaseClient.SELECT_PAGE_SIZE);
        cobj.put(ComObject.Tag.CURR_OFFSET, currOffset.get());
        cobj.put(ComObject.Tag.COUNT_RETURNED, countReturned.get());

        byte[] recordRet = databaseClient.send("ReadManager:serverSelect", selectStatement.getServerSelectShardNumber(),
            selectStatement.getServerSelectReplicaNumber(), cobj, DatabaseClient.Replica.SPECIFIED);

        ComObject retObj = new ComObject(recordRet);
        selectStatement.deserialize(retObj.getByteArray(ComObject.Tag.LEGACY_SELECT_STATEMENT));

        if (retObj.getLong(ComObject.Tag.CURR_OFFSET) != null) {
          currOffset.set(retObj.getLong(ComObject.Tag.CURR_OFFSET));
        }
        if (retObj.getLong(ComObject.Tag.COUNT_RETURNED) != null) {
          countReturned.set(retObj.getLong(ComObject.Tag.COUNT_RETURNED));
        }
        String[] localTableNames = selectStatement.getTableNames();
        TableSchema[] tableSchemas = new TableSchema[localTableNames.length];
        for (int i = 0; i < localTableNames.length; i++) {
          tableSchemas[i] = databaseClient.getCommon().getTables(dbName).get(localTableNames[i]);
        }

        String[][] primaryKeyFields = getPrimaryKeyFields(localTableNames, tableSchemas);

        lastReadRecords = readRecords;
        readRecords = null;
        synchronized (recordCache.getRecordsForTable()) {
          recordCache.getRecordsForTable().clear();
        }

        ComArray tablesArray = retObj.getArray(ComObject.Tag.TABLE_RECORDS);
        int recordCount = tablesArray == null ? 0 : tablesArray.getArray().size();
        Object[][][] localRetKeys = new Object[recordCount][][];
        Record[][] currRetRecords = new Record[recordCount][];

        fillRecordCacheWithServerResults(localTableNames, tableSchemas, primaryKeyFields, tablesArray, recordCount,
            localRetKeys, currRetRecords);

        if (localRetKeys == null || localRetKeys.length == 0) {
          selectContext.setCurrKeys(null);
        }
        else {
          selectContext.setCurrKeys(localRetKeys);
        }

        ExpressionImpl.NextReturn ret = new ExpressionImpl.NextReturn(localTableNames, localRetKeys);
        readRecords = readRecords(ret, schemaRetryCount);
        currPos = 0;
        break;
      }
      catch (SchemaOutOfSyncException e) {
        schemaRetryCount++;
        //try again
      }
    }
  }

  private String[][] getPrimaryKeyFields(String[] localTableNames, TableSchema[] tableSchemas) {
    String[][] primaryKeyFields = new String[localTableNames.length][];
    outer:
    for (int i = 0; i < localTableNames.length; i++) {
      for (Map.Entry<String, IndexSchema> entry : tableSchemas[i].getIndices().entrySet()) {
        if (entry.getValue().isPrimaryKey()) {
          primaryKeyFields[i] = entry.getValue().getFields();
          break outer;
        }
      }
    }
    return primaryKeyFields;
  }

  private void fillRecordCacheWithServerResults(String[] localTableNames, TableSchema[] tableSchemas,
                                                String[][] primaryKeyFields, ComArray tablesArray, int recordCount,
                                                Object[][][] localRetKeys, Record[][] currRetRecords) {
    for (int k = 0; k < recordCount; k++) {
      currRetRecords[k] = new Record[localTableNames.length];
      localRetKeys[k] = new Object[localTableNames.length][];
      ComArray records = (ComArray) tablesArray.getArray().get(k);
      for (int j = 0; j < localTableNames.length; j++) {
        byte[] bytes = (byte[]) records.getArray().get(j);
        if (bytes != null) {
          Record record = new Record(tableSchemas[j]);
          record.deserialize(dbName, databaseClient.getCommon(), bytes, null, true);
          currRetRecords[k][j] = record;

          Object[] key = new Object[primaryKeyFields[j].length];
          for (int i = 0; i < primaryKeyFields[j].length; i++) {
            key[i] = record.getFields()[tableSchemas[j].getFieldOffset(primaryKeyFields[j][i])];
          }

          if (localRetKeys[k][j] == null) {
            localRetKeys[k][j] = key;
          }

          recordCache.put(localTableNames[j], key, new ExpressionImpl.CachedRecord(record, bytes));
        }
      }

    }
  }

  public ExpressionImpl.CachedRecord[][] readRecords(ExpressionImpl.NextReturn nextReturn, int schemaRetryCount) {
    if (nextReturn == null || nextReturn.getKeys() == null) {
      return null;
    }

    while (true) {
      try {
        Object[][][] actualIds = nextReturn.getKeys();
        ExpressionImpl.CachedRecord[][] retRecords = new ExpressionImpl.CachedRecord[actualIds.length][];

        if (retRecords.length > 0) {
          processRetRecords(nextReturn, schemaRetryCount, actualIds, retRecords);
        }
        recordCache.clear();
        return retRecords;
      }
      catch (Exception e) {
        if (-1 != ExceptionUtils.indexOfThrowable(e, SchemaOutOfSyncException.class)) {
          // try again
        }
        else {
          throw new DatabaseException(e);
        }
      }
    }

  }

  private void processRetRecords(ExpressionImpl.NextReturn nextReturn, int schemaRetryCount, Object[][][] actualIds,
                                 ExpressionImpl.CachedRecord[][] retRecords) {
    Map<ExpressionImpl.RecordCache.Key, ExpressionImpl.CachedRecord>[] tables =
        new ConcurrentHashMap[nextReturn.getTableNames().length];
    for (int j = 0; j < nextReturn.getTableNames().length; j++) {
      tables[j] = recordCache.getRecordsForTable(nextReturn.getTableNames()[j]);
    }

    for (int i = 0; i < retRecords.length; i++) {
      retRecords[i] = new ExpressionImpl.CachedRecord[actualIds[i].length];
      for (int j = 0; j < retRecords[i].length; j++) {
        if (actualIds[i][j] == null) {
          continue;
        }

        ExpressionImpl.CachedRecord cachedRecord = tables[j].get(new ExpressionImpl.RecordCache.Key(actualIds[i][j]));
        retRecords[i][j] = cachedRecord;
        if (retRecords[i][j] == null) {
          Record record = doReadRecord(actualIds[i][j], nextReturn.getTableNames()[j], schemaRetryCount);
          retRecords[i][j] = new ExpressionImpl.CachedRecord(record, record.serialize(databaseClient.getCommon(),
              DatabaseClient.SERIALIZATION_VERSION));
        }
      }
    }
  }

  private class GetGroupByFunctionResutsForSimpleMathFunction {
    private boolean myResult;
    private String columnLabel;
    private Map.Entry<String, ColumnImpl> alias;
    private GroupByContext.GroupCounter counter;

    public GetGroupByFunctionResutsForSimpleMathFunction(String columnLabel,  Map.Entry<String, ColumnImpl> alias) {
      this.columnLabel = columnLabel;
      this.alias = alias;
    }

    boolean is() {
      return myResult;
    }

    public GroupByContext.GroupCounter getCounter() {
      return counter;
    }

    public GetGroupByFunctionResutsForSimpleMathFunction invoke() {
      String[] actualLabel = getActualColumn(alias.getValue().getColumnName());
      if (actualLabel[0] == null) {
        actualLabel[0] = selectStatement.getFromTable();
      }
      boolean isNull = false;
      Object[] values = new Object[groupByContext.getFieldContexts().size()];
      for (int i = 0; i < values.length; i++) {
        String groupColumnName = ((Column) groupByColumns.get(i)).getColumnName();
        String[] actualColumn = getActualColumn(groupColumnName);
        values[i] = getField(actualColumn, columnLabel);
        if (values[i] == null) {
          isNull = true;
        }
      }
      if (!isNull) {
        counter = groupByContext.getGroupCounters().get(actualLabel[0] + ":" + actualLabel[1]).get(values);
        myResult = true;
        return this;
      }
      myResult = false;
      return this;
    }
  }
}

