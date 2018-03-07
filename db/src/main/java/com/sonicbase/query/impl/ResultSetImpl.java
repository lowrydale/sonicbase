package com.sonicbase.query.impl;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.*;

import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.procedure.RecordImpl;
import com.sonicbase.procedure.StoredProcedureContextImpl;
import com.sonicbase.query.BinaryExpression;
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
import org.apache.commons.lang.exception.ExceptionUtils;

import java.io.*;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Responsible for
 */
public class ResultSetImpl implements ResultSet {
  private static final String UTF8_STR = "utf-8";
  private static final String LENGTH_STR = "length";
  private ComArray recordsResults;
  private StoredProcedureContextImpl procedureContext;
  private boolean restrictToThisServer;
  private Map<String, SelectFunctionImpl> functionAliases;
  private Map<String, ColumnImpl> aliases;
  private String[] tableNames;
  private Object[][][] retKeys;
  private DatabaseClient.SetOperation setOperation;
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
  private String indexUsed;
  private SelectContextImpl selectContext;
  private DatabaseClient databaseClient;
  private int currPos = -1;
  private long currTotalPos = -1;
  private Record[] currRecord;
  private Counter[] counters;
  private Limit limit;
  private long pageSize = DatabaseClient.SELECT_PAGE_SIZE;
  private Object moreServerSetResults;
  private RecordImpl cachedRecordResultAsRecord;
  private ComObject cachedRecordResutslAsCObj;

  public ResultSetImpl(String[] describeStrs) {
    this.describeStrs = describeStrs;
  }

  public ResultSetImpl(List<Map<String, String>> mapResults) {
    this.mapResults = mapResults;
  }

  public ResultSetImpl(String dbName, DatabaseClient client, String[] tableNames, DatabaseClient.SetOperation setOperation,
                       Map<String, ColumnImpl> aliases, Map<String, SelectFunctionImpl> functionAliases,
                       boolean restrictToThisServer, StoredProcedureContextImpl procedureContext) {
//    this.readRecords = readRecords(new ExpressionImpl.NextReturn(selectContext.getTableNames(), selectContext.getCurrKeys()));
    this.dbName = dbName;
    this.databaseClient = client;
    //this.retKeys = retKeys;
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

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "EI_EXPOSE_REP", justification = "copying the returned data is too slow")
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
        if (function.equals("count") || function.equals("min") || function.equals("max") || function.equals("sum") || function.equals("avg")) {
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
            GroupByContext.GroupCounter counter = groupByContext.getGroupCounters().get(actualLabel[0] + ":" + actualLabel[1]).get(values);
            if (counter.getCounter().getLongCount() != null) {
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
              if (function.equals("count")) {
                return type.getConverter().convert((long) counter.getCounter().getCount());
              }
            }
            else if (counter.getCounter().getDoubleCount() != null) {
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
              if (function.equals("count")) {
                return type.getConverter().convert((long) counter.getCounter().getCount());
              }
            }
          }
        }
      }
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

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "EI_EXPOSE_REP", justification = "copying the returned data is too slow")
    public String[] getTableNames() {
      return tableNames;
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "EI_EXPOSE_REP2", justification = "copying the passed in data is too slow")
    @SuppressWarnings("PMD.ArrayIsStoredDirectly") //copying the passed in data is too slow
    public void setTableNames(String[] tableNames) {
      this.tableNames = tableNames;
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "EI_EXPOSE_REP", justification = "copying the returned data is too slow")
    public long[][] getIds() {
      return ids;
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "EI_EXPOSE_REP2", justification = "copying the passed in data is too slow")
    @SuppressWarnings("PMD.ArrayIsStoredDirectly") //copying the passed in data is too slow
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
      DatabaseClient databaseClient, SelectStatementImpl selectStatement,
      ParameterHandler parms, Set<SelectStatementImpl.DistinctRecord> uniqueRecords, SelectContextImpl selectContext,
      Record[] retRecords,
      List<ColumnImpl> columns,
      String indexUsed, Counter[] counters, Limit limit, Offset offset,
      List<Expression> groupByColumns, GroupByContext groupContext, boolean restrictToThisServer, StoredProcedureContextImpl procedureContext) throws Exception {
    this.dbName = dbName;
    this.databaseClient = databaseClient;
    this.selectStatement = selectStatement;
    this.aliases = selectStatement.getAliases();
    this.functionAliases = selectStatement.getFunctionAliases();
    this.tableNames = selectStatement.getTableNames();
    this.parms = parms;
    this.uniqueRecords = uniqueRecords;
    this.selectContext = selectContext;
    this.indexUsed = indexUsed;
    this.columns = columns;
    this.recordCache = selectContext.getRecordCache();
    this.readRecords = readRecords(new ExpressionImpl.NextReturn(selectContext.getTableNames(), selectContext.getCurrKeys()));
    this.counters = counters;
    this.limit = limit;
    this.offset = offset;
    this.groupByColumns = groupByColumns;
    this.groupByContext = groupContext;
    this.pageSize = selectStatement.getPageSize();
    this.restrictToThisServer = restrictToThisServer;
    this.procedureContext = procedureContext;

    List<OrderByExpressionImpl> orderByExpressions = selectStatement.getOrderByExpressions();
    if (orderByExpressions.size() != 0) {
      if (orderByExpressions.size() > 1 || (selectContext.getSortWithIndex() != null && !selectContext.getSortWithIndex())) {
        if (selectContext.getCurrKeys() != null) {
//          this.readRecords = new Record[selectContext.getCurrKeys().length][];
//          int tableCount = selectContext.getTableNames().length;
//          for (int i = 0; i < readRecords.length; i++) {
//            readRecords[i] = new Record[tableCount];
//            long[] keys = selectContext.getCurrKeys()[i];
//            for (int j = 0; j < tableCount; j++) {
//              if (keys[j] != -1) {
//                readRecords[i][j] = doReadRecord(selectContext.getCurrKeys()[i][j], selectContext.getTableNames()[j]);
//              }
//            }
//          }
          sortResults(dbName, databaseClient.getCommon(), readRecords, selectContext.getTableNames(),
              selectStatement.getOrderByExpressions());
        }
      }
    }
  }

  public static void sortResults(
      String dbName,
      DatabaseCommon common,
      ExpressionImpl.CachedRecord[][] records,
      final String[] tableNames, List<OrderByExpressionImpl> orderByExpressions) {
    if (orderByExpressions.size() != 0) {
      final int[] fieldOffsets = new int[orderByExpressions.size()];
      final boolean[] ascendingFlags = new boolean[orderByExpressions.size()];
      final Comparator[] comparators = new Comparator[orderByExpressions.size()];
      final int[] tableOffsets = new int[orderByExpressions.size()];
      for (int i = 0; i < orderByExpressions.size(); i++) {
        String tableName = orderByExpressions.get(i).getTableName();
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
        if (tableName == null) {
          tableName = tableNames[0];
        }
        TableSchema tableSchema = common.getTables(dbName).get(tableName);
        fieldOffsets[i] = tableSchema.getFieldOffset(orderByExpressions.get(i).getColumnName());
        ascendingFlags[i] = orderByExpressions.get(i).isAscending();
        FieldSchema fieldSchema = tableSchema.getFields().get(fieldOffsets[i]);
        comparators[i] = fieldSchema.getType().getComparator();
      }

      Arrays.sort(records, new Comparator<ExpressionImpl.CachedRecord[]>() {
        @Override
        public int compare(ExpressionImpl.CachedRecord[] o1, ExpressionImpl.CachedRecord[] o2) {
          for (int i = 0; i < fieldOffsets.length; i++) {
            if (o1[tableOffsets[i]] == null && o2[tableOffsets[i]] == null) {
              continue;
            }
            if ((o1[tableOffsets[i]] == null || o1[tableOffsets[i]].getRecord().getFields()[fieldOffsets[i]] == null) &&
                (o2[tableOffsets[i]] == null || o2[tableOffsets[i]].getRecord().getFields()[fieldOffsets[i]] == null)) {
              continue;
            }
            if (o1[tableOffsets[i]] == null) {
              return -1 * (ascendingFlags[i] ? 1 : -1);
            }
            if (o2[tableOffsets[i]] == null) {
              return 1 * (ascendingFlags[i] ? 1 : -1);
            }
            if (o1[tableOffsets[i]].getRecord().getFields()[fieldOffsets[i]] == null) {
              return 1 * (ascendingFlags[i] ? 1 : -1);
            }
            if (o2[tableOffsets[i]].getRecord().getFields()[fieldOffsets[i]] == null) {
              return -1 * (ascendingFlags[i] ? 1 : -1);
            }
            int value = comparators[i].compare(o1[tableOffsets[i]].getRecord().getFields()[fieldOffsets[i]], o2[tableOffsets[i]].getRecord().getFields()[fieldOffsets[i]]);
            if (value < 0) {
              return -1 * (ascendingFlags[i] ? 1 : -1);
            }
            if (value > 0) {
              return 1 * (ascendingFlags[i] ? 1 : -1);
            }
          }
          return 0;
        }
      });
    }
  }

  public String getIndexUsed() {
    return indexUsed;
  }

  public boolean isAfterLast() {
    return currRecord == null;
  }

  public boolean next() throws DatabaseException {
    currPos++;

//    if (selectStatement.getFunctionAliases().size() != 0) {
//      return currPos == 0;
//    }

    if (describeStrs != null) {
      if (currPos > describeStrs.length - 1) {
        return false;
      }
      return true;
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
      if (currPos > mapResults.size() - 1) {
        return false;
      }
      return true;
    }
    if (isCount) {
      if (currPos == 0) {
        return true;
      }
      else {
        return false;
      }
    }

    currTotalPos++;
    if (setOperation == null && selectContext == null) {
      return false;
    }

    if (counters != null || (selectStatement != null && (isCount && selectStatement.isDistinct()))) {

      boolean requireEvaluation = false;
      if (selectStatement != null) {
        for (SelectFunctionImpl function : selectStatement.getFunctionAliases().values()) {
          if (function.getName().equalsIgnoreCase("min") || function.getName().equalsIgnoreCase("max")) {
          }
          else {
            requireEvaluation = true;
          }
        }
      }
      if (requireEvaluation || (counters == null || !(selectStatement.getExpression() instanceof AllRecordsExpressionImpl))) {
        boolean first = false;
        if (currPos == 0) {
          first = true;
          while (true) {
            try {
              getMoreResults();
              if (selectContext.getCurrKeys() == null) {
                break;
              }
            }
            catch (SchemaOutOfSyncException e) {
              continue;
            }
            catch (Exception e) {
              throw new DatabaseException(e);
            }
          }
        }
        return first;
      }
      else {
        if (currPos == 0) {
          return true;
        }
        else {
          return false;
        }
      }
    }

    if (((setOperation != null && retKeys == null) || (setOperation == null && selectContext.getCurrKeys() == null))
        && (readRecords == null || readRecords.length == 0)) {
      return false;
    }
//    if (readRecords != null) {
//      if (currPos >= readRecords.length) {
//        return false;
//      }
//      currRecord = readRecords[currPos];
//    }
//    else {
//    if (offset != null) {
//      while (currTotalPos < offset.getOffset() - 1) {
//        if ((selectContext.getCurrKeys().length == 0 || currPos >= selectContext.getCurrKeys().length)) {
//          while (true) {
//            try {
//              getMoreResults();
//              break;
//            }
//            catch (SchemaOutOfSyncException e) {
//              continue;
//            }
//            catch (Exception e) {
//              throw new DatabaseException(e);
//            }
//          }
//        }
//        currPos++;
//        currTotalPos++;
//      }
//    }

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
      outer:
      while (true) {
        currPos++;
        if ((selectContext.getCurrKeys().length == 0 || currPos >= selectContext.getCurrKeys().length)) {
          while (true) {
            try {
              getMoreResults();
              break;
            }
            catch (SchemaOutOfSyncException e) {
              continue;
            }
            catch (Exception e) {
              throw new DatabaseException(e);
            }
          }
        }
        if (selectContext.getCurrKeys() == null && (readRecords == null || readRecords.length == 0)) {
          currPos--;
          return true;
        }
        if ((selectContext.getCurrKeys().length == 0 || currPos >= selectContext.getCurrKeys().length)) {
          currPos--;
          return true;
        }
        boolean nonNull = true;
        for (int i = 0; i < lastFields.length; i++) {
          if (lastFields[i] == null) {
            nonNull = false;
          }
        }
        boolean hasNull = false;
        Object[] lastTmp = new Object[lastFields.length];
        for (int i = 0; i < lastFields.length; i++) {
          if (lastFields[i] == null) {
            return false;
          }
          Object field = getField(actualColumns[i], actualColumns[i][1]);
          if (nonNull && 0 != comparators[i].compare(field, lastFields[i]) && field != null && lastFields[i] != null) {
            currPos--;
            break outer;
          }
          if (lastFields[i] != null && field == null) {
            currPos--;
            break outer;
          }
          lastFields[i] = field;
          lastTmp[i] = field;
          if (field == null) {
            hasNull = true;
          }
        }
        if (!hasNull) {
          boolean notNull = true;
          for (int i = 0; i < lastFields.length; i++) {
            if (lastFields[i] != null) {
              notNull = true;
            }
          }
          if (!notNull) {
            lastFields = lastTmp;
            currPos--;
            break outer;
          }
        }

      }
      if (offset != null) {
        while (currTotalPos < offset.getOffset() - 1) {
          if ((selectContext.getCurrKeys().length == 0 || currPos >= selectContext.getCurrKeys().length)) {
            while (true) {
              try {
                getMoreResults();
                break;
              }
              catch (SchemaOutOfSyncException e) {
                continue;
              }
              catch (Exception e) {
                throw new DatabaseException(e);
              }
            }
          }
          currPos++;
          currTotalPos++;
        }
      }
    }

    if ((setOperation != null && (retKeys.length == 0 || currPos >= retKeys.length)) ||
        (setOperation == null && (selectContext.getCurrKeys().length == 0 || currPos >= selectContext.getCurrKeys().length))) {
      while (true) {
        try {
          getMoreResults();
          break;
        }
        catch (SchemaOutOfSyncException e) {
          continue;
        }
        catch (Exception e) {
          throw new DatabaseException(e);
        }
      }
    }
//    if (limit != null) {
//      if (!limit.isLimitAll() && !limit.isLimitNull()) {
//        if (offset != null) {
//          if (currTotalPos >= offset.getOffset() + limit.getRowCount()) {
//            return false;
//          }
//        }
//        else {
//          if (currTotalPos >= limit.getRowCount()) {
//            return false;
//          }
//        }
//      }
//    }
    //   readCurrentRecord();


    if ((setOperation != null && (retKeys == null || retKeys.length == 0)) ||
        (setOperation == null && selectContext.getCurrKeys() == null)) {
      return false;
    }
//    }

//    if (currRecord == null) {
//      return false;
//    }

    return true; //!isAfterLast();
  }


  private Record doReadRecord(Object[] key, String tableName) throws Exception {

    return ExpressionImpl.doReadRecord(dbName, databaseClient, selectStatement.isForceSelectOnServer(), parms,
        selectStatement.getWhereClause(), recordCache, key, tableName, columns,
        ((ExpressionImpl) selectStatement.getWhereClause()).getViewVersion(), false,
        selectContext.isRestrictToThisServer(), selectContext.getProcedureContext());
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
      if (currPos >= describeStrs.length - 1) {
        return true;
      }
      return false;
    }
    if (mapResults != null) {
      if (currPos >= mapResults.size() - 1) {
        return true;
      }
      return false;
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

  public int getRow() {
    return currPos;
  }

  public void close() throws Exception {

    if (selectStatement != null && selectStatement.isServerSelect()) {
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.dbName, dbName);
      cobj.put(ComObject.Tag.schemaVersion, databaseClient.getCommon().getSchemaVersion());
      cobj.put(ComObject.Tag.method, "serverSelectDelete");
      cobj.put(ComObject.Tag.id, selectStatement.getServerSelectResultSetId());

      byte[] recordRet = databaseClient.send(null, selectStatement.getServerSelectShardNumber(),
          selectStatement.getServerSelectReplicaNumber(), cobj, DatabaseClient.Replica.specified);

    }

  }

  private Object getField(String[] label, String columnLabel) {
    label[1] = DatabaseClient.toLower(label[1]);
    if (label[0] != null) {
      label[0] = DatabaseClient.toLower(label[0]);
    }
    if (label[0] != null) {
      for (int i = 0; i < selectContext.getTableNames().length; i++) {
        if (label[0].equals(selectContext.getTableNames()[i])) {
          TableSchema tableSchema = databaseClient.getCommon().getTables(dbName).get(label[0]);
          Integer offset = tableSchema.getFieldOffset(label[1]);
          if (offset != null) {
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
        }
      }
    }
    for (int i = 0; i < tableNames.length; i++) {
      TableSchema tableSchema = databaseClient.getCommon().getTables(dbName).get(tableNames[i]);
      Integer offset = tableSchema.getFieldOffset(label[1]);
      if (offset != null) {
        if (currPos < 0) {
          if (lastReadRecords == null) {
            return null;
          }
          if (lastReadRecords[lastReadRecords.length + currPos][i] == null) {
            continue;
          }
          return lastReadRecords[lastReadRecords.length + currPos][i].getRecord().getFields()[offset];
        }
        if (readRecords[currPos][i] == null) {
          continue;
        }
        FieldInfo fieldInfo = new FieldInfo();
        fieldInfo.labelName = label[1];
        fieldInfo.tableOffset = i;
        fieldInfo.fieldOffset = offset;
        fieldInfos.put(columnLabel, fieldInfo);
        return readRecords[currPos][i].getRecord().getFields()[offset];
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
    String[] actualColumn = null;
    SelectFunctionImpl function = functionAliases.get(DatabaseClient.toLower(columnLabel));
    if (function != null) {
      if (function != null && this.groupByContext == null) {
        if (function.getName().equalsIgnoreCase("count") || function.getName().equalsIgnoreCase("min") || function.getName().equalsIgnoreCase("max") || function.getName().equalsIgnoreCase("sum") || function.getName().equalsIgnoreCase("avg")) {
          Object obj = this.getCounterValue(function);
          if (obj instanceof Long) {
            return String.valueOf((Long)obj);
          }

          if (obj instanceof Double) {
            return String.valueOf((Double)obj);
          }
        }
      }

      if (function.getName().equalsIgnoreCase(LENGTH_STR) ||
          function.getName().equalsIgnoreCase("upper") ||
          function.getName().equalsIgnoreCase("lower") ||
          function.getName().equalsIgnoreCase("substring")) {
        actualColumn = getActualColumn(columnLabel);
      }
      else {
        actualColumn = new String[]{null, DatabaseClient.toLower(columnLabel)};
      }
    }
    else {
      actualColumn = getActualColumn(columnLabel);
    }
    Object ret = getField(actualColumn, columnLabel);

    String retString = getString(ret);

    if (function != null) {
      if (retString != null && function.getName().equalsIgnoreCase(LENGTH_STR)) {
        return String.valueOf(retString.length());
      }
      else if (function.getName().equalsIgnoreCase("min") ||
          function.getName().equalsIgnoreCase("max") ||
          function.getName().equalsIgnoreCase("sum") ||
          function.getName().equalsIgnoreCase("avg")) {
        Object obj = getCounterValue(function);
        if (obj instanceof Double) {
          return String.valueOf((double) obj);
        }
        if (obj instanceof Long) {
          return String.valueOf((long) obj);
        }
      }
    }

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
    String retString = null;
    if (ret instanceof byte[]) {
      try {
        retString = new String((byte[]) ret, UTF8_STR);
      }
      catch (UnsupportedEncodingException e) {
        throw new DatabaseException(e);
      }
    }
    if (ret instanceof Long) {
      retString = String.valueOf(ret);
    }
    else if (ret instanceof Integer) {
      retString = String.valueOf(ret);
    }
    else if (ret instanceof Short) {
      retString = String.valueOf(ret);
    }
    else if (ret instanceof Byte) {
      retString = String.valueOf(ret);
    }
    else if (ret instanceof Boolean) {
      retString = String.valueOf(ret);
    }
    else if (ret instanceof Float) {
      retString = String.valueOf(ret);
    }
    else if (ret instanceof Double) {
      retString = String.valueOf(ret);
    }
    else if (ret instanceof BigDecimal) {
      retString = String.valueOf(ret);
    }
    else if (ret instanceof Date) {
      retString = String.valueOf(ret);
    }
    else if (ret instanceof Time) {
      retString = String.valueOf(ret);
    }
    else if (ret instanceof Timestamp) {
      retString = String.valueOf(ret);
    }
    return retString;
  }

  private String[] getActualColumn(String columnLabel) {
    ColumnImpl column = aliases.get(columnLabel);
    if (column != null) {
      return new String[]{column.getTableName(), column.getColumnName()};
    }
    return new String[]{null, columnLabel};
  }

  public Boolean getBoolean(String columnLabel) {
    if (recordsResults != null) {
      return cachedRecordResultAsRecord.getBoolean(columnLabel);
    }
    FieldInfo fieldInfo = fieldInfos.get(columnLabel);
    if (canShortCircuitFieldLookup(fieldInfo)) {
      ExpressionImpl.CachedRecord ret = readRecords[currPos][fieldInfo.tableOffset];
      if (ret != null) {
        Object obj = ret.getRecord().getFields()[fieldInfo.fieldOffset];
        return getBoolean(obj);
      }
    }

    String[] actualColumn = getActualColumn(columnLabel);
    Object ret = getField(actualColumn, columnLabel);

    return getBoolean(ret);
  }

  private Boolean getBoolean(Object ret) {
    if (ret instanceof byte[]) {
      try {
        String retString = new String((byte[]) ret, UTF8_STR);
        if (retString.equalsIgnoreCase("true")) {
          return true;
        }
        return false;
      }
      catch (UnsupportedEncodingException e) {
        throw new DatabaseException(e);
      }
    }
    if (ret instanceof Boolean) {
      return (Boolean) ret;
    }
    if (ret instanceof Long) {
      return ((Long) ret) == 1 ? true : false;
    }
    if (ret instanceof Integer) {
      return ((Integer) ret) == 1 ? true : false;
    }
    if (ret instanceof Short) {
      return ((Short) ret) == 1 ? true : false;
    }
    if (ret instanceof Byte) {
      return ((Byte) ret) == 1 ? true : false;
    }
    return (Boolean) ret;
  }

  public Byte getByte(String columnLabel) {
    if (recordsResults != null) {
      return cachedRecordResultAsRecord.getByte(columnLabel);
    }

    FieldInfo fieldInfo = fieldInfos.get(columnLabel);
    if (canShortCircuitFieldLookup(fieldInfo)) {
      ExpressionImpl.CachedRecord ret = readRecords[currPos][fieldInfo.tableOffset];
      if (ret != null) {
        Object obj = ret.getRecord().getFields()[fieldInfo.fieldOffset];
        return getByte(obj, fieldInfo.function == null ? null : fieldInfo.function.getName());
      }
    }

    String[] actualColumn = null;
    SelectFunctionImpl function = functionAliases.get(DatabaseClient.toLower(columnLabel));
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
    Object ret = getField(actualColumn, columnLabel);

    fieldInfo = fieldInfos.get(columnLabel);
    if (fieldInfo != null) {
      fieldInfo.function = function;
    }

    return getByte(ret, function == null ? null : function.getName());
  }

  private Byte getByte(Object ret, String function) {
    String retString = null;
    if (ret instanceof byte[]) {
      try {
        retString = new String((byte[]) ret, UTF8_STR);
      }
      catch (UnsupportedEncodingException e) {
        throw new DatabaseException(e);
      }
    }
    if (function != null) {
      if (function.equals(LENGTH_STR)) {
        return (byte) retString.length();
      }
    }
    if (retString != null) {
      return Byte.valueOf(retString);
    }
    if (ret instanceof Byte) {
      return (Byte) ret;
    }
    if (ret instanceof Long) {
      return (byte) (long) ret;
    }
    if (ret instanceof Integer) {
      return (byte) (int) ret;
    }
    if (ret instanceof Short) {
      return (byte) (short) ret;
    }
    return null;
  }

  public Short getShort(String columnLabel) {
    if (recordsResults != null) {
      return cachedRecordResultAsRecord.getShort(columnLabel);
    }

    FieldInfo fieldInfo = fieldInfos.get(columnLabel);
    if (canShortCircuitFieldLookup(fieldInfo)) {
      ExpressionImpl.CachedRecord ret = readRecords[currPos][fieldInfo.tableOffset];
      if (ret != null) {
        Object obj = ret.getRecord().getFields()[fieldInfo.fieldOffset];
        Short retString1 = getShort(obj, fieldInfo.function == null ? null : fieldInfo.function.getName());
        if (retString1 != null) {
          return retString1;
        }
        return (Short) obj;
      }
    }

    String[] actualColumn = null;
    SelectFunctionImpl function = functionAliases.get(DatabaseClient.toLower(columnLabel));
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

    Object ret = getField(actualColumn, columnLabel);

    fieldInfo = fieldInfos.get(columnLabel);
    if (fieldInfo != null) {
      fieldInfo.function = function;
    }

    Short retString1 = getShort(ret, function == null ? null : function.getName());
    if (retString1 != null) {
      return retString1;
    }
    return (Short) ret;
  }

  private Short getShort(Object ret, String function) {
    String retString = null;
    if (ret instanceof byte[]) {
      try {
        retString = new String((byte[]) ret, UTF8_STR);
      }
      catch (UnsupportedEncodingException e) {
        throw new DatabaseException(e);
      }
    }
    if (function != null) {
      if (function.equals(LENGTH_STR)) {
        return (short) retString.length();
      }
    }
    if (retString != null) {
      return Short.valueOf(retString);
    }
    if (ret instanceof Short) {
      return (Short) ret;
    }
    if (ret instanceof Long) {
      return (short) (long) ret;
    }
    if (ret instanceof Integer) {
      return (short) (int) ret;
    }
    if (ret instanceof Byte) {
      return (short) (byte) ret;
    }
    return null;
  }

  public Integer getInt(String columnLabel) {
    if (recordsResults != null) {
      return cachedRecordResultAsRecord.getInt(columnLabel);
    }

    FieldInfo fieldInfo = fieldInfos.get(columnLabel);
    if (canShortCircuitFieldLookup(fieldInfo)) {
      ExpressionImpl.CachedRecord ret = readRecords[currPos][fieldInfo.tableOffset];
      if (ret != null) {
        Object obj = ret.getRecord().getFields()[fieldInfo.fieldOffset];
        return getInt(obj, fieldInfo.function);
      }
    }

    if (isMatchingAlias(columnLabel) && isCount) {
      return (int) count;
    }
    Integer ret = (Integer) getGroupByFunctionResults(columnLabel, DataType.Type.INTEGER);
    if (ret != null) {
      return ret;
    }

    String[] actualColumn = null;
    SelectFunctionImpl function = functionAliases.get(DatabaseClient.toLower(columnLabel));
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

    Object retObj = getField(actualColumn, columnLabel);

    fieldInfo = fieldInfos.get(columnLabel);
    if (fieldInfo != null) {
      fieldInfo.function = function;
    }

    return getInt(retObj, function);
  }

  private boolean isMatchingAlias(String columnLabel) {
    boolean matchingAlias = false;
    for (String alias : aliases.keySet()) {
      if (alias.equals(columnLabel)) {
        matchingAlias = true;
      }
    }
    return matchingAlias;
  }

  private Integer getInt(Object ret, SelectFunctionImpl function) {
    String retString = null;
    if (ret instanceof byte[]) {
      try {
        retString = new String((byte[]) ret, UTF8_STR);
      }
      catch (UnsupportedEncodingException e) {
        throw new DatabaseException(e);
      }
    }
    if (function != null) {
      if (retString != null && function.getName().equals(LENGTH_STR)) {
        return retString.length();
      }
      else if (function.getName().equalsIgnoreCase("min") ||
          function.getName().equalsIgnoreCase("max") ||
          function.getName().equalsIgnoreCase("sum") ||
          function.getName().equalsIgnoreCase("avg")) {
        Object obj = getCounterValue(function);
        if (obj instanceof Long) {
          return (int) (long) obj;
        }
        if (obj instanceof Double) {
          return (int) (double) obj;
        }
      }
    }
    if (retString != null) {
      return Integer.valueOf(retString);
    }
    if (ret instanceof Integer) {
      return (Integer) ret;
    }
    if (ret instanceof Long) {
      return (int) (long) ret;
    }
    if (ret instanceof Short) {
      return (int) (short) ret;
    }
    if (ret instanceof Byte) {
      return (int) (byte) ret;
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
    if (fieldInfo != null && currPos >= 0 && !isCount && groupByContext == null && !functionAliases.containsKey(fieldInfo.labelName)) {
      return true;
    }
    return false;
  }

  public Long getLong(String columnLabel) {
    if (recordsResults != null) {
      return cachedRecordResultAsRecord.getLong(columnLabel);
    }

    FieldInfo fieldInfo = fieldInfos.get(columnLabel);
    if (canShortCircuitFieldLookup(fieldInfo)) {
      ExpressionImpl.CachedRecord ret = readRecords[currPos][fieldInfo.tableOffset];
      if (ret != null) {
        Object obj = ret.getRecord().getFields()[fieldInfo.fieldOffset];
        return getLong(obj, fieldInfo.function);
      }
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
    Long ret = (Long) getGroupByFunctionResults(columnLabel, DataType.Type.BIGINT);
    if (ret != null) {
      return ret;
    }

    String[] actualColumn = null;
    SelectFunctionImpl function = functionAliases.get(DatabaseClient.toLower(columnLabel));
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
    Object retObj = getField(actualColumn, columnLabel);

    fieldInfo = fieldInfos.get(columnLabel);
    if (fieldInfo != null) {
      fieldInfo.function = function;
    }

    return getLong(retObj, function);
  }

  private Long getLong(Object ret, SelectFunctionImpl function) {
    String retString = null;
    if (ret instanceof byte[]) {
      try {
        retString = new String((byte[]) ret, UTF8_STR);
      }
      catch (UnsupportedEncodingException e) {
        throw new DatabaseException(e);
      }
    }
    if (function != null && groupByContext == null) {
      if (function.getName().equalsIgnoreCase(LENGTH_STR)) {
        return (long) retString.length();
      }
      else if (function.getName().equalsIgnoreCase("count") ||
          function.getName().equalsIgnoreCase("min") ||
          function.getName().equalsIgnoreCase("max") ||
          function.getName().equalsIgnoreCase("sum") ||
          function.getName().equalsIgnoreCase("avg")) {
        Object obj = getCounterValue(function);
        if (obj instanceof Long) {
          return (Long) obj;
        }
        if (obj instanceof Double) {
          return (long) (double) obj;
        }
      }
    }
    if (retString != null) {
      return Long.valueOf(retString);
    }

    if (ret instanceof Long) {
      return (Long) ret;
    }
    if (ret instanceof Integer) {
      return (long) (int) ret;
    }
    if (ret instanceof Short) {
      return (long) (short) ret;
    }
    if (ret instanceof Byte) {
      return (long) (byte) ret;
    }
    return null;
  }

  private Object getCounterValue(SelectFunctionImpl function) {
    if (function.getName().equalsIgnoreCase("count")) {
      if (counters != null) {
        for (Counter counter : counters) {
          if (counter.getLongCount() != null) {
            return counter.getCount();
          }
        }
      }
    }
    else if (function.getName().equalsIgnoreCase("min")) {
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
    }
    else if (function.getName().equalsIgnoreCase("max")) {
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
    }
    else if (function.getName().equalsIgnoreCase("avg")) {
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
    }
    else if (function.getName().equalsIgnoreCase("sum")) {
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
    }
    return 0L;
  }

  public Float getFloat(String columnLabel) {
    if (recordsResults != null) {
      return cachedRecordResultAsRecord.getFloat(columnLabel);
    }

    FieldInfo fieldInfo = fieldInfos.get(columnLabel);
    if (canShortCircuitFieldLookup(fieldInfo)) {
      ExpressionImpl.CachedRecord ret = readRecords[currPos][fieldInfo.tableOffset];
      if (ret != null) {
        Object obj = ret.getRecord().getFields()[fieldInfo.fieldOffset];
        return getFloat(obj, fieldInfo.function);
      }
    }

    if (isMatchingAlias(columnLabel) && isCount) {
      return (float) count;
    }

    Float ret = (Float) getGroupByFunctionResults(columnLabel, DataType.Type.FLOAT);
    if (ret != null) {
      return ret;
    }

    String[] actualColumn = null;
    SelectFunctionImpl function = functionAliases.get(DatabaseClient.toLower(columnLabel));
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

    Object retObj = getField(actualColumn, columnLabel);

    fieldInfo = fieldInfos.get(columnLabel);
    if (fieldInfo != null) {
      fieldInfo.function = function;
    }

    return getFloat(retObj, function);
  }

  private Float getFloat(Object ret, SelectFunctionImpl function) {
    String retString = null;
    if (ret instanceof byte[]) {
      try {
        retString = new String((byte[]) ret, UTF8_STR);
      }
      catch (UnsupportedEncodingException e) {
        throw new DatabaseException(e);
      }
    }
    if (function != null) {
      if (retString != null && function.getName().equalsIgnoreCase(LENGTH_STR)) {
        return (float) retString.length();
      }
      else if (function.getName().equalsIgnoreCase("min") ||
          function.getName().equalsIgnoreCase("max") ||
          function.getName().equalsIgnoreCase("sum") ||
          function.getName().equalsIgnoreCase("avg")) {
        Object obj = getCounterValue(function);
        if (obj instanceof Double) {
          return (float) (double) obj;
        }
        if (obj instanceof Long) {
          return (float) (long) obj;
        }
      }
    }

    if (retString != null) {
      return Float.valueOf(retString);
    }

    if (ret instanceof Float) {
      return (Float) ret;
    }
    if (ret instanceof Double) {
      return (float) (double) ret;
    }
    if (ret instanceof Long) {
      return (float) (long) ret;
    }
    if (ret instanceof Integer) {
      return (float) (int) ret;
    }
    if (ret instanceof Short) {
      return (float) (short) ret;
    }
    if (ret instanceof Byte) {
      return (float) (byte) ret;
    }
    return null;
  }

  public Double getDouble(String columnLabel) {
    if (recordsResults != null) {
      return cachedRecordResultAsRecord.getDouble(columnLabel);
    }

    FieldInfo fieldInfo = fieldInfos.get(columnLabel);
    if (canShortCircuitFieldLookup(fieldInfo)) {
      ExpressionImpl.CachedRecord ret = readRecords[currPos][fieldInfo.tableOffset];
      if (ret != null) {
        Object obj = ret.getRecord().getFields()[fieldInfo.fieldOffset];
        return getDouble(obj, fieldInfo.function);
      }
    }

    if (isMatchingAlias(columnLabel) && isCount) {
      return (double) count;
    }

    Double ret = (Double) getGroupByFunctionResults(columnLabel, DataType.Type.DOUBLE);
    if (ret != null) {
      return ret;
    }

    String[] actualColumn = null;
    SelectFunctionImpl function = functionAliases.get(DatabaseClient.toLower(columnLabel));
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

    Object retObj = getField(actualColumn, columnLabel);

    fieldInfo = fieldInfos.get(columnLabel);
    if (fieldInfo != null) {
      fieldInfo.function = function;
    }

    return getDouble(retObj, function);
  }

  private Double getDouble(Object ret, SelectFunctionImpl function) {
    String retString = null;
    if (ret instanceof byte[]) {
      try {
        retString = new String((byte[]) ret, UTF8_STR);
      }
      catch (UnsupportedEncodingException e) {
        throw new DatabaseException(e);
      }
    }
    if (function != null) {
      if (function.getName().equalsIgnoreCase(LENGTH_STR)) {
        return (double) retString.length();
      }
      else if (function.getName().equalsIgnoreCase("min") ||
          function.getName().equalsIgnoreCase("max") ||
          function.getName().equalsIgnoreCase("sum") ||
          function.getName().equalsIgnoreCase("avg")) {
        Object obj = getCounterValue(function);
        if (obj instanceof Double) {
          return (Double) obj;
        }
        if (obj instanceof Long) {
          return (double) (long) obj;
        }
      }
    }

    if (retString != null) {
      return Double.valueOf(retString);
    }

    if (ret instanceof Double) {
      return (Double) ret;
    }
    if (ret instanceof Float) {
      return (double) (float) ret;
    }
    if (ret instanceof Long) {
      return (double) (long) ret;
    }
    if (ret instanceof Integer) {
      return (double) (int) ret;
    }
    if (ret instanceof Short) {
      return (double) (short) ret;
    }
    if (ret instanceof Byte) {
      return (double) (byte) ret;
    }
    return null;
  }

  public BigDecimal getBigDecimal(String columnLabel, int scale) {
    if (recordsResults != null) {
      return cachedRecordResultAsRecord.getBigDecimal(columnLabel, scale);
    }

    FieldInfo fieldInfo = fieldInfos.get(columnLabel);
    if (canShortCircuitFieldLookup(fieldInfo)) {
      ExpressionImpl.CachedRecord ret = readRecords[currPos][fieldInfo.tableOffset];
      if (ret != null) {
        Object obj = ret.getRecord().getFields()[fieldInfo.fieldOffset];
        return getBigDecimal(obj, scale);
      }
    }

    String[] actualColumn = getActualColumn(columnLabel);
    Object ret = getField(actualColumn, columnLabel);

    return getBigDecimal(ret, scale);
  }

  private BigDecimal getBigDecimal(Object ret, int scale) {
    if (ret instanceof BigDecimal) {
      return (BigDecimal) ret;
    }
    if (ret instanceof Long) {
      return BigDecimal.valueOf((long) ret);
    }
    if (ret instanceof Integer) {
      return BigDecimal.valueOf((int) ret);
    }
    if (ret instanceof Short) {
      return BigDecimal.valueOf((short) ret);
    }
    if (ret instanceof Byte) {
      return BigDecimal.valueOf((byte) ret);
    }
    return null;
  }

  public byte[] getBytes(String columnLabel) {
    if (recordsResults != null) {
      return cachedRecordResultAsRecord.getBytes(columnLabel);
    }

    FieldInfo fieldInfo = fieldInfos.get(columnLabel);
    if (canShortCircuitFieldLookup(fieldInfo)) {
      ExpressionImpl.CachedRecord ret = readRecords[currPos][fieldInfo.tableOffset];
      if (ret != null) {
        Object obj = ret.getRecord().getFields()[fieldInfo.fieldOffset];
        if (obj instanceof Blob) {
          return ((Blob) obj).getData();
        }
        return (byte[]) obj;
      }
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
    if (canShortCircuitFieldLookup(fieldInfo)) {
      ExpressionImpl.CachedRecord ret = readRecords[currPos][fieldInfo.tableOffset];
      if (ret != null) {
        Object obj = ret.getRecord().getFields()[fieldInfo.fieldOffset];
        return (Date) obj;
      }
    }

    String[] actualColumn = getActualColumn(columnLabel);
    return (Date) getField(actualColumn, columnLabel);
  }

  public Time getTime(String columnLabel) {
    if (recordsResults != null) {
      return cachedRecordResultAsRecord.getTime(columnLabel);
    }

    FieldInfo fieldInfo = fieldInfos.get(columnLabel);
    if (canShortCircuitFieldLookup(fieldInfo)) {
      ExpressionImpl.CachedRecord ret = readRecords[currPos][fieldInfo.tableOffset];
      if (ret != null) {
        Object obj = ret.getRecord().getFields()[fieldInfo.fieldOffset];
        return (Time) obj;
      }
    }

    String[] actualColumn = getActualColumn(columnLabel);
    return (Time) getField(actualColumn, columnLabel);
  }

  public Timestamp getTimestamp(String columnLabel) {
    if (recordsResults != null) {
      return cachedRecordResultAsRecord.getTimestamp(columnLabel);
    }

    FieldInfo fieldInfo = fieldInfos.get(columnLabel);
    if (canShortCircuitFieldLookup(fieldInfo)) {
      ExpressionImpl.CachedRecord ret = readRecords[currPos][fieldInfo.tableOffset];
      if (ret != null) {
        Object obj = ret.getRecord().getFields()[fieldInfo.fieldOffset];
        return (Timestamp) obj;
      }
    }

    String[] actualColumn = getActualColumn(columnLabel);
    return (Timestamp) getField(actualColumn, columnLabel);
  }

  public InputStream getAsciiStream(String columnLabel) {
    FieldInfo fieldInfo = fieldInfos.get(columnLabel);
    if (canShortCircuitFieldLookup(fieldInfo)) {
      ExpressionImpl.CachedRecord ret = readRecords[currPos][fieldInfo.tableOffset];
      if (ret != null) {
        Object obj = ret.getRecord().getFields()[fieldInfo.fieldOffset];
        return (InputStream) obj;
      }
    }

    String[] actualColumn = getActualColumn(columnLabel);
    return (InputStream) getField(actualColumn, columnLabel);
  }

  public InputStream getUnicodeStream(String columnLabel) {
    FieldInfo fieldInfo = fieldInfos.get(columnLabel);
    if (canShortCircuitFieldLookup(fieldInfo)) {
      ExpressionImpl.CachedRecord ret = readRecords[currPos][fieldInfo.tableOffset];
      if (ret != null) {
        Object obj = ret.getRecord().getFields()[fieldInfo.fieldOffset];
        return (InputStream) obj;
      }
    }

    String[] actualColumn = getActualColumn(columnLabel);
    return (InputStream) getField(actualColumn, columnLabel);
  }

  public InputStream getBinaryStream(String columnLabel) {
    if (recordsResults != null) {
      return new ByteArrayInputStream(cachedRecordResultAsRecord.getBytes(columnLabel));
    }

    FieldInfo fieldInfo = fieldInfos.get(columnLabel);
    if (canShortCircuitFieldLookup(fieldInfo)) {
      ExpressionImpl.CachedRecord ret = readRecords[currPos][fieldInfo.tableOffset];
      if (ret != null) {
        Object obj = ret.getRecord().getFields()[fieldInfo.fieldOffset];
        if (obj == null) {
          return null;
        }
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
    if (canShortCircuitFieldLookup(fieldInfo)) {
      ExpressionImpl.CachedRecord ret = readRecords[currPos][fieldInfo.tableOffset];
      if (ret != null) {
        Object obj = ret.getRecord().getFields()[fieldInfo.fieldOffset];
        if (obj == null) {
          return null;
        }
        return new InputStreamReader(new ByteArrayInputStream((byte[]) obj));
      }
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
    if (canShortCircuitFieldLookup(fieldInfo)) {
      ExpressionImpl.CachedRecord ret = readRecords[currPos][fieldInfo.tableOffset];
      if (ret != null) {
        Object obj = ret.getRecord().getFields()[fieldInfo.fieldOffset];
        return (BigDecimal) obj;
      }
    }

    String[] actualColumn = getActualColumn(columnLabel);
    return (BigDecimal) getField(actualColumn, columnLabel);
  }

  @Override
  public Integer getInt(int columnIndex) {
    if (columnIndex == 1 && isCount) {
      return (int) count;
    }

    if (columnIndex == 1) {
      if (functionAliases != null && functionAliases.values() != null && functionAliases.values().size() != 0) {
        SelectFunctionImpl functionObj = functionAliases.values().iterator().next();
        if (functionObj != null) {
          if (functionObj.getName().equalsIgnoreCase("count") ||
              functionObj.getName().equalsIgnoreCase("min") ||
              functionObj.getName().equalsIgnoreCase("max") ||
              functionObj.getName().equalsIgnoreCase("sum") ||
              functionObj.getName().equalsIgnoreCase("avg")) {
            Object obj = getCounterValue(functionObj);
            if (obj instanceof Long) {
              return (int) (long) obj;
            }
            if (obj instanceof Double) {
              return (int) (double) obj;
            }
          }
        }
      }
    }

    List<ColumnImpl> columns = selectStatement.getSelectColumns();

    ColumnImpl column = columns.get(columnIndex - 1);
    SelectFunctionImpl function = functionAliases.get(DatabaseClient.toLower(column.getColumnName()));

    Object ret = getField(columnIndex);

    return getInt(ret, function);
  }

  public Object getField(int columnIndex) {
    List<ColumnImpl> columns = selectStatement.getSelectColumns();
    ColumnImpl column = columns.get(columnIndex - 1);
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

    if (columnIndex == 1) {
      if (functionAliases != null && functionAliases.values() != null && functionAliases.values().size() != 0) {
        SelectFunctionImpl functionObj = functionAliases.values().iterator().next();
        if (functionObj != null) {
          if (functionObj.getName().equalsIgnoreCase("count") ||
              functionObj.getName().equalsIgnoreCase("min") ||
              functionObj.getName().equalsIgnoreCase("max") ||
              functionObj.getName().equalsIgnoreCase("sum") ||
              functionObj.getName().equalsIgnoreCase("avg")) {
            Object obj = getCounterValue(functionObj);
            if (obj instanceof Long) {
              return (Long) obj;
            }
            if (obj instanceof Double) {
              return (long) (double) obj;
            }
          }
        }
      }
    }

    List<ColumnImpl> columns = selectStatement.getSelectColumns();
    ColumnImpl column = columns.get(columnIndex - 1);
    String columnName = column.getColumnName();
    SelectFunctionImpl function = functionAliases.get(DatabaseClient.toLower(column.getColumnName()));

    String[] actualColumn = getActualColumn(columnName);
    Object ret = getField(actualColumn, actualColumn[1]);
    return getLong(ret, function);
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) {
    List<ColumnImpl> columns = selectStatement.getSelectColumns();
    ColumnImpl column = columns.get(columnIndex - 1);
    String columnName = column.getColumnName();
    String[] actualColumn = getActualColumn(columnName);
    Object ret = getField(actualColumn, actualColumn[1]);
    return getBigDecimal(ret, -1);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) {
    List<ColumnImpl> columns = selectStatement.getSelectColumns();
    ColumnImpl column = columns.get(columnIndex - 1);
    String columnName = column.getColumnName();
    String[] actualColumn = getActualColumn(columnName);
    return (Timestamp) getField(actualColumn, actualColumn[1]);
  }

  @Override
  public Time getTime(int columnIndex) {
    List<ColumnImpl> columns = selectStatement.getSelectColumns();
    ColumnImpl column = columns.get(columnIndex - 1);
    String columnName = column.getColumnName();
    String[] actualColumn = getActualColumn(columnName);
    return (Time) getField(actualColumn, actualColumn[1]);
  }

  @Override
  public Date getDate(int columnIndex) {
    List<ColumnImpl> columns = selectStatement.getSelectColumns();
    ColumnImpl column = columns.get(columnIndex - 1);
    String columnName = column.getColumnName();
    String[] actualColumn = getActualColumn(columnName);
    return (Date) getField(actualColumn, actualColumn[1]);
  }

  @Override
  public byte[] getBytes(int columnIndex) {
    List<ColumnImpl> columns = selectStatement.getSelectColumns();
    ColumnImpl column = columns.get(columnIndex - 1);
    String columnName = column.getColumnName();
    String[] actualColumn = getActualColumn(columnName);
    Object ret = getField(actualColumn, actualColumn[1]);
    if (ret instanceof Blob) {
      return ((Blob) ret).getData();
    }
    return (byte[]) ret;
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale) {
    List<ColumnImpl> columns = selectStatement.getSelectColumns();
    ColumnImpl column = columns.get(columnIndex - 1);
    String columnName = column.getColumnName();
    String[] actualColumn = getActualColumn(columnName);
    Object ret = getField(actualColumn, actualColumn[1]);
    return getBigDecimal(ret, scale);
  }

  @Override
  public Double getDouble(int columnIndex) {
    if (columnIndex == 1 && this.isCount) {
      return (double) this.count;
    }

    if (columnIndex == 1) {
      if (functionAliases != null && functionAliases.values() != null && functionAliases.values().size() != 0) {
        SelectFunctionImpl functionObj = functionAliases.values().iterator().next();
        if (functionObj != null) {
          if (functionObj.getName().equalsIgnoreCase("count") ||
              functionObj.getName().equalsIgnoreCase("min") ||
              functionObj.getName().equalsIgnoreCase("max") ||
              functionObj.getName().equalsIgnoreCase("sum") ||
              functionObj.getName().equalsIgnoreCase("avg")) {
            Object obj = getCounterValue(functionObj);
            if (obj instanceof Long) {
              return (double) (long) obj;
            }
            if (obj instanceof Double) {
              return (double) (double) obj;
            }
          }
        }
      }
    }

    List<ColumnImpl> columns = selectStatement.getSelectColumns();
    ColumnImpl column = columns.get(columnIndex - 1);
    String columnLabel = column.getColumnName();
    if (isMatchingAlias(columnLabel) && isCount) {
      return (double) count;
    }
    String[] actualColumn = getActualColumn(columnLabel);
    Object ret = getField(actualColumn, columnLabel);

    SelectFunctionImpl function = functionAliases.get(DatabaseClient.toLower(columnLabel));
    return getDouble(ret, function);

  }

  @Override
  public Float getFloat(int columnIndex) {
    if (columnIndex == 1 && this.isCount) {
      return (float) this.count;
    }

    if (columnIndex == 1) {
      if (functionAliases != null && functionAliases.values() != null && functionAliases.values().size() != 0) {
        SelectFunctionImpl functionObj = functionAliases.values().iterator().next();
        if (functionObj != null) {
          if (functionObj.getName().equalsIgnoreCase("count") ||
              functionObj.getName().equalsIgnoreCase("min") ||
              functionObj.getName().equalsIgnoreCase("max") ||
              functionObj.getName().equalsIgnoreCase("sum") ||
              functionObj.getName().equalsIgnoreCase("avg")) {
            Object obj = getCounterValue(functionObj);
            if (obj instanceof Long) {
              return (float) (long) obj;
            }
            if (obj instanceof Double) {
              return (float) (double) obj;
            }
          }
        }
      }
    }

    List<ColumnImpl> columns = selectStatement.getSelectColumns();
    ColumnImpl column = columns.get(columnIndex - 1);
    String columnLabel = column.getColumnName();

    if (isMatchingAlias(columnLabel) && isCount) {
      return (float) count;
    }
    String[] actualColumn = getActualColumn(columnLabel);
    Object ret = getField(actualColumn, columnLabel);

    SelectFunctionImpl function = functionAliases.get(DatabaseClient.toLower(columnLabel));
    return getFloat(ret, function);
  }

  @Override
  public Short getShort(int columnIndex) {
    List<ColumnImpl> columns = selectStatement.getSelectColumns();
    ColumnImpl column = columns.get(columnIndex - 1);
    String columnName = column.getColumnName();
    String function = column.getFunction();
    String[] actualColumn = getActualColumn(columnName);
    Object ret = getField(actualColumn, actualColumn[1]);
    return getShort(ret, function);
  }

  @Override
  public Byte getByte(int columnIndex) {
    List<ColumnImpl> columns = selectStatement.getSelectColumns();
    ColumnImpl column = columns.get(columnIndex - 1);
    String columnName = column.getColumnName();
    String function = column.getFunction();
    String[] actualColumn = getActualColumn(columnName);
    Object ret = getField(actualColumn, actualColumn[1]);
    return getByte(ret, function);
  }

  @Override
  public Boolean getBoolean(int columnIndex) {
    List<ColumnImpl> columns = selectStatement.getSelectColumns();
    ColumnImpl column = columns.get(columnIndex - 1);
    String columnName = column.getColumnName();
    String[] actualColumn = getActualColumn(columnName);
    Object ret = getField(actualColumn, actualColumn[1]);
    return getBoolean(ret);
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) {
    List<ColumnImpl> columns = selectStatement.getSelectColumns();
    ColumnImpl column = columns.get(columnIndex - 1);
    String columnName = column.getColumnName();
    String[] actualColumn = getActualColumn(columnName);
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

    if (columnIndex == 1) {
      if (functionAliases != null && functionAliases.values() != null && functionAliases.values().size() != 0) {
        SelectFunctionImpl functionObj = functionAliases.values().iterator().next();
        if (functionObj != null) {
          if (functionObj.getName().equalsIgnoreCase("count") ||
              functionObj.getName().equalsIgnoreCase("min") ||
              functionObj.getName().equalsIgnoreCase("max") ||
              functionObj.getName().equalsIgnoreCase("sum") ||
              functionObj.getName().equalsIgnoreCase("avg")) {
            Object obj = getCounterValue(functionObj);
            if (obj instanceof Long) {
              return String.valueOf((Long) obj);
            }
            if (obj instanceof Double) {
              return String.valueOf((long) (double) obj);
            }
          }
        }
      }
    }
    if (describeStrs != null) {
      if (columnIndex == 1) {
        return describeStrs[(int) currPos];
      }
      return null;
    }
    List<ColumnImpl> columns = selectStatement.getSelectColumns();
    ColumnImpl column = columns.get(columnIndex - 1);
    String columnName = column.getColumnName();
    String function = column.getFunction();

    String[] actualColumn = getActualColumn(columnName);
    Object ret = getField(actualColumn, actualColumn[1]);

    if (function != null) {
      ExpressionList parms = column.getParameters();
      String retString = (String) ret;
      if (function.equals("upper")) {
        retString = retString.toUpperCase();
      }
      else if (function.equals("lower")) {
        retString = retString.toLowerCase();
      }
      else if (function.equals("substring")) {
        ExpressionList list = parms;
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

  private class OptimizationSettings {

    public BinaryExpressionImpl parentExpression;
    public ExpressionImpl lookupExpression;
    public OrderByExpressionImpl orderBy;
    public boolean isTableScan;
    public ColumnSettings leftColumn;
    public ColumnSettings rightColumn;
    public String fromTable;
    public boolean isTwoKeyLookup;
    public boolean ascend;
  }

  private class ColumnSettings {
    public String columnTableName;
    public String columnName;
    public DataType.Type columnType;
    public BinaryExpression.Operator operator;
    public Integer fieldOffset;
    public Object value;
  }

  private ArrayBlockingQueue<Object[]> blockKeys = new ArrayBlockingQueue<>(1000);
  private Thread probeThread;
  private AtomicBoolean doneProbing = new AtomicBoolean();
  final AtomicReference<Object[]> lastKey = new AtomicReference<>();
  private List<Object[]> probedKeys = new ArrayList<>();
  private boolean first = true;

  public void getMoreResults() {

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

        final ExpressionImpl topMostExpression = selectStatement.getExpression();


        boolean shouldOptimizeForThroughput = databaseClient.getCommon().getServersConfig().shouldOptimizeForThroughput();
        OptimizationSettings optimizationSettings = null;
        if (!shouldOptimizeForThroughput && counters == null && (selectStatement.getJoins() == null || selectStatement.getJoins().size() == 0) &&
            !selectStatement.isServerSelect() &&
            (topMostExpression instanceof BinaryExpressionImpl || topMostExpression instanceof AllRecordsExpressionImpl)) {
          selectStatement.getExpression().next((int) count, null, new AtomicLong(), limit, offset, false, true);
          ExpressionImpl binaryTopMost = topMostExpression;
          ExpressionImpl lookupExpression = findLookupExpression(binaryTopMost);
          BinaryExpressionImpl binaryLookupExpression = null;
          if (lookupExpression instanceof BinaryExpressionImpl) {
            binaryLookupExpression = (BinaryExpressionImpl) lookupExpression;
          }
          if (binaryTopMost instanceof AllRecordsExpressionImpl) {
            lookupExpression = binaryTopMost;
          }
          BinaryExpressionImpl parentExpression = null;
          if (lookupExpression != null) {
            parentExpression = findExpressionParent(lookupExpression, topMostExpression);

            if (lookupExpression instanceof AllRecordsExpressionImpl) {
              OptimizationSettings settings = optimizationSettings = new OptimizationSettings();
              settings.fromTable = selectStatement.getFromTable();
              TableSchema tableSchema = databaseClient.getCommon().getTables(dbName).get(settings.fromTable);
              boolean found = false;
              for (Map.Entry<String, IndexSchema> indexSchema : tableSchema.getIndices().entrySet()) {
                if (indexSchema.getValue().isPrimaryKey()) {
                  String[] fields = indexSchema.getValue().getFields();
                  settings.leftColumn = new ColumnSettings();
                  settings.leftColumn.columnName = fields[0];
                  settings.leftColumn.columnTableName = settings.fromTable;
                  int offset = tableSchema.getFieldOffset(fields[0]);
                  FieldSchema fieldSchema = tableSchema.getFields().get(offset);
                  settings.leftColumn.columnType = fieldSchema.getType();
                  settings.leftColumn.fieldOffset = tableSchema.getFieldOffset(settings.leftColumn.columnName);
                  found = true;
                  break;
                }
              }
              if (!found) {
                settings = null;
              }
              else {
                settings.leftColumn.operator = BinaryExpression.Operator.greaterEqual;
              }
            }
            else if (binaryLookupExpression.isOneKeyLookup()) {
              if (binaryLookupExpression.getLeftExpression() instanceof ConstantImpl ||
                  binaryLookupExpression.getLeftExpression() instanceof ParameterImpl) {
                Object value = ExpressionImpl.getValueFromExpression(selectStatement.getParms(), binaryLookupExpression.getLeftExpression());
                if (binaryLookupExpression.getRightExpression() instanceof ColumnImpl) {
                  optimizationSettings = getOptimizationSettings(binaryLookupExpression, binaryLookupExpression.getRightExpression(), value);
                }
              }
              else if (binaryLookupExpression.getRightExpression() instanceof ConstantImpl ||
                  binaryLookupExpression.getRightExpression() instanceof ParameterImpl) {
                Object value = ExpressionImpl.getValueFromExpression(selectStatement.getParms(), binaryLookupExpression.getRightExpression());
                if (binaryLookupExpression.getLeftExpression() instanceof ColumnImpl) {
                  optimizationSettings = getOptimizationSettings(binaryLookupExpression, binaryLookupExpression.getLeftExpression(), value);
                }
              }
            }
            else if (binaryLookupExpression.isTwoKeyLookup()) {
              optimizationSettings = getOptimizationSettingsForTwoKeyLookup(binaryLookupExpression);
            }
            else if (binaryLookupExpression.isTableScan()) {
              OptimizationSettings settings = optimizationSettings = new OptimizationSettings();
              settings.isTableScan = true;
              settings.fromTable = selectStatement.getFromTable();
              TableSchema tableSchema = databaseClient.getCommon().getTables(dbName).get(settings.fromTable);
              boolean found = false;
              for (Map.Entry<String, IndexSchema> indexSchema : tableSchema.getIndices().entrySet()) {
                if (indexSchema.getValue().isPrimaryKey()) {
                  String[] fields = indexSchema.getValue().getFields();
                  settings.leftColumn = new ColumnSettings();
                  settings.leftColumn.columnName = fields[0];
                  settings.leftColumn.columnTableName = settings.fromTable;
                  int offset = tableSchema.getFieldOffset(fields[0]);
                  FieldSchema fieldSchema = tableSchema.getFields().get(offset);
                  settings.leftColumn.columnType = fieldSchema.getType();
                  settings.leftColumn.fieldOffset = tableSchema.getFieldOffset(settings.leftColumn.columnName);
                  found = true;
                  break;
                }
              }
              if (!found) {
                settings = null;
              }
              else {
                settings.leftColumn.operator = BinaryExpression.Operator.greaterEqual;
              }
            }
          }

          if (optimizationSettings != null) {
            optimizationSettings.parentExpression = parentExpression;
            optimizationSettings.lookupExpression = lookupExpression;

            optimizationSettings.ascend = true;
            List<OrderByExpressionImpl> orderBy = selectStatement.getOrderByExpressions();
            if (orderBy != null && orderBy.size() != 0) {
              OrderByExpressionImpl order = orderBy.get(0);
              optimizationSettings.orderBy = order;
            }
            if (optimizationSettings.orderBy != null) {
              if (optimizationSettings.orderBy.getColumnName().equals(optimizationSettings.leftColumn.columnName) &&
                  (optimizationSettings.orderBy.getTableName() == null || optimizationSettings.orderBy.getTableName().equals(optimizationSettings.leftColumn.columnTableName))) {
                if (!optimizationSettings.orderBy.isAscending()) {
                  optimizationSettings.ascend = false;
                }
              }
            }
          }
        }

        if (optimizationSettings != null) {
          final OptimizationSettings settings = optimizationSettings;
          List<Future> futures = new ArrayList<>();
          final byte[] selectBytes = selectStatement.serialize();
          if (probeThread == null) {
            probeThread = new Thread(new Runnable() {
              @Override
              public void run() {
                SelectStatementImpl selectStatement = new SelectStatementImpl(databaseClient);
                selectStatement.deserialize(selectBytes, dbName);
                //outer.setRecordCache(recordCache);

//                if (settings.isTableScan) {
//                  AllRecordsExpressionImpl allExpression = new AllRecordsExpressionImpl();
//                  allExpression.setTableName(selectStatement.getFromTable());
//                  allExpression.setFromTable(selectStatement.getFromTable());
//                  allExpression.setClient(databaseClient);
//                  allExpression.setViewVersion(databaseClient.getCommon().getSchemaVersion());
//                  allExpression.setRecordCache(recordCache);
//                  allExpression.setOrderByExpressions(selectStatement.getOrderByExpressions());
//                  allExpression.setDbName(dbName);
//                  allExpression.setParms(selectStatement.getParms());
//
//                  selectStatement.setExpression(allExpression);
//                }
                selectStatement.setRecordCache(recordCache);
                selectStatement.setPageSize(10);
                selectStatement.setProbe(true);
                selectStatement.setServerSelectPageNumber(-1);
                selectStatement.setServerSelectShardNumber(-1);
                selectStatement.setServerSelectReplicaNumber(-1);
                selectStatement.setServerSelectResultSetId(-1);
//                if (selectStatement.isServerSelect()) {
//                  //selectStatement.setIsOnServer(true);
//                  selectStatement.forceSelectOnServer(false);
//                }
                ExpressionImpl outer = selectStatement.getExpression();
                outer.setTableName(selectStatement.getFromTable());
                outer.setClient(databaseClient);
                outer.setViewVersion(databaseClient.getCommon().getSchemaVersion());

                int iteration = 0;
                while (true) {
                  int pageSize = 10 + (iteration++ * 20);
                  selectStatement.setPageSize(pageSize);
                  selectStatement.setColumns(new ArrayList<ColumnImpl>());

                  ExpressionImpl.NextReturn ids = selectStatement.next(dbName, null,
                      restrictToThisServer, procedureContext);
                  if (ids == null || ids.getKeys() == null || ids.getKeys().length == 0) {
                    break;
                  }
                  for (Object[][] key : ids.getKeys()) {
                    ExpressionImpl.CachedRecord record = outer.getRecordCache().get(settings.leftColumn.columnTableName, key[0]);
                    if (lastKey.get() != null) {
                      synchronized (probedKeys) {
                        probedKeys.add(lastKey.get());
                      }
                    }
                    if (record != null && record.getRecord() != null) {
                      Object value = record.getRecord().getFields()[settings.leftColumn.fieldOffset];
                      lastKey.set(new Object[]{value});
                    }
                  }
                }
                doneProbing.set(true);

              }
            });
            probeThread.start();


            Object[] key = null;
            while (true) {
              synchronized (probedKeys) {
                if (probedKeys.size() != 0) {
                  key = probedKeys.get(0);
                }
              }
              if (key != null) {
                break;
              }
              if (doneProbing.get()) {
                synchronized (probedKeys) {
                  if (probedKeys.size() != 0) {
                    key = probedKeys.get(0);
                  }
                }
                if (key == null) {
                  break;
                }
              }
              try {
                Thread.sleep(5);
              }
              catch (InterruptedException e) {
                throw new DatabaseException(e);
              }
            }
            //final Object[] nextKey = probedKeys.get(0);

            final Object[] finalKey = key;
            futures.add(databaseClient.getExecutor().submit(new Callable() {
              @Override
              public Object call() throws Exception {
                try {
                  SelectStatementImpl selectStatement = new SelectStatementImpl(databaseClient);
                  selectStatement.deserialize(selectBytes, dbName);
                  selectStatement.setRecordCache(recordCache);
                  selectStatement.setPageSize(100_000);

                  BinaryExpressionImpl outer = null;

                  Object[] value = getValueForExpression(settings);

                  if (settings.isTwoKeyLookup) {
                    if (finalKey == null) {
                      if (!settings.ascend) {
                        outer = createExpressionForKeys(value, new Object[]{settings.leftColumn.value},
                            BinaryExpression.Operator.less, settings.leftColumn.operator, settings);
                      }
                      else {
                        outer = createExpressionForKeys(value, new Object[]{settings.rightColumn.value},
                            BinaryExpression.Operator.greater, settings.rightColumn.operator, settings);
                      }
                    }
                    else {
                      if (!settings.ascend) {
                        outer = createExpressionForKeys(finalKey, value,
                            BinaryExpression.Operator.greater/*settings.leftColumn.operator*/, BinaryExpression.Operator.less, settings);
                      }
                      else {
                        outer = createExpressionForKeys(value, finalKey, BinaryExpression.Operator.greater,
                            BinaryExpression.Operator.less, settings);
                      }
                    }
                  }
                  else {
                    if (finalKey == null) {
                      if (settings.leftColumn.operator == BinaryExpression.Operator.less || settings.leftColumn.operator == BinaryExpression.Operator.lessEqual) {
                        if (!settings.ascend) {
                          outer = createExpressionForSingleKey(selectStatement, value, BinaryExpression.Operator.less, settings);
                        }
                        else {
                          outer = createExpressionForKeys(value, new Object[]{settings.leftColumn.value},
                              BinaryExpression.Operator.greater, settings.leftColumn.operator, settings);
                        }
                      }
                      else {
                        outer = createExpressionForSingleKey(selectStatement, value, BinaryExpression.Operator.greater, settings);
                      }
                    }
                    else {
                      if (!settings.ascend) {
                        outer = createExpressionForKeys(finalKey, value, BinaryExpression.Operator.greater,
                            BinaryExpression.Operator.less, settings);
                      }
                      else {
                        outer = createExpressionForKeys(value, finalKey, BinaryExpression.Operator.greater,
                            BinaryExpression.Operator.less, settings);
                      }
                    }
                  }

                  if (settings.isTableScan) {
                    BinaryExpressionImpl parent = new BinaryExpressionImpl();
                    parent.setLeftExpression(outer);
                    parent.setRightExpression(selectStatement.getExpression());
                    parent.setOperator(BinaryExpression.Operator.and);
                    selectStatement.setExpression(parent);
                  }
                  else {
                    if (settings.parentExpression == null) {
                      selectStatement.setExpression(outer);
                    }
                    else {
                      ExpressionImpl topMostExpression = (ExpressionImpl) selectStatement.getExpression().getTopLevelExpression();
                      BinaryExpressionImpl lookupExpression = findLookupExpression(topMostExpression);
                      BinaryExpressionImpl parentExpression = findExpressionParent(lookupExpression, topMostExpression);
                      if (parentExpression.getLeftExpression() == lookupExpression) {
                        parentExpression.setLeftExpression(outer);
                      }
                      else if (parentExpression.getRightExpression() == lookupExpression) {
                        parentExpression.setRightExpression(outer);
                      }
                      else {
                        throw new DatabaseException("Can't find expression for replacement");
                      }
                    }
                  }

                  ExpressionImpl expression = selectStatement.getExpression();
                  expression.setTableName(selectStatement.getFromTable());
                  expression.setClient(databaseClient);
                  expression.setViewVersion(databaseClient.getCommon().getSchemaVersion());
                  expression.setRecordCache(recordCache);
                  expression.setOrderByExpressions(selectStatement.getOrderByExpressions());
                  expression.setTopLevelExpression(expression);
                  expression.setParms(selectStatement.getParms());

                  selectStatement.setColumns(new ArrayList<ColumnImpl>());
                  ExpressionImpl.NextReturn ids = selectStatement.next(dbName, null,
                      restrictToThisServer, procedureContext);
                  if (ids == null || ids.getKeys() == null || ids.getKeys().length == 0) {
                    return null;
                  }
                  return ids;
                }
                catch (Exception e) {
                  throw new DatabaseException(e);
                }
              }
            }));
          }


          outer:
          for (int i = 0; i < 8; i++) {
            boolean isLastKey = false;
            Object[] key = null;
            Object[] nextKey = null;
            while (true) {
              synchronized (probedKeys) {
                if (probedKeys.size() != 0) {
                  key = probedKeys.remove(0);
                  if (probedKeys.size() != 0) {
                    nextKey = probedKeys.get(0);
                  }
                }
              }
              if (key != null) {
                if (nextKey == null) {
                  if (doneProbing.get()) {
                    nextKey = lastKey.get();
                    lastKey.set(null);
                    isLastKey = true;
                  }
                  else {
                    while (true) {
                      if (probedKeys.size() != 0) {
                        nextKey = probedKeys.get(0);
                        if (nextKey != null) {
                          break;
                        }
                      }
                      try {
                        Thread.sleep(5);
                      }
                      catch (InterruptedException e) {
                        throw new DatabaseException(e);
                      }
                    }
                  }
                }
                break;
              }
              if (doneProbing.get()) {
                synchronized (probedKeys) {
                  if (probedKeys.size() != 0) {
                    key = probedKeys.remove(0);
                    if (probedKeys.size() != 0) {
                      nextKey = probedKeys.get(0);
                    }
                    if (key != null) {
                      if (nextKey == null) {
                        nextKey = lastKey.get();
                        lastKey.set(null);
                        isLastKey = true;
                      }
                      break;
                    }

                  }
                  if (key == null) {
                    key = lastKey.get();
                    lastKey.set(null);
                    if (key == null) {
                      break outer;
                    }
                    else {
                      isLastKey = true;
                      break;
                    }
                  }
                }
              }
              try {
                Thread.sleep(5);
              }
              catch (InterruptedException e) {
                throw new DatabaseException(e);
              }
            }

            final boolean finalIsLastKey = isLastKey;
            final Object[] finalKey = key;
            final Object[] finalNextKey = nextKey;
            futures.add(databaseClient.getExecutor().submit(new Callable() {
              @Override
              public Object call() throws Exception {
                SelectStatementImpl selectStatement = new SelectStatementImpl(databaseClient);
                selectStatement.deserialize(selectBytes, dbName);
                selectStatement.setRecordCache(recordCache);
                selectStatement.setPageSize(100_000);

                BinaryExpressionImpl outer = null;
                if (settings.isTwoKeyLookup) {
                  if (finalIsLastKey || finalNextKey == null) {
                    if (settings.leftColumn.operator == BinaryExpression.Operator.less ||
                        settings.leftColumn.operator == BinaryExpression.Operator.lessEqual) {
                      //todo: not tested
                      outer = createExpressionForKeys(finalKey, new Object[]{settings.leftColumn.value},
                          BinaryExpression.Operator.greaterEqual, settings.leftColumn.operator, settings);
                    }
                    else {
                      boolean handled = false;
                      if (!settings.ascend) {
                        outer = createExpressionForKeys(finalKey, new Object[]{settings.leftColumn.value},
                            BinaryExpression.Operator.lessEqual, settings.leftColumn.operator, settings);
                        handled = true;
                      }
                      if (!handled) {
                        outer = createExpressionForKeys(finalKey, new Object[]{settings.rightColumn.value}, BinaryExpression.Operator.greaterEqual,
                            settings.rightColumn.operator, settings);
                      }
                    }
                  }
                  else {
                    if (!settings.ascend) {
                      outer = createExpressionForKeys(finalKey, finalNextKey,
                          BinaryExpression.Operator.lessEqual, BinaryExpression.Operator.greater, settings);
                    }
                    else {
                      outer = createExpressionForKeys(finalKey, finalNextKey, BinaryExpression.Operator.greaterEqual,
                          BinaryExpression.Operator.less, settings);
                    }
                  }
                }
                else {
                  if (finalIsLastKey || finalNextKey == null) {
                    if (settings.leftColumn.operator == BinaryExpression.Operator.less ||
                        settings.leftColumn.operator == BinaryExpression.Operator.lessEqual) {
                      outer = createExpressionForKeys(finalKey, new Object[]{settings.leftColumn.value},
                          BinaryExpression.Operator.greaterEqual, settings.leftColumn.operator, settings);
                    }
                    else {
                      boolean handled = false;
                      if (!settings.ascend) {
                        outer = createExpressionForKeys(finalKey, new Object[]{settings.leftColumn.value},
                            BinaryExpression.Operator.lessEqual, settings.leftColumn.operator, settings);
                        handled = true;
                      }
                      if (!handled) {
                        outer = createExpressionForSingleKey(selectStatement, finalKey, BinaryExpression.Operator.greaterEqual, settings);
                      }
                    }
                  }
                  else {
                    if (!settings.ascend) {
                      outer = createExpressionForKeys(finalKey, finalNextKey,
                          BinaryExpression.Operator.lessEqual, BinaryExpression.Operator.greater, settings);
                    }
                    else {
                      outer = createExpressionForKeys(finalKey, finalNextKey, BinaryExpression.Operator.greaterEqual,
                          BinaryExpression.Operator.less, settings);
                    }
                  }
                }
                if (settings.isTableScan) {
                  BinaryExpressionImpl parent = new BinaryExpressionImpl();
                  parent.setLeftExpression(outer);
                  parent.setRightExpression(selectStatement.getExpression());
                  parent.setOperator(BinaryExpression.Operator.and);
                  selectStatement.setExpression(parent);
                }
                else {
                  if (settings.parentExpression == null) {
                    selectStatement.setExpression(outer);
                  }
                  else {
                    ExpressionImpl topMostExpression = (ExpressionImpl) selectStatement.getExpression().getTopLevelExpression();
                    BinaryExpressionImpl lookupExpression = findLookupExpression(topMostExpression);
                    BinaryExpressionImpl parentExpression = findExpressionParent(lookupExpression, topMostExpression);
                    if (parentExpression.getLeftExpression() == lookupExpression) {
                      parentExpression.setLeftExpression(outer);
                    }
                    else if (parentExpression.getRightExpression() == lookupExpression) {
                      parentExpression.setRightExpression(outer);
                    }
                    else {
                      throw new DatabaseException("Can't find expression for replacement");
                    }
                  }
                }

                ExpressionImpl expression = selectStatement.getExpression();
                expression.setTableName(selectStatement.getFromTable());
                expression.setClient(databaseClient);
                expression.setViewVersion(databaseClient.getCommon().getSchemaVersion());
                expression.setRecordCache(recordCache);
                expression.setOrderByExpressions(selectStatement.getOrderByExpressions());
                expression.setTopLevelExpression(expression);
                expression.setParms(selectStatement.getParms());

                selectStatement.setColumns(new ArrayList<ColumnImpl>());
                ExpressionImpl.NextReturn ids = selectStatement.next(dbName, null,
                    restrictToThisServer, procedureContext);
                if (ids == null || ids.getKeys() == null || ids.getKeys().length == 0) {
                  return null;
                }
                return ids;
              }
            }));
          }
          ExpressionImpl.NextReturn allIds = null;
          for (Future future : futures) {
            try {
              ExpressionImpl.NextReturn ids = (ExpressionImpl.NextReturn) future.get();
              if (allIds == null || allIds.getKeys() == null || allIds.getKeys().length == 0) {
                allIds = ids;
              }
              else if (ids != null && ids.getKeys() != null && ids.getKeys().length != 0) {
                Object[][][] keys = ids.getKeys();
                if (keys != null) {
                  Object[][][] newKeys = new Object[keys.length + allIds.getKeys().length][][];
                  System.arraycopy(allIds.getKeys(), 0, newKeys, 0, allIds.getKeys().length);
                  System.arraycopy(keys, 0, newKeys, allIds.getKeys().length, keys.length);
                  keys = newKeys;
                  allIds.setIds(keys);
                }
              }
            }
            catch (Exception e) {
              throw new DatabaseException(e);
            }
          }
          if (allIds != null && allIds.getIds() != null) {
            selectStatement.applyDistinct(dbName, selectContext.getTableNames(), allIds, uniqueRecords);
          }
          readRecords = readRecords(allIds);
          if (allIds != null && allIds.getIds() != null && allIds.getIds().length != 0) {
            selectContext.setCurrKeys(allIds.getKeys());
          }
          else {
            selectContext.setCurrKeys((Object[][][]) null);
          }
        }
        else {
          selectStatement.setPageSize(pageSize);
          ExpressionImpl.NextReturn ids = selectStatement.next(dbName, null,
              selectContext.isRestrictToThisServer(), selectContext.getProcedureContext());
          if (ids != null && ids.getIds() != null) {
            selectStatement.applyDistinct(dbName, selectContext.getTableNames(), ids, uniqueRecords);
          }

          readRecords = readRecords(ids);
          if (ids != null && ids.getIds() != null && ids.getIds().length != 0) {
            selectContext.setCurrKeys(ids.getKeys());
          }
          else {
            selectContext.setCurrKeys((Object[][][]) null);
          }
        }
        currPos = 0;
      }
      return;
    }

    if (selectContext.getNextShard() == -1)

    {
      selectContext.setCurrKeys((Object[][][]) null);
      currPos = 0;
      return;
    }
//    while (true) {
//
//      //todo: support multiple tables
//      TableSchema tableSchema = databaseClient.getCommon().getTables().get(selectContext.getTableNames()[0]);
//      Random rand = new Random(System.currentTimeMillis());
//      String command = "DatabaseServer:indexLookup:1:" + databaseClient.getCommon().getSchemaVersion() + ":" + rand.nextLong() + ":query0";
//      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
//      DataOutputStream out = new DataOutputStream(bytesOut);
//      out.writeUTF(tableSchema.getName());
//      out.writeUTF(selectContext.getIndexName());
//      Boolean ascending = null;
//      if (selectStatement.getOrderByExpressions() == null) {
//        out.writeInt(0);
//      }
//      else {
//        out.writeInt(selectStatement.getOrderByExpressions().size());
//        for (int j = 0; j < selectStatement.getOrderByExpressions().size(); j++) {
//          OrderByExpressionImpl expression = selectStatement.getOrderByExpressions().get(j);
//          if (expression.getColumnName().equals(tableSchema.getIndices().get(selectContext.getIndexName()).getFields()[0])) {
//            ascending = expression.isAscending();
//          }
//          expression.serialize(out);
//        }
//      }
//      if (selectContext.getNextKey() == null) {
//        out.writeBoolean(false);
//      }
//      else {
//        out.writeBoolean(true);
//        out.write(DatabaseCommon.serializeKey(tableSchema, selectContext.getIndexName(), selectContext.getNextKey()));
//      }
//      if (selectContext.getOperator() == BinaryExpression.Operator.greater) {
//        selectContext.setOperator(BinaryExpression.Operator.greaterEqual);
//      }
//      if (selectContext.getOperator() == BinaryExpression.Operator.less) {
//        selectContext.setOperator(BinaryExpression.Operator.lessEqual);
//      }
//      out.writeInt(selectContext.getOperator().getId());
//      out.close();
//
//      AtomicReference<String> selectedHost = new AtomicReference<>();
//
//      int previousSchemaVersion = databaseClient.getCommon().getSchemaVersion();
//      byte[] lookupRet = databaseClient.send(selectContext.getNextShard(), rand.nextLong(), command, bytesOut.toByteArray(), DatabaseClient.Replica.def, 30000, selectedHost);
//      if (previousSchemaVersion < databaseClient.getCommon().getSchemaVersion()) {
//        throw new SchemaOutOfSyncException();
//      }
//      ByteArrayInputStream bytes = new ByteArrayInputStream(lookupRet);
//      DataInputStream in = new DataInputStream(bytes);
//      int serializationVersion = in.readInt();
//      if (in.readBoolean()) {
//        selectContext.setNextKey(DatabaseCommon.deserializeKey(tableSchema, in));
//      }
//      else {
//        selectContext.setNextKey(null);
//        selectContext.setNextShard(selectContext.getNextShard() + (ascending == null || ascending ? 1 : -1));
//      }
//      int count = in.readInt();
//      long[] currRet = new long[count];
//      for (int k = 0; k < count; k++) {
//        currRet[k] = in.readLong();
//      }
//      selectContext.setCurrKeys(currRet);
//
//      currPos = 0;
//      if (count != 0 || (ascending == null || ascending ? selectContext.getNextShard() > databaseClient.getShardCount() :
//          selectContext.getNextShard() < 0)) {
//        break;
//      }
//    }
  }

  private Object[] getValueForExpression(ResultSetImpl.OptimizationSettings settings) {
    String[] fields = getIndexFields(settings);
    ExpressionImpl.CachedRecord[] record = ResultSetImpl.this.lastReadRecords[ResultSetImpl.this.lastReadRecords.length - 1];
    Object[] ret = new Object[fields.length];
    for (int i = 0; i < ret.length; i++) {
      ret[i] = record[0].getRecord().getField(fields[i]);
    }
    return ret;
  }

  private String[] getIndexFields(ResultSetImpl.OptimizationSettings settings) {
    TableSchema tableSchema = databaseClient.getCommon().getTables(dbName).get(settings.fromTable);
    String[] fields = null;
    for (Map.Entry<String, IndexSchema> entry : tableSchema.getIndices().entrySet()) {
      if (entry.getValue().getFields()[0].equals(settings.leftColumn.columnName)) {
        fields = entry.getValue().getFields();
        break;
      }
    }
    return fields;
  }

  private OptimizationSettings getOptimizationSettingsForTwoKeyLookup(BinaryExpressionImpl lookupExpression) {
    if (lookupExpression.getOperator() != BinaryExpression.Operator.and) {
      return null;
    }
    OptimizationSettings settings = new OptimizationSettings();

    settings.leftColumn = new ResultSetImpl.ColumnSettings();
    settings.rightColumn = new ResultSetImpl.ColumnSettings();

    BinaryExpressionImpl leftExpression = (BinaryExpressionImpl) lookupExpression.getLeftExpression();
    if (leftExpression.getOperator() == BinaryExpression.Operator.greater || leftExpression.getOperator() == BinaryExpression.Operator.greaterEqual) {
      getTwoKeySettingsForOneSideExpression(lookupExpression, settings, settings.leftColumn, settings.rightColumn);
    }
    else {
      getTwoKeySettingsForOneSideExpression(lookupExpression, settings, settings.rightColumn, settings.leftColumn);
    }

    settings.isTwoKeyLookup = true;

    return settings;
  }

  private void getTwoKeySettingsForOneSideExpression(BinaryExpressionImpl lookupExpression, OptimizationSettings
      settings,
                                                     ColumnSettings greater, ColumnSettings less) {
    BinaryExpressionImpl leftExpression = (BinaryExpressionImpl) lookupExpression.getLeftExpression();
    BinaryExpressionImpl rightExpression = (BinaryExpressionImpl) lookupExpression.getRightExpression();

    if (leftExpression.getLeftExpression() instanceof ConstantImpl ||
        leftExpression.getLeftExpression() instanceof ParameterImpl) {
      greater.value = ExpressionImpl.getValueFromExpression(selectStatement.getParms(), leftExpression.getLeftExpression());
      getColumnSettings(settings, greater, leftExpression.getRightExpression(), leftExpression.getOperator());
    }
    else {
      greater.value = ExpressionImpl.getValueFromExpression(selectStatement.getParms(), leftExpression.getRightExpression());
      getColumnSettings(settings, greater, leftExpression.getLeftExpression(), leftExpression.getOperator());
    }

    if (rightExpression.getLeftExpression() instanceof ConstantImpl ||
        rightExpression.getLeftExpression() instanceof ParameterImpl) {
      less.value = ExpressionImpl.getValueFromExpression(selectStatement.getParms(), rightExpression.getLeftExpression());
      getColumnSettings(settings, less, rightExpression.getRightExpression(), rightExpression.getOperator());
    }
    else {
      less.value = ExpressionImpl.getValueFromExpression(selectStatement.getParms(), rightExpression.getRightExpression());
      getColumnSettings(settings, less, rightExpression.getLeftExpression(), rightExpression.getOperator());
    }
  }

  private void getColumnSettings(OptimizationSettings settings, ColumnSettings columnSettings,
                                 ExpressionImpl columnExpression, BinaryExpression.Operator operator2) {
    ColumnImpl column = (ColumnImpl) columnExpression;
    String tableName = column.getTableName();
    if (tableName == null) {
      tableName = selectStatement.getFromTable();
    }
    String columnName = column.getColumnName();
    TableSchema tableSchema = databaseClient.getCommon().getTables(dbName).get(tableName);
    int fieldOffset = tableSchema.getFieldOffset(columnName);
    FieldSchema fieldSchema = tableSchema.getFields().get(fieldOffset);
    DataType.Type type = fieldSchema.getType();
    BinaryExpression.Operator operator = operator2;
    settings.fromTable = selectStatement.getFromTable();
    columnSettings.columnTableName = tableName;
    columnSettings.columnName = columnName;
    columnSettings.columnType = type;
    columnSettings.operator = operator;
    columnSettings.fieldOffset = tableSchema.getFieldOffset(columnName);
  }

  private OptimizationSettings getOptimizationSettings(BinaryExpressionImpl lookupExpression, ExpressionImpl
      columnExpression, Object value) {
    OptimizationSettings settings = new OptimizationSettings();
    ColumnImpl column = (ColumnImpl) columnExpression;
    String tableName = column.getTableName();
    if (tableName == null) {
      tableName = selectStatement.getFromTable();
    }
    String columnName = column.getColumnName();
    TableSchema tableSchema = databaseClient.getCommon().getTables(dbName).get(tableName);
    int fieldOffset = tableSchema.getFieldOffset(columnName);
    FieldSchema fieldSchema = tableSchema.getFields().get(fieldOffset);
    DataType.Type type = fieldSchema.getType();
    BinaryExpression.Operator operator = lookupExpression.getOperator();
    settings.leftColumn = new ColumnSettings();
    settings.fromTable = selectStatement.getFromTable();
    settings.leftColumn.columnTableName = tableName;
    settings.leftColumn.columnName = columnName;
    settings.leftColumn.columnType = type;
    settings.leftColumn.operator = operator;
    settings.leftColumn.value = value;
    settings.leftColumn.fieldOffset = tableSchema.getFieldOffset(columnName);
    return settings;
  }

  private BinaryExpressionImpl findExpressionParent(ExpressionImpl expression, ExpressionImpl currExpression) {
    if (!(expression instanceof BinaryExpressionImpl) || !(currExpression instanceof BinaryExpressionImpl)) {
      return null;
    }
    BinaryExpressionImpl binary = (BinaryExpressionImpl) currExpression;
    if (expression == binary.getLeftExpression()) {
      return binary;
    }
    if (expression == binary.getRightExpression()) {
      return binary;
    }
    BinaryExpressionImpl ret = findExpressionParent(expression, binary.getLeftExpression());
    if (ret != null) {
      return ret;
    }
    ret = findExpressionParent(expression, binary.getRightExpression());
    if (ret != null) {
      return ret;
    }
    return null;
  }

  private BinaryExpressionImpl findLookupExpression(ExpressionImpl expression) {
    if (!(expression instanceof BinaryExpressionImpl)) {
      return null;
    }
    BinaryExpressionImpl binary = (BinaryExpressionImpl) expression;
    if (binary.isOneKeyLookup() || binary.isTwoKeyLookup() || binary.isTableScan()) {
      return binary;
    }
    BinaryExpressionImpl ret = findLookupExpression((ExpressionImpl) binary.getLeftExpression());
    if (ret != null) {
      return ret;
    }
    ret = findLookupExpression((ExpressionImpl) binary.getRightExpression());
    if (ret != null) {
      return ret;
    }
    return null;
  }

  private BinaryExpressionImpl createExpressionForSingleKey(SelectStatementImpl selectStatement, Object[] value,
                                                            BinaryExpression.Operator greater, OptimizationSettings settings) {
    BinaryExpression.Operator operator = greater;
    if (!settings.ascend) {
      operator = BinaryExpression.Operator.less;
    }

    String[] fields = getIndexFields(settings);
    BinaryExpressionImpl left = null;
    for (int i = 0; i < value.length; i += 2) {
      if (i + 1 < value.length) {
        BinaryExpressionImpl andExpression = new BinaryExpressionImpl();
        left = andExpression;
        andExpression.setOperator(BinaryExpression.Operator.and);
        BinaryExpressionImpl lhs = new BinaryExpressionImpl();
        lhs.setIsLeftKey(true);
        andExpression.setLeftExpression(lhs);
        ColumnImpl column = new ColumnImpl(null, null, settings.leftColumn.columnTableName, fields[i], null);
        lhs.setLeftExpression(column);
        if (operator == BinaryExpression.Operator.greater) {
          lhs.setOperator(BinaryExpression.Operator.greaterEqual);
        }
        else {
          lhs.setOperator(BinaryExpression.Operator.lessEqual);
        }
        ConstantImpl constant = new ConstantImpl();
        constant.setSqlType(DataType.Type.getTypeForValue(value[i]));//settings.leftColumn.columnType.getValue());
        constant.setValue(value[i]);
        lhs.setRightExpression(constant);

        BinaryExpressionImpl rhs = new BinaryExpressionImpl();
        rhs.setIsRightKey(true);
        andExpression.setRightExpression(rhs);
        column = new ColumnImpl(null, null, settings.leftColumn.columnTableName, fields[i + 1], null);
        rhs.setLeftExpression(column);
        rhs.setOperator(operator);
        constant = new ConstantImpl();
        constant.setSqlType(DataType.Type.getTypeForValue(value[i + 1]));//settings.leftColumn.columnType.getValue());
        constant.setValue(value[i + 1]);
        rhs.setRightExpression(constant);
      }
      else {
        BinaryExpressionImpl lhs = new BinaryExpressionImpl();
        left = lhs;
        lhs.setIsLeftKey(true);
        ColumnImpl column = new ColumnImpl(null, null, settings.leftColumn.columnTableName, fields[i], null);
        lhs.setLeftExpression(column);
        lhs.setOperator(operator);
        ConstantImpl constant = new ConstantImpl();
        constant.setSqlType(DataType.Type.getTypeForValue(value[0]));//settings.leftColumn.columnType.getValue());
        constant.setValue(value[0]);
        lhs.setRightExpression(constant);

      }
    }

    left.setTableName(selectStatement.getFromTable());
    left.setClient(databaseClient);
    left.setViewVersion(databaseClient.getCommon().getSchemaVersion());
    left.setRecordCache(recordCache);
    left.setOrderByExpressions(selectStatement.getOrderByExpressions());
    return left;
  }

  private BinaryExpressionImpl createExpressionForKeys(Object[] lowerKey, Object[] higherKey, BinaryExpression.Operator
      greaterOp,
                                                       BinaryExpression.Operator lessOp, OptimizationSettings settings) {
    BinaryExpressionImpl outer = new BinaryExpressionImpl();

//    if (!settings.ascend) {
//      Object[] tmp = lowerKey;
//      lowerKey = higherKey;
//      higherKey = tmp;
//      if (greaterOp == BinaryExpression.Operator.greater) {
//        greaterOp = BinaryExpression.Operator.greaterEqual;
//      }
//    }

    String[] fields = getIndexFields(settings);

    List<BinaryExpressionImpl> stack = new ArrayList<>();
    for (int i = 0; i < lowerKey.length; i += 2) {
      if (i + 1 < lowerKey.length) {
        BinaryExpressionImpl andExpression = new BinaryExpressionImpl();
        stack.add(andExpression);
        andExpression.setOperator(BinaryExpression.Operator.and);
        BinaryExpressionImpl lhs = new BinaryExpressionImpl();
        lhs.setIsLeftKey(true);
        andExpression.setLeftExpression(lhs);
        ColumnImpl column = new ColumnImpl(null, null, settings.leftColumn.columnTableName, fields[i], null);
        lhs.setLeftExpression(column);
        if (greaterOp == BinaryExpression.Operator.greater) {
          lhs.setOperator(BinaryExpression.Operator.greaterEqual);
        }
        else {
          lhs.setOperator(BinaryExpression.Operator.lessEqual);
        }
        ConstantImpl constant = new ConstantImpl();
        constant.setSqlType(DataType.Type.getTypeForValue(lowerKey[i]));//settings.leftColumn.columnType.getValue());
        constant.setValue(lowerKey[i]);
        lhs.setRightExpression(constant);

        BinaryExpressionImpl rhs = new BinaryExpressionImpl();
        rhs.setIsRightKey(true);
        andExpression.setRightExpression(rhs);
        column = new ColumnImpl(null, null, settings.leftColumn.columnTableName, fields[i + 1], null);
        rhs.setLeftExpression(column);
        if (i == lowerKey.length - 1) {
          rhs.setOperator(greaterOp);
        }
        else {
          if (greaterOp == BinaryExpression.Operator.greater) {
            rhs.setOperator(BinaryExpression.Operator.greaterEqual);
          }
          else {
            rhs.setOperator(BinaryExpression.Operator.lessEqual);
          }
        }
        constant = new ConstantImpl();
        constant.setSqlType(DataType.Type.getTypeForValue(lowerKey[i + 1]));//settings.leftColumn.columnType.getValue());
        constant.setValue(lowerKey[i + 1]);
        rhs.setRightExpression(constant);
      }
      else {
        BinaryExpressionImpl lhs = new BinaryExpressionImpl();
        stack.add(lhs);
        lhs.setIsLeftKey(true);
        ColumnImpl column = new ColumnImpl(null, null, settings.leftColumn.columnTableName, fields[i], null);
        lhs.setLeftExpression(column);
        lhs.setOperator(greaterOp);
        ConstantImpl constant = new ConstantImpl();
        constant.setSqlType(DataType.Type.getTypeForValue(lowerKey[i]));//settings.leftColumn.columnType.getValue());
        constant.setValue(lowerKey[i]);
        lhs.setRightExpression(constant);
      }
    }

    BinaryExpressionImpl left = convertStackToTree(stack);

    stack.clear();
    for (int i = 0; i < higherKey.length; i += 2) {
      if (i + 1 < higherKey.length) {
        BinaryExpressionImpl andExpression = new BinaryExpressionImpl();
        stack.add(andExpression);
        andExpression.setOperator(BinaryExpression.Operator.and);
        BinaryExpressionImpl lhs = new BinaryExpressionImpl();
        lhs.setIsLeftKey(true);
        andExpression.setLeftExpression(lhs);
        ColumnImpl column = new ColumnImpl(null, null, settings.leftColumn.columnTableName, fields[i], null);
        lhs.setLeftExpression(column);
        if (lessOp == BinaryExpression.Operator.greater) {
          lhs.setOperator(BinaryExpression.Operator.greaterEqual);
        }
        else {
          lhs.setOperator(BinaryExpression.Operator.lessEqual);
        }
        ConstantImpl constant = new ConstantImpl();
        constant.setSqlType(DataType.Type.getTypeForValue(higherKey[i]));//settings.leftColumn.columnType.getValue());
        constant.setValue(higherKey[i]);
        lhs.setRightExpression(constant);

        BinaryExpressionImpl rhs = new BinaryExpressionImpl();
        rhs.setIsRightKey(true);
        andExpression.setRightExpression(rhs);
        column = new ColumnImpl(null, null, settings.leftColumn.columnTableName, fields[i + 1], null);
        rhs.setLeftExpression(column);
        if (i == higherKey.length - 1) {
          rhs.setOperator(lessOp);
        }
        else {
          if (lessOp == BinaryExpression.Operator.greater) {
            rhs.setOperator(BinaryExpression.Operator.greaterEqual);
          }
          else {
            rhs.setOperator(BinaryExpression.Operator.lessEqual);
          }
        }
        constant = new ConstantImpl();
        constant.setSqlType(DataType.Type.getTypeForValue(higherKey[i + 1]));//settings.leftColumn.columnType.getValue());
        constant.setValue(higherKey[i + 1]);
        rhs.setRightExpression(constant);
      }
      else {
        BinaryExpressionImpl lhs = new BinaryExpressionImpl();
        stack.add(lhs);
        lhs.setIsLeftKey(true);
        ColumnImpl column = new ColumnImpl(null, null, settings.leftColumn.columnTableName, fields[i], null);
        lhs.setLeftExpression(column);
        lhs.setOperator(lessOp);
        ConstantImpl constant = new ConstantImpl();
        constant.setSqlType(DataType.Type.getTypeForValue(higherKey[i]));//settings.leftColumn.columnType.getValue());
        constant.setValue(higherKey[i]);
        lhs.setRightExpression(constant);
      }
    }
    BinaryExpressionImpl right = convertStackToTree(stack);

    outer.setLeftExpression(left);
    outer.setRightExpression(right);
    outer.setOperator(BinaryExpression.Operator.and);

    outer.setTableName(selectStatement.getFromTable());
    outer.setClient(databaseClient);
    outer.setViewVersion(databaseClient.getCommon().getSchemaVersion());
    outer.setRecordCache(recordCache);
    outer.setOrderByExpressions(selectStatement.getOrderByExpressions());
    return outer;
  }

  private BinaryExpressionImpl convertStackToTree(List<BinaryExpressionImpl> stack) {
    BinaryExpressionImpl left;
    if (stack.size() == 1) {
      left = stack.remove(0);
    }
    else {
      left = new BinaryExpressionImpl();
      left.setOperator(BinaryExpression.Operator.and);
      left.setLeftExpression(stack.remove(0));
      left.setRightExpression(stack.remove(0));
    }
    while (stack.size() > 0) {
      BinaryExpressionImpl e = null;
      if (stack.size() == 1) {
        e = stack.remove(0);
      }
      else {
        e = new BinaryExpressionImpl();
        e.setOperator(BinaryExpression.Operator.and);
        e.setLeftExpression(stack.remove(0));
        e.setRightExpression(stack.remove(0));
      }
      BinaryExpressionImpl and = new BinaryExpressionImpl();
      and.setOperator(BinaryExpression.Operator.and);
      and.setLeftExpression(left);
      and.setRightExpression(e);
      left = and;
    }
    return left;
  }

  private void getMoreServerSetResults() {
    while (true) {
      try {
        lastReadRecords = readRecords;

        databaseClient.doServerSetSelect(dbName, tableNames, setOperation, this, restrictToThisServer, procedureContext);

//        readRecords = null;
//        synchronized (recordCache.getRecordsForTable()) {
//          recordCache .getRecordsForTable().clear();
//        }
//
//        ExpressionImpl.NextReturn ret = new ExpressionImpl.NextReturn(tableNames, retKeys);
//        readRecords = readRecords(ret);
        currPos = 0;
        break;
      }
      catch (SchemaOutOfSyncException e) {
        continue;
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }
  }

  private void getMoreServerResults(SelectStatementImpl selectStatement) {
    while (true) {
      try {
        ComObject cobj = new ComObject();
        cobj.put(ComObject.Tag.legacySelectStatement, selectStatement.serialize());
        cobj.put(ComObject.Tag.schemaVersion, databaseClient.getCommon().getSchemaVersion());
        cobj.put(ComObject.Tag.dbName, dbName);
        cobj.put(ComObject.Tag.count, DatabaseClient.SELECT_PAGE_SIZE);
        cobj.put(ComObject.Tag.method, "serverSelect");

        byte[] recordRet = databaseClient.send(null, selectStatement.getServerSelectShardNumber(),
            selectStatement.getServerSelectReplicaNumber(), cobj, DatabaseClient.Replica.specified);

        ComObject retObj = new ComObject(recordRet);
        selectStatement.deserialize(retObj.getByteArray(ComObject.Tag.legacySelectStatement), dbName);

        String[] tableNames = selectStatement.getTableNames();
        TableSchema[] tableSchemas = new TableSchema[tableNames.length];
        for (int i = 0; i < tableNames.length; i++) {
          tableSchemas[i] = databaseClient.getCommon().getTables(dbName).get(tableNames[i]);
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

        lastReadRecords = readRecords;
        readRecords = null;
        synchronized (recordCache.getRecordsForTable()) {
          recordCache.getRecordsForTable().clear();
        }

        ComArray tablesArray = retObj.getArray(ComObject.Tag.tableRecords);
        int recordCount = tablesArray == null ? 0 : tablesArray.getArray().size();
        Object[][][] retKeys = new Object[recordCount][][];
        Record[][] currRetRecords = new Record[recordCount][];
        for (int k = 0; k < recordCount; k++) {
          currRetRecords[k] = new Record[tableNames.length];
          retKeys[k] = new Object[tableNames.length][];
          ComArray records = (ComArray) tablesArray.getArray().get(k);
          for (int j = 0; j < tableNames.length; j++) {
            byte[] bytes = (byte[]) records.getArray().get(j);
            if (bytes != null) {
              Record record = new Record(tableSchemas[j]);
              record.deserialize(dbName, databaseClient.getCommon(), bytes, null, true);
              currRetRecords[k][j] = record;

              Object[] key = new Object[primaryKeyFields[j].length];
              for (int i = 0; i < primaryKeyFields[j].length; i++) {
                key[i] = record.getFields()[tableSchemas[j].getFieldOffset(primaryKeyFields[j][i])];
              }

              if (retKeys[k][j] == null) {
                retKeys[k][j] = key;
              }

              recordCache.put(tableNames[j], key, new ExpressionImpl.CachedRecord(record, bytes));
            }
          }

        }

        if (retKeys == null || retKeys.length == 0) {
          selectContext.setCurrKeys(null);
        }
        else {
          selectContext.setCurrKeys(retKeys);
        }

        ExpressionImpl.NextReturn ret = new ExpressionImpl.NextReturn(tableNames, retKeys);
        readRecords = readRecords(ret);
        currPos = 0;
        break;
      }
      catch (SchemaOutOfSyncException e) {
        continue;
      }
    }

  }

  public ExpressionImpl.CachedRecord[][] readRecords(ExpressionImpl.NextReturn nextReturn) {
    if (nextReturn == null || nextReturn.getKeys() == null) {
      return null;
    }
//    List<String> columns = new ArrayList<>();
//    for (ColumnImpl column : selectStatement.getSelectColumns()) {
//      columns.add(column.getColumnName());
//    }

//    for (int i = 0; i < nextReturn.getKeys().length; i++) {
//      Object[][] id = nextReturn.getKeys()[i];
//      for (int j = 0; j < id.length; j++) {
//        Object[] currId = id[j];
//        if (currId == null) {
//          continue;
//        }
//        if (recordCache.containsKey(nextReturn.getTableNames()[j], currId)) {
//          continue;
//        }
//        Object[][] fullId = new Object[id.length][];
//        for (int k = 0; k < id.length; k++) {
//          fullId[k] = null;
//        }
//        fullId[j] = currId;
//        idsToRead.add(fullId);
//      }
//    }
    //todo: don't do a contains and a get

    while (true) {
      try {
//        for (int j = 0; j < selectContext.getTableNames().length; j++) {
//          String tableName = selectContext.getTableNames()[j];
//
//          List<ExpressionImpl.IdEntry> keysToRead = new ArrayList<>();
//          for (int i = 0; i < idsToRead.size(); i++) {
//            keysToRead.add(new ExpressionImpl.IdEntry(i, idsToRead.get(i)[j]));
//          }
//
////          Map<Integer, Object[][]> keys = ExpressionImpl.readRecords(databaseClient, databaseClient.getCommon().getTables().get(tableName),
////              keysToRead, nextReturn.getFields().get(nextReturn.getTableNames()[j]), selectContext.getRecordCache());
//
//          //Record[][] records = ExpressionImpl.doReadRecords(databaseClient, actualIds, selectContext.getTableNames(), columns);
//        }
        Object[][][] actualIds = nextReturn.getKeys();
        ExpressionImpl.CachedRecord[][] retRecords = new ExpressionImpl.CachedRecord[actualIds.length][];

        if (retRecords.length > 0) {
          ConcurrentHashMap<ExpressionImpl.RecordCache.Key, ExpressionImpl.CachedRecord>[] tables = new ConcurrentHashMap[nextReturn.getTableNames().length];
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
                //todo: batch these reads
                Record record = doReadRecord(actualIds[i][j], nextReturn.getTableNames()[j]);
                retRecords[i][j] = new ExpressionImpl.CachedRecord(record, record.serialize(databaseClient.getCommon(), DatabaseClient.SERIALIZATION_VERSION));
              }
            }
          }
        }
        recordCache.clear();
        return retRecords;
      }
      catch (Exception e) {
        if (-1 != ExceptionUtils.indexOfThrowable(e, SchemaOutOfSyncException.class)) {
          continue;
        }
        else {
          throw new DatabaseException(e);
        }
      }
    }

  }
}

