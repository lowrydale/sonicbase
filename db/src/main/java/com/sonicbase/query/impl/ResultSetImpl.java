package com.sonicbase.query.impl;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.*;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.query.ResultSet;
import com.sonicbase.schema.DataType;
import com.sonicbase.schema.FieldSchema;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.server.ReadManager;
import com.sonicbase.server.SnapshotManager;
import com.sonicbase.util.DataUtil;
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

/**
 * Responsible for
 */
public class ResultSetImpl implements ResultSet {
  private static final String UTF8_STR = "utf-8";
  private static final String LENGTH_STR = "length";
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
  private long pageSize = ReadManager.SELECT_PAGE_SIZE;

  public ResultSetImpl(String[] describeStrs) {
    this.describeStrs = describeStrs;
  }

  public ResultSetImpl(List<Map<String, String>> mapResults) {
    this.mapResults = mapResults;
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="EI_EXPOSE_REP", justification="copying the returned data is too slow")
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
    Map < String,ColumnImpl > aliases = selectStatement.getAliases();
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
            values[i] = getField(actualColumn);
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

  public static class MultiTableRecordList {
    private String[] tableNames;
    private long[][] ids;

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="EI_EXPOSE_REP", justification="copying the returned data is too slow")
    public String[] getTableNames() {
      return tableNames;
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="EI_EXPOSE_REP2", justification="copying the passed in data is too slow")
    @SuppressWarnings("PMD.ArrayIsStoredDirectly") //copying the passed in data is too slow
    public void setTableNames(String[] tableNames) {
      this.tableNames = tableNames;
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="EI_EXPOSE_REP", justification="copying the returned data is too slow")
    public long[][] getIds() {
      return ids;
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="EI_EXPOSE_REP2", justification="copying the passed in data is too slow")
    @SuppressWarnings("PMD.ArrayIsStoredDirectly") //copying the passed in data is too slow
    public void setIds(long[][] ids) {
      this.ids = ids;
    }
  }

  public ResultSetImpl(
      String dbName,
      DatabaseClient client, SelectStatementImpl selectStatement, long count) {
    this.dbName = dbName;
    this.databaseClient = client;
    this.selectStatement = selectStatement;
    this.count = count;
    this.isCount = true;
  }

  public ResultSetImpl(
      String dbName,
      DatabaseClient databaseClient, SelectStatementImpl selectStatement,
      ParameterHandler parms, Set<SelectStatementImpl.DistinctRecord> uniqueRecords, SelectContextImpl selectContext,
      Record[] retRecords,
      List<ColumnImpl> columns,
      String indexUsed, Counter[] counters, Limit limit, Offset offset,
      List<Expression> groupByColumns, GroupByContext groupContext) throws Exception {
    this.dbName = dbName;
    this.databaseClient = databaseClient;
    this.selectStatement = selectStatement;
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
          sortResults(dbName, databaseClient.getCommon(), selectStatement, readRecords, selectContext.getTableNames());
        }
      }
    }
  }

  public static void sortResults(
      String dbName,
      DatabaseCommon common,
      SelectStatementImpl selectStatement, ExpressionImpl.CachedRecord[][] records,
      final String[] tableNames) {
    List<OrderByExpressionImpl> orderByExpressions = selectStatement.getOrderByExpressions();
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

    if (describeStrs != null) {
      if (currPos > describeStrs.length - 1) {
        return false;
      }
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
    if (selectContext == null) {
      return false;
    }

    if (counters != null) {
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
      if (currPos == 1) {
        return true;
      }
      return false;
    }

    if (selectContext.getCurrKeys() == null && (readRecords == null || readRecords.length == 0)) {
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

    if (groupByColumns != null) {
      Object[] lastFields = new Object[groupByColumns.size()];
      Comparator[] comparators = new Comparator[groupByColumns.size()];
      String[][] actualColumns = new String[groupByColumns.size()][];
      for (int i = 0; i < lastFields.length; i++) {
        String column = ((Column)groupByColumns.get(i)).getColumnName();
        String fromTable = selectStatement.getFromTable();
        TableSchema tableSchema = databaseClient.getCommon().getTables(dbName).get(fromTable);
        DataType.Type type = tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType();
        comparators[i] = type.getComparator();

        actualColumns[i] = getActualColumn(column);
        lastFields[i] = getField(actualColumns[i]);
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
          Object field = getField(actualColumns[i]);
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

    if (selectContext.getCurrKeys() == null) {
      return false;
    }
//    }

//    if (currRecord == null) {
//      return false;
//    }

    return true; //!isAfterLast();
  }


  private Record doReadRecord(Object[] key, String tableName) throws Exception {

    return ExpressionImpl.doReadRecord(dbName, databaseClient, selectStatement.isForceSelectOnServer(), parms, selectStatement.getWhereClause(), recordCache, key, tableName, columns, ((ExpressionImpl)selectStatement.getWhereClause()).getViewVersion(), false);
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
      String command = "DatabaseServer:ComObject:serverSelectDelete:";

      byte[] recordRet = databaseClient.send(null, selectStatement.getServerSelectShardNumber(),
          selectStatement.getServerSelectReplicaNumber(), command, cobj, DatabaseClient.Replica.specified);

    }

  }

  private Object getField(String[] label) {
    label[1] = label[1].toLowerCase();
    if (label[0] != null) {
      label[0] = label[0].toLowerCase();
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
            return readRecords[currPos][i].getRecord().getFields()[offset];
          }
        }
      }
    }
    for (int i = 0; i < selectContext.getTableNames().length; i++) {
      TableSchema tableSchema = databaseClient.getCommon().getTables(dbName).get(selectContext.getTableNames()[i]);
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
        return readRecords[currPos][i].getRecord().getFields()[offset];
      }
    }
    return null;
  }

  public String getString(String columnLabel) {
    if (mapResults != null) {
      return mapResults.get(currPos).get(columnLabel);
    }
    String[] actualColumn = getActualColumn(columnLabel);
    Object ret = getField(actualColumn);
    SelectStatementImpl.Function function = selectStatement.getFunctionAliases().get(columnLabel.toLowerCase());

    String retString = getString(ret);

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
    ColumnImpl column = selectStatement.getAliases().get(columnLabel);
    if (column != null) {
      return new String[]{column.getTableName(), column.getColumnName()};
    }
    return new String[]{null, columnLabel};
  }

  public Boolean getBoolean(String columnLabel) {
    String[] actualColumn = getActualColumn(columnLabel);
    Object ret = getField(actualColumn);

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
    String[] actualColumn = getActualColumn(columnLabel);
    Object ret = getField(actualColumn);

    SelectStatementImpl.Function function = selectStatement.getFunctionAliases().get(columnLabel.toLowerCase());

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
    String[] actualColumn = getActualColumn(columnLabel);
    Object ret = getField(actualColumn);

    SelectStatementImpl.Function function = selectStatement.getFunctionAliases().get(columnLabel.toLowerCase());

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
    if (isMatchingAlias(columnLabel) && isCount) {
      return (int) count;
    }
    Integer ret = (Integer) getGroupByFunctionResults(columnLabel, DataType.Type.INTEGER);
    if (ret != null) {
      return ret;
    }

    String[] actualColumn = getActualColumn(columnLabel);
    Object retObj = getField(actualColumn);

    SelectStatementImpl.Function function = selectStatement.getFunctionAliases().get(columnLabel.toLowerCase());

    return getInt(retObj, function);
  }

  private boolean isMatchingAlias(String columnLabel) {
    boolean matchingAlias = false;
    Map<String, ColumnImpl> aliases = selectStatement.getAliases();
    for (String alias : aliases.keySet()) {
      if (alias.equals(columnLabel)) {
        matchingAlias = true;
      }
    }
    return matchingAlias;
  }

  private Integer getInt(Object ret, SelectStatementImpl.Function function) {
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
        return (int)getCounterValue(function);
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

  public Long getLong(String columnLabel) {
    if (isMatchingAlias(columnLabel) && isCount) {
      return count;
    }
    Long ret = (Long) getGroupByFunctionResults(columnLabel, DataType.Type.BIGINT);
    if (ret != null) {
      return ret;
    }

    String[] actualColumn = getActualColumn(columnLabel);
    Object retObj = getField(actualColumn);

    SelectStatementImpl.Function function = selectStatement.getFunctionAliases().get(columnLabel.toLowerCase());
    return getLong(retObj, function);
  }

  private Long getLong(Object ret, SelectStatementImpl.Function function) {
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
      else if (function.getName().equalsIgnoreCase("min") ||
          function.getName().equalsIgnoreCase("max") ||
          function.getName().equalsIgnoreCase("sum") ||
          function.getName().equalsIgnoreCase("avg")) {
        return (long)getCounterValue(function);
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

  private Object getCounterValue(SelectStatementImpl.Function function) {
    if (function.getName().equalsIgnoreCase("min")) {
      String columnName = ((Column)function.getParms().getExpressions().get(0)).getColumnName();
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
      String columnName = ((Column)function.getParms().getExpressions().get(0)).getColumnName();
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
      String columnName = ((Column)function.getParms().getExpressions().get(0)).getColumnName();
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
      String columnName = ((Column)function.getParms().getExpressions().get(0)).getColumnName();
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
    return 0;
  }

  public Float getFloat(String columnLabel) {
    if (isMatchingAlias(columnLabel) && isCount) {
      return (float)count;
    }

    Float ret = (Float) getGroupByFunctionResults(columnLabel, DataType.Type.FLOAT);
    if (ret != null) {
      return ret;
    }

    String[] actualColumn = getActualColumn(columnLabel);
    Object retObj = getField(actualColumn);

    SelectStatementImpl.Function function = selectStatement.getFunctionAliases().get(columnLabel.toLowerCase());
    return getFloat(retObj, function);
  }

  private Float getFloat(Object ret, SelectStatementImpl.Function function) {
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
        return (float)getCounterValue(function);
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
    if (isMatchingAlias(columnLabel) && isCount) {
      return (double)count;
    }

    Double ret = (Double) getGroupByFunctionResults(columnLabel, DataType.Type.DOUBLE);
    if (ret != null) {
      return ret;
    }

    String[] actualColumn = getActualColumn(columnLabel);
    Object retObj = getField(actualColumn);

    SelectStatementImpl.Function function = selectStatement.getFunctionAliases().get(columnLabel.toLowerCase());
    return getDouble(retObj, function);
  }

  private Double getDouble(Object ret, SelectStatementImpl.Function function) {
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
        return (double)getCounterValue(function);
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
    String[] actualColumn = getActualColumn(columnLabel);
    Object ret = getField(actualColumn);

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
    String[] actualColumn = getActualColumn(columnLabel);
    Object ret = getField(actualColumn);
    if (ret instanceof Blob) {
      return ((Blob) ret).getData();
    }
    return (byte[]) ret;
  }

  public Date getDate(String columnLabel) {
    String[] actualColumn = getActualColumn(columnLabel);
    return (Date) getField(actualColumn);
  }

  public Time getTime(String columnLabel) {
    String[] actualColumn = getActualColumn(columnLabel);
    return (Time) getField(actualColumn);
  }

  public Timestamp getTimestamp(String columnLabel) {
    String[] actualColumn = getActualColumn(columnLabel);
    return (Timestamp) getField(actualColumn);
  }

  public InputStream getAsciiStream(String columnLabel) {
    String[] actualColumn = getActualColumn(columnLabel);
    return (InputStream) getField(actualColumn);
  }

  public InputStream getUnicodeStream(String columnLabel) {
    String[] actualColumn = getActualColumn(columnLabel);
    return (InputStream) getField(actualColumn);
  }

  public InputStream getBinaryStream(String columnLabel) {
    String[] actualColumn = getActualColumn(columnLabel);
    Object ret = getField(actualColumn);
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
    String[] actualColumn = getActualColumn(columnLabel);
    byte[] bytes = (byte[]) getField(actualColumn);
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
    String[] actualColumn = getActualColumn(columnLabel);
    return (BigDecimal) getField(actualColumn);
  }

  @Override
  public Integer getInt(int columnIndex) {
    if (columnIndex == 1 && isCount) {
      return (int) count;
    }

    List<ColumnImpl> columns = selectStatement.getSelectColumns();

    ColumnImpl column = columns.get(columnIndex - 1);
    SelectStatementImpl.Function function = selectStatement.getFunctionAliases().get(column.getColumnName().toLowerCase());

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
      return getField(actualColumn);
    }
    return null;
  }

  @Override
  public Long getLong(int columnIndex) {
    if (columnIndex == 1 && isCount) {
      return count;
    }
    List<ColumnImpl> columns = selectStatement.getSelectColumns();
    ColumnImpl column = columns.get(columnIndex - 1);
    String columnName = column.getColumnName();
    SelectStatementImpl.Function function = selectStatement.getFunctionAliases().get(column.getColumnName().toLowerCase());

    String[] actualColumn = getActualColumn(columnName);
    Object ret = getField(actualColumn);
    return getLong(ret, function);
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) {
    List<ColumnImpl> columns = selectStatement.getSelectColumns();
    ColumnImpl column = columns.get(columnIndex - 1);
    String columnName = column.getColumnName();
    String[] actualColumn = getActualColumn(columnName);
    Object ret = getField(actualColumn);
    return getBigDecimal(ret, -1);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) {
    List<ColumnImpl> columns = selectStatement.getSelectColumns();
    ColumnImpl column = columns.get(columnIndex - 1);
    String columnName = column.getColumnName();
    String[] actualColumn = getActualColumn(columnName);
    return (Timestamp) getField(actualColumn);
  }

  @Override
  public Time getTime(int columnIndex) {
    List<ColumnImpl> columns = selectStatement.getSelectColumns();
    ColumnImpl column = columns.get(columnIndex - 1);
    String columnName = column.getColumnName();
    String[] actualColumn = getActualColumn(columnName);
    return (Time) getField(actualColumn);
  }

  @Override
  public Date getDate(int columnIndex) {
    List<ColumnImpl> columns = selectStatement.getSelectColumns();
    ColumnImpl column = columns.get(columnIndex - 1);
    String columnName = column.getColumnName();
    String[] actualColumn = getActualColumn(columnName);
    return (Date) getField(actualColumn);
  }

  @Override
  public byte[] getBytes(int columnIndex) {
    List<ColumnImpl> columns = selectStatement.getSelectColumns();
    ColumnImpl column = columns.get(columnIndex - 1);
    String columnName = column.getColumnName();
    String[] actualColumn = getActualColumn(columnName);
    Object ret = getField(actualColumn);
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
    Object ret = getField(actualColumn);
    return getBigDecimal(ret, scale);
  }

  @Override
  public Double getDouble(int columnIndex) {
    List<ColumnImpl> columns = selectStatement.getSelectColumns();
    ColumnImpl column = columns.get(columnIndex - 1);
    String columnLabel = column.getColumnName();
    if (isMatchingAlias(columnLabel) && isCount) {
      return (double)count;
    }
    String[] actualColumn = getActualColumn(columnLabel);
    Object ret = getField(actualColumn);

    SelectStatementImpl.Function function = selectStatement.getFunctionAliases().get(columnLabel.toLowerCase());
    return getDouble(ret, function);
  }

  @Override
  public Float getFloat(int columnIndex) {
    List<ColumnImpl> columns = selectStatement.getSelectColumns();
    ColumnImpl column = columns.get(columnIndex - 1);
    String columnLabel = column.getColumnName();

    if (isMatchingAlias(columnLabel) && isCount) {
      return (float)count;
    }
    String[] actualColumn = getActualColumn(columnLabel);
    Object ret = getField(actualColumn);

    SelectStatementImpl.Function function = selectStatement.getFunctionAliases().get(columnLabel.toLowerCase());
    return getFloat(ret, function);
  }

  @Override
  public Short getShort(int columnIndex) {
    List<ColumnImpl> columns = selectStatement.getSelectColumns();
    ColumnImpl column = columns.get(columnIndex - 1);
    String columnName = column.getColumnName();
    String function = column.getFunction();
    String[] actualColumn = getActualColumn(columnName);
    Object ret = getField(actualColumn);
    return getShort(ret, function);
  }

  @Override
  public Byte getByte(int columnIndex) {
    List<ColumnImpl> columns = selectStatement.getSelectColumns();
    ColumnImpl column = columns.get(columnIndex - 1);
    String columnName = column.getColumnName();
    String function = column.getFunction();
    String[] actualColumn = getActualColumn(columnName);
    Object ret = getField(actualColumn);
    return getByte(ret, function);
  }

  @Override
  public Boolean getBoolean(int columnIndex) {
    List<ColumnImpl> columns = selectStatement.getSelectColumns();
    ColumnImpl column = columns.get(columnIndex - 1);
    String columnName = column.getColumnName();
    String[] actualColumn = getActualColumn(columnName);
    Object ret = getField(actualColumn);
    return getBoolean(ret);
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) {
    List<ColumnImpl> columns = selectStatement.getSelectColumns();
    ColumnImpl column = columns.get(columnIndex - 1);
    String columnName = column.getColumnName();
    String[] actualColumn = getActualColumn(columnName);
    Object ret = getField(actualColumn);
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

    if (describeStrs != null) {
      if (columnIndex == 1) {
        return describeStrs[(int)currPos];
      }
      return null;
    }
    List<ColumnImpl> columns = selectStatement.getSelectColumns();
    ColumnImpl column = columns.get(columnIndex - 1);
    String columnName = column.getColumnName();
    String function = column.getFunction();

    String[] actualColumn = getActualColumn(columnName);
    Object ret = getField(actualColumn);

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

  public void getMoreResults() {

    if (selectContext.getSelectStatement() != null) {
      if (!selectStatement.isOnServer() && selectStatement.isServerSelect()) {
        getMoreServerResults(selectStatement);
      }
      else {
        lastReadRecords = readRecords;
        readRecords = null;

        selectStatement.setPageSize(pageSize);
        ExpressionImpl.NextReturn ids = selectStatement.next(dbName, null);
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
        currPos = 0;
      }
      return;
    }

    if (selectContext.getNextShard() == -1) {
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

  private void getMoreServerResults(SelectStatementImpl selectStatement) {
    while (true) {
      try {
        ComObject cobj = new ComObject();
        cobj.put(ComObject.Tag.legacySelectStatement, selectStatement.serialize());
        cobj.put(ComObject.Tag.schemaVersion, databaseClient.getCommon().getSchemaVersion());
        cobj.put(ComObject.Tag.dbName, dbName);
        cobj.put(ComObject.Tag.count, ReadManager.SELECT_PAGE_SIZE);
        cobj.put(ComObject.Tag.method, "serverSelect");

        String command = "DatabaseServer:ComObject:serverSelect:";

        byte[] recordRet = databaseClient.send(null, selectStatement.getServerSelectShardNumber(),
            selectStatement.getServerSelectReplicaNumber(), command, cobj, DatabaseClient.Replica.specified);

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
          ComArray records = (ComArray)tablesArray.getArray().get(k);
          for (int j = 0; j < tableNames.length; j++) {
            byte[] bytes = (byte[])records.getArray().get(j);
            if (bytes != null) {
              Record record = new Record(tableSchemas[j]);
              record.deserialize(dbName, databaseClient.getCommon(), bytes, null, true);
              currRetRecords[k][j] = record;

              Object[] key = new Object[primaryKeyFields.length];
              for (int i = 0; i < primaryKeyFields.length; i++) {
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

  private ExpressionImpl.CachedRecord[][] readRecords(ExpressionImpl.NextReturn nextReturn) {
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
        for (int i = 0; i < retRecords.length; i++) {
          retRecords[i] = new ExpressionImpl.CachedRecord[actualIds[i].length];
          for (int j = 0; j < retRecords[i].length; j++) {
            if (actualIds[i][j] == null) {
              continue;
            }

            ExpressionImpl.CachedRecord cachedRecord = recordCache.get(nextReturn.getTableNames()[j], actualIds[i][j]);
            retRecords[i][j] = cachedRecord;
            if (retRecords[i][j] == null) {
              //todo: batch these reads
              Record record = doReadRecord(actualIds[i][j], nextReturn.getTableNames()[j]);
              retRecords[i][j] = new ExpressionImpl.CachedRecord(record, record.serialize(databaseClient.getCommon()));
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

