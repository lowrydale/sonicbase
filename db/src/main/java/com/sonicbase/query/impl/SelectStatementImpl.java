package com.sonicbase.query.impl;

import com.codahale.metrics.Timer;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.Record;
import com.sonicbase.common.SchemaOutOfSyncException;
import com.sonicbase.query.*;
import com.sonicbase.schema.DataType;
import com.sonicbase.schema.FieldSchema;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.server.ReadManager;
import com.sonicbase.server.SnapshotManager;
import com.sonicbase.util.DataUtil;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.Offset;
import org.apache.commons.lang.exception.ExceptionUtils;

import java.io.*;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class SelectStatementImpl extends StatementImpl implements SelectStatement {

  private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("com.sonicbase.logger");
  private final ExpressionImpl.RecordCache recordCache;

  private DatabaseClient client;
  private String fromTable;
  private ExpressionImpl expression;
  private List<OrderByExpressionImpl> orderByExpressions = new ArrayList<>();
  private List<ColumnImpl> selectColumns = new ArrayList<>();
  private List<Join> joins = new ArrayList<>();
  private boolean isCountFunction;
  private String countTable;
  private String countColumn;
  private Map<String, ColumnImpl> aliases = new HashMap<>();
  private boolean isDistinct;

  final static AtomicLong expressionCount = new AtomicLong();
  final static AtomicLong expressionDuration = new AtomicLong();

  private Map<String, Function> functionAliases = new HashMap<>();
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
  private Long pageSize;
  private boolean forceSelectOnServer;

  public SelectStatementImpl(DatabaseClient client) {
    this.client = client;
    this.recordCache = new ExpressionImpl.RecordCache();
  }

  public String getFromTable() {
    return fromTable;
  }

  public void setFromTable(String fromTable) {
    this.fromTable = fromTable.toLowerCase();
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

  public void serialize(DataOutputStream out) {
    try {
      out.writeUTF(fromTable);
      ExpressionImpl.serializeExpression(expression, out);
      out.writeInt(orderByExpressions.size());
      for (OrderByExpressionImpl orderByExpression : orderByExpressions) {
        orderByExpression.serialize(out);
      }
      out.writeInt(selectColumns.size());
      for (ColumnImpl column : selectColumns) {
        column.serialize(out);
      }
      out.writeBoolean(serverSelect);
      out.writeBoolean(serverSort);

      DataUtil.ResultLength resultLength = new DataUtil.ResultLength();

      DataUtil.writeVLong(out, serverSelectPageNumber, resultLength);
      DataUtil.writeVLong(out, serverSelectShardNumber, resultLength);
      DataUtil.writeVLong(out, serverSelectReplicaNumber, resultLength);
      DataUtil.writeVLong(out, serverSelectResultSetId, resultLength);

      DataUtil.writeVLong(out, tableNames.length, resultLength);
      for (int i = 0; i < tableNames.length; i++) {
        out.writeUTF(tableNames[i]);
      }

      DataUtil.writeVLong(out, joins.size(), resultLength);
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

      getParms().serialize(out);
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public void deserialize(DataInputStream in, String dbName) {
    try {
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
        column.deserialize(in);
        selectColumns.add(column);
      }
      serverSelect = in.readBoolean();
      serverSort = in.readBoolean();

      DataUtil.ResultLength resultLength = new DataUtil.ResultLength();

      serverSelectPageNumber = (int) DataUtil.readVLong(in, resultLength);
      serverSelectShardNumber = (int) DataUtil.readVLong(in, resultLength);
      serverSelectReplicaNumber = (int) DataUtil.readVLong(in, resultLength);
      serverSelectResultSetId = DataUtil.readVLong(in, resultLength);

      int tableCount = (int) DataUtil.readVLong(in, resultLength);
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
      int joinCount = (int) DataUtil.readVLong(in, resultLength);
      for (int i = 0; i < joinCount; i++) {
        Join join = new Join();
        join.deserialize(in);
        joins.add(join);
      }

      if (in.readBoolean()) {
        groupByContext = new GroupByContext();
        groupByContext.deserialize(in, client.getCommon(), dbName);
      }
      expression.setGroupByContext(groupByContext);
      getParms().deserialize(in);
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public void addOrderBy(String tableName, String columnName, boolean isAsc) {
    orderByExpressions.add(new OrderByExpressionImpl(tableName == null ? null : tableName.toLowerCase(), columnName.toLowerCase(), isAsc));
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

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="EI_EXPOSE_REP", justification="copying the returned data is too slow")
  public String[] getTableNames() {
    return tableNames;
  }

  public boolean isServerSelect() {
    return serverSelect;
  }

  public int getServerSelectShardNumber() {
    return serverSelectShardNumber;
  }

  public int getServerSelectReplicaNumber() {
    return serverSelectReplicaNumber;
  }

  public boolean isOnServer() {
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

  public void forceSelectOnServer() {
    this.forceSelectOnServer = true;
  }

  public boolean isForceSelectOnServer() {
    return forceSelectOnServer;
  }

  public Long getPageSize() {
    return pageSize;
  }

  public static class Function {
    private String name;
    private ExpressionList parms;

    public Function(String function, ExpressionList parameters) {
      this.name = function;
      this.parms = parameters;
    }

    public String getName() {
      return name;
    }

    public ExpressionList getParms() {
      return parms;
    }
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
    ColumnImpl columnImpl = new ColumnImpl(localFunction, parameters, localTable, localColumn, localAlias);
    if (localAlias != null) {
      aliases.put(localAlias, columnImpl);
    }
    if (localFunction != null) {
      functionAliases.put(localAlias, new Function(localFunction, parameters));
    }
    this.selectColumns.add(columnImpl);
  }

  public Map<String, Function> getFunctionAliases() {
    return functionAliases;
  }

  class DistinctRecord {
    private final Record record;
    private final List<ColumnImpl> selectColumns;
    private final Comparator[] comparators;
    private final int[] distinctFields;
    private final boolean[] isArray;

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="EI_EXPOSE_REP2", justification="copying the passed in data is too slow")
    @SuppressWarnings("PMD.ArrayIsStoredDirectly") //copying the passed in data is too slow
    public DistinctRecord(
        Record record, Comparator[] comparators, boolean[] isArray, int[] distinctFields,
        List<ColumnImpl> selectColumns) {
      this.record = record;
      this.selectColumns = selectColumns;
      this.comparators = comparators;
      this.distinctFields = distinctFields;
      this.isArray = isArray;
    }

    public boolean equals(Object rhsObj) {
      DistinctRecord rhs = (DistinctRecord) rhsObj;
      boolean equals = true;
      for (int i = 0; i < distinctFields.length; i++) {
        int compare = comparators[distinctFields[i]].compare(record.getFields()[distinctFields[i]], rhs.record.getFields()[distinctFields[i]]);
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
          hash += Arrays.hashCode((byte[])record.getFields()[distinctFields[i]]);
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

    public void appendSpaces() {
//      for (int i = 0; i < indent; i++) {
//        builder.append("  ");
//      }
    }

    public void indent() {
      indent++;
    }

    public void outdent() {
      indent--;
    }
  }

  @Override
  public Object execute(String dbName, Explain explain) throws DatabaseException {
    while (true) {
      try {
        expression.setViewVersion(client.getCommon().getSchemaVersion());
        expression.setDbName(dbName);
        expression.setTableName(fromTable);
        expression.setClient(client);
        expression.setParms(getParms());
        expression.setTopLevelExpression(getWhereClause());
        expression.setOrderByExpressions(orderByExpressions);
        expression.setLimit(limit);

        boolean haveCounters = false;
        boolean needToEvaluate = false;
        List<Counter> countersList = new ArrayList<>();
        GroupByContext groupContext = this.groupByContext;
        if (groupContext == null && groupByColumns != null && groupByColumns.size() != 0) {

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
          Map<String, Function> aliases = getFunctionAliases();
          for (Function function : aliases.values()) {
            if (function.getName().equalsIgnoreCase("count") || function.getName().equalsIgnoreCase("min") || function.getName().equalsIgnoreCase("max") ||
                function.getName().equalsIgnoreCase("avg") || function.getName().equalsIgnoreCase("sum")) {

              if (groupContext == null) {
                groupContext = new GroupByContext(fieldContexts);
                this.groupByContext = groupContext;
                expression.setGroupByContext(groupContext);
              }

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
        else {
          Map<String, Function> aliases = getFunctionAliases();
          for (Function function : aliases.values()) {
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
              for (IndexSchema indexSchema : client.getCommon().getTables(dbName).get(table).getIndices().values()) {
                if (indexSchema.getFields()[0].equals(columnName)) {
                  indexed = true;
                  break;
                }
              }

              haveCounters = true;
              if (indexed && expression instanceof AllRecordsExpressionImpl) {
                ExpressionImpl.evaluateCounter(client.getCommon(), client, dbName, counter);
              }
              else {
                needToEvaluate = true;
              }
            }
          }
        }
        if (!haveCounters) {
          needToEvaluate = true;
        }

        if (countersList.size() > 0) {
          Counter[] counters = countersList.toArray(new Counter[countersList.size()]);
          this.counters = counters;
          expression.setCounters(counters);
        }

        Integer replica = expression.getReplica();
        if (replica == null) {
          int replicaCount = client.getCommon().getServersConfig().getShards()[0].getReplicas().length;
          replica = ThreadLocalRandom.current().nextInt(0, replicaCount);
          expression.setReplica(replica);
        }

        boolean sortWithIndex = expression.canSortWithIndex();
        tableNames = new String[]{fromTable};
        if (joins.size() > 0) {
          tableNames = new String[joins.size() + 1];
          tableNames[0] = fromTable;
          for (int i = 0; i < tableNames.length - 1; i++) {
            tableNames[i + 1] = joins.get(i).rightFrom;
          }
          sortWithIndex = false;
        }

        boolean countDistinct = false;
        if (this.isCountFunction && this.isDistinct) {
          countDistinct = true;
        }

        if (!countDistinct && this.isCountFunction) {
          return countRecords(dbName, tableNames);
        }
        else {
          Set<ColumnImpl> columns = new HashSet<>();
          expression.getColumns(columns);
          if (selectColumns != null && selectColumns.size() != 0) {
            for (String tableName : tableNames) {
              for (IndexSchema indexSchema : client.getCommon().getTables(dbName).get(tableName).getIndexes().values()) {
                if (indexSchema.isPrimaryKey()) {
                  for (String field : indexSchema.getFields()) {
                    for (ColumnImpl selectColumn : selectColumns) {
                      if ((selectColumn.getTableName() == null || selectColumn.getTableName().equalsIgnoreCase(tableName)) && selectColumn.getColumnName().equalsIgnoreCase(field)) {
                        continue;
                      }
                      ColumnImpl column = new ColumnImpl();
                      column.setTableName(tableName);
                      column.setColumnName(field);
                      columns.add(column);
                    }
                  }
                  break;
                }
              }
            }
            columns.addAll(selectColumns);
          }
          else {
            for (String tableName : tableNames) {
              TableSchema tableSchema = client.getCommon().getTables(dbName).get(tableName);
              if (tableSchema == null) {
                throw new DatabaseException("Table does not exist: name=" + tableName);
              }
              for (FieldSchema field: tableSchema.getFields()) {
                ColumnImpl column = new ColumnImpl();
                column.setTableName(tableName);
                column.setColumnName(field.getName());
                columns.add(column);
              }
            }
          }

          if (orderByExpressions != null) {
            for (OrderByExpressionImpl expression : orderByExpressions) {
              ColumnImpl column = new ColumnImpl();
              column.setTableName(expression.getTableName());
              column.setColumnName(expression.getColumnName());
              columns.add(column);
            }
          }
          List<ColumnImpl> list = new ArrayList<>();
          list.addAll(columns);
          expression.setColumns(list);
          this.columns = list;

          expression.queryRewrite();
          ColumnImpl primaryColumn = expression.getPrimaryColumn();
          boolean isIndexed = false;
          if (primaryColumn != null) {
            TableSchema tableSchema = client.getCommon().getTables(dbName).get(primaryColumn.getTableName());
            for (Map.Entry<String, IndexSchema> entry : tableSchema.getIndices().entrySet()) {
              if (entry.getValue().getFields()[0].equals(primaryColumn.getColumnName())) {
                isIndexed = true;
              }
            }
          }
          serverSort = true;
          if (orderByExpressions == null || orderByExpressions.size() == 0) {
            serverSort = false;
          }
          else {
            String columnName = orderByExpressions.get(0).getColumnName();
            String tableName = orderByExpressions.get(0).getTableName();
            if (primaryColumn == null) {
              serverSort = true;
            }
            else {
              if (isIndexed && primaryColumn.getColumnName().equals(columnName) &&
                  (tableName == null ||
                  primaryColumn.getTableName().equals(tableName))) {
                serverSort = false;
              }
            }
          }

          serverSelect = false;
          if (joins.size() > 0) {
//            serverSelect = true;
//            serverSelectResultSetId = -1;
          }
          if (serverSort) {
            serverSelect = true;
            serverSelectResultSetId = -1;
          }

          Set<DistinctRecord> uniqueRecords = new HashSet<>();
          ExpressionImpl.NextReturn ids = null;
          if (needToEvaluate) {
            ids = next(dbName, explain);
            if (!serverSelect) {
              applyDistinct(dbName, tableNames, ids, uniqueRecords);
            }
          }

          if (explain != null) {
            return new ResultSetImpl(explain.getBuilder().toString().split("\\n"));
          }

          ResultSet ret = new ResultSetImpl(dbName, client, this, getParms(), uniqueRecords,
              new SelectContextImpl(ids, sortWithIndex, tableNames, expression.getNextShard(), expression.getNextKey(),
                  this, recordCache), null, list, null, counters, limit, offset, groupByColumns, this.groupByContext);
          if (countDistinct) {
            long count = 0;
            while (ret.next()) {
              count++;
            }
            return new ResultSetImpl(dbName, client, this, count);
          }
          else {
            return ret;
          }
        }
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }
  }

  private ExpressionImpl.NextReturn serverSelect(String dbName, boolean serverSort, String[] tableNames) throws Exception {
    while (true) {
      try {
        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytesOut);
        DataUtil.writeVLong(out, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
        out.writeBoolean(serverSort);

        serialize(out);

        out.close();

        int previousSchemaVersion = client.getCommon().getSchemaVersion();
        String command = "DatabaseServer:serverSelect:1:" + client.getCommon().getSchemaVersion() + ":" + dbName + ":" + ReadManager.SELECT_PAGE_SIZE;

        byte[] recordRet = client.send(null, Math.abs(ThreadLocalRandom.current().nextInt() % client.getShardCount()), Math.abs(ThreadLocalRandom.current().nextLong()), command, bytesOut.toByteArray(), DatabaseClient.Replica.def);
        if (previousSchemaVersion < client.getCommon().getSchemaVersion()) {
          throw new SchemaOutOfSyncException();
        }

        DataInputStream in = new DataInputStream(new ByteArrayInputStream(recordRet));
        long serializationVersion = DataUtil.readVLong(in);
        deserialize(in, dbName);

        DataUtil.ResultLength resultLength = new DataUtil.ResultLength();

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
        int recordCount = (int) DataUtil.readVLong(in, resultLength);
        Object[][][] retKeys = new Object[recordCount][][];
        Record[][] currRetRecords = new Record[recordCount][];
        for (int k = 0; k < recordCount; k++) {
          currRetRecords[k] = new Record[tableNames.length];
          retKeys[k] = new Object[tableNames.length][];
          for (int j = 0; j < tableNames.length; j++) {
            if (in.readBoolean()) {
              Record record = new Record(tableSchemas[j]);
              int len = (int)DataUtil.readVLong(in, resultLength); //len
              byte[] bytes = new byte[len];
              in.readFully(bytes);
              record.deserialize(dbName, client.getCommon(), bytes, null, true);
              currRetRecords[k][j] = record;

              Object[] key = new Object[primaryKeyFields.length];
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
        return new ExpressionImpl.NextReturn(tableNames, retKeys);
      }
      catch (SchemaOutOfSyncException e) {
        continue;
      }
    }

  }

  public void applyDistinct(
      String dbName, String[] tableNames, ExpressionImpl.NextReturn ids, Set<DistinctRecord> uniqueRecords) {
//    if (uniqueRecords == null || uniqueRecords.size() == 0) {
//      return;
//    }
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
        if (DataType.Type.VARCHAR == tableSchema.getFields().get(i).getType()) {
          isArray[i] = true;
        }
      }
      Comparator[] comparators = tableSchema.getComparators(fields);
      int[] distinctFields = new int[selectColumns.size()];
      for (int i = 0; i < selectColumns.size(); i++) {
        distinctFields[i] = tableSchema.getFieldOffset(selectColumns.get(i).getColumnName());
      }
      List<Object[][]> actualIds = new ArrayList<>();
      for (int i = 0; i < ids.getIds().length; i++) {
        Record record = recordCache.get(fromTable, ids.getIds()[i][tableIndex]).getRecord();
        if (!uniqueRecords.contains(new DistinctRecord(record, comparators, isArray, distinctFields, selectColumns))) {
          actualIds.add(ids.getIds()[i]);
          uniqueRecords.add(new DistinctRecord(record, comparators, isArray, distinctFields, selectColumns));
        }
      }
      ids.setIds(new Object[actualIds.size()][][]);
      for (int i = 0; i < actualIds.size(); i++) {
        ids.getIds()[i] = actualIds.get(i);
      }
      //todo: need to apply distinct on ResultSetImpl.getMoreRecords()
    }
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="EI_EXPOSE_REP2", justification="copying the passed in data is too slow")
  @SuppressWarnings("PMD.ArrayIsStoredDirectly") //copying the passed in data is too slow
  private ResultSet countRecords(String dbName, String[] tableNames) throws DatabaseException {
    long count = 0;
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
    if (joins.size() != 0) {
      while (true) {
        ExpressionImpl.NextReturn ids = next(dbName, null);
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
    }
    else {
      while (true) {
        try {
          int previousSchemaVersion = client.getCommon().getSchemaVersion();
          int shardCount = client.getShardCount();
          for (int shard = 0; shard < shardCount; shard++) {
            ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytesOut);
            DataUtil.writeVLong(out, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
            if (expression instanceof AllRecordsExpressionImpl) {
              expression = null;
            }
            if (expression == null) {
              out.writeBoolean(false);
            }
            else {
              out.writeBoolean(true);
              ExpressionImpl.serializeExpression(expression, out);
            }

            if (getParms() == null) {
              out.writeBoolean(false);
            }
            else {
              out.writeBoolean(true);
              getParms().serialize(out);
            }

            if (this.countTable == null) {
              out.writeBoolean(false);
            }
            else {
              out.writeBoolean(true);
              out.writeUTF(this.countTable);
            }

            if (this.countColumn == null) {
              out.writeBoolean(false);
            }
            else {
              out.writeBoolean(true);
              out.writeUTF(this.countColumn);
            }

            out.close();

            String command = "DatabaseServer:countRecords:1:" + client.getCommon().getSchemaVersion() + ":" + dbName + ":" + fromTable;
            byte[] lookupRet = client.send(null, shard, 0, command, bytesOut.toByteArray(), DatabaseClient.Replica.def);
            if (previousSchemaVersion < client.getCommon().getSchemaVersion()) {
              throw new SchemaOutOfSyncException();
            }
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(lookupRet));
            long serialiationVersion = DataUtil.readVLong(in);
            long currCount = in.readLong();
            count += currCount;
          }
          break;
        }
        catch (SchemaOutOfSyncException e) {
          try {
            Thread.sleep(200);
          }
          catch (InterruptedException e1) {
          }
          continue;
        }
        catch (Exception e) {
          throw new DatabaseException(e);
        }
      }
    }
    return new ResultSetImpl(dbName, client, this, count);
  }

  public ExpressionImpl.NextReturn next(String dbName, Explain explain) throws DatabaseException {
    while (true) {
      try {
        ExpressionImpl expression = getExpression();

        Integer replica = expression.getReplica();
        if (replica == null) {
          int replicaCount = client.getCommon().getServersConfig().getShards()[0].getReplicas().length;
          replica = ThreadLocalRandom.current().nextInt(0, replicaCount);
          expression.setReplica(replica);
        }
        int count = ReadManager.SELECT_PAGE_SIZE;
        if (this.pageSize != null) {
          count = (int)(long)this.pageSize;
        }

        expression.setRecordCache(recordCache);
        if (joins.size() > 0) {
          return handleJoins(count, dbName, explain);
        }
        ExpressionImpl.NextReturn ret = null;
        if (!isOnServer && serverSelect) {
          ret = serverSelect(dbName, serverSort, tableNames);
        }
        else {
          expression.forceSelectOnServer(forceSelectOnServer);
          expression.setDbName(dbName);
          ret = expression.next(count, explain);
        }
        if (ret == null) {
          return null;
        }
        if (!serverSelect) {
          dedupIds(dbName, ret.getTableNames(), ret);
        }
        return ret;
      }
      catch (SchemaOutOfSyncException e) {
        expression.setViewVersion(client.getCommon().getSchemaVersion());
        try {
          Thread.sleep(200);
        }
        catch (InterruptedException e1) {
        }
        continue;
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }

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


  class KeyEntry {
    private Object[][] key;
    Comparator[][] comparators;

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="EI_EXPOSE_REP2", justification="copying the passed in data is too slow")
    @SuppressWarnings("PMD.ArrayIsStoredDirectly") //copying the passed in data is too slow
    public KeyEntry(Object[][] id, Comparator[][] comparators) {
      this.key = id;
      this.comparators = comparators;
    }

    public boolean equals(Object otherObj) {
      KeyEntry other = (KeyEntry) otherObj;
      boolean equals = true;
      outer:
      for (int i = 0; i < key.length; i++) {
        if (key[i] != null && other.key[i] == null) {
          equals = false;
          break;
        }
        if (key[i] == null && other.key[i] != null) {
          equals = false;
          break;
        }
        if (key[0] != null && other.key[0] != null) {
          if (equals) {
            for (int j = 0; j < comparators.length; j++) {
              if (comparators[i][j].compare(key[i][j], other.key[i][j]) != 0) {
                equals = false;
                break outer;
              }
            }
          }
        }
      }
      return equals;
    }

    public int hashCode() {
      int ret = 0;
      for (int i = 0; i < key.length; i++) {
        if (key[i] != null) {
          for (int j = 0; j < key[i].length; j++) {
            ret += key[i][j].hashCode();
          }
        }
      }
      return ret;
    }
  }


  private void dedupIds(String dbName, String[] tableNames, ExpressionImpl.NextReturn ids) {
    if (ids.getIds() == null) {
      return;
    }
    if (ids.getIds().length == 1) {
      return;
    }
    Comparator[][] comparators = new Comparator[tableNames.length][];
    for (int i = 0; i < tableNames.length; i++) {
      TableSchema schema = client.getCommon().getTables(dbName).get(tableNames[i]);
      for (Map.Entry<String, IndexSchema> indexSchema : schema.getIndices().entrySet()) {
        if (indexSchema.getValue().isPrimaryKey()) {
          comparators[i] = indexSchema.getValue().getComparators();
          break;
        }
      }
    }

    Set<KeyEntry> set = new HashSet<>();
    for (int i = 0; i < ids.getIds().length; i++) {
      KeyEntry entry = new KeyEntry(ids.getIds()[i], comparators);
      set.add(entry);
    }

    Object[][][] retIds = new Object[set.size()][][];
    int offset = 0;
    for (int i = 0; i < ids.getIds().length; i++) {
      if (set.contains(new KeyEntry(ids.getIds()[i], comparators))) {
        retIds[offset] = ids.getIds()[i];
        set.remove(new KeyEntry(ids.getIds()[i], comparators));
        offset++;
      }
    }
    ids.setIds(retIds);
  }

  private ExpressionImpl.NextReturn handleJoins(int pageSize, String dbName, Explain explain) throws Exception {
    Timer.Context ctx = DatabaseClient.JOIN_EVALUATE.time();
    try {
      final AtomicReference<List<Object[][]>> multiTableIds = new AtomicReference<>();
      String[] tableNames = new String[joins.size() + 1];
      tableNames[0] = fromTable;
      for (int i = 0; i < tableNames.length - 1; i++) {
        tableNames[i + 1] = joins.get(i).rightFrom;
      }

      for (Join join : joins) {
        ((ExpressionImpl) join.expression).getColumnsInExpression(columns);
      }

      final ExpressionImpl.NextReturn ret = new ExpressionImpl.NextReturn();
      while (true) {
        try {
          boolean hadSelectRet = false;
          for (int k = 0; k < joins.size(); k++) {
            Join join = joins.get(k);

            String joinRightFrom = join.rightFrom;
            Expression joinExpression = join.expression;
            final JoinType joinType = join.type;

            Object[] lastKey = expression.getNextKey();

            if (!(joinExpression instanceof BinaryExpressionImpl)) {
              throw new DatabaseException("Join expression type not supported");
            }
            BinaryExpressionImpl joinBinaryExpression = (BinaryExpressionImpl) joinExpression;
            ExpressionImpl leftExpression = joinBinaryExpression.getLeftExpression();
            ExpressionImpl rightExpression = joinBinaryExpression.getRightExpression();
            ExpressionImpl additionalJoinExpression = null;
            if (leftExpression instanceof BinaryExpressionImpl && rightExpression instanceof BinaryExpressionImpl) {
              if (joinBinaryExpression.getOperator() != BinaryExpression.Operator.and) {
                throw new DatabaseException("Only 'and' operators are supported in join expression");
              }
              List<ExpressionImpl> otherJoinExpressions = new ArrayList<>();
              AtomicReference<BinaryExpressionImpl> actualJoinExpression = new AtomicReference<>();
              getActualJoinExpression(joinBinaryExpression, actualJoinExpression, otherJoinExpressions);
              leftExpression = actualJoinExpression.get().getLeftExpression();
              rightExpression = actualJoinExpression.get().getRightExpression();

              if (otherJoinExpressions.size() == 0) {
                additionalJoinExpression = null;
              }
              else if (otherJoinExpressions.size() == 1) {
                additionalJoinExpression = otherJoinExpressions.get(0);
              }
              else {
                BinaryExpressionImpl andExpression = new BinaryExpressionImpl();
                BinaryExpressionImpl firstAndExpression = andExpression;
                andExpression.setOperator(BinaryExpression.Operator.and);
                for (int i = 0; i < otherJoinExpressions.size(); i++) {
                  ExpressionImpl expression = otherJoinExpressions.get(i);
                  if (andExpression.getLeftExpression() == null) {
                    andExpression.setLeftExpression(expression);
                  }
                  else {
                    if (i < otherJoinExpressions.size() - 2) {
                      BinaryExpressionImpl newAndExpression = new BinaryExpressionImpl();
                      newAndExpression.setOperator(BinaryExpression.Operator.and);
                      newAndExpression.setLeftExpression(expression);
                      andExpression.setRightExpression(newAndExpression);
                      andExpression = newAndExpression;
                    }
                    else {
                      andExpression.setRightExpression(expression);
                    }
                  }
                }
                additionalJoinExpression = firstAndExpression;
              }
            }
            if (leftExpression instanceof ColumnImpl && rightExpression instanceof ColumnImpl) {
              final AtomicReference<ColumnImpl> leftColumn = new AtomicReference<>((ColumnImpl) leftExpression);
              final AtomicReference<ColumnImpl> rightColumn = new AtomicReference<>((ColumnImpl) rightExpression);

              if (!leftColumn.get().getTableName().equals(tableNames[k])) {
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
                final AtomicReference<String> leftFrom = new AtomicReference<>(k == 0 ? fromTable : tableNames[k]);
                final AtomicReference<String> rightFrom = new AtomicReference<>(tableNames[k + 1]);
                ExpressionImpl.NextReturn ids = null;
                if (joinType == JoinType.inner) {
                  if (multiTableIds.get() == null) {
                    if (explain != null) {
                      explain.appendSpaces();
                      explain.getBuilder().append("inner join based on expression: table=" + fromTable + ", expression=" + expression.toString() + "\n");
                    }
                    long begin = System.nanoTime();
                    ids = expression.next(pageSize / threadCount, explain);
                    if (ids != null && ids.getIds() != null && ids.getIds().length != 0) {
                      hadSelectRet = true;
                    }
                    expressionCount.incrementAndGet();
                    expressionDuration.set(System.nanoTime() - begin);
                  }
                }
                else if (joinType == JoinType.leftOuter || joinType == JoinType.full) {
                  if (multiTableIds.get() == null) {
                    if (explain != null) {
                      explain.appendSpaces();
                      if (joinType == JoinType.leftOuter) {
                        explain.getBuilder().append("left outer join. Retrieving all records from table: table=" + fromTable + "\n");
                      }
                      else {
                        explain.getBuilder().append("Full outer join. Retrieving all records from table: table=" + fromTable + "\n");
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
                    allExpression.setOrderByExpressions(expression.getOrderByExpressions());
                    ids = allExpression.next(pageSize / threadCount, explain);
                    if (ids != null && ids.getIds() != null && ids.getIds().length != 0) {
                      hadSelectRet = true;
                    }
                    expression.setNextShard(allExpression.getNextShard());
                    expression.setNextKey(allExpression.getNextKey());
                  }
                }
                else if (joinType == JoinType.rightOuter) {
                  if (multiTableIds.get() == null) {
                    if (explain != null) {
                      explain.appendSpaces();
                      explain.getBuilder().append("Right outer join. Retrieving all records from table: table=" + fromTable + "\n");
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
                    allExpression.setOrderByExpressions(expression.getOrderByExpressions());
                    ids = allExpression.next(pageSize / threadCount, explain);
                    if (ids != null && ids.getIds() != null && ids.getIds().length != 0) {
                      hadSelectRet = true;
                    }
                    expression.setNextShard(allExpression.getNextShard());
                    expression.setNextKey(allExpression.getNextKey());
                  }
                  leftFrom.set(joinRightFrom);
                  rightFrom.set(fromTable);
                  ColumnImpl column = leftColumn.get();
                  leftColumn.set(rightColumn.get());
                  rightColumn.set(column);
                }
                final List<String> joinColumns = new ArrayList<>();
                joinColumns.add(leftColumn.get().getColumnName());

                final TableSchema leftTable = client.getCommon().getTables(dbName).get(leftFrom.get());
                final TableSchema rightTable = client.getCommon().getTables(dbName).get(rightFrom.get());

                final AtomicInteger rightTableIndex = new AtomicInteger();
                for (int j = 0; j < tableNames.length; j++) {
                  if (tableNames[j].equals(rightTable.getName())) {
                    rightTableIndex.set(j);
                    break;
                  }
                }
                final AtomicInteger leftTableIndex = new AtomicInteger();
                for (int j = 0; j < tableNames.length; j++) {
                  if (tableNames[j].equals(leftTable.getName())) {
                    leftTableIndex.set(j);
                    break;
                  }
                }
                final AtomicReference<List<Object[][]>> idsToProcess = new AtomicReference<>();
                idsToProcess.set(new ArrayList<Object[][]>());
                if (ids != null && ids.getKeys() != null) {
                  for (Object[][] id : ids.getKeys()) {
                    Object[][] newId = new Object[tableNames.length][];
                    newId[leftTableIndex.get()] = id[0];
                    idsToProcess.get().add(newId);
                  }
                }
                else {
                  idsToProcess.set(multiTableIds.get());
                }
                joinRet.set(evaluateJoin(pageSize, dbName, idsToProcess, joinType,
                    leftColumn, rightColumn, leftTable, rightTable, rightTableIndex, leftTableIndex, explain));

                //              if (joinType == JoinType.rightOuter || joinType == JoinType.leftOuter) {
                //              if (joinType != JoinType.inner) {
                //                if (joinRet.get() != null) {
                //                  for (Object[][] key : joinRet.get().keys) {
                //                    if (key.length > rightTableIndex.get()) {
                //                      if (key[rightTableIndex.get()] == null) {
                //                        continue;
                //                      }
                //                      Record record = recordCache.get(rightTable.getName(), key[rightTableIndex.get()]);
                //                      if (record == null) {
                //                        continue;
                //                      }
                //                      boolean passes = (boolean) expression.evaluateSingleRecord(rightTable, record, getParms());
                //                      if (!passes) {
                //                        key[rightTableIndex.get()] = null;
                //                      }
                //                    }
                //                  }
                //                }
                //              }
                if (joinType != JoinType.full) {
                  if (joinRet.get() != null) {
                    AtomicReference<JoinReturn> finalJoinRet = new AtomicReference<>();
                    finalJoinRet.set(new JoinReturn());
                    TableSchema[] tables = new TableSchema[tableNames.length];
                    for (int i = 0; i < tables.length; i++) {
                      tables[i] = client.getCommon().getTables(dbName).get(tableNames[i]);
                    }
                    Object[][] previousKey = null;
                    if (lastKey != null) {
                      previousKey = new Object[tables.length][];
                      previousKey[leftTableIndex.get()] = lastKey;
                    }
                    for (Object[][] key : joinRet.get().keys) {
                      Record[] records = new Record[tableNames.length];
                      for (int i = 0; i < records.length; i++) {
                        if (key[i] != null) {
                          records[i] = recordCache.get(tableNames[i], key[i]).getRecord();
                        }
                      }
                      boolean passes = (boolean) expression.evaluateSingleRecord(tables, records, getParms());
                      if (!passes) {
                        boolean equals = true;
                        for (int i = 0; i < tables.length; i++) {
                          if (leftTableIndex.get() != i) {
                            key[i] = null;
                          }
                          else {
                            for (int j = 0; j < key[i].length; j++) {
                              if (previousKey != null) {
                                Object lhsValue = key[i][j];
                                Object rhsValue = previousKey[i][j];
                                Comparator comparator = DataType.Type.getComparatorForValue(lhsValue);
                                if (lhsValue == null || rhsValue == null) {
                                  equals = false;
                                }
                                else {
                                  if (comparator.compare(lhsValue, rhsValue) != 0) {
                                    equals = false;
                                  }
                                }
                              }
                            }
                          }
                        }
                        if (previousKey == null || !equals || key.length == 1) {
                          finalJoinRet.get().keys.add(key);
                        }
                      }
                      else {
                        finalJoinRet.get().keys.add(key);
                      }
                      previousKey = key;
                    }
                    joinRet.set(finalJoinRet.get());
                  }
                }
                if (additionalJoinExpression != null) {
                  if (explain != null) {
                    explain.appendSpaces();
                    explain.getBuilder().append("Evaluating join expression: expression=" + additionalJoinExpression.toString() + "\n");
                  }
                  if (joinRet.get() != null) {
                    List<Object[][]> retKeys = new ArrayList<>();
                    TableSchema[] tables = new TableSchema[tableNames.length];
                    for (int i = 0; i < tables.length; i++) {
                      tables[i] = client.getCommon().getTables(dbName).get(tableNames[i]);
                    }
                    for (Object[][] key : joinRet.get().keys) {
                      Record[] records = new Record[tableNames.length];
                      for (int i = 0; i < records.length; i++) {
                        if (key[i] != null) {
                          records[i] = recordCache.get(tableNames[i], key[i]).getRecord();
                        }
                      }
                      boolean passes = (boolean) additionalJoinExpression.evaluateSingleRecord(tables, records, getParms());
                      if (passes) {
                        retKeys.add(key);
                      }
                    }
                    joinRet.get().keys = retKeys;
                  }
                }

              }
              //          }
              if (joinRet.get() != null) {
                multiTableIds.set(joinRet.get().keys);
              }
            }
          }
          if (hadSelectRet && multiTableIds.get().size() == 0) {
            multiTableIds.set(null);
            continue;
          }
          break;
        }
        catch (Exception e) {
          if (-1 != ExceptionUtils.indexOfThrowable(e, SchemaOutOfSyncException.class)) {
            expression.setViewVersion(client.getCommon().getSchemaVersion());
            logger.error("SchemaOutOfSyncException");
            Thread.sleep(200);
          }
          else {
            throw e;
          }
        }
      }
      if (multiTableIds == null || multiTableIds.get() == null) {
        return null;
      }
      Object[][][] tableIds = new Object[multiTableIds.get().size()][][];
      for (int i = 0; i < tableIds.length; i++) {
        tableIds[i] = multiTableIds.get().get(i);
      }

      ret.setTableNames(tableNames);
      for (int i = 0; i < tableNames.length; i++) {
        for (Map.Entry<String, IndexSchema> entry : client.getCommon().getTables(dbName).get(tableNames[i]).getIndices().entrySet()) {
          if (entry.getValue().isPrimaryKey()) {
            ret.setFields(tableNames[i], entry.getValue().getFields());
          }
        }
      }
      ret.setIds(tableIds);
      return ret;
    }
    finally {
      ctx.stop();
    }
    //return new ResultSetImpl(client, this, getParms(), new SelectContextImpl(tableIds, false, tableNames, this), null, null);
  }

  private void getActualJoinExpression(ExpressionImpl joinExpression, AtomicReference<BinaryExpressionImpl> actualJoinExpression,
                                                       List<ExpressionImpl> otherJoinExpressions) {
    if (!(joinExpression instanceof BinaryExpressionImpl)) {
      otherJoinExpressions.add(joinExpression);
      return;
    }
    BinaryExpressionImpl joinBinaryExpression = (BinaryExpressionImpl) joinExpression;

    if (joinBinaryExpression.getLeftExpression() instanceof ColumnImpl && joinBinaryExpression.getRightExpression() instanceof ColumnImpl) {
      if (actualJoinExpression.get() == null) {
        actualJoinExpression.set(joinBinaryExpression);
      }
      else {
        otherJoinExpressions.add(joinExpression);
      }
      return;
    }

    if (joinBinaryExpression.getOperator() == BinaryExpression.Operator.and) {
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
      AtomicInteger leftTableIndex, Explain explain) throws Exception {

     if (multiTableIds.get() != null) {
      HashMap<Integer, Object[][]> keys = null;
      boolean expressionForRightTable = isExpressionForRightTable(rightTable, expression);
      List<Object[][]> newMultiTableIds = new ArrayList<>();
      int[] fieldOffsets = null;
      final AtomicReference<Map.Entry<String, IndexSchema>> indexSchema = new AtomicReference<>();
      final TableSchema tableSchema = client.getCommon().getTables(dbName).get(rightTable.getName());
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

      int leftColumnIndex = leftTable.getFieldOffset(leftColumn.get().getColumnName());

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
       if (explain != null) {
         explain.appendSpaces();
         explain.getBuilder().append("Evaluate join expression. Read join records: joinTable=" + rightTable.getName() + ", expression=" + expression.toString() + "\n");
       }

      keys = ExpressionImpl.readRecords(dbName, client, pageSize, forceSelectOnServer, tableSchema, keysToRead, new String[]{rightColumn.get().getColumnName()}, columns, recordCache, expression.getViewVersion());


      //todo: need to make sure this index is sharded with the primary key so we know they're on the same server
      if (!indexSchema.get().getValue().isPrimaryKeyGroup()) {

        if (!indexSchema.get().getValue().isPrimaryKeyGroup()) {
          List<ExpressionImpl.IdEntry> keysToRead2 = new ArrayList<>();
          for (int i = 0; i < keys.size(); i++) {
            Object[][] key = keys.get(i);
            for (int j = 0; j < key.length; j++) {
              keysToRead2.add(new ExpressionImpl.IdEntry(i, key[j]));
            }
          }

          keys = ExpressionImpl.readRecords(dbName, client, pageSize, forceSelectOnServer, tableSchema, keysToRead2, tableSchema.getPrimaryKey(), columns, recordCache, expression.getViewVersion());
        }
      }


      for (int i = 0; i < multiTableIds.get().size(); i++) {
        Object[][] id = multiTableIds.get().get(i);
        Object[][] rightIds = keys.get(i);

        int sizeBefore = newMultiTableIds.size();
        if (rightIds == null) {
          if (joinType == JoinType.full || joinType == JoinType.leftOuter || joinType == JoinType.rightOuter) {
            Object[][] newId = Arrays.copyOf(id, id.length);
            newId[rightTableIndex.get()] = null;
            newMultiTableIds.add(newId);
          }
        }
        else {
          for (Object[] rightId : rightIds) {
            if (joinType == JoinType.full) {
              Object[][] newId = Arrays.copyOf(id, id.length);
              newId[rightTableIndex.get()] = rightId;
              newMultiTableIds.add(newId);
            }
            else {
              boolean passed = true;
              if (expressionForRightTable) {
                //todo: batch this
//                List<String> joinColumns = new ArrayList();
//                for (ColumnImpl column : getSelectColumns()) {
//                  joinColumns.add(column.getColumnName());
//                }
//                passed = ExpressionImpl.doCheckRecord(client, rightId, rightTable.getName(), joinColumns, expression, getParms());
              }
              if (passed) {
                Object[][] newId = Arrays.copyOf(id, id.length);
                newId[rightTableIndex.get()] = rightId;
                newMultiTableIds.add(newId);
              }
            }
          }
          if (sizeBefore == newMultiTableIds.size() && (joinType == JoinType.full || joinType == JoinType.leftOuter || joinType == JoinType.rightOuter)) {
            Object[][] newId = Arrays.copyOf(id, id.length);
            newId[rightTableIndex.get()] = null;
            newMultiTableIds.add(newId);
          }
        }
      }
      JoinReturn joinRet = new JoinReturn();
      joinRet.keys = newMultiTableIds;
      return joinRet;
    }

    return null;
  }

  private boolean isExpressionForRightTable(TableSchema rightTable, ExpressionImpl expression) {
    Set<ColumnImpl> columns = new HashSet<>();
    expression.getColumns(columns);
    for (ColumnImpl column : columns) {
      if (column.getTableName().equals(rightTable.getName())) {
        return true;
      }
    }
    return false;
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
}
