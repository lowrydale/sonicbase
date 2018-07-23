package com.sonicbase.server;

import com.sonicbase.common.Record;
import com.sonicbase.common.ThreadUtil;
import com.sonicbase.procedure.StoredProcedureContextImpl;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.query.impl.*;
import com.sonicbase.schema.FieldSchema;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.Offset;
import org.apache.commons.io.FileUtils;
import org.apache.giraph.utils.Varint;

import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Responsible for
 */
@SuppressWarnings("squid:S1168") // I prefer to return null instead of an empty array
public class DiskBasedResultSet {

  public static final String RESULT_SETS_STR = "result-sets";
  private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("com.sonicbase.logger");

  private static AtomicLong nextResultSetId = new AtomicLong();

  private boolean setOperator;
  private List<OrderByExpressionImpl> orderByExpressions;
  private int count;
  private SelectStatementImpl select;
  private DatabaseServer server;
  private String[] tableNames;
  private long resultSetId;


  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "EI_EXPOSE_REP2", justification = "copying the passed in data is too slow")
  @SuppressWarnings("PMD.ArrayIsStoredDirectly") //copying the passed in data is too slow
  public DiskBasedResultSet(
      final short serializationVersion,
      final String dbName,
      DatabaseServer databaseServer,
      Offset offset,
      Limit limit,
      final String[] tableNames, int[] tableOffsets, final ResultSetImpl[] resultSets, final List<OrderByExpressionImpl> orderByExpressions,
      int count, SelectStatementImpl select, boolean setOperator) {
    this.server = databaseServer;
    this.tableNames = tableNames;
    this.select = select;
    this.count = count;
    this.setOperator = setOperator;
    File file = null;
    this.orderByExpressions = orderByExpressions;
    synchronized (this) {
      while (true) {
        resultSetId = nextResultSetId.getAndIncrement();
        file = new File(server.getDataDir(), RESULT_SETS_STR + File.separator + databaseServer.getShard() + File.separator + server.getReplica() + File.separator + resultSetId);
        if (!file.exists()) {
          break;
        }
      }
      file.mkdirs();
    }
    final File finalFile = file;

    final AtomicInteger fileOffset = new AtomicInteger();

    Map<String, Integer> tableOffsets2 = new HashMap<>();
    final boolean[][] keepers = new boolean[tableNames.length][];
    for (int i = 0; i < tableNames.length; i++) {
      TableSchema tableSchema = databaseServer.getClient().getCommon().getTables(dbName).get(tableNames[i]);
      tableOffsets2.put(tableNames[i], i);
      keepers[i] = new boolean[tableSchema.getFields().size()];
      for (int j = 0; j < keepers[i].length; j++) {
        keepers[i][j] = false;
      }
    }

    boolean selectAll;
    if (select == null) {
      selectAll = true;
    }
    else {
      selectAll = false;
      List<ColumnImpl> selectColumns = select.getSelectColumns();
      if (selectColumns == null || selectColumns.isEmpty()) {
        selectAll = true;
      }

      for (ColumnImpl column : selectColumns) {
        String tableName = column.getTableName();
        String columnName = column.getColumnName();
        getKeepers(dbName, databaseServer, tableNames, tableOffsets2, keepers, columnName, tableName);
      }
    }

    for (int i = 0; i < tableNames.length; i++) {
      for (Map.Entry<String, IndexSchema> indexSchema : server.getCommon().getTables(dbName).get(tableNames[i]).getIndices().entrySet()) {
        if (indexSchema.getValue().isPrimaryKey()) {
          for (String column : indexSchema.getValue().getFields()) {
            getKeepers(dbName, databaseServer, tableNames, tableOffsets2, keepers, column, tableNames[i]);
          }
        }
      }
    }

    for (OrderByExpressionImpl expression : orderByExpressions) {
      String tableName = expression.getTableName();
      String columnName = expression.getColumnName();
      getKeepers(dbName, databaseServer, tableNames, tableOffsets2, keepers, columnName, tableName);
    }

    ThreadPoolExecutor executor = ThreadUtil.createExecutor(resultSets.length, "SonicBase DiskBasedResultsSet Thread");
    List<Future> futures = new ArrayList<>();

    try {
      for (int k = 0; k < resultSets.length; k++) {
        final int localK = k;
        final boolean finalSelectAll = selectAll;
        futures.add(executor.submit(new Callable() {
          @Override
          public Object call() throws Exception {
            ResultSetImpl rs = resultSets[localK];
            ExpressionImpl.CachedRecord[][] records = rs.getReadRecordsAndSerializedRecords();
            if (records == null) {
              return null;
            }

            List<ExpressionImpl.CachedRecord[]> batch = new ArrayList<>();
            for (ExpressionImpl.CachedRecord[] row : records) {
              if (!finalSelectAll) {
                for (int i = 0; i < row.length; i++) {
                  if (row[i] == null) {
                    continue;
                  }
                  for (int j = 0; j < row[i].getRecord().getFields().length; j++)
                    if (!keepers[i][j]) {
                      row[i].getRecord().getFields()[j] = null;
                    }
                }
              }
              batch.add(row);
            }
            while (true) {
              rs.setPageSize(1000);
              rs.forceSelectOnServer();
              long begin = System.currentTimeMillis();
              int schemaRetryCount = 0;
              rs.getMoreResults(schemaRetryCount);
              records = rs.getReadRecordsAndSerializedRecords();
              if (records == null) {
                break;
              }
              logger.info("got more results: duration=" + (System.currentTimeMillis() - begin) + ", recordCount=" + records.length);
              for (ExpressionImpl.CachedRecord[] row : records) {
                batch.add(row);
              }
              synchronized (rs.getRecordCache().getRecordsForTable()) {
                rs.getRecordCache().getRecordsForTable().clear();
              }
              if (batch.size() >= 500_000) {
                ExpressionImpl.CachedRecord[][] batchRecords = new ExpressionImpl.CachedRecord[batch.size()][];
                for (int i = 0; i < batchRecords.length; i++) {
                  batchRecords[i] = batch.get(i);
                }
                begin = System.currentTimeMillis();
                ResultSetImpl.sortResults(dbName, server.getClient().getCommon(), batchRecords, tableNames, orderByExpressions);
                logger.info("sorted in-memory results: duration=" + (System.currentTimeMillis() - begin));

                for (int i = 0; i < batchRecords.length; i++) {
                  ExpressionImpl.CachedRecord[] newRow = new ExpressionImpl.CachedRecord[tableNames.length];
                  newRow[localK] = batchRecords[i][0];
                  batchRecords[i] = newRow;
                }
                writeRecordsToFile(serializationVersion, finalFile, batchRecords, fileOffset.getAndIncrement());
                batch.clear();
              }
            }
            ExpressionImpl.CachedRecord[][] batchRecords = new ExpressionImpl.CachedRecord[batch.size()][];
            for (int i = 0; i < batchRecords.length; i++) {
              batchRecords[i] = batch.get(i);
            }
            ResultSetImpl.sortResults(dbName, server.getClient().getCommon(), batchRecords, tableNames, orderByExpressions);
            for (int i = 0; i < batchRecords.length; i++) {
              ExpressionImpl.CachedRecord[] newRow = new ExpressionImpl.CachedRecord[tableNames.length];
              newRow[localK] = batchRecords[i][0];
              batchRecords[i] = newRow;
            }
            writeRecordsToFile(serializationVersion, finalFile, batchRecords, fileOffset.getAndIncrement());
            batch.clear();
            return null;
          }
        }));
      }
      for (Future future : futures) {
        future.get();
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
    finally {
      executor.shutdownNow();
    }
    mergeSort(serializationVersion, dbName, file);

    File offsetLimitFile = new File(file, "offset-limit.txt");
    try {
      try (DataOutputStream out = new DataOutputStream(new FileOutputStream(offsetLimitFile))) {
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
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
    updateAccessTime(file);
  }

  public String[] getTableNames() {
    return tableNames;
  }

  class ResultSetContext {
    DatabaseServer databaseServer;
    String dbName;
    Object rs;
    int pageNum = 0;
    int pos = 0;
    ExpressionImpl.CachedRecord[][] records;

    public ResultSetContext(DatabaseServer databaseServer, String dbName, Object rs) {
      this.databaseServer = databaseServer;
      this.dbName = dbName;
      this.rs = rs;
    }

    ExpressionImpl.CachedRecord[] nextRecord() {
      if (records == null && !nextPage()) {
        return null;
      }
      if (pos >= records.length && !nextPage()) {
        return null;
      }
      return records[pos++];
    }

    public boolean nextPage() {
      if (rs instanceof ResultSetImpl) {
        if (records != null) {
          return false;
        }
        records = ((ResultSetImpl) rs).getReadRecordsAndSerializedRecords();
        pos = 0;
        return true;
      }
      byte[][][] bytes = ((DiskBasedResultSet) rs).nextPage(pageNum++);
      if (bytes == null) {
        return false;
      }
      records = new ExpressionImpl.CachedRecord[bytes.length][];
      for (int i = 0; i < records.length; i++) {
        records[i] = new ExpressionImpl.CachedRecord[bytes[i].length];
        for (int j = 0; j < records[i].length; j++) {
          if (bytes[i][j] != null) {
            Record record = new Record(dbName, databaseServer.getCommon(), bytes[i][j]);
            records[i][j] = new ExpressionImpl.CachedRecord(record, bytes[i][j]);
          }
        }
      }
      pos = 0;
      return true;
    }
  }

  public void addRecord(String dbName, short serializationVersion, ExpressionImpl.CachedRecord[] record,
                        int tableOffset, int tableCount, List<ExpressionImpl.CachedRecord[]> batch, File file, AtomicInteger fileOffset) {
    ExpressionImpl.CachedRecord[] newRecord = new ExpressionImpl.CachedRecord[tableCount];
    System.arraycopy(record, 0, newRecord, tableOffset, record.length);
    batch.add(newRecord);
    if (batch.size() >= 500_000) {
      flushBatch(dbName, serializationVersion, batch, file, fileOffset);
    }
  }

  private void flushBatch(String dbName, short serializationVersion, List<ExpressionImpl.CachedRecord[]> batch, File file, AtomicInteger fileOffset) {
    ExpressionImpl.CachedRecord[][] batchRecords = new ExpressionImpl.CachedRecord[batch.size()][];
    for (int i = 0; i < batchRecords.length; i++) {
      batchRecords[i] = batch.get(i);
    }
    ResultSetImpl.sortResults(dbName, server.getClient().getCommon(), batchRecords, tableNames, orderByExpressions);
    writeRecordsToFile(serializationVersion, file, batchRecords, fileOffset.getAndIncrement());
    batch.clear();
  }

  public DiskBasedResultSet(Short serializationVersion, String dbName, DatabaseServer databaseServer, String[] tableNames,
                            Object[] resultSets, List<OrderByExpressionImpl> orderByExpressions,
                            int count, boolean unique, boolean intersect, boolean except, List<ColumnImpl> selectColumns) {
    this.server = databaseServer;
    this.tableNames = tableNames;
    this.select = select;
    this.count = count;
    this.setOperator = setOperator;
    File file = null;
    this.orderByExpressions = orderByExpressions;
    synchronized (this) {
      while (true) {
        resultSetId = nextResultSetId.getAndIncrement();
        file = new File(server.getDataDir(), RESULT_SETS_STR + File.separator + databaseServer.getShard() + File.separator + server.getReplica() + File.separator + resultSetId);
        if (!file.exists()) {
          break;
        }
      }
      file.mkdirs();
    }
    AtomicInteger fileOffset = new AtomicInteger();

    Map<String, Integer> tableOffsets = new HashMap<>();
    boolean[][] keepers = new boolean[tableNames.length][];
    for (int i = 0; i < tableNames.length; i++) {
      TableSchema tableSchema = databaseServer.getClient().getCommon().getTables(dbName).get(tableNames[i]);
      tableOffsets.put(tableNames[i], i);
      keepers[i] = new boolean[tableSchema.getFields().size()];
      for (int j = 0; j < keepers[i].length; j++) {
        keepers[i][j] = false;
      }
    }

    final List<ColumnImpl> localSelectColumns = selectColumns == null ? new ArrayList<>() : selectColumns;

    for (ColumnImpl column : localSelectColumns) {
      String tableName = column.getTableName();
      String columnName = column.getColumnName();
      getKeepers(dbName, databaseServer, tableNames, tableOffsets, keepers, columnName, tableName);
    }

    for (int i = 0; i < tableNames.length; i++) {
      for (Map.Entry<String, IndexSchema> indexSchema : server.getCommon().getTables(dbName).get(tableNames[i]).getIndices().entrySet()) {
        if (indexSchema.getValue().isPrimaryKey()) {
          for (String column : indexSchema.getValue().getFields()) {
            getKeepers(dbName, databaseServer, tableNames, tableOffsets, keepers, column, tableNames[i]);
          }
        }
      }
    }

    for (OrderByExpressionImpl expression : orderByExpressions) {
      String tableName = expression.getTableName();
      String columnName = expression.getColumnName();
      getKeepers(dbName, databaseServer, tableNames, tableOffsets, keepers, columnName, tableName);
    }


    final int[][] fieldOffsets = new int[tableNames.length][];
    for (int i = 0; i < tableNames.length; i++) {
      fieldOffsets[i] = new int[localSelectColumns.size()];
      TableSchema tableSchema = databaseServer.getCommon().getTables(dbName).get(tableNames[i]);
      for (int j = 0; j < fieldOffsets[i].length; j++) {
        fieldOffsets[i][j] = tableSchema.getFieldOffset(localSelectColumns.get(j).getColumnName());
      }
    }

    final Comparator[] comparators = new Comparator[localSelectColumns.size()];
    TableSchema tableSchema = databaseServer.getCommon().getTables(dbName).get(tableNames[0]);

    for (int i = 0; i < localSelectColumns.size(); i++) {
      comparators[i] = tableSchema.getFields().get(fieldOffsets[0][i]).getType().getComparator();
    }

    Comparator<ExpressionImpl.CachedRecord[]> comparator = (o1, o2) -> {
      int lhsOffset = -1;
      for (int i = 0; i < o1.length; i++) {
        if (o1[i] != null) {
          lhsOffset = i;
          break;
        }
      }
      int rhsOffset = -1;
      for (int i = 0; i < o2.length; i++) {
        if (o2[i] != null) {
          rhsOffset = i;
          break;
        }
      }
      for (int i = 0; i < localSelectColumns.size(); i++) {
        Object lhsObj = o1[lhsOffset].getRecord().getFields()[fieldOffsets[0][i]];
        Object rhsObj = o2[rhsOffset].getRecord().getFields()[fieldOffsets[1][i]];
        int compareValue = comparators[i].compare(lhsObj, rhsObj);
        if (compareValue < 0 || compareValue > 0) {
          return compareValue;
        }
      }
      return 0;
    };

    ResultSetContext lhsRs = new ResultSetContext(databaseServer, dbName, resultSets[0]);
    ResultSetContext rhsRs = new ResultSetContext(databaseServer, dbName, resultSets[1]);
    List<ExpressionImpl.CachedRecord[]> batch = new ArrayList<>();

    ExpressionImpl.CachedRecord[] lhsRecord = lhsRs.nextRecord();
    ExpressionImpl.CachedRecord[] rhsRecord = rhsRs.nextRecord();
    ExpressionImpl.CachedRecord[] lastLhsRecord = null;
    int lhsCount = 0;
    int rhsCount = 0;
    if (lhsRecord != null) {
      lhsCount = lhsRecord.length;
    }
    if (rhsRecord != null) {
      rhsCount = rhsRecord.length;
    }
    while (true) {

      if (lhsRecord == null) {
        while (rhsRecord != null) {
          if (lastLhsRecord != null && 0 == comparator.compare(lastLhsRecord, rhsRecord)) {
            rhsRecord = rhsRs.nextRecord();
            continue;
          }
          if (!intersect && !except) {
            addRecord(dbName, serializationVersion, rhsRecord, lhsCount, lhsCount + rhsCount, batch, file, fileOffset);
          }
          rhsRecord = rhsRs.nextRecord();
        }
      }
      if (rhsRecord == null) {
        while (lhsRecord != null) {
          if (lastLhsRecord != null && 0 == comparator.compare(lastLhsRecord, lhsRecord)) {
            lastLhsRecord = lhsRecord;
            lhsRecord = lhsRs.nextRecord();
            continue;
          }
          if (!intersect) {
            addRecord(dbName, serializationVersion, lhsRecord, 0, lhsCount + rhsCount, batch, file, fileOffset);
          }
          lhsRecord = lhsRs.nextRecord();
        }
      }
      if (lhsRecord != null && rhsRecord != null) {
        if (lastLhsRecord != null) {
          if (0 == comparator.compare(lastLhsRecord, lhsRecord)) {
            lastLhsRecord = lhsRecord;
            lhsRecord = lhsRs.nextRecord();
            continue;
          }
          if (0 == comparator.compare(lastLhsRecord, rhsRecord)) {
            rhsRecord = rhsRs.nextRecord();
            continue;
          }
        }
        int compareValue = comparator.compare(lhsRecord, rhsRecord);
        if (compareValue == 0) {
          if (!except) {
            addRecord(dbName, serializationVersion, lhsRecord, 0, lhsCount + rhsCount, batch, file, fileOffset);
          }
          lastLhsRecord = lhsRecord;
          lhsRecord = lhsRs.nextRecord();
          if (!unique && !except) {
            addRecord(dbName, serializationVersion, rhsRecord, lhsCount, lhsCount + rhsCount, batch, file, fileOffset);
          }
          rhsRecord = rhsRs.nextRecord();
        }
        else if (compareValue < 0) {
          if (!intersect) {
            addRecord(dbName, serializationVersion, lhsRecord, 0, lhsCount + rhsCount, batch, file, fileOffset);
          }
          lastLhsRecord = lhsRecord;
          lhsRecord = lhsRs.nextRecord();
        }
        else if (compareValue > 0) {
          if (!intersect && !except) {
            addRecord(dbName, serializationVersion, rhsRecord, lhsCount, lhsCount + rhsCount, batch, file, fileOffset);
          }
          rhsRecord = rhsRs.nextRecord();
        }
      }
      if (lhsRecord == null && rhsRecord == null) {
        break;
      }
    }

    flushBatch(dbName, serializationVersion, batch, file, fileOffset);

    mergeSort(serializationVersion, dbName, file);

    updateAccessTime(file);
  }

  public static void deleteOldResultSets(DatabaseServer server) {
    File file = new File(server.getDataDir(), RESULT_SETS_STR + File.separator + server.getShard() + File.separator + server.getReplica() + File.separator);
    File[] resultSets = file.listFiles();
    if (resultSets != null) {
      for (File resultSet : resultSets) {
        File timeFile = new File(resultSet, "time-accessed.txt");
        if (timeFile.exists()) {
          try {
            long updateTime = file.lastModified();
            if (updateTime < System.currentTimeMillis() - 24 * 60 * 60 * 1000) {
              FileUtils.deleteDirectory(resultSet);
              logger.info("Deleted old disk-based result set: dir=" + resultSet.getAbsolutePath());
            }
          }
          catch (Exception e) {
            logger.error("Error deleting result set", e);
          }
        }
      }
    }
  }

  private void updateAccessTime(File file) {
    synchronized (this) {
      try {
        File timeFile = new File(file, "time-accessed.txt");
        FileUtils.forceMkdir(file);
        if (!timeFile.exists()) {
          boolean created = timeFile.createNewFile();
          if (!created) {
            throw new DatabaseException("Error creating File: path=" + timeFile.getAbsolutePath());
          }
        }
        else {
          boolean modified = timeFile.setLastModified(System.currentTimeMillis());
          if (!modified) {
            throw new DatabaseException("Error updating file time: path=" + timeFile.getAbsolutePath());
          }
        }
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }
  }

  private void getKeepers(String dbName, DatabaseServer databaseServer, String[] tableNames,
                          Map<String, Integer> tableOffsets, boolean[][] keepers, String column,
                          String table) {
    if (table == null) {
      for (int i = 0; i < tableNames.length; i++) {
        TableSchema tableSchema = databaseServer.getClient().getCommon().getTables(dbName).get(tableNames[i]);
        Integer offset = tableSchema.getFieldOffset(column);
        if (offset != null) {
          keepers[tableOffsets.get(tableNames[i])][offset] = true;
        }
      }
    }
    else {
      TableSchema tableSchema = databaseServer.getClient().getCommon().getTables(dbName).get(table);
      Integer offset = tableSchema.getFieldOffset(column);
      if (offset != null) {
        keepers[tableOffsets.get(table)][offset] = true;
      }
    }
  }

  private void mergeSort(short serializationVersion, String dbName, File file) {
    mergeNFiles(serializationVersion, dbName, file, file.listFiles());
  }

  static class MergeRow {
    private int streamOffset;
    private Record[] row;
  }

  @SuppressWarnings("squid:S2093") // can't use try-with-resource bause out is assigned in the middle of the method
  private void mergeNFiles(short serializationVersion, String dbName, File dir, File[] files) {
    try {
      String name = "page-0";
      File outFile = new File(dir, name);
      List<DataInputStream> inStreams = new ArrayList<>();

      DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(outFile), 65_000));
      try {
        for (File file : files) {
          DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(file), 65_000));
          inStreams.add(in);
        }

        Comparator<MergeRow> comparator = null;
        if (setOperator) {
          if (orderByExpressions.isEmpty()) {
            comparator = (o1, o2) -> {
              int pos1 = 0;
              int pos2 = 0;
              for (int i = 0; i < o1.row.length; i++) {
                if (o1.row[i] != null) {
                  pos1 = i;
                  break;
                }
              }
              for (int i = 0; i < o2.row.length; i++) {
                if (o2.row[i] != null) {
                  pos2 = i;
                  break;
                }
              }
              return Integer.compare(pos1, pos2);
            };
          }
          else {
            final int[][] fieldOffsets = new int[orderByExpressions.size()][];
            final Comparator[] comparators = new Comparator[orderByExpressions.size()];
            final boolean[] ascendingFlags = new boolean[orderByExpressions.size()];

            for (int i = 0; i < comparators.length; i++) {
              fieldOffsets[i] = new int[tableNames.length];
              for (int j = 0; j < tableNames.length; j++) {
                TableSchema tableSchema = server.getClient().getCommon().getTables(dbName).get(tableNames[j]);
                int fieldOffset = tableSchema.getFieldOffset(orderByExpressions.get(i).getColumnName());
                fieldOffsets[i][j] = tableSchema.getFieldOffset(orderByExpressions.get(i).getColumnName());
                ascendingFlags[i] = orderByExpressions.get(i).isAscending();
                FieldSchema fieldSchema = tableSchema.getFields().get(fieldOffset);
                comparators[i] = fieldSchema.getType().getComparator();
              }
            }

            comparator = (o1, o2) -> {
              for (int i = 0; i < fieldOffsets.length; i++) {
                for (int j = 0; j < o1.row.length; j++) {
                  if (o1.row[j] == null && o2.row[j] == null) {
                    continue;
                  }

                  if (o1.row[j] == null) {
                    return -1 * (ascendingFlags[i] ? 1 : -1);
                  }
                  if (o2.row[j] == null) {
                    return 1 * (ascendingFlags[i] ? 1 : -1);
                  }
                  int value = comparators[i].compare(o1.row[j].getFields()[fieldOffsets[i][j]], o2.row[j].getFields()[fieldOffsets[i][j]]);
                  if (value < 0) {
                    return -1 * (ascendingFlags[i] ? 1 : -1);
                  }
                  if (value > 0) {
                    return 1 * (ascendingFlags[i] ? 1 : -1);
                  }
                  break;
                }
              }
              return 0;
            };
          }
        }
        else {

          if (!orderByExpressions.isEmpty()) {
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
              TableSchema tableSchema = server.getClient().getCommon().getTables(dbName).get(tableName);
              fieldOffsets[i] = tableSchema.getFieldOffset(orderByExpressions.get(i).getColumnName());
              ascendingFlags[i] = orderByExpressions.get(i).isAscending();
              FieldSchema fieldSchema = tableSchema.getFields().get(fieldOffsets[i]);
              comparators[i] = fieldSchema.getType().getComparator();
            }

            comparator = (o1, o2) -> {
              for (int i = 0; i < fieldOffsets.length; i++) {
                if (o1.row[tableOffsets[i]] == null && o2.row[tableOffsets[i]] == null) {
                  continue;
                }
                if (o1.row[tableOffsets[i]] == null) {
                  return -1 * (ascendingFlags[i] ? 1 : -1);
                }
                if (o2.row[tableOffsets[i]] == null) {
                  return 1 * (ascendingFlags[i] ? 1 : -1);
                }

                int value = comparators[i].compare(o1.row[tableOffsets[i]].getFields()[fieldOffsets[i]], o2.row[tableOffsets[i]].getFields()[fieldOffsets[i]]);
                if (value < 0) {
                  return -1 * (ascendingFlags[i] ? 1 : -1);
                }
                if (value > 0) {
                  return 1 * (ascendingFlags[i] ? 1 : -1);
                }
              }
              return 0;
            };
          }
        }

        ConcurrentSkipListMap<MergeRow, List<MergeRow>> currRows = null;
        if (comparator != null) {
          currRows = new ConcurrentSkipListMap<>(comparator);
        }
        else {
          currRows = new ConcurrentSkipListMap<>((o1, o2) -> 0);
        }

        for (int i = 0; i < inStreams.size(); i++) {
          DataInputStream in = inStreams.get(i);
          Record[] row = readRow(dbName, in);
          if (row == null) {
            continue;
          }
          MergeRow mergeRow = new MergeRow();
          mergeRow.row = row;
          mergeRow.streamOffset = i;
          List<MergeRow> rows = currRows.get(mergeRow);
          if (rows == null) {
            rows = new ArrayList<>();
          }
          rows.add(mergeRow);
          currRows.put(mergeRow, rows);
        }

        AtomicInteger page = new AtomicInteger();
        AtomicInteger rowNumber = new AtomicInteger();
        while (true) {
          Map.Entry<MergeRow, List<MergeRow>> first = currRows.firstEntry();
          if (first == null || first.getKey().row == null) {
            break;
          }
          List<MergeRow> toAdd = new ArrayList<>();
          for (MergeRow row : first.getValue()) {
            out = writeRow(serializationVersion, row.row, out, rowNumber, page, dir);
            Record[] nextRow = readRow(dbName, inStreams.get(row.streamOffset));
            if (nextRow != null) {
              MergeRow mergeRow = new MergeRow();
              mergeRow.row = nextRow;
              mergeRow.streamOffset = row.streamOffset;
              toAdd.add(mergeRow);
            }
          }
          currRows.remove(first.getKey());
          for (MergeRow mergeRow : toAdd) {
            List<MergeRow> rows = currRows.get(mergeRow);
            if (rows == null) {
              rows = new ArrayList<>();
            }
            rows.add(mergeRow);
            currRows.put(mergeRow, rows);
          }
        }
      }
      finally {
        out.close();
        for (DataInputStream in : inStreams) {
          in.close();
        }
      }

      for (File file : files) {
        if (file.exists()) {
          Files.delete(file.toPath());
        }
      }
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  private DataOutputStream writeRow(short serializationVersion,
                                    Record[] row, DataOutputStream out, AtomicInteger rowNumber, AtomicInteger page,
                                    File file) {
    try {
      for (int i = 0; i < row.length; i++) {
        if (row[i] == null) {
          out.writeBoolean(false);
        }
        else {
          out.writeBoolean(true);
          byte[] bytes = row[i].serialize(server.getCommon(), serializationVersion);
          Varint.writeSignedVarLong(bytes.length, out);
          out.write(bytes);
        }
      }
      if (rowNumber.incrementAndGet() > count) {
        out.close();

        File outFile = new File(file, "page-" + page.incrementAndGet());
        out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(outFile), 65_000));
        rowNumber.set(0);
      }
      return out;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  private Record[] readRow(String dbName, DataInputStream in) {
    try {
      Record[] ret = new Record[tableNames.length];
      for (int i = 0; i < tableNames.length; i++) {
        if (in.readBoolean()) {
          int len = (int) Varint.readSignedVarLong(in);
          byte[] bytes = new byte[len];
          in.readFully(bytes);
          ret[i] = new Record(dbName, server.getClient().getCommon(), bytes);
        }
      }
      return ret;
    }
    catch (EOFException e) {
      return null;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public DiskBasedResultSet(DatabaseServer databaseServer, long resultSetId) {
    this.server = databaseServer;
    this.resultSetId = resultSetId;
  }

  public long getResultSetId() {
    return resultSetId;
  }

  private void writeRecordsToFile(short serializationVersion, File file, ExpressionImpl.CachedRecord[][] records, int fileOffset) {
    try {

      File subFile = new File(file, String.valueOf(fileOffset));

      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytesOut);

      for (int i = 0; i < records.length; i++) {
        for (int j = 0; j < records[0].length; j++) {
          ExpressionImpl.CachedRecord record = records[i][j];
          if (record == null) {
            out.writeBoolean(false);
          }
          else {
            out.writeBoolean(true);
            Record rec = record.getRecord();
            byte[] bytes = rec.serialize(server.getCommon(), serializationVersion);
            Varint.writeSignedVarLong(bytes.length, out);
            out.write(bytes);
          }
        }
      }
      try (RandomAccessFile randomAccessFile = new RandomAccessFile(subFile, "rwd")) {
        randomAccessFile.write(bytesOut.toByteArray());
      }
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "EI_EXPOSE_REP2", justification = "copying the passed in data is too slow")
  @SuppressWarnings("PMD.ArrayIsStoredDirectly") //copying the passed in data is too slow
  public DiskBasedResultSet(
      DatabaseServer databaseServer, SelectStatementImpl select, String[] tableNames, long resultSetId, boolean restrictToThisServer,
      StoredProcedureContextImpl procedureContext) {
    this.server = databaseServer;
    this.resultSetId = resultSetId;
    this.tableNames = tableNames;
    this.select = select;
  }

  public void delete() {
    try {
      File file = new File(server.getDataDir(), RESULT_SETS_STR + File.separator + server.getShard() + File.separator + server.getReplica() + File.separator + resultSetId);
      FileUtils.deleteDirectory(file);
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public byte[][][] nextPage(int pageNumber) {
    try {
      File file = new File(server.getDataDir(), RESULT_SETS_STR + File.separator + server.getShard() + File.separator + server.getReplica() + File.separator + resultSetId);
      if (!file.exists()) {
        return null;
      }
      updateAccessTime(file);
      File subFile = new File(file, "page-" + pageNumber);
      if (!subFile.exists()) {
        return null;
      }
      try (RandomAccessFile randomAccessFile = new RandomAccessFile(subFile, "r")) {
        byte[] buffer = new byte[(int) randomAccessFile.length()];
        randomAccessFile.readFully(buffer);

        List<byte[][]> records = readRecords(buffer);

        if (records.isEmpty()) {
          return null;
        }

        byte[][][] ret = new byte[records.size()][][];
        for (int i = 0; i < ret.length; i++) {
          ret[i] = records.get(i);
        }
        return ret;
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  @SuppressWarnings("squid:S2189") // EOFException ends the loop
  private List<byte[][]> readRecords(byte[] buffer) throws IOException {
    List<byte[][]> records = new ArrayList<>();
    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(buffer))) {
      while (true) {
        byte[][] row = new byte[tableNames.length][];
        for (int j = 0; j < tableNames.length; j++) {
          if (in.readBoolean()) {
            int len = (int) Varint.readSignedVarLong(in);
            byte[] bytes = new byte[len];
            in.readFully(bytes);
            row[j] = bytes;
          }
        }
        records.add(row);
      }
    }
    catch (EOFException e) {
      //expected
    }
    return records;
  }
}
