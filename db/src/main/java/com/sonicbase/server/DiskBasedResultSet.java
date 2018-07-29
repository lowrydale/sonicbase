package com.sonicbase.server;

import com.sonicbase.common.Record;
import com.sonicbase.common.ThreadUtil;
import com.sonicbase.procedure.StoredProcedureContextImpl;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.query.impl.*;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.server.support.MergeNFiles;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.Offset;
import org.apache.commons.io.FileUtils;
import org.apache.giraph.utils.Varint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@SuppressWarnings({"squid:S1172", "squid:S1168", "squid:S00107"})
// all methods called from method invoker must have cobj and replayed command parms
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class DiskBasedResultSet {

  private static final String RESULT_SETS_STR = "result-sets";
  private static Logger logger = LoggerFactory.getLogger(DiskBasedResultSet.class);

  private static AtomicLong nextResultSetId = new AtomicLong();

  private boolean setOperator;
  private List<OrderByExpressionImpl> orderByExpressions;
  private int count;
  private SelectStatementImpl select;
  private DatabaseServer server;
  private String[] tableNames;
  private long resultSetId;

  public DiskBasedResultSet(
      final short serializationVersion,
      final String dbName,
      DatabaseServer databaseServer,
      Offset offset,
      Limit limit,
      final String[] tableNames, int[] tableOffsets, final ResultSetImpl[] resultSets,
      final List<OrderByExpressionImpl> orderByExpressions,
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
        file = new File(server.getDataDir(), RESULT_SETS_STR + File.separator + databaseServer.getShard() +
            File.separator + server.getReplica() + File.separator + resultSetId);
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

    boolean selectAll = getKeepers(dbName, databaseServer, tableNames, orderByExpressions, select, tableOffsets2, keepers);

    ThreadPoolExecutor executor = ThreadUtil.createExecutor(resultSets.length, "SonicBase DiskBasedResultsSet Thread");
    List<Future> futures = new ArrayList<>();

    try {
      for (int k = 0; k < resultSets.length; k++) {
        processResultSet(serializationVersion, dbName, tableNames, resultSets, orderByExpressions, finalFile,
            fileOffset, keepers, selectAll, executor, futures, k);
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

    createOffsetLimitFile(offset, limit, file);

    updateAccessTime(file);
  }

  private boolean getKeepers(String dbName, DatabaseServer databaseServer, String[] tableNames,
                             List<OrderByExpressionImpl> orderByExpressions, SelectStatementImpl select,
                             Map<String, Integer> tableOffsets2, boolean[][] keepers) {
    boolean selectAll = getSelectAll(dbName, databaseServer, tableNames, select, tableOffsets2, keepers);

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
    return selectAll;
  }

  private boolean getSelectAll(String dbName, DatabaseServer databaseServer, String[] tableNames,
                               SelectStatementImpl select, Map<String, Integer> tableOffsets2, boolean[][] keepers) {
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
    return selectAll;
  }

  private void createOffsetLimitFile(Offset offset, Limit limit, File file) {
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
  }

  private void processResultSet(short serializationVersion, String dbName, String[] tableNames,
                                ResultSetImpl[] resultSets, List<OrderByExpressionImpl> orderByExpressions,
                                File finalFile, AtomicInteger fileOffset, boolean[][] keepers, boolean selectAll,
                                ThreadPoolExecutor executor, List<Future> futures, int k) {
    final int localK = k;
    final boolean finalSelectAll = selectAll;
    futures.add(executor.submit((Callable) () -> {
      ResultSetImpl rs = resultSets[localK];
      ExpressionImpl.CachedRecord[][] records = rs.getReadRecordsAndSerializedRecords();
      if (records == null) {
        return null;
      }

      List<ExpressionImpl.CachedRecord[]> batch = new ArrayList<>();
      setFieldsToNullAsNeeded(keepers, finalSelectAll, records, batch);
      while (true) {
        if (doProcessResultSet(serializationVersion, dbName, tableNames, orderByExpressions, finalFile, fileOffset,
            localK, rs, batch)) {
          break;
        }
      }

      sortAndWriteResultsToFile(serializationVersion, dbName, tableNames, orderByExpressions, finalFile, fileOffset,
          localK, batch);

      batch.clear();
      return null;
    }));
  }

  private void setFieldsToNullAsNeeded(boolean[][] keepers, boolean finalSelectAll,
                                       ExpressionImpl.CachedRecord[][] records, List<ExpressionImpl.CachedRecord[]> batch) {
    for (ExpressionImpl.CachedRecord[] row : records) {
      if (!finalSelectAll) {
        for (int i = 0; i < row.length; i++) {
          setFieldsToNullAsNeededProcessField(keepers[i], row[i]);
        }
      }
      batch.add(row);
    }
  }

  private void setFieldsToNullAsNeededProcessField(boolean[] keeper, ExpressionImpl.CachedRecord cachedRecord) {
    if (cachedRecord == null) {
      return;
    }
    for (int j = 0; j < cachedRecord.getRecord().getFields().length; j++) {
      if (!keeper[j]) {
        cachedRecord.getRecord().getFields()[j] = null;
      }
    }
  }

  private void sortAndWriteResultsToFile(short serializationVersion, String dbName, String[] tableNames,
                                         List<OrderByExpressionImpl> orderByExpressions, File finalFile,
                                         AtomicInteger fileOffset, int localK, List<ExpressionImpl.CachedRecord[]> batch) {
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
  }

  private boolean doProcessResultSet(short serializationVersion, String dbName, String[] tableNames,
                                     List<OrderByExpressionImpl> orderByExpressions, File finalFile,
                                     AtomicInteger fileOffset, int localK, ResultSetImpl rs,
                                     List<ExpressionImpl.CachedRecord[]> batch) {
    ExpressionImpl.CachedRecord[][] records;
    rs.setPageSize(1000);
    rs.forceSelectOnServer();
    long begin = System.currentTimeMillis();
    int schemaRetryCount = 0;
    rs.getMoreResults(schemaRetryCount);
    records = rs.getReadRecordsAndSerializedRecords();
    if (records == null) {
      return true;
    }
    logger.info("got more results: duration={}, recordCount={}", (System.currentTimeMillis() - begin), records.length);
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
      logger.info("sorted in-memory results: duration={}", (System.currentTimeMillis() - begin));

      for (int i = 0; i < batchRecords.length; i++) {
        ExpressionImpl.CachedRecord[] newRow = new ExpressionImpl.CachedRecord[tableNames.length];
        newRow[localK] = batchRecords[i][0];
        batchRecords[i] = newRow;
      }
      writeRecordsToFile(serializationVersion, finalFile, batchRecords, fileOffset.getAndIncrement());
      batch.clear();
    }
    return false;
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
                        int tableOffset, int tableCount, List<ExpressionImpl.CachedRecord[]> batch, File file,
                        AtomicInteger fileOffset) {
    ExpressionImpl.CachedRecord[] newRecord = new ExpressionImpl.CachedRecord[tableCount];
    System.arraycopy(record, 0, newRecord, tableOffset, record.length);
    batch.add(newRecord);
    if (batch.size() >= 500_000) {
      flushBatch(dbName, serializationVersion, batch, file, fileOffset);
    }
  }

  private void flushBatch(String dbName, short serializationVersion, List<ExpressionImpl.CachedRecord[]> batch,
                          File file, AtomicInteger fileOffset) {
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
        file = new File(server.getDataDir(), RESULT_SETS_STR + File.separator + databaseServer.getShard() +
            File.separator + server.getReplica() + File.separator + resultSetId);
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

    getKeepers(dbName, databaseServer, tableNames, orderByExpressions, tableOffsets, keepers, localSelectColumns);

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

    Comparator<ExpressionImpl.CachedRecord[]> comparator = getRecordComparator(localSelectColumns, fieldOffsets, comparators);

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

    processRecordsFromResultSets(serializationVersion, dbName, unique, intersect, except, file, fileOffset, comparator,
        lhsRs, rhsRs, batch, lhsRecord, rhsRecord, lastLhsRecord, lhsCount, rhsCount);

    flushBatch(dbName, serializationVersion, batch, file, fileOffset);

    mergeSort(serializationVersion, dbName, file);

    updateAccessTime(file);
  }

  private void getKeepers(String dbName, DatabaseServer databaseServer, String[] tableNames,
                          List<OrderByExpressionImpl> orderByExpressions, Map<String, Integer> tableOffsets,
                          boolean[][] keepers, List<ColumnImpl> localSelectColumns) {
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
  }

  private void processRecordsFromResultSets(Short serializationVersion, String dbName, boolean unique, boolean intersect,
                                            boolean except, File file, AtomicInteger fileOffset,
                                            Comparator<ExpressionImpl.CachedRecord[]> comparator, ResultSetContext lhsRs,
                                            ResultSetContext rhsRs, List<ExpressionImpl.CachedRecord[]> batch,
                                            ExpressionImpl.CachedRecord[] lhsRecord, ExpressionImpl.CachedRecord[] rhsRecord,
                                            ExpressionImpl.CachedRecord[] lastLhsRecord, int lhsCount, int rhsCount) {
    while (true) {
      if (lhsRecord == null) {
        rhsRecord = consumeRhsRecord(serializationVersion, dbName, intersect, except, file, fileOffset, comparator,
            rhsRs, batch, rhsRecord, lastLhsRecord, lhsCount, rhsCount);
      }
      if (rhsRecord == null) {
        ConsumeLhsRecord consumeLhsRecord = new ConsumeLhsRecord(serializationVersion, dbName, intersect, file,
            fileOffset, comparator, lhsRs, batch, lhsRecord, lastLhsRecord, lhsCount, rhsCount).invoke();
        lhsRecord = consumeLhsRecord.getLhsRecord();
        lastLhsRecord = consumeLhsRecord.getLastLhsRecord();
      }
      if (lhsRecord != null && rhsRecord != null) {
        ProcessRecords processRecords = new ProcessRecords(serializationVersion, dbName, unique, intersect, except,
            file, fileOffset, comparator, lhsRs, rhsRs, batch, lhsRecord, rhsRecord, lastLhsRecord, lhsCount, rhsCount).invoke();
        lhsRecord = processRecords.getLhsRecord();
        rhsRecord = processRecords.getRhsRecord();
        lastLhsRecord = processRecords.getLastLhsRecord();
        continue;
      }
      if (lhsRecord == null && rhsRecord == null) {
        break;
      }
    }
  }


  private ExpressionImpl.CachedRecord[] consumeRhsRecord(Short serializationVersion, String dbName, boolean intersect,
                                                         boolean except, File file, AtomicInteger fileOffset,
                                                         Comparator<ExpressionImpl.CachedRecord[]> comparator,
                                                         ResultSetContext rhsRs, List<ExpressionImpl.CachedRecord[]> batch,
                                                         ExpressionImpl.CachedRecord[] rhsRecord,
                                                         ExpressionImpl.CachedRecord[] lastLhsRecord, int lhsCount, int rhsCount) {
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
    return rhsRecord;
  }

  private Comparator<ExpressionImpl.CachedRecord[]> getRecordComparator(List<ColumnImpl> localSelectColumns,
                                                                        int[][] fieldOffsets, Comparator[] comparators) {
    return (o1, o2) -> {
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
      Integer compareValue = doCompareRecords(localSelectColumns, fieldOffsets, comparators, o1[lhsOffset], o2[rhsOffset]);
      if (compareValue != null) {
        return compareValue;
      }
      return 0;
    };
  }

  private Integer doCompareRecords(List<ColumnImpl> localSelectColumns, int[][] fieldOffsets, Comparator[] comparators,
                                   ExpressionImpl.CachedRecord cachedRecord, ExpressionImpl.CachedRecord cachedRecord1) {
    for (int i = 0; i < localSelectColumns.size(); i++) {
      Object lhsObj = cachedRecord.getRecord().getFields()[fieldOffsets[0][i]];
      Object rhsObj = cachedRecord1.getRecord().getFields()[fieldOffsets[1][i]];
      int compareValue = comparators[i].compare(lhsObj, rhsObj);
      if (compareValue < 0 || compareValue > 0) {
        return compareValue;
      }
    }
    return null;
  }

  public static void deleteOldResultSets(DatabaseServer server) {
    File file = new File(server.getDataDir(), RESULT_SETS_STR + File.separator + server.getShard() + File.separator +
        server.getReplica() + File.separator);
    File[] resultSets = file.listFiles();
    if (resultSets != null) {
      for (File resultSet : resultSets) {
        File timeFile = new File(resultSet, "time-accessed.txt");
        if (timeFile.exists()) {
          try {
            long updateTime = file.lastModified();
            if (updateTime < System.currentTimeMillis() - 24 * 60 * 60 * 1000) {
              FileUtils.deleteDirectory(resultSet);
              logger.info("Deleted old disk-based result set: dir={}", resultSet.getAbsolutePath());
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

  private void mergeNFiles(short serializationVersion, String dbName, File dir, File[] files) {
    new MergeNFiles().setSerializationVersion(serializationVersion).setDbName(dbName).setDir(dir).setFiles(files).
        setSetOperator(setOperator).setTableNames(tableNames).setOrderByExpressions(orderByExpressions).
        setServer(server).setCount(count).invoke();
  }

  public DiskBasedResultSet(DatabaseServer databaseServer, long resultSetId) {
    this.server = databaseServer;
    this.resultSetId = resultSetId;
  }

  public long getResultSetId() {
    return resultSetId;
  }

  private void writeRecordsToFile(short serializationVersion, File file, ExpressionImpl.CachedRecord[][] records,
                                  int fileOffset) {
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
      File file = new File(server.getDataDir(), RESULT_SETS_STR + File.separator + server.getShard() +
          File.separator + server.getReplica() + File.separator + resultSetId);
      FileUtils.deleteDirectory(file);
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public byte[][][] nextPage(int pageNumber) {
    try {
      File file = new File(server.getDataDir(), RESULT_SETS_STR + File.separator + server.getShard() +
          File.separator + server.getReplica() + File.separator + resultSetId);
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

  private class ConsumeLhsRecord {
    private Short serializationVersion;
    private String dbName;
    private boolean intersect;
    private File file;
    private AtomicInteger fileOffset;
    private Comparator<ExpressionImpl.CachedRecord[]> comparator;
    private ResultSetContext lhsRs;
    private List<ExpressionImpl.CachedRecord[]> batch;
    private ExpressionImpl.CachedRecord[] lhsRecord;
    private ExpressionImpl.CachedRecord[] lastLhsRecord;
    private int lhsCount;
    private int rhsCount;

    public ConsumeLhsRecord(Short serializationVersion, String dbName, boolean intersect, File file,
                            AtomicInteger fileOffset, Comparator<ExpressionImpl.CachedRecord[]> comparator,
                            ResultSetContext lhsRs, List<ExpressionImpl.CachedRecord[]> batch,
                            ExpressionImpl.CachedRecord[] lhsRecord, ExpressionImpl.CachedRecord[] lastLhsRecord,
                            int lhsCount, int rhsCount) {
      this.serializationVersion = serializationVersion;
      this.dbName = dbName;
      this.intersect = intersect;
      this.file = file;
      this.fileOffset = fileOffset;
      this.comparator = comparator;
      this.lhsRs = lhsRs;
      this.batch = batch;
      this.lhsRecord = lhsRecord;
      this.lastLhsRecord = lastLhsRecord;
      this.lhsCount = lhsCount;
      this.rhsCount = rhsCount;
    }

    public ExpressionImpl.CachedRecord[] getLhsRecord() {
      return lhsRecord;
    }

    public ExpressionImpl.CachedRecord[] getLastLhsRecord() {
      return lastLhsRecord;
    }

    public ConsumeLhsRecord invoke() {
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
      return this;
    }
  }

  private class ProcessRecordsEqual {
    private Short serializationVersion;
    private String dbName;
    private boolean unique;
    private boolean except;
    private File file;
    private AtomicInteger fileOffset;
    private ResultSetContext lhsRs;
    private ResultSetContext rhsRs;
    private List<ExpressionImpl.CachedRecord[]> batch;
    private ExpressionImpl.CachedRecord[] lhsRecord;
    private ExpressionImpl.CachedRecord[] rhsRecord;
    private int lhsCount;
    private int rhsCount;
    private ExpressionImpl.CachedRecord[] lastLhsRecord;

    public ProcessRecordsEqual(Short serializationVersion, String dbName, boolean unique, boolean except, File file,
                               AtomicInteger fileOffset, ResultSetContext lhsRs, ResultSetContext rhsRs,
                               List<ExpressionImpl.CachedRecord[]> batch, ExpressionImpl.CachedRecord[] lhsRecord,
                               ExpressionImpl.CachedRecord[] rhsRecord, int lhsCount, int rhsCount) {
      this.serializationVersion = serializationVersion;
      this.dbName = dbName;
      this.unique = unique;
      this.except = except;
      this.file = file;
      this.fileOffset = fileOffset;
      this.lhsRs = lhsRs;
      this.rhsRs = rhsRs;
      this.batch = batch;
      this.lhsRecord = lhsRecord;
      this.rhsRecord = rhsRecord;
      this.lhsCount = lhsCount;
      this.rhsCount = rhsCount;
    }

    public ExpressionImpl.CachedRecord[] getLhsRecord() {
      return lhsRecord;
    }

    public ExpressionImpl.CachedRecord[] getRhsRecord() {
      return rhsRecord;
    }

    public ExpressionImpl.CachedRecord[] getLastLhsRecord() {
      return lastLhsRecord;
    }

    public ProcessRecordsEqual invoke() {
      if (!except) {
        addRecord(dbName, serializationVersion, lhsRecord, 0, lhsCount + rhsCount, batch, file, fileOffset);
      }
      lastLhsRecord = lhsRecord;
      lhsRecord = lhsRs.nextRecord();
      if (!unique && !except) {
        addRecord(dbName, serializationVersion, rhsRecord, lhsCount, lhsCount + rhsCount, batch, file, fileOffset);
      }
      rhsRecord = rhsRs.nextRecord();
      return this;
    }
  }

  private class ProcessRecordLess {
    private Short serializationVersion;
    private String dbName;
    private boolean intersect;
    private File file;
    private AtomicInteger fileOffset;
    private ResultSetContext lhsRs;
    private List<ExpressionImpl.CachedRecord[]> batch;
    private ExpressionImpl.CachedRecord[] lhsRecord;
    private int lhsCount;
    private int rhsCount;
    private ExpressionImpl.CachedRecord[] lastLhsRecord;

    public ProcessRecordLess(Short serializationVersion, String dbName, boolean intersect, File file,
                             AtomicInteger fileOffset, ResultSetContext lhsRs, List<ExpressionImpl.CachedRecord[]> batch,
                             ExpressionImpl.CachedRecord[] lhsRecord, int lhsCount, int rhsCount) {
      this.serializationVersion = serializationVersion;
      this.dbName = dbName;
      this.intersect = intersect;
      this.file = file;
      this.fileOffset = fileOffset;
      this.lhsRs = lhsRs;
      this.batch = batch;
      this.lhsRecord = lhsRecord;
      this.lhsCount = lhsCount;
      this.rhsCount = rhsCount;
    }

    public ExpressionImpl.CachedRecord[] getLhsRecord() {
      return lhsRecord;
    }

    public ExpressionImpl.CachedRecord[] getLastLhsRecord() {
      return lastLhsRecord;
    }

    public ProcessRecordLess invoke() {
      if (!intersect) {
        addRecord(dbName, serializationVersion, lhsRecord, 0, lhsCount + rhsCount, batch, file,
            fileOffset);
      }
      lastLhsRecord = lhsRecord;
      lhsRecord = lhsRs.nextRecord();
      return this;
    }
  }

  private class ProcessRecords {
    private Short serializationVersion;
    private String dbName;
    private boolean unique;
    private boolean intersect;
    private boolean except;
    private File file;
    private AtomicInteger fileOffset;
    private Comparator<ExpressionImpl.CachedRecord[]> comparator;
    private ResultSetContext lhsRs;
    private ResultSetContext rhsRs;
    private List<ExpressionImpl.CachedRecord[]> batch;
    private ExpressionImpl.CachedRecord[] lhsRecord;
    private ExpressionImpl.CachedRecord[] rhsRecord;
    private ExpressionImpl.CachedRecord[] lastLhsRecord;
    private int lhsCount;
    private int rhsCount;

    public ProcessRecords(Short serializationVersion, String dbName, boolean unique, boolean intersect, boolean except,
                          File file, AtomicInteger fileOffset, Comparator<ExpressionImpl.CachedRecord[]> comparator,
                          ResultSetContext lhsRs, ResultSetContext rhsRs, List<ExpressionImpl.CachedRecord[]> batch,
                          ExpressionImpl.CachedRecord[] lhsRecord, ExpressionImpl.CachedRecord[] rhsRecord,
                          ExpressionImpl.CachedRecord[] lastLhsRecord, int lhsCount, int rhsCount) {
      this.serializationVersion = serializationVersion;
      this.dbName = dbName;
      this.unique = unique;
      this.intersect = intersect;
      this.except = except;
      this.file = file;
      this.fileOffset = fileOffset;
      this.comparator = comparator;
      this.lhsRs = lhsRs;
      this.rhsRs = rhsRs;
      this.batch = batch;
      this.lhsRecord = lhsRecord;
      this.rhsRecord = rhsRecord;
      this.lastLhsRecord = lastLhsRecord;
      this.lhsCount = lhsCount;
      this.rhsCount = rhsCount;
    }

    public ExpressionImpl.CachedRecord[] getLhsRecord() {
      return lhsRecord;
    }

    public ExpressionImpl.CachedRecord[] getRhsRecord() {
      return rhsRecord;
    }

    public ExpressionImpl.CachedRecord[] getLastLhsRecord() {
      return lastLhsRecord;
    }

    public ProcessRecords invoke() {
      if (lastLhsRecord != null) {
        if (0 == comparator.compare(lastLhsRecord, lhsRecord)) {
          lastLhsRecord = lhsRecord;
          lhsRecord = lhsRs.nextRecord();
          return this;
        }
        if (0 == comparator.compare(lastLhsRecord, rhsRecord)) {
          rhsRecord = rhsRs.nextRecord();
          return this;
        }
      }
      int compareValue = comparator.compare(lhsRecord, rhsRecord);
      if (compareValue == 0) {
        ProcessRecordsEqual processRecordsEqual = new ProcessRecordsEqual(serializationVersion, dbName, unique,
            except, file, fileOffset, lhsRs, rhsRs, batch, lhsRecord, rhsRecord, lhsCount, rhsCount).invoke();
        lhsRecord = processRecordsEqual.getLhsRecord();
        rhsRecord = processRecordsEqual.getRhsRecord();
        lastLhsRecord = processRecordsEqual.getLastLhsRecord();
      }
      else if (compareValue < 0) {
        ProcessRecordLess processRecordLess = new ProcessRecordLess(serializationVersion, dbName, intersect,
            file, fileOffset, lhsRs, batch, lhsRecord, lhsCount, rhsCount).invoke();
        lhsRecord = processRecordLess.getLhsRecord();
        lastLhsRecord = processRecordLess.getLastLhsRecord();
      }
      else if (compareValue > 0) {
        rhsRecord = processRecordGreater(serializationVersion, dbName, intersect, except, file, fileOffset, rhsRs,
            batch, rhsRecord, lhsCount, rhsCount);
      }
      return this;
    }

    private ExpressionImpl.CachedRecord[] processRecordGreater(Short serializationVersion, String dbName,
                                                               boolean intersect, boolean except, File file,
                                                               AtomicInteger fileOffset, ResultSetContext rhsRs,
                                                               List<ExpressionImpl.CachedRecord[]> batch,
                                                               ExpressionImpl.CachedRecord[] rhsRecord, int lhsCount,
                                                               int rhsCount) {
      if (!intersect && !except) {
        addRecord(dbName, serializationVersion, rhsRecord, lhsCount, lhsCount + rhsCount, batch, file, fileOffset);
      }
      rhsRecord = rhsRs.nextRecord();
      return rhsRecord;
    }
  }
}
