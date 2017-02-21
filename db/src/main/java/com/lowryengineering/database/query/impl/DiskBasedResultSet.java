package com.lowryengineering.database.query.impl;

import com.lowryengineering.database.common.Record;
import com.lowryengineering.database.query.DatabaseException;
import com.lowryengineering.database.schema.FieldSchema;
import com.lowryengineering.database.schema.IndexSchema;
import com.lowryengineering.database.schema.TableSchema;
import com.lowryengineering.database.server.DatabaseServer;
import com.lowryengineering.database.util.DataUtil;
import org.anarres.lzo.LzoDecompressor1x;
import org.anarres.lzo.LzoInputStream;
import org.anarres.lzo.LzoOutputStream;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Responsible for
 */
public class DiskBasedResultSet {

  private static Logger logger = LoggerFactory.getLogger(DiskBasedResultSet.class);

  private static AtomicLong nextResultSetId = new AtomicLong();
  private int count;
  private SelectStatementImpl select;
  private DatabaseServer server;
  private String[] tableNames;
  private long resultSetId;


  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="EI_EXPOSE_REP2", justification="copying the passed in data is too slow")
  @SuppressWarnings("PMD.ArrayIsStoredDirectly") //copying the passed in data is too slow
  public DiskBasedResultSet(
      String dbName,
      DatabaseServer databaseServer,
      String[] tableNames, ResultSetImpl resultSet, int count, SelectStatementImpl select) {
    this.server = databaseServer;
    this.tableNames = tableNames;
    this.select = select;
    this.count = count;
    File file = null;
    while (true) {
      resultSetId = nextResultSetId.getAndIncrement();
      file = new File(server.getDataDir(), "result-sets/" + databaseServer.getShard() + "/" + server.getReplica() + "/" + resultSetId);
      if (!file.exists()) {
        break;
      }
    }
    file.mkdirs();

    ExpressionImpl.CachedRecord[][] records = resultSet.getReadRecordsAndSerializedRecords();
    if (records == null) {
      return;
    }

//    if (records != null && records.length < count) {
//      ResultSetImpl.sortResults(server.getClient().getCommon(), select, records, tableNames);
//      sorted = true;
//    }
//    writeRecordsToFile(out, records);
    int fileOffset = 0;

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

    boolean selectAll = false;
    List<ColumnImpl> selectColumns = select.getSelectColumns();
    if (selectColumns == null || selectColumns.size() == 0) {
      selectAll = true;
    }

    for (int i = 0; i < tableNames.length; i++) {
      for (Map.Entry<String, IndexSchema> indexSchema : server.getCommon().getTables(dbName).get(tableNames[i]).getIndexes().entrySet()) {
        if (indexSchema.getValue().isPrimaryKey()) {
          for (String column : indexSchema.getValue().getFields()) {
            getKeepers(dbName, databaseServer, tableNames, tableOffsets, keepers, column, tableNames[i]);
          }
        }
      }
    }

    for (ColumnImpl column : selectColumns) {
      String tableName = column.getTableName();
      String columnName = column.getColumnName();
      getKeepers(dbName, databaseServer, tableNames, tableOffsets, keepers, columnName, tableName);
    }

    for (OrderByExpressionImpl expression : select.getOrderByExpressions()) {
      String tableName = expression.getTableName();
      String columnName = expression.getColumnName();
      getKeepers(dbName, databaseServer, tableNames, tableOffsets, keepers, columnName, tableName);
    }

    List<ExpressionImpl.CachedRecord[]> batch = new ArrayList<>();
    for (ExpressionImpl.CachedRecord[] row : records) {
      if (!selectAll) {
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
//    if (!sorted) {
    while (true) {
      resultSet.setPageSize(500000);
      resultSet.forceSelectOnServer();
      long begin = System.currentTimeMillis();
      resultSet.getMoreResults();
      records = resultSet.getReadRecordsAndSerializedRecords();
      if (records == null) {
        break;
      }
      logger.info("got more results: duration=" + (System.currentTimeMillis() - begin) + ", recordCount=" + records.length);
      for (ExpressionImpl.CachedRecord[] row : records) {
//        if (!selectAll) {
//          for (int i = 0; i < row.length; i++) {
//            if (row[i] == null) {
//              continue;
//            }
//            for (int j = 0; j < row[i].getFields().length; j++)
//            if (!keepers[i][j]) {
//              row[i].getFields()[j] = null;
//            }
//          }
//        }
        batch.add(row);
      }
      synchronized (resultSet.getRecordCache().getRecordsForTable()) {
        resultSet.getRecordCache().getRecordsForTable().clear();
      }
      if (batch.size() >= 500000) {
        ExpressionImpl.CachedRecord[][] batchRecords = new ExpressionImpl.CachedRecord[batch.size()][];
        for (int i = 0; i < batchRecords.length; i++) {
          batchRecords[i] = batch.get(i);
        }
        begin = System.currentTimeMillis();
        ResultSetImpl.sortResults(dbName, server.getClient().getCommon(), select, batchRecords, tableNames);
        logger.info("sorted in-memory results: duration=" + (System.currentTimeMillis() - begin));
        writeRecordsToFile(file, batchRecords, fileOffset++);
        batch.clear();
      }
    }
    ExpressionImpl.CachedRecord[][] batchRecords = new ExpressionImpl.CachedRecord[batch.size()][];
    for (int i = 0; i < batchRecords.length; i++) {
      batchRecords[i] = batch.get(i);
    }
    ResultSetImpl.sortResults(dbName, server.getClient().getCommon(), select, batchRecords, tableNames);
    writeRecordsToFile(file, batchRecords, fileOffset++);
    batch.clear();

    mergeSort(dbName, file);
  }

  private void getKeepers(String dbName, DatabaseServer databaseServer, String[] tableNames, Map<String, Integer> tableOffsets, boolean[][] keepers, String column,
                          String table) {
    if (table == null) {
      for (int i = 0; i < tableNames.length; i++) {
        TableSchema tableSchema = databaseServer.getClient().getCommon().getTables(dbName).get(tableNames[i]);
        Integer offset = tableSchema.getFieldOffset(column);
        if (offset != null) {
          keepers[tableOffsets.get(tableNames[i])][tableSchema.getFieldOffset(column)] = true;
        }
      }
    }
    else {
      TableSchema tableSchema = databaseServer.getClient().getCommon().getTables(dbName).get(table);
      Integer offset = tableSchema.getFieldOffset(column);
      if (offset != null) {
        keepers[tableOffsets.get(table)][tableSchema.getFieldOffset(column)] = true;
      }
    }
  }

  private void mergeSort(String dbName, File file) {

    mergeNFiles(dbName, file, file.listFiles());
//    while (true) {
//      File[] files = file.listFiles();
//      if (files.length == 1) {
//        break;
//      }
//      int offset = 0;
//      for (; offset < files.length; offset += 2) {
//        if (offset >= files.length - 1) {
//          break;
//        }
//        mergeTwoFiles(file, files[offset], files[offset + 1], files.length == 2);
//      }
//      if (offset - 1 == files.length) {
//        mergeTwoFiles(file, files[offset - 1], files[0], true);
//      }
//      if (files.length <= 2) {
//        return;
//      }
//    }
  }

  private void mergeTwoFiles(String dbName, File file, File file1, File file2, boolean lastTwoFiles) throws Exception {
    String name = "tmp";
    if (lastTwoFiles) {
      name = "page-0";
    }
    File outFile = new File(file, name);
    DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new LzoOutputStream(new FileOutputStream(outFile))));

    DataInputStream in1 = new DataInputStream(new BufferedInputStream(new LzoInputStream(new FileInputStream(file1), new LzoDecompressor1x())));
    DataInputStream in2 = new DataInputStream(new BufferedInputStream(new LzoInputStream(new FileInputStream(file2), new LzoDecompressor1x())));

    List<OrderByExpressionImpl> orderByExpressions = select.getOrderByExpressions();
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

    Comparator<Record[]> comparator = new Comparator<Record[]>() {
      @Override
      public int compare(Record[] o1, Record[] o2) {
        for (int i = 0; i < fieldOffsets.length; i++) {
          if (o1[tableOffsets[i]] == null && o2[tableOffsets[i]] == null) {
            continue;
          }
          if (o1[tableOffsets[i]] == null) {
            return -1 * (ascendingFlags[i] ? 1 : -1);
          }
          if (o2[tableOffsets[i]] == null) {
            return 1 * (ascendingFlags[i] ? 1 : -1);
          }

          int value = comparators[i].compare(o1[tableOffsets[i]].getFields()[fieldOffsets[i]], o2[tableOffsets[i]].getFields()[fieldOffsets[i]]);
          if (value < 0) {
            return -1 * (ascendingFlags[i] ? 1 : -1);
          }
          if (value > 0) {
            return 1 * (ascendingFlags[i] ? 1 : -1);
          }
        }
        return 0;
      }
    };

    AtomicInteger page = new AtomicInteger();
    AtomicInteger rowNumber = new AtomicInteger();
    DataUtil.ResultLength resultLength = new DataUtil.ResultLength();
    Record[] row1 = null;
    Record[] row2 = null;
    while (true) {
      if (row1 == null) {
        row1 = readRow(dbName, in1, resultLength);
      }
      if (row2 == null) {
        row2 = readRow(dbName, in2, resultLength);
      }
      if (row1 == null) {
        if (row2 == null) {
          break;
        }
        out = writeRow(row2, out, rowNumber, page, file, resultLength);
        row2 = null;
        continue;
      }
      if (row2 == null) {
        if (row1 == null) {
          break;
        }
        out = writeRow(row1, out, rowNumber, page, file, resultLength);
        row1 = null;
        continue;
      }

      int compareValue = comparator.compare(row1, row2);
      if (compareValue == 0) {
        out = writeRow(row1, out, rowNumber, page, file, resultLength);
        out = writeRow(row2, out, rowNumber, page, file, resultLength);
        row1 = null;
        row2 = null;
      }
      else if (compareValue == 1) {
        out = writeRow(row2, out, rowNumber, page, file, resultLength);
        row2 = null;
      }
      else {
        out = writeRow(row1, out, rowNumber, page, file, resultLength);
        row1 = null;
      }
    }
    out.close();

    in1.close();
    in2.close();

    file1.delete();
    file2.delete();
    if (!lastTwoFiles) {
      outFile.renameTo(file1);
    }
  }

  static class MergeRow {
    private int streamOffset;
    private Record[] row;
  }

  private void mergeNFiles(String dbName, File dir, File[] files) {
    try {
      String name = "page-0";
      File outFile = new File(dir, name);
      DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new LzoOutputStream(new FileOutputStream(outFile))));

      List<DataInputStream> inStreams = new ArrayList<>();
      for (File file : files) {
        DataInputStream in = new DataInputStream(new BufferedInputStream(new LzoInputStream(new FileInputStream(file), new LzoDecompressor1x())));
        inStreams.add(in);
      }

      List<OrderByExpressionImpl> orderByExpressions = select.getOrderByExpressions();
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

      Comparator<MergeRow> comparator = new Comparator<MergeRow>() {
        @Override
        public int compare(MergeRow o1, MergeRow o2) {
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
        }
      };

      ConcurrentSkipListMap<MergeRow, List<MergeRow>> currRows = new ConcurrentSkipListMap<MergeRow, List<MergeRow>>(comparator);
      DataUtil.ResultLength resultLength = new DataUtil.ResultLength();
      for (int i = 0; i < inStreams.size(); i++) {
        DataInputStream in = inStreams.get(i);
        Record[] row = readRow(dbName, in, resultLength);
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
          out = writeRow(row.row, out, rowNumber, page, dir, resultLength);
          Record[] nextRow = readRow(dbName, inStreams.get(row.streamOffset), resultLength);
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
      out.close();

      for (DataInputStream in : inStreams) {
        in.close();
      }

      for (File file : files) {
        file.delete();
      }
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  private DataOutputStream writeRow(
      Record[] row, DataOutputStream out, AtomicInteger rowNumber, AtomicInteger page,
      File file, DataUtil.ResultLength resultLength)  {
    try {
      for (int i = 0; i < row.length; i++) {
        if (row[i] == null) {
          out.writeBoolean(false);
        }
        else {
          out.writeBoolean(true);
          byte[] bytes = row[i].serialize(server.getCommon());
          DataUtil.writeVLong(out, bytes.length, resultLength);
          out.write(bytes);
        }
      }
      if (rowNumber.incrementAndGet() > count) {
        out.close();

        File outFile = new File(file, "page-" + page.incrementAndGet());
        out = new DataOutputStream(new BufferedOutputStream(new LzoOutputStream(new FileOutputStream(outFile))));
        rowNumber.set(0);
      }
      return out;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  private Record[] readRow(String dbName, DataInputStream in, DataUtil.ResultLength resultLength) {
    try {
      Record[] ret = new Record[tableNames.length];
      for (int i = 0; i < tableNames.length; i++) {
        if (in.readBoolean()) {
          int len = (int) DataUtil.readVLong(in, resultLength);
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

  private void writeRecordsToFile(File file, ExpressionImpl.CachedRecord[][] records, int fileOffset) {
    try {

      File subFile = new File(file, String.valueOf(fileOffset));
      BufferedOutputStream bufferedOut = new BufferedOutputStream(new LzoOutputStream(new FileOutputStream(subFile)));
      DataOutputStream out = new DataOutputStream(bufferedOut);

      DataUtil.ResultLength resultLength = new DataUtil.ResultLength();
      for (int i = 0; i < records.length; i++) {
        for (int j = 0; j < records[0].length; j++) {
          ExpressionImpl.CachedRecord record = records[i][j];
          if (record == null) {
            out.writeBoolean(false);
          }
          else {
            out.writeBoolean(true);
            Record rec = record.getRecord();//record.serialize(server.getCommon());
            byte[] bytes = rec.serialize(server.getCommon());
            DataUtil.writeVLong(out, bytes.length, resultLength);
            out.write(bytes);
          }
        }
      }
      out.close();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="EI_EXPOSE_REP2", justification="copying the passed in data is too slow")
  @SuppressWarnings("PMD.ArrayIsStoredDirectly") //copying the passed in data is too slow
  public DiskBasedResultSet(
      DatabaseServer databaseServer, SelectStatementImpl select,
      String[] tableNames, long resultSetId) {
    this.server = databaseServer;
    this.resultSetId = resultSetId;
    this.tableNames = tableNames;
    this.select = select;
  }

  public void delete() {
    try {
      File file = new File(server.getDataDir(), "result-sets/" + server.getShard() + "/" + server.getReplica() + "/" + resultSetId);
      FileUtils.deleteDirectory(file);
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public byte[][][] nextPage(int pageNumber, int count) throws IOException {
    File file = new File(server.getDataDir(), "result-sets/" + server.getShard() + "/" + server.getReplica() + "/" + resultSetId);
    if (!file.exists()) {
      return null;
    }
    File subFile = new File(file, "page-" + pageNumber);
    if (!subFile.exists()) {
      return null;
    }
    BufferedInputStream bufferedIn = new BufferedInputStream(new LzoInputStream(new FileInputStream(subFile), new LzoDecompressor1x()));
    DataInputStream in = new DataInputStream(bufferedIn);

    DataUtil.ResultLength resultLength = new DataUtil.ResultLength();

//    //skip to the right page
//    try {
//      for (int i = 0; i < pageNumber * count; i++) {
//        for (int j = 0; j < tableNames.length; j++) {
//          if (in.readBoolean()) {
//            int len = (int) DataUtil.readVLong(in, resultLength);
//            byte[] bytes = new byte[len];
//            in.readFully(bytes);
//          }
//        }
//      }
//    }
//    catch (EOFException e) {
//      //expected
//    }

    List<byte[][]> records = new ArrayList<>();
    try {
      while (true) {
        byte[][] row = new byte[tableNames.length][];
        for (int j = 0; j < tableNames.length; j++) {
          if (in.readBoolean()) {
            int len = (int) DataUtil.readVLong(in, resultLength);
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

    if (records.size() == 0) {
      return null;
    }

    byte[][][] ret = new byte[records.size()][][];
    for (int i = 0; i < ret.length; i++) {
      ret[i] = records.get(i);
    }

    return ret;
  }
}
