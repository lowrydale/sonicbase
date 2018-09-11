package com.sonicbase.server.support;

import com.sonicbase.common.Record;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.query.impl.OrderByExpressionImpl;
import com.sonicbase.schema.FieldSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.server.DatabaseServer;
import com.sonicbase.util.Varint;

import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings({"squid:S1168", "squid:S00107"}) // I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class MergeNFiles {

  private short serializationVersion;
  private String dbName;
  private File dir;
  private File[] files;
  private boolean setOperator;
  private String[] tableNames;
  private DatabaseServer server;
  private List<OrderByExpressionImpl> orderByExpressions;
  private int count;

  public MergeNFiles setSerializationVersion(short serializationVersion) {
    this.serializationVersion = serializationVersion;
    return this;
  }

  public MergeNFiles setDbName(String dbName) {
    this.dbName = dbName;
    return this;
  }

  public MergeNFiles setDir(File dir) {
    this.dir = dir;
    return this;
  }

  public MergeNFiles setFiles(File[] files) {
    this.files = files;
    return this;
  }

  public MergeNFiles setCount(int count) {
    this.count = count;
    return this;
  }

  public static class MergeRow {
    private int streamOffset;
    private Record[] row;
  }


  @SuppressWarnings("squid:S2093") // can't use try-with-resource bause out is assigned in the middle of the method
  public void invoke() {
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
          comparator = handleSetOperator();
        }
        else {
          comparator = handleNonSetOperator(comparator);
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

  private Comparator<MergeRow> handleNonSetOperator(Comparator<MergeRow> comparator) {
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

      comparator = getComparatorForNonSetOperatorWithOrderByExpression(fieldOffsets, ascendingFlags, comparators, tableOffsets);
    }
    return comparator;
  }

  private Comparator<MergeRow> handleSetOperator() {
    Comparator<MergeRow> comparator;
    if (orderByExpressions.isEmpty()) {
      comparator = getComparatorForSetOperatorWithoutOrderByExpression();
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

      comparator = getComparatorForSetOperatorWithOrderByExpression(fieldOffsets, comparators, ascendingFlags);
    }
    return comparator;
  }

  private Comparator<MergeRow> getComparatorForNonSetOperatorWithOrderByExpression(
      int[] fieldOffsets, boolean[] ascendingFlags, Comparator[] comparators, int[] tableOffsets) {
    Comparator<MergeRow> comparator;
    comparator = (o1, o2) -> {
      for (int i = 0; i < fieldOffsets.length; i++) {
        if (o1.row[tableOffsets[i]] == null && o2.row[tableOffsets[i]] == null) {
          continue;
        }
        if (o1.row[tableOffsets[i]] == null) {
          return -1 * (ascendingFlags[i] ? 1 : -1);
        }
        if (o2.row[tableOffsets[i]] == null) {
          return (ascendingFlags[i] ? 1 : -1);
        }

        int value = comparators[i].compare(o1.row[tableOffsets[i]].getFields()[fieldOffsets[i]],
            o2.row[tableOffsets[i]].getFields()[fieldOffsets[i]]);
        if (value < 0) {
          return -1 * (ascendingFlags[i] ? 1 : -1);
        }
        if (value > 0) {
          return (ascendingFlags[i] ? 1 : -1);
        }
      }
      return 0;
    };
    return comparator;
  }

  private Comparator<MergeRow> getComparatorForSetOperatorWithoutOrderByExpression() {
    Comparator<MergeRow> comparator;
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
    return comparator;
  }

  private Comparator<MergeRow> getComparatorForSetOperatorWithOrderByExpression(
      int[][] fieldOffsets, Comparator[] comparators, boolean[] ascendingFlags) {
    Comparator<MergeRow> comparator;
    comparator = (o1, o2) -> {
      for (int i = 0; i < fieldOffsets.length; i++) {
        for (int j = 0; j < o1.row.length; j++) {
          if (!(o1.row[j] == null && o2.row[j] == null)) {
            if (o1.row[j] == null) {
              return -1 * (ascendingFlags[i] ? 1 : -1);
            }
            if (o2.row[j] == null) {
              return (ascendingFlags[i] ? 1 : -1);
            }
            int value = comparators[i].compare(o1.row[j].getFields()[fieldOffsets[i][j]], o2.row[j].getFields()[fieldOffsets[i][j]]);
            if (value < 0) {
              return -1 * (ascendingFlags[i] ? 1 : -1);
            }
            if (value > 0) {
              return (ascendingFlags[i] ? 1 : -1);
            }
            break;
          }
        }
      }
      return 0;
    };
    return comparator;
  }

  public MergeNFiles setSetOperator(boolean setOperator) {
    this.setOperator = setOperator;
    return this;
  }

  public MergeNFiles setTableNames(String[] tableNames) {
    this.tableNames = tableNames;
    return this;
  }

  public MergeNFiles setServer(DatabaseServer server) {
    this.server = server;
    return this;
  }

  public MergeNFiles setOrderByExpressions(List<OrderByExpressionImpl> orderByExpressions) {
    this.orderByExpressions = orderByExpressions;
    return this;
  }
}
