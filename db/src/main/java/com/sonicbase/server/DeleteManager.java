package com.sonicbase.server;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.*;
import com.sonicbase.index.Index;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.DateUtils;
import org.apache.commons.io.FileUtils;
import org.apache.giraph.utils.Varint;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by lowryda on 5/15/17.
 */
public class DeleteManager {

  private Logger logger;

  private final DatabaseServer databaseServer;
  private ThreadPoolExecutor executor;
  private Thread mainThread;
  private ThreadPoolExecutor freeExecutor;
  private LogManager deltaLogManager;
  private boolean shutdown;

  public DeleteManager(DatabaseServer databaseServer) {
    this.databaseServer = databaseServer;
    logger = new Logger(databaseServer.getDatabaseClient());
    this.executor = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors() * 2,
        Runtime.getRuntime().availableProcessors() * 2, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
    this.freeExecutor = new ThreadPoolExecutor(4, 4, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
    this.deltaLogManager = new LogManager(databaseServer, new File(getDeltaRoot(), "log"));
  }

  public LogManager getDeltaLogManager() {
    return deltaLogManager;
  }

  public void deleteOldLogs(long lastSnapshot) {
    deltaLogManager.deleteOldLogs(lastSnapshot, true);

    File[] files = new File(getDeltaRoot(), "batch").listFiles();
    if (files != null) {
      for (File file : files) {
        try {
          int pos = file.getName().indexOf(".");
          String dateStr = file.getName().substring(0, pos);
          int pos2 = file.getName().lastIndexOf('-'); //get rid of random int on end
          dateStr = file.getName().substring(0, pos2);
          Date date = DateUtils.fromString(dateStr);
          if (date.getTime() < lastSnapshot) {
            file.delete();
          }
        }
        catch (Exception e) {
          logger.error("Error deleting batch deletes file: filename=" + file.getAbsolutePath());
        }
      }
    }

  }

  public void shutdown() {
    shutdown = true;
    if (mainThread != null) {
      mainThread.interrupt();
    }
    executor.shutdownNow();
    freeExecutor.shutdownNow();
    deltaLogManager.shutdown();

  }

  public static class DeleteRequest {
    private Object[] key;

    public DeleteRequest(Object[] key) {
      this.key = key;
    }

    public Object[] getKey() {
      return key;
    }
  }

  public static class DeleteRequestForRecord extends DeleteRequest {
    public DeleteRequestForRecord(Object[] key) {
      super(key);
    }
  }

  public static class DeleteRequestForKeyRecord extends DeleteRequest {
    private byte[] primaryKeyBytes;

    public DeleteRequestForKeyRecord(Object[] key) {
      super(key);
    }

    public DeleteRequestForKeyRecord(Object[] key, byte[] primaryKeyBytes) {
      super(key);
      this.primaryKeyBytes = primaryKeyBytes;
    }
  }

  static class OutputState {
    private DataOutputStream out;
    private int currFileNum;
    private int currOffset;
    private File dir;
    public ArrayBlockingQueue<MergeEntry> entries = new ArrayBlockingQueue<MergeEntry>(200_000);
  }

  public class DeltaContext {
    public DataInputStream in;
    private ArrayBlockingQueue<DeltaManager.MergeEntry> entries = new ArrayBlockingQueue<>(200_000);;
    public boolean finished;
    public int fileOffset;

    public DataInputStream getIn() {
      return in;
    }
  }

  private class DeleteContext {
    private DataInputStream in;
    private ArrayBlockingQueue<MergeEntry> entries = new ArrayBlockingQueue<>(200_000);;
    public boolean finished;
  }

  public void applyDeletesToSnapshot(final String dbName, int deltaNum, AtomicLong finishedBytes) {
    try {
      File file = new File(getDeltaRoot(), "tmp");
      outer:
      for (final Map.Entry<String, TableSchema> table : databaseServer.getCommon().getTables(dbName).entrySet()) {
        for (final Map.Entry<String, IndexSchema> index : table.getValue().getIndices().entrySet()) {
          Comparator[] comparator = index.getValue().getComparators();
          boolean isPrimaryKey = index.getValue().isPrimaryKey();
          File deleteFile = new File(file, table.getKey() + "/" + index.getKey() + "/merged");

          File deltaFile = databaseServer.getDeltaManager().getSortedDeltaFile(dbName,
              deltaNum == -1 ? "full" : String.valueOf(deltaNum), table.getKey(), index.getKey(), 0);
          if (!deltaFile.exists()) {
            break;
          }
          AtomicInteger currPartition = new AtomicInteger(0);
          File deletedDir = databaseServer.getDeltaManager().getDeletedDeltaDir(dbName,
              deltaNum == -1 ? "full" : String.valueOf(deltaNum), table.getKey(), index.getKey());
          FileUtils.deleteDirectory(deletedDir);
          deletedDir.mkdirs();
          AtomicReference<File> deletedFile = new AtomicReference<>(new File(deletedDir, currPartition + ".bin"));

          AtomicReference<DataOutputStream> deletedStream = new AtomicReference<>(
              new DataOutputStream(new BufferedOutputStream(new FileOutputStream(deletedFile.get()))));
          InputStream tmpIn = new DeltaManager.ByteCounterStream(finishedBytes, new FileInputStream(deltaFile));
          DataInputStream deltaIn = new DataInputStream(new BufferedInputStream(tmpIn));
          final DeltaManager deltaManager = databaseServer.getDeltaManager();
          long deltaFileSize = deltaFile.length();
          long sizePerPartition = deltaFileSize / DeltaManager.SNAPSHOT_PARTITION_COUNT;

          DataInputStream deleteIn = null;
          if (deleteFile.exists()) {
            deleteIn = new DataInputStream(new BufferedInputStream(new FileInputStream(deleteFile)));
          }

          final DeleteContext deleteContext = new DeleteContext();
          deleteContext.in = deleteIn;
          final DeltaContext deltaContext = new DeltaContext();
          deltaContext.in = deltaIn;
          deltaContext.fileOffset = 0;

//          Thread deleteThread = new Thread(new Runnable(){
//            @Override
//            public void run() {
//              while (true) {
//                MergeEntry entry = readRow(dbName, table.getValue(), index.getValue(), deleteContext.in);
//                if (entry == null) {
//                  deleteContext.finished = true;
//                  break;
//                }
//                try {
//                  deleteContext.entries.put(entry);
//                }
//                catch (InterruptedException e) {
//                  logger.error("Error reading delete row", e);
//                }
//              }
//            }
//          });
//          deleteThread.start();
//
//          Thread deltaThread = new Thread(new Runnable(){
//            @Override
//            public void run() {
//              while (true) {
//                DeltaManager.MergeEntry entry = deltaManager.readEntry(deltaContext.in, table.getValue());
//                if (entry == null) {
//                  deltaContext.finished = true;
//                  break;
//                }
//                try {
//                  deltaContext.entries.put(entry);
//                }
//                catch (InterruptedException e) {
//                  logger.error("Error reading delta entry", e);
//                }
//              }
//            }
//          });
//          deltaThread.start();

          try {
            List<MergeEntry> deletedEntries = new ArrayList<>();
            MergeEntry deleteEntry = null;
            MergeEntry pushedDeleteEntry = null;
            DeltaManager.MergeEntry deltaEntry = null;
            while (true) {
              while (true) {
                if (deleteIn != null) {
                  if (pushedDeleteEntry != null) {
                    deleteEntry = pushedDeleteEntry;
                    pushedDeleteEntry = null;
                  }
                  else {
                    deleteEntry = readRow(dbName, table.getValue(), index.getValue(), deleteContext.in);//readRow(deleteContext);
                  }
                  deletedEntries.clear();
                  if (deleteEntry != null) {
                    deletedEntries.add(deleteEntry);
                    while (true) {
                      MergeEntry currDeleteEntry = readRow(dbName, table.getValue(), index.getValue(), deleteContext.in);//readRow(deleteContext);
                      if (currDeleteEntry == null) {
                        break;
                      }
                      int comparison = DatabaseCommon.compareKey(comparator, deleteEntry.key, currDeleteEntry.key);
                      if (comparison == 0) {
                        deletedEntries.add(currDeleteEntry);
                      }
                      else {
                        pushedDeleteEntry = currDeleteEntry;
                        break;
                      }
                    }
                  }
                }
                if (deleteEntry == null) {
                  break;
                }
                if (deltaEntry == null) {
                  deltaEntry = deltaManager.readEntry(dbName, deltaNum, deltaContext, table.getValue(), index.getValue(), finishedBytes);//readRow(deltaContext);
                }
                if (deltaEntry == null) {
                  break;
                }
                int comparison = DatabaseCommon.compareKey(comparator, deleteEntry.key, deltaEntry.getKey());
                if (comparison >= 0) {
                  break;
                }
              }

              if (deleteEntry == null) {
                while (true) {
                  if (deltaEntry == null) {
                    deltaEntry = deltaManager.readEntry(dbName, deltaNum, deltaContext, table.getValue(), index.getValue(), finishedBytes);//readRow(deltaContext);
                  }
                  if (deltaEntry == null) {
                    break;
                  }
                  deltaManager.writeEntry(deletedStream.get(), table.getValue(), index.getKey(), deltaEntry);
                  cycleDeletedFile(deletedStream, deletedFile, currPartition, sizePerPartition, table.getKey(), index.getKey());
                  deltaEntry = deltaManager.readEntry(dbName, deltaNum, deltaContext, table.getValue(), index.getValue(), finishedBytes);//readRow(deltaContext);
                  if (deltaEntry == null) {
                    break;
                  }
                }
              }
              if (deltaEntry == null) {
                break;
              }

              while (true) {
                int comparison = -1;
                if (deleteEntry != null) {
                  comparison = DatabaseCommon.compareKey(comparator, deleteEntry.key, deltaEntry.getKey());
                  if (comparison < 0) {
                    break;
                  }
                }
                if (comparison == 0) {
                  byte[][] records = deltaEntry.getRecords();
                  if (!isPrimaryKey) {
                    List<byte[]> toKeep = new ArrayList<>();
                    for (byte[] record : records) {
                      boolean found = false;
                      for (MergeEntry currDeleteEntry : deletedEntries) {
                        if (Arrays.equals(KeyRecord.getPrimaryKey(record), currDeleteEntry.primaryKey)) {
                          found = true;
                          long deltaSequence0;
                          long deltaSequence1;
                          deltaSequence0 = KeyRecord.getSequence0(record);
                          deltaSequence1 = KeyRecord.getSequence1(record);
                          if (deltaSequence0 > currDeleteEntry.sequence0 ||
                              (deltaSequence0 == currDeleteEntry.sequence0 && deltaSequence1 > currDeleteEntry.sequence1)) {
                            toKeep.add(record);
                          }
                        }
                      }
                      if (!found) {
                        toKeep.add(record);
                      }
                    }
                    if (toKeep.size() != 0) {
                      deltaEntry.setRecords(toKeep.toArray(new byte[toKeep.size()][]));
                      deltaManager.writeEntry(deletedStream.get(), table.getValue(), index.getKey(), deltaEntry);
                    }
                  }
                  else {
                    long deltaSequence0;
                    long deltaSequence1;
                    deltaSequence0 = Record.getSequence0(records[0]);
                    deltaSequence1 = Record.getSequence1(records[0]);
                    if (deltaSequence0 > deleteEntry.sequence0 ||
                        (deltaSequence0 == deleteEntry.sequence0 && deltaSequence1 > deleteEntry.sequence1)) {
                      deltaManager.writeEntry(deletedStream.get(), table.getValue(), index.getKey(), deltaEntry);
                    }
                  }
                }
                else {
                  deltaManager.writeEntry(deletedStream.get(), table.getValue(), index.getKey(), deltaEntry);
                  cycleDeletedFile(deletedStream, deletedFile, currPartition, sizePerPartition, table.getKey(), index.getKey());
                }

                deltaEntry = deltaManager.readEntry(dbName, deltaNum, deltaContext, table.getValue(), index.getValue(), finishedBytes);//readRow(deltaContext);
                if (deltaEntry == null) {
                  break;
                }
              }
              if (deltaEntry == null) {
                break;
              }
            }
          }
          finally {
//            deleteThread.interrupt();
//            deltaThread.interrupt();
          }
          deletedStream.get().writeBoolean(false);
          deletedStream.get().close();
          if (deletedFile.get().length() == 0) {
            deletedFile.get().delete();
          }
        }
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }

  }

  private MergeEntry readRow(DeleteContext deleteContext) {
    try {
      while (true) {
        MergeEntry ret = deleteContext.entries.poll(100, TimeUnit.MILLISECONDS);
        if (ret != null) {
          return ret;
        }
        if (deleteContext.finished) {
          return null;
        }
      }
    }
    catch (InterruptedException e) {
      throw new DatabaseException(e);
    }
  }

  private DeltaManager.MergeEntry readRow(DeltaContext deltaContext) {
    try {
      while (true) {
        DeltaManager.MergeEntry ret = deltaContext.entries.poll(100, TimeUnit.MILLISECONDS);
        if (ret != null) {
          return ret;
        }
        if (deltaContext.finished) {
          return null;
        }
      }
    }
    catch (InterruptedException e) {
      throw new DatabaseException(e);
    }
  }

  private void cycleDeletedFile(AtomicReference<DataOutputStream> deletedStream, AtomicReference<File> deletedFile,
                                AtomicInteger currPartition, long sizePerPartition, String key, String key1) throws IOException {
    if (deletedFile.get().length() > sizePerPartition) {
      deletedStream.get().close();
      currPartition.incrementAndGet();
      File newFile = new File(deletedFile.get().getParentFile(), currPartition.get() + ".bin");
      deletedStream.set(new DataOutputStream(new BufferedOutputStream(new FileOutputStream(newFile))));
      deletedFile.set(newFile);
    }
  }

  public void delteTempDirs() {
    File file = new File(getDeltaRoot(), "tmp");
    try {
      FileUtils.deleteDirectory(file);
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public void buildDeletionsFiles(String dbName, AtomicReference<String> currStage, AtomicLong totalBytes, AtomicLong finishedBytes) {
    int cores = Runtime.getRuntime().availableProcessors();
    ThreadPoolExecutor executor = new ThreadPoolExecutor(cores, cores, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
    try {
      File file = new File(getDeltaRoot(), "tmp");
      FileUtils.deleteDirectory(file);
      Map<Integer, Map<Integer, OutputState>> streams = new HashMap<>();
      for (Map.Entry<String, TableSchema> table : databaseServer.getCommon().getTables(dbName).entrySet()) {
        Map<Integer, OutputState> indexState = new HashMap<Integer, OutputState>();
        streams.put(table.getValue().getTableId(), indexState);
        for (Map.Entry<String, IndexSchema> index : table.getValue().getIndices().entrySet()) {
          OutputState state = new OutputState();
          state.currFileNum = 0;
          state.currOffset = 0;
          File indexFile = new File(file, table.getKey() + "/" + index.getKey() + "/" + state.currFileNum + ".bin");
          state.dir = indexFile.getParentFile();
          indexFile.getParentFile().mkdirs();
          state.out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(indexFile), 64_000));
          indexState.put(index.getValue().getIndexId(), state);
        }
      }

      writeBatchDeletes(executor, streams, currStage, totalBytes, finishedBytes);
      writeLogDeletes(executor, dbName, streams, currStage, totalBytes, finishedBytes);

      closeFiles(dbName, streams);

      mergeSort(dbName, streams, currStage, totalBytes, finishedBytes);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
    finally {
      executor.shutdownNow();
    }
  }

  private void  mergeSort(String dbName, Map<Integer, Map<Integer, OutputState>> streams,
                          AtomicReference<String> currStage, AtomicLong totalBytes, AtomicLong finishedBytes) {
    long begin = System.currentTimeMillis();
    currStage.set("recoveringSnapshot - mergeDeletes - begin");
    totalBytes.set(0);
    finishedBytes.set(0);

    for (Map.Entry<Integer, Map<Integer, OutputState>> table : streams.entrySet()) {
      for (Map.Entry<Integer, OutputState> index : table.getValue().entrySet()) {

        File[] files = index.getValue().dir.listFiles();
        if (files == null) {
          return;
        }
        for (File file : files) {
          totalBytes.addAndGet(file.length());
        }
      }
    }
    for (Map.Entry<Integer, Map<Integer, OutputState>> table : streams.entrySet()) {
      for (Map.Entry<Integer, OutputState> index : table.getValue().entrySet()) {
        mergeSort(dbName, table.getKey(), index.getKey(), index.getValue().dir, finishedBytes);
      }
    }
    currStage.set("recoveringSnapshot - mergeDeletes - end: duration=" + (System.currentTimeMillis() - begin));
  }

  static class MergeEntry {
    private Object[] key;
    private long sequence0;
    private long sequence1;
    private byte[] primaryKey;
  }

  static class MergeRow {
    private int streamOffset;
    private MergeEntry row;
  }


  private void mergeSort(String dbName, int tableId, int indexId, File dir, AtomicLong finishedBytes) {
    try {
      File[] files = dir.listFiles();
      if (files == null) {
        return;
      }
      TableSchema tableSchema = databaseServer.getCommon().getTablesById(dbName).get(tableId);
      IndexSchema indexSchema = tableSchema.getIndexesById().get(indexId);
      final Comparator[] keyComparator = indexSchema.getComparators();
      File outFile = new File(dir, "merged");
      List<DataInputStream> inStreams = new ArrayList<>();
      DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(outFile)));
      try {
        for (File file : files) {
          DataInputStream in = new DataInputStream(new BufferedInputStream(new DeltaManager.ByteCounterStream(finishedBytes, new FileInputStream(file))));
          inStreams.add(in);
        }

        Comparator<MergeRow> comparator = new Comparator<MergeRow>(){
          @Override
          public int compare(MergeRow o1, MergeRow o2) {
            return DatabaseCommon.compareKey(keyComparator, o1.row.key, o2.row.key);
          }
        };

        ConcurrentSkipListMap<MergeRow, List<MergeRow>> currRows = new ConcurrentSkipListMap<>(comparator);

        for (int i = 0; i < inStreams.size(); i++) {
          DataInputStream in = inStreams.get(i);
          MergeEntry row = readRow(dbName, tableSchema, indexSchema, in);
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
            writeRow(row.row, out, tableSchema, indexSchema, rowNumber, page, dir);
            MergeEntry nextRow = readRow(dbName, tableSchema, indexSchema, inStreams.get(row.streamOffset));
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
        if (out != null) {
          out.close();
        }
        for (DataInputStream in : inStreams) {
          in.close();
        }
      }

      for (File file : files) {
        file.delete();
      }
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }

  }

  private DataOutputStream writeRow(MergeEntry row, DataOutputStream out, TableSchema tableSchema, IndexSchema indexSchema, AtomicInteger rowNumber, AtomicInteger page, File dir) {
    try {
      out.write(DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), row.key));
      Varint.writeSignedVarLong(row.sequence0, out);
      Varint.writeSignedVarLong(row.sequence1, out);
      if (!indexSchema.isPrimaryKey()) {
        Varint.writeSignedVarInt(row.primaryKey.length, out);
        out.write(row.primaryKey);
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
    return null;
  }

  private MergeEntry readRow(String dbName, TableSchema tableSchema, IndexSchema indexSchema, DataInputStream in) {
    try {
      MergeEntry ret = new MergeEntry();
      ret.key = DatabaseCommon.deserializeKey(tableSchema, in);
      ret.sequence0 = Varint.readSignedVarLong(in);
      ret.sequence1 = Varint.readSignedVarLong(in);
      if (!indexSchema.isPrimaryKey()) {
        int len = Varint.readSignedVarInt(in);
        ret.primaryKey = new byte[len];
        in.readFully(ret.primaryKey);
      }
      return ret;
    }
    catch (EOFException e) {
      return null;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private void writeLogDeletes(ThreadPoolExecutor executor, final String dbName, final Map<Integer, Map<Integer, OutputState>> streams, AtomicReference<String> currStage, AtomicLong totalBytes, final AtomicLong finishedBytes) {
    long begin = System.currentTimeMillis();
    logger.info("recoveringSnapshot - writeLogDeletes- begin");

    currStage.set("recoveringSnapshot - writeLogDeletes");
    totalBytes.set(0);
    finishedBytes.set(0);
    List<File> files = deltaLogManager.getLogFiles();
    List<Future> futures = new ArrayList<>();
    for (final File file : files) {
      futures.add(executor.submit(new Callable(){
        @Override
        public Object call() throws Exception {
          deltaLogManager.visitQueueEntries(new DataInputStream(new BufferedInputStream(new DeltaManager.ByteCounterStream(finishedBytes, new FileInputStream(file)))), new LogManager.LogVisitor() {
            @Override
            public boolean visit(byte[] buffer) {
              try {
                DataInputStream in = new DataInputStream(new ByteArrayInputStream(buffer));
                String readDbName = in.readUTF();

                int tableId = (int) Varint.readSignedVarLong(in);
                int indexId = (int) Varint.readSignedVarLong(in);
                TableSchema tableSchema = databaseServer.getCommon().getTablesById(dbName).get(tableId);
                Object[] key = DatabaseCommon.deserializeKey(tableSchema, in);
                long sequence0 = Varint.readSignedVarLong(in);
                long sequence1 = Varint.readSignedVarLong(in);

                MergeEntry entry = new MergeEntry();
                entry.key = key;
                entry.sequence0 = sequence0;
                entry.sequence1 = sequence1;
                OutputState state = streams.get(tableId).get(indexId);
                //          state.out.write(DatabaseCommon.serializeKey(tableSchema, tableSchema.getIndexesById().get(indexId).getName(), key));
                //          Varint.writeSignedVarLong(sequence0, state.out);
                //          Varint.writeSignedVarInt(sequence1, state.out);
                //
                if (!tableSchema.getIndexesById().get(indexId).isPrimaryKey()) {
                  int len = Varint.readSignedVarInt(in);
                  entry.primaryKey = new byte[len];
                  in.readFully(entry.primaryKey);
                }

                if (!readDbName.equals(dbName)) {
                  return true;
                }

                state.entries.add(entry);

                if (state.currOffset++ >= 100_000) {
                  cycleFile(dbName, tableId, indexId, state, true, false);
                }
              }
              catch (Exception e) {
                throw new DatabaseException(e);
              }
              return false;
            }
          });
          return null;
        }
      }));
    }
    for (Future future : futures) {
      try {
        future.get();
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }
    logger.info("recoveringSnapshot - writeBatchDeletes- end: duration=" + (System.currentTimeMillis() - begin));
  }

  private void closeFiles(String dbName, Map<Integer, Map<Integer, OutputState>> streams) {
    try {
      for (Map.Entry<Integer, Map<Integer, OutputState>> table : streams.entrySet()) {
        for (Map.Entry<Integer, OutputState> index : table.getValue().entrySet()) {
          cycleFile(dbName, table.getKey(), index.getKey(), index.getValue(), false, true);
          //index.getValue().out.close();
        }
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private void writeBatchDeletes(ThreadPoolExecutor executor, final Map<Integer, Map<Integer, OutputState>> streams,
                                 AtomicReference<String> currStage, AtomicLong totalBytes, final AtomicLong finishedBytes) {

    long begin = System.currentTimeMillis();
    logger.info("recoveringSnapshot - writeBatchDeletes- begin");

    currStage.set("recoveringSnapshot - writeBatchDeletes");
    totalBytes.set(0);
    finishedBytes.set(0);
    File[] files = new File(getDeltaRoot(), "batch").listFiles();
    if (files != null && files.length != 0) {
      for (File file : files) {
        totalBytes.addAndGet(file.length());
      }
    }

    if (files != null && files.length != 0) {
      Arrays.sort(files, new Comparator<File>() {
        @Override
        public int compare(File o1, File o2) {
          return o1.getAbsolutePath().compareTo(o2.getAbsolutePath());
        }
      });
      List<Future> futures = new ArrayList<>();
      for (final File file : files) {
        futures.add(executor.submit(new Callable(){
          @Override
          public Object call() throws Exception {
            try (DataInputStream in = new DataInputStream(new BufferedInputStream(new DeltaManager.ByteCounterStream(finishedBytes, new FileInputStream(file)), 64_000))) {
              short serializationVersion = (short) Varint.readSignedVarLong(in);
              String dbName = in.readUTF();
              String tableName = in.readUTF();
              TableSchema tableSchema = databaseServer.getCommon().getTables(dbName).get(tableName);

              String indexName = in.readUTF();
              final IndexSchema indexSchema = tableSchema.getIndices().get(indexName);
              long sequence0 = in.readLong();
              long sequence1 = in.readLong();
              boolean isRecord = in.readBoolean();

              long schemaVersionToDeleteAt = in.readInt();
              int errorsInARow = 0;
              while (true) {
                Object[] key = null;
                try {
                  key = DatabaseCommon.deserializeKey(tableSchema, in);
                  MergeEntry entry = new MergeEntry();
                  entry.key = key;
                  entry.sequence0 = sequence0;
                  entry.sequence1 = sequence1;
                  OutputState state = streams.get(tableSchema.getTableId()).get(indexSchema.getIndexId());

                  if (!isRecord) {
                    int len = Varint.readSignedVarInt(in);
                    entry.primaryKey = new byte[len];
                    in.readFully(entry.primaryKey);
                  }

                  state.entries.add(entry);

                  if (state.entries.size() >= 100_000) {
                    cycleFile(dbName, tableSchema.getTableId(), indexSchema.getIndexId(), state, true, false);
                  }

                  errorsInARow = 0;
                }
                catch (EOFException e) {
                  //expected
                  break;
                }
                catch (Exception e) {
                  logger.error("Error deserializing key: " + ((errorsInARow > 20) ? " aborting" : ""), e);
                  if (errorsInARow++ > 20) {
                    break;
                  }
                  continue;
                }

              }
            }
            catch (Exception e) {
              throw new DatabaseException(e);
            }
            return null;
          }
        }));
        for (Future future : futures) {
          try {
            future.get();
          }
          catch (Exception e) {
            throw new DatabaseException(e);
          }
        }
      }
    }
    logger.info("recoveringSnapshot - writeBatchDeletes- end: duration=" + (System.currentTimeMillis() - begin));
  }

  private void cycleFile(String dbName, int tableId, int indexId, OutputState state, boolean openNew, boolean force) {
    try {
      synchronized (state) {
        if (!force && state.entries.size() < 100_000) {
          return;
        }
        TableSchema tableSchema = databaseServer.getCommon().getTablesById(dbName).get(tableId);
        IndexSchema indexSchema = tableSchema.getIndexesById().get(indexId);
        final Comparator[] comparators = indexSchema.getComparators();
        List<MergeEntry> entries = new ArrayList<>();
        state.entries.drainTo(entries);
        Collections.sort(entries, new Comparator<MergeEntry>() {
          @Override
          public int compare(MergeEntry o1, MergeEntry o2) {
            return DatabaseCommon.compareKey(comparators, o1.key, o2.key);
          }
        });

        for (MergeEntry entry : entries) {
          state.out.write(DatabaseCommon.serializeKey(tableSchema, tableSchema.getIndexesById().get(indexId).getName(), entry.key));
          Varint.writeSignedVarLong(entry.sequence0, state.out);
          Varint.writeSignedVarLong(entry.sequence1, state.out);

          if (!tableSchema.getIndexesById().get(indexId).isPrimaryKey()) {
            Varint.writeSignedVarInt(entry.primaryKey.length, state.out);
            state.out.write(entry.primaryKey);
          }
        }

        state.out.close();
        state.currOffset = 0;
        if (openNew) {
          File indexFile = new File(state.dir, ++state.currFileNum + ".bin");
          state.out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(indexFile)));
        }
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public void saveDeleteForRecord(String dbName, String tableName, String indexName, long sequence0, long sequence1,
                                    DeleteRequestForRecord request) {
    try {
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytesOut);
      TableSchema tableSchema = databaseServer.getCommon().getTables(dbName).get(tableName);
      IndexSchema indexSchema = tableSchema.getIndices().get(indexName);
      out.writeUTF(dbName);
      Varint.writeSignedVarLong(tableSchema.getTableId(), out);
      Varint.writeSignedVarLong(indexSchema.getIndexId(), out);
      out.write(DatabaseCommon.serializeKey(tableSchema, indexName, request.getKey()));
      Varint.writeSignedVarLong(sequence0, out);
      Varint.writeSignedVarLong(sequence1, out);
      byte[] body = bytesOut.toByteArray();
      deltaLogManager.logRequest(body, true, "deleteRecord", sequence0, sequence1, new AtomicLong());
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public void saveDeleteForKeyRecord(String dbName, String tableName, String indexName, long sequence0, long sequence1,
                                  DeleteRequestForKeyRecord request) {
    try {
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytesOut);
      TableSchema tableSchema = databaseServer.getCommon().getTables(dbName).get(tableName);
      IndexSchema indexSchema = tableSchema.getIndices().get(indexName);
      out.writeUTF(dbName);
      Varint.writeSignedVarLong(tableSchema.getTableId(), out);
      Varint.writeSignedVarLong(indexSchema.getIndexId(), out);
      out.write(DatabaseCommon.serializeKey(tableSchema, indexName, request.getKey()));
      Varint.writeSignedVarLong(sequence0, out);
      Varint.writeSignedVarLong(sequence1, out);
      Varint.writeSignedVarInt(request.primaryKeyBytes.length, out);
      out.write(request.primaryKeyBytes);
      byte[] body = bytesOut.toByteArray();
      deltaLogManager.logRequest(body, true, "deleteRecord", sequence0, sequence1, new AtomicLong());
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public void saveDeletesForRecords(String dbName, String tableName, String indexName, long sequence0, long sequence1,
                                    ConcurrentLinkedQueue<DeleteRequest> keysToDelete) {
    doSaveDeletesForRecords(getStandardRoot(), dbName, tableName, indexName, sequence0, sequence1, keysToDelete);
    doSaveDeletesForRecords(new File(getDeltaRoot(), "batch"), dbName, tableName, indexName, sequence0, sequence1, keysToDelete);
  }

  public void saveDeletesForKeyRecords(String dbName, String tableName, String indexName, long sequence0, long sequence1,
                                    ConcurrentLinkedQueue<DeleteRequest> keysToDelete) {
    doSaveDeletesForKeyRecords(getStandardRoot(), dbName, tableName, indexName, sequence0, sequence1, keysToDelete);
    doSaveDeletesForKeyRecords(new File(getDeltaRoot(), "batch"), dbName, tableName, indexName, sequence0, sequence1, keysToDelete);
  }

  private void doSaveDeletesForRecords(File dir, String dbName, String tableName, String indexName, long sequence0,
                                       long sequence1, ConcurrentLinkedQueue<DeleteRequest> keysToDelete) {
    try {
      String dateStr = DateUtils.toString(new Date(System.currentTimeMillis()));
      File file = new File(dir, dateStr + "-0.bin");
      while (file.exists()) {
        Random rand = new Random(System.currentTimeMillis());
        file = new File(dir, dateStr + "-" + rand.nextInt(50000) + ".bin");
      }
      file.getParentFile().mkdirs();
      TableSchema tableSchema = databaseServer.getCommon().getTables(dbName).get(tableName);
      try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)))) {
        Varint.writeSignedVarLong(DatabaseClient.SERIALIZATION_VERSION, out);
        out.writeUTF(dbName);
        out.writeUTF(tableName);
        out.writeUTF(indexName);
        out.writeLong(sequence0);
        out.writeLong(sequence1);
        out.writeBoolean(true);
        out.writeInt(databaseServer.getCommon().getSchemaVersion() + 1);
        for (DeleteRequest request : keysToDelete) {
          out.write(DatabaseCommon.serializeKey(tableSchema, indexName, request.key));
        }
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private void doSaveDeletesForKeyRecords(File dir, String dbName, String tableName, String indexName, long sequence0,
                                       long sequence1, ConcurrentLinkedQueue<DeleteRequest> keysToDelete) {
    try {
      String dateStr = DateUtils.toString(new Date(System.currentTimeMillis()));
      File file = new File(dir, dateStr + ".bin");
      while (file.exists()) {
        Random rand = new Random(System.currentTimeMillis());
        file = new File(dir, dateStr + "-" + rand.nextInt(50000) + ".bin");
      }
      file.getParentFile().mkdirs();
      TableSchema tableSchema = databaseServer.getCommon().getTables(dbName).get(tableName);
      try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)))) {
        Varint.writeSignedVarLong(DatabaseClient.SERIALIZATION_VERSION, out);
        out.writeUTF(dbName);
        out.writeUTF(tableName);
        out.writeUTF(indexName);
        out.writeLong(sequence0);
        out.writeLong(sequence1);
        out.writeBoolean(false);
        out.writeInt(databaseServer.getCommon().getSchemaVersion() + 1);
        for (DeleteRequest request : keysToDelete) {
          out.write(DatabaseCommon.serializeKey(tableSchema, indexName, request.key));
          Varint.writeSignedVarInt(((DeleteRequestForKeyRecord)request).primaryKeyBytes.length, out);
          out.write(((DeleteRequestForKeyRecord)request).primaryKeyBytes);
        }
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private AtomicReference<LogManager.ByteCounterStream> counterStream = new AtomicReference<>();

  public void doDeletes(boolean ignoreVersion) {
    try {
      int countDeleted = 0;
      synchronized (this) {
        File dir = getStandardRoot();
        if (dir.exists()) {
          File[] files = dir.listFiles();
          if (files != null && files.length != 0) {
            Arrays.sort(files, new Comparator<File>() {
              @Override
              public int compare(File o1, File o2) {
                return o1.getAbsolutePath().compareTo(o2.getAbsolutePath());
              }
            });
            List<Future> futures = new ArrayList<>();
            counterStream.set(new LogManager.ByteCounterStream(new FileInputStream(files[0])));
            try (DataInputStream in = new DataInputStream(new BufferedInputStream(counterStream.get()))) {
              short serializationVersion = (short)Varint.readSignedVarLong(in);
              String dbName = in.readUTF();
              String tableName = in.readUTF();
              TableSchema tableSchema = databaseServer.getCommon().getTables(dbName).get(tableName);

              String indexName = in.readUTF();
              final IndexSchema indexSchema = tableSchema.getIndices().get(indexName);
              long sequence0 = in.readLong();
              long sequence1 = in.readLong();
              boolean isRecord = in.readBoolean();
              String[] indexFields = indexSchema.getFields();
              int[] fieldOffsets = new int[indexFields.length];
              for (int k = 0; k < indexFields.length; k++) {
                fieldOffsets[k] = tableSchema.getFieldOffset(indexFields[k]);
              }

              long schemaVersionToDeleteAt = in.readInt();
              if (!ignoreVersion && schemaVersionToDeleteAt > databaseServer.getCommon().getSchemaVersion()) {
                return;
              }
              final Index index = databaseServer.getIndices().get(dbName).getIndices().get(tableName).get(indexName);
              List<Object[]> batch = new ArrayList<>();
              int errorsInARow = 0;
              while (true) {
                Object[] key = null;
                try {
                   key = DatabaseCommon.deserializeKey(tableSchema, in);
                   if (!isRecord) {
                     int len = (int) Varint.readSignedVarLong(in);
                     byte[] primaryKeyBytes = new byte[len];
                     in.readFully(primaryKeyBytes);
                   }
                   errorsInARow = 0;
                }
                catch (EOFException e) {
                  //expected
                  break;
                }
                catch (Exception e) {
                  logger.error("Error deserializing key: " + ((errorsInARow > 20) ? " aborting" : ""), e);
                  if (errorsInARow++ > 20) {
                    break;
                  }
                  continue;
                }
                countDeleted++;
                batch.add(key);
                if (batch.size() > 100_000) {
                  final List<Object[]> currBatch = batch;
                  batch = new ArrayList<>();
                  futures.add(executor.submit(new Callable() {
                    @Override
                    public Object call() throws Exception {
                      final List<Object> toFreeBatch = new ArrayList<>();
                      for (Object[] currKey : currBatch) {
                        synchronized (index.getMutex(currKey)) {
                          Object value = index.get(currKey);
                          if (value != null) {
                            byte[][] content = databaseServer.fromUnsafeToRecords(value);
                            if (content != null) {
                              if (indexSchema.isPrimaryKey()) {
                                if ((Record.DB_VIEW_FLAG_DELETING & Record.getDbViewFlags(content[0])) != 0) {
                                  Object toFree = index.remove(currKey);
                                  if (toFree != null) {
                                    //  toFreeBatch.add(toFree);
                                    databaseServer.freeUnsafeIds(toFree);
                                  }
                                }
                              }
                              else {
                                if ((Record.DB_VIEW_FLAG_DELETING & KeyRecord.getDbViewFlags(content[0])) != 0) {
                                  Object toFree = index.remove(currKey);
                                  if (toFree != null) {
                                    //  toFreeBatch.add(toFree);
                                    databaseServer.freeUnsafeIds(toFree);
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                      doFreeMemory(toFreeBatch);
                      return null;
                    }
                  }));
                }
              }
              final List<Object> toFreeBatch = new ArrayList<>();
              for (Object[] currKey : batch) {


//                List<Integer> selectedShards = Repartitioner.findOrderedPartitionForRecord(true, false,
//                    fieldOffsets, databaseServer.getCommon(), tableSchema,
//                    indexName, null, BinaryExpression.Operator.equal, null, currKey, null);
//                if (selectedShards.get(0) == databaseServer.getShard()) {
//                  synchronized (index.getMutex(currKey)) {
//                    Object value = index.get(currKey);
//                    byte[][] content = databaseServer.fromUnsafeToRecords(value);
//                    if (content != null) {
//                      for (int i = 0; i < content.length; i++) {
//                        Record.setDbViewFlags(content[i], (short)0);
//                      }
//                      Object newValue = databaseServer.toUnsafeFromKeys(content);
//                      index.put(currKey, newValue);
//                      databaseServer.freeUnsafeIds(value);
//                    }
//                  }
//                }
//                else {
                  synchronized (index.getMutex(currKey)) {
                    Object value = index.get(currKey);
                    if (value != null) {
                      byte[][] content = databaseServer.fromUnsafeToRecords(value);
                      if (content != null) {
                        if (indexSchema.isPrimaryKey()) {
                          if ((Record.DB_VIEW_FLAG_DELETING & Record.getDbViewFlags(content[0])) != 0) {
                            Object toFree = index.remove(currKey);
                            if (toFree != null) {
                              //toFreeBatch.add(toFree);
                              databaseServer.freeUnsafeIds(toFree);
                            }
                          }
                        }
                        else {
                          if ((Record.DB_VIEW_FLAG_DELETING & KeyRecord.getDbViewFlags(content[0])) != 0) {
                            Object toFree = index.remove(currKey);
                            if (toFree != null) {
                              //toFreeBatch.add(toFree);
                              databaseServer.freeUnsafeIds(toFree);
                            }
                          }
                        }
                      }
                    }
                  }
//                }
              }
              doFreeMemory(toFreeBatch);
            }
            catch (Exception e) {
              logger.error("Error performing deletes", e);
            }

            for (Future future : futures) {
              future.get();
            }
            bytesRead.addAndGet(counterStream.get().getCount());
            counterStream.set(null);
            files[0].delete();
          }
        }
      }
      //System.out.println("deletes - finished: count=" + countDeleted + ", shard=" + databaseServer.getShard() + ", replica=" + databaseServer.getReplica());
    }
    catch (Exception e) {
      logger.error("Error performing deletes", e);
    }
  }

  private void doFreeMemory(final List<Object> toFreeBatch) {
//    Timer timer = new Timer("Free memory");
//    timer.schedule(new TimerTask(){
//      @Override
//      public void run() {
//        //limit to 4 threads
//        Future future = freeExecutor.submit(new Callable(){
//          @Override
//          public Object call() throws Exception {
            for (Object obj : toFreeBatch) {
              databaseServer.freeUnsafeIds(obj);
            }
//            return null;
//          }
//        });
//        try {
//          future.get();
//        }
//        catch (InterruptedException e) {
//        }
//        catch (ExecutionException e) {
//          logger.error("Error deleting values", e);
//        }
//      }
//    }, 30 * 1000);
  }

  private File getReplicaRoot() {
    return new File(databaseServer.getDataDir(), "deletes/" + databaseServer.getShard() + "/" + databaseServer.getReplica() + "/");
  }

  private File getStandardRoot() {
    return new File(getReplicaRoot(), "standard");
  }

  private File getDeltaRoot() {
    return new File(getReplicaRoot(), "delta");
  }

  public void start() {

    mainThread = new Thread(new Runnable() {
      @Override
      public void run() {
        while (!shutdown) {
          try {
            Thread.sleep(2_000);
            doDeletes(false);
          }
          catch (Exception e) {
            logger.error("Error procesing deletes file", e);
          }
        }
      }
    });
    mainThread.start();
  }

  public void backupAWS(String bucket, String prefix, String subDirectory) {
    AWSClient awsClient = databaseServer.getAWSClient();
    File srcDir = getReplicaRoot();
    subDirectory += "/deletes/" + databaseServer.getShard() + "/0";

    if (srcDir.exists()) {
      awsClient.uploadDirectory(bucket, prefix, subDirectory, srcDir);
    }
  }

  public void restoreAWS(String bucket, String prefix, String subDirectory) {
    try {
      AWSClient awsClient = databaseServer.getAWSClient();
      File destDir = getReplicaRoot();
      subDirectory += "/deletes/" + databaseServer.getShard() + "/0";

      FileUtils.deleteDirectory(destDir);
      destDir.mkdirs();

      awsClient.downloadDirectory(bucket, prefix, subDirectory, destDir);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public void backupFileSystem(String directory, String subDirectory) {
    try {
      File dir = getReplicaRoot();
      File destDir = new File(directory, subDirectory + "/deletes/" + databaseServer.getShard() + "/0");
      if (dir.exists()) {
        FileUtils.copyDirectory(dir, destDir);
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public void restoreFileSystem(String directory, String subDirectory) {
    try {
      File destDir = getReplicaRoot();
      if (destDir.exists()) {
        FileUtils.deleteDirectory(destDir);
      }
      destDir.mkdirs();
      File srcDir = new File(directory, subDirectory + "/deletes/" + databaseServer.getShard() + "/0");
      if (srcDir.exists()) {
        FileUtils.copyDirectory(srcDir, destDir);
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public void getFiles(List<String> files) {
    File dir = getStandardRoot();
    File[] currFiles = dir.listFiles();
    if (currFiles != null) {
      for (File file : currFiles) {
        files.add(file.getAbsolutePath());
      }
    }
  }

  private long totalBytes = 0;
  private AtomicLong bytesRead = new AtomicLong();

  public double getPercentDeleteComplete() {

    if (totalBytes == 0) {
      return 0;
    }
    LogManager.ByteCounterStream stream = counterStream.get();
    long readBytes = bytesRead.get();
    if (stream != null) {
      readBytes += stream.getCount();
    }

    return (double)readBytes / (double)totalBytes;
  }

  private AtomicBoolean isForcingDeletes = new AtomicBoolean();

  public boolean isForcingDeletes() {
    return isForcingDeletes.get();
  }

  public void forceDeletes() {
    File dir = getStandardRoot();
    totalBytes = 0;
    bytesRead.set(0);
    isForcingDeletes.set(true);
    try {
      if (dir.exists()) {
        File[] files = dir.listFiles();
        for (File file : files) {
          totalBytes += file.length();
        }
        while (true) {
          files = dir.listFiles();
          if (files == null || files.length == 0) {
            return;
          }
          doDeletes(true);
        }
      }
    }
    finally {
      isForcingDeletes.set(false);
    }
  }
}
