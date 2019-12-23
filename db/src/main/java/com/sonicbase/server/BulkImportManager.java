package com.sonicbase.server;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.*;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.DataType;
import com.sonicbase.schema.FieldSchema;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@SuppressWarnings({"squid:S1172", "squid:S1168", "squid:S00107"})
// all methods called from method invoker must have cobj and replayed command parms
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class BulkImportManager {

  private static final String SONICBASE_ID_STR = "_sonicbase_id";
  private static final String ERROR_IMPORTING_RECORDS_STR = "Error importing records";
  private static final String ERROR_CLOSING_CONNECTION_STR = "Error closing connection";
  private static final String FROM_STR = " from ";
  private static final String WHERE_STR = " where (";
  private static final String AND_STR = " and ";
  private static final String SELECT_STR = "select ";
  private static final int BULK_IMPORT_THREAD_COUNT_PER_SERVER = 4;

  private static final Logger logger = LoggerFactory.getLogger(BulkImportManager.class);

  private final com.sonicbase.server.DatabaseServer server;
  private boolean shutdown;
  private final ConcurrentHashMap<String, Long> preProcessCountExpected = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, AtomicLong> preProcessCountProcessed = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Boolean> preProcessFinished = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, String> preProcessException = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Long> importCountExpected = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, AtomicLong> importCountProcessed = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Boolean> importFinished = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, String> importException = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, AtomicBoolean> cancelBulkImport = new ConcurrentHashMap<>();
  private final AtomicInteger countBulkImportRunning = new AtomicInteger();
  private static final Map<DataType.Type, Extractor> extractorByType = new EnumMap<>(DataType.Type.class);
  private static final Map<DataType.Type, Extractor> extractorForInternalByType = new EnumMap<>(DataType.Type.class);
  private static final String UTF_8_STR = "utf-8";
  private final AtomicInteger countCoordinating = new AtomicInteger();
  private static final Map<DataType.Type, Setter> setterByType = new EnumMap<>(DataType.Type.class);
  private final AtomicInteger coordinatesCalled = new AtomicInteger();


  BulkImportManager(final DatabaseServer server) {
    this.server = server;
  }

  public void shutdown() {
    this.shutdown = true;
  }

  private static class BulkImportStatus {
    private long preProcessCountExpected;
    private long preProcessCountProcessed;
    private boolean preProcessFinished;
    private String preProcessException;
    private long countExpected;
    private long countProcessed;
    private boolean finished;
    private String exception;
  }

  @SuppressWarnings("squid:S1172") // cobj and replayedCommand are required
  public ComObject getBulkImportProgressOnServer(ComObject cobj, boolean replayedCommand) {
    ComObject retObj = new ComObject(1);

    int count = 0;
    for (String dbName : server.getDbNames(server.getDataDir())) {
      for (String tableName : server.getCommon().getTables(dbName).keySet()) {
        count++;
      }
    }

    ComArray array = retObj.putArray(ComObject.Tag.STATUSES, ComObject.Type.OBJECT_TYPE, count);
    for (String dbName : server.getDbNames(server.getDataDir())) {
      for (String tableName : server.getCommon().getTables(dbName).keySet()) {
        ComObject currObj = new ComObject(11);
        currObj.put(ComObject.Tag.DB_NAME, dbName);
        currObj.put(ComObject.Tag.TABLE_NAME, tableName);

        setPreProcessProgress(dbName, tableName, currObj);

        setImportProgress(array, dbName, tableName, currObj);
      }
    }
    return retObj;
  }

  private void setImportProgress(ComArray array, String dbName, String tableName, ComObject currObj) {
    AtomicLong processed = importCountProcessed.get(dbName + ":" + tableName);
    if (processed == null) {
      currObj.put(ComObject.Tag.COUNT_LONG, 0);
    }
    else {
      currObj.put(ComObject.Tag.COUNT_LONG, processed.get());
    }
    Long expected = importCountExpected.get(dbName + ":" + tableName);
    if (expected == null) {
      expected = 0L;
    }
    currObj.put(ComObject.Tag.EXPECTED_COUNT, expected);

    Boolean finished = importFinished.get(dbName + ":" + tableName);
    if (finished == null) {
      currObj.put(ComObject.Tag.FINISHED, false);
    }
    else {
      currObj.put(ComObject.Tag.FINISHED, finished);
    }
    String exception = importException.get(dbName + ":" + tableName);
    if (exception != null) {
      currObj.put(ComObject.Tag.EXCEPTION, exception);
    }
    array.add(currObj);
  }

  private void setPreProcessProgress(String dbName, String tableName, ComObject currObj) {
    AtomicLong preProcessed = preProcessCountProcessed.get(dbName + ":" + tableName);
    if (preProcessed == null) {
      currObj.put(ComObject.Tag.PRE_PROCESS_COUNT_PROCESSED, 0);
    }
    else {
      currObj.put(ComObject.Tag.PRE_PROCESS_COUNT_PROCESSED, preProcessed.get());
    }
    Long preProcessExpected = preProcessCountExpected.get(dbName + ":" + tableName);
    if (preProcessExpected == null) {
      preProcessExpected = 0L;
    }
    currObj.put(ComObject.Tag.PRE_PROCESS_EXPECTED_COUNT, preProcessExpected);
    Boolean preFinished = preProcessFinished.get(dbName + ":" + tableName);
    if (preFinished == null) {
      currObj.put(ComObject.Tag.PRE_PROCESS_FINISHED, false);
    }
    else {
      currObj.put(ComObject.Tag.PRE_PROCESS_FINISHED, preFinished);
    }
    String preException = preProcessException.get(dbName + ":" + tableName);
    if (preException != null) {
      currObj.put(ComObject.Tag.PRE_PROCESS_EXCEPTION, preException);
    }
  }

  @SuppressWarnings("squid:S2629") // info is always enabled
  public ComObject startBulkImportOnServer(final ComObject cobj, boolean replayedCommand) {
    final String dbName = cobj.getString(ComObject.Tag.DB_NAME);
    final String tableName = cobj.getString(ComObject.Tag.TABLE_NAME);
    importCountProcessed.put(dbName + ":" + tableName, new AtomicLong(0));
    importFinished.put(dbName + ":" + tableName, false);
    importException.remove(dbName + ":" + tableName);
    importCountExpected.put(dbName + ":" + tableName, cobj.getLong(ComObject.Tag.EXPECTED_COUNT));
    cancelBulkImport.put(dbName + ":" + tableName, new AtomicBoolean(false));

    if (!cobj.getBoolean(ComObject.Tag.SHOULD_PROCESS)) {
      importFinished.put(dbName + ":" + tableName, true);
      return null;
    }

    ComObject retObj = new ComObject(1);
    if (countBulkImportRunning.get() >= 1) {
      retObj.put(ComObject.Tag.ACCEPTED, false);
    }
    else {
      logger.info("startBulkImportOnServer - begin: db={}, table={}", dbName, tableName);
      retObj.put(ComObject.Tag.ACCEPTED, true);

      Thread thread = new Thread(() -> startImport(cobj, dbName, tableName));
      thread.start();
    }

    return retObj;
  }

  private class ProcessKey {
    private TableSchema tableSchema;
    private ComArray keys;
    private int currSlice;
    private ComObject cobj;
    private String tableName;
    private StringBuilder fieldsStr;
    private StringBuilder parmsStr;
    private String dbName;
    private List<FieldSchema> fields;
    private AtomicLong countRead;
    private AtomicInteger countSubmitted;
    private ThreadPoolExecutor executor;
    private Connection insertConn;
    private AtomicLong countProcessed;
    private AtomicInteger countFinished;

    ProcessKey countFinished(AtomicInteger countFinished) {
      this.countFinished = countFinished;
      return this;
    }

    ProcessKey countProcessed(AtomicLong countProcessed) {
      this.countProcessed = countProcessed;
      return this;
    }

    ProcessKey insertConn(Connection insertConn) {
      this.insertConn = insertConn;
      return this;
    }

    ProcessKey executor(ThreadPoolExecutor executor) {
      this.executor = executor;
      return this;
    }

    ProcessKey countSubmitted(AtomicInteger countSubmitted) {
      this.countSubmitted = countSubmitted;
      return this;
    }

    ProcessKey countRead(AtomicLong countRead) {
      this.countRead = countRead;
      return this;
    }

    ProcessKey fields(List<FieldSchema> fields) {
      this.fields = fields;
      return this;
    }

    ProcessKey dbName(String dbName) {
      this.dbName = dbName;
      return this;
    }

    ProcessKey parmsStr(StringBuilder parmsStr) {
      this.parmsStr = parmsStr;
      return this;
    }

    ProcessKey fieldsStr(StringBuilder fieldsStr) {
      this.fieldsStr = fieldsStr;
      return this;
    }

    ProcessKey tableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    ProcessKey cobj(ComObject cobj) {
      this.cobj = cobj;
      return this;
    }

    ProcessKey tableSchema(TableSchema tableSchema) {
      this.tableSchema = tableSchema;
      return this;
    }

    ProcessKey keys(ComArray keys) {
      this.keys = keys;
      return this;
    }

    ProcessKey currSlice(int currSlice) {
      this.currSlice = currSlice;
      return this;
    }

    private void insertRecords(List<Future> futures, ResultSet rs, int batchSize, List<Object[]> currBatch,
                               String dbName, final String tableName, final List<FieldSchema> fields,
                               AtomicLong countRead, AtomicInteger countSubmitted, ThreadPoolExecutor executor,
                               final Connection insertConn, final AtomicLong countProcessed, final AtomicInteger countFinished,
                               final StringBuilder fieldsStr, final StringBuilder parmsStr, int currSlice) throws SQLException {
      while (rs.next() && !cancelBulkImport.get(dbName + ":" + tableName).get()) {
        if (shutdown) {
          break;
        }
        final Object[] currRecord = getCurrRecordFromResultSet(rs, fields);

        currBatch.add(currRecord);
        countRead.incrementAndGet();

        if (currBatch.size() >= batchSize && !cancelBulkImport.get(dbName + ":" + tableName).get()) {
          countSubmitted.incrementAndGet();
          final List<Object[]> batchToProcess = currBatch;
          currBatch = new ArrayList<>();
          futures.add(executor.submit(() -> BulkImportManager.this.insertRecords(insertConn, countProcessed,
              countFinished, batchToProcess, tableName,
              fields, fieldsStr, parmsStr)));
        }
      }
      if (!currBatch.isEmpty() && !cancelBulkImport.get(dbName + ":" + tableName).get()) {
        countSubmitted.incrementAndGet();
        BulkImportManager.this.insertRecords(insertConn, countProcessed, countFinished, currBatch, tableName, fields,
            fieldsStr, parmsStr);
      }
      logger.info("bulkImport finished reading records: currSlice={}", currSlice);
    }

    private String processFirstSlice(String[] keyFields, ComArray keys, StringBuilder fieldsStr, String tableName,
                                     TableSchema tableSchema, ComObject cobj) throws EOFException {
      String statementStr;
      Object[] currKey = DatabaseCommon.deserializeKey(tableSchema, (byte[]) keys.getArray().get(1));
      Object[] lowerKey = cobj.getByteArray(ComObject.Tag.LOWER_KEY) == null ? null :
          DatabaseCommon.deserializeKey(tableSchema, cobj.getByteArray(ComObject.Tag.LOWER_KEY));
      if (lowerKey != null) {
        statementStr = SELECT_STR + fieldsStr.toString() + FROM_STR + tableName + WHERE_STR + keyFields[0] +
            " >= " + lowerKey[0] + AND_STR + keyFields[0] + " < " + currKey[0] + ")";
      }
      else {
        statementStr = SELECT_STR + fieldsStr.toString() + FROM_STR + tableName + WHERE_STR + keyFields[0] + " < " + currKey[0] + ")";
      }
      return statementStr;
    }

    private String processWhereClause(boolean haveExpression, ComObject cobj, String statementStr) {
      String whereClause = cobj.getString(ComObject.Tag.WHERE_CLAUSE);
      if (whereClause != null) {
        int pos = whereClause.toLowerCase().indexOf("where");
        whereClause = whereClause.substring(pos + "where".length()).trim();
        if (haveExpression) {
          statementStr = statementStr + " and (" + whereClause + ")";
        }
        else {
          statementStr = statementStr + " where " + whereClause;
        }
      }
      return statementStr;
    }

    private String processLastSlice(String[] keyFields, int currSlice, ComArray keys, StringBuilder fieldsStr,
                                    String tableName, TableSchema tableSchema, ComObject cobj) throws EOFException {
      String statementStr;
      Object[] currKey = DatabaseCommon.deserializeKey(tableSchema, (byte[]) keys.getArray().get(currSlice));
      Object[] upperKey = cobj.getByteArray(ComObject.Tag.NEXT_KEY) == null ? null :
          DatabaseCommon.deserializeKey(tableSchema, cobj.getByteArray(ComObject.Tag.NEXT_KEY));
      if (upperKey != null) {
        statementStr = SELECT_STR + fieldsStr.toString() + FROM_STR + tableName + WHERE_STR + keyFields[0] +
            " >= " + currKey[0] + AND_STR + keyFields[0] + " < " + upperKey[0] + ")";
      }
      else {
        statementStr = SELECT_STR + fieldsStr.toString() + FROM_STR + tableName + WHERE_STR +
            keyFields[0] + " >= " + currKey[0] + ")";
      }
      return statementStr;
    }

    @SuppressWarnings("squid:S2629") // info is always enabled
    void invoke() {
      final List<Future> futures = new ArrayList<>();
      Connection localConn = null;
      try {
        Object[] key = DatabaseCommon.deserializeKey(tableSchema, (byte[]) keys.getArray().get(currSlice));
        logger.info("bulkImport starting slave thread: currSlice={}, key={}", currSlice, DatabaseCommon.keyToString(key));
        String user = cobj.getString(ComObject.Tag.USER);
        String password = cobj.getString(ComObject.Tag.PASSWORD);

        if (user != null) {
          localConn = getConnection(cobj.getString(ComObject.Tag.CONNECT_STRING), user, password);
        }
        else {
          localConn = getConnection(cobj.getString(ComObject.Tag.CONNECT_STRING));
        }

        IndexSchema indexSchema = null;
        for (Map.Entry<String, IndexSchema> entry : tableSchema.getIndices().entrySet()) {
          if (entry.getValue().isPrimaryKey()) {
            indexSchema = entry.getValue();
            break;
          }
        }
        String[] keyFields = indexSchema.getFields();

        StringBuilder builder = new StringBuilder("bulkImport slave thread: table=" + tableName + ", keyFields=");
        for (String field : keyFields) {
          builder.append(",").append(field);
        }
        logger.info(builder.toString());

        boolean haveExpression = true;

        String statementStr = buildSelectStatement(keyFields, haveExpression, currSlice, keys, fieldsStr, tableName,
            tableSchema, cobj);

        processReads(futures, localConn, statementStr, currSlice, tableName, fieldsStr, parmsStr, dbName, fields,
            countRead, countSubmitted, executor, insertConn, countProcessed, countFinished);
      }
      catch (Exception e) {
        logger.error(ERROR_IMPORTING_RECORDS_STR, e);

        String exceptionStr = ExceptionUtils.getStackTrace(e);
        importException.put(dbName + ":" + tableName, exceptionStr);
      }
      finally {
        try {
          if (localConn != null) {
            localConn.close();
          }
        }
        catch (SQLException e) {
          logger.error(ERROR_CLOSING_CONNECTION_STR, e);
        }
      }
    }

    private String buildSelectStatement(String[] keyFields, boolean haveExpression, int currSlice, ComArray keys,
                                        StringBuilder fieldsStr, String tableName, TableSchema tableSchema,
                                        ComObject cobj) throws EOFException {
      String statementStr;
      if (currSlice == 0) {
        if (keys.getArray().size() == 1) {
          statementStr = SELECT_STR + fieldsStr.toString() + FROM_STR + tableName;
          haveExpression = false;
        }
        else {
          statementStr = processFirstSlice(keyFields, keys, fieldsStr, tableName, tableSchema, cobj);
        }
      }
      else if (currSlice == keys.getArray().size() - 1) {
        statementStr = processLastSlice(keyFields, currSlice, keys, fieldsStr, tableName, tableSchema, cobj);
      }
      else {
        Object[] lowerKey = DatabaseCommon.deserializeKey(tableSchema, (byte[]) keys.getArray().get(currSlice));
        Object[] upperKey = DatabaseCommon.deserializeKey(tableSchema, (byte[]) keys.getArray().get(currSlice + 1));
        statementStr = SELECT_STR + fieldsStr.toString() + FROM_STR + tableName + WHERE_STR + keyFields[0] +
            " >= " + lowerKey[0] + AND_STR + keyFields[0] + " < " + upperKey[0] + ")";
      }

      return processWhereClause(haveExpression, cobj, statementStr);
    }

    private void processReads(List<Future> futures, Connection localConn, String statementStr, int currSlice,
                              String tableName, StringBuilder fieldsStr, StringBuilder parmsStr, String dbName,
                              List<FieldSchema> fields, AtomicLong countRead, AtomicInteger countSubmitted,
                              ThreadPoolExecutor executor, Connection insertConn, AtomicLong countProcessed,
                              AtomicInteger countFinished) throws SQLException, InterruptedException, ExecutionException {
      try (PreparedStatement stmt = localConn.prepareStatement(statementStr)) {
        logger.info("bulkImport select statement: slice={}, str={}", currSlice, statementStr);
        logger.info("bulkImport upsert statement: slice={}, str=UPSERT INTO {} ({}) VALUES ({})", currSlice, tableName,
            fieldsStr, parmsStr);
        try (ResultSet rs = stmt.executeQuery()) {
          int batchSize = 100;
          List<Object[]> currBatch = new ArrayList<>();

          insertRecords(futures, rs, batchSize, currBatch, dbName, tableName, fields, countRead, countSubmitted,
              executor, insertConn, countProcessed, countFinished, fieldsStr, parmsStr, currSlice);
        }
        catch (Exception e) {
          throw new DatabaseException("Error executing query: sql=" + statementStr, e);
        }
      }
      for (Future future : futures) {
        future.get();
      }
    }
  }

  private void startImport(ComObject cobj, String dbName, String tableName) {
    final ThreadPoolExecutor executor = ThreadUtil.createExecutor(8, "SonicBase startBulkImportOnServer Thread");
    countBulkImportRunning.incrementAndGet();
    try {
      final ComArray keys = cobj.getArray(ComObject.Tag.KEYS);
      final TableSchema tableSchema = server.getCommon().getTables(dbName).get(tableName);
      final List<FieldSchema> fields = tableSchema.getFields();
      final Thread[] threads = new Thread[keys.getArray().size()];

      final AtomicLong countRead = new AtomicLong();
      final AtomicLong countProcessed = importCountProcessed.get(dbName + ":" + tableName);
      final AtomicInteger countSubmitted = new AtomicInteger();
      final AtomicInteger countFinished = new AtomicInteger();
      Config config = server.getConfig();
      List<Config.Shard> shards = config.getShards();
      Config.Shard shard = shards.get(0);
      List<Config.Replica> replicasArray = shard.getReplicas();
      final String address = replicasArray.get(0).getString("address");
      final int port = replicasArray.get(0).getInt("port");

      Class.forName("com.sonicbase.jdbcdriver.Driver");
      try (Connection insertConn = getConnection(dbName, address, port)) {
        final StringBuilder fieldsStr = new StringBuilder();
        final StringBuilder parmsStr = new StringBuilder();
        boolean first = true;
        for (FieldSchema field : fields) {
          if (field.getName().equals(SONICBASE_ID_STR)) {
            continue;
          }
          if (first) {
            first = false;
          }
          else {
            fieldsStr.append(",");
            parmsStr.append(",");
          }
          fieldsStr.append(field.getName());
          parmsStr.append("?");
        }

        for (int i = 0; i < keys.getArray().size(); i++) {
          if (shutdown) {
            break;
          }
          final int currSlice = i;
          threads[i] = new Thread(() -> new ProcessKey().tableSchema(tableSchema).keys(keys).currSlice(currSlice)
            .cobj(cobj).tableName(tableName).fieldsStr(fieldsStr).parmsStr(parmsStr)
            .dbName(dbName).fields(fields).countRead(countRead).countSubmitted(countSubmitted).executor(executor)
            .insertConn(insertConn).countProcessed(countProcessed).countFinished(countFinished).invoke());

          threads[i].start();
        }
        for (int i = 0; i < threads.length; i++) {
          threads[i].join();
        }
        importFinished.put(dbName + ":" + tableName, true);
      }
    }
    catch (Exception e) {
      logger.error(ERROR_IMPORTING_RECORDS_STR, e);

      String exceptionStr = ExceptionUtils.getStackTrace(e);
      importException.put(dbName + ":" + tableName, exceptionStr);
    }
    finally {
      countBulkImportRunning.decrementAndGet();
      executor.shutdownNow();
    }
  }


  interface Extractor {
    Object extract(ResultSet rs, String field) throws SQLException, UnsupportedEncodingException;
  }

  static {
    extractorByType.put(DataType.Type.BIT, ResultSet::getBoolean);
    extractorByType.put(DataType.Type.TINYINT, ResultSet::getByte);
    extractorByType.put(DataType.Type.SMALLINT, ResultSet::getShort);
    extractorByType.put(DataType.Type.INTEGER, ResultSet::getInt);
    extractorByType.put(DataType.Type.BIGINT, ResultSet::getLong);
    extractorByType.put(DataType.Type.FLOAT, ResultSet::getDouble);
    extractorByType.put(DataType.Type.REAL, ResultSet::getFloat);
    extractorByType.put(DataType.Type.DOUBLE, ResultSet::getDouble);
    extractorByType.put(DataType.Type.NUMERIC, ResultSet::getBigDecimal);
    extractorByType.put(DataType.Type.DECIMAL, ResultSet::getBigDecimal);
    extractorByType.put(DataType.Type.CHAR, ResultSet::getString);
    extractorByType.put(DataType.Type.VARCHAR, ResultSet::getString);
    extractorByType.put(DataType.Type.LONGVARCHAR, ResultSet::getString);
    extractorByType.put(DataType.Type.DATE, ResultSet::getDate);
    extractorByType.put(DataType.Type.TIME, ResultSet::getTime);
    extractorByType.put(DataType.Type.TIMESTAMP, ResultSet::getTimestamp);
    extractorByType.put(DataType.Type.BINARY, ResultSet::getBytes);
    extractorByType.put(DataType.Type.VARBINARY, ResultSet::getBytes);
    extractorByType.put(DataType.Type.LONGVARBINARY, ResultSet::getBytes);
    extractorByType.put(DataType.Type.BLOB, ResultSet::getBytes);
    extractorByType.put(DataType.Type.CLOB, ResultSet::getString);
    extractorByType.put(DataType.Type.BOOLEAN, ResultSet::getBoolean);
    extractorByType.put(DataType.Type.ROWID, ResultSet::getLong);
    extractorByType.put(DataType.Type.NCHAR, ResultSet::getString);
    extractorByType.put(DataType.Type.NVARCHAR, ResultSet::getString);
    extractorByType.put(DataType.Type.LONGNVARCHAR, ResultSet::getString);
    extractorByType.put(DataType.Type.NCLOB, ResultSet::getString);
  }

  static {
    extractorForInternalByType.put(DataType.Type.BIT, (ResultSet rs, String field) -> rs.getBoolean(field));
    extractorForInternalByType.put(DataType.Type.TINYINT, (ResultSet rs, String field) -> rs.getByte(field));
    extractorForInternalByType.put(DataType.Type.SMALLINT, (ResultSet rs, String field) -> rs.getShort(field));
    extractorForInternalByType.put(DataType.Type.INTEGER, (ResultSet rs, String field) -> rs.getInt(field));
    extractorForInternalByType.put(DataType.Type.BIGINT, (ResultSet rs, String field) -> rs.getLong(field));
    extractorForInternalByType.put(DataType.Type.FLOAT, (ResultSet rs, String field) -> rs.getDouble(field));
    extractorForInternalByType.put(DataType.Type.REAL, (ResultSet rs, String field) -> rs.getFloat(field));
    extractorForInternalByType.put(DataType.Type.DOUBLE, (ResultSet rs, String field) -> rs.getDouble(field));
    extractorForInternalByType.put(DataType.Type.NUMERIC, (ResultSet rs, String field) -> rs.getBigDecimal(field));
    extractorForInternalByType.put(DataType.Type.DECIMAL, (ResultSet rs, String field) -> rs.getBigDecimal(field));
    extractorForInternalByType.put(DataType.Type.CHAR, (ResultSet rs, String field) -> {
      String str = rs.getString(field);
      char[] chars = new char[str.length()];
      str.getChars(0, chars.length, chars, 0);
      return chars;
    });
    extractorForInternalByType.put(DataType.Type.VARCHAR, (ResultSet rs, String field) -> {
      String str = rs.getString(field);
      char[] chars = new char[str.length()];
      str.getChars(0, chars.length, chars, 0);
      return chars;
    });
    extractorForInternalByType.put(DataType.Type.LONGVARCHAR, (ResultSet rs, String field) -> {
      String str = rs.getString(field);
      char[] chars = new char[str.length()];
      str.getChars(0, chars.length, chars, 0);
      return chars;
    });
    extractorForInternalByType.put(DataType.Type.DATE, (ResultSet rs, String field) -> rs.getDate(field));
    extractorForInternalByType.put(DataType.Type.TIME, (ResultSet rs, String field) -> rs.getTime(field));
    extractorForInternalByType.put(DataType.Type.TIMESTAMP, (ResultSet rs, String field) -> rs.getTimestamp(field));
    extractorForInternalByType.put(DataType.Type.BINARY, (ResultSet rs, String field) -> rs.getBytes(field));
    extractorForInternalByType.put(DataType.Type.VARBINARY, (ResultSet rs, String field) -> rs.getBytes(field));
    extractorForInternalByType.put(DataType.Type.LONGVARBINARY, (ResultSet rs, String field) -> rs.getBytes(field));
    extractorForInternalByType.put(DataType.Type.BLOB, (ResultSet rs, String field) -> rs.getBytes(field));
    extractorForInternalByType.put(DataType.Type.CLOB, (ResultSet rs, String field) -> {
      String str = rs.getString(field);
      char[] chars = new char[str.length()];
      str.getChars(0, chars.length, chars, 0);
      return chars;
    });
    extractorForInternalByType.put(DataType.Type.BOOLEAN, (ResultSet rs, String field) -> rs.getBoolean(field));
    extractorForInternalByType.put(DataType.Type.ROWID, (ResultSet rs, String field) -> rs.getLong(field));
    extractorForInternalByType.put(DataType.Type.NCHAR, (ResultSet rs, String field) -> {
      String str = rs.getString(field);
      char[] chars = new char[str.length()];
      str.getChars(0, chars.length, chars, 0);
      return chars;
    });
    extractorForInternalByType.put(DataType.Type.NVARCHAR, (ResultSet rs, String field) -> {
      String str = rs.getString(field);
      char[] chars = new char[str.length()];
      str.getChars(0, chars.length, chars, 0);
      return chars;
    });
    extractorForInternalByType.put(DataType.Type.LONGNVARCHAR, (ResultSet rs, String field) -> {
      String str = rs.getString(field);
      char[] chars = new char[str.length()];
      str.getChars(0, chars.length, chars, 0);
      return chars;
    });
    extractorForInternalByType.put(DataType.Type.NCLOB, (ResultSet rs, String field) -> {
      String str = rs.getString(field);
      char[] chars = new char[str.length()];
      str.getChars(0, chars.length, chars, 0);
      return chars;
    });
  }

  protected Object[] getCurrRecordFromResultSet(ResultSet rs, List<FieldSchema> fields) throws SQLException {
    final Object[] currRecord = new Object[fields.size()];

    try {
      int offset = 0;
      for (FieldSchema field : fields) {
        if (field.getName().equals(SONICBASE_ID_STR)) {
          offset++;
          continue;
        }
        Extractor extractor = extractorByType.get(field.getType());
        Object value = extractor.extract(rs, field.getName());
        if (!rs.wasNull()) {
          currRecord[offset] = value;
        }

        offset++;
      }
      return currRecord;
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public int getCountCoordinating() {
    return countCoordinating.get();
  }

  @SuppressWarnings("squid:S1172") // replayedCommand is required
  public ComObject coordinateBulkImportForTable(final ComObject cobj, boolean replayedCommand) {
    final String dbName = cobj.getString(ComObject.Tag.DB_NAME);
    final String tableName = cobj.getString(ComObject.Tag.TABLE_NAME);

    importCountProcessed.put(dbName + ":" + tableName, new AtomicLong(0));
    importFinished.put(dbName + ":" + tableName, false);
    importException.remove(dbName + ":" + tableName);
    cancelBulkImport.put(dbName + ":" + tableName, new AtomicBoolean(false));

    preProcessCountProcessed.put(dbName + ":" + tableName, new AtomicLong(0));
    preProcessCountExpected.put(dbName + ":" + tableName, 0L);
    preProcessFinished.put(dbName + ":" + tableName, false);
    preProcessException.remove(dbName + ":" + tableName);

    ComObject retObj = new ComObject(1);
    if (!cobj.getBoolean(ComObject.Tag.SHOULD_PROCESS)) {
      logger.info("coordinateBulkImportForTable - begin. Not processing: db={}, table={}", dbName, tableName);
      preProcessFinished.put(dbName + ":" + tableName, true);
      retObj.put(ComObject.Tag.ACCEPTED, false);
      return retObj;
    }

    if (countCoordinating.get() >= 5) {
      logger.info("coordinateBulkImportForTable - begin. Not accepting: db={}, table={}", dbName, tableName);
      retObj.put(ComObject.Tag.ACCEPTED, false);
      preProcessFinished.put(dbName + ":" + tableName, true);
    }
    else {
      logger.info("coordinateBulkImportForTable - begin. Accepted: db={}, table={}", dbName, tableName);

      retObj.put(ComObject.Tag.ACCEPTED, true);

      Thread thread = new Thread(() -> coordinateBulkImportForTable(cobj, tableName, dbName));
      thread.start();
    }

    return retObj;
  }

  @SuppressWarnings("squid:S2077") //don't need variable bindings
  private void coordinateBulkImportForTable(ComObject cobj, String tableName, String dbName) {
    countCoordinating.incrementAndGet();
    Connection insertConn = null;
    try {

      Class.forName(cobj.getString(ComObject.Tag.DRIVER_NAME));

      final String user = cobj.getString(ComObject.Tag.USER);
      final String password = cobj.getString(ComObject.Tag.PASSWORD);

      Connection conn = null;
      long count = 0;
      try {
        if (user != null) {
          conn = getConnection(cobj.getString(ComObject.Tag.CONNECT_STRING), user, password);
        }
        else {
          conn = getConnection(cobj.getString(ComObject.Tag.CONNECT_STRING));
        }

        try (PreparedStatement stmt = conn.prepareStatement("select count(*) from " + tableName);
             ResultSet rs = stmt.executeQuery()) {
          rs.next();
          count = rs.getLong(1);
        }

        int serverCount = server.getShardCount() * server.getReplicationFactor();
        int totalThreadCount = BULK_IMPORT_THREAD_COUNT_PER_SERVER * serverCount;

        preProcessCountExpected.put(dbName + ":" + tableName, count);

        AtomicLong countProcessed = new AtomicLong();
        preProcessCountProcessed.put(dbName + ":" + tableName, countProcessed);

        TableSchema tableSchema = server.getCommon().getTables(dbName).get(tableName);
        IndexSchema indexSchema = null;
        for (Map.Entry<String, IndexSchema> entry : tableSchema.getIndices().entrySet()) {
          if (entry.getValue().isPrimaryKey()) {
            indexSchema = entry.getValue();
            break;
          }
        }

        if (indexSchema == null) {
          preProcessFinished.put(dbName + ":" + tableName, true);
          logger.info("doImportForNoPrimaryKey - begin");
          doImportForNoPrimaryKey(conn, count, serverCount, totalThreadCount, tableSchema,
              indexSchema, tableName, dbName, cobj);
        }
        else {
          logger.info("doCoordinateBulkLoad - begin");
          doCoordinateBulkLoad(conn, count, serverCount, totalThreadCount, countProcessed, tableSchema,
              indexSchema, tableName, dbName, cobj);
          preProcessFinished.put(dbName + ":" + tableName, true);
        }
      }
      finally {
        if (conn != null) {
          conn.close();
        }
      }
      logger.info("bulkImport finished inserting records");
    }
    catch (Exception e) {
      logger.error(ERROR_IMPORTING_RECORDS_STR, e);

      String exceptionStr = ExceptionUtils.getStackTrace(e);
      preProcessException.put(dbName + ":" + tableName, exceptionStr);

      throw new DatabaseException(e);
    }
    finally {
      countCoordinating.decrementAndGet();
      if (insertConn != null) {
        try {
          insertConn.close();
        }
        catch (SQLException e) {
          logger.error(ERROR_CLOSING_CONNECTION_STR, e);
        }
      }
    }
  }

  @SuppressWarnings("squid:S2077") //don't need variable bindings
  private void doImportForNoPrimaryKey(Connection conn, long count, int serverCount, int totalThreadCount,
                                       TableSchema tableSchema, IndexSchema indexSchema,
                                       final String tableName, final String dbName, ComObject cobj) {

    importCountProcessed.put(dbName + ":" + tableName, new AtomicLong(0));
    importFinished.put(dbName + ":" + tableName, false);
    importException.remove(dbName + ":" + tableName);
    importCountExpected.put(dbName + ":" + tableName, cobj.getLong(ComObject.Tag.EXPECTED_COUNT));
    cancelBulkImport.put(dbName + ":" + tableName, new AtomicBoolean(false));
    try {
      cobj.put(ComObject.Tag.METHOD, "BulkImportManager:startBulkImportOnServer");
      cobj.put(ComObject.Tag.TABLE_NAME, tableName);
      for (int shard = 0; shard < server.getShardCount(); shard++) {
        for (int replica = 0; replica < server.getReplicationFactor(); replica++) {
          if (shard == server.getShard() && replica == server.getReplica()) {
            continue;
          }
          cobj.put(ComObject.Tag.SHOULD_PROCESS, false);
          server.getClient().send(null, shard, replica, cobj, DatabaseClient.Replica.SPECIFIED);
        }
      }

      final List<FieldSchema> fields = tableSchema.getFields();

      final StringBuilder fieldsStr = new StringBuilder();
      final StringBuilder parmsStr = new StringBuilder();
      boolean first = true;
      for (FieldSchema field : fields) {
        if (field.getName().equals(SONICBASE_ID_STR)) {
          continue;
        }
        if (first) {
          first = false;
        }
        else {
          fieldsStr.append(",");
          parmsStr.append(",");
        }
        fieldsStr.append(field.getName());
        parmsStr.append("?");
      }

      String statementStr = SELECT_STR + fieldsStr.toString() + FROM_STR + tableName;
      String whereClause = cobj.getString(ComObject.Tag.WHERE_CLAUSE);
      if (whereClause != null) {
        statementStr = statementStr + " " + whereClause;
      }

      doInsertForNoPrimaryKey(conn, tableName, dbName, fields, fieldsStr, parmsStr, statementStr);

      logger.info("Finished importing table: tableName={}", tableName);
      importFinished.put(dbName + ":" + tableName, true);
    }
    catch (Exception e) {
      logger.error(ERROR_IMPORTING_RECORDS_STR, e);

      String exceptionStr = ExceptionUtils.getStackTrace(e);
      importException.put(dbName + ":" + tableName, exceptionStr);
    }
  }

  private void doInsertForNoPrimaryKey(Connection conn, String tableName, String dbName, List<FieldSchema> fields,
                                       StringBuilder fieldsStr, StringBuilder parmsStr, String statementStr)
      throws SQLException, ClassNotFoundException, InterruptedException, ExecutionException {
    List<Future> futures = new ArrayList<>();
    ThreadPoolExecutor executor = ThreadUtil.createExecutor(8, "SonicBase doImportForNoPrimaryKey Thread");
    try (PreparedStatement stmt = conn.prepareStatement(statementStr)) {
      Config config = server.getConfig();
      List<Config.Shard> array = config.getShards();
      Config.Shard shard = array.get(0);
      List<Config.Replica> replicasArray = shard.getReplicas();
      final String address = replicasArray.get(0).getString("address");
      final int port = replicasArray.get(0).getInt("port");

      Class.forName("com.sonicbase.jdbcdriver.Driver");
      try (Connection insertConn = getConnection(dbName, address, port)) {
        final AtomicInteger countSubmitted = new AtomicInteger();
        final AtomicInteger countFinished = new AtomicInteger();
        final AtomicLong countProcessed = importCountProcessed.get(dbName + ":" + tableName);
        try (ResultSet rs = stmt.executeQuery()) {
          int batchSize = 100;
          List<Object[]> currBatch = new ArrayList<>();
          while (rs.next() && !cancelBulkImport.get(dbName + ":" + tableName).get()) {
            if (shutdown) {
              break;
            }
            final Object[] currRecord = getCurrRecordFromResultSet(rs, fields);

            currBatch.add(currRecord);

            if (currBatch.size() >= batchSize && !cancelBulkImport.get(dbName + ":" + tableName).get()) {
              countSubmitted.incrementAndGet();
              final List<Object[]> batchToProcess = currBatch;
              currBatch = new ArrayList<>();
              futures.add(executor.submit(() -> insertRecords(insertConn, countProcessed, countFinished,
                  batchToProcess, tableName, fields, fieldsStr, parmsStr)));
            }
          }
          if (!currBatch.isEmpty() && !cancelBulkImport.get(dbName + ":" + tableName).get()) {
            countSubmitted.incrementAndGet();
            insertRecords(insertConn, countProcessed, countFinished, currBatch, tableName, fields, fieldsStr, parmsStr);
          }
          logger.info("bulkImport finished reading records");
        }
      }
    }
    finally {
      executor.shutdownNow();
    }
    for (Future future : futures) {
      future.get();
    }
  }

  protected Connection getConnection(String dbName, String address, int port) throws SQLException {
    return DriverManager.getConnection("jdbc:sonicbase:" + address + ":" + port + "/" + dbName);
  }

  protected Connection getConnection(String connectString) throws SQLException {
    return DriverManager.getConnection(connectString);
  }

  protected Connection getConnection(String connectString, String username, String password) throws SQLException {
    return DriverManager.getConnection(connectString, username, password);
  }

  @SuppressWarnings("squid:S2077") // don't need variable bindings
  private void doCoordinateBulkLoad(Connection conn, long count, int serverCount, int totalThreadCount,
                                    AtomicLong countProcessed, TableSchema tableSchema, IndexSchema indexSchema,
                                    String tableName, String dbName, ComObject cobj) throws SQLException, InterruptedException {
    String[] keyFields = indexSchema.getFields();
    DataType.Type[] dataTypes = new DataType.Type[keyFields.length];
    for (int i = 0; i < dataTypes.length; i++) {
      FieldSchema fSchema = tableSchema.getFields().get(tableSchema.getFieldOffset(keyFields[i]));
      dataTypes[i] = fSchema.getType();
    }

    StringBuilder keyFieldsStr = new StringBuilder();
    boolean first = true;
    for (String keyField : keyFields) {
      if (first) {
        first = false;
      }
      else {
        keyFieldsStr.append(",");
      }
      keyFieldsStr.append(keyField);
    }
    String str = SELECT_STR + keyFieldsStr.toString() + FROM_STR + tableName;
    String whereClause = cobj.getString(ComObject.Tag.WHERE_CLAUSE);
    if (whereClause != null) {
      str = str + " " + whereClause;
    }
    logger.info("bulkImport preProcess select statement: str={}", str);
    try (PreparedStatement stmt = conn.prepareStatement(str)) {

      int[] offsets = new int[totalThreadCount];
      for (int i = 0; i < totalThreadCount; i++) {
        offsets[i] = (int) (i == 0 ? 0 : count / totalThreadCount * i);
        logger.info("bulkImport preProcess key offset: slice={}, offset={}, total={}", i, offsets[i], count);
      }
      final Object[][] keys = new Object[totalThreadCount][];

      int recordOffset = 0;
      int slice = 0;
      try (ResultSet rs = stmt.executeQuery()) {
        long actualCount = 0;
        Object[] lastPartialKey = new Object[keyFields.length];
        boolean lookingForUniqueKey = false;
        while (rs.next() && !cancelBulkImport.get(dbName + ":" + tableName).get()) {
          ProcessResult processResult = new ProcessResult(countProcessed, indexSchema, keyFields, dataTypes,
              recordOffset == offsets[slice], keys, recordOffset, slice, rs, actualCount, lastPartialKey,
              lookingForUniqueKey).invoke();
          recordOffset = processResult.getRecordOffset();
          slice = processResult.getSlice();
          actualCount = processResult.getActualCount();
          lookingForUniqueKey = processResult.isLookingForUniqueKey();
          if (processResult.is()) {
            break;
          }
        }

        startBulkImportOnServer(serverCount, tableSchema, indexSchema, tableName, dbName, cobj, keys, actualCount);
      }
    }
  }

  private void startBulkImportOnServer(int serverCount, TableSchema tableSchema, IndexSchema indexSchema,
                                       String tableName, String dbName, ComObject cobj, Object[][] keys,
                                       long actualCount) throws InterruptedException {
    long countPer = actualCount / serverCount;

    cobj.put(ComObject.Tag.EXPECTED_COUNT, countPer);
    logger.info("bulkImport got keys");

    cobj.put(ComObject.Tag.METHOD, "BulkImportManager:startBulkImportOnServer");
    cobj.put(ComObject.Tag.SHOULD_PROCESS, true);

    List<ComObject> requests = new ArrayList<>();
    int keyOffset = 0;
    for (int i = 0; i < serverCount; i++) {
      cobj.remove(ComObject.Tag.LOWER_KEY);
      cobj.remove(ComObject.Tag.NEXT_KEY);

      if (keyOffset != 0) {
        cobj.put(ComObject.Tag.LOWER_KEY, DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), keys[keyOffset]));
      }
      int count = 0;
      for (int j = 0; j < BULK_IMPORT_THREAD_COUNT_PER_SERVER; j++) {
        if (keys[keyOffset] == null) {
          break;
        }
        count++;
      }
      ComArray keyArray = cobj.putArray(ComObject.Tag.KEYS, ComObject.Type.BYTE_ARRAY_TYPE, count);
      for (int j = 0; j < BULK_IMPORT_THREAD_COUNT_PER_SERVER; j++) {
        if (keys[keyOffset] == null) {
          break;
        }
        keyArray.add(DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), keys[keyOffset]));
        keyOffset++;
      }
      if (keyOffset < keys.length && keys[keyOffset] != null) {
        cobj.put(ComObject.Tag.NEXT_KEY, DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), keys[keyOffset]));
      }
      byte[] bytes = cobj.serialize();
      requests.add(new ComObject(bytes));
    }

    startBulkImportOnServerDoSend(serverCount, tableName, dbName, requests);
  }

  private void startBulkImportOnServerDoSend(int serverCount, String tableName, String dbName,
                                             List<ComObject> requests) throws InterruptedException {
    Set<String> assigned = new HashSet<>();
    int requestOffset = 0;
    while (requestOffset < serverCount) {
      for (int shard = 0; shard < server.getShardCount(); shard++) {
        for (int replica = 0; replica < server.getReplicationFactor(); replica++) {
          requestOffset = startBulkImportOnServerDoSendToReplica(tableName, dbName, requests, assigned, requestOffset,
              shard, replica);
        }
      }
      Thread.sleep(10_000);
    }
  }

  private int startBulkImportOnServerDoSendToReplica(String tableName, String dbName, List<ComObject> requests,
                                                     Set<String> assigned, int requestOffset, int shard, int replica) {
    if (!cancelBulkImport.get(dbName + ":" + tableName).get() && !assigned.contains(shard + ":" + replica)) {
      byte[] bytes = server.getClient().send(null, shard, replica,
          requests.get(requestOffset), DatabaseClient.Replica.SPECIFIED);
      ComObject retObj = new ComObject(bytes);
      if (retObj.getBoolean(ComObject.Tag.ACCEPTED)) {
        assigned.add(shard + ":" + replica);
        logger.info("Successfully startedBulkImportOnServer: db={}, table={}, shard={}, replica={}",
            dbName, tableName, shard, replica);
        requestOffset++;
      }
    }
    return requestOffset;
  }

  protected Object getValueOfField(ResultSet rs, String keyField, DataType.Type dataType) throws SQLException {
    try {
      return extractorForInternalByType.get(dataType).extract(rs, keyField);
    }
    catch (Exception e) {
      throw new SQLException(e);
    }
  }

  interface Setter {
    void set(PreparedStatement insertStatement, int parmOffset, Object value) throws SQLException;
  }

  static {
    setterByType.put(DataType.Type.BIT, (PreparedStatement insertStmt, int parmOffset, Object value) ->
        insertStmt.setBoolean(parmOffset, (Boolean) value));
    setterByType.put(DataType.Type.TINYINT, (PreparedStatement insertStmt, int parmOffset, Object value) ->
        insertStmt.setByte(parmOffset, (Byte) value));
    setterByType.put(DataType.Type.SMALLINT, (PreparedStatement insertStmt, int parmOffset, Object value) ->
        insertStmt.setShort(parmOffset, (Short) value));
    setterByType.put(DataType.Type.INTEGER, (PreparedStatement insertStmt, int parmOffset, Object value) ->
        insertStmt.setInt(parmOffset, (Integer) value));
    setterByType.put(DataType.Type.BIGINT, (PreparedStatement insertStmt, int parmOffset, Object value) ->
        insertStmt.setLong(parmOffset, (Long) value));
    setterByType.put(DataType.Type.FLOAT, (PreparedStatement insertStmt, int parmOffset, Object value) ->
        insertStmt.setDouble(parmOffset, (Double) value));
    setterByType.put(DataType.Type.REAL, (PreparedStatement insertStmt, int parmOffset, Object value) ->
        insertStmt.setFloat(parmOffset, (Float) value));
    setterByType.put(DataType.Type.DOUBLE, (PreparedStatement insertStmt, int parmOffset, Object value) ->
        insertStmt.setDouble(parmOffset, (Double) value));
    setterByType.put(DataType.Type.NUMERIC, (PreparedStatement insertStmt, int parmOffset, Object value) ->
        insertStmt.setBigDecimal(parmOffset, (BigDecimal) value));
    setterByType.put(DataType.Type.DECIMAL, (PreparedStatement insertStmt, int parmOffset, Object value) ->
        insertStmt.setBigDecimal(parmOffset, (BigDecimal) value));
    setterByType.put(DataType.Type.CHAR, (PreparedStatement insertStmt, int parmOffset, Object value) ->
        insertStmt.setString(parmOffset, (String) value));
    setterByType.put(DataType.Type.VARCHAR, (PreparedStatement insertStmt, int parmOffset, Object value) ->
        insertStmt.setString(parmOffset, (String) value));
    setterByType.put(DataType.Type.LONGVARCHAR, (PreparedStatement insertStmt, int parmOffset, Object value) ->
        insertStmt.setString(parmOffset, (String) value));
    setterByType.put(DataType.Type.DATE, (PreparedStatement insertStmt, int parmOffset, Object value) ->
        insertStmt.setDate(parmOffset, (java.sql.Date) value));
    setterByType.put(DataType.Type.TIME, (PreparedStatement insertStmt, int parmOffset, Object value) ->
        insertStmt.setTime(parmOffset, (java.sql.Time) value));
    setterByType.put(DataType.Type.TIMESTAMP, (PreparedStatement insertStmt, int parmOffset, Object value) ->
        insertStmt.setTimestamp(parmOffset, (Timestamp) value));
    setterByType.put(DataType.Type.BINARY, (PreparedStatement insertStmt, int parmOffset, Object value) ->
        insertStmt.setBytes(parmOffset, (byte[]) value));
    setterByType.put(DataType.Type.VARBINARY, (PreparedStatement insertStmt, int parmOffset, Object value) ->
        insertStmt.setBytes(parmOffset, (byte[]) value));
    setterByType.put(DataType.Type.LONGVARBINARY, (PreparedStatement insertStmt, int parmOffset, Object value) ->
        insertStmt.setBytes(parmOffset, (byte[]) value));
    setterByType.put(DataType.Type.BLOB, (PreparedStatement insertStmt, int parmOffset, Object value) ->
        insertStmt.setBytes(parmOffset, (byte[]) value));
    setterByType.put(DataType.Type.CLOB, (PreparedStatement insertStmt, int parmOffset, Object value) ->
        insertStmt.setString(parmOffset, (String) value));
    setterByType.put(DataType.Type.BOOLEAN, (PreparedStatement insertStmt, int parmOffset, Object value) ->
        insertStmt.setBoolean(parmOffset, (Boolean) value));
    setterByType.put(DataType.Type.ROWID, (PreparedStatement insertStmt, int parmOffset, Object value) ->
        insertStmt.setLong(parmOffset, (Long) value));
    setterByType.put(DataType.Type.NCHAR, (PreparedStatement insertStmt, int parmOffset, Object value) ->
        insertStmt.setString(parmOffset, (String) value));
    setterByType.put(DataType.Type.NVARCHAR, (PreparedStatement insertStmt, int parmOffset, Object value) ->
        insertStmt.setString(parmOffset, (String) value));
    setterByType.put(DataType.Type.LONGNVARCHAR, (PreparedStatement insertStmt, int parmOffset, Object value) ->
        insertStmt.setString(parmOffset, (String) value));
    setterByType.put(DataType.Type.NCLOB, (PreparedStatement insertStmt, int parmOffset, Object value) ->
        insertStmt.setString(parmOffset, (String) value));
  }

  public static void setFieldsInInsertStatement(PreparedStatement insertStmt, int parmOffset, Object[] currRecord,
                                                List<FieldSchema> fields) {
    try {
      int fieldOffset = 0;
      for (FieldSchema field : fields) {
        if (field.getName().equals(SONICBASE_ID_STR)) {
          fieldOffset++;
          continue;
        }

        if (currRecord[fieldOffset] != null) {
          setterByType.get(field.getType()).set(insertStmt, parmOffset, currRecord[fieldOffset]);
        }
        else {
          insertStmt.setNull(parmOffset, field.getType().getValue());
        }

        parmOffset++;
        fieldOffset++;
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  @SuppressWarnings("squid:S2077") //don't need variable bindings
  private void insertRecords(Connection insertConn, AtomicLong countProcessed, AtomicInteger countFinished,
                             List<Object[]> currBatch, String tableName,
                             List<FieldSchema> fields, StringBuilder fieldsStr, StringBuilder parmsStr) {
    PreparedStatement insertStmt = null;
    try {

      insertStmt = insertConn.prepareStatement("insert ignore into " + tableName +
          " (" + fieldsStr.toString() + ") VALUES (" + parmsStr.toString() + ")");

      for (Object[] currRecord : currBatch) {
        setFieldsInInsertStatement(insertStmt, 1, currRecord, fields);

        insertStmt.addBatch();
      }
      insertStmt.executeBatch();

      countProcessed.addAndGet(currBatch.size());
    }
    catch (Exception e) {
      logger.error("Error inserting records", e);
      throw new DatabaseException(e);
    }
    finally {
      try {
        if (insertStmt != null) {
          insertStmt.close();
        }
      }
      catch (Exception e) {
        logger.error(ERROR_CLOSING_CONNECTION_STR, e);
      }
      finally {
        countFinished.incrementAndGet();
      }
    }
  }

  protected ConcurrentHashMap<String, ConcurrentHashMap<String, BulkImportStatus>> getBulkImportStatus(String dbName) {
    ConcurrentHashMap<String, ConcurrentHashMap<String, BulkImportStatus>> bulkImportStatus = new ConcurrentHashMap<>();

    try {
      ComObject cobj = new ComObject(1);
      cobj.put(ComObject.Tag.METHOD, "BulkImportManager:getBulkImportProgressOnServer");
      for (final String tableName : server.getCommon().getTables(dbName).keySet()) {
        ConcurrentHashMap<String, BulkImportStatus> tableStatus = null;
        tableStatus = bulkImportStatus.get(dbName + ":" + tableName);
        if (tableStatus == null) {
          tableStatus = new ConcurrentHashMap<>();
          bulkImportStatus.put(dbName + ":" + tableName, tableStatus);
        }
      }

      for (int i = 0; i < server.getShardCount(); i++) {
        for (int j = 0; j < server.getReplicationFactor(); j++) {
          doGetBulkImportStatus(bulkImportStatus, cobj, i, j);
        }
      }
    }
    catch (Exception e) {
      logger.error("Error in import monitor thread", e);
    }
    return bulkImportStatus;
  }

  private void doGetBulkImportStatus(ConcurrentHashMap<String, ConcurrentHashMap<String, BulkImportStatus>> bulkImportStatus,
                                     ComObject cobj, int shard, int replica) {
    String dbName;
    byte[] bytes = server.getClient().send(null, shard, replica, cobj, DatabaseClient.Replica.SPECIFIED);
    ComObject retObj = new ComObject(bytes);

    ComArray array = retObj.getArray(ComObject.Tag.STATUSES);
    for (int k = 0; k < array.getArray().size(); k++) {
      ComObject tableStatus = (ComObject) array.getArray().get(k);
      dbName = tableStatus.getString(ComObject.Tag.DB_NAME);
      String tableName = tableStatus.getString(ComObject.Tag.TABLE_NAME);
      ConcurrentHashMap<String, BulkImportStatus> destStatus = bulkImportStatus.get(dbName + ":" + tableName);
      if (destStatus == null) {
        destStatus = new ConcurrentHashMap<>();
        bulkImportStatus.put(dbName + ":" + tableName, destStatus);
      }
      BulkImportStatus currStatus = destStatus.get(shard + ":" + replica);
      if (currStatus == null) {
        currStatus = new BulkImportStatus();
        destStatus.put(shard + ":" + replica, currStatus);
      }
      currStatus.countExpected = tableStatus.getLong(ComObject.Tag.EXPECTED_COUNT);
      currStatus.countProcessed = tableStatus.getLong(ComObject.Tag.COUNT_LONG);
      currStatus.finished = tableStatus.getBoolean(ComObject.Tag.FINISHED);
      currStatus.exception = tableStatus.getString(ComObject.Tag.EXCEPTION);

      currStatus.preProcessCountExpected = tableStatus.getLong(ComObject.Tag.PRE_PROCESS_EXPECTED_COUNT);
      currStatus.preProcessCountProcessed = tableStatus.getLong(ComObject.Tag.PRE_PROCESS_COUNT_PROCESSED);
      currStatus.preProcessFinished = tableStatus.getBoolean(ComObject.Tag.PRE_PROCESS_FINISHED);
      currStatus.preProcessException = tableStatus.getString(ComObject.Tag.PRE_PROCESS_EXCEPTION);
    }
  }

  @SuppressWarnings("squid:S1172") // replayedCommand is required
  public ComObject startBulkImport(final ComObject cobj, boolean replayedCommand) {
    try {
      final String dbName = cobj.getString(ComObject.Tag.DB_NAME);
      final String tableNames = cobj.getString(ComObject.Tag.TABLE_NAME);
      String[] tables = tableNames.split(",");
      for (int i = 0; i < tables.length; i++) {
        tables[i] = tables[i].trim();
      }
      for (String tableName : tables) {
        cancelBulkImport.put(dbName + ":" + tableName, new AtomicBoolean(false));
      }

      ConcurrentHashMap<String, ConcurrentHashMap<String, BulkImportStatus>> bulkImportStatus = getBulkImportStatus(dbName);

      boolean inProgress = processBulkImportStatus(dbName, tables, bulkImportStatus);

      if (inProgress) {
        throw new DatabaseException("Cannot start bulk import. Table import currently in progress");
      }

      for (final String tableName : tables) {
        final int serverCount = server.getShardCount() * server.getReplicationFactor();

        String user = cobj.getString(ComObject.Tag.USER);
        String password = cobj.getString(ComObject.Tag.PASSWORD);

        logger.info("startBulkImport: dbName={}, tableName={}, driver={}, connectStr={}, user={}, password={}",
            dbName, tableName, cobj.getString(ComObject.Tag.DRIVER_NAME),
            cobj.getString(ComObject.Tag.CONNECT_STRING), user, password);

        final byte[] cobjBytes = cobj.serialize();

        Thread thread = new Thread(() -> doStartBulkImportForTable(dbName, tableName, cobjBytes, serverCount));
        thread.start();
      }
      return null;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private boolean processBulkImportStatus(String dbName, String[] tables,
                                          ConcurrentHashMap<String, ConcurrentHashMap<String, BulkImportStatus>> bulkImportStatus) {
    boolean inProgress = false;

    outer:
    for (Map.Entry<String, ConcurrentHashMap<String, BulkImportStatus>> entry : bulkImportStatus.entrySet()) {
      if (entry.getKey().startsWith(dbName + ":")) {
        String tableName = entry.getKey();
        for (String currTable : tables) {
          DoProcessBulkImportStatusForTable doProcessBulkImportStatusForTable = new DoProcessBulkImportStatusForTable(
              inProgress, entry, tableName, currTable).invoke();
          inProgress = doProcessBulkImportStatusForTable.isInProgress();
          if (doProcessBulkImportStatusForTable.is()) {
            break outer;
          }
        }
      }
    }
    return inProgress;
  }

  private void doStartBulkImportForTable(String dbName, String tableName, byte[] cobjBytes, int serverCount) {
    boolean hasAccepted = false;
    try {
      while (!shutdown && !cancelBulkImport.get(dbName + ":" + tableName).get()) {
        for (int shard = 0; shard < server.getShardCount(); shard++) {
          for (int replica = 0; replica < server.getReplicationFactor(); replica++) {
            hasAccepted = doStartBulkImportForTableDoSend(dbName, tableName, cobjBytes, hasAccepted, shard, replica);
          }
        }
        if (hasAccepted) {
          break;
        }
        logger.info("Failed to start coordinateBulkImportForTable: db={}, table={}", dbName, tableName);
        coordinatesCalled.set((coordinatesCalled.getAndIncrement()) % serverCount);
        Thread.sleep(5_000);
      }
    }
    catch (Exception e) {
      logger.error("Error importing table", e);
      String exceptionStr = ExceptionUtils.getStackTrace(e);
      preProcessException.put(dbName + ":" + tableName, exceptionStr);
    }
  }

  private boolean doStartBulkImportForTableDoSend(String dbName, String tableName, byte[] cobjBytes, boolean hasAccepted,
                                                  int shard, int replica) {
    ComObject cobj = new ComObject(cobjBytes);
    cobj.put(ComObject.Tag.METHOD, "BulkImportManager:coordinateBulkImportForTable");
    cobj.put(ComObject.Tag.TABLE_NAME, tableName);
    if (hasAccepted) {
      logger.info("bulkImport setting shouldProcess=false");
      cobj.put(ComObject.Tag.SHOULD_PROCESS, false);
    }
    else {
      logger.info("bulkImport setting shouldProcess=true");
      cobj.put(ComObject.Tag.SHOULD_PROCESS, true);
    }
    byte[] bytes = server.getClient().send(null, shard, replica, cobj, DatabaseClient.Replica.SPECIFIED);
    ComObject retObj = new ComObject(bytes);
    if (retObj.getBoolean(ComObject.Tag.ACCEPTED)) {
      logger.info("bulkImport successfully started coordinateBulkImportForTable: db={}, table={}, shard={}, replica={}",
          dbName, tableName, shard, replica);
      hasAccepted = true;
    }
    else {
      logger.info("bulkImport server did not accept coordinateBulkImportForTable: db={}, table={}, shard={}, replica={}",
          dbName, tableName, shard, replica);
    }
    return hasAccepted;
  }

  @SuppressWarnings("squid:S1172") // replayedCommand is required
  public ComObject cancelBulkImport(final ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.DB_NAME);
    String tableName = cobj.getString(ComObject.Tag.TABLE_NAME);
    cancelBulkImport.put(dbName + ":" + tableName, new AtomicBoolean(true));
    return null;
  }

  @SuppressWarnings("squid:S1172") // replayedCommand is required
  public ComObject  getBulkImportProgress(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.DB_NAME);

    ConcurrentHashMap<String, ConcurrentHashMap<String, BulkImportStatus>> bulkImportStatus = getBulkImportStatus(dbName);

    ComObject retObj = new ComObject(1);
    ComArray array = retObj.putArray(ComObject.Tag.PROGRESS_ARRAY, ComObject.Type.OBJECT_TYPE, bulkImportStatus.size());
    for (Map.Entry<String, ConcurrentHashMap<String, BulkImportStatus>> entry : bulkImportStatus.entrySet()) {
      if (entry.getKey().startsWith(dbName + ":")) {
        String tableName = entry.getKey();

        doGetBulkImportStatus(array, entry, tableName);
      }
    }

    return retObj;
  }

  private void doGetBulkImportStatus(ComArray array, Map.Entry<String, ConcurrentHashMap<String,
      BulkImportStatus>> entry, String tableName) {
    long countExpected = 0;
    long countProcessed = 0;
    boolean finished = true;
    String exception = null;
    for (Map.Entry<String, BulkImportStatus> serverEntry : entry.getValue().entrySet()) {
      countProcessed += serverEntry.getValue().countProcessed;
      countExpected += serverEntry.getValue().countExpected;
      if (!serverEntry.getValue().finished) {
        finished = false;
      }
      if (serverEntry.getValue().exception != null) {
        exception = serverEntry.getValue().exception;
      }
    }

    long localPreProcessCountExpected = 0;
    long localPreProcessCountProcessed = 0;
    boolean localPreProcessFinished = true;
    String preProcessEx = null;
    for (Map.Entry<String, BulkImportStatus> serverEntry : entry.getValue().entrySet()) {
      localPreProcessCountProcessed += serverEntry.getValue().preProcessCountProcessed;
      localPreProcessCountExpected += serverEntry.getValue().preProcessCountExpected;
      if (!serverEntry.getValue().preProcessFinished) {
        localPreProcessFinished = false;
      }
      if (serverEntry.getValue().preProcessException != null) {
        preProcessEx = serverEntry.getValue().preProcessException;
      }
    }

    getBulkImportStatsSetReturn(array, tableName, countExpected, countProcessed, finished, exception,
        localPreProcessCountExpected, localPreProcessCountProcessed, localPreProcessFinished, preProcessEx);
  }

  private void getBulkImportStatsSetReturn(ComArray array, String tableName, long countExpected, long countProcessed,
                                           boolean finished, String exception, long preProcessCountExpected,
                                           long preProcessCountProcessed, boolean preProcessFinished, String preProcessEx) {
    ComObject serverObj = new ComObject(9);
    serverObj.put(ComObject.Tag.TABLE_NAME, tableName);
    serverObj.put(ComObject.Tag.EXPECTED_COUNT, countExpected);
    serverObj.put(ComObject.Tag.COUNT_LONG, countProcessed);
    serverObj.put(ComObject.Tag.FINISHED, finished);
    if (exception != null) {
      serverObj.put(ComObject.Tag.EXCEPTION, exception);
    }
    serverObj.put(ComObject.Tag.PRE_PROCESS_EXPECTED_COUNT, preProcessCountExpected);
    serverObj.put(ComObject.Tag.PRE_PROCESS_COUNT_PROCESSED, preProcessCountProcessed);
    serverObj.put(ComObject.Tag.PRE_PROCESS_FINISHED, preProcessFinished);
    if (preProcessEx != null) {
      serverObj.put(ComObject.Tag.PRE_PROCESS_EXCEPTION, preProcessEx);
    }
    array.add(serverObj);
  }

  private class ProcessResult {
    private boolean myResult;
    private final AtomicLong countProcessed;
    private final IndexSchema indexSchema;
    private final String[] keyFields;
    private final DataType.Type[] dataTypes;
    private final boolean b;
    private final Object[][] keys;
    private int recordOffset;
    private int slice;
    private final ResultSet rs;
    private long actualCount;
    private final Object[] lastPartialKey;
    private boolean lookingForUniqueKey;

    public ProcessResult(AtomicLong countProcessed, IndexSchema indexSchema, String[] keyFields,
                         DataType.Type[] dataTypes, boolean b, Object[][] keys, int recordOffset, int slice, ResultSet rs,
                         long actualCount, Object[] lastPartialKey, boolean lookingForUniqueKey) {
      this.countProcessed = countProcessed;
      this.indexSchema = indexSchema;
      this.keyFields = keyFields;
      this.dataTypes = dataTypes;
      this.b = b;
      this.keys = keys;
      this.recordOffset = recordOffset;
      this.slice = slice;
      this.rs = rs;
      this.actualCount = actualCount;
      this.lastPartialKey = lastPartialKey;
      this.lookingForUniqueKey = lookingForUniqueKey;
    }

    boolean is() {
      return myResult;
    }

    public int getRecordOffset() {
      return recordOffset;
    }

    public int getSlice() {
      return slice;
    }

    public long getActualCount() {
      return actualCount;
    }

    public boolean isLookingForUniqueKey() {
      return lookingForUniqueKey;
    }

    public ProcessResult invoke() throws SQLException {
      boolean foundUniqueKey = false;
      if (lookingForUniqueKey) {
        Object[] possibleKey = new Object[keyFields.length];
        possibleKey[0] = getValueOfField(rs, keyFields[0], dataTypes[0]);
        int compareValue = DatabaseCommon.compareKey(indexSchema.getComparators(), possibleKey, lastPartialKey);
        if (compareValue != 0) {
          foundUniqueKey = true;
        }
      }
      if (foundUniqueKey || b) {
        lookingForUniqueKey = false;
        Object[] checkingKey = new Object[keyFields.length];
        checkingKey[0] = getValueOfField(rs, keyFields[0], dataTypes[0]);
        if (lastPartialKey[0] != null && 0 == DatabaseCommon.compareKey(indexSchema.getComparators(), checkingKey, lastPartialKey)) {
          lookingForUniqueKey = true;
        }
        if (!lookingForUniqueKey) {
          keys[slice] = new Object[keyFields.length];
          for (int i = 0; i < keyFields.length; i++) {
            keys[slice][i] = getValueOfField(rs, keyFields[i], dataTypes[i]);
          }
          lastPartialKey[0] = keys[slice][0];
          slice++;
        }
      }
      if (slice == keys.length) {
        myResult = true;
        return this;
      }
      countProcessed.incrementAndGet();
      recordOffset++;
      actualCount++;
      myResult = false;
      return this;
    }
  }

  private class DoProcessBulkImportStatusForTable {
    private boolean myResult;
    private boolean inProgress;
    private final Map.Entry<String, ConcurrentHashMap<String, BulkImportStatus>> entry;
    private final String tableName;
    private final String currTable;

    public DoProcessBulkImportStatusForTable(boolean inProgress, Map.Entry<String,
        ConcurrentHashMap<String, BulkImportStatus>> entry, String tableName, String currTable) {
      this.inProgress = inProgress;
      this.entry = entry;
      this.tableName = tableName;
      this.currTable = currTable;
    }

    boolean is() {
      return myResult;
    }

    public boolean isInProgress() {
      return inProgress;
    }

    public DoProcessBulkImportStatusForTable invoke() {
      if (currTable.equals(tableName)) {
        boolean finished = false;
        String exception = null;
        for (Map.Entry<String, BulkImportStatus> serverEntry : entry.getValue().entrySet()) {
          if (serverEntry.getValue().finished) {
            finished = true;
          }
          if (serverEntry.getValue().exception != null) {
            exception = serverEntry.getValue().exception;
          }
        }
        if (doProcessBulkImportStatusForTable(finished, exception)) {
          return this;
        }
      }
      myResult = false;
      return this;
    }

    private boolean doProcessBulkImportStatusForTable(boolean finished, String exception) {
      boolean localPreProcessFinished = false;
      String preProcessEx = null;
      for (Map.Entry<String, BulkImportStatus> serverEntry : entry.getValue().entrySet()) {

        if (serverEntry.getValue().preProcessFinished) {
          localPreProcessFinished = true;
        }
        if (serverEntry.getValue().preProcessException != null) {
          preProcessEx = serverEntry.getValue().preProcessException;
        }
      }
      if (!finished && exception == null) {
        inProgress = true;
        myResult = true;
        return true;
      }
      if (!localPreProcessFinished && preProcessEx == null) {
        inProgress = true;
        myResult = true;
        return true;
      }
      return false;
    }
  }
}
