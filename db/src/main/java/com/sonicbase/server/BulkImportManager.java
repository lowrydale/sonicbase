package com.sonicbase.server;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.ComArray;
import com.sonicbase.common.ComObject;
import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.common.Logger;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.DataType;
import com.sonicbase.schema.FieldSchema;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import org.apache.commons.lang.exception.ExceptionUtils;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class BulkImportManager {

  private int BULK_IMPORT_THREAD_COUNT_PER_SERVER = 4;

  private final Logger logger;
  private final DatabaseServer server;

  public BulkImportManager(final DatabaseServer server) {
    this.server = server;
    logger = new Logger(server.getDatabaseClient(), server.getShard(), server.getReplica());

//    if (server.getShard() == 0) {
//      Thread thread = new Thread(new Runnable() {
//        @Override
//        public void run() {
//          while (true) {
//            try {
//              ComObject cobj = new ComObject();
//              String command = "DatabaseServer:ComObject:getBulkImportProgressOnServer:";
//              cobj.put(ComObject.Tag.method, "getBulkImportProgressOnServer");
//              for (int i = 0; i < server.getShardCount(); i++) {
//                for (int j = 0; j < server.getReplicationFactor(); j++) {
//                  byte[] bytes = server.getClient().send(null, i, j, command, cobj, DatabaseClient.Replica.specified);
//                  ComObject retObj = new ComObject(bytes);
//
//                  ComArray array = retObj.getArray(ComObject.Tag.statuses);
//                  for (int k = 0; k < array.getArray().size(); k++) {
//                    ComObject tableStatus = (ComObject) array.getArray().get(k);
//                    String dbName = tableStatus.getString(ComObject.Tag.dbName);
//                    String tableName = tableStatus.getString(ComObject.Tag.tableName);
//                    ConcurrentHashMap<String, BulkImportStatus> destStatus = bulkImportStatus.get(dbName + ":" + tableName);
//                    if (destStatus == null) {
//                      destStatus = new ConcurrentHashMap<>();
//                      bulkImportStatus.put(dbName + ":" + tableName, destStatus);
//                    }
//                    BulkImportStatus currStatus = destStatus.get(i + ":" + j);
//                    if (currStatus == null) {
//                      currStatus = new BulkImportStatus();
//                      destStatus.put(i + ":" + j, currStatus);
//                    }
//                    currStatus.countExpected = tableStatus.getLong(ComObject.Tag.expectedCount);
//                    currStatus.countProcessed = tableStatus.getLong(ComObject.Tag.countLong);
//                    currStatus.finished = tableStatus.getBoolean(ComObject.Tag.finished);
//                    currStatus.exception = tableStatus.getString(ComObject.Tag.exception);
//
//                    currStatus.preProcessCountExpected = tableStatus.getLong(ComObject.Tag.preProcessExpectedCount);
//                    currStatus.preProcessCountProcessed = tableStatus.getLong(ComObject.Tag.prePocessCountProcessed);
//                    currStatus.preProcessFinished = tableStatus.getBoolean(ComObject.Tag.preProcessFinished);
//                    currStatus.preProcessException = tableStatus.getString(ComObject.Tag.preProcessException);
//                  }
//                }
//              }
//              Thread.sleep(5_000);
//            }
//            catch (Exception e) {
//              logger.error("Error in import monitor thread", e);
//              try {
//                Thread.sleep(10_000);
//              }
//              catch (InterruptedException e1) {
//                break;
//              }
//            }
//          }
//        }
//      });
//      thread.start();
//    }
  }

  private static class BulkImportStatus {
    private long preProcessCountExpected;
    private long preProcessCountProcessed;
    private boolean preProcessFinished;
    public String preProcessException;
    private long countExpected;
    private long countProcessed;
    private boolean finished;
    public String exception;
  }

  private ConcurrentHashMap<String, Long> preProcessCountExpected = new ConcurrentHashMap<>();
  private ConcurrentHashMap<String, AtomicLong> preProcessCountProcessed = new ConcurrentHashMap<>();
  private ConcurrentHashMap<String, Boolean> preProcessFinished = new ConcurrentHashMap<>();
  private ConcurrentHashMap<String, String> preProcessException = new ConcurrentHashMap<>();
  private ConcurrentHashMap<String, Long> importCountExpected = new ConcurrentHashMap<>();
  private ConcurrentHashMap<String, AtomicLong> importCountProcessed = new ConcurrentHashMap<>();
  private ConcurrentHashMap<String, Boolean> importFinished = new ConcurrentHashMap<>();
  private ConcurrentHashMap<String, String> importException = new ConcurrentHashMap<>();
  private ConcurrentHashMap<String, AtomicBoolean> cancelBulkImport = new ConcurrentHashMap<>();


  public ComObject getBulkImportProgressOnServer(ComObject cobj) {
    ComObject retObj = new ComObject();

    ComArray array = retObj.putArray(ComObject.Tag.statuses, ComObject.Type.objectType);
    for (String dbName : server.getDbNames(server.getDataDir())) {
      for (String tableName : server.getCommon().getTables(dbName).keySet()) {
        ComObject currObj = new ComObject();
        currObj.put(ComObject.Tag.dbName, dbName);
        currObj.put(ComObject.Tag.tableName, tableName);

        AtomicLong preProcessed = preProcessCountProcessed.get(dbName + ":" + tableName);
        if (preProcessed == null) {
          currObj.put(ComObject.Tag.prePocessCountProcessed, 0);
        }
        else {
          currObj.put(ComObject.Tag.prePocessCountProcessed, preProcessed.get());
        }
        Long preProcessExpected = preProcessCountExpected.get(dbName + ":" + tableName);
        if (preProcessExpected == null) {
          preProcessExpected = 0L;
        }
        currObj.put(ComObject.Tag.preProcessExpectedCount, preProcessExpected);
        Boolean preFinished = preProcessFinished.get(dbName + ":" + tableName);
        if (preFinished == null) {
          currObj.put(ComObject.Tag.preProcessFinished, false);
        }
        else {
          currObj.put(ComObject.Tag.preProcessFinished, preFinished);
        }
        String preException = preProcessException.get(dbName + ":" + tableName);
        if (preException != null) {
          currObj.put(ComObject.Tag.preProcessException, preException);
        }


        AtomicLong processed = importCountProcessed.get(dbName + ":" + tableName);
        if (processed == null) {
          currObj.put(ComObject.Tag.countLong, 0);
        }
        else {
          currObj.put(ComObject.Tag.countLong, processed.get());
        }
        Long expected = importCountExpected.get(dbName + ":" + tableName);
        if (expected == null) {
          expected = 0L;
        }
        currObj.put(ComObject.Tag.expectedCount, expected);

        Boolean finished = importFinished.get(dbName + ":" + tableName);
        if (finished == null) {
          currObj.put(ComObject.Tag.finished, false);
        }
        else {
          currObj.put(ComObject.Tag.finished, finished);
        }
        String exception = importException.get(dbName + ":" + tableName);
        if (exception != null) {
          currObj.put(ComObject.Tag.exception, exception);
        }
        array.add(currObj);
      }
    }
    return retObj;
  }

  private AtomicInteger countBulkImportRunning = new AtomicInteger();

  private static ConcurrentHashMap<Long, Long> returned = new ConcurrentHashMap<>();

  public ComObject startBulkImportOnServer(final ComObject cobj) {
    final String dbName = cobj.getString(ComObject.Tag.dbName);
    final String tableName = cobj.getString(ComObject.Tag.tableName);
    importCountProcessed.put(dbName + ":" + tableName, new AtomicLong(0));
    importFinished.put(dbName + ":" + tableName, false);
    importException.remove(dbName + ":" + tableName);
    importCountExpected.put(dbName + ":" + tableName, cobj.getLong(ComObject.Tag.expectedCount));
    cancelBulkImport.put(dbName + ":" + tableName, new AtomicBoolean(false));

    if (!cobj.getBoolean(ComObject.Tag.shouldProcess)) {
      importFinished.put(dbName + ":" + tableName, true);
      return null;
    }

    ComObject retObj = new ComObject();
    if (countBulkImportRunning.get() >= 1) {
      retObj.put(ComObject.Tag.accepted, false);
    }
    else {
      logger.info("startBulkImportOnServer - begin: db=" + dbName + ", table=" + tableName);
      retObj.put(ComObject.Tag.accepted, true);

      Thread thread = new Thread(new Runnable() {
        @Override
        public void run() {
          final ThreadPoolExecutor executor = new ThreadPoolExecutor(8, 8, 10_000,
              TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
          countBulkImportRunning.incrementAndGet();
          try {
            final ComArray keys = cobj.getArray(ComObject.Tag.keys);
            final TableSchema tableSchema = server.getCommon().getTables(dbName).get(tableName);
            final List<FieldSchema> fields = tableSchema.getFields();
            final Thread[] threads = new Thread[keys.getArray().size()];

            final AtomicLong countRead = new AtomicLong();
            final AtomicLong countProcessed = importCountProcessed.get(dbName + ":" + tableName);
            final AtomicInteger countSubmitted = new AtomicInteger();
            final AtomicInteger countFinished = new AtomicInteger();
            ObjectNode dict = server.getConfig();
            ObjectNode databaseDict = dict;
            ArrayNode array = databaseDict.withArray("shards");
            ObjectNode replicaDict = (ObjectNode) array.get(0);
            ArrayNode replicasArray = replicaDict.withArray("replicas");
            final String address = databaseDict.get("clientIsPrivate").asBoolean() ?
                replicasArray.get(0).get("privateAddress").asText() :
                replicasArray.get(0).get("publicAddress").asText();
            final int port = replicasArray.get(0).get("port").asInt();

            Class.forName("com.sonicbase.jdbcdriver.Driver");
            final Connection insertConn = DriverManager.getConnection("jdbc:sonicbase:" + address + ":" + port + "/" + dbName);

            final StringBuilder fieldsStr = new StringBuilder();
            final StringBuilder parmsStr = new StringBuilder();
            boolean first = true;
            for (FieldSchema field : fields) {
              if (field.getName().equals("_id")) {
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
              final int currSlice = i;
              threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                  final List<Future> futures = new ArrayList<>();
                  Connection localConn = null;
                  try {
                    Object[] key = DatabaseCommon.deserializeKey(tableSchema, (byte[]) keys.getArray().get(currSlice));
                    logger.info("bulkImport starting slave thread: currSlice=" + currSlice + ", key=" + DatabaseCommon.keyToString(key));
                    String user = cobj.getString(ComObject.Tag.user);
                    String password = cobj.getString(ComObject.Tag.password);

                    if (user != null) {
                      localConn = DriverManager.getConnection(cobj.getString(ComObject.Tag.connectString), user, password);
                    }
                    else {
                      localConn = DriverManager.getConnection(cobj.getString(ComObject.Tag.connectString));
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

                    PreparedStatement stmt = null;
                    String statementStr = null;
                    boolean haveExpression = true;
                    if (currSlice == 0) {
                      if (keys.getArray().size() == 1) {
                        statementStr = "select " + fieldsStr.toString() + " from " + tableName;
                        haveExpression = false;
                      }
                      else {
                        Object[] currKey = DatabaseCommon.deserializeKey(tableSchema, (byte[]) keys.getArray().get(1));
                        Object[] lowerKey = cobj.getByteArray(ComObject.Tag.lowerKey) == null ? null : DatabaseCommon.deserializeKey(tableSchema, cobj.getByteArray(ComObject.Tag.lowerKey));
                        if (lowerKey != null) {
                          statementStr = "select " + fieldsStr.toString() + " from " + tableName + " where (" + keyFields[0] +
                              " >= " + lowerKey[0] + " and " + keyFields[0] + " < " + currKey[0] + ")";
                        }
                        else {
                          statementStr = "select " + fieldsStr.toString() + " from " + tableName + " where (" + keyFields[0] + " < " + currKey[0] + ")";
                        }
                        //todo: expand key
                      }
                    }
                    else if (currSlice == keys.getArray().size() - 1) {
                      Object[] currKey = DatabaseCommon.deserializeKey(tableSchema, (byte[]) keys.getArray().get(currSlice));
                      Object[] upperKey = cobj.getByteArray(ComObject.Tag.nextKey) == null ? null : DatabaseCommon.deserializeKey(tableSchema, cobj.getByteArray(ComObject.Tag.nextKey));
                      if (upperKey != null) {
                        statementStr = "select " + fieldsStr.toString() + " from " + tableName + " where (" + keyFields[0] +
                            " >= " + currKey[0] + " and " + keyFields[0] + " < " + upperKey[0] + ")";
                      }
                      else {
                        statementStr = "select " + fieldsStr.toString() + " from " + tableName + " where (" +
                            keyFields[0] + " >= " + currKey[0] + ")";
                      }
                    }
                    else {
                      Object[] lowerKey = DatabaseCommon.deserializeKey(tableSchema, (byte[]) keys.getArray().get(currSlice));
                      Object[] upperKey = DatabaseCommon.deserializeKey(tableSchema, (byte[]) keys.getArray().get(currSlice + 1));
                      statementStr = "select " + fieldsStr.toString() + " from " + tableName + " where (" + keyFields[0] +
                          " >= " + lowerKey[0] + " and " + keyFields[0] + " < " + upperKey[0] + ")";
                    }

                    String whereClause = cobj.getString(ComObject.Tag.whereClause);
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

                    stmt = localConn.prepareStatement(statementStr);
                    logger.info("bulkImport select statement: slice=" + currSlice + ", str=" + statementStr);
                    logger.info("bulkImport insert statement: slice=" + currSlice + ", str=insert into " + tableName +
                        " (" + fieldsStr.toString() + ") VALUES (" + parmsStr.toString() + ")");

                    try {
                      ResultSet rs = null;
                      try {
                        rs = stmt.executeQuery();
                      }
                      catch (Exception e) {
                        throw new DatabaseException("Error executing query: sql=" + statementStr, e);
                      }

                      int batchSize = 100;
                      List<Object[]> currBatch = new ArrayList<>();
                      while (rs.next() && !cancelBulkImport.get(dbName + ":" + tableName).get()) {

                        final Object[] currRecord = getCurrRecordFromResultSet(rs, fields);

                        if (returned.put((Long)currRecord[0], 0L) != null) {
                          System.out.println("dup: " + currRecord[0]);
                        }
                        currBatch.add(currRecord);
                        countRead.incrementAndGet();

                        if (currBatch.size() >= batchSize && !cancelBulkImport.get(dbName + ":" + tableName).get()) {
                          countSubmitted.incrementAndGet();
                          final List<Object[]> batchToProcess = currBatch;
                          currBatch = new ArrayList<>();
                          futures.add(executor.submit(new Runnable() {
                            @Override
                            public void run() {
                              insertRecords(insertConn, countProcessed, countFinished, batchToProcess, tableName,
                                  fields, fieldsStr, parmsStr);
                            }
                          }));
                        }
                      }
                      if (currBatch.size() > 0 && !cancelBulkImport.get(dbName + ":" + tableName).get()) {
                        countSubmitted.incrementAndGet();
                        insertRecords(insertConn, countProcessed, countFinished, currBatch, tableName, fields, fieldsStr, parmsStr);
                      }
                      logger.info("bulkImport finished reading records: currSlice=" + currSlice);
                    }
                    finally {
                      if (stmt != null) {
                        stmt.close();
                      }
                    }
                    for (Future future : futures) {
                      future.get();
                    }
                  }
                  catch (Exception e) {
                    logger.error("Error importing records", e);

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
                      logger.error("Error closing connection", e);
                    }
                  }
                }
              });

              threads[i].start();
            }
            for (int i = 0; i < threads.length; i++) {
              threads[i].join();
            }
            importFinished.put(dbName + ":" + tableName, true);
          }
          catch (Exception e) {
            logger.error("Error importing records", e);

            String exceptionStr = ExceptionUtils.getStackTrace(e);
            importException.put(dbName + ":" + tableName, exceptionStr);
          }
          finally {
            countBulkImportRunning.decrementAndGet();
            executor.shutdownNow();
          }
        }
      });
      thread.start();
    }

    return retObj;
  }

  private Object[] getCurrRecordFromResultSet(ResultSet rs, List<FieldSchema> fields) throws SQLException {
    final Object[] currRecord = new Object[fields.size()];

    int offset = 0;
    for (FieldSchema field : fields) {
      if (field.getName().equals("_id")) {
        continue;
      }
      switch (field.getType()) {
        case BIT: {
          boolean value = rs.getBoolean(field.getName());
          if (!rs.wasNull()) {
            currRecord[offset] = value;
          }
        }
        break;
        case TINYINT: {
          byte value = rs.getByte(field.getName());
          if (!rs.wasNull()) {
            currRecord[offset] = value;
          }
        }
        break;
        case SMALLINT: {
          short value = rs.getShort(field.getName());
          if (!rs.wasNull()) {
            currRecord[offset] = value;
          }
        }
        break;
        case INTEGER: {
          int value = rs.getInt(field.getName());
          if (!rs.wasNull()) {
            currRecord[offset] = value;
          }
        }
        break;
        case BIGINT: {
          long value = rs.getLong(field.getName());
          if (!rs.wasNull()) {
            currRecord[offset] = value;
          }
        }
        break;
        case FLOAT: {
          double value = rs.getDouble(field.getName());
          if (!rs.wasNull()) {
            currRecord[offset] = value;
          }
        }
        break;
        case REAL: {
          float value = rs.getFloat(field.getName());
          if (!rs.wasNull()) {
            currRecord[offset] = value;
          }
        }
        break;
        case DOUBLE: {
          double value = rs.getDouble(field.getName());
          if (!rs.wasNull()) {
            currRecord[offset] = value;
          }
        }
        break;
        case NUMERIC: {
          BigDecimal value = rs.getBigDecimal(field.getName());
          if (!rs.wasNull()) {
            currRecord[offset] = value;
          }
        }
        break;
        case DECIMAL: {
          BigDecimal value = rs.getBigDecimal(field.getName());
          if (!rs.wasNull()) {
            currRecord[offset] = value;
          }
        }
        break;
        case CHAR: {
          String value = rs.getString(field.getName());
          if (!rs.wasNull()) {
            currRecord[offset] = value;
          }
        }
        break;
        case VARCHAR: {
          String value = rs.getString(field.getName());
          if (!rs.wasNull()) {
            currRecord[offset] = value;
          }
        }
        break;
        case LONGVARCHAR: {
          String value = rs.getString(field.getName());
          if (!rs.wasNull()) {
            currRecord[offset] = value;
          }
        }
        break;
        case DATE: {
          Date value = rs.getDate(field.getName());
          if (!rs.wasNull()) {
            currRecord[offset] = value;
          }
        }
        break;
        case TIME: {
          Time value = rs.getTime(field.getName());
          if (!rs.wasNull()) {
            currRecord[offset] = value;
          }
        }
        break;
        case TIMESTAMP: {
          Timestamp value = rs.getTimestamp(field.getName());
          if (!rs.wasNull()) {
            currRecord[offset] = value;
          }
        }
        break;
        case BINARY: {
          byte[] value = rs.getBytes(field.getName());
          if (!rs.wasNull()) {
            currRecord[offset] = value;
          }
        }
        break;
        case VARBINARY: {
          byte[] value = rs.getBytes(field.getName());
          if (!rs.wasNull()) {
            currRecord[offset] = value;
          }
        }
        break;
        case LONGVARBINARY: {
          byte[] value = rs.getBytes(field.getName());
          if (!rs.wasNull()) {
            currRecord[offset] = value;
          }
        }
        break;
        case BLOB: {
          byte[] value = rs.getBytes(field.getName());
          if (!rs.wasNull()) {
            currRecord[offset] = value;
          }
        }
        break;
        case CLOB: {
          String value = rs.getString(field.getName());
          if (!rs.wasNull()) {
            currRecord[offset] = value;
          }
        }
        break;
        case BOOLEAN: {
          boolean value = rs.getBoolean(field.getName());
          if (!rs.wasNull()) {
            currRecord[offset] = value;
          }
        }
        break;
        case ROWID: {
          long value = rs.getLong(field.getName());
          if (!rs.wasNull()) {
            currRecord[offset] = value;
          }
        }
        break;
        case NCHAR: {
          String value = rs.getString(field.getName());
          if (!rs.wasNull()) {
            currRecord[offset] = value;
          }
        }
        break;
        case NVARCHAR: {
          String value = rs.getString(field.getName());
          if (!rs.wasNull()) {
            currRecord[offset] = value;
          }
        }
        break;
        case LONGNVARCHAR: {
          String value = rs.getString(field.getName());
          if (!rs.wasNull()) {
            currRecord[offset] = value;
          }
        }
        break;
        case NCLOB: {
          String value = rs.getString(field.getName());
          if (!rs.wasNull()) {
            currRecord[offset] = value;
          }
        }
        break;
      }

      offset++;
    }
    return currRecord;
  }

  private AtomicInteger countCoordinating = new AtomicInteger();

  public ComObject coordinateBulkImportForTable(final ComObject cobj) {
    final String dbName = cobj.getString(ComObject.Tag.dbName);
    final String tableName = cobj.getString(ComObject.Tag.tableName);

    importCountProcessed.put(dbName + ":" + tableName, new AtomicLong(0));
    importFinished.put(dbName + ":" + tableName, false);
    importException.remove(dbName + ":" + tableName);
    cancelBulkImport.put(dbName + ":" + tableName, new AtomicBoolean(false));

    preProcessCountProcessed.put(dbName + ":" + tableName, new AtomicLong(0));
    preProcessCountExpected.put(dbName + ":" + tableName, 0L);
    preProcessFinished.put(dbName + ":" + tableName, false);
    preProcessException.remove(dbName + ":" + tableName);

    ComObject retObj = new ComObject();
    if (!cobj.getBoolean(ComObject.Tag.shouldProcess)) {
      logger.info("coordinateBulkImportForTable - begin. Not processing: db=" + dbName + ", table=" + tableName);
      preProcessFinished.put(dbName + ":" + tableName, true);
      retObj.put(ComObject.Tag.accepted, false);
      return retObj;
    }

    if (countCoordinating.get() >= 5) {
      logger.info("coordinateBulkImportForTable - begin. Not accepting: db=" + dbName + ", table=" + tableName);
      retObj.put(ComObject.Tag.accepted, false);
      preProcessFinished.put(dbName + ":" + tableName, true);
    }
    else {
      logger.info("coordinateBulkImportForTable - begin. Accepted: db=" + dbName + ", table=" + tableName);

      retObj.put(ComObject.Tag.accepted, true);

      Thread thread = new Thread(new Runnable() {
        @Override
        public void run() {
          countCoordinating.incrementAndGet();
          Connection insertConn = null;
          try {

            Class.forName(cobj.getString(ComObject.Tag.driverName));

            final String user = cobj.getString(ComObject.Tag.user);
            final String password = cobj.getString(ComObject.Tag.password);

            Connection conn = null;
            long count = 0;
            try {
              if (user != null) {
                conn = DriverManager.getConnection(cobj.getString(ComObject.Tag.connectString), user, password);
              }
              else {
                conn = DriverManager.getConnection(cobj.getString(ComObject.Tag.connectString));
              }

              PreparedStatement stmt = conn.prepareStatement("select count(*) from " + tableName);
              try {
                ResultSet rs = stmt.executeQuery();
                rs.next();
                count = rs.getLong(1);
              }
              finally {
                if (stmt != null) {
                  stmt.close();
                }
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
            logger.error("Error importing records", e);

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
                logger.error("Error closing connection", e);
              }
            }
          }
        }
      });
      thread.start();
    }

    return retObj;
  }

  private void doImportForNoPrimaryKey(Connection conn, long count, int serverCount, int totalThreadCount,
                                       TableSchema tableSchema, IndexSchema indexSchema,
                                       final String tableName, final String dbName, ComObject cobj) throws SQLException {

    importCountProcessed.put(dbName + ":" + tableName, new AtomicLong(0));
    importFinished.put(dbName + ":" + tableName, false);
    importException.remove(dbName + ":" + tableName);
    importCountExpected.put(dbName + ":" + tableName, cobj.getLong(ComObject.Tag.expectedCount));
    cancelBulkImport.put(dbName + ":" + tableName, new AtomicBoolean(false));
    try {
      cobj.put(ComObject.Tag.method, "startBulkImportOnServer");
      cobj.put(ComObject.Tag.tableName, tableName);
      for (int shard = 0; shard < server.getShardCount(); shard++) {
        for (int replica = 0; replica < server.getReplicationFactor(); replica++) {
          if (shard == server.getShard() && replica == server.getReplica()) {
            continue;
          }
          cobj.put(ComObject.Tag.shouldProcess, false);
          byte[] bytes = server.getClient().send(null, shard, replica, cobj, DatabaseClient.Replica.specified);
        }
      }

      final List<FieldSchema> fields = tableSchema.getFields();

      final StringBuilder fieldsStr = new StringBuilder();
      final StringBuilder parmsStr = new StringBuilder();
      boolean first = true;
      for (FieldSchema field : fields) {
        if (field.getName().equals("_id")) {
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

      String statementStr = "select " + fieldsStr.toString() + " from " + tableName;
      String whereClause = cobj.getString(ComObject.Tag.whereClause);
      if (whereClause != null) {
        statementStr = statementStr + " " + whereClause;
      }

      PreparedStatement stmt = conn.prepareStatement(statementStr);

      List<Future> futures = new ArrayList<>();
      ThreadPoolExecutor executor = new ThreadPoolExecutor(8, 8, 10_000, TimeUnit.MILLISECONDS,
          new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
      try {
        ObjectNode dict = server.getConfig();
        ObjectNode databaseDict = dict;
        ArrayNode array = databaseDict.withArray("shards");
        ObjectNode replicaDict = (ObjectNode) array.get(0);
        ArrayNode replicasArray = replicaDict.withArray("replicas");
        final String address = databaseDict.get("clientIsPrivate").asBoolean() ?
            replicasArray.get(0).get("privateAddress").asText() :
            replicasArray.get(0).get("publicAddress").asText();
        final int port = replicasArray.get(0).get("port").asInt();

        //todo: make failsafe
        Class.forName("com.sonicbase.jdbcdriver.Driver");
        final Connection insertConn = DriverManager.getConnection("jdbc:sonicbase:" + address + ":" + port + "/" + dbName);

        final AtomicInteger countSubmitted = new AtomicInteger();
        final AtomicInteger countFinished = new AtomicInteger();
        final AtomicLong countProcessed = importCountProcessed.get(dbName + ":" + tableName);
        ResultSet rs = stmt.executeQuery();
        int batchSize = 100;
        List<Object[]> currBatch = new ArrayList<>();
        while (rs.next() && !cancelBulkImport.get(dbName + ":" + tableName).get()) {

          final Object[] currRecord = getCurrRecordFromResultSet(rs, fields);

          currBatch.add(currRecord);

          if (currBatch.size() >= batchSize && !cancelBulkImport.get(dbName + ":" + tableName).get()) {
            countSubmitted.incrementAndGet();
            final List<Object[]> batchToProcess = currBatch;
            currBatch = new ArrayList<>();
            futures.add(executor.submit(new Runnable() {
              @Override
              public void run() {
                insertRecords(insertConn, countProcessed, countFinished, batchToProcess, tableName, fields, fieldsStr, parmsStr);
              }
            }));
          }
        }
        if (currBatch.size() > 0 && !cancelBulkImport.get(dbName + ":" + tableName).get()) {
          countSubmitted.incrementAndGet();
          insertRecords(insertConn, countProcessed, countFinished, currBatch, tableName, fields, fieldsStr, parmsStr);
        }
        logger.info("bulkImport finished reading records");
      }
      finally {
        if (stmt != null) {
          stmt.close();
        }
        executor.shutdownNow();
      }
      for (Future future : futures) {
        future.get();
      }
      logger.info("Finished importing table: tableName=" + tableName);
      importFinished.put(dbName + ":" + tableName, true);
    }
    catch (Exception e) {
      logger.error("Error importing records", e);

      String exceptionStr = ExceptionUtils.getStackTrace(e);
      importException.put(dbName + ":" + tableName, exceptionStr);
    }
  }

  private void doCoordinateBulkLoad(Connection conn, long count, int serverCount, int totalThreadCount,
                                    AtomicLong countProcessed, TableSchema tableSchema, IndexSchema indexSchema,
                                    String tableName, String dbName, ComObject cobj) throws SQLException, UnsupportedEncodingException, InterruptedException {
    PreparedStatement stmt;
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
    stmt = null;
    String str = "select " + keyFieldsStr.toString() + " from " + tableName;
    String whereClause = cobj.getString(ComObject.Tag.whereClause);
    if (whereClause != null) {
      str = str + " " + whereClause;
    }
    logger.info("bulkImport preProcess select statement: str=" + str);
    stmt = conn.prepareStatement(str);

    int[] offsets = new int[totalThreadCount];
    for (int i = 0; i < totalThreadCount; i++) {
      offsets[i] = (int) (i == 0 ? 0 : count / totalThreadCount * i);
      logger.info("bulkImport preProcess key offset: slice=" + i + ", offset=" + offsets[i] + ", total=" + count);
    }
    final Object[][] keys = new Object[totalThreadCount][];

    int recordOffset = 0;
    int slice = 0;
    ResultSet rs = stmt.executeQuery();
    long actualCount = 0;
    Object[] lastPartialKey = new Object[keyFields.length];
    boolean lookingForUniqueKey = false;
    while (rs.next() && !cancelBulkImport.get(dbName + ":" + tableName).get()) {
      boolean foundUniqueKey = false;
      if (lookingForUniqueKey) {
        Object[] possibleKey = new Object[keyFields.length];
        possibleKey[0] = getValueOfField(rs, keyFields[0], dataTypes[0]);
        int compareValue = DatabaseCommon.compareKey(indexSchema.getComparators(), possibleKey, lastPartialKey);
        if (compareValue != 0) {
          foundUniqueKey = true;
        }
      }
      if (foundUniqueKey || recordOffset == offsets[slice]) {
        foundUniqueKey = false;
        lookingForUniqueKey = false;
        Object[] checkingKey = new Object[keyFields.length];
        checkingKey[0] = getValueOfField(rs, keyFields[0], dataTypes[0]);
        if (lastPartialKey[0] != null) {
          if (0 == DatabaseCommon.compareKey(indexSchema.getComparators(), checkingKey, lastPartialKey)) {
            lookingForUniqueKey = true;
          }
        }
        if (!lookingForUniqueKey) {
          keys[slice] = new Object[keyFields.length];
          for (int i = 0; i < keyFields.length; i++) {
            keys[slice][i] = getValueOfField(rs, keyFields[i], dataTypes[i]);
          }
          System.out.println("Key=" + DatabaseCommon.keyToString(keys[slice]));
          lastPartialKey[0] = keys[slice][0];
          slice++;
        }
      }
      if (slice == keys.length) {
        break;
      }
      countProcessed.incrementAndGet();
      recordOffset++;
      actualCount++;
    }

//    for (int i = 0; i < keys.length; i++) {
//      if (keys[i] == null || keys[i][0] == null) {
//        throw new DatabaseException("null primary key field: table=" + tableName + ", slice=" + i);
//      }
//    }

    long countPer = actualCount / serverCount;

    cobj.put(ComObject.Tag.expectedCount, countPer);
    logger.info("bulkImport got keys");

    cobj.put(ComObject.Tag.method, "startBulkImportOnServer");
    cobj.put(ComObject.Tag.shouldProcess, true);

    List<ComObject> requests = new ArrayList<>();
    int keyOffset = 0;
    outer:
    for (int i = 0; i < serverCount; i++) {
      cobj.remove(ComObject.Tag.lowerKey);
      cobj.remove(ComObject.Tag.nextKey);

      ComArray keyArray = cobj.putArray(ComObject.Tag.keys, ComObject.Type.byteArrayType);
      if (keyOffset != 0) {
        cobj.put(ComObject.Tag.lowerKey, DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), keys[keyOffset]));
        //keyOffset++;
      }
      for (int j = 0; j < BULK_IMPORT_THREAD_COUNT_PER_SERVER; j++) {
        if (keys[keyOffset] == null) {
          break;
        }
        keyArray.add(DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), keys[keyOffset]));
        keyOffset++;
      }
      if (keyOffset < keys.length) {
        if (keys[keyOffset] != null) {
          cobj.put(ComObject.Tag.nextKey, DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), keys[keyOffset]));
        }
      }
      byte[] bytes = cobj.serialize();
      requests.add(new ComObject(bytes));
    }

    Set<String> assigned = new HashSet<>();
    int requestOffset = 0;
    while (requestOffset < serverCount) {
      for (int shard = 0; shard < server.getShardCount(); shard++) {
        for (int replica = 0; replica < server.getReplicationFactor(); replica++) {
          if (!cancelBulkImport.get(dbName + ":" + tableName).get()) {
            if (!assigned.contains(shard + ":" + replica)) {
              byte[] bytes = server.getClient().send(null, shard, replica,
                  requests.get(requestOffset), DatabaseClient.Replica.specified);
              ComObject retObj = new ComObject(bytes);
              if (retObj.getBoolean(ComObject.Tag.accepted)) {
                assigned.add(shard + ":" + replica);
                logger.info("Successfully startedBulkImportOnServer: db=" + dbName + ", table=" + tableName +
                    ", shard=" + shard + ", replica=" + replica);
                requestOffset++;
              }
            }
          }
        }
      }
      Thread.sleep(10_000);
    }
  }

  private Object getValueOfField(ResultSet rs, String keyField, DataType.Type dataType) throws SQLException, UnsupportedEncodingException {
    switch (dataType) {
      case BIGINT:
        return rs.getLong(keyField);
      case INTEGER:
        return rs.getInt(keyField);
      case BIT:
        return rs.getBoolean(keyField);
      case TINYINT:
        return rs.getByte(keyField);
      case SMALLINT:
        return rs.getShort(keyField);
      case FLOAT:
        return rs.getDouble(keyField);
      case REAL:
        return rs.getFloat(keyField);
      case DOUBLE:
        return rs.getDouble(keyField);
      case NUMERIC:
        return rs.getBigDecimal(keyField);
      case DECIMAL:
        return rs.getBigDecimal(keyField);
      case CHAR:
        return rs.getString(keyField).getBytes("utf-8");
      case VARCHAR:
        return rs.getString(keyField).getBytes("utf-8");
      case LONGVARCHAR:
        return rs.getString(keyField).getBytes("utf-8");
      case DATE:
        return rs.getDate(keyField);
      case TIME:
        return rs.getTime(keyField);
      case TIMESTAMP:
        return rs.getTimestamp(keyField);
      case BINARY:
        return rs.getBytes(keyField);
      case VARBINARY:
        return rs.getBytes(keyField);
      case LONGVARBINARY:
        return rs.getBytes(keyField);
      case BLOB:
        return rs.getBytes(keyField);
      case CLOB:
        return rs.getString(keyField).getBytes("utf-8");
      case BOOLEAN:
        return rs.getBoolean(keyField);
      case ROWID:
        return rs.getLong(keyField);
      case NCHAR:
        return rs.getString(keyField).getBytes("utf-8");
      case NVARCHAR:
        return rs.getString(keyField).getBytes("utf-8");
      case LONGNVARCHAR:
        return rs.getString(keyField).getBytes("utf-8");
      default:
        throw new DatabaseException("Data type not supported: " + dataType.name());
    }
  }

  public static void setFieldsInInsertStatement(PreparedStatement insertStmt, Object[] currRecord, List<FieldSchema> fields) {
    try {
      int parmOffset = 1;
      int fieldOffset = 0;
      for (FieldSchema field : fields) {
        if (field.getName().equals("_id")) {
          //fieldOffset++;
          continue;
        }
        switch (field.getType()) {
          case BIT: {
            if (currRecord[fieldOffset] != null) {
              insertStmt.setBoolean(parmOffset, (Boolean) currRecord[fieldOffset]);
            }
            else {
              insertStmt.setNull(parmOffset, Types.BIT);
            }
          }
          break;
          case TINYINT: {
            if (currRecord[fieldOffset] != null) {
              insertStmt.setByte(parmOffset, (Byte) currRecord[fieldOffset]);
            }
            else {
              insertStmt.setNull(parmOffset, Types.TINYINT);
            }
          }
          break;
          case SMALLINT: {
            if (currRecord[fieldOffset] != null) {
              insertStmt.setShort(parmOffset, (Short) currRecord[fieldOffset]);
            }
            else {
              insertStmt.setNull(parmOffset, Types.SMALLINT);
            }
          }
          break;
          case INTEGER: {
            if (currRecord[fieldOffset] != null) {
              insertStmt.setInt(parmOffset, (Integer) currRecord[fieldOffset]);
            }
            else {
              insertStmt.setNull(parmOffset, Types.INTEGER);
            }
          }
          break;
          case BIGINT: {
            if (currRecord[fieldOffset] != null) {
              insertStmt.setLong(parmOffset, (Long) currRecord[fieldOffset]);
            }
            else {
              insertStmt.setNull(parmOffset, Types.BIGINT);
            }
          }
          break;
          case FLOAT: {
            if (currRecord[fieldOffset] != null) {
              insertStmt.setDouble(parmOffset, (Double) currRecord[fieldOffset]);
            }
            else {
              insertStmt.setNull(parmOffset, Types.FLOAT);
            }
          }
          break;
          case REAL: {
            if (currRecord[fieldOffset] != null) {
              insertStmt.setFloat(parmOffset, (Float) currRecord[fieldOffset]);
            }
            else {
              insertStmt.setNull(parmOffset, Types.REAL);
            }
          }
          break;
          case DOUBLE: {
            if (currRecord[fieldOffset] != null) {
              insertStmt.setDouble(parmOffset, (Double) currRecord[fieldOffset]);
            }
            else {
              insertStmt.setNull(parmOffset, Types.DOUBLE);
            }
          }
          break;
          case NUMERIC: {
            if (currRecord[fieldOffset] != null) {
              insertStmt.setBigDecimal(parmOffset, (BigDecimal) currRecord[fieldOffset]);
            }
            else {
              insertStmt.setNull(parmOffset, Types.NUMERIC);
            }
          }
          break;
          case DECIMAL: {
            if (currRecord[fieldOffset] != null) {
              insertStmt.setBigDecimal(parmOffset, (BigDecimal) currRecord[fieldOffset]);
            }
            else {
              insertStmt.setNull(parmOffset, Types.DECIMAL);
            }
          }
          break;
          case CHAR: {
            if (currRecord[fieldOffset] != null) {
              insertStmt.setString(parmOffset, (String) currRecord[fieldOffset]);
            }
            else {
              insertStmt.setNull(parmOffset, Types.CHAR);
            }
          }
          break;
          case VARCHAR: {
            if (currRecord[fieldOffset] != null) {
              insertStmt.setString(parmOffset, (String) currRecord[fieldOffset]);
            }
            else {
              insertStmt.setNull(parmOffset, Types.VARCHAR);
            }
          }
          break;
          case LONGVARCHAR: {
            if (currRecord[fieldOffset] != null) {
              insertStmt.setString(parmOffset, (String) currRecord[fieldOffset]);
            }
            else {
              insertStmt.setNull(parmOffset, Types.LONGVARCHAR);
            }
          }
          break;
          case DATE: {
            if (currRecord[fieldOffset] != null) {
              insertStmt.setDate(parmOffset, (java.sql.Date) currRecord[fieldOffset]);
            }
            else {
              insertStmt.setNull(parmOffset, Types.DATE);
            }
          }
          break;
          case TIME: {
            if (currRecord[fieldOffset] != null) {
              insertStmt.setTime(parmOffset, (java.sql.Time) currRecord[fieldOffset]);
            }
            else {
              insertStmt.setNull(parmOffset, Types.TIME);
            }
          }
          break;
          case TIMESTAMP: {
            if (currRecord[fieldOffset] != null) {
              insertStmt.setTimestamp(parmOffset, (Timestamp) currRecord[fieldOffset]);
            }
            else {
              insertStmt.setNull(parmOffset, Types.TIMESTAMP);
            }
          }
          break;
          case BINARY: {
            if (currRecord[fieldOffset] != null) {
              insertStmt.setBytes(parmOffset, (byte[]) currRecord[fieldOffset]);
            }
            else {
              insertStmt.setNull(parmOffset, Types.BINARY);
            }
          }
          break;
          case VARBINARY: {
            if (currRecord[fieldOffset] != null) {
              insertStmt.setBytes(parmOffset, (byte[]) currRecord[fieldOffset]);
            }
            else {
              insertStmt.setNull(parmOffset, Types.VARBINARY);
            }
          }
          break;
          case LONGVARBINARY: {
            if (currRecord[fieldOffset] != null) {
              insertStmt.setBytes(parmOffset, (byte[]) currRecord[fieldOffset]);
            }
            else {
              insertStmt.setNull(parmOffset, Types.LONGVARBINARY);
            }
          }
          break;
          case BLOB: {
            if (currRecord[fieldOffset] != null) {
              insertStmt.setBytes(parmOffset, (byte[]) currRecord[fieldOffset]);
            }
            else {
              insertStmt.setNull(parmOffset, Types.BLOB);
            }
          }
          break;
          case CLOB: {
            if (currRecord[fieldOffset] != null) {
              insertStmt.setString(parmOffset, (String) currRecord[fieldOffset]);
            }
            else {
              insertStmt.setNull(parmOffset, Types.CLOB);
            }
          }
          break;
          case BOOLEAN: {
            if (currRecord[fieldOffset] != null) {
              insertStmt.setBoolean(parmOffset, (Boolean) currRecord[fieldOffset]);
            }
            else {
              insertStmt.setNull(parmOffset, Types.BOOLEAN);
            }
          }
          break;
          case ROWID: {
            if (currRecord[fieldOffset] != null) {
              insertStmt.setLong(parmOffset, (Long) currRecord[fieldOffset]);
            }
            else {
              insertStmt.setNull(parmOffset, Types.ROWID);
            }
          }
          break;
          case NCHAR: {
            if (currRecord[fieldOffset] != null) {
              insertStmt.setString(parmOffset, (String) currRecord[fieldOffset]);
            }
            else {
              insertStmt.setNull(parmOffset, Types.NCHAR);
            }
          }
          break;
          case NVARCHAR: {
            if (currRecord[fieldOffset] != null) {
              insertStmt.setString(parmOffset, (String) currRecord[fieldOffset]);
            }
            else {
              insertStmt.setNull(parmOffset, Types.NVARCHAR);
            }
          }
          break;
          case LONGNVARCHAR: {
            if (currRecord[fieldOffset] != null) {
              insertStmt.setString(parmOffset, (String) currRecord[fieldOffset]);
            }
            else {
              insertStmt.setNull(parmOffset, Types.LONGNVARCHAR);
            }
          }
          break;
          case NCLOB: {
            if (currRecord[fieldOffset] != null) {
              insertStmt.setString(parmOffset, (String) currRecord[fieldOffset]);
            }
            else {
              insertStmt.setNull(parmOffset, Types.NCLOB);
            }
          }
          break;
        }
        parmOffset++;
        fieldOffset++;
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private void insertRecords(Connection insertConn, AtomicLong countProcessed, AtomicInteger countFinished, List<Object[]> currBatch, String tableName,
                             List<FieldSchema> fields, StringBuilder fieldsStr, StringBuilder parmsStr) {
    PreparedStatement insertStmt = null;
    try {
      //for (int i = 0; i < 10; i++) {
        insertStmt = insertConn.prepareStatement("insert into " + tableName +
            " (" + fieldsStr.toString() + ") VALUES (" + parmsStr.toString() + ")");

        try {
          for (Object[] currRecord : currBatch) {
            setFieldsInInsertStatement(insertStmt, currRecord, fields);

            insertStmt.addBatch();
          }
          insertStmt.executeBatch();

          countProcessed.addAndGet(currBatch.size());

          //break;
        }
        catch (Exception e) {
       //   if (i >= 9) {
            logger.error("Error inserting records - aborting", e);
            throw e;
         // }
          //logger.error("Error inserting records - will retry", e);
        }
      //}
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
        logger.error("Error closing connection", e);
      }
      finally {
        countFinished.incrementAndGet();
      }
    }
  }

  private AtomicInteger coordinatesCalled = new AtomicInteger();

  private ConcurrentHashMap<String, ConcurrentHashMap<String, BulkImportStatus>> getBulkImportStatus(String dbName) {
    ConcurrentHashMap<String, ConcurrentHashMap<String, BulkImportStatus>> bulkImportStatus = new ConcurrentHashMap<>();

    try {
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.method, "getBulkImportProgressOnServer");
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
          byte[] bytes = server.getClient().send(null, i, j, cobj, DatabaseClient.Replica.specified);
          ComObject retObj = new ComObject(bytes);

          ComArray array = retObj.getArray(ComObject.Tag.statuses);
          for (int k = 0; k < array.getArray().size(); k++) {
            ComObject tableStatus = (ComObject) array.getArray().get(k);
            dbName = tableStatus.getString(ComObject.Tag.dbName);
            String tableName = tableStatus.getString(ComObject.Tag.tableName);
            ConcurrentHashMap<String, BulkImportStatus> destStatus = bulkImportStatus.get(dbName + ":" + tableName);
            if (destStatus == null) {
              destStatus = new ConcurrentHashMap<>();
              bulkImportStatus.put(dbName + ":" + tableName, destStatus);
            }
            BulkImportStatus currStatus = destStatus.get(i + ":" + j);
            if (currStatus == null) {
              currStatus = new BulkImportStatus();
              destStatus.put(i + ":" + j, currStatus);
            }
            currStatus.countExpected = tableStatus.getLong(ComObject.Tag.expectedCount);
            currStatus.countProcessed = tableStatus.getLong(ComObject.Tag.countLong);
            currStatus.finished = tableStatus.getBoolean(ComObject.Tag.finished);
            currStatus.exception = tableStatus.getString(ComObject.Tag.exception);

            currStatus.preProcessCountExpected = tableStatus.getLong(ComObject.Tag.preProcessExpectedCount);
            currStatus.preProcessCountProcessed = tableStatus.getLong(ComObject.Tag.prePocessCountProcessed);
            currStatus.preProcessFinished = tableStatus.getBoolean(ComObject.Tag.preProcessFinished);
            currStatus.preProcessException = tableStatus.getString(ComObject.Tag.preProcessException);
          }
        }
      }
    }
    catch (Exception e) {
      logger.error("Error in import monitor thread", e);
    }
    return bulkImportStatus;
  }

  public ComObject startBulkImport(final ComObject cobj) {
    try {
      final String dbName = cobj.getString(ComObject.Tag.dbName);
      final String tableNames = cobj.getString(ComObject.Tag.tableName);
      String[] tables = tableNames.split(",");
      for (int i = 0; i < tables.length; i++) {
        tables[i] = tables[i].trim();
      }
      for (String tableName : tables) {
        cancelBulkImport.put(dbName + ":" + tableName, new AtomicBoolean(false));
      }

      ConcurrentHashMap<String, ConcurrentHashMap<String, BulkImportStatus>> bulkImportStatus = getBulkImportStatus(dbName);

      boolean inProgress = false;
      outer:
      for (Map.Entry<String, ConcurrentHashMap<String, BulkImportStatus>> entry : bulkImportStatus.entrySet()) {
        if (entry.getKey().startsWith(dbName + ":")) {
          String tableName = entry.getKey();
          for (String currTable : tables) {
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
              boolean preProcessFinished = false;
              String preProcessEx = null;
              for (Map.Entry<String, BulkImportStatus> serverEntry : entry.getValue().entrySet()) {

                if (serverEntry.getValue().preProcessFinished) {
                  preProcessFinished = true;
                }
                if (serverEntry.getValue().preProcessException != null) {
                  preProcessEx = serverEntry.getValue().preProcessException;
                }
              }
              if (!finished && exception == null) {
                inProgress = true;
                break outer;
              }
              if (!preProcessFinished && preProcessEx == null) {
                inProgress = true;
                break outer;
              }
            }
          }
        }
      }

      if (inProgress) {
        throw new DatabaseException("Cannot start bulk import. Table import currently in progress");
      }

      for (final String tableName : tables) {
        ConcurrentHashMap<String, BulkImportStatus> tableStatus = null;
        final int serverCount = server.getShardCount() * server.getReplicationFactor();


        String user = cobj.getString(ComObject.Tag.user);
        String password = cobj.getString(ComObject.Tag.password);

        logger.info("startBulkImport: dbName=" + dbName + ", tableName=" + tableName +
            ", driver=" + cobj.getString(ComObject.Tag.driverName) +
            ", connectStr=" + cobj.getString(ComObject.Tag.connectString) + ", user=" + user + ", password=" + password);

        final byte[] cobjBytes = cobj.serialize();

        Thread thread = new Thread(new Runnable() {
          @Override
          public void run() {
            boolean hasAccepted = false;
            try {
              outer:
              while (!cancelBulkImport.get(dbName + ":" + tableName).get()) {
                int offset = 0;
                for (int shard = 0; shard < server.getShardCount(); shard++) {
                  for (int replica = 0; replica < server.getReplicationFactor(); replica++) {
                    //  if (offset == coordinatesCalled.get()) {
                    ComObject cobj = new ComObject(cobjBytes);
                    cobj.put(ComObject.Tag.method, "coordinateBulkImportForTable");
                    cobj.put(ComObject.Tag.tableName, tableName);
                    if (hasAccepted) {
                      logger.info("bulkImport setting shouldProcess=false");
                      cobj.put(ComObject.Tag.shouldProcess, false);
                    }
                    else {
                      logger.info("bulkImport setting shouldProcess=true");
                      cobj.put(ComObject.Tag.shouldProcess, true);
                    }
                    byte[] bytes = server.getClient().send(null, shard, replica, cobj, DatabaseClient.Replica.specified);
                    ComObject retObj = new ComObject(bytes);
                    if (retObj.getBoolean(ComObject.Tag.accepted)) {
                      logger.info("bulkImport successfully started coordinateBulkImportForTable: db=" + dbName + ", table=" + tableName +
                          ", shard=" + shard + ", replica=" + replica);
                      hasAccepted = true;
                    }
                    else {
                      logger.info("bulkImport server did not accept coordinateBulkImportForTable: db=" + dbName + ", table=" + tableName +
                          ", shard=" + shard + ", replica=" + replica);
                      //    coordinatesCalled.set((coordinatesCalled.getAndIncrement()) % serverCount);
                    }
                    //}
                    offset++;
                  }
                }
                if (hasAccepted) {
                  break outer;
                }
                logger.info("Failed to start coordinateBulkImportForTable: db=" + dbName + ", table=" + tableName);
                coordinatesCalled.set((coordinatesCalled.getAndIncrement()) % serverCount);
                try {
                  Thread.sleep(5_000);
                }
                catch (InterruptedException e) {
                  logger.error("Interrupted", e);
                }
              }
            }
            catch (Exception e) {
              logger.error("Error importing table", e);
              String exceptionStr = ExceptionUtils.getStackTrace(e);
              preProcessException.put(dbName + ":" + tableName, exceptionStr);
            }
          }
        });
        thread.start();
      }
      return null;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public ComObject cancelBulkImport(final ComObject cobj) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    String tableName = cobj.getString(ComObject.Tag.tableName);
    cancelBulkImport.put(dbName + ":" + tableName, new AtomicBoolean(true));
    return null;
  }

  public ComObject getBulkImportProgress(ComObject cobj) {
    String dbName = cobj.getString(ComObject.Tag.dbName);

    ConcurrentHashMap<String, ConcurrentHashMap<String, BulkImportStatus>> bulkImportStatus = getBulkImportStatus(dbName);

    ComObject retObj = new ComObject();
    ComArray array = retObj.putArray(ComObject.Tag.progressArray, ComObject.Type.objectType);
    for (Map.Entry<String, ConcurrentHashMap<String, BulkImportStatus>> entry : bulkImportStatus.entrySet()) {
      if (entry.getKey().startsWith(dbName + ":")) {
        String tableName = entry.getKey();
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

        long preProcessCountExpected = 0;
        long preProcessCountProcessed = 0;
        boolean preProcessFinished = true;
        String preProcessEx = null;
        for (Map.Entry<String, BulkImportStatus> serverEntry : entry.getValue().entrySet()) {
          preProcessCountProcessed += serverEntry.getValue().preProcessCountProcessed;
          preProcessCountExpected += serverEntry.getValue().preProcessCountExpected;
          if (!serverEntry.getValue().preProcessFinished) {
            preProcessFinished = false;
          }
          if (serverEntry.getValue().preProcessException != null) {
            preProcessEx = serverEntry.getValue().preProcessException;
          }
        }

        ComObject serverObj = new ComObject();
        serverObj.put(ComObject.Tag.tableName, tableName);
        serverObj.put(ComObject.Tag.expectedCount, countExpected);
        serverObj.put(ComObject.Tag.countLong, countProcessed);
        serverObj.put(ComObject.Tag.finished, finished);
        if (exception != null) {
          serverObj.put(ComObject.Tag.exception, exception);
        }
        serverObj.put(ComObject.Tag.preProcessExpectedCount, preProcessCountExpected);
        serverObj.put(ComObject.Tag.prePocessCountProcessed, preProcessCountProcessed);
        serverObj.put(ComObject.Tag.preProcessFinished, preProcessFinished);
        if (preProcessEx != null) {
          serverObj.put(ComObject.Tag.preProcessException, preProcessEx);
        }
        array.add(serverObj);
      }
    }

    return retObj;
  }

}
