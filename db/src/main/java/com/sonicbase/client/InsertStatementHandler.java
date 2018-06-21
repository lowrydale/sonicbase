package com.sonicbase.client;

import com.sonicbase.common.*;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.jdbcdriver.QueryType;
import com.sonicbase.procedure.StoredProcedureContextImpl;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.query.impl.InsertStatementImpl;
import com.sonicbase.query.impl.SelectStatementImpl;
import com.sonicbase.schema.FieldSchema;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import org.apache.commons.lang.exception.ExceptionUtils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.sonicbase.client.DatabaseClient.SERIALIZATION_VERSION;
import static com.sonicbase.client.DatabaseClient.toLower;
import static com.sonicbase.server.PartitionManager.findOrderedPartitionForRecord;
import static com.sonicbase.server.UpdateManager.*;

public class InsertStatementHandler extends StatementHandler {
  private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("com.sonicbase.logger");

  private final DatabaseClient client;
  public static ThreadLocal<List<InsertRequest>> batch = new ThreadLocal<>();


  public InsertStatementHandler(DatabaseClient client) {
    this.client = client;
  }

  @Override
  public Object execute(String dbName, ParameterHandler parms, String sqlToUse, Statement statement,
                        SelectStatementImpl.Explain explain, Long sequence0, Long sequence1, Short sequence2,
                        boolean restrictToThisServer, StoredProcedureContextImpl procedureContext, int schemaRetryCount) throws SQLException {
    Insert insert = (Insert) statement;
    final InsertStatementImpl insertStatement = new InsertStatementImpl(client);
    insertStatement.setTableName(insert.getTable().getName());
    insertStatement.setIgnore(insert.isModifierIgnore());
    Select select = insert.getSelect();
    if (select != null) {
      SelectBody selectBody = select.getSelectBody();
      AtomicInteger currParmNum = new AtomicInteger();
      if (selectBody instanceof PlainSelect) {
        SelectStatementImpl selectStatement = SelectStatementHandler.parseSelectStatement(client, parms,
            (PlainSelect) selectBody, currParmNum);
        insertStatement.setSelect(selectStatement);
      }
      else {
        throw new DatabaseException("Unsupported select type");
      }
    }

    List<Object> values = new ArrayList<>();
    List<String> columnNames = new ArrayList<>();

    List srcColumns = insert.getColumns();
    ExpressionList items = (ExpressionList) insert.getItemsList();
    List srcExpressions = items == null ? null : items.getExpressions();
    int parmOffset = 1;
    for (int i = 0; i < srcColumns.size(); i++) {
      Column column = (Column) srcColumns.get(i);
      columnNames.add(toLower(column.getColumnName()));
      if (srcExpressions != null) {
        Expression expression = (Expression) srcExpressions.get(i);
        //todo: this doesn't handle out of order fields
        if (expression instanceof JdbcParameter) {
          values.add(parms.getValue(parmOffset++));
        }
        else if (expression instanceof StringValue) {
          values.add(((StringValue) expression).getValue());
        }
        else if (expression instanceof LongValue) {
          values.add(((LongValue) expression).getValue());
        }
        else if (expression instanceof DoubleValue) {
          values.add(((DoubleValue) expression).getValue());
        }
        else {
          throw new DatabaseException("Unexpected column type: " + expression.getClass().getName());
        }
      }
    }

    for (int i = 0; i < columnNames.size(); i++) {
      if (values != null && values.size() > i) {
        insertStatement.addValue(columnNames.get(i), values.get(i));
      }
    }
    insertStatement.setColumns(columnNames);

    if (client.isExplicitTrans()) {
      List<DatabaseClient.TransactionOperation> ops = client.getTransactionOps().get();
      if (ops == null) {
        ops = new ArrayList<>();
        client.getTransactionOps().set(ops);
      }
      ops.add(new DatabaseClient.TransactionOperation(insertStatement, parms));
    }
    if (insertStatement.getSelect() != null) {
      return doInsertWithSelect(dbName, insertStatement, parms);
    }
    return doInsert(dbName, insertStatement, parms, schemaRetryCount);

  }

  public class InsertRequest {
    private String dbName;
    private InsertStatementImpl insertStatement;
    private ParameterHandler parms;
    public boolean ignore;
  }

  class PreparedInsert {
    String dbName;
    int tableId;
    int indexId;
    String tableName;
    KeyInfo keyInfo;
    Record record;
    Object[] primaryKey;
    String primaryKeyIndexName;
    public TableSchema tableSchema;
    public List<String> columnNames;
    public List<Object> values;
    public long id;
    public String indexName;
    public KeyRecord keyRecord;
    public boolean ignore;
    public int originalOffset;
    public InsertStatementImpl originalStatement;
  }

  public static class KeyInfo {
    private boolean currPartition;
    private Object[] key;
    private int shard;
    private IndexSchema indexSchema;
    public boolean currAndLastMatch;

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "EI_EXPOSE_REP", justification = "copying the returned data is too slow")
    public Object[] getKey() {
      return key;
    }

    public int getShard() {
      return shard;
    }

    public IndexSchema getIndexSchema() {
      return indexSchema;
    }

    public boolean isCurrPartition() {
      return currPartition;
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "EI_EXPOSE_REP2", justification = "copying the passed in data is too slow")
    @SuppressWarnings("PMD.ArrayIsStoredDirectly") //copying the passed in data is too slow
    public KeyInfo(int shard, Object[] key, IndexSchema indexSchema, boolean currPartition) {
      this.shard = shard;
      this.key = key;
      this.indexSchema = indexSchema;
      this.currPartition = currPartition;
    }

    public KeyInfo() {
    }

    public void setKey(Object[] key) {
      this.key = key;
    }

    public void setIndexSchema(IndexSchema indexSchema) {
      this.indexSchema = indexSchema;
    }
  }

  public List<PreparedInsert> prepareInsert(InsertRequest request,
                                            List<KeyInfo> completed, AtomicLong recordId, long nonTransId, AtomicInteger originalOffset) throws UnsupportedEncodingException, SQLException {
    List<PreparedInsert> ret = new ArrayList<>();

    String dbName = request.dbName;

    List<String> columnNames;
    List<Object> values;

    String tableName = request.insertStatement.getTableName();

    TableSchema tableSchema = client.getCommon().getTables(dbName).get(tableName);
    if (tableSchema == null) {
      throw new DatabaseException("Table does not exist: name=" + tableName);
    }
    int tableId = tableSchema.getTableId();

    long id = -1;
    for (IndexSchema indexSchema : tableSchema.getIndexes().values()) {
      if (indexSchema.isPrimaryKey() && indexSchema.getFields()[0].equals("_sonicbase_id")) {
        if (recordId.get() == -1L) {
          id = client.allocateId(dbName);
        }
        else {
          id = recordId.get();
        }
        recordId.set(id);
        break;
      }
    }


    long transId = 0;
    if (!client.isExplicitTrans()) {
      transId = nonTransId;
    }
    else {
      transId = client.getTransactionId();
    }
    Record record = prepareRecordForInsert(request.insertStatement, tableSchema, id);
    record.setTransId(transId);

    Object[] fields = record.getFields();
    columnNames = new ArrayList<>();
    values = new ArrayList<>();
    for (int i = 0; i < fields.length; i++) {
      values.add(fields[i]);
      columnNames.add(tableSchema.getFields().get(i).getName());
    }


    int primaryKeyCount = 0;
    KeyInfo primaryKey = new KeyInfo();
    try {
      tableSchema = client.getCommon().getTables(dbName).get(tableName);

      List<KeyInfo> keys = getKeys(client.getCommon(), tableSchema, columnNames, values, id);
      if (keys.size() == 0) {
        throw new DatabaseException("key not generated for record to insert");
      }
      for (final KeyInfo keyInfo : keys) {
        if (keyInfo.indexSchema.isPrimaryKey()) {
          primaryKey.key = keyInfo.key;
          primaryKey.indexSchema = keyInfo.indexSchema;
          break;
        }
      }

      outer:
      for (final KeyInfo keyInfo : keys) {
        for (KeyInfo completedKey : completed) {
          Comparator[] comparators = keyInfo.indexSchema.getComparators();

          if (completedKey.indexSchema.getName().equals(keyInfo.indexSchema.getName()) &&
              DatabaseCommon.compareKey(comparators, completedKey.key, keyInfo.key) == 0
              &&
              completedKey.shard == keyInfo.shard
              ) {
            continue outer;
          }
        }

        PreparedInsert insert = new PreparedInsert();
        insert.dbName = dbName;
        insert.originalOffset = originalOffset.getAndIncrement();
        insert.originalStatement = request.insertStatement;
        insert.keyInfo = keyInfo;
        insert.record = record;
        insert.tableId = tableId;
        insert.indexId = keyInfo.indexSchema.getIndexId();
        insert.tableName = tableName;
        insert.primaryKeyIndexName = primaryKey.indexSchema.getName();
        insert.primaryKey = primaryKey.key;
        if (!keyInfo.indexSchema.isPrimaryKey()) {
          KeyRecord keyRecord = new KeyRecord();
          byte[] primaryKeyBytes = DatabaseCommon.serializeKey(client.getCommon().getTablesById(dbName).get(tableId),
              insert.primaryKeyIndexName, primaryKey.key);
          keyRecord.setPrimaryKey(primaryKeyBytes);
          keyRecord.setDbViewNumber(client.getCommon().getSchemaVersion());
          insert.keyRecord = keyRecord;
        }
        insert.tableSchema = tableSchema;
        insert.columnNames = columnNames;
        insert.values = values;
        insert.id = id;
        insert.indexName = keyInfo.indexSchema.getName();
        insert.ignore = request.ignore;
        ret.add(insert);
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
    return ret;
  }

  public static List<KeyInfo> getKeys(DatabaseCommon common, TableSchema tableSchema, List<String> columnNames,
                                                     List<Object> values, long id) {
    List<KeyInfo> ret = new ArrayList<>();
    for (Map.Entry<String, IndexSchema> indexSchema : tableSchema.getIndices().entrySet()) {
      String[] fields = indexSchema.getValue().getFields();
      boolean shouldIndex = true;
      for (int i = 0; i < fields.length; i++) {
        boolean found = false;
        for (int j = 0; j < columnNames.size(); j++) {
          if (fields[i].equals(columnNames.get(j))) {
            found = true;
            break;
          }
        }
        if (!found) {
          shouldIndex = false;
          break;
        }
      }
      if (shouldIndex) {
        String[] indexFields = indexSchema.getValue().getFields();
        int[] fieldOffsets = new int[indexFields.length];
        for (int i = 0; i < indexFields.length; i++) {
          fieldOffsets[i] = tableSchema.getFieldOffset(indexFields[i]);
        }
        TableSchema.Partition[] currPartitions = indexSchema.getValue().getCurrPartitions();
        TableSchema.Partition[] lastPartitions = indexSchema.getValue().getLastPartitions();

        Object[] key = new Object[indexFields.length];
        if (indexFields.length == 1 && indexFields[0].equals("_sonicbase_id")) {
          key[0] = id;
        }
        else {
          for (int i = 0; i < key.length; i++) {
            for (int j = 0; j < columnNames.size(); j++) {
              if (columnNames.get(j).equals(indexFields[i])) {
                key[i] = values.get(j);
              }
            }
          }
        }

        boolean keyIsNull = false;
        for (Object obj : key) {
          if (obj == null) {
            keyIsNull = true;
          }
        }

        if (!keyIsNull) {
          List<Integer> selectedShards = findOrderedPartitionForRecord(true, false, fieldOffsets, common, tableSchema,
              indexSchema.getKey(), null, com.sonicbase.query.BinaryExpression.Operator.equal, null, key, null);
          for (int partition : selectedShards) {
            int shard = currPartitions[partition].getShardOwning();
            ret.add(new KeyInfo(shard, key, indexSchema.getValue(), true));
          }

          selectedShards = findOrderedPartitionForRecord(false, true, fieldOffsets, common, tableSchema,
              indexSchema.getKey(), null, com.sonicbase.query.BinaryExpression.Operator.equal, null, key, null);

          for (int partition : selectedShards) {
            boolean found = false;
            int shard = lastPartitions[partition].getShardOwning();
            for (KeyInfo keyInfo : ret) {
              if (keyInfo.shard == shard) {
                keyInfo.currAndLastMatch = true;
                found = true;
                break;
              }
            }
            if (!found) {
              ret.add(new KeyInfo(shard, key, indexSchema.getValue(), false));
            }
          }
        }
      }
    }
    return ret;
  }


  private Record prepareRecordForInsert(
      InsertStatementImpl statement, TableSchema schema, long id) throws UnsupportedEncodingException, SQLException {
    Record record;
    FieldSchema fieldSchema;
    Object[] valuesToStore = new Object[schema.getFields().size()];

    List<String> columnNames = statement.getColumns();
    List<Object> values = statement.getValues();
    for (int i = 0; i < schema.getFields().size(); i++) {
      fieldSchema = schema.getFields().get(i);
      for (int j = 0; j < columnNames.size(); j++) {
        if (fieldSchema.getName().equals(columnNames.get(j))) {
          Object value = values.get(j);
          if (value != null) {
            value = fieldSchema.getType().getConverter().convert(value);
            if (fieldSchema.getWidth() != 0) {
              switch (fieldSchema.getType()) {
                case VARCHAR:
                case NVARCHAR:
                case LONGVARCHAR:
                case LONGNVARCHAR:
                case CLOB:
                case NCLOB:
                  String str = new String((byte[]) value, "utf-8");
                  if (str.length() > fieldSchema.getWidth()) {
                    throw new SQLException("value too long: field=" + fieldSchema.getName() + ", width=" + fieldSchema.getWidth());
                  }
                  break;
                case VARBINARY:
                case LONGVARBINARY:
                case BLOB:
                  if (((byte[]) value).length > fieldSchema.getWidth()) {
                    throw new SQLException("value too long: field=" + fieldSchema.getName() + ", width=" + fieldSchema.getWidth());
                  }
                  break;
              }
            }
          }
          valuesToStore[i] = value;
          break;
        }
        else {
          if (fieldSchema.getName().equals("_sonicbase_id")) {
            valuesToStore[i] = id;
          }
        }
      }
    }
    record = new Record(schema);
    record.setFields(valuesToStore);

    return record;
  }

  public int doInsert(String dbName, InsertStatementImpl insertStatement, ParameterHandler parms, int schemaRetryCount) throws SQLException {
    int previousSchemaVersion = client.getCommon().getSchemaVersion();
    InsertRequest request = new InsertRequest();
    request.dbName = dbName;
    request.insertStatement = insertStatement;
    request.parms = parms;
    request.ignore = insertStatement.isIgnore();
    boolean origIgnore = insertStatement.isIgnore();
    insertStatement.setIgnore(false);
    int insertCountCompleted = 0;
    List<KeyInfo> completed = new ArrayList<>();
    AtomicLong recordId = new AtomicLong(-1L);
    while (true) {
      if (client.getShutdown()) {
        throw new DatabaseException("Shutting down");
      }

      try {
        if (batch.get() != null) {
          batch.get().add(request);
        }
        else {
          long nonTransId = 0;
          if (!client.isExplicitTrans()) {
            nonTransId = client.allocateId(dbName);
          }

          List<PreparedInsert> inserts = prepareInsert(request, completed, recordId, nonTransId, new AtomicInteger());
          List<PreparedInsert> insertsWithRecords = new ArrayList<>();
          List<PreparedInsert> insertsWithKey = new ArrayList<>();
          for (PreparedInsert insert : inserts) {
            if (insert.keyInfo.indexSchema.isPrimaryKey()) {
              insertsWithRecords.add(insert);
            }
            else {
              insertsWithKey.add(insert);
            }
          }

          for (int i = 0; i < insertsWithRecords.size(); i++) {
            PreparedInsert insert = insertsWithRecords.get(i);
            insertKeyWithRecord(dbName, insertStatement.getTableName(), insert.keyInfo, insert.record, insertStatement.isIgnore(), schemaRetryCount);
            completed.add(insert.keyInfo);
          }

          for (int i = 0; i < insertsWithKey.size(); i++) {
            PreparedInsert insert = insertsWithKey.get(i);
            insertKey(client, dbName, insertStatement.getTableName(), insert.keyInfo, insert.primaryKeyIndexName,
                insert.primaryKey, insert.keyRecord, insertStatement.isIgnore(), schemaRetryCount);
            completed.add(insert.keyInfo);
          }
        }
        break;
      }
      catch (FailedToInsertException e) {
        logger.error(e.getMessage());
        continue;
      }
      catch (Exception e) {
        String stack = ExceptionUtils.getStackTrace(e);
        if ((e.getMessage() != null && e.getMessage().toLowerCase().contains("unique constraint violated")) ||
            stack.toLowerCase().contains("unique constraint violated")) {
          if (!origIgnore) {
            throw new DatabaseException(e);
          }
          return convertInsertToUpdate(dbName, insertStatement);
        }

        int index = ExceptionUtils.indexOfThrowable(e, SchemaOutOfSyncException.class);
        if (-1 != index) {
          continue;
        }
        throw new DatabaseException(e);
      }
    }
    return 1;
  }

  public static void insertKey(DatabaseClient client, String dbName, String tableName, KeyInfo keyInfo, String primaryKeyIndexName, Object[] primaryKey,
                        KeyRecord keyRecord, boolean ignore, int schemaRetryCount) {
    try {
      int tableId = client.getCommon().getTables(dbName).get(tableName).getTableId();
      int indexId = client.getCommon().getTables(dbName).get(tableName).getIndexes().get(keyInfo.indexSchema.getName()).getIndexId();
      ComObject cobj = serializeInsertKey(client.getCommon(), dbName, 0, tableId, indexId, tableName, keyInfo, primaryKeyIndexName,
          primaryKey, keyRecord, ignore);

      byte[] keyRecordBytes = keyRecord.serialize(SERIALIZATION_VERSION);
      cobj.put(ComObject.Tag.keyRecordBytes, keyRecordBytes);

      cobj.put(ComObject.Tag.dbName, dbName);
      cobj.put(ComObject.Tag.schemaVersion, client.getCommon().getSchemaVersion());
      if (schemaRetryCount < 2) {
        cobj.put(ComObject.Tag.schemaVersion, client.getCommon().getSchemaVersion());
      }
      cobj.put(ComObject.Tag.isExcpliciteTrans, client.isExplicitTrans());
      cobj.put(ComObject.Tag.isCommitting, client.isCommitting());
      cobj.put(ComObject.Tag.transactionId, client.getTransactionId());

      client.send("UpdateManager:insertIndexEntryByKey", keyInfo.shard, 0, cobj, DatabaseClient.Replica.def);
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public static ComObject serializeInsertKey(DatabaseCommon common, String dbName, int originalOffset, int tableId, int indexId,
                                             String tableName, KeyInfo keyInfo,
                                             String primaryKeyIndexName, Object[] primaryKey, KeyRecord keyRecord, boolean ignore) throws IOException {
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.serializationVersion, SERIALIZATION_VERSION);
    cobj.put(ComObject.Tag.tableId, tableId);
    cobj.put(ComObject.Tag.indexId, indexId);
    cobj.put(ComObject.Tag.ignore, ignore);
    cobj.put(ComObject.Tag.originalOffset, originalOffset);
    byte[] keyBytes = DatabaseCommon.serializeKey(common.getTables(dbName).get(tableName), keyInfo.indexSchema.getName(), keyInfo.key);
    cobj.put(ComObject.Tag.keyBytes, keyBytes);
    byte[] keyRecordBytes = keyRecord.serialize(SERIALIZATION_VERSION);
    cobj.put(ComObject.Tag.keyRecordBytes, keyRecordBytes);
    byte[] primaryKeyBytes = DatabaseCommon.serializeKey(common.getTables(dbName).get(tableName), primaryKeyIndexName, primaryKey);
    cobj.put(ComObject.Tag.primaryKeyBytes, primaryKeyBytes);

    return cobj;
  }

  class FailedToInsertException extends RuntimeException {
    public FailedToInsertException(String msg) {
      super(msg);
    }
  }

  public void insertKeyWithRecord(String dbName, String tableName, KeyInfo keyInfo, Record record, boolean ignore,
                                  int schemaRetryCount) {
    try {
      int tableId = client.getCommon().getTables(dbName).get(tableName).getTableId();
      int indexId = client.getCommon().getTables(dbName).get(tableName).getIndexes().get(keyInfo.indexSchema.getName()).getIndexId();
      ComObject cobj = serializeInsertKeyWithRecord(dbName, 0, tableId, indexId, tableName, keyInfo, record, ignore);
      cobj.put(ComObject.Tag.dbName, dbName);
      if (schemaRetryCount < 2) {
        cobj.put(ComObject.Tag.schemaVersion, client.getCommon().getSchemaVersion());
      }
      cobj.put(ComObject.Tag.method, "UpdateManager:insertIndexEntryByKeyWithRecord");
      cobj.put(ComObject.Tag.schemaVersion, client.getCommon().getSchemaVersion());
      cobj.put(ComObject.Tag.isExcpliciteTrans, client.isExplicitTrans());
      cobj.put(ComObject.Tag.isCommitting, client.isCommitting());
      cobj.put(ComObject.Tag.transactionId, client.getTransactionId());
      cobj.put(ComObject.Tag.ignore, ignore);

      int replicaCount = client.getReplicaCount();
      Exception lastException = null;
      //for (int i = 0; i < replicaCount; i++) {
      try {
        byte[] ret = client.send(null, keyInfo.shard, 0, cobj, DatabaseClient.Replica.def);
        if (ret == null) {
          throw new FailedToInsertException("No response for key insert");
        }
        ComObject retObj = new ComObject(ret);
        int retVal = retObj.getInt(ComObject.Tag.count);
        if (retVal != 1) {
          throw new FailedToInsertException("Incorrect response from server: value=" + retVal);
        }
      }
      catch (Exception e) {
        lastException = e;
      }
      //}
      if (lastException != null) {
        if (lastException instanceof SchemaOutOfSyncException) {
          throw (SchemaOutOfSyncException) lastException;
        }
        throw new DatabaseException(lastException);
      }
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  private ComObject serializeInsertKeyWithRecord(String dbName, int originalOffset, int tableId, int indexId, String tableName,
                                                 KeyInfo keyInfo, Record record, boolean ignore) throws IOException {
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.serializationVersion, SERIALIZATION_VERSION);
    cobj.put(ComObject.Tag.originalOffset, originalOffset);
    cobj.put(ComObject.Tag.indexId, indexId);
    cobj.put(ComObject.Tag.tableId, tableId);
    cobj.put(ComObject.Tag.originalIgnore, ignore);
    byte[] recordBytes = record.serialize(client.getCommon(), SERIALIZATION_VERSION);
    cobj.put(ComObject.Tag.recordBytes, recordBytes);
    cobj.put(ComObject.Tag.keyBytes, DatabaseCommon.serializeKey(client.getCommon().getTables(dbName).get(tableName), keyInfo.indexSchema.getName(), keyInfo.key));

    return cobj;
  }

  private int convertInsertToUpdate(String dbName, InsertStatementImpl insertStatement) throws SQLException {
    List<String> columns = insertStatement.getColumns();
    String sql = "update " + insertStatement.getTableName() + " set ";
    boolean first = true;
    for (String column : columns) {
      if (!first) {
        sql += ", ";
      }
      first = false;
      sql += column + "=? ";
    }
    TableSchema tableSchema = client.getCommon().getTables(dbName).get(insertStatement.getTableName());
    IndexSchema primaryKey = null;
    for (IndexSchema indexSchema : tableSchema.getIndices().values()) {
      if (indexSchema.isPrimaryKey()) {
        primaryKey = indexSchema;
      }
    }
    sql += " where ";
    first = true;
    for (String field : primaryKey.getFields()) {
      if (!first) {
        sql += " and ";
      }
      first = false;
      sql += field + "=? ";
    }

    int parmIndex = 1;
    ParameterHandler newParms = new ParameterHandler();
    for (int i = 0; i < insertStatement.getColumns().size(); i++) {
      String column = insertStatement.getColumns().get(i);
      Object obj = insertStatement.getValues().get(i);
      for (FieldSchema field : tableSchema.getFields()) {
        if (field.getName().equals(column)) {
          setFieldInParms(field, obj, parmIndex, newParms);
          parmIndex++;
          break;
        }
      }
    }
    for (String field : primaryKey.getFields()) {
      for (FieldSchema fieldSchema : tableSchema.getFields()) {
        if (field.equals(fieldSchema.getName())) {
          for (int i = 0; i < insertStatement.getColumns().size(); i++) {
            if (field.equals(insertStatement.getColumns().get(i))) {
              setFieldInParms(fieldSchema, insertStatement.getValues().get(i), parmIndex, newParms);
              parmIndex++;
              break;
            }
          }
          break;
        }
      }
    }
    return (int) client.executeQuery(dbName, QueryType.update1, sql, newParms, false,
        null, null, null, false, null, true);
  }

  public void setFieldInParms(FieldSchema field, Object obj, int parmIndex, ParameterHandler newParms) throws SQLException {
    switch (field.getType()) {
      case BIGINT:
      case ROWID:
        newParms.setLong(parmIndex, (Long) field.getType().getConverter().convert(obj));
        break;
      case VARCHAR:
      case NCHAR:
      case NVARCHAR:
      case LONGNVARCHAR:
      case NCLOB:
      case CLOB:
      case CHAR:
      case LONGVARCHAR:
        try {
          newParms.setString(parmIndex, new String((byte[]) field.getType().getConverter().convert(obj), "utf-8"));
        }
        catch (UnsupportedEncodingException e) {
          throw new DatabaseException(e);
        }
        break;
      case BOOLEAN:
      case BIT:
        newParms.setBoolean(parmIndex, (Boolean) field.getType().getConverter().convert(obj));
        break;
      case BLOB:
      case BINARY:
      case VARBINARY:
      case LONGVARBINARY:
        newParms.setBytes(parmIndex, (byte[]) field.getType().getConverter().convert(obj));
        break;
      case TINYINT:
        newParms.setByte(parmIndex, (byte) field.getType().getConverter().convert(obj));
        break;
      case SMALLINT:
        newParms.setShort(parmIndex, (short) field.getType().getConverter().convert(obj));
        break;
      case INTEGER:
        newParms.setInt(parmIndex, (int) field.getType().getConverter().convert(obj));
        break;
      case FLOAT:
      case DOUBLE:
        newParms.setDouble(parmIndex, (double) field.getType().getConverter().convert(obj));
        break;
      case REAL:
        newParms.setFloat(parmIndex, (float) field.getType().getConverter().convert(obj));
        break;
      case NUMERIC:
      case DECIMAL:
        newParms.setBigDecimal(parmIndex, (BigDecimal) field.getType().getConverter().convert(obj));
        break;
      case DATE:
        newParms.setDate(parmIndex, (java.sql.Date) field.getType().getConverter().convert(obj));
        break;
      case TIME:
        newParms.setTime(parmIndex, (java.sql.Time) field.getType().getConverter().convert(obj));
        break;
      case TIMESTAMP:
        newParms.setTimestamp(parmIndex, (Timestamp) field.getType().getConverter().convert(obj));
        break;
    }
  }

  private int doInsertWithSelect(String dbName, InsertStatementImpl insertStatement, ParameterHandler parms) {

    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.dbName, dbName);
    cobj.put(ComObject.Tag.schemaVersion, client.getCommon().getSchemaVersion());
    cobj.put(ComObject.Tag.method, "UpdateManager:insertWithSelect");


    SelectStatementImpl select = insertStatement.getSelect();
    if (select != null) {
      String[] tableNames = new String[]{select.getFromTable()};
      select.setTableNames(tableNames);
    }

    insertStatement.serialize(cobj);

    byte[] bytes = client.send(null, Math.abs(ThreadLocalRandom.current().nextInt() % client.getShardCount()),
        Math.abs(ThreadLocalRandom.current().nextLong()), cobj, DatabaseClient.Replica.def);
    ComObject retObj = new ComObject(bytes);
    return retObj.getInt(ComObject.Tag.count);
  }

  private void removeInsertKey(List<ComObject> cobjs, int originalOffset, Object[] primaryKey) {
    for (ComObject cobj : cobjs) {
      ComArray array = cobj.getArray(ComObject.Tag.insertObjects);
      ComArray newArray = new ComArray(ComObject.Type.objectType);
      for (int i = 0; i < array.getArray().size(); i++) {
        ComObject innerObj = (ComObject) array.getArray().get(i);
        if (innerObj.getInt(ComObject.Tag.originalOffset) != originalOffset) {
          newArray.add(innerObj);
        }
      }
      cobj.putArray(ComObject.Tag.insertObject, newArray);
    }
  }


  public int[] executeBatch() throws UnsupportedEncodingException, SQLException {

    try {
      final Object mutex = new Object();
      final List<PreparedInsert> withRecordPrepared = new ArrayList<>();
      final Map<Integer, PreparedInsert> withRecordPreparedMap = new HashMap<>();
      final List<PreparedInsert> preparedKeys = new ArrayList<>();
      long nonTransId = 0;
      while (true) {
        if (client.getShutdown()) {
          throw new DatabaseException("Shutting down");
        }

        try {
          if (!client.isExplicitTrans()) {
            nonTransId = client.allocateId(batch.get().get(0).dbName);
          }
          break;
        }
        catch (Exception e) {
          int index = ExceptionUtils.indexOfThrowable(e, SchemaOutOfSyncException.class);
          if (-1 != index) {
            continue;
          }
          throw new DatabaseException(e);
        }
      }

      AtomicInteger originalOffset = new AtomicInteger();
      for (InsertRequest request : batch.get()) {
        List<PreparedInsert> inserts = prepareInsert(request, new ArrayList<KeyInfo>(), new AtomicLong(-1L), nonTransId, originalOffset);
        for (PreparedInsert insert : inserts) {
          if (client.isExplicitTrans() && insert.ignore) {
            throw new DatabaseException("'ignore' is not supported for batch operations in an explicit transaction");
          }
          if (insert.keyInfo.indexSchema.isPrimaryKey()) {
            withRecordPrepared.add(insert);
            withRecordPreparedMap.put(insert.originalOffset, insert);
          }
          else {
            preparedKeys.add(insert);
          }
        }
      }
      int schemaRetryCount = 0;
      while (true) {
        if (client.getShutdown()) {
          throw new DatabaseException("Shutting down");
        }

        final AtomicInteger totalCount = new AtomicInteger();
        try {
          if (batch.get() == null) {
            throw new DatabaseException("No batch initiated");
          }

          String dbName = batch.get().get(0).dbName;

          final List<List<PreparedInsert>> withRecordProcessed = new ArrayList<>();
          final List<List<PreparedInsert>> processed = new ArrayList<>();
          final List<ByteArrayOutputStream> withRecordBytesOut = new ArrayList<>();
          final List<DataOutputStream> withRecordOut = new ArrayList<>();
          final List<ByteArrayOutputStream> bytesOut = new ArrayList<>();
          final List<DataOutputStream> out = new ArrayList<>();
          final List<ComObject> cobjs1 = new ArrayList<>();
          final List<ComObject> cobjs2 = new ArrayList<>();
          for (int i = 0; i < client.getShardCount(); i++) {
            ByteArrayOutputStream bOut = new ByteArrayOutputStream();
            withRecordBytesOut.add(bOut);
            withRecordOut.add(new DataOutputStream(bOut));
            bOut = new ByteArrayOutputStream();
            bytesOut.add(bOut);
            out.add(new DataOutputStream(bOut));
            withRecordProcessed.add(new ArrayList<PreparedInsert>());
            processed.add(new ArrayList<PreparedInsert>());

            final ComObject cobj1 = new ComObject();
            cobj1.put(ComObject.Tag.dbName, dbName);
            if (schemaRetryCount < 2) {
              cobj1.put(ComObject.Tag.schemaVersion, client.getCommon().getSchemaVersion());
            }
            cobj1.put(ComObject.Tag.method, "UpdateManager:batchInsertIndexEntryByKeyWithRecord");
            cobj1.put(ComObject.Tag.isExcpliciteTrans, client.isExplicitTrans());
            cobj1.put(ComObject.Tag.isCommitting, client.isCommitting());
            cobj1.put(ComObject.Tag.transactionId, client.getTransactionId());
            cobj1.put(ComObject.Tag.schemaVersion, client.getCommon().getSchemaVersion());

            cobj1.putArray(ComObject.Tag.insertObjects, ComObject.Type.objectType);
            cobjs1.add(cobj1);

            final ComObject cobj2 = new ComObject();
            cobj2.put(ComObject.Tag.dbName, dbName);
            if (schemaRetryCount < 2) {
              cobj2.put(ComObject.Tag.schemaVersion, client.getCommon().getSchemaVersion());
            }
            cobj2.put(ComObject.Tag.method, "UpdateManager:batchInsertIndexEntryByKey");
            cobj2.put(ComObject.Tag.isExcpliciteTrans, client.isExplicitTrans());
            cobj2.put(ComObject.Tag.isCommitting, client.isCommitting());
            cobj2.put(ComObject.Tag.transactionId, client.getTransactionId());
            cobj2.put(ComObject.Tag.schemaVersion, client.getCommon().getSchemaVersion());
            cobj2.putArray(ComObject.Tag.insertObjects, ComObject.Type.objectType);
            cobjs2.add(cobj2);

          }
          synchronized (mutex) {
            for (PreparedInsert insert : withRecordPrepared) {
              ComObject obj = serializeInsertKeyWithRecord(insert.dbName, insert.originalOffset, insert.tableId, insert.indexId, insert.tableName, insert.keyInfo, insert.record, insert.ignore);
              cobjs1.get(insert.keyInfo.shard).getArray(ComObject.Tag.insertObjects).getArray().add(obj);
              withRecordProcessed.get(insert.keyInfo.shard).add(insert);
            }
            for (PreparedInsert insert : preparedKeys) {
              ComObject obj = serializeInsertKey(client.getCommon(), insert.dbName, insert.originalOffset, insert.tableId, insert.indexId, insert.tableName, insert.keyInfo,
                  insert.primaryKeyIndexName, insert.primaryKey, insert.keyRecord, insert.ignore);
              cobjs2.get(insert.keyInfo.shard).getArray(ComObject.Tag.insertObjects).getArray().add(obj);
              processed.get(insert.keyInfo.shard).add(insert);
            }
          }

          List<Future<ComObject>> futures = new ArrayList<>();
          for (int i = 0; i < bytesOut.size(); i++) {
            final int offset = i;
            futures.add(client.getExecutor().submit(new Callable<ComObject>() {
              @Override
              public ComObject call() throws Exception {
                if (cobjs1.get(offset).getArray(ComObject.Tag.insertObjects).getArray().size() == 0) {
                  ComObject retObj = new ComObject();
                  return retObj;
                }
                byte[] ret = client.send(null, offset, 0, cobjs1.get(offset), DatabaseClient.Replica.def);
                if (ret == null) {
                  throw new FailedToInsertException("No response for key insert");
                }
                ComObject retObj = new ComObject(ret);
                if (retObj.getInt(ComObject.Tag.count) == null) {
                  throw new DatabaseException("count not returned: obj=" + retObj.toString());
                }
                int retVal = retObj.getInt(ComObject.Tag.count);
                totalCount.addAndGet(retVal);
                return retObj;
              }
            }));
          }
          List<ComObject> responses = new ArrayList<>();
          for (Future<ComObject> future : futures) {
            responses.add(future.get());
          }

          List<ComObject> allResponses = new ArrayList<>();
          for (ComObject response : responses) {
            ComArray array = response.getArray(ComObject.Tag.batchResponses);
            if (array != null) {
              for (Object obj : array.getArray()) {
                allResponses.add((ComObject) obj);
              }
            }
          }

          for (ComObject currRet : allResponses) {
            int currOriginalOffset = currRet.getInt(ComObject.Tag.originalOffset);
            int currStatus = currRet.getInt(ComObject.Tag.intStatus);
            PreparedInsert insert = withRecordPreparedMap.get(currOriginalOffset);
            if (insert.originalOffset == currOriginalOffset) {
              if (currStatus == BATCH_STATUS_UNIQUE_CONSTRAINT_VIOLATION) {
                if (insert.ignore) {
                  try {
                    convertInsertToUpdate(dbName, insert.originalStatement);
                    currRet.put(ComObject.Tag.intStatus, BATCH_STATUS_SUCCCESS);
                  }
                  catch (Exception e) {
                    logger.error("Error updating record", e);
                    currRet.put(ComObject.Tag.intStatus, BATCH_STATUS_FAILED);
                  }
                }
                else {
                  currRet.put(ComObject.Tag.intStatus, BATCH_STATUS_FAILED);
                }
                removeInsertKey(cobjs2, insert.originalOffset, insert.primaryKey);
              }
              else if (currStatus == BATCH_STATUS_FAILED) {
                removeInsertKey(cobjs2, insert.originalOffset, insert.primaryKey);
              }
            }
          }

          for (int i = 0; i < bytesOut.size(); i++) {
            final int offset = i;
            for (PreparedInsert insert : withRecordProcessed.get(offset)) {
              synchronized (mutex) {
                withRecordPrepared.remove(insert);
              }
            }
          }

          futures = new ArrayList<>();
          for (int i = 0; i < bytesOut.size(); i++) {
            final int offset = i;
            futures.add(client.getExecutor().submit(new Callable() {
              @Override
              public Object call() throws Exception {
                if (cobjs2.get(offset).getArray(ComObject.Tag.insertObjects).getArray().size() == 0) {
                  return null;
                }
                client.send(null, offset, 0, cobjs2.get(offset), DatabaseClient.Replica.def);

                for (PreparedInsert insert : processed.get(offset)) {
                  preparedKeys.remove(insert);
                }

                return null;
              }
            }));
          }
          Exception firstException = null;
          for (Future future : futures) {
            try {
              future.get();
            }
            catch (Exception e) {
              firstException = e;
            }
          }
          if (firstException != null) {
            throw firstException;
          }

          int maxOffset = 0;
          for (ComObject currRet : allResponses) {
            int currOriginalOffset = currRet.getInt(ComObject.Tag.originalOffset);
            maxOffset = Math.max(maxOffset, currOriginalOffset);
          }
          int[] ret = new int[maxOffset + 1];
          for (ComObject currRet : allResponses) {
            int currOriginalOffset = currRet.getInt(ComObject.Tag.originalOffset);
            ret[currOriginalOffset] = currRet.getInt(ComObject.Tag.intStatus);
          }
          return ret;
        }
        catch (Exception e) {
          if (e.getCause() instanceof SchemaOutOfSyncException) {
            synchronized (mutex) {
              for (PreparedInsert insert : withRecordPrepared) {
                List<KeyInfo> keys = getKeys(client.getCommon(), client.getCommon().getTables(insert.dbName).get(insert.tableSchema.getName()), insert.columnNames, insert.values, insert.id);
                for (KeyInfo key : keys) {
                  if (key.indexSchema.getName().equals(insert.indexName)) {
                    insert.keyInfo.shard = key.shard;
                    break;
                  }
                }
              }

              for (PreparedInsert insert : preparedKeys) {
                List<KeyInfo> keys = getKeys(client.getCommon(), client.getCommon().getTables(insert.dbName).get(insert.tableSchema.getName()), insert.columnNames, insert.values, insert.id);
                for (KeyInfo key : keys) {
                  if (key.indexSchema.getName().equals(insert.indexName)) {
                    insert.keyInfo.shard = key.shard;
                    break;
                  }
                }
              }
            }
            schemaRetryCount++;
            continue;
          }
          throw new DatabaseException(e);
        }
      }
    }
    catch (Exception e) {
      if (!(e instanceof SchemaOutOfSyncException)) {
        logger.error("Error processing batch request", e);
      }
      throw new DatabaseException(e);
    }
    finally {
      batch.set(null);
    }
  }
}
