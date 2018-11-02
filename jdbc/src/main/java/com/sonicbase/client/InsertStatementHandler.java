package com.sonicbase.client;

import com.sonicbase.common.*;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.procedure.StoredProcedureContextImpl;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.query.impl.InsertStatementImpl;
import com.sonicbase.query.impl.SelectStatementImpl;
import com.sonicbase.query.impl.UpdateStatementImpl;
import com.sonicbase.schema.FieldSchema;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.PartitionUtils;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.sonicbase.client.DatabaseClient.SERIALIZATION_VERSION;
import static com.sonicbase.client.DatabaseClient.toLower;
import static java.sql.Statement.EXECUTE_FAILED;
import static java.sql.Statement.SUCCESS_NO_INFO;

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class InsertStatementHandler implements StatementHandler {
  private static final String SONICBASE_ID_STR = "_sonicbase_id";
  private static final String SHUTTING_DOWN_STR = "Shutting down";
  private static final Logger logger = LoggerFactory.getLogger(InsertStatementHandler.class);

  public static final int BATCH_STATUS_SUCCCESS = SUCCESS_NO_INFO;
  public static final int BATCH_STATUS_FAILED = EXECUTE_FAILED;
  public static final int BATCH_STATUS_UNIQUE_CONSTRAINT_VIOLATION = -100;

  private final DatabaseClient client;
  private static ThreadLocal<List<InsertRequest>> batch = new ThreadLocal<>();


  public InsertStatementHandler(DatabaseClient client) {
    this.client = client;
  }

  public static ThreadLocal<List<InsertRequest>> getBatch() {
    return batch;
  }

  public static void setBatch(ThreadLocal<List<InsertRequest>> batch) {
    InsertStatementHandler.batch = batch;
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

    GetValuesFromParms getValuesFromParms = new GetValuesFromParms(parms, insert).invoke();
    List<Object> values = getValuesFromParms.getValues();
    List<String> columnNames = getValuesFromParms.getColumnNames();

    for (int i = 0; i < columnNames.size(); i++) {
      if (values != null && values.size() > i) {
        insertStatement.addValue(columnNames.get(i), values.get(i));
      }
    }
    insertStatement.setColumns(columnNames);

    if (insertStatement.getSelect() != null) {
      return doInsertWithSelect(dbName, insertStatement);
    }
    return doInsert(dbName, insertStatement, schemaRetryCount);

  }

  public class InsertRequest {
    private String dbName;
    private InsertStatementImpl insertStatement;
    private boolean ignore;
  }

  public class PreparedInsert {
    int insertId;
    private String dbName;
    private int tableId;
    private int indexId;
    private String tableName;
    private KeyInfo keyInfo;
    private Record record;
    private Object[] primaryKey;
    private String primaryKeyIndexName;
    private TableSchema tableSchema;
    private List<String> columnNames;
    private List<Object> values;
    private long id;
    private String indexName;
    private KeyRecord keyRecord;
    private boolean ignore;
    private int originalOffset;
    private InsertStatementImpl originalStatement;
    boolean inserted;
  }

  public static class KeyInfo {
    private Object[] key;
    private int shard;
    private IndexSchema indexSchema;

    public Object[] getKey() {
      return key;
    }

    public int getShard() {
      return shard;
    }

    public IndexSchema getIndexSchema() {
      return indexSchema;
    }

    public KeyInfo(int shard, Object[] key, IndexSchema indexSchema) {
      this.shard = shard;
      this.key = key;
      this.indexSchema = indexSchema;
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
                                            List<PreparedInsert> completed, AtomicLong recordId, long nonTransId, AtomicInteger originalOffset) throws UnsupportedEncodingException, SQLException {
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

    long id = getRecordId(recordId, dbName, tableSchema);

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

    KeyInfo primaryKey = new KeyInfo();
    try {
      tableSchema = client.getCommon().getTables(dbName).get(tableName);

      List<KeyInfo> keys = getKeys(columnNames, values, tableSchema, id, primaryKey);

      int offset = originalOffset.getAndIncrement();
      outer:
      for (final KeyInfo keyInfo : keys) {
        for (PreparedInsert completedKey : completed) {
          Comparator[] comparators = keyInfo.indexSchema.getComparators();

          if (completedKey.keyInfo.indexSchema.getName().equals(keyInfo.indexSchema.getName()) &&
              DatabaseCommon.compareKey(comparators, completedKey.keyInfo.key, keyInfo.key) == 0
              &&
              completedKey.keyInfo.shard == keyInfo.shard
              ) {
            continue outer;
          }
        }

        PreparedInsert insert = new PreparedInsert();
        insert.dbName = dbName;
        insert.originalOffset = offset;
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

  private List<KeyInfo> getKeys(List<String> columnNames, List<Object> values, TableSchema tableSchema, long id, KeyInfo primaryKey) {
    List<KeyInfo> keys = getKeys(tableSchema, columnNames, values, id);
    if (keys.isEmpty()) {
      throw new DatabaseException("key not generated for record to insert");
    }
    for (final KeyInfo keyInfo : keys) {
      if (keyInfo.indexSchema.isPrimaryKey()) {
        primaryKey.key = keyInfo.key;
        primaryKey.indexSchema = keyInfo.indexSchema;
        break;
      }
    }
    return keys;
  }

  private long getRecordId(AtomicLong recordId, String dbName, TableSchema tableSchema) {
    long id = -1;
    for (IndexSchema indexSchema : tableSchema.getIndices().values()) {
      if (indexSchema.isPrimaryKey()) {// && indexSchema.getFields()[0].equals(SONICBASE_ID_STR)) {
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
    return id;
  }

  public static List<KeyInfo> getKeys(TableSchema tableSchema, List<String> columnNames,
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

        doGetKeys(tableSchema, columnNames, values, id, ret, indexSchema);
      }
    }
    return ret;
  }

  private static void doGetKeys(TableSchema tableSchema, List<String> columnNames, List<Object> values, long id, List<KeyInfo> ret, Map.Entry<String, IndexSchema> indexSchema) {
    String[] indexFields = indexSchema.getValue().getFields();
    int[] fieldOffsets = new int[indexFields.length];
    for (int i = 0; i < indexFields.length; i++) {
      fieldOffsets[i] = tableSchema.getFieldOffset(indexFields[i]);
    }
    TableSchema.Partition[] currPartitions = indexSchema.getValue().getCurrPartitions();

    Object[] key = new Object[indexFields.length];
    if (indexFields.length == 1 && indexFields[0].equals(SONICBASE_ID_STR)) {
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

    setShard(tableSchema, ret, indexSchema, currPartitions, key);
  }

  private static void setShard(TableSchema tableSchema, List<KeyInfo> ret, Map.Entry<String, IndexSchema> indexSchema, TableSchema.Partition[] currPartitions, Object[] key) {
    boolean keyIsNull = false;
    for (Object obj : key) {
      if (obj == null) {
        keyIsNull = true;
      }
    }

    if (!keyIsNull) {
      List<Integer> selectedShards = PartitionUtils.findOrderedPartitionForRecord(true, false, tableSchema,
          indexSchema.getKey(), null, com.sonicbase.query.BinaryExpression.Operator.EQUAL, null, key, null);
      for (int partition : selectedShards) {
        int shard = currPartitions[partition].getShardOwning();
        ret.add(new KeyInfo(shard, key, indexSchema.getValue()));
      }
    }
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
            checkFieldWidth(fieldSchema, value);
          }
          valuesToStore[i] = value;
          break;
        }
        else {
          if (fieldSchema.getName().equals(SONICBASE_ID_STR)) {
            valuesToStore[i] = id;
          }
        }
      }
    }
    record = new Record(schema);
    record.setFields(valuesToStore);

    return record;
  }

  private void checkFieldWidth(FieldSchema fieldSchema, Object value) throws UnsupportedEncodingException, SQLException {
    if (fieldSchema.getWidth() != 0) {
      switch (fieldSchema.getType()) {
        case VARCHAR:
        case NVARCHAR:
        case LONGVARCHAR:
        case LONGNVARCHAR:
        case CLOB:
        case NCLOB:
          String str = new String((byte[])value, "utf-8");
          if (str.length() > fieldSchema.getWidth()) {
            throw new SQLException("value too long: field=" + fieldSchema.getName() + ", width=" + fieldSchema.getWidth());
          }
          break;
        case VARBINARY:
        case LONGVARBINARY:
        case BLOB:
          if (((byte[])value).length > fieldSchema.getWidth()) {
            throw new SQLException("value too long: field=" + fieldSchema.getName() + ", width=" + fieldSchema.getWidth());
          }
          break;
      }
    }
  }

  public int doInsert(String dbName, InsertStatementImpl insertStatement, int schemaRetryCount) throws SQLException {
    InsertRequest request = new InsertRequest();
    request.dbName = dbName;
    request.insertStatement = insertStatement;
    request.ignore = insertStatement.isIgnore();
    boolean origIgnore = insertStatement.isIgnore();
    insertStatement.setIgnore(false);
    List<PreparedInsert> completed = new ArrayList<>();
    AtomicLong recordId = new AtomicLong(-1L);
    if (getBatch().get() != null) {
      getBatch().get().add(request);
    }
    while (!client.getShutdown()) {
      try {
        if (getBatch().get() == null) {
          doInsert(dbName, insertStatement, schemaRetryCount, request, completed, recordId);
        }
        break;
      }
      catch (FailedToInsertException e) {
        logger.error(e.getMessage());
        break;
      }
      catch (Exception e) {
        String stack = ExceptionUtils.getStackTrace(e);
        if (handleUniqueConstraintViolation(origIgnore, e, stack)) {
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

  private boolean handleUniqueConstraintViolation(boolean origIgnore, Exception e, String stack) {
    if ((e.getMessage() != null && e.getMessage().toLowerCase().contains("unique constraint violated")) ||
        stack.toLowerCase().contains("unique constraint violated")) {
      if (!origIgnore) {
        throw new DatabaseException(e);
      }
      return true;
    }
    return false;
  }

  private void doInsert(String dbName, InsertStatementImpl insertStatement, int schemaRetryCount, InsertRequest request,
                        List<PreparedInsert> completed, AtomicLong recordId) throws UnsupportedEncodingException, SQLException {
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

    while (true) {
      try {
        for (int i = 0; i < insertsWithRecords.size(); i++) {
          PreparedInsert insert = insertsWithRecords.get(i);
          insertKeyWithRecord(dbName, insertStatement.getTableName(), insert.keyInfo, insert.record, insertStatement.isIgnore(), schemaRetryCount);
          completed.add(insert);
        }

        for (int i = 0; i < insertsWithKey.size(); i++) {
          PreparedInsert insert = insertsWithKey.get(i);
          insertKey(client, dbName, insertStatement.getTableName(), insert.keyInfo, insert.primaryKeyIndexName,
              insert.primaryKey, insert.record, insert.keyRecord, insertStatement.isIgnore(), schemaRetryCount);
          completed.add(insert);
        }
        break;
      }
      catch (Exception e) {
        for (PreparedInsert insert : completed) {
          UpdateStatementImpl.deleteKey(client, dbName, insertStatement.getTableName(), insert.keyInfo,
              insert.keyInfo.indexSchema.getName(), insert.primaryKey, schemaRetryCount);
        }
        int index = ExceptionUtils.indexOfThrowable(e, SchemaOutOfSyncException.class);
        if (-1 != index) {
          continue;
        }

        throw new DatabaseException(e);
      }
    }
  }

  public static void insertKey(DatabaseClient client, String dbName, String tableName, KeyInfo keyInfo,
                               String primaryKeyIndexName, Object[] primaryKey, Record record,
                               KeyRecord keyRecord, boolean ignore, int schemaRetryCount) {
    try {
      int tableId = client.getCommon().getTables(dbName).get(tableName).getTableId();
      int indexId = client.getCommon().getTables(dbName).get(tableName).getIndices().get(keyInfo.indexSchema.getName()).getIndexId();
      ComObject cobj = serializeInsertKey(client, client.getCommon(), 0, dbName, 0, tableId, indexId, tableName, keyInfo, primaryKeyIndexName,
          primaryKey, record, keyRecord, ignore);

      byte[] keyRecordBytes = keyRecord.serialize(SERIALIZATION_VERSION);
      cobj.put(ComObject.Tag.KEY_RECORD_BYTES, keyRecordBytes);

      cobj.put(ComObject.Tag.DB_NAME, dbName);
      cobj.put(ComObject.Tag.SCHEMA_VERSION, client.getCommon().getSchemaVersion());
      if (schemaRetryCount < 2) {
        cobj.put(ComObject.Tag.SCHEMA_VERSION, client.getCommon().getSchemaVersion());
      }
      cobj.put(ComObject.Tag.IS_EXCPLICITE_TRANS, client.isExplicitTrans());
      cobj.put(ComObject.Tag.IS_COMMITTING, client.isCommitting());
      cobj.put(ComObject.Tag.TRANSACTION_ID, client.getTransactionId());

      client.send("UpdateManager:insertIndexEntryByKey", keyInfo.shard, 0, cobj, DatabaseClient.Replica.DEF);
    }
    catch (Exception e) {
      throw new DatabaseException("Error inserting key: db=" + dbName + ", table=" + tableName + ", index=" + keyInfo.indexSchema.getName(), e);
    }
  }

  public static ComObject serializeInsertKey(DatabaseClient client, DatabaseCommon common, int insertId, String dbName, int originalOffset, int tableId, int indexId,
                                             String tableName, KeyInfo keyInfo,
                                             String primaryKeyIndexName, Object[] primaryKey, Record record, KeyRecord keyRecord, boolean ignore) {
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.SERIALIZATION_VERSION, SERIALIZATION_VERSION);
    cobj.put(ComObject.Tag.TABLE_ID, tableId);
    cobj.put(ComObject.Tag.INDEX_ID, indexId);
    cobj.put(ComObject.Tag.IGNORE, ignore);
    cobj.put(ComObject.Tag.ORIGINAL_OFFSET, originalOffset);
    cobj.put(ComObject.Tag.ID, (long)insertId);
    byte[] keyBytes = DatabaseCommon.serializeKey(common.getTables(dbName).get(tableName), keyInfo.indexSchema.getName(), keyInfo.key);
    cobj.put(ComObject.Tag.KEY_BYTES, keyBytes);
    if (record != null) {
      byte[] recordBytes = record.serialize(client.getCommon(), SERIALIZATION_VERSION);
      cobj.put(ComObject.Tag.RECORD_BYTES, recordBytes);
    }

    byte[] keyRecordBytes = keyRecord.serialize(SERIALIZATION_VERSION);
    cobj.put(ComObject.Tag.KEY_RECORD_BYTES, keyRecordBytes);
    byte[] primaryKeyBytes = DatabaseCommon.serializeKey(common.getTables(dbName).get(tableName), primaryKeyIndexName, primaryKey);
    cobj.put(ComObject.Tag.PRIMARY_KEY_BYTES, primaryKeyBytes);

    return cobj;
  }

  class FailedToInsertException extends RuntimeException {
    FailedToInsertException(String msg) {
      super(msg);
    }
  }

  private void insertKeyWithRecord(String dbName, String tableName, KeyInfo keyInfo, Record record, boolean ignore,
                                   int schemaRetryCount) {
    int tableId = client.getCommon().getTables(dbName).get(tableName).getTableId();
    int indexId = client.getCommon().getTables(dbName).get(tableName).getIndices().get(keyInfo.indexSchema.getName()).getIndexId();
    ComObject cobj = serializeInsertKeyWithRecord(0, dbName, 0, tableId, indexId, tableName, keyInfo, record, ignore);
    cobj.put(ComObject.Tag.DB_NAME, dbName);
    if (schemaRetryCount < 2) {
      cobj.put(ComObject.Tag.SCHEMA_VERSION, client.getCommon().getSchemaVersion());
    }
    cobj.put(ComObject.Tag.SCHEMA_VERSION, client.getCommon().getSchemaVersion());
    cobj.put(ComObject.Tag.IS_EXCPLICITE_TRANS, client.isExplicitTrans());
    cobj.put(ComObject.Tag.IS_COMMITTING, client.isCommitting());
    cobj.put(ComObject.Tag.TRANSACTION_ID, client.getTransactionId());
    cobj.put(ComObject.Tag.IGNORE, ignore);

    Exception lastException = null;
    try {
      byte[] ret = client.send("UpdateManager:insertIndexEntryByKeyWithRecord", keyInfo.shard, 0, cobj, DatabaseClient.Replica.DEF);
      if (ret == null) {
        throw new FailedToInsertException("No response for key insert");
      }
      ComObject retObj = new ComObject(ret);
      int retVal = retObj.getInt(ComObject.Tag.COUNT);
      if (retVal != 1) {
        throw new FailedToInsertException("Incorrect response from server: value=" + retVal);
      }
    }
    catch (Exception e) {
      lastException = e;
    }
    if (lastException != null) {
      if (lastException instanceof SchemaOutOfSyncException) {
        throw (SchemaOutOfSyncException) lastException;
      }
      throw new DatabaseException(lastException);
    }
  }

  private ComObject serializeInsertKeyWithRecord(int insertId, String dbName, int originalOffset, int tableId, int indexId, String tableName,
                                                 KeyInfo keyInfo, Record record, boolean ignore) {
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.SERIALIZATION_VERSION, SERIALIZATION_VERSION);
    cobj.put(ComObject.Tag.ORIGINAL_OFFSET, originalOffset);
    cobj.put(ComObject.Tag.ID, (long)insertId);
    cobj.put(ComObject.Tag.INDEX_ID, indexId);
    cobj.put(ComObject.Tag.TABLE_ID, tableId);
    cobj.put(ComObject.Tag.ORIGINAL_IGNORE, ignore);
    byte[] recordBytes = record.serialize(client.getCommon(), SERIALIZATION_VERSION);
    cobj.put(ComObject.Tag.RECORD_BYTES, recordBytes);
    cobj.put(ComObject.Tag.KEY_BYTES, DatabaseCommon.serializeKey(client.getCommon().getTables(dbName).get(tableName),
        keyInfo.indexSchema.getName(), keyInfo.key));

    return cobj;
  }

  int convertInsertToUpdate(String dbName, InsertStatementImpl insertStatement) throws SQLException {
    List<String> columns = insertStatement.getColumns();
    StringBuilder sql = new StringBuilder();
    sql.append("update ").append(insertStatement.getTableName()).append(" set ");
    boolean first = true;
    for (String column : columns) {
      if (!first) {
        sql.append(", ");
      }
      first = false;
      sql.append(column).append("=? ");
    }
    TableSchema tableSchema = client.getCommon().getTables(dbName).get(insertStatement.getTableName());
    IndexSchema primaryKey = null;
    for (IndexSchema indexSchema : tableSchema.getIndices().values()) {
      if (indexSchema.isPrimaryKey()) {
        primaryKey = indexSchema;
      }
    }
    if (primaryKey == null) {
      throw new DatabaseException("primary index not found: table=" + tableSchema.getName());
    }
    sql.append(" where ");
    first = true;
    for (String field : primaryKey.getFields()) {
      if (!first) {
        sql.append(" and ");
      }
      first = false;
      sql.append(field).append("=? ");
    }

    ParameterHandler newParms = setFieldsInParms(insertStatement, tableSchema, primaryKey);

    return (int) client.executeQuery(dbName, sql.toString(), newParms, null, null, null,
        false, null, true);
  }

  private ParameterHandler setFieldsInParms(InsertStatementImpl insertStatement, TableSchema tableSchema,
                                            IndexSchema primaryKey) throws SQLException {
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

    setFieldsInParmsForPrimaryKey(insertStatement, tableSchema, primaryKey, parmIndex, newParms);

    return newParms;
  }

  private void setFieldsInParmsForPrimaryKey(InsertStatementImpl insertStatement, TableSchema tableSchema,
                                             IndexSchema primaryKey, int parmIndex, ParameterHandler newParms) throws SQLException {
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
  }

  private void setFieldInParms(FieldSchema field, Object obj, int parmIndex, ParameterHandler newParms) throws SQLException {
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

  private int doInsertWithSelect(String dbName, InsertStatementImpl insertStatement) {

    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, dbName);
    cobj.put(ComObject.Tag.SCHEMA_VERSION, client.getCommon().getSchemaVersion());
    cobj.put(ComObject.Tag.METHOD, "UpdateManager:insertWithSelect");


    SelectStatementImpl select = insertStatement.getSelect();
    if (select != null) {
      String[] tableNames = new String[]{select.getFromTable()};
      select.setTableNames(tableNames);
    }

    insertStatement.serialize(cobj);

    byte[] bytes = client.send(null, Math.abs(ThreadLocalRandom.current().nextInt() % client.getShardCount()),
        Math.abs(ThreadLocalRandom.current().nextLong()), cobj, DatabaseClient.Replica.DEF);
    ComObject retObj = new ComObject(bytes);
    return retObj.getInt(ComObject.Tag.COUNT);
  }

  private void removeInsertKey(List<ComObject> cobjs, int originalOffset) {
    for (ComObject cobj : cobjs) {
      ComArray array = cobj.getArray(ComObject.Tag.INSERT_OBJECTS);
      ComArray newArray = new ComArray(ComObject.Type.OBJECT_TYPE);
      for (int i = 0; i < array.getArray().size(); i++) {
        ComObject innerObj = (ComObject) array.getArray().get(i);
        if (innerObj.getInt(ComObject.Tag.ORIGINAL_OFFSET) != originalOffset) {
          newArray.add(innerObj);
        }
      }
      cobj.putArray(ComObject.Tag.INSERT_OBJECT, newArray);
    }
  }


  public int[] executeBatch() {

    try {
      final Object mutex = new Object();
      final List<PreparedInsert> withRecordPrepared = new ArrayList<>();
      final Map<Integer, PreparedInsert> withRecordPreparedMap = new HashMap<>();
      final List<PreparedInsert> preparedKeys = new ArrayList<>();
      long nonTransId = getNonTransId();

      AtomicInteger originalOffset = new AtomicInteger();

      prepareInserts(withRecordPrepared, withRecordPreparedMap, preparedKeys, nonTransId, originalOffset);

      int schemaRetryCount = 0;
      while (true) {
        if (client.getShutdown()) {
          throw new DatabaseException(SHUTTING_DOWN_STR);
        }

        final AtomicInteger totalCount = new AtomicInteger();
        try {
          if (getBatch().get() == null) {
            throw new DatabaseException("No batch initiated");
          }

          String dbName = getBatch().get().get(0).dbName;

          Map<Integer, PreparedInsert> withRecordPreparedById = new HashMap<>();
          for (PreparedInsert insert : withRecordPrepared) {
            withRecordPreparedById.put(insert.insertId, insert);
          }
          Map<Integer, PreparedInsert> preparedKeysById = new HashMap<>();
          for (PreparedInsert insert : preparedKeys) {
            preparedKeysById.put(insert.insertId, insert);
          }

          final List<List<PreparedInsert>> withRecordProcessed = new ArrayList<>();
          final List<List<PreparedInsert>> processed = new ArrayList<>();
          final List<ByteArrayOutputStream> withRecordBytesOut = new ArrayList<>();
          final List<DataOutputStream> withRecordOut = new ArrayList<>();
          final List<ByteArrayOutputStream> bytesOut = new ArrayList<>();
          final List<DataOutputStream> out = new ArrayList<>();
          final List<ComObject> cobjs1 = new ArrayList<>();
          final List<ComObject> cobjs2 = new ArrayList<>();

          prepareBatchComObjects(mutex, withRecordPrepared, preparedKeys, schemaRetryCount, dbName, withRecordProcessed,
              processed, withRecordBytesOut, withRecordOut, bytesOut, out, cobjs1, cobjs2);

          List<ComObject> responses = batchInsertIndexEntryByKeyWithRecord(totalCount, bytesOut, cobjs1);

          List<ComObject> allResponses = handleBatchResponses(withRecordPreparedById, withRecordPreparedMap, dbName, cobjs2, responses);

          removeInsertedRecord(mutex, withRecordPrepared, withRecordProcessed, bytesOut);

          List<ComObject> keyResponses = batchInsertIndexEntryByKey(preparedKeys, processed, bytesOut, cobjs2);

          List<ComObject> keyAllResponses = handleBatchResponsesForSecondaryKey(preparedKeysById, withRecordPreparedMap, dbName, cobjs2, keyResponses);

          int[] ret = fixupRetArray(allResponses, keyAllResponses);

          doRollbackFailedInserts(ret, dbName, withRecordPreparedById, cobjs1);
          doRollbackFailedInserts(ret, dbName, preparedKeysById, cobjs2);

          return ret;
        }
        catch (Exception e) {
          schemaRetryCount = handleExceptionForBatchInsert(mutex, withRecordPrepared, preparedKeys, schemaRetryCount, e);
        }
      }
    }
    catch (SchemaOutOfSyncException e) {
      logger.error("Error processing batch request", e);
      throw new DatabaseException(e);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
    finally {
      getBatch().set(null);
    }
  }

  private void doRollbackFailedInserts(int[] ret, String dbName, Map<Integer, PreparedInsert> preparedRecordsById, List<ComObject> cobjs1) {
    for (ComObject cobj1 : cobjs1) {
      ComArray array = cobj1.getArray(ComObject.Tag.INSERT_OBJECTS);
      for (int i = 0; i < array.getArray().size(); i++) {
        ComObject cobj = (ComObject) array.getArray().get(i);
        int id = (int)(long)cobj.getLong(ComObject.Tag.ID);
        int offset = cobj.getInt(ComObject.Tag.ORIGINAL_OFFSET);

        if (ret[offset] != BATCH_STATUS_SUCCCESS) {
          PreparedInsert insert = preparedRecordsById.get(id);
          if (insert.inserted) {
            UpdateStatementImpl.deleteKey(client, dbName, insert.tableName, insert.keyInfo,
                insert.keyInfo.indexSchema.getName(), insert.primaryKey, 0);
          }
        }
      }
    }
  }

  private List<ComObject> handleBatchResponsesForSecondaryKey(Map<Integer, PreparedInsert> preparedKeysById, Map<Integer, PreparedInsert> withRecordPreparedMap, String dbName,
                                                              List<ComObject> cobjs2, List<ComObject> responses) {
    List<ComObject> allResponses = new ArrayList<>();
    for (ComObject response : responses) {
      ComArray array = response.getArray(ComObject.Tag.BATCH_RESPONSES);
      if (array != null) {
        for (Object obj : array.getArray()) {
          allResponses.add((ComObject) obj);
        }
      }
    }

    for (ComObject currRet : allResponses) {
      int currOriginalOffset = currRet.getInt(ComObject.Tag.ORIGINAL_OFFSET);
      int currStatus = currRet.getInt(ComObject.Tag.INT_STATUS);
      int currId = (int)(long)currRet.getLong(ComObject.Tag.ID);
      PreparedInsert insert = withRecordPreparedMap.get(currOriginalOffset);
      if (insert.originalOffset == currOriginalOffset) {
        if (currStatus == BATCH_STATUS_UNIQUE_CONSTRAINT_VIOLATION) {
          batchUpsertIfNeeded(dbName, currRet, insert);
          removeInsertKey(cobjs2, insert.originalOffset);
          preparedKeysById.get(currId).inserted = false;
        }
        else if (currStatus == BATCH_STATUS_FAILED) {
          removeInsertKey(cobjs2, insert.originalOffset);
          preparedKeysById.get(currId).inserted = false;
        }
        else {
          preparedKeysById.get(currId).inserted = true;
        }
      }
    }
    return allResponses;
  }

  private long getNonTransId() {
    long nonTransId = 0;
    while (true) {
      if (client.getShutdown()) {
        throw new DatabaseException(SHUTTING_DOWN_STR);
      }
      try {
        if (!client.isExplicitTrans()) {
          nonTransId = client.allocateId(getBatch().get().get(0).dbName);
        }
        return nonTransId;
      }
      catch (Exception e) {
        int index = ExceptionUtils.indexOfThrowable(e, SchemaOutOfSyncException.class);
        if (-1 != index) {
          continue;
        }
        throw new DatabaseException(e);
      }
    }
  }

  private void removeInsertedRecord(Object mutex, List<PreparedInsert> withRecordPrepared,
                                    List<List<PreparedInsert>> withRecordProcessed, List<ByteArrayOutputStream> bytesOut) {
    for (int i = 0; i < bytesOut.size(); i++) {
      for (PreparedInsert insert : withRecordProcessed.get(i)) {
        synchronized (mutex) {
          withRecordPrepared.remove(insert);
        }
      }
    }
  }

  private void prepareBatchComObjects(Object mutex, List<PreparedInsert> withRecordPrepared,
                                      List<PreparedInsert> preparedKeys, int schemaRetryCount, String dbName,
                                      List<List<PreparedInsert>> withRecordProcessed, List<List<PreparedInsert>> processed,
                                      List<ByteArrayOutputStream> withRecordBytesOut, List<DataOutputStream> withRecordOut,
                                      List<ByteArrayOutputStream> bytesOut, List<DataOutputStream> out, List<ComObject> cobjs1,
                                      List<ComObject> cobjs2) {
    for (int i = 0; i < client.getShardCount(); i++) {
      ByteArrayOutputStream bOut = new ByteArrayOutputStream();
      withRecordBytesOut.add(bOut);
      withRecordOut.add(new DataOutputStream(bOut));
      bOut = new ByteArrayOutputStream();
      bytesOut.add(bOut);
      out.add(new DataOutputStream(bOut));
      withRecordProcessed.add(new ArrayList<>());
      processed.add(new ArrayList<>());

      final ComObject cobj1 = new ComObject();
      cobj1.put(ComObject.Tag.DB_NAME, dbName);
      if (schemaRetryCount < 2) {
        cobj1.put(ComObject.Tag.SCHEMA_VERSION, client.getCommon().getSchemaVersion());
      }
      cobj1.put(ComObject.Tag.METHOD, "UpdateManager:batchInsertIndexEntryByKeyWithRecord");
      cobj1.put(ComObject.Tag.IS_EXCPLICITE_TRANS, client.isExplicitTrans());
      cobj1.put(ComObject.Tag.IS_COMMITTING, client.isCommitting());
      cobj1.put(ComObject.Tag.TRANSACTION_ID, client.getTransactionId());

      cobj1.putArray(ComObject.Tag.INSERT_OBJECTS, ComObject.Type.OBJECT_TYPE);
      cobjs1.add(cobj1);

      final ComObject cobj2 = new ComObject();
      cobj2.put(ComObject.Tag.DB_NAME, dbName);
      if (schemaRetryCount < 2) {
        cobj2.put(ComObject.Tag.SCHEMA_VERSION, client.getCommon().getSchemaVersion());
      }
      cobj2.put(ComObject.Tag.METHOD, "UpdateManager:batchInsertIndexEntryByKey");
      cobj2.put(ComObject.Tag.IS_EXCPLICITE_TRANS, client.isExplicitTrans());
      cobj2.put(ComObject.Tag.IS_COMMITTING, client.isCommitting());
      cobj2.put(ComObject.Tag.TRANSACTION_ID, client.getTransactionId());
      cobj2.putArray(ComObject.Tag.INSERT_OBJECTS, ComObject.Type.OBJECT_TYPE);
      cobjs2.add(cobj2);

    }
    synchronized (mutex) {
      for (PreparedInsert insert : withRecordPrepared) {
        ComObject obj = serializeInsertKeyWithRecord(insert.insertId, insert.dbName, insert.originalOffset, insert.tableId,
            insert.indexId, insert.tableName, insert.keyInfo, insert.record, insert.ignore);
        cobjs1.get(insert.keyInfo.shard).getArray(ComObject.Tag.INSERT_OBJECTS).getArray().add(obj);
        withRecordProcessed.get(insert.keyInfo.shard).add(insert);
      }
      for (PreparedInsert insert : preparedKeys) {
        ComObject obj = serializeInsertKey(client, client.getCommon(), insert.insertId, insert.dbName, insert.originalOffset, insert.tableId,
            insert.indexId, insert.tableName, insert.keyInfo,
            insert.primaryKeyIndexName, insert.primaryKey, insert.record, insert.keyRecord, insert.ignore);
        cobjs2.get(insert.keyInfo.shard).getArray(ComObject.Tag.INSERT_OBJECTS).getArray().add(obj);
        processed.get(insert.keyInfo.shard).add(insert);
      }
    }
  }

  private int[] fixupRetArray(List<ComObject> allResponses, List<ComObject> keyAllResponses) {
    int maxOffset = 0;
    for (ComObject currRet : allResponses) {
      int currOriginalOffset = currRet.getInt(ComObject.Tag.ORIGINAL_OFFSET);
      maxOffset = Math.max(maxOffset, currOriginalOffset);
    }
    int[] ret = new int[maxOffset + 1];
    for (ComObject currRet : allResponses) {
      int currOriginalOffset = currRet.getInt(ComObject.Tag.ORIGINAL_OFFSET);
      ret[currOriginalOffset] = currRet.getInt(ComObject.Tag.INT_STATUS);
    }
    for (ComObject currRet : keyAllResponses) {
      int currOriginalOffset = currRet.getInt(ComObject.Tag.ORIGINAL_OFFSET);
      if (ret[currOriginalOffset] == BATCH_STATUS_SUCCCESS) {
        ret[currOriginalOffset] = currRet.getInt(ComObject.Tag.INT_STATUS);
      }
    }
    return ret;
  }

  private List<ComObject> handleBatchResponses(Map<Integer, PreparedInsert> withRecordPreparedById, Map<Integer, PreparedInsert> withRecordPreparedMap, String dbName,
                                               List<ComObject> cobjs2, List<ComObject> responses) {
    List<ComObject> allResponses = new ArrayList<>();
    for (ComObject response : responses) {
      ComArray array = response.getArray(ComObject.Tag.BATCH_RESPONSES);
      if (array != null) {
        for (Object obj : array.getArray()) {
          allResponses.add((ComObject) obj);
        }
      }
    }

    for (ComObject currRet : allResponses) {
      int currOriginalOffset = currRet.getInt(ComObject.Tag.ORIGINAL_OFFSET);
      int currId = (int)(long)currRet.getLong(ComObject.Tag.ID);
      int currStatus = currRet.getInt(ComObject.Tag.INT_STATUS);
      PreparedInsert insert = withRecordPreparedMap.get(currOriginalOffset);
      if (insert.originalOffset == currOriginalOffset) {
        if (currStatus == BATCH_STATUS_UNIQUE_CONSTRAINT_VIOLATION) {
          batchUpsertIfNeeded(dbName, currRet, insert);
          removeInsertKey(cobjs2, insert.originalOffset);
          withRecordPreparedById.get(currId).inserted = false;
        }
        else if (currStatus == BATCH_STATUS_FAILED) {
          removeInsertKey(cobjs2, insert.originalOffset);
          withRecordPreparedById.get(currId).inserted = false;
        }
        else {
          withRecordPreparedById.get(currId).inserted = true;
        }
      }
    }
    return allResponses;
  }

  private void batchUpsertIfNeeded(String dbName, ComObject currRet, PreparedInsert insert) {
    if (insert.ignore) {
      try {
        convertInsertToUpdate(dbName, insert.originalStatement);
        currRet.put(ComObject.Tag.INT_STATUS, BATCH_STATUS_SUCCCESS);
      }
      catch (Exception e) {
        logger.error("Error updating record", e);
        currRet.put(ComObject.Tag.INT_STATUS, BATCH_STATUS_FAILED);
      }
    }
    else {
      currRet.put(ComObject.Tag.INT_STATUS, BATCH_STATUS_FAILED);
    }
  }

  private void prepareInserts(List<PreparedInsert> withRecordPrepared, Map<Integer, PreparedInsert> withRecordPreparedMap,
                              List<PreparedInsert> preparedKeys, long nonTransId, AtomicInteger originalOffset) throws UnsupportedEncodingException, SQLException {
    int insertId = 0;
    for (InsertRequest request : getBatch().get()) {
      List<PreparedInsert> inserts = prepareInsert(request, new ArrayList<>(), new AtomicLong(-1L), nonTransId,
          originalOffset);
      for (PreparedInsert insert : inserts) {
        if (client.isExplicitTrans() && insert.ignore) {
          throw new DatabaseException("'ignore' is not supported for batch operations in an explicit transaction");
        }
        insert.insertId = insertId++;
        if (insert.keyInfo.indexSchema.isPrimaryKey()) {
          withRecordPrepared.add(insert);
          withRecordPreparedMap.put(insert.originalOffset, insert);
        }
        else {
          preparedKeys.add(insert);
        }
      }
    }
  }

  private int handleExceptionForBatchInsert(Object mutex, List<PreparedInsert> withRecordPrepared,
                                            List<PreparedInsert> preparedKeys, int schemaRetryCount, Exception e) {
    if (e.getCause() instanceof SchemaOutOfSyncException) {
      synchronized (mutex) {
        fixupKeysWithRecord(withRecordPrepared);

        fixupKeys(preparedKeys);
      }
      schemaRetryCount++;
      return schemaRetryCount;
    }
    throw new DatabaseException(e);
  }

  private void fixupKeys(List<PreparedInsert> preparedKeys) {
    for (PreparedInsert insert : preparedKeys) {
      List<KeyInfo> keys = getKeys(client.getCommon().getTables(insert.dbName).get(insert.tableSchema.getName()),
          insert.columnNames, insert.values, insert.id);
      for (KeyInfo key : keys) {
        if (key.indexSchema.getName().equals(insert.indexName)) {
          insert.keyInfo.shard = key.shard;
          break;
        }
      }
    }
  }

  private void fixupKeysWithRecord(List<PreparedInsert> withRecordPrepared) {
    fixupKeys(withRecordPrepared);
  }

  private List<ComObject> batchInsertIndexEntryByKey(List<PreparedInsert> preparedKeys, List<List<PreparedInsert>> processed,
                                                     List<ByteArrayOutputStream> bytesOut, List<ComObject> cobjs2) throws ExecutionException, InterruptedException {
    List<Future<ComObject>> futures = new ArrayList<>();
    for (int i = 0; i < bytesOut.size(); i++) {
      final int offset = i;
      futures.add(client.getExecutor().submit((Callable) () -> {
        if (cobjs2.get(offset).getArray(ComObject.Tag.INSERT_OBJECTS).getArray().isEmpty()) {
          return new ComObject();
        }
        byte[] ret = client.send("UpdateManager:batchInsertIndexEntryByKey", offset, 0, cobjs2.get(offset),
            DatabaseClient.Replica.DEF);

        for (PreparedInsert insert : processed.get(offset)) {
          preparedKeys.remove(insert);
        }

        return new ComObject(ret);
      }));
    }
    List<ComObject> responses = new ArrayList<>();
    for (Future<ComObject> future : futures) {
      responses.add(future.get());
    }
    return responses;
  }

  private List<ComObject> batchInsertIndexEntryByKeyWithRecord(AtomicInteger totalCount,
                                                               List<ByteArrayOutputStream> bytesOut,
                                                               List<ComObject> cobjs1) throws InterruptedException, java.util.concurrent.ExecutionException {
    List<Future<ComObject>> futures = new ArrayList<>();
    for (int i = 0; i < bytesOut.size(); i++) {
      final int offset = i;
      futures.add(client.getExecutor().submit(() -> {
        if (cobjs1.get(offset).getArray(ComObject.Tag.INSERT_OBJECTS).getArray().isEmpty()) {
          return new ComObject();
        }
        byte[] ret = client.send("UpdateManager:batchInsertIndexEntryByKeyWithRecord", offset, 0,
            cobjs1.get(offset), DatabaseClient.Replica.DEF);
        if (ret == null) {
          throw new FailedToInsertException("No response for key insert");
        }
        ComObject retObj = new ComObject(ret);
        if (retObj.getInt(ComObject.Tag.COUNT) == null) {
          throw new DatabaseException("count not returned: obj=" + retObj.toString());
        }
        int retVal = retObj.getInt(ComObject.Tag.COUNT);
        totalCount.addAndGet(retVal);
        return retObj;
      }));
    }
    List<ComObject> responses = new ArrayList<>();
    for (Future<ComObject> future : futures) {
      responses.add(future.get());
    }
    return responses;
  }

  private class GetValuesFromParms {
    private final ParameterHandler parms;
    private final Insert insert;
    private List<Object> values;
    private List<String> columnNames;

    GetValuesFromParms(ParameterHandler parms, Insert insert) {
      this.parms = parms;
      this.insert = insert;
    }

    public List<Object> getValues() {
      return values;
    }

    public List<String> getColumnNames() {
      return columnNames;
    }

    public GetValuesFromParms invoke() {
      values = new ArrayList<>();
      columnNames = new ArrayList<>();

      List srcColumns = insert.getColumns();
      ExpressionList items = (ExpressionList) insert.getItemsList();
      List srcExpressions = items == null ? null : items.getExpressions();
      int parmOffset = 1;
      for (int i = 0; i < srcColumns.size(); i++) {
        Column column = (Column) srcColumns.get(i);
        columnNames.add(toLower(column.getColumnName()));
        if (srcExpressions != null) {
          Expression expression = (Expression) srcExpressions.get(i);
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
      return this;
    }
  }
}
