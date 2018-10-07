package com.sonicbase.server;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.*;
import com.sonicbase.index.Indices;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.query.impl.CreateTableStatementImpl;
import com.sonicbase.schema.*;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static com.sonicbase.common.DatabaseCommon.sortSchemaFiles;

@SuppressWarnings({"squid:S1172", "squid:S2629", "squid:S1168", "squid:S3516", "squid:S00107"})
// all methods called from method invoker must have cobj and replayed command parms
// info is always enabled, don't need to conditionally call
// I prefer to return null instead of an empty array
// all methods called from method invoker must return a ComObject even if they are all null
// I don't know a good way to reduce the parameter count
public class SchemaManager {

  private static final String SLAVE_STR = "slave";
  private static final String MASTER_STR = "master";
  private static final Logger logger = LoggerFactory.getLogger(SchemaManager.class);

  private final com.sonicbase.server.DatabaseServer server;

  SchemaManager(com.sonicbase.server.DatabaseServer databaseServer) {
    this.server = databaseServer;
  }

  public static class AutoIncrementValue {
    private final Object mutex = new Object();
    private Object currValue = null;
    final DataType.Type dataType;

    public AutoIncrementValue(DataType.Type type) {
      this.dataType = type;
      setInitialValue();
    }

    void setInitialValue() {
      currValue = dataType.getInitialValue();
    }

    public Object increment() {
      synchronized (mutex) {
        currValue = dataType.getIncrementer().increment(currValue);
        return currValue;
      }
    }
  }

  private List<String> createIndex(String dbName, String table, String indexName, boolean isPrimary, boolean isUnique, String[] fields) {
    List<String> createdIndices = new ArrayList<>();

    TableSchema tableSchema = server.getCommon().getTableSchema(dbName, table.toLowerCase(), server.getDataDir());
    if (tableSchema == null) {
      throw new DatabaseException("Undefined table: name=" + table);
    }
    for (String field : fields) {
      Integer offset = tableSchema.getFieldOffset(field);
      if (offset == null) {
        throw new DatabaseException("Invalid field for index: indexName=" + indexName + ", field=" + field);
      }
    }

    if (tableSchema.getIndices().get(indexName) != null) {
      throw new DatabaseException("Index already exists: tableName=" + tableSchema.getName() + ", indexName=" + indexName);
    }
    createdIndices.add(indexName);

    TableSchema.Partition[] partitions = new TableSchema.Partition[server.getShardCount()];
    for (int j = 0; j < partitions.length; j++) {
      partitions[j] = new TableSchema.Partition();
      partitions[j].setShardOwning(j);
      if (j == 0) {
        partitions[j].setUnboundUpper(true);
      }
    }

    Map<Integer, IndexSchema> indicesById = tableSchema.getIndexesById();
    int highIndexId = 0;
    for (int id : indicesById.keySet()) {
      highIndexId = Math.max(highIndexId, id);
    }
    highIndexId++;

    IndexSchema indexSchema = tableSchema.addIndex(indexName, isPrimary, isUnique, fields, partitions, highIndexId);

    SnapshotManager snapshotManager = server.getSnapshotManager();
    snapshotManager.saveIndexSchema(dbName, server.getCommon().getSchemaVersion() + 1, tableSchema, indexSchema);

    server.getCommon().updateTable(dbName, tableSchema);

    doCreateIndex(dbName, tableSchema, indexName, fields);

    return createdIndices;
  }

  void addAllIndices(String dbName) {
    for (TableSchema table : server.getCommon().getTables(dbName).values()) {
      for (IndexSchema index : table.getIndices().values()) {
        server.getIndices(dbName).addIndex(table, index.getName(), index.getComparators());
      }
    }
  }

  void doCreateIndex(
      String dbName, TableSchema tableSchema, String indexName, String[] currFields) {
    Comparator[] comparators = tableSchema.getComparators(currFields);

    server.getIndices(dbName).addIndex(tableSchema, indexName, comparators);
  }

  @SchemaWriteLock
  public ComObject createDatabase(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.DB_NAME);
    String masterSlave = cobj.getString(ComObject.Tag.MASTER_SLAVE);
    dbName = dbName.toLowerCase();

    if (replayedCommand && null != server.getCommon().getSchema(dbName)) {
      return null;
    }

    synchronized (this) {
      if (server.getCommon().getDatabases().containsKey(dbName)) {
        throw new DatabaseException("Database already exists: name=" + dbName);
      }
      logger.info("Create database: shard={}, replica={}, name={}", server.getShard(), server.getReplica(), dbName);
      if (server.isDurable()) {
        File dir = null;
        if (com.sonicbase.server.DatabaseServer.USE_SNAPSHOT_MGR_OLD) {
          dir = new File(server.getDataDir(), "snapshot" + File.separator + server.getShard() + File.separator +
              server.getReplica() + File.separator + dbName);
        }
        else {
          dir = new File(server.getDataDir(), "delta" + File.separator + server.getShard() + File.separator +
              server.getReplica() + File.separator + dbName);
        }
        if (!dir.exists() && !dir.mkdirs()) {
          throw new DatabaseException("Error creating database directory: dir=" + dir.getAbsolutePath());
        }
      }

      server.getIndices().put(dbName, new Indices());
      server.getCommon().addDatabase(dbName);
      server.getCommon().saveSchema(server.getDataDir());
    }
    if (masterSlave.equals(MASTER_STR)) {
      for (int i = 0; i < server.getShardCount(); i++) {
        cobj.put(ComObject.Tag.SLAVE, true);
        cobj.put(ComObject.Tag.MASTER_SLAVE, SLAVE_STR);
        server.getDatabaseClient().send("SchemaManager:createDatabaseSlave", i, 0, cobj, DatabaseClient.Replica.DEF);
      }
      server.pushSchema();
    }
    return null;
  }

  @SchemaWriteLock
  public ComObject createDatabaseSlave(ComObject cobj, boolean replayedCommand) {

    String dbName = cobj.getString(ComObject.Tag.DB_NAME);
    String masterSlave = cobj.getString(ComObject.Tag.MASTER_SLAVE);
    dbName = dbName.toLowerCase();

    if (server.getShard() == 0 &&
        server.getCommon().getServersConfig().getShards()[0].getMasterReplica() == server.getReplica() &&
        masterSlave.equals(SLAVE_STR)) {
      return null;
    }

    synchronized (this) {
      if (server.getCommon().getDatabases().get(dbName) == null) {
        logger.info("Create database: shard={}, replica={}, name={}", server.getShard(), server.getReplica(), dbName);
        if (server.isDurable()) {
          File dir;
          if (DatabaseServer.USE_SNAPSHOT_MGR_OLD) {
            dir = new File(server.getDataDir(), "snapshot" + File.separator + server.getShard() + File.separator +
                server.getReplica() + File.separator + dbName);
          }
          else {
            dir = new File(server.getDataDir(), "delta" + File.separator + server.getShard() + File.separator +
                server.getReplica() + File.separator + dbName);
          }
          if (!dir.exists() && !dir.mkdirs()) {
            throw new DatabaseException("Error creating database directory: dir=" + dir.getAbsolutePath());
          }
        }

        server.getIndices().put(dbName, new Indices());
        server.getCommon().addDatabase(dbName);
        server.getCommon().saveSchema(server.getDataDir());
      }
    }
    return null;
  }

  @SchemaWriteLock
  public ComObject dropDatabase(ComObject cobj, boolean replayedCommand) {
    String masterSlave = cobj.getString(ComObject.Tag.MASTER_SLAVE);
    if (server.getShard() == 0 &&
        server.getCommon().getServersConfig().getShards()[0].getMasterReplica() == server.getReplica() &&
        masterSlave.equals(SLAVE_STR)) {
      return null;
    }
    try {
      short serializationVersionNumber = cobj.getShort(ComObject.Tag.SERIALIZATION_VERSION);
      String dbName = cobj.getString(ComObject.Tag.DB_NAME);

      for (String tableName : server.getCommon().getTables(dbName).keySet()) {
        //table truncate is initiated by client before dropping the database
        doDropTable(dbName, tableName);
      }

      server.getCommon().dropDatabase(dbName);
      SnapshotManager snapshotManager = server.getSnapshotManager();
      snapshotManager.deleteDbSchema(dbName);
      snapshotManager.deleteDbFiles(dbName);

      if (masterSlave.equals(MASTER_STR)) {
        Random rand = new Random(System.currentTimeMillis());
        for (int i = 0; i < server.getShardCount(); i++) {
          cobj.put(ComObject.Tag.MASTER_SLAVE, SLAVE_STR);
          server.getDatabaseClient().send(null, i, rand.nextLong(), cobj, DatabaseClient.Replica.DEF);
        }
        server.pushSchema();
      }

      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.SCHEMA_BYTES, server.getCommon().serializeSchema(serializationVersionNumber));
      return retObj;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  @SchemaWriteLock
  public ComObject dropTable(ComObject cobj, boolean replayedCommand) {
    String masterSlave = cobj.getString(ComObject.Tag.MASTER_SLAVE);
    if (server.getShard() == 0 &&
        server.getCommon().getServersConfig().getShards()[0].getMasterReplica() == server.getReplica() &&
        masterSlave.equals(SLAVE_STR)) {
      return null;
    }
    try {
      short serializationVersionNumber = cobj.getShort(ComObject.Tag.SERIALIZATION_VERSION);
      String dbName = cobj.getString(ComObject.Tag.DB_NAME);
      String tableName = cobj.getString(ComObject.Tag.TABLE_NAME);

      doDropTable(dbName, tableName);

      if (masterSlave.equals(MASTER_STR)) {
        Random rand = new Random(System.currentTimeMillis());
        for (int i = 0; i < server.getShardCount(); i++) {
          cobj.put(ComObject.Tag.MASTER_SLAVE, SLAVE_STR);
          server.getDatabaseClient().send(null, i, rand.nextLong(), cobj, DatabaseClient.Replica.DEF);
        }
        server.pushSchema();
      }

      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.SCHEMA_BYTES, server.getCommon().serializeSchema(serializationVersionNumber));
      return retObj;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private void doDropTable(String dbName, String tableName) {
    server.getCommon().getSchemaWriteLock(dbName).lock();
    try {
      server.getCommon().dropTable(dbName, tableName);
      //table truncate is initiated by client before dropping the table
      server.removeIndices(dbName, tableName);
      SnapshotManager snapshotManager = server.getSnapshotManager();
      snapshotManager.deleteTableSchema(dbName, server.getCommon().getSchemaVersion(), tableName);
      snapshotManager.deleteTableFiles(dbName, tableName);
    }
    finally {
      server.getCommon().getSchemaWriteLock(dbName).unlock();
    }
  }

  @SchemaWriteLock
  public ComObject createTableSlave(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.DB_NAME);
    String masterSlave = cobj.getString(ComObject.Tag.MASTER_SLAVE);

    if (server.getShard() == 0 &&
        server.getCommon().getServersConfig().getShards()[0].getMasterReplica() == server.getReplica() &&
        masterSlave.equals(SLAVE_STR)) {
      return null;
    }

    server.getCommon().getSchemaWriteLock(dbName).lock();
    try {
      byte[] bytes = cobj.getByteArray(ComObject.Tag.SCHEMA_BYTES);
      DatabaseCommon tmpCommon = new DatabaseCommon();
      tmpCommon.deserializeSchema(bytes);

      String tableName = cobj.getString(ComObject.Tag.TABLE_NAME);
      TableSchema tableSchema = tmpCommon.getTableSchema(dbName, tableName, server.getDataDir());

      synchronized (this) {
        if (server.getCommon().getTableSchema(dbName, tableName, server.getDataDir()) == null) {
          server.getCommon().getTables(dbName).put(tableName, tableSchema);
          server.getCommon().getTablesById(dbName).put(tableSchema.getTableId(), tableSchema);
        }
        SnapshotManager snapshotManager = server.getSnapshotManager();
        snapshotManager.saveTableSchema(dbName, tmpCommon.getSchemaVersion(), tableName, tableSchema);

        for (IndexSchema indexSchema : tableSchema.getIndices().values()) {
          if (indexSchema.isPrimaryKey()) {
            snapshotManager.saveIndexSchema(dbName, tmpCommon.getSchemaVersion(), tableSchema, indexSchema);
          }
        }

        Map<String, IndexSchema> indices = tableSchema.getIndices();
        for (Map.Entry<String, IndexSchema> index : indices.entrySet()) {
          doCreateIndex(dbName, tableSchema, index.getKey(), index.getValue().getFields());
        }
        server.getCommon().setSchemaVersion(tmpCommon.getSchemaVersion());
        server.getCommon().saveSchema(server.getDataDir());
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
    finally {
      server.getCommon().getSchemaWriteLock(dbName).unlock();
    }
    return null;
  }

  @SchemaWriteLock
  public ComObject createTable(ComObject cobj, boolean replayedCommand) {
    try {
      String masterSlave = cobj.getString(ComObject.Tag.MASTER_SLAVE);
      if (server.getShard() == 0 &&
          server.getCommon().getServersConfig().getShards()[0].getMasterReplica() == server.getReplica() &&
          masterSlave.equals(SLAVE_STR)) {
        return null;
      }
      short serializationVersionNumber = cobj.getShort(ComObject.Tag.SERIALIZATION_VERSION);
      String dbName = cobj.getString(ComObject.Tag.DB_NAME);

      String tableName = null;
      server.getCommon().getSchemaWriteLock(dbName).lock();
      try {
        CreateTableStatementImpl createTableStatement = new CreateTableStatementImpl();
        createTableStatement.deserialize(cobj.getByteArray(ComObject.Tag.CREATE_TABLE_STATEMENT));

        logger.info("Create table: shard={}, replica={}, name={}", server.getShard(), server.getReplica(),
            createTableStatement.getTablename());

        synchronized (this) {
          TableSchema schema = new TableSchema();
          tableName = createTableStatement.getTablename();
          if (server.getCommon().getTables(dbName).containsKey(tableName) && server.getShard() == 0 &&
                server.getCommon().getServersConfig().getShards()[0].getMasterReplica() == server.getReplica()) {
            throw new DatabaseException("Table already exists: name=" + tableName);
          }
          else {
            if (replayedCommand) {
              logger.info("replayedCommand: table={}, tableCount={}", tableName, server.getCommon().getTables(dbName).size());
              if (server.getCommon().getTables(dbName).containsKey(tableName.toLowerCase())) {
                ComObject retObj = new ComObject();
                retObj.put(ComObject.Tag.SCHEMA_BYTES, server.getCommon().serializeSchema(serializationVersionNumber));

                return retObj;
              }
            }

            List<String> primaryKey = createTableStatement.getPrimaryKey();
            List<FieldSchema> fields = createTableStatement.getFields();
            List<FieldSchema> actualFields = new ArrayList<>();
            FieldSchema idField = new FieldSchema();
            idField.setAutoIncrement(true);
            idField.setName("_sonicbase_id");
            idField.setType(DataType.Type.BIGINT);
            actualFields.add(idField);
            actualFields.addAll(fields);
            schema.setFields(actualFields);
            schema.setName(tableName.toLowerCase());
            schema.setPrimaryKey(primaryKey);
            TableSchema existingSchema = server.getCommon().getTableSchema(dbName, tableName, server.getDataDir());
            if (existingSchema != null) {
              schema.setIndices(existingSchema.getIndices());
            }

            Map<Integer, TableSchema> tables = server.getCommon().getTablesById(dbName);
            int highTableId = 0;
            for (int id : tables.keySet()) {
              highTableId = Math.max(highTableId, id);
            }
            highTableId++;
            schema.setTableId(highTableId);

            server.getCommon().addTable(dbName, schema);

            SnapshotManager snapshotManager = server.getSnapshotManager();
            snapshotManager.saveTableSchema(dbName, server.getCommon().getSchemaVersion() + 1, tableName, schema);

            String[] primaryKeyFields = primaryKey.toArray(new String[primaryKey.size()]);
            createIndex(dbName, tableName.toLowerCase(), "_primarykey", true, true, primaryKeyFields);

            server.getCommon().saveSchema(server.getDataDir());
          }
        }
      }
      finally {
        server.getCommon().getSchemaWriteLock(dbName).unlock();
      }

      createTableSlave(replayedCommand, masterSlave, dbName, tableName);

      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.SCHEMA_BYTES, server.getCommon().serializeSchema(serializationVersionNumber));
      return retObj;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  private void createTableSlave(boolean replayedCommand, String masterSlave, String dbName, String tableName) throws IOException {
    if (!replayedCommand && masterSlave.equals(MASTER_STR)) {
      Random rand = new Random(System.currentTimeMillis());
      for (int i = 0; i < server.getShardCount(); i++) {
        ComObject slaveObj = new ComObject();

        slaveObj.put(ComObject.Tag.SCHEMA_BYTES, server.getCommon().serializeSchema(DatabaseClient.SERIALIZATION_VERSION));
        slaveObj.put(ComObject.Tag.TABLE_NAME, tableName);
        slaveObj.put(ComObject.Tag.DB_NAME, dbName);
        slaveObj.put(ComObject.Tag.SCHEMA_VERSION, server.getCommon().getSchemaVersion());
        slaveObj.put(ComObject.Tag.METHOD, "SchemaManager:createTableSlave");
        slaveObj.put(ComObject.Tag.MASTER_SLAVE, SLAVE_STR);
        server.getDatabaseClient().send(null, i, rand.nextLong(), slaveObj, DatabaseClient.Replica.DEF);
      }
    }
  }


  @SchemaWriteLock
  public ComObject dropColumnSlave(ComObject cobj, boolean replayedCommand) {
    return addOrDropColumnSlave(cobj);
  }

  @SchemaWriteLock
  public ComObject dropColumn(ComObject cobj, boolean replayedCommand) {

    try {
      short serializationVersionNumber = cobj.getShort(ComObject.Tag.SERIALIZATION_VERSION);
      String dbName = cobj.getString(ComObject.Tag.DB_NAME);
      String tableName = cobj.getString(ComObject.Tag.TABLE_NAME).toLowerCase();
      String columnName = cobj.getString(ComObject.Tag.COLUMN_NAME).toLowerCase();

      synchronized (this) {
        TableSchema tableSchema = server.getCommon().getTableSchema(dbName, tableName, server.getDataDir());
        tableSchema.saveFields(server.getCommon().getSchemaVersion());

        for (IndexSchema indexSchema : tableSchema.getIndices().values()) {
          for (String fieldName : indexSchema.getFields()) {
            if (columnName.equals(fieldName)) {
              throw new DatabaseException("Field being used by index, cannot drop: field=" + columnName + ", index=" +
                  indexSchema.getName());
            }
          }
        }

        for (FieldSchema fieldSchema : tableSchema.getFields()) {
          if (fieldSchema.getName().equals(columnName)) {
            tableSchema.getFields().remove(fieldSchema);
            tableSchema.markChangesComplete();
            break;
          }
        }

        SnapshotManager snapshotManager = server.getSnapshotManager();
        snapshotManager.saveTableSchema(dbName, server.getCommon().getSchemaVersion() + 1, tableName, tableSchema);

        server.getCommon().saveSchema(server.getDataDir());
      }

      if (!replayedCommand) {
        Random rand = new Random(System.currentTimeMillis());
        for (int i = 0; i < server.getShardCount(); i++) {
          ComObject slaveObj = new ComObject();

          slaveObj.put(ComObject.Tag.SCHEMA_BYTES, server.getCommon().serializeSchema(DatabaseClient.SERIALIZATION_VERSION));
          slaveObj.put(ComObject.Tag.TABLE_NAME, tableName);
          slaveObj.put(ComObject.Tag.DB_NAME, dbName);
          slaveObj.put(ComObject.Tag.SCHEMA_VERSION, server.getCommon().getSchemaVersion());
          slaveObj.put(ComObject.Tag.METHOD, "SchemaManager:dropColumnSlave");
          server.getDatabaseClient().send(null, i, rand.nextLong(), slaveObj, DatabaseClient.Replica.DEF);
        }
      }

      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.SCHEMA_BYTES, server.getCommon().serializeSchema(serializationVersionNumber));

      return retObj;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  @SchemaWriteLock
  public ComObject addColumnSlave(ComObject cobj, boolean replayedCommand) {
    return addOrDropColumnSlave(cobj);
  }

  private ComObject addOrDropColumnSlave(ComObject cobj) {
    if (server.getShard() == 0 &&
        server.getCommon().getServersConfig().getShards()[0].getMasterReplica() == server.getReplica()) {
      return null;
    }

    String dbName = cobj.getString(ComObject.Tag.DB_NAME);
    server.getCommon().getSchemaWriteLock(dbName).lock();
    try {
      byte[] bytes = cobj.getByteArray(ComObject.Tag.SCHEMA_BYTES);
      DatabaseCommon tmpCommon = new DatabaseCommon();
      tmpCommon.deserializeSchema(bytes);

      String tableName = cobj.getString(ComObject.Tag.TABLE_NAME);
      TableSchema tableSchema = tmpCommon.getTableSchema(dbName, tableName, server.getDataDir());

      synchronized (this) {
        SnapshotManager snapshotManager = server.getSnapshotManager();
        server.getCommon().getTables(dbName).put(tableName, tableSchema);
        server.getCommon().getTablesById(dbName).put(tableSchema.getTableId(), tableSchema);
        snapshotManager.saveTableSchema(dbName, tmpCommon.getSchemaVersion(), tableName, tableSchema);

        server.getCommon().setSchemaVersion(tmpCommon.getSchemaVersion());
        server.getCommon().saveSchema(server.getDataDir());
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
    finally {
      server.getCommon().getSchemaWriteLock(dbName).unlock();
    }
    return null;
  }

  @SchemaWriteLock
  public ComObject addColumn(ComObject cobj, boolean replayedCommand) {

    try {
      short serializationVersionNumber = cobj.getShort(ComObject.Tag.SERIALIZATION_VERSION);
      String dbName = cobj.getString(ComObject.Tag.DB_NAME);
      String tableName = cobj.getString(ComObject.Tag.TABLE_NAME).toLowerCase();
      String columnName = cobj.getString(ComObject.Tag.COLUMN_NAME).toLowerCase();
      String dataType = cobj.getString(ComObject.Tag.DATA_TYPE);

      synchronized (this) {
        TableSchema tableSchema = server.getCommon().getTableSchema(dbName, tableName, server.getDataDir());
        tableSchema.saveFields(server.getCommon().getSchemaVersion());

        FieldSchema fieldSchema = new FieldSchema();
        fieldSchema.setType(DataType.Type.valueOf(dataType));
        fieldSchema.setName(columnName);
        tableSchema.addField(fieldSchema);
        tableSchema.markChangesComplete();

        SnapshotManager snapshotManager = server.getSnapshotManager();
        snapshotManager.saveTableSchema(dbName, server.getCommon().getSchemaVersion() + 1, tableName, tableSchema);

        server.getCommon().saveSchema(server.getDataDir());
      }

      if (!replayedCommand) {
        Random rand = new Random(System.currentTimeMillis());
        for (int i = 0; i < server.getShardCount(); i++) {
          ComObject slaveObj = new ComObject();

          slaveObj.put(ComObject.Tag.SCHEMA_BYTES, server.getCommon().serializeSchema(DatabaseClient.SERIALIZATION_VERSION));
          slaveObj.put(ComObject.Tag.TABLE_NAME, tableName);
          slaveObj.put(ComObject.Tag.DB_NAME, dbName);
          slaveObj.put(ComObject.Tag.SCHEMA_VERSION, server.getCommon().getSchemaVersion());
          slaveObj.put(ComObject.Tag.METHOD, "SchemaManager:addColumnSlave");
          server.getDatabaseClient().send(null, i, rand.nextLong(), slaveObj, DatabaseClient.Replica.DEF);
        }
      }

      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.SCHEMA_BYTES, server.getCommon().serializeSchema(serializationVersionNumber));

      return retObj;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  @SchemaWriteLock
  public ComObject createIndexSlave(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.DB_NAME);
    String masterSlave = cobj.getString(ComObject.Tag.MASTER_SLAVE);

    if (server.getShard() == 0 &&
        server.getCommon().getServersConfig().getShards()[0].getMasterReplica() == server.getReplica() &&
        masterSlave.equals(SLAVE_STR)) {
      return null;
    }

    synchronized (this) {
      DatabaseCommon tmpCommon = new DatabaseCommon();
      tmpCommon.deserializeSchema(cobj.getByteArray(ComObject.Tag.SCHEMA_BYTES));
      String tableName = cobj.getString(ComObject.Tag.TABLE_NAME);
      TableSchema tableSchema = tmpCommon.getTableSchema(dbName, tableName, server.getDataDir());
      ComArray array = cobj.getArray(ComObject.Tag.INDICES);
      for (int i = 0; i < array.getArray().size(); i++) {
        String indexName = (String) array.getArray().get(i);
        IndexSchema indexSchema = tableSchema.getIndices().get(indexName);

        if (!server.getIndices(dbName).getIndices().containsKey(indexName)) {
          doCreateIndex(dbName, tableSchema, indexName, indexSchema.getFields());
        }

        server.getCommon().getTables(dbName).get(tableName).getIndices().put(indexName, indexSchema);
        server.getCommon().getTables(dbName).get(tableName).getIndexesById().put(indexSchema.getIndexId(), indexSchema);

        SnapshotManager snapshotManager = server.getSnapshotManager();
        snapshotManager.saveIndexSchema(dbName, tmpCommon.getSchemaVersion(), tableSchema, indexSchema);
      }

      server.getCommon().saveSchema(server.getDataDir());

      return null;
    }
  }

  @SchemaWriteLock
  public ComObject createIndex(ComObject cobj, boolean replayedCommand) {
    try {
      int schemaVersion = cobj.getInt(ComObject.Tag.SCHEMA_VERSION);
      String dbName = cobj.getString(ComObject.Tag.DB_NAME);
      if (schemaVersion < server.getSchemaVersion() && !replayedCommand) {
        throw new SchemaOutOfSyncException("currVer:" + server.getCommon().getSchemaVersion() + ":");
      }
      AtomicReference<String> table = new AtomicReference<>();
      String masterSlave = cobj.getString(ComObject.Tag.MASTER_SLAVE);
      table.set(cobj.getString(ComObject.Tag.TABLE_NAME));
      String indexName = cobj.getString(ComObject.Tag.INDEX_NAME);
      boolean isUnique = cobj.getBoolean(ComObject.Tag.IS_UNIQUE);
      String fieldsStr = cobj.getString(ComObject.Tag.FIELDS_STR);
      String[] fields = fieldsStr.split(",");

      List<String> createdIndices = null;

      server.getCommon().getSchemaWriteLock(dbName).lock();
      try {

        if (replayedCommand) {
          logger.info("replayedCommand: createIndex, table={}, index={}", table.get(), indexName);
          for (String field : fields) {
            Integer offset = server.getCommon().getTableSchema(dbName, table.get(), server.getDataDir()).getFieldOffset(field);
            if (offset == null) {
              throw new DatabaseException("Invalid field for index: indexName=" + indexName + ", field=" + field);
            }
          }

          if (server.getCommon().getTableSchema(dbName, table.get(),
              server.getDataDir()).getIndices().containsKey(indexName.toLowerCase())) {
            ComObject retObj = new ComObject();
            retObj.put(ComObject.Tag.SCHEMA_BYTES, server.getCommon().serializeSchema(
                cobj.getShort(ComObject.Tag.SERIALIZATION_VERSION)));
            return retObj;
          }
        }

        synchronized (this) {
          createdIndices = createIndex(dbName, table.get(), indexName, false, isUnique, fields);
          server.getCommon().saveSchema(server.getDataDir());
        }
      }
      finally {
        server.getCommon().getSchemaWriteLock(dbName).unlock();
      }

      createIndexSlave(replayedCommand, dbName, table, masterSlave, createdIndices);

      populateIndexIfNeeded(replayedCommand, dbName, table, createdIndices);

      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.SCHEMA_BYTES, server.getCommon().serializeSchema(cobj.getShort(ComObject.Tag.SERIALIZATION_VERSION)));
      return retObj;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  private void populateIndexIfNeeded(boolean replayedCommand, String dbName, AtomicReference<String> table,
                                     List<String> createdIndices) {
    if (!replayedCommand && createdIndices != null) {
      String primaryIndexName = null;
      TableSchema tableSchema = server.getCommon().getTableSchema(dbName, table.get(), server.getDataDir());
      for (Map.Entry<String, IndexSchema> entry : tableSchema.getIndices().entrySet()) {
        if (entry.getValue().isPrimaryKey()) {
          primaryIndexName = entry.getKey();
        }
      }
      long totalSize = 0;
      if (primaryIndexName != null) {
        totalSize = getIndexTotalSize(dbName, table, primaryIndexName, totalSize);
      }

      doPopulateIndex(dbName, table, createdIndices, totalSize);
    }
  }

  private void doPopulateIndex(String dbName, AtomicReference<String> table, List<String> createdIndices, long totalSize) {
    if (totalSize != 0) {
      for (String currIndexName : createdIndices) {
        ComObject cobj = new ComObject();
        cobj.put(ComObject.Tag.DB_NAME, dbName);
        cobj.put(ComObject.Tag.SCHEMA_VERSION, server.getCommon().getSchemaVersion());
        cobj.put(ComObject.Tag.TABLE_NAME, table.get());
        cobj.put(ComObject.Tag.INDEX_NAME, currIndexName);
        cobj.put(ComObject.Tag.METHOD, "UpdateManager:populateIndex");
        for (int i = 0; i < server.getShardCount(); i++) {
          server.getDatabaseClient().send(null, i, 0, cobj, DatabaseClient.Replica.DEF);
        }
      }
    }
  }

  private long getIndexTotalSize(String dbName, AtomicReference<String> table, String primaryIndexName, long totalSize) {
    for (int shard = 0; shard < server.getShardCount(); shard++) {
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.DB_NAME, dbName);
      cobj.put(ComObject.Tag.SCHEMA_VERSION, server.getClient().getCommon().getSchemaVersion());
      byte[] response = server.getClient().send("PartitionManager:getIndexCounts", shard, 0, cobj,
          DatabaseClient.Replica.MASTER);
      ComObject retObj = new ComObject(response);
      ComArray tables = retObj.getArray(ComObject.Tag.TABLES);
      if (tables != null) {
        for (int i = 0; i < tables.getArray().size(); i++) {
          ComObject tableObj = (ComObject) tables.getArray().get(i);
          totalSize = getIndexTotalSizeForTable(table, primaryIndexName, totalSize, tableObj);
        }

      }
    }
    return totalSize;
  }

  private long getIndexTotalSizeForTable(AtomicReference<String> table, String primaryIndexName, long totalSize,
                                         ComObject tableObj) {
    String tableName = tableObj.getString(ComObject.Tag.TABLE_NAME);
    if (tableName.equals(table.get())) {
      ComArray indices = tableObj.getArray(ComObject.Tag.INDICES);
      if (indices != null) {
        for (int j = 0; j < indices.getArray().size(); j++) {
          ComObject indexObj = (ComObject) indices.getArray().get(j);
          String foundIndexName = indexObj.getString(ComObject.Tag.INDEX_NAME);
          if (primaryIndexName.equals(foundIndexName)) {
            long size = indexObj.getLong(ComObject.Tag.SIZE);
            totalSize += size;
          }
        }
      }
    }
    return totalSize;
  }

  private void createIndexSlave(boolean replayedCommand, String dbName, AtomicReference<String> table, String masterSlave,
                                List<String> createdIndices) throws IOException {
    if (!replayedCommand && masterSlave.equals(MASTER_STR)) {
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.DB_NAME, dbName);
      cobj.put(ComObject.Tag.SCHEMA_VERSION, server.getCommon().getSchemaVersion());
      cobj.put(ComObject.Tag.METHOD, "SchemaManager:createIndexSlave");
      cobj.put(ComObject.Tag.SCHEMA_BYTES, server.getCommon().serializeSchema(DatabaseClient.SERIALIZATION_VERSION));
      cobj.put(ComObject.Tag.TABLE_NAME, table.get());
      ComArray array = cobj.putArray(ComObject.Tag.INDICES, ComObject.Type.STRING_TYPE);
      for (String currIndexName : createdIndices) {
        array.add(currIndexName);
      }
      cobj.put(ComObject.Tag.MASTER_SLAVE, SLAVE_STR);

      for (int i = 0; i < server.getShardCount(); i++) {
        server.getDatabaseClient().send(null, i, 0, cobj, DatabaseClient.Replica.DEF);
      }
    }
  }

  @SchemaWriteLock
  public ComObject dropIndexSlave(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.DB_NAME);
    String masterSlave = cobj.getString(ComObject.Tag.MASTER_SLAVE);

    if (server.getShard() == 0 &&
        server.getCommon().getServersConfig().getShards()[0].getMasterReplica() == server.getReplica() &&
        masterSlave.equals(SLAVE_STR)) {
      return null;
    }

    String tableName = cobj.getString(ComObject.Tag.TABLE_NAME);
    ComArray array = cobj.getArray(ComObject.Tag.INDICES);
    for (int i = 0; i < array.getArray().size(); i++) {
      String indexName = (String) array.getArray().get(i);
      server.getUpdateManager().truncateAnyIndex(dbName, tableName, indexName);
      server.removeIndex(dbName, tableName, indexName);
      SnapshotManager snapshotManager = server.getSnapshotManager();
      snapshotManager.deleteIndexFiles(dbName, tableName, indexName);
      snapshotManager.deleteIndexSchema(dbName, server.getCommon().getSchemaVersion(), tableName, indexName);
    }

    DatabaseCommon tmpCommon = new DatabaseCommon();
    tmpCommon.deserializeSchema(cobj.getByteArray(ComObject.Tag.SCHEMA_BYTES));
    if (tmpCommon.getSchemaVersion() > server.getCommon().getSchemaVersion()) {
      server.getCommon().deserializeSchema(cobj.getByteArray(ComObject.Tag.SCHEMA_BYTES));
      server.getCommon().saveSchema(server.getDataDir());
    }

    return null;
  }

  @SchemaWriteLock
  public ComObject dropIndex(ComObject cobj, boolean replayedCommand) {
    try {
      String masterSlave = cobj.getString(ComObject.Tag.MASTER_SLAVE);
      if (server.getShard() == 0 &&
          server.getCommon().getServersConfig().getShards()[0].getMasterReplica() == server.getReplica() &&
          masterSlave.equals(SLAVE_STR)) {
        return null;
      }

      short serializationVersionNumber = cobj.getShort(ComObject.Tag.SERIALIZATION_VERSION);
      int schemaVersion = cobj.getInt(ComObject.Tag.SCHEMA_VERSION);
      String dbName = cobj.getString(ComObject.Tag.DB_NAME);
      if (schemaVersion < server.getSchemaVersion() && !replayedCommand) {
        throw new SchemaOutOfSyncException("currVer:" + server.getCommon().getSchemaVersion() + ":");
      }
      String table = cobj.getString(ComObject.Tag.TABLE_NAME);
      String indexName = cobj.getString(ComObject.Tag.INDEX_NAME);

      List<IndexSchema> toDrop = new ArrayList<>();
      synchronized (this) {
        for (Map.Entry<String, IndexSchema> entry : server.getCommon().getTableSchema(dbName, table,
            server.getDataDir()).getIndices().entrySet()) {
          String name = entry.getValue().getName();
          if (name.equals(indexName)) {
            toDrop.add(entry.getValue());
            if (entry.getValue().isPrimaryKey()) {
              throw new DatabaseException("Cannot drop primary key index: table=" + table + ", index=" + indexName);
            }
          }
        }

        for (IndexSchema indexSchema : toDrop) {
          server.getUpdateManager().truncateAnyIndex(dbName, table, indexName);
          server.removeIndex(dbName, table, indexSchema.getName());
          server.getCommon().getTableSchema(dbName, table, server.getDataDir()).getIndices().remove(indexSchema.getName());
          server.getCommon().getTableSchema(dbName, table, server.getDataDir()).getIndexesById().remove(indexSchema.getIndexId());
          SnapshotManager snapshotManager = server.getSnapshotManager();
          snapshotManager.deleteIndexFiles(dbName, table, indexSchema.getName());
          snapshotManager.deleteIndexSchema(dbName, server.getCommon().getSchemaVersion(), table, indexSchema.getName());
        }

        server.getCommon().saveSchema(server.getDataDir());
      }

      dropIndexSlave(dbName, table, toDrop);

      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.SCHEMA_BYTES, server.getCommon().serializeSchema(serializationVersionNumber));

      return retObj;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  private void dropIndexSlave(String dbName, String table, List<IndexSchema> toDrop) throws IOException {
    ComObject cobj;
    cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME, dbName);
    cobj.put(ComObject.Tag.SCHEMA_VERSION, server.getCommon().getSchemaVersion());
    cobj.put(ComObject.Tag.METHOD, "SchemaManager:dropIndexSlave");
    cobj.put(ComObject.Tag.SCHEMA_BYTES, server.getCommon().serializeSchema(DatabaseClient.SERIALIZATION_VERSION));
    cobj.put(ComObject.Tag.TABLE_NAME, table);
    cobj.put(ComObject.Tag.MASTER_SLAVE, SLAVE_STR);
    ComArray array = cobj.putArray(ComObject.Tag.INDICES, ComObject.Type.STRING_TYPE);
    for (IndexSchema indexSchema : toDrop) {
      array.add(indexSchema.getName());
    }

    for (int i = 0; i < server.getShardCount(); i++) {
      for (int j = 0; j < server.getReplicationFactor(); j++) {
        server.getDatabaseClient().send(null, i, j, cobj, DatabaseClient.Replica.SPECIFIED);
      }
    }
  }

  public void reconcileSchema() {
    try {
      if (server.getShard() != 0 || server.getReplica() != 0) {
        return;
      }
      logger.info("reconcile schema - begin");
      int threadCount = server.getShardCount() * server.getReplicationFactor();
      ThreadPoolExecutor executor = new ThreadPoolExecutor(threadCount, threadCount, 10_000,
          TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
      try {
        final ComObject highestSchemaVersions = readSchemaVersions();
        List<Future> futures = new ArrayList<>();
        for (int i = 0; i < server.getShardCount(); i++) {
          for (int j = 0; j < server.getReplicationFactor(); j++) {
            final int shard = i;
            final int replica = j;
            futures.add(executor.submit((Callable) () -> getSchemaVersion(shard, replica)));
          }
        }
        doReconcileSchema(highestSchemaVersions, futures);
      }
      finally {
        executor.shutdownNow();
      }
      logger.info("reconcile schema - end");
    }
    catch (Exception e) {
      logger.error("Error reconciling schema", e);
    }
  }

  private void doReconcileSchema(ComObject highestSchemaVersions, List<Future> futures) {
    try {
      List<ComObject> retObjs = new ArrayList<>();
      for (Future future : futures) {
        ComObject cobj = (ComObject) future.get();
        if (cobj != null) {
          retObjs.add(cobj);
        }
      }
      for (ComObject retObj : retObjs) {
        getHighestSchemaVersions(retObj.getInt(ComObject.Tag.SHARD), retObj.getInt(ComObject.Tag.REPLICA),
            highestSchemaVersions, retObj);
      }

      pushHighestSchema(highestSchemaVersions);

      if (server.getShard() == 0 && server.getReplica() == 0) {
        server.pushSchema();
      }
    }
    catch (Exception e) {
      logger.error("Error pushing schema", e);
    }
  }

  private Object getSchemaVersion(int shard, int replica) {
    long beginTime = System.currentTimeMillis();
    while (!server.getShutdown()) {
      try {
        if (server.getCommon().getServersConfig().getShards()[shard].getReplicas()[replica].isDead()) {
          logger.error("Server appears to be dead, skipping: shard={}, replica={}", shard, replica);
          return null;
        }
        if (server.getShard() == shard && server.getReplica() == replica) {
          break;
        }
        ComObject cobj = new ComObject();
        byte[] ret = server.getDatabaseClient().send("SchemaManager:getSchemaVersions", shard, replica, cobj,
            DatabaseClient.Replica.SPECIFIED);
        if (ret != null) {
          ComObject retObj = new ComObject(ret);
          retObj.put(ComObject.Tag.SHARD, shard);
          retObj.put(ComObject.Tag.REPLICA, replica);
          return retObj;
        }
      }
      catch (Exception e) {
        logger.error("Error checking if server is healthy: shard={}, replica={}", shard, replica);
      }
      if (System.currentTimeMillis() - beginTime > 2 * 60 * 1000) {
        logger.error("Server appears to be dead, skipping: shard={}, replica={}", shard, replica);
        return null;
      }
      try {
        Thread.sleep(1_000);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return null;
      }
    }
    return null;
  }

  public ComObject getSchemaVersions(ComObject cobj, boolean replayedCommand) {
    return readSchemaVersions();
  }

  private void pushHighestSchema(ComObject highestSchemaVersions) {
    ComArray databases = highestSchemaVersions.getArray(ComObject.Tag.DATABASES);
    for (int i = 0; i < databases.getArray().size(); i++) {
      ComObject db = (ComObject) databases.getArray().get(i);
      String currDbName = db.getString(ComObject.Tag.DB_NAME);
      ComArray tables = db.getArray(ComObject.Tag.TABLES);
      for (int j = 0; j < tables.getArray().size(); j++) {
        ComObject table = (ComObject) tables.getArray().get(i);
        String currTableName = table.getString(ComObject.Tag.TABLE_NAME);
        Boolean hasDiscrepency = table.getBoolean(ComObject.Tag.HAS_DISCREPANCY);
        if (hasDiscrepency != null && hasDiscrepency) {
          logger.info("Table schema has discrepancy, will push schema: db={}, table={}", currDbName, currTableName);
          byte[] tableSchema = getHighestTableSchema(currDbName, currTableName, table.getInt(ComObject.Tag.SHARD),
              table.getInt(ComObject.Tag.REPLICA));
          pushTableSchema(currDbName, currTableName, tableSchema, table.getInt(ComObject.Tag.SCHEMA_VERSION));
        }
        ComArray indices = table.getArray(ComObject.Tag.INDICES);
        for (int k = 0; k < indices.getArray().size(); k++) {
          ComObject index = (ComObject) indices.getArray().get(k);
          String currIndexName = index.getString(ComObject.Tag.INDEX_NAME);
          hasDiscrepency = index.getBoolean(ComObject.Tag.HAS_DISCREPANCY);
          if (hasDiscrepency != null && hasDiscrepency) {
            byte[] indexSchema = getHighestIndexSchema(currDbName, currTableName, currIndexName, index.getInt(ComObject.Tag.SHARD),
                index.getInt(ComObject.Tag.REPLICA));
            logger.info("Index schema has discrepancy, will push schema: db={}, table={}, index={}", currDbName,
                currTableName, currIndexName);
            pushIndexSchema(currDbName, currTableName, currIndexName, indexSchema, index.getInt(ComObject.Tag.SCHEMA_VERSION));
          }
        }
      }
    }

  }

  private void pushIndexSchema(String currDbName, String currTableName, String currIndexName,
                               byte[] indexSchema, Integer schemaVersion) {
    try {
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.DB_NAME, currDbName);
      cobj.put(ComObject.Tag.TABLE_NAME, currTableName);
      cobj.put(ComObject.Tag.INDEX_NAME, currIndexName);
      cobj.put(ComObject.Tag.INDEX_SCHEMA, indexSchema);
      cobj.put(ComObject.Tag.SCHEMA_VERSION, schemaVersion);

      server.getDatabaseClient().sendToAllShards("SchemaManager:updateIndexSchema", 0, cobj,
          DatabaseClient.Replica.ALL);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }

  }

  private byte[] getHighestIndexSchema(String currDbName, String currTableName, String currIndexName, Integer shard,
                                       Integer replica) {
    try {
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.DB_NAME, currDbName);
      cobj.put(ComObject.Tag.TABLE_NAME, currTableName);
      cobj.put(ComObject.Tag.INDEX_NAME, currIndexName);

      byte[] ret = server.getDatabaseClient().send("SchemaManager:getIndexSchema", shard, replica, cobj,
          DatabaseClient.Replica.SPECIFIED);
      ComObject retObj = new ComObject(ret);
      return retObj.getByteArray(ComObject.Tag.INDEX_SCHEMA);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private void pushTableSchema(String currDbName, String currTableName, byte[] tableSchema, Integer schemaVersion) {
    try {
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.DB_NAME, currDbName);
      cobj.put(ComObject.Tag.TABLE_NAME, currTableName);
      cobj.put(ComObject.Tag.TABLE_SCHEMA, tableSchema);
      cobj.put(ComObject.Tag.SCHEMA_VERSION, schemaVersion);

      server.getDatabaseClient().sendToAllShards("SchemaManager:updateTableSchema", 0, cobj,
          DatabaseClient.Replica.ALL);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  @SchemaWriteLock
  public ComObject updateIndexSchema(ComObject cobj, boolean replayedCommand) {
    try {
      String dbName = cobj.getString(ComObject.Tag.DB_NAME);
      String tableName = cobj.getString(ComObject.Tag.TABLE_NAME);
      String indexName = cobj.getString(ComObject.Tag.INDEX_NAME);
      byte[] bytes = cobj.getByteArray(ComObject.Tag.INDEX_SCHEMA);
      int schemaVersion = cobj.getInt(ComObject.Tag.SCHEMA_VERSION);

      logger.info("Updating schema for Index: db={},  table={}, index={}, schemaVersion={}", dbName, tableName,
          indexName, schemaVersion);

      File indexDir = server.getSnapshotManager().getIndexSchemaDir(dbName, tableName, indexName);
      File newSchemaFile = new File(indexDir, "schema." + schemaVersion + ".bin");
      File[] indexSchemas = indexDir.listFiles();
      doUpdateSchema(bytes, schemaVersion, newSchemaFile, indexSchemas);
      return null;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  @SchemaWriteLock
  public ComObject updateTableSchema(ComObject cobj, boolean replayedCommand) {
    try {

      String dbName = cobj.getString(ComObject.Tag.DB_NAME);
      String tableName = cobj.getString(ComObject.Tag.TABLE_NAME);
      byte[] bytes = cobj.getByteArray(ComObject.Tag.TABLE_SCHEMA);
      int schemaVersion = cobj.getInt(ComObject.Tag.SCHEMA_VERSION);

      logger.info("Updating schema for table: db={}, table={}, schemaVersion={}", dbName, tableName, schemaVersion);

      File tableDir = server.getSnapshotManager().getTableSchemaDir(dbName, tableName);
      File newSchemaFile = new File(tableDir, "schema." + schemaVersion + ".bin");
      File[] tableSchemas = tableDir.listFiles();
      doUpdateSchema(bytes, schemaVersion, newSchemaFile, tableSchemas);
      return null;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private void doUpdateSchema(byte[] bytes, int schemaVersion, File newSchemaFile, File[] schemas) throws IOException {
    if (schemas != null && schemas.length > 0) {
      sortSchemaFiles(schemas);
      File tableSchemaFile = schemas[schemas.length - 1];
      String filename = tableSchemaFile.getName();
      int pos = filename.indexOf('.');
      int pos2 = filename.indexOf('.', pos + 1);
      int currSchemaVersion = Integer.parseInt(filename.substring(pos + 1, pos2));
      if (currSchemaVersion < schemaVersion) {
        if (newSchemaFile.exists()) {
          Files.delete(newSchemaFile.toPath());
        }
        try (FileOutputStream out = new FileOutputStream(newSchemaFile)) {
          out.write(bytes);
        }
        server.getCommon().loadSchema(server.getDataDir());
      }
    }
    else {
      if (newSchemaFile.exists()) {
        Files.delete(newSchemaFile.toPath());
      }
      newSchemaFile.getParentFile().mkdirs();
      try (FileOutputStream out = new FileOutputStream(newSchemaFile)) {
        out.write(bytes);
      }
      server.getCommon().loadSchema(server.getDataDir());
    }
  }

  @SchemaReadLock
  public ComObject getIndexSchema(ComObject cobj, boolean replayedCommand) {
    try {
      String dbName = cobj.getString(ComObject.Tag.DB_NAME);
      String tableName = cobj.getString(ComObject.Tag.TABLE_NAME);
      String indexName = cobj.getString(ComObject.Tag.INDEX_NAME);

      ComObject retObj = new ComObject();
      File indexDir = server.getSnapshotManager().getIndexSchemaDir(dbName, tableName, indexName);
      File[] indexSchemas = indexDir.listFiles();
      if (indexSchemas != null && indexSchemas.length > 0) {
        sortSchemaFiles(indexSchemas);
        File indexSchemaFile = indexSchemas[indexSchemas.length - 1];
        byte[] bytes = IOUtils.toByteArray(new FileInputStream(indexSchemaFile));
        retObj.put(ComObject.Tag.INDEX_SCHEMA, bytes);
      }
      return retObj;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  @SchemaReadLock
  public ComObject getTableSchema(ComObject cobj, boolean replayedCommand) {
    try {
      String dbName = cobj.getString(ComObject.Tag.DB_NAME);
      String tableName = cobj.getString(ComObject.Tag.TABLE_NAME);

      ComObject retObj = new ComObject();
      File tableDir = server.getSnapshotManager().getTableSchemaDir(dbName, tableName);
      File[] tableSchemas = tableDir.listFiles();
      if (tableSchemas != null && tableSchemas.length > 0) {
        sortSchemaFiles(tableSchemas);
        File tableSchemaFile = tableSchemas[tableSchemas.length - 1];
        byte[] bytes = IOUtils.toByteArray(new FileInputStream(tableSchemaFile));
        retObj.put(ComObject.Tag.TABLE_SCHEMA, bytes);
      }
      return retObj;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private byte[] getHighestTableSchema(String currDbName, String currTableName, int shard, Integer replica) {
    try {
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.METHOD, "SchemaManager:getTableSchema");
      cobj.put(ComObject.Tag.DB_NAME, currDbName);
      cobj.put(ComObject.Tag.TABLE_NAME, currTableName);

      byte[] ret = server.getDatabaseClient().send(null, shard, replica, cobj, DatabaseClient.Replica.SPECIFIED);
      ComObject retObj = new ComObject(ret);
      return retObj.getByteArray(ComObject.Tag.TABLE_SCHEMA);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  ComObject readSchemaVersions() {
    ComObject cobj = new ComObject();
    Map<String, Schema> databasesMap = server.getCommon().getDatabases();
    ComArray databases = cobj.putArray(ComObject.Tag.DATABASES, ComObject.Type.OBJECT_TYPE);
    for (String dbName : databasesMap.keySet()) {
      ComObject dbObj = new ComObject();
      databases.add(dbObj);
      dbObj.put(ComObject.Tag.DB_NAME, dbName);
      ComArray tables = dbObj.putArray(ComObject.Tag.TABLES, ComObject.Type.OBJECT_TYPE);
      for (TableSchema tableSchema : server.getCommon().getTables(dbName).values()) {
        readSchemaVersionsForTable(dbName, tables, tableSchema);
      }
    }
    return cobj;
  }

  private void readSchemaVersionsForTable(String dbName, ComArray tables, TableSchema tableSchema) {
    ComObject tableObj = new ComObject();
    tables.add(tableObj);
    tableObj.put(ComObject.Tag.TABLE_NAME, tableSchema.getName());
    File tableDir = server.getSnapshotManager().getTableSchemaDir(dbName, tableSchema.getName());
    File[] tableSchemas = tableDir.listFiles();
    if (tableSchemas != null && tableSchemas.length > 0) {
      sortSchemaFiles(tableSchemas);
      File tableSchemaFile = tableSchemas[tableSchemas.length - 1];
      tableObj.put(ComObject.Tag.SCHEMA_VERSION, getSchemVersionFromFile(tableSchemaFile));
    }
    else {
      tableObj.put(ComObject.Tag.SCHEMA_VERSION, 0);
    }
    tableObj.put(ComObject.Tag.SHARD, server.getShard());
    tableObj.put(ComObject.Tag.REPLICA, server.getReplica());
    ComArray indices = tableObj.putArray(ComObject.Tag.INDICES, ComObject.Type.OBJECT_TYPE);
    for (IndexSchema indexSchema : tableSchema.getIndices().values()) {
      ComObject indexObj = new ComObject();
      indices.add(indexObj);
      indexObj.put(ComObject.Tag.INDEX_NAME, indexSchema.getName());
      File indexDir = server.getSnapshotManager().getIndexSchemaDir(dbName, tableSchema.getName(), indexSchema.getName());
      File[] indexSchemas = indexDir.listFiles();
      if (indexSchemas != null && indexSchemas.length > 0) {
        sortSchemaFiles(indexSchemas);
        File indexSchemaFile = indexSchemas[indexSchemas.length - 1];
        indexObj.put(ComObject.Tag.SCHEMA_VERSION, getSchemVersionFromFile(indexSchemaFile));
      }
      else {
        indexObj.put(ComObject.Tag.SCHEMA_VERSION, 0);
      }
      indexObj.put(ComObject.Tag.SHARD, server.getShard());
      indexObj.put(ComObject.Tag.REPLICA, server.getReplica());
    }
  }

  private int getSchemVersionFromFile(File schemaFile) {
    String name = schemaFile.getName();
    int pos = name.indexOf('.');
    int pos2 = name.indexOf('.', pos + 1);
    return Integer.valueOf(name.substring(pos + 1, pos2));
  }

  private void getHighestSchemaVersions(int shard, int replica, ComObject highestSchemaVersions, ComObject retObj) {
    ComArray databases = retObj.getArray(ComObject.Tag.DATABASES);
    for (int i = 0; i < databases.getArray().size(); i++) {
      ComObject db = (ComObject) databases.getArray().get(i);
      String dbName = db.getString(ComObject.Tag.DB_NAME);
      ComArray tables = db.getArray(ComObject.Tag.TABLES);
      ConcurrentHashMap<String, Integer> tablesFound = new ConcurrentHashMap<>();
      for (int j = 0; j < tables.getArray().size(); j++) {
        ComObject table = (ComObject) tables.getArray().get(j);
        String tableName = table.getString(ComObject.Tag.TABLE_NAME);

        setHighestSchemaVersionsForTable(shard, replica, highestSchemaVersions, dbName, tablesFound, table, tableName);

        setHighestSchemaVersionsForIndices(shard, replica, highestSchemaVersions, dbName, table, tableName);
      }
    }
  }

  private void setHighestSchemaVersionsForTable(int shard, int replica, ComObject highestSchemaVersions, String dbName,
                                                ConcurrentHashMap<String, Integer> tablesFound, ComObject table,
                                                String tableName) {
    int tableSchemaVersion = table.getInt(ComObject.Tag.SCHEMA_VERSION);
    tablesFound.put(tableName, tableSchemaVersion);
    ComObject highestTable = getSchemaVersion(dbName, tableName, highestSchemaVersions);
    if (highestTable == null) {
      setHighestSchemaVersion(dbName, tableName, tableSchemaVersion, shard, replica, highestSchemaVersions, tablesFound);
    }
    else {
      if (tableSchemaVersion > highestTable.getInt(ComObject.Tag.SCHEMA_VERSION)) {
        setHighestSchemaVersion(dbName, tableName, tableSchemaVersion, shard, replica, highestSchemaVersions, tablesFound);
      }
      else if (tableSchemaVersion < highestTable.getInt(ComObject.Tag.SCHEMA_VERSION)) {
        setHighestSchemaVersion(dbName, tableName, highestTable.getInt(ComObject.Tag.SCHEMA_VERSION),
            highestTable.getInt(ComObject.Tag.SHARD),
            highestTable.getInt(ComObject.Tag.REPLICA), highestSchemaVersions, tablesFound);
      }
      else {
        tablesFound.remove(tableName);
      }
    }
  }

  private void setHighestSchemaVersionsForIndices(int shard, int replica, ComObject highestSchemaVersions,
                                                  String dbName, ComObject table, String tableName) {
    ConcurrentHashMap<String, Integer> indicesFound = new ConcurrentHashMap<>();
    ComArray indices = table.getArray(ComObject.Tag.INDICES);
    for (int k = 0; k < indices.getArray().size(); k++) {
      ComObject index = (ComObject) indices.getArray().get(k);
      int indexSchemaVersion = index.getInt(ComObject.Tag.SCHEMA_VERSION);
      String indexName = index.getString(ComObject.Tag.INDEX_NAME);
      indicesFound.put(indexName, indexSchemaVersion);
      ComObject highestIndex = getSchemaVersion(dbName, tableName, indexName, highestSchemaVersions);
      if (highestIndex == null) {
        setHighestSchemaVersion(dbName, tableName, indexName, indexSchemaVersion, shard, replica,
            highestSchemaVersions, indicesFound);
      }
      else {
        if (indexSchemaVersion > highestIndex.getInt(ComObject.Tag.SCHEMA_VERSION)) {
          setHighestSchemaVersion(dbName, tableName, indexName, indexSchemaVersion, shard, replica,
              highestSchemaVersions, indicesFound);
        }
        else if (indexSchemaVersion < highestIndex.getInt(ComObject.Tag.SCHEMA_VERSION)) {
          setHighestSchemaVersion(dbName, tableName, indexName, highestIndex.getInt(ComObject.Tag.SCHEMA_VERSION),
              highestIndex.getInt(ComObject.Tag.SHARD), highestIndex.getInt(ComObject.Tag.REPLICA),
              highestSchemaVersions, indicesFound);
        }
        else {
          indicesFound.remove(indexName);
        }
      }
    }
  }

  private ComObject getSchemaVersion(String dbName, String tableName, ComObject highestSchemaVersions) {
    ComArray databases = highestSchemaVersions.getArray(ComObject.Tag.DATABASES);
    for (int i = 0; i < databases.getArray().size(); i++) {
      ComObject db = (ComObject) databases.getArray().get(i);
      String currDbName = db.getString(ComObject.Tag.DB_NAME);
      if (currDbName.equals(dbName)) {
        ComArray tables = db.getArray(ComObject.Tag.TABLES);
        for (int j = 0; j < tables.getArray().size(); j++) {
          ComObject table = (ComObject) tables.getArray().get(j);
          String currTableName = table.getString(ComObject.Tag.TABLE_NAME);
          if (currTableName.equals(tableName)) {
            return table;
          }
        }
      }
    }
    return null;
  }

  private void setHighestSchemaVersion(String dbName, String tableName, String indexName, int indexSchemaVersion,
                                       int shard, int replica, ComObject highestSchemaVersions,
                                       ConcurrentHashMap<String, Integer> indicesFound) {
    ComArray databases = highestSchemaVersions.getArray(ComObject.Tag.DATABASES);
    for (int i = 0; i < databases.getArray().size(); i++) {
      ComObject db = (ComObject) databases.getArray().get(i);
      String currDbName = db.getString(ComObject.Tag.DB_NAME);
      if (currDbName.equals(dbName)) {
        ComArray tables = db.getArray(ComObject.Tag.TABLES);
        for (int j = 0; j < tables.getArray().size(); j++) {
          if (setHighestSchemaVersionForTable(tableName, indexName, indexSchemaVersion, shard, replica,
              indicesFound, i, tables))
            return;
        }
      }
    }
  }

  private boolean setHighestSchemaVersionForTable(String tableName, String indexName, int indexSchemaVersion,
                                                  int shard, int replica, ConcurrentHashMap<String, Integer> indicesFound,
                                                  int i, ComArray tables) {
    ComObject table = (ComObject) tables.getArray().get(i);
    String currTableName = table.getString(ComObject.Tag.TABLE_NAME);
    if (currTableName.equals(tableName)) {
      ComArray indices = table.getArray(ComObject.Tag.INDICES);
      for (int k = 0; k < indices.getArray().size(); k++) {
        ComObject index = (ComObject) indices.getArray().get(k);
        String currIndexName = index.getString(ComObject.Tag.INDEX_NAME);
        indicesFound.remove(currIndexName);
        if (currIndexName.equals(indexName)) {
          index.put(ComObject.Tag.SCHEMA_VERSION, indexSchemaVersion);
          index.put(ComObject.Tag.SHARD, shard);
          index.put(ComObject.Tag.REPLICA, replica);
          index.put(ComObject.Tag.HAS_DISCREPANCY, true);
          return true;
        }
      }
      ComObject index = new ComObject();
      indices.add(index);
      index.put(ComObject.Tag.INDEX_NAME, indexName);
      index.put(ComObject.Tag.SCHEMA_VERSION, indexSchemaVersion);
      index.put(ComObject.Tag.SHARD, shard);
      index.put(ComObject.Tag.REPLICA, replica);
      index.put(ComObject.Tag.HAS_DISCREPANCY, true);

    }
    return false;
  }

  private ComObject getSchemaVersion(String dbName, String tableName, String indexName, ComObject highestSchemaVersions) {
    ComArray databases = highestSchemaVersions.getArray(ComObject.Tag.DATABASES);
    for (int i = 0; i < databases.getArray().size(); i++) {
      ComObject db = (ComObject) databases.getArray().get(i);
      String currDbName = db.getString(ComObject.Tag.DB_NAME);
      if (currDbName.equals(dbName)) {
        ComArray tables = db.getArray(ComObject.Tag.TABLES);
        for (int j = 0; j < tables.getArray().size(); j++) {
          ComObject index = getSchemaVersionForIndex(tableName, indexName, tables, j);
          if (index != null) {
            return index;
          }
        }
      }
    }
    return null;
  }

  private ComObject getSchemaVersionForIndex(String tableName, String indexName, ComArray tables, int j) {
    ComObject table = (ComObject) tables.getArray().get(j);
    String currTableName = table.getString(ComObject.Tag.TABLE_NAME);
    if (currTableName.equals(tableName)) {
      ComArray indices = table.getArray(ComObject.Tag.INDICES);
      for (int k = 0; k < indices.getArray().size(); k++) {
        ComObject index = (ComObject) indices.getArray().get(k);
        String currIndexName = index.getString(ComObject.Tag.INDEX_NAME);
        if (currIndexName.equals(indexName)) {
          return index;
        }
      }
    }
    return null;
  }

  private void setHighestSchemaVersion(String dbName, String tableName, int tableSchemaVersion, int shard,
                                       int replica, ComObject highestSchemaVersions,
                                       ConcurrentHashMap<String, Integer> tablesFound) {
    ComArray databases = highestSchemaVersions.getArray(ComObject.Tag.DATABASES);
    for (int i = 0; i < databases.getArray().size(); i++) {
      ComObject db = (ComObject) databases.getArray().get(i);
      String currDbName = db.getString(ComObject.Tag.DB_NAME);
      if (currDbName.equals(dbName)) {
        ComArray tables = db.getArray(ComObject.Tag.TABLES);

        for (int j = 0; j < tables.getArray().size(); j++) {
          ComObject table = (ComObject) tables.getArray().get(i);
          String currTableName = table.getString(ComObject.Tag.TABLE_NAME);
          tablesFound.remove(currTableName);
          if (currTableName.equals(tableName)) {
            table.put(ComObject.Tag.SCHEMA_VERSION, tableSchemaVersion);
            table.put(ComObject.Tag.SHARD, shard);
            table.put(ComObject.Tag.REPLICA, replica);
            table.put(ComObject.Tag.HAS_DISCREPANCY, true);
            return;
          }
        }
        ComObject table = new ComObject();
        tables.add(table);
        table.put(ComObject.Tag.TABLE_NAME, tableName);
        table.put(ComObject.Tag.SCHEMA_VERSION, tableSchemaVersion);
        table.put(ComObject.Tag.SHARD, shard);
        table.put(ComObject.Tag.REPLICA, replica);
        table.put(ComObject.Tag.HAS_DISCREPANCY, true);
      }
    }
  }
}
