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
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static com.sonicbase.common.DatabaseCommon.sortSchemaFiles;

/**
 * Responsible for
 */
public class SchemaManager {

  private static Logger logger = LoggerFactory.getLogger(SchemaManager.class);


  private final com.sonicbase.server.DatabaseServer server;

  public SchemaManager(com.sonicbase.server.DatabaseServer databaseServer) {
    this.server = databaseServer;
  }

  public static class AutoIncrementValue {
    private final Object mutex = new Object();
    private Object currValue = null;
    public DataType.Type dataType;

    public AutoIncrementValue(DataType.Type type) {
      this.dataType = type;
      setInitialValue();
    }

    public void setInitialValue() {
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

    if (tableSchema.getIndexes().get(indexName) != null) {
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

    server.getCommon().updateTable(server.getClient(), dbName, server.getDataDir(), tableSchema);

    doCreateIndex(dbName, tableSchema, indexName, fields);

    return createdIndices;
  }

  public void addAllIndices(String dbName) {
    for (TableSchema table : server.getCommon().getTables(dbName).values()) {
      for (IndexSchema index : table.getIndexes().values()) {
        server.getIndices(dbName).addIndex(table, index.getName(), index.getComparators());
      }
    }
  }

  public void doCreateIndex(
      String dbName, TableSchema tableSchema, String indexName, String[] currFields) {
    Comparator[] comparators = tableSchema.getComparators(currFields);

    server.getIndices(dbName).addIndex(tableSchema, indexName, comparators);
  }

  @SchemaWriteLock
  public ComObject createDatabase(ComObject cobj, boolean replayedCommand) {
    try {
      String dbName = cobj.getString(ComObject.Tag.dbName);
      String masterSlave = cobj.getString(ComObject.Tag.masterSlave);
      dbName = dbName.toLowerCase();

      if (replayedCommand && null != server.getCommon().getSchema(dbName)) {
        return null;
      }

      synchronized (this) {

        if (server.getCommon().getDatabases().containsKey(dbName)) {
          throw new DatabaseException("Database already exists: name=" + dbName);
        }
        logger.info("Create database: shard=" + server.getShard() + ", replica=" + server.getReplica() + ", name=" + dbName);
        File dir = null;
        if (com.sonicbase.server.DatabaseServer.USE_SNAPSHOT_MGR_OLD) {
          dir = new File(server.getDataDir(), "snapshot/" + server.getShard() + "/" + server.getReplica() + "/" + dbName);
        }
        else {
          dir = new File(server.getDataDir(), "delta/" + server.getShard() + "/" + server.getReplica() + "/" + dbName);
        }
        if (!dir.exists() && !dir.mkdirs()) {
          throw new DatabaseException("Error creating database directory: dir=" + dir.getAbsolutePath());
        }

        server.getIndices().put(dbName, new Indices());
        server.getCommon().addDatabase(dbName);
        server.getCommon().saveSchema(server.getClient(), server.getDataDir());
      }
      if (masterSlave.equals("master")) {
        for (int i = 0; i < server.getShardCount(); i++) {
          cobj.put(ComObject.Tag.slave, true);
          cobj.put(ComObject.Tag.masterSlave, "slave");
          cobj.put(ComObject.Tag.method, "SchemaManager:createDatabaseSlave");
          server.getDatabaseClient().send(null, i, 0, cobj, DatabaseClient.Replica.def);
        }
        server.pushSchema();
      }
    }
    finally {

    }
    return null;
  }

  @SchemaWriteLock
  public ComObject createDatabaseSlave(ComObject cobj, boolean replayedCommand) {

    String dbName = cobj.getString(ComObject.Tag.dbName);
    String masterSlave = cobj.getString(ComObject.Tag.masterSlave);
    dbName = dbName.toLowerCase();

    if (server.getShard() == 0 &&
        server.getCommon().getServersConfig().getShards()[0].getMasterReplica() == server.getReplica() &&
        masterSlave.equals("slave")) {
      return null;
    }

    synchronized (this) {
      if (server.getCommon().getDatabases().get(dbName) == null) {
        logger.info("Create database: shard=" + server.getShard() + ", replica=" + server.getReplica() + ", name=" + dbName);
        File dir = null;
        if (DatabaseServer.USE_SNAPSHOT_MGR_OLD) {
          dir = new File(server.getDataDir(), "snapshot/" + server.getShard() + "/" + server.getReplica() + "/" + dbName);
        }
        else {
          dir = new File(server.getDataDir(), "delta/" + server.getShard() + "/" + server.getReplica() + "/" + dbName);
        }
        if (!dir.exists() && !dir.mkdirs()) {
          throw new DatabaseException("Error creating database directory: dir=" + dir.getAbsolutePath());
        }

        server.getIndices().put(dbName, new Indices());
        server.getCommon().addDatabase(dbName);
        server.getCommon().saveSchema(server.getClient(), server.getDataDir());
      }
    }
    return null;
  }

  @SchemaWriteLock
  public ComObject dropTable(ComObject cobj, boolean replayedCommand) {
    String masterSlave = cobj.getString(ComObject.Tag.masterSlave);
    if (server.getShard() == 0 &&
        server.getCommon().getServersConfig().getShards()[0].getMasterReplica() == server.getReplica() &&
        masterSlave.equals("slave")) {
      return null;
    }
    try {
      short serializationVersionNumber = cobj.getShort(ComObject.Tag.serializationVersion);
      String dbName = cobj.getString(ComObject.Tag.dbName);
      String tableName = cobj.getString(ComObject.Tag.tableName);

      server.getCommon().getSchemaWriteLock(dbName).lock();
      try {
        server.getCommon().dropTable(server.getClient(), dbName, tableName, server.getDataDir());
        server.removeIndices(dbName, tableName);
        SnapshotManager snapshotManager = server.getSnapshotManager();
        snapshotManager.deleteTableSchema(dbName, server.getCommon().getSchemaVersion(), tableName);
      }
      finally {
        server.getCommon().getSchemaWriteLock(dbName).unlock();
      }

      if (masterSlave.equals("master")) {
        Random rand = new Random(System.currentTimeMillis());
        for (int i = 0; i < server.getShardCount(); i++) {

          cobj.put(ComObject.Tag.masterSlave, "slave");
          byte[] ret = server.getDatabaseClient().send(null, i, rand.nextLong(), cobj, DatabaseClient.Replica.def);
        }
        server.pushSchema();
      }

      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.schemaBytes, server.getCommon().serializeSchema(serializationVersionNumber));
      return retObj;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  @SchemaWriteLock
  public ComObject createTableSlave(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    String masterSlave = cobj.getString(ComObject.Tag.masterSlave);

    if (server.getShard() == 0 &&
        server.getCommon().getServersConfig().getShards()[0].getMasterReplica() == server.getReplica() &&
        masterSlave.equals("slave")) {
      return null;
    }

    server.getCommon().getSchemaWriteLock(dbName).lock();
    try {
      byte[] bytes = cobj.getByteArray(ComObject.Tag.schemaBytes);
      DatabaseCommon tmpCommon = new DatabaseCommon();
      tmpCommon.deserializeSchema(bytes);

      String tableName = cobj.getString(ComObject.Tag.tableName);
      TableSchema tableSchema = tmpCommon.getTableSchema(dbName, tableName, server.getDataDir());

      synchronized (this) {
        if (server.getCommon().getTableSchema(dbName, tableName, server.getDataDir()) == null) {
          server.getCommon().getTables(dbName).put(tableName, tableSchema);
          server.getCommon().getTablesById(dbName).put(tableSchema.getTableId(), tableSchema);
        }
        SnapshotManager snapshotManager = server.getSnapshotManager();
        snapshotManager.saveTableSchema(dbName, tmpCommon.getSchemaVersion(), tableName, tableSchema);

        for (IndexSchema indexSchema : tableSchema.getIndexes().values()) {
          if (indexSchema.isPrimaryKey()) {
            snapshotManager.saveIndexSchema(dbName, tmpCommon.getSchemaVersion(), tableSchema, indexSchema);
          }
        }

        Map<String, IndexSchema> indices = tableSchema.getIndexes();
        for (Map.Entry<String, IndexSchema> index : indices.entrySet()) {
          doCreateIndex(dbName, tableSchema, index.getKey(), index.getValue().getFields());
        }
        server.getCommon().setSchemaVersion(tmpCommon.getSchemaVersion());
        server.getCommon().saveSchema(server.getClient(), server.getDataDir());
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
      String masterSlave = cobj.getString(ComObject.Tag.masterSlave);
      if (server.getShard() == 0 &&
          server.getCommon().getServersConfig().getShards()[0].getMasterReplica() == server.getReplica() &&
          masterSlave.equals("slave")) {
        return null;
      }
      short serializationVersionNumber = cobj.getShort(ComObject.Tag.serializationVersion);
      String dbName = cobj.getString(ComObject.Tag.dbName);

      String tableName = null;
      server.getCommon().getSchemaWriteLock(dbName).lock();
      try {
        CreateTableStatementImpl createTableStatement = new CreateTableStatementImpl();
        createTableStatement.deserialize(cobj.getByteArray(ComObject.Tag.createTableStatement));

        logger.info("Create table: shard=" + server.getShard() + ", replica=" + server.getReplica() + ", name=" + createTableStatement.getTablename());

        synchronized (this) {
          TableSchema schema = new TableSchema();
          tableName = createTableStatement.getTablename();
          if (server.getCommon().getTables(dbName).containsKey(tableName) && server.getShard() == 0 &&
                server.getCommon().getServersConfig().getShards()[0].getMasterReplica() == server.getReplica()) {
            throw new DatabaseException("Table already exists: name=" + tableName);
          }
          else {
            if (replayedCommand) {
              logger.info("replayedCommand: table=" + tableName + ", tableCount=" + server.getCommon().getTables(dbName).size());
              if (server.getCommon().getTables(dbName).containsKey(tableName.toLowerCase())) {
                ComObject retObj = new ComObject();
                retObj.put(ComObject.Tag.schemaBytes, server.getCommon().serializeSchema(serializationVersionNumber));

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

            server.getCommon().addTable(server.getClient(), dbName, server.getDataDir(), schema);

            SnapshotManager snapshotManager = server.getSnapshotManager();
            snapshotManager.saveTableSchema(dbName, server.getCommon().getSchemaVersion() + 1, tableName, schema);

            String[] primaryKeyFields = primaryKey.toArray(new String[primaryKey.size()]);
            createIndex(dbName, tableName.toLowerCase(), "_primarykey", true, true, primaryKeyFields);

            server.getCommon().saveSchema(server.getClient(), server.getDataDir());
          }
        }
      }
      finally {
        server.getCommon().getSchemaWriteLock(dbName).unlock();
      }

      if (!replayedCommand && masterSlave.equals("master")) {
        Random rand = new Random(System.currentTimeMillis());
        for (int i = 0; i < server.getShardCount(); i++) {
          ComObject slaveObj = new ComObject();

          slaveObj.put(ComObject.Tag.schemaBytes, server.getCommon().serializeSchema(DatabaseClient.SERIALIZATION_VERSION));
          slaveObj.put(ComObject.Tag.tableName, tableName);
          slaveObj.put(ComObject.Tag.dbName, dbName);
          slaveObj.put(ComObject.Tag.schemaVersion, server.getCommon().getSchemaVersion());
          slaveObj.put(ComObject.Tag.method, "SchemaManager:createTableSlave");
          slaveObj.put(ComObject.Tag.masterSlave, "slave");
          server.getDatabaseClient().send(null, i, rand.nextLong(), slaveObj, DatabaseClient.Replica.def);
        }
      }
      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.schemaBytes, server.getCommon().serializeSchema(serializationVersionNumber));
      return retObj;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }


  @SchemaWriteLock
  public ComObject dropColumnSlave(ComObject cobj, boolean replayedCommand) {
    if (server.getShard() == 0 &&
        server.getCommon().getServersConfig().getShards()[0].getMasterReplica() == server.getReplica()) {
      return null;
    }

    String dbName = cobj.getString(ComObject.Tag.dbName);
    server.getCommon().getSchemaWriteLock(dbName).lock();
    try {
      byte[] bytes = cobj.getByteArray(ComObject.Tag.schemaBytes);
      DatabaseCommon tmpCommon = new DatabaseCommon();
      tmpCommon.deserializeSchema(bytes);

      String tableName = cobj.getString(ComObject.Tag.tableName);
      TableSchema tableSchema = tmpCommon.getTableSchema(dbName, tableName, server.getDataDir());

      synchronized (this) {
        SnapshotManager snapshotManager = server.getSnapshotManager();
        server.getCommon().getTables(dbName).put(tableName, tableSchema);
        server.getCommon().getTablesById(dbName).put(tableSchema.getTableId(), tableSchema);
        snapshotManager.saveTableSchema(dbName, tmpCommon.getSchemaVersion(), tableName, tableSchema);

        server.getCommon().setSchemaVersion(tmpCommon.getSchemaVersion());
        server.getCommon().saveSchema(server.getClient(), server.getDataDir());
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
  public ComObject dropColumn(ComObject cobj, boolean replayedCommand) {

    try {
      short serializationVersionNumber = cobj.getShort(ComObject.Tag.serializationVersion);
      String dbName = cobj.getString(ComObject.Tag.dbName);
      String tableName = cobj.getString(ComObject.Tag.tableName).toLowerCase();
      String columnName = cobj.getString(ComObject.Tag.columnName).toLowerCase();

      synchronized (this) {
        TableSchema tableSchema = server.getCommon().getTableSchema(dbName, tableName, server.getDataDir());
        tableSchema.saveFields(server.getCommon().getSchemaVersion());

        for (IndexSchema indexSchema : tableSchema.getIndices().values()) {
          for (String fieldName : indexSchema.getFields()) {
            if (columnName.equals(fieldName)) {
              throw new DatabaseException("Field being used by index, cannot drop: field=" + columnName + ", index=" + indexSchema.getName());
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

        server.getCommon().saveSchema(server.getClient(), server.getDataDir());
      }

      if (!replayedCommand) {
        Random rand = new Random(System.currentTimeMillis());
        for (int i = 0; i < server.getShardCount(); i++) {
          ComObject slaveObj = new ComObject();

          slaveObj.put(ComObject.Tag.schemaBytes, server.getCommon().serializeSchema(DatabaseClient.SERIALIZATION_VERSION));
          slaveObj.put(ComObject.Tag.tableName, tableName);
          slaveObj.put(ComObject.Tag.dbName, dbName);
          slaveObj.put(ComObject.Tag.schemaVersion, server.getCommon().getSchemaVersion());
          slaveObj.put(ComObject.Tag.method, "SchemaManager:dropColumnSlave");
          server.getDatabaseClient().send(null, i, rand.nextLong(), slaveObj, DatabaseClient.Replica.def);
        }
      }

      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.schemaBytes, server.getCommon().serializeSchema(serializationVersionNumber));

      return retObj;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  @SchemaWriteLock
  public ComObject addColumnSlave(ComObject cobj, boolean replayedCommand) {
    if (server.getShard() == 0 &&
        server.getCommon().getServersConfig().getShards()[0].getMasterReplica() == server.getReplica()) {
      return null;
    }

    String dbName = cobj.getString(ComObject.Tag.dbName);
    server.getCommon().getSchemaWriteLock(dbName).lock();
    try {
      byte[] bytes = cobj.getByteArray(ComObject.Tag.schemaBytes);
      DatabaseCommon tmpCommon = new DatabaseCommon();
      tmpCommon.deserializeSchema(bytes);

      String tableName = cobj.getString(ComObject.Tag.tableName);
      TableSchema tableSchema = tmpCommon.getTableSchema(dbName, tableName, server.getDataDir());

      synchronized (this) {
        SnapshotManager snapshotManager = server.getSnapshotManager();
        server.getCommon().getTables(dbName).put(tableName, tableSchema);
        server.getCommon().getTablesById(dbName).put(tableSchema.getTableId(), tableSchema);
        snapshotManager.saveTableSchema(dbName, tmpCommon.getSchemaVersion(), tableName, tableSchema);

        server.getCommon().setSchemaVersion(tmpCommon.getSchemaVersion());
        server.getCommon().saveSchema(server.getClient(), server.getDataDir());
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
      short serializationVersionNumber = cobj.getShort(ComObject.Tag.serializationVersion);
      String dbName = cobj.getString(ComObject.Tag.dbName);
      String tableName = cobj.getString(ComObject.Tag.tableName).toLowerCase();
      String columnName = cobj.getString(ComObject.Tag.columnName).toLowerCase();
      String dataType = cobj.getString(ComObject.Tag.dataType);

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

        server.getCommon().saveSchema(server.getClient(), server.getDataDir());
      }

      if (!replayedCommand) {
        Random rand = new Random(System.currentTimeMillis());
        for (int i = 0; i < server.getShardCount(); i++) {
          ComObject slaveObj = new ComObject();

          slaveObj.put(ComObject.Tag.schemaBytes, server.getCommon().serializeSchema(DatabaseClient.SERIALIZATION_VERSION));
          slaveObj.put(ComObject.Tag.tableName, tableName);
          slaveObj.put(ComObject.Tag.dbName, dbName);
          slaveObj.put(ComObject.Tag.schemaVersion, server.getCommon().getSchemaVersion());
          slaveObj.put(ComObject.Tag.method, "SchemaManager:addColumnSlave");
          server.getDatabaseClient().send(null, i, rand.nextLong(), slaveObj, DatabaseClient.Replica.def);
        }
      }

      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.schemaBytes, server.getCommon().serializeSchema(serializationVersionNumber));

      return retObj;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  @SchemaWriteLock
  public ComObject createIndexSlave(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    String masterSlave = cobj.getString(ComObject.Tag.masterSlave);

    if (server.getShard() == 0 &&
        server.getCommon().getServersConfig().getShards()[0].getMasterReplica() == server.getReplica() &&
        masterSlave.equals("slave")) {
      return null;
    }

    synchronized (this) {
      DatabaseCommon tmpCommon = new DatabaseCommon();
      tmpCommon.deserializeSchema(cobj.getByteArray(ComObject.Tag.schemaBytes));
      String tableName = cobj.getString(ComObject.Tag.tableName);
      TableSchema tableSchema = tmpCommon.getTableSchema(dbName, tableName, server.getDataDir());
      ComArray array = cobj.getArray(ComObject.Tag.indices);
      for (int i = 0; i < array.getArray().size(); i++) {
        String indexName = (String) array.getArray().get(i);
        IndexSchema indexSchema = tableSchema.getIndexes().get(indexName);

        if (!server.getIndices(dbName).getIndices().containsKey(indexName)) {
          doCreateIndex(dbName, tableSchema, indexName, indexSchema.getFields());
        }

        server.getCommon().getTables(dbName).get(tableName).getIndexes().put(indexName, indexSchema);
        server.getCommon().getTables(dbName).get(tableName).getIndexesById().put(indexSchema.getIndexId(), indexSchema);

        SnapshotManager snapshotManager = server.getSnapshotManager();
        snapshotManager.saveIndexSchema(dbName, tmpCommon.getSchemaVersion(), tableSchema, indexSchema);
      }

      server.getCommon().saveSchema(server.getClient(), server.getDataDir());

      return null;
    }
  }

  @SchemaWriteLock
  public ComObject createIndex(ComObject cobj, boolean replayedCommand) {
    try {
      int schemaVersion = cobj.getInt(ComObject.Tag.schemaVersion);
      String dbName = cobj.getString(ComObject.Tag.dbName);
      if (schemaVersion < server.getSchemaVersion() && !replayedCommand) {
        throw new SchemaOutOfSyncException("currVer:" + server.getCommon().getSchemaVersion() + ":");
      }
      AtomicReference<String> table = new AtomicReference<>();
      String masterSlave = cobj.getString(ComObject.Tag.masterSlave);
      table.set(cobj.getString(ComObject.Tag.tableName));
      String indexName = cobj.getString(ComObject.Tag.indexName);
      boolean isUnique = cobj.getBoolean(ComObject.Tag.isUnique);
      String fieldsStr = cobj.getString(ComObject.Tag.fieldsStr);
      String[] fields = fieldsStr.split(",");

      List<String> createdIndices = null;

      server.getCommon().getSchemaWriteLock(dbName).lock();
      try {

        if (replayedCommand) {
          logger.info("replayedCommand: createIndex, table=" + table.get() + ", index=" + indexName);
          for (String field : fields) {
            Integer offset = server.getCommon().getTableSchema(dbName, table.get(), server.getDataDir()).getFieldOffset(field);
            if (offset == null) {
              throw new DatabaseException("Invalid field for index: indexName=" + indexName + ", field=" + field);
            }
          }

          String fullIndexName = indexName;

          if (server.getCommon().getTableSchema(dbName, table.get(), server.getDataDir()).getIndexes().containsKey(fullIndexName.toLowerCase())) {
            ComObject retObj = new ComObject();
            retObj.put(ComObject.Tag.schemaBytes, server.getCommon().serializeSchema(cobj.getShort(ComObject.Tag.serializationVersion)));
            return retObj;
          }
        }

        synchronized (this) {
          createdIndices = createIndex(dbName, table.get(), indexName, false, isUnique, fields);
          server.getCommon().saveSchema(server.getClient(), server.getDataDir());
        }
      }
      finally {
        server.getCommon().getSchemaWriteLock(dbName).unlock();
      }

      if (!replayedCommand && masterSlave.equals("master")) {
        cobj = new ComObject();
        cobj.put(ComObject.Tag.dbName, dbName);
        cobj.put(ComObject.Tag.schemaVersion, server.getCommon().getSchemaVersion());
        cobj.put(ComObject.Tag.method, "SchemaManager:createIndexSlave");
        cobj.put(ComObject.Tag.schemaBytes, server.getCommon().serializeSchema(DatabaseClient.SERIALIZATION_VERSION));
        cobj.put(ComObject.Tag.tableName, table.get());
        ComArray array = cobj.putArray(ComObject.Tag.indices, ComObject.Type.stringType);
        for (String currIndexName : createdIndices) {
          array.add(currIndexName);
        }
        cobj.put(ComObject.Tag.masterSlave, "slave");

        for (int i = 0; i < server.getShardCount(); i++) {
          server.getDatabaseClient().send(null, i, 0, cobj, DatabaseClient.Replica.def);
        }
      }

      if (!replayedCommand && createdIndices != null) {
        String primaryIndexName = null;
        TableSchema tableSchema = server.getCommon().getTableSchema(dbName, table.get(), server.getDataDir());
        for (Map.Entry<String, IndexSchema> entry : tableSchema.getIndexes().entrySet()) {
          if (entry.getValue().isPrimaryKey()) {
            primaryIndexName = entry.getKey();
          }
        }
        long totalSize = 0;
        if (primaryIndexName != null) {
          for (int shard = 0; shard < server.getShardCount(); shard++) {
            cobj = new ComObject();
            cobj.put(ComObject.Tag.dbName, dbName);
            cobj.put(ComObject.Tag.schemaVersion, server.getClient().getCommon().getSchemaVersion());
            byte[] response = server.getClient().send("PartitionManager:getIndexCounts", shard, 0, cobj, DatabaseClient.Replica.master);
            ComObject retObj = new ComObject(response);
            ComArray tables = retObj.getArray(ComObject.Tag.tables);
            if (tables != null) {
              for (int i = 0; i < tables.getArray().size(); i++) {
                ComObject tableObj = (ComObject) tables.getArray().get(i);
                String tableName = tableObj.getString(ComObject.Tag.tableName);
                if (tableName.equals(table.get())) {
                  ComArray indices = tableObj.getArray(ComObject.Tag.indices);
                  if (indices != null) {
                    for (int j = 0; j < indices.getArray().size(); j++) {
                      ComObject indexObj = (ComObject) indices.getArray().get(j);
                      String foundIndexName = indexObj.getString(ComObject.Tag.indexName);
                      if (primaryIndexName.equals(foundIndexName)) {
                        long size = indexObj.getLong(ComObject.Tag.size);
                        totalSize += size;
                      }
                    }
                  }
                }
              }

            }
          }
        }

        if (totalSize != 0) {
          for (String currIndexName : createdIndices) {
            cobj = new ComObject();
            cobj.put(ComObject.Tag.dbName, dbName);
            cobj.put(ComObject.Tag.schemaVersion, server.getCommon().getSchemaVersion());
            cobj.put(ComObject.Tag.tableName, table.get());
            cobj.put(ComObject.Tag.indexName, currIndexName);
            cobj.put(ComObject.Tag.method, "UpdateManager:populateIndex");
            for (int i = 0; i < server.getShardCount(); i++) {
              server.getDatabaseClient().send(null, i, 0, cobj, DatabaseClient.Replica.def);
            }
          }
        }
      }

      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.schemaBytes, server.getCommon().serializeSchema(cobj.getShort(ComObject.Tag.serializationVersion)));
      return retObj;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  @SchemaWriteLock
  public ComObject dropIndexSlave(ComObject cobj, boolean replayedCommand) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    String masterSlave = cobj.getString(ComObject.Tag.masterSlave);

    if (server.getShard() == 0 &&
        server.getCommon().getServersConfig().getShards()[0].getMasterReplica() == server.getReplica() &&
        masterSlave.equals("slave")) {
      return null;
    }

    String tableName = cobj.getString(ComObject.Tag.tableName);
    ComArray array = cobj.getArray(ComObject.Tag.indices);
    for (int i = 0; i < array.getArray().size(); i++) {
      String indexName = (String) array.getArray().get(i);
      server.removeIndex(dbName, tableName, indexName);
      SnapshotManager snapshotManager = server.getSnapshotManager();
      snapshotManager.deleteIndexSchema(dbName, server.getCommon().getSchemaVersion(), tableName, indexName);
    }

    DatabaseCommon tmpCommon = new DatabaseCommon();
    tmpCommon.deserializeSchema(cobj.getByteArray(ComObject.Tag.schemaBytes));
    if (tmpCommon.getSchemaVersion() > server.getCommon().getSchemaVersion()) {
      server.getCommon().deserializeSchema(cobj.getByteArray(ComObject.Tag.schemaBytes));
      server.getCommon().saveSchema(server.getClient(), server.getDataDir());
    }

    return null;
  }

  @SchemaWriteLock
  public ComObject dropIndex(ComObject cobj, boolean replayedCommand) {
    try {
      String masterSlave = cobj.getString(ComObject.Tag.masterSlave);
      if (server.getShard() == 0 &&
          server.getCommon().getServersConfig().getShards()[0].getMasterReplica() == server.getReplica() &&
          masterSlave.equals("slave")) {
        return null;
      }

      short serializationVersionNumber = cobj.getShort(ComObject.Tag.serializationVersion);
      int schemaVersion = cobj.getInt(ComObject.Tag.schemaVersion);
      String dbName = cobj.getString(ComObject.Tag.dbName);
      if (schemaVersion < server.getSchemaVersion() && !replayedCommand) {
        throw new SchemaOutOfSyncException("currVer:" + server.getCommon().getSchemaVersion() + ":");
      }
      String table = cobj.getString(ComObject.Tag.tableName);
      String indexName = cobj.getString(ComObject.Tag.indexName);

      List<IndexSchema> toDrop = new ArrayList<>();
      synchronized (this) {
        for (Map.Entry<String, IndexSchema> entry : server.getCommon().getTableSchema(dbName, table, server.getDataDir()).getIndices().entrySet()) {
          String name = entry.getValue().getName();
          String outerName = name;
          if (outerName.equals(indexName)) {
            toDrop.add(entry.getValue());
            if (entry.getValue().isPrimaryKey()) {
              throw new DatabaseException("Cannot drop primary key index: table=" + table + ", index=" + indexName);
            }
          }
        }

        for (IndexSchema indexSchema : toDrop) {
          server.removeIndex(dbName, table, indexSchema.getName());
          server.getCommon().getTableSchema(dbName, table, server.getDataDir()).getIndices().remove(indexSchema.getName());
          server.getCommon().getTableSchema(dbName, table, server.getDataDir()).getIndexesById().remove(indexSchema.getIndexId());
          SnapshotManager snapshotManager = server.getSnapshotManager();
          snapshotManager.deleteIndexSchema(dbName, server.getCommon().getSchemaVersion(), table, indexSchema.getName());
        }

        server.getCommon().saveSchema(server.getClient(), server.getDataDir());
      }

      cobj = new ComObject();
      cobj.put(ComObject.Tag.dbName, dbName);
      cobj.put(ComObject.Tag.schemaVersion, server.getCommon().getSchemaVersion());
      cobj.put(ComObject.Tag.method, "SchemaManager:dropIndexSlave");
      cobj.put(ComObject.Tag.schemaBytes, server.getCommon().serializeSchema(DatabaseClient.SERIALIZATION_VERSION));
      cobj.put(ComObject.Tag.tableName, table);
      cobj.put(ComObject.Tag.masterSlave, "slave");
      ComArray array = cobj.putArray(ComObject.Tag.indices, ComObject.Type.stringType);
      for (IndexSchema indexSchema : toDrop) {
        array.add(indexSchema.getName());
      }

      for (int i = 0; i < server.getShardCount(); i++) {
        for (int j = 0; j < server.getReplicationFactor(); j++) {
          server.getDatabaseClient().send(null, i, j, cobj, DatabaseClient.Replica.specified);
        }
      }

      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.schemaBytes, server.getCommon().serializeSchema(serializationVersionNumber));

      return retObj;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public void reconcileSchema() {
    try {
      if (server.getShard() != 0 || server.getReplica() != 0) {
        return;
      }
      logger.info("reconcile schema - begin");
      int threadCount = server.getShardCount() * server.getReplicationFactor();
      ThreadPoolExecutor executor = new ThreadPoolExecutor(threadCount, threadCount, 10_000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
      try {
        final ComObject highestSchemaVersions = readSchemaVersions();
        List<Future> futures = new ArrayList<>();
        for (int i = 0; i < server.getShardCount(); i++) {
          for (int j = 0; j < server.getReplicationFactor(); j++) {
            final int shard = i;
            final int replica = j;
            futures.add(executor.submit(new Callable() {
              @Override
              public Object call() throws Exception {
                long beginTime = System.currentTimeMillis();
                while (!server.getShutdown()) {
                  try {
                    if (server.getShard() == shard && server.getReplica() == replica) {
                      break;
                    }
                    ComObject cobj = new ComObject();
                    byte[] ret = server.getDatabaseClient().send("SchemaManager:getSchemaVersions", shard, replica, cobj, DatabaseClient.Replica.specified);
                    if (ret != null) {
                      ComObject retObj = new ComObject(ret);
                      retObj.put(ComObject.Tag.shard, shard);
                      retObj.put(ComObject.Tag.replica, replica);
                      return retObj;
                    }
                    Thread.sleep(1_000);
                  }
                  catch (InterruptedException e) {
                    return null;
                  }
                  catch (Exception e) {
                    logger.error("Error checking if server is healthy: shard=" + shard + ", replica=" + replica);
                  }
                  if (System.currentTimeMillis() - beginTime > 2 * 60 * 1000) {
                    logger.error("Server appears to be dead, skipping: shard=" + shard + ", replica=" + replica);
                    break;
                  }
                }
                return null;
              }
            }));

          }
        }
        try {
          List<ComObject> retObjs = new ArrayList<>();
          for (Future future : futures) {
            ComObject cobj = (ComObject) future.get();
            if (cobj != null) {
              retObjs.add(cobj);
            }
          }
          for (ComObject retObj : retObjs) {
            getHighestSchemaVersions(retObj.getInt(ComObject.Tag.shard), retObj.getInt(ComObject.Tag.replica), highestSchemaVersions, retObj);
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
      finally {
        executor.shutdownNow();
      }
      logger.info("reconcile schema - end");
    }
    catch (Exception e) {
      logger.error("Error reconciling schema", e);
    }
  }

  public ComObject getSchemaVersions(ComObject cobj, boolean replayedCommand) {
    return readSchemaVersions();
  }

  private void pushHighestSchema(ComObject highestSchemaVersions) {
    ComArray databases = highestSchemaVersions.getArray(ComObject.Tag.databases);
    for (int i = 0; i < databases.getArray().size(); i++) {
      ComObject db = (ComObject) databases.getArray().get(i);
      String currDbName = db.getString(ComObject.Tag.dbName);
      ComArray tables = db.getArray(ComObject.Tag.tables);
      for (int j = 0; j < tables.getArray().size(); j++) {
        ComObject table = (ComObject) tables.getArray().get(i);
        String currTableName = table.getString(ComObject.Tag.tableName);
        Boolean hasDiscrepency = table.getBoolean(ComObject.Tag.hasDiscrepancy);
        if (hasDiscrepency != null && hasDiscrepency) {
          logger.info("Table schema has discrepancy, will push schema: db=" + currDbName + ", table=" + currTableName);
          byte[] tableSchema = getHighestTableSchema(currDbName, currTableName, table.getInt(ComObject.Tag.shard),
              table.getInt(ComObject.Tag.replica));
          pushTableSchema(currDbName, currTableName, tableSchema, table.getInt(ComObject.Tag.schemaVersion));
        }
        ComArray indices = table.getArray(ComObject.Tag.indices);
        for (int k = 0; k < indices.getArray().size(); k++) {
          ComObject index = (ComObject) indices.getArray().get(k);
          String currIndexName = index.getString(ComObject.Tag.indexName);
          hasDiscrepency = index.getBoolean(ComObject.Tag.hasDiscrepancy);
          if (hasDiscrepency != null && hasDiscrepency) {
            byte[] indexSchema = getHighestIndexSchema(currDbName, currTableName, currIndexName, index.getInt(ComObject.Tag.shard),
                index.getInt(ComObject.Tag.replica));
            logger.info("Index schema has discrepancy, will push schema: db=" + currDbName + ", table=" + currTableName + ", index=" + currIndexName);
            pushIndexSchema(currDbName, currTableName, currIndexName, indexSchema, index.getInt(ComObject.Tag.schemaVersion));
          }
        }
      }
    }

  }

  private void pushIndexSchema(String currDbName, String currTableName, String currIndexName,
                               byte[] indexSchema, Integer schemaVersion) {
    try {
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.dbName, currDbName);
      cobj.put(ComObject.Tag.tableName, currTableName);
      cobj.put(ComObject.Tag.indexName, currIndexName);
      cobj.put(ComObject.Tag.indexSchema, indexSchema);
      cobj.put(ComObject.Tag.schemaVersion, schemaVersion);

      server.getDatabaseClient().sendToAllShards("SchemaManager:updateIndexSchema", 0, cobj, DatabaseClient.Replica.all);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }

  }

  private byte[] getHighestIndexSchema(String currDbName, String currTableName, String currIndexName, Integer shard, Integer replica) {
    try {
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.dbName, currDbName);
      cobj.put(ComObject.Tag.tableName, currTableName);
      cobj.put(ComObject.Tag.indexName, currIndexName);

      byte[] ret = server.getDatabaseClient().send("SchemaManager:getIndexSchema", shard, replica, cobj, DatabaseClient.Replica.specified);
      ComObject retObj = new ComObject(ret);
      return retObj.getByteArray(ComObject.Tag.indexSchema);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private void pushTableSchema(String currDbName, String currTableName, byte[] tableSchema, Integer schemaVersion) {
    try {
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.dbName, currDbName);
      cobj.put(ComObject.Tag.tableName, currTableName);
      cobj.put(ComObject.Tag.tableSchema, tableSchema);
      cobj.put(ComObject.Tag.schemaVersion, schemaVersion);

      server.getDatabaseClient().sendToAllShards("SchemaManager:updateTableSchema", 0, cobj, DatabaseClient.Replica.all);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  @SchemaWriteLock
  public ComObject updateIndexSchema(ComObject cobj, boolean replayedCommand) {
    try {
      String dbName = cobj.getString(ComObject.Tag.dbName);
      String tableName = cobj.getString(ComObject.Tag.tableName);
      String indexName = cobj.getString(ComObject.Tag.indexName);
      byte[] bytes = cobj.getByteArray(ComObject.Tag.indexSchema);
      int schemaVersion = cobj.getInt(ComObject.Tag.schemaVersion);

      logger.info("Updating schema for Index: db=" + dbName + ", table=" + tableName + ", index=" + indexName + ", schemaVersion=" + schemaVersion);

      File indexDir = server.getSnapshotManager().getIndexSchemaDir(dbName, tableName, indexName);
      File newSchemaFile = new File(indexDir, "schema." + schemaVersion + ".bin");
      File[] indexSchemas = indexDir.listFiles();
      if (indexSchemas != null && indexSchemas.length > 0) {
        sortSchemaFiles(indexSchemas);
        File indexSchemaFile = indexSchemas[indexSchemas.length - 1];
        String filename = indexSchemaFile.getName();
        int pos = filename.indexOf(".");
        int pos2 = filename.indexOf(".", pos + 1);
        int currSchemaVersion = Integer.valueOf(filename.substring(pos + 1, pos2));
        if (currSchemaVersion < schemaVersion) {
          newSchemaFile.delete();
          try (FileOutputStream out = new FileOutputStream(newSchemaFile)) {
            out.write(bytes);
          }
          server.getCommon().loadSchema(server.getDataDir());
        }
      }
      else {
        newSchemaFile.delete();
        newSchemaFile.getParentFile().mkdirs();
        try (FileOutputStream out = new FileOutputStream(newSchemaFile)) {
          out.write(bytes);
        }
        server.getCommon().loadSchema(server.getDataDir());
      }
      return null;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  @SchemaWriteLock
  public ComObject updateTableSchema(ComObject cobj, boolean replayedCommand) {
    try {

      String dbName = cobj.getString(ComObject.Tag.dbName);
      String tableName = cobj.getString(ComObject.Tag.tableName);
      byte[] bytes = cobj.getByteArray(ComObject.Tag.tableSchema);
      int schemaVersion = cobj.getInt(ComObject.Tag.schemaVersion);

      logger.info("Updating schema for table: db=" + dbName + ", table=" + tableName + ", schemaVersion=" + schemaVersion);

      File tableDir = server.getSnapshotManager().getTableSchemaDir(dbName, tableName);
      File newSchemaFile = new File(tableDir, "schema." + schemaVersion + ".bin");
      File[] tableSchemas = tableDir.listFiles();
      if (tableSchemas != null && tableSchemas.length > 0) {
        sortSchemaFiles(tableSchemas);
        File tableSchemaFile = tableSchemas[tableSchemas.length - 1];
        String filename = tableSchemaFile.getName();
        int pos = filename.indexOf(".");
        int pos2 = filename.indexOf(".", pos + 1);
        int currSchemaVersion = Integer.valueOf(filename.substring(pos + 1, pos2));
        if (currSchemaVersion < schemaVersion) {
          newSchemaFile.delete();
          try (FileOutputStream out = new FileOutputStream(newSchemaFile)) {
            out.write(bytes);
          }
          server.getCommon().loadSchema(server.getDataDir());
        }
      }
      else {
        newSchemaFile.delete();
        newSchemaFile.getParentFile().mkdirs();
        try (FileOutputStream out = new FileOutputStream(newSchemaFile)) {
          out.write(bytes);
        }
        server.getCommon().loadSchema(server.getDataDir());
      }
      return null;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  @SchemaReadLock
  public ComObject getIndexSchema(ComObject cobj, boolean replayedCommand) {
    try {
      String dbName = cobj.getString(ComObject.Tag.dbName);
      String tableName = cobj.getString(ComObject.Tag.tableName);
      String indexName = cobj.getString(ComObject.Tag.indexName);

      ComObject retObj = new ComObject();
      File indexDir = server.getSnapshotManager().getIndexSchemaDir(dbName, tableName, indexName);
      File[] indexSchemas = indexDir.listFiles();
      if (indexSchemas != null && indexSchemas.length > 0) {
        sortSchemaFiles(indexSchemas);
        File indexSchemaFile = indexSchemas[indexSchemas.length - 1];
        byte[] bytes = IOUtils.toByteArray(new FileInputStream(indexSchemaFile));
        retObj.put(ComObject.Tag.indexSchema, bytes);
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
      String dbName = cobj.getString(ComObject.Tag.dbName);
      String tableName = cobj.getString(ComObject.Tag.tableName);

      ComObject retObj = new ComObject();
      File tableDir = server.getSnapshotManager().getTableSchemaDir(dbName, tableName);
      File[] tableSchemas = tableDir.listFiles();
      if (tableSchemas != null && tableSchemas.length > 0) {
        sortSchemaFiles(tableSchemas);
        File tableSchemaFile = tableSchemas[tableSchemas.length - 1];
        byte[] bytes = IOUtils.toByteArray(new FileInputStream(tableSchemaFile));
        retObj.put(ComObject.Tag.tableSchema, bytes);
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
      cobj.put(ComObject.Tag.method, "SchemaManager:getTableSchema");
      cobj.put(ComObject.Tag.dbName, currDbName);
      cobj.put(ComObject.Tag.tableName, currTableName);

      byte[] ret = server.getDatabaseClient().send(null, shard, replica, cobj, DatabaseClient.Replica.specified);
      ComObject retObj = new ComObject(ret);
      return retObj.getByteArray(ComObject.Tag.tableSchema);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  ComObject readSchemaVersions() {
    ComObject cobj = new ComObject();
    Map<String, Schema> databasesMap = server.getCommon().getDatabases();
    ComArray databases = cobj.putArray(ComObject.Tag.databases, ComObject.Type.objectType);
    for (String dbName : databasesMap.keySet()) {
      ComObject dbObj = new ComObject();
      databases.add(dbObj);
      dbObj.put(ComObject.Tag.dbName, dbName);
      ComArray tables = dbObj.putArray(ComObject.Tag.tables, ComObject.Type.objectType);
      for (TableSchema tableSchema : server.getCommon().getTables(dbName).values()) {
        ComObject tableObj = new ComObject();
        tables.add(tableObj);
        tableObj.put(ComObject.Tag.tableName, tableSchema.getName());
        File tableDir = server.getSnapshotManager().getTableSchemaDir(dbName, tableSchema.getName());
        File[] tableSchemas = tableDir.listFiles();
        if (tableSchemas != null && tableSchemas.length > 0) {
          sortSchemaFiles(tableSchemas);
          File tableSchemaFile = tableSchemas[tableSchemas.length - 1];
          tableObj.put(ComObject.Tag.schemaVersion, getSchemVersionFromFile(tableSchemaFile));
        }
        else {
          tableObj.put(ComObject.Tag.schemaVersion, 0);
        }
        tableObj.put(ComObject.Tag.shard, server.getShard());
        tableObj.put(ComObject.Tag.replica, server.getReplica());
        ComArray indices = tableObj.putArray(ComObject.Tag.indices, ComObject.Type.objectType);
        for (IndexSchema indexSchema : tableSchema.getIndexes().values()) {
          ComObject indexObj = new ComObject();
          indices.add(indexObj);
          indexObj.put(ComObject.Tag.indexName, indexSchema.getName());
          File indexDir = server.getSnapshotManager().getIndexSchemaDir(dbName, tableSchema.getName(), indexSchema.getName());
          File[] indexSchemas = indexDir.listFiles();
          if (indexSchemas != null && indexSchemas.length > 0) {
            sortSchemaFiles(indexSchemas);
            File indexSchemaFile = indexSchemas[indexSchemas.length - 1];
            indexObj.put(ComObject.Tag.schemaVersion, getSchemVersionFromFile(indexSchemaFile));
          }
          else {
            indexObj.put(ComObject.Tag.schemaVersion, 0);
          }
          indexObj.put(ComObject.Tag.shard, server.getShard());
          indexObj.put(ComObject.Tag.replica, server.getReplica());
        }
      }
    }
    return cobj;
  }

  private int getSchemVersionFromFile(File schemaFile) {
    String name = schemaFile.getName();
    int pos = name.indexOf(".");
    int pos2 = name.indexOf(".", pos + 1);
    return Integer.valueOf(name.substring(pos + 1, pos2));
  }

  private void getHighestSchemaVersions(int shard, int replica, ComObject highestSchemaVersions, ComObject retObj) {
    ComArray databases = retObj.getArray(ComObject.Tag.databases);
    for (int i = 0; i < databases.getArray().size(); i++) {
      ComObject db = (ComObject) databases.getArray().get(i);
      String dbName = db.getString(ComObject.Tag.dbName);
      ComArray tables = db.getArray(ComObject.Tag.tables);
      ConcurrentHashMap<String, Integer> tablesFound = new ConcurrentHashMap<>();
      for (int j = 0; j < tables.getArray().size(); j++) {
        ComObject table = (ComObject) tables.getArray().get(j);
        String tableName = table.getString(ComObject.Tag.tableName);
        int tableSchemaVersion = table.getInt(ComObject.Tag.schemaVersion);
        tablesFound.put(tableName, tableSchemaVersion);
        ComObject highestTable = getSchemaVersion(dbName, tableName, highestSchemaVersions);
        if (highestTable == null) {
          setHighestSchemaVersion(dbName, tableName, tableSchemaVersion, shard, replica, highestSchemaVersions, tablesFound);
        }
        else {
          if (tableSchemaVersion > highestTable.getInt(ComObject.Tag.schemaVersion)) {
            setHighestSchemaVersion(dbName, tableName, tableSchemaVersion, shard, replica, highestSchemaVersions, tablesFound);
          }
          else if (tableSchemaVersion < highestTable.getInt(ComObject.Tag.schemaVersion)) {
            setHighestSchemaVersion(dbName, tableName, highestTable.getInt(ComObject.Tag.schemaVersion), highestTable.getInt(ComObject.Tag.shard),
                highestTable.getInt(ComObject.Tag.replica), highestSchemaVersions, tablesFound);
          }
          else {
            tablesFound.remove(tableName);
          }
        }
        ConcurrentHashMap<String, Integer> indicesFound = new ConcurrentHashMap<>();
        ComArray indices = table.getArray(ComObject.Tag.indices);
        for (int k = 0; k < indices.getArray().size(); k++) {
          ComObject index = (ComObject) indices.getArray().get(k);
          int indexSchemaVersion = index.getInt(ComObject.Tag.schemaVersion);
          String indexName = index.getString(ComObject.Tag.indexName);
          indicesFound.put(indexName, indexSchemaVersion);
          ComObject highestIndex = getSchemaVersion(dbName, tableName, indexName, highestSchemaVersions);
          if (highestIndex == null) {
            setHighestSchemaVersion(dbName, tableName, indexName, indexSchemaVersion, shard, replica, highestSchemaVersions, indicesFound);
          }
          else {
            if (indexSchemaVersion > highestIndex.getInt(ComObject.Tag.schemaVersion)) {
              setHighestSchemaVersion(dbName, tableName, indexName, indexSchemaVersion, shard, replica, highestSchemaVersions, indicesFound);
            }
            else if (indexSchemaVersion < highestIndex.getInt(ComObject.Tag.schemaVersion)) {
              setHighestSchemaVersion(dbName, tableName, indexName, highestIndex.getInt(ComObject.Tag.schemaVersion),
                  highestIndex.getInt(ComObject.Tag.shard), highestIndex.getInt(ComObject.Tag.replica), highestSchemaVersions, indicesFound);
            }
            else {
              indicesFound.remove(indexName);
            }
          }
        }
      }
    }
  }

  private ComObject getSchemaVersion(String dbName, String tableName, ComObject highestSchemaVersions) {
    ComArray databases = highestSchemaVersions.getArray(ComObject.Tag.databases);
    for (int i = 0; i < databases.getArray().size(); i++) {
      ComObject db = (ComObject) databases.getArray().get(i);
      String currDbName = db.getString(ComObject.Tag.dbName);
      if (currDbName.equals(dbName)) {
        ComArray tables = db.getArray(ComObject.Tag.tables);
        for (int j = 0; j < tables.getArray().size(); j++) {
          ComObject table = (ComObject) tables.getArray().get(j);
          String currTableName = table.getString(ComObject.Tag.tableName);
          if (currTableName.equals(tableName)) {
            return table;
          }
        }
      }
    }
    return null;
  }

  private void setHighestSchemaVersion(String dbName, String tableName, String indexName, int indexSchemaVersion,
                                       int shard, int replica, ComObject highestSchemaVersions, ConcurrentHashMap<String, Integer> indicesFound) {
    ComArray databases = highestSchemaVersions.getArray(ComObject.Tag.databases);
    for (int i = 0; i < databases.getArray().size(); i++) {
      ComObject db = (ComObject) databases.getArray().get(i);
      String currDbName = db.getString(ComObject.Tag.dbName);
      if (currDbName.equals(dbName)) {
        ComArray tables = db.getArray(ComObject.Tag.tables);
        for (int j = 0; j < tables.getArray().size(); j++) {
          ComObject table = (ComObject) tables.getArray().get(i);
          String currTableName = table.getString(ComObject.Tag.tableName);
          if (currTableName.equals(tableName)) {
            ComArray indices = table.getArray(ComObject.Tag.indices);
            for (int k = 0; k < indices.getArray().size(); k++) {
              ComObject index = (ComObject) indices.getArray().get(k);
              String currIndexName = index.getString(ComObject.Tag.indexName);
              indicesFound.remove(currIndexName);
              if (currIndexName.equals(indexName)) {
                index.put(ComObject.Tag.schemaVersion, indexSchemaVersion);
                index.put(ComObject.Tag.shard, shard);
                index.put(ComObject.Tag.replica, replica);
                index.put(ComObject.Tag.hasDiscrepancy, true);
                return;
              }
            }
            ComObject index = new ComObject();
            indices.add(index);
            index.put(ComObject.Tag.indexName, indexName);
            index.put(ComObject.Tag.schemaVersion, indexSchemaVersion);
            index.put(ComObject.Tag.shard, shard);
            index.put(ComObject.Tag.replica, replica);
            index.put(ComObject.Tag.hasDiscrepancy, true);

          }
        }
      }
    }
  }

  private ComObject getSchemaVersion(String dbName, String tableName, String indexName, ComObject highestSchemaVersions) {
    ComArray databases = highestSchemaVersions.getArray(ComObject.Tag.databases);
    for (int i = 0; i < databases.getArray().size(); i++) {
      ComObject db = (ComObject) databases.getArray().get(i);
      String currDbName = db.getString(ComObject.Tag.dbName);
      if (currDbName.equals(dbName)) {
        ComArray tables = db.getArray(ComObject.Tag.tables);
        for (int j = 0; j < tables.getArray().size(); j++) {
          ComObject table = (ComObject) tables.getArray().get(j);
          String currTableName = table.getString(ComObject.Tag.tableName);
          if (currTableName.equals(tableName)) {
            ComArray indices = table.getArray(ComObject.Tag.indices);
            for (int k = 0; k < indices.getArray().size(); k++) {
              ComObject index = (ComObject) indices.getArray().get(k);
              String currIndexName = index.getString(ComObject.Tag.indexName);
              if (currIndexName.equals(indexName)) {
                return index;
              }
            }
          }
        }
      }
    }
    return null;
  }

  private void setHighestSchemaVersion(String dbName, String tableName, int tableSchemaVersion, int shard,
                                       int replica, ComObject highestSchemaVersions, ConcurrentHashMap<String, Integer> tablesFound) {
    ComArray databases = highestSchemaVersions.getArray(ComObject.Tag.databases);
    for (int i = 0; i < databases.getArray().size(); i++) {
      ComObject db = (ComObject) databases.getArray().get(i);
      String currDbName = db.getString(ComObject.Tag.dbName);
      if (currDbName.equals(dbName)) {
        ComArray tables = db.getArray(ComObject.Tag.tables);

        for (int j = 0; j < tables.getArray().size(); j++) {
          ComObject table = (ComObject) tables.getArray().get(i);
          String currTableName = table.getString(ComObject.Tag.tableName);
          tablesFound.remove(currTableName);
          if (currTableName.equals(tableName)) {
            table.put(ComObject.Tag.schemaVersion, tableSchemaVersion);
            table.put(ComObject.Tag.shard, shard);
            table.put(ComObject.Tag.replica, replica);
            table.put(ComObject.Tag.hasDiscrepancy, true);
            return;
          }
        }
        ComObject table = new ComObject();
        tables.add(table);
        table.put(ComObject.Tag.tableName, tableName);
        table.put(ComObject.Tag.schemaVersion, tableSchemaVersion);
        table.put(ComObject.Tag.shard, shard);
        table.put(ComObject.Tag.replica, replica);
        table.put(ComObject.Tag.hasDiscrepancy, true);
      }
    }
  }

}
