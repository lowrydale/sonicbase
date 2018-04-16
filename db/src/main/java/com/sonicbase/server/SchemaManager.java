package com.sonicbase.server;

import com.sonicbase.client.DatabaseClient;

import com.sonicbase.index.Indices;

import com.sonicbase.common.*;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.query.impl.CreateTableStatementImpl;
import com.sonicbase.schema.DataType;
import com.sonicbase.schema.FieldSchema;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Responsible for
 */
public class SchemaManager {

  private Logger logger;

  private final DatabaseServer server;

  public SchemaManager(DatabaseServer databaseServer) {
    this.server = databaseServer;
    this.logger = new Logger(null/*databaseServer.getDatabaseClient()*/);
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

  private ConcurrentHashMap<String, AutoIncrementValue> autoIncrementValues = new ConcurrentHashMap<String, AutoIncrementValue>();

  private List<String> createIndex(String dbName, String table, String indexName, boolean isUnique, String[] fields) {
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

    indexName = "_" + fields.length + "_" + indexName;

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

    IndexSchema indexSchema = tableSchema.addIndex(indexName, isUnique, fields, partitions, highIndexId);

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
      if (masterSlave.equals("master")) {
        for (int i = 0; i < server.getShardCount(); i++) {
          cobj.put(ComObject.Tag.slave, true);
          cobj.put(ComObject.Tag.masterSlave, "slave");
          cobj.put(ComObject.Tag.method, "createDatabaseSlave");
          server.getDatabaseClient().send(null, i, 0, cobj, DatabaseClient.Replica.def);
        }
        server.pushSchema();
      }
    }
    finally {

    }
    return null;
  }

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


        //    int schemaVersion = Integer.valueOf(parts[3]);
        //    if (schemaVersion != common.getSchemaVersion()) {
        //      throw new SchemaOutOfSyncException();
        //    }

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
            createIndex(dbName, tableName.toLowerCase(), "_primarykey", true, primaryKeyFields);

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
          //        if (i == shard) {
          //          continue;
          //        }

          ComObject slaveObj = new ComObject();

          slaveObj.put(ComObject.Tag.schemaBytes, server.getCommon().serializeSchema(DatabaseClient.SERIALIZATION_VERSION));
          slaveObj.put(ComObject.Tag.tableName, tableName);
          slaveObj.put(ComObject.Tag.dbName, dbName);
          slaveObj.put(ComObject.Tag.schemaVersion, server.getCommon().getSchemaVersion());
          slaveObj.put(ComObject.Tag.method, "createTableSlave");
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
          slaveObj.put(ComObject.Tag.method, "dropColumnSlave");
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
          slaveObj.put(ComObject.Tag.method, "addColumnSlave");
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

  public ComObject createIndexSlave(ComObject cobj) {
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
//      if (tmpCommon.getSchemaVersion() > server.getCommon().getSchemaVersion()) {
//        server.getCommon().deserializeSchema(cobj.getByteArray(ComObject.Tag.schemaBytes));
//        server.getCommon().saveSchema(server.getClient(), server.getDataDir());
//      }

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
        snapshotManager.saveIndexSchema(dbName, server.getCommon().getSchemaVersion(), tableSchema, indexSchema);
      }

      server.getCommon().saveSchema(server.getClient(), server.getDataDir());

      return null;
    }
  }

  public ComObject createIndex(ComObject cobj, boolean replayedCommand) {
    try {
      int schemaVersion = cobj.getInt(ComObject.Tag.schemaVersion);
      String dbName = cobj.getString(ComObject.Tag.dbName);
      if (schemaVersion < server.getSchemaVersion()) {
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

          String fullIndexName = "_" + fields.length + "_" + indexName;

          if (server.getCommon().getTableSchema(dbName, table.get(), server.getDataDir()).getIndexes().containsKey(fullIndexName.toLowerCase())) {
            ComObject retObj = new ComObject();
            retObj.put(ComObject.Tag.schemaBytes, server.getCommon().serializeSchema(cobj.getShort(ComObject.Tag.serializationVersion)));
            return retObj;
          }
        }

        synchronized (this) {
          createdIndices = createIndex(dbName, table.get(), indexName, isUnique, fields);
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
        cobj.put(ComObject.Tag.method, "createIndexSlave");
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
            cobj.put(ComObject.Tag.method, "getIndexCounts");
            byte[] response = server.getClient().send(null, shard, 0, cobj, DatabaseClient.Replica.master);
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
            cobj.put(ComObject.Tag.method, "populateIndex");
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


  public ComObject dropIndexSlave(ComObject cobj) {
     String dbName = cobj.getString(ComObject.Tag.dbName);
     String masterSlave = cobj.getString(ComObject.Tag.masterSlave);

     if (server.getShard() == 0 &&
         server.getCommon().getServersConfig().getShards()[0].getMasterReplica() == server.getReplica() &&
         masterSlave.equals("slave")) {
       return null;
     }

     DatabaseCommon tmpCommon = new DatabaseCommon();
     tmpCommon.deserializeSchema(cobj.getByteArray(ComObject.Tag.schemaBytes));
     if (tmpCommon.getSchemaVersion() > server.getCommon().getSchemaVersion()) {
       server.getCommon().deserializeSchema(cobj.getByteArray(ComObject.Tag.schemaBytes));
       server.getCommon().saveSchema(server.getClient(), server.getDataDir());
     }

     String tableName = cobj.getString(ComObject.Tag.tableName);
     ComArray array = cobj.getArray(ComObject.Tag.indices);
     for (int i = 0; i < array.getArray().size(); i++) {
       String indexName = (String)array.getArray().get(i);
       server.getIndices().get(dbName).getIndices().get(tableName).remove(indexName);
     }
     return null;
   }

  public ComObject dropIndex(ComObject cobj) {
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
        if (schemaVersion < server.getSchemaVersion()) {
          throw new SchemaOutOfSyncException("currVer:" + server.getCommon().getSchemaVersion() + ":");
        }
        String table = cobj.getString(ComObject.Tag.tableName);
        String indexName = cobj.getString(ComObject.Tag.indexName);

        List<IndexSchema> toDrop = new ArrayList<>();
        synchronized (this) {
          for (Map.Entry<String, IndexSchema> entry : server.getCommon().getTableSchema(dbName, table, server.getDataDir()).getIndices().entrySet()) {
            String name = entry.getValue().getName();
            int pos = name.indexOf("_", 1);
            String outerName = name.substring(pos + 1);
            if (outerName.equals(indexName)) {
              toDrop.add(entry.getValue());
              if (entry.getValue().isPrimaryKey()) {
                throw new DatabaseException("Cannot drop primary key index: table=" + table + ", index=" + indexName);
              }
            }
          }

          for (IndexSchema indexSchema : toDrop) {
            server.getIndices().get(dbName).getIndices().get(table).remove(indexSchema.getName());
            server.getCommon().getTableSchema(dbName, table, server.getDataDir()).getIndices().remove(indexSchema.getName());
            SnapshotManager snapshotManager = server.getSnapshotManager();
            snapshotManager.deleteIndexSchema(dbName, server.getCommon().getSchemaVersion(), table, indexSchema.getName());
          }

          server.getCommon().saveSchema(server.getClient(), server.getDataDir());
        }

        cobj = new ComObject();
        cobj.put(ComObject.Tag.dbName, dbName);
        cobj.put(ComObject.Tag.schemaVersion, server.getCommon().getSchemaVersion());
        cobj.put(ComObject.Tag.method, "dropIndexSlave");
        cobj.put(ComObject.Tag.schemaBytes, server.getCommon().serializeSchema(DatabaseClient.SERIALIZATION_VERSION));
        cobj.put(ComObject.Tag.tableName, table);
        cobj.put(ComObject.Tag.masterSlave, "slave");
        ComArray array = cobj.putArray(ComObject.Tag.indices, ComObject.Type.stringType);
        for (IndexSchema indexSchema : toDrop) {
          array.add(indexSchema.getName());
        }

        for (int i = 0; i < server.getShardCount(); i++) {
          for (int j = 0; j < server.getReplicationFactor(); j++) {
//            if (i == 0 && server.getCommon().getServersConfig().getShards()[0].getMasterReplica() == j) {
//              continue;
//            }
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

}
