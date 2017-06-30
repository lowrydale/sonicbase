package com.sonicbase.server;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.ComArray;
import com.sonicbase.common.ComObject;
import com.sonicbase.common.Logger;
import com.sonicbase.common.SchemaOutOfSyncException;
import com.sonicbase.index.Indices;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.query.impl.CreateTableStatementImpl;
import com.sonicbase.schema.DataType;
import com.sonicbase.schema.FieldSchema;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.DataUtil;
import javafx.scene.control.Tab;

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
    this.logger = new Logger(databaseServer.getDatabaseClient());
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

    TableSchema tableSchema = server.getCommon().getTables(dbName).get(table.toLowerCase());
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

    tableSchema.addIndex(indexName, isUnique, fields, partitions, highIndexId);

    server.getCommon().updateTable(dbName, server.getDataDir(), tableSchema);

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

  public byte[] createDatabase(ComObject cobj, boolean replayedCommand) {
    try {
      if (server.getShard() == 0 &&
          server.getCommon().getServersConfig().getShards()[0].getMasterReplica() == server.getReplica() && cobj.getBoolean(ComObject.Tag.slave) != null) {
        return null;
      }
      System.out.println("Create database: shard=" + server.getShard() + ", replica=" + server.getReplica());
      String dbName = cobj.getString(ComObject.Tag.dbName);
      String masterSlave = cobj.getString(ComObject.Tag.masterSlave);
      dbName = dbName.toLowerCase();

      if (replayedCommand && null != server.getCommon().getSchema(dbName)) {
        return null;
      }

//      if (null != server.getCommon().getSchema(dbName)) {
//        throw new DatabaseException("Database already exists: name=" + dbName);
//      }

      logger.info("Create database: shard=" + server.getShard() + ", replica=" + server.getReplica() + ", name=" + dbName);
      File dir = new File(server.getDataDir(), "snapshot/" + server.getShard() + "/" + server.getReplica() + "/" + dbName);
      if (!dir.exists() && !dir.mkdirs()) {
        throw new DatabaseException("Error creating database directory: dir=" + dir.getAbsolutePath());
      }

      server.getIndices().put(dbName, new Indices());
      server.getCommon().addDatabase(dbName);
      server.getCommon().saveSchema(server.getDataDir());

      if (cobj.getBoolean(ComObject.Tag.slave) == null) {
        for (int i = 0; i < server.getShardCount(); i++) {
          cobj.put(ComObject.Tag.slave, true);
          cobj.put(ComObject.Tag.masterSlave, "slave");
          String command = "DatabaseServer:ComObject:createDatabase:";
          server.getDatabaseClient().send(null, i, 0, command, cobj.serialize(), DatabaseClient.Replica.def);
        }
        server.pushSchema();
      }
    }
    finally {

    }
    return null;
  }

  public byte[] dropTable(ComObject cobj, boolean replayedCommand) {
    String masterSlave = cobj.getString(ComObject.Tag.masterSlave);
    if (server.getShard() == 0 &&
        server.getCommon().getServersConfig().getShards()[0].getMasterReplica() == server.getReplica() &&
        masterSlave.equals("slave")) {
      return null;
    }
    try {
      long serializationVersionNumber = cobj.getLong(ComObject.Tag.serializationVersion);
      String dbName = cobj.getString(ComObject.Tag.dbName);
      String tableName = cobj.getString(ComObject.Tag.tableName);

      server.getCommon().getSchemaWriteLock(dbName).lock();
      try {
        server.getCommon().dropTable(dbName, tableName, server.getDataDir());
        server.getIndices().get(dbName).getIndices().remove(tableName);
      }
      finally {
        server.getCommon().getSchemaWriteLock(dbName).unlock();
      }

      if (masterSlave.equals("master")) {
        Random rand = new Random(System.currentTimeMillis());
        for (int i = 0; i < server.getShardCount(); i++) {

          cobj.put(ComObject.Tag.masterSlave, "slave");
          String command = "DatabaseServer:ComObject:dropTable:";
          byte[] ret = server.getDatabaseClient().send(null, i, rand.nextLong(), command, cobj.serialize(), DatabaseClient.Replica.def);
        }
        server.pushSchema();
      }

      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.schemaBytes, server.getCommon().serializeSchema(serializationVersionNumber));
      return retObj.serialize();
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public byte[] createTableSlave(ComObject cobj, boolean replayedCommand) {
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
      server.getCommon().deserializeSchema(bytes);
      String tableName = cobj.getString(ComObject.Tag.tableName);
      TableSchema tableSchema = server.getCommon().getTables(dbName).get(tableName);
      Map<String, IndexSchema> indices = tableSchema.getIndexes();
      for (Map.Entry<String, IndexSchema> index : indices.entrySet()) {
        doCreateIndex(dbName, tableSchema, index.getKey(), index.getValue().getFields());
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

  public byte[] createTable(ComObject cobj, boolean replayedCommand) {
    try {
      String masterSlave = cobj.getString(ComObject.Tag.masterSlave);
      if (server.getShard() == 0 &&
          server.getCommon().getServersConfig().getShards()[0].getMasterReplica() == server.getReplica() &&
          masterSlave.equals("slave")) {
        return null;
      }
      long serializationVersionNumber = cobj.getLong(ComObject.Tag.serializationVersion);
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

        TableSchema schema = new TableSchema();
        tableName = createTableStatement.getTablename();
        if (replayedCommand) {
          logger.info("replayedCommand: table=" + tableName + ", tableCount=" + server.getCommon().getTables(dbName).size());
          if (server.getCommon().getTables(dbName).containsKey(tableName.toLowerCase())) {
            ComObject retObj = new ComObject();
            retObj.put(ComObject.Tag.schemaBytes, server.getCommon().serializeSchema(serializationVersionNumber));

            return retObj.serialize();
          }
        }
        if (server.getCommon().getTables(dbName).containsKey(tableName.toLowerCase())) {
          throw new DatabaseException("Table already exists: name=" + tableName);
        }


        List<String> primaryKey = createTableStatement.getPrimaryKey();
        List<FieldSchema> fields = createTableStatement.getFields();
        List<FieldSchema> actualFields = new ArrayList<>();
        FieldSchema idField = new FieldSchema();
        idField.setAutoIncrement(true);
        idField.setName("_id");
        idField.setType(DataType.Type.BIGINT);
        actualFields.add(idField);
        actualFields.addAll(fields);
        schema.setFields(actualFields);
        schema.setName(tableName.toLowerCase());
        schema.setPrimaryKey(primaryKey);

        Map<Integer, TableSchema> tables = server.getCommon().getTablesById(dbName);
        int highTableId = 0;
        for (int id : tables.keySet()) {
          highTableId = Math.max(highTableId, id);
        }
        highTableId++;
        schema.setTableId(highTableId);

        server.getCommon().addTable(dbName, server.getDataDir(), schema);

        String[] primaryKeyFields = primaryKey.toArray(new String[primaryKey.size()]);
        createIndex(dbName, tableName.toLowerCase(), "_primarykey", true, primaryKeyFields);
      }
      finally {
        server.getCommon().getSchemaWriteLock(dbName).unlock();
      }

      if (masterSlave.equals("master")) {
        Random rand = new Random(System.currentTimeMillis());
        for (int i = 0; i < server.getShardCount(); i++) {
          //        if (i == shard) {
          //          continue;
          //        }

          ComObject slaveObj = new ComObject();

          slaveObj.put(ComObject.Tag.schemaBytes, server.getCommon().serializeSchema(SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION));
          slaveObj.put(ComObject.Tag.tableName, tableName);
          slaveObj.put(ComObject.Tag.dbName, dbName);
          slaveObj.put(ComObject.Tag.schemaVersion, server.getCommon().getSchemaVersion());
          slaveObj.put(ComObject.Tag.method, "createTableSlave");
          slaveObj.put(ComObject.Tag.masterSlave, "slave");
          String command = "DatabaseServer:ComObject:createTableSlave:";
          server.getDatabaseClient().send(null, i, rand.nextLong(), command, slaveObj.serialize(), DatabaseClient.Replica.def);
        }
      }

      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.schemaBytes, server.getCommon().serializeSchema(serializationVersionNumber));

      return retObj.serialize();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public byte[] dropColumn(ComObject cobj) {

    try {
      long serializationVersionNumber = cobj.getLong(ComObject.Tag.serializationVersion);
      String dbName = cobj.getString(ComObject.Tag.dbName);
      String tableName = cobj.getString(ComObject.Tag.tableName).toLowerCase();
      String columnName = cobj.getString(ComObject.Tag.columnName).toLowerCase();

      TableSchema tableSchema = server.getCommon().getTables(dbName).get(tableName);
      tableSchema.saveFields(server.getCommon().getSchemaVersion());

      for (FieldSchema fieldSchema : tableSchema.getFields()) {
        if (fieldSchema.getName().equals(columnName)) {
          tableSchema.getFields().remove(fieldSchema);
          break;
        }
      }

      server.getCommon().saveSchema(server.getDataDir());

      server.pushSchema();

      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.schemaBytes, server.getCommon().serializeSchema(serializationVersionNumber));

      return retObj.serialize();
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public byte[] addColumn(ComObject cobj) {

    try {
      long serializationVersionNumber = cobj.getLong(ComObject.Tag.serializationVersion);
      String dbName = cobj.getString(ComObject.Tag.dbName);
      String tableName = cobj.getString(ComObject.Tag.tableName).toLowerCase();
      String columnName = cobj.getString(ComObject.Tag.columnName).toLowerCase();
      String dataType = cobj.getString(ComObject.Tag.dataType);

      TableSchema tableSchema = server.getCommon().getTables(dbName).get(tableName);
      tableSchema.saveFields(server.getCommon().getSchemaVersion());

      FieldSchema fieldSchema = new FieldSchema();
      fieldSchema.setType(DataType.Type.valueOf(dataType));
      fieldSchema.setName(columnName);
      tableSchema.addField(fieldSchema);
      tableSchema.markChangesComplete();

      server.getCommon().saveSchema(server.getDataDir());

      server.pushSchema();

      ComObject retObj = new ComObject();
      retObj.put(ComObject.Tag.schemaBytes, server.getCommon().serializeSchema(serializationVersionNumber));

      return retObj.serialize();
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public byte[] createIndexSlave(ComObject cobj) {
    String dbName = cobj.getString(ComObject.Tag.dbName);
    String masterSlave = cobj.getString(ComObject.Tag.masterSlave);

    if (server.getShard() == 0 &&
        server.getCommon().getServersConfig().getShards()[0].getMasterReplica() == server.getReplica() &&
        masterSlave.equals("slave")) {
      return null;
    }

    server.getCommon().deserializeSchema(cobj.getByteArray(ComObject.Tag.schemaBytes));
    server.getCommon().saveSchema(server.getDataDir());

    String tableName = cobj.getString(ComObject.Tag.tableName);
    TableSchema tableSchema = server.getCommon().getTables(dbName).get(tableName);
    ComArray array = cobj.getArray(ComObject.Tag.indices);
    for (int i = 0; i < array.getArray().size(); i++) {
      String indexName = (String)array.getArray().get(i);
      IndexSchema indexSchema = tableSchema.getIndexes().get(indexName);

      if (!server.getIndices(dbName).getIndices().containsKey(indexName)) {
        doCreateIndex(dbName, tableSchema, indexName, indexSchema.getFields());
      }
    }
    return null;
  }

  public List<String> createIndex(ComObject cobj, boolean replayedCommand, AtomicReference<String> table) {
    try {
      int schemaVersion = cobj.getInt(ComObject.Tag.schemaVersion);
      String dbName = cobj.getString(ComObject.Tag.dbName);
      if (schemaVersion < server.getSchemaVersion()) {
        throw new SchemaOutOfSyncException("currVer:" + server.getCommon().getSchemaVersion() + ":");
      }
      String masterSlave = cobj.getString(ComObject.Tag.masterSlave);
      table.set(cobj.getString(ComObject.Tag.tableName));
      String indexName = cobj.getString(ComObject.Tag.indexName);
      boolean isUnique = cobj.getBoolean(ComObject.Tag.isUnique);
      String fieldsStr = cobj.getString(ComObject.Tag.fieldsStr);
      String[] fields = fieldsStr.split(",");

      if (replayedCommand) {
        logger.info("replayedCommand: createIndex, table=" + table.get() + ", index=" + indexName);
        if (server.getCommon().getTables(dbName).get(table.get()).getIndexes().containsKey(indexName.toLowerCase())) {
//          ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
//          DataOutputStream out = new DataOutputStream(bytesOut);
//          server.getCommon().serializeSchema(out, serializationVersionNumber);
//          out.close();

          return null;
        }
      }

      List<String> createdIndices = createIndex(dbName, table.get(), indexName, isUnique, fields);

      if (masterSlave.equals("master")) {

        cobj = new ComObject();
        cobj.put(ComObject.Tag.dbName, dbName);
        cobj.put(ComObject.Tag.schemaVersion, server.getCommon().getSchemaVersion());
        cobj.put(ComObject.Tag.method, "createIndexSlave");
        cobj.put(ComObject.Tag.schemaBytes, server.getCommon().serializeSchema(SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION));
        cobj.put(ComObject.Tag.tableName, table.get());
        ComArray array = cobj.putArray(ComObject.Tag.indices, ComObject.Type.stringType);
        for (String currIndexName : createdIndices) {
          array.add(currIndexName);
        }
        cobj.put(ComObject.Tag.masterSlave, "slave");

        byte[] slaveBody = cobj.serialize();

        for (int i = 0; i < server.getShardCount(); i++) {
          for (int j = 0; j < server.getReplicationFactor(); j++) {
//            if (i == 0 && server.getCommon().getServersConfig().getShards()[0].getMasterReplica() == j) {
//              continue;
//            }
            String command = "DatabaseServer:ComObject:createIndexSlave:";
            server.getDatabaseClient().send(null, i, j, command, slaveBody, DatabaseClient.Replica.specified);
          }
        }
      }
      return createdIndices;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }


  public byte[] dropIndexSlave(ComObject cobj) {
     String dbName = cobj.getString(ComObject.Tag.dbName);
     String masterSlave = cobj.getString(ComObject.Tag.masterSlave);

     if (server.getShard() == 0 &&
         server.getCommon().getServersConfig().getShards()[0].getMasterReplica() == server.getReplica() &&
         masterSlave.equals("slave")) {
       return null;
     }

     server.getCommon().deserializeSchema(cobj.getByteArray(ComObject.Tag.schemaBytes));
     server.getCommon().saveSchema(server.getDataDir());

     String tableName = cobj.getString(ComObject.Tag.tableName);
     ComArray array = cobj.getArray(ComObject.Tag.indices);
     for (int i = 0; i < array.getArray().size(); i++) {
       String indexName = (String)array.getArray().get(i);
       server.getIndices().get(dbName).getIndices().get(tableName).remove(indexName);
     }
     return null;
   }

  public byte[] dropIndex(ComObject cobj) {
      try {
        String masterSlave = cobj.getString(ComObject.Tag.masterSlave);
        if (server.getShard() == 0 &&
            server.getCommon().getServersConfig().getShards()[0].getMasterReplica() == server.getReplica() &&
            masterSlave.equals("slave")) {
          return null;
        }

        long serializationVersionNumber = cobj.getLong(ComObject.Tag.serializationVersion);
        int schemaVersion = cobj.getInt(ComObject.Tag.schemaVersion);
        String dbName = cobj.getString(ComObject.Tag.dbName);
        if (schemaVersion < server.getSchemaVersion()) {
          throw new SchemaOutOfSyncException("currVer:" + server.getCommon().getSchemaVersion() + ":");
        }
        String table = cobj.getString(ComObject.Tag.tableName);
        String indexName = cobj.getString(ComObject.Tag.indexName);

        List<IndexSchema> toDrop = new ArrayList<>();
        for (Map.Entry<String, IndexSchema> entry : server.getCommon().getTables(dbName).get(table).getIndices().entrySet()) {
          String name = entry.getValue().getName();
          int pos = name.indexOf("_", 1);
          String outerName = name.substring(pos + 1);
          if (outerName.equals(indexName)) {
            toDrop.add(entry.getValue());
          }
        }

        for (IndexSchema indexSchema : toDrop) {
          server.getIndices().get(dbName).getIndices().get(table).remove(indexSchema.getName());
          server.getCommon().getTables(dbName).get(table).getIndices().remove(indexSchema.getName());
        }

        server.getCommon().saveSchema(server.getDataDir());

        cobj = new ComObject();
        cobj.put(ComObject.Tag.dbName, dbName);
        cobj.put(ComObject.Tag.schemaVersion, server.getCommon().getSchemaVersion());
        cobj.put(ComObject.Tag.method, "dropIndexSlave");
        cobj.put(ComObject.Tag.schemaBytes, server.getCommon().serializeSchema(SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION));
        cobj.put(ComObject.Tag.tableName, table);
        cobj.put(ComObject.Tag.masterSlave, "slave");
        ComArray array = cobj.putArray(ComObject.Tag.indices, ComObject.Type.stringType);
        for (IndexSchema indexSchema : toDrop) {
          array.add(indexSchema.getName());
        }

        byte[] slaveBytes = cobj.serialize();

        for (int i = 0; i < server.getShardCount(); i++) {
          for (int j = 0; j < server.getReplicationFactor(); j++) {
//            if (i == 0 && server.getCommon().getServersConfig().getShards()[0].getMasterReplica() == j) {
//              continue;
//            }
            String command = "DatabaseServer:ComObject:dropIndexSlave:";
            server.getDatabaseClient().send(null, i, j, command, slaveBytes, DatabaseClient.Replica.specified);
          }
        }

        ComObject retObj = new ComObject();
        retObj.put(ComObject.Tag.schemaBytes, server.getCommon().serializeSchema(serializationVersionNumber));

        return retObj.serialize();
      }
      catch (IOException e) {
        throw new DatabaseException(e);
      }
    }

}
