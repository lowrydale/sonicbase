package com.sonicbase.server;

import com.sonicbase.client.DatabaseClient;
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

  public byte[] createDatabase(String command, byte[] body, boolean replayedCommand) {
    try {
      if (server.getShard() == 0 && server.getReplica() == 0 && command.contains(":slave")) {
        return null;
      }
      String[] parts = command.split(":");
      String dbName = parts[5];
      String masterSlave = parts[6];
      dbName = dbName.toLowerCase();

      if (replayedCommand && null != server.getCommon().getSchema(dbName)) {
        return null;
      }

      if (null != server.getCommon().getSchema(dbName)) {
        throw new DatabaseException("Database already exists: name=" + dbName);
      }

      logger.info("Create database: shard=" + server.getShard() + ", replica=" + server.getReplica() + ", name=" + dbName);
      File dir = new File(server.getDataDir(), "snapshot/" + server.getShard() + "/" + server.getReplica() + "/" + dbName);
      if (!dir.exists() && !dir.mkdirs()) {
        throw new DatabaseException("Error creating database directory: dir=" + dir.getAbsolutePath());
      }

      server.getIndices().put(dbName, new Indices());
      server.getCommon().addDatabase(dbName);
      server.getCommon().saveSchema(server.getDataDir());

      if (masterSlave.equals("master")) {
        for (int i = 0; i < server.getShardCount(); i++) {
          command = command.replace(":master", ":slave");
          server.getDatabaseClient().send(null, i, 0, command, body, DatabaseClient.Replica.all);
        }
        server.pushSchema();
      }
    }
    finally {

    }
    return null;
  }

  public byte[] dropTable(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    if (server.getShard() == 0 && server.getReplica() == 0 && command.contains(":slave")) {
      return null;
    }
    try {
      int serializationVersionNumber = Integer.valueOf(parts[3]);
      String dbName = parts[5];
      String tableName = parts[6];
      String masterSlave = parts[7];

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

          command = command.replace(":master", ":slave");
          byte[] ret = server.getDatabaseClient().send(null, i, rand.nextLong(), command, body, DatabaseClient.Replica.all);
        }
        server.pushSchema();
      }

      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytesOut);
      server.getCommon().serializeSchema(out, serializationVersionNumber);
      out.close();

      return bytesOut.toByteArray();
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public byte[] createTableSlave(String command, byte[] body, boolean replayedCommand) {
    String[] parts = command.split(":");
    String dbName = parts[5];

    if (server.getShard() == 0 && server.getReplica() == 0 && command.contains(":slave")) {
      return null;
    }

    server.getCommon().getSchemaWriteLock(dbName).lock();
    try {
      ByteArrayInputStream bytesIn = new ByteArrayInputStream(body);
      DataInputStream in = new DataInputStream(bytesIn);
      server.getCommon().deserializeSchema(server.getCommon(), in);
      String tableName = in.readUTF();
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

  public byte[] createTable(String command, byte[] body, boolean replayedCommand) {
    try {
      if (server.getShard() == 0 && server.getReplica() == 0 && command.contains(":slave:")) {
        return null;
      }
      String[] parts = command.split(":");
      int serializationVersionNumber = Integer.valueOf(parts[3]);
      String dbName = parts[5];
      String masterSlave = parts[6];

      String tableName = null;
      server.getCommon().getSchemaWriteLock(dbName).lock();
      try {


        //    int schemaVersion = Integer.valueOf(parts[3]);
        //    if (schemaVersion != common.getSchemaVersion()) {
        //      throw new SchemaOutOfSyncException();
        //    }

        ByteArrayInputStream bytesIn = new ByteArrayInputStream(body);
        DataInputStream in = new DataInputStream(bytesIn);
        long serializationVersion = DataUtil.readVLong(in);
        CreateTableStatementImpl createTableStatement = new CreateTableStatementImpl();
        createTableStatement.deserialize(in);

        logger.info("Create table: shard=" + server.getShard() + ", replica=" + server.getReplica() + ", name=" + createTableStatement.getTablename());

        TableSchema schema = new TableSchema();
        tableName = createTableStatement.getTablename();
        if (replayedCommand) {
          logger.info("replayedCommand: table=" + tableName + ", tableCount=" + server.getCommon().getTables(dbName).size());
          if (server.getCommon().getTables(dbName).containsKey(tableName.toLowerCase())) {
            ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytesOut);
            server.getCommon().serializeSchema(out, serializationVersionNumber);
            out.close();

            return bytesOut.toByteArray();
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

          ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
          DataOutputStream out = new DataOutputStream(bytesOut);
          server.getCommon().serializeSchema(out, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
          out.writeUTF(tableName);

          command = "DatabaseServer:createTableSlave:1:" + SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION +
              ":1:" + dbName + ":slave";
          server.getDatabaseClient().send(null, i, rand.nextLong(), command, bytesOut.toByteArray(), DatabaseClient.Replica.all);
        }
      }

      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytesOut);
      server.getCommon().serializeSchema(out, serializationVersionNumber);
      out.close();

      return bytesOut.toByteArray();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public byte[] dropColumn(String command, byte[] body) {

    try {
      String[] parts = command.split(":");
      int serializationVersionNumber = Integer.valueOf(parts[3]);
      String dbName = parts[5];
      String tableName = parts[6].toLowerCase();
      String columnName = parts[7].toLowerCase();

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

      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytesOut);
      server.getCommon().serializeSchema(out, serializationVersionNumber);
      out.close();

      return bytesOut.toByteArray();
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public byte[] addColumn(String command, byte[] body) {

    try {
      String[] parts = command.split(":");
      int serializationVersionNumber = Integer.valueOf(parts[3]);
      String dbName = parts[5];
      String tableName = parts[6].toLowerCase();
      String columnName = parts[7].toLowerCase();
      String dataType = parts[8];

      TableSchema tableSchema = server.getCommon().getTables(dbName).get(tableName);
      tableSchema.saveFields(server.getCommon().getSchemaVersion());

      FieldSchema fieldSchema = new FieldSchema();
      fieldSchema.setType(DataType.Type.valueOf(dataType));
      fieldSchema.setName(columnName);
      tableSchema.addField(fieldSchema);
      tableSchema.markChangesComplete();

      server.getCommon().saveSchema(server.getDataDir());

      server.pushSchema();

      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytesOut);
      server.getCommon().serializeSchema(out, serializationVersionNumber);
      out.close();

      return bytesOut.toByteArray();
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public byte[] createIndexSlave(String command, byte[] body) {
    try {
      String[] parts = command.split(":");
      String dbName = parts[5];

      if (server.getShard() == 0 && server.getReplica() == 0 && command.contains(":slave")) {
        return null;
      }

      DataInputStream in = new DataInputStream(new ByteArrayInputStream(body));
      server.getCommon().deserializeSchema(server.getCommon(), in);
      server.getCommon().saveSchema(server.getDataDir());

      String tableName = in.readUTF();
      TableSchema tableSchema = server.getCommon().getTables(dbName).get(tableName);
      int count = in.readInt();
      for (int i = 0; i < count; i++) {
        String indexName = in.readUTF();
        IndexSchema indexSchema = tableSchema.getIndexes().get(indexName);

        if (!server.getIndices(dbName).getIndices().containsKey(indexName)) {
          doCreateIndex(dbName, tableSchema, indexName, indexSchema.getFields());
        }
      }
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
    return null;
  }

  public List<String> createIndex(String command, byte[] body, boolean replayedCommand, AtomicReference<String> table) {
    try {
      String[] parts = command.split(":");
      int serializationVersionNumber = Integer.valueOf(parts[3]);
      int schemaVersion = Integer.valueOf(parts[4]);
      String dbName = parts[5];
      if (schemaVersion < server.getSchemaVersion()) {
        throw new SchemaOutOfSyncException("currVer:" + server.getCommon().getSchemaVersion() + ":");
      }
      String masterSlave = parts[6];
      table.set(parts[7]);
      String indexName = parts[8];
      boolean isUnique = Boolean.valueOf(parts[9]);
      String fieldsStr = parts[10];
      String[] fields = fieldsStr.split(",");

      if (replayedCommand) {
        logger.info("replayedCommand: createIndex, table=" + table.get() + ", index=" + indexName);
        if (server.getCommon().getTables(dbName).get(table.get()).getIndexes().containsKey(indexName.toLowerCase())) {
          ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
          DataOutputStream out = new DataOutputStream(bytesOut);
          server.getCommon().serializeSchema(out, serializationVersionNumber);
          out.close();

          return null;
        }
      }

      List<String> createdIndices = createIndex(dbName, table.get(), indexName, isUnique, fields);

      if (masterSlave.equals("master")) {

        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytesOut);
        server.getCommon().serializeSchema(out, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
        out.writeUTF(table.get());
        out.writeInt(createdIndices.size());
        for (String currIndexName : createdIndices) {
          out.writeUTF(currIndexName);
        }
        out.close();
        byte[] slaveBody = bytesOut.toByteArray();

        for (int i = 0; i < server.getShardCount(); i++) {
          for (int j = 0; j < server.getReplicationFactor(); j++) {
            if (i == 0 && j == 0) {
              continue;
            }
            command = "DatabaseServer:createIndexSlave:1:" + SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION +
                ":1:" + dbName + ":slave";
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


  public byte[] dropIndexSlave(String command, byte[] body) {
     try {
       String[] parts = command.split(":");
       String dbName = parts[5];

       if (server.getShard() == 0 && server.getReplica() == 0 && command.contains(":slave")) {
         return null;
       }

       DataInputStream in = new DataInputStream(new ByteArrayInputStream(body));
       server.getCommon().deserializeSchema(server.getCommon(), in);
       server.getCommon().saveSchema(server.getDataDir());

       String tableName = in.readUTF();
       int count = (int)DataUtil.readVLong(in);
       for (int i = 0; i < count; i++) {
         String indexName = in.readUTF();
         server.getIndices().get(dbName).getIndices().get(tableName).remove(indexName);
       }
     }
     catch (IOException e) {
       throw new DatabaseException(e);
     }
     return null;
   }

  public byte[] dropIndex(String command, byte[] body) {
      try {
        if (server.getShard() == 0 && server.getReplica() == 0 && command.contains(":slave:")) {
          return null;
        }

        String[] parts = command.split(":");
        int serializationVersionNumber = Integer.valueOf(parts[3]);
        int schemaVersion = Integer.valueOf(parts[4]);
        String dbName = parts[5];
        if (schemaVersion < server.getSchemaVersion()) {
          throw new SchemaOutOfSyncException("currVer:" + server.getCommon().getSchemaVersion() + ":");
        }
        String table = parts[6];
        String indexName = parts[7];

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


        AtomicReference<String> selectedHost = new AtomicReference<String>();
        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytesOut);
        server.getCommon().serializeSchema(out, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
        out.writeUTF(table);
        DataUtil.writeVLong(out, toDrop.size());
        for (IndexSchema indexSchema : toDrop) {
          out.writeUTF(indexSchema.getName());
        }
        out.close();
        byte[] slaveBytes = bytesOut.toByteArray();

        for (int i = 0; i < server.getShardCount(); i++) {
          for (int j = 0; j < server.getReplicationFactor(); j++) {
            if (i == 0 && j == 0) {
              continue;
            }
            command = "DatabaseServer:dropIndexSlave:1:" + SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION +
                ":1:" + dbName + ":slave";
            server.getDatabaseClient().send(null, i, j, command, slaveBytes, DatabaseClient.Replica.specified);
          }
        }

        bytesOut = new ByteArrayOutputStream();
        out = new DataOutputStream(bytesOut);
        server.getCommon().serializeSchema(out, serializationVersionNumber);
        out.close();

        return bytesOut.toByteArray();
      }
      catch (IOException e) {
        throw new DatabaseException(e);
      }
    }

}
