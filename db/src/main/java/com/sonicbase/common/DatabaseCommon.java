package com.sonicbase.common;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.*;
import org.apache.giraph.utils.Varint;

import java.io.*;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.sonicbase.client.DatabaseClient.SERIALIZATION_VERSION;
import static com.sonicbase.client.DatabaseClient.SERIALIZATION_VERSION_21;
import static com.sonicbase.server.DatabaseServer.USE_SNAPSHOT_MGR_OLD;
import static java.sql.Types.*;

/**
 * User: lowryda
 * Date: 9/30/14
 * Time: 11:08 PM
 */
@ExcludeRename
public class DatabaseCommon {

  private org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("com.sonicbase.logger");


  private int shard = -1;
  private int replica = -1;
  private Map<String, Schema> schema = new ConcurrentHashMap<>();
  private Map<String, ReadWriteLock> schemaReadWriteLock = new ConcurrentHashMap<>();
  private Map<String, Lock> schemaReadLock = new ConcurrentHashMap<>();
  private Map<String, Lock> schemaWriteLock = new ConcurrentHashMap<>();
  private ServersConfig serversConfig;
  private ReadWriteLock internalReadWriteLock = new ReentrantReadWriteLock();
  private Lock internalReadLock = internalReadWriteLock.readLock();
  private Lock internalWriteLock = internalReadWriteLock.writeLock();
  private int schemaVersion;
  private boolean haveProLicense;
  private int[] masterReplicas;
  private boolean[][] deadNodes;

  public Lock getSchemaReadLock(String dbName) {
    Lock ret = schemaReadLock.get(dbName);
    if (ret == null) {
      ReadWriteLock lock = new ReentrantReadWriteLock();
      schemaReadWriteLock.put(dbName, lock);
      schemaReadLock.put(dbName, lock.readLock());
      schemaWriteLock.put(dbName, lock.writeLock());
      ret = schemaReadLock.get(dbName);
    }
    return ret;
  }

  public Lock getSchemaWriteLock(String dbName) {
    Lock ret = schemaWriteLock.get(dbName);
    if (ret == null) {
      ReadWriteLock lock = new ReentrantReadWriteLock();
      schemaReadWriteLock.put(dbName, lock);
      schemaReadLock.put(dbName, lock.readLock());
      schemaWriteLock.put(dbName, lock.writeLock());
      ret = schemaWriteLock.get(dbName);
    }
    return ret;
  }

  public Schema getSchema(String dbName) {

    Schema retSchema = ensureSchemaExists(dbName);
    return retSchema;
  }

  public Map<String, TableSchema> getTables(String dbName) {
    Schema retSchema = ensureSchemaExists(dbName);
    return retSchema.getTables();
  }

  public Map<Integer, TableSchema> getTablesById(String dbName) {
    Schema retSchema = ensureSchemaExists(dbName);
    return retSchema.getTablesById();
  }

  public TableSchema getTableSchema(String dbName, String tableName, String dataDir) {
    Schema dbSchema = schema.get(dbName);
    if (dbSchema == null) {
      Schema prevSchema = schema.put(dbName, new Schema());
      if (prevSchema != null) {
        schema.put(dbName, prevSchema);
      }
      dbSchema = schema.get(dbName);
    }
    TableSchema tableSchema = dbSchema.getTables().get(tableName);
    if (tableSchema != null) {
      return tableSchema;
    }
    File file = new File(dataDir, "snapshot/" + shard + "/" + replica + "/_sonicbase_schema/" + dbName);
    File tableDir = new File(file, tableName + "/table");
    try {
      loadTableSchema(dbSchema, tableDir);
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
    return dbSchema.getTables().get(tableName);
  }

  public void loadSchema(String dataDir) {
    try {
      internalWriteLock.lock();
      try {
        String dataRoot = null;
        if (USE_SNAPSHOT_MGR_OLD) {
          dataRoot = new File(dataDir, "snapshot/" + shard + "/" + replica).getAbsolutePath();
        }
        else {
          dataRoot = new File(dataDir, "delta/" + shard + "/" + replica).getAbsolutePath();
        }
        File schemaFile = new File(dataRoot, "schema.bin");
        logger.info("Loading schema: file=" + schemaFile.getAbsolutePath());
        if (schemaFile.exists()) {
          try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(schemaFile)))) {
            deserializeSchema(in);
          }
        }
        else {
          logger.info("No schema file found");
        }
        //schema.clear();
        List<String> dbNames = getDbNames(dataDir);
        for (String dbName : dbNames) {
          Schema dbSchema = schema.get(dbName);
          if (dbSchema == null) {
            dbSchema = new Schema();
            schema.put(dbName, dbSchema);
          }
          File file = new File(dataDir, "snapshot/" + shard + "/" + replica + "/_sonicbase_schema/" + dbName);
          File[] tableNames = file.listFiles();
          if (tableNames != null) {
            for (File tableFile : tableNames) {
              loadTableSchema(dbSchema, tableFile);
            }
          }
        }
      }
      finally {
        internalWriteLock.unlock();
      }
      for (String dbName : schema.keySet()) {
        createSchemaLocks(dbName);
      }
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  private void loadTableSchema(Schema dbSchema, File tableFile) throws IOException {
    String tableName = tableFile.getName();
    TableSchema previousTableSchema = dbSchema.getTables().get(tableName);
    File tableDir = new File(tableFile + "/table");
    File[] tableSchemas = tableDir.listFiles();
    if (tableSchemas != null && tableSchemas.length > 0) {
      sortSchemaFiles(tableSchemas);
      File tableSchemaFile = tableSchemas[tableSchemas.length - 1];
      TableSchema tableSchema = new TableSchema();
      try (DataInputStream in = new DataInputStream(new FileInputStream(tableSchemaFile))) {
        short serializationVersion = in.readShort();
        try {
          tableSchema.deserialize(in, serializationVersion);
          TableSchema existingSchema = dbSchema.getTables().get(tableSchema.getName());
          if (existingSchema != null) {
            for (IndexSchema indexSchema : existingSchema.getIndexes().values()) {
              tableSchema.getIndexes().put(indexSchema.getName(), indexSchema);
              tableSchema.getIndexesById().put(indexSchema.getIndexId(), indexSchema);
            }
          }
          dbSchema.getTables().put(tableSchema.getName(), tableSchema);
          dbSchema.getTablesById().put(tableSchema.getTableId(), tableSchema);
          File indicesDir = new File(tableFile, "/indices");
          if (indicesDir.exists()) {
            File[] indices = indicesDir.listFiles();
            if (indices != null) {
              for (File indexDir : indices) {
                String indexName = indexDir.getName();
                IndexSchema previousIndexSchema = previousTableSchema == null ? null : previousTableSchema.getIndices().get(indexName);
                File[] indexSchemas = indexDir.listFiles();
                if (indexSchemas != null && indexSchemas.length > 0) {
                  sortSchemaFiles(indexSchemas);
                  File indexSchemaFile = indexSchemas[indexSchemas.length - 1];
                  try (DataInputStream indexIn = new DataInputStream(new FileInputStream(indexSchemaFile))) {
                    serializationVersion = indexIn.readShort();
                    TableSchema.deserializeIndexSchema(indexIn, tableSchema);
                  }
                }
                IndexSchema currIndexSchema = tableSchema.getIndices().get(indexName);
                if (previousIndexSchema != null) {
                  currIndexSchema.setCurrPartitions(previousIndexSchema.getCurrPartitions());
                  currIndexSchema.setLastPartitions(previousIndexSchema.getLastPartitions());
                }
              }
            }
          }
        }
        catch (Exception e) {
          throw new DatabaseException("Error deserializing tableSchema: file=" + tableSchemaFile.getAbsolutePath());
        }
      }
    }
  }

  public static void sortSchemaFiles(File[] schemas) {
    Arrays.sort(schemas, new Comparator<File>() {
      @Override
      public int compare(File o1, File o2) {
        int o1SchemaVersion = getSchemaVersion(o1);
        int o2SchemaVersion = getSchemaVersion(o2);
        return Integer.compare(o1SchemaVersion, o2SchemaVersion);
      }

      public int getSchemaVersion(File file) {
        String filename = file.getName();
        int pos1 = filename.indexOf('.');
        int pos2 = filename.indexOf('.', pos1 + 1);
        if (pos1 == -1 || pos2 == -1) {
          return -1;
        }
        return Integer.valueOf(filename.substring(pos1 + 1, pos2));
      }
    });
  }

  private void createSchemaLocks(String dbName) {
    if (!schemaReadWriteLock.containsKey(dbName)) {
      ReadWriteLock lock = new ReentrantReadWriteLock();
      schemaReadWriteLock.put(dbName, lock);
      schemaReadLock.put(dbName, lock.readLock());
      schemaWriteLock.put(dbName, lock.writeLock());
    }
  }

  public void saveSchema(byte[] bytes, String dataDir) {
    try {
      internalWriteLock.lock();
      String dataRoot = null;
      if (USE_SNAPSHOT_MGR_OLD) {
        dataRoot = new File(dataDir, "snapshot/" + shard + "/" + replica).getAbsolutePath();
      }
      else {
        dataRoot = new File(dataDir, "delta/" + shard + "/" + replica).getAbsolutePath();
      }
        File schemaFile = new File(dataRoot, "schema.bin");
        if (schemaFile.exists()) {
          schemaFile.delete();
        }

        schemaFile.getParentFile().mkdirs();
        try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(schemaFile)))) {
          if (getShard() == 0 &&
              getServersConfig().getShards()[0].getMasterReplica() == getReplica()) {
            this.schemaVersion++;
            //          schema.get(dbName).incrementSchemaVersion();
          }
          serializeSchema(out, SERIALIZATION_VERSION);
        }


        loadSchema(dataDir);
        logger.info("Saved schema - postLoad: dir=" + dataRoot);

    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
    finally {
      internalWriteLock.unlock();
    }
  }
  public void saveSchema(DatabaseClient client, String dataDir) {
    try {
      internalWriteLock.lock();
      String dataRoot = null;
      if (USE_SNAPSHOT_MGR_OLD) {
        dataRoot = new File(dataDir, "snapshot/" + shard + "/" + replica).getAbsolutePath();
      }
      else {
        dataRoot = new File(dataDir, "delta/" + shard + "/" + replica).getAbsolutePath();
      }
      File schemaFile = new File(dataRoot, "schema.bin");
      if (schemaFile.exists()) {
        schemaFile.delete();
      }

      schemaFile.getParentFile().mkdirs();
      try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(schemaFile)))) {
        if (getShard() == 0 &&
            getServersConfig().getShards()[0].getMasterReplica() == getReplica()) {
          this.schemaVersion++;
          //          schema.get(dbName).incrementSchemaVersion();
        }
        serializeSchema(out, SERIALIZATION_VERSION);

      }
//      if (getShard() == 0 &&
//          getServersConfig().getShards()[0].getMasterReplica() == getReplica()) {
//        String command = "DatabaseServer:ComObject:saveSchema:";
//        ComObject cobj = new ComObject();
//        cobj.put(ComObject.Tag.method, "saveSchema");
//        cobj.put(ComObject.Tag.schemaBytes, serializeSchema(SnapshotManagerImpl.SERIALIZATION_VERSION));
//        client.send(null, 0, getReplica(), command, cobj, DatabaseClient.Replica.specified);
//      }


      loadSchema(dataDir);
      logger.info("Saved schema - postLoad: dir=" + dataRoot);

    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
    finally {
      internalWriteLock.unlock();
    }
  }

  public byte[] serializeSchema(short serializationVersionNumber) throws IOException {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytesOut);
    serializeSchema(out, serializationVersionNumber);
    out.close();
    return bytesOut.toByteArray();
  }

  public void serializeSchema(DataOutputStream out, short serializationVersionNumber) throws IOException {
    Varint.writeSignedVarLong(serializationVersionNumber, out);
    out.writeInt(this.schemaVersion);
    if (serializationVersionNumber >= SERIALIZATION_VERSION_21) {
      out.writeBoolean(haveProLicense);
    }
    if (serversConfig == null) {
      out.writeBoolean(false);
    }
    else {
      out.writeBoolean(true);
      out.write(serializeConfig(serializationVersionNumber));
    }
    out.writeInt(schema.keySet().size());
    try {
      internalReadLock.lock();
      for (String dbName : schema.keySet()) {
        // getSchemaReadLock(dbName).lock();
        try {
          out.writeUTF(dbName);
          serializeSchema(dbName, out);
        }
        finally {
          //     getSchemaReadLock(dbName).unlock();
        }
      }
    }
    finally {
      internalReadLock.unlock();
    }
  }

  public void setShard(int shard) {
    this.shard = shard;
  }

  public void setReplica(int replica) {
    this.replica = replica;
  }

  public void updateTable(DatabaseClient client, String dbName, String dataDir, TableSchema tableSchema) {
    synchronized (this) {
      schema.get(dbName).updateTable(tableSchema);
    }
  }


  public void addTable(DatabaseClient client, String dbName, String dataDir, TableSchema schema) {
    synchronized (this) {
      Schema retSchema = ensureSchemaExists(dbName);
      retSchema.addTable(schema);
    }
  }

  private Schema ensureSchemaExists(String dbName) {
    synchronized (this) {
      Schema retSchema = this.schema.get(dbName);
      if (retSchema == null) {
        retSchema = new Schema();
        this.schema.put(dbName, retSchema);
      }
      return retSchema;
    }
  }

  public void serializeSchema(String dbName, DataOutputStream out) {
    schema.get(dbName).serialize(out);
  }

  public void deserializeSchema(byte[] bytes) {
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
    deserializeSchema(in);
  }

  public void deserializeSchema(DataInputStream in) {

    try {
      internalWriteLock.lock();
      short serializationVersion = (short)Varint.readSignedVarLong(in);
      this.schemaVersion = in.readInt();
      if (serializationVersion >= SERIALIZATION_VERSION_21) {
        this.haveProLicense = in.readBoolean();
      }
      if (in.readBoolean()) {
        deserializeConfig(in);
      }
      int count = in.readInt();
      for (int i = 0; i < count; i++) {
        String dbName = in.readUTF();
//        if (common.getSchemaWriteLock(dbName) != null) { //not there on client
//          common.getSchemaWriteLock(dbName).lock();
//        }
        try {
          Schema newSchema = new Schema();
          newSchema.deserialize(in);
          schema.put(dbName, newSchema);
          createSchemaLocks(dbName);
        }
        finally {
//          if (common.getSchemaWriteLock(dbName) != null) {
//            common.getSchemaWriteLock(dbName).unlock();
//          }
        }
      }
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
    finally {
      internalWriteLock.unlock();
    }
  }

  public static int compareKey(Comparator[] comparators, Object[] o1, Object[] o2) {
    for (int i = 0; i < o1.length; i++) {
      if (o1[i] == null || o2[i] == null) {
        continue;
      }
      int value = comparators[i].compare(o1[i], o2[i]);
      if (value < 0) {
        return -1;
      }
      if (value > 0) {
        return 1;
      }
    }
    return 0;
  }

  public static Object[] deserializeKey(TableSchema tableSchema, byte[] bytes) throws EOFException {
    return deserializeKey(tableSchema, new DataInputStream(new ByteArrayInputStream(bytes)));
  }

  public static Object[] deserializeKey(TableSchema tableSchema, DataInputStream in) throws EOFException {

    int indexId = -1;
    try {
      short serializationVersion = (short)Varint.readSignedVarLong(in);
      Varint.readSignedVarLong(in);
      indexId = (int) Varint.readSignedVarLong(in);
      //logger.info("tableId=" + tableId + " indexId=" + indexId + ", indexCount=" + tableSchema.getIndices().size());
      IndexSchema indexSchema = tableSchema.getIndexesById().get(indexId);
      int[] columns = indexSchema.getFieldOffsets();
      int keyLength = (int) Varint.readSignedVarLong(in);
      Object[] fields = new Object[keyLength];
      for (int i = 0; i < keyLength; i++) {
        if (in.readBoolean()) {
          if (tableSchema.getFields().get(columns[i]).getType() == DataType.Type.BIGINT) {
            fields[i] = Varint.readSignedVarLong(in);
          }
          else if (tableSchema.getFields().get(columns[i]).getType() == DataType.Type.INTEGER) {
            fields[i] = (int) Varint.readSignedVarLong(in);
          }
          else if (tableSchema.getFields().get(columns[i]).getType() == DataType.Type.SMALLINT) {
            fields[i] = (short) Varint.readSignedVarLong(in);
          }
          else if (tableSchema.getFields().get(columns[i]).getType() == DataType.Type.TINYINT) {
            fields[i] = in.readByte();
          }
          else if (tableSchema.getFields().get(columns[i]).getType() == DataType.Type.CHAR) {
            int len = (int) Varint.readSignedVarLong(in);
            byte[] bytes = new byte[len];
            in.read(bytes);
            fields[i] = bytes;
          }
          else if (tableSchema.getFields().get(columns[i]).getType() == DataType.Type.NCHAR) {
            int len = (int) Varint.readSignedVarLong(in);
            byte[] bytes = new byte[len];
            in.read(bytes);
            fields[i] = bytes;
          }
          else if (tableSchema.getFields().get(columns[i]).getType() == DataType.Type.FLOAT) {
            fields[i] = in.readDouble();
          }
          else if (tableSchema.getFields().get(columns[i]).getType() == DataType.Type.REAL) {
            fields[i] = in.readFloat();
          }
          else if (tableSchema.getFields().get(columns[i]).getType() == DataType.Type.DOUBLE) {
            fields[i] = in.readDouble();
          }
          else if (tableSchema.getFields().get(columns[i]).getType() == DataType.Type.BOOLEAN) {
            fields[i] = in.readBoolean();
          }
          else if (tableSchema.getFields().get(columns[i]).getType() == DataType.Type.BIT) {
            fields[i] = in.readBoolean();
          }
          else if (tableSchema.getFields().get(columns[i]).getType() == DataType.Type.VARCHAR ||
              tableSchema.getFields().get(columns[i]).getType() == DataType.Type.LONGVARCHAR ||
              tableSchema.getFields().get(columns[i]).getType() == DataType.Type.LONGNVARCHAR ||
              tableSchema.getFields().get(columns[i]).getType() == DataType.Type.CLOB ||
              tableSchema.getFields().get(columns[i]).getType() == DataType.Type.NCLOB ||
              tableSchema.getFields().get(columns[i]).getType() == DataType.Type.NVARCHAR) {
            int len = (int) Varint.readSignedVarLong(in);
            byte[] bytes = new byte[len];
            in.read(bytes);
            fields[i] = bytes;
          }
          else if (tableSchema.getFields().get(columns[i]).getType() == DataType.Type.LONGVARBINARY ||
              tableSchema.getFields().get(columns[i]).getType() == DataType.Type.VARBINARY ||
              tableSchema.getFields().get(columns[i]).getType() == DataType.Type.BLOB) {
            int len = (int) Varint.readSignedVarLong(in);
            byte[] data = new byte[len];
            in.readFully(data);
            fields[i] = data;
          }
          else if (tableSchema.getFields().get(columns[i]).getType() == DataType.Type.NUMERIC) {
            int len = (int) Varint.readSignedVarLong(in);
            byte[] buffer = new byte[len];
            in.readFully(buffer);
            String str = new String(buffer, "utf-8");
            fields[i] = new BigDecimal(str);
          }
          else if (tableSchema.getFields().get(columns[i]).getType() == DataType.Type.DECIMAL) {
            int len = (int) Varint.readSignedVarLong(in);
            byte[] buffer = new byte[len];
            in.readFully(buffer);
            String str = new String(buffer, "utf-8");
            fields[i] = new BigDecimal(str);
          }
          else if (tableSchema.getFields().get(columns[i]).getType() == DataType.Type.DATE) {
            int len = (int) Varint.readSignedVarLong(in);
            byte[] buffer = new byte[len];
            in.readFully(buffer);
            String str = new String(buffer, "utf-8");
            fields[i] = Date.valueOf(str);
          }
          else if (tableSchema.getFields().get(columns[i]).getType() == DataType.Type.TIME) {
            int len = (int) Varint.readSignedVarLong(in);
            byte[] buffer = new byte[len];
            in.readFully(buffer);
            String str = new String(buffer, "utf-8");
            fields[i] = Time.valueOf(str);
          }
          else if (tableSchema.getFields().get(columns[i]).getType() == DataType.Type.TIMESTAMP) {
            int len = (int) Varint.readSignedVarLong(in);
            byte[] buffer = new byte[len];
            in.readFully(buffer);
            String str = new String(buffer, "utf-8");
            fields[i] = Timestamp.valueOf(str);
          }
        }
      }
      return fields;
    }
    catch (EOFException e) {
      throw e;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
    catch (Exception e) {
      throw new DatabaseException("Error deserializing key: indexId=" + indexId, e);
    }
  }

  public static DataType.Type[] deserializeKeyPrep(TableSchema tableSchema, byte[] bytes) throws IOException {
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
    short serializationVersion = (short)Varint.readSignedVarLong(in);
    Varint.readSignedVarLong(in);
    int indexId = (int) Varint.readSignedVarLong(in);
    //logger.info("tableId=" + tableId + " indexId=" + indexId + ", indexCount=" + tableSchema.getIndices().size());
    IndexSchema indexSchema = tableSchema.getIndexesById().get(indexId);

    String[] columns = indexSchema.getFields();
    int keyLength = indexSchema.getFields().length;
    Object[] fields = new Object[keyLength];
    DataType.Type[] types = new DataType.Type[fields.length];
    for (int i = 0; i < keyLength; i++) {
      String column = columns[i];
      if (column != null) {
        types[i] = tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType();
      }
    }
    return types;
  }

  public static Object[] deserializeKey(TableSchema tableSchema, DataType.Type[] types, DataInputStream in) throws EOFException {
    try {
      short serializationVersion = (short)Varint.readSignedVarLong(in);
      Varint.readSignedVarLong(in);
      int indexId = (int) Varint.readSignedVarLong(in);
      //logger.info("tableId=" + tableId + " indexId=" + indexId + ", indexCount=" + tableSchema.getIndices().size());
      IndexSchema indexSchema = tableSchema.getIndexesById().get(indexId);
      String[] columns = indexSchema.getFields();
      int keyLength = (int) Varint.readSignedVarLong(in);
      Object[] fields = new Object[keyLength];
      for (int i = 0; i < keyLength; i++) {
        if (in.readBoolean()) {
          if (types[i] == DataType.Type.BIGINT) {
            fields[i] = Varint.readSignedVarLong(in);
          }
          else if (types[i] == DataType.Type.INTEGER) {
            fields[i] = (int) Varint.readSignedVarLong(in);
          }
          else if (types[i] == DataType.Type.SMALLINT) {
            fields[i] = (short) Varint.readSignedVarLong(in);
          }
          else if (types[i] == DataType.Type.TINYINT) {
            fields[i] = in.readByte();
          }
          else if (types[i] == DataType.Type.CHAR) {
            int len = (int) Varint.readSignedVarLong(in);
            byte[] bytes = new byte[len];
            in.read(bytes);
            fields[i] = bytes;
          }
          else if (types[i] == DataType.Type.NCHAR) {
            int len = (int) Varint.readSignedVarLong(in);
            byte[] bytes = new byte[len];
            in.read(bytes);
            fields[i] = bytes;
          }
          else if (types[i] == DataType.Type.FLOAT) {
            fields[i] = in.readDouble();
          }
          else if (types[i] == DataType.Type.REAL) {
            fields[i] = in.readFloat();
          }
          else if (types[i] == DataType.Type.DOUBLE) {
            fields[i] = in.readDouble();
          }
          else if (types[i] == DataType.Type.BOOLEAN) {
            fields[i] = in.readBoolean();
          }
          else if (types[i] == DataType.Type.BIT) {
            fields[i] = in.readBoolean();
          }
          else if (types[i] == DataType.Type.VARCHAR ||
              types[i] == DataType.Type.LONGVARCHAR ||
              types[i] == DataType.Type.LONGNVARCHAR ||
              types[i] == DataType.Type.CLOB ||
              types[i] == DataType.Type.NCLOB ||
              types[i] == DataType.Type.NVARCHAR) {
            int len = (int) Varint.readSignedVarLong(in);
            byte[] bytes = new byte[len];
            in.read(bytes);
            fields[i] = bytes;
          }
          else if (types[i] == DataType.Type.LONGVARBINARY ||
              types[i] == DataType.Type.VARBINARY ||
              types[i] == DataType.Type.BLOB) {
            int len = (int) Varint.readSignedVarLong(in);
            byte[] data = new byte[len];
            in.readFully(data);
            fields[i] = data;
          }
          else if (types[i] == DataType.Type.NUMERIC) {
            int len = (int) Varint.readSignedVarLong(in);
            byte[] buffer = new byte[len];
            in.readFully(buffer);
            String str = new String(buffer, "utf-8");
            fields[i] = new BigDecimal(str);
          }
          else if (types[i] == DataType.Type.DECIMAL) {
            int len = (int) Varint.readSignedVarLong(in);
            byte[] buffer = new byte[len];
            in.readFully(buffer);
            String str = new String(buffer, "utf-8");
            fields[i] = new BigDecimal(str);
          }
          else if (types[i] == DataType.Type.DATE) {
            int len = (int) Varint.readSignedVarLong(in);
            byte[] buffer = new byte[len];
            in.readFully(buffer);
            String str = new String(buffer, "utf-8");
            fields[i] = Date.valueOf(str);
          }
          else if (types[i] == DataType.Type.TIME) {
            int len = (int) Varint.readSignedVarLong(in);
            byte[] buffer = new byte[len];
            in.readFully(buffer);
            String str = new String(buffer, "utf-8");
            fields[i] = Time.valueOf(str);
          }
          else if (types[i] == DataType.Type.TIMESTAMP) {
            int len = (int) Varint.readSignedVarLong(in);
            byte[] buffer = new byte[len];
            in.readFully(buffer);
            String str = new String(buffer, "utf-8");
            fields[i] = Timestamp.valueOf(str);
          }
        }
      }
      return fields;
    }
    catch (EOFException e) {
      throw e;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public static byte[] serializeKey(TableSchema tableSchema, String indexName, Object[] key) {
    try {
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytesOut);
      Varint.writeSignedVarLong(SERIALIZATION_VERSION, out);
      Varint.writeSignedVarLong(tableSchema.getTableId(), out);
      Varint.writeSignedVarLong(tableSchema.getIndexes().get(indexName).getIndexId(), out);
      IndexSchema indexSchema = tableSchema.getIndices().get(indexName);
      String[] columns = indexSchema.getFields();
      if (key == null) {
        Varint.writeSignedVarLong(0, out);
      }
      else {
        Varint.writeSignedVarLong(columns.length, out);
        for (int i = 0; i < columns.length; i++) {
          String column = columns[i];
          if (column != null) {
            if (i >= key.length || key[i] == null) {
              out.writeBoolean(false);
            }
            else {
              out.writeBoolean(true);
              if (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.BIGINT) {
                Varint.writeSignedVarLong((Long) key[i], out);
              }
              else if (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.INTEGER) {
                Varint.writeSignedVarLong((Integer) key[i], out);
              }
              else if (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.SMALLINT) {
                Varint.writeSignedVarLong((Short) key[i], out);
              }
              else if (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.TINYINT) {
                out.write((byte) key[i]);
              }
              else if (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.CHAR) {
                byte[] bytes = (byte[]) key[i];
                if (bytes == null) {
                  Varint.writeSignedVarLong(0, out);
                }
                else {
                  Varint.writeSignedVarLong(bytes.length, out);
                  out.write(bytes);
                }
              }
              else if (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.NCHAR) {
                byte[] bytes = (byte[]) key[i];
                if (bytes == null) {
                  Varint.writeSignedVarLong(0, out);
                }
                else {
                  Varint.writeSignedVarLong(bytes.length, out);
                  out.write(bytes);
                }
              }
              else if (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.FLOAT) {
                out.writeDouble((Double) key[i]);
              }
              else if (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.REAL) {
                out.writeFloat((Float) key[i]);
              }
              else if (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.DOUBLE) {
                out.writeDouble((Double) key[i]);
              }
              else if (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.BOOLEAN) {
                out.writeBoolean((Boolean) key[i]);
              }
              else if (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.BIT) {
                out.writeBoolean((Boolean) key[i]);
              }
              else if (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.VARCHAR ||
                  tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.CLOB ||
                  tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.NCLOB ||
                  tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.LONGNVARCHAR ||
                  tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.NVARCHAR ||
                  tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.LONGVARCHAR
                  ) {
                byte[] bytes = (byte[]) key[i];
                if (bytes == null) {
                  Varint.writeSignedVarLong(0, out);
                }
                else {
                  Varint.writeSignedVarLong(bytes.length, out);
                  out.write(bytes);
                }
              }
              else if (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.LONGVARBINARY ||
                  tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.VARBINARY ||
                  tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.BLOB) {
                byte[] bytes = (byte[]) key[i];
                if (bytes == null) {
                  Varint.writeSignedVarLong(0, out);
                }
                else {
                  Varint.writeSignedVarLong(bytes.length, out);
                  out.write(bytes);
                }
              }
              else if (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.NUMERIC) {
                BigDecimal value = ((BigDecimal) key[i]);
                String strValue = value.toPlainString();
                byte[] bytes = strValue.getBytes("utf-8");
                Varint.writeSignedVarLong(bytes.length, out);
                out.write(bytes);
              }
              else if (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.DECIMAL) {
                BigDecimal value = ((BigDecimal) key[i]);
                String strValue = value.toPlainString();
                byte[] bytes = strValue.getBytes("utf-8");
                Varint.writeSignedVarLong(bytes.length, out);
                out.write(bytes);
              }
              else if (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.DATE) {
                Date value = ((Date) key[i]);
                String str = value.toString();
                byte[] bytes = str.getBytes("utf-8");
                Varint.writeSignedVarLong(bytes.length, out);
                out.write(bytes);
              }
              else if (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.TIME) {
                Time value = ((Time) key[i]);
                String str = value.toString();
                byte[] bytes = str.getBytes("utf-8");
                Varint.writeSignedVarLong(bytes.length, out);
                out.write(bytes);
              }
              else if (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.TIMESTAMP) {
                Timestamp value = ((Timestamp) key[i]);
                String str = value.toString();
                byte[] bytes = str.getBytes("utf-8");
                Varint.writeSignedVarLong(bytes.length, out);
                out.write(bytes);
              }
            }
          }
        }
      }
      out.close();
      return bytesOut.toByteArray();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public static Object[] deserializeTypedKey(byte[] bytes) {
    try {

      DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));

      int serializationVersion = (int) Varint.readUnsignedVarLong(in);
      int count = (int) Varint.readUnsignedVarLong(in);
      if (count == 0) {
        return null;
      }
      Object[] ret = new Object[count];
      for (int i = 0; i < count; i++) {
        if (in.readBoolean()) {
          int type = (int) Varint.readSignedVarLong(in);
          switch (type) {
            case BIGINT:
              ret[i] = (long)Varint.readSignedVarLong(in);
              break;
            case INTEGER:
              ret[i] = (int)Varint.readSignedVarLong(in);
              break;
            case SMALLINT:
              ret[i] = (short)Varint.readSignedVarLong(in);
              break;
            case TINYINT:
              ret[i] = in.readByte();
              break;
            case LONGVARCHAR:
            case VARCHAR:
            case CHAR: {
              int len = (int) Varint.readSignedVarLong(in);
              byte[] buffer = new byte[len];
              in.readFully(buffer);
              ret[i] = buffer;
            }
              break;
            case FLOAT:
              ret[i] = in.readFloat();
              break;
            case DOUBLE:
              ret[i] = in.readDouble();
              break;
            case BIT:
              ret[i] = in.readBoolean();
              break;
            case DECIMAL: {
              int len = (int) Varint.readSignedVarLong(in);
              byte[] buffer = new byte[len];
              in.readFully(buffer);
              String value = new String(buffer, "utf-8");
              ret[i] = new BigDecimal(value);
              break;
            }
            case DATE: {
              int len = (int) Varint.readSignedVarLong(in);
              byte[] buffer = new byte[len];
              in.readFully(buffer);
              String value = new String(buffer, "utf-8");
              ret[i] = Date.valueOf(value);
            }
              break;
            case TIME: {
              int len = (int) Varint.readSignedVarLong(in);
              byte[] buffer = new byte[len];
              in.readFully(buffer);
              String value = new String(buffer, "utf-8");
              ret[i] = Time.valueOf(value);
            }
              break;
            case TIMESTAMP: {
              int len = (int) Varint.readSignedVarLong(in);
              byte[] buffer = new byte[len];
              in.readFully(buffer);
              String value = new String(buffer, "utf-8");
              ret[i] = Timestamp.valueOf(value);
            }
            break;
            default:
              throw new DatabaseException("Unknown type: type=" + type);
          }
        }
      }
      return ret;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public static byte[] serializeTypedKey(Object[] key) {
    try {
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytesOut);
      Varint.writeUnsignedVarLong(SERIALIZATION_VERSION, out);
      if (key == null) {
        Varint.writeUnsignedVarLong(0, out);
      }
      else {
        Varint.writeUnsignedVarLong(key.length, out);
        for (int i = 0; i < key.length; i++) {
          if (key[i] == null) {
            out.writeBoolean(false);
          }
          else {
            out.writeBoolean(true);
            int type = DataType.Type.getTypeForValue(key[i]);
            Varint.writeSignedVarLong(type, out);

            if (key[i] instanceof Long) {
              Varint.writeSignedVarLong((Long) key[i], out);
            }
            else if (key[i] instanceof Integer) {
              Varint.writeSignedVarLong((Integer) key[i], out);
            }
            else if (key[i] instanceof Short) {
              Varint.writeSignedVarLong((Short) key[i], out);
            }
            else if (key[i] instanceof Byte) {
              out.write((byte) key[i]);
            }
            else if (key[i] instanceof byte[]) {
              Varint.writeSignedVarLong(((byte[])key[i]).length, out);
              out.write(((byte[])key[i]));
            }
            else if (key[i] instanceof Float) {
              out.writeFloat((Float) key[i]);
            }
            else if (key[i] instanceof Double) {
              out.writeDouble((Double) key[i]);
            }
            else if (key[i] instanceof Boolean) {
              out.writeBoolean((Boolean) key[i]);
            }
            else if (key[i] instanceof BigDecimal) {
              BigDecimal value = ((BigDecimal) key[i]);
              String strValue = value.toPlainString();
              byte[] bytes = strValue.getBytes("utf-8");
              Varint.writeSignedVarLong(bytes.length, out);
              out.write(bytes);
            }
            else if (key[i] instanceof Date) {
              Date value = ((Date) key[i]);
              String str = value.toString();
              byte[] bytes = str.getBytes("utf-8");
              Varint.writeSignedVarLong(bytes.length, out);
              out.write(bytes);
            }
            else if (key[i] instanceof Time) {
              Time value = ((Time) key[i]);
              String str = value.toString();
              byte[] bytes = str.getBytes("utf-8");
              Varint.writeSignedVarLong(bytes.length, out);
              out.write(bytes);
            }
            else if (key[i] instanceof Timestamp) {
              Timestamp value = ((Timestamp) key[i]);
              String str = value.toString();
              byte[] bytes = str.getBytes("utf-8");
              Varint.writeSignedVarLong(bytes.length, out);
              out.write(bytes);
            }
            else {
              throw new DatabaseException("Unknown type: type=" + key[i].getClass().getName());
            }
          }
        }
      }
      out.close();
      return bytesOut.toByteArray();
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public static void serializeFields(
      Object[] fields, DataOutputStream outerOut, TableSchema tableSchema, int schemaVersion, boolean serializeHeader) throws IOException {

    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytesOut);
    AtomicInteger readLen = new AtomicInteger();

    Varint.writeSignedVarLong(schemaVersion, out);

    int offset = 0;
    byte[] buffer = new byte[16];
    for (Object field : fields) {
      if (field == null) {
        Varint.writeSignedVarLong(0, out);
        offset++;
      }
      else {
        if (tableSchema.getFields().get(offset).getType() == DataType.Type.BIGINT) {
          long len = Varint.sizeOfSignedVarLong((Long)field);
          Varint.writeSignedVarLong(len, out);
          Varint.writeSignedVarLong((Long)field, out);
          offset++;
        }
        else if (tableSchema.getFields().get(offset).getType() == DataType.Type.INTEGER) {
          long len = Varint.sizeOfSignedVarLong((Integer)field);
          Varint.writeSignedVarLong(len, out);
          Varint.writeSignedVarLong((Integer)field, out);
          offset++;
        }
        else if (tableSchema.getFields().get(offset).getType() == DataType.Type.SMALLINT) {
          long len = Varint.sizeOfSignedVarLong((Short)field);
          Varint.writeSignedVarLong(len, out);
          Varint.writeSignedVarLong((Short)field, out);
          offset++;
        }
        else if (tableSchema.getFields().get(offset).getType() == DataType.Type.TINYINT) {
          Varint.writeSignedVarLong(1, out);
          out.write((byte) field);
          offset++;
        }
        else if (tableSchema.getFields().get(offset).getType() == DataType.Type.CHAR) {
          byte[] bytes = (byte[]) field;
          Varint.writeSignedVarLong(bytes.length, out);
          out.write(bytes);
          offset++;
        }
        else if (tableSchema.getFields().get(offset).getType() == DataType.Type.NCHAR) {
          byte[] bytes = (byte[]) field;
          Varint.writeSignedVarLong(bytes.length, out);
          out.write(bytes);
          offset++;
        }
        else if (tableSchema.getFields().get(offset).getType() == DataType.Type.FLOAT) {
          Varint.writeSignedVarLong(8, out);
          out.writeDouble((Double) field);
          offset++;
        }
        else if (tableSchema.getFields().get(offset).getType() == DataType.Type.REAL) {
          Varint.writeSignedVarLong(4, out);
          out.writeFloat((Float) field);
          offset++;
        }
        else if (tableSchema.getFields().get(offset).getType() == DataType.Type.DOUBLE) {
          Varint.writeSignedVarLong(8, out);
          out.writeDouble((Double) field);
          offset++;
        }
        else if (tableSchema.getFields().get(offset).getType() == DataType.Type.VARCHAR ||
            tableSchema.getFields().get(offset).getType() == DataType.Type.NVARCHAR ||
            tableSchema.getFields().get(offset).getType() == DataType.Type.CLOB ||
            tableSchema.getFields().get(offset).getType() == DataType.Type.NCLOB ||
            tableSchema.getFields().get(offset).getType() == DataType.Type.LONGNVARCHAR ||
            tableSchema.getFields().get(offset).getType() == DataType.Type.LONGVARCHAR) {
          byte[] bytes = (byte[]) field;
          Varint.writeSignedVarLong(bytes.length, out);
          out.write(bytes);
          offset++;
        }
        else if (tableSchema.getFields().get(offset).getType() == DataType.Type.BOOLEAN) {
          Varint.writeSignedVarLong(1, out);
          out.write((Boolean) field ? 1 : 0);
          offset++;
        }
        else if (tableSchema.getFields().get(offset).getType() == DataType.Type.BIT) {
          Varint.writeSignedVarLong(1, out);
          out.write((Boolean) field ? 1 : 0);
          offset++;
        }
        else if (tableSchema.getFields().get(offset).getType() == DataType.Type.LONGVARBINARY ||
            tableSchema.getFields().get(offset).getType() == DataType.Type.VARBINARY ||
            tableSchema.getFields().get(offset).getType() == DataType.Type.BLOB) {
          byte[] bytes = (byte[]) field;
          Varint.writeSignedVarLong(bytes.length, out);
          out.write(bytes);
          offset++;
        }
        else if (tableSchema.getFields().get(offset).getType() == DataType.Type.NUMERIC) {
          BigDecimal value = ((BigDecimal) field);
          String strValue = value.toPlainString();
          byte[] bytes = strValue.getBytes("utf-8");
          Varint.writeSignedVarLong(bytes.length, out);
          out.write(bytes);
          offset++;
        }
        else if (tableSchema.getFields().get(offset).getType() == DataType.Type.DECIMAL) {
          BigDecimal value = ((BigDecimal) field);
          String strValue = value.toPlainString();
          byte[] bytes = strValue.getBytes("utf-8");
          Varint.writeSignedVarLong(bytes.length, out);
          out.write(bytes);
          offset++;
        }
        else if (tableSchema.getFields().get(offset).getType() == DataType.Type.DATE) {
          Date value = ((Date) field);
          String str = value.toString();
          byte[] bytes = str.getBytes("utf-8");
          Varint.writeSignedVarLong(bytes.length, out);
          out.write(bytes);
          offset++;
        }
        else if (tableSchema.getFields().get(offset).getType() == DataType.Type.TIME) {
          Time value = ((Time) field);
          String str = value.toString();
          byte[] bytes = str.getBytes("utf-8");
          Varint.writeSignedVarLong(bytes.length, out);
          out.write(bytes);
          offset++;
        }
        else if (tableSchema.getFields().get(offset).getType() == DataType.Type.TIMESTAMP) {
          Timestamp value = ((Timestamp) field);
          String str = value.toString();
          byte[] bytes = str.getBytes("utf-8");
          Varint.writeSignedVarLong(bytes.length, out);
          out.write(bytes);
          offset++;
        }
        else {
          tableSchema.getFields().get(offset).getType();
        }
      }
    }
    out.close();
    byte[] bytes = bytesOut.toByteArray();
    Varint.writeSignedVarLong(bytes.length, outerOut);
    outerOut.write(bytes);
  }

  public static Object[] deserializeFields(
      String dbName, DatabaseCommon common, DataInputStream in, TableSchema tableSchema, int schemaVersion,
      int dbViewNumber, Set<Integer> columns, boolean deserializeHeader) throws IOException {
    List<FieldSchema> currFieldList = tableSchema.getFields();
    List<FieldSchema> serializedFieldList = null;

    int serializedVersion = (int)Varint.readSignedVarLong(in);;

    serializedFieldList = tableSchema.getFieldsForVersion(schemaVersion, serializedVersion);

    Object[] fields = new Object[currFieldList.size()];
    int offset = 0;
    boolean isCurrentSchema = currFieldList == serializedFieldList;
    for (FieldSchema field : serializedFieldList) {
      int currOffset = 0;
      if (isCurrentSchema) {
        currOffset = offset++;
      }
      else {
        currOffset = field.getMapToOffset();
      }
      int size = (int) Varint.readSignedVarLong(in);
      if (size > 0) {
        if (columns != null) {
          if (!columns.contains(currOffset)) {
            in.skipBytes(size);
            continue;
          }
        }
        if (field.getType() == DataType.Type.BIGINT) {
          fields[currOffset] = Varint.readSignedVarLong(in);
        }
        else if (field.getType() == DataType.Type.INTEGER) {
          fields[currOffset] = (int) Varint.readSignedVarLong(in);
        }
        else if (field.getType() == DataType.Type.SMALLINT) {
          fields[currOffset] = (short) Varint.readSignedVarLong(in);
        }
        else if (field.getType() == DataType.Type.TINYINT) {
          fields[currOffset] = in.readByte();
        }
        else if (field.getType() == DataType.Type.NCHAR) {
          byte[] buffer = new byte[size];
          in.readFully(buffer);
          fields[currOffset] = buffer;
        }
        else if (field.getType() == DataType.Type.CHAR) {
          byte[] buffer = new byte[size];
          in.readFully(buffer);
          fields[currOffset] = buffer;
        }
        else if (field.getType() == DataType.Type.FLOAT) {
          fields[currOffset] = in.readDouble();
        }
        else if (field.getType() == DataType.Type.REAL) {
          fields[currOffset] = in.readFloat();
        }
        else if (field.getType() == DataType.Type.DOUBLE) {
          fields[currOffset] = in.readDouble();
        }
        else if (field.getType() == DataType.Type.VARCHAR ||
            field.getType() == DataType.Type.NVARCHAR ||
            field.getType() == DataType.Type.CLOB ||
            field.getType() == DataType.Type.NCLOB ||
            field.getType() == DataType.Type.LONGNVARCHAR ||
            field.getType() == DataType.Type.LONGVARCHAR) {
          byte[] buffer = new byte[size];
          in.readFully(buffer);
          fields[currOffset] = buffer;
        }
        else if (field.getType() == DataType.Type.BOOLEAN) {
          fields[currOffset] = in.readByte() == 1;
        }
        else if (field.getType() == DataType.Type.BIT) {
          fields[currOffset] = in.readByte() == 1;
        }
        else if (field.getType() == DataType.Type.VARBINARY ||
            field.getType() == DataType.Type.LONGVARBINARY ||
            field.getType() == DataType.Type.BLOB) {
          byte[] buffer = new byte[size];
          in.readFully(buffer);
          fields[currOffset] = buffer;
        }
        else if (field.getType() == DataType.Type.NUMERIC) {
          byte[] buffer = new byte[size];
          in.readFully(buffer);
          String str = new String(buffer, "utf-8");
          fields[currOffset] = new BigDecimal(str);
        }
        else if (field.getType() == DataType.Type.DECIMAL) {
          byte[] buffer = new byte[size];
          in.readFully(buffer);
          String str = new String(buffer, "utf-8");
          fields[currOffset] = new BigDecimal(str);
        }
        else if (field.getType() == DataType.Type.DATE) {
          byte[] buffer = new byte[size];
          in.readFully(buffer);
          String str = new String(buffer, "utf-8");
          fields[currOffset] = Date.valueOf(str);
        }
        else if (field.getType() == DataType.Type.TIME) {
          byte[] buffer = new byte[size];
          in.readFully(buffer);
          String str = new String(buffer, "utf-8");
          fields[currOffset] = Time.valueOf(str);
        }
        else if (field.getType() == DataType.Type.TIMESTAMP) {
          byte[] buffer = new byte[size];
          in.readFully(buffer);
          String str = new String(buffer, "utf-8");
          fields[currOffset] = Timestamp.valueOf(str);
        }
        else {
          System.out.println("unknown field");
        }
      }
      else {
        fields[currOffset] = null;
      }
    }
    return fields;
  }

  public int getShard() {
    return shard;
  }

  public int getReplica() {
    return replica;
  }

  public void setServersConfig(ServersConfig serversConfig) {
    this.serversConfig = serversConfig;

    Integer replicaCount = null;
    ServersConfig.Shard[] shards = serversConfig.getShards();
    for (ServersConfig.Shard shard : shards) {
      ServersConfig.Host[] replicas = shard.getReplicas();
      if (replicaCount == null) {
        replicaCount = replicas.length;
      }
      else {
        if (replicaCount != replicas.length) {
          throw new DatabaseException("Inconsistent replica count");
        }
      }
    }
  }

  public ServersConfig getServersConfig() {
    return serversConfig;
  }


  public Map<String, Schema> getDatabases() {
    return schema;
  }

  public void addDatabase(String dbName) {
    synchronized (this) {
      if (schema.get(dbName) != null) {
        return;
      }
      schema.put(dbName, new Schema());
      createSchemaLocks(dbName);
    }
  }

  public byte[] serializeConfig(short serializationVersionNumber) throws IOException {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytesOut);
    Varint.writeSignedVarLong(SERIALIZATION_VERSION, out);
    serversConfig.serialize(out, serializationVersionNumber);
    out.close();
    return bytesOut.toByteArray();
  }

  public void deserializeConfig(byte[] bytes) throws IOException {
    deserializeConfig(new DataInputStream(new ByteArrayInputStream(bytes)));
  }

  public void deserializeConfig(DataInputStream in) throws IOException {
    short serializationVersion = (short)Varint.readSignedVarLong(in);
    serversConfig = new ServersConfig(in, serializationVersion);
  }

  public void saveServersConfig(String dataDir) throws IOException {
    try {
      internalWriteLock.lock();
      String dataRoot = null;
      if (USE_SNAPSHOT_MGR_OLD) {
        dataRoot = new File(dataDir, "snapshot/" + shard + "/" + replica).getAbsolutePath();
      }
      else {
        dataRoot = new File(dataDir, "delta/" + shard + "/" + replica).getAbsolutePath();
      }
      File configFile = new File(dataRoot, "config.bin");
      if (configFile.exists()) {
        configFile.delete();
      }
      configFile.getParentFile().mkdirs();
      try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(configFile)))) {
        Varint.writeSignedVarLong(SERIALIZATION_VERSION, out);
        out.write(serializeConfig(SERIALIZATION_VERSION));
      }
    }
    finally {
      internalWriteLock.unlock();
    }
  }

  public int getSchemaVersion() {
    return schemaVersion;
  }

  public void dropTable(DatabaseClient client, String dbName, String tableName, String dataDir) {
    synchronized (this) {
      schema.get(dbName).getTables().remove(tableName);
    }
  }

  public static String keyToString(Object[] key) {
    try {
      if (key == null) {
        return "null";
      }
      StringBuilder keyStr = new StringBuilder("[");
      for (Object curr : key) {
        if (curr instanceof byte[]) {
          keyStr.append(",").append(new String((byte[]) curr, "utf-8"));
        }
        else {
          keyStr.append(",").append(curr);
        }
      }
      keyStr.append("]");
      return keyStr.toString();
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public void setSchema(String dbName, Schema schema) {
    this.schema.put(dbName, schema);
  }

  public void setHaveProLicense(boolean haveProLicense) {

    this.haveProLicense = haveProLicense;
  }

  public boolean haveProLicense() {

    return true;
    //return haveProLicense;
  }

  public void setSchemaVersion(int schemaVersion) {
    this.schemaVersion = schemaVersion;
  }

  public List<String> getDbNames(String dataDir) {
    File file = null;
    if (USE_SNAPSHOT_MGR_OLD) {
      file = new File(dataDir, "snapshot/" + shard + "/" + replica);
    }
    else {
      file = new File(dataDir, "delta/" + shard + "/" + replica);
    }
    Set<String> dbs = new HashSet<>();
    String[] dirs = file.list();
    List<String> ret = new ArrayList<>();
    if (dirs != null) {
      for (String dir : dirs) {
        if (dir.equals("config.bin")) {
          continue;
        }
        if (dir.equals("schema.bin")) {
          continue;
        }
        if (dir.equals("_sonicbase_schema")) {
          continue;
        }
        dbs.add(dir);
      }
    }
//    for (String db : common.getDatabases().keySet()) {
//      dbs.add(db);
//    }
    return new ArrayList<>(dbs);

  }

}