package com.sonicbase.common;

import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.*;
import org.apache.giraph.utils.Varint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.sonicbase.client.DatabaseClient.SERIALIZATION_VERSION;
import static com.sonicbase.client.DatabaseClient.SERIALIZATION_VERSION_21;
import static com.sonicbase.schema.DataType.Type.BIGINT;
import static java.sql.Types.*;

@ExcludeRename
@SuppressWarnings({"squid:S1199","squid:S1168", "squid:S00107"})
// don't want to extract code block into method
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class DatabaseCommon {

  private static final String SONICBASE_SCHEMA_STR = "_sonicbase_schema";
  private static final String SNAPSHOT_STR = "snapshot";
  private static final String UTF_8_STR = "utf-8";
  private static final String SCHEMA_BIN_STR = "schema.bin";
  private static Logger logger = LoggerFactory.getLogger(DatabaseCommon.class);

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

  public Lock getSchemaReadLock(String dbName) {
    return schemaReadLock.computeIfAbsent(dbName, k -> {
      ReadWriteLock lock = new ReentrantReadWriteLock();
      schemaReadWriteLock.put(dbName, lock);
      schemaWriteLock.put(dbName, lock.writeLock());
      return lock.readLock();
    });
  }

  public Lock getSchemaWriteLock(String dbName) {
    return schemaWriteLock.computeIfAbsent(dbName, k -> {
      ReadWriteLock lock = new ReentrantReadWriteLock();
      schemaReadWriteLock.put(dbName, lock);
      schemaReadLock.put(dbName, lock.readLock());
      return lock.writeLock();
    });
  }

  public Schema getSchema(String dbName) {

    return ensureSchemaExists(dbName);
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
    File file = new File(dataDir, SNAPSHOT_STR + File.separator + shard + File.separator + replica + File.separator +
        SONICBASE_SCHEMA_STR + File.separator + dbName);
    File tableDir = new File(file, tableName + File.separator + "table");
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
        String dataRoot = dataRoot = new File(dataDir, SNAPSHOT_STR + File.separator + shard + File.separator +
            replica).getAbsolutePath();
        File schemaFile = new File(dataRoot, SCHEMA_BIN_STR);
        logger.info("Loading schema: file={}", schemaFile.getAbsolutePath());
        if (schemaFile.exists()) {
          try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(schemaFile)))) {
            deserializeSchema(in);
          }
        }
        else {
          logger.info("No schema file found");
        }
        List<String> dbNames = getDbNames(dataDir);
        for (String dbName : dbNames) {
          Schema dbSchema = schema.get(dbName);
          if (dbSchema == null) {
            dbSchema = new Schema();
            schema.put(dbName, dbSchema);
          }
          File file = new File(dataDir, SNAPSHOT_STR + File.separator + shard + File.separator + replica +
              File.separator + SONICBASE_SCHEMA_STR + File.separator + dbName);
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
          existingSchema.getIndexesById().clear();
          existingSchema.getIndices().clear(); //ignore indices in the table will read index files

          dbSchema.getTables().put(tableSchema.getName(), tableSchema);
          dbSchema.getTablesById().put(tableSchema.getTableId(), tableSchema);
          File indicesDir = new File(tableFile, File.separator + "indices");
          if (indicesDir.exists()) {
            File[] indices = indicesDir.listFiles();
            loadIndicesForTableSchema(previousTableSchema, tableSchema, indices);
          }
        }
        catch (Exception e) {
          throw new DatabaseException("Error deserializing tableSchema: file=" + tableSchemaFile.getAbsolutePath());
        }
      }
    }
  }

  private void loadIndicesForTableSchema(TableSchema previousTableSchema, TableSchema tableSchema,
                                         File[] indices) throws IOException {
    if (indices != null) {
      for (File indexDir : indices) {
        String indexName = indexDir.getName();
        IndexSchema previousIndexSchema = previousTableSchema == null ? null :
            previousTableSchema.getIndices().get(indexName);
        File[] indexSchemas = indexDir.listFiles();
        if (indexSchemas != null && indexSchemas.length > 0) {
          sortSchemaFiles(indexSchemas);
          File indexSchemaFile = indexSchemas[indexSchemas.length - 1];
          try (DataInputStream indexIn = new DataInputStream(new FileInputStream(indexSchemaFile))) {
            indexIn.readShort(); //serializationVersion
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

  public void saveSchema(String dataDir) {
    try {
      internalWriteLock.lock();
      String dataRoot = new File(dataDir, SNAPSHOT_STR + File.separator + shard + File.separator +
          replica).getAbsolutePath();
      File schemaFile = new File(dataRoot, SCHEMA_BIN_STR);
      if (schemaFile.exists()) {
        Files.delete(schemaFile.toPath());
      }

      schemaFile.getParentFile().mkdirs();
      try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(schemaFile)))) {
        if (getShard() == 0 &&
            getServersConfig().getShards()[0].getMasterReplica() == getReplica()) {
          this.schemaVersion++;
        }
        serializeSchema(out, SERIALIZATION_VERSION);

      }
      loadSchema(dataDir);
      logger.info("Saved schema - postLoad: dir={}", dataRoot);

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
        out.writeUTF(dbName);
        serializeSchema(dbName, out);
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

  public void updateTable(String dbName, TableSchema tableSchema) {
    synchronized (this) {
      schema.get(dbName).updateTable(tableSchema);
    }
  }


  public void addTable(String dbName,TableSchema schema) {
    synchronized (this) {
      Schema retSchema = ensureSchemaExists(dbName);
      retSchema.addTable(schema);
    }
  }

  private Schema ensureSchemaExists(String dbName) {
    synchronized (this) {
      return this.schema.computeIfAbsent(dbName, k -> new Schema());
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
        Schema newSchema = new Schema();
        newSchema.deserialize(in);
        schema.put(dbName, newSchema);
        createSchemaLocks(dbName);
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
      Varint.readSignedVarLong(in); // serializationVersion
      Varint.readSignedVarLong(in);
      indexId = (int) Varint.readSignedVarLong(in);
      IndexSchema indexSchema = tableSchema.getIndexesById().get(indexId);
      int[] columns = indexSchema.getFieldOffsets();
      int keyLength = (int) Varint.readSignedVarLong(in);
      Object[] fields = new Object[keyLength];
      for (int i = 0; i < keyLength; i++) {
        if (in.readBoolean()) {
          switch (tableSchema.getFields().get(columns[i]).getType()) {
            case BIGINT:
              fields[i] = Varint.readSignedVarLong(in);
              break;
            case INTEGER:
              fields[i] = (int) Varint.readSignedVarLong(in);
              break;
            case SMALLINT:
              fields[i] = (short) Varint.readSignedVarLong(in);
              break;
            case TINYINT:
              fields[i] = in.readByte();
              break;
            case FLOAT:
            case DOUBLE:
              fields[i] = in.readDouble();
              break;
            case REAL:
              fields[i] = in.readFloat();
              break;
            case BOOLEAN:
            case BIT:
              fields[i] = in.readBoolean();
              break;
            case CHAR:
            case NCHAR:
            case VARCHAR:
            case LONGVARCHAR:
            case LONGNVARCHAR:
            case CLOB:
            case NCLOB:
            case NVARCHAR: {
              int len = (int) Varint.readSignedVarLong(in);
              byte[] bytes = new byte[len];
              in.read(bytes);
              fields[i] = bytes;
            }
              break;
            case LONGVARBINARY:
            case VARBINARY:
            case BLOB: {
              int len = (int) Varint.readSignedVarLong(in);
              byte[] data = new byte[len];
              in.readFully(data);
              fields[i] = data;
            }
              break;
            case NUMERIC: {
              int len = (int) Varint.readSignedVarLong(in);
              byte[] buffer = new byte[len];
              in.readFully(buffer);
              String str = new String(buffer, UTF_8_STR);
              fields[i] = new BigDecimal(str);
            }
              break;
            case DECIMAL: {
              int len = (int) Varint.readSignedVarLong(in);
              byte[] buffer = new byte[len];
              in.readFully(buffer);
              String str = new String(buffer, UTF_8_STR);
              fields[i] = new BigDecimal(str);
            }
              break;
            case DATE: {
              int len = (int) Varint.readSignedVarLong(in);
              byte[] buffer = new byte[len];
              in.readFully(buffer);
              String str = new String(buffer, UTF_8_STR);
              fields[i] = Date.valueOf(str);
            }
              break;
            case TIME: {
              int len = (int) Varint.readSignedVarLong(in);
              byte[] buffer = new byte[len];
              in.readFully(buffer);
              String str = new String(buffer, UTF_8_STR);
              fields[i] = Time.valueOf(str);
            }
              break;
            case TIMESTAMP: {
              int len = (int) Varint.readSignedVarLong(in);
              byte[] buffer = new byte[len];
              in.readFully(buffer);
              String str = new String(buffer, UTF_8_STR);
              fields[i] = Timestamp.valueOf(str);
            }
              break;
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
    Varint.readSignedVarLong(in); // serializationVersion
    Varint.readSignedVarLong(in);
    int indexId = (int) Varint.readSignedVarLong(in);
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

  public static Object[] deserializeKey(DataType.Type[] types, DataInputStream in) throws EOFException {
    try {
      Varint.readSignedVarLong(in); // serializationVersion
      Varint.readSignedVarLong(in);
      Varint.readSignedVarLong(in); //indexId
      int keyLength = (int) Varint.readSignedVarLong(in);
      Object[] fields = new Object[keyLength];
      for (int i = 0; i < keyLength; i++) {
        if (in.readBoolean()) {
          switch (types[i]) {
            case BIGINT:
              fields[i] = Varint.readSignedVarLong(in);
              break;
            case INTEGER:
              fields[i] = (int) Varint.readSignedVarLong(in);
              break;
            case SMALLINT:
              fields[i] = (short) Varint.readSignedVarLong(in);
              break;
            case TINYINT:
              fields[i] = in.readByte();
              break;
            case FLOAT:
              fields[i] = in.readDouble();
              break;
            case REAL:
              fields[i] = in.readFloat();
              break;
            case DOUBLE:
              fields[i] = in.readDouble();
              break;
            case BOOLEAN:
              fields[i] = in.readBoolean();
              break;
            case BIT:
              fields[i] = in.readBoolean();
              break;
            case CHAR:
            case NCHAR:
            case VARCHAR:
            case LONGVARCHAR:
            case LONGNVARCHAR:
            case CLOB:
            case NCLOB:
            case NVARCHAR: {
              int len = (int) Varint.readSignedVarLong(in);
              byte[] bytes = new byte[len];
              in.read(bytes);
              fields[i] = bytes;
              break;
            }
            case LONGVARBINARY:
            case VARBINARY:
            case BLOB: {
              int len = (int) Varint.readSignedVarLong(in);
              byte[] data = new byte[len];
              in.readFully(data);
              fields[i] = data;
              break;
            }
            case NUMERIC:
            case DECIMAL: {
              int len = (int) Varint.readSignedVarLong(in);
              byte[] buffer = new byte[len];
              in.readFully(buffer);
              String str = new String(buffer, UTF_8_STR);
              fields[i] = new BigDecimal(str);
              break;
            }
            case DATE: {
              int len = (int) Varint.readSignedVarLong(in);
              byte[] buffer = new byte[len];
              in.readFully(buffer);
              String str = new String(buffer, UTF_8_STR);
              fields[i] = Date.valueOf(str);
              break;
            }
            case TIME: {
              int len = (int) Varint.readSignedVarLong(in);
              byte[] buffer = new byte[len];
              in.readFully(buffer);
              String str = new String(buffer, UTF_8_STR);
              fields[i] = Time.valueOf(str);
              break;
            }
            case TIMESTAMP: {
              int len = (int) Varint.readSignedVarLong(in);
              byte[] buffer = new byte[len];
              in.readFully(buffer);
              String str = new String(buffer, UTF_8_STR);
              fields[i] = Timestamp.valueOf(str);
              break;
            }
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
      Varint.writeSignedVarLong(tableSchema.getIndices().get(indexName).getIndexId(), out);
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
              switch (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType()) {
                case BIGINT:
                  Varint.writeSignedVarLong((Long) key[i], out);
                  break;
                case INTEGER:
                  Varint.writeSignedVarLong((Integer) key[i], out);
                  break;
                case SMALLINT:
                  Varint.writeSignedVarLong((Short) key[i], out);
                  break;
                case TINYINT:
                  out.write((byte) key[i]);
                  break;
                case FLOAT:
                  out.writeDouble((Double) key[i]);
                  break;
                case REAL:
                  out.writeFloat((Float) key[i]);
                  break;
                case DOUBLE:
                  out.writeDouble((Double) key[i]);
                  break;
                case BOOLEAN:
                  out.writeBoolean((Boolean) key[i]);
                  break;
                case BIT:
                  out.writeBoolean((Boolean) key[i]);
                  break;
                case CHAR:
                case NCHAR:
                case VARCHAR:
                case CLOB:
                case NCLOB:
                case LONGNVARCHAR:
                case NVARCHAR:
                case LONGVARCHAR:
                case LONGVARBINARY:
                case VARBINARY:
                case BLOB: {
                  byte[] bytes = (byte[]) key[i];
                  if (bytes == null) {
                    Varint.writeSignedVarLong(0, out);
                  }
                  else {
                    Varint.writeSignedVarLong(bytes.length, out);
                    out.write(bytes);
                  }
                  break;
                }
                case NUMERIC:
                case DECIMAL: {
                  BigDecimal value = ((BigDecimal) key[i]);
                  String strValue = value.toPlainString();
                  byte[] bytes = strValue.getBytes(UTF_8_STR);
                  Varint.writeSignedVarLong(bytes.length, out);
                  out.write(bytes);
                  break;
                }
                case DATE: {
                  Date value = ((Date) key[i]);
                  String str = value.toString();
                  byte[] bytes = str.getBytes(UTF_8_STR);
                  Varint.writeSignedVarLong(bytes.length, out);
                  out.write(bytes);
                  break;
                }
                case TIME: {
                  Time value = ((Time) key[i]);
                  String str = value.toString();
                  byte[] bytes = str.getBytes(UTF_8_STR);
                  Varint.writeSignedVarLong(bytes.length, out);
                  out.write(bytes);
                  break;
                }
                case TIMESTAMP: {
                  Timestamp value = ((Timestamp) key[i]);
                  String str = value.toString();
                  byte[] bytes = str.getBytes(UTF_8_STR);
                  Varint.writeSignedVarLong(bytes.length, out);
                  out.write(bytes);
                  break;
                }
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

      Varint.readUnsignedVarLong(in); // serializationVersion
      int count = (int) Varint.readUnsignedVarLong(in);
      if (count == 0) {
        return null;
      }
      Object[] ret = new Object[count];
      for (int i = 0; i < count; i++) {
        if (in.readBoolean()) {
          int type = (int) Varint.readSignedVarLong(in);
          switch (type) {
            case Types.BIGINT:
              ret[i] = Varint.readSignedVarLong(in);
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
              String value = new String(buffer, UTF_8_STR);
              ret[i] = new BigDecimal(value);
              break;
            }
            case DATE: {
              int len = (int) Varint.readSignedVarLong(in);
              byte[] buffer = new byte[len];
              in.readFully(buffer);
              String value = new String(buffer, UTF_8_STR);
              ret[i] = Date.valueOf(value);
            }
              break;
            case TIME: {
              int len = (int) Varint.readSignedVarLong(in);
              byte[] buffer = new byte[len];
              in.readFully(buffer);
              String value = new String(buffer, UTF_8_STR);
              ret[i] = Time.valueOf(value);
            }
              break;
            case TIMESTAMP: {
              int len = (int) Varint.readSignedVarLong(in);
              byte[] buffer = new byte[len];
              in.readFully(buffer);
              String value = new String(buffer, UTF_8_STR);
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
              byte[] bytes = strValue.getBytes(UTF_8_STR);
              Varint.writeSignedVarLong(bytes.length, out);
              out.write(bytes);
            }
            else if (key[i] instanceof Date) {
              Date value = ((Date) key[i]);
              String str = value.toString();
              byte[] bytes = str.getBytes(UTF_8_STR);
              Varint.writeSignedVarLong(bytes.length, out);
              out.write(bytes);
            }
            else if (key[i] instanceof Time) {
              Time value = ((Time) key[i]);
              String str = value.toString();
              byte[] bytes = str.getBytes(UTF_8_STR);
              Varint.writeSignedVarLong(bytes.length, out);
              out.write(bytes);
            }
            else if (key[i] instanceof Timestamp) {
              Timestamp value = ((Timestamp) key[i]);
              String str = value.toString();
              byte[] bytes = str.getBytes(UTF_8_STR);
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
      Object[] fields, DataOutputStream outerOut, TableSchema tableSchema, int schemaVersion) throws IOException {

    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytesOut);

    Varint.writeSignedVarLong(schemaVersion, out);

    int offset = 0;
    for (Object field : fields) {
      if (field == null) {
        Varint.writeSignedVarLong(0, out);
        offset++;
      }
      else {
        switch (tableSchema.getFields().get(offset).getType()) {
          case BIGINT: {
            long len = Varint.sizeOfSignedVarLong((Long) field);
            Varint.writeSignedVarLong(len, out);
            Varint.writeSignedVarLong((Long) field, out);
            offset++;
            break;
          }
          case INTEGER: {
            long len = Varint.sizeOfSignedVarLong((Integer) field);
            Varint.writeSignedVarLong(len, out);
            Varint.writeSignedVarLong((Integer) field, out);
            offset++;
            break;
          }
          case SMALLINT: {
            long len = Varint.sizeOfSignedVarLong((Short) field);
            Varint.writeSignedVarLong(len, out);
            Varint.writeSignedVarLong((Short) field, out);
            offset++;
            break;
          }
          case TINYINT:
            Varint.writeSignedVarLong(1, out);
            out.write((byte) field);
            offset++;
            break;
          case FLOAT:
          case DOUBLE:
            Varint.writeSignedVarLong(8, out);
            out.writeDouble((Double) field);
            offset++;
            break;
          case REAL:
            Varint.writeSignedVarLong(4, out);
            out.writeFloat((Float) field);
            offset++;
            break;
          case CHAR:
          case NCHAR:
          case VARCHAR:
          case NVARCHAR:
          case CLOB:
          case NCLOB:
          case LONGNVARCHAR:
          case LONGVARCHAR:
          case LONGVARBINARY:
          case VARBINARY:
          case BLOB: {
            byte[] bytes = (byte[]) field;
            Varint.writeSignedVarLong(bytes.length, out);
            out.write(bytes);
            offset++;
            break;
          }
          case BOOLEAN:
          case BIT:
            Varint.writeSignedVarLong(1, out);
            out.write((Boolean) field ? 1 : 0);
            offset++;
            break;
          case NUMERIC:
          case DECIMAL: {
            BigDecimal value = ((BigDecimal) field);
            String strValue = value.toPlainString();
            byte[] bytes = strValue.getBytes(UTF_8_STR);
            Varint.writeSignedVarLong(bytes.length, out);
            out.write(bytes);
            offset++;
            break;
          }
          case DATE: {
            Date value = ((Date) field);
            String str = value.toString();
            byte[] bytes = str.getBytes(UTF_8_STR);
            Varint.writeSignedVarLong(bytes.length, out);
            out.write(bytes);
            offset++;
            break;
          }
          case TIME: {
            Time value = ((Time) field);
            String str = value.toString();
            byte[] bytes = str.getBytes(UTF_8_STR);
            Varint.writeSignedVarLong(bytes.length, out);
            out.write(bytes);
            offset++;
            break;
          }
          case TIMESTAMP: {
            Timestamp value = ((Timestamp) field);
            String str = value.toString();
            byte[] bytes = str.getBytes(UTF_8_STR);
            Varint.writeSignedVarLong(bytes.length, out);
            out.write(bytes);
            offset++;
            break;
          }
          default:
            tableSchema.getFields().get(offset).getType();
            break;
        }
      }
    }
    out.close();
    byte[] bytes = bytesOut.toByteArray();
    Varint.writeSignedVarLong(bytes.length, outerOut);
    outerOut.write(bytes);
  }

  public static Object[] deserializeFields(
      DataInputStream in, TableSchema tableSchema, int schemaVersion,
      Set<Integer> columns) throws IOException {
    List<FieldSchema> currFieldList = tableSchema.getFields();
    List<FieldSchema> serializedFieldList = null;

    int serializedVersion = (int)Varint.readSignedVarLong(in);

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
        if (columns != null && !columns.contains(currOffset)) {
          in.skipBytes(size);
          continue;
        }
        if (field.getType() == BIGINT) {
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
        else if (field.getType() == DataType.Type.FLOAT) {
          fields[currOffset] = in.readDouble();
        }
        else if (field.getType() == DataType.Type.REAL) {
          fields[currOffset] = in.readFloat();
        }
        else if (field.getType() == DataType.Type.DOUBLE) {
          fields[currOffset] = in.readDouble();
        }
        else if (field.getType() == DataType.Type.NCHAR ||
            field.getType() == DataType.Type.CHAR ||
            field.getType() == DataType.Type.VARCHAR ||
            field.getType() == DataType.Type.NVARCHAR ||
            field.getType() == DataType.Type.CLOB ||
            field.getType() == DataType.Type.NCLOB ||
            field.getType() == DataType.Type.LONGNVARCHAR ||
            field.getType() == DataType.Type.LONGVARCHAR ||
            field.getType() == DataType.Type.VARBINARY ||
            field.getType() == DataType.Type.LONGVARBINARY ||
            field.getType() == DataType.Type.BLOB) {
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
        else if (field.getType() == DataType.Type.DECIMAL ||
            field.getType() == DataType.Type.NUMERIC) {
          byte[] buffer = new byte[size];
          in.readFully(buffer);
          String str = new String(buffer, UTF_8_STR);
          fields[currOffset] = new BigDecimal(str);
        }
        else if (field.getType() == DataType.Type.DATE) {
          byte[] buffer = new byte[size];
          in.readFully(buffer);
          String str = new String(buffer, UTF_8_STR);
          fields[currOffset] = Date.valueOf(str);
        }
        else if (field.getType() == DataType.Type.TIME) {
          byte[] buffer = new byte[size];
          in.readFully(buffer);
          String str = new String(buffer, UTF_8_STR);
          fields[currOffset] = Time.valueOf(str);
        }
        else if (field.getType() == DataType.Type.TIMESTAMP) {
          byte[] buffer = new byte[size];
          in.readFully(buffer);
          String str = new String(buffer, UTF_8_STR);
          fields[currOffset] = Timestamp.valueOf(str);
        }
        else {
          logger.info("unknown field");
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
    for (ServersConfig.Shard localShard : shards) {
      ServersConfig.Host[] replicas = localShard.getReplicas();
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
      String dataRoot = dataRoot = new File(dataDir, SNAPSHOT_STR + File.separator + shard + File.separator +
          replica).getAbsolutePath();
      File configFile = new File(dataRoot, "config.bin");
      if (configFile.exists()) {
        Files.delete(configFile.toPath());
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

  public void dropTable(String dbName, String tableName) {
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
          keyStr.append(",").append(new String((byte[]) curr, UTF_8_STR));
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
  }

  public void setSchemaVersion(int schemaVersion) {
    this.schemaVersion = schemaVersion;
  }

  public List<String> getDbNames(String dataDir) {
    File file = new File(dataDir, SNAPSHOT_STR + File.separator + shard + File.separator + replica);
    Set<String> dbs = new HashSet<>();
    String[] dirs = file.list();
    if (dirs != null) {
      for (String dir : dirs) {
        if (dir.equals("config.bin")) {
          continue;
        }
        if (dir.equals(SCHEMA_BIN_STR)) {
          continue;
        }
        if (dir.equals(SONICBASE_SCHEMA_STR)) {
          continue;
        }
        dbs.add(dir);
      }
    }
    return new ArrayList<>(dbs);
  }
}
