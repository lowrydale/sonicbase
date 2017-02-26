package com.sonicbase.common;

import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.*;
import com.sonicbase.server.DatabaseServer;
import com.sonicbase.server.SnapshotManager;
import com.sonicbase.util.DataUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * User: lowryda
 * Date: 9/30/14
 * Time: 11:08 PM
 */
public class DatabaseCommon {

  private static Logger logger = LoggerFactory.getLogger(DatabaseCommon.class);


  private int shard = -1;
  private int replica = -1;
  private ConcurrentHashMap<String, Schema> schema = new ConcurrentHashMap<>();
  private ConcurrentHashMap<String, ReadWriteLock> schemaReadWriteLock = new ConcurrentHashMap<>();
  private ConcurrentHashMap<String, Lock> schemaReadLock = new ConcurrentHashMap<>();
  private ConcurrentHashMap<String, Lock> schemaWriteLock = new ConcurrentHashMap<>();
  private DatabaseServer.ServersConfig serversConfig;
  private int schemaVersion;


  public Lock getSchemaReadLock(String dbName) {
    return schemaReadLock.get(dbName);
  }

  public Lock getSchemaWriteLock(String dbName) {
    return schemaWriteLock.get(dbName);
  }

  public Schema getSchema(String dbName) {
    return schema.get(dbName);
  }

  public Map<String, TableSchema> getTables(String dbName) {
    return schema.get(dbName).getTables();
  }

  public Map<Integer, TableSchema> getTablesById(String dbName) {
    return schema.get(dbName).getTablesById();
  }

  public void loadSchema(String dataDir) {
    try {
      synchronized (this) {
        String dataRoot = new File(dataDir, "snapshot/" + shard + "/" + replica).getAbsolutePath();
        File schemaFile = new File(dataRoot, "schema.bin");
        logger.info("Loading schema: file=" + schemaFile.getAbsolutePath());
        if (schemaFile.exists()) {
          try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(schemaFile)))) {
            long serializationVersion = DataUtil.readVLong(in);
            deserializeSchema(this, in);
          }
        }
        else {
          logger.info("No schema file found");
        }
      }
      for (String dbName : schema.keySet()) {
        createSchemaLocks(dbName);
      }
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
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
      synchronized (this) {
        String dataRoot = new File(dataDir, "snapshot/" + shard + "/" + replica).getAbsolutePath();
        File schemaFile = new File(dataRoot, "schema.bin");
        if (schemaFile.exists()) {
          schemaFile.delete();
        }

        schemaFile.getParentFile().mkdirs();
        try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(schemaFile)))) {
          DataUtil.writeVLong(out, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
          if (shard == 0 && replica == 0) {
            this.schemaVersion++;
            //          schema.get(dbName).incrementSchemaVersion();
          }
          serializeSchema(out);
        }

        loadSchema(dataDir);
        logger.info("Saved schema - postLoad: dir=" + dataRoot);
      }
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public void serializeSchema(DataOutputStream out) throws IOException {
    DataUtil.writeVLong(out, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
    out.writeInt(this.schemaVersion);
    if (serversConfig == null) {
      out.writeBoolean(false);
    }
    else {
      out.writeBoolean(true);
      out.write(serializeConfig());
    }
    out.writeInt(schema.keySet().size());
    for (String dbName : schema.keySet()) {
      getSchemaReadLock(dbName).lock();
      try {
        out.writeUTF(dbName);
        serializeSchema(dbName, out);
      }
      finally {
        getSchemaReadLock(dbName).unlock();
      }
    }
  }

  public void setShard(int shard) {
    this.shard = shard;
  }

  public void setReplica(int replica) {
    this.replica = replica;
  }

  public void updateTable(String dbName, String dataDir, TableSchema tableSchema) {
    schema.get(dbName).updateTable(tableSchema);
    saveSchema(dataDir);
  }


  public void addTable(String dbName, String dataDir, TableSchema schema) {
    this.schema.get(dbName).addTable(schema);
    saveSchema(dataDir);
  }

  public void serializeSchema(String dbName, DataOutputStream out) {
    schema.get(dbName).serialize(out);
  }

  public void deserializeSchema(DatabaseCommon common, DataInputStream in) {

    try {
      long serializationVersion = DataUtil.readVLong(in);
      this.schemaVersion = in.readInt();
      if (in.readBoolean()) {
        deserializeConfig(in);
      }
      int count = in.readInt();
      for (int i = 0; i < count; i++) {
        String dbName = in.readUTF();
        Schema newSchema = new Schema();
        newSchema.deserialize(common, in);
        schema.put(dbName, newSchema);
      }
    }
    catch (IOException e) {
      throw new DatabaseException(e);
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

  public static Object[] deserializeKey(TableSchema tableSchema, DataInputStream in) {
    try {
      long serializationVersion = DataUtil.readVLong(in);
      DataUtil.ResultLength resultLength = new DataUtil.ResultLength();
      DataUtil.readVLong(in, resultLength);
      int indexId = (int) DataUtil.readVLong(in, resultLength);
      //logger.info("tableId=" + tableId + " indexId=" + indexId + ", indexCount=" + tableSchema.getIndices().size());
      IndexSchema indexSchema = tableSchema.getIndexesById().get(indexId);
      String[] columns = indexSchema.getFields();
      int keyLength = (int) DataUtil.readVLong(in, resultLength);
      Object[] fields = new Object[keyLength];
      for (int i = 0; i < keyLength; i++) {
        String column = columns[i];
        if (column != null) {
          if (in.readBoolean()) {
            if (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.BIGINT) {
              fields[i] = DataUtil.readVLong(in, resultLength);
            }
            else if (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.INTEGER) {
              fields[i] = (int) DataUtil.readVLong(in, resultLength);
            }
            else if (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.SMALLINT) {
              fields[i] = (short) DataUtil.readVLong(in, resultLength);
            }
            else if (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.TINYINT) {
              fields[i] = in.readByte();
            }
            else if (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.CHAR) {
              int len = (int) DataUtil.readVLong(in, resultLength);
              byte[] bytes = new byte[len];
              in.read(bytes);
              fields[i] = bytes;
            }
            else if (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.NCHAR) {
              int len = (int) DataUtil.readVLong(in, resultLength);
              byte[] bytes = new byte[len];
              in.read(bytes);
              fields[i] = bytes;
            }
            else if (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.FLOAT) {
              fields[i] = in.readDouble();
            }
            else if (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.REAL) {
              fields[i] = in.readFloat();
            }
            else if (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.DOUBLE) {
              fields[i] = in.readDouble();
            }
            else if (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.BOOLEAN) {
              fields[i] = in.readBoolean();
            }
            else if (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.BIT) {
              fields[i] = in.readBoolean();
            }
            else if (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.VARCHAR ||
                tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.LONGVARCHAR ||
                tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.LONGNVARCHAR ||
                tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.CLOB ||
                tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.NCLOB ||
                tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.NVARCHAR) {
              int len = (int) DataUtil.readVLong(in, resultLength);
              byte[] bytes = new byte[len];
              in.read(bytes);
              fields[i] = bytes;
            }
            else if (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.LONGVARBINARY ||
                tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.VARBINARY ||
                tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.BLOB) {
              int len = (int) DataUtil.readVLong(in, resultLength);
              byte[] data = new byte[len];
              in.readFully(data);
              fields[i] = data;
            }
            else if (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.NUMERIC) {
              int len = (int) DataUtil.readVLong(in, resultLength);
              byte[] buffer = new byte[len];
              in.readFully(buffer);
              String str = new String(buffer, "utf-8");
              fields[i] = new BigDecimal(str);
            }
            else if (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.DECIMAL) {
              int len = (int) DataUtil.readVLong(in, resultLength);
              byte[] buffer = new byte[len];
              in.readFully(buffer);
              String str = new String(buffer, "utf-8");
              fields[i] = new BigDecimal(str);
            }
            else if (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.DATE) {
              int len = (int) DataUtil.readVLong(in, resultLength);
              byte[] buffer = new byte[len];
              in.readFully(buffer);
              String str = new String(buffer, "utf-8");
              fields[i] = Date.valueOf(str);
            }
            else if (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.TIME) {
              int len = (int) DataUtil.readVLong(in, resultLength);
              byte[] buffer = new byte[len];
              in.readFully(buffer);
              String str = new String(buffer, "utf-8");
              fields[i] = Time.valueOf(str);
            }
            else if (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.TIMESTAMP) {
              int len = (int) DataUtil.readVLong(in, resultLength);
              byte[] buffer = new byte[len];
              in.readFully(buffer);
              String str = new String(buffer, "utf-8");
              fields[i] = Timestamp.valueOf(str);
            }
          }
        }
      }
      return fields;
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }


  public static byte[] serializeKey(TableSchema tableSchema, String indexName, Object[] key) {
    try {
      DataUtil.ResultLength resultLength = new DataUtil.ResultLength();
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytesOut);
      DataUtil.writeVLong(out, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
      DataUtil.writeVLong(out, tableSchema.getTableId(), resultLength);
      DataUtil.writeVLong(out, tableSchema.getIndexes().get(indexName).getIndexId(), resultLength);
      IndexSchema indexSchema = tableSchema.getIndices().get(indexName);
      String[] columns = indexSchema.getFields();
      if (key == null) {
        DataUtil.writeVLong(out, 0, resultLength);
      }
      else {
        DataUtil.writeVLong(out, columns.length, resultLength);
        for (int i = 0; i < columns.length; i++) {
          String column = columns[i];
          if (column != null) {
            if (i >= key.length || key[i] == null) {
              out.writeBoolean(false);
            }
            else {
              out.writeBoolean(true);
              if (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.BIGINT) {
                DataUtil.writeVLong(out, (Long) key[i], resultLength);
              }
              else if (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.INTEGER) {
                DataUtil.writeVLong(out, (Integer) key[i], resultLength);
              }
              else if (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.SMALLINT) {
                DataUtil.writeVLong(out, (Short) key[i], resultLength);
              }
              else if (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.TINYINT) {
                out.write((byte) key[i]);
              }
              else if (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.CHAR) {
                byte[] bytes = (byte[]) key[i];
                if (bytes == null) {
                  DataUtil.writeVLong(out, 0, resultLength);
                }
                else {
                  DataUtil.writeVLong(out, bytes.length, resultLength);
                  out.write(bytes);
                }
              }
              else if (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.NCHAR) {
                byte[] bytes = (byte[]) key[i];
                if (bytes == null) {
                  DataUtil.writeVLong(out, 0, resultLength);
                }
                else {
                  DataUtil.writeVLong(out, bytes.length, resultLength);
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
                  DataUtil.writeVLong(out, 0, resultLength);
                }
                else {
                  DataUtil.writeVLong(out, bytes.length, resultLength);
                  out.write(bytes);
                }
              }
              else if (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.LONGVARBINARY ||
                  tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.VARBINARY ||
                  tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.BLOB) {
                byte[] bytes = (byte[])key[i];
                if (bytes == null) {
                  DataUtil.writeVLong(out, 0, resultLength);
                }
                else {
                  DataUtil.writeVLong(out, bytes.length, resultLength);
                  out.write(bytes);
                }
              }
              else if (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.NUMERIC) {
                BigDecimal value = ((BigDecimal) key[i]);
                String strValue = value.toPlainString();
                byte[] bytes = strValue.getBytes("utf-8");
                DataUtil.writeVLong(out, bytes.length, resultLength);
                out.write(bytes);
              }
              else if (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.DECIMAL) {
                BigDecimal value = ((BigDecimal) key[i]);
                String strValue = value.toPlainString();
                byte[] bytes = strValue.getBytes("utf-8");
                DataUtil.writeVLong(out, bytes.length, resultLength);
                out.write(bytes);
              }
              else if (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.DATE) {
                Date value = ((Date) key[i]);
                String str = value.toString();
                byte[] bytes = str.getBytes("utf-8");
                DataUtil.writeVLong(out, bytes.length, resultLength);
                out.write(bytes);
              }
              else if (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.TIME) {
                Time value = ((Time) key[i]);
                String str = value.toString();
                byte[] bytes = str.getBytes("utf-8");
                DataUtil.writeVLong(out, bytes.length, resultLength);
                out.write(bytes);
              }
              else if (tableSchema.getFields().get(tableSchema.getFieldOffset(column)).getType() == DataType.Type.TIMESTAMP) {
                Timestamp value = ((Timestamp) key[i]);
                String str = value.toString();
                byte[] bytes = str.getBytes("utf-8");
                DataUtil.writeVLong(out, bytes.length, resultLength);
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

  public static void serializeFields(
      Object[] fields, DataOutputStream outerOut, TableSchema tableSchema, int schemaVersion, boolean serializeHeader) throws IOException {

    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytesOut);
    DataUtil.ResultLength resultLength = new DataUtil.ResultLength();
    if (serializeHeader) {
      DataUtil.writeVLong(out, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
      DataUtil.writeVLong(out, schemaVersion);
      DataUtil.writeVLong(out, fields.length, resultLength);
    }
    int offset = 0;
    byte[] buffer = new byte[16];
    for (Object field : fields) {
      if (field == null) {
        DataUtil.writeVLong(out, 0);
        offset++;
      }
      else {
        if (tableSchema.getFields().get(offset).getType() == DataType.Type.BIGINT) {
          DataUtil.writeVLong(buffer, (Long) field, resultLength);
          DataUtil.writeVLong(out, resultLength.getLength());
          out.write(buffer, 0, resultLength.getLength());
          offset++;
        }
        else if (tableSchema.getFields().get(offset).getType() == DataType.Type.INTEGER) {
          DataUtil.writeVLong(buffer, (Integer) field, resultLength);
          DataUtil.writeVLong(out, resultLength.getLength());
          out.write(buffer, 0, resultLength.getLength());
          offset++;
        }
        else if (tableSchema.getFields().get(offset).getType() == DataType.Type.SMALLINT) {
          DataUtil.writeVLong(buffer, (Short) field, resultLength);
          DataUtil.writeVLong(out, resultLength.getLength());
          out.write(buffer, 0, resultLength.getLength());
          offset++;
        }
        else if (tableSchema.getFields().get(offset).getType() == DataType.Type.TINYINT) {
          DataUtil.writeVLong(out, 1);
          out.write((byte)field);
          offset++;
        }
        else if (tableSchema.getFields().get(offset).getType() == DataType.Type.CHAR) {
          byte[] bytes = (byte[])field;
          DataUtil.writeVLong(out, bytes.length, resultLength);
          out.write(bytes);
          offset++;
        }
        else if (tableSchema.getFields().get(offset).getType() == DataType.Type.NCHAR) {
          byte[] bytes = (byte[])field;
          DataUtil.writeVLong(out, bytes.length, resultLength);
          out.write(bytes);
          offset++;
        }
        else if (tableSchema.getFields().get(offset).getType() == DataType.Type.FLOAT) {
          DataUtil.writeVLong(out, 8);
          out.writeDouble((Double)field);
          offset++;
        }
        else if (tableSchema.getFields().get(offset).getType() == DataType.Type.REAL) {
          DataUtil.writeVLong(out, 4);
          out.writeFloat((Float)field);
          offset++;
        }
        else if (tableSchema.getFields().get(offset).getType() == DataType.Type.DOUBLE) {
          DataUtil.writeVLong(out, 8);
          out.writeDouble((Double)field);
          offset++;
        }
        else if (tableSchema.getFields().get(offset).getType() == DataType.Type.VARCHAR ||
            tableSchema.getFields().get(offset).getType() == DataType.Type.NVARCHAR ||
            tableSchema.getFields().get(offset).getType() == DataType.Type.CLOB ||
            tableSchema.getFields().get(offset).getType() == DataType.Type.NCLOB ||
            tableSchema.getFields().get(offset).getType() == DataType.Type.LONGNVARCHAR ||
            tableSchema.getFields().get(offset).getType() == DataType.Type.LONGVARCHAR) {
          byte[] bytes = (byte[])field;
          DataUtil.writeVLong(out, bytes.length, resultLength);
          out.write(bytes);
          offset++;
        }
        else if (tableSchema.getFields().get(offset).getType() == DataType.Type.BOOLEAN) {
          DataUtil.writeVLong(out, 1);
          out.write((Boolean) field ? 1 : 0);
          offset++;
        }
        else if (tableSchema.getFields().get(offset).getType() == DataType.Type.BIT) {
          DataUtil.writeVLong(out, 1);
          out.write((Boolean) field ? 1 : 0);
          offset++;
        }
        else if (tableSchema.getFields().get(offset).getType() == DataType.Type.LONGVARBINARY ||
            tableSchema.getFields().get(offset).getType() == DataType.Type.VARBINARY ||
            tableSchema.getFields().get(offset).getType() == DataType.Type.BLOB) {
          byte[] bytes = (byte[])field;
          DataUtil.writeVLong(out, bytes.length, resultLength);
          out.write(bytes);
          offset++;
        }
        else if (tableSchema.getFields().get(offset).getType() == DataType.Type.NUMERIC) {
          BigDecimal value = ((BigDecimal)field);
          String strValue = value.toPlainString();
          byte[] bytes = strValue.getBytes("utf-8");
          DataUtil.writeVLong(out, bytes.length, resultLength);
          out.write(bytes);
          offset++;
        }
        else if (tableSchema.getFields().get(offset).getType() == DataType.Type.DECIMAL) {
          BigDecimal value = ((BigDecimal)field);
          String strValue = value.toPlainString();
          byte[] bytes = strValue.getBytes("utf-8");
          DataUtil.writeVLong(out, bytes.length, resultLength);
          out.write(bytes);
          offset++;
        }
        else if (tableSchema.getFields().get(offset).getType() == DataType.Type.DATE) {
          Date value = ((Date)field);
          String str = value.toString();
          byte[] bytes = str.getBytes("utf-8");
          DataUtil.writeVLong(out, bytes.length, resultLength);
          out.write(bytes);
          offset++;
        }
        else if (tableSchema.getFields().get(offset).getType() == DataType.Type.TIME) {
          Time value = ((Time)field);
          String str = value.toString();
          byte[] bytes = str.getBytes("utf-8");
          DataUtil.writeVLong(out, bytes.length, resultLength);
          out.write(bytes);
          offset++;
        }
        else if (tableSchema.getFields().get(offset).getType() == DataType.Type.TIMESTAMP) {
          Timestamp value = ((Timestamp)field);
          String str = value.toString();
          byte[] bytes = str.getBytes("utf-8");
          DataUtil.writeVLong(out, bytes.length, resultLength);
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
    DataUtil.writeVLong(outerOut, bytes.length);
    outerOut.write(bytes);
  }

  public static Object[] deserializeFields(
      String dbName, DatabaseCommon common, byte[] bytes, int byteOffset, TableSchema tableSchema, int schemaVersion, Set<Integer> columns, AtomicInteger serializedSchemaVersion, boolean deserializeHeader) throws IOException {
    DataUtil.ResultLength resultLength = new DataUtil.ResultLength();
    List<FieldSchema> currFieldList = tableSchema.getFields();
    List<FieldSchema> serializedFieldList = null;
    if (deserializeHeader) {
      long serializationVersion = DataUtil.readVLong(bytes, byteOffset, resultLength);
      byteOffset += resultLength.getLength();
      serializedSchemaVersion.set((int) DataUtil.readVLong(bytes, byteOffset, resultLength));
      byteOffset += resultLength.getLength();
      serializedFieldList = tableSchema.getFieldsForVersion(schemaVersion, serializedSchemaVersion.get());
      int fieldCount = (int) DataUtil.readVLong(bytes, byteOffset, resultLength);
    }
    else {
      serializedFieldList = tableSchema.getFieldsForVersion(schemaVersion, serializedSchemaVersion.get());
    }
    byteOffset += resultLength.getLength();
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
      int size = (int)DataUtil.readVLong(bytes, byteOffset, resultLength);
      byteOffset += resultLength.getLength();
      if (size > 0) {
        if (columns != null) {
          if (!columns.contains(currOffset)) {
            byteOffset += size;
            continue;
          }
        }
        if (field.getType() == DataType.Type.BIGINT) {
          fields[currOffset] = DataUtil.readVLong(bytes, byteOffset, resultLength);
          byteOffset += resultLength.getLength();
        }
        else if (field.getType() == DataType.Type.INTEGER) {
          fields[currOffset] = (int)DataUtil.readVLong(bytes, byteOffset, resultLength);
          byteOffset += resultLength.getLength();
        }
        else if (field.getType() == DataType.Type.SMALLINT) {
          fields[currOffset] = (short)DataUtil.readVLong(bytes, byteOffset, resultLength);
          byteOffset += resultLength.getLength();
        }
        else if (field.getType() == DataType.Type.TINYINT) {
          fields[currOffset] = bytes[byteOffset];
          byteOffset++;
        }
        else if (field.getType() == DataType.Type.NCHAR) {
          byte[] buffer = new byte[size];
          System.arraycopy(bytes, byteOffset, buffer, 0, size);
          byteOffset += size;
          fields[currOffset] = buffer;
        }
        else if (field.getType() == DataType.Type.CHAR) {
          byte[] buffer = new byte[size];
          System.arraycopy(bytes, byteOffset, buffer, 0, size);
          byteOffset += size;
          fields[currOffset] = buffer;
        }
        else if (field.getType() == DataType.Type.FLOAT) {
          fields[currOffset] = DataUtil.readDouble(bytes, byteOffset);
          byteOffset += 8;
        }
        else if (field.getType() == DataType.Type.REAL) {
          fields[currOffset] = DataUtil.readFloat(bytes, byteOffset);
          byteOffset += 4;
        }
        else if (field.getType() == DataType.Type.DOUBLE) {
          fields[currOffset] = DataUtil.readDouble(bytes, byteOffset);
          byteOffset += 8;
        }
        else if (field.getType() == DataType.Type.VARCHAR ||
            field.getType() == DataType.Type.NVARCHAR ||
            field.getType() == DataType.Type.CLOB ||
            field.getType() == DataType.Type.NCLOB ||
            field.getType() == DataType.Type.LONGNVARCHAR ||
            field.getType() == DataType.Type.LONGVARCHAR) {
          byte[] buffer = new byte[size];
          System.arraycopy(bytes, byteOffset, buffer, 0, size);
          byteOffset += size;
          fields[currOffset] = buffer;
        }
        else if (field.getType() == DataType.Type.BOOLEAN) {
          fields[currOffset] = bytes[byteOffset] == 1;
          byteOffset += 1;
        }
        else if (field.getType() == DataType.Type.BIT) {
          fields[currOffset] = bytes[byteOffset] == 1;
          byteOffset += 1;
        }
        else if (field.getType() == DataType.Type.VARBINARY ||
            field.getType() == DataType.Type.LONGVARBINARY ||
            field.getType() == DataType.Type.BLOB) {
          byte[] buffer = new byte[size];
          System.arraycopy(bytes, byteOffset, buffer, 0, size);
          byteOffset += size;
          fields[currOffset] = buffer;
        }
        else if (field.getType() == DataType.Type.NUMERIC) {
          byte[] buffer = new byte[size];
          System.arraycopy(bytes, byteOffset, buffer, 0, size);
          byteOffset += size;
          String str = new String(buffer, "utf-8");
          fields[currOffset] = new BigDecimal(str);
        }
        else if (field.getType() == DataType.Type.DECIMAL) {
          byte[] buffer = new byte[size];
          System.arraycopy(bytes, byteOffset, buffer, 0, size);
          byteOffset += size;
          String str = new String(buffer, "utf-8");
          fields[currOffset] = new BigDecimal(str);
        }
        else if (field.getType() == DataType.Type.DATE) {
          byte[] buffer = new byte[size];
          System.arraycopy(bytes, byteOffset, buffer, 0, size);
          byteOffset += size;
          String str = new String(buffer, "utf-8");
          fields[currOffset] = Date.valueOf(str);
        }
        else if (field.getType() == DataType.Type.TIME) {
          byte[] buffer = new byte[size];
          System.arraycopy(bytes, byteOffset, buffer, 0, size);
          byteOffset += size;
           String str = new String(buffer, "utf-8");
           fields[currOffset] = Time.valueOf(str);
         }
        else if (field.getType() == DataType.Type.TIMESTAMP) {
          byte[] buffer = new byte[size];
          System.arraycopy(bytes, byteOffset, buffer, 0, size);
          byteOffset += size;
           String str = new String(buffer, "utf-8");
           fields[currOffset] = Timestamp.valueOf(str);
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

  public void setServersConfig(DatabaseServer.ServersConfig serversConfig) {
    this.serversConfig = serversConfig;
    Integer replicaCount = null;
    DatabaseServer.Shard[] shards = serversConfig.getShards();
    for (DatabaseServer.Shard shard : shards) {
      DatabaseServer.Host[] replicas = shard.getReplicas();
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

  public DatabaseServer.ServersConfig getServersConfig() {
    return serversConfig;
  }


  public void addDatabase(String dbName) {
    schema.put(dbName, new Schema());
    createSchemaLocks(dbName);
  }

  public byte[] serializeConfig() throws IOException {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytesOut);
    DataUtil.writeVLong(out, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
    serversConfig.serialize(out);
    out.close();
    return bytesOut.toByteArray();
  }

  public void deserializeConfig(DataInputStream in) throws IOException {
    long serializationVersion = DataUtil.readVLong(in);
    serversConfig = new DatabaseServer.ServersConfig(in);
  }

  public void loadServersConfig(String dataDir) throws IOException {
    synchronized (this) {
      String dataRoot = new File(dataDir, "snapshot/" + shard + "/" + replica).getAbsolutePath();
      File configFile = new File(dataRoot, "config.bin");
      if (configFile.exists()) {
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(configFile)))) {
          long serializationVersion = DataUtil.readVLong(in);
          deserializeConfig(in);
        }
      }
      else {
        logger.info("No schema file found");
      }
    }

  }

  public void saveServersConfig(String dataDir) throws IOException {
    synchronized (this) {
      String dataRoot = new File(dataDir, "snapshot/" + shard + "/" + replica ).getAbsolutePath();
      File configFile = new File(dataRoot, "config.bin");
      if (configFile.exists()) {
        configFile.delete();
      }
      configFile.getParentFile().mkdirs();
      DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(configFile)));
      DataUtil.writeVLong(out, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
      out.write(serializeConfig());
      out.close();
    }
  }

  public int getSchemaVersion() {
    return schemaVersion;
  }

  public void dropTable(String dbName, String tableName, String dataDir) {
    schema.get(dbName).getTables().remove(tableName);
    saveSchema(dataDir);
  }
}
