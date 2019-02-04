package com.sonicbase.schema;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.query.DatabaseException;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class Schema {

  private Map<String, TableSchema> tables = new ConcurrentHashMap<>();
  private Map<Integer, TableSchema> tablesById = new ConcurrentHashMap<>();
  private final Object schemaMutex = new Object();
  private RecordIndexPartition[] lastRecordIndexPartitions;
  private RecordIndexPartition[] currRecordIndexPartitions;

  public void addTable(TableSchema schema) {
    synchronized (schemaMutex) {
      tables.put(schema.getName(), schema);
      tablesById.put(schema.getTableId(), schema);
    }
  }

  public void serialize(DataOutputStream out) {
    try {
      synchronized (schemaMutex) {
        out.writeShort(DatabaseClient.SERIALIZATION_VERSION);
        out.writeInt(tables.size());
        for (TableSchema table : tables.values()) {
          if (table == null) {
            System.out.println("broken");
            continue;
          }
            table.serialize(out);
        }
        if (currRecordIndexPartitions == null) {
          out.writeInt(0);
        }
        else {
          out.writeInt(currRecordIndexPartitions.length);
          for (RecordIndexPartition partition : currRecordIndexPartitions) {
            out.writeInt(partition.getShardOwning());
          }
        }
        if (lastRecordIndexPartitions == null) {
          out.writeInt(0);
        }
        else {
          out.writeInt(lastRecordIndexPartitions.length);
          for (RecordIndexPartition partition : lastRecordIndexPartitions) {
            out.writeInt(partition.getShardOwning());
          }
        }
      }
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public void deserialize(DataInputStream in) {
    try {
      short serializationVersion = in.readShort();
      synchronized (schemaMutex) {
        int tableCount = in.readInt();
        Map<String, TableSchema> newTables = new HashMap<>();
        Map<Integer, TableSchema> newTablesById = new HashMap<>();
        for (int i = 0; i < tableCount; i++) {
          TableSchema table = new TableSchema();
          table.deserialize(in, serializationVersion);
          newTables.put(table.getName(), table);
          newTablesById.put(table.getTableId(), table);
        }
        this.tables = newTables;
        this.tablesById = newTablesById;

        int partitionCount = in.readInt();
        RecordIndexPartition[] newRecordIndexPartitions = new RecordIndexPartition[partitionCount];
        for (int i = 0; i < newRecordIndexPartitions.length; i++) {
          newRecordIndexPartitions[i] = new RecordIndexPartition();
          newRecordIndexPartitions[i].setShardOwning(in.readInt());
        }

        partitionCount = in.readInt();
        if (partitionCount != 0) {
          RecordIndexPartition[] localLastRecordIndexPartitions = new RecordIndexPartition[partitionCount];
          for (int i = 0; i < localLastRecordIndexPartitions.length; i++) {
            localLastRecordIndexPartitions[i] = new RecordIndexPartition();
            localLastRecordIndexPartitions[i].setShardOwning(in.readInt());
          }
          this.lastRecordIndexPartitions = localLastRecordIndexPartitions;
        }
        this.currRecordIndexPartitions = newRecordIndexPartitions;
      }
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }


  public Map<Integer, TableSchema> getTablesById() {
    return tablesById;
  }

  public void updateTable(TableSchema tableSchema) {
    synchronized (schemaMutex) {
      tables.put(tableSchema.getName(), tableSchema);
      tablesById.put(tableSchema.getTableId(), tableSchema);
    }
  }

  public Map<String, TableSchema> getTables() {
    return tables;
  }

  public Object getSchemaLock() {
    return schemaMutex;
  }

  public void setTables(Map<String, TableSchema> tables) {
    this.tables = tables;
  }

  public void setTablesById(Map<Integer, TableSchema> tablesById) {
    this.tablesById = tablesById;
  }
}
