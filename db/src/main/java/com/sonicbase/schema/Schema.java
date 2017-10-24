package com.sonicbase.schema;

import com.sonicbase.query.DatabaseException;
import com.sonicbase.server.DatabaseServer;
import com.sonicbase.server.SnapshotManager;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Responsible for
 */
public class Schema {

  private Map<String, TableSchema> tables = new HashMap<>();
  private Map<Integer, TableSchema> tablesById = new HashMap<>();
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
        out.writeShort(DatabaseServer.SERIALIZATION_VERSION);
        out.writeInt(tables.size());
        for (TableSchema table : tables.values()) {
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
        //tables.clear();
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
        //System.out.println("tables=" + newTables.size() + ", tablesById=" + newTablesById.size() + ", thisTablesById=" + this.tablesById.size());

        int partitionCount = in.readInt();
        RecordIndexPartition[] newRecordIndexPartitions = new RecordIndexPartition[partitionCount];
        for (int i = 0; i < newRecordIndexPartitions.length; i++) {
          newRecordIndexPartitions[i] = new RecordIndexPartition();
          newRecordIndexPartitions[i].setShardOwning(in.readInt());
        }

        partitionCount = in.readInt();
        if (partitionCount != 0) {
          RecordIndexPartition[] lastRecordIndexPartitions = new RecordIndexPartition[partitionCount];
          for (int i = 0; i < lastRecordIndexPartitions.length; i++) {
            lastRecordIndexPartitions[i] = new RecordIndexPartition();
            lastRecordIndexPartitions[i].setShardOwning(in.readInt());
          }
          this.lastRecordIndexPartitions = lastRecordIndexPartitions;
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

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="EI_EXPOSE_REP", justification="copying the returned data is too slow")
  public RecordIndexPartition[] getRecordIndexPartitions() {
    return currRecordIndexPartitions;
  }

  public void initRecordsById(int shardCount, int partitionCountForRecordIndex) {
    if (currRecordIndexPartitions == null) {
      RecordIndexPartition[] partitions = new RecordIndexPartition[partitionCountForRecordIndex];
      for (int i = 0; i < partitionCountForRecordIndex; i++) {
        RecordIndexPartition partition = new RecordIndexPartition();
        partition.setShardOwning(i % shardCount);
        partitions[i] = partition;
      }
      this.currRecordIndexPartitions = partitions;
    }
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="EI_EXPOSE_REP2", justification="copying the passed in data is too slow")
  @SuppressWarnings("PMD.ArrayIsStoredDirectly") //copying the passed in data is too slow
  public void reshardRecordIndex(RecordIndexPartition[] currPartitions) {
    synchronized (schemaMutex) {
      this.lastRecordIndexPartitions = this.currRecordIndexPartitions;
      this.currRecordIndexPartitions = currPartitions;
    }
  }

  public void deleteLastRecordIndex() {
    synchronized (schemaMutex) {
      lastRecordIndexPartitions = null;
    }
  }

  public Object getSchemaLock() {
    return schemaMutex;
  }

  public void setTables(Map<String, TableSchema> tables) {
    this.tables = tables;
  }
}
