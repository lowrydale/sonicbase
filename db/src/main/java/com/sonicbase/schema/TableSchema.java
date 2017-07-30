package com.sonicbase.schema;

import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.common.ExcludeRename;
import com.sonicbase.util.DataUtil;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;

@ExcludeRename
public class TableSchema {
  private int version;
  private List<FieldSchema> fields = new ArrayList<FieldSchema>();
  private Map<String, Integer> fieldOffsets = new HashMap<String, Integer>();
  private String name;
  private Map<String, IndexSchema> indexes = new HashMap<>();
  private String[] primaryKey;
  private int tableId;
  private Map<Integer, IndexSchema> indexesById = new HashMap<>();

  public void addField(FieldSchema schema) {
    fields.add(schema);
    fieldOffsets.put(schema.getName(), fieldOffsets.size());
  }

  int getVersion() {
    return version;
  }

  void setVersion(int version) {
    this.version = version;
  }

  public List<FieldSchema> getFields() {
    return fields;
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="EI_EXPOSE_REP", justification="copying the returned data is too slow")
  public String[] getPrimaryKey() {
    return primaryKey;
  }

  public void setPrimaryKey(List<String> primaryKey) {
    this.primaryKey = new String[primaryKey.size()];
    for (int i = 0; i < primaryKey.size(); i++) {
      this.primaryKey[i] = primaryKey.get(i);
    }
  }

  public void setFields(List<FieldSchema> fields) {
    this.fields = fields;
    for (int i = 0; i < fields.size(); i++) {
      fieldOffsets.put(fields.get(i).getName(), i);
    }
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setTableId(int tableId) {
    this.tableId = tableId;
  }

  public int getTableId() {
    return tableId;
  }

  public TableSchema deepCopy() {
    return null;
  }

  public Comparator[] getComparators(String[] fields) {
    Comparator[] comparators = new Comparator[fields.length];
    for (int i = 0; i < fields.length; i++) {
      int offset = getFieldOffset(fields[i]);
      FieldSchema fieldSchema = getFields().get(offset);
      DataType.Type type = fieldSchema.getType();
      comparators[i] = type.getComparator();
    }
    return comparators;
  }

  public List<FieldSchema> getFieldsForVersion(long schemaVersion, long serializationVersion) {
    if (schemaVersion == serializationVersion || previousFields.size() == 0) {
      return fields;
    }
    for (int i = 0; i < previousFields.size(); i++) {
      PreviousFields prev = previousFields.get(i);
      if (serializationVersion == prev.schemaVersion) {
        return prev.fields;
      }
      if (serializationVersion < prev.schemaVersion) {
        PreviousFields prevPrev = i + 1 < previousFields.size() ? previousFields.get(i + 1) : null;
        if (prevPrev == null) {
          return prev.fields;
        }
        if (serializationVersion > prevPrev.schemaVersion) {
          return prev.fields;
        }
      }
    }
    return fields;
  }

  public void markChangesComplete() {
    for (PreviousFields prev : previousFields) {
      for (FieldSchema prevField : prev.fields) {
        for (int i = 0; i < fields.size(); i++) {
          FieldSchema field = fields.get(i);
          if (field.getName().equals(prevField.getName())) {
            prevField.setMapToOffset(i);
            break;
          }
        }
      }
    }
  }

  class PreviousFields {
    long schemaVersion;
    List<FieldSchema> fields = new ArrayList<>();
  }

  List<PreviousFields> previousFields = new ArrayList<>();

  public void saveFields(long schemaVersion) {
    PreviousFields prev = new PreviousFields();
    prev.schemaVersion = schemaVersion;
    for (FieldSchema fieldSchema : fields) {
      prev.fields.add(fieldSchema);
    }
    previousFields.add(prev);
  }

  public static class Partition {

    private Object[] upperKey;
    private int shardOwning;
    private boolean unboundUpper;

    public Partition() {
    }

    public Partition(int shardOwning) {
      this.shardOwning = shardOwning;
    }

    public boolean isUnboundUpper() {
      return unboundUpper;
    }

    public void setUnboundUpper(boolean unboundUpper) {
      this.unboundUpper = unboundUpper;
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="EI_EXPOSE_REP", justification="copying the returned data is too slow")
    public Object[] getUpperKey() {
      return upperKey;
    }

    public int getShardOwning() {
      return shardOwning;
    }

    public void setShardOwning(int shardOwning) {
      this.shardOwning = shardOwning;
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="EI_EXPOSE_REP2", justification="copying the passed in data is too slow")
    @SuppressWarnings("PMD.ArrayIsStoredDirectly") //copying the passed in data is too slow
    public void setUpperKey(Object[] upperKey) {
      this.upperKey = upperKey;
    }

    public void serialize(TableSchema tableSchema, String indexName, DataOutputStream out) throws IOException {
      out.writeInt(shardOwning);
      out.writeBoolean(unboundUpper);
      if (upperKey == null) {
        out.writeInt(0);
      }
      else {
        out.writeInt(1);
        out.write(DatabaseCommon.serializeKey(tableSchema, indexName, upperKey));
      }
    }

    public void deserialize(TableSchema tableSchema, DataInputStream in) throws IOException {
      shardOwning = in.readInt();
      unboundUpper = in.readBoolean();
      if (in.readInt() == 1) {
        upperKey = DatabaseCommon.deserializeKey(tableSchema, in);
      }
    }
  }

  public Integer getFieldOffset(String id) {
    return fieldOffsets.get(id);
  }

  public void addIndex(String indexName, boolean isUnique, String[] fields, Partition[] partitions, int indexId) {
    Comparator[] comparators = getComparators(fields);

    boolean isPrimaryKey = false;
    String[] primaryKey = getPrimaryKey();
    if (fields.length == primaryKey.length) {
      boolean foundAllFields = true;
      for (String primaryKeyField : primaryKey) {
        boolean found = false;
        for (String indexField : fields) {
          if (primaryKeyField.equals(indexField)) {
            found = true;
            break;
          }
        }
        if (!found) {
          foundAllFields = false;
          break;
        }
      }
      if (foundAllFields) {
        isPrimaryKey = true;
      }
    }

    boolean isPrimaryKeyGroup = false;
    if (primaryKey.length >= fields.length) {
      boolean found = true;
      for (int i = 0; i < fields.length; i++) {
        String field = fields[i];
        if (!field.equals(primaryKey[i])) {
          found = false;
          break;
        }
      }
      if (found) {
        isPrimaryKeyGroup = true;
      }
    }

    IndexSchema indexSchema = new IndexSchema(indexName, indexId, isUnique, fields, comparators, partitions, isPrimaryKey, isPrimaryKeyGroup);
    indexes.put(indexName, indexSchema);
    indexesById.put(indexId, indexSchema);
  }

  public Map<String, IndexSchema> getIndices() {
    return indexes;
  }

  public void serialize(DataOutputStream out) throws IOException {
    DataUtil.writeVLong(out, tableId, new DataUtil.ResultLength());
    out.writeInt(fields.size());
    out.writeUTF(name);
    for (FieldSchema field : fields) {
      field.serialize(out);
    }
    out.writeInt(previousFields.size());
    for (PreviousFields prev : previousFields) {
      out.writeLong(prev.schemaVersion);
      out.writeInt(prev.fields.size());
      for (FieldSchema fieldSchema : prev.fields) {
        fieldSchema.serialize(out);
      }
    }
    out.writeInt(indexes.size());
    for (Map.Entry<String, IndexSchema> entry : indexes.entrySet()) {
      out.writeUTF(entry.getKey());
      out.writeInt(entry.getValue().getIndexId());
      out.writeBoolean(entry.getValue().isUnique());
      out.writeInt(entry.getValue().getFields().length);
      for (int i = 0; i < entry.getValue().getFields().length; i++) {
        out.writeUTF(entry.getValue().getFields()[i]);
      }
      out.writeBoolean(entry.getValue().isPrimaryKey());
      out.writeBoolean(entry.getValue().isPrimaryKeyGroup());
      out.writeInt(entry.getValue().getCurrPartitions().length);
      for (int i = 0; i < entry.getValue().getCurrPartitions().length; i++) {
        out.writeInt(entry.getValue().getCurrPartitions()[i].shardOwning);
        out.writeBoolean(entry.getValue().getCurrPartitions()[i].unboundUpper);
        if (entry.getValue().getCurrPartitions()[i].upperKey == null) {
          out.writeInt(0);
        }
        else {
          out.writeInt(1);
          out.write(DatabaseCommon.serializeKey(this, entry.getKey(), entry.getValue().getCurrPartitions()[i].upperKey));
        }
      }
      if (entry.getValue().getLastPartitions() == null) {
        out.writeInt(0);
      }
      else {
        out.writeInt(entry.getValue().getLastPartitions().length);
        for (int i = 0; i < entry.getValue().getLastPartitions().length; i++) {
          out.writeInt(entry.getValue().getLastPartitions()[i].shardOwning);
          out.writeBoolean(entry.getValue().getLastPartitions()[i].unboundUpper);
          if (entry.getValue().getLastPartitions()[i].upperKey == null) {
            out.writeInt(0);
          }
          else {
            out.writeInt(1);
            out.write(DatabaseCommon.serializeKey(this, entry.getKey(), entry.getValue().getLastPartitions()[i].upperKey));
          }
        }
      }
    }
    out.writeInt(primaryKey.length);
    for (int i = 0; i < primaryKey.length; i++) {
      out.writeUTF(primaryKey[i]);
    }
  }

  public Map<String, IndexSchema> getIndexes() {
    return indexes;
  }

  public Map<Integer, IndexSchema> getIndexesById() {
    return indexesById;
  }

  public void deserialize(DataInputStream in, int serializationVersion) throws IOException {
    tableId = (int) DataUtil.readVLong(in, new DataUtil.ResultLength());
    int fieldCount = in.readInt();
    name = in.readUTF();
    for (int i = 0; i < fieldCount; i++) {
      FieldSchema field = new FieldSchema();
      field.deserialize(in, serializationVersion);
      fields.add(field);
      fieldOffsets.put(field.getName(), i);
    }
    int prevCount = in.readInt();
    for (int i = 0; i < prevCount; i++) {
      PreviousFields prev = new PreviousFields();
      prev.schemaVersion = in.readLong();
      prev.fields = new ArrayList<>();
      int count = in.readInt();
      for (int j = 0; j < count; j++) {
        FieldSchema field = new FieldSchema();
        field.deserialize(in, serializationVersion);
        prev.fields.add(field);
      }
      previousFields.add(prev);
    }
    int indexCount = in.readInt();
    for (int i = 0; i < indexCount; i++) {
      String name = in.readUTF();
      int indexId = in.readInt();
      boolean isUnique = in.readBoolean();
      int columnCount = in.readInt();
      String[] columns = new String[columnCount];
      for (int j = 0; j < columnCount; j++) {
        columns[j] = in.readUTF();
      }
      boolean isPrimaryKey = in.readBoolean();
      boolean isPrimaryKeyGroup = in.readBoolean();
      IndexSchema indexSchema = new IndexSchema();
      indexSchema.setName(name);
      indexSchema.setIsUnique(isUnique);
      indexSchema.setFields(columns);
      Comparator[] comparators = getComparators(columns);
      indexSchema.setComparators(comparators);
      indexSchema.setIndexId(indexId);
      indexSchema.setIsPrimaryKey(isPrimaryKey);
      indexSchema.setIsPrimaryKeyGroup(isPrimaryKeyGroup);
      indexes.put(name, indexSchema);
      indexesById.put(indexId, indexSchema);

      Partition[] partitions = new Partition[in.readInt()];
      for (int j = 0; j < partitions.length; j++) {
        partitions[j] = new Partition();
        partitions[j].shardOwning = in.readInt();
        partitions[j].setUnboundUpper(in.readBoolean());
        if (in.readInt() == 1) {
          partitions[j].upperKey = DatabaseCommon.deserializeKey(this, in);
        }
      }
      indexSchema.setCurrPartitions(partitions);

      partitions = new Partition[in.readInt()];
      if (partitions.length == 0) {
        partitions = null;
      }
      else {
        for (int j = 0; j < partitions.length; j++) {
          partitions[j] = new Partition();
          partitions[j].shardOwning = in.readInt();
          partitions[j].setUnboundUpper(in.readBoolean());
          if (in.readInt() == 1) {
            partitions[j].upperKey = DatabaseCommon.deserializeKey(this, in);
          }
        }
      }
      indexSchema.setLastPartitions(partitions);
    }
    int columnCount = in.readInt();
    primaryKey = new String[columnCount];
    for (int i = 0; i < columnCount; i++) {
      primaryKey[i] = in.readUTF();
    }
  }
}
