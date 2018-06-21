package com.sonicbase.schema;

import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.common.ExcludeRename;
import org.apache.giraph.utils.Varint;

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

  public List<FieldSchema> getFieldsForVersion(int schemaVersion, int serializedVersion) {
    if (schemaVersion == serializedVersion || previousFields.size() == 0) {
      return fields;
    }
    for (int i = 0; i < previousFields.size(); i++) {
      PreviousFields prev = previousFields.get(i);
      if (serializedVersion == prev.schemaVersion) {
        return prev.fields;
      }
      if (serializedVersion < prev.schemaVersion) {
        PreviousFields prevPrev = i + 1 < previousFields.size() ? previousFields.get(i + 1) : null;
        if (prevPrev == null) {
          return prev.fields;
        }
        if (serializedVersion > prevPrev.schemaVersion) {
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

  public void setIndices(Map<String, IndexSchema> indices) {
    this.indexes = indices;
  }

  public void addIndex(IndexSchema indexSchema) {
    this.indexes.put(indexSchema.getName(), indexSchema);
    this.indexesById.put(indexSchema.getIndexId(), indexSchema);
  }

  class PreviousFields {
    int schemaVersion;
    List<FieldSchema> fields = new ArrayList<>();
  }

  List<PreviousFields> previousFields = new ArrayList<>();

  public void saveFields(int schemaVersion) {
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

  public IndexSchema addIndex(String indexName, boolean isPrimaryKey, boolean isUnique, String[] fields, Partition[] partitions, int indexId) {
    Comparator[] comparators = getComparators(fields);

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

    IndexSchema indexSchema = new IndexSchema(indexName, indexId, isUnique, fields, comparators, partitions, isPrimaryKey, isPrimaryKeyGroup, this);
    indexes.put(indexName, indexSchema);
    indexesById.put(indexId, indexSchema);

    return indexSchema;
  }

  public Map<String, IndexSchema> getIndices() {
    return indexes;
  }

  public void serialize(DataOutputStream out) throws IOException {
    Varint.writeSignedVarLong(tableId, out);
    out.writeInt(fields.size());
    out.writeUTF(name);
    for (FieldSchema field : fields) {
      field.serialize(out);
    }
    out.writeInt(previousFields.size());
    for (PreviousFields prev : previousFields) {
      out.writeInt(prev.schemaVersion);
      out.writeInt(prev.fields.size());
      for (FieldSchema fieldSchema : prev.fields) {
        fieldSchema.serialize(out);
      }
    }
    out.writeInt(indexes.size());
    for (Map.Entry<String, IndexSchema> entry : indexes.entrySet()) {
      serializeIndexSchema(out, this, entry.getValue());
    }
    out.writeInt(primaryKey.length);
    for (int i = 0; i < primaryKey.length; i++) {
      out.writeUTF(primaryKey[i]);
    }
  }

  public static void serializeIndexSchema(DataOutputStream out, TableSchema tableSchema, IndexSchema indexSchema) throws IOException {
    out.writeUTF(indexSchema.getName());
    out.writeInt(indexSchema.getIndexId());
    if (indexSchema.getIndexId() > 10) {
      System.out.println(">10");
    }

    out.writeBoolean(indexSchema.isUnique());
    out.writeInt(indexSchema.getFields().length);
    for (int i = 0; i < indexSchema.getFields().length; i++) {
      out.writeUTF(indexSchema.getFields()[i]);
    }
    out.writeBoolean(indexSchema.isPrimaryKey());
    out.writeBoolean(indexSchema.isPrimaryKeyGroup());
    out.writeInt(indexSchema.getCurrPartitions().length);
    for (int i = 0; i < indexSchema.getCurrPartitions().length; i++) {
      out.writeInt(indexSchema.getCurrPartitions()[i].shardOwning);
      out.writeBoolean(indexSchema.getCurrPartitions()[i].unboundUpper);
      if (indexSchema.getCurrPartitions()[i].upperKey == null) {
        out.writeInt(0);
      }
      else {
        out.writeInt(1);
        out.write(DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), indexSchema.getCurrPartitions()[i].upperKey));
      }
    }
    if (indexSchema.getLastPartitions() == null) {
      out.writeInt(0);
    }
    else {
      out.writeInt(indexSchema.getLastPartitions().length);
      for (int i = 0; i < indexSchema.getLastPartitions().length; i++) {
        out.writeInt(indexSchema.getLastPartitions()[i].shardOwning);
        out.writeBoolean(indexSchema.getLastPartitions()[i].unboundUpper);
        if (indexSchema.getLastPartitions()[i].upperKey == null) {
          out.writeInt(0);
        }
        else {
          out.writeInt(1);
          out.write(DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), indexSchema.getLastPartitions()[i].upperKey));
        }
      }
    }
  }

  public Map<String, IndexSchema> getIndexes() {
    return indexes;
  }

  public Map<Integer, IndexSchema> getIndexesById() {
    return indexesById;
  }

  public void deserialize(DataInputStream in, short serializationVersion) throws IOException {
    tableId = (int) Varint.readSignedVarLong(in);
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
      prev.schemaVersion = in.readInt();
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
      deserializeIndexSchema(in, this);
    }
    int columnCount = in.readInt();
    primaryKey = new String[columnCount];
    for (int i = 0; i < columnCount; i++) {
      primaryKey[i] = in.readUTF();
    }
  }

  public static IndexSchema deserializeIndexSchema(DataInputStream in, TableSchema tableSchema) throws IOException {
    String name = in.readUTF();
    int indexId = in.readInt();
    if (indexId > 10) {
      System.out.println(">10");
    }
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
    indexSchema.setFields(columns, tableSchema);
    Comparator[] comparators = tableSchema.getComparators(columns);
    indexSchema.setComparators(comparators);
    indexSchema.setIndexId(indexId);
    indexSchema.setIsPrimaryKey(isPrimaryKey);
    indexSchema.setIsPrimaryKeyGroup(isPrimaryKeyGroup);

    tableSchema.getIndexes().put(indexSchema.getName(), indexSchema);
    tableSchema.getIndexesById().put(indexSchema.getIndexId(), indexSchema);

    Partition[] partitions = new Partition[in.readInt()];
    for (int j = 0; j < partitions.length; j++) {
      partitions[j] = new Partition();
      partitions[j].shardOwning = in.readInt();
      partitions[j].setUnboundUpper(in.readBoolean());
      if (in.readInt() == 1) {
        partitions[j].upperKey = DatabaseCommon.deserializeKey(tableSchema, in);
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
          partitions[j].upperKey = DatabaseCommon.deserializeKey(tableSchema, in);
        }
      }
    }
    indexSchema.setLastPartitions(partitions);
    return indexSchema;
  }
}
