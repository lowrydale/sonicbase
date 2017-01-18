package com.lowryengineering.database.query.impl;

import com.lowryengineering.database.client.DatabaseClient;
import com.lowryengineering.database.query.CreateTableStatement;
import com.lowryengineering.database.query.DatabaseException;
import com.lowryengineering.database.schema.DataType;
import com.lowryengineering.database.schema.FieldSchema;
import com.lowryengineering.database.server.SnapshotManager;
import com.lowryengineering.database.util.DataUtil;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CreateTableStatementImpl implements CreateTableStatement {
  private DatabaseClient client;
  private String tableName;
  private List<FieldSchema> fields = new ArrayList<>();
  private List<String> primaryKey = new ArrayList<String>();

  public CreateTableStatementImpl(DatabaseClient client) {
    this.client = client;
  }

  public CreateTableStatementImpl() {

  }

  public String getTablename() {
    return tableName;
  }

  public List<FieldSchema> getFields() {
    return fields;
  }

  public void setFields(List<FieldSchema> fields) {
    this.fields = fields;
  }

  public List<String> getPrimaryKey() {
    return primaryKey;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName.toLowerCase();
  }

  public void addField(String name, int type) {
    FieldSchema schema = new FieldSchema();
    schema.setName(name.toLowerCase());
    schema.setType(DataType.Type.valueOf(type));
    fields.add(schema);
  }

  public int execute(String dbName) throws DatabaseException {
    try {
      return client.doCreateTable(dbName, this);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public void setPrimaryKey(List<String> primaryKey) {
    this.primaryKey = primaryKey;
    for (int i = 0; i < primaryKey.size(); i++) {
      primaryKey.set(i, primaryKey.get(i).toLowerCase());
    }
  }

  public void serialize(DataOutputStream out) {
    try {
      DataUtil.writeVLong(out, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
      out.writeUTF(tableName);
      out.writeInt(fields.size());
      for (int i = 0; i < fields.size(); i++) {
        fields.get(i).serialize(out);
      }
      out.writeInt(primaryKey.size());
      for (int i = 0; i < primaryKey.size(); i++) {
        out.writeUTF(primaryKey.get(i));
      }
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public void deserialize(DataInputStream in) {
    try {
      long serializationVersion = DataUtil.readVLong(in);
      tableName = in.readUTF();
      int count = in.readInt();
      for (int i = 0; i < count; i++) {
        FieldSchema fieldSchema = new FieldSchema();
        fieldSchema.deserialize(in, (int)serializationVersion);
        fields.add(fieldSchema);
      }
      count = in.readInt();
      for (int i = 0; i < count; i++) {
        primaryKey.add(in.readUTF());
      }
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }
}
