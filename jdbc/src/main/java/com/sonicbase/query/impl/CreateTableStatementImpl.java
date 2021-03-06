package com.sonicbase.query.impl;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.query.CreateTableStatement;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.DataType;
import com.sonicbase.schema.FieldSchema;
import com.sonicbase.util.Varint;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class CreateTableStatementImpl implements CreateTableStatement {
  private String tableName;
  private List<FieldSchema> fields = new ArrayList<>();
  private List<String> primaryKey = new ArrayList<>();

  public CreateTableStatementImpl(DatabaseClient client) {
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

  public void setPrimaryKey(List<String> primaryKey) {
    this.primaryKey = primaryKey;
    for (int i = 0; i < primaryKey.size(); i++) {
      primaryKey.set(i, primaryKey.get(i).toLowerCase());
    }
  }

  public byte[] serialize() throws IOException {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytesOut);
    serialize(out);
    out.close();
    return bytesOut.toByteArray();
  }

  private void serialize(DataOutputStream out) {
    try {
      Varint.writeSignedVarLong(DatabaseClient.SERIALIZATION_VERSION, out);
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

  public void deserialize(byte[] bytes) {
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
    deserialize(in);
  }

  private void deserialize(DataInputStream in) {
    try {
      long serializationVersion = Varint.readSignedVarLong(in);
      tableName = in.readUTF();
      int count = in.readInt();
      for (int i = 0; i < count; i++) {
        FieldSchema fieldSchema = new FieldSchema();
        fieldSchema.deserialize(in, (short)serializationVersion);
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
