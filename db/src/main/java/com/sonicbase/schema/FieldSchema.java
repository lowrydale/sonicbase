package com.sonicbase.schema;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class FieldSchema {
  private String name;
  private DataType.Type type;
  private int width;
  private boolean autoIncrement;
  private boolean array;
  private int mapToOffset;

  public int getMapToOffset() {
    return mapToOffset;
  }

  public void setMapToOffset(int mapToOffset) {
    this.mapToOffset = mapToOffset;
  }

  public boolean isAutoIncrement() {
    return autoIncrement;
  }

  public void setAutoIncrement(boolean autoIncrement) {
    this.autoIncrement = autoIncrement;
  }

  public boolean isArray() {
    return array;
  }

  public void setArray(boolean array) {
    this.array = array;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public DataType.Type getType() {
    return type;
  }

  public void setType(DataType.Type type) {
    this.type = type;
  }

  public void setWidth(int width) {
    this.width = width;
  }

  public int getWidth() {
    return width;
  }

  public void serialize(DataOutputStream out) throws IOException {
    out.writeUTF(name);
    out.writeInt(type.getValue());
    out.writeInt(width);
    out.writeBoolean(autoIncrement);
    out.writeBoolean(array);
    out.writeInt(mapToOffset);
  }

  public void deserialize(DataInputStream in, int serializationVersion) throws IOException {
    name = in.readUTF();
    type = DataType.Type.valueOf(in.readInt());
    width = in.readInt();
    autoIncrement = in.readBoolean();
    array = in.readBoolean();
    mapToOffset = in.readInt();
  }
}
