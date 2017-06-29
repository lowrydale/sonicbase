package com.sonicbase.common;

import com.sonicbase.query.DatabaseException;
import com.sonicbase.server.SnapshotManager;
import com.sonicbase.util.DataUtil;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Iterator;

import static com.sonicbase.common.ComObject.Type.*;

/**
 * Created by lowryda on 6/24/17.
 */
public class ComObject {

  static Int2ObjectOpenHashMap<DynamicType> typesByTag = new Int2ObjectOpenHashMap<>();


  public static class DynamicType {
    int tag;

    public DynamicType(int tag) {
      this.tag = tag;
    }
  }

  public enum Type {
    longType(0),
    intType(1),
    stringType(2),
    booleanType(3),
    byteArrayType(4),
    arrayType(5),
    objectType(6);

    final int tag;

    Type(int tag) {
      this.tag = tag;
      typesByTag.put(tag, new DynamicType(tag));
    }
  };

  static Int2ObjectOpenHashMap<DynamicTag> tagsByTag = new Int2ObjectOpenHashMap<>();

  public static class DynamicTag {
    private final int tag;
    private final DynamicType type;

    public DynamicTag(int tag, DynamicType type) {
      this.tag = tag;
      this.type = type;
    }
  }

  public enum Tag {
    serializationVersion(1, longType),
    tableName(2, stringType),
    indexName(3, stringType),
    id(4, longType),
    isExcpliciteTrans(5, booleanType),
    transactionId(6, longType),
    recordLength(7, intType),
    recordBytes(8, byteArrayType),
    keyLength(9, intType),
    keyBytes(10, byteArrayType),
    isCommitting(11, booleanType),
    primaryKeyBytes(12, byteArrayType),
    bytes(13, byteArrayType),
    expression(14, byteArrayType),
    parms(15, byteArrayType),
    countColumn(16, stringType),
    countTableName(17, stringType),
    leftOperator(18, intType),
    columnOffsets(19, arrayType),
    keyCount(20, intType),
    singleValue(21, booleanType),
    keys(22, arrayType),
    offset(23, intType),
    longKey(24, longType),
    records(25, arrayType),
    retKeys(26, arrayType),
    schemaVersion(27, intType),
    preparedId(28, longType),
    isPrepared(29, booleanType),
    count(30, intType),
    viewVersion(31, longType),
    dbName(32, stringType),
    method(33, stringType),
    tableId(34, intType),
    indexId(35, intType),
    forceSelectOnServer(36, booleanType),
    evaluateExpression(37, booleanType),
    orderByExpressions(38, arrayType),
    leftKey(39, byteArrayType),
    originalLeftKey(40, byteArrayType),
    rightKey(41, byteArrayType),
    originalRightKey(42, byteArrayType),
    rightOperator(43, intType),
    counters(44, arrayType),
    groupContext(45, byteArrayType),
    selectStatement(46, byteArrayType),
    tableRecords(47, arrayType),
    counter(48, byteArrayType),
    slave(49, booleanType),
    masterSlave(50, stringType),
    finished(51, booleanType),
    shard(52, intType),
    offsets(53, arrayType),
    size(54, longType),
    tables(55, arrayType),
    indices(56, arrayType),
    force(57, booleanType),
    primaryKeyIndexName(58, stringType),
    insertObject(59, objectType),
    insertObjects(60, arrayType),
    phase(61, stringType),
    schemaBytes(62, byteArrayType),
    createTableStatement(63, byteArrayType),
    columnName(64, stringType),
    dataType(65, stringType),
    isUnique(66, booleanType),
    fieldsStr(67, stringType),
    resultSetId(68, longType),
    countLong(69, longType);

    public final int tag;

    Tag(int tag, Type type) {
      this.tag = tag;
      tagsByTag.put(tag, new DynamicTag(tag, typesByTag.get(type.tag)));
    }
  }

  public ComObject() {
    put(ComObject.Tag.serializationVersion, SnapshotManager.SNAPSHOT_SERIALIZATION_VERSION);
  }

  public ComObject(byte[] bytes) {
    deserialize(bytes);
  }

  private Int2ObjectOpenHashMap map = new Int2ObjectOpenHashMap();

  public void put(Tag tag, long value) {
    map.put(tag.tag, (Object)value);
  }

  public void put(Tag tag, int value) {
    map.put(tag.tag, (Object)value);
  }

  public void put(Tag tag, String value) {
    map.put(tag.tag, (Object)value);
  }

  public void put(Tag tag, boolean value) {
    map.put(tag.tag, (Object)value);
  }

  public void put(Tag tag, byte[] bytes) {
    map.put(tag.tag, (Object)bytes);
  }

  public Long getLong(Tag tag) {
    return (Long)map.get(tag.tag);
  }

  public Integer getInt(Tag tag) {
    return (Integer)map.get(tag.tag);
  }

  public String getString(Tag tag) {
    return (String)map.get(tag.tag);
  }

  public Boolean getBoolean(Tag tag) {
    return (Boolean)map.get(tag.tag);
  }

  public byte[] getByteArray(Tag tag) {
    return (byte[])map.get(tag.tag);
  }

  public ComArray putArray(Tag tag, Type nestedType) {
    ComArray ret = new ComArray(nestedType);
    map.put(tag.tag, ret);
    return ret;
  }

  public ComArray getArray(Tag tag) {
    return (ComArray)map.get(tag.tag);
  }

  public void remove(Tag tag) {
    map.remove(tag.tag);
  }

  public void deserialize(byte[] bytes) {
    try {
      map.clear();
      DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
      int count = (int) DataUtil.readVLong(in);
      for (int i = 0; i < count; i++) {
        int tag = (int)DataUtil.readVLong(in);
        int typeTag = (int)DataUtil.readVLong(in);
        DynamicType type = typesByTag.get(typeTag);

        Object value = null;
        if (type.tag == intType.tag) {
          value = (int)DataUtil.readVLong(in);
        }
        else if (type.tag == longType.tag) {
          value = DataUtil.readVLong(in);
        }
        else if (type.tag == stringType.tag) {
          int len = (int)DataUtil.readVLong(in);
          bytes = new byte[len];
          in.readFully(bytes);
          value = new String(bytes, "utf-8");
        }
        else if (type.tag == booleanType.tag) {
          value = in.readBoolean();
        }
        else if (type.tag == byteArrayType.tag) {
          int len = (int)DataUtil.readVLong(in);
          bytes = new byte[len];
          in.readFully(bytes);
          value = bytes;
        }
        else if (type.tag == arrayType.tag) {
          value = new ComArray(in);
        }
        else {
          throw new DatabaseException("Don't know how to deserialize type: type=" + type.tag);
        }
        map.put(tag, value);
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public byte[] serialize() {
    try {
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytesOut);
      DataUtil.writeVLong(out, map.size());
      Iterator<Int2ObjectMap.Entry<Object>> iterator = map.int2ObjectEntrySet().fastIterator();
      while (iterator.hasNext()) {
        Int2ObjectMap.Entry<Object> entry = iterator.next();
        int tag = entry.getIntKey();
        Object value = entry.getValue();
        DynamicTag tagObj = tagsByTag.get(tag);
        if (tagObj == null) {
          throw new DatabaseException("Tag not defined: tag=" + tag);
        }
        DataUtil.writeVLong(out, tag);
        DataUtil.writeVLong(out, tagObj.type.tag);
        if (tagObj.type.tag == intType.tag) {
          DataUtil.writeVLong(out, (Integer) value);
        }
        else if (tagObj.type.tag == longType.tag) {
          if (value instanceof Integer) {
            DataUtil.writeVLong(out, (Integer)value);
          }
          else {
            DataUtil.writeVLong(out, (Long) value);
          }
        }
        else if (tagObj.type.tag == stringType.tag) {
          byte[] bytes = ((String) value).getBytes("utf-8");
          DataUtil.writeVLong(out, bytes.length);
          out.write(bytes);
        }
        else if (tagObj.type.tag == booleanType.tag) {
          out.writeBoolean((Boolean) value);
        }
        else if (tagObj.type.tag == byteArrayType.tag) {
          byte[] bytes = (byte[])value;
          DataUtil.writeVLong(out, bytes.length);
          out.write(bytes);
        }
        else if (tagObj.type.tag == arrayType.tag) {
          ((ComArray)value).serialize(out);
        }
      }

      out.close();
      return bytesOut.toByteArray();
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }
}
