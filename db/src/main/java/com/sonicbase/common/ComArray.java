package com.sonicbase.common;

import com.sonicbase.query.DatabaseException;
import com.sonicbase.util.DataUtil;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by lowryda on 6/24/17.
 */
public class ComArray {
  private ComObject.DynamicType nestedType;
  private List<Object> array = new ArrayList<>();

  public ComArray(ComObject.Type nestedType) {
    this.nestedType = new ComObject.DynamicType(nestedType.tag);
  }

  public ComArray(DataInputStream in) {
    deserialize(in);
  }

  public void add(ComObject value) {
    array.add(value);
  }

  public void add(int value) {
    array.add(value);
  }

  public void add(long value) {
    array.add(value);
  }

  public void add(String value) {
    array.add(value);
  }

  public void add(byte[] value) {
    array.add(value);
  }

  public List<Object> getArray() {
    return array;
  }

  public void serialize(DataOutputStream out) {
    try {
      DataUtil.writeVLong(out, nestedType.tag);
      DataUtil.writeVLong(out, array.size());
      for (Object obj : array) {
        if (nestedType.tag == ComObject.Type.objectType.tag) {
          byte[] bytes = ((ComObject)obj).serialize();
          DataUtil.writeVLong(out, bytes.length);
          out.write(bytes);
        }
        else if (nestedType.tag == ComObject.Type.longType.tag) {
          if (obj instanceof Integer) {
            DataUtil.writeVLong(out, (Integer)obj);
          }
          else {
            DataUtil.writeVLong(out, (Long) obj);
          }
        }
        else if (nestedType.tag == ComObject.Type.intType.tag) {
          DataUtil.writeVLong(out, (Integer)obj);
        }
        else if (nestedType.tag == ComObject.Type.booleanType.tag) {
          out.writeBoolean((Boolean)obj);
        }
        else if (nestedType.tag == ComObject.Type.stringType.tag) {
          byte[] bytes = ((String)obj).getBytes("utf-8");
          DataUtil.writeVLong(out, bytes.length);
          out.write(bytes);
        }
        else if (nestedType.tag == ComObject.Type.byteArrayType.tag) {
          DataUtil.writeVLong(out, ((byte[])obj).length);
          out.write((byte[])obj);
        }
        else if (nestedType.tag == ComObject.Type.arrayType.tag) {
          ((ComArray)obj).serialize(out);
        }
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private void deserialize(DataInputStream in) {
    try {
      array.clear();
      nestedType = new ComObject.DynamicType((int)DataUtil.readVLong(in));
      int count = (int)DataUtil.readVLong(in);
      for (int i = 0; i < count; i++) {
        if (nestedType.tag == ComObject.Type.objectType.tag) {
          int len = (int)DataUtil.readVLong(in);
          byte[] bytes = new byte[len];
          in.readFully(bytes);
          ComObject obj = new ComObject();
          obj.deserialize(bytes);
          array.add(obj);
        }
        else if (nestedType.tag == ComObject.Type.longType.tag) {
          array.add(DataUtil.readVLong(in));
        }
        else if (nestedType.tag == ComObject.Type.intType.tag) {
          array.add((int)DataUtil.readVLong(in));
        }
        else if (nestedType.tag == ComObject.Type.stringType.tag) {
          int len = (int)DataUtil.readVLong(in);
          byte[] bytes = new byte[len];
          in.readFully(bytes);
          String value = new String(bytes, "utf-8");
          array.add(value);
        }
        else if (nestedType.tag == ComObject.Type.booleanType.tag) {
          array.add(in.readBoolean());
        }
        else if (nestedType.tag == ComObject.Type.byteArrayType.tag) {
          int len = (int)DataUtil.readVLong(in);
          byte[] bytes = new byte[len];
          in.readFully(bytes);
          array.add(bytes);
        }
        else if (nestedType.tag == ComObject.Type.arrayType.tag) {
          ComArray inner = new ComArray(in);
          array.add(inner);
        }
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public ComArray addArray(ComObject.Tag tag, ComObject.Type type) {
    ComArray array = new ComArray(type);
    this.array.add(array);
    return array;
  }
}
