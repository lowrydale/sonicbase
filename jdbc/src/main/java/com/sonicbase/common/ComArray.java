package com.sonicbase.common;

import com.sonicbase.query.DatabaseException;
import org.apache.giraph.utils.Varint;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by lowryda on 6/24/17.
 */
public class ComArray {
  public static final String UTF_8_STR = "utf-8";
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
      Varint.writeSignedVarLong(nestedType.tag, out);
      Varint.writeSignedVarLong(array.size(), out);
      for (Object obj : array) {
        if (nestedType.tag == ComObject.Type.OBJECT_TYPE.tag) {
          byte[] bytes = ((ComObject)obj).serialize();
          Varint.writeSignedVarLong(bytes.length, out);
          out.write(bytes);
        }
        else if (nestedType.tag == ComObject.Type.LONG_TYPE.tag) {
          if (obj instanceof Integer) {
            Varint.writeSignedVarLong((Integer)obj, out);
          }
          else {
            Varint.writeSignedVarLong((Long) obj, out);
          }
        }
        else if (nestedType.tag == ComObject.Type.INT_TYPE.tag) {
          Varint.writeSignedVarLong((Integer)obj, out);
        }
        else if (nestedType.tag == ComObject.Type.BOOLEAN_TYPE.tag) {
          out.writeBoolean((Boolean)obj);
        }
        else if (nestedType.tag == ComObject.Type.STRING_TYPE.tag) {
          byte[] bytes = ((String)obj).getBytes(UTF_8_STR);
          Varint.writeSignedVarLong(bytes.length, out);
          out.write(bytes);
        }
        else if (nestedType.tag == ComObject.Type.BYTE_ARRAY_TYPE.tag) {
          Varint.writeSignedVarLong(((byte[])obj).length, out);
          out.write((byte[])obj);
        }
        else if (nestedType.tag == ComObject.Type.ARRAY_TYPE.tag) {
          ((ComArray)obj).serialize(out);
        }
        else if (nestedType.tag == ComObject.Type.TINY_INT_TYPE.tag) {
          out.write((byte)obj);
        }
        else if (nestedType.tag == ComObject.Type.SMALL_INT_TYPE.tag) {
          out.writeShort((short)obj);
        }
        else if (nestedType.tag == ComObject.Type.FLOAT_TYPE.tag) {
          out.writeFloat((float)obj);
        }
        else if (nestedType.tag == ComObject.Type.DOUBLE_TYPE.tag) {
          out.writeDouble((double)obj);
        }
        else if (nestedType.tag == ComObject.Type.BIG_DECIMAL_TYPE.tag) {
          byte[] bytes = ((BigDecimal) obj).toPlainString().getBytes(UTF_8_STR);
          Varint.writeSignedVarLong(bytes.length, out);
          out.write(bytes);
        }
        else if (nestedType.tag == ComObject.Type.DATE_TYPE.tag) {
          Varint.writeSignedVarLong(((java.sql.Date)obj).getTime(), out);
        }
        else if (nestedType.tag == ComObject.Type.TIME_TYPE.tag) {
          Varint.writeSignedVarLong(((java.sql.Time)obj).getTime(), out);
        }
        else if (nestedType.tag == ComObject.Type.TIME_STAMP_TYPE.tag) {
          out.writeUTF(((java.sql.Timestamp)obj).toString());
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
      nestedType = new ComObject.DynamicType((int)Varint.readSignedVarLong(in));
      int count = (int)Varint.readSignedVarLong(in);
      for (int i = 0; i < count; i++) {
        if (nestedType.tag == ComObject.Type.OBJECT_TYPE.tag) {
          int len = (int)Varint.readSignedVarLong(in);
          byte[] bytes = new byte[len];
          in.readFully(bytes);
          ComObject obj = new ComObject();
          obj.deserialize(new DataInputStream(new ByteArrayInputStream(bytes)));
          array.add(obj);
        }
        else if (nestedType.tag == ComObject.Type.LONG_TYPE.tag) {
          array.add(Varint.readSignedVarLong(in));
        }
        else if (nestedType.tag == ComObject.Type.INT_TYPE.tag) {
          array.add((int)Varint.readSignedVarLong(in));
        }
        else if (nestedType.tag == ComObject.Type.STRING_TYPE.tag) {
          int len = (int)Varint.readSignedVarLong(in);
          byte[] bytes = new byte[len];
          in.readFully(bytes);
          String value = new String(bytes, UTF_8_STR);
          array.add(value);
        }
        else if (nestedType.tag == ComObject.Type.BOOLEAN_TYPE.tag) {
          array.add(in.readBoolean());
        }
        else if (nestedType.tag == ComObject.Type.BYTE_ARRAY_TYPE.tag) {
          int len = (int)Varint.readSignedVarLong(in);
          byte[] bytes = new byte[len];
          in.readFully(bytes);
          array.add(bytes);
        }
        else if (nestedType.tag == ComObject.Type.ARRAY_TYPE.tag) {
          ComArray inner = new ComArray(in);
          array.add(inner);
        }
        else if (nestedType.tag == ComObject.Type.TINY_INT_TYPE.tag) {
          array.add(in.read());
        }
        else if (nestedType.tag == ComObject.Type.SMALL_INT_TYPE.tag) {
          array.add(in.readShort());
        }
        else if (nestedType.tag == ComObject.Type.FLOAT_TYPE.tag) {
          array.add(in.readFloat());
        }
        else if (nestedType.tag == ComObject.Type.DOUBLE_TYPE.tag) {
          array.add(in.readDouble());
        }
        else if (nestedType.tag == ComObject.Type.BIG_DECIMAL_TYPE.tag) {
          int len = (int)Varint.readSignedVarLong(in);
          byte[] bytes = new byte[len];
          in.readFully(bytes);
          String str = new String(bytes, UTF_8_STR);
          array.add(new java.math.BigDecimal(str));
        }
        else if (nestedType.tag == ComObject.Type.DATE_TYPE.tag) {
          java.sql.Date date = new java.sql.Date(Varint.readSignedVarLong(in));
          array.add(date);
        }
        else if (nestedType.tag == ComObject.Type.TIME_TYPE.tag) {
          java.sql.Time time = new java.sql.Time(Varint.readSignedVarLong(in));
          array.add(time);
        }
        else if (nestedType.tag == ComObject.Type.TIME_STAMP_TYPE.tag) {
          java.sql.Timestamp timestamp = Timestamp.valueOf(in.readUTF());
          array.add(timestamp);
        }
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public ComArray addArray(ComObject.Type type) {
    ComArray localArray = new ComArray(type);
    this.array.add(localArray);
    return localArray;
  }
}
