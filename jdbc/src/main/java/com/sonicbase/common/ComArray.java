package com.sonicbase.common;

import com.sonicbase.query.DatabaseException;
import com.sonicbase.util.Varint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class ComArray {
  private static final Logger logger = LoggerFactory.getLogger(ComArray.class);

  private static final String UTF_8_STR = "utf-8";
  private ComObject.DynamicType nestedType;
  private Array array;

  public ComArray(ComObject.Type nestedType, int len) {
    array = new Array(len);
    this.nestedType = new ComObject.DynamicType(nestedType.tag);
  }

//  public ComArray(ComObject.Type nestedType) {
//    this.nestedType = new ComObject.DynamicType(nestedType.tag);
//  }

//  public ComArray(DataInputStream in) {
//    //deserialize(in);
//  }

  public ComArray(byte[] bytes, int[] offset) {
    deserialize(bytes, offset);
  }

  public class Array implements Iterable<Object> {
    private int pos;
    private Object[] array;

    public Array(int len) {
      this.array = new Object[len];
      this.pos = 0;
    }

    public Object get(int offset) {
      return array[offset];
    }

    public int size() {
      return pos;
    }

    public void add(Object value) {
      if (pos >= array.length) {
        if (logger.isDebugEnabled()) {
          logger.debug("Array out of bounds: expected={}, actual={}", array.length, pos);
        }
        Object[] newArray = new Object[(array.length + 1) * 2];
        System.arraycopy(array, 0, newArray, 0, array.length);
        array = newArray;
      }
      array[pos++] = value;
    }

    @Override
    public Iterator<Object> iterator() {
      return new MyIterator();
    }

    public boolean isEmpty() {
      return pos == 0;
    }

    class MyIterator implements Iterator<Object> {

      private int index = 0;

      public boolean hasNext() {
        return index < size();
      }

      public Object next() {
        return get(index++);
      }

      public void remove() {
        throw new UnsupportedOperationException("not supported yet");

      }
    }
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

  public Array getArray() {
    return array;
  }

  public void serialize(DataOutputStream out) {
    try {
      Varint.writeSignedVarLong(nestedType.tag, out);
      Varint.writeSignedVarLong(array.pos, out);
      for (int i = 0; i < array.pos; i++) {
        Object obj = array.array[i];
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
//          out.writeUTF(((String)obj));
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

  private void deserialize(byte[] bytes, int[] offset) {
    try {
      nestedType = new ComObject.DynamicType((int)DataUtils.readSignedVarLong(bytes, offset));
      int count = (int)DataUtils.readSignedVarLong(bytes, offset);
      array = new Array(count);
      for (int i = 0; i < count; i++) {
        if (nestedType.tag == ComObject.Type.OBJECT_TYPE.tag) {
          int len = (int) DataUtils.readSignedVarLong(bytes, offset);
          ComObject obj = new ComObject();
          obj.deserialize(bytes, offset);
          array.add(obj);
        }
        else if (nestedType.tag == ComObject.Type.LONG_TYPE.tag) {
          array.add(DataUtils.readSignedVarLong(bytes, offset));
        }
        else if (nestedType.tag == ComObject.Type.INT_TYPE.tag) {
          array.add((int)DataUtils.readSignedVarLong(bytes, offset));
        }
        else if (nestedType.tag == ComObject.Type.STRING_TYPE.tag) {
          int len = (int)DataUtils.readSignedVarLong(bytes, offset);
          String value = new String(bytes, offset[0], len, UTF_8_STR);
          offset[0] += len;
//          String value = DataUtils.bytesToUTF(bytes, offset);
          array.add(value);
        }
        else if (nestedType.tag == ComObject.Type.BOOLEAN_TYPE.tag) {
          array.add(1 == bytes[offset[0]++]);
        }
        else if (nestedType.tag == ComObject.Type.BYTE_ARRAY_TYPE.tag) {
          int len = (int)DataUtils.readSignedVarLong(bytes, offset);
          byte[] b = new byte[len];
          System.arraycopy(bytes, offset[0], b, 0, len);
          offset[0] += len;
          array.add(b);
        }
        else if (nestedType.tag == ComObject.Type.ARRAY_TYPE.tag) {
          ComArray inner = new ComArray(bytes, offset);
          array.add(inner);
        }
        else if (nestedType.tag == ComObject.Type.TINY_INT_TYPE.tag) {
          array.add(bytes[offset[0]++]);
        }
        else if (nestedType.tag == ComObject.Type.SMALL_INT_TYPE.tag) {
          array.add(DataUtils.bytesToShort(bytes, offset[0]));
          offset[0] += 2;
        }
        else if (nestedType.tag == ComObject.Type.FLOAT_TYPE.tag) {
          int v = DataUtils.bytesToInt(bytes, offset[0]);
          offset[0] += 2;
          array.add(Float.intBitsToFloat(v));
        }
        else if (nestedType.tag == ComObject.Type.DOUBLE_TYPE.tag) {
          long v = DataUtils.bytesToLong(bytes, offset[0]);
          offset[0] += 8;
          array.add(Double.longBitsToDouble(v));
        }
        else if (nestedType.tag == ComObject.Type.BIG_DECIMAL_TYPE.tag) {
          int len = (int)DataUtils.readSignedVarLong(bytes, offset);
          String str = new String(bytes, offset[0], len, UTF_8_STR);
          offset[0] += len;
          array.add(new java.math.BigDecimal(str));
        }
        else if (nestedType.tag == ComObject.Type.DATE_TYPE.tag) {
          java.sql.Date date = new java.sql.Date(DataUtils.readSignedVarLong(bytes, offset));
          array.add(date);
        }
        else if (nestedType.tag == ComObject.Type.TIME_TYPE.tag) {
          java.sql.Time time = new java.sql.Time(DataUtils.readSignedVarLong(bytes, offset));
          array.add(time);
        }
        else if (nestedType.tag == ComObject.Type.TIME_STAMP_TYPE.tag) {
          java.sql.Timestamp timestamp = Timestamp.valueOf(DataUtils.bytesToUTF(bytes, offset));
          array.add(timestamp);
        }
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public ComArray addArray(ComObject.Type type, int size) {
    ComArray localArray = new ComArray(type, size);
    this.array.add(localArray);
    return localArray;
  }

  public void remove(Object innerObj) {
    for (int i = 0; i < array.pos; i++) {
      if (array.array[i].equals(innerObj)) {
        System.arraycopy(array.array, i + 1, array.array, i, array.pos - i - 1);
        array.pos--;
        return;
      }
    }
  }
}
