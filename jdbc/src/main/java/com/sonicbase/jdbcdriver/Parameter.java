package com.sonicbase.jdbcdriver;

import com.sonicbase.query.DatabaseException;
import org.apache.commons.io.IOUtils;
import org.apache.giraph.utils.Varint;

import java.io.*;
import java.sql.SQLException;
import java.sql.Types;

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class Parameter {

  public static final java.lang.String UTF_8_STR = "utf-8";

  private Parameter() {
  }

  public abstract static class ParameterBase<Q> {
    private Q value;

    public Q getValue() {
      return value;
    }

    public ParameterBase(Q value) {
      this.value = value;
    }

    public void setValue(Q value) {
      this.value = value;
    }

    public abstract int getSqlType();

    public void serialize(DataOutputStream out, boolean writeType) throws IOException {
      if (writeType) {
        out.writeInt(getSqlType());
      }
    }
  }


  public static class NClobReader extends ParameterBase<byte[]> {
    private long length = -1L;

    public NClobReader(Reader value) throws IOException {
      super(null);
      setValue(IOUtils.toByteArray(value, UTF_8_STR));
      length = getValue().length;
    }

    public NClobReader(Reader value, long length) throws IOException {
      super(null);
      setValue(IOUtils.toByteArray(value, UTF_8_STR));
      this.length = length;
    }

    public NClobReader(byte[] bytes) {
      super(bytes);
      length = bytes.length;
    }

    @Override
    public void serialize(DataOutputStream out, boolean writeType) throws IOException {
      super.serialize(out, writeType);
      byte[] bytes = getValue();
      out.writeInt((int)length);
      out.write(bytes, 0, (int)length);
    }

    public static ParameterBase deserialize(DataInputStream in) throws IOException {
      int len = in.readInt();
      byte[] bytes = new byte[len];
      in.readFully(bytes);
      return new Parameter.NClobReader(bytes);
    }


    @Override
    public int getSqlType() {
      return Types.NCLOB;
    }
  }

  public static class ClobReader extends ParameterBase<byte[]> {
    private long length;

    public ClobReader(Reader value) throws IOException {
      super(null);
      setValue(IOUtils.toByteArray(value, UTF_8_STR));
      length = getValue().length;
    }

    public ClobReader(Reader value, long length) throws IOException {
      super(null);
      setValue(IOUtils.toByteArray(value, UTF_8_STR));
      this.length = length;
    }

    public ClobReader(byte[] bytes) {
      super(bytes);
      length = bytes.length;
    }

    @Override
    public void serialize(DataOutputStream out, boolean writeType) throws IOException {
      super.serialize(out, writeType);
      byte[] bytes = getValue();
      out.writeInt((int)length);
      out.write(bytes, 0, (int)length);
    }

    public static ParameterBase deserialize(DataInputStream in) throws IOException {
      int len = in.readInt();
      byte[] bytes = new byte[len];
      in.readFully(bytes);
      return new Parameter.ClobReader(bytes);
    }



    @Override
    public int getSqlType() {
      return Types.CLOB;
    }
  }

  public static class CharacterStream extends ParameterBase<byte[]> {
    public CharacterStream(Reader value) throws IOException {
      super(IOUtils.toString(value).getBytes(UTF_8_STR));
    }

    public CharacterStream(Reader value, long length) throws IOException {
      super(IOUtils.toString(value).substring(0, (int)length).getBytes(UTF_8_STR));
    }

    public CharacterStream(byte[] bytes) {
      super(bytes);
    }

    @Override
    public void serialize(DataOutputStream out, boolean writeType) throws IOException {
      super.serialize(out, writeType);
      byte[] bytes = getValue();
      out.writeInt(bytes.length);
      out.write(bytes);
    }

    public static ParameterBase deserialize(DataInputStream in) throws IOException {
      int len = in.readInt();
      byte[] bytes = new byte[len];
      in.readFully(bytes);
      return new Parameter.CharacterStream(bytes);
    }

    @Override
    public int getSqlType() {
      return Types.VARCHAR;
    }
  }

  public static class NCharacterStream extends ParameterBase<byte[]> {
    public NCharacterStream(Reader value) throws IOException {
      super(null);
      setValue(IOUtils.toByteArray(value, UTF_8_STR));
    }

    public NCharacterStream(Reader value, long length) {
      super(null);
      try {
        setValue(IOUtils.toString(value).substring(0, (int)length).getBytes(UTF_8_STR));
      }
      catch (IOException e) {
        throw new DatabaseException(e);
      }
    }

    public NCharacterStream(byte[] bytes) {
      super(bytes);
    }

    @Override
    public void serialize(DataOutputStream out, boolean writeType) throws IOException {
      super.serialize(out, writeType);
      byte[] bytes = getValue();
      out.writeInt(bytes.length);
      out.write(bytes);
    }

    public static ParameterBase deserialize(DataInputStream in) throws IOException {
      int len = in.readInt();
      byte[] bytes = new byte[len];
      in.readFully(bytes);
      return new Parameter.NCharacterStream(bytes);
    }

    @Override
    public int getSqlType() {
      return Types.NVARCHAR;
    }
  }

  public static class BinaryStream extends ParameterBase<byte[]> {

    private long length = -1L;

    public BinaryStream(byte[] value) {
      super(value);
      this.length = value.length;
    }
    public BinaryStream(byte[] value, long length) {
      super(value);
      this.length = length;
    }

    @Override
    public int getSqlType() {
      return Types.VARBINARY;
    }

    @Override
    public void serialize(DataOutputStream out, boolean writeType) throws IOException {
      super.serialize(out, writeType);
      byte[] buffer = getValue();
      out.writeLong(length);
      out.write(buffer);
    }

    public static ParameterBase deserialize(DataInputStream in) throws IOException {
      long length = in.readLong();
      byte[] bytes = new byte[(int) length];
      in.readFully(bytes);
      return new Parameter.BinaryStream(bytes, length);
    }
  }

  public static class AsciiStream extends ParameterBase<byte[]> {
    public AsciiStream(InputStream value) throws IOException {
      super(null);
      setValue(IOUtils.toByteArray(value));
    }

    public AsciiStream(InputStream value, long length) throws IOException {
      super(null);
      setValue(IOUtils.toString(value, UTF_8_STR).substring(0, (int)length).getBytes(UTF_8_STR));
    }

    public AsciiStream(byte[] bytes) {
      super(bytes);
    }

    @Override
    public void serialize(DataOutputStream out, boolean writeType) throws IOException {
      super.serialize(out, writeType);
      byte[] bytes = getValue();
      out.writeInt(bytes.length);
      out.write(bytes);
    }

    public static ParameterBase deserialize(DataInputStream in) throws IOException {
      int len = in.readInt();
      byte[] bytes = new byte[len];
      in.readFully(bytes);
      return new Parameter.AsciiStream(bytes);
    }



    @Override
    public int getSqlType() {
      return Types.VARCHAR;
    }
  }

  public static class NClob extends ParameterBase<byte[]> {
    public NClob(java.sql.NClob value) throws UnsupportedEncodingException {
      super(((com.sonicbase.query.impl.NClob)value).getString().getBytes(UTF_8_STR));
    }

    public NClob(byte[] bytes) {
      super(bytes);
    }

    @Override
    public void serialize(DataOutputStream out, boolean writeType) throws IOException {
       super.serialize(out, writeType);
       byte[] bytes = getValue();
       out.writeInt(bytes.length);
       out.write(bytes);
     }

     public static ParameterBase deserialize(DataInputStream in) throws IOException {
       int len = in.readInt();
       byte[] bytes = new byte[len];
       in.readFully(bytes);
       return new Parameter.NClob(bytes);
     }
    @Override
    public int getSqlType() {
      return Types.NCLOB;
    }
  }

  public static class NString extends ParameterBase<byte[]> {
    public NString(java.lang.String value) throws UnsupportedEncodingException {
      super(null);
      setValue(value.getBytes(UTF_8_STR));
    }

    @Override
    public void serialize(DataOutputStream out, boolean b) throws IOException {
      super.serialize(out, b);
      byte[] bytes = getValue();
      out.writeInt(bytes.length);
      out.write(bytes);
    }

    public static ParameterBase deserialize(DataInputStream in) throws IOException {
      int len = in.readInt();
      byte[] bytes = new byte[len];
      in.readFully(bytes);
      return new Parameter.NString(new java.lang.String(bytes, UTF_8_STR));
    }

    @Override
    public int getSqlType() {
      return Types.NVARCHAR;
    }
  }

  public static class Null extends ParameterBase<Integer> {
    private int sqlType;

    public Null(int sqlType) {
      super(null);
      this.sqlType = sqlType;
    }

    @Override
    public int getSqlType() {
      return sqlType;
    }
  }

  public static class Timestamp extends ParameterBase<java.sql.Timestamp> {
    public Timestamp(java.sql.Timestamp x) {
      super(x);
    }

    @Override
    public void serialize(DataOutputStream out, boolean b) throws IOException {
      super.serialize(out, b);
      out.writeLong(getValue().getTime());
    }

    public static ParameterBase deserialize(DataInputStream in) throws IOException {
      long time = in.readLong();
      return new Parameter.Timestamp(new java.sql.Timestamp(time));
    }

    @Override
    public int getSqlType() {
      return Types.TIMESTAMP;
    }
  }

  public static class Time extends ParameterBase<java.sql.Time> {
    public Time(java.sql.Time x) {
      super(x);
    }

    @Override
    public void serialize(DataOutputStream out, boolean b) throws IOException {
      super.serialize(out, b);
      out.writeLong(getValue().getTime());
    }

    public static ParameterBase deserialize(DataInputStream in) throws IOException {
      long time = in.readLong();
      return new Parameter.Time(new java.sql.Time(time));
    }

    @Override
    public int getSqlType() {
      return Types.TIME;
    }
  }

  public static class Date extends ParameterBase<java.sql.Date> {
    public Date(java.sql.Date x) {
      super(x);
    }

    @Override
    public void serialize(DataOutputStream out, boolean b) throws IOException {
      super.serialize(out, b);
      out.writeLong(getValue().getTime());
    }

    public static ParameterBase deserialize(DataInputStream in) throws IOException {
      long time = in.readLong();
      return new Parameter.Date(new java.sql.Date(time));
    }

    @Override
    public int getSqlType() {
      return Types.DATE;
    }
  }

  public static class Clob extends ParameterBase<byte[]> {
    public Clob(java.sql.Clob x) throws UnsupportedEncodingException {
      super(((com.sonicbase.query.impl.Clob)x).getString().getBytes(UTF_8_STR));
    }

    public Clob(byte[] bytes) {
      super(bytes);
    }

    @Override
    public void serialize(DataOutputStream out, boolean writeType) throws IOException {
       super.serialize(out, writeType);
       byte[] bytes = getValue();
       out.writeInt(bytes.length);
       out.write(bytes);
     }

     public static ParameterBase deserialize(DataInputStream in) throws IOException {
       int len = in.readInt();
       byte[] bytes = new byte[len];
       in.readFully(bytes);
       return new Parameter.Clob(bytes);
     }

    @Override
    public int getSqlType() {
      return Types.CLOB;
    }
  }

  public static class Blob extends ParameterBase<byte[]> {
    int length;

    public Blob(java.sql.Blob x) throws SQLException {
      super(x.getBytes(0, (int)x.length()));
      length = getValue().length;
    }

    public Blob(InputStream in, int len) throws IOException {
      super(null);
      setValue(IOUtils.toByteArray(in));
      length = len;
    }

    public Blob(byte[] x) {
      super(x);
    }

    @Override
    public void serialize(DataOutputStream out, boolean writeType) throws IOException {
      super.serialize(out, writeType);
      byte[] buffer = getValue();
      out.writeLong(length);
      out.write(buffer, 0, length);
    }

    public static ParameterBase deserialize(DataInputStream in) throws IOException {
      long length = in.readLong();
      byte[] bytes = new byte[(int) length];
      in.readFully(bytes);
      return new Parameter.Blob(bytes);
    }

    @Override
    public int getSqlType() {
      return Types.BLOB;
    }
  }

  public static class UnicodeStream extends ParameterBase<byte[]> {
    public UnicodeStream(InputStream x, int length) throws IOException {
      super(null);
      setValue(IOUtils.toString(x, UTF_8_STR).substring(0, length).getBytes(UTF_8_STR));
    }

    public UnicodeStream(byte[] bytes) {
      super(bytes);
    }

    @Override
    public void serialize(DataOutputStream out, boolean writeType) throws IOException {
      super.serialize(out, writeType);
      byte[] bytes = getValue();
      out.writeInt(bytes.length);
      out.write(bytes);
    }

    public static ParameterBase deserialize(DataInputStream in) throws IOException {
      int len = in.readInt();
      byte[] bytes = new byte[len];
      in.readFully(bytes);
      return new Parameter.UnicodeStream(bytes);
    }

    @Override
    public int getSqlType() {
      return Types.VARCHAR;
    }
  }


  public static class Bytes extends ParameterBase<byte[]> {
    public Bytes(byte[] x) {
      super(x);
    }

    @Override
    public void serialize(DataOutputStream out, boolean writeType) throws IOException {
      super.serialize(out, writeType);
      byte[] buffer = getValue();
      out.writeLong(buffer.length);
      out.write(buffer);
    }

    public static ParameterBase deserialize(DataInputStream in) throws IOException {
      long length = in.readLong();
      byte[] bytes = new byte[(int) length];
      in.readFully(bytes);
      return new Parameter.Bytes(bytes);
    }

    @Override
    public int getSqlType() {
      return Types.VARBINARY;
    }
  }

  public static class String extends ParameterBase<byte[]> {
    public String(byte[] x) {
      super(x);
    }

    @Override
    public void serialize(DataOutputStream out, boolean writeType) throws IOException {
      super.serialize(out, writeType);
      byte[] bytes = getValue();
      out.writeInt(bytes.length);
      out.write(bytes);
    }

    public static ParameterBase deserialize(DataInputStream in) throws IOException {
      int len = in.readInt();
      byte[] bytes = new byte[len];
      in.readFully(bytes);
      return new Parameter.String(bytes);
    }

    @Override
    public int getSqlType() {
      return Types.VARCHAR;
    }
  }

  public static class BigDecimal extends ParameterBase<java.math.BigDecimal> {
    public BigDecimal(java.math.BigDecimal x) {
      super(x);
    }

    @Override
    public int getSqlType() {
      return Types.NUMERIC;
    }

    public static ParameterBase deserialize(DataInputStream in) throws IOException {
      int len = (int) Varint.readSignedVarLong(in);
      byte[] buffer = new byte[len];
      in.readFully(buffer);
      java.lang.String str = new java.lang.String(buffer, UTF_8_STR);
      return new Parameter.BigDecimal(new java.math.BigDecimal(str));
    }

    @Override
    public void serialize(DataOutputStream out, boolean b) throws IOException {
      super.serialize(out, b);
      java.lang.String strValue = getValue().toPlainString();
      byte[] bytes = strValue.getBytes(UTF_8_STR);
      Varint.writeSignedVarLong(bytes.length, out);
      out.write(bytes);
    }
  }

  public static class Double extends ParameterBase<java.lang.Double> {
    public Double(double x) {
      super(x);
    }

    @Override
    public void serialize(DataOutputStream out, boolean b) throws IOException {
      super.serialize(out, b);
      out.writeDouble(getValue());
    }

    public static ParameterBase deserialize(DataInputStream in) throws IOException {
      return new Parameter.Double(in.readDouble());
    }

    @Override
    public int getSqlType() {
      return Types.DOUBLE;
    }
  }

  public static class Float extends ParameterBase<java.lang.Float> {
    public Float(float x) {
      super(x);
    }

    @Override
    public void serialize(DataOutputStream out, boolean b) throws IOException {
      super.serialize(out, b);
      out.writeFloat(getValue());
    }

    public static ParameterBase deserialize(DataInputStream in) throws IOException {
      return new Parameter.Float(in.readFloat());
    }

    @Override
    public int getSqlType() {
      return Types.FLOAT;
    }
  }

  public static class Long extends ParameterBase<java.lang.Long> {
    public Long(long x) {
      super(x);
    }

    @Override
    public void serialize(DataOutputStream out, boolean b) throws IOException {
      super.serialize(out, b);
      out.writeLong(getValue());
    }

    public static ParameterBase deserialize(DataInputStream in) throws IOException {
      return new Parameter.Long(in.readLong());
    }

    @Override
    public int getSqlType() {
      return Types.BIGINT;
    }
  }

  public static class Int extends ParameterBase<java.lang.Integer> {
    public Int(int x) {
      super(x);
    }

    @Override
    public void serialize(DataOutputStream out, boolean b) throws IOException {
      super.serialize(out, b);
      out.writeInt(getValue());
    }

    public static ParameterBase deserialize(DataInputStream in) throws IOException {
      return new Parameter.Int(in.readInt());
    }

    @Override
    public int getSqlType() {
      return Types.INTEGER;
    }
  }

  public static class Short extends ParameterBase<java.lang.Short> {
    public Short(short x) {
      super(x);
    }

    @Override
    public void serialize(DataOutputStream out, boolean b) throws IOException {
      super.serialize(out, b);
      out.writeShort(getValue());
    }

    public static ParameterBase deserialize(DataInputStream in) throws IOException {
      return new Parameter.Short(in.readShort());
    }

    @Override
    public int getSqlType() {
      return Types.SMALLINT;
    }
  }

  public static class Byte extends ParameterBase<java.lang.Byte> {
    public Byte(byte x) {
      super(x);
    }

    @Override
    public int getSqlType() {
      return Types.TINYINT;
    }

    @Override
    public void serialize(DataOutputStream out, boolean b) throws IOException {
       super.serialize(out, b);
       out.write(getValue());
     }

     public static ParameterBase deserialize(DataInputStream in) throws IOException {
       return new Parameter.Byte((byte)in.read());
     }


  }

  public static class Boolean extends ParameterBase<java.lang.Boolean> {
    public Boolean(boolean x) {
      super(x);
    }

    @Override
    public void serialize(DataOutputStream out, boolean b) throws IOException {
      super.serialize(out, b);
      out.writeBoolean(getValue());
    }

    public static ParameterBase deserialize(DataInputStream in) throws IOException {
      return new Parameter.Boolean(in.readBoolean());
    }

    @Override
    public int getSqlType() {
      return Types.BOOLEAN;
    }
  }
}
