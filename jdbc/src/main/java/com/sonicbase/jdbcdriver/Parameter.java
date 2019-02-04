package com.sonicbase.jdbcdriver;

import com.sonicbase.query.DatabaseException;
import com.sonicbase.util.Varint;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.sql.SQLException;
import java.sql.Types;

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class Parameter {

  private static final java.lang.String UTF_8_STR = "utf-8";

  private Parameter() {
  }

  public abstract static class ParameterBase<Q> {
    private Q value;

    protected ParameterBase() {
    }

    public Q getValue() {
      return value;
    }

    ParameterBase(Q value) {
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


  public static class NClobReader extends ParameterBase<char[]> {
    private long length = -1L;

    public NClobReader(Reader value, long length) throws IOException {
      java.lang.String str = new java.lang.String(IOUtils.toByteArray(value, UTF_8_STR), "utf-8");
      char[] chars = new char[str.length()];
      str.getChars(0, str.length(), chars, 0);
      super.setValue(chars);
      this.length = chars.length;
    }

    public NClobReader(Reader value) throws IOException {
      super(null);
      java.lang.String str = new java.lang.String(IOUtils.toByteArray(value, UTF_8_STR), "utf-8");
      char[] chars = new char[str.length()];
      str.getChars(0, str.length(), chars, 0);
      super.setValue(chars);
      this.length = chars.length;
    }

    public NClobReader(char[] bytes) {
      super(bytes);
      length = bytes.length;
    }

    @Override
    public void serialize(DataOutputStream out, boolean writeType) throws IOException {
      super.serialize(out, writeType);
      char[] bytes = getValue();
      out.writeInt(bytes.length);
      for (int i = 0; i < bytes.length; i++) {
        out.writeChar(bytes[i]);
      }
    }

    public static ParameterBase deserialize(DataInputStream in) throws IOException {
      int len = in.readInt();
      char[] bytes = new char[len];
      for (int i = 0; i < len; i++) {
        bytes[i] = in.readChar();
      }
      return new Parameter.NClobReader(bytes);
    }

    @Override
    public int getSqlType() {
      return Types.NCLOB;
    }
  }

  public static class ClobReader extends ParameterBase<char[]> {
    private long length;

    public ClobReader(Reader value, long length) throws IOException {
      java.lang.String str = new java.lang.String(IOUtils.toByteArray(value, UTF_8_STR), "utf-8");
      char[] chars = new char[str.length()];
      str.getChars(0, str.length(), chars, 0);
      super.setValue(chars);
      this.length = chars.length;
    }

    public ClobReader(Reader value) throws IOException {
      java.lang.String str = new java.lang.String(IOUtils.toByteArray(value, UTF_8_STR), "utf-8");
      char[] chars = new char[str.length()];
      str.getChars(0, str.length(), chars, 0);
      super.setValue(chars);
      this.length = chars.length;
    }

    public ClobReader(char[] bytes) {
      super(bytes);
      length = bytes.length;
    }

    @Override
    public void serialize(DataOutputStream out, boolean writeType) throws IOException {
      super.serialize(out, writeType);
      char[] bytes = getValue();
      out.writeInt(bytes.length);
      for (int i = 0; i < bytes.length; i++) {
        out.writeChar(bytes[i]);
      }
    }

    public static ParameterBase deserialize(DataInputStream in) throws IOException {
      int len = in.readInt();
      char[] bytes = new char[len];
      for (int i = 0; i < len; i++) {
        bytes[i] = in.readChar();
      }
      return new Parameter.ClobReader(bytes);
    }


    @Override
    public int getSqlType() {
      return Types.CLOB;
    }
  }

  public static class CharacterStream extends ParameterBase<char[]> {
    public CharacterStream(Reader value, long length) throws IOException {
      java.lang.String str = new java.lang.String(IOUtils.toByteArray(value, UTF_8_STR), "utf-8");
      char[] chars = new char[str.length()];
      str.getChars(0, str.length(), chars, 0);
      super.setValue(chars);
    }

    public CharacterStream(Reader value) throws IOException {
      java.lang.String str = new java.lang.String(IOUtils.toByteArray(value, UTF_8_STR), "utf-8");
      char[] chars = new char[str.length()];
      str.getChars(0, str.length(), chars, 0);
      super.setValue(chars);
    }

    public CharacterStream(char[] bytes) {
      super(bytes);
    }

    @Override
    public void serialize(DataOutputStream out, boolean writeType) throws IOException {
      super.serialize(out, writeType);
      char[] bytes = getValue();
      out.writeInt(bytes.length);
      for (int i = 0; i < bytes.length; i++) {
        out.writeChar(bytes[i]);
      }
    }

    public static ParameterBase deserialize(DataInputStream in) throws IOException {
      int len = in.readInt();
      char[] bytes = new char[len];
      for (int i = 0; i < len; i++) {
        bytes[i] = in.readChar();
      }
      return new Parameter.CharacterStream(bytes);
    }

    @Override
    public int getSqlType() {
      return Types.VARCHAR;
    }
  }

  public static class NCharacterStream extends ParameterBase<char[]> {
    public NCharacterStream(Reader value, long length) {
      try {
        java.lang.String str = new java.lang.String(IOUtils.toByteArray(value, UTF_8_STR), "utf-8");
        char[] chars = new char[str.length()];
        str.getChars(0, str.length(), chars, 0);
        super.setValue(chars);
      }
      catch (Exception  e) {
        throw new DatabaseException(e);
      }
    }

    public NCharacterStream(Reader value) throws IOException {
      java.lang.String str = new java.lang.String(IOUtils.toByteArray(value, UTF_8_STR), "utf-8");
      char[] chars = new char[str.length()];
      str.getChars(0, str.length(), chars, 0);
      super.setValue(chars);
    }

    public NCharacterStream(char[] bytes) {
        super(bytes);
      }

      @Override
      public void serialize(DataOutputStream out, boolean writeType) throws IOException {
        super.serialize(out, writeType);
        char[] bytes = getValue();
        out.writeInt(bytes.length);
        for (int i = 0; i < bytes.length; i++) {
          out.writeChar(bytes[i]);
        }
      }

      public static ParameterBase deserialize(DataInputStream in) throws IOException {
        int len = in.readInt();
        char[] bytes = new char[len];
        for (int i = 0; i < len; i++) {
          bytes[i] = in.readChar();
        }
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
    BinaryStream(byte[] value, long length) {
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

  public static class AsciiStream extends ParameterBase<char[]> {
    public AsciiStream(InputStream value) throws IOException {
      super(null);
      java.lang.String str = IOUtils.toString(value, "utf-8");
      char[] chars = new char[str.length()];
      str.getChars(0, str.length(), chars, 0);
      setValue(chars);
    }

    AsciiStream(InputStream value, long length) throws IOException {
      super(null);
      java.lang.String str = IOUtils.toString(value, "utf-8").substring(0, (int)length);
      char[] chars = new char[str.length()];
      str.getChars(0, str.length(), chars, 0);
      setValue(chars);
    }

    AsciiStream(char[] bytes) {
      super(bytes);
    }


    @Override
    public void serialize(DataOutputStream out, boolean writeType) throws IOException {
      super.serialize(out, writeType);
      char[] bytes = getValue();
      out.writeInt(bytes.length);
      for (int i = 0; i < bytes.length; i++) {
        out.writeChar(bytes[i]);
      }
    }

    public static ParameterBase deserialize(DataInputStream in) throws IOException {
      int len = in.readInt();
      char[] bytes = new char[len];
      for (int i = 0; i < len; i++) {
        bytes[i] = in.readChar();
      }
      return new Parameter.AsciiStream(bytes);
    }

    @Override
    public int getSqlType() {
      return Types.VARCHAR;
    }
  }

  public static class NClob extends ParameterBase<char[]> {
    public NClob(java.sql.NClob value) {
      java.lang.String str = ((com.sonicbase.query.impl.Clob)value).getString();
      char[] chars = new char[str.length()];
      str.getChars(0, str.length(), chars, 0);
      super.setValue(chars);
    }

    public NClob(char[] bytes) {
      super(bytes);
    }

    @Override
    public void serialize(DataOutputStream out, boolean writeType) throws IOException {
      super.serialize(out, writeType);
      char[] bytes = getValue();
      out.writeInt(bytes.length);
      for (int i = 0; i < bytes.length; i++) {
        out.writeChar(bytes[i]);
      }
    }

    public static ParameterBase deserialize(DataInputStream in) throws IOException {
      int len = in.readInt();
      char[] bytes = new char[len];
      for (int i = 0; i < len; i++) {
        bytes[i] = in.readChar();
      }
      return new Parameter.NClob(bytes);
    }
    @Override
    public int getSqlType() {
      return Types.NCLOB;
    }
  }

  public static class NString extends ParameterBase<char[]> {
    public NString(java.lang.String value) {
      java.lang.String str = value;
      char[] chars = new char[str.length()];
      str.getChars(0, str.length(), chars, 0);
      super.setValue(chars);
    }

    public NString(char[] bytes) {
      super(bytes);
    }

    @Override
    public void serialize(DataOutputStream out, boolean writeType) throws IOException {
      super.serialize(out, writeType);
      char[] bytes = getValue();
      out.writeInt(bytes.length);
      for (int i = 0; i < bytes.length; i++) {
        out.writeChar(bytes[i]);
      }
    }

    public static ParameterBase deserialize(DataInputStream in) throws IOException {
      int len = in.readInt();
      char[] bytes = new char[len];
      for (int i = 0; i < len; i++) {
        bytes[i] = in.readChar();
      }
      return new Parameter.NString(bytes);
    }

    @Override
    public int getSqlType() {
      return Types.NVARCHAR;
    }
  }

  public static class Null extends ParameterBase<Integer> {
    private final int sqlType;

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

  public static class Clob extends ParameterBase<char[]> {
    public Clob(java.sql.Clob x) throws UnsupportedEncodingException {
      java.lang.String str = ((com.sonicbase.query.impl.Clob)x).getString();
      char[] chars = new char[str.length()];
      str.getChars(0, str.length(), chars, 0);
      super.setValue(chars);
    }

    public Clob(char[] bytes) {
      super(bytes);
    }

    @Override
    public void serialize(DataOutputStream out, boolean writeType) throws IOException {
      super.serialize(out, writeType);
      char[] bytes = getValue();
      out.writeInt(bytes.length);
      for (int i = 0; i < bytes.length; i++) {
        out.writeChar(bytes[i]);
      }
    }

    public static ParameterBase deserialize(DataInputStream in) throws IOException {
      int len = in.readInt();
      char[] bytes = new char[len];
      for (int i = 0; i < len; i++) {
        bytes[i] = in.readChar();
      }
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

  public static class UnicodeStream extends ParameterBase<char[]> {
    public UnicodeStream(InputStream x, int length) throws IOException {
      super(null);
      java.lang.String str = IOUtils.toString(x, "utf-8");
      char[] chars = new char[str.length()];
      str.getChars(0, length, chars, 0);
      setValue(chars);
    }

    public UnicodeStream(char[] bytes) {
      super(bytes);
    }


    public UnicodeStream(InputStream value) throws IOException {
      super(null);
      java.lang.String str = IOUtils.toString(value, "utf-8");
      char[] chars = new char[str.length()];
      str.getChars(0, str.length(), chars, 0);
      setValue(chars);
    }

    @Override
    public void serialize(DataOutputStream out, boolean writeType) throws IOException {
      super.serialize(out, writeType);
      char[] bytes = getValue();
      out.writeInt(bytes.length);
      for (int i = 0; i < bytes.length; i++) {
        out.writeChar(bytes[i]);
      }
    }

    public static ParameterBase deserialize(DataInputStream in) throws IOException {
      int len = in.readInt();
      char[] bytes = new char[len];
      for (int i = 0; i < len; i++) {
        bytes[i] = in.readChar();
      }
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

  public static class String extends ParameterBase<char[]> {
    public String(char[] x) {
      super(x);
    }

    @Override
    public void serialize(DataOutputStream out, boolean writeType) throws IOException {
      super.serialize(out, writeType);
      char[] bytes = getValue();
      out.writeInt(bytes.length);
      for (int i = 0; i < bytes.length; i++) {
        out.writeChar(bytes[i]);
      }
    }

    public static ParameterBase deserialize(DataInputStream in) throws IOException {
      int len = in.readInt();
      char[] bytes = new char[len];
      for (int i = 0; i < len; i++) {
        bytes[i] = in.readChar();
      }
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
