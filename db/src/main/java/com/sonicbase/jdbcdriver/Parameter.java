package com.sonicbase.jdbcdriver;

import com.sonicbase.util.DataUtil;
import com.sonicbase.util.StreamUtils;

import java.io.*;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Calendar;

/**
 * Created by IntelliJ IDEA.
 * User: lowryda
 * Date: Nov 9, 2011
 * Time: 2:33:41 PM
 */
public class Parameter {

  public static abstract class ParameterBase<Q> {
    private Q value;

    public Q getValue() {
      return value;
    }

    public ParameterBase(Q value) {
      this.value = value;
    }

    public abstract int getSqlType();

    public void serialize(DataOutputStream out, boolean writeType) throws IOException {
      if (writeType) {
        out.writeInt(getSqlType());
      }
    }
  }


  public static class NClobReader extends ParameterBase<Reader> {
    private long length = -1L;

    public NClobReader(Reader value) {
      super(value);
    }

    public NClobReader(Reader reader, long length) {
      super(reader);
      this.length = length;
    }

    @Override
    public int getSqlType() {
      return Types.NCLOB;
    }
  }

  public static class ClobReader extends ParameterBase<Reader> {
    private long length;

    public ClobReader(Reader value) {
      super(value);
    }

    public ClobReader(Reader reader, long length) {
      super(reader);
      this.length = length;
    }

    @Override
    public int getSqlType() {
      return Types.CLOB;
    }
  }

  public static class CharacterStream extends ParameterBase<byte[]> {
    private long length = -1L;

    public CharacterStream(Reader value) throws IOException {
      super(StreamUtils.readerToString(value).getBytes("utf-8"));
    }
    public CharacterStream(Reader value, long length) throws IOException {
      super(StreamUtils.readerToString(value).getBytes("utf-8"));
      this.length = length;
    }

    @Override
    public int getSqlType() {
      return Types.VARCHAR;
    }
  }

  public static class NCharacterStream extends ParameterBase<Reader> {
    private long length;

    public NCharacterStream(Reader value) {
      super(value);
    }

    public NCharacterStream(Reader value, long length) {
      super(value);
      this.length = length;
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
    }
    public BinaryStream(byte[] value, long length) {
      super(value);
      this.length = length;
    }

    @Override
    public int getSqlType() {
      return Types.VARBINARY;
    }

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

  public static class AsciiStream extends ParameterBase<InputStream> {
    private long length = -1L;

    public AsciiStream(InputStream value) {
      super(value);
    }
    public AsciiStream(InputStream value, long length) {
      super(value);
      this.length = length;
    }

    @Override
    public int getSqlType() {
      return Types.VARCHAR;
    }
  }

  public static class NClob extends ParameterBase<byte[]> {
    public NClob(java.sql.NClob value) throws UnsupportedEncodingException {
      super(((com.sonicbase.query.impl.NClob)value).getString().getBytes("utf-8"));
    }

    public NClob(ParameterBase parm) {
      super(((Parameter.NClob)parm).getValue());
    }

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
      return Types.NCLOB;
    }
  }

  public static class NString extends ParameterBase<java.lang.String> {
    public NString(java.lang.String value) {
      super(value);
    }

    public void serialize(DataOutputStream out, boolean b) throws IOException {
      super.serialize(out, b);
      byte[] bytes = getValue().getBytes("utf-8");
      out.writeInt(bytes.length);
      out.write(bytes);
    }
    @Override
    public int getSqlType() {
      return Types.NVARCHAR;
    }
  }

  public static class RowId extends ParameterBase<java.sql.RowId> {
    public RowId(java.sql.RowId x) {
      super(x);
    }

    public RowId(ParameterBase parm) {
      super(((Parameter.RowId)parm).getValue());
    }

    public void serialize(DataOutputStream out, boolean b) throws IOException {
      super.serialize(out, b);
      out.writeInt(getValue().getBytes().length);
      out.write(getValue().getBytes());
    }

    @Override
    public int getSqlType() {
      return Types.ROWID;
    }
  }

  public static class Null extends ParameterBase<Integer> {
    private java.lang.String typeName;
    private int sqlType;

    public Null(int sqlType, java.lang.String typeName) {
      super(sqlType);
      this.sqlType = sqlType;
      this.typeName = typeName;
    }

    public Null(int sqlType) {
      super(sqlType);
      this.sqlType = sqlType;
      typeName = null;
    }

    public Null(ParameterBase parm) {
      super(((Parameter.Null)parm).getValue());
    }

    @Override
    public int getSqlType() {
      return Types.NULL;
    }

    public int getSqlTypeWrapping() {
      return sqlType;
    }
  }

  public static class Timestamp extends ParameterBase<java.sql.Timestamp> {
    private Calendar cal;

    public Timestamp(java.sql.Timestamp x, Calendar cal) {
      super(x);
      this.cal = cal;
    }

    public Timestamp(java.sql.Timestamp x) {
      super(x);
      this.cal = null;
    }

    public Timestamp(ParameterBase parm) {
      super(((Parameter.Timestamp)parm).getValue());
    }

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
    private Calendar cal;

    public Time(java.sql.Time x, Calendar cal) {
      super(x);
      this.cal = cal;
    }

    public Time(java.sql.Time x) {
      super(x);
      this.cal = null;
    }

    public Time(ParameterBase parm) {
      super(((Parameter.Time)parm).getValue());
    }

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
    private Calendar cal;

    public Date(java.sql.Date x, Calendar cal) {
      super(x);
      this.cal = cal;
    }

    public Date(java.sql.Date x) {
      super(x);
      this.cal = null;
    }

    public Date(ParameterBase parm) {
      super(((Parameter.Date)parm).getValue());
    }

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

  public static class Array extends ParameterBase<java.sql.Array> {
    public Array(java.sql.Array x) {
      super(x);
    }

    public Array(ParameterBase parm) {
      super(((Parameter.Array)parm).getValue());
    }

    @Override
    public int getSqlType() {
      return Types.ARRAY;
    }
  }

  public static class Clob extends ParameterBase<byte[]> {
    public Clob(java.sql.Clob x) throws UnsupportedEncodingException {
      super(((com.sonicbase.query.impl.Clob)x).getString().getBytes("utf-8"));
    }

    public Clob(ParameterBase parm) {
      super(((Parameter.Clob)parm).getValue());
    }

    public Clob(byte[] bytes) {
      super(bytes);
    }

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
    public Blob(java.sql.Blob x) throws SQLException {
      super(x.getBytes(0, (int)x.length()));
    }

    public Blob(byte[] x) throws SQLException {
      super(x);
    }

    public Blob(ParameterBase parm) {
      super(((Parameter.Blob)parm).getValue());
    }

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
      return new Parameter.BinaryStream(bytes, length);
    }

    @Override
    public int getSqlType() {
      return Types.BLOB;
    }
  }

  public static class UnicodeStream extends ParameterBase<InputStream> {
    private int length = -1;

    public UnicodeStream(InputStream x, int length) {
      super(x);
      this.length = length;
    }

    @Override
    public int getSqlType() {
      return Types.OTHER;
    }
  }


  public static class Bytes extends ParameterBase<byte[]> {
    public Bytes(byte[] x) {
      super(x);
    }

    public Bytes(ParameterBase parm) {
      super(((Parameter.Bytes)parm).getValue());
    }

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
      return new Parameter.BinaryStream(bytes, length);
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

    public String(ParameterBase parm) {
      super(((Parameter.String)parm).getValue());
    }

    public void serialize(DataOutputStream out, boolean writeType) throws IOException {
      super.serialize(out, writeType);
//      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
//      GZIPOutputStream zipOut = new GZIPOutputStream(bytesOut);
//      zipOut.write(bytes);
//      zipOut.close();
//      bytes = bytesOut.toByteArray();
      byte[] bytes = getValue();
      out.writeInt(bytes.length);
      out.write(bytes);
    }

    public static ParameterBase deserialize(DataInputStream in) throws IOException {
      int len = in.readInt();
      byte[] bytes = new byte[len];
      in.readFully(bytes);
      //      GZIPInputStream zipIn = new GZIPInputStream(new ByteArrayInputStream(bytes));
      //      int avail = zipIn.available();
      //      bytes = new byte[avail];
      //      zipIn.read(bytes);
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
      DataUtil.ResultLength resultLength = new DataUtil.ResultLength();
      int len = (int) DataUtil.readVLong(in, resultLength);
      byte[] buffer = new byte[len];
      in.readFully(buffer);
      java.lang.String str = new java.lang.String(buffer, "utf-8");
      return new Parameter.BigDecimal(new java.math.BigDecimal(str));
    }

    public void serialize(DataOutputStream out, boolean b) throws IOException {
      super.serialize(out, b);
      java.lang.String strValue = getValue().toPlainString();
      byte[] bytes = strValue.getBytes("utf-8");
      DataUtil.ResultLength resultLength = new DataUtil.ResultLength();
      DataUtil.writeVLong(out, bytes.length, resultLength);
      out.write(bytes);
    }
  }

  public static class Double extends ParameterBase<java.lang.Double> {
    public Double(double x) {
      super(x);
    }

    public Double(ParameterBase parm) {
      super(((Parameter.Double)parm).getValue());
    }

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

    public Float(ParameterBase parm) {
      super(((Parameter.Float)parm).getValue());
    }

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

    public Long(ParameterBase parm) {
      super(((Parameter.Long)parm).getValue());
    }

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

    public Int(ParameterBase parm) {
      super(((Parameter.Int)parm).getValue());
    }

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

    public Short(ParameterBase parm) {
      super(((Parameter.Short)parm).getValue());
    }

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

    public Boolean(ParameterBase parm) {
      super(((Parameter.Boolean)parm).getValue());
    }

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
