package com.sonicbase.jdbcdriver;


import com.sonicbase.client.DatabaseClient;
import com.sonicbase.query.DatabaseException;
import org.apache.commons.io.IOUtils;
import org.apache.giraph.utils.Varint;

import java.io.*;
import java.math.BigDecimal;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("squid:RedundantThrowsDeclarationCheck") //derivecd class throws SQLException
public class ParameterHandler {

  private HashMap<String, Parameter.ParameterBase> currParmsByName = new HashMap<>();
  private Map<Integer, Parameter.ParameterBase> currParmsByIndex = new HashMap<>();

  public ParameterHandler() {
  }

  public Map<Integer, Parameter.ParameterBase> getCurrParmsByIndex() {
    return currParmsByIndex;
  }

  public void clearBatch() throws SQLException {
    currParmsByName.clear();
    currParmsByIndex.clear();
  }

  public void setNull(int parameterIndex, int sqlType) throws SQLException {
    getCurrParmsByIndex().put(parameterIndex, new Parameter.Null(sqlType));
  }

  public void setBoolean(int parameterIndex, boolean x) throws SQLException {
    getCurrParmsByIndex().put(parameterIndex, new Parameter.Boolean(x));
  }

  public void setByte(int parameterIndex, byte x) throws SQLException {
    getCurrParmsByIndex().put(parameterIndex, new Parameter.Byte(x));
  }

  public void setShort(int parameterIndex, short x) throws SQLException {
    getCurrParmsByIndex().put(parameterIndex, new Parameter.Short(x));
  }

  public void setInt(int parameterIndex, int x) throws SQLException {
    getCurrParmsByIndex().put(parameterIndex, new Parameter.Int(x));
  }

  public void setLong(int parameterIndex, long x) throws SQLException {
    getCurrParmsByIndex().put(parameterIndex, new Parameter.Long(x));
  }

  public void setFloat(int parameterIndex, float x) throws SQLException {
    getCurrParmsByIndex().put(parameterIndex, new Parameter.Float(x));
  }

  public void setDouble(int parameterIndex, double x) throws SQLException {
    getCurrParmsByIndex().put(parameterIndex, new Parameter.Double(x));
  }

  public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
    getCurrParmsByIndex().put(parameterIndex, new Parameter.BigDecimal(x));
  }

  public void setString(int parameterIndex, String x) throws SQLException, UnsupportedEncodingException {
    getCurrParmsByIndex().put(parameterIndex, new Parameter.String(x.getBytes("utf-8")));
  }

  public void setBytes(int parameterIndex, byte[] x) throws SQLException {
    getCurrParmsByIndex().put(parameterIndex, new Parameter.Bytes(x));
  }

  public void setDate(int parameterIndex, Date x) throws SQLException {
    getCurrParmsByIndex().put(parameterIndex, new Parameter.Date(x));
  }

  public void setTime(int parameterIndex, Time x) throws SQLException {
    getCurrParmsByIndex().put(parameterIndex, new Parameter.Time(x));
  }

  public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
    getCurrParmsByIndex().put(parameterIndex, new Parameter.Timestamp(x));
  }

  public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException, IOException {
    getCurrParmsByIndex().put(parameterIndex, new Parameter.AsciiStream(x, length));
  }

  public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException, IOException {
    getCurrParmsByIndex().put(parameterIndex, new Parameter.UnicodeStream(x, length));
  }

  public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
//noblob
    byte[] bytes = new byte[length];
    try {
      x.read(bytes);
    } catch (Exception ex) {
      throw new SQLException(ex);
    }
    getCurrParmsByIndex().put(parameterIndex, new Parameter.BinaryStream(bytes, length));
  }

  public void clearParameters() throws SQLException {
    currParmsByIndex.clear();
    currParmsByName.clear();
  }

  public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException, IOException {
    getCurrParmsByIndex().put(parameterIndex, new Parameter.CharacterStream(reader, length));
  }

  public void setBlob(int parameterIndex, Blob x) throws SQLException {
    getCurrParmsByIndex().put(parameterIndex, new Parameter.Blob(x));
  }

  public void setClob(int parameterIndex, Clob x) throws SQLException, UnsupportedEncodingException {
    getCurrParmsByIndex().put(parameterIndex, new Parameter.Clob(x));
  }

  public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
    getCurrParmsByIndex().put(parameterIndex, new Parameter.Null(sqlType));
  }

  public ParameterMetaData getParameterMetaData() throws SQLException {
    throw new NotImplementedException();
  }

  public void setNString(int parameterIndex, String value) throws SQLException, UnsupportedEncodingException {
    getCurrParmsByIndex().put(parameterIndex, new Parameter.NString(value));
  }

  public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
    getCurrParmsByIndex().put(parameterIndex, new Parameter.NCharacterStream(value, length));
  }

  public void setNClob(int parameterIndex, NClob value) throws SQLException, UnsupportedEncodingException {
    getCurrParmsByIndex().put(parameterIndex, new Parameter.NClob(value));
  }

  public void setClob(int parameterIndex, Reader reader, long length) throws SQLException, IOException {
    getCurrParmsByIndex().put(parameterIndex, new Parameter.ClobReader(reader, length));
  }

  public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException, IOException {
    getCurrParmsByIndex().put(parameterIndex, new Parameter.Blob(inputStream, (int)length));
  }

  public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException, IOException {
    getCurrParmsByIndex().put(parameterIndex, new Parameter.NClobReader(reader, length));
  }

  public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException, IOException {
    getCurrParmsByIndex().put(parameterIndex, new Parameter.AsciiStream(x));
  }

  public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
    byte[] bytes = null;
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      IOUtils.copy(x, out);
      out.close();
      bytes = out.toByteArray();
      x.read(bytes);
    }
    catch (Exception ex) {
      throw new SQLException(ex);
    }
    getCurrParmsByIndex().put(parameterIndex, new Parameter.BinaryStream(bytes, bytes.length));

  }

  public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException, IOException {
    getCurrParmsByIndex().put(parameterIndex, new Parameter.CharacterStream(reader));
  }

  public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException, IOException {
    getCurrParmsByIndex().put(parameterIndex, new Parameter.NCharacterStream(value));
  }

  public void setClob(int parameterIndex, Reader reader) throws SQLException, IOException {
    getCurrParmsByIndex().put(parameterIndex, new Parameter.ClobReader(reader));
  }

  public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException, IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    IOUtils.copy(inputStream, out);
    out.close();
    byte[] bytes = out.toByteArray();

    getCurrParmsByIndex().put(parameterIndex, new Parameter.Blob(bytes));
  }

  public void setNClob(int parameterIndex, Reader reader) throws SQLException, IOException {
    getCurrParmsByIndex().put(parameterIndex, new Parameter.NClobReader(reader));
  }

  public byte[] serialize() throws IOException {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytesOut);
    serialize(out);
    out.close();
    return bytesOut.toByteArray();
  }

  public void serialize(DataOutputStream out) throws IOException {
    Varint.writeSignedVarLong(DatabaseClient.SERIALIZATION_VERSION, out);
    int count = currParmsByIndex.size();
    out.writeInt(count);
    for (int i = 1; i < count + 1; i++) {
      Parameter.ParameterBase parm = currParmsByIndex.get(i);
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream parmOut = new DataOutputStream(bytesOut);
      parm.serialize(parmOut, true);
      parmOut.close();
      byte[] bytes = bytesOut.toByteArray();
      Varint.writeSignedVarLong(bytes.length, out);
      out.write(bytes);
    }
  }

  public void deserialize(byte[] bytes) {
    deserialize(new DataInputStream(new ByteArrayInputStream(bytes)));
  }

  public void deserialize(DataInputStream in) {

    try {
      Varint.readSignedVarLong(in); // serializationVersion
      int count = in.readInt();
      for (int i = 0; i < count; i++) {
        int len = (int)Varint.readSignedVarLong(in);
        byte[] bytes = new byte[len];
        in.readFully(bytes);
        DataInputStream innerIn = new DataInputStream(new ByteArrayInputStream(bytes));
        int sqlType = innerIn.readInt();
        switch (sqlType) {
          case Types.BLOB:
            currParmsByIndex.put(i + 1, Parameter.Blob.deserialize(innerIn));
            break;
          case Types.NCLOB:
            currParmsByIndex.put(i + 1, Parameter.NClob.deserialize(innerIn));
            break;
          case Types.CLOB:
            currParmsByIndex.put(i + 1, Parameter.Clob.deserialize(innerIn));
            break;
          case Types.VARCHAR:
            currParmsByIndex.put(i + 1, Parameter.String.deserialize(innerIn));
            break;
          case Types.NVARCHAR:
            currParmsByIndex.put(i + 1, Parameter.NString.deserialize(innerIn));
            break;
          case Types.VARBINARY:
            currParmsByIndex.put(i + 1, Parameter.Bytes.deserialize(innerIn));
            break;
          case Types.NUMERIC:
            currParmsByIndex.put(i + 1, Parameter.BigDecimal.deserialize(innerIn));
            break;
          case Types.INTEGER:
          case Types.DECIMAL:
            currParmsByIndex.put(i + 1, Parameter.Int.deserialize(innerIn));
            break;
          case Types.BIGINT:
            currParmsByIndex.put(i + 1, Parameter.Long.deserialize(innerIn));
            break;
          case Types.TINYINT:
            currParmsByIndex.put(i + 1, Parameter.Byte.deserialize(innerIn));
            break;
          case Types.SMALLINT:
          case Types.CHAR:
            currParmsByIndex.put(i + 1, Parameter.Short.deserialize(innerIn));
            break;
          case Types.FLOAT:
          case Types.REAL:
            currParmsByIndex.put(i + 1, Parameter.Float.deserialize(innerIn));
            break;
          case Types.DOUBLE:
            currParmsByIndex.put(i + 1, Parameter.Double.deserialize(innerIn));
            break;
          case Types.BOOLEAN:
            currParmsByIndex.put(i + 1, Parameter.Boolean.deserialize(innerIn));
            break;
          case Types.DATE:
            currParmsByIndex.put(i + 1, Parameter.Date.deserialize(innerIn));
            break;
          case Types.TIME:
            currParmsByIndex.put(i + 1, Parameter.Time.deserialize(innerIn));
            break;
          case Types.TIMESTAMP:
            currParmsByIndex.put(i + 1, Parameter.Timestamp.deserialize(innerIn));
            break;
        }
      }
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public Object getValue(int parmOffset) {
    return currParmsByIndex.get(parmOffset).getValue();
  }

  public Object getValue(String parmName) {
    return currParmsByName.get(parmName).getValue();
  }
}
