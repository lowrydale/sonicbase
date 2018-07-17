/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.jdbc;

import com.sonicbase.jdbcdriver.Parameter;
import org.apache.commons.io.IOUtils;
import org.testng.annotations.Test;

import java.io.*;
import java.math.BigDecimal;
import java.sql.*;

import static org.testng.Assert.assertEquals;

public class ParameterTest {

  @Test
  public void test() throws IOException, SQLException {
    Parameter.NClobReader reader = new Parameter.NClobReader(new StringReader("123"));
    assertEquals(reader.getSqlType(), Types.NCLOB);
    assertEquals(IOUtils.toString(reader.getValue()), "123");
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytesOut);
    reader.serialize(out, false);
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray()));
    reader = (Parameter.NClobReader) Parameter.NClobReader.deserialize(in);
    assertEquals(IOUtils.toString(reader.getValue()), "123");

    Parameter.ClobReader creader = new Parameter.ClobReader(new StringReader("123"));
    assertEquals(creader.getSqlType(), Types.CLOB);
    assertEquals(IOUtils.toString(creader.getValue()), "123");
    bytesOut = new ByteArrayOutputStream();
    out = new DataOutputStream(bytesOut);
    creader.serialize(out, false);
    in = new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray()));
    creader = (Parameter.ClobReader) Parameter.ClobReader.deserialize(in);
    assertEquals(IOUtils.toString(creader.getValue()), "123");

    Parameter.CharacterStream cstream = new Parameter.CharacterStream(new StringReader("123"));
    assertEquals(cstream.getSqlType(), Types.VARCHAR);
    assertEquals(IOUtils.toString(cstream.getValue()), "123");
    bytesOut = new ByteArrayOutputStream();
    out = new DataOutputStream(bytesOut);
    cstream.serialize(out, false);
    in = new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray()));
    cstream = (Parameter.CharacterStream) Parameter.CharacterStream.deserialize(in);
    assertEquals(IOUtils.toString(cstream.getValue()), "123");

    Parameter.NCharacterStream nstream = new Parameter.NCharacterStream(new StringReader("123"));
    assertEquals(nstream.getSqlType(), Types.NVARCHAR);
    assertEquals(IOUtils.toString(nstream.getValue()), "123");
    bytesOut = new ByteArrayOutputStream();
    out = new DataOutputStream(bytesOut);
    nstream.serialize(out, false);
    in = new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray()));
    nstream = (Parameter.NCharacterStream) Parameter.NCharacterStream.deserialize(in);
    assertEquals(IOUtils.toString(nstream.getValue()), "123");


    Parameter.BinaryStream bstream = new Parameter.BinaryStream("123".getBytes("utf-8"));
    assertEquals(bstream.getSqlType(), Types.VARBINARY);
    assertEquals(IOUtils.toString(bstream.getValue()), "123");
    bytesOut = new ByteArrayOutputStream();
    out = new DataOutputStream(bytesOut);
    bstream.serialize(out, false);
    in = new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray()));
    bstream = (Parameter.BinaryStream) Parameter.BinaryStream.deserialize(in);
    assertEquals(IOUtils.toString(bstream.getValue()), "123");

    Parameter.AsciiStream astream = new Parameter.AsciiStream(new ByteArrayInputStream("123".getBytes("utf-8")));
    assertEquals(astream.getSqlType(), Types.VARCHAR);
    assertEquals(IOUtils.toString(astream.getValue()), "123");
    bytesOut = new ByteArrayOutputStream();
    out = new DataOutputStream(bytesOut);
    astream.serialize(out, false);
    in = new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray()));
    astream = (Parameter.AsciiStream) Parameter.AsciiStream.deserialize(in);
    assertEquals(IOUtils.toString(astream.getValue()), "123");

    NClob nc = new com.sonicbase.query.impl.NClob();
    nc.setString(0, "123");
    Parameter.NClob nclob = new Parameter.NClob(nc);
    assertEquals(nclob.getSqlType(), Types.NCLOB);
    assertEquals(IOUtils.toString(nclob.getValue()), "123");
    bytesOut = new ByteArrayOutputStream();
    out = new DataOutputStream(bytesOut);
    nclob.serialize(out, false);
    in = new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray()));
    nclob = (Parameter.NClob) Parameter.NClob.deserialize(in);
    assertEquals(IOUtils.toString(nclob.getValue()), "123");

    Parameter.NString nstring = new Parameter.NString("123");
    assertEquals(nstring.getSqlType(), Types.NVARCHAR);
    assertEquals(nstring.getValue(), "123".getBytes("utf-8"));
    bytesOut = new ByteArrayOutputStream();
    out = new DataOutputStream(bytesOut);
    nstring.serialize(out, false);
    in = new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray()));
    nstring = (Parameter.NString) Parameter.NString.deserialize(in);
    assertEquals(nstring.getValue(), "123".getBytes("utf-8"));

    Parameter.Timestamp timestamp = new Parameter.Timestamp(new Timestamp(123));
    assertEquals(timestamp.getSqlType(), Types.TIMESTAMP);
    assertEquals(timestamp.getValue(), new Timestamp(123));
    bytesOut = new ByteArrayOutputStream();
    out = new DataOutputStream(bytesOut);
    timestamp.serialize(out, false);
    in = new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray()));
    timestamp = (Parameter.Timestamp) Parameter.Timestamp.deserialize(in);
    assertEquals(timestamp.getValue(), new Timestamp(123));

    Parameter.Time time = new Parameter.Time(new Time(123));
    assertEquals(time.getSqlType(), Types.TIME);
    assertEquals(time.getValue(), new Time(123));
    bytesOut = new ByteArrayOutputStream();
    out = new DataOutputStream(bytesOut);
    time.serialize(out, false);
    in = new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray()));
    time = (Parameter.Time) Parameter.Time.deserialize(in);
    assertEquals(time.getValue(), new Time(123));

    Parameter.Date date = new Parameter.Date(new Date(123));
    assertEquals(date.getSqlType(), Types.DATE);
    assertEquals(date.getValue(), new Time(123));
    bytesOut = new ByteArrayOutputStream();
    out = new DataOutputStream(bytesOut);
    date.serialize(out, false);
    in = new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray()));
    date = (Parameter.Date) Parameter.Date.deserialize(in);
    assertEquals(date.getValue(), new Date(123));

    Clob cl = new com.sonicbase.query.impl.Clob();
    cl.setString(0, "123");
    Parameter.Clob clob = new Parameter.Clob(cl);
    assertEquals(clob.getSqlType(), Types.CLOB);
    assertEquals(IOUtils.toString(clob.getValue()), "123");
    bytesOut = new ByteArrayOutputStream();
    out = new DataOutputStream(bytesOut);
    clob.serialize(out, false);
    in = new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray()));
    clob = (Parameter.Clob) Parameter.Clob.deserialize(in);
    assertEquals(IOUtils.toString(clob.getValue()), "123");

    Blob bl = new com.sonicbase.query.impl.Blob();
    bl.setBytes(0, "123".getBytes("utf-8"));
    Parameter.Blob blob = new Parameter.Blob(bl);
    assertEquals(blob.getSqlType(), Types.BLOB);
    assertEquals(IOUtils.toString(blob.getValue()), "123");
    bytesOut = new ByteArrayOutputStream();
    out = new DataOutputStream(bytesOut);
    blob.serialize(out, false);
    in = new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray()));
    blob = (Parameter.Blob) Parameter.Blob.deserialize(in);
    assertEquals(IOUtils.toString(blob.getValue()), "123");

    Parameter.UnicodeStream ustream = new Parameter.UnicodeStream(new ByteArrayInputStream("123".getBytes("utf-8")), 3);
    assertEquals(ustream.getSqlType(), Types.VARCHAR);
    assertEquals(new String(ustream.getValue(), "utf-8"), "123");
    bytesOut = new ByteArrayOutputStream();
    out = new DataOutputStream(bytesOut);
    ustream.serialize(out, false);
    in = new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray()));
    ustream = (Parameter.UnicodeStream) Parameter.UnicodeStream.deserialize(in);
    assertEquals(new String(ustream.getValue(), "utf-8"), "123");

    Parameter.Bytes bytes = new Parameter.Bytes("123".getBytes("utf-8"));
    assertEquals(bytes.getSqlType(), Types.VARBINARY);
    assertEquals(IOUtils.toString(bytes.getValue()), "123");
    bytesOut = new ByteArrayOutputStream();
    out = new DataOutputStream(bytesOut);
    bytes.serialize(out, false);
    in = new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray()));
    bytes = (Parameter.Bytes) Parameter.Bytes.deserialize(in);
    assertEquals(IOUtils.toString(bytes.getValue()), "123");

    Parameter.String string = new Parameter.String("123".getBytes("utf-8"));
    assertEquals(string.getSqlType(), Types.VARCHAR);
    assertEquals(string.getValue(), "123".getBytes("utf-8"));
    bytesOut = new ByteArrayOutputStream();
    out = new DataOutputStream(bytesOut);
    string.serialize(out, false);
    in = new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray()));
    string = (Parameter.String) Parameter.String.deserialize(in);
    assertEquals(string.getValue(), "123".getBytes("utf-8"));

    Parameter.BigDecimal bigDecimal = new Parameter.BigDecimal(new BigDecimal(123));
    assertEquals(bigDecimal.getSqlType(), Types.NUMERIC);
    assertEquals(bigDecimal.getValue(), new BigDecimal(123));
    bytesOut = new ByteArrayOutputStream();
    out = new DataOutputStream(bytesOut);
    bigDecimal.serialize(out, false);
    in = new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray()));
    bigDecimal = (Parameter.BigDecimal) Parameter.BigDecimal.deserialize(in);
    assertEquals(bigDecimal.getValue(), new BigDecimal(123));

    Parameter.Double db= new Parameter.Double(123d);
    assertEquals(db.getSqlType(), Types.DOUBLE);
    assertEquals(db.getValue(), 123d);
    bytesOut = new ByteArrayOutputStream();
    out = new DataOutputStream(bytesOut);
    db.serialize(out, false);
    in = new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray()));
    db = (Parameter.Double) Parameter.Double.deserialize(in);
    assertEquals(db.getValue(), 123d);

    Parameter.Float fl = new Parameter.Float(123f);
    assertEquals(fl.getSqlType(), Types.FLOAT);
    assertEquals(fl.getValue(), 123f);
    bytesOut = new ByteArrayOutputStream();
    out = new DataOutputStream(bytesOut);
    fl.serialize(out, false);
    in = new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray()));
    fl = (Parameter.Float) Parameter.Float.deserialize(in);
    assertEquals(fl.getValue(), 123f);

    Parameter.Long l = new Parameter.Long(123L);
    assertEquals(l.getSqlType(), Types.BIGINT);
    assertEquals((long)l.getValue(), 123L);
    bytesOut = new ByteArrayOutputStream();
    out = new DataOutputStream(bytesOut);
    l.serialize(out, false);
    in = new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray()));
    l = (Parameter.Long) Parameter.Long.deserialize(in);
    assertEquals((long)l.getValue(), 123L);

    Parameter.Int i = new Parameter.Int(123);
    assertEquals(i.getSqlType(), Types.INTEGER);
    assertEquals((int)i.getValue(), 123);
    bytesOut = new ByteArrayOutputStream();
    out = new DataOutputStream(bytesOut);
    i.serialize(out, false);
    in = new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray()));
    i = (Parameter.Int) Parameter.Int.deserialize(in);
    assertEquals((int)i.getValue(), 123);

    Parameter.Short s = new Parameter.Short((short)123);
    assertEquals(s.getSqlType(), Types.SMALLINT);
    assertEquals((short)s.getValue(), (short)123);
    bytesOut = new ByteArrayOutputStream();
    out = new DataOutputStream(bytesOut);
    s.serialize(out, false);
    in = new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray()));
    s = (Parameter.Short) Parameter.Short.deserialize(in);
    assertEquals((short)s.getValue(), (short)123);

    Parameter.Byte b = new Parameter.Byte((byte)123);
    assertEquals(b.getSqlType(), Types.TINYINT);
    assertEquals((byte)b.getValue(), (byte)123);
    bytesOut = new ByteArrayOutputStream();
    out = new DataOutputStream(bytesOut);
    b.serialize(out, false);
    in = new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray()));
    b = (Parameter.Byte) Parameter.Byte.deserialize(in);
    assertEquals((byte)b.getValue(), (byte)123);

    Parameter.Boolean bn = new Parameter.Boolean(true);
    assertEquals(bn.getSqlType(), Types.BOOLEAN);
    assertEquals((boolean)bn.getValue(), true);
    bytesOut = new ByteArrayOutputStream();
    out = new DataOutputStream(bytesOut);
    bn.serialize(out, false);
    in = new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray()));
    bn = (Parameter.Boolean) Parameter.Boolean.deserialize(in);
    assertEquals((boolean)bn.getValue(), true);
  }
}
