package com.sonicbase.procedure;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.jdbcdriver.StatementProxy;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.*;
import java.util.GregorianCalendar;
import java.util.Map;

import static java.sql.Types.BIGINT;
import static java.sql.Types.VARCHAR;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

public class SonicBasePreparedStatementImplTest {

  @Test
  public void test() throws SQLException, UnsupportedEncodingException, MalformedURLException {
    StoredProcedureContext context = new StoredProcedureContextImpl();
    StatementProxy statementProxy = new StatementProxy(mock(ConnectionProxy.class),
        mock(DatabaseClient.class), "select * from persons");
    SonicBasePreparedStatementImpl statement = new SonicBasePreparedStatementImpl(context, statementProxy);

    statement.setNull(1, BIGINT);
    assertNull(statementProxy.getParms().getValue(1));

    statement.setBoolean(1, true);
    assertEquals(statementProxy.getParms().getValue(1), true);

    statement.setByte(1, (byte)100);
    assertEquals(statementProxy.getParms().getValue(1), (byte)100);

    statement.setShort(1, (short)101);
    assertEquals(statementProxy.getParms().getValue(1), (short)101);

    statement.setInt(1, 102);
    assertEquals(statementProxy.getParms().getValue(1), 102);

    statement.setLong(1, 103L);
    assertEquals(statementProxy.getParms().getValue(1), (long)103);

    statement.setFloat(1, 103f);
    assertEquals(statementProxy.getParms().getValue(1), 103f);

    statement.setDouble(1, 103d);
    assertEquals(statementProxy.getParms().getValue(1), 103d);

    statement.setBigDecimal(1, new BigDecimal(103));
    assertEquals(statementProxy.getParms().getValue(1), new BigDecimal(103));

    statement.setString(1, "value");
    assertEquals(new String((char[])statementProxy.getParms().getValue(1)), "value");

    statement.setBytes(1, "value".getBytes("utf-8"));
    assertEquals(statementProxy.getParms().getValue(1), "value".getBytes("utf-8"));

    statement.setDate(1, new Date(1900, 1, 0));
    assertEquals(statementProxy.getParms().getValue(1), new Date(1900, 1, 0));

    statement.setTime(1, new Time(10, 1, 0));
    assertEquals(statementProxy.getParms().getValue(1), new Time(10, 1, 0));

    statement.setTimestamp(1, new Timestamp(0));
    assertEquals(statementProxy.getParms().getValue(1), new Timestamp(0));

    statement.setAsciiStream(1, new ByteArrayInputStream("value".getBytes("utf-8")), "value".length());
    assertEquals(new String((char[])statementProxy.getParms().getValue(1)), "value");

    statement.setUnicodeStream(1, new ByteArrayInputStream("value".getBytes("utf-8")), "value".length());
    assertEquals(new String((char[])statementProxy.getParms().getValue(1)), "value");

    statement.setBinaryStream(1, new ByteArrayInputStream("value".getBytes("utf-8")), "value".length());
    assertEquals(statementProxy.getParms().getValue(1), "value".getBytes("utf-8"));

    try {
      statement.setObject(1, "value", VARCHAR);
      assertEquals(statementProxy.getParms().getValue(1), "value".getBytes("utf-8"));
      fail();
    }
    catch (SQLException e) {
    }

    try {
      statement.setObject(1, "value");
      assertEquals(statementProxy.getParms().getValue(1), "value".getBytes("utf-8"));
      fail();
    }
    catch (SQLException e) {
    }

    statement.setCharacterStream(1, new InputStreamReader(new ByteArrayInputStream("value".getBytes("utf-8"))), "value".length());
    assertEquals(new String((char[])statementProxy.getParms().getValue(1)), "value");

    try {
      statement.setRef(1, new Ref() {
        @Override
        public String getBaseTypeName() {
          return null;
        }

        @Override
        public Object getObject(Map<String, Class<?>> map) {
          return null;
        }

        @Override
        public Object getObject() {
          return null;
        }

        @Override
        public void setObject(Object value) {

        }
      });
      assertEquals(statementProxy.getParms().getValue(1), "value".getBytes("utf-8"));
      fail();
    }
    catch (SQLException e) {
    }

    Blob blob = new com.sonicbase.query.impl.Blob("value".getBytes("utf-8"));
    statement.setBlob(1, blob);
    assertEquals(statementProxy.getParms().getValue(1), "value".getBytes("utf-8"));

    Clob clob = new com.sonicbase.query.impl.Clob("value");
    statement.setClob(1, clob);
    assertEquals(new String((char[])statementProxy.getParms().getValue(1)), "value");

    try {
      statement.setArray(1, null);
      assertEquals(statementProxy.getParms().getValue(1), "value".getBytes("utf-8"));
      fail();
    }
    catch (SQLException e) {
    }

    try {
      statement.setDate(1, new Date(1900, 1, 0), new GregorianCalendar());
      assertEquals(statementProxy.getParms().getValue(1), new Date(1900, 1, 0));
      fail();
    }
    catch (SQLException e) {
    }

    try {
      statement.setTime(1, new Time(10, 1, 0), new GregorianCalendar());
      assertEquals(statementProxy.getParms().getValue(1), new Time(10, 1, 0));
      fail();
    }
    catch (SQLException e) {
    }

    try {
      statement.setTimestamp(1, new Timestamp(0), new GregorianCalendar());
      assertEquals(statementProxy.getParms().getValue(1), new Timestamp(0));
      fail();
    }
    catch (SQLException e) {
    }

    statement.setNull(1, BIGINT, "BIGINT");
    assertNull(statementProxy.getParms().getValue(1));

    try {
      statement.setURL(1, new URL("http://localhost"));
      assertNull(statementProxy.getParms().getValue(1));
      fail();
    }
    catch (SQLException e) {
    }

    try {
      statement.setRowId(1, new RowId() {
        @Override
        public byte[] getBytes() {
          return new byte[0];
        }
      });
      assertNull(statementProxy.getParms().getValue(1));
      fail();
    }
    catch (SQLException e) {
    }

    statement.setNString(1, "value");
    assertEquals(new String((char[])statementProxy.getParms().getValue(1)), "value");

    statement.setNCharacterStream(1, new StringReader("value"), "value".length());
    assertEquals(new String((char[])statementProxy.getParms().getValue(1)), "value");

    NClob nclob = new com.sonicbase.query.impl.NClob("value");
    statement.setNClob(1, nclob);
    assertEquals(new String((char[])statementProxy.getParms().getValue(1)), "value");

    statement.setClob(1, new StringReader("value"), "value".length());
    assertEquals(new String((char[])statementProxy.getParms().getValue(1)), "value");

    statement.setBlob(1, new ByteArrayInputStream("value".getBytes("utf-8")), "value".length());
    assertEquals(statementProxy.getParms().getValue(1), "value".getBytes("utf-8"));

    statement.setNClob(1, new StringReader("value"), "value".length());
    assertEquals(new String((char[])statementProxy.getParms().getValue(1)), "value");

    try {
      statement.setSQLXML(1, null);
      assertEquals(statementProxy.getParms().getValue(1), "value".getBytes("utf-8"));
      fail();
    }
    catch (SQLException e) {
    }

    try {
      statement.setObject(1, "value", VARCHAR, 10);
      assertEquals(statementProxy.getParms().getValue(1), "value".getBytes("utf-8"));
      fail();
    }
    catch (SQLException e) {
    }

    statement.setAsciiStream(1, new ByteArrayInputStream("value".getBytes("utf-8")), (long)"value".length());
    assertEquals(new String((char[])statementProxy.getParms().getValue(1)), "value");

    statement.setBinaryStream(1, new ByteArrayInputStream("value".getBytes("utf-8")), (long)"value".length());
    assertEquals(statementProxy.getParms().getValue(1), "value".getBytes("utf-8"));

    statement.setCharacterStream(1, new InputStreamReader(new ByteArrayInputStream("value".getBytes("utf-8"))), (long)"value".length());
    assertEquals(new String((char[])statementProxy.getParms().getValue(1)), "value");


    statement.setAsciiStream(1, new ByteArrayInputStream("value".getBytes("utf-8")));
    assertEquals(new String((char[])statementProxy.getParms().getValue(1)), "value");

    statement.setBinaryStream(1, new ByteArrayInputStream("value".getBytes("utf-8")));
    assertEquals(statementProxy.getParms().getValue(1), "value".getBytes("utf-8"));

    statement.setCharacterStream(1, new InputStreamReader(new ByteArrayInputStream("value".getBytes("utf-8"))));
    assertEquals(new String((char[])statementProxy.getParms().getValue(1)), "value");

    statement.setNCharacterStream(1, new StringReader("value"));
    assertEquals(new String((char[])statementProxy.getParms().getValue(1)), "value");

    statement.setClob(1, new InputStreamReader(new ByteArrayInputStream("value".getBytes("utf-8"))));
    assertEquals(new String((char[])statementProxy.getParms().getValue(1)), "value");

    statement.setBlob(1, new ByteArrayInputStream("value".getBytes("utf-8")));
    assertEquals(statementProxy.getParms().getValue(1), "value".getBytes("utf-8"));

    statement.setNClob(1, new InputStreamReader(new ByteArrayInputStream("value".getBytes("utf-8"))));
    assertEquals(new String((char[])statementProxy.getParms().getValue(1)), "value");

    try {
      statement.setObject(1, "value", JDBCType.BIGINT, 10);
      assertEquals(statementProxy.getParms().getValue(1), "value".getBytes("utf-8"));
      fail();
    }
    catch (SQLException e) {
    }

    try {
      statement.setObject(1, "value", JDBCType.BIGINT);
      assertEquals(statementProxy.getParms().getValue(1), "value".getBytes("utf-8"));
      fail();
    }
    catch (SQLException e) {
    }

  }
}
