package com.sonicbase.procedure;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.jdbcdriver.StatementProxy;
import com.sonicbase.query.impl.Blob;
import com.sonicbase.query.impl.Clob;
import com.sonicbase.query.impl.NClob;
import org.testng.annotations.Test;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static java.sql.Connection.TRANSACTION_READ_COMMITTED;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.*;

public class SonicBaseConnectionImplTest {

  @Test
  public void test() throws SQLException {
    ConnectionProxy connectionProxy = new ConnectionProxy("mock", (Properties) null);

    connectionProxy.addClient("mock", mock(DatabaseClient.class));

    SonicBaseConnectionImpl conn = new SonicBaseConnectionImpl(connectionProxy);
    Statement statement = conn.createStatement();
    assertNotNull(statement);

    PreparedStatement preparedStatement = conn.prepareStatement("select * from persons");
    assertEquals(((StatementProxy)preparedStatement).getSql(), "select * from persons");

    preparedStatement = conn.prepareSonicBaseStatement(new StoredProcedureContextImpl(), "select * from persons");
    assertTrue(preparedStatement instanceof SonicBasePreparedStatement);

    try {
      conn.prepareCall("select * from person");
      fail();
    }
    catch (SQLException e) {
    }

    try {
      conn.nativeSQL("select * from persons");
      fail();
    }
    catch (SQLException e) {
    }

    conn.setAutoCommit(true);
    assertEquals(connectionProxy.getAutoCommit(), true);
    assertEquals(conn.getAutoCommit(), true);

    conn.commit();
    assertEquals(conn.getAutoCommit(), true);

    conn.setAutoCommit(true);
    assertEquals(conn.getAutoCommit(), true);
    conn.rollback();
    assertEquals(conn.getAutoCommit(), true);

    try {
      conn.getMetaData();
      fail();
    }
    catch (SQLException e) {
    }

    try {
      conn.setReadOnly(true);
      fail();
    }
    catch (SQLException e) {
    }

    assertFalse(conn.isReadOnly());

    try {
      conn.setCatalog(null);
      fail();
    }
    catch (SQLException e) {
    }

    assertNull(conn.getCatalog());

    try {
      conn.setTransactionIsolation(1);
      fail();
    }
    catch (SQLException e) {
    }

    assertEquals(conn.getTransactionIsolation(), TRANSACTION_READ_COMMITTED);

    assertNull(conn.getWarnings());

    conn.clearWarnings();

    try {
      statement = conn.createStatement(10, 10);
      assertTrue(statement instanceof StatementProxy);
      fail();
    }
    catch (SQLException e) {
    }

    try {
      preparedStatement = conn.prepareStatement("select * from persons", 10, 10);
      fail();
    }
    catch (SQLException e) {
    }

    try {
      preparedStatement = conn.prepareSonicBaseStatement(new StoredProcedureContextImpl(), "select * from persons", 10, 10);
      fail();
    }
    catch (SQLException e) {
    }

    try {
      conn.prepareCall("select * from persons", 10, 10);
      fail();
    }
    catch (SQLException e) {
    }

    Map<String, Class<?>> map = new HashMap<>();
    map.put("String", String.class);
    conn.setTypeMap(map);

    assertEquals(conn.getTypeMap().get("String"), String.class);

    try {
      conn.setHoldability(1);
      fail();
    }
    catch (SQLException e) {
    }

    assertEquals(conn.getHoldability(), ResultSet.HOLD_CURSORS_OVER_COMMIT);

    try {
      conn.setSavepoint();
      fail();
    }
    catch (SQLException e) {
    }

    try {
      conn.setSavepoint("value");
      fail();
    }
    catch (SQLException e) {
    }

    try {
      conn.rollback(null);
      fail();
    }
    catch (SQLException e) {
    }

    try {
      conn.releaseSavepoint(null);
      fail();
    }
    catch (SQLException e) {
    }

    try {
      conn.createStatement(1, 1, 1);
      fail();
    }
    catch (SQLException e) {
    }

    try {
      conn.prepareStatement("select * from persons", 1, 1, 1);
      fail();
    }
    catch (SQLException e) {
    }

    try {
      conn.prepareCall("select * from persons", 1, 1, 1);
      fail();
    }
    catch (SQLException e) {
    }


    try {
      conn.prepareStatement("select * from persons", 1);
      fail();
    }
    catch (SQLException e) {
    }

    try {
      conn.prepareStatement("select * from persons", (int[])null);
      fail();
    }
    catch (SQLException e) {
    }

    try {
      conn.prepareStatement("select * from persons", (String[])null);
      fail();
    }
    catch (SQLException e) {
    }

    assertTrue(conn.createClob() instanceof Clob);

    assertTrue(conn.createBlob() instanceof Blob);

    assertTrue(conn.createNClob() instanceof NClob);

    try {
      conn.createSQLXML();
      fail();
    }
    catch (SQLException e) {
    }

    try {
      conn.isValid(10);
      fail();
    }
    catch (SQLException e) {
    }

    try {
      conn.setClientInfo(null);
      fail();
    }
    catch (SQLClientInfoException e) {
    }

    try {
      conn.setClientInfo("name", "value");
      fail();
    }
    catch (SQLClientInfoException e) {
    }

    try {
      conn.getClientInfo("name");
      fail();
    }
    catch (SQLClientInfoException e) {
    }

    try {
      conn.getClientInfo();
      fail();
    }
    catch (SQLClientInfoException e) {
    }

    try {
      conn.createArrayOf("type", null);
      fail();
    }
    catch (SQLException e) {
    }

    try {
      conn.createStruct("type", null);
      fail();
    }
    catch (SQLException e) {
    }

    try {
      conn.setSchema("schema");
      fail();
    }
    catch (SQLException e) {
    }

    try {
      conn.getSchema();
      fail();
    }
    catch (SQLException e) {
    }

    try {
      conn.abort(null);
      fail();
    }
    catch (SQLException e) {
    }

    try {
      conn.setNetworkTimeout(null, 1);
      fail();
    }
    catch (SQLException e) {
    }

    try {
      conn.getNetworkTimeout();
      fail();
    }
    catch (SQLException e) {
    }

    try {
      conn.unwrap(null);
      fail();
    }
    catch (SQLException e) {
    }

    try {
      conn.isWrapperFor(null);
      fail();
    }
    catch (SQLException e) {
    }


    conn.close();
    assertTrue(conn.isClosed());
  }
}
