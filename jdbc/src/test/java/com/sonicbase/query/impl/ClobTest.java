package com.sonicbase.query.impl;

import org.apache.commons.io.IOUtils;
import org.testng.annotations.Test;

import java.io.IOException;
import java.sql.SQLException;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

public class ClobTest {

  @Test
  public void test() throws SQLException, IOException {
    Clob clob = new Clob();
    clob.setString(0, "value");
    assertEquals(clob.getString(), "value");

    assertEquals(clob.position("lu", 0), 2);
    Clob searchStr = new Clob();
    searchStr.setString(0, "lu");
    assertEquals(clob.position(searchStr, 0), 2);

    assertEquals(IOUtils.toString(clob.getAsciiStream()), "value");

    assertEquals(IOUtils.toString(clob.getCharacterStream()), "value");

    assertEquals(clob.getSubString(2, 2), "lu");

    assertEquals(clob.length(), "value".length());

    clob.truncate(2);
    assertEquals(clob.getString(), "va");

    try {
      clob.setAsciiStream(0);
      fail();
    }
    catch (SQLException e) {
    }

    try {
      clob.setCharacterStream(0);
      fail();
    }
    catch (SQLException e) {
    }

    clob.setString(0, "value", 0, "value".length());
    assertEquals(clob.getString(), "value");

    assertEquals(IOUtils.toString(clob.getCharacterStream(2, 2)), "lu");
    clob.free();
    assertNull(clob.getString());
  }
}
