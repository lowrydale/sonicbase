package com.sonicbase.query.impl;

import org.testng.annotations.Test;

import java.io.*;
import java.math.BigDecimal;

import static java.sql.Types.*;
import static org.testng.Assert.assertEquals;

public class ConstantImplTest {

  @Test
  public void test() throws UnsupportedEncodingException {

    String str = "value";
    char[] chars = new char[str.length()];
    str.getChars(0, str.length(), chars, 0);
    ConstantImpl c = new ConstantImpl(chars, VARCHAR);
    assertEquals(c.toString(), "value");
    c = roundTrip(c);
    assertEquals(c.toString(), "value");

    c = new ConstantImpl(100L, BIGINT);
    c = roundTrip(c);
    assertEquals(c.toString(), "100");
    c.negate();
    assertEquals(c.toString(), "-100");

    c = new ConstantImpl(100, INTEGER);
    c = roundTrip(c);
    assertEquals(c.toString(), "100");
    c.negate();
    assertEquals(c.toString(), "-100");

    str = "value";
    chars = new char[str.length()];
    str.getChars(0, str.length(), chars, 0);
    c = new ConstantImpl(chars, CLOB);
    assertEquals(c.toString(), "value");
    c = roundTrip(c);
    assertEquals(c.toString(), "value");

    str = "value";
    chars = new char[str.length()];
    str.getChars(0, str.length(), chars, 0);
    c = new ConstantImpl(chars, CHAR);
    assertEquals(c.toString(), "value");
    c = roundTrip(c);
    assertEquals(c.toString(), "value");

    c = new ConstantImpl(new BigDecimal(100), NUMERIC);
    assertEquals(c.toString(), "100");
    c = roundTrip(c);
    assertEquals(c.toString(), "100");
    c.negate();
    assertEquals(c.toString(), "-100");

    c = new ConstantImpl(new BigDecimal(100), DECIMAL);
    assertEquals(c.toString(), "100");
    c = roundTrip(c);
    assertEquals(c.toString(), "100");
    c.negate();
    assertEquals(c.toString(), "-100");

    c = new ConstantImpl((short)100, SMALLINT);
    c = roundTrip(c);
    assertEquals(c.toString(), "100");
    c.negate();
    assertEquals(c.toString(), "-100");

    c = new ConstantImpl((byte)100, TINYINT);
    c = roundTrip(c);
    assertEquals(c.toString(), "100");
    c.negate();
    assertEquals(c.toString(), "-100");

    c = new ConstantImpl(100f, REAL);
    c = roundTrip(c);
    assertEquals(c.toString(), "100.0");
    c.negate();
    assertEquals(c.toString(), "-100.0");

    c = new ConstantImpl(100f, FLOAT);
    c = roundTrip(c);
    assertEquals(c.toString(), "100.0");
    c.negate();
    assertEquals(c.toString(), "-100.0");

    c = new ConstantImpl(100d, DOUBLE);
    c = roundTrip(c);
    assertEquals(c.toString(), "100.0");
    c.negate();
    assertEquals(c.toString(), "-100.0");

  }

  private ConstantImpl roundTrip(ConstantImpl c) {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytesOut);
    c.serialize((short) 100, out);
    c = new ConstantImpl();
    c.deserialize((short) 100, new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray())));
    return c;
  }
}
