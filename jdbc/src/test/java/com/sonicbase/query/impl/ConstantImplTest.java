/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.query.impl;

import org.testng.annotations.Test;

import java.io.*;
import java.math.BigDecimal;

import static java.sql.Types.*;
import static org.testng.Assert.assertEquals;

public class ConstantImplTest {

  @Test
  public void test() throws UnsupportedEncodingException {

    ConstantImpl c = new ConstantImpl("value".getBytes("utf-8"), VARCHAR);
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

    c = new ConstantImpl("value".getBytes("utf-8"), CLOB);
    assertEquals(c.toString(), "value");
    c = roundTrip(c);
    assertEquals(c.toString(), "value");

    c = new ConstantImpl("value".getBytes("utf-8"), CHAR);
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
