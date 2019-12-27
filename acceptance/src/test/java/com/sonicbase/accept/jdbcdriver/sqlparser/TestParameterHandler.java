package com.sonicbase.accept.jdbcdriver.sqlparser;

import com.sonicbase.jdbcdriver.ParameterHandler;
import org.testng.annotations.Test;

import java.io.*;

public class TestParameterHandler {

  @Test(enabled=false)
  public void test() throws Exception {
    ParameterHandler handler = new ParameterHandler();
    handler.setString(1, "testing");
    handler.setLong(2, 123);
    handler.setBoolean(3, true);
    handler.setString(4, "m");

    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytesOut);
    handler.serialize(out);

    handler = new ParameterHandler();
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray()));
    handler.deserialize(in);
    byte[] value = (byte[])handler.getValue(1);
    long longValue = (long)handler.getValue(2);
    boolean boolValue = (boolean)handler.getValue(3);
    byte[] genders = (byte[])handler.getValue(4);
  }
}
