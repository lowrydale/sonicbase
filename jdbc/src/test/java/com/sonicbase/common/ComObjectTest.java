package com.sonicbase.common;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class ComObjectTest {

  @Test
  public void testString() {
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.DB_NAME,"test");
    assertEquals(cobj.getString(ComObject.Tag.DB_NAME), "test");
  }
}
