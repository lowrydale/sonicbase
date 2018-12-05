package com.sonicbase.common;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class ComObjectTest {

  @Test
  public void testString() {
    ComObject cobj = new ComObject(1);
    cobj.put(ComObject.Tag.DB_NAME,"test");
    assertEquals(cobj.getString(ComObject.Tag.DB_NAME), "test");
  }

  @Test
  public void testByteArray() {
    ComObject cobj = new ComObject(1);
    ComArray array = cobj.putArray(ComObject.Tag.KEYS, ComObject.Type.BYTE_ARRAY_TYPE, 1);
    array.add(new byte[]{1, 2, 3});

    byte[] bytes = cobj.serialize();

    cobj = new ComObject(bytes);
    array = cobj.getArray(ComObject.Tag.KEYS);
    bytes = (byte[]) array.getArray().get(0);
    assertEquals(bytes[2], 3);
  }
}
