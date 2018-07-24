package com.sonicbase.accept.server;

import com.sonicbase.common.ComObject;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestComObject {

  @Test
  public void test() {

    ComObject cobj = new ComObject();

    cobj.put(ComObject.Tag.SERIALIZATION_VERSION, (short)554);
    cobj.put(ComObject.Tag.TABLE_NAME, "myTable");
    cobj.put(ComObject.Tag.IS_EXCPLICITE_TRANS, true);
    cobj.put(ComObject.Tag.RECORD_LENGTH, 999);
    cobj.put(ComObject.Tag.KEY_BYTES, new byte[]{1, 2, 3});

    byte[] bytes = cobj.serialize();

    cobj = new ComObject();
    cobj.deserialize(bytes);
    assertEquals((short)cobj.getShort(ComObject.Tag.SERIALIZATION_VERSION), 554);
    assertEquals(cobj.getString(ComObject.Tag.TABLE_NAME), "myTable");
    assertEquals((boolean)cobj.getBoolean(ComObject.Tag.IS_EXCPLICITE_TRANS), true);
    assertEquals((int)cobj.getInt(ComObject.Tag.RECORD_LENGTH), 999);
    assertTrue(Arrays.equals(cobj.getByteArray(ComObject.Tag.KEY_BYTES), new byte[]{1, 2, 3}));
  }
}
